"""Unit and live tests for the P5 TMDB Realtime Spark SQL lane."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pytest

from modules.elasticsearch.common.config.es_client import (
    BulkItemError,
    BulkOperationError,
    BulkResult,
    ESClient,
)
from modules.spark_sql.common.readers.es_reader import register_temp_view, rows_to_dataframe
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    prepare_analytics_records,
)
from modules.spark_sql.lanes.tmdb_realtime.change_detector import (
    detect_top_k_change,
)
from modules.spark_sql.lanes.tmdb_realtime.job import (
    build_tmdb_signal_df,
    cleanup_old_physical_indices,
    sync_once,
)


class _InMemoryESClient:
    """In-memory ES simulator supporting aliases, sorting, aggregations, PIT, and bulk errors."""

    def __init__(
        self,
        owner: str = "admin",
        *,
        _shared: Optional["_InMemoryESClient"] = None,
    ) -> None:
        self.owner = owner
        if _shared is not None:
            self._root = _shared._root
            self.indices = _shared.indices
            self.aliases = _shared.aliases
            self.create_index_calls = _shared.create_index_calls
            self.delete_calls = _shared.delete_calls
            self.search_calls = _shared.search_calls
        else:
            self._root = self
            self.indices: Dict[str, Dict[str, Dict[str, Any]]] = {}
            self.aliases: Dict[str, List[str]] = {}
            self._fail_on_write_ids: set[str] = set()
            self.create_index_calls: List[str] = []
            self.delete_calls: List[str] = []
            self.search_calls: List[str] = []

    @property
    def fail_on_write_ids(self) -> set[str]:
        return self._root._fail_on_write_ids

    @fail_on_write_ids.setter
    def fail_on_write_ids(self, value: set[str]) -> None:
        self._root._fail_on_write_ids = set(value)

    def as_owner(self, owner: str) -> "_InMemoryESClient":
        """Return a view of the same in-memory cluster authenticated as another owner."""
        return _InMemoryESClient(owner=owner, _shared=self._root)

    def _resolve_targets(self, index_or_alias: str) -> List[str]:
        if index_or_alias in self.indices:
            return [index_or_alias]
        if index_or_alias in self.aliases:
            return [idx for idx in self.aliases[index_or_alias] if idx in self.indices]
        return []

    def index_exists(self, name: str) -> bool:
        return bool(self._resolve_targets(name))

    def create_index(
        self,
        index_name: str,
        body: Optional[Dict[str, Any]] = None,  # noqa: ARG002
        *,
        require_absent: bool = True,
    ) -> Dict[str, Any]:
        if require_absent and index_name in self.indices:
            raise ValueError(f"Physical index đã tồn tại và không được tái sử dụng: {index_name}")
        self.indices.setdefault(index_name, {})
        self.create_index_calls.append(index_name)
        return {"acknowledged": True, "index": index_name}

    def refresh(self, index_or_alias: str) -> Dict[str, Any]:  # noqa: ARG002
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    def bulk_upsert(
        self,
        index_name: str,
        records: Iterable[Dict[str, Any]],
        id_field: str = "_id",
        batch_size: int = 1000,  # noqa: ARG002
        refresh: bool = False,  # noqa: ARG002
        raise_on_error: bool = True,
    ) -> BulkResult:
        rec_list = list(records)
        bucket = self.indices.setdefault(index_name, {})
        errors: List[BulkItemError] = []
        succeeded = 0
        for raw in rec_list:
            doc = dict(raw)
            doc_id = str(doc.pop(id_field))
            if doc_id in self.fail_on_write_ids:
                errors.append(
                    BulkItemError(
                        index=index_name,
                        doc_id=doc_id,
                        status=400,
                        error_type="mapper_parsing_exception",
                        reason="Simulated bulk item failure",
                    )
                )
            else:
                bucket[doc_id] = doc
                succeeded += 1

        result = BulkResult(index_name, len(rec_list), succeeded, len(errors), errors)
        if errors and raise_on_error:
            raise BulkOperationError(result)
        return result

    def count(self, index_or_alias: str) -> int:
        return sum(len(self.indices[idx]) for idx in self._resolve_targets(index_or_alias))

    def get_mapping(self, index_or_alias: str) -> Dict[str, Any]:
        props = {
            "schema_version": {"type": "keyword"},
            "analytics_run_id": {"type": "keyword"},
            "published_at": {"type": "date"},
            "tmdbId": {"type": "integer"},
            "event_time": {"type": "date"},
            "popularity": {"type": "double"},
            "vote_count": {"type": "long"},
            "vote_average": {"type": "double"},
            "prev_popularity": {"type": "double"},
            "popularity_delta": {"type": "double"},
            "updated_at": {"type": "date"},
            "title": {"type": "text"},
            "source": {"type": "keyword"},
        }
        return {
            idx: {"mappings": {"properties": props}}
            for idx in self._resolve_targets(index_or_alias)
        }

    def search_smoke(self, index_or_alias: str) -> Dict[str, Any]:
        return {
            "index_or_alias": index_or_alias,
            "searchable": True,
            "count_seen": self.count(index_or_alias),
        }

    def search(self, index_or_alias: str, body: Dict[str, Any]) -> Dict[str, Any]:
        self.search_calls.append(index_or_alias)
        items: List[tuple[str, Dict[str, Any]]] = []
        for idx in self._resolve_targets(index_or_alias):
            items.extend(self.indices[idx].items())

        if body.get("aggs"):
            schema_counts: Dict[str, int] = {}
            run_counts: Dict[str, int] = {}
            missing_published = 0
            for _, doc in items:
                sv = doc.get("schema_version")
                if sv is not None:
                    schema_counts[str(sv)] = schema_counts.get(str(sv), 0) + 1
                ar = doc.get("analytics_run_id")
                if ar is not None:
                    run_counts[str(ar)] = run_counts.get(str(ar), 0) + 1
                if doc.get("published_at") is None:
                    missing_published += 1
            return {
                "aggregations": {
                    "schema_versions": {
                        "buckets": [
                            {"key": k, "doc_count": v} for k, v in sorted(schema_counts.items())
                        ]
                    },
                    "analytics_runs": {
                        "buckets": [
                            {"key": k, "doc_count": v} for k, v in sorted(run_counts.items())
                        ]
                    },
                    "missing_published_at": {"doc_count": missing_published},
                }
            }

        for sort_spec in reversed(body.get("sort") or []):
            field, direction = next(iter(sort_spec.items()))
            reverse = str(direction).lower() == "desc"

            def _sort_key(pair: tuple[str, Dict[str, Any]]) -> Any:
                val = pair[1].get(field)
                if field in {"event_time", "updated_at"} and isinstance(val, str):
                    txt = val[:-1] + "+00:00" if val.endswith("Z") else val
                    return datetime.fromisoformat(txt).astimezone(timezone.utc)
                return val

            items.sort(key=_sort_key, reverse=reverse)

        size = int(body.get("size", 10))
        sliced = items[:size]
        return {
            "hits": {
                "total": {"value": len(items)},
                "hits": [{"_id": doc_id, "_source": dict(doc)} for doc_id, doc in sliced],
            }
        }

    def search_all_pit(
        self,
        index_or_alias: str,
        include_id: bool = True,
        **kwargs,  # noqa: ARG002
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for idx in self._resolve_targets(index_or_alias):
            for doc_id, doc in self.indices[idx].items():
                row = dict(doc)
                if include_id:
                    row["_id"] = doc_id
                rows.append(row)
        return rows

    def get_aliases(self, names: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        if names is None:
            result: Dict[str, Any] = {}
            for idx in self.indices:
                idx_aliases = {
                    alias: {} for alias, targets in self.aliases.items() if idx in targets
                }
                result[idx] = {"aliases": idx_aliases}
            return result

        wanted = set(names)
        result = {}
        for alias, targets in self.aliases.items():
            if alias not in wanted:
                continue
            for idx in targets:
                if idx in self.indices:
                    result.setdefault(idx, {"aliases": {}})["aliases"][alias] = {}
        return result

    def update_aliases(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        for action in actions:
            if "remove" in action:
                item = action["remove"]
                targets = self.aliases.get(item["alias"], [])
                if item.get("must_exist") and item["index"] not in targets:
                    raise RuntimeError(f"Alias {item['alias']} không trỏ tới {item['index']}")
                if item["index"] in targets:
                    targets.remove(item["index"])
            elif "add" in action:
                item = action["add"]
                targets = self.aliases.setdefault(item["alias"], [])
                if item["index"] not in targets:
                    targets.append(item["index"])
        return {"acknowledged": True}

    def delete_index(self, index_name: str, ignore_missing: bool = True) -> None:
        if not ignore_missing and index_name not in self.indices:
            raise RuntimeError(f"Index not found: {index_name}")
        self.indices.pop(index_name, None)
        for targets in self.aliases.values():
            if index_name in targets:
                targets.remove(index_name)
        self.delete_calls.append(index_name)


def _make_doc(
    tmdb_id: int,
    *,
    event_time: str = "2026-11-24T12:00:00Z",
    popularity: float = 100.0,
    vote_count: int = 500,
    vote_average: float = 7.5,
    prev_popularity: Optional[float] = 90.0,
    popularity_delta: Optional[float] = 10.0,
    updated_at: str = "2026-11-24T12:00:05Z",
    title: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "_id": str(tmdb_id),
        "tmdbId": tmdb_id,
        "title": f"Movie {tmdb_id}" if title is None else title,
        "event_time": event_time,
        "popularity": popularity,
        "vote_count": vote_count,
        "vote_average": vote_average,
        "prev_popularity": prev_popularity,
        "popularity_delta": popularity_delta,
        "updated_at": updated_at,
    }


def _seed_25_docs() -> List[Dict[str, Any]]:
    """Create 25 docs with deterministic Top-20 ordering and edge cases."""
    docs: List[Dict[str, Any]] = []
    # Top 2: tied on event_time=12:00 and popularity=500.0 -> tmdbId=101 must precede 102
    docs.append(_make_doc(102, event_time="2026-11-24T12:00:00Z", popularity=500.0))
    docs.append(
        _make_doc(
            101,
            event_time="2026-11-24T12:00:00Z",
            popularity=500.0,
            prev_popularity=None,
            popularity_delta=None,
            title="   Spaces Movie   ",
        )
    )
    # Ranks 3..10 at event_time=12:00 with descending popularity
    for idx in range(3, 11):
        docs.append(
            _make_doc(
                100 + idx,
                event_time="2026-11-24T12:00:00Z",
                popularity=450.0 - idx * 5,
                title="   " if idx == 3 else f"Movie {100 + idx}",
            )
        )
    # Ranks 11..20 at event_time=11:55 with descending popularity
    for idx in range(11, 21):
        docs.append(
            _make_doc(
                200 + idx,
                event_time="2026-11-24T11:55:00Z",
                popularity=300.0 - idx,
            )
        )
    # Ranks 21..25 at older event_time=11:00 (including high popularity=9999.0 that must lose on event_time)
    docs.append(_make_doc(901, event_time="2026-11-24T11:00:00Z", popularity=9999.0))
    for idx in range(2, 6):
        docs.append(_make_doc(900 + idx, event_time="2026-11-24T11:00:00Z", popularity=50.0 - idx))
    return docs


@pytest.fixture(scope="module")
def spark():
    session = create_spark_sql_session(app_name="test-tmdb-realtime-lane")
    yield session
    session.stop()


def test_sql_selects_top_20_ordered_by_event_time_then_popularity_then_id(spark) -> None:
    docs = _seed_25_docs()
    # Explicitly add one doc with missing title key entirely
    docs[3].pop("title", None)

    rows_to_dataframe(spark, "tmdb_rt", docs).createOrReplaceTempView("tmdb_rt")
    out_df = build_tmdb_signal_df(
        spark,
        schema_version="phase2-3.0",
        analytics_run_id="tmdb-test-01",
        published_at="2026-11-24T12:01:00Z",
    )
    records = prepare_analytics_records(
        out_df,
        ANALYTICS_KEY_COLUMNS["analytics_tmdb_signal"],
        analytics_family="analytics_tmdb_signal",
    )

    assert len(records) == 20
    expected_ids = [101, 102] + [100 + i for i in range(3, 11)] + [200 + i for i in range(11, 21)]
    assert [r["tmdbId"] for r in records] == expected_ids
    assert [r["_id"] for r in records] == [str(tid) for tid in expected_ids]

    # Rank 1 (tmdbId=101): trimmed title, null prev_popularity/popularity_delta kept null
    assert records[0]["title"] == "Spaces Movie"
    assert records[0]["prev_popularity"] is None
    assert records[0]["popularity_delta"] is None
    # Rank 3 (tmdbId=103): blank title collapsed to None
    assert records[2]["title"] is None
    # Rank 4 (tmdbId=104): missing title stays None
    assert records[3]["title"] is None
    # Older high-popularity movie (901) must not enter Top 20
    assert 901 not in {r["tmdbId"] for r in records}
    assert all(r["source"] == "tmdb" for r in records)


def test_change_detector_skips_when_top_20_unchanged_even_if_21st_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    es = _InMemoryESClient(owner="admin")
    docs = _seed_25_docs()[:22]  # 20 Top + 2 outside Top 20 (901, 902)
    es.create_index("tmdb_realtime")
    es.bulk_upsert("tmdb_realtime", docs)

    # Target alias holds exact normalized Top 20
    es.create_index("analytics_tmdb_signal-v1")
    top20_docs = []
    for raw in docs[:20]:
        d = dict(raw)
        if d["tmdbId"] == 101:
            d["title"] = "Spaces Movie"
        if d["tmdbId"] == 103:
            d["title"] = None
        d.update(
            {
                "source": "tmdb",
                "schema_version": "phase2-3.0",
                "analytics_run_id": "tmdb-v1",
                "published_at": "2026-11-24T12:01:00Z",
            }
        )
        top20_docs.append(d)
    es.bulk_upsert("analytics_tmdb_signal-v1", top20_docs)
    es.update_aliases([{"add": {"index": "analytics_tmdb_signal-v1", "alias": "analytics_tmdb_signal"}}])

    # Mutate movie #21 and #22 (outside Top 20)
    es.bulk_upsert(
        "tmdb_realtime",
        [
            _make_doc(901, event_time="2026-11-24T11:05:00Z", popularity=12345.0),
            _make_doc(902, event_time="2026-11-24T11:05:00Z", popularity=888.0),
        ],
    )

    detection = detect_top_k_change(es)
    assert detection.has_change is False
    assert detection.reason == "NO_CHANGE"

    def _fail_if_spark_started(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("SparkSession must NOT start when Top 20 is unchanged")

    monkeypatch.setattr(
        "modules.spark_sql.lanes.tmdb_realtime.job.create_spark_sql_session",
        _fail_if_spark_started,
    )
    res = sync_once(
        report_dir=tmp_path,
        writer_client=es.as_owner("p5"),  # type: ignore[arg-type]
        admin_client=es,  # type: ignore[arg-type]
    )
    assert res["status"] == "NO_CHANGE"
    assert res["reason"] == "NO_CHANGE"
    assert es.create_index_calls == ["tmdb_realtime", "analytics_tmdb_signal-v1"]


def test_change_detector_detects_new_top_20_entry_or_field_update() -> None:
    es = _InMemoryESClient(owner="admin")
    docs = _seed_25_docs()[:21]
    es.create_index("tmdb_realtime")
    es.bulk_upsert("tmdb_realtime", docs)

    # Target has formatting differences (+00:00 and un-trimmed whitespace) -> still NO_CHANGE
    es.create_index("analytics_tmdb_signal-v1")
    top20 = [dict(d) for d in docs[:20]]
    top20[1]["title"] = "  Spaces Movie "
    top20[1]["event_time"] = "2026-11-24T12:00:00+00:00"
    es.bulk_upsert("analytics_tmdb_signal-v1", top20)
    es.update_aliases([{"add": {"index": "analytics_tmdb_signal-v1", "alias": "analytics_tmdb_signal"}}])

    assert detect_top_k_change(es).reason == "NO_CHANGE"

    # Update popularity of a Top-20 movie
    es.bulk_upsert(
        "tmdb_realtime",
        [_make_doc(104, event_time="2026-11-24T12:00:00Z", popularity=777.0)],
    )
    changed = detect_top_k_change(es)
    assert changed.has_change is True
    assert changed.reason == "TOP_K_CHANGED"
    assert changed.source_top_ids[0] == 104

    # Sync target back, then push movie #901 into Top 20 with newer event_time
    es.bulk_upsert(
        "analytics_tmdb_signal-v1",
        [_make_doc(104, event_time="2026-11-24T12:00:00Z", popularity=777.0)],
    )
    assert detect_top_k_change(es).reason == "NO_CHANGE"

    es.bulk_upsert(
        "tmdb_realtime",
        [_make_doc(901, event_time="2026-11-24T12:10:00Z", popularity=150.0)],
    )
    entered = detect_top_k_change(es)
    assert entered.has_change is True
    assert entered.reason == "TOP_K_CHANGED"
    assert entered.source_top_ids[0] == 901


def test_sync_once_builds_versioned_index_swaps_alias_and_skips_on_second_run(
    spark,
    tmp_path: Path,
) -> None:
    es = _InMemoryESClient(owner="admin")
    p5_es = es.as_owner("p5")
    es.create_index("tmdb_realtime")
    es.bulk_upsert("tmdb_realtime", _seed_25_docs()[:22])

    # Run 1: initial build & publish
    res1 = sync_once(
        report_dir=tmp_path,
        spark=spark,
        writer_client=p5_es,  # type: ignore[arg-type]
        admin_client=es,  # type: ignore[arg-type]
    )
    assert res1["status"] == "PUBLISHED"
    assert res1["reason"] == "TARGET_MISSING"
    assert res1["row_count"] == 20
    v1_index = res1["physical_index"]
    assert v1_index.startswith("analytics_tmdb_signal-v")
    assert es.aliases["analytics_tmdb_signal"] == [v1_index]
    assert Path(res1["ready_report"]).exists()
    state = json.loads((tmp_path / "tmdb_signal_sync_state.json").read_text(encoding="utf-8"))
    assert state["physical_index"] == v1_index

    # Verify doc 101 null delta and literal _id
    doc_101 = es.indices[v1_index]["101"]
    assert doc_101["prev_popularity"] is None
    assert doc_101["popularity_delta"] is None
    assert doc_101["title"] == "Spaces Movie"
    assert doc_101["source"] == "tmdb"

    # Run 2: immediate re-run -> NO_CHANGE
    res2 = sync_once(
        report_dir=tmp_path,
        spark=spark,
        writer_client=p5_es,  # type: ignore[arg-type]
        admin_client=es,  # type: ignore[arg-type]
    )
    assert res2["status"] == "NO_CHANGE"
    assert es.aliases["analytics_tmdb_signal"] == [v1_index]

    # Run 3: update 1 Top-20 movie -> builds v2 and swaps alias
    es.bulk_upsert(
        "tmdb_realtime",
        [
            _make_doc(
                101,
                event_time="2026-11-24T12:15:00Z",
                popularity=525.0,
                prev_popularity=500.0,
                popularity_delta=25.0,
                updated_at="2026-11-24T12:15:02Z",
                title="Spaces Movie",
            )
        ],
    )
    res3 = sync_once(
        report_dir=tmp_path,
        spark=spark,
        writer_client=p5_es,  # type: ignore[arg-type]
        admin_client=es,  # type: ignore[arg-type]
    )
    assert res3["status"] == "PUBLISHED"
    assert res3["reason"] == "TOP_K_CHANGED"
    v2_index = res3["physical_index"]
    assert v2_index != v1_index
    assert es.aliases["analytics_tmdb_signal"] == [v2_index]
    assert es.indices[v2_index]["101"]["popularity_delta"] == 25.0


def test_bulk_error_aborts_without_swapping_alias(spark, tmp_path: Path) -> None:
    es = _InMemoryESClient(owner="admin")
    p5_es = es.as_owner("p5")
    es.create_index("tmdb_realtime")
    es.bulk_upsert("tmdb_realtime", _seed_25_docs()[:20])

    first = sync_once(
        report_dir=tmp_path,
        spark=spark,
        writer_client=p5_es,  # type: ignore[arg-type]
        admin_client=es,  # type: ignore[arg-type]
    )
    v1_index = first["physical_index"]
    assert es.aliases["analytics_tmdb_signal"] == [v1_index]

    # Trigger update and inject Bulk item failure on movie 101
    es.bulk_upsert(
        "tmdb_realtime",
        [_make_doc(101, event_time="2026-11-24T12:30:00Z", popularity=610.0)],
    )
    es.fail_on_write_ids = {"101"}

    with pytest.raises(BulkOperationError):
        sync_once(
            report_dir=tmp_path,
            spark=spark,
            writer_client=p5_es,  # type: ignore[arg-type]
            admin_client=es,  # type: ignore[arg-type]
        )

    assert es.aliases["analytics_tmdb_signal"] == [v1_index]


def test_cleanup_old_physical_indices_keeps_recent_and_never_deletes_aliased() -> None:
    es = _InMemoryESClient(owner="admin")
    versions = [f"analytics_tmdb_signal-v20261124t00000{i}000z" for i in range(1, 8)]
    for idx in versions:
        es.create_index(idx)
    # Point alias to the OLDEST version (v...1)
    es.update_aliases([{"add": {"index": versions[0], "alias": "analytics_tmdb_signal"}}])

    deleted = cleanup_old_physical_indices(
        es,  # type: ignore[arg-type]
        alias_name="analytics_tmdb_signal",
        keep_versions=3,
    )
    assert deleted == versions[1:4]
    assert versions[0] in es.indices
    for newest in versions[4:]:
        assert newest in es.indices


@pytest.mark.skipif(
    os.environ.get("ES_LIVE_TEST") != "1",
    reason="Đặt ES_LIVE_TEST=1 để chạy test live với Elasticsearch",
)
def test_live_tmdb_lane_roundtrip_on_dev_indices(spark) -> None:
    p5 = ESClient(owner="p5")
    src_idx = "dev_p5_tmdb_realtime_lane_test"
    out_idx = "dev_p5_analytics_tmdb_signal_lane_test"
    sample_docs = _seed_25_docs()[:22]

    try:
        p5.delete_index(src_idx)
        p5.delete_index(out_idx)
        p5.bulk_upsert(src_idx, sample_docs, refresh=True)

        detection = detect_top_k_change(p5, source_index=src_idx, target_alias=out_idx)
        assert detection.has_change is True

        register_temp_view(spark, "tmdb_rt", index_name=src_idx, client=p5)
        df = build_tmdb_signal_df(
            spark,
            schema_version="phase2-3.0",
            analytics_run_id="tmdb-live-test",
            published_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        assert df.count() == 20
    finally:
        p5.delete_index(src_idx)
        p5.delete_index(out_idx)
