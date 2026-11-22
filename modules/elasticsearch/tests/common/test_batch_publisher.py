"""Unit tests & live tests cho READY report validation và publish/rollback 6 Batch aliases."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import pytest

from modules.elasticsearch.common.config.es_client import ESClient
from modules.elasticsearch.common.publication.batch_publisher import (
    PublicationError,
    build_atomic_alias_actions,
    publish_batch_aliases,
    rollback_batch_aliases,
)
from modules.elasticsearch.common.publication.ready_report import (
    IndexReadyEntry,
    ReadyReport,
    load_ready_report,
    save_ready_report,
    validate_ready_reports_pair,
    verify_physical_indices_in_es,
)


def _make_reports(run_id: str = "20261024T104145Z") -> tuple[ReadyReport, ReadyReport]:
    run_suffix = run_id.lower()
    p2 = ReadyReport(
        owner="P2",
        schema_version="phase2-3.0",
        manifest_sha256="a" * 64,
        batch_run_id=run_id,
        batch_cutoff_time="2018-10-01T00:00:00Z",
        ref_version="20261017-5f26995f",
        sql_test_result="PASS",
        completed_at="2026-11-22T02:00:00Z",
        status="READY",
        indices=[
            IndexReadyEntry("movie_batch_stats", f"movie_batch_stats_{run_suffix}", 9724, 0, 0, 100836),
            IndexReadyEntry("movie_daily_stats", f"movie_daily_stats_{run_suffix}", 99538, 0, 0, 100836),
            IndexReadyEntry("analytics_timeseries", f"analytics_timeseries_{run_suffix}", 5000, 0, 0),
            IndexReadyEntry("analytics_top_movies", f"analytics_top_movies_{run_suffix}", 9724, 0, 0),
        ],
    )
    p3 = ReadyReport(
        owner="P3",
        schema_version="phase2-3.0",
        manifest_sha256="a" * 64,
        batch_run_id=run_id,
        batch_cutoff_time="2018-10-01T00:00:00Z",
        ref_version="20261017-5f26995f",
        sql_test_result="PASS",
        completed_at="2026-11-22T02:05:00Z",
        status="READY",
        indices=[
            IndexReadyEntry("genre_trends", f"genre_trends_{run_suffix}", 32894, 0, 0, 274480),
            IndexReadyEntry("analytics_genre_trends", f"analytics_genre_trends_{run_suffix}", 4000, 0, 0),
        ],
    )
    return p2, p3


class _FakeESForPublisher:
    def __init__(self, p2: ReadyReport, p3: ReadyReport, initial_aliases: Dict[str, str] | None = None):
        self.counts: Dict[str, int] = {}
        self.runs: Dict[str, List[Dict[str, Any]]] = {}
        self.cutoffs: Dict[str, List[Dict[str, Any]]] = {}
        for rep in (p2, p3):
            for entry in rep.indices:
                self.counts[entry.physical_index] = entry.row_count
                self.runs[entry.physical_index] = [
                    {"key": rep.batch_run_id, "doc_count": entry.row_count}
                ]
                self.cutoffs[entry.physical_index] = [
                    {"key_as_string": "2018-10-01T00:00:00.000Z", "doc_count": entry.row_count}
                ]

        self.alias_to_indices: Dict[str, List[str]] = {}
        for alias, phys in (initial_aliases or {}).items():
            self.alias_to_indices[alias] = [phys]

        self.alias_calls: List[List[Dict[str, Any]]] = []
        self.fail_on_update: Dict[str, Any] | None = None

    def index_exists(self, name: str) -> bool:
        return name in self.counts

    def count(self, name: str) -> int:
        return self.counts[name]

    def search_smoke(self, name: str) -> Dict[str, Any]:
        indices = self.alias_to_indices.get(name, [name])
        return {
            "index_or_alias": name,
            "searchable": True,
            "count_seen": sum(self.counts[index] for index in indices),
        }

    def search(self, name: str, body: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "aggregations": {
                "runs": {"buckets": self.runs[name]},
                "cutoffs": {"buckets": self.cutoffs[name]},
            }
        }

    def get_aliases(self, names=None) -> Dict[str, Any]:
        requested = set(names or [])
        out: Dict[str, Any] = {}
        for alias, indices in self.alias_to_indices.items():
            if requested and alias not in requested:
                continue
            for idx in indices:
                out.setdefault(idx, {"aliases": {}})["aliases"][alias] = {}
        return out

    def update_aliases(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        self.alias_calls.append(actions)
        if self.fail_on_update is not None and len(self.alias_calls) == 1:
            # Giả lập ES đổi dở 1 alias rồi trả về lỗi một phần
            first_add = next((a["add"] for a in actions if "add" in a), None)
            if first_add:
                self.alias_to_indices[first_add["alias"]] = [first_add["index"]]
            return self.fail_on_update

        for act in actions:
            if "remove" in act:
                rem = act["remove"]
                cur = self.alias_to_indices.get(rem["alias"], [])
                if rem["index"] in cur:
                    cur.remove(rem["index"])
            elif "add" in act:
                add = act["add"]
                cur = self.alias_to_indices.setdefault(add["alias"], [])
                if add["index"] not in cur:
                    cur.append(add["index"])
        return {"acknowledged": True}


def test_ready_report_save_and_load_roundtrip(tmp_path: Path) -> None:
    p2, _ = _make_reports()
    report_file = tmp_path / "p2_ready.json"
    save_ready_report(p2, report_file)
    loaded = load_ready_report(report_file)
    assert loaded == p2


def test_validate_ready_reports_rejects_mismatches_and_errors() -> None:
    p2, p3 = _make_reports()
    assert len(validate_ready_reports_pair(p2, p3)) == 6

    p3_wrong_run = ReadyReport(**{**p3.__dict__, "batch_run_id": "20261025T000000Z"})
    with pytest.raises(ValueError, match="batch_run_id"):
        validate_ready_reports_pair(p2, p3_wrong_run)

    p3_missing = ReadyReport(**{**p3.__dict__, "indices": p3.indices[:1]})
    with pytest.raises(ValueError, match="Thiếu Batch alias"):
        validate_ready_reports_pair(p2, p3_missing)

    p3_bulk_err = ReadyReport(
        **{
            **p3.__dict__,
            "indices": [
                IndexReadyEntry("genre_trends", "genre_trends_r1", 32894, 0, 2, 274480),
                p3.indices[1],
            ],
        }
    )
    with pytest.raises(ValueError, match="bulk_error_count"):
        validate_ready_reports_pair(p2, p3_bulk_err)


def test_verify_physical_indices_rejects_count_mismatch_or_mixed_run() -> None:
    p2, p3 = _make_reports()
    es = _FakeESForPublisher(p2, p3)
    es.counts["genre_trends_20261024t104145z"] = 100
    with pytest.raises(ValueError, match="Lệch số dòng"):
        verify_physical_indices_in_es(es, p2, p3)  # type: ignore[arg-type]

    es2 = _FakeESForPublisher(p2, p3)
    es2.runs["genre_trends_20261024t104145z"] = [
        {"key": "20261024T104145Z", "doc_count": 30000},
        {"key": "old_run", "doc_count": 2894},
    ]
    with pytest.raises(ValueError, match="batch_run_id"):
        verify_physical_indices_in_es(es2, p2, p3)  # type: ignore[arg-type]


def test_atomic_alias_actions_first_publish_vs_swap() -> None:
    target = {"movie_batch_stats": "movie_batch_stats_r2", "genre_trends": "genre_trends_r2"}

    first_actions = build_atomic_alias_actions(
        {"movie_batch_stats": [], "genre_trends": []}, target
    )
    assert all("add" in a for a in first_actions)

    swap_actions = build_atomic_alias_actions(
        {"movie_batch_stats": ["movie_batch_stats_r1"], "genre_trends": ["genre_trends_r1"]},
        target,
    )
    removes = [a["remove"] for a in swap_actions if "remove" in a]
    adds = [a["add"] for a in swap_actions if "add" in a]
    assert len(removes) == 2
    assert all(r["must_exist"] is True for r in removes)
    assert len(adds) == 2


def test_publish_success_and_partial_failure_triggers_rollback(tmp_path: Path) -> None:
    p2_r1, p3_r1 = _make_reports("20261024T104145Z")
    es_ok = _FakeESForPublisher(p2_r1, p3_r1)
    marker = tmp_path / "marker.json"
    audit_dir = tmp_path / "audits"

    res = publish_batch_aliases(es_ok, p2_r1, p3_r1, marker_path=marker, audit_dir=audit_dir)  # type: ignore[arg-type]
    assert res["status"] == "PUBLISHED"
    assert len(es_ok.alias_calls) == 1
    assert len(es_ok.alias_calls[0]) == 6
    marker_data = json.loads(marker.read_text(encoding="utf-8"))
    assert marker_data["locked"] is False
    assert marker_data["batch_run_id"] == "20261024T104145Z"

    # Đổi sang r2 nhưng giả lập response lỗi 1 phần -> khóa marker và tự rollback về r1
    p2_r2, p3_r2 = _make_reports("20261025T104145Z")
    initial_r1 = {e.alias_name: e.physical_index for r in (p2_r1, p3_r1) for e in r.indices}
    es_fail = _FakeESForPublisher(p2_r2, p3_r2, initial_aliases=initial_r1)
    es_fail.fail_on_update = {
        "acknowledged": True,
        "errors": True,
        "action_results": [{"error": {"reason": "simulated partial failure"}}],
    }

    with pytest.raises(PublicationError, match="ROLLED_BACK"):
        publish_batch_aliases(es_fail, p2_r2, p3_r2, marker_path=marker, audit_dir=audit_dir)  # type: ignore[arg-type]

    assert len(es_fail.alias_calls) == 2
    for alias, old_phys in initial_r1.items():
        assert es_fail.alias_to_indices[alias] == [old_phys]
    locked_marker = json.loads(marker.read_text(encoding="utf-8"))
    assert locked_marker["locked"] is True
    assert locked_marker["status"] == "ROLLED_BACK"


@pytest.mark.skipif(os.environ.get("ES_LIVE_TEST") != "1", reason="Đặt ES_LIVE_TEST=1 để chạy test live")
def test_live_6_alias_atomic_publish_swap_and_rollback(tmp_path: Path) -> None:
    admin = ESClient(owner="admin")
    alias_specs = [
        ("dev_p5_pub_movie_batch_stats", "P2"),
        ("dev_p5_pub_movie_daily_stats", "P2"),
        ("dev_p5_pub_genre_trends", "P3"),
        ("dev_p5_pub_analytics_timeseries", "P2"),
        ("dev_p5_pub_analytics_top_movies", "P2"),
        ("dev_p5_pub_analytics_genre_trends", "P3"),
    ]
    created_indices: List[str] = []

    def _build_run(run_id: str) -> tuple[ReadyReport, ReadyReport]:
        p2_entries: List[IndexReadyEntry] = []
        p3_entries: List[IndexReadyEntry] = []
        suffix = run_id.lower()
        for alias, owner in alias_specs:
            phys = f"{alias}_{suffix}"
            created_indices.append(phys)
            admin.delete_index(phys)
            admin._request(
                "PUT",
                f"/{phys}",
                json_body={
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                    "mappings": {
                        "properties": {
                            "batch_run_id": {"type": "keyword"},
                            "batch_cutoff_time": {
                                "type": "date",
                                "format": "strict_date_optional_time||strict_date",
                            },
                        }
                    },
                },
            )
            admin.bulk_upsert(
                phys,
                [
                    {
                        "_id": f"doc_{suffix}",
                        "batch_run_id": run_id,
                        "batch_cutoff_time": "2018-10-01T00:00:00Z",
                    }
                ],
                refresh=True,
            )
            entry = IndexReadyEntry(alias, phys, 1, 0, 0)
            if owner == "P2":
                p2_entries.append(entry)
            else:
                p3_entries.append(entry)

        p2_rep = ReadyReport(
            owner="P2",
            schema_version="phase2-3.0",
            manifest_sha256="b" * 64,
            batch_run_id=run_id,
            batch_cutoff_time="2018-10-01T00:00:00Z",
            ref_version="20261017-5f26995f",
            sql_test_result="PASS",
            completed_at="2026-11-22T02:00:00Z",
            status="READY",
            indices=p2_entries,
        )
        p3_rep = ReadyReport(
            owner="P3",
            schema_version="phase2-3.0",
            manifest_sha256="b" * 64,
            batch_run_id=run_id,
            batch_cutoff_time="2018-10-01T00:00:00Z",
            ref_version="20261017-5f26995f",
            sql_test_result="PASS",
            completed_at="2026-11-22T02:01:00Z",
            status="READY",
            indices=p3_entries,
        )
        return p2_rep, p3_rep

    expected_aliases = [spec[0] for spec in alias_specs]
    marker = tmp_path / "marker.json"
    audit_dir = tmp_path / "audits"

    try:
        p2_r1, p3_r1 = _build_run("20261024T104145Z")
        res1 = publish_batch_aliases(
            admin, p2_r1, p3_r1, marker_path=marker, audit_dir=audit_dir, expected_aliases=expected_aliases
        )
        assert res1["status"] == "PUBLISHED"
        for alias in expected_aliases:
            hit = admin.search(alias, {"size": 1})["hits"]["hits"][0]["_source"]
            assert hit["batch_run_id"] == "20261024T104145Z"

        p2_r2, p3_r2 = _build_run("20261025T104145Z")
        res2 = publish_batch_aliases(
            admin, p2_r2, p3_r2, marker_path=marker, audit_dir=audit_dir, expected_aliases=expected_aliases
        )
        assert res2["status"] == "PUBLISHED"
        for alias in expected_aliases:
            hit = admin.search(alias, {"size": 1})["hits"]["hits"][0]["_source"]
            assert hit["batch_run_id"] == "20261025T104145Z"

        rb = rollback_batch_aliases(admin, res2["before_map"], marker_path=marker, reason="live_test")
        assert rb["status"] == "ROLLED_BACK"
        for alias in expected_aliases:
            hit = admin.search(alias, {"size": 1})["hits"]["hits"][0]["_source"]
            assert hit["batch_run_id"] == "20261024T104145Z"
    finally:
        for phys in created_indices:
            admin.delete_index(phys)
