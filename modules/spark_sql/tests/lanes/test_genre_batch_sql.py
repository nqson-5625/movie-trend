"""Kiểm thử offline cho K1 (load_source, READY report) và K2 (Spark SQL genre_batch & Query 3)."""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from modules.elasticsearch.common.publication.ready_report import (
    IndexReadyEntry,
    ReadyReport,
    load_ready_report,
    save_ready_report,
    validate_ready_reports_pair,
)
from modules.elasticsearch.lanes.genre_batch.emit_ready_report import (
    build_p3_ready_report,
    verify_and_build_p3_ready_report,
)
from modules.elasticsearch.lanes.genre_batch.load_source import (
    EXPECTED_GENRE_COLUMNS,
    load_genre_source,
    parse_genre_daily_tsv,
)
from modules.spark_sql.common.readers.es_reader import rows_to_dataframe
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.key_serializer import build_record_id
from modules.spark_sql.lanes.genre_batch.build_genre_trends import (
    ANALYTICS_SQL_PATH,
    QUERY3_ISO_WEEK_SQL_PATH,
    compute_genre_trends_rows,
)

ROOT = Path(__file__).resolve().parents[4]
FIXTURES_DIR = ROOT / "contracts" / "phase2" / "genre_batch" / "fixtures"


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_parse_genre_daily_tsv_preserves_unknown_and_rejects_bad_rows(tmp_path: Path) -> None:
    manifest = {
        "run_id": "20261024T104145Z",
        "batch_cutoff_time": "2018-10-01T00:00:00Z",
        "generated_at": "2026-10-24T10:45:00Z",
        "reference": {"ref_version": "20261022-7c32dfb9"},
        "jobs": {
            "genre_daily_stats": {
                "columns": list(EXPECTED_GENRE_COLUMNS),
                "counters": {
                    "framework": {
                        "Reduce input records": 3,
                        "Reduce output records": 2,
                    },
                    "data_quality": {"duplicates_removed": 0},
                },
            }
        },
    }
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")

    row1 = "2018-09-29\tAction\t2\t2\t9.0\t4.5\t20261024T104145Z\t2018-10-01T00:00:00Z\t2026-10-24T10:45:00Z"
    row2 = "2018-09-29\tUNKNOWN\t1\t1\t3.0\t3.0\t20261024T104145Z\t2018-10-01T00:00:00Z\t2026-10-24T10:45:00Z"
    header = "\t".join(EXPECTED_GENRE_COLUMNS)

    # 1. Hỗ trợ TSV KHÔNG có header
    tsv_no_header = tmp_path / "part-00000"
    tsv_no_header.write_text("\n".join([row1, row2]) + "\n", encoding="utf-8")
    summary = load_genre_source(tsv_no_header, manifest_file, dry_run=True)
    assert summary["target_index"] == "genre_trends_20261024t104145z"
    assert summary["row_count"] == 2
    assert summary["sum_count"] == 3.0
    assert summary["unknown_rows"] == 1

    # 2. Hỗ trợ TSV CÓ header hợp lệ (như file export)
    tsv_with_header = tmp_path / "genre_daily_stats.tsv"
    tsv_with_header.write_text("\n".join([header, row1, row2]) + "\n", encoding="utf-8")
    summary_hdr = load_genre_source(tsv_with_header, manifest_file, dry_run=True)
    assert summary_hdr["row_count"] == 2
    assert summary_hdr["sum_count"] == 3.0

    # 3. Từ chối header sai tên/thứ tự cột
    bad_header = tmp_path / "bad_header.tsv"
    bad_header.write_text("\n".join(["date\tgenre\tinteraction_count\tavg_rating\trating_count\trating_sum\tbatch_run_id\tbatch_cutoff_time\tgenerated_at", row1, row2]), encoding="utf-8")
    with pytest.raises(ValueError, match="header"):
        load_genre_source(bad_header, manifest_file, dry_run=True)

    # 4. Từ chối file bị cắt cụt (chỉ còn 1 dòng trong khi manifest ghi 2 dòng / 3 lượt)
    truncated = tmp_path / "truncated.tsv"
    truncated.write_text(row1 + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Số dòng nguồn"):
        load_genre_source(truncated, manifest_file, dry_run=True)

    # 5. Từ chối generated_at lệch manifest
    bad_gen = tmp_path / "bad_gen.tsv"
    bad_gen.write_text(row1.replace("2026-10-24T10:45:00Z", "2026-10-24T11:00:00Z") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="generated_at"):
        parse_genre_daily_tsv(
            bad_gen,
            expected_run_id="20261024T104145Z",
            expected_cutoff="2018-10-01T00:00:00Z",
            expected_generated_at="2026-10-24T10:45:00Z",
        )

    records = parse_genre_daily_tsv(
        tsv_no_header,
        expected_run_id="20261024T104145Z",
        expected_cutoff="2018-10-01T00:00:00Z",
    )
    assert records[0]["_id"] == build_record_id(
        {"batch_run_id": "20261024T104145Z", "date": date(2018, 9, 29), "genre": "Action"},
        ("batch_run_id", "date", "genre"),
    )

    # Từ chối dòng >= cutoff
    bad_cutoff = tmp_path / "bad_cutoff.tsv"
    bad_cutoff.write_text(
        "2018-10-01\tAction\t1\t1\t4.0\t4.0\t20261024T104145Z\t2018-10-01T00:00:00Z\t2026-10-24T10:45:00Z\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="cutoff"):
        parse_genre_daily_tsv(bad_cutoff, expected_run_id="20261024T104145Z", expected_cutoff="2018-10-01T00:00:00Z")

    # Từ chối avg_rating sai
    bad_avg = tmp_path / "bad_avg.tsv"
    bad_avg.write_text(
        "2018-09-29\tAction\t2\t2\t9.0\t4.0\t20261024T104145Z\t2018-10-01T00:00:00Z\t2026-10-24T10:45:00Z\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="avg_rating"):
        parse_genre_daily_tsv(bad_avg, expected_run_id="20261024T104145Z", expected_cutoff="2018-10-01T00:00:00Z")


def test_p3_ready_report_validates_with_p2_pair(tmp_path: Path) -> None:
    run_id = "20261024t104145z"
    cutoff = "2018-10-01T00:00:00Z"
    ref_ver = "20261022-7c32dfb9"
    manifest_sha = "a" * 64

    p3 = build_p3_ready_report(
        batch_run_id=run_id,
        batch_cutoff_time=cutoff,
        ref_version=ref_ver,
        manifest_sha256=manifest_sha,
        source_physical_index=f"genre_trends_{run_id}",
        source_row_count=32894,
        source_sum_count=274480.0,
        analytics_physical_index=f"analytics_genre_trends_{run_id}",
        analytics_row_count=35000,
    )
    saved = save_ready_report(p3, tmp_path / f"p3_ready_{run_id}.json")
    loaded_p3 = load_ready_report(saved)

    p2_mock = ReadyReport(
        owner="p2",
        schema_version="phase2-3.0",
        manifest_sha256=manifest_sha,
        batch_run_id=run_id,
        batch_cutoff_time=cutoff,
        ref_version=ref_ver,
        sql_test_result="PASS",
        completed_at="2026-11-27T00:00:00Z",
        status="READY",
        indices=[
            IndexReadyEntry("movie_batch_stats", f"movie_batch_stats_{run_id}", 9724, 0, 0, 100836.0),
            IndexReadyEntry("movie_daily_stats", f"movie_daily_stats_{run_id}", 99538, 0, 0, 100836.0),
            IndexReadyEntry("analytics_timeseries", f"analytics_timeseries_{run_id}", 237096, 0, 0, 100836.0),
            IndexReadyEntry("analytics_top_movies", f"analytics_top_movies_{run_id}", 133157, 0, 0, 100836.0),
        ],
    )
    by_alias = validate_ready_reports_pair(p2_mock, loaded_p3)
    assert set(by_alias) == {
        "movie_batch_stats",
        "movie_daily_stats",
        "genre_trends",
        "analytics_timeseries",
        "analytics_top_movies",
        "analytics_genre_trends",
    }


def test_compute_genre_trends_matches_k0_fixture_100_percent() -> None:
    sources = _read_jsonl(FIXTURES_DIR / "genre_trends.jsonl")
    expected_analytics = _read_jsonl(FIXTURES_DIR / "analytics_genre_trends.jsonl")
    manifest_hash = hashlib.sha256((FIXTURES_DIR / "manifest.json").read_bytes()).hexdigest()

    actual = compute_genre_trends_rows(
        sources,
        ref_version="fixture-ref-v1",
        manifest_hash=manifest_hash,
        analytics_run_id="fixture-p3-v1-analytics",
        published_at="2026-11-27T00:00:00Z",
    )
    assert len(actual) == len(expected_analytics)

    by_key = {(r["period_type"], r["period_start"], r["genre"]): r for r in actual}
    for exp in expected_analytics:
        act = by_key[(exp["period_type"], exp["period_start"], exp["genre"])]
        assert act["interaction_count"] == exp["interaction_count"]
        assert act["rating_count"] == exp["rating_count"]
        assert act["rating_sum"] == pytest.approx(exp["rating_sum"])
        assert act["avg_rating"] == pytest.approx(exp["avg_rating"])
        assert act["rank_in_period"] == exp["rank_in_period"]


def test_query3_iso_week_aggregation_and_tie_break() -> None:
    sources = _read_jsonl(FIXTURES_DIR / "genre_trends.jsonl")
    manifest_hash = hashlib.sha256((FIXTURES_DIR / "manifest.json").read_bytes()).hexdigest()

    rows = compute_genre_trends_rows(
        sources,
        ref_version="fixture-ref-v1",
        manifest_hash=manifest_hash,
        analytics_run_id="fixture-p3-v1-analytics",
        published_at="2026-11-27T00:00:00Z",
        include_iso_week=True,
    )
    iso_rows = [r for r in rows if r["period_type"] == "iso_week"]
    # 2017-12-30 (Sat) & 2017-12-31 (Sun) -> Monday 2017-12-25 (2017-W52)
    # 2018-01-05 (Fri) & 2018-01-06 (Sat) -> Monday 2018-01-01 (2018-W01)
    assert {r["period_start"] for r in iso_rows} == {"2017-12-25", "2018-01-01"}

    w52 = {r["genre"]: r for r in iso_rows if r["period_start"] == "2017-12-25"}
    assert w52["Action"]["iso_week_label"] == "2017-W52"
    assert w52["Action"]["rank_in_period"] == 1  # Hòa 3 lượt với Drama -> Action thắng theo chữ cái
    assert w52["Drama"]["rank_in_period"] == 2
    assert w52["Action"]["avg_rating"] == pytest.approx(14.0 / 3.0)
    assert "UNKNOWN" in w52


def test_sql_files_follow_contract_rules() -> None:
    analytics_sql = ANALYTICS_SQL_PATH.read_text(encoding="utf-8")
    assert "FROM batch_genre" in analytics_sql
    assert "ROW_NUMBER()" in analytics_sql
    assert "ORDER BY interaction_count DESC, genre ASC" in analytics_sql
    assert "AVG(avg_rating)" not in analytics_sql.upper()

    iso_sql = QUERY3_ISO_WEEK_SQL_PATH.read_text(encoding="utf-8")
    assert "FROM batch_genre" in iso_sql
    assert "date_trunc('week', date)" in iso_sql
    assert "ORDER BY interaction_count DESC, genre ASC" in iso_sql
    assert "AVG(avg_rating)" not in iso_sql.upper()
    assert "YYYY-'W'ww" not in iso_sql


class _FakeESResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class _FakeESClient:
    def __init__(self, responses: dict[str, dict]) -> None:
        self._responses = responses

    def _request(self, method: str, path: str, **kwargs) -> _FakeESResponse:
        return _FakeESResponse(self._responses[path])


def test_verify_and_build_p3_ready_report_rejects_missing_or_mismatched_es(tmp_path: Path) -> None:
    manifest = {
        "run_id": "20261024T104145Z",
        "batch_cutoff_time": "2018-10-01T00:00:00Z",
        "generated_at": "2026-10-24T10:45:00Z",
        "reference": {"ref_version": "20261022-7c32dfb9"},
        "jobs": {
            "genre_daily_stats": {
                "columns": list(EXPECTED_GENRE_COLUMNS),
                "counters": {
                    "framework": {"Reduce input records": 16, "Reduce output records": 11},
                    "data_quality": {"duplicates_removed": 0},
                },
            }
        },
    }
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")

    ok_es = _FakeESClient(
        {
            "/genre_trends_20261024t104145z/_search": {
                "hits": {"total": {"value": 11}},
                "aggregations": {
                    "sum_inter": {"value": 16.0},
                    "sum_rating": {"value": 16.0},
                    "mismatch_meta": {"doc_count": 0},
                },
            },
            "/analytics_genre_trends_20261024t104145z/_search": {
                "hits": {"total": {"value": 27}},
                "aggregations": {
                    "mismatch_meta": {"doc_count": 0},
                    "by_period": {
                        "buckets": [
                            {"key": "day", "doc_count": 11, "sum_inter": {"value": 16.0}, "sum_rating": {"value": 16.0}},
                            {"key": "month", "doc_count": 8, "sum_inter": {"value": 16.0}, "sum_rating": {"value": 16.0}},
                            {"key": "year", "doc_count": 8, "sum_inter": {"value": 16.0}, "sum_rating": {"value": 16.0}},
                        ]
                    },
                },
            },
        }
    )
    report = verify_and_build_p3_ready_report(
        manifest_path=manifest_file,
        source_physical_index="genre_trends_20261024t104145z",
        analytics_physical_index="analytics_genre_trends_20261024t104145z",
        client=ok_es,  # type: ignore[arg-type]
    )
    assert report.status == "READY"
    assert report.indices[0].row_count == 11
    assert report.indices[1].row_count == 27

    # Thiếu dữ liệu source trên ES -> từ chối
    missing_es = _FakeESClient(
        {
            "/genre_trends_20261024t104145z/_search": {
                "hits": {"total": {"value": 1}},
                "aggregations": {"sum_inter": {"value": 2.0}, "sum_rating": {"value": 2.0}, "mismatch_meta": {"doc_count": 0}},
            }
        }
    )
    with pytest.raises(ValueError, match="manifest"):
        verify_and_build_p3_ready_report(
            manifest_path=manifest_file,
            source_physical_index="genre_trends_20261024t104145z",
            analytics_physical_index="analytics_genre_trends_20261024t104145z",
            client=missing_es,  # type: ignore[arg-type]
        )

    # Không cho xuất READY nếu SQL test FAIL
    with pytest.raises(ValueError, match="PASS"):
        verify_and_build_p3_ready_report(
            manifest_path=manifest_file,
            source_physical_index="genre_trends_20261024t104145z",
            analytics_physical_index="analytics_genre_trends_20261024t104145z",
            sql_test_result="FAIL",
            client=ok_es,  # type: ignore[arg-type]
        )


def test_query3_iso_week_sql_executes_in_spark_35_across_year_boundaries() -> None:
    pytest.importorskip("pyspark")
    spark = create_spark_sql_session(app_name="test-p3-iso-week-sql")
    try:
        sample_rows = [
            # 2015-12-31 (Thu) & 2016-01-01 (Fri) -> cùng tuần ISO 2015-W53 (Thứ 2 = 2015-12-28)
            {"batch_run_id": "r1", "batch_cutoff_time": "2019-10-01T00:00:00Z", "generated_at": "2026-11-27T00:00:00Z", "date": "2015-12-31", "genre": "Action", "interaction_count": 1, "rating_count": 1, "rating_sum": 4.0, "avg_rating": 4.0},
            {"batch_run_id": "r1", "batch_cutoff_time": "2019-10-01T00:00:00Z", "generated_at": "2026-11-27T00:00:00Z", "date": "2016-01-01", "genre": "Action", "interaction_count": 2, "rating_count": 2, "rating_sum": 10.0, "avg_rating": 5.0},
            # 2017-12-30 (Sat) & 2017-12-31 (Sun) -> 2017-W52 (Thứ 2 = 2017-12-25)
            {"batch_run_id": "r1", "batch_cutoff_time": "2019-10-01T00:00:00Z", "generated_at": "2026-11-27T00:00:00Z", "date": "2017-12-30", "genre": "Drama", "interaction_count": 2, "rating_count": 2, "rating_sum": 7.0, "avg_rating": 3.5},
            {"batch_run_id": "r1", "batch_cutoff_time": "2019-10-01T00:00:00Z", "generated_at": "2026-11-27T00:00:00Z", "date": "2017-12-31", "genre": "Action", "interaction_count": 2, "rating_count": 2, "rating_sum": 9.0, "avg_rating": 4.5},
            # 2018-12-31 (Mon) -> thuộc tuần ISO 2019-W01 (giao năm dương 2018 -> năm ISO 2019!)
            {"batch_run_id": "r1", "batch_cutoff_time": "2019-10-01T00:00:00Z", "generated_at": "2026-11-27T00:00:00Z", "date": "2018-12-31", "genre": "Comedy", "interaction_count": 3, "rating_count": 3, "rating_sum": 12.0, "avg_rating": 4.0},
        ]
        rows_to_dataframe(spark, "batch_genre", sample_rows).createOrReplaceTempView("batch_genre")
        out = spark.sql(QUERY3_ISO_WEEK_SQL_PATH.read_text(encoding="utf-8")).collect()
        by_key = {(r["period_start"].isoformat(), r["genre"]): r for r in out}

        w53 = by_key[("2015-12-28", "Action")]
        assert w53["iso_week_label"] == "2015-W53"
        assert w53["rating_count"] == 3
        assert w53["avg_rating"] == pytest.approx(14.0 / 3.0)

        assert by_key[("2017-12-25", "Action")]["iso_week_label"] == "2017-W52"
        assert by_key[("2017-12-25", "Action")]["rank_in_period"] == 1
        assert by_key[("2017-12-25", "Drama")]["rank_in_period"] == 2

        w01_2019 = by_key[("2018-12-31", "Comedy")]
        assert w01_2019["iso_week_label"] == "2019-W01"
    finally:
        spark.stop()
