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
from modules.elasticsearch.lanes.genre_batch.emit_ready_report import build_p3_ready_report
from modules.elasticsearch.lanes.genre_batch.load_source import load_genre_source, parse_genre_daily_tsv
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
        "reference": {"ref_version": "20261022-7c32dfb9"},
    }
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")

    tsv_file = tmp_path / "part-00000"
    tsv_file.write_text(
        "\n".join(
            [
                "2018-09-29\tAction\t2\t9.0\t9.0\t4.5\t20261024T104145Z\t2018-10-01T00:00:00Z\t2026-10-24T10:45:00Z",
                "2018-09-29\tUNKNOWN\t1\t1\t3.0\t3.0\t20261024T104145Z\t2018-10-01T00:00:00Z\t2026-10-24T10:45:00Z",
            ]
        ).replace("\t9.0\t9.0\t", "\t2\t9.0\t"),
        encoding="utf-8",
    )

    summary = load_genre_source(tsv_file, manifest_file, dry_run=True)
    assert summary["target_index"] == "genre_trends_20261024t104145z"
    assert summary["row_count"] == 2
    assert summary["sum_count"] == 3.0
    assert summary["unknown_rows"] == 1

    records = parse_genre_daily_tsv(
        tsv_file,
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
