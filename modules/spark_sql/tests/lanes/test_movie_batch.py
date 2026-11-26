"""SQL của lane movie_batch (P2.4) phải tái tạo đúng từng dòng fixture analytics từ fixture source.

Fixture (contracts/phase2/movie_batch/fixtures) được build_fixtures.py tính độc lập bằng Python, nên SQL khớp
fixture nghĩa là khớp contract mục 5, 6, 6.1. Chạy offline với Spark, không cần Elasticsearch:
    export PYSPARK_PYTHON=~/.venvs/movie-trend-spark/bin/python PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
    $PYSPARK_PYTHON -m pytest modules/spark_sql/tests/lanes -q
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from modules.elasticsearch.lanes.movie_batch.load_fixtures import build_records  # noqa: E402
from modules.spark_sql.common.readers.es_reader import rows_to_dataframe  # noqa: E402
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session  # noqa: E402
from modules.spark_sql.common.writers.analytics_writer import (  # noqa: E402
    ANALYTICS_KEY_COLUMNS,
    prepare_analytics_records,
    validate_analytics_dataframe,
)
from modules.spark_sql.lanes.movie_batch.job import (  # noqa: E402
    FAMILIES,
    build_frames,
    check_frames,
    with_common_columns,
)

FIXTURES = Path(__file__).resolve().parents[4] / "contracts/phase2/movie_batch/fixtures"


def fixture(name: str) -> list[dict]:
    return [json.loads(line) for line in (FIXTURES / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()]


@pytest.fixture(scope="module")
def spark():
    session = create_spark_sql_session(app_name="test-p2-movie-batch")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def frames(spark):
    rows_to_dataframe(spark, "batch_movie", fixture("movie_batch_stats")).createOrReplaceTempView("batch_movie")
    rows_to_dataframe(spark, "batch_movie_daily", fixture("movie_daily_stats")).createOrReplaceTempView(
        "batch_movie_daily")
    manifest_hash = hashlib.sha256((FIXTURES / "manifest.json").read_bytes()).hexdigest()
    ref_version = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))["reference"]["ref_version"]
    built = build_frames(spark, ref_version, manifest_hash)
    return {f: with_common_columns(df, "fixture-p2-v1-analytics", "2026-11-24T00:00:00Z").cache()
            for f, df in built.items()}


def canonical(rows: list[dict]) -> list[str]:
    return sorted(json.dumps(r, sort_keys=True) for r in rows)


@pytest.mark.parametrize("family", FAMILIES)
def test_sql_reproduces_fixture_rows_exactly(frames, family):
    records = prepare_analytics_records(frames[family], ANALYTICS_KEY_COLUMNS[family], family)
    actual = [{k: v for k, v in r.items() if k != "_id"} for r in records]
    assert canonical(actual) == canonical(fixture(family))


@pytest.mark.parametrize("family", FAMILIES)
def test_spark_ids_match_es_fixture_loader(frames, family):
    """_id do writer Spark tạo phải trùng _id lệnh nạp fixture tạo, để hai đường ghi không sinh hai bản."""
    spark_ids = {r["_id"] for r in prepare_analytics_records(frames[family], ANALYTICS_KEY_COLUMNS[family], family)}
    assert spark_ids == {r["_id"] for r in build_records(family, fixture(family))}


@pytest.mark.parametrize("family", FAMILIES)
def test_output_columns_and_types_follow_contract(frames, family):
    df = frames[family]
    validate_analytics_dataframe(df, family)
    types = {f.name: type(f.dataType) for f in df.schema.fields}
    assert types["period_start"] is T.DateType and types["batch_cutoff_time"] is T.TimestampType
    assert types["movieId"] is T.IntegerType and types["interaction_count"] is T.LongType
    assert types["avg_rating"] is T.DoubleType
    if family == "analytics_timeseries":
        assert types["distinct_movie_count"] is T.LongType
    else:
        assert types["rank_in_period"] is T.IntegerType


def test_check_frames_accepts_consistent_output(frames):
    assert check_frames(frames, expected_total=12, expected_movies=4) == {
        "analytics_timeseries": 36, "analytics_top_movies": 18}


def test_check_frames_rejects_total_different_from_source(frames):
    with pytest.raises(ValueError, match="dòng all/all"):
        check_frames(frames, expected_total=13, expected_movies=4)


def test_check_frames_rejects_missing_movie_row(frames):
    broken = dict(frames)
    broken["analytics_timeseries"] = frames["analytics_timeseries"].filter(
        "NOT (entity_type = 'movie' AND period_type = 'month' AND movieId = 99)")
    with pytest.raises(ValueError, match="kỳ month|khác tổng"):
        check_frames(broken, expected_total=12, expected_movies=4)


def test_check_frames_rejects_incomplete_top(frames):
    broken = dict(frames)
    broken["analytics_top_movies"] = frames["analytics_top_movies"].filter("rank_in_period <= 2")
    with pytest.raises(ValueError, match="đủ mọi phim"):
        check_frames(broken, expected_total=12, expected_movies=4)
