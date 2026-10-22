"""
test_spark_streaming.py - Kiểm thử tự động Module 05 theo Mục 11 & 21.4 DATA_CONTRACT.md.
Đọc dữ liệu mẫu trực tiếp từ movie-trend/data/test_data/ và xuất kết quả ra movie-trend/data/test_data/.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DATA_DIR = PROJECT_ROOT / "data" / "test_data"

from ..src.streaming_job import create_spark_session, run_sample_mode
from ..src.transforms import (
    compute_genre_realtime,
    compute_movie_realtime_stats,
    compute_tmdb_realtime,
    load_canonical_reference,
    parse_and_validate_movie_events,
)


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session(app_name="test-module05-speed", master="local[2]")
    yield session
    session.stop()


def test_event_time_window_and_dedup(spark):
    """
    Kiểm tra:
    1. Cửa sổ thời gian phải tính theo event_time (1996), KHÔNG tính theo ingest_time (2026).
    2. Hai bản ghi trùng event_id phải được dedup còn 1.
    3. Bản ghi có rating > 5.0 phải bị đẩy sang quarantine.
    """
    events = [
        json.dumps({
            "schema_version": "1.0",
            "event_id": "a" * 64,
            "source": "movielens",
            "userId": 10,
            "movieId": 22,
            "rating": 4.0,
            "event_time": "1996-03-29T18:32:00Z",
            "ingest_time": "2026-10-22T10:00:00Z",
        }),
        json.dumps({
            "schema_version": "1.0",
            "event_id": "a" * 64,
            "source": "movielens",
            "userId": 10,
            "movieId": 22,
            "rating": 4.0,
            "event_time": "1996-03-29T18:32:00Z",
            "ingest_time": "2026-10-22T10:01:00Z",
        }),
        json.dumps({
            "schema_version": "1.0",
            "event_id": "b" * 64,
            "source": "movielens",
            "userId": 11,
            "movieId": 22,
            "rating": 5.0,
            "event_time": "1996-03-29T18:35:00Z",
            "ingest_time": "2026-10-22T11:00:00Z",
        }),
        json.dumps({
            "schema_version": "1.0",
            "event_id": "c" * 64,
            "source": "movielens",
            "userId": 12,
            "movieId": 22,
            "rating": 9.0,
            "event_time": "1996-03-29T18:36:00Z",
            "ingest_time": "2026-10-22T11:00:00Z",
        }),
    ]

    raw_df = spark.createDataFrame([(e,) for e in events], ["value"])
    valid_df, quarantine_df = parse_and_validate_movie_events(raw_df)

    assert valid_df.count() == 2
    assert quarantine_df.count() == 1

    stats_10m = compute_movie_realtime_stats(valid_df, "10m").collect()
    assert len(stats_10m) == 1
    row = stats_10m[0]
    assert row["movieId"] == 22
    assert row["window_start"] == "1996-03-29T18:30:00Z"
    assert row["window_end"] == "1996-03-29T18:40:00Z"
    assert row["window_size"] == "10m"
    assert row["interaction_count"] == 2
    assert row["rating_count"] == 2
    assert row["rating_sum"] == 9.0
    assert row["avg_rating"] == 4.5
    assert row["doc_id"] == "22|1996-03-29T18:30:00Z|10m"


def test_static_join_with_canonical_reference(spark):
    """
    Kiểm tra static join với data/test_data/movie-reference.sample.tsv:
    - movieId=22 có genres 'Crime|Drama|Horror|Mystery|Thriller' (5 genres).
    - movieId=999999 không có trong reference -> gán genre 'UNKNOWN' và join_miss_count == 1.
    """
    ref_path = str(TEST_DATA_DIR / "movie-reference.sample.tsv")
    ref_df = load_canonical_reference(spark, ref_path)

    events = [
        json.dumps({
            "schema_version": "1.0",
            "event_id": "d" * 64,
            "source": "movielens",
            "userId": 1,
            "movieId": 22,
            "rating": 4.0,
            "event_time": "1996-03-29T18:36:55Z",
            "ingest_time": "2026-10-08T11:29:28Z",
        }),
        json.dumps({
            "schema_version": "1.0",
            "event_id": "e" * 64,
            "source": "movielens",
            "userId": 2,
            "movieId": 999999,
            "rating": 3.0,
            "event_time": "1996-03-29T18:37:00Z",
            "ingest_time": "2026-10-08T11:29:28Z",
        }),
    ]
    raw_df = spark.createDataFrame([(e,) for e in events], ["value"])
    valid_df, _ = parse_and_validate_movie_events(raw_df)

    genre_df, miss_count = compute_genre_realtime(valid_df, ref_df, "10m")
    assert miss_count == 1

    genres_map = {r["genre"]: r["interaction_count"] for r in genre_df.collect()}
    for g in ("Crime", "Drama", "Horror", "Mystery", "Thriller", "UNKNOWN"):
        assert genres_map.get(g) == 1


def test_tmdb_realtime_delta(spark):
    """Kiểm tra tính prev_popularity và popularity_delta theo event_time."""
    snapshots = [
        json.dumps({
            "schema_version": "1.0",
            "event_id": "1" * 64,
            "source": "tmdb",
            "tmdbId": 550,
            "title": "Fight Club",
            "popularity": 100.0,
            "vote_count": 1000,
            "vote_average": 8.4,
            "event_time": "2026-10-08T10:00:00Z",
            "ingest_time": "2026-10-08T10:00:05Z",
        }),
        json.dumps({
            "schema_version": "1.0",
            "event_id": "2" * 64,
            "source": "tmdb",
            "tmdbId": 550,
            "title": "Fight Club",
            "popularity": 125.5,
            "vote_count": 1010,
            "vote_average": 8.4,
            "event_time": "2026-10-08T10:10:00Z",
            "ingest_time": "2026-10-08T10:10:05Z",
        }),
    ]
    raw_df = spark.createDataFrame([(s,) for s in snapshots], ["value"])
    tmdb_df, q_df = compute_tmdb_realtime(raw_df)

    assert q_df.count() == 0
    rows = tmdb_df.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["tmdbId"] == 550
    assert row["popularity"] == 125.5
    assert row["prev_popularity"] == 100.0
    assert row["popularity_delta"] == 25.5
    assert row["doc_id"] == "550"


def test_sample_mode_end_to_end(spark):
    """Đọc 3 file mẫu trong data/test_data/ và xuất 3 file kết quả ra data/test_data/."""
    stats = run_sample_mode(spark, TEST_DATA_DIR, es_url=None)
    assert stats["valid_movie_events"] == 20
    assert stats["movie_realtime_rows"] > 0
    assert stats["genre_realtime_rows"] > 0
    assert stats["tmdb_realtime_rows"] == 20
    assert (TEST_DATA_DIR / "movie_realtime_stats.jsonl").exists()
    assert (TEST_DATA_DIR / "genre_realtime.jsonl").exists()
    assert (TEST_DATA_DIR / "tmdb_realtime.jsonl").exists()
