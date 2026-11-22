"""Unit tests & live tests cho ES reader (6 Temp Views) và analytics writer."""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import pytest

from modules.elasticsearch.common.config.es_client import BulkResult, ESClient
from modules.spark_sql.common.readers.es_reader import (
    SOURCE_SCHEMAS,
    _parse_date,
    read_es_source_df,
    register_temp_view,
    rows_to_dataframe,
)
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    prepare_analytics_records,
    write_analytics_df,
)
from modules.spark_sql.common.writers.key_serializer import serialize_key_v1


class _FakeESClient:
    def __init__(self, docs_by_index: Dict[str, List[Dict[str, Any]]], owner: str = "p5"):
        self.docs_by_index = docs_by_index
        self.owner = owner
        self.bulk_calls: List[tuple[str, List[Dict[str, Any]]]] = []

    def search_all_pit(self, index_or_alias: str, **kwargs) -> List[Dict[str, Any]]:
        return list(self.docs_by_index.get(index_or_alias, []))

    def bulk_upsert(self, index_name: str, records, **kwargs) -> BulkResult:
        rec_list = list(records)
        self.bulk_calls.append((index_name, rec_list))
        return BulkResult(index_name, len(rec_list), len(rec_list), 0, [])


@pytest.fixture(scope="module")
def spark():
    session = create_spark_sql_session(app_name="test-reader-writer")
    yield session
    session.stop()


def test_empty_index_and_all_null_tmdb_columns_keep_exact_schema(spark) -> None:
    empty_df = rows_to_dataframe(spark, "batch_movie_daily", [])
    assert empty_df.count() == 0
    assert empty_df.schema == SOURCE_SCHEMAS["batch_movie_daily"]

    tmdb_rows = [
        {
            "tmdbId": 550,
            "event_time": "2026-11-12T06:02:43Z",
            "popularity": 82.45,
            "vote_count": 29500,
            "vote_average": 8.4,
            "prev_popularity": None,
            "popularity_delta": None,
            "updated_at": "2026-11-12T06:03:00Z",
        }
    ]
    tmdb_df = rows_to_dataframe(spark, "tmdb_rt", tmdb_rows)
    assert tmdb_df.count() == 1
    assert tmdb_df.schema == SOURCE_SCHEMAS["tmdb_rt"]


def test_parse_date_strictly_rejects_bad_suffix_and_non_midnight() -> None:
    assert _parse_date("2017-05-03") == date(2017, 5, 3)
    assert _parse_date("2017-05-03T00:00:00Z") == date(2017, 5, 3)

    with pytest.raises(ValueError):
        _parse_date("2017-05-03_garbage")
    with pytest.raises(ValueError):
        _parse_date("2017-05-03T12:00:00Z")


def test_utc_timestamp_roundtrip_never_shifts_by_os_timezone(spark) -> None:
    fake_es = _FakeESClient(
        {
            "movie_realtime_stats": [
                {
                    "window_start": "2017-05-03T21:00:00Z",
                    "window_end": "2017-05-03T22:00:00Z",
                    "window_size": "1h",
                    "movieId": 356,
                    "interaction_count": 5,
                    "rating_count": 5,
                    "rating_sum": 22.5,
                    "avg_rating": 4.5,
                    "updated_at": "2026-11-12T06:00:00Z",
                }
            ]
        },
        owner="p4",
    )
    register_temp_view(spark, "rt_movie", client=fake_es)  # type: ignore[arg-type]

    out_df = spark.sql("""
        SELECT
            window_start,
            window_end,
            window_size,
            'movie' AS entity_type,
            CAST(movieId AS STRING) AS entity_key
        FROM rt_movie
    """)
    records = prepare_analytics_records(out_df, ANALYTICS_KEY_COLUMNS["analytics_growth"])
    assert len(records) == 1
    assert records[0]["window_start"] == "2017-05-03T21:00:00Z"
    assert records[0]["window_end"] == "2017-05-03T22:00:00Z"

    expected_id = serialize_key_v1(
        ("1h", datetime(2017, 5, 3, 21, 0, 0, tzinfo=timezone.utc), "movie", "356")
    )
    assert records[0]["_id"] == expected_id


def test_prepare_analytics_records_rejects_duplicate_keys_and_write_is_idempotent(spark) -> None:
    dup_df = spark.sql("""
        SELECT 550 AS tmdbId, TIMESTAMP '2026-11-12T06:02:43Z' AS event_time
        UNION ALL
        SELECT 550 AS tmdbId, TIMESTAMP '2026-11-12T07:02:43Z' AS event_time
    """)
    with pytest.raises(ValueError, match="trùng khóa"):
        prepare_analytics_records(dup_df, ("tmdbId",))

    fake_es = _FakeESClient({}, owner="p5")
    unique_df = spark.sql("""
        SELECT
            550 AS tmdbId,
            TIMESTAMP '2026-11-12T06:02:43Z' AS event_time,
            82.5 AS popularity,
            CAST(29500 AS BIGINT) AS vote_count,
            8.4 AS vote_average,
            CAST(NULL AS DOUBLE) AS prev_popularity,
            CAST(NULL AS DOUBLE) AS popularity_delta,
            TIMESTAMP '2026-11-12T06:03:00Z' AS updated_at,
            CAST(NULL AS STRING) AS title,
            'tmdb' AS source
    """)
    write_analytics_df(
        unique_df, "dev_p5_analytics_tmdb_signal_test", ("tmdbId",), owner="p5", client=fake_es  # type: ignore[arg-type]
    )
    write_analytics_df(
        unique_df, "dev_p5_analytics_tmdb_signal_test", ("tmdbId",), owner="p5", client=fake_es  # type: ignore[arg-type]
    )
    assert len(fake_es.bulk_calls) == 2
    assert fake_es.bulk_calls[0][1][0]["_id"] == fake_es.bulk_calls[1][1][0]["_id"]


@pytest.mark.skipif(os.environ.get("ES_LIVE_TEST") != "1", reason="Đặt ES_LIVE_TEST=1 để chạy test live")
def test_live_es_to_spark_sql_to_es_analytics_roundtrip(spark) -> None:
    p5 = ESClient(owner="p5")
    src_idx = "dev_p5_tmdb_realtime_2b_test"
    out_idx = "dev_p5_analytics_tmdb_signal_2b_test"
    sample_docs = [
        {
            "_id": "550",
            "tmdbId": 550,
            "event_time": "2026-11-12T06:02:43Z",
            "popularity": 82.45,
            "vote_count": 29500,
            "vote_average": 8.4,
            "prev_popularity": None,
            "popularity_delta": None,
            "updated_at": "2026-11-12T06:03:00Z",
        },
        {
            "_id": "603",
            "tmdbId": 603,
            "event_time": "2026-11-12T06:02:43Z",
            "popularity": 75.1,
            "vote_count": 25000,
            "vote_average": 8.2,
            "prev_popularity": 70.0,
            "popularity_delta": 5.1,
            "updated_at": "2026-11-12T06:03:00Z",
        },
    ]

    try:
        p5.delete_index(src_idx)
        p5.delete_index(out_idx)
        p5.bulk_upsert(src_idx, sample_docs, refresh=True)

        register_temp_view(spark, "tmdb_rt", index_name=src_idx, client=p5)
        analytics_df = spark.sql("""
            SELECT
                tmdbId, event_time, popularity, vote_count, vote_average,
                prev_popularity, popularity_delta, updated_at,
                CAST(NULL AS STRING) AS title,
                'tmdb' AS source
            FROM tmdb_rt
        """)

        # Ghi 2 lần liên tiếp để chứng minh idempotent (count giữ nguyên = 2)
        res1 = write_analytics_df(
            analytics_df, out_idx, ANALYTICS_KEY_COLUMNS["analytics_tmdb_signal"],
            owner="p5", client=p5, refresh=True,
        )
        res2 = write_analytics_df(
            analytics_df, out_idx, ANALYTICS_KEY_COLUMNS["analytics_tmdb_signal"],
            owner="p5", client=p5, refresh=True,
        )
        assert res1.succeeded_count == 2
        assert res2.succeeded_count == 2
        assert p5.count(out_idx) == 2

        read_back = p5.search_all_pit(out_idx)
        assert {d["tmdbId"] for d in read_back} == {550, 603}
        assert all(d["source"] == "tmdb" and d["title"] is None for d in read_back)
    finally:
        p5.delete_index(src_idx)
        p5.delete_index(out_idx)
