"""Unit tests & live smoke tests cho ESClient và health_check."""

from __future__ import annotations

import math
import os

import pytest

from modules.elasticsearch.common.config.es_client import (
    BulkOperationError,
    ESClient,
    validate_index_name,
)
from modules.elasticsearch.common.config.health_check import run_health_check


@pytest.mark.parametrize("name", ["tmdb_realtime", "movie_batch_stats_20261024t104145z", "dev_p5_smoke"])
def test_validate_index_name_ok(name: str) -> None:
    assert validate_index_name(name) == name


@pytest.mark.parametrize("bad", ["", "../x", "a/b", "UpperCase", "_system", "co dau cach"])
def test_validate_index_name_rejects(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_index_name(bad)


def test_owner_permissions_and_nan_guard() -> None:
    p1 = ESClient(owner="p1")
    with pytest.raises(PermissionError):
        p1.bulk_upsert("analytics_tmdb_signal", [{"_id": "1", "tmdbId": 1}])

    p2 = ESClient(owner="p2")
    with pytest.raises(PermissionError):
        p2.bulk_upsert("dev_p3_genre_trends_test", [{"_id": "1"}])
    with pytest.raises(PermissionError):
        p2.bulk_upsert("tmdb_realtime", [{"_id": "1"}])

    p5 = ESClient(owner="p5")
    with pytest.raises(PermissionError):
        p5.delete_index("tmdb_realtime")  # Không được xóa canonical
    with pytest.raises(ValueError):
        p5.bulk_upsert("dev_p5_tmdb_realtime_nan", [{"_id": "1", "popularity": math.nan}])


@pytest.mark.skipif(os.environ.get("ES_LIVE_TEST") != "1", reason="Đặt ES_LIVE_TEST=1 để chạy test live")
def test_live_health_bulk_and_pit() -> None:
    report = run_health_check(ESClient(owner="admin"))
    assert report["ok"] is True
    assert report["status"] == "green"
    assert report["missing_components"] == []

    p5 = ESClient(owner="p5")
    idx = "dev_p5_tmdb_realtime_pytest"
    good = {
        "_id": "550", "tmdbId": 550, "event_time": "2026-11-12T06:02:43Z",
        "popularity": 82.5, "vote_count": 100, "vote_average": 8.4,
        "prev_popularity": None, "popularity_delta": None, "updated_at": "2026-11-12T06:03:00Z",
    }
    bad = {**good, "_id": "bad_1", "tmdbId": "not_an_int"}

    try:
        p5.delete_index(idx)
        with pytest.raises(BulkOperationError) as exc:
            p5.bulk_upsert(idx, [good, bad], refresh=True)
        assert exc.value.result.failed_count == 1

        docs = [{**good, "_id": str(1000 + i), "tmdbId": 1000 + i} for i in range(25)]
        p5.bulk_upsert(idx, docs, refresh=True)
        fetched = p5.search_all_pit(idx, page_size=7)
        assert len(fetched) == 26
    finally:
        p5.delete_index(idx)
