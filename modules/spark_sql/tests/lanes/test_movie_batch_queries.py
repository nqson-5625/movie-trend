"""Query 1, 2, 4, 5, 6 của lane movie_batch (P2.5) trên fixture K0, so với kết quả tự tính bằng Python thuần.

Fixture: 4 phim, rating các ngày 2017-12-30, 2017-12-31, 2018-01-05, 2018-01-06; T = 2018-03-01T00:00:00Z.
    export PYSPARK_PYTHON=~/.venvs/movie-trend-spark/bin/python PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
    $PYSPARK_PYTHON -m pytest modules/spark_sql/tests/lanes -q
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from modules.spark_sql.common.readers.es_reader import rows_to_dataframe  # noqa: E402
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session  # noqa: E402
from modules.spark_sql.lanes.movie_batch.queries import (  # noqa: E402
    movie_window_growth,
    q01_top_movies,
    q02_movie_avg_rating,
    q04_end,
    q04_trend_7d,
    q05_end,
    q05_trend_30d,
    q06_end,
    q06_wow,
    register_analytics,
    window_bounds,
)

FIXTURES = Path(__file__).resolve().parents[4] / "contracts/phase2/movie_batch/fixtures"


def fixture(name: str) -> list[dict]:
    return [json.loads(line) for line in (FIXTURES / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()]


@pytest.fixture(scope="module")
def spark():
    session = create_spark_sql_session(app_name="test-p2-movie-batch-queries")
    rows_to_dataframe(session, "batch_movie", fixture("movie_batch_stats")).createOrReplaceTempView("batch_movie")
    rows_to_dataframe(session, "batch_movie_daily", fixture("movie_daily_stats")).createOrReplaceTempView(
        "batch_movie_daily")
    manifest = FIXTURES / "manifest.json"
    register_analytics(session, json.loads(manifest.read_text(encoding="utf-8"))["reference"]["ref_version"],
                       hashlib.sha256(manifest.read_bytes()).hexdigest())
    yield session
    session.stop()


def rows(df) -> list[dict]:
    return [r.asDict() for r in df.collect()]


def expected_growth(current_end: date, days: int) -> dict[int, tuple[int, int, float]]:
    """movieId -> (previous, current, growth), cộng thẳng từ fixture movie_daily_stats."""
    current_start, previous_start = current_end - timedelta(days=days), current_end - timedelta(days=2 * days)
    acc = defaultdict(lambda: [0, 0])
    for r in fixture("movie_daily_stats"):
        d = date.fromisoformat(r["date"])
        if previous_start <= d < current_start:
            acc[r["movieId"]][0] += r["interaction_count"]
        elif current_start <= d < current_end:
            acc[r["movieId"]][1] += r["interaction_count"]
    return {m: (p, c, (c - p) / (p + 1)) for m, (p, c) in acc.items()}


# ---------------- Query 1: Top phim ----------------

@pytest.mark.parametrize("period_type, period_start", [("all", None), ("year", "2018-01-01"), ("month", "2017-12-01")])
def test_q01_equals_top_movies_of_exactly_one_period(spark, period_type, period_start):
    got = rows(q01_top_movies(spark, period_type, period_start, top_n=10))
    expected = sorted((r for r in fixture("analytics_top_movies")
                       if r["period_type"] == period_type and r["period_start"] == period_start),
                      key=lambda r: r["rank_in_period"])
    assert [(r["rank_in_period"], r["movieId"], r["interaction_count"]) for r in got] == \
           [(r["rank_in_period"], r["movieId"], r["interaction_count"]) for r in expected]


def test_q01_tie_breaks_by_movie_id_and_honours_top_n(spark):
    got = rows(q01_top_movies(spark, "all", None, top_n=2))
    assert [(r["movieId"], r["interaction_count"]) for r in got] == [(1, 4), (2, 4)]  # hòa 4 lượt -> movieId tăng


@pytest.mark.parametrize("period_type, period_start", [("day", "2018-01-05"), ("month", "2018-01-15"),
                                                       ("year", None), ("all", "2018-01-01")])
def test_q01_rejects_mixed_or_invalid_period(spark, period_type, period_start):
    with pytest.raises(ValueError):
        q01_top_movies(spark, period_type, period_start)


# ---------------- Query 2: Rating trung bình theo phim ----------------

def test_q02_is_weighted_not_average_of_averages(spark):
    """Phim 1 tháng 12/2017: ngày 30 có 2 rating tổng 7.0 (TB 3.5), ngày 31 có 1 rating 4.0 (TB 4.0).
    Đúng là 11/3 ≈ 3.667, không phải (3.5 + 4.0) / 2 = 3.75."""
    got = rows(q02_movie_avg_rating(spark, "month", "2017-12-01", movie_ids=[1]))
    assert len(got) == 1 and got[0]["rating_count"] == 3
    assert got[0]["avg_rating"] == pytest.approx(11 / 3) and got[0]["avg_rating"] != pytest.approx(3.75)


def test_q02_all_time_matches_movie_batch_stats(spark):
    got = {r["movieId"]: r for r in rows(q02_movie_avg_rating(spark, "all", None))}
    for movie in fixture("movie_batch_stats"):
        assert got[movie["movieId"]]["rating_count"] == movie["rating_count"]
        assert got[movie["movieId"]]["avg_rating"] == pytest.approx(movie["avg_rating"])
    assert got[99]["title"] is None  # phim thiếu title vẫn có movieId


def test_q02_min_ratings_threshold(spark):
    assert {r["movieId"] for r in rows(q02_movie_avg_rating(spark, "all", None, min_ratings=4))} == {1, 2}


# ---------------- Query 4, 5, 6: tăng trưởng ----------------

def check_growth(df, current_end: date, days: int):
    got = {r["movieId"]: r for r in rows(df)}
    expected = expected_growth(current_end, days)
    assert set(got) == set(expected)
    for movie, (prev, cur, growth) in expected.items():
        r = got[movie]
        assert (r["previous_count"], r["current_count"], r["coverage_status"]) == (prev, cur, "verified")
        assert r["growth"] == pytest.approx(growth)
        assert r["current_end"] - r["current_start"] == r["current_start"] - r["previous_start"] \
               == timedelta(days=days), "hai khoảng phải bằng nhau và liền kề"
    return got


def test_q04_seven_days_half_open_windows(spark):
    got = check_growth(q04_trend_7d(spark, date(2018, 1, 13)), date(2018, 1, 13), 7)
    # current = [2018-01-06, 2018-01-13): ngày 06 (đầu khoảng) thuộc current; previous = [2017-12-30, 2018-01-06)
    assert (got[2]["previous_count"], got[2]["current_count"]) == (3, 1)
    assert got[2]["growth"] == pytest.approx((1 - 3) / (3 + 1))


def test_end_day_is_excluded_from_current_window(spark):
    """Khoảng 3 ngày kết thúc 2018-01-05: previous [12-30, 01-02), current [01-02, 01-05). Rating ngày 05 không được tính."""
    got = {r["movieId"]: r for r in rows(movie_window_growth(spark, date(2018, 1, 5), 3))}
    assert 99 not in got, "phim 99 chỉ có rating đúng ngày 2018-01-05 = mốc kết thúc"
    assert got[1]["current_count"] == 0 and got[1]["previous_count"] == 3
    assert got[1]["coverage_status"] == "verified"


def test_window_starting_before_first_rating_is_unverified(spark):
    bounds = rows(window_bounds(spark, *q04_end(date(2018, 1, 6))))
    assert len(bounds) == 1 and bounds[0]["coverage_status"] == "unverified"
    assert all(r["growth"] is None for r in rows(q04_trend_7d(spark, date(2018, 1, 6))))


def test_q05_thirty_consecutive_days_not_calendar_month(spark):
    got = check_growth(q05_trend_30d(spark, date(2018, 3, 1)), date(2018, 3, 1), 30)
    assert got[1]["previous_start"] == date(2017, 12, 31)  # 30 ngày, không phải đầu tháng
    assert all(r["current_count"] == 0 for r in got.values())


def test_q06_uses_two_full_iso_weeks_starting_monday(spark):
    got = check_growth(q06_wow(spark, date(2018, 1, 17)), date(2018, 1, 15), 7)  # 17/01 là thứ Tư -> tuần trọn trước
    some = next(iter(got.values()))
    assert some["previous_start"].weekday() == some["current_start"].weekday() == some["current_end"].weekday() == 0
    assert (some["previous_start"], some["current_end"]) == (date(2018, 1, 1), date(2018, 1, 15))


def test_q06_on_a_monday_keeps_that_monday_as_end(spark):
    df = q06_wow(spark, date(2018, 1, 15))
    assert {(r["current_start"], r["current_end"]) for r in rows(df)} == {(date(2018, 1, 8), date(2018, 1, 15))}


@pytest.mark.parametrize("end_of, query, as_of", [(q04_end, q04_trend_7d, date(2018, 3, 8)),
                                                  (q05_end, q05_trend_30d, date(2018, 3, 15)),
                                                  (q06_end, q06_wow, date(2018, 3, 14))])
def test_windows_past_cutoff_are_unverified_with_null_metrics(spark, end_of, query, as_of):
    """Mốc kết thúc sau T: Batch không phủ, phải báo unverified kể cả khi hai khoảng không có rating nào."""
    bounds = rows(window_bounds(spark, *end_of(as_of)))
    assert len(bounds) == 1 and bounds[0]["coverage_status"] == "unverified"
    assert all(r["coverage_status"] == "unverified" and r["growth"] is None and r["current_count"] is None
               for r in rows(query(spark, as_of)))


def test_window_ending_exactly_at_cutoff_is_verified(spark):
    bounds = rows(window_bounds(spark, *q05_end(date(2018, 3, 1))))
    assert bounds[0]["coverage_status"] == "verified", "end <= T là hợp lệ"
