"""Năm truy vấn nghiệp vụ bắt buộc của lane movie_batch (P2.5; DATA_CONTRACT_Phase2 mục 8.1): query 1, 2, 4, 5, 6.

Truy vấn chạy trên temp view batch_movie / batch_movie_daily của một run và hai analytics do job.build_frames() tạo,
nên luôn khớp với dữ liệu D1/D2. Kết quả không ghi thành index (chỉ sáu analytics family phải materialize).
Mốc mặc định của query 4-6 là T của run (timeline dữ liệu nguồn), có thể truyền --as-of khác để demo.

    export PYSPARK_PYTHON=~/.venvs/movie-trend-spark/bin/python PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
    $PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.queries q01 --period-type year --period-start 2015-01-01
    $PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.queries q02 --movie-id 1 --movie-id 296
    $PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.queries q04 --as-of 2018-09-01
    $PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.queries q06 --as-of 2018-09-01
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from modules.spark_sql.lanes.movie_batch.job import (
    REPORTS_DIR,
    SOURCE_VIEWS,
    TIMESERIES_PERIODS,
    TOP_PERIODS,
    build_frames,
)

QUERY_DIR = Path(__file__).resolve().parent / "sql" / "queries"


def _sql(name: str) -> str:
    return (QUERY_DIR / f"{name}.sql").read_text(encoding="utf-8")


def register_analytics(spark: SparkSession, ref_version: str, manifest_hash: str) -> None:
    """Tạo analytics_timeseries / analytics_top_movies từ batch_movie(_daily) đã đăng ký, bằng đúng SQL của job."""
    for family, df in build_frames(spark, ref_version, manifest_hash).items():
        df.createOrReplaceTempView(family)


def period_start_of(period_type: str, value: str | None) -> str | None:
    """Kiểm tra period_start là đầu kỳ UTC (mục 6.1); kỳ all không có period_start."""
    if period_type == "all":
        if value:
            raise ValueError("period_type=all không có period_start")
        return None
    if not value:
        raise ValueError(f"period_type={period_type} cần --period-start (đầu kỳ)")
    start = date.fromisoformat(value)
    if (period_type == "month" and start.day != 1) or (period_type == "year" and (start.month, start.day) != (1, 1)):
        raise ValueError(f"{value} không phải đầu kỳ {period_type}")
    return start.isoformat()


def q01_top_movies(spark: SparkSession, period_type: str = "all", period_start: str | None = None,
                   top_n: int = 10) -> DataFrame:
    if period_type not in TOP_PERIODS:
        raise ValueError(f"Top phim chỉ có kỳ {TOP_PERIODS}")
    return spark.sql(_sql("q01_top_movies"), args={
        "period_type": period_type, "period_start": period_start_of(period_type, period_start), "top_n": top_n})


def q02_movie_avg_rating(spark: SparkSession, period_type: str = "all", period_start: str | None = None,
                         movie_ids: list[int] | None = None, min_ratings: int = 1) -> DataFrame:
    if period_type not in TIMESERIES_PERIODS:
        raise ValueError(f"period_type phải thuộc {TIMESERIES_PERIODS}")
    df = spark.sql(_sql("q02_movie_avg_rating"), args={
        "period_type": period_type, "period_start": period_start_of(period_type, period_start),
        "min_ratings": min_ratings})
    return df.filter(F.col("movieId").isin(movie_ids)) if movie_ids else df


def window_bounds(spark: SparkSession, current_end: date, days: int) -> DataFrame:
    """Một dòng: hai khoảng so sánh và coverage_status; đăng ký thành view window_bounds cho query tăng trưởng."""
    df = spark.sql(_sql("window_bounds"), args={"current_end": current_end.isoformat(), "days": days})
    df.createOrReplaceTempView("window_bounds")
    return df


def movie_window_growth(spark: SparkSession, current_end: date, days: int) -> DataFrame:
    window_bounds(spark, current_end, days)
    return spark.sql(_sql("movie_window_growth"))


def q04_end(as_of: date) -> tuple[date, int]:
    """7 ngày [as_of-7, as_of) so với 7 ngày liền trước."""
    return as_of, 7


def q05_end(as_of: date) -> tuple[date, int]:
    """30 ngày liên tiếp [as_of-30, as_of) so với 30 ngày liền trước; không thay bằng tháng lịch."""
    return as_of, 30


def q06_end(as_of: date) -> tuple[date, int]:
    """Tuần ISO trọn vẹn gần nhất kết thúc trước hoặc đúng as_of (thứ Hai 00:00Z) so với tuần ISO liền trước."""
    return as_of - timedelta(days=as_of.weekday()), 7


def q04_trend_7d(spark: SparkSession, as_of: date) -> DataFrame:
    return movie_window_growth(spark, *q04_end(as_of))


def q05_trend_30d(spark: SparkSession, as_of: date) -> DataFrame:
    return movie_window_growth(spark, *q05_end(as_of))


def q06_wow(spark: SparkSession, as_of: date) -> DataFrame:
    return movie_window_growth(spark, *q06_end(as_of))


def main() -> int:
    parser = argparse.ArgumentParser(description="Chạy query 1, 2, 4, 5, 6 của lane movie_batch trên một Batch run")
    parser.add_argument("query", choices=["q01", "q02", "q04", "q05", "q06"])
    parser.add_argument("--run-id", default="20261024T104145Z")
    parser.add_argument("--period-type", default="all")
    parser.add_argument("--period-start")
    parser.add_argument("--movie-id", type=int, action="append", help="q02: lọc phim, lặp lại được")
    parser.add_argument("--as-of", help="q04-q06: mốc kết thúc YYYY-MM-DD, mặc định ngày của T")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--io-mode", default="fixture", choices=["fixture", "distributed"])
    args = parser.parse_args()

    from modules.elasticsearch.common.config.es_client import ESClient
    from modules.spark_sql.common.readers.es_reader import register_all_temp_views
    from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session

    source = json.loads((REPORTS_DIR / f"p2_source_{args.run_id}.json").read_text(encoding="utf-8"))
    indices = {e["alias_name"]: e["physical_index"] for e in source["indices"]}
    ESClient(owner="p1")  # nạp .env trước khi tạo SparkSession
    spark = create_spark_sql_session(app_name=f"p2-movie-batch-{args.query}")
    try:
        register_all_temp_views(spark, views=list(SOURCE_VIEWS), mode=args.io_mode,
                                index_overrides={view: indices[alias] for view, alias in SOURCE_VIEWS.items()})
        if args.query in ("q01", "q02"):
            register_analytics(spark, source["ref_version"], source["manifest_sha256"])
        as_of = date.fromisoformat(args.as_of or source["batch_cutoff_time"][:10])
        if args.query == "q01":
            result = q01_top_movies(spark, args.period_type, args.period_start, args.limit)
        elif args.query == "q02":
            result = q02_movie_avg_rating(spark, args.period_type, args.period_start, args.movie_id)
        else:
            end, days = {"q04": q04_end, "q05": q05_end, "q06": q06_end}[args.query](as_of)
            window_bounds(spark, end, days).show(truncate=False)
            result = movie_window_growth(spark, end, days)
        result.show(args.limit, truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
