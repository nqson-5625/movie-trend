"""Build analytics_timeseries và analytics_top_movies (P2.4) từ hai physical source của một Batch run.

Đọc movie_batch_stats_<run> / movie_daily_stats_<run> (đã nạp bằng load_batch_run), chạy SQL trong sql/,
kiểm tra tính nhất quán ngay trong Spark rồi ghi physical analytics <prefix><family>_<run>. Không đụng alias:
P5 publish sáu Batch aliases cùng lúc sau READY (DATA_CONTRACT_Phase2 mục 7.1).

    export PYSPARK_PYTHON=~/.venvs/movie-trend-spark/bin/python PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
    $PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.job --dry-run                  # tính + kiểm tra
    $PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.job                            # ghi analytics của run
    $PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.job --target-prefix dev_p2_    # ghi bản thử
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from modules.elasticsearch.common.config.es_client import ESClient, load_runtime_config
from modules.elasticsearch.lanes.movie_batch.load_fixtures import check_template
from modules.spark_sql.common.readers.es_reader import register_all_temp_views
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    validate_analytics_dataframe,
    validate_physical_target,
    write_analytics_df,
    write_analytics_df_distributed,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SQL_DIR = Path(__file__).resolve().parent / "sql"
REPORTS_DIR = PROJECT_ROOT / "data/local/phase2/es_reports"
DEFAULT_RUN_ID = "20261024T104145Z"
FAMILIES = ("analytics_timeseries", "analytics_top_movies")
SOURCE_VIEWS = {"batch_movie": "movie_batch_stats", "batch_movie_daily": "movie_daily_stats"}


def build_frames(spark: SparkSession, ref_version: str, manifest_hash: str) -> dict[str, DataFrame]:
    """Chạy SQL của lane trên temp view batch_movie / batch_movie_daily đã đăng ký."""
    spark.sql((SQL_DIR / "movie_period_stats.sql").read_text(encoding="utf-8")).createOrReplaceTempView(
        "movie_period_stats")
    params = {"ref_version": ref_version, "manifest_hash": manifest_hash}
    return {family: spark.sql((SQL_DIR / f"{family}.sql").read_text(encoding="utf-8"), args=params)
            for family in FAMILIES}


def with_common_columns(df: DataFrame, analytics_run_id: str, published_at: str) -> DataFrame:
    """Metadata bắt buộc của mọi analytics document (mục 6), giống runner dùng chung."""
    return (df.withColumn("schema_version", F.lit(load_runtime_config()["schema_version"]))
              .withColumn("analytics_run_id", F.lit(analytics_run_id))
              .withColumn("published_at", F.to_timestamp(F.lit(published_at))))


def check_frames(frames: dict[str, DataFrame], expected_total: int, expected_movies: int) -> dict[str, int]:
    """Đối soát trước khi ghi; sai ở đâu thì dừng, không ghi gì lên ES."""
    ts, top = frames["analytics_timeseries"], frames["analytics_top_movies"]
    errors = []
    all_time = ts.filter("entity_type = 'all' AND period_type = 'all'").collect()
    if len(all_time) != 1 or all_time[0]["period_start"] is not None:
        errors.append(f"cần đúng 1 dòng all/all có period_start NULL, có {len(all_time)}")
    elif (all_time[0]["rating_count"], all_time[0]["distinct_movie_count"]) != (expected_total, expected_movies):
        errors.append(f"dòng all/all = ({all_time[0]['rating_count']}, {all_time[0]['distinct_movie_count']}), "
                      f"source = ({expected_total}, {expected_movies})")
    for row in ts.filter("entity_type = 'movie'").groupBy("period_type").agg(
            F.sum("rating_count").alias("total")).collect():
        if row["total"] != expected_total:
            errors.append(f"tổng dòng movie kỳ {row['period_type']} = {row['total']}, source = {expected_total}")
    movie_by_period = (ts.filter("entity_type = 'movie'").groupBy("period_type", "period_start")
                         .agg(F.sum("rating_count").alias("movie_total"), F.count("*").alias("movies")))
    mismatched = (ts.filter("entity_type = 'all'").alias("a")
                    .join(movie_by_period.alias("m"),
                          F.col("a.period_type").eqNullSafe(F.col("m.period_type"))
                          & F.col("a.period_start").eqNullSafe(F.col("m.period_start")), "full_outer")
                    .filter("a.rating_count IS NULL OR m.movie_total IS NULL OR a.rating_count != m.movie_total "
                            "OR a.distinct_movie_count != m.movies").count())
    if mismatched:
        errors.append(f"{mismatched} kỳ có dòng all khác tổng các dòng movie")
    top_expected = ts.filter("entity_type = 'movie' AND period_type IN ('month', 'year', 'all')").count()
    top_count = top.count()
    if top_count != top_expected:
        errors.append(f"analytics_top_movies có {top_count} dòng, cần đủ mọi phim trong kỳ = {top_expected}")
    if errors:
        raise ValueError("Analytics movie_batch không nhất quán: " + "; ".join(errors))
    return {"analytics_timeseries": ts.count(), "analytics_top_movies": top_count}


def write_family(df: DataFrame, family: str, target: str, client: ESClient, io_mode: str) -> dict:
    validate_analytics_dataframe(df, family)
    target = validate_physical_target(target, family)
    check_template(client, target, set(df.columns))
    if io_mode == "distributed":
        write_analytics_df_distributed(df, target, family)
    else:
        write_analytics_df(df, target, ANALYTICS_KEY_COLUMNS[family], owner="p2", analytics_family=family)
    client.refresh(target)
    expected = df.count()
    resp = client.search(target, {"size": 0, "track_total_hits": True,
                                  "aggs": {"s": {"sum": {"field": "interaction_count"}}}})
    count = resp["hits"]["total"]["value"]
    if count != expected:
        raise RuntimeError(f"{target}: ES có {count} document, Spark tạo {expected} dòng")
    print(f"[OK] {target}: {count} document")
    return {"alias_name": family, "physical_index": target, "row_count": count, "duplicate_id_count": 0,
            "bulk_error_count": 0, "sum_count": float(resp["aggregations"]["s"]["value"])}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build analytics_timeseries + analytics_top_movies cho một Batch run")
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--source-report", type=Path,
                        help="mặc định data/local/phase2/es_reports/p2_source_<run_id>.json (do load_batch_run tạo)")
    parser.add_argument("--target-prefix", default="", choices=["", "dev_p2_"])
    parser.add_argument("--io-mode", default="distributed", choices=["distributed", "fixture"],
                        help="distributed cần SPARK_ES_PACKAGE; fixture đọc/ghi qua REST cho dữ liệu nhỏ")
    parser.add_argument("--dry-run", action="store_true", help="tính và kiểm tra, không ghi ES")
    args = parser.parse_args()

    source_report = args.source_report or REPORTS_DIR / f"p2_source_{args.run_id}.json"
    source = json.loads(source_report.read_text(encoding="utf-8"))
    if source["batch_run_id"] != args.run_id:
        raise ValueError(f"{source_report} thuộc run {source['batch_run_id']}, không phải {args.run_id}")
    indices = {e["alias_name"]: e for e in source["indices"]}
    run_tag = args.run_id.lower()

    client = ESClient(owner="p2")  # nạp .env trước khi tạo SparkSession (es.nodes, tài khoản ES)
    spark = create_spark_sql_session(app_name=f"p2-movie-batch-analytics-{run_tag}")
    try:
        register_all_temp_views(spark, views=list(SOURCE_VIEWS), mode=args.io_mode, index_overrides={
            view: indices[alias]["physical_index"] for view, alias in SOURCE_VIEWS.items()})
        published_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        frames = {family: with_common_columns(df, f"batch-{run_tag}", published_at).cache()
                  for family, df in build_frames(spark, source["ref_version"], source["manifest_sha256"]).items()}
        counts = check_frames(frames, int(indices["movie_daily_stats"]["sum_count"]),
                              indices["movie_batch_stats"]["row_count"])
        for family, n in counts.items():
            print(f"[OK] {family}: {n} dòng, đối soát với source khớp")
        if args.dry_run:
            return 0
        entries = [write_family(frames[family], family, f"{args.target_prefix}{family}_{run_tag}", client,
                                args.io_mode) for family in FAMILIES]
    finally:
        spark.stop()

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = REPORTS_DIR / f"{args.target_prefix}p2_analytics_{args.run_id}.json"
    report.write_text(json.dumps({
        "owner": "p2", "batch_run_id": args.run_id, "batch_cutoff_time": source["batch_cutoff_time"],
        "ref_version": source["ref_version"], "manifest_sha256": source["manifest_sha256"],
        "analytics_run_id": f"batch-{run_tag}", "published_at": published_at, "indices": entries,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[OK] Báo cáo: {report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
