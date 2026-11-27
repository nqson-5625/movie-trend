"""Spark SQL job & pure-Python reference implementation cho lane genre_batch (P3 - K2)."""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

LANE_DIR = Path(__file__).resolve().parent
ANALYTICS_SQL_PATH = LANE_DIR / "analytics_genre_trends.sql"
QUERY3_ISO_WEEK_SQL_PATH = LANE_DIR / "query3_genre_iso_week.sql"


def _period_start_for(period_type: str, day_str: str) -> str:
    if period_type == "day":
        return day_str
    if period_type == "month":
        return day_str[:7] + "-01"
    if period_type == "year":
        return day_str[:4] + "-01-01"
    if period_type == "iso_week":
        d = date.fromisoformat(day_str)
        monday = d - timedelta(days=d.weekday())
        return monday.isoformat()
    raise ValueError(f"period_type không hợp lệ: {period_type!r}")


def compute_genre_trends_rows(
    source_rows: Iterable[Dict[str, Any]],
    *,
    ref_version: str,
    manifest_hash: str,
    analytics_run_id: str,
    published_at: str,
    schema_version: str = "phase2-3.0",
    include_iso_week: bool = False,
) -> List[Dict[str, Any]]:
    """Pure-Python reference implementation khớp 100% logic analytics_genre_trends.sql (+ Query 3 iso_week)."""
    rows = list(source_rows)
    if not rows:
        return []

    period_types: Sequence[str] = ("day", "month", "year", "iso_week") if include_iso_week else ("day", "month", "year")
    output: List[Dict[str, Any]] = []

    for period_type in period_types:
        by_period: Dict[tuple[str, str, str], Dict[str, List[float]]] = defaultdict(lambda: defaultdict(lambda: [0, 0, 0.0]))
        for r in rows:
            p_start = _period_start_for(period_type, str(r["date"]))
            bucket = by_period[(r["batch_run_id"], r["batch_cutoff_time"], p_start)][r["genre"]]
            bucket[0] += int(r["interaction_count"])
            bucket[1] += int(r["rating_count"])
            bucket[2] += float(r["rating_sum"])

        for (run_id, cutoff, p_start), genres_map in sorted(by_period.items()):
            ranked = sorted(genres_map.items(), key=lambda kv: (-kv[1][0], kv[0]))
            for rank, (genre, (inter_cnt, rate_cnt, rate_sum)) in enumerate(ranked, start=1):
                doc: Dict[str, Any] = {
                    "schema_version": schema_version,
                    "analytics_run_id": analytics_run_id,
                    "published_at": published_at,
                    "batch_run_id": run_id,
                    "batch_cutoff_time": cutoff,
                    "ref_version": ref_version,
                    "manifest_hash": manifest_hash,
                    "period_type": period_type,
                    "period_start": p_start,
                    "genre": genre,
                    "interaction_count": inter_cnt,
                    "rating_count": rate_cnt,
                    "rating_sum": rate_sum,
                    "avg_rating": (rate_sum / rate_cnt) if rate_cnt > 0 else None,
                    "rank_in_period": rank,
                }
                if period_type == "iso_week":
                    iso_year, iso_week_num, _ = date.fromisoformat(p_start).isocalendar()
                    doc["iso_week_label"] = f"{iso_year}-W{iso_week_num:02d}"
                output.append(doc)
    return output


def run_spark_job(
    *,
    source_index: str,
    target_index: str,
    ref_version: str,
    manifest_hash: str,
    analytics_run_id: str,
    io_mode: str = "fixture",
) -> Dict[str, Any]:
    """Chạy Spark SQL từ batch_genre -> analytics_genre_trends physical index."""
    from pyspark.sql import functions as F

    from modules.elasticsearch.common.config.es_client import load_runtime_config
    from modules.spark_sql.common.readers.es_reader import register_temp_view
    from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
    from modules.spark_sql.common.writers.analytics_writer import (
        ANALYTICS_KEY_COLUMNS,
        validate_analytics_dataframe,
        validate_physical_target,
        write_analytics_df,
        write_analytics_df_distributed,
    )

    cfg = load_runtime_config()
    spark = create_spark_sql_session(app_name="movie-trend-p3-genre-batch")
    try:
        src_df = register_temp_view(spark, "batch_genre", index_name=source_index, mode=io_mode)
        src_count = src_df.count()
        sql_text = ANALYTICS_SQL_PATH.read_text(encoding="utf-8")
        published_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out_df = (
            spark.sql(sql_text)
            .withColumn("schema_version", F.lit(cfg["schema_version"]))
            .withColumn("analytics_run_id", F.lit(analytics_run_id))
            .withColumn("published_at", F.to_timestamp(F.lit(published_at)))
            .withColumn("ref_version", F.lit(ref_version))
            .withColumn("manifest_hash", F.lit(manifest_hash))
        )
        validate_analytics_dataframe(out_df, "analytics_genre_trends")
        clean_target = validate_physical_target(target_index, "analytics_genre_trends")

        if io_mode == "distributed":
            res = write_analytics_df_distributed(out_df, clean_target, "analytics_genre_trends")
            return {"source_rows": src_count, "target_index": res.index_name, "written_count": res.written_count, "mode": "distributed"}

        res = write_analytics_df(
            out_df,
            clean_target,
            ANALYTICS_KEY_COLUMNS["analytics_genre_trends"],
            owner="p3",
            refresh=True,
            analytics_family="analytics_genre_trends",
        )
        return {
            "source_rows": src_count,
            "target_index": res.index_name,
            "total_sent": res.total_sent,
            "succeeded_count": res.succeeded_count,
            "failed_count": res.failed_count,
            "mode": "fixture",
        }
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Chạy Spark SQL analytics_genre_trends (P3 - K2)")
    parser.add_argument("--source-index", required=True, help="Physical index nguồn (vd: genre_trends_<run_id>)")
    parser.add_argument("--target-index", required=True, help="Physical index đích (vd: analytics_genre_trends_<run_id>)")
    parser.add_argument("--ref-version", required=True)
    parser.add_argument("--manifest-hash", required=True)
    parser.add_argument("--analytics-run-id", required=True)
    parser.add_argument("--io-mode", choices=["fixture", "distributed"], default="fixture")
    args = parser.parse_args()

    result = run_spark_job(
        source_index=args.source_index,
        target_index=args.target_index,
        ref_version=args.ref_version,
        manifest_hash=args.manifest_hash,
        analytics_run_id=args.analytics_run_id,
        io_mode=args.io_mode,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
