"""Operational entrypoint for the shared Spark SQL runtime (no lane metrics)."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from modules.elasticsearch.common.config.es_client import load_runtime_config
from modules.elasticsearch.common.config.runtime_contract import validate_runtime_contract


def main() -> int:
    parser = argparse.ArgumentParser(description="Movie Trend shared Spark SQL runtime")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate", help="Validate executable Phase 2 inventory")
    sub.add_parser("smoke", help="Start Spark in UTC and report versions")
    query = sub.add_parser("run-query", help="Run owner SQL using shared readers/writers")
    query.add_argument(
        "--view",
        action="append",
        required=True,
        help="Temp view binding in the form temp_view=index_or_alias; repeat as needed",
    )
    query.add_argument("--query-file", required=True)
    query.add_argument("--analytics-family", required=True)
    query.add_argument("--target-index", required=True)
    query.add_argument("--analytics-run-id", required=True)
    query.add_argument("--owner", required=True, choices=["p2", "p3", "p4", "p5"])
    query.add_argument("--io-mode", choices=["fixture", "distributed"], default="distributed")
    args = parser.parse_args()

    if args.command == "validate":
        print(json.dumps(validate_runtime_contract(), ensure_ascii=False, indent=2))
        return 0

    from pyspark.sql import functions as F

    from modules.spark_sql.common.readers.es_reader import register_temp_view
    from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
    from modules.spark_sql.common.writers.analytics_writer import (
        ANALYTICS_KEY_COLUMNS,
        validate_analytics_dataframe,
        validate_physical_target,
        write_analytics_df,
        write_analytics_df_distributed,
    )

    spark = create_spark_sql_session(app_name=f"movie-trend-spark-sql-{args.command}")
    try:
        cfg = load_runtime_config()
        if args.command == "run-query":
            bindings: dict[str, str] = {}
            for item in args.view:
                if "=" not in item:
                    raise ValueError(f"--view phải có dạng temp_view=index: {item!r}")
                view_name, index_name = item.split("=", 1)
                if not view_name or not index_name or view_name in bindings:
                    raise ValueError(f"--view không hợp lệ hoặc bị trùng: {item!r}")
                bindings[view_name] = index_name
            for view_name, index_name in bindings.items():
                register_temp_view(
                    spark,
                    view_name,
                    index_name=index_name,
                    mode=args.io_mode,
                )
            sql_text = Path(args.query_file).read_text(encoding="utf-8")
            output = spark.sql(sql_text)
            published_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            output = (
                output.withColumn("schema_version", F.lit(cfg["schema_version"]))
                .withColumn("analytics_run_id", F.lit(args.analytics_run_id))
                .withColumn("published_at", F.to_timestamp(F.lit(published_at)))
            )
            validate_analytics_dataframe(output, args.analytics_family)
            target = validate_physical_target(args.target_index, args.analytics_family)
            if args.io_mode == "distributed":
                result = write_analytics_df_distributed(output, target, args.analytics_family)
                payload = result.__dict__
            else:
                result = write_analytics_df(
                    output,
                    target,
                    ANALYTICS_KEY_COLUMNS[args.analytics_family],
                    owner=args.owner,
                    analytics_family=args.analytics_family,
                )
                payload = {
                    "index_name": result.index_name,
                    "total_sent": result.total_sent,
                    "succeeded_count": result.succeeded_count,
                    "failed_count": result.failed_count,
                    "mode": "fixture",
                }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        result = {
            **validate_runtime_contract(cfg),
            "spark_runtime_version": spark.version,
            "expected_spark_version": cfg["spark_version"],
            "timezone": spark.conf.get("spark.sql.session.timeZone"),
        }
        if spark.version != cfg["spark_version"]:
            raise RuntimeError(
                f"Spark version mismatch: runtime={spark.version}, expected={cfg['spark_version']}"
            )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
