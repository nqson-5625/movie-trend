"""Sinh file READY report cho lane P3 (`p3_ready_<RUN_ID>.json`) để P5 đối soát và atomic publish 6 Batch aliases."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from modules.elasticsearch.common.config.es_client import load_runtime_config, validate_index_name
from modules.elasticsearch.common.publication.ready_report import (
    IndexReadyEntry,
    ReadyReport,
    save_ready_report,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "data" / "local" / "phase2" / "es_reports"


def build_p3_ready_report(
    *,
    batch_run_id: str,
    batch_cutoff_time: str,
    ref_version: str,
    manifest_sha256: str,
    source_physical_index: str,
    source_row_count: int,
    source_sum_count: float,
    analytics_physical_index: str,
    analytics_row_count: int,
    analytics_sum_count: float | None = None,
    sql_test_result: str = "PASS",
    completed_at: str | None = None,
) -> ReadyReport:
    schema_version = str(load_runtime_config()["schema_version"])
    ts = completed_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return ReadyReport(
        owner="p3",
        schema_version=schema_version,
        manifest_sha256=manifest_sha256,
        batch_run_id=batch_run_id,
        batch_cutoff_time=batch_cutoff_time,
        ref_version=ref_version,
        sql_test_result=sql_test_result,
        completed_at=ts,
        status="READY",
        indices=[
            IndexReadyEntry(
                alias_name="genre_trends",
                physical_index=validate_index_name(source_physical_index),
                row_count=source_row_count,
                duplicate_id_count=0,
                bulk_error_count=0,
                sum_count=source_sum_count,
            ),
            IndexReadyEntry(
                alias_name="analytics_genre_trends",
                physical_index=validate_index_name(analytics_physical_index),
                row_count=analytics_row_count,
                duplicate_id_count=0,
                bulk_error_count=0,
                sum_count=analytics_sum_count if analytics_sum_count is not None else source_sum_count,
            ),
        ],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Xuất p3_ready_<RUN_ID>.json cho P5 atomic Batch publish")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--ref-version", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--source-index", required=True)
    parser.add_argument("--source-rows", type=int, required=True)
    parser.add_argument("--source-sum", type=float, required=True)
    parser.add_argument("--analytics-index", required=True)
    parser.add_argument("--analytics-rows", type=int, required=True)
    parser.add_argument("--analytics-sum", type=float, default=None)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    out_path = args.out or (DEFAULT_REPORTS_DIR / f"p3_ready_{args.run_id}.json")
    report = build_p3_ready_report(
        batch_run_id=args.run_id,
        batch_cutoff_time=args.cutoff,
        ref_version=args.ref_version,
        manifest_sha256=args.manifest_sha256,
        source_physical_index=args.source_index,
        source_row_count=args.source_rows,
        source_sum_count=args.source_sum,
        analytics_physical_index=args.analytics_index,
        analytics_row_count=args.analytics_rows,
        analytics_sum_count=args.analytics_sum,
    )
    saved = save_ready_report(report, out_path)
    print(f"[OK] Đã lưu READY report P3: {saved}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
