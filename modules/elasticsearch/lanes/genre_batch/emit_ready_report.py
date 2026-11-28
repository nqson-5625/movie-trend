"""Sinh file READY report cho lane P3 (`p3_ready_<RUN_ID>.json`) từ kết quả đối soát ES thật."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from modules.elasticsearch.common.config.es_client import ESClient, load_runtime_config, validate_index_name
from modules.elasticsearch.common.publication.ready_report import (
    IndexReadyEntry,
    ReadyReport,
    save_ready_report,
)
from modules.elasticsearch.lanes.genre_batch.load_source import (
    extract_manifest_expectations,
    load_manifest_info,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "data" / "local" / "phase2" / "es_reports"
EXPECTED_GRAINS = ("day", "month", "year")


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
    if sql_test_result != "PASS":
        raise ValueError(f"Không thể xuất READY report khi sql_test_result={sql_test_result!r} != 'PASS'")
    if source_row_count <= 0 or source_sum_count <= 0:
        raise ValueError(f"Số liệu source không hợp lệ: rows={source_row_count}, sum={source_sum_count}")
    if analytics_row_count < source_row_count:
        raise ValueError(
            f"analytics_row_count ({analytics_row_count}) nhỏ hơn source_row_count ({source_row_count})"
        )
    src_idx = validate_index_name(source_physical_index)
    ana_idx = validate_index_name(analytics_physical_index)
    if src_idx == "genre_trends" or ana_idx == "analytics_genre_trends":
        raise ValueError("READY report phải trỏ tới physical index theo run_id, không dùng canonical alias")

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
                physical_index=src_idx,
                row_count=source_row_count,
                duplicate_id_count=0,
                bulk_error_count=0,
                sum_count=source_sum_count,
            ),
            IndexReadyEntry(
                alias_name="analytics_genre_trends",
                physical_index=ana_idx,
                row_count=analytics_row_count,
                duplicate_id_count=0,
                bulk_error_count=0,
                sum_count=analytics_sum_count if analytics_sum_count is not None else source_sum_count,
            ),
        ],
    )


def _search_agg(client: ESClient, index_name: str, body: Dict[str, Any]) -> Dict[str, Any]:
    return client._request("POST", f"/{index_name}/_search", json_body=body).json()


def verify_and_build_p3_ready_report(
    *,
    manifest_path: Path,
    source_physical_index: str,
    analytics_physical_index: str,
    sql_test_result: str = "PASS",
    client: ESClient | None = None,
    completed_at: str | None = None,
) -> ReadyReport:
    """Đối soát trực tiếp ES (source + analytics) với manifest.json rồi mới tạo báo cáo READY."""
    if sql_test_result != "PASS":
        raise ValueError(f"Không thể xuất READY report khi sql_test_result={sql_test_result!r} != 'PASS'")

    manifest, manifest_sha256 = load_manifest_info(manifest_path)
    run_id = str(manifest["run_id"])
    cutoff = str(manifest["batch_cutoff_time"])
    ref_version = str(manifest["reference"]["ref_version"])
    expectations = extract_manifest_expectations(manifest)

    src_idx = validate_index_name(source_physical_index)
    ana_idx = validate_index_name(analytics_physical_index)
    es = client or ESClient(owner="p3")

    src_res = _search_agg(
        es,
        src_idx,
        {
            "size": 0,
            "aggs": {
                "sum_inter": {"sum": {"field": "interaction_count"}},
                "sum_rating": {"sum": {"field": "rating_count"}},
                "mismatch_meta": {
                    "filter": {
                        "bool": {
                            "must_not": [
                                {"term": {"batch_run_id": run_id}},
                                {"term": {"batch_cutoff_time": cutoff}},
                            ]
                        }
                    }
                },
            },
        },
    )
    src_rows = int(src_res["hits"]["total"]["value"])
    src_aggs = src_res["aggregations"]
    if src_rows <= 0:
        raise ValueError(f"{src_idx}: không có document nào trên ES")
    if int(src_aggs["mismatch_meta"]["doc_count"]) > 0:
        raise ValueError(f"{src_idx}: có {src_aggs['mismatch_meta']['doc_count']} doc lệch batch_run_id/cutoff")
    if expectations["expected_rows"] is not None and src_rows != expectations["expected_rows"]:
        raise ValueError(f"{src_idx}: ES có {src_rows} docs != manifest ({expectations['expected_rows']})")

    src_sum_inter = float(src_aggs["sum_inter"]["value"])
    src_sum_rating = float(src_aggs["sum_rating"]["value"])
    if src_sum_inter <= 0 or abs(src_sum_inter - src_sum_rating) > 1e-6:
        raise ValueError(f"{src_idx}: sum(interaction_count)={src_sum_inter} != sum(rating_count)={src_sum_rating}")
    if expectations["expected_total"] is not None and int(round(src_sum_rating)) != expectations["expected_total"]:
        raise ValueError(f"{src_idx}: tổng ES ({src_sum_rating}) != manifest ({expectations['expected_total']})")

    ana_res = _search_agg(
        es,
        ana_idx,
        {
            "size": 0,
            "aggs": {
                "mismatch_meta": {
                    "filter": {
                        "bool": {
                            "must_not": [
                                {"term": {"batch_run_id": run_id}},
                                {"term": {"batch_cutoff_time": cutoff}},
                                {"term": {"ref_version": ref_version}},
                                {"term": {"manifest_hash": manifest_sha256}},
                            ]
                        }
                    }
                },
                "by_period": {
                    "terms": {"field": "period_type", "size": 10},
                    "aggs": {
                        "sum_inter": {"sum": {"field": "interaction_count"}},
                        "sum_rating": {"sum": {"field": "rating_count"}},
                    },
                },
            },
        },
    )
    ana_rows = int(ana_res["hits"]["total"]["value"])
    ana_aggs = ana_res["aggregations"]
    if ana_rows <= 0:
        raise ValueError(f"{ana_idx}: không có document nào trên ES")
    if int(ana_aggs["mismatch_meta"]["doc_count"]) > 0:
        raise ValueError(f"{ana_idx}: có {ana_aggs['mismatch_meta']['doc_count']} doc lệch metadata/manifest_hash")

    buckets = {b["key"]: b for b in ana_aggs["by_period"]["buckets"]}
    if set(buckets) != set(EXPECTED_GRAINS):
        raise ValueError(f"{ana_idx}: period_type trên ES là {sorted(buckets)}, kỳ vọng {list(EXPECTED_GRAINS)}")
    if int(buckets["day"]["doc_count"]) != src_rows:
        raise ValueError(f"{ana_idx}: grain 'day' có {buckets['day']['doc_count']} docs != source ({src_rows})")
    for grain in EXPECTED_GRAINS:
        g_inter = float(buckets[grain]["sum_inter"]["value"])
        g_rating = float(buckets[grain]["sum_rating"]["value"])
        if abs(g_inter - src_sum_rating) > 1e-6 or abs(g_rating - src_sum_rating) > 1e-6:
            raise ValueError(
                f"{ana_idx}: tổng grain '{grain}' (inter={g_inter}, rating={g_rating}) != source ({src_sum_rating})"
            )

    return build_p3_ready_report(
        batch_run_id=run_id,
        batch_cutoff_time=cutoff,
        ref_version=ref_version,
        manifest_sha256=manifest_sha256,
        source_physical_index=src_idx,
        source_row_count=src_rows,
        source_sum_count=src_sum_rating,
        analytics_physical_index=ana_idx,
        analytics_row_count=ana_rows,
        analytics_sum_count=src_sum_rating,
        sql_test_result=sql_test_result,
        completed_at=completed_at,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Đối soát ES và xuất p3_ready_<RUN_ID>.json cho P5 atomic Batch publish")
    parser.add_argument("--manifest", type=Path, required=True, help="Đường dẫn file manifest.json của batch run")
    parser.add_argument("--source-index", required=True, help="Physical source index genre_trends_<run_id>")
    parser.add_argument("--analytics-index", required=True, help="Physical analytics index analytics_genre_trends_<run_id>")
    parser.add_argument("--sql-test-result", default="PASS", choices=["PASS", "FAIL"])
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    report = verify_and_build_p3_ready_report(
        manifest_path=args.manifest,
        source_physical_index=args.source_index,
        analytics_physical_index=args.analytics_index,
        sql_test_result=args.sql_test_result,
    )
    out_path = args.out or (DEFAULT_REPORTS_DIR / f"p3_ready_{report.batch_run_id}.json")
    saved = save_ready_report(report, out_path)
    print(f"[OK] Đã đối soát ES và lưu READY report P3: {saved}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
