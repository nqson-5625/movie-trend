"""Sinh READY report của lane movie_batch (`p2_ready_<run_id>.json`) cho P5 atomic publish 6 Batch aliases (P2.3).

Gộp báo cáo của load_batch_run (p2_source_<run>.json) và job analytics (p2_analytics_<run>.json), rồi kiểm tra lại
trực tiếp trên ES trước khi ghi status READY:
  - 4 physical index tồn tại, đúng số document, mỗi index đúng một batch_run_id và một batch_cutoff_time;
  - 2 analytics mang đúng một ref_version và một manifest_hash, trùng manifest của run;
  - tổng lượt khớp nhau: hai source và lát all-time của hai analytics đều bằng tổng của source.
sum_count trong report là tổng lượt tương tác của run (giống cách lane genre_batch ghi cho analytics).

    .venv/bin/python -m modules.elasticsearch.lanes.movie_batch.emit_ready_report --sql-test-result PASS
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from modules.elasticsearch.common.config.es_client import ESClient, load_runtime_config
from modules.elasticsearch.common.publication.ready_report import IndexReadyEntry, ReadyReport, save_ready_report

PROJECT_ROOT = Path(__file__).resolve().parents[4]
REPORTS_DIR = PROJECT_ROOT / "data/local/phase2/es_reports"
P2_ALIASES = ("movie_batch_stats", "movie_daily_stats", "analytics_timeseries", "analytics_top_movies")
# Lát dữ liệu "toàn thời gian" của từng index và cột đếm tương ứng: tổng phải bằng tổng lượt của run
ALL_TIME_TOTALS = {
    "movie_batch_stats": (None, "total_count"),
    "movie_daily_stats": (None, "interaction_count"),
    "analytics_timeseries": ([{"term": {"entity_type": "all"}}, {"term": {"period_type": "all"}}], "rating_count"),
    "analytics_top_movies": ([{"term": {"period_type": "all"}}], "interaction_count"),
}


def _single_term(client: ESClient, index: str, field: str) -> tuple[list, int]:
    resp = client.search(index, {"size": 0, "aggs": {"v": {"terms": {"field": field, "size": 10}}}})
    buckets = resp["aggregations"]["v"]["buckets"]
    return [b.get("key_as_string", b["key"]) for b in buckets], (buckets[0]["doc_count"] if len(buckets) == 1 else -1)


def verify_in_es(client: ESClient, meta: dict, entries: dict[str, dict]) -> dict[str, float]:
    """Kiểm tra 4 physical index trên ES; trả về tổng lượt all-time của từng alias."""
    errors, totals = [], {}
    for alias in P2_ALIASES:
        index = entries[alias]["physical_index"]
        if not client.index_exists(index):
            errors.append(f"{index} chưa tồn tại")
            continue
        count = client.count(index)
        if count != entries[alias]["row_count"]:
            errors.append(f"{index}: ES có {count} document, báo cáo ghi {entries[alias]['row_count']}")
        checks = [("batch_run_id", meta["batch_run_id"]), ("batch_cutoff_time", meta["batch_cutoff_time"])]
        if alias.startswith("analytics_"):
            checks += [("ref_version", meta["ref_version"]), ("manifest_hash", meta["manifest_sha256"])]
        for field, expected in checks:
            values, docs = _single_term(client, index, field)
            normalized = [v.replace(".000Z", "Z") if isinstance(v, str) else v for v in values]
            if normalized != [expected] or docs != count:
                errors.append(f"{index}: {field} = {values}, cần đúng một giá trị {expected!r} cho mọi document")
        flt, field = ALL_TIME_TOTALS[alias]
        body = {"size": 0, "aggs": {"s": {"sum": {"field": field}}}}
        if flt:
            body["query"] = {"bool": {"filter": flt}}
        totals[alias] = client.search(index, body)["aggregations"]["s"]["value"]
    if len(set(totals.values())) > 1:
        errors.append(f"Tổng lượt all-time lệch nhau giữa các index: {totals}")
    if errors:
        raise ValueError("Chưa thể READY: " + "; ".join(errors))
    return totals


def build_ready_report(meta: dict, entries: dict[str, dict], totals: dict[str, float], sql_test_result: str,
                       completed_at: str | None = None) -> ReadyReport:
    return ReadyReport(
        owner="p2",
        schema_version=str(load_runtime_config()["schema_version"]),
        manifest_sha256=meta["manifest_sha256"],
        batch_run_id=meta["batch_run_id"],
        batch_cutoff_time=meta["batch_cutoff_time"],
        ref_version=meta["ref_version"],
        sql_test_result=sql_test_result,
        completed_at=completed_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        status="READY" if sql_test_result == "PASS" else "NOT_READY",
        indices=[IndexReadyEntry(alias_name=alias, physical_index=entries[alias]["physical_index"],
                                 row_count=entries[alias]["row_count"], duplicate_id_count=0,
                                 bulk_error_count=entries[alias]["bulk_error_count"], sum_count=float(totals[alias]))
                 for alias in P2_ALIASES],
    )


def load_reports(run_id: str) -> tuple[dict, dict[str, dict]]:
    """Metadata và entry của 4 index từ báo cáo nạp source và báo cáo ghi analytics; hai báo cáo phải cùng một run."""
    source = json.loads((REPORTS_DIR / f"p2_source_{run_id}.json").read_text(encoding="utf-8"))
    analytics = json.loads((REPORTS_DIR / f"p2_analytics_{run_id}.json").read_text(encoding="utf-8"))
    keys = ("batch_run_id", "batch_cutoff_time", "ref_version", "manifest_sha256")
    if any(source[k] != analytics[k] for k in keys):
        raise ValueError("p2_source và p2_analytics không cùng run/T/ref_version/manifest")
    entries = {e["alias_name"]: e for e in source["indices"] + analytics["indices"]}
    if set(entries) != set(P2_ALIASES):
        raise ValueError(f"Báo cáo thiếu index: cần {P2_ALIASES}, có {sorted(entries)}")
    return {k: source[k] for k in keys}, entries


def main() -> int:
    parser = argparse.ArgumentParser(description="Xuất p2_ready_<RUN_ID>.json cho P5 atomic Batch publish")
    parser.add_argument("--run-id", default="20261024T104145Z")
    parser.add_argument("--sql-test-result", required=True, choices=["PASS", "FAIL"],
                        help="kết quả test SQL của lane (modules/spark_sql/tests/lanes) vừa chạy")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    meta, entries = load_reports(args.run_id)
    totals = verify_in_es(ESClient(owner="p2"), meta, entries)
    report = build_ready_report(meta, entries, totals, args.sql_test_result)
    saved = save_ready_report(report, args.out or REPORTS_DIR / f"p2_ready_{args.run_id}.json")
    print(f"[OK] {report.status}: {saved}")
    for e in report.indices:
        print(f"     {e.alias_name:<22} {e.physical_index:<42} {e.row_count:>7} dòng, tổng lượt {e.sum_count:.0f}")
    return 0 if report.status == "READY" else 1


if __name__ == "__main__":
    sys.exit(main())
