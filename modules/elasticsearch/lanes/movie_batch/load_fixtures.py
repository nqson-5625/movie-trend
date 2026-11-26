"""Nạp fixture lane movie_batch (P2) vào index dev để dựng/thử D1, D2 khi chưa có dữ liệu thật.

Fixture: contracts/phase2/movie_batch/fixtures/*.jsonl. Mỗi bảng ghi vào <prefix><bảng>, mặc định
dev_p2_movie_batch_stats, dev_p2_movie_daily_stats, dev_p2_analytics_timeseries, dev_p2_analytics_top_movies.
Cần ES đang chạy, port-forward 9200, .env có ES_* và đã `make -C modules/elasticsearch init-templates`.

    .venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_fixtures            # nạp/ghi đè theo _id
    .venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_fixtures --replace  # xóa index dev rồi nạp lại
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from modules.elasticsearch.common.config.es_client import ESClient
from modules.spark_sql.common.writers.key_serializer import build_record_id

PROJECT_ROOT = Path(__file__).resolve().parents[4]
FIXTURES_DIR = PROJECT_ROOT / "contracts/phase2/movie_batch/fixtures"

# Grain/_id theo DATA_CONTRACT_Phase2 mục 4 và 6 (analytics giống ANALYTICS_KEY_COLUMNS của writer dùng chung)
FAMILY_KEYS = {
    "movie_batch_stats": ("batch_run_id", "movieId"),
    "movie_daily_stats": ("batch_run_id", "date", "movieId"),
    "analytics_timeseries": ("batch_run_id", "period_type", "period_start", "entity_type", "movieId"),
    "analytics_top_movies": ("batch_run_id", "period_type", "period_start", "movieId"),
}
DATE_KEYS = {"date", "period_start"}


def read_fixture(family: str) -> list[dict]:
    lines = (FIXTURES_DIR / f"{family}.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def build_records(family: str, rows: list[dict]) -> list[dict]:
    """Gắn _id; cột ngày đổi sang kiểu date trước khi băm để _id khớp writer Spark (DateType)."""
    keys = FAMILY_KEYS[family]
    records = []
    for row in rows:
        key = {k: date.fromisoformat(row[k]) if k in DATE_KEYS and row[k] else row.get(k) for k in keys}
        records.append({**row, "_id": build_record_id(key, keys)})
    if len({r["_id"] for r in records}) != len(records):
        raise ValueError(f"{family}: trùng _id trong fixture")
    return records


def check_template(client: ESClient, index: str, fields: set[str]) -> None:
    """Index phải được tạo từ template của lane (dynamic=strict, đủ field), không để ES tự đoán mapping."""
    simulated = client._request("POST", f"/_index_template/_simulate_index/{index}").json()
    mappings = simulated.get("template", {}).get("mappings", {})
    missing = fields - set(mappings.get("properties", {}))
    if mappings.get("dynamic") != "strict" or missing:
        raise RuntimeError(f"{index}: chưa có index template phù hợp (thiếu {sorted(missing)}). "
                           "Chạy: make -C modules/elasticsearch init-templates")


def main() -> int:
    parser = argparse.ArgumentParser(description="Nạp fixture movie_batch vào index dev")
    parser.add_argument("--prefix", default="dev_p2_", help="tiền tố index (mặc định dev_p2_)")
    parser.add_argument("--replace", action="store_true", help="xóa index dev cũ trước khi nạp")
    args = parser.parse_args()
    if not args.prefix.startswith("dev_p2_"):
        parser.error("fixture chỉ được nạp vào namespace dev_p2_ (contract mục 9)")

    client = ESClient(owner="p2")
    for family in FAMILY_KEYS:
        index = f"{args.prefix}{family}"
        records = build_records(family, read_fixture(family))
        if args.replace:
            client.delete_index(index)
        check_template(client, index, {k for r in records for k in r if k != "_id"})
        result = client.bulk_upsert(index, records, refresh=True)
        count = client.count(index)
        if count != len(records):
            raise RuntimeError(f"{index}: có {count} document, fixture có {len(records)} dòng")
        print(f"[OK] {index}: {result.succeeded_count} document")
    return 0


if __name__ == "__main__":
    sys.exit(main())
