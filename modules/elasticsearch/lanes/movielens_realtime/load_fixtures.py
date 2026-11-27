"""Nạp fixture lane movielens_realtime (P4) vào index dev để dựng/thử D4 khi chưa có dữ liệu thật.

Fixture: contracts/phase2/movielens_realtime/fixtures/. Ghi vào namespace dev_p4_ (contract mục 4, 9):
  dev_p4_movie_realtime_stats, dev_p4_genre_realtime, dev_p4_analytics_growth và một physical
  dev_p4_analytics_growth_current-v<snapshot_version> cho mỗi snapshot READY trong snapshots.json.
Loader KHÔNG tạo/chuyển alias (chỉ
publisher P5 / owner admin được đổi alias).
Cần ES đang chạy, port-forward 9200, .env có ES_* và đã `make -C modules/elasticsearch init-templates`.

    .venv/bin/python -m modules.elasticsearch.lanes.movielens_realtime.load_fixtures            # nạp/ghi đè theo _id
    .venv/bin/python -m modules.elasticsearch.lanes.movielens_realtime.load_fixtures --replace  # xóa index dev rồi nạp lại
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from modules.elasticsearch.common.config.es_client import ESClient
from modules.spark_sql.common.writers.key_serializer import build_record_id

PROJECT_ROOT = Path(__file__).resolve().parents[4]
FIXTURES_DIR = PROJECT_ROOT / "contracts/phase2/movielens_realtime/fixtures"
PREFIX = "dev_p4_"

# Source RT: _id literal đúng như Spark Streaming (concat_ws("|", ...)) để fixture không lệch writer canonical.
SOURCE_ID_FIELDS = {
    "movie_realtime_stats": ("movieId", "window_start", "window_size"),
    "genre_realtime": ("genre", "window_start", "window_size"),
}
# Analytics: serialize_key_v1 giống ANALYTICS_KEY_COLUMNS của writer Spark SQL dùng chung.
ANALYTICS_KEYS = {
    "analytics_growth": ("window_size", "window_start", "entity_type", "entity_key"),
    "analytics_growth_current": ("window_size", "entity_type", "entity_key"),
}
TIMESTAMP_KEYS = {"window_start"}


def read_jsonl(name: str) -> list[dict]:
    lines = (FIXTURES_DIR / name).read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def read_snapshots() -> list[dict]:
    return json.loads((FIXTURES_DIR / "snapshots.json").read_text(encoding="utf-8"))["snapshots"]


def to_utc(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def build_records(family: str, rows: list[dict]) -> list[dict]:
    """Gắn _id. Với analytics, window_start đổi sang datetime UTC trước khi băm để khớp TimestampType của writer."""
    records = []
    for row in rows:
        if family in SOURCE_ID_FIELDS:
            doc_id = "|".join(str(row[k]) for k in SOURCE_ID_FIELDS[family])
        else:
            keys = ANALYTICS_KEYS[family]
            typed = {k: to_utc(row[k]) if k in TIMESTAMP_KEYS else row[k] for k in keys}
            doc_id = build_record_id(typed, keys)
        records.append({**row, "_id": doc_id})
    if len({r["_id"] for r in records}) != len(records):
        raise ValueError(f"{family}: trùng _id trong fixture")
    return records


def load_plan() -> list[tuple[str, str, list[dict]]]:
    """(family, tên index dev, records) theo thứ tự nạp."""
    plan = [(family, f"{PREFIX}{family}", build_records(family, read_jsonl(f"{family}.jsonl")))
            for family in ("movie_realtime_stats", "genre_realtime", "analytics_growth")]
    for snap in read_snapshots():
        if snap["status"] != "READY":
            raise ValueError(f"{snap['snapshot_version']}: snapshots.json chỉ được chứa snapshot READY")
        rows = read_jsonl(snap["fixture_file"])
        plan.append(("analytics_growth_current", f"{PREFIX}{snap['physical_index']}",
                     build_records("analytics_growth_current", rows)))
    return plan


def check_template(client: ESClient, index: str, fields: set[str]) -> None:
    """Index phải được tạo từ template của lane (dynamic=strict, đủ field), không để ES tự đoán mapping."""
    simulated = client._request("POST", f"/_index_template/_simulate_index/{index}").json()
    mappings = simulated.get("template", {}).get("mappings", {})
    missing = fields - set(mappings.get("properties", {}))
    if mappings.get("dynamic") != "strict" or missing:
        raise RuntimeError(f"{index}: chưa có index template phù hợp (thiếu {sorted(missing)}). "
                           "Chạy: make -C modules/elasticsearch init-templates")


def main() -> int:
    parser = argparse.ArgumentParser(description="Nạp fixture movielens_realtime vào index dev_p4_")
    parser.add_argument("--replace", action="store_true", help="xóa index dev cũ trước khi nạp")
    args = parser.parse_args()

    client = ESClient(owner="p4")
    for _family, index, records in load_plan():
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
