"""
es_sink.py - Idempotent Bulk Upsert vào Elasticsearch & Local JSONL Artifact Writer
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List

logger = logging.getLogger("SparkSpeedLayer")


def bulk_upsert_elasticsearch(
    es_http_url: str,
    index_name: str,
    records: Iterable[Dict[str, Any]],
    id_field: str = "doc_id",
    timeout: int = 10,
) -> int:
    """
    Gửi request POST /_bulk lên Elasticsearch với action 'index' (upsert theo _id).
    Loại bỏ trường id_field khỏi body document trước khi lưu để khớp 100% schema DATA_CONTRACT.md.
    """
    lines: List[str] = []
    count = 0
    for rec in records:
        doc = dict(rec)
        doc_id = str(doc.pop(id_field))
        meta = json.dumps({"index": {"_index": index_name, "_id": doc_id}})
        payload = json.dumps(doc, ensure_ascii=False)
        lines.append(meta)
        lines.append(payload)
        count += 1

    if count == 0:
        return 0

    body = ("\n".join(lines) + "\n").encode("utf-8")
    url = f"{es_http_url.rstrip('/')}/_bulk"
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = json.loads(resp.read().decode("utf-8"))
            if resp_body.get("errors"):
                logger.warning(f"Elasticsearch bulk upsert vào {index_name} có lỗi một phần.")
            return count
    except urllib.error.URLError as e:
        logger.warning(
            f"Không kết nối được Elasticsearch ({url}): {e}. Dữ liệu vẫn được lưu ở local output."
        )
        return 0


def merge_window_stats(
    state_store: Dict[str, Dict[str, Any]],
    batch_records: Iterable[Dict[str, Any]],
    id_field: str = "doc_id",
) -> List[Dict[str, Any]]:
    """
    Cộng dồn (accumulate) số liệu của micro-batch mới vào trạng thái lũy kế của cửa sổ (10m / 1h)
    theo khóa định danh doc_id (movieId|window_start|window_size hoặc genre|window_start|window_size).
    Trả về danh sách các document đã được cập nhật lũy kế trong batch này để upsert vào Elasticsearch.
    """
    updated_docs: List[Dict[str, Any]] = []
    for rec in batch_records:
        doc = dict(rec)
        doc_id = str(doc[id_field])
        if doc_id not in state_store:
            state_store[doc_id] = doc
        else:
            prev = state_store[doc_id]
            new_inter_cnt = int(prev["interaction_count"]) + int(doc["interaction_count"])
            new_rating_cnt = int(prev["rating_count"]) + int(doc["rating_count"])
            new_rating_sum = round(float(prev["rating_sum"]) + float(doc["rating_sum"]), 4)
            new_avg_rating = round(new_rating_sum / new_rating_cnt, 4) if new_rating_cnt > 0 else 0.0

            prev["interaction_count"] = new_inter_cnt
            prev["rating_count"] = new_rating_cnt
            prev["rating_sum"] = new_rating_sum
            prev["avg_rating"] = new_avg_rating
            prev["updated_at"] = doc["updated_at"]
        updated_docs.append(dict(state_store[doc_id]))
    return updated_docs


def write_jsonl_output(
    output_file: Path,
    records: Iterable[Dict[str, Any]],
    strip_doc_id: bool = True,
) -> int:
    """Ghi danh sách record ra file JSONL để kiểm thử và tạo sample fixture."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for rec in records:
            doc = dict(rec)
            if strip_doc_id and "doc_id" in doc:
                doc.pop("doc_id")
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            count += 1
    return count