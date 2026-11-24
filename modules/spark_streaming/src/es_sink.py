"""
es_sink.py - Idempotent Bulk Upsert vào Elasticsearch & Local JSONL Artifact Writer
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.elasticsearch.common.config.es_client import ESClient

logger = logging.getLogger("SparkSpeedLayer")


def bulk_upsert_elasticsearch(
    es_http_url: str,
    index_name: str,
    records: Iterable[Dict[str, Any]],
    id_field: str = "doc_id",
    timeout: int = 10,
    owner: str = "spark_streaming",
    client: Optional[ESClient] = None,
) -> int:
    """
    Gửi request POST /_bulk lên Elasticsearch thông qua ESClient dùng chung (owner='spark_streaming').
    Tự động đọc ES_USERNAME / ES_PASSWORD từ .env (mặc định tài khoản elastic),
    retry lỗi tạm thời và loại bỏ id_field khỏi body document trước khi lưu.
    """
    rec_list = list(records)
    if not rec_list:
        return 0

    created_client = client is None
    es_client: Optional[ESClient] = client
    try:
        if es_client is None:
            es_client = ESClient(
                owner=owner,
                base_url=es_http_url,
                timeout=float(timeout),
            )
        result = es_client.bulk_upsert(
            index_name=index_name,
            records=rec_list,
            id_field=id_field,
            raise_on_error=False,
        )
        if result.failed_count > 0:
            preview = "; ".join(
                f"_id={e.doc_id} ({e.status} {e.error_type}: {e.reason})"
                for e in result.errors[:5]
            )
            logger.warning(
                f"Elasticsearch bulk upsert vào {index_name} lỗi {result.failed_count}/"
                f"{result.total_sent} bản ghi [{preview}]."
            )
        return result.succeeded_count
    except (requests.RequestException, RuntimeError) as e:
        logger.warning(
            f"Không đẩy được dữ liệu lên Elasticsearch ({es_http_url}, index={index_name}): {e}. "
            "Dữ liệu vẫn được lưu ở local output."
        )
        return 0
    finally:
        if created_client and es_client is not None:
            session = getattr(es_client, "_session", None)
            if session is not None:
                session.close()


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
