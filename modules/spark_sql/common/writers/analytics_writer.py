"""Chuẩn hóa DataFrame kết quả Spark SQL, sinh _id bằng serialize_key_v1 và ghi lên ES."""

from __future__ import annotations

import decimal
import math
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from modules.elasticsearch.common.config.es_client import (
    BulkResult,
    ESClient,
    validate_index_name,
)
from .key_serializer import serialize_key_v1

ANALYTICS_KEY_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "analytics_timeseries": ("batch_run_id", "period_type", "period_start", "entity_type", "movieId"),
    "analytics_top_movies": ("batch_run_id", "period_type", "period_start", "movieId"),
    "analytics_genre_trends": ("batch_run_id", "period_type", "period_start", "genre"),
    "analytics_growth": ("window_size", "window_start", "entity_type", "entity_key"),
    "analytics_tmdb_signal": ("tmdbId",),
}


def _to_json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, decimal.Decimal):
        value = float(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Output analytics chứa NaN/Infinity không hợp lệ: {value!r}")
        return 0.0 if value == 0.0 else value
    return value


def _to_key_value(raw: Any, data_type: T.DataType) -> Any:
    if raw is None:
        return None
    if isinstance(data_type, T.TimestampType):
        text = str(raw).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    if isinstance(data_type, T.DateType):
        return date.fromisoformat(str(raw).strip()[:10])
    return _to_json_safe(raw)


def prepare_analytics_records(
    df: DataFrame,
    key_columns: Sequence[str],
) -> List[Dict[str, Any]]:
    """Format ngày/giờ UTC ngay trong Spark JVM trước khi collect, tính _id và chống trùng khóa."""
    if not key_columns:
        raise ValueError("key_columns không được rỗng")

    missing = [col for col in key_columns if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame thiếu cột khóa bắt buộc: {missing}")

    field_types: Dict[str, T.DataType] = {f.name: f.dataType for f in df.schema.fields}

    # Format TimestampType và DateType sang chuỗi UTC bên trong Spark JVM để tránh lệch +0700 khi collect()
    formatted_df = df
    for field in df.schema.fields:
        if isinstance(field.dataType, T.TimestampType):
            formatted_df = formatted_df.withColumn(
                field.name,
                F.date_format(F.col(field.name), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )
        elif isinstance(field.dataType, T.DateType):
            formatted_df = formatted_df.withColumn(
                field.name,
                F.date_format(F.col(field.name), "yyyy-MM-dd"),
            )

    rows = formatted_df.collect()
    records: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    for row in rows:
        raw_dict = row.asDict(recursive=True)
        doc: Dict[str, Any] = {k: _to_json_safe(v) for k, v in raw_dict.items()}
        key_tuple = tuple(
            _to_key_value(doc[col], field_types[col]) for col in key_columns
        )
        doc_id = serialize_key_v1(key_tuple)
        if doc_id in seen_ids:
            raise ValueError(
                f"Phát hiện trùng khóa _id={doc_id} (tuple={key_tuple!r}) trong kết quả Spark SQL"
            )
        seen_ids.add(doc_id)
        doc["_id"] = doc_id
        records.append(doc)

    return records


def write_analytics_df(
    df: DataFrame,
    target_index: str,
    key_columns: Sequence[str],
    owner: str,
    client: Optional[ESClient] = None,
    batch_size: int = 1000,
    refresh: bool = False,
) -> BulkResult:
    """Ghi DataFrame analytics lên Elasticsearch bằng ESClient.bulk_upsert có kiểm tra owner."""
    clean_index = validate_index_name(target_index)
    es = client or ESClient(owner=owner)
    if es.owner != owner:
        raise ValueError(f"ESClient owner={es.owner!r} không khớp với owner={owner!r}")

    records = prepare_analytics_records(df, key_columns=key_columns)
    return es.bulk_upsert(
        clean_index,
        records,
        id_field="_id",
        batch_size=batch_size,
        refresh=refresh,
        raise_on_error=True,
    )
