"""Nạp dữ liệu từ Elasticsearch vào 6 Spark SQL Temp Views chuẩn theo DATA_CONTRACT_Phase2.md."""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from modules.elasticsearch.common.config.es_client import (
    ESClient,
    load_runtime_config,
    validate_index_name,
)

SOURCE_SCHEMAS: Dict[str, T.StructType] = {
    "batch_movie": T.StructType(
        [
            T.StructField("movieId", T.IntegerType(), False),
            T.StructField("title", T.StringType(), True),
            T.StructField("total_count", T.LongType(), False),
            T.StructField("rating_count", T.LongType(), False),
            T.StructField("rating_sum", T.DoubleType(), False),
            T.StructField("avg_rating", T.DoubleType(), False),
            T.StructField("batch_run_id", T.StringType(), False),
            T.StructField("batch_cutoff_time", T.TimestampType(), False),
            T.StructField("generated_at", T.TimestampType(), False),
        ]
    ),
    "batch_movie_daily": T.StructType(
        [
            T.StructField("date", T.DateType(), False),
            T.StructField("movieId", T.IntegerType(), False),
            T.StructField("interaction_count", T.LongType(), False),
            T.StructField("rating_count", T.LongType(), False),
            T.StructField("rating_sum", T.DoubleType(), False),
            T.StructField("avg_rating", T.DoubleType(), False),
            T.StructField("batch_run_id", T.StringType(), False),
            T.StructField("batch_cutoff_time", T.TimestampType(), False),
            T.StructField("generated_at", T.TimestampType(), False),
        ]
    ),
    "batch_genre": T.StructType(
        [
            T.StructField("date", T.DateType(), False),
            T.StructField("genre", T.StringType(), False),
            T.StructField("interaction_count", T.LongType(), False),
            T.StructField("rating_count", T.LongType(), False),
            T.StructField("rating_sum", T.DoubleType(), False),
            T.StructField("avg_rating", T.DoubleType(), False),
            T.StructField("batch_run_id", T.StringType(), False),
            T.StructField("batch_cutoff_time", T.TimestampType(), False),
            T.StructField("generated_at", T.TimestampType(), False),
        ]
    ),
    "rt_movie": T.StructType(
        [
            T.StructField("window_start", T.TimestampType(), False),
            T.StructField("window_end", T.TimestampType(), False),
            T.StructField("window_size", T.StringType(), False),
            T.StructField("movieId", T.IntegerType(), False),
            T.StructField("interaction_count", T.LongType(), False),
            T.StructField("rating_count", T.LongType(), False),
            T.StructField("rating_sum", T.DoubleType(), False),
            T.StructField("avg_rating", T.DoubleType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
    "rt_genre": T.StructType(
        [
            T.StructField("window_start", T.TimestampType(), False),
            T.StructField("window_end", T.TimestampType(), False),
            T.StructField("window_size", T.StringType(), False),
            T.StructField("genre", T.StringType(), False),
            T.StructField("interaction_count", T.LongType(), False),
            T.StructField("rating_count", T.LongType(), False),
            T.StructField("rating_sum", T.DoubleType(), False),
            T.StructField("avg_rating", T.DoubleType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
    "tmdb_rt": T.StructType(
        [
            T.StructField("tmdbId", T.IntegerType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("popularity", T.DoubleType(), False),
            T.StructField("vote_count", T.LongType(), False),
            T.StructField("vote_average", T.DoubleType(), False),
            T.StructField("prev_popularity", T.DoubleType(), True),
            T.StructField("popularity_delta", T.DoubleType(), True),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
}

TEMP_VIEW_TO_DEFAULT_INDEX: Dict[str, str] = {
    meta["temp_view"]: idx_name
    for idx_name, meta in load_runtime_config()["source_indices"].items()
}


def _parse_utc_datetime(raw: Any) -> datetime:
    """Chuyển chuỗi ISO hoặc datetime có tzinfo sang datetime UTC-aware."""
    if isinstance(raw, datetime):
        if raw.tzinfo is None or raw.utcoffset() is None:
            raise ValueError(f"Timestamp phải có múi giờ (tzinfo), nhận naive: {raw!r}")
        return raw.astimezone(timezone.utc)
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Timestamp không hợp lệ: {raw!r}")
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"Timestamp thiếu offset/Z múi giờ UTC: {raw!r}")
    return dt.astimezone(timezone.utc)


def _parse_date(raw: Any) -> date:
    """Chỉ nhận date thuần, chuỗi YYYY-MM-DD dài 10 ký tự, hoặc timestamp đúng 00:00:00Z."""
    if isinstance(raw, datetime):
        utc_dt = _parse_utc_datetime(raw)
        if (utc_dt.hour, utc_dt.minute, utc_dt.second, utc_dt.microsecond) != (0, 0, 0, 0):
            raise ValueError(f"Cột Date không được có giờ khác 00:00:00Z: {raw!r}")
        return utc_dt.date()
    if isinstance(raw, date):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Giá trị Date không hợp lệ: {raw!r}")
    text = raw.strip()
    if len(text) == 10:
        return date.fromisoformat(text)
    utc_dt = _parse_utc_datetime(text)
    if (utc_dt.hour, utc_dt.minute, utc_dt.second, utc_dt.microsecond) != (0, 0, 0, 0):
        raise ValueError(f"Cột Date không được có giờ khác 00:00:00Z: {raw!r}")
    return utc_dt.date()


def _coerce_value(value: Any, field: T.StructField) -> Any:
    if value is None:
        if not field.nullable:
            raise ValueError(f"Cột bắt buộc '{field.name}' bị null")
        return None

    dtype = field.dataType
    if isinstance(dtype, T.StringType):
        return str(value)
    if isinstance(dtype, (T.IntegerType, T.LongType)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"Cột '{field.name}' đòi hỏi số nguyên, nhận {value!r}")
        return int(value)
    if isinstance(dtype, T.DoubleType):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"Cột '{field.name}' đòi hỏi số thực, nhận {value!r}")
        num = float(value)
        if not math.isfinite(num):
            raise ValueError(f"Cột '{field.name}' chứa NaN/Infinity: {value!r}")
        return num
    if isinstance(dtype, T.BooleanType):
        if not isinstance(value, bool):
            raise ValueError(f"Cột '{field.name}' đòi hỏi boolean, nhận {value!r}")
        return value
    if isinstance(dtype, T.TimestampType):
        return _parse_utc_datetime(value)
    if isinstance(dtype, T.DateType):
        return _parse_date(value)

    raise TypeError(f"Chưa hỗ trợ ép kiểu cho {dtype}")


def rows_to_dataframe(
    spark: SparkSession,
    temp_view: str,
    rows: Iterable[Mapping[str, Any]],
) -> DataFrame:
    """Chuyển danh sách dict JSON từ ES thành Spark DataFrame với schema cứng của temp_view."""
    if temp_view not in SOURCE_SCHEMAS:
        raise ValueError(
            f"temp_view không hợp lệ: {temp_view!r}. Hợp lệ: {sorted(SOURCE_SCHEMAS)}"
        )
    schema = SOURCE_SCHEMAS[temp_view]
    typed_rows: List[tuple[Any, ...]] = []
    for raw_row in rows:
        typed_rows.append(
            tuple(_coerce_value(raw_row.get(field.name), field) for field in schema.fields)
        )
    return spark.createDataFrame(typed_rows, schema=schema)


def read_es_source_df(
    spark: SparkSession,
    temp_view: str,
    index_name: Optional[str] = None,
    client: Optional[ESClient] = None,
    query: Optional[Dict[str, Any]] = None,
    page_size: int = 5000,
) -> DataFrame:
    """Đọc toàn bộ document từ 1 physical index hoặc alias trên ES qua PIT và trả về DataFrame."""
    if temp_view not in SOURCE_SCHEMAS:
        raise ValueError(
            f"temp_view không hợp lệ: {temp_view!r}. Hợp lệ: {sorted(SOURCE_SCHEMAS)}"
        )
    target_index = validate_index_name(index_name or TEMP_VIEW_TO_DEFAULT_INDEX[temp_view])
    es = client or ESClient(owner="p1")  # Đọc read-only không cần quyền ghi
    docs = es.search_all_pit(
        target_index,
        page_size=page_size,
        query=query,
        include_id=False,
    )
    return rows_to_dataframe(spark, temp_view, docs)


def register_temp_view(
    spark: SparkSession,
    temp_view: str,
    index_name: Optional[str] = None,
    client: Optional[ESClient] = None,
    query: Optional[Dict[str, Any]] = None,
    page_size: int = 5000,
) -> DataFrame:
    """Đọc dữ liệu từ ES và đăng ký thành Temp View chuẩn cho spark.sql(...)."""
    df = read_es_source_df(
        spark=spark,
        temp_view=temp_view,
        index_name=index_name,
        client=client,
        query=query,
        page_size=page_size,
    )
    df.createOrReplaceTempView(temp_view)
    return df


def register_all_temp_views(
    spark: SparkSession,
    index_overrides: Optional[Mapping[str, str]] = None,
    views: Optional[Sequence[str]] = None,
    client: Optional[ESClient] = None,
) -> Dict[str, int]:
    """Nạp và đăng ký đồng loạt các Temp View được chọn, trả về số dòng từng view."""
    target_views = list(views or SOURCE_SCHEMAS.keys())
    overrides = dict(index_overrides or {})
    counts: Dict[str, int] = {}
    for view_name in target_views:
        df = register_temp_view(
            spark=spark,
            temp_view=view_name,
            index_name=overrides.get(view_name),
            client=client,
        )
        counts[view_name] = df.count()
    return counts
