"""Chuẩn hóa DataFrame kết quả Spark SQL, sinh _id bằng serialize_key_v1 và ghi lên ES."""

from __future__ import annotations

import decimal
import math
from datetime import date, datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from modules.elasticsearch.common.config.es_client import (
    BulkResult,
    ESClient,
    validate_index_name,
)
from modules.elasticsearch.common.config.es_client import load_runtime_config
from .key_serializer import serialize_key_v1

ANALYTICS_KEY_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "analytics_timeseries": ("batch_run_id", "period_type", "period_start", "entity_type", "movieId"),
    "analytics_top_movies": ("batch_run_id", "period_type", "period_start", "movieId"),
    "analytics_genre_trends": ("batch_run_id", "period_type", "period_start", "genre"),
    "analytics_growth": ("window_size", "window_start", "entity_type", "entity_key"),
    "analytics_growth_current": ("window_size", "entity_type", "entity_key"),
    "analytics_tmdb_signal": ("tmdbId",),
}

COMMON_ANALYTICS_COLUMNS = ("schema_version", "analytics_run_id", "published_at")
LITERAL_ID_FAMILIES = {"analytics_tmdb_signal"}
FAMILY_REQUIRED_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "analytics_timeseries": ("batch_cutoff_time", "ref_version", "manifest_hash"),
    "analytics_top_movies": ("batch_cutoff_time", "ref_version", "manifest_hash"),
    "analytics_genre_trends": ("batch_cutoff_time", "ref_version", "manifest_hash"),
    "analytics_growth": ("updated_at", "timeline_mode"),
    "analytics_growth_current": (
        "snapshot_version",
        "selected_window_start",
        "source_complete_through",
        "history_analytics_run_id",
        "updated_at",
        "timeline_mode",
    ),
    "analytics_tmdb_signal": ("source",),
}


@dataclass(frozen=True)
class DistributedWriteResult:
    index_name: str
    written_count: int
    mode: str = "distributed"


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
    analytics_family: str | None = None,
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
        if analytics_family in LITERAL_ID_FAMILIES:
            if len(key_tuple) != 1 or key_tuple[0] is None:
                raise ValueError(f"{analytics_family} yêu cầu đúng một literal business key")
            doc_id = str(key_tuple[0])
        else:
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
    analytics_family: str | None = None,
) -> BulkResult:
    """Ghi DataFrame analytics lên Elasticsearch bằng ESClient.bulk_upsert có kiểm tra owner."""
    clean_index = validate_index_name(target_index)
    if analytics_family is not None:
        validate_analytics_dataframe(df, analytics_family)
        clean_index = validate_physical_target(clean_index, analytics_family)
    es = client or ESClient(owner=owner)
    if es.owner != owner:
        raise ValueError(f"ESClient owner={es.owner!r} không khớp với owner={owner!r}")

    records = prepare_analytics_records(
        df, key_columns=key_columns, analytics_family=analytics_family
    )
    if analytics_family is not None:
        for record in records:
            null_common = [name for name in COMMON_ANALYTICS_COLUMNS if record.get(name) is None]
            if null_common:
                raise ValueError(f"Document {analytics_family} có common metadata null: {null_common}")
    return es.bulk_upsert(
        clean_index,
        records,
        id_field="_id",
        batch_size=batch_size,
        refresh=refresh,
        raise_on_error=True,
    )


def validate_analytics_dataframe(df: DataFrame, analytics_family: str) -> None:
    if analytics_family not in ANALYTICS_KEY_COLUMNS:
        raise ValueError(f"Analytics family không hợp lệ: {analytics_family!r}")
    required = (
        set(ANALYTICS_KEY_COLUMNS[analytics_family])
        | set(COMMON_ANALYTICS_COLUMNS)
        | set(FAMILY_REQUIRED_COLUMNS[analytics_family])
    )
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"DataFrame {analytics_family} thiếu cột bắt buộc: {missing}")
    types = {field.name: field.dataType for field in df.schema.fields}
    if not isinstance(types["schema_version"], T.StringType):
        raise ValueError("schema_version phải là STRING")
    if not isinstance(types["analytics_run_id"], T.StringType):
        raise ValueError("analytics_run_id phải là STRING")
    if not isinstance(types["published_at"], T.TimestampType):
        raise ValueError("published_at phải là TIMESTAMP")


def validate_physical_target(target_index: str, analytics_family: str) -> str:
    clean = validate_index_name(target_index)
    if clean.startswith("dev_"):
        return clean
    if analytics_family in {"analytics_growth_current", "analytics_tmdb_signal"}:
        if not clean.startswith(f"{analytics_family}-v"):
            raise ValueError(
                f"{analytics_family} phải ghi physical index dạng {analytics_family}-v<version>"
            )
        return clean
    if analytics_family == "analytics_growth" and clean == analytics_family:
        return clean
    if clean == analytics_family:
        raise ValueError(f"Không được ghi trực tiếp canonical alias {analytics_family!r}")
    if not clean.startswith(f"{analytics_family}_") and not clean.startswith(
        f"{analytics_family}-"
    ):
        raise ValueError(f"Target {clean!r} không thuộc family {analytics_family!r}")
    return clean


def write_analytics_df_distributed(
    df: DataFrame,
    target_index: str,
    analytics_family: str,
) -> DistributedWriteResult:
    """Write without collecting rows on the driver using the configured Spark ES connector."""
    validate_analytics_dataframe(df, analytics_family)
    clean = validate_physical_target(target_index, analytics_family)
    key_columns = ANALYTICS_KEY_COLUMNS[analytics_family]
    null_common = None
    for name in COMMON_ANALYTICS_COLUMNS:
        missing_value = F.col(name).isNull()
        null_common = missing_value if null_common is None else (null_common | missing_value)
    if null_common is not None and df.filter(null_common).limit(1).count():
        raise ValueError(f"DataFrame {analytics_family} có common metadata null")
    duplicate = (
        df.groupBy(*[F.col(name) for name in key_columns])
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError(f"DataFrame {analytics_family} có duplicate business key")

    numeric_problem = None
    for field in df.schema.fields:
        if isinstance(field.dataType, (T.DoubleType, T.FloatType)):
            bad = F.isnan(F.col(field.name)) | (F.abs(F.col(field.name)) == float("inf"))
            numeric_problem = bad if numeric_problem is None else (numeric_problem | bad)
    if numeric_problem is not None and df.filter(numeric_problem).limit(1).count():
        raise ValueError(f"DataFrame {analytics_family} chứa NaN/Infinity")

    if analytics_family in LITERAL_ID_FAMILIES:
        with_ids = df.withColumn("_id", F.col(key_columns[0]).cast("string"))
    else:
        field_types = {field.name: field.dataType for field in df.schema.fields}

        def serialize_spark_key(row: Any) -> str:
            values: list[Any] = []
            for name, raw in zip(key_columns, row):
                if raw is not None and isinstance(field_types[name], T.TimestampType):
                    if raw.tzinfo is None:
                        raw = raw.replace(tzinfo=timezone.utc)
                    else:
                        raw = raw.astimezone(timezone.utc)
                values.append(raw)
            return serialize_key_v1(tuple(values))

        key_udf = F.udf(serialize_spark_key, T.StringType())
        with_ids = df.withColumn("_id", key_udf(F.struct(*[F.col(name) for name in key_columns])))

    row_count = with_ids.count()
    (
        with_ids.write.format(str(load_runtime_config()["spark_es_format"]))
        .mode("append")
        .option("es.resource", clean)
        .option("es.mapping.id", "_id")
        .option("es.mapping.exclude", "_id")
        .option("es.write.operation", "index")
        .option("es.batch.write.retry.count", "3")
        .save()
    )
    return DistributedWriteResult(index_name=clean, written_count=row_count)
