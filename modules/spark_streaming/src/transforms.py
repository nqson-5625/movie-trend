"""
transforms.py - Core PySpark transformations cho Module 05 (Spark Structured Streaming)
Tuân thủ DATA_CONTRACT.md Mục 5 (event_time UTC), Mục 8 (movie_reference), Mục 11 (Realtime Views).
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, Tuple
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
# 1. Schema đầu vào theo contracts/schemas/
MOVIE_EVENT_SCHEMA = T.StructType([
    T.StructField("schema_version", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("userId", T.LongType(), True),
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("event_time", T.StringType(), True),
    T.StructField("ingest_time", T.StringType(), True),
])
TMDB_SNAPSHOT_SCHEMA = T.StructType([
    T.StructField("schema_version", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("tmdbId", T.IntegerType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("popularity", T.DoubleType(), True),
    T.StructField("vote_count", T.LongType(), True),
    T.StructField("vote_average", T.DoubleType(), True),
    T.StructField("event_time", T.StringType(), True),
    T.StructField("ingest_time", T.StringType(), True),
])
MOVIE_REFERENCE_SCHEMA = T.StructType([
    T.StructField("movieId", T.IntegerType(), False),
    T.StructField("title", T.StringType(), False),
    T.StructField("release_year", T.IntegerType(), True),
    T.StructField("genres", T.StringType(), False),
    T.StructField("imdbId", T.StringType(), True),
    T.StructField("tmdbId", T.IntegerType(), True),
    T.StructField("imdb_rating", T.DoubleType(), True),
    T.StructField("imdb_votes", T.LongType(), True),
    T.StructField("ref_version", T.StringType(), False),
    T.StructField("generated_at", T.StringType(), False),
])

WINDOW_SPECS: Dict[str, str] = {
    "10m": "10 minutes",
    "1h": "1 hour",
}

def load_canonical_reference(spark: SparkSession, ref_path: str) -> DataFrame:
    """
    Đọc bảng chuẩn movie_reference.tsv (Mục 8.1 DATA_CONTRACT.md).
    Hỗ trợ CSV quoting và giữ nguyên genres phân tách bằng '|'.
    """
    return (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "")
        .schema(MOVIE_REFERENCE_SCHEMA)
        .load(ref_path)
        .select("movieId", "title", "genres", "tmdbId")
    )

def parse_and_validate_movie_events(raw_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Parse JSON payload từ cột 'value' và kiểm tra Data Quality Contract (Mục 15.1).
    Trả về: (valid_events_df, quarantine_df)
    """
    parsed = raw_df.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.from_json(F.col("value").cast("string"), MOVIE_EVENT_SCHEMA).alias("data"),
    )
    with_ts = parsed.select(
        "raw_payload",
        "data.*",
        F.to_timestamp(F.col("data.event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_ts"),
        F.to_timestamp(F.col("data.ingest_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ingest_ts"),
    )
    is_valid = (
        (F.col("schema_version") == F.lit("1.0"))
        & (F.col("source") == F.lit("movielens"))
        & F.col("event_id").rlike("^[0-9a-f]{64}$")
        & F.col("userId").isNotNull() & (F.col("userId") > 0)
        & F.col("movieId").isNotNull() & (F.col("movieId") > 0)
        & F.col("rating").isNotNull() & (F.col("rating") >= 0.5) & (F.col("rating") <= 5.0)
        & F.col("event_ts").isNotNull()
        & F.col("ingest_ts").isNotNull()
    )
    valid_df = (
        with_ts.filter(is_valid)
        .drop("raw_payload")
        .dropDuplicates(["event_id"])  # Dedup theo event_id (Mục 16.1)
    )
    quarantine_df = (
        with_ts.filter(~is_valid)
        .select(
            F.coalesce(F.col("event_id"), F.lit("UNKNOWN")).alias("event_id"),
            F.lit("INVALID_MOVIE_EVENT_SCHEMA_OR_VALUE").alias("reason"),
            F.col("raw_payload"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("detected_at"),
        )
    )
    return valid_df, quarantine_df

def compute_movie_realtime_stats(
    valid_events_df: DataFrame,
    window_size_label: str = "10m",
) -> DataFrame:
    """
    Tính bảng movie_realtime_stats theo Mục 11.2 DATA_CONTRACT.md.
    - Bắt buộc dùng event_ts (từ event_time) làm cửa sổ nghiệp vụ.
    - Tạo doc_id = movieId|window_start|window_size để upsert idempotent vào Elasticsearch.
    """
    spark_window_duration = WINDOW_SPECS[window_size_label]
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    agg_df = (
        valid_events_df.groupBy(
            F.window(F.col("event_ts"), spark_window_duration).alias("win"),
            F.col("movieId"),
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("interaction_count"),
            F.count(F.col("rating")).cast("long").alias("rating_count"),
            F.round(F.sum(F.col("rating")), 4).cast("double").alias("rating_sum"),
        )
        .withColumn("avg_rating", F.round(F.col("rating_sum") / F.col("rating_count"), 4))
        .withColumn(
            "window_start",
            F.date_format(F.col("win.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn(
            "window_end",
            F.date_format(F.col("win.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("window_size", F.lit(window_size_label))
        .withColumn("updated_at", F.lit(now_utc_str))
        .withColumn(
            "doc_id",
            F.concat_ws("|", F.col("movieId").cast("string"), F.col("window_start"), F.col("window_size")),
        )
        .select(
            "doc_id",
            "window_start",
            "window_end",
            "window_size",
            "movieId",
            "interaction_count",
            "rating_count",
            "rating_sum",
            "avg_rating",
            "updated_at",
        )
    )
    return agg_df

def compute_genre_realtime(
    valid_events_df: DataFrame,
    ref_df: DataFrame,
    window_size_label: str = "10m",
) -> Tuple[DataFrame, int]:
    """
    Tính bảng genre_realtime theo Mục 11.3 & 17.2 DATA_CONTRACT.md:
    - Static LEFT JOIN với canonical movie_reference.
    - Nếu join miss (movieId không có trong ref), gán genre = 'UNKNOWN' và đếm join_miss_count.
    - Explode chuỗi genres phân tách bởi '|'.
    """
    spark_window_duration = WINDOW_SPECS[window_size_label]
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    joined = valid_events_df.join(
        F.broadcast(ref_df.select("movieId", "genres")),
        on="movieId",
        how="left",
    )
    join_miss_count = joined.filter(F.col("genres").isNull()).count()
    exploded = (
        joined.withColumn(
            "clean_genres",
            F.when(
                F.col("genres").isNull() | (F.col("genres") == "") | (F.col("genres") == "(no genres listed)"),
                F.lit("UNKNOWN"),
            ).otherwise(F.col("genres")),
        )
        .withColumn("genre", F.explode(F.split(F.col("clean_genres"), "\\|")))
    )
    genre_agg = (
        exploded.groupBy(
            F.window(F.col("event_ts"), spark_window_duration).alias("win"),
            F.col("genre"),
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("interaction_count"),
            F.count(F.col("rating")).cast("long").alias("rating_count"),
            F.round(F.sum(F.col("rating")), 4).cast("double").alias("rating_sum"),
        )
        .withColumn("avg_rating", F.round(F.col("rating_sum") / F.col("rating_count"), 4))
        .withColumn(
            "window_start",
            F.date_format(F.col("win.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn(
            "window_end",
            F.date_format(F.col("win.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("window_size", F.lit(window_size_label))
        .withColumn("updated_at", F.lit(now_utc_str))
        .withColumn(
            "doc_id",
            F.concat_ws("|", F.col("genre"), F.col("window_start"), F.col("window_size")),
        )
        .select(
            "doc_id",
            "window_start",
            "window_end",
            "window_size",
            "genre",
            "interaction_count",
            "rating_count",
            "rating_sum",
            "avg_rating",
            "updated_at",
        )
    )
    return genre_agg, join_miss_count

def compute_tmdb_realtime(raw_tmdb_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Xử lý luồng tmdb-snapshots sang tmdb_realtime theo Mục 11.4 & 15.2 DATA_CONTRACT.md:
    - Giữ snapshot mới nhất theo event_time cho mỗi tmdbId.
    - Tính prev_popularity và popularity_delta = popularity - prev_popularity.
    - Document ID = tmdbId.
    """
    parsed = raw_tmdb_df.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.from_json(F.col("value").cast("string"), TMDB_SNAPSHOT_SCHEMA).alias("data"),
    )
    with_ts = parsed.select(
        "raw_payload",
        "data.*",
        F.to_timestamp(F.col("data.event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_ts"),
        F.to_timestamp(F.col("data.ingest_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ingest_ts"),
    )
    is_valid = (
        (F.col("schema_version") == F.lit("1.0"))
        & (F.col("source") == F.lit("tmdb"))
        & F.col("event_id").rlike("^[0-9a-f]{64}$")
        & F.col("tmdbId").isNotNull() & (F.col("tmdbId") > 0)
        & F.col("title").isNotNull() & (F.length(F.trim(F.col("title"))) > 0)
        & F.col("popularity").isNotNull() & (F.col("popularity") >= 0.0)
        & F.col("vote_count").isNotNull() & (F.col("vote_count") >= 0)
        & F.col("vote_average").isNotNull() & (F.col("vote_average") >= 0.0) & (F.col("vote_average") <= 10.0)
        & F.col("event_ts").isNotNull()
    )
    valid_df = with_ts.filter(is_valid).drop("raw_payload").dropDuplicates(["event_id"])
    quarantine_df = (
        with_ts.filter(~is_valid)
        .select(
            F.coalesce(F.col("event_id"), F.lit("UNKNOWN")).alias("event_id"),
            F.lit("INVALID_TMDB_SNAPSHOT_SCHEMA_OR_VALUE").alias("reason"),
            F.col("raw_payload"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("detected_at"),
        )
    )
    w_asc = Window.partitionBy("tmdbId").orderBy(F.col("event_ts").asc())
    w_desc = Window.partitionBy("tmdbId").orderBy(F.col("event_ts").desc())
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with_delta = (
        valid_df.withColumn("prev_popularity", F.lag("popularity", 1).over(w_asc))
        .withColumn(
            "popularity_delta",
            F.when(
                F.col("prev_popularity").isNotNull(),
                F.round(F.col("popularity") - F.col("prev_popularity"), 4),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("rn", F.row_number().over(w_desc))
        .filter(F.col("rn") == 1)
        .withColumn("doc_id", F.col("tmdbId").cast("string"))
        .withColumn("updated_at", F.lit(now_utc_str))
        .select(
            "doc_id",
            "tmdbId",
            "event_time",
            "popularity",
            "vote_count",
            "vote_average",
            "prev_popularity",
            "popularity_delta",
            "updated_at",
        )
    )
    return with_delta, quarantine_df