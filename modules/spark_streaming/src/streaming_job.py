#!/usr/bin/env python3
"""
streaming_job.py - Entrypoint cho Module 05: Spark Structured Streaming (Speed Layer)
Owners: Thắng
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[3]
for p in (str(PROJECT_ROOT), str(CURRENT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from .es_sink import (
        bulk_upsert_elasticsearch,
        merge_window_stats,
        write_jsonl_output,
    )
    from .transforms import (
        compute_genre_realtime,
        compute_movie_realtime_stats,
        compute_tmdb_realtime,
        load_canonical_reference,
        parse_and_validate_movie_events,
    )
except ImportError:
    from es_sink import (  # type: ignore[no-redef, import-not-found]
        bulk_upsert_elasticsearch,
        merge_window_stats,
        write_jsonl_output,
    )
    from transforms import (  # type: ignore[no-redef, import-not-found]
        compute_genre_realtime,
        compute_movie_realtime_stats,
        compute_tmdb_realtime,
        load_canonical_reference,
        parse_and_validate_movie_events,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Module05-SparkSpeed) %(message)s",
)
logger = logging.getLogger("SparkSpeedLayer")


def create_spark_session(
    app_name: str = "movie-trend-speed-layer",
    master: str = "local[2]",
) -> SparkSession:
    """Khởi tạo SparkSession tối ưu cho VM 8GB RAM và bắt buộc timezone UTC."""
    
    builder = (
        SparkSession.builder.appName(app_name) # type: ignore[attr-defined]
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "768m")
        .config("spark.ui.enabled", "false")
    )
    spark_packages = os.environ.get("SPARK_KAFKA_PACKAGE", "")
    if spark_packages:
        builder = builder.config("spark.jars.packages", spark_packages)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def resolve_reference_path(custom_path: str | None = None) -> str:
    """Ưu tiên đường dẫn truyền vào -> data/prepared -> data/test_data."""
    if custom_path:
        return custom_path
    prepared = PROJECT_ROOT / "data" / "prepared" / "reference" / "movie_reference.tsv"
    if prepared.exists():
        return str(prepared)
    sample = PROJECT_ROOT / "data" / "test_data" / "movie-reference.sample.tsv"
    return str(sample)


def run_sample_mode(
    spark: SparkSession,
    test_data_dir: Path,
    es_url: str | None = None,
) -> Dict[str, int]:
    """
    Đọc trực tiếp dữ liệu mẫu tại data/test_data/ và xuất kết quả ra data/test_data/.
    """
    movie_sample_path = str(test_data_dir / "movie-events.sample.jsonl")
    tmdb_sample_path = str(test_data_dir / "tmdb-snapshots.sample.jsonl")
    ref_path = str(test_data_dir / "movie-reference.sample.tsv")

    logger.info(f"Đọc dữ liệu từ: {test_data_dir}")
    ref_df = load_canonical_reference(spark, ref_path)

    raw_movie_df = spark.read.text(movie_sample_path)
    valid_movies, movie_quarantine = parse_and_validate_movie_events(raw_movie_df)

    # Tính cả 2 cửa sổ MVP: 10m và 1h
    movie_10m = compute_movie_realtime_stats(valid_movies, "10m")
    movie_1h = compute_movie_realtime_stats(valid_movies, "1h")
    movie_stats_all = movie_10m.unionByName(movie_1h)

    genre_10m, miss_10m = compute_genre_realtime(valid_movies, ref_df, "10m")
    genre_1h, _ = compute_genre_realtime(valid_movies, ref_df, "1h")
    genre_stats_all = genre_10m.unionByName(genre_1h)

    raw_tmdb_df = spark.read.text(tmdb_sample_path)
    tmdb_stats, tmdb_quarantine = compute_tmdb_realtime(raw_tmdb_df)

    movie_records = [row.asDict() for row in movie_stats_all.collect()]
    genre_records = [row.asDict() for row in genre_stats_all.collect()]
    tmdb_records = [row.asDict() for row in tmdb_stats.collect()]

    write_jsonl_output(test_data_dir / "movie_realtime_stats.jsonl", movie_records)
    write_jsonl_output(test_data_dir / "genre_realtime.jsonl", genre_records)
    write_jsonl_output(test_data_dir / "tmdb_realtime.jsonl", tmdb_records)

    if es_url:
        bulk_upsert_elasticsearch(es_url, "movie_realtime_stats", movie_records)
        bulk_upsert_elasticsearch(es_url, "genre_realtime", genre_records)
        bulk_upsert_elasticsearch(es_url, "tmdb_realtime", tmdb_records)

    stats = {
        "valid_movie_events": valid_movies.count(),
        "movie_quarantine": movie_quarantine.count(),
        "movie_realtime_rows": len(movie_records),
        "genre_realtime_rows": len(genre_records),
        "join_miss_count": miss_10m,
        "tmdb_realtime_rows": len(tmdb_records),
        "tmdb_quarantine": tmdb_quarantine.count(),
    }
    logger.info(f"Hoàn tất chạy sample mode: {stats}")
    return stats


def run_kafka_streaming_mode(
    spark: SparkSession,
    kafka_bootstrap: str,
    ref_path: str,
    checkpoint_root: str,
    es_url: str,
    output_dir: Path,
) -> None:
    """
    Chạy Spark Structured Streaming đọc từ Kafka topics: movie-events và tmdb-snapshots.
    """
    logger.info(f"Nạp canonical reference từ: {ref_path}")
    ref_df = load_canonical_reference(spark, ref_path).cache()

    # Bộ nhớ trạng thái lũy kế xuyên qua các micro-batches (15s/30s)
    seen_movie_event_ids: set[str] = set()
    movie_window_state: Dict[str, Dict[str, Any]] = {}
    genre_window_state: Dict[str, Dict[str, Any]] = {}
    tmdb_prev_state: Dict[int, Dict[str, Any]] = {}
    tmdb_doc_state: Dict[int, Dict[str, Any]] = {}

    # 1. Đọc stream movie-events từ Kafka
    kafka_movie_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "movie-events")
        .option("startingOffsets", "earliest")
        .load()
    )

    def process_movie_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():  # type: ignore[operator]
            return
        valid_df, quarantine_df = parse_and_validate_movie_events(batch_df)

        # Dedup xuyên các micro-batches theo event_id để không cộng trùng khi replay
        if seen_movie_event_ids:
            valid_df = valid_df.filter(~F.col("event_id").isin(list(seen_movie_event_ids)))

        new_ids = [r["event_id"] for r in valid_df.select("event_id").collect()]
        if new_ids:
            seen_movie_event_ids.update(new_ids)
            m_rows_batch = []
            g_rows_batch = []
            miss_cnt = 0
            for win_label in ("10m", "1h"):
                m_stats = compute_movie_realtime_stats(valid_df, win_label)
                g_stats, miss_w = compute_genre_realtime(valid_df, ref_df, win_label)
                miss_cnt = miss_w
                m_rows_batch.extend(r.asDict() for r in m_stats.collect())
                g_rows_batch.extend(r.asDict() for r in g_stats.collect())

            # Cộng dồn (accumulate) vào trạng thái lũy kế của cửa sổ 10m & 1h theo doc_id
            updated_movie_docs = merge_window_stats(movie_window_state, m_rows_batch)
            updated_genre_docs = merge_window_stats(genre_window_state, g_rows_batch)

            write_jsonl_output(output_dir / "movie_realtime_stats.jsonl", movie_window_state.values())
            write_jsonl_output(output_dir / "genre_realtime.jsonl", genre_window_state.values())
            bulk_upsert_elasticsearch(es_url, "movie_realtime_stats", updated_movie_docs)
            bulk_upsert_elasticsearch(es_url, "genre_realtime", updated_genre_docs)
            logger.info(
                f"[Batch {batch_id}] Lũy kế {len(updated_movie_docs)} movie windows, "
                f"{len(updated_genre_docs)} genre windows (tổng active={len(movie_window_state)}, join_miss={miss_cnt})"
            )

        q_rows = [r.asDict() for r in quarantine_df.collect()]
        if q_rows:
            write_jsonl_output(
                output_dir / "quarantine" / f"movie_events_batch_{batch_id}.jsonl",
                q_rows,
                strip_doc_id=False,
            )

    movie_query = (
        kafka_movie_df.writeStream.foreachBatch(process_movie_batch)
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/movie_events")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # 2. Đọc stream tmdb-snapshots từ Kafka
    kafka_tmdb_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "tmdb-snapshots")
        .option("startingOffsets", "earliest")
        .load()
    )

    def process_tmdb_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():  # type: ignore[operator]
            return
        tmdb_stats, quarantine_df = compute_tmdb_realtime(batch_df)
        raw_rows = [r.asDict() for r in tmdb_stats.collect()]
        upserts = []

        for row in raw_rows:
            tid = int(row["tmdbId"])
            prev = tmdb_prev_state.get(tid)
            if prev is not None:
                # Bỏ qua nếu bản ghi đến muộn có event_time cũ hơn trạng thái đã lưu
                if row["event_time"] < prev["event_time"]:
                    continue
                if row["prev_popularity"] is None and row["event_time"] > prev["event_time"]:
                    row["prev_popularity"] = prev["popularity"]
                    row["popularity_delta"] = round(row["popularity"] - prev["popularity"], 4)

            tmdb_prev_state[tid] = {
                "event_time": row["event_time"],
                "popularity": row["popularity"],
            }
            tmdb_doc_state[tid] = dict(row)
            upserts.append(row)

        if upserts:
            write_jsonl_output(output_dir / "tmdb_realtime.jsonl", tmdb_doc_state.values())
            bulk_upsert_elasticsearch(es_url, "tmdb_realtime", upserts)
        logger.info(f"[TMDB Batch {batch_id}] Upserted {len(upserts)} tmdb_realtime records")

    tmdb_query = (
        kafka_tmdb_df.writeStream.foreachBatch(process_tmdb_batch)
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/tmdb_snapshots")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Spark Structured Streaming đang chạy cho cả 2 nhánh movie-events và tmdb-snapshots...")
    spark.streams.awaitAnyTermination()


def load_env_file() -> None:
    """Tự động nạp file .env tại thư mục gốc movie-trend/.env để đồng bộ với Module 01, 02, 03."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def main() -> None:
    load_env_file()
    parser = argparse.ArgumentParser(description="Module 05 - Spark Structured Streaming Speed Layer")
    parser.add_argument("--mode", choices=["sample", "kafka"], default="sample")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    )
    parser.add_argument("--ref-path", default=os.environ.get("MOVIE_REFERENCE_PATH", None))
    parser.add_argument(
        "--checkpoint-root",
        default=os.environ.get("SPARK_CHECKPOINT_ROOT", str(PROJECT_ROOT / "data" / "test_data" / "ckpt")),
    )
    parser.add_argument("--es-url", default=os.environ.get("ES_HTTP_URL", "http://localhost:9200"))
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "data" / "test_data"))
    args = parser.parse_args()

    spark = create_spark_session()
    try:
        if args.mode == "sample":
            run_sample_mode(spark, Path(args.output_dir), es_url=None)
        else:
            ref_path = resolve_reference_path(args.ref_path)
            run_kafka_streaming_mode(
                spark=spark,
                kafka_bootstrap=args.kafka_bootstrap,
                ref_path=ref_path,
                checkpoint_root=args.checkpoint_root,
                es_url=args.es_url,
                output_dir=Path(args.output_dir),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()