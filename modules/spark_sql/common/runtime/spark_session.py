"""Khởi tạo SparkSession dùng chung cho Module 07 (Spark SQL) với timezone UTC bắt buộc."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from modules.elasticsearch.common.config.es_client import load_runtime_config


def create_spark_sql_session(
    app_name: str = "movie-trend-spark-sql",
    master: str | None = None,
    driver_memory: str | None = None,
    shuffle_partitions: int = 4,
) -> SparkSession:
    """Tạo hoặc lấy SparkSession cấu hình UTC cố định, tối ưu cho VM 8GB RAM."""
    contract = load_runtime_config()
    tz = str(contract.get("timezone", "UTC"))
    if tz != "UTC":
        raise ValueError(f"Contract Phase 2 bắt buộc timezone='UTC', nhận được {tz!r}")

    spark_master = master or os.environ.get("SPARK_MASTER", "local[2]")
    spark_mem = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "768m")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", spark_mem)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark