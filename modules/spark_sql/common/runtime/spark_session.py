"""Khởi tạo SparkSession dùng chung cho Module 07 (Spark SQL) với timezone UTC bắt buộc."""

from __future__ import annotations

import os
from urllib.parse import urlparse

from pyspark.sql import SparkSession

from modules.elasticsearch.common.config.es_client import load_runtime_config
from modules.elasticsearch.common.config.runtime_contract import validate_runtime_contract


def create_spark_sql_session(
    app_name: str = "movie-trend-spark-sql",
    master: str | None = None,
    driver_memory: str | None = None,
    shuffle_partitions: int = 4,
) -> SparkSession:
    """Tạo hoặc lấy SparkSession cấu hình UTC cố định, tối ưu cho VM 8GB RAM."""
    contract = load_runtime_config()
    validate_runtime_contract(contract)
    tz = str(contract.get("timezone", "UTC"))
    if tz != "UTC":
        raise ValueError(f"Contract Phase 2 bắt buộc timezone='UTC', nhận được {tz!r}")

    spark_master = master or os.environ.get("SPARK_MASTER", "local[2]")
    spark_mem = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "768m")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", spark_mem)
        .config("spark.ui.enabled", "false")
    )
    connector_package = os.environ.get("SPARK_ES_PACKAGE", "").strip()
    if connector_package:
        builder = builder.config("spark.jars.packages", connector_package)
    es_url = os.environ.get("ES_CLUSTER_HTTP_URL") or os.environ.get("ES_HTTP_URL")
    if es_url:
        parsed = urlparse(es_url)
        if not parsed.hostname:
            raise ValueError(f"Elasticsearch URL không hợp lệ: {es_url!r}")
        builder = builder.config("es.nodes", parsed.hostname)
        builder = builder.config("es.port", str(parsed.port or (443 if parsed.scheme == "https" else 80)))
        builder = builder.config("es.nodes.wan.only", "true")
        builder = builder.config("es.net.ssl", str(parsed.scheme == "https").lower())
    es_username = os.environ.get("ES_USERNAME")
    es_password = os.environ.get("ES_PASSWORD")
    if bool(es_username) != bool(es_password):
        raise ValueError("ES_USERNAME và ES_PASSWORD phải được cấu hình cùng nhau")
    if es_username and es_password:
        builder = builder.config("es.net.http.auth.user", es_username)
        builder = builder.config("es.net.http.auth.pass", es_password)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
