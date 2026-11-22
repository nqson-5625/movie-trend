"""Unit tests cho key_serializer v1 và SparkSession runtime UTC."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

import pytest

from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.key_serializer import (
    build_record_id,
    serialize_key_v1,
)


def test_deterministic_and_hex_format() -> None:
    k1 = serialize_key_v1(("20261024T104145Z", date(2017, 5, 3), 356))
    k2 = serialize_key_v1(("20261024T104145Z", date(2017, 5, 3), 356))
    assert k1 == k2
    assert len(k1) == 64


def test_distinguishes_type_null_order_and_delimiters() -> None:
    assert serialize_key_v1((356,)) != serialize_key_v1(("356",))
    assert serialize_key_v1((None,)) != serialize_key_v1(("null",))
    assert serialize_key_v1((None,)) != serialize_key_v1(("None",))
    assert serialize_key_v1((True,)) != serialize_key_v1((1,))
    assert serialize_key_v1(("A", "B")) != serialize_key_v1(("B", "A"))
    assert serialize_key_v1(("a", "b_c")) != serialize_key_v1(("a_b", "c"))


def test_diacritics_and_utc_timestamp_normalization() -> None:
    assert serialize_key_v1(("Sci-Fi", "L\u00e9on")) == serialize_key_v1(("Sci-Fi", "Le\u0301on"))

    dt_utc = datetime(2017, 5, 3, 21, 0, 0, tzinfo=timezone.utc)
    dt_vn = datetime(2017, 5, 4, 4, 0, 0, tzinfo=timezone(timedelta(hours=7)))
    assert serialize_key_v1((dt_utc,)) == serialize_key_v1((dt_vn,))


def test_rejects_empty_nan_and_naive_datetime() -> None:
    with pytest.raises(ValueError):
        serialize_key_v1(())
    with pytest.raises(ValueError):
        serialize_key_v1((math.nan,))
    with pytest.raises(ValueError):
        serialize_key_v1((math.inf,))
    with pytest.raises(ValueError, match="tzinfo"):
        serialize_key_v1((datetime(2017, 5, 3, 21, 0, 0),))


def test_build_record_id_for_analytics_timeseries_all_row() -> None:
    row = {
        "batch_run_id": "20261024T104145Z",
        "period_type": "all",
        "period_start": None,
        "entity_type": "all",
        "movieId": None,
    }
    cols = ("batch_run_id", "period_type", "period_start", "entity_type", "movieId")
    assert build_record_id(row, cols) == serialize_key_v1(
        ("20261024T104145Z", "all", None, "all", None)
    )


def test_spark_session_forces_utc() -> None:
    spark = create_spark_sql_session(app_name="test-spark-sql-utc")
    try:
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    finally:
        spark.stop()
