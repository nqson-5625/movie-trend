import hashlib

from datetime import datetime, timezone

import pytest

from modules.data_source.src.movie_event_adapter import (
    MOVIE_EVENT_FIELDS,
    build_movie_event_id,
    normalize_ingest_time,
    to_movie_event,
    unix_timestamp_to_utc_iso,
)


# ============================================================
# COMMON REALISTIC TEST ROW
# ============================================================
#
# Đây là unit-test fixture để kiểm thử logic code.
# Evidence/sample chính thức bằng dữ liệu thật sẽ được tạo ở Step 14.
# ============================================================

BASE_ROW = {
    "userId": "15",
    "movieId": "296",
    "rating": "4.5",
    "timestamp": "964982703",
}


# ============================================================
# EVENT SCHEMA / MAPPING
# ============================================================

def test_movie_row_maps_to_expected_event_fields():
    """
    Adapter phải tạo đúng toàn bộ field của movie-events contract.
    """

    event = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-05T12:00:00Z"
        ),
    )

    assert list(
        event.keys()
    ) == MOVIE_EVENT_FIELDS

    assert (
        event["schema_version"]
        == "1.0"
    )

    assert (
        event["source"]
        == "movielens"
    )

    assert event["userId"] == 15
    assert event["movieId"] == 296
    assert event["rating"] == 4.5

    assert (
        event["ingest_time"]
        == "2026-10-05T12:00:00Z"
    )


def test_output_types_are_canonical():
    """
    Kiểm tra type logic của event trước khi JSON serialize.
    """

    event = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-05T12:00:00Z"
        ),
    )

    assert isinstance(
        event["schema_version"],
        str,
    )

    assert isinstance(
        event["event_id"],
        str,
    )

    assert isinstance(
        event["source"],
        str,
    )

    assert isinstance(
        event["userId"],
        int,
    )

    assert isinstance(
        event["movieId"],
        int,
    )

    assert isinstance(
        event["rating"],
        float,
    )

    assert isinstance(
        event["event_time"],
        str,
    )

    assert isinstance(
        event["ingest_time"],
        str,
    )


# ============================================================
# DETERMINISTIC EVENT ID
# ============================================================

def test_same_source_row_produces_same_event_id():
    """
    Contract requirement:
        cùng một source row phải có cùng event_id,
        kể cả replay ở hai thời điểm khác nhau.
    """

    first = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-05T12:00:00Z"
        ),
    )

    second = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-05T13:00:00Z"
        ),
    )

    assert (
        first["event_id"]
        == second["event_id"]
    )


def test_event_id_matches_contract_formula():
    """
    Verify chính xác rule:

        sha256(
            "movielens|userId|movieId|original_timestamp"
        )
    """

    canonical_identity = (
        "movielens|15|296|964982703"
    )

    expected = hashlib.sha256(
        canonical_identity.encode(
            "utf-8"
        )
    ).hexdigest()

    actual = build_movie_event_id(
        user_id=15,
        movie_id=296,
        original_timestamp=964982703,
    )

    assert actual == expected


def test_event_id_has_sha256_hex_length():
    """
    SHA-256 hexadecimal phải dài 64 ký tự.
    """

    event_id = build_movie_event_id(
        user_id=15,
        movie_id=296,
        original_timestamp=964982703,
    )

    assert len(event_id) == 64

    # Kiểm tra toàn bộ chuỗi là hexadecimal.
    int(
        event_id,
        16,
    )


# ============================================================
# EVENT TIME
# ============================================================

def test_event_time_is_derived_from_original_timestamp():
    """
    event_time phải derive từ MovieLens timestamp,
    không phải ingest_time.
    """

    event = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-05T12:00:00Z"
        ),
    )

    expected = (
        unix_timestamp_to_utc_iso(
            964982703
        )
    )

    assert (
        event["event_time"]
        == expected
    )


def test_event_time_does_not_change_between_replays():
    """
    Contract requirement:
        replay hôm nay hay ngày mai thì event_time vẫn giữ nguyên
        vì nó là source/business time.
    """

    first = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-05T12:00:00Z"
        ),
    )

    second = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-06T18:30:00Z"
        ),
    )

    assert (
        first["event_time"]
        == second["event_time"]
    )


def test_ingest_time_can_change_between_replays():
    """
    Ngược với event_time:
        ingest_time là operational arrival time,
        nên hai lần replay có thể khác nhau.
    """

    first = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-05T12:00:00Z"
        ),
    )

    second = to_movie_event(
        BASE_ROW,
        ingest_time=(
            "2026-10-06T18:30:00Z"
        ),
    )

    assert (
        first["ingest_time"]
        != second["ingest_time"]
    )


# ============================================================
# INGEST TIME NORMALIZATION
# ============================================================

def test_ingest_time_datetime_is_converted_to_utc():
    """
    timezone-aware datetime phải được normalize về UTC.
    """

    dt = datetime(
        2026,
        10,
        5,
        19,
        0,
        0,
        tzinfo=timezone.utc,
    )

    result = normalize_ingest_time(
        dt
    )

    assert (
        result
        == "2026-10-05T19:00:00Z"
    )


def test_ingest_time_with_offset_is_converted_to_utc():
    """
    ISO timestamp có offset phải convert chính xác về UTC.
    """

    result = normalize_ingest_time(
        "2026-10-05T19:00:00+07:00"
    )

    assert (
        result
        == "2026-10-05T12:00:00Z"
    )


def test_ingest_time_without_timezone_is_rejected():
    """
    Không tự đoán timezone cho ingest_time.
    """

    with pytest.raises(
        ValueError
    ):
        normalize_ingest_time(
            "2026-10-05T19:00:00"
        )


# ============================================================
# INPUT VALIDATION
# ============================================================

def test_missing_user_id_is_rejected():

    row = BASE_ROW.copy()
    row["userId"] = ""

    with pytest.raises(
        ValueError
    ):
        to_movie_event(
            row,
            ingest_time=(
                "2026-10-05T12:00:00Z"
            ),
        )


def test_invalid_movie_id_is_rejected():

    row = BASE_ROW.copy()
    row["movieId"] = "0"

    with pytest.raises(
        ValueError
    ):
        to_movie_event(
            row,
            ingest_time=(
                "2026-10-05T12:00:00Z"
            ),
        )


def test_rating_below_contract_range_is_rejected():

    row = BASE_ROW.copy()
    row["rating"] = "0.0"

    with pytest.raises(
        ValueError
    ):
        to_movie_event(
            row,
            ingest_time=(
                "2026-10-05T12:00:00Z"
            ),
        )


def test_rating_above_contract_range_is_rejected():

    row = BASE_ROW.copy()
    row["rating"] = "5.5"

    with pytest.raises(
        ValueError
    ):
        to_movie_event(
            row,
            ingest_time=(
                "2026-10-05T12:00:00Z"
            ),
        )


def test_invalid_timestamp_is_rejected():

    row = BASE_ROW.copy()
    row["timestamp"] = "not-a-timestamp"

    with pytest.raises(
        ValueError
    ):
        to_movie_event(
            row,
            ingest_time=(
                "2026-10-05T12:00:00Z"
            ),
        )


def test_missing_source_field_is_rejected():

    row = {
        "userId": "15",
        "movieId": "296",
        "rating": "4.5",
        # timestamp intentionally missing
    }

    with pytest.raises(
        ValueError
    ):
        to_movie_event(
            row,
            ingest_time=(
                "2026-10-05T12:00:00Z"
            ),
        )