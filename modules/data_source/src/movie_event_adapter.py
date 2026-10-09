from __future__ import annotations

import hashlib
import math

from datetime import datetime, timezone
from typing import Any


# ============================================================
# CONSTANTS THEO DATA CONTRACT
# ============================================================

SCHEMA_VERSION = "1.0"
SOURCE = "movielens"

MOVIE_EVENT_FIELDS = [
    "schema_version",
    "event_id",
    "source",
    "userId",
    "movieId",
    "rating",
    "event_time",
    "ingest_time",
]


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _get_row_value(
    row: Any,
    field: str,
) -> Any:
    """
    Lấy một field từ source row.

    Hỗ trợ các object kiểu:
        - dict
        - pandas.Series
        - object có attribute tương ứng

    Ví dụ:
        row["movieId"]
        hoặc
        row.movieId

    Hàm này giúp adapter không phụ thuộc trực tiếp vào pandas.
    """

    # dict, pandas.Series và object hỗ trợ [].
    try:
        return row[field]

    except (KeyError, TypeError, IndexError):
        pass

    # Fallback cho object có attribute.
    if hasattr(row, field):
        return getattr(
            row,
            field,
        )

    raise ValueError(
        f"Missing required field: {field}"
    )


def _normalize_required_int(
    value: Any,
    field_name: str,
) -> int:
    """
    Chuẩn hóa field bắt buộc thành integer > 0.

    Dùng cho:
        userId
        movieId

    Không sửa source row; chỉ convert giá trị trong event output.
    """

    if value is None:
        raise ValueError(
            f"{field_name} is required"
        )

    text = str(value).strip()

    if not text:
        raise ValueError(
            f"{field_name} is required"
        )

    # MovieLens IDs trong source là integer.
    # Không chấp nhận "1.0" để tránh tự sửa dữ liệu source.
    if not text.isdigit():
        raise ValueError(
            f"Invalid {field_name}: {value!r}"
        )

    result = int(text)

    if result <= 0:
        raise ValueError(
            f"{field_name} must be > 0: "
            f"{value!r}"
        )

    return result


def _normalize_rating(
    value: Any,
) -> float:
    """
    Convert MovieLens rating thành float.

    Contract của project:
        0.5 <= rating <= 5.0

    Step 5 đã profile dữ liệu này, nhưng adapter vẫn validate
    lại để tránh gửi event hỏng sang Kafka.
    """

    if value is None:
        raise ValueError(
            "rating is required"
        )

    text = str(value).strip()

    if not text:
        raise ValueError(
            "rating is required"
        )

    try:
        rating = float(text)

    except ValueError as exc:
        raise ValueError(
            f"Invalid rating: {value!r}"
        ) from exc

    if not math.isfinite(rating) or rating < 0.5 or rating > 5.0:
        raise ValueError(
            "rating must be between "
            f"0.5 and 5.0: {value!r}"
        )

    return rating


def _normalize_original_timestamp(
    value: Any,
) -> int:
    """
    Chuẩn hóa MovieLens source timestamp thành integer epoch seconds.

    Đây là timestamp GỐC của dataset.

    Nó được dùng cho cả:
        - event_time
        - deterministic event_id

    Không được thay bằng ingest_time/current time.
    """

    if value is None:
        raise ValueError(
            "timestamp is required"
        )

    text = str(value).strip()

    if not text:
        raise ValueError(
            "timestamp is required"
        )

    # MovieLens timestamp là Unix seconds dạng integer.
    # Không tự sửa các dạng như "964982703.0".
    try:
        timestamp = int(text)

    except ValueError as exc:
        raise ValueError(
            f"Invalid timestamp: {value!r}"
        ) from exc

    return timestamp


def unix_timestamp_to_utc_iso(
    timestamp: int,
) -> str:
    """
    Chuyển Unix epoch seconds của MovieLens sang UTC ISO-8601.

    Ví dụ:
        964982703
            ->
        "2000-07-30T18:45:03Z"

    event_time được derive hoàn toàn từ source timestamp,
    do đó cùng một source row luôn có cùng event_time dù replay
    vào thời điểm nào.
    """

    try:
        dt = datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        )

    except (
        OverflowError,
        OSError,
        ValueError,
    ) as exc:

        raise ValueError(
            "timestamp cannot be converted "
            f"to UTC datetime: {timestamp}"
        ) from exc

    return (
        dt
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def normalize_ingest_time(
    value: str | datetime,
) -> str:
    """
    Validate và chuẩn hóa ingest_time do Module 2 truyền vào.

    Chấp nhận:
        - timezone-aware datetime
        - ISO-8601 string có timezone
        - ISO-8601 string kết thúc bằng Z

    Output luôn là UTC ISO-8601:
        YYYY-MM-DDTHH:MM:SSZ

    Quan trọng:
        Hàm này KHÔNG tự gọi datetime.now().
        Module 2 phải quyết định thời điểm ingest thực tế.
    """

    if value is None:
        raise ValueError(
            "ingest_time must be provided "
            "by the producer"
        )

    if isinstance(value, datetime):

        dt = value

    else:

        text = str(value).strip()

        if not text:
            raise ValueError(
                "ingest_time must not be empty"
            )

        # Python fromisoformat không dùng trực tiếp "Z"
        # ở một số version, nên canonicalize thành +00:00.
        if text.endswith("Z"):
            text = (
                text[:-1]
                + "+00:00"
            )

        try:
            dt = datetime.fromisoformat(
                text
            )

        except ValueError as exc:
            raise ValueError(
                "ingest_time must be a valid "
                f"ISO-8601 timestamp: {value!r}"
            ) from exc

    # ingest_time phải có timezone.
    # Không đoán timezone cho naive datetime.
    if dt.tzinfo is None:

        raise ValueError(
            "ingest_time must include timezone"
        )

    dt_utc = dt.astimezone(
        timezone.utc
    )

    return (
        dt_utc
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def build_movie_event_id(
    user_id: int,
    movie_id: int,
    original_timestamp: int,
) -> str:
    """
    Tạo deterministic event_id theo rule trong Module 1:

        sha256(
            "movielens|userId|movieId|original_timestamp"
        )

    Ví dụ canonical input trước khi hash:

        movielens|15|296|1234567890

    Rating và ingest_time KHÔNG tham gia event_id vì contract
    hiện tại chốt identity theo source/user/movie/timestamp.

    Kết quả:
        chuỗi SHA-256 hexadecimal 64 ký tự.
    """

    identity = (
        f"{SOURCE}|"
        f"{user_id}|"
        f"{movie_id}|"
        f"{original_timestamp}"
    )

    return hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()


# ============================================================
# PUBLIC ADAPTER
# ============================================================

def to_movie_event(
    row: Any,
    ingest_time: str | datetime,
) -> dict:
    """
    Chuyển một MovieLens ratings row thành `movie-events` record.

    Đây là API chính mà Module 2 sẽ reuse.

    Parameters
    ----------
    row:
        Một record chứa:
            userId
            movieId
            rating
            timestamp

        Có thể là dict hoặc pandas.Series.

    ingest_time:
        Thời điểm event thực sự được producer đưa vào Kafka.
        Module 2 phải truyền giá trị này vào.

    Returns
    -------
    dict:
        Event đúng logical contract:

        {
            "schema_version": "1.0",
            "event_id": "...",
            "source": "movielens",
            "userId": 15,
            "movieId": 296,
            "rating": 4.5,
            "event_time": "...Z",
            "ingest_time": "...Z"
        }

    Important
    ---------
    - event_time lấy từ timestamp GỐC MovieLens.
    - ingest_time lấy từ producer.
    - event_id deterministic, không phụ thuộc ingest_time.
    """

    # --------------------------------------------------------
    # 1. Đọc source fields
    # --------------------------------------------------------

    raw_user_id = _get_row_value(
        row,
        "userId",
    )

    raw_movie_id = _get_row_value(
        row,
        "movieId",
    )

    raw_rating = _get_row_value(
        row,
        "rating",
    )

    raw_timestamp = _get_row_value(
        row,
        "timestamp",
    )

    # --------------------------------------------------------
    # 2. Validate + normalize source values
    # --------------------------------------------------------

    user_id = _normalize_required_int(
        raw_user_id,
        "userId",
    )

    movie_id = _normalize_required_int(
        raw_movie_id,
        "movieId",
    )

    rating = _normalize_rating(
        raw_rating
    )

    original_timestamp = (
        _normalize_original_timestamp(
            raw_timestamp
        )
    )

    # --------------------------------------------------------
    # 3. Business/source event time
    # --------------------------------------------------------

    event_time = (
        unix_timestamp_to_utc_iso(
            original_timestamp
        )
    )

    # --------------------------------------------------------
    # 4. Operational ingest time
    # --------------------------------------------------------

    normalized_ingest_time = (
        normalize_ingest_time(
            ingest_time
        )
    )

    # --------------------------------------------------------
    # 5. Deterministic identity
    # --------------------------------------------------------

    event_id = build_movie_event_id(
        user_id=user_id,
        movie_id=movie_id,
        original_timestamp=(
            original_timestamp
        ),
    )

    # --------------------------------------------------------
    # 6. Output đúng field order của contract
    # --------------------------------------------------------

    event = {
        "schema_version":
            SCHEMA_VERSION,

        "event_id":
            event_id,

        "source":
            SOURCE,

        "userId":
            user_id,

        "movieId":
            movie_id,

        "rating":
            rating,

        "event_time":
            event_time,

        "ingest_time":
            normalized_ingest_time,
    }

    return event
