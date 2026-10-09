from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


# ============================================================
# CONTRACT CONSTANTS
# ============================================================

SCHEMA_VERSION = "1.0"
SOURCE = "tmdb"

TMDB_SNAPSHOT_FIELDS = [
    "schema_version",
    "event_id",
    "source",
    "tmdbId",
    "title",
    "popularity",
    "vote_count",
    "vote_average",
    "event_time",
    "ingest_time",
]


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

load_dotenv(
    PROJECT_ROOT / ".env"
)

PREPARED_TMDB_DIR = (
    PROJECT_ROOT
    / "data"
    / "prepared"
    / "tmdb"
)


# ============================================================
# TMDB API CONFIG
# ============================================================
#
# MODULE_01 does not lock one exact TMDB endpoint.
# For this implementation we use:
#
#     /trending/movie/day
#
# because the project analyzes movie trends.
# ============================================================

TMDB_BASE_URL = (
    "https://api.themoviedb.org/3"
)
TMDB_ALTERNATE_BASE_URL = "https://api.tmdb.org/3"

TMDB_TRENDING_ENDPOINT = (
    "/trending/movie/day"
)

DEFAULT_TIMEOUT_SECONDS = 20


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now() -> str:
    """
    Trả về thời gian hiện tại theo UTC ISO-8601.

    Ví dụ:
        2026-10-05T16:00:00Z

    Dùng để đánh dấu thời điểm snapshot hoặc thời điểm
    local adapter xử lý record.
    """

    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def normalize_utc_iso(
    value: str | datetime,
) -> str:
    """
    Validate một timestamp và chuẩn hóa về UTC ISO-8601.

    Input hợp lệ:
        "2026-10-05T16:00:00Z"
        "2026-10-05T23:00:00+07:00"
        timezone-aware datetime

    Output:
        "2026-10-05T16:00:00Z"

    Không chấp nhận datetime không có timezone.
    """

    if value is None:
        raise ValueError(
            "timestamp must not be null"
        )

    if isinstance(value, datetime):

        dt = value

    else:

        text = str(value).strip()

        if not text:
            raise ValueError(
                "timestamp must not be empty"
            )

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
                "Invalid ISO-8601 timestamp: "
                f"{value!r}"
            ) from exc

    if dt.tzinfo is None:

        raise ValueError(
            "timestamp must include timezone"
        )

    dt = dt.astimezone(
        timezone.utc
    )

    return (
        dt
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def timestamp_for_filename(
    event_time: str,
) -> str:
    """
    Chuyển ISO timestamp thành dạng an toàn cho filename.

    Ví dụ:
        2026-10-05T16:00:00Z

    thành:
        20261005T160000Z
    """

    normalized = normalize_utc_iso(
        event_time
    )

    dt = datetime.fromisoformat(
        normalized.replace(
            "Z",
            "+00:00",
        )
    )

    return dt.strftime(
        "%Y%m%dT%H%M%SZ"
    )


# ============================================================
# VALUE VALIDATION HELPERS
# ============================================================

def _require_positive_int(
    value: Any,
    field_name: str,
) -> int:
    """
    Validate field bắt buộc là integer > 0.

    Dùng cho:
        tmdbId
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

    try:
        numeric = int(text)

    except ValueError as exc:
        raise ValueError(
            f"Invalid {field_name}: {value!r}"
        ) from exc

    if numeric <= 0:
        raise ValueError(
            f"{field_name} must be > 0: "
            f"{value!r}"
        )

    return numeric


def _require_non_empty_string(
    value: Any,
    field_name: str,
) -> str:
    """
    Validate field text bắt buộc không được null/rỗng.

    Dùng cho:
        title
    """

    if value is None:
        raise ValueError(
            f"{field_name} is required"
        )

    result = str(value).strip()

    if not result:
        raise ValueError(
            f"{field_name} must not be empty"
        )

    return result


def _require_float(
    value: Any,
    field_name: str,
) -> float:
    """
    Validate và convert một field numeric thành float.

    Dùng cho:
        popularity
        vote_average
    """

    if value is None:
        raise ValueError(
            f"{field_name} is required"
        )

    try:
        if isinstance(value, bool):
            raise ValueError("Boolean is not a numeric field")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("Numeric fields must be finite")
        return result

    except (
        TypeError,
        ValueError,
    ) as exc:

        raise ValueError(
            f"Invalid {field_name}: {value!r}"
        ) from exc


def _require_non_negative_int(
    value: Any,
    field_name: str,
) -> int:
    """
    Validate integer >= 0.

    Dùng cho:
        vote_count
    """

    if value is None:
        raise ValueError(
            f"{field_name} is required"
        )

    try:
        result = int(str(value).strip())

    except (
        TypeError,
        ValueError,
    ) as exc:

        raise ValueError(
            f"Invalid {field_name}: {value!r}"
        ) from exc

    if result < 0:

        raise ValueError(
            f"{field_name} must be >= 0: "
            f"{value!r}"
        )

    return result


def _validate_vote_average(
    value: Any,
) -> float:
    """
    Validate vote_average.

    Implementation guardrail:
        0.0 <= vote_average <= 10.0

    Đây là validation kỹ thuật cho dữ liệu nhận từ TMDB.
    """

    result = _require_float(
        value,
        "vote_average",
    )

    if result < 0.0 or result > 10.0:

        raise ValueError(
            "vote_average must be between "
            f"0 and 10: {value!r}"
        )

    return result


# ============================================================
# EVENT ID
# ============================================================

def build_tmdb_event_id(
    tmdb_id: int,
    event_time: str,
) -> str:
    """
    Tạo deterministic event_id theo rule của Module 1:

        sha256(
            "tmdb|tmdbId|event_time"
        )

    Ví dụ canonical identity:

        tmdb|550|2026-10-05T16:00:00Z

    Cùng:
        tmdbId + event_time

    thì luôn tạo cùng event_id.
    """

    normalized_event_time = (
        normalize_utc_iso(
            event_time
        )
    )

    identity = (
        f"{SOURCE}|"
        f"{tmdb_id}|"
        f"{normalized_event_time}"
    )

    return hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()


# ============================================================
# TMDB ITEM NORMALIZER
# ============================================================

def normalize_tmdb_item(
    item: dict,
    event_time: str | datetime,
    ingest_time: str | datetime,
) -> dict:
    """
    Public adapter chính của Step 13.

    Chuyển một TMDB API item thành record
    đúng logical schema `tmdb-snapshots`.

    TMDB input fields:
        id
        title
        popularity
        vote_count
        vote_average

    Output:
        schema_version
        event_id
        source
        tmdbId
        title
        popularity
        vote_count
        vote_average
        event_time
        ingest_time

    event_time:
        thời điểm snapshot được lấy từ TMDB.

    ingest_time:
        thời điểm record được adapter/producer ingest.

        Khi Module 2 gửi Kafka thật, Module 2 phải truyền
        ingest_time thực tế ngay trước lúc gửi event.
    """

    if not isinstance(item, dict):

        raise ValueError(
            "TMDB item must be a dict"
        )

    # --------------------------------------------------------
    # 1. Normalize source values
    # --------------------------------------------------------

    tmdb_id = _require_positive_int(
        item.get("id"),
        "tmdbId",
    )

    title = _require_non_empty_string(
        item.get("title"),
        "title",
    )

    popularity = _require_float(
        item.get("popularity"),
        "popularity",
    )
    if popularity < 0:
        raise ValueError("popularity must be >= 0")

    vote_count = (
        _require_non_negative_int(
            item.get("vote_count"),
            "vote_count",
        )
    )

    vote_average = (
        _validate_vote_average(
            item.get("vote_average")
        )
    )

    # --------------------------------------------------------
    # 2. Normalize time
    # --------------------------------------------------------

    normalized_event_time = (
        normalize_utc_iso(
            event_time
        )
    )

    normalized_ingest_time = (
        normalize_utc_iso(
            ingest_time
        )
    )

    # --------------------------------------------------------
    # 3. Deterministic event identity
    # --------------------------------------------------------

    event_id = build_tmdb_event_id(
        tmdb_id=tmdb_id,
        event_time=normalized_event_time,
    )

    # --------------------------------------------------------
    # 4. Build exact contract output
    # --------------------------------------------------------

    return {
        "schema_version":
            SCHEMA_VERSION,

        "event_id":
            event_id,

        "source":
            SOURCE,

        "tmdbId":
            tmdb_id,

        "title":
            title,

        "popularity":
            popularity,

        "vote_count":
            vote_count,

        "vote_average":
            vote_average,

        "event_time":
            normalized_event_time,

        "ingest_time":
            normalized_ingest_time,
    }


# ============================================================
# TMDB HTTP FETCH
# ============================================================

def fetch_trending_movies(
    api_key: str,
    timeout_seconds: int = (
        DEFAULT_TIMEOUT_SECONDS
    ),
) -> tuple[list[dict], str]:
    """
    Lấy một snapshot từ TMDB Trending Movies API.

    Nếu tên miền chính lỗi kết nối, thử tên miền thay thế.
    TMDB_BASE_URL trong .env cho phép chọn trực tiếp tên miền.

    Returns:
        (
            raw_items,
            snapshot_event_time
        )

    snapshot_event_time được lấy ngay sau khi HTTP response
    thành công và dùng chung cho toàn bộ record của snapshot.

    Hàm này KHÔNG normalize event.
    Nó chỉ chịu trách nhiệm fetch source data.
    """

    api_key = (api_key or "").strip()

    if not api_key:

        raise RuntimeError(
            "TMDB_API_KEY is missing"
        )

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")

    configured_url = os.getenv("TMDB_BASE_URL", "").strip().rstrip("/")
    allowed_urls = (TMDB_BASE_URL, TMDB_ALTERNATE_BASE_URL)
    if configured_url and configured_url not in allowed_urls:
        raise RuntimeError(
            "TMDB_BASE_URL must be https://api.themoviedb.org/3 "
            "or https://api.tmdb.org/3"
        )

    base_urls = (configured_url,) if configured_url else allowed_urls
    for index, base_url in enumerate(base_urls):
        try:
            response = requests.get(
                base_url + TMDB_TRENDING_ENDPOINT,
                params={"api_key": api_key},
                headers={"accept": "application/json"},
                timeout=timeout_seconds,
                # Credentials must not follow a redirect to another host.
                allow_redirects=False,
            )
        except requests.exceptions.SSLError:
            raise RuntimeError(
                "TMDB TLS certificate verification failed. Check your "
                "system clock, CA certificates and HTTPS proxy."
            ) from None
        except (requests.ConnectionError, requests.Timeout):
            if index + 1 < len(base_urls):
                print(
                    "[RETRY] TMDB connection failed; trying api.tmdb.org.",
                    file=sys.stderr,
                )
                continue
            raise RuntimeError(
                "Cannot connect to TMDB (DNS, connection reset or timeout). "
                "Check your network and HTTPS_PROXY configuration."
            ) from None
        except requests.RequestException:
            # Request exceptions can contain the API key in their URL.
            raise RuntimeError("TMDB HTTP request failed.") from None

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"TMDB HTTP {response.status_code}: authentication/access "
                "denied. Check TMDB_API_KEY (API Key v3) in .env."
            )
        if response.status_code == 429:
            raise RuntimeError(
                "TMDB HTTP 429: rate limit exceeded. Wait before running again."
            )
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"TMDB HTTP {response.status_code}: request failed.")

        try:
            payload = response.json()
        except ValueError:
            raise RuntimeError("TMDB response is not valid JSON.") from None
        break

    if not isinstance(payload, dict):
        raise RuntimeError("TMDB response must be a JSON object.")

    results = payload.get(
        "results"
    )

    if not isinstance(
        results,
        list,
    ):

        raise RuntimeError(
            "TMDB response does not contain "
            "a valid results list"
        )

    if not results:

        raise RuntimeError(
            "TMDB returned an empty snapshot"
        )

    snapshot_event_time = utc_now()

    return (
        results,
        snapshot_event_time,
    )


# ============================================================
# SNAPSHOT BUILDER
# ============================================================

def build_snapshot_records(
    raw_items: list[dict],
    event_time: str | datetime,
    ingest_time: str | datetime,
) -> list[dict]:
    """
    Normalize toàn bộ TMDB items thành contract records.

    Guardrail:
        cùng một snapshot không được chứa duplicate tmdbId.

    Không silently deduplicate vì duplicate source data
    cần được phát hiện thay vì tự sửa.
    """

    records = []

    seen_tmdb_ids = set()

    for item in raw_items:

        event = normalize_tmdb_item(
            item=item,
            event_time=event_time,
            ingest_time=ingest_time,
        )

        tmdb_id = event["tmdbId"]

        if tmdb_id in seen_tmdb_ids:

            raise RuntimeError(
                "Duplicate tmdbId in "
                f"one snapshot: {tmdb_id}"
            )

        seen_tmdb_ids.add(
            tmdb_id
        )

        records.append(
            event
        )

    return records


# ============================================================
# JSONL WRITER
# ============================================================

def write_snapshot_jsonl(
    records: list[dict],
    event_time: str,
) -> Path:
    """
    Ghi snapshot records thành JSON Lines.

    Output:
        data/prepared/tmdb/
        snapshot_<timestamp>.jsonl

    Ghi theo kiểu atomic:
        .tmp -> os.replace()

    Snapshot cũ không bị overwrite.
    """

    if not records:

        raise ValueError(
            "Cannot write an empty snapshot"
        )

    PREPARED_TMDB_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = (
        timestamp_for_filename(
            event_time
        )
    )

    output_path = (
        PREPARED_TMDB_DIR
        / f"snapshot_{timestamp}.jsonl"
    )

    if output_path.exists():

        raise RuntimeError(
            "Snapshot already exists: "
            f"{output_path}"
        )

    temp_path = (
        PREPARED_TMDB_DIR
        / (
            output_path.name
            + ".tmp"
        )
    )

    try:

        with temp_path.open(
            "w",
            encoding="utf-8",
        ) as f:

            for record in records:

                json.dump(
                    record,
                    f,
                    ensure_ascii=False,
                    separators=(
                        ",",
                        ":",
                    ),
                )

                f.write("\n")

        os.replace(
            temp_path,
            output_path,
        )

    finally:

        # Nếu quá trình fail trước os.replace(),
        # không để file .tmp rác.
        if temp_path.exists():

            temp_path.unlink()

    return output_path


# ============================================================
# ONCE MODE
# ============================================================

def run_once() -> Path:
    """
    Thực thi toàn bộ Step 13 đúng một lần:

        TMDB API
            ->
        fetch snapshot
            ->
        normalize
            ->
        JSONL

    Không scheduler.
    Không Kafka.

    Local `ingest_time` ở artifact này là thời điểm adapter
    xử lý snapshot.

    Khi Module 2 gửi Kafka thật, Module 2 phải gọi lại
    normalize_tmdb_item() với ingest_time thực tế tại producer.
    """

    api_key = os.getenv("TMDB_API_KEY", "").strip()

    if not api_key:

        raise RuntimeError(
            "TMDB_API_KEY is not configured "
            f"in {PROJECT_ROOT / '.env'}"
        )

    print("=" * 60)
    print("TMDB Source Adapter — Once")
    print("=" * 60)

    # --------------------------------------------------------
    # 1. Fetch real TMDB snapshot
    # --------------------------------------------------------

    raw_items, event_time = (
        fetch_trending_movies(
            api_key=api_key
        )
    )

    print(
        "[FETCH] raw items:",
        len(raw_items),
    )

    print(
        "[FETCH] event_time:",
        event_time,
    )

    # --------------------------------------------------------
    # 2. Local adapter processing time
    # --------------------------------------------------------

    ingest_time = utc_now()

    # --------------------------------------------------------
    # 3. Normalize
    # --------------------------------------------------------

    records = (
        build_snapshot_records(
            raw_items=raw_items,
            event_time=event_time,
            ingest_time=ingest_time,
        )
    )

    # --------------------------------------------------------
    # 4. Write JSONL
    # --------------------------------------------------------

    output_path = (
        write_snapshot_jsonl(
            records=records,
            event_time=event_time,
        )
    )

    print()
    print("[OK] TMDB snapshot prepared")

    print(
        "Records     :",
        len(records),
    )

    print(
        "event_time  :",
        event_time,
    )

    print(
        "ingest_time :",
        ingest_time,
    )

    print(
        "Output      :",
        output_path,
    )

    return output_path


# ============================================================
# CLI
# ============================================================

def parse_args():
    """
    Parse command line.

    Chạy một lần mặc định; --once được giữ để tương thích.

    Scheduler mỗi 10 phút thuộc Module 2.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Fetch and normalize one "
            "TMDB movie snapshot."
        )
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Fetch exactly one TMDB snapshot "
            "and write JSONL."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """
    CLI entry point.

    Chạy một snapshot, có thể truyền --once để thể hiện rõ ý định.
    """

    parse_args()

    try:
        run_once()
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
