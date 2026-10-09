import hashlib
import json

from pathlib import Path

import pytest
import requests

from modules.data_source.src import (
    tmdb_source_adapter
)

from modules.data_source.src.tmdb_source_adapter import (
    TMDB_SNAPSHOT_FIELDS,
    build_snapshot_records,
    build_tmdb_event_id,
    normalize_tmdb_item,
    normalize_utc_iso,
    timestamp_for_filename,
)


# ============================================================
# COMMON UNIT-TEST ITEM
# ============================================================

BASE_ITEM = {
    "id": 550,
    "title": "Test Movie",
    "popularity": 123.45,
    "vote_count": 1000,
    "vote_average": 8.2,
}

EVENT_TIME = (
    "2026-10-05T16:00:00Z"
)

INGEST_TIME = (
    "2026-10-05T16:00:05Z"
)


# ============================================================
# CONTRACT MAPPING
# ============================================================

def test_tmdb_item_maps_to_contract_fields():
    """
    Output phải có đúng toàn bộ field
    của tmdb-snapshots contract.
    """

    event = normalize_tmdb_item(
        item=BASE_ITEM,
        event_time=EVENT_TIME,
        ingest_time=INGEST_TIME,
    )

    assert list(
        event.keys()
    ) == TMDB_SNAPSHOT_FIELDS

    assert (
        event["schema_version"]
        == "1.0"
    )

    assert (
        event["source"]
        == "tmdb"
    )

    assert event["tmdbId"] == 550

    assert (
        event["title"]
        == "Test Movie"
    )

    assert (
        event["popularity"]
        == 123.45
    )

    assert (
        event["vote_count"]
        == 1000
    )

    assert (
        event["vote_average"]
        == 8.2
    )

    assert (
        event["event_time"]
        == EVENT_TIME
    )

    assert (
        event["ingest_time"]
        == INGEST_TIME
    )


def test_output_types_are_canonical():
    """
    Kiểm tra logical Python types trước khi JSON serialize.
    """

    event = normalize_tmdb_item(
        BASE_ITEM,
        EVENT_TIME,
        INGEST_TIME,
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
        event["tmdbId"],
        int,
    )

    assert isinstance(
        event["title"],
        str,
    )

    assert isinstance(
        event["popularity"],
        float,
    )

    assert isinstance(
        event["vote_count"],
        int,
    )

    assert isinstance(
        event["vote_average"],
        float,
    )


# ============================================================
# EVENT ID
# ============================================================

def test_same_item_same_event_time_same_event_id():
    """
    event_id phải deterministic.
    """

    first = normalize_tmdb_item(
        BASE_ITEM,
        EVENT_TIME,
        INGEST_TIME,
    )

    second = normalize_tmdb_item(
        BASE_ITEM,
        EVENT_TIME,
        "2026-10-05T16:10:00Z",
    )

    assert (
        first["event_id"]
        == second["event_id"]
    )


def test_event_id_matches_contract_formula():
    """
    Verify:
        sha256("tmdb|tmdbId|event_time")
    """

    identity = (
        "tmdb|550|"
        "2026-10-05T16:00:00Z"
    )

    expected = hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()

    actual = build_tmdb_event_id(
        tmdb_id=550,
        event_time=EVENT_TIME,
    )

    assert actual == expected


def test_event_id_changes_for_new_snapshot_time():
    """
    Cùng movie nhưng snapshot khác thời gian
    phải tạo event_id khác.
    """

    first = build_tmdb_event_id(
        550,
        "2026-10-05T16:00:00Z",
    )

    second = build_tmdb_event_id(
        550,
        "2026-10-05T16:10:00Z",
    )

    assert first != second


def test_event_id_is_sha256_hex():
    """
    SHA-256 hexadecimal phải có 64 ký tự.
    """

    event_id = build_tmdb_event_id(
        550,
        EVENT_TIME,
    )

    assert len(event_id) == 64

    int(
        event_id,
        16,
    )


# ============================================================
# SNAPSHOT SEMANTICS
# ============================================================

def test_all_items_share_snapshot_event_time():
    """
    Một lần fetch TMDB là một snapshot,
    nên toàn bộ record phải dùng cùng event_time.
    """

    items = [
        {
            "id": 1,
            "title": "Movie A",
            "popularity": 10.0,
            "vote_count": 100,
            "vote_average": 7.0,
        },
        {
            "id": 2,
            "title": "Movie B",
            "popularity": 20.0,
            "vote_count": 200,
            "vote_average": 8.0,
        },
    ]

    records = build_snapshot_records(
        raw_items=items,
        event_time=EVENT_TIME,
        ingest_time=INGEST_TIME,
    )

    assert len(records) == 2

    assert {
        record["event_time"]
        for record in records
    } == {
        EVENT_TIME
    }


def test_duplicate_tmdb_id_in_same_snapshot_is_rejected():
    """
    Không silently deduplicate source snapshot.
    """

    items = [
        {
            "id": 550,
            "title": "Movie A",
            "popularity": 10.0,
            "vote_count": 100,
            "vote_average": 7.0,
        },
        {
            "id": 550,
            "title": "Movie A duplicate",
            "popularity": 11.0,
            "vote_count": 101,
            "vote_average": 7.1,
        },
    ]

    with pytest.raises(
        RuntimeError
    ):
        build_snapshot_records(
            raw_items=items,
            event_time=EVENT_TIME,
            ingest_time=INGEST_TIME,
        )


# ============================================================
# TIME NORMALIZATION
# ============================================================

def test_time_with_offset_normalizes_to_utc():

    result = normalize_utc_iso(
        "2026-10-05T23:00:00+07:00"
    )

    assert (
        result
        == "2026-10-05T16:00:00Z"
    )


def test_naive_time_is_rejected():

    with pytest.raises(
        ValueError
    ):
        normalize_utc_iso(
            "2026-10-05T16:00:00"
        )


def test_filename_timestamp():

    result = timestamp_for_filename(
        EVENT_TIME
    )

    assert (
        result
        == "20261005T160000Z"
    )


# ============================================================
# FIELD VALIDATION
# ============================================================

def test_invalid_tmdb_id_is_rejected():

    item = BASE_ITEM.copy()

    item["id"] = 0

    with pytest.raises(
        ValueError
    ):
        normalize_tmdb_item(
            item,
            EVENT_TIME,
            INGEST_TIME,
        )


def test_empty_title_is_rejected():

    item = BASE_ITEM.copy()

    item["title"] = ""

    with pytest.raises(
        ValueError
    ):
        normalize_tmdb_item(
            item,
            EVENT_TIME,
            INGEST_TIME,
        )


def test_invalid_popularity_is_rejected():

    item = BASE_ITEM.copy()

    item["popularity"] = (
        "not-a-number"
    )

    with pytest.raises(
        ValueError
    ):
        normalize_tmdb_item(
            item,
            EVENT_TIME,
            INGEST_TIME,
        )


def test_negative_popularity_is_rejected():
    item = {**BASE_ITEM, "popularity": -0.01}
    with pytest.raises(ValueError, match="popularity must be >= 0"):
        normalize_tmdb_item(item, EVENT_TIME, INGEST_TIME)


def test_negative_vote_count_is_rejected():

    item = BASE_ITEM.copy()

    item["vote_count"] = -1

    with pytest.raises(
        ValueError
    ):
        normalize_tmdb_item(
            item,
            EVENT_TIME,
            INGEST_TIME,
        )


def test_vote_average_above_ten_is_rejected():

    item = BASE_ITEM.copy()

    item["vote_average"] = 11.0

    with pytest.raises(
        ValueError
    ):
        normalize_tmdb_item(
            item,
            EVENT_TIME,
            INGEST_TIME,
        )


def test_vote_average_below_zero_is_rejected():

    item = BASE_ITEM.copy()

    item["vote_average"] = -0.1

    with pytest.raises(
        ValueError
    ):
        normalize_tmdb_item(
            item,
            EVENT_TIME,
            INGEST_TIME,
        )


# ============================================================
# JSONL WRITER
# ============================================================

def test_snapshot_jsonl_writer(
    tmp_path,
    monkeypatch,
):
    """
    Kiểm tra writer tạo đúng JSONL.

    tmp_path của pytest được dùng để không ghi vào
    data/prepared thật khi chạy unit test.
    """

    monkeypatch.setattr(
        tmdb_source_adapter,
        "PREPARED_TMDB_DIR",
        tmp_path,
    )

    records = [
        normalize_tmdb_item(
            BASE_ITEM,
            EVENT_TIME,
            INGEST_TIME,
        )
    ]

    output_path = (
        tmdb_source_adapter
        .write_snapshot_jsonl(
            records=records,
            event_time=EVENT_TIME,
        )
    )

    assert output_path.exists()

    assert (
        output_path.name
        == "snapshot_20261005T160000Z.jsonl"
    )

    lines = (
        output_path
        .read_text(
            encoding="utf-8"
        )
        .splitlines()
    )

    assert len(lines) == 1

    loaded = json.loads(
        lines[0]
    )

    assert (
        loaded
        == records[0]
    )


def test_writer_does_not_overwrite_snapshot(
    tmp_path,
    monkeypatch,
):
    """
    Snapshot cùng timestamp không được overwrite âm thầm.
    """

    monkeypatch.setattr(
        tmdb_source_adapter,
        "PREPARED_TMDB_DIR",
        tmp_path,
    )

    records = [
        normalize_tmdb_item(
            BASE_ITEM,
            EVENT_TIME,
            INGEST_TIME,
        )
    ]

    tmdb_source_adapter.write_snapshot_jsonl(
        records,
        EVENT_TIME,
    )

    with pytest.raises(
        RuntimeError
    ):
        tmdb_source_adapter.write_snapshot_jsonl(
            records,
            EVENT_TIME,
        )


@pytest.fixture
def tmdb_http(monkeypatch):
    """Isolate HTTP tests from local credentials and host configuration."""
    monkeypatch.delenv("TMDB_BASE_URL", raising=False)
    monkeypatch.setattr(tmdb_source_adapter, "utc_now", lambda: EVENT_TIME)

    def response(payload, status=200):
        result = requests.Response()
        result.status_code = status
        result._content = json.dumps(payload).encode("utf-8")
        return result

    return response


def test_fetch_primary_host(monkeypatch, tmdb_http):
    def get(url, **kwargs):
        assert url == tmdb_source_adapter.TMDB_BASE_URL + "/trending/movie/day"
        assert kwargs["params"] == {"api_key": "test-key"}
        assert kwargs["timeout"] == 20
        assert kwargs["allow_redirects"] is False
        return tmdb_http({"results": [BASE_ITEM]})

    monkeypatch.setattr(tmdb_source_adapter.requests, "get", get)
    assert tmdb_source_adapter.fetch_trending_movies(" test-key ") == (
        [BASE_ITEM], EVENT_TIME
    )


@pytest.mark.parametrize("error", [requests.ConnectionError, requests.Timeout])
def test_fetch_uses_alternate_host_on_connection_failure(monkeypatch, tmdb_http, error):
    urls = []

    def get(url, **kwargs):
        urls.append(url)
        if len(urls) == 1:
            raise error("connection failed with api_key=test-key")
        return tmdb_http({"results": [BASE_ITEM]})

    monkeypatch.setattr(tmdb_source_adapter.requests, "get", get)
    assert tmdb_source_adapter.fetch_trending_movies("test-key") == ([BASE_ITEM], EVENT_TIME)
    assert urls == [
        tmdb_source_adapter.TMDB_BASE_URL + "/trending/movie/day",
        tmdb_source_adapter.TMDB_ALTERNATE_BASE_URL + "/trending/movie/day",
    ]


def test_fetch_uses_configured_host_directly(monkeypatch, tmdb_http):
    monkeypatch.setenv("TMDB_BASE_URL", "https://api.tmdb.org/3/")
    urls = []

    def get(url, **kwargs):
        urls.append(url)
        return tmdb_http({"results": [BASE_ITEM]})

    monkeypatch.setattr(tmdb_source_adapter.requests, "get", get)
    tmdb_source_adapter.fetch_trending_movies("test-key")
    assert urls == ["https://api.tmdb.org/3/trending/movie/day"]


def test_fetch_rejects_untrusted_host_before_sending_key(monkeypatch, tmdb_http):
    monkeypatch.setenv("TMDB_BASE_URL", "https://example.com/3")

    def get(*args, **kwargs):
        pytest.fail("Credentials must not be sent to another host")

    monkeypatch.setattr(tmdb_source_adapter.requests, "get", get)
    with pytest.raises(RuntimeError, match="TMDB_BASE_URL must be"):
        tmdb_source_adapter.fetch_trending_movies("test-key")


@pytest.mark.parametrize("status", [301, 401, 403, 429, 500])
def test_fetch_reports_http_errors_without_retrying(monkeypatch, tmdb_http, status):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        return tmdb_http({"status_message": "failed"}, status)

    monkeypatch.setattr(tmdb_source_adapter.requests, "get", get)
    with pytest.raises(RuntimeError, match=f"HTTP {status}") as exc:
        tmdb_source_adapter.fetch_trending_movies("test-key")
    assert "test-key" not in str(exc.value)
    assert len(calls) == 1


@pytest.mark.parametrize("payload", [[], {"results": None}, {"results": []}])
def test_fetch_rejects_invalid_payload(monkeypatch, tmdb_http, payload):
    monkeypatch.setattr(
        tmdb_source_adapter.requests, "get", lambda *a, **kw: tmdb_http(payload)
    )
    with pytest.raises(RuntimeError, match="TMDB"):
        tmdb_source_adapter.fetch_trending_movies("test-key")


def test_fetch_rejects_non_json_response(monkeypatch, tmdb_http):
    response = tmdb_http({})
    response._content = b"<html>Bad gateway</html>"
    monkeypatch.setattr(tmdb_source_adapter.requests, "get", lambda *a, **kw: response)
    with pytest.raises(RuntimeError, match="not valid JSON"):
        tmdb_source_adapter.fetch_trending_movies("test-key")


def test_cli_connection_failure_does_not_expose_api_key(monkeypatch, tmdb_http, capsys):
    monkeypatch.setattr("sys.argv", ["tmdb_source_adapter.py", "--once"])
    monkeypatch.setenv("TMDB_API_KEY", "test-secret-key")

    def get(*args, **kwargs):
        raise requests.ConnectionError("URL contains api_key=test-secret-key")

    monkeypatch.setattr(tmdb_source_adapter.requests, "get", get)
    with pytest.raises(SystemExit) as exc:
        tmdb_source_adapter.main()
    assert exc.value.code == 1
    output = capsys.readouterr()
    assert "[ERROR] Cannot connect to TMDB" in output.err
    assert "test-secret-key" not in output.out + output.err


def test_cli_without_arguments_crawls_and_writes_snapshot(monkeypatch, tmp_path, tmdb_http):
    monkeypatch.setattr("sys.argv", ["tmdb_source_adapter.py"])
    monkeypatch.setenv("TMDB_API_KEY", "test-key")
    monkeypatch.setattr(tmdb_source_adapter, "PREPARED_TMDB_DIR", tmp_path)
    monkeypatch.setattr(
        tmdb_source_adapter.requests, "get",
        lambda *a, **kw: tmdb_http({"results": [BASE_ITEM]}),
    )
    tmdb_source_adapter.main()
    records = [json.loads(line) for line in next(tmp_path.glob("*.jsonl")).read_text().splitlines()]
    assert records == [normalize_tmdb_item(BASE_ITEM, EVENT_TIME, EVENT_TIME)]
