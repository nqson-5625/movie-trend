"""Top-20 change detection between `tmdb_realtime` and `analytics_tmdb_signal`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping

from modules.elasticsearch.common.config.es_client import ESClient, validate_index_name

DEFAULT_TOP_K = 20
COMPARE_FIELDS = (
    "tmdbId",
    "event_time",
    "popularity",
    "vote_count",
    "vote_average",
    "prev_popularity",
    "popularity_delta",
    "updated_at",
    "title",
)
TOP_K_SORT: List[Dict[str, str]] = [
    {"event_time": "desc"},
    {"popularity": "desc"},
    {"tmdbId": "asc"},
]


def _normalize_utc_ts(value: Any) -> str | None:
    """Normalize ISO timestamp or datetime into canonical UTC `YYYY-MM-DDTHH:MM:SSZ`."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_title(value: Any) -> str | None:
    """Match SQL normalization: trim whitespace and collapse blank titles to None."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None


def _normalize_float(value: Any) -> float | None:
    """Normalize numeric floats including canonicalizing -0.0 to 0.0."""
    if value is None:
        return None
    num = float(value)
    return 0.0 if num == 0.0 else num


def _normalize_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def normalize_tmdb_row(doc: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize comparable TMDB fields so ES source and target rows match deterministically."""
    return {
        "tmdbId": _normalize_int(doc.get("tmdbId")),
        "event_time": _normalize_utc_ts(doc.get("event_time")),
        "popularity": _normalize_float(doc.get("popularity")),
        "vote_count": _normalize_int(doc.get("vote_count")),
        "vote_average": _normalize_float(doc.get("vote_average")),
        "prev_popularity": _normalize_float(doc.get("prev_popularity")),
        "popularity_delta": _normalize_float(doc.get("popularity_delta")),
        "updated_at": _normalize_utc_ts(doc.get("updated_at")),
        "title": _normalize_title(doc.get("title")),
    }


def fetch_top_k_rows(
    client: ESClient,
    index_or_alias: str,
    top_k: int = DEFAULT_TOP_K,
) -> List[Dict[str, Any]]:
    """Fetch and normalize Top-K rows sorted by `(event_time DESC, popularity DESC, tmdbId ASC)`."""
    if top_k <= 0:
        raise ValueError(f"top_k phải lớn hơn 0, nhận {top_k}")
    clean = validate_index_name(index_or_alias)
    resp = client.search(
        clean,
        {
            "size": top_k,
            "_source": list(COMPARE_FIELDS),
            "sort": TOP_K_SORT,
        },
    )
    hits = resp.get("hits", {}).get("hits", [])
    return [normalize_tmdb_row(hit.get("_source") or {}) for hit in hits]


@dataclass(frozen=True)
class ChangeDetectionResult:
    """Outcome of comparing Top-K documents between source and published analytics alias."""

    has_change: bool
    reason: str
    source_count: int
    target_count: int
    source_top_ids: tuple[int, ...]
    target_top_ids: tuple[int, ...]


def detect_top_k_change(
    client: ESClient,
    source_index: str = "tmdb_realtime",
    target_alias: str = "analytics_tmdb_signal",
    top_k: int = DEFAULT_TOP_K,
) -> ChangeDetectionResult:
    """Compare Top-K rows from `source_index` against `target_alias` without launching Spark."""
    clean_source = validate_index_name(source_index)
    clean_target = validate_index_name(target_alias)

    if not client.index_exists(clean_source):
        return ChangeDetectionResult(
            has_change=False,
            reason="SOURCE_MISSING",
            source_count=0,
            target_count=0,
            source_top_ids=(),
            target_top_ids=(),
        )

    source_rows = fetch_top_k_rows(client, clean_source, top_k=top_k)
    source_ids = tuple(int(r["tmdbId"]) for r in source_rows if r.get("tmdbId") is not None)
    if not source_rows:
        return ChangeDetectionResult(
            has_change=False,
            reason="SOURCE_EMPTY",
            source_count=0,
            target_count=0,
            source_top_ids=(),
            target_top_ids=(),
        )

    if not client.index_exists(clean_target):
        return ChangeDetectionResult(
            has_change=True,
            reason="TARGET_MISSING",
            source_count=len(source_rows),
            target_count=0,
            source_top_ids=source_ids,
            target_top_ids=(),
        )

    target_rows = fetch_top_k_rows(client, clean_target, top_k=top_k)
    target_ids = tuple(int(r["tmdbId"]) for r in target_rows if r.get("tmdbId") is not None)

    if source_rows == target_rows:
        return ChangeDetectionResult(
            has_change=False,
            reason="NO_CHANGE",
            source_count=len(source_rows),
            target_count=len(target_rows),
            source_top_ids=source_ids,
            target_top_ids=target_ids,
        )

    return ChangeDetectionResult(
        has_change=True,
        reason="TOP_K_CHANGED",
        source_count=len(source_rows),
        target_count=len(target_rows),
        source_top_ids=source_ids,
        target_top_ids=target_ids,
    )
