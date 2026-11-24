"""P5 TMDB Realtime Spark SQL lane (`tmdb_realtime` -> `analytics_tmdb_signal`)."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from .change_detector import (
    COMPARE_FIELDS,
    DEFAULT_TOP_K,
    TOP_K_SORT,
    ChangeDetectionResult,
    detect_top_k_change,
    fetch_top_k_rows,
    normalize_tmdb_row,
)

_JOB_EXPORTS = {
    "build_tmdb_signal_df",
    "cleanup_old_physical_indices",
    "generate_version_tag",
    "load_sql_query",
    "sync_once",
}

__all__ = [
    "COMPARE_FIELDS",
    "DEFAULT_TOP_K",
    "TOP_K_SORT",
    "ChangeDetectionResult",
    "build_tmdb_signal_df",
    "cleanup_old_physical_indices",
    "detect_top_k_change",
    "fetch_top_k_rows",
    "generate_version_tag",
    "load_sql_query",
    "normalize_tmdb_row",
    "sync_once",
]


def __getattr__(name: str) -> Any:
    # Lazy-load job symbols so `python -m ...job` does not trigger runpy RuntimeWarning.
    if name in _JOB_EXPORTS:
        mod = import_module(".job", __name__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
