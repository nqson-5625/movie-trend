"""Validate the executable Phase 2 platform inventory against the signed contract."""

from __future__ import annotations

from typing import Any, Dict

from .es_client import load_runtime_config

EXPECTED_SOURCE_ALIASES = {
    "movie_batch_stats",
    "movie_daily_stats",
    "genre_trends",
    "movie_realtime_stats",
    "genre_realtime",
    "tmdb_realtime",
}
EXPECTED_ANALYTICS_ALIASES = {
    "analytics_timeseries",
    "analytics_top_movies",
    "analytics_genre_trends",
    "analytics_growth",
    "analytics_growth_current",
    "analytics_tmdb_signal",
}
EXPECTED_DATA_VIEWS = {
    "dv_d1_timeseries",
    "dv_d1_top_movies",
    "dv_d2_movie_timeseries",
    "dv_d3_genre_trends",
    "dv_d4_growth_current",
    "dv_d4_growth_history",
    "dv_d5_tmdb_signal",
}


def validate_runtime_contract(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_runtime_config()
    errors: list[str] = []

    if cfg.get("schema_version") != "phase2-3.0":
        errors.append("schema_version phải là phase2-3.0")
    if cfg.get("timezone") != "UTC":
        errors.append("timezone phải là UTC")

    inventories = (
        ("source_indices", EXPECTED_SOURCE_ALIASES),
        ("analytics_indices", EXPECTED_ANALYTICS_ALIASES),
        ("data_views", EXPECTED_DATA_VIEWS),
    )
    for key, expected in inventories:
        actual = set((cfg.get(key) or {}).keys())
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        if missing:
            errors.append(f"{key} thiếu {missing}")
        if extra:
            errors.append(f"{key} có phần tử ngoài contract {extra}")

    aliases = set((cfg.get("analytics_indices") or {}).keys())
    for data_view_id, spec in (cfg.get("data_views") or {}).items():
        if not isinstance(spec, dict) or spec.get("alias") not in aliases:
            errors.append(f"Data View {data_view_id} không trỏ exact analytics alias")

    versioned = set(cfg.get("versioned_publication_aliases") or [])
    expected_versioned = {"analytics_growth_current", "analytics_tmdb_signal"}
    if versioned != expected_versioned:
        errors.append(
            "versioned_publication_aliases phải đúng analytics_growth_current và "
            "analytics_tmdb_signal"
        )

    if errors:
        raise ValueError("Runtime contract không hợp lệ: " + "; ".join(errors))
    return {
        "schema_version": cfg["schema_version"],
        "source_alias_count": len(EXPECTED_SOURCE_ALIASES),
        "analytics_alias_count": len(EXPECTED_ANALYTICS_ALIASES),
        "data_view_count": len(EXPECTED_DATA_VIEWS),
    }
