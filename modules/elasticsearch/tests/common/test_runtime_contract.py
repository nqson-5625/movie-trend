from __future__ import annotations

import copy

import pytest

from modules.elasticsearch.common.config.es_client import load_runtime_config
from modules.elasticsearch.common.config.runtime_contract import validate_runtime_contract


def test_phase2_runtime_inventory_is_exact() -> None:
    assert validate_runtime_contract() == {
        "schema_version": "phase2-3.0",
        "source_alias_count": 6,
        "analytics_alias_count": 6,
        "data_view_count": 7,
    }


def test_runtime_inventory_rejects_missing_family_and_physical_data_view() -> None:
    cfg = copy.deepcopy(load_runtime_config())
    del cfg["analytics_indices"]["analytics_growth_current"]
    cfg["data_views"]["dv_d5_tmdb_signal"]["alias"] = "analytics_tmdb_signal-v1"
    with pytest.raises(ValueError, match="analytics_growth_current"):
        validate_runtime_contract(cfg)
