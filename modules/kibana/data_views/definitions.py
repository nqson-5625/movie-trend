"""Build stable Kibana Data View payloads from the executable contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_CONFIG = (
    PROJECT_ROOT / "contracts" / "phase2" / "common" / "phase2_runtime_config.json"
)


def load_contract() -> dict[str, Any]:
    return json.loads(RUNTIME_CONFIG.read_text(encoding="utf-8"))


def data_view_definitions(contract: dict[str, Any]) -> list[dict[str, Any]]:
    views = contract.get("data_views")
    analytics_aliases = set(contract.get("analytics_indices", {}))
    if not isinstance(views, dict) or not views:
        raise ValueError("Runtime contract phải khai báo Data Views")

    definitions = []
    for data_view_id, spec in views.items():
        if not isinstance(spec, dict):
            raise ValueError(f"Data View {data_view_id} phải là object")
        alias = spec.get("alias")
        name = spec.get("name")
        time_field = spec.get("time_field")
        if alias not in analytics_aliases:
            raise ValueError(f"Data View {data_view_id} không trỏ analytics alias")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Data View {data_view_id} thiếu name")
        if time_field is not None and (
            not isinstance(time_field, str) or not time_field.strip()
        ):
            raise ValueError(f"Data View {data_view_id} có time_field không hợp lệ")

        definition: dict[str, Any] = {
            "id": data_view_id,
            "name": name,
            "title": alias,
            # Dashboard owners can create saved objects before a physical index is
            # published behind the canonical alias.
            "allowNoIndex": True,
        }
        if time_field is not None:
            definition["timeFieldName"] = time_field
        definitions.append(definition)
    return definitions
