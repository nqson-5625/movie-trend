"""Kiểm thử offline cho Kibana Saved Objects Dashboard D3 (P3 - Giang)."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

KIBANA_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = KIBANA_DIR.parents[1]
BUILDER = KIBANA_DIR / "dashboards" / "d03_genres" / "build_d03_saved_objects.py"
OUT_DIR = KIBANA_DIR / "saved_objects" / "owners" / "p3"
NDJSON_PATH = OUT_DIR / "d03_genres.ndjson"
MANIFEST_PATH = OUT_DIR / "manifest.json"


def _load_ndjson() -> list[dict]:
    return [json.loads(line) for line in NDJSON_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_d03_saved_objects_match_builder() -> None:
    proc = subprocess.run([sys.executable, str(BUILDER), "--check"], capture_output=True, text=True, cwd=PROJECT_ROOT)
    assert proc.returncode == 0, proc.stderr


def test_d03_manifest_and_object_inventory() -> None:
    manifest = _load_manifest()
    objects = _load_ndjson()
    assert manifest["owner"] == "p3"
    assert manifest["contract_version"] == "Phase2-3.0"
    assert manifest["kibana_version"] == "8.15.3"
    assert manifest["dashboard_id"] == "mt-p2-8153-d3"
    assert manifest["data_view_ids"] == ["dv_d3_genre_trends"]
    assert [obj["id"] for obj in objects] == manifest["object_ids"]
    assert len(objects) == 11
    assert sum(1 for obj in objects if obj["type"] == "dashboard") == 1
    assert sum(1 for obj in objects if obj["type"] == "lens") == 7
    assert sum(1 for obj in objects if obj["type"] == "visualization") == 3


def test_d03_uses_only_canonical_data_view_and_no_formulas() -> None:
    objects = _load_ndjson()
    for obj in objects:
        for ref in obj.get("references", []):
            if ref.get("type") == "index-pattern":
                assert ref["id"] == "dv_d3_genre_trends"
                assert "dev_" not in ref["id"]
        if obj["type"] == "lens":
            layers = obj["attributes"]["state"]["datasourceStates"]["formBased"]["layers"]
            for layer in layers.values():
                for col in layer["columns"].values():
                    assert not col.get("isFormula", False), "Contract cấm dùng Lens Formula tính lại metric"


def test_d03_grid_has_10_non_overlapping_panels_and_required_controls() -> None:
    objects = _load_ndjson()
    dash = next(obj for obj in objects if obj["type"] == "dashboard")
    attrs = dash["attributes"]
    assert attrs["timeFrom"] == "1990-01-01T00:00:00.000Z"
    assert attrs["timeTo"] == "2018-10-01T00:00:00.000Z"
    assert attrs["refreshInterval"] == {"pause": True, "value": 0}

    panels = json.loads(attrs["panelsJSON"])
    assert len(panels) == 10

    # Kiểm tra tọa độ 48 cột không chồng lấn
    occupied: set[tuple[int, int]] = set()
    for panel in panels:
        g = panel["gridData"]
        assert 0 <= g["x"] and g["x"] + g["w"] <= 48
        assert g["w"] > 0 and g["h"] > 0
        for x in range(g["x"], g["x"] + g["w"]):
            for y in range(g["y"], g["y"] + g["h"]):
                assert (x, y) not in occupied, f"Ô ({x},{y}) bị chồng lấn ở panel {panel['panelIndex']}"
                occupied.add((x, y))

    # Kiểm tra controls bắt buộc
    ctrl_panels = json.loads(attrs["controlGroupInput"]["panelsJSON"])
    by_field = {v["explicitInput"]["fieldName"]: v["explicitInput"] for v in ctrl_panels.values()}
    assert set(by_field) == {"period_type", "genre"}
    assert by_field["period_type"]["singleSelect"] is True
    assert by_field["period_type"]["selectedOptions"] == ["month"]
    assert by_field["period_type"]["dataViewId"] == "dv_d3_genre_trends"
    assert by_field["genre"]["dataViewId"] == "dv_d3_genre_trends"

    # Kiểm tra cảnh báo multi-label
    note = next(obj for obj in objects if obj["id"] == "mt-p2-8153-d3-note")
    vis_state = json.loads(note["attributes"]["visState"])
    assert "MULTI-LABEL" in vis_state["params"]["markdown"]
    assert "274.480" in vis_state["params"]["markdown"]
