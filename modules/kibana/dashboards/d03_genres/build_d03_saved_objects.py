"""Sinh Kibana 8.15.3 Saved Objects (NDJSON + owner manifest) cho Dashboard D3 (P3 - Giang).

Bố cục 48 cột, controls, Data View `dv_d3_genre_trends` và 10 panels bám sát
`MovieTrend_Kibana_8.15.3_Live_Layout_Preview.html` và `DATA_CONTRACT_Phase2.md` (Phase2-3.0).

    .venv/bin/python modules/kibana/dashboards/d03_genres/build_d03_saved_objects.py          # ghi NDJSON + manifest
    .venv/bin/python modules/kibana/dashboards/d03_genres/build_d03_saved_objects.py --check  # kiểm tra không lệch
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

KIBANA_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = KIBANA_DIR / "saved_objects" / "owners" / "p3"
NDJSON_PATH = OUT_DIR / "d03_genres.ndjson"
MANIFEST_PATH = OUT_DIR / "manifest.json"

OWNER = "p3"
KIBANA_VERSION = "8.15.3"
CONTRACT_VERSION = "Phase2-3.0"
DATA_VIEW_ID = "dv_d3_genre_trends"
DASHBOARD_ID = "mt-p2-8153-d3"
EXPORTED_AT = "2026-11-27T00:00:00Z"
TIME_FROM = "1990-01-01T00:00:00.000Z"
TIME_TO = "2018-10-01T00:00:00.000Z"

TEAL = "#176E83"
ORANGE = "#D5774A"


def _markdown_vis(vis_id: str, title: str, markdown: str, font_size: int = 12) -> dict[str, Any]:
    vis_state = {
        "title": title,
        "type": "markdown",
        "aggs": [],
        "params": {
            "fontSize": font_size,
            "openLinksInNewTab": False,
            "markdown": markdown,
        },
    }
    return {
        "id": vis_id,
        "type": "visualization",
        "managed": False,
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "8.5.0",
        "attributes": {
            "title": title,
            "visState": json.dumps(vis_state, ensure_ascii=False),
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {"query": {"query": "", "language": "kuery"}, "filter": []},
                    ensure_ascii=False,
                )
            },
        },
        "references": [],
    }


def _col(
    label: str,
    data_type: str,
    operation_type: str,
    source_field: str,
    params: dict[str, Any] | None = None,
    custom_label: bool = True,
) -> dict[str, Any]:
    return {
        "label": label,
        "dataType": data_type,
        "operationType": operation_type,
        "isBucketed": operation_type in {"date_histogram", "terms"},
        "scale": "ordinal" if operation_type == "terms" else ("interval" if operation_type == "date_histogram" else "ratio"),
        "sourceField": source_field,
        "customLabel": custom_label,
        "params": params or {},
    }


def _lens_obj(
    lens_id: str,
    title: str,
    visualization_type: str,
    columns: dict[str, dict[str, Any]],
    column_order: list[str],
    layer_visualization: dict[str, Any],
) -> dict[str, Any]:
    layer_id = f"{lens_id}-layer"
    state = {
        "visualization": layer_visualization,
        "query": {"query": "", "language": "kuery"},
        "filters": [],
        "internalReferences": [],
        "adHocDataViews": {},
        "datasourceStates": {
            "formBased": {
                "layers": {
                    layer_id: {
                        "columns": columns,
                        "columnOrder": column_order,
                        "incompleteColumns": {},
                        "sampling": 1,
                    }
                }
            },
            "indexpattern": {"layers": {}},
            "textBased": {"layers": {}},
        },
    }
    return {
        "id": lens_id,
        "type": "lens",
        "managed": False,
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "8.9.0",
        "attributes": {
            "title": title,
            "description": "",
            "visualizationType": visualization_type,
            "state": state,
        },
        "references": [
            {
                "type": "index-pattern",
                "id": DATA_VIEW_ID,
                "name": f"indexpattern-datasource-layer-{layer_id}",
            }
        ],
    }


def _metric_lens(
    lens_id: str,
    title: str,
    field: str,
    op: str,
    label: str,
    *,
    subtitle: str = "",
    decimals: int | None = None,
) -> dict[str, Any]:
    layer_id = f"{lens_id}-layer"
    col_id = "measure"
    params: dict[str, Any] = {} if op == "unique_count" else {"emptyAsNull": True}
    if decimals is not None:
        params["format"] = {"id": "number", "params": {"decimals": decimals}}
    return _lens_obj(
        lens_id=lens_id,
        title=title,
        visualization_type="lnsMetric",
        columns={col_id: _col(label, "number", op, field, params=params)},
        column_order=[col_id],
        layer_visualization={
            "layerId": layer_id,
            "layerType": "data",
            "metricAccessor": col_id,
            "titlesAndText": {
                "alignment": "left",
                "title": label,
                "subtitle": subtitle or f"{op}({field})",
                "titleWeight": "bold",
            },
            "trendline": "none",
        },
    )


def build_objects() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    panels_spec = [
        {
            "id": "mt-p2-8153-d3-nav",
            "kind": "visualization",
            "hide_title": True,
            "grid": {"x": 0, "y": 0, "w": 48, "h": 3, "i": "p-d3-nav"},
            "obj": _markdown_vis(
                "mt-p2-8153-d3-nav",
                "Điều hướng D1–D5",
                "[D1 · Tổng quan](#/view/mt-p2-8153-d1) &nbsp;|&nbsp; "
                "[D2 · Xu hướng phim](#/view/mt-p2-8153-d2) &nbsp;|&nbsp; "
                "**[D3 · Thể loại](#/view/mt-p2-8153-d3)** &nbsp;|&nbsp; "
                "[D4 · Biến động](#/view/mt-p2-8153-d4) &nbsp;|&nbsp; "
                "[D5 · TMDB](#/view/mt-p2-8153-d5)",
                font_size=13,
            ),
        },
        {
            "id": "mt-p2-8153-d3-header",
            "kind": "visualization",
            "hide_title": True,
            "grid": {"x": 0, "y": 3, "w": 48, "h": 6, "i": "p-d3-header"},
            "obj": _markdown_vis(
                "mt-p2-8153-d3-header",
                "Thể loại nào đang được quan tâm?",
                "##### MOVIE TREND / PHASE 2 · D3 · P3\n"
                "## Thể loại nào đang được quan tâm?\n"
                "So sánh thể loại theo thời gian, thứ hạng từng kỳ và bản đồ nhiệt &nbsp;—&nbsp; `BATCH` · `UTC` · `Kibana 8.15.3`",
                font_size=13,
            ),
        },
        {
            "id": "mt-p2-8153-d3-total",
            "kind": "lens",
            "hide_title": True,
            "grid": {"x": 0, "y": 9, "w": 12, "h": 6, "i": "p-d3-total"},
            "obj": _metric_lens("mt-p2-8153-d3-total", "Lượt gắn genre ở grain lọc", "rating_count", "sum", "Lượt gắn genre ở grain lọc", subtitle="sum(rating_count)", decimals=0),
        },
        {
            "id": "mt-p2-8153-d3-genres",
            "kind": "lens",
            "hide_title": True,
            "grid": {"x": 12, "y": 9, "w": 12, "h": 6, "i": "p-d3-genres"},
            "obj": _metric_lens("mt-p2-8153-d3-genres", "Số genre trong phạm vi lọc", "genre", "unique_count", "Số genre trong phạm vi lọc", subtitle="cardinality(genre)"),
        },
        {
            "id": "mt-p2-8153-d3-peak",
            "kind": "lens",
            "hide_title": True,
            "grid": {"x": 24, "y": 9, "w": 12, "h": 6, "i": "p-d3-peak"},
            "obj": _metric_lens("mt-p2-8153-d3-peak", "Rating_count kỳ lớn nhất", "rating_count", "max", "Rating_count kỳ lớn nhất", subtitle="max(rating_count)", decimals=0),
        },
        {
            "id": "mt-p2-8153-d3-avg",
            "kind": "lens",
            "hide_title": True,
            "grid": {"x": 36, "y": 9, "w": 12, "h": 6, "i": "p-d3-avg"},
            "obj": _metric_lens("mt-p2-8153-d3-avg", "Điểm TB grain tối đa", "avg_rating", "max", "Điểm TB grain tối đa", subtitle="max(avg_rating)", decimals=2),
        },
        {
            "id": "mt-p2-8153-d3-line",
            "kind": "lens",
            "hide_title": False,
            "grid": {"x": 0, "y": 15, "w": 32, "h": 18, "i": "p-d3-line"},
            "obj": _lens_obj(
                lens_id="mt-p2-8153-d3-line",
                title="Xu hướng thể loại theo thời gian",
                visualization_type="lnsXY",
                columns={
                    "cat": _col("UTC · period_start", "date", "date_histogram", "period_start", {"interval": "auto", "includeEmptyRows": True, "dropPartials": False}),
                    "split": _col("genre", "string", "terms", "genre", {"size": 5, "orderBy": {"type": "column", "columnId": "val"}, "orderDirection": "desc", "otherBucket": False, "missingBucket": False, "parentFormat": {"id": "terms"}}),
                    "val": _col("Lượt rating", "number", "sum", "rating_count", {"emptyAsNull": True}),
                },
                column_order=["cat", "split", "val"],
                layer_visualization={
                    "preferredSeriesType": "line",
                    "axisTitlesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
                    "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
                    "gridlinesVisibilitySettings": {"x": False, "yLeft": True, "yRight": False},
                    "xTitle": "UTC · period_start",
                    "yLeftTitle": "Lượt rating",
                    "fittingFunction": "None",
                    "curveType": "LINEAR",
                    "layers": [
                        {
                            "layerId": "mt-p2-8153-d3-line-layer",
                            "layerType": "data",
                            "seriesType": "line",
                            "xAccessor": "cat",
                            "splitAccessor": "split",
                            "accessors": ["val"],
                            "colorMapping": {
                                "assignments": [
                                    {"rule": {"type": "matchExact", "values": ["Drama"]}, "color": {"type": "fixed", "color": "#176E83"}, "touched": True},
                                    {"rule": {"type": "matchExact", "values": ["Comedy"]}, "color": {"type": "fixed", "color": "#D5774A"}, "touched": True},
                                    {"rule": {"type": "matchExact", "values": ["Action"]}, "color": {"type": "fixed", "color": "#7967b4"}, "touched": True},
                                    {"rule": {"type": "matchExact", "values": ["Thriller"]}, "color": {"type": "fixed", "color": "#23a6a8"}, "touched": True},
                                    {"rule": {"type": "matchExact", "values": ["Adventure"]}, "color": {"type": "fixed", "color": "#E6A832"}, "touched": True},
                                    {"rule": {"type": "matchExact", "values": ["Romance"]}, "color": {"type": "fixed", "color": "#679c61"}, "touched": True},
                                ],
                                "specialAssignments": [{"rule": {"type": "other"}, "color": {"type": "loop"}, "touched": False}],
                                "paletteId": "eui_amsterdam_color_blind",
                                "colorMode": {"type": "categorical"},
                            },
                        }
                    ],
                    "legend": {"isVisible": True, "position": "right"},
                    "valueLabels": "hide",
                },
            ),
        },
        {
            "id": "mt-p2-8153-d3-top",
            "kind": "lens",
            "hide_title": False,
            "grid": {"x": 32, "y": 15, "w": 16, "h": 18, "i": "p-d3-top"},
            "obj": _lens_obj(
                lens_id="mt-p2-8153-d3-top",
                title="Top thể loại",
                visualization_type="lnsXY",
                columns={
                    "cat": _col("Thể loại", "string", "terms", "genre", {"size": 10, "orderBy": {"type": "column", "columnId": "val"}, "orderDirection": "desc", "otherBucket": False, "missingBucket": False, "parentFormat": {"id": "terms"}}),
                    "val": _col("Lượt rating", "number", "sum", "rating_count", {"emptyAsNull": True, "format": {"id": "number", "params": {"decimals": 0}}}),
                },
                column_order=["cat", "val"],
                layer_visualization={
                    "preferredSeriesType": "bar_horizontal",
                    "axisTitlesVisibilitySettings": {"x": False, "yLeft": False, "yRight": False},
                    "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": False},
                    "gridlinesVisibilitySettings": {"x": True, "yLeft": False, "yRight": False},
                    "xTitle": "",
                    "yLeftTitle": "",
                    "layers": [
                        {
                            "layerId": "mt-p2-8153-d3-top-layer",
                            "layerType": "data",
                            "seriesType": "bar_horizontal",
                            "xAccessor": "cat",
                            "accessors": ["val"],
                            "color": ORANGE,
                            "colorMapping": {
                                "assignments": [
                                    {"rule": {"type": "matchExact", "values": ["Lượt rating"]}, "color": {"type": "fixed", "color": ORANGE}, "touched": True},
                                ],
                                "specialAssignments": [{"rule": {"type": "other"}, "color": {"type": "fixed", "color": ORANGE}, "touched": True}],
                                "paletteId": "eui_amsterdam_color_blind",
                                "colorMode": {"type": "categorical"},
                            },
                        }
                    ],
                    "legend": {"isVisible": False},
                    "valueLabels": "show",
                },
            ),
        },
        {
            "id": "mt-p2-8153-d3-heat",
            "kind": "lens",
            "hide_title": False,
            "grid": {"x": 0, "y": 33, "w": 48, "h": 20, "i": "p-d3-heat"},
            "obj": _lens_obj(
                lens_id="mt-p2-8153-d3-heat",
                title="Bản đồ nhiệt thể loại × thời gian",
                visualization_type="lnsHeatmap",
                columns={
                    "x": _col("UTC · period_start", "date", "date_histogram", "period_start", {"interval": "auto", "includeEmptyRows": True, "dropPartials": False}),
                    "y": _col("Thể loại", "string", "terms", "genre", {"size": 12, "orderBy": {"type": "column", "columnId": "v"}, "orderDirection": "desc", "otherBucket": False, "missingBucket": False, "parentFormat": {"id": "terms"}}),
                    "v": _col("Lượt rating gắn genre", "number", "sum", "rating_count", {"emptyAsNull": True}),
                },
                column_order=["x", "y", "v"],
                layer_visualization={
                    "layerId": "mt-p2-8153-d3-heat-layer",
                    "layerType": "data",
                    "xAccessor": "x",
                    "yAccessor": "y",
                    "valueAccessor": "v",
                    "gridConfig": {
                        "isCellLabelVisible": False,
                        "isXAxisLabelVisible": True,
                        "isXAxisTitleVisible": True,
                        "isYAxisLabelVisible": True,
                        "isYAxisTitleVisible": True,
                    },
                    "legend": {"isVisible": True, "position": "right"},
                    "palette": {
                        "name": "custom",
                        "type": "palette",
                        "params": {
                            "steps": 5,
                            "name": "custom",
                            "reverse": False,
                            "continuity": "above",
                            "colorStops": [
                                {"color": "#e2f0f3", "stop": 0},
                                {"color": "#9ed0da", "stop": 25},
                                {"color": "#5aaec0", "stop": 50},
                                {"color": "#268ca2", "stop": 75},
                                {"color": "#176E83", "stop": 100},
                            ],
                            "stops": [
                                {"color": "#e2f0f3", "stop": 0},
                                {"color": "#9ed0da", "stop": 25},
                                {"color": "#5aaec0", "stop": 50},
                                {"color": "#268ca2", "stop": 75},
                                {"color": "#176E83", "stop": 100},
                            ],
                        },
                    },
                },
            ),
        },
        {
            "id": "mt-p2-8153-d3-note",
            "kind": "visualization",
            "hide_title": True,
            "grid": {"x": 0, "y": 53, "w": 48, "h": 4, "i": "p-d3-note"},
            "obj": _markdown_vis(
                "mt-p2-8153-d3-note",
                "Chú thích dữ liệu",
                "> **GENRE MULTI-LABEL:** Một phim có thể thuộc nhiều thể loại nên tổng lượt gắn genre không phải tổng rating hệ thống. "
                "Đối chứng `274.480` ở source đã cho; chỉ chọn **một** `period_type` tại mỗi thời điểm.",
                font_size=12,
            ),
        },
    ]

    panels_json = []
    dashboard_refs = [
        {"type": "index-pattern", "id": DATA_VIEW_ID, "name": "controlGroup_ctrl-period-type:optionsListDataView"},
        {"type": "index-pattern", "id": DATA_VIEW_ID, "name": "controlGroup_ctrl-genre:optionsListDataView"},
    ]
    for idx, p in enumerate(panels_spec):
        ref_name = f"panel_{idx}"
        dashboard_refs.append({"type": p["kind"], "id": p["id"], "name": ref_name})
        panels_json.append(
            {
                "version": KIBANA_VERSION,
                "type": p["kind"],
                "gridData": p["grid"],
                "panelIndex": p["grid"]["i"],
                "embeddableConfig": {
                    "enhancements": {},
                    "hidePanelTitles": p["hide_title"],
                },
                "panelRefName": ref_name,
            }
        )

    control_group_input = {
        "chainingSystem": "HIERARCHICAL",
        "controlStyle": "oneLine",
        "ignoreParentSettingsJSON": json.dumps(
            {"ignoreFilters": False, "ignoreQuery": False, "ignoreTimerange": False, "ignoreValidations": False}
        ),
        "panelsJSON": json.dumps(
            {
                "ctrl-period-type": {
                    "order": 0,
                    "width": "medium",
                    "grow": False,
                    "type": "optionsListControl",
                    "explicitInput": {
                        "id": "ctrl-period-type",
                        "dataViewId": DATA_VIEW_ID,
                        "fieldName": "period_type",
                        "title": "Độ chi tiết",
                        "selectedOptions": ["month"],
                        "singleSelect": True,
                        "runOnce": False,
                        "searchTechnique": "prefix",
                        "sort": {"by": "_count", "direction": "desc"},
                        "enhancements": {},
                    },
                },
                "ctrl-genre": {
                    "order": 1,
                    "width": "medium",
                    "grow": True,
                    "type": "optionsListControl",
                    "explicitInput": {
                        "id": "ctrl-genre",
                        "dataViewId": DATA_VIEW_ID,
                        "fieldName": "genre",
                        "title": "Thể loại",
                        "selectedOptions": [],
                        "singleSelect": False,
                        "runOnce": False,
                        "searchTechnique": "prefix",
                        "sort": {"by": "_count", "direction": "desc"},
                        "enhancements": {},
                    },
                },
            },
            ensure_ascii=False,
        ),
    }

    dashboard = {
        "id": DASHBOARD_ID,
        "type": "dashboard",
        "managed": False,
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "8.15.0",
        "attributes": {
            "title": "D3 · Thể loại nào đang được quan tâm? | Movie Trend Phase 2",
            "description": "So sánh thể loại theo thời gian, thứ hạng từng kỳ và bản đồ nhiệt (Batch P3 - Giang).",
            "hits": 0,
            "timeRestore": True,
            "timeFrom": TIME_FROM,
            "timeTo": TIME_TO,
            "refreshInterval": {"pause": True, "value": 0},
            "optionsJSON": json.dumps(
                {"useMargins": True, "syncColors": False, "syncCursor": True, "syncTooltips": True, "hidePanelTitles": False}
            ),
            "panelsJSON": json.dumps(panels_json, ensure_ascii=False),
            "controlGroupInput": control_group_input,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {"query": {"query": "", "language": "kuery"}, "filter": []},
                    ensure_ascii=False,
                )
            },
        },
        "references": dashboard_refs,
    }

    objects = [p["obj"] for p in panels_spec] + [dashboard]
    manifest = {
        "owner": OWNER,
        "contract_version": CONTRACT_VERSION,
        "kibana_version": KIBANA_VERSION,
        "dashboard_id": DASHBOARD_ID,
        "data_view_ids": [DATA_VIEW_ID],
        "object_ids": [o["id"] for o in objects],
        "ndjson_file": NDJSON_PATH.name,
        "exported_at": EXPORTED_AT,
    }
    return objects, manifest


def render() -> tuple[str, str]:
    objects, manifest = build_objects()
    ndjson_text = "".join(json.dumps(obj, ensure_ascii=False) + "\n" for obj in objects)
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    return ndjson_text, manifest_text


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="chỉ kiểm tra file đã sinh có khớp với script")
    args = parser.parse_args()
    ndjson_text, manifest_text = render()
    if args.check:
        if not NDJSON_PATH.is_file() or NDJSON_PATH.read_text(encoding="utf-8") != ndjson_text:
            print(f"[ERROR] {NDJSON_PATH} lệch với build_d03_saved_objects.py", file=sys.stderr)
            return 1
        if not MANIFEST_PATH.is_file() or MANIFEST_PATH.read_text(encoding="utf-8") != manifest_text:
            print(f"[ERROR] {MANIFEST_PATH} lệch với build_d03_saved_objects.py", file=sys.stderr)
            return 1
        print("[OK] D3 saved objects (NDJSON + manifest) khớp build_d03_saved_objects.py")
        return 0
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(NDJSON_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(ndjson_text)
    with open(MANIFEST_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(manifest_text)
    print(f"[OK] Đã ghi {NDJSON_PATH} ({ndjson_text.count(chr(10))} objects)")
    print(f"[OK] Đã ghi {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
