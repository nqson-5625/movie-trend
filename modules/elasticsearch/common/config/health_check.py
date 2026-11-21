"""Kiểm tra sức khỏe cụm Elasticsearch, phiên bản và các templates đã cài."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .es_client import ESClient, load_runtime_config

REQUIRED_COMPONENTS = (
    "movie_trend_base_settings",
    "movie_trend_batch_meta",
)


def run_health_check(client: ESClient) -> Dict[str, Any]:
    """Kiểm tra health cluster, version ES, 2 component template dùng chung và liệt kê index templates."""
    contract = load_runtime_config()
    expected_prefix = ".".join(contract["elasticsearch_version"].split(".")[:2]) + "."

    info = client.info()
    health = client.health()
    version = str(info.get("version", {}).get("number", ""))
    status = str(health.get("status", "unknown"))

    # Kiểm tra 2 Component Template dùng chung đã tồn tại chưa
    missing_components: List[str] = []
    for name in REQUIRED_COMPONENTS:
        resp = client._request("GET", f"/_component_template/{name}", expected_statuses=(200, 404))
        if resp.status_code != 200:
            missing_components.append(name)

    # Liệt kê các Index Template movie_trend_* hiện có
    tpl_resp = client._request(
        "GET", "/_index_template/movie_trend_*", expected_statuses=(200, 404)
    )
    if tpl_resp.status_code == 200:
        data = tpl_resp.json()
        templates = data.get("index_templates", [])
        names: List[str] = []
        for t in templates:
            names.append(t["name"])
        installed_templates = sorted(names)
    else:
        installed_templates = []

    ok = (
        status in ("green", "yellow")
        and version.startswith(expected_prefix)
        and len(missing_components) == 0
    )

    return {
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cluster_name": health.get("cluster_name"),
        "status": status,
        "version": version,
        "expected_version_prefix": expected_prefix,
        "number_of_nodes": health.get("number_of_nodes"),
        "missing_components": missing_components,
        "installed_index_templates": installed_templates,
        "ok": ok,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Elasticsearch Phase 2 Health Check")
    parser.add_argument("--output", default=None, help="Đường dẫn lưu báo cáo JSON (tùy chọn)")
    args = parser.parse_args()

    try:
        report = run_health_check(ESClient(owner="admin"))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
