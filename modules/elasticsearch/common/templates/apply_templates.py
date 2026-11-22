"""Cài đặt Component Templates dùng chung và toàn bộ Index Templates của các lane."""
from __future__ import annotations
import json
from pathlib import Path

from modules.elasticsearch.common.config.es_client import ESClient

ES_MODULE_DIR = Path(__file__).resolve().parents[2]  # modules/elasticsearch
PROJECT_ROOT = ES_MODULE_DIR.parents[1]              # movie-trend
COMPONENTS_DIR = Path(__file__).resolve().parent / "components"
LANES_DIR = ES_MODULE_DIR / "lanes"
TEMPLATE_PREFIX = "movie_trend_"

def main() -> None: 
    client = ESClient(owner="admin")
    comp_files = sorted(COMPONENTS_DIR.glob("*.json"))
    for path in comp_files: 
        name = f"{TEMPLATE_PREFIX}{path.stem}"
        body = json.loads(path.read_text(encoding="utf-8"))
        client._request("PUT", f"/_component_template/{name}", json_body=body)
        client._request("GET", f"/_component_template/{name}")
        print(f"[OK] component_template: {name}")

    # Bước 2: Tự động quét mọi lanes/*/templates/*.json và đẩy lên ES
    tpl_files = sorted(LANES_DIR.glob("*/templates/*.json"))
    for path in tpl_files:
        name = f"{TEMPLATE_PREFIX}{path.stem}"
        body = json.loads(path.read_text(encoding="utf-8"))
        client._request("PUT", f"/_index_template/{name}", json_body=body)
        client._request("GET", f"/_index_template/{name}")
        print(f"[OK] index_template:     {name}")


    print(
        f"\nHoàn tất: {len(comp_files)} component templates, "
        f"{len(tpl_files)} lane index templates."
    )
if __name__ == "__main__":
    main()
