"""Cài đặt Component Templates dùng chung và toàn bộ Index Templates của các lane."""
from __future__ import annotations
import json
import os
from pathlib import Path
import requests
from dotenv import load_dotenv

ES_MODULE_DIR = Path(__file__).resolve().parents[2]  # modules/elasticsearch
PROJECT_ROOT = ES_MODULE_DIR.parents[1]              # movie-trend
COMPONENTS_DIR = Path(__file__).resolve().parent / "components"
LANES_DIR = ES_MODULE_DIR / "lanes"
TEMPLATE_PREFIX = "movie_trend_"

def main() -> None: 
    load_dotenv(PROJECT_ROOT/".env", verbose=True)
    es_url = os.getenv("ES_HTTP_URL")
    # print(es_url); 
    comp_files = sorted(COMPONENTS_DIR.glob("*.json"))
    for path in comp_files: 
        name = f"{TEMPLATE_PREFIX}{path.stem}"
        body = json.loads(path.read_text(encoding="utf-8"))
        resp = requests.put(f"{es_url}/_component_template/{name}", json=body, timeout=15); 
        resp.raise_for_status()
        print(f"[OK] component_template: {name}")

    # Bước 2: Tự động quét mọi lanes/*/templates/*.json và đẩy lên ES
    tpl_files = sorted(LANES_DIR.glob("*/templates/*.json"))
    for path in tpl_files:
        name = f"{TEMPLATE_PREFIX}{path.stem}"
        body = json.loads(path.read_text(encoding="utf-8"))
        resp = requests.put(f"{es_url}/_index_template/{name}", json=body, timeout=15)
        resp.raise_for_status()
        check = requests.get(f"{es_url}/_index_template/{name}", timeout=15)
        check.raise_for_status()
        print(f"[OK] index_template:     {name}")


    print(
        f"\nHoàn tất: {len(comp_files)} component templates, "
        f"{len(tpl_files)} lane index templates."
    )
if __name__ == "__main__":
    main()