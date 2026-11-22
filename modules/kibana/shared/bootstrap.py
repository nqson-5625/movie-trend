"""Create Phase 2 Kibana Spaces and canonical Data Views from the runtime contract."""

from __future__ import annotations

import json
import os
import sys
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import requests
from dotenv import load_dotenv

from modules.kibana.data_views.definitions import (
    PROJECT_ROOT,
    data_view_definitions,
    load_contract,
)
SPACE_DEFINITIONS = {
    "dev": {
        "name": "Movie Trend Dev",
        "description": "Development Space for Movie Trend Phase 2.",
    },
    "integration": {
        "name": "Movie Trend Integration",
        "description": "Shared integration Space for Phase 2 dashboards.",
    },
}
class KibanaClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        session: requests.Session | None = None,
    ) -> None:
        parsed_url = urlparse(base_url)
        if (
            parsed_url.scheme not in {"http", "https"}
            or not parsed_url.hostname
            or parsed_url.username is not None
            or parsed_url.password is not None
        ):
            raise ValueError(
                "KIBANA_HTTP_URL phải là URL http(s) hợp lệ và không chứa credentials"
            )
        self.base_url = base_url.rstrip("/") + "/"
        self.session = session or requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({"kbn-xsrf": "true", "Content-Type": "application/json"})

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> requests.Response:
        url = urljoin(self.base_url, path.lstrip("/"))
        response = self.session.request(
            method,
            url,
            json=json_body,
            timeout=30,
            allow_redirects=False,
        )
        if response.status_code not in expected_statuses:
            detail = response.text.strip().replace("\n", " ")[:500]
            suffix = f": {detail}" if detail else ""
            raise RuntimeError(
                f"Kibana API {method} {path} trả HTTP {response.status_code}{suffix}"
            )
        return response


def _ensure_space(client: KibanaClient, space_id: str, definition: dict[str, str]) -> None:
    encoded_id = quote(space_id, safe="")
    path = f"api/spaces/space/{encoded_id}"
    current = client.request("GET", path, expected_statuses=(200, 404))
    if current.status_code == 404:
        payload = {"id": space_id, **definition}
        created = client.request(
            "POST",
            "api/spaces/space",
            json_body=payload,
            expected_statuses=(200, 201, 409),
        )
        if created.status_code != 409:
            return
        current = client.request("GET", path)

    actual = current.json()
    expected = {"id": space_id, **definition}
    actual_subset = {key: actual.get(key) for key in expected}
    if actual_subset != expected:
        raise RuntimeError(
            f"Kibana Space {space_id!r} tồn tại nhưng không khớp cấu hình; "
            "không tự ghi đè Space."
        )


def _ensure_data_view(
    client: KibanaClient,
    space_id: str,
    definition: dict[str, Any],
) -> None:
    data_view_id = definition["id"]
    encoded_id = quote(data_view_id, safe="")
    prefix = f"s/{quote(space_id, safe='')}/api/data_views"
    path = f"{prefix}/data_view/{encoded_id}"
    current = client.request("GET", path, expected_statuses=(200, 404))

    if current.status_code == 404:
        created = client.request(
            "POST",
            f"{prefix}/data_view",
            json_body={"data_view": definition},
            expected_statuses=(200, 201, 409),
        )
        if created.status_code != 409:
            return
        current = client.request("GET", path)

    payload = current.json().get("data_view", current.json())
    expected = {
        "id": definition["id"],
        "name": definition["name"],
        "title": definition["title"],
        "timeFieldName": definition.get("timeFieldName"),
        "allowNoIndex": definition["allowNoIndex"],
    }
    actual = {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "title": payload.get("title"),
        "timeFieldName": payload.get("timeFieldName"),
        "allowNoIndex": payload.get("allowNoIndex", False),
    }
    if actual != expected:
        raise RuntimeError(
            f"Kibana Data View {data_view_id!r} lệch runtime contract; "
            "không tự ghi đè cấu hình hiện có."
        )


def _validate_kibana(client: KibanaClient, contract: dict[str, Any]) -> str:
    response = client.request("GET", "api/status")
    payload = response.json()
    actual_version = payload.get("version", {}).get("number")
    expected_version = contract.get("kibana_version")
    if actual_version != expected_version:
        raise RuntimeError(
            f"Kibana version {actual_version!r} không khớp runtime contract "
            f"{expected_version!r}"
        )
    status_level = payload.get("status", {}).get("overall", {}).get("level")
    if status_level != "available":
        raise RuntimeError(f"Kibana chưa sẵn sàng (overall.level={status_level!r})")
    return actual_version


def bootstrap(client: KibanaClient, contract: dict[str, Any]) -> dict[str, Any]:
    kibana_version = _validate_kibana(client, contract)
    definitions = data_view_definitions(contract)
    for space_id, space in SPACE_DEFINITIONS.items():
        _ensure_space(client, space_id, space)
    for definition in definitions:
        _ensure_data_view(client, "integration", definition)
    return {
        "spaces": sorted(SPACE_DEFINITIONS),
        "data_views": [view["id"] for view in definitions],
        "space_for_data_views": "integration",
        "kibana_version": kibana_version,
    }


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    base_url = os.environ.get("KIBANA_HTTP_URL", "http://127.0.0.1:5601")
    username = os.environ.get("KIBANA_USERNAME") or os.environ.get("ES_USERNAME", "")
    password = os.environ.get("KIBANA_PASSWORD") or os.environ.get("ES_PASSWORD", "")
    if not username or not password or password == "replace_with_secret_value":
        print(
            "Cần cấu hình KIBANA_USERNAME/KIBANA_PASSWORD (hoặc fallback "
            "ES_USERNAME/ES_PASSWORD) của tài khoản Kibana administrator.",
            file=sys.stderr,
        )
        return 2
    try:
        result = bootstrap(KibanaClient(base_url, username, password), load_contract())
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        print(f"Kibana bootstrap thất bại: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
