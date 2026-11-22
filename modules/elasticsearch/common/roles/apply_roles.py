"""Install the least-privilege Elasticsearch roles used by Phase 2."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from modules.elasticsearch.common.config.es_client import ESClient

ROLES_FILE = Path(__file__).with_name("phase2_roles.json")


def apply_roles(client: ESClient | None = None) -> list[str]:
    es = client or ESClient(owner="admin")
    roles = json.loads(ROLES_FILE.read_text(encoding="utf-8"))
    installed: list[str] = []
    for name, body in roles.items():
        es._request("PUT", f"/_security/role/{name}", json_body=body)
        installed.append(name)
    return installed


def main() -> int:
    parser = argparse.ArgumentParser(description="Install Phase 2 Elasticsearch roles")
    parser.add_argument("--dry-run", action="store_true", help="Validate and list roles only")
    args = parser.parse_args()
    if args.dry_run:
        roles = json.loads(ROLES_FILE.read_text(encoding="utf-8"))
        print(json.dumps({"validated_roles": sorted(roles)}, ensure_ascii=False, indent=2))
        return 0
    installed = apply_roles()
    print(json.dumps({"installed_roles": installed}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
