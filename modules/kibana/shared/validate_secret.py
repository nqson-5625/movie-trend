"""Validate the shape of the Kubernetes Secret consumed by Kibana."""

from __future__ import annotations

import base64
import binascii
import json
import sys
from typing import Any

PASSWORD_KEY = "kibana-system-password"
ENCRYPTION_KEYS = (
    "security-encryption-key",
    "saved-objects-encryption-key",
    "reporting-encryption-key",
)


def validate_secret(secret: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(secret, dict):
        raise ValueError("Kubernetes Secret phải là JSON object")
    data = secret.get("data")
    if not isinstance(data, dict):
        raise ValueError("Secret kibana-credentials thiếu trường data")

    decoded: dict[str, str] = {}
    for key in (PASSWORD_KEY, *ENCRYPTION_KEYS):
        encoded = data.get(key)
        if not isinstance(encoded, str) or not encoded:
            raise ValueError(f"Secret kibana-credentials thiếu key {key}")
        try:
            value = base64.b64decode(encoded, validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise ValueError(f"Secret key {key} không phải base64 UTF-8 hợp lệ") from exc
        if not value:
            raise ValueError(f"Secret key {key} không được rỗng")
        if any(character in value for character in ("\x00", "\r", "\n")):
            raise ValueError(f"Secret key {key} không được chứa NUL/newline")
        decoded[key] = value

    if decoded[PASSWORD_KEY] == "replace_with_secret_value":
        raise ValueError(f"Secret key {PASSWORD_KEY} vẫn là giá trị placeholder")
    for key in ENCRYPTION_KEYS:
        if len(decoded[key]) < 32:
            raise ValueError(f"Secret key {key} phải có ít nhất 32 ký tự")
    if len({decoded[key] for key in ENCRYPTION_KEYS}) != len(ENCRYPTION_KEYS):
        raise ValueError("Ba encryption key của Kibana phải độc lập")

    metadata = secret.get("metadata")
    secret_name = (
        metadata.get("name", "kibana-credentials")
        if isinstance(metadata, dict)
        else "kibana-credentials"
    )
    return {
        "secret": secret_name,
        "validated_keys": [PASSWORD_KEY, *ENCRYPTION_KEYS],
    }


def main() -> int:
    try:
        secret = json.load(sys.stdin)
        result = validate_secret(secret)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"Kibana Secret không hợp lệ: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
