"""Hàm mã hóa tuple khóa -> _id xác định (SHA-256) dùng chung cho Phase 2 (đóng băng v1)."""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

KEY_SERIALIZER_VERSION = "v1"


def _encode_value(value: Any) -> list[Any]:
    """Gắn nhãn kiểu dữ liệu (type-tag) chuẩn hóa để không bao giờ va chạm khóa."""
    if value is None:
        return ["null", None]

    # Lưu ý: phải kiểm tra bool trước int vì bool kế thừa int trong Python
    if isinstance(value, bool):
        return ["bool", value]

    if isinstance(value, int):
        return ["int", value]

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Không chấp nhận NaN/Infinity trong khóa: {value!r}")
        normalized = 0.0 if value == 0.0 else value
        return ["float", normalized]

    # Lưu ý: phải kiểm tra datetime trước date vì datetime kế thừa date trong Python
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(
                f"Timestamp trong khóa BẮT BUỘC phải có tzinfo (timezone-aware), nhận naive: {value!r}"
            )
        utc_dt = value.astimezone(timezone.utc)
        if utc_dt.microsecond == 0:
            iso = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            iso = utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return ["timestamp", iso]

    if isinstance(value, date):
        return ["date", value.isoformat()]

    if isinstance(value, str):
        return ["str", unicodedata.normalize("NFC", value)]

    raise TypeError(f"Kiểu dữ liệu không được hỗ trợ trong key serializer: {type(value).__name__}")


def serialize_key_v1(key_tuple: Sequence[Any]) -> str:
    """Chuyển tuple khóa có thứ tự -> chuỗi JSON UTF-8 chuẩn -> băm SHA-256 (64 ký tự hex)."""
    if not isinstance(key_tuple, (tuple, list)) or len(key_tuple) == 0:
        raise ValueError("key_tuple phải là tuple hoặc list không rỗng")

    tagged = [_encode_value(item) for item in key_tuple]
    canonical_json = json.dumps(
        {"v": KEY_SERIALIZER_VERSION, "k": tagged},
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def build_record_id(record: Mapping[str, Any], key_columns: Sequence[str]) -> str:
    """Lấy đúng tuple khóa theo danh sách cột từ 1 dict/Row rồi băm bằng serialize_key_v1."""
    values = tuple(record[col] for col in key_columns)
    return serialize_key_v1(values)
