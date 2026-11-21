"""Thư viện Elasticsearch REST Client dùng chung cho Phase 2 (Module 06)."""

from __future__ import annotations

import fnmatch
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[4]
RUNTIME_CONFIG_PATH = (
    PROJECT_ROOT / "contracts" / "phase2" / "common" / "phase2_runtime_config.json"
)

INDEX_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_\-\.]{0,254}$")
TRANSIENT_STATUSES = {429, 502, 503, 504}

# Quyền ghi theo từng owner (tuân thủ Single Writer của DATA_CONTRACT_Phase2.md)
# TODO(security): Bật ES xpack.security RBAC + TLS khi triển khai lên server/VPS dùng chung.
OWNER_WRITE_PATTERNS: Dict[str, List[str]] = {
    "p1": [],  # P1 chỉ đọc analytics_*
    "p2": [
        "movie_batch_stats*",
        "movie_daily_stats*",
        "analytics_timeseries*",
        "analytics_top_movies*",
        "dev_p2_*",
    ],
    "p3": [
        "genre_trends*",
        "analytics_genre_trends*",
        "dev_p3_*",
    ],
    "p4": [
        "analytics_growth*",
        "dev_p4_*",
    ],
    "p5": [
        "analytics_tmdb_signal*",
        "dev_p5_*",
    ],
    "spark_streaming": [
        "movie_realtime_stats",
        "genre_realtime",
        "tmdb_realtime",
    ],
    "admin": ["*"],
}


def validate_index_name(name: str) -> str:
    """Kiểm tra tên index/alias hợp lệ theo chuẩn ES và chống path traversal."""
    if not isinstance(name, str) or not name:
        raise ValueError("Tên index/alias không được rỗng")
    if ".." in name or not INDEX_NAME_RE.match(name):
        raise ValueError(f"Tên index/alias không hợp lệ: {name!r}")
    return name


def load_runtime_config() -> Dict[str, Any]:
    """Đọc file hợp đồng cấu hình chung phase2_runtime_config.json."""
    return json.loads(RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"))


@dataclass(frozen=True)
class BulkItemError:
    index: str
    doc_id: str
    status: int
    error_type: str
    reason: str


@dataclass(frozen=True)
class BulkResult:
    index_name: str
    total_sent: int
    succeeded_count: int
    failed_count: int
    errors: List[BulkItemError] = field(default_factory=list)


class BulkOperationError(RuntimeError):
    """Ném ra khi POST /_bulk có ít nhất 1 dòng lỗi (dù HTTP trả 200)."""

    def __init__(self, result: BulkResult):
        self.result = result
        preview = "; ".join(
            f"_id={e.doc_id} ({e.status} {e.error_type}: {e.reason})"
            for e in result.errors[:5]
        )
        super().__init__(
            f"Bulk upsert thất bại trên '{result.index_name}': "
            f"{result.failed_count}/{result.total_sent} dòng lỗi [{preview}]"
        )


class ESClient:
    """REST Client dùng chung cho Elasticsearch 8.x có kiểm tra owner & bắt lỗi Bulk."""

    def __init__(
        self,
        owner: str = "admin",
        base_url: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        if owner not in OWNER_WRITE_PATTERNS:
            raise ValueError(
                f"owner không hợp lệ: {owner!r}. Chọn một trong: {sorted(OWNER_WRITE_PATTERNS)}"
            )
        load_dotenv(PROJECT_ROOT / ".env", override=False)
        raw_url = (base_url or os.environ.get("ES_HTTP_URL", "http://127.0.0.1:9200")).strip().rstrip("/")
        parsed = urlparse(raw_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"ES_HTTP_URL không hợp lệ: {raw_url!r}")

        self.owner = owner
        self.base_url = raw_url
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = requests.Session()

    def _check_write_permission(self, index_name: str) -> None:
        patterns = OWNER_WRITE_PATTERNS[self.owner]
        if not any(fnmatch.fnmatchcase(index_name, pat) for pat in patterns):
            raise PermissionError(
                f"Owner '{self.owner}' KHÔNG có quyền ghi vào index '{index_name}'. "
                f"Chỉ được phép ghi vào: {patterns}"
            )

    def _check_delete_permission(self, index_name: str) -> None:
        if self.owner == "admin":
            return
        dev_prefix = f"dev_{self.owner}_*"
        if not fnmatch.fnmatchcase(index_name, dev_prefix):
            raise PermissionError(
                f"Owner '{self.owner}' chỉ được phép xóa index dev '{dev_prefix}', "
                f"không được xóa '{index_name}'."
            )

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        expected_statuses: Sequence[int] = (200, 201),
    ) -> requests.Response:
        if not path.startswith("/") or "://" in path:
            raise ValueError(f"Path ES không hợp lệ: {path!r}")
        url = f"{self.base_url}{path}"

        for attempt in range(self.max_retries + 1):
            try:
                resp = self._session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=json_body,
                    data=data,
                    headers=headers,
                    timeout=self.timeout,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt >= self.max_retries:
                    raise
                time.sleep(0.5 * (2 ** attempt))
                continue

            if resp.status_code in TRANSIENT_STATUSES and attempt < self.max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue

            if resp.status_code not in expected_statuses:
                raise RuntimeError(
                    f"ES {method.upper()} {path} lỗi HTTP {resp.status_code}: {resp.text[:500]}"
                )
            return resp

        raise RuntimeError(f"ES {method.upper()} {path} thất bại sau {self.max_retries + 1} lần thử")

    # ---- Các hàm tiện ích cơ bản ----
    def info(self) -> Dict[str, Any]:
        return self._request("GET", "/").json()

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/_cluster/health").json()

    def index_exists(self, index_name: str) -> bool:
        clean = validate_index_name(index_name)
        return self._request("HEAD", f"/{clean}", expected_statuses=(200, 404)).status_code == 200

    def delete_index(self, index_name: str, ignore_missing: bool = True) -> None:
        clean = validate_index_name(index_name)
        self._check_delete_permission(clean)
        statuses = (200, 404) if ignore_missing else (200,)
        self._request("DELETE", f"/{clean}", expected_statuses=statuses)

    def count(self, index_or_alias: str) -> int:
        clean = validate_index_name(index_or_alias)
        return int(self._request("GET", f"/{clean}/_count").json()["count"])

    def search(self, index_or_alias: str, body: Dict[str, Any]) -> Dict[str, Any]:
        clean = validate_index_name(index_or_alias)
        return self._request("POST", f"/{clean}/_search", json_body=body).json()

    def get_aliases(self, names: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        if names:
            clean = ",".join(validate_index_name(n) for n in names)
            path = f"/_alias/{clean}"
        else:
            path = "/_alias"
        resp = self._request("GET", path, expected_statuses=(200, 404))
        return {} if resp.status_code == 404 else resp.json()

    def update_aliases(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        if self.owner != "admin":
            raise PermissionError("Chỉ owner 'admin' (P5 publisher) mới được chuyển alias.")
        return self._request("POST", "/_aliases", json_body={"actions": actions}).json()

    # ---- Ghi hàng loạt có bắt lỗi từng item ----
    def bulk_upsert(
        self,
        index_name: str,
        records: Iterable[Dict[str, Any]],
        id_field: str = "_id",
        batch_size: int = 1000,
        refresh: bool = False,
        raise_on_error: bool = True,
    ) -> BulkResult:
        clean = validate_index_name(index_name)
        self._check_write_permission(clean)

        rec_list = list(records)
        if not rec_list:
            return BulkResult(clean, 0, 0, 0, [])

        errors: List[BulkItemError] = []
        succeeded = 0
        total_sent = 0

        for start in range(0, len(rec_list), batch_size):
            chunk = rec_list[start : start + batch_size]
            lines: List[str] = []
            chunk_ids: List[str] = []

            for raw in chunk:
                doc = dict(raw)
                doc_id = doc.pop(id_field, None)
                if doc_id is None or str(doc_id) == "":
                    raise ValueError(f"Thiếu khóa định danh '{id_field}' trong record")
                doc_id_str = str(doc_id)
                chunk_ids.append(doc_id_str)

                # allow_nan=False chặn NaN/Infinity ngay trước khi gửi
                lines.append(json.dumps({"index": {"_index": clean, "_id": doc_id_str}}, allow_nan=False))
                lines.append(json.dumps(doc, allow_nan=False, ensure_ascii=False))

            body = ("\n".join(lines) + "\n").encode("utf-8")
            params = {"refresh": "true"} if refresh else None
            resp = self._request(
                "POST",
                "/_bulk",
                params=params,
                data=body,
                headers={"Content-Type": "application/x-ndjson"},
                expected_statuses=(200,),
            ).json()

            items = resp.get("items", [])
            total_sent += len(chunk)
            for idx, item in enumerate(items):
                _, outcome = next(iter(item.items()))
                status = int(outcome.get("status", 200))
                err = outcome.get("error")
                if err is not None or status >= 400:
                    err_type = err.get("type", "bulk_error") if isinstance(err, dict) else "bulk_error"
                    reason = err.get("reason", str(err)) if isinstance(err, dict) else str(err)
                    errors.append(
                        BulkItemError(
                            index=str(outcome.get("_index", clean)),
                            doc_id=str(outcome.get("_id", chunk_ids[idx])),
                            status=status,
                            error_type=err_type,
                            reason=reason,
                        )
                    )
                else:
                    succeeded += 1

        result = BulkResult(clean, total_sent, succeeded, len(errors), errors)
        if result.failed_count > 0 and raise_on_error:
            raise BulkOperationError(result)
        return result

    # ---- Đọc đủ 100% dữ liệu bằng Point-in-Time (PIT) + search_after ----
    def search_all_pit(
        self,
        index_or_alias: str,
        page_size: int = 5000,
        keep_alive: str = "2m",
        query: Optional[Dict[str, Any]] = None,
        include_id: bool = True,
    ) -> List[Dict[str, Any]]:
        clean = validate_index_name(index_or_alias)
        pit_id = self._request(
            "POST", f"/{clean}/_pit", params={"keep_alive": keep_alive}
        ).json()["id"]

        docs: List[Dict[str, Any]] = []
        search_after: Optional[List[Any]] = None
        try:
            while True:
                body: Dict[str, Any] = {
                    "size": page_size,
                    "pit": {"id": pit_id, "keep_alive": keep_alive},
                    "sort": [{"_shard_doc": "asc"}],
                }
                if query is not None:
                    body["query"] = query
                if search_after is not None:
                    body["search_after"] = search_after

                data = self._request("POST", "/_search", json_body=body).json()
                pit_id = data.get("pit_id", pit_id)
                hits = data.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for h in hits:
                    row = dict(h.get("_source") or {})
                    if include_id:
                        row["_id"] = h["_id"]
                    docs.append(row)

                search_after = hits[-1].get("sort")
                if len(hits) < page_size or search_after is None:
                    break
        finally:
            self._request(
                "DELETE", "/_pit", json_body={"id": pit_id}, expected_statuses=(200, 404)
            )
        return docs