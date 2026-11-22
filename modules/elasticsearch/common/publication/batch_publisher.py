"""Công bố đồng thời 6 Batch aliases trong 1 yêu cầu POST /_aliases và tự động rollback khi lỗi."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from ..config.es_client import ESClient, PROJECT_ROOT, validate_index_name
from .ready_report import (
    DEFAULT_BATCH_ALIASES,
    ReadyReport,
    load_ready_report,
    verify_physical_indices_in_es,
)

DEFAULT_PUBLICATION_MARKER = (
    PROJECT_ROOT / "data" / "local" / "phase2" / "es_reports" / "publication_marker.json"
)
DEFAULT_AUDIT_DIR = PROJECT_ROOT / "data" / "local" / "phase2" / "es_reports"


class PublicationError(RuntimeError):
    """Ném ra khi quá trình chuyển 6 Batch aliases hoặc kiểm tra hậu kiểm thất bại."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def get_current_alias_targets(client: ESClient, aliases: Sequence[str]) -> Dict[str, List[str]]:
    """Đọc từ ES xem hiện tại mỗi alias đang trỏ tới physical index nào."""
    clean_aliases = [validate_index_name(a) for a in aliases]
    raw = client.get_aliases(clean_aliases)
    mapping: Dict[str, List[str]] = {alias: [] for alias in clean_aliases}
    for index_name, meta in raw.items():
        index_aliases = meta.get("aliases", {}) if isinstance(meta, dict) else {}
        for alias in clean_aliases:
            if alias in index_aliases:
                mapping[alias].append(index_name)
    for alias in mapping:
        mapping[alias].sort()
    return mapping


def build_atomic_alias_actions(
    before_map: Dict[str, List[str]],
    target_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Tạo danh sách actions cho POST /_aliases: chỉ remove alias đang tồn tại, gắn must_exist."""
    actions: List[Dict[str, Any]] = []
    for raw_alias, raw_new_idx in target_map.items():
        alias = validate_index_name(raw_alias)
        new_idx = validate_index_name(raw_new_idx)
        existing_indices = list(before_map.get(alias, []))

        for old_idx in existing_indices:
            if old_idx != new_idx:
                actions.append(
                    {
                        "remove": {
                            "index": validate_index_name(old_idx),
                            "alias": alias,
                            "must_exist": True,
                        }
                    }
                )
        if existing_indices != [new_idx]:
            actions.append({"add": {"index": new_idx, "alias": alias}})
    return actions


def build_restore_alias_actions(
    current_map: Dict[str, List[str]],
    restore_map: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    """Tạo actions để khôi phục 6 alias về đúng trạng thái trước khi swap."""
    actions: List[Dict[str, Any]] = []
    for raw_alias, orig_indices in restore_map.items():
        alias = validate_index_name(raw_alias)
        cur_indices = set(current_map.get(alias, []))
        desired_indices = {validate_index_name(idx) for idx in orig_indices}

        for extra_idx in sorted(cur_indices - desired_indices):
            actions.append(
                {
                    "remove": {
                        "index": validate_index_name(extra_idx),
                        "alias": alias,
                        "must_exist": True,
                    }
                }
            )
        for missing_idx in sorted(desired_indices - cur_indices):
            actions.append({"add": {"index": missing_idx, "alias": alias}})
    return actions


def inspect_aliases_response(resp: Dict[str, Any]) -> None:
    """Kiểm tra kỹ response từ POST /_aliases kể cả khi HTTP trả 200."""
    if not isinstance(resp, dict):
        raise PublicationError(f"Response _aliases sai định dạng: {type(resp).__name__}")
    if not resp.get("acknowledged", False):
        raise PublicationError(f"ES _aliases không acknowledged: {resp!r}")
    if resp.get("errors"):
        raise PublicationError(f"ES _aliases trả về errors=true: {resp!r}")

    action_results = resp.get("action_results", [])
    if isinstance(action_results, list):
        for idx, item in enumerate(action_results):
            if isinstance(item, dict) and (item.get("error") or item.get("acknowledged") is False):
                raise PublicationError(f"Action thứ {idx} trong _aliases thất bại: {item!r}")


def rollback_batch_aliases(
    client: ESClient,
    restore_map: Dict[str, List[str]],
    marker_path: Optional[Path] = None,
    reason: str = "manual_rollback",
) -> Dict[str, Any]:
    """Đảo ngược toàn bộ alias về restore_map và kiểm tra lại ánh xạ thực tế trên ES."""
    aliases = list(restore_map.keys())
    current_map = get_current_alias_targets(client, aliases)
    actions = build_restore_alias_actions(current_map, restore_map)
    resp: Dict[str, Any] = {"acknowledged": True, "noop": True}
    if actions:
        resp = client.update_aliases(actions)
        inspect_aliases_response(resp)

    verified_map = get_current_alias_targets(client, aliases)
    expected_norm = {a: sorted(v) for a, v in restore_map.items()}
    if verified_map != expected_norm:
        raise PublicationError(
            f"Xác minh sau rollback thất bại: kỳ vọng {expected_norm!r}, thực tế {verified_map!r}"
        )

    if marker_path is not None:
        _atomic_write_json(
            Path(marker_path),
            {
                "locked": True,
                "status": "ROLLED_BACK",
                "reason": reason,
                "aliases": verified_map,
                "updated_at": _utc_now_iso(),
            },
        )

    return {
        "status": "ROLLED_BACK",
        "actions": actions,
        "response": resp,
        "verified_aliases": verified_map,
    }


def publish_batch_aliases(
    client: ESClient,
    p2_report: ReadyReport,
    p3_report: ReadyReport,
    marker_path: Path | str = DEFAULT_PUBLICATION_MARKER,
    audit_dir: Path | str = DEFAULT_AUDIT_DIR,
    expected_aliases: Sequence[str] = DEFAULT_BATCH_ALIASES,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Đối soát 6 physical index trên ES, gạt đồng thời 6 alias, xác minh và ghi publication marker."""
    entries = verify_physical_indices_in_es(
        client, p2_report, p3_report, expected_aliases=expected_aliases
    )
    target_map = {alias: entry.physical_index for alias, entry in entries.items()}
    before_map = get_current_alias_targets(client, list(target_map.keys()))
    actions = build_atomic_alias_actions(before_map, target_map)

    marker_file = Path(marker_path)
    audit_root = Path(audit_dir)
    audit_root.mkdir(parents=True, exist_ok=True)
    run_tag = p2_report.batch_run_id
    snapshot_file = audit_root / f"rollback_snapshot_{run_tag}.json"
    rollback_cmd = (
        "python -m modules.elasticsearch.common.publication.batch_publisher "
        f"rollback --snapshot {snapshot_file} --marker {marker_file}"
    )

    if dry_run:
        return {
            "status": "DRY_RUN",
            "batch_run_id": run_tag,
            "before_map": before_map,
            "target_map": target_map,
            "actions": actions,
            "rollback_command": rollback_cmd,
        }

    _atomic_write_json(
        snapshot_file,
        {
            "batch_run_id": run_tag,
            "captured_at": _utc_now_iso(),
            "before_map": before_map,
            "target_map": target_map,
        },
    )

    aliases_response: Dict[str, Any] = {"acknowledged": True, "noop": True}
    after_map: Dict[str, List[str]] = {}
    try:
        if actions:
            aliases_response = client.update_aliases(actions)
            inspect_aliases_response(aliases_response)

        after_map = get_current_alias_targets(client, list(target_map.keys()))
        mismatches = {
            alias: {"expected": [phys], "actual": after_map.get(alias, [])}
            for alias, phys in target_map.items()
            if after_map.get(alias) != [phys]
        }
        if mismatches:
            raise PublicationError(f"Ánh xạ alias sau chuyển không khớp: {mismatches!r}")
        search_smokes = {
            alias: client.search_smoke(alias) for alias in target_map
        }
        smoke_mismatches = {
            alias: {"expected": entries[alias].row_count, "actual": smoke["count_seen"]}
            for alias, smoke in search_smokes.items()
            if smoke["count_seen"] != entries[alias].row_count
        }
        if smoke_mismatches:
            raise PublicationError(f"Search smoke sau publish không khớp: {smoke_mismatches!r}")
    except (RuntimeError, PublicationError) as exc:
        _atomic_write_json(
            marker_file,
            {
                "locked": True,
                "status": "ROLLBACK_TRIGGERED",
                "batch_run_id": run_tag,
                "reason": str(exc),
                "updated_at": _utc_now_iso(),
            },
        )
        rollback_info: Dict[str, Any]
        try:
            rollback_info = rollback_batch_aliases(
                client,
                before_map,
                marker_path=marker_file,
                reason=str(exc),
            )
        except Exception as rb_exc:  # noqa: BLE001 - giữ nguyên lỗi publish ban đầu
            rollback_info = {"status": "ROLLBACK_FAILED", "error": str(rb_exc)}

        fail_audit = {
            "status": "FAILED",
            "batch_run_id": run_tag,
            "batch_cutoff_time": p2_report.batch_cutoff_time,
            "ref_version": p2_report.ref_version,
            "manifest_sha256": p2_report.manifest_sha256,
            "before_map": before_map,
            "target_map": target_map,
            "actions": actions,
            "aliases_response": aliases_response,
            "observed_after_map": after_map,
            "error": str(exc),
            "rollback": rollback_info,
            "rollback_command": rollback_cmd,
            "timestamp": _utc_now_iso(),
        }
        _atomic_write_json(audit_root / f"publish_failed_{run_tag}.json", fail_audit)
        raise PublicationError(
            f"Công bố 6 alias thất bại, đã khóa marker ({rollback_info['status']}): {exc}"
        ) from exc

    published_at = _utc_now_iso()
    marker_payload = {
        "locked": False,
        "status": "PUBLISHED",
        "schema_version": p2_report.schema_version,
        "batch_run_id": run_tag,
        "batch_cutoff_time": p2_report.batch_cutoff_time,
        "ref_version": p2_report.ref_version,
        "manifest_sha256": p2_report.manifest_sha256,
        "aliases": target_map,
        "published_at": published_at,
        "search_smokes": search_smokes,
    }
    _atomic_write_json(marker_file, marker_payload)

    audit_payload = {
        **marker_payload,
        "p2_owner": p2_report.owner,
        "p3_owner": p3_report.owner,
        "before_map": before_map,
        "after_map": after_map,
        "actions": actions,
        "aliases_response": aliases_response,
        "rollback_snapshot": str(snapshot_file),
        "rollback_command": rollback_cmd,
    }
    audit_file = audit_root / f"publish_success_{run_tag}.json"
    _atomic_write_json(audit_file, audit_payload)

    return {
        **audit_payload,
        "marker_path": str(marker_file),
        "audit_path": str(audit_file),
    }


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish & rollback đồng bộ 6 Batch aliases (G2P)")
    sub = parser.add_subparsers(dest="command", required=True)

    pub_p = sub.add_parser("publish", help="Kiểm tra READY reports + ES và gạt đồng thời 6 aliases")
    pub_p.add_argument("--p2-report", required=True, help="Đường dẫn tới file p2_ready_<run>.json")
    pub_p.add_argument("--p3-report", required=True, help="Đường dẫn tới file p3_ready_<run>.json")
    pub_p.add_argument("--marker", default=str(DEFAULT_PUBLICATION_MARKER))
    pub_p.add_argument("--audit-dir", default=str(DEFAULT_AUDIT_DIR))
    pub_p.add_argument("--dry-run", action="store_true")

    rb_p = sub.add_parser("rollback", help="Rollback 6 Batch aliases từ file rollback_snapshot")
    rb_p.add_argument("--snapshot", required=True, help="Đường dẫn tới rollback_snapshot_<run>.json")
    rb_p.add_argument("--marker", default=str(DEFAULT_PUBLICATION_MARKER))

    args = parser.parse_args(argv)

    try:
        client = ESClient(owner="admin")
        if args.command == "publish":
            p2 = load_ready_report(args.p2_report)
            p3 = load_ready_report(args.p3_report)
            result = publish_batch_aliases(
                client,
                p2,
                p3,
                marker_path=Path(args.marker),
                audit_dir=Path(args.audit_dir),
                dry_run=args.dry_run,
            )
        else:
            snap = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
            restore_map = snap.get("before_map") or snap.get("aliases")
            if not isinstance(restore_map, dict) or not restore_map:
                raise ValueError(f"Snapshot {args.snapshot} không chứa 'before_map'")
            result = rollback_batch_aliases(
                client,
                restore_map,
                marker_path=Path(args.marker),
                reason=f"cli_rollback:{args.snapshot}",
            )
    except Exception as exc:  # noqa: BLE001 - trả exit code 1 rõ ràng cho CLI
        print(json.dumps({"status": "ERROR", "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
