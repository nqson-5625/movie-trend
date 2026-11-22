from __future__ import annotations

from typing import Any, Dict, List

import pytest

from modules.elasticsearch.common.publication.batch_publisher import PublicationError
from modules.elasticsearch.common.publication.versioned_publisher import (
    VersionedReadyReport,
    publish_versioned_alias,
    validate_versioned_ready_report,
)


def _report() -> VersionedReadyReport:
    return VersionedReadyReport(
        alias_name="analytics_tmdb_signal",
        physical_index="analytics_tmdb_signal-vrun001",
        version="run001",
        analytics_run_id="run001",
        schema_version="phase2-3.0",
        row_count=2,
        bulk_error_count=0,
        duplicate_key_count=0,
        completed_at="2026-11-22T08:00:00Z",
    )


class FakeVersionedES:
    def __init__(self) -> None:
        self.aliases: Dict[str, List[str]] = {
            "analytics_tmdb_signal": ["analytics_tmdb_signal-vold"]
        }
        self.counts = {
            "analytics_tmdb_signal-vrun001": 2,
            "analytics_tmdb_signal-vold": 1,
        }
        self.fail_alias_smoke = False

    def index_exists(self, name: str) -> bool:
        return name in self.counts

    def count(self, name: str) -> int:
        return self.counts[name]

    def get_mapping(self, name: str) -> Dict[str, Any]:
        return {
            name: {
                "mappings": {
                    "properties": {
                        "schema_version": {"type": "keyword"},
                        "analytics_run_id": {"type": "keyword"},
                        "published_at": {"type": "date"},
                    }
                }
            }
        }

    def search_smoke(self, name: str) -> Dict[str, Any]:
        if name in self.aliases:
            count = sum(self.counts[index] for index in self.aliases[name])
            if self.fail_alias_smoke:
                count = 0
        else:
            count = self.counts[name]
        return {"index_or_alias": name, "searchable": True, "count_seen": count}

    def search(self, name: str, body: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "aggregations": {
                "schema_versions": {
                    "buckets": [{"key": "phase2-3.0", "doc_count": self.counts[name]}]
                },
                "analytics_runs": {
                    "buckets": [{"key": "run001", "doc_count": self.counts[name]}]
                },
                "missing_published_at": {"doc_count": 0},
            }
        }

    def get_aliases(self, names=None) -> Dict[str, Any]:
        wanted = set(names or [])
        result: Dict[str, Any] = {}
        for alias, indices in self.aliases.items():
            if wanted and alias not in wanted:
                continue
            for index in indices:
                result.setdefault(index, {"aliases": {}})["aliases"][alias] = {}
        return result

    def update_aliases(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        for action in actions:
            if "remove" in action:
                item = action["remove"]
                self.aliases[item["alias"]].remove(item["index"])
            else:
                item = action["add"]
                self.aliases.setdefault(item["alias"], []).append(item["index"])
        return {"acknowledged": True}


def test_versioned_publish_swaps_one_alias_and_writes_rollback(tmp_path) -> None:
    es = FakeVersionedES()
    result = publish_versioned_alias(es, _report(), report_dir=tmp_path)  # type: ignore[arg-type]
    assert result["status"] == "PUBLISHED"
    assert es.aliases["analytics_tmdb_signal"] == ["analytics_tmdb_signal-vrun001"]
    assert (tmp_path / "rollback_analytics_tmdb_signal_run001.json").exists()


def test_versioned_publish_rolls_back_when_alias_smoke_fails(tmp_path) -> None:
    es = FakeVersionedES()
    es.fail_alias_smoke = True
    with pytest.raises(PublicationError, match="rollback=ROLLED_BACK"):
        publish_versioned_alias(es, _report(), report_dir=tmp_path)  # type: ignore[arg-type]
    assert es.aliases["analytics_tmdb_signal"] == ["analytics_tmdb_signal-vold"]


def test_versioned_report_rejects_alias_or_partial_build() -> None:
    bad_alias = VersionedReadyReport(**{**_report().__dict__, "alias_name": "analytics_growth"})
    with pytest.raises(ValueError, match="allowlist"):
        validate_versioned_ready_report(bad_alias)
    partial = VersionedReadyReport(**{**_report().__dict__, "bulk_error_count": 1})
    with pytest.raises(ValueError, match="Bulk error"):
        validate_versioned_ready_report(partial)
