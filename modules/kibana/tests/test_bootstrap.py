from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from modules.elasticsearch.common.roles.apply_roles import ROLES_FILE
from modules.kibana.shared.bootstrap import (
    SPACE_DEFINITIONS,
    _ensure_data_view,
    _ensure_space,
    _validate_kibana,
    bootstrap,
    data_view_definitions,
    load_contract,
)
from modules.kibana.shared.validate_secret import ENCRYPTION_KEYS, validate_secret

KIBANA_ROOT = Path(__file__).resolve().parents[1]


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self) -> dict[str, Any]:
        return self._payload


class FakeKibanaClient:
    def __init__(self, get_responses: list[FakeResponse]):
        self.get_responses = iter(get_responses)
        self.posts: list[tuple[str, dict[str, Any]]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> FakeResponse:
        if method == "GET":
            response = next(self.get_responses)
        else:
            self.posts.append((path, json_body or {}))
            response = FakeResponse(201)
        assert response.status_code in expected_statuses
        return response


def test_data_view_definitions_match_all_contract_aliases_and_time_fields() -> None:
    definitions = data_view_definitions(load_contract())
    assert len(definitions) == 7
    assert {view["id"] for view in definitions} == set(load_contract()["data_views"])
    assert all(view["title"].startswith("analytics_") for view in definitions)
    assert all(view["allowNoIndex"] is True for view in definitions)
    assert "timeFieldName" not in next(
        view for view in definitions if view["id"] == "dv_d1_timeseries"
    )
    assert next(
        view for view in definitions if view["id"] == "dv_d2_movie_timeseries"
    )["timeFieldName"] == "period_start"
    assert next(
        view for view in definitions if view["id"] == "dv_d4_growth_history"
    )["timeFieldName"] == "window_start"


def test_existing_data_view_is_not_silently_overwritten() -> None:
    definition = data_view_definitions(load_contract())[0]
    client = FakeKibanaClient(
        [
            FakeResponse(
                200,
                {
                    "data_view": {
                        **definition,
                        "title": "analytics_timeseries_dev",
                    }
                },
            )
        ]
    )
    with pytest.raises(RuntimeError, match="lệch runtime contract"):
        _ensure_data_view(client, "integration", definition)  # type: ignore[arg-type]
    assert client.posts == []


def test_existing_space_description_is_not_silently_ignored() -> None:
    client = FakeKibanaClient(
        [
            FakeResponse(
                200,
                {
                    "id": "integration",
                    "name": "Movie Trend Integration",
                    "description": "stale description",
                },
            )
        ]
    )
    with pytest.raises(RuntimeError, match="không khớp cấu hình"):
        _ensure_space(  # type: ignore[arg-type]
            client,
            "integration",
            SPACE_DEFINITIONS["integration"],
        )


def test_bootstrap_creates_two_spaces_and_seven_integration_views() -> None:
    contract = load_contract()
    client = FakeKibanaClient(
        [
            FakeResponse(
                200,
                {
                    "version": {"number": contract["kibana_version"]},
                    "status": {"overall": {"level": "available"}},
                },
            ),
            *[FakeResponse(404) for _ in range(9)],
        ]
    )

    result = bootstrap(client, contract)  # type: ignore[arg-type]

    assert result["spaces"] == ["dev", "integration"]
    assert result["space_for_data_views"] == "integration"
    assert result["kibana_version"] == contract["kibana_version"]
    assert len(result["data_views"]) == 7
    assert len(client.posts) == 9
    assert client.posts[0] == (
        "api/spaces/space",
        {
            "id": "dev",
            "name": "Movie Trend Dev",
            "description": "Development Space for Movie Trend Phase 2.",
        },
    )
    assert all(
        path.startswith("s/integration/api/data_views/data_view")
        for path, _ in client.posts[2:]
    )


def test_bootstrap_rejects_wrong_kibana_version() -> None:
    client = FakeKibanaClient(
        [
            FakeResponse(
                200,
                {
                    "version": {"number": "8.14.0"},
                    "status": {"overall": {"level": "available"}},
                },
            )
        ]
    )
    with pytest.raises(RuntimeError, match="không khớp runtime contract"):
        _validate_kibana(client, load_contract())  # type: ignore[arg-type]


def test_spaces_and_roles_are_scoped_to_integration() -> None:
    assert set(SPACE_DEFINITIONS) == {"dev", "integration"}
    roles = json.loads(Path(ROLES_FILE).read_text(encoding="utf-8"))
    read_role = roles["movie_trend_kibana_analytics_read"]
    assert read_role["indices"][0]["names"] == [
        "analytics_timeseries",
        "analytics_top_movies",
        "analytics_genre_trends",
        "analytics_growth",
        "analytics_growth_current",
        "analytics_tmdb_signal",
    ]
    assert read_role["indices"][0]["privileges"] == ["read", "view_index_metadata"]
    assert read_role["applications"][0]["privileges"] == ["read"]
    assert read_role["applications"][0]["resources"] == ["space:integration"]
    integrator = roles["movie_trend_kibana_integrator"]
    assert integrator["indices"] == []
    assert integrator["applications"][0]["privileges"] == [
        "feature_dashboard.all",
        "feature_visualize.all",
        "feature_savedObjectsManagement.all",
    ]
    assert "all" not in integrator["applications"][0]["privileges"]
    assert integrator["applications"][0]["resources"] == ["space:integration"]


def test_invalid_view_alias_is_rejected() -> None:
    contract = load_contract()
    contract["data_views"]["dv_d5_tmdb_signal"]["alias"] = "analytics_tmdb_signal-v1"
    with pytest.raises(ValueError, match="không trỏ analytics alias"):
        data_view_definitions(contract)


def test_kibana_manifest_matches_runtime_and_security_contract() -> None:
    contract = load_contract()
    config = yaml.safe_load(
        (KIBANA_ROOT / "k8s" / "00-kibana-config.yaml").read_text(encoding="utf-8")
    )
    kibana_config = yaml.safe_load(config["data"]["kibana.yml"])
    assert kibana_config["elasticsearch.hosts"] == ["http://elasticsearch-svc:9200"]
    assert kibana_config["uiSettings"]["overrides"]["dateFormat:tz"] == "UTC"
    assert "dateFormat:tz" not in kibana_config
    assert "namespace" not in config["metadata"]

    documents = list(
        yaml.safe_load_all(
            (KIBANA_ROOT / "k8s" / "01-kibana-deployment.yaml").read_text(
                encoding="utf-8"
            )
        )
    )
    service = next(document for document in documents if document["kind"] == "Service")
    deployment = next(
        document for document in documents if document["kind"] == "Deployment"
    )
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["selector"] == {"app": "kibana", "module": "module-08"}
    assert "namespace" not in deployment["metadata"]
    assert deployment["spec"]["strategy"]["type"] == "Recreate"

    pod_spec = deployment["spec"]["template"]["spec"]
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["terminationGracePeriodSeconds"] > 30
    assert pod_spec["securityContext"]["runAsNonRoot"] is True
    assert pod_spec["securityContext"]["fsGroup"] == 1000
    container = pod_spec["containers"][0]
    assert container["image"] == (
        f"docker.elastic.co/kibana/kibana:{contract['kibana_version']}"
    )
    secret_keys = {
        item["valueFrom"]["secretKeyRef"]["key"]
        for item in container["env"]
        if "valueFrom" in item
    }
    assert secret_keys == {
        "kibana-system-password",
        "security-encryption-key",
        "saved-objects-encryption-key",
        "reporting-encryption-key",
    }


def test_secret_validation_checks_lengths_and_distinct_keys() -> None:
    values = {
        "kibana-system-password": "not-empty",
        **{key: f"{key}-{'x' * 32}" for key in ENCRYPTION_KEYS},
    }
    secret = {
        "metadata": {"name": "kibana-credentials"},
        "data": {
            key: base64.b64encode(value.encode()).decode()
            for key, value in values.items()
        },
    }
    assert validate_secret(secret)["secret"] == "kibana-credentials"

    secret["data"][ENCRYPTION_KEYS[0]] = base64.b64encode(b"too-short").decode()
    with pytest.raises(ValueError, match="ít nhất 32"):
        validate_secret(secret)

    duplicate = base64.b64encode(b"same-key-value-that-is-at-least-32-chars").decode()
    for key in ENCRYPTION_KEYS:
        secret["data"][key] = duplicate
    with pytest.raises(ValueError, match="phải độc lập"):
        validate_secret(secret)
