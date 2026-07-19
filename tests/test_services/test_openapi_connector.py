"""Wiring tests for the ``openapi`` connection type (P1 of #325).

The spec-parsing logic lives in ``test_openapi_import.py``; this file covers the
end-to-end wiring: enum/sets membership, the ``_build_openapi_source`` runtime
(delegating to rest_api), and IR columns from the stored catalog. All mocked.
"""

from unittest.mock import patch

import pytest

from datanika.models.connection import ConnectionType
from datanika.services.connection_schemas import CONFIG_SCHEMAS, list_connection_types
from datanika.services.connection_service import (
    _NON_DB_TYPES,
    DESTINATION_TYPES,
    SOURCE_TYPES,
)
from datanika.services.dlt_runner import (
    SUPPORTED_OPENAPI_TYPES,
    DltRunnerError,
    DltRunnerService,
)
from datanika.services.ir.builder import IRBuildError, build_ir

# A stored openapi connection config (as produced by openapi_import + save).
OPENAPI_CONFIG = {
    "base_url": "https://api.petstore.io/v1",
    "auth": {"type": "bearer", "token": "tok"},
    "resources": [
        {
            "name": "pets",
            "endpoint": {"path": "pets", "method": "GET"},
            "primary_key": "id",
            "columns": [
                {"name": "id", "type": "bigint", "nullable": False},
                {"name": "name", "type": "text", "nullable": True},
            ],
            "_source": {"operation_id": "listPets", "summary": "List all pets"},
        },
        {
            "name": "orders",
            "endpoint": {"path": "orders", "method": "GET"},
            "columns": [{"name": "id", "type": "integer", "nullable": False}],
            "_source": {},
        },
    ],
}


@pytest.fixture
def svc():
    return DltRunnerService()


@pytest.fixture(autouse=True)
def _stub_egress_guard():
    """No-op the SSRF egress guard (core#338) so ``_build_openapi_source`` →
    ``_rest_api_from_parts`` doesn't resolve the (fake) base_url host via real
    DNS. The guard itself is covered by ``tests/test_security/test_egress_guard.py``.
    """
    with patch("datanika.services.dlt_runner.validate_egress_host"):
        yield


class TestWiring:
    def test_enum(self):
        assert ConnectionType.OPENAPI == "openapi"

    def test_source_only(self):
        assert "openapi" in SOURCE_TYPES
        assert "openapi" not in DESTINATION_TYPES
        assert ConnectionType.OPENAPI in _NON_DB_TYPES
        assert "openapi" in SUPPORTED_OPENAPI_TYPES
        assert "openapi" not in DltRunnerService.SUPPORTED_DESTINATION_TYPES

    def test_config_schema(self):
        assert "openapi" in CONFIG_SCHEMAS
        assert set(CONFIG_SCHEMAS["openapi"]["required"]) == {"base_url", "resources"}

    def test_listed_by_discovery(self):
        assert "openapi" in {i["type"] for i in list_connection_types()}


class TestBuildOpenApiSource:
    @patch("datanika.services.dlt_runner.rest_api_source")
    def test_builds_and_strips_private_keys(self, mock_rest, svc):
        mock_rest.return_value = "oa_src"
        result = svc._build_openapi_source(OPENAPI_CONFIG, {})
        assert result == "oa_src"
        cfg = mock_rest.call_args[0][0]
        assert cfg["client"]["base_url"] == "https://api.petstore.io/v1"
        assert cfg["client"]["auth"] == {"type": "bearer", "token": "tok"}
        assert [r["name"] for r in cfg["resources"]] == ["pets", "orders"]
        for r in cfg["resources"]:
            assert "columns" not in r  # our private keys stripped before dlt
            assert "_source" not in r
            assert "endpoint" in r

    @patch("datanika.services.dlt_runner.rest_api_source")
    def test_resource_names_selection(self, mock_rest, svc):
        svc._build_openapi_source(OPENAPI_CONFIG, {"resource_names": ["orders"]})
        cfg = mock_rest.call_args[0][0]
        assert [r["name"] for r in cfg["resources"]] == ["orders"]

    def test_requires_base_url(self, svc):
        with pytest.raises(DltRunnerError, match="base_url"):
            svc._build_openapi_source({"resources": [{"name": "x", "endpoint": {}}]}, {})

    def test_requires_catalog(self, svc):
        with pytest.raises(DltRunnerError, match="catalog"):
            svc._build_openapi_source({"base_url": "https://x.io"}, {})

    def test_unknown_resource_names_raises(self, svc):
        with pytest.raises(DltRunnerError, match="resource_names"):
            svc._build_openapi_source(OPENAPI_CONFIG, {"resource_names": ["nope"]})

    def test_dispatched_by_build_source(self, svc):
        with patch.object(svc, "_build_openapi_source", return_value="X") as m:
            assert svc.build_source("openapi", OPENAPI_CONFIG, {}) == "X"
            m.assert_called_once()


class TestIROpenApi:
    def test_columns_from_catalog(self):
        ir = build_ir("openapi", OPENAPI_CONFIG, destination_connection_id=2, table="pets")
        assert [c.target_name for c in ir.columns] == ["id", "name"]
        assert ir.columns[0].type == "bigint"
        assert ir.columns[0].nullable is False
        assert ir.primary_key == ["id"]
        assert ir.source.kind == "saas_rest"

    def test_unknown_resource_raises(self):
        with pytest.raises(IRBuildError):
            build_ir("openapi", OPENAPI_CONFIG, destination_connection_id=2, table="nope")
