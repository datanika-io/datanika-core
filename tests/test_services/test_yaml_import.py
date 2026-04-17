"""E8 — YAML pipeline import (POST /api/v1/pipelines/yaml).

Thin format-layer over the JSON bulk_import path. Tests cover:
- Parse helper (_yaml_import.parse_yaml_import) — parse errors, wrong
  top-level type, empty body, UTF-8 decode
- Endpoint — happy path, validation errors surface as JSON, same shape
  as /api/v1/import
- Semantic parity — the same YAML payload YAML-serialized from a JSON
  payload produces the same response
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import yaml
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult

# ---------------------------------------------------------------------------
# Unit tests — parse_yaml_import helper
# ---------------------------------------------------------------------------


class TestParseYamlImport:
    def test_valid_yaml_returns_dict(self):
        from datanika.services._yaml_import import parse_yaml_import

        body = "version: 2\nconnections: []\n"
        result = parse_yaml_import(body)
        assert result == {"version": 2, "connections": []}

    def test_accepts_bytes(self):
        from datanika.services._yaml_import import parse_yaml_import

        body = b"version: 2\n"
        result = parse_yaml_import(body)
        assert result == {"version": 2}

    def test_parse_error_raises(self):
        from datanika.services._yaml_import import YamlImportError, parse_yaml_import

        # Unclosed flow sequence
        body = "version: 2\nconnections: [\n"
        with pytest.raises(YamlImportError, match="parse error"):
            parse_yaml_import(body)

    def test_empty_body_raises(self):
        from datanika.services._yaml_import import YamlImportError, parse_yaml_import

        with pytest.raises(YamlImportError, match="empty"):
            parse_yaml_import("")

    def test_top_level_list_raises(self):
        from datanika.services._yaml_import import YamlImportError, parse_yaml_import

        body = "- foo\n- bar\n"
        with pytest.raises(YamlImportError, match="must be a mapping"):
            parse_yaml_import(body)

    def test_top_level_scalar_raises(self):
        from datanika.services._yaml_import import YamlImportError, parse_yaml_import

        with pytest.raises(YamlImportError, match="must be a mapping"):
            parse_yaml_import("just-a-string\n")

    def test_invalid_utf8_raises(self):
        from datanika.services._yaml_import import YamlImportError, parse_yaml_import

        with pytest.raises(YamlImportError, match="UTF-8"):
            parse_yaml_import(b"\xff\xfe\xff")

    def test_nested_structures_preserved(self):
        from datanika.services._yaml_import import parse_yaml_import

        body = """
version: 2
uploads:
  - name: orders
    dlt_config:
      mode: single_table
      table: orders
      filters:
        - op: gt
          column: created_at
          value: "2024-01-01"
      incremental:
        cursor_path: updated_at
        initial_value: "2024-01-01"
"""
        result = parse_yaml_import(body)
        assert result["version"] == 2
        upload = result["uploads"][0]
        assert upload["dlt_config"]["mode"] == "single_table"
        assert upload["dlt_config"]["filters"][0]["op"] == "gt"
        assert upload["dlt_config"]["incremental"]["cursor_path"] == "updated_at"


# ---------------------------------------------------------------------------
# Endpoint tests — POST /api/v1/pipelines/yaml
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.user_id = 1
    key.name = "Test Key"
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def app():
    return Starlette(routes=api_v1_routes)


@pytest.fixture
def client(app):
    return TestClient(app)


def _auth_headers_yaml():
    return {
        "Authorization": "Bearer etf_testkey",
        "Content-Type": "application/yaml",
    }


def _patch_auth(fake_api_key, rate_limit_ok):
    """Same auth patch used in test_import_endpoint.py."""
    import contextlib

    from cryptography.fernet import Fernet
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from datanika.models.base import Base
    from datanika.services.connection_service import ConnectionService
    from datanika.services.encryption import EncryptionService
    from datanika.services.pipeline_service import PipelineService
    from datanika.services.schedule_service import ScheduleService
    from datanika.services.transformation_service import TransformationService
    from datanika.services.upload_service import UploadService

    @contextlib.contextmanager
    def _ctx():
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=__import__("sqlalchemy.pool", fromlist=["StaticPool"]).StaticPool,
        )
        Base.metadata.create_all(engine)
        session = SASession(engine)

        enc = EncryptionService(Fernet.generate_key().decode())
        conn_svc = ConnectionService(enc)
        upload_svc = UploadService(conn_svc)
        pipeline_svc = PipelineService()
        transform_svc = TransformationService()
        schedule_svc = ScheduleService(upload_svc, transform_svc, pipeline_service=pipeline_svc)

        @contextlib.contextmanager
        def fake_session():
            yield session

        import datanika.services.api_v1_routes as routes_mod

        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session", fake_session),
            patch.object(routes_mod, "_get_conn_svc", return_value=conn_svc),
            patch.object(routes_mod, "_get_upload_svc", return_value=upload_svc),
            patch.object(routes_mod, "_get_schedule_svc", return_value=schedule_svc),
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_ok

            yield session

        session.close()
        engine.dispose()

    return _ctx()


_PAYLOAD = {
    "version": 2,
    "connections": [
        {
            "name": "Src PG",
            "connection_type": "postgres",
            "config": {
                "host": "localhost",
                "port": 5432,
                "user": "u",
                "password": "p",
                "database": "db",
            },
        },
        {
            "name": "Src MySQL",
            "connection_type": "mysql",
            "config": {
                "host": "mysql.local",
                "port": 3306,
                "user": "u",
                "password": "p",
                "database": "db",
            },
        },
    ],
    "uploads": [
        {
            "name": "Orders sync",
            "source_connection_name": "Src MySQL",
            "destination_connection_name": "Src PG",
            "dlt_config": {
                "mode": "single_table",
                "table": "orders",
                "backend": "pyarrow",
            },
        }
    ],
}


class TestYamlImportEndpoint:
    def test_happy_path_creates_resources(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            body = yaml.safe_dump(_PAYLOAD)
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content=body,
                headers=_auth_headers_yaml(),
            )
            assert resp.status_code == 201, resp.json()
            data = resp.json()
            assert "created" in data
            assert len(data["created"]["connections"]) == 2
            assert len(data["created"]["uploads"]) == 1

    def test_yaml_parse_error_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content="version: 2\nconnections: [\n",  # unclosed
                headers=_auth_headers_yaml(),
            )
            assert resp.status_code == 400
            assert "YAML parse error" in resp.json()["error"]["message"]

    def test_missing_version_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            body = yaml.safe_dump({"connections": []})
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content=body,
                headers=_auth_headers_yaml(),
            )
            assert resp.status_code == 400
            assert "version 2" in resp.json()["error"]["message"]

    def test_wrong_version_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            body = yaml.safe_dump({"version": 1, "connections": []})
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content=body,
                headers=_auth_headers_yaml(),
            )
            assert resp.status_code == 400

    def test_validation_errors_surface_as_json(self, client, fake_api_key, rate_limit_ok):
        """Schema validation errors surface the same way as JSON import."""
        bad_payload = {
            "version": 2,
            "connections": [
                {"name": "bad", "connection_type": "not-a-real-type", "config": {}},
            ],
        }
        with _patch_auth(fake_api_key, rate_limit_ok):
            body = yaml.safe_dump(bad_payload)
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content=body,
                headers=_auth_headers_yaml(),
            )
            assert resp.status_code == 400
            assert "errors" in resp.json()

    def test_top_level_list_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content="- item1\n- item2\n",
                headers=_auth_headers_yaml(),
            )
            assert resp.status_code == 400
            assert "mapping" in resp.json()["error"]["message"]

    def test_empty_body_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content="",
                headers=_auth_headers_yaml(),
            )
            assert resp.status_code == 400
            assert "empty" in resp.json()["error"]["message"]

    def test_yaml_and_json_produce_identical_result(self, client, fake_api_key, rate_limit_ok):
        """Same payload, JSON vs YAML serialization → identical created shape."""
        with _patch_auth(fake_api_key, rate_limit_ok):
            json_resp = client.post(
                "/api/v1/import",
                json=_PAYLOAD,
                headers={"Authorization": "Bearer etf_testkey"},
            )
            assert json_resp.status_code == 201

        # Fresh session for YAML — avoid name collisions with the JSON run.
        with _patch_auth(fake_api_key, rate_limit_ok):
            yaml_resp = client.post(
                "/api/v1/pipelines/yaml",
                content=yaml.safe_dump(_PAYLOAD),
                headers=_auth_headers_yaml(),
            )
            assert yaml_resp.status_code == 201

        json_created = json_resp.json()["created"]
        yaml_created = yaml_resp.json()["created"]
        # Same counts in each bucket (IDs differ because sessions are different).
        assert len(json_created["connections"]) == len(yaml_created["connections"])
        assert len(json_created["uploads"]) == len(yaml_created["uploads"])
        assert len(json_created["pipelines"]) == len(yaml_created["pipelines"])
        assert len(json_created["transformations"]) == len(yaml_created["transformations"])
