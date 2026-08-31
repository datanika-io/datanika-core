"""TDD tests for SSO routes — OIDC and SAML login flows."""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography.fernet import Fernet
from starlette.testclient import TestClient

from datanika.models.sso_config import SSOConfig, SSOProtocol
from datanika.models.user import Organization
from datanika.services.encryption import EncryptionService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def org(db_session):
    o = Organization(name="SSO Corp", slug=f"sso-test-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def oidc_config(db_session, org, encryption):
    sso = SSOConfig(
        org_id=org.id,
        protocol=SSOProtocol.OIDC,
        display_name="Test OIDC",
        oidc_issuer_url="https://idp.example.com",
        oidc_client_id="test-client-id",
        oidc_client_secret_encrypted=encryption.encrypt({"secret": "test-secret"}),
        is_active=True,
    )
    db_session.add(sso)
    db_session.flush()
    return sso


@pytest.fixture
def saml_config(db_session, org, encryption):
    sso = SSOConfig(
        org_id=org.id,
        protocol=SSOProtocol.SAML,
        display_name="Test SAML",
        saml_idp_sso_url="https://idp.example.com/saml/sso",
        saml_idp_entity_id="https://idp.example.com",
        saml_idp_cert="MIIC...",
        saml_sp_entity_id="datanika-test",
        is_active=True,
    )
    db_session.add(sso)
    db_session.flush()
    return sso


class TestSSOLogin:
    @patch("datanika.services.sso_routes._get_session")
    @patch("datanika.services.sso_routes._sso_service")
    def test_invalid_org_slug_redirects_to_error(self, mock_svc, mock_session):
        from starlette.applications import Starlette

        from datanika.services.sso_routes import sso_routes

        mock_svc.return_value.get_sso_config_by_org_slug.return_value = None
        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        app = Starlette(routes=sso_routes)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/auth/sso/login/nonexistent-org", follow_redirects=False)
        assert resp.status_code == 302
        assert "error=" in resp.headers["location"]

    @patch("datanika.services.sso_routes._get_session")
    @patch("datanika.services.sso_routes._sso_service")
    def test_inactive_config_blocked(self, mock_svc, mock_session):
        from starlette.applications import Starlette

        from datanika.services.sso_routes import sso_routes

        mock_sso = MagicMock()
        mock_sso.is_active = False
        mock_svc.return_value.get_sso_config_by_org_slug.return_value = mock_sso
        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        app = Starlette(routes=sso_routes)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/auth/sso/login/test-org", follow_redirects=False)
        assert resp.status_code == 302
        assert "auth_error=sso_not_configured" in resp.headers["location"]


class TestOIDCFlow:
    @patch("datanika.services.sso_routes._get_session")
    @patch("datanika.services.sso_routes._sso_service")
    @patch("httpx.AsyncClient")
    def test_oidc_redirects_to_idp(self, mock_httpx, mock_svc, mock_session):
        from starlette.applications import Starlette

        from datanika.services.sso_routes import sso_routes

        # Mock SSO config
        mock_sso = MagicMock()
        mock_sso.is_active = True
        mock_sso.protocol = SSOProtocol.OIDC
        mock_sso.oidc_issuer_url = "https://idp.example.com"
        mock_sso.oidc_client_id = "client-id"
        mock_svc.return_value.get_sso_config_by_org_slug.return_value = mock_sso
        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        # Mock OIDC discovery
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_httpx.return_value = mock_client

        app = Starlette(routes=sso_routes)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/auth/sso/login/test-org", follow_redirects=False)
        assert resp.status_code == 302
        location = resp.headers["location"]
        assert "idp.example.com/authorize" in location
        assert "client_id=client-id" in location
        assert "sso_state" in resp.cookies


class TestSAMLFlow:
    @patch("datanika.services.sso_routes._get_session")
    @patch("datanika.services.sso_routes._sso_service")
    def test_saml_redirects_to_idp(self, mock_svc, mock_session):
        from starlette.applications import Starlette

        from datanika.services.sso_routes import sso_routes

        mock_sso = MagicMock()
        mock_sso.is_active = True
        mock_sso.protocol = SSOProtocol.SAML
        mock_sso.saml_idp_sso_url = "https://idp.example.com/saml/sso"
        mock_sso.saml_sp_entity_id = "datanika"
        mock_svc.return_value.get_sso_config_by_org_slug.return_value = mock_sso
        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        app = Starlette(routes=sso_routes)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/auth/sso/login/test-org", follow_redirects=False)
        assert resp.status_code == 302
        location = resp.headers["location"]
        assert "idp.example.com/saml/sso" in location
        assert "SAMLRequest=" in location


class TestSSOMetadata:
    def test_metadata_returns_xml(self):
        from starlette.applications import Starlette

        from datanika.services.sso_routes import sso_routes

        app = Starlette(routes=sso_routes)
        client = TestClient(app)
        resp = client.get("/api/auth/sso/metadata/test-org")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers["content-type"]
        assert "EntityDescriptor" in resp.text
        assert "AssertionConsumerService" in resp.text
