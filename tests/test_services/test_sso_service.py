"""TDD tests for SSOService — per-org SAML/OIDC configuration."""

import uuid

import pytest
from cryptography.fernet import Fernet

from datanika.models.sso_config import SSOProtocol
from datanika.models.user import Organization
from datanika.services.encryption import EncryptionService
from datanika.services.sso_service import SSOService, SSOServiceError


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def svc(encryption):
    return SSOService(encryption)


@pytest.fixture
def org(db_session):
    o = Organization(name="SSO Corp", slug=f"sso-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def other_org(db_session):
    o = Organization(name="Other Corp", slug=f"other-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


class TestCreateSSOConfig:
    def test_create_oidc_config(self, db_session, svc, org):
        sso = svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "Azure AD",
            {"issuer_url": "https://login.microsoftonline.com/tenant/v2.0",
             "client_id": "abc123", "client_secret": "secret456"},
        )
        assert sso.id is not None
        assert sso.protocol == SSOProtocol.OIDC
        assert sso.display_name == "Azure AD"
        assert sso.oidc_issuer_url == "https://login.microsoftonline.com/tenant/v2.0"
        assert sso.oidc_client_id == "abc123"
        assert sso.oidc_client_secret_encrypted != ""  # encrypted, not plaintext
        assert sso.is_active is True

    def test_create_saml_config(self, db_session, svc, org):
        sso = svc.create_sso_config(
            db_session, org.id, SSOProtocol.SAML, "Okta",
            {"idp_sso_url": "https://okta.com/sso", "idp_entity_id": "okta-entity",
             "idp_cert": "MIIC...", "sp_entity_id": "datanika-prod"},
        )
        assert sso.protocol == SSOProtocol.SAML
        assert sso.saml_idp_sso_url == "https://okta.com/sso"
        assert sso.saml_sp_entity_id == "datanika-prod"

    def test_one_config_per_org(self, db_session, svc, org):
        svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "First",
            {"issuer_url": "https://first.com", "client_id": "a"},
        )
        with pytest.raises(SSOServiceError, match="already has"):
            svc.create_sso_config(
                db_session, org.id, SSOProtocol.SAML, "Second",
                {"idp_sso_url": "https://second.com"},
            )


class TestGetSSOConfig:
    def test_get_existing(self, db_session, svc, org):
        svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "Test",
            {"issuer_url": "https://test.com", "client_id": "x"},
        )
        result = svc.get_sso_config(db_session, org.id)
        assert result is not None
        assert result.display_name == "Test"

    def test_get_nonexistent_returns_none(self, db_session, svc, org):
        result = svc.get_sso_config(db_session, org.id)
        assert result is None

    def test_get_by_org_slug(self, db_session, svc, org):
        svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "Slug Test",
            {"issuer_url": "https://slug.com", "client_id": "y"},
        )
        result = svc.get_sso_config_by_org_slug(db_session, org.slug)
        assert result is not None
        assert result.display_name == "Slug Test"

    def test_get_by_invalid_slug_returns_none(self, db_session, svc):
        result = svc.get_sso_config_by_org_slug(db_session, "nonexistent-slug")
        assert result is None

    def test_different_orgs_isolated(self, db_session, svc, org, other_org):
        svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "Org A SSO",
            {"issuer_url": "https://a.com", "client_id": "a"},
        )
        result = svc.get_sso_config(db_session, other_org.id)
        assert result is None  # Other org has no config


class TestUpdateSSOConfig:
    def test_update_display_name(self, db_session, svc, org):
        svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "Old Name",
            {"issuer_url": "https://old.com", "client_id": "x"},
        )
        updated = svc.update_sso_config(db_session, org.id, {"display_name": "New Name"})
        assert updated.display_name == "New Name"

    def test_update_oidc_fields(self, db_session, svc, org):
        svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "Test",
            {"issuer_url": "https://old.com", "client_id": "old"},
        )
        updated = svc.update_sso_config(
            db_session, org.id,
            {"issuer_url": "https://new.com", "client_id": "new"},
        )
        assert updated.oidc_issuer_url == "https://new.com"
        assert updated.oidc_client_id == "new"

    def test_update_nonexistent_returns_none(self, db_session, svc, org):
        result = svc.update_sso_config(db_session, org.id, {"display_name": "X"})
        assert result is None


class TestDeleteSSOConfig:
    def test_soft_deletes(self, db_session, svc, org):
        svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "To Delete",
            {"issuer_url": "https://del.com", "client_id": "d"},
        )
        result = svc.delete_sso_config(db_session, org.id)
        assert result is True
        # Should not be found anymore
        assert svc.get_sso_config(db_session, org.id) is None

    def test_delete_nonexistent_returns_false(self, db_session, svc, org):
        result = svc.delete_sso_config(db_session, org.id)
        assert result is False


class TestOIDCClientSecret:
    def test_secret_encrypted_and_decryptable(self, db_session, svc, org):
        sso = svc.create_sso_config(
            db_session, org.id, SSOProtocol.OIDC, "Secret Test",
            {"issuer_url": "https://sec.com", "client_id": "c",
             "client_secret": "my-super-secret"},
        )
        # Stored value is encrypted
        assert "my-super-secret" not in (sso.oidc_client_secret_encrypted or "")

        # Can decrypt
        secret = svc.get_oidc_client_secret(sso)
        assert secret == "my-super-secret"
