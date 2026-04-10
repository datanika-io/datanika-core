"""SSOService — manage per-org SAML/OIDC identity provider configurations."""

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.hooks import emit
from datanika.models.sso_config import SSOConfig, SSOProtocol
from datanika.models.user import Organization
from datanika.services.encryption import EncryptionService

logger = logging.getLogger(__name__)


class SSOServiceError(ValueError):
    pass


class SSOService:
    def __init__(self, encryption: EncryptionService):
        self._encryption = encryption

    def create_sso_config(
        self,
        session: Session,
        org_id: int,
        protocol: SSOProtocol,
        display_name: str,
        config: dict,
    ) -> SSOConfig:
        """Create SSO config for an org. Emits sso_config.before_create hook."""
        # Check if org already has an SSO config
        existing = self.get_sso_config(session, org_id)
        if existing:
            raise SSOServiceError("Organization already has an SSO configuration")

        # Cloud plugin can gate this by plan via hook
        emit("sso_config.before_create", session=session, org_id=org_id)

        sso = SSOConfig(
            org_id=org_id,
            protocol=protocol,
            display_name=display_name,
            is_active=True,
        )

        if protocol == SSOProtocol.OIDC:
            sso.oidc_issuer_url = config.get("issuer_url", "")
            sso.oidc_client_id = config.get("client_id", "")
            client_secret = config.get("client_secret", "")
            if client_secret:
                sso.oidc_client_secret_encrypted = self._encryption.encrypt(
                    {"secret": client_secret}
                )
        elif protocol == SSOProtocol.SAML:
            sso.saml_idp_metadata_url = config.get("idp_metadata_url", "")
            sso.saml_idp_entity_id = config.get("idp_entity_id", "")
            sso.saml_idp_sso_url = config.get("idp_sso_url", "")
            sso.saml_idp_cert = config.get("idp_cert", "")
            sso.saml_sp_entity_id = config.get("sp_entity_id", "datanika")

        session.add(sso)
        session.flush()
        return sso

    def get_sso_config(self, session: Session, org_id: int) -> SSOConfig | None:
        """Get active SSO config for an org."""
        return session.execute(
            select(SSOConfig).where(
                SSOConfig.org_id == org_id,
                SSOConfig.deleted_at.is_(None),
            )
        ).scalar_one_or_none()

    def get_sso_config_by_org_slug(self, session: Session, slug: str) -> SSOConfig | None:
        """Find SSO config by org slug (for login page)."""
        org = session.execute(
            select(Organization).where(
                Organization.slug == slug,
                Organization.deleted_at.is_(None),
            )
        ).scalar_one_or_none()
        if org is None:
            return None
        return self.get_sso_config(session, org.id)

    def update_sso_config(self, session: Session, org_id: int, config: dict) -> SSOConfig | None:
        """Update SSO config fields."""
        sso = self.get_sso_config(session, org_id)
        if sso is None:
            return None

        if "display_name" in config:
            sso.display_name = config["display_name"]
        if "is_active" in config:
            sso.is_active = config["is_active"]

        if sso.protocol == SSOProtocol.OIDC:
            if "issuer_url" in config:
                sso.oidc_issuer_url = config["issuer_url"]
            if "client_id" in config:
                sso.oidc_client_id = config["client_id"]
            if "client_secret" in config:
                sso.oidc_client_secret_encrypted = self._encryption.encrypt(
                    {"secret": config["client_secret"]}
                )
        elif sso.protocol == SSOProtocol.SAML:
            if "idp_metadata_url" in config:
                sso.saml_idp_metadata_url = config["idp_metadata_url"]
            if "idp_entity_id" in config:
                sso.saml_idp_entity_id = config["idp_entity_id"]
            if "idp_sso_url" in config:
                sso.saml_idp_sso_url = config["idp_sso_url"]
            if "idp_cert" in config:
                sso.saml_idp_cert = config["idp_cert"]

        session.flush()
        return sso

    def delete_sso_config(self, session: Session, org_id: int) -> bool:
        """Soft-delete SSO config."""
        from datetime import datetime

        sso = self.get_sso_config(session, org_id)
        if sso is None:
            return False
        sso.deleted_at = datetime.utcnow()
        session.flush()
        return True

    def get_oidc_client_secret(self, sso: SSOConfig) -> str:
        """Decrypt OIDC client secret."""
        if not sso.oidc_client_secret_encrypted:
            return ""
        data = self._encryption.decrypt(sso.oidc_client_secret_encrypted)
        return data.get("secret", "")
