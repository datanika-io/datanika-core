"""SSO configuration model — per-org SAML/OIDC identity provider settings."""

import enum

from sqlalchemy import Boolean, Enum, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin


class SSOProtocol(enum.StrEnum):
    OIDC = "oidc"
    SAML = "saml"


class SSOConfig(Base, TenantMixin, TimestampMixin):
    __tablename__ = "sso_configs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    protocol: Mapped[SSOProtocol] = mapped_column(
        Enum(SSOProtocol, native_enum=False, length=10), nullable=False
    )
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)

    # OIDC fields
    oidc_issuer_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    oidc_client_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    oidc_client_secret_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)

    # SAML fields
    saml_idp_metadata_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    saml_idp_entity_id: Mapped[str | None] = mapped_column(String(500), nullable=True)
    saml_idp_sso_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    saml_idp_cert: Mapped[str | None] = mapped_column(Text, nullable=True)
    saml_sp_entity_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default="datanika"
    )

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
