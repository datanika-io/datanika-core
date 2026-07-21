"""OAuth 2.1 authorization-server models for the hosted MCP endpoint (core#393).

Datanika is the **Authorization Server** for its own ``/mcp`` Resource Server
(SPEC_REMOTE_MCP §5.3). Three tables, deliberately small — this AS serves one
resource and public clients only, it is not a general-purpose IdP (§3).

The load-bearing choice is in :class:`OAuthGrant`: consent mints **one
org-scoped API key per grant**, so revocation is the existing API-keys UI and
the MCP server stays a pure credential forwarder. The raw key is stored
**Fernet-encrypted** (the same treatment connection credentials get) because
the tool path has to replay it on every call — unlike ``api_keys.key_hash``,
which only ever needs to verify. Access/refresh tokens are separate opaque
strings kept as SHA-256 hashes, so a database read yields nothing replayable
except the encrypted key, which needs ``CREDENTIAL_ENCRYPTION_KEY`` to open.
"""

from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin


class OAuthClient(Base, TimestampMixin):
    """An MCP client registered via RFC 7591 Dynamic Client Registration.

    Not org-scoped: "Claude.ai" is one client registration that any number of
    orgs may then grant access to. Public clients only — there is no secret
    column because there is no secret; PKCE is the proof of possession.
    """

    __tablename__ = "oauth_clients"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    client_name: Mapped[str] = mapped_column(String(255), nullable=False)
    redirect_uris: Mapped[list] = mapped_column(JSON, nullable=False)


class OAuthGrant(Base, TimestampMixin, TenantMixin):
    """One user's consent for one client to reach one org's data.

    Holds the minted API key (encrypted) for the life of the grant, plus the
    one-time authorization code. ``code_hash`` is cleared on redemption so a
    replayed code cannot resolve to a grant at all.
    """

    __tablename__ = "oauth_grants"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), nullable=False)
    api_key_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("api_keys.id"), nullable=False)
    encrypted_api_key: Mapped[str] = mapped_column(Text, nullable=False)

    # One-time authorization code (SHA-256 hex), NULLed once redeemed.
    code_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    code_challenge: Mapped[str] = mapped_column(String(128), nullable=False)
    redirect_uri: Mapped[str] = mapped_column(String(2000), nullable=False)
    scope: Mapped[str] = mapped_column(String(500), nullable=False)
    # RFC 8707 — the resource this grant is for. Bound into every token so the
    # RS can refuse anything not minted for /mcp (confused-deputy, SPEC §10).
    resource: Mapped[str] = mapped_column(String(500), nullable=False)
    code_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OAuthToken(Base, TimestampMixin, TenantMixin):
    """An issued access token (+ its refresh token), both hashed at rest."""

    __tablename__ = "oauth_tokens"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    grant_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("oauth_grants.id"), nullable=False, index=True
    )
    access_token_hash: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True, index=True
    )
    refresh_token_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
