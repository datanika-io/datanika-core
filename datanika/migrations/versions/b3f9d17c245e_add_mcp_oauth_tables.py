"""Remote-MCP P2 — OAuth 2.1 authorization-server tables

Adds ``oauth_clients`` (RFC 7591 dynamic client registrations),
``oauth_grants`` (a user's consent for one client + one org, holding the
minted org-scoped API key encrypted, plus the one-time authorization code)
and ``oauth_tokens`` (issued access/refresh tokens, hashed at rest).

See ``datanika/models/mcp_oauth.py`` and SPEC_REMOTE_MCP §5.3 for why consent
mints a real API key rather than a parallel credential: revocation stays the
existing API-keys UI and the MCP server stays a pure forwarder.

Revision ID: b3f9d17c245e
Revises: d0e5f6g7h8i9
Create Date: 2026-07-21 13:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b3f9d17c245e"
down_revision: str | None = "d0e5f6g7h8i9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Timestamp columns are spelled out per table rather than factored into a
    # helper: tests/test_migrations/test_migration_coverage.py reads these files
    # as text to prove every model column has a migration, and a helper hides
    # them from it.
    op.create_table(
        "oauth_clients",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("client_id", sa.String(length=64), nullable=False),
        sa.Column("client_name", sa.String(length=255), nullable=False),
        sa.Column("redirect_uris", sa.JSON(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_oauth_clients_client_id", "oauth_clients", ["client_id"], unique=True)

    op.create_table(
        "oauth_grants",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("org_id", sa.BigInteger(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("client_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.BigInteger(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("api_key_id", sa.BigInteger(), sa.ForeignKey("api_keys.id"), nullable=False),
        sa.Column("encrypted_api_key", sa.Text(), nullable=False),
        sa.Column("code_hash", sa.String(length=64), nullable=True),
        sa.Column("code_challenge", sa.String(length=128), nullable=False),
        sa.Column("redirect_uri", sa.String(length=2000), nullable=False),
        sa.Column("scope", sa.String(length=500), nullable=False),
        sa.Column("resource", sa.String(length=500), nullable=False),
        sa.Column("code_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Unique so a code can never resolve to two grants; nullable because the
    # hash is cleared on redemption (Postgres allows many NULLs under UNIQUE).
    op.create_index("ix_oauth_grants_code_hash", "oauth_grants", ["code_hash"], unique=True)
    op.create_index("ix_oauth_grants_org_id", "oauth_grants", ["org_id"])
    op.create_index("ix_oauth_grants_client_id", "oauth_grants", ["client_id"])

    op.create_table(
        "oauth_tokens",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("org_id", sa.BigInteger(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("grant_id", sa.BigInteger(), sa.ForeignKey("oauth_grants.id"), nullable=False),
        sa.Column("access_token_hash", sa.String(length=64), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=64), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_oauth_tokens_access_token_hash", "oauth_tokens", ["access_token_hash"], unique=True
    )
    op.create_index(
        "ix_oauth_tokens_refresh_token_hash", "oauth_tokens", ["refresh_token_hash"], unique=True
    )
    op.create_index("ix_oauth_tokens_grant_id", "oauth_tokens", ["grant_id"])
    op.create_index("ix_oauth_tokens_org_id", "oauth_tokens", ["org_id"])

    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_index("ix_oauth_tokens_org_id", table_name="oauth_tokens")
    op.drop_index("ix_oauth_tokens_grant_id", table_name="oauth_tokens")
    op.drop_index("ix_oauth_tokens_refresh_token_hash", table_name="oauth_tokens")
    op.drop_index("ix_oauth_tokens_access_token_hash", table_name="oauth_tokens")
    op.drop_table("oauth_tokens")

    op.drop_index("ix_oauth_grants_client_id", table_name="oauth_grants")
    op.drop_index("ix_oauth_grants_org_id", table_name="oauth_grants")
    op.drop_index("ix_oauth_grants_code_hash", table_name="oauth_grants")
    op.drop_table("oauth_grants")

    op.drop_index("ix_oauth_clients_client_id", table_name="oauth_clients")
    op.drop_table("oauth_clients")

    connection = op.get_bind()
    connection.commit()
