"""PII separation, release N+2: drop the five legacy columns.

``docs/specs/SPEC_PII_SEPARATION.md`` Step C, tracked as core#939.
Revision ``f8b2c4d6e1a3`` unmapped ``users.email``, ``users.full_name``,
``users.oauth_provider_id``, ``invitations.email`` and ``invitations.token``
and left them in the database. That revision is what production runs, and it
does not read or write them. ``erase_user`` step 1a, which nulled them, is
already gone. This revision drops the columns.

``downgrade()`` puts the four values that still exist back from ``user_pii``
and ``invitation_pii``. The invitation plaintext is only stored as
``token_hash``, so the restored ``invitations.token`` column stays empty.

# expand-contract: contract phase. The deployed revision f8b2c4d6e1a3 unmapped
# these five columns and does not select or write them. ip_address stays on
# audit_logs.

Revision ID: b7c3e9a1d4f2
Revises: f8b2c4d6e1a3
Create Date: 2026-11-24
"""

import sqlalchemy as sa
from alembic import op

revision: str = "b7c3e9a1d4f2"
down_revision: str | None = "f8b2c4d6e1a3"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    op.drop_column("users", "email")
    op.drop_column("users", "full_name")
    op.drop_column("users", "oauth_provider_id")
    op.drop_column("invitations", "email")
    op.drop_column("invitations", "token")


def downgrade() -> None:
    op.add_column("users", sa.Column("email", sa.String(320), nullable=True))
    op.add_column("users", sa.Column("full_name", sa.String(255), nullable=True))
    op.add_column("users", sa.Column("oauth_provider_id", sa.String(255), nullable=True))
    op.add_column("invitations", sa.Column("email", sa.String(320), nullable=True))
    op.add_column("invitations", sa.Column("token", sa.String(500), nullable=True))
    op.execute(
        """
        UPDATE users AS u
        SET email = p.email,
            full_name = p.full_name,
            oauth_provider_id = p.oauth_provider_id
        FROM user_pii AS p
        WHERE p.user_id = u.id
        """
    )
    op.execute(
        """
        UPDATE invitations AS i
        SET email = p.email
        FROM invitation_pii AS p
        WHERE p.invitation_id = i.id
        """
    )
