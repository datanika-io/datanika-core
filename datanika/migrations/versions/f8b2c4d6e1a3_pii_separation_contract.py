"""PII separation, release N+2 (contract): drop the five legacy columns.

``docs/specs/SPEC_PII_SEPARATION.md`` §4 and §8a, tracked as core#939. The
release now in production stopped reading and writing these columns. This
revision is the contract: it drops them, and the models unmap them in the same
release. ``upgrade()`` drops columns only. It does not drop a table.

The email-channel recipient used to sit in ``notification_channels.config``
under the key ``email``, beside the channel secret. Release N copied it to
``notification_channel_pii``. This revision strips that key from rows that
still have it. Rows without the key are left untouched, so a second upgrade
does not rewrite them.

``downgrade()`` re-adds the columns and copies ``email``, ``full_name`` and
``oauth_provider_id`` back from ``user_pii``, and ``invitations.email`` back
from ``invitation_pii``. That is the same re-derivation release N's downgrade
uses. The plaintext invitation token has no sidecar column; ``token_hash`` was
not dropped, and the re-added ``token`` column stays empty.

expand-contract: contract phase. The previous release stopped selecting and
writing these columns outside the mapped attributes and the erasure nulls,
both removed in this release. Dropping them is the step that release schedules
once that code is what production runs.

Revision ID: f8b2c4d6e1a3
Revises: a670c1d2e3f4
Create Date: 2026-11-21
"""

import sqlalchemy as sa
from alembic import op

revision: str = "f8b2c4d6e1a3"
down_revision: str | None = "a670c1d2e3f4"
branch_labels: str | None = None
depends_on: str | None = None


def _drop_unique_on(table: str, column: str) -> None:
    """Drop a UNIQUE constraint that depends on ``column``, if one exists.

    ``DROP COLUMN`` refuses while that constraint remains. The name is read
    from the catalog so a default ``{table}_{column}_key`` name is not assumed.
    """
    rows = op.get_bind().execute(
        sa.text(
            """
            SELECT con.conname
            FROM pg_constraint AS con
            JOIN pg_class AS rel ON rel.oid = con.conrelid
            JOIN pg_attribute AS att
              ON att.attrelid = rel.oid
             AND att.attnum = ANY (con.conkey)
            WHERE rel.relname = :table
              AND att.attname = :column
              AND con.contype = 'u'
            """
        ),
        {"table": table, "column": column},
    )
    for (name,) in rows:
        op.drop_constraint(name, table, type_="unique")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    op.execute(
        """
        UPDATE notification_channels
        SET config = (config::jsonb - 'email')::json
        WHERE config::jsonb ? 'email'
        """
    )
    _drop_unique_on("users", "email")
    _drop_unique_on("invitations", "token")
    op.drop_column("users", "email")
    op.drop_column("users", "full_name")
    op.drop_column("users", "oauth_provider_id")
    op.drop_column("invitations", "email")
    op.drop_column("invitations", "token")


def downgrade() -> None:
    op.add_column("users", sa.Column("email", sa.String(length=320), nullable=True))
    op.add_column("users", sa.Column("full_name", sa.String(length=255), nullable=True))
    op.add_column("users", sa.Column("oauth_provider_id", sa.String(length=255), nullable=True))
    op.add_column("invitations", sa.Column("email", sa.String(length=320), nullable=True))
    op.add_column("invitations", sa.Column("token", sa.String(length=500), nullable=True))
    op.execute(
        """
        UPDATE users AS u
        SET email = COALESCE(u.email, p.email),
            full_name = COALESCE(u.full_name, p.full_name),
            oauth_provider_id = COALESCE(u.oauth_provider_id, p.oauth_provider_id)
        FROM user_pii AS p
        WHERE p.user_id = u.id
        """
    )
    op.execute(
        """
        UPDATE invitations AS i
        SET email = COALESCE(i.email, p.email)
        FROM invitation_pii AS p
        WHERE p.invitation_id = i.id
        """
    )
    op.create_unique_constraint("users_email_key", "users", ["email"])
    op.create_unique_constraint("invitations_token_key", "invitations", ["token"])
