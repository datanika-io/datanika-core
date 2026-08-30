"""Add password_reset_tokens and users.password_changed_at (core#623)

Revision ID: a7b8c9d0e1f2
Revises: b3f9d17c245e
Create Date: 2026-08-30 18:00:00.000000

Expand-only, so it is safe in a single release under
``plans/infra/SPEC_EXPAND_CONTRACT_MIGRATIONS.md``: a nullable ADD COLUMN, a
CREATE TABLE, and an UPDATE. The container that is still serving during the
blue/green window neither reads the column nor knows the table exists.

----------------------------------------------------------------------------
The backfill's WHERE clause, which is the only judgement call in this file
----------------------------------------------------------------------------

``password_changed_at IS NULL`` means "this account has never had a
human-chosen password", and the Settings card uses it to decide whether to
demand the current password before changing it. So the backfill decides, for
every row that already exists, which side of that gate it lands on.

The obvious clause is ``WHERE oauth_provider IS NULL``. It is wrong, and it is
wrong in the dangerous direction. ``find_or_create_oauth_user`` **backfills**
``oauth_provider`` onto a pre-existing *password* account the first time its
owner signs in with Google or GitHub. Those rows have a real password and a
non-NULL ``oauth_provider``, so that clause would skip them, leave
``password_changed_at`` NULL, and hand them the "Set a password" variant — which
takes no current password. Anyone holding a hijacked live session could then
change the password without knowing the old one.

Nothing in the schema distinguishes an OAuth-*created* row from a password row
that later linked a provider; both are bcrypt hashes and both carry a provider.
So the choice is which way to be wrong:

  * ``WHERE oauth_provider IS NULL``  → fails **open**: real password accounts
    silently lose their re-verification step.
  * no WHERE at all                   → fails **closed**: OAuth-only accounts
    are told to use "Current password", cannot produce one, and are routed to
    the email reset flow — which sets a real password and corrects the row
    permanently. An inconvenience with a working remedy.

This migration takes the second. Going forward the distinction is exact:
``register_user`` stamps the column, ``find_or_create_oauth_user`` leaves it
NULL on creation and never clears it when linking.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7b8c9d0e1f2"
down_revision: str | None = "b3f9d17c245e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("password_changed_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "password_reset_tokens",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_password_reset_tokens_token_hash",
        "password_reset_tokens",
        ["token_hash"],
        unique=True,
    )
    op.create_index("ix_password_reset_tokens_user_id", "password_reset_tokens", ["user_id"])

    # Fail closed — see the module docstring for why there is no WHERE clause.
    op.execute(
        "UPDATE users SET password_changed_at = created_at WHERE password_changed_at IS NULL"
    )

    # Force commit — Alembic env.py with SQLAlchemy 2.0 autobegin
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_index("ix_password_reset_tokens_user_id")
    op.drop_index("ix_password_reset_tokens_token_hash")
    op.drop_table("password_reset_tokens")
    op.drop_column("users", "password_changed_at")
