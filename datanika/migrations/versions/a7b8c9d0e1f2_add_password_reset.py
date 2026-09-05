"""Add password_reset_tokens and users.password_changed_at (core#623)

Revision ID: a7b8c9d0e1f2
Revises: b3f9d17c245e
Create Date: 2026-08-30 18:00:00.000000

Expand-only, so it is safe in a single release under
``docs/specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md``: a nullable ADD COLUMN, a
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

----------------------------------------------------------------------------
Why ``downgrade()`` stashes the column instead of just dropping it (#726)
----------------------------------------------------------------------------

Everything above reasons about the column's *first* consumer: the Settings
card's "has this account ever had a human-chosen password" gate, which reads
only NULL vs non-NULL. #671 gave it a second one, and that one reads the
**value** — ``redeem_refresh_token`` refuses a refresh token whose ``iat``
predates ``password_changed_at``, which is the mechanism that makes changing
your password end your other sessions.

A plain ``drop_column`` plus this file's own backfill is a data-loss cycle for
that second consumer. Roll back one release and forward again — an ordinary bad
deploy — and every row is re-stamped from ``created_at``, moving the revocation
baseline back to signup. A password change performed *because* the account was
believed compromised is silently undone, and every refresh token minted since
the account existed is valid again. It fails **open**.

Nothing that was watching could see it: the column returns with the same name,
type and nullability, the row count is unchanged, and no value is NULL, so the
schema round-trip and every count-based assertion stay green. Only comparing
values catches it — which is the gap #726 was filed for.

So the values are carried across the downgraded window in ``_ROLLBACK_STASH``
and put back by the next ``upgrade()``, NULLs included. The backfill then only
reaches rows the stash does not cover, which is exactly the set that needs it:
a fresh install, and any account the *previous* release created while serving
the rollback.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7b8c9d0e1f2"
down_revision: str | None = "b3f9d17c245e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Carries ``users.password_changed_at`` across a downgrade. Exists only between
# a ``downgrade()`` and the next ``upgrade()``; no released version of the
# application reads it, and the re-upgrade consumes and drops it.
_ROLLBACK_STASH = "password_changed_at_rollback_stash"


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

    connection = op.get_bind()

    # Put back whatever a previous downgrade() set aside, before the backfill
    # runs — see the module docstring. Absent on a first install, which is the
    # common case and where this is a no-op.
    stashed = sa.inspect(connection).has_table(_ROLLBACK_STASH)
    if stashed:
        op.execute(
            f"UPDATE users SET password_changed_at = s.password_changed_at "  # noqa: S608
            f"FROM {_ROLLBACK_STASH} s WHERE s.user_id = users.id"
        )

    # Fail closed — see the module docstring for why there is no WHERE clause
    # beyond the NULL test. Rows the stash covered are excluded rather than
    # merely non-NULL: it also held the genuine NULLs, and re-stamping one of
    # those turns an OAuth-only account into one that "has a password".
    backfill = "UPDATE users SET password_changed_at = created_at WHERE password_changed_at IS NULL"
    if stashed:
        backfill += f" AND id NOT IN (SELECT user_id FROM {_ROLLBACK_STASH})"  # noqa: S608
    op.execute(backfill)

    if stashed:
        # expand-contract: safe in a single release. No deployed version of the
        # application has ever read this table — it is written by downgrade()
        # and consumed here, so the t1 window (old code, new schema) cannot
        # observe it.
        op.drop_table(_ROLLBACK_STASH)

    # Force commit — Alembic env.py with SQLAlchemy 2.0 autobegin
    connection.commit()


def downgrade() -> None:
    op.drop_index("ix_password_reset_tokens_user_id")
    op.drop_index("ix_password_reset_tokens_token_hash")
    op.drop_table("password_reset_tokens")

    # Preserve the revocation baseline across the rollback window — see the
    # module docstring. Every row, NULLs included: NULL is itself a meaningful
    # value in this column. DROP first, because rolling back twice in a day is
    # a bad release day, not an exotic scenario, and a bare CREATE TABLE would
    # raise DuplicateTable and leave the second downgrade half-applied.
    op.execute(f"DROP TABLE IF EXISTS {_ROLLBACK_STASH}")
    op.execute(
        f"CREATE TABLE {_ROLLBACK_STASH} AS "  # noqa: S608
        f"SELECT id AS user_id, password_changed_at FROM users"
    )

    op.drop_column("users", "password_changed_at")
