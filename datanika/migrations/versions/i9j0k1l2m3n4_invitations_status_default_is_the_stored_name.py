"""``invitations.status`` defaults to the member NAME the ORM loads -- core#1393.

``m2i9j0k1l3f4`` created the column with ``server_default="pending"``. The mapper is
``Enum(InvitationStatus, native_enum=False, length=20)`` **without** ``values_callable``, which
stores and loads member **names**, so the only spellings it accepts are ``PENDING``, ``ACCEPTED``,
``EXPIRED`` and ``CANCELLED``. A row that takes the database default is written successfully and
cannot be read back:

    LookupError: 'pending' is not among the defined enum values. Enum name: invitationstatus.

The third instance of core#1391's class, found by QA's class guard
(``tests/test_migrations/test_enum_column_defaults_load.py``). QA's census found **zero**
invitation rows on production and on staging, with the lowercase default live on both, so there is
nothing to repair: only an INSERT that does not name the column meets this default -- a migration,
a backfill, a data repair, a support fix -- because every application write goes through the ORM,
whose Python-side ``default=InvitationStatus.PENDING`` writes the name.

🚨 **DDL ONLY. NO DML**, the same shape as ``h8i9j0k1l2m3``. Existing rows keep exactly what they
hold; a row already carrying the lowercase spelling is unreadable by every deployed version alike,
so leaving it alone makes nothing worse. Pinned by ``test_existing_rows_are_left_alone``.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Changing a default rewrites no row and
alters no type. At ``t1`` the previously deployed code never consults this default -- its ORM names
the column on every INSERT -- so it meets nothing it did not meet before.

The model gains ``server_default=InvitationStatus.PENDING.name`` in the same commit;
``test_the_column_default_is_the_models_server_default`` fails if the two ever disagree.

Revision ID: i9j0k1l2m3n4
Revises: h8i9j0k1l2m3
Create Date: 2026-09-17 09:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "i9j0k1l2m3n4"
down_revision: str | None = "h8i9j0k1l2m3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: What ``m2i9j0k1l3f4`` declared, restored on the way down so the downgrade cannot drift from it.
_PREVIOUS_DEFAULT = "pending"

#: ``InvitationStatus.PENDING.name`` -- the spelling the mapper stores and loads. A literal: a
#: migration must not import the models it outlives.
_NEW_DEFAULT = "PENDING"


def upgrade() -> None:
    op.alter_column("invitations", "status", server_default=_NEW_DEFAULT)


def downgrade() -> None:
    # No row rewrite in either direction. Restoring a default governs future INSERTs only.
    op.alter_column("invitations", "status", server_default=_PREVIOUS_DEFAULT)
