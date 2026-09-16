"""``uploads.mode`` / ``pipelines.mode`` default to the member NAME the ORM loads — core#1391.

``z6a1b2c3d4e5`` added both columns with ``server_default="etl"``. The mapper is
``Enum(<Mode>, native_enum=False)`` **without** ``values_callable``, which stores and loads member
**names**, so the only spellings it accepts are ``ETL`` and ``ELT``. A row that takes the database
default is therefore written successfully and cannot be read back:

    LookupError: 'etl' is not among the defined enum values. Enum name: uploadmode.
                 Possible values: ETL, ELT

QA reproduced that from the worker on staging, and measured the default as
``'etl'::character varying`` on **both** staging and production — with **zero** rows holding it on
either. The trap is armed and has not fired, for a reason nobody designed: ``z6a1b2c3d4e5`` is
dated 2026-04-16 and production was rebuilt from source on 2026-07-17, so it ran against empty
tables, and every row since was written by the ORM, whose Python-side ``default=<Mode>.ETL``
writes the name. Only an INSERT that does not name the column meets this default — a migration,
a backfill, a data repair, a support fix — and the failure then surfaces later, in a worker, as
an error naming an enum rather than a row, before ``fail_run`` can run.

**The smaller of the two fixes, chosen on QA's census.** Adding ``values_callable`` instead would
change what is *stored* and need an expand/contract migration of every existing row. Changing the
default to the name touches no existing row, and the census says there is nothing to repair.

🚨 **DDL ONLY. NO DML.** Existing rows keep exactly what they hold. A row already carrying a
lowercase spelling is unreadable by every deployed version alike, so leaving it alone makes nothing
worse; repairing one is a separate, deliberate change if a self-hosted instance ever reports it.
Pinned by ``test_existing_rows_are_left_alone`` in
``tests/test_migrations/test_h8i9_mode_server_default.py``.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Changing a default is neither destructive
nor narrowing: it rewrites no row and alters no type. At ``t1`` the previously deployed code never
consults the database default — its ORM names the column on every INSERT — so it meets nothing it
did not meet before, and a Celery worker still on the old model is equally unaffected.

The model's ``server_default=`` moves in the same commit;
``test_the_column_default_is_the_models_server_default`` fails if the two ever disagree, since the
model suite builds its schema with ``create_all`` and never runs alembic.

Revision ID: h8i9j0k1l2m3
Revises: g7h8i9j0k1l2
Create Date: 2026-09-16 21:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "h8i9j0k1l2m3"
down_revision: str | None = "g7h8i9j0k1l2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The two columns ``z6a1b2c3d4e5`` created with the same default. Literals, deliberately: a
#: migration must not import the models it outlives.
_COLUMNS = (("uploads", "mode"), ("pipelines", "mode"))

#: What ``z6a1b2c3d4e5`` declared, restored on the way down so the downgrade cannot drift from it.
_PREVIOUS_DEFAULT = "etl"

#: ``UploadMode.ETL.name`` / ``PipelineMode.ETL.name`` — the spelling the mapper stores and loads.
_NEW_DEFAULT = "ETL"


def upgrade() -> None:
    for table, column in _COLUMNS:
        op.alter_column(table, column, server_default=_NEW_DEFAULT)


def downgrade() -> None:
    # No row rewrite in either direction. Restoring a default governs future INSERTs only.
    for table, column in _COLUMNS:
        op.alter_column(table, column, server_default=_PREVIOUS_DEFAULT)
