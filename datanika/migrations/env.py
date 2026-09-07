import contextlib
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from datanika.migrations.helpers import is_public_table
from datanika.models.base import Base

with contextlib.suppress(ImportError):
    import datanika_cloud.billing.models  # noqa: F401

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url from app settings (respects DATABASE_URL_SYNC env var)
from datanika.config import settings  # noqa: E402

config.set_main_option("sqlalchemy.url", settings.database_url_sync)

target_metadata = Base.metadata


def _include_public(object, name, type_, reflected, compare_to):
    if type_ == "table":
        return is_public_table(name)
    return True


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _engine_with_search_path(search_path: str):
    """An engine whose connections arrive with ``search_path`` ALREADY set (core#933).

    🔑 **The search path must not be set with a statement.** Any statement executed on
    the connection before ``context.begin_transaction()`` autobegins a SQLAlchemy
    transaction alembic did not begin; ``begin_transaction()`` then returns a do-nothing
    context manager without assigning ``self._transaction``, and ``autocommit_block()``
    asserts on exactly that — a bare, message-less ``AssertionError`` raised from inside
    alembic, on a line copied verbatim out of alembic's own documentation. That is what
    made ``CREATE INDEX CONCURRENTLY``, ``ALTER TYPE ... ADD VALUE`` and commit-between-
    batches unavailable to every migration in this repo.

    ``options=-csearch_path=...`` is delivered in libpq's **startup packet**, so the path
    is in force on the first statement without one having been executed to put it there.

    ⚠️ **Do not "simplify" this back to a ``SET search_path`` inside
    ``context.begin_transaction()``.** That is core#933's option 1 and it is
    backend-dependent: measured on alembic 1.18.4, it WORKS on PostgreSQL/psycopg2 (where
    ``begin_transaction()`` assigns ``_transaction``, so the later statement joins
    alembic's own transaction) and FAILS on SQLite (where it does not). A fix whose
    correctness varies by driver is how this defect regenerates. The property this
    function has instead — *nothing is executed on the connection at all* — holds on both.

    ⚠️ ``connect_args`` is passed unconditionally rather than only for PostgreSQL URLs.
    Nothing runs ``run_migrations_online()`` against another backend (the whole test tree
    reaches alembic through a real Postgres), and a conditional here would be a branch
    that silently skips the fix.
    """
    return engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args={"options": f"-csearch_path={search_path}"},
    )


def run_migrations_online() -> None:
    # Phase 1: public schema.
    #
    # The connection is handed to alembic untouched — see `_engine_with_search_path`.
    # `tests/test_migrations/test_autocommit_block_in_a_real_migration.py` runs the real
    # migration tree with an `autocommit_block()` head appended and goes RED if a
    # statement is reintroduced here.
    with _engine_with_search_path("public").connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=_include_public,
            version_table_schema="public",
            # core#933: each migration gets its own transaction.
            #
            # 🔑 This is NOT tidiness and NOT alembic's generic advice — without it the
            # search-path fix above is INCOMPLETE. **15 migrations in this tree call
            # `connection.commit()` / `op.get_bind().commit()` inside `upgrade()`.**
            # They could do that safely only because alembic owned no transaction:
            # `_transaction` was None, so they were committing the connection's own
            # autobegun one. Once env.py stops executing a statement, alembic DOES own
            # it — and those commits end it out from under alembic. A later migration's
            # `autocommit_block()` then reaches `self._transaction.commit()` on a dead
            # transaction and dies with `InvalidRequestError: This transaction is
            # inactive`. Measured: the assertion is cleared and the block still fails.
            #
            # Per-migration transactions confine each of those commits to its own
            # migration, so a later one starts clean. ⚠️ Consequence worth knowing: a
            # failure part-way through `upgrade head` now leaves earlier migrations
            # APPLIED and recorded in `alembic_version`, instead of rolling the whole
            # chain back. Under expand/contract each migration is independently safe
            # and a re-run resumes from where it stopped, which is why this is the
            # right trade — but it is a change, not a no-op.
            transaction_per_migration=True,
        )
        with context.begin_transaction():
            context.run_migrations()
        # SQLAlchemy 2.0 requires explicit commit for DDL to persist
        connection.commit()

    # 🚨 There is no phase 2, and it is not coming back in this shape (core#1164).
    #
    # A second phase used to walk every `tenant_*` schema and run the migrations again
    # against each one, with its own `alembic_version`. That made `alembic upgrade head`
    # **fail outright whenever any such schema existed** — the whole public chain replayed
    # against the tenant schema, and unqualified DDL resolved through `search_path` back to
    # the tables phase 1 had just migrated. Since migrations run from the container start
    # command, the symptom was a container that could not start, reporting
    # `column "hard_cap_runs" of relation "plans" already exists` — a message that names
    # nothing about tenants.
    #
    # 🔑 **It was removed rather than repaired because it could not do its stated job even
    # in principle**, and that was measured, not assumed:
    #
    #   * `is_tenant_table()` is True for **0 of the 26** tables in `Base.metadata` — every
    #     one is in `PUBLIC_TABLES`, per the standing "all tables in public, isolated by
    #     org_id" decision. So the loop's `include_object` admitted no table at all.
    #     `test_migration_helpers.py::TestIsTenantTable::test_no_model_table_is_tenant`
    #     asserts this from the other side and predates this change.
    #   * `include_object` filters **autogenerate comparison**, never which revisions
    #     `upgrade` executes — so the loop replayed the public chain regardless. The filter
    #     could not have saved it even if it had admitted something.
    #   * `TenantService`, the only code that can create such a schema, is referenced
    #     nowhere outside `tests/test_services/test_tenant.py`.
    #
    # ⚠️ This is NOT a ruling that per-tenant schemas will never return. It is that this
    # loop would not implement them: a real per-tenant feature needs a tenant-scoped
    # revision chain, which this never had. Keeping the loop preserved the landmine, not
    # the capability.
    #
    # Guarded by `tests/test_migrations/test_autocommit_block_in_a_real_migration.py::
    # TestATenantSchemaDoesNotBreakTheDeploy`, whose negative control appends this phase
    # back and asserts the failure returns.


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
