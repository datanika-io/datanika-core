import contextlib
import re
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from datanika.migrations.helpers import (
    get_tenant_schemas,
    is_public_table,
    is_tenant_table,
)
from datanika.models.base import Base

with contextlib.suppress(ImportError):
    import datanika_cloud.billing.models  # noqa: F401

_TENANT_SCHEMA_RE = re.compile(r"^tenant_\d+$")

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


def _include_tenant(object, name, type_, reflected, compare_to):
    if type_ == "table":
        return is_tenant_table(name)
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


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        # Phase 1: public schema
        #
        # 🚨 This statement is why `op.get_context().autocommit_block()` raises a bare,
        # message-less AssertionError in EVERY migration in this repo (core#933). It
        # autobegins a SQLAlchemy transaction that alembic did not begin, and
        # `autocommit_block()` refuses exactly that state. The traceback names alembic,
        # not us, so this line is the only place a reader arrives at unaided.
        #
        # 🔴 CORRECTED 2026-09-07. This comment used to say the cause was that the SET runs
        # *before* `context.begin_transaction()`, "which never assigns `self._transaction`
        # — the attribute `autocommit_block()` asserts on". Measured against alembic
        # 1.18.4: `_transaction` is None in the WORKING case too, so it is not the
        # discriminator. The real one is `_in_connection_transaction()`, checked on the
        # line above that assertion.
        #
        # 🚨 The difference is not pedantic — it kills core#933's option 1. Moving this
        # statement INSIDE `context.begin_transaction()` autobegins just the same and
        # changes nothing; the property is "no statement may touch this connection at
        # all". Only a search path set without executing SQL (connect args / URL) can
        # satisfy it. Measured in
        # `tests/test_migrations/test_autocommit_block_availability.py`.
        #
        # Consequence: no `CREATE INDEX CONCURRENTLY`, no `ALTER TYPE ... ADD VALUE`, and
        # no commit between backfill batches. `docs/specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md`
        # states it where an author looks; `tests/test_migrations/
        # test_autocommit_block_availability.py` reproduces it with a control and goes RED
        # when it is fixed, which is when both notes must come out.
        #
        # ⚠️ The tenant loop below re-`SET`s per schema and has the same problem, so a fix
        # has to cover both phases — moving only this one leaves the loop broken while the
        # docs say it works.
        connection.execute(text("SET search_path TO public"))
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=_include_public,
            version_table_schema="public",
        )
        with context.begin_transaction():
            context.run_migrations()
        # SQLAlchemy 2.0 requires explicit commit for DDL to persist
        connection.commit()

        # Phase 2: each tenant schema
        tenant_schemas = get_tenant_schemas(connection)
        for schema in tenant_schemas:
            if not _TENANT_SCHEMA_RE.match(schema):
                continue
            connection.execute(text(f'SET search_path TO "{schema}", public'))
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                include_object=_include_tenant,
                version_table_schema=schema,
            )
            with context.begin_transaction():
                context.run_migrations()
            connection.commit()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
