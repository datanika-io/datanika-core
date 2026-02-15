from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from etlfabric.migrations.helpers import (
    get_tenant_schemas,
    is_public_table,
    is_tenant_table,
)
from etlfabric.models.base import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url from app settings (respects DATABASE_URL_SYNC env var)
from etlfabric.config import settings  # noqa: E402

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
