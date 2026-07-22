"""DuckDB loads must reach the Data Catalog (core#494).

After a successful upload into a DuckDB destination, Models / Catalog stayed
empty: *"No models found. Run an upload or transformation to populate the
catalog."* The load itself worked — dlt writes to DuckDB through its own driver
— but the catalog sync goes through **SQLAlchemy**, and the dialect was missing:

    sqlalchemy.exc.NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:duckdb

`duckdb` was installed; `duckdb_engine`, the SQLAlchemy dialect, was not.

**Installing the dialect was necessary and not sufficient**, which these tests
found and the issue's diagnosis did not. With `duckdb_engine` present,
`create_engine` and `get_table_names` both work — but `get_columns` still
raises, because the dialect derives from PostgreSQL's and queries
`pg_collation`, which DuckDB does not have::

    ProgrammingError: Catalog Error: Table with name pg_collation does not exist!

`introspect_tables` wrapped that in a bare `except Exception: continue`, so
every table was dropped silently and the catalog came back **empty even after
the dependency fix**. A test asserting only `import duckdb_engine` would have
gone green over a still-broken feature.

DuckDB is the destination in the zero-credentials onboarding template, and both
`/docs/connectors/duckdb` and `/docs/connectors/csv` tell the user to verify
their first run by browsing Catalog. **For every DuckDB user that instruction
had never once worked.** Data Preview (#260) was unreachable for the same
reason — it needs a catalog entry.

Both failures were swallowed — the per-table one here, and the whole sync in
`upload_tasks` — so nothing outside the worker log knew. Both are covered.
"""

import pytest
from sqlalchemy import create_engine, text

from datanika.services.catalog_service import CatalogService

pytest.importorskip("duckdb", reason="core#494 is specifically about the DuckDB path")


class TestTheDuckDBDialectIsInstalled:
    def test_sqlalchemy_can_build_a_duckdb_engine(self):
        """The exact failure from the production traceback.

        `create_engine` is where it raised — before any query, so no amount of
        retrying or different SQL would have helped.
        """
        engine = create_engine("duckdb:///:memory:")
        engine.dispose()

    def test_duckdb_engine_is_importable(self):
        """Names the missing package, so a failure here reads as its own fix."""
        import duckdb_engine  # noqa: F401


class TestCatalogIntrospectionWorksOnDuckDB:
    def test_introspect_tables_finds_a_real_table(self, tmp_path):
        """Drive the real consumer, not just the import.

        `import duckdb_engine` passing proves the package exists; it does not
        prove `CatalogService` can read a DuckDB file. This is the call that
        raised in production.
        """
        db_path = tmp_path / "warehouse.duckdb"
        engine = create_engine(f"duckdb:///{db_path}")
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA customersdailyload"))
                conn.execute(
                    text(
                        "CREATE TABLE customersdailyload.customers "
                        "(customer_id BIGINT, full_name VARCHAR)"
                    )
                )
                conn.execute(
                    text("INSERT INTO customersdailyload.customers VALUES (1001, 'Ada Lovelace')")
                )
        finally:
            engine.dispose()

        tables = CatalogService.introspect_tables(
            f"duckdb:///{db_path}", schema_name="customersdailyload"
        )

        assert [t["table_name"] for t in tables] == ["customers"]
        assert {c["name"] for c in tables[0]["columns"]} == {"customer_id", "full_name"}

    def test_a_dialect_that_cannot_answer_falls_back_instead_of_vanishing(self, tmp_path):
        """The defect that installing the dependency did *not* fix.

        `duckdb_engine` derives from the PostgreSQL dialect, so `get_columns`
        queries `pg_collation` — which DuckDB does not have. The old bare
        `except Exception: continue` turned that into an **empty catalog**, so
        the dependency fix alone still left Models/Catalog blank.

        Forced here rather than relying on the live dialect: whether upstream
        fixes `get_columns` should not decide whether we notice a regression.
        """
        from unittest.mock import patch

        db_path = tmp_path / "warehouse.duckdb"
        engine = create_engine(f"duckdb:///{db_path}")
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA probe"))
                conn.execute(text("CREATE TABLE probe.customers (customer_id BIGINT)"))
        finally:
            engine.dispose()

        def _boom(*args, **kwargs):
            raise RuntimeError("Catalog Error: Table with name pg_collation does not exist!")

        with patch("sqlalchemy.engine.reflection.Inspector.get_columns", _boom):
            tables = CatalogService.introspect_tables(f"duckdb:///{db_path}", schema_name="probe")

        assert [t["table_name"] for t in tables] == ["customers"]
        assert [c["name"] for c in tables[0]["columns"]] == ["customer_id"]

    def test_dlt_bookkeeping_tables_are_filtered_out(self, tmp_path):
        """The catalog should show the user's tables, not dlt's.

        Production had *only* `_dlt_loads` / `_dlt_pipeline_state` /
        `_dlt_version` in the destination, so this is also the shape a
        zero-row run leaves behind (core#493).
        """
        db_path = tmp_path / "warehouse.duckdb"
        engine = create_engine(f"duckdb:///{db_path}")
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA probe"))
                conn.execute(text("CREATE TABLE probe._dlt_loads (load_id VARCHAR)"))
                conn.execute(text("CREATE TABLE probe.customers (customer_id BIGINT)"))
        finally:
            engine.dispose()

        tables = CatalogService.introspect_tables(f"duckdb:///{db_path}", schema_name="probe")

        assert [t["table_name"] for t in tables] == ["customers"]
