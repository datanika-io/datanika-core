"""A DuckDB **source** run loads every row of a real file (core#1431).

DuckDB is offered as a source — it is in ``SOURCE_TYPES`` and in
``DltRunnerService.SUPPORTED_SOURCE_TYPES``, and its connections carry direction ``BOTH`` — and no
test drove one. ``test_source_builders_move_rows.py`` uses DuckDB only as a *destination*.

Measured before the fix, through the real ``run_upload`` on a file holding one table::

    sqlalchemy.exc.ProgrammingError: (_duckdb.CatalogException) Catalog Error:
    Table with name pg_collation does not exist!
    LINE 6: FROM pg_catalog.pg_collation

``duckdb_engine`` derives its dialect from PostgreSQL's, SQLAlchemy's PostgreSQL ``get_columns``
joins ``pg_catalog.pg_collation``, and dlt's ``sql_database`` reflects every table when the source
is built — so the run never reaches a row. The same error was already known one path over: the Data
Catalog falls back to ``information_schema`` for exactly this reason (core#494,
``CatalogService._columns_for``). The source path had no such fallback.

Witnesses are the row count and the ids **at the destination**, and the source file's bytes.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pytest
from cryptography.fernet import Fernet

from datanika.config import settings
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.upload_tasks import run_upload

ROW_IDS = (1, 2, 3, 4, 5)


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


def _duckdb_source(path: Path) -> None:
    """A real DuckDB file holding one table with known ids, plus a second table and a view.

    The second table and the view are there because ``sql_database`` reflects **everything**: a fix
    that made one table readable and left the next one raising would pass a single-table arm.
    """
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE customers (id INTEGER, name VARCHAR, spend DECIMAL(10,2))")
        con.executemany(
            "INSERT INTO customers VALUES (?, ?, ?)",
            [(i, f"c{i}", i * 1.5) for i in ROW_IDS],
        )
        con.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, placed_at TIMESTAMP)")
        con.executemany("INSERT INTO orders VALUES (?, ?, now())", [(i, i) for i in ROW_IDS])
        con.execute("CREATE VIEW recent_orders AS SELECT * FROM orders")
    finally:
        con.close()


def _destination_rows(dest: Path, table: str) -> list[int]:
    if not dest.exists():
        return []
    con = duckdb.connect(str(dest))
    try:
        found = con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            f"WHERE table_name = '{table}'"
        ).fetchall()
        if not found:
            return []
        schema = found[0][0]
        return [
            r[0] for r in con.execute(f'SELECT id FROM "{schema}"."{table}" ORDER BY id').fetchall()
        ]
    finally:
        con.close()


def _run(db_session, encryption, tmp_path, dlt_config) -> dict:
    source = tmp_path / "warehouse.duckdb"
    _duckdb_source(source)
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    dest = tmp_path / "destination.duckdb"

    org = Organization(name="Acme", slug=f"acme-1431-{tmp_path.name[-16:]}")
    db_session.add(org)
    db_session.flush()
    src = Connection(
        org_id=org.id,
        name="duckdb source",
        connection_type=ConnectionType.DUCKDB,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt({"path": str(source)}),
    )
    dst = Connection(
        org_id=org.id,
        name="warehouse",
        connection_type=ConnectionType.DUCKDB,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=encryption.encrypt({"path": str(dest)}),
    )
    db_session.add_all([src, dst])
    db_session.flush()
    upload = Upload(
        org_id=org.id,
        name="from duckdb",
        source_connection_id=src.id,
        destination_connection_id=dst.id,
        dlt_config={"write_disposition": "replace", **dlt_config},
        status=UploadStatus.DRAFT,
    )
    db_session.add(upload)
    db_session.flush()
    run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    run_upload(run.id, org.id, session=db_session, encryption=encryption)
    db_session.refresh(run)
    return {
        "status": run.status,
        "error": run.error_message or "",
        "rows_loaded": run.rows_loaded,
        "customers": _destination_rows(dest, "customers"),
        "orders": _destination_rows(dest, "orders"),
        "source_sha256_before": before,
        "source_sha256_after": hashlib.sha256(source.read_bytes()).hexdigest(),
    }


def test_a_duckdb_source_loads_every_row(db_session, encryption, tmp_path):
    reading = _run(db_session, encryption, tmp_path, {"mode": "full_database"})

    assert reading["status"] == RunStatus.SUCCESS, reading["error"][:400]
    assert reading["customers"] == list(ROW_IDS), reading
    assert reading["orders"] == list(ROW_IDS), reading


def test_a_duckdb_source_run_does_not_modify_the_file(db_session, encryption, tmp_path):
    """core#1401 opens local-file sources read-only; this is the witness for DuckDB."""
    reading = _run(db_session, encryption, tmp_path, {"mode": "full_database"})

    assert reading["status"] == RunStatus.SUCCESS, reading["error"][:400]
    assert reading["source_sha256_after"] == reading["source_sha256_before"]


def test_the_connector_column_picker_reads_a_duckdb_table(tmp_path):
    """The same inherited reflection, on a second path — measured, not assumed.

    ``ConnectionService.list_columns`` calls ``inspect(engine).get_columns`` directly, so a DuckDB
    connection's column picker raised the same ``pg_collation`` error as the run did. It is a
    different entry point with a different consumer, and fixing one would not have fixed the other
    had the replacement been installed at the run's call site instead of where both paths reach it.
    """
    from datanika.services.connection_service import ConnectionService

    source = tmp_path / "warehouse.duckdb"
    _duckdb_source(source)

    tables = ConnectionService.list_tables({"path": str(source)}, ConnectionType.DUCKDB)
    columns = ConnectionService.list_columns(
        {"path": str(source)}, ConnectionType.DUCKDB, "customers"
    )

    assert "customers" in [t["name"] for t in tables], tables
    assert [c["name"] for c in columns] == ["id", "name", "spend"], columns

    # The types are asserted, not merely present. Reflecting every column as an unnamed type is a
    # way to make the picker "work" while telling the user nothing, and it would be invisible to a
    # row-count assertion because dlt infers types from the data anyway.
    by_name = {c["name"]: c["type"].upper() for c in columns}
    assert "INTEGER" in by_name["id"], by_name
    assert "VARCHAR" in by_name["name"] or "STRING" in by_name["name"], by_name
    assert "NUMERIC" in by_name["spend"] or "DECIMAL" in by_name["spend"], by_name
    assert "10" in by_name["spend"] and "2" in by_name["spend"], (
        f"the declared precision and scale were lost: {by_name['spend']}"
    )


def test_the_replacement_is_installed_by_importing_the_shared_module():
    """A positive artifact, not the absence of an error.

    The install is a module-level side effect of ``local_file_database``, which both the run path
    and the connector path import. Asserting it here means a refactor that stops importing it goes
    red in a test that names the reason, rather than in a DuckDB run weeks later.
    """
    import duckdb_engine

    import datanika.services.local_file_database  # noqa: F401  (imported for its side effect)

    assert getattr(duckdb_engine.Dialect.get_columns, "_datanika_duckdb_reflection", False)
    assert getattr(duckdb_engine.Dialect.get_multi_columns, "_datanika_duckdb_reflection", False)


def test_a_duckdb_source_loads_a_single_named_table(db_session, encryption, tmp_path):
    """``sql_table`` reflects one table rather than the whole database — its own path."""
    reading = _run(db_session, encryption, tmp_path, {"mode": "single_table", "table": "customers"})

    assert reading["status"] == RunStatus.SUCCESS, reading["error"][:400]
    assert reading["customers"] == list(ROW_IDS), reading
    assert reading["orders"] == [], "single_table loaded a table it was not asked for"
