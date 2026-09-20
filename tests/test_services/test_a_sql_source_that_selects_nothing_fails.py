"""A SQL-source run whose selection resolves nothing fails before it loads (core#1445).

``SPEC_EARNED_VERDICTS`` §4.7. A source run answers two questions: *what did the
configuration select*, and *what did the selection hold*. An empty answer to the second is a
measurement, and the run succeeds with 0 rows. An empty answer to the first means the run never
reached anything it was asked to read, and ``success`` for it is a verdict about a load that did
not happen.

Measured before the fix, through ``DltRunnerService.execute`` on SQLite into DuckDB:

* an existing SQLite file holding no tables finished ``success`` with 0 rows, and dlt still created
  ``_dlt_loads``, ``_dlt_pipeline_state`` and ``_dlt_version`` in the destination;
* ``table_names`` naming a missing table failed, but with SQLAlchemy's reflection error, which names
  the engine URL and never says that nothing was loaded.

A run here is ``run_upload`` (``PRODUCT_RULES`` §16). Its witnesses are the run's status and
error, and the destination read back.
"""

from __future__ import annotations

import sqlite3
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
from tests.test_services.test_source_builders_move_rows import await_setup, requires_docker


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


def _sqlite(path: Path, tables: dict[str, int]) -> dict:
    con = sqlite3.connect(path)
    try:
        for name, count in tables.items():
            con.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY)")
            con.executemany(f"INSERT INTO {name} VALUES (?)", [(i,) for i in range(1, count + 1)])
        con.commit()
    finally:
        con.close()
    return {"path": str(path)}


def _run(db_session, encryption, tmp_path, source_type, source_config, dlt_config) -> dict:
    dest = tmp_path / "destination.duckdb"
    org = Organization(name="Acme", slug=f"acme-1445-{tmp_path.name[-16:]}")
    db_session.add(org)
    db_session.flush()
    src = Connection(
        org_id=org.id,
        name="source",
        connection_type=source_type,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt(source_config),
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
        name="selection",
        source_connection_id=src.id,
        destination_connection_id=dst.id,
        dlt_config={"write_disposition": "append", **dlt_config},
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
        "destination": _destination_tables(dest),
    }


def _destination_tables(dest: Path) -> list[tuple[str, str]]:
    """Every table in the destination, or ``[]`` when the run never created it."""
    if not dest.exists():
        return []
    con = duckdb.connect(str(dest))
    try:
        return con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2"
        ).fetchall()
    finally:
        con.close()


# ---------------------------------------------------------------------------------------------
# Nothing selected
# ---------------------------------------------------------------------------------------------


def test_an_existing_sqlite_file_with_no_tables_fails_before_loading(
    db_session, encryption, tmp_path
):
    source = tmp_path / "empty.sqlite"
    config = _sqlite(source, {})

    reading = _run(
        db_session, encryption, tmp_path, ConnectionType.SQLITE, config, {"mode": "full_database"}
    )

    assert reading["status"] == RunStatus.FAILED, reading
    assert str(source) in reading["error"] and "no tables" in reading["error"], reading["error"]
    assert "Nothing was loaded" in reading["error"], reading["error"]
    assert reading["destination"] == [], "the run wrote to the destination before refusing"


def test_a_named_table_that_does_not_exist_fails_and_is_named(db_session, encryption, tmp_path):
    """Even when the other names exist: loading some and dropping the rest is the same defect."""
    source = tmp_path / "events.sqlite"
    config = _sqlite(source, {"events": 3})

    reading = _run(
        db_session,
        encryption,
        tmp_path,
        ConnectionType.SQLITE,
        config,
        {"mode": "full_database", "table_names": ["events", "orders", "refunds"]},
    )

    assert reading["status"] == RunStatus.FAILED, reading
    assert "'orders'" in reading["error"] and "'refunds'" in reading["error"], reading["error"]
    assert "'events'" not in reading["error"], "a table that exists was named as missing"
    assert str(source) in reading["error"] and "Nothing was loaded" in reading["error"]
    assert reading["destination"] == []


# ---------------------------------------------------------------------------------------------
# Controls
# ---------------------------------------------------------------------------------------------


def test_control_tables_that_exist_with_no_rows_stay_success(db_session, encryption, tmp_path):
    """core#883's legitimately empty load. Failing it would be the mirror defect."""
    config = _sqlite(tmp_path / "zero.sqlite", {"events": 0})

    reading = _run(
        db_session, encryption, tmp_path, ConnectionType.SQLITE, config, {"mode": "full_database"}
    )

    assert reading["status"] == RunStatus.SUCCESS, reading["error"]
    assert reading["rows_loaded"] == 0


def test_control_names_that_exist_still_load(db_session, encryption, tmp_path):
    config = _sqlite(tmp_path / "two.sqlite", {"events": 3, "orders": 2})

    reading = _run(
        db_session,
        encryption,
        tmp_path,
        ConnectionType.SQLITE,
        config,
        {"mode": "full_database", "table_names": ["events", "orders"]},
    )

    assert reading["status"] == RunStatus.SUCCESS, reading["error"]
    assert reading["rows_loaded"] == 5


def test_control_single_table_with_a_name_the_source_folds_still_loads(
    db_session, encryption, tmp_path
):
    """Measured before the fix: ``EVENTS`` for table ``events`` loaded 3 rows via ``sql_table``."""
    config = _sqlite(tmp_path / "fold.sqlite", {"events": 3})

    reading = _run(
        db_session,
        encryption,
        tmp_path,
        ConnectionType.SQLITE,
        config,
        {"mode": "single_table", "table": "EVENTS"},
    )

    assert reading["status"] == RunStatus.SUCCESS, reading["error"]
    assert reading["rows_loaded"] == 3


def test_control_single_table_naming_a_missing_table_still_fails(db_session, encryption, tmp_path):
    config = _sqlite(tmp_path / "missing.sqlite", {"events": 3})

    reading = _run(
        db_session,
        encryption,
        tmp_path,
        ConnectionType.SQLITE,
        config,
        {"mode": "single_table", "table": "nope"},
    )

    assert reading["status"] == RunStatus.FAILED, reading


# ---------------------------------------------------------------------------------------------
# PostgreSQL: a schema that holds nothing, and one that does not exist
# ---------------------------------------------------------------------------------------------


def _postgres_source(container) -> dict:
    import sqlalchemy

    engine = sqlalchemy.create_engine(container.get_connection_url())
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS empty_schema"))
        conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS sales"))
        conn.execute(sqlalchemy.text("CREATE TABLE IF NOT EXISTS sales.orders (id int)"))
    engine.dispose()
    return {
        "host": container.get_container_host_ip(),
        "port": int(container.get_exposed_port(5432)),
        "user": container.username,
        "password": container.password,
        "database": container.dbname,
    }


@pytest.fixture(scope="module")
def postgres_source():
    """A container whose password is not also its database name.

    ``PostgresContainer``'s defaults are ``test`` for the user, the password *and* the database,
    and a run's stored text has every connection secret removed from it since core#1460. So with
    the defaults the message this test reads back says ``database '***'`` — the redactor doing
    exactly its job on a name that is indistinguishable from the password. Distinct values keep
    this test measuring the product rather than that collision; the SQLite arms above, whose
    paths survive verbatim, are the evidence that redaction leaves the diagnosis intact.
    """
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(
        "postgres:16-alpine", username="probe_user", password="pw-1445-8f2c", dbname="probe_db"
    ) as container:
        yield await_setup(
            "postgres accepting writes", lambda: _postgres_source(container), container=container
        )


@requires_docker
class TestAPostgresSchema:
    @pytest.mark.parametrize("schema", ["empty_schema", "no_such_schema"])
    def test_a_schema_that_holds_no_tables_fails_before_loading(
        self, db_session, encryption, tmp_path, postgres_source, schema
    ):
        reading = _run(
            db_session,
            encryption,
            tmp_path,
            ConnectionType.POSTGRES,
            dict(postgres_source),
            {"mode": "full_database", "source_schema": schema},
        )

        assert reading["status"] == RunStatus.FAILED, reading
        assert f"schema '{schema}'" in reading["error"], reading["error"]
        assert f"database '{postgres_source['database']}'" in reading["error"], reading["error"]
        assert "Nothing was loaded" in reading["error"]
        assert reading["destination"] == []

    def test_control_a_schema_with_a_table_loads(
        self, db_session, encryption, tmp_path, postgres_source
    ):
        reading = _run(
            db_session,
            encryption,
            tmp_path,
            ConnectionType.POSTGRES,
            dict(postgres_source),
            {"mode": "full_database", "source_schema": "sales"},
        )

        assert reading["status"] == RunStatus.SUCCESS, reading["error"]
        assert reading["rows_loaded"] == 0
