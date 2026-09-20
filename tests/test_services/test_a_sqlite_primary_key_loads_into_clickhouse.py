"""A SQLite ``INTEGER PRIMARY KEY`` loads into ClickHouse (core#1439).

Measured before the fix, through the real ``run_upload`` against a real server::

    Pipeline execution failed at `step=load` ... DatabaseTerminalException
    Code: 44. DB::Exception: Sorting key contains nullable columns, but merge tree setting
    `allow_nullable_key` is disabled.

dlt marks the reflected primary key as the table's primary key and its ClickHouse destination uses
that key as the MergeTree sorting key. SQLAlchemy's SQLite reflection reports every column of a
SQLite table as nullable, primary key included, so the column was created ``Nullable(...)`` and
ClickHouse refused to sort on it.

**AC1, measured per source rather than assumed** — the dlt schema is what the destination reads,
so it is what is reported here:

===========================================  ===============  =============
source and declaration                       ``nullable``     ``primary_key``
===========================================  ===============  =============
SQLite   ``id INTEGER PRIMARY KEY``          **True**         True
SQLite   ``code TEXT PRIMARY KEY``           **True**         True
Postgres ``id SERIAL PRIMARY KEY``           False            True
Postgres ``id INTEGER PRIMARY KEY``          False            True
DuckDB   ``id INTEGER PRIMARY KEY``          False            None (no key reflected at all)
===========================================  ===============  =============

So **SQLite is the affected source**, and a schema saying *primary key* and *nullable* about one
column is self-contradictory whatever the destination does with it.

⚠️ **The correction is narrower than "a primary key is never nullable", and the reason is
measured, not reasoned.** SQLite genuinely accepts a NULL in a ``TEXT PRIMARY KEY`` — inserting one
stores the row — so declaring every primary-key column NOT NULL would assert something untrue of
real data and break loads that work today. A single-column ``INTEGER PRIMARY KEY`` is different: it
is the rowid alias, and inserting NULL auto-assigns the next rowid, measured. That column cannot
hold NULL, so calling it NOT NULL is correcting an under-report, not inventing a constraint.

The residual — a genuinely nullable key column into ClickHouse — is refused before the load with a
message naming the column and the cause, which is AC2's second half.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

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
from datanika.services.upload_service import to_dataset_name
from datanika.tasks.upload_tasks import run_upload
from tests.test_services.test_clickhouse_destination_ports import (
    CLICKHOUSE_IMAGE,
    DATABASE,
    PASSWORD,
    USER,
    stored_connection,
)
from tests.test_services.test_source_builders_move_rows import requires_docker

pytestmark = requires_docker

WIDGETS = [(1, "bolt", 10), (2, "nut", 20), (3, "washer", 30)]


@pytest.fixture(scope="module")
def clickhouse():
    from testcontainers.clickhouse import ClickHouseContainer

    container = ClickHouseContainer(
        CLICKHOUSE_IMAGE, username=USER, password=PASSWORD, dbname=DATABASE
    )
    # dlt's ClickHouse destination dials the NATIVE port, which it derives from the stored host
    # rather than reading from the config (core#1341). A randomly mapped port is therefore not
    # reachable, so the container's own ports are bound — the same shape
    # `test_clickhouse_upload_is_catalogued.py` uses, and for the same reason.
    container.with_bind_ports(9000, 9000)
    container.with_bind_ports(8123, 8123)
    with container:
        yield container


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


def _sqlite(path: Path, ddl: str, rows: list[tuple]) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(ddl)
        placeholders = ", ".join("?" * len(rows[0]))
        table = ddl.split()[2]
        con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
        con.commit()
    finally:
        con.close()


def _loaded(clickhouse, dataset: str, table: str) -> list[tuple]:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse.get_container_host_ip(),
        port=8123,
        username=USER,
        password=PASSWORD,
    )
    try:
        result = client.query(
            f"SELECT id, name, price FROM {DATABASE}.{dataset}___{table} ORDER BY id"
        )
        return [tuple(r) for r in result.result_rows]
    finally:
        client.close()


def _run(db_session, encryption, tmp_path, clickhouse, source_path: Path, dlt_config) -> dict:
    org = Organization(name="Acme", slug=f"acme-1439-{tmp_path.name[-12:]}")
    db_session.add(org)
    db_session.flush()
    src = Connection(
        org_id=org.id,
        name="sqlite source",
        connection_type=ConnectionType.SQLITE,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt({"path": str(source_path)}),
    )
    destination_config = stored_connection(clickhouse.get_container_host_ip())
    destination_config["database"] = DATABASE
    dst = Connection(
        org_id=org.id,
        name="Click Warehouse",
        connection_type=ConnectionType.CLICKHOUSE,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=encryption.encrypt(destination_config),
    )
    db_session.add_all([src, dst])
    db_session.flush()
    upload = Upload(
        org_id=org.id,
        name=f"widgets {tmp_path.name[-8:]}",
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
        "dataset": to_dataset_name(upload.name),
    }


def test_a_sqlite_integer_primary_key_loads_into_clickhouse(
    db_session, encryption, tmp_path, clickhouse
):
    source = tmp_path / "widgets.sqlite"
    _sqlite(
        source,
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)",
        WIDGETS,
    )

    reading = _run(
        db_session,
        encryption,
        tmp_path,
        clickhouse,
        source,
        {"mode": "single_table", "table": "widgets"},
    )

    assert reading["status"] == RunStatus.SUCCESS, reading["error"][:500]
    assert _loaded(clickhouse, reading["dataset"], "widgets") == WIDGETS


def test_a_full_database_load_of_the_same_table_also_lands(
    db_session, encryption, tmp_path, clickhouse
):
    """``sql_database`` reflects through a different call than ``sql_table``."""
    source = tmp_path / "widgets_all.sqlite"
    _sqlite(
        source,
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)",
        WIDGETS,
    )

    reading = _run(db_session, encryption, tmp_path, clickhouse, source, {"mode": "full_database"})

    assert reading["status"] == RunStatus.SUCCESS, reading["error"][:500]
    assert _loaded(clickhouse, reading["dataset"], "widgets") == WIDGETS


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        ("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", "id"),
        ("CREATE TABLE t (id INTEGER PRIMARY KEY ASC, v TEXT)", "id"),
        # Not rowid aliases, and each really can hold NULL in SQLite:
        ("CREATE TABLE t (code TEXT PRIMARY KEY, v TEXT)", None),
        ("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)", None),
        ("CREATE TABLE t (id INTEGER, ver INTEGER, v TEXT, PRIMARY KEY (id, ver))", None),
        ("CREATE TABLE t (id INTEGER, v TEXT)", None),
    ],
)
def test_the_rowid_alias_rule_matches_only_what_cannot_hold_null(tmp_path, ddl, expected):
    """The rule is asserted directly, because its consequences are not observable end to end.

    Widening it to any single-column key, or to a composite one, produces a wrong ``NOT NULL`` on a
    column that can hold NULL — and the end-to-end arms cannot see that, because a composite key's
    *other* column keeps the run refused either way. A rule whose mistakes are invisible downstream
    is a rule to test where it is written.
    """
    from datanika.services.dlt_runner import _sqlite_rowid_alias

    path = tmp_path / f"rule_{abs(hash(ddl))}.sqlite"
    con = sqlite3.connect(path)
    try:
        con.execute(ddl)
        con.commit()
    finally:
        con.close()

    assert _sqlite_rowid_alias({"drivername": "sqlite", "database": str(path)}, "t") == expected


def test_a_genuinely_nullable_key_is_refused_before_the_load_and_says_why(
    db_session, encryption, tmp_path, clickhouse
):
    """AC2's second half, for the case the correction deliberately does not cover.

    A SQLite ``TEXT PRIMARY KEY`` really can hold NULL, so it stays nullable. ClickHouse really
    cannot sort on it. The run must say that, rather than surfacing ``Code: 44``.
    """
    source = tmp_path / "codes.sqlite"
    _sqlite(
        source,
        "CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT, price INTEGER)",
        [("a", "bolt", 10), ("b", "nut", 20)],
    )

    reading = _run(
        db_session,
        encryption,
        tmp_path,
        clickhouse,
        source,
        {"mode": "single_table", "table": "widgets"},
    )

    assert reading["status"] == RunStatus.FAILED, reading
    error = reading["error"]
    assert "id" in error, error
    assert "widgets" in error, error
    assert "nullable" in error.lower(), error
    assert "Code: 44" not in error, "the raw server error reached the user instead of a diagnosis"
