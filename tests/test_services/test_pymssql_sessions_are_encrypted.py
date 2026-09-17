"""SQL Server sessions opened through pymssql are encrypted, read on the server's side (core#1441).

``tests/test_pymssql_freetds.py`` pins the mechanism: importing ``datanika`` points FreeTDS at a
shipped configuration that requires encryption. A setting in a file is not a session, so this file
asks the only witness that cannot be talked into a wrong answer: SQL Server's own
``sys.dm_exec_connections.encrypt_option``, read on the very connection the product opened.

🚨 **Never replace the DMV reading with pymssql's ``encryption`` argument or a keyword in a
string.** Those record what was asked for. Only the server's view records what the session is.

How the reading reaches the product's own connection
-----------------------------------------------------
A listener on SQLAlchemy's ``Pool`` ``connect`` event runs one query on every new pymssql DBAPI
connection, whichever engine opened it. So the Test Connection arm calls the real
``ConnectionService.test_connection_verdict`` and the upload arm runs the real
``DltRunnerService.execute``, and neither is edited or wrapped.

Each arm asserts that at least one reading was taken. An arm that recorded nothing has measured
nothing, and without that assertion it would pass on an empty list.

Why there is a control, and what it proves
-------------------------------------------
The server under test does **not** force encryption, so the session property is decided by the
client. The control runs the same product path with ``FREETDSCONF`` pointed at a configuration
with no settings, and must read ``FALSE``. That shows the instrument can read ``FALSE`` on this
server, and that the shipped configuration is what makes the other arm read ``TRUE``.

If the control ever reads ``TRUE``, the instrument has stopped discriminating: a server image or a
driver whose defaults changed. Re-derive before trusting the ``TRUE`` arms.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import event
from sqlalchemy.pool import Pool

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import ConnectionService
from tests.test_services.test_source_builders_move_rows import (
    _extract_load,
    _rows,
    await_setup,
    requires_docker,
)

#: Pinned, not ``2022-latest``: a server whose TLS defaults moved would change what the control
#: reads.
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-CU24-ubuntu-22.04"
SA_PASSWORD = "Probe-1441-encrypt!x"
DATABASE = "probe"

SHIPPED = Path(__file__).resolve().parents[2] / "datanika" / "pymssql_freetds.conf"

ENCRYPT_OPTION = (
    "SELECT CAST(encrypt_option AS varchar(10)) FROM sys.dm_exec_connections "
    "WHERE session_id = @@SPID"
)

EXPECTED_ROWS = [("alpha", 100), ("beta", 200), ("gamma", 300)]


def _seed(container) -> dict:
    """Create the database and the table the upload reads. Setup only: no assertion lives here."""
    import pymssql

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(1433))
    conn = pymssql.connect(
        server=host,
        port=str(port),
        user="sa",
        password=SA_PASSWORD,
        login_timeout=10,
        autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"IF DB_ID('{DATABASE}') IS NULL CREATE DATABASE {DATABASE}")
        cur.execute(
            f"IF OBJECT_ID('{DATABASE}.dbo.widgets') IS NULL "
            f"CREATE TABLE {DATABASE}.dbo.widgets (name nvarchar(20) NOT NULL, price int NOT NULL)"
        )
        cur.execute(f"DELETE FROM {DATABASE}.dbo.widgets")
        cur.executemany(
            f"INSERT INTO {DATABASE}.dbo.widgets (name, price) VALUES (%s, %d)", EXPECTED_ROWS
        )
    finally:
        conn.close()
    return {"host": host, "port": port, "user": "sa", "password": SA_PASSWORD, "database": DATABASE}


@pytest.fixture(scope="module")
def sqlserver():
    from testcontainers.mssql import SqlServerContainer

    with SqlServerContainer(MSSQL_IMAGE, password=SA_PASSWORD, dbname="master") as container:
        yield await_setup(
            "sql server accepting logins", lambda: _seed(container), container=container
        )


@contextmanager
def pymssql_session_encryption():
    """Collect ``encrypt_option`` for every pymssql connection opened inside the block."""
    readings: list[str] = []

    def record(dbapi_connection, _connection_record):
        if not type(dbapi_connection).__module__.startswith("pymssql"):
            return
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(ENCRYPT_OPTION)
            readings.append(str(cursor.fetchone()[0]))
        finally:
            cursor.close()

    event.listen(Pool, "connect", record)
    try:
        yield readings
    finally:
        event.remove(Pool, "connect", record)


@pytest.fixture
def without_the_shipped_configuration(monkeypatch, tmp_path):
    """The control: FreeTDS reads a configuration file with no settings in it."""
    empty = tmp_path / "freetds.conf"
    empty.write_text("[global]\n", encoding="utf-8")
    monkeypatch.setenv("FREETDSCONF", str(empty))
    return empty


def _assert_the_shipped_configuration_is_in_force():
    value = os.environ.get("FREETDSCONF")
    assert value and Path(value).resolve() == SHIPPED.resolve(), (
        f"FREETDSCONF is {value!r}, not the shipped {SHIPPED}. The TRUE arms measure the mechanism "
        "importing datanika sets up, so they are meaningless without it."
    )


@requires_docker
class TestTestConnectionSessions:
    """The connection form's Test Connection button, for both types that reach pymssql."""

    @pytest.mark.parametrize("connection_type", [ConnectionType.MSSQL, ConnectionType.SYNAPSE])
    def test_the_session_is_encrypted(self, sqlserver, connection_type):
        _assert_the_shipped_configuration_is_in_force()
        with pymssql_session_encryption() as readings:
            verdict = ConnectionService.test_connection_verdict(dict(sqlserver), connection_type)
        assert verdict.ok, verdict.message
        assert readings, "no pymssql session was opened, so nothing was measured"
        assert set(readings) == {"TRUE"}, (
            f"{connection_type.value} Test Connection opened a session the server reports as "
            f"encrypt_option={readings}"
        )

    def test_control_without_the_configuration_the_session_is_not_encrypted(
        self, sqlserver, without_the_shipped_configuration
    ):
        with pymssql_session_encryption() as readings:
            verdict = ConnectionService.test_connection_verdict(
                dict(sqlserver), ConnectionType.MSSQL
            )
        assert verdict.ok, verdict.message
        assert readings, "no pymssql session was opened, so nothing was measured"
        assert set(readings) == {"FALSE"}, (
            f"the control read encrypt_option={readings}: the instrument no longer discriminates "
            "on this server, so the TRUE arms prove nothing until that is re-derived"
        )


@requires_docker
class TestUploadSourceSessions:
    """The upload's SQL Server source: the real runner, rows read back from the destination."""

    def test_rows_move_over_encrypted_sessions(self, sqlserver, tmp_path):
        _assert_the_shipped_configuration_is_in_force()
        with pymssql_session_encryption() as readings:
            db_path = _extract_load(
                tmp_path,
                "mssql",
                dict(sqlserver),
                {"mode": "single_table", "table": "widgets", "source_schema": "dbo"},
            )
        assert _rows(db_path, "widgets") == EXPECTED_ROWS
        assert readings, "no pymssql session was opened, so nothing was measured"
        assert set(readings) == {"TRUE"}, (
            f"the upload source read its rows over sessions the server reports as "
            f"encrypt_option={readings}"
        )

    def test_control_without_the_configuration_rows_move_unencrypted(
        self, sqlserver, tmp_path, without_the_shipped_configuration
    ):
        with pymssql_session_encryption() as readings:
            db_path = _extract_load(
                tmp_path,
                "mssql",
                dict(sqlserver),
                {"mode": "single_table", "table": "widgets", "source_schema": "dbo"},
            )
        assert _rows(db_path, "widgets") == EXPECTED_ROWS
        assert readings, "no pymssql session was opened, so nothing was measured"
        assert set(readings) == {"FALSE"}, (
            f"the control read encrypt_option={readings}: the instrument no longer discriminates "
            "on this server, so the TRUE arm proves nothing until that is re-derived"
        )
