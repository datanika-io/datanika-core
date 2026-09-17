"""A local-file database upload must find its file where the run executes (core#1401).

Test Connection runs in the web app. The upload runs in the worker. When only the web app could
see the file, SQLite's open-or-create mode handed the worker an **empty** database. The run
reflected no tables, finished `success` with 0 rows, and left a 0-byte file at the path, so every
later run of that upload passed too. core#979 closed this for Test Connection. This file covers
the run.

Every case below is a real run through ``DltRunnerService.execute`` into a DuckDB destination,
against a real file on disk, except where a case says it drives dlt's own engine factory directly.

Two guards, and each is exercised without the other, because two guards where either alone passes
the suite are one guard and a decoration (``test_local_file_connections.py`` learned that the hard
way):

* an existence check before the open, which is what lets the failure name the path;
* a read-only open, which is what keeps a file that disappears after the check from being created.
"""

import hashlib
import sqlite3
from pathlib import Path

import duckdb
import pytest
import sqlalchemy as sa
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database
from dlt.sources.sql_database.helpers import engine_from_credentials

import datanika.services.dlt_runner as dlt_runner
from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

#: File names SQLite's URI parser treats as syntax. Measured before the fix: opened as a URI without
#: encoding, ``hash#1.sqlite`` opened (and created) a database named ``hash``, and ``pct%20.sqlite``
#: failed to open at all.
AWKWARD_NAMES = ["online_store.sqlite", "hash#1.sqlite", "pct%20.sqlite", "with space.sqlite"]

TABLES = {"customers": 5, "orders": 7}


def _sqlite_file(path: Path, tables: dict[str, int]) -> None:
    con = sqlite3.connect(path)
    for table, count in tables.items():
        con.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, label TEXT)")
        con.executemany(
            f"INSERT INTO {table} VALUES (?, ?)",  # noqa: S608 - table names are literals above
            [(i, f"{table}-{i}") for i in range(count)],
        )
    con.commit()
    con.close()


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _execute(tmp_path: Path, source_type: str, path: Path, dlt_config: dict | None = None) -> dict:
    return DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=1,
        source_type=source_type,
        source_config={"path": str(path)},
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "dest.duckdb")},
        dlt_config=dlt_config or {},
        dataset_name="landed",
        run_id=1,
    )


def _landed_counts(tmp_path: Path) -> dict[str, int]:
    con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
    try:
        names = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'landed'"
            ).fetchall()
            if not r[0].startswith("_dlt")
        ]
        return {
            n: con.execute(f'SELECT count(*) FROM landed."{n}"').fetchone()[0]  # noqa: S608
            for n in names
        }
    finally:
        con.close()


def _worker_view(tmp_path: Path, name: str) -> Path:
    """The directory exists and the file is not in it: core#1401's arm B."""
    directory = tmp_path / "worker"
    directory.mkdir()
    return directory / name


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# SQLite
# ─────────────────────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "dlt_config",
    [{}, {"mode": "single_table", "table": "customers"}],
    ids=["full_database", "single_table"],
)
def test_a_sqlite_run_with_no_file_fails_names_the_path_and_creates_nothing(tmp_path, dlt_config):
    worker_view = _worker_view(tmp_path, "online_store.sqlite")

    with pytest.raises(DltRunnerError) as refused:
        _execute(tmp_path, "sqlite", worker_view, dlt_config)

    assert str(worker_view) in str(refused.value)
    assert not worker_view.exists(), "the run created the database it then found empty"
    assert list(worker_view.parent.iterdir()) == []


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_a_missing_file_is_refused_whatever_its_name(tmp_path, name):
    worker_view = _worker_view(tmp_path, name)

    with pytest.raises(DltRunnerError):
        _execute(tmp_path, "sqlite", worker_view)

    assert list(worker_view.parent.iterdir()) == []


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_control_a_real_sqlite_file_loads_every_row_and_is_not_modified(tmp_path, name):
    """core#1401 arm C: the file is there, and every row lands. Nothing is written beside it."""
    directory = tmp_path / "shared"
    directory.mkdir()
    path = directory / name
    _sqlite_file(path, TABLES)
    before = _digest(path)

    result = _execute(tmp_path, "sqlite", path)

    assert _landed_counts(tmp_path) == TABLES
    assert result["rows_loaded"] == sum(TABLES.values())
    assert _digest(path) == before, "the run modified the database it read"
    assert [p.name for p in directory.iterdir()] == [name], "the run created a file beside it"


def test_the_sqlite_open_is_read_only_on_its_own(tmp_path, monkeypatch):
    """The second guard without the first: a file that vanishes after the check is not created.

    Negative control: the writable credentials, the shape the run used before, DO create the file.
    That is what makes the first assertion a statement about the open rather than about SQLite.
    """
    monkeypatch.setattr(dlt_runner, "_require_local_database_file", lambda *_args: None)
    worker_view = _worker_view(tmp_path, "vanished.sqlite")

    with pytest.raises(sa.exc.OperationalError):
        _execute(tmp_path, "sqlite", worker_view)
    assert list(worker_view.parent.iterdir()) == []

    writable = DltRunnerService._to_dlt_credentials("sqlite", {"path": str(worker_view)})
    engine = engine_from_credentials(ConnectionStringCredentials(writable))
    with engine.connect() as conn:
        conn.execute(sa.text("SELECT 1"))
    engine.dispose()
    assert worker_view.exists(), "the writable credentials did not create the file either"


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# DuckDB: the same open-or-create class
# ─────────────────────────────────────────────────────────────────────────────────────────────────


def test_a_duckdb_run_with_no_file_fails_names_the_path_and_creates_nothing(tmp_path):
    worker_view = _worker_view(tmp_path, "warehouse.duckdb")

    with pytest.raises(DltRunnerError) as refused:
        _execute(tmp_path, "duckdb", worker_view)

    assert str(worker_view) in str(refused.value)
    assert list(worker_view.parent.iterdir()) == []


def test_the_duckdb_source_credentials_open_read_only(tmp_path, monkeypatch):
    """Driven through dlt's own engine factory, with the credentials ``build_source`` hands it.

    Not through ``execute``: a DuckDB source run fails later, at table reflection, for a reason of
    its own that this change does not touch. The engine factory is the consumer that matters for
    the open mode.
    """
    path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE t (a INTEGER)")
    con.execute("INSERT INTO t VALUES (1), (2)")
    con.close()
    before = _digest(path)

    creds = dlt_runner._local_file_source_credentials(
        "duckdb", DltRunnerService._to_dlt_credentials("duckdb", {"path": str(path)})
    )
    engine = engine_from_credentials(ConnectionStringCredentials(creds))
    try:
        with engine.connect() as conn:
            assert conn.execute(sa.text("SELECT count(*) FROM t")).scalar_one() == 2
            with pytest.raises(sa.exc.DBAPIError):
                conn.execute(sa.text("INSERT INTO t VALUES (3)"))
    finally:
        engine.dispose()
    assert _digest(path) == before

    # The open alone, past the existence check: a file that is not there is not created.
    monkeypatch.setattr(dlt_runner, "_require_local_database_file", lambda *_args: None)
    gone = _worker_view(tmp_path, "vanished.duckdb")
    creds = dlt_runner._local_file_source_credentials(
        "duckdb", DltRunnerService._to_dlt_credentials("duckdb", {"path": str(gone)})
    )
    with pytest.raises(sa.exc.DBAPIError):
        sql_database(credentials=creds)
    assert list(gone.parent.iterdir()) == []


def test_an_in_memory_database_is_not_a_file_to_look_for():
    """``:memory:`` names no file: there is nothing to require and no read-only mode to ask for."""
    creds = DltRunnerService._to_dlt_credentials("sqlite", {"path": ":memory:"})

    assert dlt_runner._local_file_source_credentials("sqlite", creds) == creds
