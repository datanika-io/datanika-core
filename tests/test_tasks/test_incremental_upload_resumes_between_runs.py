"""An incremental upload's second run extracts only what its first run left behind (core#1404).

Product traced the chain in code and did not measure it: ``run_upload`` calls
``DltRunnerService.execute(pipeline_id=run_id, run_id=run_id)``, so every run builds a dlt pipeline
named ``pipeline_{run_id}_run_{run_id}``; its working directory is deleted when the run ends; and
dlt restores an incremental cursor from the destination **by pipeline name**. A name no earlier run
used finds no cursor.

Measured here through the product's own ``run_upload``: a real SQLite source, a real DuckDB
destination, and the ``dlt_config`` the upload form's own ``_build_config`` stores when a user ticks
*Enable incremental*. Two runs, three rows added to the source between them. What run 2
**extracted** is read three independent ways, because under ``merge`` the destination cannot tell a
resume from a restart — both leave each row once:

1. the ``SELECT`` the loader sent to the source, captured at the SQLAlchemy engine, and the rows
   that statement selects when re-run against the unchanged source;
2. ``runs.rows_loaded`` — the pipeline's own normalize count;
3. the destination read back — under ``append``, a restart lands run 1's rows a second time.

**The control changes one thing:** the dlt pipeline name is held stable across the two runs. The
working directory is still deleted after each run, as ``run_upload`` does, so a control that resumes
shows both that the instruments can see a resume and that the per-run name is sufficient to explain
a restart: dlt's restore from the destination works whenever the name matches.

Measured 2026-09-17 on core ``dev`` ``3a2d414`` (5 rows, then 3 more; cursor ``updated_at``):

=============  ===========  ==============================  =================  =============
arm            disposition  run 2's SELECT                  run 2 rows_loaded  destination
=============  ===========  ==============================  =================  =============
as shipped     append       no WHERE, selects 8             8                  13 rows, 8 ids
as shipped     merge        no WHERE, selects 8             8                  8 rows, 8 ids
stable name    append       ``updated_at >= ?`` (50), 4     3                  8 rows, 8 ids
stable name    merge        ``updated_at >= ?`` (50), 4     3                  8 rows, 8 ids
=============  ===========  ==============================  =================  =============

(The resumed ``SELECT`` includes the boundary row; dlt drops it by the hashes it keeps in state, so
3 rows load.) ``append`` is the form's default write disposition.

Also here: core#1414 — an **Initial value** typed in the form is stored as text, and a run with an
integer cursor fails at extract.

**Fixed together (core#1404, core#1414; contract: ``docs/specs/SPEC_INCREMENTAL_UPLOADS.md``).** An
incremental upload's runs now share one dlt pipeline name, derived from the cursor's identity
(``datanika.services.incremental_identity``), and the loader converts a text initial value to the
cursor column's type. The three strict xfails lost their markers in that change. The arms at the
end are the spec's §2.2 (a failed run does not move the cursor) and §2.3 (the identity), each with
its negative control. §2.4, overlapping runs, needs two real database sessions and is in
``test_incremental_upload_runs_do_not_overlap.py``.
"""

from __future__ import annotations

import contextlib
import os
import re
import sqlite3
import urllib.parse
from pathlib import Path

import duckdb
import pytest
from cryptography.fernet import Fernet
from sqlalchemy import event
from sqlalchemy.engine import Engine

from datanika.config import settings
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.dlt_runner import DltRunnerService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.upload_tasks import run_upload
from tests.test_services.test_rerun_lands_each_record_once import form_config

svc = ExecutionService()

#: Rows present before run 1, then rows added before run 2. The cursor strictly increases, so
#: "past run 1's cursor" is exactly LATER.
FIRST = [(1, 10, "a"), (2, 20, "b"), (3, 30, "c"), (4, 40, "d"), (5, 50, "e")]
LATER = [(6, 60, "f"), (7, 70, "g"), (8, 80, "h")]
RUN_ONE_CURSOR = max(updated_at for _id, updated_at, _note in FIRST)

_FROM_EVENTS = re.compile(r'\bFROM\s+"?events"?', re.IGNORECASE)


class HarnessError(RuntimeError):
    """A setup invariant failed. Never absorbed by an ``xfail(raises=AssertionError)``."""


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    """dlt's working files and the dbt project go to tmp, where the cleanup can be observed."""
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


def _seed(path: Path, rows) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS events "
            "(id INTEGER PRIMARY KEY, updated_at INTEGER NOT NULL, note TEXT)"
        )
        con.executemany("INSERT INTO events VALUES (?, ?, ?)", rows)
        con.commit()
    finally:
        con.close()


@contextlib.contextmanager
def _source_selects(source: Path):
    """Every ``SELECT … FROM events`` any engine sends to THIS source file, with its parameters."""
    seen: list[tuple[str, object]] = []
    want = os.path.normcase(os.path.abspath(source))

    def _capture(conn, cursor, statement, parameters, context, executemany):
        database = conn.engine.url.database or ""
        if database.startswith("file:"):
            # Since core#1401 a SQLite source opens read-only, through a percent-encoded `file:`
            # URI filename (`local_file_database.sqlite_uri_filename`), so decode it back to a path.
            database = urllib.parse.unquote(database.removeprefix("file:"))
        if os.path.normcase(os.path.abspath(database)) != want:
            return
        if statement.lstrip().upper().startswith("SELECT") and _FROM_EVENTS.search(statement):
            seen.append((statement, parameters))

    event.listen(Engine, "before_cursor_execute", _capture)
    try:
        yield seen
    finally:
        event.remove(Engine, "before_cursor_execute", _capture)


def _rows_selected(source: Path, statement: str, parameters) -> int:
    """Re-run a captured statement against the unchanged source: how many rows does it select?"""
    con = sqlite3.connect(source)
    try:
        return len(con.execute(statement, parameters or ()).fetchall())
    finally:
        con.close()


def _destination(dest: Path) -> dict:
    """Read the rows back: the table a user queries, not dlt's staging copy of it."""
    con = duckdb.connect(str(dest))
    try:
        found = [
            row
            for row in con.execute(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = 'events'"
            ).fetchall()
            if not row[0].endswith("_staging")  # `merge` also writes `<dataset>_staging.events`
        ]
        if len(found) != 1:
            raise HarnessError(f"expected one destination table named events, found {found}")
        schema = found[0][0]
        rows, distinct = con.execute(
            f'SELECT count(*), count(DISTINCT id) FROM "{schema}"."events"'
        ).fetchone()
        ids = [
            r[0] for r in con.execute(f'SELECT id FROM "{schema}"."events" ORDER BY id').fetchall()
        ]
        states = con.execute(
            f'SELECT pipeline_name FROM "{schema}"."_dlt_pipeline_state" ORDER BY created_at'
        ).fetchall()
    finally:
        con.close()
    return {"rows": rows, "distinct_ids": distinct, "ids": ids, "states": [s[0] for s in states]}


def _hold_pipeline_name_stable(monkeypatch, upload_id: int) -> str:
    """The control's one change: a stable dlt pipeline name of the harness's own choosing.

    Since core#1404 the product keeps an incremental upload's name stable itself, inside a
    working directory named after the run, which ``run_upload`` still deletes after every run. So
    this patch replaces only the NAME, with one that owes nothing to the product's identity
    function, and a resume can still only come from the destination.
    """
    stable = 900_000 + upload_id
    build = DltRunnerService.build_pipeline

    def _build(self, pipeline_id, dest_type, dest_config, dataset_name=None, **_ignored):
        return build(self, stable, dest_type, dest_config, dataset_name=dataset_name)

    monkeypatch.setattr(DltRunnerService, "build_pipeline", _build)
    return f"pipeline_{stable}"


@pytest.fixture
def make_upload(db_session, encryption, tmp_path):
    """An org, a SQLite source, a DuckDB destination and an upload saved through the real form."""

    def _make(**form_values):
        source = tmp_path / "source.sqlite"
        dest = tmp_path / "dest.duckdb"
        _seed(source, FIRST)
        org = Organization(name="Acme", slug="acme-1404")
        db_session.add(org)
        db_session.flush()
        src, dst = (
            Connection(
                org_id=org.id,
                name=name,
                connection_type=ctype,
                direction=direction,
                config_encrypted=encryption.encrypt({"path": str(path)}),
            )
            for name, ctype, direction, path in (
                ("events-db", ConnectionType.SQLITE, ConnectionDirection.SOURCE, source),
                ("warehouse", ConnectionType.DUCKDB, ConnectionDirection.DESTINATION, dest),
            )
        )
        db_session.add_all([src, dst])
        db_session.flush()
        dlt_config = form_config(
            "sqlite",
            form_mode="single_table",
            form_table="events",
            form_enable_incremental=True,
            form_cursor_path="updated_at",
            **form_values,
        )
        if dlt_config.get("incremental", {}).get("cursor_path") != "updated_at":
            raise HarnessError(f"the form did not store an incremental cursor: {dlt_config}")
        upload = Upload(
            org_id=org.id,
            name="events",
            source_connection_id=src.id,
            destination_connection_id=dst.id,
            dlt_config=dlt_config,
            status=UploadStatus.DRAFT,
        )
        db_session.add(upload)
        db_session.flush()
        return org, upload, source, dest

    return _make


def _run(db_session, encryption, org, upload, source: Path) -> dict:
    run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    with _source_selects(source) as selects:
        run_upload(run.id, org.id, session=db_session, encryption=encryption)
    db_session.refresh(run)
    reading = {"status": run.status, "error": run.error_message, "rows_loaded": run.rows_loaded}
    if selects:
        statement, parameters = selects[-1]
        reading["where"] = statement.split("WHERE", 1)[1].strip() if "WHERE" in statement else None
        reading["parameters"] = list(parameters or ())
        reading["selected"] = _rows_selected(source, statement, parameters)
    return reading


def _two_runs(db_session, encryption, org, upload, source, dest) -> dict:
    first = _run(db_session, encryption, org, upload, source)
    if first["status"] != RunStatus.SUCCESS or first["rows_loaded"] != len(FIRST):
        raise HarnessError(f"run 1 did not load the source: {first}")
    if "where" not in first:
        raise HarnessError("no SELECT against the source was captured in run 1: the probe is blind")
    if first.get("where") is not None:
        raise HarnessError(f"run 1 already filtered on a cursor, so run 2 proves nothing: {first}")
    pipelines_dir = settings.dlt_pipelines_dir
    left = sorted(os.listdir(pipelines_dir)) if os.path.isdir(pipelines_dir) else []
    if left:
        raise HarnessError(f"run 1 left working directories {left}; a resume could come from them")
    _seed(source, LATER)
    second = _run(db_session, encryption, org, upload, source)
    if second["status"] != RunStatus.SUCCESS or "where" not in second:
        raise HarnessError(f"run 2 did not complete with a captured SELECT: {second}")
    return {"run_1": first, "run_2": second, "destination": _destination(dest)}


def _assert_run_two_resumed(reading: dict) -> None:
    second = reading["run_2"]
    assert second["where"] is not None and second["parameters"] == [RUN_ONE_CURSOR], (
        f"run 2 asked the source for every row again instead of the rows past run 1's cursor "
        f"({RUN_ONE_CURSOR}): WHERE={second['where']!r} parameters={second['parameters']} "
        f"selected {second['selected']} rows"
    )
    assert second["rows_loaded"] == len(LATER), (
        f"run 2 loaded {second['rows_loaded']} rows; only the {len(LATER)} added since run 1 "
        "are new"
    )


# ---------------------------------------------------------------------------------------------
# Control — the same two runs with one change: the pipeline name is stable
# ---------------------------------------------------------------------------------------------


def test_control_a_stable_pipeline_name_resumes_from_the_destination(
    db_session, encryption, make_upload, monkeypatch
):
    """Without this, a red below is also what a harness that cannot see a resume produces."""
    org, upload, source, dest = make_upload(form_write_disposition="append")
    stable = _hold_pipeline_name_stable(monkeypatch, upload.id)

    reading = _two_runs(db_session, encryption, org, upload, source, dest)

    _assert_run_two_resumed(reading)
    assert reading["destination"]["rows"] == reading["destination"]["distinct_ids"] == 8
    assert reading["destination"]["states"] == [stable, stable], reading["destination"]


# ---------------------------------------------------------------------------------------------
# core#1404 — every run of an incremental upload resumes (these two were the strict xfails)
# ---------------------------------------------------------------------------------------------


def test_under_append_run_two_loads_only_the_new_rows(db_session, encryption, make_upload):
    """``append`` is the form's default: a restart lands run 1's rows a second time."""
    org, upload, source, dest = make_upload(form_write_disposition="append")

    reading = _two_runs(db_session, encryption, org, upload, source, dest)

    _assert_run_two_resumed(reading)
    destination = reading["destination"]
    assert destination["rows"] == destination["distinct_ids"] == 8, (
        f"the destination holds {destination['rows']} rows for {destination['distinct_ids']} ids"
    )


def test_under_merge_run_two_extracts_only_the_new_rows(db_session, encryption, make_upload):
    """Under ``merge`` the destination is right either way; only the extract can tell."""
    org, upload, source, dest = make_upload(form_write_disposition="merge", form_primary_key="id")

    reading = _two_runs(db_session, encryption, org, upload, source, dest)

    destination = reading["destination"]
    if not destination["rows"] == destination["distinct_ids"] == 8:
        raise HarnessError(f"merge left duplicates, which is a different defect: {destination}")
    _assert_run_two_resumed(reading)


# ---------------------------------------------------------------------------------------------
# core#1414 — an Initial value typed in the form is stored as text
# ---------------------------------------------------------------------------------------------


def test_control_the_same_upload_with_no_initial_value_loads(db_session, encryption, make_upload):
    """The one fact the core#1414 test below changes is the Initial value."""
    org, upload, source, dest = make_upload()

    reading = _run(db_session, encryption, org, upload, source)

    assert reading["status"] == RunStatus.SUCCESS, reading["error"]
    assert _destination(dest)["ids"] == [1, 2, 3, 4, 5]


def test_an_initial_value_typed_in_the_form_is_honoured(db_session, encryption, make_upload):
    org, upload, source, dest = make_upload(form_initial_value="30")
    if upload.dlt_config["incremental"].get("initial_value") is None:
        raise HarnessError(f"the form did not store the initial value: {upload.dlt_config}")

    reading = _run(db_session, encryption, org, upload, source)

    assert reading["status"] == RunStatus.SUCCESS, (
        f"an integer cursor with the Initial value 30 failed the run: {reading['error']}"
    )
    assert _destination(dest)["ids"] == [3, 4, 5]


# ---------------------------------------------------------------------------------------------
# SPEC_INCREMENTAL_UPLOADS §2.2 — a failed run does not move the cursor
# ---------------------------------------------------------------------------------------------


@contextlib.contextmanager
def _events_refuses_inserts(dest: Path):
    """Replace the destination table with a view of itself, so a load into it fails at `step=load`.

    Everything before the load still happens: the extract reads past run 1's cursor and dlt writes
    the advanced cursor into the load package. That is the case §2.2 exists for: a cursor that
    advanced in a package whose rows never landed.
    """
    con = duckdb.connect(str(dest))
    try:
        schema = con.execute(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name = 'events' AND table_type = 'BASE TABLE' "
            "AND table_schema NOT LIKE '%_staging'"
        ).fetchone()[0]
        con.execute(f'ALTER TABLE "{schema}"."events" RENAME TO "events_held"')
        con.execute(f'CREATE VIEW "{schema}"."events" AS SELECT * FROM "{schema}"."events_held"')
    finally:
        con.close()
    try:
        yield
    finally:
        con = duckdb.connect(str(dest))
        try:
            con.execute(f'DROP VIEW "{schema}"."events"')
            con.execute(f'ALTER TABLE "{schema}"."events_held" RENAME TO "events"')
        finally:
            con.close()


def test_a_run_that_fails_at_load_does_not_move_the_cursor(db_session, encryption, make_upload):
    """Run 1 succeeds, rows are added, run 2 fails at load, run 3 succeeds (SPEC §2.2).

    Run 3 must extract from run 1's cursor, and the destination must hold every added row exactly
    once. A cursor that advanced in run 2's package would make run 3 skip the rows run 2 never
    landed.
    """
    org, upload, source, dest = make_upload(form_write_disposition="append")
    first = _run(db_session, encryption, org, upload, source)
    if first["status"] != RunStatus.SUCCESS or first["rows_loaded"] != len(FIRST):
        raise HarnessError(f"run 1 did not load the source: {first}")
    _seed(source, LATER)

    with _events_refuses_inserts(dest):
        second = _run(db_session, encryption, org, upload, source)
    if second["status"] != RunStatus.FAILED or second.get("parameters") != [RUN_ONE_CURSOR]:
        raise HarnessError(f"run 2 did not fail after extracting past run 1's cursor: {second}")

    third = _run(db_session, encryption, org, upload, source)

    assert third["status"] == RunStatus.SUCCESS, third["error"]
    assert third["parameters"] == [RUN_ONE_CURSOR], (
        f"run 3 extracted from {third['parameters']}, not from run 1's cursor ({RUN_ONE_CURSOR}): "
        "the failed run moved the cursor"
    )
    destination = _destination(dest)
    assert destination["rows"] == destination["distinct_ids"] == len(FIRST) + len(LATER), (
        destination
    )


# ---------------------------------------------------------------------------------------------
# SPEC_INCREMENTAL_UPLOADS §2.3 — the cursor belongs to one source table and one destination
# ---------------------------------------------------------------------------------------------


def _another_connection(db_session, encryption, org, name, ctype, direction, path) -> Connection:
    conn = Connection(
        org_id=org.id,
        name=name,
        connection_type=ctype,
        direction=direction,
        config_encrypted=encryption.encrypt({"path": str(path)}),
    )
    db_session.add(conn)
    db_session.flush()
    return conn


def test_a_new_destination_receives_every_row_not_only_those_past_the_old_cursor(
    db_session, encryption, make_upload, tmp_path
):
    org, upload, source, dest = make_upload(form_write_disposition="append")
    first = _run(db_session, encryption, org, upload, source)
    if first["status"] != RunStatus.SUCCESS:
        raise HarnessError(f"run 1 did not load the source: {first}")
    _seed(source, LATER)
    new_dest = tmp_path / "second-warehouse.duckdb"
    moved = _another_connection(
        db_session,
        encryption,
        org,
        "warehouse two",
        ConnectionType.DUCKDB,
        ConnectionDirection.DESTINATION,
        new_dest,
    )
    upload.destination_connection_id = moved.id
    db_session.flush()

    second = _run(db_session, encryption, org, upload, source)

    assert second["status"] == RunStatus.SUCCESS, second["error"]
    assert second["where"] is None, f"the new destination's first run filtered: {second['where']}"
    assert _destination(new_dest)["ids"] == list(range(1, 9))


def test_a_new_source_connection_extracts_from_the_initial_value(
    db_session, encryption, make_upload, tmp_path
):
    org, upload, source, dest = make_upload(form_write_disposition="append")
    first = _run(db_session, encryption, org, upload, source)
    if first["status"] != RunStatus.SUCCESS:
        raise HarnessError(f"run 1 did not load the source: {first}")
    other_source = tmp_path / "second-source.sqlite"
    _seed(other_source, [(101, 5, "older than run 1's cursor"), (102, 60, "newer")])
    moved = _another_connection(
        db_session,
        encryption,
        org,
        "events db two",
        ConnectionType.SQLITE,
        ConnectionDirection.SOURCE,
        other_source,
    )
    upload.source_connection_id = moved.id
    db_session.flush()

    second = _run(db_session, encryption, org, upload, other_source)

    assert second["status"] == RunStatus.SUCCESS, second["error"]
    assert second["where"] is None, (
        f"a cursor recorded for the first database was applied to the second: {second['where']} "
        f"{second['parameters']}, which skips its older rows"
    )
    assert {101, 102} <= set(_destination(dest)["ids"])


def test_control_a_change_outside_the_key_still_resumes(db_session, encryption, make_upload):
    """SPEC §2.3's negative control: without it, a key that restarts on every save passes above."""
    org, upload, source, dest = make_upload(form_write_disposition="append")
    first = _run(db_session, encryption, org, upload, source)
    if first["status"] != RunStatus.SUCCESS:
        raise HarnessError(f"run 1 did not load the source: {first}")
    _seed(source, LATER)
    upload.dlt_config = {**upload.dlt_config, "batch_size": 1000}
    db_session.flush()

    second = _run(db_session, encryption, org, upload, source)

    _assert_run_two_resumed({"run_2": second})
    destination = _destination(dest)
    assert destination["rows"] == destination["distinct_ids"] == 8, destination


# ---------------------------------------------------------------------------------------------
# SPEC_INCREMENTAL_UPLOADS §3 — an upload with no cursor behaves exactly as before
# ---------------------------------------------------------------------------------------------


def test_control_an_upload_without_a_cursor_keeps_one_pipeline_name_per_run(
    db_session, encryption, make_upload
):
    """core#1336 designed non-incremental uploads around no dlt state crossing runs.

    Read from the destination: each run's state row carries the pipeline name that run used.
    """
    org, upload, source, dest = make_upload(form_write_disposition="append")
    upload.dlt_config = {k: v for k, v in upload.dlt_config.items() if k != "incremental"}
    db_session.flush()

    first = _run(db_session, encryption, org, upload, source)
    second = _run(db_session, encryption, org, upload, source)

    assert first["status"] == second["status"] == RunStatus.SUCCESS, (first, second)
    states = _destination(dest)["states"]
    assert len(states) == 2 and len(set(states)) == 2, (
        f"two runs of an upload with no cursor recorded state under {states}: a shared name would "
        "carry dlt state from one run into the next"
    )
    assert all("_run_" in name for name in states), states
