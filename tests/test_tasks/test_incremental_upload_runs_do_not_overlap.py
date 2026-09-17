"""Two runs of one incremental upload never extract against one cursor at once (core#1404).

SPEC_INCREMENTAL_UPLOADS §2.4. Before core#1404 every run had its own pipeline name, so overlapping
runs could not share a cursor. Once an incremental upload's runs share a name, two runs started
together would both read past the same cursor and both land the same rows. A run of an incremental
upload therefore starts only when no other run of that upload is running, and a refused run says
which run is in progress.

**Why PostgreSQL, in a container.** The guarantee is a database one: a row lock on the upload
serialises every claim, so the second claim is decided after the first commits. SQLite has no row
locks, and its single writer turns two task-shaped sessions into ``database is locked`` rather than
into the behaviour under test (``tests/conftest.py``'s ``production_session_factory`` records that).
So these tests run the real ``run_upload``, each run in its own session from a real PostgreSQL, as
the worker does.
"""

from __future__ import annotations

import sqlite3
import threading
import time

import duckdb
import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import datanika.tasks.upload_tasks  # noqa: F401 - registers every model the upload path touches
from datanika.models.base import Base
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.dlt_runner import DltRunnerService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.upload_tasks import run_upload
from tests.test_services.test_source_builders_move_rows import await_setup, requires_docker

WAIT = 90


class HarnessError(RuntimeError):
    """A setup invariant failed."""


def _schema(container):
    engine = create_engine(container.get_connection_url())
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture(scope="module")
def postgres_engine():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as container:
        engine = await_setup(
            "postgres accepting DDL", lambda: _schema(container), container=container
        )
        try:
            yield engine
        finally:
            engine.dispose()


@pytest.fixture
def sessions(postgres_engine, monkeypatch):
    """Sessions built with production's options, bound to the container: one per actor."""
    from datanika.db import sync_session_factory

    options = {key: value for key, value in sync_session_factory.kw.items() if key != "bind"}
    session_class = sync_session_factory.class_.__mro__[1]
    factory = sessionmaker(bind=postgres_engine, class_=session_class, **options)
    # run_upload opens its own session when it is given none, as a Celery task does.
    monkeypatch.setattr("datanika.db.get_sync_session", factory)
    return factory


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    """dlt's working files and the dbt project go to tmp, not into the checkout."""
    from datanika.config import settings

    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


def _upload(sessions, encryption, tmp_path, *, incremental: bool) -> tuple[int, int, str]:
    source = tmp_path / "source.sqlite"
    con = sqlite3.connect(source)
    con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, updated_at INTEGER NOT NULL)")
    con.executemany("INSERT INTO events VALUES (?, ?)", [(i, i * 10) for i in range(1, 6)])
    con.commit()
    con.close()
    dest = tmp_path / "destination.duckdb"
    dlt_config = {"mode": "single_table", "table": "events", "write_disposition": "append"}
    if incremental:
        dlt_config["incremental"] = {"cursor_path": "updated_at"}
    with sessions() as setup:
        org = Organization(name="Overlap", slug=f"overlap-{tmp_path.name[-20:]}")
        setup.add(org)
        setup.flush()
        src, dst = (
            Connection(
                org_id=org.id,
                name=name,
                connection_type=ctype,
                direction=direction,
                config_encrypted=encryption.encrypt({"path": str(path)}),
            )
            for name, ctype, direction, path in (
                ("events db", ConnectionType.SQLITE, ConnectionDirection.SOURCE, source),
                ("warehouse", ConnectionType.DUCKDB, ConnectionDirection.DESTINATION, dest),
            )
        )
        setup.add_all([src, dst])
        setup.flush()
        upload = Upload(
            org_id=org.id,
            name="events",
            source_connection_id=src.id,
            destination_connection_id=dst.id,
            dlt_config=dlt_config,
            status=UploadStatus.DRAFT,
        )
        setup.add(upload)
        setup.flush()
        runs = [
            ExecutionService().create_run(setup, org.id, NodeType.UPLOAD, upload.id)
            for _ in range(2)
        ]
        setup.commit()
        return org.id, upload.id, str(dest), [r.id for r in runs]


def _run_row(sessions, run_id) -> Run:
    with sessions() as reader:
        return reader.execute(select(Run).where(Run.id == run_id)).scalar_one()


def _destination_ids(dest: str) -> list[int]:
    con = duckdb.connect(dest)
    try:
        schema = con.execute(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name = 'events' AND table_schema NOT LIKE '%_staging'"
        ).fetchone()[0]
        return [
            r[0] for r in con.execute(f'SELECT id FROM "{schema}"."events" ORDER BY id').fetchall()
        ]
    finally:
        con.close()


def _hold_run_in_its_engine(monkeypatch, run_id: int):
    """Pause ``run_id`` inside the loader, after it has started and before it reads a row."""
    inside, release = threading.Event(), threading.Event()
    real = DltRunnerService.execute

    def _paused(self, *args, **kwargs):
        if kwargs.get("run_id") == run_id:
            inside.set()
            if not release.wait(WAIT):
                raise HarnessError(f"run {run_id} was never released")
        return real(self, *args, **kwargs)

    monkeypatch.setattr(DltRunnerService, "execute", _paused)
    return inside, release


@requires_docker
class TestOverlappingRuns:
    def test_a_second_run_is_refused_while_the_first_runs_and_names_it(
        self, sessions, encryption, tmp_path, monkeypatch
    ):
        org_id, _upload_id, dest, (first, second) = _upload(
            sessions, encryption, tmp_path, incremental=True
        )
        inside, release = _hold_run_in_its_engine(monkeypatch, first)
        worker = threading.Thread(
            target=run_upload, args=(first, org_id), kwargs={"encryption": encryption}
        )
        worker.start()
        try:
            if not inside.wait(WAIT):
                raise HarnessError("the first run never reached its engine")
            run_upload(second, org_id, encryption=encryption)
        finally:
            release.set()
            worker.join(WAIT)

        refused, completed = _run_row(sessions, second), _run_row(sessions, first)
        assert refused.status == RunStatus.FAILED, refused.status
        assert f"run {first}" in (refused.error_message or ""), refused.error_message
        assert completed.status == RunStatus.SUCCESS, completed.error_message
        assert _destination_ids(dest) == [1, 2, 3, 4, 5], "a row landed twice, or not at all"

    def test_control_an_upload_without_a_cursor_is_not_serialised(
        self, sessions, encryption, tmp_path, monkeypatch
    ):
        """SPEC §3: runs of an upload with no cursor share no state, and keep running as before."""
        org_id, _upload_id, _dest, (first, second) = _upload(
            sessions, encryption, tmp_path, incremental=False
        )
        inside, release = _hold_run_in_its_engine(monkeypatch, first)
        worker = threading.Thread(
            target=run_upload, args=(first, org_id), kwargs={"encryption": encryption}
        )
        worker.start()
        try:
            if not inside.wait(WAIT):
                raise HarnessError("the first run never reached its engine")
            run_upload(second, org_id, encryption=encryption)
        finally:
            release.set()
            worker.join(WAIT)

        assert _run_row(sessions, second).status == RunStatus.SUCCESS
        assert _run_row(sessions, first).status == RunStatus.SUCCESS

    def test_a_claim_waits_for_a_concurrent_claim_to_commit(self, sessions, encryption, tmp_path):
        """The database half of §2.4, deterministic rather than timed.

        One session claims the upload and does not commit. A second claim must not decide until the
        first commits, and must then see the first run as running. Without the row lock it decides
        at once, reads no running run, and both runs start.
        """
        org_id, upload_id, _dest, (first, second) = _upload(
            sessions, encryption, tmp_path, incremental=True
        )
        service = ExecutionService()
        holder, contender = sessions(), sessions()
        decided: dict = {}
        try:
            assert service.start_exclusive_upload_run(holder, org_id, first, upload_id) is None

            def _contend():
                decided["busy"] = service.start_exclusive_upload_run(
                    contender, org_id, second, upload_id
                )
                decided["at"] = time.monotonic()
                contender.commit()

            thread = threading.Thread(target=_contend)
            thread.start()
            time.sleep(2.0)
            still_waiting = thread.is_alive() and "at" not in decided
            committed_at = time.monotonic()
            holder.commit()
            thread.join(WAIT)
        finally:
            holder.close()
            contender.close()

        assert still_waiting, "the second claim decided while the first was uncommitted"
        assert decided.get("busy") == first, decided
        assert decided["at"] >= committed_at
        assert _run_row(sessions, second).status == RunStatus.PENDING
