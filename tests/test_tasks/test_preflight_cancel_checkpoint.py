"""The pre-flight checkpoint: a run cancelled before its engine starts does no work (core#657).

``SPEC_RUN_CANCELLATION`` §7 **2a** — *"Read `is_cancelled` immediately before invoking the engine.
Covers `pending → cancelled` and the window between dequeue and first byte."* ⚠️ **This is 2a, not
2b.** A run already inside ``pipeline.run()`` or ``dbtRunner().invoke()`` is not stopped by
anything here; both are single opaque calls, and §6 rules revocation out.

The witness is §7.2's own discriminator: **a pre-flight stop leaves the destination with NOTHING**.
So the upload arm loads a real CSV into a real DuckDB file and reads the destination back — never
the status field, which is the thing under suspicion. The dbt arms assert the engine call itself
was never made, with a control that it is made when nothing was cancelled.

Also pinned, because each is a way a skipped run could still lie:
* the quota gate is not consulted (a quota refusal would mark the upload ERROR for a run that was
  never going to run);
* nothing is announced (``run.*_completed`` meters, and a skipped run processed nothing);
* the run records ``rows_loaded = 0`` and says why — measured, not unknown;
* the upload/pipeline row keeps its status.
"""

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
from cryptography.fernet import Fernet
from sqlalchemy import update

from datanika import hooks
from datanika.config import settings
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.pipeline import Pipeline, PipelineStatus
from datanika.models.run import Run, RunStatus
from datanika.models.transformation import Transformation
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.pipeline_tasks import run_pipeline
from datanika.tasks.transformation_tasks import run_transformation
from datanika.tasks.upload_tasks import run_upload

svc = ExecutionService()


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    """Keep dlt's working files and the dbt project out of the worktree."""
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


@pytest.fixture
def events():
    """Every hook this module cares about, recorded."""
    seen: list[str] = []
    names = (
        "run.before_execute",
        "run.upload_completed",
        "run.models_completed",
        "run.transformation_completed",
        "run.failed",
    )
    recorders = {name: (lambda n: lambda **_kw: seen.append(n))(name) for name in names}
    for name, fn in recorders.items():
        hooks.on(name, fn)
    try:
        yield seen
    finally:
        for name, fn in recorders.items():
            hooks.off(name, fn)


@pytest.fixture
def org(db_session):
    o = Organization(name="Acme", slug="acme-preflight")
    db_session.add(o)
    db_session.flush()
    return o


def _connection(db_session, encryption, org, name, ctype, direction, config):
    conn = Connection(
        org_id=org.id,
        name=name,
        connection_type=ctype,
        direction=direction,
        config_encrypted=encryption.encrypt(config),
    )
    db_session.add(conn)
    db_session.flush()
    return conn


@pytest.fixture
def csv_upload(db_session, encryption, org, tmp_path):
    drop = tmp_path / "drop"
    drop.mkdir()
    (drop / "widgets.csv").write_text("id,name\n1,alpha\n2,beta\n3,gamma\n", encoding="utf-8")
    src = _connection(
        db_session,
        encryption,
        org,
        "files",
        ConnectionType.CSV,
        ConnectionDirection.SOURCE,
        {"bucket_url": str(drop)},
    )
    dst = _connection(
        db_session,
        encryption,
        org,
        "warehouse",
        ConnectionType.DUCKDB,
        ConnectionDirection.DESTINATION,
        {"path": str(tmp_path / "dest.duckdb")},
    )
    upload = Upload(
        org_id=org.id,
        name="probe",
        source_connection_id=src.id,
        destination_connection_id=dst.id,
        dlt_config={"file_glob": "widgets.csv"},
        status=UploadStatus.DRAFT,
    )
    db_session.add(upload)
    db_session.flush()
    run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    return upload, run, tmp_path / "dest.duckdb"


def _rows_in_destination(path: Path) -> int:
    """Rows the destination actually holds. A file that was never created holds none."""
    if not path.exists():
        return 0
    con = duckdb.connect(str(path))
    try:
        tables = con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_name = 'widgets'"
        ).fetchall()
        return sum(
            con.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0]
            for schema, table in tables
        )
    finally:
        con.close()


def _cancel(db_session, org, run):
    assert svc.cancel_run(db_session, org.id, run.id) is not None
    db_session.flush()


# ---------------------------------------------------------------------------
# Upload — against a real destination
# ---------------------------------------------------------------------------


def test_control_an_uncancelled_upload_loads_the_file(
    db_session, encryption, org, csv_upload, events
):
    """Without this, "the destination holds nothing" is also what a broken harness produces."""
    upload, run, dest = csv_upload

    run_upload(run.id, org.id, session=db_session, encryption=encryption)

    db_session.refresh(run)
    assert run.status == RunStatus.SUCCESS, run.error_message
    assert _rows_in_destination(dest) == 3
    assert "run.before_execute" in events and "run.upload_completed" in events


def test_an_upload_cancelled_while_pending_loads_nothing(
    db_session, encryption, org, csv_upload, events
):
    """Red before core#657 2a: the task started the cancelled run and loaded all 3 rows."""
    upload, run, dest = csv_upload
    _cancel(db_session, org, run)

    run_upload(run.id, org.id, session=db_session, encryption=encryption)

    assert _rows_in_destination(dest) == 0, (
        "a run cancelled before it started wrote to the warehouse"
    )
    db_session.refresh(run)
    db_session.refresh(upload)
    assert run.status == RunStatus.CANCELLED
    assert run.started_at is None, "a run that never started was given a start time"
    assert run.rows_loaded == 0, "nothing was loaded, and that was measured — not unknown"
    assert "before it started" in (run.logs or "")
    assert upload.status == UploadStatus.DRAFT, "a skipped run changed its upload's status"
    assert events == [], f"a skipped run consulted or announced hooks: {events}"


def test_a_cancel_landing_after_start_stops_before_the_engine(
    db_session, encryption, org, csv_upload, events
):
    """The second checkpoint — the window between dequeue and first byte.

    The cancel is written while the task decrypts its connection configs, after ``start_run``
    committed RUNNING and before ``runner.execute`` is reached.
    """
    upload, run, dest = csv_upload
    real_decrypt = encryption.decrypt

    def decrypt_then_cancel(value):
        db_session.execute(update(Run).where(Run.id == run.id).values(status=RunStatus.CANCELLED))
        return real_decrypt(value)

    with patch.object(encryption, "decrypt", side_effect=decrypt_then_cancel):
        run_upload(run.id, org.id, session=db_session, encryption=encryption)

    assert _rows_in_destination(dest) == 0
    db_session.refresh(run)
    assert run.status == RunStatus.CANCELLED
    assert run.started_at is not None, "harness: this run should have started first"
    assert "run.upload_completed" not in events


# ---------------------------------------------------------------------------
# dbt — the engine call itself
# ---------------------------------------------------------------------------


@pytest.fixture
def warehouse(db_session, encryption, org, tmp_path):
    return _connection(
        db_session,
        encryption,
        org,
        "warehouse",
        ConnectionType.DUCKDB,
        ConnectionDirection.DESTINATION,
        {"path": str(tmp_path / "dest.duckdb")},
    )


@pytest.fixture
def pipeline_run(db_session, org, warehouse):
    pipeline = Pipeline(
        org_id=org.id,
        name="nightly",
        destination_connection_id=warehouse.id,
        status=PipelineStatus.DRAFT,
    )
    db_session.add(pipeline)
    db_session.flush()
    return pipeline, svc.create_run(db_session, org.id, NodeType.PIPELINE, pipeline.id)


@pytest.fixture
def transformation_run(db_session, org):
    # No destination connection: `run_transformation` decrypts one with the APP key, and dbt is
    # replaced below anyway, so a connection would only add a decryption this test is not about.
    transformation = Transformation(
        org_id=org.id,
        name="orders_clean",
        sql_body="select 1 as id",
    )
    db_session.add(transformation)
    db_session.flush()
    return transformation, svc.create_run(
        db_session, org.id, NodeType.TRANSFORMATION, transformation.id
    )


def _dbt_ok():
    return {"success": True, "rows_affected": 1, "logs": "ok", "raw_result": []}


@pytest.mark.parametrize("cancelled", [False, True], ids=["control", "cancelled"])
def test_a_cancelled_pipeline_never_invokes_dbt(
    db_session, encryption, org, pipeline_run, events, cancelled
):
    pipeline, run = pipeline_run
    if cancelled:
        _cancel(db_session, org, run)

    with patch("datanika.tasks.pipeline_tasks.DbtProjectService") as dbt:
        dbt.return_value.run_command.return_value = _dbt_ok()
        run_pipeline(run.id, org.id, session=db_session, encryption=encryption)

    db_session.refresh(run)
    if cancelled:
        dbt.return_value.run_command.assert_not_called()
        assert run.status == RunStatus.CANCELLED and run.started_at is None
        assert events == []
    else:
        dbt.return_value.run_command.assert_called_once()
        assert run.status == RunStatus.SUCCESS


@pytest.mark.parametrize("cancelled", [False, True], ids=["control", "cancelled"])
def test_a_cancelled_transformation_never_invokes_dbt(
    db_session, org, transformation_run, events, cancelled
):
    transformation, run = transformation_run
    if cancelled:
        _cancel(db_session, org, run)

    with patch("datanika.tasks.transformation_tasks.DbtProjectService") as dbt:
        dbt.return_value.run_model.return_value = _dbt_ok()
        run_transformation(run.id, org.id, session=db_session)

    db_session.refresh(run)
    if cancelled:
        dbt.return_value.run_model.assert_not_called()
        assert run.status == RunStatus.CANCELLED and run.started_at is None
        assert events == []
    else:
        dbt.return_value.run_model.assert_called_once()
        assert run.status == RunStatus.SUCCESS
