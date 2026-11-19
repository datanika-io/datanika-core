"""The mid-flight stop (SPEC_RUN_CANCELLATION §7 2b, core#657, core#1354).

2a leaves the destination empty: the engine never starts. 2b is a cancel of work
already inside the engine. The destination then holds a partial load, ``0 < rows < total``,
and ``rows_loaded`` is that count. A pre-flight check renamed as this test is the defect.

The file is four rows and the reader is forced to one row per chunk, so a stop has a
between-chunk moment. The cancel is the real ``cancel_run`` on the run row, written
before the second chunk is handed to the engine. Nothing here calls
``app.control.revoke``. Partial rows are not rolled back.
"""

from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
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
from tests.factories import make_org_admin

svc = ExecutionService()
TOTAL_ROWS = 4


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


@pytest.fixture
def org(db_session):
    o = Organization(name="Acme", slug="acme-midflight")
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
    lines = ["id,name", "1,alpha", "2,beta", "3,gamma", "4,delta"]
    (drop / "widgets.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    src = _connection(
        db_session,
        encryption,
        org,
        "files",
        ConnectionType.CSV,
        ConnectionDirection.SOURCE,
        {"bucket_url": str(drop)},
    )
    dest = tmp_path / "dest.duckdb"
    dst = _connection(
        db_session,
        encryption,
        org,
        "warehouse",
        ConnectionType.DUCKDB,
        ConnectionDirection.DESTINATION,
        {"path": str(dest)},
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
    return upload, run, dest


def _data_rows(path: Path) -> int:
    """Rows in destination tables that are not dlt bookkeeping."""
    if not path.exists():
        return 0
    con = duckdb.connect(str(path))
    try:
        tables = con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
        ).fetchall()
        total = 0
        for schema, table in tables:
            if table.startswith("_dlt"):
                continue
            total += con.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0]
        return total
    finally:
        con.close()


def _one_row_chunks(real_read_csv, on_second_chunk):
    """Force one-row pandas chunks. ``on_second_chunk`` runs before chunk 2 is yielded."""

    def read_csv(*args, **kwargs):
        if "chunksize" not in kwargs:
            return real_read_csv(*args, **kwargs)
        kwargs["chunksize"] = 1
        reader = real_read_csv(*args, **kwargs)
        seen = {"n": 0}

        def chunks():
            for frame in reader:
                seen["n"] += 1
                if seen["n"] == 2:
                    on_second_chunk()
                yield frame

        return chunks()

    return read_csv


def test_control_four_rows_load_when_nothing_is_cancelled(db_session, encryption, org, csv_upload):
    """The chunk patch itself must not drop rows. Without it, a short count proves nothing."""
    _upload, run, dest = csv_upload

    with patch.object(pd, "read_csv", _one_row_chunks(pd.read_csv, lambda: None)):
        run_upload(run.id, org.id, session=db_session, encryption=encryption)

    db_session.refresh(run)
    assert run.status == RunStatus.SUCCESS, run.error_message
    assert _data_rows(dest) == TOTAL_ROWS


def test_a_cancel_after_the_first_chunk_leaves_a_partial_load(
    db_session, encryption, org, csv_upload
):
    """Red before 2b: cancel lands inside the engine and every source row still arrives."""
    _upload, run, dest = csv_upload
    actor_id = make_org_admin(db_session, org.id)

    def cancel_the_running_load():
        assert svc.cancel_run(db_session, org.id, run.id, actor_user_id=actor_id) is not None

    with patch.object(pd, "read_csv", _one_row_chunks(pd.read_csv, cancel_the_running_load)):
        run_upload(run.id, org.id, session=db_session, encryption=encryption)

    rows = _data_rows(dest)
    # One-row chunks, and the cancel is visible before chunk 2 is handed over.
    # The first row stays. The rest never land. 0 or 4 is the wrong stop.
    assert rows == 1, rows
    assert 0 < rows < TOTAL_ROWS
    db_session.refresh(run)
    assert run.status == RunStatus.CANCELLED
    assert run.rows_loaded == rows
    assert run.rows_loaded not in (0, None, TOTAL_ROWS)
