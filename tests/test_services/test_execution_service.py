"""TDD tests for execution service (run lifecycle management)."""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.upload_service import UploadService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def upload_svc(conn_svc):
    return UploadService(conn_svc)


@pytest.fixture
def svc():
    return ExecutionService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-exec-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-exec-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def upload(upload_svc, conn_svc, db_session, org):
    src = conn_svc.create_connection(
        db_session,
        org.id,
        "S",
        ConnectionType.POSTGRES,
        ConnectionDirection.SOURCE,
        {"host": "src"},
    )
    dst = conn_svc.create_connection(
        db_session,
        org.id,
        "D",
        ConnectionType.POSTGRES,
        ConnectionDirection.DESTINATION,
        {"host": "dst"},
    )
    return upload_svc.create_upload(
        db_session,
        org.id,
        "test pipe",
        "desc",
        src.id,
        dst.id,
        {},
    )


class TestCreateRun:
    def test_creates_pending_run(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        assert isinstance(run, Run)
        assert isinstance(run.id, int)
        assert run.status == RunStatus.PENDING
        assert run.org_id == org.id
        assert run.target_type == NodeType.UPLOAD
        assert run.target_id == upload.id
        assert run.started_at is None
        assert run.finished_at is None


class TestStartRun:
    def test_sets_running_and_started_at(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        started = svc.start_run(db_session, run.id)
        assert started is not None
        assert started.status == RunStatus.RUNNING
        assert started.started_at is not None

    def test_nonexistent(self, svc, db_session):
        assert svc.start_run(db_session, 99999) is None


class TestCompleteRun:
    def test_sets_success_and_finished_at(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.start_run(db_session, run.id)
        completed = svc.complete_run(db_session, run.id, rows_loaded=100, logs="OK")
        assert completed is not None
        assert completed.status == RunStatus.SUCCESS
        assert completed.finished_at is not None
        assert completed.rows_loaded == 100
        assert completed.logs == "OK"

    def test_nonexistent(self, svc, db_session):
        assert svc.complete_run(db_session, 99999, 0, "") is None


class TestFailRun:
    def test_sets_failed_and_finished_at(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.start_run(db_session, run.id)
        failed = svc.fail_run(db_session, run.id, "boom", "traceback here")
        assert failed is not None
        assert failed.status == RunStatus.FAILED
        assert failed.finished_at is not None
        assert failed.error_message == "boom"
        assert failed.logs == "traceback here"

    def test_nonexistent(self, svc, db_session):
        assert svc.fail_run(db_session, 99999, "err", "") is None


class TestCancelRun:
    def test_cancel_pending(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        cancelled = svc.cancel_run(db_session, run.id)
        assert cancelled is not None
        assert cancelled.status == RunStatus.CANCELLED

    def test_cancel_running(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.start_run(db_session, run.id)
        cancelled = svc.cancel_run(db_session, run.id)
        assert cancelled is not None
        assert cancelled.status == RunStatus.CANCELLED

    def test_cancel_completed_fails(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.start_run(db_session, run.id)
        svc.complete_run(db_session, run.id, 0, "")
        assert svc.cancel_run(db_session, run.id) is None

    def test_nonexistent(self, svc, db_session):
        assert svc.cancel_run(db_session, 99999) is None


class TestGetRun:
    def test_existing(self, svc, db_session, org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        fetched = svc.get_run(db_session, org.id, run.id)
        assert fetched is not None
        assert fetched.id == run.id

    def test_wrong_org(self, svc, db_session, org, other_org, upload):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        assert svc.get_run(db_session, other_org.id, run.id) is None

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_run(db_session, org.id, 99999) is None


class TestListRuns:
    def test_empty(self, svc, db_session, org):
        result = svc.list_runs(db_session, org.id)
        assert result == []

    def test_multiple(self, svc, db_session, org, upload):
        svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        assert len(svc.list_runs(db_session, org.id)) == 2

    def test_filter_by_status(self, svc, db_session, org, upload):
        r1 = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.start_run(db_session, r1.id)
        result = svc.list_runs(db_session, org.id, status=RunStatus.RUNNING)
        assert len(result) == 1
        assert result[0].id == r1.id

    def test_filter_by_target(self, svc, db_session, org, upload):
        svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.create_run(db_session, org.id, NodeType.TRANSFORMATION, 999)
        result = svc.list_runs(
            db_session,
            org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
        )
        assert len(result) == 1

    def test_limit(self, svc, db_session, org, upload):
        for _ in range(5):
            svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        result = svc.list_runs(db_session, org.id, limit=3)
        assert len(result) == 3

    def test_filters_by_org(self, svc, db_session, org, other_org, upload):
        svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
        svc.create_run(db_session, other_org.id, NodeType.UPLOAD, 1)
        result = svc.list_runs(db_session, org.id)
        assert len(result) == 1
