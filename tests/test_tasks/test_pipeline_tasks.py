"""TDD tests for pipeline Celery tasks (mocked dlt)."""

from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from etlfabric.models.connection import ConnectionDirection, ConnectionType
from etlfabric.models.dependency import NodeType
from etlfabric.models.run import RunStatus
from etlfabric.models.user import Organization
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.execution_service import ExecutionService
from etlfabric.services.pipeline_service import PipelineService
from etlfabric.tasks.pipeline_tasks import run_pipeline


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def pipe_svc(conn_svc):
    return PipelineService(conn_svc)


@pytest.fixture
def exec_svc():
    return ExecutionService()


@pytest.fixture
def setup_pipeline(pipe_svc, conn_svc, exec_svc, db_session, encryption):
    """Create a fresh org, pipeline, and pending run per test (no commit needed)."""
    # Use a unique slug per call to avoid collisions
    import uuid

    slug = f"acme-task-{uuid.uuid4().hex[:8]}"
    org = Organization(name="Acme", slug=slug)
    db_session.add(org)
    db_session.flush()

    src = conn_svc.create_connection(
        db_session,
        org.id,
        "S",
        ConnectionType.POSTGRES,
        ConnectionDirection.SOURCE,
        {"host": "src", "port": 5432},
    )
    dst = conn_svc.create_connection(
        db_session,
        org.id,
        "D",
        ConnectionType.POSTGRES,
        ConnectionDirection.DESTINATION,
        {"host": "dst", "port": 5432},
    )
    pipeline = pipe_svc.create_pipeline(
        db_session,
        org.id,
        "test",
        "desc",
        src.id,
        dst.id,
        {"write_disposition": "append"},
    )
    run = exec_svc.create_run(db_session, org.id, NodeType.PIPELINE, pipeline.id)
    return org, pipeline, run, encryption


class TestRunPipelineTask:
    def test_transitions_to_running(self, db_session, setup_pipeline):
        org, pipeline, run, encryption = setup_pipeline
        with patch("etlfabric.tasks.pipeline_tasks.dlt") as mock_dlt:
            mock_pipeline = MagicMock()
            mock_pipeline.run.return_value = MagicMock(
                loads_count=42, asdict=MagicMock(return_value={})
            )
            mock_dlt.pipeline.return_value = mock_pipeline

            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.started_at is not None

    def test_completes_on_success(self, db_session, setup_pipeline):
        org, pipeline, run, encryption = setup_pipeline
        with patch("etlfabric.tasks.pipeline_tasks.dlt") as mock_dlt:
            mock_pipeline = MagicMock()
            mock_pipeline.run.return_value = MagicMock(
                loads_count=100, asdict=MagicMock(return_value={})
            )
            mock_dlt.pipeline.return_value = mock_pipeline

            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.finished_at is not None
        assert run.rows_loaded == 100

    def test_fails_on_error(self, db_session, setup_pipeline):
        org, pipeline, run, encryption = setup_pipeline
        with patch("etlfabric.tasks.pipeline_tasks.dlt") as mock_dlt:
            mock_pipeline = MagicMock()
            mock_pipeline.run.side_effect = RuntimeError("dlt exploded")
            mock_dlt.pipeline.return_value = mock_pipeline

            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.FAILED
        assert run.finished_at is not None
        assert "dlt exploded" in run.error_message
