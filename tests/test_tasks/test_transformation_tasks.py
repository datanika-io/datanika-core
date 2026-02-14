"""TDD tests for transformation Celery tasks (mocked dbt)."""

from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet

from etlfabric.models.dependency import NodeType
from etlfabric.models.run import RunStatus
from etlfabric.models.transformation import Materialization
from etlfabric.models.user import Organization
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.execution_service import ExecutionService
from etlfabric.services.transformation_service import TransformationService
from etlfabric.tasks.transformation_tasks import run_transformation


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def transform_svc():
    return TransformationService()


@pytest.fixture
def exec_svc():
    return ExecutionService()


@pytest.fixture
def setup_transformation(transform_svc, exec_svc, db_session):
    """Create a fresh org, transformation, and pending run per test."""
    import uuid

    slug = f"acme-trans-task-{uuid.uuid4().hex[:8]}"
    org = Organization(name="Acme", slug=slug)
    db_session.add(org)
    db_session.flush()

    transformation = transform_svc.create_transformation(
        db_session,
        org.id,
        "test_model",
        "SELECT 1 AS id",
        Materialization.TABLE,
        schema_name="staging",
    )
    run = exec_svc.create_run(db_session, org.id, NodeType.TRANSFORMATION, transformation.id)
    return org, transformation, run


class TestRunTransformationTask:
    def test_transitions_to_running_then_success(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        run_transformation(
            run_id=run.id,
            org_id=org.id,
            session=db_session,
        )
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.started_at is not None

    def test_completes_with_row_count(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        run_transformation(
            run_id=run.id,
            org_id=org.id,
            session=db_session,
        )
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.finished_at is not None
        assert run.rows_loaded is not None
        assert run.rows_loaded >= 0

    def test_fails_on_error(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        with patch(
            "etlfabric.tasks.transformation_tasks._execute_dbt",
            side_effect=RuntimeError("dbt exploded"),
        ):
            run_transformation(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
            )
        db_session.refresh(run)
        assert run.status == RunStatus.FAILED
        assert run.finished_at is not None
        assert "dbt exploded" in run.error_message

    def test_nonexistent_transformation_fails(self, db_session, exec_svc):
        import uuid

        org = Organization(name="Acme", slug=f"acme-notfound-{uuid.uuid4().hex[:8]}")
        db_session.add(org)
        db_session.flush()

        run = exec_svc.create_run(db_session, org.id, NodeType.TRANSFORMATION, 99999)
        run_transformation(
            run_id=run.id,
            org_id=org.id,
            session=db_session,
        )
        db_session.refresh(run)
        assert run.status == RunStatus.FAILED
        assert "not found" in run.error_message.lower()

    def test_celery_task_exists(self):
        from etlfabric.tasks.transformation_tasks import run_transformation_task

        assert run_transformation_task.name == "etlfabric.run_transformation"
