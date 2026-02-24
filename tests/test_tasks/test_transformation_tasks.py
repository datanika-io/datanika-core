"""TDD tests for transformation Celery tasks."""

from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.transformation import Materialization
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.transformation_service import TransformationService
from datanika.tasks.transformation_tasks import (
    _sync_catalog_after_transformation,
    run_transformation,
)


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


def _mock_dbt_project():
    """Return a patch context that mocks DbtProjectService for transformation task tests."""
    return patch("datanika.tasks.transformation_tasks.DbtProjectService")


class TestRunTransformationTask:
    def test_transitions_to_running_then_success(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_model.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
            }
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
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_model.return_value = {
                "success": True,
                "rows_affected": 42,
                "logs": "ok",
            }
            run_transformation(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
            )
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.finished_at is not None
        assert run.rows_loaded == 42

    def test_fails_on_error(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_model.side_effect = RuntimeError("dbt exploded")
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
        from datanika.tasks.transformation_tasks import run_transformation_task

        assert run_transformation_task.name == "datanika.run_transformation"


class TestCatalogSyncAfterTransformation:
    def test_syncs_catalog_after_success(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_model.return_value = {
                "success": True,
                "rows_affected": 10,
                "logs": "ok",
            }
            run_transformation(run_id=run.id, org_id=org.id, session=db_session)
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        entries = CatalogService.list_entries(db_session, org.id)
        assert len(entries) == 1
        assert entries[0].table_name == "test_model"
        assert entries[0].origin_type == NodeType.TRANSFORMATION

    def test_writes_model_yml_after_success(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_model.return_value = {
                "success": True,
                "rows_affected": 5,
                "logs": "",
            }
            run_transformation(run_id=run.id, org_id=org.id, session=db_session)
        instance.write_model_yml.assert_called_once()

    def test_catalog_sync_failure_does_not_fail_run(self, db_session, setup_transformation):
        org, transformation, run = setup_transformation
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_model.return_value = {
                "success": True,
                "rows_affected": 3,
                "logs": "",
            }
            instance.write_model_yml.side_effect = RuntimeError("yml write failed")
            run_transformation(run_id=run.id, org_id=org.id, session=db_session)
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.rows_loaded == 3

    def test_transformation_catalog_sync_introspects_columns(
        self, db_session, encryption
    ):
        """_sync_catalog_after_transformation introspects columns from the DB
        when dst_conn and dst_config are provided."""
        import uuid

        slug = f"acme-intro-{uuid.uuid4().hex[:8]}"
        org = Organization(name="Acme", slug=slug)
        db_session.add(org)
        db_session.flush()

        dst_config = {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"}
        conn = Connection(
            org_id=org.id,
            name="pg_dest",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted=encryption.encrypt(dst_config),
        )
        db_session.add(conn)
        db_session.flush()

        transform_svc = TransformationService()
        transformation = transform_svc.create_transformation(
            db_session,
            org.id,
            "test_introspect",
            "SELECT 1 AS id",
            Materialization.TABLE,
            schema_name="staging",
            destination_connection_id=conn.id,
        )

        introspected_columns = [
            {"name": "id", "data_type": "INTEGER"},
            {"name": "name", "data_type": "VARCHAR"},
        ]

        mock_dbt_svc = MagicMock()

        with (
            patch(
                "datanika.tasks.transformation_tasks.CatalogService.introspect_tables"
            ) as mock_introspect,
            patch(
                "datanika.tasks.transformation_tasks._build_sa_url",
                return_value="postgresql+psycopg2://u:p@h:5432/d",
            ),
        ):
            mock_introspect.return_value = [
                {"table_name": "test_introspect", "columns": introspected_columns}
            ]
            _sync_catalog_after_transformation(
                db_session, org.id, transformation, mock_dbt_svc, conn, dst_config
            )

        entries = CatalogService.list_entries(db_session, org.id)
        assert len(entries) == 1
        assert entries[0].columns == introspected_columns
