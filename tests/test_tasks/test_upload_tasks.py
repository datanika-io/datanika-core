"""TDD tests for upload Celery tasks (mocked dlt)."""

from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.upload import UploadStatus
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.upload_service import UploadService
from datanika.tasks.upload_tasks import run_upload


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
def exec_svc():
    return ExecutionService()


@pytest.fixture
def setup_upload(upload_svc, conn_svc, exec_svc, db_session, encryption):
    """Create a fresh org, upload, and pending run per test (no commit needed)."""
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
    upload = upload_svc.create_upload(
        db_session,
        org.id,
        "test",
        "desc",
        src.id,
        dst.id,
        {"write_disposition": "append"},
    )
    run = exec_svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    return org, upload, run, encryption


def _mock_dlt_runner():
    """Return a patch context that mocks DltRunnerService.execute for upload task tests."""
    return patch("datanika.tasks.upload_tasks.DltRunnerService")


class TestRunUploadTask:
    def test_transitions_to_running(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        with _mock_dlt_runner() as mock_runner_cls:
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 42,
                "load_info": "mock_load_info",
            }

            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.started_at is not None

    def test_completes_on_success(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        with _mock_dlt_runner() as mock_runner_cls:
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 100,
                "load_info": "mock_load_info",
            }

            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.finished_at is not None
        assert run.rows_loaded == 100

    def test_fails_on_error(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        with _mock_dlt_runner() as mock_runner_cls:
            instance = mock_runner_cls.return_value
            instance.execute.side_effect = RuntimeError("dlt exploded")

            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.FAILED
        assert run.finished_at is not None
        assert "dlt exploded" in run.error_message

    def test_upload_status_active_on_success(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        assert upload.status == UploadStatus.DRAFT

        with _mock_dlt_runner() as mock_runner_cls:
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 42,
                "load_info": "mock_load_info",
            }
            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(upload)
        assert upload.status == UploadStatus.ACTIVE

    def test_upload_status_error_on_failure(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        assert upload.status == UploadStatus.DRAFT

        with _mock_dlt_runner() as mock_runner_cls:
            instance = mock_runner_cls.return_value
            instance.execute.side_effect = RuntimeError("dlt exploded")
            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(upload)
        assert upload.status == UploadStatus.ERROR

    def test_passes_dataset_name_derived_from_upload_name(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        with _mock_dlt_runner() as mock_runner_cls:
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 10,
                "load_info": "mock_load_info",
            }

            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

            call_kwargs = instance.execute.call_args[1]
            assert call_kwargs["dataset_name"] == "test"

    def test_dataset_name_converts_spaces_to_underscores(
        self, upload_svc, conn_svc, exec_svc, db_session, encryption
    ):
        import uuid

        slug = f"acme-ds-{uuid.uuid4().hex[:8]}"
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
        upload = upload_svc.create_upload(
            db_session,
            org.id,
            "My Sales Pipeline",
            "desc",
            src.id,
            dst.id,
            {"write_disposition": "append"},
        )
        run = exec_svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)

        with _mock_dlt_runner() as mock_runner_cls:
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 10,
                "load_info": "mock_load_info",
            }
            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )
            call_kwargs = instance.execute.call_args[1]
            assert call_kwargs["dataset_name"] == "my_sales_pipeline"


class TestCatalogSyncAfterUpload:
    def _run_with_catalog_mocks(self, db_session, setup_upload, introspect_result=None):
        """Run upload with mocked DLT + mocked catalog introspection."""
        org, upload, run, encryption = setup_upload
        if introspect_result is None:
            introspect_result = [
                {"table_name": "users", "columns": [{"name": "id", "data_type": "INTEGER"}]},
            ]

        mock_dbt_instance = MagicMock()

        with (
            _mock_dlt_runner() as mock_runner_cls,
            patch.object(
                CatalogService,
                "introspect_tables",
                return_value=introspect_result,
            ) as mock_introspect,
            patch(
                "datanika.tasks.upload_tasks.DbtProjectService",
                return_value=mock_dbt_instance,
            ),
        ):
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 10,
                "load_info": "mock_load_info",
            }
            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )
        return org, upload, run, mock_introspect, mock_dbt_instance

    def test_syncs_catalog_entries_after_success(self, db_session, setup_upload):
        org, upload, run, mock_introspect, _ = self._run_with_catalog_mocks(
            db_session,
            setup_upload,
        )
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        mock_introspect.assert_called_once()
        # Verify entries were persisted
        entries = CatalogService.list_entries(db_session, org.id)
        assert len(entries) == 1
        assert entries[0].table_name == "users"

    def test_source_yml_written_after_success(self, db_session, setup_upload):
        _, _, _, _, mock_dbt_instance = self._run_with_catalog_mocks(
            db_session,
            setup_upload,
        )
        mock_dbt_instance.write_source_yml_for_connection.assert_called_once()

    def test_catalog_sync_failure_does_not_fail_run(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        with (
            _mock_dlt_runner() as mock_runner_cls,
            patch.object(
                CatalogService,
                "introspect_tables",
                side_effect=RuntimeError("introspect failed"),
            ),
        ):
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 5,
                "load_info": "ok",
            }
            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.rows_loaded == 5
