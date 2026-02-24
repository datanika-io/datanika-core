"""Integration tests verifying hook emissions from tasks and services."""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika import hooks
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.pipeline import DbtCommand, Pipeline
from datanika.models.transformation import Materialization, Transformation
from datanika.models.user import MemberRole, Organization, User
from datanika.services.auth import AuthService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.services.user_service import UserService


@pytest.fixture(autouse=True)
def _clean_hooks():
    hooks.clear()
    yield
    hooks.clear()


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


# ---------------------------------------------------------------------------
# Pipeline: run.models_completed
# ---------------------------------------------------------------------------


class TestPipelineHookEmission:
    @pytest.fixture
    def setup_pipeline(self, db_session, encryption, exec_svc):
        slug = f"acme-hook-pipe-{uuid.uuid4().hex[:8]}"
        org = Organization(name="Acme", slug=slug)
        db_session.add(org)
        db_session.flush()

        conn = Connection(
            org_id=org.id,
            name="pg_dest",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted=encryption.encrypt(
                {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"}
            ),
        )
        db_session.add(conn)
        db_session.flush()

        pipeline = Pipeline(
            org_id=org.id,
            name="hook_pipeline",
            destination_connection_id=conn.id,
            command=DbtCommand.RUN,
        )
        db_session.add(pipeline)
        db_session.flush()

        run = exec_svc.create_run(db_session, org.id, NodeType.PIPELINE, pipeline.id)
        return org, conn, pipeline, run

    def _make_node(self, name, resource_type="model", status="success"):
        node = MagicMock()
        node.node.name = name
        node.node.schema = "staging"
        node.node.resource_type.value = resource_type
        node.node.config.materialized = "view"
        node.status.value = status
        return node

    def test_emits_models_completed_with_correct_count(
        self, db_session, encryption, setup_pipeline
    ):
        org, conn, pipeline, run = setup_pipeline
        spy = MagicMock()
        hooks.on("run.models_completed", spy)

        raw_result = [
            self._make_node("m1", "model", "success"),
            self._make_node("m2", "model", "success"),
            self._make_node("m3", "model", "success"),
            self._make_node("t1", "test", "success"),
            self._make_node("t2", "test", "success"),
        ]

        from datanika.tasks.pipeline_tasks import run_pipeline

        with patch("datanika.tasks.pipeline_tasks.DbtProjectService") as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": raw_result,
            }
            run_pipeline(run_id=run.id, org_id=org.id, session=db_session, encryption=encryption)

        spy.assert_called_once_with(org_id=org.id, count=5)

    def test_only_successful_nodes_counted(self, db_session, encryption, setup_pipeline):
        org, conn, pipeline, run = setup_pipeline
        spy = MagicMock()
        hooks.on("run.models_completed", spy)

        raw_result = [
            self._make_node("m1", "model", "success"),
            self._make_node("m2", "model", "error"),
            self._make_node("t1", "test", "fail"),
            self._make_node("t2", "test", "success"),
        ]

        from datanika.tasks.pipeline_tasks import run_pipeline

        with patch("datanika.tasks.pipeline_tasks.DbtProjectService") as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": raw_result,
            }
            run_pipeline(run_id=run.id, org_id=org.id, session=db_session, encryption=encryption)

        spy.assert_called_once_with(org_id=org.id, count=2)

    def test_no_emission_when_zero_billable(self, db_session, encryption, setup_pipeline):
        org, conn, pipeline, run = setup_pipeline
        spy = MagicMock()
        hooks.on("run.models_completed", spy)

        from datanika.tasks.pipeline_tasks import run_pipeline

        with patch("datanika.tasks.pipeline_tasks.DbtProjectService") as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [],
            }
            run_pipeline(run_id=run.id, org_id=org.id, session=db_session, encryption=encryption)

        spy.assert_not_called()


# ---------------------------------------------------------------------------
# Upload: run.upload_completed
# ---------------------------------------------------------------------------


class TestUploadHookEmission:
    @pytest.fixture
    def setup_upload(self, db_session, upload_svc, conn_svc, exec_svc, encryption):
        slug = f"acme-hook-upl-{uuid.uuid4().hex[:8]}"
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

    def test_emits_upload_completed_with_table_count(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        spy = MagicMock()
        hooks.on("run.upload_completed", spy)

        introspect_result = [
            {"table_name": "users", "columns": [{"name": "id", "data_type": "INTEGER"}]},
            {"table_name": "orders", "columns": [{"name": "id", "data_type": "INTEGER"}]},
            {"table_name": "items", "columns": [{"name": "id", "data_type": "INTEGER"}]},
        ]

        mock_dbt_instance = MagicMock()
        from datanika.services.catalog_service import CatalogService
        from datanika.tasks.upload_tasks import run_upload

        with (
            patch("datanika.tasks.upload_tasks.DltRunnerService") as mock_runner_cls,
            patch.object(
                CatalogService,
                "introspect_tables",
                return_value=introspect_result,
            ),
            patch(
                "datanika.tasks.upload_tasks.DbtProjectService",
                return_value=mock_dbt_instance,
            ),
        ):
            instance = mock_runner_cls.return_value
            instance.execute.return_value = {
                "rows_loaded": 30,
                "load_info": "mock_load_info",
            }
            run_upload(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        spy.assert_called_once_with(org_id=org.id, table_count=3)

    def test_emits_fallback_count_on_catalog_failure(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        spy = MagicMock()
        hooks.on("run.upload_completed", spy)

        from datanika.services.catalog_service import CatalogService
        from datanika.tasks.upload_tasks import run_upload

        with (
            patch("datanika.tasks.upload_tasks.DltRunnerService") as mock_runner_cls,
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

        spy.assert_called_once_with(org_id=org.id, table_count=1)


# ---------------------------------------------------------------------------
# Transformation: run.transformation_completed
# ---------------------------------------------------------------------------


class TestTransformationHookEmission:
    @pytest.fixture
    def setup_transformation(self, db_session, exec_svc):
        slug = f"acme-hook-tx-{uuid.uuid4().hex[:8]}"
        org = Organization(name="Acme", slug=slug)
        db_session.add(org)
        db_session.flush()

        t = Transformation(
            org_id=org.id,
            name="my_model",
            description="desc",
            sql_body="SELECT 1",
            materialization=Materialization.VIEW,
            schema_name="staging",
        )
        db_session.add(t)
        db_session.flush()

        run = exec_svc.create_run(db_session, org.id, NodeType.TRANSFORMATION, t.id)
        return org, t, run

    def test_emits_transformation_completed(self, db_session, setup_transformation):
        org, t, run = setup_transformation
        spy = MagicMock()
        hooks.on("run.transformation_completed", spy)

        from datanika.tasks.transformation_tasks import run_transformation

        with patch("datanika.tasks.transformation_tasks.DbtProjectService") as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_model.return_value = {"rows_affected": 10, "logs": "ok"}
            run_transformation(run_id=run.id, org_id=org.id, session=db_session)

        spy.assert_called_once_with(org_id=org.id)


# ---------------------------------------------------------------------------
# connection.before_create — quota check
# ---------------------------------------------------------------------------


class TestConnectionBeforeCreateHook:
    def test_handler_raising_blocks_create(self, db_session, conn_svc):
        slug = f"acme-hook-conn-{uuid.uuid4().hex[:8]}"
        org = Organization(name="Acme", slug=slug)
        db_session.add(org)
        db_session.flush()

        def quota_check(**kw):
            raise ValueError("Connection limit reached")

        hooks.on("connection.before_create", quota_check)

        with pytest.raises(ValueError, match="Connection limit reached"):
            conn_svc.create_connection(
                db_session,
                org.id,
                "My DB",
                ConnectionType.POSTGRES,
                ConnectionDirection.SOURCE,
                {"host": "localhost"},
            )

    def test_no_handler_allows_create(self, db_session, conn_svc):
        slug = f"acme-hook-conn2-{uuid.uuid4().hex[:8]}"
        org = Organization(name="Acme", slug=slug)
        db_session.add(org)
        db_session.flush()

        conn = conn_svc.create_connection(
            db_session,
            org.id,
            "My DB",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "localhost"},
        )
        assert conn.id is not None


# ---------------------------------------------------------------------------
# schedule.before_create — quota check
# ---------------------------------------------------------------------------


class TestScheduleBeforeCreateHook:
    @pytest.fixture
    def setup_schedule(self, db_session, conn_svc, upload_svc):
        slug = f"acme-hook-sched-{uuid.uuid4().hex[:8]}"
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

        transform_svc = TransformationService()
        svc = ScheduleService(upload_svc, transform_svc)
        return org, upload, svc

    def test_handler_raising_blocks_create(self, db_session, setup_schedule):
        org, upload, svc = setup_schedule

        def quota_check(**kw):
            raise ValueError("Schedule limit reached")

        hooks.on("schedule.before_create", quota_check)

        with pytest.raises(ValueError, match="Schedule limit reached"):
            svc.create_schedule(
                db_session,
                org.id,
                NodeType.UPLOAD,
                upload.id,
                "*/5 * * * *",
            )

    def test_no_handler_allows_create(self, db_session, setup_schedule):
        org, upload, svc = setup_schedule

        schedule = svc.create_schedule(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            "*/5 * * * *",
        )
        assert schedule.id is not None


# ---------------------------------------------------------------------------
# membership.before_create — quota check
# ---------------------------------------------------------------------------


class TestMembershipBeforeCreateHook:
    @pytest.fixture
    def setup_membership(self, db_session):
        auth = AuthService(secret_key="test-secret")
        svc = UserService(auth)

        slug = f"acme-hook-mem-{uuid.uuid4().hex[:8]}"
        org = Organization(name="Acme", slug=slug)
        db_session.add(org)
        db_session.flush()

        user = User(
            email=f"user-{uuid.uuid4().hex[:6]}@example.com",
            password_hash=auth.hash_password("password123"),
            full_name="Test User",
        )
        db_session.add(user)
        db_session.flush()

        return org, user, svc

    def test_handler_raising_blocks_add_member(self, db_session, setup_membership):
        org, user, svc = setup_membership

        def quota_check(**kw):
            raise ValueError("Member limit reached")

        hooks.on("membership.before_create", quota_check)

        with pytest.raises(ValueError, match="Member limit reached"):
            svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)

    def test_no_handler_allows_add_member(self, db_session, setup_membership):
        org, user, svc = setup_membership

        membership = svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)
        assert membership.id is not None
