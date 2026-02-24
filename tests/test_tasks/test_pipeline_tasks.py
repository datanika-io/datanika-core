"""TDD tests for pipeline Celery tasks."""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.pipeline import DbtCommand, Pipeline
from datanika.models.run import RunStatus
from datanika.models.transformation import Materialization, Transformation
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.pipeline_tasks import run_pipeline


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def exec_svc():
    return ExecutionService()


@pytest.fixture
def setup_pipeline(db_session, encryption, exec_svc):
    """Create org, connection, transformations, pipeline, and pending run."""
    slug = f"acme-pipe-{uuid.uuid4().hex[:8]}"
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

    # Create transformations that the pipeline selects
    t1 = Transformation(
        org_id=org.id,
        name="src_order_items",
        description="Order items staging",
        sql_body="SELECT 1",
        materialization=Materialization.VIEW,
        schema_name="staging",
    )
    t2 = Transformation(
        org_id=org.id,
        name="src_users",
        description="Users staging",
        sql_body="SELECT 1",
        materialization=Materialization.TABLE,
        schema_name="staging",
    )
    db_session.add_all([t1, t2])
    db_session.flush()

    pipeline = Pipeline(
        org_id=org.id,
        name="my_pipeline",
        destination_connection_id=conn.id,
        command=DbtCommand.RUN,
        models=[{"name": "src_order_items"}, {"name": "src_users"}],
    )
    db_session.add(pipeline)
    db_session.flush()

    run = exec_svc.create_run(db_session, org.id, NodeType.PIPELINE, pipeline.id)
    return org, conn, pipeline, [t1, t2], run


def _mock_dbt_project():
    """Return a patch context that mocks DbtProjectService for pipeline task tests."""
    return patch("datanika.tasks.pipeline_tasks.DbtProjectService")


class TestRunPipelineTask:
    def test_transitions_to_success(self, db_session, encryption, setup_pipeline):
        org, conn, pipeline, transformations, run = setup_pipeline
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )
        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS

    def test_fails_on_dbt_error(self, db_session, encryption, setup_pipeline):
        org, conn, pipeline, transformations, run = setup_pipeline
        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": False,
                "rows_affected": 0,
                "logs": "dbt error",
                "raw_result": [],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )
        db_session.refresh(run)
        assert run.status == RunStatus.FAILED


class TestPipelineWritesModels:
    def test_pipeline_writes_all_transformation_models(
        self, db_session, encryption, setup_pipeline
    ):
        """A full pipeline run writes .sql model files for all active transformations
        that target the same destination connection (or have no explicit destination)."""
        org, conn, pipeline, transformations, run = setup_pipeline

        # Add 3 more transformations — these have never been run individually,
        # so their .sql files don't exist on disk yet.
        extra = []
        for i, name in enumerate(["dim_customers", "fct_orders", "int_payments"]):
            t = Transformation(
                org_id=org.id,
                name=name,
                description=f"Extra model {i}",
                sql_body=f"SELECT {i}",
                materialization=Materialization.VIEW,
                schema_name="staging",
                # destination_connection_id=None means "inherits pipeline destination"
            )
            extra.append(t)
        db_session.add_all(extra)

        # Add a transformation that targets a DIFFERENT destination — should NOT be written
        other_conn = Connection(
            org_id=org.id,
            name="other_dest",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted=encryption.encrypt(
                {"host": "h2", "port": 5432, "user": "u2", "password": "p2", "database": "d2"}
            ),
        )
        db_session.add(other_conn)
        db_session.flush()

        other_t = Transformation(
            org_id=org.id,
            name="other_model",
            sql_body="SELECT 999",
            materialization=Materialization.TABLE,
            schema_name="staging",
            destination_connection_id=other_conn.id,
        )
        db_session.add(other_t)

        # Add a soft-deleted transformation — should NOT be written
        import datetime

        deleted_t = Transformation(
            org_id=org.id,
            name="deleted_model",
            sql_body="SELECT -1",
            materialization=Materialization.VIEW,
            schema_name="staging",
        )
        db_session.add(deleted_t)
        db_session.flush()
        deleted_t.deleted_at = datetime.datetime.now(datetime.UTC)

        db_session.flush()

        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        # write_model should have been called for each of the 5 matching transformations
        # (2 from setup_pipeline + 3 extra with NULL destination_connection_id)
        # but NOT for other_model (different dest) or deleted_model (soft-deleted)
        write_calls = instance.write_model.call_args_list
        written_names = {call.args[1] for call in write_calls}
        assert written_names == {
            "src_order_items",
            "src_users",
            "dim_customers",
            "fct_orders",
            "int_payments",
        }
        assert len(write_calls) == 5

    def test_pipeline_writes_models_matching_destination(self, db_session, encryption):
        """Transformations explicitly targeting the pipeline's destination are also written."""
        slug = f"acme-dst-{uuid.uuid4().hex[:8]}"
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

        # Transformation that explicitly targets this destination
        t = Transformation(
            org_id=org.id,
            name="explicit_dest_model",
            sql_body="SELECT 1",
            materialization=Materialization.TABLE,
            schema_name="analytics",
            destination_connection_id=conn.id,
        )
        db_session.add(t)
        db_session.flush()

        pipeline = Pipeline(
            org_id=org.id,
            name="dst_pipeline",
            destination_connection_id=conn.id,
            command=DbtCommand.RUN,
        )
        db_session.add(pipeline)
        db_session.flush()

        exec_svc = ExecutionService()
        run = exec_svc.create_run(db_session, org.id, NodeType.PIPELINE, pipeline.id)

        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        write_calls = instance.write_model.call_args_list
        written_names = {call.args[1] for call in write_calls}
        assert "explicit_dest_model" in written_names


class TestPipelineCatalogSync:
    def test_pipeline_run_syncs_catalog(self, db_session, encryption, setup_pipeline):
        """After successful pipeline run, catalog entries exist for each model."""
        org, conn, pipeline, transformations, run = setup_pipeline

        # Build mock dbt RunResult nodes
        node1 = MagicMock()
        node1.node.name = "src_order_items"
        node1.node.schema = "staging"
        node1.node.resource_type.value = "model"
        node1.node.config.materialized = "view"
        node1.status.value = "success"

        node2 = MagicMock()
        node2.node.name = "src_users"
        node2.node.schema = "staging"
        node2.node.resource_type.value = "model"
        node2.node.config.materialized = "table"
        node2.status.value = "success"

        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [node1, node2],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS

        entries = CatalogService.list_entries(db_session, org.id)
        entry_names = {e.table_name for e in entries}
        assert "src_order_items" in entry_names
        assert "src_users" in entry_names
        assert len(entries) == 2

        for entry in entries:
            assert entry.origin_type == NodeType.TRANSFORMATION

    def test_catalog_sync_failure_does_not_fail_run(self, db_session, encryption, setup_pipeline):
        """Catalog sync is non-fatal — run still succeeds if sync raises."""
        org, conn, pipeline, transformations, run = setup_pipeline

        node = MagicMock()
        node.node.name = "src_order_items"
        node.node.schema = "staging"
        node.node.resource_type.value = "model"
        node.node.config.materialized = "view"
        node.status.value = "success"

        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 5,
                "logs": "",
                "raw_result": [node],
            }
            # write_model_yml will fail — catalog sync should be non-fatal
            instance.write_model_yml.side_effect = RuntimeError("yml write failed")
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.rows_loaded == 5
