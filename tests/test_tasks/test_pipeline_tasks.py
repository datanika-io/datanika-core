"""TDD tests for pipeline Celery tasks."""

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.pipeline import DbtCommand, Pipeline, PipelineStatus
from datanika.models.run import RunStatus
from datanika.models.transformation import Materialization, Transformation
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.pipeline_tasks import (
    _BILLABLE_RESOURCE_TYPES,
    is_billable_node,
    run_pipeline,
)


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

    def test_pipeline_status_active_on_success(self, db_session, encryption, setup_pipeline):
        org, conn, pipeline, transformations, run = setup_pipeline
        assert pipeline.status == PipelineStatus.DRAFT

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

        db_session.refresh(pipeline)
        assert pipeline.status == PipelineStatus.ACTIVE

    def test_pipeline_status_error_on_dbt_failure(self, db_session, encryption, setup_pipeline):
        org, conn, pipeline, transformations, run = setup_pipeline
        assert pipeline.status == PipelineStatus.DRAFT

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

        db_session.refresh(pipeline)
        assert pipeline.status == PipelineStatus.ERROR

    def test_pipeline_status_error_on_exception(self, db_session, encryption, setup_pipeline):
        org, conn, pipeline, transformations, run = setup_pipeline
        assert pipeline.status == PipelineStatus.DRAFT

        with _mock_dbt_project() as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.side_effect = RuntimeError("boom")
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(pipeline)
        assert pipeline.status == PipelineStatus.ERROR

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

    def test_pipeline_catalog_sync_introspects_columns(
        self, db_session, encryption, setup_pipeline
    ):
        """After a successful pipeline run, catalog entries should contain columns
        introspected from the destination database."""
        org, conn, pipeline, transformations, run = setup_pipeline

        node = MagicMock()
        node.node.name = "src_order_items"
        node.node.schema = "staging"
        node.node.resource_type.value = "model"
        node.node.config.materialized = "view"
        node.status.value = "success"

        introspected_columns = [
            {"name": "id", "data_type": "INTEGER"},
            {"name": "qty", "data_type": "BIGINT"},
        ]

        with (
            _mock_dbt_project() as mock_dbt_cls,
            patch(
                "datanika.tasks.pipeline_tasks.CatalogService.introspect_tables"
            ) as mock_introspect,
            patch(
                "datanika.tasks.pipeline_tasks._build_sa_url",
                return_value="postgresql+psycopg2://u:p@h:5432/d",
            ),
        ):
            mock_introspect.return_value = [
                {"table_name": "src_order_items", "columns": introspected_columns}
            ]
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [node],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        entries = CatalogService.list_entries(db_session, org.id)
        assert len(entries) == 1
        assert entries[0].columns == introspected_columns

    def test_pipeline_catalog_sync_falls_back_on_introspect_failure(
        self, db_session, encryption, setup_pipeline
    ):
        """If introspection fails, the run still succeeds and columns are not wiped."""
        org, conn, pipeline, transformations, run = setup_pipeline

        node = MagicMock()
        node.node.name = "src_order_items"
        node.node.schema = "staging"
        node.node.resource_type.value = "model"
        node.node.config.materialized = "view"
        node.status.value = "success"

        with (
            _mock_dbt_project() as mock_dbt_cls,
            patch(
                "datanika.tasks.pipeline_tasks.CatalogService.introspect_tables",
                side_effect=Exception("connection refused"),
            ),
            patch(
                "datanika.tasks.pipeline_tasks._build_sa_url",
                return_value="postgresql+psycopg2://u:p@h:5432/d",
            ),
        ):
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 10,
                "logs": "",
                "raw_result": [node],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS
        assert run.rows_loaded == 10


class TestRunPipelinePredictedRuns:
    """Verify run_pipeline passes predicted_runs to run.before_execute,
    per datanika-cloud/docs/billing_contract.md Path A contract.

    The cloud plugin subscribes to this hook with a handler that reads
    the kwarg and gates on usage + predicted_runs. Core's job is to
    compute the prediction and pass it; we test that forwarding here.
    """

    def test_flat_pipeline_passes_model_count(self, db_session, encryption, setup_pipeline):
        """models=[a, b] (flat) → predicted_runs=2."""
        org, conn, pipeline, transformations, run = setup_pipeline
        captured = {}

        def _capture(event, **kwargs):
            if event == "run.before_execute":
                captured["predicted_runs"] = kwargs.get("predicted_runs")

        with (
            _mock_dbt_project() as mock_dbt_cls,
            patch("datanika.hooks.emit", side_effect=_capture),
        ):
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

        assert captured["predicted_runs"] == 2  # models=[src_order_items, src_users]

    def test_custom_selector_pipeline_passes_none(self, db_session, encryption, setup_pipeline):
        """custom_selector set → predicted_runs=None (Path B fallback)."""
        org, conn, pipeline, transformations, run = setup_pipeline
        pipeline.custom_selector = "tag:nightly"
        db_session.flush()

        captured = {}

        def _capture(event, **kwargs):
            if event == "run.before_execute":
                captured["predicted_runs"] = kwargs.get("predicted_runs", "NOT_PASSED")

        with (
            _mock_dbt_project() as mock_dbt_cls,
            patch("datanika.hooks.emit", side_effect=_capture),
        ):
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

        assert captured["predicted_runs"] is None

    def test_upstream_fan_out_passes_none(self, db_session, encryption, setup_pipeline):
        """Any upstream=True flag → predicted_runs=None."""
        org, conn, pipeline, transformations, run = setup_pipeline
        pipeline.models = [{"name": "src_order_items", "upstream": True}]
        db_session.flush()

        captured = {}

        def _capture(event, **kwargs):
            if event == "run.before_execute":
                captured["predicted_runs"] = kwargs.get("predicted_runs", "NOT_PASSED")

        with (
            _mock_dbt_project() as mock_dbt_cls,
            patch("datanika.hooks.emit", side_effect=_capture),
        ):
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

        assert captured["predicted_runs"] is None

    def test_elt_mode_runs_dbt_normally(self, db_session, encryption, setup_pipeline):
        """V2 P3 — ELT pipelines run dbt against raw-landed data, same as ETL."""
        from datanika.models.pipeline import PipelineMode

        org, conn, pipeline, transformations, run = setup_pipeline
        pipeline.mode = PipelineMode.ELT
        db_session.flush()

        with _mock_dbt_project() as mock_dbt_cls:
            mock_dbt_cls.return_value.run_command.return_value = {
                "success": True,
                "rows_affected": 5,
                "logs": "dbt ran OK",
                "raw_result": [],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org.id,
                session=db_session,
                encryption=encryption,
            )
            mock_dbt_cls.return_value.run_command.assert_called_once()

        db_session.refresh(run)
        assert run.status == RunStatus.SUCCESS


class TestOnlyModelNodesAreBillable:
    """core#864 — Product DECIDED (2026-09-04, option 2): dbt test nodes are **not**
    metered, so the dead ``"test"`` arm of the billable-node counter is deleted.

    🔑 **The decision changes no count, no bill and no block today**, which is exactly why
    it was Product's to make cheaply: a passing dbt test reports ``status.value ==
    "pass"``, never ``"success"``, so the arm could never match. The alternative — adding
    ``"pass"`` to the status filter — would have raised metered volume for every tenant
    running dbt tests, i.e. a pricing change arriving as a bug fix.

    🚨 **So the test that matters is not about today's dbt.** Deleting a branch that
    cannot fire is invisible to any assertion about current behaviour. What the deletion
    actually buys is the *future*: the day dbt reports ``"success"`` for a passing test
    node, the old tuple would have started billing silently — a pricing change arriving
    as a **dependency bump**, and nobody watches for those.
    ``test_a_test_node_reporting_success_is_still_not_billable`` is that scenario, and it
    is the one that goes red against the pre-decision code.

    ⚠️ **No ``MagicMock`` anywhere here, deliberately.** The issue's own "why it went
    unnoticed" is that every unit test of this counter used a bare ``MagicMock()``, which
    answers ``status.value`` with another ``MagicMock`` and therefore passes whatever the
    real values are — a checker with one possible answer. ``SimpleNamespace`` raises
    ``AttributeError`` for anything it was not given, so a missing attribute is a failure
    rather than a fabrication. The real-dbt end of this is
    ``tests/test_services/test_dbt_result_contract.py``, which drives an actual ``dbt
    run`` and now calls this same shipped function instead of a copy of it.
    """

    @staticmethod
    def _node(status: str, resource_type: str):
        """A node result shaped like dbt's, built from real strings, not a mock."""
        return SimpleNamespace(
            status=SimpleNamespace(value=status),
            node=SimpleNamespace(resource_type=SimpleNamespace(value=resource_type)),
        )

    def test_a_successful_model_node_is_billable(self):
        """The control. A narrowing that broke this would meter nothing at all."""
        assert is_billable_node(self._node("success", "model")) is True

    def test_a_test_node_reporting_success_is_still_not_billable(self):
        """🔑 The assertion the decision is actually about — and the only red one.

        This shape does not occur with any dbt we ship against: ``TestStatus.Pass``
        carries ``"pass"``. It is asserted because the *old* tuple would have counted it
        the moment that changed, and a dependency bump is the least-watched way for a
        bill to move.
        """
        assert is_billable_node(self._node("success", "test")) is False, (
            "a test node is being metered as a model run. core#864 decided (Product, "
            "option 2) that dbt test nodes are NOT metered. If test nodes should now "
            "bill, that is a pricing decision and it does not belong in this tuple "
            "without one."
        )

    def test_a_passing_test_node_is_not_billable(self):
        """Today's real shape — passes before and after, and says so.

        Kept as the *documentation* of why the arm was dead, not as evidence of the fix:
        it is green against both versions of the code.
        """
        assert is_billable_node(self._node("pass", "test")) is False

    def test_a_failed_model_node_is_not_billable(self):
        assert is_billable_node(self._node("error", "model")) is False

    def test_a_snapshot_or_seed_node_is_not_billable(self):
        """Neither was ever in the tuple; asserted so a widening has to justify itself."""
        assert is_billable_node(self._node("success", "snapshot")) is False
        assert is_billable_node(self._node("success", "seed")) is False

    def test_a_node_result_with_no_node_is_not_billable(self):
        """dbt can hand back a result carrying no node; that is not a billing event."""
        assert is_billable_node(SimpleNamespace(status=SimpleNamespace(value="success"))) is False

    def test_the_billable_set_is_exactly_model(self):
        """🚨 Pins the decision itself, so a widening is a deliberate edit here.

        If this set GREW, something added a node kind to the meter — that is a pricing
        change and needs Product, not a commit. If it SHRANK to empty, transformation
        runs have stopped being metered entirely.
        """
        assert _BILLABLE_RESOURCE_TYPES == ("model",), (
            f"the metered dbt node kinds are now {_BILLABLE_RESOURCE_TYPES}. core#864 "
            "settled this at ('model',). Changing it changes what customers are billed."
        )
