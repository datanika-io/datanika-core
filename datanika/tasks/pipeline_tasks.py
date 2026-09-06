"""dbt pipeline execution Celery tasks."""

import logging
import traceback

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.connection import Connection
from datanika.models.dependency import NodeType
from datanika.models.pipeline import Pipeline, PipelineStatus
from datanika.models.transformation import Transformation
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import _build_sa_url, get_org_connection
from datanika.services.dbt_project import DbtProjectService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService, get_org_run
from datanika.services.pipeline_service import PipelineService
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
execution_service = ExecutionService()

#: dbt node kinds whose successful execution is metered as a model run.
#:
#: 🚨 **``"test"`` is deliberately absent (core#864, DECIDED by Product 2026-09-04,
#: option 2).** It was here, and it could never match: a passing dbt test node reports
#: ``status.value == "pass"``, never ``"success"``, so the conjunction below required a
#: node to be simultaneously a passing test and report the model success string. Test
#: nodes have never been billable, for the life of the counter.
#:
#: The decision was to make the code say so, **not** to start metering them — adding
#: ``"pass"`` to the status filter would have raised metered volume for every tenant
#: running dbt tests, i.e. a pricing change arriving as a bug fix.
#:
#: ⚠️ **Deleting the arm is not cosmetic, because the arm was one dbt release from
#: waking up.** ``tests/test_services/test_dbt_result_contract.py::TestTestNodeStatusIsNotSuccess``
#: exists precisely because the day dbt reports ``"success"`` for a passing test, the old
#: tuple would have started billing — a pricing change arriving as a dependency bump,
#: which is the thing nobody would be watching for. With the arm gone that release is a
#: non-event.
#:
#: 🚨 **Widening this tuple also requires widening the PRE-FLIGHT GATE in the same change
#: (core#1107).** The gate is ``PipelineService.predict_run_count()``, which returns
#: ``len(pipeline.models)`` and cannot see test, seed or snapshot nodes; cloud's Path A
#: guarantees zero overshoot against this meter. Metering a kind the gate cannot count
#: admits a Free org at 470/500 and then meters it to 510, past a hard cap the gate had
#: just cleared it for. **A node kind enters the meter only when the pre-flight predictor
#: can also count it cheaply.**
_BILLABLE_RESOURCE_TYPES = ("model",)


def is_billable_node(node_result) -> bool:
    """Does this dbt node result count as one metered model run? (core#864)

    Extracted from ``run_pipeline_task`` so it can be asserted against a **real** dbt
    result rather than re-typed. It used to be an inline generator expression, and
    ``test_the_billable_nodes_expression_still_counts`` asserted a **verbatim copy** of
    it — which is green whatever the shipped expression does. Every other unit test of
    the counter used bare ``MagicMock()``, and a ``MagicMock`` answers ``status.value``
    with another ``MagicMock``, so those could not fail either way.

    ``getattr`` chains rather than attribute access: ``raw_result`` is whatever dbt
    handed back, and a node result carrying no ``node`` is not a billing event.
    """
    if getattr(getattr(node_result, "status", None), "value", None) != "success":
        return False
    node = getattr(node_result, "node", None)
    resource_type = getattr(node, "resource_type", None)
    if resource_type is None:
        return False
    return getattr(resource_type, "value", None) in _BILLABLE_RESOURCE_TYPES


def _sync_catalog_after_pipeline(
    session: Session,
    org_id: int,
    raw_result: list,
    dbt_svc: DbtProjectService,
    dst_conn: Connection,
    dst_config: dict,
) -> None:
    """Create/update catalog entries for each successful model in the dbt result."""
    catalog_svc = CatalogService()
    sa_url = _build_sa_url(dst_config, dst_conn.connection_type)

    for node_result in raw_result:
        status = getattr(getattr(node_result, "status", None), "value", None)
        if status != "success":
            continue

        node = getattr(node_result, "node", None)
        if node is None:
            continue

        resource_type = getattr(getattr(node, "resource_type", None), "value", None)
        if resource_type != "model":
            continue

        name = getattr(node, "name", None)
        schema = getattr(node, "schema", "staging")
        materialized = getattr(getattr(node, "config", None), "materialized", "view")

        if not name:
            continue

        # Look up the matching Transformation to get origin_id and description
        transformation = session.execute(
            select(Transformation).where(
                Transformation.name == name,
                Transformation.org_id == org_id,
                Transformation.deleted_at.is_(None),
            )
        ).scalar_one_or_none()

        origin_id = transformation.id if transformation else 0
        description = transformation.description if transformation else None

        dbt_config = {"materialized": materialized}

        # Introspect columns from destination DB
        columns = []
        try:
            introspected = catalog_svc.introspect_tables(
                sa_url, schema_name=schema, table_names=[name]
            )
            if introspected:
                columns = introspected[0].get("columns", [])
        except Exception:
            logger.exception("Column introspection failed for %s.%s (non-fatal)", schema, name)

        catalog_svc.upsert_entry(
            session,
            org_id,
            entry_type=CatalogEntryType.DBT_MODEL,
            origin_type=NodeType.TRANSFORMATION,
            origin_id=origin_id,
            table_name=name,
            schema_name=schema,
            dataset_name=schema,
            columns=columns,
            description=description,
            dbt_config=dbt_config,
        )

        dbt_svc.write_model_yml(
            org_id,
            name,
            schema,
            columns=columns,
            description=description,
            dbt_config=dbt_config,
        )


def _write_transformation_models(
    session: Session,
    org_id: int,
    destination_connection_id: int,
    dbt_svc: DbtProjectService,
) -> None:
    """Write .sql model files for all active transformations targeting this destination.

    Includes transformations with ``destination_connection_id`` matching the
    pipeline's destination **or** NULL (inherits the pipeline destination).
    """
    transformations = (
        session.execute(
            select(Transformation).where(
                Transformation.org_id == org_id,
                Transformation.deleted_at.is_(None),
                or_(
                    Transformation.destination_connection_id == destination_connection_id,
                    Transformation.destination_connection_id.is_(None),
                ),
            )
        )
        .scalars()
        .all()
    )

    for t in transformations:
        dbt_svc.write_model(
            org_id,
            t.name,
            t.sql_body,
            schema_name=t.schema_name,
            materialization=t.materialization.value,
            incremental_config=t.incremental_config,
        )


def run_pipeline(
    run_id: int,
    org_id: int,
    session: Session | None = None,
    encryption: EncryptionService | None = None,
) -> None:
    """Execute a dbt pipeline.

    When called from Celery, ``session`` and ``encryption`` are created
    internally.  Tests pass them directly.
    """
    own_session = session is None
    #: Billable model/test count, set on the success path and announced only
    #: after the commit lands (core#522). None means "nothing to meter".
    models_completed = None
    if own_session:
        from datanika.db import get_sync_session

        session = get_sync_session()

    if encryption is None:
        from datanika.config import settings

        encryption = EncryptionService(settings.credential_encryption_key)

    try:
        # Check run quota before starting (cloud plugin may block).
        # Load the pipeline first so we can pass a cheap prediction
        # (Path A in datanika-cloud/docs/billing_contract.md). Falls
        # back to Path B (predicted_runs=None) for fan-out / custom
        # selectors where the static model list under-counts.
        from datanika.hooks import emit as _emit_hook

        run = get_org_run(session, org_id, run_id)
        pipeline = session.execute(
            select(Pipeline).where(Pipeline.id == run.target_id, Pipeline.org_id == org_id)
        ).scalar_one()

        # ELT pipelines run dbt against raw-landed data — same dbt execution,
        # different source (raw schema vs dlt-loaded schema). No short-circuit
        # needed; the mode difference is in how the *upload* lands data, not
        # how the pipeline transforms it. Pipeline tasks always run dbt.

        predicted = PipelineService.predict_run_count(pipeline)
        _emit_hook(
            "run.before_execute",
            session=session,
            org_id=org_id,
            predicted_runs=predicted,
        )

        execution_service.start_run(session, org_id, run_id)
        if own_session:
            session.commit()

        dst_conn = get_org_connection(session, org_id, pipeline.destination_connection_id)
        if dst_conn is None:
            raise UserFacingError(
                f"Destination connection {pipeline.destination_connection_id} is not "
                f"available to org {org_id}"
            )
        dst_config = encryption.decrypt(dst_conn.config_encrypted)

        org = session.get(Organization, org_id)
        default_schema = org.default_dbt_schema if org else "datanika"

        from datanika.config import settings

        dbt_svc = DbtProjectService(settings.dbt_projects_dir)
        dbt_svc.ensure_project(org_id)

        # Generate profiles.yml from destination connection
        dbt_svc.generate_profiles_yml(
            org_id,
            dst_conn.connection_type.value,
            dst_config,
            default_schema=default_schema,
        )

        # Write .sql model files for all relevant transformations
        _write_transformation_models(session, org_id, pipeline.destination_connection_id, dbt_svc)

        # Build selector
        selector = PipelineService.build_selector(pipeline.models, pipeline.custom_selector)

        # Clean stale dbt artifacts before run
        dbt_svc.clean_target(org_id)

        # Execute dbt command
        result = dbt_svc.run_command(
            org_id,
            pipeline.command.value,
            selector=selector,
            full_refresh=pipeline.full_refresh,
        )

        if result["success"]:
            execution_service.complete_run(
                session,
                org_id,
                run_id,
                rows_loaded=result["rows_affected"],
                logs=result["logs"],
            )

            raw_result = result.get("raw_result") or []

            try:
                _sync_catalog_after_pipeline(
                    session, org_id, raw_result, dbt_svc, dst_conn, dst_config
                )
            except Exception:
                logger.exception("Pipeline catalog sync failed (non-fatal)")

            # Count successful model nodes for usage metering — test nodes are not
            # metered (core#864, Product's decision; see is_billable_node).
            billable_nodes = sum(1 for r in raw_result if is_billable_node(r))
            # Recorded here, announced after the commit (core#522) — see the
            # `else` clause below.
            if billable_nodes > 0:
                models_completed = billable_nodes

            pipeline.status = PipelineStatus.ACTIVE
            session.flush()
        else:
            execution_service.fail_run(
                session,
                org_id,
                run_id,
                error_message="dbt command failed",
                logs=result["logs"],
            )
            pipeline.status = PipelineStatus.ERROR
            session.flush()

        if own_session:
            session.commit()

    except Exception as exc:
        if own_session:
            session.rollback()
        execution_service.fail_run(
            session,
            org_id,
            run_id,
            error_message=str(exc),
            logs=traceback.format_exc(),
        )
        run_obj = get_org_run(session, org_id, run_id)
        if run_obj:
            pipe = session.execute(
                select(Pipeline).where(Pipeline.id == run_obj.target_id, Pipeline.org_id == org_id)
            ).scalar_one_or_none()
            if pipe:
                pipe.status = PipelineStatus.ERROR
                session.flush()
        if own_session:
            session.commit()

    else:
        # After the commit, not before it (core#522). A commit failure used to
        # end the run FAILED having already metered its models, and cloud
        # commits the ledger row on its own session, so the rollback did not
        # take it back. `else` runs only when the whole `try` succeeded, which
        # makes the ordering structural rather than a matter of statement
        # order inside a long block — and still ahead of `finally`, which
        # closes the session handlers are handed.
        #
        # `models_completed` stays None when dbt failed or nothing billable
        # ran, so neither case announces.
        if models_completed is not None:
            from datanika.hooks import announce

            # See upload_tasks: announced, not emitted (core#456).
            announce(
                "run.models_completed",
                session=session,
                org_id=org_id,
                run_id=run_id,
                status="success",
                target_type="pipeline",
                target_id=pipeline.id,
                count=models_completed,
            )

    finally:
        if own_session:
            session.close()


@celery_app.task(bind=True, name="datanika.run_pipeline", max_retries=60)
def run_pipeline_task(self, run_id: int, org_id: int, scheduled: bool = False):
    """Celery entry point for dbt pipeline execution."""
    if scheduled:
        from datanika.tasks.dependency_helpers import check_deps_or_retry

        check_deps_or_retry(self, run_id, org_id, NodeType.PIPELINE)

    from datanika.services.concurrency_service import acquire, release

    if not acquire(org_id):
        raise self.retry(countdown=30, max_retries=60)
    try:
        run_pipeline(run_id=run_id, org_id=org_id)
    finally:
        release(org_id)
