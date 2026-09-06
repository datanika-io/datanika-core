"""Transformation execution Celery tasks."""

import logging
import traceback

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.connection import Connection
from datanika.models.dependency import NodeType
from datanika.models.transformation import Transformation
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import _build_sa_url
from datanika.services.dbt_project import DbtProjectService
from datanika.services.execution_service import ExecutionService, get_org_run
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
execution_service = ExecutionService()


def _sync_catalog_after_transformation(
    session: Session,
    org_id: int,
    transformation: Transformation,
    dbt_svc: DbtProjectService,
    dst_conn: Connection | None = None,
    dst_config: dict | None = None,
) -> None:
    """Sync catalog entry and write model YML after a successful transformation run."""
    catalog_svc = CatalogService()
    dbt_config = {"materialized": transformation.materialization.value}
    if transformation.tags:
        dbt_config["tags"] = transformation.tags

    # Introspect columns from destination DB
    columns = []
    if dst_conn is not None and dst_config is not None:
        try:
            sa_url = _build_sa_url(dst_config, dst_conn.connection_type)
            introspected = catalog_svc.introspect_tables(
                sa_url,
                schema_name=transformation.schema_name,
                table_names=[transformation.name],
            )
            if introspected:
                columns = introspected[0].get("columns", [])
        except Exception:
            logger.exception(
                "Column introspection failed for %s.%s (non-fatal)",
                transformation.schema_name,
                transformation.name,
            )

    catalog_svc.upsert_entry(
        session,
        org_id,
        entry_type=CatalogEntryType.DBT_MODEL,
        origin_type=NodeType.TRANSFORMATION,
        origin_id=transformation.id,
        table_name=transformation.name,
        schema_name=transformation.schema_name,
        dataset_name=transformation.schema_name,
        columns=columns,
        description=transformation.description,
        dbt_config=dbt_config,
    )
    dbt_svc.write_model_yml(
        org_id,
        transformation.name,
        transformation.schema_name,
        columns=columns,
        description=transformation.description,
        dbt_config=dbt_config,
    )


def run_transformation(
    run_id: int,
    org_id: int,
    session: Session | None = None,
) -> None:
    """Execute a dbt transformation.

    When called from Celery, ``session`` is created internally.
    Tests pass it directly — in that case, the caller manages transaction
    boundaries (no commit/rollback here).
    """
    own_session = session is None
    if own_session:
        from datanika.db import get_sync_session

        session = get_sync_session()

    try:
        # Check run quota before starting (cloud plugin may block).
        # Standalone transformations are Path A with predicted_runs=1,
        # per datanika-cloud/docs/billing_contract.md.
        from datanika.hooks import emit

        emit("run.before_execute", session=session, org_id=org_id, predicted_runs=1)

        execution_service.start_run(session, org_id, run_id)
        if own_session:
            session.commit()

        run = get_org_run(session, org_id, run_id)
        transformation = session.execute(
            select(Transformation).where(
                Transformation.id == run.target_id,
                Transformation.org_id == org_id,
            )
        ).scalar_one_or_none()

        if transformation is None:
            raise UserFacingError(
                f"Transformation not found: target_id={run.target_id}, org_id={org_id}"
            )

        from datanika.config import settings
        from datanika.services.connection_service import ConnectionService
        from datanika.services.encryption import EncryptionService

        org = session.get(Organization, org_id)
        default_schema = org.default_dbt_schema if org else "datanika"

        dbt_svc = DbtProjectService(settings.dbt_projects_dir)
        dbt_svc.ensure_project(org_id)

        # Generate profiles.yml from the transformation's destination connection
        dst_conn = None
        dst_config = None
        if transformation.destination_connection_id:
            encryption = EncryptionService(settings.credential_encryption_key)
            conn_svc = ConnectionService(encryption)
            dst_conn = conn_svc.get_connection(
                session, org_id, transformation.destination_connection_id
            )
            if dst_conn:
                dst_config = conn_svc.get_connection_config(
                    session, org_id, transformation.destination_connection_id
                )
                if dst_config:
                    dbt_svc.generate_profiles_yml(
                        org_id,
                        dst_conn.connection_type.value,
                        dst_config,
                        default_schema=default_schema,
                    )

        dbt_svc.write_model(
            org_id,
            transformation.name,
            transformation.sql_body,
            schema_name=transformation.schema_name,
            materialization=transformation.materialization.value,
            incremental_config=transformation.incremental_config,
        )
        dbt_svc.clean_target(org_id)
        result = dbt_svc.run_model(org_id, transformation.name)
        rows = result["rows_affected"]
        logs = result["logs"]

        execution_service.complete_run(session, org_id, run_id, rows_loaded=rows, logs=logs)

        try:
            _sync_catalog_after_transformation(
                session, org_id, transformation, dbt_svc, dst_conn, dst_config
            )
        except Exception:
            logger.exception("Catalog sync failed (non-fatal)")

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
        if own_session:
            session.commit()

    else:
        # After the commit, not before it (core#522) — a commit failure used to
        # end the run FAILED having already been metered, and cloud commits the
        # ledger row on its own session so the rollback did not undo it. `else`
        # makes the ordering structural and still runs before `finally` closes
        # the session the handlers receive.
        from datanika.hooks import announce

        # See upload_tasks: announced, not emitted (core#456).
        announce(
            "run.transformation_completed",
            session=session,
            org_id=org_id,
            run_id=run_id,
            status="success",
            target_type="transformation",
            target_id=transformation.id,
        )

    finally:
        if own_session:
            session.close()


@celery_app.task(bind=True, name="datanika.run_transformation", max_retries=60)
def run_transformation_task(self, run_id: int, org_id: int, scheduled: bool = False):
    """Celery entry point for transformation execution."""
    if scheduled:
        from datanika.models.dependency import NodeType
        from datanika.tasks.dependency_helpers import check_deps_or_retry

        check_deps_or_retry(self, run_id, org_id, NodeType.TRANSFORMATION)

    from datanika.services.concurrency_service import acquire, release

    if not acquire(org_id):
        raise self.retry(countdown=30, max_retries=60)
    try:
        run_transformation(run_id=run_id, org_id=org_id)
    finally:
        release(org_id)
