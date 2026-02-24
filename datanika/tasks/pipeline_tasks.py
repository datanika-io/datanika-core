"""dbt pipeline execution Celery tasks."""

import logging
import traceback

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.connection import Connection
from datanika.models.dependency import NodeType
from datanika.models.pipeline import Pipeline, PipelineStatus
from datanika.models.run import Run
from datanika.models.transformation import Transformation
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import _build_sa_url
from datanika.services.dbt_project import DbtProjectService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
execution_service = ExecutionService()


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
    transformations = session.execute(
        select(Transformation).where(
            Transformation.org_id == org_id,
            Transformation.deleted_at.is_(None),
            or_(
                Transformation.destination_connection_id == destination_connection_id,
                Transformation.destination_connection_id.is_(None),
            ),
        )
    ).scalars().all()

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
    if own_session:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as SyncSession

        from datanika.config import settings

        engine = create_engine(settings.database_url_sync)
        session = SyncSession(engine)

    if encryption is None:
        from datanika.config import settings

        encryption = EncryptionService(settings.credential_encryption_key)

    try:
        execution_service.start_run(session, run_id)
        if own_session:
            session.commit()

        run = session.get(Run, run_id)
        pipeline = session.execute(
            select(Pipeline).where(Pipeline.id == run.target_id, Pipeline.org_id == org_id)
        ).scalar_one()

        dst_conn = session.get(Connection, pipeline.destination_connection_id)
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
                run_id,
                rows_loaded=result["rows_affected"],
                logs=result["logs"],
            )

            try:
                raw_result = result.get("raw_result") or []
                _sync_catalog_after_pipeline(
                    session, org_id, raw_result, dbt_svc, dst_conn, dst_config
                )
            except Exception:
                logger.exception("Pipeline catalog sync failed (non-fatal)")

            pipeline.status = PipelineStatus.ACTIVE
            session.flush()
        else:
            execution_service.fail_run(
                session,
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
            run_id,
            error_message=str(exc),
            logs=traceback.format_exc(),
        )
        run_obj = session.get(Run, run_id)
        if run_obj:
            pipe = session.execute(
                select(Pipeline).where(
                    Pipeline.id == run_obj.target_id, Pipeline.org_id == org_id
                )
            ).scalar_one_or_none()
            if pipe:
                pipe.status = PipelineStatus.ERROR
                session.flush()
        if own_session:
            session.commit()

    finally:
        if own_session:
            session.close()


@celery_app.task(bind=True, name="datanika.run_pipeline")
def run_pipeline_task(self, run_id: int, org_id: int, scheduled: bool = False):
    """Celery entry point for dbt pipeline execution."""
    if scheduled:
        from datanika.tasks.dependency_helpers import check_deps_or_retry

        check_deps_or_retry(self, run_id, org_id, NodeType.PIPELINE)
    run_pipeline(run_id=run_id, org_id=org_id)
