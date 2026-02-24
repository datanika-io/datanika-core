"""Transformation execution Celery tasks."""

import logging
import traceback

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.dependency import NodeType
from datanika.models.run import Run
from datanika.models.transformation import Transformation
from datanika.services.catalog_service import CatalogService
from datanika.services.dbt_project import DbtProjectService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
execution_service = ExecutionService()


def _sync_catalog_after_transformation(
    session: Session,
    org_id: int,
    transformation: Transformation,
    dbt_svc: DbtProjectService,
) -> None:
    """Sync catalog entry and write model YML after a successful transformation run."""
    catalog_svc = CatalogService()
    dbt_config = {"materialized": transformation.materialization.value}
    if transformation.tags:
        dbt_config["tags"] = transformation.tags
    catalog_svc.upsert_entry(
        session,
        org_id,
        entry_type=CatalogEntryType.DBT_MODEL,
        origin_type=NodeType.TRANSFORMATION,
        origin_id=transformation.id,
        table_name=transformation.name,
        schema_name=transformation.schema_name,
        dataset_name=transformation.schema_name,
        columns=[],
        description=transformation.description,
        dbt_config=dbt_config,
    )
    dbt_svc.write_model_yml(
        org_id,
        transformation.name,
        transformation.schema_name,
        columns=[],
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
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as SyncSession

        from datanika.config import settings

        engine = create_engine(settings.database_url_sync)
        session = SyncSession(engine)

    try:
        execution_service.start_run(session, run_id)
        if own_session:
            session.commit()

        run = session.get(Run, run_id)
        transformation = session.execute(
            select(Transformation).where(
                Transformation.id == run.target_id,
                Transformation.org_id == org_id,
            )
        ).scalar_one_or_none()

        if transformation is None:
            raise ValueError(
                f"Transformation not found: target_id={run.target_id}, org_id={org_id}"
            )

        from datanika.config import settings
        from datanika.services.connection_service import ConnectionService
        from datanika.services.encryption import EncryptionService

        dbt_svc = DbtProjectService(settings.dbt_projects_dir)
        dbt_svc.ensure_project(org_id)

        # Generate profiles.yml from the transformation's destination connection
        if transformation.destination_connection_id:
            encryption = EncryptionService(settings.credential_encryption_key)
            conn_svc = ConnectionService(encryption)
            conn = conn_svc.get_connection(
                session, org_id, transformation.destination_connection_id
            )
            if conn:
                decrypted = conn_svc.get_connection_config(
                    session, org_id, transformation.destination_connection_id
                )
                if decrypted:
                    dbt_svc.generate_profiles_yml(org_id, conn.connection_type.value, decrypted)

        dbt_svc.write_model(
            org_id,
            transformation.name,
            transformation.sql_body,
            schema_name=transformation.schema_name,
            materialization=transformation.materialization.value,
            incremental_config=transformation.incremental_config,
        )
        result = dbt_svc.run_model(org_id, transformation.name)
        rows = result["rows_affected"]
        logs = result["logs"]

        execution_service.complete_run(session, run_id, rows_loaded=rows, logs=logs)

        try:
            _sync_catalog_after_transformation(session, org_id, transformation, dbt_svc)
        except Exception:
            logger.exception("Catalog sync failed (non-fatal)")

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
        if own_session:
            session.commit()

    finally:
        if own_session:
            session.close()


@celery_app.task(bind=True, name="datanika.run_transformation")
def run_transformation_task(self, run_id: int, org_id: int):
    """Celery entry point for transformation execution."""
    run_transformation(run_id=run_id, org_id=org_id)
