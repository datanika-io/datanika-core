"""Pipeline management service — CRUD with dlt_config validation."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from etlfabric.models.connection import ConnectionDirection
from etlfabric.models.pipeline import Pipeline, PipelineStatus
from etlfabric.services.connection_service import ConnectionService

VALID_WRITE_DISPOSITIONS = {"append", "replace", "merge"}


class PipelineConfigError(ValueError):
    """Raised when pipeline dlt_config fails validation."""


class PipelineService:
    def __init__(self, connection_service: ConnectionService):
        self._conn_svc = connection_service

    def create_pipeline(
        self,
        session: Session,
        org_id: int,
        name: str,
        description: str | None,
        source_connection_id: int,
        destination_connection_id: int,
        dlt_config: dict,
    ) -> Pipeline:
        # Validate source connection
        src = self._conn_svc.get_connection(session, org_id, source_connection_id)
        if src is None or src.direction not in (
            ConnectionDirection.SOURCE,
            ConnectionDirection.BOTH,
        ):
            raise ValueError(
                f"Invalid source connection {source_connection_id}: "
                "must exist and have direction SOURCE or BOTH"
            )

        # Validate destination connection
        dst = self._conn_svc.get_connection(session, org_id, destination_connection_id)
        if dst is None or dst.direction not in (
            ConnectionDirection.DESTINATION,
            ConnectionDirection.BOTH,
        ):
            raise ValueError(
                f"Invalid destination connection {destination_connection_id}: "
                "must exist and have direction DESTINATION or BOTH"
            )

        self.validate_pipeline_config(dlt_config)

        pipeline = Pipeline(
            org_id=org_id,
            name=name,
            description=description,
            source_connection_id=source_connection_id,
            destination_connection_id=destination_connection_id,
            dlt_config=dlt_config,
            status=PipelineStatus.DRAFT,
        )
        session.add(pipeline)
        session.flush()
        return pipeline

    def get_pipeline(self, session: Session, org_id: int, pipeline_id: int) -> Pipeline | None:
        stmt = select(Pipeline).where(
            Pipeline.id == pipeline_id,
            Pipeline.org_id == org_id,
            Pipeline.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    def list_pipelines(self, session: Session, org_id: int) -> list[Pipeline]:
        stmt = (
            select(Pipeline)
            .where(Pipeline.org_id == org_id, Pipeline.deleted_at.is_(None))
            .order_by(Pipeline.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def update_pipeline(
        self, session: Session, org_id: int, pipeline_id: int, **kwargs
    ) -> Pipeline | None:
        pipeline = self.get_pipeline(session, org_id, pipeline_id)
        if pipeline is None:
            return None

        if "dlt_config" in kwargs:
            self.validate_pipeline_config(kwargs["dlt_config"])
            pipeline.dlt_config = kwargs["dlt_config"]
        if "name" in kwargs:
            pipeline.name = kwargs["name"]
        if "description" in kwargs:
            pipeline.description = kwargs["description"]
        if "status" in kwargs:
            pipeline.status = kwargs["status"]

        session.flush()
        return pipeline

    def delete_pipeline(self, session: Session, org_id: int, pipeline_id: int) -> bool:
        pipeline = self.get_pipeline(session, org_id, pipeline_id)
        if pipeline is None:
            return False
        pipeline.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    @staticmethod
    def validate_pipeline_config(dlt_config) -> None:
        if not isinstance(dlt_config, dict):
            raise PipelineConfigError("dlt_config must be a dict")

        if not dlt_config:
            return  # empty config is valid

        disposition = dlt_config.get("write_disposition")
        if disposition is not None and disposition not in VALID_WRITE_DISPOSITIONS:
            raise PipelineConfigError(
                f"write_disposition must be one of {VALID_WRITE_DISPOSITIONS}, got '{disposition}'"
            )
        if disposition == "merge" and "primary_key" not in dlt_config:
            raise PipelineConfigError("write_disposition 'merge' requires a 'primary_key' field")
