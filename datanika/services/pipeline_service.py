"""Pipeline management service — CRUD for dbt pipeline orchestration."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.pipeline import DbtCommand, Pipeline, PipelineStatus


class PipelineConfigError(ValueError):
    """Raised when pipeline configuration fails validation."""


class PipelineService:
    def create_pipeline(
        self,
        session: Session,
        org_id: int,
        name: str,
        description: str | None,
        destination_connection_id: int,
        command: DbtCommand,
        *,
        full_refresh: bool = False,
        models: list[dict] | None = None,
        custom_selector: str | None = None,
    ) -> Pipeline:
        if not name or not name.strip():
            raise PipelineConfigError("Pipeline name cannot be empty")
        if models is None:
            models = []
        self.validate_models(models)

        pipeline = Pipeline(
            org_id=org_id,
            name=name,
            description=description,
            destination_connection_id=destination_connection_id,
            command=command,
            full_refresh=full_refresh,
            models=models,
            custom_selector=custom_selector,
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

        if "name" in kwargs:
            if not kwargs["name"] or not kwargs["name"].strip():
                raise PipelineConfigError("Pipeline name cannot be empty")
            pipeline.name = kwargs["name"]
        if "destination_connection_id" in kwargs:
            pipeline.destination_connection_id = kwargs["destination_connection_id"]
        if "description" in kwargs:
            pipeline.description = kwargs["description"]
        if "command" in kwargs:
            pipeline.command = kwargs["command"]
        if "full_refresh" in kwargs:
            pipeline.full_refresh = kwargs["full_refresh"]
        if "models" in kwargs:
            self.validate_models(kwargs["models"])
            pipeline.models = kwargs["models"]
        if "custom_selector" in kwargs:
            pipeline.custom_selector = kwargs["custom_selector"]
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
    def validate_models(models) -> None:
        if not isinstance(models, list):
            raise PipelineConfigError("models must be a list")
        for entry in models:
            if not isinstance(entry, dict):
                raise PipelineConfigError("Each model entry must be a dict")
            if not entry.get("name"):
                raise PipelineConfigError("Each model entry must have a non-empty 'name'")

    @staticmethod
    def build_selector(models: list[dict], custom_selector: str | None) -> str | None:
        """Build dbt --select string from models list or custom selector."""
        if custom_selector and custom_selector.strip():
            return custom_selector.strip()

        if not models:
            return None

        parts = []
        for m in models:
            name = m["name"]
            prefix = "+" if m.get("upstream") else ""
            suffix = "+" if m.get("downstream") else ""
            parts.append(f"{prefix}{name}{suffix}")
        return " ".join(parts)
