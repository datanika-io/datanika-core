"""Pipeline management service — CRUD for dbt pipeline orchestration."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.pipeline import DbtCommand, Pipeline, PipelineStatus
from datanika.services.connection_service import (
    TRANSFORM_DESTINATION_TYPES,
    get_org_connection,
)


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
        self._require_own_connection(session, org_id, destination_connection_id)

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
            self._require_own_connection(session, org_id, kwargs["destination_connection_id"])
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
    def _require_own_connection(session: Session, org_id: int, conn_id: int) -> None:
        """Refuse a destination connection the caller's org does not own.

        The message deliberately does not distinguish "belongs to someone else"
        from "does not exist" — telling them apart turns this into an oracle for
        probing which connection ids are live in other orgs.
        """
        conn = get_org_connection(session, org_id, conn_id)
        if conn is None:
            raise PipelineConfigError(f"Invalid destination connection {conn_id}: must exist")
        # core#862. Owning the connection is not enough: pipelines always run through dbt,
        # which needs an installed adapter for the target. Refused HERE rather
        # than in the picker because `POST /api/v1/pipelines` bypasses every
        # picker, and refused at SAVE time rather than at run time because
        # `generate_profiles_yml` raises only after `run.before_execute` has
        # fired and `start_run` has recorded a run — so a doomed run has already
        # cost the tenant quota. Naming the type is safe once ownership is
        # established; the message above deliberately stays vague because it is
        # reachable by a caller who owns nothing.
        if conn.connection_type.value not in TRANSFORM_DESTINATION_TYPES:
            raise PipelineConfigError(
                f"Connection {conn_id} is a {conn.connection_type.value} destination, "
                "which has no dbt adapter and cannot be a transformation target"
            )

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

    @staticmethod
    def predict_run_count(pipeline: Pipeline) -> int | None:
        """Predict how many dbt nodes a pipeline run will execute.

        Returns an int when the count is cheaply knowable from the
        static pipeline config, or ``None`` when a real dbt graph
        walk would be needed (fan-out flags or a custom selector).

        Callers pass the result as ``predicted_runs`` to the
        ``run.before_execute`` hook. ``None`` puts the run on Path B
        (allow-then-block fallback) per
        ``datanika-cloud/docs/billing_contract.md``.

        Rules (cheap predictor, no dbt invocation):
        - If ``custom_selector`` is set: return ``None`` (selector
          could expand to anything).
        - If any model entry has ``upstream=True`` or
          ``downstream=True``: return ``None`` (fan-out).
        - Otherwise: return ``len(pipeline.models)``, which is the
          exact count dbt will run for a flat ``--select a b c`` list.
        """
        if pipeline.custom_selector and pipeline.custom_selector.strip():
            return None
        models = pipeline.models or []
        for m in models:
            if m.get("upstream") or m.get("downstream"):
                return None
        return len(models)
