"""Pipeline management service — CRUD with dlt_config validation."""

import re
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.connection import ConnectionDirection
from datanika.models.pipeline import Pipeline, PipelineStatus
from datanika.services.connection_service import ConnectionService

VALID_WRITE_DISPOSITIONS = {"append", "replace", "merge"}
VALID_MODES = {"single_table", "full_database"}
VALID_ROW_ORDERS = {"asc", "desc"}
VALID_SCHEMA_CONTRACT_ENTITIES = {"tables", "columns", "data_type"}
VALID_SCHEMA_CONTRACT_VALUES = {"evolve", "freeze", "discard_value", "discard_row"}
VALID_FILTER_OPS = {"eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in"}
INTERNAL_CONFIG_KEYS = {
    "mode",
    "table",
    "source_schema",
    "table_names",
    "incremental",
    "batch_size",
    "filters",
}


_PIPELINE_NAME_RE = re.compile(r"^[a-zA-Z0-9 ]+$")


def validate_pipeline_name(name: str) -> None:
    """Validate pipeline name: non-empty, alphanumeric + spaces only."""
    stripped = name.strip()
    if not stripped:
        raise ValueError("Pipeline name cannot be empty")
    if not _PIPELINE_NAME_RE.match(stripped):
        raise ValueError(
            "Pipeline name must contain only alphanumeric characters and spaces"
        )


def to_dataset_name(name: str) -> str:
    """Convert pipeline name to a dataset name (snake_case)."""
    return re.sub(r"\s+", "_", name.strip()).lower()


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
        validate_pipeline_name(name)

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
            validate_pipeline_name(kwargs["name"])
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

        # -- mode validation --
        mode = dlt_config.get("mode", "full_database")
        if mode not in VALID_MODES:
            raise PipelineConfigError(f"mode must be one of {VALID_MODES}, got '{mode}'")

        if mode == "single_table":
            if "table" not in dlt_config:
                raise PipelineConfigError("single_table mode requires a 'table' field")
            if "table_names" in dlt_config:
                raise PipelineConfigError("single_table mode does not accept 'table_names'")
            incremental = dlt_config.get("incremental")
            if incremental is not None:
                if not isinstance(incremental, dict) or "cursor_path" not in incremental:
                    raise PipelineConfigError("incremental requires a 'cursor_path' field")
                row_order = incremental.get("row_order")
                if row_order is not None and row_order not in VALID_ROW_ORDERS:
                    raise PipelineConfigError(
                        f"row_order must be one of {VALID_ROW_ORDERS}, got '{row_order}'"
                    )
        else:  # full_database
            if "table" in dlt_config:
                raise PipelineConfigError("full_database mode does not accept 'table'")
            if "incremental" in dlt_config:
                raise PipelineConfigError("full_database mode does not accept 'incremental'")
            table_names = dlt_config.get("table_names")
            if table_names is not None and not isinstance(table_names, list):
                raise PipelineConfigError("table_names must be a list")

        # -- batch_size --
        batch_size = dlt_config.get("batch_size")
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise PipelineConfigError("batch_size must be a positive integer")

        # -- source_schema --
        source_schema = dlt_config.get("source_schema")
        if source_schema is not None and not isinstance(source_schema, str):
            raise PipelineConfigError("source_schema must be a string")

        # -- schema_contract --
        schema_contract = dlt_config.get("schema_contract")
        if schema_contract is not None:
            if not isinstance(schema_contract, dict):
                raise PipelineConfigError("schema_contract must be a dict")
            for entity, value in schema_contract.items():
                if entity not in VALID_SCHEMA_CONTRACT_ENTITIES:
                    raise PipelineConfigError(
                        f"schema_contract key '{entity}' not in {VALID_SCHEMA_CONTRACT_ENTITIES}"
                    )
                if value not in VALID_SCHEMA_CONTRACT_VALUES:
                    raise PipelineConfigError(
                        f"schema_contract value '{value}' not in {VALID_SCHEMA_CONTRACT_VALUES}"
                    )

        # -- filters --
        filters = dlt_config.get("filters")
        if filters is not None:
            if not isinstance(filters, list):
                raise PipelineConfigError("filters must be a list")
            for f in filters:
                if not isinstance(f, dict):
                    raise PipelineConfigError("Each filter must be a dict")
                for required in ("column", "op", "value"):
                    if required not in f:
                        raise PipelineConfigError(f"Each filter requires '{required}'")
                if f["op"] not in VALID_FILTER_OPS:
                    raise PipelineConfigError(
                        f"Filter op must be one of {VALID_FILTER_OPS}, got '{f['op']}'"
                    )
