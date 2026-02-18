"""Upload management service — CRUD with dlt_config validation."""

from datetime import UTC, datetime
from functools import partial

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.connection import ConnectionDirection
from datanika.models.upload import Upload, UploadStatus
from datanika.services.connection_service import ConnectionService
from datanika.services.naming import to_snake_case, validate_name

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

validate_upload_name = partial(validate_name, entity_label="Upload")
to_dataset_name = to_snake_case


class UploadConfigError(ValueError):
    """Raised when upload dlt_config fails validation."""


class UploadService:
    def __init__(self, connection_service: ConnectionService):
        self._conn_svc = connection_service

    def create_upload(
        self,
        session: Session,
        org_id: int,
        name: str,
        description: str | None,
        source_connection_id: int,
        destination_connection_id: int,
        dlt_config: dict,
    ) -> Upload:
        validate_upload_name(name)

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

        self.validate_upload_config(dlt_config)

        upload = Upload(
            org_id=org_id,
            name=name,
            description=description,
            source_connection_id=source_connection_id,
            destination_connection_id=destination_connection_id,
            dlt_config=dlt_config,
            status=UploadStatus.DRAFT,
        )
        session.add(upload)
        session.flush()
        return upload

    def get_upload(self, session: Session, org_id: int, upload_id: int) -> Upload | None:
        stmt = select(Upload).where(
            Upload.id == upload_id,
            Upload.org_id == org_id,
            Upload.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    def list_uploads(self, session: Session, org_id: int) -> list[Upload]:
        stmt = (
            select(Upload)
            .where(Upload.org_id == org_id, Upload.deleted_at.is_(None))
            .order_by(Upload.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def update_upload(
        self, session: Session, org_id: int, upload_id: int, **kwargs
    ) -> Upload | None:
        upload = self.get_upload(session, org_id, upload_id)
        if upload is None:
            return None

        if "dlt_config" in kwargs:
            self.validate_upload_config(kwargs["dlt_config"])
            upload.dlt_config = kwargs["dlt_config"]
        if "name" in kwargs:
            validate_upload_name(kwargs["name"])
            upload.name = kwargs["name"]
        if "description" in kwargs:
            upload.description = kwargs["description"]
        if "status" in kwargs:
            upload.status = kwargs["status"]

        session.flush()
        return upload

    def delete_upload(self, session: Session, org_id: int, upload_id: int) -> bool:
        upload = self.get_upload(session, org_id, upload_id)
        if upload is None:
            return False
        upload.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    @staticmethod
    def validate_upload_config(dlt_config) -> None:
        if not isinstance(dlt_config, dict):
            raise UploadConfigError("dlt_config must be a dict")

        if not dlt_config:
            return  # empty config is valid

        disposition = dlt_config.get("write_disposition")
        if disposition is not None and disposition not in VALID_WRITE_DISPOSITIONS:
            raise UploadConfigError(
                f"write_disposition must be one of {VALID_WRITE_DISPOSITIONS}, got '{disposition}'"
            )
        if disposition == "merge" and "primary_key" not in dlt_config:
            raise UploadConfigError("write_disposition 'merge' requires a 'primary_key' field")

        # -- mode validation --
        mode = dlt_config.get("mode", "full_database")
        if mode not in VALID_MODES:
            raise UploadConfigError(f"mode must be one of {VALID_MODES}, got '{mode}'")

        if mode == "single_table":
            if "table" not in dlt_config:
                raise UploadConfigError("single_table mode requires a 'table' field")
            if "table_names" in dlt_config:
                raise UploadConfigError("single_table mode does not accept 'table_names'")
            incremental = dlt_config.get("incremental")
            if incremental is not None:
                if not isinstance(incremental, dict) or "cursor_path" not in incremental:
                    raise UploadConfigError("incremental requires a 'cursor_path' field")
                row_order = incremental.get("row_order")
                if row_order is not None and row_order not in VALID_ROW_ORDERS:
                    raise UploadConfigError(
                        f"row_order must be one of {VALID_ROW_ORDERS}, got '{row_order}'"
                    )
        else:  # full_database
            if "table" in dlt_config:
                raise UploadConfigError("full_database mode does not accept 'table'")
            if "incremental" in dlt_config:
                raise UploadConfigError("full_database mode does not accept 'incremental'")
            table_names = dlt_config.get("table_names")
            if table_names is not None and not isinstance(table_names, list):
                raise UploadConfigError("table_names must be a list")

        # -- batch_size --
        batch_size = dlt_config.get("batch_size")
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise UploadConfigError("batch_size must be a positive integer")

        # -- source_schema --
        source_schema = dlt_config.get("source_schema")
        if source_schema is not None and not isinstance(source_schema, str):
            raise UploadConfigError("source_schema must be a string")

        # -- schema_contract --
        schema_contract = dlt_config.get("schema_contract")
        if schema_contract is not None:
            if not isinstance(schema_contract, dict):
                raise UploadConfigError("schema_contract must be a dict")
            for entity, value in schema_contract.items():
                if entity not in VALID_SCHEMA_CONTRACT_ENTITIES:
                    raise UploadConfigError(
                        f"schema_contract key '{entity}' not in {VALID_SCHEMA_CONTRACT_ENTITIES}"
                    )
                if value not in VALID_SCHEMA_CONTRACT_VALUES:
                    raise UploadConfigError(
                        f"schema_contract value '{value}' not in {VALID_SCHEMA_CONTRACT_VALUES}"
                    )

        # -- filters --
        filters = dlt_config.get("filters")
        if filters is not None:
            if not isinstance(filters, list):
                raise UploadConfigError("filters must be a list")
            for f in filters:
                if not isinstance(f, dict):
                    raise UploadConfigError("Each filter must be a dict")
                for required in ("column", "op", "value"):
                    if required not in f:
                        raise UploadConfigError(f"Each filter requires '{required}'")
                if f["op"] not in VALID_FILTER_OPS:
                    raise UploadConfigError(
                        f"Filter op must be one of {VALID_FILTER_OPS}, got '{f['op']}'"
                    )
