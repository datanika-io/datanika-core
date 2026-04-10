"""Backup & restore service.

Export/import connections, uploads, pipelines, and transformations as JSON.
"""

import enum
import re
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.connection import Connection, ConnectionType
from datanika.models.pipeline import DbtCommand, Pipeline, PipelineStatus
from datanika.models.transformation import Materialization, Transformation
from datanika.models.upload import Upload, UploadStatus
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService

SENSITIVE_KEYS = {"password", "aws_secret_access_key", "service_account_json", "api_key"}
BACKUP_VERSION = 2

_TRANSFORMATION_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


class ImportErrorCode(enum.StrEnum):
    MISSING_FIELD = "MISSING_FIELD"
    EMPTY_FIELD = "EMPTY_FIELD"
    INVALID_CONNECTION_TYPE = "INVALID_CONNECTION_TYPE"
    INVALID_ENUM_VALUE = "INVALID_ENUM_VALUE"
    INVALID_NAME_FORMAT = "INVALID_NAME_FORMAT"
    DUPLICATE_NAME = "DUPLICATE_NAME"
    UNKNOWN_CONNECTION_REF = "UNKNOWN_CONNECTION_REF"
    DIRECTION_MISMATCH = "DIRECTION_MISMATCH"


class ImportValidationError(ValueError):
    """Raised when backup data fails validation. Contains all collected errors."""

    def __init__(self, errors: list[dict]):
        self.errors = errors
        messages = "; ".join(e["message"] for e in errors)
        super().__init__(f"Import validation failed ({len(errors)} errors): {messages}")


class BackupService:
    @staticmethod
    def validate_backup(session: Session, org_id: int, data: dict) -> None:
        """Validate backup data without writing to DB. Raises ImportValidationError if invalid."""
        errors: list[dict] = []

        def _err(code: str, entity_type: str, index: int, field: str, message: str):
            errors.append(
                {
                    "code": code,
                    "entity_type": entity_type,
                    "index": index,
                    "field": field,
                    "message": message,
                }
            )

        def _check_enum(value, enum_cls, entity_type, index, field):
            """Return True if value is a valid enum member string."""
            try:
                enum_cls(value)
                return True
            except (ValueError, KeyError):
                _err(
                    ImportErrorCode.INVALID_ENUM_VALUE,
                    entity_type,
                    index,
                    field,
                    f"Invalid {field} '{value}' for {entity_type}[{index}]",
                )
                return False

        # Build set of known connection names (from file + DB) for reference checks
        file_conn_names: set[str] = set()
        for c in data.get("connections", []):
            name = c.get("name")
            if isinstance(name, str) and name.strip():
                file_conn_names.add(name.strip())

        db_conn_names: set[str] = set()
        rows = session.execute(
            select(Connection.name).where(
                Connection.org_id == org_id, Connection.deleted_at.is_(None)
            )
        ).all()
        for (name,) in rows:
            db_conn_names.add(name)

        known_conn_names = file_conn_names | db_conn_names

        def _check_required_str(entry, field, entity_type, index) -> bool:
            """Check field exists and is non-empty. Returns True if valid."""
            val = entry.get(field)
            if val is None or (not isinstance(val, str)):
                _err(
                    ImportErrorCode.MISSING_FIELD,
                    entity_type,
                    index,
                    field,
                    f"Missing required field '{field}' in {entity_type}[{index}]",
                )
                return False
            if not val.strip():
                _err(
                    ImportErrorCode.EMPTY_FIELD,
                    entity_type,
                    index,
                    field,
                    f"Field '{field}' is empty in {entity_type}[{index}]",
                )
                return False
            return True

        def _check_ref(
            conn_name: str,
            entity_type: str,
            index: int,
            field: str,
        ) -> None:
            """Check connection reference exists."""
            if conn_name not in known_conn_names:
                _err(
                    ImportErrorCode.UNKNOWN_CONNECTION_REF,
                    entity_type,
                    index,
                    field,
                    f"Unknown connection '{conn_name}' referenced in {entity_type}[{index}]",
                )

        # --- Validate connections ---
        seen_conn_names: set[str] = set()
        for i, c in enumerate(data.get("connections", [])):
            name_valid = _check_required_str(c, "name", "connection", i)

            # connection_type
            ct = c.get("connection_type")
            if ct is None:
                _err(
                    ImportErrorCode.MISSING_FIELD,
                    "connection",
                    i,
                    "connection_type",
                    f"Missing required field 'connection_type' in connection[{i}]",
                )
            else:
                try:
                    ConnectionType(ct)
                except (ValueError, KeyError):
                    _err(
                        ImportErrorCode.INVALID_CONNECTION_TYPE,
                        "connection",
                        i,
                        "connection_type",
                        f"Invalid connection_type '{ct}' in connection[{i}]",
                    )

            # config
            if c.get("config") is None:
                _err(
                    ImportErrorCode.MISSING_FIELD,
                    "connection",
                    i,
                    "config",
                    f"Missing required field 'config' in connection[{i}]",
                )

            # duplicate name
            if name_valid:
                name = c["name"].strip()
                if name in seen_conn_names:
                    _err(
                        ImportErrorCode.DUPLICATE_NAME,
                        "connection",
                        i,
                        "name",
                        f"Duplicate connection name '{name}' at index {i}",
                    )
                seen_conn_names.add(name)

        # --- Validate uploads ---
        seen_upload_names: set[str] = set()
        for i, u in enumerate(data.get("uploads", [])):
            name_valid = _check_required_str(u, "name", "upload", i)

            src_name = u.get("source_connection_name")
            if src_name is None:
                _err(
                    ImportErrorCode.MISSING_FIELD,
                    "upload",
                    i,
                    "source_connection_name",
                    f"Missing required field 'source_connection_name' in upload[{i}]",
                )
            else:
                _check_ref(src_name, "upload", i, "source_connection_name")

            dst_name = u.get("destination_connection_name")
            if dst_name is None:
                _err(
                    ImportErrorCode.MISSING_FIELD,
                    "upload",
                    i,
                    "destination_connection_name",
                    f"Missing required field 'destination_connection_name' in upload[{i}]",
                )
            else:
                _check_ref(dst_name, "upload", i, "destination_connection_name")

            # status (optional)
            status = u.get("status")
            if status is not None:
                _check_enum(status, UploadStatus, "upload", i, "status")

            # duplicate name
            if name_valid:
                name = u["name"].strip()
                if name in seen_upload_names:
                    _err(
                        ImportErrorCode.DUPLICATE_NAME,
                        "upload",
                        i,
                        "name",
                        f"Duplicate upload name '{name}' at index {i}",
                    )
                seen_upload_names.add(name)

        # --- Validate pipelines ---
        seen_pipeline_names: set[str] = set()
        for i, p in enumerate(data.get("pipelines", [])):
            name_valid = _check_required_str(p, "name", "pipeline", i)

            dst_name = p.get("destination_connection_name")
            if dst_name is None:
                _err(
                    ImportErrorCode.MISSING_FIELD,
                    "pipeline",
                    i,
                    "destination_connection_name",
                    f"Missing required field 'destination_connection_name' in pipeline[{i}]",
                )
            else:
                _check_ref(dst_name, "pipeline", i, "destination_connection_name")

            # command (optional)
            command = p.get("command")
            if command is not None:
                _check_enum(command, DbtCommand, "pipeline", i, "command")

            # status (optional)
            status = p.get("status")
            if status is not None:
                _check_enum(status, PipelineStatus, "pipeline", i, "status")

            # duplicate name
            if name_valid:
                name = p["name"].strip()
                if name in seen_pipeline_names:
                    _err(
                        ImportErrorCode.DUPLICATE_NAME,
                        "pipeline",
                        i,
                        "name",
                        f"Duplicate pipeline name '{name}' at index {i}",
                    )
                seen_pipeline_names.add(name)

        # --- Validate transformations ---
        seen_transform_names: set[str] = set()
        for i, t in enumerate(data.get("transformations", [])):
            name_valid = _check_required_str(t, "name", "transformation", i)

            # Additional name format check
            if name_valid:
                name = t["name"].strip()
                if not _TRANSFORMATION_NAME_RE.match(name):
                    _err(
                        ImportErrorCode.INVALID_NAME_FORMAT,
                        "transformation",
                        i,
                        "name",
                        f"Invalid transformation name format '{name}' at index {i}",
                    )

            _check_required_str(t, "sql_body", "transformation", i)

            # materialization (optional)
            mat = t.get("materialization")
            if mat is not None:
                _check_enum(mat, Materialization, "transformation", i, "materialization")

            # destination_connection_name (optional)
            dst_name = t.get("destination_connection_name")
            if dst_name is not None:
                _check_ref(dst_name, "transformation", i, "destination_connection_name")

            # duplicate name
            if name_valid:
                name = t["name"].strip()
                if name in seen_transform_names:
                    _err(
                        ImportErrorCode.DUPLICATE_NAME,
                        "transformation",
                        i,
                        "name",
                        f"Duplicate transformation name '{name}' at index {i}",
                    )
                seen_transform_names.add(name)

        if errors:
            raise ImportValidationError(errors)

    @staticmethod
    def export_backup(session: Session, org_id: int, encryption: EncryptionService) -> dict:
        """Export all non-deleted connections and uploads for an org.

        Sensitive credential values are replaced with ``"CHANGE_ME"``.
        """
        conns = list(
            session.execute(
                select(Connection).where(
                    Connection.org_id == org_id, Connection.deleted_at.is_(None)
                )
            )
            .scalars()
            .all()
        )

        conn_id_to_name: dict[int, str] = {}
        exported_conns: list[dict] = []
        for c in conns:
            conn_id_to_name[c.id] = c.name
            config = encryption.decrypt(c.config_encrypted)
            masked = {k: ("CHANGE_ME" if k in SENSITIVE_KEYS else v) for k, v in config.items()}
            exported_conns.append(
                {
                    "name": c.name,
                    "connection_type": c.connection_type.value,
                    "config": masked,
                    "freshness_config": c.freshness_config,
                }
            )

        uploads = list(
            session.execute(
                select(Upload).where(Upload.org_id == org_id, Upload.deleted_at.is_(None))
            )
            .scalars()
            .all()
        )

        exported_uploads: list[dict] = []
        for u in uploads:
            exported_uploads.append(
                {
                    "name": u.name,
                    "description": u.description,
                    "source_connection_name": conn_id_to_name.get(u.source_connection_id, ""),
                    "destination_connection_name": conn_id_to_name.get(
                        u.destination_connection_id, ""
                    ),
                    "dlt_config": u.dlt_config,
                    "status": u.status.value,
                }
            )

        pipelines = list(
            session.execute(
                select(Pipeline).where(Pipeline.org_id == org_id, Pipeline.deleted_at.is_(None))
            )
            .scalars()
            .all()
        )

        exported_pipelines: list[dict] = []
        for p in pipelines:
            exported_pipelines.append(
                {
                    "name": p.name,
                    "description": p.description,
                    "destination_connection_name": conn_id_to_name.get(
                        p.destination_connection_id, ""
                    ),
                    "command": p.command.value,
                    "full_refresh": p.full_refresh,
                    "models": p.models,
                    "custom_selector": p.custom_selector,
                    "status": p.status.value,
                }
            )

        transformations = list(
            session.execute(
                select(Transformation).where(
                    Transformation.org_id == org_id, Transformation.deleted_at.is_(None)
                )
            )
            .scalars()
            .all()
        )

        exported_transformations: list[dict] = []
        for t in transformations:
            entry: dict = {
                "name": t.name,
                "description": t.description,
                "sql_body": t.sql_body,
                "materialization": t.materialization.value,
                "schema_name": t.schema_name,
                "tests_config": t.tests_config,
                "tags": t.tags,
            }
            if t.destination_connection_id:
                entry["destination_connection_name"] = conn_id_to_name.get(
                    t.destination_connection_id, ""
                )
            if t.incremental_config:
                entry["incremental_config"] = t.incremental_config
            exported_transformations.append(entry)

        return {
            "version": BACKUP_VERSION,
            "exported_at": datetime.now(UTC).isoformat(),
            "connections": exported_conns,
            "uploads": exported_uploads,
            "pipelines": exported_pipelines,
            "transformations": exported_transformations,
        }

    @staticmethod
    def import_backup(
        session: Session,
        org_id: int,
        encryption: EncryptionService,
        conn_svc: ConnectionService,
        upload_svc: UploadService,
        data: dict,
        conflict_resolutions: dict,
        pipeline_svc: PipelineService | None = None,
        transformation_svc: TransformationService | None = None,
    ) -> dict:
        """Import connections, uploads, pipelines, and transformations from a backup dict.

        ``conflict_resolutions`` maps ``("connection"|"upload"|"pipeline"|"transformation", name)``
        to ``"skip"|"overwrite"|"rename"``.

        Returns counts of imported and skipped items.
        """
        version = data.get("version")
        if version not in (1, BACKUP_VERSION):
            raise ValueError(f"Unsupported backup version {version}, expected {BACKUP_VERSION}")

        BackupService.validate_backup(session, org_id, data)

        existing_conns = conn_svc.list_connections(session, org_id)
        conn_name_to_id: dict[str, int] = {c.name: c.id for c in existing_conns}

        existing_uploads = upload_svc.list_uploads(session, org_id)
        upload_name_to_id: dict[str, int] = {u.name: u.id for u in existing_uploads}

        # Map from backup connection name -> resolved DB id
        name_to_new_id: dict[str, int] = {}
        imported_conns = 0
        skipped = 0

        for c_data in data.get("connections", []):
            name = c_data["name"]
            resolution = conflict_resolutions.get(("connection", name))

            if name in conn_name_to_id:
                if resolution == "skip":
                    name_to_new_id[name] = conn_name_to_id[name]
                    skipped += 1
                    continue
                elif resolution == "overwrite":
                    conn_svc.update_connection(
                        session,
                        org_id,
                        conn_name_to_id[name],
                        name=name,
                        connection_type=ConnectionType(c_data["connection_type"]),
                        config=c_data["config"],
                    )
                    name_to_new_id[name] = conn_name_to_id[name]
                    imported_conns += 1
                    continue
                elif resolution == "rename":
                    name = f"{name} Copy"
                else:
                    # No resolution provided but conflict exists — skip by default
                    name_to_new_id[name] = conn_name_to_id[name]
                    skipped += 1
                    continue

            conn = conn_svc.create_connection(
                session,
                org_id,
                name,
                ConnectionType(c_data["connection_type"]),
                c_data["config"],
            )
            # Map the original backup name to the new ID
            name_to_new_id[c_data["name"]] = conn.id
            imported_conns += 1

        # Also include pre-existing connections for reference resolution
        for c in existing_conns:
            if c.name not in name_to_new_id:
                name_to_new_id[c.name] = c.id

        imported_uploads = 0

        for u_data in data.get("uploads", []):
            uname = u_data["name"]
            resolution = conflict_resolutions.get(("upload", uname))

            src_name = u_data["source_connection_name"]
            dst_name = u_data["destination_connection_name"]

            src_id = name_to_new_id.get(src_name)
            if src_id is None:
                raise ValueError(
                    f"Upload '{uname}' references unknown source connection '{src_name}'"
                )
            dst_id = name_to_new_id.get(dst_name)
            if dst_id is None:
                raise ValueError(
                    f"Upload '{uname}' references unknown destination connection '{dst_name}'"
                )

            if uname in upload_name_to_id:
                if resolution == "skip":
                    skipped += 1
                    continue
                elif resolution == "overwrite":
                    upload_svc.update_upload(
                        session,
                        org_id,
                        upload_name_to_id[uname],
                        name=uname,
                        description=u_data.get("description"),
                        dlt_config=u_data.get("dlt_config", {}),
                        status=UploadStatus(u_data.get("status", "draft")),
                    )
                    imported_uploads += 1
                    continue
                elif resolution == "rename":
                    uname = f"{uname} Copy"
                else:
                    skipped += 1
                    continue

            upload_svc.create_upload(
                session,
                org_id,
                uname,
                u_data.get("description"),
                src_id,
                dst_id,
                u_data.get("dlt_config", {}),
            )
            imported_uploads += 1

        # --- Import pipelines ---------------------------------------------------
        imported_pipelines = 0
        if pipeline_svc is None:
            pipeline_svc = PipelineService()

        existing_pipelines = pipeline_svc.list_pipelines(session, org_id)
        pipeline_name_to_id: dict[str, int] = {p.name: p.id for p in existing_pipelines}

        for p_data in data.get("pipelines", []):
            pname = p_data["name"]
            resolution = conflict_resolutions.get(("pipeline", pname))

            dst_name = p_data.get("destination_connection_name", "")
            dst_id = name_to_new_id.get(dst_name)
            if dst_id is None:
                raise ValueError(
                    f"Pipeline '{pname}' references unknown destination connection '{dst_name}'"
                )

            if pname in pipeline_name_to_id:
                if resolution == "skip":
                    skipped += 1
                    continue
                elif resolution == "overwrite":
                    pipeline_svc.update_pipeline(
                        session,
                        org_id,
                        pipeline_name_to_id[pname],
                        name=pname,
                        description=p_data.get("description"),
                        destination_connection_id=dst_id,
                        command=DbtCommand(p_data.get("command", "run")),
                        full_refresh=p_data.get("full_refresh", False),
                        models=p_data.get("models", []),
                        custom_selector=p_data.get("custom_selector"),
                    )
                    imported_pipelines += 1
                    continue
                elif resolution == "rename":
                    pname = f"{pname} Copy"
                else:
                    skipped += 1
                    continue

            pipeline_svc.create_pipeline(
                session,
                org_id,
                pname,
                p_data.get("description"),
                dst_id,
                DbtCommand(p_data.get("command", "run")),
                full_refresh=p_data.get("full_refresh", False),
                models=p_data.get("models"),
                custom_selector=p_data.get("custom_selector"),
            )
            imported_pipelines += 1

        # --- Import transformations ---------------------------------------------
        imported_transformations = 0
        if transformation_svc is None:
            transformation_svc = TransformationService()

        existing_transforms = transformation_svc.list_transformations(session, org_id)
        transform_name_to_id: dict[str, int] = {t.name: t.id for t in existing_transforms}

        for t_data in data.get("transformations", []):
            tname = t_data["name"]
            resolution = conflict_resolutions.get(("transformation", tname))

            dst_name = t_data.get("destination_connection_name")
            dst_id = name_to_new_id.get(dst_name) if dst_name else None

            if tname in transform_name_to_id:
                if resolution == "skip":
                    skipped += 1
                    continue
                elif resolution == "overwrite":
                    transformation_svc.update_transformation(
                        session,
                        org_id,
                        transform_name_to_id[tname],
                        name=tname,
                        sql_body=t_data["sql_body"],
                        materialization=Materialization(t_data.get("materialization", "view")),
                        description=t_data.get("description"),
                        schema_name=t_data.get("schema_name", "staging"),
                        tests_config=t_data.get("tests_config", {}),
                        destination_connection_id=dst_id,
                        tags=t_data.get("tags", []),
                        incremental_config=t_data.get("incremental_config"),
                    )
                    imported_transformations += 1
                    continue
                elif resolution == "rename":
                    tname = f"{tname} Copy"
                else:
                    skipped += 1
                    continue

            transformation_svc.create_transformation(
                session,
                org_id,
                tname,
                t_data["sql_body"],
                Materialization(t_data.get("materialization", "view")),
                description=t_data.get("description"),
                schema_name=t_data.get("schema_name", "staging"),
                tests_config=t_data.get("tests_config"),
                destination_connection_id=dst_id,
                tags=t_data.get("tags"),
                incremental_config=t_data.get("incremental_config"),
            )
            imported_transformations += 1

        return {
            "connections_imported": imported_conns,
            "uploads_imported": imported_uploads,
            "pipelines_imported": imported_pipelines,
            "transformations_imported": imported_transformations,
            "skipped": skipped,
        }

    @staticmethod
    def detect_conflicts(session: Session, org_id: int, data: dict) -> list[dict]:
        """Return list of ``{"type": ..., "name": ...}`` for items that already exist."""
        existing_conn_names = {
            row[0]
            for row in session.execute(
                select(Connection.name).where(
                    Connection.org_id == org_id, Connection.deleted_at.is_(None)
                )
            ).all()
        }
        existing_upload_names = {
            row[0]
            for row in session.execute(
                select(Upload.name).where(Upload.org_id == org_id, Upload.deleted_at.is_(None))
            ).all()
        }
        existing_pipeline_names = {
            row[0]
            for row in session.execute(
                select(Pipeline.name).where(
                    Pipeline.org_id == org_id, Pipeline.deleted_at.is_(None)
                )
            ).all()
        }
        existing_transform_names = {
            row[0]
            for row in session.execute(
                select(Transformation.name).where(
                    Transformation.org_id == org_id, Transformation.deleted_at.is_(None)
                )
            ).all()
        }

        conflicts: list[dict] = []
        for c in data.get("connections", []):
            if c["name"] in existing_conn_names:
                conflicts.append({"type": "connection", "name": c["name"]})
        for u in data.get("uploads", []):
            if u["name"] in existing_upload_names:
                conflicts.append({"type": "upload", "name": u["name"]})
        for p in data.get("pipelines", []):
            if p["name"] in existing_pipeline_names:
                conflicts.append({"type": "pipeline", "name": p["name"]})
        for t in data.get("transformations", []):
            if t["name"] in existing_transform_names:
                conflicts.append({"type": "transformation", "name": t["name"]})
        return conflicts
