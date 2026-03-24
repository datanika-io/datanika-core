"""Backup & restore service — export/import connections, uploads, pipelines, and transformations as JSON."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
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


class BackupService:
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
                    "direction": c.direction.value,
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
                select(Pipeline).where(
                    Pipeline.org_id == org_id, Pipeline.deleted_at.is_(None)
                )
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
                        direction=ConnectionDirection(c_data["direction"]),
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
                ConnectionDirection(c_data["direction"]),
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
                        materialization=Materialization(
                            t_data.get("materialization", "view")
                        ),
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
