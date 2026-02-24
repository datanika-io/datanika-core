"""Backup & restore service — export/import connections and uploads as JSON."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.upload import Upload, UploadStatus
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.upload_service import UploadService

SENSITIVE_KEYS = {"password", "aws_secret_access_key", "service_account_json", "api_key"}
BACKUP_VERSION = 1


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

        return {
            "version": BACKUP_VERSION,
            "exported_at": datetime.now(UTC).isoformat(),
            "connections": exported_conns,
            "uploads": exported_uploads,
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
    ) -> dict:
        """Import connections and uploads from a backup dict.

        ``conflict_resolutions`` maps ``("connection"|"upload", name)`` to
        ``"skip"|"overwrite"|"rename"``.

        Returns ``{"connections_imported": int, "uploads_imported": int, "skipped": int}``.
        """
        version = data.get("version")
        if version != BACKUP_VERSION:
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

        return {
            "connections_imported": imported_conns,
            "uploads_imported": imported_uploads,
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

        conflicts: list[dict] = []
        for c in data.get("connections", []):
            if c["name"] in existing_conn_names:
                conflicts.append({"type": "connection", "name": c["name"]})
        for u in data.get("uploads", []):
            if u["name"] in existing_upload_names:
                conflicts.append({"type": "upload", "name": u["name"]})
        return conflicts
