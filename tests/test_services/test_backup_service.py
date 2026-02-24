"""Tests for BackupService — export, import, and conflict detection."""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.user import Organization
from datanika.services.backup_service import BackupService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.upload_service import UploadService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def upload_svc(conn_svc):
    return UploadService(conn_svc)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-backup")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def sample_connections(db_session, conn_svc, org):
    """Create two connections: a source and a destination."""
    src = conn_svc.create_connection(
        db_session,
        org.id,
        "My Postgres",
        ConnectionType.POSTGRES,
        ConnectionDirection.SOURCE,
        {"host": "localhost", "port": 5432, "user": "admin", "password": "secret123"},
    )
    dst = conn_svc.create_connection(
        db_session,
        org.id,
        "Target DWH",
        ConnectionType.BIGQUERY,
        ConnectionDirection.DESTINATION,
        {"project": "my-proj", "dataset": "raw", "service_account_json": '{"key": "val"}'},
    )
    return src, dst


@pytest.fixture
def sample_upload(db_session, upload_svc, org, sample_connections):
    src, dst = sample_connections
    return upload_svc.create_upload(
        db_session,
        org.id,
        "Daily Sync",
        "Full database sync",
        src.id,
        dst.id,
        {"mode": "full_database", "write_disposition": "append"},
    )


class TestExportBackup:
    def test_export_masks_sensitive_fields(self, db_session, encryption, org, sample_connections):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        conns = backup["connections"]
        pg = next(c for c in conns if c["name"] == "My Postgres")
        assert pg["config"]["password"] == "CHANGE_ME"
        assert pg["config"]["host"] == "localhost"
        assert pg["config"]["port"] == 5432

        bq = next(c for c in conns if c["name"] == "Target DWH")
        assert bq["config"]["service_account_json"] == "CHANGE_ME"
        assert bq["config"]["project"] == "my-proj"

    def test_export_includes_all_connections(self, db_session, encryption, org, sample_connections):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        names = {c["name"] for c in backup["connections"]}
        assert names == {"My Postgres", "Target DWH"}

    def test_export_includes_all_uploads(self, db_session, encryption, org, sample_upload):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        assert len(backup["uploads"]) == 1
        assert backup["uploads"][0]["name"] == "Daily Sync"

    def test_export_uploads_reference_by_name(self, db_session, encryption, org, sample_upload):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        up = backup["uploads"][0]
        assert up["source_connection_name"] == "My Postgres"
        assert up["destination_connection_name"] == "Target DWH"
        assert "source_connection_id" not in up

    def test_export_excludes_deleted(
        self, db_session, encryption, conn_svc, org, sample_connections, sample_upload
    ):
        src, dst = sample_connections
        conn_svc.delete_connection(db_session, org.id, src.id)
        backup = BackupService.export_backup(db_session, org.id, encryption)
        names = {c["name"] for c in backup["connections"]}
        assert "My Postgres" not in names
        # Upload referencing deleted connection should still be excluded
        # (its source connection is gone, but we only export non-deleted uploads)
        # The upload itself is not deleted, so it remains
        assert len(backup["uploads"]) == 1

    def test_export_has_version_and_timestamp(
        self, db_session, encryption, org, sample_connections
    ):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        assert backup["version"] == 1
        assert "exported_at" in backup


class TestImportBackup:
    def _make_backup(self, connections=None, uploads=None, version=1):
        return {
            "version": version,
            "exported_at": "2026-02-24T12:00:00Z",
            "connections": connections or [],
            "uploads": uploads or [],
        }

    def test_import_creates_connections_and_uploads(
        self, db_session, encryption, conn_svc, upload_svc, org
    ):
        data = self._make_backup(
            connections=[
                {
                    "name": "Src",
                    "connection_type": "postgres",
                    "direction": "source",
                    "config": {"host": "localhost", "port": 5432},
                    "freshness_config": None,
                },
                {
                    "name": "Dst",
                    "connection_type": "bigquery",
                    "direction": "destination",
                    "config": {"project": "p", "dataset": "d"},
                    "freshness_config": None,
                },
            ],
            uploads=[
                {
                    "name": "My Upload",
                    "description": "desc",
                    "source_connection_name": "Src",
                    "destination_connection_name": "Dst",
                    "dlt_config": {},
                    "status": "draft",
                },
            ],
        )
        result = BackupService.import_backup(
            db_session, org.id, encryption, conn_svc, upload_svc, data, {}
        )
        assert result["connections_imported"] == 2
        assert result["uploads_imported"] == 1
        assert result["skipped"] == 0

    def test_import_resolves_connection_references(
        self, db_session, encryption, conn_svc, upload_svc, org
    ):
        data = self._make_backup(
            connections=[
                {
                    "name": "A",
                    "connection_type": "postgres",
                    "direction": "source",
                    "config": {"host": "h"},
                    "freshness_config": None,
                },
                {
                    "name": "B",
                    "connection_type": "postgres",
                    "direction": "destination",
                    "config": {"host": "h"},
                    "freshness_config": None,
                },
            ],
            uploads=[
                {
                    "name": "Up1",
                    "description": None,
                    "source_connection_name": "A",
                    "destination_connection_name": "B",
                    "dlt_config": {},
                    "status": "draft",
                },
            ],
        )
        BackupService.import_backup(db_session, org.id, encryption, conn_svc, upload_svc, data, {})
        uploads = upload_svc.list_uploads(db_session, org.id)
        assert len(uploads) == 1
        up = uploads[0]
        # Verify FKs point to real connections
        src = conn_svc.get_connection(db_session, org.id, up.source_connection_id)
        dst = conn_svc.get_connection(db_session, org.id, up.destination_connection_id)
        assert src is not None and src.name == "A"
        assert dst is not None and dst.name == "B"

    def test_import_skip_conflict(self, db_session, encryption, conn_svc, upload_svc, org):
        # Pre-create a connection with the same name
        conn_svc.create_connection(
            db_session,
            org.id,
            "Existing",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "old"},
        )
        data = self._make_backup(
            connections=[
                {
                    "name": "Existing",
                    "connection_type": "mysql",
                    "direction": "source",
                    "config": {"host": "new"},
                    "freshness_config": None,
                },
            ],
        )
        result = BackupService.import_backup(
            db_session,
            org.id,
            encryption,
            conn_svc,
            upload_svc,
            data,
            {("connection", "Existing"): "skip"},
        )
        assert result["skipped"] == 1
        assert result["connections_imported"] == 0
        # Original connection is unchanged
        conns = conn_svc.list_connections(db_session, org.id)
        assert len(conns) == 1
        cfg = encryption.decrypt(conns[0].config_encrypted)
        assert cfg["host"] == "old"

    def test_import_overwrite_conflict(self, db_session, encryption, conn_svc, upload_svc, org):
        existing = conn_svc.create_connection(
            db_session,
            org.id,
            "Overwrite Me",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "old"},
        )
        data = self._make_backup(
            connections=[
                {
                    "name": "Overwrite Me",
                    "connection_type": "mysql",
                    "direction": "source",
                    "config": {"host": "new"},
                    "freshness_config": None,
                },
            ],
        )
        result = BackupService.import_backup(
            db_session,
            org.id,
            encryption,
            conn_svc,
            upload_svc,
            data,
            {("connection", "Overwrite Me"): "overwrite"},
        )
        assert result["connections_imported"] == 1
        conn = conn_svc.get_connection(db_session, org.id, existing.id)
        assert conn.connection_type == ConnectionType.MYSQL
        cfg = encryption.decrypt(conn.config_encrypted)
        assert cfg["host"] == "new"

    def test_import_rename_conflict(self, db_session, encryption, conn_svc, upload_svc, org):
        conn_svc.create_connection(
            db_session,
            org.id,
            "Dupe",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "old"},
        )
        data = self._make_backup(
            connections=[
                {
                    "name": "Dupe",
                    "connection_type": "mysql",
                    "direction": "source",
                    "config": {"host": "new"},
                    "freshness_config": None,
                },
            ],
        )
        result = BackupService.import_backup(
            db_session,
            org.id,
            encryption,
            conn_svc,
            upload_svc,
            data,
            {("connection", "Dupe"): "rename"},
        )
        assert result["connections_imported"] == 1
        conns = conn_svc.list_connections(db_session, org.id)
        names = {c.name for c in conns}
        assert "Dupe" in names
        assert "Dupe Copy" in names

    def test_import_invalid_version_raises(self, db_session, encryption, conn_svc, upload_svc, org):
        data = self._make_backup(version=99)
        with pytest.raises(ValueError, match="version"):
            BackupService.import_backup(
                db_session, org.id, encryption, conn_svc, upload_svc, data, {}
            )

    def test_import_missing_connection_reference(
        self, db_session, encryption, conn_svc, upload_svc, org
    ):
        data = self._make_backup(
            connections=[
                {
                    "name": "OnlySrc",
                    "connection_type": "postgres",
                    "direction": "source",
                    "config": {"host": "h"},
                    "freshness_config": None,
                },
            ],
            uploads=[
                {
                    "name": "Broken",
                    "description": None,
                    "source_connection_name": "OnlySrc",
                    "destination_connection_name": "NonExistent",
                    "dlt_config": {},
                    "status": "draft",
                },
            ],
        )
        with pytest.raises(ValueError, match="NonExistent"):
            BackupService.import_backup(
                db_session, org.id, encryption, conn_svc, upload_svc, data, {}
            )


class TestDetectConflicts:
    def test_detect_conflicts_finds_duplicates(
        self, db_session, conn_svc, upload_svc, encryption, org
    ):
        src = conn_svc.create_connection(
            db_session,
            org.id,
            "PG",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "h"},
        )
        dst = conn_svc.create_connection(
            db_session,
            org.id,
            "BQ",
            ConnectionType.BIGQUERY,
            ConnectionDirection.DESTINATION,
            {"project": "p", "dataset": "d"},
        )
        upload_svc.create_upload(db_session, org.id, "My Upload", None, src.id, dst.id, {})
        data = {
            "version": 1,
            "connections": [
                {
                    "name": "PG",
                    "connection_type": "postgres",
                    "direction": "source",
                    "config": {},
                    "freshness_config": None,
                },
                {
                    "name": "New",
                    "connection_type": "mysql",
                    "direction": "source",
                    "config": {},
                    "freshness_config": None,
                },
            ],
            "uploads": [
                {
                    "name": "My Upload",
                    "description": None,
                    "source_connection_name": "PG",
                    "destination_connection_name": "BQ",
                    "dlt_config": {},
                    "status": "draft",
                },
            ],
        }
        conflicts = BackupService.detect_conflicts(db_session, org.id, data)
        types_names = {(c["type"], c["name"]) for c in conflicts}
        assert ("connection", "PG") in types_names
        assert ("upload", "My Upload") in types_names
        # "New" and "BQ" should NOT be in conflicts (New doesn't exist, BQ not in import)
        assert ("connection", "New") not in types_names
        assert ("connection", "BQ") not in types_names

    def test_detect_conflicts_empty_when_no_duplicates(self, db_session, encryption, org):
        data = {
            "version": 1,
            "connections": [
                {
                    "name": "Brand New",
                    "connection_type": "postgres",
                    "direction": "source",
                    "config": {},
                    "freshness_config": None,
                },
            ],
            "uploads": [],
        }
        conflicts = BackupService.detect_conflicts(db_session, org.id, data)
        assert conflicts == []
