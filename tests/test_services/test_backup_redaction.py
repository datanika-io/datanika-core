"""The backup format's redaction contract.

An export is presented to the user as a sanitized file. Two properties have to
hold for that to be true, and neither did:

1. **Nothing secret survives the export.** Asserted against the *serialized
   JSON*, not a dict — a credential nested inside a JSON-string value (a Google
   service-account keyfile) is invisible to ``backup["connections"][0]["config"]
   ["password"] == ...``.
2. **A redacted value round-trips.** Importing an export over the connection it
   came from must leave the stored credential untouched, rather than writing the
   placeholder through and destroying a working connection.
"""

import json

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionType
from datanika.models.user import Organization
from datanika.services.backup_service import (
    BACKUP_VERSION,
    REDACTED,
    BackupService,
)
from datanika.services.connection_service import SECRET_CONFIG_KEYS, ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.upload_service import UploadService


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def upload_svc(conn_svc):
    return UploadService(conn_svc)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme Redaction", slug="acme-redaction")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="Other Co", slug="other-co-redaction")
    db_session.add(org)
    db_session.flush()
    return org


#: One distinctive value per canonical secret key, so a leak names the key that
#: leaked. Derived from the canonical set rather than listed, so a key added
#: there is covered here automatically.
def _secret_values() -> dict[str, str]:
    return {key: f"SEKRIT-{key}-4f9a2b" for key in sorted(SECRET_CONFIG_KEYS)}


@pytest.fixture
def loaded_connection(db_session, conn_svc, org):
    """A connection carrying every canonical secret key, plus benign fields."""
    config = {"host": "db.example.com", "port": 5432, "database": "prod"}
    config.update(_secret_values())
    # The keyfile case: a secret nested inside a JSON-string value.
    config["keyfile_json"] = json.dumps(
        {"type": "service_account", "private_key": "SEKRIT-nested-private-key-4f9a2b"}
    )
    return conn_svc.create_connection(db_session, org.id, "Loaded", ConnectionType.POSTGRES, config)


class TestExportLeaksNothing:
    def test_no_secret_value_appears_anywhere_in_the_serialized_export(
        self, db_session, encryption, org, loaded_connection
    ):
        blob = json.dumps(BackupService.export_backup(db_session, org.id, encryption))
        leaked = sorted(k for k, v in _secret_values().items() if v in blob)
        assert not leaked, f"secret values present in plaintext in the export: {leaked}"
        assert "SEKRIT-nested-private-key-4f9a2b" not in blob, (
            "a credential nested inside a JSON-string config value survived the export"
        )

    def test_benign_fields_survive(self, db_session, encryption, org, loaded_connection):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        cfg = backup["connections"][0]["config"]
        assert cfg["host"] == "db.example.com"
        assert cfg["port"] == 5432

    def test_redacted_keys_are_marked_with_the_sentinel(
        self, db_session, encryption, org, loaded_connection
    ):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        cfg = backup["connections"][0]["config"]
        for key in _secret_values():
            assert cfg[key] == REDACTED, f"{key} was not marked redacted"


class TestRedactionRoundTrips:
    def test_overwrite_leaves_the_stored_credential_byte_identical(
        self, db_session, encryption, conn_svc, upload_svc, org, loaded_connection
    ):
        before = encryption.decrypt(loaded_connection.config_encrypted)
        backup = BackupService.export_backup(db_session, org.id, encryption)

        BackupService.import_backup(
            db_session,
            org.id,
            encryption,
            conn_svc,
            upload_svc,
            backup,
            {("connection", "Loaded"): "overwrite"},
        )

        after = encryption.decrypt(
            conn_svc.get_connection(db_session, org.id, loaded_connection.id).config_encrypted
        )
        assert after == before, "an export/import round trip changed the stored config"

    def test_creating_from_a_redacted_backup_omits_the_secret_rather_than_storing_the_sentinel(
        self, db_session, encryption, conn_svc, upload_svc, org, loaded_connection
    ):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        conn_svc.delete_connection(db_session, org.id, loaded_connection.id)

        result = BackupService.import_backup(
            db_session, org.id, encryption, conn_svc, upload_svc, backup, {}
        )

        created = next(c for c in conn_svc.list_connections(db_session, org.id))
        cfg = encryption.decrypt(created.config_encrypted)
        assert cfg["host"] == "db.example.com"
        for key in _secret_values():
            assert key not in cfg, (
                f"{key} was stored as the literal redaction placeholder; a connection that "
                "looks configured and cannot connect is worse than an empty field"
            )
        assert "Loaded" in result["credentials_required"]

    def test_a_connection_with_no_secrets_needs_no_credentials(
        self, db_session, encryption, conn_svc, upload_svc, org
    ):
        conn_svc.create_connection(
            db_session, org.id, "Plain", ConnectionType.POSTGRES, {"host": "h"}
        )
        backup = BackupService.export_backup(db_session, org.id, encryption)
        conn_svc.delete_connection(
            db_session, org.id, conn_svc.list_connections(db_session, org.id)[0].id
        )
        result = BackupService.import_backup(
            db_session, org.id, encryption, conn_svc, upload_svc, backup, {}
        )
        assert result["credentials_required"] == []

    def test_the_legacy_change_me_placeholder_is_also_treated_as_redacted(
        self, db_session, encryption, conn_svc, upload_svc, org
    ):
        """v1/v2 exports used ``CHANGE_ME``; importing one must not write it through."""
        existing = conn_svc.create_connection(
            db_session,
            org.id,
            "Legacy",
            ConnectionType.POSTGRES,
            {"host": "old", "password": "real-password"},
        )
        data = {
            "version": 2,
            "exported_at": "2026-02-24T12:00:00Z",
            "connections": [
                {
                    "name": "Legacy",
                    "connection_type": "postgres",
                    "config": {"host": "new", "password": "CHANGE_ME"},
                    "freshness_config": None,
                }
            ],
            "uploads": [],
        }
        BackupService.import_backup(
            db_session,
            org.id,
            encryption,
            conn_svc,
            upload_svc,
            data,
            {("connection", "Legacy"): "overwrite"},
        )
        cfg = encryption.decrypt(
            conn_svc.get_connection(db_session, org.id, existing.id).config_encrypted
        )
        assert cfg["host"] == "new", "the non-secret field should still be overwritten"
        assert cfg["password"] == "real-password", (
            "restoring a v2 backup overwrote a working credential with the placeholder"
        )

    @pytest.mark.parametrize("version", [1, 2, BACKUP_VERSION])
    def test_supported_versions_still_import(
        self, db_session, encryption, conn_svc, upload_svc, org, version
    ):
        data = {
            "version": version,
            "exported_at": "2026-02-24T12:00:00Z",
            "connections": [
                {
                    "name": f"V{version}",
                    "connection_type": "postgres",
                    "config": {"host": "h"},
                    "freshness_config": None,
                }
            ],
            "uploads": [],
        }
        result = BackupService.import_backup(
            db_session, org.id, encryption, conn_svc, upload_svc, data, {}
        )
        assert result["connections_imported"] == 1


class TestOrgProvenance:
    def test_export_names_the_org_it_came_from(self, db_session, encryption, org):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        assert backup["org"]["id"] == org.id
        assert backup["org"]["name"] == "Acme Redaction"
        assert backup["org"]["slug"] == "acme-redaction"

    def test_version_was_bumped_for_the_new_envelope(self):
        assert BACKUP_VERSION >= 3

    def test_same_org_is_not_foreign(self, db_session, encryption, org):
        backup = BackupService.export_backup(db_session, org.id, encryption)
        assert BackupService.foreign_org(backup, org.id) == ""

    def test_a_backup_from_another_org_is_reported(self, db_session, encryption, org, other_org):
        backup = BackupService.export_backup(db_session, other_org.id, encryption)
        assert BackupService.foreign_org(backup, org.id) == "Other Co"

    def test_a_v2_backup_without_provenance_is_not_reported_as_foreign(self, org):
        assert BackupService.foreign_org({"version": 2, "connections": []}, org.id) == ""

    def test_a_matching_id_from_a_different_deployment_is_still_foreign(self, db_session, org):
        """Org ids are per-deployment. Two installs both have an org 1."""
        elsewhere = {"version": 3, "org": {"id": org.id, "name": "Elsewhere", "slug": "elsewhere"}}
        assert BackupService.foreign_org(elsewhere, org.id) == "", "id-only comparison matches"
        assert BackupService.foreign_org(elsewhere, org.id, org.slug) == "Elsewhere"

    def test_a_foreign_backup_still_imports(
        self, db_session, encryption, conn_svc, upload_svc, org, other_org
    ):
        """Moving config between orgs is legitimate — the warning is a speed bump, not a wall."""
        conn_svc.create_connection(
            db_session, other_org.id, "Portable", ConnectionType.POSTGRES, {"host": "h"}
        )
        backup = BackupService.export_backup(db_session, other_org.id, encryption)
        result = BackupService.import_backup(
            db_session, org.id, encryption, conn_svc, upload_svc, backup, {}
        )
        assert result["connections_imported"] == 1
