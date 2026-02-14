"""TDD tests for connection management service."""

import pytest
from cryptography.fernet import Fernet

from etlfabric.models.connection import Connection, ConnectionDirection, ConnectionType
from etlfabric.models.user import Organization
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-conn-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-conn-svc")
    db_session.add(org)
    db_session.flush()
    return org


class TestCreateConnection:
    def test_returns_connection(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "My DB",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "localhost", "port": 5432},
        )
        assert isinstance(conn, Connection)
        assert isinstance(conn.id, int)
        assert conn.name == "My DB"
        assert conn.org_id == org.id

    def test_config_is_encrypted(self, svc, db_session, org, encryption):
        conn = svc.create_connection(
            db_session,
            org.id,
            "My DB",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "localhost"},
        )
        # The raw encrypted field should not contain the plaintext
        assert "localhost" not in conn.config_encrypted
        # But decrypting it should return the original
        decrypted = encryption.decrypt(conn.config_encrypted)
        assert decrypted == {"host": "localhost"}

    def test_sets_org_id(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        assert conn.org_id == org.id

    def test_sets_type_and_direction(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.REST_API,
            ConnectionDirection.BOTH,
            {},
        )
        assert conn.connection_type == ConnectionType.REST_API
        assert conn.direction == ConnectionDirection.BOTH


class TestGetConnection:
    def test_existing(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        fetched = svc.get_connection(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_connection(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        assert svc.get_connection(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.delete_connection(db_session, org.id, created.id)
        assert svc.get_connection(db_session, org.id, created.id) is None


class TestGetConnectionConfig:
    def test_decrypts_roundtrip(self, svc, db_session, org):
        config = {"host": "db.example.com", "port": 5432, "password": "s3cret"}
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            config,
        )
        result = svc.get_connection_config(db_session, org.id, created.id)
        assert result == config

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_connection_config(db_session, org.id, 99999) is None


class TestListConnections:
    def test_empty(self, svc, db_session, org):
        result = svc.list_connections(db_session, org.id)
        assert result == []

    def test_multiple(self, svc, db_session, org):
        svc.create_connection(
            db_session,
            org.id,
            "A",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.create_connection(
            db_session,
            org.id,
            "B",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        result = svc.list_connections(db_session, org.id)
        assert len(result) == 2

    def test_excludes_deleted(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "A",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.create_connection(
            db_session,
            org.id,
            "B",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        svc.delete_connection(db_session, org.id, created.id)
        result = svc.list_connections(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "B"

    def test_filters_by_org(self, svc, db_session, org, other_org):
        svc.create_connection(
            db_session,
            org.id,
            "A",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.create_connection(
            db_session,
            other_org.id,
            "B",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        result = svc.list_connections(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "A"


class TestUpdateConnection:
    def test_update_name(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "Old",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        updated = svc.update_connection(db_session, org.id, created.id, name="New")
        assert updated is not None
        assert updated.name == "New"

    def test_re_encrypts_config(self, svc, db_session, org, encryption):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "old"},
        )
        old_encrypted = created.config_encrypted
        updated = svc.update_connection(
            db_session,
            org.id,
            created.id,
            config={"host": "new"},
        )
        assert updated.config_encrypted != old_encrypted
        assert encryption.decrypt(updated.config_encrypted) == {"host": "new"}

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_connection(db_session, org.id, 99999, name="X") is None

    def test_preserves_unchanged_fields(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "Keep",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "keep"},
        )
        old_encrypted = created.config_encrypted
        updated = svc.update_connection(db_session, org.id, created.id, name="Changed")
        assert updated.name == "Changed"
        assert updated.config_encrypted == old_encrypted


class TestDeleteConnection:
    def test_sets_deleted_at(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        result = svc.delete_connection(db_session, org.id, created.id)
        assert result is True
        # Verify deleted_at is set by querying directly
        db_session.refresh(created)
        assert created.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        result = svc.delete_connection(db_session, org.id, 99999)
        assert result is False

    def test_idempotent(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.delete_connection(db_session, org.id, created.id)
        result = svc.delete_connection(db_session, org.id, created.id)
        assert result is False


class TestTestConnection:
    def test_basic_validation_success(self, svc):
        ok, msg = svc.test_connection(
            {"host": "localhost", "port": 5432},
            ConnectionType.POSTGRES,
        )
        assert ok is True

    def test_empty_config_fails(self, svc):
        ok, msg = svc.test_connection({}, ConnectionType.POSTGRES)
        assert ok is False
        assert len(msg) > 0
