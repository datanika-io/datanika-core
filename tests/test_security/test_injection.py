"""Security tests — SQL injection, command injection, dbt model name injection."""

import uuid

import pytest

from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.dbt_project import DbtProjectError, DbtProjectService
from datanika.services.encryption import EncryptionService
from datanika.services.naming import validate_name


@pytest.fixture
def encryption():
    return EncryptionService("NS7W71uT0X-FxSq_mwJfthZzc3hIatYjQHM3MhDnQX8=")


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def org(db_session):
    o = Organization(name="SecOrg", slug=f"sec-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


class TestConnectionNameInjection:
    def test_sql_injection_in_name_rejected(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name("'; DROP TABLE users; --", "Connection")

    def test_html_script_in_name_rejected(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name("<script>alert(1)</script>", "Connection")

    def test_semicolon_in_name_rejected(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name("test; DELETE FROM connections", "Connection")

    def test_backtick_injection_rejected(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name("test` OR 1=1 --", "Connection")

    def test_clean_name_accepted(self):
        validate_name("My Connection 1", "Connection")  # no raise


class TestConnectionConfigSafety:
    """Connection config is Fernet-encrypted, never interpolated into SQL."""

    def test_sql_payload_in_config_encrypted_safely(self, encryption):
        malicious = {"host": "'; DROP TABLE users; --", "port": 5432}
        encrypted = encryption.encrypt(malicious)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted["host"] == "'; DROP TABLE users; --"
        # Value survives roundtrip — never parsed as SQL

    def test_config_with_shell_command_encrypted_safely(self, encryption):
        malicious = {"host": "$(rm -rf /)", "password": "; cat /etc/passwd"}
        encrypted = encryption.encrypt(malicious)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted["host"] == "$(rm -rf /)"


class TestDbtModelNameInjection:
    def test_path_traversal_in_model_name_rejected(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError, match="must start with"):
            svc.write_model(1, "../../../etc/passwd", "SELECT 1", "staging")

    def test_semicolon_in_model_name_rejected(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError, match="must start with"):
            svc.write_model(1, "model; DROP TABLE", "SELECT 1", "staging")

    def test_space_in_model_name_rejected(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError, match="must start with"):
            svc.write_model(1, "model name with spaces", "SELECT 1", "staging")

    def test_valid_model_name_accepted(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        path = svc.write_model(1, "valid_model", "SELECT 1", "staging")
        assert path.exists()
