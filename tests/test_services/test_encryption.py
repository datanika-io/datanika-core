"""TDD tests for credential encryption service."""

import cryptography.fernet
import pytest
from cryptography.fernet import Fernet

from datanika.services.encryption import EncryptionService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


class TestEncryptionService:
    def test_encrypt_returns_string(self, encryption):
        result = encryption.encrypt({"host": "localhost", "port": 5432})
        assert isinstance(result, str)

    def test_encrypted_is_not_plaintext(self, encryption):
        data = {"password": "super_secret"}
        encrypted = encryption.encrypt(data)
        assert "super_secret" not in encrypted

    def test_decrypt_roundtrip(self, encryption):
        original = {"host": "db.example.com", "port": 5432, "password": "s3cret"}
        encrypted = encryption.encrypt(original)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted == original

    def test_decrypt_with_wrong_key_fails(self, encryption):
        encrypted = encryption.encrypt({"key": "value"})
        other_key = Fernet.generate_key().decode()
        other = EncryptionService(other_key)
        with pytest.raises(cryptography.fernet.InvalidToken):
            other.decrypt(encrypted)

    def test_encrypt_empty_dict(self, encryption):
        encrypted = encryption.encrypt({})
        assert encryption.decrypt(encrypted) == {}

    def test_encrypt_nested_data(self, encryption):
        data = {"connection": {"host": "localhost", "options": {"ssl": True}}}
        encrypted = encryption.encrypt(data)
        assert encryption.decrypt(encrypted) == data
