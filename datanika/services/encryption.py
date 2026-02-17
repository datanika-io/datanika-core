import json

from cryptography.fernet import Fernet, InvalidToken


class EncryptionError(ValueError):
    """Raised when encryption or decryption fails."""


class EncryptionService:
    def __init__(self, key: str):
        self._fernet = Fernet(key.encode("utf-8"))

    def encrypt(self, data: dict) -> str:
        raw = json.dumps(data).encode("utf-8")
        return self._fernet.encrypt(raw).decode("utf-8")

    def decrypt(self, encrypted: str) -> dict:
        try:
            raw = self._fernet.decrypt(encrypted.encode("utf-8"))
        except InvalidToken as exc:
            raise EncryptionError(
                "Failed to decrypt data — invalid key or corrupted data"
            ) from exc
        return json.loads(raw)
