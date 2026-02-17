from datetime import UTC, datetime, timedelta

import bcrypt
from jose import JWTError, jwt

ROLE_PERMISSIONS: dict[str, set[str]] = {
    "owner": {"create", "read", "update", "delete", "manage_members"},
    "admin": {"create", "read", "update", "delete"},
    "editor": {"create", "read", "update"},
    "viewer": {"read"},
}

ALGORITHM = "HS256"


class AuthService:
    def __init__(self, secret_key: str):
        self._secret_key = secret_key

    # -- Password hashing --

    def hash_password(self, password: str) -> str:
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

    def verify_password(self, plain: str, hashed: str) -> bool:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))

    # -- JWT tokens --

    def create_access_token(self, user_id: int, org_id: int, expires_minutes: int = 15) -> str:
        now = datetime.now(UTC)
        payload = {
            "user_id": user_id,
            "org_id": org_id,
            "type": "access",
            "exp": now + timedelta(minutes=expires_minutes),
            "iat": now,
        }
        return jwt.encode(payload, self._secret_key, algorithm=ALGORITHM)

    def create_refresh_token(self, user_id: int, expires_days: int = 7) -> str:
        now = datetime.now(UTC)
        payload = {
            "user_id": user_id,
            "type": "refresh",
            "exp": now + timedelta(days=expires_days),
            "iat": now,
        }
        return jwt.encode(payload, self._secret_key, algorithm=ALGORITHM)

    def decode_token(self, token: str, expected_type: str | None = None) -> dict | None:
        try:
            payload = jwt.decode(token, self._secret_key, algorithms=[ALGORITHM])
        except JWTError:
            return None
        if expected_type and payload.get("type") != expected_type:
            return None
        return payload

    # -- Role permissions --

    @staticmethod
    def has_permission(role: str, action: str) -> bool:
        permissions = ROLE_PERMISSIONS.get(role, set())
        return action in permissions
