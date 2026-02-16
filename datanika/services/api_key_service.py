"""ApiKeyService — API key CRUD and authentication."""

import hashlib
import secrets
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.api_key import ApiKey

KEY_PREFIX = "etf_"
KEY_BYTES = 32


class ApiKeyError(ValueError):
    """Raised when API key operations fail."""


class ApiKeyService:
    def create_api_key(
        self,
        session: Session,
        org_id: int,
        user_id: int,
        name: str,
        scopes: list[str] | None = None,
        expires_at: datetime | None = None,
    ) -> tuple[ApiKey, str]:
        """Create an API key. Returns (ApiKey, raw_key).

        The raw key is only available at creation time — only the hash is stored.
        """
        raw_key = KEY_PREFIX + secrets.token_urlsafe(KEY_BYTES)
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

        api_key = ApiKey(
            org_id=org_id,
            user_id=user_id,
            name=name,
            key_hash=key_hash,
            scopes=scopes,
            expires_at=expires_at,
        )
        session.add(api_key)
        session.flush()
        return api_key, raw_key

    def authenticate_api_key(
        self,
        session: Session,
        raw_key: str,
        required_scope: str | None = None,
    ) -> ApiKey | None:
        """Validate an API key, check expiry and scope. Updates last_used_at.

        Returns the ApiKey if valid, None otherwise.
        """
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        stmt = select(ApiKey).where(
            ApiKey.key_hash == key_hash,
            ApiKey.deleted_at.is_(None),
        )
        api_key = session.execute(stmt).scalar_one_or_none()
        if api_key is None:
            return None

        # Check expiry
        if api_key.expires_at is not None:
            now = datetime.now(UTC)
            # Handle timezone-naive expires_at (SQLite tests)
            expires = api_key.expires_at
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=UTC)
            if expires < now:
                return None

        # Check scope
        if (
            required_scope is not None
            and api_key.scopes is not None
            and required_scope not in api_key.scopes
        ):
            return None

        # Update last_used_at
        api_key.last_used_at = datetime.now(UTC)
        session.flush()
        return api_key

    def list_api_keys(self, session: Session, org_id: int) -> list[ApiKey]:
        """List all active (non-revoked) API keys for an org."""
        stmt = (
            select(ApiKey)
            .where(ApiKey.org_id == org_id, ApiKey.deleted_at.is_(None))
            .order_by(ApiKey.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def revoke_api_key(self, session: Session, org_id: int, key_id: int) -> bool:
        """Soft-delete an API key. Returns True if found and revoked."""
        stmt = select(ApiKey).where(
            ApiKey.id == key_id,
            ApiKey.org_id == org_id,
            ApiKey.deleted_at.is_(None),
        )
        api_key = session.execute(stmt).scalar_one_or_none()
        if api_key is None:
            return False
        api_key.deleted_at = datetime.now(UTC)
        session.flush()
        return True
