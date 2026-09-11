"""ApiKeyService — API key CRUD and authentication."""

import hashlib
import secrets
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.api_key import ApiKey
from datanika.models.user import MemberRole
from datanika.services.authorization import assert_org_role

KEY_PREFIX = "etf_"
KEY_BYTES = 32


class ApiKeyError(UserFacingError):
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
        *,
        actor_user_id: int,
    ) -> tuple[ApiKey, str]:
        """Create an API key. Returns (ApiKey, raw_key).

        🚨 ``admin`` (core#681 §1: the object IS a credential), and this is the check the
        whole issue turns on. A key's authority is its scopes at the moment it was minted,
        permanently, and ``ApiKey`` carries no role — so a **viewer able to mint an
        admin-scoped key would make every other wiring in this issue decorative**. The
        intersection at authentication time bounds what a key can do; this bounds who can
        create one at all, and neither is sufficient alone.

        ⚠️ ``user_id`` is the key's **owner**; ``actor_user_id`` is **who is minting it**.
        They are frequently the same person and must not be conflated in code: taking the
        actor from ``user_id`` would let a caller name its own authority, which is exactly
        what `SPEC_SERVICE_AUTHORIZATION` §4 forbids.


        The raw key is only available at creation time — only the hash is stored.

        Emits ``api_key.before_create`` first (core#706). This was the only
        priced dimension with no gate, and the reason was structural rather than
        an oversight in the limiter: ``api_middleware`` resolves
        ``rate_limit_rpm`` per **org** and buckets per **key**, so the published
        per-plan rate bounds nothing while key creation is unbounded. A per-key
        bucket cannot enforce a per-plan entitlement; only a cap on keys can.

        ``emit``, not ``announce`` — a subscriber refusing is the entire point,
        and exceptions must propagate or enforcement silently dies (core#456).
        Emitted **before** the row is built, so a refusal leaves nothing in the
        session for the caller's next flush to commit, and so a handler counting
        rows counts the ones it is deciding about.

        Core subscribes nothing: the cap lives on ``plans.max_api_keys`` and is
        enforced by the cloud plugin. In the core edition ``emit`` returns
        immediately and behaviour is unchanged.
        """
        # Before the quota emit: an actor who may not mint should not consume a quota
        # check, and a quota refusal must not mask an authorization one.
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.ADMIN,
            operation="create_api_key",
        )
        return self._mint(session, org_id, user_id, name, scopes, expires_at)

    def mint_consent_key(
        self,
        session: Session,
        org_id: int,
        user_id: int,
        name: str,
        scopes: list[str] | None = None,
        expires_at: datetime | None = None,
        *,
        actor_user_id: int,
    ) -> tuple[ApiKey, str]:
        """Mint a key for an OAuth **consent grant** — requires membership, not ``admin``.

        Product's distinction, and it is a distinction rather than a lowered threshold
        (core#681). ``create_api_key`` keeps ``admin`` because minting a key **for the org** is
        a credential-management act. Completing a consent flow is a different act: **obtaining
        access for yourself**, bounded by what you already have.

        Three measurements behind it, none of them mine:

        1. **No admin-class scope exists in the MCP vocabulary at all** — all fifteen are
           ``<resource>:<read|write>`` — so "a viewer mints an admin-scoped key", the risk that
           makes lowering the threshold unacceptable, is structurally unreachable here.
        2. The scope↔role **intersection is shipped**, so a viewer's key acts as a viewer
           whatever the key says.
        3. A PKCE grant is **not delegable** the way a bearer key handed to someone is.

        ``MemberRole.VIEWER`` is the lowest rank, so requiring it is requiring **any
        membership** — the "member" threshold, expressed in the vocabulary `ROLE_RANK` has. It
        still refuses a non-member and a `None` actor, which is the property that matters.

        🚨 **The emit is not optional and is why this shares :meth:`_mint`.** Skipping
        ``api_key.before_create`` on this path would turn MCP consent into an **uncapped key
        factory on a priced dimension** — a billing hole no authorization test would surface,
        because every authorization assertion would still pass. Routing both paths through one
        helper makes that unskippable by construction rather than by remembering.
        """
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.VIEWER,  # i.e. any member
            operation="mcp_consent_grant",
        )
        return self._mint(session, org_id, user_id, name, scopes, expires_at)

    def _mint(
        self,
        session: Session,
        org_id: int,
        user_id: int,
        name: str,
        scopes: list[str] | None,
        expires_at: datetime | None,
    ) -> tuple[ApiKey, str]:
        """The minting itself. **Both** entry points come through here.

        That is deliberate: the ``api_key.before_create`` emit is a quota gate on a priced
        dimension, and a second minting path that forgot it would be an uncapped key factory
        that every authorization test still calls correct.
        """
        from datanika.hooks import emit

        emit("api_key.before_create", session=session, org_id=org_id, user_id=user_id)

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

        # Debounce last_used_at: skip the UPDATE if written within the last 60s.
        # Under load (100 concurrent VUs on one key), the synchronous UPDATE
        # serializes behind a row lock → p95 8s. Debouncing reduces writes
        # from every request to at most once per key per 60s.
        now = datetime.now(UTC)
        if api_key.last_used_at is not None:
            elapsed = (now - api_key.last_used_at.replace(tzinfo=UTC)).total_seconds()
            if elapsed < 60:
                return api_key
        api_key.last_used_at = now
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

    def revoke_api_key(
        self, session: Session, org_id: int, key_id: int, *, actor_user_id: int
    ) -> bool:
        """Soft-delete an API key. Returns True if found and revoked. Requires ``admin``."""
        stmt = select(ApiKey).where(
            ApiKey.id == key_id,
            ApiKey.org_id == org_id,
            ApiKey.deleted_at.is_(None),
        )
        api_key = session.execute(stmt).scalar_one_or_none()
        if api_key is None:
            return False
        # §7.3: after the org-scoped lookup, before the mutation.
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.ADMIN,
            operation="revoke_api_key",
        )
        api_key.deleted_at = datetime.now(UTC)
        session.flush()
        return True
