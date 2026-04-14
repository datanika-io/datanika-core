"""Deterministic E2E seed fixture.

Creates a clean org + owner user + DuckDB connection that the Playwright
harness can log into without knowing anything about the runtime state.

Re-running is idempotent: if the fixture already exists in the configured
state, the script exits 0 without raising. If data has drifted (different
email, different slug, leftover extra rows in the fixture org), the script
tears the fixture org down and recreates it.

Emits the fixture as a JSON payload on stdout so Playwright / CI can
capture it with:

    uv run python -m datanika.scripts.e2e_seed > .e2e-fixture.json

Safety:
- Targets ONLY the fixture org (slug `e2e-fixture`) and the fixture user
  (email `e2e@datanika.test`). Never touches anything else.
- Refuses to run if DATABASE_URL looks like a production host. Override
  with `E2E_SEED_ALLOW_ANY_HOST=1` for CI runs against ephemeral stacks.
- Uses the existing sync engine. No schema migrations — assumes
  `alembic upgrade head` already ran.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.config import settings
from datanika.db import get_sync_session
from datanika.models.connection import Connection, ConnectionType
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService
from datanika.services.connection_service import ConnectionService, infer_direction
from datanika.services.encryption import EncryptionService

FIXTURE_ORG_SLUG = "e2e-fixture"
FIXTURE_ORG_NAME = "E2E Fixture Org"
FIXTURE_USER_EMAIL = "e2e@datanika.test"
FIXTURE_USER_PASSWORD = "e2e-fixture-password-not-a-secret"  # noqa: S105
FIXTURE_USER_NAME = "E2E Fixture User"
FIXTURE_CONNECTION_NAME = "e2e_duckdb_fixture"
FIXTURE_DUCKDB_PATH = "/tmp/e2e_fixture.duckdb"

PROD_HOST_MARKERS = ("datanika.io", "app.datanika.io", "prod", "production")


@dataclass
class SeedResult:
    org_id: int
    org_slug: str
    user_id: int
    user_email: str
    user_password: str
    connection_id: int
    connection_name: str
    connection_type: str


class UnsafeTargetError(RuntimeError):
    pass


def _assert_safe_target() -> None:
    if os.environ.get("E2E_SEED_ALLOW_ANY_HOST") == "1":
        return
    url = settings.database_url_sync.lower()
    for marker in PROD_HOST_MARKERS:
        if marker in url:
            raise UnsafeTargetError(
                f"Refusing to seed: DATABASE_URL contains {marker!r}. "
                "Set E2E_SEED_ALLOW_ANY_HOST=1 to override (CI only)."
            )


def _tear_down_fixture(session: Session) -> None:
    """Hard-delete the fixture org and everything scoped to it.

    Only touches rows that belong to the fixture org or user by the
    deterministic markers above. Never uses raw DELETE across tables.
    """
    org = session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_SLUG)
    ).scalar_one_or_none()
    if org is not None:
        session.execute(Connection.__table__.delete().where(Connection.org_id == org.id))
        session.execute(Membership.__table__.delete().where(Membership.org_id == org.id))
        session.execute(Organization.__table__.delete().where(Organization.id == org.id))
    user = session.execute(
        select(User).where(User.email == FIXTURE_USER_EMAIL)
    ).scalar_one_or_none()
    if user is not None:
        session.execute(Membership.__table__.delete().where(Membership.user_id == user.id))
        session.execute(User.__table__.delete().where(User.id == user.id))
    session.flush()


def _build_fixture(session: Session) -> SeedResult:
    auth = AuthService(settings.secret_key)
    encryption = EncryptionService(settings.credential_encryption_key)
    connection_service = ConnectionService(encryption)

    user = User(
        email=FIXTURE_USER_EMAIL,
        password_hash=auth.hash_password(FIXTURE_USER_PASSWORD),
        full_name=FIXTURE_USER_NAME,
        is_active=True,
        email_verified=True,
    )
    session.add(user)
    session.flush()

    org = Organization(name=FIXTURE_ORG_NAME, slug=FIXTURE_ORG_SLUG)
    session.add(org)
    session.flush()

    session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER))
    session.flush()

    connection = Connection(
        org_id=org.id,
        name=FIXTURE_CONNECTION_NAME,
        connection_type=ConnectionType.DUCKDB,
        direction=infer_direction(ConnectionType.DUCKDB),
        config_encrypted=connection_service._encryption.encrypt({"path": FIXTURE_DUCKDB_PATH}),
    )
    session.add(connection)
    session.flush()

    return SeedResult(
        org_id=org.id,
        org_slug=org.slug,
        user_id=user.id,
        user_email=user.email,
        user_password=FIXTURE_USER_PASSWORD,
        connection_id=connection.id,
        connection_name=connection.name,
        connection_type=ConnectionType.DUCKDB.value,
    )


def seed(session: Session | None = None) -> SeedResult:
    """Idempotently create the fixture. Returns the seeded IDs."""
    _assert_safe_target()
    owns_session = session is None
    if owns_session:
        session = get_sync_session()
    try:
        _tear_down_fixture(session)
        result = _build_fixture(session)
        # Mark real seed time so stale fixtures can be identified in logs.
        session.commit() if owns_session else session.flush()
        return result
    finally:
        if owns_session:
            session.close()


def main() -> int:
    try:
        result = seed()
    except UnsafeTargetError as exc:
        print(f"e2e-seed refused: {exc}", file=sys.stderr)
        return 2
    payload = {
        **asdict(result),
        "seeded_at": datetime.now(UTC).isoformat(),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
