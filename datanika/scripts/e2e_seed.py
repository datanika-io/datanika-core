"""Deterministic E2E seed fixture.

Creates a clean org + owner user + DuckDB connection that the Playwright
harness can log into without knowing anything about the runtime state.
Also seeds a second tenant (org B) with one-of-every-resource-type and a
viewer-role membership in org A for RBAC + tenant-isolation tests.

Re-running is idempotent: if the fixture already exists in the configured
state, the script exits 0 without raising. If data has drifted (different
email, different slug, leftover extra rows in either fixture org), the
script tears both fixture orgs down and recreates them.

Emits the fixture as a JSON payload on stdout so Playwright / CI can
capture it with:

    uv run python -m datanika.scripts.e2e_seed > .e2e-fixture.json

Safety:
- Targets ONLY the fixture orgs (slugs ``e2e-fixture`` / ``e2e-fixture-b``)
  and the three fixture users. Never touches anything else.
- Refuses to run if DATABASE_URL looks like a production host. Override
  with ``E2E_SEED_ALLOW_ANY_HOST=1`` for CI runs against ephemeral stacks.
- API key creation is opt-in via ``E2E_SEED_INCLUDE_API_KEYS=1`` — off by
  default so most test runs don't touch the api_keys table at all. When
  on, mints two keys (one per org) and returns the plaintext in the JSON
  payload; the raw key is never recoverable after this.
- Uses the existing sync engine. No schema migrations — assumes
  ``alembic upgrade head`` already ran.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.config import settings
from datanika.db import get_sync_session
from datanika.models.api_key import ApiKey
from datanika.models.connection import Connection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.pipeline import DbtCommand, Pipeline
from datanika.models.schedule import Schedule
from datanika.models.transformation import Materialization, Transformation
from datanika.models.upload import Upload
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.api_key_service import ApiKeyService
from datanika.services.auth import AuthService
from datanika.services.connection_service import ConnectionService, infer_direction
from datanika.services.encryption import EncryptionService

FIXTURE_ORG_SLUG = "e2e-fixture"
FIXTURE_ORG_NAME = "E2E Fixture Org"
FIXTURE_USER_EMAIL = "e2e@datanika.test"
FIXTURE_USER_PASSWORD = "e2e-fixture-password-not-a-secret"  # noqa: S105
FIXTURE_USER_NAME = "E2E Fixture User"
FIXTURE_CONNECTION_NAME = "e2e_duckdb_fixture"
# Platform-agnostic path: tempfile.gettempdir() resolves to %TEMP% on
# Windows and /tmp on POSIX, so Windows devs running the harness locally
# don't trip over a hardcoded /tmp/ that doesn't exist.
FIXTURE_DUCKDB_PATH = str(Path(tempfile.gettempdir()) / "e2e_fixture.duckdb")

# Second user in org A with VIEWER role — for RBAC tests (viewer can read
# but not mutate). Same password as the primary user for harness simplicity.
FIXTURE_VIEWER_USER_EMAIL = "e2e-viewer@datanika.test"
FIXTURE_VIEWER_USER_NAME = "E2E Fixture Viewer"
FIXTURE_VIEWER_USER_PASSWORD = "e2e-fixture-password-not-a-secret"  # noqa: S105

# Org B — second tenant with one of every resource type, and a dedicated
# owner user so tenant-isolation tests have a clean "user in A only" vs
# "user in B only" boundary (no shared user across tenants).
FIXTURE_ORG_B_SLUG = "e2e-fixture-b"
FIXTURE_ORG_B_NAME = "E2E Fixture Org B"
FIXTURE_ORG_B_USER_EMAIL = "e2e-org-b@datanika.test"
FIXTURE_ORG_B_USER_NAME = "E2E Fixture Org B Owner"
FIXTURE_ORG_B_USER_PASSWORD = "e2e-fixture-password-not-a-secret"  # noqa: S105
FIXTURE_ORG_B_CONNECTION_NAME = "e2e_duckdb_fixture_b"
FIXTURE_ORG_B_UPLOAD_NAME = "e2e_upload_b"
FIXTURE_ORG_B_PIPELINE_NAME = "e2e_pipeline_b"
FIXTURE_ORG_B_TRANSFORMATION_NAME = "e2e_transformation_b"
FIXTURE_ORG_B_DUCKDB_PATH = str(Path(tempfile.gettempdir()) / "e2e_fixture_b.duckdb")

FIXTURE_API_KEY_NAME_A = "e2e-fixture-api-key-a"
FIXTURE_API_KEY_NAME_B = "e2e-fixture-api-key-b"

PROD_HOST_MARKERS = ("datanika.io", "app.datanika.io", "prod", "production")


@dataclass
class SeedResult:
    # Primary org A fixture (back-compat — Playwright has hardcoded these).
    org_id: int
    org_slug: str
    user_id: int
    user_email: str
    user_password: str
    connection_id: int
    connection_name: str
    connection_type: str
    # ISO-8601 UTC timestamp of the seed operation. Part of the stable
    # JSON contract with Playwright: the harness uses this to detect
    # stale `.e2e-fixture.json` artifacts across CI runs.
    seeded_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    # Viewer-role membership in org A (RBAC tests).
    viewer_user_id: int = 0
    viewer_user_email: str = ""
    viewer_user_password: str = ""

    # Org B — one of every resource type for tenant-isolation tests
    # (core#168 tenant-jwt-boundary spec + Q1a). Dedicated owner user.
    org_b_id: int = 0
    org_b_slug: str = ""
    org_b_user_id: int = 0
    org_b_user_email: str = ""
    org_b_user_password: str = ""
    org_b_connection_id: int = 0
    org_b_upload_id: int = 0
    org_b_pipeline_id: int = 0
    org_b_transformation_id: int = 0
    org_b_schedule_id: int = 0

    # API keys — gated by ``E2E_SEED_INCLUDE_API_KEYS=1``. Sentinel values
    # (0 / "") when the flag is off so the JSON shape stays stable.
    org_a_api_key_id: int = 0
    org_a_api_key_plaintext: str = ""
    org_b_api_key_id: int = 0
    org_b_api_key_plaintext: str = ""


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
    """Hard-delete both fixture orgs and everything scoped to them.

    Only touches rows that belong to a fixture org or fixture user by the
    deterministic markers at the top of this module. Never uses raw DELETE
    across tables.

    FK-safe ordering: children before parents, with api_keys cleared before
    the users they reference.
    """
    fixture_org_slugs = (FIXTURE_ORG_SLUG, FIXTURE_ORG_B_SLUG)
    fixture_user_emails = (
        FIXTURE_USER_EMAIL,
        FIXTURE_VIEWER_USER_EMAIL,
        FIXTURE_ORG_B_USER_EMAIL,
    )

    orgs = list(
        session.execute(
            select(Organization).where(Organization.slug.in_(fixture_org_slugs))
        ).scalars()
    )
    org_ids = [o.id for o in orgs]

    if org_ids:
        # Resources with no FK dependents — delete first so nothing else
        # references their rows.
        session.execute(Schedule.__table__.delete().where(Schedule.org_id.in_(org_ids)))
        session.execute(Upload.__table__.delete().where(Upload.org_id.in_(org_ids)))
        session.execute(Pipeline.__table__.delete().where(Pipeline.org_id.in_(org_ids)))
        session.execute(Transformation.__table__.delete().where(Transformation.org_id.in_(org_ids)))
        session.execute(ApiKey.__table__.delete().where(ApiKey.org_id.in_(org_ids)))
        session.execute(Connection.__table__.delete().where(Connection.org_id.in_(org_ids)))
        session.execute(Membership.__table__.delete().where(Membership.org_id.in_(org_ids)))
        session.execute(Organization.__table__.delete().where(Organization.id.in_(org_ids)))

    users = list(session.execute(select(User).where(User.email.in_(fixture_user_emails))).scalars())
    user_ids = [u.id for u in users]
    if user_ids:
        session.execute(ApiKey.__table__.delete().where(ApiKey.user_id.in_(user_ids)))
        session.execute(Membership.__table__.delete().where(Membership.user_id.in_(user_ids)))
        session.execute(User.__table__.delete().where(User.id.in_(user_ids)))
    session.flush()


def _build_fixture(session: Session) -> SeedResult:
    auth = AuthService(settings.secret_key)
    encryption = EncryptionService(settings.credential_encryption_key)
    connection_service = ConnectionService(encryption)

    # --- Org A: primary tenant (owner + viewer + one connection) ---
    user = User(
        email=FIXTURE_USER_EMAIL,
        password_hash=auth.hash_password(FIXTURE_USER_PASSWORD),
        full_name=FIXTURE_USER_NAME,
        is_active=True,
        email_verified=True,
    )
    session.add(user)
    session.flush()

    viewer_user = User(
        email=FIXTURE_VIEWER_USER_EMAIL,
        password_hash=auth.hash_password(FIXTURE_VIEWER_USER_PASSWORD),
        full_name=FIXTURE_VIEWER_USER_NAME,
        is_active=True,
        email_verified=True,
    )
    session.add(viewer_user)
    session.flush()

    org = Organization(name=FIXTURE_ORG_NAME, slug=FIXTURE_ORG_SLUG)
    session.add(org)
    session.flush()

    session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER))
    session.add(Membership(user_id=viewer_user.id, org_id=org.id, role=MemberRole.VIEWER))
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

    # --- Org B: second tenant with one of every resource type ---
    org_b_user = User(
        email=FIXTURE_ORG_B_USER_EMAIL,
        password_hash=auth.hash_password(FIXTURE_ORG_B_USER_PASSWORD),
        full_name=FIXTURE_ORG_B_USER_NAME,
        is_active=True,
        email_verified=True,
    )
    session.add(org_b_user)
    session.flush()

    org_b = Organization(name=FIXTURE_ORG_B_NAME, slug=FIXTURE_ORG_B_SLUG)
    session.add(org_b)
    session.flush()

    session.add(Membership(user_id=org_b_user.id, org_id=org_b.id, role=MemberRole.OWNER))
    session.flush()

    org_b_connection = Connection(
        org_id=org_b.id,
        name=FIXTURE_ORG_B_CONNECTION_NAME,
        connection_type=ConnectionType.DUCKDB,
        direction=infer_direction(ConnectionType.DUCKDB),
        config_encrypted=connection_service._encryption.encrypt(
            {"path": FIXTURE_ORG_B_DUCKDB_PATH}
        ),
    )
    session.add(org_b_connection)
    session.flush()

    org_b_upload = Upload(
        org_id=org_b.id,
        name=FIXTURE_ORG_B_UPLOAD_NAME,
        source_connection_id=org_b_connection.id,
        destination_connection_id=org_b_connection.id,
        dlt_config={},
    )
    session.add(org_b_upload)
    session.flush()

    org_b_pipeline = Pipeline(
        org_id=org_b.id,
        name=FIXTURE_ORG_B_PIPELINE_NAME,
        destination_connection_id=org_b_connection.id,
        command=DbtCommand.RUN,
        models=[],
    )
    session.add(org_b_pipeline)
    session.flush()

    org_b_transformation = Transformation(
        org_id=org_b.id,
        name=FIXTURE_ORG_B_TRANSFORMATION_NAME,
        sql_body="SELECT 1 AS value",
        materialization=Materialization.VIEW,
        schema_name="staging",
        destination_connection_id=org_b_connection.id,
    )
    session.add(org_b_transformation)
    session.flush()

    org_b_schedule = Schedule(
        org_id=org_b.id,
        target_type=NodeType.PIPELINE,
        target_id=org_b_pipeline.id,
        cron_expression="0 6 * * *",
        timezone="UTC",
        is_active=False,
    )
    session.add(org_b_schedule)
    session.flush()

    # --- Optional API keys (E2E_SEED_INCLUDE_API_KEYS=1) ---
    api_key_a_id = 0
    api_key_a_raw = ""
    api_key_b_id = 0
    api_key_b_raw = ""
    if os.environ.get("E2E_SEED_INCLUDE_API_KEYS") == "1":
        api_key_svc = ApiKeyService()
        key_a, raw_a = api_key_svc.create_api_key(
            session,
            org_id=org.id,
            user_id=user.id,
            name=FIXTURE_API_KEY_NAME_A,
        )
        key_b, raw_b = api_key_svc.create_api_key(
            session,
            org_id=org_b.id,
            user_id=org_b_user.id,
            name=FIXTURE_API_KEY_NAME_B,
        )
        api_key_a_id = key_a.id
        api_key_a_raw = raw_a
        api_key_b_id = key_b.id
        api_key_b_raw = raw_b

    return SeedResult(
        org_id=org.id,
        org_slug=org.slug,
        user_id=user.id,
        user_email=user.email,
        user_password=FIXTURE_USER_PASSWORD,
        connection_id=connection.id,
        connection_name=connection.name,
        connection_type=ConnectionType.DUCKDB.value,
        viewer_user_id=viewer_user.id,
        viewer_user_email=viewer_user.email,
        viewer_user_password=FIXTURE_VIEWER_USER_PASSWORD,
        org_b_id=org_b.id,
        org_b_slug=org_b.slug,
        org_b_user_id=org_b_user.id,
        org_b_user_email=org_b_user.email,
        org_b_user_password=FIXTURE_ORG_B_USER_PASSWORD,
        org_b_connection_id=org_b_connection.id,
        org_b_upload_id=org_b_upload.id,
        org_b_pipeline_id=org_b_pipeline.id,
        org_b_transformation_id=org_b_transformation.id,
        org_b_schedule_id=org_b_schedule.id,
        org_a_api_key_id=api_key_a_id,
        org_a_api_key_plaintext=api_key_a_raw,
        org_b_api_key_id=api_key_b_id,
        org_b_api_key_plaintext=api_key_b_raw,
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
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
