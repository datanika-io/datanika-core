"""Tests for the deterministic E2E seed fixture."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import select

from datanika.models.api_key import ApiKey
from datanika.models.connection import Connection, ConnectionType
from datanika.models.pipeline import Pipeline
from datanika.models.schedule import Schedule
from datanika.models.transformation import Transformation
from datanika.models.upload import Upload
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.scripts import e2e_seed
from datanika.scripts.e2e_seed import (
    FIXTURE_CONNECTION_NAME,
    FIXTURE_ORG_B_SLUG,
    FIXTURE_ORG_B_USER_EMAIL,
    FIXTURE_ORG_SLUG,
    FIXTURE_USER_EMAIL,
    FIXTURE_USER_PASSWORD,
    FIXTURE_VIEWER_USER_EMAIL,
    SeedResult,
    UnsafeTargetError,
    _assert_safe_target,
    seed,
)
from datanika.services.auth import AuthService

_VALID_FERNET_KEY = "NS7W71uT0X-FxSq_mwJfthZzc3hIatYjQHM3MhDnQX8="


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    """The insecure default config key is not a valid Fernet key — patch it."""
    monkeypatch.setattr(e2e_seed.settings, "credential_encryption_key", _VALID_FERNET_KEY)


def test_seed_creates_fixture_on_empty_db(db_session):
    result = seed(session=db_session)

    assert isinstance(result, SeedResult)
    assert result.user_email == FIXTURE_USER_EMAIL
    assert result.org_slug == FIXTURE_ORG_SLUG
    assert result.connection_name == FIXTURE_CONNECTION_NAME
    assert result.connection_type == ConnectionType.DUCKDB.value

    user = db_session.execute(select(User).where(User.email == FIXTURE_USER_EMAIL)).scalar_one()
    assert user.email_verified is True
    assert user.is_active is True
    # Password is verifiable — the contract with Playwright is the plaintext password.
    assert AuthService("test-secret").verify_password(FIXTURE_USER_PASSWORD, user.password_hash)

    org = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_SLUG)
    ).scalar_one()

    memberships = list(
        db_session.execute(select(Membership).where(Membership.org_id == org.id)).scalars()
    )
    # Org A has the primary owner + the viewer-role user.
    assert len(memberships) == 2
    owner_memberships = [m for m in memberships if m.user_id == user.id]
    assert len(owner_memberships) == 1
    assert owner_memberships[0].role == MemberRole.OWNER

    connections = list(
        db_session.execute(select(Connection).where(Connection.org_id == org.id)).scalars()
    )
    assert len(connections) == 1
    assert connections[0].name == FIXTURE_CONNECTION_NAME
    assert connections[0].connection_type == ConnectionType.DUCKDB


def test_seed_is_idempotent(db_session):
    """Running twice must leave the same row counts — not duplicate the fixture."""
    first = seed(session=db_session)
    second = seed(session=db_session)

    # IDs may differ because teardown+rebuild re-inserts, but there must
    # be exactly one fixture user / org / connection after the second call.
    users = list(db_session.execute(select(User).where(User.email == FIXTURE_USER_EMAIL)).scalars())
    assert len(users) == 1

    orgs = list(
        db_session.execute(
            select(Organization).where(Organization.slug == FIXTURE_ORG_SLUG)
        ).scalars()
    )
    assert len(orgs) == 1

    connections = list(
        db_session.execute(select(Connection).where(Connection.org_id == orgs[0].id)).scalars()
    )
    assert len(connections) == 1

    # The second call returns the freshly-created IDs, not the first call's.
    assert second.user_id == users[0].id
    assert second.org_id == orgs[0].id
    # First call's IDs are stale by design — the contract is the fixture
    # shape, not stable integer IDs across re-runs.
    assert first.user_email == second.user_email


def test_seed_tears_down_drifted_fixture(db_session):
    """A fixture org with extra rows gets rebuilt clean."""
    seed(session=db_session)

    org = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_SLUG)
    ).scalar_one()

    # Inject drift: a second connection in the fixture org.
    extra = Connection(
        org_id=org.id,
        name="drift_connection",
        connection_type=ConnectionType.DUCKDB,
        direction="both",
        config_encrypted="{}",
    )
    db_session.add(extra)
    db_session.flush()

    seed(session=db_session)

    connections = list(
        db_session.execute(
            select(Connection).where(Connection.org_id != None)  # noqa: E711
        ).scalars()
    )
    fixture_connections = [c for c in connections if c.name == FIXTURE_CONNECTION_NAME]
    drift_connections = [c for c in connections if c.name == "drift_connection"]
    assert len(fixture_connections) == 1
    assert drift_connections == []


def test_seed_does_not_touch_non_fixture_data(db_session):
    """Seeding must leave unrelated rows alone."""
    other_user = User(
        email="real_user@example.com",
        password_hash="x" * 60,
        full_name="Real User",
    )
    db_session.add(other_user)
    other_org = Organization(name="Real Org", slug="real-org")
    db_session.add(other_org)
    db_session.flush()

    seed(session=db_session)

    # The unrelated rows must still exist.
    assert (
        db_session.execute(
            select(User).where(User.email == "real_user@example.com")
        ).scalar_one_or_none()
        is not None
    )
    assert (
        db_session.execute(
            select(Organization).where(Organization.slug == "real-org")
        ).scalar_one_or_none()
        is not None
    )


def test_assert_safe_target_blocks_prod_hosts():
    with (
        patch.object(
            e2e_seed.settings,
            "database_url_sync",
            "postgresql://u:p@app.datanika.io:5432/datanika",
        ),
        pytest.raises(UnsafeTargetError),
    ):
        _assert_safe_target()


def test_assert_safe_target_allows_override(monkeypatch):
    monkeypatch.setenv("E2E_SEED_ALLOW_ANY_HOST", "1")
    with patch.object(
        e2e_seed.settings,
        "database_url_sync",
        "postgresql://u:p@app.datanika.io:5432/datanika",
    ):
        _assert_safe_target()  # must not raise


def test_assert_safe_target_allows_localhost():
    with patch.object(
        e2e_seed.settings,
        "database_url_sync",
        "postgresql://u:p@localhost:5432/datanika",
    ):
        _assert_safe_target()  # must not raise


def test_assert_safe_target_blocks_prod_frontend_url_with_safe_db():
    """Guard must catch prod even when DATABASE_URL is a compose-service host.

    Regression for core#296: inside the prod container DATABASE_URL is a
    compose service host (e.g. ``@postgres:5432/datanika``) that contains no
    prod marker, so checking it alone lets a prod seed through. ``frontend_url``
    is the real deployment identity (``https://app.datanika.io`` in prod), so
    the guard must inspect it too.
    """
    with (
        patch.object(
            e2e_seed.settings,
            "database_url_sync",
            "postgresql://datanika:pw@postgres:5432/datanika",
        ),
        patch.object(e2e_seed.settings, "frontend_url", "https://app.datanika.io"),
        pytest.raises(UnsafeTargetError),
    ):
        _assert_safe_target()


def test_assert_safe_target_allows_localhost_frontend_url():
    """A localhost frontend_url must not trip the guard (dev/CI stacks)."""
    with (
        patch.object(
            e2e_seed.settings,
            "database_url_sync",
            "postgresql://u:p@localhost:5432/datanika",
        ),
        patch.object(e2e_seed.settings, "frontend_url", "http://localhost:3000"),
    ):
        _assert_safe_target()  # must not raise


def test_seed_result_shape():
    """The JSON contract with Playwright — keys must not silently drift."""
    # Check via the dataclass field names rather than running the script.
    from dataclasses import fields

    names = {f.name for f in fields(SeedResult)}
    assert names == {
        # Primary org A fixture (back-compat — Playwright has hardcoded these)
        "org_id",
        "org_slug",
        "user_id",
        "user_email",
        "user_password",
        "connection_id",
        "connection_name",
        "connection_type",
        "seeded_at",
        # Org A viewer-role membership (for RBAC/permission tests)
        "viewer_user_id",
        "viewer_user_email",
        "viewer_user_password",
        # Org B — one-of-every-resource-type (for tenant-isolation tests,
        # core#168 tenant-jwt-boundary spec + Q1a)
        "org_b_id",
        "org_b_slug",
        "org_b_user_id",
        "org_b_user_email",
        "org_b_user_password",
        "org_b_connection_id",
        "org_b_upload_id",
        "org_b_pipeline_id",
        "org_b_transformation_id",
        "org_b_schedule_id",
        # API keys — only populated when E2E_SEED_INCLUDE_API_KEYS=1
        "org_a_api_key_id",
        "org_a_api_key_plaintext",
        "org_b_api_key_id",
        "org_b_api_key_plaintext",
        # Read-only-scoped org A key (for API scope-enforcement tests, #297)
        "org_a_readonly_api_key_id",
        "org_a_readonly_api_key_plaintext",
    }


def test_seed_creates_viewer_user_in_org_a(db_session):
    """A second user with VIEWER role exists in org A for RBAC tests."""
    result = seed(session=db_session)

    viewer = db_session.execute(
        select(User).where(User.email == FIXTURE_VIEWER_USER_EMAIL)
    ).scalar_one()
    assert viewer.is_active is True
    assert viewer.email_verified is True
    assert result.viewer_user_id == viewer.id
    assert result.viewer_user_email == FIXTURE_VIEWER_USER_EMAIL
    assert result.viewer_user_password  # non-empty
    # Password is verifiable via the contract.
    assert AuthService("test-secret").verify_password(
        result.viewer_user_password, viewer.password_hash
    )

    # The viewer is in org A with VIEWER role, not the primary org A owner.
    org_a = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_SLUG)
    ).scalar_one()
    viewer_membership = db_session.execute(
        select(Membership).where(
            Membership.user_id == viewer.id,
            Membership.org_id == org_a.id,
        )
    ).scalar_one()
    assert viewer_membership.role == MemberRole.VIEWER


def test_seed_creates_org_b_with_full_resource_set(db_session):
    """Org B exists with one Connection, Upload, Pipeline, Transformation, Schedule.

    Covers tenant-isolation tests that need a second tenant with real resources
    (QA core#168 + Q1a — tenant-jwt-boundary spec).
    """
    result = seed(session=db_session)

    org_b = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_B_SLUG)
    ).scalar_one()
    assert result.org_b_id == org_b.id
    assert result.org_b_slug == FIXTURE_ORG_B_SLUG

    # Org B owner is a DIFFERENT user from the primary fixture user, so
    # tenant-isolation tests have a clean "user in A only" vs "user in B only"
    # distinction.
    org_b_owner = db_session.execute(
        select(User).where(User.email == FIXTURE_ORG_B_USER_EMAIL)
    ).scalar_one()
    assert result.org_b_user_id == org_b_owner.id
    assert result.org_b_user_email == FIXTURE_ORG_B_USER_EMAIL
    owner_membership = db_session.execute(
        select(Membership).where(
            Membership.user_id == org_b_owner.id,
            Membership.org_id == org_b.id,
        )
    ).scalar_one()
    assert owner_membership.role == MemberRole.OWNER

    # One of every resource type in org B.
    conn = db_session.execute(select(Connection).where(Connection.org_id == org_b.id)).scalar_one()
    assert result.org_b_connection_id == conn.id

    upload = db_session.execute(select(Upload).where(Upload.org_id == org_b.id)).scalar_one()
    assert result.org_b_upload_id == upload.id

    pipeline = db_session.execute(select(Pipeline).where(Pipeline.org_id == org_b.id)).scalar_one()
    assert result.org_b_pipeline_id == pipeline.id

    transformation = db_session.execute(
        select(Transformation).where(Transformation.org_id == org_b.id)
    ).scalar_one()
    assert result.org_b_transformation_id == transformation.id

    schedule = db_session.execute(select(Schedule).where(Schedule.org_id == org_b.id)).scalar_one()
    assert result.org_b_schedule_id == schedule.id


def test_seed_does_not_create_api_keys_by_default(db_session):
    """Without E2E_SEED_INCLUDE_API_KEYS=1, no ApiKey rows are created."""
    result = seed(session=db_session)

    api_keys = list(db_session.execute(select(ApiKey)).scalars())
    assert api_keys == []

    # Sentinel values in the result — these keep the JSON contract shape
    # stable whether the flag is on or off.
    assert result.org_a_api_key_id == 0
    assert result.org_a_api_key_plaintext == ""
    assert result.org_b_api_key_id == 0
    assert result.org_b_api_key_plaintext == ""
    assert result.org_a_readonly_api_key_id == 0
    assert result.org_a_readonly_api_key_plaintext == ""


def test_seed_creates_api_keys_when_flag_set(db_session, monkeypatch):
    """With E2E_SEED_INCLUDE_API_KEYS=1: 3 keys (A owner, B owner, A read-only)."""
    monkeypatch.setenv("E2E_SEED_INCLUDE_API_KEYS", "1")

    result = seed(session=db_session)

    api_keys = list(db_session.execute(select(ApiKey)).scalars())
    assert len(api_keys) == 3

    # Plaintext keys are returned in the result — they're hashed in the DB,
    # so Playwright must capture them here or they're lost forever.
    assert result.org_a_api_key_id > 0
    assert result.org_a_api_key_plaintext.startswith("etf_")
    assert result.org_b_api_key_id > 0
    assert result.org_b_api_key_plaintext.startswith("etf_")

    # Each key is scoped to its respective org (tenant-isolation critical).
    org_a = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_SLUG)
    ).scalar_one()
    org_b = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_B_SLUG)
    ).scalar_one()

    key_a = db_session.execute(
        select(ApiKey).where(ApiKey.id == result.org_a_api_key_id)
    ).scalar_one()
    assert key_a.org_id == org_a.id

    key_b = db_session.execute(
        select(ApiKey).where(ApiKey.id == result.org_b_api_key_id)
    ).scalar_one()
    assert key_b.org_id == org_b.id

    # Read-only-scoped key in org A: same org as the owner key, but restricted
    # to `*:read` scopes so API scope-enforcement tests can prove writes are
    # rejected (#297).
    assert result.org_a_readonly_api_key_id > 0
    assert result.org_a_readonly_api_key_plaintext.startswith("etf_")
    key_ro = db_session.execute(
        select(ApiKey).where(ApiKey.id == result.org_a_readonly_api_key_id)
    ).scalar_one()
    assert key_ro.org_id == org_a.id
    assert key_ro.scopes == e2e_seed.FIXTURE_READONLY_SCOPES
    assert all(s.endswith(":read") for s in key_ro.scopes)


def test_seed_is_idempotent_with_extended_fixture(db_session, monkeypatch):
    """Re-running with api keys on doesn't duplicate org B resources or keys."""
    monkeypatch.setenv("E2E_SEED_INCLUDE_API_KEYS", "1")

    seed(session=db_session)
    seed(session=db_session)

    # Exactly one of each resource per org after the second call.
    org_b = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_B_SLUG)
    ).scalar_one()
    for model in (Connection, Upload, Pipeline, Transformation, Schedule):
        rows = list(db_session.execute(select(model).where(model.org_id == org_b.id)).scalars())
        assert len(rows) == 1, f"{model.__name__} duplicated in org B after re-seed"

    # Viewer user and org B owner each exist exactly once.
    viewers = list(
        db_session.execute(select(User).where(User.email == FIXTURE_VIEWER_USER_EMAIL)).scalars()
    )
    assert len(viewers) == 1
    org_b_owners = list(
        db_session.execute(select(User).where(User.email == FIXTURE_ORG_B_USER_EMAIL)).scalars()
    )
    assert len(org_b_owners) == 1

    # Exactly 3 api keys (org A owner, org B owner, org A read-only).
    api_keys = list(db_session.execute(select(ApiKey)).scalars())
    assert len(api_keys) == 3


def test_seed_tears_down_drifted_org_b(db_session):
    """Drift in org B (extra rows) gets rebuilt clean on re-seed."""
    seed(session=db_session)

    org_b = db_session.execute(
        select(Organization).where(Organization.slug == FIXTURE_ORG_B_SLUG)
    ).scalar_one()

    drift = Connection(
        org_id=org_b.id,
        name="drift_org_b_connection",
        connection_type=ConnectionType.DUCKDB,
        direction="both",
        config_encrypted="{}",
    )
    db_session.add(drift)
    db_session.flush()

    seed(session=db_session)

    connections = list(
        db_session.execute(select(Connection).where(Connection.org_id == org_b.id)).scalars()
    )
    assert len(connections) == 1
    assert connections[0].name != "drift_org_b_connection"


def test_seeded_at_is_iso_utc(db_session):
    """seeded_at is pinned into the dataclass and emitted as ISO-8601 UTC,
    so Playwright can detect stale .e2e-fixture.json artifacts across runs."""
    from datetime import datetime

    result = seed(session=db_session)
    parsed = datetime.fromisoformat(result.seeded_at)
    # Must carry explicit UTC tzinfo, not naive.
    assert parsed.tzinfo is not None


def test_fixture_duckdb_path_is_platform_agnostic():
    """FIXTURE_DUCKDB_PATH must resolve under the OS temp dir, not hardcoded /tmp/."""
    import tempfile

    from datanika.scripts.e2e_seed import FIXTURE_DUCKDB_PATH

    assert FIXTURE_DUCKDB_PATH.startswith(tempfile.gettempdir())
    assert FIXTURE_DUCKDB_PATH.endswith("e2e_fixture.duckdb")
