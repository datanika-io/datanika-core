"""Tests for the deterministic E2E seed fixture."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import select

from datanika.models.connection import Connection, ConnectionType
from datanika.models.user import Membership, Organization, User
from datanika.scripts import e2e_seed
from datanika.scripts.e2e_seed import (
    FIXTURE_CONNECTION_NAME,
    FIXTURE_ORG_SLUG,
    FIXTURE_USER_EMAIL,
    FIXTURE_USER_PASSWORD,
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
    assert len(memberships) == 1
    assert memberships[0].user_id == user.id

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


def test_seed_result_shape():
    """The JSON contract with Playwright — keys must not silently drift."""
    # Check via the dataclass field names rather than running the script.
    from dataclasses import fields

    names = {f.name for f in fields(SeedResult)}
    assert names == {
        "org_id",
        "org_slug",
        "user_id",
        "user_email",
        "user_password",
        "connection_id",
        "connection_name",
        "connection_type",
    }
