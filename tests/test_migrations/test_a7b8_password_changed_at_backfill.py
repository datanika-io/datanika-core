"""The core#623 backfill, against a real Postgres with real rows in it.

``test_roundtrip.py`` already runs this migration on Postgres, but on an **empty
database** — so its ``UPDATE users SET password_changed_at = …`` touches zero
rows and proves nothing about the decision that matters here.

That decision: ``password_changed_at IS NULL`` is the "never had a password"
discriminator, so the backfill decides which side of the current-password gate
every pre-existing account lands on. The obvious clause,
``WHERE oauth_provider IS NULL``, is wrong — ``find_or_create_oauth_user``
backfills ``oauth_provider`` onto pre-existing *password* accounts on first
social login, and those rows would come out marked "never had a password",
handing them a form that takes no current password.

So this seeds all three shapes at the parent revision, migrates, and checks
where each one lands. Both the wrong clause and a missing backfill fail it.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "b3f9d17c245e"
THIS_REVISION = "a7b8c9d0e1f2"


@pytest.fixture
def seeded_at_parent(roundtrip_db_url):
    """A database at the parent revision holding the three account shapes."""
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))

    result = _run_alembic(["upgrade", PARENT_REVISION], roundtrip_db_url)
    assert result.returncode == 0, result.stderr

    with engine.begin() as conn:
        # The column must not exist yet, or the seeding below is meaningless.
        exists = conn.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = 'users' AND column_name = 'password_changed_at'"
            )
        ).scalar()
        assert exists is None, "parent revision already has the column"

        conn.execute(
            text(
                "INSERT INTO users (email, password_hash, full_name, is_active, "
                "email_verified, oauth_provider, oauth_provider_id, created_at, updated_at) "
                "VALUES "
                # 1. Registered with a password, never touched a provider.
                "('pw@example.com', '$2b$12$abcdefghijklmnopqrstuv', 'Pw', true, false, "
                "NULL, NULL, now(), now()), "
                # 2. Created by OAuth. Its hash is a random string no human knows.
                "('oauth@example.com', '$2b$12$abcdefghijklmnopqrstuv', 'OAuth', true, true, "
                "'google', 'g-1', now(), now()), "
                # 3. THE TRAP: registered with a password, later signed in with
                #    Google, so oauth_provider was backfilled onto the row.
                "('both@example.com', '$2b$12$abcdefghijklmnopqrstuv', 'Both', true, true, "
                "'google', 'g-2', now(), now())"
            )
        )
    yield engine
    engine.dispose()


def _stamps(engine) -> dict[str, bool]:
    """{email: has a password_changed_at}."""
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT email, password_changed_at IS NOT NULL FROM users ORDER BY email")
        ).all()
    return {email: stamped for email, stamped in rows}


class TestBackfillOnRealRows:
    def test_the_migration_applies(self, seeded_at_parent, roundtrip_db_url):
        result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
        assert result.returncode == 0, result.stderr

    def test_a_password_account_that_later_linked_google_keeps_its_gate(
        self, seeded_at_parent, roundtrip_db_url
    ):
        """The whole reason for the wide backfill.

        ``WHERE oauth_provider IS NULL`` leaves this row NULL, and the Settings
        card then offers to *set* a password with no current-password field —
        so a hijacked live session could change it without knowing the old one.
        """
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        assert _stamps(seeded_at_parent)["both@example.com"] is True

    def test_a_plain_password_account_is_stamped(self, seeded_at_parent, roundtrip_db_url):
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        assert _stamps(seeded_at_parent)["pw@example.com"] is True

    def test_an_oauth_created_account_is_also_stamped_and_that_is_deliberate(
        self, seeded_at_parent, roundtrip_db_url
    ):
        """Nothing in the schema separates this row from ``both@example.com``,
        so the migration errs toward the harmless direction: this account is
        asked for a current password it cannot give, and is routed to the email
        reset flow — which sets a real one and corrects the row permanently."""
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        assert _stamps(seeded_at_parent)["oauth@example.com"] is True

    def test_the_stamp_is_the_row_creation_time_not_the_migration_time(
        self, seeded_at_parent, roundtrip_db_url
    ):
        """Stamping ``now()`` would make every pre-existing refresh token look
        older than the password and log the whole instance out on deploy."""
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        with seeded_at_parent.begin() as conn:
            mismatched = conn.execute(
                text("SELECT count(*) FROM users WHERE password_changed_at <> created_at")
            ).scalar()
        assert mismatched == 0


class TestTokenTableOnPostgres:
    def test_the_hash_column_rejects_a_duplicate(self, seeded_at_parent, roundtrip_db_url):
        """SQLite model tests never run alembic, so the unique index this
        migration creates is only ever exercised here."""
        from sqlalchemy.exc import IntegrityError

        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        insert = text(
            "INSERT INTO password_reset_tokens (user_id, token_hash, expires_at, "
            "created_at, updated_at) SELECT id, :h, now() + interval '1 hour', now(), now() "
            "FROM users WHERE email = 'pw@example.com'"
        )
        with seeded_at_parent.begin() as conn:
            conn.execute(insert, {"h": "a" * 64})
        with pytest.raises(IntegrityError), seeded_at_parent.begin() as conn:
            conn.execute(insert, {"h": "a" * 64})

    def test_used_at_and_deleted_at_are_nullable(self, seeded_at_parent, roundtrip_db_url):
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        with seeded_at_parent.begin() as conn:
            nullable = dict(
                conn.execute(
                    text(
                        "SELECT column_name, is_nullable FROM information_schema.columns "
                        "WHERE table_name = 'password_reset_tokens'"
                    )
                ).all()
            )
        assert nullable["used_at"] == "YES"
        assert nullable["deleted_at"] == "YES"
        assert nullable["token_hash"] == "NO"
        assert nullable["expires_at"] == "NO"
