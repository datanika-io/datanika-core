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

``TestRollbackPreservesTheRevocationBaseline`` covers the *second* consumer this
column acquired, which did not exist when the backfill above was written — see
that class's docstring.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "b3f9d17c245e"
THIS_REVISION = "a7b8c9d0e1f2"

# 🚨 Every alembic call in this file names one of the two revisions above.
# **Never `head` or `-1` here**, however convenient they look.
#
# This class tested `a7b8c9d0e1f2`'s rollback window with `downgrade -1` /
# `upgrade head`, which was correct for exactly as long as `a7b8c9d0e1f2` was
# the head. `c1d2e3f4a5b6` (core#713) was appended on 2026-08-31 and silently
# re-pointed both: `-1` became "undo the plan seeding", so the users column
# under test was never dropped and the rollback window this file exists to
# exercise stopped happening.
#
# ⚠️ **Only one of the four tests went red.** The other three passed —
# vacuously, having stopped exercising their subject — and
# `test_the_column_round_trips_by_value` in particular now compared a value to
# itself across two no-ops. A test pinned to its subject by *position* does not
# fail when the position moves; it quietly starts testing whatever is there
# instead. The red one was luck, and it is the only reason this was found.
#
# `test_roundtrip.py` keeps `-1`/`head` deliberately, and that is not the same
# thing: its subject genuinely *is* "whichever migration is at head".


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


CREATED = "2026-01-01 00:00:00+00"
CHANGED = "2026-08-30 12:00:00+00"


@pytest.fixture
def at_head_with_a_changed_password(roundtrip_db_url):
    """A database at head holding one account that has changed its password.

    ``created_at`` is deliberately eight months before ``password_changed_at``:
    the whole defect is the gap between them, so a fixture where they are close
    together would make a wrong restore look almost right.
    """
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))

    result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
    assert result.returncode == 0, result.stderr

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO users (email, password_hash, full_name, is_active, "
                "email_verified, oauth_provider, created_at, updated_at, "
                "password_changed_at) VALUES "
                # Changed their password long after signing up — the row whose
                # revocation baseline a rollback must not move.
                "('changed@example.com', '$2b$12$abcdefghijklmnopqrstuv', 'Changed', "
                "true, true, NULL, :created, :created, :changed), "
                # Never changed it: carries the backfill's own value.
                "('never@example.com', '$2b$12$abcdefghijklmnopqrstuv', 'Never', "
                "true, true, NULL, :created, :created, :created), "
                # Created by OAuth *after* this migration ran, so the column is
                # genuinely NULL — ``find_or_create_oauth_user`` never stamps it.
                # NULL is the "never had a human-chosen password" discriminator,
                # so restoring it as a timestamp is a silent behaviour change,
                # not a rounding error.
                "('oauthonly@example.com', '$2b$12$abcdefghijklmnopqrstuv', 'OAuthOnly', "
                "true, true, 'google', :created, :created, NULL)"
            ),
            {"created": CREATED, "changed": CHANGED},
        )
    yield engine
    engine.dispose()


def _baseline(engine) -> dict[str, str | None]:
    """{email: password_changed_at as an ISO string, or None}."""
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT email, password_changed_at FROM users ORDER BY email")
        ).all()
    return {email: (stamp.isoformat() if stamp is not None else None) for email, stamp in rows}


class TestRollbackPreservesTheRevocationBaseline:
    """A downgrade + re-upgrade must not move ``password_changed_at`` backwards.

    When this migration was written the column had exactly one consumer: the
    Settings card's "has this account ever had a human-chosen password" gate,
    which reads only NULL vs non-NULL. #671 gave it a second one —
    ``redeem_refresh_token`` refuses a refresh token whose ``iat`` predates it,
    which is what makes a password change end other sessions.

    That second consumer reads the **value**, and the value is what a rollback
    destroys: ``downgrade()`` drops the column, and the next ``upgrade()``
    re-backfills every row from ``created_at``. So a password change performed
    *because* the account was believed compromised is silently undone by an
    unrelated schema rollback, and every refresh token minted since signup is
    valid again.

    It fails **open**, and it is invisible to every assertion that was watching:
    the column comes back with the same name, type and nullability, the row
    count is unchanged, and no value is NULL. Only comparing the values catches
    it, which is #726's whole subject.
    """

    def test_the_column_round_trips_by_value(
        self, at_head_with_a_changed_password, roundtrip_db_url
    ):
        """Every row's value, including the NULLs, comes back as it was."""
        before = _baseline(at_head_with_a_changed_password)

        assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

        after = _baseline(at_head_with_a_changed_password)
        assert after == before, (
            "password_changed_at did not survive a downgrade + re-upgrade.\n"
            f"  before: {before}\n"
            f"  after:  {after}\n"
            "If a baseline moved backwards, every refresh token minted between "
            "those two instants is valid again, so a password change that was "
            "meant to lock somebody out no longer does. If a NULL became a "
            "timestamp, an OAuth-only account is now asked for a current "
            "password it does not have. See this class's docstring."
        )

    def test_a_row_created_while_downgraded_is_still_backfilled(
        self, at_head_with_a_changed_password, roundtrip_db_url
    ):
        """The negative control, and the one that keeps the fix honest.

        A rollback window is served by the *previous* release, which happily
        creates users while knowing nothing about this column. Those rows are
        in no stash, so the re-upgrade's backfill still has to reach them —
        which means the fix cannot simply skip the backfill whenever it finds
        stashed values. Without this test, deleting the backfill outright
        passes every other assertion in this class.
        """
        assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0

        with at_head_with_a_changed_password.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO users (email, password_hash, full_name, is_active, "
                    "email_verified, created_at, updated_at) VALUES "
                    "('during@example.com', '$2b$12$abcdefghijklmnopqrstuv', 'During', "
                    "true, true, :created, :created)"
                ),
                {"created": CREATED},
            )

        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

        after = _baseline(at_head_with_a_changed_password)
        assert after["during@example.com"] is not None, (
            "A user created during the rollback window came back with a NULL "
            "password_changed_at, which reads as 'never had a password' and "
            "offers to set one without asking for the current one."
        )
        assert after["during@example.com"].startswith("2026-01-01T00:00:00")

    def test_the_rollback_leaves_no_table_behind(
        self, at_head_with_a_changed_password, roundtrip_db_url
    ):
        """Whatever carries the values across must not survive the re-upgrade.

        ``test_roundtrip.py`` compares the table set before and after the cycle,
        so a stash left in place turns this fix into a round-trip failure
        somewhere else — a fix that breaks a different guard is not a fix.
        """
        assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

        with at_head_with_a_changed_password.begin() as conn:
            leftovers = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name LIKE '%stash%'"
                )
            ).all()
        assert leftovers == [], f"stash table survived the re-upgrade: {leftovers}"

    def test_a_second_rollback_cycle_still_preserves_the_baseline(
        self, at_head_with_a_changed_password, roundtrip_db_url
    ):
        """Rolling back twice is not exotic — it is a bad release day.

        A stash created with a plain ``CREATE TABLE`` succeeds the first time
        and raises ``DuplicateTable`` on the second, which would leave the
        second downgrade half-applied.
        """
        for _ in range(2):
            assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0
            assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

        after = _baseline(at_head_with_a_changed_password)
        assert after["changed@example.com"].startswith("2026-08-30T12:00:00")


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
