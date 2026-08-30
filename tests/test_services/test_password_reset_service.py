"""PasswordResetService — the reset-token lifecycle (SPEC_PASSWORD_RESET D2, D3).

A reset token is a full account-takeover primitive. Every property asserted here
is one that stops it being replayable: hashed at rest, short-lived, single-use,
superseded by a newer request, and — the one that is easy to get wrong — *not
consumed by the GET that renders the form*.
"""

import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from datanika.models.password_reset import PasswordResetToken
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.password_reset_service import PasswordResetService
from datanika.services.user_service import UserService, UserServiceError


@pytest.fixture
def auth():
    return AuthService("test-secret-key-for-reset-tokens")


@pytest.fixture
def user_svc(auth):
    return UserService(auth)


@pytest.fixture
def svc(user_svc):
    return PasswordResetService(user_svc)


def _with_org(db_session, user, slug):
    org = Organization(name=f"{slug} Org", slug=f"{slug}-{user.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return user


@pytest.fixture
def user(db_session, user_svc):
    u = user_svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    return _with_org(db_session, u, "alice")


class TestTokenIssuance:
    def test_request_returns_an_opaque_token(self, svc, db_session, user):
        token = svc.request_reset(db_session, "alice@example.com")
        assert token
        # Opaque, not a JWT: a JWT carries two dots and a decodable header.
        assert token.count(".") == 0
        assert len(token) >= 32

    def test_database_stores_only_the_hash(self, svc, db_session, user):
        """A pg_dump ships off-box nightly; it must contain nothing replayable."""
        token = svc.request_reset(db_session, "alice@example.com")
        row = db_session.execute(select(PasswordResetToken)).scalar_one()
        assert row.token_hash == hashlib.sha256(token.encode()).hexdigest()
        stored = [
            getattr(row, c.name)
            for c in PasswordResetToken.__table__.columns
            if isinstance(getattr(row, c.name), str)
        ]
        assert token not in stored

    def test_ttl_is_sixty_minutes(self, svc, db_session, user):
        before = datetime.now(UTC)
        svc.request_reset(db_session, "alice@example.com")
        row = db_session.execute(select(PasswordResetToken)).scalar_one()
        expires = row.expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
        delta = expires - before
        assert timedelta(minutes=59) < delta <= timedelta(minutes=61)

    def test_two_requests_produce_different_tokens(self, svc, db_session, user):
        first = svc.request_reset(db_session, "alice@example.com")
        second = svc.request_reset(db_session, "alice@example.com")
        assert first != second

    def test_unknown_address_yields_no_token_and_no_row(self, svc, db_session, user):
        assert svc.request_reset(db_session, "nobody@example.com") is None
        assert db_session.execute(select(PasswordResetToken)).scalars().all() == []

    def test_email_is_matched_case_insensitively(self, svc, db_session, user):
        assert svc.request_reset(db_session, "  ALICE@Example.com ") is not None

    def test_oauth_only_account_still_gets_a_token(self, svc, db_session, user_svc):
        """D6: refusing would enumerate which accounts are OAuth-backed."""
        u, _ = user_svc.find_or_create_oauth_user(
            db_session,
            "bob@example.com",
            "Bob",
            "google",
            "google-sub-1",
            email_verified=True,
        )
        assert u.password_changed_at is None
        assert svc.request_reset(db_session, "bob@example.com") is not None

    def test_inactive_user_gets_no_token(self, svc, db_session, user):
        user.is_active = False
        db_session.flush()
        assert svc.request_reset(db_session, "alice@example.com") is None


class TestTokenValidationDoesNotConsume:
    def test_validate_accepts_a_fresh_token(self, svc, db_session, user):
        token = svc.request_reset(db_session, "alice@example.com")
        assert svc.validate_token(db_session, token) is not None

    def test_validate_never_sets_used_at(self, svc, db_session, user):
        """D3: corporate mail scanners prefetch every link in an inbound message.

        A GET that consumes the token means the scanner burns it and the real
        click always lands on "already used" — reproducing only for users at
        companies with mail security, i.e. exactly our target customer.
        """
        token = svc.request_reset(db_session, "alice@example.com")
        for _ in range(5):
            assert svc.validate_token(db_session, token) is not None
        row = db_session.execute(select(PasswordResetToken)).scalar_one()
        assert row.used_at is None
        # And the real click still works afterwards.
        assert svc.consume_token(db_session, token, "a brand new password") is not None

    def test_validate_rejects_expired(self, svc, db_session, user):
        token = svc.request_reset(db_session, "alice@example.com")
        row = db_session.execute(select(PasswordResetToken)).scalar_one()
        row.expires_at = datetime.now(UTC) - timedelta(minutes=1)
        db_session.flush()
        assert svc.validate_token(db_session, token) is None

    def test_validate_rejects_used(self, svc, db_session, user):
        token = svc.request_reset(db_session, "alice@example.com")
        svc.consume_token(db_session, token, "a brand new password")
        assert svc.validate_token(db_session, token) is None

    def test_validate_rejects_unknown_and_empty(self, svc, db_session, user):
        assert svc.validate_token(db_session, "never-existed") is None
        assert svc.validate_token(db_session, "") is None


class TestConsumption:
    def test_consume_sets_the_new_password(self, svc, db_session, user, auth):
        token = svc.request_reset(db_session, "alice@example.com")
        assert svc.consume_token(db_session, token, "a brand new password") is not None
        db_session.flush()
        assert auth.verify_password("a brand new password", user.password_hash)
        assert not auth.verify_password("correct horse", user.password_hash)

    def test_consume_is_single_use(self, svc, db_session, user, auth):
        token = svc.request_reset(db_session, "alice@example.com")
        assert svc.consume_token(db_session, token, "first new password") is not None
        assert svc.consume_token(db_session, token, "second new password") is None
        db_session.flush()
        assert auth.verify_password("first new password", user.password_hash)

    def test_consume_marks_used_at_and_stamps_password_changed_at(self, svc, db_session, user):
        token = svc.request_reset(db_session, "alice@example.com")
        svc.consume_token(db_session, token, "a brand new password")
        db_session.flush()
        row = db_session.execute(select(PasswordResetToken)).scalar_one()
        assert row.used_at is not None
        assert user.password_changed_at is not None

    def test_consume_rejects_expired_without_writing(self, svc, db_session, user, auth):
        token = svc.request_reset(db_session, "alice@example.com")
        row = db_session.execute(select(PasswordResetToken)).scalar_one()
        row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        db_session.flush()
        assert svc.consume_token(db_session, token, "a brand new password") is None
        db_session.flush()
        assert auth.verify_password("correct horse", user.password_hash)

    def test_consume_enforces_password_rules_and_keeps_the_token(self, svc, db_session, user):
        token = svc.request_reset(db_session, "alice@example.com")
        with pytest.raises(UserServiceError):
            svc.consume_token(db_session, token, "short")
        # A typo must not cost a round trip through the mailbox.
        assert svc.validate_token(db_session, token) is not None

    def test_consume_gives_an_oauth_only_account_a_usable_password(
        self, svc, db_session, user_svc, auth
    ):
        u, _ = user_svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        token = svc.request_reset(db_session, "bob@example.com")
        assert svc.consume_token(db_session, token, "a password bob picked") is not None
        db_session.flush()
        assert auth.verify_password("a password bob picked", u.password_hash)
        assert u.password_changed_at is not None
        # Setting a password does not unlink the provider.
        assert u.oauth_provider == "google"


class TestSupersession:
    def test_new_request_invalidates_every_outstanding_token(self, svc, db_session, user):
        first = svc.request_reset(db_session, "alice@example.com")
        second = svc.request_reset(db_session, "alice@example.com")
        assert svc.validate_token(db_session, first) is None
        assert svc.validate_token(db_session, second) is not None

    def test_superseded_token_cannot_be_consumed(self, svc, db_session, user, auth):
        first = svc.request_reset(db_session, "alice@example.com")
        svc.request_reset(db_session, "alice@example.com")
        assert svc.consume_token(db_session, first, "an attacker password") is None
        db_session.flush()
        assert auth.verify_password("correct horse", user.password_hash)

    def test_one_users_request_does_not_touch_anothers(self, svc, db_session, user, user_svc):
        other = user_svc.register_user(db_session, "carol@example.com", "carol password", "Carol")
        _with_org(db_session, other, "carol")

        alice_token = svc.request_reset(db_session, "alice@example.com")
        svc.request_reset(db_session, "carol@example.com")
        assert svc.validate_token(db_session, alice_token) is not None


class TestRetentionSweep:
    def test_sweep_deletes_only_long_expired_rows(self, svc, db_session, user):
        live = svc.request_reset(db_session, "alice@example.com")
        stale = PasswordResetToken(
            user_id=user.id,
            token_hash="0" * 64,
            expires_at=datetime.now(UTC) - timedelta(days=31),
        )
        db_session.add(stale)
        db_session.flush()

        removed = PasswordResetService.purge_expired(db_session, retention_days=30)
        assert removed == 1
        assert svc.validate_token(db_session, live) is not None
        remaining = db_session.execute(select(PasswordResetToken.token_hash)).scalars().all()
        assert "0" * 64 not in remaining


class TestTableShape:
    def test_table_is_user_scoped_not_org_scoped(self):
        """Like ``users`` itself, a reset token belongs to a person, not an org."""
        assert "org_id" not in PasswordResetToken.__table__.columns

    def test_token_hash_is_unique_and_indexed(self):
        col = PasswordResetToken.__table__.columns["token_hash"]
        assert col.unique is True
        assert col.index is True
        assert col.type.length == 64
        assert col.nullable is False

    def test_registered_as_a_public_table(self):
        from datanika.migrations.helpers import PUBLIC_TABLES

        assert "password_reset_tokens" in PUBLIC_TABLES
