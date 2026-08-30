"""Changing a password, and the rules that govern one (SPEC_PASSWORD_RESET D4, D6, D8).

Before this, ``password_hash`` was written in exactly two places and read in
one; nothing in Datanika could change it. The tests that matter most here are
the two easy-to-pass-while-broken ones:

* **D6** — the "has a password" discriminator. ``oauth_provider IS NOT NULL``
  looks right and is wrong, because ``find_or_create_oauth_user`` backfills that
  column onto pre-existing *password* accounts. A suite that only covers the
  OAuth-created case passes on the broken implementation.
* **D8** — one validator, three call sites. Three places that must agree is how
  they stop agreeing, so each call site is asserted separately.
"""

from datetime import UTC, datetime, timedelta

import pytest

from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService, UserServiceError


@pytest.fixture
def auth():
    return AuthService("test-secret-key-for-password-change")


@pytest.fixture
def svc(auth):
    return UserService(auth)


def _with_org(db_session, user, slug):
    org = Organization(name=f"{slug} Org", slug=f"{slug}-{user.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return user


def _confirmed(db_session, user):
    """Mark the address confirmed, as clicking the verification link does.

    Required before ``find_or_create_oauth_user`` will bind a provider to a
    password account (#679): the provider proving the address is only half the
    decision, and the account has to have proved it too. The tests below pin
    what happens *after* a link, so this is the precondition that makes that
    state reachable — not a relaxation of anything they assert.
    """
    user.email_verified = True
    db_session.flush()
    return user


@pytest.fixture
def user(db_session, svc):
    u = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    return _with_org(db_session, u, "alice")


# ---------------------------------------------------------------------------
# D8 — password rules
# ---------------------------------------------------------------------------


class TestValidator:
    def test_rejects_shorter_than_eight(self, auth):
        with pytest.raises(ValueError, match="8"):
            auth.validate_password_strength("short12")

    def test_accepts_exactly_eight(self, auth):
        auth.validate_password_strength("12345678")

    def test_rejects_empty(self, auth):
        with pytest.raises(ValueError):
            auth.validate_password_strength("")

    def test_rejects_over_seventy_two_bytes(self, auth):
        """bcrypt silently ignores everything past 72 bytes. Reject, never truncate."""
        with pytest.raises(ValueError, match="72"):
            auth.validate_password_strength("a" * 73)

    def test_accepts_exactly_seventy_two_bytes(self, auth):
        auth.validate_password_strength("a" * 72)

    def test_counts_bytes_not_characters(self, auth):
        """A 40-character passphrase of 2-byte characters is 80 bytes to bcrypt."""
        assert len("é" * 40) == 40
        assert len(("é" * 40).encode()) == 80
        with pytest.raises(ValueError, match="72"):
            auth.validate_password_strength("é" * 40)

    def test_imposes_no_composition_rules(self, auth):
        """NIST SP 800-63B: length only. No classes, no forced rotation."""
        auth.validate_password_strength("aaaaaaaaaaaaaaaa")
        auth.validate_password_strength("correct horse battery staple")


class TestValidatorIsWiredToEveryCallSite:
    """D8: one validator called from register, change and reset."""

    def test_register_rejects_a_short_password(self, svc, db_session):
        with pytest.raises(UserServiceError, match="8"):
            svc.register_user(db_session, "new@example.com", "short", "New")

    def test_register_rejects_an_over_long_password(self, svc, db_session):
        with pytest.raises(UserServiceError, match="72"):
            svc.register_user(db_session, "new@example.com", "a" * 73, "New")

    def test_change_rejects_a_short_password(self, svc, db_session, user):
        with pytest.raises(UserServiceError, match="8"):
            svc.change_password(db_session, user.id, "short", current_password="correct horse")

    def test_reset_rejects_a_short_password(self, svc, db_session, user):
        from datanika.services.password_reset_service import PasswordResetService

        reset = PasswordResetService(svc)
        token = reset.request_reset(db_session, "alice@example.com")
        with pytest.raises(UserServiceError, match="8"):
            reset.consume_token(db_session, token, "short")


# ---------------------------------------------------------------------------
# Part A — change password
# ---------------------------------------------------------------------------


class TestChangePassword:
    def test_happy_path(self, svc, db_session, user, auth):
        svc.change_password(
            db_session, user.id, "a whole new password", current_password="correct horse"
        )
        db_session.flush()
        assert auth.verify_password("a whole new password", user.password_hash)

    def test_the_new_password_authenticates_and_the_old_one_does_not(self, svc, db_session, user):
        svc.change_password(
            db_session, user.id, "a whole new password", current_password="correct horse"
        )
        db_session.flush()
        assert svc.authenticate(db_session, "alice@example.com", "a whole new password")
        assert svc.authenticate(db_session, "alice@example.com", "correct horse") is None

    def test_wrong_current_password_leaves_the_hash_byte_identical(self, svc, db_session, user):
        before = user.password_hash
        with pytest.raises(UserServiceError):
            svc.change_password(
                db_session, user.id, "a whole new password", current_password="wrong"
            )
        db_session.flush()
        assert user.password_hash == before
        assert user.password_changed_at is not None

    def test_missing_current_password_is_refused_for_an_account_that_has_one(
        self, svc, db_session, user
    ):
        """Fails closed: a UI that forgets to send it does not get a free change."""
        before = user.password_hash
        with pytest.raises(UserServiceError):
            svc.change_password(db_session, user.id, "a whole new password")
        db_session.flush()
        assert user.password_hash == before

    def test_reusing_the_current_password_is_refused(self, svc, db_session, user):
        with pytest.raises(UserServiceError, match="different"):
            svc.change_password(
                db_session, user.id, "correct horse", current_password="correct horse"
            )

    def test_unknown_user_is_refused(self, svc, db_session):
        with pytest.raises(UserServiceError):
            svc.change_password(db_session, 9999, "a whole new password", current_password="x")

    def test_success_stamps_password_changed_at(self, svc, db_session, user):
        user.password_changed_at = datetime(2020, 1, 1, tzinfo=UTC)
        db_session.flush()
        svc.change_password(
            db_session, user.id, "a whole new password", current_password="correct horse"
        )
        db_session.flush()
        stamped = user.password_changed_at
        if stamped.tzinfo is None:
            stamped = stamped.replace(tzinfo=UTC)
        assert stamped > datetime(2020, 1, 2, tzinfo=UTC)


# ---------------------------------------------------------------------------
# D6 — the discriminator. This is the trap.
# ---------------------------------------------------------------------------


class TestHasUsablePassword:
    def test_a_registered_user_has_one(self, svc, db_session, user):
        assert svc.has_usable_password(user) is True

    def test_an_oauth_created_user_does_not(self, svc, db_session):
        u, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        assert svc.has_usable_password(u) is False

    def test_a_password_account_that_later_linked_google_still_has_one(self, svc, db_session):
        """The D6 trap, stated as a test.

        ``find_or_create_oauth_user`` backfills ``oauth_provider`` onto a
        pre-existing password account on first social login. An implementation
        that reads "has a password" off ``oauth_provider IS NULL`` would say
        *False* here and drop current-password re-verification for someone who
        does have a password — a real weakening, since it lets anyone holding a
        hijacked live session change the password without knowing the old one.
        """
        alice = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
        _with_org(db_session, alice, "alice")
        _confirmed(db_session, alice)
        same, is_new = svc.find_or_create_oauth_user(
            db_session, "alice@example.com", "Alice", "google", "g-alice", email_verified=True
        )
        assert is_new is False
        assert same.id == alice.id
        assert same.oauth_provider == "google"  # the backfill happened
        assert svc.has_usable_password(same) is True

    def test_such_a_user_still_needs_the_current_password_to_change_it(self, svc, db_session):
        alice = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
        _with_org(db_session, alice, "alice")
        _confirmed(db_session, alice)
        svc.find_or_create_oauth_user(
            db_session, "alice@example.com", "Alice", "google", "g-alice", email_verified=True
        )
        before = alice.password_hash
        with pytest.raises(UserServiceError):
            svc.change_password(db_session, alice.id, "a whole new password")
        db_session.flush()
        assert alice.password_hash == before


class TestSetPasswordForOAuthOnlyAccount:
    def test_no_current_password_is_required(self, svc, db_session, auth):
        u, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        svc.change_password(db_session, u.id, "a password bob picked")
        db_session.flush()
        assert auth.verify_password("a password bob picked", u.password_hash)

    def test_setting_one_makes_the_account_a_password_account(self, svc, db_session):
        u, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        svc.change_password(db_session, u.id, "a password bob picked")
        db_session.flush()
        assert svc.has_usable_password(u) is True
        # And from now on a change needs the old one.
        with pytest.raises(UserServiceError):
            svc.change_password(db_session, u.id, "yet another password")

    def test_the_provider_link_survives(self, svc, db_session):
        u, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        svc.change_password(db_session, u.id, "a password bob picked")
        db_session.flush()
        assert u.oauth_provider == "google"
        assert u.oauth_provider_id == "g-1"


class TestRegisterStampsPasswordChangedAt:
    def test_register_sets_it(self, svc, db_session):
        u = svc.register_user(db_session, "new@example.com", "a fine password", "New")
        assert u.password_changed_at is not None

    def test_oauth_create_leaves_it_null(self, svc, db_session):
        u, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        assert u.password_changed_at is None

    def test_oauth_linking_does_not_clear_it(self, svc, db_session):
        alice = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
        _with_org(db_session, alice, "alice")
        _confirmed(db_session, alice)
        stamped = alice.password_changed_at
        svc.find_or_create_oauth_user(
            db_session, "alice@example.com", "Alice", "google", "g-alice", email_verified=True
        )
        assert alice.password_changed_at == stamped


# ---------------------------------------------------------------------------
# D4 — the enforceable half of "sessions after a change"
# ---------------------------------------------------------------------------


class TestRefreshTokenRevocation:
    """There is no durable session to invalidate; the refresh token is the only
    long-lived credential we mint (7 days), and its ``iat`` is already there."""

    def test_a_refresh_token_minted_before_the_change_is_rejected(
        self, svc, db_session, user, auth
    ):
        token = auth.create_refresh_token(user.id)
        user.password_changed_at = datetime.now(UTC) + timedelta(minutes=5)
        db_session.flush()
        assert svc.redeem_refresh_token(db_session, token) is None

    def test_a_refresh_token_minted_after_the_change_is_accepted(self, svc, db_session, user, auth):
        user.password_changed_at = datetime.now(UTC) - timedelta(minutes=5)
        db_session.flush()
        token = auth.create_refresh_token(user.id)
        result = svc.redeem_refresh_token(db_session, token)
        assert result is not None
        assert result["access_token"]

    def test_a_token_minted_in_the_same_second_as_the_change_is_accepted(
        self, svc, db_session, user, auth
    ):
        """``iat`` is whole seconds; ``password_changed_at`` has microseconds.

        Comparing them naively locks out the session the user establishes
        immediately after changing their password.
        """
        now = datetime.now(UTC)
        user.password_changed_at = now.replace(microsecond=700000)
        db_session.flush()
        token = auth.create_refresh_token(user.id)
        assert svc.redeem_refresh_token(db_session, token) is not None

    def test_a_null_password_changed_at_revokes_nothing(self, svc, db_session, auth):
        u, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        token = auth.create_refresh_token(u.id)
        assert svc.redeem_refresh_token(db_session, token) is not None

    def test_an_access_token_is_not_accepted_as_a_refresh_token(self, svc, db_session, user, auth):
        access = auth.create_access_token(user.id, 1)
        assert svc.redeem_refresh_token(db_session, access) is None

    def test_garbage_is_rejected(self, svc, db_session, user):
        assert svc.redeem_refresh_token(db_session, "not-a-jwt") is None

    def test_an_inactive_user_is_rejected(self, svc, db_session, user, auth):
        token = auth.create_refresh_token(user.id)
        user.is_active = False
        db_session.flush()
        assert svc.redeem_refresh_token(db_session, token) is None

    def test_changing_the_password_revokes_the_tokens_that_existed_before_it(
        self, svc, db_session, user, auth
    ):
        """End to end, with no hand-set timestamps."""
        issued = svc.authenticate(db_session, "alice@example.com", "correct horse")
        old_refresh = issued["refresh_token"]
        # Move the clock: iat is whole seconds, so a same-second change is a tie.
        user.password_changed_at = datetime.now(UTC) + timedelta(seconds=2)
        svc.change_password(
            db_session, user.id, "a whole new password", current_password="correct horse"
        )
        user.password_changed_at = datetime.now(UTC) + timedelta(seconds=2)
        db_session.flush()
        assert svc.redeem_refresh_token(db_session, old_refresh) is None
