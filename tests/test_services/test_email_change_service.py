"""Email change (SPEC_EMAIL_CHANGE, core#655 AC4).

The live address must not move when the form is submitted. It moves when the
link sent to the new address is followed, once. A second use and an expired
link do nothing. The stored token is a hash.
"""

import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from datanika.models.pii import EmailChangeRequest, UserPII
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.email_change_service import (
    ADDRESS_TAKEN,
    READY,
    EmailChangeService,
)
from datanika.services.user_service import UserService


@pytest.fixture
def user_svc():
    return UserService(AuthService("test-secret-key-for-email-change"))


@pytest.fixture
def svc(user_svc):
    return EmailChangeService(user_svc)


def _with_org(db_session, user, slug):
    org = Organization(name=f"{slug} Org", slug=f"{slug}-{user.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return user


@pytest.fixture
def user(db_session, user_svc):
    created = user_svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    return _with_org(db_session, created, "alice")


def _pii(db_session, user_id) -> UserPII:
    return db_session.get(UserPII, user_id)


def _row(db_session, token: str) -> EmailChangeRequest:
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return db_session.execute(
        select(EmailChangeRequest).where(EmailChangeRequest.token_hash == digest)
    ).scalar_one()


class TestRequestDoesNotMoveTheLiveAddress:
    def test_users_email_is_unchanged_until_confirmation(self, db_session, svc, user):
        """§3. This assertion is the one that must never go green vacuously.

        A handler that wrote the pending address into ``users.email`` would
        turn resend-verification into an open relay, and the existing resend
        test would still pass because it reads the row.
        """
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        db_session.flush()
        db_session.refresh(user)
        assert prepared.status == READY
        assert _pii(db_session, user.id).email == "alice@example.com"
        assert _pii(db_session, user.id).pending_email == "new@example.com"

    def test_an_unverified_account_can_request(self, db_session, svc, user):
        """AC1. The control exists for a typo'd signup, which is unverified."""
        assert user.email_verified is False
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        assert prepared.status == READY
        assert prepared.token

    def test_the_token_is_stored_hashed(self, db_session, svc, user):
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        row = _row(db_session, prepared.token)
        assert row.token_hash != prepared.token
        assert row.token_hash == hashlib.sha256(prepared.token.encode("utf-8")).hexdigest()
        assert prepared.token not in row.token_hash

    def test_validation_does_not_consume(self, db_session, svc, user):
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        assert svc.validate_token(db_session, prepared.token) is not None
        assert svc.validate_token(db_session, prepared.token) is not None
        assert _row(db_session, prepared.token).used_at is None


class TestConfirmationAppliesTheAddress:
    def test_following_the_link_moves_both_copies_and_verifies(self, db_session, svc, user):
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        confirmed = svc.confirm_change(db_session, prepared.token)
        db_session.flush()
        db_session.refresh(user)
        assert confirmed is not None
        assert confirmed.id == user.id
        assert _pii(db_session, user.id).email == "new@example.com"
        assert _pii(db_session, user.id).pending_email is None
        assert user.email_verified is True

    def test_a_second_use_of_the_same_link_fails(self, db_session, svc, user):
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        assert svc.confirm_change(db_session, prepared.token) is not None
        assert svc.confirm_change(db_session, prepared.token) is None
        db_session.refresh(user)
        assert _pii(db_session, user.id).email == "new@example.com"

    def test_an_expired_link_fails(self, db_session, svc, user):
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        row = _row(db_session, prepared.token)
        row.expires_at = datetime.now(UTC) - timedelta(minutes=1)
        db_session.flush()
        assert svc.confirm_change(db_session, prepared.token) is None
        db_session.refresh(user)
        assert _pii(db_session, user.id).pending_email == "new@example.com"


class TestATakenAddressIsRefused:
    def test_an_existing_account_is_refused(self, db_session, svc, user, user_svc):
        other = user_svc.register_user(db_session, "bob@example.com", "correct horse", "Bob")
        _with_org(db_session, other, "bob")
        prepared = svc.prepare_change(db_session, user.id, "bob@example.com")
        assert prepared.status == ADDRESS_TAKEN
        assert prepared.token is None
        assert _pii(db_session, user.id).pending_email is None

    def test_another_pending_address_is_refused(self, db_session, svc, user, user_svc):
        """Two people must not race the unique constraint on confirmation."""
        other = user_svc.register_user(db_session, "bob@example.com", "correct horse", "Bob")
        _with_org(db_session, other, "bob")
        assert svc.prepare_change(db_session, other.id, "shared@example.com").status == READY
        prepared = svc.prepare_change(db_session, user.id, "shared@example.com")
        assert prepared.status == ADDRESS_TAKEN
        assert _pii(db_session, user.id).pending_email is None

    def test_the_current_address_is_refused(self, db_session, svc, user):
        prepared = svc.prepare_change(db_session, user.id, "  ALICE@Example.com ")
        assert prepared.status == ADDRESS_TAKEN


class TestCancel:
    def test_cancel_clears_the_pending_address_and_burns_the_link(self, db_session, svc, user):
        prepared = svc.prepare_change(db_session, user.id, "new@example.com")
        assert svc.cancel_change(db_session, user.id) is True
        db_session.refresh(user)
        assert _pii(db_session, user.id).pending_email is None
        assert _row(db_session, prepared.token).used_at is not None
        assert svc.confirm_change(db_session, prepared.token) is None


class TestTheLimiterBoundary:
    def test_the_pair_bucket_refuses_at_limit_plus_one(self):
        """AC7. The pair bucket is actor plus target, so one user cannot spend
        the whole allowance on a fresh victim by rotating nothing."""
        from datanika.services.rate_limit_service import RateLimitService
        from tests.test_services.test_rate_limit_buckets import FakeRedis

        limiter = RateLimitService(redis_client=FakeRedis())
        bucket = EmailChangeService.pair_bucket(7, "victim@example.com")
        limit = EmailChangeService.PAIR_LIMIT
        window = EmailChangeService.WINDOW_SECONDS
        for _ in range(limit):
            assert limiter.check_window(bucket, limit=limit, window_seconds=window).allowed
        refused = limiter.check_window(bucket, limit=limit, window_seconds=window)
        assert refused.allowed is False

    def test_rotating_the_target_still_hits_the_per_user_bucket(self):
        from datanika.services.rate_limit_service import RateLimitService
        from tests.test_services.test_rate_limit_buckets import FakeRedis

        limiter = RateLimitService(redis_client=FakeRedis())
        limit = EmailChangeService.USER_LIMIT
        window = EmailChangeService.WINDOW_SECONDS
        bucket = EmailChangeService.user_bucket(7)
        for _ in range(limit):
            assert limiter.check_window(bucket, limit=limit, window_seconds=window).allowed
        assert limiter.check_window(bucket, limit=limit, window_seconds=window).allowed is False
        other = EmailChangeService.pair_bucket(8, "victim@example.com")
        assert limiter.check_window(
            other, limit=EmailChangeService.PAIR_LIMIT, window_seconds=window
        ).allowed
