"""``email_verified`` has to be reachable, or the guard that reads it is a lockout.

The column, the token minter, the ``/api/verify-email`` route, the templated mail
and the Celery task have all existed for months. **Nothing called any of them**,
so the column was ``False`` on every password account in existence and the
auto-link guard in ``user_service`` would have refused every one of them forever.

Two call sites close that, and they cover different populations:

* **signup** — every account created from now on gets the mail.
* **completing a password reset** — the only proof-of-inbox that already works
  for accounts created *before* this change, of which there are potentially many
  in self-hosted deployments. It is the same evidence by a different route: the
  link was mailed to the address on file and somebody followed it.

Companion write-up: ``plans/security/OAUTH_AUTOLINK_UNVERIFIED_LOCAL_2026-08-30.md``.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from datanika.models.password_reset import PasswordResetToken
from datanika.services.auth import AuthService
from datanika.services.password_reset_service import PasswordResetService
from datanika.services.user_service import UserService


@pytest.fixture
def auth():
    return AuthService("test-secret-key-verification-wiring")


@pytest.fixture
def user_svc(auth):
    return UserService(auth)


class TestPasswordResetProvesTheAddress:
    """Completing a reset is proof of inbox control, so it must record that."""

    def test_consuming_a_reset_token_marks_the_address_verified(self, db_session, user_svc, auth):
        user = user_svc.register_user(db_session, "resetter@example.com", "password123", "Resetter")
        assert user.email_verified is False, "precondition: register_user does not verify"

        svc = PasswordResetService(user_svc)
        raw = svc.request_reset(db_session, "resetter@example.com")
        assert raw is not None

        returned = svc.consume_token(db_session, raw, "brand-new-password")
        assert returned is not None
        db_session.refresh(user)
        assert user.email_verified is True, (
            "following a link mailed to the address is exactly the proof "
            "email_verified records; not writing it strands every pre-existing "
            "account behind the auto-link guard with no way out"
        )

    def test_a_dead_token_verifies_nothing(self, db_session, user_svc, auth):
        """The write rides on the successful branch only — an expired or
        already-used token must not upgrade the account's standing."""
        user = user_svc.register_user(db_session, "expired@example.com", "password123", "Expired")
        svc = PasswordResetService(user_svc)
        raw = svc.request_reset(db_session, "expired@example.com")
        assert raw is not None

        # Age the token past its window without touching anything else.
        row = db_session.query(PasswordResetToken).filter_by(user_id=user.id).one()
        row.expires_at = datetime.now(UTC) - timedelta(hours=1)
        db_session.flush()

        assert svc.consume_token(db_session, raw, "another-password") is None
        db_session.refresh(user)
        assert user.email_verified is False

    def test_a_rejected_password_verifies_nothing(self, db_session, user_svc, auth):
        """Validation runs before the claim, so a too-short password leaves the
        token live — and must leave the account's standing alone too."""
        from datanika.services.user_service import UserServiceError

        user = user_svc.register_user(db_session, "weakpw@example.com", "password123", "Weak")
        svc = PasswordResetService(user_svc)
        raw = svc.request_reset(db_session, "weakpw@example.com")
        assert raw is not None

        with pytest.raises(UserServiceError):
            svc.consume_token(db_session, raw, "short")
        db_session.refresh(user)
        assert user.email_verified is False


class TestSignupSendsTheVerificationMail:
    def test_the_task_is_enqueued_with_a_decodable_token(self, db_session, user_svc, auth):
        """``send_verification_email_task`` had zero callers. This is the caller.

        The token is decoded rather than merely asserted non-empty: a caller that
        mints the wrong *type* of token produces a link ``/api/verify-email``
        rejects, and the user sees "verification failed" with nothing in the logs.
        """
        from datanika.services.email_verification import (
            VerificationMailResult,
            request_email_verification,
        )

        user = user_svc.register_user(db_session, "fresh@example.com", "password123", "Fresh")

        with patch("datanika.tasks.email_tasks.send_verification_email_task") as task:
            sent = request_email_verification(user.id, user.email, auth, smtp_host="smtp.test")

        assert sent is VerificationMailResult.QUEUED
        task.delay.assert_called_once()
        to, token = task.delay.call_args.args
        assert to == "fresh@example.com"

        payload = auth.decode_token(token, expected_type="email_verify")
        assert payload is not None, "the token must be one /api/verify-email accepts"
        assert payload["user_id"] == user.id
        assert payload["email"] == "fresh@example.com"

    def test_no_smtp_means_no_enqueue_and_no_exception(self, db_session, user_svc, auth):
        """A self-hoster with no relay configured must still be able to sign up.

        Returning False rather than raising is the point: signup treats this as
        best-effort, and an unconfigured relay is a normal deployment, not an error.
        """
        from datanika.services.email_verification import (
            VerificationMailResult,
            request_email_verification,
        )

        user = user_svc.register_user(db_session, "nosmtp@example.com", "password123", "No SMTP")

        with patch("datanika.tasks.email_tasks.send_verification_email_task") as task:
            sent = request_email_verification(user.id, user.email, auth, smtp_host="")

        assert sent is VerificationMailResult.NO_RELAY
        task.delay.assert_not_called()

    def test_a_broker_failure_does_not_escape(self, db_session, user_svc, auth):
        """Redis being down must not turn a successful signup into a failed one.

        The account is already committed by the time this runs; raising here
        would show the user "Signup failed" for an account that exists.
        """
        from datanika.services.email_verification import (
            VerificationMailResult,
            request_email_verification,
        )

        user = user_svc.register_user(db_session, "broker@example.com", "password123", "Broker")

        with patch("datanika.tasks.email_tasks.send_verification_email_task") as task:
            task.delay.side_effect = OSError("broker unreachable")
            sent = request_email_verification(user.id, user.email, auth, smtp_host="smtp.test")

        assert sent is VerificationMailResult.FAILED

    def test_signup_calls_it(self):
        """The helper is only worth anything if signup actually reaches it.

        A source scan, matching the convention of the neighbouring signup tests
        in ``tests/test_ui/test_auth_state.py`` — the handler needs a live Reflex
        state, a captcha service and a session factory to invoke directly, and
        the behaviour it delegates to is covered above.
        """
        import inspect

        from datanika.ui.state import auth_state as auth_state_module

        source = inspect.getsource(auth_state_module.AuthState.signup.fn)
        assert "request_email_verification" in source, (
            "signup() must request the verification mail, or email_verified "
            "stays False forever and the auto-link guard refuses every account"
        )


class TestVerifyEmailRouteStillClosesTheLoop:
    async def test_the_route_sets_the_column(self, db_session, user_svc, auth, monkeypatch):
        """End of the loop: the token the helper mints, spent on the real route.

        Written because the two halves were built months apart and had never
        been executed against each other.
        """
        import contextlib

        from datanika.services import email_routes

        user = user_svc.register_user(db_session, "loop@example.com", "password123", "Loop")
        db_session.flush()
        token = auth.create_email_verification_token(user.id, user.email)

        @contextlib.contextmanager
        def _session():
            yield db_session

        monkeypatch.setattr(email_routes, "_auth", auth)
        monkeypatch.setattr(
            "datanika.ui.state.base_state.get_sync_session", _session, raising=False
        )

        class _Request:
            query_params = {"token": token}

        response = await email_routes.verify_email(_Request())
        assert response.status_code == 302
        assert "verify_error" not in response.headers["location"]
        db_session.refresh(user)
        assert user.email_verified is True
