"""The state handlers behind the two reset screens (D3, D5, D7).

Driven with a stand-in ``self`` and the handler's underlying ``.fn``, following
the ``test_run_dispatch.py`` convention.

The opacity requirement is the interesting one: ``/forgot-password`` must reach
an identical rendered state for a registered address, an unregistered address,
and an address whose rate-limit bucket is exhausted. A visible "too many
requests" scoped to an email address is an oracle — send four, watch the fourth
differ, and you have learned the address is real.
"""

from unittest.mock import MagicMock, patch

import pytest

import datanika.ui.state.password_reset_state as prs
from datanika.models.password_reset import PasswordResetToken
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.password_reset_service import PasswordResetService
from datanika.services.user_service import UserService
from datanika.ui.state.password_reset_state import PasswordResetState


@pytest.fixture
def user(db_session):
    svc = UserService(AuthService("k"))
    u = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    org = Organization(name="Alice Org", slug=f"alice-{u.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=u.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return u


def _state():
    """A stand-in ``self`` carrying the state's real field defaults.

    ⚠️ The private helpers have to be bound back to the real functions. Left as
    MagicMock attributes, ``self._service()`` returns a mock whose
    ``validate_token`` returns a *truthy mock*, so every "is this token live?"
    check passes and the suite reports a working reset flow while touching no
    database at all. Reflex only wraps public methods as event handlers, so the
    underscore-prefixed ones are plain functions and can be re-bound directly.
    """
    st = MagicMock()
    for name, field in PasswordResetState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    st._service = lambda: PasswordResetState._service(st)
    st._client_ip = lambda: PasswordResetState._client_ip(st)
    return st


class _TestSession:
    """``db_session`` proxy whose ``commit()`` is a ``flush()``.

    The shared fixture isolates each test inside one outer transaction it rolls
    back at teardown. A handler that calls ``session.commit()`` releases that
    savepoint, so its rows survive into the next test — verified: a row written
    and committed in one test is still visible in the next. Flushing instead
    keeps every behaviour the handler depends on (writes visible to later
    queries in the same transaction, the conditional UPDATE's rowcount) while
    leaving the rollback able to do its job.
    """

    def __init__(self, session):
        self._session = session

    def commit(self):
        self._session.flush()

    def __getattr__(self, name):
        return getattr(self._session, name)


def _session_patch(db_session):
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=_TestSession(db_session))
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


def _request(st, db_session, email, *, allow=lambda bucket: True, headers=None, smtp="smtp.test"):
    """Drive ``request_reset``. ``smtp`` defaults to configured — the shipped
    default is empty, and an unconfigured instance short-circuits (D9)."""
    with (
        patch.object(prs.settings, "smtp_host", smtp),
        patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)),
        patch.object(prs, "_allow", side_effect=lambda bucket, *a, **k: allow(bucket)),
        patch.object(prs, "send_reset_email") as send,
    ):
        st.router = MagicMock(headers=MagicMock(raw_headers=headers or {}))
        PasswordResetState.request_reset.fn(st, {"email": email})
    return send


class TestOpaqueResponses:
    def test_a_registered_address_reaches_the_confirmation(self, db_session, user):
        st = _state()
        send = _request(st, db_session, "alice@example.com")
        assert st.submitted is True
        assert st.submitted_email == "alice@example.com"
        assert send.delay.called

    def test_an_unregistered_address_reaches_the_same_confirmation(self, db_session, user):
        st = _state()
        send = _request(st, db_session, "nobody@example.com")
        assert st.submitted is True
        assert st.submitted_email == "nobody@example.com"
        assert not send.delay.called

    def test_the_two_outcomes_are_indistinguishable(self, db_session, user):
        registered = _state()
        _request(registered, db_session, "alice@example.com")
        unknown = _state()
        _request(unknown, db_session, "nobody@example.com")
        assert (registered.submitted, registered.error) == (unknown.submitted, unknown.error)

    def test_the_submitted_address_is_echoed_back_normalised(self, db_session, user):
        """The user's own input, so it leaks nothing — and it catches the typo
        that caused the problem in the first place."""
        st = _state()
        _request(st, db_session, "  Alice@Example.com  ")
        assert st.submitted_email == "alice@example.com"

    def test_an_empty_address_does_not_reach_the_confirmation(self, db_session, user):
        st = _state()
        send = _request(st, db_session, "   ")
        assert st.submitted is False
        assert not send.delay.called


class TestBucketVisibility:
    def test_over_limit_on_the_email_bucket_looks_exactly_like_success(self, db_session, user):
        """D5/D7: a visible refusal scoped to an address enumerates that address."""
        st = _state()
        send = _request(
            st,
            db_session,
            "alice@example.com",
            allow=lambda bucket: not bucket.startswith("pwreset:email:"),
        )
        assert st.submitted is True
        assert st.error == ""
        assert not send.delay.called, "over-limit must not send, and must not say so"

    def test_over_limit_on_the_ip_bucket_may_say_so(self, db_session, user):
        """An IP bucket says nothing about any account, so it can be visible."""
        st = _state()
        send = _request(
            st,
            db_session,
            "alice@example.com",
            allow=lambda bucket: not bucket.startswith("pwreset:ip:"),
            headers={"cf-connecting-ip": "203.0.113.7"},
        )
        assert st.error != ""
        assert st.submitted is False
        assert not send.delay.called

    def test_the_ip_bucket_is_skipped_when_the_client_cannot_be_identified(self, db_session, user):
        """Behind Cloudflare → Apache → 127.0.0.1 with no CF header there is no
        client to key on, and bucketing everyone together is a global lockout."""
        seen: list[str] = []
        st = _state()
        with (
            patch.object(prs.settings, "smtp_host", "smtp.test"),
            patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)),
            patch.object(prs, "_allow", side_effect=lambda b, *a, **k: seen.append(b) or True),
            patch.object(prs, "send_reset_email"),
        ):
            st.router = MagicMock(headers=MagicMock(raw_headers={"asgi-scope-client": "127.0.0.1"}))
            PasswordResetState.request_reset.fn(st, {"email": "alice@example.com"})
        assert not any(b.startswith("pwreset:ip:") for b in seen)
        assert any(b.startswith("pwreset:email:") for b in seen)

    def test_the_ip_bucket_is_used_when_the_client_is_identifiable(self, db_session, user):
        seen: list[str] = []
        st = _state()
        with (
            patch.object(prs.settings, "smtp_host", "smtp.test"),
            patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)),
            patch.object(prs, "_allow", side_effect=lambda b, *a, **k: seen.append(b) or True),
            patch.object(prs, "send_reset_email"),
        ):
            st.router = MagicMock(
                headers=MagicMock(raw_headers={"cf-connecting-ip": "203.0.113.7"})
            )
            PasswordResetState.request_reset.fn(st, {"email": "alice@example.com"})
        assert "pwreset:ip:203.0.113.7" in seen


class TestUnavailableWithoutSmtp:
    def test_direct_visit_says_so_plainly(self):
        """Instance-level, not account-level, so it discloses nothing."""
        st = _state()
        with patch.object(prs.settings, "smtp_host", ""):
            PasswordResetState.check_availability.fn(st)
        assert st.unavailable is True

    def test_a_configured_instance_is_available(self):
        st = _state()
        with patch.object(prs.settings, "smtp_host", "smtp.example.com"):
            PasswordResetState.check_availability.fn(st)
        assert st.unavailable is False

    def test_no_email_is_sent_on_an_instance_with_no_smtp(self, db_session, user):
        st = _state()
        send = _request(st, db_session, "alice@example.com", smtp="")
        assert not send.delay.called
        assert st.unavailable is True


class TestTokenLoad:
    def _load(self, st, token):
        st.router = MagicMock(page=MagicMock(params={"token": token}))
        return PasswordResetState.load_token.fn(st)

    def test_a_valid_token_renders_the_form(self, db_session, user):
        svc = PasswordResetService(UserService(AuthService("k")))
        token = svc.request_reset(db_session, "alice@example.com")
        st = _state()
        with patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)):
            self._load(st, token)
        assert st.token_valid is True
        assert st.token == token

    def test_loading_never_consumes_the_token(self, db_session, user):
        """D3, at the state layer: a corporate mail scanner GETs this URL."""
        svc = PasswordResetService(UserService(AuthService("k")))
        token = svc.request_reset(db_session, "alice@example.com")
        st = _state()
        with patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)):
            self._load(st, token)
            self._load(st, token)
        row = db_session.query(PasswordResetToken).one()
        assert row.used_at is None

    def test_loading_strips_the_token_from_the_visible_url(self, db_session, user):
        """Out of the address bar, browser history and any Referer."""
        svc = PasswordResetService(UserService(AuthService("k")))
        token = svc.request_reset(db_session, "alice@example.com")
        st = _state()
        with patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)):
            events = self._load(st, token)
        assert "replaceState" in str(events)
        assert token not in str(events)

    def test_an_unknown_token_renders_the_invalid_state(self, db_session, user):
        st = _state()
        with patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)):
            self._load(st, "never-existed")
        assert st.token_valid is False

    def test_a_missing_token_renders_the_invalid_state(self, db_session, user):
        st = _state()
        with patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)):
            self._load(st, "")
        assert st.token_valid is False


class TestSubmitNewPassword:
    def _submit(self, st, db_session, token, password, confirm=None, allow=True):
        st.token = token
        with (
            patch.object(prs.settings, "smtp_host", "smtp.test"),
            patch.object(prs, "get_sync_session", return_value=_session_patch(db_session)),
            patch.object(prs, "_allow", return_value=allow),
        ):
            st.router = MagicMock(headers=MagicMock(raw_headers={}))
            return PasswordResetState.submit_new_password.fn(
                st, {"password": password, "confirm": confirm if confirm is not None else password}
            )

    def test_a_valid_submit_signs_the_user_out_to_login(self, db_session, user):
        """D4: never auto-sign-in. An emailed link that produces a live session
        makes the email itself a bearer credential."""
        svc = PasswordResetService(UserService(AuthService("k")))
        token = svc.request_reset(db_session, "alice@example.com")
        st = _state()
        events = self._submit(st, db_session, token, "a whole new password")
        assert "/login?reset=1" in str(events)

    def test_mismatched_confirmation_is_caught_before_anything_is_written(self, db_session, user):
        svc = PasswordResetService(UserService(AuthService("k")))
        token = svc.request_reset(db_session, "alice@example.com")
        before = user.password_hash
        st = _state()
        self._submit(st, db_session, token, "a whole new password", confirm="different")
        assert st.error != ""
        assert user.password_hash == before

    def test_a_weak_password_names_the_rule(self, db_session, user):
        svc = PasswordResetService(UserService(AuthService("k")))
        token = svc.request_reset(db_session, "alice@example.com")
        st = _state()
        self._submit(st, db_session, token, "short")
        assert "8" in st.error

    def test_a_dead_token_renders_the_invalid_state_rather_than_an_error(self, db_session, user):
        st = _state()
        self._submit(st, db_session, "never-existed", "a whole new password")
        assert st.token_valid is False
