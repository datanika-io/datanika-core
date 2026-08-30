"""The web session is revalidated on every protected page load (#671).

``check_auth`` used to be, in full:

    if not self.access_token:
        return rx.redirect("/login")

which tests that a **string is non-empty**. It never decoded the token, so the
access-token expiry was not enforced anywhere in the UI, and the
``password_changed_at`` check in ``UserService.redeem_refresh_token`` — correct
and covered by eight tests — could never run, because nothing called it.

The tests here are about the three properties that follow, in the order they
matter:

1. A live access token costs **no database read**. Revalidation on every page
   load is only acceptable if the common case is a signature check.
2. An aged-out access token renews **silently** through the refresh token, so
   enforcing the expiry is invisible to a user who is using the product.
3. A refresh token that ``redeem_refresh_token`` refuses ends the session and
   **clears the state**, rather than leaving a half-signed-in shell.

Driven with a stand-in ``self`` carrying the real field defaults, following the
``test_password_reset_state.py`` / ``test_run_dispatch.py`` convention. A bare
``MagicMock`` will not do: it answers every attribute truthily, so
``if not self.access_token`` is never taken and the test measures nothing.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datanika.ui.state.auth_state as auth_state_module
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import ACCESS_TOKEN_TTL_MINUTES, AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState, OrgInfo, UserInfo

SECRET = "test-secret-key-for-session-revalidation"


@pytest.fixture
def auth():
    return AuthService(SECRET)


@pytest.fixture
def svc(auth):
    return UserService(auth)


@pytest.fixture
def user(db_session, svc):
    u = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    org = Organization(name="Alice Org", slug=f"alice-{u.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=u.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return u, org


def _state(**overrides):
    """A stand-in ``self`` carrying AuthState's real field defaults.

    The private helpers are re-bound to the real functions — left as MagicMock
    attributes they return truthy mocks, and every check under test passes
    while touching nothing.
    """
    st = MagicMock()
    for name, field in AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    # ``check_auth`` awaits ``get_state(I18nState)`` after the guard. Left as a
    # plain MagicMock it raises "can't be used in 'await' expression", which
    # reads like a harness bug and hides whichever assertion actually failed.
    st.get_state = AsyncMock(return_value=MagicMock())
    st._revalidate_session = lambda: AuthState._revalidate_session(st)
    st._clear_session = lambda: AuthState._clear_session(st)
    st._get_user_service = lambda: AuthState._get_user_service(st)
    for key, value in overrides.items():
        setattr(st, key, value)
    return st


class _SessionCtx:
    """``db_session`` as a context manager whose ``commit`` is a ``flush``.

    The shared fixture wraps each test in one transaction it rolls back, so a
    real commit would end it and detach everything after.
    """

    def __init__(self, session):
        self._session = session
        self.entered = 0

    def __call__(self):
        return self

    def __enter__(self):
        self.entered += 1
        self._session.commit = self._session.flush
        return self._session

    def __exit__(self, *exc):
        return False


class TestTheHappyPathIsFree:
    def test_a_live_access_token_revalidates_without_touching_the_database(self, auth, user):
        u, org = user
        st = _state(
            access_token=auth.create_access_token(u.id, org.id),
            refresh_token=auth.create_refresh_token(u.id),
            current_org=OrgInfo(id=org.id, name=org.name, slug=org.slug),
        )
        opened = _SessionCtx(MagicMock())
        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", opened),
        ):
            assert st._revalidate_session() is True
        assert opened.entered == 0, (
            "a valid access token opened a database session; revalidating on every "
            "page load is only affordable as a signature check"
        )

    def test_an_empty_token_is_refused_without_touching_the_database(self):
        st = _state()
        opened = _SessionCtx(MagicMock())
        with patch.object(auth_state_module, "get_sync_session", opened):
            assert st._revalidate_session() is False
        assert opened.entered == 0


class TestExpiryIsEnforced:
    def test_the_default_access_token_ttl_is_ten_minutes(self, auth):
        """Founder decision 2026-08-30. Read off the token, not off the constant."""
        assert ACCESS_TOKEN_TTL_MINUTES == 10
        payload = auth.decode_token(auth.create_access_token(1, 2), expected_type="access")
        lifetime = payload["exp"] - payload["iat"]
        assert lifetime == 10 * 60, f"token lives {lifetime}s"

    def test_an_expired_access_token_is_not_accepted_as_it_stands(self, auth, user):
        u, org = user
        expired = auth.create_access_token(u.id, org.id, expires_minutes=-1)
        assert auth.decode_token(expired, expected_type="access") is None

    def test_an_expired_access_token_renews_silently_from_the_refresh_token(
        self, auth, db_session, user
    ):
        u, org = user
        expired = auth.create_access_token(u.id, org.id, expires_minutes=-1)
        st = _state(
            access_token=expired,
            refresh_token=auth.create_refresh_token(u.id),
            current_user=UserInfo(id=u.id, email=u.email, full_name=u.full_name),
            current_org=OrgInfo(id=org.id, name=org.name, slug=org.slug),
        )
        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", _SessionCtx(db_session)),
        ):
            assert st._revalidate_session() is True

        assert st.access_token != expired, "the session kept the expired token"
        renewed = auth.decode_token(st.access_token, expected_type="access")
        assert renewed is not None and renewed["user_id"] == u.id

    def test_renewal_keeps_the_session_in_the_org_it_was_in(self, auth, db_session, svc, user):
        """The discriminating case: the *newest* membership is not the current org.

        ``redeem_refresh_token`` picks the highest membership id when it is not
        told otherwise, so a renewal would silently move the user to whichever
        org they joined most recently.
        """
        u, first_org = user
        second = Organization(name="Second Org", slug=f"second-{u.id}")
        db_session.add(second)
        db_session.flush()
        db_session.add(Membership(user_id=u.id, org_id=second.id, role=MemberRole.ADMIN))
        db_session.flush()

        st = _state(
            access_token=auth.create_access_token(u.id, first_org.id, expires_minutes=-1),
            refresh_token=auth.create_refresh_token(u.id),
            current_user=UserInfo(id=u.id, email=u.email, full_name=u.full_name),
            current_org=OrgInfo(id=first_org.id, name=first_org.name, slug=first_org.slug),
        )
        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", _SessionCtx(db_session)),
        ):
            assert st._revalidate_session() is True

        payload = auth.decode_token(st.access_token, expected_type="access")
        assert payload["org_id"] == first_org.id, (
            f"renewal moved the session from org {first_org.id} to {payload['org_id']}"
        )


class TestPasswordChangeEndsOtherSessions:
    def test_a_session_whose_refresh_token_predates_a_password_change_is_refused(
        self, auth, db_session, user
    ):
        u, org = user
        st = _state(
            access_token=auth.create_access_token(u.id, org.id, expires_minutes=-1),
            refresh_token=auth.create_refresh_token(u.id),
            current_user=UserInfo(id=u.id, email=u.email, full_name=u.full_name),
            current_org=OrgInfo(id=org.id, name=org.name, slug=org.slug),
        )
        u.password_changed_at = datetime.now(UTC) + timedelta(minutes=5)
        db_session.flush()

        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", _SessionCtx(db_session)),
        ):
            assert st._revalidate_session() is False

    def test_ending_a_session_clears_every_trace_of_the_user(self, auth, user):
        u, org = user
        st = _state(
            access_token="stale",
            refresh_token="stale",
            current_user=UserInfo(id=u.id, email=u.email, full_name="Alice"),
            current_org=OrgInfo(id=org.id, name=org.name, slug=org.slug),
            user_orgs=[OrgInfo(id=org.id, name=org.name, slug=org.slug)],
            current_role="owner",
        )
        st._clear_session()
        assert st.access_token == ""
        assert st.refresh_token == ""
        assert st.current_user.id == 0
        assert st.current_org.id == 0
        assert st.user_orgs == []
        assert st.current_role == ""


class TestCheckAuthUsesIt:
    @pytest.mark.asyncio
    async def test_a_session_that_fails_revalidation_is_sent_to_login(self):
        st = _state(access_token="whatever")
        st._revalidate_session = lambda: False
        result = await AuthState.check_auth.fn(st)
        assert result is not None
        assert "/login" in str(result)

    @pytest.mark.asyncio
    async def test_a_session_that_fails_revalidation_is_cleared(self):
        cleared = []
        st = _state(access_token="whatever")
        st._revalidate_session = lambda: False
        st._clear_session = lambda: cleared.append(True)
        await AuthState.check_auth.fn(st)
        assert cleared, (
            "check_auth redirected without clearing the session; the state object "
            "survives the redirect and still answers is_authenticated"
        )

    @pytest.mark.asyncio
    async def test_a_signed_out_visitor_is_not_told_their_session_expired(self):
        """Never signed in is not the same as timed out, and saying so is confusing."""
        st = _state()
        st._revalidate_session = lambda: False
        result = str(await AuthState.check_auth.fn(st))
        assert "/login" in result
        assert "expired" not in result

    @pytest.mark.asyncio
    async def test_a_session_that_timed_out_says_so(self):
        st = _state(access_token="aged-out")
        st._revalidate_session = lambda: False
        result = str(await AuthState.check_auth.fn(st))
        assert "expired=1" in result


class TestOrgSwitchCannotOutliveTheSession:
    def test_switching_org_from_a_dead_session_mints_nothing(self, auth, db_session, user):
        """``_apply_org_switch`` mints a fresh access *and* refresh token.

        Without a guard it is a way to extend a session forever without ever
        passing the check that ``check_auth`` performs.
        """
        u, org = user
        st = _state(
            access_token=auth.create_access_token(u.id, org.id, expires_minutes=-1),
            refresh_token="not-a-token",
            current_user=UserInfo(id=u.id, email=u.email, full_name=u.full_name),
            current_org=OrgInfo(id=org.id, name=org.name, slug=org.slug),
        )
        before = st.access_token
        st._apply_org_switch = lambda org_id: AuthState._apply_org_switch(st, org_id)

        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", _SessionCtx(db_session)),
        ):
            assert st._apply_org_switch(org.id) is False

        assert st.access_token == before or st.access_token == "", (
            "an org switch minted a token for a session that had already ended"
        )
