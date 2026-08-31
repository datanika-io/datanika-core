"""Mutating handlers revalidate the session, not only page loads (#673).

#671 put the revalidation in ``AuthState.check_auth``, which runs from
``on_load`` — so it fires on *navigation*. A tab that is already open and never
navigates again keeps acting on the Reflex state object it already holds, so an
event handler (save, delete, run, invite) still executes for a session whose
access token aged out and, transitively, for a session a password change was
meant to end.

``BaseState._check_role`` is the other choke point: ~20 mutating handlers across
9 state modules already route through it, so one guard covers them all in the
same way one ``on_load`` guard covered every protected page.

Two things these tests are deliberate about:

* **The stand-in carries real field defaults.** A bare ``MagicMock`` answers
  every attribute truthily, so ``if not auth.session_expired`` is never taken
  and the test measures nothing. This is the trap
  ``test_session_revalidation.py`` documents, and ``session_expired`` is exactly
  the kind of bool it eats.
* **``_check_role`` is not an event handler**, so it is called directly as
  ``BaseState._check_role(st, "admin")`` rather than through ``.fn``.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datanika.ui.state.auth_state as auth_state_module
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState, OrgInfo, UserInfo
from datanika.ui.state.base_state import BaseState

SECRET = "test-secret-key-for-handler-revalidation"


@pytest.fixture
def auth():
    return AuthService(SECRET)


@pytest.fixture
def svc(auth):
    return UserService(auth)


@pytest.fixture
def user(db_session, svc):
    u = svc.register_user(db_session, "handler@example.com", "correct horse", "Handler")
    org = Organization(name="Handler Org", slug=f"handler-{u.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=u.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return u, org


def _auth(**overrides):
    """A stand-in ``AuthState`` carrying its real field defaults."""
    st = MagicMock()
    for name, field in AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    st._revalidate_session = lambda: AuthState._revalidate_session(st)
    st._clear_session = lambda: AuthState._clear_session(st)
    st._get_user_service = lambda: AuthState._get_user_service(st)
    for key, value in overrides.items():
        setattr(st, key, value)
    return st


def _caller(auth_stand_in):
    """A stand-in substate of ``BaseState`` whose ``get_state`` yields ``auth``."""
    st = MagicMock()
    st.error_message = ""
    st.get_state = AsyncMock(return_value=auth_stand_in)
    return st


class _SessionCtx:
    """``db_session`` as a context manager whose ``commit`` is a ``flush``."""

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


def _signed_in(auth_svc, u, org, *, access_minutes=10):
    return _auth(
        access_token=auth_svc.create_access_token(u.id, org.id, expires_minutes=access_minutes),
        refresh_token=auth_svc.create_refresh_token(u.id),
        current_user=UserInfo(id=u.id, email=u.email, full_name=u.full_name),
        current_org=OrgInfo(id=org.id, name=org.name, slug=org.slug),
        current_role="owner",
    )


class TestTheHandlerPathIsGuarded:
    @pytest.mark.asyncio
    async def test_a_handler_with_a_dead_session_is_refused_even_though_the_role_fits(
        self, auth, user
    ):
        """The whole defect, in one assertion.

        The role is ``owner`` and the handler asks for ``admin``, so the role
        check passes. Before #673 that was the *only* check, and the mutation
        went through for a session that no longer exists.
        """
        u, org = user
        a = _signed_in(auth, u, org, access_minutes=-1)
        a.refresh_token = ""  # nothing left to renew with
        st = _caller(a)

        with patch.object(auth_state_module.settings, "secret_key", SECRET):
            allowed = await BaseState._check_role(st, "admin")

        assert allowed is False, (
            "a mutating handler ran for a session whose access token had expired "
            "and which had no way to renew — the role fit, and the role was the "
            "only thing being checked"
        )

    @pytest.mark.asyncio
    async def test_a_refused_session_is_cleared_not_merely_denied(self, auth, user):
        """Returning False without clearing leaves the tab signed in.

        ``is_authenticated`` reads ``access_token``, and the sidebar renders
        ``current_org`` / ``current_role``, so a handler that just says "no"
        leaves a shell that still looks and behaves like a session.
        """
        u, org = user
        a = _signed_in(auth, u, org, access_minutes=-1)
        a.refresh_token = ""
        st = _caller(a)

        with patch.object(auth_state_module.settings, "secret_key", SECRET):
            await BaseState._check_role(st, "admin")

        assert a.access_token == ""
        assert a.current_user.id == 0
        assert a.current_org.id == 0
        assert a.current_role == ""
        assert a.user_orgs == []

    @pytest.mark.asyncio
    async def test_the_refusal_is_flagged_as_signed_out_not_as_permission_denied(self, auth, user):
        """#673 AC3. Telling somebody they lack a role when they need to sign in
        sends them to ask an admin for access they already have."""
        u, org = user
        a = _signed_in(auth, u, org, access_minutes=-1)
        a.refresh_token = ""
        st = _caller(a)

        with patch.object(auth_state_module.settings, "secret_key", SECRET):
            await BaseState._check_role(st, "admin")

        assert a.session_expired is True, "nothing marked the session as ended"
        assert "permission" not in st.error_message.lower(), (
            f"an expired session was reported as a permission problem: {st.error_message!r}"
        )

    @pytest.mark.asyncio
    async def test_a_genuine_permission_denial_is_still_a_permission_denial(self, auth, user):
        """The negative control. A guard that refuses everything would pass every
        test above while breaking the product, and a guard that reports every
        denial as 'signed out' would send a viewer to the login page forever."""
        u, org = user
        a = _signed_in(auth, u, org)
        a.current_role = "viewer"
        st = _caller(a)

        with patch.object(auth_state_module.settings, "secret_key", SECRET):
            allowed = await BaseState._check_role(st, "admin")

        assert allowed is False
        assert a.session_expired is False, "a live session was reported as signed out"
        assert "permission" in st.error_message.lower()


class TestRenewalIsSilentHereToo:
    @pytest.mark.asyncio
    async def test_an_aged_out_token_renews_and_the_handler_proceeds(self, auth, db_session, user):
        """#673 AC1 — same silent renewal a page load gets.

        Asserting only ``allowed is True`` would pass against the unguarded
        code, which returns True without renewing anything. The discriminating
        assertion is that the token *changed*.
        """
        u, org = user
        expired = auth.create_access_token(u.id, org.id, expires_minutes=-1)
        a = _signed_in(auth, u, org)
        a.access_token = expired
        st = _caller(a)

        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", _SessionCtx(db_session)),
        ):
            allowed = await BaseState._check_role(st, "admin")

        assert allowed is True, "an aged-out token that could renew blocked the handler"
        assert a.access_token != expired, "the handler proceeded on the expired token"
        assert auth.decode_token(a.access_token, expected_type="access") is not None
        assert a.session_expired is False

    @pytest.mark.asyncio
    async def test_a_live_access_token_adds_no_database_read(self, auth, user):
        """#673 AC4, matching #671.

        ⚠️ This passes against the unguarded code too — with no revalidation
        there is trivially no query. It is a forward guard: it fails the moment
        somebody implements this by reading ``password_changed_at`` per handler,
        which is the obvious way to make revocation immediate and the reason the
        founder's 10-minute TTL exists.
        """
        u, org = user
        a = _signed_in(auth, u, org)
        st = _caller(a)
        opened = _SessionCtx(MagicMock())

        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", opened),
        ):
            assert await BaseState._check_role(st, "admin") is True

        assert opened.entered == 0, (
            "a live access token opened a database session inside a handler guard"
        )


class TestPasswordChangeReachesOpenTabs:
    @pytest.mark.asyncio
    async def test_a_handler_is_refused_when_the_refresh_token_predates_the_change(
        self, auth, db_session, user
    ):
        """#673 AC2, and the reason the issue exists.

        This is the tab that was already open when the password was changed. Its
        access token ages out within ``ACCESS_TOKEN_TTL_MINUTES``; the refresh
        token it would renew with was minted before the change, so
        ``redeem_refresh_token`` refuses it — but only if something calls it.
        """
        u, org = user
        a = _signed_in(auth, u, org, access_minutes=-1)
        st = _caller(a)

        u.password_changed_at = datetime.now(UTC) + timedelta(minutes=5)
        db_session.flush()

        with (
            patch.object(auth_state_module.settings, "secret_key", SECRET),
            patch.object(auth_state_module, "get_sync_session", _SessionCtx(db_session)),
        ):
            allowed = await BaseState._check_role(st, "admin")

        assert allowed is False, (
            "a mutating handler ran in a tab whose session a password change was meant to end"
        )
        assert a.access_token == ""
        assert a.session_expired is True


class TestTheSignedOutStateIsReachable:
    """A flag nothing renders is a silent failure, which is the shape of the
    original bug. ``page_layout`` clears ``access_token``, so the tab falls into
    the ``is_authenticated`` false branch — which used to be a bare spinner, and
    a spinner forever is indistinguishable from a hang."""

    def test_the_layout_surfaces_an_ended_session_instead_of_spinning(self):
        import datanika.ui.components.layout as layout_module

        src = __import__("inspect").getsource(layout_module)
        assert "session_expired" in src, (
            "page_layout never reads AuthState.session_expired, so a handler that "
            "ends the session leaves the tab on an indefinite spinner"
        )

    def test_the_signed_out_copy_exists_in_every_locale(self):
        import json
        from pathlib import Path

        i18n_dir = Path(auth_state_module.__file__).parent.parent.parent / "i18n"
        locales = sorted(p for p in i18n_dir.glob("*.json"))
        assert len(locales) == 9, f"expected 9 locale files, found {len(locales)}"

        required = {"auth.signed_out_title", "auth.signed_out_body", "auth.signed_out_cta"}
        for path in locales:
            keys = set(json.loads(path.read_text(encoding="utf-8")))
            missing = required - keys
            assert not missing, f"{path.name} is missing {sorted(missing)}"
