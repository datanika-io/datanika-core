"""A refused action must say so somewhere the user is looking (core#744).

QA measured the transport half on staging: a `Run` click that lands while the
websocket is down produces **no run row, no toast and no error** — Reflex's own
`processEvent` returns early when the socket is not connected, and its comment
for that path reads *"otherwise we throw the event into the void"*. The
harness's ability to *cause* that was fixed on the E2E side; the app's ability
to *exhibit* it was explicitly left open, and this is that half.

Two independent silences, and neither is the Celery worker:

1. **The refusal is written to a var nothing renders.** `BaseState._check_role`
   records a permission denial in `self.error_message` — the *substate's* copy.
   `datanika/ui/pages/uploads.py` references `error_message` zero times, and it
   is not alone: 10 of the 15 state classes that assign it are rendered by no
   page or component (core#887 carries the audit). `AuthState.session_expired`
   already documents why this cannot be fixed on `BaseState` — every substate
   gets its own copy of an inherited var, so a flag set on `UploadState` is
   invisible to `page_layout`.

2. **A dead socket looks like a slow page.** `page_layout` renders a bare
   spinner while `AuthState.is_authenticated` is false, which is also what a
   page whose state never arrived looks like. Reflex's default overlay is a
   corner `wifi-off` pulser and a transient toast; neither survives on the
   screen, and the spinner branch is exactly where the user is stuck.

The banner therefore has to sit **outside** the `is_authenticated` conditional,
and `test_the_banner_is_not_inside_the_authenticated_branch` is the assertion
that keeps it there — a banner nested in the true branch renders for everyone
except the person who needs it.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datanika.ui.state.auth_state as auth_state_module
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState, OrgInfo, UserInfo
from datanika.ui.state.base_state import BaseState

I18N = Path("datanika/i18n")
SECRET = "test-secret-key-for-refusal-visibility"


@pytest.fixture
def auth():
    return AuthService(SECRET)


@pytest.fixture
def user(db_session, auth):
    svc = UserService(auth)
    u = svc.register_user(db_session, "refusal@example.com", "correct horse", "Refusal")
    org = Organization(name="Refusal Org", slug=f"refusal-{u.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=u.id, org_id=org.id, role=MemberRole.VIEWER))
    db_session.flush()
    return u, org


def _auth_stand_in(**overrides):
    """An ``AuthState`` stand-in carrying its real field defaults.

    A bare ``MagicMock`` answers every attribute truthily, so a check like
    ``if not auth.session_expired`` is never taken and the test measures
    nothing. Same trap ``test_handler_session_revalidation.py`` documents.
    """
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
    st = MagicMock()
    st.error_message = ""
    st.get_state = AsyncMock(return_value=auth_stand_in)
    # #673 split the session half of `_check_role` into `_require_live_session`
    # so a role-free mutation can check the session without acquiring a role
    # gate. Delegate to the real implementation — a bare MagicMock returns a
    # truthy non-awaitable, and these tests turn on what the guard does.
    st._require_live_session = lambda: BaseState._require_live_session(st)
    return st


def _signed_in(auth_svc, u, org, role, *, access_minutes=10):
    return _auth_stand_in(
        access_token=auth_svc.create_access_token(u.id, org.id, expires_minutes=access_minutes),
        refresh_token=auth_svc.create_refresh_token(u.id),
        current_user=UserInfo(id=u.id, email=u.email, full_name=u.full_name),
        current_org=OrgInfo(id=org.id, name=org.name, slug=org.slug),
        current_role=role,
    )


class TestARefusalReachesTheStateTheShellReads:
    @pytest.mark.asyncio
    async def test_a_denied_role_is_recorded_where_page_layout_can_see_it(self, auth, user):
        """The defect, in one assertion.

        Before this, the only record of the refusal was ``st.error_message`` —
        the *viewer's own* substate copy — and no page renders `UploadState`'s.
        """
        u, org = user
        a = _signed_in(auth, u, org, "viewer")
        st = _caller(a)

        with patch.object(auth_state_module.settings, "secret_key", SECRET):
            allowed = await BaseState._check_role(st, "editor")

        assert allowed is False
        assert a.action_error, (
            "the handler refused and left no mark on AuthState, so the shell has "
            "nothing to render and the button did nothing and said nothing"
        )
        assert "editor" in a.action_error

    @pytest.mark.asyncio
    async def test_an_expired_session_is_not_reported_as_a_permission_problem(self, auth, user):
        """The signed-out panel is that path's channel, and it is a better one.

        Telling somebody they lack a role when they need to sign in sends them
        to ask an admin for access they already have (#673 AC3). So this branch
        must leave ``action_error`` empty rather than fill it.
        """
        u, org = user
        a = _signed_in(auth, u, org, "owner", access_minutes=-1)
        a.refresh_token = ""
        st = _caller(a)

        with patch.object(auth_state_module.settings, "secret_key", SECRET):
            await BaseState._check_role(st, "admin")

        assert a.session_expired is True
        assert a.action_error == "", (
            f"an expired session was reported as a permission problem: {a.action_error!r}"
        )

    @pytest.mark.asyncio
    async def test_a_permitted_action_clears_a_previous_refusal(self, auth, user):
        """Otherwise the callout outlives the condition that raised it.

        Navigation clears it too (``check_auth``), but a user who is granted a
        role and retries without navigating must not still be told no.
        """
        u, org = user
        a = _signed_in(auth, u, org, "owner")
        a.action_error = "Permission denied. Requires editor role or higher."
        st = _caller(a)

        with patch.object(auth_state_module.settings, "secret_key", SECRET):
            allowed = await BaseState._check_role(st, "editor")

        assert allowed is True
        assert a.action_error == ""


class TestTheRunButtonSaysWhyItRefused:
    @pytest.mark.asyncio
    async def test_a_deleted_connection_refusal_is_visible(self):
        """core#805 added this guard and reported it only to `UploadState`.

        `uploads.py` renders `error_message` nowhere, so the refusal was
        literally invisible: press Run, nothing happens, and the sentence
        explaining why goes to a var no template reads.
        """
        from datanika.ui.state.upload_state import UploadState

        fn = UploadState.run_upload.fn
        state = MagicMock()
        state._check_role = AsyncMock(return_value=True)
        mock_auth = MagicMock()
        mock_auth.current_org.id = 1
        mock_auth.current_user.id = 10
        mock_auth.action_error = ""
        state.get_state = AsyncMock(return_value=mock_auth)

        upload = MagicMock()
        upload.source_connection_id = 101
        upload.destination_connection_id = 202
        upload_svc = MagicMock()
        upload_svc.get_upload.return_value = upload
        conn_svc = MagicMock()
        conn_svc.get_connection.return_value = None  # both ends deleted

        exec_svc = MagicMock()

        with (
            patch("datanika.ui.state.upload_state.get_sync_session") as get_session,
            patch("datanika.ui.state.upload_state.ExecutionService", return_value=exec_svc),
            patch("datanika.ui.state.upload_state.EncryptionService"),
            patch("datanika.ui.state.upload_state.ConnectionService", return_value=conn_svc),
            patch("datanika.ui.state.upload_state.UploadService", return_value=upload_svc),
            patch("datanika.ui.state.upload_state.run_upload_task") as task,
            patch("datanika.ui.state.base_state.BaseState._audit"),
        ):
            session = MagicMock()
            get_session.return_value.__enter__ = MagicMock(return_value=session)
            get_session.return_value.__exit__ = MagicMock(return_value=False)
            async for _ in fn(state, upload_id=5):
                pass

        assert exec_svc.create_run.call_count == 0, (
            "harness: the refusal did not fire, so this test proves nothing"
        )
        assert task.delay.call_count == 0
        assert mock_auth.action_error, "Run refused a deleted connection and told the user nothing"
        assert "connection" in mock_auth.action_error.lower()


def _build_layout():
    """Return (rendered, captured stdout+stderr) for the shell with a body."""
    import contextlib
    import io

    import reflex as rx

    from datanika.ui.components.layout import page_layout

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        component = page_layout(rx.text("body"), title="T")
        rendered = str(component)
    return component, rendered, buf.getvalue()


class TestTheShellShowsBoth:
    def test_page_layout_mounts_the_refusal_callout(self):
        _, rendered, _ = _build_layout()
        assert "action_error" in rendered, (
            "page_layout does not render AuthState.action_error, so nothing shows a refusal"
        )

    def test_the_dismiss_control_is_wired(self):
        _, rendered, _ = _build_layout()
        assert "dismiss_action_error" in rendered

    def test_page_layout_mounts_the_connection_banner(self):
        _, rendered, _ = _build_layout()
        assert "connectErrors" in rendered, (
            "nothing on an authenticated page tells the user the socket is down"
        )

    def test_the_banner_is_not_inside_the_authenticated_branch(self):
        """The whole point. A disconnected page is stuck on the spinner branch.

        `page_layout` renders `rx.cond(AuthState.is_authenticated, shell,
        spinner)`, and a page whose state never arrived evaluates that to
        false — so a banner mounted inside the true branch is invisible to
        exactly the person it exists for.
        """
        component, rendered, _ = _build_layout()
        assert "connectErrors" in rendered  # positive control for the walk below

        auth_branches = [
            str(child) for child in component.children if "is_authenticated" in str(child)
        ]
        assert auth_branches, (
            "could not find the is_authenticated branch — this assertion is not "
            "looking at the thing it claims to"
        )
        assert all("connectErrors" not in branch for branch in auth_branches), (
            "the connection banner is nested inside the authenticated branch, so it "
            "cannot render on the spinner a dropped socket leaves behind"
        )

    def test_building_the_shell_substitutes_no_icon(self):
        """Reflex swaps an unknown icon for ``circle_help`` and only warns on stderr.

        So "the layout builds" proves nothing about its icons.
        """
        _, _, captured = _build_layout()
        assert "Invalid icon tag" not in captured, f"an icon was substituted: {captured}"


class TestTheBannerTextIsTranslated:
    def test_every_locale_carries_the_key(self):
        missing = [
            p.name
            for p in sorted(I18N.glob("*.json"))
            if "app.connection_lost" not in json.loads(p.read_text(encoding="utf-8"))
        ]
        assert missing == [], f"locales missing app.connection_lost: {missing}"

    def test_no_locale_left_it_in_english(self):
        """Key parity passes on nine copies of the English string.

        The i18n parity test compares key *sets*, so a locale that carries the
        key with the English value is indistinguishable from a translated one.
        """
        values = {
            p.name: json.loads(p.read_text(encoding="utf-8"))["app.connection_lost"]
            for p in sorted(I18N.glob("*.json"))
        }
        english = values["en.json"]
        untranslated = [name for name, v in values.items() if name != "en.json" and v == english]
        assert untranslated == [], f"still English: {untranslated}"
