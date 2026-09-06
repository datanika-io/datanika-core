"""core#1081 §2 — a credential page must refuse a visitor who is already signed in.

`SPEC_PAGE_ENTRY.md` §2 is the contract. The defect it names is not a confusing
page:

`AuthState.signup` never asks whether a session already exists, and it ends by
overwriting the live one in place (`auth_state.py`, the block ending
`self.user_orgs = [self.current_org]`). So a signed-in user who completes that
form is **silently re-identified** — new user row, new org, tokens replaced,
`user_orgs` clobbered to a single entry. No sign-out, no confirmation, no notice.
Their memberships survive in the database; the session no longer knows about
them, so the next page renders an empty new tenant and their work reads as gone.

Since landing PR #513 every marketing CTA points at `/signup`, so the signed-in
population reaches this form through the most prominent control on the site.

⚠️ **Scoped honestly: this is not privilege escalation.** The resulting session is
a brand-new org containing only the new user, so nothing becomes *reachable* that
was not before. The damage lands on the person who was signed in.

## The property, and why the obvious assertion misses it

🚨 **The regression is not "signup redirects".** A redirect-only assertion passes
against an implementation that redirects *after* the assignment — which is the
implementation that still loses the memberships. Spec AC1.3. So the load-bearing
test here is `test_the_existing_memberships_survive`, and the redirect assertion
is its companion, not its substitute.

The control matters as much: a guard that refuses **everyone** satisfies every
"nothing happened" assertion in this file. `test_a_signed_out_signup_still_works`
is what stops that, and it drives the real handler against a real session.

## Verification needs no production credential and no browser

Product could not observe the authenticated case from outside — the credential-free
bundle probe's own positive control returned zero (`check_auth` appears in **0** of
51 shipped JS files, measured during a load in which it demonstrably ran, because
`on_load` handler identity is dispatched over the websocket and is in no client
artifact). That was a statement about the instrument, not about the work: the
authenticated case is an in-process test, and the pattern already existed in
`test_handler_session_revalidation.py` and `test_invited_signup_lands_in_one_org.py`.
Everything below runs in the unit suite.
"""

import ast
import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from datanika.config import settings
from datanika.models.user import Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState, OrgInfo, UserInfo

_APP = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "datanika.py"

#: The handler this issue adds. Named once so a rename shows up here rather than
#: as four unrelated failures.
GUARD = "redirect_if_signed_in"

EMAIL = "already-signed-in@example.com"
PASSWORD = "correct-horse-battery-staple"
NAME = "Sam Signedin"


@pytest.fixture
def auth():
    return AuthService(settings.secret_key)


def _redirect_target(spec) -> str:
    """The URL an ``rx.redirect`` EventSpec sends the browser to."""
    assert spec is not None, "expected a redirect, got None"
    for name, value in spec.args:
        if name._js_expr == "path":
            return value._var_value
    raise AssertionError("EventSpec has no 'path' argument")


def _one(result):
    """A handler may return one event or a list; give me the single event."""
    if isinstance(result, list):
        assert len(result) == 1, f"expected exactly one event, got {len(result)}"
        return result[0]
    return result


class _AuthStandIn:
    """An ``AuthState`` stand-in carrying its real field defaults.

    🚨 **Not a bare ``MagicMock``.** One answers every attribute truthily, so
    ``if not self.access_token`` is never taken and the guard under test would
    look like it worked while never running — the trap
    ``test_handler_session_revalidation.py`` documents in its own docstring.
    """

    def __init__(self, **overrides):
        for name, field in AuthState.__fields__.items():
            default = field.default_factory() if field.default_factory else field.default
            setattr(self, name, default)
        self.router = SimpleNamespace(page=SimpleNamespace(params={}))
        for key, value in overrides.items():
            setattr(self, key, value)

    #: The **real** helpers, bound rather than reimplemented. Two definitions of
    #: "is this session valid" is how core#671 happened.
    _revalidate_session = AuthState._revalidate_session
    _clear_session = AuthState._clear_session

    def _get_user_service(self):
        return UserService(AuthService(settings.secret_key))


def _guard_fn():
    handler = getattr(AuthState, GUARD)
    return getattr(handler, "fn", handler)


class TestTheGuard:
    @pytest.mark.asyncio
    async def test_it_redirects_a_valid_session(self, auth):
        """AC1.1. Red against today's code: there is no guard, so nothing redirects."""
        st = _AuthStandIn(access_token=auth.create_access_token(1, 2))
        assert _redirect_target(_one(await _guard_fn()(st))) == "/"

    @pytest.mark.asyncio
    async def test_it_passes_a_signed_out_visitor(self):
        """The polarity control. Inverting the guard makes this red and the one above green."""
        st = _AuthStandIn()
        assert await _guard_fn()(st) is None
        assert st.access_token == "", "the guard must not touch a session it did not find"

    @pytest.mark.asyncio
    async def test_it_does_not_clear_a_session_it_cannot_revalidate(self, auth):
        """Spec AC1.1: no clearing, no ``auth_error``, no redirect on the failing path.

        ``check_auth`` clears and redirects, and reusing it here would send every
        prospect back to the wall this issue exists to remove. This asserts the
        guard is not ``check_auth`` with the sign flipped.
        """
        st = _AuthStandIn(access_token="not-a-token", refresh_token="")
        assert await _guard_fn()(st) is None
        assert st.access_token == "not-a-token", "the guard cleared a session it only inspected"
        assert st.auth_error == "", "the guard set an error on a page it should render plainly"


class TestItIsActuallyRegistered:
    """A guard nobody registers is the same defect wearing a fix's clothes."""

    @staticmethod
    def _on_load_by_route() -> dict[str, list[str]]:
        tree = ast.parse(_APP.read_bytes().decode("utf-8"), filename=str(_APP))
        out: dict[str, list[str]] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or getattr(node.func, "attr", None) != "add_page":
                continue
            route, handlers = None, []
            for kw in node.keywords:
                if kw.arg == "route" and isinstance(kw.value, ast.Constant):
                    route = kw.value.value
                if kw.arg == "on_load":
                    items = kw.value.elts if isinstance(kw.value, ast.List) else [kw.value]
                    for it in items:
                        t = it.func if isinstance(it, ast.Call) else it
                        if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name):
                            handlers.append(f"{t.value.id}.{t.attr}")
            if route is not None:
                out[route] = handlers
        return out

    @pytest.mark.parametrize("route", ["/login", "/signup"])
    def test_the_credential_page_carries_the_guard(self, route):
        assert f"AuthState.{GUARD}" in self._on_load_by_route().get(route, []), (
            f"{route} does not run the guard on load, so a signed-in visitor still reaches the form"
        )

    def test_signup_keeps_prefilling_the_invited_email(self):
        """Spec: *add* to the list. Replacing it silently breaks invited signups."""
        assert "AuthState.prefill_invite_email" in self._on_load_by_route()["/signup"]

    @pytest.mark.parametrize("route", ["/forgot-password", "/reset-password"])
    def test_the_recovery_pages_stay_unguarded(self, route):
        """AC1.4. A signed-out user is the only kind that can need them."""
        assert f"AuthState.{GUARD}" not in self._on_load_by_route().get(route, [])


class _SignupState:
    """A stand-in for the ``signup`` handler, seeded with a LIVE two-org session."""

    def __init__(self, service, *, access_token="", user_orgs=()):
        self.auth_error = ""
        self.signup_blocked = ""
        self.verification_mail_state = ""
        self.invite_notice = ""
        self.access_token = access_token
        self.refresh_token = ""
        self.current_user = UserInfo(id=0, email="", full_name="")
        self.current_org = OrgInfo(id=0, name="", slug="")
        self.user_orgs = list(user_orgs)
        self.current_role = ""
        self.email = ""
        self._service = service
        self.router = SimpleNamespace(page=SimpleNamespace(params={}))

    _accept_signup_invitation = getattr(
        AuthState._accept_signup_invitation, "fn", AuthState._accept_signup_invitation
    )
    _revalidate_session = AuthState._revalidate_session
    _clear_session = AuthState._clear_session

    def _client_ip(self):
        return ""

    def _get_user_service(self):
        return self._service

    #: 🚨 **Deliberately NOT "/", and a mutation sweep is what proved it has to be.**
    #: A *successful* signup ends with ``rx.redirect(self._post_auth_redirect_target())``.
    #: With this returning "/" — the production default — the refusal and the
    #: completed signup redirect to the same place, so
    #: ``test_it_redirects_instead_of_signing_up`` passed against the **unguarded**
    #: handler: a regression test satisfied by the bug it names. Measured, not
    #: reasoned: deleting the guard from ``signup`` left that assertion green while
    #: the three beside it went red.
    #:
    #: Production's own value is legitimately "/" for a signup with no
    #: ``?template=`` (#101), so the two are indistinguishable there — which is
    #: exactly why the discriminator has to be introduced by the harness.
    _AFTER_SIGNUP = "/after-signup-landing"

    def _post_auth_redirect_target(self):
        return self._AFTER_SIGNUP


def _run_signup(db_session, auth, state) -> object:
    """Execute the real ``signup`` handler against a real database session.

    ⚠️ ``commit`` is redirected to ``flush``: ``signup`` commits its own session,
    and ``db_session`` joins an outer transaction the fixture rolls back, so a
    real commit would leak rows between tests.
    """
    session_cm = MagicMock()
    session_cm.__enter__.return_value = db_session
    session_cm.__exit__.return_value = False

    with (
        patch("datanika.ui.state.auth_state.CaptchaService") as captcha,
        patch("datanika.ui.state.auth_state.get_sync_session", return_value=session_cm),
        patch("datanika.ui.state.auth_state._allow", return_value=True),
        patch(
            "datanika.ui.state.auth_state.request_email_verification",
            return_value=SimpleNamespace(value="queued"),
        ),
        patch("datanika.ui.state.auth_state.collect_events", return_value=[]),
        patch.object(db_session, "commit", db_session.flush),
    ):
        captcha.return_value.verify.return_value = True
        return AuthState.signup.fn(
            state,
            {
                "email": EMAIL,
                "password": PASSWORD,
                "full_name": NAME,
                "captcha_token": "tok",
            },
        )


@pytest.fixture
def live_session_state(db_session, auth):
    """A signed-in session that already belongs to TWO orgs."""
    svc = UserService(auth)
    user = svc.register_user(db_session, "member@example.com", PASSWORD, "Member")
    orgs = []
    for i in (1, 2):
        org = Organization(name=f"Team {i}", slug=f"team-{i}-{user.id}")
        db_session.add(org)
        db_session.flush()
        db_session.add(Membership(user_id=user.id, org_id=org.id, role="owner"))
        orgs.append(OrgInfo(id=org.id, name=org.name, slug=org.slug))
    db_session.flush()
    state = _SignupState(
        svc,
        access_token=auth.create_access_token(user.id, orgs[0].id),
        user_orgs=orgs,
    )
    state.current_user = UserInfo(id=user.id, email=user.email, full_name=user.full_name)
    state.current_org = orgs[0]
    state.current_role = "owner"
    return state


class TestSignupRefusesALiveSession:
    """AC1.2 — defence in depth, because AC1.1 is a page-load check.

    The form can still be submitted from a tab that was signed in *after* the
    page loaded, so the guard on `on_load` cannot be the only one.
    """

    def test_it_redirects_instead_of_signing_up(self, db_session, auth, live_session_state):
        """The refusal goes to "/", which is NOT where a completed signup goes.

        See ``_SignupState._AFTER_SIGNUP``: with both targets equal this assertion
        is green against the unguarded handler.
        """
        target = _redirect_target(_one(_run_signup(db_session, auth, live_session_state)))
        assert target == "/", (
            f"expected the guard's refusal to '/', got {target!r} — "
            f"{_SignupState._AFTER_SIGNUP!r} means the signup completed"
        )

    def test_the_existing_memberships_survive(self, db_session, auth, live_session_state):
        """🔑 AC1.3, and the assertion the redirect one cannot substitute for.

        An implementation that redirects *after* the assignment passes the test
        above and fails this one — and that implementation still loses the user's
        orgs, which is the entire harm.
        """
        before = [o.id for o in live_session_state.user_orgs]
        _run_signup(db_session, auth, live_session_state)
        assert [o.id for o in live_session_state.user_orgs] == before, (
            "the live session's org memberships were replaced by the signup handler"
        )

    def test_the_session_identity_survives(self, db_session, auth, live_session_state):
        before = (live_session_state.current_user.id, live_session_state.access_token)
        _run_signup(db_session, auth, live_session_state)
        after = (live_session_state.current_user.id, live_session_state.access_token)
        assert after == before, "the signup handler re-identified a live session"

    def test_no_account_is_created(self, db_session, auth, live_session_state):
        """The database control. A refusal that still writes a row is not a refusal.

        🚨 It also pins that the guard runs **before** ``register_user`` — the
        spec says before the CAPTCHA check and before any database read.
        """
        from datanika.models.user import User

        _run_signup(db_session, auth, live_session_state)
        assert db_session.query(User).filter(User.email == EMAIL).count() == 0

    def test_it_does_not_sign_the_user_out_first(self, db_session, auth, live_session_state):
        """*"Sign them out, then sign them up"* is the same substitution, renamed."""
        _run_signup(db_session, auth, live_session_state)
        assert live_session_state.access_token != "", "the handler cleared the live session"

    def test_a_signed_out_signup_still_works(self, db_session, auth):
        """🔑 The control. Without it, every assertion above is satisfied by a
        handler that refuses everyone and nobody can create an account at all."""
        from datanika.models.user import User

        state = _SignupState(UserService(auth))
        result = _run_signup(db_session, auth, state)
        assert db_session.query(User).filter(User.email == EMAIL).count() == 1, (
            "an ordinary signup no longer creates an account"
        )
        assert state.access_token, "an ordinary signup no longer establishes a session"
        assert state.auth_error == "", f"an ordinary signup errored: {state.auth_error!r}"
        # And it lands somewhere OTHER than the refusal's "/", which is what makes
        # the redirect assertion above able to fail. If this ever equals "/", that
        # assertion has quietly gone vacuous again.
        assert _redirect_target(result[-1]) == _SignupState._AFTER_SIGNUP


class TestGivingLoginAnOnLoadIsSafe:
    """SPEC_PAGE_ENTRY §8 says to land §2 after §4 because `/login` would otherwise
    *"spend a release with the window and a bare spinner"*. **Measured: `/login` has
    no spinner branch to spend it in.**

    The 1.5-9.5 s window is a *contentful-paint* gap, and on a protected page what
    fills it is `page_layout`'s `rx.cond(AuthState.is_authenticated, <app>,
    <bare spinner>)`. `/login` does not route through `page_layout` at all — it
    imports only `legal_links` from that module and returns its own
    `rx.center(rx.vstack(...))`, whose logo and headings sit outside every
    `rx.cond`. So §4's skeleton could never have covered `/login`, and the stated
    reason for the ordering does not hold.

    This is the assertion that replaces it, and it is deliberately the *narrow*
    claim: not "`/login` is fast" (unmeasured, and not ours to measure), but "the
    thing §4 changes is not on this page". It goes red the day someone converts
    `/login` to `page_layout` — which is exactly when the constraint becomes real
    again.
    """

    @pytest.mark.parametrize("page", ["login", "signup"])
    def test_the_credential_pages_do_not_route_through_page_layout(self, page):
        src = (
            (
                pathlib.Path(__file__).resolve().parents[2]
                / "datanika"
                / "ui"
                / "pages"
                / f"{page}.py"
            )
            .read_bytes()
            .decode("utf-8")
        )
        tree = ast.parse(src)
        called = {
            getattr(n.func, "id", None) or getattr(n.func, "attr", None)
            for n in ast.walk(tree)
            if isinstance(n, ast.Call)
        }
        assert "page_layout" not in called, (
            f"/{page} now renders through page_layout, so it inherits the hydrating "
            "spinner branch — SPEC_PAGE_ENTRY §8's ordering constraint is live again"
        )

    def test_the_protected_layout_really_does_gate_on_auth(self):
        """The control for the claim above: `page_layout` IS the thing that gates.

        Without this, *"login does not call page_layout"* is a fact about a name.
        """
        src = (
            (
                pathlib.Path(__file__).resolve().parents[2]
                / "datanika"
                / "ui"
                / "components"
                / "layout.py"
            )
            .read_bytes()
            .decode("utf-8")
        )
        assert "AuthState.is_authenticated" in src and "rx.spinner" in src, (
            "page_layout no longer gates its content on auth with a spinner, so the "
            "asymmetry this class asserts has changed and needs re-deriving"
        )
