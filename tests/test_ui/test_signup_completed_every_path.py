"""``user.signup_completed`` fires once for every new account, on every signup path (core#1369).

``SPEC_SIGNUP_SOCIAL_AUTH`` §8h. The event was emitted only by the password ``signup()`` handler,
so a signup through Google, GitHub or SSO never reached a subscriber — the paths that spec exists
to make the primary ones. Cloud's ``handle_signup_conversion`` is the subscriber in production.

The four properties, each driven through the REAL completion path — the provider callback, then
``AuthState.handle_oauth_complete`` fed the callback's own redirect — never the emit function:

* **AC15** a new Google/GitHub/SSO account emits once, with the new user's id; a returning user
  on the same path emits nothing; and a password signup still emits (the control).
* **AC16** a completion URL carrying ``is_new=1`` for an **existing** account emits nothing: the
  fact is the backend's, and the query string is the user's to edit.
* **AC17** reloading the completion route for a freshly-created account emits once, not once per
  load — ``handle_oauth_complete`` is an ``on_load`` handler and re-runs on a reload.
* and the one this change could newly break: **analytics must not be able to break a sign-in.**
  With the marker store down, the callback still redirects and the page still completes.

Only the provider's HTTP surface is replaced. ``OAuthService``, ``UserService`` and the rows are
real, so ``is_new`` here is the value ``find_or_create_oauth_user`` actually computes.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from datanika import hooks
from datanika.config import settings
from datanika.models.user import Organization, User
from datanika.services import oauth_routes, signup_conversion, sso_routes
from datanika.services.auth import AuthService
from datanika.services.oauth_service import OAuthService, github_provider, google_provider
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState

SENTINEL = "conversion-event-from-a-plugin"


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class _FakeRedis:
    """The two commands the marker uses, with real semantics: ``DEL`` counts what it removed."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def setex(self, key, ttl, value):
        self.store[key] = value
        self.ttls[key] = ttl
        return True

    def delete(self, *keys):
        removed = 0
        for key in keys:
            if self.store.pop(key, None) is not None:
                removed += 1
        return removed


@pytest.fixture(autouse=True)
def _commits_are_flushes(db_session):
    """Both callbacks ``commit()``. On the SQLite harness a commit inside ``db_session``'s savepoint
    persists past the test's rollback — measured: the second test found the first test's account
    (``is_new=0``) and its org slug. A flush keeps every row this module writes inside the test."""
    with patch.object(db_session, "commit", side_effect=db_session.flush):
        yield


@pytest.fixture
def marker_store():
    fake = _FakeRedis()
    with patch.object(signup_conversion, "_redis", return_value=fake):
        yield fake


@pytest.fixture
def emitted():
    """A subscriber that records every emit and contributes one event, as cloud's handler does."""
    calls: list[dict] = []

    def _subscriber(**kwargs):
        calls.append(kwargs)
        return SENTINEL

    hooks.on("user.signup_completed", _subscriber)
    try:
        yield calls
    finally:
        hooks.off("user.signup_completed", _subscriber)


class _Session:
    """``get_sync_session()`` / ``_get_session()`` as a context manager around the test session."""

    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        return False


def _auth():
    # The completion page decodes with `settings.secret_key`, so the callback must sign with it.
    return AuthService(settings.secret_key)


class _CompleteState:
    """Only what ``handle_oauth_complete`` touches — not a MagicMock, which answers everything."""

    _post_auth_redirect_target = getattr(
        AuthState._post_auth_redirect_target, "fn", AuthState._post_auth_redirect_target
    )

    def __init__(self, params: dict):
        self.router = SimpleNamespace(page=SimpleNamespace(params=params))
        self.auth_error = ""
        self.access_token = ""
        self.refresh_token = ""
        self.current_user = None
        self.current_org = SimpleNamespace(id=0, name="", slug="")
        self.user_orgs = []
        self.current_role = ""
        self.invite_notice = ""

    def _get_user_service(self):
        return UserService(_auth())

    def _load_current_role(self, user_id, org_id):
        self.current_role = "owner"


def _complete(db_session, params: dict):
    """Load ``/auth/complete`` once with ``params``, as the ``on_load`` handler does."""
    state = _CompleteState(params)
    with patch("datanika.ui.state.auth_state.get_sync_session", return_value=_Session(db_session)):
        result = AuthState.handle_oauth_complete.fn(state)
    events = result if isinstance(result, list) else [result]
    return state, events


def _params(location: str) -> dict[str, str]:
    parsed = urlparse(location)
    assert parsed.path.endswith("/auth/complete"), f"callback did not complete: {location}"
    return {key: values[0] for key, values in parse_qs(parsed.query).items()}


async def _social_callback(db_session, provider_name: str, *, email: str, sub: str) -> dict:
    """One completed Google/GitHub callback through ``oauth_routes.oauth_callback``."""
    provider = {"google": google_provider, "github": github_provider}[provider_name]("id", "secret")
    service = OAuthService(_auth(), UserService(_auth()))
    service._exchange_code = AsyncMock(return_value={"access_token": "provider-token"})
    service._fetch_userinfo = AsyncMock(
        return_value={"email": email, "email_verified": True, "name": "Nina", "sub": sub}
    )
    service._resolve_verified_email = AsyncMock(return_value=email)

    request = SimpleNamespace(
        path_params={"provider": provider_name},
        query_params={"code": "abc", "state": "st"},
        cookies={"oauth_state": f"st:{oauth_routes._sign_state('st')}"},
    )
    with (
        patch.object(oauth_routes, "_get_providers", return_value={provider_name: provider}),
        patch.object(oauth_routes, "_get_service", return_value=service),
        patch.object(oauth_routes, "_get_session", return_value=_Session(db_session)),
    ):
        response = await oauth_routes.oauth_callback(request)
    return _params(response.headers["location"])


@pytest.fixture
def sso_org(db_session):
    org = Organization(name="Acme SSO", slug="acme-sso")
    db_session.add(org)
    db_session.flush()
    return org


async def _sso_callback(db_session, org_slug: str, *, email: str) -> dict:
    """One completed OIDC callback through ``sso_routes.sso_callback``."""
    config = MagicMock()
    config.is_active = True
    config.protocol.value = "oidc"
    service = MagicMock()
    service.get_sso_config_by_org_slug.return_value = config

    request = SimpleNamespace(
        cookies={"sso_state": f"{org_slug}:st:{sso_routes._sign_state('st')}"},
        query_params={},
    )
    with (
        patch.object(sso_routes, "_get_session", return_value=_Session(db_session)),
        patch.object(sso_routes, "_sso_service", return_value=service),
        patch.object(sso_routes, "_oidc_exchange", AsyncMock(return_value=(email, "Sam", True))),
    ):
        response = await sso_routes.sso_callback(request)
    return _params(response.headers["location"])


def _user_id(db_session, email: str) -> int:
    return db_session.query(User).filter(User.email == email).one().id


def _conversions(events) -> int:
    return sum(1 for event in events if event == SENTINEL)


# ---------------------------------------------------------------------------
# AC15 — a new account emits once, with its id; a returning one emits nothing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["google", "github"])
async def test_a_new_social_account_emits_once_with_its_id(
    db_session, marker_store, emitted, provider
):
    """Red before core#1369: the social completion emitted nothing at all."""
    params = await _social_callback(db_session, provider, email="nina@example.com", sub="n-1")
    assert params["is_new"] == "1", "harness: this callback should have created the account"

    _state, events = _complete(db_session, params)

    assert emitted == [{"user_id": _user_id(db_session, "nina@example.com")}]
    assert _conversions(events) == 1, (
        "the subscriber's event must be RETURNED by the handler — that is the only way a Reflex "
        "event reaches the browser"
    )


@pytest.mark.asyncio
async def test_a_new_sso_account_emits_once_with_its_id(db_session, marker_store, emitted, sso_org):
    params = await _sso_callback(db_session, sso_org.slug, email="sam@acme.example")
    assert params["is_new"] == "1", "harness: this callback should have created the account"

    _state, events = _complete(db_session, params)

    assert emitted == [{"user_id": _user_id(db_session, "sam@acme.example")}]
    assert _conversions(events) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["google", "github"])
async def test_a_returning_social_user_emits_nothing(db_session, marker_store, emitted, provider):
    first = await _social_callback(db_session, provider, email="nina@example.com", sub="n-1")
    _complete(db_session, first)
    emitted.clear()

    again = await _social_callback(db_session, provider, email="nina@example.com", sub="n-1")
    assert again["is_new"] == "0", "harness: the second callback should find the account"

    _state, events = _complete(db_session, again)

    assert emitted == []
    assert _conversions(events) == 0


@pytest.mark.asyncio
async def test_a_returning_sso_user_emits_nothing(db_session, marker_store, emitted, sso_org):
    _complete(db_session, await _sso_callback(db_session, sso_org.slug, email="sam@acme.example"))
    emitted.clear()

    again = await _sso_callback(db_session, sso_org.slug, email="sam@acme.example")
    assert again["is_new"] == "0"

    _state, events = _complete(db_session, again)

    assert emitted == []


def test_control_a_password_signup_still_emits(emitted):
    """The one path that already worked must keep working (AC15's control).

    Green before and after this change, by design: it is here so that wiring the social path
    cannot quietly un-wire the password one.
    """
    user = SimpleNamespace(id=41, email="pat@example.com", full_name="Pat")
    fake_svc = MagicMock()
    fake_svc.register_user.return_value = user
    fake_svc.create_org.return_value = SimpleNamespace(id=410, name="Pat's Org", slug="org-41")
    fake_svc.authenticate.return_value = {
        "access_token": _auth().create_access_token(41, 410),
        "refresh_token": "r",
        "user": user,
    }
    state = SimpleNamespace(
        auth_error="",
        signup_blocked="",
        invite_notice="",
        verification_mail_state="",
        router=SimpleNamespace(page=SimpleNamespace(params={})),
        current_user=None,
        current_org=None,
        user_orgs=[],
        current_role="",
        access_token="",
        refresh_token="",
        _revalidate_session=lambda: False,
        _client_ip=lambda: "",
        _get_user_service=lambda: fake_svc,
        _accept_signup_invitation=lambda session, token, user_id: None,
        _post_auth_redirect_target=lambda: "/",
    )
    session_cm = MagicMock()
    session_cm.__enter__.return_value = MagicMock()
    session_cm.__exit__.return_value = False
    with (
        patch("datanika.ui.state.auth_state.CaptchaService") as captcha,
        patch("datanika.ui.state.auth_state.get_sync_session", return_value=session_cm),
        patch("datanika.ui.state.auth_state._allow", return_value=True),
        patch(
            "datanika.ui.state.auth_state.request_email_verification",
            return_value=SimpleNamespace(value="sent"),
        ),
    ):
        captcha.return_value.verify.return_value = True
        result = AuthState.signup.fn(
            state,
            {
                "email": "pat@example.com",
                "password": "pw12345678",
                "full_name": "Pat",
                "captcha_token": "tok",
            },
        )

    assert state.auth_error == "", f"harness: the signup did not succeed ({state.auth_error!r})"
    assert emitted == [{"user_id": 41}]
    assert _conversions(result) == 1


# ---------------------------------------------------------------------------
# AC16 — the fact is the server's, not the URL's
# ---------------------------------------------------------------------------


def test_a_forged_is_new_for_an_existing_account_emits_nothing(db_session, marker_store, emitted):
    user = UserService(_auth()).register_user(
        db_session, "old@example.com", "old-password-1", "Old"
    )
    org = UserService(_auth()).create_org(db_session, "Old Org", "old-org", user.id)
    db_session.flush()

    _state, events = _complete(
        db_session,
        {"token": _auth().create_access_token(user.id, org.id), "refresh": "r", "is_new": "1"},
    )

    assert emitted == [], "a completion URL claiming is_new=1 fired a conversion"
    assert _conversions(events) == 0


def test_a_marker_is_not_claimed_by_a_request_that_fails_to_sign_in(
    db_session, marker_store, emitted
):
    """The claim happens only after the token verifies and the user exists — so an invalid
    completion cannot spend a real new account's conversion."""
    signup_conversion.mark_signup_completed(7)

    _state, events = _complete(db_session, {"token": "not-a-jwt", "refresh": "r", "is_new": "1"})

    assert emitted == []
    assert marker_store.store, "a request that never signed in consumed the marker"


# ---------------------------------------------------------------------------
# AC17 — exactly once under on_load
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reloading_the_completion_route_emits_once(db_session, marker_store, emitted):
    params = await _social_callback(db_session, "google", email="nina@example.com", sub="n-1")

    for _load in range(3):
        _state, events = _complete(db_session, params)

    assert len(emitted) == 1, f"the completion page emitted {len(emitted)} times over 3 loads"


# ---------------------------------------------------------------------------
# Analytics must not be able to break a sign-in
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_with_the_marker_store_down_sign_in_still_completes(db_session, emitted):
    with patch.object(signup_conversion, "_redis", side_effect=RuntimeError("redis is down")):
        params = await _social_callback(db_session, "google", email="nina@example.com", sub="n-1")
        state, events = _complete(db_session, params)

    assert state.access_token, "the sign-in did not complete"
    assert state.auth_error == ""
    assert emitted == [], "no marker could be written, so nothing may fire"
    assert events and _conversions(events) == 0


@pytest.mark.asyncio
async def test_a_subscriber_that_raises_does_not_strand_the_user(db_session, marker_store):
    def _broken(**_kwargs):
        raise RuntimeError("plugin bug")

    hooks.on("user.signup_completed", _broken)
    try:
        params = await _social_callback(db_session, "google", email="nina@example.com", sub="n-1")
        state, events = _complete(db_session, params)
    finally:
        hooks.off("user.signup_completed", _broken)

    assert state.access_token
    assert events, "the completion returned nothing — the user is left on /auth/complete"


def test_the_marker_expires_within_the_flows_own_lifetime(marker_store):
    """A marker for a flow that never reached the page must not fire on a later sign-in."""
    signup_conversion.mark_signup_completed(9)

    (ttl,) = marker_store.ttls.values()
    assert 0 < ttl <= 600
