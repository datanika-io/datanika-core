"""Social login must bind an account to a *provider identity*, not to a claimed email.

An OAuth provider tells us two things about the person at the other end: an opaque
subject id, and an email address. Only the first is an identity. The second is a
claim, and it is only worth anything when the provider also says it verified it.

These tests pin three properties that together decide whether a stranger can be
handed someone else's session:

1. ``_fetch_github_email`` never yields an address GitHub has not verified.
2. The OIDC/Google branch refuses an ``email_verified`` that is false or absent.
3. ``find_or_create_oauth_user`` matches on ``(provider, provider_id)`` first,
   compares the stored subject on *every* login, and will not touch an existing
   account on the strength of an unverified email.

Companion write-up: ``plans/security/OAUTH_EMAIL_TRUST_2026-08-30.md`` (local).
Precedent for the shape of this file: ``test_sso_saml_forgery.py``.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from datanika.services.auth import AuthService
from datanika.services.oauth_service import (
    OAuthError,
    OAuthService,
    github_provider,
    google_provider,
)
from datanika.services.user_service import UserService, UserServiceError
from tests.factories import make_user

# ---------------------------------------------------------------------------
# Fakes for the provider HTTP surface
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected HTTP {self.status_code}")


class _FakeAsyncClient:
    """Routes GETs by URL fragment. Any unrouted call is a test bug, not a pass."""

    def __init__(self, routes: dict):
        self._routes = routes

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def get(self, url, **_kw):
        for fragment, response in self._routes.items():
            if fragment in url:
                return response
        raise AssertionError(f"unrouted GET {url}")

    async def post(self, url, **_kw):
        for fragment, response in self._routes.items():
            if fragment in url:
                return response
        raise AssertionError(f"unrouted POST {url}")


def _patch_http(monkeypatch, routes: dict) -> None:
    monkeypatch.setattr(
        "datanika.services.oauth_service.httpx.AsyncClient",
        lambda *_a, **_k: _FakeAsyncClient(routes),
    )


@pytest.fixture
def auth():
    return AuthService(secret_key="test-secret-key-for-oauth-trust")


@pytest.fixture
def mock_auth():
    a = MagicMock()
    a.create_access_token.return_value = "jwt_access"
    a.create_refresh_token.return_value = "jwt_refresh"
    return a


@pytest.fixture
def svc(mock_auth):
    return OAuthService(mock_auth, MagicMock())


@pytest.fixture
def user_svc(auth):
    return UserService(auth)


@pytest.fixture
def google():
    return google_provider("gid", "gsecret")


@pytest.fixture
def github():
    return github_provider("ghid", "ghsecret")


# ---------------------------------------------------------------------------
# 1. GitHub: an unverified address is never yielded
# ---------------------------------------------------------------------------
class TestGitHubEmailIsVerifiedOrNothing:
    @pytest.mark.asyncio
    async def test_unverified_addresses_yield_nothing(self, svc, monkeypatch):
        """No address is both primary and verified -> no address at all.

        The old code fell through to ``emails[0]`` in exactly this branch, i.e.
        it answered only in the case where it had no trustworthy answer.
        """
        _patch_http(
            monkeypatch,
            {
                "/user/emails": _FakeResponse(
                    200,
                    [
                        {"email": "victim@company.com", "primary": False, "verified": False},
                        {"email": "attacker@example.net", "primary": True, "verified": False},
                    ],
                )
            },
        )
        assert await svc._fetch_github_email("tok") is None

    @pytest.mark.asyncio
    async def test_verified_but_not_primary_yields_nothing(self, svc, monkeypatch):
        """Picking *some* verified address is a product decision nobody made."""
        _patch_http(
            monkeypatch,
            {
                "/user/emails": _FakeResponse(
                    200,
                    [
                        {"email": "victim@company.com", "primary": True, "verified": False},
                        {"email": "side@example.net", "primary": False, "verified": True},
                    ],
                )
            },
        )
        assert await svc._fetch_github_email("tok") is None

    @pytest.mark.asyncio
    async def test_primary_verified_is_returned(self, svc, monkeypatch):
        """Control: the supported case still works."""
        _patch_http(
            monkeypatch,
            {
                "/user/emails": _FakeResponse(
                    200,
                    [
                        {"email": "other@example.net", "primary": False, "verified": True},
                        {"email": "me@example.com", "primary": True, "verified": True},
                    ],
                )
            },
        )
        assert await svc._fetch_github_email("tok") == "me@example.com"

    @pytest.mark.asyncio
    async def test_public_profile_email_is_not_trusted(self, svc, github, monkeypatch):
        """``/user`` carries an ``email`` field and no verification flag.

        Short-circuiting on it skips the only endpoint that reports ``verified``,
        so the authoritative list must be consulted for GitHub either way.
        """
        _patch_http(
            monkeypatch,
            {
                "/user/emails": _FakeResponse(
                    200, [{"email": "victim@company.com", "primary": True, "verified": False}]
                )
            },
        )
        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(
            return_value={"email": "victim@company.com", "login": "who", "id": 1}
        )

        with pytest.raises(OAuthError, match="verified"):
            await svc.handle_callback(github, "code", "http://cb", MagicMock())


# ---------------------------------------------------------------------------
# 2. Google / OIDC: email_verified is read, and absence fails closed
# ---------------------------------------------------------------------------
class TestGoogleEmailVerifiedClaim:
    @pytest.mark.asyncio
    async def test_explicitly_unverified_is_refused(self, svc, google):
        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(
            return_value={"email": "victim@company.com", "email_verified": False, "sub": "9"}
        )
        with pytest.raises(OAuthError, match="verified"):
            await svc.handle_callback(google, "code", "http://cb", MagicMock())

    @pytest.mark.asyncio
    async def test_absent_claim_is_refused(self, svc, google):
        """Missing is not the same as true. Fail closed."""
        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(return_value={"email": "victim@company.com", "sub": "9"})
        with pytest.raises(OAuthError, match="verified"):
            await svc.handle_callback(google, "code", "http://cb", MagicMock())

    @pytest.mark.asyncio
    async def test_verified_claim_signs_in(self, svc, google, mock_auth):
        """Control. Also pins that the verification verdict reaches the user layer."""
        user = MagicMock(id=1)
        svc._user.find_or_create_oauth_user.return_value = (user, False)
        svc._user.get_user_orgs.return_value = [MagicMock(id=10)]

        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(
            return_value={
                "email": "me@example.com",
                "email_verified": True,
                "name": "Me",
                "sub": "9",
            }
        )
        session = MagicMock()
        result = await svc.handle_callback(google, "code", "http://cb", session)

        assert result["access_token"] == "jwt_access"
        _, kwargs = svc._user.find_or_create_oauth_user.call_args
        assert kwargs.get("email_verified") is True, (
            "handle_callback must tell the user layer whether the provider "
            "verified the address; otherwise the check stops at this file"
        )

    @pytest.mark.asyncio
    async def test_string_true_is_accepted(self, svc, google):
        """Some OIDC providers serialise the claim as a string."""
        svc._user.find_or_create_oauth_user.return_value = (MagicMock(id=1), False)
        svc._user.get_user_orgs.return_value = [MagicMock(id=10)]
        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(
            return_value={"email": "me@example.com", "email_verified": "true", "sub": "9"}
        )
        result = await svc.handle_callback(google, "code", "http://cb", MagicMock())
        assert result["access_token"] == "jwt_access"


# ---------------------------------------------------------------------------
# 3. The user layer: identity first, email second, unverified never
# ---------------------------------------------------------------------------
@pytest.fixture
def password_user(db_session, auth):
    """An ordinary email+password account that never opted into social login."""
    u = make_user(
        db_session,
        email="victim@company.com",
        password_hash=auth.hash_password("password123"),
        full_name="Victim",
        email_verified=True,
    )
    return u


class TestFindOrCreateOAuthUserTrust:
    def test_unverified_email_cannot_reach_an_existing_account(
        self, user_svc, db_session, password_user
    ):
        with pytest.raises(UserServiceError):
            user_svc.find_or_create_oauth_user(
                db_session,
                "victim@company.com",
                "Not The Victim",
                "github",
                "attacker-subject",
                email_verified=False,
            )
        db_session.refresh(password_user)
        assert password_user.oauth_provider is None, (
            "a refused login must leave no trace on the account it targeted"
        )

    def test_unverified_email_cannot_create_an_account(self, user_svc, db_session):
        """Otherwise an attacker squats an address before its owner signs up."""
        with pytest.raises(UserServiceError):
            user_svc.find_or_create_oauth_user(
                db_session,
                "notyet@company.com",
                "Squatter",
                "github",
                "squatter-subject",
                email_verified=False,
            )
        assert user_svc.get_user_by_email(db_session, "notyet@company.com") is None

    def test_verified_email_omitted_defaults_to_refusing(self, user_svc, db_session):
        """The parameter is keyword-only with a false default: a caller that
        forgets it fails closed rather than silently trusting."""
        with pytest.raises(UserServiceError):
            user_svc.find_or_create_oauth_user(
                db_session, "someone@company.com", "Someone", "google", "sub-1"
            )

    def test_provider_identity_wins_over_email(self, user_svc, db_session):
        """A user who changed their email at the provider is the same user.

        Email-first lookup answers 'no such user' here and creates a second
        account; identity-first answers correctly.
        """
        created, is_new = user_svc.find_or_create_oauth_user(
            db_session, "before@example.com", "Mover", "google", "sub-stable", email_verified=True
        )
        assert is_new is True

        found, is_new_again = user_svc.find_or_create_oauth_user(
            db_session, "after@example.com", "Mover", "google", "sub-stable", email_verified=True
        )
        assert is_new_again is False
        assert found.id == created.id

    def test_same_provider_different_subject_is_refused(self, user_svc, db_session):
        """The stored subject is compared on every login, not written once.

        Same provider, same email, *different* person: that is the provider
        telling us the address changed hands.
        """
        user_svc.find_or_create_oauth_user(
            db_session, "shared@example.com", "First", "google", "sub-first", email_verified=True
        )
        with pytest.raises(UserServiceError):
            user_svc.find_or_create_oauth_user(
                db_session,
                "shared@example.com",
                "Second",
                "google",
                "sub-second",
                email_verified=True,
            )

    def test_verified_email_links_a_password_account(self, user_svc, db_session, password_user):
        """Control, and a deliberate product decision: auto-linking stays, but
        only for an address the provider actually verified."""
        linked, is_new = user_svc.find_or_create_oauth_user(
            db_session,
            "victim@company.com",
            "Victim",
            "github",
            "victim-subject",
            email_verified=True,
        )
        assert is_new is False
        assert linked.id == password_user.id
        assert linked.oauth_provider == "github"
        assert linked.oauth_provider_id == "victim-subject"

    def test_legacy_row_without_subject_is_bound_late(self, user_svc, db_session, auth):
        """Rows linked before the subject was recorded must not lock the user out."""
        u = make_user(
            db_session,
            email="legacy@example.com",
            password_hash=auth.hash_password("x"),
            full_name="Legacy",
            email_verified=True,
            oauth_provider="google",
            oauth_provider_id=None,
        )

        found, is_new = user_svc.find_or_create_oauth_user(
            db_session, "legacy@example.com", "Legacy", "google", "sub-late", email_verified=True
        )
        assert is_new is False
        assert found.id == u.id
        assert found.oauth_provider_id == "sub-late"

    def test_empty_subject_does_not_match_everyone(self, user_svc, db_session, auth):
        """An empty provider_id must never be used as a lookup key."""
        u = make_user(
            db_session,
            email="blank@example.com",
            password_hash=auth.hash_password("x"),
            full_name="Blank",
            email_verified=True,
            oauth_provider="google",
            oauth_provider_id="",
        )

        found, _ = user_svc.find_or_create_oauth_user(
            db_session, "someone-else@example.com", "Other", "google", "", email_verified=True
        )
        assert found.id != u.id, "an empty subject matched an unrelated account"


# ---------------------------------------------------------------------------
# 4. The composed path, end to end
# ---------------------------------------------------------------------------
class TestComposedTakeoverIsBlocked:
    @pytest.mark.asyncio
    async def test_unverified_github_email_does_not_yield_the_victims_session(
        self, mock_auth, user_svc, db_session, password_user, github, monkeypatch
    ):
        """The whole thing, with the real UserService and the real GitHub branch.

        Each of the three parts is arguable alone; this is the composition, and
        it is the only test here that fails if any one of them regresses.
        """
        svc = OAuthService(mock_auth, user_svc)
        _patch_http(
            monkeypatch,
            {
                "/user/emails": _FakeResponse(
                    200,
                    [{"email": "victim@company.com", "primary": False, "verified": False}],
                )
            },
        )
        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(return_value={"login": "attacker", "id": 4242})

        with pytest.raises(OAuthError):
            await svc.handle_callback(github, "code", "http://cb", db_session)

        mock_auth.create_access_token.assert_not_called()
        db_session.refresh(password_user)
        assert password_user.oauth_provider is None
