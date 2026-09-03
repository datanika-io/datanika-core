"""Linking a provider identity onto an existing account is a **two-sided** decision.

``test_oauth_email_trust.py`` pins one side: the provider must say it verified the
address before that address may reach a local account. This file pins the other,
which that work did not ask about — **whether the local account ever proved the
address itself.**

It never did. ``email_verified`` defaults to ``False`` (``models/user.py``),
``register_user`` does not set it, and until this change nothing called the
verification mail that would. So "an account exists at this address" carried no
evidence that its holder controls the address, and the auto-link branch treated it
as if it did.

The rule both sides compose into:

    A provider identity may be bound to an existing account only when the
    provider has proven the address **and** the account has proven it too —
    or the account has no password of its own to hand over.

Companion write-up: ``plans/security/OAUTH_AUTOLINK_UNVERIFIED_LOCAL_2026-08-30.md``
(local, deliberately not in the public tracker). Precedent for the shape of this
file: ``test_oauth_email_trust.py``.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.oauth_service import OAuthService, github_provider
from datanika.services.user_service import UserService, UserServiceError
from tests.factories import make_user


@pytest.fixture
def auth():
    return AuthService("test-secret-key-autolink")


@pytest.fixture
def user_svc(auth):
    return UserService(auth)


@pytest.fixture
def unproven_local_account(db_session, auth):
    """Exactly what ``register_user`` produces today: a password account whose
    address nobody ever checked.

    Both columns are stated explicitly rather than left to defaults, because the
    whole point of the test is which of them the link decision reads.

    The org and membership are **load-bearing, not scenery**: without them the
    composed test below fails inside ``handle_callback`` on "User has no
    organization" — an incidental error that looks like a pass and proves
    nothing. ``register_user``'s real caller creates an org, so this is also the
    accurate shape of a squatted row.
    """
    u = make_user(
        db_session,
        email="victim@company.com",
        password_hash=auth.hash_password("attacker-chosen-password"),
        full_name="Not The Victim",
        email_verified=False,
        password_changed_at=datetime.now(UTC),
    )
    org = Organization(name="Squatted Org", slug=f"squatted-{u.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=u.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return u


class TestAutoLinkRequiresBothSides:
    def test_unproven_local_account_is_not_handed_to_a_verified_provider_email(
        self, user_svc, db_session, unproven_local_account
    ):
        """The regression test, and it is the whole attack rather than a part of it.

        The provider here is honest: it really did verify ``victim@company.com``,
        and the person signing in really does own that inbox. The lie is on our
        side of the join — the row we would match them to was created by someone
        who typed that address and was never asked to prove it.

        Against the unfixed code this returns the squatter's ``User``, so the
        assertion fails on a real takeover and not on some incidental error.
        """
        with pytest.raises(UserServiceError):
            user_svc.find_or_create_oauth_user(
                db_session,
                "victim@company.com",
                "Victim",
                "github",
                "victim-provider-subject",
                email_verified=True,
            )

        db_session.refresh(unproven_local_account)
        assert unproven_local_account.oauth_provider is None, (
            "a refused link must leave no trace on the account it targeted"
        )
        assert unproven_local_account.oauth_provider_id is None

    def test_the_refusal_names_a_way_back(self, user_svc, db_session, unproven_local_account):
        """A refusal a legitimate user cannot act on is a lockout.

        Both remedies exist today: sign in with the password, or run the password
        reset, which proves inbox control and marks the address verified.
        """
        with pytest.raises(UserServiceError) as exc:
            user_svc.find_or_create_oauth_user(
                db_session,
                "victim@company.com",
                "Victim",
                "github",
                "victim-provider-subject",
                email_verified=True,
            )
        message = str(exc.value).lower()
        assert "password" in message, (
            f"the message must route the user to a way of proving control, got: {exc.value!r}"
        )

    def test_a_proven_local_account_still_auto_links(self, user_svc, db_session, auth):
        """Negative control, or (a) gets implemented as 'deny everything'.

        Auto-linking a verified provider email onto a verified password account
        remains a deliberate product decision (SPEC_SIGNUP_SOCIAL_AUTH.md).
        """
        u = make_user(
            db_session,
            email="proven@company.com",
            password_hash=auth.hash_password("their-own-password"),
            full_name="Proven",
            email_verified=True,
            password_changed_at=datetime.now(UTC),
        )

        linked, is_new = user_svc.find_or_create_oauth_user(
            db_session,
            "proven@company.com",
            "Proven",
            "github",
            "proven-subject",
            email_verified=True,
        )
        assert is_new is False
        assert linked.id == u.id
        assert linked.oauth_provider == "github"
        assert linked.oauth_provider_id == "proven-subject"

    def test_an_account_with_no_password_of_its_own_still_links(self, user_svc, db_session, auth):
        """The second half of the condition, and it is not decoration.

        ``password_changed_at IS NULL`` means no human ever chose a password for
        this row, so there is no password login to hand over — the only thing the
        link can grant is access to someone who has already proven the address.
        Refusing here would break rows the product creates on purpose.
        """
        u = make_user(
            db_session,
            email="nopassword@company.com",
            password_hash=auth.hash_password("machine-generated"),
            full_name="No Password",
            email_verified=False,
            password_changed_at=None,
        )

        linked, is_new = user_svc.find_or_create_oauth_user(
            db_session,
            "nopassword@company.com",
            "No Password",
            "google",
            "np-subject",
            email_verified=True,
        )
        assert is_new is False
        assert linked.id == u.id
        assert linked.oauth_provider == "google"

    def test_an_already_linked_account_is_unaffected(self, user_svc, db_session, auth):
        """The guard sits on the auto-link branch only.

        A row that already carries this provider is matched by identity and must
        keep working, verified or not — otherwise every user who linked a
        provider before this change is locked out of social login.
        """
        u = make_user(
            db_session,
            email="alreadylinked@company.com",
            password_hash=auth.hash_password("x"),
            full_name="Already Linked",
            email_verified=False,
            password_changed_at=datetime.now(UTC),
            oauth_provider="google",
            oauth_provider_id="linked-subject",
        )

        found, is_new = user_svc.find_or_create_oauth_user(
            db_session,
            "alreadylinked@company.com",
            "Already Linked",
            "google",
            "linked-subject",
            email_verified=True,
        )
        assert is_new is False
        assert found.id == u.id

    def test_creating_a_fresh_account_is_unaffected(self, user_svc, db_session):
        """No existing row means no local claim to weigh — creation is untouched."""
        created, is_new = user_svc.find_or_create_oauth_user(
            db_session,
            "brandnew@company.com",
            "Brand New",
            "google",
            "new-subject",
            email_verified=True,
        )
        assert is_new is True
        assert created.email_verified is True


class TestComposedTakeoverIsBlocked:
    @pytest.mark.asyncio
    async def test_an_honest_provider_does_not_yield_the_squatters_session(
        self, user_svc, db_session, unproven_local_account, monkeypatch
    ):
        """The composition, through the real ``OAuthService`` and the real GitHub
        branch, with a provider that behaves perfectly.

        This is the test that fails if either side of the rule regresses, and the
        only one here that would notice the guard being bypassed by a caller
        rather than removed from the callee.
        """
        mock_auth = MagicMock(spec=AuthService)
        mock_auth.create_access_token.return_value = "jwt_access"
        mock_auth.create_refresh_token.return_value = "jwt_refresh"
        mock_auth.hash_password.side_effect = lambda p: f"hashed::{p}"

        svc = OAuthService(mock_auth, user_svc)
        github = github_provider("cid", "csecret")

        class _Resp:
            status_code = 200

            @staticmethod
            def json():
                # GitHub's own answer, and it is the truth: the address is
                # verified and it is the primary one.
                return [{"email": "victim@company.com", "primary": True, "verified": True}]

            @staticmethod
            def raise_for_status():
                return None

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, *a, **kw):
                return _Resp()

        monkeypatch.setattr("httpx.AsyncClient", lambda *a, **kw: _Client())
        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(
            return_value={"login": "victim", "id": 4242, "email": "victim@company.com"}
        )

        with pytest.raises(UserServiceError):
            await svc.handle_callback(github, "code", "http://cb", db_session)

        mock_auth.create_access_token.assert_not_called()
        db_session.refresh(unproven_local_account)
        assert unproven_local_account.oauth_provider is None
