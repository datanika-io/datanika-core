"""core#624 §8e — a social signup from an invitation joins the inviting org, and nothing else.

The email path was fixed by core#981 (``tests/test_ui/test_invited_signup_lands_in_one_org.py``).
The social path carries the same obligation with one structural difference: the personal org is
created INSIDE the provider callback (``find_or_create_oauth_user``), before ``/auth/complete``
ever loads. So the invitation has to be applied there as well — applying it on the page would be
applying it after the org it is meant to replace already exists.

⚠️ **Assert counts, not presence** (§8f criterion 9). "The invitee appears in the inviting org"
is satisfied by a two-org outcome and cannot see the defect.

These run against a real session: ``InvitationService``, ``UserService`` and the membership rows
are the thing in doubt, so only the provider's HTTP surface is replaced.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from datanika.models.invitation import Invitation, InvitationStatus
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService
from datanika.services.invitation_service import InvitationService
from datanika.services.oauth_service import OAuthService, google_provider
from datanika.services.user_service import UserService

# SQLite returns DateTime(timezone=True) naive; accept_invitation compares it to an aware now().
# The fixture and the reason it is module-scoped live beside core#981's tests.
from tests.test_ui.test_invited_signup_lands_in_one_org import (  # noqa: F401 — autouse fixture
    _sqlite_returns_aware_datetimes,
)

NAME = "Nina Newcomer"
EMAIL = "nina@example.com"


@pytest.fixture
def auth():
    return AuthService("test-secret-key-for-social-invitations")


@pytest.fixture
def user_svc(auth):
    return UserService(auth)


@pytest.fixture
def oauth(auth, user_svc):
    return OAuthService(auth, user_svc)


@pytest.fixture
def inviting_org(db_session, user_svc):
    owner = user_svc.register_user(db_session, "olive@example.com", "owner-password-1", "Olive")
    org = user_svc.create_org(db_session, "Acme Data", "acme-data", owner.id)
    db_session.flush()
    return org, owner


def _invite(db_session, auth, inviting_org, *, email=EMAIL) -> str:
    org, owner = inviting_org
    invitation = InvitationService(auth).create_invitation(
        db_session, org.id, email, MemberRole.EDITOR, owner.id
    )
    db_session.flush()
    return invitation.token


async def _sign_in(oauth, db_session, *, email=EMAIL, sub="google-sub-nina", invite_token=""):
    """One completed Google callback.

    ``invite_token`` is passed only when there is one, so the controls below exercise the call
    shape every existing caller uses and stay meaningful on either side of the change.
    """
    oauth._exchange_code = AsyncMock(return_value={"access_token": "provider-token"})
    oauth._fetch_userinfo = AsyncMock(
        return_value={"email": email, "email_verified": True, "name": NAME, "sub": sub}
    )
    kwargs = {"invite_token": invite_token} if invite_token else {}
    return await oauth.handle_callback(
        google_provider("gid", "gsecret"), "code", "http://cb", db_session, **kwargs
    )


def _memberships(db_session, email):
    user = db_session.query(User).filter(User.email == email).one()
    memberships = (
        db_session.query(Membership)
        .filter(Membership.user_id == user.id, Membership.deleted_at.is_(None))
        .all()
    )
    return user, memberships, [db_session.get(Organization, m.org_id) for m in memberships]


def _token_org(auth, result) -> int:
    return auth.decode_token(result["access_token"], expected_type="access")["org_id"]


# ---------------------------------------------------------------------------
# AC4 / criterion 9 — exactly one membership, in the inviting org
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_an_invited_social_signup_creates_no_personal_org(
    db_session, auth, oauth, inviting_org
):
    token = _invite(db_session, auth, inviting_org)
    org, _owner = inviting_org

    result = await _sign_in(oauth, db_session, invite_token=token)

    _user, memberships, orgs = _memberships(db_session, EMAIL)
    assert result["is_new"] is True
    assert [o.id for o in orgs] == [org.id], (
        f"an invited social signup finished in {[o.name for o in orgs]}; it must join the "
        "inviting org and nothing else"
    )
    assert memberships[0].role is MemberRole.EDITOR, "the invited role was not the one granted"
    personal = db_session.query(Organization).filter(Organization.name == f"{NAME}'s Org")
    assert personal.count() == 0, (
        "a personal org row was created — and abandoned — rather than not created"
    )
    assert result["invite"] == "joined"
    assert _token_org(auth, result) == org.id, "the session token names some other org"


@pytest.mark.asyncio
async def test_an_uninvited_social_signup_still_gets_a_personal_org(db_session, auth, oauth):
    """Control. Without it, "no personal org" is satisfied by creating no org at all."""
    result = await _sign_in(oauth, db_session)

    _user, memberships, orgs = _memberships(db_session, EMAIL)
    assert len(memberships) == 1
    assert orgs[0].name == f"{NAME}'s Org"
    assert memberships[0].role is MemberRole.OWNER
    assert result.get("invite", "") == ""
    assert _token_org(auth, result) == orgs[0].id


# ---------------------------------------------------------------------------
# AC5 / criterion 10 — an unusable invitation falls back, and says so
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["expired", "already_accepted", "garbage", "wrong_address"])
async def test_an_unusable_invitation_falls_back_to_a_personal_org_and_says_so(
    db_session, auth, oauth, inviting_org, kind
):
    if kind == "garbage":
        token = "not-a-real-token"
    else:
        token = _invite(
            db_session,
            auth,
            inviting_org,
            email="somebody-else@example.com" if kind == "wrong_address" else EMAIL,
        )
        invitation = db_session.query(Invitation).order_by(Invitation.id.desc()).first()
        if kind == "expired":
            invitation.expires_at = datetime.now(UTC) - timedelta(days=1)
        elif kind == "already_accepted":
            invitation.status = InvitationStatus.ACCEPTED
        db_session.flush()

    result = await _sign_in(oauth, db_session, invite_token=token)

    _user, memberships, orgs = _memberships(db_session, EMAIL)
    assert len(memberships) == 1, f"a {kind} invitation left the user in {len(memberships)} orgs"
    assert orgs[0].name == f"{NAME}'s Org", "never zero orgs: the personal org is the fallback"
    assert result["invite"] == "not_applied", f"a {kind} invitation was dropped silently"


@pytest.mark.asyncio
async def test_an_invitation_issued_to_another_existing_account_is_not_applied(
    db_session, auth, user_svc, oauth, inviting_org
):
    """``accept_invitation`` resolves its user from the INVITATION's address.

    So a token issued to an address that already has an account would otherwise join THAT
    account — consuming its invitation — while the person signing in is left with no org, and
    the callback fails on "User has no organization".
    """
    alice = user_svc.register_user(db_session, "alice@example.com", "alice-password-1", "Alice")
    db_session.flush()
    token = _invite(db_session, auth, inviting_org, email="alice@example.com")
    org, _owner = inviting_org

    result = await _sign_in(oauth, db_session, invite_token=token)

    _user, memberships, orgs = _memberships(db_session, EMAIL)
    assert [o.name for o in orgs] == [f"{NAME}'s Org"]
    assert result["invite"] == "not_applied"
    assert (
        db_session.query(Membership)
        .filter(Membership.user_id == alice.id, Membership.org_id == org.id)
        .count()
        == 0
    ), "someone else's sign-in joined Alice to the org"
    invitation = db_session.query(Invitation).order_by(Invitation.id.desc()).first()
    assert invitation.status is InvitationStatus.PENDING, "Alice's invitation was consumed"


# ---------------------------------------------------------------------------
# AC7 — a returning user is not given a second org
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_returning_linked_user_is_not_given_a_second_org(db_session, auth, oauth):
    await _sign_in(oauth, db_session)

    again = await _sign_in(oauth, db_session)

    _user, memberships, _orgs = _memberships(db_session, EMAIL)
    assert again["is_new"] is False
    assert len(memberships) == 1


@pytest.mark.asyncio
async def test_a_returning_user_with_an_invitation_joins_and_lands_there(
    db_session, auth, oauth, inviting_org
):
    await _sign_in(oauth, db_session)
    token = _invite(db_session, auth, inviting_org)
    org, _owner = inviting_org

    result = await _sign_in(oauth, db_session, invite_token=token)

    _user, memberships, orgs = _memberships(db_session, EMAIL)
    assert sorted(o.name for o in orgs) == sorted(["Acme Data", f"{NAME}'s Org"])
    assert result["is_new"] is False
    assert result["invite"] == "joined"
    assert _token_org(auth, result) == org.id, "the session did not land in the org just joined"
