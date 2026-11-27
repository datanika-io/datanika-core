"""A second socket that presents a live client token stays signed in.

Reflex mints a new client token when the previous socket is still registered.
AuthState lives only on that token. The minted token is empty, ``check_auth``
sends the page to /login, and ``load_connections`` returns before it reads any
rows, so the connections list never shows the seeded name. The viewer spec
waits 10s on that name and fails; the retry is a new login that does not hit
the overlap. core#1714.

The spec's ``toBeVisible`` timeout stays 10s. This file does not import the
viewer spec and does not change it.
"""

import uuid

from datanika.config import settings
from datanika.datanika import app
from datanika.services.auth import AuthService
from datanika.ui.state.auth_state import AuthState, OrgInfo, UserInfo

_ORG_ID = 7
_USER_ID = 3


def _tokens():
    namespace = app.event_namespace
    assert namespace is not None
    return namespace._token_manager, app.state_manager


def _access_token() -> str:
    return AuthService(settings.secret_key).create_access_token(_USER_ID, _ORG_ID)


async def _sign_in(manager, token: str, *, access_token: str, org_id: int) -> None:
    async with manager.modify_state(token) as root:
        auth = await root.get_state(AuthState)
        auth.access_token = access_token
        auth.refresh_token = "refresh-for-duplicate-socket"
        auth.current_user = UserInfo(id=_USER_ID, email="viewer@example.test", full_name="Viewer")
        auth.current_org = OrgInfo(id=org_id, name="Acme", slug="acme")
        auth.user_orgs = [OrgInfo(id=org_id, name="Acme", slug="acme")]
        auth.current_role = "viewer"


async def _auth(manager, token: str) -> AuthState:
    async with manager.modify_state(token) as root:
        return await root.get_state(AuthState)


async def test_duplicate_socket_keeps_the_signed_in_session():
    """The minted token carries the org, the user, and a token check_auth accepts."""
    tokens, manager = _tokens()
    old = f"qa1714-{uuid.uuid4()}"
    access = _access_token()
    sid_a = f"sid-a-{uuid.uuid4()}"
    sid_b = f"sid-b-{uuid.uuid4()}"
    try:
        assert await tokens.link_token_to_sid(old, sid_a) is None, type(tokens).__name__
        await _sign_in(manager, old, access_token=access, org_id=_ORG_ID)
        minted = await tokens.link_token_to_sid(old, sid_b)
        assert minted, type(tokens).__name__
        assert minted != old
        auth = await _auth(manager, minted)
        assert auth.current_org.id == _ORG_ID
        assert auth.current_user.id == _USER_ID
        assert auth.current_role == "viewer"
        assert [org.id for org in auth.user_orgs] == [_ORG_ID]
        assert auth._revalidate_session() is True
        assert auth._get_was_touched()
        assert "access_token" in auth.dirty_vars
        assert "current_org" in auth.dirty_vars
        restored = type(auth)._deserialize(auth._serialize())
        assert restored.current_org.id == _ORG_ID
        assert restored.current_user.id == _USER_ID
        assert restored._revalidate_session() is True
        auth.current_org.name = "Changed"
        old_auth = await _auth(manager, old)
        assert old_auth.current_org.name == "Acme"
        assert getattr(tokens.link_token_to_sid, "inherits_session_from_previous_token", False)
    finally:
        await tokens.disconnect_token(old, sid_a)
        second = tokens.sid_to_token.get(sid_b)
        if second:
            await tokens.disconnect_token(second, sid_b)


async def test_same_socket_reuses_the_token():
    """A reconnect of the same socket returns no new token and leaves the session."""
    tokens, manager = _tokens()
    old = f"qa1714-same-{uuid.uuid4()}"
    sid = f"sid-{uuid.uuid4()}"
    try:
        assert await tokens.link_token_to_sid(old, sid) is None
        await _sign_in(manager, old, access_token=_access_token(), org_id=_ORG_ID)
        assert await tokens.link_token_to_sid(old, sid) is None
        auth = await _auth(manager, old)
        assert auth.current_org.id == _ORG_ID
        assert auth._revalidate_session() is True
    finally:
        await tokens.disconnect_token(old, sid)


async def test_signed_out_duplicate_stays_signed_out():
    """An empty access token is not a session to copy onto the minted token."""
    tokens, manager = _tokens()
    old = f"qa1714-out-{uuid.uuid4()}"
    sid_a = f"sid-a-{uuid.uuid4()}"
    sid_b = f"sid-b-{uuid.uuid4()}"
    try:
        assert await tokens.link_token_to_sid(old, sid_a) is None
        await _sign_in(manager, old, access_token="", org_id=_ORG_ID)
        minted = await tokens.link_token_to_sid(old, sid_b)
        assert minted, type(tokens).__name__
        auth = await _auth(manager, minted)
        assert auth.access_token == ""
        assert auth.current_org.id == 0
        assert auth._revalidate_session() is False
    finally:
        await tokens.disconnect_token(old, sid_a)
        second = tokens.sid_to_token.get(sid_b)
        if second:
            await tokens.disconnect_token(second, sid_b)
