"""Keep a signed-in session when Reflex mints a token for a second socket.

A full page load presents the client token the previous socket still holds.
``TokenManager.link_token_to_sid`` then returns a new token and stores no
state on it. ``AuthState`` lives only in Reflex state, so the new token is
signed out: ``check_auth`` redirects, and ``load_connections`` returns while
``current_org.id`` is still 0. The connections list stays on the loader and
the seeded row never appears.

The event namespace emits ``new_token`` only after ``link_token_to_sid``
returns, so copying here lands the session before the client is told, and
before the namespace writes the router session id onto the new token.
"""

from datanika.ui.state.auth_state import AuthState

_SESSION_FIELDS = (
    "access_token",
    "refresh_token",
    "current_user",
    "current_org",
    "user_orgs",
    "current_role",
)

_MARKER = "inherits_session_from_previous_token"


def _detached(value: object) -> object:
    """A value the new token owns, not a proxy into the previous token's state."""
    wrapped = getattr(value, "__wrapped__", None)
    if wrapped is not None:
        value = wrapped
    copy_model = getattr(value, "model_copy", None)
    if callable(copy_model):
        return copy_model(deep=True)
    if isinstance(value, list):
        return [_detached(item) for item in value]
    return value


async def inherit_auth_across_duplicate_token(
    state_manager, old_token: str, new_token: str
) -> None:
    """Copy the signed-in session from ``old_token`` onto ``new_token``.

    A signed-out token is left signed out. The two tokens do not share the
    org or user objects, so one tab can edit its own state.
    """
    async with state_manager.modify_state(old_token) as old_root:
        old_auth = await old_root.get_state(AuthState)
        if not old_auth.access_token:
            return
        copied = {name: _detached(getattr(old_auth, name)) for name in _SESSION_FIELDS}

    async with state_manager.modify_state(new_token) as new_root:
        new_auth = await new_root.get_state(AuthState)
        for name, value in copied.items():
            setattr(new_auth, name, value)


def install_session_inherit(app: object) -> None:
    """Wrap the app token manager. A second call leaves the wrapper in place."""
    namespace = getattr(app, "event_namespace", None)
    if namespace is None:
        return
    manager = namespace._token_manager
    current = manager.link_token_to_sid
    if getattr(current, _MARKER, False):
        return
    state_manager = app.state_manager

    async def link_token_to_sid(token: str, sid: str) -> str | None:
        new_token = await current(token, sid)
        if new_token and new_token != token:
            await inherit_auth_across_duplicate_token(state_manager, token, new_token)
        return new_token

    setattr(link_token_to_sid, _MARKER, True)
    manager.link_token_to_sid = link_token_to_sid
