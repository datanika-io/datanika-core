"""Leaving an org must also leave the *session* behind it (core#658 R6).

`SettingsState.leave_org` soft-deletes the membership. That is not the whole
job, and the missing half is security-relevant rather than cosmetic: after the
delete the session is still pointed at the org just left —

* `AuthState.access_token` carries that org's `org_id` claim,
* `AuthState.current_org` still names it, and
* `BaseState._get_org_id` reads `current_org`,

so the next page would go on operating inside an organization this user is no
longer a member of, until the 10-minute token expiry happened to bite. A plain
`rx.redirect("/")` does not touch any of it.

So the handler re-derives the remaining orgs and either moves the session
(`switch_org` mints fresh tokens and re-reads the role) or ends it. **Both
branches are real**: a member who arrived by invitation may have had only this
one org, so "log out" is not a defensive stub.

⚠️ The stand-ins here are deliberately **not** `MagicMock()`. A bare MagicMock
answers every attribute truthily and every call successfully, so a test built
on one passes against code that never touched the object — which is exactly the
failure this file is about. Each stand-in below exposes only the attributes the
handler is allowed to use, and records what was called.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from datanika.ui.state.settings_state import SettingsState


class _Auth:
    """Stand-in for AuthState. Records which exit the handler took."""

    def __init__(self, org_ids, current_id):
        self.user_orgs = [SimpleNamespace(id=i, name=f"org-{i}", slug=f"org-{i}") for i in org_ids]
        self.current_org = SimpleNamespace(id=current_id, name="current", slug="current")
        self.current_user = SimpleNamespace(id=7, email="leaver@example.com")
        self.switched_to = None
        self.logged_out = False

    def switch_org(self, org_id):
        self.switched_to = org_id
        return f"redirect-to-org-{org_id}"

    def logout(self):
        self.logged_out = True
        return "redirect-to-login"


class _Service:
    def __init__(self):
        self.left = None

    def leave_org(self, session, org_id, *, actor_user_id):
        self.left = (org_id, actor_user_id)
        return True


class _State:
    """Stand-in for SettingsState itself — only what `leave_org` may touch."""

    def __init__(self, auth, service):
        self._auth = auth
        self._service = service
        self.error_message = "stale"

    async def get_state(self, _cls):
        return self._auth

    def _get_user_service(self):
        return self._service

    def _audit(self, *_args, **_kwargs):
        return None

    def _safe_error(self, exc, fallback):
        return fallback


async def _run(auth, service=None):
    service = service or _Service()
    state = _State(auth, service)
    with patch("datanika.ui.state.settings_state.get_sync_session") as sess:
        sess.return_value.__enter__.return_value = SimpleNamespace(commit=lambda: None)
        sess.return_value.__exit__.return_value = False
        # `.fn` — Reflex wraps a public method as an EventHandler, and calling
        # that with a stand-in `self` runs event-argument validation instead of
        # the body.
        result = await SettingsState.leave_org.fn(state)
    return result, state, service


class TestLeavingAlsoMovesTheSession:
    @pytest.mark.asyncio
    async def test_the_membership_is_actually_removed(self):
        auth = _Auth([1, 2], current_id=1)
        _result, _state, service = await _run(auth)
        assert service.left == (1, 7)

    @pytest.mark.asyncio
    async def test_a_remaining_org_takes_over_the_session(self):
        """Red before the fix: it redirected to "/" and left `current_org` at 1."""
        auth = _Auth([1, 2], current_id=1)
        await _run(auth)
        assert auth.switched_to == 2
        assert auth.logged_out is False

    @pytest.mark.asyncio
    async def test_the_org_just_left_is_not_a_switch_candidate(self):
        """The obvious bug in the obvious fix: switching back into it.

        `user_orgs` still contains the departed org at this point — nothing has
        reloaded it — so a handler that simply takes `user_orgs[0]` moves the
        session straight back where it started.
        """
        auth = _Auth([1, 2], current_id=1)
        await _run(auth)
        assert auth.switched_to != 1
        assert 1 not in [o.id for o in auth.user_orgs]

    @pytest.mark.asyncio
    async def test_the_last_org_ends_the_session(self):
        """A member who arrived by invitation may have had only this one org."""
        auth = _Auth([1], current_id=1)
        result, _state, _service = await _run(auth)
        assert auth.logged_out is True
        assert auth.switched_to is None
        assert result == "redirect-to-login"

    @pytest.mark.asyncio
    async def test_a_refusal_leaves_the_session_alone(self):
        """The last owner is refused by the service's owner-count invariant.

        Nothing about the session may move on that path — moving it would log
        someone out for an operation that did not happen.
        """

        class _Refusing(_Service):
            def leave_org(self, session, org_id, *, actor_user_id):
                raise ValueError("Cannot remove or demote the last owner")

        auth = _Auth([1, 2], current_id=1)
        result, state, _service = await _run(auth, _Refusing())
        assert auth.switched_to is None
        assert auth.logged_out is False
        assert result is None
        assert state.error_message == "Failed to leave organization"
