"""§7.1 — what a REST refusal says, and that it is not a 500 (core#681).

🚨 **This is the only mitigation the intersection decision has.** A key's authority is now
intersected with its owner's *current* org role, so a key that worked yesterday stops working
today with no deploy, nothing changed about the key, and nobody associating the failure with it.
A refusal that does not name the cause turns that into a mystery instead of a one-step answer.

⚠️ Without the wiring this file asserts, an `InsufficientRoleError` falls to `api_middleware`'s
`except Exception` and becomes **`500 Internal server error`** — which reads as *our* bug, and is
strictly worse than the bare `401` the spec already forbids.

Both handler paths are driven. The middleware has a separate async and sync implementation of the
same logic, and a rule applied to only one of the two is not a rule — the file's own comment on
`_shed_before_auth` says exactly that about a different rule.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from datanika.services.api_middleware import api_endpoint
from datanika.services.authorization import InsufficientRoleError
from datanika.services.rate_limit_service import RateLimitResult


@api_endpoint()
async def _async_refuses(request, api_key, session):
    raise InsufficientRoleError(required_role="admin", operation="delete_connection")


@api_endpoint()
def _sync_refuses(request, api_key, session):
    raise InsufficientRoleError(required_role="admin", operation="delete_connection")


@api_endpoint()
async def _async_explodes(request, api_key, session):
    raise RuntimeError("something genuinely broken")


@api_endpoint()
async def _async_ok(request, api_key, session):
    return JSONResponse({"ok": True})


def _client(handler):
    return TestClient(Starlette(routes=[Route("/t", handler, methods=["GET", "POST"])]))


_RATE_LIMIT_OK = RateLimitResult(
    allowed=True, current_count=1, limit=60, remaining=59, retry_after=0, reset_at=9999999999
)


def _authed(mock_svc, mock_session, mock_rl):
    """Authenticate, and stub the rate limiter.

    ⚠️ `_rate_limit_svc` must be patched or the middleware reaches **real Redis** — the first
    draft of this file did not, and all six tests failed including both controls. Six reds where
    the controls should be green is a harness fault, not a finding; reading them as the fix
    failing would have been the wrong conclusion twice over.
    """
    key = MagicMock()
    key.org_id = 1
    key.id = 7
    key.user_id = 42
    mock_svc.authenticate_api_key.return_value = key
    mock_session.return_value.__enter__ = lambda s: MagicMock()
    mock_session.return_value.__exit__ = lambda s, *a: None
    mock_rl.check_rate_limit.return_value = _RATE_LIMIT_OK
    mock_rl.get_limit_for_org.return_value = 60
    return key


HEADERS = {"Authorization": "Bearer etf_ok"}


@patch("datanika.services.api_middleware._rate_limit_svc")
@patch("datanika.services.api_middleware._api_key_svc")
@patch("datanika.services.api_middleware._get_session")
def test_the_async_path_refuses_with_403_not_500(mock_session, mock_svc, mock_rl):
    _authed(mock_svc, mock_session, mock_rl)
    resp = _client(_async_refuses).get("/t", headers=HEADERS)
    assert resp.status_code == 403, (
        f"got {resp.status_code}. A 500 here reads as our bug rather than the caller's "
        "permissions, and the caller has nothing to act on."
    )


@patch("datanika.services.api_middleware._rate_limit_svc")
@patch("datanika.services.api_middleware._api_key_svc")
@patch("datanika.services.api_middleware._get_session")
def test_the_sync_path_refuses_with_403_not_500(mock_session, mock_svc, mock_rl):
    """The middleware implements the same logic twice; a rule applied to one is not a rule."""
    _authed(mock_svc, mock_session, mock_rl)
    resp = _client(_sync_refuses).get("/t", headers=HEADERS)
    assert resp.status_code == 403


@patch("datanika.services.api_middleware._rate_limit_svc")
@patch("datanika.services.api_middleware._api_key_svc")
@patch("datanika.services.api_middleware._get_session")
def test_required_role_is_a_machine_readable_field(mock_session, mock_svc, mock_rl):
    """§7.1: *a field, not prose.* A script retries or escalates on it.

    Parsing an English sentence to decide is how integrations break on a copy edit.
    """
    _authed(mock_svc, mock_session, mock_rl)
    body = _client(_async_refuses).get("/t", headers=HEADERS).json()
    assert body["error"]["code"] == "insufficient_role"
    assert body["error"]["required_role"] == "admin"


@patch("datanika.services.api_middleware._rate_limit_svc")
@patch("datanika.services.api_middleware._api_key_svc")
@patch("datanika.services.api_middleware._get_session")
def test_the_refusal_does_not_read_as_expired_or_revoked(mock_session, mock_svc, mock_rl):
    """The failure mode the wording exists to prevent.

    A caller told "expired" re-mints the key, and the new one fails identically — because the
    key was never the problem. Naming the role is what ends the loop.
    """
    _authed(mock_svc, mock_session, mock_rl)
    body = _client(_async_refuses).get("/t", headers=HEADERS).json()
    text = str(body).lower()
    for forbidden in ("expired", "revoked", "invalid", "unauthorized"):
        assert forbidden not in text, f"refusal reads as {forbidden!r}: {body}"
    assert "admin" in text


@patch("datanika.services.api_middleware._rate_limit_svc")
@patch("datanika.services.api_middleware._api_key_svc")
@patch("datanika.services.api_middleware._get_session")
def test_a_real_error_is_still_a_500(mock_session, mock_svc, mock_rl):
    """The control. Without it, catching everything and calling it 403 passes every test above."""
    _authed(mock_svc, mock_session, mock_rl)
    resp = _client(_async_explodes).get("/t", headers=HEADERS)
    assert resp.status_code == 500, (
        "a genuine exception now reports as a permissions refusal — that hides real bugs "
        "behind an answer the caller will act on incorrectly"
    )


@patch("datanika.services.api_middleware._rate_limit_svc")
@patch("datanika.services.api_middleware._api_key_svc")
@patch("datanika.services.api_middleware._get_session")
def test_an_ordinary_request_still_succeeds(mock_session, mock_svc, mock_rl):
    """The other control: the new branch must not intercept the happy path."""
    _authed(mock_svc, mock_session, mock_rl)
    resp = _client(_async_ok).get("/t", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
