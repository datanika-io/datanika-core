"""A run outcome must reach the agent as data, not as an exception with the body thrown away.

`DatanikaClient._post` calls `resp.raise_for_status()`, so every 4xx becomes an
`httpx.HTTPStatusError` and the parsed body is discarded. `trigger_upload`,
`trigger_pipeline` and `trigger_transformation` all route through it.

That interacts badly with #663. Once `?wait=true` reports a failed run as **422**,
an agent calling `trigger_upload(3, wait=True)` on a pipeline that fails would
learn *that* something went wrong and lose the one field saying *what* — strictly
worse than the wrong-but-informative 200 it replaces, arriving as a fix.

**And this is already true for 408, today.** A `?wait=true` timeout is the
documented, expected outcome of a long run, and it raises through the MCP tool
rather than returning the run with `timed_out: true`. So the endpoint's decision
to put run state on the status line was already half-broken at the consumer we
care most about.

On those three endpoints 408 and 422 are **results**. Everything else — 401, 403,
404, 5xx — is still a transport failure and still raises.
"""

import pathlib
import sys

import httpx
import pytest


@pytest.fixture(autouse=True)
def _add_mcp_to_path():
    """Make ``datanika_mcp`` importable from the MCP sub-package.

    Same shape as ``test_mcp_server.py`` — the sub-package is not installed into
    the core venv; it ships as its own distribution.
    """
    mcp_src = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
    if mcp_src not in sys.path:
        sys.path.insert(0, mcp_src)


def _client(handler):
    """A real `DatanikaClient` whose transport is a stub.

    Built from the real class rather than a fake so the test exercises `_post`
    itself — the method under change — instead of a re-implementation of it.
    """
    from datanika_mcp.client import DatanikaClient

    c = DatanikaClient("http://testserver", "etf_key")
    c._http = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    )
    return c


_FAILED_RUN = {
    "id": 7,
    "status": "failed",
    "error_message": "connection refused",
    "target_type": "upload",
    "target_id": 3,
}
_TIMED_OUT_RUN = {"id": 7, "status": "running", "timed_out": True}


class TestATerminalFailureArrivesAsData:
    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("trigger_upload", (3,)),
            ("trigger_pipeline", (4,)),
            ("trigger_transformation", (5,)),
        ],
    )
    async def test_422_returns_the_run_instead_of_raising(self, method, args):
        c = _client(lambda request: httpx.Response(422, json=_FAILED_RUN))
        try:
            body = await getattr(c, method)(*args, wait=True)
        finally:
            await c._http.aclose()
        assert body["status"] == "failed"
        assert body["error_message"] == "connection refused", (
            "the agent must be told what failed, not just that something did"
        )

    async def test_408_returns_the_run_instead_of_raising(self):
        """Pre-existing, and fixed in the same change — a wait timeout is an
        expected outcome of a long run, not a broken request."""
        c = _client(lambda request: httpx.Response(408, json=_TIMED_OUT_RUN))
        try:
            body = await c.trigger_upload(3, wait=True)
        finally:
            await c._http.aclose()
        assert body["timed_out"] is True
        assert body["status"] == "running"


class TestEverythingElseStillRaises:
    """The escape hatch is scoped to two codes on three endpoints, or it becomes
    'the client silently swallows errors', which is the same bug one layer down."""

    @pytest.mark.parametrize("status", [401, 403, 404, 429, 500, 503])
    async def test_other_failures_still_raise_on_a_trigger(self, status):
        c = _client(lambda request: httpx.Response(status, json={"error": "nope"}))
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await c.trigger_upload(3, wait=True)
        finally:
            await c._http.aclose()

    @pytest.mark.parametrize("status", [408, 422])
    async def test_other_endpoints_still_raise_on_those_codes(self, status):
        """422 is not a run outcome anywhere else, so nothing else may treat it
        as one — a blanket rule in `_post` would hide real validation errors."""
        c = _client(lambda request: httpx.Response(status, json={"error": "nope"}))
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await c.bulk_import({"version": 2})
        finally:
            await c._http.aclose()

    async def test_a_successful_trigger_is_unchanged(self):
        c = _client(lambda request: httpx.Response(202, json={"run_id": 7, "status": "pending"}))
        try:
            body = await c.trigger_upload(3)
        finally:
            await c._http.aclose()
        assert body == {"run_id": 7, "status": "pending"}
