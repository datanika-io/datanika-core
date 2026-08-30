"""`?wait=true` must put the run's outcome on the status line, not just its transport.

`curl --fail` and `raise_for_status()` key off the status code, and every CI system
in common use keys off `curl`'s exit status. A pipeline step that reports success
on a failed load is worse than no step at all: it converts a loud failure into a
silent one (#663).

The endpoint had **already decided** the status line carries run state — it returns
408 when the run is still `pending`/`running` at the wait timeout, which is not an
HTTP-transaction failure either. So 200-on-failure was inconsistent with its own
behaviour, not a defensible alternative.

    success                      -> 200
    still running at timeout     -> 408   (unchanged)
    terminal but not success     -> 422

422 rather than 5xx because the failure is in the caller's pipeline — their
credentials, their SQL, their source — not in our server. A 5xx would page us for
their typo.

⚠️ **The branch asserts "not success", never `failed` by name.** `RunStatus` also
has `cancelled`, and a terminal status added later must not silently rejoin the 200
branch. That is exactly how this shape recurs.
"""

import contextlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datanika.models.run import NodeType, RunStatus
from datanika.services import api_v1_routes


def _run(status: RunStatus):
    """A stand-in with exactly the attributes `_ser_run` reads.

    Not a MagicMock: that answers every attribute truthily, so a serializer that
    stopped reading `status` would still produce a passing dict.
    """
    r = MagicMock(spec=[])
    r.id = 7
    r.target_type = NodeType.UPLOAD
    r.target_id = 3
    r.status = status
    r.started_at = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    r.finished_at = datetime(2026, 8, 30, 12, 1, tzinfo=UTC)
    r.rows_loaded = 0
    r.error_message = "connection refused"
    r.created_at = datetime(2026, 8, 30, 11, 59, tzinfo=UTC)
    return r


def _request(**params):
    req = MagicMock(spec=[])
    req.query_params = params
    return req


@contextlib.contextmanager
def _waiter_returns(run):
    """Patch the waiter where `_trigger_and_maybe_wait` imports it from.

    The import is function-local, so patching the attribute on
    `api_v1_routes` would miss it entirely and the test would exercise the real
    waiter against no database.
    """
    with patch("datanika.services.run_waiter.wait_for_run", new=AsyncMock(return_value=run)):
        yield


@pytest.fixture
def api_key():
    key = MagicMock(spec=[])
    key.org_id = 10
    return key


class TestTheOutcomeReachesTheStatusLine:
    async def test_a_failed_run_is_not_a_2xx(self, api_key):
        """The regression. `curl --fail` exits 0 against the unfixed code."""
        with _waiter_returns(_run(RunStatus.FAILED)):
            resp = await api_v1_routes._trigger_and_maybe_wait(
                _request(wait="true"), api_key, _run(RunStatus.PENDING)
            )
        assert resp.status_code == 422, (
            "a terminal non-success run must not be reported as success — "
            "curl --fail and raise_for_status() both key off this"
        )

    async def test_the_failed_body_still_carries_the_reason(self, api_key):
        """The status code is the signal; the body is still the source of truth.

        A 422 with no `error_message` would tell an agent that something broke and
        lose the one field saying what.
        """
        import json

        with _waiter_returns(_run(RunStatus.FAILED)):
            resp = await api_v1_routes._trigger_and_maybe_wait(
                _request(wait="true"), api_key, _run(RunStatus.PENDING)
            )
        body = json.loads(resp.body)
        assert body["status"] == "failed"
        assert body["error_message"] == "connection refused"
        assert body["id"] == 7

    async def test_a_cancelled_run_is_also_not_a_2xx(self, api_key):
        """Asserted as *not success*, never by naming `failed`.

        `RunStatus.CANCELLED` is terminal and is not success. A branch written as
        `if status == "failed"` passes every test above and silently returns 200
        here — which is how this defect class comes back.
        """
        with _waiter_returns(_run(RunStatus.CANCELLED)):
            resp = await api_v1_routes._trigger_and_maybe_wait(
                _request(wait="true"), api_key, _run(RunStatus.PENDING)
            )
        assert resp.status_code == 422

    async def test_a_successful_run_is_still_200(self, api_key):
        """Negative control, or the fix is 'fail everything'."""
        with _waiter_returns(_run(RunStatus.SUCCESS)):
            resp = await api_v1_routes._trigger_and_maybe_wait(
                _request(wait="true"), api_key, _run(RunStatus.PENDING)
            )
        assert resp.status_code == 200

    async def test_a_timeout_is_still_408_and_flagged(self, api_key):
        """Unchanged behaviour, pinned so the new branch cannot swallow it."""
        import json

        with _waiter_returns(_run(RunStatus.RUNNING)):
            resp = await api_v1_routes._trigger_and_maybe_wait(
                _request(wait="true"), api_key, _run(RunStatus.PENDING)
            )
        assert resp.status_code == 408
        assert json.loads(resp.body)["timed_out"] is True

    async def test_without_wait_nothing_changed(self, api_key):
        """Fire-and-forget still returns 202 immediately and never waits."""
        import json

        resp = await api_v1_routes._trigger_and_maybe_wait(
            _request(), api_key, _run(RunStatus.PENDING)
        )
        assert resp.status_code == 202
        assert json.loads(resp.body) == {"run_id": 7, "status": "pending"}


class TestTheContractIsDocumentedWhereCallersLook:
    """A behaviour change on a public endpoint that is not documented is a
    behaviour change users discover in production (#663)."""

    def test_the_openapi_spec_lists_all_three_outcomes(self):
        from datanika.services.openapi import build_openapi_spec

        spec = build_openapi_spec()
        for resource in ("uploads", "pipelines", "transformations"):
            op = spec["paths"][f"/api/v1/{resource}/{{id}}/run"]["post"]
            assert set(op["responses"]) >= {"200", "202", "408", "422"}, (
                f"{resource} trigger does not document the ?wait=true outcomes"
            )
            names = {p["name"] for p in op["parameters"]}
            assert {"wait", "timeout"} <= names, f"{resource} trigger omits ?wait/?timeout"

    def test_the_connection_test_endpoint_is_not_given_run_semantics(self):
        """It shares the helper and has no run to wait for. Documenting 408/422
        there would describe responses it cannot produce."""
        from datanika.services.openapi import build_openapi_spec

        op = build_openapi_spec()["paths"]["/api/v1/connections/{id}/test"]["post"]
        assert "408" not in op["responses"]
        assert "422" not in op["responses"]
        assert {p["name"] for p in op["parameters"]} == {"id"}

    def test_the_agent_guide_explains_the_status_codes(self):
        """Agents read this, not the OpenAPI JSON. It is where `?wait=true` is
        already recommended, so it is where the caveat has to live."""
        from datanika.services.agent_docs import AGENT_GUIDE_MD

        assert "422" in AGENT_GUIDE_MD
        assert "408" in AGENT_GUIDE_MD
        for phrase in ("curl --fail", "raise_for_status"):
            assert phrase in AGENT_GUIDE_MD, f"the guide does not mention {phrase}"


class TestAllThreeTriggersShareTheBranch:
    """The helper test above is only worth something if the routes reach it."""

    def test_every_trigger_endpoint_delegates(self):
        """AST over the module, not ``inspect.getsource`` on the attribute.

        ``api_endpoint`` does not set ``__wrapped__``, so the attribute resolves
        to the decorator's wrapper and its source contains none of the handler
        bodies. That version of this test failed for a reason that had nothing to
        do with the property, which is the shape that gets a test deleted.
        """
        import ast
        import inspect
        import pathlib

        wanted = {"trigger_upload", "trigger_pipeline", "trigger_transformation"}
        tree = ast.parse(pathlib.Path(inspect.getfile(api_v1_routes)).read_text(encoding="utf-8"))
        found = {
            node.name: ast.unparse(node)
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name in wanted
        }
        assert set(found) == wanted, f"handlers renamed or removed: {sorted(found)}"
        for name, source in found.items():
            assert "_trigger_and_maybe_wait" in source, (
                f"{name} does not go through the shared helper, so the outcome "
                "status is not applied to it"
            )
