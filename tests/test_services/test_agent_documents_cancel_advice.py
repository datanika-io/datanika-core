"""The agent documents do not advise cancelling a stuck run (core#1354).

``POST /api/v1/runs/{id}/cancel`` marks a run cancelled and does not stop it. ``cancel_run`` never
revokes the task, the run tasks never check for cancellation during the extract and the load, and
``start_run`` sets RUNNING without reading the status. Core's own served documents nonetheless
told agents to "cancel stuck runs" and to "Cancel + retry", so an agent following them left the
first run going and started a second beside it.

Each test asserts the replacement sentence is PRESENT as well as the old advice absent, in one
test: a deleted line would satisfy the ban on its own, and tell an agent nothing.
"""

import json

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.agent_docs import agent_doc_routes


@pytest.fixture
def client():
    return TestClient(Starlette(routes=agent_doc_routes))


class TestTheAgentGuide:
    def test_says_cancel_does_not_stop_a_run_and_does_not_advise_a_retry(self, client):
        text = client.get("/api/v1/agent-guide.md").text

        assert "Cancel + retry" not in text
        assert "marks the run cancelled but does not stop it" in text
        assert "`GET /runs/{id}`" in text


class TestTheAgentTiersPayload:
    def test_no_capability_offers_cancel_as_a_remedy_and_control_says_what_it_does(self, client):
        text = json.dumps(client.get("/api/v1/meta/agent-tiers").json())

        assert "cancel stuck" not in text
        assert "Cancel runs," not in text
        assert "Mark runs cancelled (this does not stop a run)" in text
