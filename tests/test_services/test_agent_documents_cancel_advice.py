"""The agent documents do not advise cancelling a stuck run (core#1354).

When this was written ``POST /api/v1/runs/{id}/cancel`` marked a run cancelled and stopped nothing,
and core's own served documents nonetheless told agents to "cancel stuck runs" and to "Cancel +
retry" — so an agent following them left the first run going and started a second beside it.

The 2026-09-22 text said a working run reads ``cancelling`` and runs to its end. The
between-chunk stop (2026-11-19) makes that sentence false for a load. Both documents now say
what the stop does: further loading stops, rows already written stay, a model already running
finishes, and an append re-run duplicates those partial rows. The ban on "Cancel + retry" and
"cancel stuck" stays, because deleting the old remedy without naming the partial rows would
tell an agent nothing about the rows already written.

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
    def test_says_further_loading_stops_and_an_append_retry_keeps_the_partial_rows(self, client):
        text = client.get("/api/v1/agent-guide.md").text

        assert "Cancel + retry" not in text
        assert "cancel stuck" not in text
        assert "stops further loading" in text
        assert "has not started its work yet" in text
        assert "A model already running finishes" in text
        assert "duplicates those partial rows" in text
        assert "retrying an append does not clear the rows already written" in text
        assert "`GET /runs/{id}`" in text
        assert "runs to its end" not in text

    def test_the_wait_outcomes_name_every_status_they_can_carry(self, client):
        """`408` read *"still `pending`/`running`"* — a hand-written status list that went stale
        the day `cancelling` was added. It is derived now (``services/run_status_prose.py``)."""
        from datanika.models.run import NON_TERMINAL_RUN_STATUSES, TERMINAL_RUN_STATUSES, RunStatus

        text = client.get("/api/v1/agent-guide.md").text
        line_408 = next(line for line in text.splitlines() if "`408`" in line)
        for status in NON_TERMINAL_RUN_STATUSES:
            assert f"`{status.value}`" in line_408, (status, line_408)
        block_422 = text[text.index("`422`") : text.index("The serialised run")]
        for status in TERMINAL_RUN_STATUSES - {RunStatus.SUCCESS}:
            assert f"`{status.value}`" in block_422, (status, block_422)


class TestTheAgentTiersPayload:
    def test_no_capability_offers_cancel_as_a_remedy_and_control_says_what_it_does(self, client):
        text = json.dumps(client.get("/api/v1/meta/agent-tiers").json())

        assert "cancel stuck" not in text
        assert "Cancel runs," not in text
        assert "Stop further loading: rows already written stay" in text
        assert "append re-run duplicates the partial rows" in text
        assert "Do not cancel and then retry an append" in text
