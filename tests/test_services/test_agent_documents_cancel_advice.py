"""The agent documents do not advise cancelling a stuck run (core#1354).

When this was written ``POST /api/v1/runs/{id}/cancel`` marked a run cancelled and stopped nothing,
and core's own served documents nonetheless told agents to "cancel stuck runs" and to "Cancel +
retry" — so an agent following them left the first run going and started a second beside it.

🔴 **Repointed 2026-09-22 (core#657), not deleted.** The documents said *"marks the run cancelled
but does not stop it"*, and two things have since changed underneath that sentence: the pre-flight
checkpoint (§7 2a) now stops a run whose work has not started, and a run that IS working reads
``cancelling`` until its worker finishes. #1354's AC3 asked for exactly this — *"when the real stop
ships, both documents are re-read against it"*. What has NOT changed is the reason for the advice:
the run an agent calls stuck is one already working, and nothing interrupts that one. So the ban on
the old remedy stands, and the sentence each test requires is the 2a one.

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
    def test_says_a_working_run_is_not_interrupted_and_does_not_advise_a_retry(self, client):
        text = client.get("/api/v1/agent-guide.md").text

        assert "Cancel + retry" not in text
        assert "stops a run only if its work has not started yet" in text
        assert "reads `cancelling` and runs to its end" in text
        assert "`GET /runs/{id}`" in text

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
        assert "Cancel runs whose work has not started (a run already working is not" in text
