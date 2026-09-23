"""Run-status lists in prose that ships with the product name every status (core#657, §4).

``SPEC_RUN_CANCELLATION`` §4 retired seven hand-maintained status lists in code. The same defect
lived in prose: the ``?wait=true`` outcomes in the OpenAPI document and the agent guide, and the
MCP ``list_runs`` tool's description, each spelled the statuses out by hand and none of them knew
``cancelling`` existed. An agent reads that description to decide which values to expect, so a
missing value is a client that cannot recognise a state the API returns.

The OpenAPI and agent-guide halves are asserted where those documents are tested; this file holds
the helper's own contract and the one list that cannot derive — the MCP package is a standalone
client that does not import core, so its docstring is checked against core's enum from here.
"""

from __future__ import annotations

import pathlib

import pytest

from datanika.errors import InternalInvariantError
from datanika.models.run import RunStatus
from datanika.services.run_status_prose import statuses_in_prose

MCP_SERVER = (
    pathlib.Path(__file__).resolve().parents[2]
    / "datanika-mcp"
    / "src"
    / "datanika_mcp"
    / "server.py"
)


class TestStatusesInProse:
    def test_one(self):
        assert statuses_in_prose({RunStatus.FAILED}) == "`failed`"

    def test_two(self):
        assert statuses_in_prose({RunStatus.FAILED, RunStatus.CANCELLED}) == (
            "`cancelled` or `failed`"
        )

    def test_three_are_sorted_and_joined(self):
        statuses = {RunStatus.RUNNING, RunStatus.PENDING, RunStatus.CANCELLING}
        assert statuses_in_prose(statuses) == "`cancelling`, `pending` or `running`"

    def test_an_empty_set_is_refused_rather_than_rendered_as_nothing(self):
        with pytest.raises(InternalInvariantError):
            statuses_in_prose(set())


class TestTheMcpToolDescription:
    def test_list_runs_names_every_status_the_api_can_return(self):
        source = MCP_SERVER.read_text(encoding="utf-8")
        start = source.index("status: Filter by status")
        described = source[start : source.index("limit:", start)]
        missing = [s.value for s in RunStatus if f"'{s.value}'" not in described]
        assert not missing, f"datanika-mcp's list_runs does not name {missing}: {described!r}"
