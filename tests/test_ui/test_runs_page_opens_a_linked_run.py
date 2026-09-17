"""``/runs?run=<id>`` opens that run's log (core#1398).

`/models` names an upload whose tables the catalogue did not read, and the notice has to link to
the run that says why (`SPEC_EARNED_VERDICTS` §4.6). The runs page had no way to be sent to one
run: every link into it went to the unfiltered list.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import datanika.ui.state.run_state as run_state_module
from datanika.models.dependency import NodeType
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService
from datanika.ui.state.run_state import RunState
from tests.test_ui.test_models_empty_state_after_a_load import _session_patch


class _St:
    _resolve_target_name = staticmethod(RunState._resolve_target_name)
    _select_run = RunState._select_run

    def __init__(self, org_id: int, params: dict):
        self._org_id = org_id
        self.runs: list = []
        self.filter_status = ""
        self.filter_target_type = ""
        self.selected_run_logs = ""
        self.selected_run_id = 0
        self.error_message = ""
        self.router = SimpleNamespace(page=SimpleNamespace(params=params))

    async def get_state(self, _cls):
        return SimpleNamespace(
            current_org=SimpleNamespace(id=self._org_id),
            current_user=SimpleNamespace(id=1),
        )


@pytest.fixture
def org_with_runs(db_session):
    import uuid

    org = Organization(name="Acme", slug=f"acme-runs-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()
    svc = ExecutionService()
    runs = []
    for logs in ("first run log", "second run log"):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        svc.complete_run(db_session, org.id, run.id, rows_loaded=1, logs=logs)
        runs.append(run)
    return org, runs


async def _load(db_session, org_id: int, params: dict) -> _St:
    st = _St(org_id, params)
    with (
        patch.object(run_state_module, "get_sync_session", return_value=_session_patch(db_session)),
        patch.object(run_state_module, "EncryptionService", MagicMock()),
    ):
        await RunState.load_runs.fn(st)
    return st


@pytest.mark.asyncio
async def test_a_run_named_in_the_url_is_opened(db_session, org_with_runs):
    org, (first, _second) = org_with_runs

    st = await _load(db_session, org.id, {"run": str(first.id)})

    assert st.selected_run_id == first.id
    assert "first run log" in st.selected_run_logs


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", "not-a-number", "999999999"])
async def test_control_nothing_is_opened_without_a_run_this_org_has(
    db_session, org_with_runs, value
):
    org, _runs = org_with_runs
    params = {} if value is None else {"run": value}

    st = await _load(db_session, org.id, params)

    assert st.selected_run_id == 0
    assert st.selected_run_logs == ""
