"""A run that ends FAILED must end its Celery task in a failure state (core#1352).

What QA measured, 2026-09-15
----------------------------
On core `master` `ada0987`, with production's worker command (`-E`) and exporter,
run 3 ended `FAILED` (`step=sync … EOFError`). The worker still logged
`Task datanika.run_upload[…] succeeded in 31.91s: None`, and
`celery_task_failed_total` stayed at 0. The control, a task that really raises,
logged `raised unexpected` at ERROR and moved `failed{exception="TypeError"}` to 1
in the same worker and exporter.

The mechanism
-------------
`run_upload`, `run_pipeline` and `run_transformation` catch every exception,
record the failure on the run row, commit and return normally. The Celery wrappers
then returned normally too, so Celery recorded SUCCESS. Every reader of Celery's
verdict (the worker log, the event stream, the exporter, the
`celery-task-failures` rule) was told the opposite of the run's own status.

The witness
-----------
Each task's own state, from Celery's tracer, through `Task.apply()`. That tracer
writes the worker's `succeeded` or `raised unexpected` line and emits the event the
exporter counts. **The run row is not the witness**, because it already reads
FAILED on `dev`.

The inner functions keep their contract (record and return), because tests and the
hooks integration call them directly. Each is replaced here by a stand-in that
writes a terminal status the way the real one does, and the wrapper, which is the
thing under test, runs for real.
"""

from __future__ import annotations

import uuid

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService, get_org_run
from datanika.tasks.pipeline_tasks import run_pipeline_task
from datanika.tasks.transformation_tasks import run_transformation_task
from datanika.tasks.upload_tasks import run_upload_task

TASKS = [
    pytest.param(
        run_upload_task, "datanika.tasks.upload_tasks.run_upload", NodeType.UPLOAD, id="upload"
    ),
    pytest.param(
        run_pipeline_task,
        "datanika.tasks.pipeline_tasks.run_pipeline",
        NodeType.PIPELINE,
        id="pipeline",
    ),
    pytest.param(
        run_transformation_task,
        "datanika.tasks.transformation_tasks.run_transformation",
        NodeType.TRANSFORMATION,
        id="transformation",
    ),
]


class _NoCloseSession:
    """The task closes the session it opens; the test's must survive to the assertions."""

    def __init__(self, session):
        self._session = session

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._session, name)


@pytest.fixture
def run_for(db_session, monkeypatch):
    """A pending run in a fresh org, the concurrency gate open, and the task's own session
    source pointed at the test database."""
    monkeypatch.setattr("datanika.services.concurrency_service.acquire", lambda org_id: True)
    monkeypatch.setattr("datanika.services.concurrency_service.release", lambda org_id: None)
    monkeypatch.setattr("datanika.db.get_sync_session", lambda: _NoCloseSession(db_session))

    def make(node_type):
        org = Organization(name="Acme", slug=f"outcome-{uuid.uuid4().hex[:8]}")
        db_session.add(org)
        db_session.flush()
        run = ExecutionService().create_run(db_session, org.id, node_type, 1)
        return org, run

    return make


def _inner_that_ends(db_session, status: RunStatus, calls: list, error: str = "dlt exploded"):
    """Stands in for an inner run function: writes a terminal status and returns normally."""

    def inner(run_id, org_id, **kwargs):
        calls.append(run_id)
        run = get_org_run(db_session, org_id, run_id)
        run.status = status
        if status is RunStatus.FAILED:
            run.error_message = error
        db_session.flush()

    return inner


@pytest.mark.parametrize(("task", "inner_path", "node_type"), TASKS)
def test_a_run_that_ended_failed_ends_its_task_in_failure(
    task, inner_path, node_type, run_for, db_session, monkeypatch
):
    org, run = run_for(node_type)
    monkeypatch.setattr(inner_path, _inner_that_ends(db_session, RunStatus.FAILED, []))

    result = task.apply(args=[run.id, org.id])

    assert result.state == "FAILURE", (
        f"the run ended FAILED and Celery recorded the task as {result.state}. Every reader of "
        "Celery's verdict (worker log, events, exporter, the celery-task-failures rule) is told "
        "the opposite of the run's own status: core#1352."
    )


@pytest.mark.parametrize(("task", "inner_path", "node_type"), TASKS)
@pytest.mark.parametrize(
    "status", [RunStatus.SUCCESS, RunStatus.CANCELLED], ids=["success", "cancelled"]
)
def test_control_a_run_that_did_not_fail_ends_its_task_in_success(
    task, inner_path, node_type, status, run_for, db_session, monkeypatch
):
    """AC2. The fix is not "fail everything": a cancelled run is not a failed one either."""
    org, run = run_for(node_type)
    monkeypatch.setattr(inner_path, _inner_that_ends(db_session, status, []))

    assert task.apply(args=[run.id, org.id]).state == "SUCCESS"


@pytest.mark.parametrize(("task", "inner_path", "node_type"), TASKS)
def test_a_failed_run_is_not_retried(task, inner_path, node_type, run_for, db_session, monkeypatch):
    """AC4. These tasks declare `max_retries=60` for the concurrency gate. A run failure that now
    raises must end the task, not start re-running the body."""
    org, run = run_for(node_type)
    calls: list = []
    monkeypatch.setattr(inner_path, _inner_that_ends(db_session, RunStatus.FAILED, calls))

    result = task.apply(args=[run.id, org.id])

    assert result.state == "FAILURE", f"expected FAILURE, not {result.state}"
    assert calls == [run.id], f"the run body executed {len(calls)} times"


def test_the_run_row_keeps_what_the_run_wrote(run_for, db_session, monkeypatch):
    """AC3. The raise happens after the inner function has recorded and committed."""
    org, run = run_for(NodeType.UPLOAD)
    monkeypatch.setattr(
        "datanika.tasks.upload_tasks.run_upload",
        _inner_that_ends(db_session, RunStatus.FAILED, [], error="dlt exploded"),
    )

    run_upload_task.apply(args=[run.id, org.id])

    db_session.refresh(run)
    assert run.status == RunStatus.FAILED
    assert run.error_message == "dlt exploded"


def test_the_task_failure_names_the_run_and_not_its_error_text(run_for, db_session, monkeypatch):
    """The worker log is no place for a run's error text: it is user-controlled and can carry a
    connection string. The row already holds it. Both halves are asserted, so a message that
    dropped everything would fail the first."""
    org, run = run_for(NodeType.UPLOAD)
    secret = "postgresql://svc:hunter2@db.internal/app"
    monkeypatch.setattr(
        "datanika.tasks.upload_tasks.run_upload",
        _inner_that_ends(db_session, RunStatus.FAILED, [], error=secret),
    )

    message = str(run_upload_task.apply(args=[run.id, org.id]).result)

    assert f"run {run.id}" in message, message
    assert "hunter2" not in message, message
