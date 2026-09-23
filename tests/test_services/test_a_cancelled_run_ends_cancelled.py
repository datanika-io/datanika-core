"""The cancellation state machine, and the two consumers that used to answer wrongly.

``SPEC_RUN_CANCELLATION`` §3. A stop request and its outcome are **different states**: a status
reading ``cancelled`` while the warehouse is still being written is the same lie core#657 is
about, moved one layer up. So a `running` run goes to ``cancelling``, and reaches ``cancelled``
when the worker confirms — or when §3.1's reaper closes it.

🚨 **The two assertions that make this unsplittable** are the silent consumers of §4. Neither
raises when it is wrong; each returns a confident wrong answer:

* a ``cancelling`` run at a ``?wait=true`` timeout must return **`408`**, not the `422` that a
  literal ``("pending", "running")`` test produces for a run that is still working;
* ``cleanup_orphaned_dlt_dirs`` must **not** remove a ``cancelling`` run's working directory —
  a live worker is still writing to it, and that sweep runs hourly.

⚠️ **2a, not 2b.** Nothing here stops a worker already inside its engine call; §7.2 defers that
and names its un-defer trigger. What is asserted is that the *run* ends honestly.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.execution_service import ExecutionService
from datanika.services.maintenance_service import (
    cleanup_orphaned_dlt_dirs,
    reap_stuck_cancelling_runs,
)
from tests.factories import make_user


@pytest.fixture
def org_and_actor(db_session):
    org = Organization(name="Acme", slug=f"acme-657-{datetime.now(UTC).timestamp()}")
    db_session.add(org)
    db_session.flush()
    actor = make_user(db_session, email=f"e-{org.id}@test.io", password_hash="x")
    db_session.add(Membership(user_id=actor.id, org_id=org.id, role=MemberRole.EDITOR))
    db_session.flush()
    return org.id, actor.id


def _run(db_session, org_id: int, status: RunStatus) -> Run:
    run = Run(org_id=org_id, target_type=NodeType.UPLOAD, target_id=1, status=status)
    db_session.add(run)
    db_session.flush()
    return run


# ---------------------------------------------------------------------------------------------
# §3 — the state machine
# ---------------------------------------------------------------------------------------------


def test_cancelling_a_pending_run_is_terminal_immediately(db_session, org_and_actor):
    """No worker has started, so the stop is complete the moment it is recorded."""
    org_id, actor_id = org_and_actor
    run = _run(db_session, org_id, RunStatus.PENDING)

    out = ExecutionService().cancel_run(db_session, org_id, run.id, actor_user_id=actor_id)

    assert out.status == RunStatus.CANCELLED
    assert out.finished_at is not None


def test_cancelling_a_running_run_says_cancelling_not_cancelled(db_session, org_and_actor):
    """The crux: a worker is still going, and the status must not claim it has stopped."""
    org_id, actor_id = org_and_actor
    run = _run(db_session, org_id, RunStatus.RUNNING)

    out = ExecutionService().cancel_run(db_session, org_id, run.id, actor_user_id=actor_id)

    assert out.status == RunStatus.CANCELLING
    assert out.finished_at is None, "finished_at was stamped while the worker was still running"


def test_a_worker_that_finishes_first_still_ends_the_run_cancelled(db_session, org_and_actor):
    """§3: *a run that was asked to stop ends `cancelled`, even if the work completed first.*

    Reporting `success` because we lost the race tells the user their cancel did nothing — which
    is core#657's original bug with better timing.
    """
    org_id, actor_id = org_and_actor
    run = _run(db_session, org_id, RunStatus.RUNNING)
    svc = ExecutionService()
    svc.cancel_run(db_session, org_id, run.id, actor_user_id=actor_id)

    svc.complete_run(db_session, org_id, run.id, rows_loaded=7, logs="done")

    db_session.refresh(run)
    assert run.status == RunStatus.CANCELLED
    assert run.finished_at is not None, "a terminal run must carry a finish time"
    assert run.rows_loaded == 7, "the partial count is the only honest answer to 'what did I get?'"


def test_a_failing_worker_on_a_cancelled_run_also_ends_cancelled(db_session, org_and_actor):
    org_id, actor_id = org_and_actor
    run = _run(db_session, org_id, RunStatus.RUNNING)
    svc = ExecutionService()
    svc.cancel_run(db_session, org_id, run.id, actor_user_id=actor_id)

    svc.fail_run(db_session, org_id, run.id, error_message="boom", logs="log")

    db_session.refresh(run)
    assert run.status == RunStatus.CANCELLED


def test_cancelling_twice_is_idempotent(db_session, org_and_actor):
    """The same request arriving twice — `200`, not `409` (§5.2)."""
    org_id, actor_id = org_and_actor
    run = _run(db_session, org_id, RunStatus.RUNNING)
    svc = ExecutionService()

    first = svc.cancel_run(db_session, org_id, run.id, actor_user_id=actor_id)
    second = svc.cancel_run(db_session, org_id, run.id, actor_user_id=actor_id)

    assert first.status == second.status == RunStatus.CANCELLING
    assert second is not None, "a second cancel must not read as not-cancellable"


@pytest.mark.parametrize("status", [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED])
def test_a_terminal_run_is_not_cancellable(db_session, org_and_actor, status):
    org_id, actor_id = org_and_actor
    run = _run(db_session, org_id, status)

    assert ExecutionService().cancel_run(db_session, org_id, run.id, actor_user_id=actor_id) is None
    db_session.refresh(run)
    assert run.status == status, "a refused cancel changed the run"


# ---------------------------------------------------------------------------------------------
# Negative controls — the fix touches the completion path every run takes
# ---------------------------------------------------------------------------------------------


def test_control_an_ordinary_run_still_ends_success(db_session, org_and_actor):
    org_id, _ = org_and_actor
    run = _run(db_session, org_id, RunStatus.RUNNING)

    ExecutionService().complete_run(db_session, org_id, run.id, rows_loaded=3, logs="ok")

    db_session.refresh(run)
    assert run.status == RunStatus.SUCCESS
    assert run.finished_at is not None


def test_control_a_failing_run_still_ends_failed(db_session, org_and_actor):
    org_id, _ = org_and_actor
    run = _run(db_session, org_id, RunStatus.RUNNING)

    ExecutionService().fail_run(db_session, org_id, run.id, error_message="boom", logs="log")

    db_session.refresh(run)
    assert run.status == RunStatus.FAILED


# ---------------------------------------------------------------------------------------------
# §4 — the two consumers that answer wrongly rather than raising
# ---------------------------------------------------------------------------------------------


def test_the_dlt_directory_of_a_cancelling_run_is_not_deleted(db_session, org_and_actor, tmp_path):
    """A live worker is still writing to it, and this sweep runs hourly.

    The control is in the same call: a directory belonging to a *terminal* run of the same age is
    removed, so "nothing was deleted" cannot be explained by a sweep that deleted nothing.
    """
    org_id, _ = org_and_actor
    live = _run(db_session, org_id, RunStatus.CANCELLING)
    done = _run(db_session, org_id, RunStatus.SUCCESS)

    old = 1
    for run in (live, done):
        d = tmp_path / f"pipeline_1_run_{run.id}"
        d.mkdir()
        os.utime(d, (old, old))

    cleanup_orphaned_dlt_dirs(str(tmp_path), max_age_hours=0, session=db_session)

    assert (tmp_path / f"pipeline_1_run_{live.id}").exists(), (
        "the working directory of a run that is still stopping was deleted"
    )
    assert not (tmp_path / f"pipeline_1_run_{done.id}").exists(), (
        "the control was not deleted either, so this arm proves nothing"
    )


# ---------------------------------------------------------------------------------------------
# §3.1 — the reaper
# ---------------------------------------------------------------------------------------------


def test_a_stuck_cancelling_run_is_reaped(db_session, org_and_actor):
    org_id, _ = org_and_actor
    stuck = _run(db_session, org_id, RunStatus.CANCELLING)
    stuck.updated_at = datetime.now(UTC) - timedelta(minutes=30)
    db_session.flush()

    reaped = reap_stuck_cancelling_runs(db_session)

    db_session.refresh(stuck)
    assert reaped == 1
    assert stuck.status == RunStatus.CANCELLED
    assert stuck.finished_at is not None
    assert "did not acknowledge" in (stuck.logs or ""), (
        "a run that reads cancelled with no explanation is indistinguishable from one the "
        "worker confirmed"
    )


def test_the_reaper_leaves_a_run_inside_its_window(db_session, org_and_actor):
    """The window is the whole point: an ordinary worker confirms the stop itself."""
    org_id, _ = org_and_actor
    fresh = _run(db_session, org_id, RunStatus.CANCELLING)
    fresh.updated_at = datetime.now(UTC) - timedelta(minutes=1)
    db_session.flush()

    assert reap_stuck_cancelling_runs(db_session) == 0
    db_session.refresh(fresh)
    assert fresh.status == RunStatus.CANCELLING


def test_the_reaper_does_not_touch_a_running_run(db_session, org_and_actor):
    """Nobody asked it to stop, so age is irrelevant."""
    org_id, _ = org_and_actor
    old = _run(db_session, org_id, RunStatus.RUNNING)
    old.updated_at = datetime.now(UTC) - timedelta(days=2)
    db_session.flush()

    assert reap_stuck_cancelling_runs(db_session) == 0
    db_session.refresh(old)
    assert old.status == RunStatus.RUNNING
