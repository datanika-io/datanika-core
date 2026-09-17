"""A cancellation committed by ANOTHER session must survive the worker's own bookkeeping (core#657).

``complete_run`` and ``fail_run`` guarded with ``if not is_cancelled(run)`` on ``run =
get_org_run(session, ...)`` — an ORM ``SELECT``, which does **not** overwrite the loaded
attributes of an instance already in the session's identity map. Every task holds exactly such an
instance for the whole engine call (``run_upload`` loads it right after ``start_run`` to read
``run.target_id``). So when the API committed ``CANCELLED`` from its own session, the worker's guard
read its own stale ``RUNNING`` and wrote ``SUCCESS`` — or ``FAILED``, and announced a failure —
over the cancellation.

The existing tests (``test_cancelled_run_is_terminal.py``) cancel and complete **in one session**,
where the identity map is never stale, so they could not see it.

This module reproduces production's shape: a file-backed database, the worker session built like
``datanika/db.py``'s ``sync_session_factory`` (``expire_on_commit=False``), the API cancelling from
a separate session, and the worker still holding the run it loaded. Measured before the fix:
``complete_run`` → ``success``, ``fail_run`` → ``failed``, and ``start_run`` on a run cancelled
while ``PENDING`` → ``running``.
"""

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from datanika import hooks
from datanika.models.base import Base
from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService, get_org_run
from tests.factories import make_org_admin

svc = ExecutionService()


@pytest.fixture
def factory(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'runs.db'}")
    Base.metadata.create_all(engine)
    try:
        # As production's worker factory: loaded attributes survive a commit.
        yield sessionmaker(engine, class_=Session, expire_on_commit=False)
    finally:
        engine.dispose()


@pytest.fixture
def pending_run(factory):
    with factory() as setup:
        org = Organization(name="Acme", slug="acme-657")
        setup.add(org)
        setup.flush()
        run = svc.create_run(setup, org.id, NodeType.UPLOAD, 1)
        setup.commit()
        return org.id, run.id


def _api_cancels(factory, org_id, run_id):
    """The API request: its own session, cancels, commits."""
    with factory() as api:
        assert (
            svc.cancel_run(api, org_id, run_id, actor_user_id=make_org_admin(api, org_id))
            is not None
        )
        api.commit()


def _final(factory, run_id):
    with factory() as reader:
        return reader.execute(select(Run.status, Run.started_at).where(Run.id == run_id)).one()


def _worker_started_and_holding(factory, org_id, run_id):
    """``run_upload``'s shape: start, commit, then load the run and keep the reference."""
    worker = factory()
    svc.start_run(worker, org_id, run_id)
    worker.commit()
    held = get_org_run(worker, org_id, run_id)
    return worker, held


@pytest.fixture
def announced():
    calls = []

    def _record(**kwargs):
        calls.append(kwargs)

    hooks.on("run.failed", _record)
    try:
        yield calls
    finally:
        hooks.off("run.failed", _record)


# ---------------------------------------------------------------------------
# Anti-vacuity: the harness really produces a stale identity map
# ---------------------------------------------------------------------------


def test_harness_the_held_run_is_stale_after_a_cross_session_cancel(factory, pending_run):
    """Without this, a green below could mean "the harness never recreated the defect"."""
    org_id, run_id = pending_run
    worker, held = _worker_started_and_holding(factory, org_id, run_id)
    _api_cancels(factory, org_id, run_id)

    reloaded = get_org_run(worker, org_id, run_id)
    try:
        assert reloaded is held, "the ORM did not hand back the identity-mapped instance"
        assert held.status == RunStatus.RUNNING, (
            "the held instance was refreshed, so nothing is stale"
        )
        assert _final(factory, run_id).status == RunStatus.CANCELLED
    finally:
        worker.close()


# ---------------------------------------------------------------------------
# Regressions — red before the guards read the database
# ---------------------------------------------------------------------------


def test_complete_run_does_not_overwrite_a_cancel_from_another_session(factory, pending_run):
    org_id, run_id = pending_run
    worker, held = _worker_started_and_holding(factory, org_id, run_id)
    _api_cancels(factory, org_id, run_id)

    returned = svc.complete_run(worker, org_id, run_id, rows_loaded=3, logs="engine finished")
    worker.commit()
    worker.close()

    assert _final(factory, run_id).status == RunStatus.CANCELLED, (
        "the worker's stale RUNNING overwrote the user's cancellation with SUCCESS"
    )
    assert returned.status == RunStatus.CANCELLED, "the returned run disagrees with the database"


def test_complete_run_still_records_what_the_worker_did(factory, pending_run):
    """Only status and finished_at belong to the cancellation; the evidence is still written."""
    org_id, run_id = pending_run
    worker, _held = _worker_started_and_holding(factory, org_id, run_id)
    _api_cancels(factory, org_id, run_id)

    svc.complete_run(worker, org_id, run_id, rows_loaded=3, logs="engine finished")
    worker.commit()
    worker.close()

    with factory() as reader:
        row = reader.execute(select(Run.rows_loaded, Run.logs).where(Run.id == run_id)).one()
    assert (row.rows_loaded, row.logs) == (3, "engine finished")


def test_fail_run_does_not_overwrite_a_cancel_or_page_anyone(factory, pending_run, announced):
    org_id, run_id = pending_run
    worker, _held = _worker_started_and_holding(factory, org_id, run_id)
    _api_cancels(factory, org_id, run_id)

    svc.fail_run(worker, org_id, run_id, error_message="boom", logs="tb")
    worker.commit()
    worker.close()

    assert _final(factory, run_id).status == RunStatus.CANCELLED
    assert announced == [], "a run the user cancelled was announced as a failure"


def test_start_run_does_not_start_a_run_cancelled_while_pending(factory, pending_run):
    """Product traced this on 2026-09-15; measured before the fix: ``running``."""
    org_id, run_id = pending_run
    _api_cancels(factory, org_id, run_id)

    with factory() as worker:
        returned = svc.start_run(worker, org_id, run_id)
        worker.commit()

    final = _final(factory, run_id)
    assert final.status == RunStatus.CANCELLED
    assert final.started_at is None, "a run that never started was given a start time"
    assert returned.status == RunStatus.CANCELLED


def test_start_run_does_not_start_a_cancelled_run_the_worker_already_holds(factory, pending_run):
    """The worker loaded the run while it was PENDING (``run_pipeline`` loads before starting)."""
    org_id, run_id = pending_run
    worker = factory()
    held = get_org_run(worker, org_id, run_id)
    assert held.status == RunStatus.PENDING
    _api_cancels(factory, org_id, run_id)

    svc.start_run(worker, org_id, run_id)
    worker.commit()
    worker.close()

    assert _final(factory, run_id).status == RunStatus.CANCELLED


# ---------------------------------------------------------------------------
# Controls — an ordinary run still moves (SPEC_RUN_CANCELLATION §7.3)
# ---------------------------------------------------------------------------


def test_control_an_uncancelled_run_starts_and_completes(factory, pending_run):
    org_id, run_id = pending_run
    worker, _held = _worker_started_and_holding(factory, org_id, run_id)
    assert _final(factory, run_id).status == RunStatus.RUNNING

    svc.complete_run(worker, org_id, run_id, rows_loaded=3, logs="ok")
    worker.commit()
    worker.close()

    final = _final(factory, run_id)
    assert final.status == RunStatus.SUCCESS
    assert final.started_at is not None


def test_control_an_uncancelled_run_fails_and_is_announced(factory, pending_run, announced):
    org_id, run_id = pending_run
    worker, _held = _worker_started_and_holding(factory, org_id, run_id)

    svc.fail_run(worker, org_id, run_id, error_message="boom", logs="tb")
    worker.commit()
    worker.close()

    assert _final(factory, run_id).status == RunStatus.FAILED
    assert len(announced) == 1


def test_control_the_guards_stay_scoped_to_the_org(factory, pending_run):
    org_id, run_id = pending_run
    with factory() as worker:
        assert svc.start_run(worker, org_id + 999, run_id) is None
        assert svc.complete_run(worker, org_id + 999, run_id, rows_loaded=1, logs="") is None
        assert svc.fail_run(worker, org_id + 999, run_id, error_message="", logs="") is None
        worker.commit()
    assert _final(factory, run_id).status == RunStatus.PENDING
