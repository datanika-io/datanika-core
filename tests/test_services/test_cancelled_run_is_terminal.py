"""core#657 AC2 — a cancelled run must not be overwritten back to success.

The lie this closes
-------------------
`POST /api/v1/runs/{id}/cancel` returns 200 with `status: cancelled`, and the worker keeps
going. When it finishes it calls `complete_run`, which sets `RunStatus.SUCCESS`
**unconditionally** — so the cancellation is cosmetic and transient, and the row flips back.

Product's framing, which is sharper than "nobody measured this": it is **not** a verdict nobody
measured, it is *a verdict reversed by the thing it claimed authority over*. The API rejects
non-cancellable states with a typed 409, which is exactly what makes the success path read as
trustworthy.

⚠️ What this file does NOT establish, stated because the issue has five ACs and this is one
--------------------------------------------------------------------------------------------
**AC1 — "a cancelled run stops doing work" — is untouched.** Nothing on the worker side asks
whether the run was cancelled (``git grep -nE "CANCELLED|revoke|is_cancel" -- datanika/tasks/``
→ 0 hits), so the run still burns warehouse load, source API quota and wall-clock. This file
only stops the *record* from lying afterwards. That is worth shipping alone — the caller is
currently told a false thing that a later read confirms — but it is not cancellation.

🚨 **AC4 — bytes are not billed after cancellation — is NOT closed by this, and the reason is
easy to get backwards.** ``complete_run`` emits **no hook at all**; the metering events fire
from the *tasks*, after it returns (``upload_tasks.py:390``, ``pipeline_tasks.py:371``,
``transformation_tasks.py:202``). So refusing the status write here leaves the
``run.*_completed`` hook firing exactly as before, and a cancelled run still meters. Anyone
reading a green suite here as "cancellation stops the bill" would be wrong.

Why the observational fields are still written
----------------------------------------------
Only ``status`` and ``finished_at`` are protected — those are the cancellation's own record.
``logs``, ``rows_loaded`` and ``bytes_processed`` are still written, because they are the
evidence of what the worker actually did before it was told to stop, and AC3 asks the product
to say what happened to partially-loaded data. Destroying that evidence to protect a status
field would trade one blind spot for another. It is safe to record ``bytes_processed`` here
precisely because this method does not meter (above).
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService


@pytest.fixture
def svc():
    return ExecutionService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-cancel-terminal")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def cancelled_run(svc, db_session, org):
    """A run that reached CANCELLED the way the API reaches it."""
    run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
    svc.start_run(db_session, org.id, run.id)
    cancelled = svc.cancel_run(db_session, org.id, run.id)
    assert cancelled is not None and cancelled.status == RunStatus.CANCELLED, (
        "fixture precondition failed — the rest of this file would assert nothing"
    )
    return cancelled


@pytest.fixture
def running_run(svc, db_session, org):
    """The negative control: an ordinary run nobody cancelled."""
    run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 2)
    svc.start_run(db_session, org.id, run.id)
    return run


class TestCompleteRunDoesNotResurrectACancelledRun:
    def test_the_status_stays_cancelled(self, svc, db_session, org, cancelled_run):
        svc.complete_run(db_session, org.id, cancelled_run.id, rows_loaded=99, logs="done")
        db_session.refresh(cancelled_run)
        assert cancelled_run.status == RunStatus.CANCELLED, (
            "complete_run overwrote a CANCELLED run back to SUCCESS. The user was told the "
            "run stopped, and a later read confirms it did not (core#657)."
        )

    def test_the_cancellation_timestamp_is_not_moved(self, svc, db_session, org, cancelled_run):
        """`finished_at` is when it was cancelled, not when the worker gave up.

        ⚠️ Both sides are read from the database. The first draft captured `original`
        from the in-memory instance and compared it to a post-`refresh` read, which
        FAILED on identical data: `cancel_run` writes `datetime.now(UTC)` (tz-aware) and
        SQLite returns it naive, so the two differed only by `tzinfo` at the same
        microsecond. That is a test comparing two representations of one value, and it
        would have been reported as the fix not working.
        """
        db_session.refresh(cancelled_run)
        original = cancelled_run.finished_at
        svc.complete_run(db_session, org.id, cancelled_run.id, rows_loaded=1, logs="x")
        db_session.refresh(cancelled_run)
        assert cancelled_run.finished_at == original

    def test_it_still_returns_the_run_rather_than_none(self, svc, db_session, org, cancelled_run):
        """`None` already means two things here; it must not come to mean a third.

        `get_org_run` returns None for "not yours or not found", and `cancel_run` returns None
        for "not in a cancellable state". A third meaning would make the refusal
        indistinguishable from a tenancy miss at every call site.
        """
        result = svc.complete_run(db_session, org.id, cancelled_run.id, rows_loaded=1, logs="x")
        assert result is not None
        assert result.id == cancelled_run.id
        assert result.status == RunStatus.CANCELLED

    def test_the_worker_s_evidence_is_still_recorded(self, svc, db_session, org, cancelled_run):
        svc.complete_run(
            db_session,
            org.id,
            cancelled_run.id,
            rows_loaded=42,
            logs="loaded 42 rows before stopping",
            bytes_processed=1234,
        )
        db_session.refresh(cancelled_run)
        assert cancelled_run.rows_loaded == 42
        assert cancelled_run.logs == "loaded 42 rows before stopping"
        assert cancelled_run.bytes_processed == 1234


class TestFailRunDoesNotOverwriteACancelledRun:
    def test_the_status_stays_cancelled(self, svc, db_session, org, cancelled_run):
        svc.fail_run(db_session, org.id, cancelled_run.id, error_message="boom", logs="trace")
        db_session.refresh(cancelled_run)
        assert cancelled_run.status == RunStatus.CANCELLED

    def test_it_does_not_announce_a_failure_the_user_did_not_have(
        self, svc, db_session, org, cancelled_run
    ):
        """A cancelled run that later errors must not page the user about a failure.

        `fail_run` announces `run.failed`, which reaches Slack, email and in-app. The user
        stopped this run on purpose; telling them it failed is a false alarm generated by
        their own cancellation.
        """
        with patch("datanika.hooks.announce") as announce:
            svc.fail_run(db_session, org.id, cancelled_run.id, error_message="boom", logs="t")
        assert announce.call_count == 0, (
            f"announced {announce.call_args_list} for a run the user cancelled"
        )

    def test_the_error_is_still_recorded(self, svc, db_session, org, cancelled_run):
        svc.fail_run(db_session, org.id, cancelled_run.id, error_message="boom", logs="trace")
        db_session.refresh(cancelled_run)
        assert cancelled_run.error_message == "boom"
        assert cancelled_run.logs == "trace"


class TestTheGuardIsNarrow:
    """Negative controls. Without these the guard is satisfied by refusing everything.

    Each drives the same method on an *uncancelled* run and requires the normal write.
    """

    def test_complete_run_still_completes_an_ordinary_run(self, svc, db_session, org, running_run):
        svc.complete_run(db_session, org.id, running_run.id, rows_loaded=7, logs="ok")
        db_session.refresh(running_run)
        assert running_run.status == RunStatus.SUCCESS
        assert running_run.finished_at is not None

    def test_fail_run_still_fails_an_ordinary_run(self, svc, db_session, org, running_run):
        with patch("datanika.hooks.announce") as announce:
            svc.fail_run(db_session, org.id, running_run.id, error_message="boom", logs="t")
        db_session.refresh(running_run)
        assert running_run.status == RunStatus.FAILED
        assert announce.call_count == 1, "the ordinary failure notification must still fire"

    def test_complete_run_still_returns_none_for_another_org(
        self, svc, db_session, org, running_run
    ):
        """The tenancy predicate must not be softened by the new early return."""
        other = Organization(name="OtherCo", slug="other-cancel-terminal")
        db_session.add(other)
        db_session.flush()
        assert (
            svc.complete_run(db_session, other.id, running_run.id, rows_loaded=1, logs="") is None
        )

    def test_a_success_run_is_not_protected_by_this_change(self, svc, db_session, org, running_run):
        """Deliberately scoped to CANCELLED, and this pins that it is deliberate.

        Whether a terminal SUCCESS or FAILED should also be write-once is a real question —
        `append_logs` exists because post-completion work must not change status — but it is
        a wider contract change with call sites I have not audited, and core#657 AC2 asks for
        CANCELLED. Narrow on purpose; this test fails if someone widens it without saying so.
        """
        svc.complete_run(db_session, org.id, running_run.id, rows_loaded=1, logs="first")
        db_session.refresh(running_run)
        assert running_run.status == RunStatus.SUCCESS

        svc.fail_run(db_session, org.id, running_run.id, error_message="later", logs="second")
        db_session.refresh(running_run)
        assert running_run.status == RunStatus.FAILED, (
            "a terminal SUCCESS is still overwritable — that is the current contract and is "
            "outside core#657 AC2. If you intended to change it, change this test too."
        )


def test_the_fixture_can_produce_an_uncancelled_run(svc, db_session, org, running_run):
    """Floor: if `running_run` were arriving already CANCELLED, every control above passes
    for the wrong reason."""
    assert running_run.status == RunStatus.RUNNING


def test_cancel_run_still_refuses_a_finished_run(svc, db_session, org, running_run):
    """Unchanged behaviour, pinned because this change is adjacent to it."""
    svc.complete_run(db_session, org.id, running_run.id, rows_loaded=1, logs="")
    assert svc.cancel_run(db_session, org.id, running_run.id) is None


def test_finished_at_is_actually_set_by_cancel(cancelled_run):
    """`test_the_cancellation_timestamp_is_not_moved` compares against this value.

    If `cancel_run` left `finished_at` NULL, that assertion would be `None == None` and would
    pass no matter what `complete_run` did.
    """
    assert cancelled_run.finished_at is not None
    assert isinstance(cancelled_run.finished_at, datetime)
