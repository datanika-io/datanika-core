"""Regression tests for ``run_maintenance_task``'s failure reporting (core#709).

The task used to swallow every database exception, zero two of its own counters, and then
fall through to the *same* ``Maintenance complete`` INFO line and the *same* dict return as
a healthy no-op. A total failure and a clean sweep were byte-identical to every observer
this task has: the worker log, ``celery_tasks_total``, and the Celery result backend.

Beat began firing this task hourly in production on 2026-08-30 (core#653 / core#717), so
every one of those runs was unverifiable until this landed.

Two groups of tests here do different jobs, and mixing them up is how this file would rot:

* **Controls** — green *before* and *after* the fix. They attribute a red run to the defect
  rather than to a harness that never reached the task, and they stop a "raise
  unconditionally" or "collapse the counters into a boolean" implementation from passing.
* **Regressions** — red before, green after.
"""

import contextlib
import logging
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from datanika.models.run import Run, RunStatus
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService
from datanika.tasks.maintenance_tasks import run_maintenance_task

MODULE_LOGGER = "datanika.tasks.maintenance_tasks"


class SimulatedDatabaseError(RuntimeError):
    """Stands in for anything the DB block can raise (the task catches bare ``Exception``)."""


def _patch_sweeps(
    *,
    dbt_targets=4242,
    dlt_dirs=11,
    orphaned_archives=33,
    expired_reset_tokens=44,
    session_factory=None,
    commit=None,
):
    """Patch all four sweeps plus the session factory at the modules the task imports from.

    The task's imports are *function-local*, so they re-resolve on every call — patching the
    source module is what takes effect, not patching a name on ``maintenance_tasks``.

    There were five sweeps until core#1000 removed the run purge. ``orphaned_archives`` has
    taken over as the "a sweep midway through the DB block throws" injection point below.
    """
    session = MagicMock()
    if commit is not None:
        session.commit.side_effect = commit
    session_cm = MagicMock()
    session_cm.__enter__.return_value = session
    factory = session_factory or MagicMock(return_value=session_cm)
    return [
        patch(
            "datanika.services.maintenance_service.cleanup_dbt_targets",
            **_spec(dbt_targets),
        ),
        patch(
            "datanika.services.maintenance_service.cleanup_orphaned_dlt_dirs",
            **_spec(dlt_dirs),
        ),
        patch(
            "datanika.services.maintenance_service.cleanup_orphaned_archives",
            **_spec(orphaned_archives),
        ),
        patch(
            "datanika.services.password_reset_service.PasswordResetService.purge_expired",
            **_spec(expired_reset_tokens),
        ),
        patch("datanika.ui.state.base_state.get_sync_session", factory),
    ]


def _spec(value):
    """``side_effect`` if the caller handed us an exception, else ``return_value``."""
    if isinstance(value, BaseException) or (
        isinstance(value, type) and issubclass(value, BaseException)
    ):
        return {"side_effect": value}
    return {"return_value": value}


def _run(**kwargs):
    patches = _patch_sweeps(**kwargs)
    for p in patches:
        p.start()
    try:
        return run_maintenance_task()
    finally:
        for p in reversed(patches):
            p.stop()


# ---------------------------------------------------------------------------
# Controls — these pass BEFORE the fix and must keep passing after it.
# ---------------------------------------------------------------------------


class TestSuccessPathIsUnchanged:
    def test_all_four_sweep_counts_are_reported_individually(self):
        """AC-3. A fix that collapses the counters into a status flag fails here.

        Five until core#1000 removed the run purge; the assertion is exhaustive on purpose,
        so a re-introduced sweep shows up here as well as in ``TestRunHistoryIsNeverPurged``.
        """
        result = _run()

        assert result == {
            "dbt_targets": 4242,
            "dlt_dirs": 11,
            "orphaned_archives": 33,
            "expired_reset_tokens": 44,
        }

    def test_a_clean_run_still_logs_maintenance_complete_at_info(self, caplog):
        """A fix that logs ERROR or raises unconditionally fails here."""
        with caplog.at_level(logging.INFO, logger=MODULE_LOGGER):
            _run()

        assert any("Maintenance complete" in r.getMessage() for r in caplog.records), (
            f"success path lost its log line; got {[r.getMessage() for r in caplog.records]}"
        )
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR], (
            "a clean run must not log at ERROR"
        )

    def test_a_failing_run_was_already_logging_the_exception(self, caplog):
        """Documents what was *not* missing.

        ``logger.exception`` already emitted an ERROR record on the failure path. The
        defect was never a missing error log — it was the success log printing *as well*,
        beneath a line that says *complete*. Pinning this keeps a future reader from
        "fixing" the wrong half.
        """
        # Deliberately tolerant of both outcomes: before the fix the task returned, after it
        # raises. Requiring either one here would make this a regression test wearing a
        # control's label, and it would stop attributing the red run to the defect.
        with (
            caplog.at_level(logging.INFO, logger=MODULE_LOGGER),
            contextlib.suppress(Exception),
        ):
            _run(orphaned_archives=SimulatedDatabaseError("connection refused"))

        assert [r for r in caplog.records if r.levelno >= logging.ERROR], (
            "the failure path must still log at ERROR"
        )


class TestFilesystemSweepStillPropagates:
    def test_a_dbt_target_sweep_failure_is_not_swallowed(self):
        """Anti-widening pin (core#709 explicitly warns against this).

        ``cleanup_dbt_targets`` runs *outside* the ``try``, so a filesystem failure has
        always propagated. A fix that widens the ``try`` to cover it would make the bug
        worse while looking like extra safety — this test goes red if anyone does.
        """
        with pytest.raises(SimulatedDatabaseError):
            _run(dbt_targets=SimulatedDatabaseError("disk gone"))


# ---------------------------------------------------------------------------
# Regressions — red before the fix.
# ---------------------------------------------------------------------------


class TestDatabaseFailureIsReported:
    def test_an_unreachable_database_does_not_return_a_success_shaped_result(self):
        """AC-2, in the issue's own words: patch the session factory to raise."""
        factory = MagicMock(side_effect=SimulatedDatabaseError("could not connect"))

        with pytest.raises(Exception) as excinfo:  # noqa: B017
            _run(session_factory=factory)

        assert not isinstance(excinfo.value, AssertionError)

    def test_the_failure_path_does_not_log_maintenance_complete(self, caplog):
        """AC-1: distinguishable *from the log alone*, without counting dict keys."""
        with (
            caplog.at_level(logging.INFO, logger=MODULE_LOGGER),
            pytest.raises(Exception),  # noqa: B017
        ):
            _run(session_factory=MagicMock(side_effect=SimulatedDatabaseError("down")))

        completes = [r for r in caplog.records if "Maintenance complete" in r.getMessage()]
        assert not completes, (
            f"a failed sweep still printed the success line: {[r.getMessage() for r in completes]}"
        )

    def test_a_failure_midway_through_the_block_is_reported(self):
        """The session opens fine and an inner sweep throws — the common real shape."""
        with pytest.raises(Exception):  # noqa: B017
            _run(orphaned_archives=SimulatedDatabaseError("deadlock detected"))

    def test_a_reset_token_purge_failure_is_reported(self):
        """The last statement in the block, and the compliance-adjacent sweep (core#623).

        Spent reset-token hashes land in every nightly ``pg_dump`` and nothing else purges
        them, so this sweep silently failing looks exactly like it working.
        """
        with pytest.raises(Exception):  # noqa: B017
            _run(expired_reset_tokens=SimulatedDatabaseError("statement timeout"))

    def test_a_commit_failure_is_reported(self):
        """The sharpest case, and the one the old external probe could not see at all.

        Every sweep in the block buffers its DELETEs; ``session.commit()`` is where they
        actually land. Measured against the unfixed task, a commit failure returned the
        **full** set of keys — byte-identical in shape to a clean run — because the
        ``except`` only overwrites two counters that were already assigned by then. So the
        single most consequential failure position was the one position that read as
        healthy. (That full set was five at the time and is four since core#1000; the
        argument is about the shape being indistinguishable, not about the number.)
        """
        with pytest.raises(Exception):  # noqa: B017
            _run(commit=SimulatedDatabaseError("could not serialize access"))


class TestFailureReportCarriesContext:
    def test_the_failure_is_a_named_maintenance_error(self):
        """A typed marker, so the Celery result backend records a name worth reading.

        Imported inside the test on purpose: at module scope this would break collection
        before the fix and take the controls down with it.
        """
        from datanika.tasks.maintenance_tasks import MaintenanceError

        with pytest.raises(MaintenanceError):
            _run(orphaned_archives=SimulatedDatabaseError("deadlock detected"))

    def test_the_error_carries_the_counters_gathered_before_the_failure(self):
        """Replaces Infra's accidental three-vs-five-key probe with a deliberate one.

        Until now the only way to tell a thrown run from a clean one was that ``dlt_dirs``
        and ``expired_reset_tokens`` happen to go unassigned in the ``except``. That is an
        accident of which names that branch writes. Carrying the partial counters in the
        error makes "how far did it get" answerable on purpose.
        """
        with pytest.raises(Exception) as excinfo:  # noqa: B017
            _run(orphaned_archives=SimulatedDatabaseError("deadlock detected"))

        message = str(excinfo.value)
        assert "dbt_targets" in message, f"partial counters missing from {message!r}"
        assert "4242" in message, f"partial counters missing from {message!r}"

    def test_the_error_survives_celerys_json_exception_codec(self):
        """Validated against the real consumer, not against this file's own assumptions.

        Infra reads the Celery **result backend**, not the log. With ``result_serializer =
        "json"`` the backend stores ``{exc_type, exc_message, exc_module}`` — so an error
        whose args are not JSON-encodable would degrade to ``str()`` or lose the counters,
        and the probe would quietly stop discriminating again. One string arg keeps it
        readable; this test fails if anyone gives ``MaintenanceError`` a richer signature.
        """
        import json

        from datanika.tasks.celery_app import celery_app
        from datanika.tasks.maintenance_tasks import MaintenanceError

        with pytest.raises(MaintenanceError) as excinfo:
            _run(orphaned_archives=SimulatedDatabaseError("deadlock detected"))

        prepared = celery_app.backend.prepare_exception(excinfo.value)
        restored = json.loads(json.dumps(prepared))

        assert restored["exc_type"] == "MaintenanceError"
        assert restored["exc_module"] == "datanika.tasks.maintenance_tasks"
        assert "4242" in " ".join(restored["exc_message"]), (
            f"partial counters did not survive serialization: {restored}"
        )

    def test_the_original_exception_is_preserved_as_the_cause(self):
        """``raise ... from exc`` — losing the root cause would trade one blind spot for another."""
        with pytest.raises(Exception) as excinfo:  # noqa: B017
            _run(orphaned_archives=SimulatedDatabaseError("deadlock detected"))

        assert isinstance(excinfo.value.__cause__, SimulatedDatabaseError), (
            f"root cause not chained; __cause__ was {excinfo.value.__cause__!r}"
        )


# ---------------------------------------------------------------------------
# core#1000 — run history is never purged. Real session, real DB sweeps.
# ---------------------------------------------------------------------------


def _drive_against(db_session):
    """Run the REAL task against a REAL session, redirecting only the filesystem sweeps.

    ``cleanup_dbt_targets`` and ``cleanup_orphaned_dlt_dirs`` are the only two steps that
    ``shutil.rmtree`` real directories off ``settings.dbt_projects_dir`` /
    ``settings.dlt_pipelines_dir``, which on a dev box are inside the worktree. They are
    stubbed for that reason and no other.

    **Every database path stays real** — the run purge, the archive sweep, the reset-token
    sweep and ``session.commit()``. Stubbing the sweep whose absence is the claim is the
    one thing that would make these tests unable to fail (WORKFLOW_RULES: never mock the
    module whose surface is the claim).
    """
    session_cm = MagicMock()
    session_cm.__enter__.return_value = db_session
    session_cm.__exit__.return_value = False
    with (
        patch("datanika.services.maintenance_service.cleanup_dbt_targets", return_value=0),
        patch("datanika.services.maintenance_service.cleanup_orphaned_dlt_dirs", return_value=0),
        patch(
            "datanika.ui.state.base_state.get_sync_session",
            MagicMock(return_value=session_cm),
        ),
    ):
        return run_maintenance_task()


def _org_with_an_ancient_completed_run(db_session):
    org = Organization(name="RetentionOrg", slug=f"retention-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()
    run = Run(
        org_id=org.id,
        target_type="upload",
        target_id=1,
        status=RunStatus.SUCCESS,
    )
    db_session.add(run)
    db_session.flush()
    # Older than any retention window anyone would plausibly choose.
    run.created_at = datetime.utcnow() - timedelta(days=400)
    db_session.flush()
    return org, run


class TestRunHistoryIsNeverPurged:
    """core#1000, founder decision 2026-09-03 (option B): the pages are right, the sweep was wrong.

    ``purge_old_runs`` soft-deleted completed runs older than 90 days and **no reader could
    observe the mark**: ``ExecutionService.list_runs``, ``get_org_run`` and
    ``dependency_check`` all carry no ``deleted_at`` predicate, and this codebase has no
    global soft-delete filter. (``cleanup_orphaned_dlt_dirs`` does read ``Run.deleted_at``,
    but only over RUNNING/PENDING runs, which the purge never marked.) So the sweep hid
    nothing, removed nothing and logged a success count hourly from 2026-08-30, when beat
    first ran (core#653).

    The retention we publish is *"for as long as the organization exists"* —
    ``datanika-landing/src/pages/privacy.astro`` and ``trust.astro``. Until now the only
    thing keeping that sentence true was the **missing read-side filter**: an ordinary
    tidy-up adding ``Run.deleted_at.is_(None)`` to ``list_runs`` would have falsified two
    published legal pages, with no failing test and nothing in the diff naming a legal page.

    These tests bind the code to the published claim from the *write* side, where the defect
    was, so the sweep cannot return without a red.
    """

    def test_an_ancient_completed_run_survives_the_hourly_sweep(self, db_session):
        org, run = _org_with_an_ancient_completed_run(db_session)

        _drive_against(db_session)

        db_session.refresh(run)
        assert run.deleted_at is None, (
            "the hourly maintenance sweep soft-deleted a 400-day-old run; /privacy and "
            "/trust publish 'retained for as long as the organization exists'"
        )
        listed = ExecutionService().list_runs(db_session, org.id)
        assert [r.id for r in listed] == [run.id], (
            f"the run left the org's history after one sweep; list_runs returned {listed!r}"
        )

    def test_the_sweep_reports_four_counters_and_no_run_purge(self, db_session):
        """Both directions in one assertion.

        ``purged_runs`` absent is the regression half — the hourly INFO line used to report
        a purge count, so reading the log was enough to conclude retention worked. The four
        named keys are the control: a fix that deletes the task, empties the block or
        collapses the counters fails here rather than passing by doing nothing.
        """
        _org_with_an_ancient_completed_run(db_session)

        result = _drive_against(db_session)

        assert set(result) == {
            "dbt_targets",
            "dlt_dirs",
            "orphaned_archives",
            "expired_reset_tokens",
        }, f"sweep counters changed shape: {result}"
