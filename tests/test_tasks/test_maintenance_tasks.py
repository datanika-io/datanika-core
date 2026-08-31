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
from unittest.mock import MagicMock, patch

import pytest

from datanika.tasks.maintenance_tasks import run_maintenance_task

MODULE_LOGGER = "datanika.tasks.maintenance_tasks"


class SimulatedDatabaseError(RuntimeError):
    """Stands in for anything the DB block can raise (the task catches bare ``Exception``)."""


def _patch_sweeps(
    *,
    dbt_targets=4242,
    dlt_dirs=11,
    purged_runs=22,
    orphaned_archives=33,
    expired_reset_tokens=44,
    session_factory=None,
    commit=None,
):
    """Patch all five sweeps plus the session factory at the modules the task imports from.

    The task's imports are *function-local*, so they re-resolve on every call — patching the
    source module is what takes effect, not patching a name on ``maintenance_tasks``.
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
        patch("datanika.services.maintenance_service.purge_old_runs", **_spec(purged_runs)),
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
    def test_all_five_sweep_counts_are_reported_individually(self):
        """AC-3. A fix that collapses the counters into a status flag fails here."""
        result = _run()

        assert result == {
            "dbt_targets": 4242,
            "dlt_dirs": 11,
            "purged_runs": 22,
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
            _run(purged_runs=SimulatedDatabaseError("connection refused"))

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
            _run(purged_runs=SimulatedDatabaseError("deadlock detected"))

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
        actually land. Measured against the unfixed task, a commit failure returned **five**
        keys — byte-identical in shape to a clean run — because the ``except`` only
        overwrites two counters that were already assigned by then. So the single most
        consequential failure position was the one position that read as healthy.
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
            _run(purged_runs=SimulatedDatabaseError("deadlock detected"))

    def test_the_error_carries_the_counters_gathered_before_the_failure(self):
        """Replaces Infra's accidental three-vs-five-key probe with a deliberate one.

        Until now the only way to tell a thrown run from a clean one was that ``dlt_dirs``
        and ``expired_reset_tokens`` happen to go unassigned in the ``except``. That is an
        accident of which names that branch writes. Carrying the partial counters in the
        error makes "how far did it get" answerable on purpose.
        """
        with pytest.raises(Exception) as excinfo:  # noqa: B017
            _run(purged_runs=SimulatedDatabaseError("deadlock detected"))

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
            _run(purged_runs=SimulatedDatabaseError("deadlock detected"))

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
            _run(purged_runs=SimulatedDatabaseError("deadlock detected"))

        assert isinstance(excinfo.value.__cause__, SimulatedDatabaseError), (
            f"root cause not chained; __cause__ was {excinfo.value.__cause__!r}"
        )
