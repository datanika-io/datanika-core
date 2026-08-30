"""core#648 — the scheduler must not be started by importing the app.

`datanika/datanika.py` calls ``scheduler_integration.start()`` and ``sync_all()``
as **module-level statements**, so every process that imports the app gets its own
APScheduler against one lock-free jobstore. Measured, twice, at two different
worker counts:

===================================  ==========================
prod, ``GRANIAN_WORKERS: "4"``       **5** ``Scheduler started``
staging, ``GRANIAN_WORKERS: "2"``    **3** ``Scheduler started``
===================================  ==========================

so the count is ``GRANIAN_WORKERS + 1`` (the granian parent imports too), and a
scheduled run can fire up to that many times. Latent only because prod currently
has zero active schedules.

Why this test is shaped the way it is
-------------------------------------

The obvious cheap test — *"exactly one compose service runs the scheduler"* —
**cannot see this bug, and would have been green throughout it.**
``docker-compose.yml`` declares exactly one scheduler-running service per colour
and always has; the multiplier is ``GRANIAN_WORKERS``, not a duplicated service.
A cardinality-over-services assertion counts 1 and passes while five schedulers
run. That is worth having as a separate guard against someone adding a *second*
scheduler service, but it is not coverage of this issue, and conflating the two
is how a green ships over a live defect. Same family as core#646, where the
manifest was never the thing that was wrong.

So this asserts the **mechanism** instead — nothing starts a scheduler at import
time — which is process-count-independent and, deliberately, **true under every
fix currently in play**: a dedicated singleton container, a leader lock, or an
opt-in flag all have to stop the import-time start. It encodes no architecture,
so it cannot silently endorse the wrong one.

``xfail(strict=True)`` rather than a red test: green on ``dev`` today so it holds
no promotion, and it **fails the moment the fix lands unless the marker is removed
with it**, so the coverage cannot ship switched off. A regression test parked on a
branch is a regression test nobody runs.
"""

import ast
import inspect
from pathlib import Path

import pytest

APP_MODULE = Path(__file__).resolve().parents[2] / "datanika" / "datanika.py"

#: Calls that arm the shared scheduler. ``sync_all`` is included because it is
#: the other half of the import-time block and writes the jobstore.
SCHEDULER_ARMING_CALLS = {"start", "sync_all"}
SCHEDULER_OBJECT = "scheduler_integration"


def _module_level_scheduler_calls(source: str) -> list[str]:
    """Calls like ``scheduler_integration.start()`` at module scope.

    Module scope only: the same call inside a function or an ``if`` guard is
    fine — that is what several of the candidate fixes look like. What is not
    fine is a statement that runs merely because something imported this module.
    """
    found: list[str] = []

    def visit(node: ast.AST) -> None:
        # Prune definitions. `ast.walk` descends into them, which would flag a
        # call inside `def boot(): scheduler_integration.start()` — i.e. it would
        # fail every correct fix. Caught by TestTheGuardCanActuallyFail below,
        # which is the entire reason that class exists.
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda):
            return
        if isinstance(node, ast.Call):
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr in SCHEDULER_ARMING_CALLS
                and isinstance(func.value, ast.Name)
                and func.value.id == SCHEDULER_OBJECT
            ):
                found.append(f"{SCHEDULER_OBJECT}.{func.attr}() at line {node.lineno}")
        for child in ast.iter_child_nodes(node):
            visit(child)

    for statement in ast.parse(source).body:
        visit(statement)
    return found


class TestTheGuardCanActuallyFail:
    """Guard-the-guard. The xfail below is only meaningful if the detector works.

    Without these, a detector that silently matched nothing would make the
    ``xfail(strict=True)`` flip to XPASS and read as "the fix landed" — the exact
    failure mode this file exists to prevent elsewhere.
    """

    def test_it_finds_a_module_level_call(self):
        assert _module_level_scheduler_calls("import x\nscheduler_integration.start()\n"), (
            "the detector missed a bare module-level start()"
        )

    def test_it_finds_one_nested_in_a_module_level_with_block(self):
        """The real file's ``sync_all`` sits inside a module-level ``with``."""
        source = "with get_sync_session() as s:\n    scheduler_integration.sync_all(s)\n"
        assert _module_level_scheduler_calls(source), (
            "the detector only looked at bare expression statements; the real "
            "sync_all() is nested inside a module-level `with`"
        )

    def test_it_ignores_a_call_inside_a_function(self):
        """A call behind a function or a guard is what every candidate fix looks
        like. Flagging it would make this test oppose the fix it is asking for."""
        source = "def boot():\n    scheduler_integration.start()\n"
        assert not _module_level_scheduler_calls(source), (
            "a call inside a function is not an import-time start; flagging it "
            "would fail every correct fix"
        )

    def test_it_ignores_an_unrelated_start(self):
        source = "celery_app.start()\n"
        assert not _module_level_scheduler_calls(source)


class TestTheSchedulerIsNotArmedByImport:
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "core#648: datanika.py starts APScheduler at import, so every granian "
            "worker gets one against a lock-free shared jobstore "
            "(GRANIAN_WORKERS + 1 instances). Remove this marker with the fix."
        ),
    )
    def test_datanika_py_does_not_arm_the_scheduler_at_import(self):
        offenders = _module_level_scheduler_calls(APP_MODULE.read_text(encoding="utf-8"))
        assert not offenders, (
            "importing datanika.datanika starts a scheduler: "
            + "; ".join(offenders)
            + ". Every process that imports the app gets its own, against one "
            "jobstore that does no locking. Move the start behind the process "
            "that owns it — see core#648."
        )

    @pytest.mark.xfail(
        strict=True,
        reason="core#648: the web tier still mutates the in-process scheduler it owns",
    )
    def test_the_web_tier_does_not_mutate_a_scheduler_it_owns(self):
        """The other half of the same coupling, and the half that makes this
        Engineering work rather than a compose change.

        ``ScheduleService`` calls ``sync_schedule`` / ``remove_schedule`` on a
        scheduler living in the web process. Once the scheduler is a separate
        deployment unit those calls have nowhere to land: ``add_job()`` on a
        scheduler that was never started does **not** write the jobstore, it
        queues into ``_pending_jobs`` and flushes on ``start()`` — so the UI
        would report the schedule saved and no job row would ever be written.
        A silent no-op, which is why this is asserted rather than assumed.
        """
        from datanika.services import schedule_service

        source = inspect.getsource(schedule_service)
        mutations = [
            call for call in ("sync_schedule", "remove_schedule") if f"_scheduler.{call}(" in source
        ]
        assert not mutations, (
            f"ScheduleService still drives the in-process scheduler ({', '.join(mutations)}). "
            "The `schedules` table is already the source of truth — sync_all() "
            "rebuilds the whole job set from it — so the web tier should write the "
            "row and let the scheduler process reconcile. See core#648 §5."
        )
