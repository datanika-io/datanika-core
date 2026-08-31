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

⚠️ That last property was **not true when this file was first written**, and the
way it was untrue is worth keeping
-----------------------------------------------------------------------------

The detector pruned ``def``/``class``/``lambda`` but walked into ``ast.If``, so a
call behind a module-level role guard was flagged as an import-time start. The
docstring on ``_module_level_scheduler_calls`` said the opposite — *"the same call
inside a function or an ``if`` guard is fine"* — and ``TestTheGuardCanActuallyFail``
had a case for every shape **except** the one they disagreed about.

Measured on the two shapes, old detector against new:

=================  ==========  ==========================================
``datanika.py``    detector    strict-xfail outcome
=================  ==========  ==========================================
defect (today)     old         XFAIL — suite green, reads "still broken"
defect (today)     new         XFAIL — suite green, reads "still broken"
env-guarded fix    old         **XFAIL — suite green, reads "still broken"**
env-guarded fix    new         XPASS → FAILURE, marker must be removed
=================  ==========  ==========================================

The old detector returns the **same** verdict for the defect and for the fix. So
if Engineering had shipped core#648's opt-in-flag fix — one of the three
candidates named in the decision, and the one the docstring pointed at — this
file would have gone on xfailing, the suite would have stayed green, and the only
signal that the fix had landed would have been absent. The marker outlives the
defect and reads exactly like the defect.

Generalising, because this is the third instance in this repo: **a guard whose
prose and whose code disagree will be trusted according to its prose and behave
according to its code**, and a guard-the-guard suite only closes that gap for the
cases it enumerates. Enumerate the shapes the fix is expected to take, not just
the shapes the bug takes.

⚠️ Both strict xfails below are also satisfied by DELETING the scheduler
--------------------------------------------------------------------------

Remove line 83 and ``ScheduleService``'s two calls and both flip to XPASS —
reading as *"the fix landed"* — with nothing anywhere arming a scheduler. 5× is
loud (duplicate ``runs`` rows, duplicated billing); 0× is silent and is found by
the customer, because nothing alerts on a due ``Schedule`` that was never
dispatched. ``TestTheSchedulerStartMustMoveNotVanish`` is that half, and it is a
plain test rather than an xfail because it is green today, green after a correct
fix, and red only if the arming disappears.

Measured on the real tree rather than argued
(``plans/qa/notes/probe-648/mutate_648_guards.py``):

=========================================  =========  =============================
state applied to ``datanika/datanika.py``  claim C    pytest
=========================================  =========  =============================
baseline (the defect, on ``dev``)          GREEN      17 passed, 2 xfailed
arming MOVED to a run module + service     GREEN      2 failed (markers due out)
arming DELETED outright                    **RED**    3 failed
=========================================  =========  =============================

The two mutated states are otherwise indistinguishable in this file — same
xfail flips, same counts elsewhere. Claim C is the whole difference.
"""

import ast
import inspect
from pathlib import Path

import pytest
import yaml

APP_MODULE = Path(__file__).resolve().parents[2] / "datanika" / "datanika.py"

#: Calls that arm the shared scheduler. ``sync_all`` is included because it is
#: the other half of the import-time block and writes the jobstore.
SCHEDULER_ARMING_CALLS = {"start", "sync_all"}
SCHEDULER_OBJECT = "scheduler_integration"


def _module_level_scheduler_calls(source: str) -> list[str]:
    """Calls like ``scheduler_integration.start()`` that run on a plain import.

    What this accepts and rejects, stated as a table because the prose version
    of it drifted from the code once already (see the module docstring):

    ==========================================  ==========
    shape                                       verdict
    ==========================================  ==========
    ``scheduler_integration.start()``           **flagged**
    inside a module-level ``with``              **flagged**
    inside ``try:`` / ``except:``               **flagged**
    inside ``def`` / ``async def`` / ``class``  accepted
    inside ``if`` / ``else`` at module scope    accepted
    ``if __name__ == "__main__":``              accepted
    passed as a reference, not called           accepted
    ==========================================  ==========

    The rule is *conditional*, not *indented*. A ``try`` block still calls the
    thing on every import — swallowing the exception changes what happens when
    arming fails, not whether it is attempted. An ``if`` does not: a web process
    that imports the app with ``DATANIKA_ROLE`` unset arms nothing.

    The ``if`` carve-out is deliberately coarse — ``if True:`` would pass. This
    is a tripwire for the *shape* of the defect, not a proof of single
    ownership; the behavioural half of core#648 is what proves that, and it
    cannot be a manifest or AST assertion (see the module docstring).
    """
    found: list[str] = []

    def visit(node: ast.AST) -> None:
        # Prune definitions. `ast.walk` descends into them, which would flag a
        # call inside `def boot(): scheduler_integration.start()` — i.e. it would
        # fail every correct fix. Caught by TestTheGuardCanActuallyFail below,
        # which is the entire reason that class exists.
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda):
            return
        # Prune both branches of a module-level conditional, for the same
        # reason — a role guard, a settings flag and `if __name__` are three of
        # the candidate fixes on core#648, and flagging them would hold the
        # strict xfail below red *after* the fix shipped. The condition itself
        # is still visited: `if scheduler_integration.start():` is not a guard.
        if isinstance(node, ast.If):
            visit(node.test)
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

    # ------------------------------------------------------------------
    # The `if`-guard cases. These were missing, and their absence is what
    # let the detector and this file's own docstring disagree for the whole
    # life of the marker below — see the module docstring's "what this
    # accepts" table. A guarded start is one of the three candidate fixes
    # named on core#648, so rejecting it would leave the strict xfail
    # xfailing *after* the fix shipped: green suite, live defect, and no
    # signal that the marker was due for removal.
    # ------------------------------------------------------------------

    def test_it_ignores_a_call_behind_a_module_level_env_guard(self):
        """`DATANIKA_ROLE=scheduler` — the opt-in-flag fix from core#648 §3."""
        source = (
            "import os\n"
            "if os.environ.get('DATANIKA_ROLE') == 'scheduler':\n"
            "    scheduler_integration.start()\n"
        )
        assert not _module_level_scheduler_calls(source), (
            "a start behind a role guard does not arm a scheduler when a web "
            "process imports the app — flagging it would keep this file's "
            "strict xfail red after the fix landed"
        )

    def test_it_ignores_a_call_behind_a_settings_flag_including_its_with_block(self):
        source = (
            "if settings.run_scheduler:\n"
            "    scheduler_integration.start()\n"
            "    with get_sync_session() as s:\n"
            "        scheduler_integration.sync_all(s)\n"
        )
        assert not _module_level_scheduler_calls(source), (
            "the guard must cover the whole conditional block, not just its first statement"
        )

    def test_it_ignores_a_dunder_main_entrypoint(self):
        source = "if __name__ == '__main__':\n    scheduler_integration.start()\n"
        assert not _module_level_scheduler_calls(source), (
            "an `if __name__` block never runs on import, which is the entire property under test"
        )

    def test_it_ignores_a_call_in_an_else_branch(self):
        source = "if settings.web_tier:\n    pass\nelse:\n    scheduler_integration.start()\n"
        assert not _module_level_scheduler_calls(source), (
            "`orelse` is as conditional as `body`; missing it would flag a "
            "correct fix written the other way round"
        )

    def test_it_still_finds_a_call_wrapped_only_in_try_except(self):
        """`try` is not a guard — the call still runs on every import.

        This is the boundary of the rule above and the reason it is stated as
        *conditional*, not as *indented*. Swallowing the exception changes what
        happens when arming fails; it does not stop the arming.
        """
        source = "try:\n    scheduler_integration.start()\nexcept RuntimeError:\n    pass\n"
        assert _module_level_scheduler_calls(source), (
            "a try/except around the call is not a role guard — it arms a "
            "scheduler in every process that imports the app"
        )


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


# ---------------------------------------------------------------------------
# The other failure direction, which nothing above can see.
#
# Both tests above are satisfied by DELETING the scheduler. Remove line 83 and
# `ScheduleService`'s two calls and they flip to XPASS — "the fix landed" — with
# nothing anywhere arming a scheduler. The feature then fails **silently and
# completely**: a customer saves a schedule, the row is written, the UI shows it
# active, and it never runs. There is no alert for that. `celery-maintenance-not-
# firing` watches beat; nothing watches whether a due `Schedule` was dispatched.
#
# 5× is loud — it shows up as duplicate `runs` rows and duplicated billing. 0× is
# silent, and is discovered by the customer. So the fix has to MOVE the arming,
# and this is the half that says so.
#
# These are NOT xfails: they are green today, green after a correct fix, and red
# only if the arming disappears or lands somewhere nothing runs. They therefore
# encode no architecture — a dedicated container, a leader lock and an opt-in
# flag all satisfy them, which is the same property the xfails above were shaped
# for and the reason this is not written as a process-count assertion. See the
# note at the bottom of this file for why that one is deliberately not here.
# ---------------------------------------------------------------------------

DATANIKA_PACKAGE = Path(__file__).resolve().parents[2] / "datanika"
COMPOSE_MANIFESTS = {
    "docker-compose.yml": Path(__file__).resolve().parents[2] / "docker-compose.yml",
    "deploy/staging/docker-compose.yml": Path(__file__).resolve().parents[2]
    / "deploy"
    / "staging"
    / "docker-compose.yml",
}


def _any_scheduler_arming_call(source: str) -> bool:
    """``scheduler_integration.start()`` **anywhere** — inside a function, behind
    a guard, at module scope.

    Deliberately the complement of ``_module_level_scheduler_calls``: that one
    asks *"does importing this arm a scheduler"*, this one asks *"does this file
    contain the arming at all"*. Both questions are needed, and answering the
    second with the first is how "the fix landed" and "the feature was deleted"
    become the same reading.
    """
    for node in ast.walk(ast.parse(source)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "start"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == SCHEDULER_OBJECT
        ):
            return True
    return False


def _modules_that_arm_the_scheduler() -> list[Path]:
    return sorted(
        path
        for path in DATANIKA_PACKAGE.rglob("*.py")
        if "__pycache__" not in path.parts
        and _any_scheduler_arming_call(path.read_text(encoding="utf-8"))
    )


def _dotted(path: Path) -> str:
    rel = path.relative_to(DATANIKA_PACKAGE.parent).with_suffix("")
    return ".".join(rel.parts)


def _all_compose_commands() -> dict[str, str]:
    """``{"<manifest>:<service>": "<command text>"}`` across every manifest."""
    commands: dict[str, str] = {}
    for label, manifest_path in COMPOSE_MANIFESTS.items():
        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
        for name, service in (manifest.get("services") or {}).items():
            cmd = (service or {}).get("command", "")
            text = " ".join(str(p) for p in cmd) if isinstance(cmd, list) else str(cmd)
            commands[f"{label}:{name}"] = text
    return commands


class TestTheSchedulerStartMustMoveNotVanish:
    def test_the_manifests_are_readable(self):
        """Arming check. Every assertion below is a search over these commands,
        and a search over an empty set is satisfied by anything."""
        commands = _all_compose_commands()
        assert len(commands) >= 5, (
            f"only parsed {len(commands)} compose services across "
            f"{sorted(COMPOSE_MANIFESTS)}; the reachability check below would be "
            "vacuous"
        )

    def test_something_in_the_tree_still_arms_the_scheduler(self):
        armers = _modules_that_arm_the_scheduler()
        assert armers, (
            "nothing under datanika/ calls scheduler_integration.start(). The two "
            "xfail guards above are both SATISFIED by this state, so a fix that "
            "deleted the arming instead of moving it reads as complete. Every "
            "saved Schedule would then be written, displayed as active, and never "
            "dispatched — and no alert covers that. See core#648."
        )

    def test_every_arming_module_is_reachable_from_a_deployment_unit(self):
        """A ``start()`` in a module nothing runs is the same as no ``start()``.

        Reachable means: it is the app module itself (imported by every service
        whose command runs ``reflex run``), or its dotted path appears in some
        compose service's command. That second clause is what a dedicated
        scheduler container satisfies, and it is why this test does not have to
        know which architecture was chosen.
        """
        commands = _all_compose_commands()
        unreachable: list[str] = []
        for module in _modules_that_arm_the_scheduler():
            if module == APP_MODULE:
                continue  # every `reflex run` service imports it
            dotted = _dotted(module)
            if not any(dotted in command for command in commands.values()):
                unreachable.append(dotted)
        assert not unreachable, (
            "these modules arm the scheduler but no compose service runs them, so "
            "no scheduled work is dispatched anywhere: " + ", ".join(unreachable) + ". "
            "Add the service that runs it (core#648 §3 chose a dedicated "
            "dispatch-only container), or the arming is dead code that reads as a "
            "shipped fix."
        )


class TestTheMoveNotVanishGuardCanActuallyFail:
    def test_it_finds_a_call_inside_a_function(self):
        """The discriminating case against ``_module_level_scheduler_calls``,
        which returns nothing for exactly this source."""
        source = "def main():\n    scheduler_integration.start()\n"
        assert _any_scheduler_arming_call(source)
        assert not _module_level_scheduler_calls(source)

    def test_it_finds_a_call_behind_a_role_guard(self):
        source = "if settings.run_scheduler:\n    scheduler_integration.start()\n"
        assert _any_scheduler_arming_call(source)
        assert not _module_level_scheduler_calls(source)

    def test_it_does_not_find_an_unrelated_start(self):
        assert not _any_scheduler_arming_call("celery_app.start()\napp.start()\n")

    def test_it_does_not_find_a_file_with_no_call(self):
        assert not _any_scheduler_arming_call("import os\n\n\ndef f():\n    return 1\n")

    def test_the_real_tree_currently_arms_it_in_exactly_one_module(self):
        """Not an invariant — a statement of where things stand.

        🔧 **The core#648 fix is expected to fail this and to update the
        expected value in the same commit.** That is the point: the move is a
        one-line edit here, and making it deliberate stops the arming quietly
        appearing in a *second* module, which is this bug all over again one
        level up. Do not delete the assertion to make it pass.
        """
        assert [_dotted(p) for p in _modules_that_arm_the_scheduler()] == ["datanika.datanika"], (
            "the set of modules that arm the scheduler changed. If a fix moved "
            "it, update the expected list. If it is now armed in two places, "
            "that is core#648 again."
        )


# ---------------------------------------------------------------------------
# ⚠️ Deliberately NOT in this file: "exactly one process arms the scheduler".
#
# That is the invariant the defect actually violates — 5 processes, not 5
# services — and it is derivable from the manifests, since `instances =
# GRANIAN_WORKERS + 1` was measured at two worker counts (prod 4 → 5, staging
# 2 → 3). It is left out on purpose, because writing it now requires guessing
# the shape of a fix that has not been written:
#
#   * dedicated container  — arming service runs 1 process   → asserts 1 ✅
#   * opt-in flag on `app` — arming service runs 5 processes → asserts 5 ❌
#   * Redis leader lock    — 5 processes call start(), one wins → the manifest
#                            cannot tell, and the assertion is unwritable ❌
#
# Two of the three make a *correct* fix fail this test. A guard that opposes two
# of the three candidate fixes is not coverage, it is a vote — and this file has
# already shipped one detector whose prose and code disagreed about precisely
# this (see the module docstring). The process-count assertion belongs in the PR
# that implements the fix, written against the shape that was chosen. The spec
# for it is on core#648.
# ---------------------------------------------------------------------------
