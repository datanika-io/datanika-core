"""core#657 AC4 — a cancelled run must not be billed.

The gate was never missing. It was being fed a literal.
------------------------------------------------------
`datanika-cloud`'s `billing/meter.py` has gated on status since cloud#84:

    def _is_billable(status: str) -> bool:
        return status == "success"

and all four handlers call it — `handle_model_runs`, `handle_upload_runs`,
`handle_transformation_run`, `handle_bytes_processed`. Its docstring anticipated this exact
case in its own words: *"Deliberately `!= "success"` rather than `== "failed"`: **`cancelled`
is not billable either**, and a status this code has never seen should not be charged for by
default."*

It also names the assumption it could not enforce, from the other repository:

    That worked only because of a property of *core*, in another repository: the
    `run.*_completed` events are announced solely from success paths. Nothing here
    enforced or recorded that assumption.

**That assumption is false, and core#657 is why.** A run can be CANCELLED while the worker
runs on — nothing worker-side asks — and the worker then reaches its ordinary success path and
announces `status="success"`, a **hardcoded literal**, at all three sites
(`upload_tasks.py:394`, `pipeline_tasks.py:375`, `transformation_tasks.py:206`).

So the user cancels, cloud's correct gate is handed the string `"success"`, and we charge them.

What this file asserts
----------------------
1. The status in the announced payload is **read from the run**, so a cancelled run announces
   `"cancelled"` — which is precisely the value cloud's gate already rejects.
2. **No announce site may hardcode it again.** Three literals is how a fourth arrives; the
   static guard is the part that outlives this fix.

⚠️ The ledger assertion itself lives in cloud, and that is deliberate
--------------------------------------------------------------------
`UsageLedger` is a cloud model and core cannot import it, so "no row was written" is not
assertable here. The honest core-side claim is the one above: **core stops asserting a
falsehood**. The end-to-end ledger test belongs in `datanika-cloud`, driving this path with
the real handlers subscribed, and it is red until this merges — core first, per the standing
merge order.

⚠️ Still not closed by this: **AC1**. The run keeps doing the work. This stops the bill and the
"completed" notification, not the warehouse load.
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import patch

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService

TASKS_DIR = Path(__file__).resolve().parents[2] / "datanika" / "tasks"


@pytest.fixture
def svc():
    return ExecutionService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-not-billed")
    db_session.add(org)
    db_session.flush()
    return org


def _announced_status(svc, session, org_id, run_id) -> str | None:
    with patch("datanika.hooks.announce") as announce:
        svc.announce_completion(
            session,
            org_id,
            run_id,
            "run.upload_completed",
            target_type="upload",
            target_id=1,
        )
    if announce.call_count == 0:
        return None
    return announce.call_args.kwargs["status"]


class TestTheAnnouncedStatusIsReadFromTheRun:
    def test_a_cancelled_run_announces_cancelled(self, svc, db_session, org):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        svc.start_run(db_session, org.id, run.id)
        svc.cancel_run(db_session, org.id, run.id)

        status = _announced_status(svc, db_session, org.id, run.id)
        assert status == RunStatus.CANCELLED.value, (
            f"announced status={status!r} for a cancelled run. cloud's `_is_billable` bills "
            "anything equal to 'success', so this string is what decides whether a user who "
            "cancelled gets charged (core#657 AC4)."
        )

    def test_an_ordinary_run_still_announces_success(self, svc, db_session, org):
        """The negative control. Without it, always announcing 'cancelled' passes."""
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 2)
        svc.start_run(db_session, org.id, run.id)
        svc.complete_run(db_session, org.id, run.id, rows_loaded=1, logs="ok")

        assert _announced_status(svc, db_session, org.id, run.id) == RunStatus.SUCCESS.value

    def test_a_failed_run_announces_failed(self, svc, db_session, org):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 3)
        svc.start_run(db_session, org.id, run.id)
        with patch("datanika.hooks.announce"):
            svc.fail_run(db_session, org.id, run.id, error_message="boom", logs="t")

        assert _announced_status(svc, db_session, org.id, run.id) == RunStatus.FAILED.value

    def test_a_run_from_another_org_announces_nothing(self, svc, db_session, org):
        """The tenancy predicate still applies — this must not become a way around it."""
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 4)
        other = Organization(name="OtherCo", slug="other-not-billed")
        db_session.add(other)
        db_session.flush()

        assert _announced_status(svc, db_session, other.id, run.id) is None

    def test_the_payload_is_passed_through(self, svc, db_session, org):
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 5)
        with patch("datanika.hooks.announce") as announce:
            svc.announce_completion(
                db_session,
                org.id,
                run.id,
                "run.upload_completed",
                target_type="upload",
                target_id=7,
                table_count=3,
                bytes_processed=99,
            )
        kwargs = announce.call_args.kwargs
        assert kwargs["table_count"] == 3
        assert kwargs["bytes_processed"] == 99
        assert kwargs["run_id"] == run.id
        assert announce.call_args.args[0] == "run.upload_completed"


# --------------------------------------------------------------------------------------
# The static guard. This is the half that outlives the fix.
# --------------------------------------------------------------------------------------


def _completion_announces(tree: ast.AST) -> list[ast.Call]:
    """Every `announce("run.<x>_completed", ...)` call in a module."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if name != "announce" or not node.args:
            continue
        first = node.args[0]
        if (
            isinstance(first, ast.Constant)
            and isinstance(first.value, str)
            and first.value.startswith("run.")
            and first.value.endswith("_completed")
        ):
            found.append(node)
    return found


def _hardcoded_status(call: ast.Call) -> str | None:
    for kw in call.keywords:
        if kw.arg == "status" and isinstance(kw.value, ast.Constant):
            return kw.value.value
    return None


def _task_modules() -> list[Path]:
    return sorted(p for p in TASKS_DIR.glob("*.py") if p.name != "__init__.py")


def test_the_scanner_finds_the_real_announce_sites():
    """Floor (§25). If this found nothing, the guard below asserts nothing.

    Note it must keep finding them *after* the fix: the sites still call
    `announce`, via `announce_completion`, so what changes is the `status=`
    kwarg, not the existence of the call.
    """
    total = sum(
        len(_completion_announces(ast.parse(p.read_text("utf-8")))) for p in _task_modules()
    )
    assert total == 0, (
        "expected the run.*_completed announces to have moved behind "
        "ExecutionService.announce_completion, leaving none inline in datanika/tasks/"
    )


def test_no_task_hardcodes_the_billing_status():
    """The literal that caused core#657 AC4 may not come back.

    `status` is the single field cloud's `_is_billable` reads. A constant there asserts
    something about the run that the code did not check — and it is billing.
    """
    offenders = []
    for path in _task_modules():
        for call in _completion_announces(ast.parse(path.read_text("utf-8"))):
            literal = _hardcoded_status(call)
            if literal is not None:
                offenders.append(f"{path.name}:{call.lineno} status={literal!r}")
    assert not offenders, (
        "a run.*_completed event announces a hardcoded status:\n  "
        + "\n  ".join(offenders)
        + "\n\ncloud's `_is_billable` bills on `status == 'success'`. Announcing a literal "
        "asserts an outcome nothing verified — which is exactly how a cancelled run came to "
        "be billed. Use ExecutionService.announce_completion, which reads it from the run."
    )


class TestTheStaticGuardCanActuallyFail:
    """§43 — these prove the scanner works, not that the tree is clean."""

    _BAD = 'announce("run.upload_completed", session=s, status="success", org_id=1)\n'
    _GOOD = 'announce("run.upload_completed", session=s, status=run_status, org_id=1)\n'
    _UNRELATED = 'announce("run.failed", session=s, status="failed", org_id=1)\n'
    _METHOD = 'svc.announce("run.models_completed", status="success")\n'

    def test_it_finds_a_hardcoded_status(self):
        call = _completion_announces(ast.parse(self._BAD))[0]
        assert _hardcoded_status(call) == "success"

    def test_it_accepts_a_computed_status(self):
        call = _completion_announces(ast.parse(self._GOOD))[0]
        assert _hardcoded_status(call) is None

    def test_it_ignores_a_non_completion_event(self):
        """`run.failed` legitimately carries a literal — it is announced only from fail_run."""
        assert _completion_announces(ast.parse(self._UNRELATED)) == []

    def test_it_sees_an_attribute_call_too(self):
        """A 4th site writing `svc.announce(...)` must not slip past the Name check."""
        calls = _completion_announces(ast.parse(self._METHOD))
        assert len(calls) == 1
        assert _hardcoded_status(calls[0]) == "success"

    def test_it_ignores_a_call_with_no_arguments(self):
        assert _completion_announces(ast.parse("announce()\n")) == []

    def test_the_module_list_is_not_empty(self):
        assert len(_task_modules()) >= 3
