"""A gating E2E failure must preserve the SERVER's view, not only the browser's (core#1528).

core#1296 is closed as **un-diagnosable**. Its failure had already localised itself — the
assertion text reads *"This is the APP, not the worker."* — and eight days later nothing
survived that could say which app path it was:

    playwright-report   196 KB   expires 2026-09-29   retention-days: 14
    test-results       6.08 MB   EXPIRED 2026-09-22   retention-days:  7

🔑 **The artifact that says WHY died in half the time of the one that says THAT, and the issue
the alert filed never expires.** Two of that issue's three alert comments are now links to
expired artifacts. Stated generally, because it is the reusable part: *an alert that outlives
its evidence leaves a permanent record of an unanswerable question.*

And the app's own output was captured **nowhere**: the only `docker logs` in `staging.yml` is
`deploy-staging`'s health wait, on a container that never became healthy. A Reflex event-handler
exception prints `[Reflex Backend Exception]` and a full traceback at INFO, so that line existed
on staging and nothing read it. Staging is recreated on the next push to `dev`; the window
closes in minutes.

This file asserts the wiring. The capture MECHANISM — that `docker logs --since` returns the
window and refuses an empty one — is proven against a real container, because a workflow step
cannot be exercised from a pull request: `e2e-staging` is `push` + `refs/heads/dev` only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
STAGING = REPO_ROOT / ".github" / "workflows" / "staging.yml"
CAPTURE = REPO_ROOT / "scripts" / "capture-staging-logs.sh"

DOC: dict[str, Any] = yaml.safe_load(STAGING.read_text(encoding="utf-8")) or {}
E2E_STEPS: list[dict[str, Any]] = list(DOC["jobs"]["e2e-staging"]["steps"])


def _step(pred) -> dict[str, Any] | None:
    for s in E2E_STEPS:
        if pred(s):
            return s
    return None


def _index(pred) -> int:
    for i, s in enumerate(E2E_STEPS):
        if pred(s):
            return i
    return -1


def _uploads() -> dict[str, dict[str, Any]]:
    """`actions/upload-artifact` steps, keyed by the artifact NAME they publish."""
    out = {}
    for s in E2E_STEPS:
        if str(s.get("uses", "")).startswith("actions/upload-artifact"):
            with_ = s.get("with") or {}
            out[str(with_.get("name"))] = with_
    return out


_is_gating = lambda s: s.get("id") == "gating"  # noqa: E731
_is_mark = lambda s: s.get("id") == "logmark"  # noqa: E731
_is_capture = lambda s: s.get("id") == "applogs"  # noqa: E731


# ── controls first ───────────────────────────────────────────────────────────────────────


def test_control_the_workflow_parses_and_the_job_has_its_landmarks() -> None:
    """Every assertion below reads `E2E_STEPS`. If that parses to nothing they all pass."""
    assert len(E2E_STEPS) >= 10, f"e2e-staging parsed to {len(E2E_STEPS)} steps"
    assert _step(_is_gating), "no `id: gating` step — the parser, not the workflow"
    assert _uploads(), "no upload-artifact step parsed out of e2e-staging"


def test_control_the_step_finder_can_return_nothing() -> None:
    assert _step(lambda s: s.get("id") == "not-a-step-in-this-job") is None


# ── the capture ──────────────────────────────────────────────────────────────────────────


def test_the_window_is_marked_before_the_specs_run() -> None:
    """`--since` needs an opening timestamp, and it has to predate the thing it bounds.

    ⚠️ Order, not mere presence. A marker recorded *after* the gating step bounds a window that
    has already closed, and `docker logs --since <a future instant>` returns **nothing** — an
    empty capture that looks exactly like a silent container.
    """
    mark, gate = _index(_is_mark), _index(_is_gating)
    assert mark >= 0, "no `id: logmark` step; the capture has no window to bound"
    assert mark < gate, (
        f"the log window is marked at step {mark} and the specs run at step {gate}. A marker "
        "taken after the specs bounds a window that is already over."
    )


def test_the_capture_fires_on_a_gating_failure_and_on_a_flaky_run() -> None:
    """core#873's lesson, applied to a new step rather than rediscovered.

    The filer and the pager in this job used to key on job-level `failure()`, so ANY failing
    step asserted a gating E2E failure — and an artifact-service timeout once paged Telegram
    and filed an issue for a run whose every spec passed.

    Asserted as the PRESENCE of the two outcome references, never as the absence of
    `failure()`: a condition can be wrong in many ways and only one of them is spelled that way.
    """
    step = _step(_is_capture)
    assert step, "no `id: applogs` step: a gating failure still preserves nothing of the server"
    cond = str(step.get("if", ""))
    assert "always()" in cond, (
        f"the capture's `if:` is {cond!r}. Without `always()` the step is skipped whenever an "
        "earlier step failed — which is the only time it is wanted."
    )
    assert "steps.gating.outcome" in cond, "the capture does not key on the gating step's outcome"
    assert "steps.flaky_gate.outputs.status" in cond, (
        "the capture does not fire on a flaky run. A flaky gating spec is a GREEN run that "
        "files a durable issue (core#757); filing one that points at no evidence is core#1296."
    )


def test_the_capture_runs_a_script_that_exists_and_is_bounded() -> None:
    """A recipe belongs where it can be tested, not inline in YAML that nothing runs."""
    step = _step(_is_capture)
    assert "capture-staging-logs.sh" in str(step.get("run", "")), (
        "the capture step does not invoke scripts/capture-staging-logs.sh"
    )
    assert CAPTURE.exists(), f"{CAPTURE} is missing; the step invokes a script that is not there"
    body = CAPTURE.read_text(encoding="utf-8")

    # 🚨 Anchored to the COMMAND, and comments stripped first. `"--since" in body` was the first
    # spelling of this assertion and a mutation caught it: dropping `--since` from the real
    # `docker logs` call left the guard GREEN, because the script's own REFUSAL MESSAGE —
    # "--since did not bound this capture" — contains the flag. The corrected artifact carries
    # the wrong text in order to explain it, so a raw-text search counts its own error message.
    # `QA_RULES` §24a, found here by mutating rather than by review.
    commands = [ln for ln in body.splitlines() if not ln.lstrip().startswith("#")]
    logs_calls = [ln for ln in commands if "docker logs" in ln]
    assert logs_calls, "no `docker logs` invocation in the capture script at all"
    unbounded = [ln.strip() for ln in logs_calls if "--since" not in ln]
    assert not unbounded, (
        f"a `docker logs` call carries no --since: {unbounded}. An unbounded dump of a "
        "container that has served a full dev day is not evidence, it is a haystack."
    )

    assert any("active-app.sh" in ln for ln in commands), (
        "the app container is not named the way the deploy names it. A hardcoded colour "
        "captures nothing for as long as the other colour serves (core#622)."
    )


def test_control_the_comment_stripper_is_narrow() -> None:
    """The repair for a false positive is to widen the stripper until it matches nothing.

    Both halves in one test, per `QA_RULES` §24a: a real command still survives stripping, and a
    line that merely *mentions* the flag does not count as carrying it.
    """
    lines = [
        "# docker logs -t --since '$SINCE' explains what this does",
        '  echo "--since did not bound this capture"',
        "  remote \"docker logs -t --since '$SINCE' '$cid' 2>&1\" > \"$dest\"",
    ]
    kept = [ln for ln in lines if not ln.lstrip().startswith("#")]
    assert len(kept) == 2, "the stripper dropped a real command line"
    calls = [ln for ln in kept if "docker logs" in ln]
    assert len(calls) == 1, (
        "the echoed refusal message was counted as a `docker logs` call, or the real call was "
        "missed — either way the assertion above measures the wrong lines"
    )


def test_the_captured_logs_are_uploaded_and_outlive_the_report() -> None:
    step = _step(lambda s: (s.get("with") or {}).get("name") == "staging-logs")
    assert step, "the captured logs are not uploaded; they die with the runner"
    # Positive form, not a ban on `ignore`: `warn` (the default, reached by deleting the key)
    # is equally silent, so an assertion that the wrong word is absent is satisfied by the
    # wrong state — `WORKFLOW_RULES` §4.
    assert str((step.get("with") or {}).get("if-no-files-found")) == "error", (
        "the capture upload must fail on an empty directory. `ignore` — and the `warn` default "
        "you get by deleting the key — is how this becomes a step that always succeeds and "
        "never records anything (core#1528 AC4)."
    )


def test_the_retention_inversion_does_not_come_back() -> None:
    """core#1528 AC2, as an invariant rather than as two numbers.

    `test-results` carries the traces, screenshots and videos — the workflow's own comment calls
    it the fallback for when the HTML report cannot render. It expired in **half** the time of
    the thing it backs up, and the issue the alert filed has no expiry at all.

    🔑 The property is *"nothing that says WHY may die before the thing that says THAT"*, so it
    is written as a comparison. Pinning `14` twice would pass a change that lowered both.
    """
    uploads = _uploads()
    for name in ("playwright-report", "test-results", "staging-logs"):
        assert name in uploads, f"e2e-staging no longer uploads {name!r}"

    report = int(uploads["playwright-report"]["retention-days"])
    for name in ("test-results", "staging-logs"):
        got = int(uploads[name]["retention-days"])
        assert got >= report, (
            f"{name} keeps {got} days and playwright-report keeps {report}. The artifact that "
            "says WHY must not die before the one that says THAT — core#1296's traces expired "
            "on 2026-09-22 while its report lives to 09-29, and the issue they explain is "
            "permanent."
        )
