"""The `scripts/` type-check gate must stay a gate (core#1288).

`_gh_log_or_none` lives in `scripts/e2e_tier_streak.py` and has a consumer in
`scripts/verify_e2e_attribution.py`, a different department's module. Its contract broke that
consumer **twice**, and a human running a promotion found it both times:

    core#1205   it began RAISING on a two-tier log     -> AmbiguousVerdictError at runtime
    core#1273   it began returning `(log, reason)`     -> "'tuple' object has no attribute
                                                          'splitlines'"

🚨 **This file asserts the gate's SHAPE, and the gate asserts the code's shape. Neither sees
behaviour.** core#1205 changed no signature; a type checker is blind to it and always will be.
That is the boundary of the claim, not a gap to close later, which is why
:func:`test_the_gate_says_out_loud_what_it_cannot_see` treats the disclaimer as part of the
gate rather than as commentary (core#1288 AC3).

**What is deliberately NOT asserted here.** That mypy catches the core#1273 shape. That is a
statement about mypy, and it is proven the only way this project accepts — by a forced red on
the real runner: `scripts/_mypy_arming_control.py` was added in this PR's first commit, CI's
`lint` job went red on it at `[attr-defined]`, and the next commit deleted it and the same job
went green. A pytest that shells out to mypy would have to **skip** wherever mypy is absent —
the `test` job installs from `uv.lock`, which does not carry it — and `QA_RULES` §11 is
explicit that a suite must not get quieter on a missing dependency.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"

CI_TEXT = CI.read_text(encoding="utf-8")
CI_DOC: dict[str, Any] = yaml.safe_load(CI_TEXT) or {}
MYPY_CFG: dict[str, Any] = (
    tomllib.loads(PYPROJECT.read_text(encoding="utf-8")).get("tool", {}).get("mypy", {})
)

#: Keys a step may carry and still be a gate. Written as an ALLOWLIST, not as a ban on
#: `continue-on-error`: banning one key is satisfied by `if: false`, by
#: `continue-on-error: ${{ ... }}`, and by anything invented next — and asserting the absence
#: of a wrong word is the trap `WORKFLOW_RULES` §4 and `QA_RULES` §7 both name. A step that is
#: exactly a name and a command cannot be masked by any mechanism, present or future.
GATE_STEP_KEYS = {"name", "run"}


def _lint_steps() -> list[dict[str, Any]]:
    return list(CI_DOC["jobs"]["lint"]["steps"])


def _installs_mypy(run: str) -> bool:
    """A step that installs mypy, pinned or not — so an UNPINNED one is still found.

    Matching on `mypy==` alone would make the pin assertion vacuous: drop the `==` and the
    step stops being found, the list goes empty, and the only thing that fires is the
    *"nothing installs mypy"* message — which names the wrong defect.
    """
    return "install" in run and "mypy" in run


def _steps_running(prefix: str) -> list[dict[str, Any]]:
    """Steps in `lint` whose command starts with `prefix` (first non-blank line)."""
    out = []
    for step in _lint_steps():
        run = str(step.get("run", "")).strip()
        first = run.splitlines()[0].strip() if run else ""
        if first == prefix or first.startswith(prefix + " "):
            out.append(step)
    return out


# ── controls first: a parser that finds nothing makes every assertion below vacuous ──────


def test_control_the_workflow_parses_and_lint_has_steps() -> None:
    """`QA_RULES` §31: report the population beside the verdict.

    If `jobs.lint.steps` ever stops parsing, every assertion in this file passes by looking
    at an empty list — which is the exact defect class the gate itself exists to catch.
    """
    assert "lint" in CI_DOC.get("jobs", {}), "ci.yml parsed, but it has no `lint` job"
    steps = _lint_steps()
    assert len(steps) >= 5, f"`lint` parsed to {len(steps)} steps; the parser is broken"
    assert _steps_running("ruff"), (
        "no `ruff` step found in `lint`. This control exists so that a mypy step being "
        "'found' is a reading rather than a property of a matcher that matches anything."
    )


def test_control_the_step_matcher_can_return_nothing() -> None:
    """A matcher that answers 'yes' to everything cannot report a missing gate."""
    assert _steps_running("this-tool-does-not-exist") == [], (
        "the step matcher returned steps for a command that is not in the workflow"
    )


# ── the gate ─────────────────────────────────────────────────────────────────────────────


def test_mypy_runs_inside_the_required_lint_job() -> None:
    """A step, not a new job — and `lint` is the one that is already required on `dev`.

    A separate `typecheck` job would need an **additive** branch-protection `contexts` POST
    *and* a verified `merge_group` run before being made required: this workflow's
    `check_response_timeout_minutes` is 30, and a required check that produces no run on
    `merge_group` ejects the entry after that long **with nothing red**. A step inside `lint`
    inherits `pull_request`, `push` and `merge_group` for free.
    """
    assert _steps_running("mypy"), (
        "no step in `lint` runs mypy. The three programs that decide whether a promotion may "
        "proceed live in `scripts/`, and a cross-module contract change has crashed the "
        "pre-flight twice (core#1205, core#1273)."
    )


def test_the_mypy_step_declares_no_file_set_of_its_own() -> None:
    """core#1288 AC4, stated as a property rather than as a comparison.

    The issue asks that the gates see the same file set. Two gates that agree is something
    somebody has to keep true; **one declaration is true by construction.** So the command
    takes no path argument and the scope lives in `pyproject.toml`, where a local `mypy` and
    this step read the same line.

    ⚠️ The predecessor of this rule is `ruff`'s `--exclude` flags, which encoded an exemption
    that had already moved into `pyproject.toml` and outlived it by months —
    `test_lint_gates_cover_what_they_name.py` now refuses those. A path argument here is the
    same shape arriving in a new tool.
    """
    for step in _steps_running("mypy"):
        args = str(step["run"]).strip().split()[1:]
        assert not args, (
            f"the mypy step passes {args}. Scope is declared once, in pyproject.toml's "
            "`[tool.mypy] files`, so that this command and a local `mypy` cannot disagree "
            "about what is checked. A path here is a second declaration (AC4)."
        )


def test_the_mypy_version_is_pinned_exactly() -> None:
    """core#1288 AC4: *"the type checker must be pinned"*.

    Exactly, with `==`, and deliberately **not** in `uv.lock`: that file is what the
    production image installs, so a lint-only tool must not be able to move a production pin
    during its own re-resolve. A range would also let the gate's verdict change without a
    commit, which makes a red un-attributable to anything in the diff.
    """
    installs = [s for s in _lint_steps() if _installs_mypy(str(s.get("run", "")))]
    assert installs, "no step installs mypy; the `mypy` command would be whatever is on PATH"
    for step in installs:
        run = str(step["run"])
        assert "mypy==" in run, (
            f"mypy is installed without an exact pin: {run.strip()!r}. A range lets this "
            "gate's verdict change with no commit behind it."
        )


def test_neither_mypy_step_can_be_masked() -> None:
    """A tick that `continue-on-error` has quarantined is not a verdict (`QA_RULES` §1).

    Asserted as an allowlist of keys rather than as a ban on that one key — see
    :data:`GATE_STEP_KEYS` for why the ban form is the weaker assertion.
    """
    for step in _steps_running("mypy") + [
        s for s in _lint_steps() if _installs_mypy(str(s.get("run", "")))
    ]:
        extra = set(step) - GATE_STEP_KEYS
        assert not extra, (
            f"the step {step.get('name')!r} carries {sorted(extra)}. A gate step is exactly a "
            f"name and a command; anything else can decide the job's colour for it."
        )


def test_the_scope_is_declared_in_pyproject_and_excludes_the_app() -> None:
    """The other half of the single declaration — and the boundary the issue drew.

    `datanika/` is 266 files that have never been type-checked, and `tests/` is 514. Bundling
    either sinks the proposal, which is why core#1288 puts them under *Not in scope*. If someone
    widens this to them, it should be a decision with its own issue, not a quiet edit.

    🔴 **Repointed 2026-09-25 (core#1571), WORKFLOW_RULES §5a.** This asserted
    ``files == ["scripts"]`` and went red on the correct change: `.github/scripts` held five
    programs and had never been type-checked, and adding it is the thing #1571 asks for. **The
    literal was not the invariant.** What core#1288 actually decided is a *boundary* — the gate
    covers the tooling directories and stops before the application and its test suite — so that
    is what is asserted now, in both directions:

    * every entry must be a **tooling** directory, so widening to `datanika/` or `tests/` still
      reds and still has to be a decision;
    * the declaration must live here rather than on the command line (AC4 of #1288).

    ⚠️ The forbidden set is named rather than derived on purpose. Deriving *"is this the
    application"* would need a predicate, and a predicate that answers wrongly in the permissive
    direction is how a boundary quietly stops being one — cf. `QA_RULES` §31 rule 4.
    """
    files = MYPY_CFG.get("files")
    assert isinstance(files, list) and files, (
        f"[tool.mypy] files is {files!r}. The scope must be declared here and non-empty: "
        "core#1288 AC4 requires that a bare `mypy` and CI cannot disagree about what is checked."
    )
    forbidden = {"datanika", "tests", "datanika-mcp", "e2e", "."}
    crossed = sorted(f for f in files if f.strip("./") in forbidden)
    assert not crossed, (
        f"[tool.mypy] files names {crossed}, which crosses the boundary core#1288 drew: "
        "`datanika/` is 266 files and `tests/` is 514, none ever type-checked, and bundling "
        "them sinks the gate rather than widening it. That is a decision with its own issue."
    )
    assert "scripts" in files, (
        f"[tool.mypy] files is {files!r} and does not name `scripts`. That directory holds "
        "verify_e2e_attribution.py and e2e_tier_streak.py — the tools that decide whether a "
        "promotion may proceed — and is the original subject of core#1288."
    )
    assert ".github/scripts" in files, (
        f"[tool.mypy] files is {files!r} and does not name `.github/scripts`. core#1571: it "
        "holds cloud_pairing_gate.py, which decides whether a promotion may proceed, and "
        "promotion_refs.py, which decides what it closes. A directory outside this list gets "
        "no `warn_unused_ignores`, so it accumulates suppressions nobody can validate."
    )


def test_unused_ignore_warnings_stay_on() -> None:
    """The setting that makes the suppression list able to shrink by itself.

    Six `# type: ignore[union-attr]` comments in `scripts/` named a code mypy does not emit
    for those lines — it reports `attr-defined` — so they suppressed **nothing** while
    reading exactly like suppressions. Nobody could have known: there was no type checker.
    With this on, an ignore that is unnecessary, or was never right, goes red and has to be
    removed. Turning it off is how a ratchet quietly becomes a snapshot.
    """
    assert MYPY_CFG.get("warn_unused_ignores") is True, (
        "[tool.mypy] warn_unused_ignores must stay true: it is the only thing that can "
        "detect a suppression which suppresses nothing, which is what six of them did."
    )


def test_import_exemptions_are_named_never_blanket() -> None:
    """core#1288 AC2: *"never by a blanket suppression or by lowering strictness"*.

    `--ignore-missing-imports` (how the issue measured its baseline) silences every
    unresolved import in the directory, a real typo included. Measured with no flag at all,
    the only unresolved import under `scripts/` is `yaml` — a declared dependency
    (`pyyaml>=6.0,<7`) that ships no inline types. So the exemption is one module by name.
    """
    overrides = MYPY_CFG.get("overrides", [])
    assert overrides, "no [[tool.mypy.overrides]] block; check nothing became blanket instead"
    for block in overrides:
        modules = block.get("module")
        modules = [modules] if isinstance(modules, str) else list(modules or [])
        assert modules, "an overrides block names no module, so it applies to everything"
        assert "*" not in "".join(modules), (
            f"a mypy override matches a wildcard module ({modules}). Name the modules: a "
            "blanket exemption hides the next unresolved import as effectively as the flag "
            "AC2 refuses."
        )
    for key in ("ignore_errors", "follow_imports"):
        assert key not in MYPY_CFG, (
            f"[tool.mypy] sets `{key}` at top level, which lowers the bar for every file at "
            "once. AC2 asks for per-line reasons instead."
        )


def test_the_gate_says_out_loud_what_it_cannot_see() -> None:
    """core#1288 AC3, and it is the clause most likely to be deleted as commentary.

    A type checker sees **shape**. It does not see **behaviour** — and core#1205 is the proof
    that lives in this repository's own history: a cross-module caller broke with **no
    signature change at all**, so this gate would have been green straight through it.

    The failure this prevents is not a bad type check. It is somebody reading a green `lint`
    and concluding a seam is covered. That reader is at the workflow file or at the config,
    so the sentence has to be at both.

    ⚠️ Asserted as the PRESENCE of the distinction and of the incident that proves it, never
    as the absence of a phrase: a guard written the other way is satisfied by deleting the
    paragraph (`WORKFLOW_RULES` §4).
    """
    mypy_region = CI_TEXT[CI_TEXT.index("Install mypy (pinned)") - 2000 :][:4000]
    pyproject_text = PYPROJECT.read_text(encoding="utf-8")
    start = pyproject_text.index("[tool.mypy]")
    mypy_block = pyproject_text[max(0, start - 3000) : start + 500]

    for label, text in (("ci.yml", mypy_region), ("pyproject.toml", mypy_block)):
        low = text.lower()
        assert "shape" in low and ("behaviour" in low or "behavior" in low), (
            f"{label} configures the type check without saying that it sees shape and not "
            "behaviour. AC3: a green type check is not evidence that a seam is covered, and "
            "the next core#1205 will arrive with this gate green."
        )
        assert "core#1205" in text, (
            f"{label} does not cite core#1205, the incident where a contract broke a caller "
            "with no signature change. Without it the disclaimer reads as boilerplate and "
            "gets trimmed; with it, it is a measurement."
        )


def test_control_the_masking_assertion_can_fail() -> None:
    """`QA_RULES` §2: a green you have not forced red is unproven.

    Drives :data:`GATE_STEP_KEYS` with a step that a real edit would plausibly produce, and
    requires it to be refused. Without this, an allowlist that had silently grown to include
    everything would score identically on the live workflow.
    """
    masked = {"name": "Type check", "run": "mypy", "continue-on-error": True}
    assert set(masked) - GATE_STEP_KEYS == {"continue-on-error"}, (
        "GATE_STEP_KEYS now admits `continue-on-error`, so the masking assertion above "
        "cannot fail and is decorative."
    )
    gated = {"name": "Type check", "run": "mypy"}
    assert not set(gated) - GATE_STEP_KEYS, (
        "a plain name+run step is refused by GATE_STEP_KEYS, so the assertion reds on the "
        "correct shape too — it would have to be loosened until it matched nothing."
    )
