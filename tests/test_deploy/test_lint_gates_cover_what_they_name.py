"""A lint gate must actually lint the paths it names (core#1237).

`scripts/` was linted by **neither** CI nor the pre-push hook — both named `datanika tests` —
and had accumulated **52 findings** in the directory holding `verify_e2e_attribution.py`,
`e2e_tier_streak.py` and `check_closing_keyword_intent.py`. **The three programs that decide
whether a promotion may proceed had no style or safety signal at all.**

🚨 **The general form, and the reason this file exists rather than a one-line workflow edit:**

```
ruff check <path that does not exist>   -> exit 1     (loud, fine)
ruff check <path with zero .py files>   -> exit 0     (silent, and reads as a pass)
```

Measured, not assumed — `ruff check deploy/` sees **0 files** and exits **0** today. So a gate
can name a path, run, print nothing, and go green while checking nothing. Adding `scripts` to
the command is the fix for the instance; asserting that every named path resolves to real files
is the fix for the class.

⚠️ The counter asks **ruff itself** (`--show-files`) rather than globbing `**/*.py`. A glob is a
different predicate: it does not honour `exclude`, `extend-exclude` or `.gitignore`, so it would
answer *"there are Python files here"* when the question is *"will the linter open any"*. Those
diverge exactly where a gate silently stops covering something.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"
HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"

#: `ruff check <paths…>` / `ruff format --check <paths…>`, capturing the argument tail.
_RUFF = re.compile(r"ruff\s+(?:check|format)\s+((?:--check\s+)?[^\n|&;]+)")

#: Flags CI passes and the hook does not. Recorded rather than asserted away — see
#: :func:`test_the_ci_only_excludes_are_exactly_the_two_known_ones`.
KNOWN_CI_ONLY_EXCLUDES = {"datanika/migrations", "datanika/i18n"}


def _ruff_invocations(text: str) -> list[list[str]]:
    """Every `ruff check` / `ruff format` argument list found in a file.

    🚨 **Comment lines are skipped, and that is not tidiness.** The first version of this
    parser matched the pre-push hook's own header — *"Mirrors the CI gate (ruff check, ruff
    format --check, pytest)."* — and produced an invocation with **no paths** and a flag
    called `pytest).`. Three assertions failed against a correctly configured repo.

    A guard that reads prose as configuration reports the documentation, not the gate. Armed
    by :func:`test_control_the_parser_ignores_a_comment_that_mentions_ruff`, so the exclusion
    is measured rather than assumed to work.
    """
    out: list[list[str]] = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        for tail in _RUFF.findall(line):
            out.append([tok.strip('"').strip("'") for tok in tail.split()])
    return out


def _paths_and_flags(tokens: list[str]) -> tuple[set[str], set[str]]:
    """Split an argument list into bare paths and `--flag value` pairs."""
    paths: set[str] = set()
    flags: set[str] = set()
    skip_next = False
    for i, tok in enumerate(tokens):
        if skip_next:
            skip_next = False
            continue
        if tok == "--check":
            continue
        if tok.startswith("--"):
            flags.add(tok)
            if "=" not in tok and i + 1 < len(tokens) and not tokens[i + 1].startswith("--"):
                flags.add(tokens[i + 1])
                skip_next = True
            continue
        paths.add(tok)
    return paths, flags


def _files_ruff_would_check(path: str) -> int:
    """How many files ruff would actually open under `path`. **Ask ruff, not the filesystem.**"""
    proc = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "--show-files", path],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return len([ln for ln in proc.stdout.splitlines() if ln.strip()])


def _real_invocations(text: str) -> list[tuple[set[str], set[str]]]:
    """Parsed invocations, minus the ones that carry no arguments at all.

    🚨 **Second thing that reads as configuration and is not**, after the comment line: the
    hook echoes a *label* before each step — `echo "pre-push: ruff check"` — and the regex
    matches inside the quoted string, yielding an invocation with an empty path set. That
    made `test_scripts_is_linted_by_both_gates` fail with *"pre-push runs ruff over []"*
    against a hook that runs it correctly two lines below.

    Dropping argument-less matches is safe here because both gates always pass paths. A bare
    `ruff check` (which lints the project root) would be dropped too — and that is worth
    knowing: if a gate is ever written that way, this file will not see it. Stated rather
    than hidden, because a guard's blind spot belongs next to the guard.
    """
    out: list[tuple[set[str], set[str]]] = []
    for tokens in _ruff_invocations(text):
        paths, flags = _paths_and_flags([t for t in tokens if t])
        if paths or flags:
            out.append((paths, flags))
    return out


def _ci_lint_paths() -> list[tuple[set[str], set[str]]]:
    return _real_invocations(CI.read_text(encoding="utf-8"))


def _hook_lint_paths() -> list[tuple[set[str], set[str]]]:
    return _real_invocations(HOOK.read_text(encoding="utf-8"))


# ── the control runs first ───────────────────────────────────────────────────────────────


def test_control_the_file_counter_can_return_zero_and_can_return_many() -> None:
    """🔑 Run first. A counter that always answers "many" passes every assertion below it,
    and one that always answers 0 fails them all for the wrong reason.

    `deploy/` is the live zero: it holds shell and config, no Python. It is *not* named by
    either gate — which is correct — and it is exactly the shape that would make a gate a
    silent no-op if someone ever added it.
    """
    assert _files_ruff_would_check("deploy") == 0, (
        "`deploy/` was the known zero-Python path this control is built on. If it now holds "
        "Python, pick another zero — do not delete the control."
    )
    assert _files_ruff_would_check("datanika") > 100


def test_control_the_parser_ignores_a_comment_that_mentions_ruff() -> None:
    """The arming for the comment-skip above, on the exact line that broke it."""
    prose = "# Mirrors the CI gate (ruff check, ruff format --check, pytest)."
    assert _ruff_invocations(prose) == [], (
        "a comment mentioning ruff was parsed as an invocation. That is how this guard "
        "reported three failures against a correctly configured repo."
    )
    real = "        run: ruff check datanika tests scripts"
    assert _ruff_invocations(real) == [["datanika", "tests", "scripts"]], (
        "skipping comments must not skip real commands — the exclusion has to be narrow or "
        "it disarms the whole file"
    )


def test_control_the_parser_ignores_an_echoed_step_label() -> None:
    """The second false positive, on the exact line that produced it.

    `echo "pre-push: ruff check"` is a label. Reading it as an invocation reported the hook
    as linting nothing, two lines above where it lints correctly.
    """
    label = 'echo "pre-push: ruff check"'
    assert _real_invocations(label) == [], "an echoed label parsed as a ruff invocation"
    assert _real_invocations('"$PY" -m ruff check datanika tests scripts') == [
        ({"datanika", "tests", "scripts"}, set())
    ], "dropping empty matches must not drop the real command on the next line"


def test_control_the_parser_finds_ruff_in_both_files() -> None:
    """A regex that matches nothing makes every path assertion vacuously true."""
    assert len(_ruff_invocations(CI.read_text(encoding="utf-8"))) >= 2, (
        "no `ruff` invocation parsed out of ci.yml; the parser is broken, not the workflow"
    )
    assert len(_ruff_invocations(HOOK.read_text(encoding="utf-8"))) >= 2, (
        "no `ruff` invocation parsed out of scripts/hooks/pre-push"
    )


# ── the assertions ───────────────────────────────────────────────────────────────────────


def test_every_linted_path_actually_contains_files_ruff_will_open() -> None:
    """core#1237 AC3, in its general form.

    A path that resolves to nothing makes the gate exit 0 having checked nothing — which is
    indistinguishable, in CI, from a clean tree.
    """
    empty: list[str] = []
    for paths, _flags in _ci_lint_paths() + _hook_lint_paths():
        for path in sorted(paths):
            if _files_ruff_would_check(path) == 0:
                empty.append(path)
    assert not empty, (
        f"these paths are named by a lint gate and ruff opens no file under them: "
        f"{sorted(set(empty))}. The step will exit 0 having checked nothing, which reads "
        "exactly like a clean tree. Either the path is wrong or the gate is decorative."
    )


def test_control_the_ac3_assertion_would_fire_on_a_decorative_gate() -> None:
    """core#1237 AC3's own arming: show the assertion CAN fail, on a synthetic gate.

    Both halves, composed, so *"every named path has files"* passing on the real workflow is
    a measurement rather than a property of a check that never fires. `deploy` is a real
    directory a plausible edit would add — it holds the server scripts — and ruff opens
    nothing in it.
    """
    synthetic = "        run: ruff check datanika deploy"
    paths, _flags = _real_invocations(synthetic)[0]
    assert paths == {"datanika", "deploy"}, "the parser must see both paths for this to arm"

    empty = [p for p in sorted(paths) if _files_ruff_would_check(p) == 0]
    assert empty == ["deploy"], (
        f"expected `deploy` to be the decorative one and `datanika` not to be; got {empty}. "
        "If ruff now opens files under deploy/, this control no longer arms anything."
    )


def test_scripts_is_linted_by_both_gates() -> None:
    """The concrete half of core#1237.

    Both, not either: CI alone leaves a direct push to `dev` unlinted (there is no
    pull-request requirement on that branch), and the hook alone is bypassed by `--admin` and
    by anything that does not push from this machine.
    """
    for label, invocations in (("ci.yml", _ci_lint_paths()), ("pre-push", _hook_lint_paths())):
        for paths, _flags in invocations:
            assert "scripts" in paths, (
                f"{label} runs ruff over {sorted(paths)} — `scripts` is missing. That "
                "directory holds verify_e2e_attribution.py, e2e_tier_streak.py and "
                "check_closing_keyword_intent.py: the tools that decide whether a promotion "
                "may proceed."
            )


def test_both_gates_name_the_same_paths() -> None:
    """`scripts/hooks/pre-push` says in its own header that it *"mirrors the CI gate"*.

    Divergence here is why the migration exemption was moved into `pyproject.toml` in the
    first place: two gates reading different file sets means a local green does not predict a
    CI green, in whichever direction the difference falls.
    """
    ci = {frozenset(p) for p, _ in _ci_lint_paths()}
    hook = {frozenset(p) for p, _ in _hook_lint_paths()}
    assert ci == hook, (
        f"ci.yml lints {[sorted(s) for s in ci]} and the hook lints {[sorted(s) for s in hook]}. "
        "A local green must predict a CI green; these disagree about which files exist."
    )


def test_the_ci_only_excludes_are_exactly_the_two_known_ones() -> None:
    """⚠️ Records a divergence rather than asserting it away, because it is real.

    `pyproject.toml` says the migration exemption was moved into `per-file-ignores` so *"both
    read the same rules, which is the only way a local green predicts a CI green"*. CI still
    passes `--exclude datanika/migrations` as well — and `--exclude` drops the files entirely,
    while `per-file-ignores` only silences `S608` there. So a migration carrying an `E501`
    passes CI and fails the hook.

    That is a small, one-directional gap (the hook is the stricter of the two, which is the
    safe direction) and it is **not** what core#1237 is about. It is pinned here so it stays
    deliberate: a third exclusion appearing, or these two changing, should be a decision.
    """
    ci_flags: set[str] = set()
    for _paths, flags in _ci_lint_paths():
        ci_flags |= {f for f in flags if not f.startswith("--")}
    assert ci_flags == KNOWN_CI_ONLY_EXCLUDES, (
        f"CI's ruff excludes are {sorted(ci_flags)}, expected {sorted(KNOWN_CI_ONLY_EXCLUDES)}. "
        "Every exclude is a set of files no CI gate reads; adding one is a decision, not a "
        "detail."
    )
    for _paths, flags in _hook_lint_paths():
        assert not {f for f in flags if not f.startswith("--")}, (
            "the pre-push hook has grown an exclude. It is currently the stricter gate, and "
            "that is the safe direction; narrowing it needs to be deliberate."
        )
