"""The pre-push hook must not describe a scope it does not have (core#1268).

`scripts/hooks/pre-push` runs **`pytest tests/test_deploy`** by default; CI's `test` job runs
**`pytest tests/`**. The hook's own header says:

    # Mirrors the CI gate (ruff check, ruff format --check, pytest).

That is true of the two ruff steps and **false of pytest**, and Engineering reported *"full tree
green via the pre-push hook"* several times on the strength of it — the real run was **25 failed,
12 errors**.

🚨 **This is the fifth member of the family and the worst-shaped.** The other four were accidents
of invocation — a pipe that ate an exit code, a `grep` that aborted, a `--stdin` that
concatenated records. **This one is a designed default whose name implies a scope it does not
have**, so nothing is malfunctioning and there is nothing to notice.

⚠️ **The narrowing itself is correct and is not what this file objects to** (founder decision,
core#964: 1105.95s against 79.89s). CI's `test` job is a **required** check on `dev`, so the
merge gate is unaffected — the exposure is entirely *believing you hold evidence you do not*.

⚠️ **Author's own audit, for the record:** every count I published this week named
`tests/test_deploy/` explicitly, and my two "the pre-push hook gated this" claims were about the
**ruff** step and the marker scan, both of which the hook genuinely runs. But I shipped a change
to `scripts/mutation_probe.py` whose tests live in `tests/test_scripts/` — outside the hook's
scope — and never ran them. They pass. That is luck, not evidence I held at the time.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"

#: `PYTEST_SCOPE="${DATANIKA_PREPUSH_SCOPE:-tests/test_deploy}"` — note the quotes around the
#: expansion. The first version of this pattern omitted them, matched nothing, and
#: `test_control_both_extractors_find_something` fired before any comparison could be made
#: vacuously true. That is the control doing its job on its own author.
_HOOK_SCOPE = re.compile(r"""PYTEST_SCOPE=["']?\$\{[A-Z_]+:-([^}]+)\}""")
#: `run: pytest tests/ --tb=short -q`
_CI_PYTEST = re.compile(r"\bpytest\s+([^\s|&;]+)")


def _strip_comments(text: str) -> str:
    """Drop whole-line `#` comments.

    core#1260, three departments deep: a substring check over a whole file is satisfied by a
    comment. Both extractors below run on the stripped text so a *discussion* of the scope
    cannot stand in for the setting.
    """
    return "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))


def hook_pytest_scope() -> str:
    m = _HOOK_SCOPE.search(_strip_comments(HOOK.read_text(encoding="utf-8")))
    assert m, "no PYTEST_SCOPE default found in the hook — this file is measuring nothing"
    return m.group(1).strip()


def ci_pytest_scope() -> str:
    for candidate in _CI_PYTEST.findall(_strip_comments(CI.read_text(encoding="utf-8"))):
        if candidate.startswith("tests"):
            return candidate.strip()
    raise AssertionError("no `pytest tests...` invocation found in ci.yml")


# ── controls first ───────────────────────────────────────────────────────────────────────


def test_control_both_extractors_find_something() -> None:
    """Neither side may be silently empty. An extractor that finds nothing makes every
    comparison below vacuously true, in either direction."""
    assert hook_pytest_scope()
    assert ci_pytest_scope()


def test_control_neither_extractor_can_be_satisfied_by_a_comment() -> None:
    """core#1260's lesson, applied to this file's own extractors."""
    commented = "# PYTEST_SCOPE=${DATANIKA_PREPUSH_SCOPE:-tests/everything}\nreal_line=1"
    assert not _HOOK_SCOPE.search(_strip_comments(commented)), (
        "a commented-out scope satisfied the hook extractor"
    )
    live = "PYTEST_SCOPE=${DATANIKA_PREPUSH_SCOPE:-tests/test_deploy}"
    assert _HOOK_SCOPE.search(_strip_comments(live)), (
        "stripping comments ate the live line — every assertion here would be vacuously FALSE, "
        "which is the direction that gets a guard deleted rather than trusted"
    )


# ── the assertions ───────────────────────────────────────────────────────────────────────


def test_the_hook_runs_a_narrower_pytest_scope_than_ci() -> None:
    """The premise, asserted rather than assumed.

    If these ever converge, the header's "mirrors the CI gate" becomes true and the assertion
    below should be *removed*, not silenced. Stating the premise is what makes that decision
    visible instead of leaving a guard that quietly stops applying.
    """
    hook, ci = hook_pytest_scope(), ci_pytest_scope()
    assert hook != ci, (
        f"the hook and CI now run the same pytest scope ({hook!r}). The header may say it "
        "mirrors the CI gate, and `test_the_header_does_not_claim_a_scope_it_narrows` should "
        "be deleted in the same change."
    )
    assert hook.startswith(ci.rstrip("/")), (
        f"hook scope {hook!r} is not a subset of CI's {ci!r} — it is running something CI does "
        "not, which is a different and larger problem than this file was written for"
    )


def test_the_header_does_not_claim_a_scope_it_narrows() -> None:
    """🔑 The sentence that licensed the wrong belief.

    Not the narrowing — that is a founder decision and correct. The defect is a header that
    says *"mirrors the CI gate (… pytest)"* over a pytest step that runs a strict subset, and
    Engineering read exactly that and reported a full-tree green that was 25 failed, 12 errors.
    """
    header = "\n".join(HOOK.read_text(encoding="utf-8").splitlines()[:12])
    claims_mirror = "Mirrors the CI gate" in header
    mentions_pytest_in_claim = bool(re.search(r"Mirrors the CI gate[^\n]*pytest", header))
    assert not (claims_mirror and mentions_pytest_in_claim), (
        "the hook's header claims to mirror the CI gate *including pytest*, while its pytest "
        f"scope is {hook_pytest_scope()!r} and CI's is {ci_pytest_scope()!r}. Say which step "
        "mirrors and which narrows — a reader who believes this line has evidence they do not "
        "hold, and nothing malfunctions to tell them."
    )


def test_the_hook_prints_the_scope_it_actually_ran() -> None:
    """The output must name the scope, so no reader has to know this file exists.

    It already does. Asserted so a later tidy-up cannot make the hook silent about it, which
    would leave the header as the only description of the scope — the state this issue is about.
    """
    body = _strip_comments(HOOK.read_text(encoding="utf-8"))
    assert re.search(r'echo "pre-push: pytest \(\$PYTEST_SCOPE\)', body), (
        "the hook no longer prints the pytest scope it ran. Its own output is the only place a "
        "reader learns the scope at the moment they are relying on it."
    )


def test_the_widening_switch_is_discoverable_from_the_hook_itself() -> None:
    """A narrowed gate needs its escape hatch named where the narrowing is, or the only way to
    get a full run is to already know the variable."""
    body = HOOK.read_text(encoding="utf-8")
    assert "DATANIKA_PREPUSH_FULL" in body
    assert "DATANIKA_PREPUSH_SCOPE" in body
