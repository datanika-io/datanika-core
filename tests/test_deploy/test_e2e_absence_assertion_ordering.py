"""An absence assertion in an E2E spec must follow a positive readiness signal.

core#1008. ``expect(locator).toHaveCount(0)`` is satisfied on its **first poll**.
Placed before the thing under test has rendered, it cannot distinguish *"the
control is correctly absent"* from *"the page has not finished loading"* -- and
it is exactly the shape a security-adjacent spec reaches for, because
least-privilege assertions are assertions about absence.

Measured for the motivating case (``e2e/scripts/probe-1008-absence-ordering.mjs``,
a 2x2 against a page that renders Edit/Delete 1.2 s after hydration -- i.e. a
viewer gate that has broken):

===========================================  ========  =======
gate                                         order     verdict
===========================================  ========  =======
BROKEN (viewer sees Edit/Delete)             shipped   PASS
BROKEN (viewer sees Edit/Delete)             fixed     FAIL
GOOD   (viewer sees neither)                 shipped   PASS
GOOD   (viewer sees neither)                 fixed     PASS
===========================================  ========  =======

The bottom two rows are the false-positive control: the fixed order is not
merely failing more often.

**Why a guard and not just the fix.** A per-instance fix covers the instance
somebody already debugged (QA_RULES 23). This is derived from the spec files
themselves, so a spec written next month is covered the moment it is written.

**Opting out.** Some absences are ready at hydration rather than after a load --
``rx.cond(AuthState.can_edit, ...)`` renders with the page. Mark those with
``// absence-ok: <reason>`` on a line above the assertion. The reason is
required, so the opt-out is a sentence somebody has to write rather than a
pragma somebody can paste.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC_DIR = REPO_ROOT / "e2e" / "tests"

#: Assertions satisfied immediately by a page that has not rendered yet.
ABSENCE_RE = re.compile(
    r"toHaveCount\(\s*0\s*\)|toBeHidden\(\)|\.not\.toBeVisible\(|\.not\.toBeAttached\("
)

#: Auto-retrying assertions/waits that only resolve once something is really there.
POSITIVE_RE = re.compile(
    r"toBeVisible\(|toBeAttached\(|toHaveText\(|toContainText\(|toHaveValue\(|"
    r"toBeEnabled\(|toHaveURL\(|waitForSelector\(|waitForResponse\(|"
    r"toHaveCount\(\s*[1-9]"
)

OPT_OUT_RE = re.compile(r"//\s*absence-ok:\s*(?P<reason>.*)")

#: Splitting on `test(` is enough: `test.describe(` does not match (the `(` is
#: not adjacent), and what this guard reasons about is assertion ORDER inside one
#: test, which is precisely what the split preserves.
TEST_START_RE = re.compile(r"\btest\s*\(")


def _spec_files() -> list[Path]:
    return sorted(SPEC_DIR.glob("*.spec.ts"))


def _test_bodies(text: str) -> list[tuple[int, str]]:
    """Return (start_line, body) for each `test(...)` in the file."""
    starts = [m.start() for m in TEST_START_RE.finditer(text)]
    bodies = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(text)
        bodies.append((text.count("\n", 0, start) + 1, text[start:end]))
    return bodies


def _violations(text: str) -> list[str]:
    """Absence assertions with no preceding positive signal and no opt-out."""
    out = []
    for first_line, body in _test_bodies(text):
        lines = body.splitlines()
        seen_positive = False
        for offset, line in enumerate(lines):
            if POSITIVE_RE.search(line):
                seen_positive = True
            if not ABSENCE_RE.search(line):
                continue
            if seen_positive:
                continue
            window = "\n".join(lines[max(0, offset - 12) : offset])
            opt = OPT_OUT_RE.search(window)
            if opt and opt.group("reason").strip():
                continue
            out.append(f"line {first_line + offset}: {line.strip()}")
    return out


def _all_absence_assertions(text: str) -> int:
    return sum(1 for line in text.splitlines() if ABSENCE_RE.search(line))


# --------------------------------------------------------------------------
# The guard, plus the controls that stop it reporting PASS while seeing nothing.
# --------------------------------------------------------------------------


def test_the_corpus_is_not_empty():
    """Anti-vacuity: this guard holds trivially over zero files."""
    specs = _spec_files()
    assert len(specs) >= 5, f"expected the e2e spec corpus, found {[p.name for p in specs]}"
    assert sum(len(_test_bodies(p.read_text(encoding="utf-8"))) for p in specs) >= 10


def test_the_parser_finds_the_absence_assertions_that_exist():
    """Anti-vacuity, the half that matters.

    A regex that matched nothing would make every spec compliant. If the
    E2E suite ever legitimately contains no absence assertion at all, delete
    this test deliberately rather than letting it decay into a decoration.
    """
    total = sum(_all_absence_assertions(p.read_text(encoding="utf-8")) for p in _spec_files())
    assert total >= 1, "no absence assertions found anywhere -- the detector is broken"


@pytest.mark.parametrize("spec", _spec_files(), ids=lambda p: p.name)
def test_absence_assertions_follow_a_positive_readiness_signal(spec: Path):
    violations = _violations(spec.read_text(encoding="utf-8"))
    assert not violations, (
        f"{spec.name}: an absence assertion runs before anything proved the page had "
        f"rendered, so it is satisfied by 'not yet' (core#1008). Wait for a positive "
        f"artifact first, or mark it `// absence-ok: <reason>`.\n  " + "\n  ".join(violations)
    )


_BROKEN = """
test("viewer sees no delete", async ({ page }) => {
  await gotoReady(page, "/connections");
  await expect(page.getByRole("button", { name: /^delete$/i })).toHaveCount(0);
});
"""

_FIXED = """
test("viewer sees no delete", async ({ page }) => {
  await gotoReady(page, "/connections");
  await expect(page.getByText("seeded-conn")).toBeVisible({ timeout: 10_000 });
  await expect(page.getByRole("button", { name: /^delete$/i })).toHaveCount(0);
});
"""

_OPTED_OUT = """
test("viewer sees no create form", async ({ page }) => {
  await gotoReady(page, "/connections");
  // absence-ok: gated on AuthState.can_edit, which hydrates with the page.
  await expect(page.getByRole("button", { name: /create/i })).toHaveCount(0);
});
"""

_OPT_OUT_WITH_NO_REASON = """
test("viewer sees no create form", async ({ page }) => {
  await gotoReady(page, "/connections");
  // absence-ok:
  await expect(page.getByRole("button", { name: /create/i })).toHaveCount(0);
});
"""

_LATER_TEST_DOES_NOT_LEND_ITS_WAIT = """
test("a", async ({ page }) => {
  await expect(page.getByText("row")).toBeVisible();
});
test("b", async ({ page }) => {
  await gotoReady(page, "/connections");
  await expect(page.getByRole("button", { name: /^edit$/i })).toHaveCount(0);
});
"""


def test_the_guard_can_fail():
    """QA_RULES 2: a green nobody has forced red is unproven."""
    assert _violations(_BROKEN), "the guard does not flag the defect it exists for"
    assert _violations(_OPT_OUT_WITH_NO_REASON), "a bare opt-out must not silence it"
    assert _violations(_LATER_TEST_DOES_NOT_LEND_ITS_WAIT), (
        "a positive wait in a DIFFERENT test must not satisfy this one -- that is "
        "the same cross-test borrowing that made the original bug invisible"
    )


def test_the_guard_does_not_fire_on_correct_code():
    """False-positive control (QA_RULES 3).

    Without this, a guard that had simply been made stricter would score
    identically on the row above.
    """
    assert not _violations(_FIXED)
    assert not _violations(_OPTED_OUT)
