"""core#944 — the nightly connector smoke's two tiers must stay coupled to reality.

The nightly ran `12 failed, 9 passed` for ten nights. The 12 decomposed into three
classes with three different owners, and only one of them was a connector problem:

* **6** credentials that existed on disk and never reached the runner (fixed);
* **2** Oracle probes running in the `smoke` job, which has no Oracle credentials,
  while the `oracle-smoke` job runs the same probes against a live XE service
  container and passes — a test-*selection* defect (Class C, this file);
* **4** probes against vendor accounts that no longer exist (Class A, pinned into
  the awaiting-provisioning tier, tracked in cloud#160).

Leaving those four red for ever is not neutral. A permanently-red job teaches
everyone to read red as normal, which is core#827's outcome reached by the
opposite route — and the nine probes that *do* watch live connectors go unwatched
behind it.

## What this file guards, and why each one is not paperwork

Both halves of the fix are hand-maintained lists, which is the failure mode this
project keeps paying for: *a hand-maintained list coupled to nothing goes stale
silently, and a derived guard that stops seeing its input passes vacuously and
for ever.* So each list is checked against the artifact it claims to describe:

1. ``EXPECT_DESELECTED`` in the workflow is compared against the number of
   Oracle-named probes **derived from the test sources by AST**. The workflow's
   `2` and the suite's two Oracle tests cannot drift apart.
2. Every name in ``AWAITING_PROVISIONING`` is resolved against the real test
   functions. A pin naming a renamed test would silently stop covering anything
   *and* silently stop being checked.
3. The deselect and the tier line are each asserted to still exist in the step.
   A deselect emits no ``skipped`` line, so the job's own skip alarm is
   structurally blind to it.

Reading the sources by **AST rather than by import** is deliberate:
``test_wave1_connectors_extract_load.py`` imports ``duckdb``, ``httpx`` and dlt at
module scope, and ``test_paid_connectors.py`` reaches for client libraries only the
nightly installs. Importing to enumerate would couple this guard's ability to run
to a dependency set it has no opinion about. Same reasoning as
``tests/test_migrations/conftest.py``.
"""

from __future__ import annotations

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SMOKE_DIR = REPO_ROOT / "tests" / "test_connector_smoke"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly-connector-smoke.yml"


# --------------------------------------------------------------------------- #
# Derivations — pure functions, so every check below can be fed a mutated input
# and shown red. A check that has only ever been run against the healthy artifact
# has not been shown able to fail.
# --------------------------------------------------------------------------- #


def probe_names(directory: Path) -> set[str]:
    """Every ``def test_*`` under ``directory``, read by AST (never imported)."""
    names: set[str] = set()
    for path in sorted(directory.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith(
                "test_"
            ):
                names.add(node.name)
    return names


def oracle_named(names: set[str]) -> set[str]:
    """The subset ``-k "not oracle"`` would deselect.

    Mirrors pytest's ``-k`` substring match, case-insensitively, against the test
    name. The node id also carries the file path, but no file under
    ``tests/test_connector_smoke/`` contains ``oracle``, which
    :func:`test_the_k_expression_matches_names_only_because_no_path_does` pins.
    """
    return {n for n in names if "oracle" in n.lower()}


def expect_deselected(workflow_text: str) -> int | None:
    m = re.search(r"^\s*EXPECT_DESELECTED=(\d+)\s*$", workflow_text, re.MULTILINE)
    return int(m.group(1)) if m else None


def unresolvable_pins(pinned: set[str], available: set[str]) -> list[str]:
    return sorted(pinned - available)


def load_pin() -> dict:
    """Load ``AWAITING_PROVISIONING`` from the smoke conftest by file path.

    By path rather than by package import so this guard does not depend on how
    pytest happens to have loaded that conftest, and so it reads the file that is
    actually on disk.
    """
    path = SMOKE_DIR / "conftest.py"
    name = "_smoke_conftest_for_guard"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader, f"cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    # ⚠️ Register BEFORE exec: @dataclass resolves its own annotations through
    # ``sys.modules[cls.__module__].__dict__``, so a module executed while absent
    # from sys.modules dies with a bare ``'NoneType' object has no attribute
    # '__dict__'`` — which reads like a broken conftest, not a broken loader.
    sys.modules[name] = mod
    try:
        spec.loader.exec_module(mod)
        return mod.AWAITING_PROVISIONING
    finally:
        sys.modules.pop(name, None)


@pytest.fixture(scope="module")
def workflow_text() -> str:
    assert WORKFLOW.is_file(), f"missing workflow: {WORKFLOW}"
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "Run connector smoke tests" in text, "workflow no longer contains the smoke step"
    return text


def executable_lines(block: str) -> str:
    """Drop comment lines, so an assertion cannot be satisfied by prose.

    🚨 **Both of the -k assertions below passed against a workflow whose pytest
    line had been stripped of `-k`, because the flag is also *described* in the
    step's own comment.** Found by mutating the real file (the whole reason for
    doing that rather than writing a fixture): a guard reading the raw step text
    was checking that we still talk about deselecting Oracle, not that we do it.

    These blocks are shell, so a line whose first non-space character is `#` is a
    comment by definition — there is no case where dropping one loses a command.
    """
    return "\n".join(ln for ln in block.splitlines() if not ln.lstrip().startswith("#"))


@pytest.fixture(scope="module")
def smoke_step(workflow_text: str) -> str:
    start = workflow_text.index("- name: Run connector smoke tests")
    nxt = workflow_text.index("- name: Telegram alert on failure", start)
    return executable_lines(workflow_text[start:nxt])


@pytest.fixture(scope="module")
def oracle_step(workflow_text: str) -> str:
    start = workflow_text.index("- name: Run Oracle connector smoke + E2E tests")
    nxt = workflow_text.index("- name: Telegram alert on failure", start)
    return executable_lines(workflow_text[start:nxt])


@pytest.fixture(scope="module")
def suite_names() -> set[str]:
    return probe_names(SMOKE_DIR)


# --------------------------------------------------------------------------- #
# Arming — if the derivation stops seeing its input, everything below is vacuous
# --------------------------------------------------------------------------- #


class TestTheDerivationIsArmed:
    """A guard that reads nothing agrees with everything."""

    def test_the_ast_walk_finds_the_suite(self, suite_names: set[str]) -> None:
        assert len(suite_names) >= 15, (
            f"AST walk over {SMOKE_DIR} found only {len(suite_names)} test functions. "
            "Every assertion in this file is derived from that set, so an empty or "
            "truncated read makes all of them pass vacuously."
        )

    def test_it_finds_probes_it_must_find(self, suite_names: set[str]) -> None:
        for known in (
            "test_asana_auth_and_current_user",
            "test_oracle_extract_load_assert",
            "test_kafka_auth_and_list_topics",
        ):
            assert known in suite_names, (
                f"{known} is not in the AST-derived name set. The parser is reading "
                "something other than the connector smoke suite."
            )

    def test_the_ast_walk_can_come_back_empty(self, tmp_path: Path) -> None:
        """Negative control on the arming check itself."""
        assert probe_names(tmp_path) == set()

    def test_an_assertion_cannot_be_satisfied_by_a_comment(self) -> None:
        """Pins the defect the mutation harness found in this very file.

        Both `-k` assertions below originally read the raw step text and stayed
        GREEN against a workflow whose pytest line had been stripped of `-k`,
        because the flag is also mentioned in the step's comment. A guard that
        checks we still *talk* about a fix is worse than no guard: it is a green
        that reads like coverage.
        """
        block = '          # we deselect with -k "not oracle" here\n          pytest tests/ -v\n'
        assert '-k "not oracle"' in block, "the raw text does contain it — that was the trap"
        assert '-k "not oracle"' not in executable_lines(block), (
            "executable_lines() is no longer stripping comments, so every command-level "
            "assertion in this file can be satisfied by prose again."
        )
        assert "pytest tests/ -v" in executable_lines(block), (
            "executable_lines() dropped a real command — it must remove comments only."
        )


# --------------------------------------------------------------------------- #
# Class C — the Oracle deselect, and the number that describes it
# --------------------------------------------------------------------------- #


class TestOracleIsDeselectedFromTheSmokeJob:
    def test_the_smoke_job_deselects_oracle(self, smoke_step: str) -> None:
        assert '-k "not oracle"' in smoke_step, (
            "The smoke job no longer deselects Oracle. It has no ORACLE_* credentials, "
            "so every Oracle probe fails there with `Missing env vars: ORACLE_*` while "
            "the oracle-smoke job runs the same probes green against a live XE service "
            "container. That is core#944 Class C — a test-selection defect that looks "
            "exactly like a connector outage."
        )

    def test_oracle_is_still_covered_by_the_other_job(self, oracle_step: str) -> None:
        """Deselecting is only legitimate because something else runs them."""
        assert "-k oracle" in oracle_step, (
            "The oracle-smoke job no longer selects the Oracle probes. Deselecting them "
            "from the smoke job is defensible ONLY while this job runs them; without it "
            "the deselect is deletion of coverage wearing a fix's clothes."
        )

    def test_expect_deselected_matches_the_suite(
        self, workflow_text: str, suite_names: set[str]
    ) -> None:
        declared = expect_deselected(workflow_text)
        derived = oracle_named(suite_names)
        assert declared is not None, (
            "EXPECT_DESELECTED is gone from the workflow. It is the only thing that can "
            "see a mis-scoped -k expression: a deselect emits no `skipped` line, so the "
            "job's skip alarm is structurally blind to a nightly that silently shrank."
        )
        assert declared == len(derived), (
            f"The workflow expects {declared} deselected probe(s); the suite contains "
            f"{len(derived)} Oracle-named test(s): {sorted(derived)}. These two numbers "
            "describe the same fact and have drifted. Update EXPECT_DESELECTED — do not "
            "relax this assertion."
        )

    def test_the_count_check_can_fail(self, workflow_text: str, suite_names: set[str]) -> None:
        """Mutate the REAL workflow text, not a fixture written from the same idea.

        A synthetic control agrees with the check including where the check is
        wrong. This flips the actual declared number in the actual file's text and
        confirms the comparison notices.
        """
        declared = expect_deselected(workflow_text)
        assert declared is not None
        mutated = workflow_text.replace(
            f"EXPECT_DESELECTED={declared}", f"EXPECT_DESELECTED={declared + 1}", 1
        )
        assert mutated != workflow_text, "the mutation did not apply — anchor drift"
        assert expect_deselected(mutated) != len(oracle_named(suite_names)), (
            "Bumping EXPECT_DESELECTED by one did not make the comparison disagree, so "
            "the comparison is not reading what it claims to read."
        )

    def test_the_k_expression_matches_names_only_because_no_path_does(self) -> None:
        """`-k` matches the whole node id, path included. Pin that assumption."""
        offenders = [p.name for p in SMOKE_DIR.glob("*.py") if "oracle" in p.name.lower()]
        assert not offenders, (
            f"{offenders} contain 'oracle' in the FILE NAME. pytest's -k matches the whole "
            'node id, so `-k "not oracle"` would deselect every test in those files, not '
            "just the Oracle probes — and the deselect count assertion is the only thing "
            "that would notice."
        )


# --------------------------------------------------------------------------- #
# Class A — the awaiting-provisioning pin
# --------------------------------------------------------------------------- #


class TestTheAwaitingProvisioningPin:
    def test_every_pinned_probe_exists(self, suite_names: set[str]) -> None:
        missing = unresolvable_pins(set(load_pin()), suite_names)
        assert not missing, (
            f"AWAITING_PROVISIONING pins {missing}, which no longer exist in "
            f"{SMOKE_DIR}. A pin naming a renamed or deleted test stops covering "
            "anything AND stops being checked — it is the hand-maintained-list "
            "failure this file exists to prevent."
        )

    def test_the_existence_check_can_fail(self, suite_names: set[str]) -> None:
        """Negative control, on the real pin plus one injected name."""
        polluted = set(load_pin()) | {"test_a_probe_that_was_renamed_away"}
        assert unresolvable_pins(polluted, suite_names) == ["test_a_probe_that_was_renamed_away"]

    def test_no_pinned_probe_is_also_deselected(self, suite_names: set[str]) -> None:
        overlap = sorted(set(load_pin()) & oracle_named(suite_names))
        assert not overlap, (
            f"{overlap} are both deselected by -k 'not oracle' and pinned as "
            "awaiting-provisioning. A pin on a probe that never runs covers nothing, and "
            "its strict xfail can never report the account coming back."
        )

    def test_every_pin_carries_evidence_and_a_tracking_issue(self) -> None:
        for name, pin in load_pin().items():
            assert re.search(r"#\d+", pin.tracking), (
                f"{name} is pinned with tracking={pin.tracking!r}, which names no issue. "
                "A tier entry with nowhere to go is how a temporary pin becomes permanent."
            )
            assert len(pin.evidence) > 20, (
                f"{name} is pinned with evidence={pin.evidence!r}. The evidence is what "
                "distinguishes 'this account is provably gone' from 'this test is annoying'."
            )
            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", pin.measured_on), (
                f"{name} has measured_on={pin.measured_on!r}, not an ISO date. An undated "
                "measurement cannot be recognised as stale."
            )

    def test_the_pin_is_not_a_licence_to_mute_the_whole_suite(self, suite_names: set[str]) -> None:
        """A tier that swallows most of the suite is muting with extra steps."""
        gating = len(suite_names) - len(oracle_named(suite_names)) - len(load_pin())
        assert gating >= len(load_pin()), (
            f"Only {gating} probe(s) remain in the gating tier against {len(load_pin())} "
            "pinned. The tier split exists to protect the live probes; once the pinned set "
            "is the larger half, the honest report is that the matrix is unprovisioned, "
            "not that the nightly is green."
        )


# --------------------------------------------------------------------------- #
# The step's own guards must survive
# --------------------------------------------------------------------------- #


class TestTheStepStillChecksItself:
    def test_the_tier_line_is_required_by_the_step(self, smoke_step: str) -> None:
        # The step's grep pattern escapes the brackets (`^\[tier\] pinned=`), so
        # match tolerantly rather than on the literal — asserting the unescaped
        # form fails against a correct workflow, which is a guard that cries wolf.
        assert re.search(r"\\?\[tier\\?\] pinned=", smoke_step), (
            "The smoke step no longer greps for the [tier] line. That line is emitted by "
            "pytest_terminal_summary in the smoke conftest and is the only source of the "
            "stale-pin verdict — pytest's own summary reports a strict XPASS as `failed` "
            "and prints no `xpassed` token at all, so parsing the prose cannot work."
        )

    def test_an_absent_tier_line_fails_the_step(self, smoke_step: str) -> None:
        """The anti-vacuity half: an absence must not read as clean."""
        assert re.search(r'if \[ -z "\$TIER" \]', smoke_step), (
            "The step no longer fails when the [tier] line is missing. An absence is not "
            "evidence that nothing was wrong — it is what a run with the gate off, or with "
            "the conftest unloaded, also looks like."
        )

    def test_the_stale_pin_verdict_is_acted_on(self, smoke_step: str) -> None:
        assert "n_stale" in smoke_step and "exit 1" in smoke_step, (
            "The step reads the stale-pin count but never exits non-zero on it. Recording "
            "a failure without acting on it is the same green — and a pin that can only "
            "ever be satisfied is muting, not tiering."
        )
