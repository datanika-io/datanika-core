"""core#992 — a smoke probe must not claim, in prose, a vendor state it cannot see.

`test_nightly_smoke_tiers.py` guards the tier that holds probes which **cannot
pass**. This file guards the opposite and quieter failure: a probe that **passes
while claiming something it never checked**.

The instance it was written for:

    def test_databricks_auth_and_list_warehouses(require_env):
        \"\"\"... Catches: PAT revoked, workspace URL changed, scope downgrade,
        trial expiry.\"\"\"
        ...
        assert warehouses, "No SQL warehouses found — trial workspace may have expired"

The trial *had* expired — `POST /api/2.0/sql/warehouses/{id}/start` returns
`400 denyReason=INACTIVE, domain=resource-gatekeeper` (cloud#124). The warehouse
row survives the expiry, so that assertion could never fire for the reason its own
message names, and both Databricks probes were green on every nightly.

**A red gets investigated; a green never does.** So a false claim in a passing
probe is worth more attention than a failing one, and it is invisible to every
check we had: the suite is green, the tier line is clean, and the docstring is
prose that nothing reads.

## What is enforced

Against `UNOBSERVABLE_STATES` in the smoke conftest, for each registered probe:

1. the forbidden phrase (the claim that was actually false) is **absent**, and
2. the `Does NOT catch:` marker naming the state is **present**.

Both halves matter. Enforcing only (1) is satisfied by deleting the sentence,
which leaves the reader with the same wrong impression and nothing to argue with;
enforcing only (2) lets the disclaimer sit beside the claim it contradicts.

Plus a structural check for assertions that cannot be false — `assert datasets is
not None` where `datasets = list(...)`, the second finding on core#992.

Docstrings and assertions are read by **AST, never by import**: the smoke modules
reach for client libraries only the nightly installs, so importing to enumerate
would couple this guard's ability to run to a dependency set it has no opinion
about. Same reasoning as `test_nightly_smoke_tiers.py`.
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


# --------------------------------------------------------------------------- #
# Derivations — pure functions, so each can be fed a mutated input and shown red
# --------------------------------------------------------------------------- #


def probe_docstrings(directory: Path) -> dict[str, str]:
    """``test_*`` function name -> its docstring, read by AST (never imported)."""
    out: dict[str, str] = {}
    for path in sorted(directory.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith(
                "test_"
            ):
                out[node.name] = ast.get_docstring(node) or ""
    return out


#: Expressions that can never evaluate to ``None``. A name bound from one of
#: these and then compared ``is not None`` is a check with one possible answer.
_NEVER_NONE_CALLS = {"list", "sorted", "dict", "set", "tuple", "len", "str", "int", "bool"}
_NEVER_NONE_NODES = (
    ast.List,
    ast.Dict,
    ast.Set,
    ast.Tuple,
    ast.ListComp,
    ast.DictComp,
    ast.SetComp,
    ast.JoinedStr,
)


def _never_none(value: ast.expr) -> bool:
    if isinstance(value, _NEVER_NONE_NODES):
        return True
    if isinstance(value, ast.Constant):
        return value.value is not None
    return (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id in _NEVER_NONE_CALLS
    )


def _is_not_none_target(node: ast.Assert) -> str | None:
    """``assert X is not None`` -> ``"X"``; anything else -> ``None``."""
    test = node.test
    if not isinstance(test, ast.Compare) or len(test.ops) != 1:
        return None
    if not isinstance(test.ops[0], ast.IsNot):
        return None
    right = test.comparators[0]
    if not (isinstance(right, ast.Constant) and right.value is None):
        return None
    return test.left.id if isinstance(test.left, ast.Name) else None


def vacuous_none_assertions(source: str, filename: str = "<mem>") -> list[tuple[str, str]]:
    """``(test name, variable)`` for every ``assert X is not None`` that must hold.

    Only reports when the same function binds ``X`` to something that cannot be
    ``None``. A bare ``assert x is not None`` on a parameter or an attribute is
    left alone — it may well be a real check.
    """
    found: list[tuple[str, str]] = []
    tree = ast.parse(source, filename=filename)
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and node.name.startswith("test_")
        ):
            continue
        never_none: set[str] = set()
        for stmt in ast.walk(node):
            if isinstance(stmt, ast.Assign) and _never_none(stmt.value):
                never_none.update(t.id for t in stmt.targets if isinstance(t, ast.Name))
            elif (
                isinstance(stmt, ast.AnnAssign)
                and stmt.value is not None
                and _never_none(stmt.value)
                and isinstance(stmt.target, ast.Name)
            ):
                never_none.add(stmt.target.id)
        for stmt in ast.walk(node):
            if isinstance(stmt, ast.Assert):
                name = _is_not_none_target(stmt)
                if name is not None and name in never_none:
                    found.append((node.name, name))
    return sorted(found)


def claim_region(doc: str, marker: str) -> str:
    """The part of a docstring that makes CLAIMS — everything before the marker.

    🚨 **The first version of this guard had no such split and went red against
    its own fix.** The corrected docstring says *"Does NOT catch: trial expiry"*,
    so a naive substring search for ``trial expiry`` fires on the sentence written
    to deny the claim. That is this project's standing trap — *a static guard over
    self-documenting code must exclude the documentation* — arriving through the
    text the fix introduced.

    Splitting rather than deleting is deliberate: the denial has to be checked
    too (it must name the state), just by a different assertion.
    """
    return doc.split(marker, 1)[0] if marker in doc else doc


def load_smoke_conftest():
    """Load the smoke conftest by file path, for its registers.

    ⚠️ Register in ``sys.modules`` BEFORE ``exec_module``: ``@dataclass`` resolves
    its annotations through ``sys.modules[cls.__module__].__dict__``, and a module
    executed while absent from it dies with a bare ``'NoneType' object has no
    attribute '__dict__'`` — which reads like a broken conftest, not a broken
    loader.
    """
    path = SMOKE_DIR / "conftest.py"
    name = "_smoke_conftest_for_claims_guard"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader, f"cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    try:
        spec.loader.exec_module(mod)
        return mod
    finally:
        sys.modules.pop(name, None)


@pytest.fixture(scope="module")
def docstrings() -> dict[str, str]:
    return probe_docstrings(SMOKE_DIR)


@pytest.fixture(scope="module")
def conftest_mod():
    return load_smoke_conftest()


# --------------------------------------------------------------------------- #
# Arming — a guard that reads nothing agrees with everything
# --------------------------------------------------------------------------- #


class TestTheDerivationIsArmed:
    def test_the_ast_walk_finds_the_suite(self, docstrings: dict[str, str]) -> None:
        assert len(docstrings) >= 15, (
            f"AST walk over {SMOKE_DIR} found only {len(docstrings)} test functions. "
            "Every assertion below is derived from that set, so a truncated read makes "
            "all of them pass vacuously."
        )

    def test_it_actually_reads_docstrings(self, docstrings: dict[str, str]) -> None:
        """Names without text would satisfy 'the phrase is absent' everywhere."""
        with_text = [n for n, d in docstrings.items() if d.strip()]
        assert len(with_text) >= 15, (
            f"Only {len(with_text)} of {len(docstrings)} probes came back with a "
            "docstring. If the extractor returns empty strings, the forbidden-phrase "
            "check passes for every probe in the suite while reading nothing."
        )

    def test_it_finds_the_probes_this_file_is_about(self, docstrings: dict[str, str]) -> None:
        for known in (
            "test_databricks_auth_and_list_warehouses",
            "test_bigquery_auth_and_list_datasets",
        ):
            assert docstrings.get(known, "").strip(), (
                f"{known} has no AST-derived docstring. The parser is reading something "
                "other than the connector smoke suite."
            )

    def test_the_ast_walk_can_come_back_empty(self, tmp_path: Path) -> None:
        assert probe_docstrings(tmp_path) == {}

    def test_the_register_is_not_empty(self, conftest_mod) -> None:
        assert conftest_mod.UNOBSERVABLE_STATES, (
            "UNOBSERVABLE_STATES is empty, so every check below iterates over nothing "
            "and passes. An empty register is not the same as no unobservable states."
        )


# --------------------------------------------------------------------------- #
# The register must describe reality
# --------------------------------------------------------------------------- #


class TestTheRegisterIsCoupledToTheSuite:
    def test_every_registered_probe_exists(self, conftest_mod, docstrings: dict[str, str]) -> None:
        missing = sorted(set(conftest_mod.UNOBSERVABLE_STATES) - set(docstrings))
        assert not missing, (
            f"UNOBSERVABLE_STATES names {missing}, which no longer exist in {SMOKE_DIR}. "
            "An entry naming a renamed probe stops governing anything AND stops being "
            "checked — the hand-maintained-list failure this project keeps paying for."
        )

    def test_the_existence_check_can_fail(self, conftest_mod, docstrings: dict[str, str]) -> None:
        polluted = set(conftest_mod.UNOBSERVABLE_STATES) | {"test_a_probe_that_was_renamed_away"}
        assert sorted(polluted - set(docstrings)) == ["test_a_probe_that_was_renamed_away"]

    def test_every_entry_carries_evidence_and_a_tracking_issue(self, conftest_mod) -> None:
        for name, entry in conftest_mod.UNOBSERVABLE_STATES.items():
            assert re.search(r"#\d+", entry.tracking), (
                f"{name} declares tracking={entry.tracking!r}, which names no issue. A "
                "recorded coverage gap with nowhere to go becomes a permanent one."
            )
            assert len(entry.evidence) > 40, (
                f"{name} declares evidence={entry.evidence!r}. The evidence is what "
                "distinguishes 'we measured that this cannot be seen' from 'nobody "
                "looked'."
            )
            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", entry.measured_on), (
                f"{name} has measured_on={entry.measured_on!r}, not an ISO date. An "
                "undated measurement cannot be recognised as stale."
            )
            assert entry.state.strip(), f"{name} declares no state."

    def test_a_probe_is_not_both_pinned_and_registered(self, conftest_mod) -> None:
        overlap = sorted(
            set(conftest_mod.AWAITING_PROVISIONING) & set(conftest_mod.UNOBSERVABLE_STATES)
        )
        assert not overlap, (
            f"{overlap} are both pinned as awaiting-provisioning and registered as making "
            "an unobservable claim. The registers answer different questions — a pinned "
            "probe is expected to FAIL, so what its docstring claims is moot — and an "
            "entry in both means one of them is wrong."
        )


# --------------------------------------------------------------------------- #
# The claims themselves
# --------------------------------------------------------------------------- #


class TestNoProbeClaimsWhatItCannotSee:
    def test_the_forbidden_claim_is_absent(self, conftest_mod, docstrings: dict[str, str]) -> None:
        marker = conftest_mod.UNOBSERVABLE_MARKER
        for name, entry in conftest_mod.UNOBSERVABLE_STATES.items():
            doc = claim_region(docstrings[name], marker).lower()
            for phrase in entry.forbidden_claims:
                assert phrase.lower() not in doc, (
                    f"{name} still claims {phrase!r} in its docstring, but "
                    f"{entry.vendor}'s {entry.state} is not observable from this probe: "
                    f"{entry.evidence} (measured {entry.measured_on}, {entry.tracking}). "
                    "A green probe that names a state it never checked spends the "
                    "credibility the gating tier exists to have."
                )

    def test_the_disclaimer_is_present(self, conftest_mod, docstrings: dict[str, str]) -> None:
        marker = conftest_mod.UNOBSERVABLE_MARKER
        for name, entry in conftest_mod.UNOBSERVABLE_STATES.items():
            doc = docstrings[name]
            assert marker in doc, (
                f"{name} does not carry a {marker!r} section. Deleting the false claim is "
                "only half the fix: without a sentence saying what this probe does NOT "
                f"see, a reader still takes green for '{entry.vendor} works'."
            )
            assert entry.state.lower() in doc.lower(), (
                f"{name} carries a {marker!r} section that does not name "
                f"{entry.state!r}. A generic disclaimer is not a statement about this "
                "gap and cannot be checked against anything."
            )

    def test_the_claim_check_can_fail(self, conftest_mod, docstrings: dict[str, str]) -> None:
        """Mutate the REAL docstring, not a fixture written from the same idea.

        A synthetic control agrees with the check including where the check is
        wrong. This re-inserts the phrase that was actually there and confirms the
        comparison notices.
        """
        marker = conftest_mod.UNOBSERVABLE_MARKER
        name = "test_databricks_auth_and_list_warehouses"
        entry = conftest_mod.UNOBSERVABLE_STATES[name]
        phrase = entry.forbidden_claims[0]
        real = docstrings[name]
        assert phrase.lower() not in claim_region(real, marker).lower(), (
            "precondition: the real docstring's claim region is clean"
        )
        # Put the claim back where it actually was — in the affirmative prose,
        # ahead of the disclaimer.
        mutated = real.replace(marker, f"Catches: PAT revoked, {phrase}.\n\n{marker}", 1)
        assert mutated != real, "anchor drift — the marker is no longer in the real docstring"
        assert phrase.lower() in claim_region(mutated, marker).lower(), (
            "Re-inserting the historical claim into the real docstring's claim region did "
            "not make the check disagree, so the check is not reading what it claims to."
        )

    def test_the_splitter_works_in_both_directions(self, conftest_mod) -> None:
        """A stripper that removes too much disarms every check downstream of it.

        Asserting only that the disclaimer is excluded is half a test: a
        ``claim_region`` returning ``""`` would satisfy it and silently accept any
        false claim in the suite.
        """
        marker = conftest_mod.UNOBSERVABLE_MARKER
        doc = f"Catches: PAT revoked, trial expiry.\n\n{marker} trial expiry / gating.\n"
        region = claim_region(doc, marker)
        assert "Catches: PAT revoked, trial expiry." in region, (
            "claim_region() dropped the affirmative prose. It must split at the marker, "
            "not truncate — otherwise every forbidden-claim check below passes vacuously."
        )
        assert marker not in region and "gating" not in region, (
            "claim_region() no longer excludes the disclaimer, so a probe goes red for the "
            "sentence written to fix it."
        )
        assert claim_region("no marker here", marker) == "no marker here", (
            "With no marker present the whole docstring is claim region — otherwise an "
            "unregistered probe would be exempt from checking by omission."
        )

    def test_the_marker_appears_exactly_once(
        self, conftest_mod, docstrings: dict[str, str]
    ) -> None:
        """Two markers would open a second 'denial' region to hide a claim in."""
        marker = conftest_mod.UNOBSERVABLE_MARKER
        for name in conftest_mod.UNOBSERVABLE_STATES:
            count = docstrings[name].count(marker)
            assert count == 1, (
                f"{name} carries {count} {marker!r} sections. Everything after the FIRST "
                "one is excluded from the claim check, so a second section is a place a "
                "false claim can sit unread."
            )

    def test_the_disclaimer_check_can_fail(self, conftest_mod, docstrings: dict[str, str]) -> None:
        marker = conftest_mod.UNOBSERVABLE_MARKER
        name = "test_bigquery_auth_and_list_datasets"
        real = docstrings[name]
        assert marker in real, "precondition: the real docstring carries the marker"
        assert marker not in real.replace(marker, "Catches:"), (
            "Removing the marker from the real docstring did not make the presence check "
            "disagree — the check is satisfied by something other than the marker."
        )


# --------------------------------------------------------------------------- #
# Assertions that cannot be false
# --------------------------------------------------------------------------- #


class TestNoProbeAssertsSomethingThatCannotBeFalse:
    def test_the_suite_has_none(self) -> None:
        offenders: list[tuple[str, str]] = []
        for path in sorted(SMOKE_DIR.glob("*.py")):
            offenders += vacuous_none_assertions(path.read_text(encoding="utf-8"), str(path))
        assert not offenders, (
            f"{offenders} assert `X is not None` on a value bound from list()/a literal, "
            "which cannot be None. The check has one possible answer and a reader takes "
            "it for a check. Assert something falsifiable, or drop it and say in the "
            "docstring that the absence of an exception IS the check. (core#992: "
            "`datasets = list(client.list_datasets(...)); assert datasets is not None`.)"
        )

    def test_the_detector_fires_on_the_shape_it_is_for(self) -> None:
        """The exact pre-fix BigQuery code. A detector never seen red proves nothing."""
        src = (
            "def test_bigquery_auth_and_list_datasets(require_env):\n"
            "    datasets = list(client.list_datasets(max_results=5))\n"
            "    assert datasets is not None\n"
        )
        assert vacuous_none_assertions(src) == [
            ("test_bigquery_auth_and_list_datasets", "datasets")
        ]

    def test_the_detector_fires_on_the_other_never_none_shapes(self) -> None:
        for expr in ("[]", "{}", "{1, 2}", "()", "sorted(x)", "len(x)", "[i for i in x]"):
            src = f"def test_x():\n    v = {expr}\n    assert v is not None\n"
            assert vacuous_none_assertions(src) == [("test_x", "v")], f"missed: {expr}"

    def test_the_detector_does_not_fire_on_a_real_check(self) -> None:
        """Narrowing it until it matches nothing is a worse bug, and a silent one."""
        for src in (
            # a value that genuinely may be None
            "def test_x():\n    v = d.get('k')\n    assert v is not None\n",
            # not bound in this function at all
            "def test_x(v):\n    assert v is not None\n",
            # a real comparison on a never-None value
            "def test_x():\n    v = list(y)\n    assert v == [1]\n",
            # emptiness IS falsifiable — this is the correct form and must survive
            "def test_x():\n    v = list(y)\n    assert v\n",
            # None on the left is still a real check against an unknown
            "def test_x():\n    assert d.attr is not None\n",
        ):
            assert vacuous_none_assertions(src) == [], f"false positive on: {src!r}"

    def test_the_detector_survives_a_mutation_of_the_real_file(self) -> None:
        """Re-inject the defect into the real BigQuery probe and confirm a red.

        Mutating the shipped artifact rather than a fixture: a synthetic control is
        written from the same mental model as the check and agrees with it,
        including where it is wrong.
        """
        path = SMOKE_DIR / "test_paid_connectors.py"
        real = path.read_text(encoding="utf-8")
        assert not vacuous_none_assertions(real, str(path)), "precondition: real file is clean"

        anchor = "    datasets = list(client.list_datasets(max_results=5))\n"
        assert real.count(anchor) == 1, (
            "anchor drift — the BigQuery probe no longer binds `datasets` from list() on "
            "its own line, so this mutation is not testing what it says it is."
        )
        mutated = real.replace(anchor, anchor + "    assert datasets is not None\n", 1)
        assert vacuous_none_assertions(mutated, str(path)) == [
            ("test_bigquery_auth_and_list_datasets", "datasets")
        ], "the detector did not notice the defect being put back into the real file."
