"""No test resolves a repository path relative to the **current working directory**.

🚨 **Why this exists.** `datanika-cloud`'s `tests/test_usage_is_visible.py` read
``Path("datanika_cloud/ui/pages/billing.py")``. That resolves only when pytest runs from the
cloud repo root — which cloud's own CI does, so it was **green there for a full day**. Core's
`cloud-suite` job runs ``pytest cloud/tests`` from the *parent* directory, where the same line
raised ``FileNotFoundError``; and because `cloud-suite` is a **required check on core's `dev`**,
it ejected core PRs from the merge queue with `reason: failed_checks` and **nothing red on the PR
itself**. (cloud#258, fixed in cloud#260.)

Core had **twelve** instances of the same shape (core#1551). They were **latent rather than
live** — core's suite is only ever invoked from the core repo root — which is precisely why a
guard is the right artifact: nothing in core's own CI can go red on them, so the next one would
have arrived the same way, unseen, and the day it became live it would have been somebody else's
gate that broke.

🔑 **Two things this guard does that a straight port of cloud's would not, both re-derived against
core's tree rather than inherited (core#1551 AC3):**

1. **A repo path with no separator is still a repo path.** Cloud's predicate required a ``/`` in
   the literal. Core's twelfth site was ``SRC = Path("datanika")`` — the *root* of five derived
   paths in one file — and a straight port would have reported it clean. That is why this file
   ships its own controls: a ported guard inherits the source's blind spots, not its lessons.
2. **A dotted top-level entry is still a repo entry.** Cloud's predicate dropped every value
   starting with ``.``; core has ``.github/``, which its deploy tests read constantly.

⚠️ **Known gap, stated so it is not discovered later.** ``Path(".")`` and ``Path("..")`` are
cwd-relative and are deliberately *not* flagged: inside a ``monkeypatch.chdir(tmp_path)`` block
they are legitimate, and this guard cannot tell the two apart from the AST alone. Measured at **0
occurrences** across core's test tree when this was written. Likewise ``subprocess(..., cwd=…)``
is out of scope.

⚠️ **Placement is deliberate and is the other core-specific re-derivation.** Cloud's copy lives at
``tests/`` because cloud's pre-push hook runs its whole suite. Core's hook runs **only**
``tests/test_deploy/`` (core#964), so the same placement in core would mean this guard fired in CI
and never before a push — and the cost this guard exists to prevent is paid at push time, by other
departments.
"""

from __future__ import annotations

import ast
import os
import pathlib

REPO = pathlib.Path(__file__).resolve().parents[2]
TESTS = REPO / "tests"

#: Directories that only exist after something has *run* — 0 tracked files each, measured.
#: They are unioned in explicitly rather than left to the disk scan below, because on a fresh
#: checkout (CI, a new worktree) they are **absent**, and a set measured from disk alone would
#: quietly stop flagging paths into them there while still flagging them on a developer box.
#: An instrument whose sensitivity depends on whether the machine has ever run a pipeline
#: reports "clean" for a different population in each place it runs.
_RUNTIME_DIRS = frozenset({"dbt_packages", "dlt_pipelines", "uploaded_files"})


def _disk_names() -> set[str]:
    """What a listing of the repository root actually shows, here and now."""
    return {p.name for p in REPO.iterdir()}


def _top_level(names) -> set[str]:
    """The construction, kept as a function so a control can drive it with a listing that is
    not this machine's — the runtime directories exist here, so a control that only reads
    ``TOP_LEVEL`` would pass whether or not the union below survives."""
    return set(names) | set(_RUNTIME_DIRS)


#: Every top-level name, **measured from the repository** so the check follows the tree rather
#: than a list that rots. Dotted entries are kept on purpose (``.github/``); files are kept too,
#: so ``open("pyproject.toml")`` is caught as readily as ``open("docs/x.md")``.
TOP_LEVEL = _top_level(_disk_names())

#: Pruned when looking for other Python test trees. Heavy, vendored, or generated — none of
#: them can contain a test tree this repo is responsible for.
_UNWALKABLE = frozenset(
    {
        ".venv",
        ".web",
        ".git",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".scratch",
        ".states",
        ".mutation-probe-state",
        *_RUNTIME_DIRS,
    }
)


def _is_repo_relative(value: str, top: set[str] | None = None) -> bool:
    """True when the literal names something inside this repository, as read from the cwd.

    ``top`` is injectable so a control can drive the predicate with a *different* repository
    listing — specifically, one in which nothing has ever run.
    """
    top = TOP_LEVEL if top is None else top
    if not value or "://" in value or value.startswith(("/", "http", "\\")):
        return False
    if len(value) > 1 and value[1] == ":":  # a Windows drive letter is absolute
        return False
    head = value.split("/", 1)[0]
    return head in top


def _resolver_name(node: ast.Call) -> str | None:
    """The name of a call that turns a string into a filesystem location.

    ``Path``/``pathlib.Path`` in either form, and the **builtin** ``open`` only.

    ⚠️ ``read_text``/``read_bytes`` are deliberately absent, and so is the *method* ``x.open``:
    their first positional argument is a mode or an encoding, never a path. Cloud's copy lists
    them, and core really does contain ``p.read_text("utf-8")`` and ``p.open("rb")`` — eight
    sites. Cloud is saved there only by its predicate happening to reject ``"utf-8"``, which is
    luck rather than design, and luck stops working the day somebody adds a top-level file
    called ``rb``.
    """
    func = node.func
    if isinstance(func, ast.Attribute):
        return "Path" if func.attr == "Path" else None
    if isinstance(func, ast.Name) and func.id in {"Path", "open"}:
        return func.id
    return None


def _unanchored_calls(tree: ast.AST) -> list[tuple[int, str]]:
    """``Path("<repo path>")`` / ``open("<repo path>")`` with no ``__file__`` in the call."""
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        if _resolver_name(node) is None:
            continue
        first = node.args[0]
        if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
            continue
        if not _is_repo_relative(first.value):
            continue
        # The anchored forms all put __file__ inside the same call:
        #   Path(__file__).resolve().parents[2] / "datanika/i18n"
        #   open(Path(__file__).parent / "datanika/x.py")
        anchored = any(isinstance(n, ast.Name) and n.id == "__file__" for n in ast.walk(node))
        if not anchored:
            out.append((node.lineno, first.value))
    return out


def _test_files() -> list[pathlib.Path]:
    return sorted(TESTS.rglob("*.py"))


def _python_test_trees() -> list[pathlib.Path]:
    """Every directory named ``tests`` under the repo that contains Python, pruned."""
    found: list[pathlib.Path] = []
    for root, dirnames, filenames in os.walk(REPO):
        dirnames[:] = [d for d in dirnames if d not in _UNWALKABLE]
        here = pathlib.Path(root)
        if here.name == "tests" and any(f.endswith(".py") for f in filenames):
            found.append(here)
    return sorted(found)


class TestTheInstrumentCanSee:
    """Coverage and sensitivity are different properties. Prove both, separately.

    A guard over an empty population is green, and reads exactly like a clean tree.
    """

    def test_the_walk_finds_what_a_second_independent_walker_finds(self):
        """Anti-vacuity, with the expected side derived from something the subject cannot move.

        ``_test_files`` is ``Path.rglob``; the oracle below is ``os.walk`` plus a string test —
        a different implementation reaching the same files. Computing the expectation *with* the
        subject is the third door into the vacuity room (WORKFLOW_RULES §4): it would be
        satisfied by the subject returning nothing.

        ⚠️ The oracle is per-**file**, not per-directory. An earlier version of this assertion
        expected every subdirectory of ``tests/`` to be reached and went red on ``tests/load``
        (a k6 script) and ``tests/fixtures`` (data) — which contain no Python at all. A control
        that refuses a correct tree invites exactly the repair that loosens it into silence.
        """
        seen = {p.relative_to(TESTS).as_posix() for p in _test_files()}
        oracle: set[str] = set()
        for root, _dirnames, filenames in os.walk(TESTS):
            for name in filenames:
                if name.endswith(".py"):
                    oracle.add((pathlib.Path(root) / name).relative_to(TESTS).as_posix())
        assert oracle, "the oracle found no Python under tests/ — the oracle is broken"
        assert seen == oracle, (
            f"the walk missed {sorted(oracle - seen)!r} and invented {sorted(seen - oracle)!r}"
        )

    def test_tests_is_the_only_python_test_tree_in_the_repo(self):
        """Coverage, not sensitivity: a second tree would be graded by nobody.

        ``e2e/tests`` is TypeScript and ``datanika-mcp`` ships none, so ``tests/`` really is the
        whole population — asserted rather than assumed, because the day that stops being true
        this guard starts reporting a subset as the whole.
        """
        trees = _python_test_trees()
        assert trees == [TESTS], f"unwatched Python test trees: {[str(t) for t in trees]}"

    def test_the_top_level_names_were_measured_not_typed(self):
        for name in ("datanika", "tests", "docs", "scripts", "deploy", "e2e", ".github"):
            assert name in TOP_LEVEL, f"{name} missing from a set that is supposed to be measured"
        # A name no list ported from datanika-cloud could contain.
        assert "datanika-mcp" in TOP_LEVEL

    def test_a_fresh_checkout_still_flags_paths_into_a_runtime_directory(self):
        """Both halves, because on *this* machine the runtime directories exist.

        ``assert _RUNTIME_DIRS <= TOP_LEVEL`` would pass here with the union deleted — the
        directories are on disk. So drive the predicate with the listing a checkout that has
        never run a pipeline would produce, and require the two to answer **differently**.
        """
        fresh_listing = {n for n in _disk_names() if n not in _RUNTIME_DIRS}
        assert not _is_repo_relative("dlt_pipelines/x.jsonl", fresh_listing), (
            "the negative half is not negative — this machine's listing still has it"
        )
        assert _is_repo_relative("dlt_pipelines/x.jsonl", _top_level(fresh_listing)), (
            "_RUNTIME_DIRS is no longer unioned in: on a fresh checkout this guard would "
            "silently stop flagging paths into directories that only exist after a run"
        )

    def test_it_flags_the_exact_shape_that_blocked_the_merge_queue(self):
        bad = ast.parse('Path("datanika/i18n")')
        assert _unanchored_calls(bad) == [(1, "datanika/i18n")]

    def test_it_flags_a_top_level_directory_named_with_no_separator(self):
        """🔑 The re-derivation. core#1551's census said eleven sites; there are twelve.

        The twelfth was ``SRC = Path("datanika")`` in
        ``tests/test_ui/test_login_signal_coverage.py`` — the root of five derived paths in that
        file. Cloud's predicate begins ``if "/" not in value: return False``, so **the census
        that produced the issue could not see it**, and a guard ported unchanged would have gone
        green over the one site still broken.
        """
        assert _unanchored_calls(ast.parse('Path("datanika")')) == [(1, "datanika")]

    def test_it_flags_a_dotted_repo_entry(self):
        """Cloud drops every value starting with ``.``; core's deploy tests read ``.github/``."""
        src = 'open(".github/workflows/ci.yml")'
        assert _unanchored_calls(ast.parse(src)) == [(1, ".github/workflows/ci.yml")]

    def test_it_flags_a_bare_top_level_file(self):
        assert _unanchored_calls(ast.parse('open("pyproject.toml")')) == [(1, "pyproject.toml")]

    def test_it_does_not_flag_the_anchored_forms(self):
        good = ast.parse('Path(__file__).resolve().parents[2] / "datanika/i18n"')
        assert _unanchored_calls(good) == []
        also = ast.parse('open(Path(__file__).parent / "datanika/x.py")')
        assert _unanchored_calls(also) == []

    def test_a_mode_or_an_encoding_is_not_treated_as_a_path(self):
        """Pins the **resolver decision**, because the end verdict cannot discriminate.

        ⚠️ The obvious form of this control — ``_unanchored_calls(ast.parse('p.open("rb")'))
        == []`` — passes whether or not ``read_text``/``x.open`` are in the resolver set,
        because ``_is_repo_relative`` rejects ``"rb"`` and ``"utf-8"`` as names anyway. Two
        independent reasons produce the same empty list, so the assertion cannot fail in the
        direction of its own conclusion and would go on passing the day the set is widened.
        (Core has eight such calls; cloud's copy lists both methods and is saved only by that
        coincidence.) Asserting ``_resolver_name`` directly is the version that reds.
        """
        for src, expected in (
            ('p.open("rb")', None),
            ('p.read_text("utf-8")', None),
            ("p.read_bytes()", None),
            ('Path("datanika")', "Path"),
            ('pathlib.Path("datanika")', "Path"),
            ('open("datanika/x")', "open"),
        ):
            node = ast.parse(src).body[0].value
            assert _resolver_name(node) == expected, src

    def test_it_does_not_flag_things_that_are_not_repo_paths(self):
        for src in (
            'Path("https://example.com/x")',
            'open("some_file.txt")',
            'Path("/absolute/path")',
            'Path("D:/Projects/Datanika/x")',
            'Path("not_a_top_level_dir/x.py")',
            'Path("")',
        ):
            assert _unanchored_calls(ast.parse(src)) == [], src


class TestEveryPathIsAnchored:
    def test_no_test_resolves_a_repo_path_against_the_cwd(self):
        offenders = []
        for path in _test_files():
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for line, value in _unanchored_calls(tree):
                offenders.append(f"{path.relative_to(REPO).as_posix()}:{line}  {value!r}")
        assert not offenders, (
            "repo paths resolved against the current working directory:\n  "
            + "\n  ".join(offenders)
            + "\n\nThese pass from the repo root and mean something else from anywhere else. "
            "The failure is not always loud: `Path.glob` on a missing directory yields nothing "
            "rather than raising, so an assertion shaped `assert offenders == []` PASSES while "
            "grading an empty population (measured on "
            "test_refusals_are_visible.py::test_every_locale_carries_the_key, core#1551).\n"
            'Anchor it: Path(__file__).resolve().parents[2] / "datanika/...".'
        )
