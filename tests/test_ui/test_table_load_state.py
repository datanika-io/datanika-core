"""core#872 AC2 — a table that has not loaded must not look like an empty one.

## The defect

Measured on production 2026-08-31: `/connections` and `/models` render **zero
rows for 5–17 seconds** after navigation while the websocket data arrives. An
empty table and a still-loading table are pixel-identical, and `/models` goes
further — it renders its *"no models yet"* callout immediately, so a user who
has data is told, in words, that they have none.

That is the load-bearing half of core#872, and it is load-bearing because of
**core#869**: one `/models` poll stayed empty for a full 30 s and *that* one was
a real emptiness. Until the two are distinguishable, an honest empty cannot be
read as honest — which is exactly what #869 needed and did not have.

It is also why core#872's create-feedback half is not sufficient on its own. The
user's recovery action for "nothing happened" is *repeating the mutation*, and
since connection-quota enforcement went live the second click is the one that
gets refused while the first silently succeeded.

## The shape of the fix, and what this file pins

A **tri-state**: not-loaded / loaded-and-empty / loaded-with-rows. Each table's
state class carries a `*_loaded` flag defaulting to `False`, its loader sets the
flag `True`, and the page branches on it before showing either rows or an
empty-state message.

`TABLES` below is the register. Every entry is resolved against the real source
by AST, and `PENDING` names the tables not yet converted — so the remainder is
**visible and counted** rather than quietly forgotten. A hand-maintained list
coupled to nothing goes stale silently; this one fails when it stops describing
the tree in either direction.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PAGES = REPO_ROOT / "datanika" / "ui" / "pages"
STATES = REPO_ROOT / "datanika" / "ui" / "state"


@dataclass(frozen=True)
class Table:
    page: str
    state_module: str
    state_class: str
    #: The list var whose emptiness is ambiguous before the load.
    rows_var: str
    #: The tri-state flag that disambiguates it.
    flag: str
    #: The loader that must set the flag.
    loader: str


TABLES: tuple[Table, ...] = (
    Table(
        "connections.py",
        "connection_state.py",
        "ConnectionState",
        "connections",
        "connections_loaded",
        "load_connections",
    ),
    Table("models.py", "model_state.py", "ModelState", "models", "models_loaded", "load_models"),
)

#: Tables that still render an ambiguous empty. Converting one means moving it
#: up into TABLES, not deleting it from here.
#:
#: These were NOT measured in core#872 — the issue observed `/connections` and
#: `/models` and says so explicitly. They are listed because the same shape is
#: visible in their source, not because anyone has watched them misreport.
PENDING: frozenset[str] = frozenset(
    {
        "api_keys.py",
        "audit_logs.py",
        "dag.py",
        "dashboard.py",
        "model_detail.py",
        "pipelines.py",
        "runs.py",
        "schedules.py",
        "settings.py",
        "transformations.py",
        "uploads.py",
    }
)


# --------------------------------------------------------------------------- #
# Derivations — AST, never import: importing a page pulls in Reflex component
# construction, and this guard has no opinion about that.
# --------------------------------------------------------------------------- #


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def class_def(path: Path, name: str) -> ast.ClassDef:
    for node in _tree(path).body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {path}")


def annotated_default(cls: ast.ClassDef, attr: str):
    """``attr: bool = False`` -> the default node, or None if unannotated."""
    for node in cls.body:
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == attr
        ):
            return node.value
    return None


def assigns_true(cls: ast.ClassDef, method: str, attr: str) -> bool:
    """Does ``method`` contain ``self.<attr> = True``?"""
    for node in cls.body:
        if not (isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == method):
            continue
        for stmt in ast.walk(node):
            if not isinstance(stmt, ast.Assign):
                continue
            if not (isinstance(stmt.value, ast.Constant) and stmt.value.value is True):
                continue
            for t in stmt.targets:
                if isinstance(t, ast.Attribute) and t.attr == attr:
                    return True
    return False


def pages_rendering_a_table() -> set[str]:
    """Pages whose source builds a table body from a state list."""
    out: set[str] = set()
    for path in sorted(PAGES.glob("*.py")):
        src = path.read_text(encoding="utf-8")
        if "rx.table.body" in src and "rx.foreach" in src:
            out.add(path.name)
    return out


# --------------------------------------------------------------------------- #
# Arming
# --------------------------------------------------------------------------- #


class TestTheDerivationIsArmed:
    def test_the_scan_finds_the_table_pages(self) -> None:
        found = pages_rendering_a_table()
        assert len(found) >= 10, (
            f"Only {len(found)} table pages found under {PAGES}. Every assertion below "
            "is derived from that set, so a truncated read makes them pass vacuously."
        )

    def test_the_register_is_not_empty(self) -> None:
        assert TABLES, "TABLES is empty — every per-table check iterates over nothing."

    def test_the_attribute_reader_can_come_back_empty(self, tmp_path: Path) -> None:
        """Negative control: the AST helpers must be able to report absence."""
        p = tmp_path / "x.py"
        p.write_text("class S:\n    other: bool = False\n", encoding="utf-8")
        cls = class_def(p, "S")
        assert annotated_default(cls, "missing_flag") is None
        assert annotated_default(cls, "other") is not None

    def test_the_true_assignment_detector_discriminates(self, tmp_path: Path) -> None:
        p = tmp_path / "y.py"
        p.write_text(
            "class S:\n"
            "    def sets(self):\n        self.flag = True\n"
            "    def clears(self):\n        self.flag = False\n"
            "    def other(self):\n        self.unrelated = True\n",
            encoding="utf-8",
        )
        cls = class_def(p, "S")
        assert assigns_true(cls, "sets", "flag") is True
        assert assigns_true(cls, "clears", "flag") is False, "False must not read as True"
        assert assigns_true(cls, "other", "flag") is False, "a different attr must not count"


# --------------------------------------------------------------------------- #
# The tri-state itself
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("table", TABLES, ids=lambda t: t.state_class)
class TestEachConvertedTableHasATriState:
    def test_the_flag_exists_and_defaults_to_not_loaded(self, table: Table) -> None:
        cls = class_def(STATES / table.state_module, table.state_class)
        default = annotated_default(cls, table.flag)
        assert default is not None, (
            f"{table.state_class}.{table.flag} is not declared. Without it the page "
            f"cannot tell an empty {table.rows_var} from one that has not arrived."
        )
        assert isinstance(default, ast.Constant) and default.value is False, (
            f"{table.state_class}.{table.flag} must default to False. Defaulting to True "
            "means the very first render — the one the user actually waits through — "
            "claims the data has loaded, which is the bug with extra steps."
        )

    def test_the_rows_var_still_exists(self, table: Table) -> None:
        """The flag is only meaningful next to the list it disambiguates."""
        cls = class_def(STATES / table.state_module, table.state_class)
        assert annotated_default(cls, table.rows_var) is not None, (
            f"{table.state_class}.{table.rows_var} is gone, so this register entry no "
            "longer describes anything."
        )

    def test_the_loader_sets_the_flag(self, table: Table) -> None:
        cls = class_def(STATES / table.state_module, table.state_class)
        assert assigns_true(cls, table.loader, table.flag), (
            f"{table.state_class}.{table.loader} never sets self.{table.flag} = True. "
            "A flag that is declared and never set leaves the page permanently in the "
            "loading state — which is a different bug, not a fix."
        )

    def test_the_page_branches_on_the_flag(self, table: Table) -> None:
        src = (PAGES / table.page).read_text(encoding="utf-8")
        assert f"{table.state_class}.{table.flag}" in src, (
            f"{table.page} never reads {table.state_class}.{table.flag}. The state "
            "carries the tri-state and the page still renders the ambiguous empty, so "
            "nothing the user sees has changed."
        )


# --------------------------------------------------------------------------- #
# The register must keep describing the tree
# --------------------------------------------------------------------------- #


class TestTheRegisterStaysCoupledToReality:
    def test_every_table_page_is_accounted_for(self) -> None:
        """Neither converted nor pending is not an option — that is how one is missed."""
        known = {t.page for t in TABLES} | PENDING
        unaccounted = sorted(pages_rendering_a_table() - known)
        assert not unaccounted, (
            f"{unaccounted} render a table from a state list but appear in neither TABLES "
            "nor PENDING. A new table page inherits core#872's defect by default: its "
            "empty body is indistinguishable from one still loading. Convert it, or add "
            "it to PENDING with your eyes open."
        )

    def test_pending_names_only_real_pages(self) -> None:
        missing = sorted(PENDING - {p.name for p in PAGES.glob("*.py")})
        assert not missing, (
            f"PENDING names {missing}, which no longer exist. An entry naming a deleted "
            "page stops covering anything AND stops being checked."
        )

    def test_a_converted_page_is_not_also_pending(self) -> None:
        overlap = sorted({t.page for t in TABLES} & PENDING)
        assert not overlap, (
            f"{overlap} are both converted and pending. Converting a table means moving "
            "it from PENDING into TABLES, not copying it."
        )

    def test_the_accounting_check_can_fail(self) -> None:
        """Mutate the real derived set, not a fixture written from the same idea."""
        known = {t.page for t in TABLES} | PENDING
        polluted = pages_rendering_a_table() | {"a_new_table_page.py"}
        assert sorted(polluted - known) == ["a_new_table_page.py"], (
            "Adding an unregistered page did not make the accounting disagree, so the "
            "check is not reading what it claims to."
        )
