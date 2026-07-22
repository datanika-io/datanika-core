"""Fixtures are shared through `conftest.py`, never by importing them.

`from tests.test_migrations.test_roundtrip import roundtrip_db_url` looks like
sharing and isn't. pytest registers an imported fixture as a **separate
FixtureDef in the importing module**, so `scope="session"` caches once per
module instead of once per session. The fixture body runs twice.

That cost a real suite: two Postgres testcontainers instead of one, two
teardowns, and on Windows the second teardown timed out against the Docker
named pipe — failing an otherwise green run (`testsfailed=0`,
`exitstatus=OK`) in TEARDOWN and leaving containers orphaned. Nothing asserted
against it; it surfaced only because the timeout happened to fire.

A fixture in `conftest.py` is discovered by every module in its directory as
one FixtureDef. Helper *functions* may still be imported freely — this only
concerns `@pytest.fixture`.
"""

from __future__ import annotations

import ast
from pathlib import Path

#: This file sits at the root of the test tree, alongside the other repo-wide
#: contract scans (`test_dependency_pins.py`, `test_hooks_contract.py`).
TESTS_ROOT = Path(__file__).parent


def _fixture_names(tree: ast.AST) -> set[str]:
    """Names defined with an @pytest.fixture decorator, bare or called."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for dec in node.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            if getattr(target, "attr", getattr(target, "id", None)) == "fixture":
                names.add(node.name)
    return names


def _fixture_imports(tree: ast.AST, known: set[str]) -> list[str]:
    """`from tests... import <a fixture name>` — the pattern that duplicates."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not (node.module or "").startswith("tests"):
            continue
        found.extend(f"{node.module}.{a.name}" for a in node.names if a.name in known)
    return found


def _parsed_test_modules() -> dict[Path, ast.AST]:
    return {
        path: ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for path in sorted(TESTS_ROOT.rglob("*.py"))
    }


class TestFixturesAreSharedViaConftest:
    def test_the_scan_sees_the_test_tree(self):
        """Anti-vacuity: an empty scan would pass having checked nothing."""
        modules = _parsed_test_modules()
        assert len(modules) >= 50, f"only {len(modules)} test modules found under {TESTS_ROOT}"

        known: set[str] = set()
        for tree in modules.values():
            known |= _fixture_names(tree)
        assert len(known) >= 20, (
            f"only {len(known)} fixtures detected — the decorator scan is probably "
            "not matching, which would make the check below vacuously green"
        )

    def test_no_test_module_imports_a_fixture_from_another_test_module(self):
        modules = _parsed_test_modules()
        known: set[str] = set()
        for tree in modules.values():
            known |= _fixture_names(tree)

        offenders = {
            str(path.relative_to(TESTS_ROOT)): imports
            for path, tree in modules.items()
            if (imports := _fixture_imports(tree, known))
        }
        assert not offenders, (
            f"Fixtures imported across test modules: {offenders}.\n"
            "pytest registers an imported fixture as a second FixtureDef, so a "
            "session- or module-scoped fixture runs its body once per importing "
            "module. Move it to a conftest.py in the nearest shared directory; "
            "plain helper functions are fine to import."
        )


class TestTheGuardDiscriminates:
    """A scan that cannot fail is decoration — prove it flags the real shape."""

    def test_an_imported_fixture_is_flagged(self):
        provider = ast.parse(
            "import pytest\n@pytest.fixture(scope='session')\ndef shared_db():\n    yield 1\n"
        )
        consumer = ast.parse("from tests.test_migrations.test_roundtrip import shared_db\n")

        known = _fixture_names(provider)
        assert known == {"shared_db"}
        assert _fixture_imports(consumer, known) == [
            "tests.test_migrations.test_roundtrip.shared_db"
        ]

    def test_importing_a_plain_helper_is_not_flagged(self):
        provider = ast.parse("def _run_alembic(cmd, url):\n    return None\n")
        consumer = ast.parse("from tests.test_migrations.conftest import _run_alembic\n")

        assert _fixture_names(provider) == set()
        assert _fixture_imports(consumer, _fixture_names(provider)) == []
