"""``autocommit_block()`` is unavailable to every migration in this tree (core#933).

The mechanism, reproduced here rather than reasoned about:
``run_migrations_online`` executes ``SET search_path`` on the connection **before** asking
alembic to begin a transaction. That statement autobegins a SQLAlchemy transaction, so
``MigrationContext.begin_transaction()`` sees it is already inside an external transaction,
returns a do-nothing context manager, and never assigns ``self._transaction``.
``autocommit_block()`` asserts on exactly that attribute — with **no message**, from inside
alembic, on a line copied verbatim out of alembic's own documentation.

Why it is a test and not a paragraph
------------------------------------
``docs/specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md`` prescribes the mechanism in three places
(*"it needs an autocommit block"*, *"backfill in batches with commits"*, and a checklist item
reading *"``CREATE INDEX`` concurrent where the table is non-trivial"*). A policy that binds
this department cannot prescribe something the runtime refuses, so the constraint has to be
stated beside each — and a stated constraint that nothing verifies is the shape this project
keeps finding. These tests are what make that text a **reading**.

🔑 **These tests assert a DEFECT, deliberately.** When ``test_autocommit_block_is_refused``
goes red, core#933 has been fixed and the three warnings in the spec, and the one in
``env.py``, are false and must come out. That is the intended way to find out.

⚠️ SQLite is sufficient and is not a compromise: autobegin is SQLAlchemy behaviour, not
Postgres behaviour, and the assertion is alembic's. The control below is what proves the
reproduction is about the pre-emptive statement rather than about the driver.
"""

from __future__ import annotations

import ast
import pathlib

import pytest
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine, text

_REPO = pathlib.Path(__file__).resolve().parents[2]
_ENV_PY = _REPO / "datanika" / "migrations" / "env.py"
_SPEC = _REPO / "docs" / "specs" / "SPEC_EXPAND_CONTRACT_MIGRATIONS.md"


def _enter_autocommit_block(*, execute_before_alembic_begins: bool) -> str:
    """Return ``"entered"`` or the exception that stopped us.

    Models ``env.py``'s two orderings against a real ``MigrationContext``.
    """
    engine = create_engine("sqlite://")
    with engine.connect() as connection:
        if execute_before_alembic_begins:
            # This is `SET search_path TO public`. Any statement will do — what
            # matters is that it autobegins a transaction on the connection.
            connection.execute(text("SELECT 1"))
        context = MigrationContext.configure(connection=connection)
        with context.begin_transaction():
            try:
                with context.autocommit_block():
                    pass
            except AssertionError:
                return "AssertionError"
            except Exception as exc:  # pragma: no cover - would be a new failure mode
                return type(exc).__name__
            return "entered"


class TestTheMechanism:
    def test_autocommit_block_is_refused(self):
        """core#933, as the deploy runs it.

        Goes red when #933 is fixed — at which point the warnings this test
        justifies are false and must be removed, not left as folklore.
        """
        assert _enter_autocommit_block(execute_before_alembic_begins=True) == "AssertionError", (
            "autocommit_block() now works with a statement executed before "
            "context.begin_transaction(). core#933 is fixed: delete the warnings in "
            "env.py and SPEC_EXPAND_CONTRACT_MIGRATIONS.md, and this test with them"
        )

    def test_it_works_when_alembic_owns_the_transaction(self):
        """The control, and it is the whole value of this file.

        Without it the test above passes for any reason — a driver that never
        supports autocommit blocks, an alembic that removed the feature, a
        typo in the context construction. Only the pair attributes the refusal
        to the pre-emptive statement.
        """
        assert _enter_autocommit_block(execute_before_alembic_begins=False) == "entered", (
            "autocommit_block() fails even when alembic owns the transaction, so the "
            "test above is not measuring core#933's mechanism"
        )


class TestTheCauseIsStillInEnvPy:
    """Derived from the real ``env.py``, so it goes red the day the ordering changes."""

    @staticmethod
    def _run_migrations_online() -> ast.FunctionDef:
        tree = ast.parse(_ENV_PY.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "run_migrations_online":
                return node
        pytest.fail(f"run_migrations_online() is gone from {_ENV_PY.name} — this reads nothing")

    def test_a_statement_is_executed_before_alembic_begins_a_transaction(self):
        """Anti-vacuity for the whole file: if this stops being true, the
        reproduction above is modelling an ordering the repo no longer has."""
        fn = self._run_migrations_online()

        first_execute = None
        first_begin = None
        for node in ast.walk(fn):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr == "execute" and first_execute is None:
                first_execute = node.lineno
            if node.func.attr == "begin_transaction" and first_begin is None:
                first_begin = node.lineno

        assert first_execute is not None, "nothing executes a statement in run_migrations_online"
        assert first_begin is not None, "nothing calls context.begin_transaction()"
        assert first_execute < first_begin, (
            "env.py no longer executes a statement before alembic begins its "
            f"transaction (execute at line {first_execute}, begin_transaction at "
            f"{first_begin}). If that was deliberate, core#933 may be fixed"
        )


class TestThePolicyDoesNotPrescribeTheUnavailable:
    """The spec binds Engineering. It must not tell an author to use this.

    Coverage, not phrase-counting: every place the document names the mechanism has to
    carry the issue reference nearby. A guard that merely counted a warning would go
    green on a document that warns once and prescribes three times.
    """

    _PRESCRIPTIONS = ("autocommit block", "autocommit_block", "CONCURRENTLY", "with commits")
    _WINDOW = 6

    def test_every_mention_of_the_mechanism_carries_the_constraint(self):
        lines = _SPEC.read_text(encoding="utf-8").splitlines()

        hits = [i for i, ln in enumerate(lines) if any(p in ln for p in self._PRESCRIPTIONS)]
        assert hits, (
            f"{_SPEC.name} no longer mentions the mechanism at all — this guard reads "
            "nothing. If the guidance was removed, remove this test too"
        )

        uncovered = [
            i + 1
            for i in hits
            if not any(
                "933" in ln
                for ln in lines[max(0, i - self._WINDOW) : i + self._WINDOW + 1]  # noqa: E203
            )
        ]
        assert not uncovered, (
            f"{_SPEC.name} prescribes an unavailable mechanism at line(s) {uncovered} "
            "without naming core#933 within "
            f"{self._WINDOW} lines. An author following it hits a bare AssertionError"
        )

    def test_env_py_states_it_where_the_cause_is(self):
        """The traceback names alembic, not us. The line that causes it is the
        only place a reader arrives at on their own."""
        source = _ENV_PY.read_text(encoding="utf-8")

        assert "933" in source, (
            "env.py does not name core#933 beside the statement that causes it, so the "
            "AssertionError stays unattributable from the traceback alone"
        )
