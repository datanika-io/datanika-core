"""``autocommit_block()`` is unavailable to every migration in this tree (core#933).

The mechanism, reproduced here rather than reasoned about: ``run_migrations_online``
executes ``SET search_path`` on the connection, which autobegins a SQLAlchemy transaction
alembic did not begin. ``autocommit_block()`` refuses that state with a **message-less**
``AssertionError``, from inside alembic, on a line copied verbatim out of alembic's own
documentation.

🔴 **CORRECTED 2026-09-07 (core#933). The mechanism this file used to state was wrong, and
it was wrong in the direction that certifies a non-fix.** It said the ``SET`` runs
*"**before** asking alembic to begin a transaction … so ``begin_transaction()`` … never
assigns ``self._transaction``, and ``autocommit_block()`` asserts on exactly that
attribute."* Measured against alembic 1.18.4:

Columns: statement placement · ``_in_connection_transaction()`` · ``_transaction`` ·
outcome.

* nothing executed at all ............... ``False`` · ``None`` · **entered**
* **before** ``configure`` (env.py today) ``True``  · ``None`` · ``AssertionError``
* **inside** ``begin_transaction()``
  — core#933's **option 1** ............. ``True``  · ``None`` · ``AssertionError``
* both places .......................... ``True``  · ``None`` · ``AssertionError``

🔑 **``_transaction`` is ``None`` in every row, including the one that works**, so it cannot
be the discriminator. The real guard is the line above the assertion::

    _in_connection_transaction = self._in_connection_transaction()
    ...
    elif _in_connection_transaction:
        assert self._transaction is not None

The assertion is *reached* only when the connection already carries a transaction alembic
did not begin. **The property is "no statement may touch this connection at all", not "the
statement must come after `begin_transaction()`".**

🚨 **Why the old wording was dangerous rather than merely imprecise.** It points at
*ordering*, which makes core#933's option 1 — *"move `SET search_path` inside
`context.begin_transaction()`"* — look like the fix. It is not: row 3 above. And this
file's own ``env.py`` check used to assert that ordering, so on an option-1 tree it failed
with *"env.py no longer executes a statement before alembic begins its transaction. If that
was deliberate, core#933 may be fixed."* **Measured: it says exactly that while
``autocommit_block()`` is still refused** — the runtime arm and the source arm give opposite
readings, and the human-legible one is the wrong one. The source check below now asserts
the real condition.

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


def _enter_autocommit_block(
    *,
    execute_before_alembic_begins: bool = False,
    execute_inside_alembics_transaction: bool = False,
) -> tuple[str, bool, bool]:
    """``(outcome, in_connection_transaction, alembic_owns_a_transaction)``.

    Models every placement ``env.py`` could use, against a real ``MigrationContext``.
    The two booleans are returned so the discriminator can be *asserted* rather than
    inferred from alembic's source — reading source tells you what should happen, and
    this file exists because what should happen and what does have already diverged once.
    """
    engine = create_engine("sqlite://")
    with engine.connect() as connection:
        if execute_before_alembic_begins:
            # This is `SET search_path TO public`. Any statement will do — what
            # matters is that it autobegins a transaction on the connection.
            connection.execute(text("SELECT 1"))
        context = MigrationContext.configure(connection=connection)
        with context.begin_transaction():
            if execute_inside_alembics_transaction:
                # core#933's option 1, as the issue words it.
                connection.execute(text("SELECT 1"))
            in_conn = context._in_connection_transaction()
            owns = context._transaction is not None
            try:
                with context.autocommit_block():
                    pass
            except AssertionError:
                return "AssertionError", in_conn, owns
            except Exception as exc:  # pragma: no cover - would be a new failure mode
                return type(exc).__name__, in_conn, owns
            return "entered", in_conn, owns


class TestTheMechanism:
    def test_autocommit_block_is_refused(self):
        """core#933, as the deploy runs it.

        Goes red when #933 is fixed — at which point the warnings this test
        justifies are false and must be removed, not left as folklore.
        """
        outcome, _, _ = _enter_autocommit_block(execute_before_alembic_begins=True)
        assert outcome == "AssertionError", (
            "autocommit_block() now works with a statement executed before "
            "context.begin_transaction(). core#933 is fixed: delete the warnings in "
            "env.py and SPEC_EXPAND_CONTRACT_MIGRATIONS.md, and this test with them"
        )

    def test_it_works_when_nothing_has_touched_the_connection(self):
        """The control, and it is the whole value of this file.

        Without it the test above passes for any reason — a driver that never supports
        autocommit blocks, an alembic that removed the feature, a typo in the context
        construction. Only the pair attributes the refusal to the statement.

        🔴 **Renamed 2026-09-07.** It was
        ``test_it_works_when_alembic_owns_the_transaction``, and alembic does **not** own a
        transaction in this arm — ``_transaction`` is ``None`` here exactly as it is in the
        failing arms, which the assertion below now pins. The old name encoded the theory
        the module docstring has just corrected, and a control whose *name* asserts the
        wrong mechanism is how that mechanism survives being disproved.
        """
        outcome, in_conn, owns = _enter_autocommit_block()
        assert outcome == "entered", (
            "autocommit_block() fails even with nothing executed on the connection, so "
            "the test above is not measuring core#933's mechanism"
        )
        assert in_conn is False, "nothing was executed, so the connection has no transaction"
        assert owns is False, (
            "alembic assigned _transaction in the WORKING arm — if that is now true, the "
            "old 'alembic owns the transaction' theory has become correct and the module "
            "docstring's table needs re-measuring"
        )

    def test_moving_the_statement_inside_alembics_transaction_does_not_help(self):
        """🚨 core#933's **option 1**, refuted — the option the issue lists first.

        *"Move `SET search_path` inside `context.begin_transaction()` so alembic owns the
        transaction"* does not work: the statement autobegins a connection transaction just
        the same, whichever side of ``begin_transaction()`` it sits on.

        This exists so the next person to attempt AC1 learns it from a red test in seconds
        rather than from a bare ``AssertionError`` in a deploy. **Only option 2 — never
        executing anything on that connection, e.g. a search path set through connect args
        — can satisfy the real condition.**
        """
        outcome, in_conn, owns = _enter_autocommit_block(execute_inside_alembics_transaction=True)
        assert outcome == "AssertionError", (
            "option 1 now works: moving the statement inside context.begin_transaction() "
            "lets autocommit_block() run. Re-measure the table in this module's docstring "
            "before acting on it — it says this arm refuses"
        )
        assert (in_conn, owns) == (True, False), (
            "option 1 fails for a different reason than measured "
            f"(in_connection_transaction={in_conn}, alembic_owns={owns})"
        )

    def test_the_discriminator_is_the_connections_transaction_not_alembics(self):
        """Pins the corrected mechanism, so it cannot quietly revert to the old story.

        ``_transaction`` is ``None`` in **every** arm; what varies is whether the
        *connection* already carries a transaction. Asserted as a pair across all three
        placements, because either half alone is satisfied by the wrong theory.
        """
        arms = {
            "nothing executed": _enter_autocommit_block(),
            "before configure": _enter_autocommit_block(execute_before_alembic_begins=True),
            "inside transaction": _enter_autocommit_block(execute_inside_alembics_transaction=True),
        }
        assert {k: v[2] for k, v in arms.items()} == dict.fromkeys(arms, False), (
            f"alembic assigned _transaction somewhere: {arms}"
        )
        assert {k: (v[1], v[0] == "entered") for k, v in arms.items()} == {
            "nothing executed": (False, True),
            "before configure": (True, False),
            "inside transaction": (True, False),
        }, f"the discriminator moved: {arms}"


class TestTheCauseIsStillInEnvPy:
    """Derived from the real ``env.py``, so it goes red the day the ordering changes."""

    @staticmethod
    def _run_migrations_online() -> ast.FunctionDef:
        tree = ast.parse(_ENV_PY.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "run_migrations_online":
                return node
        pytest.fail(f"run_migrations_online() is gone from {_ENV_PY.name} — this reads nothing")

    def test_a_statement_touches_the_connection_at_all(self):
        """Anti-vacuity for the whole file: if this stops being true, the reproduction
        above is modelling a repo that no longer exists.

        🔴 **REWRITTEN 2026-09-07 (core#933). This asserted the ORDERING** —
        ``first_execute < first_begin`` — *"env.py no longer executes a statement before
        alembic begins its transaction … If that was deliberate, core#933 may be fixed."*

        🚨 **Measured: on a tree with the issue's option 1 applied, it says exactly that,
        while ``autocommit_block()`` is still refused.** So this file's two arms gave
        opposite readings of the same tree and the human-legible one was wrong — the guard
        would have certified a non-fix. That is the failure this whole file exists to
        prevent, occurring inside the file itself.

        The real condition is that **any** statement is executed on the connection, on
        either side of ``begin_transaction()``. Ordering is irrelevant; see the table in
        the module docstring.
        """
        fn = self._run_migrations_online()

        executes = [
            node.lineno
            for node in ast.walk(fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute"
        ]
        begins = [
            node.lineno
            for node in ast.walk(fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "begin_transaction"
        ]

        assert begins, "nothing calls context.begin_transaction() — this guard reads nothing"
        assert executes, (
            "run_migrations_online() no longer executes ANY statement on the connection "
            "(context.begin_transaction() at line(s) "
            f"{begins}). That is the one change that could actually fix core#933 — "
            "verify autocommit_block() against a real Postgres, then delete the warnings "
            "in env.py and SPEC_EXPAND_CONTRACT_MIGRATIONS.md, and this file with them. "
            "⚠️ Moving the statement inside begin_transaction() is NOT that change and "
            "does not fix it: see "
            "test_moving_the_statement_inside_alembics_transaction_does_not_help"
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
