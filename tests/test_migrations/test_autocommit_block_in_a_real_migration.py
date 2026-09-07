"""``autocommit_block()`` works in a real migration, against a real Postgres (core#933).

This is core#933 **AC1**. It replaces ``test_autocommit_block_availability.py``, which
asserted the *defect* and instructed its own deletion the day the defect went away.

What broke, in one paragraph
----------------------------
``run_migrations_online()`` executed ``SET search_path TO public`` on the connection before
handing it to alembic. Any statement there autobegins a SQLAlchemy transaction alembic did
not begin; ``MigrationContext.begin_transaction()`` then returns a do-nothing context
manager **without assigning** ``self._transaction``, and ``autocommit_block()`` asserts on
exactly that — a bare, message-less ``AssertionError`` from inside alembic, on a line copied
verbatim out of alembic's own documentation. So ``CREATE INDEX CONCURRENTLY``,
``ALTER TYPE ... ADD VALUE`` and commit-between-batches were unavailable to every migration
in this repo. Fixed by delivering the search path in libpq's startup packet
(``options=-csearch_path=...``) so nothing is executed at all.

Why this test is shaped the way it is
-------------------------------------
🔑 **Three cheaper shapes were rejected, and each one passes while proving nothing.**

* *"Construct a ``MigrationContext`` and enter the block."* That is what the old file did,
  and it is what got the mechanism wrong twice — see the SQLite section below. It never
  touches ``env.py``, so it cannot tell you whether the repo's own migration entrypoint is
  fixed.
* *"Assert ``env.py`` executes no statement."* A source-shape assertion is satisfied by any
  refactor that moves the statement somewhere this parser does not look, and says nothing
  about whether the block actually works.
* *"Run the migration and assert alembic exited 0."* A migration whose body silently did
  nothing also exits 0. The index is asserted to **exist**, by name and schema.

So: the **real** migration tree (the real ``env.py``, every real revision) is copied, one
head using ``autocommit_block()`` is appended, and ``alembic upgrade head`` is run against a
**real Postgres**. ``CREATE INDEX CONCURRENTLY`` is the payload deliberately — Postgres
refuses it inside a transaction block, so it cannot succeed unless the block genuinely
escaped one. It is its own control.

🚨 **The negative control is the load-bearing half.** ``test_the_old_env_py_still_fails``
takes that same copied tree and puts the ``SET search_path`` statement **back** into its
``env.py``, then asserts the upgrade fails. Without it, a green here is satisfied by an
alembic that stopped asserting, a Postgres that stopped caring, or a migration that never
ran. With it, the pass is attributed to the change in ``env.py`` and nothing else.

🔴 SQLite is NOT sufficient here, and the file this replaces said it was
-----------------------------------------------------------------------
``test_autocommit_block_availability.py`` measured on SQLite and stated, as its corrected
mechanism, that core#933's **option 1** (*move the ``SET`` inside
``context.begin_transaction()``*) *"does not help"* and that *"only option 2 … can satisfy
the real condition"*. Measured on 2026-09-07 against **PostgreSQL 16 / psycopg2** and
alembic 1.18.4, reading both attributes on **both** sides of ``begin_transaction()``:

===========================  ====================  ====================  ==============
arm                          BEFORE begin_txn      AFTER begin_txn       outcome
                             (in_conn, owns_txn)   (in_conn, owns_txn)
===========================  ====================  ====================  ==============
SQLite, nothing executed     (False, False)        (False, False)        entered
SQLite, SET before           (True,  False)        (True,  False)        AssertionError
SQLite, SET inside (opt 1)   (False, False)        (True,  False)        AssertionError
Postgres, nothing executed   (False, False)        (True,  **True**)     entered
Postgres, SET before         (True,  False)        (True,  False)        AssertionError
Postgres, SET inside (opt 1) (False, False)        (True,  **True**)     **entered**
===========================  ====================  ====================  ==============

**Option 1 works on PostgreSQL and fails on SQLite** — because on Postgres
``begin_transaction()`` really does call ``connection.begin()`` and assign ``_transaction``,
so a statement executed *after* it joins alembic's own transaction. The old file's claim was
true of the backend it measured and false of the only backend migrations ever run on.

Two things follow, and they are why this test uses a real database:

1. **The discriminator is the state BEFORE ``begin_transaction()``**, not inside it. The old
   helper read it inside, where it is too late to tell the arms apart.
2. **Option 2 was still the right fix**, but for a reason the old file did not have: not
   *"option 1 cannot work"* — it can — but that option 1's correctness is **driver-dependent**,
   and a fix that holds on one backend and not another is how this defect regenerates.

⚠️ This is the third mechanism written down for core#933 and the second correction. The
first said ``begin_transaction()`` "never assigns ``_transaction``"; the second said
``_in_connection_transaction()`` was the discriminator and option 1 was refuted. Both were
derived by reasoning or by measuring the convenient backend. **Measure the one that ships.**

⚠️ Running by hand on Windows: ``export UV_NO_SYNC=1`` first (see ``_run_alembic``).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import (
    PROJECT_ROOT,
    _run_alembic,
    head_revision,
)

#: The head appended to the real tree. `CREATE INDEX CONCURRENTLY` is chosen because
#: Postgres refuses it inside a transaction block — so it cannot pass unless the
#: autocommit block genuinely left one.
#:
#: ⚠️ `audit_logs` is the table core#933 was found on (core#693 wanted this index and
#: shipped a plain `CREATE INDEX` instead). Using it keeps the control honest about the
#: thing that actually motivated the issue.
_AUTOCOMMIT_HEAD = '''"""core#933 AC1: a migration that uses op.get_context().autocommit_block().

CREATE INDEX CONCURRENTLY cannot run inside a transaction block, so this migration
completes only if the autocommit block really escaped one.
"""

from alembic import op

revision = "cc933autocommit"
down_revision = "{down_revision}"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("CREATE INDEX CONCURRENTLY ix_cc933_probe ON audit_logs (user_id)")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_cc933_probe")
'''

#: The pre-fix `env.py` shape, reintroduced into a copy of the tree for the negative
#: control. Anchored on the function signature rather than on a comment, so a doc edit
#: cannot silently turn the control into a no-op.
_ANCHOR = '    with _engine_with_search_path("public").connect() as connection:'
_MUTANT = (
    '    with _engine_with_search_path("public").connect() as connection:\n'
    '        connection.execute(__import__("sqlalchemy").text("SET search_path TO public"))'
)


def _reset_db(db_url: str) -> None:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()


def _tree_with_autocommit_head(tmp_path: Path, *, break_env_py: bool = False) -> Path:
    """A copy of the real migration tree with one ``autocommit_block()`` head appended.

    Copies ``datanika/migrations/`` verbatim — the same ``env.py``, every real revision —
    so what is exercised is the artifact that ships, not a model of it.

    ``break_env_py`` reintroduces the pre-fix statement, which is the negative control.
    """
    real = PROJECT_ROOT / "datanika" / "migrations"
    dest = tmp_path / "migrations"
    shutil.copytree(real, dest, ignore=shutil.ignore_patterns("__pycache__"))

    env_py = dest / "env.py"
    source = env_py.read_text(encoding="utf-8")
    assert _ANCHOR in source, (
        'env.py no longer opens phase 1 with _engine_with_search_path("public") — this '
        "helper is patching a file it does not recognise, so the negative control below "
        "would be mutating nothing and passing for the wrong reason"
    )
    if break_env_py:
        env_py.write_text(source.replace(_ANCHOR, _MUTANT, 1), encoding="utf-8")

    head, _ = head_revision()
    (dest / "versions" / "cc933_autocommit.py").write_text(
        _AUTOCOMMIT_HEAD.format(down_revision=head), encoding="utf-8"
    )

    ini = tmp_path / "alembic.ini"
    ini.write_text(
        (PROJECT_ROOT / "alembic.ini")
        .read_text(encoding="utf-8")
        .replace("script_location = datanika/migrations", f"script_location = {dest}"),
        encoding="utf-8",
    )
    return ini


class TestAutocommitBlockRunsInARealMigration:
    """core#933 AC1, both directions.

    ⚠️ Running a foreign migration tree leaves ``alembic_version`` holding a revision the
    real tree has never heard of, and the Postgres container is session-scoped and shared
    by every module in this directory. Without the reset these tests pass in isolation and
    break other files with ``Can't locate revision identified by 'cc933autocommit'`` — a
    failure that names this file nowhere. Same hazard, same remedy, as
    ``test_data_preservation_roundtrip.TestAgainstAMutatedMigrationTree``.
    """

    @pytest.fixture(autouse=True)
    def _leave_the_shared_database_usable(self, roundtrip_db_url):
        _reset_db(roundtrip_db_url)
        yield
        _reset_db(roundtrip_db_url)

    def test_a_migration_using_autocommit_block_completes(self, roundtrip_db_url, tmp_path):
        ini = _tree_with_autocommit_head(tmp_path)

        r = _run_alembic(["upgrade", "head"], roundtrip_db_url, ini)

        assert r.returncode == 0, (
            "`alembic upgrade head` failed on a migration using "
            "op.get_context().autocommit_block(). If the traceback ends in a bare "
            "`AssertionError` inside alembic's migration.py, core#933 has regressed: "
            "something in datanika/migrations/env.py is executing a statement on the "
            "connection before context.begin_transaction().\n"
            f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
        )

    def test_the_index_it_created_actually_exists(self, roundtrip_db_url, tmp_path):
        """Exit 0 is not the assertion — a migration whose body did nothing exits 0 too."""
        ini = _tree_with_autocommit_head(tmp_path)
        r = _run_alembic(["upgrade", "head"], roundtrip_db_url, ini)
        assert r.returncode == 0, f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"

        engine = create_engine(roundtrip_db_url)
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT schemaname, indexdef FROM pg_indexes "
                        "WHERE indexname = 'ix_cc933_probe'"
                    )
                ).fetchone()
        finally:
            engine.dispose()

        assert row is not None, (
            "alembic exited 0 but ix_cc933_probe does not exist. The migration ran and "
            "created nothing, which is what an autocommit block that silently swallowed "
            "its statement would look like"
        )
        assert row[0] == "public", f"index landed in schema {row[0]!r}, not public"

    def test_the_old_env_py_still_fails(self, roundtrip_db_url, tmp_path):
        """🚨 The control. Same tree, same head, ``SET search_path`` put back.

        Without this, every green above is satisfied by an alembic that stopped
        asserting, a Postgres that stopped refusing CONCURRENTLY inside a transaction,
        or a migration that was never reached. This is what attributes the pass to
        ``env.py``.
        """
        ini = _tree_with_autocommit_head(tmp_path, break_env_py=True)

        r = _run_alembic(["upgrade", "head"], roundtrip_db_url, ini)

        assert r.returncode != 0, (
            "Reintroducing `SET search_path TO public` into env.py no longer breaks "
            "autocommit_block(). Either alembic changed its behaviour or this control is "
            "patching a file shape that no longer exists — in both cases the tests above "
            "are no longer attributable and must be re-derived before being trusted.\n"
            f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
        )
        assert "AssertionError" in r.stderr, (
            "the mutated tree failed, but not with the AssertionError core#933 is about, "
            "so this control is measuring some other breakage:\n"
            f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
        )


class TestThePhase2LoopStillReachesTheMigrations:
    """Coverage for the tenant loop, which core#933 restructured (core#1164).

    Phase 2 stopped sharing one connection with phase 1 and now opens one per schema. No
    `tenant_*` schema exists any longer (all tables moved to `public`), so ordinary runs
    never enter that loop at all — an untested loop that a refactor has just rewritten is
    exactly what a real tenant schema would discover in production.

    🔴 **What this asserts is NOT "the run succeeds", because it does not — and that is a
    PRE-EXISTING bug, measured, not introduced here.** With any `tenant_*` schema present,
    `alembic upgrade head` fails at revision ``k0g7h8i9j1d2`` with ``column
    "hard_cap_runs" of relation "plans" already exists``. Phase 2 gives the tenant schema
    its own empty ``alembic_version``, so the **entire** revision chain re-runs against it;
    unqualified DDL then resolves through ``search_path`` to the ``public`` tables the
    first phase already migrated.

    Measured on 2026-09-07, same container, same scenario, two trees:

    ==============================  ==========  ============
    env.py                          no tenant   with tenant
    ==============================  ==========  ============
    ``origin/dev`` (pre-fix)        rc=0        **rc=1**
    this branch (core#933 fix)      rc=0        **rc=1**
    ==============================  ==========  ============

    Identical failure on both, so the fix neither caused it nor cures it. Filed as
    core#1164 rather than fixed here: repairing it is a decision about whether the
    tenant-schema phase should exist at all, and bundling it into a transaction-semantics
    change would make both un-bisectable.

    🔑 **So the assertion is that phase 2 gets *far enough to fail that way*** — which is a
    positive statement about the restructured loop. A broken connection-per-schema would
    fail earlier and differently (a connect error, a missing search path, no phase-2 output
    at all), and this test would catch it. It also goes red the day core#1164 is fixed,
    which is the correct moment to delete it.
    """

    @pytest.fixture(autouse=True)
    def _leave_the_shared_database_usable(self, roundtrip_db_url):
        _reset_db(roundtrip_db_url)
        yield
        engine = create_engine(roundtrip_db_url)
        with engine.begin() as conn:
            conn.execute(text('DROP SCHEMA IF EXISTS "tenant_933" CASCADE'))
        engine.dispose()
        _reset_db(roundtrip_db_url)

    def test_phase_2_opens_its_connection_and_runs_migrations_in_the_tenant_schema(
        self, roundtrip_db_url
    ):
        first = _run_alembic(["upgrade", "head"], roundtrip_db_url)
        assert first.returncode == 0, (
            "the baseline upgrade with NO tenant schema failed, so nothing below is "
            f"attributable to phase 2:\nstdout:\n{first.stdout}\nstderr:\n{first.stderr}"
        )

        engine = create_engine(roundtrip_db_url)
        with engine.begin() as conn:
            conn.execute(text('CREATE SCHEMA "tenant_933"'))
        engine.dispose()

        second = _run_alembic(["upgrade", "head"], roundtrip_db_url)

        assert second.returncode != 0, (
            "`alembic upgrade head` now SUCCEEDS with a tenant_* schema present. That is "
            "good news and this test is stale: core#1164 has been fixed (or the phase-2 "
            "loop removed). Delete this class and say which."
        )
        assert "hard_cap_runs" in second.stderr, (
            "phase 2 failed, but not at the known core#1164 collision. Something in the "
            "restructured connection-per-schema loop is broken — the loop is supposed to "
            "reach revision k0g7h8i9j1d2 and die on the pre-existing search_path "
            f"resolution, not earlier.\nstdout:\n{second.stdout}\nstderr:\n{second.stderr}"
        )
        assert "Running upgrade" in second.stderr, (
            "phase 2 never ran a single migration, so its connection never carried a "
            f"usable context:\nstdout:\n{second.stdout}\nstderr:\n{second.stderr}"
        )

        engine = create_engine(roundtrip_db_url)
        try:
            with engine.connect() as conn:
                present = conn.execute(
                    text(
                        "SELECT schema_name FROM information_schema.schemata "
                        "WHERE schema_name = 'tenant_933'"
                    )
                ).fetchone()
        finally:
            engine.dispose()
        assert present is not None, (
            "the tenant schema vanished during the run — phase 2 is doing something "
            "destructive it did not do before"
        )
