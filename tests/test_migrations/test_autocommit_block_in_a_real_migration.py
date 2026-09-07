"""``autocommit_block()`` works in a real migration, against a real Postgres (core#933).

This is core#933 **AC1**. It replaces ``test_autocommit_block_availability.py``, which
asserted the *defect* and instructed its own deletion the day the defect went away.

🆕 It also carries **core#1164** — ``TestATenantSchemaDoesNotBreakTheDeploy`` — because that
defect lives in the same file for the same reason: ``env.py`` is the migration entrypoint,
and the only honest way to test it is to run the real tree against a real Postgres. The
filename is kept rather than widened; renaming a test file costs a delete-plus-add in the
diff and buys a word.

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


#: The tenant phase as it stood before core#1164, appended to a copy of the fixed tree.
#:
#: 🚨 **Deliberately a literal, not `git show origin/dev:...`.** Reading the mutant out of a
#: moving ref works exactly once: after this lands, `origin/dev` carries the FIXED file and
#: the "control" silently starts asserting that the fix fails to fail. A control that stops
#: controlling is worse than none, because it still reports green.
#:
#: Self-contained on purpose — it re-imports `re` and `get_tenant_schemas`, which the fixed
#: `env.py` no longer needs, so it cannot be quietly disarmed by an import cleanup.
_TENANT_PHASE_MUTANT = """

# core#1164 negative control — the removed phase 2, restored.
import re as _re1164  # noqa: E402
from datanika.migrations.helpers import get_tenant_schemas as _gts1164  # noqa: E402


def _core1164_tenant_phase() -> None:
    with _engine_with_search_path("public").connect() as connection:
        schemas = _gts1164(connection)
    for schema in schemas:
        if not _re1164.match(r"^tenant_\\d+$", schema):
            continue
        with _engine_with_search_path(f\'"{schema}",public\').connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                version_table_schema=schema,
                transaction_per_migration=True,
            )
            with context.begin_transaction():
                context.run_migrations()
            connection.commit()


if not context.is_offline_mode():
    _core1164_tenant_phase()
"""


def _tree_with_tenant_phase_restored(tmp_path: Path) -> Path:
    """A copy of the real tree with the removed tenant phase appended to its `env.py`."""
    real = PROJECT_ROOT / "datanika" / "migrations"
    dest = tmp_path / "migrations"
    shutil.copytree(real, dest, ignore=shutil.ignore_patterns("__pycache__"))

    env_py = dest / "env.py"
    source = env_py.read_text(encoding="utf-8")
    assert "_engine_with_search_path" in source, (
        "env.py no longer defines _engine_with_search_path, so the appended control below "
        "would raise NameError instead of reproducing core#1164 — it would still 'fail', "
        "and the control would pass for entirely the wrong reason"
    )
    env_py.write_text(source + _TENANT_PHASE_MUTANT, encoding="utf-8")

    ini = tmp_path / "alembic.ini"
    ini.write_text(
        (PROJECT_ROOT / "alembic.ini")
        .read_text(encoding="utf-8")
        .replace("script_location = datanika/migrations", f"script_location = {dest}"),
        encoding="utf-8",
    )
    return ini


class TestATenantSchemaDoesNotBreakTheDeploy:
    """core#1164 — `alembic upgrade head` must survive a `tenant_*` schema existing.

    Migrations run from the container start command, so this failure is **a container that
    cannot start**: a wedged deploy, discovered at the worst possible moment, with a
    traceback naming a column in `plans` rather than the loop that caused it.

    The phase that caused it is removed rather than repaired, and that is not a bet on
    per-tenant schemas never returning. It is that **the loop could not do its stated job
    even in principle**, measured:

    * ``is_tenant_table()`` returns True for **0 of the 26** tables in ``Base.metadata`` —
      every one is in ``PUBLIC_TABLES``. So the loop's ``include_object=_include_tenant``
      admitted no table at all. (``test_migration_helpers.py::TestIsTenantTable::
      test_no_model_table_is_tenant`` already asserts this, from the other side.)
    * ``include_object`` filters **autogenerate comparison**, not which revisions
      ``upgrade`` executes — so the loop replayed the *public* chain against the tenant
      schema regardless, which is the collision itself.
    * ``TenantService`` — the only thing that can create such a schema — is referenced
      nowhere outside ``tests/test_services/test_tenant.py``.

    A future per-tenant-schema feature would need a tenant-scoped revision chain, which
    this loop never had. Keeping it would preserve the landmine, not the capability.
    """

    _SCHEMA = "tenant_1164"

    @pytest.fixture(autouse=True)
    def _leave_the_shared_database_usable(self, roundtrip_db_url):
        _reset_db(roundtrip_db_url)
        yield
        engine = create_engine(roundtrip_db_url)
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{self._SCHEMA}" CASCADE'))
        engine.dispose()
        _reset_db(roundtrip_db_url)

    def _create_schema(self, url: str) -> None:
        engine = create_engine(url)
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{self._SCHEMA}"'))
        engine.dispose()

    def _schema_exists(self, url: str) -> bool:
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                return (
                    conn.execute(
                        text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"),
                        {"s": self._SCHEMA},
                    ).fetchone()
                    is not None
                )
        finally:
            engine.dispose()

    def test_upgrade_head_succeeds_with_a_tenant_schema_present(self, roundtrip_db_url):
        first = _run_alembic(["upgrade", "head"], roundtrip_db_url)
        assert first.returncode == 0, (
            "the baseline upgrade with NO tenant schema failed, so nothing below is "
            f"attributable:\nstdout:\n{first.stdout}\nstderr:\n{first.stderr}"
        )

        self._create_schema(roundtrip_db_url)
        # 🔑 Arming. Without this the test passes on a run where the schema was never
        # created — i.e. it would assert nothing, greenly.
        assert self._schema_exists(roundtrip_db_url), (
            f"{self._SCHEMA} was not created, so the upgrade below is not being asked "
            "the question this test exists to ask"
        )

        second = _run_alembic(["upgrade", "head"], roundtrip_db_url)

        assert second.returncode == 0, (
            "`alembic upgrade head` failed with a tenant_* schema present (core#1164). "
            "If the error names `hard_cap_runs` on `plans`, a tenant phase has been "
            "reintroduced into env.py: it gives the schema its own empty alembic_version, "
            "replays the whole public chain against it, and unqualified DDL resolves "
            "through search_path back to the public tables.\n"
            f"stdout:\n{second.stdout}\nstderr:\n{second.stderr}"
        )

    def test_the_tenant_schema_is_left_untouched(self, roundtrip_db_url):
        """Succeeding by DROPPING the schema would also satisfy the test above."""
        assert _run_alembic(["upgrade", "head"], roundtrip_db_url).returncode == 0
        self._create_schema(roundtrip_db_url)
        assert self._run_and_count_tables(roundtrip_db_url) == 0, (
            "the migration run created tables inside the tenant schema. Nothing should "
            "be writing there any more — see this class's docstring."
        )
        assert self._schema_exists(roundtrip_db_url), (
            "the tenant schema vanished during `alembic upgrade head`. Succeeding by "
            "destroying the input is not succeeding."
        )

    def _run_and_count_tables(self, url: str) -> int:
        r = _run_alembic(["upgrade", "head"], url)
        assert r.returncode == 0, f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                return conn.execute(
                    text("SELECT count(*) FROM information_schema.tables WHERE table_schema = :s"),
                    {"s": self._SCHEMA},
                ).scalar()
        finally:
            engine.dispose()

    def test_restoring_the_tenant_phase_brings_the_failure_back(self, roundtrip_db_url, tmp_path):
        """🚨 The control. Without it, both tests above are satisfied by a Postgres that
        stopped caring, an alembic that stopped replaying, or a schema never created.

        This appends the removed phase to a copy of the **real** tree and asserts the
        upgrade fails **at the known collision** — not merely that it fails, which any
        syntax error in the appended block would also produce.
        """
        ini = _tree_with_tenant_phase_restored(tmp_path)

        assert _run_alembic(["upgrade", "head"], roundtrip_db_url, ini).returncode == 0, (
            "the mutated tree cannot even complete a no-tenant upgrade, so its failure "
            "below would not be attributable to the restored phase"
        )
        self._create_schema(roundtrip_db_url)

        r = _run_alembic(["upgrade", "head"], roundtrip_db_url, ini)

        assert r.returncode != 0, (
            "restoring the tenant phase no longer reproduces core#1164. Either alembic "
            "changed behaviour or this control no longer patches a file shape that "
            "exists — in both cases the two tests above are no longer attributable.\n"
            f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
        )
        assert "hard_cap_runs" in r.stderr, (
            "the mutated tree failed, but NOT at core#1164's collision — so this control "
            "is measuring some other breakage (a NameError in the appended block would "
            f"look like this).\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
        )
