"""Release N₀ of the PII-separation chain: ``audit_logs`` gets ``jsonb`` + a ``user_id`` index.

Spec: ``docs/specs/SPEC_PII_SEPARATION.md`` D13 and acceptance criteria 0a / 0b / 0c.
Issue: core#693.

Why this file exists, and why the existing guards do not cover it
-----------------------------------------------------------------
``test_expand_contract.py`` compares the **deployed release's models** against the new
schema and says so in its own docstring:

    Not covered, and left explicit rather than implied: type narrowing, and a new
    ``UNIQUE`` on an existing column.

A ``json`` → ``jsonb`` conversion is a *type change*, so the t1-window guard reads it and
declines to judge it — correctly, because whether a type change is safe is not derivable
from an AST. ``test_expand_contract_policy.py`` reaches the same conclusion from the other
side: it flags ``alter_column(type_=...)`` as ``type_change`` and demands the author state a
reason. **Both guards end at "ask a human."** This file is the measurement that answers them.

The question, stated precisely
------------------------------
Under blue/green the **previously deployed** container serves while the new one runs
``alembic upgrade head``. That previous code declares the column as SQLAlchemy's *generic*
``JSON``::

    old_values: Mapped[dict | None] = mapped_column(JSON, nullable=True)

So for the length of the swap, generic-``JSON`` bind and result processing runs against a
``jsonb`` column, on this driver. D13 refuses to assert that from reasoning:

    Whether SQLAlchemy's generic ``JSON`` bind and result processing round-trip cleanly
    against a ``jsonb`` column on this driver is **not asserted here**, because it is the
    kind of claim that reads as obviously true and is dialect- and driver-specific.

🚨 **The pre-migration model is pinned HERE, as a standalone table, deliberately.** The
obvious implementation imports ``datanika.models.audit_log.AuditLog`` — which declares
generic ``JSON`` today, so it happens to be the pre-migration definition *right now*. The
moment anybody changes that model to ``JSON().with_variant(JSONB, "postgresql")`` — which
D13 itself contemplates — such a probe would silently start testing the **post**-migration
definition against the post-migration column, i.e. it would stop testing the blue/green
case while staying green. A local table on its own ``MetaData`` cannot drift that way.

What each test discriminates
----------------------------
Every assertion here is false before the migration lands, which is the only reason to
trust it afterwards:

* ``test_payload_columns_are_jsonb`` — ``information_schema`` reads ``json`` today.
* ``test_containment_operator_is_expressible`` — ``new_values ? 'email'`` is an **error**
  on a ``json`` column (the operator is jsonb-only), which is criterion 0a's whole point.
* ``test_generic_json_model_round_trips_against_jsonb`` — the blue/green case (0c).
* ``test_user_id_index_exists`` / ``test_planner_can_use_the_user_id_index`` — 0b. The
  planner test uses ``enable_seqscan = off`` rather than asserting "no Seq Scan" on a live
  plan: at 1 row Postgres will *always* choose a sequential scan because it is genuinely
  cheaper, so the naive form of criterion 0b fails on a correct implementation. With
  sequential scans penalised, a table with no index on ``user_id`` still reports ``Seq
  Scan`` (there is nothing else available) and a table with one reports an index path. That
  is what makes it a measurement rather than a restatement of the previous test.
"""

from __future__ import annotations

import json

import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from tests.test_migrations.conftest import _run_alembic

INDEX_NAME = "ix_audit_logs_user_id"


# ---------------------------------------------------------------------------
# The PREVIOUSLY DEPLOYED model definition, frozen.
#
# This is a copy of `datanika/models/audit_log.py` as it stands at the moment N₀
# ships, reduced to the columns that matter and given its own registry so it can
# never collide with the real one. It must NOT be replaced by an import — see the
# module docstring.
# ---------------------------------------------------------------------------
class _PreMigrationBase(DeclarativeBase):
    pass


class _PreMigrationAuditLog(_PreMigrationBase):
    """`audit_logs` as the deployed release declares it: generic `JSON`."""

    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    org_id: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    user_id: Mapped[int | None] = mapped_column(sa.BigInteger, nullable=True)
    action: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    resource_type: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    resource_id: Mapped[int | None] = mapped_column(sa.BigInteger, nullable=True)
    old_values: Mapped[dict | None] = mapped_column(sa.JSON, nullable=True)
    new_values: Mapped[dict | None] = mapped_column(sa.JSON, nullable=True)
    ip_address: Mapped[str | None] = mapped_column(sa.String(45), nullable=True)
    created_at: Mapped[object] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )


#: A payload chosen so that a *silent* failure has somewhere to show: nested
#: containers, a non-ASCII value, and keys deliberately out of sorted order — jsonb
#: reorders object keys on storage, so comparing the parsed value (not its text) is
#: the only correct assertion, and this payload makes that difference real rather
#: than theoretical.
_PAYLOAD = {
    "zeta": "last-by-sort-order",
    "email": "n0-probe@qa.example.com",
    "nested": {"beta": [1, 2, {"gamma": None}], "alpha": True},
    "unicode": "Ωмега — ünïcode",
    "alpha": 1.5,
}


@pytest.fixture(scope="module")
def migrated_db(roundtrip_db_url: str) -> str:
    """A blank Postgres migrated to ``head``.

    Resets at the **start** rather than the end: this directory shares one
    session-scoped container across modules, and a module that only cleans up after
    itself is still at the mercy of whatever ran before it (WORKFLOW_RULES §13 trap 19
    — a foreign ``alembic_version`` left behind by a neighbour).
    """
    engine = create_engine(roundtrip_db_url)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            conn.execute(text("CREATE SCHEMA public"))
    finally:
        engine.dispose()

    result = _run_alembic(["upgrade", "head"], roundtrip_db_url)
    assert result.returncode == 0, f"alembic upgrade head failed:\n{result.stdout}\n{result.stderr}"
    return roundtrip_db_url


@pytest.fixture(scope="module")
def org_id(migrated_db: str) -> int:
    """A real ``organizations`` row, because ``audit_logs.org_id`` is a foreign key.

    Learned by running this file red before the migration existed: the round-trip test
    failed on ``audit_logs_org_id_fkey`` rather than on anything to do with ``jsonb``,
    which is a red for the wrong reason — it would have failed identically *after* the
    migration and told me nothing either way.
    """
    engine = create_engine(migrated_db)
    try:
        with engine.begin() as conn:
            return conn.execute(
                text(
                    "INSERT INTO organizations (name, slug, default_dbt_schema) "
                    "VALUES ('N0 probe org', 'n0-probe-org', 'datanika') RETURNING id"
                )
            ).scalar_one()
    finally:
        engine.dispose()


def _column_type(db_url: str, column: str) -> str:
    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'audit_logs' "
                    "AND column_name = :col"
                ),
                {"col": column},
            ).scalar_one()
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Criterion 0a — the columns are jsonb, and the containment operator works
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("column", ["old_values", "new_values"])
def test_payload_columns_are_jsonb(migrated_db: str, column: str) -> None:
    """0a, first half. Reads ``json`` before the migration lands."""
    assert _column_type(migrated_db, column) == "jsonb", (
        f"audit_logs.{column} is not jsonb. Every other assertion in this file is "
        "about behaviour ON a jsonb column, so they would all be measuring the wrong "
        "thing if this one is false — read this failure first."
    )


def test_containment_operator_is_expressible(migrated_db: str) -> None:
    """0a, second half — *"today that query is a syntax error, which is the whole point"*.

    D12.4's residual sweep is a containment query. On ``json`` there are no containment
    operators at all, so the sweep cannot be *written*, let alone return zero — and a
    query that cannot run does not return zero, it raises. This asserts the capability,
    not a result.
    """
    engine = create_engine(migrated_db)
    try:
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT count(*) FROM audit_logs WHERE new_values ? 'email'")
            ).scalar_one()
    finally:
        engine.dispose()
    assert count == 0, "no audit rows are seeded here; the point is that the query RUNS"


# ---------------------------------------------------------------------------
# Criterion 0c — the blue/green case
# ---------------------------------------------------------------------------


def test_generic_json_model_round_trips_against_jsonb(migrated_db: str, org_id: int) -> None:
    """0c. A write **and** a read through the pre-migration model definition.

    Both halves are named in the criterion because they fail independently: a bind
    that Postgres refuses breaks the write, and a result processor that double-decodes
    (``json.loads`` applied to an already-parsed dict) breaks the read. A test that
    only inserted would report the second as green.
    """
    engine = create_engine(migrated_db)
    try:
        with Session(engine) as session:
            row = _PreMigrationAuditLog(
                org_id=org_id,
                user_id=None,
                action="update",
                resource_type="n0-probe",
                resource_id=None,
                old_values=_PAYLOAD,
                new_values={"only": "one key"},
            )
            session.add(row)
            session.commit()
            row_id = row.id

        # A NEW session, so nothing is served out of the identity map — otherwise the
        # "read" is a read of the object we just constructed in Python and the column
        # is never consulted.
        with Session(engine) as session:
            fetched = session.get(_PreMigrationAuditLog, row_id)
            assert fetched is not None
            assert fetched.old_values == _PAYLOAD, (
                "the deployed release's generic-JSON model does not round-trip against "
                "a jsonb column. Under blue/green that code is SERVING while this "
                "migration runs, so N₀ cannot ship alone — per D13 it merges into N "
                "together with the `JSON().with_variant(JSONB, 'postgresql')` model change."
            )
            assert fetched.new_values == {"only": "one key"}

        # And the value is genuinely stored as jsonb, not as an opaque string that
        # happens to survive the Python round trip. Without this, a column that had
        # silently stayed `text` would pass everything above.
        with engine.connect() as conn:
            typeof = conn.execute(
                text("SELECT jsonb_typeof(old_values) FROM audit_logs WHERE id = :i"),
                {"i": row_id},
            ).scalar_one()
            assert typeof == "object"

            # `CAST(:probe AS jsonb)`, not `:probe::jsonb` — SQLAlchemy's `text()` reads
            # the `::` as the start of another bind parameter name and the statement
            # reaches Postgres malformed.
            hit = conn.execute(
                text("SELECT count(*) FROM audit_logs WHERE old_values @> CAST(:probe AS jsonb)"),
                {"probe": json.dumps({"email": _PAYLOAD["email"]})},
            ).scalar_one()
            assert hit == 1, "jsonb containment cannot find a key the model just wrote"
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Criterion 0b — the user_id index
# ---------------------------------------------------------------------------


def test_user_id_index_exists(migrated_db: str) -> None:
    engine = create_engine(migrated_db)
    try:
        with engine.connect() as conn:
            names = {
                r[0]
                for r in conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE tablename = 'audit_logs'")
                )
            }
    finally:
        engine.dispose()
    assert INDEX_NAME in names, f"expected {INDEX_NAME} on audit_logs, found {sorted(names)}"


def test_the_index_is_valid(migrated_db: str) -> None:
    """A ``CREATE INDEX CONCURRENTLY`` that fails leaves an **invalid** index behind.

    It still appears in ``pg_indexes``, so the previous test passes on it, and the
    planner will never use it. This is the discriminating half.
    """
    engine = create_engine(migrated_db)
    try:
        with engine.connect() as conn:
            valid = conn.execute(
                text(
                    "SELECT i.indisvalid FROM pg_index i "
                    "JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = :n"
                ),
                {"n": INDEX_NAME},
            ).scalar_one()
    finally:
        engine.dispose()
    assert valid is True, (
        f"{INDEX_NAME} exists but is INVALID — a failed CREATE INDEX CONCURRENTLY. "
        "Drop it and re-run the migration; the planner is ignoring it meanwhile."
    )


def test_planner_can_use_the_user_id_index(migrated_db: str) -> None:
    """0b, expressed so that it can actually fail.

    The criterion as written — *"EXPLAIN no longer shows Seq Scan"* — is false on a
    correct implementation at this table size, because a sequential scan of one page is
    genuinely cheaper than any index path and Postgres is right to choose it. Penalising
    sequential scans asks the question the criterion means: *is there an index path at
    all?* With no index on ``user_id`` the plan still says ``Seq Scan``, because there is
    nothing else to choose.
    """
    engine = create_engine(migrated_db)
    try:
        with engine.connect() as conn:
            conn.execute(text("SET LOCAL enable_seqscan = off"))
            plan = "\n".join(
                r[0]
                for r in conn.execute(text("EXPLAIN SELECT * FROM audit_logs WHERE user_id = 1"))
            )
    finally:
        engine.dispose()
    assert "Index Scan" in plan or "Bitmap" in plan, (
        "with sequential scans penalised the planner still has no index path for "
        f"audit_logs.user_id:\n{plan}"
    )
