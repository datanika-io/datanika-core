"""audit_logs: convert old_values/new_values to jsonb, index user_id.

Release **N₀** of the PII-separation chain — ``docs/specs/SPEC_PII_SEPARATION.md`` D13,
tracked as core#693. Deliberately **code-free**: no model changes ride with it, so its
revert is one line and the one operation in the plan whose blue/green tolerance is not
obvious carries its own tiny rollback instead of dragging the expand release back with it.

Why now, rather than with the release that needs it
---------------------------------------------------
Both operations are free today and neither is free later, and nothing purges this table:
``audit_logs`` is in none of ``run_maintenance_task``'s five sweeps and ``AuditLog`` is the
one model with no ``deleted_at``. It grows monotonically for the life of the product, so
there is no future point at which this gets cheaper.

* ``ALTER … TYPE jsonb`` is a full table rewrite under ``ACCESS EXCLUSIVE``. At 117 rows /
  88 kB it completes faster than the transaction wrapping it; at the first real cohort it
  is an outage.
* ``CREATE INDEX CONCURRENTLY`` is instant now and an online build later — and until it
  exists, ``WHERE user_id = X`` is a sequential scan, which is what D12.4's erasure sweep
  and ``AuditService.list_logs(user_id=…)`` both do.

⚠️ **The jsonb conversion is a capability, not an optimization.** On ``json`` there are no
containment operators at all — ``?``, ``?|`` and ``@>`` are jsonb-only — so the residual
erasure sweep cannot be *expressed*, let alone return zero. A query that cannot run does
not return zero; it raises.

expand-contract: SAFE IN ONE RELEASE, and measured rather than argued.
    This carries a ``type_change``, which ``test_expand_contract_policy.py`` flags because
    widening and narrowing are not distinguishable from an AST. It is neither: ``json`` and
    ``jsonb`` accept exactly the same set of values, so no value that fits before can fail
    after. The real t1 risk is the other direction — the PREVIOUSLY DEPLOYED code, whose
    model declares SQLAlchemy's generic ``JSON``, serves against the converted column for
    the length of the blue/green swap, and psycopg2 renders that bind as an explicit
    ``%(new_values)s::JSON`` cast. Whether that round-trips against a ``jsonb`` column is
    dialect- and driver-specific and reads as obviously true, which is why it is asserted
    by a probe instead: ``tests/test_migrations/test_n0_audit_jsonb_blue_green.py``, named
    in ``ci.yml``'s ``migration-roundtrip`` job. If that probe ever goes red, this
    migration cannot ship alone — per D13 it merges into release N together with the
    ``JSON().with_variant(JSONB, "postgresql")`` model change.

    The index is expand-only: adding an index removes nothing and tightens nothing.

Revision ID: d4e9f1a6b3c7
Revises: f6a7b8c9d0e1
Create Date: 2026-09-02
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d4e9f1a6b3c7"
down_revision: str | None = "f6a7b8c9d0e1"
branch_labels: str | None = None
depends_on: str | None = None

INDEX_NAME = "ix_audit_logs_user_id"

# ⚠️ The index is created NON-concurrently, and that is a deliberate departure from D13's
# wording. `autocommit_block()` — the mechanism D13 names, and the only way to run
# `CREATE INDEX CONCURRENTLY` from inside alembic — **is unavailable in this repo**, and
# that was measured rather than assumed:
#
#     File ".../alembic/runtime/migration.py", line 329, in autocommit_block
#         assert self._transaction is not None
#     AssertionError
#
# `migrations/env.py` executes `SET search_path TO public` on the connection *before*
# calling `context.begin_transaction()`. That autobegins a SQLAlchemy transaction, so
# alembic takes its "already inside an external transaction" branch, never sets
# `_transaction`, and `autocommit_block` asserts. This is true of every migration in the
# repo, not just this one — filed separately so the next person needing CONCURRENTLY,
# `VACUUM` or `ALTER TYPE … ADD VALUE` finds the answer instead of the assertion.
#
# The remaining options were a second AUTOCOMMIT connection, or a plain index. A plain
# index wins on the merits at this size, in both directions:
#
#   * What CONCURRENTLY buys is not blocking writes during the build. `audit_logs` is
#     **117 rows / 88 kB**, so the build is sub-millisecond and the SHARE lock is not an
#     outage even with the previous container serving through the blue/green swap.
#   * What CONCURRENTLY costs is real: it is **not transactional**, so a failure part-way
#     leaves an INVALID index that the planner ignores, that `pg_indexes` still lists, and
#     that makes the next deploy's retry die on "relation already exists" — a transient
#     failure wedging every subsequent deploy until somebody drops it by hand. A plain
#     `CREATE INDEX` rolls back with the migration.
#
# The whole reason this ships as its own release is that both operations are cheap *now*.
# Paying CONCURRENTLY's failure modes to protect a sub-millisecond lock would spend the
# very margin that justifies doing it today. `test_the_index_is_valid` in the probe pins
# the property that actually matters — the planner can use what was built.


def upgrade() -> None:
    # `postgresql_using` is required: Postgres has no implicit json → jsonb cast in an
    # ALTER TYPE, so without it this fails with "column cannot be cast automatically".
    for column in ("old_values", "new_values"):
        op.alter_column(
            "audit_logs",
            column,
            existing_type=sa.JSON(),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=True,
            postgresql_using=f"{column}::jsonb",
        )

    op.create_index(INDEX_NAME, "audit_logs", ["user_id"], unique=False)

    op.get_bind().commit()


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name="audit_logs")

    # jsonb → json is a lossless cast of the *value*: every jsonb document renders as
    # valid json text. It does NOT restore the original bytes — jsonb normalised key
    # order, whitespace and duplicate keys on the way in, and nothing recorded what they
    # were. That is a property of the forward migration, not of this rollback, and the
    # data-preservation harness compares parsed values (`json.dumps(sort_keys=True)`), so
    # it is not a round-trip failure. Deliberately NOT marked `one_way`: the downgrade
    # works, and the marker means "cannot be rolled back", not "text is not byte-equal".
    for column in ("old_values", "new_values"):
        op.alter_column(
            "audit_logs",
            column,
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=sa.JSON(),
            existing_nullable=True,
            postgresql_using=f"{column}::json",
        )

    op.get_bind().commit()
