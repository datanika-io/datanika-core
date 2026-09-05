"""``plans.hard_cap_bytes`` must default to ``false`` (core#1071).

The column ships ``server_default = true`` (``z5v2w3x4y6a7``), so **every plan row created
out of band takes a hard byte cap** — and out of band is the only way paid rows are ever
created (core#928). A rebuilt production would therefore block Pro and Enterprise on volume
mid-cycle, against `/pricing`'s verbatim *"No surprise mid-cycle blocks."*

🔑 **Its own sibling settles it.** ``hard_cap_runs`` is the same kind of column in the same
table with the same semantics and defaults to ``false`` (``k0g7h8i9j1d2``). Nobody chose the
difference — no issue, comment or commit argues for it. Product's rule
(core#1071, third instance): **a column that gates may fail open; a column that prices must
have no default.** Both of these gate.

**DDL only, no DML — and that is the load-bearing half.** ``free`` is ``hard_cap_bytes =
true`` in production and that is *correct*: `/pricing` publishes Free as capped at 10 GiB and
it is enforced today. A blanket ``UPDATE plans SET hard_cap_bytes = false`` — the shape
``c5e9a3b7d2f4`` legitimately used for ``overage_run_price_cents`` — would un-cap Free and
make a different published claim false.
``test_the_free_plan_keeps_its_published_hard_cap`` is what stops that arriving later.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Changing a default rewrites no
existing row, so at ``t1`` the previously deployed code reads exactly the values it read
before. The direction to watch is the downgrade, which restores ``true`` — also harmless for
the same reason, asserted below.

Why this file rather than a model assertion
--------------------------------------------
Cloud's tests build the schema with ``Base.metadata.create_all``, which reads the **model** —
so a cloud-side assertion goes green the moment the model changes, whatever the database
holds. Only alembic against a real Postgres can say what the *column* does, and the column is
what a row created by a script outside both repositories will meet. That is precisely the
creator this defect is about.

⚠️ There was no such test for any of the three byte columns before this one, which is why the
default survived: ``datanika-cloud/tests/test_bytes_migration_roundtrip.py``'s docstring says
*"plans.hard_cap_bytes (Boolean, default False)"* while asserting only that the column
exists. A stale comment naming the safe value terminates the search.
"""

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "d7f2c8a4b1e6"
THIS_REVISION = "e8b3d5c7f2a9"

#: An INSERT that deliberately OMITS ``hard_cap_bytes`` — the shape every out-of-band creator
#: uses (``scripts/seed_annual_plans.py``, ``billing/e2e_admin.py``, whatever made
#: production's monthly rows), and the one the column default is answering.
#:
#: ⚠️ ``overage_run_price_cents`` must be stated: ``c5e9a3b7d2f4`` dropped its default and the
#: column is NOT NULL, so an INSERT omitting it fails at this revision for an unrelated reason.
_INSERT_OMITTING = text(
    "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, price_cents, "
    "interval, overage_run_price_cents) "
    "VALUES ('Pro', :slug, :price, 'pro_probe', 7900, 'monthly', 0)"
)


@pytest.fixture
def at_parent(roundtrip_db_url):
    """A database migrated to the revision immediately before this one."""
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    result = _run_alembic(["upgrade", PARENT_REVISION], roundtrip_db_url)
    assert result.returncode == 0, f"could not reach {PARENT_REVISION}: {result.stderr}"
    return engine


def _column_default(engine, column: str):
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_name = 'plans' AND column_name = :col"
            ),
            {"col": column},
        ).scalar_one()


def _stored(engine, slug: str, column: str):
    with engine.begin() as conn:
        return conn.execute(
            text(f"SELECT {column} FROM plans WHERE slug = :slug"),  # noqa: S608 - literal names
            {"slug": slug},
        ).scalar_one()


# ---------------------------------------------------------------------------
# Controls — these attribute the reds below. Without them, "the default is
# false" and "this file never reached the schema" are the same observation.
# ---------------------------------------------------------------------------


def test_control_the_default_is_true_at_the_parent_revision(at_parent):
    default = _column_default(at_parent, "hard_cap_bytes")
    assert default is not None and "true" in default, (
        f"hard_cap_bytes already defaulted to something other than true at "
        f"{PARENT_REVISION} (got {default!r}), so every assertion below would pass without "
        "this migration doing anything"
    )


def test_control_an_omitting_insert_is_hard_capped_at_the_parent_revision(at_parent):
    """The defect itself, reproduced. This is what an out-of-band creator gets today."""
    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-parent", "price": "pri_parent"})

    assert _stored(at_parent, "pro-parent", "hard_cap_bytes") is True, (
        "a plan row created without hard_cap_bytes did NOT come out hard-capped at the "
        "parent revision — core#1071's premise no longer holds and this file needs re-deriving"
    )


def test_control_the_sibling_gate_already_defaults_to_false(at_parent):
    """``hard_cap_runs`` is the comparison the whole decision rests on.

    If this ever stops being ``false``, the argument *"two sibling gates should not disagree
    about what silence means"* has lost one of its two sides and the direction of this
    migration needs re-deciding rather than preserving.
    """
    default = _column_default(at_parent, "hard_cap_runs")
    assert default is not None and "false" in default, (
        f"hard_cap_runs no longer defaults to false (got {default!r}) — the sibling this "
        "migration aligns hard_cap_bytes with has moved"
    )


# ---------------------------------------------------------------------------
# Regressions — red before this migration exists.
# ---------------------------------------------------------------------------


def test_the_column_defaults_to_false_after_the_migration(at_parent, roundtrip_db_url):
    """AC5 — assert the DDL default itself, not merely that the column exists."""
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    default = _column_default(at_parent, "hard_cap_bytes")
    assert default is not None and "false" in default, (
        f"hard_cap_bytes still defaults to {default!r}; a fresh row is still hard-capped"
    )


def test_an_omitting_insert_now_lands_unblocked(at_parent, roundtrip_db_url):
    """The criterion, asserted by writing a row rather than by reading a catalogue.

    This is the line where *"No surprise mid-cycle blocks"* stops being false on a
    from-scratch deployment for every paid row somebody creates out of band.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-fresh", "price": "pri_fresh"})

    assert _stored(at_parent, "pro-fresh", "hard_cap_bytes") is False, (
        "a plan row created without hard_cap_bytes still arrives hard-capped, so a rebuilt "
        "production still blocks Pro and Enterprise on volume mid-cycle"
    )


def test_the_two_sibling_gates_now_agree_on_what_silence_means(at_parent, roundtrip_db_url):
    """The finding, stated as an invariant rather than left in a commit message.

    Derived from the catalogue on both sides so it cannot be satisfied by restating a
    constant: whatever ``hard_cap_runs`` says absence means, ``hard_cap_bytes`` must say the
    same. Two gates in one table with one purpose disagreeing was the whole defect.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    runs = _column_default(at_parent, "hard_cap_runs")
    byts = _column_default(at_parent, "hard_cap_bytes")
    assert runs == byts, (
        f"hard_cap_runs defaults to {runs!r} and hard_cap_bytes to {byts!r}. Sibling gates "
        "must agree about what silence means, or neither default can be reasoned about."
    )


def test_the_free_plan_keeps_its_published_hard_cap(at_parent, roundtrip_db_url):
    """🚨 The bound on the fail-open, and the row a blanket UPDATE would have un-capped.

    `/pricing` publishes Free as *"hard-capped at 10 GB — runs stop"*, and that is enforced
    today. ``c1d2e3f4a5b6`` writes ``free``'s ``hard_cap_bytes`` explicitly, so it never
    depended on the default — exactly the sort of claim that is true when written and false
    later. Asserted against the real migrated database, before **and** after, so a migration
    that reaches for DML reds here instead of quietly un-capping the free tier.
    """
    assert _stored(at_parent, "free", "hard_cap_bytes") is True, (
        "the free plan was not hard-capped before the migration — re-derive this file"
    )

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    assert _stored(at_parent, "free", "hard_cap_bytes") is True, (
        "changing the column default changed the free plan's stored value. It must not: "
        "this migration alters the column, never a row. `/pricing` publishes Free as capped."
    )


def test_an_existing_hard_capped_row_is_left_alone(at_parent, roundtrip_db_url):
    """DDL only, on a row that is not ``free`` — so the assertion is about the statement.

    ``free`` is protected above and is also the one row a migration created. A row somebody
    else made, carrying an explicit ``true``, is the case a blanket UPDATE would reach and
    the free-plan assertion would not notice.
    """
    with at_parent.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, "
                "price_cents, interval, overage_run_price_cents, hard_cap_bytes) VALUES "
                "('Capped', 'capped-on-purpose', 'pri_cap', 'pro_cap', 100, 'monthly', 0, true)"
            )
        )

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    assert _stored(at_parent, "capped-on-purpose", "hard_cap_bytes") is True, (
        "an existing row that had deliberately opted into a hard byte cap was rewritten"
    )


def test_an_explicit_true_still_stores_as_itself(at_parent, roundtrip_db_url):
    """Anti-widening control: changing a default must not disturb explicit writes."""
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, "
                "price_cents, interval, overage_run_price_cents, hard_cap_bytes) VALUES "
                "('Cap', 'capped-after', 'pri_after', 'pro_after', 100, 'monthly', 0, true)"
            )
        )

    assert _stored(at_parent, "capped-after", "hard_cap_bytes") is True


def test_the_priced_byte_columns_keep_no_default(at_parent, roundtrip_db_url):
    """AC8 — ``bytes_included`` and the overage price are the *priced* half of the rule.

    They are already correct and must not be "made symmetric" with the gate. A price has no
    safe reading of "nobody decided", which is why the answer there is no default at all
    (cloud#177, where a ``server_default`` of 1 cent silently billed). Asserted before and
    after so a later edit to this migration that tidies them in reds here.
    """
    for column in ("bytes_included", "overage_bytes_price_cents_per_gb"):
        assert _column_default(at_parent, column) is None, (
            f"{column} already carried a default before this migration"
        )

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    for column in ("bytes_included", "overage_bytes_price_cents_per_gb"):
        assert _column_default(at_parent, column) is None, (
            f"{column} gained a server default. A column that PRICES must have no default: "
            "absence means nobody decided, and there is no safe reading of that for money."
        )


def test_the_downgrade_restores_true_without_rewriting_rows(at_parent, roundtrip_db_url):
    """A rollback must re-arm the old default for NEW rows and leave existing rows alone.

    Restoring a default is not a backfill, and asserting that here is the point: a downgrade
    that "helpfully" set every row back to ``true`` would re-cap every paid tier on the way
    back — the exact damage this migration exists to undo, arriving through the one path
    nobody rehearses. core#726 is the standing proof that a value moved by a downgrade fails
    open.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-unblocked", "price": "pri_unb"})
    assert _stored(at_parent, "pro-unblocked", "hard_cap_bytes") is False

    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade failed: {down.stderr}"

    default = _column_default(at_parent, "hard_cap_bytes")
    assert default is not None and "true" in default, f"the default was not restored: {default!r}"
    assert _stored(at_parent, "pro-unblocked", "hard_cap_bytes") is False, (
        "the downgrade rewrote an existing row, re-capping a plan that had been correctly "
        "left unblocked"
    )
    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-after-down", "price": "pri_ad"})
    assert _stored(at_parent, "pro-after-down", "hard_cap_bytes") is True

    # And back up again, because a rollback is rarely the last thing that happens.
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _stored(at_parent, "free", "hard_cap_bytes") is True
