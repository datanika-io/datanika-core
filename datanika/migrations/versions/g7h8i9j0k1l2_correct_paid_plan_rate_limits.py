"""Correct rate_limit_rpm on the paid plan rows the April UPDATE never reached.

``r7n4o5p6q8k9`` runs::

    UPDATE plans SET rate_limit_rpm = 120 WHERE slug = 'pro-monthly'
    UPDATE plans SET rate_limit_rpm = 300 WHERE slug = 'enterprise-monthly'

**No migration has ever created those rows.** Over the whole chain the only slug any migration
``INSERT``s is ``free`` (``q6m3n4o5p7j8``); the four paid slugs are created out of band — annual
by ``datanika-cloud/scripts/seed_annual_plans.py``, monthly by something in neither repo. So on
the from-scratch 2026-07-16 rebuild every paid-slug UPDATE matched **zero rows**, and the rows
were created afterwards holding the column's ``server_default`` of **60**.

The consequence is live on a paid tier: ``/api/reference#rate-limits`` and ``/docs/ai-agents``
both publish **Free 30 · Pro 120 · Enterprise 300**, so a rebuilt database serves **half** the
published Pro limit and **a fifth** of Enterprise. Same class as ``f6a7b8c9d0e1`` (core#780),
which corrected ``max_parallel_runs`` and ``sso_enabled``; this is core#928's next column, found
independently by Growth from the published end while Engineering had the mechanism.

🔴 **The deferral this replaces gave a reason that conflated two columns.**
``test_plan_seed_updates_reach_real_rows.py`` listed both pairs as *"NOT corrected
deliberately … the burst claim was DELETED rather than implemented (core#703), so the April
migration's 120 is an intent with nothing corroborating it."*

What core#703 deleted is the per-**second** burst column, **which is not a column on ``Plan`` at
all** — the published page now says so in its own words: *"We deliberately do not print the
ceiling as a figure here. It is an operational setting (``API_RATE_LIMIT_BURST``), not a plan
entitlement … the per-plan burst column this section used to carry was exactly that mistake."*
The per-**minute** figure is a plan entitlement, it is published in two places on ``main``, and
it equals this migration's intent exactly. There was nothing to decide.

⚠️ **What this migration does NOT do, stated because the same table already records it for four
other columns.** It repairs deployments where the paid rows **already exist** — production, and
any environment seeded before this runs. It does **not** fix the from-scratch case: it runs at
the end of the chain, and on a fresh build the paid rows are created *after* ``alembic upgrade
head``, so this UPDATE matches zero rows for exactly the reason the original did. The real fix
is deciding how paid rows get created at all (core#928 AC4 / core#1060), and that is a larger
decision than a data migration. The per-slug ``rows matched`` line below is what tells the two
cases apart in a deploy log.

Revision ID: g7h8i9j0k1l2
Revises: f1a4c8e2d6b3
Create Date: 2026-09-07 18:40:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "g7h8i9j0k1l2"
down_revision: str | None = "f1a4c8e2d6b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


#: slug -> published requests-per-minute allowance.
#:
#: Taken from `/api/reference#rate-limits` and `/docs/ai-agents`, which agree, and which also
#: agree with `r7n4o5p6q8k9`'s intent — so the founder's *"the page wins"* ruling and the
#: migration's own value point at the same number and nothing had to be chosen.
#:
#: `free` is listed although it is already correct: it is the one slug a migration `INSERT`s, so
#: its April UPDATE matched. Including it makes this the complete published table rather than a
#: list of exceptions, and its `rows matched: 1` in the deploy log is the control that tells a
#: working migration from one whose WHERE clause matches nothing at all.
#:
#: The annual slugs carry their monthly tier's allowance. `seed_annual_plans.py`'s
#: COPIED_QUOTA_COLUMNS already includes `rate_limit_rpm`, so an annual row seeded *after* this
#: inherits the corrected value — but one seeded *before* it holds 60, which is why they are
#: named here rather than left to the copy.
PUBLISHED_RATE_LIMIT_RPM: dict[str, int] = {
    "free": 30,
    "pro-monthly": 120,
    "pro-annual": 120,
    "enterprise-monthly": 300,
    "enterprise-annual": 300,
}

#: What the column falls back to when the row is created outside the chain. Named so the
#: downgrade below cannot drift from it.
SERVER_DEFAULT_RPM = 60


def _apply(conn, sql: str, params: dict, label: str) -> None:
    result = conn.execute(sa.text(sql), params)
    # `print`, not `logging` — same reasoning as f6a7b8c9d0e1. Migrations run from the
    # container start command, so stdout reaches the deploy log unconditionally; a logger
    # can be silenced by configuration, and a report that can be silenced is the exact
    # failure this migration exists to correct.
    print(
        f"[core#928] {label:<44} rows matched: {result.rowcount}"
        + ("   <-- ZERO: row absent at this point in the chain" if not result.rowcount else "")
    )


def upgrade() -> None:
    conn = op.get_bind()
    for slug, value in PUBLISHED_RATE_LIMIT_RPM.items():
        _apply(
            conn,
            "UPDATE plans SET rate_limit_rpm = :v WHERE slug = :s",
            {"v": value, "s": slug},
            f"rate_limit_rpm={value} slug={slug}",
        )


def downgrade() -> None:
    """Revert only a row still holding exactly what ``upgrade()`` wrote.

    core#726's lesson, and f6a7b8c9d0e1 applies the same rule: a blanket
    ``SET rate_limit_rpm = 60`` would discard an operator's deliberately raised ceiling and
    hand the next ``upgrade()`` a value it would overwrite anyway. Matching on the current
    value leaves a customised row alone in both directions.
    """
    conn = op.get_bind()
    for slug, value in PUBLISHED_RATE_LIMIT_RPM.items():
        conn.execute(
            sa.text("UPDATE plans SET rate_limit_rpm = :d WHERE slug = :s AND rate_limit_rpm = :v"),
            {"d": SERVER_DEFAULT_RPM, "s": slug, "v": value},
        )
