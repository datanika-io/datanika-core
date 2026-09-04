"""NULL ``plans.max_schedules`` on the four paid slugs — cloud#151 step 4.

`/pricing`, the homepage, `/features/volume-pricing/`, `/docs/scheduling-guide`, two
blog posts and the ``SoftwareApplication`` JSON-LD all advertise **"Unlimited
schedules"** on Pro and Enterprise. Production enforces ``max_schedules = 9999``.
Under the founder's standing option-(c) decision the published page stays and the
code moves. This is the last of cloud#151's four steps, and the only one that
changes data.

**Its gate was a read off the running image, not a branch comparison, and the gate
is now open.** Cloud ships *inside* the core image at a pinned ``ref: master``, so
a cloud promotion with no core promotion behind it is a change that has not
shipped. All three readers had to be in the serving image before any row could be
NULLed, or the first paying customer on an "Unlimited schedules" tier would meet a
``TypeError`` on their next schedule and a dead billing page on the one page that
would let them upgrade. What cleared it:

===========================================  =====================================
cloud ``master`` d96c654 @ 08:35:27+03:00    steps 1, 2 and 3a  (readers + model)
core  ``master`` c87b1ea @ 08:41:50+03:00    step 3b  (this column's DROP NOT NULL)
``deploy-pointer`` run 33841453310           **success**
===========================================  =====================================

Cloud was promoted **before** core, which is the required order and is what makes
the built image contain the readers. Infra should still confirm on the container
before promoting this, because the rule is *ask the running artifact* and the
table above is derived from GitHub:

    docker exec datanika-app /app/.venv/bin/python -c \\
      "import inspect; from datanika_cloud.billing.service import BillingService as B; \\
       print('max_schedules is None' in inspect.getsource(B.check_schedule_quota))"

(⚠️ the container's own interpreter, called directly — ``uv run`` re-syncs the venv
on the way to reporting on it, so it audits a repair rather than the artifact. And
read the serving colour first: ``datanika-app`` is blue, ``datanika-app-b`` green.)

**Expand/contract: DML only, no DDL.** No column is added, dropped, narrowed or
constrained. ``e3a5c7b9d1f4`` already dropped ``NOT NULL``, and it shipped in the
release that is serving now — so at ``t1`` the previously deployed container reads
these NULLs through code that was written for them. That ordering is the whole
content of cloud#151.

⚠️ **Unconditional by slug, with no value predicate — deliberately.** The obvious
``WHERE max_schedules = 9999`` would be wrong: core#928 establishes that **no
migration has ever created the paid rows**, so the whole chain's paid-slug UPDATEs
matched zero rows on the 2026-07-16 rebuild and the columns hold whatever created
them afterwards. Three different numbers are attested for this one column —
production measures ``9999``, the ``server_default`` is ``10``, and
``o4k1l2m3n5h6``'s downgrade writes ``999999`` — so a value predicate would skip
exactly the rows it could not predict. `e2e_admin.py` writes ``bytes_included``
directly but **not** this column, so the ``IS NULL`` guard that ``c1d2e3f4a5b6``
needed has no counterpart here.

🚨 **This does NOT fix a from-scratch build, and that is a separate defect.** On a
fresh database alembic runs to head against an empty ``plans``, this UPDATE matches
zero rows, and the paid rows are created out of band afterwards taking the column's
``server_default`` of **10**. That is the core#928 class, it is **pre-existing** —
a fresh build gets 10 today, before and after this migration, since no migration
has ever set this column for a paid slug — and this migration therefore regresses
nothing. Fixing it means dropping the default in core *and* the cloud model as a
pair, which is its own decision and is filed as **core#1047**.

⚠️ ``tests/test_migrations/test_plan_seed_updates_reach_real_rows.py`` does **not**
catch that, and I checked rather than assumed: it reports 7 passed against this
file. Two measured blind spots, filed as **core#1048** — its ``server_default``
extractor requires ``server_default=sa.text("…")`` while ``j9f6g7h8i0c1`` writes
``server_default="10"`` as a bare string (so ``max_schedules``, ``hard_cap_runs``
and ``bytes_included`` are skipped entirely, on the branch whose comment says the
source "cannot say" what the row holds), and its slug extractor reads inline SQL
literals, so any ``WHERE slug IN ({interpolated})`` is invisible to it.

Revision ID: a9c4e2b7d5f3
Revises: e3a5c7b9d1f4
Create Date: 2026-09-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a9c4e2b7d5f3"
down_revision: str | None = "e3a5c7b9d1f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The four slugs sold with "Unlimited schedules". Same set, same order and the
#: same justification as ``c1d2e3f4a5b6._NO_MID_CYCLE_BLOCK`` — the annual slugs
#: carry their monthly tier's entitlement, which is what ``seed_annual_plans.py``
#: asserts ("annual and monthly differ in price and cadence, never in quota").
#:
#: ⚠️ ``free`` is deliberately absent. Its cap of **2** is published on the same
#: page, is enforced today, and is the one plan row a migration actually creates
#: (``q6m3n4o5p7j8`` INSERTs it with ``max_schedules`` supplied explicitly), so it
#: is also the one row that would be reached if this ever grew a wildcard.
UNLIMITED_SCHEDULE_SLUGS: tuple[str, ...] = (
    "pro-monthly",
    "pro-annual",
    "enterprise-monthly",
    "enterprise-annual",
)

#: What a NULL is restored to on the way down. 9999 is the fair-use figure the four
#: paid slugs carry in production today, so it is faithful for every row this
#: migration NULLs. NULL has no exact integer inverse, so this is a choice and is
#: stated rather than implied — and it matches ``e3a5c7b9d1f4._DOWNGRADE_FILL``, so
#: downgrading past both revisions lands on one number rather than two.
_DOWNGRADE_FILL = 9999


def _slug_list() -> str:
    return ", ".join(f"'{s}'" for s in UNLIMITED_SCHEDULE_SLUGS)


def upgrade() -> None:
    op.execute(sa.text(f"UPDATE plans SET max_schedules = NULL WHERE slug IN ({_slug_list()})"))


def downgrade() -> None:
    # ``AND max_schedules IS NULL`` here, and no value predicate on the way up.
    # That asymmetry is deliberate, not an oversight: the upgrade cannot predict
    # the current value (three are attested), while the downgrade knows exactly
    # which rows it is undoing — the ones holding NULL. A row someone has since
    # given a finite ceiling was not NULLed by this migration, and a rollback is
    # the worst moment to overwrite a deliberate value.
    op.execute(
        sa.text(
            f"UPDATE plans SET max_schedules = {_DOWNGRADE_FILL} "
            f"WHERE slug IN ({_slug_list()}) AND max_schedules IS NULL"
        )
    )
