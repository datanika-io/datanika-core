"""``plans.hard_cap_bytes`` defaults to ``false`` — core#1071.

`/pricing` publishes, verbatim: *"Free is hard-capped at 10 GB — runs stop. **Pro and
Enterprise bill overage** at $0.50/GB and $0.25/GB … **No surprise mid-cycle blocks.**"*
The founder's standing rule is that the published page is the acceptance criterion
(coordinator decision 1, 2026-08-31), so this is not a judgement about what is reasonable.

``z5v2w3x4y6a7`` gave this column ``server_default = true``. **No migration creates a paid
slug** — over the whole chain the only slug any migration ``INSERT``s is ``free``
(``q6m3n4o5p7j8``) — so every paid row is created out of band, afterwards, and takes the
default. Measured by Infra on 2026-09-04 against a from-scratch rebuild of the serving
image: a rebuilt production carries ``hard_cap_bytes = true`` on all four paid slugs, i.e.
**it blocks Pro and Enterprise on volume mid-cycle**, which is the one behaviour the page
promises it will not do. That is core#928's asymmetry, and this column is its sharpest
instance because the failure is visible to the customer as an outage rather than as a wrong
number on an invoice.

🔑 **Its own sibling settles the direction.** ``hard_cap_runs`` is the same kind of column in
the same table with the same semantics, and ``k0g7h8i9j1d2`` gives it ``server_default =
false``. **Nobody chose the difference** — there is no issue, comment or commit message
anywhere that argues for it. Product's rule, of which this is the third instance
(``max_schedules`` core#1047, ``overage_run_price_cents`` cloud#177):

    A column that GATES may fail open. A column that PRICES must have no default at all.

Absence means *nobody decided*. For a gate the safe reading of that is "do not block" — a
wrongly permissive gate under-charges, and that is recoverable. For a price there is no safe
reading, which is why the answer there is no default rather than a lenient one.
``bytes_included`` and ``overage_bytes_price_cents_per_gb`` are the priced half, already
carry no default, and are deliberately **not** touched here.

🚨 **DDL ONLY. NO DML — and that is the load-bearing half.**
``free`` is ``hard_cap_bytes = true`` in production and that is **correct**: the page sells
Free as capped at 10 GiB and cloud enforces it today. A blanket ``UPDATE plans SET
hard_cap_bytes = false`` — the shape ``c5e9a3b7d2f4`` legitimately used for
``overage_run_price_cents`` — would un-cap Free and make a *different* published claim false.
Existing rows keep exactly what they hold; only a row created later without naming the column
is affected. Pinned by
``tests/test_migrations/test_e8b3_hard_cap_bytes_defaults_false.py::test_the_free_plan_keeps_its_published_hard_cap``
and by ``::test_an_existing_hard_capped_row_is_left_alone``.

⚠️ **A correction migration by slug is not an alternative here, and it is what core#928,
core#1047 and core#1060 all propose.** That is right for repairing rows that exist and does
nothing for this: every one of these defects arose because a row was created out of band
*afterwards* and took the default, so a slug-keyed UPDATE runs at the same point in the chain
and matches zero rows for the same reason the UPDATE it corrects did.
**Correction migrations fix the past; the default is the only thing that fixes the future.**

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Changing a default is neither
destructive nor narrowing: it rewrites no row and alters no type, so at ``t1`` the previously
deployed code reads exactly the values it read before, and a Celery worker still running the
old model meets nothing new. The model's own ``default=`` is used by the ORM for INSERTs it
issues, not for DDL, so the paired cloud change is independent in both directions —
``datanika-cloud/tests/test_migration_coverage.py`` compares column **names** only
(cloud#171), so neither half is enforced by a test. They ship together because the pair is
one decision, not because a test makes them.

Pairs with ``datanika_cloud/billing/models.py`` (``Plan.hard_cap_bytes`` → ``default=False``)
and with ``check_bytes_quota`` reading its gate alone, both in
datanika-io/datanika-cloud#. See core#1071.

🔔 **This moves the rebuild-parity drill's pinned fingerprint and it is a SHRANK.**
``deploy/server/rebuild-parity-drill.sh`` pins ``EXPECTED_GAP=d0e77bd8d96b9219`` (26 columns,
4 missing slugs), measured against alembic head ``b4d8f1a2c6e9``. Four of those 26 lines are
``<paid slug>.hard_cap_bytes: production=false default_would_give=true``; this migration
makes the rebuilt default agree with production, so they disappear and the gap becomes 22.
**The new fingerprint cannot be computed from this repository** — it is a sha256 over the
live catalogue and only the box can produce it. Infra re-measures against the ``:staging``
image after this merges, exactly as they did for core#1047, and re-pins in that commit.

✅ **Done 2026-09-06 (Infra): the pin is now ``b46433131dde4c20``, 22 columns.** Measured on
the box against ``:staging`` (head ``f1a4c8e2d6b3``), with ``:latest`` (head ``d7f2c8a4b1e6``)
run against the same production rows as the control — it still returned the old
``d0e77bd8d96b9219`` / 26, which is what attributes the move to this migration rather than to
production drifting. The predicted 22 held; the fingerprint was measured, never computed.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e8b3d5c7f2a9"
down_revision: str | None = "d7f2c8a4b1e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: What the column carried before this migration, restored on the way down. Named rather than
#: inlined so the downgrade cannot drift from what ``z5v2w3x4y6a7`` declared.
_PREVIOUS_DEFAULT = "true"

#: What it carries after, and what ``hard_cap_runs`` has carried since ``k0g7h8i9j1d2``.
_NEW_DEFAULT = "false"


def upgrade() -> None:
    # `server_default=sa.text(...)` is alembic's SET DEFAULT. Omitting the argument means
    # "leave the default alone", so a refactor that drops this line changes behaviour
    # silently — the two are distinguished by a sentinel, not by falsiness.
    op.alter_column("plans", "hard_cap_bytes", server_default=sa.text(_NEW_DEFAULT))


def downgrade() -> None:
    # No row rewrite, on purpose — in either direction. Restoring a default governs future
    # INSERTs only; rows a release left unblocked stay unblocked, which is what a rollback
    # should preserve. Backfilling on the way down would re-cap every paid tier, i.e. do the
    # damage this migration exists to undo, through the path nobody rehearses (core#726).
    op.alter_column("plans", "hard_cap_bytes", server_default=sa.text(_PREVIOUS_DEFAULT))
