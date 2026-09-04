"""Model runs are not billed — zero the price and drop its default (cloud#177).

**Founder decision, 2026-09-04**, verbatim: *"If the page describes this run as free, it
must be free — what even is the question?"* The framing offered was a choice between
publishing a runs overage rate and giving one up. It was rejected in favour of the rule
this project already applies to `/pricing`: **the published page is the acceptance
criterion, and the code moves.**

What disagreed
--------------
+-------------------------------------+--------------------------------------------------+
| ``pricing-tiers.ts`` header         | *"a secondary, **non-billed** fair-use quota"*    |
| ``SPEC_PRICING_V2`` §2.2            | a *"fair-use orchestration limit"*                |
| ``/pricing``, published             | ``15,000 model runs / month`` — **and no rate**   |
| this schema, before now             | 1 cent/run, uncapped, on all four paid slugs      |
+-------------------------------------+--------------------------------------------------+

``hard_cap_runs`` is ``false`` on every paid slug since core#713, so the runs dimension
did not *block* — it **billed**, at a rate no surface we own discloses. A fair-use limit
that silently bills is not a fair-use limit.

Two changes, and they answer different questions
------------------------------------------------
1. ``UPDATE plans SET overage_run_price_cents = 0`` — **deliberately unqualified.**
2. ``ALTER COLUMN … DROP DEFAULT`` — absence stops meaning "charge".

🚨 **Why the UPDATE carries no ``WHERE slug``.** Every other plan correction in this tree
keys on slug (``o4k1l2m3n5h6``, ``f6a7b8c9d0e1``, ``a9c4e2b7d5f3``) and each inherits the
same defect: **no migration creates a paid slug** — over the whole chain the only slug any
migration ``INSERT``s is ``free`` — so on a from-scratch database a slug-keyed ``UPDATE``
matches **zero rows** and the paid rows are created afterwards, out of band, taking the
default. That is core#928's asymmetry, and core#1047 and core#1060 are two more instances
of it. A slug list also silently misses rows nobody enumerated: the ``e2e-*`` plans
``datanika-cloud/billing/e2e_admin.py`` creates, and whatever a future environment names
its own. *"Model runs are not billed"* is a statement about the product, not about four
strings, so the ``UPDATE`` is written as one. Pinned by
``test_a_slug_no_list_could_have_named_is_zeroed_too``, which a slug-keyed version fails
and every other test in that file passes.

⚠️ This **supersedes** ``q6m3n4o5p7j8``'s ``UPDATE plans SET overage_run_price_cents = 1
WHERE slug = 'enterprise-monthly'``, which is left in place: rewriting a shipped migration
is worse than being superseded by a later one, and on a fresh build that statement already
matched zero rows for the reason above.

🔑 **The pairing, which is worth more than either fix.** ``max_schedules`` carried
``server_default="10"``: *absence gave product away*, corrected in ``b4d8f1a2c6e9``
(core#1047). ``overage_run_price_cents`` carried ``server_default="1"``: *absence takes
money*. Same ``create_table`` in ``j9f6g7h8i0c1``, opposite failure directions, neither
default deliberate. **A column that only gates may fail open; a column that prices must
not have a default at all.**

⚠️ **And this is why the two migrations are not mirror images.** ``b4d8f1a2c6e9`` also
made ``max_schedules`` *nullable*, because NULL there means *uncapped* and three readers
honour it. This column stays ``NOT NULL`` on purpose: NULL has **no reader** here —
``datanika_cloud/billing/tasks.py::_overage_price_cents`` evaluates
``plan.overage_run_price_cents * overage_quantity`` with no None handling — so a nullable
column would move the failure from INSERT time to charge time, inside the hourly billing
tick. ``NOT NULL`` with no default turns an omitted price into a refusal at the moment the
row is created, which is the only place anyone can act on it.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Both statements are safe for the
*previously deployed* code, which is what blue/green demands and CI cannot check:

* ``DROP DEFAULT`` is a loosening. It rewrites no row and narrows no type. The old
  container's cloud model still carries a Python-side ``default=1``, so ORM inserts from
  it keep supplying a value and cannot meet the ``NOT NULL``.
* Writing ``0`` is a value the old code already reads correctly:
  ``_issue_charge_for_cycle`` computes ``amount_cents <= 0`` and skips the charge. It
  does not raise, and the direction of the change is *stop charging*, which is the safe
  one to be running on either side of a swap.
* Celery workers restart separately and meet the new values for longer than the web swap.
  Same conclusion: they stop issuing runs charges, which is the intent.

Pairs with dropping ``default=1`` from ``datanika_cloud/billing/models.py`` and making
``billing/e2e_admin.py`` state a price. ⚠️ ``datanika-cloud/tests/test_migration_coverage.py``
compares column **names** only, so that pair has no red window in either direction
(cloud#171) — neither half is enforced by a test, only by shipping them together.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c5e9a3b7d2f4"
down_revision: str | None = "b4d8f1a2c6e9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: What the column carried before this migration, restored on the way down. Named rather
#: than inlined so the downgrade cannot drift from what ``j9f6g7h8i0c1`` declared.
_PREVIOUS_DEFAULT = "1"

#: The price every plan carries after this migration. Model runs are a fair-use quota;
#: `/pricing` publishes an allowance and no overage rate, so the rate is zero.
NOT_BILLED = 0


def upgrade() -> None:
    # No WHERE clause, on purpose — see the module docstring. A slug list is how every
    # earlier plan correction in this tree quietly missed the rows that matter.
    op.execute(f"UPDATE plans SET overage_run_price_cents = {NOT_BILLED}")

    # `server_default=None` is alembic's DROP DEFAULT. It is not the same as omitting the
    # argument, which means "leave the default alone" — the two are distinguished by a
    # sentinel, so a refactor that drops this line changes behaviour silently.
    op.alter_column("plans", "overage_run_price_cents", server_default=None)


def downgrade() -> None:
    # Restore the DDL, deliberately NOT the prices. A true inverse would re-price every
    # row this zeroed, and that would start billing customers — on a dimension `/pricing`
    # publishes as included — at the moment an operator is rolling back for an unrelated
    # reason. Under-billing is recoverable; a surprise charge on a page that promised none
    # is not. Re-adding a default governs future INSERTs only, which is the whole change.
    op.alter_column("plans", "overage_run_price_cents", server_default=sa.text(_PREVIOUS_DEFAULT))
