"""Regression test for the ``usage_ledger.quantity`` BigInt widening (core#272).

Asserts that the migration calls ``alter_column`` with ``type_=sa.BigInteger()`` on
``usage_ledger.quantity``. Live-DB round-trip is covered by ``test_roundtrip.py``; this is
a cheap static check so an accidental reversion shows up in CI without Postgres.

**Revision-graph reachability is NOT checked here, and that is deliberate (core#1062).**
This file used to carry ``test_migration_is_reachable_from_head``, whose final statement was
``assert cur is not None`` on a variable seeded with a string literal and only ever
reassigned from another revision id — unreachable, therefore a tautology. It passed against a
migration deliberately orphaned by pointing its ``down_revision`` at a nonexistent revision.

🟢 **The invariant was never unguarded, and removing this test closed no hole.** The same
mutation was caught by **23** other tests in ``tests/test_migrations/``, the precise one being
``test_migration_coverage.py::TestRevisionGraph::test_exactly_one_head``. Read that as the
guard for this property; a per-migration dict walk is a second, worse implementation of a
check that already exists one file over.

The second-order reason it could never have worked: a walk built from ``{parent: child}``
**cannot represent a branch**, since two migrations sharing a ``down_revision`` collapse to
one entry. A genuine fork — exactly the failure ``alembic heads`` reports — is invisible to it
by construction. Same lesson as the standing rule: validate against the real consumer.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_VERSIONS = Path(__file__).parent.parent.parent / "datanika" / "migrations" / "versions"
_MIGRATION = _VERSIONS / "b8c3d4e5f6g7_bigint_usage_ledger_quantity.py"


@pytest.fixture(scope="module")
def migration_source() -> str:
    assert _MIGRATION.exists(), f"Missing migration file: {_MIGRATION}"
    return _MIGRATION.read_text(encoding="utf-8")


def _alter_column_call(body: str, *, table: str, column: str) -> str | None:
    """The text of the one ``alter_column(...)`` call naming both ``table`` and ``column``.

    Whitespace-tolerant on purpose: the previous assertions pinned an exact newline plus eight
    spaces, so ``ruff format`` moving the arguments produced a red on a correct migration.
    """
    for match in re.finditer(r"alter_column\((.*?)\n    \)", body, re.DOTALL):
        args = match.group(1)
        if f'"{table}"' in args and f'"{column}"' in args:
            return args
    return None


class TestBigIntQuantityMigration:
    def test_revision_id_and_parent(self, migration_source):
        assert 'revision: str = "b8c3d4e5f6g7"' in migration_source
        assert 'down_revision: str | None = "a7b1c2d3e4f5"' in migration_source

    def test_upgrade_widens_to_bigint(self, migration_source):
        """Must call ``alter_column`` with ``type_=sa.BigInteger()`` on
        ``usage_ledger.quantity`` — all four facts inside ONE call (core#1062).

        This was four independent containment checks over the whole ``upgrade()`` body, which
        is the token-not-branch shape of core#1055: a migration widening a *different* column
        satisfied every one of them as long as the string ``"quantity"`` appeared anywhere in
        the function — a comment would have done it. Asserting one ``alter_column(...)`` call
        that names both the table and the column is what makes the four facts about the same
        statement.

        It also dropped ``'alter_column(\\n        "usage_ledger"'``, an exact newline plus
        eight spaces, which turns any reformat of the migration into a **false red** — the
        opposite failure and just as expensive.
        """
        upgrade_match = re.search(r"def upgrade\(\).*?(?=\ndef |\Z)", migration_source, re.DOTALL)
        assert upgrade_match
        upgrade_body = upgrade_match.group(0)

        call = _alter_column_call(upgrade_body, table="usage_ledger", column="quantity")
        assert call, (
            "upgrade() contains no alter_column() naming both usage_ledger and quantity.\n"
            f"--- upgrade() ---\n{upgrade_body}"
        )
        assert "type_=sa.BigInteger()" in call, f"widening absent from the call:\n{call}"
        assert "existing_type=sa.Integer()" in call, f"prior type absent from the call:\n{call}"

    def test_downgrade_reverses_the_widening(self, migration_source):
        """Downgrade should narrow back to Integer — even though narrowing
        may fail on rows that prompted the widening, the symmetry is what
        ``test_roundtrip.py`` exercises on an empty DB.
        """
        downgrade_match = re.search(
            r"def downgrade\(\).*?(?=\ndef |\Z)", migration_source, re.DOTALL
        )
        assert downgrade_match
        downgrade_body = downgrade_match.group(0)

        call = _alter_column_call(downgrade_body, table="usage_ledger", column="quantity")
        assert call, (
            "downgrade() contains no alter_column() naming both usage_ledger and quantity.\n"
            f"--- downgrade() ---\n{downgrade_body}"
        )
        assert "type_=sa.Integer()" in call, f"narrowing absent from the call:\n{call}"
        assert "existing_type=sa.BigInteger()" in call, f"prior type absent from the call:\n{call}"

    def test_control_the_call_matcher_can_miss(self):
        """The matcher must be able to return None, or every assertion above is vacuous.

        ``_alter_column_call`` is the transform the three tests above depend on; a version
        that returned the whole body regardless would make each of them a containment check
        over ``upgrade()`` again — i.e. exactly the defect core#1062 removed, restored
        silently. Both directions are pinned: it finds the real call, and it declines a call
        naming a different column.
        """
        real = _MIGRATION.read_text(encoding="utf-8")
        assert _alter_column_call(real, table="usage_ledger", column="quantity") is not None
        assert _alter_column_call(real, table="usage_ledger", column="not_a_column") is None
        assert _alter_column_call(real, table="not_a_table", column="quantity") is None
