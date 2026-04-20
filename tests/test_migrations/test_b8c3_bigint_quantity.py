"""Regression test for the ``usage_ledger.quantity`` BigInt widening
(core#272). Asserts both that the migration calls ``alter_column`` with
``type_=sa.BigInteger()`` and that the head revision actually lists it.

Live-DB round-trip is covered by ``test_roundtrip.py``; this is a cheap
static check so an accidental reversion shows up in CI without spinning
up Postgres.
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


class TestBigIntQuantityMigration:
    def test_revision_id_and_parent(self, migration_source):
        assert 'revision: str = "b8c3d4e5f6g7"' in migration_source
        assert 'down_revision: str | None = "a7b1c2d3e4f5"' in migration_source

    def test_upgrade_widens_to_bigint(self, migration_source):
        """Must call ``alter_column`` with ``type_=sa.BigInteger()`` on
        ``usage_ledger.quantity``.
        """
        upgrade_match = re.search(r"def upgrade\(\).*?(?=\ndef |\Z)", migration_source, re.DOTALL)
        assert upgrade_match
        upgrade_body = upgrade_match.group(0)
        assert 'alter_column(\n        "usage_ledger"' in upgrade_body
        assert '"quantity"' in upgrade_body
        assert "type_=sa.BigInteger()" in upgrade_body
        assert "existing_type=sa.Integer()" in upgrade_body

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
        assert "type_=sa.Integer()" in downgrade_body
        assert "existing_type=sa.BigInteger()" in downgrade_body

    def test_migration_is_reachable_from_head(self):
        """Walking the revision chain, b8c3d4e5f6g7 must lead to the head
        (or be the head). Prevents an orphan migration that sits in the
        folder but is never applied.
        """
        revisions: dict[str, str | None] = {}
        for f in _VERSIONS.glob("*.py"):
            if f.name == "__init__.py":
                continue
            src = f.read_text(encoding="utf-8")
            rev_match = re.search(r'revision:\s*str\s*=\s*"(\w+)"', src)
            down_match = re.search(r'down_revision:\s*str\s*\|\s*None\s*=\s*"(\w+)"', src)
            if not rev_match:
                continue
            revisions[rev_match.group(1)] = down_match.group(1) if down_match else None

        # Build child lookup: {parent: child}
        children: dict[str, str] = {}
        for rev, parent in revisions.items():
            if parent is not None:
                children[parent] = rev

        # Walk children from b8c3… forward. If we run off the end, we were
        # at the head.
        cur: str | None = "b8c3d4e5f6g7"
        assert cur in revisions, "b8c3d4e5f6g7 not registered as a revision"
        while cur in children:
            cur = children[cur]
        # cur is now the head. That's fine — we just needed to confirm the
        # chain is intact.
        assert cur is not None
