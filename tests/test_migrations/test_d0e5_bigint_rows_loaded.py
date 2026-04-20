"""Regression test for the ``runs.rows_loaded`` BigInt widening
(core#283). Guards against accidental reversion to int32 that would
silently reject any Enterprise backfill ≥ 2^31 rows.

Live-DB round-trip is covered by ``test_roundtrip.py`` (the probe that
shipped xfailed in core#282 and is now a passing test here in core#283).
This is a cheap static check so a reversion surfaces in CI without
Postgres.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_VERSIONS = Path(__file__).parent.parent.parent / "datanika" / "migrations" / "versions"
_MIGRATION = _VERSIONS / "d0e5f6g7h8i9_bigint_runs_rows_loaded.py"


@pytest.fixture(scope="module")
def migration_source() -> str:
    assert _MIGRATION.exists(), f"Missing migration file: {_MIGRATION}"
    return _MIGRATION.read_text(encoding="utf-8")


class TestBigIntRowsLoadedMigration:
    def test_revision_id_and_parent(self, migration_source):
        assert 'revision: str = "d0e5f6g7h8i9"' in migration_source
        assert 'down_revision: str | None = "c9d4e5f6g7h8"' in migration_source

    def test_upgrade_widens_to_bigint(self, migration_source):
        upgrade_match = re.search(r"def upgrade\(\).*?(?=\ndef |\Z)", migration_source, re.DOTALL)
        assert upgrade_match
        upgrade_body = upgrade_match.group(0)
        assert 'alter_column(\n        "runs"' in upgrade_body
        assert '"rows_loaded"' in upgrade_body
        assert "type_=sa.BigInteger()" in upgrade_body
        assert "existing_type=sa.Integer()" in upgrade_body

    def test_downgrade_narrows_back(self, migration_source):
        downgrade_match = re.search(
            r"def downgrade\(\).*?(?=\ndef |\Z)", migration_source, re.DOTALL
        )
        assert downgrade_match
        downgrade_body = downgrade_match.group(0)
        assert "type_=sa.Integer()" in downgrade_body
        assert "existing_type=sa.BigInteger()" in downgrade_body


class TestRunModelColumnType:
    """Model-level regression. SQLite maps both Integer and BigInteger
    to one INTEGER type so insertion tests can't see the boundary; the
    declaration is the authoritative statement.
    """

    def test_rows_loaded_is_bigint(self):
        from sqlalchemy import BigInteger

        from datanika.models.run import Run

        col = Run.__table__.c.rows_loaded
        assert isinstance(col.type, BigInteger), (
            f"Run.rows_loaded must be BigInteger (got {type(col.type).__name__}). "
            "Enterprise backfills overflow int32 — see core#283."
        )
