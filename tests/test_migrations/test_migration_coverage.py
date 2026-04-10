"""Verify every model table has a corresponding migration.

Catches the case where a new model is added but the developer forgets
to generate an Alembic migration, which leads to 'relation does not exist'
errors in production.
"""

import importlib
import pkgutil
import re
from pathlib import Path

import datanika.models as models_pkg


def _collect_model_tables() -> set[str]:
    """Import all model modules and extract __tablename__ values."""
    tables = set()
    pkg_path = Path(models_pkg.__file__).parent
    for info in pkgutil.iter_modules([str(pkg_path)]):
        if info.name == "base":
            continue
        mod = importlib.import_module(f"datanika.models.{info.name}")
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if isinstance(attr, type) and hasattr(attr, "__tablename__"):
                tablename = getattr(attr, "__tablename__", None)
                if tablename and not tablename.startswith("_"):
                    tables.add(tablename)
    return tables


def _collect_migrated_tables() -> set[str]:
    """Scan all migration files for create_table/rename_table and CREATE TABLE."""
    tables = set()
    versions_dir = Path(__file__).parent.parent.parent / "datanika" / "migrations" / "versions"
    # Match: op.create_table("name" or CREATE TABLE name or rename_table(..., "name")
    create_pattern = re.compile(
        r"""(?:create_table|CREATE\s+TABLE)\s*\(?\s*['"](\w+)['"]""", re.I
    )
    # rename_table("old", "new") — the new name is the second arg
    rename_pattern = re.compile(
        r"""rename_table\s*\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]""", re.I
    )
    for f in versions_dir.glob("*.py"):
        content = f.read_text(encoding="utf-8")
        for match in create_pattern.finditer(content):
            tables.add(match.group(1))
        for match in rename_pattern.finditer(content):
            tables.add(match.group(2))  # new table name
    return tables


# Tables managed by cloud plugin — not in core migrations
_CLOUD_TABLES = {"plans", "subscriptions", "usage_ledger"}


class TestMigrationCoverage:
    def test_every_model_has_migration(self):
        model_tables = _collect_model_tables() - _CLOUD_TABLES
        migrated_tables = _collect_migrated_tables()
        missing = model_tables - migrated_tables
        assert not missing, (
            f"Models define tables that have no migration: {sorted(missing)}. "
            f"Run: uv run alembic revision --autogenerate -m 'add <table>'"
        )
