"""Verify every model table and column has a corresponding migration.

Catches the case where a new model or column is added but the developer
forgets to generate an Alembic migration, which leads to 'relation does
not exist' or 'column does not exist' errors in production.

Uses static analysis of migration files — no database connection required.
"""

import importlib
import pkgutil
import re
from pathlib import Path

import datanika.models as models_pkg
from datanika.models.base import Base


def _import_all_models():
    """Import all model modules to register them with Base.metadata."""
    pkg_path = Path(models_pkg.__file__).parent
    for info in pkgutil.iter_modules([str(pkg_path)]):
        if info.name == "base":
            continue
        importlib.import_module(f"datanika.models.{info.name}")


def _get_model_schema() -> dict[str, set[str]]:
    """Return {table_name: {col1, col2, ...}} from SQLAlchemy model metadata."""
    _import_all_models()
    schema = {}
    for table in Base.metadata.tables.values():
        schema[table.name] = {col.name for col in table.columns}
    return schema


def _scan_migration_files() -> tuple[set[str], dict[str, set[str]]]:
    """Scan migration files and return (tables, {table: columns}).

    Detects:
    - op.create_table("name", ...) — extracts table name and sa.Column names
    - op.rename_table("old", "new") — transfers columns from old → new name
    - op.add_column("table", sa.Column("col", ...)) — registers column addition

    Operations are processed in source order within each file so that a
    rename_table correctly captures the columns of the table at that point
    in time (any later create_table reusing the old name gets fresh columns).
    """
    versions_dir = Path(__file__).parent.parent.parent / "datanika" / "migrations" / "versions"
    tables: set[str] = set()
    columns: dict[str, set[str]] = {}

    create_table_re = re.compile(r'op\.create_table\(\s*"(\w+)"')
    rename_table_re = re.compile(r'op\.rename_table\(\s*"(\w+)"\s*,\s*"(\w+)"')
    column_in_create_re = re.compile(r'sa\.Column\(\s*"(\w+)"')
    add_column_re = re.compile(r'op\.add_column\(\s*"(\w+)"\s*,\s*sa\.Column\(\s*"(\w+)"')

    for f in sorted(versions_dir.glob("*.py")):
        content = f.read_text(encoding="utf-8")

        # Only look at upgrade() function
        upgrade_match = re.search(r"def upgrade\(\).*?(?=\ndef |\Z)", content, re.DOTALL)
        if not upgrade_match:
            continue
        upgrade_body = upgrade_match.group(0)

        # Collect all op calls with their source positions so we can
        # process them in order (rename_table must see the table's columns
        # as they exist at that exact point in the file).
        events: list[tuple[int, str, tuple]] = []
        for m in create_table_re.finditer(upgrade_body):
            events.append((m.start(), "create", (m.group(1), m.start())))
        for m in rename_table_re.finditer(upgrade_body):
            events.append((m.start(), "rename", (m.group(1), m.group(2))))
        for m in add_column_re.finditer(upgrade_body):
            events.append((m.start(), "add_col", (m.group(1), m.group(2))))
        events.sort(key=lambda e: e[0])

        for _pos, kind, args in events:
            if kind == "create":
                table_name, start = args
                tables.add(table_name)
                rest = upgrade_body[start:]
                block_end = len(rest)
                next_op = re.search(r"\n    op\.", rest[10:])
                if next_op:
                    block_end = next_op.start() + 10
                block = rest[:block_end]
                cols = column_in_create_re.findall(block)
                columns.setdefault(table_name, set()).update(cols)
            elif kind == "rename":
                old_name, new_name = args
                tables.add(new_name)
                if old_name in columns:
                    columns.setdefault(new_name, set()).update(columns.pop(old_name))
            elif kind == "add_col":
                table_name, col_name = args
                columns.setdefault(table_name, set()).add(col_name)

    return tables, columns


# Tables managed by cloud plugin — not in core migrations
_CLOUD_TABLES = {"plans", "subscriptions", "usage_ledger"}


class TestMigrationCoverage:
    def test_every_model_has_migration(self):
        """Every model table must exist in migrations."""
        model_schema = _get_model_schema()
        migrated_tables, _ = _scan_migration_files()

        model_tables = set(model_schema.keys()) - _CLOUD_TABLES
        missing_tables = model_tables - migrated_tables
        assert not missing_tables, (
            f"Models define tables that have no migration: {sorted(missing_tables)}. "
            f"Run: uv run alembic revision --autogenerate -m 'add <table>'"
        )

    def test_every_column_has_migration(self):
        """Every model column must appear in a migration (create_table or add_column)."""
        model_schema = _get_model_schema()
        _, migrated_columns = _scan_migration_files()

        missing = {}
        for table, model_cols in model_schema.items():
            if table in _CLOUD_TABLES:
                continue
            migration_cols = migrated_columns.get(table, set())
            if not migration_cols:
                continue  # missing table caught by other test
            diff = model_cols - migration_cols
            if diff:
                missing[table] = sorted(diff)

        assert not missing, (
            f"Model columns not found in any migration: {missing}. "
            f"Add a migration with op.add_column() for each missing column."
        )
