"""Enforce the expand/contract migration policy at PR time.

Why this exists
---------------
Blue/green deploys start the new container *while the old one still serves*, and
``alembic upgrade head`` runs in its startup command. So there is a window where the
**previously deployed code runs against the new schema**:

    t0  old code + old schema      <- serving
    t1  old code + NEW schema      <- serving, new container already migrated   *** here ***
    t2  new code + new schema

A migration that removes or tightens something breaks production at ``t1`` -- not at the
swap, and **not in CI**, which only ever runs one version against one schema.

Policy: ``plans/infra/SPEC_EXPAND_CONTRACT_MIGRATIONS.md`` (#449).

The policy shipped as a document whose only control was a human answering a checklist
question -- the same weakness that let promotion-ref enumeration leak until it was
automated. This is the machine-checked half.

What it does NOT do
-------------------
It cannot prove a migration is safe; it flags the operations that are *categorically*
unsafe in a single release and demands the author say why. The heavier guard -- running
the previously-deployed release's test suite against the new schema -- is still the real
answer for correctness, and is not built.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

MIGRATIONS = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "migrations" / "versions"

# Operations that are unsafe in the same release as the code needing them, because the
# previously-deployed version is still running against the migrated schema at t1.
DROP_CALLS = {"drop_column", "drop_table", "drop_constraint", "rename_table"}

# Migrations that predate the policy. They are grandfathered, NOT endorsed: each one
# would have needed splitting across two releases under the current rule. Deliberately an
# explicit list rather than a date cutoff — a date drifts silently, a list has to be
# edited on purpose, and editing it is exactly the moment someone should think twice.
PRE_POLICY_BASELINE = {
    "b8c3d4e5f6g7_bigint_usage_ledger_quantity.py",
    "c9d4e5f6g7h8_widen_notification_type_varchar.py",
    "d0e5f6g7h8i9_bigint_runs_rows_loaded.py",
    "d5d30abce35c_add_catalog_entries_table.py",
    "e1f2a3b4c5d6_rename_pipeline_to_upload_add_dbt_pipelines.py",
    "i8e5f6g7h9b0_rename_ls_columns_to_paddle.py",
}

# A migration may carry a destructive op if it states which release stopped using the old
# shape. Free-form on purpose: the value is the author having to think, not the format.
JUSTIFICATION = "expand-contract:"


def _destructive_ops(path: pathlib.Path) -> list[str]:
    """Destructive operations in ``upgrade()``.

    ``create_table`` bodies are excluded structurally: ``nullable=False`` on a brand-new
    table is harmless, and a regex over the file text flags 23 of 33 migrations, which
    would make the guard noise people learn to bypass. ``downgrade()`` is ignored — it is
    destructive by definition and never runs in the t1 window.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    upgrade = next(
        (n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "upgrade"), None
    )
    if upgrade is None:
        return []

    found: list[str] = []
    for node in ast.walk(upgrade):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")

        if name in DROP_CALLS:
            found.append(name)
        elif name == "create_unique_constraint":
            # Old code may still be writing duplicates.
            found.append("add_unique_constraint")
        elif name == "alter_column":
            kwargs = {k.arg: k for k in node.keywords}
            if "new_column_name" in kwargs:
                found.append("rename_column")
            nullable = kwargs.get("nullable")
            if nullable is not None and isinstance(nullable.value, ast.Constant):
                if nullable.value.value is False:
                    # Old code may still insert NULLs.
                    found.append("set_not_null")
            if "type_" in kwargs:
                # Widening is safe and narrowing is not, and the two are not reliably
                # distinguishable from the AST alone — so ask the author.
                found.append("type_change")

    return sorted(set(found))


def _migration_files() -> list[pathlib.Path]:
    return sorted(p for p in MIGRATIONS.glob("*.py") if p.name != "__init__.py")


def test_migrations_directory_is_found() -> None:
    """Guard the guard: a wrong path would make every assertion below vacuously pass."""
    assert MIGRATIONS.is_dir(), f"migrations directory not found at {MIGRATIONS}"
    assert len(_migration_files()) > 10, "suspiciously few migrations — is the path right?"


@pytest.mark.parametrize("path", _migration_files(), ids=lambda p: p.name)
def test_migration_is_expand_only_or_justified(path: pathlib.Path) -> None:
    """New migrations must be expand-only, grandfathered, or explicitly justified."""
    ops = _destructive_ops(path)
    if not ops:
        return
    if path.name in PRE_POLICY_BASELINE:
        return
    if JUSTIFICATION in path.read_text(encoding="utf-8"):
        return

    pytest.fail(
        f"{path.name} performs {', '.join(ops)} in upgrade().\n\n"
        "Under blue/green the PREVIOUSLY DEPLOYED code runs against this schema while the\n"
        "swap is in flight, so removing or tightening breaks production at that moment —\n"
        "and CI cannot catch it, because CI only runs one version against one schema.\n\n"
        "Split it across releases (expand in N, contract in N+1), or if it is genuinely\n"
        "safe, say why in a comment containing 'expand-contract:' — e.g.\n"
        "    # expand-contract: contract phase; release 0.3.0 stopped reading this column\n\n"
        "Policy: plans/infra/SPEC_EXPAND_CONTRACT_MIGRATIONS.md (#449)"
    )


def test_baseline_entries_all_exist() -> None:
    """A stale baseline silently re-permits whatever it names.

    If a grandfathered migration is renamed or deleted, its entry stops matching anything
    and quietly loses meaning — the same drift that makes hand-maintained lists rot.
    """
    names = {p.name for p in _migration_files()}
    missing = sorted(PRE_POLICY_BASELINE - names)
    assert not missing, (
        f"baseline names migrations that no longer exist: {missing}. "
        "Remove them — a stale entry grandfathers nothing and hides that fact."
    )


def test_baseline_entries_are_actually_destructive() -> None:
    """Baseline entries must still need grandfathering.

    If one becomes clean, it should leave the list — otherwise the baseline grows into a
    blanket exemption nobody re-examines, which is how an allowlist stops being a control.
    """
    stale = [
        name
        for name in sorted(PRE_POLICY_BASELINE)
        if (MIGRATIONS / name).exists() and not _destructive_ops(MIGRATIONS / name)
    ]
    assert not stale, f"baseline entries no longer destructive, remove them: {stale}"
