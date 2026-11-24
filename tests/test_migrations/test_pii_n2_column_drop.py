"""core#939 release N+2 drops the five legacy columns.

The unmap revision ``f8b2c4d6e1a3`` left them in the database. This revision
is the contract. ``audit_logs.ip_address`` stays. ``upgrade()`` contains no
``op.drop_table``.
"""

from __future__ import annotations

import ast
from pathlib import Path

VERSIONS = Path(__file__).resolve().parents[2] / "datanika" / "migrations" / "versions"
PARENT = "f8b2c4d6e1a3"
DROPPED = {
    ("users", "email"),
    ("users", "full_name"),
    ("users", "oauth_provider_id"),
    ("invitations", "email"),
    ("invitations", "token"),
}


def _children() -> list[Path]:
    found: list[Path] = []
    for path in sorted(VERSIONS.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        marker = f'"{PARENT}"'
        if f"down_revision: str | None = {marker}" in text or f"down_revision = {marker}" in text:
            found.append(path)
    return found


def _function(path: Path, name: str) -> ast.FunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _calls(fn: ast.FunctionDef, name: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == name
    ]


def _const_args(call: ast.Call) -> tuple[object, ...]:
    return tuple(arg.value for arg in call.args if isinstance(arg, ast.Constant))


def test_the_contract_revision_drops_only_the_five_legacy_columns():
    children = _children()
    assert len(children) == 1, children
    path = children[0]
    text = path.read_text(encoding="utf-8")
    upgrade = _function(path, "upgrade")
    dropped = {_const_args(call) for call in _calls(upgrade, "drop_column")}
    assert dropped == DROPPED
    assert _calls(upgrade, "drop_table") == []
    assert "expand-contract:" in text
    assert "one_way" not in text


def _added_columns(fn: ast.FunctionDef) -> set[tuple[object, ...]]:
    found: set[tuple[object, ...]] = set()
    for call in _calls(fn, "add_column"):
        table = call.args[0]
        column = call.args[1]
        assert isinstance(table, ast.Constant)
        assert isinstance(column, ast.Call)
        name = column.args[0]
        assert isinstance(name, ast.Constant)
        found.add((table.value, name.value))
    return found


def test_downgrade_restores_the_columns_from_the_sidecars_and_does_not_invent_a_token():
    path = _children()[0]
    downgrade = _function(path, "downgrade")
    assert _added_columns(downgrade) == DROPPED
    sql = "\n".join(
        node.value
        for node in ast.walk(downgrade)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and "UPDATE" in node.value
    )
    assert "user_pii" in sql
    assert "invitation_pii" in sql
    assert "token" not in sql
