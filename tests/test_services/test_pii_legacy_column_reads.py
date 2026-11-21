"""The legacy PII columns are not read or written, except the erasure nulls.

core#939. Release N+1 stopped the product from treating these columns as the
address, and that release is already in production. A later release drops the
columns. This guard is what makes that drop a migration: the code deployed
before it must not select, filter, or insert them.

``erase_user`` step 1a still assigns NULL. The columns still exist, and an
erasure that skips them leaves the original. That block is deleted in the
release that drops the columns, not in this one.

Measured on ``af2ab047`` before this change: 22 executable reads and writes
outside that block and outside the mapped column definitions. Two further
``inv.email`` hits are the settings page rendering ``InvitationItem``, which
is filled from the column at ``settings_state.py``. Those are not a second
read of ``invitations.email``.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCAN_ROOTS = (ROOT / "datanika", ROOT / "tests" / "factories.py")

USER_ATTRS = frozenset({"email", "full_name", "oauth_provider_id"})
INVITATION_ATTRS = frozenset({"email", "token"})
USER_NAMES = frozenset({"User", "user", "viewer_user", "org_b_user"})
INVITATION_NAMES = frozenset({"Invitation", "invitation", "inv", "existing_inv"})

# Step 1a. A store of None on these names, inside erase_user only.
STEP_1A = frozenset({"email", "full_name", "oauth_provider_id", "token"})


def _py_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        if root.is_file():
            files.append(root)
            continue
        for path in root.rglob("*.py"):
            posix = path.relative_to(ROOT).as_posix()
            if posix.startswith("datanika/migrations/"):
                continue
            files.append(path)
    return files


def _enclosing_function(tree: ast.AST, node: ast.AST) -> str | None:
    for parent in ast.walk(tree):
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in ast.walk(parent):
                if child is node:
                    return parent.name
    return None


def _is_step_1a_store(tree: ast.AST, node: ast.Attribute) -> bool:
    if not isinstance(node.ctx, ast.Store):
        return False
    if node.attr not in STEP_1A:
        return False
    if _enclosing_function(tree, node) != "erase_user":
        return False
    # The assignment's value is None. Walk assigns whose target is this node.
    for parent in ast.walk(tree):
        if isinstance(parent, ast.Assign) and node in parent.targets:
            return isinstance(parent.value, ast.Constant) and parent.value.value is None
        if isinstance(parent, ast.AnnAssign) and parent.target is node:
            return isinstance(parent.value, ast.Constant) and parent.value.value is None
    return False


def _receiver_name(node: ast.Attribute) -> str | None:
    value = node.value
    if isinstance(value, ast.Name):
        return value.id
    return None


def legacy_touches() -> list[str]:
    """Executable reads and writes of the five legacy columns."""
    found: list[str] = []
    for path in _py_files():
        tree = ast.parse(path.read_bytes().decode("utf-8"), filename=str(path))
        rel = path.relative_to(ROOT).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id == "User":
                    banned = USER_ATTRS
                elif node.func.id == "Invitation":
                    banned = INVITATION_ATTRS
                else:
                    banned = frozenset()
                for keyword in node.keywords:
                    if keyword.arg in banned:
                        found.append(f"{rel}:{node.lineno}: {node.func.id}({keyword.arg}=)")
            if not isinstance(node, ast.Attribute):
                continue
            if _is_step_1a_store(tree, node):
                continue
            receiver = _receiver_name(node)
            # The settings page's ``inv`` is an ``InvitationItem``, not a row.
            # The column read is the state line that copies ``inv.email`` into it.
            if rel.startswith("datanika/ui/pages/") and receiver == "inv":
                continue
            user_touch = receiver in USER_NAMES and node.attr in USER_ATTRS
            invitation_touch = receiver in INVITATION_NAMES and node.attr in INVITATION_ATTRS
            if user_touch or invitation_touch:
                found.append(f"{rel}:{node.lineno}: {receiver}.{node.attr}")
    return sorted(found)


def test_legacy_pii_columns_have_no_remaining_read_or_write():
    touches = legacy_touches()
    assert touches == [], (
        f"{len(touches)} remaining reads/writes of users.email, users.full_name, "
        f"users.oauth_provider_id, invitations.email, invitations.token:\n" + "\n".join(touches)
    )


def test_the_count_instrument_sees_a_constructor_write():
    """A guard that cannot see ``User(email=)`` would report zero on the unfixed tree."""
    sample = ast.parse(
        'def f():\n    User(email="a@b.c", full_name="A")\n    Invitation(token="t")\n'
    )
    writes = [
        keyword.arg
        for node in ast.walk(sample)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        for keyword in node.keywords
        if node.func.id in {"User", "Invitation"} and keyword.arg
    ]
    assert writes == ["email", "full_name", "token"]
