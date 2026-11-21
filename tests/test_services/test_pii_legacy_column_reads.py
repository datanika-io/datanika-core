"""The legacy PII columns are not read or written.

core#939 release N+2 dropped ``users.email``, ``users.full_name``,
``users.oauth_provider_id``, ``invitations.email`` and ``invitations.token``.
This guard fails if product code starts referring to those attributes again.
The settings page's ``inv`` is an ``InvitationItem``, filled from the sidecar,
not a row.
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
