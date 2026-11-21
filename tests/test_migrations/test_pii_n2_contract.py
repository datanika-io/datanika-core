"""core#939 release N+2 — the contract that drops the five legacy PII columns.

The release in production stopped reading those columns. This file is red until
the drop, the model unmap, the erasure block, and the load-test lookup all land
together. ``audit_logs.ip_address`` stays.
"""

from __future__ import annotations

import ast
from pathlib import Path

from datanika.models.invitation import Invitation
from datanika.models.user import User

ROOT = Path(__file__).resolve().parents[2]
VERSIONS = ROOT / "datanika" / "migrations" / "versions"
SEED = ROOT / "scripts" / "loadtest" / "seed_loadtest_org.py"
USER_SERVICE = ROOT / "datanika" / "services" / "user_service.py"

_DROPS = (
    ("users", "email"),
    ("users", "full_name"),
    ("users", "oauth_provider_id"),
    ("invitations", "email"),
    ("invitations", "token"),
)


def _upgrade(path: Path) -> ast.FunctionDef | None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "upgrade"
        ),
        None,
    )


def _downgrade_text(path: Path) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next(
        (
            item
            for item in tree.body
            if isinstance(item, ast.FunctionDef) and item.name == "downgrade"
        ),
        None,
    )
    if node is None:
        return ""
    return ast.get_source_segment(path.read_text(encoding="utf-8"), node) or ""


def _drop_pairs(upgrade: ast.FunctionDef) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for node in ast.walk(upgrade):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name != "drop_column":
            continue
        args = [
            arg.value
            for arg in node.args
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
        ]
        if len(args) >= 2:
            found.append((args[0], args[1]))
    return found


def _call_names(upgrade: ast.FunctionDef) -> list[str]:
    names: list[str] = []
    for node in ast.walk(upgrade):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        names.append(func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", ""))
    return names


def _contract_migration() -> Path | None:
    matches: list[Path] = []
    for path in sorted(VERSIONS.glob("*.py")):
        upgrade = _upgrade(path)
        if upgrade is None:
            continue
        if set(_drop_pairs(upgrade)) == set(_DROPS):
            matches.append(path)
    if len(matches) != 1:
        return None
    return matches[0]


def test_n2_drops_exactly_the_five_legacy_columns():
    path = _contract_migration()
    assert path is not None, "no migration drops exactly the five legacy PII columns"
    upgrade = _upgrade(path)
    assert upgrade is not None
    assert _call_names(upgrade).count("drop_table") == 0
    text = path.read_text(encoding="utf-8")
    assert "expand-contract:" in text
    assert "audit_logs" not in text
    # The recipient key the spec names on an email channel. Not chat_id.
    assert "- 'email'" in text or '- "email"' in text
    assert "chat_id" not in text
    assert "notification_channels" in text


def test_n2_downgrade_rederives_the_columns_from_the_sidecars():
    path = _contract_migration()
    assert path is not None, "no contract migration to downgrade"
    body = _downgrade_text(path)
    assert "one_way" not in body
    assert "user_pii" in body
    assert "invitation_pii" in body
    assert "FROM user_pii" in body or "from user_pii" in body
    assert "FROM invitation_pii" in body or "from invitation_pii" in body
    for column in ("email", "full_name", "oauth_provider_id", "token"):
        assert column in body


def test_the_models_no_longer_map_the_legacy_columns():
    user_cols = set(User.__table__.columns.keys())
    invitation_cols = set(Invitation.__table__.columns.keys())
    assert "email" not in user_cols
    assert "full_name" not in user_cols
    assert "oauth_provider_id" not in user_cols
    assert "email_verified" in user_cols
    assert "email" not in invitation_cols
    assert "token" not in invitation_cols
    assert "token_hash" in invitation_cols


def test_erase_user_step_1a_is_gone():
    text = USER_SERVICE.read_text(encoding="utf-8")
    assert "step 1a" not in text
    assert "DELETE THIS BLOCK" not in text
    tree = ast.parse(text)
    erase = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "erase_user"
    )
    for node in ast.walk(erase):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Attribute) and target.attr in {
                "email",
                "full_name",
                "oauth_provider_id",
                "token",
            }:
                raise AssertionError(f"erase_user still assigns .{target.attr}")


def test_the_loadtest_lookup_uses_the_sidecar():
    text = SEED.read_text(encoding="utf-8")
    assert "User.email" not in text
    assert "UserPII" in text
    assert "UserPII.email" in text
