"""core#939 — unmap the five legacy PII columns.

``origin/master`` still maps them, and SELECT names every mapped column, so this
release does not drop them. The drop waits until that revision is what production
runs. ``audit_logs.ip_address`` stays.
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


def _call_names(upgrade: ast.FunctionDef) -> list[str]:
    names: list[str] = []
    for node in ast.walk(upgrade):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        names.append(func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", ""))
    return names


def test_the_contract_revision_drops_nothing():
    """origin/master still selects the five columns, so this revision cannot drop them.

    Already-applied revisions are outside this assertion. A drop there is history.
    """
    matches = sorted(VERSIONS.glob("f8b2c4d6e1a3*.py"))
    assert len(matches) == 1
    text = matches[0].read_text(encoding="utf-8")
    assert "op.drop_column(" not in text
    assert "op.drop_table(" not in text
    assert "op.drop_constraint(" not in text
    upgrade = _upgrade(matches[0])
    assert upgrade is not None
    names = _call_names(upgrade)
    assert "drop_column" not in names
    assert "drop_table" not in names
    assert "drop_constraint" not in names


def test_the_stored_recipient_key_is_stripped_and_downgrade_restores_it():
    matches = [
        path
        for path in sorted(VERSIONS.glob("*.py"))
        if "notification_channels" in path.read_text(encoding="utf-8")
        and (
            "- 'email'" in path.read_text(encoding="utf-8")
            or '- "email"' in path.read_text(encoding="utf-8")
        )
    ]
    assert len(matches) == 1
    path = matches[0]
    text = path.read_text(encoding="utf-8")
    upgrade = _upgrade(path)
    assert upgrade is not None
    names = _call_names(upgrade)
    assert "drop_column" not in names
    assert "drop_table" not in names
    assert "drop_constraint" not in names
    assert "audit_logs" not in text
    assert "chat_id" not in text
    body = _downgrade_text(path)
    assert body
    assert "one_way" not in body
    assert "notification_channel_pii" in body
    assert "email" in body


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
