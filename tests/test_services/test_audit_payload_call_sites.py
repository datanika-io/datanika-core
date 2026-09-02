"""D11 — no audit-payload call site may hand a PII key to ``log_action``.

``docs/specs/SPEC_PII_SEPARATION.md`` §7 criterion **1a**, and it is the half of D12.1 the
redactor cannot do. Both mechanisms exist because they reach different things:

* **The redactor** (``test_audit_pii_redaction.py``) is a runtime chokepoint. It catches
  every call site, including ones nobody has written yet — but it is **nominal**, so it
  cannot tell ``{"name": "My Postgres"}`` from ``{"name": "Anna's Org"}``.
* **This guard** is a source sweep. It cannot see a payload built dynamically — but it
  fails at *authoring* time, on the next call site somebody writes, before the value has
  ever reached a database.

Neither subsumes the other, which is the whole of D12.1.

Why the resolver, rather than only checking literals at the call site
---------------------------------------------------------------------
Two of the six offending call sites do not pass a literal. They pass a local built a few
lines earlier, and one of those is behind a conditional::

    old_values = {"email": member_info.email, "role": member_info.role} if member_info else {}
    ...
    self._audit(..., old_values=old_values)

A guard that only inspected literal arguments would be green on both, i.e. green on a
third of the defect it is named after. So a simple local ``Name`` is resolved back to its
nearest preceding assignment in the same function, through ``IfExp`` branches.

⚠️ **What it deliberately does NOT do is guess.** A payload argument it cannot resolve to
a literal — a function call, an attribute, a comprehension — is reported in the failure
message as *unanalysable* but does not by itself fail the test. A classifier that returns
"probably fine" for a shape it does not understand is producing a skip, and a skip is the
same colour as a pass; naming them out loud is the honest middle, and the runtime redactor
is what actually defends those cases. ``test_the_sweep_is_armed`` keeps that from becoming
a way for the whole guard to quietly stop finding anything.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from datanika.services.audit_service import PII_PAYLOAD_KEYS

SOURCE_ROOT = pathlib.Path(__file__).resolve().parents[2] / "datanika"

#: The keyword arguments that become ``audit_logs.old_values`` / ``new_values``.
PAYLOAD_KWARGS = ("old_values", "new_values")

#: Functions whose payload arguments are the ones that reach the column. ``_audit`` is
#: ``BaseState``'s helper (the ~30 UI call sites) and ``log_action`` is the service method
#: it forwards to, which two callers in ``auth_state`` reach directly.
PAYLOAD_CALLEES = ("_audit", "log_action")


def _python_files() -> list[pathlib.Path]:
    return sorted(p for p in SOURCE_ROOT.rglob("*.py") if "__pycache__" not in p.parts)


def _callee_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _dict_keys(node: ast.AST) -> set[str] | None:
    """String keys of a dict literal, following ``IfExp`` branches. ``None`` if not one."""
    if isinstance(node, ast.Dict):
        return {
            k.value for k in node.keys if isinstance(k, ast.Constant) and isinstance(k.value, str)
        }
    if isinstance(node, ast.IfExp):
        body = _dict_keys(node.body)
        orelse = _dict_keys(node.orelse)
        if body is None and orelse is None:
            return None
        return (body or set()) | (orelse or set())
    return None


def _resolve(name: str, scope: ast.AST, before_lineno: int) -> set[str] | None:
    """Keys of the dict most recently assigned to ``name`` in ``scope`` above ``before_lineno``."""
    best: tuple[int, set[str]] | None = None
    for node in ast.walk(scope):
        if not isinstance(node, ast.Assign):
            continue
        if node.lineno >= before_lineno:
            continue
        if not any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            continue
        keys = _dict_keys(node.value)
        if keys is None:
            continue
        if best is None or node.lineno > best[0]:
            best = (node.lineno, keys)
    return None if best is None else best[1]


def _enclosing_scopes(tree: ast.AST) -> list[ast.AST]:
    kinds = ast.FunctionDef | ast.AsyncFunctionDef | ast.Module
    return [n for n in ast.walk(tree) if isinstance(n, kinds)]


def _scan() -> tuple[list[str], list[str], int]:
    """``(offending, unanalysable, call_sites_seen)`` across ``datanika/``."""
    offending: list[str] = []
    unanalysable: list[str] = []
    seen = 0

    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        scopes = _enclosing_scopes(tree)
        rel = path.relative_to(SOURCE_ROOT.parent).as_posix()

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or _callee_name(node) not in PAYLOAD_CALLEES:
                continue
            for kw in node.keywords:
                if kw.arg not in PAYLOAD_KWARGS:
                    continue
                seen += 1
                keys = _dict_keys(kw.value)
                if keys is None and isinstance(kw.value, ast.Name):
                    # Narrowest enclosing scope that physically contains the call.
                    holder = min(
                        (
                            s
                            for s in scopes
                            if isinstance(s, ast.FunctionDef | ast.AsyncFunctionDef)
                            and s.lineno <= node.lineno <= (s.end_lineno or node.lineno)
                        ),
                        key=lambda s: (s.end_lineno or 0) - s.lineno,
                        default=None,
                    )
                    if holder is not None:
                        keys = _resolve(kw.value.id, holder, node.lineno)
                if keys is None:
                    unanalysable.append(f"{rel}:{node.lineno} {kw.arg}={ast.unparse(kw.value)}")
                    continue
                hits = sorted(keys & PII_PAYLOAD_KEYS)
                if hits:
                    offending.append(f"{rel}:{node.lineno} {kw.arg} carries {hits}")
    return offending, unanalysable, seen


def test_the_sweep_is_armed() -> None:
    """Guard the guard.

    Every assertion below is vacuously true if the walker finds no call sites — a wrong
    ``SOURCE_ROOT``, a renamed helper, a parse failure. Pin both ends: there really are
    payload call sites, and the key set really is populated (an empty ``PII_PAYLOAD_KEYS``
    makes every intersection empty and this whole file green forever).
    """
    _, _, seen = _scan()
    assert seen >= 30, (
        f"only {seen} audit payload arguments found — is the walker reaching datanika/?"
    )
    assert PII_PAYLOAD_KEYS, "PII_PAYLOAD_KEYS is empty; every check here would pass vacuously"
    assert "email" in PII_PAYLOAD_KEYS


def test_no_call_site_writes_a_pii_key_into_an_audit_payload() -> None:
    """D11. Red first against the six sites in ``settings_state.py``.

    Five are named in §2a; the sixth — ``leave_org``, whose payload is nothing but an
    address — is in neither §2a's table nor D11's list, and this guard is how it was
    found. That is the argument for deriving the check instead of enumerating the sites,
    made by the check against the document that specified it.
    """
    offending, unanalysable, _ = _scan()
    detail = "\n  ".join(offending)
    note = (
        ""
        if not unanalysable
        else "\n\nNot analysable from source (the runtime redactor covers these):\n  "
        + "\n  ".join(unanalysable)
    )
    assert not offending, (
        "audit payloads must carry internal IDs, not personal data (D11). The address is "
        "then resolvable through user_pii / invitation_pii while those rows exist, and "
        "stops resolving once they are erased — so erasure needs to sweep nothing.\n  "
        + detail
        + note
    )


@pytest.mark.parametrize("key", sorted(PII_PAYLOAD_KEYS))
def test_the_guard_can_see_each_pii_key(key: str, tmp_path: pathlib.Path) -> None:
    """Every key in the set is one this walker would actually catch.

    Without this, a key could sit in ``PII_PAYLOAD_KEYS`` while the detection path for it
    was broken, and the suite would report clean. Built as a synthetic module rather than
    by mutating a real one, because the mutation harnesses in this repo have twice been
    the thing that was broken.
    """
    src = tmp_path / "probe.py"
    src.write_text(
        f'def handler(self):\n    payload = {{"{key}": "x"}}\n'
        f"    self._audit(s, 1, 2, 'update', 'thing', old_values=payload)\n",
        encoding="utf-8",
    )
    tree = ast.parse(src.read_text(encoding="utf-8"))
    scopes = _enclosing_scopes(tree)
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _callee_name(node) in PAYLOAD_CALLEES:
            for kw in node.keywords:
                if kw.arg in PAYLOAD_KWARGS and isinstance(kw.value, ast.Name):
                    holder = next(s for s in scopes if isinstance(s, ast.FunctionDef))
                    found.append(_resolve(kw.value.id, holder, node.lineno))
    assert found and found[0] == {key}, (
        f"the resolver cannot see a payload carrying {key!r} through a local variable, "
        "so the main assertion in this file is blind to it"
    )
