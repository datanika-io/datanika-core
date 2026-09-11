"""A method that ACCEPTS an actor must USE it (core#681).

The failure this exists to prevent, committed by me while wiring the last three subsystems:
`create_api_key`, `revoke_api_key` and `export_backup` were given `actor_user_id` **and a
docstring stating they require `admin`** — and no check. The contract said the control existed;
nothing enforced it. Every caller was updated, every signature read correctly, and the guard was
decorative.

That is this issue's own defect arriving inside the fix for it: *two layers hardened above an
unguarded one reads, from every instrument we have, as three.*

⚠️ What this guard is a guard OF, stated because my first version got it wrong
------------------------------------------------------------------------------
The first draft asserted *"calls `assert_org_role`"* and reported **8 offenders**, all false:

* `import_backup` and `_execute_validated_import` **delegate** — they thread the actor into
  services that check, which is correct and must stay allowed;
* six `UserService` methods enforce through **`_assert_may_manage`**, the mechanism
  `SPEC_ORG_ROLES` §4 already shipped for membership.

So the number was a count of *"does not call one particular function"* while the sentence
claimed *"does not enforce"*. Those are different sets, and the gap between them is where a
guard starts lying. **Ask what the number is a number of.**

The honest predicate is: **enforces directly, or hands the actor to something that does.**
"""

from __future__ import annotations

import ast
import pathlib

import pytest

SERVICES = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "services"

#: Functions that constitute enforcement in this codebase.
ENFORCERS = {"assert_org_role", "_assert_may_manage", "_actor_membership"}

#: The enforcers THEMSELVES take an actor, because being handed one is what they are for.
#: Exempt by name rather than by heuristic.
#:
#: ⚠️ Third refinement of this predicate, and the same lesson each time: the scan's
#: population included the thing it was scanning for. Draft 1 missed delegation, draft 2
#: missed the legacy enforcer, draft 3 flagged the enforcers. Each version reported real
#: numbers about a set that was not the one the sentence named.
PRIMITIVES = ENFORCERS


def _uses_actor(node: ast.AST) -> bool:
    """Enforces directly, or delegates by passing `actor_user_id=` onward."""
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        name = getattr(call.func, "id", None) or getattr(call.func, "attr", None)
        if name in ENFORCERS:
            return True
        if any(k.arg == "actor_user_id" for k in call.keywords):
            return True
    return False


def _actor_taking_methods():
    for path in sorted(SERVICES.glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            takes = any(a.arg == "actor_user_id" for a in node.args.kwonlyargs) or any(
                a.arg == "actor_user_id" for a in node.args.args
            )
            if takes and node.name not in PRIMITIVES:
                yield path, node


def test_the_scan_finds_the_wired_services():
    """Floor (§25). An empty scan makes the assertion below vacuous, and this file's whole
    subject is a guard that passes while checking nothing."""
    found = {p.name for p, _ in _actor_taking_methods()}
    assert len(found) >= 6, f"only {sorted(found)} — the scan is not seeing the wired services"


def test_every_method_that_accepts_an_actor_uses_it():
    offenders = [
        f"{p.name}:{n.lineno} {n.name}" for p, n in _actor_taking_methods() if not _uses_actor(n)
    ]
    assert not offenders, (
        "these take `actor_user_id` and neither enforce with it nor pass it on, so their "
        "signature and docstring claim a control that does not exist:\n  " + "\n  ".join(offenders)
    )


class TestTheGuardCanActuallyFail:
    """§43 — proves the predicate discriminates, rather than returning True for everything."""

    ACCEPTS_AND_IGNORES = "def f(session, org_id, *, actor_user_id: int):\n    return org_id\n"
    ENFORCES = (
        "def f(session, org_id, *, actor_user_id: int):\n"
        "    assert_org_role(session, org_id, actor_user_id, required='admin', operation='f')\n"
    )
    DELEGATES = (
        "def f(session, org_id, *, actor_user_id: int):\n"
        "    return svc.create_thing(session, org_id, actor_user_id=actor_user_id)\n"
    )
    LEGACY_ENFORCER = (
        "def f(session, org_id, *, actor_user_id: int):\n"
        "    self._assert_may_manage(session, org_id, actor_user_id)\n"
    )

    @pytest.mark.parametrize(
        "src,expected",
        [
            (ACCEPTS_AND_IGNORES, False),
            (ENFORCES, True),
            (DELEGATES, True),
            (LEGACY_ENFORCER, True),
        ],
        ids=["ignores", "enforces", "delegates", "legacy-enforcer"],
    )
    def test_the_predicate_discriminates(self, src, expected):
        node = ast.parse(src).body[0]
        assert _uses_actor(node) is expected

    def test_delegation_is_allowed_on_purpose(self):
        """`import_backup` threads the actor into services that check; it enforces nothing
        itself and must not be required to. Requiring a direct check here would push the
        control *down* into a place that cannot see the org boundary."""
        assert _uses_actor(ast.parse(self.DELEGATES).body[0]) is True
