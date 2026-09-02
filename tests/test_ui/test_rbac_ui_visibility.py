"""RBAC UI-visibility: a control is rendered only to roles that may invoke it.

Companion to ``test_rbac_enforcement.py`` (which locks the *handler*
``_check_role`` gates). This module locks the *visibility* gates: a viewer must
not SEE the create / edit / delete controls whose handlers they cannot invoke —
"a viewer that gets to a delete button by accident is a regression".

Uses AST source-inspection (no Reflex state instantiation, matching the repo's
RBAC test convention) so the invariant survives refactors of the page markup.

Regression guard for core#313, core#658, core#886.

core#886 — what changed, and why it is the point of the issue
-------------------------------------------------------------
This module used to walk a hand-written ``RESOURCE_PAGES`` list of five pages,
matching handlers against a hand-written ``GATE_FOR_PREFIX`` map of five verb
prefixes (``delete_``, ``edit_``, ``copy_``, ``run_``, ``toggle_``).

Both halves were claims about a codebase that moves, and both were wrong:

* ``GATE_FOR_PREFIX`` covered neither ``remove_`` nor ``revoke_``, so
  ``remove_member``, ``remove_dependency`` and ``revoke_api_key`` were invisible
  to the checker **even on a page that was in the list**.
* ``RESOURCE_PAGES`` omitted ``dag``, ``api_keys`` and ``settings`` entirely.
* And the prefix idea fails in a way no widening fixes:
  ``SettingsState.cancel_invitation`` is a one-click admin-gated database
  mutation spelled with a verb no destructive-verb list contains. It survived
  core#658's sweep of this page **and** core#851's sweep of the whole product,
  because both derived their lists from verbs. It sat between
  ``add_member_by_email`` (gated) and ``remove_member`` (gated), on the same
  card.

**So nothing here is enumerated by name or by spelling any more.** The
requirement is derived from what the handler itself declares:

    if a handler calls ``self._check_role("<role>")``, then every control that
    invokes it must render under a gate whose own threshold is at least
    ``<role>``

which is exactly the invariant the issue is about, stated once, with the
codebase as its own source of truth. When someone adds a page, a component, or a
handler, the guard covers it on the next run without anybody remembering to add
it to a list.

Three properties this needs in order not to be decorative:

1. **The scan covers ``datanika/ui/``, not ``datanika/ui/pages/``.** A control
   moved into a component (core#851 does this for the API-key row, rendered on
   two surfaces) must not move out of view. The old ``_GateChecker`` scanned one
   module at a time and would have gone green on ``api_keys.py`` while the
   ungated Revoke button sat in ``components/api_key_row.py``.
2. **Gates resolve through call sites.** A helper whose buttons carry no gate of
   their own is gated if *every* call site of that helper is inside a gate — an
   intersection, so one ungated call site is enough to fail. That is how
   ``save_connection`` (gated at the form's call site, never inline) is
   correctly accepted without the special-cased ``test_create_form_is_gated``
   this module used to need, and how ``remove_member`` is accepted through
   ``rx.cond(member.can_manage, _remove_member_dialog(member))``.
3. **Every gate earns its threshold** (:class:`TestGatesAreRoleDerived`). A
   registry of gate names would just be the old hand-written list under a new
   name, so each entry is checked against the predicate that computes it —
   two by reading the ``check_role_hierarchy`` constant out of the computed var,
   two by *executing* the service predicate across the whole role table. A gate
   that stopped being role-derived would fail there rather than silently
   licensing every control it wraps.
"""

import ast
import inspect
from pathlib import Path

import pytest

from datanika.services.auth import ROLE_RANK, assignable_roles, may_manage_member

# ── the visibility vars on AuthState, and the role each must require ─────────
VIS_VARS = {"can_edit": "editor", "can_delete": "admin", "can_administer": "admin"}

#: Every gate name that licenses a control, and the minimum membership role it
#: is true for. Each entry is proven against its own predicate by
#: :class:`TestGatesAreRoleDerived` — this is not a list of exemptions.
ROLE_GATES: dict[str, str] = {
    # AuthState computed vars — proven by reading check_role_hierarchy's argument
    "can_edit": "editor",
    "can_delete": "admin",
    "can_administer": "admin",
    # SettingsState — proven by executing the predicates that compute them
    "can_manage_members": "admin",  # bool(assignable_roles(role))
    "is_owner": "owner",  # role == "owner"
    # MemberItem, per row — proven by executing may_manage_member over ROLE_RANK
    "can_manage": "admin",
}


# --------------------------------------------------------------------------- #
# AST helpers
# --------------------------------------------------------------------------- #


def _module_source(dotted: str) -> str:
    mod = __import__(dotted, fromlist=[dotted.rsplit(".", 1)[-1]])
    with open(inspect.getfile(mod), encoding="utf-8") as f:
        return f.read()


def _ui_root() -> Path:
    import datanika.ui

    return Path(inspect.getfile(datanika.ui)).parent


def _ui_modules() -> list:
    """Every UI source file — pages *and* components — excluding state."""
    root = _ui_root()
    state = root / "state"
    return sorted(
        p
        for p in root.rglob("*.py")
        if "__pycache__" not in p.parts and not p.is_relative_to(state)
    )


def _state_modules() -> list:
    root = _ui_root() / "state"
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def _attr_names(node: ast.AST) -> set[str]:
    """Every attribute name referenced anywhere under ``node``.

    ``AuthState.can_edit`` -> ``{"can_edit"}``; a compound
    ``AuthState.can_administer & Other.flag`` -> ``{"can_administer", "flag"}``.
    """
    return {n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)}


def _is_rx_call(node: ast.AST, name: str) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == name
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "rx"
    )


def _dec_name(dec: ast.AST) -> str:
    if isinstance(dec, ast.Call):
        dec = dec.func
    if isinstance(dec, ast.Attribute):
        return dec.attr
    if isinstance(dec, ast.Name):
        return dec.id
    return ""


# --------------------------------------------------------------------------- #
# Pass 1 — what role does each handler declare?
# --------------------------------------------------------------------------- #


def declared_handler_roles() -> dict[str, dict[str, str]]:
    """``{handler_name: {StateClassName: role}}`` from ``self._check_role("x")``.

    The *handler* is the source of truth for what a control requires. Nothing
    here reads a name, a prefix or a docstring.
    """
    out: dict[str, dict[str, str]] = {}
    for path in _state_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for cls in ast.walk(tree):
            if not isinstance(cls, ast.ClassDef):
                continue
            for fn in cls.body:
                if not isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                for call in ast.walk(fn):
                    if (
                        isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "_check_role"
                        and call.args
                        and isinstance(call.args[0], ast.Constant)
                        and isinstance(call.args[0].value, str)
                    ):
                        out.setdefault(fn.name, {})[cls.name] = call.args[0].value
    return out


# --------------------------------------------------------------------------- #
# Pass 2 — every control, and the gates lexically enclosing it
# --------------------------------------------------------------------------- #

#: Reflex event-handler keyword arguments.
#:
#: 🔑 A census fails in **two** independent places: the **predicate** (which
#: names count) and the **matcher** (which syntax is reachable). A predicate fix
#: reads like a complete fix, which is why this list exists alongside the
#: handler-derived requirement above.
#:
#: ⚠️ ``on_click`` is not the only way a handler reaches a control. Two
#: admin-gated controls hang off other events — ``rx.select(on_change=…)`` drives
#: ``change_member_role``, ``rx.upload(on_drop=…)`` drives
#: ``handle_restore_upload``. Both are correctly gated today, so neither is a
#: bug; but a matcher that could not see them would stay green if either gate
#: were removed, which is precisely the failure this module exists to prevent.
#:
#: core#851's confirmation guard has the same defect facing the other way: it
#: matches ``ast.Call`` only, so ``on_click=SettingsState.leave_org`` — a
#: handler taking no arguments, hence a bare ``ast.Attribute`` — is unreachable
#: to it however its verb list is edited.
HANDLER_KWARGS = (
    "on_click",
    "on_change",
    "on_drop",
    "on_submit",
    "on_blur",
    "on_key_down",
)


def _handler_refs(value: ast.AST) -> list[tuple[str | None, str]]:
    """Every ``<Name>.<attr>`` reference under an event-handler argument.

    Three shapes occur in this codebase and all three must be reached:

    * ``State.handler(row.id)`` — an ``ast.Call`` over an ``ast.Attribute``
    * ``State.handler`` — a bare ``ast.Attribute``; a handler taking no
      arguments is referenced without parentheses
    * ``lambda v: State.handler(row.id, v)`` — inside an ``ast.Lambda``

    Deliberately generous: it returns every attribute reference it finds, and
    :func:`ungated_controls` keeps only those naming a role-declaring handler.
    Over-collection costs nothing here; under-collection is the bug.
    """
    out: list[tuple[str | None, str]] = []
    for sub in ast.walk(value):
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
            owner = sub.func.value
            out.append((owner.id if isinstance(owner, ast.Name) else None, sub.func.attr))
        elif isinstance(sub, ast.Attribute):
            owner = sub.value
            out.append((owner.id if isinstance(owner, ast.Name) else None, sub.attr))
    return out


class _ModuleWalker(ast.NodeVisitor):
    """Collect, per enclosing top-level function, the controls it renders and the
    helpers it calls, each with the ``rx.cond`` gates active at that point."""

    def __init__(self, module: str) -> None:
        self.module = module
        self.fn: str | None = None
        self.stack: list[set[str]] = []
        #: (module, enclosing_fn, StateName, handler, local_gates, lineno)
        self.controls: list[tuple] = []
        #: (callee_name, module, caller_fn, gates_at_call_site)
        self.calls: list[tuple] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        prev_fn, prev_stack = self.fn, self.stack
        self.fn, self.stack = node.name, []
        self.generic_visit(node)
        self.fn, self.stack = prev_fn, prev_stack

    def visit_Call(self, node: ast.Call) -> None:
        active: set[str] = set().union(*self.stack) if self.stack else set()

        # Any call carrying an event-handler kwarg, not only ``rx.button``.
        seen: set[tuple[str | None, str]] = set()
        for kw in node.keywords:
            if kw.arg not in HANDLER_KWARGS:
                continue
            for ref in _handler_refs(kw.value):
                if ref in seen:
                    continue
                seen.add(ref)
                self.controls.append((self.module, self.fn, ref[0], ref[1], active, node.lineno))

        # A helper invoked directly: ``_remove_member_dialog(member)``.
        if isinstance(node.func, ast.Name):
            self.calls.append((node.func.id, self.module, self.fn, active))
        # A helper passed as a callback: ``rx.foreach(State.rows, member_row)``.
        for arg in node.args:
            if isinstance(arg, ast.Name):
                self.calls.append((arg.id, self.module, self.fn, active))

        if _is_rx_call(node, "cond") and node.args:
            self.stack.append(_attr_names(node.args[0]))
            self.generic_visit(node)
            self.stack.pop()
        else:
            self.generic_visit(node)


def _walk_ui() -> list[_ModuleWalker]:
    walkers = []
    for path in _ui_modules():
        walker = _ModuleWalker(path.name)
        walker.visit(ast.parse(path.read_text(encoding="utf-8")))
        walkers.append(walker)
    return walkers


def _effective_gates(walkers: list[_ModuleWalker]):
    """Return ``fn(module, function) -> gates guaranteed active in its body``.

    A helper is gated only if **every** call site of it is gated — an
    intersection, so one ungated caller is enough. A function with no call site
    is a page entry point and is ungated.
    """
    call_sites: dict[str, list[tuple]] = {}
    for walker in walkers:
        for callee, module, caller, gates in walker.calls:
            if caller is not None:
                call_sites.setdefault(callee, []).append((module, caller, gates))

    memo: dict[tuple, set[str]] = {}

    def resolve(module: str, fn: str | None, seen: frozenset = frozenset()) -> set[str]:
        if fn is None:
            return set()
        key = (module, fn)
        if key in memo:
            return memo[key]
        if key in seen:  # recursive component; claim nothing
            return set()
        sites = call_sites.get(fn, [])
        if not sites:
            result: set[str] = set()
        else:
            result = set.intersection(
                *(gates | resolve(m, c, seen | {key}) for m, c, gates in sites)
            )
        memo[key] = result
        return result

    return resolve


def ungated_controls() -> list[tuple]:
    """Every control whose gate is absent or weaker than its handler requires.

    Returns ``(module, lineno, State, handler, required_role, gates_seen)``.
    """
    roles = declared_handler_roles()
    walkers = _walk_ui()
    resolve = _effective_gates(walkers)

    violations = []
    for walker in walkers:
        for module, fn, state, handler, local, lineno in walker.controls:
            owners = roles.get(handler)
            if not owners:
                continue  # handler declares no role: nothing to enforce here
            required = owners.get(state) or next(iter(owners.values()))
            gates = local | resolve(module, fn)
            strongest = max(
                (ROLE_RANK[ROLE_GATES[g]] for g in gates if g in ROLE_GATES), default=-1
            )
            if strongest < ROLE_RANK[required]:
                violations.append((module, lineno, state, handler, required, sorted(gates)))
    return violations


def _computed_var_role(source: str, class_name: str, var_name: str) -> str | None:
    """Return the role constant passed to ``check_role_hierarchy`` inside the
    ``@rx.var`` computed var ``class_name.var_name``, or ``None`` if it is not a
    computed var wired to ``check_role_hierarchy``."""
    tree = ast.parse(source)
    for cls in ast.walk(tree):
        if not (isinstance(cls, ast.ClassDef) and cls.name == class_name):
            continue
        for fn in cls.body:
            if not (isinstance(fn, ast.FunctionDef) and fn.name == var_name):
                continue
            if "var" not in {_dec_name(d) for d in fn.decorator_list}:
                return None
            for call in ast.walk(fn):
                if (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Name)
                    and call.func.id == "check_role_hierarchy"
                ):
                    for arg in call.args:
                        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                            return arg.value
    return None


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


class TestVisibilityVars:
    """AuthState exposes the visibility gates with the same thresholds the
    handlers enforce (editor for create/edit, admin for delete and for the
    org-level facilities)."""

    def test_authstate_defines_visibility_vars(self):
        src = _module_source("datanika.ui.state.auth_state")
        for var, role in VIS_VARS.items():
            assert _computed_var_role(src, "AuthState", var) == role, (
                f"AuthState.{var} must be an @rx.var calling "
                f"check_role_hierarchy(self.current_role, '{role}')"
            )

    def test_can_administer_is_not_an_alias_of_can_delete(self):
        """Both are admin-threshold, deliberately, and both must stay declared.

        core#886 gates *create* controls whose handlers require admin
        (API keys, notification channels) — ``can_edit`` is too weak for those
        and ``can_delete`` reads as the wrong question. Collapsing the two would
        make the next change to either one invisible.
        """
        src = _module_source("datanika.ui.state.auth_state")
        assert _computed_var_role(src, "AuthState", "can_administer") == "admin"
        assert _computed_var_role(src, "AuthState", "can_delete") == "admin"


class TestGatesAreRoleDerived:
    """Every name in ``ROLE_GATES`` earns its threshold.

    Without this the registry is the old hand-written exemption list wearing a
    new name: anybody could license an ungated control by inventing a gate.
    """

    @pytest.mark.parametrize("var", ["can_edit", "can_delete", "can_administer"])
    def test_authstate_gate_threshold_matches_registry(self, var):
        src = _module_source("datanika.ui.state.auth_state")
        assert _computed_var_role(src, "AuthState", var) == ROLE_GATES[var]

    def test_can_manage_members_is_true_exactly_for_admin_and_above(self):
        """``can_manage_members = bool(assignable_roles(current_role))``.

        Executed against the real predicate rather than asserted, so a change to
        ``may_grant_role`` that let editors invite people would fail here — and
        the invitation Cancel button this gate now covers would be re-examined
        rather than silently re-licensed.
        """
        for role, rank in ROLE_RANK.items():
            gate_true = bool(assignable_roles(role))
            assert gate_true == (rank >= ROLE_RANK[ROLE_GATES["can_manage_members"]]), (
                f"assignable_roles({role!r}) -> {gate_true}; registry claims this "
                f"gate means >= {ROLE_GATES['can_manage_members']}"
            )

    def test_can_manage_is_never_true_below_admin(self):
        """``MemberItem.can_manage = may_manage_member(actor, target)``.

        The whole role table, both axes: no actor below the registered threshold
        may manage *any* member. (The converse does not hold — an admin cannot
        manage an owner — which is why this is one-directional.)
        """
        floor = ROLE_RANK[ROLE_GATES["can_manage"]]
        for actor, actor_rank in ROLE_RANK.items():
            if actor_rank >= floor:
                continue
            for target in ROLE_RANK:
                assert not may_manage_member(actor, target), (
                    f"may_manage_member({actor!r}, {target!r}) is True, but the "
                    f"registry claims can_manage means >= {ROLE_GATES['can_manage']}"
                )

    def test_is_owner_is_the_owner_comparison(self):
        """``is_owner`` is assigned ``self.current_role == "owner"``."""
        src = _module_source("datanika.ui.state.settings_state")
        found = [
            node
            for node in ast.walk(ast.parse(src))
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Attribute) and t.attr == "is_owner" for t in node.targets)
        ]
        assert found, "SettingsState.is_owner is never assigned"
        for node in found:
            literals = {c.value for c in ast.walk(node.value) if isinstance(c, ast.Constant)}
            assert ROLE_GATES["is_owner"] in literals, (
                f"SettingsState.is_owner is assigned from {ast.unparse(node.value)!r}, "
                f"which does not compare against {ROLE_GATES['is_owner']!r}"
            )


class TestDestructiveControlsGated:
    """Derived from what handlers declare — no page list, no verb list."""

    def test_every_role_declaring_control_is_gated(self):
        violations = ungated_controls()
        assert not violations, (
            "controls rendered to roles whose handlers refuse them:\n"
            + "\n".join(
                f"  {m}:{ln}  {s}.{h} needs >= {need}, gates seen: {gates}"
                for m, ln, s, h, need, gates in violations
            )
        )

    def test_the_scan_actually_reaches_the_product(self):
        """A derivation that finds nothing passes vacuously.

        Both halves are asserted, because either one silently emptying is the
        failure this guard cannot otherwise see — an import rename in
        ``_ui_modules`` or a change to ``_check_role``'s call shape would leave
        ``test_every_role_declaring_control_is_gated`` green over an empty set.
        """
        roles = declared_handler_roles()
        assert len(roles) >= 15, f"only {len(roles)} role-declaring handlers found"

        controls = [c for w in _walk_ui() for c in w.controls if c[3] in roles]
        assert len(controls) >= 15, f"only {len(controls)} role-declaring controls found"

        modules = {w.module for w in _walk_ui() if w.controls}
        assert "settings.py" in modules and "api_key_row.py" in modules, (
            f"the scan must cover pages AND components; saw {sorted(modules)}"
        )

    def test_the_component_surface_is_covered(self):
        """core#851 moved Revoke into ``components/api_key_row.py``.

        The old per-module ``_GateChecker`` would have passed ``api_keys.py``
        while the ungated button sat one import away. Naming the file here keeps
        that specific blindness from returning.
        """
        handlers = {c[3] for w in _walk_ui() if w.module == "api_key_row.py" for c in w.controls}
        assert "revoke_api_key" in handlers

    def test_cancel_invitation_is_in_scope(self):
        """The site every verb-derived sweep missed (core#886).

        ``cancel_invitation`` is an admin-gated database mutation whose name
        contains no destructive verb. It is here as a named regression because
        it is the specific evidence that widening a prefix list was never the
        fix — and because a future refactor that reintroduced a name-based
        derivation would pass every other test in this module.
        """
        roles = declared_handler_roles()
        assert roles.get("cancel_invitation", {}).get("SettingsState") == "admin"
        sites = [c for w in _walk_ui() for c in w.controls if c[3] == "cancel_invitation"]
        assert sites, "cancel_invitation is rendered nowhere — did the control move?"


class TestCheckerSelfCheck:
    """Guard the derivation itself so a broken checker cannot hide regressions.

    Every case here is a *forced red* on a real mechanism, not a shape the
    checker was written around. ``test_every_role_declaring_control_is_gated``
    is green on the current tree, so these are the only evidence that it is
    able to fail at all.
    """

    @staticmethod
    def _walk(src: str) -> _ModuleWalker:
        walker = _ModuleWalker("synthetic.py")
        walker.visit(ast.parse(src))
        return walker

    def test_ungated_button_is_seen_with_no_gates(self):
        w = self._walk(
            "import reflex as rx\n"
            "def page():\n"
            "    return rx.button('x', on_click=S.delete_thing(i))\n"
        )
        assert w.controls[0][3] == "delete_thing"
        assert w.controls[0][4] == set()

    def test_gate_is_recorded_from_an_enclosing_cond(self):
        w = self._walk(
            "import reflex as rx\n"
            "def page():\n"
            "    return rx.cond(AuthState.can_delete,\n"
            "        rx.button('x', on_click=S.delete_thing(i)))\n"
        )
        assert w.controls[0][4] == {"can_delete"}

    def test_compound_conditions_contribute_every_name(self):
        w = self._walk(
            "import reflex as rx\n"
            "def page():\n"
            "    return rx.cond(AuthState.can_administer & S.show_form,\n"
            "        rx.button('x', on_click=S.save_channel))\n"
        )
        assert {"can_administer", "show_form"} <= w.controls[0][4]

    def test_gates_resolve_through_a_call_site(self):
        w = self._walk(
            "import reflex as rx\n"
            "def row():\n"
            "    return rx.button('x', on_click=S.delete_thing(i))\n"
            "def page():\n"
            "    return rx.cond(AuthState.can_delete, row())\n"
        )
        assert _effective_gates([w])("synthetic.py", "row") == {"can_delete"}

    def test_one_ungated_call_site_defeats_the_others(self):
        """The intersection is the load-bearing part.

        A helper gated on one page and rendered bare on another is ungated —
        this is exactly the shape ``api_key_create_controls`` has, appearing on
        both ``/api-keys`` and the Settings card.
        """
        w = self._walk(
            "import reflex as rx\n"
            "def block():\n"
            "    return rx.button('x', on_click=S.create_api_key)\n"
            "def gated():\n"
            "    return rx.cond(AuthState.can_administer, block())\n"
            "def bare():\n"
            "    return block()\n"
        )
        assert _effective_gates([w])("synthetic.py", "block") == set()

    def test_a_callback_reference_counts_as_a_call_site(self):
        """``rx.foreach(rows, member_row)`` passes the helper without calling it."""
        w = self._walk(
            "import reflex as rx\n"
            "def member_row(m):\n"
            "    return rx.button('x', on_click=S.remove_member(m.id))\n"
            "def card():\n"
            "    return rx.cond(S.can_manage_members, rx.foreach(S.members, member_row))\n"
        )
        assert _effective_gates([w])("synthetic.py", "member_row") == {"can_manage_members"}

    def test_a_weaker_gate_does_not_satisfy_a_stronger_requirement(self):
        """An admin control under ``can_edit`` only is still a violation.

        This is the failure the old ``GATE_FOR_PREFIX`` map could not express at
        all: it mapped a name to one gate, so "gated, but not enough" had no
        representation.
        """
        assert ROLE_RANK[ROLE_GATES["can_edit"]] < ROLE_RANK["admin"]

    def test_an_unknown_gate_licenses_nothing(self):
        """``rx.cond(State.show_form, <admin button>)`` is not a role gate.

        Half the conds in the product are UI state (``show_create``,
        ``editing_upload_id``, ``restore_pending``). If any of them counted, the
        guard would pass on almost anything.
        """
        assert "show_form" not in ROLE_GATES
        assert "show_create" not in ROLE_GATES
        assert "restore_pending" not in ROLE_GATES

    def test_a_select_on_change_is_a_control(self):
        """``rx.select(on_change=lambda v: State.change_member_role(...))``.

        Admin-gated, and invisible to a matcher that knew only ``rx.button``.
        The handler sits inside a lambda — the third of the three reference
        shapes ``_handler_refs`` has to reach.
        """
        w = self._walk(
            "import reflex as rx\n"
            "def row(m):\n"
            "    return rx.select(m.roles,\n"
            "        on_change=lambda v: S.change_member_role(m.id, v))\n"
        )
        assert ("S", "change_member_role") in {(c[2], c[3]) for c in w.controls}

    def test_an_upload_on_drop_is_a_control(self):
        """``rx.upload(on_drop=...)`` drives the admin-gated backup restore."""
        w = self._walk(
            "import reflex as rx\n"
            "def card():\n"
            "    return rx.upload(on_drop=B.handle_restore_upload(rx.upload_files()))\n"
        )
        assert ("B", "handle_restore_upload") in {(c[2], c[3]) for c in w.controls}

    def test_a_no_argument_handler_is_seen(self):
        """``on_click=State.leave_org`` is a bare ``ast.Attribute``, not a Call.

        This is the shape core#851's guard cannot reach, and the reason
        ``SettingsState.leave_org`` went unfound by three separate sweeps. Named
        here so this matcher cannot quietly regress to Call-only.
        """
        w = self._walk(
            "import reflex as rx\ndef row():\n    return rx.button('leave', on_click=S.leave_org)\n"
        )
        assert ("S", "leave_org") in {(c[2], c[3]) for c in w.controls}

    def test_a_handler_with_no_check_role_is_not_required_to_be_gated(self):
        """``edit_channel`` persists nothing and declares no role.

        The guard must not invent requirements — over-reach would push people to
        add gates that do not correspond to any refusal, and the next reader
        could not tell which gates were load-bearing.
        """
        assert "edit_channel" not in declared_handler_roles()
