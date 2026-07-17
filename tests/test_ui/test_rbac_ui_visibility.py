"""RBAC UI-visibility: destructive/create controls are gated by membership role.

Companion to ``test_rbac_enforcement.py`` (which locks the *handler* ``_check_role``
gates). This module locks the *visibility* gates: a viewer must not SEE the
create / edit / delete controls whose handlers they cannot invoke — "a viewer
that gets to a delete button by accident is a regression".

Uses AST source-inspection (no Reflex state instantiation, matching the repo's
RBAC test convention) so the invariant survives refactors of the page markup.

Regression guard for datanika-core#313.
"""

import ast
import inspect

import pytest

# visibility computed-var -> the minimum role its check_role_hierarchy call must require
VIS_VARS = {"can_edit": "editor", "can_delete": "admin"}

# The five tenant resource pages whose list rows expose create/destructive controls.
RESOURCE_PAGES = [
    "datanika.ui.pages.connections",
    "datanika.ui.pages.uploads",
    "datanika.ui.pages.pipelines",
    "datanika.ui.pages.transformations",
    "datanika.ui.pages.schedules",
]

# handler-name prefix -> the visibility var that must gate that control.
# save_* (the form submit) is gated at the form's call site, not inline, and is
# verified separately by ``test_create_form_is_gated``.
GATE_FOR_PREFIX = {
    "delete_": "can_delete",
    "edit_": "can_edit",
    "copy_": "can_edit",
    "run_": "can_edit",
    "toggle_": "can_edit",
}


# --------------------------------------------------------------------------- #
# AST helpers
# --------------------------------------------------------------------------- #


def _module_source(dotted: str) -> str:
    mod = __import__(dotted, fromlist=[dotted.rsplit(".", 1)[-1]])
    with open(inspect.getfile(mod), encoding="utf-8") as f:
        return f.read()


def _attr_names(node: ast.AST) -> set[str]:
    """Every attribute name referenced anywhere under ``node``.

    ``AuthState.can_edit`` -> ``{"can_edit"}``; a compound
    ``AuthState.can_edit & Other.flag`` -> ``{"can_edit", "flag"}``.
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


def _button_handler(call: ast.Call) -> str | None:
    """For an ``rx.button(...)`` call, return its ``on_click`` handler method name.

    ``on_click=State.delete_x(row.id)`` -> ``"delete_x"``;
    ``on_click=State.save_x`` -> ``"save_x"``; ``None`` if not a button / no on_click.
    """
    if not _is_rx_call(call, "button"):
        return None
    for kw in call.keywords:
        if kw.arg != "on_click":
            continue
        value = kw.value
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
            return value.func.attr
        if isinstance(value, ast.Attribute):
            return value.attr
    return None


def _required_gate(handler: str) -> str | None:
    return next((g for p, g in GATE_FOR_PREFIX.items() if handler.startswith(p)), None)


class _GateChecker(ast.NodeVisitor):
    """Walk a page module tracking enclosing ``rx.cond`` visibility-gates and
    record any create/destructive button rendered outside its required gate."""

    def __init__(self) -> None:
        self.gate_stack: list[set[str]] = []
        self.violations: list[tuple[str, str]] = []  # (handler, required_gate)

    def visit_Call(self, node: ast.Call) -> None:
        handler = _button_handler(node)
        if handler:
            required = _required_gate(handler)
            if required:
                active: set[str] = set().union(*self.gate_stack) if self.gate_stack else set()
                if required not in active:
                    self.violations.append((handler, required))

        if _is_rx_call(node, "cond") and node.args:
            gates = _attr_names(node.args[0]) & set(VIS_VARS)
            self.gate_stack.append(gates)
            self.generic_visit(node)
            self.gate_stack.pop()
        else:
            self.generic_visit(node)


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


def _form_is_gated(source: str) -> bool:
    """True if some ``rx.cond(<...can_edit...>, ... *_form() ...)`` exists — the
    create/edit form is only rendered for roles that pass ``can_edit``."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not (_is_rx_call(node, "cond") and node.args):
            continue
        if "can_edit" not in _attr_names(node.args[0]):
            continue
        for branch in node.args[1:]:
            for sub in ast.walk(branch):
                if (
                    isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Name)
                    and sub.func.id.endswith("_form")
                ):
                    return True
    return False


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


class TestVisibilityVars:
    """AuthState exposes the visibility gates with the same thresholds the
    handlers enforce (editor for create/edit, admin for delete)."""

    def test_authstate_defines_visibility_vars(self):
        src = _module_source("datanika.ui.state.auth_state")
        for var, role in VIS_VARS.items():
            assert _computed_var_role(src, "AuthState", var) == role, (
                f"AuthState.{var} must be an @rx.var calling "
                f"check_role_hierarchy(self.current_role, '{role}')"
            )


class TestDestructiveControlsGated:
    """Every create/destructive row control is wrapped in the matching gate."""

    @pytest.mark.parametrize("page", RESOURCE_PAGES)
    def test_destructive_controls_are_role_gated(self, page):
        checker = _GateChecker()
        checker.visit(ast.parse(_module_source(page)))
        assert not checker.violations, (
            f"{page}: create/destructive buttons rendered without their role gate: "
            f"{checker.violations}"
        )

    @pytest.mark.parametrize("page", RESOURCE_PAGES)
    def test_create_form_is_gated(self, page):
        assert _form_is_gated(_module_source(page)), (
            f"{page}: create/edit form must render only under rx.cond(AuthState.can_edit, ...)"
        )


class TestGateCheckerSelfCheck:
    """Guard the AST checker itself so a broken checker can't hide regressions."""

    def test_ungated_delete_is_flagged(self):
        src = (
            "import reflex as rx\n"
            "def page():\n"
            "    return rx.button('x', on_click=S.delete_thing(i))\n"
        )
        checker = _GateChecker()
        checker.visit(ast.parse(src))
        assert ("delete_thing", "can_delete") in checker.violations

    def test_gated_delete_passes(self):
        src = (
            "import reflex as rx\n"
            "def page():\n"
            "    return rx.cond(AuthState.can_delete,\n"
            "        rx.button('x', on_click=S.delete_thing(i)))\n"
        )
        checker = _GateChecker()
        checker.visit(ast.parse(src))
        assert checker.violations == []

    def test_wrong_gate_is_flagged(self):
        # a delete button under only can_edit is still a violation
        src = (
            "import reflex as rx\n"
            "def page():\n"
            "    return rx.cond(AuthState.can_edit,\n"
            "        rx.button('x', on_click=S.delete_thing(i)))\n"
        )
        checker = _GateChecker()
        checker.visit(ast.parse(src))
        assert ("delete_thing", "can_delete") in checker.violations
