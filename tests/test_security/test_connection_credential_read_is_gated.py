"""Reading back a stored warehouse credential is a privileged action (core#972 sibling).

`ConnectionState.edit_connection` and `.copy_connection` call
`ConnectionService.get_connection_config`, which **decrypts** the connection's
config, and hand the result to `_populate_form_from_config` — which writes it
into `form_password`, `form_aws_secret_access_key`, `form_service_account_json`,
`form_client_secret`, `form_refresh_token` and friends. Those are public Reflex
state vars, so the values are serialized to the caller's browser.

Neither handler checked the caller's role.

⚠️ **The buttons are gated and that is not the same thing.**
`connections.py` wraps both in `rx.cond(AuthState.can_edit, ...)`. A Reflex
event handler is dispatched **by name** over the websocket; whether a button was
rendered has no bearing on whether the event can be sent. The same file already
knows this — `save_connection` checks `editor` and `delete_connection` checks
`admin`, and `_delete_connection_dialog`'s docstring says outright that "the gate
lives inside the helper". The two credential-*reading* handlers were the ones
where the reasoning was skipped, because reading does not look like an action.

`editor` is the role asserted, not `admin`: it is what `can_edit` resolves to
(`check_role_hierarchy(role, "editor")`), and it is what `save_connection`
requires — so the gate matches the UI's own claim about who may edit rather than
inventing a new boundary.
"""

import ast
import inspect
import pathlib
import textwrap

import pytest

import datanika.ui.state
from datanika.ui.state.base_state import check_role_hierarchy
from datanika.ui.state.connection_state import ConnectionState

STATE_DIR = pathlib.Path(datanika.ui.state.__file__).parent

#: Handlers that decrypt a stored credential and put it into public state.
CREDENTIAL_READING = ["edit_connection", "copy_connection"]

#: The control: same module, same service call, and correct as it stands —
#: it opens a connection with the credential and never assigns it to state.
DECRYPTS_WITHOUT_DISCLOSING = "test_saved_connection"


def _unwrap(func):
    return getattr(func, "fn", func)


def _parse(name: str) -> ast.AST:
    return ast.parse(textwrap.dedent(inspect.getsource(_unwrap(getattr(ConnectionState, name)))))


def _src(name: str) -> str:
    """Handler source with the docstring removed.

    🚨 The fixed handlers *explain* why they gate, quoting ``_check_role`` and
    ``get_connection_config`` in prose. A textual guard over self-documenting
    code is satisfied by the documentation — the failure mode is that the
    control below (`test_the_control_handler_stays_ungated`) would go red the
    moment somebody wrote a comment mentioning the gate they deliberately did
    not add. Comments go too: ``ast.unparse`` drops them.
    """
    tree = _parse(name)
    fn = tree.body[0]
    if (
        fn.body
        and isinstance(fn.body[0], ast.Expr)
        and isinstance(fn.body[0].value, ast.Constant)
        and isinstance(fn.body[0].value.value, str)
    ):
        fn.body = fn.body[1:]
    return ast.unparse(fn)


def _awaited_role_gate(name: str) -> str | None:
    """The role passed to an **awaited** ``self._check_role(...)``, if any.

    Structural because the textual version cannot see the defect that matters:
    ``_check_role`` is a coroutine, so ``if not self._check_role("editor")``
    parses, reads as a guard in review, and refuses nobody.
    """
    for node in ast.walk(_parse(name)):
        if not isinstance(node, ast.Await):
            continue
        call = node.value
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "_check_role"
            and call.args
            and isinstance(call.args[0], ast.Constant)
        ):
            return call.args[0].value
    return None


@pytest.mark.parametrize("name", CREDENTIAL_READING)
def test_the_handler_still_decrypts(name):
    """Anti-vacuity.

    Every assertion below is about a handler that reads a credential. If the
    read moves elsewhere, the gate assertions keep passing while guarding
    nothing, so pin the premise first.
    """
    assert "get_connection_config" in _src(name), (
        f"{name} no longer calls get_connection_config, so the tests in this module "
        "no longer describe it — repoint them at whatever reads the credential now"
    )


@pytest.mark.parametrize("name", CREDENTIAL_READING)
def test_the_gate_is_an_awaited_editor_check(name):
    """The regression. Red on both handlers before the fix.

    `editor` because that is what `AuthState.can_edit` resolves to and what
    `save_connection` already requires; awaited because an un-awaited
    `_check_role` is a truthy coroutine that refuses nobody.
    """
    assert inspect.iscoroutinefunction(_unwrap(getattr(ConnectionState, name)))
    assert _awaited_role_gate(name) == "editor", (
        f"ConnectionState.{name} decrypts the stored connection config and writes it "
        "into public form state, so any authenticated member — a viewer included — "
        "can retrieve the org's warehouse password, AWS secret key or service-account "
        "JSON by dispatching the event directly. Hiding the button is not a gate. "
        "Add `if not await self._check_role('editor'): return`."
    )


@pytest.mark.parametrize("name", CREDENTIAL_READING)
def test_the_gate_precedes_the_decrypt(name):
    """Order matters, and a present-but-late gate reads exactly like a correct one.

    A check placed after `get_connection_config` still refuses the *caller*, but
    the credential has already been decrypted and — depending on where the early
    return lands — may already be in the form. Assert the gate is the first
    thing the handler does.
    """
    fn = _parse(name).body[0]
    body = [
        s for s in fn.body if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
    ]
    first = ast.unparse(body[0])
    assert "_check_role" in first, (
        f"ConnectionState.{name}'s first statement is `{first}`, not the role check. "
        "The gate must run before anything is read or decrypted."
    )


def test_the_control_handler_stays_ungated():
    """The false-positive control, and the reason this is not "gate every decrypt".

    `test_saved_connection` decrypts in order to *open* the connection and puts
    only a verdict in state. The Test button is deliberately outside
    `rx.cond(AuthState.can_edit, ...)` in `connections.py`, so gating it would
    take a member-visible action away and buy nothing.

    If somebody satisfies the assertions above by decorating the whole class,
    this goes red.
    """
    src = _src(DECRYPTS_WITHOUT_DISCLOSING)
    assert "get_connection_config" in src, (
        "the control no longer decrypts, so it no longer controls anything"
    )
    assert "_check_role" not in src, (
        "test_saved_connection acquired a role gate. It reads a credential to use it, "
        "not to disclose it — the Test button is member-visible on purpose. A blanket "
        "gate over every decrypting handler is the cheap rule this module rejects."
    )


def test_editor_is_the_role_can_edit_resolves_to():
    """Pin the correspondence the gate relies on.

    If `can_edit` is ever retuned to `admin`, the handler gate and the button
    condition part company silently — the button disappears for editors while
    the event they can no longer see still succeeds for them.
    """
    assert check_role_hierarchy("editor", "editor")
    assert check_role_hierarchy("admin", "editor")
    assert not check_role_hierarchy("viewer", "editor"), (
        "a viewer satisfies the editor gate, so the fix in this module stops nothing"
    )

    # `can_edit` is a Reflex computed var, not a plain function, so
    # `inspect.getsource` cannot reach it — read it out of the module instead.
    tree = ast.parse((STATE_DIR / "auth_state.py").read_text(encoding="utf-8"))
    can_edit = next(
        (
            fn
            for cls in ast.walk(tree)
            if isinstance(cls, ast.ClassDef) and cls.name == "AuthState"
            for fn in cls.body
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and fn.name == "can_edit"
        ),
        None,
    )
    assert can_edit is not None, "AuthState.can_edit is gone; this correspondence is unpinned"
    roles = {
        n.args[1].value
        for n in ast.walk(can_edit)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "check_role_hierarchy"
        and len(n.args) > 1
        and isinstance(n.args[1], ast.Constant)
    }
    assert roles == {"editor"}, (
        f"AuthState.can_edit now resolves to {roles or 'something this test cannot read'}, "
        "not `editor`. The handler gates and the button conditions have parted company: "
        "the Edit button disappears for a role that can still dispatch the event."
    )
