"""A secret may reach a public Reflex state var only from a role-gated handler (core#972).

## The class, stated once

Every finding this module holds down has the same shape:

> **a value crossed a boundary under a different name and stopped being the
> thing the system was protecting.**

`NotificationChannel.config` is a credential store — it is where the Telegram
**bot token** and the Slack **webhook URL** live, named by
``notification_service._CONFIG_REQUIRED``. `ChannelItem.config` is a rendering
DTO. The name survived the crossing; the protection did not. `ChannelItem` is
an element of `NotificationState.channels`, a **public** Reflex state var, so
every one of those values was serialized to every connected client — and the
channel list is *deliberately* member-visible (core#886), so a non-admin
member's browser received the org's bot token. The delete dialog meanwhile
renders `notifications.delete_secret`: *"Its webhook URL or bot token is not
shown again."* True of the DOM, false of the wire.

`ConnectionService.get_connection_config` decrypts a warehouse credential.
`ConnectionState.form_password` is a form field. Same crossing, same shape,
worse payload — and it reached state from a handler with no role gate at all.

## Why this is a taint rule and not a name rule

Two cheaper rules were measured first and both are wrong:

* **"no public state var may be named like a secret"** — flags 18 members, of
  which 16 are `form_*` inputs the user *typed*. A value the user just supplied
  is not a disclosure; the direction is the whole question.
* **"no handler that decrypts may be ungated"** — flags 7, of which 4 are
  correct as they stand: `test_saved_connection`, `load_preview`,
  `preview_result` and `preview_compiled_sql` decrypt in order to *use* the
  credential and never put it in state. Gating those would take a member-visible
  action away for no security gain, and a guard whose remedy is a product
  regression will be "fixed" by an exemption.

What separates the defects from the four false positives is not the read and
not the name. It is the **assignment into a public state var** — the boundary
crossing itself. So that is what is asserted, and the four decrypt-and-discard
handlers are the negative control below: if a future change makes them fail
here, the extractor has stopped discriminating.

## Both halves of the rule are load-bearing

*Role-gated* is the exemption rather than a blanket ban because an admin
editing a channel legitimately needs the stored token back in the form. The
rule is not "secrets never reach the client"; it is "only a caller the server
has checked may receive one".

⚠️ Read the *sources* off the models and the services, never a hand-kept list.
A new encrypted column or a new decrypting service method is then covered on
the day it is written, which is the only moment anybody is thinking about it.
"""

import ast
import pathlib
import re

import pytest

import datanika.models
import datanika.services
import datanika.ui.state

MODELS_DIR = pathlib.Path(datanika.models.__file__).parent
SERVICES_DIR = pathlib.Path(datanika.services.__file__).parent
STATE_DIR = pathlib.Path(datanika.ui.state.__file__).parent

ROLE_GUARD = "_check_role"

#: Column names that *look* secret-bearing but are not credentials.
#: Each entry states why, so removing one is an argument rather than an edit.
NOT_A_SECRET = {
    "password_changed_at": "a timestamp — the revocation baseline, not a secret",
}


def _call_names(node: ast.AST) -> set[str]:
    names: set[str] = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            f = n.func
            if isinstance(f, ast.Attribute):
                names.add(f.attr)
            elif isinstance(f, ast.Name):
                names.add(f.id)
    return names


def _classes(path: pathlib.Path) -> list[ast.ClassDef]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]


def secret_columns() -> set[str]:
    """ORM attributes that hold, or directly encode, a credential.

    Derived from ``Mapped[...]`` annotations in ``datanika/models`` so a new
    encrypted column is covered without anyone remembering to come here.
    ``config`` is included by name: it is the JSON credential store for
    notification channels, and it is precisely the column whose crossing this
    module exists to stop.
    """
    pattern = re.compile(r"(secret|token|password|credential|private_key|encrypted)", re.I)
    found: set[str] = set()
    for path in sorted(MODELS_DIR.glob("*.py")):
        for cls in _classes(path):
            for stmt in cls.body:
                if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
                    continue
                if not ast.unparse(stmt.annotation).startswith("Mapped"):
                    continue
                name = stmt.target.id
                if pattern.search(name) or name == "config":
                    found.add(name)
    return found - set(NOT_A_SECRET)


def decrypting_methods() -> set[str]:
    """Service methods whose body calls ``.decrypt(...)``.

    The bare ``decrypt`` is dropped: it is ``EncryptionService.decrypt`` itself,
    and keeping it would make every service that owns an encryptor a source.
    """
    found: set[str] = set()
    for path in sorted(SERVICES_DIR.glob("*.py")):
        for cls in _classes(path):
            for fn in cls.body:
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for n in ast.walk(fn):
                    if (
                        isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Attribute)
                        and n.func.attr == "decrypt"
                    ):
                        found.add(fn.name)
    return found - {"decrypt"}


def _is_secret_expr(node: ast.AST, sources: set[str], columns: set[str]) -> bool:
    """Does this expression tree read a credential?"""
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            f = n.func
            if isinstance(f, ast.Attribute) and f.attr in sources:
                return True
            if isinstance(f, ast.Name) and f.id in sources:
                return True
        if isinstance(n, ast.Attribute) and n.attr in columns:
            return True
    return False


def _public_self_assignments(fn: ast.AST) -> list[tuple[ast.Assign, str]]:
    """``self.<public> = ...`` assignments inside ``fn``."""
    out = []
    for n in ast.walk(fn):
        if not isinstance(n, ast.Assign):
            continue
        for tgt in n.targets:
            if (
                isinstance(tgt, ast.Attribute)
                and isinstance(tgt.value, ast.Name)
                and tgt.value.id == "self"
                and not tgt.attr.startswith("_")
            ):
                out.append((n, tgt.attr))
    return out


def _leaking_helpers(cls: ast.ClassDef) -> set[str]:
    """Private methods that copy one of their parameters into a public state var.

    ``edit_connection`` never assigns the decrypted config itself — it hands it
    to ``_populate_form_from_config``. A rule that only looked at the handler's
    own assignments would report the worst instance in the tree as clean.
    """
    leaking = set()
    for fn in cls.body:
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not fn.name.startswith("_"):
            continue
        params = {a.arg for a in fn.args.args} - {"self"}
        for assign, _attr in _public_self_assignments(fn):
            names = {x.id for x in ast.walk(assign.value) if isinstance(x, ast.Name)}
            if names & params:
                leaking.add(fn.name)
    return leaking


def secret_flows() -> list[dict]:
    """Every place a credential reaches a public Reflex state var."""
    sources = decrypting_methods()
    columns = secret_columns()
    flows: list[dict] = []

    for path in sorted(STATE_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for cls in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
            helpers = _leaking_helpers(cls)
            for fn in cls.body:
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if fn.name.startswith("_"):
                    continue

                # Locals bound from a credential read.
                tainted: set[str] = set()
                for n in ast.walk(fn):
                    if isinstance(n, ast.Assign) and _is_secret_expr(n.value, sources, columns):
                        for tgt in n.targets:
                            if isinstance(tgt, ast.Name):
                                tainted.add(tgt.id)

                sinks: list[str] = []
                for assign, attr in _public_self_assignments(fn):
                    names = {x.id for x in ast.walk(assign.value) if isinstance(x, ast.Name)}
                    if (names & tainted) or _is_secret_expr(assign.value, sources, columns):
                        sinks.append(f"self.{attr}")
                for n in ast.walk(fn):
                    if (
                        isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Attribute)
                        and isinstance(n.func.value, ast.Name)
                        and n.func.value.id == "self"
                        and n.func.attr in helpers
                    ):
                        args = list(n.args) + [k.value for k in n.keywords]
                        argnames: set[str] = set()
                        for a in args:
                            argnames |= {x.id for x in ast.walk(a) if isinstance(x, ast.Name)}
                            if _is_secret_expr(a, sources, columns):
                                sinks.append(f"{n.func.attr}(...)")
                        if argnames & tainted:
                            sinks.append(f"{n.func.attr}(...)")

                if sinks:
                    flows.append(
                        {
                            "key": (path.name, cls.name, fn.name),
                            "line": fn.lineno,
                            "sinks": sorted(set(sinks)),
                            "gated": ROLE_GUARD in _call_names(fn),
                        }
                    )
    return flows


# --------------------------------------------------------------------------
# Anti-vacuity. A sweep that finds nothing satisfies every assertion below in
# silence, and that is the documented failure mode of a source-derived guard in
# this repo. Each floor is set under the measured count and above zero.
# --------------------------------------------------------------------------


def test_the_secret_column_extractor_is_armed():
    columns = secret_columns()
    assert len(columns) >= 6, (
        f"only {len(columns)} secret-bearing columns found in {MODELS_DIR}: {sorted(columns)}. "
        "The model annotations have changed shape and this module is now vacuous."
    )
    assert "config" in columns, (
        "NotificationChannel.config is the column core#972 is about; if it is no longer "
        "matched, the guard cannot see the defect it was written for"
    )
    assert "config_encrypted" in columns, "Connection.config_encrypted is no longer matched"


def test_the_decrypting_method_extractor_is_armed():
    methods = decrypting_methods()
    assert "get_connection_config" in methods, (
        "ConnectionService.get_connection_config is the method that hands a decrypted "
        "warehouse credential to the UI; the extractor no longer sees it"
    )
    assert len(methods) >= 3, f"only {len(methods)} decrypting service methods found: {methods}"


def test_the_flow_extractor_is_armed():
    """At least the legitimate, role-gated flows must still be found.

    If this drops to zero the tree looks clean for the same reason a broken
    metric looks healthy — nothing is being examined.
    """
    flows = secret_flows()
    assert flows, (
        "no credential-to-public-state flows found at all. Either every such flow was "
        "removed (in which case lower this floor deliberately) or the AST walk has "
        "stopped matching and the assertion below can no longer fail."
    )


def test_no_credential_reaches_a_public_state_var_without_a_role_gate():
    """The rule. Red before the fix on four handlers across two modules.

    * ``NotificationState.load_channels`` → ``self.channels`` — core#972 itself.
      Not fixable with a gate: the list is *meant* to be member-visible, so the
      secret must leave the DTO.
    * ``NotificationState.edit_channel`` → the form — read off client-visible
      state, which is why it needs both a server-side re-read and a gate.
    * ``ConnectionState.edit_connection`` / ``.copy_connection`` → the form,
      carrying a decrypted warehouse password, AWS secret key or service-account
      JSON, from a handler with no role check whatsoever. The buttons are hidden
      behind ``rx.cond(AuthState.can_edit, ...)``, which is a *render* condition;
      a Reflex event handler is dispatched by name over the websocket and does
      not care which buttons were drawn.
    """
    offenders = [f for f in secret_flows() if not f["gated"]]
    assert not offenders, (
        "these handlers put a credential into a public Reflex state var — which is "
        "serialized to the connected client — without checking the caller's role "
        "(core#972):\n  "
        + "\n  ".join(
            f"{f['key'][0]}:{f['line']}  {f['key'][1]}.{f['key'][2]} -> {', '.join(f['sinks'])}"
            for f in sorted(offenders, key=lambda f: f["key"])
        )
        + "\n\nTwo remedies, and which one applies is a product question:\n"
        "  * the value must not be there at all -> drop it from the DTO and re-read "
        "server-side when an authorised caller actually needs it;\n"
        "  * an authorised caller legitimately needs it -> add "
        "`if not await self._check_role(<role>): return`.\n"
        "Do NOT widen the extractor's exemptions."
    )


#: Handlers that decrypt a credential in order to **use** it and never put it in
#: state. They must keep failing to appear above: they are the discriminator
#: between "this rule found the disclosure" and "this rule flags every decrypt".
DECRYPT_WITHOUT_DISCLOSING = [
    ("connection_state.py", "ConnectionState", "test_saved_connection"),
    ("model_detail_state.py", "ModelDetailState", "load_preview"),
    ("transformation_state.py", "TransformationState", "preview_result"),
    ("transformation_state.py", "TransformationState", "preview_compiled_sql"),
]


@pytest.mark.parametrize("key", DECRYPT_WITHOUT_DISCLOSING, ids=lambda k: f"{k[1]}.{k[2]}")
def test_decrypting_without_disclosing_is_not_flagged(key):
    """The false-positive control.

    Each of these reads a decrypted credential and hands it to a database
    driver. None of them assigns it to a public state var, so none is a
    disclosure — and a rule that reported them would be answered with a role
    gate that removes a member-visible action for no gain.

    This also pins the *cheaper* rule that was rejected: "any handler that
    decrypts must be gated" makes all four of these red.
    """
    flagged = {f["key"] for f in secret_flows()}
    assert key not in flagged, (
        f"{key[1]}.{key[2]} decrypts a credential but does not assign it to a public "
        "state var, so flagging it means the taint analysis has lost its "
        "discrimination — every decrypt now looks like a disclosure"
    )


@pytest.mark.parametrize("key", DECRYPT_WITHOUT_DISCLOSING, ids=lambda k: f"{k[1]}.{k[2]}")
def test_the_control_handlers_still_exist(key):
    """A control that names a deleted handler controls nothing."""
    path = STATE_DIR / key[0]
    assert path.exists(), f"{key[0]} is gone; this control is dead"
    names = {
        (cls.name, fn.name)
        for cls in _classes(path)
        for fn in cls.body
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert (key[1], key[2]) in names, (
        f"{key[1]}.{key[2]} no longer exists, so the false-positive control above "
        "passes vacuously — repoint it at a handler that really does decrypt "
        "without disclosing"
    )
