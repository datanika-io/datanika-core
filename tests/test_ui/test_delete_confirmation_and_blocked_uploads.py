"""Delete asks first, and a broken upload says so (core#804, core#805).

Two defects with one shape: the product told the user something untrue and then
behaved as though it were true.

**core#804.** ``/connections`` Delete was wired straight to the handler
(``on_click=ConnectionState.delete_connection(conn.id)``), so one click retired
a production connection — no dialog, no toast, no undo affordance. Measured
live: ``document.querySelectorAll('[role="dialog"], [role="alertdialog"]')``
returned **0** immediately after the click. This project has already lost
connections 13–17 to a mis-aimed delete on this exact page; the usual account is
a selector bug, and it was, but a selector bug could delete five rows unattended
only because *nothing between the click and the mutation asked a second
question*. Worse, the safety rule written in response — `WORKFLOW_RULES.md` §7b,
*"assert the dialog is open before clicking inside it"* — described a control
that did not exist, and an agent following it literally concludes the click
failed and clicks again. On this page that is a second deletion.

**core#805.** Soft-deleting a connection left dependent uploads reading
``active``, showing their source as a bare ``#31``, and offering a live **Run**
button that queues a run which cannot succeed.

⚠️ **These are structural assertions over the component tree, not screenshots.**
That is deliberate for the dialog: what makes §7b true again is specifically
that a ``role="alertdialog"`` element exists and that the destructive handler
hangs off its *action*, not off the trigger. A test that only checked "a dialog
component is present" would pass on a dialog whose Delete button still deleted
on the way in.

**core#851 — the remaining ten, and the change of shape that matters.** core#804
fixed two pages of twelve, and this module policed exactly those two by name. A
parametrize list is a hand-maintained claim about a codebase that moves: it goes
green on a page nobody added to it, which is the whole reason ten one-click
deletes survived a PR whose subject was one-click deletes. So the call sites are
now **derived** — every ``<Something>State.<destructive>()`` reference under
``datanika/ui/`` is discovered by walking the source, and each one must either be
reached through ``rx.alert_dialog.action`` or appear in
:data:`UNCONFIRMED_BY_DESIGN` with a reason.

Three things keep that from being decorative:

1. **The scan covers ``datanika/ui/``, not ``datanika/ui/pages/``.** Moving a
   destructive control into a component (which core#851 does for the API-key
   row, rendered on two surfaces) must not move it out of the guard's view.
   That lexical blindness is precisely what ``_GateChecker`` in
   ``test_rbac_ui_visibility.py`` suffers from for role gates, and it is why the
   dialog helpers here carry their own ``rx.cond``.
2. **Each exclusion must be earned, not asserted.** For every entry in
   ``UNCONFIRMED_BY_DESIGN`` the guard reads the *state handler* and requires it
   to touch no session, service or commit. The day ``remove_column_test`` starts
   persisting, its exclusion stops being true and this goes red — rather than
   remaining a comment somebody wrote in 2026.
3. **The dialog is a claim the client makes; the handler check is the refusal.**
   So the guard also requires every persisted destructive handler to carry a
   server-side refusal. That assertion was **red on `DagState.remove_dependency`**
   when it was written — the one persisted destructive handler in the product
   with no role check at all, which no existing test could see because
   ``test_rbac_enforcement.py``'s ``EXPECTED_ROLES`` had no ``dag_state`` entry.
   A guard whose first run is green has not been shown able to fail.

**SPEC_MUTATION_FEEDBACK §7 / D8 — four ways this census was blind.** Deciding
``leave_org``'s confirmation exposed that the guard could not *see* the control
it was deciding about. core#851 named two gaps; measuring found two more.

===  ====================================================================
gap  what it was, and what it hid
===  ====================================================================
1    the predicate was a list of **spellings** — missed ``cancel_invitation``
     (spelled ``cancel_``) and ``leave_org``. Now verb-prefix ∪
     ``_audit(…, "delete", …)``, with :data:`CENSUS_DISAGREEMENT` asserting
     every disagreement rather than silently unioning them.
2    the matcher walked only ``ast.Call`` — so **every zero-argument handler
     was invisible by node type**, 43 call-shaped references against 205
     attribute-shaped ones. The invisible set included
     ``AccountState.delete_account``.
3    a confirmation was recognised only as ``alert_dialog.action`` — the
     typed confirmation on ``delete_account`` is a form ``on_submit`` inside
     ``alert_dialog.content``, and could never have passed. See
     :data:`FORM_CONFIRMED`.
4    🚨 the role check was a **substring over the handler source, which
     includes the docstring**. ``leave_org``'s docstring explains why it
     deliberately has no role check, and that explanation contains the
     literal ``_check_role`` — so closing gap 2 would have made this guard
     go **green on it because of a sentence saying it is not guarded**. Now
     read by AST; see :func:`_self_calls`.
===  ====================================================================

**Fifteen negative controls, every one applied to a real shipped file** — a
synthetic control is written from the same mental model as the check, so it
agrees with the check including where the check is wrong. Each had to turn *its
own* assertion red **and name the cause**; a red for an unrelated reason is not
a control. Reproduce any of them by applying the mutation and running the named
test:

*mutation on the real file* -> *assertion that caught it*

1.  remove the ``ast.Attribute`` arm
    -> ``test_the_matcher_sees_both_reference_shapes``
2.  narrow that arm to ``on_click`` only
    -> ``test_the_scan_is_not_blind``, naming ``leave_org``
3.  drop the audit derivation from the predicate
    -> ``test_the_scan_is_not_blind``, naming ``cancel_invitation``
4.  delete ``leave_org``'s ``_require_live_session`` call
    -> ``test_every_persisted_destructive_handler_checks_a_role``
5.  🔑 leave that call **only in a comment** -> same assertion. This is gap 4,
    and it is the whole reason the check reads the AST.
6.  rewire ``leave_org`` onto the dialog *trigger*
    -> ``test_every_destructive_call_is_behind_a_confirmation``
7.  unwire ``cancel_invitation`` back to a bare button -> same
8.  withdraw ``delete_account`` from :data:`FORM_CONFIRMED` -> same
9.  collapse the leave dialog to one outcome branch
    -> ``test_the_dialog_renders_exactly_one_of_them``
10. move ``transfer_ownership`` onto its trigger
    -> ``test_the_handler_hangs_off_the_action``
11. ``return`` ``update_org``'s toast instead of yielding it
    -> ``test_it_yields_a_toast`` in the ratchet module
12. delete ``change_member_role``'s toast -> same
13. unclassify ``transfer_ownership``
    -> ``test_every_committing_handler_is_classified``
14. undeclare ``leave_org``'s census disagreement
    -> ``test_every_census_disagreement_is_declared``
15. slip a service call in front of the refusal
    -> ``test_the_check_is_the_first_thing_the_handler_does``

⚠️ **One of those fifteen was wrong on its first run and went green** — it
removed the ``ast.Constant`` requirement from :func:`_is_state_reset`, which
only makes the skip *more* permissive, and since the next statement in
``delete_account`` is an ``if`` rather than an assignment, nothing extra was
skipped and the outcome was identical. **A mutation that changes no behaviour is
not a control**, however plausible it looks written down. The replacement
exercises the property the allowance exists for.

⚠️ **And eight of them silently matched nothing on their first run.** These
files are ``i/lf w/crlf`` in the working tree, so a multi-line anchor written
with ``\\n`` matches zero times — which reads as *"the anchor moved"* rather than
*"my harness cannot see the file"*. The tell was that **every single-line
control passed and every multi-line one failed**. Normalise line endings before
matching source.
"""

import ast
import json
import re
from pathlib import Path

import pytest
import reflex as rx

import datanika.i18n
import datanika.ui
from datanika.i18n import SUPPORTED_LOCALES, get_translations
from datanika.ui.pages import connections as connections_page
from datanika.ui.pages import uploads as uploads_page
from datanika.ui.state.connection_state import ConnectionItem
from datanika.ui.state.upload_state import UploadItem

UI_ROOT = Path(datanika.ui.__file__).parent
STATE_ROOT = UI_ROOT / "state"

#: Handler-name prefixes that denote "this takes something away".
#:
#: ⚠️ This is a list of **spellings**, not of controls, and that is its limit.
#: It is deliberately kept beside :func:`_audited_delete_handlers` rather than
#: replaced by it — see :data:`CENSUS_DISAGREEMENT`. Neither derivation is
#: complete on its own: the verb list catches an un-audited deletion, the audit
#: list catches an unusually-named one, and *that they disagree at all* is what
#: makes the pair worth having.
DESTRUCTIVE_PREFIXES = ("delete_", "remove_", "revoke_", "purge_")


def _state_modules() -> list[Path]:
    return sorted(STATE_ROOT.glob("*.py"))


def _audits_delete(fn: ast.AST) -> bool:
    """Does this handler write an audit row with ``action="delete"``?

    A semantic the author *declared*, rather than a word they happened to
    choose. It finds ``cancel_invitation`` and ``leave_org``, which no edit to
    :data:`DESTRUCTIVE_PREFIXES` ever could.
    """
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_audit"
            and len(node.args) >= 4
            and isinstance(node.args[3], ast.Constant)
            and node.args[3].value == "delete"
        ):
            return True
    return False


def _audited_delete_handlers() -> frozenset[str]:
    names: set[str] = set()
    for path in _state_modules():
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _audits_delete(node):
                names.add(node.name)
    return frozenset(names)


AUDITED_DELETE = _audited_delete_handlers()


def _is_destructive(handler: str) -> bool:
    """The census predicate: verb-prefix **∪** audit-``delete``."""
    return handler.startswith(DESTRUCTIVE_PREFIXES) or handler in AUDITED_DELETE


#: Where the two derivations disagree, and why. A silent union hides the second
#: class, which is a *finding* rather than a false positive — that is how
#: [core#934] was found. Adding a disagreement without declaring it fails.
#:
#: Classes: ``"audit-only"``  the verb list missed a real deletion.
#:          ``"verb-only"``   persists but writes no ``delete`` audit row, or
#:                            does not persist at all.
CENSUS_DISAGREEMENT: dict[str, str] = {
    "cancel_invitation": (
        "audit-only. Spelled `cancel_`, which is in no verb list, and it is a "
        "real DB mutation with an audit row. core#851's eleventh site."
    ),
    "leave_org": (
        "audit-only. Neither `leave` nor `org` is a destructive verb, and the "
        "handler removes the actor's access to everything rather than a row. "
        "core#851's twelfth site."
    ),
    "delete_account": (
        "verb-only. Account erasure is not org-scoped, and `audit_logs` is — "
        "there is no org to attribute the row to once the user is gone. The "
        "confirmation is a typed one (SPEC_PII_SEPARATION D9), not an "
        "`alert_dialog.action`; see FORM_CONFIRMED."
    ),
    "remove_dependency": (
        "verb-only, and this one is a defect rather than a design: it persists "
        "and writes no audit row at all. Filed as core#934 — the disagreement "
        "is what surfaced it, which is the argument for keeping both lists."
    ),
    "remove_model": "verb-only and form-local; see UNCONFIRMED_BY_DESIGN.",
    "remove_column_test": "verb-only and form-local; see UNCONFIRMED_BY_DESIGN.",
}

#: Destructive-looking call sites that deliberately have **no** confirmation.
#: The value is the argument, not a name — an exclusion list of bare identifiers
#: is indistinguishable from an oversight, and each of these is checked below
#: against the handler's actual body.
UNCONFIRMED_BY_DESIGN: dict[str, str] = {
    "pages/pipelines.py::remove_model": (
        "Form-local. `PipelineState.remove_model` pops an entry out of the "
        "unsaved form's model list; nothing is persisted until Save. A "
        "confirmation here would be noise, and worse, it would train the user "
        "to click through confirmations — which is what makes the ones on real "
        "deletes work."
    ),
    "pages/model_detail.py::remove_column_test": (
        "Form-local, and core#851 listed this as one of the ten before "
        "measuring it. `ModelDetailState.remove_column_test` only rebuilds "
        "`self.columns` in state; the write happens in `save_model_detail`, "
        "behind the page's own Save button. Same argument as `remove_model`."
    ),
}


#: Sites confirmed through a **form's `on_submit` inside `alert_dialog.content`**
#: rather than through `alert_dialog.action`. The value is the argument.
#:
#: ⚠️ This is not a weakening of "the handler hangs off the action". It is the
#: same guarantee reached by the only construct that can carry a *typed*
#: confirmation: the submit button lives inside the dialog content and the
#: handler fires on the form, so nothing destructive is reachable without the
#: dialog open. A second entry here should be argued, not appended.
FORM_CONFIRMED: dict[str, str] = {
    "pages/settings.py::delete_account": (
        "SPEC_PII_SEPARATION D9 requires the user to *type* a confirmation "
        "(their password, or the org name for an account that has never had "
        "one), which needs an `rx.form`. The submit control sits inside "
        "`alert_dialog.content` beside `alert_dialog.cancel`, and the handler "
        "is on the form's `on_submit`. There is no `alert_dialog.action`, and "
        "adding one would mean submitting the form from outside it."
    ),
}

#: Persisted destructive handlers whose server-side refusal is deliberately
#: **not** ``_check_role``. Each must call the alternative named here, checked
#: by AST rather than believed.
ALTERNATIVE_REFUSAL: dict[str, tuple[str, str]] = {
    "leave_org": (
        "_require_live_session",
        "SPEC_ORG_ROLES R6 — leaving is the one member-management action every "
        "member has, so a minimum-role gate would contradict the feature. The "
        "refusal that does exist is the service's owner-count invariant, which "
        "refuses the last owner. The session check is still required (core#673).",
    ),
    "delete_account": (
        "_require_live_session",
        "Account erasure is not org-scoped: there is no role within an org that "
        "grants or withholds deleting your own account. The refusal is the "
        "sole-owner check, surfaced in the dialog before the confirm control is "
        "usable (SPEC_PII_SEPARATION §9a).",
    ),
}


def _walk(component) -> list:
    """Every component in a rendered tree, including both branches of a cond."""
    out = [component]
    for child in getattr(component, "children", []) or []:
        out.extend(_walk(child))
    return out


def _tags(component) -> list[str]:
    return [type(c).__name__ for c in _walk(component)]


def _ui_modules() -> list[Path]:
    """Every UI source file — pages *and* components."""
    return sorted(p for p in UI_ROOT.rglob("*.py") if "__pycache__" not in p.parts)


class _DestructiveCallVisitor(ast.NodeVisitor):
    """Collect ``<X>State.<destructive>`` references with their enclosing calls.

    🚨 **Two node types, not one.** A Reflex handler that takes a row id is
    written ``on_click=ConnectionState.delete_connection(conn.id)`` — an
    ``ast.Call``. A handler that takes **no arguments** is written
    ``on_click=SettingsState.leave_org`` — an ``ast.Attribute``, with no
    ``Call`` node anywhere for a ``visit_Call`` to fire on.

    This class had only the ``Call`` arm, so every zero-argument destructive
    handler was invisible to it **by node type**, independently of its name —
    and no edit to :data:`DESTRUCTIVE_PREFIXES` could ever have found one.
    Measured on ``origin/dev``: **43** call-shaped handler references against
    **205** attribute-shaped ones. The invisible set included
    ``AccountState.delete_account`` — account erasure, no grace period, the most
    destructive control in the product. It is correctly implemented today, which
    is the point: **the guard was green about it without being able to see it**,
    so an unwiring tomorrow would not have been caught.

    ``_ModuleWalker`` in ``test_rbac_ui_visibility.py`` already handled both
    shapes and found ``leave_org`` on its first run, which is the only reason
    anybody looked here.
    """

    def __init__(self) -> None:
        self.stack: list[str] = []
        self.found: list[tuple[str, str]] = []  # (handler, enclosing-call chain)

    def _chain(self) -> str:
        return " < ".join(reversed(self.stack))

    def visit_Call(self, node: ast.Call) -> None:
        name = ast.unparse(node.func)
        if isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            owner = ast.unparse(node.func.value)
            if _is_destructive(attr) and owner.endswith("State"):
                self.found.append((attr, self._chain()))
        self.stack.append(name)
        self.generic_visit(node)
        self.stack.pop()

    def visit_keyword(self, node: ast.keyword) -> None:
        """The ``ast.Attribute`` arm, scoped to **event-handler wiring only**.

        ⚠️ It has to be scoped, and the first version was not. A bare
        ``<X>State.<destructive>`` attribute is not necessarily a handler:
        ``AccountState.delete_error`` is a *state var* rendered inside the
        delete dialog, and an unscoped arm reported it as a one-click
        destructive control invoked from ``rx.callout``. Requiring the
        attribute to be the value of an ``on_*`` keyword is what distinguishes
        "this wires a handler" from "this reads a var whose name starts with
        delete_".
        """
        if node.arg and node.arg.startswith("on_") and isinstance(node.value, ast.Attribute):
            owner = ast.unparse(node.value.value)
            if _is_destructive(node.value.attr) and owner.endswith("State"):
                self.found.append((node.value.attr, self._chain()))
        self.generic_visit(node)


def _destructive_call_sites() -> dict[str, list[str]]:
    """``{"pages/foo.py::delete_bar": [enclosing-call chain, ...]}`` for all of ui/."""
    sites: dict[str, list[str]] = {}
    for path in _ui_modules():
        visitor = _DestructiveCallVisitor()
        visitor.visit(ast.parse(path.read_text(encoding="utf-8")))
        rel = path.relative_to(UI_ROOT).as_posix()
        for handler, context in visitor.found:
            sites.setdefault(f"{rel}::{handler}", []).append(context)
    return sites


def _state_handler(handler: str) -> tuple[str, ast.AST, str]:
    """``(state-module filename, function node, source)`` for this handler name.

    The node is returned alongside the text because re-parsing a source segment
    is a trap here: ``ast.get_source_segment`` unindents only the first line, so
    ``ast.parse("class _S:\\n" + segment)`` raises ``IndentationError`` on every
    method — a checker that crashes rather than one that lies, but still a
    checker that reports nothing about the thing it was pointed at.
    """
    matches: list[tuple[str, ast.AST, str]] = []
    for path in sorted(STATE_ROOT.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == handler:
                matches.append((path.name, node, ast.get_source_segment(source, node) or ""))
    assert len(matches) == 1, (
        f"expected exactly one state handler named {handler!r}, found "
        f"{[m[0] for m in matches]} — the guard resolves handlers by name"
    )
    return matches[0]


def _self_calls(fn: ast.AST) -> set[str]:
    """Names of ``self.<helper>()`` calls this handler actually makes.

    🚨 **Use this, never a substring over the handler source.** That source is
    ``ast.get_source_segment``, which **includes the docstring** — and the prose
    most likely to contain a token is the comment explaining why the token is
    absent. Measured, with ``remove_member`` as the positive control:

    ==========================  =================  ==================  ==========
    handler                     substring present  in docstring only   really calls
    ==========================  =================  ==================  ==========
    ``SettingsState.leave_org``       yes                yes               **no**
    ``SettingsState.remove_member``   yes                no                yes
    ==========================  =================  ==================  ==========

    ``leave_org``'s docstring says *"every other member-management handler in
    this class calls ``_check_role("admin")``, and doing that here would
    contradict the paragraph above."* A substring check reads that as a role
    check. So the moment the ``ast.Attribute`` arm above made ``leave_org``
    visible, the role assertion would have gone **green on it — because of a
    sentence saying it is not guarded.**
    """
    names: set[str] = set()
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
        ):
            names.add(node.func.attr)
    return names


def _is_state_reset(stmt: ast.stmt) -> bool:
    """``self.<var> = <constant>`` — a UI reset, not work.

    Deliberately narrow: only a **constant** value, and only onto ``self``. A
    call, a subscript, or anything reading the request payload is work and must
    not be allowed to precede the refusal.
    """
    return (
        isinstance(stmt, ast.Assign)
        and isinstance(stmt.value, ast.Constant)
        and all(
            isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id == "self"
            for t in stmt.targets
        )
    )


def _persists(handler_source: str) -> bool:
    """Does this handler reach the database?

    Deliberately generous: any commit, any session, or any ``svc.``/``_svc.``
    call counts. A false positive costs an unnecessary dialog; a false negative
    lets a real delete out of the guard.
    """
    markers = ("session.commit()", "get_sync_session(", "svc.", "Service()")
    return any(m in handler_source for m in markers)


class TestTheDeleteDialogExists:
    """core#804 AC1 — the click opens a dialog instead of mutating."""

    @pytest.mark.parametrize(
        ("page", "builder"),
        [
            ("connections", lambda: connections_page.connections_table()),
            ("uploads", lambda: uploads_page.uploads_table()),
        ],
    )
    def test_the_table_contains_an_alert_dialog(self, page, builder):
        tags = _tags(builder())
        assert any("AlertDialog" in t for t in tags), (
            f"/{page} has no alert dialog in its row actions — Delete is still "
            "wired straight to the handler, and WORKFLOW_RULES §7b still "
            "describes a control that does not exist"
        )

    @pytest.mark.parametrize(
        ("page", "builder"),
        [
            ("connections", lambda: connections_page.connections_table()),
            ("uploads", lambda: uploads_page.uploads_table()),
        ],
    )
    def test_it_has_a_cancel_and_an_action(self, page, builder):
        tags = _tags(builder())
        assert any("Cancel" in t for t in tags), f"/{page}: no way to back out"
        assert any("Action" in t for t in tags), f"/{page}: no confirm control"


class TestTheDestructiveHandlerHangsOffTheConfirmButton:
    """The load-bearing half: *where* the handler is attached.

    A dialog whose trigger still deletes is a dialog that reports the deletion
    after the fact. Read from source, because the handler binding is what the
    generated JS wires up and it is not visible in the component tag names.

    core#851 turned this from a two-entry parametrize list into a sweep of
    ``datanika/ui/``. The list could only ever be as complete as the memory of
    whoever last edited it.
    """

    def test_the_scan_is_not_blind(self):
        """A derived guard that derives nothing passes everything.

        Anchors first, count second — the count is the weaker check and would
        otherwise mask which site went missing.
        """
        sites = _destructive_call_sites()
        # One per site core#851 enumerated, so a change to
        # DESTRUCTIVE_PREFIXES or to the owner-name heuristic cannot silently
        # narrow the sweep to the subset that happens to still match.
        for expected in (
            "pages/connections.py::delete_connection",
            "pages/uploads.py::delete_upload",
            "pages/settings.py::remove_member",
            "components/api_key_row.py::revoke_api_key",
            "pages/settings.py::delete_channel",
            "pages/pipelines.py::delete_pipeline",
            "pages/schedules.py::delete_schedule",
            "pages/transformations.py::delete_transformation",
            "pages/dag.py::remove_dependency",
            "pages/pipelines.py::remove_model",
            "pages/model_detail.py::remove_column_test",
            # 🚨 The three the pre-core#851 sweep could not see, anchored BY NAME
            # rather than by a raised count — a count rises for the wrong reason.
            # `cancel_invitation` was hidden by the predicate (spelled `cancel_`);
            # `leave_org` and `delete_account` by the matcher (no arguments, so
            # `ast.Attribute` and never an `ast.Call`). `delete_account` is the
            # one that matters most: account erasure with no grace period, and
            # this guard was green about it while unable to read it.
            "pages/settings.py::cancel_invitation",
            "pages/settings.py::leave_org",
            "pages/settings.py::delete_account",
        ):
            assert expected in sites, f"the sweep no longer sees {expected}"
        # Eleven, not the twelve core#851 counted: `revoke_api_key` was rendered
        # on two surfaces from two copies of the same row markup, and core#851
        # collapsed them into `components/api_key_row.py`. Two call sites became
        # one because there is now one dialog, not because one stopped being
        # watched — `/settings` still renders it, through the component.
        assert len(sites) >= 14, (
            f"only {len(sites)} destructive call sites found across {UI_ROOT}. "
            "A sweep that suddenly finds fewer has probably stopped matching, "
            "not been fixed."
        )

    def test_the_matcher_sees_both_reference_shapes(self):
        """Gap 2, asserted directly rather than inferred from a count.

        A handler taking a row id is an ``ast.Call``; one taking no arguments is
        an ``ast.Attribute``. If either arm regresses, the count above can still
        be satisfied by the other shape alone.
        """
        sites = _destructive_call_sites()
        assert "pages/connections.py::delete_connection" in sites, "the ast.Call arm is blind"
        assert "pages/settings.py::leave_org" in sites, "the ast.Attribute arm is blind"

    def test_a_call_site_is_counted_once(self):
        """Both arms visiting the same node would double-count every call site,
        which inflates the count above into a check that cannot fail."""
        contexts = _destructive_call_sites()["pages/connections.py::delete_connection"]
        assert len(contexts) == 1, contexts

    def test_every_destructive_call_is_behind_a_confirmation(self):
        offenders: list[str] = []
        for site, contexts in sorted(_destructive_call_sites().items()):
            if site in UNCONFIRMED_BY_DESIGN:
                continue
            for ctx in contexts:
                if "alert_dialog.trigger" in ctx:
                    offenders.append(f"{site} hangs off the dialog *trigger*, not its action")
                elif "alert_dialog.action" in ctx:
                    continue
                elif site in FORM_CONFIRMED:
                    # The typed-confirmation shape: the handler is on a form's
                    # `on_submit` and that form is inside `alert_dialog.content`.
                    # Both halves are required — a form outside the dialog is
                    # exactly the one-click control this module exists to stop.
                    if "alert_dialog.content" not in ctx or "rx.form" not in ctx:
                        offenders.append(
                            f"{site} is in FORM_CONFIRMED but its form is not inside "
                            f"alert_dialog.content — invoked from `{ctx or '<top level>'}`"
                        )
                else:
                    offenders.append(f"{site} is invoked from `{ctx or '<top level>'}`")
        assert not offenders, (
            "destructive controls that mutate on the first click (core#851):\n  "
            + "\n  ".join(offenders)
            + "\n\nEach must hang off rx.alert_dialog.action, or carry a typed "
            "confirmation declared in FORM_CONFIRMED, or be added to "
            "UNCONFIRMED_BY_DESIGN with an argument."
        )

    @pytest.mark.parametrize("site", sorted(FORM_CONFIRMED))
    def test_each_form_confirmed_site_still_exists(self, site):
        assert site in _destructive_call_sites(), (
            f"{site} claims a typed confirmation but the sweep no longer sees it."
        )

    def test_every_census_disagreement_is_declared(self):
        """core#851's own proposal: assert the disagreement, do not silently union it.

        A handler in the audit census but not the verb one means the verb list
        missed a real deletion. A handler in the verb census that persists and
        writes no ``delete`` audit row means the *audit* is missing — which is
        how [core#934] was found. Unioning the two hides the second class.
        """
        verbs = {
            h
            for h in (s.split("::", 1)[1] for s in _destructive_call_sites())
            if h.startswith(DESTRUCTIVE_PREFIXES)
        }
        audited = {
            h
            for h in (s.split("::", 1)[1] for s in _destructive_call_sites())
            if h in AUDITED_DELETE
        }
        undeclared = sorted((verbs ^ audited) - set(CENSUS_DISAGREEMENT))
        assert not undeclared, (
            f"{undeclared} are seen by one census derivation and not the other, with no "
            "entry in CENSUS_DISAGREEMENT. Decide which it is:\n"
            "  audit-only -> the verb list missed a real deletion (add it, or accept it)\n"
            "  verb-only  -> it persists and writes no audit row, which is a defect worth "
            "filing, or it is form-local and belongs in UNCONFIRMED_BY_DESIGN."
        )

    @pytest.mark.parametrize("handler", sorted(CENSUS_DISAGREEMENT))
    def test_each_declared_disagreement_still_disagrees(self, handler):
        """The other direction — a declaration that has stopped being true.

        If someone adds the missing ``_audit`` call to ``remove_dependency``
        (core#934), the two derivations agree about it and this entry must go,
        rather than sitting here asserting something about nothing.
        """
        sites = {s.split("::", 1)[1] for s in _destructive_call_sites()}
        assert handler in sites, f"{handler} is declared but the census no longer sees it"
        by_verb = handler.startswith(DESTRUCTIVE_PREFIXES)
        by_audit = handler in AUDITED_DELETE
        assert by_verb != by_audit, (
            f"{handler} is now found by both derivations (verb={by_verb}, audit={by_audit}) "
            "— they agree, so remove its CENSUS_DISAGREEMENT entry."
        )

    @pytest.mark.parametrize("site", sorted(UNCONFIRMED_BY_DESIGN))
    def test_each_exclusion_still_refers_to_a_real_call_site(self, site):
        """A stale exclusion is a hole with a reassuring comment over it."""
        assert site in _destructive_call_sites(), (
            f"{site} is excluded from the confirmation requirement but no longer "
            "exists. Delete the entry rather than leaving it to cover something else."
        )

    @pytest.mark.parametrize("site", sorted(UNCONFIRMED_BY_DESIGN))
    def test_each_exclusion_is_earned_by_the_handler_not_by_the_comment(self, site):
        """The stated reason is 'form-local'. Check that it still is."""
        handler = site.split("::", 1)[1]
        module, _node, source = _state_handler(handler)
        assert not _persists(source), (
            f"{site} is excluded from the confirmation requirement on the grounds "
            f"that it changes nothing persistent, but {module}::{handler} now "
            "reaches the database. The exclusion has expired — give it a dialog."
        )


class TestTheDialogIsAClaimAndTheHandlerIsTheRefusal:
    """core#851 — a confirmation the client renders is not authorization.

    Every persisted destructive handler must refuse on its own, because the
    dialog exists only in the browser and the API path never sees it.

    ⚠️ **Written red.** ``DagState.remove_dependency`` was the single persisted
    destructive handler with no ``_check_role`` call, so before core#851 this
    failed naming exactly that handler. It was invisible to
    ``test_rbac_enforcement.py`` because that module's ``EXPECTED_ROLES`` is a
    hand-written allowlist with no ``dag_state`` key — the guard walked the
    entries someone remembered rather than the handlers that exist.
    """

    def test_every_persisted_destructive_handler_checks_a_role(self):
        seen: set[str] = set()
        unguarded: list[str] = []
        for site in sorted(_destructive_call_sites()):
            handler = site.split("::", 1)[1]
            if handler in seen:
                continue
            seen.add(handler)
            module, node, source = _state_handler(handler)
            if not _persists(source):
                continue
            calls = _self_calls(node)
            required = ALTERNATIVE_REFUSAL.get(handler, ("_check_role", ""))[0]
            if required not in calls:
                unguarded.append(f"{module}::{handler} (expected {required})")
        assert not unguarded, (
            "persisted destructive handlers reachable with no server-side refusal — "
            "the confirmation dialog is a claim the client makes, this is the "
            "refusal:\n  " + "\n  ".join(unguarded)
        )

    @pytest.mark.parametrize("handler", sorted(ALTERNATIVE_REFUSAL))
    def test_each_alternative_refusal_names_a_handler_that_still_exists(self, handler):
        """An exemption from the role check is a claim, checked the same way
        ``UNCONFIRMED_BY_DESIGN`` is: against the handler, not the comment."""
        sites = {s.split("::", 1)[1] for s in _destructive_call_sites()}
        assert handler in sites, (
            f"{handler} is exempted from the role check but the census no longer "
            "sees it. Delete the entry rather than leaving it to cover something else."
        )
        _module, node, _source = _state_handler(handler)
        assert "_check_role" not in _self_calls(node), (
            f"{handler} now calls _check_role after all — drop its "
            "ALTERNATIVE_REFUSAL entry so the ordinary rule applies."
        )

    def test_the_check_is_the_first_thing_the_handler_does(self):
        """A role check after the mutation is a log line, not a gate."""
        seen: set[str] = set()
        late: list[str] = []
        for site in sorted(_destructive_call_sites()):
            handler = site.split("::", 1)[1]
            if handler in seen:
                continue
            seen.add(handler)
            module, node, source = _state_handler(handler)
            if not _persists(source):
                continue
            required = ALTERNATIVE_REFUSAL.get(handler, ("_check_role", ""))[0]
            # Skip the docstring; ast.Expr is what a bare string statement is.
            # Also skip a leading `self.<var> = <constant>` — clearing a stale
            # error banner before deciding is not "work", it cannot reach the
            # database, and `delete_account` legitimately opens with
            # `self.delete_error = ""`. Anything else — a service call, a
            # session, reading the form payload — must come *after* the refusal.
            body = [s for s in node.body if not isinstance(s, ast.Expr)]
            while body and _is_state_reset(body[0]):
                body = body[1:]
            if not body or required not in ast.unparse(body[0]):
                late.append(f"{module}::{handler} (expected {required} first)")
        assert not late, (
            "the server-side refusal must precede any work, not follow it:\n  " + "\n  ".join(late)
        )


#: What each dialog must name, per site. Not every row model has a ``name`` —
#: a schedule is identified by what it triggers, a dependency by both its ends —
#: so the pair is declared rather than assumed. ``#7`` on its own identifies
#: nothing to a human, which is the bare-identifier defect core#805 was about.
IDENTIFIER_FIELDS: dict[str, tuple[str, ...]] = {
    "pages/connections.py::delete_connection": ("conn.id", "conn.name"),
    "pages/uploads.py::delete_upload": ("u.id", "u.name"),
    "pages/settings.py::remove_member": ("member.id", "member.email"),
    "components/api_key_row.py::revoke_api_key": ("key.id", "key.name"),
    "pages/settings.py::delete_channel": ("ch.id", "ch.name"),
    "pages/pipelines.py::delete_pipeline": ("p.id", "p.name"),
    "pages/schedules.py::delete_schedule": ("s.id", "s.target_name"),
    "pages/transformations.py::delete_transformation": ("t.id", "t.name"),
    "pages/dag.py::remove_dependency": ("d.id", "d.upstream_name", "d.downstream_name"),
    "pages/settings.py::cancel_invitation": ("inv.id", "inv.email"),
    # No row id: the subject is the organization itself, and the fields the
    # dialog must name are what identifies it plus what the click will actually
    # do. `leaving_signs_me_out` is the branch var, so requiring it here means a
    # dialog cannot silently stop disclosing the outcome (SPEC_MUTATION_FEEDBACK
    # D7a, criterion 10).
    "pages/settings.py::leave_org": (
        "SettingsState.org_name",
        "SettingsState.leaving_signs_me_out",
    ),
    # ⚠️ `transfer_ownership` is deliberately absent. It gained a dialog in the
    # same change (SPEC_MUTATION_FEEDBACK D7b) but it is **not a deletion** —
    # it audits as `transfer_ownership`, not `delete`, and no verb matches it —
    # so it is correctly outside this census. Its own assertions are in
    # TestTheOwnershipTransferDialog below; putting it here would make the
    # equality check demand a census membership it should not have.
    #
    # A typed confirmation, so what it must name is the destructive fact rather
    # than a row: `has_password` selects which text the user has to type.
    "pages/settings.py::delete_account": ("AccountState.has_password",),
}


def _references(segment: str, handler: str) -> bool:
    """Does this source segment wire up ``handler``, in **either** shape?

    ⚠️ The obvious test — ``f".{handler}(" in segment`` — is the same call-shape
    assumption that made ``_DestructiveCallVisitor`` blind, one layer down. It
    can never match ``on_click=SettingsState.leave_org``, so the three checks
    below it would silently report on a *different* function, or raise.
    """
    return re.search(rf"State\.{re.escape(handler)}\b", segment) is not None


def _dialog_body(site: str) -> str:
    """Source between ``alert_dialog.content`` and ``alert_dialog.action``.

    Scoped to the *enclosing function*, not the file: ``settings.py`` holds two
    dialogs, and a file-wide ``str.index`` would check the first one twice and
    the second never — a guard that reports on a control it did not read.
    """
    rel, handler = site.split("::", 1)
    source = (UI_ROOT / rel).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        segment = ast.get_source_segment(source, node) or ""
        if not _references(segment, handler):
            continue
        start = segment.index("alert_dialog.content")
        # A FORM_CONFIRMED dialog has no `alert_dialog.action` at all — its
        # confirm control is the form's submit button. Ending at the action
        # would raise ValueError there, so the body runs to the end of the
        # dialog instead. `.index` is kept for the common case deliberately: a
        # dialog that *should* have an action and does not must fail loudly.
        if site in FORM_CONFIRMED:
            return segment[start:]
        end = segment.index("alert_dialog.action", start)
        return segment[start:end]
    raise AssertionError(f"no function in {rel} contains a {handler} call")


class TestTheDialogNamesWhatItWillDelete:
    """core#804 AC2, widened by core#851 — an id *and* something recognisable.

    The id is what an automated caller aims by; the label is what a human
    recognises. Matching on both is also the selector rule §7b now recommends,
    and it is what makes a mis-aimed delete impossible rather than merely
    recoverable — the distinction that mattered when prod connections 13–17
    went, because that loop was confirming deletions it had already aimed wrong.
    """

    def test_every_confirmed_site_declares_its_identifiers(self):
        confirmed = {s for s in _destructive_call_sites() if s not in UNCONFIRMED_BY_DESIGN}
        assert confirmed == set(IDENTIFIER_FIELDS), (
            "IDENTIFIER_FIELDS has drifted from the sweep.\n"
            f"  undeclared: {sorted(confirmed - set(IDENTIFIER_FIELDS))}\n"
            f"  stale:      {sorted(set(IDENTIFIER_FIELDS) - confirmed)}"
        )

    @pytest.mark.parametrize("site", sorted(IDENTIFIER_FIELDS))
    def test_the_identifiers_appear_in_the_dialog_body(self, site):
        body = _dialog_body(site)
        for field in IDENTIFIER_FIELDS[site]:
            assert field in body, f"{site}: dialog body never mentions {field}"

    @pytest.mark.parametrize("site", sorted(IDENTIFIER_FIELDS))
    def test_the_dialog_offers_a_way_out(self, site):
        rel, handler = site.split("::", 1)
        source = (UI_ROOT / rel).read_text(encoding="utf-8")
        tree = ast.parse(source)
        segment = next(
            seg
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and _references(seg := ast.get_source_segment(source, node) or "", handler)
        )
        assert "alert_dialog.cancel" in segment, f"{site}: no way to back out"


class TestLeavingDisclosesWhichOutcomeItProduces:
    """SPEC_MUTATION_FEEDBACK D7a, criterion 10 — the substance of the fix.

    ``leave_org`` ends in ``switch_org`` **or** ``logout``, and one Leave button
    produces both. A dialog that only asks *"are you sure?"* leaves the more
    serious outcome undisclosed, which is what this whole class of defect is
    about. So the dialog is required to render one branch or the other, and the
    branch has to come from the same list the handler re-derives from.

    ⚠️ These are structural assertions, not screenshots — the point is that both
    strings are *reachable* and mutually exclusive, which a render of one state
    cannot show.
    """

    SITE = "pages/settings.py::leave_org"

    def test_both_outcomes_have_copy(self):
        en = json.loads((Path(datanika.i18n.__file__).parent / "en.json").read_bytes())
        for key in ("settings.leave_org_signs_you_out", "settings.leave_org_switches_you"):
            assert key in en, key

    def test_the_dialog_renders_exactly_one_of_them(self):
        body = _dialog_body(self.SITE)
        assert "settings.leave_org_signs_you_out" in body
        assert "settings.leave_org_switches_you" in body
        # Two branches of ONE `rx.cond`: "both" and "neither" are unreachable by
        # construction rather than by a second rule that could drift out of step.
        assert body.count("rx.cond(") >= 1, body
        assert "SettingsState.leaving_signs_me_out" in body

    def test_the_state_derives_the_branch_from_the_same_list_the_handler_uses(self):
        source = (STATE_ROOT / "settings_state.py").read_text(encoding="utf-8")
        assert "auth_state.user_orgs" in source, (
            "the dialog's branch must be derived from `user_orgs`, which is what "
            "`leave_org` itself re-derives membership from — a second rule for "
            "the same question is a second answer waiting to disagree"
        )

    def test_leaving_is_not_expected_to_toast(self):
        """The decision, made mechanical so nobody 'fixes' it later.

        Adding ``yield await self._saved_toast(...)`` makes ``leave_org`` an
        async generator, and ``return`` *with a value* is a ``SyntaxError``
        there — its two terminal statements are ``return auth_state.switch_org(…)``
        and ``return auth_state.logout()``. Yielding the events instead compiles
        and is still wrong: the event is a navigation, so the toast races a
        redirect. The dialog is the acknowledgement.
        """
        _module, node, _source = _state_handler("leave_org")
        returns_a_value = [
            n for n in ast.walk(node) if isinstance(n, ast.Return) and n.value is not None
        ]
        assert returns_a_value, (
            "leave_org no longer returns its terminal event. If it became a "
            "generator, SPEC_MUTATION_FEEDBACK D7a needs revisiting rather than "
            "this assertion being deleted."
        )
        assert "_saved_toast" not in _self_calls(node), (
            "leave_org must not toast — see SPEC_MUTATION_FEEDBACK D7a. If you "
            "are adding one, the handler cannot keep `return <event>`."
        )


class TestTheOwnershipTransferDialog:
    """SPEC_MUTATION_FEEDBACK D7b.

    ⚠️ **Deliberately outside the destructive census.** ``transfer_ownership``
    audits as ``transfer_ownership``, not ``delete``, and matches no destructive
    verb — so neither derivation sees it and neither should. It earns a
    confirmation for a different reason: it is the only route to
    ``MemberRole.OWNER``, it demotes the actor in the same transaction, and only
    the new owner can transfer it back. The undo lives in somebody else's hands.
    """

    def _segment(self) -> str:
        source = (UI_ROOT / "pages" / "settings.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                segment = ast.get_source_segment(source, node) or ""
                if node.name == "_transfer_ownership_dialog":
                    return segment
        raise AssertionError("no _transfer_ownership_dialog in settings.py")

    def test_the_handler_hangs_off_the_action(self):
        segment = self._segment()
        action = segment.index("alert_dialog.action")
        assert _references(segment[action:], "transfer_ownership"), (
            "the handler must be on the dialog's action, not its trigger"
        )
        trigger = segment.index("alert_dialog.trigger")
        assert not _references(segment[trigger:action], "transfer_ownership"), (
            "transfer_ownership fires from the dialog trigger — the dialog would "
            "be reporting the transfer after the fact"
        )

    def test_the_dialog_names_the_successor_and_the_org(self):
        segment = self._segment()
        for field in ("SettingsState.transfer_to_email", "SettingsState.org_name"):
            assert field in segment, f"the dialog never mentions {field}"

    def test_it_states_that_only_the_new_owner_can_undo_it(self):
        assert "settings.transfer_ownership_irreversible" in self._segment()

    def test_it_offers_a_way_out(self):
        assert "alert_dialog.cancel" in self._segment()


class TestTheDialogWarnsAboutDependents:
    """core#805 AC4 — deleting a connection with dependents warns first."""

    def test_the_connection_row_carries_a_dependent_count_and_names(self):
        item = ConnectionItem()
        assert hasattr(item, "dependent_count")
        assert hasattr(item, "dependent_names")

    def test_the_dialog_renders_them(self):
        source = (UI_ROOT / "pages" / "connections.py").read_text(encoding="utf-8")
        assert "dependent_count" in source
        assert "dependent_names" in source, (
            "a bare count says how many; the names say which, and the user needs "
            "the second to decide"
        )


class TestABlockedUploadIsHonest:
    """core#805 AC1–AC3 on the row model, which is what the page renders."""

    def test_the_row_knows_when_a_connection_is_gone(self):
        item = UploadItem()
        for field in (
            "source_connection_deleted",
            "destination_connection_deleted",
            "is_blocked",
        ):
            assert hasattr(item, field), field

    def test_a_blocked_row_does_not_read_active(self):
        source = (UI_ROOT / "pages" / "uploads.py").read_text(encoding="utf-8")
        assert 'rx.cond(u.is_blocked, _t["uploads.status_blocked"], u.status)' in source, (
            "the status badge must not print `active` for an upload whose "
            "connection was deleted — it cannot run"
        )

    def test_the_run_control_is_disabled_when_blocked(self):
        source = (UI_ROOT / "pages" / "uploads.py").read_text(encoding="utf-8")
        run_control = source[source.index("def _run_control") : source.index("def uploads_table")]
        assert "disabled=True" in run_control
        assert "uploads.run_blocked_reason" in run_control, (
            "a disabled control with no reason is only marginally better than a live one that fails"
        )

    def test_the_reason_is_not_delivered_by_tooltip(self):
        # A disabled button takes no pointer events, so a tooltip on it never
        # fires: the explanation would exist and be unreachable.
        source = (UI_ROOT / "pages" / "uploads.py").read_text(encoding="utf-8")
        run_control = source[source.index("def _run_control") : source.index("def uploads_table")]
        assert "rx.tooltip" not in run_control


class TestTheSourceNameSurvivesDeletion:
    """core#805 AC1 — never a bare ``#id``."""

    def test_the_page_renders_a_name_plus_a_deleted_marker(self):
        source = (UI_ROOT / "pages" / "uploads.py").read_text(encoding="utf-8")
        assert "_connection_cell(" in source
        assert '_t["common.deleted"]' in source

    def test_the_service_can_be_asked_for_retired_connections(self):
        import inspect

        from datanika.services.connection_service import ConnectionService

        sig = inspect.signature(ConnectionService.list_connections)
        assert "include_deleted" in sig.parameters
        assert sig.parameters["include_deleted"].default is False, (
            "must be opt-in: a caller that resolves a connection in order to RUN "
            "something has to keep getting the filtered list"
        )

    def test_get_connection_still_refuses_a_deleted_connection(self):
        # The whole point of `include_deleted` is that it is display-only. If
        # this ever loosens, a deleted connection becomes runnable again.
        from datanika.services.connection_service import get_org_connection

        source = ast.unparse(
            ast.parse(Path(get_org_connection.__code__.co_filename).read_text(encoding="utf-8"))
        )
        assert "Connection.deleted_at.is_(None)" in source


class TestTheNewStringsAreTranslated:
    KEYS = (
        "common.deleted",
        "connections.delete_title",
        "connections.delete_body",
        "connections.delete_dependents",
        "connections.delete_reversible",
        "connections.delete_confirm",
        "connections.deleted_toast",
        "uploads.delete_title",
        "uploads.delete_body",
        "uploads.delete_confirm",
        "uploads.deleted_toast",
        "uploads.status_blocked",
        "uploads.run_blocked_reason",
        # core#851 — the remaining eight controls.
        "settings.remove_member_title",
        "settings.remove_member_body",
        "settings.remove_member_reversible",
        "settings.remove_member_confirm",
        "settings.member_removed_toast",
        "api_keys.revoke_title",
        "api_keys.revoke_body",
        "api_keys.revoke_irreversible",
        "api_keys.revoke_confirm",
        "api_keys.revoked_toast",
        "notifications.delete_title",
        "notifications.delete_body",
        "notifications.delete_secret",
        "notifications.delete_confirm",
        "notifications.deleted_toast",
        "pipelines.delete_title",
        "pipelines.delete_body",
        "pipelines.delete_reversible",
        "pipelines.delete_confirm",
        "pipelines.deleted_toast",
        "schedules.delete_title",
        "schedules.delete_body",
        "schedules.delete_reversible",
        "schedules.delete_confirm",
        "schedules.deleted_toast",
        "transformations.delete_title",
        "transformations.delete_body",
        "transformations.delete_reversible",
        "transformations.delete_confirm",
        "transformations.deleted_toast",
        "dag.delete_title",
        "dag.delete_body",
        "dag.delete_reversible",
        "dag.delete_confirm",
        "dag.deleted_toast",
    )

    @staticmethod
    def _keys_rendered_by_the_dialogs() -> set[str]:
        """Every ``_t["..."]`` inside a function that opens a confirmation."""
        referenced: set[str] = set()
        for site in _destructive_call_sites():
            if site in UNCONFIRMED_BY_DESIGN:
                continue
            rel, handler = site.split("::", 1)
            source = (UI_ROOT / rel).read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if f".{handler}(" not in (ast.get_source_segment(source, node) or ""):
                    continue
                for sub in ast.walk(node):
                    if (
                        isinstance(sub, ast.Subscript)
                        and isinstance(sub.value, ast.Name)
                        and sub.value.id == "_t"
                        and isinstance(sub.slice, ast.Constant)
                    ):
                        referenced.add(sub.slice.value)
        return referenced

    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    def test_every_string_the_dialogs_render_exists_in_every_locale(self, locale):
        """The KEYS tuple above is hand-written, which is the same claim-shaped
        defect as a hand-written page list: it covers what someone remembered.

        So derive the strings from the dialogs themselves. This deliberately
        picks up pre-existing keys too — the trigger labels ``api_keys.revoke``
        and ``settings.remove`` — because what matters to the user is that
        every word on the dialog is in their language, not which release
        introduced it.

        🚨 **Reads the locale's own JSON, not ``get_translations``.** That
        helper returns *English merged with the locale's overrides* and
        documents itself as guaranteeing every English key is present — so
        ``key in get_translations(loc)`` is true for every key in ``en.json``
        and can never fail. The first version of this test used it and stayed
        green while a key was deleted from ``sr.json``; it was a checker with
        one possible answer, found by mutating the real locale file rather than
        by re-reading the test. The same vacuous line sits in
        ``test_every_key_is_present_and_not_left_in_english`` below and is
        harmless *there* only because its second assertion — value differs from
        English — is what actually catches a missing key, via the merge.
        """
        referenced = self._keys_rendered_by_the_dialogs()
        assert len(referenced) >= 30, (
            f"only {len(referenced)} dialog strings found — the extractor has "
            "probably stopped matching `_t[...]`"
        )
        raw = json.loads(
            (Path(datanika.i18n.__file__).parent / f"{locale}.json").read_text(encoding="utf-8")
        )
        for key in sorted(referenced):
            assert key in raw, (
                f"{locale}.json is missing {key}, rendered by a delete dialog. "
                "It will fall back to English for that user."
            )
            assert raw[key].strip(), f"{locale}: {key} is blank"

    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    def test_every_key_is_present_and_not_left_in_english(self, locale):
        t = get_translations(locale)
        en = get_translations("en")
        for key in self.KEYS:
            assert key in t, f"{locale} is missing {key}"
            assert t[key].strip(), f"{locale}: {key} is blank"
            if locale != "en":
                assert t[key] != en[key], (
                    f"{locale}: {key} is still the English string — an untranslated "
                    "value passes key-parity while showing English to that user"
                )


class TestTheDialogIsAModalAlert:
    """`role="alertdialog"`, not a popover — this is what §7b now depends on."""

    def test_reflex_alert_dialog_is_what_is_used(self):
        modules = {
            s.split("::", 1)[0] for s in _destructive_call_sites() if s not in UNCONFIRMED_BY_DESIGN
        }
        assert modules, "no confirmed destructive sites — the sweep found nothing"
        for rel in sorted(modules):
            source = (UI_ROOT / rel).read_text(encoding="utf-8")
            assert "rx.alert_dialog.root(" in source, (
                f"{rel} has a destructive control but no rx.alert_dialog.root — "
                "an rx.dialog is a plain dialog, not role='alertdialog', and §7b "
                "depends on the alert variant"
            )
        assert hasattr(rx, "alert_dialog")
