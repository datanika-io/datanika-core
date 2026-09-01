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
   So the guard also requires every persisted destructive handler to call
   ``_check_role``. That assertion was **red on `DagState.remove_dependency`**
   when it was written — the one persisted destructive handler in the product
   with no role check at all, which no existing test could see because
   ``test_rbac_enforcement.py``'s ``EXPECTED_ROLES`` had no ``dag_state`` entry.
   A guard whose first run is green has not been shown able to fail.
"""

import ast
from pathlib import Path

import pytest
import reflex as rx

import datanika.ui
from datanika.i18n import SUPPORTED_LOCALES, get_translations
from datanika.ui.pages import connections as connections_page
from datanika.ui.pages import uploads as uploads_page
from datanika.ui.state.connection_state import ConnectionItem
from datanika.ui.state.upload_state import UploadItem

UI_ROOT = Path(datanika.ui.__file__).parent
STATE_ROOT = UI_ROOT / "state"

#: Handler-name prefixes that denote "this takes something away".
DESTRUCTIVE_PREFIXES = ("delete_", "remove_", "revoke_", "purge_")

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
    """Collect ``<X>State.<destructive>(...)`` calls with their enclosing calls."""

    def __init__(self) -> None:
        self.stack: list[str] = []
        self.found: list[tuple[str, str]] = []  # (handler, enclosing-call chain)

    def visit_Call(self, node: ast.Call) -> None:
        name = ast.unparse(node.func)
        if isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            owner = ast.unparse(node.func.value)
            if attr.startswith(DESTRUCTIVE_PREFIXES) and owner.endswith("State"):
                self.found.append((attr, " < ".join(reversed(self.stack))))
        self.stack.append(name)
        self.generic_visit(node)
        self.stack.pop()


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
        ):
            assert expected in sites, f"the sweep no longer sees {expected}"
        # Eleven, not the twelve core#851 counted: `revoke_api_key` was rendered
        # on two surfaces from two copies of the same row markup, and core#851
        # collapsed them into `components/api_key_row.py`. Two call sites became
        # one because there is now one dialog, not because one stopped being
        # watched — `/settings` still renders it, through the component.
        assert len(sites) >= 11, (
            f"only {len(sites)} destructive call sites found across {UI_ROOT}. "
            "A sweep that suddenly finds fewer has probably stopped matching, "
            "not been fixed."
        )

    def test_every_destructive_call_is_behind_a_confirmation(self):
        offenders: list[str] = []
        for site, contexts in sorted(_destructive_call_sites().items()):
            if site in UNCONFIRMED_BY_DESIGN:
                continue
            for ctx in contexts:
                if "alert_dialog.action" not in ctx:
                    offenders.append(f"{site} is invoked from `{ctx or '<top level>'}`")
                elif "alert_dialog.trigger" in ctx:
                    offenders.append(f"{site} hangs off the dialog *trigger*, not its action")
        assert not offenders, (
            "destructive controls that mutate on the first click (core#851):\n  "
            + "\n  ".join(offenders)
            + "\n\nEach must hang off rx.alert_dialog.action, or be added to "
            "UNCONFIRMED_BY_DESIGN with an argument."
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
            module, _node, source = _state_handler(handler)
            if not _persists(source):
                continue
            if "_check_role" not in source:
                unguarded.append(f"{module}::{handler}")
        assert not unguarded, (
            "persisted destructive handlers reachable with no role check — the "
            "confirmation dialog is a claim the client makes, this is the "
            "refusal:\n  " + "\n  ".join(unguarded)
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
            # Skip the docstring; ast.Expr is what a bare string statement is.
            body = [s for s in node.body if not isinstance(s, ast.Expr)]
            if "_check_role" not in ast.unparse(body[0]):
                late.append(f"{module}::{handler}")
        assert not late, "the role check must precede any work, not follow it:\n  " + "\n  ".join(
            late
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
}


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
        if f".{handler}(" not in segment:
            continue
        start = segment.index("alert_dialog.content")
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
            and f".{handler}(" in (seg := ast.get_source_segment(source, node) or "")
        )
        assert "alert_dialog.cancel" in segment, f"{site}: no way to back out"


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
        """
        referenced = self._keys_rendered_by_the_dialogs()
        assert len(referenced) >= 30, (
            f"only {len(referenced)} dialog strings found — the extractor has "
            "probably stopped matching `_t[...]`"
        )
        t = get_translations(locale)
        for key in sorted(referenced):
            assert key in t, f"{locale} is missing {key}, rendered by a delete dialog"
            assert t[key].strip(), f"{locale}: {key} is blank"

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
