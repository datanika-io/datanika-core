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


def _walk(component) -> list:
    """Every component in a rendered tree, including both branches of a cond."""
    out = [component]
    for child in getattr(component, "children", []) or []:
        out.extend(_walk(child))
    return out


def _tags(component) -> list[str]:
    return [type(c).__name__ for c in _walk(component)]


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
    """

    @staticmethod
    def _delete_call_context(path: Path, handler: str) -> list[str]:
        """Names of the enclosing calls for each `State.<handler>(...)` reference."""
        tree = ast.parse(path.read_text(encoding="utf-8"))
        contexts: list[str] = []

        class Visitor(ast.NodeVisitor):
            def __init__(self):
                self.stack: list[str] = []

            def visit_Call(self, node):
                name = ast.unparse(node.func)
                if name.endswith(f".{handler}"):
                    contexts.append(" < ".join(reversed(self.stack)))
                self.stack.append(name)
                self.generic_visit(node)
                self.stack.pop()

        Visitor().visit(tree)
        return contexts

    @pytest.mark.parametrize(
        ("filename", "handler"),
        [
            ("connections.py", "delete_connection"),
            ("uploads.py", "delete_upload"),
        ],
    )
    def test_delete_is_reached_only_through_the_dialog_action(self, filename, handler):
        contexts = self._delete_call_context(UI_ROOT / "pages" / filename, handler)
        assert contexts, f"no {handler} call site found in {filename} — guard is blind"
        for ctx in contexts:
            assert "alert_dialog.action" in ctx, (
                f"{filename}: {handler} is invoked from `{ctx}`. It must hang off "
                "rx.alert_dialog.action, or the confirmation is decorative."
            )
            assert "alert_dialog.trigger" not in ctx, (
                f"{filename}: {handler} is on the dialog *trigger* — it would fire "
                "on opening the dialog, i.e. before the user confirms."
            )


class TestTheDialogNamesWhatItWillDelete:
    """core#804 AC2 — id *and* name.

    The id is what an automated caller aims by; the name is what a human
    recognises. Matching on both is also the selector rule §7b now recommends.
    """

    @pytest.mark.parametrize(
        ("filename", "fields"),
        [
            ("connections.py", ("conn.id", "conn.name")),
            ("uploads.py", ("u.id", "u.name")),
        ],
    )
    def test_both_identifiers_appear_in_the_dialog_body(self, filename, fields):
        source = (UI_ROOT / "pages" / filename).read_text(encoding="utf-8")
        start = source.index("alert_dialog.content")
        end = source.index("alert_dialog.action", start)
        body = source[start:end]
        for field in fields:
            assert field in body, f"{filename}: dialog body never mentions {field}"


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
    )

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
        for filename in ("connections.py", "uploads.py"):
            source = (UI_ROOT / "pages" / filename).read_text(encoding="utf-8")
            assert "rx.alert_dialog.root(" in source, filename
        assert hasattr(rx, "alert_dialog")
