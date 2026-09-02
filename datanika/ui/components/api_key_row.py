"""One API-key table row, shared by ``/api-keys`` and the Settings card (core#851).

Revoke is rendered on **two** surfaces — ``pages/api_keys.py`` and the API keys
card inside ``pages/settings.py`` — driven by the same
:meth:`ApiKeyState.revoke_api_key` handler. Before core#851 the row markup was
duplicated verbatim in both files.

Duplicating a *confirmation dialog* is a different proposition from duplicating a
table row: the two copies would carry the same warning about the same
irreversible-to-the-user action, and the first time one of them is reworded the
product starts telling two different stories about the same button. core#862
states the general form for lookup tables — *a second hardcoded copy is the same
defect one generation later* — and it applies to copy as much as to data.

So the row lives here once, and both pages import it. The confirmation guard in
``tests/test_ui/test_delete_confirmation_and_blocked_uploads.py`` derives its
call sites from all of ``datanika/ui/`` rather than from ``datanika/ui/pages/``
precisely so that moving a destructive control into a component does not move it
out of the guard's view — which is the same lexical-blindness trap that
``_GateChecker`` has for role gates.
"""

import reflex as rx

from datanika.ui.state.api_key_state import ApiKeyItem, ApiKeyState
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def api_key_create_controls() -> rx.Component:
    """The New / name / Create block, rendered on both API-key surfaces.

    Moved here from ``pages/api_keys.py`` and ``pages/settings.py`` — where it
    was duplicated verbatim — by core#886, for the same reason the row moved:
    the gate it now carries has to be true in both places or in neither.

    ⚠️ The caller supplies the gate (``rx.cond(AuthState.can_administer, …)``),
    not this function. Both call sites are inside a card that also renders an
    ungated keys table, so the gate has to select *between* subtrees at the
    card level; a helper that gated itself would still leave the caller free to
    render it unconditionally, which is the mistake this module exists to make
    impossible. The derived guard in
    ``tests/test_ui/test_rbac_ui_visibility.py`` resolves gates through call
    sites, so it checks the callers rather than trusting this docstring.
    """
    return rx.vstack(
        rx.hstack(
            rx.spacer(),
            rx.button(_t["api_keys.new"], on_click=ApiKeyState.toggle_create, size="2"),
            width="100%",
            align="center",
        ),
        rx.cond(
            ApiKeyState.show_create,
            rx.vstack(
                rx.separator(),
                # A neighbouring rx.text() is not a label (core#720). The two
                # copies of this input were each unlabelled and each counted
                # separately in the accessibility ratchet; consolidating them
                # made it one input, so it is labelled here rather than moved
                # from one allowance to another.
                rx.el.label(
                    rx.text(_t["common.name"], size="2", weight="medium"),
                    html_for="api-key-name",
                ),
                rx.input(
                    id="api-key-name",
                    placeholder="e.g. CI/CD deploy key",
                    value=ApiKeyState.new_key_name,
                    on_change=ApiKeyState.set_new_key_name,
                    width="100%",
                ),
                rx.button(
                    _t["api_keys.create"],
                    on_click=ApiKeyState.create_api_key,
                    size="2",
                ),
                spacing="3",
                width="100%",
            ),
        ),
        spacing="4",
        width="100%",
    )


def revoke_api_key_dialog(key: ApiKeyItem) -> rx.Component:
    """Ask before revoking, and say what breaks.

    ``revoke_api_key`` is a soft delete at the database level
    (``api_key_service.py`` sets ``deleted_at``), and core#851 nearly filed it as
    *"irreversible"* on that basis alone. The row is recoverable by an operator;
    the **key** is not recoverable by the user, because the plaintext was shown
    once at creation and is not stored. So the copy speaks to the consequence the
    user actually faces — every integration holding this key starts being
    refused — rather than to the storage mechanics behind it.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(
                _t["api_keys.revoke"],
                size="1",
                color_scheme="red",
                variant="ghost",
            ),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["api_keys.revoke_title"]),
            rx.alert_dialog.description(_t["api_keys.revoke_body"]),
            rx.vstack(
                # id *and* name: the id is what an automated caller aims by, the
                # name is what a human recognises (core#804 AC2).
                rx.card(
                    rx.text("#", key.id, "  ", key.name, size="2", weight="bold"),
                    rx.text(key.scopes, size="1", color="var(--gray-9)"),
                ),
                rx.text(_t["api_keys.revoke_irreversible"], size="1", color="var(--gray-9)"),
                spacing="3",
                width="100%",
                margin_top="12px",
            ),
            rx.flex(
                rx.alert_dialog.cancel(
                    rx.button(_t["common.cancel"], variant="soft", color_scheme="gray"),
                ),
                rx.alert_dialog.action(
                    rx.button(
                        _t["api_keys.revoke_confirm"],
                        color_scheme="red",
                        on_click=ApiKeyState.revoke_api_key(key.id),
                    ),
                ),
                spacing="3",
                justify="end",
                margin_top="16px",
            ),
            max_width="480px",
        ),
    )


def api_key_row(key: ApiKeyItem) -> rx.Component:
    """The table row itself. Identical on both surfaces, by construction.

    The role gate lives **here**, not at the two call sites, for the same reason
    the copy does: ``/api-keys`` and the Settings card render the same row, and
    a gate duplicated across two files is a gate that can be true in one place
    and absent in the other. ``revoke_api_key`` gates on
    ``_check_role("admin")``, so ``can_delete`` is the matching threshold — a
    viewer or editor sees the key's metadata (which they may read) and no
    Revoke button (which they may not press). core#886.
    """
    return rx.table.row(
        rx.table.cell(key.name),
        rx.table.cell(key.scopes),
        rx.table.cell(key.created_at),
        rx.table.cell(key.last_used_at),
        rx.table.cell(key.expires_at),
        rx.table.cell(
            rx.cond(AuthState.can_delete, revoke_api_key_dialog(key), rx.fragment()),
        ),
    )
