"""API Keys page — create and manage programmatic API keys."""

import reflex as rx

from datanika.ui.components.api_key_row import api_key_create_controls
from datanika.ui.components.api_key_row import api_key_row as _key_row
from datanika.ui.components.layout import page_layout
from datanika.ui.state.api_key_state import ApiKeyState
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def api_keys_page() -> rx.Component:
    """API keys, rendered for what the viewer may actually do.

    ``create_api_key`` and ``revoke_api_key`` both gate on
    ``_check_role("admin")``, so the gate here is ``can_administer`` and not
    ``can_edit``: an editor handed the create form gets a Create button that
    always refuses, which is core#658's defect. ``toggle_create`` carries no
    role check of its own — it only flips a client-side flag — which is why the
    gate wraps the whole block rather than the mutating button alone. Hiding
    Create while leaving New visible would replace a control that fails with a
    control that opens onto nothing. core#886.
    """
    return page_layout(
        rx.vstack(
            rx.cond(
                ApiKeyState.error_message != "",
                rx.callout(
                    ApiKeyState.error_message,
                    icon="triangle_alert",
                    color_scheme="red",
                    width="100%",
                ),
            ),
            # Create key section
            rx.card(
                rx.vstack(
                    rx.hstack(
                        rx.heading(_t["api_keys.title"], size="4"),
                        width="100%",
                        align="center",
                    ),
                    rx.cond(
                        AuthState.can_administer,
                        api_key_create_controls(),
                        rx.fragment(),
                    ),
                    rx.cond(
                        ApiKeyState.new_key_raw != "",
                        rx.callout(
                            rx.vstack(
                                rx.text(_t["api_keys.copy_warning"], weight="bold"),
                                rx.code(ApiKeyState.new_key_raw, size="2"),
                                spacing="2",
                            ),
                            icon="key",
                            color_scheme="green",
                            width="100%",
                        ),
                    ),
                    spacing="4",
                    width="100%",
                ),
                width="100%",
            ),
            # Keys table
            rx.cond(
                ApiKeyState.keys.length() > 0,
                rx.table.root(
                    rx.table.header(
                        rx.table.row(
                            rx.table.column_header_cell(_t["common.name"]),
                            rx.table.column_header_cell(_t["api_keys.scopes"]),
                            rx.table.column_header_cell(_t["api_keys.created"]),
                            rx.table.column_header_cell(_t["api_keys.last_used"]),
                            rx.table.column_header_cell(_t["api_keys.expires"]),
                            rx.table.column_header_cell(_t["common.actions"]),
                        ),
                    ),
                    rx.table.body(
                        rx.foreach(ApiKeyState.keys, _key_row),
                    ),
                    width="100%",
                ),
                rx.text(_t["api_keys.no_keys"], color="gray"),
            ),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.api_keys"],
    )
