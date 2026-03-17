"""API Keys page — create and manage programmatic API keys."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.api_key_state import ApiKeyItem, ApiKeyState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def _key_row(key: ApiKeyItem) -> rx.Component:
    return rx.table.row(
        rx.table.cell(key.name),
        rx.table.cell(key.scopes),
        rx.table.cell(key.created_at),
        rx.table.cell(key.last_used_at),
        rx.table.cell(key.expires_at),
        rx.table.cell(
            rx.button(
                _t["api_keys.revoke"],
                on_click=ApiKeyState.revoke_api_key(key.id),
                size="1",
                color_scheme="red",
                variant="ghost",
            ),
        ),
    )


def api_keys_page() -> rx.Component:
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
                        rx.spacer(),
                        rx.button(
                            _t["api_keys.new"],
                            on_click=ApiKeyState.toggle_create,
                            size="2",
                        ),
                        width="100%",
                        align="center",
                    ),
                    rx.cond(
                        ApiKeyState.show_create,
                        rx.vstack(
                            rx.separator(),
                            rx.text(_t["common.name"], size="2", weight="medium"),
                            rx.input(
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
