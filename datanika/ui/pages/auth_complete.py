"""OAuth completion page — processes tokens from OAuth callback redirect."""

import reflex as rx

from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def auth_complete_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.spinner(size="3"),
            rx.text(_t["auth.completing_sign_in"], size="3", color="gray"),
            spacing="4",
            align="center",
        ),
        height="100vh",
    )
