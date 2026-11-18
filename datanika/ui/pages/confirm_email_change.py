"""/confirm-email-change?token=… — apply a pending address (SPEC_EMAIL_CHANGE).

A Reflex page, not a backend route. Apache forwards an explicit prefix list to
the backend and everything else to the SPA, so a new route outside ``/api/``
would serve HTML. A GET that wrote ``users.email`` would also let a mail
scanner apply the change before the recipient clicked.

The page load validates and does not consume. The button does.
"""

import reflex as rx

from datanika.ui.state.email_change_state import EmailChangeState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

_CARD = {
    "spacing": "4",
    "width": "360px",
    "padding": "32px",
    "border": "1px solid var(--gray-a5)",
    "border_radius": "12px",
    "bg": "var(--color-background)",
}


def _brand() -> rx.Component:
    return rx.hstack(
        rx.image(src="/logo.png", alt="Datanika", width="48px", height="48px"),
        rx.heading(_t["app.name"], size="7"),
        spacing="3",
        align="center",
    )


def _confirm() -> rx.Component:
    return rx.vstack(
        rx.heading(_t["email_change.title"], size="5"),
        rx.text(_t["email_change.body"], size="2", color="var(--gray-11)"),
        rx.button(
            _t["email_change.confirm_button"],
            on_click=EmailChangeState.confirm,
            width="100%",
            size="3",
        ),
        spacing="3",
        width="100%",
        align="start",
    )


def confirm_email_change_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            _brand(),
            rx.cond(
                EmailChangeState.confirmed,
                rx.text(_t["email_change.done"], size="2"),
                rx.cond(
                    EmailChangeState.token_valid,
                    _confirm(),
                    rx.text(_t["email_change.invalid"], size="2", color="var(--gray-11)"),
                ),
            ),
            rx.link(_t["auth.sign_in"], href="/login", size="2"),
            **_CARD,
        ),
        height="100vh",
    )
