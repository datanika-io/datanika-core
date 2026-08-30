"""/reset-password?token=… — choose a new password (core#623).

A Reflex page for the same reason as ``/forgot-password`` (see that module), and
for one more: the page has to render a **form**, which the two existing email
routes — ``/api/verify-email`` and ``/api/accept-invite``, both of which act and
redirect — cannot do.

The page load only *validates*; it never consumes the token. Corporate mail
security fetches every URL in an inbound message before the recipient sees it,
so a consuming GET means the scanner burns the token and the user's own click
always lands on "already used".

There is exactly one failure screen. Expired, already used, superseded by a
newer request, and never existed all render the same words — distinguishing them
tells an attacker which tokens were real. And nothing on this page links
off-site: while the token is still in the address bar it would ride out on the
``Referer``.
"""

import reflex as rx

from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.password_reset_state import PasswordResetState

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
        rx.image(src="/logo.png", width="48px", height="48px"),
        rx.heading(_t["app.name"], size="7"),
        spacing="3",
        align="center",
    )


def _form() -> rx.Component:
    return rx.vstack(
        rx.cond(
            PasswordResetState.error != "",
            rx.callout(
                PasswordResetState.error,
                icon="triangle_alert",
                color_scheme="red",
                width="100%",
            ),
        ),
        rx.form(
            rx.vstack(
                rx.el.label(
                    rx.text(_t["auth.new_password"], size="2", weight="medium"),
                    html_for="reset-password",
                ),
                rx.input(
                    id="reset-password",
                    name="password",
                    type="password",
                    custom_attrs={"autoComplete": "new-password"},
                    width="100%",
                ),
                rx.el.label(
                    rx.text(_t["auth.confirm_password"], size="2", weight="medium"),
                    html_for="reset-confirm",
                ),
                rx.input(
                    id="reset-confirm",
                    name="confirm",
                    type="password",
                    custom_attrs={"autoComplete": "new-password"},
                    width="100%",
                ),
                rx.text(_t["account.password_rules"], size="1", color="gray"),
                rx.button(
                    _t["auth.set_password"],
                    type="submit",
                    width="100%",
                    size="3",
                ),
                spacing="3",
                width="100%",
            ),
            on_submit=PasswordResetState.submit_new_password,
            width="100%",
        ),
        spacing="3",
        width="100%",
    )


def _invalid() -> rx.Component:
    return rx.vstack(
        rx.text(_t["auth.reset_link_invalid_body"], size="2", color="gray"),
        rx.link(
            _t["auth.request_new_link"],
            href="/forgot-password",
            on_click=PasswordResetState.clear_error,
            size="2",
        ),
        spacing="3",
        width="100%",
        align="start",
    )


def reset_password_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            _brand(),
            rx.cond(
                PasswordResetState.token_valid,
                rx.heading(_t["auth.reset_password_heading"], size="5"),
                rx.heading(_t["auth.reset_link_invalid_heading"], size="5"),
            ),
            rx.cond(
                PasswordResetState.token_valid,
                _form(),
                _invalid(),
            ),
            rx.link(_t["auth.back_to_sign_in"], href="/login", size="2"),
            **_CARD,
        ),
        height="100vh",
    )
