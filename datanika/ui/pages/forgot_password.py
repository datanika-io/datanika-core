"""/forgot-password — request a reset link (core#623).

A **Reflex page**, not a backend Starlette route, and that is load-bearing
rather than a layout preference. The Apache vhost forwards an explicit list of
prefixes (``/_event``, ``/api/``, ``/mcp``, ``/ping``, ``/healthz``, ``/readyz``,
``/_upload``, the OAuth AS paths) to ``:8000`` and **everything else** to the
Reflex frontend on ``:3000``. A new backend route outside ``/api/`` therefore
serves the SPA instead of itself — the exact failure that hit ``/mcp`` and every
OAuth discovery document, each needing an Infra vhost change to fix. A page
needs no vhost change at all; its handlers reach the backend over ``/_event``,
which is already proxied.

The confirmation screen is identical whether or not the address has an account.
The dead end that usually creates is removed with copy, not disclosure: the
submitted address is echoed back (the user's own input, so it leaks nothing, and
it is what catches the typo that caused the problem), the 60-minute expiry is
stated so waiting is bounded, and /signup is one click away.
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


def _request_form() -> rx.Component:
    return rx.vstack(
        rx.text(_t["auth.forgot_password_intro"], size="2", color="gray"),
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
                    rx.text(_t["auth.email"], size="2", weight="medium"),
                    html_for="forgot-email",
                ),
                rx.input(
                    id="forgot-email",
                    placeholder=_t["auth.ph_email"],
                    name="email",
                    type="email",
                    width="100%",
                ),
                rx.button(
                    _t["auth.send_reset_link"],
                    type="submit",
                    width="100%",
                    size="3",
                ),
                spacing="3",
                width="100%",
            ),
            on_submit=PasswordResetState.request_reset,
            width="100%",
        ),
        spacing="3",
        width="100%",
    )


def _confirmation() -> rx.Component:
    """Screen 2. Byte-identical for a real account, an unknown address, and an
    address whose hourly bucket is spent."""
    return rx.vstack(
        rx.text(
            PasswordResetState.submitted_email,
            size="3",
            weight="bold",
            width="100%",
            word_break="break-all",
        ),
        rx.text(_t["auth.reset_link_sent_body"], size="2", color="gray"),
        rx.text(_t["auth.reset_link_sent_hint"], size="2", color="gray"),
        rx.text(
            _t["auth.no_account"],
            " ",
            rx.link(_t["auth.sign_up"], href="/signup"),
            size="2",
            color="gray",
        ),
        spacing="3",
        width="100%",
        align="start",
    )


def _unavailable() -> rx.Component:
    """D9: instance-level, so it discloses nothing about any account."""
    return rx.callout(
        _t["auth.reset_unavailable"],
        icon="info",
        color_scheme="gray",
        width="100%",
    )


def forgot_password_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            _brand(),
            rx.cond(
                PasswordResetState.submitted,
                rx.heading(_t["auth.reset_link_sent_heading"], size="5"),
                rx.heading(_t["auth.forgot_password_heading"], size="5"),
            ),
            rx.cond(
                PasswordResetState.unavailable,
                _unavailable(),
                rx.cond(
                    PasswordResetState.submitted,
                    _confirmation(),
                    _request_form(),
                ),
            ),
            rx.link(
                _t["auth.back_to_sign_in"],
                href="/login",
                on_click=PasswordResetState.reset_form,
                size="2",
            ),
            **_CARD,
        ),
        height="100vh",
    )
