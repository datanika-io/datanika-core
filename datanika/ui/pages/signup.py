"""Signup page."""

import reflex as rx

from datanika.ui.components.captcha import captcha_script
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def signup_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.hstack(
                rx.image(src="/logo.png", width="48px", height="48px"),
                rx.heading(_t["app.name"], size="7"),
                spacing="3",
                align="center",
            ),
            rx.text(_t["auth.create_account_heading"], size="3", color="gray"),
            rx.cond(
                AuthState.auth_error != "",
                rx.callout(
                    AuthState.auth_error,
                    icon="triangle_alert",
                    color_scheme="red",
                    width="100%",
                ),
            ),
            rx.form(
                rx.vstack(
                    rx.text(_t["auth.full_name"], size="2", weight="medium"),
                    rx.input(
                        placeholder=_t["auth.ph_full_name"],
                        name="full_name",
                        width="100%",
                    ),
                    rx.text(_t["auth.email"], size="2", weight="medium"),
                    rx.input(
                        placeholder=_t["auth.ph_email"],
                        name="email",
                        width="100%",
                    ),
                    rx.text(_t["auth.password"], size="2", weight="medium"),
                    rx.input(
                        placeholder=_t["auth.ph_password"],
                        name="password",
                        type="password",
                        width="100%",
                    ),
                    rx.button(
                        _t["auth.create_account"],
                        type="submit",
                        width="100%",
                        size="3",
                    ),
                    captcha_script("signup"),
                    spacing="3",
                    width="100%",
                ),
                on_submit=AuthState.signup,
            ),
            rx.text(
                _t["auth.have_account"],
                " ",
                rx.link(
                    _t["auth.sign_in"],
                    href="/login",
                    on_click=AuthState.clear_auth_error,
                ),
                size="2",
                color="gray",
            ),
            spacing="4",
            width="360px",
            padding="32px",
            border="1px solid var(--gray-a5)",
            border_radius="12px",
            bg="var(--color-background)",
        ),
        height="100vh",
    )
