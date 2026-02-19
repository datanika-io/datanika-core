"""Login page."""

import reflex as rx

from datanika.config import settings
from datanika.ui.components.captcha import captcha_script
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_backend = settings.oauth_redirect_base_url
_t = I18nState.translations


def login_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.hstack(
                rx.image(src="/logo.png", width="48px", height="48px"),
                rx.heading(_t["app.name"], size="7"),
                spacing="3",
                align="center",
            ),
            rx.text(_t["auth.sign_in_heading"], size="3", color="gray"),
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
                        _t["auth.sign_in"],
                        type="submit",
                        width="100%",
                        size="3",
                    ),
                    captcha_script("login"),
                    spacing="3",
                    width="100%",
                ),
                on_submit=AuthState.login,
            ),
            rx.divider(),
            rx.text(_t["auth.or_continue_with"], size="2", color="gray", text_align="center"),
            rx.hstack(
                rx.link(
                    rx.button(
                        "Google",
                        variant="outline",
                        size="3",
                        width="100%",
                    ),
                    href=f"{_backend}/api/auth/login/google",
                    width="100%",
                ),
                rx.link(
                    rx.button(
                        "GitHub",
                        variant="outline",
                        size="3",
                        width="100%",
                    ),
                    href=f"{_backend}/api/auth/login/github",
                    width="100%",
                ),
                width="100%",
                spacing="3",
            ),
            rx.text(
                _t["auth.no_account"],
                " ",
                rx.link(
                    _t["auth.sign_up"],
                    href="/signup",
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
