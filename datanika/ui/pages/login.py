"""Login page."""

import json

import reflex as rx

from datanika.config import settings
from datanika.ui.components.captcha import captcha_script
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_backend = settings.oauth_redirect_base_url
_t = I18nState.translations


def _social_login_button(label: str, provider: str) -> rx.Component:
    """Start a social login in the current tab (#418).

    This has to be a real browser navigation: ``/api/auth/login/<provider>``
    is a backend Starlette route that 302s to the provider, so nothing the
    frontend router does can serve it. It also has to stay in *this* tab —
    the previous ``rx.link(..., is_external=True)`` compiled to
    ``target="_blank"``, so the user authenticated in a second tab and ended
    up signed in there while the original still showed the sign-in form.

    Neither ``rx.link`` nor ``rx.el.a`` can express that: both render a
    react-router ``Link``, which treats a *same-origin* absolute URL as an
    in-app route — and in production the backend and frontend share
    ``app.datanika.io``, so the click would be swallowed by the router.
    (In dev the origins differ, so that breakage would not show up locally.)
    ``rx.redirect`` has the same same-origin branch. Hence an explicit
    assignment, which is unambiguous whatever the router does.
    """
    target = f"{_backend}/api/auth/login/{provider}"
    return rx.button(
        label,
        variant="outline",
        size="3",
        width="100%",
        type="button",
        on_click=rx.call_script(f"window.location.assign({json.dumps(target)})"),
    )


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
                    rx.el.label(
                        rx.text(_t["auth.email"], size="2", weight="medium"),
                        html_for="login-email",
                    ),
                    rx.input(
                        id="login-email",
                        placeholder=_t["auth.ph_email"],
                        name="email",
                        width="100%",
                    ),
                    rx.el.label(
                        rx.text(_t["auth.password"], size="2", weight="medium"),
                        html_for="login-password",
                    ),
                    rx.input(
                        id="login-password",
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
                _social_login_button("Google", "google"),
                _social_login_button("GitHub", "github"),
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
