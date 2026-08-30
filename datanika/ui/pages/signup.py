"""Signup page."""

import reflex as rx

from datanika.ui.components.captcha import captcha_script
from datanika.ui.components.i18n_text import interpolate
from datanika.ui.components.layout import PRIVACY_URL, TERMS_URL
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
                    rx.el.label(
                        rx.text(_t["auth.full_name"], size="2", weight="medium"),
                        html_for="signup-full-name",
                    ),
                    rx.input(
                        id="signup-full-name",
                        placeholder=_t["auth.ph_full_name"],
                        name="full_name",
                        width="100%",
                    ),
                    rx.el.label(
                        rx.text(_t["auth.email"], size="2", weight="medium"),
                        html_for="signup-email",
                    ),
                    rx.input(
                        id="signup-email",
                        placeholder=_t["auth.ph_email"],
                        name="email",
                        default_value=AuthState.invite_email,
                        width="100%",
                    ),
                    rx.el.label(
                        rx.text(_t["auth.password"], size="2", weight="medium"),
                        html_for="signup-password",
                    ),
                    rx.input(
                        id="signup-password",
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
                # One translated sentence with the links substituted into it,
                # not fragments joined by punctuation chosen in Python (#682).
                # The old shape hardcoded English word order and used a middot
                # to do the work of "and"; it read as broken grammar in ru, es
                # and el, and key parity could not see it because every
                # fragment *was* translated.
                *interpolate(
                    _t["legal.signup_agreement"],
                    terms=rx.link(
                        _t["legal.terms"],
                        href=TERMS_URL,
                        is_external=True,
                    ),
                    privacy=rx.link(
                        _t["legal.privacy"],
                        href=PRIVACY_URL,
                        is_external=True,
                    ),
                ),
                size="1",
                color="gray",
                align="center",
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
