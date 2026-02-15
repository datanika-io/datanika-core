"""Login page."""

import reflex as rx

from etlfabric.ui.state.auth_state import AuthState


def login_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.heading("ETL Fabric", size="7"),
            rx.text("Sign in to your account", size="3", color="gray"),
            rx.cond(
                AuthState.auth_error != "",
                rx.callout(
                    AuthState.auth_error,
                    icon="triangle_alert",
                    color_scheme="red",
                    width="100%",
                ),
            ),
            rx.vstack(
                rx.text("Email", size="2", weight="medium"),
                rx.input(
                    placeholder="you@example.com",
                    value=AuthState.login_email,
                    on_change=AuthState.set_login_email,
                    type="email",
                    width="100%",
                ),
                rx.text("Password", size="2", weight="medium"),
                rx.input(
                    placeholder="Password",
                    value=AuthState.login_password,
                    on_change=AuthState.set_login_password,
                    type="password",
                    width="100%",
                ),
                rx.button(
                    "Sign In",
                    on_click=AuthState.login,
                    width="100%",
                    size="3",
                ),
                spacing="3",
                width="100%",
            ),
            rx.divider(),
            rx.text("or continue with", size="2", color="gray", text_align="center"),
            rx.hstack(
                rx.link(
                    rx.button(
                        "Google",
                        variant="outline",
                        size="3",
                        width="100%",
                    ),
                    href="/api/auth/login/google",
                    width="100%",
                ),
                rx.link(
                    rx.button(
                        "GitHub",
                        variant="outline",
                        size="3",
                        width="100%",
                    ),
                    href="/api/auth/login/github",
                    width="100%",
                ),
                width="100%",
                spacing="3",
            ),
            rx.text(
                "Don't have an account? ",
                rx.link("Sign up", href="/signup"),
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
