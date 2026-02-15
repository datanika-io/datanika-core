"""Signup page."""

import reflex as rx

from etlfabric.ui.state.auth_state import AuthState


def signup_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.heading("ETL Fabric", size="7"),
            rx.text("Create your account", size="3", color="gray"),
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
                rx.text("Full Name", size="2", weight="medium"),
                rx.input(
                    placeholder="Alice Smith",
                    value=AuthState.signup_full_name,
                    on_change=AuthState.set_signup_full_name,
                    width="100%",
                ),
                rx.text("Email", size="2", weight="medium"),
                rx.input(
                    placeholder="you@example.com",
                    value=AuthState.signup_email,
                    on_change=AuthState.set_signup_email,
                    type="email",
                    width="100%",
                ),
                rx.text("Password", size="2", weight="medium"),
                rx.input(
                    placeholder="Password",
                    value=AuthState.signup_password,
                    on_change=AuthState.set_signup_password,
                    type="password",
                    width="100%",
                ),
                rx.button(
                    "Create Account",
                    on_click=AuthState.signup,
                    width="100%",
                    size="3",
                ),
                spacing="3",
                width="100%",
            ),
            rx.text(
                "Already have an account? ",
                rx.link("Sign in", href="/login"),
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
