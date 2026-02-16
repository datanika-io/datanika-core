"""OAuth completion page — processes tokens from OAuth callback redirect."""

import reflex as rx


def auth_complete_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.spinner(size="3"),
            rx.text("Completing sign in...", size="3", color="gray"),
            spacing="4",
            align="center",
        ),
        height="100vh",
    )
