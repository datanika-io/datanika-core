"""Dashboard page — overview."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout


def dashboard_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.card(
                rx.text("Welcome to ETL Fabric", size="4"),
                rx.text(
                    "Manage your data pipelines, transformations, and schedules.",
                    color="gray",
                ),
                width="100%",
            ),
            rx.hstack(
                rx.card(
                    rx.vstack(
                        rx.icon("plug", size=24),
                        rx.text("Connections", weight="bold"),
                        rx.text("Manage data sources and destinations", size="2", color="gray"),
                        align="center",
                    ),
                    width="200px",
                ),
                rx.card(
                    rx.vstack(
                        rx.icon("git-branch", size=24),
                        rx.text("Pipelines", weight="bold"),
                        rx.text("Extract and load data with dlt", size="2", color="gray"),
                        align="center",
                    ),
                    width="200px",
                ),
                rx.card(
                    rx.vstack(
                        rx.icon("code", size=24),
                        rx.text("Transformations", weight="bold"),
                        rx.text("Transform data with dbt", size="2", color="gray"),
                        align="center",
                    ),
                    width="200px",
                ),
                rx.card(
                    rx.vstack(
                        rx.icon("clock", size=24),
                        rx.text("Schedules", weight="bold"),
                        rx.text("Automate pipeline runs", size="2", color="gray"),
                        align="center",
                    ),
                    width="200px",
                ),
                spacing="4",
                wrap="wrap",
            ),
            spacing="6",
            width="100%",
        ),
        title="Dashboard",
    )
