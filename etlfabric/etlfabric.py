import reflex as rx


def index() -> rx.Component:
    return rx.box(
        rx.heading("ETL Fabric", size="7"),
        rx.text("Multi-tenant data pipeline management platform"),
        padding="2em",
    )


app = rx.App()
app.add_page(index, route="/")
