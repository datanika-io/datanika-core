import reflex as rx

config = rx.Config(
    app_name="etlfabric",
    db_url="sqlite:///reflex.db",
    disable_plugins=["reflex.plugins.sitemap.SitemapPlugin"],
)
