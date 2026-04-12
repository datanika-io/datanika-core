"""Pipeline Templates page — pre-configured source→destination starters.

Shows a grid of cards. Clicking a card navigates the user to ``/connections``
with ``?template=<slug>``; ConnectionState picks up the slug on page load
and prefills the source connection form so the user only has to enter their
own credentials. See ``datanika/data/pipeline_templates.py`` for the
underlying registry.
"""

import reflex as rx

from datanika.data.pipeline_templates import LAUNCH_TEMPLATES, PipelineTemplate
from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

# Explicit references so the test_no_orphan_keys_in_json scanner can find the
# per-template name/description keys. The card component below looks them up
# dynamically via ``_t[tpl.name_key]`` because the slug-to-key mapping lives
# in the static template registry, not in this page module. Add a row here
# whenever a new template is appended to LAUNCH_TEMPLATES.
_TEMPLATE_I18N_REFS = (
    _t["templates.stripe_to_postgres.name"],
    _t["templates.stripe_to_postgres.description"],
    _t["templates.postgres_to_bigquery.name"],
    _t["templates.postgres_to_bigquery.description"],
    _t["templates.csv_to_duckdb.name"],
    _t["templates.csv_to_duckdb.description"],
)


def _template_card(tpl: PipelineTemplate) -> rx.Component:
    """Render a single template card.

    Built at import time from a static template — no Reflex state vars needed,
    so we render the source/destination labels as plain Python strings.
    """
    return rx.link(
        rx.card(
            rx.vstack(
                rx.hstack(
                    rx.badge(tpl.icon_source, color_scheme="blue", size="2"),
                    rx.icon("arrow_right", size=18, color="var(--slate-9)"),
                    rx.badge(tpl.icon_destination, color_scheme="violet", size="2"),
                    align="center",
                    spacing="2",
                ),
                rx.heading(_t[tpl.name_key], size="4"),
                rx.text(
                    _t[tpl.description_key],
                    size="2",
                    color="var(--slate-11)",
                ),
                rx.spacer(),
                rx.button(
                    _t["templates.use_this_template"],
                    width="100%",
                    variant="solid",
                ),
                spacing="3",
                align="start",
                height="100%",
            ),
            width="100%",
            height="100%",
            _hover={"border_color": "var(--accent-9)", "cursor": "pointer"},
        ),
        href=f"/connections?template={tpl.slug}",
        underline="none",
        width="100%",
    )


def pipeline_templates_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.heading(_t["templates.heading"], size="6"),
            rx.text(
                _t["templates.subtitle"],
                size="3",
                color="var(--slate-11)",
            ),
            rx.grid(
                *[_template_card(tpl) for tpl in LAUNCH_TEMPLATES],
                columns=rx.breakpoints(initial="1", md="3"),
                spacing="4",
                width="100%",
            ),
            rx.callout(
                _t["templates.help_text"],
                icon="info",
                color_scheme="gray",
                size="1",
                margin_top="1rem",
            ),
            spacing="4",
            width="100%",
            align="stretch",
        ),
        title=_t["templates.heading"],
    )
