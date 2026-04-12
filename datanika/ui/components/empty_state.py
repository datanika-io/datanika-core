"""Reusable empty-state component for list pages.

Driven by a small preset table so callers pass a page key rather than
a bag of strings. The onboarding checklist on the dashboard points to
these pages; the CTA here is the second half of the same motion.
"""

from dataclasses import dataclass

import reflex as rx

from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


@dataclass(frozen=True)
class EmptyStatePreset:
    icon: str
    title_key: str
    body_key: str
    cta_key: str
    cta_href: str


_KEYS_FOR_SCANNER = (
    _t["empty.connections.title"],
    _t["empty.connections.body"],
    _t["empty.connections.cta"],
    _t["empty.pipelines.title"],
    _t["empty.pipelines.body"],
    _t["empty.pipelines.cta"],
    _t["empty.transformations.title"],
    _t["empty.transformations.body"],
    _t["empty.transformations.cta"],
)

EMPTY_STATE_PRESETS: dict[str, EmptyStatePreset] = {
    "connections": EmptyStatePreset(
        icon="database",
        title_key="empty.connections.title",
        body_key="empty.connections.body",
        cta_key="empty.connections.cta",
        cta_href="#connection-form",
    ),
    "pipelines": EmptyStatePreset(
        icon="git-branch",
        title_key="empty.pipelines.title",
        body_key="empty.pipelines.body",
        cta_key="empty.pipelines.cta",
        cta_href="#pipeline-form",
    ),
    "transformations": EmptyStatePreset(
        icon="code",
        title_key="empty.transformations.title",
        body_key="empty.transformations.body",
        cta_key="empty.transformations.cta",
        cta_href="#transformation-form",
    ),
}


def empty_state(page_key: str) -> rx.Component:
    """Render the canonical empty state for a list page.

    Raises KeyError if `page_key` is not in EMPTY_STATE_PRESETS so new
    pages can't silently skip having an empty state designed.
    """
    preset = EMPTY_STATE_PRESETS[page_key]
    return rx.card(
        rx.vstack(
            rx.icon(preset.icon, size=40, color="var(--violet-9)"),
            rx.heading(_t[preset.title_key], size="4"),
            rx.text(
                _t[preset.body_key],
                size="2",
                color="var(--slate-10)",
                text_align="center",
                max_width="42ch",
            ),
            rx.link(
                rx.button(
                    _t[preset.cta_key],
                    rx.icon("arrow-right", size=16),
                    size="2",
                    color_scheme="violet",
                ),
                href=preset.cta_href,
                underline="none",
            ),
            align="center",
            spacing="3",
            padding_y="8",
        ),
        width="100%",
    )
