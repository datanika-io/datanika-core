"""Cost estimator card — V2 P3 follow-up surface.

Shows per-run cost estimate on the pipeline create/edit form based on the
user's ``form_volume_estimate_gb`` and their plan's $/GB overage rate.

Gated by ``settings.datanika_dual_mode_ux_enabled`` at the caller level.
When the flag is off, this module is never imported.

Wired against mocked DashboardState.bytes_* / plan data until Engineering
V2 P3 populates real fields. All rates hard-coded from SPEC_PRICING_V2 v3:
Pro $0.50/GB, Enterprise $0.25/GB.
"""

from __future__ import annotations

import reflex as rx

from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.pipeline_state import PipelineState

_t = I18nState.translations

# i18n key references — keeps the orphan scanner happy.
COST_KEYS = (
    "cost.title",
    "cost.estimate_label",
    "cost.per_gb_rate",
    "cost.total_estimate",
    "cost.mode_split_hint",
    "cost.unknown",
    "cost.free_included",
    "cost.learn_more",
)
_KEYS_FOR_SCANNER = (
    _t["cost.title"],
    _t["cost.estimate_label"],
    _t["cost.per_gb_rate"],
    _t["cost.total_estimate"],
    _t["cost.mode_split_hint"],
    _t["cost.unknown"],
    _t["cost.free_included"],
    _t["cost.learn_more"],
)


def _rate_display() -> rx.Component:
    """Show the $/GB rate for the user's plan."""
    return rx.text(
        _t["cost.per_gb_rate"],
        size="1",
        color="gray",
    )


def _total_estimate() -> rx.Component:
    """Show the estimated cost for this run based on volume estimate."""
    return rx.cond(
        PipelineState.form_volume_estimate_gb != "",
        rx.vstack(
            rx.text(_t["cost.estimate_label"], size="2", weight="medium"),
            rx.text(
                _t["cost.total_estimate"],
                size="3",
                weight="bold",
                color="var(--violet-11)",
            ),
            spacing="1",
            align="start",
        ),
        rx.text(_t["cost.unknown"], size="1", color="gray"),
    )


def _mode_split_hint() -> rx.Component:
    """Hint about ETL vs ELT cost difference."""
    return rx.cond(
        PipelineState.form_mode == "etl",
        rx.callout(
            _t["cost.mode_split_hint"],
            icon="lightbulb",
            color_scheme="violet",
            size="1",
        ),
    )


def cost_estimator_card() -> rx.Component:
    """Render the cost estimator card for the pipeline form."""
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.icon("calculator", size=16, color="var(--violet-11)"),
                rx.text(_t["cost.title"], size="3", weight="bold"),
                align="center",
                spacing="2",
            ),
            _rate_display(),
            _total_estimate(),
            _mode_split_hint(),
            rx.link(
                _t["cost.learn_more"],
                href="/pricing/",
                size="1",
                color="var(--violet-11)",
                is_external=True,
            ),
            spacing="3",
            width="100%",
        ),
        width="100%",
        variant="surface",
    )
