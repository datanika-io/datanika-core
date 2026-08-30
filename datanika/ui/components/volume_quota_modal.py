"""Volume quota modal — V2 pricing pivot Path A predict-and-reject UX.

Appears when a page's state sets ``is_quota_error = True`` with
``quota_metric == "bytes_processed"``. For Free-plan orgs, the modal is a
hard block (Q6 HARD SUNSET — no overage). For Pro/Enterprise orgs, it's a
soft-overage confirmation (Q3 SILENT — no upsell to the next tier).

Callers:
    volume_quota_modal(ConnectionState)  # or any BaseState subclass

The modal pulls ``plan_name`` from DashboardState (the only place it lives
today). Until Engineering's bytes quota hook adds the ``metric="bytes_processed"``
attr on QuotaExceededError, this modal never triggers — it's safely gated
by the feature flag at the mount site.
"""

from __future__ import annotations

import reflex as rx

from datanika.plugin_registry import BILLING_ROUTE
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.dashboard_state import DashboardState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def _free_hard_block_content() -> rx.Component:
    return rx.vstack(
        rx.text(
            _t["quota.volume_quota_reached_title"],
            size="4",
            weight="bold",
            color="var(--red-11)",
        ),
        rx.text(_t["quota.volume_quota_reached_body"], size="2"),
        rx.link(
            rx.button(
                _t["quota.upgrade_to_next_tier"],
                size="2",
                color_scheme="violet",
                variant="solid",
            ),
            href=BILLING_ROUTE,
        ),
        spacing="3",
        align="start",
        width="100%",
    )


def _pro_overage_content() -> rx.Component:
    return rx.vstack(
        rx.text(
            _t["quota.volume_quota_reached_title"],
            size="4",
            weight="bold",
            color="var(--amber-11)",
        ),
        rx.text(_t["quota.allow_as_overage"], size="2"),
        rx.button(
            _t["quota.allow_as_overage_button"],
            size="2",
            color_scheme="amber",
            variant="solid",
        ),
        spacing="3",
        align="start",
        width="100%",
    )


def volume_quota_modal(state_cls: type[BaseState]) -> rx.Component:
    """Render the Path A volume quota modal for ``state_cls``."""
    return rx.cond(
        state_cls.is_quota_error & (state_cls.quota_metric == "bytes_processed"),
        rx.card(
            rx.cond(
                DashboardState.plan_name == "Free",
                _free_hard_block_content(),
                _pro_overage_content(),
            ),
            width="100%",
            style={"border": "2px solid var(--red-8)"},
        ),
    )
