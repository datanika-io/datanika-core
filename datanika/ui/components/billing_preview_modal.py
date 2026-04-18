"""Billing preview modal — V2 P3 follow-up surface.

Accessible from Settings page. Shows cumulative GB consumed this billing
cycle, projected overage, and line items for the upcoming invoice.

Gated by ``settings.datanika_dual_mode_ux_enabled`` at the caller level.

Wired against mocked DashboardState.bytes_* / plan data until Engineering
V2 P3 populates real fields. Rates from SPEC_PRICING_V2 v3.
"""

from __future__ import annotations

import reflex as rx

from datanika.ui.state.dashboard_state import DashboardState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

BILLING_KEYS = (
    "billing.preview_title",
    "billing.plan_label",
    "billing.cycle_label",
    "billing.volume_used",
    "billing.volume_included",
    "billing.overage_gb",
    "billing.overage_cost",
    "billing.projected_total",
    "billing.no_overage",
    "billing.close",
    "billing.cycle.closes_on",
    "billing.cycle.closes_in_days",
    "billing.cycle.closes_today",
    "billing.projected_overage.heading",
    "billing.projected_overage.see_notification",
)
_KEYS_FOR_SCANNER = (
    _t["billing.preview_title"],
    _t["billing.plan_label"],
    _t["billing.cycle_label"],
    _t["billing.volume_used"],
    _t["billing.volume_included"],
    _t["billing.overage_gb"],
    _t["billing.overage_cost"],
    _t["billing.projected_total"],
    _t["billing.no_overage"],
    _t["billing.close"],
    _t["billing.cycle.closes_on"],
    _t["billing.cycle.closes_in_days"],
    _t["billing.cycle.closes_today"],
    _t["billing.projected_overage.heading"],
    _t["billing.projected_overage.see_notification"],
)


def _usage_row(label_key: str, value: rx.Var) -> rx.Component:
    return rx.hstack(
        rx.text(_t[label_key], size="2", color="gray"),
        rx.spacer(),
        rx.text(value, size="2", weight="medium"),
        width="100%",
    )


def _cycle_countdown_row() -> rx.Component:
    """Cycle-close countdown row — shown only when cloud populated
    ``cycle_ends_at``. Renders one of three copy variants based on the
    days-remaining computed var, so the label stays human across the
    full run of a billing cycle.
    """
    return rx.cond(
        DashboardState.has_cycle_end,
        rx.hstack(
            rx.icon("calendar-clock", size=14, color="var(--slate-10)"),
            rx.text(
                rx.cond(
                    DashboardState.cycle_days_remaining <= 0,
                    _t["billing.cycle.closes_today"],
                    rx.cond(
                        DashboardState.cycle_days_remaining == 1,
                        _t["billing.cycle.closes_on"] + " " + DashboardState.cycle_ends_at,
                        _t["billing.cycle.closes_in_days"].to(str)
                        + " "
                        + DashboardState.cycle_days_remaining.to(str)
                        + " — "
                        + DashboardState.cycle_ends_at,
                    ),
                ),
                size="1",
                color="var(--slate-11)",
            ),
            align="center",
            spacing="1",
            width="100%",
        ),
        rx.fragment(),
    )


def _projected_overage_callout() -> rx.Component:
    """Callout shown when the org is already over its volume cap. Links
    the user to the most recent ``charge_incoming`` notification so they
    can see the exact projected charge the cloud plugin computed at
    cycle-close T-24h.

    Kept deliberately terse — the full projected-charge amount lives on
    the notification itself; duplicating it here would drift out of
    sync with the notification's single source of truth.
    """
    return rx.cond(
        DashboardState.bytes_percent >= 100,
        rx.box(
            rx.hstack(
                rx.icon("triangle-alert", size=14, color="var(--amber-11)"),
                rx.text(
                    _t["billing.projected_overage.heading"],
                    size="1",
                    weight="bold",
                    color="var(--amber-11)",
                ),
                align="center",
                spacing="2",
            ),
            rx.text(
                _t["billing.projected_overage.see_notification"],
                size="1",
                color="var(--slate-11)",
            ),
            padding="8px",
            border_radius="6px",
            bg="var(--amber-a2)",
            border="1px solid var(--amber-a6)",
            width="100%",
        ),
        rx.fragment(),
    )


def billing_preview_modal() -> rx.Component:
    """Render the billing preview as an inline card for the settings page.

    Uses DashboardState.bytes_used / bytes_limit / plan_name which are
    populated via the ``usage.get_summary`` hook from the cloud plugin.
    Cycle close countdown + projected-overage callout added for V2 P5
    Option B (see `charge_notification_hooks.py` + core#252).
    """
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.icon("receipt", size=16, color="var(--violet-11)"),
                rx.text(_t["billing.preview_title"], size="3", weight="bold"),
                align="center",
                spacing="2",
            ),
            rx.separator(),
            _usage_row("billing.plan_label", DashboardState.plan_name),
            _usage_row("billing.volume_used", DashboardState.bytes_used_display),
            _usage_row("billing.volume_included", DashboardState.bytes_limit_display),
            _cycle_countdown_row(),
            rx.cond(
                DashboardState.bytes_percent >= 100,
                rx.vstack(
                    rx.separator(),
                    _usage_row(
                        "billing.overage_gb",
                        rx.cond(
                            DashboardState.bytes_limit > 0,
                            (
                                (DashboardState.bytes_used - DashboardState.bytes_limit) / (1024**3)
                            ).to(str),
                            "0",
                        ),
                    ),
                    rx.text(
                        _t["billing.overage_cost"],
                        size="2",
                        weight="bold",
                        color="var(--red-11)",
                    ),
                    _projected_overage_callout(),
                    spacing="2",
                    width="100%",
                ),
                rx.text(
                    _t["billing.no_overage"],
                    size="1",
                    color="var(--green-11)",
                ),
            ),
            spacing="3",
            width="100%",
        ),
        width="100%",
        variant="surface",
    )
