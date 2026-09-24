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
    """Free tier: hard block, no overage.

    🚨 **Both strings here used to be TEMPLATES painted raw** (#1540), and this component is
    gated, so nobody ever saw them. Neither could be filled from anything the UI holds:

    * ``quota.volume_quota_reached_body`` needs ``{needed}`` — the projected size of the run
      being refused. ``BaseState`` carries ``is_quota_error`` and ``quota_metric`` and nothing
      else about the run, so no producer exists. Substituting ``{plan}`` and ``{remaining}`` and
      leaving ``{needed}`` would paint a confident sentence with a rendering bug in the middle of
      it — strictly worse than an obviously-unfinished template, and the same trap as #1536's
      ``has_overage_figures``, which refuses to draw a partial set because it would read as a
      confident ``$0.00``.
    * ``quota.upgrade_to_next_tier`` needs the **next** tier's ``{plan}`` and ``{gb}``.
      ``DashboardState.plan_name`` is the *current* plan, so the obvious substitution renders
      "Upgrade to Free (… included)" to a Free user.

    So this renders the placeholder-free keys that already exist and are already translated in
    all nine locales. The two templates stay in the locale files for whenever a producer arrives;
    ``tests/test_i18n/test_placeholders_are_substituted.py`` holds them in a **two-way** ratchet,
    so the day something paints them the exemption fails for being stale.
    """
    return rx.vstack(
        rx.text(
            _t["quota.volume_quota_reached_title"],
            size="4",
            weight="bold",
            color="var(--red-11)",
        ),
        rx.text(_t["quota.upgrade_hint"], size="2"),
        rx.link(
            rx.button(
                _t["quota.upgrade_button"],
                size="2",
                variant="solid",
            ),
            href=BILLING_ROUTE,
        ),
        spacing="3",
        align="start",
        width="100%",
    )


def _pro_overage_content() -> rx.Component:
    """Pro/Enterprise: soft overage confirmation.

    ``quota.allow_as_overage`` is a template — "Allow this run as overage ({gb} GB × ${rate} =
    ${total})" — and is substituted here rather than painted raw (#1540).

    ⚠️ **Gated on ``has_overage_figures``, and that gate is the point rather than tidiness.**
    ``{rate}`` and ``{total}`` are *pricing* figures: per ``SPEC_USAGE_VISIBILITY`` §2.5 they come
    from the biller and are never recomputed in the page. A partial set would render
    "0 GB × $0 = $0" — a confident price we invented. When the figures are absent the sentence is
    omitted entirely and the button, which carries no placeholders, still explains the action.
    """
    return rx.vstack(
        rx.text(
            _t["quota.volume_quota_reached_title"],
            size="4",
            weight="bold",
            color="var(--amber-11)",
        ),
        rx.cond(
            DashboardState.has_overage_figures,
            rx.text(
                _t["quota.allow_as_overage"]
                .replace("{gb}", DashboardState.bytes_overage_gb)
                .replace("{rate}", DashboardState.bytes_overage_rate)
                .replace("{total}", DashboardState.bytes_overage_total),
                size="2",
            ),
        ),
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
