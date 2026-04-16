"""ELT nudge card — V2 P3 follow-up surface.

Shows a "switch to ELT and save ~$X/month" banner on the pipeline run detail
page when the pipeline is in ETL mode and recent usage suggests ELT would be
cheaper.

Gated by ``settings.datanika_dual_mode_ux_enabled`` at the caller level.

Per SPEC_DUAL_MODE_UX acceptance criterion #8: visible when pipeline has
>=5 runs AND >=20 GB in the last 30 days AND mode == "etl".
Dismissal: 30 days per pipeline (acceptance criterion #9).
"""

from __future__ import annotations

import reflex as rx

from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

NUDGE_KEYS = (
    "nudge.title",
    "nudge.message",
    "nudge.savings_hint",
    "nudge.switch_cta",
    "nudge.learn_more",
    "nudge.dismiss",
)
_KEYS_FOR_SCANNER = (
    _t["nudge.title"],
    _t["nudge.message"],
    _t["nudge.savings_hint"],
    _t["nudge.switch_cta"],
    _t["nudge.learn_more"],
    _t["nudge.dismiss"],
)


def elt_nudge_card() -> rx.Component:
    """Render the ELT nudge banner for the runs page.

    The visibility condition (>=5 runs, >=20 GB, mode==etl, not dismissed
    within 30 days) will be evaluated by the state layer once Engineering's
    V2 P3 run data is available. Until then, the card is structurally correct
    and tested against mocked state.
    """
    return rx.callout(
        rx.vstack(
            rx.hstack(
                rx.icon("zap", size=16, color="var(--violet-11)"),
                rx.text(_t["nudge.title"], size="2", weight="bold"),
                align="center",
                spacing="2",
            ),
            rx.text(_t["nudge.message"], size="2"),
            rx.text(
                _t["nudge.savings_hint"],
                size="2",
                weight="medium",
                color="var(--green-11)",
            ),
            rx.hstack(
                rx.button(
                    _t["nudge.switch_cta"],
                    size="1",
                    color_scheme="violet",
                ),
                rx.link(
                    _t["nudge.learn_more"],
                    href="/pricing/",
                    size="1",
                    color="var(--violet-11)",
                    is_external=True,
                ),
                rx.spacer(),
                rx.button(
                    _t["nudge.dismiss"],
                    size="1",
                    variant="ghost",
                    color_scheme="gray",
                ),
                width="100%",
                align="center",
                spacing="2",
            ),
            spacing="2",
            width="100%",
        ),
        icon="lightbulb",
        color_scheme="violet",
        width="100%",
    )
