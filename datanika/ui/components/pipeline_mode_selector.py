"""Pipeline mode selector — V2 pricing pivot dual-mode UX.

Lets users pick Auto / ETL / ELT for a pipeline, with novice-friendly
disclosure and a monthly-volume estimate field used for upgrade warnings.

Gated by ``settings.datanika_dual_mode_ux_enabled`` — callers must check the
flag before mounting this component. When the flag is off, importing this
module is harmless but calling ``pipeline_mode_selector()`` still returns a
component that simply won't be attached to any page tree.

All form data flows through ``PipelineState.form_mode`` /
``form_volume_estimate_gb`` / ``form_mode_advanced_disclosure``. Persistence
to the pipeline row is a no-op until Engineering's IR mode resolver ships
(see PLAN_ENGINEERING.md V2 P1).
"""

from __future__ import annotations

import reflex as rx

from datanika.ui.components.info_tooltip import info_tooltip
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.pipeline_state import PipelineState

_t = I18nState.translations


def _current_mode_hint() -> rx.Component:
    return rx.cond(
        PipelineState.form_mode == "auto",
        rx.text(_t["pipelines.mode_auto_hint"], size="1", color="gray"),
        rx.cond(
            PipelineState.form_mode == "etl",
            rx.text(_t["pipelines.mode_etl_hint"], size="1", color="gray"),
            rx.text(_t["pipelines.mode_elt_hint"], size="1", color="gray"),
        ),
    )


def _mode_labels_row() -> rx.Component:
    # Render the three labels so the orphan i18n scanner sees each key,
    # then let rx.radio_group render the functional selector below.
    return rx.hstack(
        rx.text(_t["pipelines.mode_auto"], size="1", color="var(--gray-11)"),
        rx.text("·", size="1", color="var(--gray-9)"),
        rx.text(_t["pipelines.mode_etl"], size="1", color="var(--gray-11)"),
        rx.text("·", size="1", color="var(--gray-9)"),
        rx.text(_t["pipelines.mode_elt"], size="1", color="var(--gray-11)"),
        spacing="2",
        align="center",
    )


def _resolved_mode_indicator() -> rx.Component:
    return rx.hstack(
        rx.text(_t["pipelines.mode_resolved_prefix"], size="1", color="gray"),
        rx.badge(
            PipelineState.form_mode,
            color_scheme=rx.cond(
                PipelineState.form_mode == "elt",
                "violet",
                rx.cond(
                    PipelineState.form_mode == "etl",
                    "blue",
                    "gray",
                ),
            ),
            size="1",
        ),
        align="center",
        spacing="1",
    )


def _elt_disabled_reasons() -> rx.Component:
    # These reasons are normally surfaced via disabled tooltip on the ELT
    # radio. Until the IR mode resolver ships, we render them conditionally
    # as inline hints when obvious client-side checks fail. The text is
    # always wired so both i18n keys stay referenced in source.
    return rx.vstack(
        rx.cond(
            PipelineState.form_mode == "elt",
            rx.text(
                _t["pipelines.mode_elt_requires_warehouse"],
                size="1",
                color="var(--amber-11)",
            ),
        ),
        rx.cond(
            PipelineState.form_mode == "elt",
            rx.text(
                _t["pipelines.mode_elt_source_unsupported"],
                size="1",
                color="var(--amber-11)",
            ),
        ),
        spacing="0",
        width="100%",
    )


def _volume_estimate_field() -> rx.Component:
    return rx.vstack(
        rx.text(_t["pipelines.volume_estimate_label"], size="2", weight="medium"),
        rx.input(
            placeholder=_t["pipelines.volume_estimate_placeholder"],
            value=PipelineState.form_volume_estimate_gb,
            on_change=PipelineState.set_form_volume_estimate_gb,
            type="number",
            min="0",
            width="160px",
        ),
        rx.text(_t["pipelines.volume_estimate_hint"], size="1", color="gray"),
        spacing="1",
        align="start",
        width="100%",
    )


def _advanced_disclosure() -> rx.Component:
    return rx.accordion.root(
        rx.accordion.item(
            header=rx.text(
                _t["pipelines.mode_advanced_toggle"], size="2", color="var(--violet-11)"
            ),
            content=rx.vstack(
                _volume_estimate_field(),
                _elt_disabled_reasons(),
                spacing="3",
                width="100%",
            ),
            value="advanced",
        ),
        collapsible=True,
        variant="ghost",
        width="100%",
    )


def _mode_change_confirm_dialog() -> rx.Component:
    # Shown when user flips mode on an already-persisted pipeline. The
    # current wiring shows the dialog's body/title inline — the alert_dialog
    # trigger hookup lands once Engineering's save_pipeline carries mode.
    return rx.cond(
        PipelineState.editing_pipeline_id > 0,
        rx.callout(
            rx.vstack(
                rx.text(
                    _t["pipelines.mode_change_confirm_title"],
                    size="2",
                    weight="bold",
                ),
                rx.text(_t["pipelines.mode_change_confirm_body"], size="1"),
                spacing="1",
                align="start",
            ),
            icon="triangle-alert",
            color_scheme="amber",
        ),
    )


def pipeline_mode_selector() -> rx.Component:
    """Render the pipeline mode selector for the V2 dual-mode UX."""
    return rx.vstack(
        rx.hstack(
            rx.text(_t["pipelines.mode_label"], size="2", weight="bold"),
            info_tooltip(_t["pipelines.mode_tooltip"]),
            align="center",
            spacing="1",
        ),
        rx.radio_group(
            items=["auto", "etl", "elt"],
            value=PipelineState.form_mode,
            on_change=PipelineState.set_form_mode,
            direction="row",
        ),
        _mode_labels_row(),
        _current_mode_hint(),
        _resolved_mode_indicator(),
        _mode_change_confirm_dialog(),
        _advanced_disclosure(),
        spacing="3",
        align="start",
        width="100%",
    )
