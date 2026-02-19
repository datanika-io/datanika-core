"""Full-screen SQL editor page for transformations."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.components.sql_autocomplete import (
    REF_AUTOCOMPLETE_JS,
    ref_hidden_buttons,
    ref_popover,
)
from datanika.ui.pages.transformations import preview_display
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.transformation_state import TransformationState

_t = I18nState.translations


def sql_editor_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.link(
                rx.hstack(
                    rx.icon("arrow-left", size=16),
                    rx.text(_t["transformations.back_to_form"], size="2"),
                    spacing="1",
                    align="center",
                ),
                href="/transformations",
                underline="none",
            ),
            rx.text(
                TransformationState.form_name,
                size="3",
                weight="bold",
                color="var(--gray-11)",
            ),
            rx.box(
                rx.text_area(
                    placeholder=_t["transformations.ph_sql"],
                    value=TransformationState.form_sql_body,
                    on_change=TransformationState.set_form_sql_body,
                    id="sql-editor",
                    min_height="50vh",
                    width="100%",
                ),
                ref_popover(),
                position="relative",
                width="100%",
            ),
            ref_hidden_buttons(),
            rx.script(REF_AUTOCOMPLETE_JS),
            rx.hstack(
                rx.button(
                    _t["transformations.preview_sql"],
                    size="2",
                    variant="outline",
                    on_click=TransformationState.preview_compiled_sql_from_form,
                    disabled=~TransformationState.can_preview,
                ),
                rx.button(
                    _t["transformations.preview_result"],
                    size="2",
                    variant="outline",
                    on_click=TransformationState.preview_result_from_form,
                    disabled=~TransformationState.can_preview,
                ),
                rx.button(
                    _t["common.save"],
                    size="2",
                    on_click=TransformationState.save_sql_and_return,
                ),
                spacing="2",
            ),
            preview_display(),
            spacing="4",
            width="100%",
        ),
        title=_t["transformations.sql_editor"],
    )
