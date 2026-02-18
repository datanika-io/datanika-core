"""Models page — catalog entries list with last run status."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.model_state import ModelState

_t = I18nState.translations


def _type_color(entry_type: rx.Var[str]) -> rx.Var[str]:
    return rx.cond(entry_type == "source_table", "blue", "purple")


def _status_color(status: rx.Var[str]) -> rx.Var[str]:
    return rx.cond(
        status == "success",
        "green",
        rx.cond(
            status == "failed",
            "red",
            rx.cond(status == "running", "blue", "gray"),
        ),
    )


def models_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell(_t["common.type"]),
                rx.table.column_header_cell(_t["models.origin"]),
                rx.table.column_header_cell(_t["models.table_name"]),
                rx.table.column_header_cell(_t["models.schema"]),
                rx.table.column_header_cell(_t["models.last_status"]),
                rx.table.column_header_cell(_t["models.last_run"]),
                rx.table.column_header_cell(_t["models.rows"]),
                rx.table.column_header_cell(_t["models.columns"]),
                rx.table.column_header_cell(_t["common.actions"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                ModelState.models,
                lambda m: rx.table.row(
                    rx.table.cell(
                        rx.badge(m.entry_type, color_scheme=_type_color(m.entry_type)),
                    ),
                    rx.table.cell(rx.text(m.origin_name)),
                    rx.table.cell(rx.code(m.table_name)),
                    rx.table.cell(rx.text(m.schema_name)),
                    rx.table.cell(
                        rx.cond(
                            m.last_run_status,
                            rx.badge(
                                m.last_run_status,
                                color_scheme=_status_color(m.last_run_status),
                            ),
                            rx.text("-", color="gray"),
                        ),
                    ),
                    rx.table.cell(rx.text(m.last_run_datetime)),
                    rx.table.cell(rx.text(m.last_run_rows)),
                    rx.table.cell(rx.text(m.column_count)),
                    rx.table.cell(
                        rx.link(
                            rx.icon_button(
                                rx.icon("eye", size=16),
                                size="1",
                                variant="ghost",
                            ),
                            href=rx.cond(
                                m.id > 0,
                                f"/models/{m.id}",
                                "/models",
                            ),
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def models_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.cond(
                ModelState.models.length() == 0,
                rx.callout(
                    _t["models.no_models"],
                    icon="info",
                ),
                models_table(),
            ),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.models"],
    )
