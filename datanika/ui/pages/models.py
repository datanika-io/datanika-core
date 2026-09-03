"""Models page — catalog entries list with last run status."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.components.table_loading import table_loading
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
            # core#872: the emptiness question is only worth ASKING once the
            # answer has arrived. Before this outer cond, `models == []` on the
            # first paint took the empty branch and told a user who has data, in
            # words, that they have none — for 5-17 seconds, and once for 30.
            rx.cond(
                ModelState.models_loaded,
                rx.cond(
                    ModelState.models.length() == 0,
                    rx.callout(
                        # "Run an upload to populate the catalog" is correct only
                        # for someone who has never run one. Told to a user whose
                        # load just went green with a row count, it sends them back
                        # around the same loop (core#883).
                        rx.cond(
                            ModelState.loaded_without_catalog,
                            _t["models.no_models_after_load"],
                            _t["models.no_models"],
                        ),
                        icon="info",
                    ),
                    models_table(),
                ),
                table_loading(),
            ),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.models"],
    )
