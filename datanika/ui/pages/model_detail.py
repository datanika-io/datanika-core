"""Model detail page — view/edit columns, description, dbt config."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.model_detail_state import ColumnItem, ModelDetailState

_t = I18nState.translations

_CUSTOM_TEST_OPTIONS = [
    "accepted_values",
    "relationships",
    "expression_is_true",
    "not_constant",
    "not_null_proportion",
    "accepted_range",
    "sequential_values",
]


def _type_color(entry_type: rx.Var[str]) -> rx.Var[str]:
    return rx.cond(entry_type == "source_table", "blue", "purple")


def header_section() -> rx.Component:
    return rx.hstack(
        rx.badge(
            ModelDetailState.entry_type,
            color_scheme=_type_color(ModelDetailState.entry_type),
            size="2",
        ),
        rx.heading(ModelDetailState.table_name, size="5"),
        rx.text(
            _t["model_detail.schema_label"],
            ": ",
            ModelDetailState.schema_name,
            " | ",
            _t["model_detail.origin_label"],
            ": ",
            ModelDetailState.origin_name,
            color="gray",
            size="2",
        ),
        spacing="3",
        align="center",
        wrap="wrap",
    )


def description_section() -> rx.Component:
    return rx.vstack(
        rx.text(_t["model_detail.description"], size="3", weight="bold"),
        rx.text_area(
            value=ModelDetailState.form_description,
            on_change=ModelDetailState.set_form_description,
            placeholder="Add a description for this model...",
            width="100%",
            rows="3",
        ),
        spacing="2",
        width="100%",
    )


def alias_section() -> rx.Component:
    return rx.vstack(
        rx.text(_t["model_detail.alias"], size="3", weight="bold"),
        rx.input(
            placeholder="dbt alias (optional)",
            value=ModelDetailState.form_alias,
            on_change=ModelDetailState.set_form_alias,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def tags_section() -> rx.Component:
    return rx.vstack(
        rx.text(_t["model_detail.tags"], size="3", weight="bold"),
        rx.input(
            placeholder="Comma-separated tags (e.g. finance, daily)",
            value=ModelDetailState.form_tags,
            on_change=ModelDetailState.set_form_tags,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def config_section() -> rx.Component:
    return rx.vstack(
        rx.text(_t["model_detail.dbt_config"], size="3", weight="bold"),
        rx.text_area(
            value=ModelDetailState.form_dbt_config,
            on_change=ModelDetailState.set_form_dbt_config,
            placeholder='{"materialized": "view"}',
            width="100%",
            rows="4",
        ),
        spacing="2",
        width="100%",
    )


def _test_badges(col: rx.Var[ColumnItem]) -> rx.Component:
    """Compact badge summary of tests on a column."""
    return rx.hstack(
        rx.cond(
            col.has_not_null,
            rx.badge("required", color_scheme="red", size="1"),
            rx.fragment(),
        ),
        rx.cond(col.has_unique, rx.badge("unique", color_scheme="blue", size="1"), rx.fragment()),
        rx.cond(
            col.accepted_values_csv != "",
            rx.badge("values", color_scheme="green", size="1"),
            rx.fragment(),
        ),
        rx.cond(
            col.relationship_to != "",
            rx.badge("FK", color_scheme="orange", size="1"),
            rx.fragment(),
        ),
        spacing="1",
        wrap="wrap",
    )


def _custom_test_form() -> rx.Component:
    """Form shown when adding a custom dbt_utils test to a column."""
    return rx.cond(
        ModelDetailState.adding_test_column != "",
        rx.card(
            rx.vstack(
                rx.text(_t["model_detail.add_test"], size="2", weight="bold"),
                rx.select(
                    _CUSTOM_TEST_OPTIONS,
                    placeholder="Select test type...",
                    value=ModelDetailState.custom_test_type,
                    on_change=ModelDetailState.set_custom_test_type,
                    width="100%",
                ),
                # accepted_values -> CSV input
                rx.cond(
                    ModelDetailState.custom_test_type == "accepted_values",
                    rx.input(
                        placeholder="Comma-separated values (e.g. active, inactive)",
                        value=ModelDetailState.custom_test_expression,
                        on_change=ModelDetailState.set_custom_test_expression,
                        width="100%",
                    ),
                    rx.fragment(),
                ),
                # relationships -> to + field inputs
                rx.cond(
                    ModelDetailState.custom_test_type == "relationships",
                    rx.hstack(
                        rx.input(
                            placeholder="to (e.g. ref('users'))",
                            value=ModelDetailState.custom_test_min_value,
                            on_change=ModelDetailState.set_custom_test_min_value,
                            width="50%",
                        ),
                        rx.input(
                            placeholder="field (e.g. id)",
                            value=ModelDetailState.custom_test_max_value,
                            on_change=ModelDetailState.set_custom_test_max_value,
                            width="50%",
                        ),
                        width="100%",
                    ),
                    rx.fragment(),
                ),
                # expression_is_true -> expression input
                rx.cond(
                    ModelDetailState.custom_test_type == "expression_is_true",
                    rx.input(
                        placeholder="Expression (e.g. amount > 0)",
                        value=ModelDetailState.custom_test_expression,
                        on_change=ModelDetailState.set_custom_test_expression,
                        width="100%",
                    ),
                    rx.fragment(),
                ),
                # not_null_proportion -> at_least input
                rx.cond(
                    ModelDetailState.custom_test_type == "not_null_proportion",
                    rx.input(
                        placeholder="at_least (0.0 - 1.0)",
                        value=ModelDetailState.custom_test_proportion,
                        on_change=ModelDetailState.set_custom_test_proportion,
                        width="100%",
                    ),
                    rx.fragment(),
                ),
                # accepted_range -> min/max inputs
                rx.cond(
                    ModelDetailState.custom_test_type == "accepted_range",
                    rx.hstack(
                        rx.input(
                            placeholder="min_value",
                            value=ModelDetailState.custom_test_min_value,
                            on_change=ModelDetailState.set_custom_test_min_value,
                            width="50%",
                        ),
                        rx.input(
                            placeholder="max_value",
                            value=ModelDetailState.custom_test_max_value,
                            on_change=ModelDetailState.set_custom_test_max_value,
                            width="50%",
                        ),
                        width="100%",
                    ),
                    rx.fragment(),
                ),
                # sequential_values -> interval input (reuses min_value)
                rx.cond(
                    ModelDetailState.custom_test_type == "sequential_values",
                    rx.input(
                        placeholder="interval (optional, e.g. 1)",
                        value=ModelDetailState.custom_test_min_value,
                        on_change=ModelDetailState.set_custom_test_min_value,
                        width="100%",
                    ),
                    rx.fragment(),
                ),
                rx.hstack(
                    rx.button(
                        _t["common.add"],
                        on_click=ModelDetailState.add_custom_test,
                        size="1",
                        color_scheme="blue",
                    ),
                    rx.button(
                        _t["common.cancel"],
                        on_click=ModelDetailState.cancel_custom_test_form,
                        size="1",
                        variant="outline",
                    ),
                    spacing="2",
                ),
                spacing="2",
                width="100%",
            ),
            width="100%",
        ),
        rx.fragment(),
    )


def _expanded_column_card(col: rx.Var[ColumnItem]) -> rx.Component:
    """Expanded editing card for a single column."""
    return rx.cond(
        ModelDetailState.expanded_column == col.name,
        rx.card(
            rx.vstack(
                # Description
                rx.text(_t["model_detail.description"], size="2", weight="bold"),
                rx.input(
                    placeholder="Column description...",
                    value=col.description,
                    on_change=lambda v: ModelDetailState.set_column_description_by_name(
                        col.name, v
                    ),
                    width="100%",
                ),
                rx.separator(),
                # Tests section
                rx.text(_t["model_detail.tests"], size="2", weight="bold"),
                rx.hstack(
                    rx.checkbox(
                        _t["model_detail.required_not_null"],
                        checked=col.has_not_null,
                        on_change=lambda checked: ModelDetailState.toggle_column_not_null(
                            col.name, checked
                        ),
                    ),
                    rx.checkbox(
                        _t["model_detail.unique"],
                        checked=col.has_unique,
                        on_change=lambda checked: ModelDetailState.toggle_column_unique(
                            col.name, checked
                        ),
                    ),
                    spacing="4",
                ),
                # Additional tests
                rx.text(_t["model_detail.tests"], size="2", weight="bold"),
                rx.foreach(
                    col.additional_tests,
                    lambda t: rx.badge(
                        t,
                        rx.icon(
                            "x",
                            size=12,
                            cursor="pointer",
                            on_click=ModelDetailState.remove_column_test(
                                col.name, t,
                            ),
                        ),
                        variant="outline",
                        size="1",
                    ),
                ),
                # Add custom test button
                rx.button(
                    rx.icon("plus", size=14),
                    " ",
                    _t["model_detail.add_test"],
                    on_click=ModelDetailState.open_custom_test_form(col.name),
                    size="1",
                    variant="outline",
                ),
                # Custom test form (when adding_test_column matches)
                rx.cond(
                    ModelDetailState.adding_test_column == col.name,
                    _custom_test_form(),
                    rx.fragment(),
                ),
                spacing="3",
                width="100%",
            ),
            width="100%",
        ),
        rx.fragment(),
    )


def _column_row(col: rx.Var[ColumnItem]) -> rx.Component:
    """A single column row with expand/collapse and inline test badges."""
    return rx.vstack(
        rx.hstack(
            rx.code(col.name, size="2"),
            rx.badge(col.data_type, variant="outline", size="1"),
            _test_badges(col),
            rx.spacer(),
            rx.icon_button(
                rx.icon(
                    rx.cond(
                        ModelDetailState.expanded_column == col.name,
                        "chevron-up",
                        "chevron-down",
                    ),
                    size=16,
                ),
                size="1",
                variant="ghost",
                on_click=ModelDetailState.toggle_column_expand(col.name),
            ),
            width="100%",
            align="center",
        ),
        _expanded_column_card(col),
        width="100%",
        spacing="1",
    )


def editable_columns_section() -> rx.Component:
    return rx.vstack(
        rx.text(_t["model_detail.columns"], size="3", weight="bold"),
        rx.cond(
            ModelDetailState.columns.length() > 0,
            rx.vstack(
                rx.foreach(ModelDetailState.columns, _column_row),
                spacing="2",
                width="100%",
            ),
            rx.text(_t["model_detail.no_columns"], color="gray"),
        ),
        spacing="2",
        width="100%",
    )


def actions_section() -> rx.Component:
    return rx.hstack(
        rx.button(
            _t["common.save"],
            on_click=ModelDetailState.save_model_detail,
            color_scheme="blue",
        ),
        rx.link(
            rx.button(_t["model_detail.back_to_models"], variant="outline"),
            href="/models",
        ),
        spacing="3",
    )


def model_detail_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.cond(
                ModelDetailState.error_message,
                rx.callout(
                    ModelDetailState.error_message,
                    icon="triangle_alert",
                    color_scheme="red",
                ),
                rx.fragment(),
            ),
            header_section(),
            rx.separator(),
            description_section(),
            alias_section(),
            tags_section(),
            config_section(),
            editable_columns_section(),
            rx.separator(),
            actions_section(),
            spacing="5",
            width="100%",
        ),
        title=_t["model_detail.title"],
    )
