"""Searchable select component — a filterable dropdown built with popover + input."""

import reflex as rx

from datanika.ui.components.secure_input import no_autofill_attrs


def searchable_select(
    options: rx.Var[list[str]] | list[str],
    value: rx.Var[str],
    on_change: rx.EventHandler,
    placeholder: rx.Var[str] | str = "",
    search_placeholder: str = "Search...",
    width: str = "100%",
    size: str = "2",
) -> rx.Component:
    """A select component with a search/filter input.

    Uses a popover with an input for filtering and a scrollable list of options.
    Implemented as a pure-frontend component using rx.el and rx.script to avoid
    needing per-instance backend state for the filter text.
    """
    # Unique ID per instance based on placeholder (good enough for our use case)
    uid = f"ss_{hash(str(placeholder)) % 100000}"
    input_id = f"{uid}_input"
    list_id = f"{uid}_list"

    js_filter = f"""
    (function() {{
        const input = document.getElementById('{input_id}');
        const list = document.getElementById('{list_id}');
        if (!input || !list) return;
        input.addEventListener('input', function() {{
            const q = this.value.toLowerCase();
            for (const item of list.children) {{
                const text = item.textContent.toLowerCase();
                item.style.display = text.includes(q) ? '' : 'none';
            }}
        }});
    }})();
    """

    return rx.popover.root(
        rx.popover.trigger(
            rx.button(
                rx.cond(
                    value != "",
                    value,
                    rx.text(placeholder, color="var(--gray-a10)"),
                ),
                rx.spacer(),
                rx.icon("chevron-down", size=14),
                variant="outline",
                width=width,
                size=size,
                justify_content="space-between",
                display="flex",
                align_items="center",
                font_weight="normal",
                color="var(--gray-12)",
            ),
        ),
        rx.popover.content(
            rx.vstack(
                rx.input(
                    id=input_id,
                    name=input_id,
                    placeholder=search_placeholder,
                    size="2",
                    width="100%",
                    auto_focus=True,
                    # This filter box sits inside forms that also carry password
                    # inputs (the connection form is one), so it is eligible for
                    # the same positional autofill that core#618 was about. It
                    # is a transient filter, never a credential — opt it out.
                    custom_attrs=no_autofill_attrs(),
                ),
                rx.scroll_area(
                    rx.vstack(
                        rx.foreach(
                            options,
                            lambda opt: rx.popover.close(
                                rx.box(
                                    rx.text(opt, size="2"),
                                    on_click=on_change(opt),
                                    padding="6px 8px",
                                    border_radius="4px",
                                    cursor="pointer",
                                    width="100%",
                                    _hover={"background": "var(--accent-a3)"},
                                    background=rx.cond(
                                        opt == value,
                                        "var(--accent-a4)",
                                        "transparent",
                                    ),
                                ),
                            ),
                        ),
                        id=list_id,
                        spacing="0",
                        width="100%",
                    ),
                    max_height="200px",
                    width="100%",
                ),
                spacing="2",
                width="100%",
            ),
            width="var(--radix-popover-trigger-width)",
            side="bottom",
            align="start",
        ),
        rx.script(js_filter),
    )
