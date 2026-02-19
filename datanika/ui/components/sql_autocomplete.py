"""Shared SQL autocomplete components for ref() and source() completion."""

import reflex as rx

from datanika.ui.state.transformation_state import TransformationState

REF_AUTOCOMPLETE_JS = """
(function() {
    if (window.__refAutocompleteBound) return;
    window.__refAutocompleteBound = true;
    var debounceTimer = null;
    document.addEventListener('keydown', function(e) {
        var ta = document.getElementById('sql-editor');
        if (!ta || document.activeElement !== ta) return;
        if (!document.getElementById('ref-popover-box')) return;
        var map = {
            ArrowDown: 'ref-nav-down', ArrowUp: 'ref-nav-up',
            Enter: 'ref-select', Escape: 'ref-dismiss'
        };
        var btn = map[e.key];
        if (btn) {
            e.preventDefault();
            var el = document.getElementById(btn);
            if (el) el.click();
        }
    }, true);
    document.addEventListener('input', function(e) {
        if (e.target.id !== 'sql-editor') return;
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function() {
            var el = document.getElementById('ref-detect');
            if (el) el.click();
        }, 300);
    });
})();
"""


def ref_hidden_buttons() -> rx.Component:
    """Hidden buttons that JavaScript clicks programmatically to trigger state events."""
    return rx.box(
        rx.el.button(
            id="ref-nav-up",
            on_click=TransformationState.ref_navigate_up,
        ),
        rx.el.button(
            id="ref-nav-down",
            on_click=TransformationState.ref_navigate_down,
        ),
        rx.el.button(
            id="ref-select",
            on_click=TransformationState.ref_select_current,
        ),
        rx.el.button(
            id="ref-dismiss",
            on_click=TransformationState.ref_dismiss,
        ),
        rx.el.button(
            id="ref-detect",
            on_click=TransformationState.detect_ref_suggestions,
        ),
        display="none",
    )


def ref_popover() -> rx.Component:
    """Autocomplete popover that appears when typing {{ ref(' in the SQL editor."""
    return rx.cond(
        TransformationState.show_ref_popover,
        rx.box(
            rx.foreach(
                TransformationState.ref_suggestions,
                lambda name: rx.box(
                    rx.text(name, size="2"),
                    padding="4px 8px",
                    cursor="pointer",
                    background=rx.cond(
                        name == TransformationState.ref_selected_name,
                        "var(--accent-3)",
                        "transparent",
                    ),
                    _hover={"background": "var(--accent-4)"},
                    on_click=TransformationState.select_ref_suggestion(name),
                ),
            ),
            id="ref-popover-box",
            position="absolute",
            bottom="0",
            left="0",
            width="100%",
            max_height="160px",
            overflow_y="auto",
            background="var(--color-background)",
            border="1px solid var(--gray-6)",
            border_radius="6px",
            box_shadow="0 4px 12px rgba(0,0,0,0.15)",
            z_index="10",
        ),
        rx.fragment(),
    )
