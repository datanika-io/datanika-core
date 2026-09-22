"""Every control on every page has an accessible name, and no trigger hands its ARIA to a div.

core#1409, found by the core#720 accessibility sweep and still reported by it on every `dev` push:

* ``aria-allowed-attr`` (critical) on ``div[aria-haspopup="dialog"][type="button"]``. Reflex's
  overlay triggers — ``RadixThemesTriggerComponent.create`` — **wrap their children in a ``Flex``
  ``<div>`` whenever a child carries ``on_click``**, and Radix's ``asChild`` then merges the
  trigger's ``type``, ``aria-haspopup`` and ``aria-expanded`` onto that ``div``, which may carry
  none of them. The notification bell did it in the header of every page; the delete-account
  button did it on ``/settings``. Moving the handler onto the trigger itself removes the wrapper:
  the props land on the button, where they are valid, and Radix composes the click.
* ``button-name`` (critical) on icon-only buttons with no ``aria-label`` (the bell, sign out, …)
  **and on unnamed Select triggers** — axe counts ``.rt-SelectTrigger[role="combobox"]`` under
  ``button-name``, which is how the upload wizard reached seven.

🚨 **The trap this file exists for, measured before the fix was written.** ``rx.select(items,
aria_label=…)`` looks like the fix and does nothing: Reflex's high-level select routes an unknown
prop to ``Select.Root``, which renders no element, so the trigger — the element axe checks — stays
unnamed. Only ``custom_attrs`` reaches the trigger. A source-level guard (*"every ``rx.select`` has
``aria_label``"*) would be green on that non-fix, so this one reads the **component tree**, where
each trigger's own attributes are, and a control below proves it tells the two apart.

Names are translated, like any label: an ``aria-label`` is read aloud, so it is copy.
"""

from __future__ import annotations

import importlib
import io
from contextlib import redirect_stdout

import pytest
import reflex as rx
from reflex.components.radix.themes.base import RadixThemesTriggerComponent

from tests.test_ui.test_every_page_constructs import FACTORIES

#: The child an overlay trigger may hand its props to. Anything else — a ``Flex``, a ``Box`` —
#: is a non-interactive element receiving ``type="button"`` and ``aria-*``.
_INTERACTIVE_CHILDREN = frozenset({"Button", "IconButton"})


def _walk(component):
    yield component
    for child in getattr(component, "children", []) or []:
        yield from _walk(child)


def _label(component) -> object:
    return (getattr(component, "custom_attrs", None) or {}).get("aria-label")


def _has_name(component) -> bool:
    """An ``aria-label`` is present, and — when it is a literal — not blank.

    ⚠️ Never ``bool(label)``: a translated name is a Reflex ``Var``, and ``bool()`` on a Var
    raises. A Var here is ``_t[key]``, which the locale-parity test guarantees resolves.
    """
    label = _label(component)
    if label is None:
        return False
    if isinstance(label, str):
        return bool(label.strip())
    return True


def _is_overlay_trigger(component) -> bool:
    return isinstance(component, RadixThemesTriggerComponent) and type(component).__name__.endswith(
        "Trigger"
    )


def trigger_hands_props_to_a_non_control(component) -> bool:
    """The ``aria-allowed-attr`` shape: an overlay trigger whose first child is not a button."""
    first = (component.children or [None])[0]
    return type(first).__name__ not in _INTERACTIVE_CHILDREN


def select_trigger_is_named(component) -> bool:
    return _has_name(component)


def is_icon_only_button(component) -> bool:
    name = type(component).__name__
    if name == "IconButton":
        return True
    return (
        name == "Button"
        and bool(component.children)
        and all(type(child).__name__ == "Icon" for child in component.children)
    )


def _build(module: str, attr: str):
    with redirect_stdout(io.StringIO()):  # Reflex prints icon warnings; not this file's concern
        return getattr(importlib.import_module(f"datanika.ui.pages.{module}"), attr)()


def _census() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {
        "triggers": [],
        "wrapped": [],
        "selects": [],
        "unnamed_selects": [],
        "icon_only": [],
        "unnamed_icon_only": [],
    }
    for module, attr in FACTORIES:
        where = f"{module}.{attr}"
        for component in _walk(_build(module, attr)):
            if _is_overlay_trigger(component):
                found["triggers"].append(where)
                if trigger_hands_props_to_a_non_control(component):
                    kid = type((component.children or [None])[0]).__name__
                    found["wrapped"].append(f"{where}: {type(component).__name__} > {kid}")
            name = type(component).__name__
            if name == "SelectTrigger":
                found["selects"].append(where)
                if not select_trigger_is_named(component):
                    found["unnamed_selects"].append(where)
            if is_icon_only_button(component):
                found["icon_only"].append(where)
                if not _has_name(component):
                    found["unnamed_icon_only"].append(f"{where}: {name}")
    return found


CENSUS = _census()


class TestTheCensusSawTheControls:
    """Anti-vacuity: every assertion below is a "for all", and an empty census satisfies all."""

    def test_it_walked_the_pages(self):
        assert len(FACTORIES) >= 45

    @pytest.mark.parametrize(
        ("kind", "floor"), [("triggers", 30), ("selects", 20), ("icon_only", 10)]
    )
    def test_it_found_enough_of_each_control(self, kind, floor):
        assert len(CENSUS[kind]) >= floor, (kind, len(CENSUS[kind]))


class TestEveryControlIsNamed:
    def test_no_overlay_trigger_hands_its_props_to_a_div(self):
        """``aria-allowed-attr``. Put the handler on the trigger, not on its child."""
        assert not sorted(set(CENSUS["wrapped"])), sorted(set(CENSUS["wrapped"]))

    def test_every_select_trigger_has_a_name(self):
        """``button-name``. ``custom_attrs={"aria-label": _t[...]}`` — never ``aria_label=``."""
        assert not sorted(set(CENSUS["unnamed_selects"])), sorted(set(CENSUS["unnamed_selects"]))

    def test_every_icon_only_button_has_a_name(self):
        """``button-name``. ``aria_label=_t[...]``, the key of what the button does."""
        assert not sorted(set(CENSUS["unnamed_icon_only"])), sorted(
            set(CENSUS["unnamed_icon_only"])
        )


class TestTheChecksTellTheShapesApart:
    """Each check against the shape that fooled us and the shape that fixes it — through the SAME
    functions the page census uses, so a check that cannot fail is caught here."""

    def test_a_root_level_aria_label_does_not_name_the_trigger(self):
        root_only = rx.select(["a"], aria_label="Status")
        on_trigger = rx.select(["a"], custom_attrs={"aria-label": "Status"})
        trigger_of = lambda select: next(  # noqa: E731
            c for c in _walk(select) if type(c).__name__ == "SelectTrigger"
        )
        assert not select_trigger_is_named(trigger_of(root_only))
        assert select_trigger_is_named(trigger_of(on_trigger))

    def test_a_handler_on_the_child_is_what_wraps_it(self):
        event = rx.console_log("clicked")
        on_child = rx.popover.trigger(rx.icon_button(rx.icon("bell"), on_click=event))
        on_trigger = rx.popover.trigger(rx.icon_button(rx.icon("bell")), on_click=event)
        assert trigger_hands_props_to_a_non_control(on_child)
        assert not trigger_hands_props_to_a_non_control(on_trigger)

    def test_an_icon_only_button_is_told_from_a_labelled_one(self):
        assert is_icon_only_button(rx.button(rx.icon("pencil")))
        assert is_icon_only_button(rx.icon_button(rx.icon("x")))
        assert not is_icon_only_button(rx.button("Delete"))
