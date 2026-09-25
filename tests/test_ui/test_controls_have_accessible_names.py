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
* ``label`` (critical) on the ``<input type="file">`` inside every ``rx.upload`` (core#1568). It was
  the last blocking node in the sweep, and the same trap as the select above one level deeper:
  ``rx.upload`` declares **eleven** props and ``aria_label`` is not among them, so an unknown prop
  lands on the wrapper ``Box`` — as does ``id=``, which is why ``html_for`` cannot reach the input
  either. The input is built inside Reflex as a bare ``Input.create(type="file")``, so
  ``datanika/ui/components/file_upload.py`` is the only place that can name it.

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

from datanika.ui.components.file_upload import named_upload
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


def is_file_input(component) -> bool:
    """The ``<input type="file">`` that ``rx.upload`` hides inside its dropzone.

    ⚠️ ``str(kind)`` and never ``kind or ""``: ``type`` is a Reflex ``Var`` and ``bool()`` on one
    raises — the same hazard ``_has_name`` above is written around. It cost a probe run to find.
    """
    if getattr(component, "tag", None) != "input":
        return False
    kind = getattr(component, "type", None)
    return kind is not None and "file" in str(kind)


def file_input_is_named(component) -> bool:
    return _has_name(component)


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
        "file_inputs": [],
        "unnamed_file_inputs": [],
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
            if is_file_input(component):
                found["file_inputs"].append(where)
                if not file_input_is_named(component):
                    found["unnamed_file_inputs"].append(where)
    return found


CENSUS = _census()


class TestTheCensusSawTheControls:
    """Anti-vacuity: every assertion below is a "for all", and an empty census satisfies all."""

    def test_it_walked_the_pages(self):
        assert len(FACTORIES) >= 45

    @pytest.mark.parametrize(
        ("kind", "floor"),
        [("triggers", 30), ("selects", 20), ("icon_only", 10), ("file_inputs", 3)],
    )
    def test_it_found_enough_of_each_control(self, kind, floor):
        assert len(CENSUS[kind]) >= floor, (kind, len(CENSUS[kind]))

    def test_it_reached_every_upload_in_the_product(self):
        """``file_inputs`` is floored at 3 against **6** measured (2026-09-25): three ``rx.upload``
        sites, each seen twice because ``FACTORIES`` lists a sub-factory *and* its whole page.

        🔑 **This walk sees a control the axe sweep cannot.** Reflex builds *both* branches of an
        ``rx.cond``, so the csv/json/parquet upload inside ``connection_form`` is in this tree even
        though the swept connection form renders PostgreSQL. core#1568 was filed on one node because
        one node is all axe could score; there were three, and the other two were **never measured**
        rather than clean. The floor is deliberately below 6 so deleting one upload page is not a
        false red, and the site set below is what actually pins the population.
        """
        assert {where.split(".")[0] for where in CENSUS["file_inputs"]} == {
            "connections",
            "settings",
            "transformations",
        }, sorted(set(CENSUS["file_inputs"]))


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

    def test_every_file_input_has_a_name(self):
        """``label`` (core#1568). Build uploads with ``named_upload`` — never bare ``rx.upload``.

        The visible ``rx.button`` beside the input is a **sibling**, not a label, so it names
        nothing; and neither ``aria_label=`` nor ``id=`` on ``rx.upload`` reaches the input, because
        both land on the wrapper ``Box``.
        """
        assert not sorted(set(CENSUS["unnamed_file_inputs"])), sorted(
            set(CENSUS["unnamed_file_inputs"])
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

    def test_an_upload_level_aria_label_does_not_name_the_file_input(self):
        """core#1568's trap, driven with all three populations.

        The middle case is the one that matters: it is what a careful author writes, it reads as a
        fix in a diff, and the name lands on the wrapper ``<div>`` while the input stays anonymous.
        A source-level guard (*"every rx.upload has aria_label"*) would be green on it.
        """
        bare = rx.upload(rx.button("Import"), accept={".json": ["application/json"]})
        looks_fixed = rx.upload(
            rx.button("Import"), accept={".json": ["application/json"]}, aria_label="Import file"
        )
        actually_fixed = named_upload(
            rx.button("Import"),
            accessible_name="Import file",
            accept={".json": ["application/json"]},
        )
        input_of = lambda upload: next(c for c in _walk(upload) if is_file_input(c))  # noqa: E731

        assert not file_input_is_named(input_of(bare))
        assert not file_input_is_named(input_of(looks_fixed)), (
            "aria_label= on rx.upload named the file input, so this control can no longer tell a "
            "fix from a non-fix. Re-derive where Upload forwards undeclared props."
        )
        assert file_input_is_named(input_of(actually_fixed))

    def test_the_finder_does_not_mistake_a_text_input_for_a_file_one(self):
        """``is_file_input`` selects on ``type``, so a guard that matched every ``<input>`` would
        drag all 153 baselined text inputs into this file's census and report them unnamed."""
        assert not is_file_input(rx.el.input(type="text", id="x"))
        assert not is_file_input(rx.el.input(id="x"))

    def test_named_upload_refuses_the_prop_that_looks_like_the_fix(self):
        """Passing ``aria_label`` to the helper is the mistake above; it must not pass silently."""
        with pytest.raises(TypeError, match="aria_label"):
            named_upload(rx.button("Import"), accessible_name="Import file", aria_label="Import")
