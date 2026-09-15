"""SPEC_FIELD_REQUIREDNESS — requiredness is DERIVED, and the marker follows it (core#1311 slice).

landing#572 item 2 is what it looked like from outside: ``openapi_fields()`` rendered "Base URL *" —
the asterisk baked into the translated string — while passing no ``required`` to the input. So the
form told a sighted user the field was mandatory and told a screen reader it was not (§1c). That
is the root defect, and AC5 is what closes it: the marker and the HTML attribute are one value.

The slice is ``connections.base_url`` + ``connections.name`` (AC3). ``name`` is single-site and
always required; ``base_url`` is rendered by two connectors and required by only one of them. So
the pair exercises a derived marker in BOTH directions — a mechanism that only ever renders ``*``
would pass a ``name``-only test.

⚠️ No assertion here is "the string omits an asterisk" on its own. That is satisfied by deleting the
label (the spec's AC2 warning), so every such check is paired with the presence of what should be
there.
"""

import re

import pytest

from datanika.i18n import SUPPORTED_LOCALES, get_translations
from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.ui.components.connection_config_fields import openapi_fields, rest_api_fields
from datanika.ui.pages.connections import connection_form

EN = get_translations("en")
_KEY = re.compile(r'\["(connections\.[a-z0-9_]+)"\]')

#: (builder, field) — the slice, in the order the two directions are asserted.
_SLICE = [
    (openapi_fields, "base_url"),
    (rest_api_fields, "base_url"),
    (connection_form, "name"),
]


def _children(node) -> list:
    return node.get("children", []) if isinstance(node, dict) else []


def _walk(node):
    yield node
    for child in _children(node):
        yield from _walk(child)


def _props(node) -> list[str]:
    return node.get("props", []) if isinstance(node, dict) else []


def _is_input_for(node, field: str) -> bool:
    return f'name:"cfg-{field.replace("_", "-")}"' in _props(node)


def _is_label(node) -> bool:
    return isinstance(node, dict) and node.get("name") == "RadixThemesText"


class _Pair:
    """A rendered label and the input it labels, as a user and a screen reader each meet them."""

    def __init__(self, key: str, marker: bool, required: bool):
        self.key, self.marker, self.required = key, marker, required

    def __repr__(self) -> str:
        return f"<{self.key}: marker={self.marker} required={self.required}>"


def _label_state(label: dict) -> tuple[str, bool]:
    """(the label's i18n key, whether a sighted user sees a required marker on it)."""
    keys = _KEY.findall(str(label))
    assert len(keys) == 1, f"expected one translated label, found {keys}: {str(label)[:200]}"
    key = keys[0]
    # A marker is visible either as its own rendered text, or baked into the translation.
    rendered = any(
        "*" in str(node.get("contents", "")) and "translations" not in str(node.get("contents", ""))
        for node in _walk(label)
        if isinstance(node, dict)
    )
    baked = EN.get(key, "").rstrip().endswith("*")
    return key, rendered or baked


def _pair(component, field: str) -> _Pair:
    """The label rendered immediately before ``field``'s input, in the same container."""
    for node in _walk(component.render()):
        kids = _children(node)
        for i, kid in enumerate(kids):
            if _is_input_for(kid, field):
                label = next((k for k in reversed(kids[:i]) if _is_label(k)), None)
                assert label is not None, f"no label precedes the cfg-{field} input"
                key, marker = _label_state(label)
                return _Pair(key, marker, "required:true" in _props(kid))
    raise AssertionError(f"no input named cfg-{field} is rendered")


class TestTheMarkerAndTheAttributeAreOneValue:
    @pytest.mark.parametrize(
        "builder, field",
        [pytest.param(b, f, id=f"{b.__name__}-{f}") for b, f in _SLICE],
    )
    def test_the_marker_and_the_required_attribute_agree(self, builder, field):
        """AC5. Red before the fix on ``openapi_fields``: "Base URL *" over an input with no
        ``required`` — mandatory to the eye, optional to assistive technology (§1c)."""
        pair = _pair(builder(), field)
        assert pair.marker == pair.required, (
            f"{builder.__name__} renders {pair!r}: the label and the HTML attribute disagree, so a "
            "sighted user and a screen-reader user are told opposite things about one field"
        )

    def test_the_slice_exercises_both_directions(self):
        """AC3. Without an unmarked, unrequired field in the slice, a mechanism that only ever
        renders the marker would satisfy every other assertion in this file."""
        states = {(p.marker, p.required) for p in (_pair(b(), f) for b, f in _SLICE)}
        assert (True, True) in states, f"no marked, required field in the slice: {states}"
        assert (False, False) in states, f"no unmarked, optional field in the slice: {states}"


class TestTheTranslatedLabelsCarryNoMarker:
    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    @pytest.mark.parametrize("key", ["connections.base_url", "connections.name"])
    def test_the_label_is_a_name_not_a_sentence_about_the_form(self, locale, key):
        """AC1 for this slice (§2.2) — paired with presence, so deleting the label cannot pass."""
        value = get_translations(locale).get(key, "")
        assert value.strip(), f"{locale}: {key} is missing or empty"
        assert not value.rstrip().endswith("*"), (
            f"{locale}: {key} = {value!r} carries the marker inside the translation, where it "
            "cannot vary by connector and nothing ties it to the input's required attribute"
        )
        if locale != "en":
            assert value != EN[key], f"{locale}: {key} is the English string"


class TestTheFormAndTheSchema:
    """AC2 — derived on BOTH sides and compared, never inferred from a string.

    The form's requiredness is read off the rendered input; the schema's off ``CONFIG_SCHEMAS``.
    """

    #: (connection type, field) -> why the form and the stored-config schema legitimately differ.
    #: A ratchet: the observed set must EQUAL this, so a new difference fails, and so does this
    #: one disappearing without its entry being removed.
    KNOWN_DIFFERENCES = {
        ("openapi", "base_url"): (
            "The form leaves Base URL optional: a blank value saves, and connection_state.py "
            "backfills it from the spec's `servers` entry. CONFIG_SCHEMAS describes the STORED "
            "config, which always carries base_url after that backfill, so it lists the field as "
            "required. Which of the two the marker should follow is Product's decision on "
            "core#1311, not this markup's."
        ),
    }

    def test_the_form_follows_the_schema_except_where_recorded(self):
        observed = set()
        for conn_type, builder in (("rest_api", rest_api_fields), ("openapi", openapi_fields)):
            form_required = _pair(builder(), "base_url").required
            schema_required = "base_url" in CONFIG_SCHEMAS[conn_type]["required"]
            if form_required != schema_required:
                observed.add((conn_type, "base_url"))
        assert observed == set(self.KNOWN_DIFFERENCES), (
            f"form/schema differences observed: {sorted(observed)}; recorded: "
            f"{sorted(self.KNOWN_DIFFERENCES)}. A new difference is a requiredness question for "
            "Product (SPEC_FIELD_REQUIREDNESS §4); a vanished one means the record is stale."
        )


class TestTheFormExplainsTheMarkerOnce:
    def test_the_legend_is_rendered_exactly_once(self):
        """§2.6 — one legend, not a per-field explanation."""
        rendered = str(connection_form().render())
        assert rendered.count('"connections.required_legend"') == 1

    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    def test_the_legend_is_translated_and_carries_no_glyph_of_its_own(self, locale):
        value = get_translations(locale).get("connections.required_legend", "")
        assert value.strip(), f"{locale}: connections.required_legend is missing or empty"
        assert "*" not in value, (
            "the marker glyph is rendered beside the legend, never inside the translation (§2.4)"
        )
        if locale != "en":
            assert value != EN.get("connections.required_legend"), f"{locale}: English string"
