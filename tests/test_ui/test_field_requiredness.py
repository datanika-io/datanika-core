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
from datanika.ui.components.connection_config_fields import (
    db_fields,
    mongodb_fields,
    openapi_fields,
    rest_api_fields,
)
from datanika.ui.pages.connections import connection_form

EN = get_translations("en")
_KEY = re.compile(r'\["(connections\.[a-z0-9_]+)"\]')

#: (builder, field) — the slice, in the order the two directions are asserted.
#:
#: Slice 2 adds ``connections.port`` (core#1311 AC4). It has the same both-directions property
#: AC3 requires, and it is the one shared key whose contradiction is fully measured: the seven
#: ``_DB_TYPES`` connectors are refused a blank port by ``_validate_connection_form``, MongoDB is
#: not — its branch checks host and database and deliberately omits port, because MongoDB has a
#: real default port and the connector works without one.
_SLICE = [
    (openapi_fields, "base_url"),
    (rest_api_fields, "base_url"),
    (connection_form, "name"),
    (db_fields, "port"),
    (mongodb_fields, "port"),
]


def _children(node) -> list:
    """Every child, **including both branches of an ``rx.cond``**.

    🚨 Widened for slice 2, and the widening is the point. A rendered cond does not put its
    branches in ``children`` — it exposes them as ``true_value`` / ``false_value`` — so this used
    to return ``[]`` for one, and **every conditionally-rendered field was outside this guard's
    population**. MongoDB's Port is exactly such a field (it is hidden when a DNS seed list is
    used, which takes no port), and it is the shared key whose label and attribute contradict.

    ``_pair`` **raises** when it finds no input, so this failed loudly rather than passing — but
    the same blind spot on a *scanning* assertion would have read as clean. Both branches are
    followed because a field is a field whichever arm renders it.
    """
    if not isinstance(node, dict):
        return []
    out = list(node.get("children", []))
    for arm in ("true_value", "false_value"):
        branch = node.get(arm)
        if isinstance(branch, dict):
            out.append(branch)
        elif isinstance(branch, list):
            out.extend(branch)
    return out


def _walk(node):
    yield node
    for child in _children(node):
        yield from _walk(child)


def _props(node) -> list[str]:
    return node.get("props", []) if isinstance(node, dict) else []


def _is_input_for(node, field: str) -> bool:
    return f'name:"cfg-{field.replace("_", "-")}"' in _props(node)


def _is_label(node) -> bool:
    """The label a user reads immediately before the input.

    🔁 Repointed for core#720, not relaxed. This matched only a bare ``RadixThemesText`` sibling —
    which is exactly the shape #720 is about: text beside an input gives it no accessible name. The
    connector labels are now ``<label htmlFor="cfg-…">`` elements wrapping that same text, so the
    requiredness invariant below is asserted on them unchanged, and ``_pair`` additionally checks
    that a ``<label>`` found this way is bound to THIS input.
    """
    return isinstance(node, dict) and node.get("name") in {"RadixThemesText", '"label"'}


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
                if label.get("name") == '"label"':
                    bound = f'htmlFor:"cfg-{field.replace("_", "-")}"'
                    assert bound in _props(label), (
                        f"the <label> before the cfg-{field} input names another input: "
                        f"{_props(label)}"
                    )
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


#: (key, locale) pairs whose translation legitimately EQUALS the English string, because the word
#: is the same in that language. Each entry is a claim about the language, not a licence.
#:
#: The ``value != EN[key]`` check below is a proxy for *"this locale was translated rather than
#: copy-pasted"*, and it is a good proxy for a sentence. It is a **false positive for a loanword**,
#: and `connections.port` is one: `de` and `fr` both use "Port" for a network port, and a
#: translator chose it in each — the marker was the only thing this change removed.
#:
#: 🔑 The entry is a two-way ratchet. If one of these locales later diverges from English, the
#: test fails **for having a stale entry**, so this set cannot quietly accumulate exemptions —
#: which is the failure mode of every allowlist (`WORKFLOW_RULES` §5a: repoint a guard at its
#: invariant, never relax it).
_SAME_WORD_AS_ENGLISH = {
    ("connections.port", "de"),
    ("connections.port", "fr"),
}


class TestTheTranslatedLabelsCarryNoMarker:
    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    @pytest.mark.parametrize(
        "key", ["connections.base_url", "connections.name", "connections.port"]
    )
    def test_the_label_is_a_name_not_a_sentence_about_the_form(self, locale, key):
        """AC1 for this slice (§2.2) — paired with presence, so deleting the label cannot pass."""
        value = get_translations(locale).get(key, "")
        assert value.strip(), f"{locale}: {key} is missing or empty"
        assert not value.rstrip().endswith("*"), (
            f"{locale}: {key} = {value!r} carries the marker inside the translation, where it "
            "cannot vary by connector and nothing ties it to the input's required attribute"
        )
        if locale != "en":
            if (key, locale) in _SAME_WORD_AS_ENGLISH:
                assert value == EN[key], (
                    f"{locale}: {key} no longer matches English, so its entry in "
                    "_SAME_WORD_AS_ENGLISH is stale — remove it rather than leaving a dead "
                    "exemption behind"
                )
            else:
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
            "required. Ruled by Product 2026-09-15 on core#1311: the marker follows the FORM."
        ),
        ("postgres", "port"): (
            "The form REQUIRES a port and the stored-config schema does not — the difference "
            "points the opposite way to openapi's, which is why recording it is worth more than "
            "the string fix that surfaced it. `_validate_connection_form` refuses a blank port "
            "for every `_DB_TYPES` connector, while CONFIG_SCHEMAS omits `port` from `required` "
            "because it carries a default (5432). Neither side is wrong: §2.7 says the marker "
            "describes what THIS FORM asks of the person, and this form asks. Changing the "
            "validator to match the schema would change which fields are required, which §4 "
            "puts out of scope."
        ),
    }

    #: (connection type, builder, field) — every pair this ratchet compares. MongoDB's port is
    #: here precisely because it is NOT expected to differ: form optional, schema optional. An
    #: agreement is only evidence when the instrument could have reported a disagreement.
    COMPARED = (
        ("rest_api", rest_api_fields, "base_url"),
        ("openapi", openapi_fields, "base_url"),
        ("postgres", db_fields, "port"),
        ("mongodb", mongodb_fields, "port"),
    )

    def test_the_form_follows_the_schema_except_where_recorded(self):
        observed = set()
        for conn_type, builder, field in self.COMPARED:
            form_required = _pair(builder(), field).required
            schema_required = field in CONFIG_SCHEMAS[conn_type]["required"]
            if form_required != schema_required:
                observed.add((conn_type, field))
        assert observed == set(self.KNOWN_DIFFERENCES), (
            f"form/schema differences observed: {sorted(observed)}; recorded: "
            f"{sorted(self.KNOWN_DIFFERENCES)}. A new difference is a requiredness question for "
            "Product (SPEC_FIELD_REQUIREDNESS §4); a vanished one means the record is stale."
        )

    def test_the_comparison_sees_both_answers(self):
        """Anti-vacuity for the ratchet itself.

        A comparison that only ever visits pairs which differ, or only pairs which agree, cannot
        distinguish a working ratchet from a broken one. This requires the set it walks to
        contain at least one of each.
        """
        verdicts = set()
        for conn_type, builder, field in self.COMPARED:
            verdicts.add(
                _pair(builder(), field).required == (field in CONFIG_SCHEMAS[conn_type]["required"])
            )
        assert verdicts == {True, False}, (
            f"the ratchet's population yields only {verdicts}; it cannot tell agreement from "
            "disagreement and its verdict says nothing"
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
