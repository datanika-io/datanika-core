"""SPEC_FIELD_REQUIREDNESS — requiredness is DERIVED, and the marker follows it (core#1311 slice).

landing#572 item 2 is what it looked like from outside: ``openapi_fields()`` rendered "Base URL *" —
the asterisk baked into the translated string — while passing no ``required`` to the input. So the
form told a sighted user the field was mandatory and told a screen reader it was not (§1c). That
is the root defect, and AC5 is what closes it: the marker and the HTML attribute are one value.

The first slice was ``connections.base_url`` + ``connections.name`` (AC3). ``name`` is single-site
and always required; ``base_url`` is rendered by two connectors and required by only one of them.
So the pair exercises a derived marker in BOTH directions — a mechanism that only ever renders
``*`` would pass a ``name``-only test. Slice 2 added ``connections.port``; slice 3 adds
``account``, ``bucket_url``, ``database``, ``dataset`` and ``gcp_project`` — see ``_SLICE``.

⚠️ Slice 3's seven sites were NOT self-contradictory before the change: the marker was baked into
the translation and the input already carried ``required=True``, so AC5's marker-equals-attribute
assertion was already satisfied at every one of them. What was wrong is §2.2 — the marker lived
inside a string that cannot vary by connector and is tied to nothing. So the test that goes red
against the unfixed tree is :class:`TestTheTranslatedLabelsCarryNoMarker`, and AC5's value is that
it goes red if the string is stripped WITHOUT the call site being converted. The two halves are
coupled, and each is the other's guard.

⚠️ No assertion here is "the string omits an asterisk" on its own. That is satisfied by deleting the
label (the spec's AC2 warning), so every such check is paired with the presence of what should be
there.
"""

import ast
import inspect
import re
from pathlib import Path

import pytest

from datanika.i18n import SUPPORTED_LOCALES, get_translations
from datanika.models.connection import ConnectionType
from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.ui.components.connection_config_fields import (
    bigquery_fields,
    db_fields,
    mongodb_fields,
    openapi_fields,
    rest_api_fields,
    s3_fields,
    snowflake_fields,
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
#:
#: Slice 3 adds the five keys whose EVERY render site is gated by ``_validate_connection_form``:
#: ``account`` (:328), ``bucket_url`` (:335), ``database`` (:317/:332/:343), ``dataset`` (:325),
#: ``gcp_project`` (:323). Seven sites, every one required, so §2.7's question has a single answer
#: per key and no per-connector Product ruling is needed. The remaining four keys (``host``,
#: ``db_path``, ``http_path``, ``token``) each touch a site the validator has NO branch for, and
#: are deliberately not here — see core#1547.
#:
#: ⚠️ **TWO members carry ``(False, False)``** — ``(openapi_fields, "base_url")`` and
#: ``(mongodb_fields, "port")``. Slice 3 adds seven ``(True, True)`` members, so AC3's
#: both-directions property rests on exactly those two.
#:
#: 🔑 Measured by mutation, because the obvious reading is wrong in a way that matters: dropping
#: EITHER one leaves ``test_the_slice_exercises_both_directions`` **green**, and is caught only by
#: :class:`TestTheSliceCoversEveryDerivedSite`. Only dropping both reds it. So neither member is
#: redundant and neither is sufficient: **either deletion on its own is invisible to AC3's test**,
#: and it is the pair that stands between this slice and an all-required population — which is
#: precisely the population a marker-always mechanism passes.
_SLICE = [
    (openapi_fields, "base_url"),
    (rest_api_fields, "base_url"),
    (connection_form, "name"),
    (db_fields, "port"),
    (mongodb_fields, "port"),
    (snowflake_fields, "account"),
    (s3_fields, "bucket_url"),
    (db_fields, "database"),
    (snowflake_fields, "database"),
    (mongodb_fields, "database"),
    # ``gcp_project`` is the i18n key; ``project`` is the field the input is named for. The
    # guard matches on the input's name, so the two must not be conflated here.
    (bigquery_fields, "project"),
    (bigquery_fields, "dataset"),
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
    """AC5 — and 🔑 **this class becomes unfalsifiable at every site that adopts the helper.**

    Measured by mutation, 2026-09-23: marking MongoDB's Port `required=True` — restoring exactly
    the §1c contradiction this slice removes — leaves every test in this class **green**.
    `labelled_config_input` derives the marker *and* the attribute from one value, so they agree
    **by construction**; there is no longer a pair that can disagree.

    That is not a gap, it is the fix working, and the distinction matters for what guards what:

    * AC5 was the right test for the **pre-helper** world. It caught the original defect
      (`openapi_fields` rendering "Base URL *" over an input with no `required`), and it still
      guards every call site that has **not** been migrated — which is most of them, since ten
      ` *` keys remain.
    * Once a site is derived, the only remaining question is whether the derived **value** is
      correct. That is :class:`TestTheFormAndTheSchema`'s question, and it is what actually
      caught the mutation above.

    ⚠️ **So do not read this class's green as covering a migrated field.** Conflating the two
    leaves a slice looking guarded when only half of it is.
    """

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


def _derived_sites() -> set[tuple[str, str]]:
    """``(builder name, field)`` for every ``labelled_config_input`` call on the connection form.

    Read from the SOURCE of the modules the slice's builders live in, never from ``_SLICE`` — an
    expectation computed by the thing under test is satisfied by that thing doing nothing.
    """
    sites: set[tuple[str, str]] = set()
    for module in {inspect.getmodule(builder) for builder, _ in _SLICE}:
        tree = ast.parse(Path(inspect.getfile(module)).read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef):
                continue
            for node in ast.walk(fn):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "labelled_config_input"
                    and len(node.args) >= 2
                    and isinstance(node.args[1], ast.Constant)
                ):
                    sites.add((fn.name, node.args[1].value))
    return sites


class TestTheSliceCoversEveryDerivedSite:
    """🚨 A scanning guard that stops seeing a site does not fail.

    Every assertion in this file is parametrized over ``_SLICE``, so a migrated field that is
    never added — or one quietly dropped — is simply not asserted, and the file stays green while
    covering less. Nothing else in the suite would say so: the marker is derived, so the site
    cannot contradict itself, and the AC2 ratchet only walks ``COMPARED``.

    The population is therefore derived from the source and required to EQUAL ``_SLICE``, which
    makes the next slice's author add their sites here rather than discover the gap later.
    """

    def test_the_slice_is_exactly_the_set_of_derived_sites(self):
        derived = _derived_sites()
        declared = {(builder.__name__, field) for builder, field in _SLICE}
        assert declared == derived, (
            f"_SLICE and the call sites disagree.\n"
            f"  rendered but not asserted: {sorted(derived - declared)}\n"
            f"  asserted but not rendered: {sorted(declared - derived)}\n"
            "A field migrated to labelled_config_input must be added to _SLICE; one removed from "
            "the form must be dropped from it."
        )

    def test_the_scanner_can_see_a_site(self):
        """Anti-vacuity: an empty scan makes the equality above assert ``set() == set()``.

        ``_SLICE`` could then be emptied and every test in this file would pass, having graded
        nothing — the shape that let a locale test pass while reading zero locales (core#1551).
        """
        derived = _derived_sites()
        assert len(derived) >= len(_SLICE) >= 5, (
            f"the AST scan found {len(derived)} call sites for a slice of {len(_SLICE)}; it has "
            "stopped matching the call shape and the equality above proves nothing"
        )
        assert ("connection_form", "name") in derived, (
            "the scan cannot see the first migrated site, which is in a different module from "
            "the rest — so it is reading only one of the two files it must read"
        )


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
    # ``Dataset`` is the term the de/fr/es translators chose, and it is what Google's own
    # BigQuery console shows in those locales. Measured before this change: all three read
    # ``"Dataset *"``, i.e. they already matched English apart from the marker, so the marker is
    # the only thing this slice removed from them. No translation is edited here — changing a
    # translated WORD is a content decision, not part of deriving a marker.
    ("connections.dataset", "de"),
    ("connections.dataset", "es"),
    ("connections.dataset", "fr"),
}


class TestTheTranslatedLabelsCarryNoMarker:
    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    @pytest.mark.parametrize(
        "key",
        [
            "connections.base_url",
            "connections.name",
            "connections.port",
            "connections.account",
            "connections.bucket_url",
            "connections.database",
            "connections.dataset",
            "connections.gcp_project",
        ],
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
        # Slice 3. Six of its seven sites; `s3`'s Bucket URL is the seventh and is UNCOMPARABLE
        # below, because `s3` has no schema at all.
        ("snowflake", snowflake_fields, "account"),
        ("postgres", db_fields, "database"),
        ("snowflake", snowflake_fields, "database"),
        ("mongodb", mongodb_fields, "database"),
        ("bigquery", bigquery_fields, "project"),
        ("bigquery", bigquery_fields, "dataset"),
    )

    #: Connection types the ratchet CANNOT compare, because they have no ``CONFIG_SCHEMAS`` entry.
    #:
    #: 🚨 ``s3`` is the one member of 37 in ``ConnectionType`` with no schema — measured with the
    #: enum as the control, 36 of 37 present. Its Bucket URL is form-required
    #: (``_validate_connection_form:335``) and has no schema side to compare against, so adding it
    #: to ``COMPARED`` raises ``KeyError`` rather than reporting a difference.
    #:
    #: It is named here rather than quietly left out: an instrument that cannot see part of its
    #: population otherwise reports that part as clean, and a site missing from ``COMPARED`` looks
    #: identical to a site that agrees.
    UNCOMPARABLE = {"s3"}

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

    def test_the_types_it_cannot_compare_are_named_and_still_uncomparable(self):
        """A site the ratchet cannot see must be named, not omitted.

        Two-way, like ``KNOWN_DIFFERENCES``: if ``s3`` gains a ``CONFIG_SCHEMAS`` entry this
        fails, and the correct repair is to move the site into ``COMPARED`` — not to widen this
        set. If it is still absent, the site stays visibly out of scope instead of being
        indistinguishable from one that agrees.
        """
        unknown = self.UNCOMPARABLE - {m.value for m in ConnectionType}
        assert not unknown, (
            f"{sorted(unknown)} is not a ConnectionType: this set names connectors, and a typo "
            "here silently excuses nothing at all"
        )
        absent = {t for t in self.UNCOMPARABLE if t not in CONFIG_SCHEMAS}
        assert absent == self.UNCOMPARABLE, (
            f"{sorted(self.UNCOMPARABLE - absent)} now has a CONFIG_SCHEMAS entry — compare its "
            "form requiredness against that schema in COMPARED and drop it from UNCOMPARABLE"
        )
        # Control: the membership test above proves nothing unless it can also answer True.
        compared_types = {t for t, _, _ in self.COMPARED}
        assert compared_types, "COMPARED is empty, so the control below cannot fire"
        schemaless = compared_types - set(CONFIG_SCHEMAS)
        assert not schemaless, f"COMPARED names types with no schema: {sorted(schemaless)}"


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
