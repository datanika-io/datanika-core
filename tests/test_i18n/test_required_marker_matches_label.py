"""A label may not say "optional" while wearing the required marker (core#822).

``i18n/en.json`` held one string, ``connections.api_key`` = *"API Key
(optional)"*, and **7 of the 9** connector forms that render it append ``" *"``.
So the Shopify form said:

    API Key (optional) *

Simultaneously optional and required — observed live on production, 2026-08-31.
For Shopify the key is genuinely mandatory (``DltRunner`` raises *"Shopify
source requires 'api_key' and 'store'"*, and the input is rendered
``required=True``), so a user who believed the label and left it blank got a
form that refuses to submit, or a run that fails later, for a field the label
told them to skip.

Two connectors — ``rest_api`` and ``openapi`` — are the reason the string was
worded that way: an unauthenticated endpoint is a real use case, and the
production ``githubpublicapi`` connection has an empty key. One string cannot
serve both, so the key was split.

**This guard is deliberately about the class, not about ``api_key``.** It scans
every ``rx.text`` label in the UI, so the next label that gains a required
marker over an optional-worded string fails here rather than on a user's screen.
"""

import ast
import json
from pathlib import Path

import pytest

import datanika.ui
from datanika.i18n import SUPPORTED_LOCALES, get_translations

UI_ROOT = Path(datanika.ui.__file__).parent
EN = get_translations("en")

# The required marker as it is written throughout most of the UI: a second
# positional argument to rx.text, next to the translated label.
REQUIRED_MARKER = "*"

# SPEC_FIELD_REQUIREDNESS (core#1311) moves labels into helpers that take
# requiredness ONCE and render both the marker and the input attribute from it.
# For these the site's requiredness is the literal `required=` keyword. Without
# this set, a label migrated into a helper silently stops being scanned — a
# scanning guard does not fail when a site leaves its view (ENGINEERING_RULES §54).
DERIVED_MARKER_HELPERS = {"labelled_config_input", "field_label"}

# A scan that silently finds nothing passes every assertion below it. Both
# counts are pinned so an extractor that stops matching fails loudly instead
# of reporting a clean sweep of zero labels.
MIN_LABEL_SITES = 60
MIN_REQUIRED_SITES = 20


def _t_key(arg) -> str | None:
    """The key of a ``_t["some.key"]`` expression, or ``None``."""
    if (
        isinstance(arg, ast.Subscript)
        and isinstance(arg.value, ast.Name)
        and arg.value.id == "_t"
        and isinstance(arg.slice, ast.Constant)
        and isinstance(arg.slice.value, str)
    ):
        return arg.slice.value
    return None


def _label_sites() -> list[tuple[str, int, str, bool]]:
    """(file, lineno, i18n key, carries the required marker) for every label.

    Two call shapes: ``rx.text(_t[key], " *", ...)``, and a derived-marker helper called as
    ``helper(_t[key], ..., required=<literal>)``.
    """
    sites: list[tuple[str, int, str, bool]] = []
    for path in sorted(UI_ROOT.rglob("*.py")):
        rel = str(path.relative_to(UI_ROOT))
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func

            helper = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if helper in DERIVED_MARKER_HELPERS:
                key = _t_key(node.args[0]) if node.args else None
                required = next(
                    (
                        kw.value.value
                        for kw in node.keywords
                        if kw.arg == "required"
                        and isinstance(kw.value, ast.Constant)
                        and isinstance(kw.value.value, bool)
                    ),
                    False,
                )
                if key is not None:
                    sites.append((rel, node.lineno, key, required))
                continue

            if not (
                isinstance(func, ast.Attribute)
                and func.attr == "text"
                and isinstance(func.value, ast.Name)
                and func.value.id == "rx"
            ):
                continue

            key = None
            marked = False
            for arg in node.args:
                if _t_key(arg) is not None:
                    key = _t_key(arg)
                elif (
                    isinstance(arg, ast.Constant)
                    and isinstance(arg.value, str)
                    and REQUIRED_MARKER in arg.value
                ):
                    marked = True
            if key is not None:
                sites.append((rel, node.lineno, key, marked))
    return sites


SITES = _label_sites()
REQUIRED_SITES = [s for s in SITES if s[3]]

# Words that mean "optional" in the nine shipped locales. Only ``en`` is
# asserted against below — the others exist so a future contributor extending
# this guard has the list rather than re-deriving it.
OPTIONAL_WORDS_EN = ("optional",)


class TestTheScannerIsArmed:
    """Every assertion in this file is vacuous if the scan finds nothing."""

    def test_it_found_labels(self):
        assert len(SITES) >= MIN_LABEL_SITES, (
            f"only {len(SITES)} rx.text labels found — the extractor has stopped "
            "matching the codebase, so the guards below prove nothing"
        )

    def test_it_found_required_markers(self):
        assert len(REQUIRED_SITES) >= MIN_REQUIRED_SITES, (
            f"only {len(REQUIRED_SITES)} required markers found; the ' *' "
            "convention has changed and this guard no longer sees it"
        )

    def test_every_scanned_key_exists(self):
        missing = sorted({key for _, _, key, _ in SITES if key not in EN})
        assert not missing, f"labels reference keys absent from en.json: {missing}"


class TestTheScannerSeesTheDerivedMarker:
    """SPEC_FIELD_REQUIREDNESS moves labels off ``rx.text(_t[...], " *")`` and into a helper that
    takes ``required=`` once and renders both the marker and the input attribute (core#1311).

    🚨 A scanner that only knows the old call shape does not fail when a label moves — it simply
    stops counting that site, and the guard above goes quietly blind one field at a time
    (ENGINEERING_RULES §54). So the migrated sites are asserted by key AND by requiredness,
    in both directions.
    """

    def test_helper_rendered_labels_are_sites_carrying_their_required_value(self):
        pairs = {(key, marked) for _, _, key, marked in SITES}
        assert ("connections.name", True) in pairs, (
            "the connection-name label is not seen as required"
        )
        assert ("connections.base_url", True) in pairs, (
            "rest_api's Base URL is not seen as required"
        )
        assert ("connections.base_url", False) in pairs, (
            "openapi's Base URL is not seen as optional"
        )


class TestNoLabelIsOptionalAndRequiredAtOnce:
    def test_no_required_label_is_worded_optional(self):
        offenders = [
            f"{path}:{lineno} {key!r} = {EN[key]!r}"
            for path, lineno, key, marked in SITES
            if marked and key in EN and any(w in EN[key].lower() for w in OPTIONAL_WORDS_EN)
        ]
        assert not offenders, (
            "these labels render the required marker over a string that calls the "
            "field optional:\n  " + "\n  ".join(offenders)
        )


class TestTheApiKeySplit:
    """The concrete core#822 case, asserted by which builder uses which key."""

    REQUIRED_BUILDERS = {
        "stripe_fields",
        "saas_api_key_fields",
        "shopify_fields",
        "jira_fields",
        "zendesk_fields",
        "airtable_fields",
        "freshdesk_fields",
        # core#1574 moved asana off `saas_api_key_fields` (it needed a workspace field). This guard
        # caught the new builder immediately and demanded the decision, which is the whole point of
        # it — so here is the decision rather than an inherited string: **required.**
        # `CONFIG_SCHEMAS["asana"]["required"] == ["api_key"]` and `DltRunnerService` raises
        # "Asana source requires 'api_key'", so there is no unauthenticated Asana use case the way
        # there is for `rest_api`/`openapi`.
        "asana_fields",
    }
    OPTIONAL_BUILDERS = {"rest_api_fields", "openapi_fields"}

    @staticmethod
    def _api_key_usage() -> dict[str, set[str]]:
        """builder function name -> the connections.api_key* keys it renders."""
        path = UI_ROOT / "components" / "connection_config_fields.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        usage: dict[str, set[str]] = {}
        for fn in tree.body:
            if not isinstance(fn, ast.FunctionDef):
                continue
            for node in ast.walk(fn):
                if (
                    isinstance(node, ast.Subscript)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "_t"
                    and isinstance(node.slice, ast.Constant)
                    and str(node.slice.value).startswith("connections.api_key")
                    and not str(node.slice.value).startswith("connections.ph_")
                ):
                    usage.setdefault(fn.name, set()).add(node.slice.value)
        return usage

    def test_the_builders_split_as_expected(self):
        usage = self._api_key_usage()
        assert set(usage) == self.REQUIRED_BUILDERS | self.OPTIONAL_BUILDERS, (
            "the set of builders rendering an API-key label has changed; decide "
            "deliberately whether the new one is required or optional rather "
            "than letting it inherit whichever string is nearest"
        )
        for name in self.REQUIRED_BUILDERS:
            assert usage[name] == {"connections.api_key"}, name
        for name in self.OPTIONAL_BUILDERS:
            assert usage[name] == {"connections.api_key_optional"}, name

    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    def test_both_keys_exist_and_differ_in_every_locale(self, locale):
        t = get_translations(locale)
        assert "connections.api_key" in t
        assert "connections.api_key_optional" in t
        # A translator who copies the plain value into the optional key
        # reproduces the bug in that locale only, where nobody looks.
        assert t["connections.api_key_optional"] != t["connections.api_key"], locale
        assert len(t["connections.api_key_optional"]) > len(t["connections.api_key"]), (
            f"{locale}: the optional variant must carry an extra qualifier"
        )

    def test_the_required_string_is_not_worded_optional_in_english(self):
        assert "optional" not in EN["connections.api_key"].lower()
        assert "optional" in EN["connections.api_key_optional"].lower()

    def test_the_locale_files_stay_parseable(self):
        # Cheap belt-and-braces: these files are edited by hand and a trailing
        # comma turns every i18n assertion above into a collection error.
        for locale in SUPPORTED_LOCALES:
            path = Path(datanika.ui.__file__).parent.parent / "i18n" / f"{locale}.json"
            json.loads(path.read_text(encoding="utf-8"))


def _baked_marker_keys(translations) -> frozenset[str]:
    """Keys whose *translated string* itself ends in the marker — the second authoring surface.

    Taken as a parameter rather than read from ``EN`` directly so the guards below can be driven
    with a **synthetic** population. core#1311 emptied the real one (slice 4, 2026-09-25), and a
    predicate that can only be exercised against today's strings stops being testable the moment
    the defect it watches for is absent — which is exactly when it matters that it still works.
    """
    return frozenset(
        key
        for key, value in translations.items()
        if isinstance(value, str) and value.rstrip().endswith(REQUIRED_MARKER)
    )


#: The real population. **Empty since core#1311 slice 4**, and that is the fixed state, not a
#: broken scan — :class:`TestNoMarkerIsBakedIntoAnyTranslation` asserts the emptiness directly and
#: is armed by a synthetic control, so this zero is a reading rather than a silence.
BAKED_MARKER_KEYS = _baked_marker_keys(EN)


def _double_marked(sites, baked=None, translations=None) -> list[str]:
    """Sites that render the marker over a string which already carries one."""
    baked = BAKED_MARKER_KEYS if baked is None else baked
    translations = EN if translations is None else translations
    return [
        f"{path}:{lineno} {key!r} = {translations.get(key)!r}"
        for path, lineno, key, marked in sites
        if marked and key in baked
    ]


class TestNoMarkerIsBakedIntoAnyTranslation:
    """SPEC_FIELD_REQUIREDNESS AC1 / §2.2 — the marker lives at the call site, in no locale's prose.

    core#1311 started at twelve ``en.json`` labels ending in ` *`, agreeing across nine locales: 108
    strings encoding twelve pieces of translatable information plus one character repeated 108
    times. Slice 4 (2026-09-25) removed the last four — ``host``, ``db_path``, ``http_path``,
    ``token`` — so the population is now empty and this class is what keeps it that way.

    🚨 **All nine locales, not just ``en``.** The per-key guard in
    ``tests/test_ui/test_field_requiredness.py`` walks a named list; this one walks every string in
    every locale, because the way this defect comes back is a translator re-appending the glyph in
    one locale, where nobody looks — the same reasoning as ``TestTheApiKeySplit``'s per-locale
    assertion.

    ⚠️ **An emptiness assertion is worthless on its own** — it is satisfied by a detector that
    matches nothing, and by deleting the labels (``WORKFLOW_RULES`` §4). So it is paired with a
    synthetic control that requires the detector to *find* a baked marker, and with the presence
    facts: every locale has strings, and every key a call site references exists (asserted by
    :class:`TestTheScannerIsArmed`).
    """

    def test_the_detector_finds_a_baked_marker(self):
        """The control. Driven with a fabricated locale so it holds whatever the real one says."""
        assert _baked_marker_keys({"a.b": "Host *"}) == {"a.b"}
        assert _baked_marker_keys({"a.b": "Host*"}) == {"a.b"}, (
            "the detector must not depend on the space before the glyph — `rstrip()` then "
            "`endswith` is the invariant, and a translator dropping the space still bakes a marker"
        )

    def test_the_detector_does_not_flag_a_plain_label(self):
        """The negative half. A detector that flags everything is repaired by loosening it until
        it flags nothing, which is how an emptiness assertion comes to prove the opposite."""
        assert _baked_marker_keys({"a.b": "Host", "a.c": "5 * 3 items", "a.d": 7}) == frozenset()

    @pytest.mark.parametrize("locale", sorted(SUPPORTED_LOCALES))
    def test_no_translated_string_carries_the_marker(self, locale):
        translations = get_translations(locale)
        assert translations, f"{locale}: no translations loaded, so this assertion grades nothing"
        offenders = sorted(
            f"{key} = {translations[key]!r}" for key in _baked_marker_keys(translations)
        )
        assert not offenders, (
            f"{locale}: these translated strings carry the required marker, where it cannot vary "
            "by connector and nothing ties it to the input's required attribute "
            "(SPEC_FIELD_REQUIREDNESS §2.2). Remove it from all nine locales and derive it at the "
            "call site with `labelled_config_input(..., required=<literal>)` in the SAME change "
            "(§2.9):\n  " + "\n  ".join(offenders)
        )


class TestNoFieldShowsTwoRequiredMarkers:
    """SPEC_FIELD_REQUIREDNESS §2.9 — call-site markers + baked markers <= 1 for every field.

    ``field_label`` appends ``required_marker()`` **after** the label it is handed, so porting a
    call site whose translated string still ends in ``*`` renders ``Host * *``. Four keys were in
    that state until core#1311 slice 4 — ``connections.host``, ``http_path``, ``token``,
    ``db_path`` — and they are exactly the labels core#1547's AC2 was written to add a marker to.

    🚨 **Neither existing instrument can express this, and both are correct for their own
    question.** ``test_ui/test_field_requiredness.py::_label_state`` computes a label's marker as
    ``rendered or baked``: an OR, so one marker and two are the same value to it, and AC5's
    ``marker == required`` is **green** on a double. ``_label_sites`` above scans call sites only
    and never reads the string's own suffix, so the baked marker is outside its population. This is
    the third question, and it needs the two populations intersected.

    The invariant is not *"these four keys are baked"* — that was the instance, and core#1311
    removed it (``WORKFLOW_RULES`` §5a). It is that the two authoring surfaces never both fire for
    one field, which is true before that work, during it and after it.

    🔴 **REPOINTED 2026-09-25 (core#1311 slice 4), not deleted.** This class used to open with
    ``assert BAKED_MARKER_KEYS`` as its anti-vacuity, and its two synthetic controls indexed
    ``sorted(BAKED_MARKER_KEYS)[0]`` — so emptying the real population turned the anti-vacuity red
    and the controls into ``IndexError``. The docstring's own advice at the time was *"delete the
    class rather than leaving it green"*, and that was the wrong half of §5a: what had expired was
    the **instance the controls were driven with**, never the invariant. A real second authoring
    surface can reappear at any time — one translator re-appending a glyph is enough — and the
    ``field_label`` mechanism that concatenates a marker after the label it is handed is still
    there. So the controls now inject a **synthetic** baked population and the guard survives its
    own subject being absent, which is the state a guard spends most of its life in.
    """

    def test_the_site_extractor_still_sees_marked_sites(self):
        """Anti-vacuity for the half that CAN go blind.

        The real baked population is empty by design now, so an intersection being empty says
        nothing about it — but the call-site scan can still stop matching, and a blind scan reads
        exactly like a clean sweep. That half is asserted here; the other half's arming is the two
        synthetic controls below, which do not depend on today's strings at all.
        """
        assert REQUIRED_SITES, (
            "no call site renders the marker; the site extractor has stopped matching"
        )

    def test_a_synthetic_double_marked_site_is_caught(self):
        """The control, driven with a population that does not exist today.

        A guard whose real population happens to be clean is indistinguishable from one that
        cannot see — so ask it for a reading about a fabricated offender and require it to fire.
        Both the site and the baked key are fabricated, so this keeps working now that core#1311
        has left no real baked key to borrow.
        """
        fake = {"synthetic.key": "Host *"}
        fabricated = [("<synthetic>", 0, "synthetic.key", True)]
        assert _double_marked(fabricated, baked=_baked_marker_keys(fake), translations=fake), (
            "the predicate does not flag a call site rendering the marker over an already-marked "
            "string, so its verdict on the real population says nothing"
        )

    def test_a_synthetic_single_marked_site_is_not_caught(self):
        """The negative half. A predicate that flags everything is not discriminating, and the
        obvious repair for "it flags everything" is to loosen it until it flags nothing."""
        fake = {"synthetic.baked": "Host *", "synthetic.plain": "Host"}
        baked = _baked_marker_keys(fake)
        assert baked == {"synthetic.baked"}, "the synthetic population is not set up as intended"
        # marker at the call site, nothing baked into the string -> one marker, not two.
        assert not _double_marked(
            [("<synthetic>", 0, "synthetic.plain", True)], baked=baked, translations=fake
        )
        # baked marker, nothing at the call site -> also one marker.
        assert not _double_marked(
            [("<synthetic>", 0, "synthetic.baked", False)], baked=baked, translations=fake
        )

    def test_no_site_renders_a_marker_over_an_already_marked_string(self):
        offenders = _double_marked(SITES)
        assert not offenders, (
            "these call sites render a required marker over a translated string that already ends "
            "in one, so the user sees it twice (SPEC_FIELD_REQUIREDNESS §2.9). Fix by removing the "
            "marker from the string in all nine locales in the SAME change, never by leaving both:"
            "\n  " + "\n  ".join(offenders)
        )
