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


#: Keys whose *translated string* already ends in the marker — the second authoring surface.
#: Nine today, all ``connections.*`` (SPEC_FIELD_REQUIREDNESS §2.8). core#1311 empties this set;
#: the guard below must keep working when it does, which is why nothing here names a key.
BAKED_MARKER_KEYS = frozenset(
    key
    for key, value in EN.items()
    if isinstance(value, str) and value.rstrip().endswith(REQUIRED_MARKER)
)


def _double_marked(sites) -> list[str]:
    """Sites that render the marker over a string which already carries one."""
    return [
        f"{path}:{lineno} {key!r} = {EN.get(key)!r}"
        for path, lineno, key, marked in sites
        if marked and key in BAKED_MARKER_KEYS
    ]


class TestNoFieldShowsTwoRequiredMarkers:
    """SPEC_FIELD_REQUIREDNESS §2.9 — call-site markers + baked markers <= 1 for every field.

    ``field_label`` appends ``required_marker()`` **after** the label it is handed, so porting a
    call site whose translated string still ends in ``*`` renders ``Host * *``. Four keys are in
    that state today — ``connections.host``, ``http_path``, ``token``, ``db_path`` — and they are
    exactly the labels core#1547's AC2 was written to add a marker to.

    🚨 **Neither existing instrument can express this, and both are correct for their own
    question.** ``test_ui/test_field_requiredness.py::_label_state`` computes a label's marker as
    ``rendered or baked``: an OR, so one marker and two are the same value to it, and AC5's
    ``marker == required`` is **green** on a double. ``_label_sites`` above scans call sites only
    and never reads the string's own suffix, so the baked marker is outside its population. This is
    the third question, and it needs the two populations intersected.

    The invariant is not *"these four keys are baked"* — that is today's instance, and core#1311
    removes it (``WORKFLOW_RULES`` §5a). It is that the two authoring surfaces never both fire for
    one field, which is true before that work, during it and after it.
    """

    def test_both_populations_are_non_empty(self):
        """Anti-vacuity. An intersection with an empty set is empty, so without this the
        assertion below passes the day either scan goes blind — and a blind scan reads exactly
        like a clean sweep."""
        assert BAKED_MARKER_KEYS, (
            "no en.json value ends in the marker, so the baked-marker population is empty and "
            "the intersection below cannot report anything. If core#1311 has genuinely emptied "
            "it, this guard's subject is gone: delete the class rather than leaving it green."
        )
        assert REQUIRED_SITES, (
            "no call site renders the marker; the site extractor has stopped matching"
        )

    def test_a_synthetic_double_marked_site_is_caught(self):
        """The control, driven with a site that does not exist today.

        A guard whose real population happens to be clean is indistinguishable from one that
        cannot see — so ask it for a reading about a fabricated offender and require it to fire.
        """
        baked_key = sorted(BAKED_MARKER_KEYS)[0]
        fabricated = [("<synthetic>", 0, baked_key, True)]
        assert _double_marked(fabricated), (
            "the predicate does not flag a call site rendering the marker over an already-marked "
            "string, so its verdict on the real population says nothing"
        )

    def test_a_synthetic_single_marked_site_is_not_caught(self):
        """The negative half. A predicate that flags everything is not discriminating, and the
        obvious repair for "it flags everything" is to loosen it until it flags nothing."""
        unbaked = sorted(k for k in EN if k not in BAKED_MARKER_KEYS)
        assert unbaked, "every key is baked; the negative control has no subject"
        assert not _double_marked([("<synthetic>", 0, unbaked[0], True)])
        assert not _double_marked([("<synthetic>", 0, sorted(BAKED_MARKER_KEYS)[0], False)])

    def test_no_site_renders_a_marker_over_an_already_marked_string(self):
        offenders = _double_marked(SITES)
        assert not offenders, (
            "these call sites render a required marker over a translated string that already ends "
            "in one, so the user sees it twice (SPEC_FIELD_REQUIREDNESS §2.9). Fix by removing the "
            "marker from the string in all nine locales in the SAME change, never by leaving both:"
            "\n  " + "\n  ".join(offenders)
        )
