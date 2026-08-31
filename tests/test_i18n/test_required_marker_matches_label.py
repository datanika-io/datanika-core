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

# The required marker as it is written throughout the UI: a second positional
# argument to rx.text, next to the translated label.
REQUIRED_MARKER = "*"

# A scan that silently finds nothing passes every assertion below it. Both
# counts are pinned so an extractor that stops matching fails loudly instead
# of reporting a clean sweep of zero labels.
MIN_LABEL_SITES = 60
MIN_REQUIRED_SITES = 20


def _label_sites() -> list[tuple[str, int, str, bool]]:
    """(file, lineno, i18n key, carries the required marker) for every rx.text label."""
    sites: list[tuple[str, int, str, bool]] = []
    for path in sorted(UI_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
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
                # _t["some.key"]
                if (
                    isinstance(arg, ast.Subscript)
                    and isinstance(arg.value, ast.Name)
                    and arg.value.id == "_t"
                    and isinstance(arg.slice, ast.Constant)
                    and isinstance(arg.slice.value, str)
                ):
                    key = arg.slice.value
                elif (
                    isinstance(arg, ast.Constant)
                    and isinstance(arg.value, str)
                    and REQUIRED_MARKER in arg.value
                ):
                    marked = True
            if key is not None:
                sites.append((str(path.relative_to(UI_ROOT)), node.lineno, key, marked))
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
