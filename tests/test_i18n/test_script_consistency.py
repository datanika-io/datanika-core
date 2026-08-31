"""core#695 — ``sr.json`` mixes Serbian Cyrillic and Serbian Latin on one screen.

Serbian is digraphic and **both orthographies are correct Serbian**. Using both
at once, in one UI, is not. On ``/login`` a Serbian user sees the link
*"Zaboravili ste lozinku?"* and, in the same card's footer,
*"Услови коришћења · Политика приватности"*.

Key parity cannot see this — every key is present in every locale and every value
is a correct translation. The defect is one level above the level any existing
test looks at, which is the same reason core#682's gender-agreement bug survived.

What the rule is, and what it deliberately is not
-------------------------------------------------

🚨 **"No Latin characters in ``sr.json``" is the obvious rule and it is wrong.**
Measured over the real file: **88 values legitimately mix scripts inside a single
string** — ``api_keys.new = "Нови API кључ"``, ``audit.ip_address = "IP адреса"``,
``connections.aws_access_key = "ИД кључа за приступ AWS"``. Latin acronyms inside
Cyrillic Serbian prose are correct Serbian. A rule that flagged those would arrive
with 88 false positives, acquire an allowlist on day one, and become a gate turned
off one line at a time.

A further **65 values are byte-identical to ``en.json``** — ``app.name``,
every ``ph_*`` placeholder (``you@example.com``, ``s3://my-bucket/path``,
``AKIAIOSFODNN7EXAMPLE``), and technical identifiers. Those are exempted
**mechanically**, by comparing against ``en.json``, not by a hand-maintained list —
the class of enumeration that silently omits members and is invisible to the tests
that consume it (core#809, core#732).

So the rule is: a value is *minority-script prose* iff, after removing
``{placeholders}`` and URLs, it contains letters of only one script **and**
differs from its English original. That leaves **70** Latin-only Serbian values
against roughly 550 Cyrillic ones.

The assertion is **internal consistency, not a chosen script.** Which orthography
Serbian should ship in is a Product/Growth call about the audience — Cyrillic is
the current majority (10,158 characters vs 4,201) and is what the newest copy
uses, so converting the 70 is the smaller change; Serbian Latin is more common in
tech contexts, in which case ~550 move instead. This file passes either way, and
so takes no position on a decision that is not QA's.

⚠️ Two related findings this measurement turned up, recorded rather than asserted
because each is a different defect: ``connections.cluster_hint`` and
``connections.cluster_replication`` are **English prose copied verbatim** into
``sr.json`` and pass key parity, and ``es.json`` mixes *tú* and *usted* two lines
apart on ``/signup``. The register mismatch is deliberately **not** guarded here:
detecting it needs a list of verb forms, and a linguistic heuristic written by the
same person who writes its fixtures is the shape this project has been burned by
three times. It is on core#695 as a human review item.

``xfail(strict=True)``: green on ``dev`` today so it holds no promotion, and it
fails the moment the file is made consistent, in either direction, unless the
marker goes with it.
"""

import json
import re
from pathlib import Path

import pytest

I18N = Path(__file__).resolve().parents[2] / "datanika" / "i18n"

CYRILLIC = re.compile(r"[Ѐ-ӿԀ-ԯ]")
LATIN = re.compile(r"[A-Za-z]")
PLACEHOLDER = re.compile(r"\{[^}]*\}")
URL = re.compile(r"https?://\S+")


def _prose(value: str) -> str:
    """The part of a value that is meant to be read as language.

    ``{terms}`` interpolation slots and URLs carry Latin letters no translation
    changes, so they are removed before the script is judged.
    """
    return URL.sub(" ", PLACEHOLDER.sub(" ", value))


def classify(value: str, english: str | None) -> str:
    """``"cyrillic" | "latin" | "mixed" | "exempt"`` for one value.

    ``exempt`` means *identical to the English original* — a proper noun, a
    placeholder or a technical identifier, decided by comparison rather than by a
    list somebody has to remember to update.
    """
    body = _prose(value)
    has_cyrillic, has_latin = bool(CYRILLIC.search(body)), bool(LATIN.search(body))
    if has_cyrillic and has_latin:
        return "mixed"
    if has_latin:
        return "exempt" if value == english else "latin"
    if has_cyrillic:
        return "cyrillic"
    return "exempt"


def _load(locale: str) -> dict[str, str]:
    data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if isinstance(v, str)}


def script_split(locale: str) -> dict[str, list[str]]:
    """``{classification: [keys]}`` for one locale file."""
    english = _load("en")
    buckets: dict[str, list[str]] = {"cyrillic": [], "latin": [], "mixed": [], "exempt": []}
    for key, value in sorted(_load(locale).items()):
        buckets[classify(value, english.get(key))].append(key)
    return buckets


class TestTheScriptGuardCanActuallyFail:
    def test_serbian_cyrillic_prose_is_classified_cyrillic(self):
        assert classify("Услови коришћења", "Terms of Service") == "cyrillic"

    def test_serbian_latin_prose_is_classified_latin(self):
        assert classify("Zaboravili ste lozinku?", "Forgot your password?") == "latin"

    def test_a_latin_acronym_inside_cyrillic_prose_is_mixed_not_latin(self):
        """🚨 88 real values look like this. Flagging them is how this guard would
        acquire an allowlist and stop being a guard."""
        assert classify("Нови API кључ", "New API key") == "mixed"
        assert classify("ИД кључа за приступ AWS", "AWS access key ID") == "mixed"

    def test_a_value_identical_to_english_is_exempt(self):
        assert classify("Datanika", "Datanika") == "exempt"
        assert classify("you@example.com", "you@example.com") == "exempt"
        assert classify("s3://my-bucket/path", "s3://my-bucket/path") == "exempt"

    def test_an_interpolation_slot_does_not_make_a_value_latin(self):
        assert classify("Креирањем налога прихватате {terms}.", "By creating {terms}.") == (
            "cyrillic"
        )

    def test_a_url_inside_prose_does_not_make_a_value_latin(self):
        assert classify("Погледајте https://datanika.io/docs за детаље.", "See docs.") == (
            "cyrillic"
        )

    def test_the_real_file_is_readable_and_the_classifier_is_armed(self):
        """Anti-vacuity. Every assertion below counts members of these buckets, and
        an empty split would satisfy the invariant by having examined nothing."""
        split = script_split("sr")
        assert sum(len(keys) for keys in split.values()) >= 500, (
            f"only classified {sum(len(k) for k in split.values())} sr.json values"
        )
        assert split["cyrillic"], "no Cyrillic values found — the classifier is broken"
        assert split["exempt"], "no exempt values found — the en.json comparison is broken"


class TestSerbianShipsInOneOrthography:
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "core#695: sr.json holds ~70 Latin-script Serbian values alongside "
            "~550 Cyrillic ones, and they render on the same card. Awaiting a "
            "Product call on which orthography ships. Remove this marker with it."
        ),
    )
    def test_no_screen_can_show_both_serbian_scripts(self):
        split = script_split("sr")
        cyrillic, latin = split["cyrillic"], split["latin"]
        minority, majority = sorted(
            (("Latin", latin), ("Cyrillic", cyrillic)), key=lambda pair: len(pair[1])
        )
        assert not minority[1], (
            f"sr.json mixes orthographies: {len(cyrillic)} Cyrillic values and "
            f"{len(latin)} Latin ones. Both are correct Serbian; using both in one "
            f"UI is not. The smaller set is {minority[0]} "
            f"({len(minority[1])} values) — converting it is the lesser change, "
            f"but which orthography ships is a Product call, and this test passes "
            f"either way. Minority keys: {minority[1][:8]}"
            f"{' ...' if len(minority[1]) > 8 else ''}. "
            f"({len(split['mixed'])} values legitimately mix a Latin acronym into "
            f"{majority[0]} prose and are NOT counted.) See core#695."
        )
