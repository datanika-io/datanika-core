"""core#1170 AC3.2 — the saved-connection row must answer, not go quiet.

`SPEC_EARNED_VERDICTS` §3.2. AC3.3 gave the product six sentences for *"we asked
and cannot know from here"*, in nine languages. This row could not say any of
them.

**The defect.** `test_saved_connection` called the two-tuple
`ConnectionService.test_connection`, threw the message away, and mapped the
neutral verdict onto `test_status = ""` — the column's *nobody has asked* state.
So pressing **Test** on a probe-exempt connection was **byte-identical to never
pressing it**: no icon, no sentence, no change. The verdict was computed
correctly and arrived nowhere, and the six keys x nine locales rendered on no
screen in the product.

**Why not just show the red cross.** Because refusing to guess is not a failure.
Painting *"we did not test this"* as a failure is the same class of lie AC3.3
exists to stop, told in the other direction — which is why this takes a **fourth**
status rather than reusing `fail`.

⚠️ **The empty note matters as much as the sentence.** `_set_row_test_status`
writes `test_note` on every call, so a later real verdict clears a stale *"not
tested"* line. Without that, a green tick keeps the caption explaining why we
once could not test it.

**Negative controls, all five exercised before this file was committed**, each
turning exactly the expected test(s) red and green again on revert: revert the
handler to the two-tuple; stop writing `note` in the setter (2 red); remove the
`untested` branch from the markup; put the icon tag back inside an `rx.cond`;
blank one locale's string.

🚨 **One control did NOT go red, and the limit it exposed is stated rather than
papered over.** Disabling the branch at runtime — `if verdict.ok is None and
False:` — leaves all eight green, because the handler is asserted on its
**source**. This file catches the handler being *reverted*; it cannot catch the
handler being made *unreachable*. Driving `test_saved_connection` end to end
needs a session and an org, which is why it is asserted this way and why the
limit is named here instead of being discovered later by someone trusting the
word "control".
"""

import ast
import json
import pathlib

import datanika.ui
from datanika.services.connection_service import SAAS_PROBE_EXEMPT
from datanika.ui.state.connection_state import _VERDICT_KEYS, ConnectionItem, ConnectionState

UI = pathlib.Path(datanika.ui.__file__).parent
I18N_DIR = UI.parent / "i18n"
LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")


def _locale(name: str) -> dict:
    return json.loads((I18N_DIR / f"{name}.json").read_text(encoding="utf-8"))


def _rows(state: ConnectionState) -> ConnectionItem:
    return state.connections[0]


def _state_with_one_row() -> ConnectionState:
    state = ConnectionState()
    state.connections = [ConnectionItem(id=7, name="Sheets", connection_type="google_sheets")]
    return state


# --------------------------------------------------------------------------- 1


def test_not_asked_and_cannot_know_are_different_row_states():
    """The whole defect in one assertion: they used to be the same string."""
    state = _state_with_one_row()
    assert _rows(state).test_status == "", "a fresh row starts at 'nobody asked'"

    state._set_row_test_status(7, "untested", "We cannot verify this from here.")

    row = _rows(state)
    assert row.test_status == "untested", (
        "a neutral verdict must not reuse '' — that is the state meaning nobody "
        "pressed Test, and collapsing them is what made the button do nothing"
    )
    assert row.test_status != "", "'untested' collapsed back onto the not-asked state"
    assert row.test_note, "the row carries the sentence, or the icon explains nothing"


def test_a_neutral_verdict_is_not_rendered_as_a_failure():
    """Refusing to guess is not failing. The other direction of the same lie."""
    state = _state_with_one_row()
    state._set_row_test_status(7, "untested", "We cannot verify this from here.")
    assert _rows(state).test_status != "fail"


# --------------------------------------------------------------------------- 2


def test_a_real_verdict_clears_a_stale_not_tested_note():
    """Otherwise a green tick keeps the caption explaining why we could not test."""
    state = _state_with_one_row()
    state._set_row_test_status(7, "untested", "We cannot verify this from here.")
    assert _rows(state).test_note

    state._set_row_test_status(7, "ok")

    row = _rows(state)
    assert row.test_status == "ok"
    assert row.test_note == "", (
        "the 'not tested' sentence survived onto a successful verdict — the row "
        "now says it connected AND explains why it could not be tested"
    )


def test_the_setter_still_only_touches_its_own_row():
    """A setter that cleared everyone's note would pass every test above."""
    state = ConnectionState()
    state.connections = [
        ConnectionItem(id=1, name="a", test_status="ok"),
        ConnectionItem(id=2, name="b", test_status="untested", test_note="kept"),
    ]
    state._set_row_test_status(1, "fail")

    assert state.connections[1].test_status == "untested"
    assert state.connections[1].test_note == "kept", "editing row 1 wiped row 2"


# --------------------------------------------------------------------------- 3


def test_the_markup_actually_renders_the_untested_branch():
    """State can be perfect and the row still silent.

    A source assertion rather than a render, for the reason
    `test_icon_tags_are_real.py` gives: the surface is what shipped, and a
    branch nothing references is a value nobody sees.
    """
    src = (UI / "pages" / "connections.py").read_text(encoding="utf-8")
    assert '"untested"' in src, "connections.py never mentions the neutral status"
    assert "test_note" in src, (
        "the row renders no note, so the neutral icon explains nothing — which "
        "is a new silence rather than an honest answer"
    )


def test_every_icon_tag_on_this_row_is_a_literal():
    """⚠️ `rx.icon(rx.cond(...))` is a `DynamicIcon` and is NEVER list-checked.

    Measured: Reflex validates only the literal form, warning and substituting
    `circle_help`; the dynamic form skips the check entirely. So an icon name
    inside an `rx.cond` opts out of core#701's guard silently, and
    `test_icon_tags_are_real.py` — which collects `ast.Constant` first arguments
    — cannot see it either. Before this change `connections.py` contributed
    **zero** tags to that guard; it now contributes three.
    """
    tree = ast.parse((UI / "pages" / "connections.py").read_text(encoding="utf-8"))
    dynamic = []
    literal = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Attribute) and f.attr == "icon" and node.args:
            if isinstance(node.args[0], ast.Constant):
                literal += 1
            else:
                dynamic.append(node.lineno)

    assert literal >= 3, (
        f"only {literal} literal icon tags found — the extractor is not seeing "
        "this file, so the assertion below proves nothing"
    )
    assert not dynamic, (
        f"lines {dynamic} pass a non-literal tag to rx.icon; those tags are "
        "invisible to test_icon_tags_are_real.py and to Reflex's own validator"
    )


def test_the_handler_itself_maps_the_neutral_verdict_onto_the_new_status():
    """Without this, every test above passes on the **pre-fix** handler.

    `test_saved_connection` needs a session and an org, so the tests above drive
    `_set_row_test_status` directly — and a handler still calling the two-tuple
    and writing `""` would satisfy every one of them. That is the gap this
    closes, and it is asserted on the function's own source rather than a line
    range, because a range named for one method silently measures the next.

    ⚠️ **Source, therefore blind to reachability.** A handler that still *reads*
    correctly but can never enter the branch passes this. Measured, not assumed:
    `if verdict.ok is None and False:` leaves this test green.
    """
    src = (UI / "state" / "connection_state.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    fn = next(
        (
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef | ast.FunctionDef)
            and n.name == "test_saved_connection"
        ),
        None,
    )
    assert fn is not None, "test_saved_connection is gone — this test proves nothing"
    body = ast.get_source_segment(src, fn) or ""
    assert len(body.splitlines()) > 10, "extracted body is too short to be the handler"

    assert "test_connection_verdict" in body, (
        "the handler is back on the two-tuple, which discards the message and "
        "cannot tell a neutral verdict from a failure"
    )
    assert '"untested"' in body, (
        "the handler no longer routes the neutral verdict to its own status, so "
        "pressing Test on an exempt connection changes nothing again"
    )
    assert "_verdict_message" in body, (
        "the handler stopped translating, so the row falls back to the service's "
        "English in all nine locales"
    )


# --------------------------------------------------------------------------- 4


def test_every_exempt_reason_reaches_a_real_sentence_in_every_locale():
    """The AC's actual claim: the vocabulary is *reachable*, not merely present.

    Key parity is enforced in `tests/test_i18n`; what is asserted here is that
    each of the six exempt reasons maps to a key that carries a real, translated
    string in all nine locales — not an empty value, and not the English pasted
    nine times.
    """
    assert SAAS_PROBE_EXEMPT, "no exempt types — this test would assert nothing"
    en = _locale("en")

    for name in SAAS_PROBE_EXEMPT:
        reason = f"not_tested_{name}"
        key = _VERDICT_KEYS.get(reason)
        assert key, f"{reason} has no i18n key, so this row falls back to English"

        for loc in LOCALES:
            data = _locale(loc)
            assert key in data, f"{loc}.json is missing {key}"
            assert data[key].strip(), f"{loc}.json has an empty {key}"
            if loc != "en":
                assert data[key] != en[key], f"{loc}.json's {key} is the English verbatim"
