"""Regression test for core#618 — the connection form must be un-autofillable.

**The defect.** Not one input in the connection form set ``autocomplete``, ``name``
or ``id``. Chrome's password manager therefore fell back to its positional
heuristic: pair the nearest preceding text input with the first
``type="password"`` input and fill the saved credentials for the origin. On
``google_ads`` that put the signed-in user's Datanika **email** into
``Customer ID`` and their Datanika **password** into ``Developer token`` —
fields the user never touched, that look deliberately filled, and that
``_build_google_ads_source`` then transmits to ``googleads.googleapis.com`` as a
``developer-token`` header. A credential for our own system, handed to a third
party, because of a missing HTML attribute.

**Why this test walks the rendered tree rather than the source.** The fields are
hand-written per connector, not generated from a schema, so "read the code and
check" scales with 37 connectors and fails silently for the 38th. Rendering
``connection_form()`` and inspecting the JSX props asserts the property that
actually reaches the browser, across **every** branch at once — ``rx.cond`` puts
both arms in the tree, so one walk covers every connection type the form can
display.

**Two node shapes, and getting this wrong makes the test lie.** An ``rx.input``
with ``value=`` + ``on_change=`` does *not* render as a text field: Reflex wraps
it in a ``DebounceInput`` and passes the real control through the ``element``
prop, where a walk over ``children`` never sees it. The first draft of this file
looked only for ``RadixThemesTextField.Root`` and found **one** field in the
whole form — the one stateless input in ``searchable_select`` — while reporting
nothing wrong about the other eighty-odd. Only the anti-vacuity guard below
caught it.

**The invariant, stated as a biconditional on purpose.** ``type="password"`` and
``autoComplete="new-password"`` must imply each other. Two independent flags
saying "this is a secret" is how they drift; making the test reject either one
alone is what keeps ``secret=True`` the single source of truth in
``datanika/ui/components/secure_input.py``.
"""

import pytest

from datanika.ui.components.connection_config_fields import type_fields
from datanika.ui.pages.connections import connection_form

#: JSX component names Reflex emits for a text-entry control rendered *without*
#: state binding (``searchable_select``'s pure-frontend filter box).
BARE_TEXT_ENTRY_TAGS = {"RadixThemesTextField.Root", "RadixThemesTextArea"}

#: ...and the same two, seen through the debounce wrapper Reflex adds as soon as
#: the input has ``value=`` + ``on_change=``. Which is every field on this form.
DEBOUNCED_ELEMENTS = {"RadixThemesTextField.Root", "RadixThemesTextArea"}

#: ``rx.upload`` renders an ``<input type="file">``. File inputs are not
#: autofillable and carry no credential, so they are deliberately out of scope.


def _walk(node):
    """Yield every dict in a rendered Reflex tree.

    Recurses through ``children`` **and** through ``rx.cond``'s ``true_value`` /
    ``false_value``, which is the point: an assertion that only followed
    ``children`` would silently skip all 30-odd per-connector field groups.
    """
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def _props(node: dict) -> dict[str, str]:
    """Parse a rendered node's props list (``['type:"password"', ...]``).

    Values are kept as the raw JS source Reflex emits, so a string prop compares
    as ``'"password"'`` — quotes included. That is deliberate: it distinguishes a
    literal from a state binding, and a state-bound ``autoComplete`` would not be
    a fix.
    """
    parsed = {}
    for prop in node.get("props", []):
        if ":" not in prop:
            continue
        key, _, value = prop.partition(":")
        parsed[key.strip().strip('"')] = value.strip()
    return parsed


def _text_entry_fields(component) -> list[tuple[str, dict[str, str]]]:
    """Every text input / textarea in a component tree, with its props.

    Handles both node shapes described in the module docstring.
    """
    found = []
    for node in _walk(component.render()):
        name = node.get("name")
        if name in BARE_TEXT_ENTRY_TAGS:
            found.append((name, _props(node)))
        elif name == "DebounceInput":
            props = _props(node)
            element = props.get("element", "")
            if element in DEBOUNCED_ELEMENTS:
                found.append((element, props))
    return found


def _label(props: dict[str, str]) -> str:
    """Best available human handle for a field, for assertion messages."""
    return props.get("name", props.get("placeholder", "<unnamed>"))


@pytest.fixture(scope="module")
def form_fields() -> list[tuple[str, dict[str, str]]]:
    """Every text-entry control the connection form can render, any type."""
    return _text_entry_fields(connection_form())


def test_the_walk_actually_reaches_the_per_connector_fields(form_fields):
    """Anti-vacuity guard.

    Every assertion below is a "for all" over this list, so an empty or
    truncated list passes them all while testing nothing. This is not a
    hypothetical: the first draft of this file returned **1** field instead of
    80-odd, because it did not know about ``DebounceInput``, and every other
    test in the file would have gone green on completely unfixed code.
    """
    assert len(form_fields) > 60, (
        f"Only found {len(form_fields)} text-entry fields in the connection form. "
        "Expected 60+ (every rx.cond branch of type_fields() plus the form's own "
        "inputs). The walker is not reaching the per-connector field groups, so "
        "every other assertion in this file is vacuous."
    )
    # The specific fields from the #618 report must be among them.
    names = {props.get("name", "").strip('"') for _, props in form_fields}
    for expected in ("cfg-customer-id", "cfg-developer-token", "cfg-password"):
        assert expected in names, (
            f"{expected!r} is not among the rendered field names. "
            "Either the field was renamed or the walk is missing a branch. "
            f"Saw: {sorted(names)}"
        )


def test_every_text_entry_field_declares_autocomplete(form_fields):
    """No input may be left to Chrome's positional heuristic.

    This is the whole defect: with no ``autocomplete`` anywhere, there was
    nothing for the browser to disambiguate on.
    """
    missing = [(tag, props) for tag, props in form_fields if "autoComplete" not in props]
    assert not missing, (
        f"{len(missing)} of {len(form_fields)} text-entry fields in the connection "
        f"form carry no autoComplete attribute, so Chrome will fill them from the "
        f"saved site credential (core#618). Offenders: "
        f"{[_label(p) for _, p in missing][:10]}"
    )


def test_password_fields_use_new_password_and_nothing_else_does(form_fields):
    """``type="password"`` and ``autoComplete="new-password"`` imply each other.

    ``autocomplete="off"`` is widely ignored by Chrome on password inputs;
    ``new-password`` is the token it honours. Asserting the biconditional (not
    just one direction) is what stops a future field from declaring one flag and
    forgetting the other.
    """
    for tag, props in form_fields:
        is_password = props.get("type") == '"password"'
        wants_new_password = props.get("autoComplete") == '"new-password"'
        assert is_password == wants_new_password, (
            f"{_label(props)} ({tag}) has type={props.get('type')!r} but "
            f"autoComplete={props.get('autoComplete')!r}. A secret field must be "
            f'type="password" AND autoComplete="new-password"; a non-secret field '
            f"must be neither. Set secret=True on config_input() instead of "
            f"hand-writing either attribute."
        )


def test_non_password_fields_opt_out_of_autofill(form_fields):
    """The username half of the pair needs an opt-out too.

    Chrome fills a *pair*. Locking the password half down is the load-bearing
    fix, but a bare text field with no ``autocomplete`` is still eligible for
    address/email autofill on its own — which is how the account email landed in
    ``Customer ID``.
    """
    for tag, props in form_fields:
        if props.get("type") == '"password"':
            continue
        assert props.get("autoComplete") == '"off"', (
            f'{_label(props)} ({tag}) should carry autoComplete="off"; '
            f"got {props.get('autoComplete')!r}."
        )


def test_every_field_has_a_stable_name_and_id(form_fields):
    """Give the browser something to key on other than document order.

    ``name``/``id`` derived from the field key is what makes the inputs
    identifiable; without it, position is all Chrome has.
    """
    for tag, props in form_fields:
        placeholder = props.get("placeholder", "?")
        assert props.get("name", '""') not in ('""', None), (
            f"A {tag} with placeholder {placeholder} has no name attribute."
        )
        assert props.get("id", '""') not in ('""', None), (
            f"A {tag} with placeholder {placeholder} has no id attribute."
        )


def test_third_party_password_managers_are_opted_out_too(form_fields):
    """1Password and LastPass ignore ``autocomplete`` and need their own flags.

    Same egress path, same blast radius — a manager that fills the Datanika
    credential into ``Developer token`` sends it to Google whichever vendor it
    came from.
    """
    for tag, props in form_fields:
        assert props.get("data-1p-ignore") == '"true"', (
            f"{_label(props)} ({tag}) is missing data-1p-ignore (1Password)."
        )
        assert props.get("data-lpignore") == '"true"', (
            f"{_label(props)} ({tag}) is missing data-lpignore (LastPass)."
        )


def test_type_fields_alone_is_also_covered():
    """``type_fields()`` owns the per-connector inputs; assert on it directly.

    Asserting the property on the component that owns the inputs — rather than
    only on today's one caller — keeps it true if the fields are embedded
    somewhere else (a wizard, a modal) tomorrow.
    """
    fields = _text_entry_fields(type_fields())
    assert len(fields) > 60, f"expected 60+ per-connector fields, got {len(fields)}"
    for tag, props in fields:
        assert "autoComplete" in props, f"{_label(props)} ({tag}) has no autoComplete"
