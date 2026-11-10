"""The save gate must refuse exactly the fields the form marks required (core#1547 AC1).

`test_every_connection_type_is_gated.py` asks *"does this type have a branch at all?"* — the
ratchet that shipped first. This file asks the next question, and it is the one that stays useful
once the ledger is empty: **is the branch right?**

## The invariant

For every connection type, the set of fields `_validate_connection_form` refuses blank equals the
set of `cfg-*` inputs its renderer draws with `required=True`. Both directions matter and they fail
differently:

* **Marked but ungated** is core#1547 itself — the form prints `*`, announces `required` to
  assistive technology, and saves the field empty. The failure then surfaces at connect time, by
  which point the user has left the form that caused it.
* **Gated but unmarked** is the same contradiction pointed the other way, and it is the defect a
  careless fix introduces: the user sees no marker, leaves the field blank, and the save refuses.
  `SPEC_FIELD_REQUIREDNESS` §1c is about exactly this disagreement between two signals.

## Why the expected value is RENDERED rather than listed

🔑 **An assertion that computes its expected value with the thing under test is satisfied by that
thing doing nothing** (`WORKFLOW_RULES` §4, measured on core#1543). The obvious shape here —
compare `_REQUIRED_FORM_FIELDS` against a hand-written list — restates the table twice and passes
whatever it says. So the expectation is taken from something the gate cannot move: the **rendered
component**, walked for inputs carrying `required:true`.

That also makes it a guard over the *mechanism*. A connector whose renderer gains a marked field
fails here until the gate catches up, and one whose marker is removed fails until the gate lets go.

## Why it drives the STATE rather than calling the pure function

`_validate_connection_form` takes one parameter per form field the table can name, and
`_validate_form` has to pass all of them. A
field added to the signature but forgotten at the call site arrives as `""` on every save, so the
type is refused permanently — and a test that called the pure function with hand-built kwargs would
be **green throughout**, because it would supply the value the real caller drops. So every
behavioural assertion below goes through `ConnectionState`, setting `form_*` vars the way the form
does. Measured: dropping one line from `_validate_form` reds this file and nothing else.
"""

from __future__ import annotations

import re

import pytest

from datanika.models.connection import ConnectionType
from datanika.ui.components import connection_config_fields as ccf
from datanika.ui.state.connection_state import _REQUIRED_FORM_FIELDS, ConnectionState

_INPUT_NAME = re.compile(r'name:"cfg-([a-z0-9-]+)"')

#: Types whose renderer is the shared `saas_api_key_fields()` rather than `<type>_fields`.
_SHARED_RENDERER = {
    "hubspot": "saas_api_key_fields",
    "notion": "saas_api_key_fields",
    "pipedrive": "saas_api_key_fields",
    "slack": "saas_api_key_fields",
}

#: A non-blank value for every field the table names, so "all present" can be asserted. Values are
#: placeholders; the gate only ever calls `.strip()` on them.
_FILLED = "x"


def _children(node) -> list:
    """Every child, including both arms of an `rx.cond` — a field is a field whichever arm draws it.

    Same widening as `test_field_requiredness.py`, and for the same reason: a rendered cond exposes
    its branches as `true_value` / `false_value`, not as `children`, so a naive walk returns `[]`
    and every conditionally-rendered field falls silently outside the population.
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


def _renderer(conn_type: str):
    return getattr(ccf, _SHARED_RENDERER.get(conn_type, f"{conn_type}_fields"), None)


def _marked_required(conn_type: str) -> set[str] | None:
    """Fields the renderer draws with `required=True`. `None` when the type has no renderer."""
    fn = _renderer(conn_type)
    if fn is None:
        return None
    found = set()
    for node in _walk(fn().render()):
        props = node.get("props", []) if isinstance(node, dict) else []
        match = _INPUT_NAME.search(" ".join(props))
        if match and "required:true" in props:
            found.add(match.group(1).replace("-", "_"))
    return found


#: The types this file can speak about: those with a renderer AND a row in the table.
TABLE_TYPES = sorted(_REQUIRED_FORM_FIELDS)


class TestTheInstrumentCanSee:
    """Every assertion below is vacuous if the render walk finds nothing."""

    def test_the_walk_finds_marked_fields(self):
        total = sum(len(_marked_required(t) or ()) for t in TABLE_TYPES)
        assert total >= 25, (
            f"the render walk found only {total} required-marked inputs across {len(TABLE_TYPES)} "
            "types; it has stopped matching the rendered shape, and an empty result would report "
            "every gate as over-strict rather than failing"
        )

    def test_every_table_row_names_a_real_connection_type(self):
        members = {m.value for m in ConnectionType}
        unknown = sorted(set(_REQUIRED_FORM_FIELDS) - members)
        assert not unknown, f"{unknown} are not ConnectionType members"

    def test_every_table_row_has_a_renderer(self):
        missing = [t for t in TABLE_TYPES if _renderer(t) is None]
        assert not missing, (
            f"{missing} are gated but draw no fields component, so the gate refuses a field the "
            "user is never shown. Either the renderer is missing or `_SHARED_RENDERER` needs a row."
        )


class TestTheGateAndTheFormAgree:
    @pytest.mark.parametrize("conn_type", TABLE_TYPES)
    def test_the_gate_refuses_exactly_what_the_form_marks(self, conn_type):
        gated = {field for field, _ in _REQUIRED_FORM_FIELDS[conn_type]}
        marked = _marked_required(conn_type)
        assert gated == marked, (
            f"{conn_type}: the gate refuses {sorted(gated)} but the form marks {sorted(marked)}.\n"
            f"  marked but ungated: {sorted(marked - gated)} — printed `*`, announced `required`, "
            "saved empty (core#1547 itself)\n"
            f"  gated but unmarked: {sorted(gated - marked)} — refused on save with nothing on "
            "screen saying so (SPEC_FIELD_REQUIREDNESS §1c, pointed the other way)"
        )

    @pytest.mark.parametrize("conn_type", TABLE_TYPES)
    def test_every_label_is_the_one_the_user_reads(self, conn_type):
        """A refusal has to name a field the user can find on the form.

        `api_key` is drawn as "API Key" for eight connectors and as **"Access Token"** for
        `facebook_ads`, `github` and `salesforce`. A per-field label table would name the wrong
        control for three of them, which is why the label is stored per (type, field).
        """
        for field, label in _REQUIRED_FORM_FIELDS[conn_type]:
            assert label and label[0].isupper(), (
                f"{conn_type}.{field} has label {label!r}; the message reads '<label> is required' "
                "and is shown to the user"
            )


class TestTheGateActuallyRefuses:
    """Behavioural, through the real state object — see the module docstring for why."""

    @staticmethod
    def _state(conn_type: str, *, blank: str | None) -> ConnectionState:
        state = ConnectionState()
        state.form_name = "n"
        state.form_type = conn_type
        state.form_use_raw_json = False
        for field, _ in _REQUIRED_FORM_FIELDS[conn_type]:
            setattr(state, f"form_{field}", "" if field == blank else _FILLED)
        return state

    @pytest.mark.parametrize("conn_type", TABLE_TYPES)
    def test_a_complete_form_is_accepted(self, conn_type):
        """The direction that catches a field the caller forgot to pass.

        A parameter added to the gate but missing from `_validate_form` arrives as `""`, so the
        type is refused on **every** save. That is invisible to the pure function and is exactly
        what this asserts against.
        """
        assert self._state(conn_type, blank=None)._validate_form() == ""

    @pytest.mark.parametrize(
        "conn_type, field",
        [
            pytest.param(t, f, id=f"{t}-{f}")
            for t in TABLE_TYPES
            for f, _ in _REQUIRED_FORM_FIELDS[t]
        ],
    )
    def test_each_required_field_is_refused_when_blank(self, conn_type, field):
        message = self._state(conn_type, blank=field)._validate_form()
        expected = dict(_REQUIRED_FORM_FIELDS[conn_type])[field]
        assert message == f"{expected} is required", (
            f"{conn_type} saved with a blank {field}: {message!r}. Before core#1547 this returned "
            "'' for 19 types, and the failure surfaced at connect time instead."
        )


class TestOpenapiKeepsItsSpecialCase:
    """AC4 — `openapi` must NOT gain a `base_url` requirement, and this is where that is held."""

    def test_openapi_has_no_row(self):
        assert "openapi" not in _REQUIRED_FORM_FIELDS, (
            "`openapi`'s Base URL is filled from the spec's `servers` entry and is honestly "
            "unmarked (SPEC_FIELD_REQUIREDNESS §2.7). `_build_config` already refuses a spec that "
            "yields no usable base URL. Gating it here breaks the spec-with-`servers`-and-blank-"
            "field row that §2.7 rules correct — measured on core#1547 AC4."
        )

    def test_openapi_marks_the_spec_and_leaves_base_url_unmarked(self):
        """The spec textarea is required. Base URL stays unmarked: a blank saves and
        `_build_config` fills it from the spec's `servers` entry. The blank spec is
        refused there too, which is why this type still has no table row."""
        marked = _marked_required("openapi")
        assert marked == {"openapi_spec"}, (
            f"openapi marks {sorted(marked or [])}. The required field is openapi_spec. "
            "base_url stays unmarked."
        )


#: core#1606. The form field, the refusal text, and the i18n key the label is drawn from.
#: `google_analytics` binds the JSON textarea to `api_key`, not `service_account_json`.
#: Zendesk reuses `connections.jira_email` — correct as displayed text; not renamed here.
_RULED = (
    ("google_analytics", "api_key", "Service Account JSON", "connections.service_account_json"),
    ("jira", "email", "Email", "connections.jira_email"),
    ("zendesk", "email", "Email", "connections.jira_email"),
)

_LABEL_KEY = re.compile(r'\["(connections\.[a-z0-9_]+)"\]')


def _bound_label(conn_type: str, field: str):
    """The `<label htmlFor="cfg-…">` the renderer draws for `field`, or None."""
    fn = _renderer(conn_type)
    if fn is None:
        return None
    bound = f'htmlFor:"cfg-{field.replace("_", "-")}"'
    for node in _walk(fn().render()):
        if isinstance(node, dict) and bound in (node.get("props") or []):
            return node
    return None


def _label_shows_a_marker(label) -> bool:
    """True when a sighted user sees `*`, from either authoring surface.

    The derived marker is a `contents` of `'"*"'` inside an aria-hidden span. A hand-written
    `" *"` argument renders as a contents string that contains the glyph. Reading only one of
    those is how a sweep reported every derived site as unmarked (core#1610).
    """
    for node in _walk(label):
        if not isinstance(node, dict):
            continue
        contents = node.get("contents")
        if isinstance(contents, str) and "*" in contents and "translations" not in contents:
            return True
    return False


class TestTheThreeRuledFieldsAreRequired:
    """core#1606. Marker and gate move together: either alone is the contradiction above."""

    @pytest.mark.parametrize(
        "conn_type, field, label, key",
        [pytest.param(*row, id=f"{row[0]}-{row[1]}") for row in _RULED],
    )
    def test_the_form_marks_the_field_the_gate_names(self, conn_type, field, label, key):
        gated = dict(_REQUIRED_FORM_FIELDS[conn_type])
        assert gated.get(field) == label, (
            f"{conn_type}.{field} is gated as {gated.get(field)!r}; the user reads {label!r}"
        )
        assert field in (_marked_required(conn_type) or set()), (
            f"{conn_type}.{field} is refused on save but the input carries no required attribute"
        )
        bound = _bound_label(conn_type, field)
        assert bound is not None, f"{conn_type} draws no label for {field}"
        assert _label_shows_a_marker(bound), (
            f"{conn_type}.{field} is required and its label draws no marker"
        )
        keys = _LABEL_KEY.findall(str(bound))
        assert key in keys, f"{conn_type}.{field} label keys {keys}; expected {key}"
        assert "connections.service_account_json_optional" not in keys, (
            f"{conn_type}.{field} still says (optional) — core#1606 AC3"
        )

    @pytest.mark.parametrize(
        "conn_type, field, label, key",
        [pytest.param(*row, id=f"{row[0]}-{row[1]}") for row in _RULED],
    )
    def test_a_blank_ruled_field_is_refused_through_the_state(self, conn_type, field, label, key):
        state = ConnectionState()
        state.form_name = "n"
        state.form_type = conn_type
        state.form_use_raw_json = False
        for gated_field, _gated_label in _REQUIRED_FORM_FIELDS[conn_type]:
            setattr(state, f"form_{gated_field}", _FILLED)
        setattr(state, f"form_{field}", "")
        assert state._validate_form() == f"{label} is required"
