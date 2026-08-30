"""Regression tests for core#609 — a verdict must not outlive its configuration.

**The defect, observed on production in one uninterrupted form session:**

1. type `postgres`, real credentials → **✓ Connected successfully** (correct)
2. change the type to `bigquery` — badge **stays green**
3. fill in a completely different config — badge **stays green**
4. click Test Connection, the handler crashes (core#608) — badge **stays green**

End state: a form describing a BigQuery connection that has never once
connected, positively asserting "Connected successfully", with a red error toast
floating above it. The toast disappears after a few seconds; the badge does not.

**Why this is the serious half.** core#608 is a bad error message. This is a
*false green*: the UI asserts success for a configuration it never validated, and
because Reflex state is durable server-side the assertion survives a full reload
and a re-login. A user can reasonably read that badge, hit **Create Connection**,
wire it into an upload and put it on a schedule, and nothing contradicts them
until a run fails at 03:00.

**Why the sweep over every field.** The narrow reading of the report ("clear it
when the type changes") fixes step 2 and leaves step 3 — a green badge above a
config the user has since rewritten field by field. There are 50-odd config
fields across 37 connectors, most of whose setters Reflex was generating
automatically, so "remember to clear it" is not a control. Enumerating the
fields off the class means a field added next year is covered on the day it is
added, and a setter that forgets fails here rather than on prod.
"""

import pytest

from datanika.ui.state.connection_state import ConnectionState

#: Form fields that are **not** part of the configuration under test.
#:
#: `_build_config` never reads `form_name` — it is the connection's label — so
#: renaming a connection does not invalidate a verdict about its credentials.
#: The exemption is named rather than implicit so it can be argued with, and
#: `test_the_exemption_does_not_become_a_hole` stops it growing quietly.
NON_CONFIG_FORM_FIELDS = {"form_name"}


def _config_form_fields() -> set[str]:
    """Every `form_*` var on the state, minus the exemptions.

    Derived from the class, not typed out: the whole point is that a field added
    later is covered without anyone remembering this file exists.
    """
    return {f for f in ConnectionState.get_fields() if f.startswith("form_")} - (
        NON_CONFIG_FORM_FIELDS
    )


def _a_different_value(current):
    """A value of the right type that differs from `current`."""
    if isinstance(current, bool):
        return not current
    if isinstance(current, int):
        return current + 1
    return f"{current}-changed"


def _green(state: ConnectionState) -> None:
    """Put the state in the exact position step 1 of the repro leaves it in."""
    state.test_success = True
    state.test_message = "Connected successfully"


def test_there_are_config_fields_to_sweep():
    """Anti-vacuity guard.

    Every parametrised test below is driven by `_config_form_fields()`. If that
    returned an empty set — a renamed prefix, a Reflex API change in
    `get_fields()` — the sweep would collect zero cases and the file would report
    success while asserting nothing.
    """
    fields = _config_form_fields()
    assert len(fields) > 40, (
        f"expected 40+ config form fields, found {len(fields)}: {sorted(fields)}. "
        "The sweep below is only as good as this set."
    )
    # The fields from the reported repro must be in it.
    for expected in ("form_type", "form_project", "form_dataset", "form_host"):
        assert expected in fields, f"{expected} missing from the swept set"


def test_changing_the_connection_type_clears_the_verdict():
    """Step 2 of the repro: postgres tests green, user switches to bigquery."""
    state = ConnectionState()
    state.set_form_type("postgres")
    _green(state)

    state.set_form_type("bigquery")

    assert state.test_message == "", (
        "a verdict about a postgres config must not survive onto a bigquery one"
    )
    assert state.test_success is False


@pytest.mark.parametrize("field", sorted(_config_form_fields()))
def test_editing_any_config_field_clears_the_verdict(field):
    """Step 3 of the repro, generalised to every field on every connector."""
    state = ConnectionState()
    _green(state)

    setter = getattr(state, f"set_{field}", None)
    assert setter is not None, (
        f"{field} has no set_{field} — the form cannot bind it, and nothing can "
        "invalidate a verdict when it changes."
    )
    setter(_a_different_value(getattr(state, field)))

    assert state.test_message == "", (
        f"editing {field} left a stale 'Connected successfully' on the form"
    )
    assert state.test_success is False, f"editing {field} left test_success True"


@pytest.mark.parametrize("field", sorted(_config_form_fields()))
def test_the_setter_still_assigns_the_field(field):
    """Invalidating must not be all a setter does.

    A `_set_config_field` that cleared the verdict and forgot to assign would
    pass every test above while making the whole form unusable.
    """
    state = ConnectionState()
    target = _a_different_value(getattr(state, field))

    getattr(state, f"set_{field}")(target)

    assert getattr(state, field) == target, f"set_{field} did not store its argument"


def test_renaming_a_connection_does_not_clear_the_verdict():
    """The one exemption, asserted positively so it stays deliberate."""
    state = ConnectionState()
    _green(state)

    state.set_form_name("Renamed")

    assert state.test_message == "Connected successfully"
    assert state.test_success is True


def test_the_exemption_does_not_become_a_hole():
    """`NON_CONFIG_FORM_FIELDS` must stay a justified exception, not a bucket.

    The same shape as `WITHDRAWN_SOURCE_TYPES` in the picker coverage test: an
    exemption list that nothing checks is how "for all fields" quietly becomes
    "for the fields we still bother with".
    """
    assert sorted(NON_CONFIG_FORM_FIELDS) == ["form_name"], (
        "A field was exempted from verdict invalidation. It is only correct for "
        "fields `_build_config` never reads — anything else reopens core#609 for "
        "that field. Justify it here or take it out."
    )


def test_uploading_a_file_clears_the_verdict():
    """The upload widget writes the config without going through a setter.

    `handle_file_upload` assigns `form_uploaded_file_id` directly, so the
    per-setter fix does not reach it — and for csv/json/parquet the uploaded
    file *is* the configuration. A green badge from a previous file surviving a
    new upload is the same false green.
    """
    state = ConnectionState()
    _green(state)

    state._record_uploaded_file(7, "orders.csv")

    assert state.form_uploaded_file_id == 7
    assert state.form_uploaded_file_name == "orders.csv"
    assert state.test_message == ""
    assert state.test_success is False


def test_applying_a_pipeline_template_clears_the_verdict():
    """Templates prefill the form by direct assignment, same gap as the upload."""
    from datanika.data.pipeline_templates import list_templates

    templates = list_templates()
    assert templates, "no pipeline templates to exercise the prefill path with"

    state = ConnectionState()
    _green(state)

    state._apply_template_defaults(templates[0].slug)

    assert state.test_message == ""
    assert state.test_success is False
