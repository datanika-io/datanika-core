"""Every `CONFIG_SCHEMAS` property must survive the structured connection form.

## The defect this links two sources against (core#638, widened by core#662)

`CONFIG_SCHEMAS` (published by `/api/v1/meta/connection-types`) and the Reflex form's
two serialisers — `ConnectionState._build_config()` and `._populate_form_from_config()`
— are hand-maintained lists of the same key names with **no code linking them**. A key
present in the schema and missing from `_build_config` is silently dropped the next
time a user opens that connection in the structured form and clicks Save. The
connection then reverts to a default on its next run, having been "edited" in a way
the user never asked for.

`tests/test_connector_type_contracts.py` asserts on `CONFIG_SCHEMAS`. The form tests
assert on the form. **Each source is checked against itself**, so both are green and
the gap between them is invisible — which is why `mongodb.auth_source` shipped with a
test whose docstring says *"a setting with no surface is the core#499 mistake"*.

## Why this ships as a ratchet rather than a clean green

Measured, the drift is **21 keys across 13 types**, not one. Fixing all of them means
touching thirteen connector field groups and re-deciding, per key, whether the schema or
the form has the right name — and for `slack` and `salesforce` neither matches what the
runner reads (core#662). Holding this test back until then leaves the *mechanism*
unguarded for however long that takes, and the mechanism is what keeps producing new
instances.

So `_DROPPED_ON_SAVE` records what is broken today and the check forbids that set from
**growing**. `test_the_ledger_does_not_outlive_the_defects` forbids it from rotting:
an entry that is no longer dropped must be deleted, so the list shrinks to nothing as
core#662 is worked off and cannot quietly become a permanent exemption.

## The stand-in `self` is deliberately not a mock

`ConnectionState()` cannot be instantiated (`ReflexRuntimeError`), and the obvious
workaround — a `MagicMock` — would make this test **unable to fail**: a mock answers
every attribute with a truthy mock, so `if self.form_host:` is always true,
`_build_config` writes every key regardless of which branch it is in, and nothing is
ever reported as dropped. That is the core#644 lesson, where a mock `self` made twelve
tests green while touching no database.

`FormStub` is built from `ConnectionState.get_fields()` instead, so it carries exactly
the vars the class declares, at their declared defaults, and a var a serialiser reads
but the class does not declare raises `AttributeError` rather than answering.
"""

import copy

import pytest

from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.ui.state.connection_state import ConnectionState

#: Placeholder per JSON-Schema type. Values are irrelevant — this test is about
#: which *keys* survive, not what they hold.
_PLACEHOLDER: dict[str, object] = {
    "string": "probe-value",
    "integer": 4242,
    "boolean": True,
    "array": ["probe-value"],
    "object": {"k": "v"},
}

#: Keys the structured form drops on save, as measured 2026-08-30. **A debt ledger
#: with an issue number, not a parking space** — every entry is a live silent-data-loss
#: bug tracked by core#662, and `test_the_ledger_does_not_outlive_the_defects` deletes
#: it from here the moment it is fixed.
_DROPPED_ON_SAVE: dict[str, set[str]] = {
    "clickhouse": {"cluster_replication"},
    "csv": {"path"},
    "google_sheets": {"spreadsheet_id"},
    "jira": {"api_token", "server_url"},
    "json": {"path"},
    "mongodb": {"auth_source"},  # core#638, the field that started this; leaves with core#626
    "parquet": {"path"},
    "rest_api": {"auth_password", "auth_token", "auth_type", "auth_user"},
    "salesforce": {"client_id", "client_secret", "password", "security_token", "username"},
    "shopify": {"access_token", "shop_url"},
    "slack": {"token"},
    "zendesk": {"api_token"},
}

#: Types whose `_populate_form_from_config` raises on a synthetic config, so the
#: round trip cannot be measured at all. Not a pass — an unmeasured type.
_UNPROBEABLE: dict[str, str] = {
    "openapi": (
        "raises ValueError('Spec did not parse to an object') on a placeholder "
        "openapi_spec; needs a real spec fixture to probe (core#662)"
    ),
}


class FormStub:
    """A stand-in `self` carrying exactly `ConnectionState`'s declared vars.

    See the module docstring for why this is not a `MagicMock`. The serialisers are
    private (underscore-prefixed), so Reflex leaves them as plain functions and they
    can be bound to any object with the right attributes.
    """

    _clear_test_verdict = ConnectionState._clear_test_verdict
    _build_config = ConnectionState._build_config
    _populate_form_from_config = ConnectionState._populate_form_from_config

    def __init__(self):
        for name, field in ConnectionState.get_fields().items():
            setattr(self, name, copy.deepcopy(getattr(field, "default", None)))


def synthetic_config(schema: dict) -> dict:
    return {
        key: _PLACEHOLDER.get(spec.get("type"), "probe-value")
        for key, spec in schema["properties"].items()
    }


def dropped_keys(conn_type: str, config: dict) -> set[str]:
    """Keys that do not survive `populate -> build` — i.e. what Save would lose."""
    stub = FormStub()
    stub._populate_form_from_config("probe", conn_type, config)
    return set(config) - set(stub._build_config())


class TestTheCheckCanFail:
    """A test that has never failed has never been shown to be able to."""

    def test_an_unhandled_schema_property_is_reported(self):
        """The whole point: a key with no line in `_build_config` must be caught."""
        cfg = synthetic_config(CONFIG_SCHEMAS["mongodb"])
        cfg["a_key_no_serialiser_knows_about"] = "x"
        assert "a_key_no_serialiser_knows_about" in dropped_keys("mongodb", cfg)

    def test_a_handled_property_is_not_reported(self):
        """...and the check must stay quiet on a key that does round-trip, or it
        would be a check that always fires, which is the same as one that never does."""
        cfg = synthetic_config(CONFIG_SCHEMAS["mongodb"])
        assert "host" not in dropped_keys("mongodb", cfg)

    def test_the_stub_refuses_unknown_vars_instead_of_answering(self):
        """If this ever starts answering, every assertion above becomes vacuous."""
        with pytest.raises(AttributeError):
            _ = FormStub().form_this_var_does_not_exist


class TestEverySchemaKeySurvivesSave:
    @pytest.mark.parametrize("conn_type", sorted(CONFIG_SCHEMAS))
    def test_no_new_key_is_silently_dropped(self, conn_type: str):
        if conn_type in _UNPROBEABLE:
            pytest.skip(_UNPROBEABLE[conn_type])
        lost = dropped_keys(conn_type, synthetic_config(CONFIG_SCHEMAS[conn_type]))
        new = lost - _DROPPED_ON_SAVE.get(conn_type, set())
        assert not new, (
            f"{conn_type}: {sorted(new)} is declared in CONFIG_SCHEMAS but has no line in "
            "ConnectionState._build_config() / ._populate_form_from_config(). It will be "
            "silently dropped the next time this connection is opened in the structured "
            "form and saved.\n"
            "Add the missing line to BOTH serialisers (plus a form_<field> var, its setter, "
            "and a reset in _populate_form_from_config). Do not add it to _DROPPED_ON_SAVE "
            "— that ledger is for pre-existing debt tracked by core#662, not for new keys."
        )

    def test_the_parametrisation_actually_collected_something(self):
        """A parametrised suite can go green by collecting nothing. Ours cannot be
        empty unless CONFIG_SCHEMAS is, so pin the count against zero explicitly."""
        assert len(CONFIG_SCHEMAS) > 30


class TestTheLedgerDoesNotRot:
    def test_the_ledger_does_not_outlive_the_defects(self):
        """An entry for a key that now round-trips is a stale exemption, and a stale
        exemption is how a ratchet quietly becomes a permanent allowance."""
        stale: list[str] = []
        for conn_type, keys in _DROPPED_ON_SAVE.items():
            if conn_type in _UNPROBEABLE:
                continue
            lost = dropped_keys(conn_type, synthetic_config(CONFIG_SCHEMAS[conn_type]))
            for key in sorted(keys - lost):
                stale.append(f"{conn_type}.{key}")
        assert not stale, (
            "These keys now survive save, so their _DROPPED_ON_SAVE entries are stale "
            f"and must be deleted: {stale}"
        )

    def test_every_ledger_entry_names_a_real_schema_property(self):
        wrong: list[str] = []
        for conn_type, keys in _DROPPED_ON_SAVE.items():
            schema = CONFIG_SCHEMAS.get(conn_type)
            if schema is None:
                wrong.append(f"{conn_type}: not a connection type any more")
                continue
            for key in sorted(keys - set(schema["properties"])):
                wrong.append(f"{conn_type}.{key}: not in CONFIG_SCHEMAS any more")
        assert not wrong, wrong

    def test_unprobeable_types_are_still_unprobeable(self):
        """If one starts probing cleanly, it belongs in the real check, not in a
        skip list that would otherwise hide it forever."""
        still_broken = []
        for conn_type in _UNPROBEABLE:
            try:
                dropped_keys(conn_type, synthetic_config(CONFIG_SCHEMAS[conn_type]))
            except Exception:  # noqa: BLE001
                still_broken.append(conn_type)
        assert still_broken == sorted(_UNPROBEABLE), (
            "one of these can now be probed; move it out of _UNPROBEABLE so "
            "test_no_new_key_is_silently_dropped covers it"
        )
