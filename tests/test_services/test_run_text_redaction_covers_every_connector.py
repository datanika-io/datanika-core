"""Every connector type's declared credential fields are removed from a run's stored text.

``core#1460`` established the property at the store and proved it on **two** connectors. This file
asks the same question of **every** connector type the product offers, and it asks it from the
product's own registry rather than from a list somebody typed: add a connector tomorrow and it is
swept the moment it has a schema, with nobody editing this file (``WORKFLOW_RULES`` §5a).

The sweep plants a distinct marker in every string-typed property of every schema in
``CONFIG_SCHEMAS``, builds a text shaped like something a driver quotes back — every marker in every
spelling :func:`_secret_spellings` knows — and reads what survives
:func:`~datanika.services.connection_service.redact_run_text`.

**Two controls, because a clean sweep across 36 connectors otherwise measures nothing:**

* ``test_the_instrument_finds_every_marker_with_nothing_held`` — with no config held, every marker
  must still be readable. Without it, "no marker survived" is equally explained by a search that
  cannot see its own markers, which is the exact reading in doubt.
* ``test_a_field_that_is_not_a_credential_survives`` — a hostname, a database name and a username
  must come through untouched. Without it, "no marker survived" is equally explained by a redactor
  that shreds everything, and the diagnosis a run stores would be worthless.

A structured (object or array) property is deliberately outside this sweep: its inner shape is the
user's rather than the schema's, so it is asserted per shape instead of by a census.
"""

from __future__ import annotations

import ast
import base64
import pathlib
from urllib.parse import quote, quote_plus

import pytest

from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.services.connection_service import (
    RUN_TEXT_WITHHELD,
    SECRET_CONFIG_KEYS,
    redact_run_text,
)

#: Long enough to be redacted rather than withheld, and distinctive enough that one marker can
#: never be a substring of another.
_NONCE = "Wm4Rt"

#: A marker carries ``@``, ``/`` and a space **on purpose**: without them ``quote_plus`` and
#: ``quote(safe="")`` return the value unchanged, the five spellings collapse into one, and a
#: redactor that had stopped covering an encoding would still read clean. A real credential
#: routinely carries these characters, which is why `_secret_spellings` exists at all.
_AWKWARD = "@a/b c"

CONNECTOR_TYPES = sorted(CONFIG_SCHEMAS)


def _marker(conn_type: str, key: str) -> str:
    return f"QAmark{_NONCE}{conn_type.replace('_', '')}{key.replace('_', '')}{_AWKWARD}End"


def _spellings(value: str) -> list[str]:
    """The same renderings ``_secret_spellings`` covers, written out independently.

    Importing the private helper would make this assert that the redactor agrees with itself.
    """
    encoded = base64.b64encode(value.encode()).decode("ascii")
    return [value, quote_plus(value), quote(value, safe=""), encoded, quote_plus(encoded)]


def _fields(conn_type: str) -> dict[str, bool]:
    """``{string property -> is it declared a credential}`` for one connector type."""
    return {
        key: prop.get("format") == "password"
        for key, prop in CONFIG_SCHEMAS[conn_type].get("properties", {}).items()
        if isinstance(prop, dict) and prop.get("type") == "string"
    }


def _config(conn_type: str) -> dict:
    """A config carrying a distinct marker in every string property the schema declares."""
    return {key: _marker(conn_type, key) for key in _fields(conn_type)}


def _driver_echo(markers: list[str]) -> str:
    """Text shaped like a driver quoting back what it was handed, in every spelling."""
    lines = ["could not connect to the server: authentication failed"]
    lines += [f"  arg={spelling}" for m in markers for spelling in _spellings(m)]
    return "\n".join(lines)


def _readable(text: str | None, marker: str) -> bool:
    return text is not None and any(spelling in text for spelling in _spellings(marker))


def _stored(conn_type: str) -> tuple[str | None, dict[str, bool]]:
    config = _config(conn_type)
    return redact_run_text(_driver_echo(list(config.values())), [config]), _fields(conn_type)


# --------------------------------------------------------------------------------------------
# Anti-vacuity. A sweep that covers nothing passes every assertion below it.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("conn_type", CONNECTOR_TYPES)
def test_a_markers_spellings_do_not_collapse(conn_type):
    """Guard the guard: with a tame marker the spellings collapse and this file goes blind.

    Only the first four are asserted distinct. ``quote_plus`` of a base64 payload equals the
    payload whenever it happens to contain no ``+`` or ``/``, which is a property of the bytes and
    not of the redactor.
    """
    for key in _fields(conn_type):
        raw, plus, pct, encoded, _ = _spellings(_marker(conn_type, key))
        assert len({raw, plus, pct, encoded}) == 4, (conn_type, key, raw, plus, pct)


def test_the_sweep_reaches_every_connector_type():
    assert len(CONNECTOR_TYPES) >= 30, f"only {len(CONNECTOR_TYPES)} connector schemas found"
    credentials = sum(sum(_fields(t).values()) for t in CONNECTOR_TYPES)
    assert credentials >= 30, f"only {credentials} declared credential fields found"
    ordinary = sum(sum(not v for v in _fields(t).values()) for t in CONNECTOR_TYPES)
    assert ordinary >= 30, f"only {ordinary} ordinary fields found, so the control is thin"


def test_the_instrument_finds_every_marker_with_nothing_held():
    """Control: with no config held, the search must read back every marker it planted."""
    for conn_type in CONNECTOR_TYPES:
        config = _config(conn_type)
        text = redact_run_text(_driver_echo(list(config.values())), [])
        unreadable = [key for key, value in config.items() if not _readable(text, value)]
        assert not unreadable, (
            f"{conn_type}: the probe cannot read back its own markers {unreadable}, so every "
            "'nothing survived' reading in this file would be vacuous"
        )


# --------------------------------------------------------------------------------------------
# The property.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("conn_type", CONNECTOR_TYPES)
def test_every_declared_credential_field_is_removed_from_stored_run_text(conn_type):
    text, fields = _stored(conn_type)
    if text == RUN_TEXT_WITHHELD:
        return  # withholding the whole text also satisfies the property
    survived = [
        key
        for key, credential in fields.items()
        if credential and _readable(text, _marker(conn_type, key))
    ]
    assert not survived, (
        f"{conn_type}: a run's stored text can carry the value of {survived}, which this "
        "connector's own schema declares a credential"
    )


@pytest.mark.parametrize("conn_type", CONNECTOR_TYPES)
def test_a_field_that_is_not_a_credential_survives(conn_type):
    """Control: the redactor is selective, and the run still says what went wrong."""
    text, fields = _stored(conn_type)
    assert text != RUN_TEXT_WITHHELD, f"{conn_type}: the whole text was withheld"
    assert "authentication failed" in text, f"{conn_type}: the diagnosis did not survive"
    removed = [
        key
        for key, credential in fields.items()
        if not credential and not _readable(text, _marker(conn_type, key))
    ]
    assert not removed, (
        f"{conn_type}: {removed} were removed although the schema does not declare them "
        "credentials — a redactor that shreds the diagnosis makes every other assertion here "
        "pass for the wrong reason"
    )


# --------------------------------------------------------------------------------------------
# The drift this sweep cannot see on its own: a config key no schema declares.
# --------------------------------------------------------------------------------------------

_CONNECTION_STATE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "datanika"
    / "ui"
    / "state"
    / "connection_state.py"
)

#: Config keys the connection form writes that no schema in ``CONFIG_SCHEMAS`` declares.
#:
#: Every schema-derived check in this repository — this sweep, and
#: ``test_secret_key_coverage.py``'s link from ``format: password`` to ``SECRET_CONFIG_KEYS`` —
#: takes its input from ``CONFIG_SCHEMAS``. A key outside it is therefore outside all of them, and
#: nothing anywhere goes red to say so. This list is the ratchet: it is asserted **exactly**, so a
#: twelfth undeclared key fails this test rather than arriving unexamined.
_UNDECLARED_FORM_KEYS = frozenset(
    {
        "aws_access_key_id",
        "aws_secret_access_key",
        "bucket_url",
        "endpoint_url",
        "extra_headers",
        "instance_url",
        "region_name",
        "spreadsheet_url",
        "store",
        "table_engine_type",
        "uploaded_file_id",
    }
)


def _form_written_config_keys() -> set[str]:
    """Keys the connection form assigns as ``config["<literal>"] = ...``."""
    tree = ast.parse(_CONNECTION_STATE.read_text(encoding="utf-8"))
    return {
        target.slice.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Subscript)
        and isinstance(target.value, ast.Name)
        and target.value.id == "config"
        and isinstance(target.slice, ast.Constant)
        and isinstance(target.slice.value, str)
    }


def test_the_form_key_scan_is_not_vacuous():
    written = _form_written_config_keys()
    assert len(written) >= 40, f"the scan found only {len(written)} keys the form writes"
    assert {"password", "api_key", "host"} <= written, sorted(written)


def test_no_new_config_key_escapes_every_schema_derived_check():
    declared = {key for schema in CONFIG_SCHEMAS.values() for key in schema.get("properties", {})}
    undeclared = _form_written_config_keys() - declared
    assert undeclared == _UNDECLARED_FORM_KEYS, (
        "the set of config keys the form writes without any schema declaring them has changed: "
        f"new={sorted(undeclared - _UNDECLARED_FORM_KEYS)} "
        f"gone={sorted(_UNDECLARED_FORM_KEYS - undeclared)}. A key here is invisible to every "
        "check that derives its input from CONFIG_SCHEMAS. Declare it in the connector's schema, "
        "or add it here deliberately."
    )


def test_the_canonical_secret_set_is_reachable_from_the_sweep():
    """Guard the guard: the sweep is only meaningful while the schemas name the canonical keys."""
    declared_credentials = {
        key for conn in CONNECTOR_TYPES for key, credential in _fields(conn).items() if credential
    }
    assert declared_credentials <= SECRET_CONFIG_KEYS, sorted(
        declared_credentials - SECRET_CONFIG_KEYS
    )
    assert len(declared_credentials) >= 10, sorted(declared_credentials)
