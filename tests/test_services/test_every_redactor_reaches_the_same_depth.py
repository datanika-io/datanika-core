"""Every redactor that protects a connection config reaches the same secrets, at the same depth.

Three functions protect one object — a decrypted connection config — and each answers a different
consumer: a run's stored text, the message a connection test shows, and an exported backup file.
They now derive from one walker, so a value one of them removes is a value all three remove.

**Why a key-name oracle is not enough on its own.** `SECRET_CONFIG_KEYS` names keys, and some
credentials are not stored under a name the product chooses:

* ``headers`` is declared free-form by ``CONFIG_SCHEMAS["openapi"]`` and handed to the HTTP client
  on every request, so the names inside it are the **user's** — `X-Api-Key`, `Private-Token`,
  `Authorization`, anything;
* ``auth`` is written by the connection form, and one of its branches stores what the user typed
  into a field the form renders ``secret=True`` under a key that is not a credential key.

So :data:`SECRET_CONTAINER_KEYS` declares the **container**, and everything in it is a credential
except :data:`CONTAINER_STRUCTURE_KEYS` — which is what lets a backup stay importable.

**The controls are half the file**, because "the marker is gone" is equally explained by a redactor
that shreds everything: a non-credential field must come through untouched, a structure key must
survive an export, and an export/import round trip must restore the original value.
"""

from __future__ import annotations

import json

import pytest

from datanika.services.backup_service import REDACTED, BackupService
from datanika.services.connection_service import (
    CONTAINER_STRUCTURE_KEYS,
    SECRET_CONTAINER_KEYS,
    _redact_secrets,
    redact_run_text,
)

#: Carries `@`, `/` and a space on purpose: with an alphanumeric marker three of the five
#: spellings collapse to the same string and an arm can pass while a whole encoding is unhandled.
MARKER = "surface@Zq7Kx/marker depthEnd"

#: A credential inside a free-form container, and one inside the container stored as JSON *text*.
IN_A_HEADER = {"base_url": "https://api.example.com", "headers": {"X-Api-Key": MARKER}}
IN_JSON_TEXT = {
    "base_url": "https://api.example.com",
    "extra_headers": json.dumps({"Authorization": f"Bearer {MARKER}"}),
}
IN_AUTH = {
    "base_url": "https://api.example.com",
    "auth": {"type": "http_basic", "username": MARKER, "password": ""},
}

SHAPES = {"header": IN_A_HEADER, "json-text": IN_JSON_TEXT, "auth": IN_AUTH}


def _echo(config: dict) -> str:
    """Text shaped like a client quoting back the config it was handed.

    ⚠️ ``json.dumps`` renders a JSON-text value with its quotes **escaped**, which is none of the
    spellings a replacement of the value *as stored* would look for. That is why this, and not a
    plainer text, is the shape the assertions run against.
    """
    return f"the request was refused: client config was {json.dumps(config)} -- 401"


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_the_instrument_can_read_its_own_marker_with_nothing_held(shape):
    """Without this, every `not in` below is equally explained by a search that sees nothing."""
    text = _echo(SHAPES[shape])
    assert MARKER in text
    assert MARKER in (redact_run_text(text, []) or "")
    assert MARKER in (_redact_secrets(text, {}) or "")


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_a_run_text_carries_no_credential_from_a_container(shape):
    assert MARKER not in (redact_run_text(_echo(SHAPES[shape]), [SHAPES[shape]]) or "")


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_a_connection_test_message_carries_no_credential_from_a_container(shape):
    config = SHAPES[shape]
    assert MARKER not in (_redact_secrets(_echo(config), config) or "")


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_an_exported_backup_carries_no_credential_from_a_container(shape):
    assert MARKER not in json.dumps(BackupService._redact(SHAPES[shape]))


# ---------------------------------------------------------------------------------------------
# Controls — a redactor that shredded everything would pass every assertion above
# ---------------------------------------------------------------------------------------------


def test_a_field_the_schema_does_not_declare_a_credential_survives():
    config = {"base_url": "https://api.example.com", "headers": {"X-Api-Key": MARKER}}
    text = f"connecting to {config['base_url']} failed: authentication failed"

    assert "authentication failed" in (redact_run_text(text, [config]) or "")
    assert config["base_url"] in (redact_run_text(text, [config]) or "")


def test_an_exported_backup_keeps_the_shape_the_importer_needs():
    """The reason a container is redacted leaf by leaf rather than replaced whole.

    ``type``, ``name`` and ``location`` say *which auth scheme*, and a header's key says *which
    header*. Blanking the container removes the credential and the description of how to re-enter
    it, so an import with nothing to carry forward leaves the user re-deriving the scheme.
    """
    auth = {"type": "api_key", "name": "X-Key", "location": "header", "api_key": MARKER}
    exported = BackupService._redact({"base_url": "https://api.example.com", "auth": auth})

    assert exported["auth"]["type"] == "api_key"
    assert exported["auth"]["name"] == "X-Key"
    assert exported["auth"]["location"] == "header"
    assert exported["auth"]["api_key"] == REDACTED
    assert "X-Api-Key" in BackupService._redact(IN_A_HEADER)["headers"], (
        "the header's name is not the secret; its value is"
    )


def test_an_export_import_round_trip_restores_a_nested_credential():
    """The round trip is what makes redaction non-destructive, and it has to descend too."""
    stored = {"base_url": "https://api.example.com", "headers": {"X-Api-Key": MARKER}}
    exported = BackupService._redact(stored)

    resolved, needs_credentials = BackupService._resolve_redactions(exported, stored)

    assert resolved == stored, resolved
    assert needs_credentials is False


def test_a_round_trip_with_nothing_stored_asks_for_the_credential():
    """And the opposite direction: an absent secret must be *reported*, not silently kept."""
    exported = BackupService._redact(IN_A_HEADER)

    resolved, needs_credentials = BackupService._resolve_redactions(exported, None)

    assert needs_credentials is True
    assert REDACTED not in json.dumps(resolved), "the sentinel was stored as if it were a value"


def test_the_container_and_structure_declarations_do_not_overlap():
    """A key that is both would be a container key whose contents are never secret."""
    assert not (SECRET_CONTAINER_KEYS & CONTAINER_STRUCTURE_KEYS)
    assert SECRET_CONTAINER_KEYS and CONTAINER_STRUCTURE_KEYS
