"""The openapi connection form must refuse a spec it cannot load from (core#1345, core#1348).

core#1345 (S2, QA 2026-09-15): the form saved a spec that parses to zero readable resources as
`resources: []`, showed nothing anywhere on the page, and every run then failed with
`OpenAPI source has no resource catalog`. The parse warnings reached only the API response.

core#1348 item 2: with Base URL left blank, a relative first `servers` URL such as `/api/v1` was
stored as the base URL, which gives a connection with no host. `SPEC_FIELD_REQUIREDNESS` §2.7 leaves
the openapi Base URL unmarked *because* the form fills it from the spec. That is only honest if the
form refuses what it cannot fill.

Both refusals happen in `_build_config`, which `save_connection` calls before any service call.
The `save_connection` tests prove "nothing is saved" as behaviour, and their control proves the
harness reaches `create_connection` at all. Without that control, an empty recorder would be
satisfied by a harness that never got that far.

That control earned its place on the first run. `save_connection` builds an `EncryptionService`
from `settings.credential_encryption_key` before `_build_config`, and the test environment's default
key is not a valid Fernet key. All three tests, the control included, failed on that `ValueError`,
which was a red about this harness. The fixture now supplies a real key. The second harness red came
next: the handler writes `error_message`, a var declared on `BaseState`, and a standalone state has
no parent to hold it. `_save` now gives the state a real `BaseState` parent.
"""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import json

import pytest
from cryptography.fernet import Fernet

from datanika.config import settings
from datanika.errors import UserFacingError
from datanika.services.connection_service import ConnectionService
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.connection_state import ConnectionState

WIDGET = {
    "type": "object",
    "required": ["id"],
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
}


def _spec(*, server: str = "https://api.example.com", media_type: str = "application/json") -> str:
    rows = {"schema": {"type": "array", "items": WIDGET}}
    response = {"description": "ok", "content": {media_type: rows}}
    return json.dumps(
        {
            "openapi": "3.0.1",
            "info": {"title": "t", "version": "1"},
            "servers": [{"url": server}],
            "paths": {
                "/widgets": {"get": {"operationId": "listWidgets", "responses": {"200": response}}}
            },
        }
    )


READABLE = _spec()
#: Parses, with one warning and zero resources: its only response is not JSON.
NO_READABLE_ENDPOINT = _spec(media_type="text/plain")
RELATIVE_SERVER = _spec(server="/api/v1")


def _field_default(field):
    """A declared var's default. List vars declare `default_factory` and carry MISSING instead."""
    default = getattr(field, "default", None)
    if default is dataclasses.MISSING:
        factory = getattr(field, "default_factory", dataclasses.MISSING)
        return factory() if factory is not dataclasses.MISSING else None
    return copy.deepcopy(default)


class _ConnectionForm:
    """A stand-in `self` with ConnectionState's declared vars, so the REAL `_build_config` runs."""

    _build_config = ConnectionState._build_config

    def __init__(self, **values):
        for name, field in ConnectionState.get_fields().items():
            setattr(self, name, _field_default(field))
        self.form_type = "openapi"
        for name, value in values.items():
            setattr(self, name, value)


class TestTheFormRefusesWhatItCannotLoad:
    def test_a_spec_with_no_readable_endpoint_is_refused_with_the_parsers_reason(self):
        with pytest.raises(UserFacingError) as refused:
            _ConnectionForm(form_openapi_spec=NO_READABLE_ENDPOINT)._build_config()

        assert "Skipped GET /widgets" in str(refused.value), (
            "the refusal must carry the parser's own reason, which today reaches only the API"
        )

    def test_a_relative_servers_url_with_a_blank_base_url_is_refused_naming_the_field(self):
        with pytest.raises(UserFacingError) as refused:
            _ConnectionForm(form_openapi_spec=RELATIVE_SERVER)._build_config()

        assert "Base URL" in str(refused.value)

    def test_control_a_readable_spec_builds_its_catalog(self):
        config = _ConnectionForm(form_openapi_spec=READABLE)._build_config()

        assert [resource["name"] for resource in config["resources"]] == ["widgets"]
        assert config["base_url"] == "https://api.example.com"

    def test_control_a_relative_servers_url_saves_once_base_url_is_filled(self):
        config = _ConnectionForm(
            form_openapi_spec=RELATIVE_SERVER, form_base_url="https://api.example.com"
        )._build_config()

        assert config["base_url"] == "https://api.example.com"


class _CreateReachedError(Exception):
    """Raised by the create recorder after recording, so the handler stops before real I/O."""


@pytest.fixture
def created(monkeypatch) -> list[dict]:
    calls: list[dict] = []

    async def _allow(self, role):
        return True

    class _Auth:
        class current_org:  # noqa: N801 - mirrors AuthState's attribute name
            id = 1

        class current_user:  # noqa: N801 - mirrors AuthState's attribute name
            id = 1

    async def _get_state(self, state_cls):
        return _Auth()

    def _record_create(self, session, org_id, name, connection_type, config, **kwargs):
        calls.append(config)
        raise _CreateReachedError

    monkeypatch.setattr(settings, "credential_encryption_key", Fernet.generate_key().decode())
    monkeypatch.setattr(ConnectionState, "_check_role", _allow)
    monkeypatch.setattr(ConnectionState, "get_state", _get_state)
    monkeypatch.setattr(
        "datanika.ui.state.connection_state.get_sync_session",
        lambda: contextlib.nullcontext(object()),
    )
    monkeypatch.setattr(ConnectionService, "create_connection", _record_create)
    return calls


async def _save(**values) -> ConnectionState:
    # `error_message` and `is_quota_error` are declared on BaseState, and a standalone
    # ConnectionState has no parent state instance, so the handler's first write to either raised
    # AttributeError. That was a red about this harness (the second one; see the module
    # docstring). A real BaseState parent holds those vars.
    parent = BaseState(init_substates=False)
    state = ConnectionState(parent_state=parent, init_substates=False)
    state.form_name = "fake rest api"
    state.form_type = "openapi"
    for name, value in values.items():
        setattr(state, name, value)
    [event async for event in state.save_connection()]
    return state


class TestNothingIsSaved:
    async def test_control_a_readable_spec_reaches_create_connection(self, created):
        await _save(form_openapi_spec=READABLE)

        assert len(created) == 1, (
            "the harness never reached create_connection, so the two refusals below would pass "
            "without proving anything"
        )

    async def test_a_spec_with_no_readable_endpoint_saves_nothing(self, created):
        state = await _save(form_openapi_spec=NO_READABLE_ENDPOINT)

        assert created == []
        assert "Skipped GET /widgets" in state.error_message

    async def test_a_relative_servers_url_saves_nothing(self, created):
        state = await _save(form_openapi_spec=RELATIVE_SERVER)

        assert created == []
        assert "Base URL" in state.error_message
