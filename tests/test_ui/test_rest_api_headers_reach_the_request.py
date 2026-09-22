"""The REST API form's header field reaches the request it exists for (core#1467).

The form rendered an **Extra Headers (optional, JSON)** field and saved it as
``config["extra_headers"]`` — raw text — and nothing read that key: ``dlt_runner`` builds its client
from ``config["headers"]``.
So a user who filled the field got a connection that behaved exactly as if they had left it blank,
and nothing said so.

Product's decision on the issue: **keep the capability, and make the form write ``headers``**, the
key the runner already reads, rather than wiring a second key into the client — so there is no
precedence rule to design. Its criteria, each asserted below by behaviour:

1. the input is parsed as JSON and stored as an **object** under ``headers`` — never the raw
   string, which the runner would hand to the client as a mapping and break the connection;
2. invalid input is **refused at save**, naming the field, and the refusal does not echo what was
   typed — a header value is often a credential;
3. a connection saved before this still carries the inert ``extra_headers`` text: the edit form
   shows it so it is not lost, and **saving** migrates it. Nothing migrates it without that save —
   those values have never affected a request, and activating them silently would change what an
   existing connection sends.
"""

from __future__ import annotations

import json

import pytest

import datanika.services.dlt_runner as dlt_runner
from datanika.errors import UserFacingError
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.connection_state import ConnectionState

BASE_URL = "https://api.example.com"
RESOURCES = {"resources": [{"name": "things", "endpoint": {"path": "things"}}]}


def _state(**fields) -> ConnectionState:
    state = ConnectionState(parent_state=BaseState(init_substates=False), init_substates=False)
    state.form_type = "rest_api"
    state.form_base_url = BASE_URL
    for name, value in fields.items():
        setattr(state, name, value)
    return state


@pytest.fixture
def sent_client(monkeypatch):
    """What ``rest_api_source`` is handed as its client — the request the connector will make."""
    captured: dict = {}

    def _fake_rest_api_source(config, **_kwargs):
        captured.update(config["client"])
        return object()

    monkeypatch.setattr(dlt_runner, "rest_api_source", _fake_rest_api_source)
    monkeypatch.setattr(dlt_runner, "validate_egress_host", lambda url: None)

    def build(config: dict) -> dict:
        dlt_runner.DltRunnerService()._build_rest_api_source(config, dict(RESOURCES))
        return captured

    return build


class TestTheFieldReachesTheRequest:
    def test_headers_are_stored_as_an_object_under_the_key_the_runner_reads(self):
        config = _state(form_extra_headers='{"X-Api-Version": "2"}')._build_config()

        assert config["headers"] == {"X-Api-Version": "2"}
        assert isinstance(config["headers"], dict), "a raw string would break the client"

    def test_the_runner_sends_them(self, sent_client):
        config = _state(form_extra_headers='{"X-Api-Version": "2"}')._build_config()

        assert sent_client(config).get("headers") == {"X-Api-Version": "2"}

    def test_control_an_empty_field_sends_no_headers(self, sent_client):
        config = _state(form_extra_headers="")._build_config()

        assert "headers" not in config
        assert "headers" not in sent_client(config)


class TestBadInputIsRefusedAtSave:
    @pytest.mark.parametrize(
        "typed",
        [
            '{"X-Api-Key": "sk_live_SECRET"',  # truncated JSON
            '["X-Api-Version"]',  # not an object
            '{"X-Retries": 3}',  # a value that is not text
            '"sk_live_SECRET"',  # a bare string
        ],
    )
    def test_it_is_refused_naming_the_field_and_not_echoing_the_input(self, typed):
        with pytest.raises(UserFacingError) as refused:
            _state(form_extra_headers=typed)._build_config()

        message = str(refused.value)
        assert "Extra Headers" in message
        assert "SECRET" not in message, "a header value is often a credential"


class TestAConnectionSavedBeforeThisKeepsItsValue:
    OLD = {"base_url": BASE_URL, "extra_headers": '{"X-Tenant": "acme"}'}

    def test_the_edit_form_shows_the_stored_value(self):
        state = _state()
        state._populate_form_from_config("c", "rest_api", dict(self.OLD))

        assert state.form_extra_headers == '{"X-Tenant": "acme"}'

    def test_saving_moves_it_to_headers(self):
        state = _state()
        state._populate_form_from_config("c", "rest_api", dict(self.OLD))

        config = state._build_config()

        assert config == {"base_url": BASE_URL, "headers": {"X-Tenant": "acme"}}

    def test_nothing_migrates_it_without_a_save(self, sent_client):
        """The stored row, untouched, still sends nothing — the explicit save is the safeguard."""
        assert "headers" not in sent_client(dict(self.OLD))

    def test_a_stored_headers_object_round_trips_through_the_form(self):
        state = _state()
        state._populate_form_from_config(
            "c", "rest_api", {"base_url": BASE_URL, "headers": {"X-Tenant": "acme"}}
        )

        assert json.loads(state.form_extra_headers) == {"X-Tenant": "acme"}
        assert state._build_config()["headers"] == {"X-Tenant": "acme"}
