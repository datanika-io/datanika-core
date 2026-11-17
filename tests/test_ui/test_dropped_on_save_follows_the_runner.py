"""A stored alias must still be the credential the runner reads after Save.

core#662. ``_DROPPED_ON_SAVE`` records schema keys the structured form does not
write back. For the SaaS aliases the runner reads the form's canonical name
first and the schema alias second (re-measured on ``dlt_runner`` and
``SAAS_PROBES``: slack ``api_key`` then ``bot_token``; salesforce
``access_token`` then ``api_key``). Opening an alias-only connection and
saving it therefore blanks the credential even though a run would have
accepted it.

ClickHouse is not an alias pair. The destination reads ``table_engine_type``.
The schema's ``cluster_replication`` boolean is a name neither serialiser
writes. REST API is the same shape: the source reads ``auth``, and the four
``auth_*`` schema keys are not that name.

``mongodb.auth_source`` is measured here and must survive. It is not a ledger
entry, and this file does not implement TLS verification (core#626).
"""

import pytest

from datanika.services.connection_schemas import CONFIG_SCHEMAS
from tests.test_ui.test_connection_config_roundtrip import (
    FormStub,
    dropped_keys,
    synthetic_config,
)


def _saved(conn_type: str, config: dict) -> dict:
    stub = FormStub()
    stub._populate_form_from_config("probe", conn_type, config)
    return stub._build_config()


@pytest.mark.parametrize(
    ("conn_type", "stored", "expected"),
    [
        (
            "salesforce",
            {"api_key": "tok", "instance_url": "https://example.my.salesforce.com"},
            {
                "access_token": "tok",
                "api_key": "tok",
                "instance_url": "https://example.my.salesforce.com",
            },
        ),
        (
            "shopify",
            {"access_token": "tok", "store": "acme"},
            {"api_key": "tok", "access_token": "tok", "store": "acme"},
        ),
        (
            "jira",
            {"api_token": "tok", "email": "a@b.c", "domain": "acme"},
            {
                "api_key": "tok",
                "api_token": "tok",
                "email": "a@b.c",
                "domain": "acme",
            },
        ),
        (
            "slack",
            {"bot_token": "xoxb-probe"},
            {"api_key": "xoxb-probe", "bot_token": "xoxb-probe"},
        ),
        (
            "zendesk",
            {"api_token": "tok", "domain": "acme", "email": "a@b.c"},
            {
                "api_key": "tok",
                "api_token": "tok",
                "subdomain": "acme",
                "domain": "acme",
                "email": "a@b.c",
            },
        ),
    ],
)
def test_an_alias_only_config_keeps_the_name_the_runner_reads(conn_type, stored, expected):
    built = _saved(conn_type, stored)
    assert expected.items() <= built.items()


def test_both_salesforce_names_collapse_to_the_one_the_runner_prefers():
    built = _saved(
        "salesforce",
        {
            "access_token": "canonical",
            "api_key": "alias",
            "instance_url": "https://example.my.salesforce.com",
        },
    )
    assert built["access_token"] == "canonical"
    assert built["api_key"] == "canonical"


def test_both_zendesk_domain_names_collapse_to_subdomain():
    built = _saved(
        "zendesk",
        {
            "subdomain": "canon",
            "domain": "alias",
            "api_key": "tok",
            "email": "a@b.c",
        },
    )
    assert built["subdomain"] == "canon"
    assert built["domain"] == "canon"
    assert built["api_key"] == "tok"


def test_clickhouse_publishes_the_engine_name_the_runner_reads():
    props = CONFIG_SCHEMAS["clickhouse"]["properties"]
    assert "table_engine_type" in props
    assert props["table_engine_type"]["type"] == "string"
    assert "cluster_replication" not in props


def test_a_clickhouse_engine_other_than_replicated_survives_save():
    built = _saved(
        "clickhouse",
        {
            "host": "ch",
            "database": "d",
            "user": "u",
            "table_engine_type": "shared_merge_tree",
        },
    )
    assert built.get("table_engine_type") == "shared_merge_tree"


def test_the_ratchet_placeholder_engine_survives_save():
    built = _saved(
        "clickhouse",
        {"host": "ch", "database": "d", "user": "u", "table_engine_type": "probe-value"},
    )
    assert built.get("table_engine_type") == "probe-value"


def test_a_stored_cluster_replication_flag_saves_as_the_engine_the_runner_reads():
    built = _saved(
        "clickhouse",
        {"host": "ch", "database": "d", "user": "u", "cluster_replication": True},
    )
    assert built.get("table_engine_type") == "replicated_merge_tree"
    assert "cluster_replication" not in built


def test_unticking_clickhouse_replication_does_not_write_the_engine_back():
    stub = FormStub()
    stub._populate_form_from_config(
        "probe",
        "clickhouse",
        {
            "host": "ch",
            "database": "d",
            "user": "u",
            "table_engine_type": "replicated_merge_tree",
        },
    )
    stub.form_cluster_replication = False
    built = stub._build_config()
    assert built.get("table_engine_type") != "replicated_merge_tree"


def test_rest_api_publishes_the_auth_object_the_runner_reads():
    props = CONFIG_SCHEMAS["rest_api"]["properties"]
    assert "auth" in props
    assert props["auth"]["type"] == "object"
    for key in ("auth_type", "auth_token", "auth_user", "auth_password"):
        assert key not in props


def test_rest_api_auth_object_survives_save():
    auth = {"type": "bearer", "token": "sekrit"}
    built = _saved("rest_api", {"base_url": "https://api.example.com", "auth": auth})
    assert built.get("auth") == auth


def test_rest_api_api_key_field_still_writes_api_key_and_not_auth():
    stub = FormStub()
    stub.form_type = "rest_api"
    stub.form_base_url = "https://api.example.com"
    stub.form_api_key = "my-key"
    built = stub._build_config()
    assert built["api_key"] == "my-key"
    assert "auth" not in built


def test_mongodb_auth_source_survives_a_structured_save():
    """Fresh round trip. Leave it off the ledger only while this holds."""
    lost = dropped_keys("mongodb", synthetic_config(CONFIG_SCHEMAS["mongodb"]))
    assert "auth_source" not in lost
