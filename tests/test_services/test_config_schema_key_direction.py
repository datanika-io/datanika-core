"""Published connection schemas use the keys the form already writes.

Contract: docs/specs/SPEC_CONFIG_SCHEMA_KEY_DIRECTION.md. The form and the
loader stay. A name the loader already accepts as an alias stays a property
and stays off ``required``. A name the loader does not read leaves the schema.
"""

import json
from pathlib import Path

from datanika.services.connection_schemas import CONFIG_SCHEMAS

_REQUIRED = {
    "jira": {"api_key", "email", "domain"},
    "shopify": {"api_key", "store"},
    "slack": {"api_key"},
    "salesforce": {"access_token", "instance_url"},
    "zendesk": {"subdomain", "email", "api_key"},
    "google_sheets": {"spreadsheet_url", "service_account_json"},
}

_OPTIONAL = {
    "jira": {"api_token"},
    "shopify": {"access_token"},
    "slack": {"bot_token"},
    "salesforce": {"api_key"},
    "zendesk": {"api_token", "domain"},
    "google_sheets": set(),
}

_ABSENT = {
    "jira": {"server_url"},
    "shopify": {"shop_url"},
    "slack": {"token"},
    "salesforce": {"client_id", "client_secret", "username", "password", "security_token"},
    "zendesk": set(),
    "google_sheets": {"spreadsheet_id"},
}

_PASSWORD = {
    "jira": {"api_key", "api_token"},
    "shopify": {"api_key", "access_token"},
    "slack": {"api_key", "bot_token"},
    "salesforce": {"access_token", "api_key"},
    "zendesk": {"api_key", "api_token"},
    "google_sheets": {"service_account_json"},
}

_FILE_TYPES = ("csv", "json", "parquet")

_GUIDE = Path(__file__).resolve().parents[2] / "AI_IMPORT_GUIDE.md"


def _accepts(schema: dict, config: dict) -> bool:
    """True when ``config`` meets this object's required keys and anyOf."""
    if set(config) - set(schema["properties"]):
        return False
    if any(key not in config for key in schema["required"]):
        return False
    if "anyOf" not in schema:
        return True
    return any(all(key in config for key in branch["required"]) for branch in schema["anyOf"])


def _guide_configs() -> dict[str, dict]:
    """One object per connection example. The guide writes each of these on one line."""
    found: dict[str, dict] = {}
    for line in _GUIDE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line.startswith("{") or '"connection_type"' not in line:
            continue
        body = json.loads(line)
        conn_type = body.get("connection_type")
        if conn_type in _REQUIRED or conn_type in _FILE_TYPES:
            found[conn_type] = body["config"]
    return found


def test_the_acceptance_check_rejects_an_alias_only_body():
    """The helper must be able to fail, or a green schema reading says nothing."""
    toy = {
        "properties": {"api_key": {}, "api_token": {}},
        "required": ["api_key"],
    }
    assert _accepts(toy, {"api_key": "x"})
    assert not _accepts(toy, {"api_token": "x"})
    file_schema = {
        "properties": {"bucket_url": {}, "uploaded_file_id": {}},
        "required": [],
        "anyOf": [{"required": ["bucket_url"]}, {"required": ["uploaded_file_id"]}],
    }
    assert _accepts(file_schema, {"bucket_url": "/data"})
    assert _accepts(file_schema, {"uploaded_file_id": 7})
    assert _accepts(file_schema, {"bucket_url": "/data", "uploaded_file_id": 7})
    assert not _accepts(file_schema, {})
    assert not _accepts(file_schema, {"path": "/data"})


def test_named_types_publish_the_keys_the_form_writes():
    for conn_type, required in _REQUIRED.items():
        schema = CONFIG_SCHEMAS[conn_type]
        assert set(schema["required"]) == required, conn_type
        assert set(schema["properties"]) == required | _OPTIONAL[conn_type], conn_type
        assert schema["additionalProperties"] is False
        for key in _ABSENT[conn_type]:
            assert key not in schema["properties"], f"{conn_type}.{key}"
        for key in _PASSWORD[conn_type]:
            assert schema["properties"][key].get("format") == "password", f"{conn_type}.{key}"
        alias_only = {key: "x" for key in _OPTIONAL[conn_type]}
        if alias_only:
            assert not _accepts(schema, alias_only), conn_type
        assert _accepts(schema, {key: "x" for key in required})


def test_file_schemas_require_one_locator_and_drop_path():
    for conn_type in _FILE_TYPES:
        schema = CONFIG_SCHEMAS[conn_type]
        assert set(schema["properties"]) == {"bucket_url", "uploaded_file_id"}, conn_type
        assert schema["properties"]["bucket_url"]["type"] == "string"
        assert schema["properties"]["uploaded_file_id"]["type"] == "integer"
        assert "format" not in schema["properties"]["bucket_url"]
        assert schema["required"] == []
        assert schema["anyOf"] == [
            {"required": ["bucket_url"]},
            {"required": ["uploaded_file_id"]},
        ]
        assert schema["additionalProperties"] is False
        assert "path" not in schema["properties"]
        assert _accepts(schema, {"bucket_url": "/data/orders.csv"})
        assert _accepts(schema, {"uploaded_file_id": 7})
        assert _accepts(schema, {"bucket_url": "/data/orders.csv", "uploaded_file_id": 7})
        assert not _accepts(schema, {})
        assert not _accepts(schema, {"path": "/data/orders.csv"})


def test_the_import_guide_examples_satisfy_the_published_schema():
    guide = _GUIDE.read_text(encoding="utf-8")
    configs = _guide_configs()
    assert set(configs) == set(_REQUIRED) | set(_FILE_TYPES)
    for conn_type, config in configs.items():
        assert _accepts(CONFIG_SCHEMAS[conn_type], config), (conn_type, config)
    assert 'CONFIG_SCHEMAS["salesforce"]' not in guide
    assert "client_id" not in configs["salesforce"]
    assert "token" not in configs["slack"]
    assert "path" not in configs["csv"]
    assert "spreadsheet_id" not in configs["google_sheets"]
