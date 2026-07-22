"""Every destination's stored config must resolve into dlt credentials (core#565).

BigQuery and Databricks could not complete a single run. `_to_dlt_credentials`
translated only for **SQL** types, so warehouse configs passed through untouched
and dlt never saw a credential it recognised:

    stored                    dlt reads
    keyfile_json              project_id, private_key, client_email, …
    host / token              server_hostname / access_token
    account                   host

The failure lands at the *last* step, after the user has created a GCP service
account or a Databricks PAT and filled in every field — and Test Connection
never exercises the dlt credential path, so nothing hints at it.

**These tests drive the config the connection form actually writes**, not the
config `CONFIG_SCHEMAS` describes. For BigQuery those disagreed — the form wrote
`keyfile_json`, the schema declared `service_account_json` — and reading the
schema is how you write a test that passes while production fails.

## Why ADC has to be neutralised

On a machine with `gcloud` Application Default Credentials, BigQuery resolves
**whatever you pass, including nothing at all**: dlt falls back to ADC and hands
back someone's real project. Measured during this fix — passing no credentials
resolved to a live personal project. A test written without disabling that would
pass locally, pass for the wrong reason, and say nothing about prod, where there
is no ADC. `_no_ambient_google_credentials` is therefore load-bearing, not
hygiene.
"""

import json
import os
import tempfile

import pytest

from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "datanika-test-565",
    "private_key_id": "kid",
    "private_key": "-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----\n",
    "client_email": "svc@datanika-test-565.iam.gserviceaccount.com",
    "client_id": "1",
    "token_uri": "https://oauth2.googleapis.com/token",
}

#: Exactly what `connection_state.py` writes for each type — see the `elif t ==`
#: branches. Deliberately not built from CONFIG_SCHEMAS: for BigQuery the two
#: disagreed, and the form is what reaches production.
STORED_CONFIG = {
    "bigquery": {
        "project": "datanika-test-565",
        "dataset": "analytics",
        "keyfile_json": json.dumps(SERVICE_ACCOUNT),
    },
    "databricks": {
        "host": "adb-1234.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/abc123",
        "token": "dapi-fake-token",
        "catalog": "main",
        "schema": "public",
    },
    "snowflake": {
        "account": "xy12345.us-east-1",
        "user": "loader",
        "password": "s3cret",
        "database": "ANALYTICS",
        "warehouse": "COMPUTE_WH",
        "schema": "PUBLIC",
    },
}


@pytest.fixture(autouse=True)
def _no_ambient_google_credentials(monkeypatch):
    """Make Application Default Credentials undiscoverable for these tests.

    Without this, BigQuery resolves from the developer's own gcloud login and
    every assertion below passes regardless of the code under test.
    """
    monkeypatch.setenv("CLOUDSDK_CONFIG", tempfile.mkdtemp(prefix="no-adc-"))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _resolved_credentials(svc, connection_type, config, tmp_path):
    """Resolve the destination the way `pipeline.run()` does — no network."""
    import dlt

    destination = svc.build_destination(connection_type, config)
    pipeline = dlt.pipeline(
        pipeline_name=f"t565_{connection_type}",
        destination=destination,
        dataset_name="probe_ds",
        pipelines_dir=str(tmp_path / "state"),
    )
    return pipeline.destination_client().config.credentials


class TestTheStoredConfigResolves:
    """The reported failure was `ConfigFieldMissingException` at resolution."""

    @pytest.mark.parametrize("connection_type", sorted(STORED_CONFIG))
    def test_credentials_resolve_without_raising(self, svc, connection_type, tmp_path):
        _resolved_credentials(svc, connection_type, STORED_CONFIG[connection_type], tmp_path)


class TestTheCredentialsCarryOurValues:
    """Resolution succeeding is not the same as our config being used.

    This is the distinction ADC hid: BigQuery resolved fine while ignoring
    everything passed to it.
    """

    def test_bigquery_uses_the_service_account_we_stored(self, svc, tmp_path):
        creds = _resolved_credentials(svc, "bigquery", STORED_CONFIG["bigquery"], tmp_path)

        assert creds.project_id == "datanika-test-565"
        assert creds.client_email == SERVICE_ACCOUNT["client_email"]
        assert creds.private_key == SERVICE_ACCOUNT["private_key"]

    def test_databricks_host_and_token_are_renamed(self, svc, tmp_path):
        creds = _resolved_credentials(svc, "databricks", STORED_CONFIG["databricks"], tmp_path)

        assert creds.server_hostname == "adb-1234.azuredatabricks.net"
        assert creds.access_token == "dapi-fake-token"
        assert creds.http_path == "/sql/1.0/warehouses/abc123"

    def test_snowflake_account_becomes_host(self, svc, tmp_path):
        creds = _resolved_credentials(svc, "snowflake", STORED_CONFIG["snowflake"], tmp_path)

        assert creds.host == "xy12345.us-east-1"
        assert creds.username == "loader"
        assert creds.database == "ANALYTICS"


class TestBigQueryKeyNames:
    """The form and the schema disagreed; both names must work."""

    def test_the_schemas_old_name_still_resolves(self, svc, tmp_path):
        """Connections saved against the old schema must not break."""
        config = {
            "project": "datanika-test-565",
            "dataset": "analytics",
            "service_account_json": json.dumps(SERVICE_ACCOUNT),
        }
        creds = _resolved_credentials(svc, "bigquery", config, tmp_path)

        assert creds.client_email == SERVICE_ACCOUNT["client_email"]

    def test_the_schema_now_matches_what_the_form_writes(self):
        from datanika.services.connection_schemas import CONFIG_SCHEMAS

        assert "keyfile_json" in CONFIG_SCHEMAS["bigquery"]["properties"]


class TestBadInputFailsClearly:
    def test_malformed_json_says_so(self, svc):
        with pytest.raises(DltRunnerError, match="not valid JSON"):
            svc.build_destination(
                "bigquery",
                {"project": "p", "dataset": "d", "keyfile_json": "{not json"},
            )

    @pytest.mark.parametrize("missing", ["project_id", "private_key", "client_email"])
    def test_a_truncated_key_file_names_the_missing_field(self, svc, missing):
        """dlt's own error blames `credentials` as a whole; this names the field."""
        partial = {k: v for k, v in SERVICE_ACCOUNT.items() if k != missing}

        with pytest.raises(DltRunnerError, match=missing):
            svc.build_destination(
                "bigquery",
                {"project": "p", "dataset": "d", "keyfile_json": json.dumps(partial)},
            )


class TestSynapseIsNotCovered:
    """Stated rather than silently skipped.

    `SynapseCredentials` resolution requires an ODBC driver to be installed, so
    it cannot be exercised here or in CI. core#565 lists synapse as *suspected*;
    it stays suspected — this file does not pretend otherwise.
    """

    def test_synapse_is_documented_as_unverified(self):
        assert "synapse" not in STORED_CONFIG
        assert os.name is not None  # keeps the test honest rather than empty
