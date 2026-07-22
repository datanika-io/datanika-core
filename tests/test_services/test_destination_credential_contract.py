"""No destination may hand dlt a credential field it does not declare (core#577).

core#565 proved BigQuery and Databricks broken and #576 fixed them. Synapse and
Snowflake were left *suspected* — and "probably fine" is where this bites, since
`redshift`/`clickhouse` were also only ever inferred from a mapping table rather
than checked.

**This file checks all of them at once, by comparing what
`_to_dlt_credentials` produces against the fields dlt's own credential class
declares.** That comparison needs no network and no driver, which matters:
`synapse` could not be verified in #565 because resolving it requires an ODBC
driver present neither locally nor in CI. The driver is needed to *connect*, not
to check the mapping — so the thing that blocked verification never actually had
to.

It immediately found the gap: `synapse` was in neither `_RENAME_USER_TYPES` nor
`SOURCE_DRIVERNAME_MAP`, so `user` reached dlt un-renamed and no `username` was
ever set. Fixed in the same change.

Outcome for the four destinations #577 asks about:

* **synapse** — proven **broken**, then fixed here.
* **snowflake** — proven **correct at the config layer**: `account` → `host`, and
  every produced key is one dlt declares. A *live* run stays unobtainable (the
  trial signup is blocked by Snowflake's risk engine — CEO-parked), so this is
  the strongest statement available and is stated as such rather than implied.
* **redshift, clickhouse** — confirmed clean rather than inferred.

`drivername` is deliberately not required: `SnowflakeCredentials` and
`SynapseCredentials` both declare a working class-level default (`'snowflake'`,
`'synapse'`), checked rather than assumed.
"""

import dataclasses
import json
import tempfile

import pytest

from datanika.services.dlt_runner import DltRunnerService

SERVICE_ACCOUNT = json.dumps(
    {
        "type": "service_account",
        "project_id": "p",
        "private_key_id": "k",
        "private_key": "-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----\n",
        "client_email": "s@p.iam.gserviceaccount.com",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
)

#: What the connection form writes for each destination — the `elif t ==`
#: branches in `connection_state.py`. Deliberately the form's keys, not
#: `CONFIG_SCHEMAS`': for BigQuery those disagreed (core#565), and the form is
#: what reaches production.
STORED_CONFIG = {
    "postgres": {"host": "h", "port": 5432, "database": "db", "user": "u", "password": "p"},
    "redshift": {"host": "h", "port": 5439, "database": "db", "user": "u", "password": "p"},
    "clickhouse": {"host": "h", "port": 8123, "database": "db", "user": "u", "password": "p"},
    "snowflake": {
        "account": "xy1.us-east-1",
        "user": "u",
        "password": "p",
        "database": "db",
        "warehouse": "wh",
        "schema": "sc",
    },
    "synapse": {"host": "h", "port": 1433, "database": "db", "user": "u", "password": "p"},
    "bigquery": {"project": "p", "dataset": "d", "keyfile_json": SERVICE_ACCOUNT},
    "databricks": {
        "host": "h",
        "http_path": "/sql/1.0/x",
        "token": "t",
        "catalog": "main",
        "schema": "sc",
    },
}


@pytest.fixture(autouse=True)
def _no_ambient_cloud_credentials(monkeypatch):
    """Ambient credentials lie — see core#565.

    With `gcloud` ADC present, BigQuery resolved a config containing **no
    credentials at all** and returned a live personal project. Any cloud SDK
    with a default-credential chain needs the same treatment, which is why this
    is autouse rather than applied to the Google cases only.
    """
    monkeypatch.setenv("CLOUDSDK_CONFIG", tempfile.mkdtemp(prefix="no-adc-"))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _declared_credential_fields(destination_name: str) -> set[str]:
    """Field names dlt's credential class for this destination declares."""
    import dlt

    factory = getattr(dlt.destinations, destination_name)
    for field in dataclasses.fields(factory.spec):
        if field.name == "credentials":
            annotated = field.type
            break
    else:  # pragma: no cover - every destination spec has credentials
        raise AssertionError(f"{destination_name} spec declares no credentials field")

    names: set[str] = set()
    for candidate in getattr(annotated, "__args__", (annotated,)):
        if dataclasses.is_dataclass(candidate):
            names |= {f.name for f in dataclasses.fields(candidate) if not f.name.startswith("_")}
    return names


class TestNoUnknownCredentialFields:
    @pytest.mark.parametrize("destination", sorted(STORED_CONFIG))
    def test_every_produced_key_is_one_dlt_declares(self, svc, destination):
        """The check that found synapse.

        dlt ignores or rejects fields it does not know, and the symptom is a
        run that fails at the very end — after the user has created a service
        account or a PAT and filled in every field.
        """
        produced = set(svc._to_dlt_credentials(destination, STORED_CONFIG[destination]))
        declared = _declared_credential_fields(destination)

        unknown = sorted(produced - declared)
        assert not unknown, (
            f"{destination}: {unknown} are sent to dlt but not declared by its credential "
            f"class. dlt declares {sorted(declared)}. A key it does not know is a key it "
            "cannot authenticate with — see core#565 and core#577."
        )

    def test_the_scan_reads_real_dlt_classes(self):
        """Anti-vacuity: an empty `declared` set would make every case pass."""
        for destination in ("postgres", "bigquery", "databricks"):
            declared = _declared_credential_fields(destination)
            assert len(declared) >= 3, f"{destination} declared only {declared}"

        assert "server_hostname" in _declared_credential_fields("databricks")
        assert "project_id" in _declared_credential_fields("bigquery")


class TestTheUsernameRenameReachesEveryone:
    """`user` is what the form stores; `username` is what dlt reads."""

    @pytest.mark.parametrize(
        "destination", ["postgres", "redshift", "clickhouse", "snowflake", "synapse"]
    )
    def test_user_becomes_username(self, svc, destination):
        creds = svc._to_dlt_credentials(destination, STORED_CONFIG[destination])

        assert creds.get("username") == "u", f"{destination} did not rename user → username"
        assert "user" not in creds


class TestSnowflakeAndSynapseSpecifically:
    """The two #577 names, each resolved to a definite statement."""

    def test_snowflake_account_is_sent_as_host(self, svc):
        creds = svc._to_dlt_credentials("snowflake", STORED_CONFIG["snowflake"])

        assert creds["host"] == "xy1.us-east-1"
        assert "account" not in creds

    def test_synapse_sends_a_username_not_a_user(self, svc):
        """Proven broken by this file, then fixed — no ODBC driver required."""
        creds = svc._to_dlt_credentials("synapse", STORED_CONFIG["synapse"])

        assert creds["username"] == "u"
        assert "user" not in creds

    @pytest.mark.parametrize("destination", ["snowflake", "synapse"])
    def test_drivername_is_defaulted_by_dlt_not_by_us(self, destination):
        """We send no `drivername` for these two — checked, not assumed."""
        import dlt

        factory = getattr(dlt.destinations, destination)
        for field in dataclasses.fields(factory.spec):
            if field.name == "credentials":
                candidates = getattr(field.type, "__args__", (field.type,))
                break
        defaults = {
            f.default
            for c in candidates
            if dataclasses.is_dataclass(c)
            for f in dataclasses.fields(c)
            if f.name == "drivername"
        }
        assert destination in defaults, (
            f"{destination} has no working default drivername {defaults}; we send none, so "
            "one of the two has to supply it"
        )


class TestEveryDestinationIsCovered:
    """A new destination must not be able to skip this check.

    core#565 happened because warehouse types were added without anyone
    checking that their stored keys were the keys dlt reads. A contract that
    covers only the destinations someone remembered to list would leave exactly
    that door open.
    """

    def test_no_supported_destination_is_missing_from_the_table(self):
        supported = set(DltRunnerService.SUPPORTED_DESTINATION_TYPES)
        # duckdb/sqlite/mssql/mysql are file- or DSN-shaped and covered by the
        # SQL path tests; everything credential-bearing must be here.
        untested = supported - set(STORED_CONFIG) - {"duckdb", "sqlite", "mssql", "mysql"}
        assert not untested, (
            f"{sorted(untested)} are supported destinations with no entry in STORED_CONFIG, "
            "so nothing checks that their stored keys are the ones dlt reads — the core#565 "
            "gap. Add them with the config the connection form writes."
        )
