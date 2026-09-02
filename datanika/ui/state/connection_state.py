"""Connection state for Reflex UI."""

import json
import logging
import re

import reflex as rx
from pydantic import BaseModel

from datanika.config import settings
from datanika.models.connection import ConnectionType
from datanika.services.connection_service import (
    ConnectionService,
)
from datanika.services.encryption import EncryptionService
from datanika.ui.state.base_state import BaseState, get_sync_session

logger = logging.getLogger(__name__)

# SaaS source types that use endpoint/resource selection (not SQL mode).
#
# Must equal `dlt_runner.SUPPORTED_SAAS_TYPES` — the loader's view of which
# connectors are SaaS. `tests/test_connector_type_contracts.py` asserts it.
SAAS_SOURCE_TYPES = {
    "stripe",
    "github",
    "hubspot",
    "salesforce",
    "shopify",
    "jira",
    "slack",
    "google_analytics",
    "google_ads",
    "facebook_ads",
    "zendesk",
    "airtable",
    "notion",
    "pipedrive",
    "freshdesk",
    "asana",
}

# File-based source types
FILE_SOURCE_TYPES = {"s3", "csv", "json", "parquet"}

# Source types that need their own config instead of SQL mode.
#
# Membership here is what hides the SQL block (Load Mode / Write Disposition /
# Source schema / Table names) on the upload form — `uploads.py` renders it
# under `~UploadState.form_is_non_sql_source`. **Omission is not neutral**: an
# unclassified type falls through to the SQL branch and is shown four controls
# its loader never reads. That was core#503 — pipedrive/freshdesk/asana were
# added to SOURCE_TYPES and to `dlt_runner.SUPPORTED_SAAS_TYPES`, but not here,
# so an HTTP API asked the user for a source schema and table names.
#
# `tests/test_connector_type_contracts.py` fails if a source type is left
# unclassified. Add new connectors there and here in the same change.
NON_SQL_SOURCE_TYPES = (
    SAAS_SOURCE_TYPES
    | FILE_SOURCE_TYPES
    | {
        "google_sheets",
        "mongodb",
        "rest_api",
        "kafka",
        # Resources come from the uploaded spec, not from a SQL schema.
        "openapi",
    }
    # pipedrive/freshdesk/asana used to be listed explicitly here, because
    # core#503 fixed their SQL-control leak while deliberately keeping them out
    # of SAAS_SOURCE_TYPES — that set renders the endpoint picker, and the
    # picker did nothing (core#532). Now that the selection is honoured they
    # belong in SAAS_SOURCE_TYPES, which this union already covers.
)

# Default available endpoints per SaaS connector — the checkbox list, and the
# names the selection is resolved against.
#
# These MUST be the resource names the loader actually builds. They were not:
# the original list described the dlt *verified sources*, while what runs is the
# REST fallback (no verified-source package is installed — core#543), so Stripe
# offered `Product`/`Price` where the fallback defines `products`/`prices`, and
# HubSpot offered six resources where three exist. While the selection was
# ignored (core#532) that was merely misleading; once honoured it would fail the
# run. Corrected below against the built sources.
#
# `tests/test_services/test_saas_endpoint_selection.py` builds each source and
# asserts both directions — nothing offered that isn't built, nothing built that
# isn't offered. Update that test's expectations by running it, not by hand.
SAAS_DEFAULT_ENDPOINTS: dict[str, list[str]] = {
    "stripe": ["charges", "customers", "invoices", "prices", "products", "subscriptions"],
    "github": ["issues", "pulls", "commits", "stargazers"],
    "hubspot": ["companies", "contacts", "deals"],
    "salesforce": ["accounts", "contacts", "opportunities"],
    "shopify": ["orders", "products", "customers"],
    "jira": ["issues", "projects"],
    "slack": ["channels", "users"],
    # Both Google connectors expose a single `report` resource, because both are
    # a *query* rather than a set of collections: GA4 takes dimensions+metrics,
    # Ads takes GAQL. One endpoint each is the honest offer — inventing several
    # would tick boxes the loader cannot fetch, which is the core#532 mistake.
    "google_analytics": ["report"],
    "google_ads": ["report"],
    # facebook_ads now builds via the Graph API fallback. `leads` is gone on
    # purpose: it is not an ad-account edge (lead records hang off a lead-gen
    # form, not the account), so offering it would tick a box for a resource
    # the loader cannot fetch — the core#532 mistake in miniature.
    "facebook_ads": ["ad_sets", "ads", "campaigns", "creatives"],
    "zendesk": ["organizations", "tickets", "users"],
    "airtable": ["tables"],
    "notion": ["databases", "pages"],
    "pipedrive": [
        "activities",
        "deals",
        "organizations",
        "persons",
        "pipelines",
        "stages",
        "users",
    ],
    "freshdesk": ["agents", "companies", "contacts", "groups", "tickets"],
    "asana": ["projects", "tags", "tasks", "users", "workspaces"],
}

# Default ports for database connection types
_DEFAULT_PORTS: dict[str, str] = {
    "postgres": "5432",
    "mysql": "3306",
    "mssql": "1433",
    "redshift": "5439",
    "mongodb": "27017",
    "clickhouse": "8123",
    "synapse": "1433",
    "oracle": "1521",
}

# Connection types that use the SQL database form group (host/port/user/pass/db/schema)
_DB_TYPES = {"postgres", "mysql", "mssql", "redshift", "clickhouse", "synapse", "oracle"}

# Mapping of pipeline-template ``source_config_defaults`` keys to the
# matching ConnectionState ``form_*`` attribute. Module-level (not a class
# attribute) so it isn't picked up as a Reflex state var, and so the
# template-prefill helper stays trivially testable via FakeState.
#
# Add new entries here when a template needs to prefill a non-credential
# field that isn't already covered. Never add password / secret / api_key /
# service_account_json — templates carry safe defaults only.
_TEMPLATE_FORM_FIELD_MAP: dict[str, str] = {
    "host": "form_host",
    "port": "form_port",
    "database": "form_database",
    "schema": "form_schema",
    "path": "form_path",
    "project": "form_project",
    "dataset": "form_dataset",
    "account": "form_account",
    "warehouse": "form_warehouse",
    "role": "form_role",
    "bucket_url": "form_bucket_url",
    "base_url": "form_base_url",
    "property_id": "form_property_id",
    "bootstrap_servers": "form_bootstrap_servers",
    "topics": "form_topics",
}


def _fill_openapi_auth(scheme: dict, token: str) -> dict:
    """Merge a user-supplied token into a detected OpenAPI auth scheme."""
    t = scheme.get("type")
    if t == "api_key":
        return {
            "type": "api_key",
            "name": scheme.get("name", ""),
            "location": scheme.get("location", "header"),
            "api_key": token,
        }
    if t == "http_basic":
        return {"type": "http_basic", "username": token, "password": ""}
    return {"type": "bearer", "token": token}


def _validate_connection_form(
    name: str,
    conn_type: str,
    use_raw_json: bool,
    *,
    host: str = "",
    port: str = "",
    database: str = "",
    path: str = "",
    project: str = "",
    dataset: str = "",
    account: str = "",
    user: str = "",
    bucket_url: str = "",
    base_url: str = "",
    uploaded_file_id: int = 0,
    spreadsheet_url: str = "",
    service_account_json: str = "",
) -> str:
    """Return an error message if required fields are missing, or '' if valid."""
    if not name.strip():
        return "Connection name is required"

    # core#593: the type picker defaults to "" (placeholder), so an untouched
    # form has no type. Reject before the raw-JSON early return — save() calls
    # ConnectionType(form_type) regardless of raw mode, so an empty type there
    # would surface as a generic "Failed to save" instead of this clear message.
    if not conn_type.strip():
        return "Connection type is required"

    if use_raw_json:
        return ""

    if conn_type in _DB_TYPES:
        if not host.strip():
            return "Host is required"
        if not port.strip():
            return "Port is required"
        if not database.strip():
            return "Database is required"
    elif conn_type == "sqlite":
        if not path.strip():
            return "Database path is required"
    elif conn_type == "bigquery":
        if not project.strip():
            return "GCP Project ID is required"
        if not dataset.strip():
            return "Dataset is required"
    elif conn_type == "snowflake":
        if not account.strip():
            return "Account is required"
        if not user.strip():
            return "User is required"
        if not database.strip():
            return "Database is required"
    elif conn_type == "s3":
        if not bucket_url.strip():
            return "Bucket URL is required"
    elif conn_type in ("csv", "json", "parquet"):
        if not bucket_url.strip() and not uploaded_file_id:
            return "File upload or file path is required"
    elif conn_type == "mongodb":
        if not host.strip():
            return "Host is required"
        if not database.strip():
            return "Database is required"
    elif conn_type == "google_sheets":
        if not spreadsheet_url.strip():
            return "Spreadsheet URL is required"
        if not service_account_json.strip():
            return "Service Account JSON is required"
    elif conn_type == "rest_api":
        if not base_url.strip():
            return "Base URL is required"
    return ""


class ConnectionItem(BaseModel):
    id: int = 0
    name: str = ""
    connection_type: str = ""
    test_status: str = ""  # "" = untested, "ok" = success, "fail" = failure
    #: How many live uploads / pipelines / transformations point at this
    #: connection, and their names, so the delete dialog can say what it is
    #: about to break rather than deleting silently (core#804, core#805).
    dependent_count: int = 0
    dependent_names: str = ""


class ConnectionState(BaseState):
    connections: list[ConnectionItem] = []
    form_name: str = ""
    # "" so the type picker shows its placeholder and forces a deliberate
    # choice, rather than silently defaulting to postgres (core#593).
    form_type: str = ""
    form_config: str = "{}"
    form_use_raw_json: bool = False

    # 0 = creating new, >0 = editing existing connection
    editing_conn_id: int = 0

    # Test connection feedback
    test_message: str = ""
    test_success: bool = False
    #: core#821 — the verdict has three states, not two. `test_success`
    #: alone cannot express *not tested*, and rendering that as failure is
    #: the same lie as rendering it as success: the connection may be fine,
    #: we simply did not check. Both flags false = a plain failure.
    test_untested: bool = False

    # SQL database fields (postgres, mysql, mssql, redshift)
    form_host: str = ""
    # "" until a type is chosen; set_form_type fills the type's default port.
    form_port: str = ""
    form_user: str = ""
    form_password: str = ""
    form_database: str = ""
    form_schema: str = ""

    # SQLite
    form_path: str = ""

    # BigQuery
    form_project: str = ""
    form_dataset: str = ""
    form_keyfile_json: str = ""

    # Snowflake (also uses form_user, form_password, form_database, form_schema)
    form_account: str = ""
    form_warehouse: str = ""
    form_role: str = ""

    # S3 (also used by csv/json/parquet for bucket_url)
    form_bucket_url: str = ""
    form_aws_access_key_id: str = ""
    form_aws_secret_access_key: str = ""
    form_region_name: str = ""
    form_endpoint_url: str = ""

    # REST API
    form_base_url: str = ""
    form_api_key: str = ""
    form_extra_headers: str = ""

    # OpenAPI connector (spec pasted; endpoints auto-discovered on save)
    form_openapi_spec: str = ""

    # File upload (csv/json/parquet)
    form_uploaded_file_id: int = 0
    form_uploaded_file_name: str = ""

    # Google Sheets
    form_spreadsheet_url: str = ""
    form_service_account_json: str = ""

    # ClickHouse options
    form_cluster_replication: bool = False
    form_secure: bool = False

    # Oracle — connect by SID instead of service name (legacy single-instance)
    form_oracle_use_sid: bool = False

    # MongoDB transport options (core#626).
    #
    # ⚠️ `form_mongodb_tls` is deliberately NOT `form_secure`. That one is
    # ClickHouse's "Use HTTPS (TLS)", and HTTPS is wrong for the MongoDB wire
    # protocol anyway. Sharing one var across two connectors because the labels
    # rhyme means a mid-form type switch carries the tick across.
    form_mongodb_tls: bool = False
    form_mongodb_srv: bool = False
    # Where the MongoDB user was created. Blank means `admin`, which is where
    # Atlas and every managed provider put them (core#550).
    form_auth_source: str = ""

    # Databricks
    form_http_path: str = ""
    form_token: str = ""
    form_catalog: str = ""

    # SaaS sources
    form_owner: str = ""
    form_repo: str = ""
    form_instance_url: str = ""  # Salesforce
    form_store: str = ""  # Shopify
    form_domain: str = ""  # Jira, Zendesk
    form_email: str = ""  # Jira, Zendesk
    form_property_id: str = ""  # Google Analytics
    form_customer_id: str = ""  # Google Ads
    form_developer_token: str = ""  # Google Ads
    form_client_id: str = ""  # Google Ads OAuth
    form_client_secret: str = ""  # Google Ads OAuth
    form_refresh_token: str = ""  # Google Ads OAuth
    form_login_customer_id: str = ""  # Google Ads manager (MCC) account
    form_account_id: str = ""  # Facebook Ads
    form_base_id: str = ""  # Airtable
    form_bootstrap_servers: str = ""  # Kafka
    form_topics: str = ""  # Kafka
    form_group_id: str = ""  # Kafka

    # Pipeline template currently driving the form prefill (empty = none).
    # Set when the user clicks a card on /pipelines/templates and arrives
    # at /connections via ?template=<slug>. Used to show a banner explaining
    # the multi-step flow.
    selected_template_slug: str = ""

    def _apply_template_defaults(self, slug: str) -> None:
        """Look up a pipeline template and prefill matching form fields.

        - No-op if ``slug`` is empty or doesn't match a known template.
        - Sets ``form_type`` to the template's source connector.
        - Copies entries from ``source_config_defaults`` into matching
          ``form_*`` attributes via ``_TEMPLATE_FIELD_MAP``.
        - Never touches credential fields (password, api_key, secrets,
          service-account JSON). Templates only carry non-sensitive defaults.
        - Stores the slug in ``selected_template_slug`` so the UI can show
          a "you're following a template" banner.

        Pure helper — testable without Reflex / DB / async context.
        """
        from datanika.data.pipeline_templates import get_template

        tpl = get_template(slug)
        if tpl is None:
            return

        self.form_type = str(tpl.source_type)
        self.selected_template_slug = tpl.slug

        for key, value in tpl.source_config_defaults.items():
            attr = _TEMPLATE_FORM_FIELD_MAP.get(key)
            if attr is None:
                continue
            setattr(self, attr, value)

        # Direct assignment above, so the per-setter invalidation does not fire
        # (core#609). Prefilling a template rewrites both the type and the
        # config, which is exactly when a previous verdict stops being true.
        self._clear_test_verdict()

    async def load_template_from_query(self):
        """Read ``?template=<slug>`` from the page URL and prefill the form.

        Wired into the /connections page ``on_load`` so a user arriving from
        the templates grid lands with the source connector preselected.
        """
        slug = self.router.page.params.get("template", "")
        if slug:
            self._apply_template_defaults(slug)

    # ------------------------------------------------------------------
    # Config-field setters (core#609)
    #
    # Every one of these routes through `_set_config_field`, which clears the
    # test verdict. A "Connected successfully" badge is a statement about a
    # configuration, and it must not outlive the configuration it describes —
    # on prod a green badge survived a type change, a completely different
    # config and a failed test, and it is *durable server-side state*, so it
    # survived a full reload and a re-login too.
    #
    # They are also all written out rather than left to Reflex's auto-setters.
    # An auto-setter assigns the var and nothing else, so there is nowhere to
    # hang the invalidation — and `state_auto_setters` is deprecated and goes
    # away in Reflex 0.9 regardless. `test_connection_verdict_staleness.py`
    # sweeps every `form_*` field on the class, so a field added later without
    # a setter here fails CI rather than silently reopening the hole.
    #
    # `form_name` is deliberately exempt: `_build_config` never reads it, so
    # renaming a connection does not change what was tested. See
    # `NON_CONFIG_FORM_FIELDS`.

    def _clear_test_verdict(self) -> None:
        """Forget any previous Test Connection result."""
        self.test_message = ""
        self.test_success = False
        self.test_untested = False

    def _set_config_field(self, field: str, value) -> None:
        """Assign a config form field and invalidate the stale verdict."""
        setattr(self, field, value)
        self._clear_test_verdict()

    def set_form_name(self, value: str):
        # Exempt from the invalidation above — the name is a label, not config.
        self.form_name = re.sub(r"[^a-zA-Z0-9 ]", "", value)

    def set_form_type(self, value: str):
        self.form_type = value
        self.form_port = _DEFAULT_PORTS.get(value, "")
        # core#626 D5. This reset the port default and the test verdict and
        # **not** the booleans, so `form_secure`, `form_cluster_replication` and
        # `form_oracle_use_sid` all survived a mid-form type switch — a
        # pre-existing bug with no reporter, because the two connectors that
        # could collide were rarely switched between.
        #
        # Adding MongoDB's TLS checkbox is what makes it reachable: tick
        # ClickHouse's "Use HTTPS (TLS)", switch the dropdown to MongoDB, and
        # you would arrive with TLS silently pre-checked. Against a server
        # without TLS that is a connection failure with no visible cause, from
        # a control the user never touched.
        #
        # Reset every boolean rather than the new pair: a per-field list is one
        # more thing to forget the next time somebody adds a checkbox.
        self.form_cluster_replication = False
        self.form_secure = False
        self.form_oracle_use_sid = False
        self.form_mongodb_tls = False
        self.form_mongodb_srv = False
        self._clear_test_verdict()

    def set_form_config(self, value: str):
        self._set_config_field("form_config", value)

    def set_form_use_raw_json(self, value: bool):
        self._set_config_field("form_use_raw_json", value)

    def set_form_host(self, value: str):
        self._set_config_field("form_host", value)

    def set_form_port(self, value: str):
        self._set_config_field("form_port", value)

    def set_form_user(self, value: str):
        self._set_config_field("form_user", value)

    def set_form_password(self, value: str):
        self._set_config_field("form_password", value)

    def set_form_database(self, value: str):
        self._set_config_field("form_database", value)

    def set_form_schema(self, value: str):
        self._set_config_field("form_schema", value)

    def set_form_path(self, value: str):
        self._set_config_field("form_path", value)

    def set_form_project(self, value: str):
        self._set_config_field("form_project", value)

    def set_form_dataset(self, value: str):
        self._set_config_field("form_dataset", value)

    def set_form_keyfile_json(self, value: str):
        self._set_config_field("form_keyfile_json", value)

    def set_form_account(self, value: str):
        self._set_config_field("form_account", value)

    def set_form_warehouse(self, value: str):
        self._set_config_field("form_warehouse", value)

    def set_form_role(self, value: str):
        self._set_config_field("form_role", value)

    def set_form_bucket_url(self, value: str):
        self._set_config_field("form_bucket_url", value)

    def set_form_aws_access_key_id(self, value: str):
        self._set_config_field("form_aws_access_key_id", value)

    def set_form_aws_secret_access_key(self, value: str):
        self._set_config_field("form_aws_secret_access_key", value)

    def set_form_region_name(self, value: str):
        self._set_config_field("form_region_name", value)

    def set_form_endpoint_url(self, value: str):
        self._set_config_field("form_endpoint_url", value)

    def set_form_base_url(self, value: str):
        self._set_config_field("form_base_url", value)

    def set_form_api_key(self, value: str):
        self._set_config_field("form_api_key", value)

    def set_form_extra_headers(self, value: str):
        self._set_config_field("form_extra_headers", value)

    def set_form_openapi_spec(self, value: str):
        self._set_config_field("form_openapi_spec", value)

    def set_form_uploaded_file_id(self, value: int):
        self._set_config_field("form_uploaded_file_id", value)

    def set_form_uploaded_file_name(self, value: str):
        self._set_config_field("form_uploaded_file_name", value)

    def set_form_spreadsheet_url(self, value: str):
        self._set_config_field("form_spreadsheet_url", value)

    def set_form_service_account_json(self, value: str):
        self._set_config_field("form_service_account_json", value)

    def set_form_cluster_replication(self, value: bool):
        self._set_config_field("form_cluster_replication", value)

    def set_form_secure(self, value: bool):
        self._set_config_field("form_secure", value)

    def set_form_oracle_use_sid(self, value: bool):
        self._set_config_field("form_oracle_use_sid", value)

    def set_form_mongodb_tls(self, value: bool):
        self._set_config_field("form_mongodb_tls", value)

    def set_form_mongodb_srv(self, value: bool):
        self._set_config_field("form_mongodb_srv", value)

    def set_form_auth_source(self, value: str):
        self._set_config_field("form_auth_source", value)

    def set_form_http_path(self, value: str):
        self._set_config_field("form_http_path", value)

    def set_form_token(self, value: str):
        self._set_config_field("form_token", value)

    def set_form_catalog(self, value: str):
        self._set_config_field("form_catalog", value)

    def set_form_owner(self, value: str):
        self._set_config_field("form_owner", value)

    def set_form_repo(self, value: str):
        self._set_config_field("form_repo", value)

    def set_form_instance_url(self, value: str):
        self._set_config_field("form_instance_url", value)

    def set_form_store(self, value: str):
        self._set_config_field("form_store", value)

    def set_form_domain(self, value: str):
        self._set_config_field("form_domain", value)

    def set_form_email(self, value: str):
        self._set_config_field("form_email", value)

    def set_form_property_id(self, value: str):
        self._set_config_field("form_property_id", value)

    def set_form_customer_id(self, value: str):
        self._set_config_field("form_customer_id", value)

    def set_form_developer_token(self, value: str):
        self._set_config_field("form_developer_token", value)

    def set_form_client_id(self, value: str):
        self._set_config_field("form_client_id", value)

    def set_form_client_secret(self, value: str):
        self._set_config_field("form_client_secret", value)

    def set_form_refresh_token(self, value: str):
        self._set_config_field("form_refresh_token", value)

    def set_form_login_customer_id(self, value: str):
        self._set_config_field("form_login_customer_id", value)

    def set_form_account_id(self, value: str):
        self._set_config_field("form_account_id", value)

    def set_form_base_id(self, value: str):
        self._set_config_field("form_base_id", value)

    def set_form_bootstrap_servers(self, value: str):
        self._set_config_field("form_bootstrap_servers", value)

    def set_form_topics(self, value: str):
        self._set_config_field("form_topics", value)

    def set_form_group_id(self, value: str):
        self._set_config_field("form_group_id", value)

    def _record_uploaded_file(self, file_id: int, original_name: str) -> None:
        """Store the uploaded file on the form, and invalidate the verdict.

        Split out of ``handle_file_upload`` so it is reachable without an async
        context, a session or a real file — and because for csv/json/parquet the
        uploaded file **is** the configuration, so a green badge earned by a
        previous file must not survive a new one (core#609). This path assigns
        the fields directly, so the per-setter invalidation never reaches it.
        """
        self.form_uploaded_file_id = file_id
        self.form_uploaded_file_name = original_name
        self._clear_test_verdict()

    async def handle_file_upload(self, files: list[rx.UploadFile]):
        """Receive uploaded file, call FileUploadService.save_file, store ID.

        The annotation is load-bearing: ``POST /_upload`` finds this handler by
        scanning type hints for a ``list[rx.UploadFile]``, and 500s before
        calling it if there isn't one (#452).
        """
        from datanika.services.file_upload_service import FileUploadService

        # #673. This one is reached over HTTP rather than through the event
        # pipeline — Reflex routes ``POST /_upload`` here by type hint — so no
        # rendered page gates it and the server-side check is the only check.
        if not await self._require_live_session():
            return

        if not files:
            return
        file = files[0]
        upload_data = await file.read()
        filename = file.filename

        org_id = await self._get_org_id()
        file_svc = FileUploadService(settings.file_uploads_dir)
        try:
            with get_sync_session() as session:
                record = file_svc.save_file(session, org_id, filename, upload_data)
                session.commit()
                self._record_uploaded_file(record.id, record.original_name)
                self.error_message = ""
        except ValueError as e:
            self.error_message = str(e)

    def _validate_form(self) -> str:
        """Return an error message if required fields are missing, or '' if valid."""
        return _validate_connection_form(
            name=self.form_name,
            conn_type=self.form_type,
            use_raw_json=self.form_use_raw_json,
            host=self.form_host,
            port=self.form_port,
            database=self.form_database,
            path=self.form_path,
            project=self.form_project,
            dataset=self.form_dataset,
            account=self.form_account,
            user=self.form_user,
            bucket_url=self.form_bucket_url,
            base_url=self.form_base_url,
            uploaded_file_id=self.form_uploaded_file_id,
            spreadsheet_url=self.form_spreadsheet_url,
            service_account_json=self.form_service_account_json,
        )

    def _build_config(self) -> dict:
        """Build connection config dict from structured form fields."""
        if self.form_use_raw_json:
            return json.loads(self.form_config)

        config: dict = {}
        t = self.form_type

        if t in _DB_TYPES:
            if self.form_host:
                config["host"] = self.form_host
            if self.form_port:
                config["port"] = int(self.form_port)
            if self.form_user:
                config["user"] = self.form_user
            if self.form_password:
                config["password"] = self.form_password
            if self.form_database:
                config["database"] = self.form_database
            if self.form_schema:
                config["schema"] = self.form_schema
            # ClickHouse options
            if t == "clickhouse":
                if self.form_cluster_replication:
                    config["table_engine_type"] = "replicated_merge_tree"
                config["secure"] = self.form_secure
            if t == "oracle" and self.form_oracle_use_sid:
                config["use_sid"] = True

        elif t in ("duckdb", "sqlite"):
            if self.form_path:
                config["path"] = self.form_path

        elif t == "bigquery":
            if self.form_project:
                config["project"] = self.form_project
            if self.form_dataset:
                config["dataset"] = self.form_dataset
            if self.form_keyfile_json:
                config["keyfile_json"] = self.form_keyfile_json

        elif t == "snowflake":
            if self.form_account:
                config["account"] = self.form_account
            if self.form_user:
                config["user"] = self.form_user
            if self.form_password:
                config["password"] = self.form_password
            if self.form_database:
                config["database"] = self.form_database
            if self.form_warehouse:
                config["warehouse"] = self.form_warehouse
            if self.form_role:
                config["role"] = self.form_role
            if self.form_schema:
                config["schema"] = self.form_schema

        elif t == "s3":
            if self.form_bucket_url:
                config["bucket_url"] = self.form_bucket_url
            if self.form_aws_access_key_id:
                config["aws_access_key_id"] = self.form_aws_access_key_id
            if self.form_aws_secret_access_key:
                config["aws_secret_access_key"] = self.form_aws_secret_access_key
            if self.form_region_name:
                config["region_name"] = self.form_region_name
            if self.form_endpoint_url:
                config["endpoint_url"] = self.form_endpoint_url

        elif t in ("csv", "json", "parquet"):
            if self.form_uploaded_file_id:
                config["uploaded_file_id"] = self.form_uploaded_file_id
            if self.form_bucket_url:
                config["bucket_url"] = self.form_bucket_url

        elif t == "google_sheets":
            if self.form_spreadsheet_url:
                config["spreadsheet_url"] = self.form_spreadsheet_url
            if self.form_service_account_json:
                config["service_account_json"] = self.form_service_account_json

        elif t == "rest_api":
            if self.form_base_url:
                config["base_url"] = self.form_base_url
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_extra_headers:
                config["extra_headers"] = self.form_extra_headers

        elif t == "mongodb":
            if self.form_host:
                config["host"] = self.form_host
            # ⚠️ The port IS still stored under SRV, and the spec says not to.
            # Deviation recorded on core#626. `mongodb+srv://h:27017/` is invalid
            # per the URI spec, but that is enforced where it belongs —
            # `build_connection_uri` composes the authority as `host` alone when
            # `srv` is set, so a stored port structurally cannot reach the URI.
            #
            # Dropping it here instead would do two bad things. It breaks
            # `test_connection_config_roundtrip.py`'s invariant that every
            # declared schema key survives a save — the guard core#638 exists to
            # hold — and it silently destroys a port the user typed, which is
            # the same shape of loss that guard was written about. Keeping it
            # means unticking SRV restores the port the user had.
            if self.form_port:
                config["port"] = int(self.form_port)
            if self.form_user:
                config["user"] = self.form_user
            if self.form_password:
                config["password"] = self.form_password
            if self.form_database:
                config["database"] = self.form_database
            # core#638 — every new key needs a line in **both** serialisers.
            # A key present in one and absent from the other is silently dropped
            # on the next structured-form save, which is exactly what happened
            # to `auth_source`: set it via raw JSON, reopen the connection in the
            # form, click Save, and it reverts to `admin` on the next run.
            if self.form_auth_source:
                config["auth_source"] = self.form_auth_source
            # Written unconditionally rather than only when true, so unticking
            # a box persists as False instead of dropping the key and falling
            # back to the default. `build_connection_uri` reads both with
            # `.get(..., False)`, so an older config without them is unaffected.
            config["tls"] = self.form_mongodb_tls or self.form_mongodb_srv
            config["srv"] = self.form_mongodb_srv

        elif t == "databricks":
            if self.form_host:
                config["host"] = self.form_host
            if self.form_http_path:
                config["http_path"] = self.form_http_path
            if self.form_token:
                config["token"] = self.form_token
            if self.form_catalog:
                config["catalog"] = self.form_catalog
            if self.form_schema:
                config["schema"] = self.form_schema

        elif t == "stripe":
            if self.form_api_key:
                config["api_key"] = self.form_api_key

        elif t == "github":
            if self.form_api_key:
                config["access_token"] = self.form_api_key
            if self.form_owner:
                config["owner"] = self.form_owner
            if self.form_repo:
                config["repo"] = self.form_repo

        elif t == "hubspot":
            if self.form_api_key:
                config["api_key"] = self.form_api_key

        elif t == "salesforce":
            if self.form_api_key:
                config["access_token"] = self.form_api_key
            if self.form_instance_url:
                config["instance_url"] = self.form_instance_url

        elif t == "shopify":
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_store:
                config["store"] = self.form_store

        elif t == "jira":
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_email:
                config["email"] = self.form_email
            if self.form_domain:
                config["domain"] = self.form_domain

        elif t == "slack":
            if self.form_api_key:
                config["api_key"] = self.form_api_key

        elif t == "google_analytics":
            if self.form_api_key:
                config["service_account_json"] = self.form_api_key
            if self.form_property_id:
                config["property_id"] = self.form_property_id

        elif t == "google_ads":
            if self.form_customer_id:
                config["customer_id"] = self.form_customer_id
            if self.form_developer_token:
                config["developer_token"] = self.form_developer_token
            if self.form_client_id:
                config["client_id"] = self.form_client_id
            if self.form_client_secret:
                config["client_secret"] = self.form_client_secret
            if self.form_refresh_token:
                config["refresh_token"] = self.form_refresh_token
            if self.form_login_customer_id:
                config["login_customer_id"] = self.form_login_customer_id

        elif t == "facebook_ads":
            if self.form_api_key:
                config["access_token"] = self.form_api_key
            if self.form_account_id:
                config["account_id"] = self.form_account_id

        elif t == "zendesk":
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_domain:
                config["subdomain"] = self.form_domain
            if self.form_email:
                config["email"] = self.form_email

        elif t == "airtable":
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_base_id:
                config["base_id"] = self.form_base_id

        elif t == "notion":
            if self.form_api_key:
                config["api_key"] = self.form_api_key

        elif t == "kafka":
            if self.form_bootstrap_servers:
                config["bootstrap_servers"] = self.form_bootstrap_servers
            if self.form_topics:
                config["topics"] = [t.strip() for t in self.form_topics.split(",") if t.strip()]
            if self.form_group_id:
                config["group_id"] = self.form_group_id

        elif t in ("pipedrive", "asana"):
            if self.form_api_key:
                config["api_key"] = self.form_api_key

        elif t == "freshdesk":
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_domain:
                config["domain"] = self.form_domain

        elif t == "openapi":
            from datanika.services.openapi_import import (
                OpenApiImportError,
                parse_openapi_spec,
            )

            if not self.form_openapi_spec.strip():
                raise ValueError("Paste an OpenAPI spec")
            try:
                parsed = parse_openapi_spec(
                    self.form_openapi_spec, base_url_override=self.form_base_url or None
                )
            except OpenApiImportError as exc:
                raise ValueError(str(exc)) from exc
            if not parsed.base_url:
                raise ValueError("No base URL found in the spec — set the Base URL field")
            config["spec_inline"] = self.form_openapi_spec
            config["base_url"] = parsed.base_url
            config["resources"] = parsed.resources
            if self.form_api_key and parsed.auth_schemes:
                config["auth"] = _fill_openapi_auth(parsed.auth_schemes[0], self.form_api_key)

        return config

    def _reset_form_fields(self):
        """Clear all typed form fields and exit edit mode."""
        self.editing_conn_id = 0
        self.form_name = ""
        # Back to the placeholder, not postgres — the next connection starts
        # blank and forces a deliberate type choice (core#593).
        self.form_type = ""
        self.form_config = "{}"
        self.form_use_raw_json = False
        self.form_host = ""
        self.form_port = ""
        self.form_user = ""
        self.form_password = ""
        self.form_database = ""
        self.form_schema = ""
        self.form_path = ""
        self.form_project = ""
        self.form_dataset = ""
        self.form_keyfile_json = ""
        self.form_account = ""
        self.form_warehouse = ""
        self.form_role = ""
        self.form_bucket_url = ""
        self.form_aws_access_key_id = ""
        self.form_aws_secret_access_key = ""
        self.form_region_name = ""
        self.form_endpoint_url = ""
        self.form_base_url = ""
        self.form_api_key = ""
        self.form_extra_headers = ""
        self.form_openapi_spec = ""
        self.form_uploaded_file_id = 0
        self.form_uploaded_file_name = ""
        self.form_spreadsheet_url = ""
        self.form_service_account_json = ""
        self.form_cluster_replication = False
        self.form_secure = False
        self.form_oracle_use_sid = False
        self.form_mongodb_tls = False
        self.form_mongodb_srv = False
        self.form_auth_source = ""
        self.form_http_path = ""
        self.form_token = ""
        self.form_catalog = ""
        self.form_owner = ""
        self.form_repo = ""
        self.form_instance_url = ""
        self.form_store = ""
        self.form_domain = ""
        self.form_email = ""
        self.form_property_id = ""
        self.form_customer_id = ""
        self.form_account_id = ""
        self.form_base_id = ""
        self.form_bootstrap_servers = ""
        self.form_topics = ""
        self.form_group_id = ""
        self.error_message = ""
        # Assigned directly rather than via `_clear_test_verdict`: this function
        # already cleared both halves correctly, and it is borrowed by a
        # standalone FakeState in tests/test_ui/test_state_setters.py that has
        # no methods bound beyond this one. Routing it through the helper would
        # buy consistency and cost a working test its independence.
        self.test_message = ""
        self.test_success = False
        self.test_untested = False

    def _populate_form_from_config(self, name: str, conn_type: str, config: dict):
        """Fill form fields from a decrypted config dict."""
        self.form_name = name
        self.form_type = conn_type
        self.form_use_raw_json = False
        self.error_message = ""
        # Both halves, not just the message. Leaving `test_success` True kept a
        # green verdict one keystroke away from reappearing (core#609).
        self._clear_test_verdict()

        # Reset all type-specific fields first
        self.form_host = ""
        self.form_port = _DEFAULT_PORTS.get(conn_type, "")
        self.form_user = ""
        self.form_password = ""
        self.form_database = ""
        self.form_schema = ""
        self.form_path = ""
        self.form_project = ""
        self.form_dataset = ""
        self.form_keyfile_json = ""
        self.form_account = ""
        self.form_warehouse = ""
        self.form_role = ""
        self.form_bucket_url = ""
        self.form_aws_access_key_id = ""
        self.form_aws_secret_access_key = ""
        self.form_region_name = ""
        self.form_endpoint_url = ""
        self.form_base_url = ""
        self.form_api_key = ""
        self.form_extra_headers = ""
        self.form_openapi_spec = ""
        self.form_uploaded_file_id = 0
        self.form_uploaded_file_name = ""
        self.form_spreadsheet_url = ""
        self.form_service_account_json = ""
        self.form_cluster_replication = False
        self.form_secure = False
        self.form_oracle_use_sid = False
        self.form_mongodb_tls = False
        self.form_mongodb_srv = False
        self.form_auth_source = ""
        self.form_http_path = ""
        self.form_token = ""
        self.form_catalog = ""
        self.form_owner = ""
        self.form_repo = ""
        self.form_instance_url = ""
        self.form_store = ""
        self.form_domain = ""
        self.form_email = ""
        self.form_property_id = ""
        self.form_customer_id = ""
        self.form_account_id = ""
        self.form_base_id = ""
        self.form_bootstrap_servers = ""
        self.form_topics = ""
        self.form_group_id = ""

        if conn_type in _DB_TYPES:
            self.form_host = config.get("host", "")
            self.form_port = str(config.get("port", _DEFAULT_PORTS.get(conn_type, "")))
            self.form_user = config.get("user", "")
            self.form_password = config.get("password", "")
            self.form_database = config.get("database", "")
            self.form_schema = config.get("schema", "")
            if conn_type == "clickhouse":
                self.form_cluster_replication = (
                    config.get("table_engine_type") == "replicated_merge_tree"
                )
                self.form_secure = config.get("secure", False)
            if conn_type == "oracle":
                self.form_oracle_use_sid = config.get("use_sid", False)
        elif conn_type in ("duckdb", "sqlite"):
            self.form_path = config.get("path", "")
        elif conn_type == "bigquery":
            self.form_project = config.get("project", "")
            self.form_dataset = config.get("dataset", "")
            self.form_keyfile_json = config.get("keyfile_json", "")
        elif conn_type == "snowflake":
            self.form_account = config.get("account", "")
            self.form_user = config.get("user", "")
            self.form_password = config.get("password", "")
            self.form_database = config.get("database", "")
            self.form_warehouse = config.get("warehouse", "")
            self.form_role = config.get("role", "")
            self.form_schema = config.get("schema", "")
        elif conn_type == "s3":
            self.form_bucket_url = config.get("bucket_url", "")
            self.form_aws_access_key_id = config.get("aws_access_key_id", "")
            self.form_aws_secret_access_key = config.get("aws_secret_access_key", "")
            self.form_region_name = config.get("region_name", "")
            self.form_endpoint_url = config.get("endpoint_url", "")
        elif conn_type in ("csv", "json", "parquet"):
            self.form_bucket_url = config.get("bucket_url", "")
            self.form_uploaded_file_id = config.get("uploaded_file_id", 0)
            if self.form_uploaded_file_id:
                self.form_uploaded_file_name = config.get("uploaded_file_name", "uploaded file")
        elif conn_type == "google_sheets":
            self.form_spreadsheet_url = config.get("spreadsheet_url", "")
            self.form_service_account_json = config.get("service_account_json", "")
        elif conn_type == "rest_api":
            self.form_base_url = config.get("base_url", "")
            self.form_api_key = config.get("api_key", "")
            self.form_extra_headers = config.get("extra_headers", "")
        elif conn_type == "mongodb":
            self.form_host = config.get("host", "")
            self.form_port = str(config.get("port", _DEFAULT_PORTS.get("mongodb", "")))
            self.form_user = config.get("user", "")
            self.form_password = config.get("password", "")
            self.form_database = config.get("database", "")
            self.form_auth_source = config.get("auth_source", "")
            self.form_mongodb_srv = config.get("srv", False)
            self.form_mongodb_tls = config.get("tls", False)
        elif conn_type == "databricks":
            self.form_host = config.get("host", "")
            self.form_http_path = config.get("http_path", "")
            self.form_token = config.get("token", "")
            self.form_catalog = config.get("catalog", "")
            self.form_schema = config.get("schema", "")
        elif conn_type == "stripe":
            self.form_api_key = config.get("api_key", "")
        elif conn_type == "github":
            self.form_api_key = config.get("access_token", "")
            self.form_owner = config.get("owner", "")
            self.form_repo = config.get("repo", "")
        elif conn_type == "hubspot":
            self.form_api_key = config.get("api_key", "")
        elif conn_type == "salesforce":
            self.form_api_key = config.get("access_token", "")
            self.form_instance_url = config.get("instance_url", "")
        elif conn_type == "shopify":
            self.form_api_key = config.get("api_key", "")
            self.form_store = config.get("store", "")
        elif conn_type == "jira":
            self.form_api_key = config.get("api_key", "")
            self.form_email = config.get("email", "")
            self.form_domain = config.get("domain", "")
        elif conn_type == "slack":
            self.form_api_key = config.get("api_key", "")
        elif conn_type == "google_analytics":
            self.form_api_key = config.get("service_account_json", "")
            self.form_property_id = config.get("property_id", "")
        elif conn_type == "google_ads":
            self.form_customer_id = config.get("customer_id", "")
            self.form_developer_token = config.get("developer_token", "")
            self.form_client_id = config.get("client_id", "")
            self.form_client_secret = config.get("client_secret", "")
            self.form_refresh_token = config.get("refresh_token", "")
            self.form_login_customer_id = config.get("login_customer_id", "")
        elif conn_type == "facebook_ads":
            self.form_api_key = config.get("access_token", "")
            self.form_account_id = config.get("account_id", "")
        elif conn_type == "zendesk":
            self.form_api_key = config.get("api_key", "")
            self.form_domain = config.get("subdomain", "")
            self.form_email = config.get("email", "")
        elif conn_type == "airtable":
            self.form_api_key = config.get("api_key", "")
            self.form_base_id = config.get("base_id", "")
        elif conn_type == "notion":
            self.form_api_key = config.get("api_key", "")
        elif conn_type == "kafka":
            self.form_bootstrap_servers = config.get("bootstrap_servers", "")
            topics = config.get("topics", [])
            self.form_topics = ", ".join(topics) if isinstance(topics, list) else topics
            self.form_group_id = config.get("group_id", "")
        elif conn_type in ("pipedrive", "asana"):
            self.form_api_key = config.get("api_key", "")
        elif conn_type == "freshdesk":
            self.form_api_key = config.get("api_key", "")
            self.form_domain = config.get("domain", "")
        elif conn_type == "openapi":
            self.form_openapi_spec = config.get("spec_inline", "")
            self.form_base_url = config.get("base_url", "")
            _auth = config.get("auth") or {}
            self.form_api_key = (
                _auth.get("token") or _auth.get("api_key") or _auth.get("username") or ""
            )

    async def load_connections(self):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            rows = svc.list_connections(session, org_id)
            items = []
            for c in rows:
                dependents = svc.list_dependents(session, org_id, c.id)
                items.append(
                    ConnectionItem(
                        id=c.id,
                        name=c.name,
                        connection_type=c.connection_type.value,
                        dependent_count=len(dependents),
                        dependent_names=", ".join(dependents),
                    )
                )
            self.connections = items
        self.error_message = ""

    async def save_connection(self):
        """Create a new connection or update an existing one."""
        if not await self._check_role("editor"):
            return
        validation_error = self._validate_form()
        if validation_error:
            self.error_message = validation_error
            return
        from datanika.ui.state.auth_state import AuthState

        auth_state = await self.get_state(AuthState)
        org_id = auth_state.current_org.id
        user_id = auth_state.current_user.id
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        try:
            config = self._build_config()
        except (json.JSONDecodeError, ValueError) as e:
            self.error_message = f"Invalid config: {e}"
            return
        # Captured before `_reset_form_fields()` clears it. core#872: a create
        # that says "saved" is acceptable; an update that says "created" is not,
        # because it tells the user a new row exists.
        was_edit = bool(self.editing_conn_id)
        try:
            with get_sync_session() as session:
                if self.editing_conn_id:
                    svc.update_connection(
                        session,
                        org_id,
                        self.editing_conn_id,
                        name=self.form_name,
                        connection_type=ConnectionType(self.form_type),
                        config=config,
                    )
                    self._audit(
                        session,
                        org_id,
                        user_id,
                        "update",
                        "connection",
                        resource_id=self.editing_conn_id,
                        new_values={"name": self.form_name, "connection_type": self.form_type},
                    )
                else:
                    # Pass the template slug through so the Plausible
                    # funnel step 3 (``template_first_run_triggered``)
                    # can attribute this connection's first run back to
                    # the template the user started from. #93.
                    conn = svc.create_connection(
                        session,
                        org_id,
                        self.form_name,
                        ConnectionType(self.form_type),
                        config,
                        source_template_slug=self.selected_template_slug or None,
                    )
                    self._audit(
                        session,
                        org_id,
                        user_id,
                        "create",
                        "connection",
                        resource_id=conn.id,
                        new_values={"name": self.form_name, "connection_type": self.form_type},
                    )
                session.commit()
        except Exception as e:
            self._set_error(e, "Failed to save connection")
            return
        # Slug was persisted on the Connection row; detach from UI state so
        # a subsequent non-template create doesn't inherit it. #93.
        self.selected_template_slug = ""
        self._reset_form_fields()
        # Yielded before the refetch, deliberately. The table repopulating is
        # asynchronous and was measured lagging 5-17 seconds; the toast is the
        # acknowledgement, the table is not (core#872 D5).
        if was_edit:
            yield await self._saved_toast("connections.saved_toast", "Connection saved")
        else:
            yield await self._saved_toast("connections.created_toast", "Connection created")
        await self.load_connections()

    async def edit_connection(self, conn_id: int):
        """Load a saved connection into the form for editing."""
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            conn = svc.get_connection(session, org_id, conn_id)
            config = svc.get_connection_config(session, org_id, conn_id)
        if conn is None or config is None:
            self.error_message = "Connection not found"
            return
        self._populate_form_from_config(conn.name, conn.connection_type.value, config)
        self.editing_conn_id = conn_id

    async def copy_connection(self, conn_id: int):
        """Load a saved connection into the form as a new copy."""
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            conn = svc.get_connection(session, org_id, conn_id)
            config = svc.get_connection_config(session, org_id, conn_id)
        if conn is None or config is None:
            self.error_message = "Connection not found"
            return
        self._populate_form_from_config(f"{conn.name} copy", conn.connection_type.value, config)
        self.editing_conn_id = 0

    def cancel_edit(self):
        """Cancel editing and reset the form."""
        self._reset_form_fields()

    async def delete_connection(self, conn_id: int):
        if not await self._check_role("admin"):
            return
        from datanika.ui.state.auth_state import AuthState

        auth_state = await self.get_state(AuthState)
        org_id = auth_state.current_org.id
        user_id = auth_state.current_user.id
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            conn = svc.get_connection(session, org_id, conn_id)
            old_values = (
                {"name": conn.name, "connection_type": conn.connection_type.value} if conn else {}
            )
            svc.delete_connection(session, org_id, conn_id)
            self._audit(
                session,
                org_id,
                user_id,
                "delete",
                "connection",
                resource_id=conn_id,
                old_values=old_values,
            )
            session.commit()
        await self.load_connections()
        # Nothing told the user the delete happened beyond a row vanishing, and
        # for a *reversible* soft delete that reads far scarier than it is
        # (core#804). core#851 moved the body to `BaseState._deleted_toast` when
        # six more handlers needed the same three lines.
        yield await self._deleted_toast("connections.deleted_toast", "Connection retired")

    async def test_connection_from_form(self):
        """Test connectivity using the current form fields (before saving)."""
        validation_error = self._validate_form()
        if validation_error:
            self.test_success = False
            self.test_untested = False
            self.test_message = validation_error
            return
        try:
            config = self._build_config()
        except (json.JSONDecodeError, ValueError) as e:
            self.test_success = False
            self.test_untested = False
            self.test_message = f"Invalid config: {e}"
            return
        # Backstop. `test_connection` is now contracted to return a verdict for
        # every type (core#608, `tests/test_services/test_connection_test_contract.py`),
        # so this should never fire — but an exception escaping here is what
        # produced BOTH reported symptoms at once: Reflex's generic "Contact the
        # website administrator" toast, *and* a previous run's green badge left
        # untouched on screen because these two lines never ran (core#609).
        # Converting an unknown failure into a red verdict is strictly safer
        # than letting it through.
        try:
            ok, msg = ConnectionService.test_connection(config, ConnectionType(self.form_type))
        except Exception:
            logger.exception("Connection test crashed for type %s", self.form_type)
            ok, msg = False, "The connection test failed unexpectedly — please report this"
        # core#821: `ok` is now True / False / None. A crash above yields
        # False, so an unknown failure still reads as a failure and never as
        # the neutral verdict.
        self.test_success = ok is True
        self.test_untested = ok is None
        self.test_message = msg

    async def test_saved_connection(self, conn_id: int):
        """Test connectivity for an already-saved connection."""
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            config = svc.get_connection_config(session, org_id, conn_id)
            conn = svc.get_connection(session, org_id, conn_id)
        if config is None or conn is None:
            self._set_row_test_status(conn_id, "fail")
            return
        # Same backstop as `test_connection_from_form`. A crash here left the row
        # icon showing whatever it showed before — including a previous green
        # tick — rather than reporting the failure (core#608 / core#609).
        try:
            ok, _msg = ConnectionService.test_connection(config, conn.connection_type)
        except Exception:
            logger.exception(
                "Connection test crashed for saved connection %s (%s)",
                conn_id,
                conn.connection_type.value,
            )
            ok = False
        # core#821: an untested type gets no row icon at all — `""` is the
        # column's existing 'no verdict' state. Showing a red cross for a
        # connection nobody checked would be a fresh false claim.
        if ok is None:
            self._set_row_test_status(conn_id, "")
        else:
            self._set_row_test_status(conn_id, "ok" if ok else "fail")

    def _set_row_test_status(self, conn_id: int, status: str):
        """Update test_status for a specific connection row."""
        self.connections = [
            item.model_copy(update={"test_status": status}) if item.id == conn_id else item
            for item in self.connections
        ]
