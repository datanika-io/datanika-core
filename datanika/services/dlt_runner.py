"""DltRunnerService — builds dlt pipeline/source/destination and executes pipelines."""

import logging
import os
import re
import shutil
from pathlib import PurePosixPath

import dlt
from dlt.common import json as dlt_json
from dlt.sources.filesystem import filesystem, read_csv, read_parquet
from dlt.sources.rest_api import rest_api_source
from dlt.sources.sql_database import sql_database, sql_table

from datanika.services.egress_guard import build_guarded_session, validate_egress_host
from datanika.services.mongodb_source import DEFAULT_AUTH_SOURCE as _MONGO_DEFAULT_AUTH_SOURCE

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10_000

SUPPORTED_FILE_TYPES = {"s3", "csv", "json", "parquet"}

DEFAULT_FILE_GLOBS = {"csv": "*.csv", "json": "*.json", "parquet": "*.parquet", "s3": "*"}

# dlt's `filesystem()` is a *lister*: it yields one FileItem per matched file
# (name, size, mtime, mime type). Reading contents requires piping it through a
# transformer. Without one the destination gets a table of filenames and the run
# reports success — core#492, found on prod on the advertised onboarding path.
FILE_FORMAT_BY_TYPE = {"csv": "csv", "json": "json", "parquet": "parquet"}

# `s3` (and any glob-driven source) carries no format in its type, so it is
# inferred from the pattern's extension. Deliberately not defaulted: guessing
# wrong here reproduces exactly the failure this fixes, just one layer along.
FILE_FORMAT_BY_EXTENSION = {
    ".csv": "csv",
    ".tsv": "csv",
    ".txt": "csv",
    ".json": "json",
    ".jsonl": "json",
    ".ndjson": "json",
    ".parquet": "parquet",
    ".pq": "parquet",
}

_GLOB_WILDCARDS = "*?["

AWS_CREDENTIAL_KEYS = {"aws_access_key_id", "aws_secret_access_key", "region_name", "endpoint_url"}

SUPPORTED_REST_TYPES = {"rest_api"}

SUPPORTED_OPENAPI_TYPES = {"openapi"}

SUPPORTED_SHEETS_TYPES = {"google_sheets"}

SUPPORTED_MONGODB_TYPES = {"mongodb"}

SUPPORTED_SAAS_TYPES = {
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

# Kafka is a streaming source with its own builder
SUPPORTED_KAFKA_TYPES = {"kafka"}

#: Graph API version for the Facebook Ads fallback. Facebook retires versions on
#: a rolling ~2-year schedule, so this is a config-overridable default
#: (`dlt_config["api_version"]`) rather than a constant baked into the path — a
#: retired version should be a setting to change, not a release to cut.
DEFAULT_FACEBOOK_API_VERSION = "v21.0"

#: How long a Kafka consumer waits for a message before deciding the topic is
#: drained and the run is done. A batch pipeline needs a stop condition that an
#: infinite consumer does not supply; overridable per upload via
#: ``dlt_config["idle_timeout_ms"]``, since "quiet" means something different on
#: a busy topic than on an hourly one.
DEFAULT_KAFKA_IDLE_TIMEOUT_MS = 10_000

#: The report a Google Analytics connection loads when nobody says otherwise.
#:
#: GA4's Data API has no "just give me the table" endpoint — `runReport` takes a
#: POST body naming dimensions and metrics, and a different body is a different
#: table. That is why this connector shipped unusable (core#543): there was no
#: obvious default, so nothing was chosen and it raised instead.
#:
#: Daily traffic is the report almost everyone starts from, and it is the one a
#: dashboard can be built on without knowing anything about the property.
#: Overridable per upload via `dlt_config["dimensions"]` / `["metrics"]`.
DEFAULT_GA4_DIMENSIONS = ["date"]
DEFAULT_GA4_METRICS = ["sessions", "totalUsers", "screenPageViews"]
DEFAULT_GA4_START_DATE = "28daysAgo"
DEFAULT_GA4_END_DATE = "today"

#: GA4 caps `runReport` at 100k rows per request; page below that so a wide
#: date range still completes rather than silently truncating.
GA4_PAGE_SIZE = 10_000

GA4_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"

#: ⚠️ Google Ads pins the major version **in the URL path**, ships four majors a
#: year, and sunsets each one roughly twelve months after release. `v25` is
#: current as of 2026-07. So this is a **dated** value, not a constant: left
#: alone it stops working on a schedule, and the failure arrives as a 404 from
#: Google rather than as anything our tests can see. It is overridable per
#: upload via `dlt_config["api_version"]` so a sunset is a config change on a
#: live connection rather than a release — and `_google_ads_report_error`
#: names the version when Google refuses, so the cause is legible.
GOOGLE_ADS_API_VERSION = "v25"

#: Endpoints as module constants so a test can point them at a local server.
#: Nothing in production ever changes them; the alternative is a real-socket
#: test that cannot exist, and #545's lesson is that a connector with no
#: row-level probe is a connector nobody has shown works.
GOOGLE_ADS_API_HOST = "https://googleads.googleapis.com"
GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"  # noqa: S105 - a URL

#: Campaign performance by day — the Ads equivalent of GA4's daily-traffic
#: default: the report almost every account starts from, and one that can be
#: built without knowing anything about how the account is structured.
#: Overridable per upload via `dlt_config["query"]` (raw GAQL).
DEFAULT_GOOGLE_ADS_QUERY = (
    "SELECT campaign.id, campaign.name, campaign.status, segments.date, "
    "metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions "
    "FROM campaign "
    "WHERE segments.date DURING LAST_30_DAYS"
)

#: Where MongoDB looks for the user when none is specified.
#:
#: The database in a Mongo URI path doubles as the auth database, so omitting
#: `authSource` means "the user lives inside the database you are reading" —
#: which is not where anyone puts it. `MONGO_INITDB_ROOT_USERNAME`, Atlas and
#: every managed provider create users in `admin` (core#550).
#: Re-exported so existing importers keep working; the value and the URI
#: assembly that reads it now live together in `mongodb_source` (core#625).
DEFAULT_MONGO_AUTH_SOURCE = _MONGO_DEFAULT_AUTH_SOURCE

#: Cloud-warehouse fields whose stored name is not the name dlt reads (core#565).
#:
#: `_to_dlt_credentials` translated only for SQL types, so these passed through
#: untouched and dlt saw no credential it recognised — BigQuery and Databricks
#: could not complete a run, and Snowflake was suspected and then confirmed.
#: Names taken from dlt's own credential classes in the installed version, not
#: from documentation.
_WAREHOUSE_CREDENTIAL_RENAMES = {
    "databricks": {"host": "server_hostname", "token": "access_token"},
    # `SnowflakeCredentials` has no `account`; the account identifier is its host.
    "snowflake": {"account": "host"},
}

#: The config key on a warehouse connection that names **where to write** — the
#: BigQuery dataset, the Databricks / Snowflake schema.
#:
#: One table, two uses, and that is the point (core#610). It used to exist only
#: as `_NON_CREDENTIAL_WAREHOUSE_KEYS` below — a set of keys to *strip* from the
#: credentials — and the stripped value was then dropped on the floor:
#: `creds.pop(key, None)` with the return value unbound. So `dataset` was
#: correctly recognised as "not a credential" and never routed anywhere else. A
#: required, user-filled BigQuery **Dataset** field changed nothing, and rows
#: landed in a dataset named after the *upload* instead. Proven on prod: the user
#: set `docs_bigquery`, BigQuery reported `bigqueryfirstrun`.
#:
#: The comment on the old set described the right intent ("`schema`/`dataset`
#: describe where to write rather than how to authenticate") — only half of it
#: was implemented. Deriving the strip set from this mapping means a key can no
#: longer be removed from the credentials without something knowing where it
#: belongs.
_DESTINATION_DATASET_KEYS = {
    "bigquery": "dataset",
    "databricks": "schema",
    "snowflake": "schema",
}

#: Keys stored on a warehouse connection that are *not* credentials. dlt rejects
#: unknown fields. Derived, never written out — see above.
_NON_CREDENTIAL_WAREHOUSE_KEYS = {
    destination_type: {key} for destination_type, key in _DESTINATION_DATASET_KEYS.items()
}


def destination_dataset_name(destination_type: str, destination_config: dict) -> str | None:
    """The dataset / schema the user named on the destination connection.

    ``None`` when the destination has no such concept (postgres, mysql, duckdb,
    ...), which is the signal for the caller to fall back to its own default.
    Returning ``None`` rather than ``""`` matters: an empty string is a valid
    argument to ``dlt.pipeline(dataset_name=...)`` and would silently create a
    nameless dataset.
    """
    key = _DESTINATION_DATASET_KEYS.get(destination_type)
    if key is None:
        return None
    value = destination_config.get(key)
    if not isinstance(value, str):
        return None
    return value.strip() or None


INTERNAL_CONFIG_KEYS = {
    "mode",
    "table",
    "source_schema",
    "table_names",
    "incremental",
    "batch_size",
    "backend",
    "filters",
    "bucket_url",
    "file_glob",
    "file_format",
    "table_name",
    "delimiter",
    "encoding",
    "resources",
    "resource_names",
    "paginator",
    "client",
    "resource_defaults",
    "base_url",
    "headers",
    "uploaded_file_id",
    "spreadsheet_url",
    "service_account_json",
    "sheet_names",
    "collection_names",
    "merge_config",
    "query",
    # --- builder-specific keys -------------------------------------------
    # Anything a builder reads out of `dlt_config` MUST be listed here.
    # `execute()` forwards every *unlisted* key to `pipeline.run()`, which
    # raises `TypeError: Pipeline.run() got an unexpected keyword argument`.
    # So a key that a builder consumes but this set omits fails the run —
    # after the source is built, so every `build_source` test still passes.
    #
    # Sixteen were missing (core#551). `endpoints` meant the endpoint picker
    # (core#532) would have raised on any upload that used it; `api_version`
    # meant the Facebook version override (core#543) could not be set;
    # `owner`/`repo` meant GitHub could not be configured at all.
    # `tests/test_services/test_dlt_config_keys.py` now derives this from the
    # source so the two cannot drift again.
    "endpoints",
    "api_version",
    "topics",
    "idle_timeout_ms",
    "start_from",
    "enable_auto_commit",
    # The four Kafka security keys (core#1054). They are listed here **not**
    # because a builder reads them as configuration from `dlt_config` — they are
    # connection-only, see `_kafka_security_kwargs` — but because the builder
    # reads them to REFUSE them. Without that read they fall through to
    # `pipeline.run()` and dlt raises `TypeError: Pipeline.run() got an
    # unexpected keyword argument 'security_protocol'`, which names dlt and
    # sends the user nowhere near the connection form.
    "security_protocol",
    "sasl_mechanism",
    "sasl_plain_username",
    "sasl_plain_password",
    "owner",
    "repo",
    "github_source_type",
    "start_date",
    "property_id",
    "customer_id",
    "account_id",
    "base_id",
    "database",
    "auth",
    # Google Analytics report shape (core#543).
    "dimensions",
    "metrics",
    "end_date",
}

# Client-side row-filter lambdas — applied AFTER source yield.
#
# Used only for non-SQL sources (REST, Mongo, SaaS, Kafka) where server-side
# filtering isn't uniformly available. For SQL sources in single_table mode,
# the same (op, column, value) specs are translated to a SQLAlchemy WHERE via
# ``_sql_filter_pushdown.make_query_adapter`` — no in-memory filtering.
#
# If you add a new op here, add the equivalent SQL translation in
# ``_sql_filter_pushdown._filter_to_where`` too so SQL users don't lose the
# pushdown.
FILTER_OPS = {
    "eq": lambda col, val: lambda row: row.get(col) == val,
    "ne": lambda col, val: lambda row: row.get(col) != val,
    "gt": lambda col, val: lambda row: row.get(col) is not None and row.get(col) > val,
    "gte": lambda col, val: lambda row: row.get(col) is not None and row.get(col) >= val,
    "lt": lambda col, val: lambda row: row.get(col) is not None and row.get(col) < val,
    "lte": lambda col, val: lambda row: row.get(col) is not None and row.get(col) <= val,
    "in": lambda col, val: lambda row: row.get(col) in val,
    "not_in": lambda col, val: lambda row: row.get(col) not in val,
}


def _extract_rows_loaded(pipeline) -> int:
    """Extract total rows loaded from dlt pipeline's normalize step.

    dlt 1.21+ stores items_count in NormalizeInfo.row_counts (from the
    normalize step), not in LoadJobMetrics. We read it from the pipeline's
    last_trace after run() completes.
    """
    try:
        trace = pipeline.last_trace
        if trace is None:
            return 0
        normalize_info = trace.last_normalize_info
        if normalize_info is None:
            return 0
        row_counts = normalize_info.row_counts
        # row_counts is {table_name: count} — exclude dlt internal tables
        return sum(v for k, v in row_counts.items() if not k.startswith("_dlt_"))
    except Exception:
        return 0


class DltRunnerError(ValueError):
    """Raised when dlt runner encounters an unsupported configuration."""


# ---------------------------------------------------------------------------
# SaaS pagination (core#823)
# ---------------------------------------------------------------------------
#
# Every SaaS connector reaches ``_rest_api_fallback``, which used to pass **no
# paginator at all** — so all of them depended on dlt's *runtime*
# auto-detection. Measured on production: a Stripe account holding 15 customers
# landed 10, and the run reported `success` with a plausible row count.
#
# Auto-detection is not a safety net here; it is the bug's hiding place. It
# scores a paginator per response, and against the real vendor response shapes
# it produces three different wrong answers (all measured against
# ``dlt.sources.helpers.rest_client.detector.PaginatorFactory``):
#
#   * ``SinglePagePaginator`` for Stripe, HubSpot, Jira, Airtable and Asana —
#     one page, no error, run green. This is the reported defect.
#   * a *correct* paginator for Link-header APIs and for Zendesk/Facebook,
#     which is why those connectors happened to work.
#   * a cursor paginator with the **wrong query parameter** for Notion and
#     Pipedrive (it guesses ``cursor``; the vendors use ``start_cursor`` and
#     ``start``), which re-requests page one rather than truncating.
#
# So silence is not neutral, and the fix is to state the contract per vendor.
#
# 🚨 **These are dicts, and every one was checked against ``rest_api_source``
# itself — not against ``create_paginator``.** The two dlt entry points
# disagree: ``create_paginator`` happily builds a paginator carrying
# ``has_more_path`` or ``stop_after_empty_page``, and ``rest_api_source``'s
# config schema then **rejects** the same dict, because
# ``JSONResponseCursorPaginatorConfig`` declares neither field. A table
# validated with the factory therefore passes its own test and fails in
# production — which is what the first draft of this table did.
#
# Dicts rather than paginator *instances* is also deliberate: an instance is
# stateful (it carries the cursor between pages), so a module-level one would
# leak position across runs. Dicts are additionally the only thing a user can
# express through **Use raw JSON config**, so whatever we can write here, they
# can write too.
#: Salesforce's REST API version, in the path of every Query API call.
SALESFORCE_API_VERSION = "v59.0"

#: Shapes the two user-influenceable values are allowed to take.
#:
#: ⚠️ ``api_version`` is read from ``dlt_config``, i.e. it is **user input**, and
#: it is interpolated into a request path. Unvalidated, `../..` in it reshapes
#: the URL — the egress guard pins the host, so this is not SSRF, but it still
#: lets a caller aim the request at a path we did not choose. The sObject
#: pattern backs the ``noqa: S608`` below: bandit is right that an f-string
#: builds the SOQL, and a constant-shaped identifier is what makes it safe.
_SALESFORCE_API_VERSION_RE = re.compile(r"^v\d+\.\d+$")
_SOQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

#: (resource name, sObject) for the objects loaded by default (core#850).
#:
#: The names are unchanged from the describe-endpoint resources they replace, so
#: the landed tables keep their names; only their contents become records.
SALESFORCE_DEFAULT_OBJECTS: tuple[tuple[str, str], ...] = (
    ("accounts", "Account"),
    ("contacts", "Contact"),
    ("opportunities", "Opportunity"),
)


def _salesforce_default_resources(api_version: str) -> list[dict]:
    """Query-API resources for the default objects (core#850).

    These used to be ``services/data/vXX.X/sobjects/<Name>`` — the **sObject
    Basic Information** resource, which answers one object of field metadata
    plus up to 20 ``recentItems`` scoped to the *calling user*. So a Salesforce
    upload that ran successfully landed describe output, with a row count, a
    green status and a table in the warehouse; it was simply not the data the
    user asked for.

    🚨 ``FIELDS(STANDARD)`` is not interchangeable with the other two. SOQL has
    no ``SELECT *``, and of Salesforce's three field bundles both
    ``FIELDS(ALL)`` and ``FIELDS(CUSTOM)`` require ``LIMIT 200`` or less — which
    would cap every extract at 200 rows. That is core#823's silent truncation
    arriving through the query instead of through the paginator, and it is worse
    here because the limit would look deliberate. ``FIELDS(STANDARD)`` carries
    no such requirement.

    ⚠️ ``data_selector`` is stated rather than left to dlt's detection, for the
    reason the paginator table above already gives: a heuristic that guesses
    right for nine vendors and wrong for the tenth fails silently, and the
    Query API wraps its rows in ``records`` beside ``totalSize`` and ``done``.
    """
    if not _SALESFORCE_API_VERSION_RE.match(api_version):
        raise DltRunnerError(
            f"Invalid Salesforce api_version {api_version!r}: expected a form like 'v59.0'"
        )
    for _name, sobject in SALESFORCE_DEFAULT_OBJECTS:
        if not _SOQL_IDENTIFIER_RE.match(sobject):
            raise DltRunnerError(f"Invalid Salesforce sObject name {sobject!r}")
    return [
        {
            "name": name,
            "endpoint": {
                "path": f"services/data/{api_version}/query",
                # noqa justified by _SOQL_IDENTIFIER_RE above, asserted for every
                # entry immediately before this: the only interpolation is an
                # identifier matched against ^[A-Za-z][A-Za-z0-9_]*$.
                "params": {"q": f"SELECT FIELDS(STANDARD) FROM {sobject}"},  # noqa: S608
                "data_selector": "records",
            },
        }
        for name, sobject in SALESFORCE_DEFAULT_OBJECTS
    ]


SAAS_PAGINATORS: dict[str, dict] = {
    # `starting_after` carrying the last item's id. There is deliberately no
    # `has_more` stop condition — the schema rejects `has_more_path` — so the
    # walk ends when a page comes back empty and `data.[-1].id` resolves to
    # nothing. That costs exactly one extra request per resource and cannot
    # run away.
    "stripe": {
        "type": "cursor",
        "cursor_path": "data.[-1].id",
        "cursor_param": "starting_after",
    },
    # RFC 5988 `Link: <...>; rel="next"`. Absent on the last page, which the
    # paginator reads as "stop" rather than as an error.
    "github": {"type": "header_link"},
    "shopify": {"type": "header_link"},
    "freshdesk": {"type": "header_link"},
    # CRM v3: `{"results": [...], "paging": {"next": {"after": "..."}}}`.
    # `paging` is omitted entirely on the last page — safe, no has_more_path.
    "hubspot": {"type": "cursor", "cursor_path": "paging.next.after", "cursor_param": "after"},
    # Web API cursor: `response_metadata.next_cursor`, empty when exhausted.
    "slack": {
        "type": "cursor",
        "cursor_path": "response_metadata.next_cursor",
        "cursor_param": "cursor",
    },
    # Both hand back an absolute next-page URL rather than a cursor to re-send.
    "zendesk": {"type": "json_link", "next_url_path": "next_page"},
    # Salesforce's Query API hands back a next-page **path** (not an absolute
    # URL) in `nextRecordsUrl`, e.g. `/services/data/v59.0/query/01g...-2000`.
    # It was exempt only because its resources were describe endpoints with
    # nothing to paginate (core#850); now that they are queries, it pages.
    "salesforce": {"type": "json_link", "next_url_path": "nextRecordsUrl"},
    "facebook_ads": {"type": "json_link", "next_url_path": "paging.next"},
    # Asana's offset token, echoed back as `offset`.
    "asana": {"type": "cursor", "cursor_path": "next_page.offset", "cursor_param": "offset"},
    # Pipedrive v1 is offset/limit. `total_path` is None deliberately: the
    # response has no `total`, and OffsetPaginator's default of "total" would
    # look for a key that never arrives. A short page ends the walk.
    "pipedrive": {
        "type": "offset",
        "limit": 100,
        "offset_param": "start",
        "limit_param": "limit",
        "total_path": None,
    },
    # Notion echoes `next_cursor` back as **`start_cursor`**. Worth stating
    # rather than leaving to auto-detection, which guesses the parameter name
    # `cursor`: Notion ignores an unknown parameter and returns page one again,
    # so the guess re-reads the same rows instead of truncating.
    "notion": {"type": "cursor", "cursor_path": "next_cursor", "cursor_param": "start_cursor"},
    # Airtable's continuation token is `offset` in both directions. Same
    # reasoning as Notion — auto-detection guesses `cursor` here too.
    "airtable": {"type": "cursor", "cursor_path": "offset", "cursor_param": "offset"},
}

#: Connectors deliberately left to dlt's auto-detection, each with the hazard
#: that a client-level paginator would introduce. Not "unsure" — a reason.
#:
#: A paginator is configured on the **client**, so it applies to every resource
#: the connector defines. Both entries below have default resources whose
#: pagination contracts differ from one another, and for both the damage from
#: guessing is worse than the truncation this table exists to fix. The real fix
#: for these is per-resource ``endpoint.paginator`` entries, which is a larger
#: change than this one.
SAAS_PAGINATION_EXEMPT: dict[str, str] = {
    "jira": (
        "`rest/api/3/search` is offset-paginated (startAt/maxResults) while "
        "`rest/api/3/project` returns a bare unpaginated array. An offset "
        "paginator would page `search` correctly and then re-request `project` "
        "forever, since Jira ignores `startAt` there and keeps answering with "
        "the same non-empty array. A hang is worse than a short table."
    ),
}

#: Every connector type that reaches ``_rest_api_fallback``.
#:
#: Kept as data so the paginator decision can be *enforced* rather than
#: remembered: ``tests/test_services/test_saas_pagination.py`` re-derives this
#: set from the source of ``_build_saas_source`` and fails if the two disagree,
#: so connector fifteen cannot be added without answering the question that
#: fourteen connectors had never been asked.
REST_FALLBACK_SAAS_TYPES: frozenset[str] = frozenset(
    {
        "stripe",
        "github",
        "hubspot",
        "salesforce",
        "shopify",
        "jira",
        "slack",
        "facebook_ads",
        "zendesk",
        "airtable",
        "notion",
        "pipedrive",
        "freshdesk",
        "asana",
    }
)


#: The paginator ``type`` values dlt accepts, derived from dlt rather than
#: restated, so this cannot drift when dlt adds one.
def _paginator_type_names() -> list[str]:
    from dlt.sources.rest_api.config_setup import PAGINATOR_MAP

    return sorted(PAGINATOR_MAP)


def describe_paginator_rejection(spec, exc: Exception) -> str:
    """Turn dlt's paginator validation failure into something actionable.

    dlt is right to refuse a bad paginator, but it refuses in the form of a
    sixteen-line dump of every config type it tried — which reaches the user as
    the upload's error message. This says what to do instead.

    Same shape as core#608's ``NoSuchModuleError`` handling: the underlying
    library's verdict is correct and its wording is unusable.
    """
    return (
        f"Invalid paginator config {spec!r}. Expected a dict with a 'type' key, "
        f"one of: {', '.join(_paginator_type_names())} — plus that type's own "
        f"fields. Note dlt's paginator *classes* accept options its *config "
        f"schema* does not (has_more_path, stop_after_empty_page), so an option "
        f"you find in the paginator docs may still be refused here. dlt said: {exc}"
    )


# Drivernames used by sql_database() source (SQLAlchemy connections)
SOURCE_DRIVERNAME_MAP = {
    "postgres": "postgresql",
    "mysql": "mysql+pymysql",
    "mssql": "mssql+pymssql",
    "sqlite": "sqlite",
    "redshift": "redshift+redshift_connector",
    "clickhouse": "clickhousedb+connect",
    "duckdb": "duckdb",
    "oracle": "oracle+oracledb",
}

# Types where user→username renaming is needed
_RENAME_USER_TYPES = {
    "postgres",
    "mysql",
    "mssql",
    "sqlite",
    "redshift",
    "snowflake",
    "clickhouse",
    "oracle",
    # Synapse was missing here (core#577). It is in neither this set nor
    # SOURCE_DRIVERNAME_MAP, so `user` was never renamed and dlt received a
    # field it does not declare — with no username at all. Proven by comparing
    # what `_to_dlt_credentials` produces against `SynapseCredentials`' own
    # fields, which needs no ODBC driver and no network; the driver is only
    # required to *connect*, and its absence is why this went unproven in #565.
    "synapse",
}

# ClickHouse table engine types supported by dlt
CLICKHOUSE_ENGINE_TYPES = {"merge_tree", "replicated_merge_tree", "shared_merge_tree"}


def _normalize_oracle_identifier(name: str | None) -> str | None:
    """Normalize an Oracle table/schema identifier for dlt/SQLAlchemy reflection.

    Oracle stores unquoted identifiers UPPERCASE, but SQLAlchemy reflection
    expects the *normalized* (lowercase) form and denormalizes it back to
    uppercase for the data-dictionary lookup. Passing an UPPERCASE name makes
    SQLAlchemy treat it as a case-sensitive quoted identifier, so ``sql_table`` /
    ``sql_database`` reflection misses the table (NoSuchTableError, #347).
    ``normalize_name`` lower-cases a plain uppercase name and leaves genuinely
    quoted / mixed-case names alone.
    """
    if not name:
        return name
    from sqlalchemy.dialects.oracle.base import OracleDialect

    return OracleDialect().normalize_name(name) or name


def _json_chunks(file_obj, chunksize: int):
    """Yield lists of records from one JSON file, whatever shape it is.

    dlt ships `read_jsonl`, which is **JSON Lines only**, but our default glob
    for the `json` connection type is `*.json` — the array/object case. Feeding
    an array to `read_jsonl` does not fail: the whole file parses as one value,
    so dlt writes a parent row with no business columns plus a `__value` child
    table. Another green run with the data in the wrong shape (core#492).

    So the shape is detected rather than assumed:
      - `[...]`        -> the array's elements are the records
      - one JSON value -> a single record (a dict) or its elements (a list)
      - otherwise      -> JSON Lines, streamed a line at a time
    """
    head = file_obj.read(1024)
    file_obj.seek(0)
    first = head.lstrip()[:1]

    if first == b"[":
        records = dlt_json.loadb(file_obj.read())
        for start in range(0, len(records), chunksize):
            yield records[start : start + chunksize]
        return

    chunk = []
    for line in file_obj:
        line = line.strip()
        if not line:
            continue
        try:
            chunk.append(dlt_json.loadb(line))
        except Exception:
            # Not line-delimited after all — most likely one pretty-printed
            # object spanning many lines. Re-read it as a whole.
            file_obj.seek(0)
            value = dlt_json.loadb(file_obj.read())
            yield value if isinstance(value, list) else [value]
            return
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


@dlt.transformer(name="json_records")
def read_json(items, chunksize: int = 1000):
    """Read JSON files whose shape is not known ahead of time.

    Companion to dlt's `read_csv` / `read_parquet` / `read_jsonl`, covering the
    gap that `read_jsonl` leaves: arrays and single objects.
    """
    for file_obj in items:
        with file_obj.open() as f:
            yield from _json_chunks(f, chunksize)


def _resolve_file_format(connection_type: str, file_glob: str, dlt_config: dict) -> str:
    """Decide which reader a file source needs — or refuse to guess.

    Order: an explicit `file_format` wins; then the connection type, which
    names the format for `csv`/`json`/`parquet`; then the glob's extension,
    which is all `s3` has to go on.

    A bare `s3` glob (`*`) reaches none of those, and **that raises**. Loading
    the file listing instead is what core#492 was, and picking a format at
    random would be the same bug with a different payload.
    """
    explicit = dlt_config.get("file_format")
    if explicit:
        normalized = str(explicit).lower().lstrip(".")
        if normalized in {"jsonl", "ndjson"}:
            return "json"
        if normalized not in {"csv", "json", "parquet"}:
            raise DltRunnerError(
                f"Unsupported file_format {explicit!r}. Supported: csv, json, parquet."
            )
        return normalized

    by_type = FILE_FORMAT_BY_TYPE.get(connection_type)
    if by_type:
        return by_type

    suffix = PurePosixPath(file_glob).suffix.lower()
    by_extension = FILE_FORMAT_BY_EXTENSION.get(suffix)
    if by_extension:
        return by_extension

    raise DltRunnerError(
        f"Cannot tell what format {file_glob!r} matches, so there is no way to read it. "
        "Set 'file_format' (csv, json or parquet) in the pipeline config, or narrow "
        "the file pattern to one extension (e.g. '*.csv')."
    )


def _build_format_reader(file_format: str, dlt_config: dict):
    """Build the transformer for a resolved format, with its format options."""
    if file_format == "csv":
        # `read_csv` forwards **pandas_kwargs, so the CSV knobs are pandas'.
        # Only forwarded when set — pandas already infers the header row and
        # column types, and an unrequested override is a silent wrong answer.
        pandas_kwargs = {}
        if dlt_config.get("delimiter"):
            pandas_kwargs["sep"] = dlt_config["delimiter"]
        if dlt_config.get("encoding"):
            pandas_kwargs["encoding"] = dlt_config["encoding"]
        return read_csv(**pandas_kwargs)

    if file_format == "parquet":
        return read_parquet()

    return read_json()


def describe_empty_file_match(bucket_url: str, file_glob: str) -> str:
    """Explain *why* nothing matched, as specifically as the location allows.

    The production case (core#493) was a `bucket_url` set to a full file path:
    the glob is applied *under* that value, so `*.csv` under
    `/docs_samples/customers.csv` matched nothing and the run went green with
    0 rows. "No files matched" alone would leave the user re-checking a glob
    that was never the problem, so the file-vs-directory case is called out by
    name.

    Local paths are diagnosed precisely; remote buckets get the general form,
    because probing a URL for existence costs a second round trip and the
    credentials to do it may be exactly what is wrong.
    """
    # Paths are quoted by hand rather than with !r: on Windows `repr` doubles
    # every backslash, so the path we tell the user to try comes back looking
    # like a typo of itself.
    general = (
        f"No files matched '{file_glob}' under '{bucket_url}'. "
        "The run would have completed with zero rows."
    )

    if "://" in bucket_url:
        return f"{general} Check the prefix and the file pattern."

    if os.path.isfile(bucket_url):
        return (
            f"{general} '{bucket_url}' is a file, but this field must be the "
            "directory that contains your files — the pattern is matched inside "
            f"it. Try '{os.path.dirname(bucket_url)}' instead."
        )

    if not os.path.exists(bucket_url):
        return f"{general} That path does not exist."

    return f"{general} The directory exists but holds nothing matching that pattern."


def _bigquery_credentials(creds: dict) -> dict:
    """Explode a service-account JSON into the fields dlt actually reads.

    dlt's `GcpServiceAccountCredentials` reads `project_id`, `private_key`,
    `client_email` and friends. It does **not** read a blob — passing the JSON
    under any key at all is silently ignored, which is why BigQuery failed in
    prod with *"missing project_id, private_key, client_email"* while the
    connection form was filled in correctly (core#565).

    **Both key names are accepted on purpose.** The connection form writes
    `keyfile_json`; `CONFIG_SCHEMAS` declares `service_account_json`. Connections
    already stored carry the former, so reading only the schema's name would fix
    nothing for anyone who has a BigQuery connection today. The schema is
    corrected to match reality in the same change; this keeps working either way.
    """
    raw = creds.pop("keyfile_json", None) or creds.pop("service_account_json", None)

    if not raw:
        # Leave it alone: dlt will fail with its own message, and on a machine
        # with Application Default Credentials it may legitimately succeed
        # without one.
        return creds

    if isinstance(raw, str):
        try:
            parsed = dlt_json.loads(raw)
        except Exception as exc:
            raise DltRunnerError(
                "BigQuery service account JSON is not valid JSON — paste the whole key "
                "file, including the surrounding braces."
            ) from exc
    else:
        parsed = raw

    if not isinstance(parsed, dict):
        raise DltRunnerError("BigQuery service account JSON must be a JSON object.")

    for field in ("project_id", "private_key", "client_email"):
        if not parsed.get(field):
            raise DltRunnerError(
                f"BigQuery service account JSON is missing {field!r}. Use the key file "
                "downloaded from the Google Cloud console, not a truncated copy."
            )

    # The stored `project` is the project to bill/query; the key's `project_id`
    # is where the service account lives. They are usually the same, and when
    # they differ the explicit field wins.
    project_override = creds.pop("project", None)
    merged = {**parsed}
    if project_override:
        merged["project_id"] = project_override
    return merged


def _ga4_access_token(service_account_json) -> str:
    """Mint a read-only Data API token from the service-account JSON.

    GA4 takes an OAuth bearer token, not an API key, so there is no way to hand
    dlt a static credential — the token has to be minted per run. `google-auth`
    is already a dependency (Google Sheets uses it), so this adds no new
    surface.

    Called from inside the resource, never at build time: `build_source` must
    stay offline so a source can be constructed and inspected without valid
    credentials or a network.
    """
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account

    info = service_account_json
    if isinstance(info, str):
        try:
            info = dlt_json.loads(info)
        except Exception as exc:
            raise DltRunnerError(
                "Google Analytics service account JSON is not valid JSON — paste the whole "
                "key file, including the surrounding braces."
            ) from exc

    try:
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=[GA4_SCOPE]
        )
    except Exception as exc:
        raise DltRunnerError(
            f"Google Analytics service account JSON is missing required fields ({exc}). "
            "Use the key file downloaded from the Google Cloud console."
        ) from exc

    credentials.refresh(Request())
    return credentials.token


def _google_ads_digits(value: str) -> str:
    """Customer IDs are shown as `123-456-7890` and sent as `1234567890`.

    Google's own docs spell this out for `login-customer-id`, and it applies to
    the path segment too. Users copy the hyphenated form off the Ads UI because
    that is the only form the UI ever shows them, so stripping is the
    difference between working and a 404 that blames the customer ID.
    """
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _google_ads_access_token(config: dict) -> str:
    """Exchange the stored refresh token for an access token.

    Unlike GA4 this is a **user** credential, not a service account: service
    accounts reaching the Ads API additionally require domain-wide delegation
    on a Google Workspace domain, which a self-serve user cannot arrange. So
    the form collects the installed-app triple (client id + secret + refresh
    token), which is what Google's own OAuth flow hands out — and why the old
    `service_account_json` field could never have worked here even with a
    developer token beside it.

    Called from inside the resource, never at build time: `build_source` must
    stay offline so a source can be constructed and inspected without valid
    credentials or a network (same rule as GA4).
    """
    session = build_guarded_session()
    response = session.post(
        GOOGLE_OAUTH_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": config.get("client_id", ""),
            "client_secret": config.get("client_secret", ""),
            "refresh_token": config.get("refresh_token", ""),
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise DltRunnerError(
            f"Google refused the OAuth refresh ({response.status_code}): "
            f"{response.text[:300]}. Re-authorise this connection — a refresh token is "
            "revoked when the Google account's password changes, and expires if unused "
            "for six months."
        )
    token = (response.json() or {}).get("access_token")
    if not token:
        raise DltRunnerError(
            "Google's OAuth response carried no access_token, so the Google Ads request "
            "cannot be authenticated."
        )
    return token


def _google_ads_flatten(value: dict, prefix: str = "") -> dict:
    """One flat column per leaf, joined with `_`."""
    flat: dict = {}
    for key, item in value.items():
        name = f"{prefix}{key}"
        if isinstance(item, dict):
            flat.update(_google_ads_flatten(item, f"{name}_"))
        else:
            flat[name] = item
    return flat


def _google_ads_rows(payload) -> list[dict]:
    """Flatten a `searchStream` response into rows dlt can land.

    Two shapes here are easy to get wrong, and both fail *quietly*:

    * **`searchStream` answers with a JSON array of chunks**, not the single
      object every other endpoint in the API returns. Reading `payload["results"]`
      raises on a list — or, if written defensively with `.get`, yields nothing
      and reports a successful run with zero rows. That is the core#493 shape.
    * **Each `GoogleAdsRow` is nested** — `{"campaign": {"id": …}, "metrics":
      {"clicks": …}}`. Handed to dlt unflattened it lands as child tables, so
      the user's first `SELECT clicks` fails against a table that has no such
      column. GA4 needed the mirror image of this fix (parallel arrays); the
      shape differs, the lesson does not.

    Columns are derived from the rows themselves rather than from `fieldMask`,
    so a field Google returns without being asked still lands instead of being
    silently dropped.
    """
    chunks = payload if isinstance(payload, list) else [payload]
    rows: list[dict] = []
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        for row in chunk.get("results") or []:
            rows.append(_google_ads_flatten(row))
    return rows


def _ga4_rows(payload: dict) -> list[dict]:
    """Flatten one `runReport` response into named columns.

    GA4 answers with parallel arrays — `dimensionHeaders`/`metricHeaders` name
    the columns, and each row carries `dimensionValues`/`metricValues` in the
    same order. Loading that shape as-is gives a table of nested lists that no
    one can query, so the headers are zipped back onto the values here.

    Metrics arrive as **strings** whatever their type. They are coerced using
    the type GA4 declares, because a `sessions` column of text cannot be summed
    — which would make the load technically successful and analytically
    useless, the failure this whole issue has been about.
    """
    dimension_names = [h.get("name") for h in payload.get("dimensionHeaders", [])]
    metric_headers = payload.get("metricHeaders", [])
    metric_names = [h.get("name") for h in metric_headers]
    metric_types = [h.get("type", "") for h in metric_headers]

    rows = []
    for row in payload.get("rows", []):
        record: dict = {}
        for name, cell in zip(dimension_names, row.get("dimensionValues", []), strict=False):
            record[name] = cell.get("value")
        for name, kind, cell in zip(
            metric_names, metric_types, row.get("metricValues", []), strict=False
        ):
            record[name] = _ga4_metric_value(cell.get("value"), kind)
        rows.append(record)
    return rows


def _ga4_metric_value(raw, kind: str):
    """Coerce a GA4 metric string using the type GA4 declares for it."""
    if raw is None or raw == "":
        return None
    try:
        if kind == "TYPE_INTEGER":
            return int(raw)
        if kind in ("TYPE_FLOAT", "TYPE_SECONDS", "TYPE_CURRENCY", "TYPE_MINUTES", "TYPE_HOURS"):
            return float(raw)
    except (TypeError, ValueError):
        # An unparseable number is worth keeping as text rather than dropping:
        # losing the value silently is worse than a column that needs a cast.
        return raw
    return raw


def _kafka_topics(raw) -> list[str]:
    """Normalise the topics field to a list.

    The connection form stores topics as a **comma-separated string**
    (``CONFIG_SCHEMAS["kafka"]``), but the builder used to do
    ``topics if isinstance(topics, list) else [topics]`` — so ``"orders,events"``
    became a single topic literally named ``orders,events``, which subscribes to
    nothing and yields no rows. A second, quieter bug sitting behind core#551's
    louder one.
    """
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(",") if t.strip()]
    return [str(t).strip() for t in raw if str(t).strip()]


#: How a Kafka consumer authenticates to the broker. These live on the
#: **connection**, never on an upload's ``dlt_config`` — ``_kafka_security_kwargs``
#: says why that is a security boundary rather than a preference.
KAFKA_SECURITY_KEYS = (
    "security_protocol",
    "sasl_mechanism",
    "sasl_plain_username",
    "sasl_plain_password",
)

#: What ``kafka-python`` accepts for ``security_protocol``. Checked here so a typo
#: in a free-text form field names the field, instead of surfacing as a bootstrap
#: timeout that reads like an unreachable broker.
_KAFKA_SECURITY_PROTOCOLS = ("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")

#: Confluent Cloud's default and the most common managed choice. Redpanda
#: Serverless wants ``SCRAM-SHA-256``; both are set explicitly on the connection.
_DEFAULT_KAFKA_SASL_MECHANISM = "PLAIN"


def _kafka_security_kwargs(config: dict, dlt_config: dict) -> dict:
    """The SASL/TLS kwargs for ``KafkaConsumer``, read from the CONNECTION only.

    **Why there is no ``dlt_config`` fallback, when ``topics`` has one.**
    ``Connection.config`` is Fernet-encrypted at rest (``config_encrypted``), and
    every value stored under a :data:`~datanika.services.connection_service.SECRET_CONFIG_KEYS`
    name is stripped out of user-visible errors by ``_redact_secrets`` and out of
    ``BackupService.export_backup``. ``Upload.dlt_config`` is a plain ``JSON``
    column with none of that. So accepting ``sasl_plain_password`` there would
    write a broker password in clear text into the database and into every
    backup, outside the redactor's reach — the convenient fallback is precisely
    the one that leaks.

    A security key found in ``dlt_config`` therefore **raises**. Ignoring it
    would be worse than either alternative: the run then fails with a bootstrap
    timeout that names nothing, which is the exact failure this whole change
    exists to remove.
    """
    misplaced = sorted(k for k in KAFKA_SECURITY_KEYS if k in dlt_config)
    if misplaced:
        raise DltRunnerError(
            "Kafka security settings belong on the connection, not in the pipeline "
            f"config: {', '.join(misplaced)}. Set them on the Kafka connection — it "
            "stores credentials encrypted and keeps them out of error messages and "
            "backups, and the pipeline config does neither."
        )

    kwargs = {k: config[k] for k in KAFKA_SECURITY_KEYS if config.get(k) not in (None, "")}

    protocol = kwargs.get("security_protocol")
    if protocol is None:
        return kwargs
    protocol = str(protocol).strip().upper()
    if protocol not in _KAFKA_SECURITY_PROTOCOLS:
        raise DltRunnerError(
            f"Unknown Kafka security_protocol {protocol!r}. "
            f"Expected one of: {', '.join(_KAFKA_SECURITY_PROTOCOLS)}."
        )
    kwargs["security_protocol"] = protocol

    if protocol.startswith("SASL_"):
        missing = [k for k in ("sasl_plain_username", "sasl_plain_password") if k not in kwargs]
        if missing:
            # Without this the consumer negotiates as anonymous and the broker
            # simply drops it, surfacing as `Unable to bootstrap from [...]` —
            # a message about the network for a problem in the credentials.
            raise DltRunnerError(
                f"security_protocol {protocol} needs {' and '.join(missing)} "
                "on the Kafka connection."
            )
        kwargs.setdefault("sasl_mechanism", _DEFAULT_KAFKA_SASL_MECHANISM)

    return kwargs


def _kafka_record(message) -> dict:
    """One Kafka message as a row, with the provenance needed to deduplicate.

    The value is decoded as JSON when it parses and kept as text when it does
    not — a topic carrying plain strings or protobuf should still land rather
    than fail the run, and burying the raw payload is how you end up unable to
    say what arrived.
    """
    raw = message.value
    if isinstance(raw, bytes | bytearray):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = None
    else:
        text = raw if isinstance(raw, str) else None

    value: dict
    if text is None:
        value = {"_kafka_value_raw": bytes(raw).hex()}
    else:
        try:
            parsed = dlt_json.loads(text)
        except Exception:
            value = {"_kafka_value": text}
        else:
            value = parsed if isinstance(parsed, dict) else {"_kafka_value": parsed}

    key = message.key
    if isinstance(key, bytes | bytearray):
        try:
            key = key.decode("utf-8")
        except UnicodeDecodeError:
            key = bytes(key).hex()

    return {
        **value,
        "_kafka_topic": message.topic,
        "_kafka_partition": message.partition,
        "_kafka_offset": message.offset,
        "_kafka_timestamp": message.timestamp,
        "_kafka_key": key,
    }


def _select_endpoints(source, endpoints):
    """Narrow a SaaS source to the endpoints the user ticked (core#532).

    Applied at the one place every SaaS source passes through, rather than in
    each builder: the sixteen branches construct their resources differently
    (verified-source module or REST fallback) but all return a `DltSource`, and
    `with_resources` is dlt's own way to select on one.

    Two sharp edges, both pinned by tests:

    * `with_resources()` with **no arguments selects everything**, so an empty
      or absent list must short-circuit rather than be forwarded. The form only
      writes the key when something is ticked, so `[]` means "unset".
    * dlt raises `ResourcesNotFoundError` for an unknown name. That is the right
      outcome, but its message does not say where the name came from, and the
      names come from a hardcoded picker list that has drifted before — so it is
      re-raised as a `DltRunnerError` naming both sides.

    **A selection where *nothing* resolves is not a selection.** Until core#532
    the picker wrote its names into `dlt_config["endpoints"]` and no builder
    read them, and it defaulted to *every* endpoint ticked — so every upload
    saved before that fix carries a full list of names that no longer exist
    (`Subscription`, `Account`, … against a loader that builds `subscriptions`,
    `charges`, …). Those values are the residue of a control that did nothing,
    not a choice anyone made, and there is no migration for them: they live in
    the `uploads.dlt_config` JSON blob.

    Raising on them would turn "loads more than you asked for" into "fails
    every run", for uploads whose owner never touched the picker. So a
    selection that resolves to nothing at all is treated as unset — the
    pre-fix behaviour, loudly logged. A *partial* match is different: somebody
    did choose, and a name that has since disappeared is worth stopping for.
    """
    if not endpoints:
        return source

    available = set(source.resources.keys())
    known = [e for e in endpoints if e in available]
    unknown = [e for e in endpoints if e not in available]

    if unknown and not known:
        logger.warning(
            "Ignoring a stale endpoint selection %s — none of them exist on this source "
            "(it builds %s). This upload predates core#532, when the picker wrote names "
            "the loader never read. Loading everything, as it did before; re-select the "
            "endpoints on the upload to narrow it.",
            sorted(unknown),
            sorted(available),
        )
        return source

    if unknown:
        raise DltRunnerError(
            f"Unknown endpoint(s) {sorted(unknown)} for this source. "
            f"Available: {sorted(available)}."
        )

    return source.with_resources(*endpoints)


def _first_file_or_none(lister):
    """Peek the lister dlt itself will use.

    Deliberately not a second implementation with `fsspec_filesystem` +
    `glob_files`: that path rejects our plain credentials dict (only
    `filesystem()`'s config injection coerces it), and any separate matching
    logic could drift from the loader's. Peeking the same resource means
    detection and loading agree by construction.

    Re-iterating afterwards still yields the files, so the peeked lister goes
    on to the pipeline unchanged.
    """
    return next(iter(lister), None)


def _file_table_name(connection_type: str, file_glob: str, dlt_config: dict) -> str:
    """Name the destination table for a file source.

    Required, not cosmetic: a piped transformer takes the transformer's name,
    so the unrenamed result lands in a table called **`_read_csv`** — which
    reads as a dlt internal. Order: an explicit `table_name`; then the glob's
    stem when it names one file (`customers.csv` -> `customers`); then the
    connection type, which is at least stable and predictable.
    """
    explicit = dlt_config.get("table_name")
    if explicit:
        return str(explicit)

    if not any(char in file_glob for char in _GLOB_WILDCARDS):
        stem = PurePosixPath(file_glob).stem
        if stem:
            return stem

    return connection_type


class DltRunnerService:
    """Builds dlt pipeline objects from connection configs and pipeline settings."""

    SUPPORTED_SOURCE_TYPES = {
        "postgres",
        "mysql",
        "mssql",
        "sqlite",
        "clickhouse",
        "duckdb",
        "oracle",
    }
    #: Destinations dlt can actually build a factory for.
    #:
    #: 🚨 **Written longhand since core#862. It used to be**
    #: ``(SUPPORTED_SOURCE_TYPES - {"oracle"}) | {...}``, which encodes *"every
    #: SQL source is also a destination"*. That is true for postgres, mssql,
    #: clickhouse and duckdb, and **false for mysql and sqlite** — dlt ships no
    #: destination for either, so ``build_destination``'s unconditional
    #: ``getattr(dlt.destinations, connection_type)`` raised ``AttributeError``
    #: and neither has ever loaded a row (core#865).
    #:
    #: The comment on that line said *"Oracle is source-only — dlt ships no
    #: Oracle destination"*, which is **exactly the reasoning that should have
    #: excluded the other two**. One exception was carved by hand and the two
    #: that needed identical treatment were missed; a derivation with a manual
    #: subtraction is a list with extra steps, and it hides which members were
    #: ever checked. So: no derivation, and a test that asks dlt directly.
    #:
    #: ⚠️ **Membership here is a claim, not a capability.** Assert
    #: ``hasattr(dlt.destinations, t)`` on the module — never ``t in`` this set.
    #: See ``test_connector_type_contracts.py``; measured 2026-09-01 against
    #: dlt 1.21.0, where the pre-fix set of 11 resolved 9.
    SUPPORTED_DESTINATION_TYPES = {
        "postgres",
        "mssql",
        "clickhouse",
        "duckdb",
        "bigquery",
        "snowflake",
        "redshift",
        "databricks",
        "synapse",
    }

    def __init__(self, pipelines_dir: str | None = None):
        self._pipelines_dir = pipelines_dir

    @staticmethod
    def _to_dlt_credentials(connection_type: str, config: dict) -> dict:
        """Convert stored connection config to dlt-compatible credentials.

        Adds drivername for SQL source types, renames user→username, and maps
        the cloud-warehouse fields whose names never lined up (core#565).

        **BigQuery and Databricks could not complete a single run.** This
        translated only for SQL types, so warehouse configs passed through
        untouched and dlt never saw a credential it recognised:

            stored                          dlt wants
            keyfile_json                    project_id, private_key, client_email, …
            host / token                    server_hostname / access_token
            account                         host

        Verified against dlt's own resolution rather than a table — and worth
        knowing why that mattered: on a developer machine with gcloud ADC,
        BigQuery resolves *whatever you pass*, including nothing at all, because
        Application Default Credentials fill the gap. The failure only appears
        where there is no ADC, which is prod.
        """
        creds = dict(config)

        for key in _NON_CREDENTIAL_WAREHOUSE_KEYS.get(connection_type, ()):
            creds.pop(key, None)

        if connection_type in _WAREHOUSE_CREDENTIAL_RENAMES:
            for stored, wanted in _WAREHOUSE_CREDENTIAL_RENAMES[connection_type].items():
                if stored in creds:
                    creds[wanted] = creds.pop(stored)

        if connection_type == "bigquery":
            creds = _bigquery_credentials(creds)

        # Rename user → username for SQL and Snowflake types
        if connection_type in _RENAME_USER_TYPES and "user" in creds:
            creds["username"] = creds.pop("user")

        drivername = SOURCE_DRIVERNAME_MAP.get(connection_type)
        if drivername:
            creds["drivername"] = drivername
            # SQLite: path stored as "path", dlt expects "database"
            if connection_type == "sqlite" and "path" in creds:
                creds["database"] = creds.pop("path")
            # DuckDB: path stored as "path", dlt expects "database"
            if connection_type == "duckdb" and "path" in creds:
                creds["database"] = creds.pop("path")

        # Oracle: connect by service name (PDB/RAC/Autonomous) unless use_sid.
        # dlt builds the SQLAlchemy URL from these components, so — as with
        # _build_sa_url — a "database" in the URL path resolves to a SID (#329).
        if connection_type == "oracle":
            use_sid = bool(creds.pop("use_sid", False))
            if not use_sid and "database" in creds:
                query = dict(creds.get("query") or {})
                query["service_name"] = creds.pop("database")
                creds["query"] = query

        return creds

    def build_destination(self, connection_type: str, config: dict):
        """Map ConnectionType to a dlt destination factory.

        Supports SQL databases and cloud warehouses.
        ClickHouse accepts ``table_engine_type`` in config
        (merge_tree | replicated_merge_tree | shared_merge_tree).
        Raises DltRunnerError for unsupported types.
        """
        if connection_type not in self.SUPPORTED_DESTINATION_TYPES:
            raise DltRunnerError(f"Unsupported destination type: {connection_type}")

        factory = getattr(dlt.destinations, connection_type, None)
        if factory is None:
            # core#865. `mysql` and `sqlite` are in SUPPORTED_DESTINATION_TYPES
            # and dlt ships no destination for either, so this used to be a bare
            # `AttributeError: module 'dlt.destinations' has no attribute
            # 'mysql'` raised from inside a Celery task — which reads as a dlt
            # bug rather than as us advertising something we do not have.
            #
            # ⚠️ The entries stay. Withdrawing an advertised capability is a
            # product decision (core#865, and core#862 for the SUPPORTED_ADAPTERS
            # twin), so what changes here is only that the refusal names its
            # cause. tests/test_services/test_supported_sets_resolve.py holds the
            # list, asserts it only ever shrinks, and fails on a NEW one.
            raise DltRunnerError(
                f"Destination type '{connection_type}' is advertised but dlt ships no "
                f"destination for it. Choose another destination for this pipeline."
            )
        kwargs: dict = {"credentials": self._to_dlt_credentials(connection_type, config)}

        # ClickHouse: pass table_engine_type for cluster support
        if connection_type == "clickhouse":
            engine = config.get("table_engine_type", "merge_tree")
            if engine in CLICKHOUSE_ENGINE_TYPES:
                kwargs["table_engine_type"] = engine

        return factory(**kwargs)

    def build_source(
        self,
        connection_type: str,
        config: dict,
        dlt_config: dict,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """Build a dlt source from connection config.

        Branches on dlt_config mode:
        - single_table → sql_table() with optional incremental
        - full_database (default) → sql_database() with optional table_names filter

        Raises DltRunnerError for unsupported types.
        """
        if connection_type in SUPPORTED_FILE_TYPES:
            return self._build_file_source(connection_type, config, dlt_config)

        if connection_type in SUPPORTED_SHEETS_TYPES:
            return self._build_google_sheets_source(config, dlt_config)

        if connection_type in SUPPORTED_REST_TYPES:
            return self._build_rest_api_source(config, dlt_config)

        if connection_type in SUPPORTED_OPENAPI_TYPES:
            return self._build_openapi_source(config, dlt_config)

        if connection_type in SUPPORTED_MONGODB_TYPES:
            return self._build_mongodb_source(config, dlt_config, batch_size)

        if connection_type in SUPPORTED_SAAS_TYPES:
            source = self._build_saas_source(connection_type, config, dlt_config)
            return _select_endpoints(source, dlt_config.get("endpoints"))

        if connection_type in SUPPORTED_KAFKA_TYPES:
            return self._build_kafka_source(config, dlt_config)

        if connection_type not in self.SUPPORTED_SOURCE_TYPES:
            raise DltRunnerError(f"Unsupported source type: {connection_type}")

        mode = dlt_config.get("mode", "full_database")
        schema = dlt_config.get("source_schema")
        if connection_type == "oracle":
            schema = _normalize_oracle_identifier(schema)

        creds = self._to_dlt_credentials(connection_type, config)

        # Arrow backend (E6) — when callers set backend="pyarrow", dlt's
        # sql_database/sql_table yields pa.Table per chunk instead of dicts.
        # Bypasses JSON normalization — 5.8x speedup on large MySQL loads.
        backend = dlt_config.get("backend")

        # E10 — SQL filter pushdown. Translate FILTER_OPS entries to a
        # query_adapter_callback so WHERE runs on the source DB, not after
        # fetch. Full-database mode doesn't support per-table filters here
        # (a callback binds to a single table); we fall through to the
        # in-memory add_filter path for that case.
        from datanika.services._sql_filter_pushdown import make_query_adapter

        filters_cfg = dlt_config.get("filters")
        query_adapter = None
        if filters_cfg and mode == "single_table":
            query_adapter = make_query_adapter(filters_cfg)

        if mode == "single_table":
            table = dlt_config["table"]
            if connection_type == "oracle":
                table = _normalize_oracle_identifier(table)
            kwargs = {"credentials": creds, "table": table, "chunk_size": batch_size}
            if schema is not None:
                kwargs["schema"] = schema
            if backend is not None:
                kwargs["backend"] = backend
            if query_adapter is not None:
                kwargs["query_adapter_callback"] = query_adapter
            incremental_cfg = dlt_config.get("incremental")
            if incremental_cfg is not None:
                inc_kwargs = {"cursor_path": incremental_cfg["cursor_path"]}
                if "initial_value" in incremental_cfg:
                    inc_kwargs["initial_value"] = incremental_cfg["initial_value"]
                if "row_order" in incremental_cfg:
                    inc_kwargs["row_order"] = incremental_cfg["row_order"]
                kwargs["incremental"] = dlt.sources.incremental(**inc_kwargs)
            return sql_table(**kwargs)
        else:
            kwargs = {"credentials": creds, "chunk_size": batch_size}
            if schema is not None:
                kwargs["schema"] = schema
            if backend is not None:
                kwargs["backend"] = backend
            table_names = dlt_config.get("table_names")
            if table_names is not None:
                if connection_type == "oracle":
                    table_names = [_normalize_oracle_identifier(t) for t in table_names]
                kwargs["table_names"] = table_names
            return sql_database(**kwargs)

    def _build_file_source(self, connection_type: str, config: dict, dlt_config: dict):
        """Build a dlt filesystem source that loads file **contents**.

        `filesystem()` alone lists files; piping it through a format reader is
        what produces rows. See `_resolve_file_format` for how the reader is
        chosen and `_file_table_name` for why the result is always renamed.
        """
        bucket_url = dlt_config.get("bucket_url") or config.get("bucket_url", "")
        if not bucket_url:
            raise DltRunnerError("File sources require 'bucket_url' in config or dlt_config")

        file_glob = dlt_config.get("file_glob") or DEFAULT_FILE_GLOBS.get(connection_type, "*")

        kwargs = {"bucket_url": bucket_url, "file_glob": file_glob}

        if connection_type == "s3":
            credentials = {k: v for k, v in config.items() if k in AWS_CREDENTIAL_KEYS}
            if credentials:
                kwargs["credentials"] = credentials

        file_format = _resolve_file_format(connection_type, file_glob, dlt_config)
        reader = _build_format_reader(file_format, dlt_config)
        table_name = _file_table_name(connection_type, file_glob, dlt_config)

        lister = filesystem(**kwargs)

        # Matching nothing is not a successful load of nothing (core#493). A
        # typo'd path, a moved file, an export that didn't run and an emptied
        # prefix all produced `success` / `Rows: 0`, which is byte-identical to
        # a healthy run. Fail here, where the reason is still known.
        if _first_file_or_none(lister) is None:
            raise DltRunnerError(describe_empty_file_match(bucket_url, file_glob))

        return (lister | reader).with_name(table_name)

    @staticmethod
    def _rest_api_from_parts(
        base_url: str,
        resources: list,
        *,
        auth: dict | None = None,
        headers: dict | None = None,
        paginator: dict | None = None,
        resource_defaults: dict | None = None,
    ):
        """Assemble a ``rest_api_source`` from client parts + a resource list.

        Shared by the generic ``rest_api`` connector and the ``openapi``
        connector (whose resources are derived from a spec).
        """
        # Two layers, because the pre-flight can only see base_url (core#338):
        # it rejects an obviously-internal target before any request is built,
        # while the guarded session re-checks every URL the worker actually
        # sends — redirect hops, an absolute resource path that overrides
        # base_url, and paginator `next` links (core#403).
        validate_egress_host(base_url)
        client_config: dict = {"base_url": base_url, "session": build_guarded_session()}
        if headers:
            client_config["headers"] = headers
        if auth:
            client_config["auth"] = auth
        if paginator:
            client_config["paginator"] = paginator
        rest_config: dict = {"client": client_config, "resources": resources}
        if resource_defaults:
            rest_config["resource_defaults"] = resource_defaults

        if not paginator:
            return rest_api_source(rest_config)

        # A bad paginator must fail, and fail *legibly* (core#823). dlt already
        # rejects one — but with a sixteen-line dump of every config type it
        # tried, which is what the user sees on their upload.
        #
        # Whether the paginator is to blame is decided **behaviourally**: build
        # again without it and see if that succeeds. A substring match on dlt's
        # message would be a second, worse model of dlt's validator — and this
        # whole defect came from consulting the wrong part of dlt.
        try:
            return rest_api_source(rest_config)
        except Exception as exc:
            without = dict(rest_config)
            without["client"] = {k: v for k, v in client_config.items() if k != "paginator"}
            try:
                rest_api_source(without)
            except Exception:
                raise  # something other than the paginator is wrong; keep dlt's words
            raise DltRunnerError(describe_paginator_rejection(paginator, exc)) from exc

    def _build_rest_api_source(self, config: dict, dlt_config: dict):
        """Build a dlt REST API source from connection config + dlt_config."""
        base_url = config.get("base_url") or dlt_config.get("base_url")
        if not base_url:
            raise DltRunnerError("REST API source requires 'base_url'")

        resources = dlt_config.get("resources")
        if not resources or not isinstance(resources, list):
            raise DltRunnerError("REST API source requires 'resources' list in dlt_config")

        return self._rest_api_from_parts(
            base_url,
            resources,
            headers=config.get("headers") or dlt_config.get("headers"),
            auth=config.get("auth") or dlt_config.get("auth"),
            paginator=dlt_config.get("paginator"),
            resource_defaults=dlt_config.get("resource_defaults"),
        )

    def _build_openapi_source(self, config: dict, dlt_config: dict):
        """Build a rest_api source from a spec-derived ``openapi`` connection.

        The connection stores the full resource catalog (from
        ``openapi_import.parse_openapi_spec``); the upload's
        ``dlt_config.resource_names`` selects a subset. Our private keys
        (``columns``, ``_source``) are stripped before handing resources to dlt.
        """
        base_url = config.get("base_url") or dlt_config.get("base_url")
        if not base_url:
            raise DltRunnerError("OpenAPI source requires 'base_url'")

        catalog = config.get("resources")
        if not catalog or not isinstance(catalog, list):
            raise DltRunnerError("OpenAPI source has no resource catalog — re-parse the spec")

        selected = dlt_config.get("resource_names")
        if selected:
            wanted = set(selected)
            catalog = [r for r in catalog if r.get("name") in wanted]
            if not catalog:
                raise DltRunnerError(
                    "None of the requested resource_names exist in this connection"
                )

        resources = [
            {k: v for k, v in r.items() if k not in ("columns", "_source")} for r in catalog
        ]
        return self._rest_api_from_parts(
            base_url,
            resources,
            headers=config.get("headers"),
            auth=config.get("auth"),
            paginator=config.get("paginator") or dlt_config.get("paginator"),
        )

    def _build_google_sheets_source(self, config: dict, dlt_config: dict):
        """Build a dlt source for Google Sheets using gspread."""
        from datanika.services.google_sheets_source import google_sheets_source

        spreadsheet_url = config.get("spreadsheet_url") or dlt_config.get("spreadsheet_url", "")
        if not spreadsheet_url:
            raise DltRunnerError("Google Sheets source requires 'spreadsheet_url'")

        credentials_json = config.get("service_account_json") or dlt_config.get(
            "service_account_json", ""
        )
        if not credentials_json:
            raise DltRunnerError("Google Sheets source requires 'service_account_json'")

        sheet_names = dlt_config.get("sheet_names")

        return google_sheets_source(
            spreadsheet_url=spreadsheet_url,
            credentials_json=credentials_json,
            sheet_names=sheet_names,
        )

    def _build_mongodb_source(self, config: dict, dlt_config: dict, batch_size: int):
        """Build a dlt source for MongoDB using pymongo.

        **The database in a MongoDB URI path is also the authentication
        database.** This used to build `mongodb://u:p@host:port/<target-db>`
        with no `authSource`, so the driver looked for the user *inside the
        database being read* — and MongoDB users are conventionally created in
        `admin`, which is what `MONGO_INITDB_ROOT_USERNAME` does in the official
        image and what Atlas and every managed provider do. So the connector
        could not authenticate against any standard deployment (core#550).

        It went unnoticed because an unauthenticated mongod is unaffected, and
        that is what a local dev instance looks like.

        `auth_source` defaults to `admin` rather than to the target database:
        that is the configuration almost everyone has, and leaving the old
        behaviour as the default would mean the connector stays broken unless
        the user knows to set a field nobody told them about. Anyone who really
        does keep the user inside the target database sets `auth_source` to that
        database name and is back to the previous behaviour — explicitly.
        """
        from datanika.services.mongodb_source import build_connection_uri, mongodb_source

        database = config.get("database") or dlt_config.get("database", "")
        if not database:
            raise DltRunnerError("MongoDB source requires 'database' in config")

        # Assembled by the shared builder rather than inline. The URI used to be
        # written out here *and* in `connection_service._test_mongodb`, and the
        # `authSource` fix above landed only here — so Test Connection failed on
        # connections whose runs succeeded (core#625).
        uri = build_connection_uri({**config, "database": database})

        collection_names = dlt_config.get("collection_names")

        # E11 — server-side filter + incremental pushdown.
        # `query` is a verbatim Mongo filter dict; `incremental` (same config
        # shape as sql_table) becomes {cursor_path: {"$gt": initial_value}}
        # merged into the filter. Mirrors what sql_table does today.
        query = dlt_config.get("query")
        incremental_cursor = None
        incremental_cfg = dlt_config.get("incremental")
        if incremental_cfg is not None:
            cursor_path = incremental_cfg.get("cursor_path")
            initial_value = incremental_cfg.get("initial_value")
            if cursor_path is not None and initial_value is not None:
                incremental_cursor = (cursor_path, initial_value)

        return mongodb_source(
            connection_uri=uri,
            database=database,
            collection_names=collection_names,
            batch_size=batch_size,
            query=query,
            incremental_cursor=incremental_cursor,
        )

    def _build_saas_source(self, connection_type: str, config: dict, dlt_config: dict):
        """Build a source for a SaaS connector.

        **The REST fallback is the real implementation, not the fallback.** This
        docstring used to say the verified sources were "bundled via ``dlt init``
        at Docker build time"; nothing bundles them, and the Dockerfile says so
        deliberately — *"dlt verified sources … use REST API fallback when not
        installed via `dlt init`. This avoids dependency conflicts in Docker."*
        So in every deployment, dev and prod alike, each ``from <source> import``
        below raises ``ImportError`` and the fallback runs (core#543).

        That mattered twice: the picker offered resource names taken from the
        verified sources while the fallback built different ones (core#532), and
        the three connectors with **no** fallback were not degraded but dead —
        their ``except ImportError`` re-raised, so a run could only ever fail.

        The verified-source branches are kept because a self-hoster may run
        ``dlt init``, and the imports are what make that work with no code
        change. They are simply not the path anyone here exercises.
        """
        # core#823. Resolved once, for every branch below.
        #
        # The user's own paginator wins over ours: ``"paginator"`` has always
        # been an accepted ``dlt_config`` key (it is in ``INTERNAL_CONFIG_KEYS``)
        # and this builder never read it, so the documented **Use raw JSON
        # config** escape hatch turned the run green having changed nothing.
        # It has to work here, because our per-vendor table is a set of claims
        # about other people's APIs and some of them will be wrong.
        #
        # An override applies to the ``SAAS_PAGINATION_EXEMPT`` connectors too —
        # those are the ones most likely to need it.
        paginator = dlt_config.get("paginator") or SAAS_PAGINATORS.get(connection_type)

        if connection_type == "stripe":
            api_key = config.get("api_key") or config.get("stripe_secret_key", "")
            if not api_key:
                raise DltRunnerError("Stripe source requires 'api_key'")
            try:
                from stripe_analytics import stripe_source

                endpoints = dlt_config.get("endpoints")
                kwargs: dict = {"stripe_secret_key": api_key}
                if endpoints:
                    kwargs["endpoints"] = tuple(endpoints)
                start_date = dlt_config.get("start_date")
                if start_date:
                    kwargs["start_date"] = start_date
                return stripe_source(**kwargs)
            except ImportError:
                return self._rest_api_fallback(
                    "https://api.stripe.com/v1/",
                    {"type": "bearer", "token": api_key},
                    dlt_config.get("resources")
                    or [
                        {"name": "customers", "endpoint": {"path": "customers"}},
                        {"name": "invoices", "endpoint": {"path": "invoices"}},
                        {"name": "subscriptions", "endpoint": {"path": "subscriptions"}},
                        {"name": "products", "endpoint": {"path": "products"}},
                        {"name": "prices", "endpoint": {"path": "prices"}},
                        {"name": "charges", "endpoint": {"path": "charges"}},
                    ],
                    paginator=paginator,
                )

        if connection_type == "github":
            access_token = config.get("access_token") or config.get("api_key", "")
            if not access_token:
                raise DltRunnerError("GitHub source requires 'access_token'")
            owner = dlt_config.get("owner") or config.get("owner", "")
            repo = dlt_config.get("repo") or config.get("repo", "")
            if not owner or not repo:
                raise DltRunnerError("GitHub source requires 'owner' and 'repo'")
            try:
                from github import github_reactions, github_repo_events

                source_type = dlt_config.get("github_source_type", "reactions")
                if source_type == "events":
                    return github_repo_events(owner, repo, access_token=access_token)
                return github_reactions(owner, repo, access_token=access_token, items_per_page=100)
            except ImportError:
                return self._rest_api_fallback(
                    "https://api.github.com/",
                    None,
                    dlt_config.get("resources")
                    or [
                        {
                            "name": "issues",
                            "endpoint": {
                                "path": f"repos/{owner}/{repo}/issues",
                            },
                        },
                        {
                            "name": "pulls",
                            "endpoint": {
                                "path": f"repos/{owner}/{repo}/pulls",
                            },
                        },
                        {
                            "name": "commits",
                            "endpoint": {
                                "path": f"repos/{owner}/{repo}/commits",
                            },
                        },
                        {
                            "name": "stargazers",
                            "endpoint": {
                                "path": f"repos/{owner}/{repo}/stargazers",
                            },
                        },
                    ],
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/vnd.github+json",
                    },
                    paginator=paginator,
                )

        if connection_type == "hubspot":
            api_key = config.get("api_key") or config.get("access_token", "")
            if not api_key:
                raise DltRunnerError("HubSpot source requires 'api_key'")
            try:
                from hubspot import hubspot

                return hubspot(api_key=api_key)
            except ImportError:
                return self._rest_api_fallback(
                    "https://api.hubapi.com/",
                    {"type": "bearer", "token": api_key},
                    dlt_config.get("resources")
                    or [
                        {
                            "name": "contacts",
                            "endpoint": {
                                "path": "crm/v3/objects/contacts",
                            },
                        },
                        {
                            "name": "companies",
                            "endpoint": {
                                "path": "crm/v3/objects/companies",
                            },
                        },
                        {
                            "name": "deals",
                            "endpoint": {
                                "path": "crm/v3/objects/deals",
                            },
                        },
                    ],
                    paginator=paginator,
                )

        if connection_type == "salesforce":
            access_token = config.get("access_token") or config.get("api_key", "")
            instance_url = config.get("instance_url", "")
            if not access_token or not instance_url:
                raise DltRunnerError("Salesforce source requires 'access_token' and 'instance_url'")
            try:
                from salesforce import salesforce_source

                return salesforce_source()
            except ImportError:
                return self._rest_api_fallback(
                    instance_url.rstrip("/") + "/",
                    {"type": "bearer", "token": access_token},
                    dlt_config.get("resources")
                    or _salesforce_default_resources(
                        str(dlt_config.get("api_version") or SALESFORCE_API_VERSION)
                    ),
                    paginator=paginator,
                )

        if connection_type == "shopify":
            api_key = config.get("api_key") or config.get("access_token", "")
            store = config.get("store", "")
            if not api_key or not store:
                raise DltRunnerError("Shopify source requires 'api_key' and 'store'")
            try:
                from shopify_dlt import shopify_source

                return shopify_source(
                    private_app_password=api_key,
                    shop_url=f"{store}.myshopify.com",
                )
            except ImportError:
                return self._rest_api_fallback(
                    f"https://{store}.myshopify.com/",
                    None,
                    dlt_config.get("resources")
                    or [
                        {
                            "name": "orders",
                            "endpoint": {
                                "path": "admin/api/2024-01/orders.json",
                            },
                        },
                        {
                            "name": "products",
                            "endpoint": {
                                "path": "admin/api/2024-01/products.json",
                            },
                        },
                        {
                            "name": "customers",
                            "endpoint": {
                                "path": "admin/api/2024-01/customers.json",
                            },
                        },
                    ],
                    headers={"X-Shopify-Access-Token": api_key},
                    paginator=paginator,
                )

        if connection_type == "jira":
            email = config.get("email", "")
            api_token = config.get("api_key") or config.get("api_token", "")
            domain = config.get("domain", "")
            if not api_token or not domain:
                raise DltRunnerError("Jira source requires 'api_key' and 'domain'")
            try:
                from jira import jira

                return jira(
                    subdomain=domain,
                    email=email,
                    api_token=api_token,
                )
            except ImportError:
                import base64

                auth_str = base64.b64encode(f"{email}:{api_token}".encode()).decode()
                return self._rest_api_fallback(
                    f"https://{domain}.atlassian.net/",
                    None,
                    dlt_config.get("resources")
                    or [
                        {"name": "issues", "endpoint": {"path": "rest/api/3/search"}},
                        {"name": "projects", "endpoint": {"path": "rest/api/3/project"}},
                    ],
                    headers={"Authorization": f"Basic {auth_str}"},
                    paginator=paginator,
                )

        if connection_type == "slack":
            bot_token = config.get("api_key") or config.get("bot_token", "")
            if not bot_token:
                raise DltRunnerError("Slack source requires 'api_key' (bot token)")
            try:
                from slack import slack_source

                return slack_source(access_token=bot_token)
            except ImportError:
                return self._rest_api_fallback(
                    "https://slack.com/",
                    {"type": "bearer", "token": bot_token},
                    dlt_config.get("resources")
                    or [
                        {
                            "name": "channels",
                            "endpoint": {
                                "path": "api/conversations.list",
                            },
                        },
                        {"name": "users", "endpoint": {"path": "api/users.list"}},
                    ],
                    paginator=paginator,
                )

        if connection_type == "google_analytics":
            credentials_json = config.get("service_account_json", "")
            property_id = config.get("property_id", "") or dlt_config.get("property_id", "")
            if not property_id:
                raise DltRunnerError("Google Analytics source requires 'property_id'")
            try:
                from google_analytics import google_analytics

                kwargs_ga: dict = {"property_id": int(property_id)}
                if credentials_json:
                    kwargs_ga["credentials"] = credentials_json
                return google_analytics(**kwargs_ga)
            except ImportError:
                # The Data API directly, since no verified source is installed
                # anywhere (core#543). This used to re-raise "run dlt init
                # google_analytics" — an instruction for a server the user does
                # not operate, and the only outcome this connector ever had.
                #
                # What blocked it was not a missing library but a missing
                # decision: `runReport` takes a POST body naming dimensions and
                # metrics, and a different body is a different table, so there
                # was no obvious default and nothing was chosen. Daily traffic
                # is the report almost everyone starts from; the rest is
                # overridable per upload.
                if not credentials_json:
                    raise DltRunnerError(
                        "Google Analytics source requires 'service_account_json'"
                    ) from None
                return self._build_ga4_source(property_id, credentials_json, dlt_config)

        if connection_type == "google_ads":
            return self._build_google_ads_source(config, dlt_config)

        if connection_type == "facebook_ads":
            access_token = config.get("access_token") or config.get("api_key", "")
            account_id = config.get("account_id", "") or dlt_config.get("account_id", "")
            if not access_token or not account_id:
                raise DltRunnerError("Facebook Ads source requires 'access_token' and 'account_id'")
            try:
                from facebook_ads import facebook_ads_source

                return facebook_ads_source(account_id=account_id, access_token=access_token)
            except ImportError:
                # Falls back to the Graph API like every other SaaS connector
                # (core#543). Before this, the ImportError re-raised "run
                # dlt init facebook_ads" — advice for a server the user does
                # not operate, and the only possible outcome, every time.
                #
                # `act_` is part of the ad-account identifier; users paste it
                # inconsistently, so it is normalised rather than demanded.
                account = account_id if str(account_id).startswith("act_") else f"act_{account_id}"
                version = dlt_config.get("api_version") or DEFAULT_FACEBOOK_API_VERSION
                return self._rest_api_fallback(
                    f"https://graph.facebook.com/{version}/",
                    {"type": "bearer", "token": access_token},
                    dlt_config.get("resources")
                    or [
                        {"name": "campaigns", "endpoint": {"path": f"{account}/campaigns"}},
                        # Resource names are the picker's; paths are Facebook's.
                        # `ad_sets`/`creatives` read better than `adsets`/
                        # `adcreatives` and the two are independent here.
                        {"name": "ad_sets", "endpoint": {"path": f"{account}/adsets"}},
                        {"name": "ads", "endpoint": {"path": f"{account}/ads"}},
                        {"name": "creatives", "endpoint": {"path": f"{account}/adcreatives"}},
                    ],
                    paginator=paginator,
                )

        if connection_type == "zendesk":
            subdomain = config.get("subdomain", "") or config.get("domain", "")
            email = config.get("email", "")
            api_token = config.get("api_key") or config.get("api_token", "")
            if not subdomain or not api_token:
                raise DltRunnerError("Zendesk source requires 'subdomain' and 'api_key'")
            try:
                from zendesk import zendesk_support

                creds = {"subdomain": subdomain, "email": email, "token": api_token}
                return zendesk_support(credentials=creds)
            except ImportError:
                return self._rest_api_fallback(
                    f"https://{subdomain}.zendesk.com/",
                    None,
                    dlt_config.get("resources")
                    or [
                        {"name": "tickets", "endpoint": {"path": "api/v2/tickets.json"}},
                        {"name": "users", "endpoint": {"path": "api/v2/users.json"}},
                        {
                            "name": "organizations",
                            "endpoint": {
                                "path": "api/v2/organizations.json",
                            },
                        },
                    ],
                    headers={"Authorization": f"Bearer {api_token}"},
                    paginator=paginator,
                )

        if connection_type == "airtable":
            api_key = config.get("api_key") or config.get("access_token", "")
            base_id = config.get("base_id", "") or dlt_config.get("base_id", "")
            if not api_key or not base_id:
                raise DltRunnerError("Airtable source requires 'api_key' and 'base_id'")
            try:
                from airtable import airtable_source

                return airtable_source(base_id=base_id, access_token=api_key)
            except ImportError:
                return self._rest_api_fallback(
                    f"https://api.airtable.com/v0/{base_id}/",
                    {"type": "bearer", "token": api_key},
                    dlt_config.get("resources")
                    or [
                        {"name": "tables", "endpoint": {"path": ""}},
                    ],
                    paginator=paginator,
                )

        if connection_type == "notion":
            api_key = config.get("api_key") or config.get("access_token", "")
            if not api_key:
                raise DltRunnerError("Notion source requires 'api_key'")
            try:
                from notion import notion_databases

                return notion_databases(api_key=api_key)
            except ImportError:
                return self._rest_api_fallback(
                    "https://api.notion.com/v1/",
                    {"type": "bearer", "token": api_key},
                    dlt_config.get("resources")
                    or [
                        {"name": "databases", "endpoint": {"path": "databases"}},
                        {"name": "pages", "endpoint": {"path": "pages"}},
                    ],
                    headers={"Notion-Version": "2022-06-28"},
                    paginator=paginator,
                )

        # Pipedrive / Freshdesk / Asana have no pinned verified-source module, so
        # they go straight to the generic REST fallback (the same path every other
        # SaaS connector lands on when its verified source isn't installed).
        if connection_type == "pipedrive":
            api_key = config.get("api_key") or config.get("api_token", "")
            if not api_key:
                raise DltRunnerError("Pipedrive source requires 'api_key'")
            return self._rest_api_fallback(
                "https://api.pipedrive.com/v1/",
                {"type": "api_key", "api_key": api_key, "name": "api_token", "location": "query"},
                dlt_config.get("resources")
                or [
                    {"name": "deals", "endpoint": {"path": "deals"}},
                    {"name": "persons", "endpoint": {"path": "persons"}},
                    {"name": "organizations", "endpoint": {"path": "organizations"}},
                    {"name": "activities", "endpoint": {"path": "activities"}},
                    {"name": "pipelines", "endpoint": {"path": "pipelines"}},
                    {"name": "stages", "endpoint": {"path": "stages"}},
                    {"name": "users", "endpoint": {"path": "users"}},
                ],
                paginator=paginator,
            )

        if connection_type == "freshdesk":
            api_key = config.get("api_key", "")
            domain = config.get("domain", "")
            if not api_key:
                raise DltRunnerError("Freshdesk source requires 'api_key'")
            if not domain:
                raise DltRunnerError("Freshdesk source requires 'domain'")
            return self._rest_api_fallback(
                f"https://{domain}.freshdesk.com/api/v2/",
                {"type": "http_basic", "username": api_key, "password": "X"},
                dlt_config.get("resources")
                or [
                    {"name": "tickets", "endpoint": {"path": "tickets"}},
                    {"name": "contacts", "endpoint": {"path": "contacts"}},
                    {"name": "agents", "endpoint": {"path": "agents"}},
                    {"name": "companies", "endpoint": {"path": "companies"}},
                    {"name": "groups", "endpoint": {"path": "groups"}},
                ],
                paginator=paginator,
            )

        if connection_type == "asana":
            access_token = config.get("api_key") or config.get("access_token", "")
            if not access_token:
                raise DltRunnerError("Asana source requires 'api_key'")
            return self._rest_api_fallback(
                "https://app.asana.com/api/1.0/",
                {"type": "bearer", "token": access_token},
                dlt_config.get("resources")
                or [
                    {"name": "workspaces", "endpoint": {"path": "workspaces"}},
                    {"name": "projects", "endpoint": {"path": "projects"}},
                    {"name": "tasks", "endpoint": {"path": "tasks"}},
                    {"name": "users", "endpoint": {"path": "users"}},
                    {"name": "tags", "endpoint": {"path": "tags"}},
                ],
                paginator=paginator,
            )

        raise DltRunnerError(f"Unsupported SaaS source type: {connection_type}")

    def _build_ga4_source(self, property_id, service_account_json, dlt_config: dict):
        """Google Analytics 4 via the Data API's `runReport` (core#543).

        Written by hand rather than through `_rest_api_fallback` for two
        reasons the generic path cannot cover:

        * **The credential is a minted token, not a static key.** GA4 wants an
          OAuth bearer refreshed from the service-account JSON, so there is
          nothing to hand a declarative config up front.
        * **The response is parallel arrays.** `dimensionHeaders` /
          `metricHeaders` name the columns and each row carries values in the
          same order, so a `data_selector` alone would load nested lists nobody
          can query. `_ga4_rows` zips them back together.

        Paged with `limit`/`offset` because GA4 truncates at its page size and
        says so only in `rowCount` — a silent short read otherwise, which is the
        family of bug this whole issue belongs to.
        """
        dimensions = list(dlt_config.get("dimensions") or DEFAULT_GA4_DIMENSIONS)
        metrics = list(dlt_config.get("metrics") or DEFAULT_GA4_METRICS)
        start_date = dlt_config.get("start_date") or DEFAULT_GA4_START_DATE
        end_date = dlt_config.get("end_date") or DEFAULT_GA4_END_DATE
        url = f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"

        @dlt.resource(name="report", primary_key=tuple(dimensions))
        def _report():
            # Minted here, not in the builder: `build_source` must stay offline
            # so a source can be constructed and inspected without credentials.
            token = _ga4_access_token(service_account_json)
            session = build_guarded_session()  # SSRF guard, as everywhere else
            offset = 0
            while True:
                response = session.post(
                    url,
                    json={
                        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
                        "dimensions": [{"name": d} for d in dimensions],
                        "metrics": [{"name": m} for m in metrics],
                        "limit": GA4_PAGE_SIZE,
                        "offset": offset,
                    },
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=60,
                )
                if response.status_code >= 400:
                    # GA4 explains itself well; surfacing its message beats
                    # "request failed" when the cause is usually a property the
                    # service account was never granted access to.
                    raise DltRunnerError(
                        f"Google Analytics rejected the report request "
                        f"({response.status_code}): {response.text[:400]}"
                    )
                payload = response.json()
                rows = _ga4_rows(payload)
                if rows:
                    yield rows
                offset += len(rows)
                if len(rows) < GA4_PAGE_SIZE or offset >= int(payload.get("rowCount", 0)):
                    break

        @dlt.source(name="google_analytics")
        def _ga4_source():
            return [_report()]

        return _ga4_source()

    def _build_google_ads_source(self, config: dict, dlt_config: dict):
        """Google Ads over the REST interface (core#555).

        **Transport, confirmed before building rather than assumed** — the
        issue asks for exactly that, and GA4 is the precedent where the obvious
        path was not the one that worked. The Ads API is gRPC-first but ships a
        documented REST interface (`googleAds:searchStream`, GAQL in the body).
        REST wins here for the same reason it did for GA4 (#569) and Facebook
        Ads (#554): the two alternatives — bundling the dlt verified source, or
        the official `google-ads` client — both drag in `grpcio`/`protobuf`,
        and the Dockerfile deliberately installs no verified sources precisely
        because of that class of dependency conflict. A builder that imports a
        package the image does not carry is core#551's bug wearing a new hat,
        not a fallback.

        **What changed since the withdrawal:** nothing about the API. The gap
        was that the form collected `customer_id` + `service_account_json`,
        and neither a developer token nor a usable OAuth credential. Both are
        now collected, so the connector can authenticate.

        No `primary_key` is declared, deliberately: the query is user-overridable
        via `dlt_config["query"]`, so any key named here would be a guess about
        columns a custom GAQL query need not select — and a primary key naming
        an absent column fails the load rather than degrading it.
        """
        missing = [
            field
            for field in ("developer_token", "client_id", "client_secret", "refresh_token")
            if not config.get(field)
        ]
        customer_id = _google_ads_digits(
            config.get("customer_id", "") or dlt_config.get("customer_id", "")
        )
        if not customer_id:
            missing.append("customer_id")
        if missing:
            raise DltRunnerError(
                "Google Ads source is missing required credentials: "
                f"{', '.join(missing)}. Every Google Ads API request needs a developer "
                "token (from your Google Ads manager account's API Center) plus an OAuth "
                "client and refresh token."
            )

        query = str(dlt_config.get("query") or DEFAULT_GOOGLE_ADS_QUERY).strip()
        version = str(dlt_config.get("api_version") or GOOGLE_ADS_API_VERSION).strip()
        developer_token = config["developer_token"]
        login_customer_id = _google_ads_digits(config.get("login_customer_id", ""))
        url = f"{GOOGLE_ADS_API_HOST}/{version}/customers/{customer_id}/googleAds:searchStream"

        @dlt.resource(name="report")
        def _report():
            # Minted inside the resource, never in the builder: `build_source`
            # stays offline so a source can be constructed without credentials.
            token = _google_ads_access_token(config)
            session = build_guarded_session()  # SSRF guard, as everywhere else
            headers = {
                "Authorization": f"Bearer {token}",
                "developer-token": developer_token,
            }
            if login_customer_id:
                # Only when the OAuth credentials belong to a manager (MCC)
                # account rather than the target account itself.
                headers["login-customer-id"] = login_customer_id

            response = session.post(url, json={"query": query}, headers=headers, timeout=120)
            if response.status_code >= 400:
                # Google explains itself well here and the likely causes are all
                # user-fixable (unapproved developer token, wrong customer id,
                # a sunset API version), so its own message beats ours.
                raise DltRunnerError(
                    f"Google Ads rejected the report request ({response.status_code}) "
                    f"for customer {customer_id} on API {version}: {response.text[:400]}"
                )
            rows = _google_ads_rows(response.json())
            if rows:
                yield rows

        @dlt.source(name="google_ads")
        def _google_ads_source():
            return [_report()]

        return _google_ads_source()

    def _build_kafka_source(self, config: dict, dlt_config: dict):
        """Build a Kafka source that actually consumes (core#551).

        This used to do ``from kafka import kafka_consumer`` — the name of the
        **dlt verified source** created by ``dlt init kafka``, not of
        ``kafka-python`` (which exports ``KafkaConsumer``). Neither shipped, so
        the import always raised and every Kafka upload failed at run time,
        telling the user to run ``dlt init`` on a server they do not operate.
        ``kafka-python`` is now a dependency and this consumes directly.

        Two decisions worth stating, because neither has an obviously right
        answer:

        **Bounding.** A consumer iterates forever; a batch pipeline must stop.
        ``consumer_timeout_ms`` ends iteration after a quiet interval, so a run
        drains what is there and finishes. Configurable, because "quiet" means
        something different on a busy topic than on an hourly one.

        **Offsets.** The connection *requires* a ``group_id``, which is a
        promise that successive runs resume rather than re-read, so offsets are
        committed (``enable_auto_commit``). The honest cost: a crash between
        commit and load loses that window. Every message therefore carries its
        ``_kafka_partition``/``_kafka_offset``, declared as the resource primary
        key — with a ``merge`` write disposition that makes re-reads idempotent,
        so an operator who wants at-least-once can have it by turning
        auto-commit off and letting the key deduplicate.

        **Authentication (core#1054).** This consumer took no security kwargs at
        all until then, so it could address only a broker that lets anyone in —
        and no managed Kafka does. Confluent Cloud, Redpanda Serverless, Aiven
        and Upstash are SASL over TLS on their free tiers, which made "we ship a
        Kafka connector" true of a code path no customer could reach.
        ``_kafka_security_kwargs`` reads the credential off the **connection**,
        and states why it refuses to read one out of ``dlt_config``. An
        unauthenticated broker is unaffected: with no security fields set that
        helper returns ``{}`` and the call below is what it always was.
        """
        bootstrap_servers = config.get("bootstrap_servers", "")
        topics = _kafka_topics(dlt_config.get("topics") or config.get("topics", []))
        group_id = config.get("group_id", "datanika-consumer")
        if not bootstrap_servers:
            raise DltRunnerError("Kafka source requires 'bootstrap_servers'")
        if not topics:
            raise DltRunnerError("Kafka source requires 'topics'")

        try:
            from kafka import KafkaConsumer
        except ImportError:  # pragma: no cover - kafka-python is a dependency
            raise DltRunnerError(
                "Kafka support is missing from this build — kafka-python is not installed. "
                "This is a packaging fault, not a configuration one."
            ) from None

        servers = [s.strip() for s in str(bootstrap_servers).split(",") if s.strip()]
        idle_timeout_ms = int(dlt_config.get("idle_timeout_ms", DEFAULT_KAFKA_IDLE_TIMEOUT_MS))
        auto_offset_reset = dlt_config.get("start_from", "earliest")
        auto_commit = bool(dlt_config.get("enable_auto_commit", True))
        security = _kafka_security_kwargs(config, dlt_config)

        def _topic_resource(topic: str):
            @dlt.resource(
                name=topic,
                primary_key=("_kafka_partition", "_kafka_offset"),
                # Typed explicitly so the destination schema does not depend on
                # what happened to arrive: dlt only materialises a column it saw
                # data for, so a batch where no message carried a key would
                # silently produce a table without `_kafka_key`, and the next
                # batch would add it. Provenance columns should be present
                # whether or not this particular window used them.
                columns={
                    "_kafka_topic": {"data_type": "text"},
                    "_kafka_partition": {"data_type": "bigint"},
                    "_kafka_offset": {"data_type": "bigint"},
                    "_kafka_timestamp": {"data_type": "bigint"},
                    "_kafka_key": {"data_type": "text"},
                },
            )
            def _consume():
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=servers,
                    group_id=group_id,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=auto_commit,
                    consumer_timeout_ms=idle_timeout_ms,
                    # Splatted rather than named: an empty dict is the
                    # unauthenticated broker this builder already supported, so
                    # the PLAINTEXT path is byte-for-byte what it was.
                    **security,
                )
                try:
                    for message in consumer:
                        yield _kafka_record(message)
                finally:
                    consumer.close()

            return _consume

        @dlt.source(name="kafka")
        def _kafka_source():
            # One resource per topic, so each lands in its own table rather
            # than being fused into one by message shape.
            return [_topic_resource(topic)() for topic in topics]

        return _kafka_source()

    @classmethod
    def _rest_api_fallback(
        cls,
        base_url: str,
        auth: dict | None,
        resources: list,
        *,
        paginator: dict | None,
        headers: dict | None = None,
    ):
        """Generic REST API source used when a verified source module is not installed.

        Despite the name this is **the** production path for all ten SaaS
        connectors — none of the verified dlt source modules are installed in
        the image (the Dockerfile is deliberate about it, to avoid dependency
        conflicts), so the ``except ImportError`` branch is what runs.

        It is deliberately a thin alias over :meth:`_rest_api_from_parts` rather
        than a second client builder. The two used to differ in one line — this
        one omitted the pinning session — and the divergence was invisible at
        every call site, so a connector could pick the unguarded path by
        accident. ``salesforce`` did, and its ``instance_url`` is free-form user
        input, which made core#405's DNS-rebinding TOCTOU reachable on the very
        path core#405 was written to close. One builder means the wrong choice
        is no longer available; ``tests/test_security/
        test_egress_saas_fallback_pinning.py`` fails any new call site that
        reaches for ``rest_api_source`` directly.

        ``paginator`` is **keyword-only and has no default** on purpose
        (core#823). It used to be absent entirely, so every SaaS connector
        loaded page one and stopped — silently, on a green run. A default of
        ``None`` would reinstate exactly that failure for the next connector
        somebody adds; with no default, forgetting it is a ``TypeError`` at
        build time instead of five missing customers in a warehouse.
        """
        return cls._rest_api_from_parts(
            base_url, resources, auth=auth, headers=headers, paginator=paginator
        )

    def build_pipeline(
        self,
        pipeline_id: int,
        destination_type: str,
        destination_config: dict,
        dataset_name: str | None = None,
        run_id: int | None = None,
    ):
        """Create a dlt.Pipeline with the given destination.

        When ``run_id`` is provided, pipeline name includes it to avoid
        cross-run state pollution.  ``pipelines_dir`` isolates working files.
        """
        destination = self.build_destination(destination_type, destination_config)
        name = f"pipeline_{pipeline_id}"
        if run_id is not None:
            name = f"{name}_run_{run_id}"
        kwargs: dict = {
            "pipeline_name": name,
            "destination": destination,
        }
        if self._pipelines_dir:
            kwargs["pipelines_dir"] = self._pipelines_dir
        if dataset_name is not None:
            kwargs["dataset_name"] = dataset_name
        return dlt.pipeline(**kwargs)

    def cleanup_pipeline(self, pipeline_id: int, run_id: int | None = None) -> None:
        """Remove dlt working directory for a pipeline run."""
        if not self._pipelines_dir:
            return
        name = f"pipeline_{pipeline_id}"
        if run_id is not None:
            name = f"{name}_run_{run_id}"
        pipeline_dir = os.path.join(self._pipelines_dir, name)
        if os.path.isdir(pipeline_dir):
            shutil.rmtree(pipeline_dir, ignore_errors=True)
            logger.info("Cleaned up dlt pipeline dir: %s", pipeline_dir)

    def execute(
        self,
        pipeline_id: int,
        source_type: str,
        source_config: dict,
        destination_type: str,
        destination_config: dict,
        dlt_config: dict,
        batch_size: int | None = None,
        dataset_name: str | None = None,
        run_id: int | None = None,
    ) -> dict:
        """Execute a dlt pipeline.

        Extracts batch_size from dlt_config if not passed explicitly.
        Filters INTERNAL_CONFIG_KEYS before passing to pipeline.run().

        Returns {"rows_loaded": int, "load_info": LoadInfo}.
        """
        if batch_size is None:
            batch_size = dlt_config.get("batch_size", DEFAULT_BATCH_SIZE)

        pipeline = self.build_pipeline(
            pipeline_id,
            destination_type,
            destination_config,
            dataset_name=dataset_name,
            run_id=run_id,
        )
        source = self.build_source(source_type, source_config, dlt_config, batch_size=batch_size)

        # Apply row-level filters. SQL sources in single_table mode (E10)
        # have already pushed filters to the DB via query_adapter_callback
        # in build_source — don't re-apply them in memory.
        filters_cfg = dlt_config.get("filters")
        pushed_down = source_type in self.SUPPORTED_SOURCE_TYPES and (
            dlt_config.get("mode", "full_database") == "single_table"
        )
        if filters_cfg and not pushed_down:
            for f in filters_cfg:
                filter_fn = FILTER_OPS[f["op"]](f["column"], f["value"])
                source.add_filter(filter_fn)

        # Apply per-table merge hints for full_database mode
        merge_config = dlt_config.get("merge_config")
        if merge_config and dlt_config.get("mode", "full_database") == "full_database":
            for resource_name, resource in source.resources.items():
                if resource_name in merge_config:
                    pk = merge_config[resource_name]["primary_key"]
                    resource.apply_hints(write_disposition="merge", primary_key=pk)
                else:
                    resource.apply_hints(write_disposition="append")

        run_kwargs = {k: v for k, v in dlt_config.items() if k not in INTERNAL_CONFIG_KEYS}
        # When merge_config is used, write_disposition/primary_key are applied per-resource
        if merge_config:
            run_kwargs.pop("write_disposition", None)
            run_kwargs.pop("primary_key", None)
        load_info = pipeline.run(source, **run_kwargs)
        rows_loaded = _extract_rows_loaded(pipeline)

        return {
            "rows_loaded": rows_loaded,
            "load_info": load_info,
        }
