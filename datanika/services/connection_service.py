"""Connection management service — CRUD with encrypted credentials."""

import base64
import json
import logging
import re
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import NamedTuple
from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session

from datanika.config import settings
from datanika.errors import UserFacingError
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.services.egress_guard import build_guarded_session, validate_egress_host
from datanika.services.encryption import EncryptionService
from datanika.services.naming import validate_name

logger = logging.getLogger(__name__)

validate_connection_name = partial(validate_name, entity_label="Connection")


class ConnectionVerdict(NamedTuple):
    """What Test Connection concluded, in a form the UI can translate.

    ``ok`` keeps the three-valued shape ``_test_saas_source`` established and
    ``ConnectionState`` already renders: ``True`` green, ``False`` red, ``None``
    **neutral — never green**. Its docstring states the rule this type carries
    to every branch: *failure and "not tested" are different answers and neither
    may be rendered as the other.*

    ``message`` is English, for the API, the logs and any caller with no locale.

    ``reason`` is a machine-readable slug, **not an i18n key**, and the
    distinction is the layering rule this codebase already states —
    ``BaseState._translated``: *"Services raise plain English — they have no
    locale and no business having one."* The UI owns the reason → key mapping
    (`connection_state._VERDICT_KEYS`), which also keeps the key literals inside
    the tree `tests/test_i18n` scans, so a key with no reader is still caught.

    ``arg`` is the single interpolation value — a path, or a connector name. One
    scalar rather than a dict deliberately: a bare ``dict`` on a public Reflex
    state var is the shape core#972 was.

    A branch with no ``reason`` renders ``message`` as-is, which is exactly
    today's behaviour, so this is additive rather than a rewrite of every
    connector's verdict.
    """

    ok: bool | None
    message: str
    reason: str = ""
    arg: str = ""


# Connector types that exist but are no longer offered.
#
# A withdrawn type is absent from `SOURCE_TYPES`, `CONFIG_SCHEMAS` and the
# connections picker, so it cannot be created — but keeps its `ConnectionType`
# member, its SaaS *classification*, and its loader dispatch, so a connection
# someone already stored still resolves and still fails with an error that
# explains itself rather than "Unsupported source type".
#
# This is a named concept rather than "whichever lists we remembered to edit"
# because the first attempt at core#555 removed it from the dispatch set too and
# gave existing rows a worse error, and because withdrawal is not rare: it is
# the honest answer whenever a connector cannot work with the credentials we
# collect. `tests/test_connector_type_contracts.py` pins both halves.
# `google_ads` was withdrawn here and is back (core#555): the developer token is
# a string the user pastes, exactly like a service account JSON, so the gap was a
# missing form field rather than an external gate.
#
# `s3` is withdrawn as of core#863, and it is a DIFFERENT kind of gap — not a
# missing form field and not a credential gate. The transport is simply absent:
# s3fs left `uv.lock` when the dbt upgrade dropped dlt's `clickhouse` extra, so
# `fsspec.get_filesystem_class("s3")` raises `ImportError: Install s3fs to access
# S3`. Measured, not inferred.
#
# ⚠️ `gs://` and `az://` still resolve in fsspec, but do NOT turn that into
# user-facing copy suggesting an alternative: there is no `gcs` or `azure`
# ConnectionType — checked, 37 members, none of them an object store other than
# this one. "Use GCS instead" would name a connector that does not exist, which
# is a worse falsehood than the one being fixed here.
#
# ⚠️ TEMPORARY, and nobody has to remember to check. `TestDeferredCapability` in
# `tests/test_security/test_dependency_advisories.py` goes red the day `uv.lock`
# carries `google-cloud-storage >= 3.7` — the point at which dbt-bigquery's
# `google-cloud-storage>=2.4,<3.2` ceiling has moved far enough that s3fs can
# resolve again. It asserts the EXTERNAL condition on purpose: an assertion that
# "s3fs is absent" would only fail once somebody had already done the work.
#
# ⚠️ To restore: put `s3` back into SOURCE_TYPES, CONFIG_SCHEMAS and PICKER_TYPES
# and re-enable the four `requires_s3fs` tests. Those must pass **unmodified**
# against MinIO — they are the only executable evidence the transport ever
# worked, they cost four months to obtain, and hiding the picker entry is not a
# reason to drop them.
WITHDRAWN_SOURCE_TYPES: set[str] = {"s3"}

# Types that can serve as sources (databases + files + rest_api + sheets)
SOURCE_TYPES = {
    "postgres",
    "mysql",
    "mssql",
    "oracle",
    "sqlite",
    "rest_api",
    # `s3` withdrawn — core#863; see WITHDRAWN_SOURCE_TYPES above.
    "csv",
    "json",
    "parquet",
    "google_sheets",
    "mongodb",
    "clickhouse",
    "duckdb",
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
    "kafka",
    "openapi",
}

#: Types that can serve as destinations — i.e. that dlt can LOAD into.
#:
#: 🚨 ``mysql`` and ``sqlite`` were removed in core#862. They had been advertised
#: here for the life of the constant and **neither has ever loaded a row**:
#: ``DltRunner.build_destination`` is an unconditional
#: ``getattr(dlt.destinations, connection_type)`` and dlt has no attribute for
#: either, so every such upload died with ``AttributeError`` before a socket was
#: opened (core#865, measured across all eleven — 9 resolved).
#:
#: Both remain fully supported **extract sources**, which is a different layer:
#: extraction goes through SQLAlchemy and is verified against a real MySQL 8.4
#: container moving real rows (``test_mysql_after_dbt_mysql_removal.py``).
#: ⚠️ Do not "restore" a member here from any other list without first checking
#: ``hasattr(dlt.destinations, x)``. This set feeds ``infer_direction``, so a
#: wrong member also mislabels the connector's direction everywhere it renders.
DESTINATION_TYPES = {
    "postgres",
    "mssql",
    "bigquery",
    "snowflake",
    "redshift",
    "clickhouse",
    "duckdb",
    "databricks",
    "synapse",
}

#: Destinations that dbt can also TRANSFORM in — i.e. that have a dbt adapter.
#: A strict subset of ``DESTINATION_TYPES`` since core#825.
#:
#: 🚨 Loading data into a destination and running dbt models against it are
#: DIFFERENT capabilities with different requirements. dlt needs a driver; dbt
#: needs an installed **adapter**. Until core#825 these two sets were textually
#: identical — the same eleven strings — which is why nothing had ever diverged
#: and why nothing bound them. ``databricks`` and ``synapse`` are the live
#: example: dlt loads into both, dbt transforms in neither.
#:
#: ⚠️ ``mysql`` was that example until core#862 and is no longer in EITHER set.
#: Do not read the old wording ("dlt loads into MySQL through SQLAlchemy") as a
#: reason to restore it to ``DESTINATION_TYPES``: dlt can indeed write to MySQL
#: via its ``sqlalchemy`` destination, but ``build_destination`` resolves by
#: NAME — ``getattr(dlt.destinations, connection_type)`` — and there is no
#: ``mysql`` attribute, so it raised and never loaded a row. That sentence was
#: true of dlt and false of us, which is the most expensive kind of comment.
#:
#: ⚠️ Offering a transform destination dbt cannot build in is not cosmetic:
#: ``generate_profiles_yml`` raises **after** ``run.before_execute`` has fired
#: and after ``start_run``, so a run structurally incapable of succeeding has
#: already consumed the tenant's quota.
#:
#: ✅ **core#862 removed the last three wrong members.** ``sqlite``,
#: ``databricks`` and ``synapse`` sat in ``SUPPORTED_ADAPTERS`` with **no adapter
#: installed**, so they survived the intersection and were still offered; the
#: set is now 7 and every member is importable. Measured with
#: ``importlib.util.find_spec`` — before the fix, 7 of the 10 listed adapters
#: resolved, which is the negative control for the guard that now enforces it.
#:
#: ⚠️ **The refusal is server-side, in ``PipelineService`` and
#: ``TransformationService``, not here.** A picker filters what the browser
#: renders; ``POST /api/v1/pipelines`` never sees it. And the failure this
#: prevents is expensive rather than merely ugly, which is why it cannot wait
#: for run time: ``generate_profiles_yml`` raises *after* ``run.before_execute``
#: has fired and after ``start_run``, so the quota is already spent on a run
#: that was structurally incapable of succeeding.
#:
#: Written longhand rather than computed, following ``SQL_SOURCE_TYPES`` in
#: ``tests/test_connector_type_contracts.py``: adding a connector should force a
#: deliberate choice instead of inheriting one. Computing it here would also drag
#: ``dbt.cli.main`` into every module that touches connections.
#: ``test_connector_type_contracts.py`` asserts this equals
#: ``DESTINATION_TYPES & SUPPORTED_ADAPTERS``, so the longhand cannot rot.
TRANSFORM_DESTINATION_TYPES = {
    "postgres",
    "mssql",
    "bigquery",
    "snowflake",
    "redshift",
    "clickhouse",
    "duckdb",
}


def infer_direction(connection_type: str | ConnectionType) -> ConnectionDirection:
    """Derive direction from connection type."""
    ct = connection_type.value if isinstance(connection_type, ConnectionType) else connection_type
    is_src = ct in SOURCE_TYPES
    is_dst = ct in DESTINATION_TYPES
    if is_src and is_dst:
        return ConnectionDirection.BOTH
    if is_dst:
        return ConnectionDirection.DESTINATION
    return ConnectionDirection.SOURCE


# File-backed sources. Still non-SQL, but they *are* testable: the location can
# be listed. Kept as a subset of _NON_DB_TYPES so query/list_tables behaviour is
# unchanged — only `test_connection` treats them specially (core#493).
_FILE_TYPES = {
    ConnectionType.S3,
    ConnectionType.CSV,
    ConnectionType.JSON,
    ConnectionType.PARQUET,
}

#: SQL connection types whose "database" is a **local file**, not a network
#: service (core#978, core#979 / SPEC_LOCAL_FILE_CONNECTIONS).
#:
#: Derived from ``_build_sa_url``: exactly the two branches that read
#: ``config["path"]`` and emit ``<dialect>:///<path>`` with no host, no port and
#: no user. Everything that follows — no network timeout, a read-only open, and
#: a vocabulary with no *credentials* or *network* in it — follows from that one
#: property rather than from a list of connector names.
_LOCAL_FILE_DB_TYPES = {ConnectionType.SQLITE, ConnectionType.DUCKDB}

#: Values of ``path`` that name no file at all. An in-memory database is created
#: fresh on every connect by definition, so "does it already exist?" is not a
#: question about it and read-only is not a mode it has — measured: duckdb
#: refuses ``:memory:`` with ``read_only=True`` outright.
_IN_MEMORY_PATHS = {":memory:", ""}

#: URL schemes that name something **outside this container**, so the value
#: means the same thing in the web process and in the Celery worker.
#:
#: ⚠️ **The check below fails CLOSED**: anything that is not one of these is
#: treated as local. An unknown-but-remote scheme is therefore refused, which is
#: a visible, reportable wrong answer — while an unknown-but-local scheme let
#: through is a silent tenancy hole on a shared box. Those two errors are not
#: symmetric and the guard is aimed accordingly.
_REMOTE_URL_SCHEMES = frozenset(
    {"s3", "s3a", "gs", "gcs", "az", "azure", "abfs", "abfss", "adl", "http", "https", "memory"}
)

#: Connection types whose config carries a filesystem **location** the platform
#: has to have an opinion about (D4). Derived, not remembered: the two local-file
#: SQL dialects plus the three types whose ``bucket_url``/``path`` is handed to
#: dlt's ``filesystem()``.
#:
#: ``s3`` is excluded deliberately — its ``bucket_url`` is remote, so it means
#: the same thing in both containers, and it is the case D4's remote-scheme
#: carve-out exists for.
_LOCAL_PATH_TYPES = _LOCAL_FILE_DB_TYPES | {
    ConnectionType.CSV,
    ConnectionType.JSON,
    ConnectionType.PARQUET,
}


class LocalPathNotAllowedError(UserFacingError):
    """A connection would store a local path on a deployment that forbids them.

    A ``ValueError`` on purpose: ``BaseState._set_error`` and ``_safe_error``
    both surface a ``ValueError``'s message verbatim and everything else as a
    generic toast, so the refusal reaches the user through machinery that
    already exists rather than through a new branch in every caller.

    ``reason`` follows ``ConnectionVerdict``'s split — the service has no locale,
    the UI owns the key.
    """

    reason = "local_path_not_allowed"


def is_local_filesystem_location(value: str) -> bool:
    """Does this ``path``/``bucket_url`` name a location on the local disk?"""
    if not value:
        return False
    text = value.strip()
    if text in _IN_MEMORY_PATHS:
        # Nothing is stored, so there is no filesystem location to have an
        # opinion about. Refusing it would block a harmless configuration and
        # tell the user to upload a file that has nothing to do with it.
        return False
    scheme, sep, _rest = text.partition("://")
    # Local == not one of the known-remote schemes: a bare path, a `file://`
    # URL, or a Windows drive letter (`D:\data`, which `urlparse` would read as
    # a one-character scheme).
    return not (sep and scheme.lower() in _REMOTE_URL_SCHEMES)


def assert_local_paths_allowed(config: dict, connection_type: ConnectionType) -> None:
    """Refuse a local path at **save** time when the deployment forbids one (D4).

    🚨 **At save, not at test.** The run reads the stored config and Test
    Connection is optional, so a test-time refusal stops nobody.

    🚨 **On writes only.** Existing rows may already hold local paths —
    production connection id=14 does — and they must keep loading and listing.
    Nothing here is reachable from a read path; the callers are
    ``create_connection`` and ``update_connection``.
    """
    if settings.datanika_allow_local_file_paths:
        return
    if connection_type not in _LOCAL_PATH_TYPES:
        return
    for key in ("bucket_url", "path", "database"):
        value = (config or {}).get(key)
        if isinstance(value, str) and is_local_filesystem_location(value):
            raise LocalPathNotAllowedError(
                "Local file paths aren't available on this deployment. Upload the file "
                "instead, or point this connection at a database."
            )


#: The kwarg each DBAPI accepts for a **bounded connect**, where the default is
#: ``connect_timeout``.
#:
#: 🚨 This is a *network* parameter and it is derived from the dialect, not
#: accumulated as carve-outs (SPEC_LOCAL_FILE_CONNECTIONS D3). It was a chain of
#: ``elif``s with two exceptions already in it, and duckdb was the missing third
#: case: ``duckdb.connect()`` accepts only ``(database, read_only, config)`` and
#: raises ``TypeError: connect(): incompatible function arguments`` on
#: ``connect_timeout``. The generic ``except`` caught it and told the user to
#: *check your credentials and network settings* — for a local file, on the one
#: onboarding path we advertise as needing neither.
_CONNECT_TIMEOUT_KWARG = {
    ConnectionType.MSSQL: "login_timeout",  # pymssql
    ConnectionType.ORACLE: "tcp_connect_timeout",  # oracledb
}


def _connect_args(connection_type: ConnectionType, config: dict) -> dict:
    """Connect-args for one dialect, derived from what the dialect *is*.

    Two rules, and each answers a different question:

    * **Is there a network to time out?** A local-file database opens a file
      descriptor, so a connect timeout is meaningless and — for duckdb —
      fatal.
    * **Is this a read?** Test Connection must be incapable of writing (D1), so
      a local-file open is read-only wherever read-only is a mode that exists.
    """
    if connection_type in _LOCAL_FILE_DB_TYPES:
        path = str(config.get("path") or config.get("database") or ":memory:")
        if path in _IN_MEMORY_PATHS:
            return {}
        if connection_type == ConnectionType.DUCKDB:
            return {"read_only": True}
        # sqlite carries read-only in the URL, not in connect_args — see
        # ``_build_sa_url``.
        return {}
    return {_CONNECT_TIMEOUT_KWARG.get(connection_type, "connect_timeout"): 5}


# Connection types that don't support SQL queries (SELECT 1 testing or execute_query)
_NON_DB_TYPES = {
    ConnectionType.S3,
    ConnectionType.CSV,
    ConnectionType.JSON,
    ConnectionType.PARQUET,
    ConnectionType.REST_API,
    ConnectionType.GOOGLE_SHEETS,
    ConnectionType.MONGODB,
    ConnectionType.STRIPE,
    ConnectionType.GITHUB,
    ConnectionType.HUBSPOT,
    ConnectionType.SALESFORCE,
    ConnectionType.SHOPIFY,
    ConnectionType.JIRA,
    ConnectionType.SLACK,
    ConnectionType.GOOGLE_ANALYTICS,
    ConnectionType.GOOGLE_ADS,
    ConnectionType.FACEBOOK_ADS,
    ConnectionType.ZENDESK,
    ConnectionType.AIRTABLE,
    ConnectionType.NOTION,
    ConnectionType.PIPEDRIVE,
    ConnectionType.FRESHDESK,
    ConnectionType.ASANA,
    ConnectionType.KAFKA,
    ConnectionType.OPENAPI,
}


# --------------------------------------------------------------------------
# SaaS credential probes (core#821)
# --------------------------------------------------------------------------
#
# `test_connection` used to answer `(True, "Test not applicable for this type")`
# for every type below after making **no network call at all**, and `True` is
# what drives the green styling. Product proved it on production with a
# fabricated token against a store that does not exist. Two credentials on disk
# — a Pipedrive key returning 401 and a Freshdesk key returning 403
# `account_suspended` — would both also have gone green.
#
# That set is exactly the connectors whose credentials expire, get revoked or
# get mistyped, and this button is the only pre-run validation the product
# offers. Its first real credential check was the first pipeline run, which for
# a scheduled upload is hours later and arrives as a failed run rather than a
# form error.
#
# 🚨 **Several of these APIs report failure with HTTP 200.** Slack answers
# `200 {"ok": false, "error": "invalid_auth"}`; Pipedrive carries a `success`
# flag. A probe that reads only `status_code` calls those a pass — which is the
# reported bug rebuilt inside its own fix. Hence `ok:`, a per-vendor body
# predicate, and a test that exercises both the false and the true case.
#
# Probes deliberately hit each vendor's cheapest *identity* endpoint rather than
# a data endpoint: it is the credential we are testing, the response is small,
# and it needs no scope beyond the one a token must already have.

#: Kept in step with `dlt_runner.DEFAULT_FACEBOOK_API_VERSION` by
#: `test_saas_connection_probe.py`; duplicated rather than imported because
#: importing `dlt_runner` pulls dlt into every UI import.
_FACEBOOK_API_VERSION = "v21.0"


def _first(config: dict, *names: str) -> str:
    """First non-empty value among ``names``, as a stripped string."""
    for name in names:
        value = config.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value not in (None, "", {}, []):
            return str(value)
    return ""


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _basic(user: str, password: str) -> dict:
    import base64

    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()}


def _flag_check(flag: str):
    """Body predicate for vendors that report failure inside an HTTP 200.

    Returns ``(ok, detail)``. A response that is not JSON at all counts as a
    failure: these endpoints always answer JSON, so HTML means a captive portal,
    a proxy, or the wrong host — none of which is a working credential.
    """

    def check(response):
        try:
            body = response.json()
        except ValueError:
            return False, "the response was not JSON"
        if body.get(flag) is True:
            return True, ""
        return False, str(body.get("error") or body.get("message") or body)

    return check


#: type value -> probe spec.
#:
#: ``fields`` is a tuple of *alternative-name groups*; every group must yield a
#: value or the probe is skipped and the missing one named. The names match what
#: ``dlt_runner._build_saas_source`` reads, so a connection that tests is a
#: connection that can run.
SAAS_PROBES: dict[str, dict] = {
    "stripe": {
        "fields": (("api_key", "stripe_secret_key"),),
        "url": lambda c: "https://api.stripe.com/v1/customers",
        "headers": lambda c: _bearer(_first(c, "api_key", "stripe_secret_key")),
        "params": lambda c: {"limit": 1},
    },
    "github": {
        "fields": (("access_token", "api_key"),),
        "url": lambda c: "https://api.github.com/user",
        "headers": lambda c: {
            **_bearer(_first(c, "access_token", "api_key")),
            "Accept": "application/vnd.github+json",
        },
    },
    "hubspot": {
        "fields": (("api_key", "access_token"),),
        "url": lambda c: "https://api.hubapi.com/crm/v3/objects/contacts",
        "headers": lambda c: _bearer(_first(c, "api_key", "access_token")),
        "params": lambda c: {"limit": 1},
    },
    "salesforce": {
        "fields": (("access_token", "api_key"), ("instance_url",)),
        "url": lambda c: _first(c, "instance_url").rstrip("/") + "/services/data/v59.0/limits",
        "headers": lambda c: _bearer(_first(c, "access_token", "api_key")),
    },
    "shopify": {
        "fields": (("api_key", "access_token"), ("store",)),
        "url": lambda c: f"https://{_first(c, 'store')}.myshopify.com/admin/api/2024-01/shop.json",
        "headers": lambda c: {"X-Shopify-Access-Token": _first(c, "api_key", "access_token")},
    },
    "jira": {
        "fields": (("api_key", "api_token"), ("domain",)),
        "url": lambda c: f"https://{_first(c, 'domain')}.atlassian.net/rest/api/3/myself",
        "headers": lambda c: _basic(_first(c, "email"), _first(c, "api_key", "api_token")),
    },
    "slack": {
        "fields": (("api_key", "bot_token"),),
        "url": lambda c: "https://slack.com/api/auth.test",
        "headers": lambda c: _bearer(_first(c, "api_key", "bot_token")),
        # 200 + {"ok": false} is Slack's normal way of saying "bad token".
        "ok": _flag_check("ok"),
    },
    "facebook_ads": {
        "fields": (("access_token", "api_key"),),
        "url": lambda c: (
            f"https://graph.facebook.com/{_first(c, 'api_version') or _FACEBOOK_API_VERSION}/me"
        ),
        "headers": lambda c: _bearer(_first(c, "access_token", "api_key")),
    },
    "zendesk": {
        "fields": (("api_key", "api_token"), ("subdomain", "domain")),
        "url": lambda c: (
            f"https://{_first(c, 'subdomain', 'domain')}.zendesk.com/api/v2/users/me.json"
        ),
        "headers": lambda c: _bearer(_first(c, "api_key", "api_token")),
    },
    "airtable": {
        "fields": (("api_key", "access_token"),),
        "url": lambda c: "https://api.airtable.com/v0/meta/whoami",
        "headers": lambda c: _bearer(_first(c, "api_key", "access_token")),
    },
    "notion": {
        "fields": (("api_key", "access_token"),),
        "url": lambda c: "https://api.notion.com/v1/users/me",
        "headers": lambda c: {
            **_bearer(_first(c, "api_key", "access_token")),
            # Notion answers 400 without it, which would read as a bad token.
            "Notion-Version": "2022-06-28",
        },
    },
    "pipedrive": {
        "fields": (("api_key", "api_token"),),
        "url": lambda c: "https://api.pipedrive.com/v1/users/me",
        "params": lambda c: {"api_token": _first(c, "api_key", "api_token")},
        "ok": _flag_check("success"),
    },
    "freshdesk": {
        "fields": (("api_key",), ("domain",)),
        "url": lambda c: f"https://{_first(c, 'domain')}.freshdesk.com/api/v2/agents/me",
        # Freshdesk takes the API key as the basic-auth *username*.
        "headers": lambda c: _basic(_first(c, "api_key"), "X"),
    },
    "asana": {
        "fields": (("api_key", "access_token"),),
        "url": lambda c: "https://app.asana.com/api/1.0/users/me",
        "headers": lambda c: _bearer(_first(c, "api_key", "access_token")),
    },
}

#: Types that get an explicit **"not tested"** verdict rather than a probe.
#:
#: Not green, and not red either — a neutral third state, because claiming a
#: failure for a connection that may be perfectly good is the same kind of lie
#: as claiming success. ``test_connection`` returns ``None`` for these.
SAAS_PROBE_EXEMPT: dict[str, str] = {
    "rest_api": (
        "Not tested: a REST API connection is a base URL plus arbitrary auth, "
        "and the resource paths live on the upload rather than the connection — "
        "there is no endpoint we know is safe to call. The first run reports."
    ),
    "openapi": (
        "Not tested: authentication and the resource catalog come from the "
        "imported spec, and calling an arbitrary catalog entry may have side "
        "effects. The first run reports."
    ),
    "google_sheets": (
        "Not tested: the credential is a service-account JSON, and verifying it "
        "means minting an OAuth token — which belongs with the Google helpers, "
        "not in this service. Sharing the sheet with the service account is the "
        "step that usually fails, and it is per-upload, not per-connection."
    ),
    "google_analytics": (
        "Not tested: the credential is a service-account JSON that has to be "
        "exchanged for an OAuth token before anything can be called, and the "
        "property grant is separate from the key being valid."
    ),
    "google_ads": (
        "Not tested: authentication needs a developer token plus an OAuth "
        "refresh exchange, so a probe here would duplicate the token-minting "
        "code that lives with the Google Ads source."
    ),
    "kafka": (
        "Not tested: Kafka is not HTTP. A metadata fetch needs a broker "
        "connection with its own timeout and SASL negotiation, which cannot "
        "share the guarded HTTP session the other probes use."
    ),
}


def _saas_probe_url(connection_type, config: dict) -> str:
    """The URL a type's probe would call. Separate so tests can redirect it."""
    return SAAS_PROBES[connection_type.value]["url"](config)


# --------------------------------------------------------------------------
# Why a connection test failed — said out loud, with the secrets taken out
# --------------------------------------------------------------------------
#
# Two defects filed a day apart turned out to be one class (core#608, core#625):
# a failed Test Connection tells the user nothing about *why*. #608 reached them
# as Reflex's "An error occurred. Contact the website administrator." because an
# exception escaped; #625 reached them as "Connection failed — check your
# credentials and network settings" because `except Exception` caught the cause
# and threw it away. In #625 the credentials were correct and the advice was
# actively wrong: the driver had said "Authentication failed against database X",
# which is the one sentence that would have resolved it.
#
# So the fix for #608 could not be another fixed string, or it would have been a
# fresh instance of the thing it was closing.
#
# The reason this was not simply done in the first place is real: driver
# exceptions quote the connection URI, and the URI contains the password. That
# is what `_redact_secrets` is for, and why it fails **closed**.

#: Config keys whose values must never appear in a message shown to a user.
#: Superset of what any one connector stores — a key absent from a config costs
#: nothing here, and a key missing from this set is a credential disclosure.
SECRET_CONFIG_KEYS = frozenset(
    {
        "password",
        "token",
        "api_key",
        "access_token",
        "refresh_token",
        "client_secret",
        "developer_token",
        # ⚠️ Keep, even though `s3` left CONFIG_SCHEMAS in core#863. This set is
        # documented as a *superset* of schema password fields, so an entry with
        # no schema behind it now reads like tidyable dead weight. It is not:
        # connections already stored still hold one, and only ONE direction is
        # asserted (schema field -> this set), so removing it breaks nothing in
        # CI while making `BackupService.export_backup` write a live AWS secret
        # into a backup in clear text.
        "aws_secret_access_key",
        "keyfile_json",
        "service_account_json",
        "credentials",
        "secret",
        # Added #651. Every one of these is already marked `format: password`
        # in CONFIG_SCHEMAS and was absent here, so its value could be quoted
        # back verbatim in a connection-test error. The link that would have
        # caught it now exists: tests/test_services/test_secret_key_coverage.py
        # asserts this set is a superset of every sensitive schema field.
        "api_token",
        "auth_password",
        "auth_token",
        "aws_access_key_id",
        "security_token",
        # Added core#1054 with the Kafka SASL fields. A broker password reaches
        # user-visible prose the same way a database one does: kafka-python's
        # bootstrap errors quote the client config, so this is not a formality.
        "sasl_plain_password",
    }
)

#: A driver's reason is one line of context, not a stack trace.
_MAX_REASON_CHARS = 300

#: Below this length a secret is too short to remove from prose without
#: shredding it — "a" would turn "authentication failed" into "***uthentic…".
_MIN_REDACTABLE_SECRET = 4


def _credentials_json_text(value) -> str | None:
    """The exact JSON text a credentials blob is encoded from, or ``None``.

    Accepts both shapes ``dlt_runner`` accepts: the string the connection form
    writes, and a dict. The redactor below must know the same normalisation,
    or a spelling exists that it cannot mask.
    """
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        return json.dumps(value)
    return None


def _secret_spellings(value: str) -> tuple[str, ...]:
    """Every rendering of a secret that can reach a user-visible message.

    Drivers quote the URI we built, we percent-encode the userinfo when
    building it, and since core#869 the BigQuery catalog URL carries the whole
    service-account key base64-encoded. A base64 payload matches neither of the
    first two spellings, so without this an entire private key could be quoted
    back verbatim in a connection-test error.
    """
    encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return (value, quote_plus(value), encoded, quote_plus(encoded))


def _redact_secrets(text: str, config: dict) -> str | None:
    """Strip every secret in `config` out of `text`, or refuse.

    Returns ``None`` when the text cannot be shown safely — a secret too short
    to substring-replace cleanly. **Fails closed on purpose**: the caller then
    falls back to the generic message, so the worst case is the old behaviour
    rather than a password on screen.
    """
    for key in SECRET_CONFIG_KEYS:
        value = config.get(key)
        if isinstance(value, dict):
            # Additive: dicts were skipped entirely before, so this cannot make
            # an existing message less redacted, and it never trips the
            # fail-closed rule below.
            for form in _secret_spellings(json.dumps(value)):
                text = text.replace(form, "***")
            continue
        if not isinstance(value, str) or not value:
            continue
        if len(value) < _MIN_REDACTABLE_SECRET:
            return None
        for form in _secret_spellings(value):
            text = text.replace(form, "***")
    return text


def _failure_reason(exc: Exception, config: dict) -> str:
    """The driver's own explanation, redacted and trimmed — or ``""``."""
    reason = _redact_secrets(str(exc), config)
    if reason is None:
        return ""
    reason = " ".join(reason.split())
    if len(reason) > _MAX_REASON_CHARS:
        reason = reason[:_MAX_REASON_CHARS].rstrip() + "…"
    return reason


def describe_connection_failure(exc: Exception, config: dict, fallback: str) -> str:
    """`fallback`, plus what the driver actually said, when it can be shown."""
    reason = _failure_reason(exc, config)
    return f"{fallback}: {reason}" if reason else fallback


def _build_sa_url(config: dict, connection_type: ConnectionType, *, read_only: bool = False) -> str:
    """Build a SQLAlchemy connection URL from config dict and connection type.

    ``read_only`` is honoured only where the dialect carries the mode in the URL
    rather than in connect-args — sqlite today. It defaults to ``False`` so
    every existing caller (the loaders, ``execute_query``, the API) keeps a
    writable URL; **only Test Connection asks for a read** (D1).
    """
    if connection_type in (ConnectionType.POSTGRES, ConnectionType.REDSHIFT):
        driver = "postgresql+psycopg2"
        port = config.get("port", 5432 if connection_type == ConnectionType.POSTGRES else 5439)
        return (
            f"{driver}://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.MYSQL:
        port = config.get("port", 3306)
        return (
            f"mysql+pymysql://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.MSSQL:
        port = config.get("port", 1433)
        return (
            f"mssql+pymssql://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.SQLITE:
        path = config.get("path", ":memory:")
        if read_only and path not in _IN_MEMORY_PATHS:
            # SQLite has no read-only *connect arg*; the mode travels in a URI
            # filename, which needs `uri=true` for the driver to parse it at
            # all. Measured: with this, a path that does not exist fails and
            # **no file is created**; without it, SQLite's open-or-create
            # semantics manufacture the very database the check then reports
            # finding (core#979).
            return f"sqlite:///file:{path}?mode=ro&uri=true"
        return f"sqlite:///{path}"

    if connection_type == ConnectionType.SNOWFLAKE:
        url = (
            f"snowflake://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('account', '')}"
            f"/{config.get('database', '')}"
            f"/{config.get('schema', '')}"
        )
        params = []
        if config.get("warehouse"):
            params.append(f"warehouse={quote_plus(config['warehouse'])}")
        if config.get("role"):
            params.append(f"role={quote_plus(config['role'])}")
        if params:
            url += "?" + "&".join(params)
        return url

    if connection_type == ConnectionType.BIGQUERY:
        project = config.get("project", "")
        dataset = config.get("dataset", "")
        url = f"bigquery://{project}/{dataset}"
        # Without this the dialect falls back to Application Default
        # Credentials, which do not exist in the container: the *load* still
        # succeeds (dlt gets the key straight from the connection config), so
        # rows land in BigQuery and then never appear under Models/Catalog
        # (core#869). Every other credentialed branch embeds its secret.
        keyfile = _credentials_json_text(
            config.get("keyfile_json") or config.get("service_account_json")
        )
        if keyfile:
            # `quote_plus` is load-bearing: base64 contains `+`, which a query
            # string decodes back as a space. Unencoded, the payload is
            # corrupted for exactly those keys whose encoding contains `+` --
            # working for some service accounts and not others.
            encoded = base64.b64encode(keyfile.encode("utf-8")).decode("ascii")
            url += f"?credentials_base64={quote_plus(encoded)}"
        return url

    if connection_type == ConnectionType.DATABRICKS:
        host = config.get("host", "")
        http_path = config.get("http_path", "")
        token = config.get("token", config.get("password", ""))
        catalog = config.get("catalog", config.get("database", ""))
        return (
            f"databricks://token:{quote_plus(token)}@{host}"
            f"?http_path={quote_plus(http_path)}&catalog={quote_plus(catalog)}"
        )

    if connection_type == ConnectionType.SYNAPSE:
        port = config.get("port", 1433)
        return (
            f"mssql+pymssql://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.DUCKDB:
        path = config.get("path", config.get("database", ":memory:"))
        return f"duckdb:///{path}"

    if connection_type == ConnectionType.CLICKHOUSE:
        port = config.get("port", 8123)
        secure_qs = "?secure=1" if config.get("secure", False) else ""
        return (
            f"clickhousedb+connect://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}{secure_qs}"
        )

    if connection_type == ConnectionType.ORACLE:
        port = config.get("port", 1521)
        userinfo = (
            f"{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
        )
        if config.get("use_sid"):
            # Legacy SID connect (classic single-instance): the URL path is the SID.
            return f"oracle+oracledb://{userinfo}{config.get('database', '')}"
        # Default: service-name connect (PDB / RAC / Autonomous). SQLAlchemy's
        # oracledb dialect reads the service name from the ?service_name= query;
        # a value in the URL path is treated as a SID (#329).
        return f"oracle+oracledb://{userinfo}?service_name={quote_plus(config.get('database', ''))}"

    raise UserFacingError(f"Unsupported connection type for URL building: {connection_type}")


def get_org_connection(session: Session, org_id: int, conn_id: int) -> Connection | None:
    """Resolve a connection *within* an org — the single definition of ownership.

    Every read of a `Connection` goes through here. A bare
    `session.get(Connection, conn_id)` returns whichever tenant's row happens to
    carry that primary key, and connection ids are small sequential integers, so
    an id that arrived in a request body is another org's id until proven
    otherwise. `tests/test_security/test_tenant_fk_boundary.py` fails the build
    if a primary-key lookup reappears anywhere under `datanika/`.
    """
    stmt = select(Connection).where(
        Connection.id == conn_id,
        Connection.org_id == org_id,
        Connection.deleted_at.is_(None),
    )
    return session.execute(stmt).scalar_one_or_none()


class ConnectionService:
    def __init__(self, encryption: EncryptionService):
        self._encryption = encryption

    def create_connection(
        self,
        session: Session,
        org_id: int,
        name: str,
        connection_type: ConnectionType,
        config: dict,
        source_template_slug: str | None = None,
    ) -> Connection:
        from datanika.hooks import emit

        emit("connection.before_create", session=session, org_id=org_id)
        validate_connection_name(name)
        # SSRF pre-flight gate (core#338): reject connectors whose user-supplied
        # base_url resolves to a non-public host at create time. Only fires when
        # a base_url is present, so DB connectors and hardcoded-host SaaS
        # (stripe/github/…) are unaffected.
        base_url = (config or {}).get("base_url")
        if base_url:
            validate_egress_host(base_url)
        # D4 / core#969: a file-based destination turns whatever directory it
        # points at into a store of record, and nothing constrained where that
        # may be — the image (destroyed on rebuild), /tmp, a path that resolves
        # differently in the web tier and the worker. None of those fail loudly:
        # the connection saves, dlt writes, and the data is gone at the next
        # deploy with a `succeeded` run and a row count in the UI.
        assert_local_paths_allowed(config, connection_type)
        direction = infer_direction(connection_type)
        # Normalise "" → None so analytics queries can filter on a single
        # NULL check. ConnectionState.selected_template_slug defaults to ""
        # when a user creates a connection outside the template flow. #93.
        conn = Connection(
            org_id=org_id,
            name=name,
            connection_type=connection_type,
            direction=direction,
            config_encrypted=self._encryption.encrypt(config),
            source_template_slug=source_template_slug or None,
        )
        session.add(conn)
        session.flush()
        return conn

    def consume_template_first_run(self, session: Session, org_id: int, conn_id: int) -> str | None:
        """Return the template slug on the connection's first eligible run,
        then mark it so subsequent calls return None. Idempotent.

        Feeds the ``template_first_run_triggered`` Plausible event (#93).
        The caller (``PipelineState.run_pipeline`` / ``UploadState.run_upload``)
        emits an ``rx.call_script`` when this returns a slug, and nothing
        when it returns None — so a connection fires the funnel step 3
        event exactly once over its lifetime, regardless of how many
        runs it sees.
        """
        from datetime import UTC, datetime

        conn = self.get_connection(session, org_id, conn_id)
        if conn is None or not conn.source_template_slug:
            return None
        if conn.template_first_run_fired_at is not None:
            return None
        conn.template_first_run_fired_at = datetime.now(UTC)
        session.flush()
        return conn.source_template_slug

    def get_connection(self, session: Session, org_id: int, conn_id: int) -> Connection | None:
        return get_org_connection(session, org_id, conn_id)

    def get_connection_config(self, session: Session, org_id: int, conn_id: int) -> dict | None:
        conn = self.get_connection(session, org_id, conn_id)
        if conn is None:
            return None
        return self._encryption.decrypt(conn.config_encrypted)

    def list_connections(
        self, session: Session, org_id: int, *, include_deleted: bool = False
    ) -> list[Connection]:
        """List an org's connections.

        ``include_deleted`` exists for *display* only, and is off by default so
        no caller picks up retired rows by accident. It is what lets a screen
        say ``q3signupsexport (deleted)`` instead of the bare ``#31`` that
        core#805 was filed about: the name is still on the row, it was only the
        join that filtered it out. Never use it to resolve a connection for
        running anything — that is ``get_connection``, which filters
        ``deleted_at`` and must keep doing so.
        """
        # The org_id constraint stays literally inside the first `.where()`.
        # Building the conditions in a list first is equivalent SQL and made
        # `tests/test_security/test_tenant_fk_boundary.py` stop recognising this
        # query as org-scoped — the S1 guard from core#733 would have gone blind
        # here while the code stayed correct, which is worse than a red test.
        stmt = select(Connection).where(Connection.org_id == org_id)
        if not include_deleted:
            stmt = stmt.where(Connection.deleted_at.is_(None))
        return list(session.execute(stmt.order_by(Connection.created_at.desc())).scalars().all())

    @staticmethod
    def list_dependents(session: Session, org_id: int, conn_id: int) -> list[str]:
        """Names of the live objects that would break if ``conn_id`` were deleted.

        Deleting a connection is a pure soft delete with no cascade and no
        check, so nothing told the user that three uploads pointed at it
        (core#805). Returning *names* rather than a count is deliberate: the
        count answers "how many" and the names answer "which", and the user
        needs the second to decide.

        Reads live rows only — a dependent that is itself soft-deleted is not
        a reason to keep a connection.
        """
        from datanika.models.pipeline import Pipeline
        from datanika.models.transformation import Transformation
        from datanika.models.upload import Upload

        names: list[str] = []
        upload_stmt = select(Upload.name).where(
            Upload.org_id == org_id,
            Upload.deleted_at.is_(None),
            (Upload.source_connection_id == conn_id)
            | (Upload.destination_connection_id == conn_id),
        )
        names.extend(session.execute(upload_stmt).scalars().all())
        for model in (Pipeline, Transformation):
            stmt = select(model.name).where(
                model.org_id == org_id,
                model.deleted_at.is_(None),
                model.destination_connection_id == conn_id,
            )
            names.extend(session.execute(stmt).scalars().all())
        return names

    def update_connection(
        self, session: Session, org_id: int, conn_id: int, **kwargs
    ) -> Connection | None:
        conn = self.get_connection(session, org_id, conn_id)
        if conn is None:
            return None

        if "name" in kwargs:
            validate_connection_name(kwargs["name"])
            conn.name = kwargs["name"]
        if "connection_type" in kwargs:
            conn.connection_type = kwargs["connection_type"]
            conn.direction = infer_direction(kwargs["connection_type"])
        if "config" in kwargs:
            # The type may be changing in the same call, so validate against the
            # type this row will *have*, not the one it had. Reading `conn.
            # connection_type` after the assignment above is what makes that
            # true; taking it from `kwargs` alone would miss a config-only edit.
            assert_local_paths_allowed(kwargs["config"], conn.connection_type)
            conn.config_encrypted = self._encryption.encrypt(kwargs["config"])

        session.flush()
        return conn

    def delete_connection(self, session: Session, org_id: int, conn_id: int) -> bool:
        conn = self.get_connection(session, org_id, conn_id)
        if conn is None:
            return False
        conn.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    @staticmethod
    def execute_query(
        config: dict,
        connection_type: ConnectionType,
        query: str,
    ) -> tuple[list[str], list[list]]:
        """Execute a read-only SQL query. Returns (column_names, rows)."""
        if connection_type in _NON_DB_TYPES:
            raise UserFacingError(f"Cannot execute SQL on {connection_type.value} connections")
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                columns = list(result.keys())
                rows = [list(row) for row in result.fetchall()]
                return columns, rows
        finally:
            engine.dispose()

    @staticmethod
    def _test_mongodb(config: dict) -> tuple[bool, str]:
        """Test MongoDB connectivity via server_info(). Returns (success, message).

        The URI comes from `mongodb_source.build_connection_uri` — the same
        function the run path uses — rather than being assembled here (core#625).
        It *was* assembled here, and core#550's `authSource` fix landed only on
        the run path, so this reported failure for connections whose runs
        succeeded: `server_info()` authenticates for real, and without
        `authSource` the driver looked for the user inside the database being
        read instead of in `admin`.
        """
        try:
            from pymongo import MongoClient
        except ImportError:
            return False, "Driver not installed for mongodb"

        from datanika.services.mongodb_source import build_connection_uri

        uri = build_connection_uri(config)

        # core#626 D8. A DNS seed list adds a DNS round trip before any
        # connection is attempted, then TLS adds a handshake, against a cluster
        # that may be on another continent. On this box that budget is not
        # obviously safe: a dead provider resolver cost 7.9-9.5 s per lookup
        # until 2026-08-29, and a cold `api.paddle.com` lookup measured 20.1 s.
        #
        # Raised only for SRV, not unconditionally, so an ordinary unreachable
        # host still reports in five seconds. A timeout against a *working*
        # cluster surfaces as "connection failed — check your credentials",
        # which sends the user to re-check credentials that were always
        # correct — the worst failure mode a setup flow can have.
        timeout_ms = 10000 if config.get("srv") else 5000

        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
            client.server_info()
            client.close()
            return True, "Connected successfully"
        except Exception as exc:
            logger.warning("MongoDB connection test failed", exc_info=True)
            return False, describe_connection_failure(
                exc, config, "Connection failed — check your credentials and network settings"
            )

    @staticmethod
    def _test_file_source(config: dict, connection_type: ConnectionType) -> tuple[bool, str]:
        """Actually test a file source, instead of declaring the test N/A.

        These types previously fell into `_NON_DB_TYPES` and returned
        `(True, "Test not applicable for this type")` unconditionally — so a
        wrong path tested **exactly like a right one**, and the first signal
        anything was wrong was a green run with zero rows (core#493). Our own
        CSV guide warned *"a wrong path looks exactly like a right one here"*;
        that was a description of a gap, not a fact of life.

        ⚠️ **This used to claim the check and the loader "agree by construction"
        because they use the same lister. That is true of the glob semantics and
        false of the filesystem** (core#979 AC5, SPEC_LOCAL_FILE_CONNECTIONS D7).
        Test Connection runs in the **web** process; the extract runs in
        **celery**; the two share exactly two named volumes. For a *local*
        ``bucket_url`` the guarantee does not hold — same code, different
        container — and it is the kind of claim someone relies on while deciding
        not to check something. It holds for ``s3://`` and other remote schemes,
        where the URL means the same thing in both containers.
        """
        from dlt.sources.filesystem import filesystem

        from datanika.services.dlt_runner import (
            AWS_CREDENTIAL_KEYS,
            DEFAULT_FILE_GLOBS,
            describe_empty_file_match,
        )

        location = config.get("bucket_url") or config.get("path") or ""
        if not location:
            return False, "Set the bucket URL or path first — there is nothing to test yet"

        # The pipeline's own `file_glob` lives in per-upload config, not on the
        # connection, so the type's default is the best available stand-in.
        # Narrower globs can still match nothing; the run-time check covers that.
        file_glob = DEFAULT_FILE_GLOBS.get(connection_type.value, "*")

        kwargs = {"bucket_url": location, "file_glob": file_glob}
        if connection_type == ConnectionType.S3:
            credentials = {k: v for k, v in config.items() if k in AWS_CREDENTIAL_KEYS}
            if credentials:
                kwargs["credentials"] = credentials

        try:
            first = next(iter(filesystem(**kwargs)), None)
        except Exception as exc:
            logger.warning("File-source connection test failed for %s: %s", location, exc)
            # D2 / AC5: a **local** file source has no credentials. Telling its
            # user to check some is the same misdirection core#978 found on
            # duckdb — advice for a thing that does not exist, on the path we
            # advertise as needing nothing. `s3` keeps the credentials clause
            # because for `s3` it is the likeliest cause.
            if connection_type == ConnectionType.S3:
                return False, (
                    f"Could not read {location} — check the bucket URL, permissions and credentials"
                )
            return False, f"Could not read {location} — check the path and its permissions"

        if first is None:
            return False, describe_empty_file_match(location, file_glob)

        return True, f"Connected — found files matching {file_glob}"

    @staticmethod
    def _test_saas_source(config: dict, connection_type: ConnectionType) -> tuple[bool | None, str]:
        """Verify a SaaS credential with one cheap authenticated request (core#821).

        Returns the same ``(verdict, message)`` shape as every other branch,
        with a third verdict: ``None`` for "not tested". That third state is the
        point. This branch used to answer ``(True, "Test not applicable for this
        type")`` for twenty types without touching the network, and ``True`` is
        what paints the message green — so a revoked token, a suspended account
        and a token that was never real all read as working.

        Failure and "not tested" are different answers and neither may be
        rendered as the other: calling an unverifiable connection *failed* is
        the same class of lie, told in the opposite direction.
        """
        name = connection_type.value

        reason = SAAS_PROBE_EXEMPT.get(name)
        if reason is not None:
            return None, reason

        probe = SAAS_PROBES.get(name)
        if probe is None:
            # Unreachable while `test_no_saas_type_is_undecided` holds. If a new
            # type ever slips past it, "not tested" is the safe answer — the one
            # thing we must not do is go back to claiming success.
            return None, f"Not tested: no credential probe exists for {name}"

        missing = [group[0] for group in probe["fields"] if not _first(config, *group)]
        if missing:
            return False, f"Missing required field(s): {', '.join(missing)}"

        url = _saas_probe_url(connection_type, config)
        # Two layers, as in `dlt_runner._rest_api_from_parts`: several of these
        # URLs are built from free-form user input (`instance_url`, `store`,
        # `domain`, `subdomain`), so the host is checked before a request is
        # built and the guarded session re-checks every hop, redirects included.
        try:
            validate_egress_host(url)
        except Exception as exc:
            return False, str(exc)

        session = build_guarded_session()
        try:
            response = session.get(
                url,
                headers=probe.get("headers", lambda c: {})(config),
                params=probe.get("params", lambda c: {})(config),
                timeout=10,
            )
        except Exception as exc:
            logger.warning("Credential probe failed for %s", name, exc_info=True)
            return False, describe_connection_failure(
                exc, config, f"Could not reach the {name} API"
            )
        finally:
            session.close()

        if response.status_code in (401, 403):
            return False, f"{name} rejected these credentials (HTTP {response.status_code})"
        if response.status_code >= 400:
            return False, f"{name} responded HTTP {response.status_code}"

        # The 200-that-means-no case. Slack and Pipedrive both answer 200 with
        # a failure flag in the body, so a status-only check would call a dead
        # token healthy — the bug this method exists to remove, rebuilt inside
        # it.
        check = probe.get("ok")
        if check is not None:
            ok, detail = check(response)
            if not ok:
                return False, f"{name} rejected these credentials: {detail}"

        return True, "Credentials verified"

    @staticmethod
    def _test_local_file_db(
        config: dict, connection_type: ConnectionType
    ) -> ConnectionVerdict | None:
        """Answer for a local-file database, or ``None`` to fall through.

        Five outcomes, five sentences (D2), because they call for five different
        user actions — and none of them mentions credentials or a network, which
        a file on a disk does not have.

        🚨 **Two independent mechanisms, doing two different jobs.** Measured:

        * The **existence check** is what produces the right *sentence*. SQLite
          returns the identical ``unable to open database file`` for "there is
          nothing here" and "the directory is not readable", so the driver
          cannot distinguish them and the check has to.
        * The **read-only open** is what makes the check honest. Without it
          SQLite's open-or-create semantics create the database the check then
          reports finding, so the user's evidence that the path was right is the
          artifact the check just fabricated (core#979).

        Neither substitutes for the other: read-only alone gives the right
        boolean with the wrong message; the existence check alone still writes.
        """
        if connection_type not in _LOCAL_FILE_DB_TYPES:
            return None

        path = str(config.get("path") or config.get("database") or ":memory:")
        in_memory = path in _IN_MEMORY_PATHS

        if not in_memory and not Path(path).exists():
            return ConnectionVerdict(
                False,
                f"No database at '{path}'. Check the path, or create the file first.",
                reason="file_missing",
                arg=path,
            )

        try:
            url = _build_sa_url(config, connection_type, read_only=not in_memory)
        except ValueError as exc:
            return ConnectionVerdict(False, str(exc))

        # 🚨 **The in-memory case still connects, and that is the point.**
        # An earlier draft returned ``True`` for ``:memory:`` without opening
        # anything — a control that can only give one answer, which is the exact
        # shape SPEC_LOCAL_FILE_CONNECTIONS exists to stop. Opening it is a real
        # question with a real failure: the dialect has to resolve and the driver
        # has to be installed, and ``duckdb_engine`` missing from the image is a
        # thing that has happened here before (core#602). An existing test caught
        # the short-circuit by asserting ``create_engine`` was called at all.
        engine = None
        try:
            engine = create_engine(url, connect_args=_connect_args(connection_type, config))
            with engine.connect() as conn:
                # 🚨 **Not ``SELECT 1``.** Measured: SQLite opens *any* readable
                # file and answers ``SELECT 1`` from the driver without ever
                # reading a page, so a text file renamed ``.sqlite`` tested
                # green. The header is only validated when a page is actually
                # read — so the probe has to read the catalog, which is also the
                # question the user is asking: *is there a database here?*
                # ``inspect`` is dialect-agnostic and covers duckdb the same way.
                inspect(conn).get_table_names()
            if in_memory:
                # Not "Connected successfully": an in-memory database keeps
                # nothing, and a green tick with no caveat invites the reader to
                # believe their data is somewhere.
                return ConnectionVerdict(
                    True,
                    "Connected — in-memory database. Nothing is written to disk.",
                    reason="file_in_memory",
                )
            return ConnectionVerdict(
                True,
                f"Connected — read the database at '{path}'.",
                reason="file_found",
                arg=path,
            )
        except Exception:
            logger.warning(
                "Local-file connection test failed for %s at %s",
                connection_type.value,
                path,
                exc_info=True,
            )
            if in_memory:
                return ConnectionVerdict(
                    False,
                    f"The {connection_type.value} driver is not available in this build.",
                    reason="driver_unavailable",
                    arg=connection_type.value,
                )
            return ConnectionVerdict(
                False,
                f"Cannot open '{path}' — check that it is a "
                f"{connection_type.value} database and that it is readable.",
                reason="file_unopenable",
                arg=path,
            )
        finally:
            if engine is not None:
                engine.dispose()

    @staticmethod
    def test_connection_verdict(config: dict, connection_type: ConnectionType) -> ConnectionVerdict:
        """The structured form of :meth:`test_connection` (D6).

        Carries an i18n **key** alongside the English sentence so the UI can
        translate. ``test_message`` is rendered raw from this service today, and
        `en.json` holds the *button label* and not one verdict string — so every
        user in all nine locales reads these verdicts in English. New sentences
        must not add to that, which is why they arrive with keys and a reader in
        the same change.

        A branch that has not been converted returns ``key=""`` and the UI falls
        back to the English text, which is exactly today's behaviour — so this
        is additive rather than a rewrite of every connector's message.
        """
        if not config:
            return ConnectionVerdict(False, "Configuration is empty")

        if connection_type == ConnectionType.MONGODB:
            return ConnectionVerdict(*ConnectionService._test_mongodb(config))

        if connection_type in _FILE_TYPES:
            return ConnectionVerdict(*ConnectionService._test_file_source(config, connection_type))

        if connection_type in _NON_DB_TYPES:
            return ConnectionVerdict(*ConnectionService._test_saas_source(config, connection_type))

        local = ConnectionService._test_local_file_db(config, connection_type)
        if local is not None:
            return local

        return ConnectionVerdict(*ConnectionService._test_sql_connection(config, connection_type))

    @staticmethod
    def test_connection(config: dict, connection_type: ConnectionType) -> tuple[bool | None, str]:
        """Test real database connectivity via SELECT 1. Returns (success, message).

        Kept as the two-tuple every existing caller expects; see
        :meth:`test_connection_verdict` for the form the UI uses.
        """
        v = ConnectionService.test_connection_verdict(config, connection_type)
        return v.ok, v.message

    @staticmethod
    def _test_sql_connection(
        config: dict, connection_type: ConnectionType
    ) -> tuple[bool | None, str]:
        """The network-database branch: build a URL, open it, ``SELECT 1``."""
        try:
            url = _build_sa_url(config, connection_type)
        except ValueError as e:
            return False, str(e)

        connect_args = _connect_args(connection_type, config)

        # Build the engine. Every failure here has to come back as a verdict.
        #
        # This used to catch `ImportError` alone (core#608). SQLAlchemy raises
        # `NoSuchModuleError` — an `ArgumentError`, *not* an `ImportError` —
        # when a URL names a dialect that is not registered, so `databricks`
        # escaped the service, escaped the Reflex event handler, and the user
        # got "An error occurred. Contact the website administrator." for a
        # cloud warehouse: the destination they are most likely evaluating us
        # on, and an administrator they do not have.
        #
        # The line below already had a broad `except Exception`; the line above
        # it did not, and that asymmetry was the whole bug. Widening to
        # `(ImportError, NoSuchModuleError)` would have fixed the reported type
        # and left the next surprise uncaught, which is the same bug again —
        # `bigquery` proves the point, since its dialect does ship and it
        # crashed anyway.
        try:
            engine = create_engine(url, connect_args=connect_args)
        except NoSuchModuleError:
            return False, (
                f"No database driver for {connection_type.value} is installed in this build"
            )
        except ImportError:
            return False, f"Driver not installed for {connection_type.value}"
        except Exception as exc:
            logger.warning("Could not build a %s engine", connection_type.value, exc_info=True)
            return False, describe_connection_failure(
                exc,
                config,
                f"Could not open a {connection_type.value} connection",
            )

        # Oracle rejects a bare ``SELECT 1`` (ORA-00923) — it needs FROM DUAL.
        probe = "SELECT 1 FROM DUAL" if connection_type == ConnectionType.ORACLE else "SELECT 1"
        try:
            with engine.connect() as conn:
                conn.execute(text(probe))
            return True, "Connected successfully"
        except ImportError:
            return False, f"Driver not installed for {connection_type.value}"
        except Exception as exc:
            # The driver's own words, secrets removed (core#625). "check your
            # credentials and network settings" is advice, not information, and
            # when the credentials are in fact correct it is the wrong advice.
            logger.warning("Connection test failed for %s", connection_type.value, exc_info=True)
            return False, describe_connection_failure(
                exc, config, "Connection failed — check your credentials and network settings"
            )
        finally:
            engine.dispose()

    @staticmethod
    def is_select_only(query: str) -> bool:
        """Return True if a query is a single read-only SELECT statement.

        Rejects DDL/DML, multiple statements, and CTE-prefixed mutations.
        Comments and leading/trailing whitespace are ignored.
        """
        if not query or not query.strip():
            return False
        # Strip line and block comments
        cleaned_lines = []
        in_block_comment = False
        for line in query.splitlines():
            i = 0
            line_chars = []
            while i < len(line):
                if in_block_comment:
                    if line[i : i + 2] == "*/":
                        in_block_comment = False
                        i += 2
                    else:
                        i += 1
                elif line[i : i + 2] == "/*":
                    in_block_comment = True
                    i += 2
                elif line[i : i + 2] == "--":
                    break
                else:
                    line_chars.append(line[i])
                    i += 1
            cleaned_lines.append("".join(line_chars))
        cleaned = " ".join(cleaned_lines).strip().rstrip(";").strip()
        if not cleaned:
            return False
        # Reject multiple statements
        if ";" in cleaned:
            return False
        upper = cleaned.upper()
        forbidden_re = re.compile(
            r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|"
            r"GRANT|REVOKE|MERGE)\b"
        )
        if upper.startswith("SELECT"):
            return forbidden_re.search(upper) is None
        if upper.startswith("WITH "):
            # CTE — must contain SELECT and no mutating keywords anywhere
            if forbidden_re.search(upper):
                return False
            return re.search(r"\bSELECT\b", upper) is not None
        return False

    @staticmethod
    def list_tables(
        config: dict, connection_type: ConnectionType, schema: str | None = None
    ) -> list[dict]:
        """List tables in a SQL connection. Returns [{schema, name}].

        Raises ValueError for non-SQL connection types.
        """
        if connection_type in _NON_DB_TYPES:
            raise UserFacingError(f"Cannot list tables for {connection_type.value} connections")
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            insp = inspect(engine)
            schemas = [schema] if schema else insp.get_schema_names()
            tables = []
            for sch in schemas:
                # Skip system schemas
                if sch in ("information_schema", "pg_catalog", "sys"):
                    continue
                try:
                    for name in insp.get_table_names(schema=sch):
                        tables.append({"schema": sch, "name": name})
                except Exception:
                    # Skipping a schema we cannot introspect is right -- one
                    # permission-denied schema must not empty the whole table
                    # picker. But the user then sees a SHORT list with no
                    # indication anything was omitted, and the usual reading of
                    # that is "the connector is broken" (core#723).
                    logger.warning(
                        "Schema %r skipped during table listing; the returned list is partial",
                        sch,
                        exc_info=True,
                    )
                    continue
            return tables
        finally:
            engine.dispose()

    @staticmethod
    def list_columns(
        config: dict,
        connection_type: ConnectionType,
        table: str,
        schema: str | None = None,
    ) -> list[dict]:
        """List columns of a table. Returns [{name, type, nullable}]."""
        if connection_type in _NON_DB_TYPES:
            raise UserFacingError(f"Cannot list columns for {connection_type.value} connections")
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            insp = inspect(engine)
            cols = insp.get_columns(table, schema=schema)
            return [
                {
                    "name": c["name"],
                    "type": str(c["type"]),
                    "nullable": c.get("nullable", True),
                }
                for c in cols
            ]
        finally:
            engine.dispose()

    @staticmethod
    def preview_table(
        config: dict,
        connection_type: ConnectionType,
        table: str,
        schema: str | None = None,
        limit: int = 100,
    ) -> tuple[list[str], list[list]]:
        """Return the first N rows of a table. Returns (columns, rows)."""
        limit = max(1, min(int(limit), 1000))
        # Quote identifiers safely with the engine's preparer
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            preparer = engine.dialect.identifier_preparer
            qualified = preparer.quote(table)
            if schema:
                qualified = f"{preparer.quote_schema(schema)}.{qualified}"
            if connection_type == ConnectionType.ORACLE:
                # Oracle has no LIMIT clause — use the 12c+ row-limiting form.
                # noqa S608: neither interpolated value is user text. `qualified`
                # went through the dialect's own identifier_preparer above, and
                # `limit` is `max(1, min(int(limit), 1000))` — an int, clamped.
                query = f"SELECT * FROM {qualified} FETCH FIRST {limit} ROWS ONLY"  # noqa: S608
            else:
                query = f"SELECT * FROM {qualified} LIMIT {limit}"  # noqa: S608
            with engine.connect() as conn:
                result = conn.execute(text(query))
                columns = list(result.keys())
                rows = [list(row) for row in result.fetchall()]
                return columns, rows
        finally:
            engine.dispose()
