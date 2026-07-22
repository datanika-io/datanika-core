"""DltRunnerService — builds dlt pipeline/source/destination and executes pipelines."""

import logging
import os
import shutil
from pathlib import PurePosixPath

import dlt
from dlt.common import json as dlt_json
from dlt.sources.filesystem import filesystem, read_csv, read_parquet
from dlt.sources.rest_api import rest_api_source
from dlt.sources.sql_database import sql_database, sql_table

from datanika.services.egress_guard import build_guarded_session, validate_egress_host

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
    # Oracle is source-only — dlt ships no Oracle destination.
    SUPPORTED_DESTINATION_TYPES = (SUPPORTED_SOURCE_TYPES - {"oracle"}) | {
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

        Adds drivername for SQL source types, renames user→username.
        """
        creds = dict(config)

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

        factory = getattr(dlt.destinations, connection_type)
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
            return self._build_saas_source(connection_type, config, dlt_config)

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
        return rest_api_source(rest_config)

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
        """Build a dlt source for MongoDB using pymongo."""
        from urllib.parse import quote_plus

        from datanika.services.mongodb_source import mongodb_source

        database = config.get("database") or dlt_config.get("database", "")
        if not database:
            raise DltRunnerError("MongoDB source requires 'database' in config")

        host = config.get("host", "localhost")
        port = config.get("port", 27017)
        user = config.get("user", "")
        password = config.get("password", "")

        if user:
            uri = f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"
        else:
            uri = f"mongodb://{host}:{port}/{database}"

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
        """Build a dlt verified source for SaaS connectors.

        Uses official dlt verified sources (bundled via ``dlt init`` at Docker
        build time).  Falls back to a generic REST API source when the verified
        source module is not installed.
        """
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
                    or [
                        {
                            "name": "accounts",
                            "endpoint": {
                                "path": "services/data/v59.0/sobjects/Account",
                            },
                        },
                        {
                            "name": "contacts",
                            "endpoint": {
                                "path": "services/data/v59.0/sobjects/Contact",
                            },
                        },
                        {
                            "name": "opportunities",
                            "endpoint": {
                                "path": "services/data/v59.0/sobjects/Opportunity",
                            },
                        },
                    ],
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
                raise DltRunnerError(
                    "Google Analytics verified source not installed (run dlt init google_analytics)"
                ) from None

        if connection_type == "google_ads":
            credentials_json = config.get("service_account_json", "")
            customer_id = config.get("customer_id", "") or dlt_config.get("customer_id", "")
            if not customer_id:
                raise DltRunnerError("Google Ads source requires 'customer_id'")
            try:
                from google_ads import google_ads

                return google_ads(customer_id=customer_id)
            except ImportError:
                raise DltRunnerError(
                    "Google Ads verified source not installed (run dlt init google_ads)"
                ) from None

        if connection_type == "facebook_ads":
            access_token = config.get("access_token") or config.get("api_key", "")
            account_id = config.get("account_id", "") or dlt_config.get("account_id", "")
            if not access_token or not account_id:
                raise DltRunnerError("Facebook Ads source requires 'access_token' and 'account_id'")
            try:
                from facebook_ads import facebook_ads_source

                return facebook_ads_source(account_id=account_id, access_token=access_token)
            except ImportError:
                raise DltRunnerError(
                    "Facebook Ads verified source not installed (run dlt init facebook_ads)"
                ) from None

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
            )

        raise DltRunnerError(f"Unsupported SaaS source type: {connection_type}")

    def _build_kafka_source(self, config: dict, dlt_config: dict):
        """Build a Kafka consumer source."""
        bootstrap_servers = config.get("bootstrap_servers", "")
        topics = dlt_config.get("topics") or config.get("topics", [])
        group_id = config.get("group_id", "datanika-consumer")
        if not bootstrap_servers:
            raise DltRunnerError("Kafka source requires 'bootstrap_servers'")
        if not topics:
            raise DltRunnerError("Kafka source requires 'topics'")
        try:
            from kafka import kafka_consumer

            return kafka_consumer(
                topics=topics if isinstance(topics, list) else [topics],
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
            )
        except ImportError:
            raise DltRunnerError(
                "Kafka verified source not installed (run dlt init kafka)"
            ) from None

    @staticmethod
    def _rest_api_fallback(
        base_url: str,
        auth: dict | None,
        resources: list,
        headers: dict | None = None,
    ):
        """Generic REST API source used when a verified source module is not installed."""
        validate_egress_host(base_url)  # SSRF pre-flight guard (core#338)
        client: dict = {"base_url": base_url}
        if auth:
            client["auth"] = auth
        if headers:
            client["headers"] = headers
        return rest_api_source({"client": client, "resources": resources})

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
