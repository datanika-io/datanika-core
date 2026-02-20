"""DltRunnerService — builds dlt pipeline/source/destination and executes pipelines."""

import dlt
from dlt.sources.filesystem import filesystem
from dlt.sources.rest_api import rest_api_source
from dlt.sources.sql_database import sql_database, sql_table

DEFAULT_BATCH_SIZE = 10_000

SUPPORTED_FILE_TYPES = {"s3", "csv", "json", "parquet"}

DEFAULT_FILE_GLOBS = {"csv": "*.csv", "json": "*.json", "parquet": "*.parquet", "s3": "*"}

AWS_CREDENTIAL_KEYS = {"aws_access_key_id", "aws_secret_access_key", "region_name", "endpoint_url"}

SUPPORTED_REST_TYPES = {"rest_api"}

SUPPORTED_SHEETS_TYPES = {"google_sheets"}

SUPPORTED_MONGODB_TYPES = {"mongodb"}

INTERNAL_CONFIG_KEYS = {
    "mode",
    "table",
    "source_schema",
    "table_names",
    "incremental",
    "batch_size",
    "filters",
    "bucket_url",
    "file_glob",
    "resources",
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
}

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


def _extract_rows_loaded(load_info) -> int:
    """Sum items_count across all job metrics in a dlt LoadInfo."""
    total = 0
    for metrics_list in load_info.metrics.values():
        for metrics in metrics_list:
            for jm in metrics.get("job_metrics", {}).values():
                total += getattr(jm, "items_count", 0)
    return total


class DltRunnerError(ValueError):
    """Raised when dlt runner encounters an unsupported configuration."""


DRIVERNAME_MAP = {
    "postgres": "postgresql",
    "mysql": "mysql+pymysql",
    "mssql": "mssql+pymssql",
    "sqlite": "sqlite",
    "redshift": "redshift+redshift_connector",
}

# Types where user→username renaming is needed
_RENAME_USER_TYPES = {"postgres", "mysql", "mssql", "sqlite", "redshift", "snowflake"}


class DltRunnerService:
    """Builds dlt pipeline objects from connection configs and pipeline settings."""

    SUPPORTED_SOURCE_TYPES = {"postgres", "mysql", "mssql", "sqlite"}
    SUPPORTED_DESTINATION_TYPES = SUPPORTED_SOURCE_TYPES | {"bigquery", "snowflake", "redshift"}

    @staticmethod
    def _to_dlt_credentials(connection_type: str, config: dict) -> dict:
        """Convert stored connection config to dlt-compatible credentials.

        Adds drivername for SQL types, renames user→username.
        """
        creds = dict(config)

        # Rename user → username for SQL and Snowflake types
        if connection_type in _RENAME_USER_TYPES and "user" in creds:
            creds["username"] = creds.pop("user")

        drivername = DRIVERNAME_MAP.get(connection_type)
        if drivername:
            creds["drivername"] = drivername
            # SQLite: path stored as "path", dlt expects "database"
            if connection_type == "sqlite" and "path" in creds:
                creds["database"] = creds.pop("path")

        return creds

    def build_destination(self, connection_type: str, config: dict):
        """Map ConnectionType to a dlt destination factory.

        Supports SQL databases and cloud warehouses.
        Raises DltRunnerError for unsupported types.
        """
        if connection_type not in self.SUPPORTED_DESTINATION_TYPES:
            raise DltRunnerError(f"Unsupported destination type: {connection_type}")

        factory = getattr(dlt.destinations, connection_type)
        return factory(credentials=self._to_dlt_credentials(connection_type, config))

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

        if connection_type in SUPPORTED_MONGODB_TYPES:
            return self._build_mongodb_source(config, dlt_config, batch_size)

        if connection_type not in self.SUPPORTED_SOURCE_TYPES:
            raise DltRunnerError(f"Unsupported source type: {connection_type}")

        mode = dlt_config.get("mode", "full_database")
        schema = dlt_config.get("source_schema")

        creds = self._to_dlt_credentials(connection_type, config)

        if mode == "single_table":
            kwargs = {"credentials": creds, "table": dlt_config["table"], "chunk_size": batch_size}
            if schema is not None:
                kwargs["schema"] = schema
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
            table_names = dlt_config.get("table_names")
            if table_names is not None:
                kwargs["table_names"] = table_names
            return sql_database(**kwargs)

    def _build_file_source(self, connection_type: str, config: dict, dlt_config: dict):
        """Build a dlt filesystem source for file-based connections."""
        bucket_url = dlt_config.get("bucket_url") or config.get("bucket_url", "")
        if not bucket_url:
            raise DltRunnerError("File sources require 'bucket_url' in config or dlt_config")

        file_glob = dlt_config.get("file_glob") or DEFAULT_FILE_GLOBS.get(connection_type, "*")

        kwargs = {"bucket_url": bucket_url, "file_glob": file_glob}

        if connection_type == "s3":
            credentials = {k: v for k, v in config.items() if k in AWS_CREDENTIAL_KEYS}
            if credentials:
                kwargs["credentials"] = credentials

        return filesystem(**kwargs)

    def _build_rest_api_source(self, config: dict, dlt_config: dict):
        """Build a dlt REST API source from connection config + dlt_config."""
        base_url = config.get("base_url") or dlt_config.get("base_url")
        if not base_url:
            raise DltRunnerError("REST API source requires 'base_url'")

        resources = dlt_config.get("resources")
        if not resources or not isinstance(resources, list):
            raise DltRunnerError("REST API source requires 'resources' list in dlt_config")

        client_config: dict = {"base_url": base_url}

        headers = config.get("headers") or dlt_config.get("headers")
        if headers:
            client_config["headers"] = headers

        auth = config.get("auth") or dlt_config.get("auth")
        if auth:
            client_config["auth"] = auth

        paginator = dlt_config.get("paginator")
        if paginator:
            client_config["paginator"] = paginator

        rest_config: dict = {"client": client_config, "resources": resources}

        resource_defaults = dlt_config.get("resource_defaults")
        if resource_defaults:
            rest_config["resource_defaults"] = resource_defaults

        return rest_api_source(rest_config)

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
            uri = (
                f"mongodb://{quote_plus(user)}:{quote_plus(password)}"
                f"@{host}:{port}/{database}"
            )
        else:
            uri = f"mongodb://{host}:{port}/{database}"

        collection_names = dlt_config.get("collection_names")

        return mongodb_source(
            connection_uri=uri,
            database=database,
            collection_names=collection_names,
            batch_size=batch_size,
        )

    def build_pipeline(
        self,
        pipeline_id: int,
        destination_type: str,
        destination_config: dict,
        dataset_name: str | None = None,
    ):
        """Create a dlt.Pipeline with the given destination."""
        destination = self.build_destination(destination_type, destination_config)
        kwargs = {
            "pipeline_name": f"pipeline_{pipeline_id}",
            "destination": destination,
        }
        if dataset_name is not None:
            kwargs["dataset_name"] = dataset_name
        return dlt.pipeline(**kwargs)

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
    ) -> dict:
        """Execute a dlt pipeline.

        Extracts batch_size from dlt_config if not passed explicitly.
        Filters INTERNAL_CONFIG_KEYS before passing to pipeline.run().

        Returns {"rows_loaded": int, "load_info": LoadInfo}.
        """
        if batch_size is None:
            batch_size = dlt_config.get("batch_size", DEFAULT_BATCH_SIZE)

        pipeline = self.build_pipeline(
            pipeline_id, destination_type, destination_config, dataset_name=dataset_name
        )
        source = self.build_source(source_type, source_config, dlt_config, batch_size=batch_size)

        # Apply row-level filters
        filters_cfg = dlt_config.get("filters")
        if filters_cfg:
            for f in filters_cfg:
                filter_fn = FILTER_OPS[f["op"]](f["column"], f["value"])
                source.add_filter(filter_fn)

        run_kwargs = {k: v for k, v in dlt_config.items() if k not in INTERNAL_CONFIG_KEYS}
        load_info = pipeline.run(source, **run_kwargs)
        rows_loaded = _extract_rows_loaded(load_info)

        return {
            "rows_loaded": rows_loaded,
            "load_info": load_info,
        }
