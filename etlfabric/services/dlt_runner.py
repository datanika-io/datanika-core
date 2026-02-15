"""DltRunnerService — builds dlt pipeline/source/destination and executes pipelines."""

import dlt
from dlt.sources.sql_database import sql_database, sql_table

DEFAULT_BATCH_SIZE = 10_000

INTERNAL_CONFIG_KEYS = {
    "mode",
    "table",
    "source_schema",
    "table_names",
    "incremental",
    "batch_size",
    "filters",
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


class DltRunnerError(ValueError):
    """Raised when dlt runner encounters an unsupported configuration."""


class DltRunnerService:
    """Builds dlt pipeline objects from connection configs and pipeline settings."""

    SUPPORTED_DB_TYPES = {"postgres", "mysql", "mssql", "sqlite"}

    def build_destination(self, connection_type: str, config: dict):
        """Map ConnectionType to a dlt destination factory.

        Supports: postgres, mysql, mssql, sqlite.
        Raises DltRunnerError for unsupported types.
        """
        if connection_type not in self.SUPPORTED_DB_TYPES:
            raise DltRunnerError(f"Unsupported destination type: {connection_type}")

        factory = getattr(dlt.destinations, connection_type)
        return factory(credentials=config)

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
        if connection_type not in self.SUPPORTED_DB_TYPES:
            raise DltRunnerError(f"Unsupported source type: {connection_type}")

        mode = dlt_config.get("mode", "full_database")
        schema = dlt_config.get("source_schema")

        if mode == "single_table":
            kwargs = {"credentials": config, "table": dlt_config["table"], "chunk_size": batch_size}
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
            kwargs = {"credentials": config, "chunk_size": batch_size}
            if schema is not None:
                kwargs["schema"] = schema
            table_names = dlt_config.get("table_names")
            if table_names is not None:
                kwargs["table_names"] = table_names
            return sql_database(**kwargs)

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
    ) -> dict:
        """Execute a dlt pipeline.

        Extracts batch_size from dlt_config if not passed explicitly.
        Filters INTERNAL_CONFIG_KEYS before passing to pipeline.run().

        Returns {"rows_loaded": int, "load_info": LoadInfo}.
        """
        if batch_size is None:
            batch_size = dlt_config.get("batch_size", DEFAULT_BATCH_SIZE)

        pipeline = self.build_pipeline(pipeline_id, destination_type, destination_config)
        source = self.build_source(source_type, source_config, dlt_config, batch_size=batch_size)

        # Apply row-level filters
        filters_cfg = dlt_config.get("filters")
        if filters_cfg:
            for f in filters_cfg:
                filter_fn = FILTER_OPS[f["op"]](f["column"], f["value"])
                source.add_filter(filter_fn)

        run_kwargs = {k: v for k, v in dlt_config.items() if k not in INTERNAL_CONFIG_KEYS}
        load_info = pipeline.run(source, **run_kwargs)
        rows_loaded = getattr(load_info, "loads_count", 0) or 0

        return {
            "rows_loaded": rows_loaded,
            "load_info": load_info,
        }
