"""DltRunnerService — builds dlt pipeline/source/destination and executes pipelines."""

import dlt
from dlt.sources.sql_database import sql_database

DEFAULT_BATCH_SIZE = 10_000


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
        """Build a dlt sql_database source from connection config.

        Uses dlt's sql_database verified source for all DB types.
        Sets chunk_size=batch_size on the source to stream rows in batches.
        Raises DltRunnerError for unsupported types.
        """
        if connection_type not in self.SUPPORTED_DB_TYPES:
            raise DltRunnerError(f"Unsupported source type: {connection_type}")

        return sql_database(credentials=config, chunk_size=batch_size)

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
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> dict:
        """Execute a dlt pipeline.

        Chunking is handled internally by dlt via chunk_size on the source —
        rows stream in batches without loading full tables into memory.

        Returns {"rows_loaded": int, "load_info": LoadInfo}.
        """
        pipeline = self.build_pipeline(pipeline_id, destination_type, destination_config)
        source = self.build_source(source_type, source_config, dlt_config, batch_size=batch_size)

        load_info = pipeline.run(source, **dlt_config)
        rows_loaded = getattr(load_info, "loads_count", 0) or 0

        return {
            "rows_loaded": rows_loaded,
            "load_info": load_info,
        }
