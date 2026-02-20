"""DbtProjectService — manages per-tenant dbt project directories and executes dbt commands."""

import json
import re
from pathlib import Path

import yaml
from dbt.cli.main import dbtRunner


def _to_plain(obj):
    """Convert nested data to plain Python types safe for yaml.safe_dump."""
    return json.loads(json.dumps(obj))


class DbtProjectError(ValueError):
    """Raised when dbt project operations fail."""


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")
SUPPORTED_ADAPTERS = {"postgres", "mysql", "mssql", "sqlite", "bigquery", "snowflake", "redshift"}


def _sum_rows_affected(result) -> int:
    """Extract total rows_affected from a dbt invocation result.

    dbt 1.7 stores adapter_response as a plain dict, so we use .get()
    instead of attribute access.
    """
    total = 0
    if result.result:
        for node_result in result.result:
            resp = getattr(node_result, "adapter_response", None)
            if isinstance(resp, dict):
                total += resp.get("rows_affected") or 0
            elif resp and hasattr(resp, "rows_affected"):
                total += resp.rows_affected or 0
    return total


def _validate_identifier(name: str, label: str = "Name") -> None:
    """Validate that a name is a safe identifier (no path traversal, no special chars)."""
    if not name:
        raise DbtProjectError(f"{label} cannot be empty")
    if not _IDENTIFIER_RE.match(name):
        raise DbtProjectError(
            f"{label} must start with a letter or underscore and contain only "
            f"letters, digits, underscores, and hyphens. Got: {name!r}"
        )


class DbtProjectService:
    """Manages per-tenant dbt project directories and executes dbt commands."""

    def __init__(self, projects_dir: str):
        self._projects_dir = Path(projects_dir)

    def get_project_path(self, org_id: int) -> Path:
        """Return path: {projects_dir}/tenant_{org_id}/"""
        return self._projects_dir / f"tenant_{org_id}"

    def ensure_project(self, org_id: int) -> Path:
        """Create dbt project scaffold if it doesn't exist. Returns project path."""
        project_path = self.get_project_path(org_id)
        project_path.mkdir(parents=True, exist_ok=True)
        (project_path / "models").mkdir(exist_ok=True)
        (project_path / "macros").mkdir(exist_ok=True)
        (project_path / "tests").mkdir(exist_ok=True)
        (project_path / "snapshots").mkdir(exist_ok=True)

        yml_path = project_path / "dbt_project.yml"
        if not yml_path.exists():
            project_name = f"tenant_{org_id}"
            content = {
                "name": project_name,
                "version": "1.0.0",
                "config-version": 2,
                "profile": project_name,
                "model-paths": ["models"],
                "macro-paths": ["macros"],
                "test-paths": ["tests"],
                "snapshot-paths": ["snapshots"],
            }
            yml_path.write_text(yaml.safe_dump(content, default_flow_style=False))

        return project_path

    def write_model(
        self,
        org_id: int,
        model_name: str,
        sql_body: str,
        schema_name: str = "staging",
        materialization: str = "view",
        incremental_config: dict | None = None,
    ) -> Path:
        """Write a .sql model file + schema.yml config. Returns path to the .sql file."""
        _validate_identifier(model_name, "Model name")
        _validate_identifier(schema_name, "Schema name")
        project_path = self.get_project_path(org_id)
        schema_dir = project_path / "models" / schema_name
        schema_dir.mkdir(parents=True, exist_ok=True)

        sql_path = schema_dir / f"{model_name}.sql"
        sql_path.write_text(sql_body)

        schema_yml_path = schema_dir / "schema.yml"
        if schema_yml_path.exists():
            schema_config = yaml.safe_load(schema_yml_path.read_text()) or {}
        else:
            schema_config = {"version": 2, "models": []}

        if "models" not in schema_config:
            schema_config["models"] = []

        # Build dbt config dict
        model_cfg: dict = {"materialized": materialization}
        if materialization == "incremental" and incremental_config:
            for key in ("unique_key", "strategy", "updated_at", "on_schema_change"):
                if incremental_config.get(key):
                    model_cfg[key] = incremental_config[key]

        # Update or add model entry
        existing = [m for m in schema_config["models"] if m.get("name") == model_name]
        if existing:
            existing[0]["config"] = model_cfg
        else:
            schema_config["models"].append(
                {
                    "name": model_name,
                    "config": model_cfg,
                }
            )

        schema_yml_path.write_text(yaml.safe_dump(schema_config, default_flow_style=False))
        return sql_path

    def generate_profiles_yml(
        self, org_id: int, connection_type: str, connection_config: dict
    ) -> Path:
        """Generate profiles.yml from a destination connection config. Returns path."""
        if connection_type not in SUPPORTED_ADAPTERS:
            raise DbtProjectError(f"Unsupported dbt adapter: {connection_type}")

        project_path = self.get_project_path(org_id)
        profile_name = f"tenant_{org_id}"

        output = self._build_profile_output(connection_type, connection_config)

        profile = {
            profile_name: {
                "target": "default",
                "outputs": {"default": output},
            }
        }

        profiles_path = project_path / "profiles.yml"
        profiles_path.write_text(yaml.safe_dump(profile, default_flow_style=False))
        return profiles_path

    @staticmethod
    def _build_profile_output(connection_type: str, config: dict) -> dict:
        """Build adapter-specific dbt profile output dict."""
        if connection_type == "bigquery":
            output = {
                "type": "bigquery",
                "method": "service-account-json",
                "project": config.get("project", ""),
                "dataset": config.get("dataset", ""),
                "threads": 4,
            }
            if "keyfile_json" in config:
                output["keyfile_json"] = config["keyfile_json"]
            return output
        if connection_type == "snowflake":
            return {
                "type": "snowflake",
                "account": config.get("account", ""),
                "user": config.get("user", ""),
                "password": config.get("password", ""),
                "database": config.get("database", ""),
                "warehouse": config.get("warehouse", ""),
                "role": config.get("role", ""),
                "schema": config.get("schema", "PUBLIC"),
                "threads": 4,
            }
        # Map connection type to dbt adapter type
        dbt_type = {"mssql": "sqlserver"}.get(connection_type, connection_type)
        # postgres, mysql, sqlserver, sqlite, redshift — same shape
        return {
            "type": dbt_type,
            "host": config.get("host", ""),
            "port": config.get("port", 5432),
            "user": config.get("user", ""),
            "password": config.get("password", ""),
            "dbname": config.get("database", ""),
            "schema": config.get("schema", "public"),
            "threads": 4,
        }

    VALID_COMMANDS = {"build", "run", "test", "seed", "snapshot", "compile"}
    FULL_REFRESH_COMMANDS = {"build", "run"}

    def run_command(
        self,
        org_id: int,
        command: str,
        selector: str | None = None,
        full_refresh: bool = False,
    ) -> dict:
        """Execute an arbitrary dbt command with optional selector and full-refresh.

        Returns {"success": bool, "rows_affected": int, "logs": str}.
        """
        if command not in self.VALID_COMMANDS:
            raise DbtProjectError(
                f"Invalid dbt command: {command!r}. Must be one of {self.VALID_COMMANDS}"
            )

        project_path = self.get_project_path(org_id)
        if not project_path.exists():
            raise DbtProjectError(f"dbt project not found for org {org_id}")

        args = [
            command,
            "--project-dir",
            str(project_path),
            "--profiles-dir",
            str(project_path),
        ]
        if selector:
            args.extend(["--select", selector])
        if full_refresh and command in self.FULL_REFRESH_COMMANDS:
            args.append("--full-refresh")

        runner = dbtRunner()
        result = runner.invoke(args)

        rows_affected = _sum_rows_affected(result)

        logs = str(result.result) if result.result else ""
        if not logs and result.exception:
            logs = str(result.exception)

        return {
            "success": result.success,
            "rows_affected": rows_affected,
            "logs": logs,
        }

    def run_model(self, org_id: int, model_name: str) -> dict:
        """Execute `dbt run --select model_name` for the tenant project.

        Returns {"success": bool, "rows_affected": int, "logs": str}.
        """
        _validate_identifier(model_name, "Model name")
        project_path = self.get_project_path(org_id)
        if not project_path.exists():
            raise DbtProjectError(f"dbt project not found for org {org_id}")

        runner = dbtRunner()
        result = runner.invoke(
            [
                "run",
                "--select",
                model_name,
                "--project-dir",
                str(project_path),
                "--profiles-dir",
                str(project_path),
            ]
        )

        rows_affected = _sum_rows_affected(result)

        logs = str(result.result) if result.result else ""
        if not logs and result.exception:
            logs = str(result.exception)

        return {
            "success": result.success,
            "rows_affected": rows_affected,
            "logs": logs,
        }

    def write_tests_config(
        self,
        org_id: int,
        model_name: str,
        tests_config: dict,
        schema_name: str = "staging",
    ) -> Path:
        """Write column tests into schema.yml for the given model. Returns schema.yml path."""
        _validate_identifier(model_name, "Model name")
        _validate_identifier(schema_name, "Schema name")
        project_path = self.get_project_path(org_id)
        schema_dir = project_path / "models" / schema_name
        schema_yml_path = schema_dir / "schema.yml"

        if schema_yml_path.exists():
            schema_config = yaml.safe_load(schema_yml_path.read_text()) or {}
        else:
            schema_config = {"version": 2, "models": []}

        if "models" not in schema_config:
            schema_config["models"] = []

        # Find or create model entry
        existing = [m for m in schema_config["models"] if m.get("name") == model_name]
        if existing:
            model_entry = existing[0]
        else:
            model_entry = {"name": model_name}
            schema_config["models"].append(model_entry)

        # Build column entries from tests_config
        columns_cfg = tests_config.get("columns", {})
        columns_list = []
        for col_name, col_tests in columns_cfg.items():
            tests = []
            if isinstance(col_tests, list):
                tests = list(col_tests)
            elif isinstance(col_tests, dict):
                for test_name, test_cfg in col_tests.items():
                    if test_name in ("not_null", "unique"):
                        tests.append(test_name)
                    elif test_name == "accepted_values":
                        tests.append({"accepted_values": {"values": test_cfg}})
                    elif test_name == "relationships":
                        tests.append({"relationships": test_cfg})
            columns_list.append({"name": col_name, "tests": tests})

        if columns_list:
            model_entry["columns"] = columns_list
        else:
            model_entry.pop("columns", None)

        schema_yml_path.write_text(yaml.safe_dump(schema_config, default_flow_style=False))
        return schema_yml_path

    def run_test(self, org_id: int, model_name: str) -> dict:
        """Execute `dbt test --select model_name` for the tenant project.

        Returns {"success": bool, "logs": str}.
        """
        _validate_identifier(model_name, "Model name")
        project_path = self.get_project_path(org_id)
        if not project_path.exists():
            raise DbtProjectError(f"dbt project not found for org {org_id}")

        runner = dbtRunner()
        result = runner.invoke(
            [
                "test",
                "--select",
                model_name,
                "--project-dir",
                str(project_path),
                "--profiles-dir",
                str(project_path),
            ]
        )

        logs = str(result.result) if result.result else ""
        if not logs and result.exception:
            logs = str(result.exception)
        return {
            "success": result.success,
            "logs": logs,
        }

    def compile_model(self, org_id: int, model_name: str) -> dict:
        """Execute `dbt compile --select model_name` for the tenant project.

        Returns {"success": bool, "compiled_sql": str, "logs": str}.
        """
        _validate_identifier(model_name, "Model name")
        project_path = self.get_project_path(org_id)
        if not project_path.exists():
            raise DbtProjectError(f"dbt project not found for org {org_id}")

        runner = dbtRunner()
        result = runner.invoke(
            [
                "compile",
                "--select",
                model_name,
                "--project-dir",
                str(project_path),
                "--profiles-dir",
                str(project_path),
            ]
        )

        compiled_sql = ""
        if result.success and result.result:
            for node_result in result.result:
                # compiled_code may be on node_result directly or on node_result.node
                code = getattr(node_result, "compiled_code", None)
                if not code:
                    node = getattr(node_result, "node", None)
                    if node:
                        code = getattr(node, "compiled_code", None)
                if code:
                    compiled_sql = code
                    break

        logs = ""
        if result.exception:
            logs = str(result.exception)
        elif not compiled_sql and result.result:
            # Extract concise message from results instead of raw repr
            messages = []
            for nr in result.result:
                msg = getattr(nr, "message", None)
                if msg:
                    messages.append(msg)
            logs = "; ".join(messages) if messages else "Compilation produced no output"
        return {
            "success": result.success,
            "compiled_sql": compiled_sql,
            "logs": logs,
        }

    def remove_model(self, org_id: int, model_name: str, schema_name: str = "staging") -> bool:
        """Delete a model .sql file. Returns True if file existed."""
        _validate_identifier(model_name, "Model name")
        _validate_identifier(schema_name, "Schema name")
        project_path = self.get_project_path(org_id)
        sql_path = project_path / "models" / schema_name / f"{model_name}.sql"
        if sql_path.exists():
            sql_path.unlink()
            return True
        return False

    def write_packages_yml(self, org_id: int, packages: list[dict]) -> Path:
        """Write packages.yml for the tenant project. Returns path."""
        project_path = self.get_project_path(org_id)
        packages_path = project_path / "packages.yml"
        content = {"packages": packages}
        packages_path.write_text(yaml.safe_dump(content, default_flow_style=False))
        return packages_path

    def install_packages(self, org_id: int) -> dict:
        """Run `dbt deps` to install packages. Returns {"success": bool, "logs": str}."""
        project_path = self.get_project_path(org_id)
        if not project_path.exists():
            raise DbtProjectError(f"dbt project not found for org {org_id}")

        packages_path = project_path / "packages.yml"
        if not packages_path.exists():
            raise DbtProjectError(f"packages.yml not found for org {org_id}")

        runner = dbtRunner()
        result = runner.invoke(
            ["deps", "--project-dir", str(project_path), "--profiles-dir", str(project_path)]
        )

        logs = str(result.result) if result.result else ""
        return {"success": result.success, "logs": logs}

    def write_snapshot(
        self,
        org_id: int,
        snapshot_name: str,
        sql_body: str,
        unique_key: str,
        strategy: str = "timestamp",
        updated_at: str | None = None,
        check_cols: list[str] | None = None,
        target_schema: str = "snapshots",
    ) -> Path:
        """Write a dbt snapshot file. Returns path to the .sql file."""
        _validate_identifier(snapshot_name, "Snapshot name")
        _validate_identifier(target_schema, "Target schema")
        _validate_identifier(unique_key, "Unique key")
        if strategy not in ("timestamp", "check"):
            raise DbtProjectError(f"Invalid snapshot strategy: {strategy!r}")
        if updated_at:
            _validate_identifier(updated_at, "Updated at column")
        if check_cols:
            for col in check_cols:
                _validate_identifier(col, "Check column")

        project_path = self.get_project_path(org_id)
        snapshots_dir = project_path / "snapshots"
        snapshots_dir.mkdir(parents=True, exist_ok=True)

        config_lines = [
            f"        target_schema='{target_schema}',",
            f"        unique_key='{unique_key}',",
            f"        strategy='{strategy}',",
        ]
        if strategy == "timestamp" and updated_at:
            config_lines.append(f"        updated_at='{updated_at}',")
        if strategy == "check" and check_cols:
            config_lines.append(f"        check_cols={check_cols!r},")

        config_block = "\n".join(config_lines)
        content = (
            f"{{% snapshot {snapshot_name} %}}\n"
            f"{{{{\n"
            f"    config(\n"
            f"{config_block}\n"
            f"    )\n"
            f"}}}}\n"
            f"{sql_body}\n"
            f"{{% endsnapshot %}}\n"
        )

        sql_path = snapshots_dir / f"{snapshot_name}.sql"
        sql_path.write_text(content)
        return sql_path

    def run_snapshot(self, org_id: int, snapshot_name: str) -> dict:
        """Execute `dbt snapshot --select snapshot_name`. Returns result dict."""
        _validate_identifier(snapshot_name, "Snapshot name")
        project_path = self.get_project_path(org_id)
        if not project_path.exists():
            raise DbtProjectError(f"dbt project not found for org {org_id}")

        runner = dbtRunner()
        result = runner.invoke(
            [
                "snapshot",
                "--select",
                snapshot_name,
                "--project-dir",
                str(project_path),
                "--profiles-dir",
                str(project_path),
            ]
        )

        rows_affected = 0
        if result.result:
            for node_result in result.result:
                resp = getattr(node_result, "adapter_response", None)
                if resp and hasattr(resp, "rows_affected"):
                    rows_affected += resp.rows_affected or 0

        logs = str(result.result) if result.result else ""
        return {"success": result.success, "rows_affected": rows_affected, "logs": logs}

    def remove_snapshot(self, org_id: int, snapshot_name: str) -> bool:
        """Delete a snapshot .sql file. Returns True if file existed."""
        _validate_identifier(snapshot_name, "Snapshot name")
        project_path = self.get_project_path(org_id)
        sql_path = project_path / "snapshots" / f"{snapshot_name}.sql"
        if sql_path.exists():
            sql_path.unlink()
            return True
        return False

    def write_source_yml_for_connection(
        self,
        org_id: int,
        conn_name_snake: str,
        sources: list[dict],
    ) -> Path:
        """Write models/{conn_name_snake}_src.yml.

        *sources* is a list of dbt source definitions::

            [{
                "name": dataset_name,
                "description": "...",
                "schema": dataset_name,
                "tables": [{"name": "t1", "columns": [{"name": "c1", "data_type": "INT"}]}],
                "freshness": {...} | None,
            }]
        """
        project_path = self.get_project_path(org_id)
        models_dir = project_path / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        # Build dbt-compatible source entries
        dbt_sources = []
        for src in sources:
            source_def: dict = {
                "name": src["name"],
                "schema": src.get("schema", src["name"]),
                "tables": [],
            }
            if src.get("description"):
                source_def["description"] = src["description"]
            if src.get("freshness"):
                source_def["freshness"] = src["freshness"]
            for tbl in src.get("tables", []):
                tbl_def: dict = {"name": tbl["name"]}
                if tbl.get("columns"):
                    col_list = []
                    for c in tbl["columns"]:
                        col_entry = {"name": c["name"], "data_type": c.get("data_type", "")}
                        if c.get("description"):
                            col_entry["description"] = c["description"]
                        if c.get("tests"):
                            col_entry["tests"] = _to_plain(c["tests"])
                        col_list.append(col_entry)
                    tbl_def["columns"] = col_list
                source_def["tables"].append(tbl_def)
            dbt_sources.append(source_def)

        _validate_identifier(conn_name_snake, "Connection name")
        content = {"version": 2, "sources": dbt_sources}
        yml_path = models_dir / f"{conn_name_snake}_src.yml"
        yml_path.write_text(yaml.safe_dump(content, default_flow_style=False))
        return yml_path

    def write_model_yml(
        self,
        org_id: int,
        model_name: str,
        schema_name: str,
        columns: list[dict],
        description: str | None = None,
        dbt_config: dict | None = None,
    ) -> Path:
        """Write models/{schema_name}/{model_name}.yml."""
        _validate_identifier(model_name, "Model name")
        _validate_identifier(schema_name, "Schema name")
        project_path = self.get_project_path(org_id)
        schema_dir = project_path / "models" / schema_name
        schema_dir.mkdir(parents=True, exist_ok=True)

        model_entry: dict = {"name": model_name}
        if description:
            model_entry["description"] = description
        if dbt_config:
            model_entry["config"] = dbt_config
        if columns:
            col_list = []
            for c in columns:
                col_entry = {"name": c["name"], "data_type": c.get("data_type", "")}
                if c.get("description"):
                    col_entry["description"] = c["description"]
                if c.get("tests"):
                    col_entry["tests"] = _to_plain(c["tests"])
                col_list.append(col_entry)
            model_entry["columns"] = col_list

        content = {"version": 2, "models": [model_entry]}
        yml_path = schema_dir / f"{model_name}.yml"
        yml_path.write_text(yaml.safe_dump(content, default_flow_style=False))
        return yml_path

    def write_sources_yml(
        self,
        org_id: int,
        source_name: str,
        schema_name: str,
        tables: list[dict],
        freshness_config: dict | None = None,
    ) -> Path:
        """Write sources.yml with optional freshness config. Returns path."""
        project_path = self.get_project_path(org_id)
        models_dir = project_path / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        source_def: dict = {
            "name": source_name,
            "schema": schema_name,
            "tables": tables,
        }
        if freshness_config:
            source_def["freshness"] = freshness_config

        content = {"version": 2, "sources": [source_def]}
        sources_path = models_dir / "sources.yml"
        sources_path.write_text(yaml.safe_dump(content, default_flow_style=False))
        return sources_path

    def check_freshness(self, org_id: int, source_name: str | None = None) -> dict:
        """Run `dbt source freshness`. Returns {"success": bool, "logs": str}."""
        if source_name:
            _validate_identifier(source_name, "Source name")
        project_path = self.get_project_path(org_id)
        if not project_path.exists():
            raise DbtProjectError(f"dbt project not found for org {org_id}")

        cmd = [
            "source",
            "freshness",
            "--project-dir",
            str(project_path),
            "--profiles-dir",
            str(project_path),
        ]
        if source_name:
            cmd.extend(["--select", f"source:{source_name}"])

        runner = dbtRunner()
        result = runner.invoke(cmd)

        logs = str(result.result) if result.result else ""
        return {"success": result.success, "logs": logs}
