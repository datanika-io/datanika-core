"""DbtProjectService — manages per-tenant dbt project directories and executes dbt commands."""

from pathlib import Path

import yaml
from dbt.cli.main import dbtRunner


class DbtProjectError(ValueError):
    """Raised when dbt project operations fail."""


SUPPORTED_ADAPTERS = {"postgres", "mysql", "mssql", "sqlite"}


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
            }
            yml_path.write_text(yaml.dump(content, default_flow_style=False))

        return project_path

    def write_model(
        self,
        org_id: int,
        model_name: str,
        sql_body: str,
        schema_name: str = "staging",
        materialization: str = "view",
    ) -> Path:
        """Write a .sql model file + schema.yml config. Returns path to the .sql file."""
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

        # Update or add model entry
        existing = [m for m in schema_config["models"] if m.get("name") == model_name]
        if existing:
            existing[0]["config"] = {"materialized": materialization}
        else:
            schema_config["models"].append(
                {
                    "name": model_name,
                    "config": {"materialized": materialization},
                }
            )

        schema_yml_path.write_text(yaml.dump(schema_config, default_flow_style=False))
        return sql_path

    def generate_profiles_yml(
        self, org_id: int, connection_type: str, connection_config: dict
    ) -> Path:
        """Generate profiles.yml from a destination connection config. Returns path."""
        if connection_type not in SUPPORTED_ADAPTERS:
            raise DbtProjectError(f"Unsupported dbt adapter: {connection_type}")

        project_path = self.get_project_path(org_id)
        profile_name = f"tenant_{org_id}"

        profile = {
            profile_name: {
                "target": "default",
                "outputs": {
                    "default": {
                        "type": connection_type,
                        "host": connection_config.get("host", ""),
                        "port": connection_config.get("port", 5432),
                        "user": connection_config.get("user", ""),
                        "password": connection_config.get("password", ""),
                        "dbname": connection_config.get("database", ""),
                        "schema": connection_config.get("schema", "public"),
                        "threads": 4,
                    }
                },
            }
        }

        profiles_path = project_path / "profiles.yml"
        profiles_path.write_text(yaml.dump(profile, default_flow_style=False))
        return profiles_path

    def run_model(self, org_id: int, model_name: str) -> dict:
        """Execute `dbt run --select model_name` for the tenant project.

        Returns {"success": bool, "rows_affected": int, "logs": str}.
        """
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

        rows_affected = 0
        if result.result:
            for node_result in result.result:
                resp = getattr(node_result, "adapter_response", None)
                if resp and hasattr(resp, "rows_affected"):
                    rows_affected += resp.rows_affected or 0

        logs = str(result.result) if result.result else ""

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

        schema_yml_path.write_text(yaml.dump(schema_config, default_flow_style=False))
        return schema_yml_path

    def run_test(self, org_id: int, model_name: str) -> dict:
        """Execute `dbt test --select model_name` for the tenant project.

        Returns {"success": bool, "logs": str}.
        """
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
        return {
            "success": result.success,
            "logs": logs,
        }

    def compile_model(self, org_id: int, model_name: str) -> dict:
        """Execute `dbt compile --select model_name` for the tenant project.

        Returns {"success": bool, "compiled_sql": str, "logs": str}.
        """
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
                code = getattr(node_result, "compiled_code", None)
                if code:
                    compiled_sql = code
                    break

        logs = str(result.result) if result.result else ""
        return {
            "success": result.success,
            "compiled_sql": compiled_sql,
            "logs": logs,
        }

    def remove_model(self, org_id: int, model_name: str, schema_name: str = "staging") -> bool:
        """Delete a model .sql file. Returns True if file existed."""
        project_path = self.get_project_path(org_id)
        sql_path = project_path / "models" / schema_name / f"{model_name}.sql"
        if sql_path.exists():
            sql_path.unlink()
            return True
        return False
