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

    def remove_model(self, org_id: int, model_name: str, schema_name: str = "staging") -> bool:
        """Delete a model .sql file. Returns True if file existed."""
        project_path = self.get_project_path(org_id)
        sql_path = project_path / "models" / schema_name / f"{model_name}.sql"
        if sql_path.exists():
            sql_path.unlink()
            return True
        return False
