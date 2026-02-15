"""TDD tests for DbtProjectService — dbt project scaffolding & execution."""

from unittest.mock import MagicMock, patch

import pytest
import yaml

from etlfabric.services.dbt_project import DbtProjectError, DbtProjectService


@pytest.fixture
def svc(tmp_path):
    return DbtProjectService(str(tmp_path))


@pytest.fixture
def projects_dir(tmp_path):
    return tmp_path


# ---------------------------------------------------------------------------
# get_project_path
# ---------------------------------------------------------------------------
class TestGetProjectPath:
    def test_correct_path_format(self, svc, projects_dir):
        path = svc.get_project_path(1)
        assert path == projects_dir / "tenant_1"

    def test_different_org_ids_produce_different_paths(self, svc, projects_dir):
        p1 = svc.get_project_path(1)
        p2 = svc.get_project_path(2)
        assert p1 != p2
        assert p1.name == "tenant_1"
        assert p2.name == "tenant_2"


# ---------------------------------------------------------------------------
# ensure_project
# ---------------------------------------------------------------------------
class TestEnsureProject:
    def test_creates_directory_structure(self, svc):
        path = svc.ensure_project(1)
        assert path.is_dir()
        assert (path / "models").is_dir()
        assert (path / "macros").is_dir()
        assert (path / "tests").is_dir()

    def test_creates_dbt_project_yml(self, svc):
        path = svc.ensure_project(1)
        yml_path = path / "dbt_project.yml"
        assert yml_path.exists()
        content = yaml.safe_load(yml_path.read_text())
        assert content["name"] == "tenant_1"

    def test_idempotent(self, svc):
        path1 = svc.ensure_project(1)
        path2 = svc.ensure_project(1)
        assert path1 == path2
        assert path1.is_dir()

    def test_dbt_project_yml_has_correct_content(self, svc):
        path = svc.ensure_project(5)
        content = yaml.safe_load((path / "dbt_project.yml").read_text())
        assert content["name"] == "tenant_5"
        assert content["profile"] == "tenant_5"
        assert content["config-version"] == 2
        assert "models" in content["model-paths"]


# ---------------------------------------------------------------------------
# write_model
# ---------------------------------------------------------------------------
class TestWriteModel:
    def test_creates_sql_file_with_correct_content(self, svc):
        svc.ensure_project(1)
        path = svc.write_model(1, "my_model", "SELECT 1 AS id")
        assert path.exists()
        assert path.read_text() == "SELECT 1 AS id"

    def test_creates_in_correct_schema_subdirectory(self, svc):
        svc.ensure_project(1)
        path = svc.write_model(1, "my_model", "SELECT 1", schema_name="raw")
        assert "raw" in str(path)
        assert path.parent.name == "raw"

    def test_creates_schema_yml_with_materialization(self, svc):
        svc.ensure_project(1)
        svc.write_model(1, "my_model", "SELECT 1", materialization="table")
        schema_path = svc.get_project_path(1) / "models" / "staging" / "schema.yml"
        assert schema_path.exists()
        content = yaml.safe_load(schema_path.read_text())
        assert content["models"][0]["name"] == "my_model"
        assert content["models"][0]["config"]["materialized"] == "table"

    def test_overwrites_existing_model(self, svc):
        svc.ensure_project(1)
        svc.write_model(1, "my_model", "SELECT 1")
        path = svc.write_model(1, "my_model", "SELECT 2 AS updated")
        assert path.read_text() == "SELECT 2 AS updated"

    def test_handles_special_characters_in_name(self, svc):
        svc.ensure_project(1)
        path = svc.write_model(1, "my-model_v2", "SELECT 1")
        assert path.exists()
        assert path.name == "my-model_v2.sql"


# ---------------------------------------------------------------------------
# generate_profiles_yml
# ---------------------------------------------------------------------------
class TestGenerateProfilesYml:
    def test_postgres_profile(self, svc):
        svc.ensure_project(1)
        path = svc.generate_profiles_yml(
            1,
            "postgres",
            {
                "host": "localhost",
                "port": 5432,
                "user": "admin",
                "password": "secret",
                "database": "mydb",
                "schema": "public",
            },
        )
        assert path.exists()
        content = yaml.safe_load(path.read_text())
        profile = content["tenant_1"]["outputs"]["default"]
        assert profile["type"] == "postgres"
        assert profile["host"] == "localhost"

    def test_mysql_profile(self, svc):
        svc.ensure_project(1)
        path = svc.generate_profiles_yml(
            1,
            "mysql",
            {
                "host": "localhost",
                "port": 3306,
                "user": "admin",
                "password": "secret",
                "database": "mydb",
                "schema": "public",
            },
        )
        content = yaml.safe_load(path.read_text())
        profile = content["tenant_1"]["outputs"]["default"]
        assert profile["type"] == "mysql"

    def test_unsupported_type_raises(self, svc):
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError, match="Unsupported dbt adapter"):
            svc.generate_profiles_yml(1, "oracle", {})

    def test_file_written_to_correct_path(self, svc):
        svc.ensure_project(1)
        path = svc.generate_profiles_yml(
            1,
            "postgres",
            {
                "host": "h",
                "port": 5432,
                "user": "u",
                "password": "p",
                "database": "d",
                "schema": "s",
            },
        )
        assert path.name == "profiles.yml"
        assert path.parent == svc.get_project_path(1)


# ---------------------------------------------------------------------------
# run_model
# ---------------------------------------------------------------------------
class TestRunModel:
    @patch("etlfabric.services.dbt_project.dbtRunner")
    def test_success_returns_results_dict(self, mock_runner_cls, svc):
        svc.ensure_project(1)
        svc.write_model(1, "test_model", "SELECT 1")
        svc.generate_profiles_yml(
            1,
            "postgres",
            {
                "host": "h",
                "port": 5432,
                "user": "u",
                "password": "p",
                "database": "d",
                "schema": "s",
            },
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.result = [MagicMock(adapter_response=MagicMock(rows_affected=10))]
        mock_runner_cls.return_value.invoke.return_value = mock_result

        result = svc.run_model(1, "test_model")
        assert result["success"] is True
        assert result["rows_affected"] == 10
        assert "logs" in result

    @patch("etlfabric.services.dbt_project.dbtRunner")
    def test_failure_returns_success_false(self, mock_runner_cls, svc):
        svc.ensure_project(1)
        svc.write_model(1, "test_model", "SELECT 1")
        svc.generate_profiles_yml(
            1,
            "postgres",
            {
                "host": "h",
                "port": 5432,
                "user": "u",
                "password": "p",
                "database": "d",
                "schema": "s",
            },
        )

        mock_result = MagicMock()
        mock_result.success = False
        mock_result.result = []
        mock_runner_cls.return_value.invoke.return_value = mock_result

        result = svc.run_model(1, "test_model")
        assert result["success"] is False

    @patch("etlfabric.services.dbt_project.dbtRunner")
    def test_passes_correct_select_flag(self, mock_runner_cls, svc):
        svc.ensure_project(1)
        svc.write_model(1, "test_model", "SELECT 1")
        svc.generate_profiles_yml(
            1,
            "postgres",
            {
                "host": "h",
                "port": 5432,
                "user": "u",
                "password": "p",
                "database": "d",
                "schema": "s",
            },
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.result = []
        mock_runner_cls.return_value.invoke.return_value = mock_result

        svc.run_model(1, "test_model")
        invoke_args = mock_runner_cls.return_value.invoke.call_args[0][0]
        assert "--select" in invoke_args
        idx = invoke_args.index("--select")
        assert invoke_args[idx + 1] == "test_model"

    def test_project_not_found_raises(self, svc):
        with pytest.raises(DbtProjectError, match="project not found"):
            svc.run_model(999, "nonexistent")


# ---------------------------------------------------------------------------
# remove_model
# ---------------------------------------------------------------------------
class TestRemoveModel:
    def test_removes_existing_file(self, svc):
        svc.ensure_project(1)
        path = svc.write_model(1, "my_model", "SELECT 1")
        assert path.exists()
        result = svc.remove_model(1, "my_model")
        assert result is True
        assert not path.exists()

    def test_returns_false_for_nonexistent(self, svc):
        svc.ensure_project(1)
        result = svc.remove_model(1, "nonexistent")
        assert result is False

    def test_handles_missing_schema_directory(self, svc):
        svc.ensure_project(1)
        result = svc.remove_model(1, "model", schema_name="nonexistent_schema")
        assert result is False
