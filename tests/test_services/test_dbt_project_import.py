"""Version-2 import of a dbt project (SPEC_DBT_PROJECT_AND_DOWNSTREAM).

The document is the existing import: JSON on ``POST /api/v1/import`` and YAML on
``POST /api/v1/pipelines/yaml``. Models stay in ``transformations``. A refusal
creates nothing, and a failure leaves the project files the org already had.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.dependency import Dependency
from datanika.models.transformation import Transformation
from datanika.services.dbt_project import DbtProjectService
from tests.test_services.test_import_endpoint import _auth_headers, _patch_auth


@pytest.fixture
def fake_api_key():
    from unittest.mock import MagicMock

    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.user_id = 1
    key.name = "Test Key"
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    from datanika.services.rate_limit_service import RateLimitResult

    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def client():
    from datanika.services.api_v1_routes import api_v1_routes

    return TestClient(Starlette(routes=api_v1_routes))


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("datanika.config.settings.dbt_projects_dir", str(tmp_path))
    return tmp_path


def _project(projects_dir: Path, org_id: int = 10) -> Path:
    return projects_dir / f"tenant_{org_id}"


def _seed_existing(projects_dir: Path) -> Path:
    project = _project(projects_dir)
    (project / "macros").mkdir(parents=True)
    (project / "marker.txt").write_text("keep\n", encoding="utf-8")
    (project / "packages.yml").write_text(
        "packages:\n- package: keep/pkg\n  version: '0.1.0'\n",
        encoding="utf-8",
    )
    kept = project / "macros" / "generate_schema_name.sql"
    kept.write_text("-- owned by the project\n", encoding="utf-8")
    return project


def _document() -> dict:
    return {
        "version": 2,
        "transformations": [
            {
                "name": "stg_orders",
                "sql_body": "select 1 as id",
                "materialization": "view",
                "schema_name": "staging",
                "tests_config": {"columns": {"id": ["not_null"]}},
            }
        ],
        "macros": [
            {
                "name": "cents_to_dollars",
                "sql_body": "{% macro cents_to_dollars(v) %}{{ v }}{% endmacro %}\n",
            }
        ],
        "packages": [{"package": "dbt-labs/dbt_utils", "version": "1.3.0"}],
        "snapshots": [
            {
                "snapshot_name": "orders_snap",
                "sql_body": "select 1 as id, current_timestamp as updated_at",
                "unique_key": "id",
                "strategy": "timestamp",
                "updated_at": "updated_at",
                "target_schema": "snap_schema",
            }
        ],
        "tests": [{"name": "orders_positive", "sql_body": "select id from staging.stg_orders\n"}],
    }


def _post(client, payload, projects_dir, fake_api_key, rate_limit_ok):
    with (
        _patch_auth(fake_api_key, rate_limit_ok) as session,
        patch.object(
            DbtProjectService,
            "install_packages",
            return_value={"success": True, "logs": "installed"},
        ) as deps,
    ):
        resp = client.post("/api/v1/import", json=payload, headers=_auth_headers())
        # Count before the harness closes the session.
        transformations = session.query(Transformation).count()
        dependencies = session.query(Dependency).count()
        return resp, transformations, dependencies, deps


class TestVersion2ProjectImport:
    def test_one_document_lands_models_macros_packages_snapshots_and_tests(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        resp, transformations, dependencies, deps = _post(
            client, _document(), projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 201, resp.text
        project = _project(projects_dir)
        model = (project / "models" / "staging" / "stg_orders.sql").read_text(encoding="utf-8")
        assert model == "select 1 as id"
        yml = yaml.safe_load(
            (project / "models" / "staging" / "stg_orders.yml").read_text(encoding="utf-8")
        )
        columns = yml["models"][0]["columns"]
        assert columns == [{"name": "id", "tests": ["not_null"]}]
        macro = (project / "macros" / "cents_to_dollars.sql").read_text(encoding="utf-8")
        assert "cents_to_dollars" in macro
        packages = yaml.safe_load((project / "packages.yml").read_text(encoding="utf-8"))
        assert packages["packages"] == [{"package": "dbt-labs/dbt_utils", "version": "1.3.0"}]
        deps.assert_called_once()
        snap = (project / "snapshots" / "orders_snap.sql").read_text(encoding="utf-8")
        assert "{% snapshot orders_snap %}" in snap
        assert "strategy='timestamp'" in snap
        singular = (project / "tests" / "orders_positive.sql").read_text(encoding="utf-8")
        assert singular.startswith("select id")
        assert transformations == 1
        assert dependencies == 0
        assert not (project / "profiles.yml").exists()
        project_yml = yaml.safe_load((project / "dbt_project.yml").read_text(encoding="utf-8"))
        assert project_yml["name"] == "tenant_10"
        created = resp.json()["created"]
        assert "macros" not in created
        assert len(created["transformations"]) == 1

    def test_yaml_door_writes_the_same_files(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        body = yaml.safe_dump(_document())
        with (
            _patch_auth(fake_api_key, rate_limit_ok),
            patch.object(
                DbtProjectService,
                "install_packages",
                return_value={"success": True, "logs": "installed"},
            ),
        ):
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content=body,
                headers={
                    "Authorization": "Bearer etf_testkey",
                    "Content-Type": "application/yaml",
                },
            )
        assert resp.status_code == 201, resp.text
        assert (_project(projects_dir) / "macros" / "cents_to_dollars.sql").is_file()

    def test_a_snapshot_materialization_stays_a_transformation(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        payload = {
            "version": 2,
            "transformations": [
                {
                    "name": "orders_snapshot_model",
                    "sql_body": "select 1 as id",
                    "materialization": "snapshot",
                }
            ],
        }
        resp, _transformations, _dependencies, deps = _post(
            client, payload, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 201, resp.text
        project = _project(projects_dir)
        model = (project / "models" / "staging" / "orders_snapshot_model.sql").read_text(
            encoding="utf-8"
        )
        assert model == "select 1 as id"
        assert not (project / "snapshots" / "orders_snapshot_model.sql").exists()
        deps.assert_not_called()

    def test_absent_packages_key_leaves_packages_yml_and_skips_deps(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        project = _seed_existing(projects_dir)
        before = (project / "packages.yml").read_text(encoding="utf-8")
        payload = {
            "version": 2,
            "macros": [{"name": "kept_macro", "sql_body": "-- macro\n"}],
        }
        resp, _transformations, _dependencies, deps = _post(
            client, payload, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 201, resp.text
        assert (project / "packages.yml").read_text(encoding="utf-8") == before
        deps.assert_not_called()
        assert (project / "macros" / "generate_schema_name.sql").read_text(encoding="utf-8") == (
            "-- owned by the project\n"
        )

    def test_empty_package_list_replaces_packages_and_runs_deps(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        _seed_existing(projects_dir)
        resp, _transformations, _dependencies, deps = _post(
            client, {"version": 2, "packages": []}, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 201, resp.text
        packages = yaml.safe_load(
            (_project(projects_dir) / "packages.yml").read_text(encoding="utf-8")
        )
        assert packages["packages"] == []
        deps.assert_called_once()

    def test_deps_failure_leaves_the_project_and_creates_no_row(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        project = _seed_existing(projects_dir)
        before = (project / "packages.yml").read_text(encoding="utf-8")

        def _fail(self, org_id):
            dumped = self.get_project_path(org_id) / "dbt_packages"
            dumped.mkdir(parents=True, exist_ok=True)
            (dumped / "new.txt").write_text("nope", encoding="utf-8")
            return {"success": False, "logs": "deps failed"}

        payload = {
            "version": 2,
            "transformations": [{"name": "stg_orders", "sql_body": "select 1"}],
            "packages": [{"package": "dbt-labs/dbt_utils", "version": "1.3.0"}],
        }
        with (
            _patch_auth(fake_api_key, rate_limit_ok) as session,
            patch.object(DbtProjectService, "install_packages", _fail),
        ):
            resp = client.post("/api/v1/import", json=payload, headers=_auth_headers())
            assert resp.status_code == 400, resp.text
            assert session.query(Transformation).count() == 0
        assert (project / "packages.yml").read_text(encoding="utf-8") == before
        assert (project / "marker.txt").read_text(encoding="utf-8") == "keep\n"
        assert not (project / "models" / "staging" / "stg_orders.sql").exists()
        assert not (project / "dbt_packages").exists()


_REFUSAL_KEYS = ("project_archive", "git_remote", "git_url", "files", "seeds")


class TestImportRefusals:
    @pytest.mark.parametrize("key", _REFUSAL_KEYS)
    def test_named_key_creates_nothing(
        self, client, fake_api_key, rate_limit_ok, projects_dir, key
    ):
        project = _seed_existing(projects_dir)
        payload = {"version": 2, "macros": [{"name": "m", "sql_body": "-- m\n"}], key: []}
        resp, transformations, _dependencies, _deps = _post(
            client, payload, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 400, resp.text
        assert transformations == 0
        assert not (project / "macros" / "m.sql").exists()
        assert (project / "marker.txt").read_text(encoding="utf-8") == "keep\n"

    def test_a_git_package_creates_nothing(self, client, fake_api_key, rate_limit_ok, projects_dir):
        project = _seed_existing(projects_dir)
        before = (project / "packages.yml").read_text(encoding="utf-8")
        payload = {
            "version": 2,
            "packages": [{"git": "https://example.com/pkg.git", "revision": "main"}],
        }
        resp, _transformations, _dependencies, deps = _post(
            client, payload, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 400, resp.text
        assert (project / "packages.yml").read_text(encoding="utf-8") == before
        deps.assert_not_called()

    def test_a_hub_package_with_an_extra_key_creates_nothing(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        payload = {
            "version": 2,
            "packages": [{"package": "dbt-labs/dbt_utils", "version": "1.3.0", "revision": "main"}],
        }
        resp, _transformations, _dependencies, deps = _post(
            client, payload, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 400, resp.text
        deps.assert_not_called()
        assert not _project(projects_dir).exists()

    @pytest.mark.parametrize(
        "section,item",
        [
            ("transformations", {"name": "stg_orders.py", "sql_body": "select 1"}),
            ("macros", {"name": "cents.py", "sql_body": "-- m\n"}),
            ("tests", {"name": "orders.py", "sql_body": "select 1\n"}),
        ],
    )
    def test_a_py_name_creates_nothing(
        self, client, fake_api_key, rate_limit_ok, projects_dir, section, item
    ):
        resp, transformations, _dependencies, _deps = _post(
            client, {"version": 2, section: [item]}, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 400, resp.text
        assert transformations == 0
        assert not _project(projects_dir).exists()

    def test_generate_schema_name_macro_creates_nothing(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        project = _seed_existing(projects_dir)
        payload = {
            "version": 2,
            "macros": [
                {"name": "fine_macro", "sql_body": "-- fine\n"},
                {"name": "generate_schema_name", "sql_body": "-- replaced\n"},
            ],
        }
        resp, _transformations, _dependencies, _deps = _post(
            client, payload, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 400, resp.text
        assert not (project / "macros" / "fine_macro.sql").exists()
        assert (project / "macros" / "generate_schema_name.sql").read_text(encoding="utf-8") == (
            "-- owned by the project\n"
        )

    def test_a_name_outside_the_project_creates_nothing(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        payload = {
            "version": 2,
            "transformations": [
                {"name": "stg_orders", "sql_body": "select 1", "schema_name": "../outside"}
            ],
        }
        resp, transformations, _dependencies, _deps = _post(
            client, payload, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 400, resp.text
        assert transformations == 0
        assert not (projects_dir / "outside").exists()
        assert not _project(projects_dir).exists()

    def test_new_sections_count_toward_the_cap(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        macros = [{"name": f"m_{i}", "sql_body": "-- m\n"} for i in range(1001)]
        resp, _transformations, _dependencies, _deps = _post(
            client, {"version": 2, "macros": macros}, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 400, resp.text
        errors = resp.json()["errors"]
        assert len(errors) == 1
        assert errors[0]["code"] == "IMPORT_TOO_LARGE"
        assert not _project(projects_dir).exists()

    def test_file_sections_need_transformations_write(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        fake_api_key.scopes = ["connections:write"]
        resp, transformations, _dependencies, _deps = _post(
            client,
            {"version": 2, "macros": [{"name": "m", "sql_body": "-- m\n"}]},
            projects_dir,
            fake_api_key,
            rate_limit_ok,
        )
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["code"] == "insufficient_scope"
        assert "transformations:write" in resp.json()["error"]["required_scopes"]
        assert transformations == 0
        assert not _project(projects_dir).exists()

    def test_an_empty_package_list_still_needs_the_scope(
        self, client, fake_api_key, rate_limit_ok, projects_dir
    ):
        fake_api_key.scopes = ["connections:write"]
        resp, _transformations, _dependencies, deps = _post(
            client, {"version": 2, "packages": []}, projects_dir, fake_api_key, rate_limit_ok
        )
        assert resp.status_code == 403, resp.text
        deps.assert_not_called()
        assert not _project(projects_dir).exists()

    def test_published_schema_names_the_four_sections(self):
        from datanika.services.openapi import build_openapi_spec

        props = build_openapi_spec()["components"]["schemas"]["ImportRequest"]["properties"]
        assert set(("macros", "packages", "snapshots", "tests")).issubset(props)
