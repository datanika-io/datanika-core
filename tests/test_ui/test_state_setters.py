"""Tests for explicit setter methods on all state classes."""

import inspect


class TestAuthStateSetters:
    def test_clear_auth_error_exists(self):
        from datanika.ui.state.auth_state import AuthState

        method = getattr(AuthState, "clear_auth_error", None)
        assert method is not None, "AuthState missing clear_auth_error"
        assert callable(method)

    def test_form_handlers_accept_form_data(self):
        from datanika.ui.state.auth_state import AuthState

        for name in ["login", "signup"]:
            handler = getattr(AuthState, name)
            fn = handler.fn if hasattr(handler, "fn") else handler
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert "form_data" in params, f"AuthState.{name} should accept form_data"


class TestConnectionFormValidation:
    """Tests for _validate_connection_form required-field checks."""

    def _validate(self, **kwargs):
        from datanika.ui.state.connection_state import _validate_connection_form

        return _validate_connection_form(**kwargs)

    # -- Connection name always required --

    def test_empty_name_rejected(self):
        err = self._validate(name="", conn_type="postgres", use_raw_json=False)
        assert "name is required" in err.lower()

    def test_whitespace_name_rejected(self):
        err = self._validate(name="   ", conn_type="postgres", use_raw_json=False)
        assert "name is required" in err.lower()

    # -- Raw JSON skips type-specific checks --

    def test_raw_json_skips_field_checks(self):
        err = self._validate(name="My Conn", conn_type="postgres", use_raw_json=True)
        assert err == ""

    # -- DB types (postgres, mysql, mssql, redshift) --

    def test_db_missing_host(self):
        for t in ("postgres", "mysql", "mssql", "redshift"):
            err = self._validate(
                name="X", conn_type=t, use_raw_json=False,
                host="", port="5432", database="db",
            )
            assert "Host is required" in err, f"Failed for {t}"

    def test_db_missing_port(self):
        err = self._validate(
            name="X", conn_type="postgres", use_raw_json=False,
            host="localhost", port="", database="db",
        )
        assert "Port is required" in err

    def test_db_missing_database(self):
        err = self._validate(
            name="X", conn_type="mysql", use_raw_json=False,
            host="localhost", port="3306", database="",
        )
        assert "Database is required" in err

    def test_db_valid(self):
        err = self._validate(
            name="X", conn_type="postgres", use_raw_json=False,
            host="localhost", port="5432", database="mydb",
        )
        assert err == ""

    # -- SQLite --

    def test_sqlite_missing_path(self):
        err = self._validate(
            name="X", conn_type="sqlite", use_raw_json=False, path="",
        )
        assert "path is required" in err.lower()

    def test_sqlite_valid(self):
        err = self._validate(
            name="X", conn_type="sqlite", use_raw_json=False, path="/data/my.db",
        )
        assert err == ""

    # -- BigQuery --

    def test_bigquery_missing_project(self):
        err = self._validate(
            name="X", conn_type="bigquery", use_raw_json=False,
            project="", dataset="raw",
        )
        assert "Project ID is required" in err

    def test_bigquery_missing_dataset(self):
        err = self._validate(
            name="X", conn_type="bigquery", use_raw_json=False,
            project="proj", dataset="",
        )
        assert "Dataset is required" in err

    def test_bigquery_valid(self):
        err = self._validate(
            name="X", conn_type="bigquery", use_raw_json=False,
            project="proj", dataset="raw",
        )
        assert err == ""

    # -- Snowflake --

    def test_snowflake_missing_account(self):
        err = self._validate(
            name="X", conn_type="snowflake", use_raw_json=False,
            account="", user="u", database="db",
        )
        assert "Account is required" in err

    def test_snowflake_missing_user(self):
        err = self._validate(
            name="X", conn_type="snowflake", use_raw_json=False,
            account="acct", user="", database="db",
        )
        assert "User is required" in err

    def test_snowflake_missing_database(self):
        err = self._validate(
            name="X", conn_type="snowflake", use_raw_json=False,
            account="acct", user="u", database="",
        )
        assert "Database is required" in err

    def test_snowflake_valid(self):
        err = self._validate(
            name="X", conn_type="snowflake", use_raw_json=False,
            account="acct", user="u", database="db",
        )
        assert err == ""

    # -- S3 --

    def test_s3_missing_bucket_url(self):
        err = self._validate(
            name="X", conn_type="s3", use_raw_json=False, bucket_url="",
        )
        assert "Bucket URL is required" in err

    def test_s3_valid(self):
        err = self._validate(
            name="X", conn_type="s3", use_raw_json=False,
            bucket_url="s3://my-bucket",
        )
        assert err == ""

    # -- File types (csv, json, parquet) --

    def test_file_type_missing_path(self):
        for t in ("csv", "json", "parquet"):
            err = self._validate(
                name="X", conn_type=t, use_raw_json=False, bucket_url="",
            )
            assert "path is required" in err.lower(), f"Failed for {t}"

    def test_file_type_valid(self):
        err = self._validate(
            name="X", conn_type="csv", use_raw_json=False,
            bucket_url="/data/files",
        )
        assert err == ""

    # -- REST API --

    def test_rest_api_missing_base_url(self):
        err = self._validate(
            name="X", conn_type="rest_api", use_raw_json=False, base_url="",
        )
        assert "Base URL is required" in err

    def test_rest_api_valid(self):
        err = self._validate(
            name="X", conn_type="rest_api", use_raw_json=False,
            base_url="https://api.example.com",
        )
        assert err == ""


class TestConnectionStateTestMethods:
    def test_test_connection_from_form_exists(self):
        from datanika.ui.state.connection_state import ConnectionState

        method = getattr(ConnectionState, "test_connection_from_form", None)
        assert method is not None, "ConnectionState missing test_connection_from_form"
        fn = method.fn if hasattr(method, "fn") else method
        assert inspect.iscoroutinefunction(fn)

    def test_test_saved_connection_exists(self):
        from datanika.ui.state.connection_state import ConnectionState

        method = getattr(ConnectionState, "test_saved_connection", None)
        assert method is not None, "ConnectionState missing test_saved_connection"
        fn = method.fn if hasattr(method, "fn") else method
        assert inspect.iscoroutinefunction(fn)

    def test_test_state_vars_exist(self):
        from datanika.ui.state.connection_state import ConnectionState

        fields = ConnectionState.get_fields()
        assert "test_message" in fields, "ConnectionState missing test_message"
        assert "test_success" in fields, "ConnectionState missing test_success"


class TestConnectionStateSetters:
    def test_all_setters_exist(self):
        from datanika.ui.state.connection_state import ConnectionState

        expected = [
            "set_form_name",
            "set_form_type",
            "set_form_config",
            "set_form_host",
            "set_form_port",
            "set_form_user",
            "set_form_password",
            "set_form_database",
            "set_form_schema",
            "set_form_path",
            "set_form_project",
            "set_form_dataset",
            "set_form_keyfile_json",
            "set_form_account",
            "set_form_warehouse",
            "set_form_role",
            "set_form_bucket_url",
            "set_form_aws_access_key_id",
            "set_form_aws_secret_access_key",
            "set_form_region_name",
            "set_form_endpoint_url",
            "set_form_base_url",
            "set_form_api_key",
            "set_form_extra_headers",
            "set_form_use_raw_json",
        ]
        for name in expected:
            method = getattr(ConnectionState, name, None)
            assert method is not None, f"ConnectionState missing {name}"
            assert callable(method)

    def test_bool_setter_signature(self):
        from datanika.ui.state.connection_state import ConnectionState

        handler = ConnectionState.set_form_use_raw_json
        fn = handler.fn if hasattr(handler, "fn") else handler
        sig = inspect.signature(fn)
        params = sig.parameters
        assert "value" in params
        assert params["value"].annotation is bool


class TestPipelineStateSetters:
    def test_all_setters_exist(self):
        from datanika.ui.state.pipeline_state import PipelineState

        expected = [
            "set_form_name",
            "set_form_description",
            "set_form_source_id",
            "set_form_dest_id",
            "set_form_mode",
            "set_form_write_disposition",
            "set_form_primary_key",
            "set_form_table",
            "set_form_source_schema",
            "set_form_table_names",
            "set_form_batch_size",
            "set_form_enable_incremental",
            "set_form_cursor_path",
            "set_form_initial_value",
            "set_form_row_order",
            "set_form_sc_tables",
            "set_form_sc_columns",
            "set_form_sc_data_type",
            "set_form_config",
            "set_form_use_raw_json",
        ]
        for name in expected:
            method = getattr(PipelineState, name, None)
            assert method is not None, f"PipelineState missing {name}"
            assert callable(method)

    def test_bool_setter_signatures(self):
        from datanika.ui.state.pipeline_state import PipelineState

        for name in ["set_form_enable_incremental", "set_form_use_raw_json"]:
            handler = getattr(PipelineState, name)
            fn = handler.fn if hasattr(handler, "fn") else handler
            sig = inspect.signature(fn)
            params = sig.parameters
            assert "value" in params
            assert params["value"].annotation is bool


class TestTransformationStateSetters:
    def test_all_setters_exist(self):
        from datanika.ui.state.transformation_state import TransformationState

        expected = [
            "set_form_name",
            "set_form_sql_body",
            "set_form_materialization",
            "set_form_description",
            "set_form_schema_name",
            "set_form_tests_config",
        ]
        for name in expected:
            method = getattr(TransformationState, name, None)
            assert method is not None, f"TransformationState missing {name}"
            assert callable(method)


class TestScheduleStateSetters:
    def test_all_setters_exist(self):
        from datanika.ui.state.schedule_state import ScheduleState

        expected = [
            "set_form_target_type",
            "set_form_target_id",
            "set_form_cron",
            "set_form_timezone",
        ]
        for name in expected:
            method = getattr(ScheduleState, name, None)
            assert method is not None, f"ScheduleState missing {name}"
            assert callable(method)


class TestDagStateSetters:
    def test_all_setters_exist(self):
        from datanika.ui.state.dag_state import DagState

        expected = [
            "set_form_upstream_type",
            "set_form_upstream_id",
            "set_form_downstream_type",
            "set_form_downstream_id",
        ]
        for name in expected:
            method = getattr(DagState, name, None)
            assert method is not None, f"DagState missing {name}"
            assert callable(method)


class TestSettingsStateSetters:
    def test_all_setters_exist(self):
        from datanika.ui.state.settings_state import SettingsState

        expected = [
            "set_edit_org_name",
            "set_edit_org_slug",
            "set_invite_email",
            "set_invite_role",
        ]
        for name in expected:
            method = getattr(SettingsState, name, None)
            assert method is not None, f"SettingsState missing {name}"
            assert callable(method)


class TestRunStateSetters:
    def test_filter_setters_exist(self):
        from datanika.ui.state.run_state import RunState

        expected = [
            "set_filter",
            "set_target_type_filter",
        ]
        for name in expected:
            method = getattr(RunState, name, None)
            assert method is not None, f"RunState missing {name}"
            assert callable(method)

    def test_filter_setters_are_async(self):
        from datanika.ui.state.run_state import RunState

        for name in ["set_filter", "set_target_type_filter"]:
            handler = getattr(RunState, name)
            fn = handler.fn if hasattr(handler, "fn") else handler
            assert inspect.iscoroutinefunction(fn), f"RunState.{name} should be async"
