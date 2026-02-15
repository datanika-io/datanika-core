"""Tests for explicit setter methods on all state classes."""

import inspect


class TestAuthStateSetters:
    def test_clear_auth_error_exists(self):
        from etlfabric.ui.state.auth_state import AuthState

        method = getattr(AuthState, "clear_auth_error", None)
        assert method is not None, "AuthState missing clear_auth_error"
        assert callable(method)

    def test_form_handlers_accept_form_data(self):
        from etlfabric.ui.state.auth_state import AuthState

        for name in ["login", "signup"]:
            handler = getattr(AuthState, name)
            fn = handler.fn if hasattr(handler, "fn") else handler
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert "form_data" in params, f"AuthState.{name} should accept form_data"


class TestConnectionStateSetters:
    def test_all_setters_exist(self):
        from etlfabric.ui.state.connection_state import ConnectionState

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
        from etlfabric.ui.state.connection_state import ConnectionState

        handler = ConnectionState.set_form_use_raw_json
        fn = handler.fn if hasattr(handler, "fn") else handler
        sig = inspect.signature(fn)
        params = sig.parameters
        assert "value" in params
        assert params["value"].annotation is bool


class TestPipelineStateSetters:
    def test_all_setters_exist(self):
        from etlfabric.ui.state.pipeline_state import PipelineState

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
        from etlfabric.ui.state.pipeline_state import PipelineState

        for name in ["set_form_enable_incremental", "set_form_use_raw_json"]:
            handler = getattr(PipelineState, name)
            fn = handler.fn if hasattr(handler, "fn") else handler
            sig = inspect.signature(fn)
            params = sig.parameters
            assert "value" in params
            assert params["value"].annotation is bool


class TestTransformationStateSetters:
    def test_all_setters_exist(self):
        from etlfabric.ui.state.transformation_state import TransformationState

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
        from etlfabric.ui.state.schedule_state import ScheduleState

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
        from etlfabric.ui.state.dag_state import DagState

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
        from etlfabric.ui.state.settings_state import SettingsState

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
        from etlfabric.ui.state.run_state import RunState

        expected = [
            "set_filter",
            "set_target_type_filter",
        ]
        for name in expected:
            method = getattr(RunState, name, None)
            assert method is not None, f"RunState missing {name}"
            assert callable(method)

    def test_filter_setters_are_async(self):
        from etlfabric.ui.state.run_state import RunState

        for name in ["set_filter", "set_target_type_filter"]:
            handler = getattr(RunState, name)
            fn = handler.fn if hasattr(handler, "fn") else handler
            assert inspect.iscoroutinefunction(fn), f"RunState.{name} should be async"
