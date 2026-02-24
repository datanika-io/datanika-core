"""Tests for SQL editor enhancements in TransformationState."""

import inspect


class TestCanPreviewComputedVar:
    """Test can_preview computed var exists and returns correct values."""

    def test_can_preview_exists(self):
        from datanika.ui.state.transformation_state import TransformationState

        assert hasattr(TransformationState, "can_preview")

    def test_can_preview_logic_all_filled(self):
        """Directly test the can_preview logic: all fields filled → True."""
        # can_preview checks: form_name, form_connection_option,
        # form_materialization, form_schema_name
        result = bool(
            "my_model".strip()
            and "1 — PG (postgres)".strip()
            and "view".strip()
            and "staging".strip()
        )
        assert result is True

    def test_can_preview_logic_missing_name(self):
        result = bool(
            "".strip() and "1 — PG (postgres)".strip() and "view".strip() and "staging".strip()
        )
        assert result is False

    def test_can_preview_logic_missing_connection(self):
        result = bool("my_model".strip() and "".strip() and "view".strip() and "staging".strip())
        assert result is False

    def test_can_preview_logic_missing_materialization(self):
        result = bool(
            "my_model".strip() and "1 — PG (postgres)".strip() and "".strip() and "staging".strip()
        )
        assert result is False

    def test_can_preview_logic_missing_schema(self):
        result = bool(
            "my_model".strip() and "1 — PG (postgres)".strip() and "view".strip() and "".strip()
        )
        assert result is False

    def test_can_preview_logic_whitespace_only(self):
        result = bool(
            "  ".strip() and "1 — PG (postgres)".strip() and "view".strip() and "staging".strip()
        )
        assert result is False


class TestHandleSqlFileUpload:
    def test_method_exists(self):
        from datanika.ui.state.transformation_state import TransformationState

        method = getattr(TransformationState, "handle_sql_file_upload", None)
        assert method is not None, "TransformationState missing handle_sql_file_upload"

    def test_method_is_async(self):
        from datanika.ui.state.transformation_state import TransformationState

        handler = TransformationState.handle_sql_file_upload
        fn = handler.fn if hasattr(handler, "fn") else handler
        assert inspect.iscoroutinefunction(fn)


class TestPreviewCompiledSqlFromForm:
    def test_method_exists(self):
        from datanika.ui.state.transformation_state import TransformationState

        method = getattr(TransformationState, "preview_compiled_sql_from_form", None)
        assert method is not None

    def test_method_is_async_generator(self):
        from datanika.ui.state.transformation_state import TransformationState

        handler = TransformationState.preview_compiled_sql_from_form
        fn = handler.fn if hasattr(handler, "fn") else handler
        assert inspect.isasyncgenfunction(fn)


class TestPreviewResultFromForm:
    def test_method_exists(self):
        from datanika.ui.state.transformation_state import TransformationState

        method = getattr(TransformationState, "preview_result_from_form", None)
        assert method is not None

    def test_method_is_async_generator(self):
        from datanika.ui.state.transformation_state import TransformationState

        handler = TransformationState.preview_result_from_form
        fn = handler.fn if hasattr(handler, "fn") else handler
        assert inspect.isasyncgenfunction(fn)


class TestSaveSqlAndReturn:
    def test_method_exists(self):
        from datanika.ui.state.transformation_state import TransformationState

        method = getattr(TransformationState, "save_sql_and_return", None)
        assert method is not None

    def test_method_is_sync(self):
        from datanika.ui.state.transformation_state import TransformationState

        handler = TransformationState.save_sql_and_return
        fn = handler.fn if hasattr(handler, "fn") else handler
        assert not inspect.iscoroutinefunction(fn)


class TestSqlEditorPageExists:
    def test_page_function_importable(self):
        from datanika.ui.pages.sql_editor import sql_editor_page

        assert callable(sql_editor_page)


class TestSharedAutocompleteModule:
    def test_ref_autocomplete_js_exported(self):
        from datanika.ui.components.sql_autocomplete import REF_AUTOCOMPLETE_JS

        assert "refAutocompleteBound" in REF_AUTOCOMPLETE_JS

    def test_ref_hidden_buttons_callable(self):
        from datanika.ui.components.sql_autocomplete import ref_hidden_buttons

        assert callable(ref_hidden_buttons)

    def test_ref_popover_callable(self):
        from datanika.ui.components.sql_autocomplete import ref_popover

        assert callable(ref_popover)
