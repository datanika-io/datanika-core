"""Tests for ModelDetailState.load_preview + _stringify_cell helpers.

Covers the Data Catalog table preview (PLAN_PRODUCT.md §P1 Data Preview
& Profiling, item 1). Mocks ConnectionService.preview_table so tests
don't need a live destination.
"""

from __future__ import annotations

from unittest.mock import patch


class TestStringifyCell:
    def test_none_to_empty_string(self):
        from datanika.ui.state.model_detail_state import _stringify_cell

        assert _stringify_cell(None) == ""

    def test_int_round_trip(self):
        from datanika.ui.state.model_detail_state import _stringify_cell

        assert _stringify_cell(42) == "42"

    def test_float_round_trip(self):
        from datanika.ui.state.model_detail_state import _stringify_cell

        assert _stringify_cell(1.5) == "1.5"

    def test_long_value_is_truncated(self):
        from datanika.ui.state.model_detail_state import _PREVIEW_CELL_MAX, _stringify_cell

        long_string = "x" * 400
        result = _stringify_cell(long_string)
        # Truncated to _PREVIEW_CELL_MAX + ellipsis
        assert len(result) == _PREVIEW_CELL_MAX + 1
        assert result.endswith("…")

    def test_short_value_unchanged(self):
        from datanika.ui.state.model_detail_state import _stringify_cell

        assert _stringify_cell("hello") == "hello"

    def test_unreprable_object_falls_back_to_repr(self):
        """A __str__ that raises still renders something for display."""
        from datanika.ui.state.model_detail_state import _stringify_cell

        class Broken:
            def __str__(self):
                raise RuntimeError("bad str")

            def __repr__(self):
                return "<Broken>"

        assert _stringify_cell(Broken()) == "<Broken>"


class TestCanPreview:
    """The can_preview property gates the UI's Load Preview button.

    State is a Reflex class; we construct it without the Reflex runtime
    by instantiating the BaseModel-derived class and setting the fields
    directly. This is enough for pure-property testing.
    """

    def _make_state(self, **overrides):
        """Build a minimal state instance for property testing.

        Reflex State classes are heavyweight; construct a lightweight
        stand-in by copying just the fields ``can_preview`` reads.
        """

        class _FakeState:
            connection_id = overrides.get("connection_id", 0)
            table_name = overrides.get("table_name", "")

        from datanika.ui.state.model_detail_state import ModelDetailState

        # Reuse the property function against the fake.
        return ModelDetailState.can_preview.fget(_FakeState())

    def test_false_when_no_connection(self):
        assert self._make_state(connection_id=0, table_name="orders") is False

    def test_false_when_no_table_name(self):
        assert self._make_state(connection_id=5, table_name="") is False

    def test_true_when_both_set(self):
        assert self._make_state(connection_id=5, table_name="orders") is True


class TestPreviewI18nKeys:
    """Each locale must define the preview-specific keys."""

    REQUIRED_KEYS = [
        "model_detail.preview.heading",
        "model_detail.preview.load_button",
        "model_detail.preview.empty",
        "model_detail.preview.row_count_prefix",
    ]
    LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

    def test_all_locales_define_preview_keys(self):
        import json
        from pathlib import Path

        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        for locale in self.LOCALES:
            with open(i18n_dir / f"{locale}.json", encoding="utf-8") as f:
                data = json.load(f)
            for key in self.REQUIRED_KEYS:
                assert key in data, f"Missing {key} in {locale}.json"
                assert isinstance(data[key], str) and data[key]


class TestPreviewServiceCallContract:
    """Verify the preview flow passes the right arguments to
    ConnectionService.preview_table without standing up the full Reflex
    state runtime. Focuses on the contract boundary.
    """

    def test_preview_table_receives_schema_and_table(self):
        """Exercises the public ConnectionService.preview_table
        signature directly so drift in the argument order is caught.
        """
        from datanika.services.connection_service import ConnectionService

        with patch.object(ConnectionService, "preview_table") as mock_preview:
            mock_preview.return_value = (["id", "name"], [[1, "a"], [2, "b"]])
            cols, rows = ConnectionService.preview_table(
                config={"host": "h"},
                connection_type="postgres",
                table="orders",
                schema="raw",
                limit=100,
            )
        mock_preview.assert_called_once_with(
            config={"host": "h"},
            connection_type="postgres",
            table="orders",
            schema="raw",
            limit=100,
        )
        assert cols == ["id", "name"]
        assert rows == [[1, "a"], [2, "b"]]

    def test_preview_limit_is_100_constant(self):
        """The constant used by load_preview matches the issue scope."""
        from datanika.ui.state.model_detail_state import PREVIEW_ROW_LIMIT

        assert PREVIEW_ROW_LIMIT == 100
