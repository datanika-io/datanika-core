"""Tests for column-level editing state handlers in ModelDetailState."""

from datanika.ui.state.model_detail_state import (
    ColumnItem,
    _recompute_columns,
    _validate_column_tests,
)


# ---------------------------------------------------------------------------
# _recompute_columns — sets display fields from tests list
# ---------------------------------------------------------------------------
class TestColumnRecompute:
    def test_empty_tests_all_false(self):
        cols = [ColumnItem(name="id", data_type="INT", tests=[])]
        result = _recompute_columns(cols)
        assert result[0].has_not_null is False
        assert result[0].has_unique is False
        assert result[0].accepted_values_csv == ""
        assert result[0].relationship_to == ""
        assert result[0].relationship_field == ""

    def test_not_null_detected(self):
        cols = [ColumnItem(name="id", tests=["not_null"])]
        result = _recompute_columns(cols)
        assert result[0].has_not_null is True
        assert result[0].has_unique is False

    def test_unique_detected(self):
        cols = [ColumnItem(name="id", tests=["unique"])]
        result = _recompute_columns(cols)
        assert result[0].has_unique is True

    def test_both_not_null_and_unique(self):
        cols = [ColumnItem(name="id", tests=["not_null", "unique"])]
        result = _recompute_columns(cols)
        assert result[0].has_not_null is True
        assert result[0].has_unique is True

    def test_accepted_values_csv(self):
        cols = [ColumnItem(
            name="status",
            tests=[{"accepted_values": {"values": ["active", "inactive", "pending"]}}],
        )]
        result = _recompute_columns(cols)
        assert result[0].accepted_values_csv == "active, inactive, pending"

    def test_relationship_fields(self):
        cols = [ColumnItem(
            name="user_id",
            tests=[{"relationships": {"to": "ref('users')", "field": "id"}}],
        )]
        result = _recompute_columns(cols)
        assert result[0].relationship_to == "ref('users')"
        assert result[0].relationship_field == "id"

    def test_mixed_tests(self):
        cols = [ColumnItem(
            name="user_id",
            tests=[
                "not_null",
                {"relationships": {"to": "ref('users')", "field": "id"}},
            ],
        )]
        result = _recompute_columns(cols)
        assert result[0].has_not_null is True
        assert result[0].relationship_to == "ref('users')"

    def test_multiple_columns(self):
        cols = [
            ColumnItem(name="id", tests=["not_null", "unique"]),
            ColumnItem(name="status", tests=[
                {"accepted_values": {"values": ["a", "b"]}},
            ]),
        ]
        result = _recompute_columns(cols)
        assert result[0].has_not_null is True
        assert result[0].has_unique is True
        assert result[1].accepted_values_csv == "a, b"

    def test_dbt_utils_test_preserved(self):
        cols = [ColumnItem(
            name="amount",
            tests=[
                "not_null",
                {"dbt_utils.expression_is_true": {"expression": "amount > 0"}},
            ],
        )]
        result = _recompute_columns(cols)
        assert result[0].has_not_null is True
        # dbt_utils tests don't affect simple booleans
        assert result[0].has_unique is False

    def test_additional_tests_populated(self):
        cols = [ColumnItem(
            name="amount",
            tests=[
                "not_null",
                {"dbt_utils.expression_is_true": {"expression": "amount > 0"}},
                {"dbt_utils.accepted_range": {"min_value": 0, "max_value": 100}},
            ],
        )]
        result = _recompute_columns(cols)
        assert result[0].additional_tests == [
            "dbt_utils.expression_is_true",
            "dbt_utils.accepted_range",
        ]

    def test_additional_tests_includes_all_dict_tests(self):
        """All dict tests (including accepted_values/relationships) appear in additional_tests."""
        cols = [ColumnItem(
            name="status",
            tests=[
                "not_null",
                {"accepted_values": {"values": ["a", "b"]}},
                {"relationships": {"to": "ref('users')", "field": "id"}},
                {"dbt_utils.not_constant": {}},
            ],
        )]
        result = _recompute_columns(cols)
        assert result[0].additional_tests == [
            "accepted_values",
            "relationships",
            "dbt_utils.not_constant",
        ]

    def test_additional_tests_empty_when_no_custom(self):
        cols = [ColumnItem(name="id", tests=["not_null", "unique"])]
        result = _recompute_columns(cols)
        assert result[0].additional_tests == []


# ---------------------------------------------------------------------------
# Toggle tests — not_null on/off, unique on/off
# ---------------------------------------------------------------------------
class TestToggleTests:
    def test_toggle_not_null_on(self):
        col = ColumnItem(name="id", tests=[])
        cols = _recompute_columns([col])
        # Simulate toggling on
        tests = list(cols[0].tests)
        tests.append("not_null")
        updated = _recompute_columns([cols[0].model_copy(update={"tests": tests})])
        assert updated[0].has_not_null is True

    def test_toggle_not_null_off(self):
        col = ColumnItem(name="id", tests=["not_null", "unique"])
        tests = [t for t in col.tests if t != "not_null"]
        updated = _recompute_columns([col.model_copy(update={"tests": tests})])
        assert updated[0].has_not_null is False
        assert updated[0].has_unique is True

    def test_toggle_unique_on(self):
        col = ColumnItem(name="id", tests=["not_null"])
        tests = list(col.tests) + ["unique"]
        updated = _recompute_columns([col.model_copy(update={"tests": tests})])
        assert updated[0].has_unique is True
        assert updated[0].has_not_null is True

    def test_toggle_unique_off_preserves_others(self):
        col = ColumnItem(name="id", tests=["not_null", "unique"])
        tests = [t for t in col.tests if t != "unique"]
        updated = _recompute_columns([col.model_copy(update={"tests": tests})])
        assert updated[0].has_unique is False
        assert updated[0].has_not_null is True


# ---------------------------------------------------------------------------
# Accepted values — CSV parsing
# ---------------------------------------------------------------------------
class TestAcceptedValues:
    def test_csv_parsing(self):
        values = ["active", "inactive", "pending"]
        col = ColumnItem(
            name="status",
            tests=[{"accepted_values": {"values": values}}],
        )
        result = _recompute_columns([col])
        assert result[0].accepted_values_csv == "active, inactive, pending"

    def test_whitespace_trimmed(self):
        csv_input = "  a ,  b , c  "
        values = [v.strip() for v in csv_input.split(",") if v.strip()]
        assert values == ["a", "b", "c"]

    def test_empty_csv_removes_test(self):
        csv_input = ""
        values = [v.strip() for v in csv_input.split(",") if v.strip()]
        assert values == []
        col = ColumnItem(name="status", tests=[])
        result = _recompute_columns([col])
        assert result[0].accepted_values_csv == ""


# ---------------------------------------------------------------------------
# Relationships — set to/field, clear removes
# ---------------------------------------------------------------------------
class TestRelationships:
    def test_set_relationship(self):
        col = ColumnItem(
            name="user_id",
            tests=[{"relationships": {"to": "ref('users')", "field": "id"}}],
        )
        result = _recompute_columns([col])
        assert result[0].relationship_to == "ref('users')"
        assert result[0].relationship_field == "id"

    def test_clear_relationship(self):
        col = ColumnItem(name="user_id", tests=[])
        result = _recompute_columns([col])
        assert result[0].relationship_to == ""
        assert result[0].relationship_field == ""


# ---------------------------------------------------------------------------
# Custom tests — add/remove dbt_utils tests
# ---------------------------------------------------------------------------
class TestCustomTests:
    def test_add_expression_is_true(self):
        test_entry = {"dbt_utils.expression_is_true": {"expression": "amount > 0"}}
        col = ColumnItem(name="amount", tests=[test_entry])
        result = _recompute_columns([col])
        assert len(result[0].tests) == 1
        assert "dbt_utils.expression_is_true" in result[0].tests[0]

    def test_add_accepted_range(self):
        test_entry = {"dbt_utils.accepted_range": {"min_value": 0, "max_value": 100}}
        col = ColumnItem(name="score", tests=[test_entry])
        result = _recompute_columns([col])
        assert len(result[0].tests) == 1

    def test_add_not_constant(self):
        test_entry = {"dbt_utils.not_constant": {}}
        col = ColumnItem(name="status", tests=[test_entry])
        result = _recompute_columns([col])
        assert len(result[0].tests) == 1

    def test_remove_custom_test(self):
        tests = [
            "not_null",
            {"dbt_utils.expression_is_true": {"expression": "x > 0"}},
        ]
        col = ColumnItem(name="x", tests=tests)
        # Remove the custom test using the display key (same as UI passes)
        from datanika.ui.state.model_detail_state import ModelDetailState
        updated = ModelDetailState._remove_test_by_display(
            col, "dbt_utils.expression_is_true"
        )
        updated = _recompute_columns([updated])
        assert updated[0].has_not_null is True
        assert len(updated[0].tests) == 1
        assert updated[0].additional_tests == []

    def test_remove_preserves_other_custom_tests(self):
        tests = [
            "not_null",
            {"dbt_utils.expression_is_true": {"expression": "x > 0"}},
            {"dbt_utils.not_constant": {}},
        ]
        col = ColumnItem(name="x", tests=tests)
        from datanika.ui.state.model_detail_state import ModelDetailState
        updated = ModelDetailState._remove_test_by_display(
            col, "dbt_utils.expression_is_true"
        )
        updated = _recompute_columns([updated])
        assert updated[0].has_not_null is True
        assert len(updated[0].tests) == 2
        assert updated[0].additional_tests == ["dbt_utils.not_constant"]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class TestValidation:
    def test_valid_standard_tests(self):
        tests = ["not_null", "unique"]
        assert _validate_column_tests(tests) is None

    def test_valid_accepted_values(self):
        tests = [{"accepted_values": {"values": ["a", "b"]}}]
        assert _validate_column_tests(tests) is None

    def test_valid_relationships(self):
        tests = [{"relationships": {"to": "ref('users')", "field": "id"}}]
        assert _validate_column_tests(tests) is None

    def test_valid_dbt_utils_test(self):
        tests = [{"dbt_utils.expression_is_true": {"expression": "x > 0"}}]
        assert _validate_column_tests(tests) is None

    def test_invalid_string_test(self):
        tests = ["invalid_test_name"]
        result = _validate_column_tests(tests)
        assert result is not None
        assert "invalid_test_name" in result

    def test_invalid_dict_test(self):
        tests = [{"unknown_test": {}}]
        result = _validate_column_tests(tests)
        assert result is not None

    def test_mixed_valid_and_invalid(self):
        tests = ["not_null", "bad_test"]
        result = _validate_column_tests(tests)
        assert result is not None
        assert "bad_test" in result

    def test_empty_tests_valid(self):
        assert _validate_column_tests([]) is None


# ---------------------------------------------------------------------------
# Dedup — no more than 1 test of each type per column
# ---------------------------------------------------------------------------
class TestDedup:
    def _add_test(self, col, test_entry):
        # Simulate what add_custom_test does: dedup then append
        key = next(iter(test_entry), "")
        new_tests = [
            t for t in col.tests
            if not (isinstance(t, dict) and next(iter(t), "") == key)
        ]
        new_tests.append(test_entry)
        return col.model_copy(update={"tests": new_tests})

    def test_add_duplicate_replaces_existing(self):
        col = ColumnItem(
            name="status",
            tests=[{"accepted_values": {"values": ["a", "b"]}}],
        )
        updated = self._add_test(col, {"accepted_values": {"values": ["x", "y", "z"]}})
        result = _recompute_columns([updated])
        # Only one accepted_values test, with the new values
        av_tests = [t for t in result[0].tests if isinstance(t, dict) and "accepted_values" in t]
        assert len(av_tests) == 1
        assert av_tests[0]["accepted_values"]["values"] == ["x", "y", "z"]

    def test_add_duplicate_dbt_utils_replaces(self):
        col = ColumnItem(
            name="x",
            tests=[{"dbt_utils.expression_is_true": {"expression": "x > 0"}}],
        )
        updated = self._add_test(
            col, {"dbt_utils.expression_is_true": {"expression": "x > 10"}}
        )
        result = _recompute_columns([updated])
        expr_tests = [
            t for t in result[0].tests
            if isinstance(t, dict) and "dbt_utils.expression_is_true" in t
        ]
        assert len(expr_tests) == 1
        assert expr_tests[0]["dbt_utils.expression_is_true"]["expression"] == "x > 10"

    def test_different_types_coexist(self):
        col = ColumnItem(
            name="user_id",
            tests=[
                {"accepted_values": {"values": ["a"]}},
                {"relationships": {"to": "ref('users')", "field": "id"}},
            ],
        )
        result = _recompute_columns([col])
        assert len(result[0].tests) == 2
        assert "accepted_values" in result[0].additional_tests
        assert "relationships" in result[0].additional_tests


# ---------------------------------------------------------------------------
# Form builder — accepted_values and relationships via form
# ---------------------------------------------------------------------------
class TestFormBuilder:
    def test_build_accepted_values(self):
        from datanika.ui.state.model_detail_state import ModelDetailState
        state = ModelDetailState()
        state.custom_test_type = "accepted_values"
        state.custom_test_expression = "active, inactive, pending"
        entry = state._build_custom_test_entry()
        assert entry == {"accepted_values": {"values": ["active", "inactive", "pending"]}}

    def test_build_accepted_values_trims_whitespace(self):
        from datanika.ui.state.model_detail_state import ModelDetailState
        state = ModelDetailState()
        state.custom_test_type = "accepted_values"
        state.custom_test_expression = "  a ,  b , c  "
        entry = state._build_custom_test_entry()
        assert entry == {"accepted_values": {"values": ["a", "b", "c"]}}

    def test_build_accepted_values_empty_returns_none(self):
        from datanika.ui.state.model_detail_state import ModelDetailState
        state = ModelDetailState()
        state.custom_test_type = "accepted_values"
        state.custom_test_expression = ""
        assert state._build_custom_test_entry() is None

    def test_build_relationships(self):
        from datanika.ui.state.model_detail_state import ModelDetailState
        state = ModelDetailState()
        state.custom_test_type = "relationships"
        state.custom_test_min_value = "ref('users')"
        state.custom_test_max_value = "id"
        entry = state._build_custom_test_entry()
        assert entry == {"relationships": {"to": "ref('users')", "field": "id"}}

    def test_build_relationships_empty_to_returns_none(self):
        from datanika.ui.state.model_detail_state import ModelDetailState
        state = ModelDetailState()
        state.custom_test_type = "relationships"
        state.custom_test_min_value = ""
        state.custom_test_max_value = "id"
        assert state._build_custom_test_entry() is None
