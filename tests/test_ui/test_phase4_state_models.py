"""Tests for Phase 4 rx.Base data model classes used in UI state."""

from etlfabric.ui.state.dag_state import DependencyItem
from etlfabric.ui.state.dashboard_state import DashboardStats
from etlfabric.ui.state.run_state import RunState


class TestDashboardStats:
    def test_create_with_fields(self):
        stats = DashboardStats(
            total_pipelines=5,
            total_transformations=3,
            total_schedules=2,
            recent_runs_success=10,
            recent_runs_failed=1,
            recent_runs_total=11,
        )
        assert stats.total_pipelines == 5
        assert stats.total_transformations == 3
        assert stats.total_schedules == 2
        assert stats.recent_runs_success == 10
        assert stats.recent_runs_failed == 1
        assert stats.recent_runs_total == 11

    def test_defaults(self):
        stats = DashboardStats()
        assert stats.total_pipelines == 0
        assert stats.total_transformations == 0
        assert stats.total_schedules == 0
        assert stats.recent_runs_success == 0
        assert stats.recent_runs_failed == 0
        assert stats.recent_runs_total == 0


class TestDependencyItem:
    def test_create_with_fields(self):
        item = DependencyItem(
            id=1,
            upstream_type="pipeline",
            upstream_id=10,
            downstream_type="transformation",
            downstream_id=20,
        )
        assert item.id == 1
        assert item.upstream_type == "pipeline"
        assert item.upstream_id == 10
        assert item.downstream_type == "transformation"
        assert item.downstream_id == 20

    def test_defaults(self):
        item = DependencyItem()
        assert item.id == 0
        assert item.upstream_type == ""
        assert item.upstream_id == 0
        assert item.downstream_type == ""
        assert item.downstream_id == 0


class TestRunStateFilterTargetType:
    def test_filter_target_type_field_exists(self):
        """RunState should have a filter_target_type field for target type filtering."""
        fields = RunState.get_fields()
        assert "filter_target_type" in fields
