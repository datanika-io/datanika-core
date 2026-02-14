"""Tests for rx.Base data model classes used in UI state."""

from etlfabric.ui.state.connection_state import ConnectionItem
from etlfabric.ui.state.pipeline_state import PipelineItem
from etlfabric.ui.state.run_state import RunItem
from etlfabric.ui.state.schedule_state import ScheduleItem
from etlfabric.ui.state.transformation_state import TransformationItem


class TestConnectionItem:
    def test_create_with_fields(self):
        item = ConnectionItem(id=1, name="My DB", connection_type="postgres", direction="source")
        assert item.id == 1
        assert item.name == "My DB"
        assert item.connection_type == "postgres"
        assert item.direction == "source"

    def test_defaults(self):
        item = ConnectionItem()
        assert item.id == 0
        assert item.name == ""
        assert item.connection_type == ""
        assert item.direction == ""


class TestPipelineItem:
    def test_create_with_fields(self):
        item = PipelineItem(
            id=5,
            name="ETL",
            description="desc",
            status="active",
            source_connection_id=1,
            destination_connection_id=2,
        )
        assert item.id == 5
        assert item.name == "ETL"
        assert item.status == "active"
        assert item.source_connection_id == 1
        assert item.destination_connection_id == 2

    def test_defaults(self):
        item = PipelineItem()
        assert item.id == 0
        assert item.description == ""
        assert item.status == ""


class TestTransformationItem:
    def test_create_with_fields(self):
        item = TransformationItem(
            id=3,
            name="orders",
            description="all orders",
            materialization="table",
            schema_name="marts",
        )
        assert item.id == 3
        assert item.name == "orders"
        assert item.materialization == "table"
        assert item.schema_name == "marts"

    def test_defaults(self):
        item = TransformationItem()
        assert item.id == 0
        assert item.schema_name == ""


class TestScheduleItem:
    def test_create_with_fields(self):
        item = ScheduleItem(
            id=7,
            target_type="pipeline",
            target_id=1,
            cron_expression="0 * * * *",
            timezone="UTC",
            is_active=True,
        )
        assert item.id == 7
        assert item.target_type == "pipeline"
        assert item.cron_expression == "0 * * * *"
        assert item.is_active is True

    def test_defaults(self):
        item = ScheduleItem()
        assert item.id == 0
        assert item.is_active is True


class TestRunItem:
    def test_create_with_fields(self):
        item = RunItem(
            id=10,
            target_type="pipeline",
            target_id=2,
            status="success",
            started_at="2024-01-01",
            finished_at="2024-01-01",
            rows_loaded=100,
            error_message="",
        )
        assert item.id == 10
        assert item.status == "success"
        assert item.rows_loaded == 100

    def test_defaults(self):
        item = RunItem()
        assert item.id == 0
        assert item.status == ""
        assert item.rows_loaded == 0
        assert item.error_message == ""
