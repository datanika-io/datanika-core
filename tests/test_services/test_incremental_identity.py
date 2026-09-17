"""The identity an incremental upload's cursor is recorded under (core#1404).

Contract: ``SPEC_INCREMENTAL_UPLOADS`` §2.3.

A cursor is valid only for the source connection, source schema, table, cursor column and initial
value it was recorded under, and for the destination connection and destination schema the rows went
to. Changing any of them must restart the cursor. Changing anything else must not, and re-saving the
form with the same values must not either: "compare values, not their stored spelling".

The identity is the dlt pipeline name, because dlt restores a cursor from the destination by that
name.
"""

from __future__ import annotations

import pytest

from datanika.services.dlt_runner import DltRunnerService
from datanika.services.incremental_identity import (
    RESUMABLE_INCREMENTAL_SOURCE_TYPES,
    incremental_pipeline_name,
    refuses_incremental,
)

BASE = {
    "upload_id": 7,
    "source_type": "postgres",
    "source_connection_id": 11,
    "destination_connection_id": 12,
    "destination_schema": "raw",
    "dlt_config": {
        "mode": "single_table",
        "table": "events",
        "source_schema": "public",
        "write_disposition": "append",
        "incremental": {"cursor_path": "updated_at", "initial_value": "30"},
    },
}


def _name(**changes):
    args = {**BASE, "dlt_config": {**BASE["dlt_config"]}}
    config_changes = changes.pop("dlt_config", {})
    incremental_changes = config_changes.pop("incremental", None)
    args.update(changes)
    args["dlt_config"].update(config_changes)
    if incremental_changes is not None:
        args["dlt_config"]["incremental"] = {
            **BASE["dlt_config"]["incremental"],
            **incremental_changes,
        }
    return incremental_pipeline_name(**args)


def test_the_resumable_types_are_the_loaders_sql_source_types():
    """A literal set, so the form and the API do not import dlt; pinned to the loader's own set."""
    assert RESUMABLE_INCREMENTAL_SOURCE_TYPES == DltRunnerService.SUPPORTED_SOURCE_TYPES


def test_an_incremental_single_table_upload_has_a_stable_name():
    first, second = _name(), _name()
    assert first is not None and first == second
    assert first.startswith("upload_7_incremental_")


@pytest.mark.parametrize(
    "change",
    [
        pytest.param({"source_connection_id": 99}, id="source connection"),
        pytest.param({"dlt_config": {"source_schema": "sales"}}, id="source schema"),
        pytest.param({"dlt_config": {"table": "orders"}}, id="table"),
        pytest.param({"dlt_config": {"incremental": {"cursor_path": "id"}}}, id="cursor column"),
        pytest.param({"dlt_config": {"incremental": {"initial_value": "40"}}}, id="initial value"),
        pytest.param({"destination_connection_id": 99}, id="destination connection"),
        pytest.param({"destination_schema": "raw_v2"}, id="destination schema"),
        pytest.param({"upload_id": 8}, id="upload"),
    ],
)
def test_a_change_to_a_key_field_restarts_the_cursor(change):
    assert _name(**change) != _name()


@pytest.mark.parametrize(
    "change",
    [
        pytest.param({"dlt_config": {"write_disposition": "merge"}}, id="write disposition"),
        pytest.param({"dlt_config": {"batch_size": 5000}}, id="batch size"),
        pytest.param({"dlt_config": {"schema_contract": {"columns": "freeze"}}}, id="contract"),
        pytest.param({"dlt_config": {"incremental": {"row_order": "asc"}}}, id="row order"),
        pytest.param({"dlt_config": {"primary_key": "id"}}, id="primary key"),
    ],
)
def test_a_change_to_anything_else_keeps_the_cursor(change):
    """SPEC §2.3's negative control: without it, a key that restarts on every save passes."""
    assert _name(**change) == _name()


@pytest.mark.parametrize(
    "stored, equivalent",
    [
        pytest.param({"initial_value": "30"}, {"initial_value": 30}, id="text or number"),
        pytest.param({"initial_value": " 30 "}, {"initial_value": "30"}, id="padding"),
    ],
)
def test_the_same_value_stored_differently_keeps_the_cursor(stored, equivalent):
    assert _name(dlt_config={"incremental": stored}) == _name(
        dlt_config={"incremental": equivalent}
    )


def test_an_empty_initial_value_is_the_same_as_none():
    config = {**BASE["dlt_config"], "incremental": {"cursor_path": "updated_at"}}
    absent = incremental_pipeline_name(**{**BASE, "dlt_config": config})
    empty = _name(dlt_config={"incremental": {"initial_value": ""}})
    assert absent == empty


def test_an_empty_source_schema_is_the_same_as_none():
    config = {k: v for k, v in BASE["dlt_config"].items() if k != "source_schema"}
    assert incremental_pipeline_name(**{**BASE, "dlt_config": config}) == _name(
        dlt_config={"source_schema": ""}
    )


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"dlt_config": {"mode": "full_database"}}, id="full database"),
        pytest.param({"source_type": "mongodb"}, id="mongodb"),
        pytest.param({"source_type": "stripe"}, id="saas"),
        pytest.param({"source_type": "csv"}, id="file"),
    ],
)
def test_no_stable_name_where_no_cursor_resumes(overrides):
    """SPEC §3: an upload with no resumable cursor keeps one pipeline name per run."""
    assert _name(**overrides) is None


def test_no_stable_name_without_a_cursor():
    config = {k: v for k, v in BASE["dlt_config"].items() if k != "incremental"}
    assert incremental_pipeline_name(**{**BASE, "dlt_config": config}) is None


class TestSaveRefusal:
    """SPEC §2.5: a surface that accepts a cursor resumes it or refuses it at save."""

    def test_a_sql_source_keeps_its_cursor(self):
        assert refuses_incremental("postgres", BASE["dlt_config"]) is None

    @pytest.mark.parametrize("source_type", ["mongodb", "stripe", "csv", "rest_api", "kafka"])
    def test_every_other_source_is_refused_with_a_reason(self, source_type):
        reason = refuses_incremental(source_type, BASE["dlt_config"])
        assert reason and source_type in reason

    def test_a_config_without_a_cursor_is_never_refused(self):
        config = {k: v for k, v in BASE["dlt_config"].items() if k != "incremental"}
        assert refuses_incremental("mongodb", config) is None
