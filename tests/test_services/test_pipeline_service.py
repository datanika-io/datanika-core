"""TDD tests for pipeline service."""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.pipeline import Pipeline, PipelineStatus
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineConfigError, PipelineService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def svc(conn_svc):
    return PipelineService(conn_svc)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-pipe-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-pipe-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def source_conn(conn_svc, db_session, org):
    return conn_svc.create_connection(
        db_session,
        org.id,
        "Source DB",
        ConnectionType.POSTGRES,
        ConnectionDirection.SOURCE,
        {"host": "src"},
    )


@pytest.fixture
def dest_conn(conn_svc, db_session, org):
    return conn_svc.create_connection(
        db_session,
        org.id,
        "Dest DB",
        ConnectionType.POSTGRES,
        ConnectionDirection.DESTINATION,
        {"host": "dst"},
    )


@pytest.fixture
def both_conn(conn_svc, db_session, org):
    return conn_svc.create_connection(
        db_session,
        org.id,
        "Both DB",
        ConnectionType.POSTGRES,
        ConnectionDirection.BOTH,
        {"host": "both"},
    )


class TestCreatePipeline:
    def test_basic(self, svc, db_session, org, source_conn, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "my_pipe",
            "desc",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert isinstance(pipe, Pipeline)
        assert isinstance(pipe.id, int)
        assert pipe.name == "my_pipe"
        assert pipe.status == PipelineStatus.DRAFT

    def test_with_config(self, svc, db_session, org, source_conn, dest_conn):
        config = {"write_disposition": "append"}
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            config,
        )
        assert pipe.dlt_config == config

    def test_invalid_source_connection(self, svc, db_session, org, dest_conn):
        with pytest.raises(ValueError, match="source"):
            svc.create_pipeline(
                db_session,
                org.id,
                "p",
                "d",
                99999,
                dest_conn.id,
                {},
            )

    def test_wrong_direction_source(self, svc, db_session, org, dest_conn):
        """A DESTINATION-only connection cannot be used as source."""
        with pytest.raises(ValueError, match="source"):
            svc.create_pipeline(
                db_session,
                org.id,
                "p",
                "d",
                dest_conn.id,
                dest_conn.id,
                {},
            )

    def test_wrong_direction_dest(self, svc, db_session, org, source_conn):
        """A SOURCE-only connection cannot be used as destination."""
        with pytest.raises(ValueError, match="destination"):
            svc.create_pipeline(
                db_session,
                org.id,
                "p",
                "d",
                source_conn.id,
                source_conn.id,
                {},
            )

    def test_both_direction_works_as_source(self, svc, db_session, org, both_conn, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            both_conn.id,
            dest_conn.id,
            {},
        )
        assert pipe.source_connection_id == both_conn.id

    def test_both_direction_works_as_dest(self, svc, db_session, org, source_conn, both_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            both_conn.id,
            {},
        )
        assert pipe.destination_connection_id == both_conn.id


class TestGetPipeline:
    def test_existing(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        fetched = svc.get_pipeline(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_pipeline(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert svc.get_pipeline(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.delete_pipeline(db_session, org.id, created.id)
        assert svc.get_pipeline(db_session, org.id, created.id) is None


class TestListPipelines:
    def test_empty(self, svc, db_session, org):
        assert svc.list_pipelines(db_session, org.id) == []

    def test_multiple(self, svc, db_session, org, source_conn, dest_conn):
        svc.create_pipeline(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.create_pipeline(
            db_session,
            org.id,
            "B",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert len(svc.list_pipelines(db_session, org.id)) == 2

    def test_excludes_deleted(self, svc, db_session, org, source_conn, dest_conn):
        p1 = svc.create_pipeline(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.create_pipeline(
            db_session,
            org.id,
            "B",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.delete_pipeline(db_session, org.id, p1.id)
        result = svc.list_pipelines(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "B"

    def test_filters_by_org(
        self, svc, conn_svc, db_session, org, other_org, source_conn, dest_conn
    ):
        svc.create_pipeline(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        # Create connections for other org
        other_src = conn_svc.create_connection(
            db_session,
            other_org.id,
            "S2",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        other_dst = conn_svc.create_connection(
            db_session,
            other_org.id,
            "D2",
            ConnectionType.POSTGRES,
            ConnectionDirection.DESTINATION,
            {},
        )
        svc.create_pipeline(
            db_session,
            other_org.id,
            "B",
            "d",
            other_src.id,
            other_dst.id,
            {},
        )
        result = svc.list_pipelines(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "A"


class TestUpdatePipeline:
    def test_update_name(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "Old",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        updated = svc.update_pipeline(db_session, org.id, created.id, name="New")
        assert updated is not None
        assert updated.name == "New"

    def test_update_status(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        updated = svc.update_pipeline(
            db_session,
            org.id,
            created.id,
            status=PipelineStatus.ACTIVE,
        )
        assert updated.status == PipelineStatus.ACTIVE

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_pipeline(db_session, org.id, 99999, name="X") is None

    def test_config_re_validates(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        with pytest.raises(PipelineConfigError):
            svc.update_pipeline(
                db_session,
                org.id,
                created.id,
                dlt_config={"write_disposition": "invalid"},
            )


class TestDeletePipeline:
    def test_sets_deleted_at(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert svc.delete_pipeline(db_session, org.id, created.id) is True
        db_session.refresh(created)
        assert created.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        assert svc.delete_pipeline(db_session, org.id, 99999) is False


class TestValidatePipelineConfig:
    def test_append_valid(self):
        PipelineService.validate_pipeline_config({"write_disposition": "append"})

    def test_replace_valid(self):
        PipelineService.validate_pipeline_config({"write_disposition": "replace"})

    def test_merge_with_pk_valid(self):
        PipelineService.validate_pipeline_config(
            {"write_disposition": "merge", "primary_key": "id"}
        )

    def test_invalid_disposition(self):
        with pytest.raises(PipelineConfigError, match="write_disposition"):
            PipelineService.validate_pipeline_config({"write_disposition": "invalid"})

    def test_merge_without_pk(self):
        with pytest.raises(PipelineConfigError, match="primary_key"):
            PipelineService.validate_pipeline_config({"write_disposition": "merge"})

    def test_empty_config_valid(self):
        PipelineService.validate_pipeline_config({})

    def test_non_dict_raises(self):
        with pytest.raises(PipelineConfigError):
            PipelineService.validate_pipeline_config("not a dict")


class TestValidatePipelineConfigModes:
    """Validate mode-specific dlt_config rules (Step 20)."""

    # -- mode field --

    def test_invalid_mode_raises(self):
        with pytest.raises(PipelineConfigError, match="mode"):
            PipelineService.validate_pipeline_config({"mode": "unknown"})

    def test_full_database_explicit(self):
        PipelineService.validate_pipeline_config({"mode": "full_database"})

    def test_single_table_with_table(self):
        PipelineService.validate_pipeline_config({"mode": "single_table", "table": "customers"})

    def test_legacy_no_mode_accepted(self):
        """Configs without mode are treated as full_database — no error."""
        PipelineService.validate_pipeline_config({"write_disposition": "append"})

    # -- single_table rules --

    def test_single_table_requires_table(self):
        with pytest.raises(PipelineConfigError, match="table"):
            PipelineService.validate_pipeline_config({"mode": "single_table"})

    def test_single_table_rejects_table_names(self):
        with pytest.raises(PipelineConfigError, match="table_names"):
            PipelineService.validate_pipeline_config(
                {"mode": "single_table", "table": "x", "table_names": ["x"]}
            )

    # -- full_database rules --

    def test_full_database_rejects_table(self):
        with pytest.raises(PipelineConfigError, match="table"):
            PipelineService.validate_pipeline_config({"mode": "full_database", "table": "x"})

    def test_full_database_rejects_incremental(self):
        with pytest.raises(PipelineConfigError, match="incremental"):
            PipelineService.validate_pipeline_config(
                {"mode": "full_database", "incremental": {"cursor_path": "updated_at"}}
            )

    def test_full_database_accepts_table_names(self):
        PipelineService.validate_pipeline_config(
            {"mode": "full_database", "table_names": ["a", "b"]}
        )

    def test_full_database_table_names_must_be_list(self):
        with pytest.raises(PipelineConfigError, match="table_names"):
            PipelineService.validate_pipeline_config(
                {"mode": "full_database", "table_names": "customers"}
            )

    # -- incremental (single_table) --

    def test_incremental_requires_cursor_path(self):
        with pytest.raises(PipelineConfigError, match="cursor_path"):
            PipelineService.validate_pipeline_config(
                {"mode": "single_table", "table": "t", "incremental": {}}
            )

    def test_incremental_valid(self):
        PipelineService.validate_pipeline_config(
            {
                "mode": "single_table",
                "table": "t",
                "incremental": {
                    "cursor_path": "updated_at",
                    "initial_value": "2024-01-01",
                    "row_order": "asc",
                },
            }
        )

    def test_incremental_invalid_row_order(self):
        with pytest.raises(PipelineConfigError, match="row_order"):
            PipelineService.validate_pipeline_config(
                {
                    "mode": "single_table",
                    "table": "t",
                    "incremental": {"cursor_path": "id", "row_order": "random"},
                }
            )

    # -- batch_size --

    def test_batch_size_positive_int(self):
        PipelineService.validate_pipeline_config({"batch_size": 5000})

    def test_batch_size_zero_raises(self):
        with pytest.raises(PipelineConfigError, match="batch_size"):
            PipelineService.validate_pipeline_config({"batch_size": 0})

    def test_batch_size_negative_raises(self):
        with pytest.raises(PipelineConfigError, match="batch_size"):
            PipelineService.validate_pipeline_config({"batch_size": -1})

    def test_batch_size_non_int_raises(self):
        with pytest.raises(PipelineConfigError, match="batch_size"):
            PipelineService.validate_pipeline_config({"batch_size": "big"})

    # -- source_schema --

    def test_source_schema_string(self):
        PipelineService.validate_pipeline_config(
            {"mode": "full_database", "source_schema": "public"}
        )

    def test_source_schema_non_string_raises(self):
        with pytest.raises(PipelineConfigError, match="source_schema"):
            PipelineService.validate_pipeline_config({"source_schema": 123})


class TestValidateSchemaContract:
    """Step 23: schema_contract validation."""

    def test_valid_schema_contract(self):
        PipelineService.validate_pipeline_config(
            {
                "schema_contract": {
                    "tables": "evolve",
                    "columns": "freeze",
                    "data_type": "discard_value",
                }
            }
        )

    def test_schema_contract_all_options(self):
        for option in ("evolve", "freeze", "discard_value", "discard_row"):
            PipelineService.validate_pipeline_config({"schema_contract": {"tables": option}})

    def test_schema_contract_must_be_dict(self):
        with pytest.raises(PipelineConfigError, match="schema_contract"):
            PipelineService.validate_pipeline_config({"schema_contract": "evolve"})

    def test_schema_contract_invalid_entity(self):
        with pytest.raises(PipelineConfigError, match="schema_contract"):
            PipelineService.validate_pipeline_config({"schema_contract": {"invalid_key": "evolve"}})

    def test_schema_contract_invalid_value(self):
        with pytest.raises(PipelineConfigError, match="schema_contract"):
            PipelineService.validate_pipeline_config(
                {"schema_contract": {"tables": "invalid_value"}}
            )

    def test_schema_contract_empty_dict_valid(self):
        PipelineService.validate_pipeline_config({"schema_contract": {}})

    def test_schema_contract_partial_valid(self):
        PipelineService.validate_pipeline_config({"schema_contract": {"columns": "discard_row"}})


class TestValidateFilters:
    """Step 33: data quality filters validation."""

    def test_valid_filter(self):
        PipelineService.validate_pipeline_config(
            {"filters": [{"column": "status", "op": "eq", "value": "active"}]}
        )

    def test_multiple_filters(self):
        PipelineService.validate_pipeline_config(
            {
                "filters": [
                    {"column": "status", "op": "eq", "value": "active"},
                    {"column": "age", "op": "gt", "value": 18},
                ]
            }
        )

    def test_filters_must_be_list(self):
        with pytest.raises(PipelineConfigError, match="filters"):
            PipelineService.validate_pipeline_config({"filters": "bad"})

    def test_filter_must_be_dict(self):
        with pytest.raises(PipelineConfigError, match="filter.*dict"):
            PipelineService.validate_pipeline_config({"filters": ["bad"]})

    def test_filter_requires_column(self):
        with pytest.raises(PipelineConfigError, match="column"):
            PipelineService.validate_pipeline_config({"filters": [{"op": "eq", "value": "x"}]})

    def test_filter_requires_op(self):
        with pytest.raises(PipelineConfigError, match="op"):
            PipelineService.validate_pipeline_config({"filters": [{"column": "c", "value": "x"}]})

    def test_filter_requires_value(self):
        with pytest.raises(PipelineConfigError, match="value"):
            PipelineService.validate_pipeline_config({"filters": [{"column": "c", "op": "eq"}]})

    def test_invalid_op(self):
        with pytest.raises(PipelineConfigError, match="op"):
            PipelineService.validate_pipeline_config(
                {"filters": [{"column": "c", "op": "invalid", "value": 1}]}
            )

    def test_all_valid_ops(self):
        for op in ("eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in"):
            PipelineService.validate_pipeline_config(
                {"filters": [{"column": "c", "op": op, "value": 1}]}
            )

    def test_empty_filters_list_valid(self):
        PipelineService.validate_pipeline_config({"filters": []})
