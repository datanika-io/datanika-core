"""TDD tests for upload service."""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.upload_service import (
    UploadConfigError,
    UploadService,
    to_dataset_name,
    validate_upload_name,
)


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def svc(conn_svc):
    return UploadService(conn_svc)


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


class TestCreateUpload:
    def test_basic(self, svc, db_session, org, source_conn, dest_conn):
        upload = svc.create_upload(
            db_session,
            org.id,
            "my pipe",
            "desc",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert isinstance(upload, Upload)
        assert isinstance(upload.id, int)
        assert upload.name == "my pipe"
        assert upload.status == UploadStatus.DRAFT

    def test_with_config(self, svc, db_session, org, source_conn, dest_conn):
        config = {"write_disposition": "append"}
        pipe = svc.create_upload(
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
            svc.create_upload(
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
            svc.create_upload(
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
            svc.create_upload(
                db_session,
                org.id,
                "p",
                "d",
                source_conn.id,
                source_conn.id,
                {},
            )

    def test_both_direction_works_as_source(self, svc, db_session, org, both_conn, dest_conn):
        pipe = svc.create_upload(
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
        pipe = svc.create_upload(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            both_conn.id,
            {},
        )
        assert pipe.destination_connection_id == both_conn.id


class TestGetUpload:
    def test_existing(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        fetched = svc.get_upload(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_upload(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert svc.get_upload(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.delete_upload(db_session, org.id, created.id)
        assert svc.get_upload(db_session, org.id, created.id) is None


class TestListUploads:
    def test_empty(self, svc, db_session, org):
        assert svc.list_uploads(db_session, org.id) == []

    def test_multiple(self, svc, db_session, org, source_conn, dest_conn):
        svc.create_upload(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.create_upload(
            db_session,
            org.id,
            "B",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert len(svc.list_uploads(db_session, org.id)) == 2

    def test_excludes_deleted(self, svc, db_session, org, source_conn, dest_conn):
        p1 = svc.create_upload(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.create_upload(
            db_session,
            org.id,
            "B",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.delete_upload(db_session, org.id, p1.id)
        result = svc.list_uploads(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "B"

    def test_filters_by_org(
        self, svc, conn_svc, db_session, org, other_org, source_conn, dest_conn
    ):
        svc.create_upload(
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
        svc.create_upload(
            db_session,
            other_org.id,
            "B",
            "d",
            other_src.id,
            other_dst.id,
            {},
        )
        result = svc.list_uploads(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "A"


class TestUpdateUpload:
    def test_update_name(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session,
            org.id,
            "Old",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        updated = svc.update_upload(db_session, org.id, created.id, name="New")
        assert updated is not None
        assert updated.name == "New"

    def test_update_status(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        updated = svc.update_upload(
            db_session,
            org.id,
            created.id,
            status=UploadStatus.ACTIVE,
        )
        assert updated.status == UploadStatus.ACTIVE

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_upload(db_session, org.id, 99999, name="X") is None

    def test_config_re_validates(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        with pytest.raises(UploadConfigError):
            svc.update_upload(
                db_session,
                org.id,
                created.id,
                dlt_config={"write_disposition": "invalid"},
            )


class TestDeleteUpload:
    def test_sets_deleted_at(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert svc.delete_upload(db_session, org.id, created.id) is True
        db_session.refresh(created)
        assert created.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        assert svc.delete_upload(db_session, org.id, 99999) is False


class TestValidateUploadConfig:
    def test_append_valid(self):
        UploadService.validate_upload_config({"write_disposition": "append"})

    def test_replace_valid(self):
        UploadService.validate_upload_config({"write_disposition": "replace"})

    def test_merge_with_pk_valid_single_table(self):
        UploadService.validate_upload_config(
            {
                "mode": "single_table",
                "table": "t",
                "write_disposition": "merge",
                "primary_key": "id",
            }
        )

    def test_invalid_disposition(self):
        with pytest.raises(UploadConfigError, match="write_disposition"):
            UploadService.validate_upload_config({"write_disposition": "invalid"})

    def test_merge_without_pk_single_table(self):
        with pytest.raises(UploadConfigError, match="primary_key"):
            UploadService.validate_upload_config(
                {"mode": "single_table", "table": "t", "write_disposition": "merge"}
            )

    def test_empty_config_valid(self):
        UploadService.validate_upload_config({})

    def test_non_dict_raises(self):
        with pytest.raises(UploadConfigError):
            UploadService.validate_upload_config("not a dict")


class TestValidateUploadConfigModes:
    """Validate mode-specific dlt_config rules (Step 20)."""

    # -- mode field --

    def test_invalid_mode_raises(self):
        with pytest.raises(UploadConfigError, match="mode"):
            UploadService.validate_upload_config({"mode": "unknown"})

    def test_full_database_explicit(self):
        UploadService.validate_upload_config({"mode": "full_database"})

    def test_single_table_with_table(self):
        UploadService.validate_upload_config({"mode": "single_table", "table": "customers"})

    def test_legacy_no_mode_accepted(self):
        """Configs without mode are treated as full_database — no error."""
        UploadService.validate_upload_config({"write_disposition": "append"})

    # -- single_table rules --

    def test_single_table_requires_table(self):
        with pytest.raises(UploadConfigError, match="table"):
            UploadService.validate_upload_config({"mode": "single_table"})

    def test_single_table_rejects_table_names(self):
        with pytest.raises(UploadConfigError, match="table_names"):
            UploadService.validate_upload_config(
                {"mode": "single_table", "table": "x", "table_names": ["x"]}
            )

    # -- full_database rules --

    def test_full_database_rejects_table(self):
        with pytest.raises(UploadConfigError, match="table"):
            UploadService.validate_upload_config({"mode": "full_database", "table": "x"})

    def test_full_database_rejects_incremental(self):
        with pytest.raises(UploadConfigError, match="incremental"):
            UploadService.validate_upload_config(
                {"mode": "full_database", "incremental": {"cursor_path": "updated_at"}}
            )

    def test_full_database_accepts_table_names(self):
        UploadService.validate_upload_config({"mode": "full_database", "table_names": ["a", "b"]})

    def test_full_database_table_names_must_be_list(self):
        with pytest.raises(UploadConfigError, match="table_names"):
            UploadService.validate_upload_config(
                {"mode": "full_database", "table_names": "customers"}
            )

    # -- incremental (single_table) --

    def test_incremental_requires_cursor_path(self):
        with pytest.raises(UploadConfigError, match="cursor_path"):
            UploadService.validate_upload_config(
                {"mode": "single_table", "table": "t", "incremental": {}}
            )

    def test_incremental_valid(self):
        UploadService.validate_upload_config(
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
        with pytest.raises(UploadConfigError, match="row_order"):
            UploadService.validate_upload_config(
                {
                    "mode": "single_table",
                    "table": "t",
                    "incremental": {"cursor_path": "id", "row_order": "random"},
                }
            )

    # -- batch_size --

    def test_batch_size_positive_int(self):
        UploadService.validate_upload_config({"batch_size": 5000})

    def test_batch_size_zero_raises(self):
        with pytest.raises(UploadConfigError, match="batch_size"):
            UploadService.validate_upload_config({"batch_size": 0})

    def test_batch_size_negative_raises(self):
        with pytest.raises(UploadConfigError, match="batch_size"):
            UploadService.validate_upload_config({"batch_size": -1})

    def test_batch_size_non_int_raises(self):
        with pytest.raises(UploadConfigError, match="batch_size"):
            UploadService.validate_upload_config({"batch_size": "big"})

    # -- source_schema --

    def test_source_schema_string(self):
        UploadService.validate_upload_config({"mode": "full_database", "source_schema": "public"})

    def test_source_schema_non_string_raises(self):
        with pytest.raises(UploadConfigError, match="source_schema"):
            UploadService.validate_upload_config({"source_schema": 123})


class TestValidateSchemaContract:
    """Step 23: schema_contract validation."""

    def test_valid_schema_contract(self):
        UploadService.validate_upload_config(
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
            UploadService.validate_upload_config({"schema_contract": {"tables": option}})

    def test_schema_contract_must_be_dict(self):
        with pytest.raises(UploadConfigError, match="schema_contract"):
            UploadService.validate_upload_config({"schema_contract": "evolve"})

    def test_schema_contract_invalid_entity(self):
        with pytest.raises(UploadConfigError, match="schema_contract"):
            UploadService.validate_upload_config({"schema_contract": {"invalid_key": "evolve"}})

    def test_schema_contract_invalid_value(self):
        with pytest.raises(UploadConfigError, match="schema_contract"):
            UploadService.validate_upload_config({"schema_contract": {"tables": "invalid_value"}})

    def test_schema_contract_empty_dict_valid(self):
        UploadService.validate_upload_config({"schema_contract": {}})

    def test_schema_contract_partial_valid(self):
        UploadService.validate_upload_config({"schema_contract": {"columns": "discard_row"}})


class TestValidateFilters:
    """Step 33: data quality filters validation."""

    def test_valid_filter(self):
        UploadService.validate_upload_config(
            {"filters": [{"column": "status", "op": "eq", "value": "active"}]}
        )

    def test_multiple_filters(self):
        UploadService.validate_upload_config(
            {
                "filters": [
                    {"column": "status", "op": "eq", "value": "active"},
                    {"column": "age", "op": "gt", "value": 18},
                ]
            }
        )

    def test_filters_must_be_list(self):
        with pytest.raises(UploadConfigError, match="filters"):
            UploadService.validate_upload_config({"filters": "bad"})

    def test_filter_must_be_dict(self):
        with pytest.raises(UploadConfigError, match="filter.*dict"):
            UploadService.validate_upload_config({"filters": ["bad"]})

    def test_filter_requires_column(self):
        with pytest.raises(UploadConfigError, match="column"):
            UploadService.validate_upload_config({"filters": [{"op": "eq", "value": "x"}]})

    def test_filter_requires_op(self):
        with pytest.raises(UploadConfigError, match="op"):
            UploadService.validate_upload_config({"filters": [{"column": "c", "value": "x"}]})

    def test_filter_requires_value(self):
        with pytest.raises(UploadConfigError, match="value"):
            UploadService.validate_upload_config({"filters": [{"column": "c", "op": "eq"}]})

    def test_invalid_op(self):
        with pytest.raises(UploadConfigError, match="op"):
            UploadService.validate_upload_config(
                {"filters": [{"column": "c", "op": "invalid", "value": 1}]}
            )

    def test_all_valid_ops(self):
        for op in ("eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in"):
            UploadService.validate_upload_config(
                {"filters": [{"column": "c", "op": op, "value": 1}]}
            )

    def test_empty_filters_list_valid(self):
        UploadService.validate_upload_config({"filters": []})


class TestValidateMergeConfig:
    """Per-table merge_config validation for full_database mode."""

    def test_full_database_merge_with_merge_config_valid(self):
        UploadService.validate_upload_config(
            {
                "mode": "full_database",
                "write_disposition": "merge",
                "merge_config": {
                    "customers": {"primary_key": "id"},
                    "orders": {"primary_key": "order_id"},
                },
            }
        )

    def test_full_database_merge_composite_keys(self):
        UploadService.validate_upload_config(
            {
                "mode": "full_database",
                "write_disposition": "merge",
                "merge_config": {
                    "order_items": {"primary_key": ["order_id", "item_id"]},
                },
            }
        )

    def test_full_database_merge_requires_merge_config(self):
        with pytest.raises(UploadConfigError, match="merge_config"):
            UploadService.validate_upload_config(
                {"mode": "full_database", "write_disposition": "merge"}
            )

    def test_merge_config_must_be_dict(self):
        with pytest.raises(UploadConfigError, match="merge_config.*dict"):
            UploadService.validate_upload_config(
                {
                    "mode": "full_database",
                    "write_disposition": "merge",
                    "merge_config": "not a dict",
                }
            )

    def test_merge_config_values_must_have_primary_key(self):
        with pytest.raises(UploadConfigError, match="primary_key"):
            UploadService.validate_upload_config(
                {
                    "mode": "full_database",
                    "write_disposition": "merge",
                    "merge_config": {"customers": {}},
                }
            )

    def test_merge_config_primary_key_must_be_str_or_list(self):
        with pytest.raises(UploadConfigError, match="primary_key"):
            UploadService.validate_upload_config(
                {
                    "mode": "full_database",
                    "write_disposition": "merge",
                    "merge_config": {"customers": {"primary_key": 123}},
                }
            )

    def test_merge_config_values_must_be_dicts(self):
        with pytest.raises(UploadConfigError, match="merge_config"):
            UploadService.validate_upload_config(
                {
                    "mode": "full_database",
                    "write_disposition": "merge",
                    "merge_config": {"customers": "id"},
                }
            )

    def test_single_table_merge_rejects_merge_config(self):
        with pytest.raises(UploadConfigError, match="merge_config"):
            UploadService.validate_upload_config(
                {
                    "mode": "single_table",
                    "table": "t",
                    "write_disposition": "merge",
                    "primary_key": "id",
                    "merge_config": {"t": {"primary_key": "id"}},
                }
            )

    def test_full_database_merge_does_not_require_primary_key(self):
        """full_database merge needs merge_config, not primary_key."""
        UploadService.validate_upload_config(
            {
                "mode": "full_database",
                "write_disposition": "merge",
                "merge_config": {"customers": {"primary_key": "id"}},
            }
        )


class TestValidateUploadName:
    def test_valid_simple_name(self):
        validate_upload_name("My Pipeline")

    def test_valid_alphanumeric(self):
        validate_upload_name("Sales2024")

    def test_valid_with_spaces(self):
        validate_upload_name("My Sales Pipeline 2024")

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_upload_name("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_upload_name("   ")

    def test_special_chars_raises(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_upload_name("my-pipeline")

    def test_underscore_raises(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_upload_name("my_pipeline")

    def test_dots_raises(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_upload_name("my.pipeline")

    def test_at_sign_raises(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_upload_name("user@pipeline")

    def test_strips_whitespace(self):
        validate_upload_name("  My Pipeline  ")


class TestToDatasetName:
    def test_simple_name(self):
        assert to_dataset_name("My Sales Pipeline") == "my_sales_pipeline"

    def test_already_lowercase(self):
        assert to_dataset_name("sales") == "sales"

    def test_multiple_spaces(self):
        assert to_dataset_name("My   Sales   Pipeline") == "my_sales_pipeline"

    def test_leading_trailing_spaces(self):
        assert to_dataset_name("  My Pipeline  ") == "my_pipeline"

    def test_single_word(self):
        assert to_dataset_name("Sales") == "sales"

    def test_mixed_case_with_numbers(self):
        assert to_dataset_name("Sales 2024 Q1") == "sales_2024_q1"


class TestCreateUploadNameValidation:
    def test_rejects_special_chars(self, svc, db_session, org, source_conn, dest_conn):
        with pytest.raises(ValueError, match="alphanumeric"):
            svc.create_upload(db_session, org.id, "my-pipe!", "d", source_conn.id, dest_conn.id, {})

    def test_rejects_empty_name(self, svc, db_session, org, source_conn, dest_conn):
        with pytest.raises(ValueError, match="cannot be empty"):
            svc.create_upload(db_session, org.id, "", "d", source_conn.id, dest_conn.id, {})


class TestUpdateUploadNameValidation:
    def test_rejects_special_chars(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session, org.id, "Valid Name", "d", source_conn.id, dest_conn.id, {}
        )
        with pytest.raises(ValueError, match="alphanumeric"):
            svc.update_upload(db_session, org.id, created.id, name="bad-name!")

    def test_accepts_valid_name(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_upload(
            db_session, org.id, "Old Name", "d", source_conn.id, dest_conn.id, {}
        )
        updated = svc.update_upload(db_session, org.id, created.id, name="New Name 2")
        assert updated.name == "New Name 2"
