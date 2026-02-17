"""TDD tests for transformation management service."""

import pytest

from datanika.models.transformation import Materialization, Transformation
from datanika.models.user import Organization
from datanika.services.transformation_service import (
    TransformationConfigError,
    TransformationService,
)


@pytest.fixture
def svc():
    return TransformationService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-transform-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-transform-svc")
    db_session.add(org)
    db_session.flush()
    return org


class TestCreateTransformation:
    def test_basic(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session, org.id, "my_model", "SELECT 1", Materialization.VIEW
        )
        assert isinstance(t, Transformation)
        assert isinstance(t.id, int)
        assert t.name == "my_model"
        assert t.sql_body == "SELECT 1"
        assert t.materialization == Materialization.VIEW
        assert t.org_id == org.id

    def test_all_fields(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session,
            org.id,
            "full_model",
            "SELECT * FROM raw.orders",
            Materialization.TABLE,
            description="All orders",
            schema_name="marts",
            tests_config={"unique": ["id"]},
        )
        assert t.description == "All orders"
        assert t.schema_name == "marts"
        assert t.tests_config == {"unique": ["id"]}
        assert t.materialization == Materialization.TABLE

    def test_empty_sql_body_rejected(self, svc, db_session, org):
        with pytest.raises(TransformationConfigError, match="sql_body"):
            svc.create_transformation(db_session, org.id, "bad", "", Materialization.VIEW)

    def test_whitespace_sql_body_rejected(self, svc, db_session, org):
        with pytest.raises(TransformationConfigError, match="sql_body"):
            svc.create_transformation(db_session, org.id, "bad", "   \n\t  ", Materialization.VIEW)

    def test_default_values(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session, org.id, "defaults", "SELECT 1", Materialization.VIEW
        )
        assert t.schema_name == "staging"
        assert t.tests_config == {}
        assert t.description is None


class TestGetTransformation:
    def test_existing(self, svc, db_session, org):
        created = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        fetched = svc.get_transformation(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_transformation(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org):
        created = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        assert svc.get_transformation(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org):
        created = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        svc.delete_transformation(db_session, org.id, created.id)
        assert svc.get_transformation(db_session, org.id, created.id) is None


class TestListTransformations:
    def test_empty(self, svc, db_session, org):
        result = svc.list_transformations(db_session, org.id)
        assert result == []

    def test_multiple(self, svc, db_session, org):
        svc.create_transformation(db_session, org.id, "a", "SELECT 1", Materialization.VIEW)
        svc.create_transformation(db_session, org.id, "b", "SELECT 2", Materialization.TABLE)
        result = svc.list_transformations(db_session, org.id)
        assert len(result) == 2

    def test_excludes_deleted(self, svc, db_session, org):
        created = svc.create_transformation(
            db_session, org.id, "a", "SELECT 1", Materialization.VIEW
        )
        svc.create_transformation(db_session, org.id, "b", "SELECT 2", Materialization.TABLE)
        svc.delete_transformation(db_session, org.id, created.id)
        result = svc.list_transformations(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "b"

    def test_filters_by_org(self, svc, db_session, org, other_org):
        svc.create_transformation(db_session, org.id, "a", "SELECT 1", Materialization.VIEW)
        svc.create_transformation(db_session, other_org.id, "b", "SELECT 2", Materialization.TABLE)
        result = svc.list_transformations(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "a"


class TestUpdateTransformation:
    def test_update_name(self, svc, db_session, org):
        created = svc.create_transformation(
            db_session, org.id, "old", "SELECT 1", Materialization.VIEW
        )
        updated = svc.update_transformation(db_session, org.id, created.id, name="new")
        assert updated is not None
        assert updated.name == "new"

    def test_sql_body_revalidates(self, svc, db_session, org):
        created = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        updated = svc.update_transformation(db_session, org.id, created.id, sql_body="SELECT 2")
        assert updated.sql_body == "SELECT 2"

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_transformation(db_session, org.id, 99999, name="x") is None

    def test_invalid_sql_body_rejected(self, svc, db_session, org):
        created = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        with pytest.raises(TransformationConfigError, match="sql_body"):
            svc.update_transformation(db_session, org.id, created.id, sql_body="")


class TestDeleteTransformation:
    def test_sets_deleted_at(self, svc, db_session, org):
        created = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        result = svc.delete_transformation(db_session, org.id, created.id)
        assert result is True
        db_session.refresh(created)
        assert created.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        result = svc.delete_transformation(db_session, org.id, 99999)
        assert result is False


class TestModelNameValidation:
    def test_valid_names(self):
        for name in ("my_model", "model-v2", "_private", "A123", "stg_orders"):
            TransformationService.validate_model_name(name)

    def test_rejects_empty(self):
        with pytest.raises(TransformationConfigError, match="empty"):
            TransformationService.validate_model_name("")

    def test_rejects_spaces(self):
        with pytest.raises(TransformationConfigError, match="Model name"):
            TransformationService.validate_model_name("my model")

    def test_rejects_dots(self):
        with pytest.raises(TransformationConfigError, match="Model name"):
            TransformationService.validate_model_name("my.model")

    def test_rejects_slashes(self):
        with pytest.raises(TransformationConfigError, match="Model name"):
            TransformationService.validate_model_name("../evil")

    def test_rejects_starts_with_digit(self):
        with pytest.raises(TransformationConfigError, match="Model name"):
            TransformationService.validate_model_name("1model")

    def test_create_rejects_invalid_name(self, svc, db_session, org):
        with pytest.raises(TransformationConfigError, match="Model name"):
            svc.create_transformation(
                db_session, org.id, "bad model!", "SELECT 1", Materialization.VIEW
            )

    def test_update_rejects_invalid_name(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session, org.id, "valid_name", "SELECT 1", Materialization.VIEW
        )
        with pytest.raises(TransformationConfigError, match="Model name"):
            svc.update_transformation(db_session, org.id, t.id, name="bad name!")


class TestConnectionAndTags:
    def test_create_with_connection_and_tags(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session, org.id, "tagged_model", "SELECT 1", Materialization.VIEW,
            tags=["finance", "daily"],
        )
        assert t.tags == ["finance", "daily"]
        assert t.destination_connection_id is None

    def test_update_tags(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        updated = svc.update_transformation(
            db_session, org.id, t.id, tags=["updated"]
        )
        assert updated.tags == ["updated"]

    def test_default_tags_empty(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session, org.id, "m", "SELECT 1", Materialization.VIEW
        )
        assert t.tags == []


class TestValidation:
    def test_empty_schema_name_rejected(self):
        with pytest.raises(TransformationConfigError, match="schema_name"):
            TransformationService.validate_schema_name("")

    def test_tests_config_non_dict_rejected(self):
        with pytest.raises(TransformationConfigError, match="tests_config"):
            TransformationService.validate_tests_config("not a dict")

    def test_tests_config_empty_dict_valid(self):
        TransformationService.validate_tests_config({})

    def test_tests_config_none_defaults(self, svc, db_session, org):
        t = svc.create_transformation(
            db_session,
            org.id,
            "m",
            "SELECT 1",
            Materialization.VIEW,
            tests_config=None,
        )
        assert t.tests_config == {}


class TestValidateTestsConfig:
    """Step 21: validate_tests_config structured column tests."""

    VALID_GENERIC_TESTS = {"not_null", "unique", "accepted_values", "relationships"}

    def test_empty_columns_valid(self):
        TransformationService.validate_tests_config({"columns": {}})

    def test_column_with_simple_tests_list(self):
        TransformationService.validate_tests_config({"columns": {"id": ["not_null", "unique"]}})

    def test_column_with_accepted_values_dict(self):
        TransformationService.validate_tests_config(
            {"columns": {"status": {"accepted_values": ["active", "inactive"]}}}
        )

    def test_column_with_relationships_dict(self):
        TransformationService.validate_tests_config(
            {"columns": {"user_id": {"relationships": {"to": "ref('users')", "field": "id"}}}}
        )

    def test_column_with_mixed_tests(self):
        TransformationService.validate_tests_config(
            {
                "columns": {
                    "id": ["not_null", "unique"],
                    "status": {"accepted_values": ["a", "b"]},
                }
            }
        )

    def test_columns_must_be_dict(self):
        with pytest.raises(TransformationConfigError, match="columns"):
            TransformationService.validate_tests_config({"columns": "bad"})

    def test_column_tests_must_be_list_or_dict(self):
        with pytest.raises(TransformationConfigError, match="list or dict"):
            TransformationService.validate_tests_config({"columns": {"id": "not_null"}})

    def test_invalid_test_name_in_list(self):
        with pytest.raises(TransformationConfigError, match="Unknown test"):
            TransformationService.validate_tests_config(
                {"columns": {"id": ["not_null", "nonexistent_test"]}}
            )

    def test_invalid_test_name_in_dict(self):
        with pytest.raises(TransformationConfigError, match="Unknown test"):
            TransformationService.validate_tests_config(
                {"columns": {"id": {"nonexistent_test": []}}}
            )

    def test_accepted_values_must_be_list(self):
        with pytest.raises(TransformationConfigError, match="accepted_values"):
            TransformationService.validate_tests_config(
                {"columns": {"status": {"accepted_values": "bad"}}}
            )

    def test_relationships_must_be_dict(self):
        with pytest.raises(TransformationConfigError, match="relationships"):
            TransformationService.validate_tests_config(
                {"columns": {"fk": {"relationships": "bad"}}}
            )

    def test_relationships_requires_to_and_field(self):
        with pytest.raises(TransformationConfigError, match="to.*field"):
            TransformationService.validate_tests_config(
                {"columns": {"fk": {"relationships": {"to": "ref('x')"}}}}
            )

    def test_unknown_top_level_key_ignored(self):
        """Extra top-level keys besides 'columns' are ignored (forward compatible)."""
        TransformationService.validate_tests_config({"columns": {}, "extra": True})
