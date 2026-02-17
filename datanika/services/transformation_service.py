"""Transformation management service — CRUD with dbt config validation."""

import re
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.transformation import Materialization, Transformation

_MODEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


class TransformationConfigError(ValueError):
    """Raised when transformation configuration fails validation."""


class TransformationService:
    def create_transformation(
        self,
        session: Session,
        org_id: int,
        name: str,
        sql_body: str,
        materialization: Materialization,
        description: str | None = None,
        schema_name: str = "staging",
        tests_config: dict | None = None,
        destination_connection_id: int | None = None,
        tags: list[str] | None = None,
    ) -> Transformation:
        self.validate_model_name(name)
        self.validate_sql_body(sql_body)
        self.validate_schema_name(schema_name)
        if tests_config is None:
            tests_config = {}
        self.validate_tests_config(tests_config)

        transformation = Transformation(
            org_id=org_id,
            name=name,
            sql_body=sql_body,
            materialization=materialization,
            description=description,
            schema_name=schema_name,
            tests_config=tests_config,
            destination_connection_id=destination_connection_id,
            tags=tags or [],
        )
        session.add(transformation)
        session.flush()
        return transformation

    def get_transformation(
        self, session: Session, org_id: int, transformation_id: int
    ) -> Transformation | None:
        stmt = select(Transformation).where(
            Transformation.id == transformation_id,
            Transformation.org_id == org_id,
            Transformation.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    def list_transformations(self, session: Session, org_id: int) -> list[Transformation]:
        stmt = (
            select(Transformation)
            .where(Transformation.org_id == org_id, Transformation.deleted_at.is_(None))
            .order_by(Transformation.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def update_transformation(
        self, session: Session, org_id: int, transformation_id: int, **kwargs
    ) -> Transformation | None:
        transformation = self.get_transformation(session, org_id, transformation_id)
        if transformation is None:
            return None

        if "sql_body" in kwargs:
            self.validate_sql_body(kwargs["sql_body"])
            transformation.sql_body = kwargs["sql_body"]
        if "name" in kwargs:
            self.validate_model_name(kwargs["name"])
            transformation.name = kwargs["name"]
        if "description" in kwargs:
            transformation.description = kwargs["description"]
        if "materialization" in kwargs:
            transformation.materialization = kwargs["materialization"]
        if "schema_name" in kwargs:
            self.validate_schema_name(kwargs["schema_name"])
            transformation.schema_name = kwargs["schema_name"]
        if "tests_config" in kwargs:
            self.validate_tests_config(kwargs["tests_config"])
            transformation.tests_config = kwargs["tests_config"]
        if "destination_connection_id" in kwargs:
            transformation.destination_connection_id = kwargs["destination_connection_id"]
        if "tags" in kwargs:
            transformation.tags = kwargs["tags"]

        session.flush()
        return transformation

    def delete_transformation(self, session: Session, org_id: int, transformation_id: int) -> bool:
        transformation = self.get_transformation(session, org_id, transformation_id)
        if transformation is None:
            return False
        transformation.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    @staticmethod
    def validate_model_name(name: str) -> None:
        if not name or not name.strip():
            raise TransformationConfigError("Model name cannot be empty")
        if not _MODEL_NAME_RE.match(name):
            raise TransformationConfigError(
                "Model name must start with a letter or underscore and contain only "
                "letters, digits, underscores, and hyphens"
            )

    @staticmethod
    def validate_sql_body(sql_body: str) -> None:
        if not sql_body or not sql_body.strip():
            raise TransformationConfigError("sql_body must not be empty or whitespace")

    @staticmethod
    def validate_schema_name(schema_name: str) -> None:
        if not schema_name or not schema_name.strip():
            raise TransformationConfigError("schema_name must not be empty or whitespace")

    VALID_GENERIC_TESTS = {"not_null", "unique", "accepted_values", "relationships"}

    @staticmethod
    def validate_tests_config(tests_config) -> None:
        if not isinstance(tests_config, dict):
            raise TransformationConfigError("tests_config must be a dict")

        columns = tests_config.get("columns")
        if columns is None:
            return  # no column tests is valid

        if not isinstance(columns, dict):
            raise TransformationConfigError("columns must be a dict")

        valid = TransformationService.VALID_GENERIC_TESTS
        for col_name, col_tests in columns.items():
            if isinstance(col_tests, list):
                for test_name in col_tests:
                    if test_name not in valid:
                        raise TransformationConfigError(
                            f"Unknown test '{test_name}' on column '{col_name}'"
                        )
            elif isinstance(col_tests, dict):
                for test_name, test_cfg in col_tests.items():
                    if test_name not in valid:
                        raise TransformationConfigError(
                            f"Unknown test '{test_name}' on column '{col_name}'"
                        )
                    if test_name == "accepted_values" and not isinstance(test_cfg, list):
                        raise TransformationConfigError("accepted_values must be a list")
                    if test_name == "relationships":
                        if not isinstance(test_cfg, dict):
                            raise TransformationConfigError("relationships must be a dict")
                        if "to" not in test_cfg or "field" not in test_cfg:
                            raise TransformationConfigError(
                                "relationships requires 'to' and 'field' keys"
                            )
            else:
                raise TransformationConfigError(
                    f"Tests for column '{col_name}' must be a list or dict"
                )
