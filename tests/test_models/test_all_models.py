"""TDD tests for all database models. Written BEFORE implementation."""

from datetime import datetime

from datanika.models.base import Base


# ---------------------------------------------------------------------------
# Helper to get table columns as a dict of {name: column_obj}
# ---------------------------------------------------------------------------
def _columns(table_name: str) -> dict:
    table = Base.metadata.tables[table_name]
    return {c.name: c for c in table.columns}


def _has_fk_to(table_name: str, col_name: str, target_table: str) -> bool:
    cols = _columns(table_name)
    col = cols[col_name]
    return any(fk.column.table.name == target_table for fk in col.foreign_keys)


def _pk_is_autoincrement(table_name: str) -> bool:
    cols = _columns(table_name)
    return cols["id"].autoincrement is not False


# ===========================================================================
# Organization
# ===========================================================================
class TestOrganization:
    def test_table_exists(self):
        assert "organizations" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("organizations")
        assert "id" in cols
        assert "name" in cols
        assert "slug" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("organizations")

    def test_slug_is_unique(self):
        cols = _columns("organizations")
        assert cols["slug"].unique

    def test_create_organization(self, db_session):
        from datanika.models.user import Organization

        org = Organization(name="Acme Corp", slug="acme-corp")
        db_session.add(org)
        db_session.flush()

        assert isinstance(org.id, int)
        assert org.name == "Acme Corp"
        assert org.slug == "acme-corp"
        assert isinstance(org.created_at, datetime)


# ===========================================================================
# User
# ===========================================================================
class TestUser:
    def test_table_exists(self):
        assert "users" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("users")
        assert "id" in cols
        assert "email" in cols
        assert "password_hash" in cols
        assert "full_name" in cols
        assert "is_active" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("users")

    def test_email_is_unique(self):
        cols = _columns("users")
        assert cols["email"].unique

    def test_create_user(self, db_session):
        from datanika.models.user import User

        user = User(
            email="alice@example.com",
            password_hash="hashed_pw",
            full_name="Alice Smith",
        )
        db_session.add(user)
        db_session.flush()

        assert isinstance(user.id, int)
        assert user.email == "alice@example.com"
        assert user.is_active is True


# ===========================================================================
# Membership (User <-> Organization, many-to-many with role)
# ===========================================================================
class TestMembership:
    def test_table_exists(self):
        assert "memberships" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("memberships")
        assert "id" in cols
        assert "user_id" in cols
        assert "org_id" in cols
        assert "role" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("memberships")

    def test_foreign_keys(self):
        assert _has_fk_to("memberships", "user_id", "users")
        assert _has_fk_to("memberships", "org_id", "organizations")

    def test_role_enum_values(self):
        from datanika.models.user import MemberRole

        assert set(MemberRole) == {
            MemberRole.OWNER,
            MemberRole.ADMIN,
            MemberRole.EDITOR,
            MemberRole.VIEWER,
        }

    def test_create_membership(self, db_session):
        from datanika.models.user import MemberRole, Membership, Organization, User

        org = Organization(name="Acme", slug="acme")
        user = User(email="bob@example.com", password_hash="h", full_name="Bob")
        db_session.add_all([org, user])
        db_session.flush()

        membership = Membership(user_id=user.id, org_id=org.id, role=MemberRole.ADMIN)
        db_session.add(membership)
        db_session.flush()

        assert isinstance(membership.id, int)
        assert membership.role == MemberRole.ADMIN


# ===========================================================================
# Connection
# ===========================================================================
class TestConnection:
    def test_table_exists(self):
        assert "connections" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("connections")
        assert "id" in cols
        assert "org_id" in cols
        assert "name" in cols
        assert "connection_type" in cols
        assert "direction" in cols
        assert "config_encrypted" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("connections")

    def test_org_id_fk(self):
        assert _has_fk_to("connections", "org_id", "organizations")

    def test_enums(self):
        from datanika.models.connection import ConnectionDirection, ConnectionType

        assert ConnectionType.POSTGRES in ConnectionType
        assert ConnectionType.MYSQL in ConnectionType
        assert ConnectionType.REST_API in ConnectionType
        assert ConnectionType.BIGQUERY in ConnectionType
        assert ConnectionType.SNOWFLAKE in ConnectionType
        assert ConnectionType.S3 in ConnectionType
        assert ConnectionType.CSV in ConnectionType

        assert ConnectionDirection.SOURCE in ConnectionDirection
        assert ConnectionDirection.DESTINATION in ConnectionDirection
        assert ConnectionDirection.BOTH in ConnectionDirection

    def test_create_connection(self, db_session):
        from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-conn")
        db_session.add(org)
        db_session.flush()

        conn = Connection(
            org_id=org.id,
            name="Production DB",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.SOURCE,
            config_encrypted="encrypted_blob",
        )
        db_session.add(conn)
        db_session.flush()

        assert isinstance(conn.id, int)
        assert conn.connection_type == ConnectionType.POSTGRES


# ===========================================================================
# Upload
# ===========================================================================
class TestUpload:
    def test_table_exists(self):
        assert "uploads" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("uploads")
        assert "id" in cols
        assert "org_id" in cols
        assert "name" in cols
        assert "description" in cols
        assert "source_connection_id" in cols
        assert "destination_connection_id" in cols
        assert "dlt_config" in cols
        assert "status" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("uploads")

    def test_foreign_keys(self):
        assert _has_fk_to("uploads", "org_id", "organizations")
        assert _has_fk_to("uploads", "source_connection_id", "connections")
        assert _has_fk_to("uploads", "destination_connection_id", "connections")

    def test_status_enum(self):
        from datanika.models.upload import UploadStatus

        assert set(UploadStatus) == {
            UploadStatus.DRAFT,
            UploadStatus.ACTIVE,
            UploadStatus.PAUSED,
            UploadStatus.ERROR,
        }

    def test_create_upload(self, db_session):
        from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
        from datanika.models.upload import Upload, UploadStatus
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-pipe")
        db_session.add(org)
        db_session.flush()

        src = Connection(
            org_id=org.id,
            name="src",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.SOURCE,
            config_encrypted="x",
        )
        dst = Connection(
            org_id=org.id,
            name="dst",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted="y",
        )
        db_session.add_all([src, dst])
        db_session.flush()

        upload = Upload(
            org_id=org.id,
            name="user_sync",
            description="Sync users",
            source_connection_id=src.id,
            destination_connection_id=dst.id,
            dlt_config={"write_disposition": "merge"},
            status=UploadStatus.DRAFT,
        )
        db_session.add(upload)
        db_session.flush()

        assert isinstance(upload.id, int)
        assert upload.status == UploadStatus.DRAFT


# ===========================================================================
# Transformation
# ===========================================================================
class TestTransformation:
    def test_table_exists(self):
        assert "transformations" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("transformations")
        assert "id" in cols
        assert "org_id" in cols
        assert "name" in cols
        assert "description" in cols
        assert "sql_body" in cols
        assert "materialization" in cols
        assert "schema_name" in cols
        assert "tests_config" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("transformations")

    def test_org_id_fk(self):
        assert _has_fk_to("transformations", "org_id", "organizations")

    def test_materialization_enum(self):
        from datanika.models.transformation import Materialization

        assert set(Materialization) == {
            Materialization.VIEW,
            Materialization.TABLE,
            Materialization.INCREMENTAL,
            Materialization.EPHEMERAL,
            Materialization.SNAPSHOT,
        }

    def test_create_transformation(self, db_session):
        from datanika.models.transformation import Materialization, Transformation
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-txf")
        db_session.add(org)
        db_session.flush()

        txf = Transformation(
            org_id=org.id,
            name="stg_users",
            description="Staging users model",
            sql_body="SELECT id, name FROM {{ source('raw', 'users') }}",
            materialization=Materialization.VIEW,
            schema_name="staging",
            tests_config={"columns": {"id": ["unique", "not_null"]}},
        )
        db_session.add(txf)
        db_session.flush()

        assert isinstance(txf.id, int)
        assert txf.materialization == Materialization.VIEW


# ===========================================================================
# Dependency (DAG edges)
# ===========================================================================
class TestDependency:
    def test_table_exists(self):
        assert "dependencies" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("dependencies")
        assert "id" in cols
        assert "org_id" in cols
        assert "upstream_type" in cols
        assert "upstream_id" in cols
        assert "downstream_type" in cols
        assert "downstream_id" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("dependencies")

    def test_node_type_enum(self):
        from datanika.models.dependency import NodeType

        assert set(NodeType) == {NodeType.UPLOAD, NodeType.TRANSFORMATION, NodeType.PIPELINE}

    def test_create_dependency(self, db_session):
        from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
        from datanika.models.dependency import Dependency, NodeType
        from datanika.models.upload import Upload, UploadStatus
        from datanika.models.transformation import Materialization, Transformation
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-dep")
        db_session.add(org)
        db_session.flush()

        # Create a real upload and transformation to reference
        src = Connection(
            org_id=org.id,
            name="s",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.SOURCE,
            config_encrypted="x",
        )
        dst = Connection(
            org_id=org.id,
            name="d",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted="y",
        )
        db_session.add_all([src, dst])
        db_session.flush()

        upload = Upload(
            org_id=org.id,
            name="p",
            source_connection_id=src.id,
            destination_connection_id=dst.id,
            dlt_config={},
            status=UploadStatus.DRAFT,
        )
        txf = Transformation(
            org_id=org.id,
            name="t",
            sql_body="SELECT 1",
            materialization=Materialization.VIEW,
            schema_name="staging",
        )
        db_session.add_all([upload, txf])
        db_session.flush()

        dep = Dependency(
            org_id=org.id,
            upstream_type=NodeType.UPLOAD,
            upstream_id=upload.id,
            downstream_type=NodeType.TRANSFORMATION,
            downstream_id=txf.id,
        )
        db_session.add(dep)
        db_session.flush()

        assert isinstance(dep.id, int)
        assert dep.upstream_id == upload.id
        assert dep.downstream_id == txf.id


# ===========================================================================
# Schedule
# ===========================================================================
class TestSchedule:
    def test_table_exists(self):
        assert "schedules" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("schedules")
        assert "id" in cols
        assert "org_id" in cols
        assert "target_type" in cols
        assert "target_id" in cols
        assert "cron_expression" in cols
        assert "timezone" in cols
        assert "is_active" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("schedules")

    def test_create_schedule(self, db_session):
        from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
        from datanika.models.dependency import NodeType
        from datanika.models.upload import Upload, UploadStatus
        from datanika.models.schedule import Schedule
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-sched")
        db_session.add(org)
        db_session.flush()

        src = Connection(
            org_id=org.id,
            name="s",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.SOURCE,
            config_encrypted="x",
        )
        dst = Connection(
            org_id=org.id,
            name="d",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted="y",
        )
        db_session.add_all([src, dst])
        db_session.flush()

        upload = Upload(
            org_id=org.id,
            name="p",
            source_connection_id=src.id,
            destination_connection_id=dst.id,
            dlt_config={},
            status=UploadStatus.DRAFT,
        )
        db_session.add(upload)
        db_session.flush()

        sched = Schedule(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
            cron_expression="0 3 * * *",
            timezone="UTC",
            is_active=True,
        )
        db_session.add(sched)
        db_session.flush()

        assert isinstance(sched.id, int)
        assert sched.is_active is True
        assert sched.target_id == upload.id


# ===========================================================================
# Run (execution history)
# ===========================================================================
class TestRun:
    def test_table_exists(self):
        assert "runs" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("runs")
        assert "id" in cols
        assert "org_id" in cols
        assert "target_type" in cols
        assert "target_id" in cols
        assert "status" in cols
        assert "started_at" in cols
        assert "finished_at" in cols
        assert "logs" in cols
        assert "rows_loaded" in cols
        assert "error_message" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("runs")

    def test_run_status_enum(self):
        from datanika.models.run import RunStatus

        assert set(RunStatus) == {
            RunStatus.PENDING,
            RunStatus.RUNNING,
            RunStatus.SUCCESS,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        }

    def test_create_run(self, db_session):
        from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
        from datanika.models.dependency import NodeType
        from datanika.models.upload import Upload, UploadStatus
        from datanika.models.run import Run, RunStatus
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-run")
        db_session.add(org)
        db_session.flush()

        src = Connection(
            org_id=org.id,
            name="s",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.SOURCE,
            config_encrypted="x",
        )
        dst = Connection(
            org_id=org.id,
            name="d",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted="y",
        )
        db_session.add_all([src, dst])
        db_session.flush()

        upload = Upload(
            org_id=org.id,
            name="p",
            source_connection_id=src.id,
            destination_connection_id=dst.id,
            dlt_config={},
            status=UploadStatus.DRAFT,
        )
        db_session.add(upload)
        db_session.flush()

        run = Run(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
            status=RunStatus.PENDING,
        )
        db_session.add(run)
        db_session.flush()

        assert isinstance(run.id, int)
        assert run.status == RunStatus.PENDING
        assert run.started_at is None
        assert run.finished_at is None
        assert run.rows_loaded is None


# ===========================================================================
# CatalogEntry
# ===========================================================================
class TestCatalogEntry:
    def test_table_exists(self):
        assert "catalog_entries" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("catalog_entries")
        assert "id" in cols
        assert "org_id" in cols
        assert "entry_type" in cols
        assert "origin_type" in cols
        assert "origin_id" in cols
        assert "table_name" in cols
        assert "schema_name" in cols
        assert "dataset_name" in cols
        assert "connection_id" in cols
        assert "description" in cols
        assert "columns" in cols
        assert "dbt_config" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("catalog_entries")

    def test_connection_id_fk(self):
        assert _has_fk_to("catalog_entries", "connection_id", "connections")

    def test_entry_type_enum(self):
        from datanika.models.catalog_entry import CatalogEntryType

        assert set(CatalogEntryType) == {
            CatalogEntryType.SOURCE_TABLE,
            CatalogEntryType.DBT_MODEL,
        }

    def test_create_catalog_entry(self, db_session):
        from datanika.models.catalog_entry import CatalogEntry, CatalogEntryType
        from datanika.models.dependency import NodeType
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-cat")
        db_session.add(org)
        db_session.flush()

        entry = CatalogEntry(
            org_id=org.id,
            entry_type=CatalogEntryType.DBT_MODEL,
            origin_type=NodeType.TRANSFORMATION,
            origin_id=42,
            table_name="stg_users",
            schema_name="staging",
            dataset_name="analytics",
            description="Staged user table",
            columns=[{"name": "id", "data_type": "INT"}, {"name": "email", "data_type": "TEXT"}],
            dbt_config={"materialized": "view"},
        )
        db_session.add(entry)
        db_session.flush()

        assert isinstance(entry.id, int)
        assert entry.entry_type == CatalogEntryType.DBT_MODEL
        assert entry.origin_type == NodeType.TRANSFORMATION
        assert len(entry.columns) == 2


# ===========================================================================
# UploadedFile
# ===========================================================================
class TestUploadedFile:
    def test_table_exists(self):
        assert "uploaded_files" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("uploaded_files")
        assert "id" in cols
        assert "org_id" in cols
        assert "original_name" in cols
        assert "content_type" in cols
        assert "file_size" in cols
        assert "file_hash" in cols
        assert "archive_path" in cols
        assert "created_at" in cols
        assert "updated_at" in cols
        assert "deleted_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("uploaded_files")

    def test_org_id_fk(self):
        assert _has_fk_to("uploaded_files", "org_id", "organizations")

    def test_create_uploaded_file(self, db_session):
        from datanika.models.uploaded_file import UploadedFile
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-upfile")
        db_session.add(org)
        db_session.flush()

        uf = UploadedFile(
            org_id=org.id,
            original_name="data.csv",
            content_type="csv",
            file_size=1024,
            file_hash="a" * 64,
            archive_path="/tmp/a.tar.gz",
        )
        db_session.add(uf)
        db_session.flush()

        assert isinstance(uf.id, int)
        assert uf.original_name == "data.csv"
        assert uf.content_type == "csv"
        assert uf.file_size == 1024
        assert uf.deleted_at is None


# ===========================================================================
# Cross-model: relationships
# ===========================================================================
class TestRelationships:
    def test_user_memberships_relationship(self, db_session):
        from datanika.models.user import MemberRole, Membership, Organization, User

        org = Organization(name="Acme", slug="acme-rel")
        user = User(email="rel@example.com", password_hash="h", full_name="Rel Test")
        db_session.add_all([org, user])
        db_session.flush()

        m = Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER)
        db_session.add(m)
        db_session.flush()

        db_session.refresh(user)
        assert len(user.memberships) == 1
        assert user.memberships[0].org_id == org.id

    def test_org_memberships_relationship(self, db_session):
        from datanika.models.user import MemberRole, Membership, Organization, User

        org = Organization(name="Acme", slug="acme-rel2")
        user = User(email="rel2@example.com", password_hash="h", full_name="Rel Test2")
        db_session.add_all([org, user])
        db_session.flush()

        m = Membership(user_id=user.id, org_id=org.id, role=MemberRole.VIEWER)
        db_session.add(m)
        db_session.flush()

        db_session.refresh(org)
        assert len(org.memberships) == 1

    def test_catalog_entry_creation(self, db_session):
        from datanika.models.catalog_entry import CatalogEntry, CatalogEntryType
        from datanika.models.dependency import NodeType
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-catalog")
        db_session.add(org)
        db_session.flush()

        entry = CatalogEntry(
            org_id=org.id,
            entry_type=CatalogEntryType.SOURCE_TABLE,
            origin_type=NodeType.UPLOAD,
            origin_id=1,
            table_name="users",
            schema_name="public",
            dataset_name="my_pipeline",
            columns=[{"name": "id", "data_type": "INTEGER"}],
            dbt_config={},
        )
        db_session.add(entry)
        db_session.flush()

        assert isinstance(entry.id, int)
        assert entry.entry_type == CatalogEntryType.SOURCE_TABLE
        assert entry.table_name == "users"
        assert entry.columns == [{"name": "id", "data_type": "INTEGER"}]

    def test_upload_connection_relationships(self, db_session):
        from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
        from datanika.models.upload import Upload, UploadStatus
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-rel3")
        db_session.add(org)
        db_session.flush()

        src = Connection(
            org_id=org.id,
            name="src",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.SOURCE,
            config_encrypted="x",
        )
        dst = Connection(
            org_id=org.id,
            name="dst",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted="y",
        )
        db_session.add_all([src, dst])
        db_session.flush()

        upload = Upload(
            org_id=org.id,
            name="test_pipe",
            source_connection_id=src.id,
            destination_connection_id=dst.id,
            dlt_config={},
            status=UploadStatus.DRAFT,
        )
        db_session.add(upload)
        db_session.flush()

        db_session.refresh(upload)
        assert upload.source_connection.name == "src"
        assert upload.destination_connection.name == "dst"


# ===========================================================================
# Pipeline (dbt)
# ===========================================================================
class TestPipeline:
    def test_table_exists(self):
        assert "pipelines" in Base.metadata.tables

    def test_columns(self):
        cols = _columns("pipelines")
        assert "id" in cols
        assert "org_id" in cols
        assert "name" in cols
        assert "description" in cols
        assert "destination_connection_id" in cols
        assert "command" in cols
        assert "full_refresh" in cols
        assert "models" in cols
        assert "custom_selector" in cols
        assert "status" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_pk_is_integer_autoincrement(self):
        assert _pk_is_autoincrement("pipelines")

    def test_foreign_keys(self):
        assert _has_fk_to("pipelines", "org_id", "organizations")
        assert _has_fk_to("pipelines", "destination_connection_id", "connections")

    def test_dbt_command_enum(self):
        from datanika.models.pipeline import DbtCommand

        assert set(DbtCommand) == {
            DbtCommand.BUILD,
            DbtCommand.RUN,
            DbtCommand.TEST,
            DbtCommand.SEED,
            DbtCommand.SNAPSHOT,
            DbtCommand.COMPILE,
        }

    def test_pipeline_status_enum(self):
        from datanika.models.pipeline import PipelineStatus

        assert set(PipelineStatus) == {
            PipelineStatus.DRAFT,
            PipelineStatus.ACTIVE,
            PipelineStatus.PAUSED,
            PipelineStatus.ERROR,
        }

    def test_create_pipeline(self, db_session):
        from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
        from datanika.models.pipeline import DbtCommand, Pipeline, PipelineStatus
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-dbt-pipe")
        db_session.add(org)
        db_session.flush()

        dst = Connection(
            org_id=org.id,
            name="dst",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted="y",
        )
        db_session.add(dst)
        db_session.flush()

        pipeline = Pipeline(
            org_id=org.id,
            name="nightly_build",
            description="Build all models",
            destination_connection_id=dst.id,
            command=DbtCommand.BUILD,
            full_refresh=False,
            models=[{"name": "orders", "upstream": True, "downstream": False}],
            status=PipelineStatus.DRAFT,
        )
        db_session.add(pipeline)
        db_session.flush()

        assert isinstance(pipeline.id, int)
        assert pipeline.command == DbtCommand.BUILD
        assert pipeline.status == PipelineStatus.DRAFT
        assert pipeline.models == [{"name": "orders", "upstream": True, "downstream": False}]
