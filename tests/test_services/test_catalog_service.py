"""TDD tests for catalog service."""

import pytest
from sqlalchemy import create_engine, text

from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.dependency import NodeType
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService


@pytest.fixture
def svc():
    return CatalogService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-catalog-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-catalog-svc")
    db_session.add(org)
    db_session.flush()
    return org


class TestIntrospectTables:
    def test_returns_table_and_columns(self):
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
            conn.commit()
        result = CatalogService.introspect_tables(
            "sqlite:///:memory:", schema_name=None,
        )
        # sqlite in-memory is ephemeral per connection, use a file-based test
        # Instead, test with a temp file
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        url = f"sqlite:///{db_path}"
        eng = create_engine(url)
        with eng.connect() as conn:
            conn.execute(text("CREATE TABLE orders (id INTEGER, amount REAL)"))
            conn.execute(text("CREATE TABLE customers (cid INTEGER, name TEXT)"))
            conn.commit()
        eng.dispose()

        result = CatalogService.introspect_tables(url, schema_name=None)
        table_names = [t["table_name"] for t in result]
        assert "orders" in table_names
        assert "customers" in table_names
        # Check columns
        orders = next(t for t in result if t["table_name"] == "orders")
        col_names = [c["name"] for c in orders["columns"]]
        assert "id" in col_names
        assert "amount" in col_names

    def test_filters_dlt_system_tables(self):
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        url = f"sqlite:///{db_path}"
        eng = create_engine(url)
        with eng.connect() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER)"))
            conn.execute(text("CREATE TABLE _dlt_loads (load_id TEXT)"))
            conn.execute(text("CREATE TABLE _dlt_version (version INTEGER)"))
            conn.commit()
        eng.dispose()

        result = CatalogService.introspect_tables(url, schema_name=None)
        table_names = [t["table_name"] for t in result]
        assert "users" in table_names
        assert "_dlt_loads" not in table_names
        assert "_dlt_version" not in table_names

    def test_filter_by_table_names(self):
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        url = f"sqlite:///{db_path}"
        eng = create_engine(url)
        with eng.connect() as conn:
            conn.execute(text("CREATE TABLE t1 (id INTEGER)"))
            conn.execute(text("CREATE TABLE t2 (id INTEGER)"))
            conn.execute(text("CREATE TABLE t3 (id INTEGER)"))
            conn.commit()
        eng.dispose()

        result = CatalogService.introspect_tables(url, schema_name=None, table_names=["t1", "t3"])
        table_names = [t["table_name"] for t in result]
        assert table_names == ["t1", "t3"]


class TestUpsertEntry:
    def test_creates_new_entry(self, svc, db_session, org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "users", "public", "my_pipeline",
            columns=[{"name": "id", "data_type": "INTEGER"}],
            connection_id=None,
        )
        assert entry.id is not None
        assert entry.table_name == "users"
        assert entry.entry_type == CatalogEntryType.SOURCE_TABLE

    def test_updates_existing_entry(self, svc, db_session, org):
        entry1 = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "users", "public", "my_pipeline",
            columns=[{"name": "id", "data_type": "INTEGER"}],
        )
        entry2 = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "users", "public", "my_pipeline",
            columns=[
                {"name": "id", "data_type": "INTEGER"},
                {"name": "email", "data_type": "TEXT"},
            ],
        )
        assert entry1.id == entry2.id
        assert len(entry2.columns) == 2

    def test_different_dataset_creates_separate(self, svc, db_session, org):
        e1 = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "users", "public", "dataset_a",
        )
        e2 = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 2,
            "users", "public", "dataset_b",
        )
        assert e1.id != e2.id


class TestGetEntry:
    def test_existing(self, svc, db_session, org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "orders", "public", "ds",
        )
        found = svc.get_entry(db_session, org.id, entry.id)
        assert found is not None
        assert found.id == entry.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_entry(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "orders", "public", "ds",
        )
        assert svc.get_entry(db_session, other_org.id, entry.id) is None


class TestListEntries:
    def test_empty(self, svc, db_session, org):
        assert svc.list_entries(db_session, org.id) == []

    def test_returns_all_types(self, svc, db_session, org):
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds",
        )
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.DBT_MODEL, NodeType.TRANSFORMATION, 2,
            "t2", "staging", "ds",
        )
        assert len(svc.list_entries(db_session, org.id)) == 2

    def test_filter_by_type(self, svc, db_session, org):
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds",
        )
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.DBT_MODEL, NodeType.TRANSFORMATION, 2,
            "t2", "staging", "ds",
        )
        sources = svc.list_entries(db_session, org.id, entry_type=CatalogEntryType.SOURCE_TABLE)
        assert len(sources) == 1
        assert sources[0].table_name == "t1"

    def test_excludes_deleted(self, svc, db_session, org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds",
        )
        svc.delete_entry(db_session, org.id, entry.id)
        assert svc.list_entries(db_session, org.id) == []

    def test_filters_by_org(self, svc, db_session, org, other_org):
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds",
        )
        svc.upsert_entry(
            db_session, other_org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t2", "public", "ds",
        )
        result = svc.list_entries(db_session, org.id)
        assert len(result) == 1
        assert result[0].table_name == "t1"


class TestUpdateEntry:
    def test_update_description(self, svc, db_session, org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds",
        )
        updated = svc.update_entry(db_session, org.id, entry.id, description="New desc")
        assert updated.description == "New desc"

    def test_update_columns(self, svc, db_session, org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds",
            columns=[{"name": "id", "data_type": "INT"}],
        )
        new_cols = [{"name": "id", "data_type": "INT"}, {"name": "name", "data_type": "TEXT"}]
        updated = svc.update_entry(db_session, org.id, entry.id, columns=new_cols)
        assert len(updated.columns) == 2

    def test_update_dbt_config(self, svc, db_session, org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.DBT_MODEL, NodeType.TRANSFORMATION, 1,
            "t1", "staging", "ds",
        )
        updated = svc.update_entry(
            db_session, org.id, entry.id, dbt_config={"materialized": "table"},
        )
        assert updated.dbt_config == {"materialized": "table"}

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_entry(db_session, org.id, 99999, description="x") is None


class TestDeleteEntry:
    def test_soft_deletes(self, svc, db_session, org):
        entry = svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds",
        )
        assert svc.delete_entry(db_session, org.id, entry.id) is True
        assert svc.get_entry(db_session, org.id, entry.id) is None

    def test_nonexistent(self, svc, db_session, org):
        assert svc.delete_entry(db_session, org.id, 99999) is False


class TestGetEntriesByConnection:
    def test_returns_source_tables_for_connection(self, svc, db_session, org):
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds", connection_id=10,
        )
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t2", "public", "ds", connection_id=10,
        )
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 2,
            "t3", "public", "ds2", connection_id=20,
        )
        result = svc.get_entries_by_connection(db_session, org.id, 10)
        assert len(result) == 2
        assert {e.table_name for e in result} == {"t1", "t2"}

    def test_excludes_dbt_models(self, svc, db_session, org):
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.SOURCE_TABLE, NodeType.UPLOAD, 1,
            "t1", "public", "ds", connection_id=10,
        )
        svc.upsert_entry(
            db_session, org.id,
            CatalogEntryType.DBT_MODEL, NodeType.TRANSFORMATION, 2,
            "model1", "staging", "ds", connection_id=10,
        )
        result = svc.get_entries_by_connection(db_session, org.id, 10)
        assert len(result) == 1
        assert result[0].table_name == "t1"
