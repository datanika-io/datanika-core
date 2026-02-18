"""Tests for rx.Base data model classes used in UI state."""

from datanika.models.connection import ConnectionDirection
from datanika.ui.state.connection_state import ConnectionItem, _infer_direction
from datanika.ui.state.upload_state import UploadItem
from datanika.ui.state.run_state import RunItem
from datanika.ui.state.schedule_state import ScheduleItem
from datanika.ui.state.transformation_state import TransformationItem


class TestConnectionItem:
    def test_create_with_fields(self):
        item = ConnectionItem(id=1, name="My DB", connection_type="postgres")
        assert item.id == 1
        assert item.name == "My DB"
        assert item.connection_type == "postgres"

    def test_defaults(self):
        item = ConnectionItem()
        assert item.id == 0
        assert item.name == ""
        assert item.connection_type == ""


class TestInferDirection:
    def test_both_for_database_types(self):
        for t in ("postgres", "mysql", "mssql", "sqlite"):
            assert _infer_direction(t) == ConnectionDirection.BOTH

    def test_source_only_for_file_and_rest(self):
        for t in ("csv", "json", "parquet", "s3", "rest_api"):
            assert _infer_direction(t) == ConnectionDirection.SOURCE

    def test_destination_only_for_cloud_warehouses(self):
        for t in ("bigquery", "snowflake", "redshift"):
            assert _infer_direction(t) == ConnectionDirection.DESTINATION


class TestUploadItem:
    def test_create_with_fields(self):
        item = UploadItem(
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
        item = UploadItem()
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
            target_type="upload",
            target_id=1,
            cron_expression="0 * * * *",
            timezone="UTC",
            is_active=True,
        )
        assert item.id == 7
        assert item.target_type == "upload"
        assert item.cron_expression == "0 * * * *"
        assert item.is_active is True

    def test_defaults(self):
        item = ScheduleItem()
        assert item.id == 0
        assert item.is_active is True


class TestConnectionBuildConfig:
    """Tests for ConnectionState._build_config()."""

    def _make_state(self, **overrides):
        """Create a ConnectionState-like object with form fields for _build_config."""
        from datanika.ui.state.connection_state import ConnectionState

        defaults = {
            "form_type": "postgres",
            "form_host": "",
            "form_port": "",
            "form_user": "",
            "form_password": "",
            "form_database": "",
            "form_schema": "",
            "form_path": "",
            "form_project": "",
            "form_dataset": "",
            "form_keyfile_json": "",
            "form_account": "",
            "form_warehouse": "",
            "form_role": "",
            "form_bucket_url": "",
            "form_aws_access_key_id": "",
            "form_aws_secret_access_key": "",
            "form_region_name": "",
            "form_endpoint_url": "",
            "form_base_url": "",
            "form_api_key": "",
            "form_extra_headers": "",
            "form_uploaded_file_id": 0,
            "form_uploaded_file_name": "",
            "form_spreadsheet_url": "",
            "form_service_account_json": "",
            "form_use_raw_json": False,
            "form_config": "{}",
        }
        defaults.update(overrides)

        class FakeState:
            pass

        obj = FakeState()
        for k, v in defaults.items():
            setattr(obj, k, v)
        # Bind the real _build_config method
        import types

        obj._build_config = types.MethodType(ConnectionState._build_config, obj)
        return obj

    def test_build_config_postgres(self):
        state = self._make_state(
            form_type="postgres",
            form_host="localhost",
            form_port="5432",
            form_user="admin",
            form_password="secret",
            form_database="mydb",
            form_schema="public",
        )
        config = state._build_config()
        assert config == {
            "host": "localhost",
            "port": 5432,
            "user": "admin",
            "password": "secret",
            "database": "mydb",
            "schema": "public",
        }

    def test_build_config_mysql(self):
        state = self._make_state(
            form_type="mysql",
            form_host="db.example.com",
            form_port="3306",
            form_user="root",
            form_password="pass",
            form_database="shop",
        )
        config = state._build_config()
        assert config["host"] == "db.example.com"
        assert config["port"] == 3306
        assert config["user"] == "root"
        assert config["database"] == "shop"
        assert "schema" not in config  # empty schema omitted

    def test_build_config_mssql(self):
        state = self._make_state(
            form_type="mssql",
            form_host="sql.local",
            form_port="1433",
            form_user="sa",
            form_password="pwd",
            form_database="erp",
        )
        config = state._build_config()
        assert config["host"] == "sql.local"
        assert config["port"] == 1433

    def test_build_config_redshift(self):
        state = self._make_state(
            form_type="redshift",
            form_host="cluster.redshift.amazonaws.com",
            form_port="5439",
            form_user="awsuser",
            form_password="pw",
            form_database="analytics",
            form_schema="raw",
        )
        config = state._build_config()
        assert config["port"] == 5439
        assert config["schema"] == "raw"

    def test_build_config_sqlite(self):
        state = self._make_state(form_type="sqlite", form_path="/data/my.db")
        config = state._build_config()
        assert config == {"path": "/data/my.db"}

    def test_build_config_bigquery(self):
        state = self._make_state(
            form_type="bigquery",
            form_project="my-gcp-project",
            form_dataset="raw_data",
            form_keyfile_json='{"type": "service_account"}',
        )
        config = state._build_config()
        assert config["project"] == "my-gcp-project"
        assert config["dataset"] == "raw_data"
        assert config["keyfile_json"] == '{"type": "service_account"}'

    def test_build_config_bigquery_no_keyfile(self):
        state = self._make_state(
            form_type="bigquery",
            form_project="proj",
            form_dataset="ds",
        )
        config = state._build_config()
        assert "keyfile_json" not in config

    def test_build_config_snowflake(self):
        state = self._make_state(
            form_type="snowflake",
            form_account="abc123.us-east-1",
            form_user="snow_user",
            form_password="snow_pass",
            form_database="ANALYTICS",
            form_warehouse="COMPUTE_WH",
            form_role="SYSADMIN",
            form_schema="PUBLIC",
        )
        config = state._build_config()
        assert config == {
            "account": "abc123.us-east-1",
            "user": "snow_user",
            "password": "snow_pass",
            "database": "ANALYTICS",
            "warehouse": "COMPUTE_WH",
            "role": "SYSADMIN",
            "schema": "PUBLIC",
        }

    def test_build_config_s3(self):
        state = self._make_state(
            form_type="s3",
            form_bucket_url="s3://my-bucket/path",
            form_aws_access_key_id="AKID",
            form_aws_secret_access_key="secret",
            form_region_name="us-east-1",
            form_endpoint_url="https://minio.local:9000",
        )
        config = state._build_config()
        assert config["bucket_url"] == "s3://my-bucket/path"
        assert config["aws_access_key_id"] == "AKID"
        assert config["aws_secret_access_key"] == "secret"
        assert config["region_name"] == "us-east-1"
        assert config["endpoint_url"] == "https://minio.local:9000"

    def test_build_config_s3_no_endpoint(self):
        state = self._make_state(
            form_type="s3",
            form_bucket_url="s3://bucket",
            form_aws_access_key_id="AK",
            form_aws_secret_access_key="SK",
            form_region_name="eu-west-1",
        )
        config = state._build_config()
        assert "endpoint_url" not in config

    def test_build_config_csv(self):
        state = self._make_state(form_type="csv", form_bucket_url="/data/files")
        config = state._build_config()
        assert config == {"bucket_url": "/data/files"}

    def test_build_config_json(self):
        state = self._make_state(form_type="json", form_bucket_url="/data/json")
        config = state._build_config()
        assert config == {"bucket_url": "/data/json"}

    def test_build_config_parquet(self):
        state = self._make_state(form_type="parquet", form_bucket_url="/data/parquet")
        config = state._build_config()
        assert config == {"bucket_url": "/data/parquet"}

    def test_build_config_rest_api(self):
        state = self._make_state(
            form_type="rest_api",
            form_base_url="https://api.example.com",
            form_api_key="my-key",
            form_extra_headers='{"X-Custom": "val"}',
        )
        config = state._build_config()
        assert config["base_url"] == "https://api.example.com"
        assert config["api_key"] == "my-key"
        assert config["extra_headers"] == '{"X-Custom": "val"}'

    def test_build_config_rest_api_minimal(self):
        state = self._make_state(
            form_type="rest_api",
            form_base_url="https://api.example.com",
        )
        config = state._build_config()
        assert config == {"base_url": "https://api.example.com"}
        assert "api_key" not in config
        assert "extra_headers" not in config

    def _call_set_form_type(self, state, value):
        """Call the underlying set_form_type function, bypassing Reflex EventHandler."""
        from datanika.ui.state.connection_state import ConnectionState

        fn = ConnectionState.set_form_type.fn
        fn(state, value)

    def test_port_default_postgres(self):
        state = self._make_state(form_type="mysql", form_port="3306")
        self._call_set_form_type(state, "postgres")
        assert state.form_port == "5432"

    def test_port_default_mysql(self):
        state = self._make_state(form_type="postgres", form_port="5432")
        self._call_set_form_type(state, "mysql")
        assert state.form_port == "3306"

    def test_port_default_mssql(self):
        state = self._make_state(form_type="postgres", form_port="5432")
        self._call_set_form_type(state, "mssql")
        assert state.form_port == "1433"

    def test_port_default_redshift(self):
        state = self._make_state(form_type="postgres", form_port="5432")
        self._call_set_form_type(state, "redshift")
        assert state.form_port == "5439"

    def test_port_cleared_for_non_db_type(self):
        state = self._make_state(form_type="postgres", form_port="5432")
        self._call_set_form_type(state, "s3")
        assert state.form_port == ""

    def test_raw_json_override(self):
        state = self._make_state(
            form_type="postgres",
            form_host="localhost",
            form_use_raw_json=True,
            form_config='{"custom": "value"}',
        )
        config = state._build_config()
        assert config == {"custom": "value"}

    def test_build_config_db_schema_omitted_when_empty(self):
        state = self._make_state(
            form_type="postgres",
            form_host="localhost",
            form_port="5432",
            form_user="u",
            form_password="p",
            form_database="db",
            form_schema="",
        )
        config = state._build_config()
        assert "schema" not in config


class TestRunItem:
    def test_create_with_fields(self):
        item = RunItem(
            id=10,
            target_type="upload",
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
