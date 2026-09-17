"""SQL Server and Azure Synapse destinations load through FreeTDS (core#1379).

The founder decided FreeTDS for both destinations: Debian's `tdsodbc`, no Microsoft driver and no
driver EULA in the image. Three things stood between that decision and a load, each measured on
core#1379 before this file existed:

1. **dlt refuses any driver not named after Microsoft's.** `MsSqlCredentials.on_resolved` compares
   the name against `SUPPORTED_DRIVERS` and never looks at the library. Registering FreeTDS under
   Microsoft's name would get past it, and would make every Microsoft keyword in a connection
   string a silent no-op while the string still read as if Microsoft's driver honoured it. So the
   gate is widened for FreeTDS by name, in our own spec, and the name stays honest.
2. **FreeTDS ignores Microsoft's encryption keywords without an error.** Encryption has to be asked
   for in FreeTDS's own vocabulary, `Encryption=require`. That setting was measured to produce
   `encrypt_option = TRUE` in the server's `sys.dm_exec_connections`; the keyword's presence is what
   these unit tests pin, and the built-image measurement is what shows it working.
3. **dlt maps its `json` type to a native `json` column, which SQL Server 2022 rejects**
   (`Msg 2715`) with either driver. Synapse's own mapper already writes `nvarchar(max)`; SQL Server
   gets the same.

None of these tests connect to anything, so none needs an ODBC runtime: a driver name that is set
explicitly never reaches `pyodbc.drivers()`.
"""

from __future__ import annotations

import pytest
from dlt.common.exceptions import SystemConfigurationException

from datanika.services.dlt_runner import DltRunnerService

STORED = {
    "host": "sql.example.com",
    "port": 1433,
    "database": "Analytics",
    "user": "u",
    "password": "p",
}


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _resolved_dsn(destination) -> str:
    """Resolve the destination's configuration the way a pipeline does, without connecting.

    ``dataset_name`` is the pipeline's to supply; it is set here only so resolution reaches the
    credentials, which is where the driver gate and the connection string live.
    """
    initial = destination.spec()
    initial.dataset_name = "census"
    config = destination.configuration(initial)
    return config.credentials.to_odbc_dsn()


def _dsn_parts(dsn: str) -> dict[str, str]:
    return dict(part.split("=", 1) for part in dsn.split(";") if part)


class TestTheDriverGate:
    def test_stock_dlt_refuses_freetds_by_name(self):
        """The reason a spec of our own exists. If dlt ever accepts FreeTDS itself, this goes red
        and the subclass can be retired rather than kept out of habit."""
        import dlt

        stock = dlt.destinations.mssql(
            credentials={
                "host": "h",
                "port": 1433,
                "username": "u",
                "password": "p",
                "database": "d",
                "driver": "FreeTDS",
            }
        )
        with pytest.raises(SystemConfigurationException, match="FreeTDS"):
            stock.configuration(stock.spec())

    @pytest.mark.parametrize("destination_type", ["mssql", "synapse"])
    def test_the_destination_resolves_with_freetds(self, svc, destination_type):
        parts = _dsn_parts(_resolved_dsn(svc.build_destination(destination_type, STORED)))

        assert parts["DRIVER"] == "FreeTDS", parts
        assert parts["SERVER"] == "sql.example.com,1433"


class TestEncryptionIsRequestedInFreeTdsVocabulary:
    @pytest.mark.parametrize("destination_type", ["mssql", "synapse"])
    def test_the_connection_string_requires_encryption(self, svc, destination_type):
        """`ENCRYPTION=require` is the FreeTDS keyword measured to encrypt. Microsoft's
        `Encrypt=yes` connects in cleartext through FreeTDS and reports success, so its presence
        would prove nothing -- the assertion is on the keyword that works, not on the absence of
        one that does not."""
        parts = _dsn_parts(_resolved_dsn(svc.build_destination(destination_type, STORED)))

        assert parts.get("ENCRYPTION") == "require", parts

    def test_synapse_keeps_its_long_as_max_keyword(self, svc):
        parts = _dsn_parts(_resolved_dsn(svc.build_destination("synapse", STORED)))

        assert parts.get("LONGASMAX") == "yes", parts


class TestJsonColumnsLoadToSqlServer:
    @staticmethod
    def _json_type(destination) -> str:
        mapper = destination.capabilities().get_type_mapper()
        return mapper.to_destination_type(
            {"name": "payload", "data_type": "json"}, {"name": "rows", "columns": {}}
        )

    def test_stock_dlt_maps_json_to_a_type_sql_server_2022_rejects(self):
        """The defect, pinned so the override is retired when dlt changes rather than forgotten."""
        import dlt

        assert self._json_type(dlt.destinations.mssql()) == "json"

    def test_mssql_maps_json_to_nvarchar_max(self, svc):
        assert self._json_type(svc.build_destination("mssql", STORED)) == "nvarchar(max)"

    def test_other_types_are_unchanged(self, svc):
        mapper = svc.build_destination("mssql", STORED).capabilities().get_type_mapper()
        table = {"name": "rows", "columns": {}}

        assert (
            mapper.to_destination_type({"name": "t", "data_type": "text"}, table) == "nvarchar(max)"
        )
        assert mapper.to_destination_type({"name": "b", "data_type": "bigint"}, table) == "bigint"


class TestTheSourcePathIsUntouched:
    def test_mssql_source_credentials_still_use_pymssql(self, svc):
        """`_to_dlt_credentials` serves the SOURCE path too. The FreeTDS shaping belongs to the
        destination builder only; a `driver` here would reach SQLAlchemy's pymssql dialect."""
        creds = svc._to_dlt_credentials("mssql", STORED)

        assert creds["drivername"] == "mssql+pymssql"
        assert "driver" not in creds
        assert "query" not in creds


class TestDbtProfile:
    def test_sqlserver_profile_names_the_freetds_driver(self):
        from datanika.services.dbt_project import DbtProjectService

        output = DbtProjectService._build_profile_output("mssql", STORED)

        assert output["type"] == "sqlserver"
        assert output["driver"] == "FreeTDS"

    def test_sqlserver_profile_defaults_to_the_sql_server_port(self):
        from datanika.services.dbt_project import DbtProjectService

        config = {k: v for k, v in STORED.items() if k != "port"}

        assert DbtProjectService._build_profile_output("mssql", config)["port"] == 1433
