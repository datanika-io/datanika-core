"""SQL Server and Azure Synapse destinations, loading through FreeTDS (core#1379).

The founder decided FreeTDS for both destinations: Debian's ``tdsodbc``, so no Microsoft driver
and no driver EULA ships inside an AGPL image. The limitation that follows from it -- the server
certificate is not verified -- is stated in each destination's documentation, not hidden here.

Three things stood between that decision and a load, each measured on core#1379:

1. **dlt refuses a driver by NAME.** ``MsSqlCredentials.on_resolved`` compares ``driver`` with
   ``SUPPORTED_DRIVERS`` (Microsoft's two names) and never looks at which library the name loads.
   Registering FreeTDS in ``odbcinst.ini`` under Microsoft's name would get past it -- and would
   leave every connection string reading as if Microsoft's driver honoured Microsoft's keywords,
   while FreeTDS silently ignores them. So the gate is widened here, for FreeTDS, by its own name.

   ⚠️ It has to be widened in the destination's SPEC, not only on a credentials subclass: dlt
   resolves the ``credentials`` field against the type the client configuration declares, so a
   subclass instance handed to the stock factory is re-resolved as ``MsSqlCredentials`` and
   refused. Measured.

2. **FreeTDS ignores Microsoft's encryption keywords without an error.** ``Encrypt=yes`` connects
   in cleartext and reports success. Encryption is requested in FreeTDS's own vocabulary,
   ``Encryption=require`` (dlt upper-cases query keys, and the upper-cased keyword was measured to
   work), which gave ``encrypt_option = TRUE`` in the server's own ``sys.dm_exec_connections``.
   The image also sets it in FreeTDS's ``[global]`` section, for dbt-sqlserver, whose connection
   string is not ours. A test of this must read the server's DMV -- never the keyword -- because
   the keyword's presence is exactly the reading that passes while a session is cleartext.

3. **dlt maps ``json`` to a native ``json`` column**, which SQL Server 2022 rejects with
   ``Msg 2715`` whichever driver is used. Synapse's own mapper already writes ``nvarchar(max)``;
   SQL Server gets the same here.
"""

from __future__ import annotations

from typing import ClassVar

import dlt
from dlt.common.configuration import configspec
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials
from dlt.destinations.impl.mssql.factory import MsSqlTypeMapper
from dlt.destinations.impl.synapse.configuration import (
    SynapseClientConfiguration,
    SynapseCredentials,
)

#: The name Debian's ``tdsodbc`` registers in ``/etc/odbcinst.ini``. The Dockerfile asserts
#: ``pyodbc.drivers()`` contains exactly this, and the dbt profile asks for it too.
FREETDS_DRIVER = "FreeTDS"

#: FreeTDS's own keyword. Microsoft's ``Encrypt=yes`` would be silently ignored (see above).
FREETDS_ENCRYPTION = {"encryption": "require"}


@configspec(init=False)
class FreeTdsMsSqlCredentials(MsSqlCredentials):
    SUPPORTED_DRIVERS: ClassVar[list[str]] = [FREETDS_DRIVER]


@configspec(init=False)
class FreeTdsSynapseCredentials(SynapseCredentials):
    SUPPORTED_DRIVERS: ClassVar[list[str]] = [FREETDS_DRIVER]


@configspec
class FreeTdsMsSqlClientConfiguration(MsSqlClientConfiguration):
    credentials: FreeTdsMsSqlCredentials = None


@configspec
class FreeTdsSynapseClientConfiguration(SynapseClientConfiguration):
    credentials: FreeTdsSynapseCredentials = None


class SqlServer2022TypeMapper(MsSqlTypeMapper):
    """dlt's SQL Server mapper, writing ``json`` the way dlt's own Synapse mapper does."""

    def to_destination_type(self, column, table):
        if column["data_type"] == "json":
            return f"nvarchar({column.get('precision', 'max')})"
        return super().to_destination_type(column, table)


class mssql_freetds(dlt.destinations.mssql):  # noqa: N801 - dlt factories are lower-case classes
    spec = FreeTdsMsSqlClientConfiguration

    def _raw_capabilities(self):
        caps = super()._raw_capabilities()
        caps.type_mapper = SqlServer2022TypeMapper
        return caps


class synapse_freetds(dlt.destinations.synapse):  # noqa: N801
    spec = FreeTdsSynapseClientConfiguration


FREETDS_DESTINATIONS = {"mssql": mssql_freetds, "synapse": synapse_freetds}


def freetds_credentials(credentials: dict) -> dict:
    """Destination credentials for FreeTDS, from the shape ``_to_dlt_credentials`` produces.

    ``drivername`` is dropped: it is the SOURCE path's SQLAlchemy dialect (``mssql+pymssql``), and
    the destination's own ``drivername`` is a ``Final`` field on dlt's class.
    """
    shaped = {k: v for k, v in credentials.items() if k != "drivername"}
    shaped["driver"] = FREETDS_DRIVER
    shaped["query"] = {**(shaped.get("query") or {}), **FREETDS_ENCRYPTION}
    return shaped
