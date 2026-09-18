"""Every connection type that reaches pymssql passes only arguments pymssql accepts (core#1443).

Synapse's Test Connection answered *"check your credentials and network settings"* for every
connection, because ``_connect_args`` gave it ``connect_timeout``, and ``pymssql.connect`` has a
fixed signature with no such parameter. The call raised ``TypeError`` before any network activity.

The types are DERIVED from ``_build_sa_url``, not listed here: a list is exactly how Synapse was
missed, since the connect-timeout map named only MSSQL. The next type routed to ``mssql+pymssql``
is picked up without anyone remembering to add it.

The server-side witness, a real SQL Server that the Synapse arm now logs in to, is
``tests/test_services/test_pymssql_sessions_are_encrypted.py``.
"""

from __future__ import annotations

import inspect

import pymssql
import pytest

from datanika.errors import UserFacingError
from datanika.models.connection import ConnectionType
from datanika.services.connection_service import _build_sa_url, _connect_args

CONFIG = {
    "host": "db.example.test",
    "port": 1433,
    "user": "probe",
    "password": "probe",
    "database": "probe",
}


def _types_reaching_pymssql() -> list[ConnectionType]:
    found = []
    for connection_type in ConnectionType:
        try:
            url = _build_sa_url(dict(CONFIG), connection_type)
        except UserFacingError:  # the builder's refusal for a type with no SQLAlchemy URL
            url = ""
        if url.startswith("mssql+pymssql://"):
            found.append(connection_type)
    return found


PYMSSQL_TYPES = _types_reaching_pymssql()


def test_the_derivation_finds_the_types_known_to_reach_pymssql():
    """The derivation is an instrument, so it is shown to find what is known to be there."""
    assert {ConnectionType.MSSQL, ConnectionType.SYNAPSE} <= set(PYMSSQL_TYPES), PYMSSQL_TYPES


def test_the_signature_check_can_still_fail():
    """If ``pymssql.connect`` took ``**kwargs``, every case below would pass, whatever it passed."""
    parameters = inspect.signature(pymssql.connect).parameters.values()
    assert not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters), (
        "pymssql.connect now takes **kwargs, so an unknown argument no longer fails at the "
        "signature. Repoint these tests at a connection attempt."
    )


@pytest.mark.parametrize("connection_type", PYMSSQL_TYPES, ids=lambda t: t.value)
def test_connect_args_are_parameters_of_pymssql_connect(connection_type):
    accepted = set(inspect.signature(pymssql.connect).parameters)
    unknown = set(_connect_args(connection_type, dict(CONFIG))) - accepted
    assert not unknown, (
        f"{connection_type.value} reaches pymssql and passes {sorted(unknown)}, which "
        "pymssql.connect does not accept: the call raises TypeError before it reaches the server"
    )
