"""MongoDB authenticates against the database the user actually lives in (core#550).

**The database in a Mongo URI path doubles as the authentication database.** The
builder emitted `mongodb://u:p@host:port/<target-db>` with no `authSource`, so
the driver looked for the user *inside the database being read* — while MongoDB
users are conventionally created in `admin` (`MONGO_INITDB_ROOT_USERNAME`, Atlas,
every managed provider). Every authenticated deployment was rejected.

It went unnoticed because an unauthenticated mongod is unaffected, and that is
exactly what a local dev instance looks like. Same shape as core#492 one
connector over: *the string we construct was checked; what happens when
something consumes it was not.*

The end-to-end proof lives in
`test_source_builders_move_rows.py::TestMongoDbSourceMovesRows`, which drives a
real mongod. This file pins the **decision** rather than the plumbing: which
auth database is used, and whether the escape hatch works — because defaulting
to `admin` is a behaviour change for anyone whose user really does live in the
target database, and that trade-off should be visible.
"""

from unittest.mock import patch

import pytest

from datanika.services.dlt_runner import DEFAULT_MONGO_AUTH_SOURCE, DltRunnerService


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _uri(svc, config, dlt_config=None):
    """The connection URI handed to `mongodb_source`."""
    with patch("datanika.services.mongodb_source.mongodb_source") as source:
        svc.build_source("mongodb", config, dlt_config or {})
    assert source.call_count == 1
    return source.call_args.kwargs["connection_uri"]


BASE = {"host": "db.example.com", "port": 27017, "database": "analytics"}
AUTHED = {**BASE, "user": "reader", "password": "s3cret"}


class TestAuthenticatedConnections:
    def test_auth_source_defaults_to_admin(self, svc):
        """The configuration almost everyone has.

        Leaving the old behaviour as the default would mean the connector stays
        broken unless the user knows to set a field nobody told them about.
        """
        assert "authSource=admin" in _uri(svc, AUTHED)

    def test_the_target_database_is_still_the_one_read(self, svc):
        """authSource changes where the *user* is looked up, not what is loaded."""
        uri = _uri(svc, AUTHED)

        assert "/analytics?" in uri, f"target database changed: {uri}"

    def test_the_auth_database_is_overridable(self, svc):
        """The escape hatch for anyone whose user lives in the target database.

        That configuration works today and this change would otherwise break it,
        so it has to remain expressible — explicitly rather than by accident.
        """
        uri = _uri(svc, {**AUTHED, "auth_source": "analytics"})

        assert "authSource=analytics" in uri
        assert "authSource=admin" not in uri

    def test_credentials_are_still_escaped(self, svc):
        """A password with URI-special characters must not corrupt the string.

        Appending a query is the kind of change that quietly breaks quoting.
        """
        uri = _uri(svc, {**AUTHED, "user": "a@b", "password": "p/w?x=1&y"})

        assert "a%40b" in uri
        assert "p%2Fw%3Fx%3D1%26y" in uri
        # The only `?` is the one starting the query string.
        assert uri.count("?") == 1


class TestUnauthenticatedConnections:
    def test_no_auth_source_is_sent_without_a_user(self, svc):
        """An unauthenticated mongod rejects nothing; sending authSource is noise.

        This is also the configuration that has always worked, so it must not
        change shape.
        """
        uri = _uri(svc, BASE)

        assert "authSource" not in uri
        assert uri == "mongodb://db.example.com:27017/analytics"


class TestTheDefaultIsDeclared:
    def test_the_constant_is_admin(self):
        """Named rather than inlined, so the choice is greppable and reviewable."""
        assert DEFAULT_MONGO_AUTH_SOURCE == "admin"

    def test_it_is_exposed_in_the_connection_schema(self):
        """A setting with no surface is the core#499 mistake.

        Defaulting fixes the standard deployment, but the non-standard one needs
        a way to say so — via the API and the discovery endpoint at minimum.
        """
        from datanika.services.connection_schemas import CONFIG_SCHEMAS

        properties = CONFIG_SCHEMAS["mongodb"]["properties"]
        assert "auth_source" in properties
        assert "admin" in properties["auth_source"]["description"]
