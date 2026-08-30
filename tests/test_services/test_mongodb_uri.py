"""Regression tests for core#625 — one MongoDB URI, built in one place.

**The defect.** The URI was assembled twice: once in
`dlt_runner._build_mongodb_source` for the run path, once in
`connection_service._test_mongodb` for Test Connection. core#550 added
`authSource` to the first and not the second, so the two disagreed about the
same connection — and in the direction that does the most damage.

`client.server_info()` authenticates for real. Without `authSource` the driver
looks for the user inside the database being read, and MongoDB users live in
`admin` (that is what `MONGO_INITDB_ROOT_USERNAME`, Atlas and every managed
provider create). So **Test Connection failed on connections whose runs
succeeded**: the button that exists to build confidence told the user their
working configuration was broken, and the swallowed error sent them to check
credentials that were already correct.

**Why these tests point at `build_connection_uri` and at `_test_mongodb`, not at
the run path.** A test asserting `authSource=admin` passes trivially against
`_build_mongodb_source` — that half was already fixed. The assertion only
discriminates if it is aimed at the half that was broken, which is the whole
lesson of the issue.
"""

import re
from pathlib import Path

import pytest

from datanika.services.mongodb_source import DEFAULT_AUTH_SOURCE, build_connection_uri


def test_an_authenticated_uri_names_the_auth_source():
    uri = build_connection_uri(
        {"host": "db", "port": 27017, "database": "orders", "user": "svc", "password": "pw"}
    )
    assert "authSource=admin" in uri


def test_the_default_auth_source_is_admin():
    """Not the target database. Changing this reopens core#550 for everyone.

    Anyone who genuinely keeps the user inside the target database sets
    `auth_source` to that database name and gets the old behaviour explicitly.
    """
    assert DEFAULT_AUTH_SOURCE == "admin"


def test_an_explicit_auth_source_wins():
    uri = build_connection_uri(
        {
            "host": "db",
            "database": "orders",
            "user": "svc",
            "password": "pw",
            "auth_source": "orders",
        }
    )
    assert "authSource=orders" in uri


def test_an_unauthenticated_uri_carries_no_auth_source():
    """An unauthenticated mongod rejects nothing; sending one would be noise."""
    uri = build_connection_uri({"host": "db", "port": 27017, "database": "orders"})
    assert uri == "mongodb://db:27017/orders"
    assert "authSource" not in uri


def test_credentials_are_percent_encoded():
    uri = build_connection_uri({"host": "db", "database": "o", "user": "a@b", "password": "p/w+x"})
    assert "a%40b" in uri
    assert "p%2Fw%2Bx" in uri
    assert "a@b:" not in uri


def test_test_connection_builds_the_same_uri_as_a_run(monkeypatch):
    """The actual defect: what `_test_mongodb` hands to pymongo.

    Captured at the `MongoClient` boundary, because that is the value that
    decided whether the user saw a green tick or a false failure.
    """
    from datanika.services.connection_service import ConnectionService

    seen = {}

    class _FakeClient:
        def __init__(self, uri, **kwargs):
            seen["uri"] = uri

        def server_info(self):
            return {"version": "7.0.0"}

        def close(self):
            pass

    monkeypatch.setattr("pymongo.MongoClient", _FakeClient)

    config = {
        "host": "db.example.internal",
        "port": 27017,
        "database": "orders",
        "user": "svc",
        "password": "pw-long-enough",
    }
    ok, _message = ConnectionService.test_connection(
        config, __import__("datanika.models.connection", fromlist=["x"]).ConnectionType.MONGODB
    )

    assert ok is True
    assert seen["uri"] == build_connection_uri(config), (
        "Test Connection must use the same URI the run path uses — assembling it "
        "separately is what made the two disagree (core#625)."
    )
    assert "authSource=admin" in seen["uri"]


@pytest.mark.parametrize(
    "source_file",
    ["datanika/services/connection_service.py", "datanika/services/dlt_runner.py"],
)
def test_nobody_assembles_a_mongo_uri_by_hand_any_more(source_file):
    """The durable half of the fix.

    Fixing both call sites leaves the drift free to recur the next time someone
    needs a URI. This asserts the *shape* of the fix — one builder — rather than
    its current output, so a third `f"mongodb://..."` fails here instead of in
    production six months from now.
    """
    text = Path(source_file).read_text(encoding="utf-8")
    handwritten = re.findall(r'f?"mongodb(?:\+srv)?://\{', text)
    assert not handwritten, (
        f"{source_file} assembles a MongoDB URI inline. Use "
        "`mongodb_source.build_connection_uri` — core#625 is what happens when "
        "two copies drift."
    )
