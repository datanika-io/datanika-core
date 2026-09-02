"""TLS and DNS seed list (SRV) for MongoDB — core#626.

Contract: ``docs/specs/SPEC_MONGODB_TLS_SRV.md``. Numbered comments cite its
acceptance criteria.

**What was actually broken.** Every MongoDB URI was a plain ``mongodb://``
string with no transport options, and ``MongoClient('mongodb://…')`` builds no
SSL context at all — so Atlas, DocumentDB, Cosmos DB's Mongo API and any
``net.tls.mode: requireTLS`` deployment could not connect, while the landing
site advertised MongoDB. Atlas also hands the user a ``mongodb+srv://`` string,
which the host+port form could not accept in any form.

⚠️ **AC8 is not in this file and cannot be.** *"A real MongoDB Atlas M0 cluster
connects"* needs a cluster; until that is measured the feature is **shipped,
unverified against Atlas**, and the docs must not say otherwise. What is here is
the half that is decidable without one: the exact bytes handed to pymongo.
"""

import pytest

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import ConnectionService
from datanika.services.mongodb_source import build_connection_uri

BASE = {
    "host": "db.example.internal",
    "port": 27017,
    "database": "orders",
    "user": "svc",
    "password": "pw-long-enough",
}


class TestExistingConnectionsAreUntouched:
    """D6. Not a side effect — a requirement.

    Config is a single Fernet blob, so there is no migration and no backfill: a
    stored connection simply has neither key. Turning TLS on for a server that
    does not offer it converts a working connection into a failing one without
    the user touching anything, so both keys default off, including for
    Atlas-shaped hostnames.
    """

    def test_a_config_with_neither_key_is_byte_identical_to_today(self):
        """AC1. The literal is the pre-change output, written out rather than
        derived — deriving it from the function under test would make this
        assertion true by construction."""
        assert (
            build_connection_uri(BASE)
            == "mongodb://svc:pw-long-enough@db.example.internal:27017/orders?authSource=admin"
        )

    def test_an_unauthenticated_config_is_byte_identical_to_today(self):
        assert (
            build_connection_uri({"host": "db", "port": 27017, "database": "orders"})
            == "mongodb://db:27017/orders"
        )

    def test_tls_is_off_for_an_atlas_shaped_host(self):
        """D3 — do not infer SRV or TLS from the hostname. ``cluster0.abc.mongodb.net``
        is indistinguishable from any other hostname; there is no signal, so
        there must be no guess."""
        uri = build_connection_uri({**BASE, "host": "cluster0.abc.mongodb.net"})
        assert uri.startswith("mongodb://")
        assert "tls=" not in uri


class TestTls:
    def test_tls_on_appends_tls_true(self):
        """AC2."""
        uri = build_connection_uri({**BASE, "tls": True})
        assert "tls=true" in uri
        assert uri.startswith("mongodb://")

    def test_tls_works_without_credentials(self):
        """A ``requireTLS`` mongod with no auth is a real deployment, and the
        unauthenticated branch used to return early before any query string
        existed — the easiest place to lose this."""
        uri = build_connection_uri({"host": "db", "port": 27017, "database": "orders", "tls": True})
        assert uri == "mongodb://db:27017/orders?tls=true"

    def test_the_query_string_stays_a_single_question_mark(self):
        """``test_mongodb_uri.py`` asserts this for the authSource case; two
        options is where a hand-rolled second ``?`` appears."""
        uri = build_connection_uri({**BASE, "tls": True})
        assert uri.count("?") == 1
        assert "authSource=admin" in uri and "tls=true" in uri


class TestSrv:
    def test_srv_uses_the_srv_scheme(self):
        """AC3."""
        assert build_connection_uri({**BASE, "srv": True}).startswith("mongodb+srv://")

    def test_srv_emits_no_port(self):
        """AC3. ``mongodb+srv://host:27017/`` is invalid per the URI spec — the
        SRV records supply the ports."""
        uri = build_connection_uri({**BASE, "srv": True})
        assert ":27017" not in uri
        assert uri == (
            "mongodb+srv://svc:pw-long-enough@db.example.internal/orders?authSource=admin&tls=true"
        )

    def test_srv_forces_tls_even_when_tls_is_false(self):
        """D2. The ``mongodb+srv`` scheme defaults ``tls=true`` and pymongo
        honours it, so a user cannot construct the combination anyway — and a
        control that lies about its effect is worse than no control."""
        uri = build_connection_uri({**BASE, "srv": True, "tls": False})
        assert "tls=true" in uri

    def test_tls_is_emitted_explicitly_rather_than_left_to_the_driver(self):
        """D2. Relying on a driver default that a future pymongo could change is
        how a security property becomes an accident."""
        uri = build_connection_uri({**BASE, "srv": True})
        assert "tls=true" in uri, (
            "the scheme's implicit default is not enough; write the option out"
        )

    def test_credentials_are_still_percent_encoded_under_srv(self):
        uri = build_connection_uri(
            {
                "host": "c.mongodb.net",
                "database": "o",
                "user": "a@b",
                "password": "p/w+x",
                "srv": True,
            }
        )
        assert "a%40b" in uri and "p%2Fw%2Bx" in uri
        assert "a@b:" not in uri


class TestTestConnection:
    def _capture(self, monkeypatch) -> dict:
        seen: dict = {}

        class _FakeClient:
            def __init__(self, uri, **kwargs):
                seen["uri"] = uri
                seen["kwargs"] = kwargs

            def server_info(self):
                return {"version": "7.0.0"}

            def close(self):
                pass

        monkeypatch.setattr("pymongo.MongoClient", _FakeClient)
        return seen

    def test_test_connection_carries_the_transport_options(self, monkeypatch):
        """The run path and Test Connection share one builder (core#625). If
        only one grew TLS they would disagree again, in the direction that does
        the most damage: a false failure on a working configuration."""
        seen = self._capture(monkeypatch)
        config = {**BASE, "srv": True}
        ok, _ = ConnectionService.test_connection(config, ConnectionType.MONGODB)
        assert ok is True
        assert seen["uri"] == build_connection_uri(config)
        assert seen["uri"].startswith("mongodb+srv://")

    def test_srv_gets_a_longer_server_selection_budget(self, monkeypatch):
        """AC9 / D8.

        SRV adds a DNS round trip before any connection is attempted, then TLS
        adds a handshake, against a cluster that may be on another continent. On
        this box that budget is not obviously safe: a dead provider resolver was
        costing 7.9-9.5 s per lookup until 2026-08-29, and a cold
        ``api.paddle.com`` lookup measured 20.1 s.

        A timeout against a *working* cluster surfaces as "connection failed —
        check your credentials", which sends the user to re-check credentials
        that were always correct. That is the worst failure mode a setup flow
        can have.
        """
        seen = self._capture(monkeypatch)
        ConnectionService.test_connection({**BASE, "srv": True}, ConnectionType.MONGODB)
        assert seen["kwargs"]["serverSelectionTimeoutMS"] >= 10000

    def test_a_plain_connection_keeps_the_short_budget(self, monkeypatch):
        """The negative control. Without it the assertion above is satisfied by
        raising the timeout for everything, which would make every failed
        localhost test twice as slow to report."""
        seen = self._capture(monkeypatch)
        ConnectionService.test_connection(BASE, ConnectionType.MONGODB)
        assert seen["kwargs"]["serverSelectionTimeoutMS"] == 5000


class TestFailuresDoNotLeakThePassword:
    """AC11. ``describe_connection_failure`` redacts URI-embedded credentials and
    is tested with ``mongodb://`` URIs; ``mongodb+srv://`` is a different scheme
    and a redactor keyed on the old one would silently stop matching."""

    @pytest.mark.parametrize("srv", [False, True])
    def test_the_password_is_absent_from_a_failure_message(self, monkeypatch, srv):
        from pymongo.errors import ServerSelectionTimeoutError

        uri = build_connection_uri({**BASE, "srv": srv})

        class _FakeClient:
            def __init__(self, *a, **k):
                pass

            def server_info(self):
                raise ServerSelectionTimeoutError(f"{uri}: connection refused")

            def close(self):
                pass

        monkeypatch.setattr("pymongo.MongoClient", _FakeClient)
        ok, message = ConnectionService.test_connection(
            {**BASE, "srv": srv}, ConnectionType.MONGODB
        )
        assert ok is False
        assert "pw-long-enough" not in message, message
