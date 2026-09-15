"""Every phase of a MySQL or ClickHouse connection test has a bound (core#1367).

A connect timeout bounds the TCP connect. For these two drivers, the connect args now also carry a
read and write bound, so the handshake, the login and the probe query each get a limit too:

* PyMySQL gets ``read_timeout`` and ``write_timeout``.
* clickhouse-connect gets ``send_receive_timeout``.

The witness is a loopback server that accepts the connection and sends nothing. The test runs in a
daemon thread with a hard join, so on code without the bound it fails rather than hangs. The
fixture then closes the accepted sockets, which releases the driver.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from datanika.models.connection import ConnectionType
from datanika.services import connection_service
from datanika.services.connection_service import ConnectionService

JOIN_SECONDS = 15


@pytest.fixture
def quiet_server():
    """A loopback server that accepts every connection and never sends a byte."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    held: list[socket.socket] = []

    def accept():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            held.append(conn)

    threading.Thread(target=accept, daemon=True).start()
    try:
        yield srv.getsockname()[1]
    finally:
        srv.close()
        for conn in held:
            conn.close()


@pytest.fixture
def short_io_bound(monkeypatch):
    """One second keeps the test fast. `raising=False` so that, on code without the constant, the
    red is this test's join rather than an AttributeError about the harness."""
    monkeypatch.setattr(connection_service, "_TEST_IO_TIMEOUT_SECONDS", 1, raising=False)


def _unused_port() -> int:
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def _config(port: int) -> dict:
    return {
        "host": "127.0.0.1",
        "port": port,
        "user": "probe",
        "password": "probe",
        "database": "probe",
    }


def _verdict_within(config: dict, connection_type: ConnectionType, seconds: float):
    """(verdict or None, elapsed). None means the test was still running when the join expired."""
    result: dict = {}

    def target():
        result["verdict"] = ConnectionService.test_connection_verdict(config, connection_type)

    started = time.monotonic()
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    return result.get("verdict"), time.monotonic() - started


BOUNDED = [
    pytest.param(ConnectionType.MYSQL, ("read_timeout", "write_timeout"), id="mysql"),
    pytest.param(ConnectionType.CLICKHOUSE, ("send_receive_timeout",), id="clickhouse"),
]


class TestAConnectionTestHasABound:
    @pytest.mark.parametrize("connection_type,_keys", BOUNDED)
    def test_a_server_that_sends_nothing_gets_a_failure_verdict_in_time(
        self, quiet_server, short_io_bound, connection_type, _keys
    ):
        verdict, elapsed = _verdict_within(_config(quiet_server), connection_type, JOIN_SECONDS)

        assert verdict is not None, f"no verdict after {elapsed:.0f}s from a server sending nothing"
        assert verdict.ok is False, verdict

    @pytest.mark.parametrize("connection_type,keys", BOUNDED)
    def test_the_connect_args_carry_the_bound(self, connection_type, keys):
        args = connection_service._connect_args(connection_type, _config(1))

        assert all(args.get(key) for key in keys), args

    @pytest.mark.parametrize("connection_type,_keys", BOUNDED)
    def test_control_a_refused_port_answers_promptly(self, connection_type, _keys):
        """A real connect is attempted and returns normally, before and after the change."""
        verdict, elapsed = _verdict_within(_config(_unused_port()), connection_type, JOIN_SECONDS)

        assert verdict is not None, f"no verdict after {elapsed:.0f}s from a refused port"
        assert verdict.ok is False, verdict
