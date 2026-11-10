"""A unit test that would pass by calling the network must fail (core#1599).

The success condition of the refusal tests is the suite guard's own refusal.
On a tree with no guard the call is not refused, so those tests fail. That is
the regression. A mocked ``subprocess.run`` and a loopback connect are the
controls: the guard does not flag them.
"""

from __future__ import annotations

import ast
import socket
import subprocess
from pathlib import Path

import pytest

from tests.network_guard import OPT_IN, REFUSAL, mark_opt_ins

_PHRASE = "unit suite refuses the network"


def _outcome(call) -> str:
    """Empty when the suite guard refused the call. Otherwise what happened."""
    try:
        call()
    except BaseException as exc:
        if _PHRASE in str(exc):
            return ""
        return f"{type(exc).__name__}: {exc}"
    return "the call returned"


def test_the_refusal_phrase_is_the_guards():
    """The tests match a phrase. It has to be the one the guard raises."""
    assert REFUSAL == _PHRASE


def test_a_public_connect_is_refused():
    """``create_connection`` to a public address is not a unit-test concern.

    1.1.1.1 is an address literal, so this does not depend on DNS. Without the
    guard the call connects or raises a kernel error, and neither is the refusal.
    """

    def _call():
        sock = socket.create_connection(("1.1.1.1", 443), timeout=3)
        sock.close()

    outcome = _outcome(_call)
    assert outcome == "", f"a public connect was not refused ({outcome})"


def test_a_public_name_lookup_is_refused():
    """Resolving a public hostname is a packet. An IP literal is not, and stays allowed."""
    outcome = _outcome(lambda: socket.getaddrinfo("example.com", 443))
    assert outcome == "", f"a public lookup was not refused ({outcome})"
    socket.getaddrinfo("1.1.1.1", 443)


def test_spawning_gh_is_refused():
    """The failure this guard exists for was a test shelling out to ``gh``.

    ``gh api user`` is a live GitHub call when the CLI has credentials, and a
    token-less CLI exit when it does not. Either one is the call returning.
    The guard has to refuse before the process starts.
    """

    def _call():
        subprocess.run(["gh", "api", "user"], check=False, capture_output=True, timeout=15)

    outcome = _outcome(_call)
    assert outcome == "", f"spawning gh was not refused ({outcome})"


def test_loopback_is_still_allowed():
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    try:
        client = socket.create_connection(("127.0.0.1", port), timeout=2)
        client.close()
    finally:
        server.close()


def test_a_mocked_subprocess_is_not_refused(monkeypatch):
    """Patching the seam the test actually calls is not a network call."""

    def _fake(args, *args_, **kwargs):
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(subprocess, "run", _fake)
    assert subprocess.run(["gh", "api", "user"]).returncode == 0


def _smoke_nodeids() -> list[str]:
    root = Path(__file__).resolve().parents[1] / "test_connector_smoke"
    found: list[str] = []
    for path in sorted(root.glob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found.extend(
            f"tests/test_connector_smoke/{path.name}::{name}" for name in _test_names(tree.body)
        )
    return found


def _test_names(body, prefix: str = "") -> list[str]:
    names: list[str] = []
    for node in body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith(
            "test_"
        ):
            names.append(prefix + node.name)
        elif isinstance(node, ast.ClassDef):
            names.extend(_test_names(node.body, prefix + node.name + "::"))
    return names


def test_every_connector_smoke_test_names_the_service_it_calls():
    """The opt-in list is the smoke tests, each with the service it actually calls.

    A new smoke test that is not in the list fails here, before a nightly run
    is the thing that discovers the guard refused it.
    """
    found = _smoke_nodeids()
    assert len(found) >= 20, f"only {len(found)} smoke tests scanned — the scan missed the tree"
    assert sorted(found) == sorted(OPT_IN)
    assert all(isinstance(service, str) and service.strip() for service in OPT_IN.values())


def test_the_marker_is_applied_only_to_the_enumerated_tests():
    class _Item:
        def __init__(self, nodeid: str) -> None:
            self.nodeid = nodeid
            self.markers: list = []

        def add_marker(self, marker) -> None:
            self.markers.append(marker)

    opted = _Item(next(iter(OPT_IN)))
    plain = _Item(
        "tests/test_deploy/test_unit_suite_refuses_the_network.py::test_loopback_is_still_allowed"
    )
    parametrized = _Item(opted.nodeid + "[extra]")
    mark_opt_ins([opted, plain, parametrized])
    assert plain.markers == []
    assert opted.markers[0].mark.kwargs["service"] == OPT_IN[opted.nodeid]
    assert parametrized.markers[0].mark.kwargs["service"] == OPT_IN[opted.nodeid]


def test_needs_network_without_a_service_is_refused():
    from tests.network_guard import service_for

    class _Marker:
        kwargs = {"service": ""}

    class _Item:
        def get_closest_marker(self, name):
            assert name == "needs_network"
            return _Marker()

    with pytest.raises(BaseException, match=_PHRASE):
        service_for(_Item())
