"""The hosted MCP transport stays stateless, and one private name is not the only path.

core#645. GHSA-jpw9-pfvf-9f58 applies to stateful Streamable HTTP once the
server authenticates. ``/mcp`` authenticates every request, so ``stateless``
has to stay on. ``FastMCP.streamable_http_app`` is the supported way to reach
the low-level server. ``_mcp_server`` and the mcp 2.x rename ``_lowlevel_server``
are fallbacks. This file does not migrate the package to mcp 2.x.
"""

import pathlib
import sys

_MCP_SRC = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
if _MCP_SRC not in sys.path:
    sys.path.insert(0, _MCP_SRC)

from datanika_mcp.server import (  # noqa: E402
    lowlevel_mcp_server,
    make_remote_transport,
    mcp,
)


class _OnlyPublic:
    """A stand-in with the supported app and neither private server name."""

    def __init__(self) -> None:
        self.app = object()
        self._manager = None

    def streamable_http_app(self) -> None:
        self._manager = type("Manager", (), {"app": self.app})()

    @property
    def session_manager(self):
        if self._manager is None:
            raise RuntimeError("streamable_http_app has not been called")
        return self._manager


class _OnlyV1:
    _mcp_server = object()


class _OnlyV2:
    _lowlevel_server = object()


def test_hosted_transport_stays_stateless():
    """Flipping this flag is a decision. The advisory's exclusion is this flag."""
    assert mcp.settings.stateless_http is True
    assert mcp.settings.json_response is True
    asgi, _lifespan = make_remote_transport()
    assert asgi._manager.stateless is True
    assert asgi._manager.json_response is True


def test_public_streamable_app_is_enough_to_reach_the_server():
    fake = _OnlyPublic()
    assert lowlevel_mcp_server(fake) is fake.app
    assert not hasattr(fake, "_mcp_server")
    assert not hasattr(fake, "_lowlevel_server")


def test_v1_and_v2_private_names_both_resolve_when_there_is_no_public_app():
    v1 = _OnlyV1()
    v2 = _OnlyV2()
    assert lowlevel_mcp_server(v1) is v1._mcp_server
    assert lowlevel_mcp_server(v2) is v2._lowlevel_server


def test_the_served_manager_uses_the_resolved_server():
    asgi, _lifespan = make_remote_transport()
    assert asgi._manager.app is lowlevel_mcp_server(mcp)
