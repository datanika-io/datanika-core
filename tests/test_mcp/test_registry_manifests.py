"""Parity tests for the datanika-mcp registry manifests (#359, refs #355).

``server.json`` (official MCP registry) and ``smithery.yaml`` (Smithery) are
package-metadata manifests that must not drift from the actual package
(``datanika-mcp/pyproject.toml`` + ``server.py``). These tests fail CI on a
version bump or an env-var rename that forgets to update the manifests.
"""

import json
import re
import tomllib
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MCP_DIR = _REPO_ROOT / "datanika-mcp"
_EXPECTED_ENV_VARS = {"DATANIKA_URL", "DATANIKA_API_KEY", "DATANIKA_ALLOW_WRITE"}

# The official registry's namespace for this server. `io.datanika/*` is
# DNS-authenticated (apex TXT on datanika.io); the earlier
# `io.github.datanika-io/*` route was abandoned because the registry needs a
# `read:org` token the mcp-publisher OAuth app can't get for our org.
_REGISTRY_NAME = "io.datanika/datanika-mcp"


def _pyproject() -> dict:
    with (_MCP_DIR / "pyproject.toml").open("rb") as fh:
        return tomllib.load(fh)


def _server_json() -> dict:
    return json.loads((_MCP_DIR / "server.json").read_text(encoding="utf-8"))


def _package_env_var_names() -> set[str]:
    """Env var names the server actually reads, scraped from server.py source."""
    source = (_MCP_DIR / "src" / "datanika_mcp" / "server.py").read_text(encoding="utf-8")
    return set(re.findall(r"DATANIKA_[A-Z_]+", source))


class TestServerJson:
    def test_is_valid_json_with_expected_shape(self):
        data = _server_json()
        assert data["name"] == _REGISTRY_NAME
        assert data["repository"]["subfolder"] == "datanika-mcp"
        assert len(data["packages"]) == 1

    def test_version_matches_pyproject(self):
        version = _pyproject()["project"]["version"]
        data = _server_json()
        assert data["version"] == version
        assert data["packages"][0]["version"] == version

    def test_package_points_at_pypi_identifier(self):
        pkg = _server_json()["packages"][0]
        assert pkg["registryType"] == "pypi"
        assert pkg["identifier"] == _pyproject()["project"]["name"] == "datanika-mcp"
        assert pkg["transport"]["type"] == "stdio"

    def test_env_vars_match_server_source(self):
        names = {e["name"] for e in _server_json()["packages"][0]["environmentVariables"]}
        assert names == _EXPECTED_ENV_VARS
        # No drift vs what server.py actually reads.
        assert names == _package_env_var_names()

    def test_api_key_marked_required_and_secret(self):
        env = _server_json()["packages"][0]["environmentVariables"]
        by_name = {e["name"]: e for e in env}
        assert by_name["DATANIKA_API_KEY"]["isRequired"] is True
        assert by_name["DATANIKA_API_KEY"]["isSecret"] is True
        # URL has a working default in server.py; it must not be marked required.
        assert by_name["DATANIKA_URL"].get("isRequired", False) is False


class TestPypiOwnershipMarker:
    """The official registry proves package ownership via the PyPI README.

    ``mcp-publisher publish`` greps the **published** PyPI README for
    ``mcp-name: <server name>``; without it the publish 400s (which is exactly
    what blocked the 0.1.0 listing). Two things therefore have to hold, and
    neither is obvious from reading either file alone.
    """

    def test_readme_carries_the_mcp_name_marker(self):
        readme = (_MCP_DIR / "README.md").read_text(encoding="utf-8")
        assert f"mcp-name: {_REGISTRY_NAME}" in readme, (
            "datanika-mcp/README.md must carry the `mcp-name:` marker — the "
            "registry reads it from the published PyPI README to prove we own "
            "the package."
        )

    def test_marker_matches_server_json_name(self):
        """A rename in one file without the other silently breaks publishing."""
        readme = (_MCP_DIR / "README.md").read_text(encoding="utf-8")
        declared = re.search(r"^mcp-name:\s*(\S+)", readme, re.MULTILINE)
        assert declared, "no `mcp-name:` line found in datanika-mcp/README.md"
        assert declared.group(1) == _server_json()["name"]

    def test_that_readme_is_the_one_shipped_to_pypi(self):
        """The marker is worthless if the file never reaches PyPI."""
        assert _pyproject()["project"]["readme"] == "README.md"


class TestSmitheryYaml:
    def test_stdio_startcommand_with_api_key_required(self):
        yaml = pytest.importorskip("yaml")
        data = yaml.safe_load((_MCP_DIR / "smithery.yaml").read_text(encoding="utf-8"))
        start = data["startCommand"]
        assert start["type"] == "stdio"
        schema = start["configSchema"]
        assert "apiKey" in schema["required"]
        assert set(schema["properties"]) == {"url", "apiKey", "allowWrite"}

    def test_command_invokes_the_package(self):
        yaml = pytest.importorskip("yaml")
        data = yaml.safe_load((_MCP_DIR / "smithery.yaml").read_text(encoding="utf-8"))
        cmd = data["startCommand"]["commandFunction"]
        assert "datanika-mcp" in cmd
        assert "--api-key" in cmd
