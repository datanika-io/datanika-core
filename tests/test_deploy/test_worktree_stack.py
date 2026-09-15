"""A worktree's local stack must be isolated by default, and refused when it is not (core#1197).

The three collisions QA measured on 2026-09-15, each silent -- a shared image tag, fixed container
names and default host ports, and Reflex's default `api_url` -- are described in
`scripts/worktree_stack.py`.

Where the line is:

* `identity`, `overlay` and `check` are pure functions over compose's JSON rendering. They are
  tested directly, and each refusal is seen on a configuration that has exactly that one defect.
* The overlay is only worth anything if COMPOSE merges it the way the pure tests assume: `!override`
  replacing ports, environment merged, container names replaced. `TestRealCompose` renders the
  repository's own compose files with the generated overlay on stdin and checks the result, with
  the plain render as the control that must be refused. `docker compose config` starts nothing and
  needs no daemon; those tests skip only where there is no docker CLI at all.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "scripts" / "worktree_stack.py"
SHELL = ROOT / "scripts" / "worktree-stack.sh"

_spec = importlib.util.spec_from_file_location("worktree_stack", MODULE)
assert _spec and _spec.loader, f"cannot load {MODULE}"
ws = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ws)

AGENT = "infra"
OFFSET = ws.OFFSETS[AGENT]
OWN_IMAGE = f"{ws.IMAGE_REPO}:worktree-{AGENT}"


def _port(published: int, target: int, host_ip: str = "127.0.0.1") -> dict:
    return {
        "mode": "ingress",
        "host_ip": host_ip,
        "target": target,
        "published": str(published),
        "protocol": "tcp",
    }


# The shapes `docker compose config --format json` produced for the real files on 2026-09-15.
BASE = {
    "name": "datanika-core-infra",
    "services": {
        "app": {
            "container_name": "datanika-app",
            "image": OWN_IMAGE,
            "ports": [_port(3000, 3000), _port(8000, 8000)],
            "environment": {"DATANIKA_EDITION": "cloud"},
        },
        "app_b": {
            "container_name": "datanika-app-b",
            "image": OWN_IMAGE,
            "ports": [_port(3010, 3000), _port(8010, 8000)],
        },
        "celery": {"container_name": "datanika-celery", "image": OWN_IMAGE},
        "grafana": {
            "container_name": "datanika-grafana",
            "image": "grafana/grafana:latest",
            "ports": [_port(3001, 3000)],
        },
        "postgres": {
            "container_name": "datanika-postgres",
            "image": "postgres:16-alpine",
            "ports": [_port(5432, 5432)],
        },
        "proxy": {
            "container_name": "datanika-local-proxy",
            "image": "nginx:1.27-alpine",
            "ports": [_port(3100, 80)],
        },
    },
}


def _load_overlay(text: str) -> dict:
    """Drop compose's `!override` / `!reset` tags, then `safe_load` -- no custom loader."""
    return yaml.safe_load(text.replace(" !override", "").replace(" !reset", ""))


def _apply(base: dict, overlay_text: str) -> dict:
    """What compose does with this overlay, for the keys it sets. TestRealCompose checks this."""
    doc = _load_overlay(overlay_text)
    merged = copy.deepcopy(base)
    merged["name"] = doc["name"]
    for name, change in doc["services"].items():
        service = merged["services"][name]
        service["container_name"] = change["container_name"]
        if "ports" in change:
            service["ports"] = []
            for spec in change["ports"]:
                host_ip, published, target = spec.split(":")
                service["ports"].append(_port(int(published), int(target), host_ip))
        if "environment" in change:
            service.setdefault("environment", {}).update(change["environment"])
    return merged


def _isolated() -> dict:
    return _apply(BASE, ws.overlay(BASE, AGENT, OFFSET))


def _problems(config: dict) -> list[str]:
    return ws.check(config, AGENT, OFFSET)[1]


class TestIdentity:
    def test_each_department_gets_its_own_tag_project_and_band(self, tmp_path: Path) -> None:
        bands = {}
        for dept in ws.OFFSETS:
            core = tmp_path / f"datanika-core-{dept}"
            core.mkdir()
            found = ws.identity(core)
            assert found["tag"] == f"{ws.IMAGE_REPO}:worktree-{dept}"
            assert found["project"] == f"wt-{dept}"
            bands[dept] = int(found["offset"])
        assert len(set(bands.values())) == len(bands), f"two departments share a band: {bands}"

    def test_the_old_shared_tag_is_nobodys(self, tmp_path: Path) -> None:
        core = tmp_path / "datanika-core-qa"
        core.mkdir()
        assert ws.identity(core)["tag"] != f"{ws.IMAGE_REPO}:worktree"

    def test_a_worktree_with_no_band_must_choose_one(self, tmp_path: Path) -> None:
        core = tmp_path / "datanika-core-newdept"
        core.mkdir()
        with pytest.raises(ws.RefusalError, match="--port-offset"):
            ws.identity(core)
        assert ws.identity(core, 40_000)["offset"] == "40000"

    @pytest.mark.parametrize("offset", [0, 15_000, 60_000])
    def test_a_band_that_straddles_or_overflows_is_refused(self, tmp_path: Path, offset) -> None:
        core = tmp_path / "datanika-core-x"
        core.mkdir()
        with pytest.raises(ws.RefusalError, match="multiple of"):
            ws.identity(core, offset)

    def test_a_non_canonical_directory_is_refused(self, tmp_path: Path) -> None:
        core = tmp_path / "some-checkout"
        core.mkdir()
        with pytest.raises(ws.RefusalError, match="datanika-core-"):
            ws.identity(core)


class TestOverlay:
    def test_it_makes_the_fixture_isolated(self) -> None:
        assert _problems(_isolated()) == []

    def test_each_reflex_server_is_sent_to_its_own_backend(self) -> None:
        doc = _load_overlay(ws.overlay(BASE, AGENT, OFFSET))
        assert doc["services"]["app"]["environment"]["REFLEX_API_URL"] == "http://localhost:38000"
        assert doc["services"]["app_b"]["environment"]["REFLEX_API_URL"] == "http://localhost:38010"
        # grafana publishes a 3000 target but has no backend: it is not a Reflex server.
        assert "environment" not in doc["services"]["grafana"]

    def test_an_empty_render_is_refused(self) -> None:
        with pytest.raises(ws.RefusalError):
            ws.overlay({"services": {}}, AGENT, OFFSET)


class TestCheckRefusesEachCollision:
    """Each defect alone, on a configuration that is otherwise isolated."""

    def test_a_default_container_name(self) -> None:
        config = _isolated()
        config["services"]["proxy"]["container_name"] = "datanika-local-proxy"
        assert [p for p in _problems(config) if "container name" in p]

    def test_a_default_host_port(self) -> None:
        config = _isolated()
        config["services"]["proxy"]["ports"][0]["published"] = "3100"
        assert [p for p in _problems(config) if "outside" in p]

    def test_a_port_in_another_departments_band(self) -> None:
        config = _isolated()
        config["services"]["proxy"]["ports"][0]["published"] = "13100"
        assert [p for p in _problems(config) if "outside" in p]

    def test_a_port_bound_to_every_interface(self) -> None:
        config = _isolated()
        config["services"]["postgres"]["ports"][0]["host_ip"] = ""
        assert [p for p in _problems(config) if "every interface" in p]

    def test_the_default_backend_url(self) -> None:
        config = _isolated()
        config["services"]["app"]["environment"]["REFLEX_API_URL"] = "http://localhost:8000"
        assert [p for p in _problems(config) if "REFLEX_API_URL" in p]

    def test_the_shared_image_tag(self) -> None:
        config = _isolated()
        config["services"]["celery"]["image"] = f"{ws.IMAGE_REPO}:worktree"
        assert [p for p in _problems(config) if "image" in p]

    def test_the_default_project(self) -> None:
        config = _isolated()
        config["name"] = "datanika-core-infra"
        assert [p for p in _problems(config) if "project" in p]

    def test_an_empty_configuration_is_not_isolated(self) -> None:
        assert _problems({"name": f"wt-{AGENT}", "services": {}})


def test_the_stack_script_writes_nothing_to_disk() -> None:
    """core#1197 AC4: the overlay is streamed. Nothing may be staged in a shared directory."""
    lines = SHELL.read_text(encoding="utf-8").splitlines()
    body = "\n".join(ln for ln in lines if not ln.lstrip().startswith("#"))
    for forbidden in ("mktemp", "tee ", "mkdir ", "cp ", "mv ", "rm "):
        assert forbidden not in body, f"{forbidden!r} in worktree-stack.sh"
    for target in re.findall(r"(?<![0-9&])>{1,2}\s*([^\s;|)]+)", body):
        assert target in ("/dev/null", "&2", "&1"), f"worktree-stack.sh redirects into {target!r}"


# ------------------------------------------------------------------------------------------------
# Compose itself. `config` renders client-side: no daemon, no containers, no pulls.
# ------------------------------------------------------------------------------------------------


def _compose_version() -> tuple[int, int, int] | None:
    if shutil.which("docker") is None:
        return None
    result = subprocess.run(
        ["docker", "compose", "version", "--short"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    match = re.match(r"v?(\d+)\.(\d+)\.(\d+)", result.stdout.strip())
    if result.returncode != 0 or not match:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


COMPOSE = _compose_version()
requires_compose = pytest.mark.skipif(COMPOSE is None, reason="no docker compose CLI on this host")
FILES = ["-f", "docker-compose.yml", "-f", "docker-compose.local.yml"]


def _required_variables() -> dict[str, str]:
    names: set[str] = set()
    for name in ("docker-compose.yml", "docker-compose.local.yml"):
        text = (ROOT / name).read_text(encoding="utf-8")
        names.update(re.findall(r"\$\{([A-Z0-9_]+):\?", text))
    assert names, "found no required variables: the pattern is wrong, not the compose files"
    return {name: "rendering-only" for name in names}


def _render(args: list[str], stdin: str | None = None) -> dict:
    env = {**os.environ, **_required_variables(), "DATANIKA_IMAGE_TAG": f"worktree-{AGENT}"}
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        input=stdin,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@requires_compose
class TestRealCompose:
    def test_compose_is_new_enough_for_the_overlay(self) -> None:
        assert COMPOSE >= (2, 24, 4), f"docker compose {COMPOSE} predates `!override`"

    def test_the_real_files_plus_the_overlay_render_an_isolated_stack(self) -> None:
        base = _render([*FILES, "--profile", "*", "config", "--no-interpolate", "--format", "json"])
        text = ws.overlay(base, AGENT, OFFSET)
        project = ["-p", f"wt-{AGENT}", *FILES, "-f", "-"]
        full = _render([*project, "--profile", "*", "config", "--format", "json"], stdin=text)
        report, problems = ws.check(full, AGENT, OFFSET)
        assert problems == [], "\n".join(report + problems)
        assert len(full["services"]) == len(base["services"]) >= 10

    def test_control_the_plain_render_is_refused(self) -> None:
        full = _render(
            ["-p", f"wt-{AGENT}", *FILES, "--profile", "*", "config", "--format", "json"]
        )
        problems = ws.check(full, AGENT, OFFSET)[1]
        assert any("container name" in p for p in problems), problems
        assert any("outside" in p for p in problems), problems
        assert any("REFLEX_API_URL" in p for p in problems), problems

    @pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash to drive the script")
    def test_the_script_renders_a_fake_worktree_as_isolated(self, tmp_path: Path) -> None:
        core = tmp_path / "datanika-core-fake"
        (core / "scripts").mkdir(parents=True)
        for name in ("docker-compose.yml", "docker-compose.local.yml"):
            shutil.copy2(ROOT / name, core / name)
        shutil.copy2(SHELL, core / "scripts" / SHELL.name)
        shutil.copy2(MODULE, core / "scripts" / MODULE.name)
        lines = [f"{key}={value}\n" for key, value in _required_variables().items()]
        (core / "test.env").write_text("".join(lines), encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if k != "MSYS_NO_PATHCONV"}
        env["PYTHON"] = sys.executable.replace("\\", "/")
        command = ["bash", f"scripts/{SHELL.name}", "--port-offset", "30000"]
        result = subprocess.run(
            [*command, "--env-file", "test.env", "config"],
            cwd=core,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=180,
            check=False,
            env=env,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "isolated:" in result.stdout, result.stdout
        assert "container_name: wt-fake-app" in result.stdout, result.stdout
