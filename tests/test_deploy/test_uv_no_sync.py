"""Every container that boots through `uv run` must pin `UV_NO_SYNC` (core#785).

`uv run` **re-syncs the venv from `uv.lock`** before running its command. The
`Dockerfile` grafts two packages in *after* `uv sync --frozen`, with
`uv pip install /cloud` and `uv pip install ./datanika-mcp`, and neither consults
the lock — so the shipped venv contains `datanika_cloud` and `datanika_mcp` while
the lockfile does not list them, and making the venv match the lock is exactly
what a sync is for.

Measured on the serving worker 2026-08-31: `ls -ld /app/.venv` reads the image
**build** time, not the container start time, so today's start-time sync is a
no-op. That is uv's current behaviour, not a guarantee we hold. If it ever
changes, the cloud plugin and the `/mcp` surface disappear at container start
with every signal green — the box's build succeeded, the build-time import
assertion (#612) validated the *image* and ran earlier, and cloud degrades
silently because `DATANIKA_EDITION` gates it rather than failing loudly.

🔑 **Services are found by what their command RUNS, never by name.** A guard with
a hardcoded `["app", "app_b", "celery", "beat"]` would pass forever the day
someone adds a fifth `uv run` service, which is the failure mode it exists to
prevent. The same reasoning as `test_beat_singleton.py`.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]

COMPOSE_MANIFESTS = {
    "docker-compose.yml": ROOT / "docker-compose.yml",
    "deploy/staging/docker-compose.yml": ROOT / "deploy" / "staging" / "docker-compose.yml",
}

DOCKERFILE = ROOT / "Dockerfile"


def _command_text(service: dict) -> str:
    """A compose `command:` as one string, whatever form it was written in."""
    command = service.get("command", "")
    if isinstance(command, list):
        return " ".join(str(part) for part in command)
    return str(command)


def _uv_run_services(manifest: dict) -> dict[str, dict]:
    """Services whose command invokes `uv run` — by behaviour, not by name."""
    return {
        name: service
        for name, service in (manifest.get("services") or {}).items()
        if re.search(r"\buv\s+run\b", _command_text(service))
    }


@pytest.fixture(params=sorted(COMPOSE_MANIFESTS), ids=sorted(COMPOSE_MANIFESTS))
def compose(request):
    path = COMPOSE_MANIFESTS[request.param]
    return request.param, yaml.safe_load(path.read_text(encoding="utf-8"))


def test_the_manifest_actually_uses_uv_run(compose):
    """Anti-vacuity. If nobody uses `uv run`, every assertion below is empty.

    This must be the first thing to go red if the start commands are rewritten to
    call `/app/.venv/bin/...` directly — which is the *better* fix (#785 lists it
    as the alternative). Its failure is then a prompt to delete this file, not a
    bug. Silence would instead let the file sit here asserting nothing.
    """
    label, manifest = compose
    services = _uv_run_services(manifest)
    assert services, (
        f"{label}: no service runs `uv run`. If the start commands now call the "
        f"venv binaries directly, this whole guard is obsolete — delete it "
        f"deliberately rather than leaving it green and vacuous."
    )


def test_every_uv_run_service_pins_uv_no_sync(compose):
    label, manifest = compose
    missing = []
    for name, service in _uv_run_services(manifest).items():
        env = service.get("environment") or {}
        if isinstance(env, list):  # `- KEY=value` form
            env = dict(item.split("=", 1) for item in env if "=" in item)
        if str(env.get("UV_NO_SYNC", "")).strip() not in {"1", "true", "True"}:
            missing.append(name)
    assert not missing, (
        f"{label}: {missing} boot through `uv run` without `UV_NO_SYNC=1`.\n\n"
        "`uv run` re-syncs the venv from uv.lock at container start. "
        "`datanika_cloud` and `datanika_mcp` are installed by the Dockerfile "
        "AFTER `uv sync --frozen` and are not in the lock, so a sync is free to "
        "remove them — in production, at boot, after every build-time check has "
        "already passed. See core#785."
    )


def test_the_graft_installs_that_make_this_matter_are_still_there():
    """The premise, asserted against the artifact rather than restated.

    If the Dockerfile ever puts cloud and mcp *into* the lock, the hazard is gone
    and this guard should be reconsidered on purpose. Until then it is real.
    """
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    body = "\n".join(line for line in dockerfile.splitlines() if not line.lstrip().startswith("#"))
    assert re.search(r"uv\s+sync\s+--frozen", body), "expected a locked sync in the image build"
    grafts = re.findall(r"uv pip install\s+(\S+)", body)
    assert grafts, (
        "no `uv pip install` graft found in the Dockerfile. If cloud and mcp are "
        "now installed from the lockfile, core#785's hazard no longer exists and "
        "UV_NO_SYNC becomes belt-and-braces rather than load-bearing — say so "
        "explicitly instead of letting this test quietly stop meaning anything."
    )


def test_no_uv_run_service_relies_on_env_file_for_this(compose):
    """It must be in `environment:`, not left to `.env.docker` on the box.

    🚨 The whole class of bug behind core#117: a setting that lives only in the
    box's `.env.docker` is preserved by the deploy rather than shipped by it, so
    no promotion ever disturbs it and no diff ever shows it. A guarantee we intend
    to hold has to be in the file CD actually writes.
    """
    label, manifest = compose
    for name, service in _uv_run_services(manifest).items():
        env = service.get("environment") or {}
        if isinstance(env, list):
            env = dict(item.split("=", 1) for item in env if "=" in item)
        assert "UV_NO_SYNC" in env, (
            f"{label}:{name} — UV_NO_SYNC must be declared in `environment:`, "
            f"where the deploy ships it, not left to the box's .env.docker."
        )
