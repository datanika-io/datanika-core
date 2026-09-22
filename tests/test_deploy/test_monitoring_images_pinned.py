"""Every third-party monitoring image is pinned to a VERSION and a DIGEST (core#1478).

They all floated on ``:latest`` until 2026-09-22, and the deploy runs
``up -d --force-recreate prometheus grafana blackbox`` on every promotion — so any promotion could
move a version underneath configuration validated against a specific one, with no diff to show
it. core#1476 is the concrete instance: its two Grafana ``[database]`` options were verified
against 13.1.0's own ``grafana.ini``, and whether ``wal = true`` does anything turned out to be a
property of whichever build ``latest`` resolved to. By 2026-09-22 Docker Hub's ``latest`` had
already moved off the digest production runs.

Stated as the PRESENCE of the right thing — ``name:version@sha256:<64 hex>`` — never as a ban on
the word ``latest`` (WORKFLOW_RULES §4): a ban is satisfied by ``grafana/grafana:13`` too, which
floats just as surely, and by a digest-only reference nobody can read.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
STAGING = ROOT / "deploy" / "staging" / "docker-compose.yml"

#: A readable version (which may carry a leading ``v``) AND an immutable digest.
PINNED = re.compile(r"^[a-z0-9][a-z0-9./_-]*:v?\d[\w.-]*@sha256:[0-9a-f]{64}$")
OURS = "ghcr.io/datanika-io/"


def _services(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["services"]


def _monitoring() -> dict[str, str]:
    return {
        name: svc["image"]
        for name, svc in _services(COMPOSE).items()
        if "monitoring" in (svc.get("profiles") or [])
    }


def test_the_monitoring_profile_is_what_this_file_thinks_it_is() -> None:
    """Anti-vacuity: an empty selection would make every assertion below pass on nothing."""
    assert set(_monitoring()) == {
        "prometheus",
        "grafana",
        "blackbox",
        "node-exporter",
        "cadvisor",
        "postgres-exporter",
        "celery-exporter",
    }


@pytest.mark.parametrize("service", sorted(_monitoring()))
def test_every_monitoring_image_is_pinned_to_a_version_and_a_digest(service: str) -> None:
    image = _monitoring()[service]
    assert not image.startswith(OURS), f"{service} is our own image; it does not belong here"
    assert PINNED.match(image), (
        f"{service}: {image!r} is not `name:version@sha256:<digest>`. A floating tag lets the next "
        "`--force-recreate` move the version under configuration validated against another one."
    )


def test_the_staging_exporter_carries_the_same_pin_as_production() -> None:
    """Staging exists to exercise the exporter on a real worker before production does."""
    staging = _services(STAGING)["celery-exporter"]["image"]
    assert staging == _monitoring()["celery-exporter"], (staging, _monitoring()["celery-exporter"])


def test_promtool_runs_the_prometheus_production_runs() -> None:
    """`test_celery_failure_rule.py` runs promtool at a version it names; it must be production's.

    Until this pin existed that constant could only be a measurement ("compose floats, so read
    buildinfo"). Now the compose file is the authority and the two can be compared.
    """
    from tests.test_deploy.test_celery_failure_rule import PROMETHEUS_VERSION

    tag = _monitoring()["prometheus"].split("@", 1)[0].split(":", 1)[1]
    assert tag == f"v{PROMETHEUS_VERSION}", (tag, PROMETHEUS_VERSION)


def test_the_pin_pattern_discriminates() -> None:
    """The pattern can fail, in each direction that matters."""
    digest = "@sha256:" + "a" * 64
    assert PINNED.match("grafana/grafana:13.1.0" + digest)
    assert PINNED.match("gcr.io/cadvisor/cadvisor:v0.55.1" + digest)
    assert not PINNED.match("grafana/grafana:latest")  # floats
    assert not PINNED.match("grafana/grafana:latest" + digest)  # the tag lies to the reader
    assert not PINNED.match("grafana/grafana:13.1.0")  # readable, but mutable
    assert not PINNED.match("grafana/grafana" + digest)  # immutable, but unreadable
    assert not PINNED.match("grafana/grafana:13.1.0@sha256:" + "a" * 63)  # truncated digest
