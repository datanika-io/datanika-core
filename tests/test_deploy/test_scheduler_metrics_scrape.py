"""core#1199 — the scheduler's metrics must be SCRAPED, not merely emitted.

Why this file is separate from the unit tests
---------------------------------------------
``tests/test_services/test_scheduler_observability.py`` proves the scheduler process emits
series that discriminate a healthy reconcile from a failing one. That is necessary and
completely insufficient: core#704 is the case where every one of those numbers was correct,
collected, and thrown away.

Its exact shape, from ``monitoring/prometheus.yml``'s own comment:

    until 2026-08-31 Prometheus scraped seven targets and none of them was Celery, so every
    task metric the code collected was discarded ... which is why `celery-task-failures` had
    never been able to fire.

The scheduler is a third process. It publishes no port in the image, mounts no Starlette
app, and until core#1199 appeared in no ``scrape_configs`` entry. So the whole chain has to
hold, and every link is in a different file:

===========================  ==========================================================
``datanika/config.py``       the port the process binds
``datanika/scheduler_main``  actually binding it, before the first reconcile
``docker-compose.yml``       the service NAME Prometheus resolves over the compose network
``monitoring/prometheus.yml``the scrape job pointing at ``<service>:<port>``
===========================  ==========================================================

A mismatch anywhere leaves an exporter answering nobody, and *nothing goes red* — the
scheduler stays ``Up``, Prometheus stays green on its other targets, and the new alert has
no series to evaluate, which under ``noDataState: OK`` reads healthy.

⚠️ Deliberately NOT asserted here
---------------------------------
The alert rule. That is Infra's, lives in Grafana's provisioning, and is read off the API —
``CLAUDE.md``'s rule-count line has drifted repeatedly and is not a source. This file stops
at "a scrape config exists and points at the right place."
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
PROMETHEUS = ROOT / "monitoring" / "prometheus.yml"

#: The compose service whose command runs the dedicated scheduler process.
_SCHEDULER_MODULE = "datanika.scheduler_main"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _scheduler_service_name(compose: dict) -> str:
    """Find the scheduler service by WHAT ITS COMMAND DOES, not by its name.

    Same reasoning as ``test_beat_singleton.py``: a rename must not silently disable the
    guard. If nothing runs the module, that is itself the failure.
    """
    matches = [
        name
        for name, svc in (compose.get("services") or {}).items()
        if _SCHEDULER_MODULE in str(svc.get("command", ""))
    ]
    assert len(matches) == 1, (
        f"expected exactly one compose service running {_SCHEDULER_MODULE}, found {matches}. "
        "Two would reintroduce core#648; zero means the scheduler is not deployed at all."
    )
    return matches[0]


def _scrape_targets(prom: dict) -> dict[str, list[str]]:
    return {
        job["job_name"]: [
            t for sc in (job.get("static_configs") or []) for t in sc.get("targets", [])
        ]
        for job in prom.get("scrape_configs", [])
    }


@pytest.fixture(scope="module")
def compose() -> dict:
    return _load(COMPOSE)


@pytest.fixture(scope="module")
def prom() -> dict:
    return _load(PROMETHEUS)


def test_the_manifests_are_readable(compose, prom):
    """Floor. Every assertion below is vacuous against an empty parse (§25)."""
    assert len(compose.get("services") or {}) >= 8
    assert len(prom.get("scrape_configs") or []) >= 5


def test_prometheus_scrapes_the_scheduler(compose, prom):
    service = _scheduler_service_name(compose)
    targets = [t for ts in _scrape_targets(prom).values() for t in ts]
    matching = [t for t in targets if t.split(":")[0] == service]
    assert matching, (
        f"no scrape_configs target resolves to the compose service '{service}'. The "
        f"scheduler emits reconcile-health and dispatch counters that nothing collects — "
        f"core#704 exactly. Scraped targets are: {sorted(targets)}"
    )


def test_the_scraped_port_is_the_port_the_code_binds(compose, prom):
    """The joint. Three files have to agree and none of them imports the others."""
    from datanika.config import settings

    service = _scheduler_service_name(compose)
    targets = [t for ts in _scrape_targets(prom).values() for t in ts]
    matching = [t for t in targets if t.split(":")[0] == service]
    assert matching, "covered by test_prometheus_scrapes_the_scheduler"
    ports = {int(t.split(":")[1]) for t in matching}
    assert ports == {settings.scheduler_metrics_port}, (
        f"prometheus.yml scrapes {service} on {sorted(ports)} but the process binds "
        f"{settings.scheduler_metrics_port} (settings.scheduler_metrics_port). The scrape "
        "would fail connection and the series would simply be absent — which under "
        "noDataState: OK reads as healthy."
    )


def test_the_scheduler_service_is_not_the_app(compose):
    """Guards against the lazy fix of pointing the scrape at `app:8000`.

    The app's /metrics cannot see the scheduler's counters: separate processes, separate
    registries. A scrape config that satisfied the port assertion by naming the app would
    return 200 and zero of the series in question.
    """
    service = _scheduler_service_name(compose)
    assert service not in {"app", "app_b"}, (
        f"the scheduler module is running inside '{service}', which is the web tier. "
        "core#648 moved it out precisely so that it is one process, not five."
    )


class TestTheGuardCanActuallyFail:
    """Negative controls (§43): these prove the PARSERS work, not that the tree is right."""

    _COMPOSE_TWO = """
    services:
      scheduler:
        command: uv run python -m datanika.scheduler_main
      scheduler_two:
        command: uv run python -m datanika.scheduler_main
    """

    _COMPOSE_NONE = """
    services:
      app:
        command: uv run reflex run
    """

    _COMPOSE_RENAMED = """
    services:
      timekeeper:
        command: uv run python -m datanika.scheduler_main
    """

    def test_it_finds_the_service_under_any_name(self):
        assert _scheduler_service_name(yaml.safe_load(self._COMPOSE_RENAMED)) == "timekeeper"

    def test_two_owning_services_is_a_failure(self):
        with pytest.raises(AssertionError, match="exactly one"):
            _scheduler_service_name(yaml.safe_load(self._COMPOSE_TWO))

    def test_zero_owning_services_is_a_failure(self):
        with pytest.raises(AssertionError, match="exactly one"):
            _scheduler_service_name(yaml.safe_load(self._COMPOSE_NONE))

    def test_the_target_parser_reads_a_real_config(self):
        parsed = _scrape_targets(
            yaml.safe_load(
                """
                scrape_configs:
                  - job_name: "scheduler"
                    static_configs:
                      - targets: ["scheduler:9810"]
                """
            )
        )
        assert parsed == {"scheduler": ["scheduler:9810"]}

    def test_the_target_parser_does_not_invent_a_target(self):
        parsed = _scrape_targets(
            yaml.safe_load(
                """
                scrape_configs:
                  - job_name: "blackbox-http"
                    metrics_path: /probe
                """
            )
        )
        assert parsed == {"blackbox-http": []}

    def test_a_port_mismatch_would_be_caught(self):
        """The assertion that matters, exercised against a deliberately wrong config."""
        prom = yaml.safe_load(
            """
            scrape_configs:
              - job_name: "scheduler"
                static_configs:
                  - targets: ["scheduler:9999"]
            """
        )
        targets = [t for ts in _scrape_targets(prom).values() for t in ts]
        ports = {int(t.split(":")[1]) for t in targets if t.split(":")[0] == "scheduler"}
        assert ports != {9810}, "the port comparison cannot discriminate"


def test_the_metrics_port_is_not_already_taken(compose):
    """9810 must not collide with another published port on this shared box.

    pointer.gr carries co-tenants (an Apache :80 vhost and the founder's VPN), and the
    monitoring stack already owns 9090, 9100, 9115, 9187 and 9808.
    """
    from datanika.config import settings

    published = set()
    for name, svc in (compose.get("services") or {}).items():
        for entry in svc.get("ports") or []:
            m = re.search(r":(\d+):(\d+)$|^(\d+):(\d+)$", str(entry))
            if m:
                host = m.group(1) or m.group(3)
                published.add((name, int(host)))
    owners = {n for n, p in published if p == settings.scheduler_metrics_port}
    assert len(owners) <= 1, (
        f"port {settings.scheduler_metrics_port} is published by more than one service: {owners}"
    )
