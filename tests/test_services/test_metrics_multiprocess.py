"""App HTTP metrics must aggregate across Granian workers (core#895).

What QA measured on production, and the control that makes it evidence
----------------------------------------------------------------------
::

    app counter, six reads          1, 2, 7, 1, 1, 2
    single-process exporter, same   1, 1, 1, 1, 1, 1

Six granian processes, each with its own `prometheus_client` registry, and the scrape answered
by whichever one accepted the connection. So Prometheus records **one arbitrary worker's share**
of the traffic — and the number moves when nothing about the system has.

The steady `1, 1, 1, 1, 1, 1` from a single-process exporter is what turns that into a finding
rather than a story about load: same probe, same instant, one process, no variance.

Why this test spawns real processes
-----------------------------------
A mocked registry cannot reproduce it. The defect **is** process boundaries: `prometheus_client`
picks its value class **at import time** from ``PROMETHEUS_MULTIPROC_DIR``, so a test that sets
the variable after importing the counters measures nothing about multiprocess mode. Each child
here is a separate interpreter with the variable already set, which is the only shape that
matches production.

⚠️ Six SLOs, not five
---------------------
`docs/slo_instruments.yml` has **six** ``source: app`` entries and every one of them carries
``blocked_by`` today, so this change removes **six** keys — the entire population, not a sample.
(A seventh match for `blocked_by` in that file is the field's own description in the header
comment. It stays: the mechanism is general and outlives this fix. Counting it would be the
third time in this work that a grep total included a comment.)

The count is spelled out because it was **five** for a while, and the reason matters. When
core#908 wrote the guard, the sixth entry
(`error-rate-slos-webhook-handler-paddle-http-5xx`) had been missed — the key was applied
entry-by-entry from memory rather than as a rule. It was not yet producing a wrong verdict, but
only because its sufficiency query counts **successful** Paddle webhooks and that count is 0 at
0 paying users: shielded by an accident of traffic, not by a guard. The first real webhook would
have flipped it to a PASS computed from an instrument we had measured to be broken. The guard is
what added the sixth key, and that is why all six exist to be removed here.

Removed in this same change: the six keys, and
`test_every_app_sourced_slo_is_blocked_while_core_895_is_open` — an open-issue guard that
outlives its issue inverts into a guard against the fix. **`BLOCKED_BASELINE` is kept**, as an
empty set: `blocked_by` is a general ratchet, and an empty baseline asserts there is no blocked
instrument today, which is a claim that can fail. A fix that shipped beside a test asserting it
had not shipped would leave five unguarded and the sixth blocked forever — and a stuck
NO_VERDICT reads as *"no instrument"*, which is the failure core#721 existed to end.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

#: Each child increments the app's real counter once, in its own interpreter.
CHILD = """
import sys
sys.path.insert(0, r"{root}")
from datanika.services.metrics import http_requests_total
http_requests_total.labels("GET", "/probe", "200").inc()
"""


def _run_children(tmp_path: pathlib.Path, n: int) -> None:
    env = dict(os.environ)
    env["PROMETHEUS_MULTIPROC_DIR"] = str(tmp_path)
    for _ in range(n):
        proc = subprocess.run(
            [sys.executable, "-c", CHILD.format(root=str(ROOT))],
            env=env,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, f"child failed: {proc.stderr[-600:]}"


def _counter_total(body: str) -> float:
    total = 0.0
    for line in body.splitlines():
        if line.startswith("http_requests_total{") and 'path="/probe"' in line:
            total += float(line.rsplit(" ", 1)[1])
    return total


@pytest.fixture
def multiproc_dir(tmp_path, monkeypatch):
    d = tmp_path / "promdir"
    d.mkdir()
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(d))
    return d


def test_the_scrape_sums_every_worker_not_one_of_them(multiproc_dir):
    """core#895 AC1. Four workers each counting one request must read **4**, not 1.

    This is the assertion the production measurement demands: the value must stop depending on
    which process answered the scrape.
    """
    from prometheus_client import generate_latest

    from datanika.services.metrics import build_scrape_registry

    _run_children(multiproc_dir, 4)

    body = generate_latest(build_scrape_registry()).decode()
    assert _counter_total(body) == 4.0, (
        "the scrape returned "
        f"{_counter_total(body)} for 4 workers that each counted one request. That is the "
        "core#895 defect: Prometheus records one arbitrary worker's share, and the number "
        "moves when nothing about the system has."
    )


def test_the_children_really_wrote_separate_process_files(multiproc_dir):
    """Floor. If the children shared one file — or wrote nothing — the assertion above could
    pass for a reason that has nothing to do with aggregation."""
    _run_children(multiproc_dir, 3)
    files = sorted(p.name for p in multiproc_dir.glob("*.db"))
    assert len(files) >= 3, (
        f"expected one db file per child process, found {files}. Same-file writes would make "
        "the sum correct by accident and tell us nothing about multiprocess mode."
    )


def test_without_the_variable_it_serves_the_process_registry(monkeypatch):
    """The control, and the pre-fix behaviour.

    Unset, `build_scrape_registry` must return the ordinary in-process registry — a developer
    running `reflex run` with one process has no multiproc directory and must still get metrics.
    Without this, "always build a MultiProcessCollector" would pass the test above and break
    every single-process deployment.
    """
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    from prometheus_client import REGISTRY

    from datanika.services.metrics import build_scrape_registry

    assert build_scrape_registry() is REGISTRY


def test_a_missing_directory_does_not_take_the_endpoint_down(monkeypatch, tmp_path):
    """`/metrics` 500ing takes every other metric down with it (the `celery_queue_length`
    lesson at metrics.py's own swallow). A misconfigured path must degrade, not break."""
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path / "does-not-exist"))
    from prometheus_client import generate_latest

    from datanika.services.metrics import build_scrape_registry

    generate_latest(build_scrape_registry())  # must not raise


# ---------------------------------------------------------------------------
# core#895 AC4: what PR #1281 removed together with the per-process counters.
#
# `build_scrape_registry()` returned a registry that only `MultiProcessCollector` populates. A
# collector registered through `plugin_registry.get_prometheus_registry()` computes at scrape time
# and writes no mmap file, so it is outside that mechanism by construction. From the
# 2026-09-11 20:45:48Z deploy on, production and staging served none of the cloud plugin's ledger
# series. QA's control was Prometheus history: the cloud series stop at that swap while the HTTP
# series continue.
#
# Asserted on the SERVED object (QA_RULES §30): its own interpreter with the variable set before
# any import, the plugin seam's real registration call, core's real `/metrics` route, and the
# outer `Mount("")` that Reflex puts around `app._api` when granian calls the factory.
# ---------------------------------------------------------------------------

#: Stands in for a plugin collector that computes at scrape time, as cloud's ledger collector does.
PROBE = "datanika_probe_scrape_time_collector"

SERVER = """
import sys
sys.path.insert(0, r"__ROOT__")

from prometheus_client.core import GaugeMetricFamily
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.plugin_registry import get_prometheus_registry
from datanika.services.metrics import PrometheusMiddleware, http_requests_total, metrics_routes


class ScrapeTimeProbe:
    def collect(self):
        yield GaugeMetricFamily("__PROBE__", "stand-in for a plugin collector", value=1.0)


get_prometheus_registry().register(ScrapeTimeProbe())

# This process counts one request of its own. With the variable set, that lands in this process's
# own .db file, so the aggregate already carries it; rendering the in-process registry as well
# would count it twice.
http_requests_total.labels("GET", "/probe", "200").inc()

api = Starlette(routes=list(metrics_routes))
api.add_middleware(PrometheusMiddleware)
served = Starlette()
served.mount("", api)

response = TestClient(served).get("/metrics")
sys.stdout.write("STATUS %d\\n" % response.status_code)
sys.stdout.write(response.text)
"""

#: prometheus_client's own per-process collectors. Served from whichever worker answered, their
#: counters go backwards between scrapes, which is the core#895 defect itself.
PER_PROCESS_FAMILIES = ("python_gc_objects_collected_total", "python_info")


def _serve_one_scrape(multiproc_dir: pathlib.Path | None) -> str:
    """Run the served-shaped app in its own interpreter and return one `/metrics` body."""
    env = dict(os.environ)
    env.pop("PROMETHEUS_MULTIPROC_DIR", None)
    if multiproc_dir is not None:
        env["PROMETHEUS_MULTIPROC_DIR"] = str(multiproc_dir)
    source = SERVER.replace("__ROOT__", str(ROOT)).replace("__PROBE__", PROBE)
    proc = subprocess.run(
        [sys.executable, "-c", source],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, f"the served-app process failed: {proc.stderr[-1500:]}"
    status, _, body = proc.stdout.partition("\n")
    assert status == "STATUS 200", f"/metrics answered {status!r}, so nothing below is a reading"
    return body


def _sample_lines(body: str, name: str) -> list[str]:
    """Lines carrying a VALUE for exactly this metric name. Never `# HELP` or `# TYPE`."""
    return [
        line
        for line in body.splitlines()
        if not line.startswith("#") and line.startswith((f"{name} ", f"{name}{{"))
    ]


@pytest.fixture(scope="module")
def aggregated_scrape(tmp_path_factory) -> str:
    """Three workers count one request each, then a fourth process serves `/metrics`."""
    directory = tmp_path_factory.mktemp("promdir")
    _run_children(directory, 3)
    return _serve_one_scrape(directory)


@pytest.fixture(scope="module")
def single_process_scrape() -> str:
    return _serve_one_scrape(None)


def test_a_collector_registered_through_the_plugin_seam_is_served_in_multiprocess_mode(
    aggregated_scrape,
):
    """AC4, the structural half: every collector registered through the plugin seam reaches the
    response. Asserted as a rule over a stand-in, not as the cloud series' names, because a name
    list can be satisfied by hardcoding the names."""
    assert _sample_lines(aggregated_scrape, PROBE) == [f"{PROBE} 1.0"], (
        "a collector registered through plugin_registry.get_prometheus_registry() is missing "
        "from /metrics while PROMETHEUS_MULTIPROC_DIR is set. That is how the cloud plugin's "
        "ledger series vanished from production on 2026-09-11 with every check green."
    )


def test_the_serving_process_counts_are_not_rendered_twice(aggregated_scrape):
    """Three workers plus the serving process each counted one request, so the scrape reads 4.

    Reading 5 means the serving process's in-process registry was rendered beside the aggregate:
    its own increment counted once from its file and again from memory."""
    assert _counter_total(aggregated_scrape) == 4.0, (
        f"the scrape read {_counter_total(aggregated_scrape)} for 4 increments across 4 processes"
    )


def test_per_process_default_collectors_stay_out_of_the_aggregated_scrape(aggregated_scrape):
    """PR #1281 removed these knowingly, with zero readers. Adding them back from whichever
    worker answered would reintroduce the arbitrary-worker reading for their series."""
    for name in PER_PROCESS_FAMILIES:
        assert _sample_lines(aggregated_scrape, name) == [], (
            f"{name} was served from one arbitrary worker in multiprocess mode"
        )


def test_control_one_process_serves_the_whole_default_registry(single_process_scrape):
    """The control. The families the previous test asserts absent must exist in this environment,
    or their absence there proves nothing. The probe must render here too, or the multiprocess
    test's red could be a harness that renders no collector at all."""
    assert _sample_lines(single_process_scrape, PROBE) == [f"{PROBE} 1.0"]
    for name in PER_PROCESS_FAMILIES:
        assert _sample_lines(single_process_scrape, name), (
            f"{name} is absent even from the single-process scrape, so asserting its absence "
            "from the aggregated scrape is vacuous in this environment"
        )
