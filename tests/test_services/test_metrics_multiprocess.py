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
`docs/slo_instruments.yml` has **six** ``source: app`` entries. Five carried
``blocked_by``; the sixth (`error-rate-slos-webhook-handler-paddle-http-5xx`) did not, and was
reading NO_VERDICT only because its sufficiency query counts **successful** Paddle webhooks and
that count is 0 at 0 paying users — shielded by an accident of traffic, not by a guard. All six
`blocked_by` keys, `BLOCKED_BASELINE`, and
`test_every_app_sourced_slo_is_blocked_while_core_895_is_open` are removed in this same change,
because a fix that ships beside a test asserting it has not shipped leaves five unguarded and
the sixth blocked forever — and a stuck NO_VERDICT reads as *"no instrument"*, which is the
failure core#721 existed to end.
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
