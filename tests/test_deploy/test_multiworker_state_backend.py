"""A manifest may not run >1 Reflex backend process without shared state (core#646).

Measured on **production** 2026-08-30: 48% of reconnects were served a state that had
lost an earlier write (11 stale of 23 comparisons), against 0 of 23 on a single socket
in the same minute. That is a logout, because ``AuthState.access_token`` is a plain
server-side state var and ``check_auth`` redirects to ``/login`` when it is empty. It is
the cause of #472 and of #529's golden-path 3-of-6.

The trap has two halves and each is asserted separately below, because either one alone
looks harmless:

1. **Reflex does not read ``REDIS_URL``.** Its config prefix is ``REFLEX_``, so the name
   it looks for is ``REFLEX_REDIS_URL``. Every manifest sets ``REDIS_URL`` (which core's
   own Pydantic settings use for Celery) and Reflex ignores it, falling back to
   ``StateManagerDisk`` + ``LocalTokenManager`` — both **per process**.
2. **Reflex's own guard was overridden.** ``processes.get_num_workers()`` returns 1 when
   Redis is absent, precisely so a single process owns all state. Setting
   ``GRANIAN_WORKERS`` in the environment bypasses that fallback
   (``exec.py``: ``if "GRANIAN_WORKERS" not in os.environ``).

Everything here is **derived from the installed Reflex**, never restated. A restated
constant is how the defect survived: ``REDIS_URL`` was set, looked right, and was read
by nobody. If Reflex changes its prefix or drops the single-worker fallback, the
premise tests below fail and say so, rather than the manifest test silently checking a
name that no longer matters.
"""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PROD_COMPOSE = ROOT / "docker-compose.yml"
STAGING_COMPOSE = ROOT / "deploy" / "staging" / "docker-compose.yml"
CHART = ROOT / "deploy" / "helm" / "datanika"
CHART_VALUES = CHART / "values.yaml"
APP_TEMPLATE = CHART / "templates" / "app-deployment.yaml"

# Services that serve HTTP/websocket traffic from the Reflex app. `celery` is
# deliberately excluded: it runs no Reflex event loop and holds no client state.
APP_SERVICES = ("app", "app_b")


# --------------------------------------------------------------------------- #
# Premise 1 — the env var name Reflex actually reads, derived not restated
# --------------------------------------------------------------------------- #


def reflex_redis_env_var() -> str:
    """The env var Reflex reads for its state backend, asked of Reflex itself."""
    from reflex.config import Config

    prefixes = list(Config._prefixes)
    assert prefixes, "reflex.Config._prefixes is empty; cannot derive the env var name"
    return prefixes[0] + "REDIS_URL"


def test_reflex_reads_its_own_prefixed_redis_url_and_not_the_bare_one():
    """`REDIS_URL` alone leaves Reflex with no redis configured.

    This is the premise of the whole module, so it is proven against the installed
    package rather than asserted in prose. If it ever stops holding, this test fails
    and the manifest requirement below should be revisited.
    """
    from reflex.config import Config

    assert reflex_redis_env_var() == "REFLEX_REDIS_URL"
    assert "" not in Config._prefixes, (
        "reflex.Config now reads unprefixed env vars, so a bare REDIS_URL would be "
        "picked up and the premise of core#646 no longer holds"
    )


def test_reflex_state_manager_defaults_to_a_per_process_backend():
    """Without a redis url, Reflex selects a state manager that is not shared.

    `StateManagerDisk.get_state` returns `self.states[client_token]` when present,
    **without consulting disk**, so a write made on one process is invisible to any
    other process that has already cached that token. `StateManagerMemory` is worse
    still. Either way, more than one process means more than one session store.
    """
    import dataclasses

    from reflex import constants
    from reflex.config import Config

    fields = {f.name: f for f in dataclasses.fields(Config)}
    default_mode = fields["state_manager_mode"].default
    assert default_mode in (
        constants.StateManagerMode.DISK,
        constants.StateManagerMode.MEMORY,
    ), f"unexpected default state manager mode {default_mode!r}"


def test_reflex_own_guard_is_a_single_worker_fallback():
    """Reflex protects itself by running one worker when redis is absent.

    `GRANIAN_WORKERS` in the environment is what overrides that fallback, which is
    why the manifest check below exists at all. Read out of the source so the test
    fails loudly if the guard is removed upstream rather than quietly passing.
    """
    import inspect

    from reflex.utils import exec as reflex_exec
    from reflex.utils import processes

    src = inspect.getsource(processes.get_num_workers)
    assert "return 1" in src and "redis" in src.lower(), (
        "reflex.utils.processes.get_num_workers no longer falls back to a single "
        "worker without redis; re-derive core#646's requirement"
    )
    assert 'if "GRANIAN_WORKERS" not in os.environ' in inspect.getsource(reflex_exec), (
        "reflex no longer defers to a preset GRANIAN_WORKERS; re-derive core#646"
    )


# --------------------------------------------------------------------------- #
# The regression check
# --------------------------------------------------------------------------- #


def _service_env(service: dict) -> dict[str, str]:
    env = service.get("environment") or {}
    if isinstance(env, list):  # `- KEY=value` form
        out = {}
        for item in env:
            key, _, value = str(item).partition("=")
            out[key] = value
        return out
    return {str(k): "" if v is None else str(v) for k, v in env.items()}


def _backend_process_count(service: dict) -> int:
    """How many Reflex backend processes this service starts.

    Absent `GRANIAN_WORKERS`, Reflex decides for itself and its own guard applies,
    so an unset value is reported as 1 — the safe case this test must not flag.
    """
    raw = _service_env(service).get("GRANIAN_WORKERS", "")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 1


def _has_shared_state(env: dict[str, str]) -> bool:
    return bool(env.get(reflex_redis_env_var(), "").strip())


def _compose_app_services(path: Path):
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    for name in APP_SERVICES:
        service = (doc.get("services") or {}).get(name)
        if service is not None:
            yield name, service


@pytest.mark.parametrize(
    "manifest", [PROD_COMPOSE, STAGING_COMPOSE], ids=["prod-compose", "staging-compose"]
)
def test_multiworker_compose_services_configure_reflex_shared_state(manifest: Path):
    var = reflex_redis_env_var()
    offenders = []
    for name, service in _compose_app_services(manifest):
        workers = _backend_process_count(service)
        if workers > 1 and not _has_shared_state(_service_env(service)):
            offenders.append(f"{name} (GRANIAN_WORKERS={workers}, no {var})")
    try:
        shown = manifest.relative_to(ROOT).as_posix()
    except ValueError:  # a synthetic manifest from the self-test below
        shown = manifest.as_posix()
    assert not offenders, (
        f"{shown} runs multiple Reflex backend "
        f"processes without shared state: {offenders}. Each process keeps its own "
        f"session store, so a reconnect that lands elsewhere is served a stale state "
        f"- measured at 48% on prod (core#646). Set {var} on those services, or drop "
        f"GRANIAN_WORKERS so Reflex's own single-worker fallback applies."
    )


def test_helm_app_deployment_configures_shared_state_when_scaled_out():
    """Same requirement one layer up: replicas are processes too.

    The chart defaults to `replicaCount: 1`, so this passes today. It is a forward
    guard for the self-hoster who scales the app Deployment — and the chart's secret
    only sets `REDIS_URL`, which Reflex does not read.
    """
    values = yaml.safe_load(CHART_VALUES.read_text(encoding="utf-8"))
    replicas = int(((values.get("app") or {}).get("replicaCount")) or 1)
    if replicas <= 1:
        pytest.skip("chart ships replicaCount=1; guard applies once it is raised")
    rendered = APP_TEMPLATE.read_text(encoding="utf-8")
    assert reflex_redis_env_var() in rendered, (
        f"app.replicaCount={replicas} but the app Deployment never sets "
        f"{reflex_redis_env_var()} (core#646)"
    )


# --------------------------------------------------------------------------- #
# The checker's own logic, exercised on synthetic inputs
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("env", "workers"),
    [
        ({}, 1),
        ({"GRANIAN_WORKERS": "1"}, 1),
        ({"GRANIAN_WORKERS": "4"}, 4),
        ({"GRANIAN_WORKERS": 2}, 2),
        ({"GRANIAN_WORKERS": "not-a-number"}, 1),
    ],
)
def test_backend_process_count_reads_the_forms_compose_allows(env, workers):
    assert _backend_process_count({"environment": env}) == workers
    assert _backend_process_count({"environment": [f"{k}={v}" for k, v in env.items()]}) == workers


def test_shared_state_detection_rejects_the_name_that_fooled_us():
    var = reflex_redis_env_var()
    assert not _has_shared_state({"REDIS_URL": "redis://redis:6379/0"})
    assert not _has_shared_state({var: "   "})
    assert not _has_shared_state({})
    assert _has_shared_state({var: "redis://:pw@redis:6379/0"})


def test_the_check_fires_on_a_manifest_shaped_like_the_defect(tmp_path: Path):
    """Force the assertion red on a synthetic manifest carrying exactly the bug.

    A check that has only ever been run against the tree it is meant to guard has
    not been shown to be able to fail on anything else.
    """
    bad = tmp_path / "docker-compose.yml"
    bad.write_text(
        yaml.safe_dump(
            {
                "services": {
                    "app": {
                        "environment": {
                            "GRANIAN_WORKERS": "4",
                            "REDIS_URL": "redis://redis:6379/0",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="without shared state"):
        test_multiworker_compose_services_configure_reflex_shared_state(bad)

    good = tmp_path / "ok-compose.yml"
    good.write_text(
        yaml.safe_dump(
            {
                "services": {
                    "app": {
                        "environment": {
                            "GRANIAN_WORKERS": "4",
                            reflex_redis_env_var(): "redis://:pw@redis:6379/0",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    test_multiworker_compose_services_configure_reflex_shared_state(good)
