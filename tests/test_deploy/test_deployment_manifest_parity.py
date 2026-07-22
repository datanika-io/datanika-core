"""Every shipped deployment manifest must share what the tiers share (core#529).

The web tier stores an uploaded file and the Celery worker reads it back. If the
two do not share that directory, the upload succeeds, the run is queued, and the
worker dies on `tarfile.open` before any data moves — a run that ends `failed` in
under 100ms with 0 rows.

`docker-compose.yml` gets this right, and #471's comment on it says why:

    # Must match app/app_b exactly — the LOAD runs here, so if the worker
    # resolves a different directory the failure surfaces at run time, not at
    # upload time (#471).

**That knowledge lived in a comment in one manifest, and the other two shipped
without it.** Staging (a box-only compose file) and the Helm chart both mount
`dbt_projects` alone, so core#529 reproduced on staging for three CI rounds and
the same defect is latent for every Helm self-hoster on the CSV → DuckDB
onboarding path the landing page calls "the first pipeline you ever run".

So the requirement is **derived** here rather than restated. A restated list
drifts — that is how this happened. Compose is the reference deployment: any
directory it mounts on *both* `app` and `celery` is by construction a cross-tier
directory, and any env var it sets identically on both is by construction a
cross-tier agreement. Both must hold in the chart too, and a future one added to
compose fails this test until the chart catches up.
"""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
CHART = ROOT / "deploy" / "helm" / "datanika"
APP_TEMPLATE = CHART / "templates" / "app-deployment.yaml"
CELERY_TEMPLATE = CHART / "templates" / "celery-deployment.yaml"

# The tiers that run application code. `app_b` is the blue/green sibling of
# `app` and is covered by the same reasoning, but it is a compose-only concept.
WEB, WORKER = "app", "celery"


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def _named_volume_mounts(service: dict) -> dict[str, str]:
    """Container paths backed by a **named volume**, keyed by path.

    Bind mounts are deliberately excluded: those carry host config (Prometheus
    rules, certificates) and say nothing about what the tiers exchange.
    """
    mounts = {}
    for entry in service.get("volumes") or []:
        if not isinstance(entry, str) or ":" not in entry:
            continue
        source, _, dest = entry.partition(":")
        if source.startswith((".", "/")):
            continue
        mounts[dest.split(":")[0]] = source
    return mounts


def _environment(service: dict) -> dict[str, str]:
    """Compose accepts `environment` as a mapping or a `KEY=value` list."""
    env = service.get("environment") or {}
    if isinstance(env, list):
        pairs = (entry.split("=", 1) for entry in env if "=" in entry)
        return {k: v for k, v in pairs}
    return {str(k): str(v) for k, v in env.items()}


class TestSharedDirectories:
    def test_compose_declares_at_least_the_two_known_shared_directories(self, compose):
        """Anti-vacuity. A parse that silently yields nothing would make every
        assertion below pass having checked no manifest at all."""
        shared = self._shared_paths(compose)
        assert len(shared) >= 2, (
            f"expected compose to share dbt_projects and uploaded_files between "
            f"{WEB} and {WORKER}; found {shared}. The scan is broken, not the chart."
        )

    @staticmethod
    def _shared_paths(compose: dict) -> set[str]:
        services = compose["services"]
        web = _named_volume_mounts(services[WEB])
        worker = _named_volume_mounts(services[WORKER])
        return set(web) & set(worker)

    def test_helm_shares_every_directory_compose_shares(self, compose):
        """The bug core#529 is: `/app/uploaded_files` was shared in compose and
        in neither other manifest."""
        app_text = APP_TEMPLATE.read_text(encoding="utf-8")
        celery_text = CELERY_TEMPLATE.read_text(encoding="utf-8")

        missing = []
        for path in sorted(self._shared_paths(compose)):
            for tier, text in ((WEB, app_text), (WORKER, celery_text)):
                if f"mountPath: {path}" not in text:
                    missing.append(f"{tier} deployment does not mount {path}")

        assert not missing, (
            "the Helm chart does not share a directory the tiers exchange data through, "
            "so the web tier will write a file the worker cannot read:\n  " + "\n  ".join(missing)
        )


class TestSharedEnvironment:
    """A shared directory is only half of it — both tiers must also *resolve*
    the same path.

    `config.py` defaults `file_uploads_dir` to `./uploaded_files`, which is
    relative to the working directory. Two tiers with the same mount and
    different working directories still diverge, which is exactly why compose
    pins the absolute value on both rather than relying on the default.
    """

    @staticmethod
    def _shared_env(compose: dict) -> dict[str, str]:
        services = compose["services"]
        web = _environment(services[WEB])
        worker = _environment(services[WORKER])
        return {k: web[k] for k in set(web) & set(worker) if web[k] == worker[k]}

    def test_compose_pins_at_least_one_cross_tier_variable(self, compose):
        """Anti-vacuity, same reasoning as above."""
        assert self._shared_env(compose), (
            "compose sets no environment variable identically on both tiers — "
            "either the scan is broken or #471's pinning was removed"
        )

    def test_helm_pins_the_same_cross_tier_variables(self, compose):
        values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
        chart_env = {str(k): str(v) for k, v in (values.get("env") or {}).items()}

        shared = self._shared_env(compose)
        wrong = {
            key: (expected, chart_env.get(key))
            for key, expected in shared.items()
            if chart_env.get(key) != expected
        }
        assert not wrong, (
            "the Helm chart does not pin a value compose pins on both tiers, so the "
            f"two tiers can resolve different directories: {wrong}"
        )

    def test_the_pinned_variables_actually_reach_the_containers(self, compose):
        """`values.yaml` is not the consumer — `secret.yaml` is.

        The Secret enumerates its keys explicitly and only then falls through to
        ``env.extra``, so a key added to ``values.env`` and nowhere else is
        inert: it never lands in the Secret, and the deployments consume the
        Secret via ``envFrom``. The first version of the test above asserted
        ``values.yaml`` alone and passed over a chart that still shipped the
        bug — the same mistake as reading a config schema instead of what the
        form writes (core#565). Assert the plumbing, not the declaration.
        """
        secret = (CHART / "templates" / "secret.yaml").read_text(encoding="utf-8")
        values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
        # Two ways in: named explicitly in the Secret, or supplied through the
        # `env.extra` range. Being in `values.env` is neither.
        extra = (values.get("env") or {}).get("extra") or {}

        unplumbed = [
            key
            for key in sorted(self._shared_env(compose))
            if key not in secret and key not in extra
        ]
        assert not unplumbed, (
            "these variables are declared in values.yaml but never rendered into the env "
            f"Secret, so no container ever sees them: {unplumbed}"
        )
