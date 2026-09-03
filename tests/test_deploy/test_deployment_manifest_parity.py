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

import re
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


# ---------------------------------------------------------------------------
# Commands (core#957)
# ---------------------------------------------------------------------------

# Compose service -> the `values.yaml` key whose `command` the chart renders.
# Only the three tiers that exist in BOTH manifests. `celery-exporter` is
# deliberately absent: the chart ships no exporter at all, so there is nothing
# to hold it to.
COMMAND_TIERS = {"app": "app", "celery": "celery", "beat": "beat"}

# A flag-shaped token: `-E`, `-l`, `--env`, `--backend-host`. The lookbehind
# stops it matching the tail of `celery_app:celery_app` or a negative number.
_FLAG = re.compile(r"(?<![\w-])(--?[A-Za-z][\w-]*)")

# Flags compose passes that the chart deliberately does not. EMPTY today, and it
# should stay that way — a compose flag with no chart counterpart is a real
# divergence until somebody writes down why it is not. Anything added here needs
# the reason beside it, not just the token.
COMPOSE_ONLY_FLAGS: dict[str, set[str]] = {}


def _command_flags(command) -> set[str]:
    """Flags in a compose or chart `command`, whatever shape it is written in.

    Three shapes occur across these two files and all must reduce to the same
    set: a YAML list (`[uv, run, celery, -A, …]`), a folded scalar (`command: >`),
    and a `sh -c "…"` wrapper whose flags live *inside* a quoted string. That
    last one is why this joins and regexes rather than using `shlex`: `shlex`
    yields the whole `sh -c` payload as one opaque token, so `--env prod` inside
    it would be invisible and the app tier would be checked vacuously.
    """
    if command is None:
        return set()
    text = " ".join(str(c) for c in command) if isinstance(command, list) else str(command)
    return set(_FLAG.findall(text))


class TestSharedCommands:
    """`-E` was in compose and in neither the chart nor any test (core#957).

    Same failure shape as the mount and the env var above, one layer down: a
    behaviour the reference deployment depends on, recorded in a comment on one
    manifest, absent from the artifact we hand self-hosters. `helm-lint` cannot
    see it — the chart renders perfectly well without the flag.

    Derived from compose, never restated. A restated list is what drifted.
    """

    @staticmethod
    def _chart_command(values: dict, tier_key: str):
        return (values.get(tier_key) or {}).get("command")

    @pytest.fixture(scope="class")
    def values(self) -> dict:
        return yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))

    def test_every_compared_command_parses_to_real_flags(self, compose, values):
        """Anti-vacuity, and it is the whole reason this class is trustworthy.

        The assertion below is a subset check, so an extractor that returns the
        empty set passes it for every tier while reading nothing. Both sides of
        every comparison must be non-trivial before the comparison means
        anything. `>= 2` because the thinnest real command here (`beat`) carries
        `-A`, `-l` and `--schedule`.
        """
        thin = {}
        for service, tier_key in COMMAND_TIERS.items():
            compose_flags = _command_flags(compose["services"][service].get("command"))
            chart_flags = _command_flags(self._chart_command(values, tier_key))
            if len(compose_flags) < 2:
                thin[f"compose:{service}"] = sorted(compose_flags)
            if len(chart_flags) < 2:
                thin[f"chart:{tier_key}"] = sorted(chart_flags)
        assert not thin, (
            "a command parsed to fewer than two flags, so the parity assertion below "
            f"would pass having compared nothing: {thin}"
        )

    def test_helm_passes_every_flag_compose_passes(self, compose, values):
        missing = {}
        for service, tier_key in COMMAND_TIERS.items():
            compose_flags = _command_flags(compose["services"][service].get("command"))
            chart_flags = _command_flags(self._chart_command(values, tier_key))
            gap = compose_flags - chart_flags - COMPOSE_ONLY_FLAGS.get(service, set())
            if gap:
                missing[tier_key] = sorted(gap)
        assert not missing, (
            "the Helm chart's command omits a flag the reference deployment passes, so a "
            "chart deployment behaves differently from production in a way `helm-lint` "
            f"cannot see: {missing}\n"
            "Add it to values.yaml, or record it in COMPOSE_ONLY_FLAGS with the reason."
        )

    def test_the_commands_actually_reach_the_containers(self):
        """`values.yaml` is a declaration; the Deployment is the consumer.

        Same lesson as `test_the_pinned_variables_actually_reach_the_containers`
        above — a `command` key nothing renders is inert, and the test that reads
        only `values.yaml` would go green over it.
        """
        templates = {
            "app": APP_TEMPLATE,
            "celery": CELERY_TEMPLATE,
            "beat": CHART / "templates" / "beat-deployment.yaml",
        }
        # `\b` rather than a bare substring: `.Values.celery.command` is a prefix
        # of `.Values.celery.commandXX`, so `in` accepts a template that renders
        # a key which does not exist. Caught by mutating this file's own control
        # — the substring version stayed green on exactly that.
        unplumbed = [
            tier
            for tier, path in templates.items()
            if not re.search(rf"\.Values\.{tier}\.command\b", path.read_text(encoding="utf-8"))
        ]
        assert not unplumbed, (
            "these tiers declare a command in values.yaml that no template renders, so "
            f"the container runs its image default instead: {unplumbed}"
        )
