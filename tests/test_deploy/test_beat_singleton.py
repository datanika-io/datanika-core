"""Celery Beat must exist, exactly once, with durable schedule state (core#653).

Three separate ways to reintroduce core#653, and this file blocks each one
--------------------------------------------------------------------------
core#653 was not "someone forgot a container". It is a defect whose entire symptom is
*absence*: `celery_app.py` has defined an hourly ``datanika.run_maintenance`` since the
project started, no beat process ever existed, and so five maintenance sweeps have never
run in production. Nothing errored. Nothing logged. The only ``run_maintenance`` line in
the production worker's log is ``. datanika.run_maintenance`` — the task *registration*
banner, which reads exactly like evidence that it works.

That shape is why the fix needs mechanical guards rather than a comment:

1. **No beat at all.** The original bug. ``test_every_manifest_runs_beat``.
2. **Two beats.** Beat is a pure producer; a second one double-fires every entry. This is
   the same hazard that keeps ``celery`` out of the blue/green pair, and it is why the Helm
   Deployment hardcodes ``replicas: 1`` instead of exposing a ``replicaCount`` knob.
   ``test_exactly_one_beat_per_manifest`` / ``test_helm_beat_is_a_hardcoded_singleton``.
3. **A beat whose schedule state is ephemeral.** The subtle one, and the reason this file
   is longer than "assert a service exists". ``beat_schedule`` uses an INTERVAL
   (``schedule: 3600.0``). ``PersistentScheduler`` seeds ``last_run_at = now`` whenever it
   finds no stored state, so a beat with a container-local schedule file restarts its hour
   on every deploy. On 2026-08-30 this project shipped **16 promotions in one day**; an
   hourly job would have fired zero times, while ``docker ps`` showed a healthy beat
   container the whole time. That is core#653 again, wearing a green tick.
   ``test_beat_schedule_lives_on_a_named_volume``.

``worker -B`` is rejected for a fourth reason (``test_no_worker_dash_b``): it is the
smaller diff, and its failure mode is invisibility. A beat thread that dies inside a live
worker leaves the container ``Up`` and ``healthy``, which is indistinguishable from the
bug. A separate container has a ``container_last_seen`` series, so ``container-down`` can
watch it — asserted by ``test_alert_rule_watches_the_beat_container``.

Derived, not restated
---------------------
The beat service is found by looking for ``celery … beat`` in a command, never by name, so
renaming the service does not silently empty this suite. The same convention as
``test_deploy_service_coverage.py`` and ``test_deployment_manifest_parity.py``, and for the
same reason: a restated list drifts, and drift is the bug being hunted.
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

CHART = ROOT / "deploy" / "helm" / "datanika"
BEAT_TEMPLATE = CHART / "templates" / "beat-deployment.yaml"
BEAT_PVC = CHART / "templates" / "beat-pvc.yaml"
ALERTS = ROOT / "monitoring" / "grafana" / "provisioning" / "alerting" / "alerts.yml"


def _strip_comments(text: str) -> str:
    """Drop `#` comment lines before searching a manifest for what it *does*.

    Not optional, and this file proved it on its own first run: `beat-deployment.yaml`
    explains at length why it does **not** expose a `replicaCount`, and the assertion
    forbidding that string matched the explanation. Same failure the service-coverage
    test hit when it parsed a service called `won` out of prose.
    """
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _command_text(service: dict) -> str:
    """A compose `command:` as one string, whatever form it was written in."""
    cmd = service.get("command", "")
    if isinstance(cmd, list):
        return " ".join(str(part) for part in cmd)
    return str(cmd)


def _beat_services(manifest: dict) -> dict[str, dict]:
    """Services whose command runs `celery … beat` — found by behaviour, not by name."""
    out = {}
    for name, service in (manifest.get("services") or {}).items():
        text = _command_text(service)
        if re.search(r"\bcelery\b.*\bbeat\b", text):
            out[name] = service
    return out


@pytest.fixture(params=sorted(COMPOSE_MANIFESTS), ids=sorted(COMPOSE_MANIFESTS))
def compose(request):
    path = COMPOSE_MANIFESTS[request.param]
    return request.param, yaml.safe_load(path.read_text(encoding="utf-8"))


def test_every_manifest_runs_beat(compose):
    """core#653's original state: the schedule is defined and nothing runs it."""
    label, manifest = compose
    beats = _beat_services(manifest)
    assert beats, (
        f"{label} defines no service running `celery … beat`. `celery_app.py` schedules "
        "`datanika.run_maintenance` hourly and beat is its only invocation path, so "
        "without one the five maintenance sweeps never run — silently, which is core#653."
    )


def test_exactly_one_beat_per_manifest(compose):
    """Two beats double-fire every scheduled entry."""
    label, manifest = compose
    beats = _beat_services(manifest)
    assert len(beats) == 1, (
        f"{label} has {len(beats)} beat services ({sorted(beats)}). Beat is a singleton: "
        "a second one dispatches every due entry a second time. This is the same reason "
        "`celery` is deliberately not part of the blue/green pair."
    )


def test_beat_is_not_blue_green(compose):
    """A profile-gated second colour is two beats by another name."""
    label, manifest = compose
    ((name, service),) = _beat_services(manifest).items()
    assert "bluegreen" not in (service.get("profiles") or []), (
        f"{label}: service `{name}` is in the bluegreen profile. During a swap both colours "
        "run, so every scheduled task would fire twice for the length of the swap."
    )


def test_beat_schedule_lives_on_a_named_volume(compose):
    """The subtle regression: an interval schedule that resets on every deploy.

    `PersistentScheduler` seeds `last_run_at = now` when it finds no stored state, so an
    ephemeral schedule file makes an hourly job restart its hour on every recreate.
    """
    label, manifest = compose
    ((name, service),) = _beat_services(manifest).items()
    command = _command_text(service)

    match = re.search(r"--schedule[=\s]+(\S+)", command)
    assert match, (
        f"{label}: service `{name}` does not pass `--schedule`, so celery writes "
        "`celerybeat-schedule` into the container's own filesystem and the hourly "
        "interval restarts on every deploy (core#653, subtler form)."
    )
    schedule_path = match.group(1)

    mounted = []
    for mount in service.get("volumes") or []:
        if not isinstance(mount, str) or ":" not in mount:
            continue
        source, target = mount.split(":")[:2]
        # A named volume, not a bind mount: bind sources start with . or /
        if not source.startswith((".", "/")):
            mounted.append(target)

    assert any(schedule_path.startswith(f"{target.rstrip('/')}/") for target in mounted), (
        f"{label}: `{name}` writes its schedule to {schedule_path}, which is not inside a "
        f"named volume (named-volume mounts: {mounted or 'none'}). The file must survive "
        "`up -d`, or `last_run_at` is lost and an hourly task can never fire on a day with "
        "frequent deploys — while the container reads perfectly healthy."
    )


def test_no_worker_dash_b(compose):
    """`worker -B` is the alternative whose failure mode is the bug itself."""
    label, manifest = compose
    for name, service in (manifest.get("services") or {}).items():
        command = _command_text(service)
        if "worker" not in command:
            continue
        assert not re.search(r"\s-B\b|\s--beat\b", command), (
            f"{label}: service `{name}` embeds beat in the worker (`-B`). A beat thread "
            "that dies inside a live worker leaves the container Up and healthy, which is "
            "exactly the unobservable state core#653 is about. Run beat as its own "
            "container so `container_last_seen` can see it die."
        )


def test_deploy_step_recreates_beat_alongside_celery():
    """A compose service named by no deploy step keeps its old config forever (core#616)."""
    workflow = (ROOT / ".github" / "workflows" / "deploy-pointer.yml").read_text(encoding="utf-8")
    manifest = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    ((beat_name, _),) = _beat_services(manifest).items()

    ups = re.findall(r"compose[^\n]*?\bup\b\s+-d\b([^\n'\"]*)", workflow)
    assert any(beat_name in line.split() for line in ups), (
        f"deploy-pointer.yml never brings up `{beat_name}`. It would run on whatever "
        "config it was first started with, indefinitely and with no error — core#616."
    )


def test_helm_beat_is_a_hardcoded_singleton():
    """`replicas` must not be a knob: two beats is not a supported configuration."""
    assert BEAT_TEMPLATE.exists(), (
        "deploy/helm/datanika/templates/beat-deployment.yaml is missing, so every Helm "
        "self-hoster ships core#653 — maintenance sweeps that never run."
    )
    text = _strip_comments(BEAT_TEMPLATE.read_text(encoding="utf-8"))

    assert re.search(r"^\s*replicas:\s*1\s*$", text, re.MULTILINE), (
        "beat-deployment.yaml must hardcode `replicas: 1`. Exposing a replicaCount offers "
        "a footgun with a friendly name — two beats double-fire every scheduled task."
    )
    assert "replicaCount" not in text, (
        "beat-deployment.yaml references a replicaCount value; beat is a singleton."
    )
    assert re.search(r"type:\s*Recreate", text), (
        "beat-deployment.yaml must use `strategy: Recreate`. A RollingUpdate runs the old "
        "and new beat concurrently for the length of the rollout — a double-fire window."
    )


def test_helm_beat_state_is_persistent():
    """An emptyDir here is the reset-on-deploy defect, in Kubernetes."""
    assert BEAT_PVC.exists(), "deploy/helm/datanika/templates/beat-pvc.yaml is missing."
    template = _strip_comments(BEAT_TEMPLATE.read_text(encoding="utf-8"))
    assert "persistentVolumeClaim" in template, (
        "beat-deployment.yaml must mount a PersistentVolumeClaim for its schedule state. "
        "With an emptyDir the hourly interval restarts on every rollout (core#653)."
    )
    assert "emptyDir" not in template, "beat schedule state must not be an emptyDir."

    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    schedule_flag = " ".join(str(p) for p in values["beat"]["command"])
    match = re.search(r"--schedule\s+(\S+)", schedule_flag)
    assert match, "values.yaml beat.command must pass --schedule."
    mount_dir = match.group(1).rsplit("/", 1)[0]
    assert f"mountPath: {mount_dir}" in template, (
        f"values.yaml writes the schedule under {mount_dir} but beat-deployment.yaml does "
        "not mount a volume there — the flag and the mount must agree, or the file is "
        "written to the container's own filesystem and lost on every rollout."
    )


def test_alert_rule_watches_the_beat_container():
    """Being a container is only useful if something looks at it."""
    manifest = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    ((_, service),) = _beat_services(manifest).items()
    container = service["container_name"]

    alerts = ALERTS.read_text(encoding="utf-8")
    patterns = re.findall(r'container_last_seen\{name=~"([^"]+)"\}', alerts)
    assert patterns, "no container_last_seen selector found in alerts.yml"

    covered = any(re.fullmatch(pattern, container) for pattern in patterns)
    assert covered, (
        f"No container_last_seen alert selector matches `{container}`. Selectors present: "
        f"{patterns}. core#653's acceptance criterion 3 is that beat's absence is "
        "detectable; an unwatched container satisfies criterion 2 and not this one."
    )


def test_beat_alert_selector_does_not_match_staging():
    """core#615's mistake, in the other direction: Prometheus anchors regexes.

    `datanika-.*` would match `datanika-staging-beat`, which is redeployed on every push
    to `dev` — paging `critical` on every merge.
    """
    staging = yaml.safe_load(
        (ROOT / "deploy" / "staging" / "docker-compose.yml").read_text(encoding="utf-8")
    )
    ((_, staging_service),) = _beat_services(staging).items()
    staging_container = staging_service["container_name"]

    alerts = ALERTS.read_text(encoding="utf-8")
    for pattern in re.findall(r'container_last_seen\{name=~"([^"]+)"\}', alerts):
        assert not re.fullmatch(pattern, staging_container), (
            f"alert selector `{pattern}` matches the STAGING container "
            f"`{staging_container}`. Staging is redeployed on every push to dev, so this "
            "would page critical on every merge (core#615's backtested failure)."
        )
