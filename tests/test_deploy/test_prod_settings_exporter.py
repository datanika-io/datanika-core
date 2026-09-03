"""The prod-settings exporter and the rules that watch it must name the same metrics.

🚨 **Why this guard exists.** `deploy/server/export-prod-settings.sh` is applied by
**no workflow** (core#747) — it is hand-installed on the box. So nothing mechanical
connects the script to the Grafana rules that read its output. Rename a metric in the
script and the rule keeps evaluating a series nobody emits; under `noDataState: OK`,
which every rule in this project uses, that reads as **health**. The deploy's own
rule-health check would still pass, because a rule watching a non-existent metric is
perfectly *evaluable* — it just always says everything is fine.

This is the same shape as `celery-task-failures`, which watched a counter the app
process never served for the life of the project.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "server" / "export-prod-settings.sh"
ALERTS = REPO / "monitoring" / "grafana" / "provisioning" / "alerting" / "alerts.yml"

#: Metric families the exporter is contracted to emit.
EXPECTED_FAMILIES = {
    "datanika_prod_setting",
    "datanika_prod_setting_violation",
    "datanika_prod_settings_scrape_success",
    "datanika_prod_settings_last_run_timestamp_seconds",
}


def _script_text() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def _emitted_metric_names(text: str) -> set[str]:
    """Metric names for which the script actually writes a SAMPLE.

    🚨 **A `# TYPE` header is not an emission, and an earlier version of this guard
    counted it as one.** The mutation control that renamed the metric on the `echo`
    line — leaving the `# HELP`/`# TYPE` headers untouched, which is exactly what a
    careless rename does — left this test **green**, because the name was still
    present in the header text. A TYPE declaration with no samples under it produces
    no series at all; Prometheus simply has nothing to store.

    So: consider only `echo` payloads that are *not* Prometheus comment lines. This
    is the project's *assert on the executable line, not the prose documenting it*
    rule, arriving one level down — the "prose" here is the metric's own metadata.
    """
    names: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue  # shell comment: prose, not an emission
        for payload in re.findall(r'echo\s+"((?:[^"\\]|\\.)*)"', stripped):
            if payload.lstrip().startswith("#"):
                continue  # a HELP/TYPE header declares; it does not emit
            for m in re.finditer(r"(datanika_prod_[a-z0-9_]+)\s*[{ ]", payload):
                names.add(m.group(1))
    return names


def _rule_expressions() -> dict[str, str]:
    doc = yaml.safe_load(ALERTS.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for group in doc["groups"]:
        for rule in group["rules"]:
            for datum in rule.get("data", []):
                expr = datum.get("model", {}).get("expr")
                if expr:
                    out[rule["title"]] = expr
    return out


def test_the_script_exists_and_is_executable_shell() -> None:
    assert SCRIPT.is_file(), f"{SCRIPT} is missing"
    assert _script_text().startswith("#!"), "no shebang"


def test_the_exporter_emits_every_metric_family_it_promises() -> None:
    emitted = _emitted_metric_names(_script_text())
    missing = EXPECTED_FAMILIES - emitted
    assert not missing, f"the exporter no longer emits: {sorted(missing)}"


def test_every_prod_setting_metric_a_rule_reads_is_one_the_script_emits() -> None:
    """The coupling that actually matters, in the direction that fails silently."""
    emitted = _emitted_metric_names(_script_text())
    referenced: dict[str, set[str]] = {}
    for title, expr in _rule_expressions().items():
        found = set(re.findall(r"datanika_prod_[a-z0-9_]+", expr))
        if found:
            referenced[title] = found

    assert referenced, (
        "no alert rule reads any datanika_prod_* metric. The exporter would be a "
        "number nobody looks at — which is the state core#725 was filed about."
    )

    for title, names in referenced.items():
        orphans = names - emitted
        assert not orphans, (
            f"alert rule {title!r} reads {sorted(orphans)}, which "
            f"export-prod-settings.sh does not emit. Under noDataState: OK that rule "
            f"can never fire and reads as health."
        )


def test_the_local_file_path_setting_is_required_false_not_merely_recorded() -> None:
    """core#969 / core#985: the code default is True, so prod is safe only if graded.

    A manifest entry with `require=-` is *recorded* but not graded. This setting must
    be graded, or the exporter reports the unsafe value and nothing objects.
    """
    text = _script_text()
    manifest = re.search(r"MANIFEST=\"(.*?)\"", text, re.S)
    assert manifest, "MANIFEST block not found"
    rows = [r for r in manifest.group(1).splitlines() if r.strip()]
    entry = [r for r in rows if "datanika_allow_local_file_paths" in r]
    assert entry, "datanika_allow_local_file_paths is not in the manifest at all"
    assert entry[0].strip().endswith("|false"), (
        f"datanika_allow_local_file_paths must be REQUIRED false, got: {entry[0]!r}"
    )


def test_absent_is_graded_as_a_violation() -> None:
    """An image without the setting is not a compliant image.

    Treating a missing attribute as its required value is `noDataState: OK` in a
    different costume, and it is the exact reading that would have been produced on
    2026-09-03 before the gate shipped.
    """
    text = _script_text()
    assert re.search(r"ABSENT\)\s*val=-1;\s*state=absent", text), (
        "the absent branch no longer maps to state=absent"
    )
    # state=absent must not equal any require= value, so viol stays 1.
    assert "viol=1" in text and '[ "${state}" = "${require}" ] && viol=0' in text, (
        "the violation is no longer computed by comparing state to require"
    )


def test_the_watchdog_covers_the_metric_disappearing() -> None:
    """A filtering rule yields no series when its input vanishes — which reads as OK."""
    exprs = _rule_expressions()
    watchdogs = [e for e in exprs.values() if "absent(" in e and "datanika_prod_" in e]
    assert watchdogs, (
        "no absent() watchdog over the datanika_prod_* metrics. Without one, losing "
        "the cron, the script or the textfile mount silently disables the violation "
        "rule and production reads as compliant."
    )


def test_staging_is_not_graded_by_this_exporter() -> None:
    """Staging has its own .env.docker and deliberately keeps the permissive default."""
    text = _script_text()
    containers = re.search(r'CONTAINERS="([^"]*)"', text)
    assert containers, "CONTAINERS list not found"
    assert "staging" not in containers.group(1), (
        "the exporter would grade staging, which runs from /opt/datanika-staging with "
        "its own env file and is not required to match production"
    )
