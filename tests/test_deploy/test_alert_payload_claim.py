"""The alert-payload claim that /privacy and /trust publish must stay true (core#1228).

Both legal pages state, as a representation about this repository's monitoring config:

    Telegram (Telegram FZ-LLC) -- delivery of infrastructure alerts to our on-call channel.
    Alert payloads carry service and container identifiers, not customer records.

Nothing connected that sentence to the config it describes. It was measured true on production
on 2026-09-10, and it could become false through a change that is locally reasonable and passes
every other check -- the Hetzner shape again: a legal page asserting something about
infrastructure with no mechanical link to it.

THREE PLACES CAN MAKE IT FALSE, AND EACH IS A REVIEW POINT HERE
--------------------------------------------------------------
1. What a rule's labels and annotations INTERPOLATE (``alerts.yml``). Allowlisted: container /
   instance / job identifiers and the numeric value.
2. What the Telegram message RENDERS (``contactpoints.yml``). Allowlisted: status, alertname,
   severity, and each alert's summary and description. Rendering ``.Labels`` wholesale -- or any
   label beyond those two -- would print whatever series labels a query happened to return.
3. What the exporter puts INTO series labels (``docker-compose.yml``). The stat_statements
   collector's include-query option would put SQL text into a label.

An allowlist rather than a denylist, on purpose: a new interpolation is refused until someone
establishes it can only ever carry an identifier or a number, which is what the pages promise.

WHAT THIS DOES NOT COVER, stated so it is not inferred
-----------------------------------------------------
- Rules or contact points created in the Grafana UI rather than provisioned from this repository.
- A series label that already carries customer data is not rendered today (point 2 keeps it out
  of the message), so it is not checked here -- if the message ever renders labels, point 2
  refuses first.
- ``tests/test_deploy/test_telegram_alert_payloads.py`` is on a different axis entirely: it proves
  a payload is DELIVERABLE whatever text is interpolated. A payload that sends perfectly can still
  carry a customer record, and would pass that suite green.

Every check is armed both ways: the real files pass, and a mutated copy of each real file carrying
the change the check exists to catch is refused -- in the same test.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
ALERTING = ROOT / "monitoring" / "grafana" / "provisioning" / "alerting"
RULES = ALERTING / "alerts.yml"
CONTACT_POINTS = ALERTING / "contactpoints.yml"
COMPOSE = ROOT / "docker-compose.yml"

# A Go-template action, with the optional whitespace-trim markers. YAML is parsed first, so the
# many comments in these files that QUOTE template syntax can never be mistaken for a site.
_ACTION = re.compile(r"\{\{-?\s*(.*?)\s*-?\}\}", re.S)

#: Every expression a rule's labels or annotations may interpolate. Adding one is a deliberate
#: act: first establish it can only carry a service or container identifier, or a number.
RULE_ALLOWLIST = frozenset({"$labels.name", "$labels.instance", "$labels.job", "$value"})

#: Every action the Telegram message may contain.
CONTACT_ALLOWLIST = frozenset(
    {
        ".Status | toUpper",
        ".CommonLabels.alertname",
        ".CommonLabels.severity",
        "range .Alerts",
        ".Annotations.summary",
        ".Annotations.description",
        "end",
    }
)

INCLUDE_QUERY = "include_query"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def rule_sites(doc: dict) -> list[tuple[str, str, str]]:
    """``(rule uid, field, expression)`` for every action in every rule's labels/annotations."""
    sites: list[tuple[str, str, str]] = []
    for group in doc.get("groups") or []:
        for rule in group.get("rules") or []:
            for section in ("labels", "annotations"):
                for key, value in (rule.get(section) or {}).items():
                    for expr in _ACTION.findall(str(value)):
                        sites.append((str(rule.get("uid")), f"{section}.{key}", expr.strip()))
    return sites


def contact_sites(doc: dict) -> list[tuple[str, str, str]]:
    """``(receiver uid, setting, action)`` for every action in every receiver's settings."""
    sites: list[tuple[str, str, str]] = []
    for point in doc.get("contactPoints") or []:
        for receiver in point.get("receivers") or []:
            for key, value in (receiver.get("settings") or {}).items():
                for expr in _ACTION.findall(str(value)):
                    sites.append((str(receiver.get("uid")), key, expr.strip()))
    return sites


def exporter_command(doc: dict) -> list[str]:
    service = (doc.get("services") or {}).get("postgres-exporter") or {}
    command = service.get("command") or []
    return command.split() if isinstance(command, str) else [str(arg) for arg in command]


def rule_violations(doc: dict) -> list[str]:
    return [
        f"{uid} {field}: {{{{ {expr} }}}}"
        for uid, field, expr in rule_sites(doc)
        if expr not in RULE_ALLOWLIST
    ]


def contact_violations(doc: dict) -> list[str]:
    return [
        f"{uid} {field}: {{{{ {expr} }}}}"
        for uid, field, expr in contact_sites(doc)
        if expr not in CONTACT_ALLOWLIST
    ]


def exporter_violations(doc: dict) -> list[str]:
    return [
        f"postgres-exporter runs {arg!r}, which can export query text as a series label"
        for arg in exporter_command(doc)
        if INCLUDE_QUERY in arg
    ]


# ---------------------------------------------------------------------------------------------
# Population first: a parser pointed at the wrong structure finds nothing, and nothing passes.
# ---------------------------------------------------------------------------------------------


def test_every_file_parses_to_a_real_population() -> None:
    rules, contacts = rule_sites(_load(RULES)), contact_sites(_load(CONTACT_POINTS))
    # Floors, not exact counts: an exact count would go red on every new annotation and teach
    # people to delete the test. A floor still catches an empty or misdirected parse.
    assert len(rules) >= 5, f"parsed only {len(rules)} rule template sites: {rules}"
    assert len(contacts) >= 5, f"parsed only {len(contacts)} contact-point template sites"
    assert any(
        arg.startswith("--collector.stat_statements") for arg in exporter_command(_load(COMPOSE))
    ), (
        "postgres-exporter no longer runs --collector.stat_statements, so the exporter check "
        "below is no longer looking at the collector it was written for -- re-derive it"
    )


# ---------------------------------------------------------------------------------------------
# The claim, as it stands.
# ---------------------------------------------------------------------------------------------


def test_rule_templates_interpolate_only_identifiers_and_numbers() -> None:
    violations = rule_violations(_load(RULES))
    assert not violations, (
        "alert rules interpolate expressions the /privacy and /trust claim does not cover "
        f"(allowed: {sorted(RULE_ALLOWLIST)}):\n  " + "\n  ".join(violations)
    )


def test_the_telegram_message_renders_only_allowlisted_fields() -> None:
    violations = contact_violations(_load(CONTACT_POINTS))
    assert not violations, (
        "the Telegram message renders fields the /privacy and /trust claim does not cover:\n  "
        + "\n  ".join(violations)
    )


def test_the_exporter_does_not_put_query_text_into_labels() -> None:
    violations = exporter_violations(_load(COMPOSE))
    assert not violations, "\n".join(violations)


# ---------------------------------------------------------------------------------------------
# Controls: each check refuses a mutated copy of the REAL file carrying the change it is for.
# ---------------------------------------------------------------------------------------------


def _first_annotated_rule(doc: dict) -> dict:
    for group in doc["groups"]:
        for rule in group["rules"]:
            if (rule.get("annotations") or {}).get("summary"):
                return rule
    raise AssertionError("no rule with a summary annotation to mutate")


def _telegram_receiver(doc: dict) -> dict:
    for point in doc["contactPoints"]:
        for receiver in point["receivers"]:
            if "message" in (receiver.get("settings") or {}):
                return receiver
    raise AssertionError("no receiver with a message template to mutate")


def _add_to_rule_summary(snippet: str):
    def mutate(doc: dict) -> None:
        _first_annotated_rule(doc)["annotations"]["summary"] += f" {snippet}"

    return mutate


def _add_to_message(snippet: str):
    def mutate(doc: dict) -> None:
        _telegram_receiver(doc)["settings"]["message"] += f"\n{snippet}\n"

    return mutate


def _add_include_query(doc: dict) -> None:
    doc["services"]["postgres-exporter"]["command"].append(
        "--collector.stat_statements.include_query"
    )


_CONTROLS = {
    "a rule interpolates a database label": (
        RULES,
        _add_to_rule_summary("{{ $labels.datname }}"),
        rule_violations,
        "$labels.datname",
    ),
    "a rule interpolates every label at once": (
        RULES,
        _add_to_rule_summary("{{ $labels }}"),
        rule_violations,
        "{{ $labels }}",
    ),
    "the message renders each alert's whole label set": (
        CONTACT_POINTS,
        _add_to_message("{{ range .Alerts }}{{ .Labels }}{{ end }}"),
        contact_violations,
        ".Labels",
    ),
    "the message renders a query-derived common label": (
        CONTACT_POINTS,
        _add_to_message("{{ .CommonLabels.queryid }}"),
        contact_violations,
        ".CommonLabels.queryid",
    ),
    "the exporter exports query text": (
        COMPOSE,
        _add_include_query,
        exporter_violations,
        INCLUDE_QUERY,
    ),
}


@pytest.mark.parametrize("name", list(_CONTROLS))
def test_each_check_refuses_the_change_it_exists_for(name: str) -> None:
    path, mutate, check, expected = _CONTROLS[name]
    real = _load(path)
    assert not check(real), "the real file must pass before a mutant means anything"
    mutant = copy.deepcopy(real)
    mutate(mutant)
    assert mutant != real, f"control {name!r} changed nothing -- it would test nothing"
    violations = check(mutant)
    assert any(expected in v for v in violations), (
        f"control {name!r} was NOT refused. Violations reported: {violations}"
    )
