"""Static semantics of the Grafana alerting config (core#599).

Paired CI probe for core#598, where the founder was paged **four times** by
`app.datanika.io` availability alerts that were not outages. The root cause — a
lossy upstream DNS resolver on the VPS — is not CI-catchable. The three config
defects that turned a **single 15-second failed sample** into four critical
pages are all static properties of files in this repo, and each is checked here.

Why a bad `for:` cannot debounce these rules
--------------------------------------------
Every availability rule is a *filtering* expression: `probe_success == 0`
returns a series **only while the probe is failing**. Successes are absent from
the result, so `reduce: last` never sees a recovery — it only ever sees the
failing sample, for as long as that sample sits inside the query window.

With `relativeTimeRange.from: 60` and a 30s group interval, one failed sample is
visible to 2-3 consecutive evaluations. `for: 30s` needs 2. **So `for: 30s`
debounced nothing at all**, and a single 15-second blip paged critical. The
arithmetic that actually holds is:

    for >= window + group_interval

...*or* the duration requirement lives in the query instead (a range/subquery
aggregation demanding more than one sample). A rule satisfying **neither** is
the defect. See `TestBlipArithmetic`.

Do not "fix" a failure here by raising `for:` on a rule whose query already
encodes the duration — that undoes core#584's 60-second detection floor, which
is the trade-off core#600 deliberately rejected.

The other assertions
--------------------
* `TestAnnotationForAgreement` — the alert body said *"failed continuously for
  2 minutes"* while the deployed rule was `for: 30s`. #584 changed the `for:`
  and left the prose, which then sat in production for five weeks describing a
  threshold that no longer existed. It was one of two leads the founder
  hand-checked during triage: **a wrong number in a pager message costs triage
  time exactly when triage time is expensive.**
* `TestRouteCompleteness` — the `severity = critical` route shortened
  `group_wait` but inherited the root `group_interval: 5m`, so every RESOLVED
  notice arrived >=5 min after its FIRING. Every incident then looked like a
  uniform 5-minute outage, which is a property of the notification pipeline and
  encodes nothing about the system.
* `TestReferentialIntegrity` — a rule naming an `instance` nobody scrapes can
  never fire, and reads exactly like a rule that is fine.
* `TestThresholdSatisfiability` — core#504 shipped `gt [0]` against a series
  whose value is always `0`: unsatisfiable, silently dead for months.
* `TestScrapeIntervalCoupling` — `[2m:15s]` hardcodes the blackbox scrape
  interval. If `prometheus.yml` retunes that job, the sample count per window
  changes and the threshold silently means something else.

Everything here is derived from the config rather than restated from it, per
`test_deployment_manifest_parity.py`: a restated list drifts, and drift is the
bug being hunted.

Not in scope: runtime behaviour (is the rule loaded, does Grafana evaluate it,
does Telegram deliver). That is the CD rule-health gate from core#426/#528.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
MONITORING = ROOT / "monitoring"
ALERTS = MONITORING / "grafana" / "provisioning" / "alerting" / "alerts.yml"
POLICIES = MONITORING / "grafana" / "provisioning" / "alerting" / "policies.yml"
PROMETHEUS = MONITORING / "prometheus.yml"

# Rules whose defect is filed and not yet fixed. `strict=True` is load-bearing:
# when the rule is fixed the case XPASSes, pytest fails, and the entry *must* be
# deleted. The bug can neither be forgotten nor silently repaired.
KNOWN_VIOLATIONS = {
    ("blip", "app-unhealthy"): "core#604 — bare `up == 0` filter with for: 30s",
    ("annotation", "app-unhealthy"): "core#604 — prose says 2 minutes, for: 30s",
}

_UNIT_SECONDS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
    "w": 604800, "week": 604800, "weeks": 604800,
}

# Aggregations that genuinely accumulate across samples in their range.
_RANGE_AGGREGATIONS = (
    "count_over_time", "sum_over_time", "avg_over_time", "min_over_time",
    "max_over_time", "stddev_over_time", "quantile_over_time",
    "increase", "rate", "irate", "delta", "idelta",
)

# A duration claimed as the *condition's* duration, e.g. "has failed for 5
# minutes", "stuck in past_due for over 72h". Deliberately narrow: "in the last
# 15 minutes", "over 26h ago" and "with a 12h grace" describe the query window
# or a future plan, not `for:`, and must not be flagged.
_CONTINUITY_DURATION = re.compile(
    r"\bfor\s+(?:over\s+|more\s+than\s+|longer\s+than\s+|at\s+least\s+|"
    r"continuously\s+)*(\d+(?:\.\d+)?)\s*"
    r"(seconds?|secs?|minutes?|mins?|hours?|days?|weeks?|[smhdw])\b",
    re.IGNORECASE,
)

# `[2m]` or the subquery form `[2m:15s]`.
_RANGE_SELECTOR = re.compile(r"\[(\d+(?:\.\d+)?)([smhdw])(?::(\d+(?:\.\d+)?)([smhdw]))?\]")

# `time() - <anything> > 90` — a staleness comparison. The metric must have been
# missing for N seconds, so no single sample can satisfy it.
_STALENESS = re.compile(r"time\(\s*\)\s*-.*?>\s*(\d+(?:\.\d+)?)", re.DOTALL)

_GRAFANA_DURATION = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhdw]?)$")


def _seconds(value, *, default: float | None = None) -> float:
    """Parse a Grafana duration (`30s`, `5m`, `0s`, bare seconds) to seconds."""
    if value is None:
        if default is None:
            raise ValueError("missing duration")
        return default
    if isinstance(value, (int, float)):
        return float(value)
    match = _GRAFANA_DURATION.match(str(value).strip())
    if not match:
        raise ValueError(f"unparseable duration {value!r}")
    number, unit = match.groups()
    return float(number) * _UNIT_SECONDS[unit or "s"]


class Rule:
    """One alert rule, flattened with the group context it needs."""

    def __init__(self, group: dict, raw: dict):
        self.raw = raw
        self.group_name = group.get("name", "?")
        self.group_interval = _seconds(group.get("interval"), default=60.0)
        self.uid = raw.get("uid") or raw.get("title", "?")
        self.title = raw.get("title", self.uid)
        self.for_seconds = _seconds(raw.get("for"), default=0.0)
        self.severity = (raw.get("labels") or {}).get("severity")

        nodes = raw.get("data") or []
        self.query_nodes = [n for n in nodes if n.get("datasourceUid") != "__expr__"]
        self.reducers = [
            n["model"].get("reducer")
            for n in nodes
            if (n.get("model") or {}).get("type") == "reduce"
        ]
        self.thresholds = [
            cond.get("evaluator", {})
            for n in nodes
            if (n.get("model") or {}).get("type") == "threshold"
            for cond in (n["model"].get("conditions") or [])
        ]

        models = [n.get("model") or {} for n in self.query_nodes]
        self.expr = next((m["expr"] for m in models if m.get("expr")), "")
        self.raw_sql = next((m["rawSql"] for m in models if m.get("rawSql")), "")
        self.query_text = self.expr or self.raw_sql
        self.window = _seconds(
            (self.query_nodes[0].get("relativeTimeRange") or {}).get("from")
            if self.query_nodes else None,
            default=0.0,
        )
        annotations = raw.get("annotations") or {}
        self.annotation_text = " ".join(str(v) for v in annotations.values())

    @property
    def is_promql(self) -> bool:
        """SQL rules read committed database state, which is not sampled.

        A scrape blip is a PromQL-only phenomenon, so the blip arithmetic
        applies only to rules with a PromQL `expr`.
        """
        return bool(self.expr)

    def __repr__(self) -> str:  # pragma: no cover - test id only
        return self.uid


def _strip_label_matchers(expr: str) -> str:
    """Remove `{...}` so `=~` inside a selector is not read as a comparison."""
    return re.sub(r"\{[^{}]*\}", "{}", expr)


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _rules_from(doc: dict) -> list[Rule]:
    return [Rule(g, r) for g in doc.get("groups", []) for r in g.get("rules", [])]


def _scrape_interval(prom: dict, job_name: str | None = None) -> float:
    """Derived, not restated — a per-job override wins over the global."""
    global_interval = _seconds(
        (prom.get("global") or {}).get("scrape_interval"), default=15.0
    )
    if job_name is None:
        return global_interval
    for job in prom.get("scrape_configs", []):
        if job.get("job_name") == job_name:
            return _seconds(job.get("scrape_interval"), default=global_interval)
    return global_interval


def _encodes_duration_in_query(rule: Rule, scrape_interval: float) -> bool:
    """Does the *query* already demand more than one sample?

    Two accepted forms, both narrow on purpose:

    1. A range/subquery aggregation whose window spans >= 2 samples **and**
       whose threshold demands >= 2 of them. The second half matters:
       `count_over_time((probe_success == 0)[2m:15s]) > 0` passes a naive
       "uses an aggregation" test while being exactly as single-sample
       triggered as the bare filter it replaced (core#599 comment).
    2. A staleness comparison `time() - <metric> > N` with N >= 2 samples.
       The metric must have gone unseen for N seconds, which no single scrape
       can produce.
    """
    expr = rule.expr
    if not expr:
        return False

    staleness = _STALENESS.search(expr)
    if staleness and float(staleness.group(1)) >= 2 * scrape_interval:
        return True

    selector = _RANGE_SELECTOR.search(expr)
    if not selector:
        return False
    if not any(fn in expr for fn in _RANGE_AGGREGATIONS):
        return False

    range_seconds = float(selector.group(1)) * _UNIT_SECONDS[selector.group(2)]
    step = (
        float(selector.group(3)) * _UNIT_SECONDS[selector.group(4)]
        if selector.group(3)
        else scrape_interval
    )
    samples = range_seconds / step if step else 0
    if samples < 2:
        return False

    # For a sample *count* the threshold decides how many are required.
    if "count_over_time" in expr:
        for evaluator in rule.thresholds:
            params = evaluator.get("params") or [0]
            kind = evaluator.get("type")
            if kind == "gt":
                required = float(params[0]) + 1
            elif kind == "gte":
                required = float(params[0])
            else:
                # lt/lte/outside_range against a count do not express "N or
                # more samples failed"; treat as not encoding a duration.
                return False
            if required < 2:
                return False
        return True

    return True


def _is_filtering(rule: Rule) -> bool:
    """A filtering rule yields a series only while unhealthy.

    That is what makes `reduce: last` blind to recovery. `absent()` behaves the
    same way — it returns a series precisely when the metric is missing.
    """
    if not rule.expr:
        return False
    if "absent(" in rule.expr:
        return True
    stripped = _strip_label_matchers(rule.expr)
    return bool(re.search(r"(==|!=|>=|<=|>|<)", stripped))


def _mark(kind: str, rule: Rule):
    """Attach the xfail marker for a filed-but-unfixed rule."""
    reason = KNOWN_VIOLATIONS.get((kind, rule.uid))
    marks = []
    if reason:
        marks.append(
            pytest.mark.xfail(
                strict=True,
                reason=(
                    f"{reason}. When the rule is fixed this XPASSes and fails "
                    f"the suite — delete the KNOWN_VIOLATIONS entry, do not "
                    f"relax the assertion."
                ),
            )
        )
    return pytest.param(rule, id=rule.uid, marks=marks)


ALERT_DOC = _load(ALERTS)
PROM_DOC = _load(PROMETHEUS)
POLICY_DOC = _load(POLICIES)
ALL_RULES = _rules_from(ALERT_DOC)
GLOBAL_SCRAPE = _scrape_interval(PROM_DOC)

PROMQL_RULES = [r for r in ALL_RULES if r.is_promql]
FILTERING_RULES = [r for r in PROMQL_RULES if _is_filtering(r)]


def test_the_config_actually_parsed():
    """A silently-empty parse would make every assertion below vacuously true.

    This is the check that stops this file from becoming another green that
    proves nothing: if `alerts.yml` moved or the schema changed, every
    parametrized test would collect zero cases and the suite would still pass.
    """
    assert len(ALL_RULES) >= 25, f"only {len(ALL_RULES)} rules parsed from {ALERTS}"
    assert len(FILTERING_RULES) >= 10, "filtering-rule detection collapsed"
    assert POLICY_DOC.get("policies"), "policies.yml parsed to nothing"
    assert PROM_DOC.get("scrape_configs"), "prometheus.yml parsed to nothing"


class TestBlipArithmetic:
    """A single failed sample must not be able to page anyone."""

    @pytest.mark.parametrize(
        "rule", [_mark("blip", r) for r in FILTERING_RULES]
    )
    def test_single_sample_cannot_reach_firing(self, rule: Rule):
        if _encodes_duration_in_query(rule, GLOBAL_SCRAPE):
            return  # duration lives in the query; `for: 0s` is correct there

        required = rule.window + rule.group_interval
        assert rule.for_seconds >= required, (
            f"{rule.uid} ({rule.title!r}, group {rule.group_name!r}) is a "
            f"filtering expression whose duration requirement is encoded "
            f"NOWHERE.\n"
            f"    expr            : {rule.expr}\n"
            f"    for             : {rule.for_seconds:g}s\n"
            f"    query window    : {rule.window:g}s\n"
            f"    group interval  : {rule.group_interval:g}s\n"
            f"    needs for >=    : {required:g}s\n\n"
            f"One failed sample stays in the {rule.window:g}s window across "
            f"{max(1, int(rule.window // rule.group_interval))} or more "
            f"evaluations, and `reduce: last` never sees a recovery because a "
            f"filtering expression drops healthy samples entirely. So `for: "
            f"{rule.for_seconds:g}s` debounces nothing.\n\n"
            f"Fix by EITHER raising `for` to >= {required:g}s, OR moving the "
            f"duration into the query (e.g. "
            f"count_over_time((<expr>)[2m:{GLOBAL_SCRAPE:g}s]) with a "
            f"threshold demanding >= 2 samples). Do not simply raise `for` on "
            f"a rule that already aggregates — see core#600."
        )


class TestAnnotationForAgreement:
    """The pager message must not state a threshold that does not exist."""

    @pytest.mark.parametrize(
        "rule", [_mark("annotation", r) for r in ALL_RULES if r.annotation_text]
    )
    def test_prose_duration_matches_something_real(self, rule: Rule):
        claims = _CONTINUITY_DURATION.findall(rule.annotation_text)
        if not claims:
            return

        query = rule.query_text
        threshold_params = {
            str(p)
            for evaluator in rule.thresholds
            for p in (evaluator.get("params") or [])
        }

        for number, unit in claims:
            claimed = float(number) * _UNIT_SECONDS[unit.lower()]
            if claimed == rule.for_seconds:
                continue
            # The prose may legitimately describe a duration in the *query*
            # ("has not been seen for over 60 seconds" -> `... > 60`).
            literal = number.rstrip("0").rstrip(".") if "." in number else number
            in_query = (
                re.search(rf"\b{re.escape(literal)}\s*{unit[0].lower()}\b", query)
                or re.search(rf"\b{re.escape(literal)}\s+{re.escape(unit)}\b",
                             query, re.IGNORECASE)
                or re.search(rf"(?<![\w.]){re.escape(literal)}(?![\w.])", query)
                or literal in threshold_params
            )
            if in_query:
                continue

            assert False, (
                f"{rule.uid} ({rule.title!r}) tells the on-call a duration that "
                f"exists nowhere in the rule.\n"
                f"    annotation claims : {number} {unit} "
                f"({claimed:g}s)\n"
                f"    actual `for`      : {rule.for_seconds:g}s\n"
                f"    query             : {query.strip()[:160]}\n\n"
                f"core#598: the alert body read 'failed continuously for 2 "
                f"minutes' while the rule was `for: 30s` and the real failure "
                f"was 15 seconds — three different numbers for one event, and "
                f"the wrong one sent triage chasing a regression that did not "
                f"exist.\n\n"
                f"Fix by correcting the prose, or delete the duration from it "
                f"and let Grafana template the value."
            )


class TestRouteCompleteness:
    """A route that shortens `group_wait` must not inherit a long `group_interval`."""

    @staticmethod
    def _walk(routes, parent: dict):
        for route in routes or []:
            inherited = {**parent, **{k: v for k, v in route.items() if k != "routes"}}
            yield route, parent, inherited
            yield from TestRouteCompleteness._walk(route.get("routes"), inherited)

    def test_urgent_routes_set_their_own_group_interval(self):
        failures = []
        for policy in POLICY_DOC.get("policies", []):
            root = {k: v for k, v in policy.items() if k != "routes"}
            for route, parent, _ in self._walk(policy.get("routes"), root):
                if "group_wait" not in route:
                    continue
                if _seconds(route["group_wait"]) >= _seconds(
                    parent.get("group_wait"), default=30.0
                ):
                    continue
                if "group_interval" in route:
                    continue
                failures.append(
                    f"  route {route.get('matchers')} shortens group_wait to "
                    f"{route['group_wait']} but inherits group_interval="
                    f"{parent.get('group_interval')} from its parent"
                )
        assert not failures, (
            "A notification route declared itself urgent and then inherited a "
            "slow batching interval:\n"
            + "\n".join(failures)
            + "\n\ncore#598: the `severity = critical` route set group_wait: 10s "
            "and inherited group_interval: 5m, so a RESOLVED notice could not "
            "arrive sooner than 5 minutes after the FIRING one. Every incident "
            "then measured exactly 5 minutes — a property of the pipeline, not "
            "of the system, and it made a 15-second blip look like an outage."
        )


class TestReferentialIntegrity:
    """A rule watching something nobody scrapes can never fire."""

    @staticmethod
    def _blackbox_targets(prom: dict) -> set[str]:
        targets: set[str] = set()
        for job in prom.get("scrape_configs", []):
            for static in job.get("static_configs") or []:
                targets.update(static.get("targets") or [])
        return targets

    def test_every_probed_instance_is_a_configured_target(self):
        targets = self._blackbox_targets(PROM_DOC)
        missing = []
        for rule in ALL_RULES:
            for instance in re.findall(r'instance\s*=\s*"([^"]+)"', rule.expr):
                if instance not in targets:
                    missing.append(f"  {rule.uid}: instance={instance!r}")
        assert not missing, (
            "Alert rules reference probe instances that `prometheus.yml` never "
            "scrapes, so they can never fire and look identical to healthy:\n"
            + "\n".join(missing)
        )

    def test_every_referenced_job_is_a_configured_job(self):
        jobs = {j.get("job_name") for j in PROM_DOC.get("scrape_configs", [])}
        missing = []
        for rule in ALL_RULES:
            for op, pattern in re.findall(r'job\s*(=~|=)\s*"([^"]+)"', rule.expr):
                matcher = re.compile(f"^{pattern}$") if op == "=~" else None
                hit = (
                    any(matcher.match(j) for j in jobs if j)
                    if matcher
                    else pattern in jobs
                )
                if not hit:
                    missing.append(f"  {rule.uid}: job{op}{pattern!r}")
        assert not missing, (
            "Alert rules reference scrape jobs that do not exist in "
            "`prometheus.yml`:\n" + "\n".join(missing)
        )

    def test_every_scrape_target_is_a_real_compose_service(self):
        compose = _load(ROOT / "docker-compose.yml")
        services = set(compose.get("services") or {})
        orphans = []
        for job in PROM_DOC.get("scrape_configs", []):
            for static in job.get("static_configs") or []:
                for target in static.get("targets") or []:
                    if target.startswith(("http://", "https://")):
                        continue  # blackbox probe URL, not a scrape host
                    host = target.split(":")[0]
                    if host not in services:
                        orphans.append(
                            f"  job {job.get('job_name')!r} scrapes {target!r} "
                            f"but no compose service is named {host!r}"
                        )
        assert not orphans, (
            "`prometheus.yml` scrapes hosts that no longer exist in "
            "`docker-compose.yml`. A permanently-down target is the noise that "
            "makes a real down target unremarkable (core#598 'Also found'):\n"
            + "\n".join(orphans)
        )


class TestThresholdSatisfiability:
    """A threshold that cannot be met is a rule that cannot fire."""

    @staticmethod
    def _constant_value(rule: Rule) -> float | None:
        """The only value the expression can produce, when that is knowable."""
        if not rule.expr:
            return None
        if rule.expr.strip().startswith("absent("):
            return 1.0
        match = re.search(r"==\s*(\d+(?:\.\d+)?)\s*$", _strip_label_matchers(rule.expr))
        return float(match.group(1)) if match else None

    @pytest.mark.parametrize("rule", PROMQL_RULES, ids=lambda r: r.uid)
    def test_threshold_can_be_satisfied(self, rule: Rule):
        value = self._constant_value(rule)
        if value is None:
            return
        for evaluator in rule.thresholds:
            kind = evaluator.get("type")
            params = [float(p) for p in (evaluator.get("params") or [])]
            satisfied = {
                "gt": lambda: value > params[0],
                "gte": lambda: value >= params[0],
                "lt": lambda: value < params[0],
                "lte": lambda: value <= params[0],
                "eq": lambda: value == params[0],
                "ne": lambda: value != params[0],
                "within_range": lambda: params[0] < value < params[1],
                "outside_range": lambda: value < params[0] or value > params[1],
            }.get(kind)
            if satisfied is None:
                continue
            assert satisfied(), (
                f"{rule.uid} ({rule.title!r}) can never fire.\n"
                f"    expr        : {rule.expr}\n"
                f"    always value: {value:g} (the expression filters, so the "
                f"series exists only at this value)\n"
                f"    threshold   : {kind} {params}\n\n"
                f"core#504 shipped exactly this shape and it was silently dead "
                f"for months while reading as healthy."
            )


class TestScrapeIntervalCoupling:
    """A subquery step that drifts from the scrape interval changes the meaning."""

    @pytest.mark.parametrize("rule", PROMQL_RULES, ids=lambda r: r.uid)
    def test_subquery_step_matches_the_scrape_interval(self, rule: Rule):
        for match in _RANGE_SELECTOR.finditer(rule.expr):
            if not match.group(3):
                continue  # plain range vector, no explicit step
            step = float(match.group(3)) * _UNIT_SECONDS[match.group(4)]
            assert step == GLOBAL_SCRAPE, (
                f"{rule.uid} ({rule.title!r}) uses a subquery step of "
                f"{step:g}s while `prometheus.yml` scrapes every "
                f"{GLOBAL_SCRAPE:g}s.\n"
                f"    expr: {rule.expr}\n\n"
                f"The sample count per window — and therefore what the "
                f"threshold means — is derived from this step. If the scrape "
                f"interval was retuned, this rule's threshold silently changed "
                f"meaning; if the step was mistyped, it always was wrong."
            )
