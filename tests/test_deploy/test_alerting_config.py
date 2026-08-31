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

  ⚠️ This does **not** cover #598's "Also found" item, the `datanika-app` scrape
  target that has been `down` since the blue/green cutover, and #599's claim
  that it would is wrong. Both `app` and `app_b` are real services in
  `docker-compose.yml`; under blue/green exactly one colour runs at a time, so
  the other target failing to resolve is a **runtime** property and the intended
  design — `app-unhealthy` collapses the pair with `max()` for precisely that
  reason. No static check can see it, and one that appeared to would be lying.
* `TestThresholdSatisfiability` — core#504 shipped `gt [0]` against a series
  whose value is always `0`: unsatisfiable, silently dead for months.
* `TestFilterThresholdAgreement` — **added 2026-08-31, core#754.** The check
  above covers the *constant* case only (`== N`, `absent()`); a one-sided
  comparison emits a **range**, and `_constant_value` returns `None` for it,
  which *skips*. A bare `X < 1` returns the metric's own value, so paired with
  this file's near-universal `gt [0]` the rule fires on the open interval
  `(0, 1)` and **never at 0** — and `increase()` over a task that has stopped
  arriving is exactly 0. That is a rule silent during the outage and noisy
  either side of healthy. Found by mutation while writing
  `celery-maintenance-not-firing` for core#704: the naive form was substituted
  for the shipped `< bool 1` and the file still reported 196 passed, while the
  four other mutations run the same way went correctly red. **No live rule has
  this defect** — verified across all 30 PromQL rules, the only `<` in the set
  being the `bool` form. This is the gate that stops the next one.
* `TestScrapeIntervalCoupling` — `[2m:15s]` hardcodes the blackbox scrape
  interval. If `prometheus.yml` retunes that job, the sample count per window
  changes and the threshold silently means something else.
* `TestBlueGreenColourCoverage` — **added 2026-08-30, core#599 gap (b), out of
  core#622.** A label selector that matches `datanika-app` but not
  `datanika-app-b` watches *nothing* for as long as the other colour serves,
  and the colours alternate on every deploy. `container-high-memory` did this
  for five weeks. ⚠️ It sat **inside** the 30 rules core#604's audit examined
  and passed, because that audit classified rules by *debounce shape* and this
  defect is in *series selection* — an orthogonal axis. Query shape (#600/#604)
  and series selection (#615, #616, #622) are the two axes with known
  instances; assume a third exists.

Tightened 2026-08-30 — core#599 gap (a)
---------------------------------------
`_encodes_duration_in_query` used to exempt the staleness form
`time() - <metric> > N`. That exemption meant `container-down` and
`app-container-down`, both `severity: critical`, were green **in both
directions**: they did not appear in the red control run either, so nothing
about `TestBlipArithmetic` discriminated the bug from the fix on them. A
staleness threshold gates *entering* the failing state and says nothing about
blindness to *recovery*, which is the property this file exists to enforce.
Measured against the real pre-core#617 config (`63ef6df~2`), removing it takes
`TestBlipArithmetic` from **1 flagged rule to 3**, with no false positives on
current `dev`.

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
# The two entries that lived here before were core#604's `app-unhealthy`
# violations, deleted when that rule was fixed — which is exactly the handoff
# this dict was designed for: the fix makes them XPASS, `strict=True` fails the
# suite, and the fixer has to come here. Add an entry only for a defect that is
# filed and deliberately not being fixed in the same change; never to get a red
# suite green.
# Empty on purpose, twice over now.
#
# The first pair of entries were core#604's `app-unhealthy` violations, deleted
# when Infra fixed that rule. The third was `("colour", "container-high-memory")`
# for core#622, added while writing `TestBlueGreenColourCoverage` because the
# check and the fix lived in different files (`tests/` vs `monitoring/`) owned by
# different departments, and the alternative was either a red `dev` or holding a
# finished check hostage to another team's queue.
#
# It lasted about an hour. Infra's fix landed on `dev` mid-rebase, the case
# XPASSed, `strict=True` failed the suite, and the entry had to come out — which
# is the entire mechanism, observed live rather than argued for. An entry here
# cannot outlive the defect it documents.
KNOWN_VIOLATIONS: dict[tuple[str, str], str] = {}

_UNIT_SECONDS = {
    "s": 1,
    "sec": 1,
    "secs": 1,
    "second": 1,
    "seconds": 1,
    "m": 60,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
    "h": 3600,
    "hour": 3600,
    "hours": 3600,
    "d": 86400,
    "day": 86400,
    "days": 86400,
    "w": 604800,
    "week": 604800,
    "weeks": 604800,
}

# Aggregations that genuinely accumulate across samples in their range.
_RANGE_AGGREGATIONS = (
    "count_over_time",
    "sum_over_time",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "stddev_over_time",
    "quantile_over_time",
    "increase",
    "rate",
    "irate",
    "delta",
    "idelta",
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

# --- blue/green colour coverage (core#599 gap (b), out of core#622) ----------
# The app runs as a PAIR and the colours alternate on every deploy. A selector
# that names one colour watches NOTHING for as long as the other one serves —
# silently, with the rule still listed, still evaluating, still green.
#
# These are the two literal container/job names. Prometheus label matchers are
# fully anchored, so the check is: compile the matcher, test it against both
# strings, and flag anything that matches exactly one. That is semantic rather
# than a grep for `(-b)?`, so `datanika-.*` passes and `datanika-(app|celery)`
# does not.
#
# The container name and the Prometheus job name happen to be identical per
# colour, so one pair of literals covers both label families.
BLUE = "datanika-app"
GREEN = "datanika-app-b"

# Labels whose values carry a colour. `instance` and `datname` do not.
_COLOUR_LABELS = ("name", "job", "container", "container_name", "pod")

# `foo=~"bar"`, `foo="bar"`, and the negative forms.
_LABEL_MATCHER = re.compile(r'(\w+)\s*(=~|!~|=|!=)\s*"([^"]*)"')


def _matches(operator: str, value: str, candidate: str) -> bool:
    """Evaluate one Prometheus label matcher against a concrete label value.

    Regex matchers in Prometheus are **fully anchored** — `=~"datanika-app"`
    does not match `datanika-app-b`. `re.fullmatch` is that semantic exactly;
    `re.search` would silently make this check pass on the bug it exists to
    catch, which is worth stating because it is a one-character mistake.
    """
    if operator in ("=~", "!~"):
        try:
            hit = re.fullmatch(value, candidate) is not None
        except re.error:  # pragma: no cover - an invalid regex is its own bug
            return False
    else:
        hit = value == candidate
    return not hit if operator in ("!=", "!~") else hit


def _colour_blind_matchers(expr: str) -> list[str]:
    """Matchers on a colour-bearing label that cover exactly ONE colour.

    Covering neither is fine — that is a selector about some other container
    (`datanika-(celery|postgres|redis)`), not a half-blind app selector.
    """
    if not expr:
        return []
    offenders = []
    for label, operator, value in _LABEL_MATCHER.findall(expr):
        if label not in _COLOUR_LABELS:
            continue
        blue = _matches(operator, value, BLUE)
        green = _matches(operator, value, GREEN)
        if blue != green:
            seen = BLUE if blue else GREEN
            missed = GREEN if blue else BLUE
            offenders.append(f'{label}{operator}"{value}"  (matches {seen}, MISSES {missed})')
    return offenders


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
            if self.query_nodes
            else None,
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
    global_interval = _seconds((prom.get("global") or {}).get("scrape_interval"), default=15.0)
    if job_name is None:
        return global_interval
    for job in prom.get("scrape_configs", []):
        if job.get("job_name") == job_name:
            return _seconds(job.get("scrape_interval"), default=global_interval)
    return global_interval


def _encodes_duration_in_query(rule: Rule, scrape_interval: float) -> bool:
    """Does the *query* already demand more than one sample?

    ONE accepted form, narrow on purpose: a range/subquery aggregation whose
    window spans >= 2 samples **and** whose threshold demands >= 2 of them. The
    second half matters: `count_over_time((probe_success == 0)[2m:15s]) > 0`
    passes a naive "uses an aggregation" test while being exactly as
    single-sample triggered as the bare filter it replaced (core#599 comment).

    ⚠️ **The staleness form `time() - <metric> > N` used to be accepted here and
    is NOT any more (core#599 gap (a), removed 2026-08-30).** The old rationale
    was "the metric must have gone unseen for N seconds, which no single scrape
    can produce" — true, and about the wrong half of the mechanism. A staleness
    threshold raises the bar for *entering* the failing state. It does nothing
    about blindness to *recovery*, which is what core#598 actually was:

        - one cadvisor stall of 61s produces ONE sample where
          `time() - container_last_seen > 60`
        - that sample is filtered-in, so it sits in the 60s query window
        - `reduce: last` returns it for 2 evaluations at the 30s group interval
        - `for: 30s` is satisfied — and the box was healthy the whole time

    "No single scrape can produce it" and "no single *sample* can page you" are
    different claims, and only the second is the property this check exists to
    enforce. The exemption meant `container-down` and `app-container-down`
    (both `severity: critical`) were green here **in both directions** — they
    did not appear in the red control run either, so nothing about this check
    discriminated the bug from the fix on them. A check that cannot fail on a
    whole family of rules is the same silent-green defect one layer up.

    Removing it costs no false positives: `backup-stale` is the only remaining
    bare-staleness rule and it carries `for: 30m` against a 90s requirement, so
    it passes on the `for` path. The two container rules were fixed in core#617
    and pass on the aggregation path above.
    """
    expr = rule.expr
    if not expr:
        return False

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


def _evaluate(kind: str | None, value: float, params: list[float]) -> bool | None:
    """Can a Grafana threshold evaluator ever be true for this constant value?

    `None` means "not a comparison this understands" — the caller skips it
    rather than guessing, because a wrong guess here fails a rule that is fine.
    """
    try:
        if kind == "gt":
            return value > params[0]
        if kind == "gte":
            return value >= params[0]
        if kind == "lt":
            return value < params[0]
        if kind == "lte":
            return value <= params[0]
        if kind == "eq":
            return value == params[0]
        if kind == "ne":
            return value != params[0]
        if kind == "within_range":
            return params[0] < value < params[1]
        if kind == "outside_range":
            return value < params[0] or value > params[1]
    except IndexError:
        return None
    return None


# --- filtering comparison vs evaluator (core#754) ---------------------------
#
# A bare PromQL comparison is a FILTER, not a boolean. `X < 1` returns **X's own
# value** wherever the condition holds, so the number that reaches Grafana's
# evaluator is bounded by the comparison rather than equal to it. The pair
# (filter, evaluator) can therefore disagree in two ways, and both read as a
# perfectly healthy rule everywhere else in this file:
#
#   unsatisfiable    no value the filter admits satisfies the evaluator.
#                    `X > 85` paired with `lt [1]` is dead for every metric,
#                    every range, forever. That pairing is one copy-paste away:
#                    `lt [1]` is the evaluator the three `*-down` rules use.
#
#   extreme excluded the evaluator discards the *unbounded* end of the filter's
#                    range — which is the severe end, the reason the rule was
#                    written. `X < 1` with `gt [0]` fires on the open interval
#                    (0, 1) and **never at 0**, and `increase()` over a task
#                    that has stopped arriving is exactly 0. So the rule is
#                    silent during the outage it exists to detect and noisy on
#                    the fractional values either side of healthy.
#
# `X < bool 1` is the fix: `bool` collapses the filter to 1/0, and the evaluator
# then means what it appears to mean. Exempt below for exactly that reason.
#
# Found by mutation while writing `celery-maintenance-not-firing` for core#704 —
# the naive `< 1` was substituted for the shipped `< bool 1` and the whole file
# still reported 196 passed. `_constant_value` recognises `absent(` and `== N`
# and returns None for everything else, and a None *skips*.
#
# Set operators: `A and B` / `A unless B` take their VALUE from the left operand
# — B only decides which series survive. So the range to check is the left
# operand's, reached by descending into it. `A or B` emits a union of two
# ranges, which this does not model, so it is skipped.
#
# 🚨 That descent is load-bearing, and the first version of this check did not
# have it. It skipped any expression containing a set operator, on the reasoning
# that a trailing comparison is not the produced value there — true, and it made
# the check blind to *the exact rule the issue was found on*.
# `celery-maintenance-not-firing` is `(<filter> < bool 1) and on() (<staleness>)`:
# mutating its `< bool 1` to the naive `< 1` left the suite **green, 30 passed**,
# while the same mutation on a rule without an `and` went correctly red. A check
# that cannot see its own motivating defect is this project's signature failure
# wearing a lint's clothes, and only running it against the mutated live config
# — not against synthetics — surfaced it.

_NUMBER = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
_FILTER_COMPARISON = re.compile(rf"(<=|>=|<|>)\s*(bool\s+)?({_NUMBER})\s*$")
_SET_OPERATOR = re.compile(r"\b(?:and|or|unless)\b")

_NEG = float("-inf")
_POS = float("inf")

# (low, high, low_closed, high_closed)
Interval = tuple[float, float, bool, bool]


def _describe(interval: Interval) -> str:
    """`(0, +inf)` / `[93600, +inf)` — so the failure message shows the arithmetic."""
    low, high, low_closed, high_closed = interval
    left = "[" if low_closed and low != _NEG else "("
    right = "]" if high_closed and high != _POS else ")"
    low_text = "-inf" if low == _NEG else f"{low:g}"
    high_text = "+inf" if high == _POS else f"{high:g}"
    return f"{left}{low_text}, {high_text}{right}"


def _contains(interval: Interval, x: float) -> bool:
    low, high, low_closed, high_closed = interval
    if not low <= x <= high:
        return False
    if x == low and not low_closed:
        return False
    return not (x == high and not high_closed)


def _intersects(a: Interval, b: Interval) -> bool:
    """Is there any value both intervals contain?"""
    low = max(a[0], b[0])
    high = min(a[1], b[1])
    if low > high:
        return False
    if low < high:
        return True
    return _contains(a, low) and _contains(b, low)  # they meet at one point


def _unwrap(expr: str) -> str:
    """Strip parentheses that enclose the WHOLE expression, repeatedly.

    `(a + b) * (c + d)` is not wrapped — the first paren closes early — so the
    balance is walked rather than the ends compared.
    """
    expr = expr.strip()
    while expr.startswith("(") and expr.endswith(")"):
        depth = 0
        closes_at = -1
        for index, char in enumerate(expr):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    closes_at = index
                    break
        if closes_at != len(expr) - 1:
            return expr
        expr = expr[1:-1].strip()
    return expr


def _leftmost_set_operator(expr: str) -> tuple[str, str] | None:
    """The first `and`/`or`/`unless` at paren depth 0, with everything left of it."""
    depth = 0
    for index, char in enumerate(expr):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0:
            match = _SET_OPERATOR.match(expr, index)
            if match:
                return match.group(0), expr[:index]
    return None


def _produced_range(expr: str, _depth: int = 0) -> Interval | None:
    """The range of values this expression can emit, when that is knowable.

    `None` means "not a shape this understands" and the caller skips — a wrong
    guess here reds a rule that is fine, which is how a lint gets muted.
    """
    if not expr or _depth > 8:
        return None
    expr = _unwrap(_strip_label_matchers(expr) if _depth == 0 else expr)
    split = _leftmost_set_operator(expr)
    if split:
        operator, left = split
        # `and` / `unless` emit the LEFT operand's value; `or` emits a union.
        return None if operator == "or" else _produced_range(left, _depth + 1)
    match = _FILTER_COMPARISON.search(expr)
    if not match or match.group(2):  # `bool` disables the filter
        return None
    operator, number = match.group(1), float(match.group(3))
    if operator == ">":
        return (number, _POS, False, False)
    if operator == ">=":
        return (number, _POS, True, False)
    if operator == "<":
        return (_NEG, number, False, False)
    return (_NEG, number, False, True)  # "<="


def _evaluator_ranges(kind: str | None, params: list[float]) -> list[Interval] | None:
    """Values for which a Grafana evaluator is true. `None` = not understood."""
    try:
        if kind == "gt":
            return [(params[0], _POS, False, False)]
        if kind == "gte":
            return [(params[0], _POS, True, False)]
        if kind == "lt":
            return [(_NEG, params[0], False, False)]
        if kind == "lte":
            return [(_NEG, params[0], False, True)]
        if kind == "eq":
            return [(params[0], params[0], True, True)]
        if kind == "ne":
            return [(_NEG, params[0], False, False), (params[0], _POS, False, False)]
        if kind == "within_range":
            return [(params[0], params[1], False, False)]
        if kind == "outside_range":
            return [(_NEG, params[0], False, False), (params[1], _POS, False, False)]
    except IndexError:
        return None
    return None


def _reaches_tail(ranges: list[Interval], unbounded_end: float) -> bool:
    """Does any evaluator range extend to the filter's unbounded end?

    That end is the severe one: for `< N` it is -inf (the metric at its floor,
    i.e. the counter that stopped); for `> N` it is +inf (the runaway).
    """
    if unbounded_end == _POS:
        return any(high == _POS for _, high, _, _ in ranges)
    return any(low == _NEG for low, _, _, _ in ranges)


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

    @pytest.mark.parametrize("rule", [_mark("blip", r) for r in FILTERING_RULES])
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
            str(p) for evaluator in rule.thresholds for p in (evaluator.get("params") or [])
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
                or re.search(rf"\b{re.escape(literal)}\s+{re.escape(unit)}\b", query, re.IGNORECASE)
                or re.search(rf"(?<![\w.]){re.escape(literal)}(?![\w.])", query)
                or literal in threshold_params
            )
            if in_query:
                continue

            raise AssertionError(
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


def _walk_routes(routes, parent: dict):
    """Yield (route, parent, inherited) for every route, depth-first."""
    for route in routes or []:
        inherited = {**parent, **{k: v for k, v in route.items() if k != "routes"}}
        yield route, parent, inherited
        yield from _walk_routes(route.get("routes"), inherited)


def _route_violations(policy_doc: dict) -> list[str]:
    """Routes that declare themselves urgent and then inherit slow batching.

    Extracted as a pure function so `TestTheLintCanFail` can drive the shipping
    logic with a deliberately broken policy document rather than a copy of it.
    """
    failures = []
    for policy in policy_doc.get("policies", []):
        root = {k: v for k, v in policy.items() if k != "routes"}
        for route, parent, _ in _walk_routes(policy.get("routes"), root):
            if "group_wait" not in route:
                continue
            if _seconds(route["group_wait"]) >= _seconds(parent.get("group_wait"), default=30.0):
                continue
            if "group_interval" in route:
                continue
            failures.append(
                f"  route {route.get('matchers')} shortens group_wait to "
                f"{route['group_wait']} but inherits group_interval="
                f"{parent.get('group_interval')} from its parent"
            )
    return failures


class TestRouteCompleteness:
    """A route that shortens `group_wait` must not inherit a long `group_interval`."""

    def test_urgent_routes_set_their_own_group_interval(self):
        failures = _route_violations(POLICY_DOC)
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


def _configured_targets(prom: dict) -> set[str]:
    targets: set[str] = set()
    for job in prom.get("scrape_configs", []):
        for static in job.get("static_configs") or []:
            targets.update(static.get("targets") or [])
    return targets


def _orphan_instances(rules: list[Rule], prom: dict) -> list[str]:
    """Alert rules naming a probe `instance` that `prometheus.yml` never scrapes."""
    targets = _configured_targets(prom)
    return [
        f"  {rule.uid}: instance={instance!r}"
        for rule in rules
        for instance in re.findall(r'instance\s*=\s*"([^"]+)"', rule.expr)
        if instance not in targets
    ]


def _orphan_jobs(rules: list[Rule], prom: dict) -> list[str]:
    """Alert rules naming a scrape job that `prometheus.yml` does not define."""
    jobs = {j.get("job_name") for j in prom.get("scrape_configs", [])}
    missing = []
    for rule in rules:
        for op, pattern in re.findall(r'job\s*(=~|=)\s*"([^"]+)"', rule.expr):
            matcher = re.compile(f"^{pattern}$") if op == "=~" else None
            hit = any(matcher.match(j) for j in jobs if j) if matcher else pattern in jobs
            if not hit:
                missing.append(f"  {rule.uid}: job{op}{pattern!r}")
    return missing


def _orphan_scrape_targets(prom: dict, services: set[str]) -> list[str]:
    """Scrape targets naming a host that is not a compose service."""
    orphans = []
    for job in prom.get("scrape_configs", []):
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
    return orphans


class TestReferentialIntegrity:
    """A rule watching something nobody scrapes can never fire."""

    def test_every_probed_instance_is_a_configured_target(self):
        missing = _orphan_instances(ALL_RULES, PROM_DOC)
        assert not missing, (
            "Alert rules reference probe instances that `prometheus.yml` never "
            "scrapes, so they can never fire and look identical to healthy:\n" + "\n".join(missing)
        )

    def test_every_referenced_job_is_a_configured_job(self):
        missing = _orphan_jobs(ALL_RULES, PROM_DOC)
        assert not missing, (
            "Alert rules reference scrape jobs that do not exist in "
            "`prometheus.yml`:\n" + "\n".join(missing)
        )

    def test_every_scrape_target_is_a_real_compose_service(self):
        compose = _load(ROOT / "docker-compose.yml")
        services = set(compose.get("services") or {})
        orphans = _orphan_scrape_targets(PROM_DOC, services)
        assert not orphans, (
            "`prometheus.yml` scrapes hosts that no longer exist in "
            "`docker-compose.yml`. A scrape target naming a service that was "
            "renamed or removed is permanently down, and a permanently-down "
            "target is the noise that makes a real one unremarkable.\n"
            "NB this does NOT cover the `datanika-app` target from #598 'Also "
            "found' — see the module docstring; that one is a runtime property "
            "of blue/green and is by design:\n" + "\n".join(orphans)
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
            satisfied = _evaluate(kind, value, params)
            if satisfied is None:
                continue
            assert satisfied, (
                f"{rule.uid} ({rule.title!r}) can never fire.\n"
                f"    expr        : {rule.expr}\n"
                f"    always value: {value:g} (the expression filters, so the "
                f"series exists only at this value)\n"
                f"    threshold   : {kind} {params}\n\n"
                f"core#504 shipped exactly this shape and it was silently dead "
                f"for months while reading as healthy."
            )


class TestFilterThresholdAgreement:
    """A bare comparison is a filter — the evaluator must agree with its range.

    `TestThresholdSatisfiability` above covers the *constant* case (`== N`,
    `absent()`), where the expression can emit exactly one value. This covers
    the one-sided case, where it emits a **range**, and the range is what the
    evaluator has to be reconciled against. core#754.
    """

    @pytest.mark.parametrize("rule", [_mark("filter-reachable", r) for r in PROMQL_RULES])
    def test_the_evaluator_is_reachable(self, rule: Rule):
        produced = _produced_range(rule.expr)
        if produced is None:
            return
        for evaluator in rule.thresholds:
            kind = evaluator.get("type")
            params = [float(p) for p in (evaluator.get("params") or [])]
            ranges = _evaluator_ranges(kind, params)
            if ranges is None:
                continue
            assert any(_intersects(produced, r) for r in ranges), (
                f"{rule.uid} ({rule.title!r}) can never fire.\n"
                f"    expr      : {rule.expr}\n"
                f"    the filter emits values in {_describe(produced)} — a bare "
                f"comparison returns the metric's OWN value where the condition "
                f"holds, not a 1/0\n"
                f"    evaluator : {kind} {params}, true on "
                f"{' or '.join(_describe(r) for r in ranges)}\n"
                f"    the two ranges do not overlap, so no sample can ever "
                f"reach the threshold.\n\n"
                f"Fix: use `bool` (`X {'<' if produced[0] == _NEG else '>'} bool "
                f"N`) so the expression emits 1/0 and the evaluator means what "
                f"it looks like, or retune the evaluator to the filter's range."
            )

    @pytest.mark.parametrize("rule", [_mark("filter-extreme", r) for r in PROMQL_RULES])
    def test_the_severe_end_of_the_filter_still_fires(self, rule: Rule):
        """The evaluator must not discard the open end of the filter's range.

        This is core#754's own shape, and it is the one that survives every
        other check in this file: `X < 1` with `gt [0]` *is* satisfiable — on
        (0, 1) — so a reachability check alone passes it. It is nonetheless
        silent at 0, and 0 is precisely what `increase()` returns for a task
        that has stopped arriving.
        """
        produced = _produced_range(rule.expr)
        if produced is None:
            return
        unbounded_end = _NEG if produced[0] == _NEG else _POS
        for evaluator in rule.thresholds:
            kind = evaluator.get("type")
            params = [float(p) for p in (evaluator.get("params") or [])]
            ranges = _evaluator_ranges(kind, params)
            if ranges is None:
                continue
            if not any(_intersects(produced, r) for r in ranges):
                continue  # unreachable entirely — the sibling test names that
            severe = "the metric at its floor" if unbounded_end == _NEG else "a runaway value"
            example = "0" if unbounded_end == _NEG else "an arbitrarily large value"
            assert _reaches_tail(ranges, unbounded_end), (
                f"{rule.uid} ({rule.title!r}) is silent in the severe case it "
                f"exists to detect.\n"
                f"    expr      : {rule.expr}\n"
                f"    the filter emits values in {_describe(produced)}, "
                f"unbounded towards {'-inf' if unbounded_end == _NEG else '+inf'} "
                f"({severe})\n"
                f"    evaluator : {kind} {params}, true on "
                f"{' or '.join(_describe(r) for r in ranges)} — which does NOT "
                f"reach that end\n"
                f"    so {example} passes the filter and is then discarded by "
                f"the threshold. The rule fires on the middle of the range and "
                f"goes quiet as the condition gets worse.\n\n"
                f"Fix: `bool`. `X {'<' if unbounded_end == _NEG else '>'} bool N` "
                f"emits 1 when the condition holds and 0 when it does not, and "
                f"`gt [0]` then reads correctly. This is what "
                f"`celery-maintenance-not-firing` does and why."
            )

    def test_the_filter_check_is_looking_at_something(self):
        """Anti-vacuity — `_produced_range` returning None everywhere is green.

        Both tests above skip on `None`, so a regex that stops matching turns
        this whole class into 60 passing no-ops. That is the failure mode this
        file exists to prevent, so it is asserted rather than assumed.
        """
        examined = [r.uid for r in PROMQL_RULES if _produced_range(r.expr) is not None]
        assert len(examined) >= 5, (
            f"only {len(examined)} live rules have a top-level filtering "
            f"comparison ({examined}); `_produced_range` is almost certainly "
            f"parsing nothing and both checks above are vacuous."
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


class TestBlueGreenColourCoverage:
    """A rule that watches one colour watches nothing half the time.

    core#622: `container-high-memory` selects
    `name=~"datanika-(app|celery)"`. Prometheus anchors regex matchers, so that
    does **not** match `datanika-app-b` — the rule watched celery alone for 719
    of the last 720 hours, and nobody noticed because a rule watching nothing
    is indistinguishable from a rule watching something healthy.

    ⚠️ **The core#604 audit could not have caught this, and that is the point.**
    That audit classified all 30 rules by *debounce shape* — how the duration
    requirement is encoded. This defect is in *which series the rule selects*,
    an orthogonal axis, so #622 sat **inside** the set #604 examined and passed.
    Two axes now have known instances: query shape (#600/#604) and series
    selection (#615, #616, #622). Expect a third.

    The check is mechanical and semantic rather than a grep for `(-b)?`: compile
    each matcher and evaluate it against both literal colour names. Matching
    neither is fine (that is a selector about some other container); matching
    exactly one is the bug.
    """

    @pytest.mark.parametrize("rule", [_mark("colour", r) for r in PROMQL_RULES])
    def test_selectors_cover_both_colours(self, rule: Rule):
        offenders = _colour_blind_matchers(rule.expr)
        listed = "".join(f"    {o}\n" for o in offenders)
        assert not offenders, (
            f"{rule.uid} ({rule.title!r}) selects only ONE blue/green colour:\n"
            f"{listed}"
            f"    expr: {rule.expr}\n\n"
            f"The app runs as a pair and the colours ALTERNATE on every deploy "
            f"({BLUE} = blue, {GREEN} = green), so this rule is blind for as "
            f"long as the colour it misses is the one serving. It does not go "
            f"red when that happens — it goes quiet, which reads identically "
            f"to healthy.\n\n"
            f"Fix by making the matcher cover both: `datanika-app(-b)?`, or "
            f"`datanika-(app(-b)?|celery)` for a mixed selector, or "
            f"`datanika-.*` where the rule genuinely applies to every "
            f"container. Prometheus anchors regex matchers, so the optional "
            f"group is required — `datanika-app` alone does NOT match "
            f"`datanika-app-b`."
        )

    def test_the_colour_check_is_looking_at_something(self):
        """Anti-vacuity: this suite passes trivially if nothing is selected.

        Every parametrized check in this file can go green by collecting
        nothing, and two of them already would have. If the label names or the
        expression shape move, this fails instead of quietly passing.
        """
        seen = [r.uid for r in PROMQL_RULES if _LABEL_MATCHER.search(r.expr or "")]
        assert len(seen) >= 5, (
            f"only {len(seen)} rules have any label matcher at all ({seen}); "
            f"the colour check is almost certainly parsing nothing."
        )
        covering = [
            r.uid
            for r in PROMQL_RULES
            if any(
                _matches(op, val, BLUE) or _matches(op, val, GREEN)
                for lbl, op, val in _LABEL_MATCHER.findall(r.expr or "")
                if lbl in _COLOUR_LABELS
            )
        ]
        assert len(covering) >= 3, (
            f"only {len(covering)} rules select an app colour at all "
            f"({covering}). Either the colour names changed (update BLUE/GREEN) "
            f"or matcher parsing broke — both make this check vacuous."
        )


# ---------------------------------------------------------------------------
# Negative controls
# ---------------------------------------------------------------------------
#
# core#599: "Each assertion must be demonstrated failing against a deliberately
# broken copy before it is trusted — this whole family of monitoring bugs is
# 'green that proves nothing', and a lint that has never failed is another
# instance of it."
#
# So every assertion above is exercised here in BOTH directions: broken input
# must raise, and the corrected input must not. These call the shipping
# assertions and the shipping helpers directly — never a reimplementation — so
# a check that is loosened later loses its own negative control with it.
#
# The fixtures are the real 2026-08-29 shapes, read off the config as it stood
# at the parent of the fix commit (`1f9c269^`), not invented ones.


def _synthetic(
    *,
    expr: str = 'probe_success{instance="https://app.datanika.io/healthz"} == 0',
    for_: str = "30s",
    window: int = 60,
    interval: str = "30s",
    threshold: tuple = ("lt", [1]),
    reducer: str = "last",
    annotations: dict | None = None,
    uid: str = "synthetic",
) -> Rule:
    """Build one Rule with the shape Grafana provisioning actually produces."""
    group = {"name": "synthetic-group", "interval": interval}
    raw = {
        "uid": uid,
        "title": f"Synthetic {uid}",
        "for": for_,
        "labels": {"severity": "critical"},
        "annotations": annotations or {},
        "data": [
            {
                "refId": "A",
                "relativeTimeRange": {"from": window, "to": 0},
                "datasourceUid": "PBFA97CFB590B2093",
                "model": {"expr": expr, "refId": "A"},
            },
            {
                "refId": "B",
                "datasourceUid": "__expr__",
                "model": {"type": "reduce", "reducer": reducer, "expression": "A"},
            },
            {
                "refId": "C",
                "datasourceUid": "__expr__",
                "model": {
                    "type": "threshold",
                    "expression": "B",
                    "conditions": [{"evaluator": {"type": threshold[0], "params": threshold[1]}}],
                },
            },
        ],
    }
    return Rule(group, raw)


class TestTheLintCanFail:
    """Each assertion, demonstrated red on broken input and green on the fix."""

    # --- blip arithmetic ---------------------------------------------------

    def test_blip_check_rejects_the_shape_that_paged_four_times(self):
        """The literal pre-#600 `app-external-down`: filter, for: 30s, 60s window."""
        rule = _synthetic()  # the defaults ARE the 2026-08-29 shape
        with pytest.raises(AssertionError, match="debounces nothing"):
            TestBlipArithmetic().test_single_sample_cannot_reach_firing(rule)

    def test_blip_check_accepts_the_600_fix(self):
        """Duration moved into the query, so `for: 0s` is correct there."""
        rule = _synthetic(
            expr=(
                "count_over_time((probe_success"
                '{instance="https://app.datanika.io/healthz"} == 0)[2m:15s])'
            ),
            for_="0s",
            threshold=("gt", [3]),
        )
        TestBlipArithmetic().test_single_sample_cannot_reach_firing(rule)

    def test_blip_check_rejects_an_aggregation_that_still_needs_one_sample(self):
        """The trap a naive "does it aggregate?" check walks straight past.

        `count_over_time(...) > 0` is exactly as single-sample triggered as the
        bare filter it replaced: one failed sample makes the count 1, which is
        > 0. Reading the aggregation alone would call this fixed.
        """
        rule = _synthetic(
            expr=(
                "count_over_time((probe_success"
                '{instance="https://app.datanika.io/healthz"} == 0)[2m:15s])'
            ),
            for_="0s",
            threshold=("gt", [0]),
        )
        with pytest.raises(AssertionError, match="debounces nothing"):
            TestBlipArithmetic().test_single_sample_cannot_reach_firing(rule)

    def test_blip_check_accepts_a_for_long_enough_to_outlast_the_window(self):
        """The other legitimate fix: `for` >= window + group interval."""
        rule = _synthetic(for_="90s")
        TestBlipArithmetic().test_single_sample_cannot_reach_firing(rule)

    def test_blip_check_rejects_a_bare_staleness_comparison(self):
        """core#599 gap (a). This control asserted the OPPOSITE until 2026-08-30.

        The old claim was "`time() - metric > N` cannot be satisfied by any
        single sample", which is true and about the wrong half of the mechanism.
        A staleness threshold raises the bar for *entering* the failing state;
        it says nothing about blindness to *recovery*. One 61s cadvisor stall
        produces one filtered-in sample, `reduce: last` returns it for two
        evaluations at a 30s group interval, and `for: 30s` is satisfied while
        the box was healthy throughout.

        The cost of the old exemption was not a wrong message — it was that
        `container-down` and `app-container-down`, both `severity: critical`,
        were green here **in both directions**. They did not appear in the red
        control run either, so this check discriminated nothing on them.

        The fixture is the real pre-core#617 `container-down`, not an invention.
        """
        rule = _synthetic(
            expr=(
                "time() - max by (name) "
                '(container_last_seen{name=~"datanika-(celery|postgres|redis)"}) > 60'
            ),
            for_="30s",
            threshold=("gt", [0]),
        )
        with pytest.raises(AssertionError, match="debounces nothing"):
            TestBlipArithmetic().test_single_sample_cannot_reach_firing(rule)

    def test_blip_check_accepts_the_617_staleness_fix(self):
        """Wrapping the same staleness expr in a counted subquery is the fix."""
        rule = _synthetic(
            expr=(
                "count_over_time(((time() - max by (name) "
                '(container_last_seen{name=~"datanika-(celery|postgres|redis)"}))'
                " > 60)[2m:15s])"
            ),
            for_="0s",
            threshold=("gt", [3]),
        )
        TestBlipArithmetic().test_single_sample_cannot_reach_firing(rule)

    def test_blip_check_still_accepts_backup_stale(self):
        """Tightening gap (a) must not cost a false positive on the real config.

        `backup-stale` is the only bare-staleness rule left after core#617. It
        is legitimate on the `for` path (`30m` against a 90s requirement), and
        it is also the one rule where the staleness argument genuinely holds:
        `datanika_backup_last_success_timestamp_seconds` only advances when a
        backup succeeds, so the condition cannot self-clear between two
        evaluations the way a liveness heartbeat can. That distinction is not
        mechanically visible in the expression, which is exactly why the check
        must not try to infer it — it defers to `for` instead.
        """
        rule = _synthetic(
            expr="time() - datanika_backup_last_success_timestamp_seconds > 93600",
            for_="30m",
            threshold=("gt", [0]),
        )
        TestBlipArithmetic().test_single_sample_cannot_reach_firing(rule)

    # --- blue/green colour coverage (core#599 gap (b)) ----------------------

    def test_colour_check_rejects_the_622_selector(self):
        """The literal `container-high-memory` matcher, five weeks unnoticed."""
        rule = _synthetic(
            expr='container_memory_usage_bytes{name=~"datanika-(app|celery)"} > 1.5e+9'
        )
        with pytest.raises(AssertionError, match="selects only ONE blue/green colour"):
            TestBlueGreenColourCoverage().test_selectors_cover_both_colours(rule)

    def test_colour_check_accepts_the_optional_group(self):
        rule = _synthetic(expr='max(up{job=~"datanika-app(-b)?"}) == 0')
        TestBlueGreenColourCoverage().test_selectors_cover_both_colours(rule)

    def test_colour_check_accepts_a_wildcard(self):
        rule = _synthetic(
            expr='increase(container_start_time_seconds{name=~"datanika-.*"}[1h]) > 3'
        )
        TestBlueGreenColourCoverage().test_selectors_cover_both_colours(rule)

    def test_colour_check_ignores_selectors_about_other_containers(self):
        """Matching NEITHER colour is not a violation — `container-down` is fine."""
        rule = _synthetic(
            expr=(
                "time() - max by (name) "
                '(container_last_seen{name=~"datanika-(celery|postgres|redis)"}) > 60'
            )
        )
        TestBlueGreenColourCoverage().test_selectors_cover_both_colours(rule)

    def test_colour_check_ignores_labels_that_carry_no_colour(self):
        """`datname`/`instance` are not colour-bearing; flagging them is noise."""
        rule = _synthetic(
            expr='rate(pg_stat_statements_seconds_total{datname="datanika"}[5m]) > 0.5'
        )
        TestBlueGreenColourCoverage().test_selectors_cover_both_colours(rule)

    def test_colour_check_catches_an_exact_match_not_just_a_regex(self):
        """`job="datanika-app"` is the same defect written without a regex."""
        rule = _synthetic(expr='up{job="datanika-app"} == 0')
        with pytest.raises(AssertionError, match="MISSES datanika-app-b"):
            TestBlueGreenColourCoverage().test_selectors_cover_both_colours(rule)

    def test_colour_matcher_anchors_like_prometheus_does(self):
        """`re.search` here would pass the bug through; `fullmatch` is the point.

        Prometheus anchors regex label matchers. One character of difference in
        this helper (`search` for `fullmatch`) makes the whole check green on
        core#622, so it is asserted directly rather than only through the rules.
        """
        assert _matches("=~", "datanika-app", BLUE)
        assert not _matches("=~", "datanika-app", GREEN), (
            "unanchored: 'datanika-app' must NOT match 'datanika-app-b'"
        )
        assert _matches("=~", "datanika-app(-b)?", GREEN)
        assert _matches("=~", "datanika-.*", GREEN)
        assert not _matches("=~", "datanika-(app|celery)", GREEN)
        assert _matches("=", "datanika-app", BLUE)
        assert not _matches("=", "datanika-app", GREEN)

    def test_filtering_detection_discriminates(self):
        """If `_is_filtering` collapsed, TestBlipArithmetic would silently empty."""
        assert _is_filtering(_synthetic(expr="probe_success == 0"))
        assert _is_filtering(_synthetic(expr='absent(up{job="datanika-app"})'))
        # A label matcher's `=~` must not be read as a comparison operator.
        assert not _is_filtering(_synthetic(expr='sum(rate(x{job=~"a|b"}[5m]))'))

    # --- annotation / `for` agreement --------------------------------------

    def test_annotation_check_rejects_prose_naming_a_dead_threshold(self):
        """The exact drift that misdirected triage on 2026-08-29."""
        rule = _synthetic(
            for_="30s",
            annotations={
                "description": (
                    "External blackbox probe to https://app.datanika.io/healthz "
                    "has failed continuously for 2 minutes."
                )
            },
        )
        with pytest.raises(AssertionError, match="exists nowhere in the rule"):
            TestAnnotationForAgreement().test_prose_duration_matches_something_real(rule)

    def test_annotation_check_accepts_prose_that_matches_for(self):
        rule = _synthetic(
            for_="2m",
            annotations={"description": "has failed continuously for 2 minutes."},
        )
        TestAnnotationForAgreement().test_prose_duration_matches_something_real(rule)

    def test_annotation_check_accepts_a_duration_that_lives_in_the_query(self):
        """`for: 0s` with the duration in the expression is #600's shape."""
        rule = _synthetic(
            expr="time() - datanika_backup_last_success_timestamp_seconds > 90",
            for_="0s",
            annotations={"description": "No successful backup for 90 seconds."},
        )
        TestAnnotationForAgreement().test_prose_duration_matches_something_real(rule)

    def test_annotation_check_ignores_prose_about_the_query_window(self):
        """Deliberately narrow: "in the last 15 minutes" is not a `for:` claim.

        A duration regex that fired on every time-shaped phrase would make the
        check unusable, and an unusable check gets relaxed away within a week.
        """
        rule = _synthetic(
            for_="30s",
            annotations={"description": "More than 5 uploads failed in the last 15 minutes."},
        )
        TestAnnotationForAgreement().test_prose_duration_matches_something_real(rule)

    # --- route completeness ------------------------------------------------

    def test_route_check_rejects_the_2026_08_29_policy(self):
        broken = {
            "policies": [
                {
                    "group_wait": "30s",
                    "group_interval": "5m",
                    "repeat_interval": "4h",
                    "routes": [
                        {
                            "matchers": ["severity = critical"],
                            "group_wait": "10s",
                            "repeat_interval": "1h",
                        }
                    ],
                }
            ]
        }
        assert _route_violations(broken), (
            "the urgent route inherits group_interval: 5m and the check missed it"
        )

    def test_route_check_accepts_a_route_that_sets_its_own_interval(self):
        fixed = {
            "policies": [
                {
                    "group_wait": "30s",
                    "group_interval": "5m",
                    "routes": [
                        {
                            "matchers": ["severity = critical"],
                            "group_wait": "10s",
                            "group_interval": "1m",
                        }
                    ],
                }
            ]
        }
        assert not _route_violations(fixed)

    def test_route_check_reaches_nested_routes(self):
        """A violation one level down must not be invisible."""
        nested = {
            "policies": [
                {
                    "group_wait": "30s",
                    "group_interval": "5m",
                    "routes": [
                        {
                            "matchers": ["team = infra"],
                            "routes": [
                                {
                                    "matchers": ["severity = critical"],
                                    "group_wait": "5s",
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        assert _route_violations(nested)

    # --- referential integrity ---------------------------------------------

    def test_instance_check_rejects_an_unscraped_instance(self):
        rule = _synthetic(expr='probe_success{instance="https://gone.example/"} == 0')
        prom = {
            "scrape_configs": [
                {
                    "job_name": "blackbox-http",
                    "static_configs": [{"targets": ["https://app.datanika.io/healthz"]}],
                }
            ]
        }
        assert _orphan_instances([rule], prom)

    def test_instance_check_accepts_a_scraped_instance(self):
        rule = _synthetic()
        prom = {
            "scrape_configs": [
                {
                    "job_name": "blackbox-http",
                    "static_configs": [{"targets": ["https://app.datanika.io/healthz"]}],
                }
            ]
        }
        assert not _orphan_instances([rule], prom)

    def test_job_check_rejects_an_undefined_job(self):
        rule = _synthetic(expr='up{job="datanika-app-c"} == 0')
        prom = {"scrape_configs": [{"job_name": "datanika-app"}]}
        assert _orphan_jobs([rule], prom)

    def test_job_check_resolves_a_regex_matcher(self):
        """`job=~"datanika-app(-b)?"` matches real jobs and must not be flagged."""
        rule = _synthetic(expr='max(up{job=~"datanika-app(-b)?"}) == 0')
        prom = {
            "scrape_configs": [
                {"job_name": "datanika-app"},
                {"job_name": "datanika-app-b"},
            ]
        }
        assert not _orphan_jobs([rule], prom)

    def test_scrape_target_check_rejects_a_renamed_service(self):
        prom = {
            "scrape_configs": [
                {
                    "job_name": "cadvisor",
                    "static_configs": [{"targets": ["cadvisor-old:8080"]}],
                }
            ]
        }
        assert _orphan_scrape_targets(prom, {"cadvisor", "app"})

    def test_scrape_target_check_ignores_blackbox_probe_urls(self):
        """A probe URL is a parameter, not a host to resolve against compose."""
        prom = {
            "scrape_configs": [
                {
                    "job_name": "blackbox-http",
                    "static_configs": [{"targets": ["https://app.datanika.io/healthz"]}],
                }
            ]
        }
        assert not _orphan_scrape_targets(prom, set())

    # --- threshold satisfiability ------------------------------------------

    def test_threshold_check_rejects_the_core_504_shape(self):
        """A `== 0` filtered series compared `gt [0]` can never be true."""
        rule = _synthetic(expr="datanika_upload_failures == 0", threshold=("gt", [0]))
        with pytest.raises(AssertionError, match="can never fire"):
            TestThresholdSatisfiability().test_threshold_can_be_satisfied(rule)

    def test_threshold_check_accepts_a_satisfiable_threshold(self):
        rule = _synthetic(expr="probe_success == 0", threshold=("lt", [1]))
        TestThresholdSatisfiability().test_threshold_can_be_satisfied(rule)

    def test_threshold_check_accepts_absent_against_gt_zero(self):
        """`absent()` yields 1 when the metric is missing, so `gt [0]` is right."""
        rule = _synthetic(expr='absent(up{job="datanika-app"})', threshold=("gt", [0]))
        TestThresholdSatisfiability().test_threshold_can_be_satisfied(rule)

    # --- filter/threshold agreement (core#754) -----------------------------
    #
    # The shape that motivated the check is the FIRST one: it is satisfiable,
    # so a reachability check alone passes it, and it is silent at exactly the
    # value the rule was written to catch. Every case below was run against the
    # shipping lint first and all three passed it — that measurement is the
    # issue, and these are the tests that turn it red.

    def test_filter_check_rejects_the_754_shape(self):
        """`< 1` + `gt [0]`: fires on (0, 1), silent at 0 — a stopped counter."""
        rule = _synthetic(
            expr="sum(increase(celery_task_succeeded_total[3h])) < 1",
            threshold=("gt", [0]),
            uid="maintenance-not-firing",
        )
        with pytest.raises(AssertionError, match="silent in the severe case"):
            TestFilterThresholdAgreement().test_the_severe_end_of_the_filter_still_fires(rule)

    def test_filter_check_accepts_the_bool_form_that_shipped(self):
        """`< bool 1` emits 1/0, so `gt [0]` is correct. The live rule's shape."""
        rule = _synthetic(
            expr="sum(increase(celery_task_succeeded_total[3h])) < bool 1",
            threshold=("gt", [0]),
        )
        TestFilterThresholdAgreement().test_the_severe_end_of_the_filter_still_fires(rule)
        TestFilterThresholdAgreement().test_the_evaluator_is_reachable(rule)

    def test_filter_check_rejects_a_strictly_unsatisfiable_pair(self):
        """`< 1` + `gt [1]`: no value below 1 is above 1, for any metric."""
        rule = _synthetic(expr="sum(increase(x[3h])) < 1", threshold=("gt", [1]))
        with pytest.raises(AssertionError, match="can never fire"):
            TestFilterThresholdAgreement().test_the_evaluator_is_reachable(rule)

    def test_filter_check_rejects_the_mirrored_pair(self):
        """`> 85` + `lt [1]` — `lt [1]` is the evaluator the *-down rules use.

        Not in core#754's write-up. It falls out of treating the comparison as
        a range rather than special-casing `<`, and it is the likelier
        copy-paste of the two: the three `*-down` rules are the templates
        anyone reaches for.
        """
        rule = _synthetic(expr="node_cpu_percent > 85", threshold=("lt", [1]))
        with pytest.raises(AssertionError, match="can never fire"):
            TestFilterThresholdAgreement().test_the_evaluator_is_reachable(rule)

    def test_filter_check_rejects_a_ceiling_that_drops_the_runaway(self):
        """`> 85` + `lt [95]`: fires on (85, 95), silent at 100% CPU."""
        rule = _synthetic(expr="node_cpu_percent > 85", threshold=("lt", [95]))
        with pytest.raises(AssertionError, match="silent in the severe case"):
            TestFilterThresholdAgreement().test_the_severe_end_of_the_filter_still_fires(rule)

    def test_filter_check_accepts_every_live_shape_it_examines(self):
        """The negative control that matters: no false positive on real rules.

        A check with no false-positive control gets muted the first time it
        reds someone else's correct config. Asserts non-empty separately, or
        this passes by examining nothing.
        """
        examined = [r for r in PROMQL_RULES if _produced_range(r.expr) is not None]
        assert examined, "no live rule exercised — this control is vacuous"
        for rule in examined:
            TestFilterThresholdAgreement().test_the_evaluator_is_reachable(rule)
            TestFilterThresholdAgreement().test_the_severe_end_of_the_filter_still_fires(rule)

    def test_filter_check_descends_into_the_left_operand_of_and(self):
        """`A and B` emits A's value — so A is the operand that must be checked.

        This is the live shape of `celery-maintenance-not-firing`, and the case
        the first version of this check silently skipped: reading the trailing
        `> 10800` would be wrong, and skipping made the check blind to the very
        defect core#754 is about. Descending is the only reading that is both
        correct and non-vacuous.
        """
        good = "(sum(increase(x[3h])) < bool 1) and on() ((time() - min(y)) > 10800)"
        bad = "(sum(increase(x[3h])) < 1) and on() ((time() - min(y)) > 10800)"
        assert _produced_range(good) is None  # `bool` on the left: not a filter
        assert _produced_range(bad) == (_NEG, 1.0, False, False)  # the naive form
        # `or` unions two ranges, which this does not model — skipped, not guessed.
        assert _produced_range("a > 5 or b < 1") is None
        # and the trailing comparison is never mistaken for the produced value
        assert _produced_range("x > 5 and y < 1") == (5.0, _POS, False, False)

    def test_filter_check_rejects_the_754_shape_behind_an_and(self):
        """The mutation that proved the first implementation blind.

        Live-config mutation, not a synthetic: `celery-maintenance-not-firing`
        with `< bool 1` naively rewritten to `< 1` reported **30 passed** before
        the descent was added.
        """
        rule = _synthetic(
            expr=(
                '(sum(increase(celery_task_succeeded_total{name="datanika.run_maintenance"}'
                "[3h])) < 1) and on() ((time() - min(celery_task_succeeded_created)) > 10800)"
            ),
            threshold=("gt", [0]),
            uid="celery-maintenance-not-firing",
        )
        with pytest.raises(AssertionError, match="silent in the severe case"):
            TestFilterThresholdAgreement().test_the_severe_end_of_the_filter_still_fires(rule)

    def test_unwrap_does_not_strip_parens_that_are_not_wrappers(self):
        """`(a) * (b)` starts and ends with a paren and is not parenthesised."""
        assert _unwrap("(a + b)") == "a + b"
        assert _unwrap("((a + b))") == "a + b"
        assert _unwrap("(a + b) * (c + d)") == "(a + b) * (c + d)"
        assert _unwrap("count_over_time((x == 0)[2m:15s])") == "count_over_time((x == 0)[2m:15s])"
        assert _leftmost_set_operator("(a and b) + c") is None  # nested, not top level
        assert _leftmost_set_operator("a and b") == ("and", "a ")
        assert _leftmost_set_operator("operand_count > 5") is None  # not a word boundary

    def test_filter_range_arithmetic(self):
        """The primitives, since every message above is derived from them."""
        assert _produced_range("x < 1") == (_NEG, 1.0, False, False)
        assert _produced_range("x <= 1") == (_NEG, 1.0, False, True)
        assert _produced_range("x > 85") == (85.0, _POS, False, False)
        assert _produced_range("x >= 85") == (85.0, _POS, True, False)
        assert _produced_range("x < bool 1") is None  # `bool` is not a filter
        assert _produced_range("x == 0") is None  # the constant case, handled above
        assert _produced_range("count_over_time((x == 0)[2m:15s])") is None  # nested
        # scientific notation is how `container-high-memory` writes its threshold
        assert _produced_range('container_memory{name="a"} > 2e+9') == (2e9, _POS, False, False)
        # label matchers must not be read as comparisons
        assert _produced_range('up{job=~"datanika-app(-b)?"}') is None

        assert _intersects((0.0, _POS, False, False), (_NEG, 1.0, False, False))  # (0,1)
        assert not _intersects((1.0, _POS, False, False), (_NEG, 1.0, False, False))  # meet, open
        assert _intersects((1.0, _POS, True, False), (_NEG, 1.0, False, True))  # meet, closed
        assert not _intersects((85.0, _POS, False, False), (_NEG, 1.0, False, False))

        assert _reaches_tail([(_NEG, 1.0, False, False)], _NEG)
        assert not _reaches_tail([(0.0, _POS, False, False)], _NEG)
        assert _reaches_tail([(0.0, _POS, False, False)], _POS)

    # --- subquery / scrape coupling ----------------------------------------

    def test_subquery_check_rejects_a_step_that_drifted_from_the_scrape(self):
        rule = _synthetic(expr="count_over_time((probe_success == 0)[2m:30s])")
        with pytest.raises(AssertionError, match="subquery step"):
            TestScrapeIntervalCoupling().test_subquery_step_matches_the_scrape_interval(rule)

    def test_subquery_check_accepts_the_matching_step(self):
        rule = _synthetic(expr="count_over_time((probe_success == 0)[2m:15s])")
        TestScrapeIntervalCoupling().test_subquery_step_matches_the_scrape_interval(rule)

    # --- the primitives the parse guard rests on ---------------------------

    def test_duration_parsing_covers_the_grafana_forms(self):
        assert _seconds("30s") == 30
        assert _seconds("2m") == 120
        assert _seconds("0s") == 0
        assert _seconds(60) == 60  # `relativeTimeRange.from` is a bare int
        assert _seconds(None, default=7.5) == 7.5
        with pytest.raises(ValueError):
            _seconds("soon")

    def test_scrape_interval_prefers_a_per_job_override(self):
        prom = {
            "global": {"scrape_interval": "15s"},
            "scrape_configs": [{"job_name": "slow", "scrape_interval": "60s"}],
        }
        assert _scrape_interval(prom) == 15
        assert _scrape_interval(prom, "slow") == 60
        assert _scrape_interval(prom, "absent-job") == 15
