"""`celery-task-failures` must fire on the task exceptions production actually has (core#1356), and
must not fire on a user's failed run (core#1352).

The rule shipped as `sum(rate(celery_task_failed_total[15m])) > 0.1` and could not fire, for two
independent reasons, both measured on production on 2026-09-15:

1. THE NUMBER. `> 0.1` per second over 15 minutes means more than 90 failures. Production runs
   40-48 Celery tasks a DAY (14 days of `increase()`), so the threshold was out of reach.

2. THE SHAPE. celery-exporter 0.12.2 keeps a 0-valued failed series per (hostname, name,
   queue_name) with `exception=""`, and labels a real failure `exception="<Class>"`
   (`src/exporter.py:293-300` in the image). So the first failure of a kind lands in a NEW series
   that starts at 1, and `rate()` or `increase()` on the raw counter never counts a series' first
   sample. `hostname` is the worker's container id and changes on every deploy, so at our volume
   nearly every failure is the first of its kind: no threshold on the raw form could fire.
   Production shows the same thing on the success counter: over the hour after a deploy the raw
   form read 1.00 and aggregate-then-increase read 2.03.

Then core#1352 made a run task raise `RunFailedError` when its run ended FAILED. Counted, that pages
on every failed user run, so since 2026-09-16 the rule EXCLUDES that class, by the label the
exporter derives from it: `^(\\w+)\\(` on the exception's repr (`src/exporter.py:433` and
`:443-447` in the image). For the real class that reads 'RunFailedError' -- measured on the image,
with the event field built as Celery 5.6.2 builds it (`celery/worker/request.py:650-652`). Nothing
else connects the selector to the class, so the class is read from
`datanika/tasks/run_outcome.py`'s source -- parsed, not imported -- and a rename fails here instead
of quietly paging on failed runs again.

Two layers:

* `shape_problems` -- static checks on the rule as provisioned. They run everywhere, CI
  included, and `TestTheCheckCanFail` shows each one failing on a one-property mutation of the
  shipping rule, or on a rule as it stood before.
* `test_promtool_*` -- behaviour, from promtool at production's Prometheus version, against
  series that model the exporter's labelling, with the rule's OWN expression substituted in. Two
  controls must fail the same harness, each in its own direction: the expression that shipped
  first misses a first failure, and the expression that counts failed runs fires where silence is
  required. It needs docker and pulls an image, so it is OPT-IN (`DATANIKA_PROMTOOL=1`) rather than
  a cost on every department's pre-push.
"""

from __future__ import annotations

import ast
import copy
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
ALERTS = ROOT / "monitoring" / "grafana" / "provisioning" / "alerting" / "alerts.yml"
RUN_OUTCOME = ROOT / "datanika" / "tasks" / "run_outcome.py"
UID = "celery-task-failures"

# Production's version, read from /api/v1/status/buildinfo on 2026-09-15 and again on 2026-09-16.
# docker-compose.yml pins `prom/prometheus:latest`, so this is a measurement, not something the
# repository can tell you.
PROMETHEUS_VERSION = "3.13.1"

# The rule as it was provisioned until core#1356: the negative control for a first failure.
SHIPPED_BEFORE = {
    "uid": UID,
    "for": "5m",
    "data": [
        {
            "refId": "A",
            "relativeTimeRange": {"from": 60, "to": 0},
            "datasourceUid": "PBFA97CFB590B2093",
            "model": {"expr": "sum(rate(celery_task_failed_total[15m])) > 0.1", "refId": "A"},
        }
    ],
    "annotations": {
        "summary": "High Celery task failure rate",
        "description": "More than 0.1 task failures per second over the last 15 minutes.",
    },
}

# The rule as core#1356 first fixed it (on `dev` 2026-09-15, never deployed): right about a first
# failure, and a page on every failed user run once core#1352 made those raise. The negative control
# for silence.
COUNTS_FAILED_RUNS = "increase(sum(celery_task_failed_total)[15m:15s]) > 0.5"

# celery-exporter 0.12.2 derives a failure's `exception` label from the task-failed event's
# `exception` field, which Celery fills with `safe_repr(exception)`, using this pattern, and falls
# back to "UnknownException" (src/exporter.py:433 and :443-447 in the image).
_EXPORTER_EXCEPTION_PATTERN = re.compile(r"^(\w+)\(")

_RAW_RANGE = re.compile(r"\b(?:rate|irate|increase)\(\s*celery_task_failed_total\b")
_AGGREGATE_THEN_INCREASE = re.compile(
    r"\bincrease\(\s*sum\(\s*celery_task_failed_total\s*(\{[^}]*\})?\s*\)"
    r"\s*\[(\d+)([smh]):\d+[smh]\]\s*\)"
)
_MATCHER = re.compile(r'\s*(\w+)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"\s*')
_TRAILING_COMPARISON = re.compile(r"(>=|>)\s*(bool\s+)?(\d+(?:\.\d+)?)\s*$")
_UNIT = {"s": 1, "m": 60, "h": 3600}


def _seconds(text: object) -> float:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smh]?)", str(text).strip())
    if not match:
        raise ValueError(f"unparseable duration {text!r}")
    return float(match.group(1)) * _UNIT[match.group(2) or "s"]


def _load_rule() -> dict:
    doc = yaml.safe_load(ALERTS.read_text(encoding="utf-8"))
    found = [r for g in doc.get("groups", []) for r in g.get("rules", []) if r.get("uid") == UID]
    assert len(found) == 1, f"expected exactly one {UID} rule in {ALERTS}, found {len(found)}"
    return found[0]


def _expr(rule: dict) -> str:
    exprs = [n["model"]["expr"] for n in rule.get("data", []) if (n.get("model") or {}).get("expr")]
    assert len(exprs) == 1, f"{UID}: expected one PromQL query node, found {len(exprs)}"
    return exprs[0]


def exporter_exception_label(event_exception_field: str) -> str:
    """The `exception` label celery-exporter 0.12.2 gives a failure with this event field."""
    match = _EXPORTER_EXCEPTION_PATTERN.match(event_exception_field)
    return match.group(1) if match else "UnknownException"


def raised_run_failure_classes(source: str) -> set[str]:
    """The classes `raise_if_run_failed` raises, read from the module's source."""
    helpers = [
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "raise_if_run_failed"
    ]
    assert len(helpers) == 1, f"expected one raise_if_run_failed, found {len(helpers)}"
    return {
        node.exc.func.id
        for node in ast.walk(helpers[0])
        if isinstance(node, ast.Raise)
        and isinstance(node.exc, ast.Call)
        and isinstance(node.exc.func, ast.Name)
    }


def run_failure_label(source: str | None = None) -> str:
    """The `exception` label a failed run's task failure carries on production."""
    if source is None:
        source = RUN_OUTCOME.read_text(encoding="utf-8")
    classes = raised_run_failure_classes(source)
    assert len(classes) == 1, f"raise_if_run_failed should raise one class, found {sorted(classes)}"
    (name,) = classes
    # The event field is the exception's repr. The message never reaches the label.
    return exporter_exception_label(f"{name}('upload run 1 ended FAILED')")


def _selector_problems(selector: str | None, run_failed: str) -> list[str]:
    exclusion = f'exception!="{run_failed}"'
    if not selector:
        return [
            f"the selector does not exclude {exclusion}, so every failed user run fires this "
            "(core#1352)"
        ]
    matchers = []
    for part in (p for p in selector.strip()[1:-1].split(",") if p.strip()):
        match = _MATCHER.fullmatch(part)
        if not match:
            return [f"unparseable matcher {part!r} in {selector}"]
        matchers.append(match.groups())
    if len(matchers) != 1:
        return [
            f"the selector must be exactly {{{exclusion}}}: any other matcher changes which "
            "zero-valued series keep the sum alive, and only the promtool harness can say whether "
            "a first failure still fires"
        ]
    if matchers[0] != ("exception", "!=", run_failed):
        return [
            f"the selector is {selector}, but a failed run is labelled {run_failed!r} (the class "
            f"raise_if_run_failed raises), so it must read {{{exclusion}}} (core#1352)"
        ]
    return []


def shape_problems(rule: dict, run_failed: str | None = None) -> list[str]:
    """Every way `rule` misses one task exception of any kind, or fires on a failed run.

    Empty is healthy. `run_failed` is the label a failed run carries; by default it is derived from
    the class `raise_if_run_failed` raises.
    """
    run_failed = run_failed or run_failure_label()
    expr = _expr(rule)
    problems: list[str] = []
    if _RAW_RANGE.search(expr):
        problems.append(
            "rate()/increase() is applied to the raw counter: the first failure of a kind lands in "
            "a new series born at 1, which that form never counts"
        )
    window = _AGGREGATE_THEN_INCREASE.search(expr)
    if not window:
        problems.append("the expression does not aggregate before increase()")
    else:
        # Only in this form is the compared number a COUNT of failures. A rate's threshold is per
        # second, so "0.1 < 1" would wrongly read as "fires on one failure" -- which is exactly
        # how the old rule looked reasonable.
        comparison = _TRAILING_COMPARISON.search(expr)
        if not comparison:
            problems.append("no trailing > comparison to read the threshold from")
        else:
            operator, _, number = comparison.groups()
            value = float(number)
            if not (value < 1 if operator == ">" else value <= 1):
                problems.append(f"threshold {operator} {number} does not fire on ONE failure")
        window_seconds = float(window.group(2)) * _UNIT[window.group(3)]
        if _seconds(rule.get("for", "0s")) >= window_seconds:
            problems.append(
                f"for: {rule.get('for')} outlasts the {window_seconds:g}s window, so one failure "
                "leaves the window before the alert may fire"
            )
        problems.extend(_selector_problems(window.group(1), run_failed))
    prose = " ".join(str(v) for v in (rule.get("annotations") or {}).values())
    if "exception" not in prose.lower() or "core#1352" not in prose or run_failed not in prose:
        problems.append(
            "the annotations must say the rule watches task exceptions, and name the class it "
            f"excludes: {run_failed}, which is how a failed run reports (core#1352)"
        )
    return problems


def _with(*, expr: str | None = None, for_: str | None = None, annotations=None) -> dict:
    """The SHIPPING rule with exactly one property changed."""
    rule = copy.deepcopy(_load_rule())
    if expr is not None:
        for node in rule["data"]:
            if (node.get("model") or {}).get("expr"):
                node["model"]["expr"] = expr
    if for_ is not None:
        rule["for"] = for_
    if annotations is not None:
        rule["annotations"] = annotations
    return rule


def test_the_shipping_rule_fires_on_one_exception_and_not_on_a_failed_run() -> None:
    assert shape_problems(_load_rule()) == []


def test_a_failed_run_is_labelled_by_the_class_the_helper_raises() -> None:
    # The anchor for everything above: 'RunFailedError' is what the exporter image derived for this
    # class on 2026-09-16. If the class is renamed, this and the shipping rule both go red.
    assert run_failure_label() == "RunFailedError"


class TestTheCheckCanFail:
    """Each check, seen failing. A guard nobody has watched refuse is not evidence."""

    def test_it_rejects_the_rule_as_it_shipped(self) -> None:
        problems = shape_problems(SHIPPED_BEFORE)
        assert any("raw counter" in p for p in problems), problems
        assert any("does not aggregate" in p for p in problems), problems
        assert any("core#1352" in p for p in problems), problems

    def test_it_rejects_the_raw_form_even_with_a_threshold_of_one(self) -> None:
        problems = shape_problems(_with(expr="sum(increase(celery_task_failed_total[15m])) > 0.5"))
        assert any("raw counter" in p for p in problems), problems

    def test_it_rejects_an_unreachable_threshold_on_the_right_shape(self) -> None:
        expr = 'increase(sum(celery_task_failed_total{exception!="RunFailedError"})[15m:15s]) > 90'
        problems = shape_problems(_with(expr=expr))
        assert [p for p in problems if "ONE failure" in p], problems
        assert not any("raw counter" in p for p in problems), problems

    def test_it_rejects_a_for_that_outlasts_the_window(self) -> None:
        problems = shape_problems(_with(for_="20m"))
        assert [p for p in problems if "outlasts" in p], problems

    def test_it_rejects_the_rule_that_pages_on_every_failed_run(self) -> None:
        problems = shape_problems(_with(expr=COUNTS_FAILED_RUNS))
        assert [p for p in problems if "every failed user run" in p], problems
        assert not any("raw counter" in p or "does not aggregate" in p for p in problems), problems

    def test_it_rejects_an_exclusion_of_a_class_nothing_raises(self) -> None:
        expr = 'increase(sum(celery_task_failed_total{exception!="RunFailed"})[15m:15s]) > 0.5'
        problems = shape_problems(_with(expr=expr))
        assert [p for p in problems if "raise_if_run_failed raises" in p], problems

    def test_it_rejects_a_selector_that_does_more_than_exclude(self) -> None:
        expr = (
            'increase(sum(celery_task_failed_total{exception!="RunFailedError",name!="x"})'
            "[15m:15s]) > 0.5"
        )
        problems = shape_problems(_with(expr=expr))
        assert [p for p in problems if "must be exactly" in p], problems

    def test_it_rejects_prose_that_does_not_say_what_it_watches(self) -> None:
        problems = shape_problems(
            _with(annotations={"summary": "Celery task failed", "description": "A task failed."})
        )
        assert [p for p in problems if "core#1352" in p], problems

    def test_it_rejects_prose_that_does_not_name_the_excluded_class(self) -> None:
        # The description as it stood on `dev` before this change: true about exceptions, and
        # silent about the exclusion.
        prose = {
            "summary": "A Celery task raised an exception in the last 15 minutes",
            "description": "This watches task exceptions only: a failed pipeline run does not "
            "currently raise one (core#1352).",
        }
        problems = shape_problems(_with(annotations=prose))
        assert [p for p in problems if "name the class it excludes" in p], problems

    def test_a_renamed_class_reddens_the_shipping_rule(self) -> None:
        renamed = RUN_OUTCOME.read_text(encoding="utf-8").replace("RunFailedError", "RunEndedError")
        label = run_failure_label(renamed)
        assert label == "RunEndedError"
        problems = shape_problems(_load_rule(), run_failed=label)
        assert [p for p in problems if "raise_if_run_failed raises" in p], problems
        assert [p for p in problems if "name the class it excludes" in p], problems

    def test_a_helper_raising_two_classes_is_refused(self) -> None:
        source = (
            "def raise_if_run_failed(org_id, run_id, kind):\n"
            "    if org_id:\n"
            "        raise FirstError('x')\n"
            "    raise SecondError('y')\n"
        )
        with pytest.raises(AssertionError, match="one class"):
            run_failure_label(source)

    def test_the_label_falls_back_when_the_exporter_pattern_misses(self) -> None:
        assert exporter_exception_label("TypeError('boom')") == "TypeError"
        assert exporter_exception_label("pkg.mod.RunFailedError('x')") == "UnknownException"


# --------------------------------------------------------------------------------------------
# Behaviour, from promtool itself. Opt-in: DATANIKA_PROMTOOL=1.
# --------------------------------------------------------------------------------------------

# Every assertion is about the expression under test (@EXPR@, in its `> bool` form). Zero-valued
# series carry no exception label, as on production; a task exception carries exception="E" or
# "F"; a failed run carries @RUN_FAILED@, the label derived from the class the helper raises.
_HARNESS = """\
rule_files: [rules.yml]
evaluation_interval: 1m
tests:
  # control: 2 failures a second on an existing series
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '0+30x240'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 20m, exp_samples: [{labels: '{}', value: 1}]}
  # our volume: 3 failures in 10 minutes on a series that already exists
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '1x79 2x20 3x20 4x120'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 32m, exp_samples: [{labels: '{}', value: 1}]}
  # THE CASE: a first failure of a kind, a new series born at 1 at 30m; it clears later
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '_x119 1x120'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 1}]}
      - {expr: '@EXPR@', eval_time: 40m, exp_samples: [{labels: '{}', value: 1}]}
      - {expr: '@EXPR@', eval_time: 50m, exp_samples: [{labels: '{}', value: 0}]}
  # nothing fails
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
  # exporter restart with 5 historic failures: must stay silent
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x79 _x39 0x120'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '5x79 _x160'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
  # exporter restart, then a first failure of a kind at 40m: must fire
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x79 _x39 0x120'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '5x79 _x160'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="F"}'
        values: '_x159 1x80'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 45m, exp_samples: [{labels: '{}', value: 1}]}
  # scrape gaps of 4 and 7.5 minutes (inside and past the 5m lookback), 3 historic failures
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x99 _x15 0x124'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '3x99 _x15 3x124'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x99 _x29 0x110'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '3x99 _x29 3x110'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
  # a deploy: the new worker's zero series appear; the old series persist
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '2x240'
      - series: 'celery_task_failed_total{hostname="w2",name="t"}'
        values: '_x99 0x140'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
  # core#1352: the first failed run of a kind, a new series born at 1: must stay silent
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="@RUN_FAILED@"}'
        values: '_x119 1x120'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
  # eight more failed runs in 10 minutes on an existing series: must stay silent
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="@RUN_FAILED@"}'
        values: '1x79 3x20 6x20 9x120'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 32m, exp_samples: [{labels: '{}', value: 0}]}
  # a deploy, then the new worker's first failed run: must stay silent
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="@RUN_FAILED@"}'
        values: '2x240'
      - series: 'celery_task_failed_total{hostname="w2",name="t"}'
        values: '_x99 0x140'
      - series: 'celery_task_failed_total{hostname="w2",name="t",exception="@RUN_FAILED@"}'
        values: '_x119 1x120'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
  # a first task exception of a kind, in a window that already holds failed runs: must fire
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x240'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="@RUN_FAILED@"}'
        values: '_x99 1x20 2x120'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '_x119 1x120'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 1}]}
  # exporter restart with historic failures of both kinds, neither returning: must stay silent
  - interval: 15s
    input_series:
      - series: 'celery_task_failed_total{hostname="w1",name="t"}'
        values: '0x79 _x39 0x120'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="E"}'
        values: '5x79 _x160'
      - series: 'celery_task_failed_total{hostname="w1",name="t",exception="@RUN_FAILED@"}'
        values: '7x79 _x160'
    promql_expr_test:
      - {expr: '@EXPR@', eval_time: 35m, exp_samples: [{labels: '{}', value: 0}]}
      - {expr: '@EXPR@', eval_time: 45m, exp_samples: [{labels: '{}', value: 0}]}
"""

_NOOP_RULES = (
    "groups:\n  - name: noop\n    rules:\n      - record: harness:noop\n        expr: vector(1)\n"
)

# promtool's report of a failed expectation: what it expected, then what it got.
_EXPECTED_SILENCE_GOT_FIRING = re.compile(r"exp: \{\} 0E\+00\s+got: \{\} 1E\+00")
_EXPECTED_FIRING_GOT_SILENCE = re.compile(r"exp: \{\} 1E\+00\s+got: \{\} 0E\+00")

requires_promtool = pytest.mark.skipif(
    os.environ.get("DATANIKA_PROMTOOL") != "1",
    reason=f"opt-in: DATANIKA_PROMTOOL=1 runs promtool {PROMETHEUS_VERSION} in docker",
)


def _bool_form(expr: str) -> str:
    match = _TRAILING_COMPARISON.search(expr)
    assert match, f"no trailing comparison in {expr!r}"
    operator, _, number = match.groups()
    return f"{expr[: match.start()].rstrip()} {operator} bool {number}"


def _promtool(tmp_path: Path, expr: str) -> subprocess.CompletedProcess[str]:
    if shutil.which("docker") is None:
        pytest.fail("DATANIKA_PROMTOOL=1 was set, but there is no docker to run promtool in")
    harness = _HARNESS.replace("@EXPR@", _bool_form(expr))
    harness = harness.replace("@RUN_FAILED@", run_failure_label())
    (tmp_path / "rules.yml").write_text(_NOOP_RULES, encoding="utf-8")
    (tmp_path / "test.yml").write_text(harness, encoding="utf-8")
    source = str(tmp_path).replace("\\", "/")
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        "--mount",
        f"type=bind,src={source},dst=/t,readonly",
        "-w",
        "/t",
        "--entrypoint",
        "promtool",
        f"prom/prometheus:v{PROMETHEUS_VERSION}",
        "test",
        "rules",
        "test.yml",
    ]
    return subprocess.run(command, capture_output=True, text=True, timeout=600, check=False)


@requires_promtool
def test_promtool_the_shipping_expression_fires_on_what_production_produces(
    tmp_path: Path,
) -> None:
    result = _promtool(tmp_path, _expr(_load_rule()))
    assert result.returncode == 0, result.stdout + result.stderr


@requires_promtool
def test_promtool_control_the_expression_that_shipped_before_fails(tmp_path: Path) -> None:
    result = _promtool(tmp_path, _expr(SHIPPED_BEFORE))
    output = result.stdout + result.stderr
    assert result.returncode != 0, "the harness passed the old expression, so it proves nothing"
    # It must fail by MISSING a failure; firing where silence is required is the other control.
    assert _EXPECTED_FIRING_GOT_SILENCE.search(output), output


@requires_promtool
def test_promtool_control_the_expression_that_counts_failed_runs_fails(tmp_path: Path) -> None:
    result = _promtool(tmp_path, COUNTS_FAILED_RUNS)
    output = result.stdout + result.stderr
    assert result.returncode != 0, "the harness passed an expression that counts failed runs"
    # It must fail by FIRING where silence is required, which only the failed-run cases require.
    assert _EXPECTED_SILENCE_GOT_FIRING.search(output), output
    assert not _EXPECTED_FIRING_GOT_SILENCE.search(output), output
