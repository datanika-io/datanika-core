"""The disk alert and the deploy's disk check must name the same two numbers (core#727).

Two instruments, one quantity
-----------------------------
`scripts/prune-docker-cache.sh` runs before every prod build. It **fails** the deploy under
a floor of free disk and **warns** under a higher band. Independently, Grafana pages on
free disk. Until core#727 those two disagreed: the alert said `critical` at 80% used —
about 32 GiB free on this 158 GB disk — while the deploy called the same disk merely
"warning" until 20 GiB and did not fail until 5 GiB.

Nothing was broken, and that is the point. Each instrument was internally consistent, so
neither could surface the disagreement; an operator paged at 32 GiB free would find a
deploy step reporting the box as healthy, and the natural conclusion is that the alert is
noisy. That is how a `critical` rule earns a mute.

Derived from both sources, restated from neither
------------------------------------------------
The alert thresholds are parsed out of `alerts.yml`, the floor out of the argument
`deploy-pointer.yml` actually passes, and the warning band out of the script's own
comparison. Change any one of the three and this test fails until the others agree, which
is the property a comment saying "these mirror the deploy step" cannot have.

Units are checked, not assumed: the script divides by 1073741824, so its numbers are
**GiB**, and the alert thresholds are compared as bytes against that base. A GB/GiB mixup
is a silent 7% disagreement — exactly the kind that survives review.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
ALERTS = ROOT / "monitoring" / "grafana" / "provisioning" / "alerting" / "alerts.yml"
PRUNE = ROOT / "scripts" / "prune-docker-cache.sh"
DEPLOY = ROOT / ".github" / "workflows" / "deploy-pointer.yml"

GIB = 1024**3

# `node_filesystem_avail_bytes{...} < 21474836480`
_ALERT_THRESHOLD = re.compile(r"node_filesystem_avail_bytes\{[^}]*\}\s*<\s*(\d+)")
# `if [ "$AVAIL_INT" -lt 20 ]; then` — the warning band, a literal in the script.
_SCRIPT_WARN = re.compile(r'"\$AVAIL_INT"\s+-lt\s+(\d+)\b')
# `bash .../prune-docker-cache.sh 20GB 5` — arg 2 is the floor in GiB.
_DEPLOY_INVOCATION = re.compile(r"prune-docker-cache\.sh\s+(\S+)\s+(\d+)")
# `disk_avail_gb() { ... $1/1073741824 ... }` — proves the script's unit is GiB.
_GIB_DIVISOR = re.compile(r"/\s*1073741824")


def _alert_thresholds(text: str) -> dict[str, int]:
    """uid -> the byte threshold in that rule's expression."""
    doc = yaml.safe_load(text)
    out: dict[str, int] = {}
    for group in doc.get("groups") or []:
        for rule in group.get("rules") or []:
            uid = rule.get("uid", "")
            if not uid.startswith("disk-space"):
                continue
            for node in rule.get("data") or []:
                expr = (node.get("model") or {}).get("expr") or ""
                if match := _ALERT_THRESHOLD.search(expr):
                    out[uid] = int(match.group(1))
    return out


@pytest.fixture(scope="module")
def alert() -> dict[str, int]:
    return _alert_thresholds(ALERTS.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def prune_text() -> str:
    return PRUNE.read_text(encoding="utf-8")


def test_the_parsers_actually_found_something(alert: dict[str, int], prune_text: str) -> None:
    """A regex that silently matches nothing turns every assertion below vacuous."""
    assert set(alert) == {"disk-space-warning", "disk-space-critical"}, alert
    assert all(v > 0 for v in alert.values()), alert
    assert _SCRIPT_WARN.search(prune_text), "warning band not found in prune-docker-cache.sh"
    assert _DEPLOY_INVOCATION.search(DEPLOY.read_text(encoding="utf-8")), (
        "deploy-pointer.yml no longer invokes prune-docker-cache.sh with two arguments"
    )


def test_the_script_measures_in_gibibytes(prune_text: str) -> None:
    """The comparison below is only meaningful if both sides use the same base."""
    assert _GIB_DIVISOR.search(prune_text), (
        "prune-docker-cache.sh no longer divides by 1073741824, so its numbers may be GB "
        "rather than GiB and the alert thresholds would be ~7% off while looking identical"
    )


def test_critical_matches_the_floor_that_aborts_the_deploy(alert: dict[str, int]) -> None:
    floor_gib = int(_DEPLOY_INVOCATION.search(DEPLOY.read_text(encoding="utf-8")).group(2))
    assert alert["disk-space-critical"] == floor_gib * GIB, (
        f"`Disk Space Critical` fires at {alert['disk-space-critical'] / GIB:g} GiB free "
        f"but the deploy aborts at {floor_gib} GiB. The page must mean 'production can no "
        f"longer be deployed', so the two numbers have to be the same one."
    )


def test_warning_matches_the_band_the_deploy_warns_at(
    alert: dict[str, int], prune_text: str
) -> None:
    warn_gib = int(_SCRIPT_WARN.search(prune_text).group(1))
    assert alert["disk-space-warning"] == warn_gib * GIB, (
        f"`Disk Space Low` fires at {alert['disk-space-warning'] / GIB:g} GiB free but the "
        f"deploy warns at {warn_gib} GiB."
    )


def test_the_bands_are_ordered(alert: dict[str, int]) -> None:
    """A critical above its warning fires first and makes the warning unreachable."""
    assert alert["disk-space-critical"] < alert["disk-space-warning"], alert


# ── negative control ─────────────────────────────────────────────────────────────────
# The pre-core#727 rule, verbatim. It is a percentage, so it carries no byte threshold at
# all and the parser must report that rather than silently finding nothing to compare.

_PRE_FIX_ALERTS = """
groups:
  - name: Infra
    rules:
      - uid: disk-space-critical
        title: Disk Space Critical
        data:
          - model:
              expr: '(1 - node_filesystem_avail_bytes{mountpoint="/"}
                / node_filesystem_size_bytes{mountpoint="/"}) * 100 > 80'
"""

_DISAGREEING_ALERTS = """
groups:
  - name: Infra
    rules:
      - uid: disk-space-warning
        data:
          - model:
              expr: 'node_filesystem_avail_bytes{mountpoint="/"} < 21474836480'
      - uid: disk-space-critical
        data:
          - model:
              expr: 'node_filesystem_avail_bytes{mountpoint="/"} < 10737418240'
"""


def test_parser_reports_the_percentage_form_as_uncomparable() -> None:
    assert _alert_thresholds(_PRE_FIX_ALERTS) == {}


def test_the_check_rejects_a_disagreeing_critical() -> None:
    """10 GiB against a 5 GiB deploy floor — plausible, wrong, and caught."""
    thresholds = _alert_thresholds(_DISAGREEING_ALERTS)
    floor_gib = int(_DEPLOY_INVOCATION.search(DEPLOY.read_text(encoding="utf-8")).group(2))
    assert thresholds["disk-space-critical"] != floor_gib * GIB
