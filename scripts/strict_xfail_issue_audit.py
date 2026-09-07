"""A strict xfail must not outlive the issue it pins (core#1025, second half).

``pytest.mark.xfail(strict=True)`` asserts *"this is broken, and CI must fail if it stops being
broken."* If the issue named in its ``reason`` is **closed**, one of the two is wrong: either the
marker should have come out with the fix, or the fix never landed.

core#896 is the case this exists for. It was closed ``COMPLETED`` while four strict xfails named
it, and the commit credited with fixing it touched **only the test file** — 624 insertions, zero
lines of production code. Every other signal agreed it was fine: the issue read CLOSED, CI was
green (a strict xfail that still fails *is* a pass), the ``shipped-to-prod`` label was on it, and
a production metric read as fixed for reasons unrelated to the property.

## Why this is a separate script and a scheduled job

🚨 **It reads the network, so it must never be able to red-light an unrelated PR.** Somebody
closing an issue would turn another department's branch red for a reason that has nothing to do
with their diff. `.github/workflows/strict-xfail-audit.yml` runs it on a schedule and on
``workflow_dispatch`` — never on ``pull_request``.

⚠️ **A scheduled workflow runs the DEFAULT BRANCH's copy of itself** (`WORKFLOW_RULES` §7). So
after this merges to ``dev`` it does nothing until it is promoted to ``master``, and the
verification is ``git show origin/master:.github/workflows/strict-xfail-audit.yml``, not the
merge.

## The census is imported, not reimplemented

``applied_markers()`` lives in ``tests/test_deploy/test_strict_xfail_reasons_name_an_issue.py``
and is deliberately imported rather than copied. A second census would be a second model of the
same rules that agrees with the first exactly where both are wrong — and this check's coverage
*is* the set of markers that census can resolve, so they cannot be allowed to drift.

## Three ways this check could report clean while measuring nothing

Each one is closed by an assertion rather than by care:

1. **An empty census.** A scanner that finds no markers reports no closed issues, which is the
   same output as a healthy tree. ``audit()`` raises ``CouldNotMeasureError`` on an empty census.
2. **A failed GitHub read.** An unreachable API, a rate limit or a permissions failure must not
   read as "not closed". Every unreadable token is collected and raises ``CouldNotMeasureError`` —
   the run is *unmeasured*, never green.
3. **A token pointing at the wrong repository.** ``cloud#N`` lives in the private
   ``datanika-cloud``; resolving it against ``datanika-core`` would 404, and a 404 swallowed as
   "not closed" is failure mode 2 wearing a different hat. The repo is derived from the prefix
   and an unknown prefix is an error.

Usage::

    python scripts/strict_xfail_issue_audit.py            # audit the tree
    python scripts/strict_xfail_issue_audit.py --json     # machine-readable

Exit codes: ``0`` every named issue is open · ``1`` at least one is closed (the finding) ·
``2`` the tree could not be measured. **2 is not a pass.**
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

#: `<prefix>#<number>` -> the repository that prefix names.
REPO_FOR_PREFIX: dict[str, str] = {
    "core": "datanika-io/datanika-core",
    "cloud": "datanika-io/datanika-cloud",
}

_TOKEN = re.compile(r"^(core|cloud)#(\d+)$")


class CouldNotMeasureError(RuntimeError):
    """The tree was not audited. Deliberately not a subclass of anything that reads as a pass."""


@dataclass(frozen=True)
class Finding:
    token: str
    state: str
    state_reason: str | None
    title: str
    markers: tuple[str, ...]

    def render(self) -> str:
        where = "\n".join(f"      {m}" for m in self.markers)
        reason = f" ({self.state_reason})" if self.state_reason else ""
        return (
            f"  {self.token} is {self.state}{reason} — {self.title}\n"
            f"    but these strict xfails still pin it:\n{where}"
        )


#: A reader takes a token and returns `(state, state_reason, title)`. Injected so the audit is
#: testable without the network — the alternative is mocking `subprocess`, which would make the
#: tests assert the shape of a command rather than the behaviour of the check.
Reader = Callable[[str], "tuple[str, str | None, str]"]


def repo_for(token: str) -> str:
    """Resolve a token to its repository. An unknown prefix is an error, never a default.

    Defaulting to core would send every `cloud#N` to a repository that does not contain it, and
    a 404 read as "not closed" is exactly the reassuring-failure shape this file exists to stop.
    """
    m = _TOKEN.match(token)
    if not m:
        raise CouldNotMeasureError(f"{token!r} is not a core#N/cloud#N token")
    prefix = m.group(1)
    if prefix not in REPO_FOR_PREFIX:
        raise CouldNotMeasureError(f"no repository is registered for the {prefix!r} prefix")
    return REPO_FOR_PREFIX[prefix]


def gh_reader(token: str) -> tuple[str, str | None, str]:
    """Read one issue's state through `gh`. Raises rather than returning a reassuring default."""
    number = token.split("#", 1)[1]
    # S603/S607: fixed argv, no shell. `gh` is resolved from PATH deliberately -- it lives
    # at D:/Tools/gh/bin locally and is preinstalled on the runner, and hardcoding either
    # path would break the other.
    proc = subprocess.run(  # noqa: S603
        [  # noqa: S607
            "gh",
            "issue",
            "view",
            number,
            "--repo",
            repo_for(token),
            "--json",
            "state,stateReason,title",
        ],
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        # WORKFLOW_RULES §7: never `text=True` here — the Windows locale codec is cp1251 and
        # would silently mis-decode any non-ASCII title.
        raise CouldNotMeasureError(
            f"could not read {token}: gh exited {proc.returncode}: "
            f"{proc.stderr.decode('utf-8', 'replace').strip()[:200]}"
        )
    try:
        payload = json.loads(proc.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CouldNotMeasureError(f"could not parse the response for {token}: {exc}") from exc
    if not isinstance(payload, dict) or "state" not in payload:
        # A count or a key read off an error body is the `len(json.load(...)) == 5` trap
        # (WORKFLOW_RULES, 2026-09-06). Assert the SHAPE, not merely that parsing succeeded.
        raise CouldNotMeasureError(f"the response for {token} is not an issue payload: {payload!r}")
    return payload["state"], payload.get("stateReason"), payload.get("title", "")


def tokens_to_markers(markers: Iterable) -> dict[str, list[str]]:
    """Invert the census: issue token -> the marker labels pinning it."""
    out: dict[str, list[str]] = {}
    for m in markers:
        for token in sorted(m.tokens):
            out.setdefault(token, []).append(m.label)
    return out


def audit(markers: list, read: Reader) -> list[Finding]:
    """Every issue that is CLOSED while a strict xfail still pins it.

    Raises `CouldNotMeasureError` when the census is empty or any token is unreadable. Both are
    states in which this check knows nothing, and neither may render as an empty finding list —
    an empty list is what a healthy tree returns.
    """
    if not markers:
        raise CouldNotMeasureError(
            "the strict-xfail census is empty. That is the same output a healthy tree produces, "
            "so it is refused rather than reported as clean — fix the scanner first."
        )

    by_token = tokens_to_markers(markers)
    if not by_token:
        raise CouldNotMeasureError(
            f"{len(markers)} strict xfail marker(s) exist and none names an issue. "
            "test_strict_xfail_reasons_name_an_issue.py is the guard for that; this check "
            "cannot audit what it cannot attribute."
        )

    findings: list[Finding] = []
    unreadable: list[str] = []
    for token, labels in sorted(by_token.items()):
        try:
            state, state_reason, title = read(token)
        except CouldNotMeasureError as exc:
            unreadable.append(str(exc))
            continue
        if state.upper() == "CLOSED":
            findings.append(
                Finding(token, state.upper(), state_reason, title, tuple(sorted(labels)))
            )

    if unreadable:
        raise CouldNotMeasureError(
            "could not read "
            f"{len(unreadable)} of {len(by_token)} issue(s); this run measured nothing:\n  "
            + "\n  ".join(unreadable)
        )
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args(argv)

    sys.path.insert(0, str(REPO_ROOT))
    from tests.test_deploy.test_strict_xfail_reasons_name_an_issue import (  # noqa: E402
        applied_markers,
    )

    markers = applied_markers(REPO_ROOT)
    try:
        findings = audit(markers, gh_reader)
    except CouldNotMeasureError as exc:
        print(f"COULD NOT MEASURE: {exc}", file=sys.stderr)
        print("::error::strict-xfail audit measured nothing. This is not a pass.")
        return 2

    by_token = tokens_to_markers(markers)
    if args.json:
        print(
            json.dumps(
                {
                    "markers": len(markers),
                    "issues": sorted(by_token),
                    "closed": [
                        {"token": f.token, "title": f.title, "markers": list(f.markers)}
                        for f in findings
                    ],
                },
                indent=2,
            )
        )
    else:
        print(f"strict xfail markers applied : {len(markers)}")
        print(f"issues they name             : {len(by_token)}  {sorted(by_token)}")
        print(f"closed while still pinned    : {len(findings)}")

    if findings:
        print()
        for f in findings:
            print(f.render())
            print(
                "::error::"
                f"{f.token} is closed but {len(f.markers)} strict xfail(s) still pin it. "
                "Either the fix never landed (core#896) or the marker outlived it."
            )
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
