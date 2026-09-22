"""Run the E2E API budget's model test where CI and the pre-push hook will see it (core#1296).

`e2e/fixtures/api-budget.ts` decides when the gating tenant-isolation specs may send a request
without the server's fixed-window rate limiter refusing it. When it decides wrong, a
cross-tenant probe is answered with a 429 **before the tenant check runs**, and the run proves
nothing about the boundary it is named for. It decided wrong about 2.5% of the time (core#1296):
it reset its counter late and forgot the request it had sent during the lag.

`e2e/fixtures/api-budget.test.ts` replays that against a model of
`datanika/services/rate_limit_service.py` and sweeps every burst start in a minute. It is a
`node:test` file rather than a Playwright spec, so it runs in no E2E tier and needs no stack.
This module is what makes it run at all. Nothing else in CI invokes Node on a pull request.

⚠️ **A missing Node FAILS here, it does not skip** (`docs/QA_RULES.md` §11). The skip would be
silent on exactly the machine that cannot check the budget. Node is on `PATH` on the
`ubuntu-24.04` runner (22.23.2 on image 20260907) and on the shared dev host (22.14.0).
`--experimental-strip-types` needs 22.6 or later.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_TEST = REPO_ROOT / "e2e" / "fixtures" / "api-budget.test.ts"
PLAYWRIGHT_CONFIG = REPO_ROOT / "e2e" / "playwright.config.ts"
MIN_NODE = (22, 6)

#: A top-level `node:test` declaration. Counted so a file that silently stops registering tests
#: — a loader change, a module-format flip — reads as a failure rather than as `# pass 0`.
_DECLARED = re.compile(r'^test\("', re.MULTILINE)


def _node() -> str:
    node = shutil.which("node")
    assert node, (
        "node is not on PATH, so the API budget's model test cannot run. Install Node >= "
        f"{MIN_NODE[0]}.{MIN_NODE[1]}; do not skip this — a skip here is silent on exactly the "
        "machine that cannot check the budget (QA_RULES §11)."
    )
    version = subprocess.run(
        [node, "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    m = re.match(r"v(\d+)\.(\d+)", version)
    assert m, f"could not read a Node version from {version!r}"
    found = (int(m.group(1)), int(m.group(2)))
    assert found >= MIN_NODE, (
        f"Node {version} cannot strip TypeScript types; the model test needs "
        f">= {MIN_NODE[0]}.{MIN_NODE[1]} (--experimental-strip-types)."
    )
    return node


def test_the_api_budget_model_test_passes() -> None:
    declared = len(_DECLARED.findall(MODEL_TEST.read_text(encoding="utf-8")))
    assert declared >= 6, f"{MODEL_TEST.name} declares {declared} tests; expected at least 6"

    proc = subprocess.run(
        [
            _node(),
            "--experimental-strip-types",
            "--no-warnings",
            "--test",
            "--test-reporter=tap",
            str(MODEL_TEST),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
        check=False,
    )
    out = proc.stdout
    tail = f"\n--- stdout (tail) ---\n{out[-6000:]}\n--- stderr (tail) ---\n{proc.stderr[-2000:]}"

    passed = re.search(r"^# pass (\d+)$", out, re.MULTILINE)
    failed = re.search(r"^# fail (\d+)$", out, re.MULTILINE)
    assert passed and failed, f"no TAP summary in the model test's output{tail}"
    assert proc.returncode == 0 and int(failed.group(1)) == 0, (
        f"the API budget's model test failed ({failed.group(1)} of {declared}). Read the "
        f"failing case before touching BOUNDARY_SKEW_MS: widening it hides a rejection "
        f"rather than preventing one.{tail}"
    )
    assert int(passed.group(1)) == declared, (
        f"{passed.group(1)} passed of {declared} declared: a test that did not run is not a "
        f"test that passed.{tail}"
    )
    assert "ok 1 - replays the 2026-09-11 flake" in out, f"the replay case did not run{tail}"


def test_the_model_test_is_outside_playwrights_test_dir() -> None:
    """It is not an E2E spec, and it must never be collected as one.

    Inside `testDir` it would enter the GATING tier with no marker at all, which is the one
    thing `docs/QA_RULES.md` §10 forbids a new spec to do.
    """
    config = PLAYWRIGHT_CONFIG.read_text(encoding="utf-8")
    m = re.search(r'testDir:\s*"([^"]+)"', config)
    assert m, "could not find testDir in e2e/playwright.config.ts"
    test_dir = (PLAYWRIGHT_CONFIG.parent / m.group(1)).resolve()
    assert test_dir.is_dir(), f"testDir {test_dir} does not exist"
    assert test_dir not in MODEL_TEST.resolve().parents, (
        f"{MODEL_TEST.relative_to(REPO_ROOT)} is inside Playwright's testDir ({m.group(1)})"
    )
