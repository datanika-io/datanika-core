"""Playwright 1.59.1 cannot install Chromium on Ubuntu 26.04 (core#1619).

`ubuntu-latest` is that image. The overage soak was pinned to `ubuntu-24.04`
in core#1620. The other two jobs that run `npx playwright install` were still
on `ubuntu-latest`, and both failed there on run 42664288152.
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = (
    ROOT / ".github" / "workflows" / "ci.yml",
    ROOT / ".github" / "workflows" / "staging.yml",
    ROOT / ".github" / "workflows" / "overage-e2e-nightly.yml",
)


def test_every_playwright_browser_install_uses_ubuntu_24_04() -> None:
    pinned: list[str] = []
    offenders: list[str] = []
    for path in WORKFLOWS:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for name, job in document["jobs"].items():
            steps = job.get("steps") or []
            installs = [step for step in steps if "playwright install" in str(step.get("run", ""))]
            if not installs:
                continue
            where = f"{path.name}:{name}"
            pinned.append(where)
            if job.get("runs-on") != "ubuntu-24.04":
                offenders.append(f"{where} runs-on={job.get('runs-on')}")
    assert pinned == [
        "ci.yml:e2e-sso",
        "staging.yml:e2e-staging",
        "overage-e2e-nightly.yml:overage-e2e",
    ]
    assert offenders == []
