"""Every image build must pull its base image (core#1313).

THE DEFECT THIS GUARDS
----------------------
``datanika/Dockerfile`` starts ``FROM python:3.12-slim`` -- a FLOATING tag. Docker does not
re-fetch a tag it already has, so without an explicit pull every build uses whatever copy of
that tag is in the local (or gha) cache. Because the base never changes, no layer above it is
ever invalidated, and ``RUN apt-get update && apt-get install ...`` is served from cache
indefinitely.

**A REBUILD IS NOT A REFRESH.** Measured on the production box 2026-09-12:

    python:3.12-slim cached on the box   created 2026-07-14   bottom layer f2ec4de8...
    python:3.12-slim current upstream    created 2026-09-01   bottom layer 411a8667...

The serving image reported ``Created`` as the previous night and carried an apt layer dated
"7 weeks ago". Seven weeks of OS package updates had never been applied to anything shipped.

WHY A TEST AND NOT A COMMENT
----------------------------
The failure is silent and directional: it can only ever ship *older* packages, and the build
logs look identical either way. Nothing turns red -- except ``image-cve``, which did turn red,
continuously, and was read as routine because it is non-required (twice, by the author of this
test, before promoting). A guard that cannot be read habitually is the point.

WHAT THIS DOES NOT BUY
----------------------
Freshness, not reproducibility. Two builds a week apart still differ and nothing records which
base either used. Pinning by digest is the stronger fix; it needs a deliberate bump process
that does not exist yet. If that process ever lands, this test should be replaced by one
asserting the digest pin -- not deleted.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

# Workflows that build an image. A new one must be added here deliberately -- the same
# reasoning as install-server-scripts.sh's explicit list: building an image is a deliberate
# act, and a glob would make coverage a side effect of adding a file.
BUILDING_WORKFLOWS = ("ci.yml", "build-push-image.yml")


def _steps_using_build_push_action(workflow: Path) -> list[tuple[str, dict]]:
    """Every ``docker/build-push-action`` step in a workflow, with its job name."""
    doc = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    found: list[tuple[str, dict]] = []
    for job_name, job in (doc.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            uses = str(step.get("uses") or "")
            if uses.startswith("docker/build-push-action"):
                found.append((f"{workflow.name}:{job_name}", step))
    return found


def test_the_parser_actually_found_build_steps() -> None:
    """Anti-vacuity: an empty parse would make every assertion below pass silently.

    This is the shape that has bitten this repo repeatedly -- a check whose input is empty
    reports success. Assert the population before asserting anything about it.
    """
    total = 0
    for name in BUILDING_WORKFLOWS:
        total += len(_steps_using_build_push_action(WORKFLOW_DIR / name))
    assert total >= 4, (
        f"found only {total} docker/build-push-action steps across {BUILDING_WORKFLOWS}. "
        "Either a build moved to a workflow not listed in BUILDING_WORKFLOWS, or the parse "
        "broke -- both of which would make the freshness assertions below vacuous."
    )


def test_every_image_build_pulls_its_base() -> None:
    """No ``docker/build-push-action`` step may build against an unrefreshed base."""
    missing: list[str] = []
    for name in BUILDING_WORKFLOWS:
        for where, step in _steps_using_build_push_action(WORKFLOW_DIR / name):
            with_block = step.get("with") or {}
            # YAML parses an unquoted `true` to a bool; accept the string form too, since
            # `pull: "true"` is equally valid to the action.
            value = with_block.get("pull")
            if value is not True and str(value).lower() != "true":
                missing.append(f"{where} (step: {step.get('name', '<unnamed>')})")
    assert not missing, (
        "these image builds do not pull their base image, so they build against whatever "
        f"copy of the floating tag is cached: {missing}. `FROM python:3.12-slim` never "
        "changes on its own -- without `pull: true` the apt layer above it is served from "
        "cache indefinitely and the image ships OS packages frozen at the date the cache "
        "was first populated (core#1313: seven weeks, in production)."
    )


def test_the_cache_source_pulls_too() -> None:
    """The build carrying ``cache-to`` must pull, because it seeds everyone else's cache.

    ``build-push-image.yml`` populates the gha cache with ``mode=max``; the three builds in
    ``ci.yml`` read it. A stale base there propagates to all of them, and none can refresh it
    on its own -- so this one is not merely an instance of the rule above, it is the origin.
    """
    seeds = [
        (where, step)
        for name in BUILDING_WORKFLOWS
        for where, step in _steps_using_build_push_action(WORKFLOW_DIR / name)
        if "cache-to" in (step.get("with") or {})
    ]
    assert seeds, (
        "no build declares `cache-to`, so this test no longer identifies the cache SOURCE. "
        "If caching moved, re-derive which build seeds the others before deleting this."
    )
    for where, step in seeds:
        value = (step.get("with") or {}).get("pull")
        assert value is True or str(value).lower() == "true", (
            f"{where} writes the shared gha cache but does not pull its base. Every build "
            "reading that cache inherits the staleness and cannot fix it locally."
        )


def test_the_production_deploy_pulls_its_base() -> None:
    """The deploy builds on the box with its own local cache -- the one that actually served.

    Asserted against the raw text rather than the parsed YAML because the command is embedded
    in a shell string inside a `run:` block, where structure gives no help.
    """
    text = (WORKFLOW_DIR / "deploy-pointer.yml").read_text(encoding="utf-8")
    builds = re.findall(r"docker compose build[^'\"\n]*", text)
    assert builds, (
        "found no `docker compose build` in deploy-pointer.yml. If the deploy stopped "
        "building on the box this test must be rewritten, not removed -- the question "
        "'does the thing that serves production refresh its base' still needs an answer."
    )
    unpulled = [b for b in builds if "--pull" not in b]
    assert not unpulled, (
        f"the production deploy builds without --pull: {unpulled}. The box's cached copy of "
        "python:3.12-slim dated 2026-07-14 against a 2026-09-01 upstream, so the image that "
        "served production carried an apt layer seven weeks old while reporting a Created "
        "timestamp from the night before."
    )
