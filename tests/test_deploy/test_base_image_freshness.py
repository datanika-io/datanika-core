"""The base image, AND the OS packages built on it, must actually refresh (core#1313).

THE DEFECT THIS GUARDS
----------------------
``datanika/Dockerfile`` started ``FROM python:3.12-slim`` -- a FLOATING tag. Docker does not
re-fetch a tag it already has, so every build used whatever copy of that tag was cached. Because
the base never changed, no layer above it was ever invalidated, and the package step was served
from cache indefinitely.

**A REBUILD IS NOT A REFRESH.** Measured on the production box 2026-09-12:

    python:3.12-slim cached on the box   created 2026-07-14   bottom layer f2ec4de8...
    python:3.12-slim current upstream    created 2026-09-01   bottom layer 411a8667...

The serving image reported ``Created`` as the previous night and carried an apt layer dated
"7 weeks ago".

THREE ATTEMPTS, AND WHY ONLY THE THIRD ONE REFRESHES ANYTHING
------------------------------------------------------------
1. ``pull: true`` on every build (run 34692477424). The base was current; the package layer still
   came from cache.
2. Pinning the base by digest (run 34695877776). The pin named the digest the floating tag ALREADY
   resolved to, so the parent was identical and the package step read ``CACHED`` again. A layer's
   cache key follows the resolved parent's CONTENT, not the ``FROM`` line's text.
   🔴 This docstring said the opposite until 2026-09-15 -- "changing that line's text invalidates
   the instruction and everything after it" -- and so did the Dockerfile.
3. ``ARG APT_REFRESHED_ON`` + ``apt-get upgrade`` (2026-09-15). Both halves are needed, and both
   were measured rather than reasoned:

   - a new build-arg value invalidates the RUN after it (same value -> ``CACHED``, new value ->
     ``EXECUTED``, the positive control in the same run);
   - ``apt-get install <list>`` run fresh fixed only the packages the list pulls in and left the
     base image's own packages upgradable. ``apt-get upgrade`` first left none. Upstream had
     published no newer base, so no digest bump could have delivered those fixes at all.

``pull: true`` and the pin both stay. The pin is what makes the base a reviewed input; pulling
costs nothing. Neither of them refreshes a package on its own, and nothing here says they do.

WHY A TEST AND NOT A COMMENT
----------------------------
The failure is silent and directional: it can only ever ship *older* packages, and the build logs
look identical either way. Nothing turns red -- except ``image-cve``, which did turn red,
continuously, and was read as routine because it is non-required (twice, by the author of the
first version of this test, before promoting). And the comment in the Dockerfile was WRONG for
three days while this suite was green, which is the argument for asserting structure rather than
trusting prose -- and for testing each check against the broken shape it exists to reject.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
DOCKERFILE = REPO_ROOT / "Dockerfile"

# Workflows that build an image. A new one must be added here deliberately -- the same
# reasoning as install-server-scripts.sh's explicit list: building an image is a deliberate
# act, and a glob would make coverage a side effect of adding a file.
BUILDING_WORKFLOWS = ("ci.yml", "build-push-image.yml")

LEVER = "APT_REFRESHED_ON"
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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


def _instructions(text: str) -> list[tuple[str, str]]:
    """Logical Dockerfile instructions as ``(KEYWORD, rest)``.

    Continuation lines are joined and comment lines dropped -- including comment lines INSIDE a
    continuation, which Docker also ignores. Parsing structure rather than grepping the file is
    deliberate: this Dockerfile's comments quote the very commands the checks look for, and a
    whole-file substring match would be satisfied by the comment explaining the fix.
    """
    out: list[tuple[str, str]] = []
    buf = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if stripped.startswith("#") or (not stripped and not buf):
            continue
        if not stripped:
            continue
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        buf += line
        keyword, _, rest = buf.strip().partition(" ")
        out.append((keyword.upper(), rest.strip()))
        buf = ""
    return out


def _stage(instructions: list[tuple[str, str]], alias: str) -> list[tuple[str, str]]:
    """The instructions of the stage named ``alias``, from its FROM up to the next FROM."""
    for i, (keyword, rest) in enumerate(instructions):
        if keyword == "FROM" and re.search(rf"\bAS\s+{re.escape(alias)}\s*$", rest, re.I):
            end = next(
                (j for j in range(i + 1, len(instructions)) if instructions[j][0] == "FROM"),
                len(instructions),
            )
            return instructions[i:end]
    return []


def lever_problems(dockerfile: str) -> list[str]:
    """Every reason the base stage's package layer would NOT refresh. Empty means it will."""
    stage = _stage(_instructions(dockerfile), "base")
    if not stage:
        return ["no `FROM ... AS base` stage was found"]
    package_steps = [
        i for i, (kw, rest) in enumerate(stage) if kw == "RUN" and "apt-get install" in rest
    ]
    if not package_steps:
        return ["the base stage has no RUN step that runs `apt-get install`"]
    first = package_steps[0]
    run = stage[first][1]
    problems: list[str] = []

    arg_at = next(
        (
            i
            for i, (kw, rest) in enumerate(stage)
            if kw == "ARG" and rest.split("=", 1)[0].strip() == LEVER
        ),
        None,
    )
    if arg_at is None:
        problems.append(
            f"no `ARG {LEVER}=<date>` in the base stage, so nothing can re-run the package step "
            "short of editing it -- the defect that kept production's apt layer seven weeks old"
        )
    else:
        if arg_at > first:
            problems.append(
                f"`ARG {LEVER}` is declared AFTER the package step, so a new value cannot "
                "invalidate it"
            )
        rest = stage[arg_at][1]
        default = rest.split("=", 1)[1].strip() if "=" in rest else ""
        if not _ISO_DATE.match(default):
            problems.append(
                f"`ARG {LEVER}` must default to an ISO date so a bump is a readable diff "
                f"(got {default!r})"
            )

    if f"${{{LEVER}}}" not in run and f"${LEVER}" not in run:
        problems.append(
            f"the package step does not print ${LEVER}, so a build log cannot show whether the "
            "refresh actually ran"
        )

    update, upgrade, install = (
        run.find("apt-get update"),
        run.find("apt-get upgrade"),
        run.find("apt-get install"),
    )
    if upgrade == -1:
        problems.append(
            "the package step never runs `apt-get upgrade`, and `install` does not upgrade a "
            "package the base image already ships"
        )
    elif not (update != -1 and update < upgrade < install):
        problems.append(
            "`apt-get upgrade` must run after `apt-get update` and before `apt-get install`"
        )

    if "apt-get -s upgrade" not in run or "exit 1" not in run:
        problems.append(
            "the package step does not refuse upgrades that were kept back, so a fix that needs "
            "a new dependency would ship unapplied with nothing red"
        )
    return problems


# ---------------------------------------------------------------------------------------------
# The base is pulled and pinned -- reproducibility. Kept, and correctly described now.
# ---------------------------------------------------------------------------------------------


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
        f"copy of the base is cached: {missing}. Pulling is not sufficient on its own "
        "(run 34692477424), but a build that does not even pull cannot see a newer base."
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
        "python:3.12-slim dated 2026-07-14 against a 2026-09-01 upstream."
    )


def test_the_base_image_is_pinned_by_digest() -> None:
    """The base must be pinned by digest, not by a floating tag.

    What the pin buys is REPRODUCIBILITY: the base is a reviewed input and a bump is a diff.
    It does NOT refresh anything by itself. 🔴 Corrected 2026-09-15 -- this docstring said a
    digest in the `FROM` line invalidates the layers above it because "the instruction text
    changed". Run 34695877776 pinned the digest the tag already resolved to, and the package
    step read CACHED. Refreshing packages is ``APT_REFRESHED_ON``'s job, tested below.
    """
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    from_lines = [
        line.strip()
        for line in dockerfile.splitlines()
        if line.startswith("FROM ") and not line.startswith("FROM base")
    ]
    assert from_lines, (
        "no FROM lines found in the Dockerfile. This test cannot be vacuously true: if the "
        "Dockerfile moved, re-point REPO_ROOT rather than deleting the assertion."
    )

    # Only external bases need a digest. `FROM variant-${...}` names a stage in this file.
    external = [f for f in from_lines if "@sha256:" not in f and not f.startswith("FROM variant-")]
    assert not external, (
        f"these base images are not pinned by digest: {external}. A bare tag is not re-fetched "
        "by Docker, so the build silently uses whatever copy is cached. To bump: "
        "`docker buildx imagetools inspect <image>` and paste the index digest in."
    )


def test_the_pinned_digest_is_well_formed() -> None:
    """A truncated or hand-typed digest fails the build late and confusingly.

    Asserted separately from the pin itself so a malformed digest is not reported as "not
    pinned", which would send the reader to the wrong fix.
    """
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    digests = re.findall(r"^FROM\s+\S+@sha256:([0-9a-f]*)", dockerfile, re.MULTILINE)
    assert digests, "expected at least one digest-pinned FROM; found none"
    malformed = [d for d in digests if len(d) != 64]
    assert not malformed, (
        f"digest(s) are not 64 hex characters: {[(d[:12] + '...', len(d)) for d in malformed]}. "
        "A truncated digest fails at image resolution with a message about the manifest, which "
        "reads as a registry problem rather than a typo."
    )


# ---------------------------------------------------------------------------------------------
# The package layer has a lever that actually re-runs it -- freshness. core#1313's real fix.
# ---------------------------------------------------------------------------------------------


def test_the_base_stage_and_its_package_step_are_found() -> None:
    """Anti-vacuity for everything below: the parse must reach the thing it grades."""
    stage = _stage(_instructions(DOCKERFILE.read_text(encoding="utf-8")), "base")
    runs = [rest for kw, rest in stage if kw == "RUN" and "apt-get install" in rest]
    assert stage, "no `FROM ... AS base` stage parsed out of the Dockerfile"
    assert len(runs) == 1, (
        f"expected exactly one package step in the base stage, parsed {len(runs)} out of "
        f"{len(stage)} instructions -- the lever checks would be grading the wrong step"
    )


def test_the_package_layer_has_a_working_refresh_lever() -> None:
    problems = lever_problems(DOCKERFILE.read_text(encoding="utf-8"))
    assert not problems, "the package layer will not refresh:\n  - " + "\n  - ".join(problems)


def test_the_refresh_date_is_a_real_date_and_not_in_the_future() -> None:
    """A typo'd or future date still invalidates, but it stops the diff meaning anything."""
    stage = _stage(_instructions(DOCKERFILE.read_text(encoding="utf-8")), "base")
    values = [
        rest.split("=", 1)[1].strip()
        for kw, rest in stage
        if kw == "ARG" and rest.split("=", 1)[0].strip() == LEVER and "=" in rest
    ]
    assert len(values) == 1, f"expected one `ARG {LEVER}=<date>` in the base stage, got {values}"
    refreshed = date.fromisoformat(values[0])
    assert refreshed <= date.today() + timedelta(days=1), (
        f"{LEVER}={refreshed} is in the future -- write the day the refresh was made"
    )


# Each mutation is applied to the REAL Dockerfile, so these prove the checks read its actual
# structure -- not that they agree with a fixture written to satisfy them.
_MUTATIONS = {
    "the 2026-09-12 shape: no lever at all": (
        lambda t: re.sub(rf"^ARG {LEVER}=.*\n", "", t, flags=re.M),
        f"no `ARG {LEVER}",
    ),
    "the lever declared after the package step": (
        lambda t: re.sub(rf"^ARG {LEVER}=.*\n", "", t, flags=re.M).replace(
            "# Install uv", f"ARG {LEVER}=2026-09-15\n# Install uv", 1
        ),
        "declared AFTER the package step",
    ),
    "a lever whose value is not a date": (
        lambda t: re.sub(rf"^(ARG {LEVER}=).*$", r"\1latest", t, flags=re.M),
        "must default to an ISO date",
    ),
    "install only, no upgrade": (
        lambda t: re.sub(
            r"DEBIAN_FRONTEND=noninteractive apt-get upgrade.*?/dev/null && \\\n",
            "",
            t,
            flags=re.S,
        ),
        "never runs `apt-get upgrade`",
    ),
    "no refusal of kept-back upgrades": (
        lambda t: t.replace("apt-get -s upgrade", "true"),
        "does not refuse upgrades that were kept back",
    ),
    "the step no longer prints its refresh date": (
        lambda t: t.replace('"${APT_REFRESHED_ON}"', '"unknown"'),
        f"does not print ${LEVER}",
    ),
}


@pytest.mark.parametrize("name", list(_MUTATIONS))
def test_each_check_rejects_the_shape_it_exists_for(name: str) -> None:
    """Both halves in one test: the real file passes AND the broken shape is rejected."""
    real = DOCKERFILE.read_text(encoding="utf-8")
    mutate, expected = _MUTATIONS[name]
    mutated = mutate(real)
    assert mutated != real, f"mutation {name!r} changed nothing -- it would test nothing"
    assert not lever_problems(real), "the real Dockerfile must pass before a mutant means anything"
    problems = lever_problems(mutated)
    assert any(expected in p for p in problems), (
        f"mutation {name!r} was NOT detected. Problems reported: {problems}"
    )
