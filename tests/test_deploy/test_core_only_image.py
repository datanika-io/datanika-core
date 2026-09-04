"""The core-only image variant, guarded at the source (core#1014).

`SPEC_RELEASE_VERSIONING` tells self-hosters to pin a release image, and none of
them can pull one: `ghcr.io/datanika-io/datanika-core` is private and has to stay
private, because every tag it carries grafts the closed-source `datanika-cloud`
tree in at `/cloud`. The artifact they should get is the AGPL core *without* the
billing plugin.

Docker has no conditional `COPY`, so the edition is a **stage selection**:
`final` descends from `variant-${DATANIKA_IMAGE_EDITION}`, and only
`variant-cloud` copies the tree in. Measured on buildx 0.30 before this was
written, five arms:

    context WITHOUT datanika-cloud/, EDITION=core   -> builds, /cloud absent
    context WITHOUT datanika-cloud/, EDITION=cloud  -> FAILS at the COPY   <- control
    context WITH    datanika-cloud/, EDITION=cloud  -> builds, /cloud present
    context WITH    datanika-cloud/, EDITION=core   -> builds, /cloud ABSENT
    EDITION=<typo>                                  -> FAILS resolving the stage

The fourth arm is the one that matters: BuildKit does not build a stage the
selected target does not descend from, so the cloud tree cannot reach a core
image even from a context that contains it.

WHAT THIS FILE IS ACTUALLY FOR
------------------------------
Not the arms above -- those are a build, and `core-only-image` in `ci.yml` runs
them. This guards the property a build cannot: that **every build-time assertion
stays in a stage both editions reach**. The whole reason the split is one file
with a build arg, rather than a second Dockerfile, is that duplicating the file
is how one copy silently loses a guard. If someone later moves the VCS check or
the `/mcp` import check into `variant-cloud` "because that is where the cloud
tree is", the core-only image stops being checked and every build stays green.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "Dockerfile"
CI = ROOT / ".github" / "workflows" / "ci.yml"
PROBE = ROOT / "scripts" / "probe_built_image.py"

JOB = "core-only-image"

# Every assertion the Dockerfile makes about its own output. Each is identified by a
# string that appears in the RUN itself -- not in a comment, which this module strips.
BUILD_TIME_ASSERTIONS = {
    "VCS history is not in the image": "/cloud/.git",
    "the image is the edition asked for": "DATANIKA_IMAGE_EDITION",
    "uv's download cache is gone": "uv cache clean",
    "the /mcp tool surface imports": "datanika_mcp.server",
}


def _lines() -> list[str]:
    """Dockerfile lines with comments and blanks removed, continuations joined.

    Comments are stripped first and deliberately: this repo has a documented trap
    where a guard reading raw text is satisfied by the comment that explains the
    thing rather than by the thing.
    """
    out: list[str] = []
    pending = ""
    for raw in DOCKERFILE.read_text(encoding="utf-8").splitlines():
        if raw.lstrip().startswith("#") or not raw.strip():
            continue
        pending += raw.rstrip()
        if pending.endswith("\\"):
            pending = pending[:-1] + " "
            continue
        out.append(pending)
        pending = ""
    if pending:
        out.append(pending)
    return out


def _stages() -> dict[str, list[str]]:
    """stage name -> its instruction lines. Pre-FROM lines land under ``<global>``."""
    stages: dict[str, list[str]] = {"<global>": []}
    current = "<global>"
    for line in _lines():
        match = re.match(r"^FROM\s+(\S+)(?:\s+AS\s+(\S+))?\s*$", line, re.IGNORECASE)
        if match:
            current = match.group(2) or match.group(1)
            stages.setdefault(current, [])
            stages[current].append(line)
            continue
        stages[current].append(line)
    return stages


def _parents() -> dict[str, str]:
    """stage name -> the image/stage its FROM names."""
    parents = {}
    for line in _lines():
        match = re.match(r"^FROM\s+(\S+)(?:\s+AS\s+(\S+))?\s*$", line, re.IGNORECASE)
        if match and match.group(2):
            parents[match.group(2)] = match.group(1)
    return parents


def _ancestors(stage: str) -> list[str]:
    """`stage` and every stage it descends from, following FROM upwards."""
    parents = _parents()
    chain = [stage]
    seen = {stage}
    while chain[-1] in parents:
        nxt = parents[chain[-1]]
        # The variant selection is templated; both branches are ancestors of `final`.
        if "${DATANIKA_IMAGE_EDITION}" in nxt:
            return chain + ["variant-core", "variant-cloud", "base"]
        if nxt in seen:
            break
        seen.add(nxt)
        chain.append(nxt)
    return chain


# ---------------------------------------------------------------------------------
# The stage graph
# ---------------------------------------------------------------------------------


def test_the_edition_is_a_build_arg_declared_before_the_first_from():
    """`ARG` before the first `FROM` is the only kind a `FROM` line can read."""
    lines = _lines()
    first_from = next(i for i, ln in enumerate(lines) if ln.upper().startswith("FROM "))
    globals_ = lines[:first_from]
    assert any(re.match(r"^ARG\s+DATANIKA_IMAGE_EDITION\s*=", ln) for ln in globals_), (
        "DATANIKA_IMAGE_EDITION must be declared (with a default) before the first FROM"
    )


def test_the_default_edition_is_cloud_so_production_is_unchanged():
    """`docker-compose.yml` and `deploy-pointer.yml` pass no build arg. If the default
    ever flips, the next deploy builds an image with no billing plugin, every container
    reads healthy, and nothing is metered -- core#772 arriving through a new door."""
    arg = next(ln for ln in _lines() if ln.startswith("ARG DATANIKA_IMAGE_EDITION"))
    assert arg.split("=", 1)[1].strip() == "cloud", (
        f"the default edition must be 'cloud' (production builds it), got: {arg!r}"
    )


def test_both_variant_stages_exist_and_final_selects_between_them():
    stages = _stages()
    assert "variant-core" in stages and "variant-cloud" in stages
    assert "final" in stages
    assert _parents()["final"] == "variant-${DATANIKA_IMAGE_EDITION}", (
        "final must descend from the templated variant, or the build arg selects nothing"
    )


def test_final_is_the_last_stage():
    """`docker build` with no `--target` builds the LAST stage. If a stage is appended
    after `final`, an unqualified build silently produces something else."""
    named = []
    for line in _lines():
        match = re.match(r"^FROM\s+(\S+)(?:\s+AS\s+(\S+))?\s*$", line, re.IGNORECASE)
        if match:
            named.append(match.group(2) or match.group(1))
    assert named, "no FROM lines found — the parser is broken, not the Dockerfile"
    assert named[-1] == "final", f"last stage is {named[-1]!r}, not 'final'"


def test_the_arg_is_redeclared_inside_final():
    """The documented gotcha. A pre-FROM ARG is visible to FROM lines and to nothing
    else; without re-declaring it, `$DATANIKA_IMAGE_EDITION` is empty inside the stage
    and the edition assertion falls through to its error branch on every build."""
    assert any(re.match(r"^ARG\s+DATANIKA_IMAGE_EDITION\s*$", ln) for ln in _stages()["final"]), (
        "final must re-declare `ARG DATANIKA_IMAGE_EDITION` to read it in a RUN"
    )


def test_the_cloud_tree_is_copied_in_exactly_one_stage_and_it_is_variant_cloud():
    hits = {
        name: [ln for ln in body if ln.startswith("COPY") and "datanika-cloud" in ln]
        for name, body in _stages().items()
    }
    carrying = {name: lines for name, lines in hits.items() if lines}
    assert list(carrying) == ["variant-cloud"], (
        "`COPY datanika-cloud/` must appear only in variant-cloud. Anywhere else and "
        f"the core-only build either fails or ships the closed tree. Found in: {carrying}"
    )


def test_the_core_variant_adds_nothing():
    """It must be a bare `FROM base`. Anything here has to be added to the cloud
    variant too, which is the duplication this single-file split exists to avoid."""
    body = [ln for ln in _stages()["variant-core"] if not ln.upper().startswith("FROM ")]
    assert body == [], f"variant-core must add no instructions, found: {body}"


# ---------------------------------------------------------------------------------
# The property a build cannot check: no guard may live in only one edition
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("what,needle", sorted(BUILD_TIME_ASSERTIONS.items()))
def test_every_build_time_assertion_runs_in_both_editions(what, needle):
    """Find the stage each assertion lives in, and require both editions to reach it.

    This is the guard that earns the file. Moving the VCS check into `variant-cloud`
    is a natural-looking edit -- that is where the cloud tree is -- and it would stop
    the core-only image being checked at all while every build stayed green.
    """
    stages = _stages()
    homes = [
        name
        for name, body in stages.items()
        if any(needle in ln for ln in body if ln.upper().startswith("RUN "))
    ]
    assert homes, f"no RUN instruction asserts {what!r} (looked for {needle!r})"
    reachable_from_final = set(_ancestors("final"))
    for home in homes:
        assert home in reachable_from_final, (
            f"the assertion for {what!r} lives in stage {home!r}, which is not on "
            "final's ancestry — so it does not run in either edition"
        )
        assert home not in {"variant-core", "variant-cloud"}, (
            f"the assertion for {what!r} lives in {home!r}, a per-edition stage, so it "
            "runs in ONE edition only. Every guard belongs in `base` or `final`."
        )


def test_the_edition_assertion_can_fail_the_build():
    """A check that only prints is not a check."""
    body = "\n".join(_stages()["final"])
    assert "DATANIKA_IMAGE_EDITION" in body
    assert re.search(r"exit\s+1", body), "the edition check must exit non-zero"
    # Both directions, or it only ever catches one of the two ways to be wrong.
    assert "/cloud/pyproject.toml" in body, "cloud edition must assert the tree IS there"
    assert "-e /cloud" in body, "core edition must assert the tree is NOT there"


# ---------------------------------------------------------------------------------
# The CI job
# ---------------------------------------------------------------------------------


def _job() -> dict:
    workflow = yaml.safe_load(CI.read_text(encoding="utf-8"))
    assert JOB in workflow["jobs"], f"{JOB} is missing from ci.yml"
    return workflow["jobs"][JOB]


def test_the_job_builds_the_core_edition():
    build = next(s for s in _job()["steps"] if "build-push-action" in str(s.get("uses", "")))
    assert "DATANIKA_IMAGE_EDITION=core" in str(build["with"]["build-args"])


def test_the_job_does_not_push_anything():
    """GHCR visibility is per PACKAGE, not per tag, so publishing a core-only image
    means a second package and a one-way visibility decision. Build and verify first;
    someone else decides about publishing, with the artifact in front of them."""
    build = next(s for s in _job()["steps"] if "build-push-action" in str(s.get("uses", "")))
    assert build["with"]["push"] is False, "this job must not publish an image"
    assert build["with"]["load"] is True, "but it must load it, or the probe has nothing"


def test_the_job_checks_out_no_cloud_repo():
    """Two independent properties in one assertion, and both matter.

    The context having no `datanika-cloud/` is what makes this a test of the edition
    split rather than of a flag. And needing no `CLOUD_REPO_TOKEN` is what lets this
    job run on a FORK PR, where `image-probe` and `image-cve` skip every step and
    report green having built nothing.
    """
    steps = _job()["steps"]
    for step in steps:
        with_ = step.get("with") or {}
        assert "datanika-cloud" not in str(with_.get("repository", "")), (
            "the core-only job must not check out the cloud repo"
        )
    assert "CLOUD_REPO_TOKEN" not in yaml.dump(_job()), (
        "needing the cloud token would make this job skip on fork PRs, which is the "
        "hole it exists to cover"
    )


def test_the_job_asserts_its_context_has_no_cloud_tree():
    """A build that would have failed loudly is better than one that quietly picks up
    a tree left over from another step."""
    reshape = next(s for s in _job()["steps"] if "Reshape" in str(s.get("name", "")))
    assert "build-context/datanika-cloud" in reshape["run"]
    assert "exit 1" in reshape["run"]


def test_the_job_probes_with_the_core_edition():
    probe = next(s for s in _job()["steps"] if s.get("id") == "probe")
    assert "--edition core" in probe["run"]
    assert "probe_built_image.py" in probe["run"]


def test_the_probe_step_is_not_quarantined():
    """Non-required and quarantined-green are different things. `continue-on-error`
    here would make the job structurally unable to report anything."""
    for step in _job()["steps"]:
        assert not step.get("continue-on-error"), (
            f"step {step.get('name')!r} carries continue-on-error"
        )


def test_the_probe_supports_and_discriminates_the_edition():
    """The probe must assert BOTH directions. A check that only says 'core has no
    /cloud' passes on an image that is empty, broken, or simply the wrong artifact."""
    source = PROBE.read_text(encoding="utf-8")
    assert '"--edition"' in source
    assert 'choices=("cloud", "core")' in source
    assert 'default="cloud"' in source, "the default must keep image-probe unchanged"
    body = "\n".join(ln for ln in source.splitlines() if not ln.lstrip().startswith("#"))
    assert 'args.edition == "core"' in body
    # The cloud arm is what proves the check can tell the two apart at all.
    assert 'facts["cloud_tree"]' in body and 'facts["cloud_installed"]' in body
