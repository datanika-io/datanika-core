"""The published image must not carry either repository's git history (core#1014).

`Dockerfile` does `COPY datanika/ .` and `COPY datanika-cloud/ /cloud/`. Both are
unfiltered unless an ignore file applies, and **the repo's own `.dockerignore`
applies to neither build path**: the GHA build roots its context at
`build-context/` (this checkout is `build-context/datanika/`) and the compose
build roots it at the monorepo root. Docker reads `.dockerignore` from the
context root, so it was read by nobody and `.git` shipped.

That matters because `/cloud/` is the **private** `datanika-cloud` tree. A GHCR
package flipped to public would publish that repository's entire history.

🔑 Two independent defects, and the second is the one a text-only fix misses.
Measured with `--no-cache` on a synthetic context of the workflow's exact shape:

    A  ignore at datanika/.dockerignore, pattern `.git`     -> .git PRESENT
    B  ignore at the CONTEXT ROOT,       pattern `.git`     -> .git PRESENT
    C  ignore at the CONTEXT ROOT,       pattern `**/.git`  -> absent
    E  Dockerfile.dockerignore,          pattern `**/.git`  -> absent
    E2 same as E, file renamed away (negative control)      -> .git PRESENT
    F  compose path (`context: ..`),     pattern `**/.git`  -> absent

**B is why this test checks pattern SHAPE and not merely presence.** Ignore
patterns match paths relative to the context root, which is one level *above*
this checkout — so a bare `.git` matches `<context>/.git` and never
`datanika-cloud/.git`. Moving the file without making the pattern recursive
looks like a fix and ships the history anyway.

**E vs E2 is why the fix is `Dockerfile.dockerignore` rather than a relocated
`.dockerignore`:** BuildKit reads an ignore file adjacent to the Dockerfile in
preference to the context root's, and the Dockerfile is inside this checkout on
both paths. One committed file covers both. No `.dockerignore` can be committed
for the compose path at all — its context root is the monorepo root, which is
outside every git repo.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "Dockerfile"
IGNORE = ROOT / "Dockerfile.dockerignore"
WORKFLOW = ROOT / ".github" / "workflows" / "build-push-image.yml"

# The paths each build path presents to Docker, relative to ITS context root.
# GHA:     build-context/{datanika,datanika-cloud}/   (workflow's reshape step)
# compose: {datanika,datanika-cloud}/                 (docker-compose.yml `context: ..`)
CONTEXT_SHAPES = {
    "gha": ["datanika/.git", "datanika-cloud/.git"],
    "compose": ["datanika/.git", "datanika-cloud/.git"],
}

# Things that must survive the ignore file, or the build breaks rather than the
# history leaking. An over-broad pattern is the failure mode on the other side.
MUST_NOT_BE_IGNORED = [
    "datanika/pyproject.toml",
    "datanika/uv.lock",
    "datanika/Dockerfile",
    "datanika/datanika/config.py",
    "datanika/datanika-mcp/pyproject.toml",
    "datanika-cloud/pyproject.toml",
    "datanika-cloud/datanika_cloud/plugin.py",
]


def _patterns() -> list[str]:
    """Effective patterns: comments and blanks are not patterns."""
    return [
        line.strip()
        for line in IGNORE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _matches(pattern: str, path: str) -> bool:
    """Docker's ignore matching, narrowed to the forms we actually use.

    Go's `filepath.Match` is applied per path *segment*, with a leading `**/`
    meaning "at any depth". Deliberately not a general implementation: it exists
    to answer one question — does this pattern reach a NESTED `.git`?
    """
    if pattern.startswith("**/"):
        tail = pattern[3:]
        return any(_matches(tail, seg) for seg in path.split("/"))
    if "/" in pattern:
        return pattern == path
    # No separator: anchored at the context root, i.e. the FIRST segment only.
    # This is the semantics that made arm B fail.
    from fnmatch import fnmatchcase

    return fnmatchcase(path.split("/")[0], pattern)


def test_ignore_file_sits_beside_the_dockerfile_not_at_the_context_root():
    assert IGNORE.is_file(), (
        "Dockerfile.dockerignore is missing. A plain .dockerignore in this "
        "directory is read by no build path — see this module's docstring."
    )
    assert IGNORE.parent == DOCKERFILE.parent, (
        "BuildKit only reads an adjacent ignore file when it sits next to the "
        "Dockerfile it is named for."
    )


@pytest.mark.parametrize("shape", sorted(CONTEXT_SHAPES))
def test_every_git_directory_is_excluded_in_both_context_shapes(shape):
    pats = _patterns()
    for path in CONTEXT_SHAPES[shape]:
        assert any(_matches(p, path) for p in pats), (
            f"No pattern in Dockerfile.dockerignore excludes {path!r} for the "
            f"{shape} build. A bare `.git` does NOT — patterns are relative to "
            f"the context root, which is above this checkout. Use `**/.git`."
        )


def test_a_bare_git_pattern_would_not_satisfy_this_guard():
    """Negative control on the matcher itself.

    Without this, a matcher that returned True for everything would make the
    test above vacuous, and the defect it exists for is precisely a pattern that
    looks right and matches nothing.
    """
    assert not _matches(".git", "datanika-cloud/.git")
    assert _matches("**/.git", "datanika-cloud/.git")
    assert _matches(".git", ".git")


@pytest.mark.parametrize("keep", MUST_NOT_BE_IGNORED)
def test_the_ignore_file_does_not_exclude_something_the_build_needs(keep):
    pats = _patterns()
    hit = [p for p in pats if _matches(p, keep)]
    assert not hit, f"{keep!r} would be excluded from the build context by {hit!r}"


def test_the_dockerfile_asserts_the_outcome_rather_than_trusting_the_ignore_file():
    """The load-bearing check is in the image, not in this repo's text.

    Comment lines are stripped first: a guard that reads raw text is satisfied by
    the comment that explains the flag, which is a documented trap in this repo.
    """
    body = "\n".join(
        line
        for line in DOCKERFILE.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "/cloud/.git" in body, (
        "Dockerfile has no executable assertion naming /cloud/.git. The ignore "
        "file not being read is exactly the failure this must survive."
    )
    assert "/app/.git" in body
    assert re.search(r"exit\s+1", body), "The check must fail the build, not merely print."


def test_the_publishing_workflow_still_builds_the_context_this_guard_assumes():
    """If the reshape step stops producing build-context/, the shapes above are wrong."""
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = wf["jobs"]["build-push"]["steps"]
    reshape = next(s for s in steps if s.get("name") == "Reshape build context")
    run = reshape["run"]
    assert "build-context/datanika" in run and "build-context/datanika-cloud" in run
    build = next(s for s in steps if s.get("name") == "Build & push")
    assert build["with"]["context"] == "build-context"
    assert build["with"]["file"] == "build-context/datanika/Dockerfile"
