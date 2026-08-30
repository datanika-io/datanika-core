"""The deploy's docker prune must stay capped, and must never unreference the rollback.

core#666. The prod box builds its own image every deploy, so each deploy leaves the
previous image dangling. Five weeks of that was 270-of-283 images `<none>` and
`docker system df` advertising 53 GB "reclaimable".

Two findings from measuring it on 2026-08-30, both of which this test exists to keep:

**1. The obvious prune reclaims nothing.** `docker image prune` deleted 275 image
records, the Build Cache row grew by ~52 GB, and `df` did not move a byte — total
reclaimed **0 B**. Those layers are BuildKit cache records the dangling images merely
also referenced. The only command that moves the disk is `docker builder prune`, and it
must be **capped**: `-af` empties the cache and makes the next deploy a fully cold build
on a box that compiles lxml/xmlsec from source.

**2. Every shorter variant destroys the blue/green rollback.** The rollback is an
`Exited (137)` container plus the dangling image it references. `container prune`
removes the container, which unreferences the image, which the next image prune then
deletes — leaving no way to roll back without a rebuild. `system prune -a` does the same
and takes staging with it. The box is also shared with co-tenants, so an exited container
is somebody's deliberate state.

Both are one edit away, and every unsafe spelling is shorter than the safe one. That is
exactly the shape that needs a test rather than a comment — this file is the comment that
fails.

Not asserted here: that the prune *worked*. The script checks that on the box, against
the live container set, which is the only place the question can be answered.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-pointer.yml"
PRUNE_SCRIPT = ROOT / "scripts" / "prune-docker-cache.sh"

# Spellings that unreference or delete an image a container still holds. Each is a
# real way to break the rollback, not a stylistic preference.
FORBIDDEN = [
    (r"docker\s+container\s+prune", "container prune unreferences the rollback image"),
    (r"docker\s+system\s+prune", "system prune -a takes the rollback and staging"),
    (r"docker\s+image\s+prune\s+(-\w*a|--all)", "image prune -a takes every untagged image"),
    (r"docker\s+builder\s+prune[^\n|]*(-\w*a\b|--all)", "builder prune -af forces a cold build"),
]


def _workflow_text() -> str:
    return DEPLOY_WORKFLOW.read_text(encoding="utf-8")


def _script_text() -> str:
    return PRUNE_SCRIPT.read_text(encoding="utf-8")


def _code_only(text: str) -> str:
    """Strip `#` comment lines, so the prose explaining a hazard is not read as one."""
    return "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))


def test_prune_script_exists_and_is_referenced_by_the_deploy() -> None:
    """A prune that no deploy step calls is a prune that never runs (core#616's lesson)."""
    assert PRUNE_SCRIPT.is_file(), f"{PRUNE_SCRIPT} is missing"
    assert PRUNE_SCRIPT.name in _workflow_text(), (
        "scripts/prune-docker-cache.sh is not named by any step in deploy-pointer.yml. "
        "Config CD does not apply is config that is not deployed — core#616."
    )


@pytest.mark.parametrize("pattern,why", FORBIDDEN, ids=[w for _, w in FORBIDDEN])
def test_no_rollback_destroying_prune(pattern: str, why: str) -> None:
    """None of the unsafe prune spellings may appear in executable lines."""
    for label, text in (
        ("deploy-pointer.yml", _workflow_text()),
        ("prune-docker-cache.sh", _script_text()),
    ):
        hit = re.search(pattern, _code_only(text))
        assert hit is None, f"{label} contains `{hit.group(0)}` — {why} (core#666)"


def test_builder_prune_is_capped() -> None:
    """The builder prune must carry a storage cap, or it is a cold build every deploy."""
    code = _code_only(_script_text())
    assert re.search(r"docker\s+builder\s+prune", code), (
        "no `docker builder prune` in the script — the image prune alone reclaims 0 B "
        "(measured 2026-08-30), so nothing would bound the disk"
    )
    assert re.search(r"--keep-storage|--max-used-space", code), (
        "`docker builder prune` has no cap. Uncapped it empties the build cache and the "
        "next deploy compiles lxml/xmlsec from source. Pass --keep-storage (docker 28) "
        "or --max-used-space."
    )


def test_protected_images_are_derived_not_hardcoded() -> None:
    """The rollback image id changes every deploy — a literal one is stale on arrival."""
    code = _code_only(_script_text())
    assert "docker ps -aq" in code and "docker inspect" in code, (
        "the protected-image set must be derived from the live container list. The "
        "colours alternate on every deploy, so the rollback image id differs each time "
        "— core#666 recorded `aefa1c358978` and it was already stale when acted on."
    )
    literal_sha = re.search(r"\b(sha256:)?[0-9a-f]{12,64}\b", code)
    assert literal_sha is None, (
        f"hardcoded image id {literal_sha.group(0)!r} in prune-docker-cache.sh — derive it"
    )


def test_prune_verifies_protected_images_survived() -> None:
    """The script must fail closed if a prune removed a container-referenced image."""
    code = _code_only(_script_text())
    assert "docker image inspect" in code, (
        "nothing re-checks the protected images after pruning. The safety of this step "
        "rests on `image prune` skipping container-held images; verify it rather than "
        "assume it, because every unsafe variant is a shorter edit away."
    )
    assert "exit 1" in code, "the post-prune check must fail the deploy, not just warn"


def test_prune_runs_before_the_build() -> None:
    """Prune at the start: after the swap the retiring colour IS the rollback.

    Pruning after the swap verifies but before the old colour stops would hit an image
    that is still serving. Pruning after it stops would hit the rollback. Only at the
    start of the *next* deploy is the colour being retired two generations back.
    """
    doc = yaml.safe_load(_workflow_text())
    names = [s.get("name", "") for s in doc["jobs"]["deploy"]["steps"]]

    def index_of(fragment: str) -> int:
        for i, n in enumerate(names):
            if fragment.lower() in n.lower():
                return i
        raise AssertionError(f"no deploy step named like {fragment!r}; steps are {names}")

    prune_at = index_of("prune")
    build_at = index_of("Build image")
    swap_at = index_of("blue/green")

    assert prune_at < build_at, (
        f"prune (step {prune_at}) must run before the build (step {build_at}), or it "
        "evicts the cache the build is about to use"
    )
    assert prune_at < swap_at, (
        f"prune (step {prune_at}) must run before the swap (step {swap_at}) — after it, "
        "the image being pruned is either still serving or is the rollback"
    )


def test_cloud_checkout_pins_an_explicit_ref() -> None:
    """Production must state which cloud it ships, not inherit it from a repo setting.

    With no `ref:` this resolved cloud's default branch. That IS `master`, so the built
    pair has always been right — but by repository setting, not by anything in this file.
    Flipping cloud's default would have silently deployed cloud `dev` to production.
    cloud#110 is the same omission one severity lower.
    """
    doc = yaml.safe_load(_workflow_text())
    steps = doc["jobs"]["deploy"]["steps"]
    cloud = [
        s for s in steps if (s.get("with") or {}).get("repository") == "datanika-io/datanika-cloud"
    ]
    assert len(cloud) == 1, f"expected one cloud checkout, found {len(cloud)}"
    assert cloud[0]["with"].get("ref") == "master", (
        "the production deploy must pin cloud to `master` explicitly — prod is the "
        "master/master pair, and an unpinned checkout silently follows a repo setting"
    )
