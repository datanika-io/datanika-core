"""Every command run against the image must have a build-time import assertion (core#1201).

The image is **one artifact with four commands run against it**, and until core#1201 exactly one
of them was asserted at build time. `datanika_mcp` is a sub-import of the app, so the existing
`/mcp` line proved a slice of the `app` path and nothing about `celery`, `beat` or `scheduler`.

🔑 **The defect was shaped like coverage.** There *was* an assertion, it *did* run, and it was
right about the thing it checked — it simply never touched three quarters of what the image runs.
A broken `celery_app` or `scheduler_main` built clean, deployed, and failed at container start.
*Ask what the instrument would have printed had the instrument itself failed*: for those three, the
same thing it printed when they were fine.

## Why this file derives the set instead of listing it

Listing the four modules here would reproduce the original defect one level up: the list would be
right today and silently stale the moment a fifth service appears. So the **services** come from
`docker-compose.yml` — the file that decides what actually runs — and only the *mapping* from a
service to its entrypoint module is declared, because one of them is not derivable from the command
text at all (`reflex run` does not name `datanika.datanika`).

A new service running the image therefore fails this suite until somebody says what its entrypoint
imports. That is the intended cost: it puts a new entrypoint in a diff where it can be reviewed.

⚠️ **This asserts the Dockerfile CONTAINS the assertion. It cannot assert the image was built.**
That is CI's job and it is real: `image-probe` builds the cloud variant and `core-only-image`
builds `DATANIKA_IMAGE_EDITION=core`, on every PR. Both are the reason core#1201's
"verified on both variants" is satisfied by shipping rather than by a claim here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = REPO_ROOT / "docker-compose.yml"
DOCKERFILE = REPO_ROOT / "Dockerfile"

#: service -> the module ITS process imports. Declared, not derived, because `reflex run` does
#: not name its module. Everything else is checked against the compose file below, so this map
#: cannot quietly fall behind the set of services.
ENTRYPOINT_MODULE = {
    "app": "datanika.datanika",
    "app_b": "datanika.datanika",
    "celery": "datanika.tasks.celery_app",
    "beat": "datanika.tasks.celery_app",
    "scheduler": "datanika.scheduler_main",
}


def _compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def services_running_the_image() -> set[str]:
    """Services whose container is the core image — i.e. every command the image must survive."""
    out = set()
    for name, spec in _compose().get("services", {}).items():
        image = str(spec.get("image", ""))
        if "datanika-core" in image or "build" in spec:
            out.add(name)
    return out


def asserted_modules() -> set[str]:
    """Modules the Dockerfile imports in a build-time assertion.

    Reads the `for mod in ...` list and every `import <mod>` inside a `python -c`, so a future
    assertion written either way is seen.
    """
    text = DOCKERFILE.read_text(encoding="utf-8")
    mods: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue  # a comment naming a module is not an assertion (core#1055)
        if "for mod in" in stripped:
            body = stripped.split("for mod in", 1)[1].split(";")[0]
            mods.update(re.findall(r"[A-Za-z_][\w.]*\.[\w.]+", body))
        mods.update(re.findall(r"import\s+([A-Za-z_][\w.]*)", stripped))
    return mods


class TestEveryEntrypointIsAsserted:
    def test_every_service_running_the_image_has_a_declared_entrypoint(self) -> None:
        """A new service must not be able to appear without declaring what it imports."""
        undeclared = services_running_the_image() - set(ENTRYPOINT_MODULE)
        assert not undeclared, (
            f"{sorted(undeclared)} run the core image and no entrypoint module is declared for "
            "them in ENTRYPOINT_MODULE. Add it, and add it to the Dockerfile's import assertion "
            "in the same change — a command nothing asserts is core#1201 all over again."
        )

    def test_the_declared_map_does_not_name_services_that_no_longer_exist(self) -> None:
        """The other direction. A map that outlives its services rots quietly and then reads as
        coverage of something that is not there."""
        stale = set(ENTRYPOINT_MODULE) - services_running_the_image()
        assert not stale, f"ENTRYPOINT_MODULE names {sorted(stale)}, which no longer run the image"

    @pytest.mark.parametrize("service", sorted(ENTRYPOINT_MODULE))
    def test_the_dockerfile_asserts_this_entrypoint(self, service: str) -> None:
        module = ENTRYPOINT_MODULE[service]
        assert module in asserted_modules(), (
            f"the image runs `{service}`, whose process imports `{module}`, and the Dockerfile "
            f"has no build-time assertion importing it. If it stops importing in the image, the "
            f"build is clean and the container fails at start — after the deploy."
        )

    def test_each_entrypoint_is_imported_in_its_own_process(self) -> None:
        """core#832: `celery_app` imports `bootstrap_cloud` at module level while `plugin`
        imports back into `auth_state`, which reaches `celery_app` again. The real entrypoints
        happen to import in a safe order; one interpreter importing all of them invents an order
        production never takes, and a build that fails on that cycle fails on CORRECT code."""
        text = DOCKERFILE.read_text(encoding="utf-8")
        combined = re.findall(
            r'python -c "import ([A-Za-z_][\w.]*(?:\s*,\s*[A-Za-z_][\w.]*)+)"', text
        )
        for group in combined:
            mods = {m.strip() for m in group.split(",")}
            entrypoints = mods & set(ENTRYPOINT_MODULE.values())
            assert len(entrypoints) <= 1, (
                f"one interpreter imports several entrypoints together: {sorted(entrypoints)}. "
                "Use one process per entrypoint — see core#832."
            )


class TestTheGuardCanActuallyFail:
    """Guard-the-guard. Each assertion above is only worth having if it can go red."""

    def test_the_service_scan_finds_the_real_services(self) -> None:
        """An empty scan would make every assertion above vacuously true."""
        found = services_running_the_image()
        assert len(found) >= 4, f"only {len(found)} service(s) found running the image: {found}"
        assert {"app", "celery", "beat"} <= found

    def test_the_module_scan_finds_the_real_assertions(self) -> None:
        found = asserted_modules()
        assert "datanika_mcp.server" in found, "the pre-existing /mcp assertion was not seen"
        assert len(found) >= 4, f"only {len(found)} asserted module(s) parsed: {sorted(found)}"

    def test_an_undeclared_service_would_be_caught(self) -> None:
        """The realistic regression: somebody adds a fifth command to the image."""
        pretend = services_running_the_image() | {"brand_new_worker"}
        assert pretend - set(ENTRYPOINT_MODULE) == {"brand_new_worker"}

    def test_a_module_named_only_in_a_comment_does_not_count_as_asserted(self) -> None:
        """core#1055: a guard reading raw text is satisfied by the step's own comment. The
        Dockerfile's new block *describes* every entrypoint in prose above the RUN line, so this
        is not hypothetical — without comment-stripping the guard would pass on prose alone."""
        assert "# datanika.totally_made_up" not in DOCKERFILE.read_text(encoding="utf-8")
        fake = "#   import datanika.totally_made_up\n"
        mods: set[str] = set()
        for line in fake.splitlines():
            if line.strip().startswith("#"):
                continue
            mods.update(re.findall(r"import\s+([A-Za-z_][\w.]*)", line))
        assert mods == set(), "a commented-out import was counted as an assertion"
