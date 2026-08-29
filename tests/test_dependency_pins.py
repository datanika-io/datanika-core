"""CI must test the dependency tree that ships (core#470).

`reflex>=0.7` had no upper bound. Production installs from `uv.lock` via
`uv sync --frozen` (reflex **0.8.26**); CI ran `uv pip install -e ".[dev]"`,
which ignores the lock and re-resolved to the **0.9.x** line. So CI green and
production behaviour were statements about different dependency trees.

That is not a papercut. #452 was a Reflex *handler-discovery* bug — the exact
class of behaviour that varies between framework versions — and 2,493 tests
were green while the onboarding path returned 500 in prod.

Two guards, because the failure has two halves:

* :class:`TestInstalledMatchesLock` — the loud mismatch check. It compares the
  *installed* version against the one `uv.lock` pins, so an environment that
  resolved its own tree fails instead of quietly disagreeing with prod.
* :class:`TestEveryDependencyIsBounded` — the class fix. Bounding only
  ``reflex`` would leave the same latent failure in every dependency free to
  float a major; at the time of writing `oracledb>=2.0` was resolving to
  **4.0.2** and `pytest>=8.0` to **9.0.2**.

An upper bound is not what makes CI and prod agree — the lockfile is. Bounds
make a major bump a **deliberate** act (`uv lock` plus an edit here) rather
than something that happens on whichever day a maintainer cuts a release.
"""

import pathlib
import re
import tomllib
from importlib import metadata

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_PYPROJECT = _ROOT / "pyproject.toml"
_LOCK = _ROOT / "uv.lock"

#: Dependencies allowed to float without an upper bound, each with a reason.
#: Empty on purpose: every current dependency is bounded, and an entry here
#: should be an argued exception rather than a place to park work.
_ACCEPTED_UNBOUNDED: dict[str, str] = {}

#: Checked by name because it is the one that actually broke, and the one whose
#: behaviour the app is most coupled to.
_CRITICAL = "reflex"


def _locked_versions() -> dict[str, str]:
    text = _LOCK.read_text(encoding="utf-8")
    return {
        name.lower(): version
        for name, version in re.findall(
            r'^name = "([^"]+)"\nversion = "([^"]+)"', text, re.MULTILINE
        )
    }


def _declared_dependencies() -> list[tuple[str, str]]:
    """[(group, spec)] across runtime and every optional-dependency extra."""
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    project = data["project"]
    out = [("runtime", spec) for spec in project["dependencies"]]
    for extra, specs in (project.get("optional-dependencies") or {}).items():
        out.extend((f"extra:{extra}", spec) for spec in specs)
    return out


def _dist_name(spec: str) -> str:
    return re.split(r"[<>=\[!~ ]", spec, maxsplit=1)[0].strip()


class TestEveryDependencyIsBounded:
    def test_no_dependency_may_float_a_major(self):
        unbounded = [
            f"{group}: {spec}"
            for group, spec in _declared_dependencies()
            if "<" not in spec and _dist_name(spec) not in _ACCEPTED_UNBOUNDED
        ]
        assert not unbounded, (
            "Dependencies with no upper bound — CI and prod may resolve "
            "different majors (core#470):\n  " + "\n  ".join(unbounded)
        )

    def test_accepted_exceptions_carry_a_reason(self):
        """An exception without a stated reason is just an unbounded pin."""
        for name, reason in _ACCEPTED_UNBOUNDED.items():
            assert reason.strip(), f"{name} is exempted with no reason given"

    def test_the_lock_satisfies_every_declared_bound(self):
        """A bound that excludes what we ship would break the next `uv lock`."""
        locked = _locked_versions()
        broken = []
        for _group, spec in _declared_dependencies():
            name = _dist_name(spec).lower()
            upper = re.search(r"<\s*([0-9][0-9a-zA-Z.]*)", spec)
            version = locked.get(name)
            if not upper or not version:
                continue
            ceiling = [int(p) for p in upper.group(1).split(".") if p.isdigit()]
            actual = [int(p) for p in version.split(".") if p.isdigit()]
            if actual[: len(ceiling)] >= ceiling:
                broken.append(f"{spec} but lock has {version}")
        assert not broken, "Upper bound excludes the locked version:\n  " + "\n  ".join(broken)


class TestInstalledMatchesLock:
    """The loud mismatch check the issue asks for."""

    def test_reflex_installed_is_the_locked_version(self):
        locked = _locked_versions().get(_CRITICAL)
        assert locked, f"{_CRITICAL} not found in uv.lock"

        try:
            installed = metadata.version(_CRITICAL)
        except metadata.PackageNotFoundError:  # pragma: no cover
            pytest.fail(f"{_CRITICAL} is not installed")

        assert installed == locked, (
            f"{_CRITICAL} {installed} is installed but uv.lock pins {locked}.\n"
            "This environment is not testing what production runs (core#470). "
            "Install from the lock — `uv sync --frozen`, or "
            "`uv export --frozen | uv pip install -r -` — rather than "
            "re-resolving with `uv pip install -e .`."
        )

    def test_installed_reflex_satisfies_the_declared_bound(self):
        """Belt to the lock's braces: catches a hand-installed override too."""
        spec = next(spec for _g, spec in _declared_dependencies() if _dist_name(spec) == _CRITICAL)
        upper = re.search(r"<\s*([0-9][0-9a-zA-Z.]*)", spec)
        assert upper, f"{_CRITICAL} must keep an upper bound — it is why #470 exists"

        installed = [int(p) for p in metadata.version(_CRITICAL).split(".") if p.isdigit()]
        ceiling = [int(p) for p in upper.group(1).split(".") if p.isdigit()]
        assert installed[: len(ceiling)] < ceiling, (
            f"{_CRITICAL} {metadata.version(_CRITICAL)} violates {spec}"
        )


# ---------------------------------------------------------------------------
# core#602 — the same lesson one layer out: CI green, shipped image broken.
#
# #470 was "local green and CI green are statements about different dependency
# trees". #602 is "CI green and the *image* are", and this time our own
# Dockerfile creates the divergence.
#
# What actually happened: `mcp` is declared only in core's **dev extra**, and
# the image is built with `uv sync --frozen --no-dev`, so core installs no `mcp`
# at all. The sole thing that installs it is a later, separate
# `uv pip install ./datanika-mcp`, which resolves against PyPI rather than the
# lock. The sub-package said `mcp>=1.0.0` with no ceiling, so that step took
# `mcp` 2.x, where `FastMCP` had been renamed -- `datanika_mcp` then failed to
# import, `/mcp` was never mounted, and the blue/green post-swap assertion
# failed in production. Five weeks latent, because nothing rebuilt.
#
# Three guards, because the failure has three independent halves:
#
#   * :class:`TestSubPackagePinsAreNoLooserThanCore` -- the source-level drift.
#     One dependency declared in two files that are installed in sequence.
#   * :class:`TestTheImageCannotFloatAwayFromTheLock` -- the mechanism. This is
#     the one that asserts what was actually true that night: a *correct*
#     ceiling is not enough, because a later unconstrained install step can
#     move anything, including packages neither file names.
#   * The build-time import assertion -- the artifact must check itself, so a
#     broken image fails the build instead of being shipped and discovered by
#     a production deploy, which is the most expensive possible place.
# ---------------------------------------------------------------------------

_SUB_PACKAGES = {"datanika-mcp": _ROOT / "datanika-mcp" / "pyproject.toml"}
_DOCKERFILE = _ROOT / "Dockerfile"


def _ceiling(spec: str) -> tuple[int, ...] | None:
    match = re.search(r"<\s*([0-9][0-9a-zA-Z.]*)", spec)
    if not match:
        return None
    return tuple(int(p) for p in match.group(1).split(".") if p.isdigit())


def _sub_package_dependencies(path: pathlib.Path) -> list[str]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return list(data["project"].get("dependencies") or [])


def _core_specs_by_name() -> dict[str, str]:
    """Core's declaration for each dependency, wherever it is declared.

    Runtime and extras both count. A dev-extra pin is what CI resolves, so a
    sub-package that disagrees with it ships something CI never tested -- which
    is precisely the #602 shape.
    """
    return {_dist_name(spec).lower(): spec for _group, spec in _declared_dependencies()}


def _dockerfile_logical_lines() -> list[str]:
    """Dockerfile lines with backslash continuations joined, comments dropped."""
    joined: list[str] = []
    buffer = ""
    for raw in _DOCKERFILE.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip()
        if line.endswith("\\"):
            buffer += line[:-1] + " "
            continue
        buffer += line
        joined.append(buffer)
        buffer = ""
    if buffer:
        joined.append(buffer)
    return [ln for ln in joined if ln.strip() and not ln.lstrip().startswith("#")]


class TestSubPackagePinsAreNoLooserThanCore:
    """A dependency declared in two files that install in sequence must agree."""

    def test_every_sub_package_dependency_is_bounded(self):
        unbounded = [
            f"{name}: {spec}"
            for name, path in _SUB_PACKAGES.items()
            for spec in _sub_package_dependencies(path)
            if "<" not in spec
        ]
        assert not unbounded, (
            "Sub-package dependencies with no upper bound (core#602). These are "
            "installed by a separate `uv pip install` step that does NOT consult "
            "uv.lock, so an unbounded spec floats to whatever PyPI serves on "
            "build day -- that is how `mcp` 2.x reached production:\n  "
            + "\n  ".join(unbounded)
        )

    def test_sub_package_may_not_raise_a_ceiling_core_set(self):
        core = _core_specs_by_name()
        looser = []
        for name, path in _SUB_PACKAGES.items():
            for spec in _sub_package_dependencies(path):
                dist = _dist_name(spec).lower()
                core_spec = core.get(dist)
                if core_spec is None:
                    continue
                core_ceiling = _ceiling(core_spec)
                if core_ceiling is None:
                    continue
                sub_ceiling = _ceiling(spec)
                if sub_ceiling is None:
                    looser.append(f"{name} declares {spec!r}; core declares {core_spec!r}")
                    continue
                width = min(len(core_ceiling), len(sub_ceiling))
                if sub_ceiling[:width] > core_ceiling[:width]:
                    looser.append(f"{name} declares {spec!r}; core declares {core_spec!r}")
        assert not looser, (
            "A sub-package declares a looser bound than core for the same "
            "dependency (core#602). The sub-package is installed LAST, so its "
            "constraint is the one that decides what ships:\n  " + "\n  ".join(looser)
        )


class TestTheImageCannotFloatAwayFromTheLock:
    """The pin was correct and the artifact was still wrong. Guard the mechanism.

    `uv sync --frozen` installs exactly what `uv.lock` resolved. Every
    `uv pip install` after it re-resolves against PyPI and may move *any*
    already-installed package -- `mcp` pulls anyio, httpx, pydantic, starlette
    and uvicorn, all of which core locks. A ceiling on one dependency does not
    address that; constraining the install to the lock does.
    """

    def test_every_install_after_the_sync_is_constrained_to_the_lock(self):
        lines = _dockerfile_logical_lines()
        sync = next(
            (i for i, ln in enumerate(lines) if "uv sync" in ln and "--frozen" in ln), None
        )
        assert sync is not None, "Dockerfile no longer installs from the lock at all"

        floating = [
            ln
            for ln in lines[sync + 1 :]
            if "uv pip install" in ln
            and not re.search(r"(--constraint[= ]|\s-c\s|--no-deps)", ln)
        ]
        assert not floating, (
            "An install step after `uv sync --frozen` can re-resolve and move "
            "packages the lock pinned (core#602). Pass `--constraint` (built "
            "from `uv export --frozen`) or `--no-deps`:\n  " + "\n  ".join(floating)
        )

    def test_the_constraints_are_derived_from_the_lock_not_hand_written(self):
        text = _DOCKERFILE.read_text(encoding="utf-8")
        assert "uv export" in text and "--frozen" in text, (
            "The constraints file must be generated from uv.lock via "
            "`uv export --frozen`. A hand-maintained list is a third place for "
            "the same versions to drift (core#602)."
        )

    def test_the_build_asserts_the_mcp_entrypoint_imports(self):
        """Nothing asserted on the artifact. That is why this shipped."""
        lines = _dockerfile_logical_lines()
        install = next(
            (i for i, ln in enumerate(lines) if "uv pip install" in ln and "datanika-mcp" in ln),
            None,
        )
        assert install is not None, "Dockerfile no longer installs ./datanika-mcp"

        after = " ".join(lines[install + 1 :])
        assert "import datanika_mcp" in after, (
            "The build must import `datanika_mcp` after installing it "
            "(core#602). Without it a broken image builds cleanly and the "
            "failure surfaces in the production blue/green post-swap probe -- "
            "the most expensive place to learn it. The running container said "
            "exactly this: No module named 'mcp.server.fastmcp'."
        )
