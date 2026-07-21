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
