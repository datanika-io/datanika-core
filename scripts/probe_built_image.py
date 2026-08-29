#!/usr/bin/env python3
"""Assert the BUILT IMAGE contains what production needs (core#602).

Why this exists
---------------
The image is the one artifact that ships, and it was the one artifact nothing
asserted on. core#602 shipped a broken `/mcp` for **five weeks** behind a green
CI and a green image build, and was found only when a deploy failed.

`build-push-image.yml` does build the image — but only on push to `dev`/`master`,
and it only asserts the build *succeeds*. The `RUN uv pip install ./datanika-mcp`
step succeeds perfectly; the breakage is at **import** time, inside the running
container, which nothing exercised. That is a green that would have looked
identical had the thing failed.

What actually breaks, precisely
-------------------------------
Core's `mcp>=1.0.0,<2` lives in `[project.optional-dependencies] dev`, and the
Dockerfile installs with `uv sync --frozen --no-dev`. **So the production image
installs no `mcp` from core at all**, and the pin never applies. The sole
constraint is `datanika-mcp/pyproject.toml`'s unbounded `mcp>=1.0.0`, resolved by
a *separate* `uv pip install ./datanika-mcp` that goes to PyPI rather than the
lock.

That has two consequences, and the second is the reason this probe does not
simply assert a version number:

1. `mcp` floats to 2.x, where `FastMCP` was renamed, so `mcp.server.fastmcp`
   stops existing.
2. That unlocked resolve can move **anything else** whose tree overlaps the
   locked one. `datanika-mcp` also declares an unbounded `httpx>=0.27`, and the
   transitive set reaches `anyio`, `pydantic`, `starlette` and `uvicorn`.

So `assert mcp < 2` is necessary but weak: it would not catch a rename *within*
the 1.x line, and it says nothing about the other packages that step can move.
This probe asserts the thing the deploy asserts — that the import actually
works — plus lock fidelity for every package the lock names.

Ownership: Engineering owns the pin, the source-level pin-drift guard, and a
`RUN` import assertion inside the Dockerfile. This is the complementary half:
theirs makes any build self-checking, this makes a build actually *happen* at PR
time, which theirs cannot do.

Usage
-----
    python scripts/probe_built_image.py --image datanika-qa-probe:local
    python scripts/probe_built_image.py --image <ref> --lock-pins pins.txt
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

# `importlib.metadata` rather than `pip freeze`: a uv-created venv need not have
# pip in it, and we must not install anything into the image we are inspecting.
_INSTALLED_SNIPPET = (
    "import json,importlib.metadata as m;"
    "print(json.dumps({d.metadata['Name']: d.version "
    "for d in m.distributions() if d.metadata['Name']}))"
)

# The exact chain `datanika/datanika.py` imports to mount /mcp.
#
# ⚠️ The SUBMODULES are load-bearing, and this is measured rather than argued.
# Against a real image carrying mcp 2.1.1:
#
#     import datanika_mcp                                  -> SUCCEEDS
#     from datanika_mcp.server import make_remote_transport -> ModuleNotFoundError:
#         No module named 'mcp.server.fastmcp'
#
# So a probe that checked only the top-level package would have gone GREEN on the
# exact image that broke production. The failure is raised from inside
# `datanika_mcp.server`, which is why the chain has to be walked.
_MCP_IMPORT_SNIPPET = (
    "import datanika_mcp;"
    "from datanika_mcp.client import DatanikaClient;"
    "from datanika_mcp.server import make_remote_transport;"
    "from datanika_mcp.session import DatanikaSession;"
    "import mcp;"
    "print(getattr(mcp,'__version__','unknown'))"
)

# The app's OWN mount predicate, copied from datanika/datanika.py. If this
# raises ImportError the app logs a warning and starts anyway with /mcp absent —
# which is exactly why #602 stayed latent.
_ROUTES_SNIPPET = (
    "from datanika.services.mcp_routes import mcp_lifespan, mcp_routes;"
    "paths=[getattr(r,'path',None) for r in mcp_routes];"
    "print(repr(paths))"
)

_PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*==\s*([^\s;#]+)")


def normalize(name: str) -> str:
    """PEP 503 normalisation, so `Foo_Bar` and `foo-bar` compare equal."""
    return re.sub(r"[-_.]+", "-", name).strip().lower()


def parse_pins(text: str) -> dict[str, str]:
    """Read `name==version` lines out of `uv export` output.

    Ignores `--index` / `--hash` lines, comments, blanks and environment
    markers. Deliberately tolerant: a line it cannot read is skipped rather than
    fataled, because a parser that dies on an unfamiliar flag turns a useful
    probe into a permanently red one that gets deleted.
    """
    pins: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "-", "@")):
            continue
        match = _PIN.match(line)
        if match:
            pins[normalize(match.group(1))] = match.group(2)
    return pins


def pin_drift(
    locked: dict[str, str], installed: dict[str, str]
) -> list[tuple[str, str, str]]:
    """Packages the lock names that the image installed at a DIFFERENT version.

    Only the intersection is compared, and that is deliberate:

    * A locked package that is **absent** from the image is not drift. The image
      installs `--no-dev`, so every dev-only dependency is legitimately missing.
    * An installed package the lock does **not** name is not drift either — it
      arrived with `datanika-cloud` or `datanika-mcp`, which are installed from
      source and are not in core's lock.

    What *is* drift is a package present in both at two different versions:
    the lock said one thing and the shipped artifact does another.
    """
    drift = []
    for name, locked_version in sorted(locked.items()):
        actual = installed.get(name)
        if actual is not None and actual != locked_version:
            drift.append((name, locked_version, actual))
    return drift


def last_line(text: str) -> str:
    """The final non-empty stdout line.

    Anything the interpreter emits ahead of our own `print` (a deprecation
    notice, a config warning) would otherwise be parsed as the answer.
    """
    lines = [line for line in text.splitlines() if line.strip()]
    return lines[-1].strip() if lines else ""


def run_in_image(image: str, argv: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "none", image, *argv],
        capture_output=True,
        text=True,
        timeout=180,
    )


def python_in_image(image: str) -> list[str]:
    """Resolve the interpreter to inspect the venv AS BUILT.

    ⚠️ Deliberately NOT `uv run python`. `uv run` re-syncs from the lock, which
    would *repair* the very drift this probe exists to detect and report a clean
    image that is not clean. Call the venv interpreter directly.
    """
    probe = run_in_image(image, ["/app/.venv/bin/python", "-c", "print('ok')"])
    if probe.returncode == 0 and "ok" in probe.stdout:
        return ["/app/.venv/bin/python"]
    return ["uv", "run", "--no-sync", "python"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="image ref to probe")
    parser.add_argument(
        "--lock-pins",
        help="`uv export --frozen --no-dev --no-emit-project --no-hashes` output",
    )
    args = parser.parse_args()

    failures: list[str] = []
    print(f"Probing image: {args.image}\n")

    py = python_in_image(args.image)
    print(f"interpreter: {' '.join(py)}\n")

    # --- A. the import chain the /mcp mount depends on --------------------
    print("[A] datanika_mcp import chain")
    result = run_in_image(args.image, [*py, "-c", _MCP_IMPORT_SNIPPET])
    if result.returncode == 0:
        print(f"    OK — datanika_mcp imports; mcp version {last_line(result.stdout)}")
    else:
        tail = (result.stderr or result.stdout).strip().splitlines()
        detail = tail[-1] if tail else "(no output)"
        failures.append(
            "A: `import datanika_mcp` FAILED inside the built image, so the app "
            "will log 'datanika-mcp not installed; /mcp endpoint not mounted' "
            "and start anyway with /mcp absent. The blue/green post-swap "
            "assertion then fails and the deploy is blocked (core#602).\n"
            f"       {detail}"
        )
        print(f"    FAIL — {detail}")

    # --- B. the app's own mount predicate ---------------------------------
    print("\n[B] the mount predicate from datanika/datanika.py")
    result = run_in_image(args.image, [*py, "-c", _ROUTES_SNIPPET])
    if result.returncode == 0:
        paths = result.stdout.strip()
        print(f"    routes: {paths}")
        if "/mcp" not in paths:
            failures.append(
                f"B: mcp_routes imported but exposes no /mcp path — got {paths}"
            )
    else:
        stderr = (result.stderr or result.stdout).strip()
        tail = stderr.splitlines()
        detail = tail[-1] if tail else "(no output)"
        # A red must not point at the wrong layer. Only claim #602 when the
        # failure is actually an ImportError about the mcp packages; anything
        # else is a different problem wearing the same colour.
        if "ImportError" in stderr or "ModuleNotFoundError" in stderr:
            failures.append(
                "B: the app's own /mcp mount predicate raises ImportError, which "
                "is the swallowed exception in datanika/datanika.py — the app "
                "starts fine and /mcp is simply gone (core#602).\n"
                f"       {detail}"
            )
        else:
            failures.append(
                "B: the mount predicate failed for a reason that is NOT an "
                "ImportError. This is probably an environment gap in the probe "
                "rather than core#602 — read it before triaging it as such.\n"
                f"       {detail}"
            )
        print(f"    FAIL — {detail}")

    # --- C. lock fidelity --------------------------------------------------
    print("\n[C] installed versions vs uv.lock")
    if not args.lock_pins:
        print("    SKIPPED — no --lock-pins supplied")
    else:
        with open(args.lock_pins, encoding="utf-8") as handle:
            locked = parse_pins(handle.read())
        result = run_in_image(args.image, [*py, "-c", _INSTALLED_SNIPPET])
        if result.returncode != 0:
            failures.append(f"C: could not enumerate installed packages: {result.stderr.strip()}")
            print("    FAIL — could not enumerate installed packages")
        else:
            raw = json.loads(last_line(result.stdout))
            installed = {normalize(k): v for k, v in raw.items()}
            drift = pin_drift(locked, installed)
            compared = len(set(locked) & set(installed))
            print(f"    lock names {len(locked)}, image has {len(installed)}, compared {compared}")
            if drift:
                for name, want, got in drift:
                    print(f"    DRIFT {name}: lock {want} -> image {got}")
                failures.append(
                    f"C: {len(drift)} package(s) in the image differ from uv.lock. "
                    "The Dockerfile's two unlocked `uv pip install` steps resolve "
                    "against PyPI, so they can move anything whose tree overlaps "
                    "the locked one:\n"
                    + "\n".join(f"       {n}: lock {w} -> image {g}" for n, w, g in drift)
                )
            else:
                print(f"    OK — no drift across {compared} shared packages")

    print("\n" + "=" * 70)
    if failures:
        print(f"IMAGE PROBE FAILED — {len(failures)} finding(s)\n")
        for item in failures:
            print(f"  * {item}\n")
        return 1
    print("IMAGE PROBE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
