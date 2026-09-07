#!/usr/bin/env python3
"""core#1140 — refuse to build an image whose cloud tree imports a core symbol that is not there.

## The direction core#1123 cannot see

``cloud_pairing_gate.py`` grades ``datanika-cloud``'s own ``master``↔``dev`` relationship. It
never reads the core tree, so **no value of its inputs can express** the other half of the
pairing constraint:

======  ==========================================================  ==================
   #    shape                                                       core#1123 covers it?
======  ==========================================================  ==================
   A    core deploys while cloud ``dev`` carries commits the image   yes — this is what
        has never been built against                                 it was built for
   B    cloud ``master`` carries an import that the core tree        no — structural,
        beside it cannot satisfy                                     not a defect in it
======  ==========================================================  ==================

Direction B is what this script is for. The instant cloud is promoted alone,
``master...dev`` reads ``identical`` and core#1123 passes, while the core ``master`` an image
is built from may still declare neither ``ConfigurationError`` nor ``InternalInvariantError``.

## Where B actually hurts, stated accurately

🚨 **On our own box it is a wedged deploy that FAILS SAFE, and it must not be described as
worse than it is.** ``uv pip install /cloud`` (``Dockerfile``) installs without importing, and
the only build-time import assertion is the ``/mcp`` surface — so a cross-repo symbol mismatch
**builds green**. It surfaces at container start, where ``datanika/datanika.py`` and
``datanika/tasks/celery_app.py`` call ``bootstrap_cloud()`` with no ``try``/``except``,
deliberately. The new colour dies at import, its healthcheck never passes,
``deploy-bluegreen.sh`` refuses the repoint, and production keeps serving the old colour.

**The residual that is NOT safe is the tag path.** ``build-push-image.yml`` checks the cloud
repo out at ``master`` for a ``v*`` tag while the core tree is the tag's own SHA. Cut a release
on a core ``master`` that predates a cloud-only promotion and the job pushes ``:v0.x.y`` **and
``:latest``** — green, with no assertion anywhere — as an image that dies at container start.
Blue/green protects our box. It does not protect a self-hoster's.

⚠️ **This says nothing about whether that package should be public** — it is private, and
core#1014 settled that as a founder decision rather than a pending task. A broken pairing
sitting in a private registry is still the rollback artifact on our own box, and still one
visibility setting away from being everyone's.

## Why AST rather than an import

core#832: ``import datanika_cloud.plugin`` raises, because ``plugin.py`` imports back into
``datanika.ui.state.auth_state``, which reaches ``celery_app`` again. Both real entrypoints
happen to import in a safe order; the probe order does not exist in production. So an
interpreter-based check is unavailable here on a repo that has already been bitten by exactly
this.

The import *surface*, unlike the pairing semantics core#1123's docstring correctly calls
unreachable by heuristics, is **syntax**. Both trees are already on the runner. No interpreter,
no settings, no database, no network.

## Scope, and why it stops where it does

Graded: every ``datanika_cloud/**/*.py`` — that is the tree ``uv pip install /cloud`` installs
and the only cloud code a container imports at start. **Cloud's own ``tests/`` are deliberately
excluded**: they are not installed into the image, so a break there cannot wedge a deploy or
poison a published tag. It is cloud CI's job, and cloud#137 is the issue about that gate being
wrong in a different way. Widening this one to cover tests would make it red for a reason it
cannot act on.

## What it asserts

For each ``from datanika... import a, b`` in the cloud tree, ``a`` and ``b`` must each be bound
at module scope in the corresponding core module — or, when that module is a package, exist as
a submodule of it. For each ``import datanika.x.y``, the module must resolve to a file.

Bindings counted: classes, functions, module-level assignments (including annotated and
augmented), imports and aliases, ``try``/``if``/``with`` bodies at module level, and anything
named in ``__all__``. A re-export is a binding — ``from datanika.errors import X`` is satisfied
by ``errors.py`` itself doing ``from .foo import X``.

## Fail-closed

Finding **no** references to check is not a pass. The cloud tree imports core in dozens of
places; a run that grades zero of them has measured nothing, and zero unresolved references is
what that looks like from the outside. Under ``--min-refs`` the script exits non-zero with
``NO-VERDICT``.
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass
from pathlib import Path

CORE_TOP = "datanika"
CLOUD_PKG = "datanika_cloud"

# The cloud tree imports core in dozens of places. If a run grades fewer than this it has
# almost certainly been pointed at the wrong directory — and "0 unresolved" is exactly what
# that looks like from the outside.
DEFAULT_MIN_REFS = 20


@dataclass(frozen=True)
class Ref:
    """One `name` that a cloud file requires from a core `module`."""

    file: str
    line: int
    module: str
    name: str | None  # None => the module itself must resolve (plain `import datanika.x`)


def _module_to_paths(core_root: Path, module: str) -> tuple[Path | None, Path | None]:
    """Return (module_file, package_init) for a dotted core module name.

    A module can be `datanika/errors.py` or `datanika/errors/__init__.py`; both are legal and
    the second is what makes submodule imports resolvable.
    """
    parts = module.split(".")
    if not parts or parts[0] != CORE_TOP:
        return None, None
    base = core_root.joinpath(*parts)
    mod_file = base.with_suffix(".py")
    pkg_init = base / "__init__.py"
    return (mod_file if mod_file.is_file() else None, pkg_init if pkg_init.is_file() else None)


def _bound_names(tree: ast.Module) -> set[str]:
    """Every name bound at module scope, including inside module-level `try`/`if`/`with`.

    A conditional or `try`-guarded import still binds the name for an importer, so a scan that
    only walked `tree.body` would report false failures on exactly the defensive-import style
    this codebase uses.
    """
    names: set[str] = set()

    def target_names(node: ast.AST) -> None:
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Tuple | ast.List):
            for elt in node.elts:
                target_names(elt)
        elif isinstance(node, ast.Starred):
            target_names(node.value)

    def walk(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    target_names(t)
            elif isinstance(node, ast.AnnAssign | ast.AugAssign):
                target_names(node.target)
            elif isinstance(node, ast.Import):
                for a in node.names:
                    # `import a.b.c` binds `a`; `import a.b as c` binds `c`.
                    names.add(a.asname or a.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                for a in node.names:
                    if a.name == "*":
                        # Cannot enumerate; treat as opaque so we never fail on a re-export
                        # we simply cannot see. Recorded honestly in the summary.
                        names.add("*")
                    else:
                        names.add(a.asname or a.name)
            elif isinstance(node, ast.Try):
                walk(node.body)
                for h in node.handlers:
                    walk(h.body)
                walk(node.orelse)
                walk(node.finalbody)
            elif isinstance(node, ast.If | ast.While | ast.For | ast.AsyncFor):
                walk(node.body)
                walk(node.orelse)
            elif isinstance(node, ast.With | ast.AsyncWith):
                walk(node.body)

    walk(tree.body)

    # `__all__ = [...]` names things the module intends to export; honour it even when the
    # binding itself came from a form we did not model.
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.List | ast.Tuple):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets):
            continue
        for elt in node.value.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                names.add(elt.value)
    return names


def collect_refs(cloud_pkg_root: Path) -> list[Ref]:
    """Every core symbol the cloud package requires, with the file:line that requires it."""
    refs: list[Ref] = []
    for path in sorted(cloud_pkg_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:  # a tree we cannot parse is not a tree we can clear
            print(f"  PARSE FAILURE {path}: {exc}", file=sys.stderr)
            raise
        rel = path.as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                # `node.level > 0` is a relative import: intra-cloud, not our business.
                if node.level or not node.module:
                    continue
                if node.module != CORE_TOP and not node.module.startswith(CORE_TOP + "."):
                    continue
                for a in node.names:
                    refs.append(Ref(rel, node.lineno, node.module, a.name))
            elif isinstance(node, ast.Import):
                for a in node.names:
                    if a.name == CORE_TOP or a.name.startswith(CORE_TOP + "."):
                        refs.append(Ref(rel, node.lineno, a.name, None))
    return refs


def resolve(core_root: Path, ref: Ref) -> str | None:
    """Return a failure reason, or None when the reference resolves."""
    mod_file, pkg_init = _module_to_paths(core_root, ref.module)
    if mod_file is None and pkg_init is None:
        return f"core module '{ref.module}' does not exist in the core tree"

    if ref.name is None:
        return None  # `import datanika.x` — the module resolving is the whole requirement

    if ref.name == "*":
        return None  # star import: the module resolved; names are unknowable by AST

    target = mod_file or pkg_init
    if target is None:  # unreachable: the pair was checked above. Explicit, not asserted.
        return f"core module '{ref.module}' does not exist in the core tree"
    try:
        tree = ast.parse(target.read_text(encoding="utf-8"), filename=str(target))
    except SyntaxError as exc:
        return f"core module '{ref.module}' does not parse: {exc}"

    names = _bound_names(tree)
    if ref.name in names:
        return None
    if "*" in names:
        # The core module star-imports from somewhere; we cannot prove absence.
        return None
    if pkg_init is not None:
        # `from datanika.services import concurrency_service` — a submodule, not a binding.
        sub = pkg_init.parent / f"{ref.name}.py"
        if sub.is_file() or (pkg_init.parent / ref.name / "__init__.py").is_file():
            return None
    return f"'{ref.name}' is not defined in core module '{ref.module}'"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--core", required=True, help="path to the core tree (contains datanika/)")
    ap.add_argument(
        "--cloud", required=True, help="path to the cloud tree (contains datanika_cloud/)"
    )
    ap.add_argument("--min-refs", type=int, default=DEFAULT_MIN_REFS)
    args = ap.parse_args()

    core_root = Path(args.core).resolve()
    cloud_root = Path(args.cloud).resolve()
    cloud_pkg = cloud_root / CLOUD_PKG

    print("core#1140 cross-tree import gate")
    print(f"  core tree:  {core_root}")
    print(f"  cloud tree: {cloud_root}")

    if not (core_root / CORE_TOP).is_dir():
        print(f"  NO-VERDICT: {core_root / CORE_TOP} is not a directory — wrong --core path?")
        return 1
    if not cloud_pkg.is_dir():
        print(f"  NO-VERDICT: {cloud_pkg} is not a directory — wrong --cloud path?")
        return 1

    refs = collect_refs(cloud_pkg)
    files = len({r.file for r in refs})
    print(f"  graded {len(refs)} core reference(s) across {files} cloud file(s)")

    if len(refs) < args.min_refs:
        print(
            f"  NO-VERDICT: only {len(refs)} reference(s) found, below the floor of "
            f"{args.min_refs}. A run that grades nothing reports zero failures, which is "
            f"indistinguishable from a pass. Check --cloud points at the tree, not the repo "
            f"root's parent."
        )
        return 1

    failures = [(r, why) for r in refs if (why := resolve(core_root, r)) is not None]

    if failures:
        print()
        print(f"  REFUSED: {len(failures)} cross-tree reference(s) do not resolve.")
        print("  The cloud tree beside this core tree imports symbols the core tree does not")
        print("  declare. An image built from this pair installs green and dies at container")
        print("  start, when bootstrap_cloud() runs.")
        print()
        for r, why in failures:
            print(f"    {r.file}:{r.line}  {why}")
        print()
        print("  Fix by pairing the trees, not by relaxing this gate: promote the core half")
        print("  that declares these symbols, or build against the cloud ref that predates")
        print("  them. See docs/runbooks/RUNBOOK_DEV_TO_MASTER.md, 'Order: cloud first'.")
        return 1

    print("  PASS: every core symbol the cloud tree imports is declared in the core tree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
