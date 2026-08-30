#!/usr/bin/env python
"""Targeted mutation probe — an audit instrument, deliberately not a CI gate.

Break one line of a module, run that module's own tests, and record whether they
noticed. A mutant that **survives** is a line the suite does not watch.

    python scripts/mutation_probe.py \\
        --module datanika/services/api_middleware.py \\
        --tests tests/test_services/test_api_middleware.py

Why this is not wired into CI is argued in
``plans/engineering/AUDIT_TESTS_THAT_CANNOT_FAIL.md``. Short version: a sampled
pass over all 221 modules measured out at 25–35 hours, and the audit's decisive
finding is that mutation is structurally blind to the class of defect that cost
us the most — core#492 was a *missing* transformer, and there is no mutant for
code that was never written. Use this when a module's blast radius justifies an
hour, not on every push.

────────────────────────────────────────────────────────────────────────────────
SAFETY CONTRACT — read before changing anything below
────────────────────────────────────────────────────────────────────────────────
This file mutates tracked source in place. On 2026-08-30 an earlier harness hit a
two-minute command timeout mid-run, its ``finally:`` never executed, and a mutated
constant was left sitting in ``datanika/``. So the recovery here does **not**
depend on this process surviving:

1. **Pre-flight refusal.** Will not start unless ``git status --porcelain`` is
   empty. A restore can then never destroy work, because there is none.
2. **Sentinel written first.** Pristine bytes go to ``<state>/originals/`` and
   ``<state>/SENTINEL`` names every touched path *before* the source is written.
   A SIGKILL cannot outrun a write that already happened.
3. **Restore is standalone and idempotent.** ``--restore`` recovers with no
   surviving process and is a no-op when there is nothing to do. It also runs
   automatically at the start of the next probe.
4. **Verification asks git, not itself.** After restoring it shells out to
   ``git status --porcelain`` and exits non-zero if anything is still dirty, and
   separately re-reads every file and compares bytes.

``tests/test_scripts/test_mutation_probe.py`` proves property 3 by killing a real
run mid-mutation with ``Popen.kill()`` — which on Windows is ``TerminateProcess``
and runs no handler of any kind — and then recovering from the sentinel alone.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_STATE_DIRNAME = ".mutation-probe-state"


# ────────────────────────────────────────────────────────────── state & safety
class Store:
    """On-disk record of what has been mutated. The whole safety story lives here."""

    def __init__(self, state_dir: Path):
        self.dir = state_dir
        self.originals = state_dir / "originals"
        self.sentinel = state_dir / "SENTINEL"

    def _blob_name(self, rel: Path) -> str:
        return hashlib.sha1(str(rel).encode()).hexdigest()[:16] + "__" + rel.name

    def save_original(self, repo: Path, rel: Path) -> bytes:
        """Persist pristine bytes BEFORE the caller writes a mutant. Returns them."""
        self.originals.mkdir(parents=True, exist_ok=True)
        data = (repo / rel).read_bytes()
        dest = self.originals / self._blob_name(rel)
        if not dest.exists():
            dest.write_bytes(data)
            entries = self._entries()
            entries[rel.as_posix()] = {"repo": str(repo), "blob": dest.name}
            self.sentinel.write_text(json.dumps(entries, indent=1), encoding="utf-8")
        return data

    def _entries(self) -> dict:
        if not self.sentinel.exists():
            return {}
        return json.loads(self.sentinel.read_text(encoding="utf-8"))

    def repos(self) -> set[Path]:
        return {Path(m["repo"]) for m in self._entries().values()}

    def restore(self, verbose: bool = True) -> int:
        """Idempotent. 0 iff every file matches its original AND git says clean."""
        entries = self._entries()
        if not entries and verbose:
            print("[restore] no sentinel; nothing to restore")
        for rel, meta in entries.items():
            repo, blob = Path(meta["repo"]), self.originals / meta["blob"]
            target = repo / rel
            if not blob.exists():
                print(f"[restore] !! ORIGINAL MISSING for {rel} — cannot recover it here")
                return 3
            want = blob.read_bytes()
            if target.read_bytes() != want:
                target.write_bytes(want)
                if verbose:
                    print(f"[restore] rewrote {rel}")
            if target.read_bytes() != want:  # verify the write landed
                print(f"[restore] !! FAILED to restore {rel}")
                return 3
        rc = self.verify(verbose)
        if rc == 0 and entries:
            self.sentinel.unlink()
            shutil.rmtree(self.originals, ignore_errors=True)
            if verbose:
                print("[restore] sentinel cleared")
        return rc

    def verify(self, verbose: bool = True, extra_repos: set[Path] | None = None) -> int:
        """Ask git whether the tree is clean. Never our own bookkeeping."""
        rc = 0
        for repo in self.repos() | (extra_repos or set()):
            out = subprocess.run(
                ["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True
            )
            dirty = [ln for ln in out.stdout.splitlines() if ln.strip()]
            if dirty:
                print(f"[verify] !! {repo} IS DIRTY:")
                for ln in dirty:
                    print("   ", ln)
                rc = 4
            elif verbose:
                print(f"[verify] {repo}: clean")
        return rc


# ──────────────────────────────────────────────────────────────────── mutants
class Mut:
    __slots__ = ("lineno", "col", "kind", "before", "after")

    def __init__(self, lineno: int, col: int, kind: str, before: str, after: str):
        self.lineno, self.col, self.kind = lineno, col, kind
        self.before, self.after = before, after

    def __repr__(self) -> str:
        return f"L{self.lineno} {self.kind}: {self.before!r} -> {self.after!r}"


CMP_FLIP = {
    ast.Eq: ("==", "!="),
    ast.NotEq: ("!=", "=="),
    ast.Lt: ("<", ">="),
    ast.LtE: ("<=", ">"),
    ast.Gt: (">", "<="),
    ast.GtE: (">=", "<"),
    ast.Is: (" is ", " is not "),
    ast.IsNot: (" is not ", " is "),
    ast.In: (" in ", " not in "),
    ast.NotIn: (" not in ", " in "),
}

MUTANT_PREFIX = "MUTANT_"


def _docstring_ids(tree: ast.AST) -> set[int]:
    """Docstrings are guaranteed equivalent mutants — each one burns a full test
    run to learn nothing. Excluded rather than merely tolerated."""
    out: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body:
            first = body[0]
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                out.add(id(first.value))
    return out


def enumerate_mutants(src: str) -> list[Mut]:
    """Deterministic, source-order. Four operators, chosen because they map onto
    the defect shapes we have actually shipped: a renamed config/credential key,
    a flipped guard, a swapped boolean default, an inverted method check."""
    tree = ast.parse(src)
    skip = _docstring_ids(tree)
    muts: list[Mut] = []
    for node in ast.walk(tree):
        if id(node) in skip:
            continue
        if isinstance(node, ast.Constant) and isinstance(node.value, bool):
            muts.append(
                Mut(node.lineno, node.col_offset, "bool", str(node.value), str(not node.value))
            )
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and 1 <= len(node.value) <= 48
            and node.value.strip()
            and "\n" not in node.value
            and node.lineno == node.end_lineno
        ):
            muts.append(
                Mut(node.lineno, node.col_offset, "str", node.value, MUTANT_PREFIX + node.value)
            )
        elif isinstance(node, ast.Compare) and len(node.ops) == 1:
            op = type(node.ops[0])
            if op in CMP_FLIP:
                a, b = CMP_FLIP[op]
                muts.append(Mut(node.lineno, node.col_offset, "cmp", a, b))
        elif isinstance(node, ast.BoolOp):
            a, b = ("and", "or") if isinstance(node.op, ast.And) else ("or", "and")
            muts.append(Mut(node.lineno, node.col_offset, "boolop", a, b))
    muts.sort(key=lambda m: (m.lineno, m.col, m.kind, m.before))
    return muts


def apply_mut(src: str, m: Mut) -> str | None:
    """Textual, anchored to the mutant's own line. Returns None when it cannot be
    applied unambiguously — we skip rather than guess, because a mis-applied
    mutant that still parses would be reported as a survivor and read as a gap."""
    lines = src.splitlines(keepends=True)
    line = lines[m.lineno - 1]
    if m.kind == "str":
        for q in ('"', "'"):
            if q + m.before + q in line:
                lines[m.lineno - 1] = line.replace(q + m.before + q, q + m.after + q, 1)
                return "".join(lines)
        return None
    start = line.find(m.before, max(0, m.col))
    if start < 0:
        start = line.find(m.before)
    if start < 0:
        return None
    lines[m.lineno - 1] = line[:start] + m.after + line[start + len(m.before) :]
    return "".join(lines)


# ───────────────────────────────────────────────────────────────────── driver
def run_tests(
    repo: Path, tests: list[str], timeout: int, k: str | None, python: str
) -> tuple[bool, str]:
    env = dict(os.environ, UV_NO_SYNC="1", PYTHONDONTWRITEBYTECODE="1")
    cmd = [python, "-m", "pytest", *tests, "-x", "-q", "--no-header", "-p", "no:cacheprovider"]
    if k:
        cmd[len(cmd) - 5 : len(cmd) - 5] = ["-k", k]
    try:
        r = subprocess.run(cmd, cwd=repo, capture_output=True, text=True, timeout=timeout, env=env)
        return r.returncode == 0, ((r.stdout or "") + (r.stderr or ""))[-700:]
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--repo", default=".", help="repo root (default: cwd)")
    ap.add_argument("--module", help="source file to mutate, relative to --repo")
    ap.add_argument("--tests", nargs="*", default=[], help="pytest targets to run per mutant")
    ap.add_argument("--k", default=None, help="pytest -k expression")
    ap.add_argument("--lines", default=None, help="restrict mutants to LO-HI")
    ap.add_argument("--limit", type=int, default=40, help="max mutants, sampled evenly")
    ap.add_argument("--kinds", default="bool,str,cmp,boolop")
    ap.add_argument("--timeout", type=int, default=900, help="seconds per pytest run")
    ap.add_argument("--out", default=None, help="write the JSON result here")
    ap.add_argument("--python", default=sys.executable)
    ap.add_argument(
        "--state", default=None, help=f"state dir (default <repo>/{DEFAULT_STATE_DIRNAME})"
    )
    ap.add_argument("--restore", action="store_true", help="recover from a sentinel and exit")
    ap.add_argument("--check", action="store_true", help="verify the tree is clean and exit")
    ap.add_argument("--list", action="store_true", help="print the mutants and exit")
    a = ap.parse_args()

    repo = Path(a.repo).resolve()
    store = Store(Path(a.state) if a.state else repo / DEFAULT_STATE_DIRNAME)
    store.dir.mkdir(parents=True, exist_ok=True)

    if a.restore:
        return store.restore()
    if a.check:
        return store.verify(extra_repos={repo})
    if not a.module:
        ap.error("--module is required unless --restore/--check")

    rel = Path(a.module)
    if a.list:
        for m in enumerate_mutants((repo / rel).read_text(encoding="utf-8")):
            print(m)
        return 0

    # (1) PRE-FLIGHT ---------------------------------------------------------
    if store.sentinel.exists():
        print("[pre-flight] stale sentinel from an earlier run; restoring it first")
        if store.restore() != 0:
            return 5
    st = subprocess.run(["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True)
    if st.stdout.strip():
        print("[pre-flight] REFUSING — the tree is dirty, so a restore could destroy work:")
        print(st.stdout)
        print(f"  (state dir {store.dir} must be gitignored)")
        return 2
    print("[pre-flight] tree clean")

    # (2) SENTINEL BEFORE THE FIRST WRITE ------------------------------------
    original = store.save_original(repo, rel)
    src = original.decode("utf-8")

    kinds = set(a.kinds.split(","))
    muts = [m for m in enumerate_mutants(src) if m.kind in kinds]
    if a.lines:
        lo, _, hi = a.lines.partition("-")
        muts = [m for m in muts if int(lo) <= m.lineno <= int(hi or lo)]
    if len(muts) > a.limit:  # sample evenly rather than truncating to the top of the file
        step = len(muts) / a.limit
        muts = [muts[int(i * step)] for i in range(a.limit)]

    print(f"[plan] {rel.as_posix()}: {len(muts)} mutants; tests={a.tests or '(none)'}")

    t0 = time.monotonic()
    ok, tail = run_tests(repo, a.tests, a.timeout, a.k, a.python)
    baseline = time.monotonic() - t0
    if not ok:
        print(f"[baseline] THE TESTS ARE ALREADY RED — aborting; nothing measured here\n{tail}")
        store.restore()
        return 6
    print(f"[baseline] green in {baseline:.1f}s")

    survived: list[Mut] = []
    killed = skipped = 0
    try:
        for n, m in enumerate(muts, 1):
            mutated = apply_mut(src, m)
            if mutated is None or mutated == src:
                skipped += 1
                continue
            (repo / rel).write_text(mutated, encoding="utf-8", newline="")
            ok, _ = run_tests(repo, a.tests, a.timeout, a.k, a.python)
            (repo / rel).write_bytes(original)
            if ok:
                survived.append(m)
            else:
                killed += 1
            print(f"[{n:3d}/{len(muts)}] {'SURVIVED' if ok else 'killed  '}  {m}", flush=True)
    finally:
        # Belt. The braces are the sentinel: if this never runs, --restore does.
        rc = store.restore()

    applicable = killed + len(survived)
    result = {
        "module": rel.as_posix(),
        "tests": a.tests,
        "k": a.k,
        "baseline_seconds": round(baseline, 1),
        "mutants": len(muts),
        "applicable": applicable,
        "killed": killed,
        "skipped": skipped,
        "score": round(killed / applicable, 3) if applicable else None,
        "survived": [str(m) for m in survived],
        "restore_rc": rc,
    }
    print(json.dumps(result, indent=1))
    if a.out:
        Path(a.out).write_text(json.dumps(result, indent=1), encoding="utf-8")
    return rc


if __name__ == "__main__":
    sys.exit(main())
