#!/usr/bin/env python
"""Run a chosen suite with the N+1 legacy reads deleted — `SPEC_PII_SEPARATION` §8a.3.

    python scripts/n1_mutation_probe.py tests/test_services/test_user_service.py ...
    python scripts/n1_mutation_probe.py --restore        # standalone recovery

**What this is for.** core#939's N+1 release deletes the legacy halves of three `or_`
reads plus one fallback. Fixtures that build a `User` or an `Invitation` by hand produce
rows those reads can no longer see, so the release's real gate is *"the suite is green
with the legacy reads gone"*. This applies that mutation, runs the tests you name, and
puts the source back.

🚨 **It is deliberately WIDER than §8a.3's original run, and that gap is the point.**
That run mutated only #939 items 1 and 2 — the two clauses in `user_service.py` — and
reported *18 failures*, a number since quoted as the blast radius. N+1 also deletes item
6 (`invitation_service.get_invitation_by_token`) and the legacy fallback in
`accept_invitation` (`:141`, marked but absent from #939's item list — §8a.8). A probe
narrower than the release under-reports, and its count then reads as a total.

⚠️ **A green here is weaker evidence than it looks, and §8a.9 says why:** an uncovered
path contributes zero reds however broken it is, and a test can go *vacuous* rather than
red. Measured on `test_org_role_authority.py`'s
`test_accepting_a_stored_owner_invitation_creates_no_owner`:
under this mutation it kept passing because the invitation was not found **at all**, so
an assertion written to prove the owner-role guard proved nothing and nothing went red.
Pair this instrument with `tests/test_pii_fixture_invariant.py`, which asks the question
over *source* and does not depend on anything being exercised.

Items 3/4/5/7 are write-side and cannot make an existing fixture invisible, so they are
out of scope here on purpose.

────────────────────────────────────────────────────────────────────────────────
SAFETY — inherited from `scripts/mutation_probe.py`, not reimplemented
────────────────────────────────────────────────────────────────────────────────
Pristine bytes are written to `.n1-mutation-probe-state/` **before** any source is
touched, so `--restore` recovers with no surviving process — a two-minute tool timeout
once left a mutated constant sitting in `datanika/` because a `finally:` never ran.

One deliberate difference from that probe: it refuses to start on a dirty tree and
verifies with a repo-wide `git status`. This one **must** run on a working branch (Step A
is measured by mutating a tree that has uncommitted fixture changes), so its post-restore
check is scoped with `git status --porcelain -- <the mutated files>`. Same guarantee about
the files it touched; no false alarm about the files it did not. A check that fires on
legitimate work is one people learn to ignore.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mutation_probe import Store, clean_env  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".n1-mutation-probe-state"

USER_SVC = Path("datanika/services/user_service.py")
INV_SVC = Path("datanika/services/invitation_service.py")

#: Read-side clauses core#939 deletes, as the files spell them. Deleted outright.
DELETIONS: dict[Path, list[str]] = {
    USER_SVC: [
        # item 1 — get_user_by_email
        "                    func.lower(User.email) == email,  # legacy half — removed in N+1\n",
        # item 2 — find_or_create_oauth_user
        "                        User.oauth_provider_id == oauth_provider_id,"
        "  # legacy — gone in N+1\n",
    ],
    INV_SVC: [
        # item 6 — get_invitation_by_token
        "                    Invitation.token == token,  # legacy — removed in N+1\n",
        # core#1010 (PR #1018, merged 2026-09-03T20:19:30Z) added a FIFTH deletion
        # site after §8a.8 was written: `_has_active_membership` carries its own
        # legacy `users.email` half. Not ambiguous with USER_SVC's — 24 spaces of
        # indent against 20, and each anchor is scoped to its own file — but N+1
        # deletes both, so a probe that deletes one is narrower than the release
        # again, which is the defect this module's docstring exists to warn about.
        #
        # 🚨 MEASURED, and it is the OPPOSITE of what was predicted. The
        # handoff expected the un-widened probe to report "a red that is not a
        # defect". It does not: without this clause
        # `test_invitation_service.py` returns **13 passed**, because the one test
        # that detects the coupling needs the clause GONE to fail. So the narrow
        # probe was silent, not noisy — a false green, which is the worse error and
        # the one that would have argued for narrowing it back.
        "                        func.lower(User.email) == email,"
        "  # legacy half — removed in N+1\n",
    ],
}

#: `accept_invitation`'s fallback is a substitution, not a deletion — dropping the line
#: would be a SyntaxError, so the N+1 shape is the sidecar read standing alone.
SUBSTITUTIONS: dict[Path, list[tuple[str, str]]] = {
    INV_SVC: [
        (
            "        invited_email = pii.email if pii is not None else invitation.email\n",
            "        invited_email = pii.email if pii is not None else None\n",
        ),
    ],
}


def _decode(raw: bytes) -> tuple[str, bool]:
    """Text plus whether the file is CRLF.

    Read and written as **bytes**: a text-mode round trip translates line endings, an
    in-process equality check cannot see it because both sides are already translated,
    and the tree is left dirty across every file the harness touched.
    """
    return raw.decode("utf-8").replace("\r\n", "\n"), b"\r\n" in raw


def _encode(text: str, crlf: bool) -> bytes:
    return (text.replace("\n", "\r\n") if crlf else text).encode("utf-8")


def _scoped_status(paths: list[Path]) -> list[str]:
    out = subprocess.run(
        ["git", "status", "--porcelain", "--", *[p.as_posix() for p in paths]],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=clean_env(),
    )
    return [ln for ln in out.stdout.splitlines() if ln.strip() and not ln.startswith("??")]


def main(argv: list[str]) -> int:
    store = Store(STATE)
    if "--restore" in argv:
        return store.restore_scoped()

    targets = [a for a in argv if not a.startswith("-")]
    if not targets:
        print(__doc__)
        return 2

    # A previous run may have been killed. Recover before mutating anything further.
    if store.sentinel.exists():
        print("[n1] a sentinel from an earlier run exists — restoring first")
        if store.restore_scoped() != 0:
            return 3

    files = sorted({*DELETIONS, *SUBSTITUTIONS}, key=lambda p: p.as_posix())
    plan: dict[Path, tuple[bytes, bytes]] = {}
    armed = 0

    # ── arm: every clause must match exactly once, or the run measures nothing ──
    for rel in files:
        text, crlf = _decode((ROOT / rel).read_bytes())
        mutated = text
        for clause in DELETIONS.get(rel, []):
            n = text.count(clause)
            if n != 1:
                print(f"REFUSING TO RUN: {rel.name} clause matched {n} times, expected 1:")
                print(f"  {clause!r}")
                return 3
            mutated = mutated.replace(clause, "")
            armed += 1
        for old, new in SUBSTITUTIONS.get(rel, []):
            n = text.count(old)
            if n != 1:
                print(f"REFUSING TO RUN: {rel.name} substitution matched {n} times, expected 1:")
                print(f"  {old!r}")
                return 3
            mutated = mutated.replace(old, new)
            armed += 1
        if mutated == text:
            print(f"REFUSING TO RUN: mutation was a no-op in {rel.name}")
            return 3
        plan[rel] = (_encode(text, crlf), _encode(mutated, crlf))
    print(f"[n1] armed: {armed} clauses matched exactly once each across {len(files)} files")

    rc = 1
    try:
        for rel, (_original, mutant) in plan.items():
            store.save_original(ROOT, rel)  # pristine bytes on disk BEFORE the write
            (ROOT / rel).write_bytes(mutant)
        # Arm again against the artifact pytest will import, not against our own plan.
        for rel in files:
            on_disk, _ = _decode((ROOT / rel).read_bytes())
            for clause in DELETIONS.get(rel, []):
                assert clause not in on_disk, f"{rel.name}: mutation did not reach the disk"
            for old, _new in SUBSTITUTIONS.get(rel, []):
                assert old not in on_disk, f"{rel.name}: substitution did not reach the disk"
        print("[n1] mutation on disk; running pytest\n")
        rc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:randomly", *targets],
            cwd=str(ROOT),
            env=clean_env(UV_NO_SYNC="1", PYTHONIOENCODING="utf-8"),
        ).returncode
    finally:
        restore_rc = store.restore_scoped()
    return rc or restore_rc


def _restore_scoped(self: Store) -> int:
    """`Store.restore` with the dirty check scoped to the files this probe mutated.

    Bolted on rather than forked: the byte-level recovery in `Store.restore` is the
    part that matters and is already proven by `tests/test_scripts/test_mutation_probe.py`
    (which kills a real run with `TerminateProcess` and recovers from the sentinel alone).
    Only the *verification* differs, and only because this probe runs on a working branch.
    """
    entries = self._entries()
    if not entries:
        print("[n1] no sentinel; nothing to restore")
        return 0
    rels = [Path(rel) for rel in entries]
    for rel, meta in entries.items():
        blob = self.originals / meta["blob"]
        target = Path(meta["repo"]) / rel
        if not blob.exists():
            print(f"[n1] !! ORIGINAL MISSING for {rel} — cannot recover it here")
            return 3
        want = blob.read_bytes()
        if target.read_bytes() != want:
            target.write_bytes(want)
        if target.read_bytes() != want:
            print(f"[n1] !! FAILED to restore {rel}")
            return 3
    dirty = _scoped_status(rels)
    print(f"[n1] restore: git status on the mutated files -> {dirty or 'CLEAN'}")
    if dirty:
        print("[n1] !! NOT CLEAN — fix before trusting anything above")
        return 4
    self.sentinel.unlink()
    for blob in self.originals.glob("*"):
        blob.unlink()
    self.originals.rmdir()
    return 0


Store.restore_scoped = _restore_scoped  # type: ignore[attr-defined]


if __name__ == "__main__":
    os.chdir(ROOT)
    raise SystemExit(main(sys.argv[1:]))
