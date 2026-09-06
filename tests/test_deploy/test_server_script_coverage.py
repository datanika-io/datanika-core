"""Every file in `deploy/server/` must be installed by some deploy step — or say why not.

This is `test_deploy_service_coverage.py`'s question asked of a second surface (core#747).
That file asks *"is this compose service named by some deploy step?"*; this one asks *"is
this box-side file put on the box by some deploy step?"* — and until now the answer for
`deploy/server/` was **no, for every file in it**.

Why the gap was invisible
-------------------------
The deploy tarball ships the whole tree to `/opt/datanika/datanika/deploy/server/`. That is
worse than not shipping it: the correct content sits on the box at a path nothing reads,
beside the stale copy cron actually runs. Measured 2026-09-04 — `backup-offsite.sh` on the
box was `874c8a3c…` while `origin/master` had `58baca17…`. core#1017 was merged, promoted,
and **not running**, and every signal said it had shipped.

The README's table hid it in a way worth recording: its "Applied by" column meant two things
at once. For `backup-offsite.sh` it read *"cron, 03:00"* — which names what **runs** the
copy and says nothing about what **puts it there**. Read quickly, that is a column that
looks filled in.

Derived, not restated
---------------------
The installed set is parsed out of `scripts/install-server-scripts.sh` rather than written
here, for the reason `test_deploy_service_coverage.py` gives: a restated list drifts, and
drift is the bug being hunted. Add a file to `deploy/server/` and this test fails until
either the installer names it or `NOT_INSTALLED` explains it.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVER_DIR = ROOT / "deploy" / "server"
APACHE_DIR = ROOT / "deploy" / "apache"
INSTALLER = ROOT / "scripts" / "install-server-scripts.sh"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-pointer.yml"
GITATTRIBUTES = ROOT / ".gitattributes"

# `NAME=( a b c )` across however many lines, up to the closing paren.
_ARRAY = re.compile(r"^(INSTALL|INSTALL_DATA)=\(\s*(.*?)^\)", re.M | re.S)


# Files deliberately present in `deploy/server/` and deliberately NOT installed from it.
#
# ⚠️ An entry here is a claim that needs a reason, not a way to silence the test.
NOT_INSTALLED: dict[str, str] = {
    "README.md": "Documentation for this directory. Never installed anywhere.",
    "deploy-pointer.sh": (
        "A dev-machine fallback for when CD itself is broken. It runs FROM the workstation "
        "and SSHes to the box; installing it onto the box would be meaningless."
    ),
    "networkd-99-datanika-dns.conf": (
        "Hand-applied on purpose, and the one file here that must stay that way. A pushed "
        "network configuration that fails leaves the box unreachable with no way in to "
        "revert it, so this trades automation for a rollback that always works "
        "(`rm -rf /etc/systemd/network/10-netplan-eth0.network.d && networkctl reload`)."
    ),
    "staging-docker-compose.yml": (
        "Installed by the staging deploy in ci.yml to /opt/datanika-staging/, not by the "
        "prod deploy, and not into /opt/datanika/scripts/."
    ),
    "apache-prod-active-ports.conf": (
        "A snapshot of a value that ALTERNATES. deploy-bluegreen.sh rewrites the live file "
        "on every swap; this copy is here for its shape, never as a statement about which "
        "colour is serving. Installing it would overwrite the swap's own output."
    ),
    # core#745. These two are byte-identical DUPLICATES of deploy/apache/, which is the
    # directory sync-vhosts.sh actually reads. They are pinned identical below rather than
    # merely excused, so the day somebody edits the copy that does nothing, CI says so.
    "apache-app.datanika.io.conf": (
        "core#745: a duplicate of deploy/apache/app.datanika.io.conf, which is what "
        "sync-vhosts.sh installs. Pinned byte-identical by "
        "test_the_apache_duplicates_have_not_drifted."
    ),
    "apache-staging-app.datanika.io.conf": (
        "core#745: a duplicate of deploy/apache/staging-app.datanika.io.conf. Same pin."
    ),
}

# core#745's duplicate pairs: the decoy in deploy/server/ -> the real one in deploy/apache/.
APACHE_DUPLICATES = {
    "apache-app.datanika.io.conf": "app.datanika.io.conf",
    "apache-staging-app.datanika.io.conf": "staging-app.datanika.io.conf",
}


# Files in deploy/server/ or deploy/apache/ that need no `eol=lf` rule (core#1076).
#
# ⚠️ Same bar as NOT_INSTALLED above: an entry here is a claim that needs a reason. The
# reason has to be "this file never reaches a Linux machine", not "adding the rule was
# inconvenient" — everything else in these two directories is installed onto the box,
# byte-compared there by CD, or both.
EOL_EXEMPT: dict[str, str] = {
    "README.md": (
        "Documentation for the directory it sits in. Listed in NOT_INSTALLED, never "
        "shipped, never byte-compared — the only file in either directory that no machine "
        "outside a developer's checkout ever reads."
    ),
}


def _installer_text() -> str:
    return INSTALLER.read_text(encoding="utf-8")


# --------------------------------------------------------------------------------------
# core#1089: ask git for the effective attribute state, never reconstruct it.
#
# The predecessor of this block scanned `.gitattributes` for lines *containing* `eol=lf`
# and then reimplemented gitattributes matching with `PurePosixPath.match`. It modelled
# neither of the two rules that decide the answer:
#
#   * matching is **last-match-wins**, per attribute;
#   * `-text` **unsets** `text`, and a rule that unsets it contains no `eol=lf` to scan for.
#
# So appending one line — `deploy/server/*.conf -text` — removed the pinning while the
# guard stayed green. QA measured exactly that (core#1089).
#
# 🚨 And the obvious repair reproduces the defect. `git check-attr eol` still answers `lf`
# after a `-text` append, because `eol` keeps the value the earlier rule gave it; it is
# simply **inert**, since git only applies the conversion to files it treats as text. A
# predicate of `eol == "lf"` therefore shells out to git and gets the same wrong answer.
# `test_the_pinning_notices_a_later_rule_that_unsets_text` pins both halves in-suite so
# nobody re-simplifies this back.
# --------------------------------------------------------------------------------------


def _git(args: list[str], cwd: Path) -> bytes:
    """Run git and INSIST it answered.

    A git that failed and a git that found nothing must never look alike: an empty stdout
    parsed by a lenient caller is how `grep -c` on a mangled ref prints a clean `0`
    (WORKFLOW_RULES §13 trap 13b). Anything other than a clean exit raises, so a broken
    invocation reads as NO-VERDICT — a red — and never as a pass.
    """
    proc = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed in {cwd} (rc={proc.returncode}): "
            f"{proc.stderr.decode('utf-8', 'replace').strip()}"
        )
    return proc.stdout


def git_check_attr(paths: list[str], cwd: Path) -> dict[str, dict[str, str]]:
    """`git check-attr -z text eol -- <paths>` → `{path: {"text": ..., "eol": ...}}`.

    `-z` because the human format is `<path>: <attr>: <value>` and a path may contain a
    colon. Decoded explicitly rather than with `text=True`: the locale codec on the dev
    machine is cp1251 and would silently mis-decode any non-ASCII byte
    (WORKFLOW_RULES §7).

    Note the paths need not exist — `check-attr` answers by pattern, which is what lets
    the arming test below run against a repository holding nothing but `.gitattributes`.
    """
    if not paths:
        raise RuntimeError("git_check_attr called with no paths — that is a vacuous read")
    raw = _git(["check-attr", "-z", "text", "eol", "--", *paths], cwd)
    fields = raw.decode("utf-8").split("\0")
    if fields and fields[-1] == "":
        fields.pop()
    if not fields or len(fields) % 3:
        raise RuntimeError(
            f"git check-attr -z returned {len(fields)} fields for {len(paths)} path(s); "
            f"expected a multiple of 3 (<path>, <attr>, <value>)"
        )
    out: dict[str, dict[str, str]] = {}
    for i in range(0, len(fields), 3):
        out.setdefault(fields[i], {})[fields[i + 1]] = fields[i + 2]
    return out


def is_pinned_to_lf(attrs: dict[str, str]) -> bool:
    """Is this path's effective attribute state a real LF pin?

    **Both halves, and that is the whole point of core#1089.** `text: set` without
    `eol: lf` normalises on commit but lets `core.autocrlf` decide the checkout;
    `eol: lf` without `text: set` is inert, and is precisely the state a `-text` append
    leaves behind.
    """
    return attrs.get("text") == "set" and attrs.get("eol") == "lf"


def _deployed_files() -> list[Path]:
    """Every file in deploy/server/ + deploy/apache/ that is not EOL-exempt."""
    found: list[Path] = []
    for directory in (SERVER_DIR, APACHE_DIR):
        for path in sorted(directory.iterdir()):
            if path.is_file() and path.name not in EOL_EXEMPT:
                found.append(path)
    return found


@pytest.fixture(scope="module")
def installed_names() -> set[str]:
    """Filenames the installer names, parsed from its own INSTALL / INSTALL_DATA arrays."""
    found: set[str] = set()
    for _name, body in _ARRAY.findall(_installer_text()):
        for line in body.split("\n"):
            token = line.split("#", 1)[0].strip()
            if token:
                found.add(token)
    return found


@pytest.fixture(scope="module")
def server_files() -> set[str]:
    return {p.name for p in SERVER_DIR.iterdir() if p.is_file()}


def test_the_parser_actually_found_something(installed_names, server_files):
    """A parser that silently matches nothing turns this file green forever.

    The installer's array syntax can be reformatted; if `_ARRAY` stops matching, every
    assertion below passes vacuously — the exact silent-green failure this suite exists to
    prevent. So assert on the parse itself, and name a file we know must be there.
    """
    assert INSTALLER.exists(), f"{INSTALLER} is missing — the installer is the whole fix"
    assert len(installed_names) >= 4, (
        f"the INSTALL/INSTALL_DATA parser matched {sorted(installed_names)}; it is reading "
        f"{INSTALLER.name} and should find at least 4 entries"
    )
    assert "backup-offsite.sh" in installed_names, (
        "the installer no longer names backup-offsite.sh — that is the file whose stale copy "
        "on the box motivated core#747 in the first place"
    )
    assert len(server_files) >= 8, sorted(server_files)


def test_every_server_file_is_installed_or_explicitly_exempt(installed_names, server_files):
    orphans = sorted(server_files - installed_names - set(NOT_INSTALLED))
    assert not orphans, (
        "these files live in deploy/server/ and are installed by NO deploy step:\n"
        + "".join(f"    {f}\n" for f in orphans)
        + "\nThe deploy ships this directory to /opt/datanika/datanika/deploy/server/, "
        "where nothing reads it. If the file belongs on the box, add it to INSTALL (or "
        "INSTALL_DATA) in scripts/install-server-scripts.sh. If it genuinely must not be "
        "CD-installed, add it to NOT_INSTALLED here WITH THE REASON.\n\n"
        "core#747: backup-offsite.sh sat like this. core#1017's fix was merged, promoted, "
        "and never ran, while the corrected file sat on the box at a path nothing reads."
    )


def test_the_installer_names_no_file_that_does_not_exist(installed_names, server_files):
    """The other direction: the installer aborts on a missing file, mid-deploy, in prod."""
    unknown = sorted(installed_names - server_files)
    assert not unknown, (
        "the installer lists files that are not in deploy/server/:\n"
        + "".join(f"    {f}\n" for f in unknown)
        + "\nThis fails the deploy on the box rather than here, which is the expensive place."
    )


def test_no_exemption_names_a_file_that_is_gone(server_files):
    """A stale excuse is worse than none — it reads as a decision somebody made."""
    stale = sorted(set(NOT_INSTALLED) - server_files)
    assert not stale, (
        "NOT_INSTALLED names files deploy/server/ no longer holds: "
        f"{stale}. Delete the entries rather than carrying a stale excuse."
    )


def test_the_apache_duplicates_have_not_drifted():
    """core#745: deploy/server/ holds copies of vhosts sync-vhosts.sh does NOT read.

    `sync-vhosts.sh` reads `deploy/apache/`. The two vhosts in `deploy/server/` are
    duplicates that no workflow applies, sitting under a README that used to claim
    sync-vhosts.sh applied them. They are identical today, which is exactly what makes them
    harmless *now* and dangerous later: edit the copy in `deploy/server/` and the change is
    real, reviewed, merged, deployed — and has no effect on any vhost.

    Pinning them byte-identical converts that latent decoy into one that announces itself.

    ⚠️ Line endings are normalised before comparing, and that is core#1076 (measured
    2026-09-04, after the `.gitattributes` fix shipped). The first version of this test
    compared raw working-tree bytes, which on Windows is **not a property of the repository
    at all** — it is a property of when each file was last written to disk. `core.autocrlf`
    is `true` machine-wide here, so a file checked out before its `text eol=lf` rule existed
    stays CRLF, and one checked out after is LF. The two paths below are the *same git
    object* (`i/lf` on both, identical blob OIDs) and the test still failed, refusing every
    department's push while passing in CI, where Linux materialises both as LF.

    🔑 `21e6b01c` added the missing `deploy/server/*.conf text eol=lf` rule and is correct
    and necessary — but it does **not** fix an existing checkout, and cannot. With `text`
    set, git's filter normalises on read, so a CRLF working copy compares equal to the index
    and reads as *unmodified*; no checkout, pull or branch switch will ever re-materialise
    it. Verified: switching to `origin/master` and back across the fix left
    `deploy/server/apache-app.datanika.io.conf` at `w/crlf` beside `deploy/apache/` at
    `w/lf`. The only thing that clears it is `rm` + `git checkout -- deploy/server/`.

    So the byte comparison had two possible reds and only one of them was drift. Content
    equality is the property this test names, and it is what is compared now;
    `test_the_deployed_conf_files_are_pinned_to_lf` below owns the line-ending half, at the
    layer that actually decides it.
    """
    # Anti-vacuity (core#1089 AC4, carried from the closed PR #1082): an empty or emptied
    # APACHE_DUPLICATES makes the loop below assert nothing while still reporting green,
    # and it is also the floor the pinning guard's own walk is measured against.
    assert len(APACHE_DUPLICATES) >= 2, (
        f"APACHE_DUPLICATES holds {len(APACHE_DUPLICATES)} pair(s). core#745 has two decoy "
        f"vhosts; fewer means either one was deleted (resolve #745 and delete this test) or "
        f"the mapping was truncated, in which case this test compares nothing."
    )

    for decoy, real in APACHE_DUPLICATES.items():
        a = (SERVER_DIR / decoy).read_bytes().replace(b"\r\n", b"\n")
        b = (APACHE_DIR / real).read_bytes().replace(b"\r\n", b"\n")
        assert a == b, (
            f"deploy/server/{decoy} has drifted from deploy/apache/{real}.\n"
            f"Only deploy/apache/ is read by scripts/sync-vhosts.sh, so whichever of these "
            f"you just edited, the one that reaches Apache is deploy/apache/{real}.\n"
            f"Resolve core#745 by deleting the deploy/server/ copy, or re-sync the two.\n"
            f"(Line endings are normalised before this comparison, so a CRLF checkout is "
            f"NOT what you are looking at — this is a real content difference.)"
        )


def test_the_deployed_conf_files_are_pinned_to_lf():
    """core#1076: every file here is deployed to a Linux box, so it must be `eol=lf`.

    This is the guard that would have caught #1076 in the first place, and it is deliberately
    at a different layer from the drift test above. That one compares *content* and is now
    immune to line endings; this one asserts the **cause** — that `.gitattributes` pins each
    deployed file to LF — so the two cannot both be satisfied by the same mistake.

    Why it matters beyond tidiness: `sync-vhosts.sh` byte-compares on the box before
    reloading Apache, and Infra hand-installs the rest of `deploy/server/`. A file committed
    with CRLF makes every deploy see a spurious change; a shell script or a systemd-networkd
    unit with CRLF can fail outright on Linux. `.gitattributes` already carried the rule and
    the comment explaining it for `deploy/apache/`, and simply did not carry it one directory
    over — which is how the rule reached some of the tree and not the rest.

    The set is derived from what is on disk, not restated, so a new deployed file is covered
    the day it is added rather than the day someone remembers this test.

    🚨 **What this asserts is the state git resolves, not the rules the file appears to
    declare** (core#1089). It used to scan `.gitattributes` for `eol=lf` substrings and
    re-implement the matching; appending `deploy/server/*.conf -text` then removed the
    pinning with this test still green, because `-text` carries no `eol=lf` to find and
    last-match-wins was never modelled. `git check-attr` answers the question that is
    actually being asked. See the block above `_git` for why `eol` alone is not the
    predicate.
    """
    paths = _deployed_files()

    # Anti-vacuity, and it comes FIRST: a walk that finds nothing would otherwise pass by
    # having nothing to check. Both directories have held files since core#747.
    assert len(paths) >= len(APACHE_DUPLICATES) * 2, (
        f"only {len(paths)} files walked across deploy/server/ + deploy/apache/ — this test "
        f"cannot have checked the {len(APACHE_DUPLICATES)} pinned vhost pairs"
    )

    rels = [p.relative_to(ROOT).as_posix() for p in paths]
    resolved = git_check_attr(rels, ROOT)

    unpinned = {
        rel: resolved.get(rel, {}) for rel in rels if not is_pinned_to_lf(resolved.get(rel, {}))
    }
    assert not unpinned, (
        "These files are deployed to a Linux box and git does not resolve them to a real "
        f"LF pin (core#1076, core#1089): {unpinned}\n"
        "A real pin is BOTH `text: set` and `eol: lf`. `text: unset` with `eol: lf` is the "
        "shape a later `-text` rule leaves behind — the eol value survives and is inert, "
        "because git only converts files it treats as text.\n"
        "Fix by adding or restoring a `<dir>/*.<ext> text eol=lf` rule, and remember git "
        "applies the LAST matching rule per attribute — a narrower rule added below wins."
    )


def _repo_from_real_gitattributes(tmp_path: Path, extra: str = "") -> Path:
    """A throwaway git repo carrying THIS repo's `.gitattributes` bytes, plus `extra`.

    Two reasons it is a copy of the real file rather than a fixture written here:

    1. **A synthetic rule set agrees with the check including where the check is wrong.**
       The bytes come out of the artifact under test, so the arming below exercises the
       rules we actually ship.
    2. It is the only way to arm this without mutating a tree five departments share.
       core PR #1085 is the standing reason: a harness that "restored" `.gitattributes`
       with `git checkout --` threw away its author's own uncommitted rule, and only a
       post-restore control noticed. Nothing here writes to the real tree at all.

    The repo holds no files. `git check-attr` answers by pattern, so it does not need any.
    """
    repo = tmp_path / "probe"
    repo.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repo)], capture_output=True, check=True)
    (repo / ".gitattributes").write_bytes(GITATTRIBUTES.read_bytes() + extra.encode("utf-8"))
    return repo


def test_the_pinning_notices_a_later_rule_that_unsets_text(tmp_path):
    """core#1089's arming: the guard must fail when someone UNPINS, not merely pass today.

    ⚠️ The control is deliberately an **append**, not a deletion. Deleting the `eol=lf`
    line reds under the old string-scanning implementation *and* under this one, so it
    cannot tell them apart and would have certified the broken guard. The append is also
    the realistic regression: nobody deletes a rule with a comment above it explaining why
    it exists — they add a narrower one underneath and do not know it wins.

    Three assertions, and the middle one is the reason this test exists rather than a
    comment:

    * the unmutated copy resolves to a real pin — a positive control, and simultaneously
      the proof that the pin lives in the **tracked** `.gitattributes` rather than in
      somebody's `.git/info/attributes` or a global `core.attributesFile`, since this
      throwaway repo has neither;
    * `-text` leaves `text: unset` **with `eol` still reading `lf`** — the exact trap that
      makes `eol == "lf"` a wrong predicate;
    * and `is_pinned_to_lf` therefore rejects it.
    """
    probe = "deploy/server/apache-app.datanika.io.conf"

    clean = git_check_attr([probe], _repo_from_real_gitattributes(tmp_path / "a"))[probe]
    assert is_pinned_to_lf(clean), (
        f"positive control failed: this repo's own .gitattributes does not pin {probe} "
        f"when read in isolation — it resolved to {clean}. Either the rule has been "
        f"removed, or the pin only appears to exist because of a LOCAL attributes file "
        f"that CI and every other checkout will not have."
    )

    unpinned = git_check_attr(
        [probe], _repo_from_real_gitattributes(tmp_path / "b", "\ndeploy/server/*.conf -text\n")
    )[probe]

    assert unpinned.get("eol") == "lf", (
        "the arming premise no longer holds: after `-text`, `eol` is expected to survive "
        f"as `lf` and it read {unpinned!r}. If git has changed this, the warning against "
        "an `eol`-only predicate needs re-deriving before it is trusted."
    )
    assert unpinned.get("text") == "unset", (
        f"`-text` did not unset `text` — it resolved to {unpinned!r}, so this control is "
        f"no longer reproducing core#1089's mutation and proves nothing."
    )
    assert not is_pinned_to_lf(unpinned), (
        "the pinning predicate accepts a state where `text` is unset. That is core#1089 "
        "verbatim: the eol value is still `lf` and is inert, so the file is no longer "
        "pinned and this guard would not say so."
    )


def test_an_eol_exempt_file_stays_out_of_the_pinning_requirement():
    """AC3: the exemption must keep working, or the guard gets loosened on first contact.

    A binary asset or a Windows-only file legitimately needs no LF pin. If exempting one
    were impossible, the next person to add such a file would widen the predicate instead
    — which is how a guard stops guarding. So: every `EOL_EXEMPT` name really is present
    (a stale exemption is a claim about a file that is gone), it really is excluded from
    the walk, and it really would fail the predicate if it were not — otherwise the
    exemption is doing nothing and the permissive path has never been exercised.
    """
    assert EOL_EXEMPT, "no exemptions left — delete this test rather than let it pass vacuously"
    walked = {p.relative_to(ROOT).as_posix() for p in _deployed_files()}

    for name, reason in EOL_EXEMPT.items():
        assert reason.strip(), f"EOL_EXEMPT[{name}] carries no reason"
        present = [d / name for d in (SERVER_DIR, APACHE_DIR) if (d / name).is_file()]
        assert present, (
            f"EOL_EXEMPT names {name}, which is in neither deploy/server/ nor "
            f"deploy/apache/. A stale exemption silently widens the next real gap."
        )
        for path in present:
            rel = path.relative_to(ROOT).as_posix()
            assert rel not in walked, f"{rel} is exempt yet still walked by the pinning guard"
            attrs = git_check_attr([rel], ROOT)[rel]
            assert not is_pinned_to_lf(attrs), (
                f"{rel} is exempt but git already pins it ({attrs}). The exemption is "
                f"then untested: drop it from EOL_EXEMPT so the guard covers the file."
            )


def test_the_deploy_workflow_invokes_the_installer():
    """An installer nothing runs is core#747 one level up, and looks identical to a fix.

    This is the assertion that matters most in the file. Everything above checks the
    installer's *contents*; a correct installer that no workflow invokes leaves the box
    exactly as stale as before, with a script in the repo that reads like the problem was
    solved.
    """
    text = DEPLOY_WORKFLOW.read_text(encoding="utf-8")
    # Strip comment lines first: this workflow documents itself at length and quotes the
    # very commands being searched for (test_deploy_service_coverage.py's `won` incident).
    body = "\n".join(
        line.split(" # ", 1)[0] for line in text.split("\n") if not line.lstrip().startswith("#")
    )
    assert "install-server-scripts.sh" in body, (
        "deploy-pointer.yml does not invoke scripts/install-server-scripts.sh, so nothing "
        "installs deploy/server/ onto the box. The installer existing is not the fix — "
        "core#747 is precisely a correct file that no deploy step runs."
    )


def test_the_installer_refuses_a_truncated_list():
    """The installer must fail closed when its own list is emptied.

    An install step that installs nothing and exits 0 is the failure this whole file is
    about, wearing a green tick. The script carries a floor; assert it is still there, and
    that the floor is not trivially satisfiable.
    """
    text = _installer_text()
    assert re.search(r'\[\s*"\$\{installed\}"\s*-ge\s*([1-9]\d*)\s*\]', text), (
        "scripts/install-server-scripts.sh no longer asserts a minimum number of installed "
        "files. Without that floor, emptying INSTALL makes the deploy step a silent no-op "
        "that reports success forever."
    )
    floor = int(re.search(r'\[\s*"\$\{installed\}"\s*-ge\s*([1-9]\d*)\s*\]', text).group(1))
    assert floor >= 5, f"the installed-count floor is {floor}; it must cover the real list"


# ── core#1117 ────────────────────────────────────────────────────────────────────────
# A fix that ships while its own file still documents the pre-fix world is not finished.
#
# core#747 landed the installer on 2026-09-04 and closed. Four scripts in deploy/server/
# kept a banner reading "NOTHING DEPLOYS THIS FILE … after changing it, install it and
# compare sha256 against git" — an instruction that is now actively harmful: it tells the
# next agent to hand-copy a root-owned cron script over the file the deploy manages and
# hash-verifies, reintroducing the drift #747 closed, while they believe they are following
# a written procedure. It survived the fix because the fix touched a *different* file, and
# the banner sits on the file most likely to be read by whoever edits these next.
#
# 🔑 The predicate is POSITIVE — "each installed script's header names its installer" —
# never "must not contain 'NOTHING DEPLOYS'". A negative form would go red on a corrected
# banner that *explains* the old wording, which is exactly what these four now do.

HEADER_LINES = 40
INSTALLER_NAME = "install-server-scripts.sh"


def test_every_installed_script_header_names_its_installer(installed_names):
    """An installed file must not claim it is uninstalled (core#1117 AC4)."""
    checked: list[str] = []
    silent: list[str] = []
    for name in sorted(installed_names):
        if not name.endswith(".sh"):
            continue  # INSTALL_DATA carries backup-pubkey.asc, which has no comment header
        path = SERVER_DIR / name
        if not path.is_file():
            continue  # test_the_installer_names_no_file_that_does_not_exist owns that case
        checked.append(name)
        header = "\n".join(path.read_text(encoding="utf-8").splitlines()[:HEADER_LINES])
        if INSTALLER_NAME not in header:
            silent.append(name)

    assert len(checked) >= 4, (
        f"only {checked} were scanned. This guard reads the INSTALL/INSTALL_DATA arrays, so "
        f"a parser change makes it pass over an empty set — the silent green this file exists "
        f"to prevent."
    )
    assert not silent, (
        "these scripts are installed by the deploy, and their first "
        f"{HEADER_LINES} lines never mention {INSTALLER_NAME}:\n"
        + "".join(f"    {n}\n" for n in silent)
        + "\nA reader editing one of them has no way to learn that the deploy installs it, "
        "and the banner they replace it with will say what the old ones said: hand-install "
        "and compare hashes. Say in the header that the deploy installs the file and "
        "asserts sha256 against the repo copy (core#1117)."
    )
