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
from pathlib import Path, PurePosixPath

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
    """
    attrs = GITATTRIBUTES.read_text(encoding="utf-8")
    rules = {
        line.split()[0]
        for line in (raw.strip() for raw in attrs.splitlines())
        if line and not line.startswith("#") and "eol=lf" in line
    }
    assert rules, ".gitattributes declares no eol=lf rules at all — this test is vacuous"

    def covered(rel: Path) -> bool:
        # A rule covers a file if it matches by extension glob in the file's own directory,
        # or by a repo-wide extension glob such as `*.sh`.
        return any(
            PurePosixPath(rel.as_posix()).match(rule) or PurePosixPath(rel.name).match(rule)
            for rule in rules
        )

    checked, uncovered = 0, []
    for directory in (SERVER_DIR, APACHE_DIR):
        for path in sorted(directory.iterdir()):
            if not path.is_file() or path.name in EOL_EXEMPT:
                continue
            checked += 1
            rel = path.relative_to(ROOT)
            if not covered(rel):
                uncovered.append(rel.as_posix())

    # Anti-vacuity: a walk that finds nothing passes for the wrong reason. Both directories
    # have held files since core#747; zero here means the walk broke, not that we are clean.
    assert checked >= len(APACHE_DUPLICATES) * 2, (
        f"only {checked} files walked across deploy/server/ + deploy/apache/ — this test "
        f"cannot have checked the {len(APACHE_DUPLICATES)} pinned vhost pairs"
    )

    assert not uncovered, (
        "These files are deployed to a Linux box but no .gitattributes rule pins them to "
        f"LF, so `core.autocrlf` decides their bytes per checkout (core#1076): {uncovered}\n"
        "Add a `<dir>/*.<ext> text eol=lf` rule, next to the ones already there."
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
