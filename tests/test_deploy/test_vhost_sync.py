"""`sync-vhosts.sh` must restore every vhost it replaced, on every failure path (core#607).

**The defect this exists to prevent.** The vhost sync overwrote a vhost in place after
backing it up, and was called twice. Its guards aborted with a bare ``exit 1``, and the
restore loop lived ONLY inside the ``configtest``-failed branch. So when the FIRST call
replaced production and the SECOND hit a guard, the run aborted with production's vhost
replaced and unrestored — and **Apache keeps serving from memory**, so nothing looked
wrong. The damage would surface at the next reload for an unrelated reason (certbot,
logrotate, the next deploy), on a box that also carries a webdav co-tenant and the
founder's VPN.

Same shape as core#603: recovery that exists, is correct, and is unreachable from the
paths that need it.

**Two further defects, found on the real box rather than reasoned about.** The old code
wrote backups to a predictable ``/tmp/<name>.bak`` and the restore trusted whatever was
there, so a backup from an EARLIER deploy could be restored over a vhost this run never
touched. And ``/tmp/zapp-datanika-io.conf.bak`` on prod was a **dangling relative symlink**
dated 2026-07-17 — the fossil of the ``cp -a`` bug the old comment claims was fixed. Since
``[ -f ]`` follows symlinks it was false, so the one restore path that did run had been a
silent no-op for the production vhost for six weeks.

**So this file forces the failures rather than inspecting the source.** It runs the shipped
script byte-identically (via ``VHOST_SYNC_TEST_ROOT``, which only prefixes host paths)
against a fake tree, with ``apachectl`` replaced by a recording shim. Every scenario
asserts on **file contents left behind** — the thing the incident was about — not on log
text, because the log is what lied last time.

It runs anywhere bash does, Git Bash included, so the dev machine's pre-push hook exercises
it too. A missing bash is a **failure**, not a skip.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "sync-vhosts.sh"

PROD_LINK = "zapp-datanika-io.conf"
STAGING_LINK = "zstaging-app-datanika-io.conf"
PROD_SRC = "app.datanika.io.conf"
STAGING_SRC = "staging-app.datanika.io.conf"

LIVE_PROD = "# LIVE prod vhost — the one that must survive a failed sync\n"
LIVE_STAGING = "# LIVE staging vhost\n"
REPO_PROD = "# REPO prod vhost — the incoming version\n"
REPO_STAGING = "# REPO staging vhost — the incoming version\n"


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - a box with no bash cannot run the deploy either
        pytest.fail("bash not found; this suite must not silently stop testing the deploy script")
    return exe


def _posix_path(path: Path) -> str:
    """A path safe to place in a colon-separated bash PATH.

    `Path.as_posix()` keeps the Windows drive letter (`D:/Temp/x`); bash reads the `:` as
    the PATH separator, so one entry silently becomes the two nonexistent directories `D`
    and `/Temp/x` — and every shim stops being used while the suite still passes.
    """
    exe = shutil.which("cygpath")
    if exe:
        return subprocess.run(
            [exe, "-u", str(path)], capture_output=True, text=True, check=True
        ).stdout.strip()
    text = path.as_posix()
    if len(text) > 1 and text[1] == ":":
        return f"/{text[0].lower()}{text[2:]}"
    return text


def _write_lf(path: Path, body: str) -> None:
    """Write with LF endings, always.

    `Path.write_text` translates `\\n` to `\\r\\n` on Windows. Every file here is read by
    bash or compared with `cmp`, and a stray CR makes a correct comparison false while
    being invisible in a diff.
    """
    path.write_bytes(body.encode("utf-8"))


def _write_exec(path: Path, body: str) -> None:
    _write_lf(path, body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class Harness:
    """A fake box: sites-enabled symlinks into sites-available, a repo tree, and a shim."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.fake = tmp / "fake"
        self.bin = tmp / "bin"
        self.root = tmp / "root"
        self.fake.mkdir()
        self.bin.mkdir()

        self.available = self.root / "etc/apache2/sites-available"
        self.enabled = self.root / "etc/apache2/sites-enabled"
        self.available.mkdir(parents=True)
        self.enabled.mkdir(parents=True)

        self.repo = self.root / "opt/datanika/datanika/deploy/apache"
        self.repo.mkdir(parents=True)

        _write_lf(self.available / PROD_LINK, LIVE_PROD)
        _write_lf(self.available / STAGING_LINK, LIVE_STAGING)
        _write_lf(self.repo / PROD_SRC, REPO_PROD)
        _write_lf(self.repo / STAGING_SRC, REPO_STAGING)

        # sites-enabled holds symlinks, exactly as on the box. Where the platform refuses
        # (Windows without developer mode), fall back to a copy — the script resolves with
        # `readlink -f`, which is the identity on a real file, so the scenarios still hold.
        self.symlinked = True
        for name in (PROD_LINK, STAGING_LINK):
            try:
                (self.enabled / name).symlink_to(f"../sites-available/{name}")
            except (OSError, NotImplementedError):
                self.symlinked = False
                shutil.copy(self.available / name, self.enabled / name)

        self.calls = self.fake / "calls.log"
        _write_lf(self.calls, "")
        self.set_configtest(0)
        _write_exec(
            self.bin / "apachectl",
            f'''#!/usr/bin/env bash
echo "apachectl $*" >> "{self.calls}"
if [ "$1" = configtest ]; then
  rc="$(cat "{self.fake}/configtest")"
  [ "$rc" = 0 ] && echo "Syntax OK" || echo "Syntax error on line 1 of fake vhost"
  exit "$rc"
fi
exit 0
''',
        )

    # --- shim programming -------------------------------------------------------------
    def set_configtest(self, rc: int) -> None:
        _write_lf(self.fake / "configtest", str(rc) + "\n")

    # --- run + inspect ----------------------------------------------------------------
    def run(self) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env["VHOST_SYNC_TEST_ROOT"] = self.root.as_posix()
        # PATH is prepended *inside* bash, in POSIX form — see `_posix_path`.
        return subprocess.run(
            [
                _bash(),
                "-c",
                f'export PATH="{_posix_path(self.bin)}:$PATH"; exec bash "{SCRIPT.as_posix()}"',
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )

    def content(self, name: str) -> str:
        """What the vhost the box actually serves from now contains."""
        return (self.available / name).read_bytes().decode("utf-8")

    def apachectl_calls(self) -> list[str]:
        return [ln for ln in self.calls.read_text().splitlines() if ln.strip()]


@pytest.fixture
def box(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


# --------------------------------------------------------------------------------------
# The harness must be able to fail. A shim that is never invoked, or a fake tree the
# script never reads, makes every assertion below vacuously true — which is the exact
# defect class this file is about.
# --------------------------------------------------------------------------------------


def test_the_shim_is_actually_intercepted(box: Harness) -> None:
    """If PATH interception breaks, the scenarios run against the real apachectl."""
    result = box.run()
    assert result.returncode == 0, result.stdout + result.stderr
    calls = box.apachectl_calls()
    assert any(c.startswith("apachectl configtest") for c in calls), (
        "the apachectl shim was never invoked — PATH interception is broken and every "
        f"scenario in this file is running against the real box's binaries.\ncalls={calls}"
    )


def test_the_script_under_test_exists_and_has_one_exit(box: Harness) -> None:
    """`fatal()` is the only permitted abort — core#603's rule, asserted not assumed."""
    body = SCRIPT.read_bytes().decode("utf-8")
    # `\b` after the 1 keeps `trap 'exit 129' HUP` (a deliberate signal trap) out of this:
    # substring matching flagged all three signal traps, which is the assertion failing
    # for a reason unrelated to the property it guards.
    bare = [
        ln.strip()
        for ln in body.splitlines()
        if re.search(r"\bexit 1\b", ln) and not ln.strip().startswith("#") and "fatal()" not in ln
    ]
    assert bare == [], f"bare `exit 1` outside fatal() — core#603's defect returning: {bare}"
    assert "trap 'on_exit $?' EXIT" in body, "recovery must hang off EXIT"
    err_traps = [
        ln.strip()
        for ln in body.splitlines()
        if re.match(r"\s*trap\b", ln) and re.search(r"\bERR\b", ln)
    ]
    assert err_traps == [], (
        "an ERR trap fires on a strict subset of the failure paths; its presence is what "
        f"made an unreachable recovery look like a working one (core#603): {err_traps}"
    )


# --------------------------------------------------------------------------------------
# Control: the happy path. A suite about failures needs one, or a script that aborts for
# the wrong reason passes every failure test.
# --------------------------------------------------------------------------------------


def test_happy_path_applies_both_and_reloads(box: Harness) -> None:
    result = box.run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert box.content(PROD_LINK) == REPO_PROD
    assert box.content(STAGING_LINK) == REPO_STAGING
    assert any(c == "apachectl graceful" for c in box.apachectl_calls()), (
        f"Apache was never gracefully reloaded: {box.apachectl_calls()}"
    )


def test_no_changes_means_no_reload(box: Harness) -> None:
    """A needless reload is not free on a box with a webdav co-tenant and a VPN."""
    _write_lf(box.available / PROD_LINK, REPO_PROD)
    _write_lf(box.available / STAGING_LINK, REPO_STAGING)
    result = box.run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert box.apachectl_calls() == [], (
        f"Apache was touched despite nothing changing: {box.apachectl_calls()}"
    )


# --------------------------------------------------------------------------------------
# core#607 itself: the second call aborts after the first has already replaced production.
# --------------------------------------------------------------------------------------


def test_second_vhost_missing_in_repo_restores_the_first(box: Harness) -> None:
    """THE incident shape. First sync replaces prod, second hits a guard, run aborts.

    Before the fix the restore lived only in the configtest-failed branch, which this path
    never reaches — so production was left holding the repo version with no record.
    """
    (box.repo / STAGING_SRC).unlink()

    result = box.run()

    assert result.returncode != 0, "the run must fail — the staging vhost is missing"
    assert box.content(PROD_LINK) == LIVE_PROD, (
        "PRODUCTION WAS LEFT REPLACED. This is core#607 exactly: the first sync_one "
        "overwrote the prod vhost, the second aborted on a guard, and nothing restored "
        "it. Apache keeps serving from memory, so this surfaces at the next unrelated "
        f"reload.\nstdout:\n{result.stdout}"
    )
    assert box.content(STAGING_LINK) == LIVE_STAGING


def test_second_vhost_target_missing_restores_the_first(box: Harness) -> None:
    """The other guard: the symlink resolves to nothing."""
    (box.enabled / STAGING_LINK).unlink()
    if box.symlinked:
        (box.enabled / STAGING_LINK).symlink_to("../sites-available/gone.conf")
    else:
        pytest.skip("needs symlinks to build a dangling enabled-link")

    result = box.run()

    assert result.returncode != 0
    assert box.content(PROD_LINK) == LIVE_PROD, (
        f"production left replaced after the second guard aborted\n{result.stdout}"
    )


def test_configtest_failure_restores_both(box: Harness) -> None:
    """The path the old code DID handle — it must keep working."""
    box.set_configtest(1)

    result = box.run()

    assert result.returncode != 0
    assert box.content(PROD_LINK) == LIVE_PROD, f"prod not restored\n{result.stdout}"
    assert box.content(STAGING_LINK) == LIVE_STAGING, f"staging not restored\n{result.stdout}"
    assert "apachectl graceful" not in box.apachectl_calls(), (
        "Apache must not be reloaded onto a config its own configtest rejected"
    )


def test_configtest_failure_does_not_reload_but_restore_still_verifies(box: Harness) -> None:
    """Restore must re-run configtest rather than assume putting the file back worked."""
    box.set_configtest(1)
    result = box.run()
    assert result.returncode != 0
    configtests = [c for c in box.apachectl_calls() if c.startswith("apachectl configtest")]
    assert len(configtests) >= 2, (
        "restore must verify the box is syntactically sane afterwards, not assert it: "
        f"{box.apachectl_calls()}"
    )


# --------------------------------------------------------------------------------------
# The two defects found on the real box.
# --------------------------------------------------------------------------------------


def test_a_stale_backup_from_an_earlier_run_is_never_used(box: Harness) -> None:
    """A predictable /tmp/<name>.bak let an EARLIER deploy's backup be restored.

    The old restore loop was `if [ -f "/tmp/$d.bak" ]`, which trusts anything at that path.
    Here the prod vhost is already current, so THIS run must not touch it — even though a
    stale backup with different content exists where the old code would look.
    """
    _write_lf(box.available / PROD_LINK, REPO_PROD)  # prod already current -> not synced
    stale = Path("/tmp") / f"{PROD_LINK}.bak"
    wrote_stale = False
    try:
        try:
            _write_lf(stale, "# STALE backup from a previous deploy — must never be used\n")
            wrote_stale = True
        except OSError:  # pragma: no cover - no writable /tmp
            pytest.skip("no writable /tmp to plant a stale backup in")

        box.set_configtest(1)  # force the failure path that does the restoring
        result = box.run()

        assert result.returncode != 0
        assert box.content(PROD_LINK) == REPO_PROD, (
            "a stale /tmp backup was restored over a vhost this run never synced — the "
            f"restore must only touch files it backed up itself\n{result.stdout}"
        )
        assert box.content(STAGING_LINK) == LIVE_STAGING
    finally:
        if wrote_stale:
            stale.unlink(missing_ok=True)


def test_backup_is_a_regular_file_not_a_symlink(box: Harness) -> None:
    """`cp -a` on the enabled symlink backed up the LINK, so restore restored nothing.

    On prod, `/tmp/zapp-datanika-io.conf.bak` was still a dangling relative symlink from
    2026-07-17. `[ -f ]` follows symlinks, so the check was false and the restore skipped
    production while printing nothing at all.

    Asserted through behaviour: the restore must put back real *content*.
    """
    box.set_configtest(1)
    result = box.run()
    assert result.returncode != 0
    assert box.content(PROD_LINK) == LIVE_PROD
    assert box.content(PROD_LINK).strip() != "", "restored an empty file — a dangling-link backup"
    assert "restored" in result.stdout, (
        "the restore ran silently; a no-op restore that prints nothing is how the prod "
        f"backup went unnoticed for six weeks\n{result.stdout}"
    )


# --------------------------------------------------------------------------------------
# Signals — a cancelled CD job SIGHUPs the remote script when the SSH channel closes.
# --------------------------------------------------------------------------------------


def test_sigterm_between_commands_still_restores(box: Harness) -> None:
    """Fault injected where bash is BETWEEN commands, so nothing fails conventionally.

    core#603's lesson on where to inject: a signal delivered inside a command substitution
    *is* a command failure, which even an ERR trap catches — so that placement cannot
    discriminate. `apachectl graceful` is reached after both files are already replaced,
    and killing the script there leaves the EXIT trap as the only thing that can recover.
    """
    _write_exec(
        box.bin / "apachectl",
        f'''#!/usr/bin/env bash
echo "apachectl $*" >> "{box.calls}"
if [ "$1" = configtest ]; then echo "Syntax OK"; exit 0; fi
if [ "$1" = graceful ]; then kill -TERM "$PPID"; sleep 5; fi
exit 0
''',
    )

    result = box.run()

    assert result.returncode != 0, "a TERM during the reload must not report success"
    assert box.content(PROD_LINK) == LIVE_PROD, (
        f"a cancelled deploy left production replaced\n{result.stdout}"
    )
    assert box.content(STAGING_LINK) == LIVE_STAGING
