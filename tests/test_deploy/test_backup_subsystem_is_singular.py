"""core#746 — there is exactly ONE backup subsystem, and the retired one cannot come back.

## What was retired, and why all three had to go together

Three April-dated files in ``scripts/`` formed a complete, superseded backup subsystem:

======================  ==================================================================
file                    what it was
======================  ==================================================================
``backup-postgres.sh``  a container-local ``pg_dump`` into ``/backups/{daily,weekly,monthly}``
``verify-backup.sh``    a "verifier" that restored the newest ``/backups/daily`` dump
``setup-backup-cron.sh``  the installer that armed both, addressed to *"the Hetzner server"*
======================  ==================================================================

``deploy/server/backup-offsite.sh`` strictly supersedes the first: same ``pg_dump``, **plus** GPG
encryption, **plus** rsync to a different provider, **plus** the file volumes, **plus** a
node-exporter freshness metric and retention on both legs. ``deploy/server/restore-drill.sh``
supersedes the second: a throwaway ``postgres:16-alpine`` container, the off-site artifact, row
counts compared against the live database, and Prometheus metrics.

🔑 **They had to be deleted as one unit.** Removing only ``verify-backup.sh`` — which is what
core#746 literally asks for — leaves ``setup-backup-cron.sh`` installing a weekly cron for a file
that no longer exists. That is not a smaller bug than the one being fixed; it is a *new* silent
failure, and it fires nightly.

## Three measurements that settle it, taken on production 2026-09-07

1. **The verifier was inert.** It globs ``${BACKUP_DIR}/daily/*.sql.gz`` with ``BACKUP_DIR=/backups``.
   In the running ``datanika-postgres``, ``/backups`` exists and is **empty**; ``/backups/daily``
   does not exist. It has therefore never verified anything.
2. **The two died together, and that is the whole story.** ``/backups/daily`` is missing *because*
   ``backup-postgres.sh`` — the only thing that ever created it — stopped being the backup
   mechanism. The verifier is inert as a consequence, not as a separate accident.
3. **Nothing ran it.** The production crontab holds ``backup-offsite.sh`` (daily 03:00) and
   ``restore-drill.sh`` (monthly), and **no entry for either retired script**.

## The correction this file also records

core#746 was relayed to me as *"a full restore inside the PRODUCTION postgres instance … can take
production with it"*. **The issue's own body already refutes that** — it creates a separate database
and drops it, and says in as many words that the issue *"should not overstate it"*. Measured, it is
weaker still: nothing invokes it. The real hazard was never a running restore; it was
``setup-backup-cron.sh`` being **one manual command away** from arming a vacuous check that competes
with live traffic — the *"inert-but-present"* shape the issue names, where repairing the obvious bug
arms the dangerous one.

## Why the assertions below are shaped this way

The tempting guard is *"``scripts/verify-backup.sh`` must not exist"*. That is too narrow twice over:
it says nothing about the other two, and it says nothing about the property that actually matters —
**that exactly one backup mechanism is wired**. So the assertions are:

* the three retired paths are absent (named individually, so a failure says *which*);
* no script anywhere installs a cron pointing at a backup script under ``/scripts/``;
* the live pair still exists **and is still in the installer's list**, because a guard that only
  forbids things passes triumphantly on a tree with no backups at all.

That last one is the anti-vacuity control, and it is the reason this file is not simply three
``assert not path.exists()`` lines.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Retired by core#746. Named individually rather than globbed: a glob would silently start
# covering (or stop covering) files as `scripts/` changes, and the failure message would not
# say which one came back.
RETIRED = (
    "scripts/verify-backup.sh",
    "scripts/backup-postgres.sh",
    "scripts/setup-backup-cron.sh",
)

# The mechanism that replaced them. Both are installed onto the box by
# scripts/install-server-scripts.sh on every deploy (core#747).
LIVE = (
    "deploy/server/backup-offsite.sh",
    "deploy/server/restore-drill.sh",
)

INSTALLER = ROOT / "scripts" / "install-server-scripts.sh"

# A cron line that runs a backup/verify script from inside the postgres container. This is the
# shape setup-backup-cron.sh installed, and the shape that must never reappear: the backup that
# matters runs on the HOST, against the off-site leg.
_CONTAINER_BACKUP_CRON = re.compile(
    r"""[0-9*/,\s-]+ +.*docker\s+exec\s+\S*postgres\S*.*/scripts/\S*(backup|verify)\S*\.sh""",
    re.IGNORECASE,
)


def _text_files() -> list[Path]:
    """Every shell/yaml/python file that could install a cron, excluding this test."""
    out: list[Path] = []
    for pattern in ("scripts/**/*.sh", "deploy/**/*.sh", ".github/**/*.yml", "*.yml"):
        out.extend(p for p in ROOT.glob(pattern) if p.is_file())
    return out


@pytest.mark.parametrize("rel", RETIRED)
def test_the_retired_backup_scripts_are_gone(rel):
    path = ROOT / rel
    assert not path.exists(), (
        f"{rel} is back. It was retired in core#746 as part of a superseded backup subsystem "
        f"(container-local pg_dump into /backups, a verifier asserting a TABLE count, and the "
        f"installer that armed both against 'the Hetzner server' — a box terminated 2026-07-14).\n"
        f"The live mechanism is deploy/server/backup-offsite.sh + restore-drill.sh. If you need "
        f"something these did, add it there; do not reinstate a second, weaker verifier that runs "
        f"inside the production postgres instance."
    )


@pytest.mark.parametrize("rel", LIVE)
def test_the_live_backup_mechanism_still_exists(rel):
    """Anti-vacuity: a file that only forbids things passes on a tree with no backups at all."""
    assert (ROOT / rel).is_file(), (
        f"{rel} is missing. The guard above forbids the retired subsystem; without this "
        f"assertion it would pass just as happily on a repository that had no backup mechanism "
        f"whatsoever, which is the failure it is supposed to prevent."
    )


@pytest.mark.parametrize("rel", LIVE)
def test_the_live_mechanism_is_still_installed_onto_the_box(rel):
    """Existing in the repo is not the property. core#747: `deploy/server/` needs an installer.

    A backup script that is in git and not on the box is exactly the drift core#1017 measured —
    merged, promoted, and not running, with every signal green.
    """
    assert INSTALLER.is_file(), f"{INSTALLER} is missing — nothing installs deploy/server/"
    body = INSTALLER.read_text(encoding="utf-8")
    name = Path(rel).name
    assert name in body, (
        f"{name} is not named in scripts/install-server-scripts.sh, so the deploy does not "
        f"install it and the copy that runs from cron can drift from git indefinitely."
    )


def test_nothing_installs_a_container_side_backup_cron():
    """The retired installer's shape, forbidden repo-wide rather than at one path.

    Deleting `setup-backup-cron.sh` removes today's instance. This forbids the *shape*, so the
    next person automating backups cannot recreate a cron that `docker exec`s a backup script
    inside the production database container.
    """
    offenders: list[str] = []
    for path in _text_files():
        if path.resolve() == Path(__file__).resolve():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if _CONTAINER_BACKUP_CRON.search(line):
                offenders.append(f"{path.relative_to(ROOT).as_posix()}:{i}: {line.strip()[:100]}")

    assert not offenders, (
        "a cron entry runs a backup/verify script inside the postgres container:\n  "
        + "\n  ".join(offenders)
        + "\n\nThe backup that matters runs on the HOST (deploy/server/backup-offsite.sh) and "
        "ships off-site to a different provider. A container-side copy competes with live "
        "traffic for shared buffers, WAL and disk, and — as core#746 measured — the one we had "
        "verified nothing while doing it."
    )


def test_the_cron_shape_detector_can_actually_see_one():
    """Arming. Without this, the scan above is satisfied by a regex that matches nothing.

    The positive case is the exact line that was deleted from setup-backup-cron.sh.
    """
    real = (
        "CRON_VERIFY='0 4 * * 0 docker exec datanika-postgres /bin/bash "
        "/scripts/verify-backup.sh >> /var/log/datanika-backup.log 2>&1'"
    )
    assert _CONTAINER_BACKUP_CRON.search(real), (
        "the detector cannot see the very line core#746 is about — it would report a clean "
        "repository forever"
    )

    also_real = (
        "0 3 * * * docker exec datanika-postgres /bin/bash /scripts/backup-postgres.sh "
        ">> /var/log/datanika-backup.log 2>&1"
    )
    assert _CONTAINER_BACKUP_CRON.search(also_real)

    # Negative controls: the LIVE host-side cron lines must not trip it, or the guard reds on a
    # correct repository — which is how a guard gets deleted (core#1162's lesson, other polarity).
    for benign in (
        "0 3 * * * /opt/datanika/scripts/backup-offsite.sh >> /var/log/datanika-backup.log 2>&1",
        "0 5 1 * * /opt/datanika/scripts/restore-drill.sh >> /var/log/datanika-restore-drill.log 2>&1",
        "docker exec datanika-postgres psql -U datanika -c 'SELECT 1'",
    ):
        assert not _CONTAINER_BACKUP_CRON.search(benign), (
            f"the detector fires on a correct line, which is how guards get switched off: {benign}"
        )


def test_the_scan_is_not_looking_at_an_empty_tree():
    """The other anti-vacuity half: prove the file walk finds real files."""
    files = _text_files()
    assert len(files) >= 10, (
        f"the scan found only {len(files)} file(s); it is almost certainly rooted wrong, and "
        f"'no offenders' from an empty walk is indistinguishable from a clean repository"
    )
    names = {p.name for p in files}
    assert "backup-offsite.sh" in names, (
        "the walk did not reach deploy/server/, so its 'no container-side cron' verdict covers "
        "less than it claims"
    )
