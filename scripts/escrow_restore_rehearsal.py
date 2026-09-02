#!/usr/bin/env python3
"""Prove the ESCROW alone can turn an off-site backup back into a working service.

core#748. Run this **from the escrow machine**, against the off-site artifact.
Do not run it on the app box, and do not fold it into ``restore-drill.sh``.

Why not the drill
-----------------
``deploy/server/restore-drill.sh`` runs monthly *on the app box*. It decrypts the
off-site copy with ``/root/.gnupg`` and would read ``CREDENTIAL_ENCRYPTION_KEY``
from that box's own ``.env.docker``. **Both of those are inputs the disaster
removes.** On 2026-07-14 we lost the host, its data and its backups together; a
check proving the box can read its own backups says nothing whatsoever about that
morning. It would report PASS forever, including on the day the escrow rotted.

So the assertion has to be made from the other side: the two escrowed files plus
the artifact sitting on Aweb, with the app box touched for nothing. That needs no
second host -- only ``gpg`` and this script.

What it asserts
---------------
1. an EMPTY keyring cannot decrypt the artifact          (control)
2. the escrowed private key alone CAN
3. the decrypted dump reached its terminator
4. the escrowed ``CREDENTIAL_ENCRYPTION_KEY`` is Fernet-shaped
5. ``connections`` is present and NOT EMPTY               (anti-vacuity)
6. every non-null ``config_encrypted`` decrypts under the escrowed key
7. each plaintext is well-formed JSON
8. a DIFFERENT valid Fernet key decrypts nothing          (control)

Controls 1 and 8 are not decoration. Without 1, "it decrypted" only proves gpg
found *a* key somewhere; without 8, it only proves Fernet round-trips.

Secrets
-------
Nothing here prints a key, a token or a decrypted credential. Values are reported
as SHA-256 prefixes, byte counts, booleans and JSON *field names*.

Usage
-----
    python scripts/escrow_restore_rehearsal.py \\
        --archive  datanika_2026-09-02_030001.sql.gz.gpg \\
        --privkey  secrets/datanika-backup-privkey.asc \\
        --env      secrets/pointer-app.env.docker

Exit 0 = the escrow is sufficient. Exit 1 = it is not, and the recovery you are
about to attempt will not work.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover - a recovery box may not have core installed
    print("FATAL: `cryptography` is required. pip install cryptography")
    sys.exit(2)

# pg_dump writes COPY blocks; this pulls one out by table name.
_COPY = r"^COPY public\.{table} \(([^)]*)\) FROM stdin;\n(.*?)^\\\.$"

# The column has been spelled both ways across this project's history. Name it
# from the artifact rather than from memory, and refuse if neither appears --
# guessing would let a genuine schema change read as "no credentials to check",
# which is a vacuous pass wearing a green tick.
_ENCRYPTED_COLUMNS = ("config_encrypted", "credentials")


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.total = 0

    def check(self, label: str, ok: bool, detail: str = "") -> bool:
        self.total += 1
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f"  -- {detail}" if detail else ""))
        if not ok:
            self.failures.append(label)
        return ok

    def warn(self, label: str, detail: str = "") -> None:
        print(f"  [WARN] {label}" + (f"  -- {detail}" if detail else ""))


def _sha(value: bytes | str) -> str:
    return hashlib.sha256(value.encode() if isinstance(value, str) else value).hexdigest()[:16]


def _gnupghome_for_gpg(path: Path) -> str:
    """The keyring path in the form *this* gpg understands.

    ⚠️ Not portability decoration. The escrow lives on a Windows machine, and the
    `gpg` on its PATH is the MSYS build shipped with Git. Handed a native
    `C:\\Users\\...\\Temp\\...` it cannot start `gpg-agent` at all --

        gpg: error running '/usr/bin/gpg-agent': exit status 2

    -- and reports that as an *import* failure, which names the wrong cause. So
    convert with `cygpath` when it is there, and leave the path alone when it is
    not (a real Linux recovery box, where it is already correct).
    """
    if shutil.which("cygpath") is None:
        return str(path)
    converted = subprocess.run(  # noqa: S603
        ["cygpath", "-u", str(path)], capture_output=True, text=True, check=False
    )
    return converted.stdout.strip() or str(path)


def _gpg(gnupghome: Path, *args: str, stdin: bytes | None = None) -> subprocess.CompletedProcess:
    # GNUPGHOME in the environment, NOT `--homedir`. Both work on Linux; only the
    # env-var form is what the runbook documents and what has been measured here.
    env = {**os.environ, "GNUPGHOME": _gnupghome_for_gpg(gnupghome)}
    return subprocess.run(  # noqa: S603
        ["gpg", "--batch", "--quiet", *args],
        input=stdin,
        capture_output=True,
        check=False,
        env=env,
    )


def _copy_block(sql: str, table: str) -> tuple[list[str], list[list[str]]]:
    match = re.search(_COPY.format(table=table), sql, re.MULTILINE | re.DOTALL)
    if not match:
        return [], []
    columns = [c.strip() for c in match.group(1).split(",")]
    rows = [r.split("\t") for r in match.group(2).split("\n") if r.strip()]
    return columns, rows


def _env_value(path: Path, key: str) -> str | None:
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.strip().startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip("\"'")
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--archive", required=True, type=Path, help="the *.sql.gz.gpg off-site copy")
    ap.add_argument("--privkey", required=True, type=Path, help="escrowed backup private key")
    ap.add_argument("--env", required=True, type=Path, help="escrowed .env.docker copy")
    args = ap.parse_args()

    if shutil.which("gpg") is None:
        print("FATAL: `gpg` is not on PATH.")
        return 2

    r = Report()
    ciphertext = args.archive.read_bytes()
    print(f"\nArchive: {args.archive.name} ({len(ciphertext):,} bytes)")

    print("\n[1] CONTROL -- an empty keyring must NOT decrypt it")
    with tempfile.TemporaryDirectory() as empty:
        result = _gpg(Path(empty), "--decrypt", stdin=ciphertext)
        r.check(
            "empty keyring refuses (control is live, not inert)",
            result.returncode != 0,
            "" if result.returncode != 0 else "IT DECRYPTED -- everything below is worthless",
        )

    print("\n[2] The escrowed private key, alone, in a throwaway keyring")
    with tempfile.TemporaryDirectory() as home:
        gnupghome = Path(home)
        imported = _gpg(gnupghome, "--import", str(args.privkey))
        if not r.check(
            "escrowed private key imports",
            imported.returncode == 0,
            imported.stderr.decode("utf-8", "replace").strip()[:300],
        ):
            return 1
        decrypted = _gpg(gnupghome, "--decrypt", stdin=ciphertext)
        if not r.check(
            "it decrypts the off-site artifact with the app box untouched",
            decrypted.returncode == 0,
            decrypted.stderr.decode("utf-8", "replace").strip()[:200],
        ):
            return 1
        gz = decrypted.stdout

    sql = gzip.decompress(gz).decode("utf-8", "replace")
    print("\n[3] The restored SQL")
    r.check("gunzips", len(sql) > 0, f"{len(sql):,} chars")
    r.check("reached its terminator (not truncated)", "PostgreSQL database dump complete" in sql)

    print("\n[4] The escrowed CREDENTIAL_ENCRYPTION_KEY")
    key = _env_value(args.env, "CREDENTIAL_ENCRYPTION_KEY")
    if not r.check("present in the escrow file", bool(key)):
        return 1
    try:
        shaped = len(base64.urlsafe_b64decode(key)) == 32
    except Exception:  # noqa: BLE001
        shaped = False
    if not r.check("Fernet-shaped (32 bytes urlsafe-b64)", shaped):
        return 1
    print(f"        sha256[:16] = {_sha(key)}   (hash only -- never the value)")
    fernet = Fernet(key.encode())

    print("\n[5] connections.config_encrypted, out of the dump itself")
    columns, rows = _copy_block(sql, "connections")
    if not r.check("connections COPY block found", bool(columns)):
        return 1
    if not r.check(
        "connections is NOT empty (a vacuous pass is not a pass)", bool(rows), f"{len(rows)} rows"
    ):
        return 1
    found = [c for c in _ENCRYPTED_COLUMNS if c in columns]
    if not r.check(
        "exactly one encrypted-config column", len(found) == 1, f"found={found} of {columns}"
    ):
        return 1
    idx = columns.index(found[0])

    print(f"\n[6] Fernet-decrypt all {len(rows)} blobs with the ESCROWED key")
    ok = bad = 0
    shapes: Counter[str] = Counter()
    for n, fields in enumerate(rows, 1):
        if idx >= len(fields) or fields[idx] == r"\N":
            continue
        try:
            plain = fernet.decrypt(fields[idx].encode())
        except (InvalidToken, ValueError, TypeError) as exc:
            bad += 1
            print(f"        row {n}: FAILED -> {type(exc).__name__}")
            continue
        ok += 1
        try:
            shapes[",".join(sorted(json.loads(plain)))] += 1
        except (ValueError, TypeError):
            shapes["<not JSON>"] += 1
    for shape, count in sorted(shapes.items()):
        print(f"        {count:>3} x {{{shape}}}")  # FIELD NAMES only, never values
    r.check("every non-null blob decrypts", bad == 0 and ok > 0, f"{ok} ok, {bad} failed")
    r.check("every plaintext is well-formed JSON", shapes["<not JSON>"] == 0)

    print("\n[7] CONTROL -- a different valid Fernet key must decrypt nothing")
    wrong = Fernet(Fernet.generate_key())
    spurious = 0
    for fields in rows:
        if idx >= len(fields) or fields[idx] == r"\N":
            continue
        try:
            wrong.decrypt(fields[idx].encode())
            spurious += 1
        except Exception:  # noqa: BLE001, S110
            pass
    r.check("wrong key decrypts nothing", spurious == 0, f"{spurious} spurious successes")

    # Not a pass/fail: the bytes are simply not in this artifact by design (core#954).
    print("\n[8] Rows that point OUTSIDE the dump (core#954)")
    ucols, urows = _copy_block(sql, "uploaded_files")
    if ucols and urows:
        r.warn(
            "uploaded_files rows restore with archive_path into a volume no backup carries",
            f"{len(urows)} row(s); the .tar.gz files live only in `datanika_uploaded_files`",
        )
    else:
        print("        none in this dump")

    print("\n" + "=" * 70)
    if r.failures:
        print(f"ESCROW REHEARSAL FAILED -- {len(r.failures)} of {r.total} checks red:")
        for f in r.failures:
            print(f"  - {f}")
        print("\nDo not rely on the escrow until these are green.")
        return 1
    print(f"ESCROW REHEARSAL PASS -- {r.total}/{r.total}.")
    print("The escrowed key alone turns the off-site ciphertext back into usable")
    print("customer credentials, with the app box touched for nothing.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
