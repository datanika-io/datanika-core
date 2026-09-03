# Runbook — what a restore needs besides the dump

**Owner: Infra. Written 2026-08-31 for [core#748]. Read this before, not during, a recovery.**

The off-site dump restores cleanly — 28 tables, 19 populated, ~20 s, every row matching prod. That
is measured monthly by `deploy/server/restore-drill.sh` and it is **not** the same as having a
working service. `backup-offsite.sh` captures exactly one artifact: a single `pg_dump`. Everything
below is required to turn that dump back into Datanika, and none of it is inside the dump.

> 🚨 **The failure mode this runbook exists to prevent:** a restore that succeeds at the SQL layer
> and yields a database whose connection credentials cannot be decrypted. It looks like a success.
> QA's independent third-machine restore on 2026-08-31 decrypted all 6 connection configs — **but
> only because `CREDENTIAL_ENCRYPTION_KEY` was supplied out of band.**

---

## The prerequisites

| # | Input | Lives at | Independent copies | Replaceable? |
|---|---|---|---|---|
| 1 | The dump itself | Aweb `185.226.65.96:/opt/datanika-backups/*.sql.gz.gpg` (30 d) + app box `/opt/datanika/backups/*.sql.gz` (7 d) | 2 | n/a |
| 2 | **Backup decryption key** | app box `/root/.gnupg` **+** `secrets/datanika-backup-privkey.asc` **+** [the third location](#the-third-location-answered-2026-09-01) | 2 verified + 1 reported | ❌ **no** — without it every off-site copy is noise |
| 3 | **`CREDENTIAL_ENCRYPTION_KEY`** | app box `.env.docker` **+** `secrets/pointer-app.env.docker` **+** [the third location](#the-third-location-answered-2026-09-01) | 2 verified + 1 reported | ❌ **NO. This is the one truly irreplaceable secret we hold.** |
| 4 | `SECRET_KEY` | same two files | 2 | ✅ yes — rotating it invalidates sessions and outstanding reset links; users log in again |
| 5 | `POSTGRES_PASSWORD` | same two files | 2 | ✅ yes — set it to anything on the restored instance |
| 6 | TLS origin cert + key | app box `/etc/ssl/datanika/` **+** `secrets/datanika-origin.{pem,key}` | 2 | ✅ yes — reissue from Cloudflare |
| 7 | Off-site SSH key | app box `/root/.ssh/aweb_backup` | **1 — app box only** | ✅ yes — generate a new pair and add it on Aweb |
| 8 | Paddle / OAuth / SMTP / Telegram / Grafana secrets | `.env.docker` + `secrets/pointer-app.env.docker` | 2 | ✅ yes — reissue from each vendor |
| 9 | Application source | git (`datanika-core`, `datanika-cloud`) | GitHub + every worktree | ✅ |
| 10 | Apache vhosts, compose, deploy scripts | git `deploy/server/` + the box | 2 | ✅ |
| 11 | DNS / Cloudflare config | Cloudflare account; documented in root `CLAUDE.md` | 1 + docs | ✅ |
| 12 | **Uploaded source archives** | `datanika_uploaded_files` docker volume on the app box — **15 KB, 3 files** (2026-09-03) **+** Aweb `:/opt/datanika-backups/datanika_uploaded_files_*.tar.gz.gpg` (30 d) | 2 | ❌ **no** — the bytes exist nowhere else |
| 13 | Per-tenant dbt projects **and whatever a user pointed a file-based destination at** | `datanika_dbt_projects` docker volume — 1.4 MB **+** Aweb `:/opt/datanika-backups/datanika_dbt_projects_*.tar.gz.gpg` (30 d) | 2 | ⚠️ **split** — see below |

**Rows 2 and 3 are the whole point of this document.** Everything else can be re-created from a
vendor, a certificate authority, or git. Those two cannot be re-created from anything.

> ✅ **Rows 12–13 CLOSED 2026-09-03 ([core#954]).** `backup-offsite.sh` now archives both volumes,
> encrypts each to the same recipient, round-trips the ciphertext before shipping, and gates each
> artifact on an **independent** file count (`find` before the tar, compared after) rather than a
> byte floor — a byte floor cannot tell an empty volume from a tar that produced nothing.
>
> What the gap was: the dump *references* bytes it does not contain. `uploaded_files` restores 3 rows
> whose `archive_path` is `/app/uploaded_files/archives/<sha256>.tar.gz`, and 3 connection configs
> decrypt to `{"uploaded_file_id": …}`. A dump-only restore yielded **dangling `archive_path`s** — the
> identical broken state [core#471] fixed for image rebuilds, arriving through the restore door.
>
> 🔑 **Row 13's "is it regenerable?" question is ANSWERED, and the answer is split — which is why the
> volume is backed up for a reason nobody predicted.** Measured 2026-09-03 rather than assumed:
>
> - **The `tenant_<org_id>/` subtrees ARE regenerable.** `DbtProjectService.ensure_project()`
>   recreates the scaffold idempotently, and `write_model` / `generate_profiles_yml` /
>   `write_source_yml_for_connection` are all called from database state immediately before each run.
>   `Transformation.sql_body` is a `Text` column, so the model SQL lives in the database. Corroborated
>   by the volume itself: there were **0 `models/*.sql` files** on production, which is exactly what
>   that design predicts. `write_packages_yml`, `install_packages`, `write_snapshot` and
>   `write_tests_config` have **no callers at all** outside the service.
> - **`_docs_samples/` is NOT, and it is 92% of the volume by bytes.** It holds `warehouse.duckdb`
>   (1.32 MB) and sample CSV/JSON, and **no code path writes it** — `get_project_path` only ever
>   produces `tenant_<org_id>`. Production connection **id=14** (org 23, type `duckdb`, `deleted_at`
>   NULL) has `path: /app/dbt_projects/_docs_samples/warehouse.duckdb`.
>
> **So the volume is a store of record because a user pointed a file-based destination into it**, not
> because dbt projects are precious. Nothing constrains where a DuckDB/SQLite destination path may
> live, so any volume can become one. Keep row 13 backed up even if the dbt-scaffolding argument
> stops applying — and see [core#969] for the underlying "destination paths are unconstrained" issue.
>
> ⚠️ **Two honest limits on this.** The volume copy is taken live with no quiesce, so a DuckDB file
> being written at 03:00 is captured torn — strictly better than no copy, and not a guarantee. And
> the **restore drill does not yet exercise these artifacts** ([core#970]); their only proof today is
> the creation-time round-trip plus the one manual restore recorded on [core#954].

### Why #3 is irreplaceable

**`connections.config_encrypted`** is Fernet-encrypted with `CREDENTIAL_ENCRYPTION_KEY`. The
ciphertext is in the dump; the key is not. Lose the key and every customer's warehouse credential is
permanently undecryptable — the rows survive and are worthless. There is no derivation, no reset, no
vendor to ask.

> ⚠️ **This document said `connections.credentials` until 2026-09-03. That column does not exist.**
> Corrected here because an incident responder following the old text would run `\d connections`,
> not find it, and have to guess whether the ciphertext was in the dump at all — at the worst
> possible moment. Found by the rehearsal below refusing rather than passing vacuously.

### Why #2 exists at all, and why it is not stored with the backup

Since [core#675] the off-site copy is GPG ciphertext, because Aweb is a shared, general-purpose,
**unpatched** host ([landing#389]) running a VPN endpoint, three unrelated bots, a public web server
and Plausible. It has no business being able to read our users' rows.

🚨 **Never escrow the key onto Aweb.** Putting the Fernet key — or the backup private key — beside
the ciphertext it decrypts converts *"an attacker who reaches Aweb obtains encrypted credentials"*
into *"an attacker who reaches Aweb obtains our customers' warehouse credentials."* Aweb holds
**ciphertext and no key of any kind**, and that invariant is what the encryption is for.

---

## The one command that checks all of it — `scripts/escrow_restore_rehearsal.py`

**Run this from the escrow machine, not the app box.** It is the whole chain in one call, with both
negative controls built in:

```bash
scp root@185.226.65.96:/opt/datanika-backups/<newest>.sql.gz.gpg .
python scripts/escrow_restore_rehearsal.py \
    --archive <newest>.sql.gz.gpg \
    --privkey secrets/datanika-backup-privkey.asc \
    --env     secrets/pointer-app.env.docker
```

🚨 **Why this is NOT part of `deploy/server/restore-drill.sh`, and must not be moved there.**
The drill runs monthly *on the app box*. It decrypts with `/root/.gnupg` and would read
`CREDENTIAL_ENCRYPTION_KEY` from that box's own `.env.docker` — **both of them inputs the disaster
removes.** On 2026-07-14 we lost the host, its data and its backups together; a check proving the box
can read its own backups says nothing whatsoever about that morning, and would report PASS on every
run including the day the escrow rotted. The drill proves the *data* survives. Only this proves the
*escrow is sufficient*, because it is the only one that touches the app box for nothing.

**Last run 2026-09-03 against `datanika_2026-09-02_030001.sql.gz.gpg`: PASS, 13/13.**
23 connection configs decrypted under the escrowed key, 0 failures, all well-formed JSON. Both
controls live: an **empty keyring refused** the artifact, and a **different valid Fernet key
decrypted nothing**. Without those two, "it decrypted" would only prove that gpg found *a* key
somewhere and that Fernet round-trips.

⚠️ **On Windows — where the escrow actually lives — `gpg` is the MSYS build shipped with Git, and it
cannot start `gpg-agent` under a native `C:\…` path.** It reports that as an *import* failure, naming
the wrong cause entirely. The script converts with `cygpath` when present; if you run the steps by
hand instead, use a Git-Bash `mktemp -d` path rather than a Windows one.

## Verify the escrow is still valid — do this, do not assume it

An escrow copy that has silently drifted is worse than none, because it is believed. Both checks
compare **hashes**, never values, and neither prints a secret.

**1. Does the escrowed `CREDENTIAL_ENCRYPTION_KEY` still match production?**

```bash
BOX=$(ssh -i ~/.ssh/id_ed25519 root@185.25.22.188 \
  "grep '^CREDENTIAL_ENCRYPTION_KEY=' /opt/datanika/datanika/.env.docker | cut -d= -f2- | tr -d '\r\n' | sha256sum | cut -c1-32")
ESC=$(grep '^CREDENTIAL_ENCRYPTION_KEY=' secrets/pointer-app.env.docker \
  | cut -d= -f2- | tr -d '\r\n' | sha256sum | cut -c1-32)
[ "$BOX" = "$ESC" ] && echo MATCH || echo "*** ESCROW IS STALE ***"
```

Last run **2026-09-03: MATCH** (`8f835af6…`, unchanged since 2026-08-31). The two files differ by
exactly one non-secret line (`DATANIKA_OVERAGE_CHARGE_ENABLE`), so a size difference between them is
**not** evidence of key drift — compare the value, not the file.

🔑 **Re-derived on 2026-09-03 across the whole file, not just the three named keys**: every key
compared by name and by SHA-256 of its value, printing neither. **39 keys live, 38 escrowed, and all
38 shared values are byte-identical** — zero secret drift, despite the escrow copy being dated
2026-07-17 and the live file 2026-07-24. The single absentee is `DATANIKA_OVERAGE_CHARGE_ENABLE`,
which is [cloud#141]: **a restore from escrow comes up with overage charging OFF.** That is a
recovery step (below), not something to fix by editing the escrow file.

```bash
# name + value-hash on both sides, then diff. Prints no secret.
hash_env() { while IFS= read -r l; do case "$l" in \#*|"") continue;; *=*) ;; *) continue;; esac
  printf '%s %s\n' "${l%%=*}" "$(printf '%s' "${l#*=}" | sha256sum | cut -c1-12)"; done | sort; }
ssh root@185.25.22.188 'cat /opt/datanika/datanika/.env.docker' | hash_env > /tmp/live
hash_env < secrets/pointer-app.env.docker > /tmp/escrow
diff /tmp/live /tmp/escrow
```

**2. Can the escrowed backup key decrypt an off-site artifact *without the box*?**

This is the one that matters, because it is the disaster scenario. Import the escrowed key into a
throwaway keyring — never your default one — and decrypt a real off-site file:

```bash
export GNUPGHOME=$(mktemp -d) && chmod 700 "$GNUPGHOME"
gpg --batch --quiet --import secrets/datanika-backup-privkey.asc
gpg --batch --quiet --decrypt <a copy of some *.sql.gz.gpg> | gunzip | head -5
rm -rf "$GNUPGHOME"; unset GNUPGHOME
```

Last run **2026-08-31: PROVEN** — the `secrets/` copy alone decrypted box-produced ciphertext, and
an **empty** keyring correctly failed to (the negative control, without which "it decrypted" only
proves gpg found *a* key somewhere).

---

## Recovering with the off-site copy only

The order matters: restore data last, because everything before it can be done while the dump
downloads.

1. **Get the dump**: `scp root@185.226.65.96:/opt/datanika-backups/<newest>.sql.gz.gpg .`
2. **Decrypt it** with the escrowed key (procedure above). If this fails, stop — nothing later works.
3. **Rebuild the box from source**, per the July 2026 restore writeup
   (`plans/infra/notes/RESTORE_POINTER_GR_2026-07-17.md`).
4. **Restore `.env.docker` from `secrets/pointer-app.env.docker`.** ⚠️ This step is what makes the
   restored credentials usable. Doing it *after* users start reconnecting connections silently
   re-encrypts under a new key and the old ciphertext becomes unrecoverable.
   ⚠️ **Then add back `DATANIKA_OVERAGE_CHARGE_ENABLE=1`** — it is the one key the escrow copy lacks
   ([cloud#141]), so a restore that stops here comes up with overage charging **off** and bills
   nobody, silently, with every container healthy.
5. **Restore TLS** from `secrets/datanika-origin.{pem,key}`.
6. `gunzip -c dump.sql.gz | psql …`
7. **Generate a new off-site SSH key** (row 7 — the old one died with the box) and re-add it on Aweb.
8. **Re-import the backup public key** on the new box, or backups refuse to run — by design:
   `gpg --import deploy/server/backup-pubkey.asc`

---

## The third location — answered 2026-09-01

**The founder chose a personal cloud-storage account and reported placing both files in it**
([cloud#133]). What they reported placing there:

- `secrets/datanika-backup-privkey.asc` (row 2)
- `secrets/pointer-app.env.docker` (row 3 — this is the file `CREDENTIAL_ENCRYPTION_KEY` lives in)

🔑 **The location is the cloud account, not a folder on the laptop.** They reach it by syncing a
local directory, and it is the **remote account** that is the third copy — the sync folder is on the
same machine rows 2 and 3 are already escrowed on, so naming the path would describe a copy that
dies with the machine it exists to survive. If the laptop is gone, sign in to the account from
anywhere and the files are there.

🚨 **The provider and account are deliberately NOT named in this file.** `datanika-core` is public
AGPL, and this document already states that row 3 decrypts every customer's warehouse credential;
adding "and a copy lives at *vendor X*" turns a named internet-facing account into a target and buys
a recoverer nothing, because they would still need its password. The two non-public records carry
the vendor and account:

- `plans/SECRETS_INVENTORY.md` rows 38–39 (local-only, outside every git repo)
- **[cloud#133]** (private repo)

### Three caveats — the gap is narrowed, not closed

Recorded honestly because a third copy that is believed and wrong is worse than none.

1. ⚠️ **Nobody has verified the files are actually there.** This section records *what the founder
   reported*, not a listing anyone performed. I did not look, deliberately — reading either file
   serves no purpose here and copying secret material into a transcript is how the Docs-QA password
   leaked three times. **Before relying on this in a recovery, confirm the two files exist** — and
   confirm it by *presence and size*, never by printing a value.

2. ⚠️ **The backup private key has no passphrase** (`cron` runs `backup-offsite.sh` unattended, so it
   cannot have one — see row 2 and the fingerprint note in `SECRETS_INVENTORY.md`). Unless it was
   encrypted before being placed, the escrowed copy is **the key in the clear, behind that account's
   password alone**. I advised `gpg -c` on the file before uploading. **Whether that was done is
   UNVERIFIED — do not assume either way.** If it was not, the account password is the only thing
   standing between an account compromise and every off-site dump. Ask the founder; if the answer is
   no, `gpg -c` it and re-upload, and record the answer here.

3. ⚠️ **The account's own recovery key is now a dependency of our disaster recovery.** Cloud-storage
   providers that encrypt client-side issue a recovery key that is the only way back in when the
   password is lost. **If that recovery key lives solely on the same laptop, the third location is
   not independent** — the laptop failure this escrow exists to survive would take the password
   manager, the `secrets/` directory *and* the way back into the escrow account together. Founder to
   confirm where the recovery key is kept. Until they do, treat row 2 and row 3 as **2 verified
   copies plus 1 reported copy of unconfirmed independence**, which is what the table says.

### What is still owed

- Confirm the two files are present (caveat 1).
- Confirm whether the backup key was encrypted before upload (caveat 2).
- Confirm the account recovery key is not stored only on the escrowed laptop (caveat 3).

Until all three are answered, `secrets/` remains load-bearing: it is not a convenience copy, it is
half of the disaster recovery plan.

[core#471]: https://github.com/datanika-io/datanika-core/issues/471
[core#675]: https://github.com/datanika-io/datanika-core/issues/675
[core#748]: https://github.com/datanika-io/datanika-core/issues/748
[core#954]: https://github.com/datanika-io/datanika-core/issues/954
[core#969]: https://github.com/datanika-io/datanika-core/issues/969
[core#970]: https://github.com/datanika-io/datanika-core/issues/970
[cloud#133]: https://github.com/datanika-io/datanika-cloud/issues/133
[cloud#141]: https://github.com/datanika-io/datanika-cloud/issues/141
[landing#389]: https://github.com/datanika-io/datanika-landing/issues/389
