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

**Rows 2 and 3 are the whole point of this document.** Everything else can be re-created from a
vendor, a certificate authority, or git. Those two cannot be re-created from anything.

### Why #3 is irreplaceable

`connections.credentials` is Fernet-encrypted with `CREDENTIAL_ENCRYPTION_KEY`. The ciphertext is in
the dump; the key is not. Lose the key and every customer's warehouse credential is permanently
undecryptable — the rows survive and are worthless. There is no derivation, no reset, no vendor to
ask.

### Why #2 exists at all, and why it is not stored with the backup

Since [core#675] the off-site copy is GPG ciphertext, because Aweb is a shared, general-purpose,
**unpatched** host ([landing#389]) running a VPN endpoint, three unrelated bots, a public web server
and Plausible. It has no business being able to read our users' rows.

🚨 **Never escrow the key onto Aweb.** Putting the Fernet key — or the backup private key — beside
the ciphertext it decrypts converts *"an attacker who reaches Aweb obtains encrypted credentials"*
into *"an attacker who reaches Aweb obtains our customers' warehouse credentials."* Aweb holds
**ciphertext and no key of any kind**, and that invariant is what the encryption is for.

---

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

Last run **2026-08-31: MATCH** (`8f835af6…`). `SECRET_KEY` and `POSTGRES_PASSWORD` also matched. The
two files differ by exactly one non-secret line (`DATANIKA_OVERAGE_CHARGE_ENABLE`), so a size
difference between them is **not** evidence of key drift — compare the value, not the file.

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

[core#675]: https://github.com/datanika-io/datanika-core/issues/675
[core#748]: https://github.com/datanika-io/datanika-core/issues/748
[landing#389]: https://github.com/datanika-io/datanika-landing/issues/389
