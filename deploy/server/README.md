# Canonical copies of production box configuration

Files here are **not deployed from this directory.** They are checked-in copies of configuration that
lives on the servers, kept in git so that a box loss is not also a configuration loss.

**Why this directory exists.** These copies previously lived only in `plans/infra/scripts/`, a local
directory outside every git repo — no reflog, no remote, no recovery. The Hetzner box was terminated
on 2026-07-14 and took the server, its data and its backups with it; prod was rebuilt from source.
The only thing that made that survivable was having the configuration written down somewhere. Writing
it down somewhere unversioned was the remaining half of the problem.

🚨 **"Applied by" means INSTALLED by, not scheduled by** — the distinction is [core#747], and this
table once blurred it. `backup-offsite.sh` and `restore-drill.sh` said *"cron, 03:00"* and
*"monthly cron"*, which names what **runs** the copy on the box and says nothing about what **puts
it there**. Read quickly, that is a column that looks filled in. Nothing put them there, and the
deploy tarball ships this whole tree to `/opt/datanika/datanika/deploy/server/`, which made it worse
rather than better: the correct content sat on the box at a path nothing reads, beside the stale
copy that actually ran.

✅ **Closed 2026-09-04.** `scripts/install-server-scripts.sh` installs the five files below, and
`deploy-pointer.yml` invokes it immediately after the source transfer — before the build, so a bad
install aborts the deploy while production is still untouched. It is idempotent (copy + hash
compare), atomic per file (temp file + rename, because `backup-offsite.sh` may be mid-run at 03:00),
and **fails closed** if its own list is truncated.

| File | Lives on the box at | Installed by | Then run by |
|---|---|---|---|
| `backup-offsite.sh` | `/opt/datanika/scripts/backup-offsite.sh` | ✅ `scripts/install-server-scripts.sh`, from the deploy | cron, 03:00 |
| `restore-drill.sh` | `/opt/datanika/scripts/restore-drill.sh` | ✅ `scripts/install-server-scripts.sh` | cron, monthly 05:00 |
| `rebuild-parity-drill.sh` | `/opt/datanika/scripts/rebuild-parity-drill.sh` | ✅ `scripts/install-server-scripts.sh` | cron, monthly 05:30 |
| `export-prod-settings.sh` | `/opt/datanika/scripts/export-prod-settings.sh` | ✅ `scripts/install-server-scripts.sh` | cron, every 5 min |
| `backup-pubkey.asc` | `/opt/datanika/scripts/backup-pubkey.asc` | ✅ `scripts/install-server-scripts.sh` (the file); `gpg --import` into root's keyring stays manual | `backup-offsite.sh` |
| `deploy/apache/app.datanika.io.conf` | `/etc/apache2/sites-enabled/zapp-datanika-io.conf` | `scripts/sync-vhosts.sh` — this is the directory it applies | Apache |
| `deploy/apache/staging-app.datanika.io.conf` | `/etc/apache2/sites-enabled/zstaging-app-datanika-io.conf` | `scripts/sync-vhosts.sh` | Apache |
| `deploy/apache/datanika-prod-active.conf.example` | `/etc/apache2/conf-enabled/datanika-prod-active.conf` | rewritten on every blue/green swap by `deploy-bluegreen.sh`; the example is the shape, not the live colour | Apache |
| `networkd-99-datanika-dns.conf` | `/etc/systemd/network/10-netplan-eth0.network.d/99-datanika-dns.conf` | **by hand, deliberately** (see below) | `systemd-networkd` |
| `deploy-pointer.sh` | dev-machine fallback | n/a — never installed | by hand, only if CD is broken |

⚠️ **`export-prod-settings.sh` was absent from this table entirely** until 2026-09-04, while running
from cron every five minutes on production. A file missing from the inventory is worse than one
listed with the wrong column, because nothing prompts you to check it.

🚨 **`sync-vhosts.sh` applies `deploy/apache/`, not this directory** ([core#745]). The two vhosts
and the active-colour example used to have a second copy here (`apache-app.datanika.io.conf`,
`apache-staging-app.datanika.io.conf`, `apache-prod-active-ports.conf`). Those copies were
byte-identical on 2026-09-04 (`9e2cf96e`, `98ff0099`, and the ports file matched
`datanika-prod-active.conf.example`), which is what made them harmless then and dangerous later:
an edit to the copy in this directory is real, reviewed, merged and deployed, and reaches no
vhost. The copies are gone.
`tests/test_deploy/test_server_script_coverage.py::test_no_file_under_deploy_server_duplicates_a_file_under_deploy_apache`
fails if a file here is the same content as a file under `deploy/apache/` again.

**Coverage is enforced, not remembered.** `tests/test_deploy/test_server_script_coverage.py` parses
the installer's own `INSTALL` / `INSTALL_DATA` arrays and requires every file in this directory to be
either installed or listed in its `NOT_INSTALLED` map **with a reason**. It also asserts
`deploy-pointer.yml` actually *calls* the installer — an installer nothing runs is core#747 one level
up, and looks identical to a fix.

**Verifying an install by hand** (the deploy does this for you now; this is for incident work):

```bash
K="-i ~/.ssh/id_ed25519"; B=root@185.25.22.188
ssh $K $B "sha256sum /opt/datanika/scripts/<file>"
sha256sum deploy/server/<file>          # the two must match, and you must LOOK
```

Never edit the box copy — that is how the repo copy and the running copy diverge silently, which is
the condition this directory exists to end.

[core#745]: https://github.com/datanika-io/datanika-core/issues/745

[core#747]: https://github.com/datanika-io/datanika-core/issues/747

## Two things to know before trusting a file here

**The box is the source of truth, not this directory.** These copies drift, and they drift silently.
When they were moved here on 2026-08-31 the two Apache vhosts were **21 and 23 lines behind the box**
— missing the entire `SetEnv proxy-nokeepalive 1` fix and the comment explaining the 502 race it
solved. Nothing had flagged it, because nothing compares them. **Refresh from the box before relying
on one**, and refresh it here in the same change:

```bash
ssh -i ~/.ssh/id_ed25519 root@185.25.22.188 \
  'cat /etc/apache2/sites-enabled/zapp-datanika-io.conf' > deploy/apache/app.datanika.io.conf
```

**`deploy/apache/datanika-prod-active.conf.example` is a snapshot of a value that alternates.**
`deploy-bluegreen.sh` rewrites `/etc/apache2/conf-enabled/datanika-prod-active.conf` on every swap.
The example records a shape, never which colour is live — read that from the box, per
`docs/INFRA_RULES.md` §3. It does not live in this directory, and `sync-vhosts.sh` does not apply it.

## `backup-pubkey.asc` is a PUBLIC key, and that is the point

The off-site backup leg is encrypted to it ([core#675]). Committing the **public** half to a public
AGPL repo is safe and deliberate — a rebuilt box needs it before it can take a backup at all, and
`backup-offsite.sh` refuses to run without it rather than silently shipping plaintext.

🚨 **The private half must never appear here, on the off-site host, or in any backup.** It is on the
production box (root-only keyring) and escrowed on the founder's dev machine. Co-locating it with
the ciphertext it decrypts is the specific remedy forbidden by [core#748]; see
`docs/runbooks/RUNBOOK_RESTORE_PREREQUISITES.md`. `tests/test_deploy/test_backup_encryption.py`
asserts the committed file is a public key block and that no deploy step ships key material
off-site.

## The one file that is deliberately not CD-synced

`networkd-99-datanika-dns.conf` is applied **by hand**. A pushed network configuration that fails
leaves the box unreachable with no way in to revert it, so this one trades automation for a rollback
that always works:

```bash
rm -rf /etc/systemd/network/10-netplan-eth0.network.d && networkctl reload
```

A netplan `99-*.yaml` drop-in does **not** work for this: netplan *unions* `nameservers.addresses`
rather than replacing them, so the dead provider resolver survives. systemd list options accumulate
too, which is why the file opens with an empty `DNS=` to reset the list first.
