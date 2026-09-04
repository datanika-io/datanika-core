# Canonical copies of production box configuration

Files here are **not deployed from this directory.** They are checked-in copies of configuration that
lives on the servers, kept in git so that a box loss is not also a configuration loss.

**Why this directory exists.** These copies previously lived only in `plans/infra/scripts/`, a local
directory outside every git repo — no reflog, no remote, no recovery. The Hetzner box was terminated
on 2026-07-14 and took the server, its data and its backups with it; prod was rebuilt from source.
The only thing that made that survivable was having the configuration written down somewhere. Writing
it down somewhere unversioned was the remaining half of the problem.

🚨 **"Applied by" means INSTALLED by, not scheduled by** — the distinction is [core#747], and this
table previously blurred it. `backup-offsite.sh` and `restore-drill.sh` said *"cron, 03:00"* and
*"monthly cron"*, which names what **runs** the copy on the box and says nothing about what **puts
it there**. Nothing does. The deploy tarball ships this whole tree to
`/opt/datanika/datanika/deploy/server/`, which makes it worse rather than better: the correct
content sits on the box at a path nothing reads, beside the stale copy that actually runs.

| File | Lives on the box at | Installed by | Then run by |
|---|---|---|---|
| `apache-app.datanika.io.conf` | `/etc/apache2/sites-enabled/zapp-datanika-io.conf` | `scripts/sync-vhosts.sh`, from the deploy | Apache |
| `apache-staging-app.datanika.io.conf` | `/etc/apache2/sites-enabled/` (staging vhost) | `scripts/sync-vhosts.sh` | Apache |
| `apache-prod-active-ports.conf` | `/etc/apache2/conf-enabled/datanika-prod-active.conf` | rewritten on every blue/green swap | Apache |
| `networkd-99-datanika-dns.conf` | `/etc/systemd/network/10-netplan-eth0.network.d/99-datanika-dns.conf` | **by hand, deliberately** (see below) | `systemd-networkd` |
| `backup-offsite.sh` | `/opt/datanika/scripts/backup-offsite.sh` | 🚨 **BY HAND — no workflow installs it** ([core#747]) | cron, 03:00 |
| `restore-drill.sh` | `/opt/datanika/scripts/restore-drill.sh` | 🚨 **BY HAND — no workflow installs it** ([core#747]) | cron, monthly 05:00 |
| `rebuild-parity-drill.sh` | `/opt/datanika/scripts/rebuild-parity-drill.sh` | 🚨 **BY HAND — no workflow installs it** ([core#747]) | cron, monthly 05:30 |
| `backup-pubkey.asc` | `/opt/datanika/scripts/backup-pubkey.asc` | **by hand** — `gpg --import` into root's keyring | `backup-offsite.sh` |
| `staging-docker-compose.yml` | `/opt/datanika-staging/docker-compose.yml` | staging deploy | compose |
| `deploy-pointer.sh` | dev-machine fallback | n/a — never installed | by hand, only if CD is broken |

**Installing one of the hand-installed files, and proving you did:** copy the committed bytes and
compare hashes on both sides. Never edit the box copy — that is how the repo copy and the running
copy diverge silently, which is the condition this directory exists to end.

```bash
K="-i ~/.ssh/id_ed25519"; B=root@185.25.22.188
scp $K deploy/server/<file> $B:/opt/datanika/scripts/<file>
ssh $K $B "chmod 0755 /opt/datanika/scripts/<file>; sha256sum /opt/datanika/scripts/<file>"
sha256sum deploy/server/<file>          # the two must match, and you must LOOK
```

[core#747]: https://github.com/datanika-io/datanika-core/issues/747

## Two things to know before trusting a file here

**The box is the source of truth, not this directory.** These copies drift, and they drift silently.
When they were moved here on 2026-08-31 the two Apache vhosts were **21 and 23 lines behind the box**
— missing the entire `SetEnv proxy-nokeepalive 1` fix and the comment explaining the 502 race it
solved. Nothing had flagged it, because nothing compares them. **Refresh from the box before relying
on one**, and refresh it here in the same change:

```bash
ssh -i ~/.ssh/id_ed25519 root@185.25.22.188 \
  'cat /etc/apache2/sites-enabled/zapp-datanika-io.conf' > deploy/server/apache-app.datanika.io.conf
```

**`apache-prod-active-ports.conf` is a snapshot of a value that alternates.** It records whichever
colour was serving when it was captured. It is here for its *shape*, never as a statement about which
colour is live — read that from the box, per `docs/INFRA_RULES.md` §3.

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
