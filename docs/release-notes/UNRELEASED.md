# Unreleased

Notes for the next `v0.x` tag. Self-hoster-facing: written for someone who runs
`git checkout <tag> && docker compose up -d --build` and needs to know what will behave
differently afterwards.

> ⚠️ **This file does not reach the GitHub Release yet.** `release.yml` runs
> `gh release create --generate-notes` with no notes file, so the published notes are the
> auto-generated PR-title list and nothing else ([core#680]). Until #680 ships the optional
> `--notes-file` step, this file is reviewable in the PR that cuts the release but is **not**
> what a self-hoster sees on the release page. Whoever implements #680 should wire it to
> `docs/release-notes/<tag>.md`; this is the first file written for that convention.

## Breaking

Add a section here for anything that behaves differently for a self-hoster after the next tag, in
the shape `v0.2.0.md` uses: who this affects, what changed, and what to do. The file is rotated to
`<tag>.md` when that tag is cut.

### SQL Server and Synapse **sources** now require an encrypted session

**Who this affects:** anyone with a SQL Server (`mssql`) or Synapse **source** connection whose
server will not negotiate TLS with the client — encryption turned off on the instance, or only
protocol versions the client's OpenSSL refuses. For such a connection, Test Connection, the catalog
read and every upload from it now stop at login, where before they connected. Nothing else about
the connection changes and no connection needs to be saved again.

**Who this does not affect.** A server that already negotiated TLS behaves exactly as before: Azure
SQL and Azure Synapse require it on their side, and a SQL Server instance with a certificate
configured — including the self-signed one it generates for itself — negotiates it. The SQL Server
and Synapse **destinations** are untouched: they connect through the ODBC driver, which does not
read this file, and they already carry the equivalent keyword in their own connection string
([core#1379]).

**What changed.** The package now ships `datanika/pymssql_freetds.conf`, a FreeTDS client
configuration whose `[global]` section sets `encryption = require`, and importing `datanika` points
`FREETDSCONF` at that file when the variable is unset or empty. `pymssql` carries its own FreeTDS
inside the wheel and takes its client settings from that file rather than from a connection option,
so this covers every login the product makes through it: Test Connection, the catalog read and the
upload source ([core#1441]).

**What to do.** Nothing, if your server negotiates TLS. If it does not, either configure a
certificate on the server — SQL Server accepts a self-signed one — or point `FREETDSCONF` at a
FreeTDS configuration of your own, in the environment the app and Celery worker containers read
(`.env.docker` in the shipped compose file).

**🚨 Your own `FREETDSCONF` replaces the shipped file entirely.** FreeTDS reads **one**
configuration file, so pointing the variable at your own path means the shipped `[global]` section
is not read at all — its `encryption` line included, and anything this file gains later. Copy
`datanika/pymssql_freetds.conf` and edit the copy rather than starting from an empty one. The
variable is read at each login, so setting it in the container environment is enough; there is
nothing to rebuild.

**Also worth knowing:**

- **The shipped file's comments are the reference for what each setting does**, and for what it
  does not do about server certificates ([core#1379]). Read it before replacing it.
- **`pymssql`'s own `encryption=` connection keyword is a different thing** and does not reach the
  FreeTDS inside the wheel. This configuration file is where the setting lives.
- **Synapse's Test Connection gained its login timeout in the same change** ([core#1443]): it had
  been passing a keyword `pymssql.connect` does not accept, so it raised before reaching the
  server. That is a fix rather than a break — but it is why a Synapse connection that never got as
  far as a login may now report one.

[core#680]: https://github.com/datanika-io/datanika-core/issues/680
[core#1379]: https://github.com/datanika-io/datanika-core/issues/1379
[core#1441]: https://github.com/datanika-io/datanika-core/issues/1441
[core#1443]: https://github.com/datanika-io/datanika-core/issues/1443
