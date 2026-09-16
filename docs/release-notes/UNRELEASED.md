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

### ClickHouse: the connection's port field is the HTTP port, and the native port is derived

**Who this affects:** anyone whose ClickHouse connection stores a **native TCP** port — `9000`, or
`9440` for TLS — in the connection form's port field. **That connection stops loading** after this
change, with an error at dlt's `sync` step.

**What changed.** A ClickHouse destination's stored port is now sent to dlt as its **HTTP** port,
and the native port is derived from the connection's `secure` flag: **9000**, or **9440** with TLS.
Those are ClickHouse's own defaults and match both a stock server and ClickHouse Cloud. Previously
the stored port was passed through as dlt's *native* port and nothing set `http_port` at all, so
dlt fell back to its default of 8443 — a port that by itself switches the HTTP client to TLS.

**Why a connection like this may exist.** Before the change, a load would fail at `sync` with the
form's default port, because the native client was being pointed at the HTTP interface. Entering
the native port by hand was the only way to get a load past that step, so an operator who worked
around the old behaviour is exactly who this breaks.

**What to do.** Edit the connection and set the port field to the server's **HTTP** port —
normally `8123`, or `8443` with TLS — and leave the native port alone; it is no longer entered
anywhere. Nothing else about the connection changes.

**Also worth knowing:**

- **dbt transformations against such a connection were already broken.** dbt reads the stored port
  as the HTTP port and always has, so this change makes the loader agree with dbt rather than
  breaking something that worked.
- **Test Connection was not a check on this.** It speaks HTTP and passed against a connection that
  could not load. A passing Test Connection never meant the loader could write.
- **A non-default native port is not supported.** The derivation cannot express one, and there is
  no form field for it. Tracked as [core#1341].
- **The ClickHouse _source_ is unchanged.** Its driver speaks HTTP and keeps using the stored port.

[core#680]: https://github.com/datanika-io/datanika-core/issues/680
[core#1341]: https://github.com/datanika-io/datanika-core/issues/1341
