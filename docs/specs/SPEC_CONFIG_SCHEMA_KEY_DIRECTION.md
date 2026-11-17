# SPEC — Direction for connection config key names

**Author**: Product · **Date**: 2026-11-16 · **Status**: contract, ready for Engineering
**Tracking**: [core#1603](https://github.com/datanika-io/datanika-core/issues/1603) acceptance criteria 2 and 4
**Implementation**: Engineering edits `CONFIG_SCHEMAS`. This spec does not rename a key.
**Verified against**: core `origin/dev` @ `3154dec3c394c41b07205c6976a841ccfb88ba78`.
Re-derive before editing the schema. The symbols below are from that revision.

Acceptance criteria 3 and 5 of [core#1603] shipped in
[pull request 1616](https://github.com/datanika-io/datanika-core/pull/1616).
This spec does not rebuild that guard and does not edit those docstrings.

---

## Decision

`CONFIG_SCHEMAS` moves to the keys the connection form writes and the loader requires.
The form stays. Stored rows keep `connections.config_encrypted` as it is.

`openapi_inline.py` builds `ConnectionCreate` from `CONFIG_SCHEMAS`, and
`GET /api/v1/meta/connection-types` returns the same document. That is the
published surface. `connection_schemas.py` still lists config validation as
future work, so a create call does not reject a body the schema would reject.
`additionalProperties` is `false` on every `_schema()`.

The issue table names four types. The 2026-09-26 comment on the issue measured
nine: a type whose schema requires a key `_build_config` cannot write. The
other five are `csv`, `google_sheets`, `json`, `parquet`, and `zendesk`.
This ruling covers all nine. `zendesk` is the same subclass as the credential
renames, and it interlocks with [core#1606] ruling 3.

Two shapes are not a rename of one value:

- `jira` `domain` is the Atlassian subdomain. The loader builds
  `https://{domain}.atlassian.net/`. `server_url` is a different value.
- `shopify` `store` is the shop name. The loader builds
  `{store}.myshopify.com`. `shop_url` is a different value.
- `google_sheets` `spreadsheet_url` is the URL the loader requires.
  `spreadsheet_id` is a different value.
- `csv`, `json`, and `parquet` `path` is a filesystem path. The file loader
  requires `bucket_url`. An uploaded file is stored as `uploaded_file_id` and
  resolved to a `bucket_url` in `run_upload` before that loader runs.

Do not copy `server_url` into `domain`, `shop_url` into `store`,
`spreadsheet_id` into `spreadsheet_url`, or `path` into `bucket_url`.

## What each schema publishes

Required keys are the keys a form save writes and the loader's error names.
A name the loader already accepts as an alias stays a property and stays off
`required`, until a later contract release removes it. A name the loader does
not read leaves the schema. A validating client that sends only the alias then
fails `required`. The server still runs a stored row that holds the alias,
because this change does not drop the loader's existing read.

| type | `required` after this change | stays as an optional property | leaves the schema |
|---|---|---|---|
| `jira` | `api_key`, `email`, `domain` | `api_token` | `server_url` |
| `shopify` | `api_key`, `store` | `access_token` | `shop_url` |
| `slack` | `api_key` | `bot_token` | `token` |
| `salesforce` | `access_token`, `instance_url` | `api_key` | `client_id`, `client_secret`, `username`, `password`, `security_token` |
| `zendesk` | `subdomain`, `email`, `api_key` | `api_token`, `domain` | — |
| `google_sheets` | `spreadsheet_url`, `service_account_json` | — | `spreadsheet_id` |
| `csv`, `json`, `parquet` | one of `bucket_url` or `uploaded_file_id` | the other of those two | `path` |

Credential properties keep `format: password`. That includes the optional
aliases `api_token`, `access_token`, `bot_token`, and `api_key` where that
alias is the credential. `uploaded_file_id` is an integer. `bucket_url` is a
string.

For `csv`, `json`, and `parquet`, the form accepts a bucket URL, an uploaded
file, or both. The published schema uses `anyOf` so at least one of those two
satisfies `required`, and a body that carries both still validates. Requiring
both, or requiring `path`, publishes a body the form does not have to save.

`salesforce` is an auth-model change. The five-field password flow is not what
the form collects and not what the loader runs. The schema becomes the bearer
token plus the instance URL. Do not add those five fields to the form.

## Zendesk and core#1606 ruling 3

Ruling 3 requires `email` on the form. This schema keeps `email` required.
The fallback loader still sends `Authorization: Bearer` with the token. This
change does not switch that header to email-plus-token, and it does not make
the loader refuse a missing email. Ruling 3 left that composition as its own
issue. The credential name in the schema becomes `api_key`, which is the key
the form writes and the key the loader's error names. `api_token` stays the
optional alias that loader already reads:
`config.get("api_key") or config.get("api_token", "")`.

## Expand/contract

A rename that changes which key is written into `connections.config_encrypted` is
expand/contract, in the sense of
[`SPEC_EXPAND_CONTRACT_MIGRATIONS.md`](SPEC_EXPAND_CONTRACT_MIGRATIONS.md).
The column is unchanged. The previous release is still serving while the new
container migrates, and the worker restarts on its own, so it keeps reading
the key it shipped with.

Release N reads the old name and the new name, and keeps writing the old name.
It may write the new name beside the old one. It does not rewrite existing
rows, and it does not stop reading the old name.

Release N+1, after release N is what production runs, writes only the new name
and then stops reading the old name. A rewrite of leftover rows belongs to
that later release.

This schema edit does not open that window. The form already writes the keys
in the table above, and the loader already requires them. Editing
`CONFIG_SCHEMAS` changes the document `openapi_inline` publishes. It does not
write `connections.config_encrypted`. Do not migrate rows in the same change. Do not
remove an alias read in the same change. Removing the read is the contract
half.

## Same change, still not this document

When Engineering edits the schema, `AI_IMPORT_GUIDE.md` follows it. The Slack
example there sends `token`, which this revision's loader does not read. The
Jira example sends `api_token`, and the Shopify example sends `access_token`.
Those two still run, as aliases. The Salesforce example already sends
`access_token` and `instance_url`, and its comment tells the reader not to
retarget that example at the five-field schema. After the schema matches that
example, that comment comes out. The guide does not move to the five-field
flow.

## Out of scope

- Renaming a key in `connection_schemas.py`, `connection_state.py`, or
  `dlt_runner.py` inside the pull request that adds this spec.
- The redaction guard and the docstring correction from pull request 1616.
- `google_analytics`. The form field is `api_key` and the stored key is
  already `service_account_json`. [core#1606] ruled that field.
- `csv` / `json` / `parquet` local-path policy. Dropping `path` from the
  schema does not make a filesystem path a hosted warehouse.
- Zendesk auth composition. Ruling 3 of [core#1606] keeps that separate.
- Connector guides on the landing site.

[core#1603]: https://github.com/datanika-io/datanika-core/issues/1603
[core#1606]: https://github.com/datanika-io/datanika-core/issues/1606
