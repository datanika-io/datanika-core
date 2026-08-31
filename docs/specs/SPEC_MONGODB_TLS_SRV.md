# SPEC — MongoDB connection options: TLS, DNS seed list (SRV), and the auth database

**Author**: Product · **Date**: 2026-08-30 · **Status**: contract, ready for Engineering
**Tracking**: [core#626](https://github.com/datanika-io/datanika-core/issues/626)
**Implementation**: Engineering (core). Product owns this spec and the acceptance criteria.
**Verified against**: `origin/dev` @ `f6851eb`, including [core#625] and [core#630], both merged today.

---

## 1. Scope, and one correction that shrinks it

Engineering [confirmed empirically](https://github.com/datanika-io/datanika-core/issues/626#issuecomment-5467981850)
that `MongoClient('mongodb://…')` builds **no SSL context at all**, so Atlas — and DocumentDB, Cosmos
DB's Mongo API, and any `net.tls.mode: requireTLS` deployment — cannot connect today.

**SRV is not a missing dependency.** `dnspython` is installed and `parse_uri("mongodb+srv://…")`
performs a real DNS lookup in our venv now. `build_connection_uri` simply always writes the literal
`mongodb://` and the form collects `host` + `port` separately. **This is config surface, not a new
integration** — which is why it is a form-shape decision rather than an engineering investigation.

---

## 2. What I found while speccing it: `auth_source` shipped with no way to enter it ⭐

[core#625] added `auth_source` to `CONFIG_SCHEMAS["mongodb"]` this morning, with a test whose docstring
reads *"A setting with no surface is the core#499 mistake."* **The setting still has no surface.**

- `mongodb_fields()` in `ui/components/connection_config_fields.py` renders host, port, user, password,
  database. **No `auth_source` input.**
- `ConnectionState._build_config()`'s `mongodb` branch writes those same five keys and nothing else.
- `ConnectionState._populate_form_from_config()`'s `mongodb` branch reads those same five.

So `auth_source` is reachable only through the raw-JSON escape hatch — **and it does not survive.**
Set it via raw JSON, later open that connection in the structured form and click Save, and
`_build_config()` rebuilds the config from five fields: **the key is silently dropped** and the
connection reverts to `admin` on its next run. For a user whose Mongo user lives inside the target
database — the entire reason [core#550] exposed this field — that is a working connection that
silently stops working after an unrelated edit.

**The test that was supposed to catch this asserts on `CONFIG_SCHEMAS`, not on the form.** The JSON
Schema and the Reflex form are two hand-maintained string sources with **no code linking them**
(`connection_schemas.py`'s own docstring still says *"(future) UI form rendering"*). A schema
assertion is not a UI assertion, and the gap between them is exactly where this fell.

**Therefore this spec covers `auth_source` too.** It is one field in the same `mongodb_fields()`
function, it is the field TLS/SRV has to compose with, and shipping TLS while leaving a live
silent-data-loss bug next to it would be indefensible. **The drop-on-edit defect is filed separately as
[core#638]**, so it stays tracked even if delivery splits — and because the general case (any config
key present in one serialiser and absent from the other) is bigger than MongoDB.

---

## 3. Decisions

### D1 · Explicit fields. No raw connection-string passthrough — but not for the stated reason.

**Ratifying Engineering's inclination: no passthrough.** One of the two reasons given for it does not
hold, and it matters that the record is right, because a false reason gets reused.

> ❌ *"a passthrough bypasses `validate_egress_host`"* — **there is nothing to bypass.**
> `validate_egress_host` is called in exactly one place in the connection lifecycle
> (`connection_service.py:364`, guarded by `if base_url:`) plus two REST/OpenAPI-specific paths.
> MongoDB configs have no `base_url` key, so **MongoDB connections are entirely outside that guard
> today** — at create, at test and at run. Per-field assembly does not preserve a protection that was
> never there.
>
> And this is **not a gap to file**: the guard's own comment states the exclusion is deliberate —
> *"Only fires when a base_url is present, so DB connectors and hardcoded-host SaaS (stripe/github/…)
> are unaffected"* ([core#338]). Whether DB connectors should be inside an SSRF gate is a settled
> decision, not something this spec reopens. The correction matters only because a reason that does
> not hold gets reused in the next design argument.
>
> ✅ *"it puts the password in a field we do not classify as a secret"* — **this one is decisive**, and
> more so than it looks. Reflex form inputs here are **controlled** (`value=` / `on_change=`), so a
> pasted connection string ships to the server on every keystroke and lands in server-side state. It
> would be a `type="text"` input, so the password is **visible on screen** — and Product photographs
> this exact form for every connector guide. It is also autofill-targetable, which is the precise
> shape [core#618] was about and [core#630] was written to prevent.

The same reasoning rules out a "paste your Atlas string here and we'll split it up" helper, which was
the tempting middle path. The friction it would remove is removed by documentation instead (§6).

### D2 · Two checkboxes, and SRV drives TLS because the protocol says so.

| Field | Control | Default |
|---|---|---|
| `srv` | checkbox — **Use DNS seed list (mongodb+srv)** | off |
| `tls` | checkbox — **Use TLS** | off |

**They are separate keys, not one.** Self-hosted `requireTLS` on an ordinary host needs TLS without
SRV, and that is a real and common deployment.

**But SRV implies TLS, and that is the MongoDB URI specification, not our invention** — the
`mongodb+srv` scheme defaults `tls=true`, and pymongo honours it. So:

- **SRV on ⇒ TLS on, forced.** The TLS checkbox renders **checked and disabled** with the SRV callout
  explaining why. Do not let a user construct a combination the driver will override anyway; a control
  that lies about its effect is worse than no control.
- **SRV off ⇒ TLS is independently settable.**
- `build_connection_uri` must still emit `tls=true` explicitly when SRV is on. Relying on a driver
  default that a future pymongo could change is how a security property becomes an accident.

**SRV on ⇒ no port.** `mongodb+srv://host:27017/` is invalid per the spec — the SRV records supply the
ports. So when SRV is checked, the **Port field is hidden** and no port is written into the URI or the
config.

> ⚠️ **This introduces the first dependent field in the connection form.** There is no precedent: the
> only conditional rendering today is type→field-group, the raw-JSON toggle, and one checkbox→callout
> (`cluster_hint`). Keep it to exactly this — hide Port, disable TLS, show one callout. Do not
> generalise a dependent-field framework for two controls.

### D3 · Inference is limited to normalising what the user pasted, and it must be visible.

**Do not infer SRV from the hostname.** An Atlas host (`cluster0.abc.mongodb.net`) is
indistinguishable from any other hostname. There is no signal, so there must be no guess.

**Do normalise a pasted scheme.** Atlas hands the user one string, and the single most likely support
ticket is a full connection string in the Host box. When the Host value starts with `mongodb://` or
`mongodb+srv://`, strip the scheme and everything from the first `/`, `?` or `@` onward, and — if the
scheme was `+srv` — **tick the SRV checkbox**.

Two constraints:
1. **The result must be visible.** The Host field visibly updates and the checkbox visibly ticks. A
   transformation the user cannot see is indistinguishable from a bug when it guesses wrong.
2. **Discard any credentials in the paste.** If the pasted string contained `user:pass@`, those
   characters are **dropped, not routed into the User/Password fields**. Moving a password from a
   text field into a password field still means it existed in the text field.

Nice-to-have, not a ship gate. If it complicates the PR, ship without it and let the docs carry it.

### D4 · `auth_source` gets a real field, and both serialisers get the line they are missing.

Optional text input labelled **Authentication database**, below Database, placeholder `admin`
(a technical identifier — exempt from i18n per WORKFLOW_RULES §6), with `connections.auth_source_hint`
below it.

**The load-bearing half is not the input.** Every new key — `auth_source`, `tls`, `srv` — needs an
explicit line in **both** `_build_config()` and `_populate_form_from_config()`, plus a `form_<field>`
var and its setter. A key present in one and absent from the other is silently dropped on the next
save. That is the live `auth_source` defect (§2), and it will reproduce identically for `tls` and
`srv` if only one side is written.

**Acceptance criterion 6 exists to catch exactly this**, and it must be written to fail against the
current code.

### D5 · Do not reuse `form_secure`, and reset booleans on type change.

`ConnectionState.set_form_type()` resets the port default and the test verdict — **not booleans**.
`form_secure`, `form_cluster_replication` and `form_oracle_use_sid` survive a mid-form type switch.

So a user who ticks ClickHouse's *Use HTTPS (TLS)* and then switches the dropdown to MongoDB would
arrive with TLS silently pre-checked if the two shared a state var. Against a server without TLS that
is a connection failure with no visible cause.

1. **Use a dedicated `form_mongodb_tls`.** Do not reuse `form_secure` because the labels rhyme.
2. **Reset every boolean form field in `set_form_type()`.** This also fixes the existing
   ClickHouse↔Oracle carry-over, which is the same bug with no reporter.

### D6 · Existing connections: nothing changes, and that is a requirement, not a side effect.

Config is a single Fernet blob in `connections.config_encrypted` (`Text`), so **no migration** — a new
key is purely a change to what gets JSON-encoded.

Follow the ClickHouse `secure` precedent: `config.get("tls", False)` / `config.get("srv", False)`. An
existing stored connection has neither key, both evaluate `False`, and `build_connection_uri` emits a
**byte-identical** URI to today's.

**Never auto-enable TLS on an existing connection.** Turning it on for a server that does not offer it
converts a working connection into a failing one, and the user did not touch anything. Off by default,
always, including for Atlas-shaped hosts.

### D7 · Explicitly out of scope, with reasons

| Not shipping | Why |
|---|---|
| `replicaSet` URI option | SRV covers Atlas, which is the actual demand. Self-hosted multi-node without SRV is a case we have zero evidence of. `build_connection_uri` is now the one place to add it — a few lines when someone asks. |
| `tlsCAFile` / custom CA | Needs file upload, storage and encryption-at-rest for a cert. A materially larger feature. A self-signed MongoDB will fail verification, and the docs must say so rather than imply it works. |
| `tlsAllowInvalidCertificates` | **Refused, deliberately.** It turns TLS into theatre and it is the option that ends up copy-pasted into someone's production setup guide. The answer to a self-signed cert is the CA, not disabling verification. |
| Putting MongoDB inside `validate_egress_host` | Its exclusion is an explicit [core#338] decision covering all DB connectors, not an oversight (D1). Not this spec's to reopen. |

### D8 · Test Connection needs a longer server-selection timeout when SRV is on.

`_test_mongodb` uses `MongoClient(uri, serverSelectionTimeoutMS=5000)`. SRV adds a DNS round trip
before any connection is attempted, then TLS adds a handshake, against a cluster that may be on
another continent. **On this box that budget is not obviously safe**: a dead provider resolver was
costing 7.9–9.5 s per lookup until 2026-08-29, and a cold `api.paddle.com` lookup measured 20.1 s.

A timeout against a *working* Atlas cluster surfaces as "connection failed — check your credentials",
which sends the user to re-check credentials that were always correct. That is the worst failure mode
a setup flow can have.

**Require at least `serverSelectionTimeoutMS=10000` when `srv` is on.** Simply raising it
unconditionally is also acceptable.

---

## 4. The form, after this change

```
Connection Name *        [ ]
Type                     [ mongodb                    ▾ ]

Host *                   [ cluster0.abc.mongodb.net     ]
Port                     [ 27017 ]        ← hidden when DNS seed list is on
User                     [ ]
Password *               [ •••• ]         ← type=password, secure_input
Database *               [ ]
Authentication database  [ admin ]        ← NEW; blank means admin
                         ⓘ Where your MongoDB user was created…

[ ] Use DNS seed list (mongodb+srv)       ← NEW
[x] Use TLS                               ← NEW; checked + disabled when SRV is on

ⓘ A DNS seed list connection always uses TLS and takes no port.
  MongoDB Atlas gives you one of these.        ← shown only when SRV is on
```

All text/secret inputs keep going through `config_input(...)` from `secure_input.py` ([core#630]) —
`autoComplete`, `data-1p-ignore`, `data-lpignore`, and the non-positional `cfg-<field>` name/id.
Checkboxes stay bare `rx.checkbox(...)`, matching ClickHouse and Oracle; they are not an autofill
target and `secure_input.py` has no boolean variant by design.

**URI assembly stays in `build_connection_uri` and nowhere else.**
`test_mongodb_uri.py::test_nobody_assembles_a_mongo_uri_by_hand_any_more` greps
`connection_service.py` and `dlt_runner.py` for `r'f?"mongodb(?:\+srv)?://\{'` and already tolerates
`+srv` — so the guard permits this work and still fails a second assembly site. Both the run path
(`_build_mongodb_source`) and the test path (`_test_mongodb`) call the one builder as of [core#625];
that stays true.

Expected output shape:

```
srv off, tls off →  mongodb://u:p@host:27017/db?authSource=admin        (unchanged)
srv off, tls on  →  mongodb://u:p@host:27017/db?authSource=admin&tls=true
srv on           →  mongodb+srv://u:p@host/db?authSource=admin&tls=true   (no port)
```

Credentials stay `quote_plus`-encoded. The existing multi-parameter query string must remain a single
`?` with `&`-joined pairs — `test_mongodb_uri.py` asserts `uri.count("?") == 1`.

---

## 5. i18n — 5 new keys × 9 locales

| Key | `en` |
|---|---|
| `connections.mongodb_srv` | Use DNS seed list (mongodb+srv) |
| `connections.mongodb_tls` | Use TLS |
| `connections.mongodb_srv_hint` | A DNS seed list connection always uses TLS and takes no port. MongoDB Atlas gives you one of these. |
| `connections.auth_source` | Authentication database |
| `connections.auth_source_hint` | Where your MongoDB user was created. Leave blank for `admin`, which is where Atlas and most deployments put them. |

**Do not reuse `connections.secure`** ("Use HTTPS (TLS)") — HTTPS is wrong for the MongoDB wire
protocol, and one shared key across two connectors makes both harder to change.

No placeholder keys: `admin` and `27017` are technical identifiers, exempt under WORKFLOW_RULES §6.

The `description` strings in `connection_schemas.py` are raw English and feed
`/api/v1/meta/connection-types`, not the UI. Update them for the new keys in the same PR — they are
what an AI agent reads.

---

## 6. Acceptance criteria

Product verifies these on prod after promotion.

1. A MongoDB connection with **TLS off and no `auth_source`** produces a URI byte-identical to today's.
2. **TLS on** appends `tls=true` and the connection reaches a `requireTLS` server.
3. **DNS seed list on** produces `mongodb+srv://…` with **no port**, carrying `tls=true` explicitly.
4. With DNS seed list on, the **Port field is not rendered** and the TLS checkbox is **checked and not
   interactive**.
5. `auth_source` has a **visible input**, and a value entered there survives save → reopen → save.
6. **The regression test for §2**: a connection whose config contains `auth_source` (or `tls`, or
   `srv`), opened in the structured form and saved unchanged, retains every key. **Written red-first
   against current `dev`, where it must fail on `auth_source`.**
7. Switching the type dropdown from `clickhouse` (secure ticked) to `mongodb` leaves MongoDB's TLS
   checkbox **unticked**.
8. **A real MongoDB Atlas M0 cluster connects**, Test Connection passes, and a pipeline moves rows.
9. Test Connection against a reachable Atlas cluster does **not** time out — the SRV lookup plus TLS
   handshake completes inside the budget (D8).
10. All 5 keys in all 9 locale files; `test_all_locales_have_same_keys` green.
11. No password appears in any failure message — `describe_connection_failure` already redacts
    URI-embedded credentials and is tested with MongoDB URIs; `+srv` URIs must be covered too.

> **Criterion 8 is the one that closes the issue, and it needs an Atlas M0 signup.** Per the CEO's
> 2026-07-21 reclassification, that is agent-doable — persisted browser profile plus Gmail — so
> **attempt it and escalate only on a genuine wall** (SMS, payment card, unsolvable captcha), naming
> what is still needed. Until criterion 8 passes, the feature is *shipped, unverified against Atlas*,
> and the docs must not claim otherwise.

---

## 7. Docs, and the two-step Growth is already mid-way through

`datanika-landing/src/content/connectors/mongodb.md` currently carries *"MongoDB Atlas requires
allowlisting IPs"*, which implies Atlas otherwise works. It does not.

**Step 1 (Growth, in flight today):** state that Atlas, DocumentDB, Cosmos DB's Mongo API and any
`requireTLS` deployment are **not supported yet**, per Engineering's assessment on the issue.

**Step 2 (after this ships):** flip it to a supported path with a worked Atlas example — take the
`mongodb+srv://` string Atlas gives you and map each part to a form field, with a note that
Authentication database stays blank because Atlas users live in `admin`. That decomposition is what
replaces the connection-string field we deliberately did not build (D1), so it is not optional polish;
it is the other half of the design.

Also document: the CA limitation (self-signed certs are not supported yet, D7), and that TLS stays off
for existing connections until you tick it (D6).

Growth owns the page. Product supplies the worked example and re-captures
`public/docs/connectors/mongodb/02-add-connection.png` against the shipped form, since it will show
three fields it does not today.

---

## 8. Dependencies

**None.** `dnspython` is installed, `pymongo` 4.16.0 is installed, no new package, no migration, no
Infra change, no new credential. The only external requirement is an Atlas M0 for criterion 8, and it
gates *verification*, not implementation.

Filed separately by this spec:
- **[core#638]** — `auth_source` (and any config key not in both serialisers) is silently dropped on
  structured-form save. §2.

[core#638]: https://github.com/datanika-io/datanika-core/issues/638
[core#338]: https://github.com/datanika-io/datanika-core/issues/338
[core#499]: https://github.com/datanika-io/datanika-core/issues/499
[core#550]: https://github.com/datanika-io/datanika-core/issues/550
[core#618]: https://github.com/datanika-io/datanika-core/issues/618
[core#625]: https://github.com/datanika-io/datanika-core/issues/625
[core#626]: https://github.com/datanika-io/datanika-core/issues/626
[core#630]: https://github.com/datanika-io/datanika-core/pull/630
