# SPEC — What Test Connection means on a local-file source

**Author**: Product · **Date**: 2026-09-03 · **Status**: contract, ready for Engineering
**Tracking**: [core#978] (duckdb) · [core#979] (sqlite) · answers [core#979] AC4 for both sides
**Implementation**: Engineering. Product owns this spec and the acceptance criteria.
**Verified against**: `origin/dev` @ `8fe8901`, by calling `ConnectionService.test_connection`
directly with controls in both directions (§2). Nothing below is read off source alone.

---

## 1. The question, and why the two filed bugs do not contain it

[core#978] and [core#979] are code defects and Engineering has them. This spec exists because fixing
them as filed leaves the real question unanswered:

> **What is Test Connection *claiming* when the source is a file on a local disk?**

Today it claims `Connected successfully`. That sentence is a promise about the user's data being
reachable, and on a local-file source the button is not in a position to make it — for three
independent reasons, none of which the two issues share:

1. **It answers from the wrong process.** `ConnectionState.test_connection_from_form` is a Reflex
   state handler and runs in the **web** container. The extract runs in **celery**. The two share
   exactly two named volumes (`dbt_projects`, `uploaded_files`); every other path differs.
2. **It can manufacture its own evidence.** SQLite is open-or-create, so the check *creates* the
   database it then reports finding.
3. **It can be structurally unable to succeed** and still return a confident sentence — DuckDB, on
   every input, blaming credentials a local file does not have.

🚨 **The through-line, and the reason this is a product question rather than three bug fixes: a
control that can only return one answer must not be rendered as if it could return two.** A green tick
that was a foregone conclusion is worse than no tick, because the user spends real trust on it. This
is [core#821]'s finding (twenty SaaS types painted green without making a request) arriving on the
**first screen a new user sees** — DuckDB is the connector our own docs recommend to someone who has
no warehouse and no credentials.

⚠️ It is also why [core#793]'s walkthrough cannot currently be captured as written: the guide's own
first step is a Test Connection that cannot go green on any machine.

---

## 2. The measurement

Run on `origin/dev` @ `8fe8901`. Each finding carries the control that makes it a finding rather
than a broken probe.

```
=== A. duckdb (#978) ===
  duckdb real file (path key)     -> ok=False  'Connection failed — check your credentials and
                                                network settings: connect(): incompatible f…'
  duckdb in-memory :memory:       -> ok=False  same
  duckdb real file (database key) -> ok=False  same
  CONTROL sqlite real file        -> ok=True   'Connected successfully'

=== B. sqlite (#979) ===
  file existed before : False
  verdict             : ok=True  'Connected successfully'
  file exists AFTER   : True      <-- created by the check
  CONTROL(neg) missing parent dir -> ok=False  'Connection failed — check your credentials and
                                                network settings: (sqlite3.OperationalError)…'

=== C. local-path file source: is there ANY tenancy boundary? ===
  csv connection, bucket_url = a shared directory -> ok=True 'Connected — found files matching *.csv'
  files that one connection yields                -> ['orgA_….csv', 'orgB_….csv']
  CONTROL(neg) nonexistent dir -> ok=False "No files matched '*.csv' under …"
  _test_file_source signature  -> (config, connection_type)   # takes no org
  'org' appears in its source  -> False
```

Three things the controls establish that the verdicts alone do not:

- **A is not a broken harness.** The sqlite control is the same call, the same branch, one line
  apart, and it is green. DuckDB has no working input.
- **B is not a checker with one possible answer.** The negative control fails. So `ok=True` on a
  path with nothing at it is a *wrong* answer, not an incapable one.
- **The B control also exposes something #979 did not file:** the *failure* message for sqlite is the
  same `check your credentials and network settings` string as duckdb's. **The misdirection is not a
  duckdb bug. It is the whole local-file class**, on both the success and failure paths.

### 2a. The prescribed read-only forms were run, not just written down

D1 names two specific incantations. A spec that hands an implementer an unverified one is worse than
a spec that stays vague, so both were executed:

```
sqlite:///file:<path>?mode=ro&uri=true   REAL file    -> ok=True
                                         ABSENT file  -> ok=False, and the file is NOT created
CONTROL, today's form  sqlite:///<path>   ABSENT file  -> ok=True,  and the file IS created   (#979)

duckdb.connect(path, read_only=True)      REAL file    -> ok=True
                                          ABSENT file  -> ok=False, and the file is NOT created
duckdb.connect(path, connect_timeout=5)                -> TypeError                            (#978)
```

The control is the point: today's form and the read-only form differ **only** in the flag, and the
ghost file appears under one and not the other. That is what makes "stop creating the file" a
one-line change rather than a redesign.

🚨 **But one requirement in D2 does not fall out of this, and it is the one most likely to be
quietly dropped.** The read-only absent-file failure is:

```
OperationalError: (sqlite3.OperationalError) unable to open database file
```

— which is **character-for-character the message an unwritable or missing parent directory
produces.** So *"no database at that path"* and *"cannot open that path"* **cannot be told apart from
the exception**, and D2 asks for exactly that distinction because they call for different user
actions. The implementer needs an explicit existence/readability check alongside the open; reading
the driver's error text will not do it, and an `except` that maps this one message to *"no database
at that path"* will confidently tell a user with a permissions problem to fix their path.

⚠️ This is also why AC4 says the negative control must fail **for its own reason**. Two failures with
the same message are not two controls.

### 2b. What (C) is, and what it is not

(C) is a **local reproduction of a shape**, not a production exploit — deliberately. It shows that
`_test_file_source` and the dlt lister behind it take no org, consult no ownership, and yield every
file the *process* can read under the path the user typed. `bucket_url` is free text with no scheme
check and no path validation (`connection_state.py:238`, `:794`).

On a **hosted** deployment that composes with two facts already in the source tree: uploaded files
land in a flat, non-per-org layout under a volume mounted into both `app` and `celery`, and
`file_glob` is user-controlled per upload (`dlt_runner.py:1411`). Ownership of an upload is enforced
on the **database row** (`get_org_uploaded_file`) and nowhere on the filesystem.

🚨 **The security write-up is deliberately not in this file.** `datanika-core` is public; the
detailed path, the glob behaviour and the reachability argument are in
`plans/security/LOCAL_PATH_FILE_SOURCE_2026-09-03.md` (private repo) and tracked by the neutrally
titled core issue [core#985] — the [core#748] precedent, for the same reason. **I did not test
this against production and nobody should**: doing so would mean reading another org's data.

What matters *here* is only the product consequence, and it is decided in D4.

---

## 3. The contract

**Test Connection answers exactly one question:** *will the run find your data?* It must answer it
from where the run will happen, in the vocabulary of the thing that was tried — or say that it did
not answer it.

Three verdicts, not two. The shape already exists and is already plumbed to the UI:
`ConnectionService._test_saas_source` returns `bool | None`, and `ConnectionState` carries
`test_success` / `test_untested` / `test_message` (`connection_state.py:1407-1433`). Its docstring
already states the governing rule, and it generalises exactly:

> *Failure and "not tested" are different answers and neither may be rendered as the other: calling
> an unverifiable connection failed is the same class of lie, told in the opposite direction.*

| verdict | means | rendered |
|---|---|---|
| `True` | the run will find your data, checked where the run will read it | green |
| `False` | it will not, and here is which of the reasons | red |
| `None` | not checkable from here — the run is the first real test | **neutral**, never green |

**A local-file check that cannot reach the run's filesystem returns `None`, not `True`.**

---

## 4. Decisions

### D1 · Test Connection is a read, and must be incapable of writing

Open-or-create is disqualifying for a checker. SQLite must be opened read-only
(`sqlite:///file:<path>?mode=ro&uri=true`), DuckDB with `read_only=True`.

This is stated as a **product** rule and not an implementation note, because the general form is what
stops the next connector doing it: **a check that can bring its subject into existence is not a
check.** It is why "the file is there now" cannot be offered as evidence that the path was right.

⚠️ Read-only also changes an honest case: opening a genuinely new, empty local database becomes a
*failure*. That is correct and is D2's second sentence — "no database at that path" is exactly what
the user needs to hear, and creating one for them silently is how the wrong path survives to the run.

### D2 · A file-backed connector must never say "credentials" or "network"

Measured: both duckdb (success path) and sqlite (failure path) return
`Connection failed — check your credentials and network settings` for a local file that has neither.
The user is sent to check two things that do not exist, on the one path we advertise as needing
neither.

Four outcomes, four sentences. They call for four different user actions, which is the test of
whether a message earns its own string:

| outcome | message | what the user does |
|---|---|---|
| database/file found and readable | *"Connected — read `<name>` at `<path>`."* | continue |
| nothing at that path | *"No database at `<path>`. Check the path, or create the file first."* | fix the path |
| path exists, cannot be opened | *"Cannot open `<path>` — check permissions."* | fix permissions |
| cannot check from here | *"Not tested. This path is read by the worker, which does not share this filesystem — the first run is the real test."* | proceed knowingly |

**Assert on the message, not only on the boolean.** The misdirection is half the cost of both bugs,
and a fix that flips the boolean while keeping the sentence has fixed the cheaper half.

🚨 **Rows 2 and 3 are not separable by catching the exception** — see §2a. Both surface as
unable to open database file. They need an explicit existence/readability check.

### D3 · Derive connect-args from the dialect, not from a list of carve-outs

There are already three (`mssql` → `login_timeout`, `oracle` → `tcp_connect_timeout`, `sqlite` →
none) and DuckDB is the fourth case, missing — which is what [core#978] is. `connect_timeout` is a
**network** parameter; it belongs to network databases and to no others.

This is Engineering's design call and [core#978] AC3 already states it. It is repeated here only to
record the product consequence of getting it wrong again: **the next file-backed connector we add
fails on every input, and tells the user to check credentials.**

### D4 · A local filesystem path is a self-hosted feature. On a hosted deployment it is refused at
create time.

This answers [core#979] AC4, and the same answer covers [core#793]'s destination-side question — one
answer, both sides, as that criterion asks.

**The reasoning is not tidiness.** On `app.datanika.io`, a path a user types is not a path on their
machine. It names a location inside **our** container, on infrastructure shared with every other
tenant. The feature as built therefore offers a hosted user something we never meant to offer, and it
offers it through a field with no validation of any kind. See §2b.

**Decision.** A new setting — default **permitted**, so nothing changes for self-hosters, who are the
only people the feature was ever for, and so no existing local deployment breaks:

- `DATANIKA_ALLOW_LOCAL_FILE_PATHS` (bool). **Set to `false` in production `.env.docker`.**
- When false, a `bucket_url`/`path` that resolves to a local filesystem location is refused at
  **connection create/save**, not merely at test time. Refusal at test time is not enough: the run
  reads the stored config, and Test Connection is optional.
- The refusal is a **helpful** message, not a validation error — it must name a route that **works**:
  *"Local file paths aren't available on this deployment. Upload the file instead, or point this
  connection at a database."* The upload route is already the primary one in the form
  (`uploaded_file_id`).

  > 🚨 **CORRECTED 2026-09-03. This bullet used to read *"…available on Datanika Cloud. Upload the
  > file, or point this connection at S3 or a database"*, and both halves were wrong.** Engineering
  > shipped the corrected sentence (`connection_service.py:338`, i18n key
  > `connections.local_path_not_allowed`); this is the spec catching up to it, recorded rather than
  > silently overwritten so it is not "restored".
  >
  > 1. 🚨 **`s3` is WITHDRAWN** — [core#863]. `ConnectionType.S3` survives for existing rows, but it
  >    is not in `PICKER_TYPES`, and `connections.py:101` deliberately renders a
  >    `connections.s3_withdrawn` notice for anyone who *searches* the picker for "s3". So the old
  >    copy refused a user and then pointed them at the one route the very next screen also refuses.
  >    🔑 **A refusal that names routes which work is good copy; naming a route that does not work is
  >    worse than naming none** — the reader has already been told "no" once, and the second no is
  >    the one that makes them leave.
  > 2. ⚠️ **"Datanika Cloud" contradicted this decision's own reasoning** — see the note directly
  >    below on why this is a *setting* and not an edition check. A self-hoster who sets the flag
  >    would have been told their deployment is Datanika Cloud.
  >
  > ⚠️ **Restore the S3 half only in the same change that returns `s3` to `PICKER_TYPES`** — the same
  > coupling `connections.py:104` already states for the withdrawal notice.
- `s3://` and other remote schemes are unaffected **by the local-path test** — it explicitly excludes
  them, because a bucket URL means the same thing in both containers. ⚠️ That is a statement about
  *this check*, not about the connector being available; see the correction above.

⚠️ **Why the setting rather than a hardcoded edition check.** `DATANIKA_EDITION=cloud` gates billing,
and a self-hoster running the cloud plugin is a shape we support. The property that matters is *"is
this deployment multi-tenant?"*, which is a deployment fact, not an edition.

🚨 **Expand/contract note for whoever ships it:** this is a *validation* change with no schema
change, but existing rows may already hold local paths. The refusal must apply to writes only —
never to reads — or the connections page breaks for anyone who has one. List them, do not hide them.

### D5 · Where the check cannot run, say so — do not move the check

The tempting fix for §1's first reason is "dispatch Test Connection to the worker." **Do not do that
as part of this work.** It turns a synchronous button into a queued job with its own failure modes
(worker down, queue backed up), and a Test Connection that hangs is a worse first-run experience than
one that is honest about its limits.

With D4 in place, the hosted case has no local paths left to check, and the self-hosted case is the
one the docs already cover — where the guides ([landing#459], 5 of 5 corrected) tell the reader to
mount the path into **both** containers. There, the web-process check is sound.

So: `None` + the D2 message for the residual case, and the worker-side check is a separate issue if
anyone ever wants it. Recorded so the next reader knows it was considered and declined.

### D6 · The verdict text is not translated, in any locale

Measured: `en.json` holds **`connections.test`** (the button label) and **zero** keys for any verdict
sentence. `test_message` is rendered raw from the service (`pages/connections.py:134`). Every user in
all nine locales reads these in English, including the four new sentences in D2.

This is not a rider — it is the reason to fix the copy and the i18n in one change. The service
currently returns prose; it should return a **key plus interpolation values**, with the UI doing the
lookup through `I18nState`, exactly as `_deleted_toast` does. Otherwise D2 ships nine locales' worth
of new English.

⚠️ [core#872]'s i18n scanner trap applies: keys added before their reader exists read as orphans, and
the documented remedy for a false orphan is to **delete the key** — which silently drops nine
translations with every check green. Land the keys and the reader in the same commit.

### D7 · The docstring that makes the wrong promise

`_test_file_source`'s docstring says the check and the loader *"agree by construction"* because they
use the same lister. That is true of the **glob semantics** and false of the **filesystem**: same
code, different container. For a local `bucket_url` the guarantee does not hold.

Correct it, or make it true. Left as written it is the kind of claim someone reasonably relies on
while deciding not to check something. ([core#979] AC5.)

---

## 5. Acceptance criteria

Each is written as something a user or a test can observe. **Red-first for all of them** — none has
existing coverage.

1. `test_connection` succeeds against a real `.duckdb` file, against `:memory:`, and through the
   `database` config key. All three: the config has two spellings and the guide uses a file.
2. **Shown red first** by restoring `connect_timeout` for duckdb. A test written after the fix has
   not been shown able to fail.
3. Test Connection on a `sqlite` path that does not exist **fails**, and **the file does not exist
   afterwards**. Assert the second — the boolean alone passes on an implementation that still writes.
4. The two controls from §2 still behave: a real sqlite file passes; an unwritable/absent parent
   directory fails **for its own reason**, distinguishable from "nothing at that path".
5. No message returned for `sqlite`, `duckdb`, `csv`, `json` or `parquet` contains the words
   *credential*, *credentials* or *network*. Asserted over the message set, not per call site.
6. With `DATANIKA_ALLOW_LOCAL_FILE_PATHS=false`, saving a `csv`/`json`/`parquet`/`sqlite`/`duckdb`
   connection whose path is local is **refused at save**, with the D4 message. With it `true`
   (the default), it is saved. Both directions, or the guard proves nothing.
7. An existing connection holding a local path still **loads and lists** with the setting false. The
   refusal is on writes only.
8. Every new sentence has a key in **all nine** locale files, and `test_no_orphan_keys_in_json`
   passes — keys and their reader in the same commit.
9. A `None` verdict renders **neutral, not green**. The existing `test_untested` var already
   supports this; assert the rendering, because [core#821]'s whole finding was that the verdict is a
   **colour** and colour is what the user reads.

---

## 6. Ship order

1. **D3 + D1 + D2 + D6** — the connector fixes, the read-only opens, the four messages and their
   27 locale entries. This is [core#978] and [core#979]'s code half and unblocks [core#793]'s
   `04-first-run` recapture.
2. **D7** — one docstring, in the file D1 already touches.
3. **D4** — the hosted refusal. Separable, and it is the one with a production `.env.docker` change
   behind it, so it wants Infra in the loop.

**Blocked on nothing.** No migration, no new credential, no vendor spend.

⚠️ **Do not fold [core#821] into this.** That is the SaaS types going green without a request, it is
already open with its own contract, and this spec borrows its three-verdict shape rather than
extending its scope.

[core#748]: https://github.com/datanika-io/datanika-core/issues/748
[core#793]: https://github.com/datanika-io/datanika-core/issues/793
[core#821]: https://github.com/datanika-io/datanika-core/issues/821
[core#872]: https://github.com/datanika-io/datanika-core/issues/872
[core#985]: https://github.com/datanika-io/datanika-core/issues/985
[core#978]: https://github.com/datanika-io/datanika-core/issues/978
[core#979]: https://github.com/datanika-io/datanika-core/issues/979
[core#863]: https://github.com/datanika-io/datanika-core/issues/863
[landing#459]: https://github.com/datanika-io/datanika-landing/issues/459
