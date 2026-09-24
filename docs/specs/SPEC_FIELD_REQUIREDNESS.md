# SPEC — How a form says a field is required

**Status:** contract, ready to build
**Owner:** Product (the convention). Engineering owns the implementation.
**Unblocks:** [core#1311], which is explicitly *"Blocked by: Product's convention decision."*
**Context:** [landing#572] item 2 is the confirmed instance; it is not the only one.
**Amended:** 2026-09-15 — §2.7 added, AC2 and AC6 corrected, §4 records the first per-connector
difference. This is the ruling on the question the first slice put to Product on [core#1311].
**Amended again:** 2026-09-24 — §2.7's enforcement premise struck as **measured false**, §2.8 added
(the gate is the source of truth), AC4 settled for its two *unverified* cases, §4 records the second
per-connector ruling. **The 2026-09-15 ruling is unchanged; only the reason it rests on is.**

[core#1311]: https://github.com/datanika-io/datanika-core/issues/1311
[landing#572]: https://github.com/datanika-io/datanika-landing/issues/572

---

## 1. What is actually wrong, and it is bigger than the twelve keys

[core#1311] found that 12 `en.json` labels end in ` *` and all 9 locales agree — **108 strings**.
Three further measurements, taken against `origin/dev` before writing this:

**1a. We ship two opposite conventions at once.** Twelve labels mark *required* with ` *`. Four
labels mark *optional* in the other direction — `connections.endpoint_url`,
`connections.api_key_optional`, `connections.service_account_json_optional`,
`uploads.schema_contract`. Everything else is unmarked.

> 🚨 **With both conventions live, an unmarked field is genuinely ambiguous.** It could be optional
> (and correctly unmarked under the required-marking convention) or required (and missing its `*`).
> A reader cannot tell, and neither can a reviewer. **This is the defect that survives fixing all
> 108 strings**, and no amount of correcting asterisks addresses it.

**1b. The marker carries no translatable content.** `connections.base_url` is `"Base URL *"`,
`"Базовый URL *"`, `"基础 URL *"`, `"الرابط الأساسي *"` — the glyph is byte-identical in all nine.
So 108 strings encode 12 pieces of translatable information plus one character repeated 108 times.
A translator was asked nine times to place a character that has no linguistic content, including once
across a bidi boundary in Arabic, where a trailing neutral character is exactly where rendering bugs
live.

**1c. 🚨 The two requiredness signals we ship are independently authored, and they already
contradict — in the accessibility layer, not only in the prose.** `config_input` forwards `**props`
to `rx.input`, so `required=True` becomes the HTML `required` attribute and assistive technology
announces it. `openapi_fields()` renders the label `"Base URL *"` and passes **no** `required`. So
that field tells a sighted user it is mandatory and tells a screen reader it is not.

**That is the root defect.** The label and the attribute are two hand-maintained copies of one fact.
[landing#572] item 2 is what it looks like when they disagree.

## 2. The decision

**2.1 · Requiredness is DERIVED, never authored.** One value per field decides both the HTML
`required` attribute and the visual marker. A call site states requiredness once. It is never
possible for the two to disagree, because there is only one of them.

**2.2 · Translated strings carry no marker, in any locale.** All twelve ` *` suffixes come out, and
so do the four `(optional)` label suffixes. A label is a name, not a sentence about the form.
⚠️ **Placeholders are out of scope** — `uploads.ph_initial_value`, `ph_row_order`, `ph_table_names`
and `model_detail.ph_alias` also say "(optional)", and a placeholder is allowed to be a hint. Do not
sweep them in by pattern-matching on the word.

**2.3 · Mark REQUIRED. Do not mark optional.** One convention, not two. **The invariant that makes
an unmarked field readable: every required field carries the marker.** Under it, unmarked means
optional, with no ambiguity. Marking both is redundant when the invariant holds and incoherent when
it does not — which is today's state.

**2.4 · The marker is `*`, as a separate element adjacent to the label — never concatenated into
it.** Chosen over a translated word ("required"):

- it needs **zero** new translated strings, against nine for a word;
- it is what the product already shows, so no user relearns anything;
- as a sibling element it inherits the row's direction, rather than depending on bidi resolution of a
  neutral character appended to an RTL string.

The codebase already contains this pattern: `openapi_fields()` renders `openapi_spec` as
`rx.text(_t["connections.openapi_spec"], " *", …)` — marker at the call site — two lines above the
`base_url` line that is the bug. **The fix is the pattern already in the file.**

**2.5 · The glyph is never the only signal.** The same derived value sets HTML `required`. A marker
without the attribute is a sighted-user-only control, which is how 1c happened.

**2.6 · A form that shows markers explains them once.** One legend — *"\* Required field"* — one new
i18n key, nine strings. That replaces 108 and is the only translation work this creates.

**2.7 · 🆕 The marker describes what THIS FORM requires of the person filling it in** *(added
2026-09-15)* — not what the stored configuration, the API schema or any other surface requires.

A field is required on a form when that form refuses to save while the field is blank. That is the
only fact the marker and the HTML `required` attribute may express, because it is the only one the
person looking at the field can act on.

> 🔴 **CORRECTED 2026-09-24. The next sentence used to read: *"And the attribute **enforces** it: a
> browser will not submit a form while a `required` input is empty. So marking a field the form fills
> by itself does not merely mislabel it — it removes the path that fills it."* **That is false on this
> form, and the error is mine.** Measured by Engineering ([core#1311]) and re-derived here against
> `origin/dev` with controls in both directions:
>
> | reading | result |
> |---|---|
> | `grep -c 'rx\.form('` in `ui/pages/connections.py` | **0** |
> | `grep -c 'on_submit'` in `ui/pages/connections.py` | **0** |
> | pages that *do* carry `rx.form(` — the positive control | **5**: `login.py`, `signup.py`, `forgot_password.py`, `reset_password.py`, `settings.py` (×2) |
> | the rendered tree | carries `cfg-port` and `required`, and **no `onSubmit`** |
> | how the save fires | `on_click`, not a form submission |
>
> **The connection form has no `<form>` element, so the browser never runs constraint validation and
> the HTML `required` attribute blocks nothing.**
>
> 🔑 **The consequence, stated plainly because it is the part that must not be re-derived:
> `required=True` is NOT a safety property on this form.** It is an *announcement* to assistive
> technology and nothing else. The single gate is `_validate_connection_form`
> (`datanika/ui/state/connection_state.py:279-353`).
>
> ⚠️ **The failure mode this sentence predicted cannot happen, and the real one points the other
> way.** The old text feared that marking a self-filling field would *block* the path that fills it.
> Nothing blocks anything. What actually happens is the inverse: a field can carry `required=True`,
> carry **no** visible marker, and be **saved blank** by a form that neither requires it nor fills it
> — which is §2.8, and it is live on two connectors today.
>
> ✅ **Why the 2026-09-15 ruling survives unchanged.** The ruling is about *which surface the marker
> describes*. That rests on what the person looking at the field can act on, never on the browser
> enforcing anything. **Removing the enforcement premise makes the rule matter more, not less:** with
> no enforcement, the marker and the attribute are the only things that tell a user a field is
> needed, and neither of them is the thing that decides.

`connection_schemas.py` answers a different question: what a stored configuration, or a request to
the API, must contain. The two legitimately differ when **the form supplies the value itself**. The
first case, measured on `origin/dev` on 2026-09-15:

| | form, field left blank | stored config and API schema |
|---|---|---|
| `rest_api` Base URL | refused — there is nothing to derive it from | required |
| `openapi` Base URL | **saves** — filled from the spec's `servers` entry | required |

So `openapi`'s Base URL is **unmarked and carries no `required` attribute**, and that is correct.

**A field the form fills is honestly unmarked only if the form refuses what it cannot fill.** When the
fallback yields no usable value, the save must fail with a message that names the field — for
`openapi`, *"No base URL found in the spec — set the Base URL field"*. An unmarked field whose
fallback can silently yield an unusable value is a defect of the fallback, not of the marker, and it
is fixed in the fallback.

**2.8 · 🆕 The gate is the source of truth, and there are THREE copies of the fact, not two**
*(added 2026-09-24)*.

§1c described two independently-authored signals — the label's marker and the HTML `required`
attribute — and called their disagreement the root defect. **With §2.7's enforcement premise gone,
there are three**, and the third is the only one with any effect:

| # | signal | where it is authored | what it does |
|---|---|---|---|
| 1 | the visible marker | the call site's label | tells a sighted user |
| 2 | the HTML `required` attribute | the call site's `config_input` | tells assistive technology **only** |
| 3 | **`_validate_connection_form`** | `ui/state/connection_state.py:279-353` | **decides whether the save happens** |

**So §2.1's "one value per field" must be derived from 3.** A call site that states requiredness
while the gate says otherwise has not stated one fact twice — it has stated a *different* fact, and
the user experiences the gate. `connection_schemas.py` remains a fourth surface answering §2.7's
different question, and is not a candidate.

🚨 **Measured on `origin/dev` 2026-09-24, mechanically, and the population is 20 of 37 connection
types — not the two this section was first drafted around.** `_validate_connection_form` is a flat
`if/elif` chain over `conn_type` (`connection_state.py:279-353`) with **no `else`**; its last
statement is `return ""`, i.e. *valid*. 17 types have a branch. **The other 20 have none, so the form
saves them with every type-specific field blank:**

> `duckdb` · `databricks` · `stripe` · `github` · `hubspot` · `salesforce` · `shopify` · `jira` ·
> `slack` · `google_analytics` · `google_ads` · `facebook_ads` · `zendesk` · `airtable` · `notion` ·
> `pipedrive` · `freshdesk` · `asana` · `kafka` · `openapi`

🔑 **Structurally, not just by omission: the validator is never given their values.** `_validate_form`
(`connection_state.py:865-884`) passes **13** named kwargs — `host`, `port`, `database`, `path`,
`project`, `dataset`, `account`, `user`, `bucket_url`, `base_url`, `uploaded_file_id`,
`spreadsheet_url`, `service_account_json`. `api_key`, `token`, `bootstrap_servers`, `topics`,
`owner`, `repo`, `http_path`, `catalog` and the rest **are not parameters of the gate at all.** So
adding a branch is not a one-line change: the field has to be handed to the validator first. And it
is the *only* gate — `save_connection` calls `_validate_form()` at `:1501` and returns on its message
(`:1759` is the Test-Connection path), with nothing else between the form and `ConnectionService`.

**And most of those 20 DO show the user a marker.** Counted per renderer:

| connector | `required=True` attrs | ` *` markers | gate |
|---|---|---|---|
| `google_ads` | 5 | 5 | **none** |
| `github` | 3 | 3 | **none** |
| `airtable`, `facebook_ads`, `freshdesk`, `jira`, `kafka`, `salesforce`, `shopify`, `zendesk` | 2 each | 2 each | **none** |
| `google_analytics`, `stripe` | 1 | 1 | **none** |
| **`databricks`** | 3 (+Catalog with none) | **0** | **none** |
| **`duckdb`** | 1 | **0** | **none** |

🚨 **So the general case is: the form prints `*`, announces `required`, and saves the field empty.**
That is the defect, and it is not a labelling defect — under §2.3's invariant (*every required field
carries the marker*) these markers are **telling the truth about the product's intent and lying about
the product's behaviour.** The marker is correct; **the gate is missing.**

⚠️ **`databricks` and `duckdb` are the sub-case where even the marker is absent**, which is §1c
inverted: a field that looks *optional* to a sighted user, is announced *required* to a screen
reader, and saves empty. Their field sites are `connection_config_fields.py:430-439` (duckdb Path)
and `:508-545` (databricks Host, HTTP Path, Token, Catalog — Catalog carrying no attribute either,
while `connection_schemas.py:162-170` requires it).

⚠️ **Unlike `openapi`'s Base URL there is no fallback filling any of these.** `connection_service.py:1104`
builds `databricks://token:{token}@{host}` and `:1119` builds `duckdb:///{path}`; from blank values
those are malformed, so the failure moves from save time to connect time, where the user has lost the
field that caused it. `_build_config` (`:886-`) omits a blank field from the config dict entirely
rather than storing an empty string, so the stored config is *missing the key*, not carrying a blank.

⚠️ **`openapi` needs its own reading and is NOT settled here.** §2.7's 2026-09-15 measurement — Base
URL saves blank because the form fills it from the spec's `servers` entry — was taken on the save
path. What is newly measured is that the **gate** has no `openapi` branch either, so *nothing refuses*
even when the fill has nothing to work from. §2.7's own condition (*"the save must fail with a message
that names the field"* when the fallback yields no usable value) is therefore **unmet**, and that is a
defect of the fallback exactly as §2.7 says. It is the one member of the 20 whose correct end state
might still be an unmarked field.

⚠️ **Not measured, stated so it is not inferred:** whether `duckdb:///` resolves to an in-memory
database or errors. It is not load-bearing — an in-memory DuckDB is not a connection worth saving
either (cf. [core#793]) — but the claim above is about the three signals disagreeing, not about what
the driver does with the empty string.

🔑 **The mechanism that fixes it already exists and these six call sites do not use it.**
`secure_input.py:166-183` defines `field_label(label, *, required, field)`, which renders the marker
**exactly when** `required` is true and binds `html_for` to the same id `config_input` builds. That
is §2.1. `databricks_fields()` and `duckdb_fields()` still hand-write `rx.el.label(rx.text(...))`
beside a separate `config_input(required=...)` — two copies, which is the thing this spec exists to
remove. **Porting a call site to `field_label` is what "derived" means in AC1.**

## 3. Acceptance criteria

**AC1 — One source of truth.** A field's requiredness is stated once at its call site. Assert that no
`en.json` value ends in ` *` and no **label** key ends in `(optional)`; state the placeholder
carve-out in the test so a later reader does not "complete" it.

**AC2 — The marker follows the form, and every difference from the schema is recorded.** 🔴
*Corrected 2026-09-15: this read "the marker follows the schema", which §2.7 shows is the wrong
surface.* The test derives requiredness on both sides — from the value that sets a field's marker and
attribute (§2.1), and from `connection_schemas.py` — and compares them. **Every difference is an
explicit entry carrying its reason**, and the test fails both when a new, unrecorded difference
appears and when a recorded one disappears while its entry remains. ⚠️ **Never assert that a string
contains or omits an asterisk**: absence-of-a-character is satisfied by deleting the label
(`WORKFLOW_RULES` §4's standing trap).

**AC3 — Both directions are exercised.** [core#1311]'s suggested first slice is
`connections.base_url` + `connections.name`, and the reason is load-bearing: **`name` is single-site
and always required; `base_url` is multi-site and required, on the form, for only one of its two
connectors** (§2.7). So the pair exercises a derived marker in both directions — **a mechanism that
only ever renders `*` would pass a `name`-only test.** Any slice chosen instead must have that
property.

**AC4 — The five shared keys are checked against every site that renders them.** `connections.database`
(3 sites), `connections.host` (3), `connections.base_url` (2), `connections.db_path` (2),
`connections.port` (2). ⚠️ **The other seven are single-site: their markers are *unverified*, not
known-wrong.** Do not describe fixing them as correcting an error.

> ✅ **SETTLED 2026-09-24 — the two sites this criterion left *unverified* are now measured, and both
> are known-wrong.** They were `connections.host` at its `databricks` site and `connections.db_path`
> at its `duckdb` site. **Both save blank** (§2.8). They are no longer "unverified"; fixing them *is*
> correcting an error, and AC6's "derived, not deleted" applies — the correct end state is the marker
> **present** and the gate refusing, not the attribute removed.
>
> 🔑 **And the method generalises, which is worth more than the two answers.** `_validate_connection_form`
> is a **pure function of `(name, conn_type, use_raw_json, **fields)` with no I/O** — so *"does this
> form refuse while this field is blank?"* is answered by **reading `connection_state.py:279-353`**,
> for every field and every connector, in one pass. **No browser walk is needed to discharge AC4, and
> none should be spent on it.** That was not knowable while §2.7 claimed the browser enforced
> anything: under the old premise the attribute was part of the answer, and the attribute's effect can
> only be seen by driving the page. **Striking a false premise turned a twelve-walk question into a
> file read.**
>
> ⚠️ **Read the chain's SHAPE, not just its branches.** It is `if / elif …` with **no `else`**, so an
> unlisted `conn_type` is silently valid. Absence of a branch is the finding; a grep for a connector
> name returns 0 and looks exactly like a grep aimed at the wrong file. Enumerate the branches and
> diff against `ConnectionType` (`models/connection.py:11-48`, **37** members) rather than searching
> for the connector you happen to be asking about.
>
> 🔴 **Doing exactly that is what turned this from a two-field finding into a 20-type one** (§2.8).
> I had hand-read the chain and written *"databricks and duckdb"* into this spec; the set difference
> returned **20**, and my own first draft of §2.8 used `stripe` as a **control** when `stripe` is an
> instance. **My reading of a list is not an enumeration of it** — and the error was in the
> flattering direction, because a two-field defect needs no scoping conversation and a 20-type one
> does.

**AC5 — The accessibility contradiction is closed, and shown closed.** For at least one field where
the label and the attribute disagree today (`openapi_fields()`'s `base_url` is the known case),
assert that the rendered attribute and the rendered marker agree. This is the assertion that would
have caught [landing#572] item 2.

**AC6 — The `*` is not de-asterisked into a second defect.** A find-and-replace across 108 strings
looks like the whole job and ships a contradiction pointing the other way. The marker is **derived,
not deleted**: every field the form genuinely requires keeps it — in the first slice, Connection Name
and `rest_api`'s Base URL.

> 🔴 **Corrected 2026-09-15.** This criterion used to say *"the `*` is CORRECT for `openapi`, where
> `base_url` genuinely is required by the API schema."* The example was wrong, and it contradicted AC3
> in the same list. The API schema does require `base_url` for `openapi`; the **form** does not — a
> blank value saves and is filled from the spec's `servers` entry. The sentence took a fact about one
> surface as a fact about another, which §2.7 now rules out. The first slice ([core#1311]) followed
> the form, as AC3 does, and that is the ruling. The API schema is unchanged by it.

## 4. Out of scope

- Changing which fields *are* required. This spec governs how requiredness is communicated, not what
  it is. If AC4 finds a shared key whose two sites genuinely differ, that is a product question per
  connector and it comes back to Product. **First instance, ruled 2026-09-15:** `connections.base_url`
  differs between `rest_api` (required) and `openapi` (optional, filled from the spec), and the
  difference is correct — see §2.7. Whether the **API** should also fill `openapi`'s `base_url` from
  the spec is a separate question, and this ruling does not take it.

  🆕 **Second instance, ruled 2026-09-24 — and this one goes the other way, so the pair is the
  precedent rather than either alone.** **Ruling: where a field carries a marker or the attribute and
  nothing fills it, the form must refuse. `_validate_connection_form` gains a branch for each of the
  20 un-gated types (§2.8), and the fields are handed to it as parameters — they are not parameters
  today.** For `databricks` (Host, HTTP Path, Token, **Catalog**) and `duckdb` (Database Path) the
  marker is added too, via `field_label`; for the other 18 the marker is already right and only the
  gate is missing.

  **Scope note, because 20 types is not one change.** The per-connector question §4 reserves for
  Product is *"which fields does this connector genuinely require?"*, and for 18 of the 20 it is
  **already answered by the call site** — someone wrote `required=True` and a `*` deliberately. Those
  need no new product decision, only a gate that matches. **The two that need a decision are the two
  with no marker** (`databricks`, `duckdb`), and both are ruled above. **`openapi` is the one
  genuinely open case** (§2.8's last note): its Base URL may stay unmarked, but the save must refuse
  when the spec yields no usable value.

  **The test that separates this from the `openapi` ruling, and it is the only one that matters:
  does the form FILL the field when it is blank?** `openapi` does — from the spec's `servers` entry —
  so leaving it unmarked is honest. **Nothing fills these five.** §2.7's own sentence already decides
  it: *"A field the form fills is honestly unmarked only if the form refuses what it cannot fill."*
  Here the form neither fills nor refuses, which is the one combination that sentence forbids.

  ⚠️ **Do not discharge this by deleting `required=True` from the four attributes that carry it.**
  That reads like "making the signals agree" and it agrees them on the wrong value — it would ship a
  connector whose every field is optional and whose every connection fails at connect time. AC6,
  pointed at a new instance.

  ⚠️ **`databricks` Catalog is the one to check last and it is the easiest to miss:** it is the only
  one of the five with **no** `required` attribute today, so a sweep that starts from "fields carrying
  `required=True`" will not see it. It is schema-required all the same
  (`connection_schemas.py:162-170`). *An instrument built from the wrong population reports the rest
  as clean.*
- Error-message copy for a missing required field.
- The `uploads` and `model_detail` placeholder hints (§2.2).
