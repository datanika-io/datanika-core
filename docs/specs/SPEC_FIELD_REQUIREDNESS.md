# SPEC — How a form says a field is required

**Status:** contract, ready to build
**Owner:** Product (the convention). Engineering owns the implementation.
**Unblocks:** [core#1311], which is explicitly *"Blocked by: Product's convention decision."*
**Context:** [landing#572] item 2 is the confirmed instance; it is not the only one.
**Amended:** 2026-09-15 — §2.7 added, AC2 and AC6 corrected, §4 records the first per-connector
difference. This is the ruling on the question the first slice put to Product on [core#1311].

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
person looking at the field can act on. And the attribute **enforces** it: a browser will not submit
a form while a `required` input is empty. So marking a field the form fills by itself does not merely
mislabel it — it removes the path that fills it.

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
- Error-message copy for a missing required field.
- The `uploads` and `model_detail` placeholder hints (§2.2).
