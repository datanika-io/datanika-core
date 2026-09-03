# Product rules

Rules Product paid for. Each one is here because it was learned from something that went wrong, or
that nearly did, and because the next person will not re-derive it — they will repeat it.

This file is in the repository on purpose. These used to live in `plans/product/current_state.md`,
which is **rewritten from scratch every session** by design, so a rule kept there was one rewrite
away from gone. Process rules that bind every department are in `plans/WORKFLOW_RULES.md`; this file
holds the ones specific to product work — driving the live app, judging evidence, and writing specs
other people are held to.

---

## 1. Ask what your evidence records — not what it shows

The single most expensive class of Product mistake is not a wrong reading. It is a **correct reading
of the wrong thing**.

- **A screen that shows no history is not a record of your action.** If a UI has no audit view, its
  looking unchanged tells you nothing about whether your change applied. Your written log outranks
  the screen. Do not redo work because a page looks the same.
- **A green run row is not data movement.** The standing acceptance criterion for every connector is
  **rows of real data in the destination**, verified in the destination — never a `success` status.
  This is not pedantry: [#492](https://github.com/datanika-io/datanika-core/issues/492) shipped file
  connectors that returned one row of `file_name`/`size_in_bytes` per file and reported `success`,
  and [#493](https://github.com/datanika-io/datanika-core/issues/493) made a zero-match glob complete
  as `success` too, so a wrong path and a right path were indistinguishable.
- **A filter can hide a passing result as easily as a failing one.** A prod verification once
  reported "no verdict" because the reader had a 220-character cap and the message was 236
  characters. Bound your filters deliberately, and when a result is *absent*, suspect the instrument
  before the system.
- **`count(col)` on a `json` column counts the literal `null`.** It answers *"is the column set?"*,
  not *"does the payload contain anything?"* — which needs `json_typeof(col) = 'object'`. A census
  reading "116 of 116 populated" was a fact about nullness; the real answer was 30 rows with an
  object and **0** with an email. Mirror-image of the same error: `audit_logs.ip_address` was called
  PII because of its *name* while never having been written at all.
- **A guard that "finds zero" may be finding zero because the feature does not exist yet.** *"No
  payload contains a PII key"* is true today against no redactor whatsoever. A guard must
  **construct** its input and be **shown red against a stub**; a sweep must be proved against a
  **planted** row. A test that has never failed has never been shown to be able to.

- **A schema entry is not a form field.** `connection_schemas.py` gained `auth_source` for `mongodb`
  in [#550](https://github.com/datanika-io/datanika-core/issues/550); the **rendered form never did**.
  `mongodb` is special-cased to `mongodb_fields()` in `connection_config_fields.py`, which hard-codes
  five inputs and in which `auth_source` appears **zero** times — there is no renderer for the schema
  entry and no `ConnectionState` var for an input to bind to. A landing issue was filed on the schema
  reading, asked for a recapture of a *"seven-field form"*, and would have re-stamped `verified_date`
  to assert a UI change that never shipped. Driving the live form settled it in one call: **six**
  inputs, and `auth_source` absent from `innerHTML` and `innerText` alike. The cited evidence was
  real — it just recorded something other than what was claimed. **When the claim is about what a
  user sees, the renderer outranks the schema, and production outranks the renderer.**

- **An exit code records the wrapper's fate, not the suite's verdict.** In one session **four**
  background runs reported **`exit code 0`** while their own final line read `1 failed, 4697 passed`.
  That `1` was not incidental — it was a *second, older copy* of the destination contract asserting
  `infer_direction("mysql") == BOTH`, green for the life of the project while asserting the exact bug
  [#862](https://github.com/datanika-io/datanika-core/issues/862) removes. The mechanism is mundane
  and will recur: a wrapper ending `... | tail -6` reports **`tail`'s** status, and `set -uo pipefail`
  *without* `-e` does not propagate the failure either. **Read the summary line; never the code.** A
  run is green when you have seen the words `0 failed` — better still, when the count matches one you
  predicted before running it.
- **Targeted tests cannot find a contract you did not know was duplicated.** Every targeted run for
  #862 was green; only the full suite found the second copy, because the whole defect was *a file
  nobody thought to look in*. Before merging a change to a shared contract, grep for every assertion
  site (`grep -rn <symbol> tests/`) **and** run the full suite once. The targeted run tells you your
  change works; it cannot tell you what else believed the old contract.
- **A negative probe proves only what it probed.** `hasattr(dlt.destinations, "mysql") is False` does
  **not** mean dlt cannot write to MySQL — it means there is no destination *of that name*. dlt writes
  to MySQL through its `sqlalchemy` destination; our `build_destination` resolves by name and so never
  asks. #862's narrowing is therefore a statement about **our resolution strategy**, not about dlt's
  limits, and it is one careless step from being read backwards by whoever next tries to "restore"
  MySQL. When a probe comes back negative, write down precisely what it probed.

## 2. Count the instruction, not the phrase

A document corrected to *deny* an old behaviour still *contains* the old words. The connector guides
fixed to say *"There is no 'Configure pipeline' button"* still match `grep -l "Configure pipeline"` —
which returned **32** while the true remaining count was **31**, found by grepping the instruction
(`click **Run now**`) instead.

Grep for what a reader is told to *do*. Otherwise you will keep re-finding work that is already done,
and — worse — you will believe a corrected file is a broken one.

## 3. Driving the live product

Product is the department most likely to click through production, and every trap here is
destructive. The long-form incident is `plans/WORKFLOW_RULES.md` §7b; these are the rules.

1. **Never aim a destructive control by ordinal.** A page-wide `.last()` on a Delete/Confirm button
   resolves to the last **table row** when no dialog is open. That is how production connections
   13–17 were deleted in one session, including a demo set another capture depended on. `.last()` is
   defensible for reading; never for deleting.
2. **Scope every confirmation to the dialog and assert the dialog is open first.**
   `getByRole('dialog').getByRole('button', {name: …})`. The failure above was not a wrong button —
   it was clicking when there was nothing to click.
   🚨 **But check that the dialog exists on that page, because on most of them it does not.**
   Measured 2026-08-31: **twelve** destructive one-click controls in `datanika/ui/pages/`. core#804
   added a real `role="alertdialog"` to **two** — `/connections` and `/uploads` — and it reaches
   production only on the next promotion. The other ten (`/schedules`, `/pipelines`,
   `/transformations`, `/settings` ×3, `/api-keys`, `/dag`, `/models`) still mutate on the first
   click: **core#851**. An agent that follows this rule literally where no dialog exists concludes
   the click failed and clicks again — which on a delete page is a second deletion. **This rule and
   rule 1 are not substitutes**: aiming by row content makes a mis-click impossible, a dialog makes
   it recoverable, and 2026-07-22 needed both.
3. **Target by id, inside the card, with the subject asserted.** Chrome's password manager renders
   `#deleteButton` (one credential) and `#deleteAllButton` (**everything on the device**) on the same
   page, with near-identical labels in the founder's locale. The single-credential delete needs no
   confirmation dialog; the catastrophic one does — so "a dialog appeared" is not a safety signal.
4. **Repair with a targeted DB update, not by re-running a UI loop.** Recovery from the incident above
   was one `UPDATE connections SET deleted_at = NULL`. Soft delete is what saved it, not care — and
   uploads, schedules and API keys do not all behave that way.
5. **Do not submit forms you are only photographing.** An Active schedule fires nightly runs into live
   alerting.

### 3a. The app's own quirks, measured on production

🆕 **Moved here 2026-09-03 from `plans/product/current_state.md`, where it had survived several
sessions by luck.** That file is *rewritten from scratch* every session by standing rule, `plans/` is
private, and these are facts about the product — so a handoff file was the one place they could not
safely live. Same placement rule that put the rest of this document here.

- **The production session expires ~5 minutes after login** (`ACCESS_TOKEN_TTL_MINUTES = 10` with
  revalidation). Plan a capture run around it; do not read a mid-run redirect to `/login` as a bug.
- **Connection-form state bleeds across a connector-type change.** Switch the type and fields from
  the previous type can persist. Re-read what is actually in the inputs before trusting a form.
- **The Connection type dropdown's options are plain `<p>` elements**, not `<option>`s — so
  `selectOption` does not work and role-based selection finds nothing.
- **The endpoint picker ships with every box ticked.** A SaaS connection created without touching it
  selects everything, which is not what a screenshot should imply and not what a first run should do.
- **BigQuery stores the service account under `keyfile_json`**, not under any of the names the form
  labels suggest.
- **`browser_snapshot` prints input values**, so it is not a safe way to inspect a form holding a
  credential — see §4. **The OS clipboard is the working channel** for filling one without printing
  it; `browser_run_code_unsafe` is a bare JS VM with no `require`, `import`, `process` or `fetch`,
  so it cannot read a file.
- **When two orgs share a destination database, the upload name is a shared resource.** Two tenants
  can collide on it, and nothing in the UI says so.

## 4. The capture gate: refuse to shoot when a credential field is non-empty

**A screenshot capture step must read every input's `.value` and abort if any credential field is
non-empty.** Not "check the shot before committing" — that check is impossible to perform.

A `type="password"` field renders as dots, and `innerText` **cannot see input values at all**. So a
screenshot containing a live credential looks exactly like one that does not.

This is not hypothetical. A Google Ads connector capture came back holding the signed-in account's
email and password, typed by nobody: Chrome autofilled them into `Customer ID` and `Developer token`
because no input in the connection form set `autocomplete`, `name` or `id`
([#618](https://github.com/datanika-io/datanika-core/issues/618)). The file was destroyed rather than
published. Copy the gate into every capture pass — the forms with a password field are still the
common case.

Related, and the reason the gate is permanent rather than a fix that expired: **#618's repair is the
opposite of [#672](https://github.com/datanika-io/datanika-core/issues/672)'s.** Connection-credential
fields must *suppress* password managers; `/login` and `/signup` must *invite* them. Reusing
`no_autofill_attrs()` on an auth form is a usability regression wearing a security label.

## 5. `innerText` is blind in three specific ways

Each of these has produced a confident wrong answer:

- **It cannot see input values.** A form full of autofilled credentials reads as an empty page. Read
  `.value` per input.
- **It cannot see shadow DOM.** `chrome://` pages nest their content in shadow roots, so
  `document.body.innerText` returns `""` and reads exactly like a blank page. Walk the shadow roots.
- **It cannot see an attribute.** *"Every field is empty"* and *"every field carries the right
  `autocomplete` token"* are different assertions, and only the second one discriminates. After
  removing a saved Chrome credential, the empty-value column stopped being evidence at all — an
  unfixed build would have read identically. **The attributes are the verification.**

## 6. The browser lies about deploys, and the tell is precision

The persisted Playwright profile is **shared by every department and survives deploys**, so a
post-deploy check can measure pre-deploy JavaScript. A fix verified the morning after promotion
measured the exact pre-fix geometry — `flex: 0 0 auto`, right edge **973**, `scrollWidth` **600** — and
was one step from being filed as a regression against a fix that had shipped correctly.

**Reproducing the "before" numbers to the pixel is the signature of stale code, not of a failed fix.**
A genuine regression almost never lands on the identical values.

- Bust the cache with a **query parameter plus a reload**. A plain `page.reload()` is not enough.
- 🚨 **Never `context.clearCookies()`.** The context is the shared profile, not your session. Calling
  it once wiped every cookie in it, taking the founder's Google / Search Console / Plausible logins
  with it while another department was working. Ask for a separate context if you need isolation.
- Establish what the server is serving before concluding anything about the client. The deployed SHA
  and the active colour are cheap; a false bug report is not.

## 7. Specs are contracts, and a bullet is not a spec

- **Write `SPEC_<topic>.md` and commit it before Engineering starts.** The spec is what they are held
  to; acceptance criteria are Product's to write, in terms of what the user can observe, not how it is
  built.
- **Design text does not belong in a plan bullet.** A bullet outlives the task, and its text outlives
  its truth. One bullet absorbed a spec's worth of consent-screen design during an edit, and a routing
  pass later read it and dispatched a department to build a page that had been live in production for
  six weeks. The durable statement belongs in the spec; the tracker holds a pointer.
- **Scope `closes #N` to what the PR actually does.** A PR fixing 4 of 36 connector guides carried
  `closes` [datanika-landing#272](https://github.com/datanika-io/datanika-landing/issues/272); GitHub
  closed it, and the remaining 31 stopped existing as tracked work. Use `refs #N` and close by hand
  when the work is genuinely finished. *(Note the cross-repo hazard in that sentence: a bare `#272`
  written in this repository auto-links to a completely unrelated core issue. Always qualify an issue
  number that belongs to another repo.)*
- **When you decline an acceptance criterion, say so on the record.** #682's *"no separator character
  remains in Python source"* over-reached — the survivors are list separators between standalone
  links, not connectives inside a sentence — and the implementer was right to refuse it. Agreeing in
  writing is what stops the next session re-filing it.

## 8. Verify against `origin/dev`, and verify the freshness of `origin/dev`

`origin/dev` moved **three times** during a single Product session, and an item routed as unfixed had
already shipped. Read with `git cat-file -p origin/dev:<path>` — no checkout, so it cannot touch the
tree — and `git fetch origin --prune` first, because `origin/dev` is a *local* ref and a stale one
reads exactly like a current one.

Two corollaries:

- **A relayed "X appears nowhere" is a claim to verify, not a finding.** A rate-limit number said to
  be in no spec turned out to be three filed decisions in `SPEC_PRICING_V2`, an open founder decision,
  and **already published** on two public pages. The check took one grep. *(This rule caught itself
  during the writing of this file: two specs were about to be documented as deleted, and both were in
  `datanika-cloud` — a repo the search had not covered.)*
- **A rebase-merge gives your commit a different SHA**, so `git branch -r --contains <local sha>`
  returns empty and reads exactly like "not merged". **Verify by content**, not by SHA.

## 9. What the docs owe a shipped feature

- Every user-facing feature needs a page under `datanika-landing/src/pages/docs/` **and** a cross-link
  from the UI. Prefer consolidating related docs onto one page over scattering.
- **`verified_by: product-ui` means somebody drove the form.** It is not a synonym for "checked
  against the schema" — that is `source-code`. Moving the stamp without driving the UI is the
  "verified by" fiction, and no test will stop you: nothing asserts on the field.
- **Never fan one capture across connectors nobody ran.** A single verified Postgres run does not
  license the same screenshots in the MySQL guide. Fanning out is how these guides drifted into
  documenting fields that did not exist — a redshift `Schema` field, four fictional `rest-api` auth
  fields, a Kafka SASL section — in guides that all read as authoritative.

## 10. When a refactor makes a guard blind, fix the refactor — not the guard

Two of this repo's guards are **lexical**: they read source *shape*, not behaviour. Both went red on
core#804's delete dialog, neither was wrong, and each one was one keystroke away from being silenced
in a way nobody would ever have noticed.

- **`tests/test_security/test_tenant_fk_boundary.py`** recognises an org-scoped query by seeing
  `Model.org_id == org_id` **inside the `.where()` call**. Adding an `include_deleted` flag by
  building the conditions in a list first — `select(C).where(*conditions)` — is byte-identical SQL,
  and the scanner stops recognising it. The S1 guard from core#733 would have gone **blind on a query
  that was still perfectly correct.** That is strictly worse than a red test, because the red test
  eventually goes green and the blind spot never announces itself. Fix: keep `org_id` in the first
  `.where()` and chain the optional predicate onto the statement.
- **`tests/test_ui/test_rbac_ui_visibility.py`'s `_GateChecker`** tracks enclosing `rx.cond` role
  gates **lexically**. Moving a Delete button into a `_delete_connection_dialog()` helper takes it out
  of the gate's view, even though the call site was still wrapped in `rx.cond(AuthState.can_delete,
  …)`. Fix: make the helper **self-gating** — put the `rx.cond` inside it — rather than teaching the
  checker to follow function calls.

**The rule: when a lexical guard stops recognising your code, move the code back into the shape it
recognises, or move the gate to where it can see it.** Do not teach the guard about indirection, and
do not add an allowlist entry. Both trade a cheap, dumb, reliable check for a clever one, and the
cleverness is on the wrong side of that trade — a guard's whole value is that it cannot be argued
with.

**Corollary, and it is the useful half: a guard that goes red on your refactor has just told you
something.** The temptation is to read it as friction from a test that does not understand your
change. Ask first whether the shape it wanted was load-bearing. Twice out of two, here, it was.

⚠️ **The exception that proves it.** `tests/test_i18n/test_i18n.py`'s orphan-key scanner knew only
`_t["key"]`, so two keys read from `i18n.translations` inside a state handler looked like orphans.
That one *was* widened — because it was not a guard going blind, it was a guard that had never seen a
second, legitimate usage channel. The discriminator: **would the change make a real defect
invisible?** Widening the i18n scanner makes nothing invisible. Loosening either guard above would.
And note the failure mode if you get it wrong here: the obvious "fix" for an orphan-key failure is to
delete the key, which silently drops the translation for all nine locales and leaves the English
fallback.

## 11. A substring check over source is satisfied by prose *about* the code

The generalisation, and it is the most portable thing on this page:

> **A substring check over source is satisfied by prose about the code — and the prose most likely to
> contain the token is the comment explaining why the token is absent.**

`tests/test_ui/test_delete_confirmation_and_blocked_uploads.py` asked whether a persisted
destructive handler is role-gated like this — **fixed in [core#851]/PR #976; recorded because the
shape will recur, not because it is live**:

```python
module, _node, source = _state_handler(handler)
if "_check_role" not in source:          # `source` is the WHOLE handler, docstring included
    unguarded.append(...)
```

`SettingsState.leave_org` is deliberately **not** role-gated (`SPEC_ORG_ROLES` R6 — leaving is the
one action every member has), and its docstring says so — in a sentence that contains the literal
`` `_check_role("admin")` `` while explaining the comparison. So when [core#851]'s census was widened
to *see* `leave_org` at all (it had been invisible to the matcher: a Reflex handler taking no
arguments is referenced without parentheses, so there is no `ast.Call` node), the role assertion
would have gone **green on it** — certifying as guarded the one handler whose documentation states it
is not.

🚨 **Closing a matcher gap can therefore make a guard newly, silently wrong.** Widening what a check
*sees* is not a safe operation on a check that decides by string containment. Ask, before widening:
*of the things this will now examine, which ones talk about themselves?*

**The fix is not a better substring.** Parse it. The shipped guard now extracts the handler's actual
`self.<name>(...)` calls (`_self_calls(node)`) and asks whether `_check_role` is among them, so a
docstring can no longer answer for the code:

```python
body = [s for s in node.body if not isinstance(s, ast.Expr)]   # a bare string statement is the docstring
if "_check_role" not in ast.unparse(body[0]):
```

⚠️ **The technique was already in the file, in the very next method** —
`test_the_check_is_the_first_thing_the_handler_does` strips the docstring before matching, with a
comment saying why. One assertion in a class did it correctly and its neighbour did not, and nothing
reconciles two methods that agree on the token and disagree on the corpus. When you find a guard
matching on text, check its siblings before you trust any of them.

**Corollary for writing code, not tests:** a docstring that quotes the identifier it is explaining
the absence of is a hazard to every text-based tool that will ever read the file. That is not an
argument for vaguer docstrings — the `leave_org` one is excellent and should not change — it is an
argument for guards that parse. The prose is right; the reader was wrong.

## 12. An argued absence of a control is a decision. An unargued one is an oversight.

Product is asked constantly to add a confirmation, a toast, a warning. Declining is often correct,
and **the decline has to be written down where the next reader will meet the question**, or it is
indistinguishable from nobody having thought about it — and it gets "fixed" by someone with less
context.

Two live examples, both recorded in `SPEC_MUTATION_FEEDBACK.md` §D7 rather than left implicit:

- **`change_member_role` gets no dialog.** The control is an `rx.select`. A confirmation on every
  option change fires on `viewer → editor` exactly as it fires on `editor → admin`. **A dialog that
  fires on harmless changes is how a user learns to dismiss dialogs unread** — which would defeat the
  entire delete-confirmation programme ([core#804], [core#851]) that the same department spent three
  sessions building. It does get a toast: it changes *another person's* privileges and currently says
  nothing.
- **`update_org` gets no dialog.** There is an explicit Save button; the click is the declared
  intent, and a settings form is the canonical place where a confirmation buys nothing. It gets a
  toast, because success and failure are pixel-identical — the form goes on showing exactly what the
  user typed either way.

**A dialog is spent capital.** Every one you add makes every existing one slightly cheaper to
dismiss, so the argument for adding one has to survive being written next to the ones you declined.

Three practical consequences:

1. **Record the decline in the spec, beside the acceptances**, in the same table. A separate
   "rejected ideas" section is read by nobody; the table is read by whoever implements.
2. **Give the reason, not the verdict.** *"No dialog — not destructive"* is a verdict and will be
   overturned. *"It would fire identically on the harmless and the serious case"* is a reason, and it
   also tells the next person the condition under which it changes.
3. **Make the executable artifact point at the prose.** The ratchet lists in
   `tests/test_ui/test_mutations_acknowledge.py` carry the classification; each entry references the
   spec section that decided it. A test entry with no argument behind it is an allowlist, and an
   allowlist is how a guard goes quiet.

⚠️ Note the asymmetry with rule 10, which says: when a lexical guard goes red on your refactor,
*change the code, not the guard*. That rule and this one meet at the same place. There, the guard had
a load-bearing shape and you were arguing it away. Here, the guard has no opinion and **you** are
supplying one. The discriminator is unchanged: **would this make a real defect invisible?** Excluding
`change_member_role` from the dialog census makes nothing invisible — it is still in the *toast*
census, and the ratchet fails on any new handler that is in neither.
