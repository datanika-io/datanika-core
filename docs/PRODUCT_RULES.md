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
3. **Target by id, inside the card, with the subject asserted.** Chrome's password manager renders
   `#deleteButton` (one credential) and `#deleteAllButton` (**everything on the device**) on the same
   page, with near-identical labels in the founder's locale. The single-credential delete needs no
   confirmation dialog; the catastrophic one does — so "a dialog appeared" is not a safety signal.
4. **Repair with a targeted DB update, not by re-running a UI loop.** Recovery from the incident above
   was one `UPDATE connections SET deleted_at = NULL`. Soft delete is what saved it, not care — and
   uploads, schedules and API keys do not all behave that way.
5. **Do not submit forms you are only photographing.** An Active schedule fires nightly runs into live
   alerting.

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
