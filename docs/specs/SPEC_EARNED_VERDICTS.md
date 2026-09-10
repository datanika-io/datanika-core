# SPEC — Earned verdicts: the product may not report an outcome it did not measure

**Author:** Product · **Status:** contract, ready for Engineering
**Written:** 2026-09-07 · **Binds:** Engineering
**Source of truth for:** the residue of [core#821] and [core#823]
**Verified against:** `origin/dev` @ `dc92f45` and `origin/master` @ `5726b8f`, both fetched
2026-09-07. Every count below was measured on those trees, and the measurement is named beside it.

> ⚠️ **Read §0 first.** Both issues this spec covers are labelled `shipped-to-prod` and read OPEN,
> and **both were fixed on `dev` on 2026-08-31 and are in production now.** A reader who starts from
> the issue bodies will spend a session re-deriving work that is done. What is *not* done is the
> contract, and that is what this file is.

---

## §0 — What is already fixed, and three corrections to the record

### 0a · Both defects are fixed and live. Measured, not inferred.

| | fix present on `origin/master` (production) | measurement |
|---|---|---|
| [core#823] silent truncation | ✅ | `dlt_runner.py` carries `REST_FALLBACK_SAAS_TYPES` and 17 `paginator=` occurrences on **both** `master` and `dev` |
| [core#821] vacuous green | ✅ | `connection_service.py` returns `bool \| None` on both branches (4 occurrences of the tri-state annotation) |

So the two headline defects are closed. **This spec is about what neither fix established**, which
is the invariant in §1. Neither PR was wrong to stop where it did — a fix is not obliged to write
the contract — but a fix that removes *one* cause of a false verdict leaves the product still able
to emit one, and nothing yet says it may not.

### 0b · 🔴 A correction to my own previous handoff

`plans/product/current_state.md` (2026-09-07) told the next session:

> *"Test Connection's verdict is still not evidence — but ONE way. [core#821]'s residue is the
> **colour** problem, with **zero i18n keys** on that text."*

**Half of that is wrong, and it is the half that would have sent someone to fix a working screen.**
Measured on `connections.py:134-153`: the New Connection form's callout picks its colour with
`test_untested` as the **outer** `rx.cond`, so `None` short-circuits to `color_scheme="gray"` and
`icon="info"` before the green/red branch is ever evaluated. **The colour is correct on that
surface.** The i18n half is right — see §3.3, where the zero is confirmed in all nine locales.

The real residue is on a **different surface** (§3.2), which is why "the colour problem" found
nothing when checked: I had recorded the right suspicion against the wrong screen.

### 0c · 🔴 A correction to a finding produced during this session's own investigation

A source sweep for this spec reported, correctly, that **`datanika/` contains zero references to
`failed_jobs`, `has_failed_jobs` or `raise_on_failed_jobs`** — dlt's own "did any load job fail"
signal. Read as a defect, that is alarming: it says a load could half-fail and still be marked
`SUCCESS`.

**It is true and it is not a defect.** `dlt 1.21.0`'s `LoaderConfiguration.raise_on_failed_jobs`
defaults to **`True`** — read off the installed package, not the docs:

```
dlt 1.21.0
LoaderConfiguration.raise_on_failed_jobs = True
```

So a failed load job raises out of `pipeline.run()`, propagates to `upload_tasks.py`'s
`except Exception`, and the run is marked `FAILED`. We do not read the signal because dlt never
hands it to us intact.

🔑 **The clause this earns is a smaller and better one than the bug would have been**, and it is
§4.4: *our run status is honest because of a third-party default that nothing in our tree pins,
asserts, or would notice changing.* That default was `False` in dlt before 1.0. Same family as the
graft-install lesson in `CLAUDE.md` — **a constraint you rely on and do not state is not a
constraint.**

⚠️ Recorded at this length because the sweep's finding was handed to me and reads as a serious bug,
and confirming it would have cost Engineering a redundant fix and put a false claim in a contract.
`WORKFLOW_RULES` §"A CORRECTION to your work is the least-audited input you will ever receive"
applies to a *report* you commissioned exactly as it applies to a correction you received.

---

## §1 — The invariant

Both defects are the same defect, and the founder's framing names it exactly: **a success signal
emitted without the thing succeeding.**

> **A verdict the product shows must be earned by a measurement of the thing the verdict describes.**
>
> Where no measurement was made, the product says *"not measured"* — a third answer, visually
> distinct from both success and failure — and never picks one of the two it did not earn.

Three corollaries, each of which one of the two defects violated:

1. **Silence is not the third answer.** A control that answers nothing is indistinguishable from a
   control that is broken, and the user's next move is to press it again. (§3.2 — live today.)
2. **Absence is not zero.** A quantity we failed to read is `NULL`, not `0`. A zero is a
   measurement; an unread value is not. (§4.2 — live today.)
3. **A number shown without its unit of trust is a claim.** "Rows: 10" beside a green badge asserts
   completeness. If the number is *rows this run wrote* rather than *rows the source held*, the
   product must say which. (§4.3 — live today.)

🚨 **What this spec does NOT require, deliberately.** It does not require source-to-destination
reconciliation. For most of these APIs there is no cheap total to reconcile against — our own
paginator table records that Pipedrive's responses carry no `total` at all
(`dlt_runner.py:483-489`). Demanding proof of completeness would be an unbuildable contract, and an
unbuildable contract is quietly ignored. **The contract is the weaker and enforceable one: never
claim a completeness you did not measure, and never discard a signal you already have.**

---

## §2 — Why the two fixes did not settle it

| | what the fix closed | what it left open |
|---|---|---|
| [core#821] | the vacuous `True` for 20 SaaS types; a real probe for 14; a neutral `None` for 6 | the `None` verdict is **invisible** on the saved-connection surface (§3.2), and its six sentences are **untranslated** (§3.3) |
| [core#823] | auto-detection truncating 13 connectors; the ignored `dlt_config["paginator"]` | nothing anywhere distinguishes a complete load from an incomplete one (§4.1); a failed count reads as `0` (§4.2); `jira` is still exposed **by decision** and the user is not told (§4.5) |

🔑 **The shape worth naming: both fixes removed a cause and neither added a detector.** After
[core#823], a fifteenth connector cannot silently omit a paginator — `paginator` is keyword-only
with no default, so it is a `TypeError` at build time. That is a good ratchet and it is the *only*
one. It constrains our code; it does not constrain the vendor. If Stripe changes its cursor
semantics tomorrow, the run still goes green with a plausible number, exactly as it did in August.

---

## §3 — Contract A: what Test Connection must do when it genuinely cannot test

### 3.1 · The decision already shipped is right. Keep it.

`test_connection` returns `bool | None`; `None` means *not tested*; the six exempt types each carry
a stated reason rather than a shrug (`connection_service.py:584-621`). On the New Connection form
this renders grey with an `info` icon.

**That is the answer to the question this spec was asked**, and it is already the product's answer:
when the product cannot test, it says so in a third colour. Two clauses make it true everywhere.

### 3.2 · AC1 — a "not tested" verdict must be visible on **every** surface that offers the button

🚨 **Live defect.** `ConnectionState.test_saved_connection` (`connection_state.py:1663`) reads:

```python
ok, _msg = ConnectionService.test_connection(config, conn.connection_type)
```

The message is **discarded**, and `None` sets the row's status to `""` — which `connections.py:308-327`
renders with `visibility="hidden"`. So a user who clicks **Test** on a saved `rest_api`, `kafka`,
`openapi` or Google-family connection gets **no icon, no text and no change of any kind**: a result
byte-identical to never having pressed the button.

**This is corollary 1 in production.** It is a smaller lie than the green one [core#821] fixed, and
it is the same kind: the user asked a question and the product's answer is indistinguishable from
the control being broken. The obvious next move is to click again.

**Acceptance:**
- The saved-connection row shows a **neutral** third marker for `None` — distinct from the green
  tick and the red cross, and distinct from *never tested*.
- The verdict's **reason is reachable from that row.** A tooltip is sufficient; silence is not. The
  six sentences exist and are good; they are simply thrown away here.
- ⚠️ **Do not reuse `""`.** `test_status = ""` already means *never tested*, and overloading it is
  how the two states became indistinguishable. A user who has never clicked and a user who clicked
  and cannot be told are owed different screens.

### 3.3 · AC2 — the six "not tested" sentences need all nine locales

Measured across `datanika/i18n/*.json`, 2026-09-07 — every locale carries **744 lines** and **6**
`connections.test*` keys, and **`"Not tested"` appears 0 times in all nine**:

```
en 0   ru 0   de 0   fr 0   es 0   el 0   zh 0   ar 0   sr 0
```

The mechanism to fix it already exists and is one line of data. `_verdict_message`
(`connection_state.py:1582-1607`) translates via `_VERDICT_KEYS`, keyed on `ConnectionVerdict.reason`
— and `_test_saas_source` returns a bare 2-tuple, so `reason` takes its default `""`, misses the
map, and falls through to the service's English.

**Acceptance:**
- ✅ **SHIPPED 2026-09-10.** Each of the six exempt types carries a `reason` slug derived from the
  connector name (`not_tested_<name>`), and each slug a key in **all nine** locales — 742 → 748 keys
  per file, parity green. `_test_saas_source`'s exempt branch returns a 3-tuple;
  `ConnectionVerdict(*...)` already splatted, so nothing else changed.
  ⚠️ **The slug is derived, not stored beside the sentence.** A second column in `SAAS_PROBE_EXEMPT`
  would be a hand-maintained parallel list, and a guard over it would assert a mapping against the
  copy of it that produced the mapping.
- ⚠️ **These are `callout text`, which `WORKFLOW_RULES` §6 lists under Translate** — not the
  "dynamic error messages" it lists under Skip. They are six fixed sentences, one per connector, and
  they are the entire content of that surface.
- The defensive fallback at `connection_service.py:1245`
  (`f"Not tested: no credential probe exists for {name}"`) stays English **by decision**: it is
  dynamic, and it is unreachable while `test_no_saas_type_is_undecided` holds. Say so in a comment
  so the next i18n sweep does not read it as an omission.
- 🚨 **The English strings are NOT free to be rewritten while adding keys. See §3.3a.**

### 3.3a · 🚨 These six sentences are ALREADY the source for a second corpus

**Do not treat AC2 as a copy-editing opportunity.** Adding a key is additive; rewriting the English
underneath it silently breaks documentation in another repository.

`datanika-landing` shipped the corrected Test Connection copy across the guide corpus, and
`docs/GROWTH_RULES.md` states the direction of authority explicitly:

> *"A **"not tested"** verdict is neither a pass nor a failure, and copy must not render it as
> either. … **Core's own framing is the one to reuse:** reporting an unverified connection as working
> and reporting it as failed are the same lie told in opposite directions."*

So the guides **derive from these strings**. Measured on landing `main`, 2026-09-07: the phrase
*"not tested"* appears in **exactly six** connector guides — `rest-api`, `openapi`, `google-sheets`,
`google-analytics`, `google-ads`, `kafka` — a **1:1 match with `SAAS_PROBE_EXEMPT`**. That is not a
coincidence; it is the coupling.

**The three terms that must not drift**, because both corpora and a landing guard now depend on them:

| term | where it is load-bearing |
|---|---|
| **"not tested"** — the verdict's *name* | the six guides, `SPEC_TEST_CONNECTION_GUIDE_COPY.md`, `GROWTH_RULES` |
| **neutral** — *"neither green nor red"* | `connections.py:134-153`'s `color_scheme`, and the guides' promise about what the user will see |
| **the first pipeline run** — where verification actually happens | every one of the six guides routes the reader there |

> 🔴 **CORRECTED 2026-09-10, on implementing it — and the correction makes the constraint NARROWER,
> not looser.** This clause said the keys must carry *"the existing English as `en`, unchanged"*.
> **Shipped: they do not, and they should not.**
>
> **Two measurements changed the answer.**
>
> 1. **The guides PARAPHRASE; they do not quote.** Measured on landing `main`: the sentences
>    *"belongs with the Google helpers, not in this service"* and *"resource paths live on the
>    upload"* appear **0 times** across `src/content/connectors/`. What the guides assert is the
>    **behaviour** — *"returns a neutral **not tested** verdict with that reason"*. **So the coupling
>    is on the verdict's existence and neutrality, not on its wording.**
> 2. **`ConnectionVerdict` already separates the two**, and its own docstring says so: `message` is
>    *"English, for the API, the logs and any caller with no locale"*, while the UI owns
>    `reason` → key. **They were never meant to be the same string.**
>
> 🚨 **And preserving the English verbatim would have shipped an implementation detail to users.**
> The `google_sheets` message reads *"…which belongs with the Google helpers, **not in this
> service**"*. That is a sentence about **our code layout**. A user cannot act on it, and it is in
> the one place a user goes when they are already confused.
>
> **What actually holds:**
>
> - **`message` is unchanged** — byte for byte. The API, the logs and the guides see exactly what
>   they saw before, so nothing downstream moves.
> - **The UI string is new copy**, shorter, and written to say *what happened and what to do next*.
> - **The three terms in the table above still may not drift** — *"not tested"*, **neutral**, and
>   *the first run*. Those are what the guides actually depend on, and all six new strings carry
>   them.
>
> 🔑 **The original clause was right about the hazard and wrong about the mechanism.** The hazard is
> a generator reseeding 22 guides from copy that moved underneath it. The mechanism protecting
> against it is **not** "never touch the English" — it is *"do not move the terms the guides assert"*.
> A blanket freeze would have locked in a sentence about the Google helpers forever.

*(Superseded, kept as the record:)* **Acceptance for AC2 therefore gains one clause:** the nine-locale
keys carry **the existing English as `en`, unchanged**.

🔑 **The mechanism to be careful of is a generator, not a person.** Growth reseeded **22** guides from
a shared template today. A template is a multiplier in both directions: it fixed 22 pages at once, and
a wrong sentence in it would have shipped 22 times. **The product string is upstream of that template**,
so a rewrite here propagates through a generator nobody re-reads. Same shape as `WORKFLOW_RULES` §4's
*"a guide corrected to deny an old behaviour still contains the old words"* — one level up, at the
thing that writes the guides.

### 3.4 · Not a defect: the API's collapse of `None`

`api_v1_routes.test_connection` returns `{"success": ok is True, "tested": ok is not None, …}`,
mapping `None` to `success: false` with the distinction in a new `tested` field. That is correct and
deliberate — `docs/api_versioning.md` classes a `bool` becoming nullable as a breaking change.
**Do not "fix" this to match the UI.** A wire contract and a screen have different obligations, and
this one already carries the third state additively.

---

## §4 — Contract B: what a partial load must show

### 4.1 · The measurement

`ExecutionService.complete_run` (`execution_service.py:59-85`) sets `run.status = RunStatus.SUCCESS`
**unconditionally**. There is no predicate on rows, on `load_info`, or on anything else. The only
route to a non-success terminal status is an exception reaching `upload_tasks.py:344`.

`RunStatus` has five members and none of them is partial:

```python
PENDING · RUNNING · SUCCESS · FAILED · CANCELLED      # models/run.py:11-16
```

**Decision: do not add a sixth.** `SPEC_RUN_CANCELLATION` already records that a new status member
has to reach **seven hand-maintained status lists**, and there is already one pending (`cancelling`).
Buying a `PARTIAL` member would cost that twice over and would still be a status nothing knows how
to *set*, because §1 declines to build reconciliation. The three clauses below are cheaper than a
status member and each closes a live falsehood.

### 4.2 · AC3 — a count we failed to read is not zero

🚨 **Live defect, two halves.**

**The extractor.** `_extract_rows_loaded` (`dlt_runner.py:311-329`) ends:

```python
    except Exception:
        return 0
```

Any failure to read dlt's trace yields **0 rows on a green run** — indistinguishable from a genuinely
empty load.

**The UI.** The schema deliberately pays for the distinction the UI then erases.
`models/run.py:39` is `Mapped[int | None]`, `nullable=True`, and the sibling `bytes_processed`
carries a comment saying `NULL` means *"not measured"* and that writing `0` "would erase that". Then:

```
ui/state/run_state.py:25        rows_loaded: int = 0
ui/state/run_state.py:87        rows_loaded=r.rows_loaded or 0
ui/state/dashboard_state.py:178 rows_loaded=r.rows_loaded or 0
ui/state/model_state.py:164     last_run_rows=last_run.rows_loaded or 0 if last_run else 0
```

So a never-measured run and a genuinely-empty run both render `0`.

**Acceptance:**
- `_extract_rows_loaded` returns `None` when it cannot read the trace, and logs why. `0` is reserved
  for a measured zero.
- `Run.rows_loaded is None` renders as a **non-numeric** marker (`—`) in both tables, never `0`.
- ⚠️ The four `or 0` coercions above are the change. **Do not fix this in the template** — a
  `rx.cond` in two pages leaves the state var lying to every other reader, and `model_state.py:178`
  already makes a decision (`(r.rows_loaded or 0) > 0`) on the coerced value.

### 4.3 · AC4 — the "Rows" column must say what it counts

`runs.py:74` and `dashboard.py:63` render `r.rows_loaded` under a header reading exactly **"Rows"**
(`runs.rows`, `dashboard.rows`). Nothing qualifies it — no tooltip, no footnote, no help text.

The number is dlt's **normalize-step** count with `_dlt_*` tables excluded. That is neither *what the
source held* nor *what the destination confirmed*. Beside a green badge it reads as a completeness
claim, and in the [core#823] incident it read `18` while the source held `23`.

**Acceptance:**
- The column gains a tooltip stating the number is **rows this run wrote**, not rows available at
  source. One key, nine locales, both pages.
- ⚠️ **Keep the header word "Rows".** The fix is a qualifier, not a longer header; a two-word column
  heading is not more honest, it is just narrower.

### 4.4 · AC5 — pin the dlt default our run status depends on

Per §0c: `SUCCESS` is honest only because `dlt`'s `raise_on_failed_jobs` defaults to `True`. Nothing
in our tree states that, and it was `False` before dlt 1.0.

**Acceptance:**
- A test asserts `LoaderConfiguration.raise_on_failed_jobs` is `True` as resolved by the installed
  dlt, with a comment naming what breaks if it is not: a load with failed jobs returning a `LoadInfo`
  we would mark `SUCCESS`.
- ⚠️ **Assert the resolved value, not the pin.** Reading it off `LoaderConfiguration` is the check;
  reading our `pyproject.toml` is not, and an env override (`LOAD__RAISE_ON_FAILED_JOBS`) would be
  invisible to the second.
- This is a **guard, not a fix.** Nothing is broken today and the test must say so, or the next
  reader will go looking for the bug it prevents.

### 4.5 · AC6 — a connector we know does not paginate must say so in the run

`SAAS_PAGINATION_EXEMPT` holds **one** entry, `jira`, with a good reason:
an offset paginator would page `search` correctly and then re-request the unpaginated `project`
array forever — *"A hang is worse than a short table."*

**That trade is correct and this spec affirms it.** But by `dlt_runner.py`'s own analysis, `jira`
falls in the `SinglePagePaginator` group — so the exempt connector is *still exposed to the exact
defect [core#823] fixed*, and the only place that is written down is a source comment.

**Acceptance:**
- A run using a connector in `SAAS_PAGINATION_EXEMPT` appends a line to `run.logs` saying this
  connector is not paginated by us and the table may be short. Use the existing mechanism —
  `execution_service.append_logs`, exactly as `upload_tasks.py:300` and `:325` already do for the
  two catalog warnings. **No new machinery.**
- The line is derived from `SAAS_PAGINATION_EXEMPT`, not from a literal, so entry two is covered on
  the day it is added.
- 🔴 **Fix the comment while you are there.** `dlt_runner.py:505` says *"Both entries below"* and the
  dict has held **one** since `salesforce` moved out. A stale count in a comment about completeness
  is the smallest possible instance of this spec's own subject.

---

## §5 — Acceptance criteria that can tell a real success from a fabricated one

The founder's brief asked for exactly this, and `SPEC_AUDIT_TRAIL` §4 is the cautionary case: **three
of its test-design clauses were wrong, and two of them went red on correct code.** So the rules here
are written to avoid repeating that.

### 5.1 · Every AC above is an invariant, not an assertion

None of §3 or §4 names a data structure to inspect, a fixture to patch, or an intermediate state to
read. That is deliberate. `SPEC_AUDIT_TRAIL` §4's three wrong clauses shared one shape — each
asserted on **machinery** (`session.new`, `session.dirty`, *"a row exists afterwards"*) rather than
on the property the user is owed — and machinery is what a harness silently changes underneath a
spec.

> **Product states the property. Engineering chooses the assertion and shows it red.**

### 5.2 · The negative control must be the shape that motivated the AC

For each AC, the mutation is named and it is the **pre-fix behaviour**, which is free:

| AC | mutation that must go red | why this one |
|---|---|---|
| 3.2 | return `None` and set `test_status = ""` | that is today's code |
| 3.3 | delete one of the six new keys from **one** non-English locale | the parity test reads merged translations; [core#851] found a guard that could not fail this way — read the locale's own JSON |
| 4.2 | make `_extract_rows_loaded` raise | must show `—`, not `0` |
| 4.4 | force `raise_on_failed_jobs = False` in the resolved config | proves the guard is reading the resolved value |
| 4.5 | remove `jira` from `SAAS_PAGINATION_EXEMPT` | the log line must disappear — proving it is derived, not literal |

### 5.3 · 🚨 The one assertion that must **not** be written

**Do not assert that a SaaS run "succeeds and lands rows".** [core#823]'s own issue body says why,
and it is the single most important sentence in either issue:

> *"A test that asserts 'a SaaS run succeeds and lands rows' passes right now, with 10 of 15. …
> A fixture with fewer records than the page size cannot fail, which is precisely how this
> survived."*

Any completeness test must serve **more records than the page size** and compare the count the
fixture served against the count that landed. Engineering's shipped regression test does this — 15
rows at a page size of 10 — and it is the model to copy.

### 5.4 · Report the number either way

Where an AC implies a sweep (§4.2's four `or 0` sites, §3.3's nine locales), **state the count in the
PR body even when it is zero.** An absent sentence and a completed sweep look identical — the lesson
from [core#1097] AC5.5, where the sweep found **eleven** loaders against a spec that named one.

---

## §6 — Out of scope

- **Source-to-destination reconciliation.** §1 declines it, with the reason. If it is ever built, it
  is a new spec and it starts by asking which vendors expose a countable total.
- **A `PARTIAL` run status.** §4.1 declines it, with the cost.
- **[core#869]** BigQuery reflection returning 0 tables — a different defect on the same screen, QA's.
- **[core#850]** `salesforce` default resources being describe endpoints. Filed from [core#823]'s
  work; `salesforce` has since gained a paginator, and the resource question is separate.
- **The two catalog-sync WARNINGs** (`upload_tasks.py:300`, `:325`). They are correct as diagnostics
  and this spec deliberately reuses their mechanism rather than replacing it.

---

## §7 — What was measured that contradicted the written record

Kept because each of these was believed by someone, including by me:

1. **My own handoff located [core#821]'s residue on the wrong surface** (§0b). The form's colour is
   correct; the saved-connection row is the broken one.
2. **A commissioned sweep produced a true fact that reads as a serious bug and is not one** (§0c).
   `raise_on_failed_jobs` defaults to `True`.
3. **Both issues read OPEN with `shipped-to-prod` and both fixes are live in production.** On this
   tracker that label means *the bug is live*, not *the fix shipped* — and here it is stale in the
   reassuring direction for once.
4. **`docs/specs/README.md` did not index this spec's two nearest siblings.** `SPEC_AUDIT_TRAIL.md`
   and `SPEC_LOCAL_FILE_CONNECTIONS.md` were on disk and absent from the index; three status cells
   were stale. Corrected in the same change as this file.
5. 🆕 **AC2 looked like a self-contained i18n chore and is not** (§3.3a, added 2026-09-07). These six
   English sentences are the upstream source for six connector guides in another repository, and
   `GROWTH_RULES` says so in as many words. **Nothing in this spec said that when AC2 was written** —
   an implementer following AC2 as originally phrased could reasonably have tidied the English while
   adding keys, and broken six published pages plus a landing guard, in a repo they were not working
   in and would not have run.

[core#821]: https://github.com/datanika-io/datanika-core/issues/821
[core#823]: https://github.com/datanika-io/datanika-core/issues/823
[core#850]: https://github.com/datanika-io/datanika-core/issues/850
[core#851]: https://github.com/datanika-io/datanika-core/issues/851
[core#869]: https://github.com/datanika-io/datanika-core/issues/869
[core#1097]: https://github.com/datanika-io/datanika-core/issues/1097
