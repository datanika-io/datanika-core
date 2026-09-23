# SPEC — Showing a customer what they have used

**Status:** contract, ready to build
**Owner:** Product (what is shown, and where). Engineering owns the implementation, in **two repos**.
**Closes the design question on:** [core#1513], [cloud#180], [cloud#174]
**Written:** 2026-09-23, against `origin/dev` and against a running stack.

[core#1513]: https://github.com/datanika-io/datanika-core/issues/1513
[cloud#180]: https://github.com/datanika-io/datanika-cloud/issues/180
[cloud#174]: https://github.com/datanika-io/datanika-cloud/issues/174
[core#713]: https://github.com/datanika-io/datanika-core/issues/713
[landing#656]: https://github.com/datanika-io/datanika-landing/issues/656

---

## 0. Why one spec and not three tickets

Three issues describe three surfaces — the dashboard's Plan Usage card ([core#1513]), the billing
page's usage card ([cloud#180]) and the billing page's upgrade card ([cloud#174]). They are the same
defect seen from three angles: **we bill in a unit we never display.**

Specified separately they will disagree, because each would pick its own divisor, its own wording for
a cap, and its own answer to *"what does an org with no subscription see?"* — and the two repos have
no test that can see the other. That is [core#1513]'s own framing, and it is why this is a contract
rather than three tickets.

---

## 1. The measurement, and the thing every issue got wrong

**Bytes are the billed dimension. No screen in the product shows a byte count.** That much every
issue states correctly.

### 1a. What I read off a running stack, on a fresh Free org

| reading | value |
|---|---|
| dashboard Plan Usage card, new Free org | `Free` · **`0 / 500 model runs this month`** · no byte line |
| `plans` row, `free` | `runs_included=500` `hard_cap_runs=true` **`bytes_included=10737418240`** (10 GiB) **`hard_cap_bytes=true`** |
| `subscriptions` | **0 rows** — every org resolves to `free` |
| `settings.datanika_dual_mode_ux_enabled`, from the app process's own interpreter | **`False`** |

### 1b. 🚨 THE FLAG IS NOT THE ONLY GATE, AND REMOVING IT ALONE CHANGES NOTHING

Every one of the three issues, and the remedy [core#1513] floats (*"show the dashboard's volume
dimension … instead of behind the ETL/ELT UX flag"*), names **one** gate. There are **two**, in two
different repos, and they are independent.

I called the hook for real rather than reasoning about it — `usage.get_summary`, against the running
app, for the actual Free org and for a **fabricated org id** as a control:

```
--- org 7  [REAL ORG] ---                    --- org 999999 [FABRICATED, negative control] ---
  plan_name   = 'Free'                         plan_name   = 'Free'
  runs_limit  = 500                            runs_limit  = 500
  bytes_limit = 0   (GiB: 0.0)                 bytes_limit = 0   (GiB: 0.0)
  has_volume_data would be: False              has_volume_data would be: False
```

**`bytes_limit` is `0` for an org whose plan row carries 10 GiB.** Core gates the volume dimension on
`DashboardState.has_volume_data`, which is `bytes_limit > 0`. So:

> **Removing `if settings.datanika_dual_mode_ux_enabled:` from `dashboard.py` would render nothing at
> all.** The component would be mounted and `has_volume_data` would still be `False`.

The reason, read from the **installed** package in the serving container rather than from a branch —
`BillingService.fill_usage_summary` sets exactly three fields:

```python
context["runs_used"]  = self.get_current_usage(session, context["org_id"], "model_runs")
context["runs_limit"] = plan.runs_included
context["plan_name"]  = plan.name
```

It never assigns `bytes_used` or `bytes_limit`, so both keep core's default of `0`.

### 1c. 🔑 This exact defect was already fixed once, for the other dimension

`fill_usage_summary`'s own docstring:

> *"The seventh call site, and the only one that gates nothing. Returning early leaves `runs_limit`
> unset, so a Free org saw no allowance and no progress bar — **the orgs on the tightest cap were the
> ones shown nothing about it.**"*

That is [cloud#180]'s headline — *"the only hard-capped tier is the only tier with no usage meter"* —
written as a past-tense fix, while **the identical defect is live right now one dimension over.** Free
is hard-capped on **both** dimensions; the runs half was repaired and the bytes half was never
written.

**Design consequence, and it is the main reason this spec exists:** the fix is not "unhide a
component". It is *"a dimension the plan row carries is a dimension the customer sees"*, and it has to
be stated once, for both dimensions and all three surfaces, or the next dimension repeats it a third
time.

### 1d. ⚠️ A control that should worry whoever builds this

The fabricated org returned **identical** output to the real one. `_resolve_plan` falls back to the
free plan for an org id that does not exist, so `plan_name` and `runs_limit` are **not readings about
that org**. Do not use this hook as evidence that a particular org is entitled to something. AC7
requires a test that distinguishes them.

---

## 2. The decision

**2.1 · A dimension the plan row carries is a dimension the customer sees.** If a plan row has a
non-`NULL` allowance for a dimension, every usage surface shows it. Nothing about what is displayed
depends on a UX feature flag, on whether a subscription row exists, or on which dimension someone
considered primary when the surface was written.

**2.2 · Volume leads. Runs follow.** Bytes are what we charge for; `SPEC_PRICING_V2` §2.2 demotes runs
to a fair-use line. Every surface orders volume first. This reverses the current order on all three.

**2.3 · A `NULL` allowance renders nothing — never `0`.** `bytes_included IS NULL` means *this plan
has no volume dimension*, which is not the same as *an allowance of zero*, and a `0 / 0 GB` meter is
the worse of the two lies. Applies to every dimension, not just bytes.

**2.4 · A cap and an allowance must not look the same.** `hard_cap_* = true` means *runs stop*;
`false` means *overage bills* ([core#713]). A full bar means "blocked" on Free and "now billing" on
Pro, and a customer cannot be shown the same thing for both.

- capped: name the wall — *"10 GiB included, hard cap"*.
- uncapped with a price: name the price — *"100 GB included, then $0.50/GB"*.
- uncapped with no price (`overage_*_price_cents*` `NULL`): *"included allowance"*, and **no
  suggestion that exceeding it costs money**.

**2.5 · Every published figure is read from the plan row.** Never a constant in the page, never a
number copied from `/pricing`. Binding the displayed half to a local constant converts a visible
mismatch into a coherent assertion of something untrue ([cloud#174] says this and it is right).

**2.6 · The divisor is `1024**3`, everywhere.** `billing/tasks.py` converts with `1024**3`, core's
`bytes_used_display` already does, and `/pricing` publishes binary GB. A surface that picks `1000**3`
disagrees with the invoice. `SPEC_PRICING_V2` records the $0.40-vs-$0.39 version of this, where the
site agreed with the biller and the spec was the outlier.

**2.7 · An org with no subscription row is a Free org and sees a Free org's meters.** `subscriptions`
holds 0 rows, so this is 100% of orgs today, not an edge case. No usage surface may be gated on
`has_subscription`.

**2.8 · 🚨 Do NOT flip `datanika_dual_mode_ux_enabled`.** It also mounts the ETL/ELT mode selector,
which persists nothing ([landing#656]) — flipping it publishes a broken control to fix an unrelated
one. **The volume dimension stops being gated on it; the flag keeps its own job.** Per §1b this is
necessary and *not* sufficient on its own.

---

## 3. The three surfaces

Same contract, three renderings. `get_current_usage(session, org_id, "bytes_processed")` already
handles the byte metric uniformly — its own docstring says so — so no metering change is in scope.

### 3.1 · Core — the dashboard Plan Usage card

`datanika/ui/pages/dashboard.py`. `_volume_dimension()` is **already written**, and
`quota.volume_title` / `volume_usage` / `volume_overage` / `volume_quota_reached_title` /
`volume_quota_reached_body` are **already present in all nine locales** (verified: `en ru el de fr es
zh ar sr` = 5 keys each).

> 🔑 **Core's half needs no new translated strings and no new component.** It is the removal of one
> `if` and whatever §3.2 has to supply to make `has_volume_data` true.

### 3.2 · Cloud — `fill_usage_summary`

Assign the two byte fields the way the runs fields are already assigned. This is what actually makes
a meter appear, on the dashboard **and** anywhere else reading the hook.

### 3.3 · Cloud — the billing page

The usage card ([cloud#180]) renders for every org, volume first. The upgrade card ([cloud#174])
gains the volume allowance and the per-GB overage rate from the plan row, with runs demoted per §2.2.

---

## 4. Acceptance criteria

**AC1 — A Free org with no subscription sees its volume allowance on the dashboard.** The meter shows
a non-zero limit of **10 GiB** and is marked as a hard cap. ⚠️ **Prove it red against §1b: the
current code is a free negative control, and a change that only removes the flag gate must still
fail this.** A test that passes with `fill_usage_summary` unchanged is testing nothing.

**AC2 — The billing page usage card renders for an org with no subscription row**, volume first, then
runs. Prove it red by restoring the `has_subscription` gate.

**AC3 — The upgrade card names the volume allowance and the per-GB overage rate**, both read from the
plan row. Prove it red by removing one dimension; scope the assertion to the card, not the page.

**AC4 — A `NULL` allowance renders nothing, on every surface.** Drive it with a plan row whose
`bytes_included IS NULL` and assert no volume element exists — **not** that it shows `0`.

**AC5 — Capped and uncapped are distinguishable, and the test requires them to DIFFER.** Drive the
same component with a `hard_cap_bytes=true` plan and a `hard_cap_bytes=false` plan and require the
rendered output to differ. ⚠️ **Asserting that each renders "something" passes on a component that
ignores the flag entirely** — that is the `core#1492` shape, where a guard that answers both
populations the same way looks cautious and discriminates nothing.

**AC6 — The divisor is asserted against the biller, not against a literal.** A test that hardcodes
`10` for 10 GiB passes under either divisor. Derive the expected value with `1024**3` from the same
constant the biller uses, and include a value where the two divisors visibly disagree.

**AC7 — The usage hook distinguishes a real org from one that does not exist.** Per §1d, it currently
does not. At minimum, a fabricated org id must not produce a confident allowance. *(If the ruling is
that falling back to Free is correct, say so on [cloud#180] and make the test assert the fallback
deliberately — what is unacceptable is that the two are indistinguishable by accident.)*

**AC8 — i18n.** Core needs **no** new keys (§3.1). Any new cloud string lands in all nine locales of
`CLOUD_TRANSLATIONS`. ⚠️ **There is no parity test over that dict** — `test_plugin.py` asserts one key
in one locale. [cloud#174] AC0 and [cloud#180] AC7 both call that a prerequisite; it is, and it is the
first thing to build.

**AC9 — 🔗 Landing's guard flips with this change, by design.** `tests/in-app-usage-claims.test.ts`
in `datanika-landing` has its flip condition written against [core#1513]: when a screen ships showing
byte usage, it **starts refusing** the currently-corrected blog copy. That is deliberate, so the copy
is restored in the same change rather than rotting on a checklist. **Comment on [core#1513] when this
reaches `master`** — that is all Growth asked for, and it is the whole handoff.

---

## 5. Out of scope

- **Whether runs are billed at all** — `cloud#177`, founder's, answered *not billed*. This spec
  displays runs as a fair-use line and takes no position on pricing.
- **Any change to metering, quota enforcement or plan rows.** Read-and-render, end to end.
- **Flipping `datanika_dual_mode_ux_enabled`** (§2.8) and the ETL/ELT selector behind it
  ([landing#656]).
- **The schedules line** — `cloud#170`.
- **Landing copy.** The corrected posts stand until AC9 fires.

---

## 6. Sequencing

Merging order is **core first, then cloud** (`WORKFLOW_RULES`: cloud has no migrations of its own).
Promotion order is the inverse — **cloud first, then core** — because cloud ships inside core's image,
and a cloud promotion with no core promotion behind it has not shipped.

⚠️ **Neither half is useful alone, and each will look like a working change on its own branch.** Core
without cloud renders an empty dimension; cloud without core fills fields nothing displays. Land them
in one release and verify on the serving container, not on either branch.
