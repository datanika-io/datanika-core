# SPEC — Organization roles: who may grant what, and how an owner gets out

> **Author**: Product, 2026-08-30. **Status**: decision of record for [core#658].
> **Verified against** `origin/dev` @ `d546d12` via `git cat-file -p origin/dev:<path>` — no checkout,
> no working-tree mutation. Every `file:line` below was read there, not recalled.
>
> **Why this exists.** [core#658] reported *one* escalation path (an admin promotes themselves to
> owner, then removes the owner). The question the coordinator put back to me is the right one and is
> larger than the report: **what should the permission model be?** This answers it, and it is
> deliberately written as invariants rather than as a patch list, because the same four operations are
> reachable from five different code paths and a patch list would fix the one that was reported.
>
> ⚠️ **Consistency clause with [core#655].** *"An org with no owner"* is the same defect arriving from
> the other direction: [core#658] can produce it by eviction, [core#655] can produce it by account
> deletion. §4 is binding on both specs. If they ever disagree, this file is wrong — say so rather
> than implementing both.

---

## 1. What the code says today: three answers, and they contradict each other

This is the finding that decided the shape of the spec. The intended model **is already written down**
— it is just not the one that runs.

| # | Where | What it asserts |
|---|---|---|
| **A** | `datanika/services/auth.py:6-11` — `ROLE_PERMISSIONS` | `owner: {create, read, update, delete, manage_members}` · **`admin: {create, read, update, delete}`** — no `manage_members`. **Member management is owner-only.** |
| **B** | `datanika/ui/state/settings_state.py:144, 229, 263, 292` | `_check_role("admin")` on invite, change-role, remove-member and cancel-invitation. **Member management is admin-and-up.** |
| **C** | `datanika/services/user_service.py:376-394, 347-364, 319-345` | No actor at all. `change_role`, `remove_member` and `add_member` take no caller identity and compare nothing. **Member management is unauthenticated at the service layer**; the only gate is whichever UI handler happened to call it. |

**A has tests and no callers.** `AuthService.has_permission` (`auth.py:121`) is referenced from exactly
two files, both test files, and one of them is a *security* test:
`tests/test_security/test_auth_security.py:117` asserts
`has_permission("admin", "manage_members") is False`. So we ship a green security test asserting a rule
the product does not enforce, because the function it tests is never called in production.

> This is the **seventh** instance of *"machinery exists, entry point does not"* — after [core#623],
> email verification, `send_quota_warning_email_task`, `client_ip.py` ([core#670]), `secure_input.py`
> on the auth pages ([core#672]) and `PaddleClient.cancel_subscription`. It is also the first instance
> where the orphaned machinery is a **security control with a passing test**, which is a worse shape
> than a dead feature: the test is evidence for a claim that is false.

The same split shows up on org rename: `settings_state.update_org:103` gates on `owner`, while the
service beneath it (`user_service.py:298-306`) accepts **owner or admin**. So "the only owner-exclusive
power is renaming the org" — which is what I wrote in the audit — is true only *through the UI*. It is
not a property of the system.

### 1.1 A second escalation path, not in [core#658]'s body

[core#658] describes self-promotion via the member-row role dropdown. There is a shorter one:

**An admin can invite an arbitrary email address directly as `owner`.**
`datanika/ui/pages/settings.py:278` renders the invite role select as
`["owner", "admin", "editor", "viewer"]`; `settings_state.add_member_by_email:144` gates on `admin`;
neither `user_service.add_member` nor `invitation_service.py:98` (which builds the `Membership` from
`invitation.role` on accept) compares the granted role to anything.

So the attacker does not need to promote *themselves* — they invite an address they already control,
as owner, and the second owner arrives with no interaction from the real owner at all. That also arms
the removal step, because `_check_last_owner` (`user_service.py:504-513`) permits 2 → 1.

**Membership is constructed in six places** (`git grep -n "Membership(" origin/dev`), of which two
take a caller-controlled role: `user_service.py:342` (`add_member`) and `invitation_service.py:98`
(invite accept). SSO JIT provisioning hardcodes `VIEWER` (`sso_routes.py:307`) and is not a path.

There is **no REST or MCP surface** for membership, so the Reflex settings page is the whole attack
surface. That is what makes this cheap.

> ⚠️ **How that last claim was nearly wrong, recorded because it is the same trap this spec is
> about.** My first check was `git grep` over **`datanika/api/`** — a directory that **does not
> exist**. `git ls-tree -d origin/dev:datanika/` returns `data, dbt_projects, i18n, migrations,
> models, scripts, services, tasks, ui`; the REST layer lives in
> **`datanika/services/api_v1_routes.py`**. So the grep returned nothing **because the path was
> wrong**, not because the surface was absent, and it would have read identically either way.
>
> Re-derived properly — `git grep -liE "membership|change_role|remove_member"` across *every* route
> module — the hits are `email_routes.py` (invite accept), `invitation_service.py`, `sso_routes.py`
> (JIT viewer) and `user_service.py`. **`api_v1_routes.py` is not among them**, and neither is the
> MCP package. The conclusion survives; the evidence for it did not.
>
> This is AUDIT_LAUNCH_READINESS.md (`plans/product/notes/AUDIT_LAUNCH_READINESS.md (local only)`) §7 trap 1 — *"I twice concluded X
> does not exist from a grep of one surface"* — walked into while writing the spec that cites it.
> **A grep over a path you have not confirmed exists is not evidence of absence.** `git ls-tree`
> first, or derive the file list rather than typing it.

---

## 2. Decision — the model of record

Five rules. Each exists because of one named harm; none is there for symmetry.

### R1 · `owner` is a distinct authority, not the top of the role ladder

**`owner` is removed from every role-assignment control** — the member-row dropdown
(`settings.py:188`) and the invite-role select (`settings.py:278`) both become
`["admin", "editor", "viewer"]`. `change_role` and `add_member` **refuse `MemberRole.OWNER`
unconditionally**, from any caller.

Ownership is reached only through a **dedicated operation** (§3), owner-only, separately confirmed and
separately audited.

> **This is the root-cause fix, and it is why the spec is not just "add a check".** Today, promoting to
> owner and re-roling a viewer are *the same click on the same control*. Any guard on that control is
> one predicate away from being wrong. Removing `owner` from the control removes the path, so a future
> bug in the predicate cannot reach ownership.

### R2 · Ceiling — no member may grant a role at or above their own

Owner grants `admin` / `editor` / `viewer`. Admin grants `editor` / `viewer`. Editor and viewer have no
member management at all.

**Admin cannot mint admin.** Deliberate, and this is the rule most likely to be argued with, so the
reason: `admin` carries `delete` on every resource in the org (`auth.py:8`). Who may destroy a
customer's pipelines is a decision the owner should make, not one that self-replicates. At 5 seats on
Pro and 10 on Enterprise, "the owner promotes admins" costs one click a quarter.

### R3 · Reach — you may only administer members strictly below you

Remove, change-role and cancel-invitation apply only to members whose role is **strictly below** the
actor's. An admin cannot remove another admin, and cannot touch an owner.

### R4 · Owner-on-owner is the one peer exception, and it is deliberate

An owner **may** remove or demote another owner (subject to R5). Rationale: two co-founders separating
must not need us. R1+R2 make this safe — a second owner exists only because an owner deliberately
created one, so this is not reachable by escalation, only by an owner's own earlier decision.

> Rejected alternative: *no owner may ever act on another owner.* It closes nothing that R1+R2 leave
> open, and it manufactures exactly the state this whole audit exists to eliminate — one only we can
> resolve, with no support tooling to resolve it.

### R5 · An org always has at least one owner. Every count-reducing path checks it

The guard exists and works today (`_check_last_owner`) on remove and demote. It is **extended, not
replaced**, to cover every operation that can reduce the active-owner count to zero:

| path | today | required |
|---|---|---|
| remove member | ✅ guarded | keep |
| demote owner | ✅ guarded | keep |
| **leave org (R6)** | does not exist | must check |
| **transfer ownership (§3)** | does not exist | N/A — it preserves the count |
| **delete account ([core#655])** | does not exist | **must check** |
| **delete org ([core#655])** | does not exist | N/A — it removes the org |

**The last two lines are the [core#655] consistency clause.** Deleting your account is an
owner-count-reducing operation. A sole owner must transfer or delete the org first; [core#655] must
refuse the deletion otherwise, with copy that names the two exits. Shipping erasure without that check
produces ownerless orgs — [core#658]'s end state by a different route.

### R6 · Any member may leave; the last owner cannot, and that is the point

Anyone may remove their own membership, subject to R5. This closes audit item **P8** (a viewer or
editor is currently in an org until someone else removes them — no `leave_*` handler exists anywhere).

The last owner's only exits are **transfer then leave**, or **delete the org**. That is a normal,
explainable constraint — GitHub, Stripe and Paddle all behave this way — **but only if both exits
actually exist.** Today neither does, and that, not the guard, is the bug.

### What this does not change

Self-service on your own row stays: leaving is R6; **raising your own role is R2-blocked**, which is
the [core#658] fix stated once rather than as a special case.

---

## 3. Ownership transfer — the new operation

**Owner-only.** *Settings → Organization → Transfer ownership.*

1. Successor is chosen from **existing active members** — not an email field, not a pending invitation.
   Inviting a stranger straight into ownership is exactly §1.1.
2. Confirmation dialog names the consequence in full: *"<name> becomes the owner. You become an admin.
   This cannot be undone by you."* Typed confirmation is **not** required — this is reversible by the
   new owner, unlike deletion.
3. On confirm, atomically: successor → `owner`, actor → `admin`. **Owner count is preserved
   throughout**, so R5 is never transiently violated.
4. Audited as **one** row, not as two `change_role` events. The row is
   `action="update"`, `resource_type="member"`, `resource_id=<successor>`, carrying both
   `owner_user_id` values — which is what answers *"who handed this org to that account"*.

   > 🚨 **This clause used to read *"audited as its own action (`transfer_ownership`)"*, and that
   > wording is what caused [core#1127].** `AuditAction` has six members and
   > `transfer_ownership` is not one of them, so `BaseState._audit`'s `AuditAction(action)`
   > raised and its deliberate swallow dropped the row. **The implementation was faithful to
   > this spec; the spec was unsatisfiable** — for the life of the product the
   > highest-privilege action in it wrote no audit row, and every check was green.
   >
   > ⚠️ **Adding the member is not the repair.** `audit_logs.action` is
   > `Enum(AuditAction, native_enum=False)`, so under blue/green the *previously deployed*
   > container raises `LookupError` when it **reads** a value it does not know — and
   > `/audit-logs` lists rows for a whole org, so one such row breaks the page for every
   > reader on the old colour mid-swap. A new verb is an expand/contract pair (teach the
   > enum to read it in release N, start writing it in N+1), which is a two-release price
   > for a synonym. `erase_user` set the precedent this now follows: record the fact inside
   > the existing enum.
   >
   > 🔑 **The general lesson, which is worth more than the string:** a spec clause naming a
   > value from a closed vocabulary must be checked against that vocabulary, or it becomes
   > an instruction to write a defect. `tests/test_services/test_audit_call_site_vocabulary.py`
   > now fails on the next one at authoring time.
   > *(Corrected 2026-09-07 by Engineering, [core#1127]. Recorded rather than silently
   > rewritten, because the wording is the cause and deleting it hides that.)*

**No acceptance handshake.** Decided, with a reason that will expire: an acceptance flow needs a
reliable email round trip, and [core#652] establishes that email notification channels have never
dispatched. Re-open this when [core#652] and email verification are both live; the successor is
already a member of an org they chose to join, so the exposure is a role change, not a data grant.

**Multi-owner stays possible** — an owner may promote a second owner through this same operation
("Add owner"), it is simply not reachable from the role dropdown. Bus-factor is a real need; the
defect was never that two owners can exist, it was that an admin could become the second one.

---

## 4. Consequences, per operation — the table Engineering implements against

Actor role → what they may do to a target. **Service layer enforces all of it**; the UI mirrors it.

| operation | viewer | editor | admin | owner |
|---|---|---|---|---|
| invite / add member as `viewer`/`editor` | ✗ | ✗ | ✓ | ✓ |
| invite / add member as `admin` | ✗ | ✗ | **✗** (R2) | ✓ |
| invite / add member as `owner` | ✗ | ✗ | **✗** (R1) | **✗** (R1 — use transfer) |
| change role of viewer/editor | ✗ | ✗ | ✓ | ✓ |
| change role of admin | ✗ | ✗ | **✗** (R3) | ✓ |
| change role of owner | ✗ | ✗ | **✗** (R3) | ✓ (R4, subject to R5) |
| remove viewer/editor | ✗ | ✗ | ✓ | ✓ |
| remove admin | ✗ | ✗ | **✗** (R3) | ✓ |
| remove owner | ✗ | ✗ | **✗** (R3) | ✓ (R4, subject to R5) |
| raise own role | ✗ | ✗ | **✗** (R2) | n/a |
| leave org | ✓ | ✓ | ✓ | ✓ if another owner remains (R5) |
| transfer ownership | ✗ | ✗ | ✗ | ✓ |
| rename org / change slug | ✗ | ✗ | **✗** | ✓ |

The last row resolves the §1 split deliberately: the **UI's** answer (`owner`) is the one we keep, and
`user_service.update_org` is tightened to match. An org's identity is not an admin's to change —
`organizations.slug` is unique, appears in SSO URLs, and per [SPEC_PII_SEPARATION.md](SPEC_PII_SEPARATION.md)
D4 is about to stop being derived from a person's name.

### Where enforcement lives

**In `UserService`, not in the Reflex state.** Every one of `change_role`, `remove_member`,
`add_member` and the new `leave_org` / `transfer_ownership` takes an **`actor_user_id`** and resolves
the actor's membership itself. The UI check stays as a second layer for the error message, but it is
not the control.

Reason, from the audit's own trap list: a capability's absence must be checked on *every* surface. There
is no REST or MCP membership surface today — but `ROLE_PERMISSIONS` is proof of what happens to a rule
that lives one layer above the thing it protects. **A rule enforced only in the UI is a rule that the
next surface will not have.**

`ROLE_PERMISSIONS` is then either wired to the real checks or deleted. It must not stay as a third
answer with a passing test. Recommended: keep it, derive the checks from it, and let
`test_auth_security.py:117` finally mean something.

### UI honesty (audit **P8**, second half)

`member_row` (`settings.py:182-204`) renders the role select and the Remove button for **every** member
regardless of the viewer's role or the target's. The server checks are real, so this is a presentation
bug, not a hole — but under *"do everything right before launch"* a control that exists only to be
refused is a defect. Render per §4: hide what the actor may not do, and show the actor's own row with
**Leave** instead of **Remove**.

---

## 5. Acceptance criteria

Written so a plausible half-implementation fails. Each must be **red before the fix** — a test that has
never failed has never been shown to be able to.

**Escalation is closed at the service layer, with no UI in the loop:**

1. `change_role(actor=admin, target=self, new_role=OWNER)` raises. Also with `new_role=ADMIN`.
2. `add_member(actor=admin, role=OWNER)` raises. `add_member(actor=admin, role=ADMIN)` raises.
3. Accepting an invitation whose stored `role` is `owner` **does not** create an owner membership —
   assert on the `Membership` row, not on the API response. (This is §1.1's second path; a fix that
   only changes the select leaves it open.)
4. `change_role(actor=admin, target=admin_b, ...)` and `remove_member(actor=admin, target=owner)` raise.
5. **The negative control**: `change_role(actor=admin, target=viewer, new_role=EDITOR)` still succeeds.
   A fix that denies everything passes 1-4.

**Owner count is an invariant, not a check on two paths:**

6. Sole owner: `leave_org` raises; `change_role(self → admin)` raises; `remove_member(self)` raises.
7. Sole owner: **account deletion raises** ([core#655]'s method, by whatever name it ships). If
   [core#655] merges first, this test is added to *its* suite and referenced here.
8. Two owners: each of remove / demote / leave succeeds and leaves exactly one owner.
9. **After every operation in this suite, assert `count(owner memberships where deleted_at is null) >= 1`
   for the org.** A shared assertion helper, called from every test — not nine hand-written copies,
   which is how [core#651]'s two lists diverged.

**Transfer:**

10. `transfer_ownership(actor=owner, successor=editor)` → successor is `owner`, actor is `admin`,
    owner count is 1 throughout (assert inside the transaction, not only after).
11. `transfer_ownership(actor=admin, ...)` raises.
12. Successor must be an existing active member — a pending invitation and a non-member both raise.

**Surfaces:**

13. `grep` proof, as a test: **no code path outside `transfer_ownership` constructs or assigns
    `MemberRole.OWNER`**, except the two account-creation paths (`user_service.py:268` signup default
    org, `:496` OAuth default org) which are explicitly allowlisted by name. Derive this from
    `Membership(` construction sites rather than asserting a hardcoded count — the count is what rots.
14. UI: as a `viewer`, the members table renders **no** role select and **no** Remove button on other
    rows, and **Leave** on their own.

**i18n:** every new string in all 9 locales — `settings.transfer_ownership`, its confirmation body,
`settings.leave_org`, and the refusal messages for R2/R3/R5. Refusals are user-visible copy here, not
dynamic error text: they are fixed sentences with a known cause, so they get keys.

---

## 6. Ship order

1. **R1 + R2 + R3 + actor-aware service methods.** This is [core#658] closed, and it is safe to ship
   alone — it only *removes* permissions, so nothing that works today for a legitimate owner breaks.
2. **R6 leave + UI honesty.** Closes audit P8.
3. **Transfer ownership.** Must land **before or with** [core#655]'s account deletion, because R5's
   deletion check is unimplementable-in-good-faith without an exit: refusing a sole owner's erasure
   request with no route out is worse than the current state, not better.
4. `ROLE_PERMISSIONS` wired or deleted.

⚠️ **Nothing here is a destructive migration.** No column changes, no data backfill. Steps 1-2 are
pure logic, so the expand/contract policy does not bind. Existing orgs with two owners keep them.

---

## 7. Filed by this spec

- Audit **P8** (member cannot leave; dishonest member-row controls) is folded into [core#658] as R6 +
  §4, rather than filed separately — same code path, same tests, and a separate issue would produce a
  second PR touching the same four functions.

[core#651]: https://github.com/datanika-io/datanika-core/issues/651
[core#652]: https://github.com/datanika-io/datanika-core/issues/652
[core#655]: https://github.com/datanika-io/datanika-core/issues/655
[core#658]: https://github.com/datanika-io/datanika-core/issues/658
[core#670]: https://github.com/datanika-io/datanika-core/issues/670
[core#672]: https://github.com/datanika-io/datanika-core/issues/672
[core#623]: https://github.com/datanika-io/datanika-core/issues/623
