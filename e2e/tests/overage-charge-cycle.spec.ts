import { test, expect } from "@playwright/test";

/**
 * V2 P5 Option B — overage charge cycle E2E @slow
 *
 * Reference: plans/qa/SPEC_OVERAGE_BILLING_TESTS.md §5 + §7 gate 4
 * Paired with cloud#47 (emission unit tests) and core#264 (reception tests).
 *
 * The happy-path cycle:
 *   1. Seed tenant on V2 Pro plan (100 GB included, overage price set).
 *   2. Seed usage_ledger totalling 105 GB within the current billing period.
 *   3. Fast-forward clock to T-23h before cycle end.
 *   4. Trigger the `emit_charge_incoming_notices` Celery task.
 *   5. Assert: in-app Notification(type=CHARGE_INCOMING) visible in the
 *      notifications drawer; projected amount = 500 cents.
 *   6. Fast-forward to cycle end (T+1min).
 *   7. Trigger `charge_cycle_overages`.
 *   8. Assert: Paddle sandbox records a subscription-charge; Charge row
 *      status=issued; paddle_transaction_id populated (txn_…).
 *   9. Re-run the task — idempotent. Paddle is not called again; no second
 *      Charge row is issued (unique idempotency_key barrier).
 *
 * Client-side prerequisites (all three must be true or the suite skips):
 *   - DATANIKA_E2E_OVERAGE_CHARGE=1 — explicit opt-in; unsafe by default
 *   - PADDLE_SANDBOX_VENDOR_ID + PADDLE_SANDBOX_API_KEY — sandbox creds
 *   - Test-only admin endpoints live (see list below)
 *
 * Server-side prerequisites (staging app env — not skip gates, but the
 * charge task no-ops / errors without them):
 *   - DATANIKA_OVERAGE_CHARGE_ENABLE=1 — else `charge_cycle_overages` returns
 *     {..., "disabled": true} and issues nothing (kill-switch, cloud#68)
 *   - PADDLE_OVERAGE_PRODUCT_ID set — else PaddleClient.charge_subscription
 *     raises ValueError (core#273)
 *
 * Test-only admin endpoints required (Engineering V2 P5 — see core#361):
 *   POST /api/admin/e2e/seed-overage-tenant
 *     body: { planSlug, includedGB, overagePriceCents, usageGB }
 *     returns: { orgId, subscriptionId, billingPeriodStart, billingPeriodEnd, authToken }
 *   POST /api/admin/e2e/advance-clock
 *     body: { toIso: string }  // fast-forward system time for scheduler-relative logic
 *   POST /api/admin/e2e/run-task
 *     body: { taskName: 'emit_charge_incoming_notices' | 'charge_cycle_overages' }
 *     returns: { taskName, result }  // result = task's native return (int for
 *              notices; { issued, skipped_idempotent, failed } for the charge loop)
 *   GET  /api/admin/e2e/charges?subscriptionId=<id>
 *     returns: Array<{ id, status, amountCents, paddleTransactionId }>
 *
 * All are gated behind DATANIKA_ENV !== 'production' guards. Endpoints
 * tracked in core#361; this spec reconciled to the shipped cloud interface
 * in core#362.
 *
 * This spec is red-tests-first: the @slow tag + env gate keep it out of
 * PR CI; when Engineering ships the endpoints (core#361), flip the
 * FAST_FORWARD_NOT_READY constant to false and the assertions start
 * pulling real data.
 */

const GATE =
  process.env.DATANIKA_E2E_OVERAGE_CHARGE !== "1" ||
  !process.env.PADDLE_SANDBOX_VENDOR_ID ||
  !process.env.PADDLE_SANDBOX_API_KEY;

const BASE_URL = process.env.DATANIKA_E2E_BASE_URL ?? "https://staging-app.datanika.io";

// Armed: the test-only admin endpoints shipped (cloud#72). The tests still run
// only where the GATE env is set (DATANIKA_E2E_OVERAGE_CHARGE=1 + Paddle sandbox
// creds) — i.e. the staging overage run, never PR CI. See core#374 / core#267.
const FAST_FORWARD_NOT_READY = false;

// Native return of the `charge_cycle_overages` Celery task (cloud dev:
// datanika_cloud/billing/tasks.py). `disabled` appears only when the
// DATANIKA_OVERAGE_CHARGE_ENABLE kill-switch is off.
type ChargeSummary = {
  issued: number;
  skipped_idempotent: number;
  failed: number;
  disabled?: boolean;
};

test.describe("V2 P5 overage charge cycle @slow", () => {
  test.skip(
    () => GATE,
    "Requires DATANIKA_E2E_OVERAGE_CHARGE=1 + Paddle sandbox creds",
  );

  test.skip(
    () => FAST_FORWARD_NOT_READY,
    "Engineering V2 P5 admin endpoints not shipped yet — see core#361",
  );

  test("cycle: seed → T-23h notify → T+0 charge → retry no-op", async ({
    request,
  }) => {
    // Step 1 — seed overage tenant
    const seedRes = await request.post(`${BASE_URL}/api/admin/e2e/seed-overage-tenant`, {
      data: {
        planSlug: "pro",
        includedGB: 100,
        overagePriceCents: 100, // $1.00 / GB
        usageGB: 105,
      },
    });
    expect(seedRes.ok()).toBeTruthy();
    const seed = (await seedRes.json()) as {
      orgId: number;
      subscriptionId: number;
      billingPeriodStart: string;
      billingPeriodEnd: string;
      authToken: string;
    };

    const authHeaders = { Authorization: `Bearer ${seed.authToken}` };

    // Step 3 — fast-forward to T-23h
    const tMinus23h = new Date(
      new Date(seed.billingPeriodEnd).getTime() - 23 * 3600 * 1000,
    ).toISOString();
    const adv1 = await request.post(`${BASE_URL}/api/admin/e2e/advance-clock`, {
      data: { toIso: tMinus23h },
    });
    expect(adv1.ok()).toBeTruthy();

    // Step 4 — fire the notice task
    const warn = await request.post(`${BASE_URL}/api/admin/e2e/run-task`, {
      data: { taskName: "emit_charge_incoming_notices" },
    });
    expect(warn.ok()).toBeTruthy();

    // Step 5 — assert the CHARGE_INCOMING in-app notification landed.
    // seed.authToken is a full-access API key (cloud#72 seeds it via
    // ApiKeyService with scopes=None), NOT a session JWT — so read the notice
    // through the REST API with a Bearer header rather than hydrating the
    // /notifications UI with a session cookie. GET /api/v1/notifications is
    // @api_endpoint(required_scope="notifications:read"); a null-scope key
    // satisfies any scope, and the list is scoped to the key's org + user =
    // the seeded overage tenant.
    const notifRes = await request.get(`${BASE_URL}/api/v1/notifications`, {
      headers: authHeaders,
    });
    expect(notifRes.ok()).toBeTruthy();
    const notifBody = (await notifRes.json()) as {
      items: Array<{ type: string; title: string; message: string }>;
      unread_count: number;
    };
    const incoming = notifBody.items.find((n) => n.type === "charge_incoming");
    expect(incoming, "expected a charge_incoming notification").toBeTruthy();
    // Projected overage = 5 GB × $1.00 = $5.00
    // (title: "Upcoming overage charge: $5.00").
    expect(`${incoming!.title} ${incoming!.message}`).toContain("$5.00");

    // Step 6 — fast-forward to cycle end
    const cycleEnd = new Date(
      new Date(seed.billingPeriodEnd).getTime() + 60 * 1000,
    ).toISOString();
    const adv2 = await request.post(`${BASE_URL}/api/admin/e2e/advance-clock`, {
      data: { toIso: cycleEnd },
    });
    expect(adv2.ok()).toBeTruthy();

    // Step 7 — run the charge loop
    const settle1 = await request.post(`${BASE_URL}/api/admin/e2e/run-task`, {
      data: { taskName: "charge_cycle_overages" },
    });
    expect(settle1.ok()).toBeTruthy();
    const settle1Body = (await settle1.json()) as {
      taskName: string;
      result: ChargeSummary;
    };
    // Exactly one charge issued this cycle; kill-switch must be on (no `disabled`).
    expect(settle1Body.result.disabled).toBeFalsy();
    expect(settle1Body.result.issued).toBe(1);

    // Step 8 — verify Charge row via admin list endpoint
    const listRes = await request.get(
      `${BASE_URL}/api/admin/e2e/charges?subscriptionId=${seed.subscriptionId}`,
      { headers: authHeaders },
    );
    expect(listRes.ok()).toBeTruthy();
    const charges = (await listRes.json()) as Array<{
      id: number;
      status: string;
      amountCents: number;
      paddleTransactionId: string | null;
    }>;
    expect(charges).toHaveLength(1);
    // Post-issue status is ISSUED (Paddle accepted). It flips to PAID only on
    // the transaction.completed webhook, which is out of scope for this cycle.
    expect(charges[0].status).toBe("issued");
    expect(charges[0].amountCents).toBe(500);
    expect(charges[0].paddleTransactionId).toMatch(/^txn_/); // Paddle transaction id

    // Step 9 — idempotency: re-run the charge loop
    const settle2 = await request.post(`${BASE_URL}/api/admin/e2e/run-task`, {
      data: { taskName: "charge_cycle_overages" },
    });
    expect(settle2.ok()).toBeTruthy();
    const settle2Body = (await settle2.json()) as {
      taskName: string;
      result: ChargeSummary;
    };
    // The unique idempotency_key barrier means no second charge is issued.
    // (Whether the row is de-selected or hits the guard is Eng's call — the
    // invariant that matters is issued === 0, asserted robustly here.)
    expect(settle2Body.result.issued).toBe(0);

    // Charge row count unchanged.
    const listRes2 = await request.get(
      `${BASE_URL}/api/admin/e2e/charges?subscriptionId=${seed.subscriptionId}`,
      { headers: authHeaders },
    );
    const charges2 = (await listRes2.json()) as Array<unknown>;
    expect(charges2).toHaveLength(1);
  });

  test("Paddle 4xx response marks Charge failed with reason", async ({ request }) => {
    // Scenario: Paddle sandbox returns a 4xx on the POST /subscriptions/{id}/charge
    // request (pre-acceptance rejection). After CHARGE_MAX_RETRIES the Charge goes
    // to status=failed with `last_error` set to the Paddle body.
    //
    // NB: this is the pre-acceptance path. A *post-acceptance* collection decline
    // is different — the charge stays ISSUED while Paddle runs dunning (see
    // docs/billing-failed-payment-policy.md §Grace period). Don't conflate them.
    //
    // The grace-period policy is now specified (cloud#48), but exercising this
    // needs a sandbox subscription/product that forces a 4xx + a decline-simulation
    // harness. Kept skipped until that harness lands.
    test.skip(true, "Needs sandbox 4xx-forcing harness — see core#361 / SPEC §4.4");
  });

  test("no charge when usage under included", async ({ request }) => {
    // Sanity gate: 80 GB on a 100 GB plan is UNDER the allotment, so cycle
    // close must issue nothing — catches a regression where the charge loop
    // fires on any usage, overage or not. (The describe-level GATE skip keeps
    // this out of PR CI; it runs only in the staging overage run.)
    const seedRes = await request.post(`${BASE_URL}/api/admin/e2e/seed-overage-tenant`, {
      data: { planSlug: "pro", includedGB: 100, overagePriceCents: 100, usageGB: 80 },
    });
    expect(seedRes.ok()).toBeTruthy();
    const seed = (await seedRes.json()) as {
      subscriptionId: number;
      billingPeriodEnd: string;
      authToken: string;
    };

    // Fast-forward past cycle end and run the charge loop.
    const cycleEnd = new Date(
      new Date(seed.billingPeriodEnd).getTime() + 60 * 1000,
    ).toISOString();
    const adv = await request.post(`${BASE_URL}/api/admin/e2e/advance-clock`, {
      data: { toIso: cycleEnd },
    });
    expect(adv.ok()).toBeTruthy();

    const settle = await request.post(`${BASE_URL}/api/admin/e2e/run-task`, {
      data: { taskName: "charge_cycle_overages" },
    });
    expect(settle.ok()).toBeTruthy();
    const body = (await settle.json()) as { result: ChargeSummary };
    // Under the allotment ⇒ nothing issued; kill-switch on (no `disabled`).
    expect(body.result.disabled).toBeFalsy();
    expect(body.result.issued).toBe(0);

    // And no Charge row exists for this subscription.
    const listRes = await request.get(
      `${BASE_URL}/api/admin/e2e/charges?subscriptionId=${seed.subscriptionId}`,
      { headers: { Authorization: `Bearer ${seed.authToken}` } },
    );
    expect(listRes.ok()).toBeTruthy();
    const charges = (await listRes.json()) as Array<unknown>;
    expect(charges).toHaveLength(0);
  });
});