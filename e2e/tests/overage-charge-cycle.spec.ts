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
 *   4. Trigger the `emit_charge_incoming_warnings` Celery task.
 *   5. Assert: in-app Notification(type=CHARGE_INCOMING) visible in the
 *      notifications drawer; projected amount = 500 cents.
 *   6. Fast-forward to cycle end (T+1min).
 *   7. Trigger `settle_overage_charges`.
 *   8. Assert: Paddle sandbox records a subscription-charge; Charge row
 *      status=succeeded; paddle_charge_id populated.
 *   9. Re-run the task — idempotent. Paddle is not called again; Charge
 *      row unchanged.
 *
 * Prerequisites (all three must be true or the suite skips):
 *   - DATANIKA_E2E_OVERAGE_CHARGE=1 — explicit opt-in; unsafe by default
 *   - PADDLE_SANDBOX_VENDOR_ID + PADDLE_SANDBOX_API_KEY — sandbox creds
 *   - Test-only admin endpoints live (see list below)
 *
 * Test-only admin endpoints required (Engineering V2 P5):
 *   POST /api/admin/e2e/seed-overage-tenant
 *     body: { planSlug, includedGB, overagePriceCents, usageGB }
 *     returns: { orgId, subscriptionId, billingPeriodStart, billingPeriodEnd, authToken }
 *   POST /api/admin/e2e/advance-clock
 *     body: { toIso: string }  // fast-forward system time for scheduler-relative logic
 *   POST /api/admin/e2e/run-task
 *     body: { taskName: 'emit_charge_incoming_warnings' | 'settle_overage_charges' }
 *     returns: { status, logs }
 *
 * All three are gated behind DATANIKA_ENV !== 'production' guards. See
 * core#249 for Engineering's admin-endpoint ship task.
 *
 * This spec is red-tests-first: the @slow tag + env gate keep it out of
 * PR CI; when Engineering ships the endpoints + charge loop, flip the
 * FAST_FORWARD_NOT_READY constant to false and the assertions start
 * pulling real data.
 */

const GATE =
  process.env.DATANIKA_E2E_OVERAGE_CHARGE !== "1" ||
  !process.env.PADDLE_SANDBOX_VENDOR_ID ||
  !process.env.PADDLE_SANDBOX_API_KEY;

const BASE_URL = process.env.DATANIKA_E2E_BASE_URL ?? "https://staging-app.datanika.io";

// Flip to false when Engineering ships the test-only admin endpoints +
// charge loop. At that point every test below should turn green.
const FAST_FORWARD_NOT_READY = true;

test.describe("V2 P5 overage charge cycle @slow", () => {
  test.skip(
    () => GATE,
    "Requires DATANIKA_E2E_OVERAGE_CHARGE=1 + Paddle sandbox creds",
  );

  test.skip(
    () => FAST_FORWARD_NOT_READY,
    "Engineering V2 P5 admin endpoints not shipped yet — see core#249",
  );

  test("cycle: seed → T-23h notify → T+0 charge → retry no-op", async ({
    request,
    page,
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

    // Step 4 — fire the warning task
    const warn = await request.post(`${BASE_URL}/api/admin/e2e/run-task`, {
      data: { taskName: "emit_charge_incoming_warnings" },
    });
    expect(warn.ok()).toBeTruthy();

    // Step 5 — assert in-app notification visible
    await page.context().addCookies([
      {
        name: "session_token",
        value: seed.authToken,
        url: BASE_URL,
      },
    ]);
    await page.goto(`${BASE_URL}/notifications`);
    const card = page.getByRole("article", { name: /overage charge/i }).first();
    await expect(card).toBeVisible();
    await expect(card).toContainText(/\$5\.00/); // 5 GB * $1.00
    await expect(card).toContainText(/Pro/);

    // Step 6 — fast-forward to cycle end
    const cycleEnd = new Date(
      new Date(seed.billingPeriodEnd).getTime() + 60 * 1000,
    ).toISOString();
    const adv2 = await request.post(`${BASE_URL}/api/admin/e2e/advance-clock`, {
      data: { toIso: cycleEnd },
    });
    expect(adv2.ok()).toBeTruthy();

    // Step 7 — settle the charge
    const settle1 = await request.post(`${BASE_URL}/api/admin/e2e/run-task`, {
      data: { taskName: "settle_overage_charges" },
    });
    expect(settle1.ok()).toBeTruthy();
    const settle1Body = (await settle1.json()) as { chargeId: number; paddleChargeId: string };

    expect(settle1Body.paddleChargeId).toMatch(/^cha_/); // Paddle charge IDs prefixed with cha_

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
      paddleChargeId: string | null;
    }>;
    expect(charges).toHaveLength(1);
    expect(charges[0].status).toBe("succeeded");
    expect(charges[0].amountCents).toBe(500);
    expect(charges[0].paddleChargeId).toBe(settle1Body.paddleChargeId);

    // Step 9 — idempotency: re-run the settle task
    const settle2 = await request.post(`${BASE_URL}/api/admin/e2e/run-task`, {
      data: { taskName: "settle_overage_charges" },
    });
    expect(settle2.ok()).toBeTruthy();
    const settle2Body = (await settle2.json()) as {
      status: string;
      skippedReason?: string;
    };
    // Engineering may report "noop" via either (a) 200 + skippedReason
    // or (b) an idempotency-specific status. Accept either.
    expect(settle2Body.status === "noop" || settle2Body.skippedReason).toBeTruthy();

    // Charge row count unchanged.
    const listRes2 = await request.get(
      `${BASE_URL}/api/admin/e2e/charges?subscriptionId=${seed.subscriptionId}`,
      { headers: authHeaders },
    );
    const charges2 = (await listRes2.json()) as Array<unknown>;
    expect(charges2).toHaveLength(1);
  });

  test("Paddle 4xx response marks Charge failed with reason", async ({ request }) => {
    // Scenario: seed a tenant whose Paddle sandbox subscription has been
    // manually set to "card-declines-always". Settle the overage. Assert
    // Charge.status=failed, Charge.failure_reason matches the Paddle body.
    //
    // Gated on Engineering ship gate 3 (failed-payment policy) per
    // SPEC_OVERAGE_BILLING_TESTS.md §4.4 + §10.5. When the policy lands,
    // seed the decline-always subscription in the sandbox and uncomment
    // this scenario.
    test.skip(true, "Engineering ship gate 3 — failed-payment policy not yet specified");
  });

  test("no charge when usage under included", async ({ request }) => {
    // Scenario: seed 80 GB usage on a 100 GB plan. Run settle at cycle
    // close. Assert: no Paddle call, no Charge row.
    //
    // This is the "sanity" gate — catches a regression where the charge
    // loop fires on any usage, overage or not.
    test.skip(
      FAST_FORWARD_NOT_READY,
      "Engineering V2 P5 not shipped — see core#249",
    );
  });
});
