import type { APIRequestContext, APIResponse } from "@playwright/test";

/**
 * Is the thing answering us the API, or the SPA wearing its clothes? (core#1209)
 *
 * A local stack without the one-origin proxy in front returns **200 with the Reflex SPA**
 * for `/api/v1/...`. Measured on a running stack, all three origins, same path:
 *
 * ```
 *   frontend :3000   status=200  content-type=text/html    <!DOCTYPE html>...
 *   backend  :8000   status=401  content-type=application/json
 *   proxy    :3100   status=401  content-type=application/json
 * ```
 *
 * 🚨 **The cross-tenant probes survive that; their own-resource CONTROL does not.**
 * `tenant-jwt-boundary.spec.ts` rejects any 2xx on a foreign resource, so the SPA's 200
 * fails it — loudly and correctly. But the control beside it asserted only
 * `expect(response.status()).toBe(200)`, and **HTML is a 200**. So a proxy-less run
 * reported *"isolation is broken and the endpoint is healthy"* — pointing the reader at
 * precisely the wrong conclusion, with a green control vouching for it.
 *
 * A vacuous pass on a tenant-isolation suite is the worst artifact this project can
 * produce, and the vacuity was in the part whose whole job was to prevent it.
 */

/** The one-request precondition: prove the origin in front of us serves the API. */
export async function assertApiOrigin(
  request: APIRequestContext,
  probePath = "/api/v1/connections/1",
): Promise<void> {
  const response = await request.get(probePath, { failOnStatusCode: false });
  const contentType = response.headers()["content-type"] ?? "";

  if (!contentType.includes("application/json")) {
    const body = (await response.text()).slice(0, 200);
    throw new Error(
      [
        `HARNESS: ${probePath} answered ${response.status()} with content-type ` +
          `"${contentType}" — this origin is not serving the API.`,
        "",
        "Almost certainly the one-origin proxy is not in front: a bare Reflex frontend",
        "returns 200 with the SPA for every unmatched path, including /api/v1/*.",
        "",
        "Every isolation assertion below is meaningless against HTML. Some would FAIL",
        "(a 200 is a forbidden outcome for a cross-tenant probe) and some would PASS",
        "(HTML does not contain org B's data, and HTML is a 200) — and the passing ones",
        "are the controls. Refusing here is the only reading that is not misleading.",
        "",
        `First bytes: ${body}`,
      ].join("\n"),
    );
  }
}

/**
 * Assert a response is a real API response and return its parsed body.
 *
 * Use this for every **own-resource** control. `status === 200` alone is satisfied by the
 * SPA, which is what made the control vacuous in the first place.
 */
export async function expectApiJson(response: APIResponse, label: string): Promise<unknown> {
  const contentType = response.headers()["content-type"] ?? "";
  const status = response.status();

  if (!contentType.includes("application/json")) {
    const body = (await response.text()).slice(0, 200);
    throw new Error(
      `HARNESS: ${label} returned ${status} with content-type "${contentType}", not JSON. ` +
        `A 200 from the SPA satisfies a status check and proves nothing. Body: ${body}`,
    );
  }
  try {
    return await response.json();
  } catch (error) {
    throw new Error(
      `HARNESS: ${label} claimed content-type JSON at status ${status} and did not parse: ` +
        `${String(error)}`,
    );
  }
}
