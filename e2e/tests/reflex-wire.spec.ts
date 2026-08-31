import { test, expect } from "../fixtures/auth";
import { gotoReady } from "../fixtures/auth";
import { clickAndAwaitReflexEvent, watchReflexWire } from "../fixtures/reflex-wire";

/**
 * Regression tests for the harness's own readiness and delivery gates (core#744).
 *
 * ── Why a spec, and not just "we checked once" ───────────────────────────────
 * The gate these protect is `gotoReady`, and its defect was that it COULD NOT
 * FAIL: it waited for the `/_event` socket to go quiet for 600ms and then
 * returned successfully whatever it had seen, so a socket that never opened
 * became a silent 12-second sleep and a socket that died satisfied "quiet"
 * sooner than a healthy one. Nothing in the suite would have noticed if someone
 * reintroduced that shape, because every spec that depends on it would simply go
 * back to flaking ~50% and being called "the known race".
 *
 * So each assertion added in that fix gets a test that forces it RED, and each
 * one asserts on the MESSAGE. A test that only checked "it threw" would be
 * satisfied by any unrelated timeout — which is the same defect one level up.
 *
 * ── The mechanism, verified before it was relied on ──────────────────────────
 * `plans/qa/notes/probe-744/forcing-mechanism.mjs` establishes against the real
 * app that `page.routeWebSocket` is transparent to `page.on("websocket")`
 * (routed pass-through reports byte-identical counters to an unrouted load:
 * 1 socket / 3 sent / 4 received / 1 hydration), that a route which never calls
 * `connectToServer()` yields zero observed sockets, and that a route can swallow
 * exactly one outbound frame while the page hydrates normally. Without that
 * third property the two gates could not be tested independently, and a red in
 * the delivery test could just have meant "the browser was broken".
 *
 * ── Tier: INFORMATIONAL ──────────────────────────────────────────────────────
 * New spec. Under §5 of the QA operating manual, specs graduate INTO the gating
 * tier on three consecutive greens on `dev`; they do not enter it. That applies
 * to a spec written by the person who also wrote the code it guards, today,
 * exactly as much as to anyone else's.
 */
test.describe("Reflex wire gates: readiness and delivery @informational", () => {
  test.setTimeout(120_000);

  test("readiness fails BY NAME when Reflex never opens an /_event socket", async ({ page }) => {
    // A route that never calls connectToServer() black-holes the socket: the
    // page still loads and renders, which is precisely the state the old gate
    // reported as ready.
    await page.routeWebSocket(/\/_event/, () => {});

    await expect(
      gotoReady(page, "/login"),
      "gotoReady must reject, and must say the socket is what is missing — " +
        "the whole defect was that it returned success here",
    ).rejects.toThrow(/never opened an \/_event WebSocket/);

    // Distinguish "the socket was black-holed" from "the app was unreachable".
    // Both make gotoReady reject; only the first is what this test is about, and
    // asserting on the message above already refuses the second. This line says
    // so out loud, because on the very first execution of this spec the
    // rejection did NOT match — most plausibly a still-warming app — and an
    // unexplained flip in a negative control is worth making self-diagnosing.
    expect(page.url(), "the page itself must have loaded").toContain("/login");
  });

  test("readiness fails BY NAME when the socket opens but never hydrates", async ({ page }) => {
    // Swallow the client's `hydrate` event. The socket connects, the server is
    // real and healthy, and it simply never has cause to send
    // `is_hydrated_rx_state_":true` back. The result is an OPEN, SILENT socket —
    // the exact state the old 600ms-quiet heuristic scored highest, because
    // silence was its success condition.
    //
    // ⚠️ The obvious control — forward client->server and drop everything
    // server->client — does NOT work, and finding that out is why this comment
    // exists. Under `routeWebSocket`, `page.on("websocket")` reports the REAL
    // network socket that `connectToServer()` opens, not the page's mocked one.
    // So the observer sits behind the route and still sees the server's
    // hydration frame even when the page never receives it: readiness passes and
    // the test silently proves nothing. Starve the request, not the response.
    await page.routeWebSocket(/\/_event/, (ws) => {
      const server = ws.connectToServer();
      ws.onMessage((message) => {
        const text = typeof message === "string" ? message : message.toString("utf8");
        if (text.includes("state.hydrate")) return;
        server.send(message);
      });
      server.onMessage((message) => ws.send(message));
    });

    await expect(
      gotoReady(page, "/login"),
      "an open-but-silent socket is the case the old 600ms-quiet heuristic " +
        "scored HIGHEST, because silence was its success condition",
    ).rejects.toThrow(/never reported the page hydrated/);

    expect(page.url(), "the page itself must have loaded").toContain("/login");
  });

  test("delivery fails BY NAME when the event frame never reaches the wire", async ({ page }) => {
    // Forward everything except the one event under test. The page hydrates
    // normally, so the readiness gate passes and this test isolates delivery.
    let swallowed = 0;
    await page.routeWebSocket(/\/_event/, (ws) => {
      const server = ws.connectToServer();
      ws.onMessage((message) => {
        const text = typeof message === "string" ? message : message.toString("utf8");
        if (text.includes("auth_state.login")) {
          swallowed += 1;
          return;
        }
        server.send(message);
      });
      server.onMessage((message) => ws.send(message));
    });

    await gotoReady(page, "/login");
    await page.getByLabel(/email/i).fill("wire-gate-negative-control@datanika.test");
    await page.getByLabel(/password/i).fill("not-a-real-password");

    await expect(
      clickAndAwaitReflexEvent(page, page.getByRole("button", { name: /log in|sign in/i }), {
        handlerMatch: "auth_state.login",
        what: "the login button (negative control)",
      }),
      "the click lands on the element and resolves; only the wire knows it went nowhere",
    ).rejects.toThrow(/no \/_event frame carrying "auth_state\.login" was sent/);

    expect(swallowed, "the route must actually have swallowed the frame").toBeGreaterThan(0);
  });

  test("delivery SUCCEEDS on a healthy socket (positive control)", async ({ page }) => {
    // Without this, the two reds above are satisfied by an assertion that always
    // throws — which is a gate that blocks everything and proves nothing.
    await gotoReady(page, "/login");
    const wire = watchReflexWire(page);
    const sentBefore = wire.framesSent;

    await page.getByLabel(/email/i).fill("wire-gate-positive-control@datanika.test");
    await page.getByLabel(/password/i).fill("not-a-real-password");

    await clickAndAwaitReflexEvent(
      page,
      page.getByRole("button", { name: /log in|sign in/i }),
      { handlerMatch: "auth_state.login", what: "the login button (positive control)" },
    );

    expect(
      wire.framesSent,
      "delivery returned, so at least one frame must have gone out",
    ).toBeGreaterThan(sentBefore);
    expect(wire.hydrations, "readiness returned, so the page must have hydrated").toBeGreaterThan(0);
  });
});
