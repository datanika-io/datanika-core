import type { Page, WebSocket } from "@playwright/test";

/**
 * Read the Reflex `/_event` WebSocket directly, so "the app is ready" and "the
 * click was delivered" are things the test OBSERVES rather than things it hopes.
 *
 * ── Why this exists (core#744 / core#792) ────────────────────────────────────
 * `golden-path` flaked ~50% on `dev` and held the promotion. When it failed the
 * upload row existed in `DRAFT`, no `runs` row was ever written, `run_upload`
 * was never dispatched, and the Celery worker was healthy and consuming other
 * tasks throughout. Two rounds of triage went to the worker, because the
 * assertion text said "Is the Celery worker running?".
 *
 * The traces from run 33380618991 say what actually happened. Both the attempt
 * and its retry:
 *
 *   attempt   26.47s goto /uploads · 28.34s click Run · 28.40s goto /runs   (60ms)
 *   retry     19.14s goto /uploads · 21.22s click Run · 21.25s goto /runs   (30ms)
 *
 * and in both, `Disconnect websocket on unload` is logged ~10ms after that
 * navigation starts. That console line is emitted by Reflex's `disconnectTrigger`
 * ONLY when `socket.current?.connected` — so the socket was live, and the
 * navigation tore it down 30-60ms after the click.
 *
 * Reflex sends events asynchronously (`addEvents` → `queueEvents` →
 * `processEvent` → `applyEvent` → `socket.emit`), and `processEvent` opens with
 * its own comment: "Only proceed if the socket is up or no event in the queue
 * uses state, otherwise we throw the event into the void". Every way that race
 * can be lost — queued while reconnecting, or emitted into a buffer that the
 * unload handler's `socket.disconnect()` then discards — has one signature: no
 * frame on the wire, no run row, no toast, no error. Which is what was measured.
 *
 * A user does not navigate 30ms after clicking. The harness did.
 *
 * ── The rule this module enforces ────────────────────────────────────────────
 * Never leave a page until the event it was supposed to send is ON THE WIRE, and
 * never treat silence as readiness. `gotoReady` used to wait for the socket to go
 * QUIET for 600ms and then return successfully no matter what it had seen — a
 * closed socket and a socket that never opened are the two quietest things there
 * are, so its success condition was satisfied *best* by the failure it existed to
 * prevent. It could not fail; it degraded to a 12-second sleep and said nothing.
 */

/** Reflex's event socket. The configured transport is `websocket`, so there is no polling leg. */
const EVENT_PATH = "/_event";

/**
 * Reflex's own "this page is live" marker, observed on the wire.
 *
 * Measured against the running app (`plans/qa/notes/probe-744/wire-format.mjs`),
 * the server's closing hydration frame is exactly:
 *
 *   42/_event,["event",{"delta":{"reflex___state____state":
 *     {"is_hydrated_rx_state_":true}},"events":[],"final":true}]
 *
 * This is a stronger readiness signal than "a frame arrived": the engine.io open
 * packet (`0{"sid":...}`) and the namespace ack (`40/_event,{...}`) both arrive
 * on any socket that merely connects, and neither says the backend state machine
 * answered. `is_hydrated_rx_state_":true` is the framework's own statement that
 * this page's state is mounted and its handlers are attached.
 *
 * If Reflex ever renames it, this gate fails CLOSED — every spec goes red at the
 * readiness step with a message naming this constant. That is the right polarity
 * and is the whole reason it is not spelled as a loose "some frame arrived".
 */
const HYDRATED_MARKER = '"is_hydrated_rx_state_":true';

/** How many recent sent-frame payloads to keep for delivery assertions. */
const SENT_LOG_LIMIT = 40;

/**
 * 🚨 `sentLog` holds RAW client→server payloads, and those carry secrets.
 *
 * Measured: the login submit frame is
 *
 *   42/_event,["event",{"name":"...auth_state.login","payload":{"form_data":
 *     {"email":"...","password":"<cleartext>", ...}}, ...}]
 *
 * So the buffer legitimately contains user passwords, and on a signup flow it
 * contains the ones this suite creates. NOTHING in this module may print a
 * payload: `describeWire` emits counts only, and the delivery assertion reports
 * how many frames went out, never what was in them. CI logs and the
 * `playwright-report` artifact are both readable by anyone with repo access.
 *
 * If you are tempted to add "…and here are the frames we saw" to a failure
 * message to make it easier to debug — don't. Log the COUNT and the matched
 * handler NAME, which is all any of these assertions needs.
 */

export type ReflexWire = {
  /** `/_event` sockets opened on this page since instrumentation. */
  socketsOpened: number;
  /** Sockets currently open. */
  open: Set<WebSocket>;
  framesSent: number;
  framesReceived: number;
  /** Server frames carrying `is_hydrated_rx_state_":true` — Reflex saying "page live". */
  hydrations: number;
  /** ms epoch of the last frame in either direction; 0 if none. */
  lastActivityAt: number;
  /** ms epoch of the last SERVER to client frame; 0 if none. */
  lastServerFrameAt: number;
  socketsClosed: number;
  errors: string[];
  /** Ring buffer of recent client to server payloads. */
  sentLog: string[];
};

const wires = new WeakMap<Page, ReflexWire>();

/**
 * Frame payloads are text on this wire (verified in probe-744: socket.io emits
 * `42/_event,[...]`). `String()` rather than `Buffer.toString("utf8")` on purpose
 * — it is identical for a Buffer and keeps this file free of an ambient `Buffer`
 * type, which `e2e/` has no `@types/node` for.
 */
function payloadOf(frame: { payload: unknown }): string {
  return String(frame.payload);
}

/**
 * Instrument `page` once. Idempotent.
 *
 * Idempotence is not tidiness here: the previous implementation called
 * `page.on("websocket", ...)` inside `gotoReady`, which `runUploadAndAwait` calls
 * once per poll — 31 times in the trace above. Every call added a listener that
 * was never removed, and each one re-registered frame handlers on every later
 * socket.
 */
export function watchReflexWire(page: Page): ReflexWire {
  const existing = wires.get(page);
  if (existing) return existing;

  const wire: ReflexWire = {
    socketsOpened: 0,
    open: new Set(),
    framesSent: 0,
    framesReceived: 0,
    hydrations: 0,
    lastActivityAt: 0,
    lastServerFrameAt: 0,
    socketsClosed: 0,
    errors: [],
    sentLog: [],
  };
  wires.set(page, wire);

  page.on("websocket", (ws) => {
    if (!ws.url().includes(EVENT_PATH)) return;
    wire.socketsOpened += 1;
    wire.open.add(ws);
    wire.lastActivityAt = Date.now();

    ws.on("framesent", (frame) => {
      wire.framesSent += 1;
      wire.lastActivityAt = Date.now();
      wire.sentLog.push(payloadOf(frame));
      if (wire.sentLog.length > SENT_LOG_LIMIT) wire.sentLog.shift();
    });
    ws.on("framereceived", (frame) => {
      wire.framesReceived += 1;
      wire.lastActivityAt = Date.now();
      wire.lastServerFrameAt = Date.now();
      if (payloadOf(frame).includes(HYDRATED_MARKER)) wire.hydrations += 1;
    });
    ws.on("socketerror", (err) => wire.errors.push(String(err)));
    ws.on("close", () => {
      wire.open.delete(ws);
      wire.socketsClosed += 1;
    });
  });

  return wire;
}

/**
 * One-line evidence block. Every failure this module raises carries it, so a red
 * names its own layer instead of handing the reader a bare timeout.
 */
export function describeWire(wire: ReflexWire): string {
  const idle = wire.lastActivityAt === 0 ? "never" : `${Date.now() - wire.lastActivityAt}ms ago`;
  return (
    `[/_event sockets opened=${wire.socketsOpened} open=${wire.open.size} ` +
    `closed=${wire.socketsClosed} framesSent=${wire.framesSent} ` +
    `framesReceived=${wire.framesReceived} hydrations=${wire.hydrations} ` +
    `lastActivity=${idle}` +
    (wire.errors.length ? ` socketErrors=${JSON.stringify(wire.errors.slice(-3))}` : "") +
    "]"
  );
}

export type ReadyMark = { socketsOpened: number; hydrations: number };

/** Counters to measure the NEXT navigation against. Take this before `page.goto`. */
export function markWire(wire: ReflexWire): ReadyMark {
  return { socketsOpened: wire.socketsOpened, hydrations: wire.hydrations };
}

/**
 * Block until this page has a live, answering Reflex socket — or throw saying
 * which of those was missing.
 *
 * Three positive conditions, each read off the wire:
 *   1. a NEW `/_event` socket opened for this navigation (measured as a delta,
 *      not as a leftover count from an earlier page);
 *   2. at least one SERVER to client frame arrived on it, which is what
 *      distinguishes "socket constructed" from "handshake completed and the
 *      backend answered";
 *   3. it is still open when we return, because a socket that opened, answered
 *      and then died satisfies every "went quiet" heuristic perfectly.
 *
 * The 600ms quiet window is kept — it is a reasonable proxy for "hydration burst
 * over, handlers attached" and costs nothing — but it is no longer the success
 * condition, and running out of it is no longer silent. If quiet is never reached
 * AND the socket count kept climbing, that is a reconnect loop and it throws; a
 * single stable socket that is merely chatty is fine and proceeds.
 */
export async function awaitReflexReady(
  page: Page,
  mark: ReadyMark,
  what: string,
  opts: { timeoutMs?: number; quietMs?: number } = {},
): Promise<void> {
  const wire = watchReflexWire(page);
  // 30s, not the 12s the old spin used, and deliberately generous. The old gate
  // returned successfully on expiry, so a slow page merely proceeded; this one
  // fails, and the ONE way this change could turn a pass into a false red is a
  // page that hydrates later than the budget while the later `expect`s (15s,
  // auto-retrying) would still have caught up. A wide budget costs nothing on a
  // healthy run and removes that case; the spec itself allows 300s.
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const quietMs = opts.quietMs ?? 600;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline && wire.socketsOpened <= mark.socketsOpened) {
    await page.waitForTimeout(50);
  }
  if (wire.socketsOpened <= mark.socketsOpened) {
    throw new Error(
      `Reflex never opened an /_event WebSocket for ${what} within ${timeoutMs}ms. ` +
        "Nothing this page does can reach the server: Reflex discards stateful " +
        "events while the socket is down (state.js processEvent — 'otherwise we " +
        `throw the event into the void'). ${describeWire(wire)}`,
    );
  }

  while (Date.now() < deadline && wire.hydrations <= mark.hydrations) {
    await page.waitForTimeout(50);
  }
  if (wire.hydrations <= mark.hydrations) {
    throw new Error(
      `The /_event socket for ${what} opened but Reflex never reported the page ` +
        `hydrated within ${timeoutMs}ms (no server frame carrying ` +
        `${HYDRATED_MARKER}). The socket connecting is not the same as the backend ` +
        "state machine answering — an engine.io open packet and a namespace ack " +
        "arrive on any socket that merely connects. Until hydration lands, no " +
        `handler is attached and a click goes nowhere. ${describeWire(wire)}`,
    );
  }

  const socketsAtQuietStart = wire.socketsOpened;
  while (Date.now() < deadline && Date.now() - wire.lastActivityAt < quietMs) {
    await page.waitForTimeout(50);
  }
  if (Date.now() - wire.lastActivityAt < quietMs && wire.socketsOpened > socketsAtQuietStart) {
    throw new Error(
      `The /_event socket for ${what} kept reconnecting for the whole ${timeoutMs}ms ` +
        `budget (${wire.socketsOpened - socketsAtQuietStart} further sockets opened). ` +
        `Events queued during a reconnect are discarded on unload. ${describeWire(wire)}`,
    );
  }

  if (wire.open.size === 0) {
    throw new Error(
      `The /_event socket for ${what} closed before the page was usable. A closed ` +
        "socket is the quietest thing on the wire, which is exactly why the old " +
        `"wait for quiet" gate returned success here. ${describeWire(wire)}`,
    );
  }
}

/**
 * Click something that fires a Reflex event handler, and do not return until the
 * event is on the wire.
 *
 * This is the fix for core#744's actual mechanism. `locator.click()` resolving
 * proves only that Playwright found and pressed a DOM node; the Reflex event it
 * triggers is emitted several microtasks later, and anything that tears the page
 * down in between — `page.goto`, a nav click, a reload — takes the unsent frame
 * with it, silently.
 *
 * `handlerMatch` is matched against the raw socket.io payload, which carries the
 * fully-qualified handler name (e.g. `...upload_state.run_upload`). Matching on
 * the method name is specific enough and survives state-module renames.
 */
export async function clickAndAwaitReflexEvent(
  page: Page,
  locator: { click: (options?: { timeout?: number }) => Promise<void> },
  opts: { handlerMatch: string; what: string; clickTimeoutMs?: number; timeoutMs?: number },
): Promise<void> {
  const wire = watchReflexWire(page);
  const timeoutMs = opts.timeoutMs ?? 15_000;
  const matchedBefore = wire.sentLog.filter((p) => p.includes(opts.handlerMatch)).length;
  const framesSentBefore = wire.framesSent;

  await locator.click({ timeout: opts.clickTimeoutMs ?? 15_000 });

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (wire.sentLog.filter((p) => p.includes(opts.handlerMatch)).length > matchedBefore) return;
    await page.waitForTimeout(50);
  }

  const others = wire.framesSent - framesSentBefore;
  throw new Error(
    `${opts.what}: the click landed on the element, but no /_event frame carrying ` +
      `"${opts.handlerMatch}" was sent within ${timeoutMs}ms (${others} other frame(s) ` +
      "went out in that window). The browser never delivered the event, so the server " +
      "did nothing and said nothing — no row, no toast, no error. This is NOT the " +
      "Celery worker: no task was ever enqueued, because the handler never ran. Look " +
      "at the client — a socket that was down or reconnecting at click time (Reflex " +
      "discards those events), or a control whose handler is not wired. " +
      `${describeWire(wire)}`,
  );
}
