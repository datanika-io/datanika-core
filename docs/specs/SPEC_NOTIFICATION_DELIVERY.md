# SPEC — Notification delivery must be observable

**Issue:** [core#652](https://github.com/datanika-io/datanika-core/issues/652) ·
**Owner:** Product (contract) → Engineering (implementation) ·
**Status:** Spec only, no code · **Written:** 2026-09-02 against `origin/dev` @ `969fcb0`

> 🚨 **Read §1 before §3.** #652 is titled *"`notify()` is never passed an `email_service`"*, and a
> fix that passes one is **not** a fix. There are six independent places where this path loses
> information, and wiring the argument closes exactly one of them. The other five produce the same
> user-visible silence with more code in it.

---

## §0. What the product promises, and what it does

*"Tell me when my 3 a.m. run fails"* is the promise a scheduler exists to keep. Settings → Alerting
accepts an email address, stores it, and renders a green **On** badge next to it. **No email has ever
been sent from that surface, on any org, in any edition, whether or not SMTP is configured.**

The badge is the part that makes this a Product issue rather than a bug report. `settings.py:835`
renders `rx.cond(ch.is_active, rx.badge("On", color_scheme="green"), rx.badge("Off"))` — a green
affirmative next to a channel type that has never dispatched. `email` sits in the type dropdown
(`settings.py:852`) on equal footing with `slack`, `telegram` and `webhook`. Nothing on the page
distinguishes the one that cannot work.

**A green badge is an assertion.** This is the same family as a published per-seat price for a seat
nobody can buy: the product is telling the user something is true, and it is not.

---

## §1. The six silences, measured

All line numbers against `origin/dev` @ `969fcb0`.

| # | where | what is lost | who it affects |
|---|---|---|---|
| 1 | `notification_service.py:110` — `notify(..., *, email_service=None)`, and **all three** call sites omit it (`:311`, `quota_notification_hooks.py:107`, `charge_notification_hooks.py:67`) | `_dispatch_email` returns at its first guard. Nothing is ever attempted. | email only |
| 2 | `notification_service.py:139` — `logger.warning("Email service disabled; …")` | Names a cause that is **not** the one that fired. Prod has a working Resend relay. Sends the reader to check SMTP config. | email only |
| 3 | `notification_service.py:117-125` — `try: self._dispatch(...) except Exception: logger.exception(...)` | Every failure below this line is swallowed. Wiring an `EmailService` with `raise_on_error=True` still ends here. | **all four channel types** |
| 4 | `notification_service.py:154` — `email_service.send(to, subject, body)`, return value discarded | `send()` returns `False` on failure when `raise_on_error` is off, which is the constructor default. | email only |
| 5 | `_dispatch_slack`/`_dispatch_telegram`/`_dispatch_webhook` — `httpx.post(...)` with **zero** `raise_for_status()` in the module | A 401 from Slack, a 404 webhook, a revoked Telegram token: all return a response object and the code proceeds. | **slack, telegram, webhook** |
| 6 | `models/notification_channel.py` — columns are `name`, `channel_type`, `config`, `events`, `is_active` | There is no delivery record of any kind. No `last_attempt_at`, no `last_status`, no `last_error`. Nothing the user or an operator can read. | **all four** |

### 🔑 The correction this forces to the issue text

#652 says *"Slack, Telegram and webhook on the same channel work correctly."* **That is true only of
the dispatch call.** Silences 3 and 5 mean those three channels are equally unobservable: they work
when everything is right, and are indistinguishable from working when it is not. Email is not a
broken channel among three healthy ones — it is the one that does not even reach the socket.

Fixing only email would leave three channels that fail silently, and would make the alerting feature
*look* fixed.

### What is already fixed, and must not be re-fixed

QA's earlier finding — *four email tasks declare `autoretry_for` that can never fire because
`EmailService.send` swallows everything* — **has shipped**. `EmailService.__init__` takes
`raise_on_error` ([core#700]) and all five tasks in `tasks/email_tasks.py` pass `raise_on_error=True`.
Do not re-derive this.

⚠️ Two residues of that fix are load-bearing here:

- `send()` returns `False` **before** the `try` when `not is_enabled()` (`email_service.py:56`), so
  `raise_on_error` cannot fire for unconfigured SMTP. Configuration failure and transport failure are
  different signals and must stay different.
- `send_quota_warning_email_task` carries `raise_on_error=True` and **no `autoretry_for`** — it
  raises and does not retry. It also has zero non-test callers, as does `send_email_task`.

---

## §2. Decisions (Product's call — implement, do not re-litigate)

**D1 — Notification email goes through the Celery task, not a synchronous `EmailService`.**
`_dispatch_email` runs inside the run-completion hook, which runs inside the worker's task. A
blocking SMTP round trip there delays the task that is reporting a *finished* run, and a transient
relay failure has no retry. Dispatch via `send_email_task.delay(...)`, which already carries
`autoretry_for` + backoff. The `email_service=` parameter then has no reason to exist and should be
deleted rather than wired — **a parameter no caller passes is not a seam, it is a defect that
compiles.**

**D2 — Every channel type records the outcome of every attempt.** Silence 6 is the one that changes
what the user can see, and it is what the other five reduce to. A durable row, not a log line and not
a counter: this project has repeatedly shipped metrics nothing scrapes, and a log line on a box the
user cannot read is not feedback. Minimum shape — `last_attempt_at`, `last_status`
(`success` | `failed` | `skipped`), `last_error` (truncated, never the payload).

⚠️ **`last_error` must never carry channel config.** A webhook URL and a Telegram bot token are
credentials, and an HTTP error body can echo the request.

**D3 — The Settings row shows delivery state, not just `is_active`.** `is_active` answers *"is this
channel switched on?"*; the user is asking *"is this channel working?"*. Those are different
questions and today one badge answers the wrong one. A channel that is On and has never delivered
must not render as an unqualified green.

**D4 — No silent `except Exception` may remain on the dispatch path.** Silence 3 may catch, but it
must record (D2) and it must re-raise or mark the channel. `_dispatch_slack` and friends get
`raise_for_status()`.

**D5 — The two dead tasks are decided here so nobody has to decide twice.** `send_email_task` stays
(it is the generic transport D1 needs). `send_quota_warning_email_task` is superseded — its templated
body duplicates `_build_quota_warning_email` (`notification_service.py:196`), and a second template
for one message is how the two start disagreeing. Delete it with the change that makes D1 real, and
give the surviving path `autoretry_for`.

**D6 — Out of scope for this spec:** a **Send test notification** button (it is the right feature and
it needs its own rate-limiting decision, exactly as [core#700] AC4 does), per-event templating,
digesting, and any new channel type.

---

## §3. Acceptance criteria

Demonstrated, not asserted from code. Each of 1, 2 and 5 must be **shown red against current `dev`**
before it goes green.

1. A `run.failed` hook fired for an org with an active email channel results in a **mail transport
   call** — the fake is the socket, not the mailer. ⚠️ *A test asserting `notify()` was called is not
   this test; that test exists today and passes.*
2. With SMTP unconfigured, the same path records `skipped` with a reason naming **SMTP configuration**
   — distinct from a transport failure and distinct from a missing channel.
3. All four event types that reach `notify()` — `run_success`, `run_failure`, `quota_warning`,
   `charge_incoming` — deliver to a configured email channel.
4. Slack, Telegram and webhook dispatch is unchanged **in the success path**, and a non-2xx response
   now records `failed` instead of being discarded.
5. A webhook channel pointed at a URL returning 500 shows as failed **in the UI**, without reading a
   log. This is the criterion that would have caught the original defect on the day it shipped.
6. A channel that has never successfully delivered does not render an unqualified green **On**.
7. `last_error` contains no value from `channel.config`, proven by a test that puts a distinctive
   token in a webhook URL and asserts it is absent from the stored error.
8. Nothing in this work logs, stores or returns an email address, bot token or webhook URL in a new
   place.

---

## §4. Adjacent, not in scope — recorded so it is not lost

- **`rx.badge("On")` / `rx.badge("Off")` (`settings.py:835`) are bare English literals.** Badge labels
  are on `WORKFLOW_RULES.md` §6's translate list, so this is a nine-locale i18n gap on a surface D3
  is about to change. Fold it into whoever does D3; it is three keys, not an issue of its own.
- **`_build_quota_warning_email` and `EmailService.send_quota_warning_email` are two templates for one
  message.** D5 resolves it. Noted here because the duplication is what makes D5 look like tidying.

[core#700]: https://github.com/datanika-io/datanika-core/issues/700
