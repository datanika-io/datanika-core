"""Make an unhandled request exception leave a traceback (core#1287).

## What was measured

``reflex.utils.exec.run_granian_backend_prod`` builds the production server command
and hardcodes ``--log-level critical``, ignoring the ``loglevel`` it is handed::

    command = [
        "granian",
        *("--log-level", "critical"),
        ...
    ]

Granian is what surfaces an unhandled ASGI exception, so at ``critical`` the traceback
is discarded before it reaches stdout. Confirmed on a real container: a request that
returns **500** produces **zero** lines in ``docker logs``.

``PrometheusMiddleware`` still records ``http_requests_total{status="500"}``, so we
learn *that* a 500 happened and never *which* failure it was. That is the shape this
project keeps finding one level down — an instrument that records the event and
discards its identity — with the server doing it.

## What this covers, and what it does NOT

🔑 **This is the HTTP surface only: the Starlette routes on ``app._api``** — the
``/api/v1`` REST tree, the OAuth AS routes, the Paddle webhook, ``/mcp``, ``/metrics``.

⚠️ **A Reflex EVENT-HANDLER exception is a different path and was never silent.**
``reflex.app.default_backend_exception_handler`` calls
``console.error(f"[Reflex Backend Exception]\\n {traceback.format_exc()}")``, and
``reflex.utils.console``'s own ``_LOG_LEVEL`` defaults to ``INFO``, so ``error()``
prints. Measured in the installed Reflex, not inferred. An incident responder looking
for a failed Run button wants ``[Reflex Backend Exception]``; one looking for a failed
``POST /api/v1/...`` wants the line this module emits. **Saying "a 500 leaves no
traceback anywhere" sends half of them to the wrong grep.**

## Why a middleware and not a higher granian log level

Raising granian to ``info`` is not ours to set — it is Reflex's hardcoded call — and
it would also turn on per-request access logging on a box where
``http_requests_total`` already covers that. The cost would land on every request to
fix something that happens on very few (core#1287 AC2).

## Why it re-raises

Swallowing would turn a 500 into a hang or a blank 200 depending on how far the
response got. ``ServerErrorMiddleware`` sits above every user middleware and owns the
500 response; this one only observes on the way past.

``BaseException`` is deliberately not caught: a client disconnect arrives as
``asyncio.CancelledError``, which is a ``BaseException`` and is not an error.
"""

import logging

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger("datanika.unhandled")


class ExceptionLoggingMiddleware:
    """Log a traceback for any exception that escapes the router, then re-raise.

    Installed with ``app._api.add_middleware(ExceptionLoggingMiddleware)`` in
    ``datanika/datanika.py``, **after** ``PrometheusMiddleware`` so that this one is
    the outermost user middleware: ``add_middleware`` inserts at position 0 and the
    stack is built outermost-first, so the last one added wraps the rest. That keeps
    ``PrometheusMiddleware``'s ``.app`` chain — which it walks at build time to find
    the route table — exactly as it was, and means an exception raised *by* the
    metrics middleware is logged too.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await self.app(scope, receive, send)
        except Exception:
            # ``scope["path"]`` only. The query string is deliberately excluded: a
            # token or a one-time code can ride in it, and this line goes to stdout,
            # which is scraped and retained. The path is enough to name the route.
            logger.exception(
                "Unhandled exception serving %s %s",
                scope.get("method", scope.get("type", "?")),
                scope.get("path", "?"),
            )
            raise
