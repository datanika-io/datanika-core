"""core#1287 — an unhandled request exception must leave a traceback.

## What was measured, and what the issue got wrong

``reflex.utils.exec.run_granian_backend_prod`` hardcodes ``--log-level critical`` and
ignores the ``loglevel`` it is handed, so granian — the thing that surfaces an
unhandled ASGI exception — discards the traceback before it reaches stdout. That part
of the issue reproduces exactly against the installed Reflex.

🔴 **Two of the issue's claims do NOT survive measurement, and both narrow the fix:**

1. *"A 500 in production leaves no traceback anywhere."* The **Reflex event-handler**
   path was never silent: ``reflex.app.default_backend_exception_handler`` calls
   ``console.error("[Reflex Backend Exception]\\n" + traceback.format_exc())`` and
   ``reflex.utils.console._LOG_LEVEL`` defaults to ``INFO``, so ``error()`` prints.
   An incident responder chasing a failed **Run button** wants that string; one
   chasing a failed ``POST /api/v1/...`` wants the line this middleware emits.
2. The ``/api/v1`` REST tree was **not** blind either. ``api_middleware.api_endpoint``
   already wraps every decorated handler in ``except Exception: logger.exception("API
   handler error")`` (``api_middleware.py``), as do ``email_routes``' two handlers.

**What is genuinely uncovered is every backend route that is NOT ``api_endpoint``-
decorated** — the OAuth AS routes that the whole remote-MCP flow runs through, SSO /
SAML, ``/mcp``, ``/metrics``, the agent-doc routes, and the cloud plugin's webhook and
``/api/admin/e2e/*`` handlers, which is where core#1269's 500 actually lived. This
middleware covers that surface in one place instead of asking every future route
author to remember.

## Why the controls are shaped the way they are

`docs/QA_RULES.md` §3 — a red proves the test failed, never *why*. The permissive
control here is the one that matters: a middleware that logged **every** request, or
logged handled ``HTTPException``s, would satisfy a bare "something was logged"
assertion while burying the signal it exists to produce.
"""

import logging

import pytest
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from datanika.logging_config import JSONFormatter
from datanika.services.error_logging import ExceptionLoggingMiddleware

LOGGER_NAME = "datanika.unhandled"


class BoomError(RuntimeError):
    """A distinctive type, so the assertions cannot pass on somebody else's error."""


async def _raises(request):
    raise BoomError("the database went away mid-request")


async def _ok(request):
    return JSONResponse({"ok": True})


async def _http_error(request):
    raise HTTPException(status_code=403, detail="nope")


def _app(*, with_middleware: bool = True) -> Starlette:
    """Build the app the way ``datanika/datanika.py`` does.

    ``add_middleware`` — not ``Starlette(middleware=[...])`` — because that is the
    production install shape, and it is what decides where the middleware lands in
    the stack relative to ``ExceptionMiddleware`` and ``ServerErrorMiddleware``.
    A harness that constructs the stack differently is testing a different object.
    """
    app = Starlette(
        routes=[
            Route("/boom", _raises),
            Route("/ok", _ok),
            Route("/forbidden", _http_error),
        ]
    )
    if with_middleware:
        app.add_middleware(ExceptionLoggingMiddleware)
    return app


def _records(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == LOGGER_NAME]


class TestTheTracebackReachesTheLog:
    def test_an_exception_escaping_the_router_is_logged_with_exc_info(self, caplog):
        caplog.set_level(logging.ERROR, logger=LOGGER_NAME)
        client = TestClient(_app(), raise_server_exceptions=False)

        assert client.get("/boom").status_code == 500

        records = _records(caplog)
        assert len(records) == 1, f"expected exactly one record, got {len(records)}"
        record = records[0]
        assert record.levelno == logging.ERROR
        assert record.exc_info is not None, (
            "the record carries no exc_info, so JSONFormatter has no traceback to "
            "render and the log line names the path without naming the failure — "
            "which is the defect core#1287 is about, one layer up."
        )
        assert record.exc_info[0] is BoomError
        assert "/boom" in record.getMessage()

    def test_the_json_formatter_renders_the_failing_frame(self, caplog):
        """The real consumer is the formatter, not the record.

        `QA_RULES` §16 — asserting on ``exc_info`` is asserting on the pipeline's own
        report. What reaches ``docker logs`` is whatever ``JSONFormatter`` emits, so
        that is what has to contain the frame.
        """
        caplog.set_level(logging.ERROR, logger=LOGGER_NAME)
        client = TestClient(_app(), raise_server_exceptions=False)
        client.get("/boom")

        line = JSONFormatter().format(_records(caplog)[0])

        assert '"level": "ERROR"' in line
        assert "BoomError" in line, "the exception TYPE is missing from the emitted line"
        assert "_raises" in line, (
            "the failing FRAME is missing. Without it the line says a 500 happened "
            "and not where, which is the count-without-identity shape again."
        )
        assert "the database went away mid-request" in line

    def test_without_the_middleware_nothing_logs_it(self, caplog):
        """The control that ATTRIBUTES the red (`QA_RULES` §3).

        Without it, *"a record appeared"* is equally explained by Starlette,
        ``ServerErrorMiddleware``, or pytest's logging plugin having logged it — and
        the test above would stay green after this middleware was deleted.
        """
        caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
        client = TestClient(_app(with_middleware=False), raise_server_exceptions=False)

        assert client.get("/boom").status_code == 500
        assert _records(caplog) == [], (
            "something other than ExceptionLoggingMiddleware is logging this, so the "
            "assertion above proves nothing about the middleware."
        )

    def test_the_500_still_happens_and_the_body_is_unchanged(self):
        """It observes on the way past. Swallowing would turn a 500 into a hang."""
        with_mw = TestClient(_app(), raise_server_exceptions=False).get("/boom")
        without = TestClient(_app(with_middleware=False), raise_server_exceptions=False).get(
            "/boom"
        )
        assert with_mw.status_code == without.status_code == 500
        assert with_mw.text == without.text


class TestItDoesNotLogWhatIsAlreadyHandled:
    """The permissive controls (`QA_RULES` §3).

    A middleware that logged everything would pass the class above and be useless:
    an ERROR per 403 buries the one line an incident needs.
    """

    def test_a_successful_request_logs_nothing(self, caplog):
        caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
        assert TestClient(_app()).get("/ok").status_code == 200
        assert _records(caplog) == []

    def test_a_handled_http_exception_is_not_logged_as_unhandled(self, caplog):
        caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
        resp = TestClient(_app(), raise_server_exceptions=False).get("/forbidden")
        assert resp.status_code == 403
        assert _records(caplog) == [], (
            "a 403 is an answer, not a failure. Starlette's ExceptionMiddleware "
            "converts it BELOW this middleware, so seeing one here means the "
            "middleware was installed at the wrong depth."
        )

    def test_a_missing_route_is_not_logged_as_unhandled(self, caplog):
        caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
        resp = TestClient(_app(), raise_server_exceptions=False).get("/no-such-path")
        assert resp.status_code == 404
        assert _records(caplog) == []

    def test_a_cancelled_request_is_not_logged_as_an_error(self, caplog):
        """A client disconnect arrives as ``CancelledError``, a ``BaseException``.

        Catching ``BaseException`` would file every disconnect as an application
        error — and at that point the log is noise and the next real traceback is
        invisible inside it. This pins the choice of ``except Exception``.
        """
        import anyio

        async def _cancelled(request):
            raise anyio.get_cancelled_exc_class()()

        app = Starlette(routes=[Route("/gone", _cancelled)])
        app.add_middleware(ExceptionLoggingMiddleware)

        caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
        with pytest.raises(BaseException):  # noqa: B017 - the type is the point, not the message
            TestClient(app).get("/gone")
        assert _records(caplog) == []


class TestTheMiddlewareIsWiredIntoTheAppThatServes:
    """`QA_RULES` §1 — a correct middleware nobody installs is not a fix.

    Asserted against the source rather than by importing ``datanika.datanika``,
    which has heavy top-level side effects (the same reason
    ``tests/test_app_plugin_init.py`` reads the file). That is a weaker instrument
    than driving the object, so the anti-vacuity test below is not optional.
    """

    @staticmethod
    def _source() -> str:
        from pathlib import Path

        import datanika

        return (Path(datanika.__file__).parent / "datanika.py").read_text(encoding="utf-8")

    def test_datanika_py_installs_it_on_the_backend_app(self):
        src = self._source()
        assert "ExceptionLoggingMiddleware" in src, (
            "datanika/datanika.py does not install ExceptionLoggingMiddleware. Every "
            "backend route that is not api_endpoint-decorated — the OAuth AS, SSO, "
            "/mcp, the cloud webhook — then returns 500 with nothing in docker logs."
        )
        assert "app._api.add_middleware(ExceptionLoggingMiddleware)" in src

    def test_it_is_installed_after_prometheus_so_it_wraps_it(self):
        """Order is load-bearing in both directions.

        ``add_middleware`` inserts at position 0 and the stack is built
        outermost-first, so the LAST one added is the OUTERMOST. Adding this one
        after ``PrometheusMiddleware`` leaves Prometheus's ``.app`` chain — which it
        walks at construction to find the route table — untouched, and puts an
        exception raised by the metrics middleware itself inside our try.
        """
        src = self._source()
        prom = src.index("app._api.add_middleware(PrometheusMiddleware)")
        ours = src.index("app._api.add_middleware(ExceptionLoggingMiddleware)")
        assert prom < ours, (
            "ExceptionLoggingMiddleware is installed BEFORE PrometheusMiddleware, so "
            "it is the inner of the two: a failure inside the metrics middleware is "
            "then unlogged, and Prometheus's route-table walk sees a different chain."
        )

    def test_the_source_matcher_is_not_inert(self):
        """Anti-vacuity: a source grep that cannot fail is a comment.

        (`QA_RULES` §2 / coordinator rule 26 — prove the instrument can SEE, not only
        that it can fail.)
        """
        assert "app._api.add_middleware(ExceptionLoggingMiddleware)" not in (
            "app._api.add_middleware(PrometheusMiddleware)"
        )
        assert "ExceptionLoggingMiddleware" in self._source()


class TestTheBoundaryWithApiEndpoint:
    """Pin what is already covered, so the fix is not sold as wider than it is.

    ``api_endpoint`` catches and returns a 500 **response**; nothing propagates, so
    this middleware adds no second line for that surface. If that decorator ever
    stops catching, the middleware picks it up — which is the point of having both.
    """

    def test_a_handler_that_returns_500_without_raising_is_not_logged_here(self, caplog):
        async def _returns_500(request):
            return JSONResponse({"error": "Internal server error"}, status_code=500)

        app = Starlette(routes=[Route("/handled", _returns_500)])
        app.add_middleware(ExceptionLoggingMiddleware)

        caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
        assert TestClient(app).get("/handled").status_code == 500
        assert _records(caplog) == [], (
            "a 500 RESPONSE is not an unhandled exception. Logging it here would "
            "double-log every api_endpoint failure, which already calls "
            "logger.exception('API handler error')."
        )

    def test_api_endpoint_still_logs_its_own_failures(self):
        """The claim above is about the decorator, so read the decorator."""
        from pathlib import Path

        import datanika

        src = (Path(datanika.__file__).parent / "services/api_middleware.py").read_text(
            encoding="utf-8"
        )
        assert 'logger.exception("API handler error")' in src, (
            "api_middleware no longer logs its own handler failures. The middleware "
            "in this module does NOT cover that path — nothing propagates out of a "
            "decorator that returns a response — so this is now a silent surface."
        )
