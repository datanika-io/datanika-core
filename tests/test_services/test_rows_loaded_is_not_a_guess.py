"""A row count we failed to read is not zero (core#1170 AC3, AC5).

``docs/specs/SPEC_EARNED_VERDICTS.md`` §4.2 and §4.4.

The defect
----------
``_extract_rows_loaded`` had three ``return 0`` paths — no trace, no normalize info, and a bare
``except Exception``. Any of them yielded **0 rows on a green run**, indistinguishable from a
genuinely empty load. The schema deliberately pays for that distinction and the code then erased
it: ``models/run.py:39`` is ``Mapped[int | None]``, ``nullable=True``, and its sibling
``bytes_processed`` carries a comment saying ``NULL`` means *"not measured"* and that writing ``0``
*"would erase that"*.

Why ``0`` vs ``None`` is not cosmetic here
------------------------------------------
``ui/state/model_state.py`` decides whether the catalog is empty *because nothing ran* or *because
something ran and produced nothing* by asking whether any successful run loaded rows. Fed a
coerced ``0`` for "we could not read the trace", that question is answered wrongly and the user is
shown the wrong empty state — a verdict derived from a number nobody measured.

🔑 **The discriminating test is the pair.** A suite that only asserts ``None`` on failure passes
against an extractor that returns ``None`` for *everything*, including a real empty load. Every
failure case below is matched by ``test_a_measured_zero_stays_zero``, which is the one that would
catch that.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from datanika.services.dlt_runner import _extract_rows_loaded


def _pipeline(trace):
    return SimpleNamespace(last_trace=trace)


def _trace(row_counts):
    return SimpleNamespace(last_normalize_info=SimpleNamespace(row_counts=row_counts))


class TestAnUnreadableCountIsNone:
    def test_no_trace_is_none(self, caplog):
        with caplog.at_level("WARNING"):
            assert _extract_rows_loaded(_pipeline(None)) is None
        assert caplog.records, "returned None and said nothing — the operator cannot act on silence"

    def test_no_normalize_info_is_none(self, caplog):
        with caplog.at_level("WARNING"):
            assert (
                _extract_rows_loaded(_pipeline(SimpleNamespace(last_normalize_info=None))) is None
            )
        assert caplog.records, "returned None and said nothing"

    def test_a_raising_trace_is_none(self, caplog):
        class Exploding:
            @property
            def last_trace(self):
                raise RuntimeError("dlt changed its trace shape")

        with caplog.at_level("WARNING"):
            assert _extract_rows_loaded(Exploding()) is None
        assert caplog.records, "swallowed an exception silently"
        assert any(
            "dlt changed its trace shape" in r.getMessage() or r.exc_info for r in caplog.records
        ), (
            "the log line names neither the exception nor carries a traceback, so the reason "
            "for the None is unrecoverable"
        )


class TestAMeasuredCountIsStillANumber:
    """🚨 The control. Without it every assertion above is satisfied by
    ``def _extract_rows_loaded(_): return None``."""

    def test_a_real_count_is_returned(self):
        assert _extract_rows_loaded(_pipeline(_trace({"users": 18, "orders": 5}))) == 23

    def test_dlt_internal_tables_are_still_excluded(self):
        assert _extract_rows_loaded(_pipeline(_trace({"users": 3, "_dlt_loads": 99}))) == 3

    def test_a_measured_zero_stays_zero(self):
        """The distinction the whole change exists for.

        An empty load is a **measured** zero and must stay ``0``. If this returns ``None`` the
        fix has replaced one conflation with its mirror image, and the schema's three states
        collapse again — just in the other direction.
        """
        assert _extract_rows_loaded(_pipeline(_trace({}))) == 0

    def test_a_table_of_only_dlt_internals_is_a_measured_zero(self):
        assert _extract_rows_loaded(_pipeline(_trace({"_dlt_pipeline_state": 1}))) == 0


def _resolved_loader_config(tmp_path):
    """dlt's ``LoaderConfiguration`` as the providers actually resolve it.

    ``LOAD_VOLUME_PATH`` must be present or resolution raises
    ``ConfigFieldMissingException`` on an unrelated field — an ERROR, which is the same colour
    as a failure and would prove nothing about ``raise_on_failed_jobs``.
    """
    import os

    from dlt.common.configuration.resolve import resolve_configuration
    from dlt.load.configuration import LoaderConfiguration

    os.environ.setdefault("LOAD_VOLUME_PATH", str(tmp_path))
    return resolve_configuration(LoaderConfiguration())


class TestTheDltDefaultOurRunStatusDependsOn:
    """core#1170 AC5 — **a guard, not a fix. Nothing is broken today.**

    ``execution_service.complete_run`` sets ``RunStatus.SUCCESS`` unconditionally. That is honest
    **only because** dlt raises when a load job fails: with ``raise_on_failed_jobs`` false,
    ``pipeline.run()`` returns a ``LoadInfo`` carrying failed jobs, we never see an exception,
    and we mark that run ``SUCCESS``. It defaults to ``True`` in dlt >= 1.0 and was ``False``
    before. Nothing in this tree said so until this test.

    ⚠️ Asserted as **resolved**, not as pinned — reading ``pyproject.toml`` would miss an env
    override, which is the way this changes without a diff.
    """

    def test_raise_on_failed_jobs_is_true_as_resolved(self, tmp_path):
        assert _resolved_loader_config(tmp_path).raise_on_failed_jobs is True, (
            "dlt's `raise_on_failed_jobs` resolved to False. `execution_service.complete_run` "
            "sets RunStatus.SUCCESS unconditionally, so a load with FAILED JOBS would return a "
            "LoadInfo instead of raising and we would mark that run SUCCESS. Either restore the "
            "default (look for a RAISE_ON_FAILED_JOBS env override — invisible in pyproject.toml) "
            "or make complete_run inspect the LoadInfo before it decides."
        )

    def test_the_resolution_is_live_and_an_override_really_reaches_it(self, tmp_path, monkeypatch):
        """🚨 The control, and it is what makes the assertion above worth having.

        Reading a dataclass default would pass identically and would be blind to exactly the
        change this guard exists to catch. Setting the override and watching the resolved value
        move is the only way to know we are reading the resolver rather than the class body.

        🔴 **This also corrects SPEC_EARNED_VERDICTS §4.4**, which names the override as
        ``LOAD__RAISE_ON_FAILED_JOBS``. Measured against dlt 1.21.0: that key does **not** reach
        it — the resolved value stays ``True``. The key that works is the unsectioned
        ``RAISE_ON_FAILED_JOBS``. Someone auditing for the spec's variable would have found
        nothing and concluded the override was absent.
        """
        assert _resolved_loader_config(tmp_path).raise_on_failed_jobs is True

        monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
        assert _resolved_loader_config(tmp_path).raise_on_failed_jobs is True, (
            "the sectioned key now reaches LoaderConfiguration. SPEC_EARNED_VERDICTS §4.4 names "
            "it and this test says it does not work — one of them is now wrong; re-measure "
            "before trusting either."
        )
        monkeypatch.delenv("LOAD__RAISE_ON_FAILED_JOBS")

        monkeypatch.setenv("RAISE_ON_FAILED_JOBS", "false")
        assert _resolved_loader_config(tmp_path).raise_on_failed_jobs is False, (
            "an explicit RAISE_ON_FAILED_JOBS=false did NOT move the resolved value, so the "
            "assertion above is reading a constant rather than a resolution and would not "
            "notice a real override"
        )


def test_the_extractor_is_annotated_as_optional():
    """A signature that says ``-> int`` cannot express "not measured".

    Cheap, and it is the half a reader checks first — if the annotation says ``int`` while the
    body returns ``None``, the next person to touch a caller writes ``rows or 0`` again in good
    faith.
    """
    import inspect

    ann = inspect.signature(_extract_rows_loaded).return_annotation
    assert "None" in str(ann), f"return annotation is {ann!r}, which cannot carry 'not measured'"


@pytest.mark.parametrize("value", [None, 0, 23])
def test_complete_run_accepts_an_unmeasured_count(value):
    """``complete_run``'s parameter must admit ``None`` too, or the extractor's honesty
    dies one call later at the type boundary."""
    import inspect

    from datanika.services.execution_service import ExecutionService

    ann = inspect.signature(ExecutionService.complete_run).parameters["rows_loaded"].annotation
    assert "None" in str(ann), (
        f"complete_run(rows_loaded: {ann!r}) cannot carry 'not measured', so "
        "_extract_rows_loaded returning None would be coerced or would fail type checks here"
    )
