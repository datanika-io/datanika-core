"""#93 — Source-scan regression tests for the ``template_first_run_triggered``
Plausible event wiring.

The actual consume-and-emit logic is covered by service-level tests in
``tests/test_services/test_connection_service.py``
(``TestConsumeTemplateFirstRun``). This module pins the *wiring* —
that ``ConnectionState.save_connection`` passes the template slug into
``create_connection``, and that ``PipelineState.run_pipeline`` /
``UploadState.run_upload`` call ``consume_template_first_run`` and emit
``rx.call_script`` with the ``template_first_run_triggered`` event.
A runtime test would need to stand up Reflex state init + mock the
execution/pipeline/upload services — more machinery than signal. If a
future refactor moves this logic to a different call site these
source-scan tests fail loudly.
"""

import inspect


class TestConnectionStateSaveConnectionPassesTemplateSlug:
    def test_save_connection_passes_selected_template_slug_to_create(self):
        import datanika.ui.state.connection_state as cs

        source = inspect.getsource(cs.ConnectionState.save_connection.fn)
        assert "source_template_slug=self.selected_template_slug" in source, (
            "ConnectionState.save_connection must pass "
            "`source_template_slug=self.selected_template_slug or None` "
            "to ConnectionService.create_connection so the template "
            "origin is persisted on the Connection row. #93."
        )

    def test_save_connection_clears_slug_after_save(self):
        import datanika.ui.state.connection_state as cs

        source = inspect.getsource(cs.ConnectionState.save_connection.fn)
        assert 'self.selected_template_slug = ""' in source, (
            "ConnectionState.save_connection must clear "
            "`selected_template_slug` after a successful save so a "
            "subsequent non-template create doesn't inherit the slug "
            "from the previous template flow. #93."
        )


class TestPipelineStateRunPipelineEmitsEvent:
    def test_consumes_template_first_run_on_destination_connection(self):
        import datanika.ui.state.pipeline_state as ps

        source = inspect.getsource(ps.PipelineState.run_pipeline.fn)
        assert "consume_template_first_run" in source, (
            "PipelineState.run_pipeline must call "
            "ConnectionService.consume_template_first_run on the "
            "pipeline's destination_connection_id to check and latch "
            "the funnel step 3 event. #93."
        )
        assert "destination_connection_id" in source, (
            "PipelineState.run_pipeline must read the pipeline's "
            "destination_connection_id when consuming the template "
            "first-run latch. #93."
        )

    def test_yields_rx_call_script_for_template_first_run_triggered(self):
        import datanika.ui.state.pipeline_state as ps

        source = inspect.getsource(ps.PipelineState.run_pipeline.fn)
        assert "rx.call_script" in source, (
            "PipelineState.run_pipeline must yield an rx.call_script "
            "with the Plausible event when the consume helper returns "
            "a slug. #93."
        )
        assert "template_first_run_triggered" in source, (
            "The call_script must emit the `template_first_run_triggered` "
            "Plausible event name (funnel step 3). #93."
        )

    def test_guards_window_plausible_undefined(self):
        import datanika.ui.state.pipeline_state as ps

        source = inspect.getsource(ps.PipelineState.run_pipeline.fn)
        assert "window.plausible" in source, "The call_script must reference window.plausible. #93."
        assert "if(window.plausible)" in source or "if (window.plausible)" in source, (
            "The call_script must guard window.plausible with an `if` "
            "so self-hosted installs without the tracker loaded no-op "
            "cleanly instead of throwing ReferenceError. #93."
        )


class TestUploadStateRunUploadEmitsEvent:
    def test_consumes_template_first_run_on_both_connections(self):
        import datanika.ui.state.upload_state as us

        source = inspect.getsource(us.UploadState.run_upload.fn)
        assert "consume_template_first_run" in source, (
            "UploadState.run_upload must call ConnectionService.consume_template_first_run. #93."
        )
        # Both ends — an upload's template flow could live on either side.
        assert "source_connection_id" in source, (
            "UploadState.run_upload must check upload.source_connection_id "
            "for a template origin. #93."
        )
        assert "destination_connection_id" in source, (
            "UploadState.run_upload must check upload.destination_connection_id "
            "for a template origin. #93."
        )

    def test_yields_rx_call_script_for_template_first_run_triggered(self):
        import datanika.ui.state.upload_state as us

        source = inspect.getsource(us.UploadState.run_upload.fn)
        assert "rx.call_script" in source, (
            "UploadState.run_upload must yield an rx.call_script when "
            "the consume helper returns a slug. #93."
        )
        assert "template_first_run_triggered" in source, (
            "The call_script must emit the `template_first_run_triggered` "
            "Plausible event name. #93."
        )

    def test_guards_window_plausible_undefined(self):
        import datanika.ui.state.upload_state as us

        source = inspect.getsource(us.UploadState.run_upload.fn)
        assert "if(window.plausible)" in source or "if (window.plausible)" in source, (
            "The call_script must guard window.plausible with an `if` "
            "so self-hosted installs without the tracker loaded no-op "
            "cleanly. #93."
        )


class TestConnectionServiceSignatureHasTemplateSlugKwarg:
    """Guard against a future refactor that drops the kwarg — the UI
    passes it as a keyword argument, so any signature change that
    silently removes it would break the wiring without a test failure
    at the call site."""

    def test_create_connection_accepts_source_template_slug_kwarg(self):
        from datanika.services.connection_service import ConnectionService

        sig = inspect.signature(ConnectionService.create_connection)
        assert "source_template_slug" in sig.parameters, (
            "ConnectionService.create_connection must accept a "
            "`source_template_slug` keyword argument. #93."
        )
        # Default must be None so callers that don't care don't need to pass it.
        assert sig.parameters["source_template_slug"].default is None
