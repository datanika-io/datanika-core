"""Every `dlt_config` key a builder reads must be declared internal (core#551).

`DltRunnerService.execute()` ends with:

    run_kwargs = {k: v for k, v in dlt_config.items() if k not in INTERNAL_CONFIG_KEYS}
    load_info = pipeline.run(source, **run_kwargs)

So the set is an **allowlist inverted**: anything not in it is handed to dlt,
and dlt raises ``TypeError: Pipeline.run() got an unexpected keyword argument``.
A key that a builder consumes but the set omits therefore fails the run — and
fails it *after* the source is built, which is why every `build_source` test in
this repo passed while the connector was unusable.

Sixteen keys were missing when this was written, found by an
``xfail(strict=True)`` Kafka probe that drove the real ``execute()`` rather than
the builder. Among them:

* ``endpoints`` — the endpoint picker (core#532) would have raised on any upload
  that used it, immediately after that feature shipped
* ``api_version`` — the Facebook version override (core#543) could not be set
* ``owner`` / ``repo`` — GitHub could not be configured at all
* ``start_date`` (Stripe), ``base_id`` (Airtable), ``database`` (MongoDB)

Two lists that must agree, maintained separately, drifting silently: the same
shape as core#532's picker-versus-builder mismatch. So this derives one from
the other instead of restating it.
"""

import ast
import pathlib

import pytest

from datanika.services.dlt_runner import INTERNAL_CONFIG_KEYS

_RUNNER = pathlib.Path(__file__).parent.parent.parent / "datanika" / "services" / "dlt_runner.py"


def _keys_read_from_dlt_config() -> set[str]:
    """Every literal key the module reads out of `dlt_config`.

    Matches ``dlt_config.get("x")`` and ``dlt_config["x"]``. A dynamic key
    (``dlt_config.get(name)``) is invisible here — see the anti-vacuity test
    below for why that is acceptable but worth knowing.
    """
    tree = ast.parse(_RUNNER.read_text(encoding="utf-8"), filename=str(_RUNNER))
    found: set[str] = set()

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and getattr(node.func.value, "id", None) == "dlt_config"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            found.add(node.args[0].value)

        if (
            isinstance(node, ast.Subscript)
            and getattr(node.value, "id", None) == "dlt_config"
            and isinstance(node.slice, ast.Constant)
        ):
            found.add(node.slice.value)

    return found


class TestTheAllowlistCoversTheBuilders:
    def test_the_scan_sees_the_builders(self):
        """Anti-vacuity: an empty scan would make the check below pass trivially."""
        keys = _keys_read_from_dlt_config()
        assert len(keys) >= 30, (
            f"only {len(keys)} dlt_config keys found in {_RUNNER.name} — the AST match is "
            "probably not working, which would make the real check vacuous"
        )
        # Anchors that must exist regardless of refactors.
        for key in ("mode", "table", "resources"):
            assert key in keys, f"expected {key!r} among the scanned keys"

    def test_no_builder_key_leaks_into_pipeline_run(self):
        leaking = sorted(_keys_read_from_dlt_config() - INTERNAL_CONFIG_KEYS)
        assert not leaking, (
            f"{leaking} are read by a builder but missing from INTERNAL_CONFIG_KEYS, so "
            "execute() forwards them to pipeline.run() and dlt raises TypeError. The run "
            "fails after the source is built, so build_source tests will not catch it. "
            "Add them to INTERNAL_CONFIG_KEYS."
        )


class TestTheGuardDiscriminates:
    """A contract test that cannot fail is decoration."""

    def test_a_missing_key_would_be_reported(self):
        """Simulate the drift this exists to catch."""
        read = {"mode", "table", "a_brand_new_builder_key"}
        leaking = sorted(read - INTERNAL_CONFIG_KEYS)
        assert leaking == ["a_brand_new_builder_key"]

    @pytest.mark.parametrize(
        "key",
        ["endpoints", "api_version", "topics", "owner", "repo", "start_date"],
    )
    def test_the_keys_that_were_actually_broken_are_covered(self, key):
        """Pin the specific regressions rather than trusting the set scan alone.

        Each of these was consumed by a builder and absent from the allowlist,
        so each one failed its connector at run time.
        """
        assert key in INTERNAL_CONFIG_KEYS
