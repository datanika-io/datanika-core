"""The MongoDB connection form after core#626 — TLS, SRV and `auth_source`.

Contract: ``docs/specs/SPEC_MONGODB_TLS_SRV.md``, criteria 4, 5 and 7.

⚠️ **A conditional field cannot be asserted by absence.** ``rx.cond`` over a
*state var* renders **both** branches into the JSX and picks between them in the
browser, so "the Port field is not rendered when SRV is on" is not a statement
about the rendered tree. What is assertable — and is the thing that actually
breaks — is the **wiring**: that Port is inside a cond keyed on the SRV var, and
that the TLS checkbox's ``disabled`` is bound to it rather than hardcoded.

The round-trip half of the contract (criterion 6 — a config key present in one
serialiser and absent from the other is silently dropped on the next save) is
already guarded by ``test_connection_config_roundtrip.py``'s ratchet, whose
ledger carries ``mongodb: {auth_source}`` with the comment *"leaves with
core#626"*. Deleting that entry is the red-first evidence: leave it in place
after the fix and ``test_the_ledger_does_not_outlive_the_defects`` fails naming
it.
"""

import pytest

from datanika.ui.components.connection_config_fields import mongodb_fields
from datanika.ui.state.connection_state import ConnectionState


def _walk(node):
    """Every dict in a rendered Reflex tree, through ``rx.cond`` branches too."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def _props(node: dict) -> dict[str, str]:
    parsed = {}
    for prop in node.get("props", []):
        if ":" not in prop:
            continue
        key, _, value = prop.partition(":")
        parsed[key.strip().strip('"')] = value.strip()
    return parsed


@pytest.fixture(scope="module")
def rendered() -> str:
    return str(mongodb_fields().render())


@pytest.fixture(scope="module")
def nodes() -> list[dict]:
    return [n for n in _walk(mongodb_fields().render()) if isinstance(n, dict)]


class TestTheFieldsExist:
    def test_the_walk_reaches_the_mongodb_fields(self, rendered):
        """Anti-vacuity guard. Every assertion below is a substring test, and a
        substring test over an empty string fails in the reassuring direction
        only because nothing was searched."""
        assert "cfg-host" in rendered and "cfg-database" in rendered, rendered[:400]

    def test_auth_source_has_a_visible_input(self, rendered):
        """Criterion 5, and the defect that widened this spec.

        ``auth_source`` shipped in ``CONFIG_SCHEMAS`` with a test whose docstring
        reads *"a setting with no surface is the core#499 mistake"* — and it had
        no surface. It was reachable only through the raw-JSON escape hatch, and
        it did not survive: reopening the connection in the structured form and
        clicking Save rebuilt the config from five fields and dropped it, so a
        working connection silently reverted to ``admin`` after an unrelated
        edit.
        """
        # `config_input` slugifies: `auth_source` renders as `cfg-auth-source`.
        assert "cfg-auth-source" in rendered

    def test_the_two_checkboxes_are_bound_to_their_own_state_vars(self, rendered):
        assert "form_mongodb_srv" in rendered
        assert "form_mongodb_tls" in rendered

    def test_tls_does_not_reuse_the_clickhouse_secure_var(self, rendered):
        """D5. ``form_secure`` is ClickHouse's *Use HTTPS (TLS)*. Sharing it
        because the labels rhyme is what makes a mid-form type switch arrive at
        MongoDB with TLS silently pre-checked — a connection failure with no
        visible cause."""
        assert "form_secure" not in rendered


class TestTheDependentFields:
    def test_the_tls_checkbox_is_disabled_by_the_srv_var(self, nodes):
        """Criterion 4, and the half a screenshot cannot show.

        SRV implies TLS — that is the MongoDB URI specification, not our
        invention — so the checkbox must render checked and non-interactive
        rather than letting a user construct a combination the driver overrides
        anyway. A control that lies about its effect is worse than no control.

        ``disabled`` must be *bound to the var*, not hardcoded: a literal
        ``disabled={true}`` would satisfy a laxer assertion while disabling the
        checkbox permanently.
        """
        disabled = [p for p in (_props(n) for n in nodes) if "disabled" in p]
        assert disabled, "no node in the MongoDB field group declares `disabled`"
        assert any("form_mongodb_srv" in p["disabled"] for p in disabled), (
            f"`disabled` is not bound to the SRV var: {[p['disabled'] for p in disabled]}"
        )

    def test_the_port_field_is_conditional_on_the_srv_var(self, rendered):
        """Criterion 4. ``mongodb+srv://host:27017/`` is invalid per the URI
        spec — the SRV records supply the ports — so Port must not be collected
        when SRV is on.

        Asserted as *wiring* rather than absence, for the reason in the module
        docstring: both branches of a state-var cond are in the JSX.
        """
        assert "cfg-port" in rendered, "the Port field vanished entirely"
        port_at = rendered.index("cfg-port")
        srv_before_port = rendered.rfind("form_mongodb_srv", 0, port_at)
        assert srv_before_port != -1, (
            "the Port field is not inside a conditional keyed on form_mongodb_srv"
        )


class TestBooleansResetOnTypeChange:
    """Criterion 7 / D5.

    ``set_form_type`` resets the port default and the test verdict and **not**
    booleans, so ``form_secure``, ``form_cluster_replication`` and
    ``form_oracle_use_sid`` all survive a mid-form type switch today. That is an
    existing bug with no reporter; adding a fourth boolean is what makes it
    reachable from the flow this spec is about.
    """

    def _stub(self):
        """A stand-in carrying the class's declared vars at their declared defaults.

        Deliberately not a ``MagicMock``: a mock answers every attribute with a
        truthy mock, so an assertion that a boolean was reset to ``False`` would
        be comparing against something that was never ``True`` and never became
        ``False`` — a test that cannot fail (core#644).
        """

        class _Stub:
            pass

        stub = _Stub()
        for name, field in ConnectionState.get_fields().items():
            if not name.startswith("form_") and name != "test_success":
                continue
            default = field.default_factory() if field.default_factory else field.default
            setattr(stub, name, default)
        stub._clear_test_verdict = lambda: None
        return stub

    def test_clickhouse_secure_does_not_carry_into_mongodb(self):
        """Criterion 7 itself, which is satisfied by D5's *first* rule.

        ⚠️ The `form_mongodb_tls is False` line below is a **control, not the
        assertion** — it holds because the two connectors use separate vars, so
        MongoDB's was never `True` to begin with. Mutation testing caught me
        treating it as the assertion: deleting the mongodb resets from
        `set_form_type` left this test green, because a "for all" over a value
        that was never in the failing state cannot report anything. The test
        that detects that mutation is the next one, which sets the vars first.

        What is load-bearing here is `form_secure`, which *was* `True`.
        """
        stub = self._stub()
        stub.form_secure = True
        ConnectionState.set_form_type.fn(stub, "mongodb")
        assert stub.form_mongodb_tls is False  # control — separate vars (D5.1)
        assert stub.form_secure is False, (
            "set_form_type must reset every boolean form field, not just the new ones — "
            "the ClickHouse/Oracle carry-over is the same bug with no reporter"
        )

    def test_mongodb_booleans_do_not_carry_into_clickhouse(self):
        """D5.2, and the test that actually guards the mongodb resets.

        Both vars are set `True` first, so removing either reset line from
        `set_form_type` turns this red. Verified by doing exactly that.
        """
        stub = self._stub()
        stub.form_mongodb_tls = True
        stub.form_mongodb_srv = True
        ConnectionState.set_form_type.fn(stub, "clickhouse")
        assert stub.form_mongodb_tls is False
        assert stub.form_mongodb_srv is False

    def test_oracle_use_sid_is_reset_too(self):
        stub = self._stub()
        stub.form_oracle_use_sid = True
        ConnectionState.set_form_type.fn(stub, "mongodb")
        assert stub.form_oracle_use_sid is False
