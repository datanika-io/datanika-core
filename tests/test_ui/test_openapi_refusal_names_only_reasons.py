"""The openapi form's zero-endpoint refusal lists reasons, and only reasons (core#1416).

## The defect

`core#1345` made the form refuse a spec with no loadable endpoint and show the parser's
reasons. The refusal is right; its message had three problems, all measured through the
form on a stack built from `master`:

1. **A warning that is not a reason took a reason's slot.** With a spec carrying an OAuth2
   security scheme, the first "reason the spec has no endpoint" was the OAuth2 warning —
   which has nothing to do with why no endpoint loaded. The form shows at most three
   (`parsed.warnings[:3]`), so on a spec skipping three endpoints a real reason was pushed
   out behind `and 1 more`.
2. **Internal delivery-phase labels reached the user**: `(P3)` and `in P1`. A reader can
   act on neither.
3. **Doubled punctuation** — each warning ended in `.` and the form joined with `"; "`,
   rendering `(P3).; Skipped`.

## Why the assertions are exact strings

⚠️ **`WORKFLOW_RULES` §4: assert the PRESENCE of the right thing, never the absence of the
wrong word.** A test that banned `P3` would pass if the whole reason were deleted, and a
test that banned the OAuth2 sentence would pass if warnings stopped being produced at all.
So this asserts the **full rendered message**, which can only be satisfied by the message
being right — and `test_the_oauth2_warning_still_exists` separately pins that the warning
was *re-homed*, not removed.

## The stand-in `self`

`ConnectionState()` raises `ReflexRuntimeError`, and a `MagicMock` would make this unable
to fail — a mock answers every attribute truthily, so every branch of `_build_config`
runs. `FormStub` carries exactly the vars the class declares, at their declared defaults,
so a var the serialiser reads but the class does not declare raises `AttributeError`.
(Same reasoning as `tests/test_ui/test_connection_config_roundtrip.py`.)
"""

import copy
import json

import pytest

from datanika.errors import UserFacingError
from datanika.services.openapi_import import parse_openapi_spec
from datanika.ui.state.connection_state import ConnectionState

# --------------------------------------------------------------------------- #
# Spec B from core#1416: two GETs that both skip, plus one OAuth2 scheme.
# --------------------------------------------------------------------------- #

SPEC_B = {
    "openapi": "3.0.3",
    "info": {"title": "Corp", "version": "1.0"},
    "servers": [{"url": "https://api.corp.example/v1"}],
    "components": {
        "securitySchemes": {
            "corpOAuth": {
                "type": "oauth2",
                "flows": {
                    "clientCredentials": {
                        "tokenUrl": "https://api.corp.example/oauth/token",
                        "scopes": {},
                    }
                },
            }
        }
    },
    "paths": {
        # templated -> a detail endpoint, returns one record
        "/users/{id}": {
            "get": {
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {"schema": {"type": "object"}},
                        }
                    }
                }
            }
        },
        # object response -> declares no list of records
        "/status": {
            "get": {
                "responses": {
                    "200": {
                        "content": {
                            "application/json; charset=utf-8": {"schema": {"type": "object"}},
                        }
                    }
                }
            }
        },
    },
}

#: Spec A from the issue is spec B without the security scheme. The OAuth2 warning is the
#: only difference, so the two messages being IDENTICAL is what proves the warning stopped
#: taking a reason's slot.
SPEC_A = copy.deepcopy(SPEC_B)
del SPEC_A["components"]

EXPECTED = (
    "This spec has no endpoint the connector can load: "
    "Skipped /users/{id}: a path with a parameter returns one record, not a list; "
    "Skipped GET /status: its JSON response declares no list of records"
)


class FormStub:
    """A stand-in `self` carrying exactly `ConnectionState`'s declared vars."""

    _build_config = ConnectionState._build_config

    def __init__(self, spec: dict):
        for name, field in ConnectionState.get_fields().items():
            setattr(self, name, copy.deepcopy(getattr(field, "default", None)))
        self.form_use_raw_json = False
        self.form_type = "openapi"
        self.form_openapi_spec = json.dumps(spec)
        self.form_base_url = ""
        self.form_api_key = ""


def refusal(spec: dict) -> str:
    with pytest.raises(UserFacingError) as exc:
        FormStub(spec)._build_config()
    return str(exc.value)


class TestTheRefusalNamesOnlyReasons:
    def test_spec_b_renders_exactly_the_reasons_and_nothing_else(self):
        """AC1 + AC2 + AC3, as one exact string.

        It covers all three because there is only one message: an auth warning in it
        fails AC1, a phase label fails AC2, and `.;` fails AC3.
        """
        assert refusal(SPEC_B) == EXPECTED

    def test_the_security_scheme_changes_nothing_about_the_refusal(self):
        """AC1, stated as a difference rather than as a string.

        Spec A and spec B differ by exactly one security scheme and by nothing that
        affects whether an endpoint loads, so their refusals must be identical. This
        arm would still fail if EXPECTED were wrong in the same way twice.
        """
        assert refusal(SPEC_A) == refusal(SPEC_B)

    def test_the_oauth2_warning_still_exists_and_carries_no_phase_label(self):
        """The warning is RE-HOMED, not deleted.

        Without this, deleting the warning entirely would satisfy the exact-string
        assertions above — the §4 trap, where a ban on a phrase is met by removing the
        sentence that carried it.
        """
        parsed = parse_openapi_spec(json.dumps(SPEC_B))
        oauth = [w for w in parsed.warnings if "corpOAuth" in w]
        assert oauth == ["OAuth2 scheme 'corpOAuth' is not supported — supply a static token"]

    def test_the_skip_reasons_are_a_subset_of_the_warnings(self):
        """The API's `warnings` stays complete, so nothing that reads it loses data.

        The form reads the narrower list; `warnings` remains the full record.
        """
        parsed = parse_openapi_spec(json.dumps(SPEC_B))
        assert parsed.skip_reasons, "no skip reasons recorded, so the form has nothing to show"
        assert set(parsed.skip_reasons) <= set(parsed.warnings)
        assert len(parsed.warnings) > len(parsed.skip_reasons), (
            "the OAuth2 warning should be in warnings but not in skip_reasons; if these "
            "are equal, the two lists are not actually distinguishing anything"
        )

    def test_no_reason_ends_in_a_period_so_the_join_cannot_double_up(self):
        """AC3 at the source rather than only in the rendered string.

        Asserts a property of every reason, so a fourth reason added later cannot
        reintroduce `.;` without this going red.
        """
        parsed = parse_openapi_spec(json.dumps(SPEC_B))
        for reason in parsed.skip_reasons:
            assert not reason.endswith("."), reason
