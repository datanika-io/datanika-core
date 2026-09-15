"""OpenAPI connector placeholders (core#1348 item 1).

Two copy defects on the ``openapi`` connection form:

* ``ph_openapi_spec`` said "OpenAPI 3.x" only, but Swagger 2.0 is accepted and
  converted (``parse_openapi_spec`` -> ``_swagger2_to_openapi3``).
* the API-key field reused the shared ``ph_api_key`` ("API key for Authorization
  header"), but for ``openapi`` the spec's ``securitySchemes`` decide where the
  key goes (a header the spec names, a query parameter, a cookie, or HTTP Basic's
  username -- ``openapi_import._extract_auth``), so it gets its own placeholder.

The AST test is deliberately AST-based, not a source ``grep``: the code comment
next to the fix mentions ``ph_api_key`` while explaining why it is wrong here, and
a substring check would be satisfied by that comment (PRODUCT_RULES 11).
"""

import ast
import inspect
import json
from pathlib import Path

import pytest

I18N_DIR = Path(__file__).resolve().parents[2] / "datanika" / "i18n"
LOCALES = ["en", "de", "el", "es", "fr", "ru", "sr", "zh", "ar"]


def _load(locale: str) -> dict:
    return json.loads((I18N_DIR / f"{locale}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("locale", LOCALES)
def test_openapi_spec_placeholder_names_both_openapi3_and_swagger2(locale):
    """AC1: the spec placeholder names OpenAPI 3.x AND Swagger 2.0, every locale.

    The version tokens are Latin in every locale (they are product/format names),
    so this reads across all nine without per-locale wording.
    """
    val = _load(locale)["connections.ph_openapi_spec"]
    # Typography-robust: some locales hyphenate the compound (German
    # "Swagger-2.0-Spezifikation"), so assert the product name and the version
    # token independently rather than a fixed "Swagger 2.0" spelling. Still
    # discriminating -- origin/dev names neither Swagger nor 2.0.
    assert "OpenAPI" in val and "3.x" in val, f"{locale}: must name OpenAPI 3.x -> {val!r}"
    assert "Swagger" in val and "2.0" in val, f"{locale}: must name Swagger 2.0 -> {val!r}"


@pytest.mark.parametrize("locale", LOCALES)
def test_openapi_api_key_placeholder_exists_and_names_no_fixed_location(locale):
    """AC2: openapi has its own api-key placeholder that does not name a location.

    The shared ``ph_api_key`` names the Authorization header; the openapi one must
    not, because the spec decides the location. Asserted as "does not say
    Authorization" plus "is distinct from the shared key's value".
    """
    d = _load(locale)
    assert "connections.ph_openapi_api_key" in d, f"{locale}: missing ph_openapi_api_key"
    openapi_val = d["connections.ph_openapi_api_key"]
    assert openapi_val != d["connections.ph_api_key"], (
        f"{locale}: openapi api-key placeholder must differ from the shared one"
    )
    assert "Authorization" not in openapi_val, (
        f"{locale}: openapi placeholder must not name the Authorization header -> {openapi_val!r}"
    )


def _t_subscript_keys(func) -> set[str]:
    """Every ``_t["..."]`` string key referenced in ``func``'s code (AST, not text).

    Comments are absent from the AST, so a comment that mentions a key is not
    counted -- which is the whole point (PRODUCT_RULES 11).
    """
    tree = ast.parse(inspect.cleandoc("\n" + inspect.getsource(func)))
    keys: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id == "_t"
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            keys.add(node.slice.value)
    return keys


def test_openapi_form_uses_its_own_api_key_placeholder():
    """AC2 (wiring): openapi_fields references ph_openapi_api_key, not ph_api_key."""
    from datanika.ui.components import connection_config_fields as m

    keys = _t_subscript_keys(m.openapi_fields)
    # anti-vacuity: the extractor must actually see this function's subscripts
    assert "connections.ph_openapi_spec" in keys, "AST extractor saw no _t keys -- proves nothing"
    assert "connections.ph_openapi_api_key" in keys, "openapi_fields must use its own placeholder"
    assert "connections.ph_api_key" not in keys, (
        "openapi_fields must not reuse ph_api_key (a comment may mention it; the code must not)"
    )
