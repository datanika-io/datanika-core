"""External OpenAPI 3.x spec → a ``rest_api`` connector config (P1, paste-mode).

See ``plans/engineering/SPEC_OPENAPI_CONNECTOR.md``. Deterministic, no network:
the caller supplies the spec text (paste). URL fetch is deferred to P2 behind
SSRF guards. Output feeds ``DltRunnerService._build_openapi_source`` (which
delegates to the existing ``rest_api`` runtime) and the IR builder.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from datanika.services.openapi_schema import json_schema_to_column_type, resolve_ref

# §7 caps — enforced before/while walking, typed errors on breach.
MAX_SPEC_BYTES = 5_000_000
MAX_RESOURCES = 300

# Response-object properties that commonly wrap a collection (the data-selector).
_ENVELOPE_KEYS = ("data", "results", "items", "records", "value", "rows")


class OpenApiImportError(ValueError):
    """Typed parse failure.

    ``code`` ∈ {``invalid_spec``, ``spec_too_large``, ``unsupported_version``,
    ``too_complex``} so the API/UI can branch without regex-matching messages.
    """

    def __init__(self, message: str, code: str):
        super().__init__(message)
        self.code = code


@dataclass
class ParsedConnector:
    """The derived shape of a rest_api connector for one spec."""

    base_url: str
    auth_schemes: list[dict]
    resources: list[dict]
    warnings: list[str] = field(default_factory=list)


def _load(raw: str | dict) -> dict:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        raise OpenApiImportError("Spec must be a string or dict", "invalid_spec")
    if len(raw.encode("utf-8")) > MAX_SPEC_BYTES:
        raise OpenApiImportError("Spec exceeds the size limit", "spec_too_large")
    text = raw.strip()
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        pass
    try:
        import yaml

        loaded = yaml.safe_load(text)
    except Exception as exc:
        raise OpenApiImportError(
            f"Could not parse spec as JSON or YAML: {exc}", "invalid_spec"
        ) from exc
    if not isinstance(loaded, dict):
        raise OpenApiImportError("Spec did not parse to an object", "invalid_spec")
    return loaded


def parse_openapi_spec(raw: str | dict, *, base_url_override: str | None = None) -> ParsedConnector:
    """Parse an OpenAPI 3.0/3.1 spec into a ``ParsedConnector``.

    Extracts the base URL (``servers``), auth schemes (``securitySchemes``), and
    one resource per GET collection endpoint — each with ``columns`` derived
    from the response schema (no live call). Templated (detail) endpoints are
    skipped in P1. Anything skipped or ambiguous is recorded in ``warnings``.
    """
    spec = _load(raw)

    version = str(spec.get("openapi", ""))
    if not version.startswith("3."):
        raise OpenApiImportError(
            "Only OpenAPI 3.0/3.1 is supported (Swagger 2.0 is not supported in P1)"
            if "swagger" in spec
            else "Missing or unsupported 'openapi' version (need 3.x)",
            "unsupported_version",
        )

    warnings: list[str] = []
    base_url = base_url_override or _extract_base_url(spec, warnings)
    auth_schemes = _extract_auth(spec, warnings)

    paths = spec.get("paths") or {}
    if not isinstance(paths, dict):
        raise OpenApiImportError("Spec 'paths' is not an object", "invalid_spec")
    if len(paths) > MAX_RESOURCES * 4:
        raise OpenApiImportError(f"Spec has more than {MAX_RESOURCES * 4} paths", "too_complex")

    resources: list[dict] = []
    for path, item in paths.items():
        if not isinstance(item, dict):
            continue
        if "{" in path:
            warnings.append(
                f"Skipped templated endpoint {path} — detail endpoints need a parent (P3)."
            )
            continue
        op = item.get("get")
        if not isinstance(op, dict):
            continue
        resource = _resource_from_get(spec, path, op, warnings)
        if resource is not None:
            resources.append(resource)
        if len(resources) > MAX_RESOURCES:
            raise OpenApiImportError(
                f"Spec exposes more than {MAX_RESOURCES} readable endpoints", "too_complex"
            )

    return ParsedConnector(
        base_url=base_url, auth_schemes=auth_schemes, resources=resources, warnings=warnings
    )


def _extract_base_url(spec: dict, warnings: list[str]) -> str:
    servers = spec.get("servers")
    if isinstance(servers, list) and servers and isinstance(servers[0], dict):
        url = servers[0].get("url", "") or ""
        for var, meta in (servers[0].get("variables") or {}).items():
            default = (meta or {}).get("default")
            if default is not None:
                url = url.replace("{" + var + "}", str(default))
        if url:
            return url
    warnings.append("No 'servers' URL in the spec — set the base URL manually.")
    return ""


def _extract_auth(spec: dict, warnings: list[str]) -> list[dict]:
    schemes = ((spec.get("components") or {}).get("securitySchemes")) or {}
    out: list[dict] = []
    for name, scheme in schemes.items():
        if not isinstance(scheme, dict):
            continue
        t = scheme.get("type")
        if t == "http" and scheme.get("scheme") == "bearer":
            out.append({"type": "bearer"})
        elif t == "http" and scheme.get("scheme") == "basic":
            out.append({"type": "http_basic"})
        elif t == "apiKey":
            out.append(
                {
                    "type": "api_key",
                    "name": scheme.get("name", ""),
                    "location": scheme.get("in", "header"),
                }
            )
        elif t == "oauth2":
            warnings.append(
                f"OAuth2 scheme '{name}' is not supported in P1 — supply a static token."
            )
        else:
            warnings.append(f"Unsupported auth scheme '{name}' (type {t}).")
    return out


def _resource_from_get(spec: dict, path: str, op: dict, warnings: list[str]) -> dict | None:
    item_schema, data_selector = _response_item_schema(spec, op)
    if item_schema is None:
        warnings.append(f"Skipped GET {path} — no array/collection JSON response schema.")
        return None
    columns = _columns_from_schema(spec, item_schema)
    endpoint: dict = {"path": path.lstrip("/"), "method": "GET"}
    if data_selector:
        endpoint["data_selector"] = data_selector
    resource: dict = {
        "name": _resource_name(path),
        "endpoint": endpoint,
        "columns": columns,
        "_source": {"operation_id": op.get("operationId"), "summary": op.get("summary")},
    }
    pk = _primary_key(columns)
    if pk:
        resource["primary_key"] = pk
    return resource


def _response_item_schema(spec: dict, op: dict) -> tuple[dict | None, str | None]:
    """Return (item_schema, data_selector) for a GET's success JSON response."""
    responses = op.get("responses") or {}
    resp = None
    for code in ("200", "201", "2XX", "default"):
        if code in responses:
            resp = responses[code]
            break
    if resp is None:
        for code, r in responses.items():
            if str(code).startswith("2"):
                resp = r
                break
    resp = resolve_ref(spec, resp)
    if not isinstance(resp, dict):
        return None, None
    schema = (((resp.get("content") or {}).get("application/json")) or {}).get("schema")
    schema = resolve_ref(spec, schema)
    if not isinstance(schema, dict):
        return None, None
    if schema.get("type") == "array":
        return resolve_ref(spec, schema.get("items") or {}), None
    if schema.get("type") == "object" or "properties" in schema:
        props = schema.get("properties") or {}
        # prefer a conventional envelope key, else any array-typed property
        ordered = [k for k in _ENVELOPE_KEYS if k in props] + [
            k for k in props if k not in _ENVELOPE_KEYS
        ]
        for key in ordered:
            sub = resolve_ref(spec, props[key])
            if isinstance(sub, dict) and sub.get("type") == "array":
                return resolve_ref(spec, sub.get("items") or {}), key
    return None, None


def _columns_from_schema(spec: dict, item_schema: dict) -> list[dict]:
    item_schema = resolve_ref(spec, item_schema)
    if not isinstance(item_schema, dict):
        return []
    props = item_schema.get("properties") or {}
    required = set(item_schema.get("required") or [])
    columns: list[dict] = []
    for name, prop in props.items():
        resolved = resolve_ref(spec, prop)
        columns.append(
            {
                "name": name,
                "type": json_schema_to_column_type(resolved),
                "nullable": name not in required,
            }
        )
    return columns


def _resource_name(path: str) -> str:
    segs = [s for s in path.split("/") if s and "{" not in s]
    name = segs[-1] if segs else "resource"
    return re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower() or "resource"


def _primary_key(columns: list[dict]) -> str | None:
    names = [c["name"] for c in columns]
    if "id" in names:
        return "id"
    for n in names:
        if n.lower().endswith("_id") or n.lower() == "uuid":
            return n
    return None
