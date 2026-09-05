"""External OpenAPI 3.x spec → a ``rest_api`` connector config.

See ``docs/specs/SPEC_OPENAPI_CONNECTOR.md``.

**This module is deterministic and touches no network** — the caller supplies
the spec text. That separation is a design property, not a phase we are waiting
to leave: keeping the parser network-free is what makes it cheap to fuzz.

URL fetch was this connector's P2 and **has shipped** — it lives in
``openapi_fetch.py`` (core#410), which is the only place the spec path talks to
the outside world and which carries SPEC §7's guards (https only, host must
resolve public, every redirect hop re-checked, 10 s timeout, 5 MB cap enforced
while streaming).

⚠️ **It is reachable through the API only.** ``api_v1_routes`` accepts a
``spec_url`` and calls ``resolve_spec``; the connector form under
``datanika/ui/`` has no URL field at all, so in the product this connector is
still paste-only. Do not write a docs page telling a user to paste a URL into a
box that does not exist.

Output feeds ``DltRunnerService._build_openapi_source`` (which delegates to the
existing ``rest_api`` runtime) and the IR builder.
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

# How far to descend looking for the row array. Enough for the usual one-level
# envelope; bounded because the spec is user input.
_MAX_SELECTOR_DEPTH = 3

# Query params that mean "only rows changed since X", and the row fields that
# carry the matching timestamp.
_INCREMENTAL_PARAMS = (
    "updated_since",
    "updated_after",
    "modified_since",
    "since",
    "start_date",
    "start_time",
    "from",
    "after",
)
_CURSOR_FIELDS = ("updated_at", "modified_at", "last_modified", "updated", "created_at")


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

    swagger = str(spec.get("swagger", ""))
    if swagger.startswith("2."):
        # Normalise into the 3.x subset this parser reads (core#411) rather
        # than teaching every function below two dialects.
        spec = _swagger2_to_openapi3(spec)

    version = str(spec.get("openapi", ""))
    if not version.startswith("3."):
        raise OpenApiImportError(
            f"Swagger {swagger} is not supported — convert to OpenAPI 3.x (2.0 is supported)"
            if swagger
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
        resource = _resource_from_get(spec, path, item, op, warnings)
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


def _resource_from_get(
    spec: dict, path: str, path_item: dict, op: dict, warnings: list[str]
) -> dict | None:
    item_schema, data_selector = _response_item_schema(spec, op)
    if item_schema is None:
        warnings.append(f"Skipped GET {path} — no array/collection JSON response schema.")
        return None
    columns = _columns_from_schema(spec, item_schema)
    endpoint: dict = {"path": path.lstrip("/"), "method": "GET"}
    if data_selector:
        endpoint["data_selector"] = data_selector

    response = _success_response(spec, op) or {}
    envelope = resolve_ref(
        spec, (((response.get("content") or {}).get("application/json")) or {}).get("schema")
    )
    envelope_props = (envelope or {}).get("properties") or {}
    paginator = _paginator_from_operation(
        spec, path_item, op, envelope_props, response.get("headers") or {}
    )
    if paginator:
        endpoint["paginator"] = paginator
    incremental = _incremental_from_operation(spec, path_item, op, item_schema)
    if incremental:
        endpoint["incremental"] = incremental
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


# --- Pagination detection (P2, core#411) -----------------------------------
#
# P1 emitted nothing and relied on dlt's runtime auto-detection, which fails
# *silently*: a wrong guess stops after page 1 and looks like a small table
# rather than an error. These heuristics are the shapes real specs declare,
# ordered most-authoritative first — a next-link in the body or a Link header
# is a statement of fact, whereas query-parameter names are inference.
#
# Emitted configs match dlt's own paginator constructors (PAGINATOR_MAP);
# nothing here invents a config shape dlt would reject.

_OFFSET_PARAMS = (("offset", "limit"), ("skip", "top"), ("skip", "take"), ("start", "count"))
_PAGE_PARAMS = ("page", "page_number", "pagenum")
_CURSOR_PARAMS = ("cursor", "after", "page_token", "next_token", "starting_after")
_NEXT_LINK_KEYS = ("next", "next_url", "next_page_url", "nextPageUrl", "next_page")
_CURSOR_RESPONSE_KEYS = ("next_cursor", "cursor", "next_page_token", "next_token", "end_cursor")
_TOTAL_KEYS = ("total", "total_count", "totalCount", "count", "total_results")

_DEFAULT_PAGE_SIZE = 100


def _query_params(spec: dict, path_item: dict, op: dict) -> dict[str, dict]:
    """Query parameters for an operation, including path-level inherited ones."""
    out: dict[str, dict] = {}
    for raw in list(path_item.get("parameters") or []) + list(op.get("parameters") or []):
        param = resolve_ref(spec, raw)
        if isinstance(param, dict) and param.get("in") == "query" and param.get("name"):
            out[str(param["name"])] = param
    return out


def _page_size(param: dict | None) -> int:
    """Prefer the API's declared ceiling over a number we made up."""
    schema = (param or {}).get("schema") or {}
    for key in ("maximum", "default"):
        value = schema.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return _DEFAULT_PAGE_SIZE


def _first_key(props: dict, candidates) -> str | None:
    lowered = {k.lower(): k for k in props}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _paginator_from_operation(
    spec: dict, path_item: dict, op: dict, envelope_props: dict, response_headers: dict
) -> dict | None:
    """Derive a dlt paginator config, or ``None`` to leave dlt's guess alone."""
    # 1. A next-URL in the body: authoritative, no inference needed.
    next_key = _first_key(envelope_props, _NEXT_LINK_KEYS)
    if next_key:
        return {"type": "json_link", "next_url_path": next_key}

    # 2. A declared Link header: RFC 5988, equally authoritative.
    if _first_key(response_headers, ("link",)):
        return {"type": "header_link"}

    params = _query_params(spec, path_item, op)
    total_key = _first_key(envelope_props, _TOTAL_KEYS)

    # 3. Cursor: only when the request carries one *and* the response returns
    #    one — a lone `cursor` param with nowhere to read the next value from
    #    would page forever against the same cursor.
    cursor_param = _first_key(params, _CURSOR_PARAMS)
    cursor_key = _first_key(envelope_props, _CURSOR_RESPONSE_KEYS)
    if cursor_param and cursor_key:
        return {"type": "cursor", "cursor_param": cursor_param, "cursor_path": cursor_key}

    # 4. Offset/limit under any of its common spellings.
    for offset_name, limit_name in _OFFSET_PARAMS:
        offset_param = _first_key(params, (offset_name,))
        limit_param = _first_key(params, (limit_name,))
        if offset_param and limit_param:
            return {
                "type": "offset",
                "offset_param": offset_param,
                "limit_param": limit_param,
                "limit": _page_size(params.get(limit_param)),
                # dlt defaults total_path to "total"; leaving that in place for
                # an API with no such field makes it hunt for a key that never
                # arrives. None falls back to stop-on-empty-page.
                "total_path": total_key,
            }

    # 5. Page number.
    page_param = _first_key(params, _PAGE_PARAMS)
    if page_param:
        return {"type": "page_number", "page_param": page_param, "total_path": total_key}

    # Nothing declared — stay silent rather than guess; dlt's runtime
    # auto-detection is still there and is a better guess than ours.
    return None


def _incremental_from_operation(
    spec: dict, path_item: dict, op: dict, item_schema: dict
) -> dict | None:
    """Map a "changed since" filter onto dlt's incremental config.

    Missing this is not a visible failure: the extract simply pulls the whole
    table every run, reports success, and quietly spends the customer's byte
    quota. Both halves are required — a ``start_param`` with no row field to
    read the high-water mark from would send an empty filter forever.
    """
    declared = op.get("x-incremental")
    if isinstance(declared, dict) and declared.get("cursor_path") and declared.get("start_param"):
        # An explicit spec declaration beats our inference.
        return {
            "cursor_path": str(declared["cursor_path"]),
            "start_param": str(declared["start_param"]),
        }

    params = _query_params(spec, path_item, op)
    start_param = None
    for name, param in params.items():
        schema = param.get("schema") or {}
        is_datetime = schema.get("format") in ("date-time", "date")
        if is_datetime and name.lower() in _INCREMENTAL_PARAMS:
            start_param = name
            break
    if not start_param:
        return None

    props = (item_schema or {}).get("properties") or {}
    cursor = _first_key(props, _CURSOR_FIELDS)
    if not cursor:
        return None
    return {"cursor_path": cursor, "start_param": start_param}


# --- Swagger 2.0 shim (P2, core#411) ---------------------------------------
#
# 2.0 specs were rejected outright, which is a large share of the real world.
# The parser stays 3.x-only; this normalises 2.0 into the subset it reads,
# rather than teaching every function two dialects.


def _swagger2_to_openapi3(spec: dict) -> dict:
    """Convert the parts of a Swagger 2.0 document this parser consumes."""
    converted: dict = {
        "openapi": "3.0.0",
        "info": spec.get("info") or {},
        "paths": {},
        "components": {},
    }

    scheme = (spec.get("schemes") or ["https"])[0]
    host = spec.get("host") or ""
    base_path = spec.get("basePath") or ""
    if host:
        converted["servers"] = [{"url": f"{scheme}://{host}{base_path}"}]

    if spec.get("definitions"):
        converted["components"]["schemas"] = _rewrite_refs(spec["definitions"])

    security = spec.get("securityDefinitions") or {}
    if security:
        converted["components"]["securitySchemes"] = {
            name: _convert_security_scheme(scheme_def)
            for name, scheme_def in security.items()
            if isinstance(scheme_def, dict)
        }

    for path, item in (spec.get("paths") or {}).items():
        if not isinstance(item, dict):
            continue
        converted["paths"][path] = {
            key: (_convert_operation(value) if key in _HTTP_METHODS else _rewrite_refs(value))
            for key, value in item.items()
        }
    return converted


_HTTP_METHODS = ("get", "put", "post", "delete", "patch", "options", "head")


def _convert_security_scheme(scheme: dict) -> dict:
    kind = scheme.get("type")
    if kind == "basic":
        return {"type": "http", "scheme": "basic"}
    return _rewrite_refs(scheme)


def _convert_operation(op: dict) -> dict:
    if not isinstance(op, dict):
        return op
    out = {k: _rewrite_refs(v) for k, v in op.items() if k not in ("parameters", "responses")}

    # 2.0 puts `type`/`format` on the parameter itself; 3.0 nests them under
    # `schema`. Normalising matters beyond tidiness: pagination and incremental
    # detection both read `param["schema"]`, so without this a converted spec
    # loses its declared `maximum` and its date-time hints.
    params = []
    for raw in op.get("parameters") or []:
        if not isinstance(raw, dict):
            continue
        param = _rewrite_refs(raw)
        if "schema" not in param and param.get("in") != "body":
            schema = {
                k: param.pop(k)
                for k in ("type", "format", "maximum", "default", "enum")
                if k in param
            }
            if schema:
                param["schema"] = schema
        params.append(param)
    if params:
        out["parameters"] = params

    # 2.0 hangs the response schema directly off the response; 3.0 puts it
    # under content[media-type].
    responses = {}
    for code, resp in (op.get("responses") or {}).items():
        if not isinstance(resp, dict):
            continue
        converted = {k: _rewrite_refs(v) for k, v in resp.items() if k != "schema"}
        if "schema" in resp:
            converted["content"] = {"application/json": {"schema": _rewrite_refs(resp["schema"])}}
        responses[code] = converted
    if responses:
        out["responses"] = responses
    return out


def _rewrite_refs(node):
    """Rewrite ``#/definitions/X`` → ``#/components/schemas/X`` everywhere.

    Miss this and every ``$ref`` dangles: the spec converts, the parse
    "succeeds", and every resource comes back with zero columns.
    """
    if isinstance(node, dict):
        return {
            key: (
                value.replace("#/definitions/", "#/components/schemas/")
                if key == "$ref" and isinstance(value, str)
                else _rewrite_refs(value)
            )
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_rewrite_refs(item) for item in node]
    return node


def _success_response(spec: dict, op: dict) -> dict | None:
    """The resolved success response object for a GET, if any."""
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
    return resp if isinstance(resp, dict) else None


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
    return _find_collection(spec, schema)


def _find_collection(
    spec: dict, schema: dict, prefix: str = "", depth: int = 0
) -> tuple[dict | None, str | None]:
    """Locate the row array in a response schema, returning (items, selector).

    Walks nested objects (``{"response": {"items": [...]}}`` → ``response.items``)
    rather than only the top level: a one-level-deep envelope is a common shape,
    and missing it yields **zero rows from a healthy API** — no error, just an
    empty table. Depth is bounded because a spec is user input.
    """
    if not isinstance(schema, dict):
        return None, None
    if schema.get("type") == "array":
        return resolve_ref(spec, schema.get("items") or {}), (prefix or None)
    if schema.get("type") != "object" and "properties" not in schema:
        return None, None

    props = schema.get("properties") or {}
    # Conventional envelope keys first, then anything else — a `data` array
    # beats an unrelated `errors` array when both are present.
    ordered = [k for k in _ENVELOPE_KEYS if k in props] + [
        k for k in props if k not in _ENVELOPE_KEYS
    ]
    for key in ordered:
        sub = resolve_ref(spec, props[key])
        if isinstance(sub, dict) and sub.get("type") == "array":
            path = f"{prefix}.{key}" if prefix else key
            return resolve_ref(spec, sub.get("items") or {}), path

    if depth >= _MAX_SELECTOR_DEPTH:
        return None, None
    for key in ordered:
        sub = resolve_ref(spec, props[key])
        if isinstance(sub, dict) and (sub.get("type") == "object" or "properties" in sub):
            path = f"{prefix}.{key}" if prefix else key
            items, selector = _find_collection(spec, sub, path, depth + 1)
            if items is not None:
                return items, selector
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
