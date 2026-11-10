"""Every rendered cfg-* input: the marker and the HTML required attribute agree (core#1610 AC3).

``test_field_requiredness.py`` asserts the same thing over ``_SLICE``, the
``labelled_config_input`` migration ratchet. A hand-written label beside a
``config_input`` is the shape that can disagree, and the slice excludes it.
This file walks the rendered form instead.

The marker is read from both authoring surfaces: a ``contents`` string that
contains ``*`` under the ``htmlFor``-bound label. The derived marker is
``{"contents": "\\"*\\""}`` inside an aria-hidden span, so a needle that wants
a ``"*"`` literal reports every derived site as unmarked.
"""

from __future__ import annotations

import re

from datanika.models.connection import ConnectionType
from datanika.ui.components import connection_config_fields as ccf

# These eight have no ``<type>_fields``. csv/json/parquet render through
# ``file_upload_fields``; the five SQL types render through ``db_fields``.
# Both components are walked under their own names, so skipping the eight
# does not grade their inputs clean.
_VIA_UPLOAD = frozenset({"csv", "json", "parquet"})
_VIA_DB = frozenset({"mssql", "mysql", "postgres", "redshift", "synapse"})
NO_OWN_RENDERER = _VIA_UPLOAD | _VIA_DB

_SHARED_SAAS = {
    "hubspot": "saas_api_key_fields",
    "notion": "saas_api_key_fields",
    "pipedrive": "saas_api_key_fields",
    "slack": "saas_api_key_fields",
}

_INPUT_NAME = re.compile(r'name:"cfg-([a-z0-9-]+)"')


def _children(node) -> list:
    if not isinstance(node, dict):
        return []
    out = list(node.get("children", []))
    for arm in ("true_value", "false_value"):
        branch = node.get(arm)
        if isinstance(branch, dict):
            out.append(branch)
        elif isinstance(branch, list):
            out.extend(branch)
    return out


def _walk(node):
    yield node
    for child in _children(node):
        yield from _walk(child)


def _props(node) -> list:
    return node.get("props", []) if isinstance(node, dict) else []


def _shows_marker(label) -> bool:
    """True when a sighted user sees a star, from either authoring surface."""
    for node in _walk(label):
        if not isinstance(node, dict):
            continue
        contents = node.get("contents", "")
        if isinstance(contents, str) and "*" in contents and "translations" not in contents:
            return True
    return False


def _label_for(root, slug: str):
    needle = f'htmlFor:"cfg-{slug}"'
    for node in _walk(root):
        if isinstance(node, dict) and needle in _props(node):
            return node
    return None


def _renderers() -> dict[str, object]:
    """One callable per type that has a renderer, plus the two shared components."""
    found: dict[str, object] = {}
    for member in ConnectionType:
        name = member.value
        if name in NO_OWN_RENDERER:
            continue
        fn_name = _SHARED_SAAS.get(name, f"{name}_fields")
        found[name] = getattr(ccf, fn_name)
    found["db_fields"] = ccf.db_fields
    found["file_upload_fields"] = ccf.file_upload_fields
    return found


def _rendered_inputs() -> list[tuple[str, str, bool, bool]]:
    """``(renderer, field, marker, required)`` for every cfg-* input."""
    rows = []
    for name, fn in _renderers().items():
        rendered = fn().render()
        for node in _walk(rendered):
            match = _INPUT_NAME.search(" ".join(_props(node)))
            if not match:
                continue
            slug = match.group(1)
            label = _label_for(rendered, slug)
            assert label is not None, f"{name} cfg-{slug} has no htmlFor-bound label"
            rows.append(
                (
                    name,
                    slug.replace("-", "_"),
                    _shows_marker(label),
                    "required:true" in _props(node),
                )
            )
    return rows


def test_the_eight_rendererless_types_are_named():
    members = {m.value for m in ConnectionType}
    no_own_function = {name for name in members if not hasattr(ccf, f"{name}_fields")}
    assert no_own_function - set(_SHARED_SAAS) == NO_OWN_RENDERER, (
        f"types with no <type>_fields and no shared saas renderer: "
        f"{sorted(no_own_function - set(_SHARED_SAAS))}. The eight are "
        f"{sorted(NO_OWN_RENDERER)}: csv/json/parquet draw file_upload_fields, "
        "and mssql/mysql/postgres/redshift/synapse draw db_fields."
    )


def test_the_marker_matches_required_on_every_rendered_input():
    rows = _rendered_inputs()
    renderers = {name for name, _, _, _ in rows}
    assert len(rows) >= 80, (
        f"the walk found {len(rows)} cfg-* inputs; it has stopped matching the "
        "rendered shape, and an empty result would report a clean sweep of zero"
    )
    assert len(renderers) >= 25, f"the walk rendered only {sorted(renderers)}"
    marked_required = sum(1 for _, _, marker, required in rows if marker and required)
    unmarked_optional = sum(1 for _, _, marker, required in rows if not marker and not required)
    assert marked_required > 0, "no marked, required input; the walk cannot see a star"
    assert unmarked_optional > 0, "no unmarked, optional input; the walk only ever sees stars"
    disagree = [
        f"{name}.{field}: marker={marker} required={required}"
        for name, field, marker, required in rows
        if marker != required
    ]
    assert not disagree, (
        "the label and the HTML attribute disagree, so a sighted user and a "
        "screen-reader user are told opposite things:\n  " + "\n  ".join(disagree)
    )
