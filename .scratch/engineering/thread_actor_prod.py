"""Thread `actor_user_id` into PRODUCTION callers of the newly guarded methods (core#681).

Separate from `thread_actor.py` because the actor differs by surface and must be the *real*
one — never a test helper:

    api_v1_routes.py   -> api_key.user_id     (branch A: on REST the only actor is the key's owner)
    ui/state/*.py      -> user_id             (already in scope for the `_audit` row)
    backup_service.py  -> actor_user_id       (threaded in from `backup_state`)

⚠️ **`datanika/ui/pages/` is excluded, and that exclusion is the point.**
`on_click=UploadState.delete_upload(u.id)` is a Reflex event binding to a STATE HANDLER that
happens to share a name with the service method. Nothing in the syntax distinguishes them — the
same namesake trap that made a previous transform write
`make_org_admin("test", "postgres")` into an MCP tool call.

That is the general lesson this file exists to encode: **a syntactic transform is right about
call SHAPE and can be wrong about call INTENT.** So this one refuses to guess — it takes an
explicit actor expression per file and skips anything it has no rule for, printing what it
skipped so a human reads the remainder.
"""

from __future__ import annotations

import ast
import pathlib
import sys

TARGETS = {
    "create_upload", "update_upload", "delete_upload",
    "create_pipeline", "update_pipeline", "delete_pipeline",
    "create_schedule", "update_schedule", "delete_schedule", "toggle_active",
}

ACTOR_BY_FILE = {
    "api_v1_routes.py": "api_key.user_id",
    "upload_state.py": "user_id",
    "pipeline_state.py": "user_id",
    "schedule_state.py": "user_id",
    "backup_service.py": "actor_user_id",
}

#: Reflex event bindings to state handlers that share a service method's name.
EXCLUDE_DIRS = ("ui/pages",)


def _name(node: ast.Call) -> str | None:
    f = node.func
    return getattr(f, "attr", None) or getattr(f, "id", None)


def patch(path: pathlib.Path) -> tuple[int, list[str]]:
    posix = path.as_posix()
    if any(d in posix for d in EXCLUDE_DIRS):
        return 0, [f"{path.name}: EXCLUDED (Reflex event binding, not a service call)"]
    actor = ACTOR_BY_FILE.get(path.name)
    if actor is None:
        return 0, [f"{path.name}: no actor rule — skipped rather than guessed"]

    raw = path.read_bytes()
    nl = "\r\n" if raw.count(b"\r\n") else "\n"
    src = raw.replace(b"\r\n", b"\n").decode("utf-8")
    tree = ast.parse(src)
    lines = src.split("\n")
    starts, acc = [], 0
    for ln in lines:
        starts.append(acc)
        acc += len(ln) + 1

    edits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _name(node) not in TARGETS:
            continue
        if any(k.arg == "actor_user_id" for k in node.keywords):
            continue
        end = starts[node.end_lineno - 1] + node.end_col_offset
        assert src[end - 1] == ")", f"{path}:{node.lineno} does not end in ')'"
        edits.append(end - 1)

    for pos in sorted(edits, reverse=True):
        sep = " " if src[:pos].rstrip().endswith(",") else ", "
        src = src[:pos] + sep + f"actor_user_id={actor}" + src[pos:]

    if edits:
        path.write_bytes(src.replace("\n", nl).encode("utf-8"))
    return len(edits), []


if __name__ == "__main__":
    total, skipped = 0, []
    for arg in sys.argv[1:]:
        n, sk = patch(pathlib.Path(arg))
        if n:
            print(f"  {arg}: {n} threaded")
        total += n
        skipped += sk
    print(f"  total: {total}")
    for s in skipped:
        print(f"  {s}")
