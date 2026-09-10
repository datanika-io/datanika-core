"""One-off: thread `actor_user_id` into calls that core#681 made require it.

AST-driven rather than regex, because these call sites are formatted every way — single
line, multi-line, with and without trailing commas — and a regex that handles four of the
five shapes silently skips the fifth. The parser knows where each call ends; I do not.

🔑 **The actor is DERIVED from the call's own first two positional arguments**, not from a
fixed `org.id`. Every target has the shape `(session, org_id, ...)`, so the actor is built as
`make_org_admin(<that session>, <that org>)` — an admin of the org **this call targets**.

That is not tidiness. The first version substituted a blanket `org.id` and produced an actor
who was an admin of the wrong org in a test that creates in `other_org`; the service refused,
correctly, because authority does not travel between orgs. Deriving it makes that class of
mistake unrepresentable rather than something to notice.

Skips (and reports) any call it cannot read that way, rather than guessing — a call whose org
arrives by keyword needs a human, and silently threading the wrong actor into it would be the
same bug the derivation exists to prevent.

Idempotent: a call that already has the keyword is left alone.
"""

from __future__ import annotations

import ast
import pathlib
import sys

TARGETS = {
    "create_connection", "update_connection", "delete_connection", "import_backup",
    "create_upload", "update_upload", "delete_upload",
    "create_pipeline", "update_pipeline", "delete_pipeline",
    "create_schedule", "update_schedule", "delete_schedule", "toggle_active",
}

#: Namesakes live here: MCP exposes TOOLS with these names and unrelated signatures, and
#: Reflex state classes expose HANDLERS with them. Nothing in the syntax distinguishes
#: either from the service method, so they are excluded by path rather than detected.
EXCLUDE = ("tests/test_mcp",)


def _call_name(node: ast.Call) -> str | None:
    f = node.func
    return getattr(f, "attr", None) or getattr(f, "id", None)


def patch(path: pathlib.Path) -> tuple[int, list[str]]:
    if any(d in path.as_posix() for d in EXCLUDE):
        return 0, [f"{path.name}: EXCLUDED (namesakes -- MCP tools, not the service)"]
    raw = path.read_bytes()
    nl = "\r\n" if raw.count(b"\r\n") else "\n"
    src = raw.replace(b"\r\n", b"\n").decode("utf-8")
    tree = ast.parse(src)
    lines = src.split("\n")

    starts, acc = [], 0
    for ln in lines:
        starts.append(acc)
        acc += len(ln) + 1

    edits: list[tuple[int, str]] = []
    skipped: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node) not in TARGETS:
            continue
        if any(k.arg == "actor_user_id" for k in node.keywords):
            continue
        if len(node.args) < 2:
            skipped.append(f"{path.name}:{node.lineno} ({_call_name(node)}) — org not positional")
            continue
        sess = ast.get_source_segment(src, node.args[0])
        org = ast.get_source_segment(src, node.args[1])
        if not sess or not org:
            skipped.append(f"{path.name}:{node.lineno} — could not read args")
            continue
        end = starts[node.end_lineno - 1] + node.end_col_offset
        assert src[end - 1] == ")", f"{path}:{node.lineno} does not end in ')'"
        edits.append((end - 1, f"actor_user_id=make_org_admin({sess}, {org})"))

    for pos, text in sorted(edits, reverse=True):
        before = src[:pos].rstrip()
        sep = " " if before.endswith(",") else ", "
        src = src[:pos] + sep + text + src[pos:]

    if edits:
        path.write_bytes(src.replace("\n", nl).encode("utf-8"))
    return len(edits), skipped


if __name__ == "__main__":
    total, all_skipped = 0, []
    for arg in sys.argv[1:]:
        n, sk = patch(pathlib.Path(arg))
        if n or sk:
            print(f"  {arg}: {n} threaded, {len(sk)} skipped")
        total += n
        all_skipped += sk
    print(f"  total threaded: {total}")
    if all_skipped:
        print(f"  SKIPPED {len(all_skipped)} — need a human:")
        for s in all_skipped:
            print(f"    {s}")
