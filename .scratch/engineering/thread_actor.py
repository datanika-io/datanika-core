"""One-off: thread `actor_user_id` into calls that core#681 made require it.

AST-driven rather than regex, because these call sites are formatted every way — single
line, multi-line, with and without trailing commas — and a regex that handles four of the
five shapes silently skips the fifth. The parser knows where each call ends; I do not.

Inserts before the closing paren and lets `ruff format` reflow, so the insertion point does
not have to be pretty, only correct.

Refuses a call that already has the keyword, so it is idempotent.
"""

from __future__ import annotations

import ast
import pathlib
import sys

TARGETS = {"create_connection", "update_connection", "delete_connection", "import_backup"}
ACTOR = "actor_user_id=make_org_admin(db_session, org.id)"


def _call_name(node: ast.Call) -> str | None:
    f = node.func
    return getattr(f, "attr", None) or getattr(f, "id", None)


def patch(path: pathlib.Path) -> int:
    raw = path.read_bytes()
    nl = "\r\n" if raw.count(b"\r\n") else "\n"
    src = raw.replace(b"\r\n", b"\n").decode("utf-8")
    tree = ast.parse(src)
    lines = src.split("\n")

    # byte offset of the start of each line, to convert (lineno, col) -> absolute index
    starts, acc = [], 0
    for ln in lines:
        starts.append(acc)
        acc += len(ln) + 1

    edits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if _call_name(node) not in TARGETS:
            continue
        if any(k.arg == "actor_user_id" for k in node.keywords):
            continue
        end = starts[node.end_lineno - 1] + node.end_col_offset  # just past ')'
        assert src[end - 1] == ")", f"{path}:{node.lineno} does not end in ')'"
        edits.append(end - 1)

    if not edits:
        return 0
    for pos in sorted(edits, reverse=True):
        before = src[:pos].rstrip()
        sep = " " if before.endswith(",") else ", "
        src = src[:pos] + sep + ACTOR + src[pos:]

    path.write_bytes(src.replace("\n", nl).encode("utf-8"))
    return len(edits)


if __name__ == "__main__":
    total = 0
    for arg in sys.argv[1:]:
        n = patch(pathlib.Path(arg))
        print(f"  {arg}: {n} call(s) threaded")
        total += n
    print(f"  total {total}")
