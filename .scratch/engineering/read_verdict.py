"""Read a suite verdict, and REFUSE rather than infer.

Three times in one session I read a status that was about the wrapper rather than the tests:

1. the exit code belonged to ``grep`` (a pipeline's status is its last stage's);
2. the exit code belonged to the wrapper script, pytest's own captured into a variable whose
   echo was truncated out of the notification;
3. a verdict file that **existed and was empty**, because it was truncated at the start of a
   24-minute run.

The third is the worst, and that is the point: ``grep -c failed empty.txt`` returns ``0`` and
reads as *"no failures."* An absent measurement is not a passing one — the same shape as
`FIELD_HAZARDS`'s two ways a probe inside a container measures nothing and prints a clean zero.

So this refuses on every ambiguous state instead of resolving it in the comfortable direction:

* file missing, empty, or unparseable  -> REFUSE
* ``pytest_exit_code`` key absent      -> REFUSE
* value ``UNKNOWN`` (the sentinel a killed run leaves behind) -> REFUSE
* exit code 0 but FAILED/ERROR lines present -> REFUSE, because the two disagree and the
  disagreement is exactly what the first two bugs looked like

Exit codes here: 0 green, 1 red, 2 refuse-to-say. **2 is not a pass**, and the caller must
treat it as such — the shape `scripts/verify_e2e_attribution.py` already uses for QA's tiers.
"""

from __future__ import annotations

import pathlib
import sys

REQUIRED = ("pytest_exit_code", "failed_lines", "error_lines", "summary")


def main(path_str: str) -> int:
    path = pathlib.Path(path_str)
    if not path.exists():
        print(f"REFUSE: {path} does not exist — the run did not get far enough to write it")
        return 2
    raw = path.read_text(encoding="utf-8", errors="replace").strip()
    if not raw:
        print(f"REFUSE: {path} is EMPTY (0 bytes). An incomplete run is not a green one.")
        return 2

    fields: dict[str, str] = {}
    for line in raw.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            fields[k.strip()] = v.strip()

    missing = [k for k in REQUIRED if k not in fields]
    if missing:
        print(f"REFUSE: missing {missing} in {path}. Absence is not absence-of-failure.")
        return 2

    rc = fields["pytest_exit_code"]
    if rc == "UNKNOWN":
        print("REFUSE: sentinel still present — the run was killed or timed out before finishing.")
        return 2
    try:
        rc_i = int(rc)
        failed = int(fields["failed_lines"])
        errors = int(fields["error_lines"])
    except ValueError:
        print(f"REFUSE: unparseable numbers in {path}: {fields}")
        return 2

    if rc_i == 0 and (failed or errors):
        print(
            f"REFUSE: exit code 0 but {failed} FAILED and {errors} ERROR lines. "
            "The two disagree, which is what a wrapper's status looks like."
        )
        return 2

    print(f"  exit={rc_i} failed={failed} errors={errors}")
    print(f"  {fields['summary']}")
    if rc_i != 0 or failed or errors:
        print("RED")
        return 1
    print("GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
