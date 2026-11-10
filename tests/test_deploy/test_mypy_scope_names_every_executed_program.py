"""Every workflow-executed Python directory is in mypy's scope, or named exempt (core#1581).

The ruff completeness guard enumerates tracked ``*.py``. Porting that onto mypy is the wrong
invariant: ``datanika/`` and ``tests/`` are linted and deliberately not type-checked (core#1288).
This file asks a narrower question — which programs do the workflows actually run — and requires
each resulting directory to be in ``[tool.mypy] files`` or in :data:`EXEMPT` with a reason.

Two measurements in the issue are not implemented here, on purpose:

* "every ``# type: ignore`` sits inside the scope" is red on ``datanika/`` and ``tests/`` and
  was blind to ``.github/scripts`` (that tree had zero ignores). It is not this guard.
* A path is relative to the step's ``working-directory``, then checkout prefixes
  (``core/``, ``datanika/``, ``build-context/datanika/``) come off. Skipping either one reports
  a covered directory that mypy does not check, or a phantom directory that does not exist.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
PYPROJECT = REPO_ROOT / "pyproject.toml"

# Longest first. ``datanika/`` must not eat ``datanika-mcp/``.
_CHECKOUT_PREFIXES = (
    "build-context/datanika/",
    "core/",
    "datanika/",
)

_INTERPRETER = re.compile(r"(?<![\w.-])(?:[\w./\\-]+/)?python(?:3(?:\.\d+)?)?(?![\w.-])\s+(\S+)")

#: Executed trees mypy is recorded as not checking, and why.
#:
#: An entry is a decision, not coverage. The working-directory control classifies
#: ``e2e/scripts/detect_flaky_gating.py`` as uncovered against a ``files`` list that
#: names ``scripts``. Delete an entry in the same change that adds its directory to
#: ``[tool.mypy] files`` — leaving it afterwards hides a later removal.
EXEMPT: dict[str, str] = {
    "e2e/scripts": (
        "Workflows execute this tree (the flaky-gating detector, the informational-tier "
        "reader, the SSO seeder, the SSO and overage coverage assertions, the overage "
        "failure classifier). [tool.mypy] files does not name it: pyproject records that "
        "as its own change rather than a fold into the list this guard reads (core#1571 "
        "AC4). This entry names that decision. It does not type-check the tree."
    ),
}


def _code_without_comment(line: str) -> str:
    """The line with a leading or trailing comment removed. Quotes keep their ``#``."""
    if line.lstrip().startswith("#"):
        return ""
    out: list[str] = []
    quote = ""
    for char in line:
        if quote:
            if char == quote:
                quote = ""
            out.append(char)
            continue
        if char in "'\"":
            quote = char
            out.append(char)
            continue
        if char == "#":
            break
        out.append(char)
    return "".join(out)


def program_tokens(line: str) -> list[str]:
    """``.py`` paths a line executes. A comment and a pytest argument answer nothing.

    The interpreter is the anchor. ``pytest tests/foo.py`` and ``python -m pytest tests/foo.py``
    carry a ``.py`` token and execute no program file. ``python -c`` and ``python -`` (a
    heredoc) likewise. A ``python -m`` module is not a path — mapping it onto ``datanika/``
    would be the widening core#1288 refused.
    """
    code = _code_without_comment(line)
    if not code.strip():
        return []
    found: list[str] = []
    for match in _INTERPRETER.finditer(code):
        token = match.group(1).strip("'\"")
        if token.startswith("-"):
            continue
        token = token.rstrip("\\").strip("'\"")
        if token.endswith(".py"):
            found.append(token)
    return found


def _clean_relative(token: str) -> str:
    parts: list[str] = []
    for part in token.replace("\\", "/").split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/".join(parts)


def _normalise_checkout_prefix(path: str) -> str:
    for prefix in _CHECKOUT_PREFIXES:
        if path.startswith(prefix):
            return path[len(prefix) :]
    return path


def resolve_program(token: str, working_directory: str) -> str:
    """Repo-relative path. Join ``working_directory`` first, then drop checkout prefixes.

    Skipping the join is what reported ``scripts/detect_flaky_gating.py`` covered. That
    step's directory is ``e2e``.
    """
    relative = _clean_relative(token)
    wd = _clean_relative(working_directory)
    if wd:
        relative = _clean_relative(f"{wd}/{relative}")
    return _normalise_checkout_prefix(relative)


def program_directory(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else ""


def directory_named(directory: str, tokens: list[str] | set[str]) -> bool:
    """Anchored prefix. ``scripts`` covers ``scripts``, not ``scripts_old`` or ``e2e/scripts``."""
    for token in tokens:
        name = token.strip().strip("/")
        if not name or name == ".":
            continue
        if directory == name or directory.startswith(name + "/"):
            return True
    return False


def _step_scripts(doc: Any) -> list[tuple[str, str]]:
    if not isinstance(doc, dict):
        return []
    jobs = doc.get("jobs")
    if not isinstance(jobs, dict):
        return []
    found: list[tuple[str, str]] = []
    for job in jobs.values():
        if not isinstance(job, dict):
            continue
        default_wd = ""
        defaults = job.get("defaults")
        if isinstance(defaults, dict):
            run_defaults = defaults.get("run")
            if isinstance(run_defaults, dict) and isinstance(
                run_defaults.get("working-directory"), str
            ):
                default_wd = run_defaults["working-directory"]
        steps = job.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict):
                continue
            script = step.get("run")
            if not isinstance(script, str):
                continue
            wd = step.get("working-directory")
            if not isinstance(wd, str) or not wd.strip():
                wd = default_wd
            found.append((wd, script))
    return found


def programs_in_document(doc: Any) -> set[str]:
    found: set[str] = set()
    for working_directory, script in _step_scripts(doc):
        for line in script.splitlines():
            for token in program_tokens(line):
                found.add(resolve_program(token, working_directory))
    return found


def programs_in_workflows(root: Path | None = None) -> set[str]:
    workflow_root = root or WORKFLOWS
    found: set[str] = set()
    for path in sorted(workflow_root.glob("*.yml")) + sorted(workflow_root.glob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        found.update(programs_in_document(doc))
    return found


def mypy_files(pyproject: Path | None = None) -> list[str]:
    document = tomllib.loads((pyproject or PYPROJECT).read_text(encoding="utf-8"))
    files = document.get("tool", {}).get("mypy", {}).get("files", [])
    if not isinstance(files, list):
        return []
    return [entry for entry in files if isinstance(entry, str)]


def uncovered_directories(
    programs: set[str],
    files: list[str],
    exempt: dict[str, str],
) -> list[str]:
    missing: list[str] = []
    for directory in sorted({program_directory(path) for path in programs}):
        if directory_named(directory, files):
            continue
        if directory_named(directory, set(exempt)):
            continue
        missing.append(directory)
    return missing


def exemptions_matching_nothing(directories: set[str], exempt: dict[str, str]) -> list[str]:
    return sorted(key for key in exempt if not directory_named(key.strip("/"), directories))


# ── controls first ───────────────────────────────────────────────────────────────────────


def test_a_comment_and_a_pytest_argument_answer_nothing() -> None:
    """The matcher is interpreter-anchored. A bare ``*.py`` grep answers on all three lines."""
    assert program_tokens("# python scripts/forgotten.py") == []
    assert program_tokens("pytest tests/test_deploy/test_mypy_gate.py -q") == []
    assert program_tokens("python -m pytest tests/test_deploy/test_mypy_gate.py") == []
    assert program_tokens("python3 -c \"open('hi.py')\"") == []
    assert program_tokens("python scripts/e2e_tier_streak.py --job e2e-staging") == [
        "scripts/e2e_tier_streak.py"
    ]


def test_a_root_relative_program_in_files_is_covered() -> None:
    """Positive. A resolver that reports every path uncovered passes the discriminating test
    for the wrong reason. This subject is the one that must come back covered.
    """
    path = resolve_program("scripts/e2e_tier_streak.py", "")
    assert path == "scripts/e2e_tier_streak.py"
    assert directory_named(program_directory(path), ["scripts", ".github/scripts"]) is True


def test_working_directory_is_not_the_scripts_entry() -> None:
    """Control 2. staging.yml runs the flaky detector under ``working-directory: e2e``.

    The token is ``scripts/detect_flaky_gating.py``. Resolved against that step it is
    ``e2e/scripts/detect_flaky_gating.py``, and ``files`` naming ``scripts`` does not cover
    it. A resolver that ignores ``working-directory`` reports the token covered.
    """
    doc = yaml.safe_load((WORKFLOWS / "staging.yml").read_text(encoding="utf-8"))
    hits = sorted(
        path for path in programs_in_document(doc) if path.endswith("detect_flaky_gating.py")
    )
    assert hits == ["e2e/scripts/detect_flaky_gating.py"], (
        f"resolved to {hits}. A ``scripts/`` hit means working-directory was ignored, and "
        "the scripts entry in [tool.mypy] files would then call this program covered."
    )
    files = ["scripts", ".github/scripts"]
    assert directory_named(program_directory(hits[0]), files) is False, (
        f"{hits[0]} was reported covered by {files}. That is false coverage: the file "
        "lives under e2e/scripts, which this list does not name."
    )
    assert (
        directory_named(program_directory(hits[0]), ["scripts", ".github/scripts", "e2e/scripts"])
        is True
    )


def test_checkout_prefixes_collapse_to_the_real_directory() -> None:
    """Four extracted directories are checkout names. They must not surface as gaps."""
    build = yaml.safe_load((WORKFLOWS / "build-push-image.yml").read_text(encoding="utf-8"))
    deploy = yaml.safe_load((WORKFLOWS / "deploy-pointer.yml").read_text(encoding="utf-8"))
    ci = yaml.safe_load((WORKFLOWS / "ci.yml").read_text(encoding="utf-8"))
    watch = yaml.safe_load((WORKFLOWS / "cve-watch.yml").read_text(encoding="utf-8"))
    files = ["scripts", ".github/scripts"]

    def _ending(doc: Any, name: str) -> list[str]:
        return sorted(path for path in programs_in_document(doc) if path.endswith(name))

    gate = _ending(build, "cross_tree_import_gate.py")
    assert gate == [".github/scripts/cross_tree_import_gate.py"], gate
    assert directory_named(program_directory(gate[0]), files) is True

    pairing = _ending(deploy, "cloud_pairing_gate.py")
    assert pairing == [".github/scripts/cloud_pairing_gate.py"], pairing
    assert directory_named(program_directory(pairing[0]), files) is True

    probe = _ending(ci, "probe_built_image.py")
    assert probe == ["scripts/probe_built_image.py"], probe
    assert directory_named(program_directory(probe[0]), files) is True

    report = _ending(watch, "cve_report.py")
    assert report == [".github/scripts/cve_report.py"], report
    assert directory_named(program_directory(report[0]), files) is True


def test_github_scripts_is_uncovered_when_files_names_only_scripts() -> None:
    """The guard would have gone red on ``.github/scripts`` before core#1571."""
    missing = uncovered_directories(
        {".github/scripts/promotion_refs.py", "scripts/e2e_tier_streak.py"},
        ["scripts"],
        {},
    )
    assert missing == [".github/scripts"]


def test_a_fabricated_directory_of_executed_python_is_uncovered() -> None:
    missing = uncovered_directories(
        {"other_dir/new_tool.py", "scripts/e2e_tier_streak.py"},
        ["scripts", ".github/scripts"],
        {},
    )
    assert missing == ["other_dir"]


def test_scripts_does_not_cover_a_lookalike_directory() -> None:
    assert directory_named("scripts_old", ["scripts"]) is False
    assert directory_named("e2e/scripts", ["scripts"]) is False


def test_an_exemption_that_matches_nothing_is_reported() -> None:
    dead = exemptions_matching_nothing({"scripts", ".github/scripts"}, {"nowhere": "gone"})
    assert dead == ["nowhere"]
    assert exemptions_matching_nothing({"e2e/scripts"}, {"e2e/scripts": "live"}) == []


def test_every_executed_directory_is_named_or_exempt() -> None:
    """The live scan. An empty population is not a pass — the floor is the same invocation."""
    programs = programs_in_workflows()
    assert len(programs) >= 15, (
        f"the workflow scan found {len(programs)} programs. This repository executes more "
        "than that, so a small number means the scanner is blind, not that the tree shrank."
    )
    assert "e2e/scripts/detect_flaky_gating.py" in programs
    assert "scripts/e2e_tier_streak.py" in programs
    assert ".github/scripts/cloud_pairing_gate.py" in programs
    files = mypy_files()
    missing = uncovered_directories(programs, files, EXEMPT)
    assert not missing, (
        f"{len(missing)} executed director(ies) are outside [tool.mypy] files "
        f"{files} and outside EXEMPT: {missing}. Programs searched: {sorted(programs)}."
    )


def test_no_exemption_matches_nothing_and_each_reason_is_written() -> None:
    programs = programs_in_workflows()
    directories = {program_directory(path) for path in programs}
    dead = exemptions_matching_nothing(directories, EXEMPT)
    assert not dead, (
        f"EXEMPT names {dead} and no executed program resolves under it. Delete the entry."
    )
    blank = [key for key, reason in EXEMPT.items() if not reason.strip()]
    assert not blank, f"EXEMPT entries with no reason: {blank}"


def test_the_guard_does_not_put_the_refused_trees_in_mypy_files() -> None:
    """core#1288. Satisfying the scan by adding these roots is the refusal, not a fix."""
    refused = {"datanika", "tests", "datanika-mcp", "e2e", "."}
    named = {entry.strip("/") for entry in mypy_files()}
    assert not (named & refused), named & refused
