"""Version-2 import of macros, Hub packages, snapshots, and singular tests.

SPEC_DBT_PROJECT_AND_DOWNSTREAM. Models stay in the transformations section.
A name ending in ``.py`` is the declared Python body this document refuses;
the document has no separate language field. Validation runs before any file
is written. A failure restores the org project tree the call found.
"""

import shutil
import tempfile
from pathlib import Path

from datanika.services.dbt_project import DbtProjectError, DbtProjectService, _validate_identifier

#: Keys that are a refusal, not ignored unknown keys.
REFUSED_IMPORT_KEYS = ("project_archive", "git_remote", "git_url", "files", "seeds")

#: Sections that need ``transformations:write`` even when the list is empty.
FILE_IMPORT_SECTIONS = ("macros", "packages", "snapshots", "tests")

_OWNED_MACRO = "generate_schema_name"
_HUB_PACKAGE_KEYS = frozenset({"package", "version"})


def refused_import_key_errors(data: dict) -> list[dict]:
    """One error when the document carries a key this import refuses.

    Returned alone by the caller, before the object cap and before any other
    validation, so a refusal creates nothing.
    """
    present = [key for key in REFUSED_IMPORT_KEYS if key in data]
    if not present:
        return []
    return [
        {
            "code": "REFUSED_KEY",
            "message": f"this import refuses {', '.join(present)}",
        }
    ]


def validate_project_sections(data: dict) -> list[dict]:
    """Errors for the project sections. Does not write files."""
    errors: list[dict] = []
    _validate_transformations(data, errors)
    _validate_named_sql(data, "macros", errors, refuse_owned_macro=True)
    _validate_named_sql(data, "tests", errors, refuse_owned_macro=False)
    _validate_packages(data, errors)
    _validate_snapshots(data, errors)
    return errors


def apply_project_files(org_id: int, data: dict) -> None:
    """Write the project files for a document that already validated.

    Raises ``DbtProjectError`` after restoring the previous tree when ``dbt
    deps`` fails or a writer raises. The caller's database transaction is
    what rolls the rows back; this function only puts the files back.
    """
    if not _writes_project_files(data):
        return
    from datanika.config import settings

    service = DbtProjectService(settings.dbt_projects_dir)
    project = service.get_project_path(org_id)
    snapshot = _capture_tree(project)
    try:
        service.ensure_project(org_id)
        _write_models(service, org_id, data.get("transformations") or [])
        for macro in data.get("macros") or []:
            service.write_macro(org_id, macro["name"], macro["sql_body"])
        for snap in data.get("snapshots") or []:
            service.write_snapshot(
                org_id,
                snap["snapshot_name"],
                snap["sql_body"],
                snap["unique_key"],
                strategy=snap["strategy"],
                updated_at=snap.get("updated_at"),
                check_cols=snap.get("check_cols"),
                target_schema=snap["target_schema"],
            )
        for singular in data.get("tests") or []:
            service.write_singular_test(org_id, singular["name"], singular["sql_body"])
        if "packages" in data and data["packages"] is not None:
            service.write_packages_yml(org_id, data["packages"])
            installed = service.install_packages(org_id)
            if not installed.get("success"):
                detail = installed.get("logs") or "dbt deps failed"
                raise DbtProjectError(detail)
    except Exception:
        _restore_tree(project, snapshot)
        raise
    finally:
        if snapshot is not None:
            shutil.rmtree(snapshot, ignore_errors=True)


def _write_models(service: DbtProjectService, org_id: int, transformations: list) -> None:
    for item in transformations:
        schema_name = item.get("schema_name") or "staging"
        materialization = item.get("materialization") or "view"
        service.write_model(
            org_id,
            item["name"],
            item["sql_body"],
            schema_name=schema_name,
            materialization=materialization,
            incremental_config=item.get("incremental_config"),
        )
        tests_config = item.get("tests_config")
        if tests_config:
            service.write_tests_config(org_id, item["name"], tests_config, schema_name=schema_name)


def _writes_project_files(data: dict) -> bool:
    if data.get("transformations") or data.get("macros") or data.get("snapshots"):
        return True
    if data.get("tests"):
        return True
    return "packages" in data and data["packages"] is not None


def _capture_tree(project: Path) -> Path | None:
    if not project.exists():
        return None
    dest = Path(tempfile.mkdtemp(prefix="dbt-import-"))
    shutil.copytree(project, dest / "tree")
    return dest


def _restore_tree(project: Path, snapshot: Path | None) -> None:
    if project.exists():
        shutil.rmtree(project)
    if snapshot is not None:
        shutil.copytree(snapshot / "tree", project)


def _list_section(data: dict, section: str, errors: list[dict]) -> list:
    if section not in data or data[section] is None:
        return []
    value = data[section]
    if not isinstance(value, list):
        errors.append({"code": "INVALID_SHAPE", "message": f"{section} must be a list"})
        return []
    return value


def _identifier_error(section: str, index: int, label: str, name) -> dict | None:
    try:
        _validate_identifier(str(name), label)
    except DbtProjectError as exc:
        return {"code": "INVALID_NAME", "message": f"{section}[{index}]: {exc}"}
    return None


def _python_name_error(section: str, index: int, name) -> dict | None:
    if isinstance(name, str) and name.endswith(".py"):
        return {
            "code": "REFUSED_PYTHON",
            "message": (
                f"{section}[{index}]: '{name}' is a Python body, which this import refuses"
            ),
        }
    return None


def _missing(section: str, index: int, field: str) -> dict:
    return {"code": "MISSING_FIELD", "message": f"{section}[{index}]: {field} is required"}


def _validate_transformations(data: dict, errors: list[dict]) -> None:
    for index, item in enumerate(_list_section(data, "transformations", errors)):
        if not isinstance(item, dict):
            errors.append(_missing("transformations", index, "object"))
            continue
        name = item.get("name")
        python = _python_name_error("transformations", index, name)
        if python is not None:
            errors.append(python)
        schema_name = item.get("schema_name")
        if schema_name is not None:
            ident = _identifier_error("transformations", index, "Schema name", schema_name)
            if ident is not None:
                errors.append(ident)


def _validate_named_sql(
    data: dict, section: str, errors: list[dict], *, refuse_owned_macro: bool
) -> None:
    for index, item in enumerate(_list_section(data, section, errors)):
        if not isinstance(item, dict):
            errors.append(_missing(section, index, "object"))
            continue
        name = item.get("name")
        if not name or not str(name).strip():
            errors.append(_missing(section, index, "name"))
        else:
            python = _python_name_error(section, index, name)
            if python is not None:
                errors.append(python)
            elif refuse_owned_macro and name == _OWNED_MACRO:
                errors.append(
                    {
                        "code": "REFUSED_MACRO",
                        "message": (
                            f"{section}[{index}]: {_OWNED_MACRO} is owned by the project "
                            "and cannot be imported"
                        ),
                    }
                )
            else:
                ident = _identifier_error(section, index, "Name", name)
                if ident is not None:
                    errors.append(ident)
        if not item.get("sql_body"):
            errors.append(_missing(section, index, "sql_body"))


def _validate_packages(data: dict, errors: list[dict]) -> None:
    for index, item in enumerate(_list_section(data, "packages", errors)):
        if not isinstance(item, dict) or set(item.keys()) != _HUB_PACKAGE_KEYS:
            errors.append(
                {
                    "code": "REFUSED_PACKAGE",
                    "message": (
                        f"packages[{index}]: a package must be Hub package and version, "
                        "and nothing else"
                    ),
                }
            )
            continue
        package = item.get("package")
        version = item.get("version")
        if not isinstance(package, str) or not package.strip():
            errors.append(_missing("packages", index, "package"))
        if not isinstance(version, str) or not version.strip():
            errors.append(_missing("packages", index, "version"))


def _validate_snapshots(data: dict, errors: list[dict]) -> None:
    for index, item in enumerate(_list_section(data, "snapshots", errors)):
        if not isinstance(item, dict):
            errors.append(_missing("snapshots", index, "object"))
            continue
        for field in ("snapshot_name", "sql_body", "unique_key", "strategy", "target_schema"):
            if not item.get(field):
                errors.append(_missing("snapshots", index, field))
        strategy = item.get("strategy")
        if strategy is not None and strategy not in ("timestamp", "check"):
            errors.append(
                {
                    "code": "INVALID_ENUM_VALUE",
                    "message": f"snapshots[{index}]: invalid strategy '{strategy}'",
                }
            )
        if strategy == "timestamp" and not item.get("updated_at"):
            errors.append(_missing("snapshots", index, "updated_at"))
        if strategy == "check" and not item.get("check_cols"):
            errors.append(_missing("snapshots", index, "check_cols"))
        for field, label in (
            ("snapshot_name", "Snapshot name"),
            ("unique_key", "Unique key"),
            ("target_schema", "Target schema"),
            ("updated_at", "Updated at column"),
        ):
            value = item.get(field)
            if value:
                ident = _identifier_error("snapshots", index, label, value)
                if ident is not None:
                    errors.append(ident)
        check_cols = item.get("check_cols")
        if isinstance(check_cols, list):
            for column in check_cols:
                ident = _identifier_error("snapshots", index, "Check column", column)
                if ident is not None:
                    errors.append(ident)
