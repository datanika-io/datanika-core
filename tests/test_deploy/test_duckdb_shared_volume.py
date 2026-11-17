"""The DuckDB file has its own shared volume (core#793).

SPEC_DUCKDB_SHARED_VOLUME. Compose is the reference: `app`, `app_b`, and
`celery` mount `duckdb_data` at `/var/datanika/duckdb`. Staging uses its own
key, `staging_duckdb`, at the same path. Helm mounts that path from a
ReadWriteMany claim. The file does not go on `uploaded_files` or `dbt_projects`.
"""

import copy
from pathlib import Path

import yaml

from tests.test_deploy.test_deployment_manifest_parity import _named_volume_mounts

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
STAGING = ROOT / "deploy" / "staging" / "docker-compose.yml"
CHART = ROOT / "deploy" / "helm" / "datanika"
DUCKDB_PATH = "/var/datanika/duckdb"
PROD_MOUNT = f"duckdb_data:{DUCKDB_PATH}"
TIERS = ("app", "app_b", "celery")
ABSENT = ("beat", "scheduler")


def _compose(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _mounts(service: dict) -> dict[str, str]:
    return _named_volume_mounts(service)


def _shared_by_app_and_celery(compose: dict) -> set[str]:
    services = compose["services"]
    web = set(_mounts(services["app"]))
    worker = set(_mounts(services["celery"]))
    return web & worker


def test_prod_declares_duckdb_data_on_the_three_tiers():
    compose = _compose(COMPOSE)
    assert "duckdb_data" in compose["volumes"]
    for name in TIERS:
        mounts = _mounts(compose["services"][name])
        assert mounts.get(DUCKDB_PATH) == "duckdb_data", name
        assert PROD_MOUNT in (compose["services"][name].get("volumes") or [])


def test_beat_and_scheduler_do_not_mount_the_duckdb_path():
    compose = _compose(COMPOSE)
    for name in ABSENT:
        assert DUCKDB_PATH not in _mounts(compose["services"][name])


def test_a_duckdb_mount_on_beat_is_visible():
    compose = _compose(COMPOSE)
    beat = copy.deepcopy(compose["services"]["beat"])
    beat.setdefault("volumes", []).append(PROD_MOUNT)
    assert DUCKDB_PATH in _mounts(beat)


def test_app_b_mounts_every_named_volume_path_app_and_celery_share():
    compose = _compose(COMPOSE)
    app_b = set(_mounts(compose["services"]["app_b"]))
    missing = _shared_by_app_and_celery(compose) - app_b
    assert not missing, missing


def test_omitting_a_shared_path_on_app_b_fails_the_guard():
    compose = _compose(COMPOSE)
    services = compose["services"]
    shared = _shared_by_app_and_celery(compose)
    assert shared, "the guard has nothing to omit"
    dropped = sorted(shared)[0]
    app_b = copy.deepcopy(services["app_b"])
    app_b["volumes"] = [
        entry
        for entry in app_b.get("volumes") or []
        if not (isinstance(entry, str) and entry.split(":", 1)[-1].split(":")[0] == dropped)
    ]
    missing = shared - set(_mounts(app_b))
    assert missing == {dropped}


def test_staging_mounts_its_own_key_at_the_same_path():
    compose = _compose(STAGING)
    assert "staging_duckdb" in compose["volumes"]
    for name in TIERS:
        mounts = _mounts(compose["services"][name])
        assert mounts.get(DUCKDB_PATH) == "staging_duckdb", name
    for name in ABSENT:
        assert DUCKDB_PATH not in _mounts(compose["services"][name])
    absent = ROOT / "deploy" / "server" / "staging-docker-compose.yml"
    assert not absent.exists()


def test_helm_mounts_the_path_from_a_read_write_many_claim():
    pvc = (CHART / "templates" / "duckdb-pvc.yaml").read_text(encoding="utf-8")
    assert "ReadWriteMany" in pvc
    assert ".Values.app.duckdbSize" in pvc
    values = (CHART / "values.yaml").read_text(encoding="utf-8")
    assert "duckdbSize:" in values
    app = (CHART / "templates" / "app-deployment.yaml").read_text(encoding="utf-8")
    celery = (CHART / "templates" / "celery-deployment.yaml").read_text(encoding="utf-8")
    beat = (CHART / "templates" / "beat-deployment.yaml").read_text(encoding="utf-8")
    for text in (app, celery):
        assert f"mountPath: {DUCKDB_PATH}" in text
        assert 'claimName: {{ include "datanika.fullname" . }}-duckdb' in text
    assert f"mountPath: {DUCKDB_PATH}" not in beat


def test_backup_lists_do_not_name_the_staging_volume():
    backup = (ROOT / "deploy" / "server" / "backup-offsite.sh").read_text(encoding="utf-8")
    drill = (ROOT / "deploy" / "server" / "restore-drill.sh").read_text(encoding="utf-8")
    for text, key in ((backup, "FILE_VOLUMES"), (drill, "VOLUMES")):
        assignment = next(line for line in text.splitlines() if line.startswith(f'{key}="'))
        assert "staging" not in assignment
        assert "datanika_duckdb_data" in assignment
