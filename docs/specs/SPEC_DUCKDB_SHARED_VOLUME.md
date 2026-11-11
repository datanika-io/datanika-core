# SPEC — Shared volume for the DuckDB destination path

**Author**: Product · **Date**: 2026-11-11 · **Status**: contract, ready for Engineering
**Tracking**: [core#793](https://github.com/datanika-io/datanika-core/issues/793)
**Implementation**: Engineering. This spec does not add the mount, the claim, or the backup line.
**Verified against**: core `origin/dev` @ `d82da770dd3bd1afc1f16ea6594844d6f7c84ec0`.
Landing `origin/dev` @ `86584703aaead55c8c2049441780dba07d1d3429` still tells the
reader to add the volume. Re-derive both before implementing; line numbers below
are from that core revision.

---

## Decision

Ship the volume the DuckDB guide already names.

- Prod compose key `duckdb_data`, mounted at `/var/datanika/duckdb`.
- The mount is on `app`, `app_b`, and `celery`.
- Staging compose mounts a volume at the same path on those three services.
  The key is `staging_duckdb`, beside `staging_dbt` and `staging_uploads`.
  Staging is started with `docker compose -p datanika-staging`, so its runtime
  volume name is the staging project's, and it stays off the prod backup list.
- Helm mounts `/var/datanika/duckdb` on the app Deployment and the celery
  Deployment, from a new `ReadWriteMany` claim.

The prod project directory is `datanika`, so compose key `duckdb_data` is the
Docker volume `datanika_duckdb_data`.

`/app/uploaded_files` and `/app/dbt_projects` stay what they are. A DuckDB file
does not go on either.

## Why those two paths are the wrong store

On this revision the web process and the worker already share exactly two file
volumes, and the inactive colour shares them too.

| Service | Mounts |
|---|---|
| `app` | `dbt_projects:/app/dbt_projects`, `uploaded_files:/app/uploaded_files` (`docker-compose.yml` 192, 195) |
| `app_b` | the same two (`docker-compose.yml` 281, 287) |
| `celery` | the same two (`docker-compose.yml` 328, 329) |

`deploy/staging/docker-compose.yml` mounts `staging_dbt:/app/dbt_projects` and
`staging_uploads:/app/uploaded_files` on `app` (92–93), `app_b` (158–159), and
`celery` (188–189). Helm `app-deployment.yaml` (51, 55) and
`celery-deployment.yaml` (42, 47) mount only those two paths. Nothing mounts
`/var/datanika`. `deploy/server/staging-docker-compose.yml` is absent on this
revision. Leave it absent.

`/app/uploaded_files` is `FILE_UPLOADS_DIR`, the upload-archive store. A
warehouse file beside those archives mixes two lifecycles. The 2026-09-24 note
on core#793 reported this path and left the choice open for that reason.

`/app/dbt_projects` holds `tenant_{org_id}` dbt projects. The one production
DuckDB file that has ever previewed sits at
`/app/dbt_projects/_docs_samples/warehouse.duckdb` (connection 14).
`deploy/server/backup-offsite.sh` records why that tree is on the backup list:
`_docs_samples/` is not a dbt project, it was 92% of the volume, and a path a
user typed inside the volume made the volume a store of record. That file works
because its author ignored the guide. It is the example of the wrong path.

## Why this volume

QA walked the guide's Step 1 stanza on 2026-09-25 (landing#723, core ancestor
`04c82247`): `duckdb_data` at `/var/datanika/duckdb` on `app` and `celery`.

- The two-container probe passed in both directions.
- A file at `/var/datanika/NOT-SHARED.probe` was visible in `app` and absent in
  `celery`, so `/var/datanika` itself is the image layer and only the mounted
  subdirectory is shared.
- A file on `/app/uploaded_files` was visible in both, so the probe can see a
  share when one exists.
- The run landed 7 rows. Both containers saw `analytics.duckdb` at 1 323 008
  bytes. Data preview in the web app showed Rows: 7.

Stock compose on `d82da770` still has no `duckdb` volume and no `/var/datanika`
mount. The path the guide recommends is still a path the web process cannot read.

Keep the mount path `/var/datanika/duckdb`. Keep the prod compose key
`duckdb_data`. Both are what the guide and that walk already used.

## What Engineering ships

### Prod compose

In `docker-compose.yml`:

- Top-level volume `duckdb_data`.
- `duckdb_data:/var/datanika/duckdb` on `app`, `app_b`, and `celery`.

`beat` and `scheduler` do not open the DuckDB file. `scheduler` starts at line
488 and does not mount the file volumes. Leave both services without this mount.

`app_b` is the colour that is down between deploys. Data preview runs in
whichever colour is serving. Both colours carry the mount, so a swap keeps
opening the same file.

### Staging

In `deploy/staging/docker-compose.yml`, `app`, `app_b`, and `celery` mount
`staging_duckdb:/var/datanika/duckdb`, and the volume is declared beside
`staging_dbt` and `staging_uploads`.

### Helm

A new claim in the shape of `deploy/helm/datanika/templates/uploads-pvc.yaml`:
`accessModes: [ReadWriteMany]`. Two Deployments mount it, and
`app.replicaCount` can be greater than 1. A claim the worker cannot see is the
`FileNotFoundError` that template's comment already names for the CSV → DuckDB
first run.

`app-deployment.yaml` and `celery-deployment.yaml` gain
`mountPath: /var/datanika/duckdb`. `beat-deployment.yaml` keeps
`/app/beat_state` and does not gain this path.

Capacity is a values key beside `.Values.app.uploadedFilesSize`
(`values.yaml`, `uploadedFilesSize: 5Gi` on this revision). Engineering picks
the number. This spec does not.

### The parity test stays a derivation

`tests/test_deploy/test_deployment_manifest_parity.py` takes every named-volume
container path mounted on both compose `app` and compose `celery`, and requires
`mountPath: {path}` in both Helm templates. Bind mounts are excluded. Adding
`duckdb_data` to those two services without the Helm mount fails
`test_helm_shares_every_directory_compose_shares`. Leave the derivation as it
is. Do not special-case `/var/datanika/duckdb` out of it.

`len(shared) >= 2` is the empty-parse floor. Leave it. It still passes if a
third shared path disappears and the original two remain, which is why the Helm
assertion is the guard that has to keep deriving the set.

`app_b` is outside that intersection (the test says so at line 40). No test
under `tests/test_deploy/` fails when `app_b` omits a named-volume path that
`app` and `celery` both mount. This change adds that guard, for every such
path, including this one.

The parity test reads prod `docker-compose.yml` and the Helm templates. It does
not read `deploy/staging/docker-compose.yml`. Staging's three mounts are a
requirement of this spec on their own.

### Backup and the restore drill

`deploy/server/backup-offsite.sh` line 62:

```
FILE_VOLUMES="datanika_uploaded_files datanika_dbt_projects"
```

Extend that assignment with `datanika_duckdb_data`.

`deploy/server/restore-drill.sh` line 198:

```
VOLUMES="datanika_uploaded_files datanika_dbt_projects"
```

Extend that assignment with the same name. The two lists stay the same set.
Leave the staging volume off both lists.

`tests/test_deploy/test_backup_encryption.py` pins those lines, and one of the
pins is deliberately loose:

- `archives_the_referenced_volumes` requires
  `{"datanika_uploaded_files", "datanika_dbt_projects"} <=` the `FILE_VOLUMES`
  assignment. A third name does not fail it, and removing the third name later
  would not fail it either. Require `datanika_duckdb_data` in that set.
- `drill_exercises_both_volume_artifacts` matches the exact line
  `^VOLUMES="datanika_uploaded_files datanika_dbt_projects"$`. Point that match
  at the extended assignment.
- The mutations `archives_the_referenced_volumes` and
  `drill_exercises_both_volume_artifacts` replace those exact strings with a
  shorter list. Update the strings they search for so each mutation still
  removes a required member and the predicate goes false.

Match the assignment, the way those predicates already do. A check that the
name merely occurs in the file stays green after the assignment drops it,
because the comment above the loop explains the volume. That is the defect the
test's own comment records.

A listed volume with no readable mountpoint aborts (`exit 1`). The list and
the mount ship together. A backup that runs after the script is on the box and
before compose has created `datanika_duckdb_data` aborts. That is the current
missing-volume behavior. A missing volume stays a failure.

### Deploy

`app`, `app_b`, and `celery` are already named by the deploy. Compose creates
`duckdb_data` when a service that mounts it is started.
`tests/test_deploy/test_deploy_service_coverage.py` checks that each compose
service is named by a deploy step. It does not check volumes. Leave the deploy
workflow without a step whose only effect is to create an empty volume.

## Out of scope

- `DATANIKA_ALLOW_LOCAL_FILE_PATHS` stays as it is. `SPEC_LOCAL_FILE_CONNECTIONS.md`
  D4 refuses a local filesystem path at connection create/save when that flag
  is false, and production sets it false. This volume does not make a DuckDB
  destination available on the hosted app. The guide's Cloud callout stays
  until a separate decision.
- The DuckDB guide is unchanged in the mount change. After the mount is on
  core `origin/dev`, Product edits Step 1 so it names the shipped path and
  stops telling the reader to insert a volume. The two-container probe stays
  the mount check. Test Connection stays red for a brand-new file: QA measured
  that a missing parent directory is not the reason (landing#723).
- Refusing a DuckDB path outside this mount (core#793's second proposal) is a
  different change. On the hosted app, D4 already refuses every local path.
- `duckdb/04-first-run.png` stays where it is. Recapturing it is landing#395.
- Connection 14 stays at
  `/app/dbt_projects/_docs_samples/warehouse.duckdb`. This spec does not move it.

## Done, from the user's side

A self-hoster sets Database Path to `/var/datanika/duckdb/analytics.duckdb` and
leaves compose alone.

- Rows a load writes are visible in Data preview, in the serving web process.
- The file is still there after the containers are recreated.
- The inactive colour mounts the same path, so a blue/green swap opens the same
  file.

The guide edit that stops asking the reader to add the volume waits until this
mount is on core `origin/dev`.

## Acceptance

- [ ] `docker-compose.yml` declares `duckdb_data` and mounts it at
      `/var/datanika/duckdb` on `app`, `app_b`, and `celery`.
- [ ] `beat` and `scheduler` do not mount it.
- [ ] `deploy/staging/docker-compose.yml` mounts `staging_duckdb` at that path
      on `app`, `app_b`, and `celery`.
      `deploy/server/staging-docker-compose.yml` is still absent.
- [ ] Helm app and celery mount `/var/datanika/duckdb` from a `ReadWriteMany`
      claim. Capacity is a values key beside `uploadedFilesSize`. Beat does
      not mount the path.
- [ ] `test_deployment_manifest_parity.py` still derives shared paths. Helm
      omitting the new path fails it. The derivation is unchanged.
- [ ] `app_b` omitting a named-volume path that `app` and `celery` share fails
      a test this change adds.
- [ ] `FILE_VOLUMES` and the drill's `VOLUMES` both include
      `datanika_duckdb_data` beside the two existing names, and
      `test_backup_encryption.py` requires the new name on the assignment.
      The staging volume is on neither list.
- [ ] `DATANIKA_ALLOW_LOCAL_FILE_PATHS` is unchanged, and `duckdb.md` is
      unchanged, in the mount pull request.
