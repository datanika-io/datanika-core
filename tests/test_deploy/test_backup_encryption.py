"""Invariants of the encrypted off-site backup path (core#675, core#748).

These pin `deploy/server/backup-offsite.sh` and `deploy/server/restore-drill.sh`.
Neither script is deployed by any workflow (core#747) -- they are hand-installed --
so these tests are the only mechanical thing standing between an edit and a silent
reintroduction of unencrypted production backups on a shared, unpatched host.

Every invariant below is paired with a mutation that must break it. A structural
assertion over script text is exactly the kind of check that passes because it
matches something incidental, so `test_*_mutation` re-runs each predicate against
text where the property has been removed and asserts it goes red. A predicate that
cannot fail is worth nothing here -- see `plans/engineering/AUDIT_TESTS_THAT_CANNOT_FAIL.md`.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKUP = REPO_ROOT / "deploy" / "server" / "backup-offsite.sh"
DRILL = REPO_ROOT / "deploy" / "server" / "restore-drill.sh"
PUBKEY = REPO_ROOT / "deploy" / "server" / "backup-pubkey.asc"


@pytest.fixture(scope="module")
def backup_text() -> str:
    return BACKUP.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def drill_text() -> str:
    return DRILL.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# predicates -- defined separately so the mutation tests can call them on
# altered text rather than duplicating the regex in two places.
# --------------------------------------------------------------------------


def _offsite_rsync_lines(text: str) -> list[str]:
    return [
        line.strip()
        for line in text.splitlines()
        if not line.strip().startswith("#") and "rsync" in line and "REMOTE" in line
    ]


def ships_only_ciphertext(text: str) -> bool:
    """EVERY rsync that reaches Aweb must carry a ciphertext, never a plaintext.

    ⚠️ This used to `return` on the FIRST off-site rsync it found, which was
    correct while there was exactly one. core#954 adds a second (the per-volume
    archives), and under the old form a plaintext rsync added *after* the dump's
    would have passed unexamined — the check would have gone on measuring the
    line that was already right. Two artifacts is the minimum; the count guard
    is what stops the loop's rsync silently disappearing.
    """
    lines = _offsite_rsync_lines(text)
    if len(lines) < 2:
        return False
    for stripped in lines:
        if '"${FILE}"' in stripped or '"${VTAR}"' in stripped:
            return False
        if '"${ENC}"' not in stripped and '"${VENC}"' not in stripped:
            return False
    return True


def size_gate_measures_plaintext(text: str) -> bool:
    """The >1000 byte floor must be taken on the dump, before encryption.

    core#675 asked for this explicitly: gpg adds a ~600 byte header, so an
    encrypted empty dump is not obviously small and the same numeric floor
    applied to ciphertext would be a weaker check wearing the same number.
    """
    size_at = text.find('SIZE=$(stat -c%s "${FILE}")')
    encrypt_at = text.find("--encrypt")
    return 0 <= size_at < encrypt_at


def verifies_roundtrip_before_shipping(text: str) -> bool:
    """Ciphertext must be decrypted and compared to the dump before it leaves."""
    match = re.search(r"--decrypt \"\$\{ENC\}\".*?cmp -s - \"\$\{FILE\}\"", text, re.S)
    if not match:
        return False
    rsync_at = text.find("rsync -az")
    return match.end() < rsync_at


def refuses_without_key(text: str) -> bool:
    """Pre-flight: no recipient key -> exit non-zero before pg_dump runs.

    Anchored on the real invocation, not on the word "pg_dump": the header
    comment above the guard contains that word, so a bare `.find("pg_dump")`
    matches prose and reports the guard as being in the wrong place. Same
    family as WORKFLOW_RULES section 4 -- count the instruction, not the phrase.
    """
    guard_at = text.find('gpg --list-keys "${GPG_RECIPIENT}"')
    dump_at = text.find("docker exec datanika-postgres pg_dump")
    return 0 <= guard_at < dump_at


def prunes_both_remote_patterns(text: str) -> bool:
    """'*.sql.gz' does not match '*.sql.gz.gpg' -- both sweeps must exist.

    Without the .gpg sweep the off-site copy grows without bound; without the
    plaintext sweep a legacy or regressed dump lingers in the clear.
    """
    return "-name '*.sql.gz.gpg' -mtime" in text and "-name '*.sql.gz' -delete" in text


def sends_no_key_to_aweb(text: str) -> bool:
    """Aweb must receive ciphertext and nothing else -- no key material, ever."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        transfers_off_site = ("rsync" in stripped or "scp" in stripped) and "REMOTE" in stripped
        key_tokens = ("gnupg", "privkey", "pubkey", ".asc", "GNUPGHOME")
        if transfers_off_site and any(tok in stripped for tok in key_tokens):
            return False
    return True


def archives_the_referenced_volumes(text: str) -> bool:
    """The dump references bytes it does not contain (core#954).

    `uploaded_files.archive_path` restores as a path into `datanika_uploaded_files`,
    and a live `duckdb` connection's `path` pointed into `datanika_dbt_projects`
    (prod connection id=14, measured 2026-09-03). A dump-only backup restores rows
    that name files nothing holds. Both volumes must be captured.

    ⚠️ Read off the ASSIGNMENT, not off the file. A bare `"datanika_dbt_projects"
    in text` is satisfied by the comment block above the loop that *explains* the
    volume — so dropping it from `FILE_VOLUMES` left the check green. Caught by
    this file's own mutation half, which is the second time that exact substring
    defect has been found in an Infra guard.
    """
    match = re.search(r'^FILE_VOLUMES="([^"]*)"', text, re.M)
    if not match:
        return False
    volumes = set(match.group(1).split())
    return {"datanika_uploaded_files", "datanika_dbt_projects"} <= volumes


def missing_volume_aborts(text: str) -> bool:
    """A volume with no readable mountpoint must abort, never be skipped.

    Silently skipping is the exact failure this change exists to end, and it would
    still write the freshness metric — so the "Backup Stale" alert would stay green
    over a backup that captured nothing.
    """
    match = re.search(
        r'if \[ -z "\$\{MP\}" \] \|\| \[ ! -d "\$\{MP\}" \]; then\n(.*?)\n    fi', text, re.S
    )
    return bool(match) and "exit 1" in match.group(1)


def volume_archive_is_verified_by_member_count(text: str) -> bool:
    """The per-artifact gate, and it must compare against an INDEPENDENT count.

    `SRC_FILES` is taken with `find` before the tar runs, so the expectation does
    not come from the archive it is checking. A byte floor cannot distinguish an
    empty volume (legitimate on a fresh install) from a tar that produced nothing;
    a member count can, and it fails on the direction that loses data.

    Ordering matters as much as presence: the check must precede the rsync, or a
    truncated archive ships and is graded afterwards.
    """
    count_at = text.find('if [ "${TAR_FILES}" -lt "${SRC_FILES}" ]')
    src_at = text.find('SRC_FILES=$(find "${MP}" -type f | wc -l)')
    tar_at = text.find('tar czf "${VTAR}"')
    rsync_at = text.find('rsync -az -e "ssh ${SSH_OPTS}" "${VENC}"')
    return 0 <= src_at < tar_at < count_at < rsync_at


def volume_ciphertext_is_round_tripped(text: str) -> bool:
    """Same round-trip the dump gets, for the same reason, on the same artifact."""
    match = re.search(r'--decrypt "\$\{VENC\}".*?cmp -s - "\$\{VTAR\}"', text, re.S)
    if not match:
        return False
    return match.end() < text.find('rsync -az -e "ssh ${SSH_OPTS}" "${VENC}"')


def prunes_volume_patterns(text: str) -> bool:
    """'*.sql.gz' does not match '*.tar.gz' — the volume artifacts need their own.

    Exactly the trap the script already documents one glob over: without these the
    off-site directory grows without bound, and a plaintext volume archive (which
    is *user data*, not just schema) could linger there in the clear.
    """
    return "-name '*.tar.gz.gpg' -mtime" in text and "-name '*.tar.gz' -delete" in text


def emits_a_per_volume_metric(text: str) -> bool:
    """A total cannot distinguish "uploads vanished" from "dbt projects grew".

    The freshness timestamp is written on the same successful run, so it stays
    green through a volume that stopped being captured. The per-volume COUNT is
    the series that discriminates.
    """
    # The metric lines are built inside a double-quoted shell string, so the
    # label quotes are backslash-escaped in the file. `\\?` accepts both spellings
    # rather than pinning the escaping, which is incidental to the invariant.
    return all(
        re.search(rf'{name}\{{volume=\\?"\$\{{VOL\}}\\?"\}}', text)
        for name in ("datanika_backup_last_files_count", "datanika_backup_last_files_size_bytes")
    )


def drill_reads_ciphertext(text: str) -> bool:
    return "*.sql.gz.gpg" in text and "ls -t ${REMOTE_DIR}/*.sql.gz 2>" not in text


def drill_fails_on_offsite_plaintext(text: str) -> bool:
    """A plaintext dump off-site is core#675 regressing; the drill must refuse."""
    match = re.search(r"PLAINTEXT_COUNT.*?\n.*?if \[ \"\$\{PLAINTEXT_COUNT\}\" != 0 \]", text, re.S)
    return bool(match) and "RESTORE DRILL FAIL" in text


def drill_has_no_plaintext_fallback(text: str) -> bool:
    """A failed decrypt must terminate the drill, never fall through.

    A fallback to an unencrypted dump would mask precisely the condition this
    drill exists to detect -- off-site copies that cannot be read.
    """
    match = re.search(r"if ! gpg [^\n]*--decrypt[^\n]*; then\n(.*?)\nfi", text, re.S)
    if not match:
        return False
    return "exit 1" in match.group(1)


PREDICATES = {
    "ships_only_ciphertext": (ships_only_ciphertext, "backup"),
    "size_gate_measures_plaintext": (size_gate_measures_plaintext, "backup"),
    "verifies_roundtrip_before_shipping": (verifies_roundtrip_before_shipping, "backup"),
    "refuses_without_key": (refuses_without_key, "backup"),
    "prunes_both_remote_patterns": (prunes_both_remote_patterns, "backup"),
    "sends_no_key_to_aweb": (sends_no_key_to_aweb, "backup"),
    "archives_the_referenced_volumes": (archives_the_referenced_volumes, "backup"),
    "missing_volume_aborts": (missing_volume_aborts, "backup"),
    "volume_archive_is_verified_by_member_count": (
        volume_archive_is_verified_by_member_count,
        "backup",
    ),
    "volume_ciphertext_is_round_tripped": (volume_ciphertext_is_round_tripped, "backup"),
    "prunes_volume_patterns": (prunes_volume_patterns, "backup"),
    "emits_a_per_volume_metric": (emits_a_per_volume_metric, "backup"),
    "drill_reads_ciphertext": (drill_reads_ciphertext, "drill"),
    "drill_fails_on_offsite_plaintext": (drill_fails_on_offsite_plaintext, "drill"),
    "drill_has_no_plaintext_fallback": (drill_has_no_plaintext_fallback, "drill"),
}

_ABORT_ECHO_TAIL = 'is the failure this change exists to end."'

# Each mutation removes exactly the property its predicate asserts.
MUTATIONS = {
    "ships_only_ciphertext": (
        'rsync -az -e "ssh ${SSH_OPTS}" "${ENC}"',
        'rsync -az -e "ssh ${SSH_OPTS}" "${FILE}"',
    ),
    "size_gate_measures_plaintext": ('SIZE=$(stat -c%s "${FILE}")', 'SIZE=$(stat -c%s "${ENC}")'),
    "verifies_roundtrip_before_shipping": ('--decrypt "${ENC}"', '--list-packets "${ENC}"'),
    "refuses_without_key": ('gpg --list-keys "${GPG_RECIPIENT}"', "true"),
    "prunes_both_remote_patterns": ("-name '*.sql.gz.gpg' -mtime", "-name '*.nope' -mtime"),
    "archives_the_referenced_volumes": (
        'FILE_VOLUMES="datanika_uploaded_files datanika_dbt_projects"',
        'FILE_VOLUMES="datanika_uploaded_files"',
    ),
    # Split out so the line stays inside the ruff limit. Removing the `exit 1`
    # leaves the diagnostic echo in place, which is the realistic regression:
    # a volume that cannot be read gets *reported* and then skipped anyway.
    "missing_volume_aborts": (
        _ABORT_ECHO_TAIL + "\n        exit 1",
        _ABORT_ECHO_TAIL,
    ),
    "volume_archive_is_verified_by_member_count": (
        'if [ "${TAR_FILES}" -lt "${SRC_FILES}" ]',
        'if [ "${TAR_FILES}" -lt 0 ]',
    ),
    "volume_ciphertext_is_round_tripped": ('--decrypt "${VENC}"', '--list-packets "${VENC}"'),
    "prunes_volume_patterns": ("-name '*.tar.gz.gpg' -mtime", "-name '*.nope2' -mtime"),
    "emits_a_per_volume_metric": (
        'datanika_backup_last_files_count{volume=\\"${VOL}\\"}',
        "datanika_backup_last_files_count_total",
    ),
    "drill_reads_ciphertext": ("*.sql.gz.gpg", "*.sql.gz"),
    "drill_fails_on_offsite_plaintext": (
        'if [ "${PLAINTEXT_COUNT}" != 0 ]',
        'if [ "${PLAINTEXT_COUNT}" = 999 ]',
    ),
    "drill_has_no_plaintext_fallback": (
        'tail -10 "${WORK}/gpg.log" || true\n    exit 1',
        'tail -10 "${WORK}/gpg.log" || true',
    ),
}


class TestBackupScriptInvariants:
    def test_only_ciphertext_leaves_the_box(self, backup_text):
        assert ships_only_ciphertext(backup_text), (
            "the off-site rsync must ship ${ENC}; shipping ${FILE} is core#675"
        )

    def test_size_gate_is_taken_on_the_plaintext(self, backup_text):
        assert size_gate_measures_plaintext(backup_text)

    def test_ciphertext_is_round_tripped_before_it_is_shipped(self, backup_text):
        assert verifies_roundtrip_before_shipping(backup_text), (
            "encryption that silently produced garbage would ship nightly for 30 days"
        )

    def test_missing_key_aborts_before_the_dump(self, backup_text):
        assert refuses_without_key(backup_text)

    def test_both_remote_retention_patterns_present(self, backup_text):
        assert prunes_both_remote_patterns(backup_text)

    def test_no_key_material_is_ever_sent_off_site(self, backup_text):
        assert sends_no_key_to_aweb(backup_text), (
            "co-locating the key with the ciphertext is the forbidden remedy in core#748"
        )

    def test_the_volumes_the_dump_references_are_archived(self, backup_text):
        assert archives_the_referenced_volumes(backup_text), (
            "a dump-only backup restores rows naming files nothing holds (core#954)"
        )

    def test_an_unreadable_volume_aborts_rather_than_being_skipped(self, backup_text):
        assert missing_volume_aborts(backup_text)

    def test_each_volume_archive_is_gated_on_an_independent_file_count(self, backup_text):
        assert volume_archive_is_verified_by_member_count(backup_text)

    def test_each_volume_ciphertext_is_round_tripped_before_shipping(self, backup_text):
        assert volume_ciphertext_is_round_tripped(backup_text)

    def test_volume_retention_patterns_present(self, backup_text):
        assert prunes_volume_patterns(backup_text)

    def test_the_metric_is_per_volume(self, backup_text):
        assert emits_a_per_volume_metric(backup_text)

    def test_ships_only_ciphertext_examines_the_volume_rsync_too(self, backup_text):
        """The second half of the strengthening, which the shared MUTATIONS table
        cannot express (one mutation per predicate).

        The old predicate returned on the first off-site rsync. Mutating the
        *dump's* line proves nothing about the volume line, so mutate that one
        directly — otherwise "every off-site rsync is examined" is an untested
        claim about a check, which is the shape this whole file exists to refuse.
        """
        assert ships_only_ciphertext(backup_text) is True
        mutated = backup_text.replace(
            'rsync -az -e "ssh ${SSH_OPTS}" "${VENC}"',
            'rsync -az -e "ssh ${SSH_OPTS}" "${VTAR}"',
        )
        assert mutated != backup_text, "mutation target not found — the mutation is stale"
        assert ships_only_ciphertext(mutated) is False, (
            "the predicate did not look at the volume rsync — it is still reading only the first"
        )

    def test_gnupghome_is_overridable_so_the_failure_path_is_testable(self, backup_text):
        assert 'GNUPGHOME="${GNUPGHOME:-/root/.gnupg}"' in backup_text, (
            "a hardcoded GNUPGHOME makes the key-loss negative control untestable -- "
            "it silently overrode the empty keyring and both controls reported a false red"
        )


class TestRestoreDrillInvariants:
    def test_drill_reads_the_encrypted_artifact(self, drill_text):
        assert drill_reads_ciphertext(drill_text)

    def test_drill_refuses_when_plaintext_is_found_off_site(self, drill_text):
        assert drill_fails_on_offsite_plaintext(drill_text)

    def test_drill_has_no_fallback_to_plaintext(self, drill_text):
        assert drill_has_no_plaintext_fallback(drill_text)

    def test_drill_is_the_thing_that_exercises_decryption(self, drill_text):
        """If the drill stopped decrypting, encryption would be unverified for 30 d."""
        assert "--decrypt" in drill_text and "RESTORE DRILL FAIL" in drill_text


class TestPublicKeyIsCommittedAndIsNotPrivate:
    def test_public_key_is_present(self):
        assert PUBKEY.exists(), "a rebuilt box needs the recipient key to make backups at all"

    def test_committed_key_is_the_public_half_only(self):
        text = PUBKEY.read_text(encoding="utf-8")
        assert text.startswith("-----BEGIN PGP PUBLIC KEY BLOCK-----")
        assert "PRIVATE KEY" not in text, "a private key must never be committed to a public repo"

    def test_backup_script_targets_the_committed_key(self, backup_text):
        assert "backup@datanika.io" in backup_text


class TestTheseChecksCanActuallyFail:
    """The mutation half. Without this the suite above is unfalsifiable."""

    @pytest.mark.parametrize("name", sorted(MUTATIONS))
    def test_predicate_goes_red_when_its_property_is_removed(self, name, backup_text, drill_text):
        predicate, which = PREDICATES[name]
        original = backup_text if which == "backup" else drill_text
        old, new = MUTATIONS[name]

        assert old in original, f"mutation target {old!r} not found -- the mutation is stale"
        mutated = original.replace(old, new)
        assert mutated != original

        assert predicate(original) is True, f"{name} should hold on the real script"
        assert predicate(mutated) is False, (
            f"{name} still passed after its property was removed -- the check is inert"
        )

    def test_every_predicate_has_a_mutation_or_is_explicitly_exempt(self):
        """Stops a new predicate being added without a proof that it can fail."""
        exempt = {"sends_no_key_to_aweb"}
        missing = set(PREDICATES) - set(MUTATIONS) - exempt
        assert not missing, f"predicates with no mutation coverage: {sorted(missing)}"
