"""One link between three hand-maintained lists of secret config keys.

There are three places that independently decide "this config key holds a
credential":

1. ``connection_schemas.CONFIG_SCHEMAS`` — per-connector, marks a field with
   ``"format": "password"``. This is the one a connector author edits.
2. ``connection_service.SECRET_CONFIG_KEYS`` — what gets stripped out of a
   driver exception before it is shown to a user.
3. ``backup_service.SENSITIVE_KEYS`` — what gets redacted out of an export.

(2) and (3) were separate literals, and drifted: (3) held 4 keys against (2)'s
12, and (2) itself was missing 5 keys that (1) already marked sensitive. Both
gaps are silent — a key absent from a redaction set produces no error, just a
credential in a place it should not be.

So the tests below assert the *links*, not the contents of any one list. A new
connector that adds a sensitive field under a new name turns this file red, and
the fix is to add the key to the canonical set. Asserting on a list's contents
would have caught none of it, which is exactly why it sat.
"""

from datanika.services.backup_service import SENSITIVE_KEYS
from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.services.connection_service import SECRET_CONFIG_KEYS


def _schema_password_fields() -> dict[str, set[str]]:
    """{config key -> {connection types that mark it sensitive}}."""
    found: dict[str, set[str]] = {}
    for conn_type, schema in CONFIG_SCHEMAS.items():
        for key, prop in schema.get("properties", {}).items():
            if isinstance(prop, dict) and prop.get("format") == "password":
                found.setdefault(key, set()).add(conn_type)
    return found


class TestSecretKeyCoverage:
    def test_the_probe_finds_password_fields_at_all(self):
        """Guard the guard: a broken extractor would make every test below vacuous."""
        fields = _schema_password_fields()
        assert len(fields) >= 10, f"only found {len(fields)} sensitive schema fields"
        assert "password" in fields
        assert "postgres" in fields["password"]

    def test_every_schema_password_field_is_in_the_canonical_secret_set(self):
        missing = {
            k: sorted(v)
            for k, v in _schema_password_fields().items()
            if k not in SECRET_CONFIG_KEYS
        }
        assert not missing, (
            "config keys marked `format: password` in CONFIG_SCHEMAS but absent from "
            f"connection_service.SECRET_CONFIG_KEYS — their values can reach a user-facing "
            f"error message verbatim: {missing}"
        )

    def test_backup_redaction_derives_from_the_canonical_set(self):
        assert SENSITIVE_KEYS == SECRET_CONFIG_KEYS, (
            "backup_service.SENSITIVE_KEYS must be the canonical set, not a second copy of it. "
            f"only-in-backup={sorted(set(SENSITIVE_KEYS) - set(SECRET_CONFIG_KEYS))} "
            f"only-in-canonical={sorted(set(SECRET_CONFIG_KEYS) - set(SENSITIVE_KEYS))}"
        )

    def test_canonical_set_is_immutable(self):
        """A mutable set here would let one caller's edit silently change every caller's."""
        assert isinstance(SECRET_CONFIG_KEYS, frozenset)
