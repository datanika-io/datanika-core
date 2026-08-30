from datetime import UTC, datetime, timedelta

import bcrypt
from jose import JWTError, jwt

# The org permission model. `plans/product/SPEC_ORG_ROLES.md` is the decision of
# record; this table is the machine-readable half of it and `UserService`
# derives its member-management checks from here rather than restating them.
#
# ⚠️ Until [core#658] this table had **zero production callers** —
# `AuthService.has_permission` was referenced only from test files. So
# `has_permission("admin", "manage_members") is False` was a *green security
# test asserting a rule nothing enforced*, which is a worse shape than a dead
# feature: the green argues against investigating. It is enforcement now.
#
# Two member-management capabilities, because reach is the thing that differs:
#
#   manage_members        — act on a member of ANY role, including other admins
#                           and owners. Owner only.
#   manage_members_below  — act only on members strictly below your own role,
#                           and grant only roles strictly below your own.
#                           Owner and admin.
#
# The "strictly below" half cannot live in a flat set — it is relational — so
# this table answers *whether* a role may manage members and `UserService`
# answers *which*. Admin deliberately cannot mint another admin
# (SPEC_ORG_ROLES R2): `admin` carries `delete` on every resource in the org,
# and who may destroy a customer's pipelines should not be self-replicating.
#
# `owner` is absent from every grantable role. Ownership moves only through
# `UserService.transfer_ownership` (SPEC_ORG_ROLES R1/§3).
ROLE_PERMISSIONS: dict[str, set[str]] = {
    "owner": {
        "create",
        "read",
        "update",
        "delete",
        "manage_members",
        "manage_members_below",
        "manage_org",
    },
    "admin": {"create", "read", "update", "delete", "manage_members_below"},
    "editor": {"create", "read", "update"},
    "viewer": {"read"},
}

# Authority ordering, used for the relational half of the model above.
# Higher acts on lower; equals may not act on each other except owner-on-owner
# (SPEC_ORG_ROLES R4), which is handled explicitly rather than by rank.
ROLE_RANK: dict[str, int] = {"viewer": 0, "editor": 1, "admin": 2, "owner": 3}


def may_grant_role(actor_role: str, granted_role: str) -> bool:
    """May `actor_role` assign `granted_role` to somebody? (R1 + R2)

    `owner` is never grantable — not even by an owner — because ownership
    moves through `UserService.transfer_ownership` alone (R1). Otherwise the
    granted role must rank strictly below the actor's (R2).
    """
    if granted_role == "owner":
        return False
    if "manage_members_below" not in ROLE_PERMISSIONS.get(actor_role, set()):
        return False
    if granted_role not in ROLE_RANK:
        return False
    return ROLE_RANK[granted_role] < ROLE_RANK[actor_role]


def may_manage_member(actor_role: str, target_role: str) -> bool:
    """May `actor_role` act on a member holding `target_role`? (R3 + R4)

    An owner holds `manage_members` and so reaches any member, including
    another owner (R4). Everyone else reaches strictly below themselves.
    """
    perms = ROLE_PERMISSIONS.get(actor_role, set())
    if "manage_members_below" not in perms:
        return False
    if "manage_members" in perms:
        return True
    if target_role not in ROLE_RANK:
        return False
    return ROLE_RANK[target_role] < ROLE_RANK[actor_role]


def assignable_roles(actor_role: str) -> list[str]:
    """Every role `actor_role` may grant, lowest first.

    The single source for both the service check and the role selects in the
    settings page. Rendering a control the server will refuse is core#658 AC4;
    deriving the options from the same predicate is how it stays true after
    the next role is added.
    """
    return [r for r in sorted(ROLE_RANK, key=ROLE_RANK.get) if may_grant_role(actor_role, r)]


ALGORITHM = "HS256"

# Session lifetime — founder decision, 2026-08-30 (#671).
#
# 10 minutes is short because it is not a session length: it is how long a
# stolen access token stays usable, and how long a password change takes to
# lock out the sessions it was meant to lock out. The user never sees it,
# because ``AuthState._revalidate_session`` renews silently from the refresh
# token on the next page load.
#
# ⚠️ These are the *only* numbers. Passing ``expires_minutes`` explicitly is
# for tests that need an already-expired token; a caller that hardcodes a
# lifetime here is a second answer to a question with one answer.
ACCESS_TOKEN_TTL_MINUTES = 10
REFRESH_TOKEN_TTL_DAYS = 7

# NIST SP 800-63B: length only. No character-class requirements, no forced
# rotation, no hints — those measurably push people toward weaker, reused
# passwords without buying anything.
MIN_PASSWORD_LENGTH = 8

# bcrypt silently ignores everything past 72 bytes, so a 100-character
# passphrase is really a 72-character one — and if the algorithm ever changes,
# those users' passwords change meaning. Reject rather than truncate. Bytes,
# not characters: 40 accented characters are 80 bytes.
MAX_PASSWORD_BYTES = 72


class AuthService:
    def __init__(self, secret_key: str):
        self._secret_key = secret_key

    # -- Password hashing --

    def hash_password(self, password: str) -> str:
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

    def verify_password(self, plain: str, hashed: str) -> bool:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))

    @staticmethod
    def validate_password_strength(password: str) -> None:
        """Raise ``ValueError`` if ``password`` breaks a rule (core#623, D8).

        The **only** password rule in the product. It is a ``staticmethod`` and
        it lives here rather than in each caller because three places that must
        agree is how they stop agreeing — ``register_user``, the Settings
        change form and the reset flow all route through this one function.
        """
        if len(password) < MIN_PASSWORD_LENGTH:
            raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters.")
        if len(password.encode("utf-8")) > MAX_PASSWORD_BYTES:
            raise ValueError(
                f"Password must be at most {MAX_PASSWORD_BYTES} bytes. Accented "
                "characters and emoji count as more than one byte each."
            )

    # -- JWT tokens --

    def create_access_token(
        self, user_id: int, org_id: int, expires_minutes: int = ACCESS_TOKEN_TTL_MINUTES
    ) -> str:
        now = datetime.now(UTC)
        payload = {
            "user_id": user_id,
            "org_id": org_id,
            "type": "access",
            "exp": now + timedelta(minutes=expires_minutes),
            "iat": now,
        }
        return jwt.encode(payload, self._secret_key, algorithm=ALGORITHM)

    def create_email_verification_token(
        self, user_id: int, email: str, expires_hours: int = 24
    ) -> str:
        now = datetime.now(UTC)
        payload = {
            "user_id": user_id,
            "email": email,
            "type": "email_verify",
            "exp": now + timedelta(hours=expires_hours),
            "iat": now,
        }
        return jwt.encode(payload, self._secret_key, algorithm=ALGORITHM)

    def create_refresh_token(self, user_id: int, expires_days: int = REFRESH_TOKEN_TTL_DAYS) -> str:
        now = datetime.now(UTC)
        payload = {
            "user_id": user_id,
            "type": "refresh",
            "exp": now + timedelta(days=expires_days),
            "iat": now,
        }
        return jwt.encode(payload, self._secret_key, algorithm=ALGORITHM)

    def decode_token(self, token: str, expected_type: str | None = None) -> dict | None:
        try:
            payload = jwt.decode(token, self._secret_key, algorithms=[ALGORITHM])
        except JWTError:
            return None
        if expected_type and payload.get("type") != expected_type:
            return None
        return payload

    # -- Role permissions --

    @staticmethod
    def has_permission(role: str, action: str) -> bool:
        permissions = ROLE_PERMISSIONS.get(role, set())
        return action in permissions
