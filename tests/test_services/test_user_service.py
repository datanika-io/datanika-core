"""TDD tests for user management service."""

import pytest

from etlfabric.models.user import MemberRole, Membership, Organization, User
from etlfabric.services.auth import AuthService
from etlfabric.services.user_service import UserService, UserServiceError


@pytest.fixture
def auth():
    return AuthService(secret_key="test-secret-key-for-user-svc")


@pytest.fixture
def svc(auth):
    return UserService(auth)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-user-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-user-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def user(db_session, auth):
    u = User(
        email="existing@example.com",
        password_hash=auth.hash_password("password123"),
        full_name="Existing User",
    )
    db_session.add(u)
    db_session.flush()
    return u


@pytest.fixture
def user_with_org(db_session, user, org):
    """User with an owner membership in org."""
    m = Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER)
    db_session.add(m)
    db_session.flush()
    return user


# ---------------------------------------------------------------------------
# register_user
# ---------------------------------------------------------------------------


class TestRegisterUser:
    def test_returns_user_with_hash(self, svc, db_session):
        u = svc.register_user(db_session, "alice@example.com", "secret", "Alice")
        assert isinstance(u, User)
        assert isinstance(u.id, int)
        assert u.full_name == "Alice"
        assert u.password_hash != "secret"

    def test_email_lowercased(self, svc, db_session):
        u = svc.register_user(db_session, "ALICE@EXAMPLE.COM", "secret", "Alice")
        assert u.email == "alice@example.com"

    def test_is_active_default(self, svc, db_session):
        u = svc.register_user(db_session, "alice@example.com", "secret", "Alice")
        assert u.is_active is True

    def test_duplicate_email_error(self, svc, db_session):
        svc.register_user(db_session, "dup@example.com", "secret", "First")
        with pytest.raises(UserServiceError, match="[Ee]mail.*already"):
            svc.register_user(db_session, "dup@example.com", "secret", "Second")

    def test_empty_email_error(self, svc, db_session):
        with pytest.raises(UserServiceError, match="[Ee]mail.*required"):
            svc.register_user(db_session, "", "secret", "Alice")

    def test_empty_password_error(self, svc, db_session):
        with pytest.raises(UserServiceError, match="[Pp]assword.*required"):
            svc.register_user(db_session, "alice@example.com", "", "Alice")


# ---------------------------------------------------------------------------
# authenticate
# ---------------------------------------------------------------------------


class TestAuthenticate:
    def test_valid_returns_tokens(self, svc, db_session, user_with_org, org):
        result = svc.authenticate(db_session, "existing@example.com", "password123")
        assert result is not None
        assert "access_token" in result
        assert "refresh_token" in result
        assert "user" in result

    def test_wrong_password_returns_none(self, svc, db_session, user_with_org):
        result = svc.authenticate(db_session, "existing@example.com", "wrong")
        assert result is None

    def test_nonexistent_email_returns_none(self, svc, db_session):
        result = svc.authenticate(db_session, "nobody@example.com", "password")
        assert result is None

    def test_inactive_user_returns_none(self, svc, db_session, user_with_org):
        user_with_org.is_active = False
        db_session.flush()
        result = svc.authenticate(db_session, "existing@example.com", "password123")
        assert result is None

    def test_tokens_contain_user_id(self, svc, db_session, user_with_org, auth):
        result = svc.authenticate(db_session, "existing@example.com", "password123")
        payload = auth.decode_token(result["access_token"])
        assert payload["user_id"] == user_with_org.id


# ---------------------------------------------------------------------------
# authenticate_for_org
# ---------------------------------------------------------------------------


class TestAuthenticateForOrg:
    def test_valid_with_membership(self, svc, db_session, user_with_org, org):
        result = svc.authenticate_for_org(db_session, "existing@example.com", "password123", org.id)
        assert result is not None
        assert result["user"].id == user_with_org.id

    def test_no_membership_returns_none(self, svc, db_session, user_with_org, other_org):
        result = svc.authenticate_for_org(
            db_session, "existing@example.com", "password123", other_org.id
        )
        assert result is None

    def test_wrong_password_returns_none(self, svc, db_session, user_with_org, org):
        result = svc.authenticate_for_org(db_session, "existing@example.com", "wrong", org.id)
        assert result is None

    def test_token_contains_correct_org_id(self, svc, db_session, user_with_org, org, auth):
        result = svc.authenticate_for_org(db_session, "existing@example.com", "password123", org.id)
        payload = auth.decode_token(result["access_token"])
        assert payload["org_id"] == org.id


# ---------------------------------------------------------------------------
# get_user_by_email
# ---------------------------------------------------------------------------


class TestGetUserByEmail:
    def test_existing(self, svc, db_session, user):
        found = svc.get_user_by_email(db_session, "existing@example.com")
        assert found is not None
        assert found.id == user.id

    def test_nonexistent(self, svc, db_session):
        assert svc.get_user_by_email(db_session, "nobody@example.com") is None

    def test_case_insensitive(self, svc, db_session, user):
        found = svc.get_user_by_email(db_session, "EXISTING@EXAMPLE.COM")
        assert found is not None
        assert found.id == user.id


# ---------------------------------------------------------------------------
# get_user
# ---------------------------------------------------------------------------


class TestGetUser:
    def test_existing(self, svc, db_session, user):
        found = svc.get_user(db_session, user.id)
        assert found is not None
        assert found.email == "existing@example.com"

    def test_nonexistent(self, svc, db_session):
        assert svc.get_user(db_session, 99999) is None


# ---------------------------------------------------------------------------
# create_org
# ---------------------------------------------------------------------------


class TestCreateOrg:
    def test_returns_org_with_owner_membership(self, svc, db_session, user):
        org = svc.create_org(db_session, "NewCo", "newco", user.id)
        assert isinstance(org, Organization)
        assert org.name == "NewCo"
        assert org.slug == "newco"
        # Should have created owner membership
        memberships = (
            db_session.query(Membership)
            .filter_by(org_id=org.id, user_id=user.id, role=MemberRole.OWNER)
            .all()
        )
        assert len(memberships) == 1

    def test_slug_uniqueness(self, svc, db_session, user):
        svc.create_org(db_session, "First", "unique-slug", user.id)
        with pytest.raises(UserServiceError, match="[Ss]lug.*already"):
            svc.create_org(db_session, "Second", "unique-slug", user.id)

    def test_nonexistent_owner_error(self, svc, db_session):
        with pytest.raises(UserServiceError, match="[Uu]ser.*not found"):
            svc.create_org(db_session, "Co", "co", 99999)

    def test_empty_name_error(self, svc, db_session, user):
        with pytest.raises(UserServiceError, match="[Nn]ame.*required"):
            svc.create_org(db_session, "", "slug", user.id)

    def test_empty_slug_error(self, svc, db_session, user):
        with pytest.raises(UserServiceError, match="[Ss]lug.*required"):
            svc.create_org(db_session, "Name", "", user.id)


# ---------------------------------------------------------------------------
# get_user_orgs
# ---------------------------------------------------------------------------


class TestGetUserOrgs:
    def test_returns_orgs(self, svc, db_session, user_with_org, org):
        orgs = svc.get_user_orgs(db_session, user_with_org.id)
        assert len(orgs) == 1
        assert orgs[0].id == org.id

    def test_empty_when_no_memberships(self, svc, db_session, user):
        orgs = svc.get_user_orgs(db_session, user.id)
        assert orgs == []

    def test_excludes_removed(self, svc, db_session, user):
        org = svc.create_org(db_session, "Co", "co-get-orgs", user.id)
        # Add a second owner so we can remove the first
        other = User(email="keeper@example.com", password_hash="hash", full_name="Keeper")
        db_session.add(other)
        db_session.flush()
        svc.add_member(db_session, org.id, other.id, MemberRole.OWNER)
        # Get the original membership and soft-delete it
        membership = db_session.query(Membership).filter_by(org_id=org.id, user_id=user.id).first()
        svc.remove_member(db_session, org.id, membership.id)
        orgs = svc.get_user_orgs(db_session, user.id)
        assert orgs == []


# ---------------------------------------------------------------------------
# update_org
# ---------------------------------------------------------------------------


class TestUpdateOrg:
    def test_update_name(self, svc, db_session, org):
        updated = svc.update_org(db_session, org.id, name="Renamed")
        assert updated is not None
        assert updated.name == "Renamed"

    def test_update_slug(self, svc, db_session, org):
        updated = svc.update_org(db_session, org.id, slug="new-slug")
        assert updated is not None
        assert updated.slug == "new-slug"

    def test_nonexistent_returns_none(self, svc, db_session):
        assert svc.update_org(db_session, 99999, name="X") is None


# ---------------------------------------------------------------------------
# add_member
# ---------------------------------------------------------------------------


class TestAddMember:
    def test_returns_membership(self, svc, db_session, user, org):
        m = svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)
        assert isinstance(m, Membership)
        assert m.org_id == org.id
        assert m.user_id == user.id
        assert m.role == MemberRole.EDITOR

    def test_duplicate_error(self, svc, db_session, user, org):
        svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)
        with pytest.raises(UserServiceError, match="[Aa]lready a member"):
            svc.add_member(db_session, org.id, user.id, MemberRole.VIEWER)

    def test_nonexistent_user_error(self, svc, db_session, org):
        with pytest.raises(UserServiceError, match="[Uu]ser.*not found"):
            svc.add_member(db_session, org.id, 99999, MemberRole.VIEWER)

    def test_nonexistent_org_error(self, svc, db_session, user):
        with pytest.raises(UserServiceError, match="[Oo]rganization.*not found"):
            svc.add_member(db_session, 99999, user.id, MemberRole.VIEWER)


# ---------------------------------------------------------------------------
# remove_member
# ---------------------------------------------------------------------------


class TestRemoveMember:
    def test_soft_deletes(self, svc, db_session, user, org):
        m = svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)
        result = svc.remove_member(db_session, org.id, m.id)
        assert result is True
        db_session.refresh(m)
        assert m.deleted_at is not None

    def test_nonexistent_returns_false(self, svc, db_session, org):
        result = svc.remove_member(db_session, org.id, 99999)
        assert result is False

    def test_prevents_removing_last_owner(self, svc, db_session, user_with_org, org):
        # user_with_org is the sole owner
        membership = (
            db_session.query(Membership)
            .filter_by(org_id=org.id, user_id=user_with_org.id, role=MemberRole.OWNER)
            .first()
        )
        with pytest.raises(UserServiceError, match="[Ll]ast owner"):
            svc.remove_member(db_session, org.id, membership.id)

    def test_allows_removing_non_owner(self, svc, db_session, user_with_org, org):
        # Add a second user as editor
        other = User(
            email="other@example.com",
            password_hash="hash",
            full_name="Other",
        )
        db_session.add(other)
        db_session.flush()
        m = svc.add_member(db_session, org.id, other.id, MemberRole.EDITOR)
        result = svc.remove_member(db_session, org.id, m.id)
        assert result is True


# ---------------------------------------------------------------------------
# list_members
# ---------------------------------------------------------------------------


class TestListMembers:
    def test_returns_all_active(self, svc, db_session, user, org):
        svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)
        members = svc.list_members(db_session, org.id)
        assert len(members) == 1

    def test_excludes_deleted(self, svc, db_session, user, org):
        m = svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)
        svc.remove_member(db_session, org.id, m.id)
        members = svc.list_members(db_session, org.id)
        assert len(members) == 0

    def test_empty_org(self, svc, db_session, org):
        members = svc.list_members(db_session, org.id)
        assert members == []


# ---------------------------------------------------------------------------
# change_role
# ---------------------------------------------------------------------------


class TestChangeRole:
    def test_changes_role(self, svc, db_session, user, org):
        m = svc.add_member(db_session, org.id, user.id, MemberRole.EDITOR)
        updated = svc.change_role(db_session, org.id, m.id, MemberRole.ADMIN)
        assert updated is not None
        assert updated.role == MemberRole.ADMIN

    def test_nonexistent_returns_none(self, svc, db_session, org):
        result = svc.change_role(db_session, org.id, 99999, MemberRole.ADMIN)
        assert result is None

    def test_prevents_demoting_last_owner(self, svc, db_session, user_with_org, org):
        membership = (
            db_session.query(Membership)
            .filter_by(org_id=org.id, user_id=user_with_org.id, role=MemberRole.OWNER)
            .first()
        )
        with pytest.raises(UserServiceError, match="[Ll]ast owner"):
            svc.change_role(db_session, org.id, membership.id, MemberRole.ADMIN)

    def test_allows_when_multiple_owners(self, svc, db_session, user_with_org, org):
        other = User(
            email="other2@example.com",
            password_hash="hash",
            full_name="Other",
        )
        db_session.add(other)
        db_session.flush()
        svc.add_member(db_session, org.id, other.id, MemberRole.OWNER)
        # Now we can demote the first owner
        membership = (
            db_session.query(Membership)
            .filter_by(org_id=org.id, user_id=user_with_org.id, role=MemberRole.OWNER)
            .first()
        )
        updated = svc.change_role(db_session, org.id, membership.id, MemberRole.ADMIN)
        assert updated.role == MemberRole.ADMIN


# ---------------------------------------------------------------------------
# get_membership
# ---------------------------------------------------------------------------


class TestGetMembership:
    def test_existing(self, svc, db_session, user_with_org, org):
        m = svc.get_membership(db_session, org.id, user_with_org.id)
        assert m is not None
        assert m.role == MemberRole.OWNER

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_membership(db_session, org.id, 99999) is None


# ---------------------------------------------------------------------------
# find_or_create_oauth_user (Step 27)
# ---------------------------------------------------------------------------
class TestFindOrCreateOAuthUser:
    def test_existing_user_returns_not_new(self, svc, db_session, user):
        result_user, is_new = svc.find_or_create_oauth_user(
            db_session, user.email, "Existing", "google", "g123"
        )
        assert result_user.id == user.id
        assert is_new is False

    def test_new_user_creates_user_and_org(self, svc, db_session):
        result_user, is_new = svc.find_or_create_oauth_user(
            db_session, "new_oauth@test.com", "OAuth User", "github", "gh456"
        )
        assert is_new is True
        assert result_user.email == "new_oauth@test.com"
        assert result_user.oauth_provider == "github"
        assert result_user.oauth_provider_id == "gh456"

        # Verify org was created
        orgs = svc.get_user_orgs(db_session, result_user.id)
        assert len(orgs) == 1
        assert "OAuth User" in orgs[0].name

    def test_oauth_fields_set_on_existing_user(self, svc, db_session, user):
        """If existing user has no oauth_provider, it gets updated."""
        assert user.oauth_provider is None
        svc.find_or_create_oauth_user(
            db_session, user.email, "Existing", "google", "g789"
        )
        assert user.oauth_provider == "google"
        assert user.oauth_provider_id == "g789"
