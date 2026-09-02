"""A handler that writes to the database must tell the user it did (core#872).

Contract: ``docs/specs/SPEC_MUTATION_FEEDBACK.md`` D1/D2/D6.

[core#804] and [core#851] instrumented the **destructive** direction: ten
confirmation dialogs and nine success toasts. **Creating was not.** Measured on
production while creating a BigQuery connection: the create succeeded, and the
page showed no toast, no inline confirmation, no error, the same four old rows
for at least five seconds, and an apparently unchanged form. Every signal
available to the user said *nothing happened*, and the recovery action a user
reaches for is repeating the mutation.

🚨 **Why that is worse than untidiness.** Connection quota enforcement went live
2026-08-31. On a Free org at 4 of 5 connections an invisible first create
**spends the last slot**, and the user's *second* click is the one refused — so
the success and the failure arrive in the opposite order from the one the user
perceives, and the error they eventually read describes the wrong event.

## 🔑 Why the classifier is not the one the spec describes

D2 says a handler is in scope if its body *"opens a session or commits"*.
**Measured, that selects 69 handlers, not 20** — every ``load_*`` opens a session
to read, and a toast on a page load is nonsense. Narrowing to *commits* selects
**40**, against the 20 the spec measured. The extra 20 are real mutations that
legitimately do not toast (a redirect is the acknowledgement for ``login``; the
switch is the acknowledgement for ``toggle_schedule``), so a one-list allowlist
would ship with ~20 entries — the outcome the spec's own ship order says to
avoid.

So this is a **two-list ratchet**, and the derived half is the equality:

* ``ACKNOWLEDGES`` — mutations that must yield a toast.
* ``ACKNOWLEDGED_ELSEWHERE`` — mutations whose feedback is something other than a
  toast, each with its reason beside it.
* ``test_every_committing_handler_is_classified`` asserts the union **equals**
  the derived set. A new committing handler therefore fails until somebody
  decides which list it belongs in — which is the property that catches the
  eleventh handler, and the only reason a derived guard beats ten repairs.

⚠️ Deciding that the other 20 owe the user an acknowledgement is a **Product**
call, not an Engineering one. This file records the split; it does not settle it.

## Classify by behaviour, never by name

Both directions of the naming heuristic are wrong here, which §2a established by
getting them wrong first: ``add_model`` and ``add_custom_test`` look constructive
and only edit the unsaved form's in-memory list; ``save_sql_and_return`` is named
``save_`` and is a bare ``rx.redirect`` that saves nothing. A name-keyed guard
would demand a toast for an operation that does not exist.

⚠️ And the converse: ``SettingsState.add_member_by_email`` **writes through a
helper** and contains no ``commit`` of its own. A classifier that only reads the
handler's own body misses it — the silent direction — so ``self._helper()`` calls
are resolved one level within the class.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

STATE_DIR = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "ui" / "state"

#: The two helpers on ``BaseState`` that emit a translated toast.
TOAST_HELPERS = {"_saved_toast", "_deleted_toast"}

#: Mutations that must acknowledge with a toast: the ten constructive handlers
#: core#872 measured, plus the nine destructive ones core#804/#851 shipped.
ACKNOWLEDGES = frozenset(
    {
        # Constructive — core#872.
        "ApiKeyState.create_api_key",
        "ConnectionState.save_connection",
        "DagState.add_dependency",
        "ModelDetailState.save_model_detail",
        "NotificationState.save_channel",
        "PipelineState.save_pipeline",
        "ScheduleState.save_schedule",
        "SettingsState.add_member_by_email",
        "TransformationState.save_transformation",
        "UploadState.save_upload",
        # Destructive — already shipped by core#804 / core#851.
        "ApiKeyState.revoke_api_key",
        "ConnectionState.delete_connection",
        "DagState.remove_dependency",
        "NotificationState.delete_channel",
        "PipelineState.delete_pipeline",
        "ScheduleState.delete_schedule",
        "SettingsState.remove_member",
        "TransformationState.delete_transformation",
        "UploadState.delete_upload",
    }
)

#: Mutations whose feedback is deliberately something other than a toast.
#: ⚠️ Not an exemption list to grow — each entry is a claim about what the user
#: sees instead, and several are arguable. Revisiting them is Product's call.
ACKNOWLEDGED_ELSEWHERE = frozenset(
    {
        "AuthState.login",  # redirects to the app
        "AuthState.signup",  # redirects, plus the verification-mail notice (core#700)
        "AuthState.logout",  # redirects to /login
        "AccountState.change_password",  # renders its own `success` flag
        "AccountState.delete_account",  # signs the user out
        "PasswordResetState.request_reset",  # its own confirmation screen (core#623)
        "PasswordResetState.submit_new_password",  # redirects to /login?reset=1
        "ConnectionState.handle_file_upload",  # the file name appears in the form
        "NotificationCenterState.mark_read",  # the unread badge is the feedback
        "NotificationCenterState.mark_all_read",  # ditto, and a toast per bell click is noise
        "NotificationCenterState.dismiss",  # the row disappears from the panel
        "NotificationState.toggle_channel_active",  # the switch is the feedback
        "ScheduleState.toggle_schedule",  # the switch is the feedback
        "OnboardingState.dismiss_checklist",  # the checklist disappears
        "BackupState.handle_restore_upload",  # the card renders its own status line
        "BackupState.confirm_restore",  # ditto, and it reloads the page on success
        "PipelineState.run_pipeline",  # "Run triggered" toast (D6)
        "UploadState.run_upload",  # "Run triggered" toast (D6)
        # 🔴 The five below are arguably defects, and are NOT this issue's scope.
        # `cancel_invitation` is core#851's eleventh site — a one-click
        # irreversible mutation with neither a dialog nor a toast, reported there.
        "SettingsState.update_org",
        "SettingsState.change_member_role",
        "SettingsState.transfer_ownership",
        "SettingsState.leave_org",
        "SettingsState.cancel_invitation",
    }
)

#: Form-only handlers, kept as a positive control on the classifier (§2a).
FORM_ONLY = {
    "PipelineState.add_model",
    "ModelDetailState.add_custom_test",
    "TransformationState.save_sql_and_return",
}


def _commits(fn: ast.AST) -> bool:
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "commit"
        ):
            return True
    return False


def _self_calls(fn: ast.AST) -> set[str]:
    """Names of ``self._helper()`` calls made by this handler."""
    names = set()
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
        ):
            names.add(node.func.attr)
    return names


def _yields_a_toast(fn: ast.AST) -> bool:
    """Is a toast helper's result **yielded**?

    ⚠️ ``yield``, not ``return``. A plain ``async def`` handler delivers a single
    state update *after it returns*, so everything inside it — a slow service
    call, a commit, a refetch — is invisible to the browser until then. A toast
    that is returned rather than yielded never reaches the user, and nothing
    fails. That is core#872 in one sentence, and it is why the obvious fix
    ("reload the table after create") changes nothing: the refetch is already
    there, and it is not what is missing.
    """
    for node in ast.walk(fn):
        if not isinstance(node, ast.Yield) or node.value is None:
            continue
        for inner in ast.walk(node.value):
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr in TOAST_HELPERS
            ):
                return True
    return False


def _scan() -> tuple[dict[str, ast.AST], set[str]]:
    handlers: dict[str, ast.AST] = {}
    committers: set[str] = set()
    for path in sorted(STATE_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for cls in (n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)):
            methods = {
                fn.name: fn
                for fn in cls.body
                if isinstance(fn, ast.AsyncFunctionDef | ast.FunctionDef)
            }
            private_commits = {n for n, fn in methods.items() if n.startswith("_") and _commits(fn)}
            for name, fn in methods.items():
                if name.startswith("_"):
                    continue
                key = f"{cls.name}.{name}"
                handlers[key] = fn
                if _commits(fn) or (_self_calls(fn) & private_commits):
                    committers.add(key)
    return handlers, committers


HANDLERS, COMMITTERS = _scan()


class TestTheScanFoundSomething:
    """Anti-vacuity. Every assertion below is a "for all" over a derived set, and
    an empty set satisfies all of them — a checker with one possible answer."""

    def test_the_walk_reaches_the_state_classes(self):
        assert len(HANDLERS) >= 80, len(HANDLERS)

    def test_the_classifier_finds_the_committers(self):
        assert len(COMMITTERS) >= 35, sorted(COMMITTERS)

    def test_a_handler_that_writes_through_a_helper_is_still_a_writer(self):
        """``add_member_by_email`` contains no ``commit`` of its own — it
        delegates to ``_send_invitation`` / ``_add_existing_user``. A classifier
        reading only the handler's own body misses it, which is the silent
        direction of this check."""
        assert "SettingsState.add_member_by_email" in COMMITTERS


class TestTheClassifierIsBehavioural:
    @pytest.mark.parametrize("name", sorted(FORM_ONLY))
    def test_the_form_only_handlers_are_out_of_scope(self, name):
        """§2a. These edit unsaved in-memory state or navigate; none commits. If
        the classifier ever starts sweeping them in it has become a name
        heuristic, and this fails before anyone ships a pointless toast."""
        assert name in HANDLERS, f"{name} vanished — update this control"
        assert name not in COMMITTERS, (
            f"{name} was classified as a database writer. It is not one — the "
            "classifier has regressed to matching names."
        )


class TestTheRatchet:
    def test_every_committing_handler_is_classified(self):
        """The derived half, and the reason this beats ten repairs.

        A newly-added handler that commits belongs in exactly one of the two
        lists, and until somebody says which, this fails. That is what catches
        the eleventh handler — the one nobody has written yet.
        """
        classified = ACKNOWLEDGES | ACKNOWLEDGED_ELSEWHERE
        unclassified = sorted(COMMITTERS - classified)
        assert not unclassified, (
            f"{unclassified} write to the database and are in neither list. Decide: does "
            "the user get a toast (ACKNOWLEDGES) or is there other visible feedback "
            "(ACKNOWLEDGED_ELSEWHERE, with the reason beside the entry)?"
        )

    def test_neither_list_outlives_the_handlers(self):
        """The other direction. A renamed or deleted handler must fail by name
        rather than sit in a list forever asserting something about nothing."""
        classified = ACKNOWLEDGES | ACKNOWLEDGED_ELSEWHERE
        gone = sorted(classified - COMMITTERS)
        assert not gone, (
            f"{gone} are classified but no longer commit — renamed, deleted, or the "
            "mutation moved. Update the lists."
        )

    def test_the_two_lists_are_disjoint(self):
        assert not (ACKNOWLEDGES & ACKNOWLEDGED_ELSEWHERE)


class TestEveryMutationAcknowledges:
    @pytest.mark.parametrize("name", sorted(ACKNOWLEDGES))
    def test_it_yields_a_toast(self, name):
        assert name in HANDLERS, f"{name} no longer exists"
        assert _yields_a_toast(HANDLERS[name]), (
            f"{name} writes to the database and yields no toast. The user gets no "
            "acknowledgement, and the recovery action they reach for is repeating the "
            "mutation — which on a quota-enforced org spends a slot and makes the "
            "SECOND click the one that errors.\n"
            "Add `yield await self._saved_toast(<key>, <fallback>)` after the write "
            "succeeds, on a path the `except` returns before reaching."
        )


class TestNoHardcodedToasts:
    def test_every_toast_goes_through_the_translated_helpers(self):
        """D6. Two toasts bypassed ``I18nState`` entirely and shipped a
        hardcoded English string, so eight of nine locales showed English.

        The count is the check; the two known sites are not — a third would
        otherwise be found by a user rather than by CI.
        """
        ui = STATE_DIR.parent
        offenders = []
        for path in sorted(ui.rglob("*.py")):
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if "rx.toast(" not in line:
                    continue
                argument = line.split("rx.toast(", 1)[1].lstrip()
                if argument.startswith(('"', "'")):
                    offenders.append(f"{path.relative_to(ui.parent)}:{lineno}")
        assert not offenders, (
            f"hardcoded toast strings at {offenders}. Route them through "
            "`_saved_toast`/`_deleted_toast`, which read the reactive I18nState dict."
        )
