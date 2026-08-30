"""Regression test for core#672 — the auth forms must declare their autofill role.

**The defect.** ``/login`` and ``/signup`` set **no** ``autocomplete`` attribute on
any input. Measured on production and confirmed in source, so it was not a stale
bundle. ``/forgot-password``'s email field had none either — found while driving
the reset flow on prod for the Docs-QA rotation, and not in the issue body.

**This is core#618's defect on the two forms that fix never reached**, and the
correct repair is its *mirror image*. #618 was a credential leaking **outward**:
with nothing to disambiguate on, Chrome paired the nearest text input with the
first ``type="password"`` input and filled the saved site credential into
``Customer ID`` / ``Developer token``, which ``_build_google_ads_source`` then
sent to Google as a header. Here the browser recycles a credential **inward**:

* ``/signup``'s password field, lacking ``new-password``, is offered the user's
  *existing* saved Datanika password for their *new* account — two accounts
  sharing one password, created by an autofill nobody typed.
* ``/login``'s pair, lacking ``username`` / ``current-password``, is recalled by
  position rather than by declaration.

**So the fix must NOT be ``no_autofill_attrs()``.** These are the legitimate
credential-pair forms; a password manager filling them is the desired behaviour.
Suppressing managers here would be a usability regression wearing a security
fix's clothes. ``test_auth_inputs_do_not_suppress_password_managers`` is the
assertion that refuses that repair, and it is the one most likely to catch a
future well-meaning edit.

**Why a rendered walk rather than a source grep.** Same reason as
``test_connection_form_autofill.py``, whose helpers this file reuses in shape:
the property that matters is the one that reaches the browser. A grep for
``autoComplete`` in ``login.py`` would have passed the moment the string appeared
anywhere in the module, including in a comment.

**Anti-vacuity is not optional here.** Every assertion below is a lookup into a
dict built by walking a tree. An empty dict satisfies all of them. The sibling
file learned this the expensive way — its first draft found 1 field of 80 and
would have gone green on entirely unfixed code — so
``test_the_walk_reaches_every_auth_field`` runs first and fails loudly on a
short harvest.
"""

import pytest

from datanika.ui.pages.forgot_password import forgot_password_page
from datanika.ui.pages.login import login_page
from datanika.ui.pages.reset_password import reset_password_page
from datanika.ui.pages.signup import signup_page

#: JSX component names Reflex emits for a text-entry control. The auth inputs are
#: uncontrolled (``rx.form`` + ``on_submit``), so they render bare rather than
#: wrapped in ``DebounceInput`` — but both shapes are handled, because
#: ``signup``'s email field carries ``default_value=AuthState.invite_email`` and a
#: future edit adding ``value=``/``on_change=`` would otherwise make this file
#: silently stop seeing it.
BARE_TEXT_ENTRY_TAGS = {"RadixThemesTextField.Root", "RadixThemesTextArea"}
DEBOUNCED_ELEMENTS = {"RadixThemesTextField.Root", "RadixThemesTextArea"}

#: The autofill role every auth input must declare, keyed by ``(page, name)``.
#:
#: ``username`` rather than ``email`` on the address fields is deliberate: the
#: WHATWG tokens distinguish "the account identifier" (``username``) from "an
#: e-mail address" (``email``, for contact forms). These fields are the account
#: identifier, and ``username`` is what pairs with ``current-password`` for a
#: manager to store and recall the credential as one unit.
#:
#: ``/forgot-password`` gets ``username`` for the same reason — it is the account
#: identifier, and offering the saved Datanika address there is helpful, not a
#: leak.
EXPECTED_TOKENS = {
    ("login", "email"): "username",
    ("login", "password"): "current-password",
    ("signup", "full_name"): "name",
    ("signup", "email"): "username",
    ("signup", "password"): "new-password",
    ("forgot-password", "email"): "username",
    # Already correct before this change (core#623 shipped them right); asserted
    # here so the four auth pages are covered by one guard and the next auth form
    # added is in range of it.
    ("reset-password", "password"): "new-password",
    ("reset-password", "confirm"): "new-password",
}

#: Vendor opt-out flags from ``secure_input.no_autofill_attrs``. Correct on the
#: connection form, wrong here — see the module docstring.
MANAGER_SUPPRESSION_ATTRS = ("data-1p-ignore", "data-lpignore")


def _walk(node):
    """Yield every dict in a rendered Reflex tree.

    Recurses through ``children`` **and** through ``rx.cond``'s ``true_value`` /
    ``false_value``. ``/forgot-password`` and ``/reset-password`` put their whole
    form inside an ``rx.cond`` (request form vs. confirmation screen; valid token
    vs. expired), so a walk that followed only ``children`` would find **zero**
    fields on two of the four pages and report nothing wrong.
    """
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def _props(node: dict) -> dict[str, str]:
    """Parse a rendered node's props list (``['type:"password"', ...]``).

    Values keep the raw JS source Reflex emits, so a string prop compares as
    ``'"password"'`` — quotes included. That distinguishes a literal from a state
    binding, and a state-bound ``autoComplete`` would not be a fix.
    """
    parsed = {}
    for prop in node.get("props", []):
        if ":" not in prop:
            continue
        key, _, value = prop.partition(":")
        parsed[key.strip().strip('"')] = value.strip()
    return parsed


def _fields(component) -> dict[str, dict[str, str]]:
    """Every named text input in one page, keyed by its ``name`` prop."""
    found = {}
    for node in _walk(component.render()):
        tag = node.get("name")
        props = None
        if tag in BARE_TEXT_ENTRY_TAGS:
            props = _props(node)
        elif tag == "DebounceInput":
            candidate = _props(node)
            if candidate.get("element", "") in DEBOUNCED_ELEMENTS:
                props = candidate
        if props is None:
            continue
        name = props.get("name", "").strip('"')
        if name:
            found[name] = props
    return found


@pytest.fixture(scope="module")
def auth_fields() -> dict[tuple[str, str], dict[str, str]]:
    """Every named input across all four auth pages, keyed by ``(page, name)``."""
    pages = {
        "login": login_page(),
        "signup": signup_page(),
        "forgot-password": forgot_password_page(),
        "reset-password": reset_password_page(),
    }
    return {
        (page, name): props
        for page, component in pages.items()
        for name, props in _fields(component).items()
    }


def test_the_walk_reaches_every_auth_field(auth_fields):
    """Anti-vacuity guard — run this before believing anything else in the file.

    Every other assertion is a dict lookup. A walker that returns ``{}`` — because
    a page moved its form behind a new ``rx.cond`` arm, or Reflex changed a
    component name — satisfies "for all" trivially and reports a clean pass on
    completely unfixed code.
    """
    missing = sorted(set(EXPECTED_TOKENS) - set(auth_fields))
    assert not missing, (
        f"The walk did not reach {len(missing)} of {len(EXPECTED_TOKENS)} expected "
        f"auth inputs: {missing}. Every assertion in this file is a lookup into "
        f"this mapping, so they are all vacuous until this passes. Found: "
        f"{sorted(auth_fields)}"
    )


def test_every_auth_input_declares_its_autofill_role(auth_fields):
    """The defect itself: five inputs across three pages declared nothing.

    With no token, Chrome guesses from document order — which is exactly the
    heuristic core#618 was about, pointed the other way.
    """
    wrong = {}
    for key, expected in EXPECTED_TOKENS.items():
        actual = auth_fields[key].get("autoComplete")
        if actual != f'"{expected}"':
            wrong[key] = actual
    assert not wrong, (
        "Auth inputs with a missing or wrong autoComplete token (core#672). "
        "Expected vs got: "
        + ", ".join(
            f"{page}/{name}: want {EXPECTED_TOKENS[(page, name)]!r}, got {got!r}"
            for (page, name), got in sorted(wrong.items())
        )
    )


def test_signup_offers_a_new_password_and_login_recalls_the_current_one(auth_fields):
    """The two tokens that carry the actual user-visible behaviour.

    Stated separately from the table above because these two are the *point*:
    ``new-password`` is what makes Chrome offer to **generate** on signup instead
    of filling the account's existing password, and ``current-password`` is what
    makes it **recall** on login instead of guessing. A table can be edited into
    agreement with a regression; this test names the outcome.
    """
    assert auth_fields[("signup", "password")].get("autoComplete") == '"new-password"', (
        "/signup's password field must be new-password, or Chrome offers the "
        "user's existing saved Datanika password for their new account."
    )
    assert auth_fields[("login", "password")].get("autoComplete") == '"current-password"', (
        "/login's password field must be current-password so it pairs with the "
        "username field above it."
    )
    assert auth_fields[("signup", "password")].get("type") == '"password"'
    assert auth_fields[("login", "password")].get("type") == '"password"'


def test_auth_inputs_do_not_suppress_password_managers(auth_fields):
    """core#672 acceptance criterion 3 — the fix must not be #618's fix.

    ``no_autofill_attrs()`` is right for connector credentials and wrong here.
    These are the credential-pair forms a manager is *supposed* to fill; the
    tempting repair (reuse the component that already exists) would ship a
    usability regression under a security label. This assertion is the one that
    refuses it.
    """
    for key, props in sorted(auth_fields.items()):
        for attr in MANAGER_SUPPRESSION_ATTRS:
            assert attr not in props, (
                f"{key[0]}/{key[1]} carries {attr!r}, which opts the field out of "
                "1Password/LastPass. The auth forms are the legitimate "
                "credential-pair forms — do not use no_autofill_attrs() here "
                "(core#672 AC3)."
            )
        assert props.get("autoComplete") != '"off"', (
            f"{key[0]}/{key[1]} declares autoComplete=off. Chrome ignores 'off' on "
            "password inputs anyway, and on the auth forms suppressing autofill is "
            "not the goal — declaring the role is."
        )


def test_every_auth_input_has_a_stable_name_and_id(auth_fields):
    """Give the browser something to key on other than position.

    A declared token still needs a stable field identity for a manager to store
    the credential against; ``name``/``id`` is half the fix in core#618 and is
    equally half of this one.
    """
    for key, props in sorted(auth_fields.items()):
        assert props.get("id"), f"{key[0]}/{key[1]} has no id attribute."
        assert props.get("name"), f"{key[0]}/{key[1]} has no name attribute."
