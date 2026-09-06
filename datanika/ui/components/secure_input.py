"""Every ``autocomplete`` decision in the product, in both directions.

The module began as *"text inputs a password manager will not fill"* (core#618)
and now owns the opposite case too (core#672). That is deliberate rather than
tidy-mindedness: **the two are one decision seen from two sides**, and splitting
them is what produced #672. The connection form was hardened while ``/login`` and
``/signup`` — the two oldest and most-used forms in the product — were left
declaring nothing at all, because nothing named the choice in a single place a
reader would meet. Suppression lives in :func:`no_autofill_attrs`, declaration in
:func:`autofill_attrs`, and picking the wrong one is now a visible mistake
instead of an invisible omission.

**Why this module exists (core#618).** Not one input in the connection form set
``autocomplete``, ``name`` or ``id``. With nothing to disambiguate on, Chrome
falls back to a positional heuristic — pair the nearest preceding text input
with the first ``type="password"`` input, and fill the saved credentials for the
origin. On the ``google_ads`` form that put the signed-in user's Datanika
**email** into ``Customer ID`` and their Datanika **password** into
``Developer token``. Both fields look deliberately filled; neither was typed.
``_build_google_ads_source`` then sends the stored ``developer_token`` to
``googleads.googleapis.com`` as a request header, so a credential for *our*
system leaves the building on the next run.

**The fix is one flag, not two.** ``secret=True`` sets ``type="password"`` *and*
``autocomplete="new-password"`` together. Hand-writing either one at a call site
is how they drift apart, and a field with only one of them is still fillable —
so ``tests/test_ui/test_connection_form_autofill.py`` asserts the two imply each
other, in both directions, across every rendered field.

**Why ``new-password`` rather than ``off``.** Chrome deliberately ignores
``autocomplete="off"`` on password inputs — it decided sites were using it to
fight password managers and overrode them. ``new-password`` is the token it does
honour: it means "this is a value being created, not recalled", so Chrome offers
to *generate* rather than to fill. That is the behaviour we want on a connector
secret. ``off`` remains correct for the non-secret fields, whose exposure is the
ordinary address/email autofill rather than the credential pair.

These helpers are deliberately state-free — they import nothing but Reflex — so
any form can use them without dragging a state class into the import graph.
"""

import reflex as rx

from datanika.errors import UserFacingError

#: Vendor opt-outs. 1Password and LastPass ignore ``autocomplete`` entirely and
#: look for these instead. Same egress path, same blast radius: a manager that
#: fills the Datanika credential into ``Developer token`` sends it to Google
#: whichever vendor it came from.
_MANAGER_IGNORE_FLAGS = {"data-1p-ignore": "true", "data-lpignore": "true"}


def no_autofill_attrs(*, secret: bool = False) -> dict[str, str]:
    """The attribute set that opts one control out of every password manager.

    A fresh dict each call — Reflex stores ``custom_attrs`` on the component, and
    a shared module-level dict would be one mutation away from leaking between
    fields.
    """
    return {
        "autoComplete": "new-password" if secret else "off",
        **_MANAGER_IGNORE_FLAGS,
    }


#: The WHATWG autofill tokens the auth forms are allowed to declare.
#:
#: ``username`` is the *account identifier*, which is what our email fields are —
#: not ``email``, which is for a contact address on an ordinary form. The
#: distinction matters to a password manager: ``username`` + ``current-password``
#: is the pair it stores and recalls as one credential, and an ``email`` token
#: does not join that pair.
AUTH_AUTOFILL_TOKENS = frozenset({"username", "current-password", "new-password", "name"})


def autofill_attrs(token: str) -> dict[str, str]:
    """Declare an auth input's autofill role (core#672).

    The **inverse** of :func:`no_autofill_attrs`, and choosing between them is
    the whole decision:

    * A **connector credential** must not be filled from the browser's store —
      the value belongs to a third-party system, and #618 is what happens when
      the browser guesses. Use :func:`no_autofill_attrs`.
    * An **auth form** must be filled from the browser's store — it is the
      Datanika credential, and a manager recalling it is the desired behaviour.
      Use this. ⚠️ Reaching for ``no_autofill_attrs`` here is the plausible wrong
      repair: it wears a security label and ships a usability regression.
      ``tests/test_ui/test_auth_form_autofill.py`` refuses it.

    Note the vendor opt-out flags (``data-1p-ignore``, ``data-lpignore``) are
    deliberately **absent** — this function is the half that invites managers in.

    Args:
        token: one of :data:`AUTH_AUTOFILL_TOKENS`. Validated rather than passed
            through, because a typo (``"current_password"``, ``"newpassword"``)
            is silently ignored by every browser: the attribute is present, the
            page looks fixed, and the behaviour is identical to having no token
            at all. That is the exact shape of defect this module exists for, so
            it fails loudly here instead.
    """
    if token not in AUTH_AUTOFILL_TOKENS:
        # core#1113: developer text under a marker that says user-facing. Converted
        # here only to keep core#1094 step 2 behaviour-neutral.
        raise UserFacingError(
            f"{token!r} is not a recognised autofill token. "
            f"Browsers ignore an unknown token silently, so this would look "
            f"fixed and behave exactly like the bug (core#672). "
            f"Expected one of: {sorted(AUTH_AUTOFILL_TOKENS)}"
        )
    return {"autoComplete": token}


def config_input(
    field: str,
    *,
    secret: bool = False,
    width: str = "100%",
    **props,
) -> rx.Component:
    """A config-form text input that no password manager will fill.

    Args:
        field: the schema key this input edits, e.g. ``"developer_token"``. It
            becomes the ``name``/``id`` (as ``cfg-developer-token``) so the
            browser has something to key on other than document order.
        secret: the field holds a credential. Sets ``type="password"`` and
            ``autocomplete="new-password"`` together — never set either by hand.
        width: defaults to full width, which every call site wanted anyway.
        **props: forwarded to ``rx.input`` (``placeholder``, ``value``,
            ``on_change``, ``required``, ...).

    Note on ``id`` uniqueness: the same helper renders in several ``rx.cond``
    branches (``db_fields()`` alone is instantiated for the plain-DB group, for
    ClickHouse and for Oracle), so ``cfg-host`` appears more than once in the
    component tree. Only one branch ever mounts, so the DOM never holds a
    duplicate; and Reflex collapses same-named refs into a single declaration, so
    the compiled page has one ``ref_cfg_host``, not three. Both were checked
    rather than assumed.
    """
    slug = field.replace("_", "-")
    return rx.input(
        name=f"cfg-{slug}",
        id=f"cfg-{slug}",
        type="password" if secret else "text",
        custom_attrs=no_autofill_attrs(secret=secret),
        width=width,
        **props,
    )


def config_text_area(field: str, *, width: str = "100%", **props) -> rx.Component:
    """A config-form textarea that no password manager will fill.

    There is no ``secret`` flag here on purpose. Chrome's password manager fills
    ``<input type="password">`` and nothing else, so a textarea is never the
    credential-pair half — ``autocomplete="off"`` is both correct and sufficient,
    and a ``new-password`` token on a ``<textarea>`` would be meaningless. The
    textareas that *do* hold secrets (service-account JSON, extra headers) are
    protected by the same opt-out as the rest.
    """
    slug = field.replace("_", "-")
    return rx.text_area(
        name=f"cfg-{slug}",
        id=f"cfg-{slug}",
        custom_attrs=no_autofill_attrs(),
        width=width,
        **props,
    )
