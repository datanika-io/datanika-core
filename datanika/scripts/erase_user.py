"""Operator-run GDPR erasure — ``docs/specs/SPEC_PII_SEPARATION.md`` D10, core#655.

    python -m datanika.scripts.erase_user --email someone@example.com --dry-run
    python -m datanika.scripts.erase_user --email someone@example.com --confirm

**Why this exists as well as the Settings control, rather than instead of it.** The person
most likely to need an erasure is the one who cannot reach the UI: locked out by an address
they typo'd at signup, or a former member of an org they no longer belong to. Every route
out of that state goes through us, so a self-service button alone leaves exactly the people
this obligation is about without a route. `datanika.io/privacy` points at an address, and
this is what answers it.

Conversely, shipping only this script would be the audit's recurring *"machinery exists,
entry point does not"* finding — D10 requires **both**, and the spec does not accept one as
done.

⚠️ **Two things this deliberately does not print.**

* **The address or the name.** Criterion 20: no email, name or token value in any log
  line — including the erasure's own record. The operator typed the address, so echoing it
  adds nothing and puts it in a terminal scrollback, a CI log, or a ticket attachment.
  What is printed is the user id and a count per class of work.
* **A "are you sure?" that defaults to yes.** ``--confirm`` is required and there is no
  interactive prompt, because this runs in contexts where stdin is not a person.

There is **no undo, ever** (D2). Re-registering the same address produces a new account
with a new id and no link to the old one: not by us, not by support, not by a database
query. That is the point rather than a limitation — retaining a tombstone to link them
would mean retaining a pseudonymous identifier that re-identifies the person on lookup.
"""

from __future__ import annotations

import argparse
import sys

from datanika.config import settings
from datanika.db import get_sync_session
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService, UserServiceError


def _service() -> UserService:
    return UserService(AuthService(settings.secret_key))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m datanika.scripts.erase_user",
        description="Erase a person's personal data (GDPR Art. 17). There is no undo.",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--email", help="the address the request arrived from")
    target.add_argument("--user-id", type=int, help="when the address is already gone")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="actually erase. Without it this is a dry run and nothing is written.",
    )
    args = parser.parse_args(argv)

    svc = _service()
    with get_sync_session() as session:
        if args.email:
            user = svc.get_user_by_email(session, args.email)
            if user is None:
                # Deliberately does not distinguish "no such account" from "already
                # erased": both are the same fact to the person asking, and to anyone
                # using this script to probe for addresses.
                print("No matching active account.", file=sys.stderr)
                return 2
            user_id = user.id
        else:
            user_id = args.user_id
            if svc.get_user(session, user_id) is None:
                print("No matching account.", file=sys.stderr)
                return 2

        if not args.confirm:
            pii = svc.get_user_pii(session, user_id)
            print(f"DRY RUN — nothing written. user id={user_id}")
            print(f"  personal-data row present: {pii is not None}")
            print("  re-run with --confirm to erase. There is no undo.")
            return 0

        try:
            counts = svc.erase_user(session, user_id)
        except UserServiceError as exc:
            # The sole-owner refusal (§9a(1)) lands here, and it names both exits.
            print(f"REFUSED: {exc}", file=sys.stderr)
            session.rollback()
            return 3
        session.commit()

    print(f"Erased user id={user_id}")
    for key, value in sorted(counts.items()):
        print(f"  {key}: {value}")
    if counts.get("audit_payloads_redacted"):
        # A canary, not a cleanup: after D11 and the log_action redactor this must be
        # zero, so a non-zero count means one of those two failed rather than that this
        # run did extra work.
        print(
            "  ⚠️  the residual audit sweep redacted something. It is supposed to find "
            "nothing — D11 and the audit redactor should make a PII-bearing payload "
            "impossible. Investigate before treating this run as routine.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - entry point
    raise SystemExit(main())
