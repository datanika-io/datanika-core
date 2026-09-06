"""The marker for exceptions whose own text we authored and may show a user (core#1094).

Read this before adding an exception class anywhere in core or cloud.

## What it is for

``BaseState._safe_error`` and ``BaseState._set_error`` replace a caught
exception with a curated fallback *unless* the exception's own text is
something we wrote. That decision needs a boundary, and until core#1094 the
boundary was **negative**: *"a ``ValueError``, unless it is one of theirs."*

A negative rule fails in the direction you cannot recover from. core#1032 is
the instance: ``pydantic.ValidationError`` is a ``ValueError`` subclass, so
pydantic's own report — including ``input_value=``, which echoes the offending
value back into a rendered string — reached the billing page while our
``"Failed to load billing data"`` was passed and never used. Nothing had
changed in any handler; the class simply arrived inside ``ValueError``'s
subtree, on a dependency bump whose diff contained no exception handling at
all.

``UserFacingError`` inverts that. The predicate becomes one positive test, and
a class we did not author can never land inside it by inheritance.

## The rule

**Raise ``UserFacingError`` (or a subclass of it) when the message is written
for the person on the other side of the screen.** Raise anything else when it
is not, and accept that the user will see the handler's fallback instead --
that is the recoverable failure, and it is the one this class chooses.

Every deliberate carrier of user-facing text in this codebase inherits it. The
census that established the surface is in ``tests/test_errors.py``: **266 raise
sites**, of which 227 are raises of 25 declared classes and 39 were bare
``raise ValueError(...)``.

## Why it still subclasses ValueError, permanently

Load-bearing, not legacy. Call sites across services, tasks and the UI catch
``ValueError`` to mean *"a refusal we authored"*; ``datanika/ui/state`` alone
holds eight such handlers. Detaching the marker from ``ValueError`` would make
every one of them stop catching, silently, and the failure would surface as an
unhandled exception in a Celery task rather than as a test.

⚠️ The converse is what the migration is for: after core#1094's contract step a
bare ``ValueError`` is **not** user-facing any more, so raising one in a layer
a state handler wraps means the user gets the fallback. ``tests/test_errors.py``
asserts there are none.
"""


class UserFacingError(ValueError):
    """An exception whose ``str()`` was authored for a user and is safe to render.

    Subclass it for a domain-specific carrier (``ScheduleConfigError``,
    ``QuotaExceededError``, ...); raise it directly for a one-off refusal that
    does not earn a class.

    ⚠️ **Do not use it to carry operator or developer text** — an env var to
    set, an internal invariant, a "this branch is unreachable". Those are real
    exceptions and they belong outside the marker, so the user gets the
    handler's fallback and the detail stays in the log.
    """
