"""The product's accent and gray, chosen once (``docs/specs/SPEC_BUTTON_CONTRAST.md`` §0, §4).

Reflex defaults ``rx.App`` to ``accent_color="blue"`` (``reflex/app.py``), so an app that passes
no theme is not an app with no accent — it is an app wearing a choice nobody here made, and
Radix's ``blue-9`` carries white text at **3.26:1**, under WCAG AA. Every solid button in the
product inherited that. The accent is therefore stated rather than defaulted.

Two constants rather than two literals inside ``rx.App(...)``, for one reason each:

* :data:`ACCENT_COLOR` is also needed *away* from the theme call. A colour computed at render
  time — ``run_in_progress_color``, which paints the Run button yellow while a run is going —
  has to be able to name "the accent" for its other branch, and a second hard-coded ``"violet"``
  is a second place to forget. The guard reads it from here too, so it grades whatever the
  product is actually set to rather than a colour this file once mentioned.
* :data:`GRAY_COLOR` would otherwise not exist at all. ``gray_color`` defaults to ``"auto"``,
  and Radix maps the accent to a gray behind your back (``violet → mauve``,
  ``blue/indigo/iris → slate``). So **choosing the accent silently rechooses every gray in the
  product**, and ~88 text colours now reference the gray scale. Pinning it to the same value
  ``auto`` would have picked changes nothing today and makes a future accent change a two-line
  decision instead of a one-line surprise. *A value implied by another value is a value nothing
  reviews.*

⚠️ **Changing :data:`ACCENT_COLOR` expires SPEC_BUTTON_CONTRAST §7.** Every contrast number in
that spec is an artifact of this pair plus the pinned ``@radix-ui/themes`` version, and
:data:`GRAY_COLOR` is pinned to the gray that pairs with *violet* — so the two move together or
§4 becomes wrong silently. ``tests/test_ui/test_buttons_are_legible.py`` recomputes rather than
remembers, so the change lands as a red rather than as a drift.
"""

from __future__ import annotations

#: The Radix accent scale. ``violet-9`` is ``#6e56cf``, 5.39:1 against white — over ``indigo``
#: (5.21:1) because it is already the brand: the landing site builds ``.btn-primary`` on
#: Tailwind ``violet-500``, and the app had already overridden Reflex's blue by hand at its nine
#: most prominent calls to action. 🔑 The brand hex itself (``#8b5cf6``) measures **4.23:1** and
#: fails AA — Radix's step is the same hue a little deeper, so this keeps the brand *and* fixes
#: the contrast instead of trading one for the other. **Take the Radix step, never the landing
#: hex.**
ACCENT_COLOR = "violet"

#: The Radix gray scale. ``mauve`` is what ``gray_color="auto"`` pairs with ``violet``; it is
#: written down so it is reviewed. Accessibility does not discriminate between the candidates on
#: white (``gray-11`` 5.92:1, ``mauve-11`` 5.90:1, ``slate-11`` 5.94:1), which is precisely why
#: it is a decision rather than a calculation.
GRAY_COLOR = "mauve"
