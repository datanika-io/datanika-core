"""Weave components into a *translated sentence* instead of concatenating fragments.

A sentence assembled in Python — a translated lead-in, a space, the Terms link, a
hardcoded separator, the Privacy link — pins English word order, an English
connective and (implicitly) English's lack of grammatical gender into the layout
code. Every fragment was translated and
``test_all_locales_have_same_keys`` passed, and the result was still
ungrammatical in `ru`, `es` and `el`: the determiner, the case and the connective
all live *between* the fragments, where no translator could reach them (#682).

The fix is not better translations. It is giving each locale the whole sentence
and letting it decide where the links go.
"""


def interpolate(template, **slots) -> list:
    """Return children for ``rx.text(...)``, with ``{name}`` replaced by components.

    ``template`` is a reactive ``Var`` (a value out of ``I18nState.translations``),
    so the substitution compiles to ``String.prototype.split`` in the browser
    rather than happening here — the locale is not known at build time.

    ⚠️ **Slots must appear in the template in the order they are passed.** The
    split is sequential and nothing can inspect the string during compilation, so
    a locale that swapped ``{privacy}`` and ``{terms}`` would render ``undefined``
    for the tail. That constraint is enforced per locale by
    ``tests/test_ui/test_signup_legal_sentence.py``, not left as a comment here —
    the whole reason #682 existed is that a defect of exactly this shape was
    invisible to every test in the suite.

    A missing placeholder degrades the same way, and is caught by the same test.
    """
    children: list = []
    rest = template
    for name, component in slots.items():
        parts = rest.split("{" + name + "}")
        children.append(parts[0])
        children.append(component)
        rest = parts[1]
    children.append(rest)
    return children
