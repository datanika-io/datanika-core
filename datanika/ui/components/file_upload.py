"""``rx.upload`` whose hidden file input carries an accessible name (core#1568).

``rx.upload`` renders a react-dropzone wrapper containing **two** things: the children you pass
(normally a visible ``rx.button``) and a separate ``<input type="file">`` which is the actual form
control. The button is a **sibling**, not a label, so it names nothing — and the input reaches the
accessibility tree, which is how the core#720 sweep scored it ``label`` / critical. The ``label``
rule does not evaluate elements outside that tree, so a genuinely hidden input would have been
reported ``inapplicable``; it was scored, so assistive technology gets there.

🚨 **Two things that look like the fix and are not.** Both were measured against the rendered tree
before this module was written, not reasoned about:

* ``rx.upload(..., aria_label=...)`` — ``Upload`` declares exactly eleven props (``accept``,
  ``disabled``, ``max_files``, ``max_size``, ``min_size``, ``multiple``, ``no_click``, ``no_drag``,
  ``no_keyboard``, ``on_drop``, ``on_drop_rejected``) and any other prop is forwarded to the wrapper
  ``Box``. So the name lands on a ``<div>`` and the input stays anonymous. This is the same trap as
  ``rx.select(..., aria_label=...)`` recorded in
  ``tests/test_ui/test_controls_have_accessible_names``, one level deeper.
* ``rx.el.label(..., html_for="backup_upload")`` — ``rx.upload``'s ``id=`` also lands on the
  ``Box``, so a label pointing at it names the ``<div>``. Reflex builds the input as a bare
  ``Input.create(type="file")`` with **no id at all**, so there is nothing for ``html_for`` to
  target.

The only reachable surface is the built component tree, so that is what this module edits. The
rendered order is ``aria-label``, ``type``, then the spread ``...getInputProps()`` — react-dropzone
wins every key it sets, and it does not set ``aria-label``, so the name survives.
"""

from collections.abc import Iterator

import reflex as rx


def _file_inputs(component: rx.Component) -> Iterator[rx.Component]:
    """Every ``<input type="file">`` in the tree rooted at ``component``."""
    stack = [component]
    while stack:
        node = stack.pop()
        if getattr(node, "tag", None) == "input":
            kind = getattr(node, "type", None)
            # `str(kind)`, never `kind or ""`: `type` is a Reflex Var and bool() on one raises.
            if kind is not None and "file" in str(kind):
                yield node
        stack.extend(getattr(node, "children", None) or [])


def named_upload(*children, accessible_name, **props) -> rx.Component:
    """An ``rx.upload`` whose ``<input type="file">`` has an accessible name.

    Args:
        *children: The visible affordance, normally an ``rx.button``.
        accessible_name: What a screen reader announces for the file input. Translated copy —
            an ``aria-label`` is read aloud, so it is a label like any other. Passing the same
            key the visible button uses is correct and is what every call site does: the
            accessible name then matches the visible label (WCAG 2.5.3 Label in Name).
        **props: Forwarded to ``rx.upload`` unchanged.

    Returns:
        The upload component, with the name attached to the input itself.

    Raises:
        TypeError: ``aria_label``/``aria_labelledby`` was passed. Those reach the wrapper ``Box``,
            not the input, so accepting them would ship a silent non-fix.
        RuntimeError: no file input was found in the built tree — i.e. a Reflex upgrade changed
            what ``rx.upload`` renders. Failing here is deliberate: the alternative is an app that
            builds cleanly and ships an unnamed control, which is the defect this module closes.
            ``tests/test_ui/test_every_page_constructs.py`` builds every page, so CI catches it.
    """
    forbidden = {"aria_label", "aria_labelledby"} & set(props)
    if forbidden:
        msg = (
            f"named_upload() does not take {', '.join(sorted(forbidden))}: rx.upload forwards any "
            "prop it does not declare to its wrapper <div>, so that would name the div and leave "
            "the file input anonymous. Use accessible_name=, which names the input itself."
        )
        raise TypeError(msg)

    component = rx.upload(*children, **props)
    inputs = list(_file_inputs(component))
    if not inputs:
        msg = (
            "rx.upload rendered no <input type='file'>, so named_upload() cannot give it an "
            "accessible name. Reflex's Upload.create builds the input as Input.create(type='file') "
            "inside a Box; if that changed, re-derive the tree shape and update _file_inputs(). Do "
            "not silently skip the naming — an unnamed file input is core#1568."
        )
        raise RuntimeError(msg)

    for file_input in inputs:
        attrs = dict(file_input.custom_attrs or {})
        attrs.setdefault("aria-label", accessible_name)
        file_input.custom_attrs = attrs
    return component
