"""Every text input in the product must have an accessible name (core#720).

**This rule has one confirmed production instance, and it was found by accident.**
Three E2E specs failed deterministically for months with
``locator.fill: Test timeout 60000ms exceeded``. The cause was not a flake and not a
selector style: ``page.getByLabel(/email/i)`` matches a ``<label>``, an ``aria-label``
or an ``aria-labelledby``, and the Reflex auth forms rendered ``rx.text("Email")`` as a
**sibling ``<p>``** before an unlabelled ``rx.input``. The inputs had no accessible name
at all -- a screen reader announced nothing. Fixed for the auth forms in ``8a5c90d``
(``rx.el.label(..., html_for="login-email")``), and found only because a *test harness*
tripped over it.

**Why a ratchet and not a flat rule.** A flat "all inputs must be named" is red on arrival
in a dozen files, and a gate that must be loosened on its first day teaches everyone to
loosen gates (core#720 says this in its own scope section). So the baseline below records
what is already unnamed, and the assertion is that it does not **grow** -- and that when
it shrinks, it shrinks visibly in a diff.

Where an input is counted (2026-09-15)
--------------------------------------

Until then this guard counted each ``rx.input`` call once, where it is written, and judged
it named only when a string-literal ``id`` was the target of a string-literal ``html_for``.
Two consequences, both measured by mutating the real tree rather than argued:

* **A labelling fix written with an f-string was invisible.** ``config_input()`` builds its
  id as ``f"cfg-{slug}"``, so a correct ``html_for=f"cfg-{slug}"`` moved nothing, and
  lowering the baseline after such a fix went red on correct code.
* **The count hid its own reach.** ``secure_input.py``'s ``2`` stood for **80** connector
  config fields: every ``config_input(...)`` / ``config_text_area(...)`` call renders its own
  input from one line of ``secure_input.py``. A new unlabelled connector field added nothing
  to the count, and a fix reaching 3 of the 80 would have halved it.

So an input whose ``id`` is built from its function's **parameters** is an *input factory*.
It is counted **where the factory is called**, not where it is written, because each call
renders a distinct input that needs a distinct name. A call that passes the id straight
through from its own parameters (``labelled_config_input`` calling ``config_input(field)``)
makes the caller a factory too, and the counting moves out to *its* callers.

An input, or a factory call, is **named** when any of these holds:

1. It carries ``aria_label`` / ``aria_labelledby``. At a factory call site this counts only if
   every factory on the way forwards its ``**kwargs`` into the input.
2. A string-literal ``html_for`` anywhere in the UI names its string-literal id -- or the id a
   factory call **evaluates to** (``config_input("host")`` -> ``"cfg-host"``).
3. A **label binding** reaches it in the same function: an ``html_for=`` whose expression is the
   id's expression, once parameters are replaced by the call's arguments and single-assignment
   locals by their values. The binding may sit in a helper the function calls, but only if that
   helper binds it on **every** return path. It must be on the same return path as the input,
   and in the same branch of any ``if``, conditional expression or ``rx.cond``: a label rendered
   on another path names nothing on this one.

WARNING -- what this still cannot see, stated so nobody assumes otherwise:

* The baseline is a per-file *count*. One change that names an input and adds an unnamed one in
  the same file leaves the count equal and passes.
* An input with a literal id, or no id, inside a helper is counted once and not per call. Naming
  it is one change in the helper, so that is also where its count lives.
* Label bindings are followed one helper deep, and conditions are never evaluated: a helper that
  binds only when a flag is true is not credited even when every caller passes true. That
  **under**-credits and never over-credits, which is the direction a ratchet can afford.

This is not a substitute for an axe sweep (core#720's main scope). It checks one rule,
statically, but on **every** form -- including connection config, the pipeline builder, the
upload wizard and settings, which the E2E harness never reaches.

Proven able to fail by mutating the **real** artifact, not a fixture: stripping ``html_for``
off ``login-email`` in ``datanika/ui/pages/login.py`` reds the auth-form test and the ratchet
(harness: ``plans/qa/notes/probe-720/mutate_720.py``). The factory rules were armed the same
way, against the previous instrument as a control; that matrix is on core#720.
"""

import ast
import collections
import copy
import pathlib
from dataclasses import dataclass, field

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
UI_ROOT = REPO_ROOT / "datanika" / "ui"

#: Calls that render a text-entry control the user types into.
_INPUT_CALLS = {("rx", "input"), ("rx", "text_area"), ("rx", "el", "input")}

#: Props that carry an accessible name on their own, without a paired ``<label>``.
_ARIA_NAME_PROPS = {"aria_label", "aria_labelledby"}

#: Calls whose arguments are alternative branches, like an ``if``.
_CONDITIONAL_CALLS = {("rx", "cond"), ("rx", "match")}

#: String methods a factory id may apply to a literal argument and still be evaluated.
_STR_METHODS = {"replace", "lower", "upper", "strip"}

#: Where unnamed inputs are counted today, and how many each file has.
#:
#: 🚨 **This table may only ever go DOWN because of a product change.** When you give an input
#: a label, lower the number here in the same commit -- that is what makes the progress visible
#: in a diff instead of invisible in a green tick. Raising an entry means shipping a form no
#: screen-reader user can complete.
#:
#: Re-measured 2026-09-15 on ``origin/dev d79c46f`` when the counting rule changed (module
#: docstring): **153 across 14 files**, where the per-call-site rule had reported 65 across 12.
#: The rise is measurement, not regression -- ``secure_input.py`` (2) and ``searchable_select.py``
#: (1) moved out to the **91** call sites that actually render those inputs: config_input 71,
#: config_text_area 6, searchable_select 11, and 3 through labelled_config_input.
KNOWN_UNLABELLED: dict[str, int] = {
    "datanika/ui/components/captcha.py": 1,
    # 71 config_input + 5 config_text_area calls with rx.text beside them, and 2
    # labelled_config_input calls whose label is text, not a bound <label>.
    "datanika/ui/components/connection_config_fields.py": 78,
    # The locale dropdown's filter box, and 10 more like it: searchable_select() builds its
    # input id from a hash of its placeholder, so each call renders its own unnamed input.
    "datanika/ui/components/language_switcher.py": 1,
    "datanika/ui/components/pipeline_mode_selector.py": 1,
    "datanika/ui/pages/audit_logs.py": 2,
    "datanika/ui/pages/connections.py": 3,
    "datanika/ui/pages/dag.py": 3,
    "datanika/ui/pages/model_detail.py": 14,
    "datanika/ui/pages/pipelines.py": 6,
    "datanika/ui/pages/schedules.py": 3,
    "datanika/ui/pages/settings.py": 13,
    "datanika/ui/pages/sql_editor.py": 1,
    "datanika/ui/pages/transformations.py": 9,
    "datanika/ui/pages/uploads.py": 18,
}

#: The eight ids ``8a5c90d`` bound to a ``<label>``. These are the production fix for
#: the defect this module exists for; if one loses its label the regression is the
#: original bug, so they are asserted by name rather than only by count.
LABELLED_AUTH_INPUT_IDS = frozenset(
    {
        "forgot-email",
        "login-email",
        "login-password",
        "reset-confirm",
        "reset-password",
        "signup-email",
        "signup-full-name",
        "signup-password",
    }
)

#: Lower bound on inputs the walk must find. An analyser that silently stops walking
#: returns an empty set, which would satisfy every "no new violations" assertion in this
#: file. A skip is the same colour as a pass; so is a vacuous pass.
_MIN_INPUTS_EXPECTED = 60

_DEAD_FACTORY = "input factory with no resolved call site"


def _dotted(node: ast.AST) -> tuple[str, ...]:
    """``('rx', 'input')`` for ``rx.input``; ``()`` for anything not a dotted name."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return tuple(reversed(parts))
    return ()


def _str_literal(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _text(node: ast.AST) -> str:
    return ast.unparse(node)


def _param_names(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    a = fn.args
    names = {p.arg for p in a.posonlyargs + a.args + a.kwonlyargs}
    names |= {p.arg for p in (a.vararg, a.kwarg) if p is not None}
    return names


def _own_nodes(fn: ast.AST):
    """Every node of ``fn`` except the bodies of nested defs and classes."""
    stack = list(ast.iter_child_nodes(fn))
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            continue
        stack.extend(ast.iter_child_nodes(node))


def _evaluate(node: ast.AST) -> str | None:
    """The string an id expression produces, if it is built only from literals."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.JoinedStr):
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
            elif isinstance(value, ast.FormattedValue):
                if value.conversion != -1 or value.format_spec is not None:
                    return None
                inner = _evaluate(value.value)
                if inner is None:
                    return None
                parts.append(inner)
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _evaluate(node.left), _evaluate(node.right)
        return left + right if left is not None and right is not None else None
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _STR_METHODS
        and not node.keywords
    ):
        base = _evaluate(node.func.value)
        args = [_evaluate(arg) for arg in node.args]
        if base is None or any(arg is None for arg in args):
            return None
        return getattr(base, node.func.attr)(*args)
    return None


class _Module:
    def __init__(self, rel: str, tree: ast.Module):
        self.rel = rel
        self.tree = tree
        parts = rel[: -len(".py")].split("/")
        self.is_package = parts[-1] == "__init__"
        self.dotted = ".".join(parts[:-1] if self.is_package else parts)
        self.parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                self.parents[child] = parent
        self.functions = {
            node.name: node
            for node in tree.body
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        }
        self.imports: dict[str, tuple[str, str]] = {}
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom):
                continue
            base = node.module or ""
            if node.level:
                pkg = self.dotted.split(".") if self.is_package else self.dotted.split(".")[:-1]
                keep = pkg[: len(pkg) - (node.level - 1)] if node.level > 1 else pkg
                base = ".".join(keep + ([node.module] if node.module else []))
            for alias in node.names:
                self.imports[alias.asname or alias.name] = (base, alias.name)


@dataclass
class _Pending:
    """An input whose id still depends on the parameters of ``fn``."""

    mod: _Module
    fn: ast.FunctionDef | ast.AsyncFunctionDef
    id_expr: ast.AST
    node: ast.Call
    forwards: bool
    origin: str


@dataclass
class _Occurrence:
    rel: str
    line: int
    what: str
    named: bool
    reason: str


@dataclass
class _Analysis:
    """One pass over the UI modules: inputs, label bindings, input factories, occurrences."""

    sources: dict[str, bytes | str]
    modules: dict[str, _Module] = field(default_factory=dict)
    html_for: set[str] = field(default_factory=set)
    inputs: list[tuple[str, int, str, str | None, bool]] = field(default_factory=list)
    occurrences: list[_Occurrence] = field(default_factory=list)
    factories: dict[str, list[_Pending]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for rel, src in sorted(self.sources.items()):
            text = src.decode("utf-8") if isinstance(src, bytes) else src
            # A SyntaxError here must NOT be swallowed: a file that cannot be parsed is a
            # file whose inputs are invisible, which reads as "no violations".
            self.modules[rel] = _Module(rel, ast.parse(text, filename=rel))
        self._by_dotted = {m.dotted: m for m in self.modules.values()}
        self._locals: dict[int, dict[str, ast.AST]] = {}
        self._bindings_cache: dict[int, list[tuple[ast.Call, str]]] = {}
        self._always_cache: dict[int, list[ast.AST]] = {}
        self._calls_to: dict[tuple[str, str], list] = collections.defaultdict(list)
        for mod in self.modules.values():
            for node in ast.walk(mod.tree):
                if not isinstance(node, ast.Call):
                    continue
                for kw in node.keywords:
                    if kw.arg == "html_for" and _str_literal(kw.value) is not None:
                        self.html_for.add(_str_literal(kw.value))
                target = self._resolve(mod, node)
                if target is not None:
                    tmod, tfn = target
                    self._calls_to[(tmod.rel, tfn.name)].append(
                        (mod, node, self._enclosing(mod, node))
                    )
        self._count()

    @classmethod
    def of_tree(cls, root: pathlib.Path) -> "_Analysis":
        return cls(
            {
                path.relative_to(REPO_ROOT).as_posix(): path.read_bytes()
                for path in sorted(root.rglob("*.py"))
            }
        )

    @property
    def files_walked(self) -> int:
        return len(self.modules)

    # -- resolution --------------------------------------------------------------------
    def _resolve(self, mod: _Module, call: ast.Call):
        if not isinstance(call.func, ast.Name):
            return None
        name = call.func.id
        if name in mod.functions:
            return mod, mod.functions[name]
        if name in mod.imports:
            dotted, original = mod.imports[name]
            target = self._by_dotted.get(dotted)
            if target is not None and original in target.functions:
                return target, target.functions[original]
        return None

    def _enclosing(self, mod: _Module, node: ast.AST):
        top = set(map(id, mod.functions.values()))
        cur = mod.parents.get(node)
        while cur is not None:
            if id(cur) in top:
                return cur
            cur = mod.parents.get(cur)
        return None

    # -- expressions -----------------------------------------------------------------------
    def _single_assignments(self, fn) -> dict[str, ast.AST]:
        key = id(fn)
        if key not in self._locals:
            counts: collections.Counter[str] = collections.Counter()
            values: dict[str, ast.AST] = {}
            for node in _own_nodes(fn):
                targets: list[ast.AST] = []
                if isinstance(node, ast.Assign):
                    targets = list(node.targets)
                    if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                        values[node.targets[0].id] = node.value
                elif isinstance(
                    node,
                    ast.AnnAssign
                    | ast.AugAssign
                    | ast.NamedExpr
                    | ast.For
                    | ast.AsyncFor
                    | ast.comprehension,
                ):
                    targets = [node.target]
                elif isinstance(node, ast.withitem) and node.optional_vars is not None:
                    targets = [node.optional_vars]
                for target in targets:
                    for name in ast.walk(target):
                        if isinstance(name, ast.Name):
                            counts[name.id] += 1
            params = _param_names(fn)
            self._locals[key] = {
                k: v for k, v in values.items() if counts[k] == 1 and k not in params
            }
        return self._locals[key]

    def _substitute_locals(self, expr: ast.AST, fn) -> ast.AST:
        single = self._single_assignments(fn)

        def sub(node: ast.AST, depth: int) -> ast.AST:
            class _Sub(ast.NodeTransformer):
                def visit_Name(self, name: ast.Name) -> ast.AST:
                    if isinstance(name.ctx, ast.Load) and name.id in single and depth < 5:
                        return sub(copy.deepcopy(single[name.id]), depth + 1)
                    return name

            return _Sub().visit(node)

        return sub(copy.deepcopy(expr), 0)

    @staticmethod
    def _lift(expr: ast.AST, callee, call: ast.Call) -> ast.AST:
        """``expr`` (in ``callee``'s scope) as seen from the caller of ``call``."""
        a = callee.args
        positional = a.posonlyargs + a.args
        mapping: dict[str, ast.AST] = {}
        for index, arg in enumerate(call.args):
            if isinstance(arg, ast.Starred):
                break
            if index < len(positional):
                mapping[positional[index].arg] = arg
        for kw in call.keywords:
            if kw.arg is not None:
                mapping[kw.arg] = kw.value
        for param, default in zip(
            positional[len(positional) - len(a.defaults) :], a.defaults, strict=False
        ):
            mapping.setdefault(param.arg, default)
        for param, default in zip(a.kwonlyargs, a.kw_defaults, strict=False):
            if default is not None:
                mapping.setdefault(param.arg, default)
        params = _param_names(callee)

        class _Lift(ast.NodeTransformer):
            def visit_Name(self, name: ast.Name) -> ast.AST:
                if isinstance(name.ctx, ast.Load) and name.id in params:
                    if name.id in mapping:
                        return copy.deepcopy(mapping[name.id])
                    return ast.Name(id=f"__unbound_{name.id}__", ctx=ast.Load())
                return name

        return _Lift().visit(copy.deepcopy(expr))

    @staticmethod
    def _mentions(expr: ast.AST, names: set[str]) -> bool:
        return any(
            isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id in names
            for n in ast.walk(expr)
        )

    @staticmethod
    def _forwards_kwargs(fn, call: ast.Call) -> bool:
        kwarg = fn.args.kwarg
        return kwarg is not None and any(
            kw.arg is None and isinstance(kw.value, ast.Name) and kw.value.id == kwarg.arg
            for kw in call.keywords
        )

    # -- where a node sits -------------------------------------------------------------------
    def _scope(self, mod: _Module, node: ast.AST, fn) -> ast.AST | None:
        """The ``return`` statement holding ``node``, else ``fn`` itself."""
        cur = mod.parents.get(node)
        while cur is not None and cur is not fn:
            if isinstance(cur, ast.Return):
                return cur
            cur = mod.parents.get(cur)
        return fn

    @staticmethod
    def _within(stmts: list[ast.stmt], node: ast.AST) -> bool:
        return any(stmt is node for stmt in stmts)

    def _branches(self, mod: _Module, node: ast.AST, stop: ast.AST | None) -> frozenset:
        """The alternative branches ``node`` sits in, below ``stop``."""
        out = set()
        prev, cur = node, mod.parents.get(node)
        while cur is not None and cur is not stop:
            key = None
            if isinstance(cur, ast.If | ast.While):
                key = (
                    "body"
                    if self._within(cur.body, prev)
                    else ("orelse" if self._within(cur.orelse, prev) else "test")
                )
            elif isinstance(cur, ast.IfExp):
                key = "body" if prev is cur.body else ("orelse" if prev is cur.orelse else "test")
            elif isinstance(cur, ast.BoolOp):
                key = next((i for i, v in enumerate(cur.values) if v is prev), "?")
            elif isinstance(
                cur, ast.For | ast.AsyncFor | ast.comprehension | ast.Match | ast.Lambda | ast.Try
            ):
                key = "repeat-or-guard"
            elif isinstance(cur, ast.Call) and _dotted(cur.func) in _CONDITIONAL_CALLS:
                key = next((i for i, v in enumerate(cur.args) if v is prev), "kw")
            if key is not None:
                out.add((id(cur), key))
            prev, cur = cur, mod.parents.get(cur)
        return frozenset(out)

    # -- label bindings ----------------------------------------------------------------------
    def _always_binds(self, mod: _Module, fn) -> list[ast.AST]:
        """Expressions ``fn`` passes to ``html_for`` unconditionally on EVERY return path."""
        key = id(fn)
        if key not in self._always_cache:
            returns = [n for n in _own_nodes(fn) if isinstance(n, ast.Return)]
            per_return: list[dict[str, ast.AST]] = []
            for ret in returns:
                found: dict[str, ast.AST] = {}
                if ret.value is not None:
                    for node in ast.walk(ret.value):
                        if not isinstance(node, ast.Call):
                            continue
                        for kw in node.keywords:
                            if kw.arg != "html_for" or _str_literal(kw.value) is not None:
                                continue
                            if self._branches(mod, node, ret):
                                continue
                            expr = self._substitute_locals(kw.value, fn)
                            found[_text(expr)] = expr
                per_return.append(found)
            common = set(per_return[0]).intersection(*per_return[1:]) if per_return else set()
            self._always_cache[key] = [per_return[0][t] for t in sorted(common)]
        return self._always_cache[key]

    def _bindings(self, mod: _Module, fn) -> list[tuple[ast.Call, str]]:
        """``(node, id expression text)`` for every label binding written in ``fn``."""
        key = id(fn)
        if key not in self._bindings_cache:
            out: list[tuple[ast.Call, str]] = []
            for node in _own_nodes(fn):
                if not isinstance(node, ast.Call):
                    continue
                for kw in node.keywords:
                    if kw.arg == "html_for" and _str_literal(kw.value) is None:
                        out.append((node, _text(self._substitute_locals(kw.value, fn))))
                target = self._resolve(mod, node)
                if target is None or target[1] is fn:
                    continue
                tmod, tfn = target
                for expr in self._always_binds(tmod, tfn):
                    lifted = self._substitute_locals(self._lift(expr, tfn, node), fn)
                    out.append((node, _text(lifted)))
            self._bindings_cache[key] = out
        return self._bindings_cache[key]

    def _bound(self, mod: _Module, fn, node: ast.AST, id_text: str) -> bool:
        scope = self._scope(mod, node, fn)
        for binding, text in self._bindings(mod, fn):
            if text != id_text:
                continue
            bscope = self._scope(mod, binding, fn)
            if bscope is scope and self._branches(mod, binding, scope) <= self._branches(
                mod, node, scope
            ):
                return True
            if bscope is fn and self._branches(mod, binding, fn) <= self._branches(mod, node, fn):
                return True
        return False

    # -- counting ----------------------------------------------------------------------------
    def _record(self, mod: _Module, node: ast.AST, what: str, named: bool, reason: str) -> None:
        self.occurrences.append(_Occurrence(mod.rel, node.lineno, what, named, reason))

    def _count(self) -> None:
        queue: collections.deque[_Pending] = collections.deque()
        for mod in self.modules.values():
            for node in ast.walk(mod.tree):
                if not (isinstance(node, ast.Call) and _dotted(node.func) in _INPUT_CALLS):
                    continue
                what = ".".join(_dotted(node.func))
                fn = self._enclosing(mod, node)
                kws = {kw.arg: kw.value for kw in node.keywords if kw.arg}
                has_aria = bool(set(kws) & _ARIA_NAME_PROPS)
                id_node = kws.get("id")
                id_literal = _str_literal(id_node)
                self.inputs.append((mod.rel, node.lineno, what, id_literal, has_aria))
                if has_aria:
                    self._record(mod, node, what, True, "aria prop")
                elif id_node is None:
                    self._record(mod, node, what, False, "no id and no aria prop")
                elif id_literal is not None:
                    named = id_literal in self.html_for
                    self._record(mod, node, what, named, f"id={id_literal!r}")
                elif fn is None:
                    self._record(mod, node, what, False, "non-literal id outside a function")
                else:
                    expr = self._substitute_locals(id_node, fn)
                    if self._bound(mod, fn, node, _text(expr)):
                        self._record(mod, node, what, True, f"label bound in {fn.name}()")
                    elif self._mentions(expr, _param_names(fn)):
                        queue.append(
                            _Pending(
                                mod,
                                fn,
                                expr,
                                node,
                                self._forwards_kwargs(fn, node),
                                f"{what} in {mod.rel}:{node.lineno}",
                            )
                        )
                    else:
                        value = _evaluate(expr)
                        named = value is not None and value in self.html_for
                        self._record(mod, node, what, named, f"id={_text(expr)}")

        seen: set[tuple[int, int]] = set()
        while queue:
            pend = queue.popleft()
            if (id(pend.fn), id(pend.node)) in seen:
                continue
            seen.add((id(pend.fn), id(pend.node)))
            self.factories.setdefault(f"{pend.mod.rel}::{pend.fn.name}", []).append(pend)
            sites = self._calls_to.get((pend.mod.rel, pend.fn.name), [])
            if not sites:
                self._record(pend.mod, pend.node, pend.origin, False, _DEAD_FACTORY)
                continue
            for cmod, call, cfn in sites:
                what = f"{pend.fn.name}(...)"
                names = {kw.arg for kw in call.keywords if kw.arg}
                if names & _ARIA_NAME_PROPS and pend.forwards:
                    self._record(cmod, call, what, True, "aria prop forwarded to the input")
                    continue
                lifted = self._lift(pend.id_expr, pend.fn, call)
                if cfn is not None:
                    lifted = self._substitute_locals(lifted, cfn)
                    if self._bound(cmod, cfn, call, _text(lifted)):
                        self._record(cmod, call, what, True, f"label bound in {cfn.name}()")
                        continue
                    if self._mentions(lifted, _param_names(cfn)):
                        queue.append(
                            _Pending(
                                cmod,
                                cfn,
                                lifted,
                                call,
                                pend.forwards and self._forwards_kwargs(cfn, call),
                                pend.origin,
                            )
                        )
                        continue
                value = _evaluate(lifted)
                named = value is not None and value in self.html_for
                shown = repr(value) if value is not None else _text(lifted)
                self._record(cmod, call, what, named, f"id={shown} from {pend.origin}")

    # -- results -----------------------------------------------------------------------------
    def is_named(self, id_literal: str | None, has_aria: bool) -> bool:
        """The literal rule: aria, or a literal label pointed at a literal id.

        ``placeholder`` is deliberately **not** accepted. It is announced inconsistently,
        disappears the moment the field has a value, and treating it as a name is what
        makes an unlabelled form look compliant.
        """
        if has_aria:
            return True
        return id_literal is not None and id_literal in self.html_for

    def unnamed_by_file(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for occ in self.occurrences:
            if not occ.named:
                counts[occ.rel] = counts.get(occ.rel, 0) + 1
        return counts

    def unnamed_in(self, rel: str) -> list[_Occurrence]:
        return sorted(
            (o for o in self.occurrences if o.rel == rel and not o.named), key=lambda o: o.line
        )


@pytest.fixture(scope="module")
def walk() -> _Analysis:
    return _Analysis.of_tree(UI_ROOT)


def test_the_walk_actually_reaches_the_inputs(walk: _Analysis):
    """Anti-vacuity. Every other test here is satisfied by finding nothing."""
    assert walk.files_walked >= 20, (
        f"Only {walk.files_walked} UI modules parsed. The walk is not reaching "
        f"{UI_ROOT}, so 'no new unlabelled inputs' below means nothing."
    )
    assert len(walk.inputs) >= _MIN_INPUTS_EXPECTED, (
        f"Found only {len(walk.inputs)} input controls under {UI_ROOT}, expected at "
        f"least {_MIN_INPUTS_EXPECTED}. Either the walk broke, or the app lost most of "
        "its forms. Both need a human -- do not lower this bound to make it pass."
    )
    assert walk.html_for, (
        "No html_for= target was found anywhere in the UI. Since an input is judged "
        "named by pointing at one, an empty set would mark every input in the product "
        "as a violation -- or, with a baseline this wide, hide a real regression."
    )
    cross_module = [o for o in walk.occurrences if "(...)" in o.what]
    assert cross_module, (
        "No input was counted at a factory call site. connection_config_fields.py alone "
        "calls config_input() dozens of times, so an empty list means call resolution "
        "broke -- and every connector field would silently fall out of the count."
    )
    dead = [o for o in walk.occurrences if o.reason == _DEAD_FACTORY]
    assert not dead, (
        "These inputs build their id from parameters, but no call to the function holding "
        "them was resolved, so they are counted where they are written instead of where "
        "they render:\n"
        + "\n".join(f"  {o.rel}:{o.line}  {o.what}" for o in dead)
        + "\nEither the function is dead code, or it is imported in a way _resolve does not "
        "follow (an alias, a module attribute). Fix the resolution rather than the baseline."
    )


def test_no_form_gains_an_input_without_an_accessible_name(walk: _Analysis):
    """The ratchet. Compared per file and in both directions, on purpose."""
    actual = walk.unnamed_by_file()
    regressions, improvements = [], []

    for rel in sorted(set(actual) | set(KNOWN_UNLABELLED)):
        now = actual.get(rel, 0)
        allowed = KNOWN_UNLABELLED.get(rel, 0)
        if now > allowed:
            detail = "\n".join(
                f"      L{o.line:<5} {o.what:<28} {o.reason}" for o in walk.unnamed_in(rel)
            )
            regressions.append(
                f"  {rel}: {allowed} allowed -> {now} found (+{now - allowed})\n{detail}"
            )
        elif now < allowed:
            improvements.append(f'    "{rel}": {now},   # was {allowed}')

    assert not regressions, (
        "An input was added with no accessible name -- a screen reader announces "
        "nothing for it, and `page.getByLabel(...)` cannot find it either:\n"
        + "\n".join(regressions)
        + "\n\nGive it a name: rx.el.label(..., html_for='some-id') paired with "
        "rx.input(id='some-id'), as datanika/ui/pages/login.py does. A neighbouring "
        "rx.text() is NOT a label -- that is exactly the defect (core#720)."
    )
    assert not improvements, (
        "Inputs were given accessible names -- thank you. Lower KNOWN_UNLABELLED in "
        "this file to lock the improvement in, or the next regression will be "
        "measured against a stale ceiling:\n" + "\n".join(improvements)
    )


def test_the_baseline_names_only_files_that_still_exist(walk: _Analysis):
    """A baseline entry for a deleted file is a permanent free pass.

    Deleting the form is one of the ways this guard could be 'satisfied' without any
    input gaining a name, so a stale entry has to fail rather than quietly hold.
    """
    missing = [rel for rel in KNOWN_UNLABELLED if not (REPO_ROOT / rel).exists()]
    assert not missing, (
        "KNOWN_UNLABELLED names files that no longer exist: "
        + ", ".join(sorted(missing))
        + ". Remove the entries -- an exemption for a file nobody can see is one "
        "nobody will ever revisit."
    )


def test_the_auth_forms_keep_the_accessible_names_they_were_given(walk: _Analysis):
    """Direct regression guard on 8a5c90d -- the production fix this module descends from."""
    lost = sorted(LABELLED_AUTH_INPUT_IDS - walk.html_for)
    assert not lost, (
        "These auth inputs lost the <label> that 8a5c90d gave them: "
        + ", ".join(lost)
        + ". This is the original core#720 defect returning: the field renders, the "
        "text beside it still reads correctly, and both a screen reader and "
        "page.getByLabel() get nothing."
    )

    named_ids = {
        id_literal
        for _rel, _line, _call, id_literal, has_aria in walk.inputs
        if id_literal is not None and walk.is_named(id_literal, has_aria)
    }
    orphaned = sorted(LABELLED_AUTH_INPUT_IDS - named_ids)
    assert not orphaned, (
        "A <label html_for=...> still exists for these ids but no input carries them: "
        + ", ".join(orphaned)
        + ". A label pointing at nothing is not a name; check the input's id= was not "
        "renamed or made dynamic."
    )


@pytest.mark.parametrize(
    ("source", "expected", "why"),
    [
        ('rx.input(aria_label="Email")', True, "aria_label is an accessible name"),
        ('rx.input(aria_labelledby="email-hint")', True, "aria_labelledby is one too"),
        ('rx.input(id="login-email")', True, "an id a label points at"),
        ('rx.input(placeholder="Email")', False, "placeholder is NOT an accessible name"),
        ('rx.input(name="email", type="email")', False, "name= is submitted, not announced"),
        ('rx.input(id="not-pointed-at")', False, "an id no label points at names nothing"),
    ],
)
def test_the_naming_rule_itself(walk: _Analysis, source: str, expected: bool, why: str):
    """Guard the guard.

    The real control for this module is the mutation harness against the real UI -- a
    synthetic case is written from the same model as the check and agrees with it
    including where it is wrong (WORKFLOW_RULES §13). These pin the one thing the
    mutation cannot show: that `placeholder` and `name` are rejected, which is the
    distinction the whole rule rests on.
    """
    call = ast.parse(source).body[0].value
    kwargs = {kw.arg for kw in call.keywords if kw.arg}
    id_literal = next((_str_literal(kw.value) for kw in call.keywords if kw.arg == "id"), None)
    named = walk.is_named(id_literal, bool(kwargs & _ARIA_NAME_PROPS))
    assert named is expected, f"{source} -> named={named}, expected {expected}: {why}"


_COMPONENT = "datanika/ui/components/fields.py"
_PAGE = "datanika/ui/pages/form.py"
_IMPORT = "from datanika.ui.components.fields import field_input, closed_input, labelled, both\n"
_FIELD_INPUT = (
    "def field_input(field, **props):\n"
    "    slug = field.replace('_', '-')\n"
    "    return rx.input(id=f'cfg-{slug}', **props)\n"
)
_CLOSED_INPUT = "def closed_input(field):\n    return rx.input(id=f'cfg-{field}')\n"


def _labelled(label_body: str) -> str:
    return (
        _FIELD_INPUT
        + "def field_label(label, *, field, bind=True):\n"
        + "    slug = field.replace('_', '-')\n"
        + label_body
        + "def labelled(label, field, **props):\n"
        + "    return rx.fragment(field_label(label, field=field), field_input(field, **props))\n"
    )


_BOUND = "    return rx.el.label(label, html_for=f'cfg-{slug}')\n"
_BOUND_EVERY_PATH = (
    "    if bind:\n"
    "        return rx.el.label(label, html_for=f'cfg-{slug}')\n"
    "    return rx.el.label(label, ' ', html_for=f'cfg-{slug}')\n"
)
_BOUND_ONE_PATH = (
    "    if bind:\n"
    "        return rx.el.label(label, html_for=f'cfg-{slug}')\n"
    "    return rx.text(label)\n"
)
_WRONG_EXPRESSION = "    return rx.el.label(label, html_for=f'cfg-{label}')\n"


@pytest.mark.parametrize(
    ("component", "page", "expected", "why"),
    [
        (
            _FIELD_INPUT,
            "def form():\n    return rx.vstack(rx.text('Host'), field_input('host'))\n",
            {_PAGE: 1},
            "a factory input is counted where it is called, not where it is written",
        ),
        (
            _FIELD_INPUT,
            "def form():\n"
            "    return rx.vstack(rx.el.label('Host', html_for='cfg-host'), field_input('host'))\n",
            {},
            "a literal label names the id the call evaluates to",
        ),
        (
            _labelled(_BOUND),
            "def form():\n    return labelled('Host', 'host')\n",
            {},
            "a wrapper whose helper binds the same expression names every call",
        ),
        (
            _labelled(_BOUND_EVERY_PATH),
            "def form():\n    return labelled('Host', 'host')\n",
            {},
            "a helper that binds on every return path counts",
        ),
        (
            _labelled(_BOUND_ONE_PATH),
            "def form():\n    return labelled('Host', 'host')\n",
            {_PAGE: 1},
            "a helper that binds on only one return path does not count",
        ),
        (
            _labelled(_WRONG_EXPRESSION),
            "def form():\n    return labelled('Host', 'host')\n",
            {_PAGE: 1},
            "a label bound to a different expression names nothing",
        ),
        (
            "def both(label, field):\n"
            "    slug = field.replace('_', '-')\n"
            "    return rx.cond(State.edit, rx.el.label(label, html_for=f'cfg-{slug}'),"
            " rx.input(id=f'cfg-{slug}'))\n",
            "def form():\n    return both('Host', 'host')\n",
            {_PAGE: 1},
            "a label in the other rx.cond branch names nothing",
        ),
        (
            _FIELD_INPUT,
            "def form():\n    return field_input('host', aria_label='Host')\n",
            {},
            "aria at a call site counts when the factory forwards **props",
        ),
        (
            _CLOSED_INPUT,
            "def form():\n    return closed_input('host', aria_label='Host')\n",
            {_PAGE: 1},
            "aria at a call site does not count when nothing forwards it",
        ),
        (
            _FIELD_INPUT,
            "def form():\n    return rx.text('no input here')\n",
            {_COMPONENT: 1},
            "a factory nobody calls is counted where it is written, not dropped",
        ),
    ],
)
def test_where_an_input_is_counted(component: str, page: str, expected: dict, why: str):
    """Guard the guard, for the factory rules. Same caveat as above: synthetic cases agree
    with the model they were written from, so the real control is the mutation matrix on
    core#720. These pin the rules against silent drift, one rule per case -- and the whole
    per-file result is compared, so an input counted in the wrong file fails here."""
    analysis = _Analysis({_COMPONENT: component, _PAGE: _IMPORT + page})
    assert analysis.unnamed_by_file() == expected, (
        f"{why}: counted {analysis.unnamed_by_file()}, expected {expected}"
    )
