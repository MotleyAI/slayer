"""Moved to :mod:`slayer.core.syntax` in SLayer 2.0; a known 1.x integrator's names stay, each deprecated."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from slayer.core import syntax
from slayer.core.deprecation import deprecated_input, raise_moved

_BETWEEN = re.compile(r"\bbetween\b", re.IGNORECASE)
#: Node classes 2.0 builds for text the 1.x parser refused.
_REFUSED = (syntax.Cast, syntax.TemporalLiteral, syntax.BoundVariable)


def _refused(text: str, why: str) -> ValueError:
    return ValueError(f"Invalid filter expression {text!r}: {why} is not supported by parse_filter_expr")


def _code(text: str) -> str:
    """``text`` with its string literals blanked."""
    out = text
    for span in syntax.string_literal_spans(text):
        out = out[:span.start] + " " * (span.end - span.start) + out[span.end:]
    return out


def _call_parts(args: tuple[Any, ...], kwargs: tuple[tuple[str, Any], ...], project: Callable[[Any], Any]) -> dict[str, Any]:
    named = [*kwargs, *((a.name, a.value) for a in args if isinstance(a, syntax.NamedArg))]
    return {
        "args": tuple(project(a) for a in args if not isinstance(a, syntax.NamedArg)),
        "kwargs": tuple((name, project(value)) for name, value in named),
    }


def _projected(node: Any, *, text: str) -> Any:
    """``node`` with each 2.0-only node class rewritten to the node the 1.x parser built for the same text."""
    def project(item: Any) -> Any:
        return _projected(item, text=text)

    if isinstance(node, tuple):
        return tuple(project(n) for n in node)
    if isinstance(node, _REFUSED):
        raise _refused(text, type(node).__name__)
    if isinstance(node, syntax.DateTrunc):
        return syntax.AggCall(source=syntax.Literal(value=node.unit), agg="date_trunc", args=(project(node.value),))
    if isinstance(node, syntax.AggCall):
        if node.filter is not None:
            raise _refused(text, "FILTER (WHERE …)")
        return node.model_copy(update={"source": project(node.source), **_call_parts(node.args, node.kwargs, project)})
    if isinstance(node, syntax.TransformCall):
        return node.model_copy(update={"input": project(node.input), **_call_parts(node.args, node.kwargs, project)})
    if isinstance(node, syntax.ScalarCall):
        return node.model_copy(update={"args": project(node.args), "written_args": None})
    if isinstance(node, (syntax.Arith, syntax.Cmp)):
        return node.model_copy(update={"left": project(node.left), "right": project(node.right)})
    if isinstance(node, syntax.UnaryOp):
        return node.model_copy(update={"operand": project(node.operand)})
    if isinstance(node, (syntax.BoolOp,)):
        return node.model_copy(update={"operands": project(node.operands)})
    if isinstance(node, syntax.ArrayLit):
        return node.model_copy(update={"elements": project(node.elements)})
    if isinstance(node, (syntax.Ref, syntax.DottedRef, syntax.StarSource, syntax.Literal, syntax.Placeholder,
                         syntax.TupleLit)):
        return node
    raise _refused(text, type(node).__name__)


def _parse_filter_expr(text: str) -> Any:
    """``text`` read by the 2.0 parser, projected onto the 1.x node shapes; text the 1.x parser refused raises."""
    spellings = syntax.nonossie_spellings(text)
    tree = syntax.parse_expr(text)
    if spellings:
        raise _refused(text, f"the {spellings[0]!r} spelling")
    if _BETWEEN.search(_code(text)):
        raise _refused(text, "BETWEEN")
    return _projected(tree, text=text)


_PARSE = "slayer.core.syntax.parse_expr (a different tree shape: see the upgrade guide, Integrators reading SLayer objects)"
_KEPT: dict[str, tuple[Callable[[], object], object]] = {
    "AggCall": (lambda: deprecated_input("slayer.engine.syntax.AggCall", "slayer.core.syntax.AggCall", removal="3.0"),
                syntax.AggCall),
    "Arith": (lambda: deprecated_input("slayer.engine.syntax.Arith", "slayer.core.syntax.Arith", removal="3.0"),
              syntax.Arith),
    "ArrayLit": (lambda: deprecated_input("slayer.engine.syntax.ArrayLit", "slayer.core.syntax.ArrayLit",
                                          removal="3.0"), syntax.ArrayLit),
    "BoolOp": (lambda: deprecated_input("slayer.engine.syntax.BoolOp", "slayer.core.syntax.BoolOp", removal="3.0"),
               syntax.BoolOp),
    "Cmp": (lambda: deprecated_input("slayer.engine.syntax.Cmp", "slayer.core.syntax.Cmp", removal="3.0"), syntax.Cmp),
    "DottedRef": (lambda: deprecated_input("slayer.engine.syntax.DottedRef", "slayer.core.syntax.DottedRef",
                                           removal="3.0"), syntax.DottedRef),
    "Literal": (lambda: deprecated_input("slayer.engine.syntax.Literal", "slayer.core.syntax.Literal", removal="3.0"),
                syntax.Literal),
    "Placeholder": (lambda: deprecated_input("slayer.engine.syntax.Placeholder", "slayer.core.syntax.Placeholder",
                                             removal="3.0"), syntax.Placeholder),
    "Ref": (lambda: deprecated_input("slayer.engine.syntax.Ref", "slayer.core.syntax.Ref", removal="3.0"), syntax.Ref),
    "ScalarCall": (lambda: deprecated_input("slayer.engine.syntax.ScalarCall", "slayer.core.syntax.ScalarCall",
                                            removal="3.0"), syntax.ScalarCall),
    "StarSource": (lambda: deprecated_input("slayer.engine.syntax.StarSource", "slayer.core.syntax.StarSource",
                                            removal="3.0"), syntax.StarSource),
    "TransformCall": (lambda: deprecated_input("slayer.engine.syntax.TransformCall", "slayer.core.syntax.TransformCall",
                                               removal="3.0"), syntax.TransformCall),
    "TupleLit": (lambda: deprecated_input("slayer.engine.syntax.TupleLit", "slayer.core.syntax.TupleLit",
                                          removal="3.0"), syntax.TupleLit),
    "UnaryOp": (lambda: deprecated_input("slayer.engine.syntax.UnaryOp", "slayer.core.syntax.UnaryOp", removal="3.0"),
                syntax.UnaryOp),
    "parse_filter_expr": (lambda: deprecated_input("slayer.engine.syntax.parse_filter_expr", _PARSE, removal="3.0"),
                          _parse_filter_expr),
}

if not TYPE_CHECKING:  # the moved names stay static errors

    def __getattr__(name: str) -> object:
        if name.startswith("__"):
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        if name not in _KEPT:
            raise_moved("slayer.engine.syntax", "slayer.core.syntax", removal="3.0", attribute=True)
        warn, value = _KEPT[name]
        warn()
        return value
