"""Ossie Layer 1 conformance of a Mode B filter: which constructs ``OSSIE_SQL_2026`` defines with SLayer's meaning.

The judgment reads the one Mode B parse (``core.syntax``): its node kinds and operators, the calls as written
(each scalar's ``ossie`` flag) and the non-Ossie spellings the parser reports rewriting.
"""

from __future__ import annotations

import datetime
import re
from collections.abc import Callable, Iterable
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import AMBIGUOUS_WEEKDAY_NAMES, DATE_PART_ALIASES, Conformance
from slayer.core.errors import IllegalWindowInFilterError, UnknownConformanceError
from slayer.core.keys import SCALAR_SPECS, SCALAR_SPELLINGS
from slayer.core.syntax import (
    AggCall, ArrayLit, Arith, BoolOp, BoundVariable, Cast, Cmp, DateTrunc, DottedRef, Literal, NamedArg, Placeholder,
    Ref, ScalarCall, StarSource, TemporalLiteral, TransformCall, TupleLit, UnaryOp, call_spans, legacy_spans,
    nonossie_spellings, parse_template, variable_names,
)

__all__ = ["FilterReading", "is_iso_temporal", "parse_conformance", "read_filter"]

#: Ossie's comparison operators (``is`` / ``is not`` only against NULL).
OSSIE_COMPARISONS = frozenset({"==", "!=", "<", "<=", ">", ">=", "in", "not in", "is", "is not", "is distinct from",
                               "is not distinct from"})
OSSIE_ARITHMETIC = frozenset({"+", "-", "*", "/", "%"})
OSSIE_BOOLEAN = frozenset({"and", "or"})
OSSIE_UNARY = frozenset({"-", "not"})
#: Rewritten callee spellings Ossie defines with the meaning the rewrite gives them.
OSSIE_CALL_SPELLINGS = frozenset({"iff", "datediff", "dateadd", "current_timestamp", "date_trunc", "year", "quarter",
                                  "month", "day", "dayofyear", "hour", "minute", "second"})
#: ``CAST`` target types Ossie defines (the first word of the written type).
OSSIE_CAST_TYPES = frozenset({"VARCHAR", "STRING", "INTEGER", "INT", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE",
                              "BOOLEAN", "DATE", "TIMESTAMP"})
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_ISO_TIME = re.compile(r"[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?")
_ISO_ZONE = re.compile(r"Z|[+-]\d{2}:?\d{2}")

Reference = Ref | DottedRef


class LiteralComparison(BaseModel):
    """A reference compared with string literals (a temporal field takes only ISO-8601 ones)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    reference: Any
    texts: Tuple[str, ...]


class FilterReading(BaseModel):
    """What the strict binding needs of a filter: why it is no Ossie expression, a window or aggregate in it, its
    references and its comparisons with string literals."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    problem: Optional[str] = None
    window: bool = False
    aggregate: bool = False
    references: List[Any] = []
    comparisons: List[LiteralComparison] = []


def parse_conformance(value: Any) -> Conformance:
    """``value`` as a conformance; raises ``UnknownConformanceError`` naming the two values."""
    try:
        return Conformance(value)
    except ValueError:
        raise UnknownConformanceError(value) from None


def is_iso_temporal(text: str) -> bool:
    """Whether ``text`` is an ISO-8601 date or timestamp."""
    if not _iso_shaped(text):
        return False
    try:
        datetime.datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


def _iso_shaped(text: str) -> bool:
    """A date, optionally a time, then optionally a zone."""
    date = _ISO_DATE.match(text)
    if date is None:
        return False
    rest = text[date.end():]
    if not rest:
        return True
    time = _ISO_TIME.match(rest)
    if time is None:
        return False
    rest = rest[time.end():]
    return not rest or _ISO_ZONE.fullmatch(rest) is not None


def read_filter(text: str) -> FilterReading:
    """Judge filter ``text`` (its variables already bound) against Ossie Layer 1."""
    out = FilterReading()
    if variable_names(text):
        out.problem = f"the variables {variable_names(text)} are not bound; pass them as the run's variables"
        return out
    try:
        tree = parse_template(text)
    except IllegalWindowInFilterError:
        out.window = True
        return out
    except ValueError as exc:
        out.problem = f"the filter does not parse: {exc}"
        return out
    out.problem = _text_problem(text)
    if out.problem is None:
        _visit(tree, out)
    return out


def _text_problem(text: str) -> Optional[str]:
    legacy = legacy_spans(text)
    if legacy:
        return f"{legacy[0].written!r} is a deprecated SLayer spelling; write {legacy[0].twin!r}"
    spellings = nonossie_spellings(text)
    if spellings:
        return f"{spellings[0]} is SLayer syntax, not an Ossie expression"
    for call in call_spans(text):
        name = call.callee.lower()
        spec = SCALAR_SPECS.get(name)
        if spec is not None and not spec.ossie:
            return f"{call.callee}() is a SLayer function Ossie does not define"
        if spec is None and name in (SCALAR_SPELLINGS | set(DATE_PART_ALIASES) | AMBIGUOUS_WEEKDAY_NAMES) \
                and name not in OSSIE_CALL_SPELLINGS:
            return f"{call.callee}() is a SLayer function Ossie does not define"
    return None


def _refuse(out: FilterReading, why: str) -> None:
    if out.problem is None:
        out.problem = why


def _visit(node: Any, out: FilterReading) -> None:
    if out.problem is not None:
        return
    visitor = next((_VISITORS[c] for c in type(node).__mro__ if c in _VISITORS), None)
    if visitor is None:
        _refuse(out, f"{type(node).__name__} is not an Ossie construct")
    else:
        visitor(node, out)


def _visit_all(nodes: Iterable[Any], out: FilterReading) -> None:
    for node in nodes:
        _visit(node, out)


def _operator(node: Any, out: FilterReading, allowed: frozenset[str], operands: Iterable[Any]) -> None:
    if node.op not in allowed:
        _refuse(out, f"the operator {node.op!r} is not an Ossie operator")
    _visit_all(operands, out)


def _reference_node(node: Any, out: FilterReading) -> None:
    if node.evaluated:
        _refuse(out, "AGG(…) is reserved to model-scoped metrics")
    out.references.append(node)


def _cast(node: Cast, out: FilterReading) -> None:
    if node.type_text.split("(")[0].split(" ")[0] not in OSSIE_CAST_TYPES:
        _refuse(out, f"CAST to {node.type_text} is not an Ossie type")
    _visit(node.source, out)


def _aggregate(node: AggCall, out: FilterReading) -> None:
    out.aggregate = True
    if node.kwargs:
        _refuse(out, f"{node.agg}() takes no keyword arguments in Ossie")
    _visit_all((p for p in (node.source, *node.args, node.filter) if p is not None), out)


def _refusal(why: Callable[[Any], str]) -> Callable[[Any, FilterReading], None]:
    return lambda node, out: _refuse(out, why(node))


def _comparison(node: Cmp, out: FilterReading) -> None:
    if node.op not in OSSIE_COMPARISONS:
        _refuse(out, f"the comparison {node.op!r} is not an Ossie operator")
    if node.op in ("is", "is not") and not (isinstance(node.right, Literal) and node.right.value is None):
        _refuse(out, "IS compares only with NULL in Ossie")
    for side, other in ((node.left, node.right), (node.right, node.left)):
        texts = _string_literals(other)
        if isinstance(side, (Ref, DottedRef)) and texts:
            out.comparisons.append(LiteralComparison(reference=side, texts=tuple(texts)))
    _visit(node.left, out)
    _visit(node.right, out)


def _string_literals(node: Any) -> List[str]:
    if isinstance(node, Literal) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, TupleLit):
        return [e.value for e in node.elements if isinstance(e.value, str)]
    return []


#: Each Mode B node kind's judgment; a kind missing here is no Ossie construct.
_VISITORS: Dict[type, Callable[[Any, FilterReading], None]] = {
    Ref: _reference_node,
    DottedRef: _reference_node,
    Literal: lambda _node, _out: None,
    TemporalLiteral: lambda _node, _out: None,
    StarSource: lambda _node, _out: None,
    TupleLit: lambda node, out: _visit_all(node.elements, out),
    ScalarCall: lambda node, out: _visit_all(node.args, out),
    DateTrunc: lambda node, out: _visit(node.value, out),
    Cast: _cast,
    Arith: lambda node, out: _operator(node, out, OSSIE_ARITHMETIC, (node.left, node.right)),
    UnaryOp: lambda node, out: _operator(node, out, OSSIE_UNARY, (node.operand,)),
    Cmp: _comparison,
    BoolOp: lambda node, out: _operator(node, out, OSSIE_BOOLEAN, node.operands),
    AggCall: _aggregate,
    TransformCall: _refusal(lambda node: f"the transform {node.op}() is SLayer syntax, not an Ossie expression"),
    Placeholder: _refusal(lambda _node: "an unbound variable is not an Ossie expression"),
    BoundVariable: _refusal(lambda _node: "an unbound variable is not an Ossie expression"),
    ArrayLit: _refusal(lambda _node: "a list literal is SLayer syntax, not an Ossie expression"),
    NamedArg: _refusal(lambda node: f"the keyword argument {node.name}= is SLayer syntax, not an Ossie expression"),
}
