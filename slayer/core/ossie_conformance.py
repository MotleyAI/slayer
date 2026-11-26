"""Ossie Layer 1 conformance of a Mode B filter: which constructs ``OSSIE_SQL_2026`` defines with SLayer's meaning.

The judgment reads the one Mode B parse (``core.syntax``): its node kinds and operators, the calls as written
(each scalar's Ossie argument counts, each aggregate's spelling, the built-in it names and its shape) and the
non-Ossie spellings the parser reports rewriting.
"""

from __future__ import annotations

import datetime
import re
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import (
    AMBIGUOUS_WEEKDAY_NAMES, BUILTIN_AGGREGATIONS, DATE_PART_ALIASES, Conformance, normalize_aggregation_name,
)
from slayer.core.errors import UnknownConformanceError
from slayer.core.keys import SCALAR_SPECS, SCALAR_SPELLINGS
from slayer.core.syntax import (
    WINDOW_FUNCTION_ARITIES, AggCall, ArrayLit, Arith, BoolOp, BoundVariable, Cast, Cmp, DateTrunc, DottedRef, Literal,
    NamedArg, Placeholder, Ref, ScalarCall, StarSource, TemporalLiteral, TransformCall, TupleLit, UnaryOp, WindowCall,
    WindowFrame, call_spans, legacy_spans, nonossie_spellings, parse_bound_expr, parse_template, variable_names,
    walk_parsed,
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
#: ``DATE_TRUNC`` units Ossie defines.
OSSIE_DATE_TRUNC_UNITS = frozenset({"year", "quarter", "month", "week", "day", "hour", "minute", "second"})
#: Aggregate functions Ossie defines, as written.
OSSIE_AGGREGATE_SPELLINGS = frozenset({"sum", "count", "avg", "min", "max", "stddev", "stddev_pop", "stddev_samp",
                                       "variance", "var_pop", "var_samp", "median", "approx_count_distinct",
                                       "approx_percentile", "percentile_cont", "percentile_disc"})
#: The names the parser gives Ossie's ``COUNT(DISTINCT …)`` and ``PERCENTILE_CONT(…) WITHIN GROUP (…)``; SLayer's own
#: calls of these names are refused by their written callee.
REWRITTEN_AGGREGATES = frozenset({"count_distinct", "percentile"})
#: Ossie aggregates SLayer reads in no plain-call form, and why.
OSSIE_UNREAD_CALLS = {
    "percentile_cont": "PERCENTILE_CONT is read only as PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)",
    "percentile_disc": "PERCENTILE_DISC is an Ossie aggregate SLayer does not support yet",
}
#: The named arguments the parser gives each rewritten aggregate; any other argument was written.
REWRITTEN_ARGUMENTS = {"count_distinct": (), "percentile": ("p",)}
#: Arguments an Ossie aggregate takes after its value (one value otherwise).
OSSIE_AGGREGATE_EXTRA_ARGS = {"approx_percentile": 1}
#: ANSI order of a frame's bound kinds.
FRAME_BOUND_ORDER = {"unbounded preceding": 0, "preceding": 1, "current row": 2, "following": 3,
                     "unbounded following": 4}
#: ``CAST`` target types Ossie defines (the first word of the written type).
OSSIE_CAST_TYPES = frozenset({"VARCHAR", "STRING", "INTEGER", "INT", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE",
                              "BOOLEAN", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ"})
#: Bare Ossie keywords SLayer cannot evaluate yet (the time-of-day type comes with DEV-2282).
UNSUPPORTED_OSSIE_KEYWORDS = frozenset({"current_time"})
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


def read_filter(text: str, *, variables: Mapping[str, Any] | None = None) -> FilterReading:
    """Judge filter ``text`` against Ossie Layer 1, its variables bound but a whole ``IN`` list's, which binds from
    ``variables`` as native binding does."""
    out = FilterReading()
    unbound = [n for n in variable_names(text) if n not in (variables or {})]
    if unbound:
        out.problem = f"the variables {unbound} are not bound; pass them as the run's variables"
        return out
    try:
        tree = parse_bound_expr(text, variables) if variables else parse_template(text)
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
        name = call.name.lower()
        if name in REWRITTEN_AGGREGATES:
            return f"{call.callee}() is a SLayer function Ossie does not define"
        spec = SCALAR_SPECS.get(name)
        if spec is not None and not spec.ossie_arg_counts:
            return f"{call.callee}() is a SLayer function Ossie does not define"
        if spec is not None and not spec.ossie_admits(call.arguments):
            return f"{call.callee}() with {call.arguments} arguments is not an Ossie call"
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
    if isinstance(node, Ref) and not node.quoted and node.name.lower() in UNSUPPORTED_OSSIE_KEYWORDS:
        _refuse(out, f"{node.name} is not supported yet")
    out.references.append(node)


def _date_trunc(node: DateTrunc, out: FilterReading) -> None:
    if node.unit.lower() not in OSSIE_DATE_TRUNC_UNITS:
        _refuse(out, f"DATE_TRUNC to {node.unit!r} is not an Ossie unit")
    _visit(node.value, out)


def _cast(node: Cast, out: FilterReading) -> None:
    if node.type_text.split("(")[0].split(" ")[0] not in OSSIE_CAST_TYPES:
        _refuse(out, f"CAST to {node.type_text} is not an Ossie type")
    _visit(node.source, out)


def _aggregate(node: AggCall, out: FilterReading) -> None:
    out.aggregate = True
    _aggregate_call(node, out)


def _aggregate_call(node: AggCall, out: FilterReading) -> None:
    """An aggregate judged as Ossie writes it, windowed or not: an Ossie spelling of a SLayer built-in."""
    name = node.agg.lower()
    if name in OSSIE_UNREAD_CALLS:
        _refuse(out, OSSIE_UNREAD_CALLS[name])
    if name not in OSSIE_AGGREGATE_SPELLINGS | REWRITTEN_AGGREGATES:
        _refuse(out, f"{node.agg}() is not an Ossie function")
    elif normalize_aggregation_name(name) not in BUILTIN_AGGREGATIONS:
        _refuse(out, f"{node.agg}() is not supported yet")
    if node.kwargs:
        _refuse(out, f"{node.agg}() takes no keyword arguments in Ossie")
    # A rewritten Ossie aggregate's arguments are the parser's (``p=`` of ``PERCENTILE_CONT(p)``), not written.
    rewritten = name in REWRITTEN_AGGREGATES
    _aggregate_arguments(node, name=name, rewritten=rewritten, out=out)
    args = [a.value if rewritten and isinstance(a, NamedArg) else a for a in node.args]
    _visit_all((p for p in (node.source, *args, node.filter) if p is not None), out)


def _aggregate_arguments(node: AggCall, *, name: str, rewritten: bool, out: FilterReading) -> None:
    if rewritten:
        if tuple(a.name if isinstance(a, NamedArg) else None for a in node.args) != REWRITTEN_ARGUMENTS[name]:
            _refuse(out, f"{node.agg}() takes no further arguments in Ossie")
        return
    if isinstance(node.source, StarSource) and name != "count":
        _refuse(out, f"{node.agg}(*) is not an Ossie aggregate; only COUNT(*) is")
    extra = OSSIE_AGGREGATE_EXTRA_ARGS.get(name, 0)
    if len(node.args) != extra:
        _refuse(out, f"{node.agg}() takes {extra + 1} argument(s) in Ossie")


def _window(node: WindowCall, out: FilterReading) -> None:
    out.window = True
    function = node.function
    inside = (function, node.partition, tuple(o.value for o in node.order))
    if any(isinstance(n, WindowCall) for n in walk_parsed(inside)):
        _refuse(out, "a window function cannot sit inside a window")
    if isinstance(function, AggCall):
        _aggregate_call(function, out)
    elif isinstance(function, ScalarCall) and function.name in WINDOW_FUNCTION_ARITIES:
        low, high = WINDOW_FUNCTION_ARITIES[function.name]
        if not low <= len(function.args) <= high:
            counts = str(low) if low == high else f"{low} to {high}"
            _refuse(out, f"{function.written or function.name}() takes {counts} argument(s) in Ossie")
        _visit_all(function.args, out)
    else:
        _refuse(out, f"{type(function).__name__} is not an Ossie window function")
    if node.frame is not None:
        _frame(node.frame, out)
    _visit_all((*node.partition, *(o.value for o in node.order)), out)


def _frame(frame: WindowFrame, out: FilterReading) -> None:
    start, end = frame.start, frame.end
    if end is None:
        if FRAME_BOUND_ORDER[start[0]] > FRAME_BOUND_ORDER["current row"]:
            _refuse(out, "a single frame bound cannot be a FOLLOWING one")
        return
    if (
        start[0] == "unbounded following"
        or end[0] == "unbounded preceding"
        or (FRAME_BOUND_ORDER[start[0]], _offset_rank(start)) > (FRAME_BOUND_ORDER[end[0]], _offset_rank(end))
    ):
        _refuse(out, "the frame bounds are out of order")


def _offset_rank(bound: Tuple[str, Optional[Decimal]]) -> Decimal:
    """Order within a bound kind: a farther PRECEDING comes first."""
    offset = bound[1] or Decimal(0)
    # ``copy_negate`` is exact: ``-offset`` would round, or overflow on ``1e1000000000``.
    return offset.copy_negate() if bound[0] == "preceding" else offset


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
    if isinstance(node, BoundVariable):
        return [v for v in node.values if isinstance(v, str)]
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
    DateTrunc: _date_trunc,
    Cast: _cast,
    Arith: lambda node, out: _operator(node, out, OSSIE_ARITHMETIC, (node.left, node.right)),
    UnaryOp: lambda node, out: _operator(node, out, OSSIE_UNARY, (node.operand,)),
    Cmp: _comparison,
    BoolOp: lambda node, out: _operator(node, out, OSSIE_BOOLEAN, node.operands),
    AggCall: _aggregate,
    WindowCall: _window,
    TransformCall: _refusal(lambda node: f"the transform {node.op}() is SLayer syntax, not an Ossie expression"),
    Placeholder: _refusal(lambda _node: "an unbound variable is not an Ossie expression"),
    BoundVariable: lambda _node, _out: None,
    ArrayLit: _refusal(lambda _node: "a list literal is SLayer syntax, not an Ossie expression"),
    NamedArg: _refusal(lambda node: f"the keyword argument {node.name}= is SLayer syntax, not an Ossie expression"),
}
