"""Ossie Layer 1 conformance of a Mode B filter: which constructs ``OSSIE_SQL_2026`` defines with SLayer's meaning.

The judgment reads the one Mode B parse (``core.syntax``): its node kinds and operators, the calls as written
(each scalar's ``ossie`` flag) and the non-Ossie spellings the parser reports rewriting.
"""

from __future__ import annotations

import datetime
import re
from typing import Any, List, Optional, Tuple

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
_ISO = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?")

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
    if not _ISO.fullmatch(text):
        return False
    try:
        datetime.datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


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


def _visit(node: Any, out: FilterReading) -> None:  # noqa: C901 — one exhaustive dispatch, fail-closed tail
    if out.problem is not None:
        return
    if isinstance(node, (Ref, DottedRef)):
        if node.evaluated:
            _refuse(out, "AGG(…) is reserved to model-scoped metrics")
        out.references.append(node)
    elif isinstance(node, (Literal, TemporalLiteral, StarSource)):
        return
    elif isinstance(node, TupleLit):
        for element in node.elements:
            _visit(element, out)
    elif isinstance(node, ScalarCall):
        for arg in node.args:
            _visit(arg, out)
    elif isinstance(node, DateTrunc):
        _visit(node.value, out)
    elif isinstance(node, Cast):
        if node.type_text.split("(")[0].split(" ")[0] not in OSSIE_CAST_TYPES:
            _refuse(out, f"CAST to {node.type_text} is not an Ossie type")
        _visit(node.source, out)
    elif isinstance(node, Arith):
        if node.op not in OSSIE_ARITHMETIC:
            _refuse(out, f"the operator {node.op!r} is not an Ossie operator")
        _visit(node.left, out)
        _visit(node.right, out)
    elif isinstance(node, UnaryOp):
        if node.op not in OSSIE_UNARY:
            _refuse(out, f"the operator {node.op!r} is not an Ossie operator")
        _visit(node.operand, out)
    elif isinstance(node, Cmp):
        _comparison(node, out)
    elif isinstance(node, BoolOp):
        if node.op not in OSSIE_BOOLEAN:
            _refuse(out, f"the operator {node.op!r} is not an Ossie operator")
        for operand in node.operands:
            _visit(operand, out)
    elif isinstance(node, AggCall):
        out.aggregate = True
        if node.kwargs:
            _refuse(out, f"{node.agg}() takes no keyword arguments in Ossie")
        for part in (node.source, *node.args, node.filter):
            if part is not None:
                _visit(part, out)
    elif isinstance(node, TransformCall):
        _refuse(out, f"the transform {node.op}() is SLayer syntax, not an Ossie expression")
    elif isinstance(node, (Placeholder, BoundVariable)):
        _refuse(out, "an unbound variable is not an Ossie expression")
    elif isinstance(node, ArrayLit):
        _refuse(out, "a list literal is SLayer syntax, not an Ossie expression")
    elif isinstance(node, NamedArg):
        _refuse(out, f"the keyword argument {node.name}= is SLayer syntax, not an Ossie expression")
    else:
        _refuse(out, f"{type(node).__name__} is not an Ossie construct")


def _comparison(node: Cmp, out: FilterReading) -> None:
    if node.op not in OSSIE_COMPARISONS:
        _refuse(out, f"the comparison {node.op!r} is not an Ossie operator")
    if node.op in ("is", "is not") and not (isinstance(node.right, Literal) and node.right.value is None):
        _refuse(out, "IS compares only with NULL in Ossie")
    for side, other in ((node.left, node.right), (node.right, node.left)):
        texts = _string_literals(other)
        if isinstance(side, (Ref, DottedRef)) and texts:
            out.comparisons.append(LiteralComparison(reference=side, texts=texts))
    _visit(node.left, out)
    _visit(node.right, out)


def _string_literals(node: Any) -> Tuple[str, ...]:
    if isinstance(node, Literal) and isinstance(node.value, str):
        return (node.value,)
    if isinstance(node, TupleLit):
        return tuple(e.value for e in node.elements if isinstance(e.value, str))
    return ()
