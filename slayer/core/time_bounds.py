"""Frame-bound predicate analysis for trailing-window / shifted CTEs.

Some CTEs must read rows from OUTSIDE the query's visible time frame:

* a duration-windowed measure's ``_src`` subquery (``sum(revenue, window='90d')``)
  — the trailing window reaches back before the earliest visible bucket, or that
  bucket under-counts;
* a ``time_shift`` shifted CTE — the shifted value for the earliest visible
  bucket comes from a bucket outside the frame.

This module identifies that *semantic class*, so every spelling of one intent
(``date_range``, a time-point comparison, ``gran(col)`` against a point) agrees:

    A ROW-phase filter conjunct that is a relational bound, with a temporal
    literal, on the raw column of one of the query's time dimensions is a FRAME
    bound, not a population filter. Frame bounds constrain the visible buckets
    only. Everything else is a population filter and is applied unchanged.

Dependency-free by design (imports only :mod:`slayer.core.keys`) so both the
engine planner and the SQL generator can call it without either importing the
other — the same placement rationale as :mod:`slayer.core.window_duration`.
"""

from __future__ import annotations

from datetime import date
from typing import AbstractSet, Optional

from slayer.core.keys import ArithmeticKey, LiteralKey, ValueKey

__all__ = [
    "RELATIONAL_OPS",
    "is_temporal_literal",
    "is_frame_bound",
    "strip_frame_bounds",
]

#: Operators that can express a frame bound. ``==`` / ``!=`` / ``in`` / ``is``
#: are deliberately absent: an equality on a raw timestamp means "this instant"
#: or "this set", never a range, and stripping one would sum the whole window
#: where a single instant was asked for.
RELATIONAL_OPS = frozenset({"<", "<=", ">", ">="})

_AND = "and"


def is_temporal_literal(key: object) -> bool:
    """Is ``key`` a literal usable as a frame-bound endpoint?

    A **bare** ``LiteralKey`` holding a ``str``, ``date`` or ``datetime`` (a
    resolved time point) — not an expression tree that merely contains one, and
    not a dynamic expression such as a zero-argument scalar call.
    ``created_at < None`` (matches nothing) and ``created_at >= 5`` (type-invalid)
    are therefore never frame bounds; ``bool`` is excluded for free.
    """
    return isinstance(key, LiteralKey) and isinstance(key.value, (str, date))


def is_frame_bound(*, key: object, time_columns: AbstractSet[ValueKey]) -> bool:
    """Is ``key`` a single frame bound on one of ``time_columns``?

    ``time_columns`` holds the RAW column keys (``ColumnKey`` / ``ColumnSqlKey``)
    of the query's non-hidden time dimensions. Matching is by ValueKey identity,
    so a derived (``Column.sql``) temporal column is covered without any special
    casing, and a same-named column on a joined model cannot collide.

    Both operand orders count — ``'2024-06-01' <= created_at`` says the same
    thing as ``created_at >= '2024-06-01'``.
    """
    if not isinstance(key, ArithmeticKey) or key.op not in RELATIONAL_OPS:
        return False
    if len(key.operands) != 2:
        return False
    lhs, rhs = key.operands
    if lhs in time_columns:
        return is_temporal_literal(rhs)
    if rhs in time_columns:
        return is_temporal_literal(lhs)
    return False


def strip_frame_bounds(
    *, key: ValueKey, time_columns: AbstractSet[ValueKey],
) -> Optional[ValueKey]:
    """Return ``key`` with its top-level frame bounds removed.

    * ``None`` — the whole predicate was a frame bound (or a conjunction of
      them); the caller omits the filter from the CTE entirely.
    * the **same object** — nothing was stripped; the caller can skip building a
      rewrite entry, and the CTE renders the host's predicate verbatim.
    * a new key — the residual population predicate.

    A top-level ``and`` is split: each operand is tested independently, frame
    bounds are dropped, survivors are rebuilt in their original order (a lone
    survivor replaces the conjunction). Nested ``and`` is recursed into.

    ``or`` and ``not`` are never descended into — no sound split exists under a
    disjunction or a negation, and keeping the predicate whole preserves the
    unstripped result, which is the safe direction to err in.
    """
    if not time_columns:
        return key
    if is_frame_bound(key=key, time_columns=time_columns):
        return None
    if not (isinstance(key, ArithmeticKey) and key.op == _AND):
        return key

    kept: list[ValueKey] = []
    changed = False
    for operand in key.operands:
        residual = strip_frame_bounds(key=operand, time_columns=time_columns)
        if residual is None:
            changed = True
            continue
        if residual is not operand:
            changed = True
        kept.append(residual)

    if not changed:
        return key
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return ArithmeticKey(op=_AND, operands=tuple(kept))
