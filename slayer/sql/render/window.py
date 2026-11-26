"""A window function call (queries/window-functions) rendered as one sqlglot ``Window``: Ossie's NULL placement on
every order item through the dialect, a frame kept only where it changes the value, ``FILTER`` as a ``CASE``
argument."""

from __future__ import annotations

from typing import Any, Callable, List, Optional

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.keys import StarKey, WindowFrameSpec, WindowKey
from slayer.core.refs import key_display
from slayer.sql.dialects.base import SqlDialect

__all__ = ["WINDOW_FUNCTIONS", "frame_changes_value", "render_window", "window_order_items"]

#: Ranking and offset functions: a frame never changes their value.
_FRAMELESS = frozenset({
    "row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "lag", "lead",
})

_CALLS: dict[str, Callable[[List[Expression]], Expression]] = {
    "row_number": lambda a: exp.RowNumber(),
    "rank": lambda a: exp.Rank(),
    "dense_rank": lambda a: exp.DenseRank(),
    "percent_rank": lambda a: exp.PercentRank(),
    "cume_dist": lambda a: exp.CumeDist(),
    "ntile": lambda a: exp.Ntile(this=a[0]),
    "lag": lambda a: exp.Lag(this=a[0], offset=a[1] if len(a) > 1 else None, default=a[2] if len(a) > 2 else None),
    "lead": lambda a: exp.Lead(this=a[0], offset=a[1] if len(a) > 1 else None, default=a[2] if len(a) > 2 else None),
    "first_value": lambda a: exp.FirstValue(this=a[0]),
    "last_value": lambda a: exp.LastValue(this=a[0]),
    "nth_value": lambda a: exp.NthValue(this=a[0], offset=a[1]),
    "sum": lambda a: exp.Sum(this=a[0]),
    "count": lambda a: exp.Count(this=a[0]),
    "count_distinct": lambda a: exp.Count(this=exp.Distinct(expressions=[a[0]])),
    "avg": lambda a: exp.Avg(this=a[0]),
    "min": lambda a: exp.Min(this=a[0]),
    "max": lambda a: exp.Max(this=a[0]),
    "stddev": lambda a: exp.StddevSamp(this=a[0]),
    "stddev_samp": lambda a: exp.StddevSamp(this=a[0]),
    "stddev_pop": lambda a: exp.StddevPop(this=a[0]),
    "variance": lambda a: exp.Variance(this=a[0]),
    "var_samp": lambda a: exp.Variance(this=a[0]),
    "var_pop": lambda a: exp.VariancePop(this=a[0]),
    "median": lambda a: exp.Median(this=a[0]),
    "percentile_cont": lambda a: exp.WithinGroup(
        this=exp.PercentileCont(this=a[1]), expression=exp.Order(expressions=[exp.Ordered(this=a[0])])),
}
#: Every function a window may apply.
WINDOW_FUNCTIONS = frozenset(_CALLS)


def frame_changes_value(key: WindowKey) -> bool:
    """Whether ``key``'s frame is part of its meaning (a ranking or offset function ignores it)."""
    return key.frame is not None and key.function not in _FRAMELESS


def _bound(bound: tuple) -> tuple[Any, Optional[str]]:
    kind, offset = bound
    if kind == "current row":
        return "CURRENT ROW", None
    if kind.startswith("unbounded"):
        return "UNBOUNDED", kind.split()[1].upper()
    return exp.Literal.number(str(offset)), kind.upper()


def _spec(frame: WindowFrameSpec) -> exp.WindowSpec:
    start, start_side = _bound(frame.start)
    end, end_side = _bound(frame.end) if frame.end is not None else ("CURRENT ROW", None)
    return exp.WindowSpec(kind=frame.unit.upper(), start=start, start_side=start_side, end=end, end_side=end_side)


def window_order_items(
    key: WindowKey, *, values: List[Expression], dialect: SqlDialect,
) -> List[Expression]:
    """``key``'s order items with their NULL placement stated through ``dialect``."""
    range_offset = key.frame is not None and key.frame.unit == "range" and any(
        b is not None and b[1] is not None for b in (key.frame.start, key.frame.end))
    out: List[Expression] = []
    for value, item, desc, nulls_first in zip(values, key.order, key.descending, key.nulls_first, strict=True):
        out += dialect.window_order_item(value, descending=desc, nulls_first=nulls_first,
                                         range_offset=range_offset, label=key_display(item))
    return out


def render_window(key: WindowKey, *, part: Callable[[Any], Expression], dialect: SqlDialect) -> Expression:
    """``key`` as ``function(args) OVER (PARTITION BY … ORDER BY … frame)``; each operand rendered by ``part``."""
    args = [exp.Star() if isinstance(a, StarKey) and key.function == "count" else part(a) for a in key.args]
    if key.filter is not None:
        condition = part(key.filter)
        value = exp.Literal.number(1) if isinstance(args[0], exp.Star) else args[0]
        args[0] = exp.Case(ifs=[exp.If(this=condition, true=value)])
    call = dialect.window_call(key.function, args=args, build=_CALLS[key.function])
    order = window_order_items(key, values=[part(o) for o in key.order], dialect=dialect)
    return exp.Window(
        this=call,
        partition_by=[part(p) for p in key.partition] or None,
        order=exp.Order(expressions=order) if order else None,
        spec=_spec(key.frame) if frame_changes_value(key) and key.frame is not None else None,
    )
