"""A window function call (queries/window-functions) rendered as one sqlglot ``Window``: Ossie's NULL placement on
every order item through the dialect, a frame kept only where it changes the value, ``FILTER`` as a ``CASE``
argument."""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.keys import FieldTypeFn, StarKey, WindowFrameSpec, WindowKey, value_type
from slayer.core.refs import key_display
from slayer.sql.dialects.base import SqlDialect

__all__ = ["frame_changes_value", "render_window", "window_order_items"]

#: Ranking and offset functions: a frame never changes their value.
_FRAMELESS = frozenset({
    "row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "lag", "lead",
})



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
    key: WindowKey, *, values: List[Expression], dialect: SqlDialect, labels: Optional[Sequence[str]] = None,
) -> List[Expression]:
    """``key``'s order items with their NULL placement stated through ``dialect``; ``labels`` name them in errors."""
    frame = key.frame if frame_changes_value(key) else None
    range_offset = frame is not None and frame.unit == "range" and any(
        b is not None and b[1] is not None for b in (frame.start, frame.end))
    out: List[Expression] = []
    names = list(labels) if labels is not None else [key_display(item) for item in key.order]
    for value, name, desc, nulls_first in zip(values, names, key.descending, key.nulls_first, strict=True):
        out += dialect.window_order_item(value, descending=desc, nulls_first=nulls_first,
                                         range_offset=range_offset, label=name)
    return out


def render_window(
    key: WindowKey, *, part: Callable[[Any], Expression], dialect: SqlDialect, labels: Optional[Sequence[str]] = None,
    field_type: Optional[FieldTypeFn] = None,
) -> Expression:
    """``key`` as ``function(args) OVER (PARTITION BY … ORDER BY … frame)``; each operand rendered by ``part`` and typed
    by ``field_type``, ``labels`` naming its order items in errors."""
    args = [exp.Star() if isinstance(a, StarKey) and key.function == "count" else part(a) for a in key.args]
    if key.filter is not None:
        condition = part(key.filter)
        value = exp.Literal.number(1) if isinstance(args[0], exp.Star) else args[0]
        args[0] = exp.Case(ifs=[exp.If(this=condition, true=value)])
    input_type = value_type(key.args[0], field_type=field_type) if key.args and field_type is not None else None
    call = dialect.window_call(key, args, input_type=input_type)
    order = window_order_items(key, values=[part(o) for o in key.order], dialect=dialect, labels=labels)
    window = exp.Window(
        this=call,
        partition_by=[part(p) for p in key.partition] or None,
        order=exp.Order(expressions=order) if order else None,
        spec=_spec(key.frame) if frame_changes_value(key) and key.frame is not None else None,
    )
    return dialect.finish_window(key, window)
