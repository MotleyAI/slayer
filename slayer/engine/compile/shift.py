"""The shifted evaluation of ``time_shift``: its moved frame and per-leaf rule."""

from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import UNIT_MONTHS
from slayer.core.granularity import Granularity, granularity_parts
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    LiteralKey,
    REGROUP_LEAF_PREFIX,
    TimeTruncKey,
    ValueKey,
    constituent_grain,
    is_kernel_requiring,
    is_transform,
    split_top_level_and,
)
from slayer.core.time_bounds import is_frame_bound
from slayer.core.time_points import add_units, date_bound, floor_to, parse_temporal_value
from slayer.engine.compile.regroup import substitute_in_bound_filter
from slayer.ir.bound import BoundFilter, bound_filter_from_key

__all__ = ["MovedFrame", "carried_placeholders", "is_placeholder", "moved_frame"]

_LOWER_OPS = frozenset({">", ">="})
_MIRROR = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}


def is_placeholder(key: ValueKey) -> bool:
    return isinstance(key, FieldKey) and key.leaf.startswith(REGROUP_LEAF_PREFIX)


class MovedFrame(BaseModel):
    """A shifted evaluation's filters: the frame bounds (moved axis bounds, then the kept date ranges) first."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    filters: List[BoundFilter]
    n_frame: int


def _instant(value: object) -> Tuple[datetime, bool]:
    """``(instant, is_date)`` of a frame-bound literal."""
    if isinstance(value, str):
        parsed = parse_temporal_value(value)
        if parsed is None:
            raise ValueError(f"time_shift cannot move the frame bound {value!r}: not a time point")
        value = parsed
    if isinstance(value, datetime):
        return value, False
    assert isinstance(value, date)
    return datetime(value.year, value.month, value.day), True


def _side(conjunct: ArithmeticKey, field: ValueKey, *, date_axis: bool) -> Optional[Tuple[bool, datetime, bool]]:
    """``(is_lower, instant, is_date)``: an inclusive lower or exclusive upper instant, a DATE axis's in whole days;
    None past the last representable instant."""
    lhs, rhs = conjunct.operands
    op = conjunct.op if lhs == field else _MIRROR[conjunct.op]
    literal = rhs if lhs == field else lhs
    assert isinstance(literal, LiteralKey)
    value, is_date = _instant(literal.value)
    if date_axis:
        canonical = date_bound(op, value)
        if canonical is None:
            return None
        op, day = canonical
        value, is_date = datetime(day.year, day.month, day.day), True
    elif op in (">", "<="):
        try:
            value += timedelta(microseconds=1)
        except OverflowError:
            return None
    return op in _LOWER_OPS, value, is_date


def _offset(value: datetime, unit: Granularity, n: int) -> datetime:
    """``value`` plus ``n`` ``unit`` steps, a month step clamped to the target month's last day."""
    base, multiple, _ = granularity_parts(unit)
    if base not in UNIT_MONTHS:
        return add_units(value, base, n * multiple)
    start = add_units(value.replace(day=1), base, n * multiple)
    return start.replace(day=min(value.day, monthrange(start.year, start.month)[1]))


def _moved_start(value: datetime, *, bucket: Granularity, unit: Granularity, periods: int) -> Optional[datetime]:
    """The moved lower bound; None past the representable range, where no source bucket exists."""
    try:
        return floor_to(_offset(floor_to(value, bucket), unit, periods), bucket)
    except (OverflowError, ValueError):
        return None


def _moved_end(value: datetime, *, bucket: Granularity, unit: Granularity, periods: int) -> Optional[datetime]:
    """The moved exclusive upper bound; None past the representable range, where no source bucket exists."""
    try:
        last = floor_to(value - timedelta(microseconds=1), bucket)
        return add_units(floor_to(_offset(last, unit, periods), bucket), bucket, 1)
    except (OverflowError, ValueError):
        return None


def _literal(value: datetime, *, is_date: bool) -> LiteralKey:
    midnight = value == datetime(value.year, value.month, value.day)
    return LiteralKey(value=value.date() if is_date and midnight else value)


def _without_axis_bounds(
    bf: BoundFilter, *, field: ValueKey, date_axis: bool, lowers: List[Tuple[datetime, bool]],
    uppers: List[Tuple[datetime, bool]],
) -> Optional[BoundFilter]:
    """``bf`` minus its frame bounds on ``field`` (collected into ``lowers`` / ``uppers``); None when nothing remains."""
    conjuncts = split_top_level_and(bf.value_key)
    residual: List[ValueKey] = []
    for conjunct in conjuncts:
        if is_frame_bound(key=conjunct, time_fields={field}):
            assert isinstance(conjunct, ArithmeticKey)
            side = _side(conjunct, field, date_axis=date_axis)
            if side is not None:
                is_lower, value, is_date = side
                (lowers if is_lower else uppers).append((value, is_date))
        else:
            residual.append(conjunct)
    if not residual:
        return None
    if len(residual) == len(conjuncts):
        return bf
    rest = residual[0] if len(residual) == 1 else ArithmeticKey(op="and", operands=tuple(residual))
    return substitute_in_bound_filter(bf, {bf.value_key: rest})


def moved_frame(
    *, filters: Sequence[BoundFilter], n_date_range: int, axis: TimeTruncKey, date_axis: bool, periods: int,
    unit: Granularity,
) -> MovedFrame:
    """The frame bounds on ``axis``'s column (a DATE column iff ``date_axis``) moved to whole source buckets of the
    visible buckets; every other filter is kept, date-range filters still leading."""
    field, bucket = axis.source, axis.granularity
    lowers: List[Tuple[datetime, bool]] = []
    uppers: List[Tuple[datetime, bool]] = []
    kept: List[BoundFilter] = []
    n_kept_range = 0
    for i, bf in enumerate(filters):
        rest = _without_axis_bounds(bf, field=field, date_axis=date_axis, lowers=lowers, uppers=uppers)
        if rest is not None:
            kept.append(rest)
            n_kept_range += i < n_date_range
    moved: List[BoundFilter] = []
    if lowers:
        value, is_date = max(lowers)
        start = _moved_start(value, bucket=bucket, unit=unit, periods=periods)
        if start is not None:
            moved.append(bound_filter_from_key(
                ArithmeticKey(op=">=", operands=(field, _literal(start, is_date=is_date)))))
    if uppers:
        value, is_date = min(uppers)
        end = _moved_end(value, bucket=bucket, unit=unit, periods=periods)
        if end is not None:
            moved.append(bound_filter_from_key(
                ArithmeticKey(op="<", operands=(field, _literal(end, is_date=is_date)))))
    return MovedFrame(filters=[*moved, *kept], n_frame=len(moved) + n_kept_range)


def _re_evaluated(
    original: ValueKey, *, axis: ValueKey, dim_keys: List[ValueKey],
    td_keys: List[ValueKey], active_bucket: Optional[ValueKey],
) -> bool:
    if is_kernel_requiring(original):
        return True
    grain = constituent_grain(
        c=original, projected_dim_keys=dim_keys, projected_td_keys=td_keys,
        active_bucket=active_bucket,
    )
    return axis in grain


def carried_placeholders(
    inner: ValueKey, *, axis: ValueKey, to_original: Dict[ValueKey, ValueKey],
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> List[ValueKey]:
    """Consumer-level placeholders of ``inner`` read at their in-frame value: an
    axis-free leaf is constant along the axis; one whose grain holds the axis, or a
    ranked / windowed one, is re-evaluated instead."""
    carried: List[ValueKey] = []

    def visit(node: ValueKey) -> None:
        if is_placeholder(node):
            original = to_original.get(node)
            if original is not None and node not in carried and not _re_evaluated(
                original, axis=axis, dim_keys=dim_keys, td_keys=td_keys,
                active_bucket=active_bucket,
            ):
                carried.append(node)
            return
        if isinstance(node, AggregateKey) or is_transform(node):
            return
        for child in node.children():
            visit(child)

    visit(inner)
    return carried
