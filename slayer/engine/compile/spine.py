"""The host of a ``time_spine × P`` population: its spine column, bounds and bucket series."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from math import ceil
from typing import List, Optional, Tuple

from slayer.core.enums import UNIT_MONTHS, UNIT_SECONDS, TimeGranularity
from slayer.core.granularity import Granularity, granularity_parts, nests_into, resolve_granularity
from slayer.core.keys import AggregateKey, ArithmeticKey, ColumnKey, LiteralKey, TimeTruncKey, ValueKey, source_anchor_path, walk_value_keys
from slayer.core.models import SlayerModel
from slayer.core.refs import key_display
from slayer.core.time_bounds import is_frame_bound, strip_frame_bounds
from slayer.core.time_points import add_units, floor_to
from slayer.core.time_spine import TIME_SPINE_COLUMN, TIME_SPINE_MODEL, is_spine
from slayer.engine.elaborate_env import (
    check_time_dimension_column,
    check_spine_aggregation,
    check_spine_filter,
    check_spine_lower_bound,
    check_spine_plain_use,
    check_spine_raw_rows,
)
from slayer.ir.bound import BoundFilter, bound_filter_from_key
from slayer.ir.planned import SpineFactor
from slayer.ir.prebound import PreboundQuery, walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle

_MIRROR = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}
_SERIES_FALLBACKS = (TimeGranularity.DAY, TimeGranularity.HOUR, TimeGranularity.MINUTE, TimeGranularity.SECOND)


def spine_column(model: Optional[SlayerModel]) -> Optional[ColumnKey]:
    """The spine column as the host reads it: its own column, or across the product hop."""
    if model is None:
        return None
    if is_spine(model):
        return ColumnKey(path=(), leaf=TIME_SPINE_COLUMN)
    if model.population_spine:
        return ColumnKey(path=(TIME_SPINE_MODEL,), leaf=TIME_SPINE_COLUMN)
    return None


def _mentions(key: ValueKey, spine: ColumnKey) -> bool:
    return any(k == spine for k in walk_value_keys(key))


def _bare(key: ValueKey, spine: ColumnKey) -> bool:
    """``spine`` occurs outside a bucket of it."""
    if isinstance(key, TimeTruncKey) and key.column == spine:
        return False
    return key == spine or any(_bare(c, spine) for c in key.children())


def _spine_aggregate(
    key: ValueKey, *, spine: ColumnKey, host: SlayerModel, bundle: ResolvedSourceBundle,
) -> Optional[AggregateKey]:
    for k in walk_value_keys(key):
        if not isinstance(k, AggregateKey):
            continue
        home = walk_key_path(model=host, path=source_anchor_path(k.source), bundle=bundle)
        if _mentions(k.source, spine) or (home is not None and is_spine(home)):
            return k
    return None


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    assert isinstance(value, date)
    return datetime(value.year, value.month, value.day)


def _bound(conjunct: ArithmeticKey, spine: ColumnKey) -> Tuple[str, datetime]:
    """``("lower" | "upper", instant)`` of one frame bound; an upper instant is exclusive."""
    column, literal = conjunct.operands
    op = conjunct.op
    if column != spine:
        column, literal, op = literal, column, _MIRROR[op]
    assert isinstance(literal, LiteralKey)
    value = _as_datetime(literal.value)
    if op in (">=", ">"):
        return "lower", value
    return "upper", value + timedelta(microseconds=1) if op == "<=" else value


def _conjuncts(key: ValueKey) -> List[ValueKey]:
    if isinstance(key, ArithmeticKey) and key.op == "and":
        return [c for operand in key.operands for c in _conjuncts(operand)]
    return [key]


def series_granularity(granularities: List[Granularity]) -> Granularity:
    """The finest spine granularity nesting into all the others, else the coarsest built-in that does."""
    for g in granularities:
        if all(nests_into(g, other) for other in granularities):
            return g
    return next(g for g in _SERIES_FALLBACKS if all(nests_into(g, other) for other in granularities))


def _series_size(*, granularity: Granularity, lower: datetime, upper: datetime) -> int:
    """At least the number of ``granularity`` buckets from the bucket of ``lower`` up to ``upper``."""
    start = floor_to(lower, granularity)
    if upper <= start:
        return 1
    base, multiple, _ = granularity_parts(granularity)
    if base in UNIT_MONTHS:
        months = (upper.year * 12 + upper.month) - (start.year * 12 + start.month) + 1
        return max(1, ceil(months / (UNIT_MONTHS[base] * multiple)))
    return max(1, ceil((upper - start).total_seconds() / (UNIT_SECONDS[base] * multiple)))


def plan_spine(
    *, prebound: PreboundQuery, host: SlayerModel, spine: ColumnKey, bundle: ResolvedSourceBundle,
) -> SpineFactor:
    """The host's bucket series, after checking every position the spine column appears in."""
    check_spine_raw_rows(raw_rows=not prebound.distinct_dimension_values)
    n_dims, n_tds = prebound.n_dims, prebound.n_time_dimensions
    dims = prebound.declared_measures[:n_dims]
    tds = prebound.declared_measures[n_dims:n_dims + n_tds]
    measures = prebound.declared_measures[n_dims + n_tds:]
    for dm in dims:
        key = dm.bound.value_key
        check_spine_plain_use(offender=dm.declared_name if _mentions(key, spine) else None, position="dimension")
    for dm in measures:
        key = dm.bound.value_key
        agg = _spine_aggregate(key, spine=spine, host=host, bundle=bundle)
        check_spine_aggregation(offender=key_display(agg) if agg is not None else None)
        check_spine_plain_use(offender=dm.declared_name if _bare(key, spine) else None, position="measure")
    spine_tds = [
        dm.bound.value_key for dm in tds
        if isinstance(dm.bound.value_key, TimeTruncKey) and dm.bound.value_key.column == spine
    ]
    projected = set(spine_tds)
    for spec in prebound.order_specs:
        key = spec.bound.value_key
        check_spine_plain_use(
            offender=key_display(key) if key not in projected and _mentions(key, spine) else None,
            position="order key",
        )
    lowers: List[datetime] = []
    uppers: List[datetime] = []
    texts = list(prebound.bound_filter_texts) + [None] * len(prebound.bound_filters)
    for bf, text in zip(prebound.bound_filters, texts):
        for conjunct in _conjuncts(bf.value_key):
            if is_frame_bound(key=conjunct, time_columns={spine}):
                assert isinstance(conjunct, ArithmeticKey)
                kind, value = _bound(conjunct, spine)
                (lowers if kind == "lower" else uppers).append(value)
            elif _mentions(conjunct, spine):
                check_spine_filter(offender=text or key_display(conjunct))
            agg = _spine_aggregate(conjunct, spine=spine, host=host, bundle=bundle)
            check_spine_aggregation(offender=key_display(agg) if agg is not None else None)
    check_spine_lower_bound(has_lower=bool(lowers))
    granularity = series_granularity([k.granularity for k in spine_tds] or [TimeGranularity.DAY])
    lower = max(lowers)
    upper = min(uppers) if uppers else add_units(floor_to(bundle.now, granularity), granularity, 1)
    return SpineFactor(
        granularity=granularity, lower=lower, upper=upper,
        size=_series_size(granularity=granularity, lower=lower, upper=upper),
    )


def host_mask(key: ValueKey, *, spine: Optional[ColumnKey]) -> Optional[ValueKey]:
    """A host filter without its spine bounds (they decide bucket existence, not host rows)."""
    return key if spine is None else strip_frame_bounds(key=key, time_columns={spine})


def check_axis_rebucket(
    *, host_key: ValueKey, rerooted: ValueKey, host: SlayerModel, root: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> None:
    """A spine bucket attributed through a fact's axis obeys that axis column's recorded granularity."""
    spine = spine_column(host)
    if not (isinstance(host_key, TimeTruncKey) and host_key.column == spine and isinstance(rerooted, TimeTruncKey)):
        return
    column = rerooted.column
    owner = walk_key_path(model=root, path=tuple(column.path), bundle=bundle)
    leaf = column.leaf if isinstance(column, ColumnKey) else column.column_name
    col = owner.get_column(leaf) if owner is not None else None
    if col is None or col.granularity is None:
        return
    check_time_dimension_column(
        name=".".join((*column.path, leaf)), column_type=col.type,
        upstream_granularity=resolve_granularity(col.granularity, defined=bundle.granularities),
        requested_granularity=rerooted.granularity,
    )


def shifted_spine_bounds(
    *, prebound: PreboundQuery, spine: Optional[ColumnKey], periods: int, unit: Granularity,
) -> List[BoundFilter]:
    """The host's spine bounds moved by ``periods`` ``unit`` steps: the range a shifted
    producer reads its buckets from."""
    if spine is None:
        return []
    out: List[BoundFilter] = []
    for bf in prebound.bound_filters:
        for conjunct in _conjuncts(bf.value_key):
            if not is_frame_bound(key=conjunct, time_columns={spine}):
                continue
            assert isinstance(conjunct, ArithmeticKey)
            kind, value = _bound(conjunct, spine)
            steps = periods if kind == "lower" else periods + 1
            moved = LiteralKey(value=add_units(floor_to(value, unit), unit, steps))
            key = ArithmeticKey(op=">=" if kind == "lower" else "<", operands=(spine, moved))
            out.append(bound_filter_from_key(key))
    return out
