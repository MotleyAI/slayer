"""The host of a ``time_spine × P`` population: its spine field, bounds and bucket series."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from math import ceil
from typing import List, Optional, Tuple

from slayer.core.enums import UNIT_MONTHS, UNIT_SECONDS, DataType, TimeGranularity
from slayer.core.granularity import Granularity, granularity_parts, nests_into, resolve_granularity
from slayer.core.keys import (
    AggregateKey, ArithmeticKey, FieldKey, FieldSqlKey, LiteralKey, TimeTruncKey, TransformKey, ValueKey, source_anchor_path,
    is_attached_source, split_top_level_and,
    field_leaf, walk_value_keys,
)
from slayer.core.models import SlayerDataset
from slayer.core.refs import bucket_remedy, key_display
from slayer.core.relationship_walker import SPINE_TOKEN, reaches_spine, spine_token
from slayer.core.time_bounds import is_frame_bound, strip_frame_bounds
from slayer.core.time_points import add_units, floor_to
from slayer.core.time_spine import TIME_SPINE_FIELD, TIME_SPINE_DATASET, check_default_time_dimension, is_spine
from slayer.engine.compile.shift import is_placeholder
from slayer.core.time_zones import CIVIL
from slayer.engine.elaborate_env import (
    check_time_dimension_field,
    check_spine_aggregation,
    check_spine_filter,
    check_spine_lower_bound,
    check_spine_plain_use,
    check_spine_raw_rows,
)
from slayer.ir.planned import SpineFactor
from slayer.ir.prebound import PreboundQuery, walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle

_MIRROR = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}
_SERIES_FALLBACKS = (TimeGranularity.DAY, TimeGranularity.HOUR, TimeGranularity.MINUTE, TimeGranularity.SECOND)


def spine_field(dataset: Optional[SlayerDataset], *, bundle: ResolvedSourceBundle) -> Optional[FieldKey]:
    """The spine field as the host reads it: its own field, or across the product hop."""
    if dataset is None:
        return None
    if is_spine(dataset):
        return FieldKey(path=(), leaf=TIME_SPINE_FIELD)
    if dataset.population_spine:
        hop = spine_token(dataset=dataset, datasets_by_name=bundle.datasets_by_name)
        return FieldKey(path=(hop,), leaf=TIME_SPINE_FIELD)
    return None


def _mentions(key: ValueKey, spine: FieldKey) -> bool:
    return any(k == spine for k in walk_value_keys(key))


def _reads_spine(key: ValueKey, spine: FieldKey) -> bool:
    """``spine`` occurs in ``key`` other than as a transform's axis (the query's active bucket)."""
    if key == spine:
        return True
    children = [key.input, *key.partition_keys] if isinstance(key, TransformKey) else key.children()
    return any(_reads_spine(c, spine) for c in children)


def _bare(key: ValueKey, spine: FieldKey) -> bool:
    """``spine`` occurs outside a bucket of it."""
    if isinstance(key, TimeTruncKey) and key.source == spine:
        return False
    return key == spine or any(_bare(c, spine) for c in key.children())


def _spine_aggregate(
    key: ValueKey, *, spine: FieldKey, host: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Optional[AggregateKey]:
    for k in walk_value_keys(key):
        # A re-aggregation reads its operand's cells, never the spine's rows.
        if not isinstance(k, AggregateKey) or is_attached_source(k.source) or any(
                is_placeholder(r) for r in walk_value_keys(k.source)):
            continue
        home = walk_key_path(dataset=host, path=source_anchor_path(k.source), bundle=bundle)
        if _mentions(k.source, spine) or (home is not None and is_spine(home)):
            return k
    return None


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    assert isinstance(value, date)
    return datetime(value.year, value.month, value.day)


def _bound(conjunct: ArithmeticKey, spine: FieldKey) -> Tuple[str, datetime]:
    """``("lower" | "upper", instant)`` of one frame bound; an upper instant is exclusive."""
    field, literal = conjunct.operands
    op = conjunct.op
    if field != spine:
        field, literal, op = literal, field, _MIRROR[op]
    assert isinstance(literal, LiteralKey)
    value = _as_datetime(literal.value)
    if op in (">=", ">"):
        return "lower", value
    return "upper", value + timedelta(microseconds=1) if op == "<=" else value


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
    *, prebound: PreboundQuery, host: SlayerDataset, spine: FieldKey, bundle: ResolvedSourceBundle,
) -> SpineFactor:
    """The host's bucket series, after checking every position the spine field appears in."""
    check_spine_raw_rows(raw_rows=not prebound.distinct_dimension_values)
    for dataset in bundle.datasets_by_name.values():
        check_default_time_dimension(dataset)  # the save-time check, for a dataset stored without it
    spine_tds = _checked_spine_time_dimensions(prebound=prebound, host=host, spine=spine, bundle=bundle)
    lowers, uppers = _spine_bounds(prebound=prebound, host=host, spine=spine, bundle=bundle)
    check_spine_lower_bound(has_lower=bool(lowers))
    granularity = series_granularity([k.granularity for k in spine_tds] or [TimeGranularity.DAY])
    lower = max(lowers)
    upper = min(uppers) if uppers else add_units(floor_to(bundle.now, granularity), granularity, 1)
    return SpineFactor(
        granularity=granularity, lower=lower, upper=upper,
        size=_series_size(granularity=granularity, lower=lower, upper=upper), path=tuple(spine.path),
    )


def _checked_spine_time_dimensions(
    *, prebound: PreboundQuery, host: SlayerDataset, spine: FieldKey, bundle: ResolvedSourceBundle,
) -> List[TimeTruncKey]:
    """The spine time dimensions, after checking the dimension, metric and order positions."""
    n_dims, n_tds = prebound.n_dims, prebound.n_time_dimensions
    for dm in prebound.declared_metrics[:n_dims]:
        key = dm.bound.value_key
        check_spine_plain_use(
            offender=dm.declared_name if _mentions(key, spine) else None, position="dimension",
            remedy=bucket_remedy(key),
        )
    for dm in prebound.declared_metrics[n_dims + n_tds:]:
        key = dm.bound.value_key
        agg = _spine_aggregate(key, spine=spine, host=host, bundle=bundle)
        check_spine_aggregation(offender=key_display(agg) if agg is not None else None)
        check_spine_plain_use(offender=dm.declared_name if _bare(key, spine) else None, position="metric")
    spine_tds = [
        dm.bound.value_key for dm in prebound.declared_metrics[n_dims:n_dims + n_tds]
        if isinstance(dm.bound.value_key, TimeTruncKey) and dm.bound.value_key.source == spine
    ]
    projected = set(spine_tds)
    for spec in prebound.order_specs:
        key = spec.bound.value_key
        check_spine_plain_use(
            offender=key_display(key) if key not in projected and _mentions(key, spine) else None,
            position="order key",
        )
    return spine_tds


def _spine_bounds(
    *, prebound: PreboundQuery, host: SlayerDataset, spine: FieldKey, bundle: ResolvedSourceBundle,
) -> Tuple[List[datetime], List[datetime]]:
    """``(lowers, uppers)`` of the filters' spine frame bounds; any other spine conjunct fails."""
    lowers: List[datetime] = []
    uppers: List[datetime] = []
    texts = list(prebound.bound_filter_texts) + [None] * len(prebound.bound_filters)
    for bf, text in zip(prebound.bound_filters, texts):
        for conjunct in split_top_level_and(bf.value_key):
            if is_frame_bound(key=conjunct, time_fields={spine}):
                assert isinstance(conjunct, ArithmeticKey)
                kind, value = _bound(conjunct, spine)
                (lowers if kind == "lower" else uppers).append(value)
            elif _reads_spine(conjunct, spine):
                check_spine_filter(offender=text or key_display(conjunct))
            agg = _spine_aggregate(conjunct, spine=spine, host=host, bundle=bundle)
            check_spine_aggregation(offender=key_display(agg) if agg is not None else None)
    return lowers, uppers


def check_off_spine_order_keys(
    prebound: PreboundQuery, *, host: Optional[SlayerDataset], bundle: ResolvedSourceBundle,
) -> None:
    """Outside a spine population no bucket of the spine is projected, so no order key may read it."""
    def reads_spine(path: Tuple[str, ...]) -> bool:
        if host is None:
            return bool({TIME_SPINE_DATASET, SPINE_TOKEN} & set(path))
        return reaches_spine(root=host, path=path, datasets_by_name=bundle.datasets_by_name)

    for spec in prebound.order_specs:
        key = spec.bound.value_key
        reads = any(isinstance(k, FieldKey) and reads_spine(k.path) for k in walk_value_keys(key))
        check_spine_plain_use(offender=key_display(key) if reads else None, position="order key")


def is_spine_bound(key: ValueKey, *, spine: Optional[FieldKey]) -> bool:
    return spine is not None and is_frame_bound(key=key, time_fields={spine})


def host_mask(key: ValueKey, *, spine: Optional[FieldKey]) -> Optional[ValueKey]:
    """A host filter without its spine bounds (they decide bucket existence, not host rows)."""
    return key if spine is None else strip_frame_bounds(key=key, time_fields={spine})


def check_axis_rebucket(
    *, host_key: ValueKey, rerooted: ValueKey, host: SlayerDataset, root: SlayerDataset,
    bundle: ResolvedSourceBundle,
) -> None:
    """A spine bucket attributed through a fact's axis obeys that axis field's recorded granularity."""
    spine = spine_field(host, bundle=bundle)
    if not (isinstance(host_key, TimeTruncKey) and host_key.source == spine and isinstance(rerooted, TimeTruncKey)):
        return
    field = rerooted.source
    if not isinstance(field, (FieldKey, FieldSqlKey)):
        return
    owner = walk_key_path(dataset=root, path=tuple(field.path), bundle=bundle)
    leaf = field_leaf(field)
    col = owner.get_field(leaf) if owner is not None else None
    if col is None or col.granularity is None:
        return
    name = ".".join((*field.path, leaf))
    upstream = resolve_granularity(col.granularity, defined=bundle.granularities)
    stage = owner is not None and owner.name in bundle.stage_displays
    zone = col.time_zone if col.time_zone is not None or stage else bundle.datasource_time_zone
    zoned = col.datatype is DataType.DATETIME and zone is not None and zone != CIVIL
    check_time_dimension_field(
        name=name, field_type=col.datatype, requested_granularity=rerooted.granularity,
        bucketed_leaves=() if upstream is None else ((name, upstream),),
        zone=bundle.time_zone, bucketed_zones=((name, zone),) if zoned and zone is not None else (),
    )

