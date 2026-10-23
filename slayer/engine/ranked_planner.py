"""Ranking-key resolution for ``first`` / ``last``: WHAT a ranked aggregate's
``ROW_NUMBER`` orders by, settled at plan time — the explicit ranking key, else the
first DATE/TIMESTAMP row-level dimension, else the first time dimension's untruncated
source (never the bucket, which ties every row), else the model default.
A target-rooted producer passes its already-re-rooted aggregate, so it resolves
host-style on its own root."""

from __future__ import annotations

from typing import List, Optional, Sequence, cast

from slayer.core.enums import RANKED_AGGREGATIONS
from slayer.core.keys import (
    VALUE_KEY_TYPES,
    AggregateKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    temporal_type,
    walk_value_keys,
)
from slayer.core.join_walker import model_column_type
from slayer.core.models import SlayerModel
from slayer.engine.reference_closure import column_default_key
from slayer.ir.planned import (
    SlotId,
    ValueSlot,
)
from slayer.ir.source_bundle import ResolvedSourceBundle

__all__ = [
    "explicit_ranking_key",
    "ordered_row_keys",
    "resolve_ranking_time_key",
]

def explicit_ranking_key(key: AggregateKey) -> Optional[ValueKey]:
    """The explicit ranking key (the first positional, a row-level expression), or ``None``."""
    if key.agg not in RANKED_AGGREGATIONS:
        return None
    for arg in key.args:
        return cast(ValueKey, arg) if isinstance(arg, VALUE_KEY_TYPES) else None
    return None


def _temporal_row_dimension_key(
    *,
    row_keys: Sequence[ValueKey],
    source_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    """The first row-level dimension typed DATE / TIMESTAMP — never a bucket, which ties a cell's rows."""
    column_type = model_column_type(model=source_model, models_by_name=bundle.models_by_name)
    for key in row_keys:
        if isinstance(key, TimeTruncKey) or any(
            isinstance(k, (AggregateKey, TransformKey)) for k in walk_value_keys(key)
        ):
            continue
        if temporal_type(key, column_type=column_type) is not None:
            return key
    return None


def _time_dimension_raw_column(
    *, row_keys: Sequence[ValueKey],
) -> Optional[ValueKey]:
    """The first time dimension's untruncated source."""
    for key in row_keys:
        if isinstance(key, TimeTruncKey):
            return key.source
    return None


def resolve_ranking_time_key(
    *,
    key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    row_keys: Sequence[ValueKey] = (),
) -> ValueKey:
    """What a ranked aggregate's ``ROW_NUMBER`` orders by, in the producer's root
    coordinates: the explicit ranking key, else the first temporal row-level dimension,
    else the first time dimension's untruncated source, else the model ``default_time_dimension``
    (typed via ``column_default_key`` so a derived default's crossings close). A
    target-rooted producer passes its already-re-rooted aggregate, so this is host-style."""
    arg = explicit_ranking_key(key)
    if arg is not None:
        return arg
    temporal = _temporal_row_dimension_key(
        row_keys=row_keys, source_model=root_model, bundle=bundle,
    )
    if temporal is not None:
        return temporal
    raw = _time_dimension_raw_column(row_keys=row_keys)
    if raw is not None:
        return raw
    default = root_model.effective_default_time_dimension
    if default:
        return column_default_key(path=(), leaf=default, base=root_model)
    raise ValueError(
        "first/last aggregation requires a ranking time column "
        "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
        "model's default_time_dimension); none is resolvable for "
        f"model {root_model.name!r}."
    )


def ordered_row_keys(
    *, row_slots: Sequence[ValueSlot], public_projection: Sequence[SlotId],
) -> List[ValueKey]:
    """Row-dimension keys in base-SELECT render order (publicly projected first,
    then the rest); the ranking precedence is order-sensitive."""
    by_id = {s.id: s for s in row_slots}
    seen: set = set()
    ordered: List[ValueKey] = []
    for sid in public_projection:
        slot = by_id.get(sid)
        if slot is not None and sid not in seen:
            seen.add(sid)
            ordered.append(slot.key)
    for slot in row_slots:
        if slot.id not in seen:
            seen.add(slot.id)
            ordered.append(slot.key)
    return ordered




