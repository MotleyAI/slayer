"""Ranking-key resolution for ``first`` / ``last``: WHICH column a ranked
aggregate's ``ROW_NUMBER`` orders by, settled at plan time — the explicit time arg,
else the first DATE/TIMESTAMP row dimension, else the first time dimension's RAW
column (never the truncated bucket, which ties every row), else the model default.
A target-rooted producer passes its already-re-rooted aggregate, so it resolves
host-style on its own root."""

from __future__ import annotations

from typing import List, Optional, Sequence

from slayer.core.enums import DataType, RANKED_AGGREGATIONS
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    TimeTruncKey,
    ValueKey,
)
from slayer.core.join_walker import terminal_model
from slayer.core.models import SlayerModel
from slayer.engine.reference_closure import column_default_key
from slayer.ir.planned import (
    SlotId,
    ValueSlot,
)
from slayer.ir.source_bundle import ResolvedSourceBundle

__all__ = [
    "explicit_ranking_time_arg",
    "ordered_row_keys",
    "resolve_ranking_time_key",
]

_TEMPORAL_TYPES = (DataType.DATE, DataType.TIMESTAMP)


def explicit_ranking_time_arg(key: AggregateKey) -> Optional[ValueKey]:
    """The explicit positional ranking-time arg (first positional iff a column ref), or ``None``."""
    if key.agg not in RANKED_AGGREGATIONS:
        return None
    for arg in key.args:
        return arg if isinstance(arg, (ColumnKey, ColumnSqlKey)) else None
    return None


def _temporal_row_dimension_key(
    *,
    row_keys: Sequence[ValueKey],
    source_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    models_by_name = bundle.models_by_name
    for key in row_keys:
        if not isinstance(key, ColumnKey):
            continue
        model = terminal_model(
            root=source_model, path=key.path, models_by_name=models_by_name,
        )
        if model is None:
            continue
        col = next((c for c in model.columns if c.name == key.leaf), None)
        if col is not None and col.type in _TEMPORAL_TYPES:
            return key
    return None


def _time_dimension_raw_column(
    *, row_keys: Sequence[ValueKey],
) -> Optional[ValueKey]:
    """The first time dimension's RAW column, un-truncated."""
    for key in row_keys:
        if isinstance(key, TimeTruncKey):
            return key.column
    return None


def resolve_ranking_time_key(
    *,
    key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    row_keys: Sequence[ValueKey] = (),
) -> ValueKey:
    """The column a ranked aggregate's ``ROW_NUMBER`` orders by, in the producer's
    root coordinates: explicit positional arg, else the first temporal row dimension,
    else the first time dimension's raw column, else the model ``default_time_dimension``
    (typed via ``column_default_key`` so a derived default's crossings close). A
    target-rooted producer passes its already-re-rooted aggregate, so this is host-style."""
    arg = explicit_ranking_time_arg(key)
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
    if root_model.default_time_dimension:
        return column_default_key(
            path=(), leaf=root_model.default_time_dimension, base=root_model,
        )
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




