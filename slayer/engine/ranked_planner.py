"""Ranking-key resolution for ``first`` / ``last``: WHICH column a ranked
aggregate's ``ROW_NUMBER`` orders by, settled at plan time. host-rooted — explicit
time arg, else first DATE/TIMESTAMP row dimension, else first time dimension's RAW
column (never the truncated bucket, which ties every row), else the model default.
target-rooted — the same arg re-anchored, else the target default (host out of scope)."""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from slayer.core.enums import DataType
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    TimeTruncKey,
    ValueKey,
    reroot_value_key,
)
from slayer.core.models import SlayerModel
from slayer.engine.planned import (
    SlotId,
    ValueSlot,
)
from slayer.engine.source_bundle import ResolvedSourceBundle

__all__ = [
    "RANKED_AGGREGATIONS",
    "explicit_ranking_time_arg",
    "ordered_row_keys",
    "resolve_ranking_time_key",
]

#: The aggregations that rank; named once so classifier, planner and renderer agree.
RANKED_AGGREGATIONS = ("first", "last")

_TEMPORAL_TYPES = (DataType.DATE, DataType.TIMESTAMP)


def explicit_ranking_time_arg(key: AggregateKey) -> Optional[ValueKey]:
    """The explicit positional ranking-time arg (first positional iff a column ref), or ``None``."""
    if key.agg not in RANKED_AGGREGATIONS:
        return None
    for arg in key.args:
        return arg if isinstance(arg, (ColumnKey, ColumnSqlKey)) else None
    return None


def _ranking_key_name(key: ValueKey) -> str:
    if isinstance(key, ColumnKey):
        return ".".join((*key.path, key.leaf))
    if isinstance(key, ColumnSqlKey):
        return ".".join((*key.path, key.column_name))
    return type(key).__name__


def _resolves_on(*, key: ValueKey, model: SlayerModel) -> bool:
    """Whether ``key`` is reachable FROM ``model`` — a shallow check catching the
    common mistake (a HOST column as a TARGET-rooted ranking key) at plan time."""
    if isinstance(key, ColumnKey):
        leaf, path = key.leaf, key.path
    elif isinstance(key, ColumnSqlKey):
        leaf, path = key.column_name, key.path
    else:
        return True
    if path:
        return any(j.target_model == path[0] for j in (model.joins or []))
    return any(c.name == leaf for c in model.columns)


def _temporal_row_dimension_key(
    *,
    row_keys: Sequence[ValueKey],
    source_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    for key in row_keys:
        if not isinstance(key, ColumnKey):
            continue
        model: Optional[SlayerModel] = source_model
        for hop in key.path:
            model = bundle.get_referenced_model(hop)
            if model is None:
                break
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
    target_path: Tuple[str, ...] = (),
) -> ValueKey:
    """The column a ranked aggregate's ``ROW_NUMBER`` orders by, in the RANKED
    scope's coordinates. ``row_keys`` (host row dimensions) are candidates only for
    a HOST-rooted plan; a target-rooted CTE goes straight to the target's default."""
    arg = explicit_ranking_time_arg(key)
    if arg is not None:
        # Re-anchor into the target's coordinates in lockstep with the source.
        rerooted = reroot_value_key(arg, target_path=target_path)
        if target_path and not _resolves_on(key=rerooted, model=root_model):
            # A host column can't rank a target-rooted CTE (the relation runs 1:N).
            raise ValueError(
                f"first/last ranking column "
                f"{_ranking_key_name(rerooted)!r} is not resolvable on model "
                f"{root_model.name!r}, where a cross-model first/last ranks "
                f"its rows. Name a column of {root_model.name!r} (or one it "
                f"joins to), or drop the argument to rank by its "
                f"default_time_dimension."
            )
        return rerooted

    if not target_path:
        temporal = _temporal_row_dimension_key(
            row_keys=row_keys, source_model=root_model, bundle=bundle,
        )
        if temporal is not None:
            return temporal
        raw = _time_dimension_raw_column(row_keys=row_keys)
        if raw is not None:
            return raw
        if root_model.default_time_dimension:
            return ColumnKey(path=(), leaf=root_model.default_time_dimension)
        raise ValueError(
            "first/last aggregation requires a ranking time column "
            "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
            "model's default_time_dimension); none is resolvable for "
            f"model {root_model.name!r}."
        )

    if root_model.default_time_dimension:
        return ColumnKey(path=(), leaf=root_model.default_time_dimension)
    raise ValueError(
        f"first/last aggregation requires a ranking time column "
        f"(an explicit positional time arg, or the target "
        f"model's default_time_dimension); none is resolvable "
        f"for cross-model aggregate on target "
        f"{root_model.name!r}."
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




