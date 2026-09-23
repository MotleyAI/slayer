"""The time_shift regime classifier and per-leaf shifted-evaluation rule (DEV-1958)."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    REGROUP_LEAF_PREFIX,
    ScalarCallKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    constituent_grain,
    is_boolean_shaped,
    source_anchor_path,
    window_kwarg_of,
)
from slayer.core.enums import RANKED_AGGREGATIONS

__all__ = ["carried_placeholders", "is_placeholder", "_series_mode"]


def is_placeholder(key: ValueKey) -> bool:
    return isinstance(key, ColumnKey) and key.leaf.startswith(REGROUP_LEAF_PREFIX)


def _series_flags(node: ValueKey, *, to_original: Dict[ValueKey, ValueKey]) -> Tuple[bool, bool]:
    """(has_transform, has_cross_model_agg) over a composite; a placeholder
    resolves to its original aggregate."""
    has_transform = False
    has_cross_model = False
    if isinstance(node, TransformKey):
        has_transform = True
    elif isinstance(node, AggregateKey):
        if source_anchor_path(node.source):
            has_cross_model = True
    elif is_placeholder(node):
        original = to_original.get(node)
        if original is None:
            has_cross_model = True  # unknown placeholder — fail closed
        else:
            return _series_flags(original, to_original=to_original)
    elif not isinstance(node, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        for child in node.children():
            t, x = _series_flags(child, to_original=to_original)
            has_transform = has_transform or t
            has_cross_model = has_cross_model or x
    return has_transform, has_cross_model


def _series_mode(inner: ValueKey, *, to_original: Dict[ValueKey, ValueKey]) -> bool:
    """Whether a transform shifts its materialised series (True) or re-aggregates
    (False) — a nested transform, a predicate root, or a composite carrying a
    transform or cross-model aggregate leaf."""
    if isinstance(inner, TransformKey) or is_boolean_shaped(inner):
        return True
    if isinstance(inner, (ArithmeticKey, ScalarCallKey)):
        has_transform, has_cross_model = _series_flags(inner, to_original=to_original)
        return has_transform or has_cross_model
    return False


def _re_evaluated(
    original: ValueKey, *, axis: ValueKey, dim_keys: List[ValueKey],
    td_keys: List[ValueKey], active_bucket: Optional[ValueKey],
) -> bool:
    if isinstance(original, AggregateKey) and (
        original.agg in RANKED_AGGREGATIONS or window_kwarg_of(original) is not None
    ):
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
        if isinstance(node, (AggregateKey, TransformKey)):
            return
        for child in node.children():
            visit(child)

    visit(inner)
    return carried
