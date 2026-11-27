"""The time_shift regime classifier and per-leaf shifted-evaluation rule."""

from __future__ import annotations

from typing import List, Mapping, Optional, Tuple

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    AttachedKey,
    FieldKey,
    FieldSqlKey,
    CastKey,
    ListSetKey,
    ScalarCallKey,
    TimeTruncKey,
    ValueKey,
    constituent_grain,
    is_boolean_shaped,
    is_kernel_requiring,
    is_transform,
    source_anchor_path,
)

__all__ = ["carried_placeholders", "_series_mode"]


def _series_flags(node: ValueKey, *, to_original: Mapping[ValueKey, ValueKey]) -> Tuple[bool, bool]:
    """(has_transform, has_cross_model_agg) over a composite; an attached value
    resolves to its original aggregate."""
    has_transform = False
    has_cross_dataset = False
    if is_transform(node):
        has_transform = True
    elif isinstance(node, AggregateKey):
        if source_anchor_path(node.source):
            has_cross_dataset = True
    elif isinstance(node, AttachedKey):
        original = to_original.get(node)
        if original is None:
            has_cross_dataset = True  # unknown attached value — fail closed
        else:
            return _series_flags(original, to_original=to_original)
    elif not isinstance(node, (FieldKey, FieldSqlKey, TimeTruncKey)):
        for child in node.children():
            t, x = _series_flags(child, to_original=to_original)
            has_transform = has_transform or t
            has_cross_dataset = has_cross_dataset or x
    return has_transform, has_cross_dataset


def _series_mode(inner: ValueKey, *, to_original: Mapping[ValueKey, ValueKey]) -> bool:
    """Whether a transform shifts its materialised series (True) or re-aggregates
    (False) — a nested transform, a predicate root, or a composite carrying a
    transform or cross-dataset aggregate leaf."""
    if is_transform(inner) or is_boolean_shaped(inner):
        return True
    if isinstance(inner, AttachedKey) and is_transform(to_original.get(inner)):
        return True
    if isinstance(inner, (ArithmeticKey, ScalarCallKey, CastKey, ListSetKey)):
        has_transform, has_cross_dataset = _series_flags(inner, to_original=to_original)
        return has_transform or has_cross_dataset
    return False


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
    inner: ValueKey, *, axis: ValueKey, to_original: Mapping[ValueKey, ValueKey],
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> List[ValueKey]:
    """Consumer-level attached values of ``inner`` read at their in-frame value: an
    axis-free leaf is constant along the axis; one whose grain holds the axis, or a
    ranked / windowed one, is re-evaluated instead."""
    carried: List[ValueKey] = []

    def visit(node: ValueKey) -> None:
        if isinstance(node, AttachedKey):
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
