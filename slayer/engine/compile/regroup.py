"""The regroup primitive's structural core: an aggregation-based dimension groups
by a value that exists only after aggregating at a finer grain. Owns the pieces
shared by discovery and substitution (the position typing pass lives in the
checker, ``elaborate_env``); orchestration lives in ``compile/stages``."""

from __future__ import annotations

from typing import Dict, Mapping

from slayer.core.keys import REGROUP_LEAF_PREFIX, AggregateKey, ColumnKey, ValueKey, substitute_value_keys, walk_value_keys
from slayer.sql.naming import canonical_aggregate_alias
from slayer.ir.bound import BoundFilter

__all__ = [
    "REGROUP_LEAF_PREFIX",
    "RegroupPlaceholderRegistry",
    "substitute_in_bound_filter",
    "reserved_prefix_columns",
]


def reserved_prefix_columns(model) -> list:
    """``model`` columns colliding with the reserved regroup placeholder prefix."""
    if model is None:
        return []
    return [c.name for c in getattr(model, "columns", []) or []
            if c.name.startswith(REGROUP_LEAF_PREFIX)]


class RegroupPlaceholderRegistry:
    """Mints a distinct reserved-leaf ``ColumnKey`` per structural aggregate, keyed
    by ``AggregateKey`` identity NOT canonical alias (which two distinct aggregates
    can share); a monotonic index guarantees distinct leaves."""

    def __init__(self) -> None:
        self._by_key: Dict[ValueKey, ColumnKey] = {}

    def placeholder_for(self, key: ValueKey) -> ColumnKey:
        existing = self._by_key.get(key)
        if existing is not None:
            return existing
        idx = len(self._by_key)
        seed = (
            (canonical_aggregate_alias(key, profile="stage_formula")
             if isinstance(key, AggregateKey) else None)
            or getattr(key, "agg", None)
            or getattr(key, "op", None)
            or "regroup"
        )
        placeholder = ColumnKey(path=(), leaf=f"{REGROUP_LEAF_PREFIX}{idx}__{seed}")
        self._by_key[key] = placeholder
        return placeholder


def substitute_in_bound_filter(
    bf: BoundFilter, mapping: Mapping[ValueKey, ValueKey],
) -> BoundFilter:
    """Substitute placeholders in a filter and RECOMPUTE its phase (may lower to ROW)."""
    new_vk = substitute_value_keys(key=bf.value_key, mapping=mapping)
    refs = tuple(walk_value_keys(new_vk))
    phase = max((k.phase for k in refs), default=new_vk.phase)
    return BoundFilter(value_key=new_vk, phase=phase, referenced_keys=refs)
