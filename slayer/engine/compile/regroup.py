"""The regroup primitive's structural core: an aggregation-based dimension groups
by a value that exists only after aggregating at a finer grain. Owns the pieces
shared by discovery and substitution (the position typing pass lives in the
checker, ``elaborate_env``); orchestration lives in ``compile/stages``."""

from __future__ import annotations

from typing import AbstractSet, Callable, Dict, Mapping, Optional

from slayer.core.enums import DataType
from slayer.core.keys import REGROUP_LEAF_PREFIX, AggregateKey, FieldKey, ValueKey, substitute_value_keys, walk_value_keys
from slayer.engine.key_metadata import SlotTyping
from slayer.sql.naming import canonical_aggregate_alias
from slayer.ir.bound import BoundFilter
from slayer.core.models import SlayerDataset

__all__ = [
    "REGROUP_LEAF_PREFIX",
    "RegroupPlaceholderRegistry",
    "substitute_in_bound_filter",
    "reserved_prefix_fields",
]


def reserved_prefix_fields(dataset: SlayerDataset | None) -> list[str]:
    """``dataset`` fields colliding with the reserved regroup placeholder prefix."""
    if dataset is None:
        return []
    return [c.name for c in dataset.fields if c.name.startswith(REGROUP_LEAF_PREFIX)]


class RegroupPlaceholderRegistry:
    """Mints a distinct reserved-leaf ``FieldKey`` per structural aggregate, keyed
    by ``AggregateKey`` identity NOT canonical alias (which two distinct aggregates
    can share); a monotonic index guarantees distinct leaves, skipping ``reserved``
    (an enclosing plan's placeholders a sub-plan may read). ``types`` holds each
    minted leaf's type under ``typing``, the minting plan's."""

    def __init__(
        self, *, reserved: AbstractSet[ValueKey] = frozenset(), typing: Optional[SlotTyping] = None,
    ) -> None:
        self._by_key: Dict[ValueKey, FieldKey] = {}
        self._reserved = reserved
        self._next = 0
        self._typing = typing
        self.types: Dict[str, Callable[[], Optional[DataType]]] = {}

    def placeholder_for(self, key: ValueKey) -> FieldKey:
        existing = self._by_key.get(key)
        if existing is not None:
            return existing
        seed = (
            (canonical_aggregate_alias(key, profile="stage_expression")
             if isinstance(key, AggregateKey) else None)
            or getattr(key, "agg", None)
            or getattr(key, "op", None)
            or "regroup"
        )
        placeholder = self._mint(seed)
        while placeholder in self._reserved:
            placeholder = self._mint(seed)
        self._by_key[key] = placeholder
        typing = self._typing
        if typing is not None:
            self.types[placeholder.leaf] = lambda: typing.value_type(key)
        return placeholder

    def _mint(self, seed: str) -> FieldKey:
        self._next += 1
        return FieldKey(path=(), leaf=f"{REGROUP_LEAF_PREFIX}{self._next - 1}__{seed}")


def substitute_in_bound_filter(
    bf: BoundFilter, mapping: Mapping[ValueKey, ValueKey],
    *, substitute: Callable[..., ValueKey] = substitute_value_keys,
) -> BoundFilter:
    """Substitute placeholders in a filter and RECOMPUTE its phase (may lower to ROW).

    ``substitute`` selects the traversal law (deep by default; the re-aggregation
    pre-substitution passes the consumer-scoped one)."""
    new_vk = substitute(key=bf.value_key, mapping=mapping)
    refs = tuple(walk_value_keys(new_vk))
    phase = max((k.phase for k in refs), default=new_vk.phase)
    return bf.model_copy(update={"value_key": new_vk, "phase": phase, "referenced_keys": refs})
