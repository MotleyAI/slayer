"""The datasource of an unpinned stored memory whose population names no stored dataset (``models/stored-upgrade``).

Every load-time upgrade reads it: qualification, literal typing, the custom-bucket rewrite and the granularity rename.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence

from slayer.core import vocabulary
from slayer.core.time_spine import is_spine_spelling

__all__ = ["memory_fallback_population", "raw_memory_datasource", "uses_fallback_datasource"]


def uses_fallback_datasource(population: str | None, *, stored_names: Collection[str]) -> bool:
    """Whether an unpinned stored query's population names no stored dataset: none, or a spine spelling no stored
    dataset spells exactly."""
    return population is None or (is_spine_spelling(population) and population not in stored_names)


def memory_fallback_population(data: dict, *, identities: Sequence[tuple[str, str]]) -> tuple[bool, str | None]:
    """``(triggered, population)``: whether a raw stored memory's query, pinned by no inline dataset, names no stored
    dataset as its population, and that population."""
    query = data.get("query")
    if not isinstance(query, dict):
        return False, None
    try:
        population = vocabulary.query(query, vocabulary.Spellings(stored=True)).get("population")
    except ValueError:
        return False, None
    if vocabulary.is_extension_object(population):
        population = vocabulary.extension_dataset(population)
    if population is not None and not isinstance(population, str):
        return False, None
    return uses_fallback_datasource(population, stored_names={n for _, n in identities}), population


def raw_memory_datasource(data: dict, *, identities: Sequence[tuple[str, str]]) -> str | None:
    """A triggering memory's datasource: the one its dataset-level entities name, else the only one holding a
    dataset; ``None`` when neither decides."""
    stored = set(identities)
    pinned = {
        parts[0] for entity in data.get("entities") or [] if isinstance(entity, str)
        for parts in [entity.split(".")] if len(parts) in (2, 3) and (parts[0], parts[1]) in stored
    }
    if len(pinned) == 1:
        return pinned.pop()
    holding = {ds for ds, _ in identities}
    return holding.pop() if len(holding) == 1 else None
