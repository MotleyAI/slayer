"""SlayerModel v29 / SlayerQuery v19 / Memory v14: every stored relationship becomes ``trusted``, so stored models,
queries and memories keep their results and SQL. Stored-only; documents are not migrated."""

from typing import Any

from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import stamp_memory_query

_RELATIONSHIP_KEYS = ("relationships", "joins")
_POPULATION_KEYS = ("population", "source_model")


def _trusted_relationships(owner: dict) -> dict:
    for key in _RELATIONSHIP_KEYS:
        if isinstance(owner.get(key), list):
            owner[key] = [{**r, "trusted": r.get("trusted", True)} if isinstance(r, dict) else r for r in owner[key]]
    return owner


def trust_dataset(data: dict) -> dict:
    """``data`` with every relationship it stores, its backing queries' included, trusted."""
    data = _trusted_relationships(data)
    if isinstance(data.get("source_queries"), list):
        data["source_queries"] = [trust_query(dict(q)) if isinstance(q, dict) else q for q in data["source_queries"]]
    return data


def trust_query(data: dict) -> dict:
    """``data`` with every relationship its extensions and inline population declare trusted."""
    if isinstance(data.get("extensions"), list):
        data["extensions"] = [_trusted_relationships(dict(e)) if isinstance(e, dict) else e for e in data["extensions"]]
    for key in _POPULATION_KEYS:
        population: Any = data.get(key)
        if isinstance(population, dict):
            data[key] = trust_dataset(dict(population))
    return data


register_migration(entity="SlayerModel", source_version=28, stored_only=True)(trust_dataset)
register_migration(entity="SlayerQuery", source_version=18, stored_only=True)(trust_query)
register_migration(entity="Memory", source_version=13, stored_only=True)(stamp_memory_query)
