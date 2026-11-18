"""SlayerModel v26 / SlayerQuery v16 / Memory v12 / Embedding v2: stored documents respelled in Ossie's vocabulary.

Stored-only and silent; fresh input converts with a deprecation instead (``slayer.core.vocabulary``).
"""

from typing import Any

from slayer.core.vocabulary import stored_dataset, stored_query
from slayer.storage.migrations import register_migration

#: The SlayerQuery version current while SlayerModel was at v25 and Memory at v11.
_QUERY_VERSION = 15
#: Embedding entity kinds before 2.0 and their replacements.
ENTITY_KINDS = {"model": "dataset", "column": "field", "measure": "metric"}


def _stamped(query: Any) -> Any:
    """A version-less stored query, stamped with its era's query version so the query step runs on it."""
    if isinstance(query, dict) and "version" not in query:
        return {**query, "version": _QUERY_VERSION}
    return query


def upgrade_dataset(data: dict) -> dict:
    data = stored_dataset(data)
    if isinstance(data.get("source_queries"), list):
        data["source_queries"] = [_stamped(q) for q in data["source_queries"]]
    return data


def upgrade_memory(data: dict) -> dict:
    if isinstance(data.get("query"), dict):
        data["query"] = _stamped(data["query"])
    return data


def upgrade_embedding(data: dict) -> dict:
    kind = data.get("entity_kind")
    if isinstance(kind, str) and kind in ENTITY_KINDS:
        data["entity_kind"] = ENTITY_KINDS[kind]
    return data


register_migration(entity="SlayerModel", source_version=25, stored_only=True)(upgrade_dataset)
register_migration(entity="SlayerQuery", source_version=15, stored_only=True)(stored_query)
register_migration(entity="Memory", source_version=11, stored_only=True)(upgrade_memory)
register_migration(entity="Embedding", source_version=1)(upgrade_embedding)
