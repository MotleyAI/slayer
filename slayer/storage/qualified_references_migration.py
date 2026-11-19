"""SlayerModel v27 / SlayerQuery v17 / Memory v13: stamps only; stored queries are upgraded to qualified references by
the model-aware conversion in the storage load pipeline (``slayer.storage.qualified_references``). Stored-only."""

from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import stamp_memory_query


def _stamp(data: dict) -> dict:
    return data


register_migration(entity="SlayerModel", source_version=26, stored_only=True)(_stamp)
register_migration(entity="SlayerQuery", source_version=16, stored_only=True)(_stamp)
register_migration(entity="Memory", source_version=12, stored_only=True)(stamp_memory_query)
