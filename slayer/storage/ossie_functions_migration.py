"""SlayerModel v32 / SlayerQuery v20 / Memory v15: stamps only; a stored granularity named like an Ossie function the
library reserved here is renamed through its gate (``slayer.storage.granularity_rename``). Stored-only."""

from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import stamp_memory_query


def _stamp(data: dict) -> dict:
    return data


register_migration(entity="SlayerModel", source_version=31, stored_only=True)(_stamp)
register_migration(entity="SlayerQuery", source_version=19, stored_only=True)(_stamp)
register_migration(entity="Memory", source_version=14, stored_only=True)(stamp_memory_query)
