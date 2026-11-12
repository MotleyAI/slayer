"""SlayerModel v22 / SlayerQuery v14 / Memory v10: stamps only; stored extension columns are inlined by the
model-aware conversion in the storage load pipeline (``slayer.storage.extension_columns``). Stored-only."""

from slayer.storage.many_handling_migration import stamp_model_stages
from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import stamp_memory_query


def _stamp(data: dict) -> dict:
    return data


register_migration(entity="SlayerModel", source_version=21, stored_only=True)(stamp_model_stages)
register_migration(entity="SlayerQuery", source_version=13, stored_only=True)(_stamp)
register_migration(entity="Memory", source_version=9, stored_only=True)(stamp_memory_query)
