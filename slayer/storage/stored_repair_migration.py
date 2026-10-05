"""SlayerModel v14 / SlayerQuery v6 / Memory v4: stored-only repairs for documents written by 0.10.x and by v13 builds.

Re-applies the rank-direction rewrite (now covering persisted ``raw_formula`` order items) and repairs legacy ``date_range`` values.
"""

from slayer.storage.legacy_time_literals import repair_date_ranges
from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import rewrite_model_ranks, rewrite_query_ranks, stamp_memory_query


def _repair_query(data: dict) -> dict:
    return repair_date_ranges(rewrite_query_ranks(data))


register_migration(entity="SlayerModel", source_version=13, stored_only=True)(rewrite_model_ranks)
register_migration(entity="SlayerQuery", source_version=5, stored_only=True)(_repair_query)
register_migration(entity="Memory", source_version=3, stored_only=True)(stamp_memory_query)
