"""v7 → v8 schema migration for SlayerModel.

v8 adds one optional field, ``source_kind`` (the kind of object ``sql_table``
names). The forward conversion is a no-op: it defaults to ``None``, which is
also correct for a pre-v8 model — its backing object is unknown, and the next
ingest classifies it for real rather than guessing ``"table"``.
"""

from __future__ import annotations

from slayer.storage.migrations import register_migration


@register_migration(entity="SlayerModel", source_version=7)
def _model_v7_to_v8(data: dict) -> dict:
    """No-op forward. ``source_kind`` defaults to ``None`` on validation."""
    return data
