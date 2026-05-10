"""v5 → v6 schema migration for SlayerModel (DEV-1375).

v6 introduces a single new optional field — ``Column.sampled`` — which
caches the per-column sample-value snapshot the search index reads at
query time. The forward conversion is a no-op because the new field
defaults to ``None``; first subsequent ingest / refresh-samples
populates the cache.
"""

from __future__ import annotations

from slayer.storage.migrations import register_migration


@register_migration("SlayerModel", 5)
def _model_v5_to_v6(data: dict) -> dict:
    """No-op forward. ``Column.sampled`` defaults to ``None`` on validation."""
    return data
