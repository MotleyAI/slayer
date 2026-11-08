"""v8 → v9 schema migration for SlayerModel.

v9 lifts the ``__`` ban on model names and makes Mode-A join qualifiers
dotted-canonical (DEV-1743). The forward converter is a no-op: the actual
legacy ``customers__regions.name`` → ``customers.regions.name`` rewrite runs in
the storage load path (``StorageBackend._migrate_and_refine_on_load``), because
resolving a multi-hop walk needs sibling models a per-document dict converter
cannot see. The step just carries the payload forward.
"""

from __future__ import annotations

from slayer.storage.migrations import register_migration


@register_migration(entity="SlayerModel", source_version=8)
def _model_v8_to_v9(data: dict) -> dict:
    """No-op forward. The legacy-``__`` rewrite runs in the load path."""
    return data
