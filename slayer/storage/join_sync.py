"""Transparent StorageBackend wrapper that keeps inner joins symmetric.

When a model is saved, any inner join A→B automatically creates (or updates)
the reverse join B→A on the target model.  When a model is deleted, reverse
inner joins pointing back to it are removed from other models.

On first access the wrapper also performs a one-time reconciliation of all
stored models to heal asymmetric inner joins (e.g. from hand-edited YAML
files or older code that lacked sync).
"""

from typing import List, Optional, Set

from slayer.core.enums import JoinType
from slayer.core.models import DatasourceConfig, ModelJoin, SlayerModel
from slayer.storage.base import StorageBackend


# ---------------------------------------------------------------------------
# Helpers (operate on the *raw* inner storage to avoid recursion)
# ---------------------------------------------------------------------------


async def _mirror_inner_joins(model: SlayerModel, storage: StorageBackend) -> None:
    """Ensure every inner join on *model* has a matching reverse on the target.

    If the reverse join already exists but has stale ``join_pairs``, it is
    updated in-place.
    """
    for join in model.joins:
        if join.join_type != JoinType.INNER or join.target_model == model.name:
            continue
        target = await storage.get_model(join.target_model)
        if target is None:
            continue
        reverse_pairs = [[tgt, src] for src, tgt in join.join_pairs]
        existing = next(
            (j for j in target.joins
             if j.target_model == model.name and j.join_type == JoinType.INNER),
            None,
        )
        if existing is not None:
            if existing.join_pairs != reverse_pairs:
                existing.join_pairs = reverse_pairs
                await storage.save_model(target)
        else:
            target.joins.append(ModelJoin(
                target_model=model.name,
                join_pairs=reverse_pairs,
                join_type=JoinType.INNER,
            ))
            await storage.save_model(target)


async def _remove_inner_joins_to(
    source_name: str,
    target_names: Set[str],
    storage: StorageBackend,
) -> None:
    """Remove inner joins that point back to *source_name* from each target."""
    for target_name in target_names:
        target = await storage.get_model(target_name)
        if target is None:
            continue
        before = len(target.joins)
        target.joins = [
            j for j in target.joins
            if not (j.target_model == source_name and j.join_type == JoinType.INNER)
        ]
        if len(target.joins) < before:
            await storage.save_model(target)


async def _reconcile_all_inner_joins(storage: StorageBackend) -> None:
    """Scan every model and add missing reverse inner joins.

    Intended to run once at startup to heal asymmetries from hand-edited
    files, batch creation ordering, or older code without sync.
    """
    names = await storage.list_models()
    for name in names:
        model = await storage.get_model(name)
        if model is None:
            continue
        await _mirror_inner_joins(model, storage)


# ---------------------------------------------------------------------------
# Wrapper
# ---------------------------------------------------------------------------


class JoinSyncStorage(StorageBackend):
    """Decorator around any :class:`StorageBackend` that keeps inner joins
    symmetric automatically."""

    def __init__(self, inner: StorageBackend) -> None:
        self._inner = inner
        self._reconciled = False

    # -- helpers ----------------------------------------------------------

    async def _ensure_reconciled(self) -> None:
        if not self._reconciled:
            self._reconciled = True
            await _reconcile_all_inner_joins(self._inner)

    # -- model operations (with sync) -------------------------------------

    async def save_model(self, model: SlayerModel) -> None:
        await self._ensure_reconciled()
        old = await self._inner.get_model(model.name)
        await self._inner.save_model(model)

        # Mirror outward: ensure reverse inner joins exist / are up-to-date.
        await _mirror_inner_joins(model, self._inner)

        # Clean up: if an inner join was removed or changed to LEFT, remove
        # the now-orphaned reverse join on the target model.
        if old is not None:
            old_inner_targets = {
                j.target_model for j in old.joins
                if j.join_type == JoinType.INNER and j.target_model != old.name
            }
            new_inner_targets = {
                j.target_model for j in model.joins
                if j.join_type == JoinType.INNER and j.target_model != model.name
            }
            removed = old_inner_targets - new_inner_targets
            if removed:
                await _remove_inner_joins_to(model.name, removed, self._inner)

    async def delete_model(self, name: str) -> bool:
        await self._ensure_reconciled()
        model = await self._inner.get_model(name)
        result = await self._inner.delete_model(name)
        if result and model:
            inner_targets = {
                j.target_model for j in model.joins
                if j.join_type == JoinType.INNER and j.target_model != name
            }
            if inner_targets:
                await _remove_inner_joins_to(name, inner_targets, self._inner)
        return result

    # -- pure delegation --------------------------------------------------

    async def get_model(self, name: str) -> Optional[SlayerModel]:
        await self._ensure_reconciled()
        return await self._inner.get_model(name)

    async def list_models(self) -> List[str]:
        await self._ensure_reconciled()
        return await self._inner.list_models()

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        return await self._inner.save_datasource(datasource)

    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]:
        return await self._inner.get_datasource(name)

    async def list_datasources(self) -> List[str]:
        return await self._inner.list_datasources()

    async def delete_datasource(self, name: str) -> bool:
        return await self._inner.delete_datasource(name)
