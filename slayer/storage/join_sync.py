"""Transparent StorageBackend wrapper that keeps inner joins symmetric.

When a model is saved, any inner join A→B automatically creates (or updates)
the reverse join B→A on the target model.  When a model is deleted, reverse
inner joins pointing back to it are removed from other models.

On first access the wrapper also performs a one-time reconciliation of all
stored models to heal asymmetric inner joins (e.g. from hand-edited YAML
files or older code that lacked sync).

v4 (DEV-1330): join targets are resolved within the parent model's
``data_source`` only — cross-datasource joins are never auto-mirrored.
"""

from typing import List, Optional, Set, Tuple

from slayer.core.enums import JoinType
from slayer.core.models import DatasourceConfig, ModelJoin, SlayerModel
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend


# ---------------------------------------------------------------------------
# Helpers (operate on the *raw* inner storage to avoid recursion)
# ---------------------------------------------------------------------------


async def _mirror_inner_joins(model: SlayerModel, storage: StorageBackend) -> None:
    """Ensure every inner join on *model* has a matching reverse on the target.

    Targets are looked up inside the parent model's data_source — joins
    that resolve only to a different datasource are silently skipped.
    """
    for join in model.joins:
        if join.join_type != JoinType.INNER or join.target_model == model.name:
            continue
        target = await storage.get_model(join.target_model, data_source=model.data_source)
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
    source_data_source: str,
    target_names: Set[str],
    storage: StorageBackend,
) -> None:
    """Remove inner joins that point back to *source_name* from each target.

    Targets are resolved within the source model's datasource — same scoping
    rule as ``_mirror_inner_joins``.
    """
    for target_name in target_names:
        target = await storage.get_model(target_name, data_source=source_data_source)
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
    identities = await storage._list_all_model_identities()
    for ds, name in identities:
        model = await storage.get_model(name, data_source=ds)
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

    async def _save_model_impl(self, model: SlayerModel) -> None:
        """Decorator-level save: delegate the actual write to the inner
        backend (which itself runs the cycle-detection template method
        before its own ``_save_model_impl``), then keep inner-join mirrors
        in sync. Overriding ``_save_model_impl`` rather than ``save_model``
        keeps cycle validation at the public layer single-pass — the
        ``StorageBackend.save_model`` template runs validation on this
        decorator before invoking ``_save_model_impl``.
        """
        await self._ensure_reconciled()
        old = await self._inner.get_model(model.name, data_source=model.data_source)
        # The inner backend's ``save_model`` is also a template method — we
        # already validated at the outer layer, so skip re-validation here.
        await self._inner.save_model(model, _validate=False)

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
                await _remove_inner_joins_to(
                    model.name, model.data_source, removed, self._inner
                )

    async def delete_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> bool:
        await self._ensure_reconciled()
        if data_source is None:
            identity = await self._inner.resolve_model_identity(name)
            if identity is None:
                return False
            data_source, name = identity
        model = await self._inner.get_model(name, data_source=data_source)
        result = await self._inner.delete_model(name, data_source=data_source)
        if result and model:
            inner_targets = {
                j.target_model for j in model.joins
                if j.join_type == JoinType.INNER and j.target_model != name
            }
            if inner_targets:
                await _remove_inner_joins_to(
                    name, model.data_source, inner_targets, self._inner
                )
        return result

    # -- pure delegation --------------------------------------------------

    async def _list_all_model_identities(self) -> List[Tuple[str, str]]:
        await self._ensure_reconciled()
        return await self._inner._list_all_model_identities()

    async def get_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> Optional[SlayerModel]:
        await self._ensure_reconciled()
        return await self._inner.get_model(name, data_source=data_source)

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        return await self._inner.save_datasource(datasource)

    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]:
        return await self._inner.get_datasource(name)

    async def list_datasources(self) -> List[str]:
        return await self._inner.list_datasources()

    async def delete_datasource(self, name: str) -> bool:
        return await self._inner.delete_datasource(name)

    async def get_datasource_priority(self) -> List[str]:
        return await self._inner.get_datasource_priority()

    async def _set_datasource_priority_raw(self, priority: List[str]) -> None:
        return await self._inner._set_datasource_priority_raw(priority)

    # -- memories (DEV-1357 v2) — pure delegation -------------------------

    async def _save_memory_row(self, memory: Memory) -> None:
        await self._inner._save_memory_row(memory)

    async def _get_memory_row(self, memory_id: int) -> Optional[Memory]:
        return await self._inner._get_memory_row(memory_id)

    async def _list_memories_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[Memory]:
        return await self._inner._list_memories_rows(entities=entities)

    async def _delete_memory_row(self, memory_id: int) -> bool:
        return await self._inner._delete_memory_row(memory_id)

    async def _next_memory_seq(self) -> int:
        return await self._inner._next_memory_seq()
