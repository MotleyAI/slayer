"""A read-only view of a store with some models replaced, for planning against models not yet saved."""

from __future__ import annotations

from collections.abc import Hashable, Iterable

from slayer.core.models import SlayerModel
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures


class ModelOverlayStorage(AccessFilteredStorage):
    """``backend`` read with ``models`` in place of their stored namesakes; it never writes or closes ``backend``."""

    def __init__(self, backend: StorageBackend, *, models: Iterable[SlayerModel]) -> None:
        super().__init__(backend, tags=(), bypass=True)
        self._overlay = {(m.data_source, m.name): m for m in models}

    async def get_model(self, name: str, data_source: str | None = None) -> SlayerModel | None:
        target = await self._resolve_target_or_none(name, data_source=data_source)
        model = self._overlay.get(target) if target is not None else None
        if model is not None:
            return model.model_copy(deep=True)
        return await self._inner.get_model(name, data_source=data_source)

    async def graph_fingerprint(self) -> str | None:
        return None

    async def cache_identity(self) -> Hashable:
        return await StorageBackend.cache_identity(self)

    async def aclose(self) -> None:
        """No-op: the backend outlives the view."""

    async def save_model(
        self, model: SlayerModel, *, _validate: bool = True, failures: DocumentLoadFailures | None = None,
    ) -> None:
        raise PermissionError(f"model overlay is read-only: cannot save {model.name!r}")

    async def _save_model_impl(self, model: SlayerModel) -> None:
        raise PermissionError(f"model overlay is read-only: cannot save {model.name!r}")
