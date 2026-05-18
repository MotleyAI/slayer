"""YAML-based storage for models and datasources.

v4 (DEV-1330): models live under ``<base_dir>/models/<data_source>/<name>.yaml``
so two datasources sharing a table name don't collide. The datasource priority
list — used to disambiguate bare-name lookups — is stored at
``<base_dir>/priority.yaml``.

On open, ``migrate_yaml_layout`` walks the legacy flat layout and moves each
file into the new subdirectory. See ``slayer/storage/v4_migration.py`` for the
contract details.

DEV-1405: embedding rows now live in a SQLite sidecar at
``<base_dir>/embeddings.db`` (via :class:`SidecarEmbeddingStore`) instead of
a single ``embeddings.yaml`` whose whole-file-rewrite-on-save bottlenecked
``slayer ingest``. Any pre-DEV-1405 ``embeddings.yaml`` is silently renamed
to ``embeddings.yaml.legacy`` on first open; re-run ``slayer ingest`` (or
rely on ``--ingest-on-startup``) to repopulate ``embeddings.db``. Memory ids
are now derived from ``memories.yaml`` itself (``last_row.id + 1``), so the
companion ``counters.yaml`` file is no longer used; it is similarly renamed
to ``counters.yaml.legacy`` if present. Both renames are idempotent: if a
``.legacy`` file already exists at upgrade time, both files are left alone.
"""

import os
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pydantic import ValidationError

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend, _validate_path_component
from slayer.storage.sidecar_embedding_store import (
    SidecarEmbeddingsMixin,
    SidecarEmbeddingStore,
)
from slayer.storage.v4_migration import migrate_yaml_layout


_LEGACY_RENAMES = ("embeddings.yaml", "counters.yaml")


class YAMLStorage(SidecarEmbeddingsMixin, StorageBackend):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.models_dir = os.path.join(base_dir, "models")
        self.datasources_dir = os.path.join(base_dir, "datasources")
        self._priority_path = os.path.join(base_dir, "priority.yaml")
        self._memories_path = os.path.join(base_dir, "memories.yaml")
        os.makedirs(self.models_dir, exist_ok=True)
        os.makedirs(self.datasources_dir, exist_ok=True)
        # Idempotent — moves any pre-v4 flat files into <data_source>/ subdirs.
        migrate_yaml_layout(base_dir)
        # Idempotent — rename pre-DEV-1405 sidecar files out of the way.
        # If a ``.legacy`` companion already exists (user upgraded twice or
        # manually restored), leave both files in place so we never clobber
        # an existing backup.
        for filename in _LEGACY_RENAMES:
            current = os.path.join(base_dir, filename)
            legacy = os.path.join(base_dir, filename + ".legacy")
            if os.path.exists(current) and not os.path.exists(legacy):
                os.rename(current, legacy)
        self._embeddings_store = SidecarEmbeddingStore(
            db_path=os.path.join(base_dir, "embeddings.db"),
        )

    # ---- internal helpers --------------------------------------------------

    def _model_path(self, data_source: str, name: str) -> str:
        return os.path.join(self.models_dir, data_source, f"{name}.yaml")

    # ---- model CRUD --------------------------------------------------------

    async def _save_model_impl(self, model: SlayerModel) -> None:
        target_dir = os.path.join(self.models_dir, model.data_source)
        os.makedirs(target_dir, exist_ok=True)
        path = os.path.join(target_dir, f"{model.name}.yaml")
        data = model.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def _list_all_model_identities(self) -> List[Tuple[str, str]]:
        result: List[Tuple[str, str]] = []
        if not os.path.isdir(self.models_dir):
            return result
        for ds in sorted(os.listdir(self.models_dir)):
            ds_dir = os.path.join(self.models_dir, ds)
            if not os.path.isdir(ds_dir):
                continue
            for filename in sorted(os.listdir(ds_dir)):
                if filename.endswith((".yaml", ".yml")):
                    result.append((ds, filename.rsplit(".", 1)[0]))
        return result

    async def get_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> Optional[SlayerModel]:
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return None
        data_source, name = target
        path = self._model_path(data_source, name)
        if not os.path.exists(path):  # NOSONAR(S6549) — name/data_source were sanitized by _resolve_target_or_none above (rejects '..', path separators, NULs); SlayerModel Pydantic validators sanitize the save path
            return None
        with open(path) as f:
            data = yaml.safe_load(f)
        return await self._migrate_and_refine_on_load(
            name=name, data=data, data_source=data_source,
        )

    async def _delete_model_row(
        self, *, data_source: str, name: str,
    ) -> bool:
        path = self._model_path(data_source, name)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    async def update_column_sampled(
        self,
        *,
        data_source: str,
        model_name: str,
        column_name: str,
        sampled: Optional[str],
    ) -> None:
        path = self._model_path(data_source, model_name)
        if not os.path.exists(path):
            raise ValueError(
                f"update_column_sampled: model {model_name!r} in datasource "
                f"{data_source!r} not found."
            )
        with open(path) as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design
            data = yaml.safe_load(f) or {}
        cols = data.get("columns") or []
        for col in cols:
            if isinstance(col, dict) and col.get("name") == column_name:
                if sampled is None:
                    col.pop("sampled", None)
                else:
                    col["sampled"] = sampled
                break
        else:
            raise ValueError(
                f"update_column_sampled: column {column_name!r} not found "
                f"on model {model_name!r} in datasource {data_source!r}."
            )
        with open(path, "w") as f:  # NOSONAR(S7493)
            yaml.dump(data, f, sort_keys=False)

    # ---- datasource CRUD ---------------------------------------------------

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        path = os.path.join(self.datasources_dir, f"{datasource.name}.yaml")
        data = datasource.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]:
        # DEV-1405: sanitize before composing the filesystem path.
        _validate_path_component(name, kind="datasource name")
        path = os.path.join(self.datasources_dir, f"{name}.yaml")
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            ds = DatasourceConfig.model_validate(data)
            return ds.resolve_env_vars()
        except yaml.YAMLError as exc:
            raise ValueError(
                f"Datasource '{name}': invalid YAML in {path} — {exc}"
            ) from exc
        except ValidationError as exc:
            raise ValueError(
                f"Datasource '{name}': invalid config — {exc}"
            ) from exc

    async def list_datasources(self) -> List[str]:
        result = []
        for filename in sorted(os.listdir(self.datasources_dir)):
            if filename.endswith((".yaml", ".yml")):
                result.append(filename.rsplit(".", 1)[0])
        return result

    async def _delete_datasource_row(self, name: str) -> bool:
        path = os.path.join(self.datasources_dir, f"{name}.yaml")
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    # ---- datasource priority -----------------------------------------------

    async def get_datasource_priority(self) -> List[str]:
        if not os.path.exists(self._priority_path):
            return []
        with open(self._priority_path) as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            data = yaml.safe_load(f) or {}
        priority = data.get("priority", [])
        if not isinstance(priority, list):
            return []
        return [str(p) for p in priority]

    async def _set_datasource_priority_raw(self, priority: List[str]) -> None:
        with open(self._priority_path, "w") as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            yaml.dump({"priority": list(priority)}, f, sort_keys=False)

    # ---- memories (DEV-1357 v2) -------------------------------------------

    def _read_yaml_list(self, path: str) -> List[Dict[str, Any]]:
        if not os.path.exists(path):
            return []
        with open(path) as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            data = yaml.safe_load(f) or []
        if not isinstance(data, list):
            return []
        return [d for d in data if isinstance(d, dict)]

    def _write_yaml_list(self, path: str, rows: List[Dict[str, Any]]) -> None:
        with open(path, "w") as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            yaml.dump(rows, f, sort_keys=False)

    async def _next_memory_seq(self) -> int:
        """DEV-1405: derive the next id straight from ``memories.yaml``.
        Returns ``max(int_ids) + 1`` over the current rows (or ``1`` for
        an empty file). Ids of deleted memories may be reused — there is
        no separate counter file.

        Note: the SQLite backend reaches the same invariant via
        ``SELECT MAX(id) + 1``; we don't rely on ``rows[-1]`` here because
        ``_save_memory_row``'s filter-and-append upsert pattern (and any
        hand-edit of the file) can leave the tail row out of id order.
        """
        rows = self._read_yaml_list(self._memories_path)
        max_id = max(
            (r["id"] for r in rows if isinstance(r.get("id"), int)),
            default=0,
        )
        return max_id + 1

    async def _save_memory_row(self, memory: Memory) -> None:
        rows = self._read_yaml_list(self._memories_path)
        rows = [r for r in rows if r.get("id") != memory.id]
        rows.append(memory.model_dump(mode="json"))
        self._write_yaml_list(self._memories_path, rows)

    async def _get_memory_row(self, memory_id: int) -> Optional[Memory]:
        for row in self._read_yaml_list(self._memories_path):
            if row.get("id") == memory_id:
                return Memory.model_validate(row)
        return None

    async def _list_memories_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[Memory]:
        rows = [
            Memory.model_validate(r)
            for r in self._read_yaml_list(self._memories_path)
        ]
        if entities is None:
            return rows
        wanted = set(entities)
        return [r for r in rows if wanted & set(r.entities)]

    async def _delete_memory_row(self, memory_id: int) -> bool:
        rows = self._read_yaml_list(self._memories_path)
        kept = [r for r in rows if r.get("id") != memory_id]
        if len(kept) == len(rows):
            return False
        self._write_yaml_list(self._memories_path, kept)
        return True

    # Embedding CRUD lives in :class:`SidecarEmbeddingsMixin`, which
    # forwards to ``self._embeddings_store`` set in ``__init__`` above.
    # The mixin owns the SQL once and both backends consume it — see
    # ``slayer/storage/sidecar_embedding_store.py``.
