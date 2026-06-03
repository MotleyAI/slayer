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

import contextlib
import os
from typing import Any, Dict, Iterator, List, Optional, Tuple

import yaml
from pydantic import ValidationError

try:  # POSIX-only; Windows users get the no-op fallback.
    import fcntl as _fcntl
except ImportError:  # pragma: no cover — Windows
    _fcntl = None  # type: ignore[assignment]

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.memories.models import Memory
from slayer.storage.base import (
    StorageBackend,
    _validate_path_component,
    _write_sample_fields,
)
from slayer.storage.sidecar_embedding_store import (
    SidecarEmbeddingsMixin,
    SidecarEmbeddingStore,
)
from slayer.storage.v4_migration import migrate_yaml_layout


_LEGACY_RENAMES = ("embeddings.yaml", "counters.yaml")
_YAML_EXTS = (".yaml", ".yml")


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

    # ---- graph fingerprint -------------------------------------------------

    def graph_fingerprint(self) -> str:
        """Max mtime (as a string) across all YAML files under base_dir.

        Covers memories.yaml, datasource YAMLs, and model YAMLs.  Changes
        whenever any write touches those files, so the graph cache is
        invalidated on every relevant mutation.
        """
        max_mtime = 0.0
        for root, _dirs, files in os.walk(self.base_dir):
            for fname in files:
                if fname.endswith(_YAML_EXTS):
                    try:
                        max_mtime = max(
                            max_mtime,
                            os.path.getmtime(os.path.join(root, fname)),
                        )
                    except OSError:
                        pass
        return str(max_mtime)

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
        sampled_values: Optional[List[str]],
        distinct_count: Optional[int],
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
                _write_sample_fields(
                    col,
                    sampled=sampled,
                    sampled_values=sampled_values,
                    distinct_count=distinct_count,
                )
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

    @staticmethod
    def _is_int_shaped_id(value: Any) -> bool:
        """DEV-1428: pure-digit, no-leading-zero id form. ``"0"`` counts
        but ``"001"`` and ``"42abc"`` do not."""
        if not isinstance(value, str) or not value:
            return False
        if not value.isdigit():
            return False
        if value != "0" and value.startswith("0"):
            return False
        return True

    async def _next_memory_seq(self) -> str:
        """DEV-1428: derive the next int-shaped id from ``memories.yaml``.
        Returns ``str(max(int_shaped_ids) + 1)`` (or ``"1"`` for an
        empty corpus). Non-int-shaped ids (``"001"``, ``"42abc"``,
        user-supplied strings like ``"kb.policy"``) are ignored.
        """
        rows = self._read_yaml_list(self._memories_path)
        max_id = 0
        for r in rows:
            raw = r.get("id")
            # Legacy int rows in pre-DEV-1428 files migrate at validation
            # time; the allocator walk accepts both shapes pre-load.
            if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
                max_id = max(max_id, raw)
            elif isinstance(raw, str) and self._is_int_shaped_id(raw):
                max_id = max(max_id, int(raw))
        return str(max_id + 1)

    def _normalize_legacy_rows(
        self, rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """DEV-1428: dedupe legacy duplicate rows where the same logical
        id exists in both int and str form. Fails loud when their content
        differs."""
        seen: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            raw = row.get("id")
            if isinstance(raw, bool):
                continue
            if isinstance(raw, int):
                key = str(raw)
            elif isinstance(raw, str):
                key = raw
            else:
                continue
            if key not in seen:
                seen[key] = row
                continue
            prior = seen[key]
            if self._rows_content_equal(prior, row):
                # Prefer the legacy int form (per plan) when both shapes
                # carry the same content — the v1→v2 migration normalises
                # it through ``Memory.model_validate`` anyway.
                if isinstance(prior.get("id"), int):
                    continue
                seen[key] = row
                continue
            raise ValueError(
                f"Cannot migrate Memory rows: id {key!r} exists in both "
                f"int and str forms with different content "
                f"(learning={prior.get('learning')!r} vs "
                f"{row.get('learning')!r}). Resolve manually."
            )
        return list(seen.values())

    @staticmethod
    def _rows_content_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        # DEV-1428: "content" excludes ``created_at`` — two legacy rows for
        # the same logical memory may carry different timestamps (e.g. one
        # written on int-id v1, then re-saved as str on v2). The plan's
        # "fail loud if content differs" rule covers the actually-lossy
        # case (different learning / entities / attached query).
        keys = ("learning", "entities", "query")
        return all(a.get(k) == b.get(k) for k in keys)

    async def _save_memory_row(self, memory: Memory) -> None:
        rows = self._read_yaml_list(self._memories_path)
        rows = [r for r in rows if str(r.get("id")) != memory.id]
        rows.append(memory.model_dump(mode="json"))
        self._write_yaml_list(self._memories_path, rows)

    async def _get_memory_row(self, memory_id: str) -> Optional[Memory]:
        rows = self._normalize_legacy_rows(
            self._read_yaml_list(self._memories_path),
        )
        for row in rows:
            if str(row.get("id")) == memory_id:
                return Memory.model_validate(row)
        return None

    async def _list_memories_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[Memory]:
        rows = self._normalize_legacy_rows(
            self._read_yaml_list(self._memories_path),
        )
        memories = [Memory.model_validate(r) for r in rows]
        if entities is None:
            return memories
        wanted = set(entities)
        return [m for m in memories if wanted & set(m.entities)]

    async def _delete_memory_row(self, memory_id: str) -> bool:
        rows = self._read_yaml_list(self._memories_path)
        kept = [r for r in rows if str(r.get("id")) != memory_id]
        if len(kept) == len(rows):
            return False
        self._write_yaml_list(self._memories_path, kept)
        return True

    @contextlib.contextmanager
    def _memories_file_lock(self) -> Iterator[None]:
        """DEV-1428: serialise whole-file memories rewrites for the
        cascade-strip path. Without the lock, two concurrent cascades
        (or a cascade + a user save) can both read the same row list,
        write back partially-overlapping mutations, and lose the
        difference.

        Implementation: an advisory ``flock`` on a sibling
        ``memories.lock`` file (so a race with the YAML reader on the
        live file is impossible). No-op on platforms without ``fcntl``
        — for now that's only Windows, which isn't a supported
        deployment target for the file-based store anyway.
        """
        if _fcntl is None:
            yield
            return
        lock_path = self._memories_path + ".lock"
        # Open in append-binary so the file is created if missing and
        # no truncation happens on subsequent locks.
        with open(lock_path, "ab") as lock_file:
            _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_EX)
            try:
                yield
            finally:
                _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_UN)

    async def strip_dangling_entities_from_memories(
        self, *, canonical_id: str,
    ) -> int:
        # YAML override: take the file-level lock around the entire
        # cascade walk so concurrent cascades / saves can't interleave
        # whole-file rewrites and lose unrelated edits (DEV-1428).
        with self._memories_file_lock():
            return await super().strip_dangling_entities_from_memories(
                canonical_id=canonical_id,
            )

    # Embedding CRUD lives in :class:`SidecarEmbeddingsMixin`, which
    # forwards to ``self._embeddings_store`` set in ``__init__`` above.
    # The mixin owns the SQL once and both backends consume it — see
    # ``slayer/storage/sidecar_embedding_store.py``.
