"""Abstract storage protocol and factory."""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from slayer.core.errors import AmbiguousModelError, MemoryNotFoundError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage import migrations as _mig
from slayer.storage.type_refinement import (
    has_refineable_columns,
    refine_dict_with_live_schema,
)


def storage_base_dir(path: str) -> str:
    """Return the on-disk directory associated with a storage path.

    For a SQLite file (``foo.db``/``.sqlite``/``.sqlite3``), returns its parent
    directory; otherwise the path is itself a directory. Used by callers that
    need to colocate auxiliary files (demo databases, etc.) next to the storage.
    """
    if path.endswith((".db", ".sqlite", ".sqlite3")):
        return os.path.dirname(path) or "."
    return path


def default_storage_path() -> str:
    """Return the platform-appropriate default storage directory.

    Resolution order:
    1. $SLAYER_STORAGE environment variable (if set)
    2. $SLAYER_MODELS_DIR environment variable (legacy, if set)
    3. Platform default:
       - Linux: $XDG_DATA_HOME/slayer (defaults to ~/.local/share/slayer)
       - macOS: ~/Library/Application Support/slayer
       - Windows: %LOCALAPPDATA%/slayer
    """
    env = os.environ.get("SLAYER_STORAGE") or os.environ.get("SLAYER_MODELS_DIR")
    if env:
        return env

    if os.name == "nt":
        # Windows
        base = Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    else:
        # MacOS, Linux, etc.
        base = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

    return str(base / "slayer")


_PATH_COMPONENT_DISALLOWED = ("/", "\\", "\x00")


def _validate_path_component(value: str, *, kind: str) -> None:
    """Reject strings that could traverse out of the storage tree.

    Used at the public ``get_model``/``delete_model`` boundaries to
    sanitize user-controlled strings *before* a backend composes them
    into a filesystem path or SQL key. Mirrors the validators on the
    ``SlayerModel`` Pydantic class — those guard the save path; this
    guards the read/delete paths where Pydantic validation is bypassed
    (since callers pass raw strings, not model instances).

    Rejects: empty / whitespace-only, ``..``, any path separator
    (``/``, ``\\``), and embedded NULs. Lives in ``StorageBackend`` so
    every backend gets the same defense without duplication
    (per the backend-agnostic memory rule).
    """
    if not isinstance(value, str) or not value or not value.strip():
        raise ValueError(
            f"Invalid {kind} {value!r}: must be a non-empty string."
        )
    if value.strip() != value:
        raise ValueError(
            f"Invalid {kind} {value!r}: leading/trailing whitespace is not allowed."
        )
    if value == ".." or value.startswith("..") or "/.." in value or "\\.." in value:
        raise ValueError(
            f"Invalid {kind} {value!r}: path traversal sequences are not allowed."
        )
    for ch in _PATH_COMPONENT_DISALLOWED:
        if ch in value:
            raise ValueError(
                f"Invalid {kind} {value!r}: must not contain {ch!r}."
            )


class StorageBackend(ABC):
    """Abstract storage backend. All methods are async.

    Implementations with sync I/O (YAML files, SQLite) simply use
    ``async def`` with synchronous code inside — this is fine for
    fast local I/O. Implementations with true async I/O (e.g., asyncpg
    for Postgres) can ``await`` as needed.

    v4 (DEV-1330) keys models by ``(data_source, name)`` instead of bare
    ``name``. Concrete backends implement the lower-level CRUD against the
    composite key; this class provides a generic ``resolve_model_identity``
    helper so bare-name lookups fall back to the priority list consistently
    across backends.
    """

    # ---- model CRUD (composite key) ----------------------------------------

    @abstractmethod
    async def save_model(self, model: SlayerModel) -> None: ...

    @abstractmethod
    async def _list_all_model_identities(self) -> List[Tuple[str, str]]:
        """Return every saved ``(data_source, name)`` pair.

        Backends override this with whatever is cheapest (filesystem walk,
        SQL ``SELECT``). The bare-name resolver and ``list_models`` build on
        it.
        """

    @abstractmethod
    async def get_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> Optional[SlayerModel]: ...

    async def delete_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> bool:
        """Delete one model by ``(data_source, name)`` and cascade-delete
        every embedding row tagged with that model's canonical prefix.

        Bare ``name`` resolves through the priority list (see
        ``_resolve_target_or_none``). Returns ``False`` when no model matches
        — no cascade is attempted in that case.
        """
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return False
        resolved_data_source, resolved_name = target
        deleted = await self._delete_model_row(
            data_source=resolved_data_source, name=resolved_name,
        )
        if deleted:
            await self.delete_embeddings_for_canonical(
                canonical_id_prefix=f"{resolved_data_source}.{resolved_name}",
            )
        return deleted

    @abstractmethod
    async def _delete_model_row(
        self, *, data_source: str, name: str,
    ) -> bool:
        """Delete the persisted row for ``(data_source, name)``. Returns
        ``True`` if a row was removed, ``False`` when the identity did not
        exist. Embedding cascade is handled by the public ``delete_model``
        wrapper on the ABC; backends only do the row I/O here."""

    @abstractmethod
    async def update_column_sampled(
        self,
        *,
        data_source: str,
        model_name: str,
        column_name: str,
        sampled: Optional[str],
    ) -> None:
        """Patch a single column's ``sampled`` field in-place (DEV-1375).

        Avoids a full ``save_model`` per refresh — read-modify-write the
        single field, leave every other field untouched. Raises
        ``ValueError`` when the model or column doesn't exist.
        """

    # ---- shared model lookup / load helpers --------------------------------

    async def _resolve_target_or_none(
        self,
        name: str,
        *,
        data_source: Optional[str],
    ) -> Optional[Tuple[str, str]]:
        """Sanitize inputs and resolve a bare ``name`` to its
        ``(data_source, name)`` identity via the priority list.

        Returns ``None`` when ``data_source`` was omitted and no model with
        that bare name exists in storage. Both ``get_model`` and
        ``delete_model`` consume this; backends only need to handle the
        case where the resolved record was deleted out from under them
        between the lookup and the I/O.
        """
        _validate_path_component(name, kind="model name")
        if data_source is not None:
            _validate_path_component(data_source, kind="data_source")
            return (data_source, name)
        identity = await self.resolve_model_identity(name)
        if identity is None:
            return None
        return identity

    async def _migrate_and_refine_on_load(
        self,
        *,
        name: str,
        data: Any,
        data_source: str,
    ) -> SlayerModel:
        """DEV-1361 storage-driven type refinement, shared across backends.

        When the on-disk model dict is below the current ``SlayerModel`` version,
        run the migrator chain to bring it forward, then introspect the live
        datasource and refine ``DOUBLE → INT`` for base columns whose live SQL
        type is integer. Validates the resulting dict into a ``SlayerModel``
        and persists it back via ``save_model`` when a migration ran, so
        subsequent loads short-circuit on the version check.

        Hard-fails with ``ValueError`` when a migration ran, the dict has
        refineable DOUBLE base columns, and the named datasource entry is
        missing — silently skipping refinement and persisting the v5 dict
        would leave base integer columns stuck at ``DOUBLE`` forever. Models
        with no refineable columns (text-only, query-backed, sql-mode, or
        already-narrowed) load without needing a live datasource.
        """
        write_back = False
        if isinstance(data, dict):
            pre_version = int(data.get("version", 1))
            if pre_version < _mig.CURRENT_VERSIONS["SlayerModel"]:
                data = _mig.migrate("SlayerModel", data)
                write_back = True
                if has_refineable_columns(data):
                    ds = await self.get_datasource(data_source)
                    if ds is None:
                        raise ValueError(
                            f"Cannot migrate model {name!r}: datasource "
                            f"{data_source!r} is unavailable for type "
                            f"refinement. Restore the datasource entry or "
                            f"remove the stale model file."
                        )
                    refine_dict_with_live_schema(data, ds)
        model = SlayerModel.model_validate(data)
        if write_back:
            await self.save_model(model)
        return model

    # ---- datasource CRUD ---------------------------------------------------

    @abstractmethod
    async def save_datasource(self, datasource: DatasourceConfig) -> None: ...

    @abstractmethod
    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]: ...

    @abstractmethod
    async def list_datasources(self) -> List[str]: ...

    async def delete_datasource(self, name: str) -> bool:
        """Delete the datasource config and cascade-delete every embedding
        row tagged with the datasource's canonical prefix (the datasource
        doc itself, plus every model / column / measure / aggregation
        embedding under it).

        Models that lived in the deleted datasource are *not* themselves
        deleted by this call (matches pre-DEV-1386 behaviour); they become
        orphans referencing a missing datasource config. Re-creating the
        datasource and re-running ``slayer ingest`` repopulates embeddings.
        """
        deleted = await self._delete_datasource_row(name)
        if deleted:
            await self.delete_embeddings_for_canonical(
                canonical_id_prefix=name,
            )
        return deleted

    @abstractmethod
    async def _delete_datasource_row(self, name: str) -> bool:
        """Delete the datasource config row. Returns ``True`` when a row
        was removed. Embedding cascade is handled by the public
        ``delete_datasource`` wrapper on the ABC."""

    # ---- datasource priority (bare-name disambiguation) -------------------

    @abstractmethod
    async def get_datasource_priority(self) -> List[str]:
        """Return the configured priority order (most-preferred first).

        Empty list = no priority configured; bare-name lookups raise
        ``AmbiguousModelError`` whenever a name appears in ≥2 datasources.
        """

    @abstractmethod
    async def _set_datasource_priority_raw(self, priority: List[str]) -> None:
        """Persist the priority list verbatim. Validation happens in the
        public ``set_datasource_priority`` wrapper below."""

    async def set_datasource_priority(self, priority: List[str]) -> None:
        """Validate and persist the datasource priority list.

        Each entry must already exist as a saved ``DatasourceConfig``;
        unknown names raise ``ValueError``. Pass ``[]`` to clear the
        priority.
        """
        if priority:
            known = set(await self.list_datasources())
            unknown = [p for p in priority if p not in known]
            if unknown:
                raise ValueError(
                    f"set_datasource_priority: unknown datasource(s) "
                    f"{sorted(unknown)}; known datasources: {sorted(known) or '[]'}."
                )
        await self._set_datasource_priority_raw(list(priority))

    # ---- list_models with auto-detect or required arg ----------------------

    async def list_models(self, data_source: Optional[str] = None) -> List[str]:
        """List model names within a single datasource.

        Resolution rules:

        * ``data_source`` supplied → return models stored under that
          ``data_source`` (possibly empty). The name is accepted as long as
          it appears in either a registered ``DatasourceConfig`` *or* in any
          saved model's ``data_source`` field — that keeps ``list_models``
          consistent with ``get_model``, which can already retrieve models
          stored without a corresponding config (e.g. an orphan after the
          datasource entry was deleted, or a model imported from another
          environment). Unknown names — neither registered nor referenced by
          any saved model — still raise ``ValueError`` so typos surface.
        * ``data_source`` is ``None`` and ≥1 model exists in exactly one
          datasource → return that datasource's model names.
        * ``data_source`` is ``None`` and storage is empty → return ``[]``.
        * ``data_source`` is ``None`` and ≥2 datasources hold models → raise
          ``ValueError`` listing them.
        """
        identities = await self._list_all_model_identities()
        if data_source is not None:
            known = set(await self.list_datasources())
            existing_sources = {ds for ds, _ in identities}
            if data_source not in known and data_source not in existing_sources:
                raise ValueError(
                    f"list_models: unknown data_source {data_source!r}; "
                    f"known datasources: {sorted(known | existing_sources) or '[]'}."
                )
            return sorted(name for ds, name in identities if ds == data_source)
        distinct_sources = sorted({ds for ds, _ in identities})
        if not distinct_sources:
            return []
        if len(distinct_sources) == 1:
            return sorted(name for _, name in identities)
        raise ValueError(
            f"list_models: models exist in multiple datasources "
            f"{distinct_sources}; supply data_source=... to pick one."
        )

    # ---- bare-name resolver (priority-aware) ------------------------------

    async def resolve_model_identity(
        self,
        name: str,
        *,
        prefer_data_source: Optional[str] = None,
    ) -> Optional[Tuple[str, str]]:
        """Resolve a bare model name to a ``(data_source, name)`` tuple.

        * No matches → ``None``.
        * One match → return it.
        * Multiple matches:
            - If ``prefer_data_source`` is in the candidates, return that.
            - Else walk ``get_datasource_priority()`` and return the first
              listed datasource that has the name.
            - Else raise ``AmbiguousModelError``.

        ``prefer_data_source`` is the resolution hint used internally for
        join targets (the parent model's ``data_source``); explicit caller
        kwargs should be passed through ``get_model(name, data_source=...)``
        instead of this helper.
        """
        identities = await self._list_all_model_identities()
        candidates = [ds for ds, n in identities if n == name]
        if not candidates:
            return None
        if len(candidates) == 1:
            return (candidates[0], name)
        if prefer_data_source is not None and prefer_data_source in candidates:
            return (prefer_data_source, name)
        priority = await self.get_datasource_priority()
        for ds in priority:
            if ds in candidates:
                return (ds, name)
        raise AmbiguousModelError(name=name, candidates=candidates)

    # ---- memories (DEV-1357 v2) -------------------------------------------
    #
    # Monotonic positive-int ids, no reuse on delete, and the missing-row
    # → ``MemoryNotFoundError`` policy live on this class so they can
    # never diverge between backends. Backends only implement the row-
    # shaped CRUD primitives + the seq counter below.

    @abstractmethod
    async def _save_memory_row(self, memory: Memory) -> None:
        """Persist a fully-populated ``Memory`` (id and created_at set)."""

    @abstractmethod
    async def _get_memory_row(self, memory_id: int) -> Optional[Memory]:
        """Read a ``Memory`` by id; return ``None`` when not present."""

    @abstractmethod
    async def _list_memories_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[Memory]:
        """Return every ``Memory`` whose stored entity set has non-empty
        intersection with ``entities``. ``entities=None`` returns all rows.
        ``entities=[]`` returns ``[]`` (intersection with the empty set is
        empty)."""

    @abstractmethod
    async def _delete_memory_row(self, memory_id: int) -> bool:
        """Delete by id; return ``True`` if a row was removed, ``False``
        when the id did not exist."""

    @abstractmethod
    async def _next_memory_seq(self) -> int:
        """Atomically allocate and return the next memory sequence
        integer. Counter is monotonically increasing — deleted ids are
        never reused."""

    async def save_memory(
        self,
        *,
        learning: str,
        entities: List[str],
        query: Optional[SlayerQuery] = None,
    ) -> Memory:
        """Allocate the next id, persist the memory, return it."""
        seq = await self._next_memory_seq()
        memory = Memory(
            id=seq,
            learning=learning,
            entities=list(entities),
            query=query,
        )
        await self._save_memory_row(memory)
        return memory

    async def get_memory(self, memory_id: int) -> Memory:
        row = await self._get_memory_row(memory_id)
        if row is None:
            raise MemoryNotFoundError(memory_id)
        return row

    async def list_memories(
        self, *, entities: Optional[List[str]] = None
    ) -> List[Memory]:
        return await self._list_memories_rows(entities=entities)

    async def delete_memory(self, memory_id: int) -> None:
        if not await self._delete_memory_row(memory_id):
            raise MemoryNotFoundError(memory_id)
        # Cascade: drop any embedding rows tagged with this memory's
        # canonical id so an orphan embedding never survives its source.
        await self.delete_embeddings_for_canonical(
            canonical_id_prefix=f"memory:{memory_id}",
        )

    # ---- embeddings sidecar (DEV-1386) ------------------------------------
    #
    # One row per ``(canonical_id, embedding_model_name)`` pair. The active
    # ``embedding_model_name`` (from ``SLAYER_EMBEDDING_MODEL``) selects
    # which rows the search service actually reads — changing the env var
    # leaves prior rows in place but inert.

    @abstractmethod
    async def save_embedding(self, row: Embedding) -> None:
        """Upsert one embedding row keyed by
        ``(canonical_id, embedding_model_name)``."""

    @abstractmethod
    async def get_embedding(
        self, *, canonical_id: str, embedding_model_name: str,
    ) -> Optional[Embedding]:
        """Fetch one embedding row; ``None`` when no row matches."""

    @abstractmethod
    async def list_embeddings(
        self, *, embedding_model_name: str,
    ) -> List[Embedding]:
        """Return every row for ``embedding_model_name``. Used by the
        search service to load the entire corpus into a numpy matrix."""

    @abstractmethod
    async def delete_embeddings_for_canonical(
        self, *, canonical_id_prefix: str,
    ) -> int:
        """Cascade-delete embedding rows whose ``canonical_id`` starts with
        ``canonical_id_prefix``. Returns the row-count deleted.

        Used by ``delete_model`` (prefix ``"<ds>.<model>"`` cascades to
        child column / measure / aggregation embeddings), ``delete_memory``
        (prefix ``"memory:<id>"`` — exact match for one row), and
        ``delete_datasource`` (prefix ``"<ds>"``).
        """


# ---------------------------------------------------------------------------
# Storage factory with pluggable registry
# ---------------------------------------------------------------------------

_STORAGE_REGISTRY: Dict[str, Callable[[str], StorageBackend]] = {}


def register_storage(scheme: str, factory: Callable[[str], StorageBackend]) -> None:
    """Register a storage backend factory for a URI scheme.

    Example:
        register_storage("redis", lambda path: RedisStorage(url=path))
    """
    _STORAGE_REGISTRY[scheme.lower().strip()] = factory


def resolve_storage(path: str) -> StorageBackend:
    """Create a StorageBackend from a path or URI.

    Resolution order:
    1. URI scheme (e.g., "sqlite:///data.db", "yaml://./dir") → registered factory
    2. File extension .db/.sqlite/.sqlite3 → SQLiteStorage
    3. Everything else → YAMLStorage (directory)

    Third-party backends can register via register_storage().
    """
    # Check for URI scheme
    if "://" in path:
        scheme, _, remainder = path.partition("://")
        scheme = scheme.lower()
        if scheme in _STORAGE_REGISTRY:
            return _wrap_join_sync(_STORAGE_REGISTRY[scheme](remainder))
        # Built-in schemes
        if scheme == "yaml":
            from slayer.storage.yaml_storage import YAMLStorage

            return _wrap_join_sync(YAMLStorage(base_dir=remainder))
        if scheme == "sqlite":
            from slayer.storage.sqlite_storage import SQLiteStorage

            # sqlite:///abs/path → remainder="/abs/path" (keep absolute)
            # sqlite://rel/path → remainder="rel/path" (keep relative)
            db_path = remainder if remainder.startswith("/") else remainder.lstrip("/")
            return _wrap_join_sync(SQLiteStorage(db_path=db_path))
        raise ValueError(
            f"Unknown storage scheme '{scheme}'. "
            f"Built-in: yaml, sqlite. "
            f"Registered: {', '.join(_STORAGE_REGISTRY) or 'none'}. "
            f"Use register_storage() to add custom backends."
        )

    # Extension-based detection
    if path.endswith((".db", ".sqlite", ".sqlite3")):
        from slayer.storage.sqlite_storage import SQLiteStorage

        return _wrap_join_sync(SQLiteStorage(db_path=path))

    # Default: YAML directory
    from slayer.storage.yaml_storage import YAMLStorage

    return _wrap_join_sync(YAMLStorage(base_dir=path))


def _wrap_join_sync(storage: StorageBackend) -> StorageBackend:
    """Wrap a storage backend with automatic inner-join synchronization."""
    from slayer.storage.join_sync import JoinSyncStorage

    return JoinSyncStorage(inner=storage)
