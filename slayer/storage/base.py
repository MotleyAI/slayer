"""Abstract storage protocol and factory."""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from slayer.core.errors import AmbiguousModelError, LearningOrQueryNotFoundError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.learnings.models import Learning, SavedQuery


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

    @abstractmethod
    async def delete_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> bool: ...

    # ---- datasource CRUD ---------------------------------------------------

    @abstractmethod
    async def save_datasource(self, datasource: DatasourceConfig) -> None: ...

    @abstractmethod
    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]: ...

    @abstractmethod
    async def list_datasources(self) -> List[str]: ...

    @abstractmethod
    async def delete_datasource(self, name: str) -> bool: ...

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

    # ---- learnings + saved queries (DEV-1357) -----------------------------
    #
    # ID format (``L<int>``/``Q<int>``), monotonic non-reuse, and the
    # missing-row → ``LearningOrQueryNotFoundError`` policy live on this
    # class so they can never diverge between backends. Backends only
    # implement the row-shaped CRUD primitives + the seq counters below.

    @abstractmethod
    async def _save_learning_row(self, learning: Learning) -> None:
        """Persist a fully-populated ``Learning`` (id and created_at set)."""

    @abstractmethod
    async def _get_learning_row(self, learning_id: str) -> Optional[Learning]:
        """Read a ``Learning`` by id; return ``None`` when not present."""

    @abstractmethod
    async def _list_learnings_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[Learning]:
        """Return every ``Learning`` whose stored entity set has non-empty
        intersection with ``entities``. ``entities=None`` returns all rows.
        ``entities=[]`` returns ``[]`` (intersection with the empty set is
        empty)."""

    @abstractmethod
    async def _delete_learning_row(self, learning_id: str) -> bool:
        """Delete by id; return ``True`` if a row was removed, ``False``
        when the id did not exist."""

    @abstractmethod
    async def _next_learning_seq(self) -> int:
        """Atomically allocate and return the next learning sequence
        integer. Counter is monotonically increasing — deleted ids are
        never reused."""

    @abstractmethod
    async def _save_saved_query_row(self, saved: SavedQuery) -> None: ...

    @abstractmethod
    async def _get_saved_query_row(
        self, query_id: str
    ) -> Optional[SavedQuery]: ...

    @abstractmethod
    async def _list_saved_queries_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[SavedQuery]: ...

    @abstractmethod
    async def _delete_saved_query_row(self, query_id: str) -> bool: ...

    @abstractmethod
    async def _next_saved_query_seq(self) -> int: ...

    async def save_learning(
        self, *, body: str, entities: List[str]
    ) -> Learning:
        """Allocate a new ``L<int>`` id, persist the learning, return it."""
        seq = await self._next_learning_seq()
        learning = Learning(
            id=f"L{seq}",
            body=body,
            entities=list(entities),
        )
        await self._save_learning_row(learning)
        return learning

    async def get_learning(self, learning_id: str) -> Learning:
        row = await self._get_learning_row(learning_id)
        if row is None:
            raise LearningOrQueryNotFoundError(learning_id)
        return row

    async def list_learnings(
        self, *, entities: Optional[List[str]] = None
    ) -> List[Learning]:
        return await self._list_learnings_rows(entities=entities)

    async def delete_learning(self, learning_id: str) -> None:
        if not await self._delete_learning_row(learning_id):
            raise LearningOrQueryNotFoundError(learning_id)

    async def save_saved_query(
        self,
        *,
        query: SlayerQuery,
        description: str,
        entities: List[str],
    ) -> SavedQuery:
        """Allocate a new ``Q<int>`` id, persist the saved query, return it."""
        seq = await self._next_saved_query_seq()
        saved = SavedQuery(
            id=f"Q{seq}",
            description=description,
            query=query,
            entities=list(entities),
        )
        await self._save_saved_query_row(saved)
        return saved

    async def get_saved_query(self, query_id: str) -> SavedQuery:
        row = await self._get_saved_query_row(query_id)
        if row is None:
            raise LearningOrQueryNotFoundError(query_id)
        return row

    async def list_saved_queries(
        self, *, entities: Optional[List[str]] = None
    ) -> List[SavedQuery]:
        return await self._list_saved_queries_rows(entities=entities)

    async def delete_saved_query(self, query_id: str) -> None:
        if not await self._delete_saved_query_row(query_id):
            raise LearningOrQueryNotFoundError(query_id)


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
