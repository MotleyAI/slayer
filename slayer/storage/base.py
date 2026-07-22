"""Abstract storage protocol and factory."""

import asyncio
import os
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from collections.abc import Callable, Iterable

from slayer.core.errors import (
    AmbiguousModelError,
    IdCollisionError,
    MemoryNotFoundError,
)
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.memories.models import (
    MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX,
    Memory,
    _validate_memory_id_charset,
)
from slayer.storage import migrations as _mig
from slayer.storage.type_refinement import (
    has_refineable_columns,
    has_sqlite_widenable_columns,
    refine_dict_with_live_schema,
)


def _write_sample_fields(
    col: dict[str, Any],
    *,
    sampled: str | None,
    sampled_values: list[str] | None,
    distinct_count: int | None,
) -> None:
    """Apply the DEV-1375 + DEV-1480 sample-field write convention to a
    column dict in place: ``None`` pops the corresponding key, non-None
    writes it.

    Lives on the ABC module so every backend's ``update_column_sampled``
    implementation can route through the same write logic (see
    ``feedback_backend_agnostic.md``).
    """
    if sampled is None:
        col.pop("sampled", None)
    else:
        col["sampled"] = sampled
    if sampled_values is None:
        col.pop("sampled_values", None)
    else:
        col["sampled_values"] = sampled_values
    if distinct_count is None:
        col.pop("distinct_count", None)
    else:
        col["distinct_count"] = distinct_count


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
       - macOS: ~/Library/Application Support/slayer (ignores $XDG_DATA_HOME)
       - Windows: %LOCALAPPDATA%/slayer
    """
    env = os.environ.get("SLAYER_STORAGE") or os.environ.get("SLAYER_MODELS_DIR")
    if env:
        return env

    if os.name == "nt":
        # Windows
        base = Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        # macOS
        base = Path.home() / "Library" / "Application Support"
    else:
        # Linux, etc.
        base = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

    return str(base / "slayer")


_PATH_COMPONENT_DISALLOWED = ("/", "\\", "\x00", ".")


def _entity_matches_cascade(
    *, entry: str, canonical_id: str, is_memory_ref: bool,
) -> bool:
    """Match predicate for ``strip_dangling_entities_from_memories``.

    Split per the plan: ``memory:<id>`` refs are exact-match only;
    ``<ds>[.<model>[.<leaf>]]`` refs match exactly OR as strict
    dotted-path descendants.
    """
    if is_memory_ref:
        return entry == canonical_id
    if entry == canonical_id:
        return True
    return entry.startswith(f"{canonical_id}.")


def _fs_equivalence_key(value: str) -> str:
    """Key under which two ids collide on a case-insensitive filesystem."""
    return value.casefold()


def _find_case_colliding_id(
    candidate: str, existing: Iterable[str],
) -> str | None:
    """Return an existing id that casefold-equals ``candidate`` but is
    spelled differently, or ``None``. An exact match never counts
    (upserts); a collider is reported even alongside an exact match so a
    legacy store holding both spellings surfaces the pair.
    """
    key = _fs_equivalence_key(candidate)
    for entry in existing:
        if entry != candidate and _fs_equivalence_key(entry) == key:
            return entry
    return None


def _validate_path_component(value: str, *, kind: str) -> None:
    """Reject strings that could traverse out of the storage tree or
    collide with canonical-id namespace boundaries.

    Used at the public ``get_model``/``delete_model`` boundaries to
    sanitize user-controlled strings *before* a backend composes them
    into a filesystem path or SQL key. Mirrors the validators on the
    ``SlayerModel`` and ``DatasourceConfig`` Pydantic classes — those
    guard the save path; this guards the read/delete paths where Pydantic
    validation is bypassed (since callers pass raw strings, not model
    instances).

    Rejects: empty / whitespace-only, ``..``, any path separator
    (``/``, ``\\``), embedded NULs, and ``.`` (DEV-1405: dots are the
    canonical-id namespace delimiter — allowing ``prod.db`` as a
    datasource name would let ``delete_datasource('prod')`` cascade-nuke
    embeddings rooted at ``prod.db.*``). Lives in ``StorageBackend`` so
    every backend gets the same defense without duplication (per the
    backend-agnostic memory rule).
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

    #: Backends whose ids become filenames (YAML) set this to True: saves
    #: then reject ids differing only by case, which would alias to the
    #: same file on case-insensitive filesystems. Wrappers copy it from
    #: the inner backend.
    _ids_collide_as_filenames = False

    # ---- model CRUD (composite key) ----------------------------------------

    async def save_model(
        self, model: SlayerModel, *, _validate: bool = True,
    ) -> None:
        """Persist a model.

        Runs save-time validation (case-collision rejection for
        filename-backed stores, then derived-column cycle detection) and
        delegates to the backend-specific :meth:`_save_model_impl`. The
        ``_validate=False`` escape hatch is for trusted internal callers —
        currently only the migration write-back in
        :meth:`_migrate_and_refine_on_load` — that must persist legacy
        data which may not pass current invariants.

        Validation rules live in this base class so every backend gets
        them uniformly without duplication; concrete backends must NOT
        override this method.
        """
        if _validate:
            if self._ids_collide_as_filenames:
                await self._check_model_identity_collision(model)
            from slayer.engine.column_dependency import validate_no_column_cycles
            await validate_no_column_cycles(model=model, storage=self)
        await self._save_model_impl(model)

    async def _check_model_identity_collision(self, model: SlayerModel) -> None:
        """Reject a model whose ``data_source`` or ``name`` differs only
        by case from an existing one — both are filename components in
        the YAML backend."""
        identities = await self._list_all_model_identities()
        known_ds = {ds for ds, _ in identities}
        known_ds.update(await self.list_datasources())
        collide = _find_case_colliding_id(model.data_source, known_ds)
        if collide is not None:
            raise IdCollisionError(
                kind="datasource", new_id=model.data_source, existing_id=collide,
            )
        names_in_ds = [n for ds, n in identities if ds == model.data_source]
        collide = _find_case_colliding_id(model.name, names_in_ds)
        if collide is not None:
            raise IdCollisionError(
                kind="model",
                new_id=model.name,
                existing_id=collide,
                data_source=model.data_source,
            )

    @abstractmethod
    async def _save_model_impl(self, model: SlayerModel) -> None:
        """Backend-specific write of ``model`` to durable storage.

        Concrete backends implement only this method, not ``save_model``.
        Shared validation lives in :meth:`save_model` (the template
        method).
        """

    @abstractmethod
    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        """Return every saved ``(data_source, name)`` pair.

        Backends override this with whatever is cheapest (filesystem walk,
        SQL ``SELECT``). The bare-name resolver and ``list_models`` build on
        it.
        """

    @abstractmethod
    async def get_model(
        self,
        name: str,
        data_source: str | None = None,
    ) -> SlayerModel | None: ...

    async def delete_model(
        self,
        name: str,
        data_source: str | None = None,
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
            canonical = f"{resolved_data_source}.{resolved_name}"
            await self.delete_embeddings_for_canonical(
                canonical_id_prefix=canonical,
            )
            await self.strip_dangling_entities_from_memories(
                canonical_id=canonical,
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
        sampled: str | None,
        sampled_values: list[str] | None,
        distinct_count: int | None,
    ) -> None:
        """Patch a single column's sample-value fields in-place (DEV-1375 +
        DEV-1480).

        Writes ``sampled``, ``sampled_values``, and ``distinct_count`` as a
        single read-modify-write so the three stay consistent with each
        other. ``None`` for any field drops the corresponding key from the
        persisted dict; non-None writes it. Other column fields are
        untouched.

        Raises ``ValueError`` when the model or column doesn't exist.
        """

    # ---- shared model lookup / load helpers --------------------------------

    async def _resolve_target_or_none(
        self,
        name: str,
        *,
        data_source: str | None,
    ) -> tuple[str, str] | None:
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

    async def _apply_refinement_or_raise(
        self, *, name: str, data: dict, data_source: str,
    ) -> None:
        """Inner gate of :meth:`_migrate_and_refine_on_load`: decide whether
        live introspection is needed for ``data`` and dispatch to it.

        Hard-fails (``ValueError``) when the dict has DOUBLE base columns
        and the datasource is missing. SQLite-INT widening with missing DS
        is best-effort — logs a warning and skips. No-op when neither
        predicate fires.
        """
        needs_double = has_refineable_columns(data)
        needs_sqlite_int = has_sqlite_widenable_columns(data)
        if not (needs_double or needs_sqlite_int):
            return
        ds = await self.get_datasource(data_source)
        if ds is not None:
            refine_dict_with_live_schema(data, ds)
            return
        if needs_double:
            raise ValueError(
                f"Cannot migrate model {name!r}: datasource "
                f"{data_source!r} is unavailable for type "
                f"refinement. Restore the datasource entry or "
                f"remove the stale model file."
            )
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "Datasource %r unavailable; skipping SQLite "
            "affinity probe for INT base columns on %r. "
            "Re-run `slayer ingest` once the datasource is "
            "back to widen any mis-typed columns.",
            data_source,
            name,
        )

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
        would leave base integer columns stuck at ``DOUBLE`` forever.
        DEV-1538 SQLite-INT widening with missing DS is best-effort: logs
        a warning and skips. Models with no refineable or widenable
        columns load without needing a live datasource.
        """
        if not isinstance(data, dict):
            # e.g. a zero-byte YAML file (yaml.safe_load -> None) left behind
            # by a full disk or interrupted write. Fail with the remediation
            # instead of a bare Pydantic model_type error.
            raise ValueError(
                f"Model {name!r} in datasource {data_source!r} has an empty or "
                f"corrupt stored definition (got {type(data).__name__} instead "
                f"of a mapping). Delete the stored entry (YAML layout: "
                f"models/{data_source}/{name}.yaml) and re-run `slayer ingest` "
                f"to recreate it."
            )
        write_back = False
        pre_version = int(data.get("version", 1))
        if pre_version < _mig.CURRENT_VERSIONS["SlayerModel"]:
            data = _mig.migrate("SlayerModel", data)
            write_back = True
            await self._apply_refinement_or_raise(
                name=name, data=data, data_source=data_source,
            )
        model = SlayerModel.model_validate(data)
        if write_back:
            # DEV-1410: legacy on-disk models may contain derived-column
            # cycles that current save-time validation would reject. The
            # migration write-back must not re-validate; otherwise users
            # could not load a broken legacy model to repair it.
            await self.save_model(model, _validate=False)
        return model

    # ---- datasource CRUD ---------------------------------------------------

    @abstractmethod
    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        """Persist a datasource config (upsert by exact name).
        Filename-backed implementations should call
        :meth:`check_datasource_id_collision` before writing."""

    async def check_datasource_id_collision(self, name: str) -> None:
        """Raise :class:`IdCollisionError` when ``name`` differs only by
        case from an existing datasource name or a saved model's
        ``data_source``. Public so backends can call it from
        ``save_datasource``."""
        existing = set(await self.list_datasources())
        existing.update(ds for ds, _ in await self._list_all_model_identities())
        collide = _find_case_colliding_id(name, existing)
        if collide is not None:
            raise IdCollisionError(
                kind="datasource", new_id=name, existing_id=collide,
            )

    @abstractmethod
    async def get_datasource(self, name: str) -> DatasourceConfig | None: ...

    @abstractmethod
    async def list_datasources(self) -> list[str]: ...

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
        # DEV-1405: sanitize the raw name before it composes a filesystem
        # path (YAMLStorage) or a cascade LIKE prefix. Mirrors the
        # validation done on the save side by ``DatasourceConfig.name``.
        _validate_path_component(name, kind="datasource name")
        deleted = await self._delete_datasource_row(name)
        if deleted:
            await self.delete_embeddings_for_canonical(
                canonical_id_prefix=name,
            )
            await self.strip_dangling_entities_from_memories(
                canonical_id=name,
            )
        return deleted

    @abstractmethod
    async def _delete_datasource_row(self, name: str) -> bool:
        """Delete the datasource config row. Returns ``True`` when a row
        was removed. Embedding cascade is handled by the public
        ``delete_datasource`` wrapper on the ABC."""

    # ---- datasource priority (bare-name disambiguation) -------------------

    @abstractmethod
    async def get_datasource_priority(self) -> list[str]:
        """Return the configured priority order (most-preferred first).

        Empty list = no priority configured; bare-name lookups raise
        ``AmbiguousModelError`` whenever a name appears in ≥2 datasources.
        """

    @abstractmethod
    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        """Persist the priority list verbatim. Validation happens in the
        public ``set_datasource_priority`` wrapper below."""

    async def set_datasource_priority(self, priority: list[str]) -> None:
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

    async def list_models(self, data_source: str | None = None) -> list[str]:
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
        prefer_data_source: str | None = None,
    ) -> tuple[str, str] | None:
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

    # ---- memories (DEV-1357 v2 / DEV-1428) -------------------------------
    #
    # DEV-1428: memory ids are non-empty strings. Auto-allocation walks
    # ``max(int-shaped id) + 1`` over the existing corpus where
    # "int-shaped" means pure-digit, no-leading-zero. User-supplied ids
    # share the namespace and may collide with prior rows → upsert
    # unconditionally (``created_at`` of the original row preserved).
    # Cascade-on-delete in ``delete_memory`` strips any embedding rows
    # under the freed id AND drops the corresponding ``memory:<id>`` ref
    # from every other memory's ``entities`` list (defense layer 1).

    @abstractmethod
    async def _save_memory_row(self, memory: Memory) -> None:
        """Persist a fully-populated ``Memory`` (id and created_at set).
        Backends upsert by id; ``created_at`` of the existing row must
        be preserved when present."""

    @abstractmethod
    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        """Read a ``Memory`` by id; return ``None`` when not present."""

    async def get_memory_row(self, memory_id: str) -> Memory | None:
        """Non-raising existence check / fetch. Public so the resolver
        and the ingest-time cleanup pass can probe without catching
        ``MemoryNotFoundError``."""
        return await self._get_memory_row(memory_id)

    @abstractmethod
    async def _list_memories_rows(
        self, *, entities: list[str] | None
    ) -> list[Memory]:
        """Return every ``Memory`` whose stored entity set has non-empty
        intersection with ``entities``. ``entities=None`` returns all rows.
        ``entities=[]`` returns ``[]`` (intersection with the empty set is
        empty)."""

    @abstractmethod
    async def _delete_memory_row(self, memory_id: str) -> bool:
        """Delete by id; return ``True`` if a row was removed, ``False``
        when the id did not exist."""

    @abstractmethod
    async def _next_memory_seq(self) -> str:
        """Return the next int-shaped memory id (as a string), strictly
        above any int-shaped id currently held by the corpus. Pure-digit,
        no-leading-zero ids count toward the max walk; ``"42abc"`` and
        ``"001"`` are ignored. Empty corpus → ``"1"``."""

    async def save_memory(
        self,
        *,
        learning: str,
        entities: list[str],
        query: SlayerQuery | None = None,
        id: str | None = None,  # noqa: A002 — public kwarg matching MCP / REST
        description: str | None = None,
    ) -> Memory:
        """Persist a memory.

        * ``id=None`` → allocator picks the next int-shaped id (``str``).
        * ``id="some-string"`` → user-supplied; rejected on bad charset
          or empty. Duplicate id → unconditional upsert; ``created_at``
          of the original row is preserved. On filename-backed (YAML)
          storage an id differing only by case from an existing one
          raises :class:`IdCollisionError`.

        DEV-1549: ``description`` is an optional compact preview shown
        by ``search(compact=True)`` and ``inspect_model(compact=True)``.
        Length is hard-capped on the ``Memory`` model.
        """
        if id is not None:
            _validate_memory_id_charset(id)
            if self._ids_collide_as_filenames:
                ids = [m.id for m in await self._list_memories_rows(entities=None)]
                collide = _find_case_colliding_id(id, ids)
                if collide is not None:
                    raise IdCollisionError(
                        kind="memory", new_id=id, existing_id=collide,
                    )
            existing = await self._get_memory_row(id)
            assigned_id = id
            preserved_created_at = (
                existing.created_at if existing is not None else None
            )
        else:
            assigned_id = await self._next_memory_seq()
            preserved_created_at = None
        kwargs: dict[str, Any] = {
            "id": assigned_id,
            "learning": learning,
            "description": description,
            "entities": list(entities),
            "query": query,
        }
        if preserved_created_at is not None:
            kwargs["created_at"] = preserved_created_at
        memory = Memory(**kwargs)
        await self._save_memory_row(memory)
        return memory

    async def get_memory(self, memory_id: str) -> Memory:
        row = await self._get_memory_row(memory_id)
        if row is None:
            raise MemoryNotFoundError(memory_id)
        return row

    async def list_memories(
        self, *, entities: list[str] | None = None
    ) -> list[Memory]:
        return await self._list_memories_rows(entities=entities)

    async def delete_memory(self, memory_id: str) -> None:
        if not await self._delete_memory_row(memory_id):
            raise MemoryNotFoundError(memory_id)
        # Cascade: drop any embedding rows tagged with this memory's
        # canonical id so an orphan embedding never survives its source.
        await self.delete_embeddings_for_canonical(
            canonical_id_prefix=f"{_MEMORY_PREFIX}{memory_id}",
        )
        # DEV-1428 cascade-strip: drop ``memory:<id>`` refs from every
        # other memory's ``entities`` list.
        await self.strip_dangling_entities_from_memories(
            canonical_id=f"{_MEMORY_PREFIX}{memory_id}",
        )

    async def strip_dangling_entities_from_memories(
        self, *, canonical_id: str,
    ) -> int:
        """DEV-1428 defense layer 1: remove ``canonical_id`` from every
        memory's ``entities`` list.

        Match predicate is split by ref kind:

        * ``memory:<id>`` → exact-match only (memory ids are opaque
          strings after the prefix; ``memory:42`` must not strip
          ``memory:421`` or ``memory:42.y``).
        * ``<ds>[.<model>[.<leaf>]]`` → exact-match OR strict dotted
          descendant (per the same rule used by
          ``delete_embeddings_for_canonical``).

        Per-row read-modify-write: re-fetches each candidate row, drops
        matching entries, and writes back via ``_save_memory_row``
        directly — does NOT route through ``MemoryService.save_memory``.
        With memory embeddings rendered from ``learning`` alone (no tags)
        the content hash never changes, so no embedding refresh fires.

        Returns the number of memories rewritten.
        """
        if not canonical_id:
            return 0
        is_memory_ref = canonical_id.startswith(_MEMORY_PREFIX)
        memories = await self._list_memories_rows(entities=None)
        rewritten = 0
        for memory in memories:
            if not self._memory_has_cascade_candidate(
                memory=memory,
                canonical_id=canonical_id,
                is_memory_ref=is_memory_ref,
            ):
                continue
            if await self._rewrite_memory_dropping_entity(
                memory_id=memory.id,
                canonical_id=canonical_id,
                is_memory_ref=is_memory_ref,
            ):
                rewritten += 1
        return rewritten

    @staticmethod
    def _memory_has_cascade_candidate(
        *, memory: Memory, canonical_id: str, is_memory_ref: bool,
    ) -> bool:
        """Cheap snapshot check — does ``memory.entities`` contain any
        entry the cascade would strip? Used to skip the read-modify-write
        round-trip for memories the cascade can't touch."""
        if not memory.entities:
            return False
        return any(
            _entity_matches_cascade(
                entry=e,
                canonical_id=canonical_id,
                is_memory_ref=is_memory_ref,
            )
            for e in memory.entities
        )

    async def _rewrite_memory_dropping_entity(
        self, *, memory_id: str, canonical_id: str, is_memory_ref: bool,
    ) -> bool:
        """Re-fetch the row, drop matching entities, write back via
        ``_save_memory_row``. Returns ``True`` if a write happened.

        Re-fetch is what makes the cascade safe under concurrent saves:
        the snapshot check above is racy against a write that lands
        between the read and the cascade rewrite, but the per-row
        write here always operates on the freshest stored state.
        """
        fresh = await self._get_memory_row(memory_id)
        if fresh is None:
            # Concurrent delete won the race; nothing to write.
            return False
        fresh_kept = [
            e for e in fresh.entities
            if not _entity_matches_cascade(
                entry=e,
                canonical_id=canonical_id,
                is_memory_ref=is_memory_ref,
            )
        ]
        if fresh_kept == fresh.entities:
            return False
        await self._save_memory_row(
            fresh.model_copy(update={"entities": fresh_kept})
        )
        return True

    # ---- graph fingerprint (DEV-1464) -------------------------------------

    async def graph_fingerprint(self) -> str:
        """Return a string that changes whenever storage content changes.

        Used by ``slayer.search.graph`` to decide whether to rebuild the
        ephemeral in-memory LadybugDB property graph.  The default
        implementation returns ``"0"``; concrete backends override this
        to provide a meaningful fingerprint (e.g. the db file's mtime for
        SQLiteStorage, or the max mtime across all YAML files for
        YAMLStorage).

        Implementations may raise ``OSError`` when the underlying files
        are inaccessible; callers treat that as a forced rebuild.
        """
        await asyncio.sleep(0)
        return "0"

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
    ) -> Embedding | None:
        """Fetch one embedding row; ``None`` when no row matches."""

    @abstractmethod
    async def list_embeddings(
        self, *, embedding_model_name: str,
    ) -> list[Embedding]:
        """Return every row for ``embedding_model_name``. Used by the
        search service to load the entire corpus into a numpy matrix."""

    @abstractmethod
    async def delete_embeddings_for_canonical(
        self, *, canonical_id_prefix: str,
    ) -> int:
        """Cascade-delete embedding rows whose ``canonical_id`` is exactly
        ``canonical_id_prefix`` or is a strict descendant under the
        dotted-path namespace (``canonical_id_prefix + "." + …``). Never
        a character prefix — ``"orders"`` does not match
        ``"orders_archive"``; ``"memory:4"`` does not match ``"memory:42"``.
        Returns the row-count deleted.

        Used by ``delete_model`` (root ``"<ds>.<model>"`` matches the
        model doc and every column / measure / aggregation under it),
        ``delete_memory`` (root ``"memory:<id>"`` — exact match for one
        row, no descendants), and ``delete_datasource`` (root ``"<ds>"``
        — the datasource doc plus every descendant).
        """

    # Batched read/write helpers (DEV-1405). Default implementations call
    # the single-row methods M times so any third-party backend keeps
    # working unchanged; the bundled backends override these to issue one
    # round-trip via :class:`SidecarEmbeddingStore`.

    async def save_embeddings(self, rows: list[Embedding]) -> None:
        """Persist many embedding rows in one round-trip. Default
        implementation calls :meth:`save_embedding` for each row."""
        for row in rows:
            await self.save_embedding(row)

    async def get_embeddings_for_canonical_ids(
        self,
        *,
        canonical_ids: list[str],
        embedding_model_name: str,
    ) -> dict[str, "Embedding"]:
        """Fetch every embedding row in ``canonical_ids`` under the given
        ``embedding_model_name`` in one round-trip. Returns a dict keyed
        by ``canonical_id``; missing ids are simply absent from the dict.
        Default implementation calls :meth:`get_embedding` for each id."""
        out: dict[str, Embedding] = {}
        for canonical_id in canonical_ids:
            row = await self.get_embedding(
                canonical_id=canonical_id,
                embedding_model_name=embedding_model_name,
            )
            if row is not None:
                out[canonical_id] = row
        return out


# ---------------------------------------------------------------------------
# Storage factory with pluggable registry
# ---------------------------------------------------------------------------

_STORAGE_REGISTRY: dict[str, Callable[[str], StorageBackend]] = {}


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
