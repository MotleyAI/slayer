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
from typing import Any
from collections.abc import Iterator

import yaml
from pydantic import ValidationError

try:  # POSIX-only; Windows users get the no-op fallback.
    import fcntl as _fcntl
except ImportError:  # pragma: no cover — Windows
    _fcntl = None  # type: ignore[assignment]

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.memories.models import Memory, _validate_memory_id_charset
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
_YAML_EXTS = (".yaml", ".yml")  # NOSONAR(S1192) — full filenames in _LEGACY_RENAMES are semantically distinct from this extension tuple

_MD_FENCE = "---\n"


# ---- memory <-> .md (DEV-1658) --------------------------------------------


def _memory_to_md(memory: Memory) -> str:
    """Serialise a :class:`Memory` to ``---\\n{frontmatter}---\\n{learning}``.

    ``id`` and ``learning`` are excluded from the frontmatter (``id`` is the
    filename; ``learning`` is the body). None/empty ``description`` /
    ``entities`` / ``query`` are omitted. The learning body is written
    verbatim — no appended/normalized trailing newline — so a read → write
    round-trip is byte-stable (seed skip-if-unchanged depends on this).
    """
    data = memory.model_dump(mode="json")
    learning = data.pop("learning")
    data.pop("id", None)
    if not data.get("description"):
        data.pop("description", None)
    if not data.get("entities"):
        data.pop("entities", None)
    if data.get("query") is None:
        data.pop("query", None)
    fm = yaml.safe_dump(
        data, sort_keys=True, default_flow_style=False, allow_unicode=True,
    )
    return f"{_MD_FENCE}{fm}{_MD_FENCE}{learning}"


def _md_to_memory(memory_id: str, text: str) -> Memory:
    """Inverse of :func:`_memory_to_md`. Splits only the FIRST frontmatter
    block, so a learning body that itself contains a ``---`` line survives.
    ``id`` is injected from the filename (single source of truth)."""
    if text.startswith(_MD_FENCE):
        head, sep, body = text.partition("\n" + _MD_FENCE)
        if sep:
            fm = yaml.safe_load(head[len(_MD_FENCE):])
            data = dict(fm) if isinstance(fm, dict) else {}
            data["id"] = memory_id
            data["learning"] = body
            return Memory.model_validate(data)
    # No frontmatter fence: whole text is the learning body.
    return Memory.model_validate({"id": memory_id, "learning": text})


def _atomic_write_text(path: str, text: str) -> None:
    """Crash-safe write: temp file + ``os.replace`` (atomic on POSIX)."""
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — sync I/O in async by design
        f.write(text)
    os.replace(tmp, path)


def _exact_entry_exists(dir_path: str, entry_name: str) -> bool:
    """True iff ``dir_path`` contains an entry named exactly ``entry_name``.

    ``os.path.exists`` matches any case variant on a case-insensitive
    filesystem, so ``get_model("Orders")`` would silently open
    ``orders.yaml``. Comparing against ``os.listdir`` restores exact-id
    semantics: a case mismatch reads as "not found".
    """
    try:
        return entry_name in os.listdir(dir_path)
    except (FileNotFoundError, NotADirectoryError):
        return False


def _normalize_legacy_memory_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """DEV-1428 legacy dedupe (int/str id duplicates); fails loud on
    divergent content. Used by the one-time ``memories.yaml`` migration."""
    seen: dict[str, dict[str, Any]] = {}
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
        if YAMLStorage._rows_content_equal(prior, row):
            if isinstance(prior.get("id"), int):
                continue
            seen[key] = row
            continue
        raise ValueError(
            f"Cannot migrate Memory rows: id {key!r} exists in both int and "
            f"str forms with different content "
            f"(learning={prior.get('learning')!r} vs "
            f"{row.get('learning')!r}). Resolve manually."
        )
    return list(seen.values())


def migrate_memories_layout(base_dir: str) -> None:
    """DEV-1658: one-time migration of a legacy flat ``memories.yaml`` into
    per-id ``memories/<id>.md`` files, then delete the legacy file.

    Fails loud (raises, legacy file preserved) on invalid YAML, a non-list
    root, or a non-dict row — a corrupt file must never be treated as empty
    and deleted. Crash-safe: every ``.md`` is written before the legacy file
    is removed, so a crash mid-run re-migrates cleanly on the next open.
    """
    legacy = os.path.join(base_dir, "memories.yaml")
    if not os.path.exists(legacy):
        return
    with open(legacy, encoding="utf-8") as f:  # NOSONAR(S7493) — sync I/O in async by design
        raw = yaml.safe_load(f)  # YAMLError propagates → fail loud
    if raw is None:
        rows: list[Any] = []
    elif not isinstance(raw, list):
        raise ValueError(
            f"{legacy}: expected a top-level YAML list of memory rows, got "
            f"{type(raw).__name__}. Refusing to migrate."
        )
    else:
        rows = raw
    for r in rows:
        if not isinstance(r, dict):
            raise ValueError(
                f"{legacy}: every memory row must be a mapping; got "
                f"{type(r).__name__}. Refusing to migrate."
            )
        # Fail loud on a row whose id can't be migrated (bool / None / list /
        # empty string). Without this, ``_normalize_legacy_memory_rows`` would
        # silently drop the row and the legacy file would then be deleted —
        # data loss. Preserve the file for manual repair instead.
        rid = r.get("id")
        if (
            isinstance(rid, bool)
            or not isinstance(rid, (int, str))
            or (isinstance(rid, str) and not rid)
        ):
            raise ValueError(
                f"{legacy}: memory row has a missing or invalid id ({rid!r}). "
                f"Refusing to migrate; fix the row by hand."
            )
    normalized = _normalize_legacy_memory_rows(rows)
    mem_dir = os.path.join(base_dir, "memories")
    # Reject ids that differ only by case BEFORE writing anything: on a
    # case-insensitive filesystem the second ``.md`` write would clobber
    # the first and the legacy file would then be deleted — data loss.
    # Stems already in memories/ participate too, so a crash-resumed run
    # can't clobber a file written by a previous attempt.
    id_by_key: dict[str, str] = {}
    if os.path.isdir(mem_dir):
        for fname in os.listdir(mem_dir):
            if fname.endswith(".md"):
                stem = fname[: -len(".md")]
                id_by_key[stem.casefold()] = stem
    for r in normalized:
        rid = str(r["id"])
        prior = id_by_key.get(rid.casefold())
        if prior is not None and prior != rid:
            raise ValueError(
                f"{legacy}: memory ids {prior!r} and {rid!r} differ only by "
                f"case and would collide as files. Rename one, then reopen."
            )
        id_by_key[rid.casefold()] = rid
    os.makedirs(mem_dir, exist_ok=True)
    for r in normalized:
        mem = Memory.model_validate(r)
        _atomic_write_text(
            os.path.join(mem_dir, f"{mem.id}.md"), _memory_to_md(mem),
        )
    # Guard the removal against a concurrent migrator (two workers opening the
    # same fresh base_dir both run this once): the .md writes are atomic and
    # idempotent, so only the double os.remove would crash. FileNotFoundError
    # here means another process already finished the migration.
    try:
        os.remove(legacy)
    except FileNotFoundError:
        pass


class YAMLStorage(SidecarEmbeddingsMixin, StorageBackend):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.models_dir = os.path.join(base_dir, "models")
        self.datasources_dir = os.path.join(base_dir, "datasources")
        self._priority_path = os.path.join(base_dir, "priority.yaml")
        # DEV-1658: memories are one ``.md`` file per id under ``memories/``.
        # ``_memories_path`` still names the legacy flat file (used only by the
        # one-time migration below to find it).
        self._memories_path = os.path.join(base_dir, "memories.yaml")
        self._memories_dir = os.path.join(base_dir, "memories")
        # Lock file is a SIBLING of memories/ (not inside it, or the ``*.md``
        # glob would trip over it). Reentrant within the process.
        self._memories_lock_path = os.path.join(base_dir, "memories.lock")
        self._mem_lock_fh: Any = None
        self._mem_lock_depth = 0
        os.makedirs(self.models_dir, exist_ok=True)
        os.makedirs(self.datasources_dir, exist_ok=True)
        os.makedirs(self._memories_dir, exist_ok=True)
        # Idempotent — moves any pre-v4 flat files into <data_source>/ subdirs.
        migrate_yaml_layout(base_dir)
        # DEV-1658: one-time migration of a legacy flat ``memories.yaml`` into
        # per-id ``.md`` files. Fails loud on a corrupt/non-list legacy file
        # (never deletes it); a crash mid-migration re-runs cleanly.
        migrate_memories_layout(base_dir)
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

    async def graph_fingerprint(self) -> str:
        """(file_count, max_mtime) across all YAML files under base_dir.

        Including the file count ensures that deleting a YAML file (which
        doesn't change the max mtime of the remaining files) still invalidates
        the graph cache.  OSError propagates to the caller (treated as a
        forced rebuild by ``slayer.search.graph._get_or_rebuild``).
        """
        file_count = 0
        max_mtime = 0.0
        for root, _dirs, files in os.walk(self.base_dir):
            for fname in files:
                # DEV-1658: memories are ``.md`` files now — count them too so
                # a memory create/update/delete invalidates the graph cache.
                if fname.endswith(_YAML_EXTS) or fname.endswith(".md"):
                    max_mtime = max(
                        max_mtime,
                        os.path.getmtime(os.path.join(root, fname)),
                    )
                    file_count += 1
        return f"{file_count}:{max_mtime}"

    # ---- internal helpers --------------------------------------------------

    def _model_path(self, data_source: str, name: str) -> str:
        return os.path.join(self.models_dir, data_source, f"{name}.yaml")

    def _model_entry_exists(self, data_source: str, name: str) -> bool:
        """Exact-case existence check for both path components of a model
        (the datasource directory and the model file)."""
        return _exact_entry_exists(
            self.models_dir, data_source,
        ) and _exact_entry_exists(
            os.path.join(self.models_dir, data_source), f"{name}.yaml",
        )

    # ---- model CRUD --------------------------------------------------------

    async def _save_model_impl(self, model: SlayerModel) -> None:
        target_dir = os.path.join(self.models_dir, model.data_source)
        os.makedirs(target_dir, exist_ok=True)
        path = os.path.join(target_dir, f"{model.name}.yaml")
        data = model.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
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
        data_source: str | None = None,
    ) -> SlayerModel | None:
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return None
        data_source, name = target
        path = self._model_path(data_source, name)  # NOSONAR(S6549) — name/data_source were sanitized by _resolve_target_or_none above (rejects '..', path separators, NULs); SlayerModel Pydantic validators sanitize the save path
        if not self._model_entry_exists(data_source, name):
            return None
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            # e.g. a file truncated mid-write by a full disk.
            raise ValueError(
                f"Model {name!r} in datasource {data_source!r}: invalid YAML in "
                f"{path} — {exc}. Delete the file and re-run `slayer ingest` to "
                f"recreate it."
            ) from exc
        return await self._migrate_and_refine_on_load(
            name=name, data=data, data_source=data_source,
        )

    async def _delete_model_row(
        self, *, data_source: str, name: str,
    ) -> bool:
        # Exact match required: os.remove on a case-insensitive filesystem
        # would otherwise delete a case-variant sibling's file.
        if not self._model_entry_exists(data_source, name):
            return False
        os.remove(self._model_path(data_source, name))
        return True

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
        path = self._model_path(data_source, model_name)
        if not self._model_entry_exists(data_source, model_name):
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
        await self.check_datasource_id_collision(datasource.name)
        path = os.path.join(self.datasources_dir, f"{datasource.name}.yaml")
        data = datasource.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        # DEV-1405: sanitize before composing the filesystem path.
        _validate_path_component(name, kind="datasource name")
        path = os.path.join(self.datasources_dir, f"{name}.yaml")
        if not _exact_entry_exists(self.datasources_dir, f"{name}.yaml"):
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

    async def list_datasources(self) -> list[str]:
        result = []
        for filename in sorted(os.listdir(self.datasources_dir)):
            if filename.endswith((".yaml", ".yml")):
                result.append(filename.rsplit(".", 1)[0])
        return result

    async def _delete_datasource_row(self, name: str) -> bool:
        if not _exact_entry_exists(self.datasources_dir, f"{name}.yaml"):
            return False
        os.remove(os.path.join(self.datasources_dir, f"{name}.yaml"))
        return True

    # ---- datasource priority -----------------------------------------------

    async def get_datasource_priority(self) -> list[str]:
        if not os.path.exists(self._priority_path):
            return []
        with open(self._priority_path) as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            data = yaml.safe_load(f) or {}
        priority = data.get("priority", [])
        if not isinstance(priority, list):
            return []
        return [str(p) for p in priority]

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        with open(self._priority_path, "w") as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            yaml.dump({"priority": list(priority)}, f, sort_keys=False)

    # ---- memories (DEV-1357 v2) -------------------------------------------

    def _read_yaml_list(self, path: str) -> list[dict[str, Any]]:
        if not os.path.exists(path):
            return []
        with open(path) as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            data = yaml.safe_load(f) or []
        if not isinstance(data, list):
            return []
        return [d for d in data if isinstance(d, dict)]

    def _write_yaml_list(self, path: str, rows: list[dict[str, Any]]) -> None:
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

    def _memory_md_path(self, memory_id: str) -> str:
        # Validate the id charset before it becomes a path component. save_memory
        # already validates, but get_memory / delete_memory feed a raw id here —
        # without this an id like "../secret" would escape the memories/ dir
        # (CWE-22). The forbidden set includes "/" and "\\", so a valid id is
        # always a single safe path segment.
        _validate_memory_id_charset(memory_id)
        return os.path.join(self._memories_dir, f"{memory_id}.md")

    def _memory_ids_on_disk(self) -> list[str]:
        """Every id with a ``memories/<id>.md`` file (stems, ``.md`` stripped)."""
        if not os.path.isdir(self._memories_dir):
            return []
        return [
            fname[: -len(".md")]
            for fname in os.listdir(self._memories_dir)
            if fname.endswith(".md")
        ]

    async def _list_memory_ids(self) -> list[str]:
        return self._memory_ids_on_disk()

    async def _next_memory_seq(self) -> str:
        """DEV-1658: next int-shaped id from the ``memories/`` dir stems.
        Non-int stems (``help.intro``, ``kb.policy.42``, ``001``) are ignored.
        Called under the memories lock via the ``save_memory`` override, so
        allocation + write is atomic.
        """
        max_id = 0
        for mid in self._memory_ids_on_disk():
            if self._is_int_shaped_id(mid):
                max_id = max(max_id, int(mid))
        return str(max_id + 1)

    def _normalize_legacy_rows(
        self, rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """DEV-1428 legacy dedupe — now used only by the one-time
        ``memories.yaml`` → per-file migration. Delegates to the module-level
        implementation."""
        return _normalize_legacy_memory_rows(rows)

    @staticmethod
    def _rows_content_equal(a: dict[str, Any], b: dict[str, Any]) -> bool:
        # DEV-1428: "content" excludes ``created_at`` — two legacy rows for
        # the same logical memory may carry different timestamps (e.g. one
        # written on int-id v1, then re-saved as str on v2). The plan's
        # "fail loud if content differs" rule covers the actually-lossy
        # case (different learning / entities / attached query).
        # DEV-1549: ``description`` is part of the persisted content too.
        keys = ("learning", "description", "entities", "query")
        return all(a.get(k) == b.get(k) for k in keys)

    async def save_memory(  # noqa: A002 — mirrors the base signature
        self,
        *,
        learning: str,
        entities: list[str],
        query: Any = None,
        id: str | None = None,  # noqa: A002
        description: str | None = None,
    ) -> Memory:
        # DEV-1658: hold the reentrant memories lock across the whole
        # allocate-and-write transaction. base.save_memory does
        # ``_next_memory_seq()`` then ``_save_memory_row()`` as two steps;
        # locking only the seq call would let two concurrent id=None saves
        # pick the same int id and clobber. base.save_memory never awaits a
        # yielding coroutine, so the lock is not held across an event-loop
        # yield (the reentrant depth counter stays consistent).
        with self._memories_file_lock():
            return await super().save_memory(
                learning=learning, entities=entities, query=query,
                id=id, description=description,
            )

    async def _save_memory_row(self, memory: Memory) -> None:
        with self._memories_file_lock():
            os.makedirs(self._memories_dir, exist_ok=True)
            _atomic_write_text(
                self._memory_md_path(memory.id), _memory_to_md(memory),
            )

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        # Lock-free read: writes are atomic (temp + os.replace), so a reader
        # always sees a complete old-or-new file. A concurrent delete between
        # the check and the open surfaces as FileNotFoundError → treat as
        # "missing" (return None) rather than crash.
        path = self._memory_md_path(memory_id)
        # The .md content carries no id (the filename is the identity), so
        # an exact listdir check is the only way to keep a case-variant
        # lookup from opening the wrong file on a case-insensitive FS.
        if not _exact_entry_exists(self._memories_dir, f"{memory_id}.md"):
            return None
        try:
            with open(path, encoding="utf-8") as f:  # NOSONAR(S7493) — sync I/O in async by design
                return _md_to_memory(memory_id, f.read())
        except FileNotFoundError:
            return None

    async def _list_memories_rows(
        self, *, entities: list[str] | None
    ) -> list[Memory]:
        memories: list[Memory] = []
        for mid in self._memory_ids_on_disk():
            path = self._memory_md_path(mid)
            try:
                with open(path, encoding="utf-8") as f:  # NOSONAR(S7493) — sync I/O in async by design
                    memories.append(_md_to_memory(mid, f.read()))
            except FileNotFoundError:
                # Deleted between listdir and open (lock-free read) — skip.
                continue
        # Deterministic order for the tantivy num_threads=1 doc-id tiebreak
        # and the search "newest" fallback (which re-sorts by recency anyway).
        memories.sort(key=lambda m: (m.created_at, m.id))
        if entities is None:
            return memories
        wanted = set(entities)
        return [m for m in memories if wanted & set(m.entities)]

    async def _delete_memory_row(self, memory_id: str) -> bool:
        with self._memories_file_lock():
            path = self._memory_md_path(memory_id)
            if not _exact_entry_exists(self._memories_dir, f"{memory_id}.md"):
                return False
            os.remove(path)
            return True

    @contextlib.contextmanager
    def _memories_file_lock(self) -> Iterator[None]:
        """DEV-1658: reentrant advisory lock over ALL memory mutations
        (allocate+save, save, delete, cascade-strip). A single ``flock`` on
        ``<base_dir>/memories.lock`` is held on one persistent fd; nested
        acquisitions (e.g. delete → cascade → per-row save) bump a depth
        counter and only the outermost release unlocks. No-op without
        ``fcntl`` (Windows, unsupported for the file store). Safe against the
        depth counter because no mutation holds the lock across an event-loop
        yield.
        """
        if _fcntl is None:
            yield
            return
        if self._mem_lock_depth == 0:
            fh = open(self._memories_lock_path, "ab")
            _fcntl.flock(fh.fileno(), _fcntl.LOCK_EX)
            self._mem_lock_fh = fh
        self._mem_lock_depth += 1
        try:
            yield
        finally:
            self._mem_lock_depth -= 1
            if self._mem_lock_depth == 0:
                _fcntl.flock(self._mem_lock_fh.fileno(), _fcntl.LOCK_UN)
                self._mem_lock_fh.close()
                self._mem_lock_fh = None

    async def strip_dangling_entities_from_memories(
        self, *, canonical_id: str,
    ) -> int:
        # YAML override: hold the reentrant lock across the whole cascade walk
        # so concurrent cascades / saves can't interleave. base.strip only
        # reads + writes memory files (no embedding calls), so the lock is not
        # held across an event-loop yield.
        with self._memories_file_lock():
            return await super().strip_dangling_entities_from_memories(
                canonical_id=canonical_id,
            )

    # Embedding CRUD lives in :class:`SidecarEmbeddingsMixin`, which
    # forwards to ``self._embeddings_store`` set in ``__init__`` above.
    # The mixin owns the SQL once and both backends consume it — see
    # ``slayer/storage/sidecar_embedding_store.py``.
