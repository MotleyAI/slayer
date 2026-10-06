"""Abstract storage protocol and factory."""

import asyncio
import logging
import os
import sys
import warnings
from abc import ABC, abstractmethod
from contextvars import ContextVar
from pathlib import Path
from typing import Any
from collections.abc import Callable, Iterable

from slayer.core.enums import DataType, JoinCardinality, TimeGranularity, invert_cardinality
from slayer.core.granularity import resolve_granularity, unknown_granularity_message
from slayer.core.time_spine import TIME_SPINE_MODEL, spine_model
from slayer.core.errors import (
    AmbiguousModelError,
    IdCollisionError,
    MemoryNotFoundError,
    DefaultTimeDimensionTypeError,
    ReservedModelNameError,
    StoredDocumentLoadError,
    UnknownGranularityError,
)
from slayer.engine.column_dependency import validate_derived_columns
from slayer.core.join_walker import edges_between
from slayer.core.models import (
    DatasourceConfig,
    SlayerModel,
    _validate_column_name,
    is_base_column_sql,
    physical_column_sql,
)
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.memories.models import (
    MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX,
    Memory,
    _validate_memory_id_charset,
)
from slayer.storage import migrations as _mig
from slayer.storage.document_loading import DocumentLoadFailures, Loaded, stored_document_boundary
from slayer.storage.legacy_time_literals import legacy_literal_sites, repaired_filter
from slayer.storage.legacy_alias_rewrite import (
    apply_dunder_rewrite_to_model_dict,
    extract_dunder_chains,
    mode_a_surface_texts,
)
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.sql_template import check_aggregation_definition
from slayer.storage.type_refinement import (
    has_refineable_columns,
    has_sqlite_widenable_columns,
    refine_dict_with_live_schema,
)



_TO_ONE_CARDINALITIES = {"many_to_one", "one_to_one"}
# v8 is the first version written only by ingest that refined both DOUBLE and SQLite INT
# columns; older documents are refined against the live schema on load.
_LIVE_REFINEMENT_BELOW_VERSION = 8
_MODEL_LOAD_CONCURRENCY = 8
_MEMORY_FILTER_REPAIR_BELOW_VERSION = 4
# Raw document reads of the enclosing ``load_models`` pass, by ``(backend, data_source, name)``.
_RAW_PASS: ContextVar[dict[tuple[object, str, str], asyncio.Future[dict | None]] | None] = ContextVar(
    "_RAW_PASS", default=None)


def _is_exact_inverse_join(a: dict, b: dict) -> bool:
    """True iff join dicts on opposite models mirror each other: swapped pair set, same join type, equal ``name``, and inversion-consistent cardinalities (unset counts as consistent)."""
    if (a.get("name") or None) != (b.get("name") or None):
        return False
    try:
        a_pairs = {(str(x), str(y)) for x, y in a.get("join_pairs") or []}
        b_pairs = {(str(y), str(x)) for x, y in b.get("join_pairs") or []}
    except (TypeError, ValueError):
        return False
    if not a_pairs or a_pairs != b_pairs:
        return False
    if str(a.get("join_type") or "left") != str(b.get("join_type") or "left"):
        return False
    ca, cb = a.get("cardinality"), b.get("cardinality")
    if ca is None or cb is None:
        return True
    try:
        return invert_cardinality(JoinCardinality(ca)) == JoinCardinality(cb)
    except ValueError:
        return False


def _inverse_survivor(
    *, model_a: str, join_a: dict, model_b: str, join_b: dict,
) -> str:
    """Which model keeps its half of an exact-inverse pair: to-one side, else cardinality-carrying side, else lexicographic — a pure function of both halves, so load orders agree."""
    a_card, b_card = join_a.get("cardinality"), join_b.get("cardinality")
    a_to_one = a_card in _TO_ONE_CARDINALITIES
    b_to_one = b_card in _TO_ONE_CARDINALITIES
    if a_to_one != b_to_one:
        return model_a if a_to_one else model_b
    if (a_card is None) != (b_card is None):
        return model_a if a_card is not None else model_b
    return min((model_a, model_b), (model_b, model_a))[0]


def _canonical_key(*, key: Any, columns: Any) -> Any:
    """``key`` unless it names no declared column but is exactly one base column's physical rename."""
    if not isinstance(key, str) or not isinstance(columns, list):
        return key
    cols = [c for c in columns if isinstance(c, dict) and isinstance(c.get("name"), str)]
    if any(c["name"] == key for c in cols):
        return key
    renames = [
        c["name"] for c in cols
        if isinstance(c.get("sql"), str) and is_base_column_sql(c["sql"])
        and physical_column_sql(sql=c["sql"], name=c["name"]) == key
    ]
    return renames[0] if len(renames) == 1 else key


def canonical_join_pairs(*, pairs: Any, source_columns: Any, target_columns: Any) -> Any:
    """Raw ``join_pairs`` with stored physical spellings rewritten to ``Column.name``."""
    if not isinstance(pairs, list):
        return pairs
    return [
        [_canonical_key(key=p[0], columns=source_columns), _canonical_key(key=p[1], columns=target_columns)]
        if isinstance(p, list) and len(p) == 2 else p
        for p in pairs
    ]


def _undeclared_key(*, key: Any, columns: list) -> bool:
    """A valid column name that no column of ``columns`` declares or spells physically."""
    if not isinstance(key, str):
        return False
    try:
        _validate_column_name(key, "join key")
    except ValueError:
        return False
    return not any(
        isinstance(c, dict) and isinstance(c.get("name"), str) and (
            c["name"] == key
            or (is_base_column_sql(c.get("sql")) and physical_column_sql(sql=c.get("sql"), name=c["name"]) == key)
        )
        for c in columns
    )


def _stored_type(*, key: Any, columns: Any) -> str | None:
    """The stored ``type`` of the column named ``key`` when it is a valid ``DataType``."""
    if not isinstance(columns, list):
        return None
    col = next((c for c in columns if isinstance(c, dict) and c.get("name") == key), None)
    try:
        return DataType(col["type"]).value if col is not None and "type" in col else None
    except ValueError:
        return None


def _key_pairs(join: Any) -> list[tuple[Any, Any]]:
    pairs = join.get("join_pairs") if isinstance(join, dict) else None
    return [(p[0], p[1]) for p in pairs if isinstance(p, list) and len(p) == 2] if isinstance(pairs, list) else []


def _incoming_keys(*, sibling: dict, target: str, target_columns: list) -> list[tuple[Any, str | None]]:
    """``target``-side keys of ``sibling``'s joins into ``target``, each typed like its source key."""
    joins = sibling.get("joins")
    out: list[tuple[Any, str | None]] = []
    for join in joins if isinstance(joins, list) else []:
        if not isinstance(join, dict) or join.get("target_model") != target:
            continue
        canonical = {**join, "join_pairs": canonical_join_pairs(
            pairs=join.get("join_pairs"), source_columns=sibling.get("columns"), target_columns=target_columns,
        )}
        out.extend((tgt, _stored_type(key=src, columns=sibling.get("columns"))) for src, tgt in _key_pairs(canonical))
    return out


def _stored_counterpart(*, join: dict, name: str, peer: dict | None, columns: Any):
    """The exact-inverse of ``join`` in ``peer``'s raw joins (canonicalised against ``columns``, this document's), or ``None``."""
    peer_joins = peer.get("joins") if isinstance(peer, dict) else None
    if not isinstance(peer, dict) or not isinstance(peer_joins, list):
        return None
    return next(
        (
            j for j in peer_joins
            if isinstance(j, dict) and j.get("target_model") == name
            and _is_exact_inverse_join(join, {**j, "join_pairs": canonical_join_pairs(
                pairs=j.get("join_pairs"), source_columns=peer.get("columns"),
                target_columns=columns,
            )})
        ),
        None,
    )


def _checked_join_names(model: SlayerModel) -> list[str]:
    """Names of ``model``'s named edges; raises on duplicates."""
    names = [j.name for j in model.joins if j.name]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise ValueError(
            f"Model '{model.name}': duplicate join names {dupes}. Each "
            f"edge name incident to a model must be unique."
        )
    return names


def _checked_join_name_namespace(
    *, model: SlayerModel, names: list[str],
    identities: Iterable[tuple[str, str]],
) -> set[str]:
    """Datasource model-name namespace; raises when an edge name collides."""
    ds_model_names = {
        n for ds, n in identities if ds == model.data_source
    } | {model.name}
    for n in names:
        if n in ds_model_names:
            raise ValueError(
                f"Model '{model.name}': join name '{n}' collides with "
                f"model '{n}' in datasource '{model.data_source}'. Edge "
                f"names and model names share the path-segment namespace."
            )
    return ds_model_names


def _check_edges_against_peers(
    *, model: SlayerModel, peers: dict[str, SlayerModel],
) -> None:
    """Reject edge-name reuse across incident edges and exact-inverse twins."""
    incident_to_self = {
        j.name for p in peers.values() for j in p.joins
        if j.target_model == model.name and j.name
    }
    for join in model.joins:
        target = peers.get(join.target_model)
        if join.name:
            _check_edge_name_free(
                model=model, join=join, target=target, peers=peers,
                incident_to_self=incident_to_self,
            )
        if target is not None:
            _check_not_exact_inverse(model=model, join=join, target=target)


def _check_edge_name_free(
    *, model: SlayerModel, join, target: SlayerModel | None,
    peers: dict[str, SlayerModel], incident_to_self: set,
) -> None:
    target_incident = set(incident_to_self)
    if target is not None:
        target_incident |= {j2.name for j2 in target.joins if j2.name}
        target_incident |= {
            j3.name for p in peers.values() for j3 in p.joins
            if j3.target_model == join.target_model and j3.name
        }
    if join.name in target_incident:
        raise ValueError(
            f"Model '{model.name}': join name '{join.name}' is "
            f"already used by another edge incident to "
            f"'{model.name}' or '{join.target_model}'."
        )


def _check_not_exact_inverse(
    *, model: SlayerModel, join, target: SlayerModel,
) -> None:
    raw = join.model_dump(mode="json")
    for j2 in target.joins:
        if j2.target_model != model.name:
            continue
        if _is_exact_inverse_join(raw, j2.model_dump(mode="json")):
            raise ValueError(
                f"Model '{model.name}': join to '{target.name}' is "
                f"the exact inverse of the edge already declared on "
                f"'{target.name}' — reverse traversal is automatic; "
                f"remove this declaration."
            )


def _warn_unnamed_parallel_edges(
    *, model: SlayerModel, peers: dict[str, SlayerModel],
) -> None:
    for peer_name in {j.target_model for j in model.joins}:
        target = peers.get(peer_name)
        if target is None:
            continue
        edges = edges_between(source=model, target=target)
        unnamed = [e for e in edges if e.name is None]
        # An unnamed edge in a parallel set is unaddressable (bare token ambiguous).
        if len(edges) >= 2 and unnamed:
            warnings.warn(
                f"Model '{model.name}': {len(edges)} parallel edges connect "
                f"'{model.name}' and '{peer_name}' and {len(unnamed)} of "
                f"them are unnamed — the bare path token is ambiguous and "
                f"an unnamed edge has no token of its own. Name the edges "
                f"to make them addressable.",
                UserWarning,
                stacklevel=2,
            )


def _write_sample_fields(
    col: dict[str, Any],
    *,
    sampled: str | None,
    sampled_values: list[str] | None,
    distinct_count: int | None,
) -> None:
    """Write ``sampled``/``sampled_values``/``distinct_count`` into a column dict in place (``None`` pops the key, non-None writes it); shared by every backend's ``update_column_sampled``."""
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
    """The on-disk directory for a storage path: a SQLite file's parent, else the path itself (callers colocate auxiliary files beside storage)."""
    if path.endswith((".db", ".sqlite", ".sqlite3")):
        return os.path.dirname(path) or "."
    return path


def default_storage_path() -> str:
    """Default storage dir: $SLAYER_STORAGE, else legacy $SLAYER_MODELS_DIR, else the platform data dir (XDG on Linux, Application Support on macOS, LOCALAPPDATA on Windows)."""
    env = os.environ.get("SLAYER_STORAGE") or os.environ.get("SLAYER_MODELS_DIR")
    if env:
        return env

    if os.name == "nt":
        base = Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

    return str(base / "slayer")


_PATH_COMPONENT_DISALLOWED = ("/", "\\", "\x00", ".")


def _entity_matches_cascade(
    *, entry: str, canonical_id: str, is_memory_ref: bool,
) -> bool:
    """Cascade match: ``memory:<id>`` refs exact-only; ``<ds>[.<model>[.<leaf>]]`` refs exact or strict dotted descendant."""
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
    """An existing id that casefold-equals ``candidate`` but is spelled differently, else ``None`` (exact matches are upserts, never collisions)."""
    key = _fs_equivalence_key(candidate)
    for entry in existing:
        if entry != candidate and _fs_equivalence_key(entry) == key:
            return entry
    return None


def _validate_path_component(value: str, *, kind: str) -> None:
    """Reject a user-supplied path component that could traverse the storage tree
    or cross the canonical-id namespace: empty/whitespace, ``..``, path separators,
    NULs, and ``.`` (the id delimiter — ``prod.db`` as a datasource name would let
    ``delete_datasource('prod')`` nuke ``prod.db.*``). Guards the raw-string
    read/delete paths that bypass Pydantic."""
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


def _validate_default_time_dimension(model: SlayerModel) -> None:
    column = model.get_column(model.default_time_dimension) if model.default_time_dimension else None
    if column is not None and column.type not in (DataType.DATE, DataType.TIMESTAMP):
        raise DefaultTimeDimensionTypeError(model=model.name, column=column.name, column_type=column.type)


class StorageBackend(ABC):
    """Abstract async storage backend keyed by ``(data_source, name)``. Concrete backends implement the composite-key CRUD; this class supplies shared validation and the priority-aware bare-name resolver."""

    #: True on filename-backed backends (YAML): saves reject ids differing only
    #: by case (they alias on case-insensitive filesystems). Wrappers copy it.
    _ids_collide_as_filenames = False

    # ---- model CRUD (composite key) ----------------------------------------

    async def save_model(
        self, model: SlayerModel, *, _validate: bool = True, failures: DocumentLoadFailures | None = None,
    ) -> None:
        """Persist a model: reserved-name and case-collision rejection, then derived-column well-formedness (reference arity + cycles) and join-edge validation, before the backend write. ``_validate=False`` (migration write-back only) skips all of it. Backends must NOT override this."""
        if _validate:
            if model.name == TIME_SPINE_MODEL:
                raise ReservedModelNameError(name=model.name)
            if self._ids_collide_as_filenames:
                await self._check_model_identity_collision(model)
            # A skipped (unloadable) peer is invisible to these checks; its edge names go unchecked.
            loaded, unloaded = await self.load_models(data_source=model.data_source, exclude=model.name)
            peers = {m.name: m for m in (failures or DocumentLoadFailures()).skip((loaded, unloaded))}
            validate_derived_columns(model=model, peers=peers, unloaded=frozenset(e.name for e in unloaded))
            self._validate_join_edges(model, peers=peers, identities=await self._list_all_model_identities())
            await self._validate_aggregations(model)
            await self._validate_column_granularities(model)
            _validate_default_time_dimension(model)
        await self._save_model_impl(model)
        self._forget_raw(data_source=model.data_source, name=model.name)

    async def _validate_column_granularities(self, model: SlayerModel) -> None:
        named = [(c, c.granularity) for c in model.columns if c.granularity is not None and not isinstance(c.granularity, TimeGranularity)]
        if not named:
            return
        ds = await self.get_datasource(model.data_source) if model.data_source else None
        defined = ds.granularity_definitions if ds is not None else {}
        for column, granularity in named:
            if resolve_granularity(granularity, defined=defined) is None:
                raise UnknownGranularityError(summary=unknown_granularity_message(
                    name=str(granularity), defined=defined.values(),
                    where=f"column {column.name!r} of model {model.name!r}",
                ))

    async def _validate_aggregations(self, model: SlayerModel) -> None:
        if not model.aggregations:
            return
        ds = await self.get_datasource(model.data_source) if model.data_source else None
        dialect = dialect_for_ds_type(ds.type).sqlglot_name if ds else ""
        for agg in model.aggregations:
            check_aggregation_definition(
                where=f"Model '{model.name}', aggregation '{agg.name}'", agg=agg, dialect=dialect,
            )

    @staticmethod
    def _validate_join_edges(
        model: SlayerModel, *, peers: dict[str, SlayerModel], identities: list[tuple[str, str]],
    ) -> None:
        """Save-time join validation against the loaded datasource ``peers``: reject duplicate incident edge names, edge/model name collisions (both directions), and exact-inverse re-declarations; warn on unnamed parallel edges."""
        clash = next((n for n, p in sorted(peers.items()) if any(j.name == model.name for j in p.joins)), None)
        if clash is not None:
            raise ValueError(
                f"Model name '{model.name}' collides with the join name "
                f"'{model.name}' declared on model '{clash}' in datasource "
                f"'{model.data_source}'. Edge names and model names share "
                f"the path-segment namespace."
            )
        if not model.joins:
            return
        names = _checked_join_names(model)
        _checked_join_name_namespace(model=model, names=names, identities=identities)
        # Join targets always; every peer when a named edge must be checked against incident edges.
        if not names:
            targets = {j.target_model for j in model.joins}
            peers = {n: p for n, p in peers.items() if n in targets}
        _check_edges_against_peers(model=model, peers=peers)
        _warn_unnamed_parallel_edges(model=model, peers=peers)

    async def _check_model_identity_collision(self, model: SlayerModel) -> None:
        """Reject a model whose ``data_source`` or ``name`` case-collides with an existing one (both are YAML filename components)."""
        identities = await self._list_all_model_identities()
        known_ds = {ds for ds, _ in identities}
        known_ds.update(await self.list_datasources())
        collide = _find_case_colliding_id(
            candidate=model.data_source, existing=known_ds,
        )
        if collide is not None:
            raise IdCollisionError(
                kind="datasource", new_id=model.data_source, existing_id=collide,
            )
        names_in_ds = [n for ds, n in identities if ds == model.data_source]
        collide = _find_case_colliding_id(
            candidate=model.name, existing=names_in_ds,
        )
        if collide is not None:
            raise IdCollisionError(
                kind="model",
                new_id=model.name,
                existing_id=collide,
                data_source=model.data_source,
            )

    async def builtin_models(
        self, data_source: str, *, detailed: bool = False, failures: DocumentLoadFailures | None = None,
    ) -> list[SlayerModel]:
        """The datasource's built-in models (the time spine, unless a stored model shadows it);
        ``detailed`` describes its wiring, else its description points at ``inspect``. Empty for an unknown datasource."""
        if await self.get_datasource(data_source) is None:
            return []
        if (data_source, TIME_SPINE_MODEL) in await self._list_all_model_identities():
            return []
        if not detailed:
            return [spine_model(data_source=data_source)]
        peers = (failures or DocumentLoadFailures()).skip(await self.load_models(data_source=data_source))
        return [spine_model(data_source=data_source, wired=peers)]

    async def load_models(self, *, data_source: str | None = None, exclude: str | None = None) -> Loaded[SlayerModel]:
        """Every stored model (of ``data_source`` when given, bar ``exclude``), and the ones that failed to load."""
        sem = asyncio.Semaphore(_MODEL_LOAD_CONCURRENCY)

        async def _load(ds: str, name: str) -> SlayerModel | StoredDocumentLoadError | None:
            async with sem:
                try:
                    return await self.get_model(name, data_source=ds)
                except StoredDocumentLoadError as exc:
                    return exc

        token = _RAW_PASS.set({}) if _RAW_PASS.get() is None else None
        try:
            loaded = await asyncio.gather(*(
                _load(ds, name) for ds, name in await self._list_all_model_identities()
                if (data_source is None or ds == data_source) and name != exclude
            ))
        finally:
            if token is not None:
                _RAW_PASS.reset(token)
        return (
            [m for m in loaded if isinstance(m, SlayerModel)],
            [e for e in loaded if isinstance(e, StoredDocumentLoadError)],
        )

    async def get_model_or_builtin(self, name: str, data_source: str | None = None) -> SlayerModel | None:
        """``get_model``, falling back to a built-in model of the datasource (the only one when unnamed)."""
        model = await self.get_model(name, data_source=data_source)
        if model is not None or name != TIME_SPINE_MODEL:
            return model
        datasources = [data_source] if data_source is not None else await self.list_datasources()
        if len(datasources) != 1:
            return None
        return next(iter(await self.builtin_models(datasources[0], detailed=True)), None)

    @abstractmethod
    async def _save_model_impl(self, model: SlayerModel) -> None:
        """Backend-specific durable write of ``model`` (backends implement this, not ``save_model``)."""

    @abstractmethod
    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        """Every saved ``(data_source, name)`` pair (backends pick the cheapest enumeration)."""

    @abstractmethod
    async def get_model(
        self,
        name: str,
        data_source: str | None = None,
    ) -> SlayerModel | None: ...

    async def _load_raw_model_dict(
        self, *, name: str, data_source: str,
    ) -> dict | None:
        """The persisted model dict verbatim — no migration, no validation; ``None`` when absent. The legacy-``__`` rewrite reads sibling models raw through this to avoid recursing their load pipeline; YAML/SQLite override, the default ``None`` limits the rewrite to first-hop walks."""
        await asyncio.sleep(0)  # awaited protocol hook; async impls override
        return None

    async def _cached_raw_model_dict(self, *, name: str, data_source: str) -> dict | None:
        """``_load_raw_model_dict``, read once per ``load_models`` pass (callers must not mutate it)."""
        memo = _RAW_PASS.get()
        if memo is None:
            return await self._load_raw_model_dict(name=name, data_source=data_source)
        read = memo.get((self, data_source, name))
        if read is None:
            read = memo[(self, data_source, name)] = asyncio.ensure_future(
                self._load_raw_model_dict(name=name, data_source=data_source))
        return await asyncio.shield(read)  # one waiter's cancellation must not fail the others

    def _forget_raw(self, *, data_source: str, name: str) -> None:
        """Drop ``(data_source, name)`` from the enclosing pass's raw reads after a write to it."""
        memo = _RAW_PASS.get()
        if memo is not None:
            memo.pop((self, data_source, name), None)

    async def delete_model(
        self,
        name: str,
        data_source: str | None = None,
    ) -> bool:
        """Delete one model by ``(data_source, name)`` and cascade-delete its embeddings. Bare ``name`` resolves via the priority list; ``False`` (no cascade) when no model matches."""
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return False
        resolved_data_source, resolved_name = target
        deleted = await self._delete_model_row(
            data_source=resolved_data_source, name=resolved_name,
        )
        if deleted:
            self._forget_raw(data_source=resolved_data_source, name=resolved_name)
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
        """Delete the persisted row for ``(data_source, name)`` (``True`` if removed); the embedding cascade is the public ``delete_model`` wrapper's job."""

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
        """Patch a column's ``sampled``/``sampled_values``/``distinct_count`` as one read-modify-write (``None`` drops the key; other fields untouched). Raises ``ValueError`` when the model or column is absent."""

    # ---- shared model lookup / load helpers --------------------------------

    async def _resolve_target_or_none(
        self,
        name: str,
        *,
        data_source: str | None,
    ) -> tuple[str, str] | None:
        """Sanitize inputs and resolve a bare ``name`` to its ``(data_source, name)`` identity via the priority list; ``None`` when ``data_source`` is omitted and no such bare name exists."""
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
        """Refine ``data`` against the live datasource when it has refineable columns. Hard-fails (``ValueError``) when DOUBLE base columns need it but the datasource is missing; SQLite-INT widening under a missing/unreachable DS is best-effort (warn and skip). No-op when neither predicate fires."""
        needs_double = has_refineable_columns(data)
        needs_sqlite_int = has_sqlite_widenable_columns(data)
        if not (needs_double or needs_sqlite_int):
            return
        ds = await self.get_datasource(data_source)
        if ds is not None:
            try:
                refine_dict_with_live_schema(data, ds)
            except Exception:
                # Unreachable DS: DOUBLE narrowing is required (propagate); INT
                # widening is advisory (persisted INT is safe) — warn and skip.
                if needs_double:
                    raise
                self._warn_skipped_int_probe(name=name, data_source=data_source)
            return
        if needs_double:
            raise ValueError(
                f"Cannot migrate model {name!r}: datasource "
                f"{data_source!r} is unavailable for type "
                f"refinement. Restore the datasource entry or "
                f"remove the stale model file."
            )
        self._warn_skipped_int_probe(name=name, data_source=data_source)

    @staticmethod
    def _warn_skipped_int_probe(*, name: str, data_source: str) -> None:
        # Sanitize CR/LF before logging (log injection, S5145).
        safe_ds = data_source.replace("\r", "\\r").replace("\n", "\\n")
        safe_name = name.replace("\r", "\\r").replace("\n", "\\n")
        logging.getLogger(__name__).warning(
            "Datasource '%s' unavailable; skipping SQLite "
            "affinity probe for INT base columns on '%s'. "
            "Re-run `slayer ingest` once the datasource is "
            "back to widen any mis-typed columns.",
            safe_ds,
            safe_name,
        )

    async def _migrate_and_refine_on_load(
        self,
        *,
        name: str,
        data: Any,
        data_source: str,
    ) -> SlayerModel:
        """Migrate a below-version on-disk model dict forward, refine ``DOUBLE → INT`` base columns against the live datasource, validate into a ``SlayerModel``, and (when a migration ran) persist it back so later loads short-circuit. Hard-fails when refineable DOUBLE columns need a missing datasource; SQLite-INT widening under a missing DS is best-effort. No live datasource needed when nothing is refineable/widenable."""
        if not isinstance(data, dict):
            # e.g. a zero-byte/corrupt YAML file (safe_load -> None): give the
            # remediation, not a bare Pydantic model_type error.
            raise ValueError(
                f"Model {name!r} in datasource {data_source!r} has an empty or "
                f"corrupt stored definition (got {type(data).__name__} instead "
                f"of a mapping). Delete the stored entry (YAML layout: "
                f"models/{data_source}/{name}.yaml) and re-run `slayer ingest` "
                f"to recreate it."
            )
        write_back = False
        data = _mig.stamp_stored(data)
        pre_version = int(data["version"])
        if pre_version < _mig.CURRENT_VERSIONS["SlayerModel"]:
            data = _mig.migrate("SlayerModel", data)
            # Rewrite legacy ``__`` split-alias qualifiers to dotted on the RAW
            # dict, before validation (every forward migration may carry them).
            data = await self._rewrite_legacy_join_aliases(
                name=name, data=data, data_source=data_source,
            )
            await self._canonicalize_join_key_spellings(data=data, data_source=data_source)
            await self._declare_join_keys(name=name, data=data, data_source=data_source)
            await self._repair_source_query_filters(data=data, data_source=data_source)
            # Collapse stored exact-inverse mirror pairs.
            data = await self._dedup_exact_inverse_joins(
                name=name, data=data, data_source=data_source,
            )
            write_back = True
            if pre_version < _LIVE_REFINEMENT_BELOW_VERSION:
                await self._apply_refinement_or_raise(
                    name=name, data=data, data_source=data_source,
                )
        model = SlayerModel.model_validate(data)
        if write_back:
            # Write-back must not re-validate: legacy models may hold cycles or
            # fanning refs a user needs to load to repair.
            await self.save_model(model, _validate=False)
        return model

    async def _canonicalize_join_key_spellings(self, *, data: dict, data_source: str) -> None:
        """Rewrite stored physical join-key spellings in ``data`` to ``Column.name``, both sides."""
        joins = data.get("joins")
        if not isinstance(joins, list):
            return
        for join in joins:
            if not isinstance(join, dict) or not isinstance(join.get("target_model"), str):
                continue
            peer = await self._cached_raw_model_dict(
                name=join["target_model"], data_source=data_source,
            )
            join["join_pairs"] = canonical_join_pairs(
                pairs=join.get("join_pairs"), source_columns=data.get("columns"),
                target_columns=peer.get("columns") if isinstance(peer, dict) else None,
            )

    async def _declare_join_keys(self, *, name: str, data: dict, data_source: str) -> None:
        """Declare each join key naming no column of this table- or SQL-backed document as its hidden base column:
        source keys from its own joins, target keys from its siblings' joins into it. Typed like the opposite key."""
        columns = data.get("columns")
        if data.get("source_queries") or not isinstance(columns, list):
            return
        keys = await self._outgoing_keys(joins=data.get("joins"), data_source=data_source)
        keys += await self._sibling_keys_into(name=name, columns=columns, data_source=data_source)
        for key, key_type in keys:
            if _undeclared_key(key=key, columns=columns):
                columns.append({"name": key, "hidden": True, **({"type": key_type} if key_type else {})})

    async def _outgoing_keys(self, *, joins: Any, data_source: str) -> list[tuple[Any, str | None]]:
        """Source-side keys of ``joins``, each typed like its target key."""
        keys: list[tuple[Any, str | None]] = []
        for join in joins if isinstance(joins, list) else []:
            target = join.get("target_model") if isinstance(join, dict) else None
            peer = await self._cached_raw_model_dict(name=target, data_source=data_source) if isinstance(target, str) else None
            peer_columns = peer.get("columns") if isinstance(peer, dict) else None
            keys.extend((src, _stored_type(key=tgt, columns=peer_columns)) for src, tgt in _key_pairs(join))
        return keys

    async def _sibling_keys_into(self, *, name: str, columns: list, data_source: str) -> list[tuple[Any, str | None]]:
        """Target-side keys of every same-datasource sibling's joins into ``name``."""
        keys: list[tuple[Any, str | None]] = []
        for ds, sibling_name in await self._list_all_model_identities():
            if ds != data_source or sibling_name == name:
                continue
            sibling = await self._cached_raw_model_dict(name=sibling_name, data_source=ds)
            if sibling is not None:
                keys.extend(_incoming_keys(sibling=sibling, target=name, target_columns=columns))
        return keys

    async def _repair_source_query_filters(self, *, data: dict, data_source: str) -> None:
        queries = data.get("source_queries")
        if not isinstance(queries, list):
            return
        stage_names = frozenset(q["name"] for q in queries if isinstance(q, dict) and isinstance(q.get("name"), str))
        for query in queries:
            await self._repair_query_filters(query=query, data_source=data_source, stage_names=stage_names)

    async def _repair_query_filters(
        self, *, query: Any, data_source: str | None, stage_names: frozenset[str] = frozenset(),
    ) -> None:
        """Repair, in place, legacy time literals a raw stored query's ``filters`` compare with a DATE or TIMESTAMP column."""
        filters = query.get("filters") if isinstance(query, dict) else None
        if not isinstance(filters, list):
            return
        found = [legacy_literal_sites(f) if isinstance(f, str) else None for f in filters]
        if not any(found):
            return
        host = await self._query_host(source=query.get("source_model"), data_source=data_source, stage_names=stage_names)
        if host is None:
            return
        for i, pairs in enumerate(found):
            if pairs is None:
                continue
            temporal = [
                literal for literal, column in pairs
                if await self._stored_column_type(host=host, parts=[p.name for p in column.parts])
                in (DataType.DATE.value, DataType.TIMESTAMP.value)
            ]
            if temporal:
                filters[i] = repaired_filter(filters[i], temporal)

    async def _query_host(
        self, *, source: Any, data_source: str | None, stage_names: frozenset[str],
    ) -> tuple[dict, str] | None:
        """The raw stored model a stored query reads and its datasource; ``None`` for a stage or an unresolvable source."""
        if isinstance(source, dict) and isinstance(source.get("columns"), list):
            return source, str(source.get("data_source") or data_source)
        name = source.get("source_name") if isinstance(source, dict) else source
        if not isinstance(name, str) or name in stage_names:
            return None
        if data_source is None:
            try:
                identity = await self.resolve_model_identity(name)
            except AmbiguousModelError:
                return None
            data_source = identity[0] if identity is not None else None
        raw = await self._cached_raw_model_dict(name=name, data_source=data_source) if data_source else None
        return (raw, data_source) if isinstance(raw, dict) and data_source else None

    async def _stored_column_type(self, *, host: tuple[dict, str], parts: list[str]) -> str | None:
        """The stored type of the column ``parts`` names from ``host``, walking its stored joins."""
        current, data_source = host
        quals, leaf = parts[:-1], parts[-1]
        if quals and quals[0] == current.get("name"):
            quals = quals[1:]
        for hop in quals:
            joins = current.get("joins")
            joins = [j for j in joins if isinstance(j, dict)] if isinstance(joins, list) else []
            target = next((j.get("target_model") for j in joins if j.get("name") == hop), None) or next(
                (hop for j in joins if not j.get("name") and j.get("target_model") == hop), None)
            peer = await self._cached_raw_model_dict(name=target, data_source=data_source) if isinstance(target, str) else None
            if peer is None:
                return None
            current = peer
        return _stored_type(key=leaf, columns=current.get("columns"))

    async def _dedup_exact_inverse_joins(
        self, *, name: str, data: dict, data_source: str,
    ) -> dict:
        """Drop this document's half of a stored exact-inverse join pair when the counterpart holds the surviving half. Load-order-independent (survivor reads only the two halves); a missing/drifted peer leaves the document untouched."""
        joins = data.get("joins")
        if not isinstance(joins, list) or not joins:
            return data
        cache: dict[str, dict | None] = {}
        kept = [
            join for join in joins
            if await self._survives_stored_inverse(
                join=join, name=name, data_source=data_source, cache=cache,
                columns=data.get("columns"),
            )
        ]
        if len(kept) != len(joins):
            data["joins"] = kept
        return data

    async def _survives_stored_inverse(
        self, *, join, name: str, data_source: str,
        cache: dict[str, dict | None], columns: Any,
    ) -> bool:
        """True when ``join`` has no stored exact-inverse counterpart, or wins
        against it."""
        peer_name = join.get("target_model") if isinstance(join, dict) else None
        if not isinstance(peer_name, str) or peer_name == name:
            return True
        if peer_name not in cache:
            cache[peer_name] = await self._cached_raw_model_dict(
                name=peer_name, data_source=data_source,
            )
        counterpart = _stored_counterpart(
            join=join, name=name, peer=cache[peer_name], columns=columns,
        )
        return counterpart is None or _inverse_survivor(
            model_a=name, join_a=join,
            model_b=peer_name, join_b=counterpart,
        ) == name

    async def _rewrite_legacy_join_aliases(
        self, *, name: str, data: dict, data_source: str,
    ) -> dict:
        """Rewrite legacy ``__`` split-alias qualifiers to dotted across every Mode-A surface on the raw dict. Each ``a__b__c`` is naive-split and resolved as a join walk (first hop against ``data``'s joins, deeper hops against sibling raw dicts); only fully resolvable walks are rewritten, an unresolvable ``__`` is left verbatim. Mutates and returns ``data``."""
        all_chains: set[tuple[str, ...]] = set()
        for text in mode_a_surface_texts(data):
            all_chains |= extract_dunder_chains(text)
        if not all_chains:
            return data

        raw_cache: dict[str, dict | None] = {name: data}
        resolvable: set[tuple[str, ...]] = set()
        for chain in all_chains:
            if await self._dunder_chain_is_join_walk(
                chain=chain, host=data, data_source=data_source, cache=raw_cache,
            ):
                resolvable.add(chain)
        if not resolvable:
            return data

        apply_dunder_rewrite_to_model_dict(data, resolvable=resolvable)
        return data

    async def _dunder_chain_is_join_walk(
        self,
        *,
        chain: tuple[str, ...],
        host: dict,
        data_source: str,
        cache: dict[str, dict | None],
    ) -> bool:
        """True iff every hop in ``chain`` is a join target on the preceding model (host first, then each hop's own model); sibling dicts loaded raw and memoised, a broken intermediate collapses the chain."""
        # An exact ``__``-named join target beats a split-alias reading: a host
        # join to a model literally named ``"__".join(chain)`` is exact, not a walk.
        host_joins = host.get("joins")
        host_targets = {
            j.get("target_model")
            for j in host_joins if isinstance(j, dict)
        } if isinstance(host_joins, list) else set()
        if "__".join(chain) in host_targets:
            return False

        current: dict | None = host
        for i, hop in enumerate(chain):
            if current is None:
                return False
            joins = current.get("joins")
            targets = {
                j.get("target_model")
                for j in joins if isinstance(j, dict)
            } if isinstance(joins, list) else set()
            if hop not in targets:
                return False
            if i == len(chain) - 1:
                return True
            if hop not in cache:
                cache[hop] = await self._cached_raw_model_dict(
                    name=hop, data_source=data_source,
                )
            current = cache[hop]
        return True

    # ---- datasource CRUD ---------------------------------------------------

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        """Persist a datasource config (upsert by exact name) once its custom granularities pass the save-time rules. Backends must NOT override this."""
        datasource.check_granularities()
        await self._save_datasource_impl(datasource)

    @abstractmethod
    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        """Backend-specific durable write; filename-backed backends should call ``check_datasource_id_collision`` first."""

    async def check_datasource_id_collision(self, name: str) -> None:
        """Raise :class:`IdCollisionError` when ``name`` case-collides with an existing datasource name or a saved model's ``data_source`` (public so backends call it from ``save_datasource``)."""
        existing = set(await self.list_datasources())
        existing.update(ds for ds, _ in await self._list_all_model_identities())
        collide = _find_case_colliding_id(candidate=name, existing=existing)
        if collide is not None:
            raise IdCollisionError(
                kind="datasource", new_id=name, existing_id=collide,
            )

    @abstractmethod
    async def get_datasource(self, name: str) -> DatasourceConfig | None: ...

    @abstractmethod
    async def list_datasources(self) -> list[str]: ...

    async def delete_datasource(self, name: str) -> bool:
        """Delete the datasource config and cascade-delete every embedding under its canonical prefix. Models in the datasource are NOT deleted — they become orphans; re-creating the datasource and re-ingesting repopulates embeddings."""
        # Sanitize the raw name before it composes a path / cascade prefix.
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
        """Delete the datasource config row (``True`` when removed); cascade is the public ``delete_datasource`` wrapper's job."""

    # ---- datasource priority (bare-name disambiguation) -------------------

    @abstractmethod
    async def get_datasource_priority(self) -> list[str]:
        """The configured priority order (most-preferred first); empty = none, so a bare name in ≥2 datasources raises ``AmbiguousModelError``."""

    @abstractmethod
    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        """Persist the priority list verbatim (validation is in the public wrapper)."""

    async def set_datasource_priority(self, priority: list[str]) -> None:
        """Validate (each entry must be a saved ``DatasourceConfig``, else ``ValueError``) and persist the priority list; ``[]`` clears it."""
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
        """Model names within one datasource. With ``data_source``: models stored under it (accepted if registered OR referenced by any saved model, so orphans list; unknown names raise ``ValueError``). Without it: the sole datasource's models, ``[]`` when empty, or a ``ValueError`` naming the datasources when ≥2 hold models."""
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
        """Resolve a bare model name to ``(data_source, name)``: ``None`` if no match, the sole match, ``prefer_data_source`` when it is a candidate, else the first priority-listed datasource holding it, else raise ``AmbiguousModelError``. ``prefer_data_source`` is the internal join-target hint; explicit callers use ``get_model(..., data_source=...)``."""
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

    # ---- memories ----
    # Ids are non-empty strings; auto-allocation walks max(int-shaped id)+1
    # (pure-digit, no leading zero). User ids share the namespace and upsert
    # (original ``created_at`` preserved). delete_memory cascades embeddings
    # and drops ``memory:<id>`` refs from every other memory's ``entities``.

    @abstractmethod
    async def _save_memory_row(self, memory: Memory) -> None:
        """Persist a fully-populated ``Memory`` (upsert by id; preserve any existing ``created_at``)."""

    @abstractmethod
    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        """Read a ``Memory`` by id; return ``None`` when not present."""

    async def get_memory_row(self, memory_id: str) -> Memory | None:
        """Non-raising fetch/existence check (public so the resolver and ingest cleanup can probe)."""
        return await self._get_memory_row(memory_id)

    @abstractmethod
    async def _list_memories_rows(
        self, *, entities: list[str] | None
    ) -> Loaded[Memory]:
        """Every ``Memory`` whose entity set intersects ``entities`` (``None`` = all rows, ``[]`` = none), and the rows that failed to load."""

    async def _memory_from_stored(self, *, memory_id: str, decode: Callable[[], Any]) -> Memory:
        """Decode, repair and validate one stored memory inside its load boundary."""
        with stored_document_boundary(kind="memory", name=memory_id):
            data = _mig.stamp_stored(decode())
            # Memories are never written back, so the repair repeats on every load of a pre-v4 document.
            if isinstance(data, dict) and int(data["version"]) < _MEMORY_FILTER_REPAIR_BELOW_VERSION:
                await self._repair_query_filters(query=data.get("query"), data_source=None)
            return Memory.model_validate(data)

    @abstractmethod
    async def _delete_memory_row(self, memory_id: str) -> bool:
        """Delete by id; ``True`` if a row was removed, ``False`` when absent."""

    @abstractmethod
    async def _next_memory_seq(self) -> str:
        """Next int-shaped memory id (string), above every int-shaped id held — pure-digit, no leading zero (``"42abc"``/``"001"`` ignored); empty corpus → ``"1"``."""

    async def save_memory(
        self,
        *,
        learning: str,
        entities: list[str],
        query: SlayerQuery | None = None,
        id: str | None = None,  # noqa: A002 — public kwarg matching MCP / REST
        description: str | None = None,
    ) -> Memory:
        """Persist a memory. ``id=None`` allocates the next int-shaped id; a supplied id is charset-checked and upserts (original ``created_at`` preserved; a case-collision raises :class:`IdCollisionError` on YAML backends). ``description`` is an optional compact preview (capped on the model)."""
        if id is not None:
            _validate_memory_id_charset(id)
            if self._ids_collide_as_filenames:
                ids = await self._memory_ids()
                collide = _find_case_colliding_id(candidate=id, existing=ids)
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

    async def _memory_ids(self) -> list[str]:
        """Every stored memory id, loadable or not."""
        memories, errors = await self._list_memories_rows(entities=None)
        return [m.id for m in memories] + [e.name for e in errors]

    async def load_memories(self, *, entities: list[str] | None = None) -> Loaded[Memory]:
        """The memories ``list_memories`` returns, and the ones that failed to load."""
        return await self._list_memories_rows(entities=entities)

    async def list_memories(
        self, *, entities: list[str] | None = None, failures: DocumentLoadFailures | None = None,
    ) -> list[Memory]:
        return (failures or DocumentLoadFailures()).skip(await self._list_memories_rows(entities=entities))

    async def delete_memory(self, memory_id: str) -> None:
        if not await self._delete_memory_row(memory_id):
            raise MemoryNotFoundError(memory_id)
        # Cascade: drop embeddings tagged with this memory's canonical id...
        await self.delete_embeddings_for_canonical(
            canonical_id_prefix=f"{_MEMORY_PREFIX}{memory_id}",
        )
        # ...and drop ``memory:<id>`` refs from every other memory's entities.
        await self.strip_dangling_entities_from_memories(
            canonical_id=f"{_MEMORY_PREFIX}{memory_id}",
        )

    async def strip_dangling_entities_from_memories(
        self, *, canonical_id: str,
    ) -> int:
        """Remove ``canonical_id`` from every memory's ``entities`` list; returns the count rewritten. Match: ``memory:<id>`` exact-only, ``<ds>[.<model>[.<leaf>]]`` exact or strict dotted descendant. Per-row read-modify-write via ``_save_memory_row`` (not the service); learning-only embeddings mean no refresh fires."""
        if not canonical_id:
            return 0
        is_memory_ref = canonical_id.startswith(_MEMORY_PREFIX)
        memories = await self.list_memories()
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
        """Cheap snapshot check: does ``memory.entities`` hold anything the cascade would strip? (skips the round-trip when not)."""
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
        """Re-fetch the row, drop matching entities, write back (``True`` if written). The re-fetch makes the cascade safe under concurrent saves — it always writes the freshest state."""
        fresh = await self._get_memory_row(memory_id)
        if fresh is None:
            return False  # concurrent delete won the race
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

    # ---- graph fingerprint ----

    async def graph_fingerprint(self) -> str:
        """A string that changes whenever storage content changes (drives LadybugDB graph rebuilds). Default ``"0"``; backends override with file mtimes. May raise ``OSError`` when files are inaccessible — callers force a rebuild."""
        await asyncio.sleep(0)
        return "0"

    # ---- embeddings sidecar ----
    # One row per ``(canonical_id, embedding_model_name)``; the active model
    # (``SLAYER_EMBEDDING_MODEL``) selects which rows search reads.

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
        """Every row for ``embedding_model_name`` (search loads the whole corpus into a numpy matrix)."""

    @abstractmethod
    async def delete_embeddings_for_canonical(
        self, *, canonical_id_prefix: str,
    ) -> int:
        """Cascade-delete embedding rows whose ``canonical_id`` is exactly ``canonical_id_prefix`` or a strict dotted descendant (never a character prefix — ``"orders"`` ≠ ``"orders_archive"``, ``"memory:4"`` ≠ ``"memory:42"``). Returns the count deleted; used by delete_model/_memory/_datasource."""

    # Batched helpers: defaults call the single-row methods M times so any
    # backend works unchanged; bundled backends override with one round-trip.

    async def save_embeddings(self, rows: list[Embedding]) -> None:
        """Persist many embedding rows in one round-trip (default: one :meth:`save_embedding` per row)."""
        for row in rows:
            await self.save_embedding(row)

    async def get_embeddings_for_canonical_ids(
        self,
        *,
        canonical_ids: list[str],
        embedding_model_name: str,
    ) -> dict[str, "Embedding"]:
        """Fetch every ``canonical_ids`` row under ``embedding_model_name`` in one round-trip, keyed by ``canonical_id`` (missing ids absent; default: one :meth:`get_embedding` per id)."""
        out: dict[str, Embedding] = {}
        for canonical_id in canonical_ids:
            row = await self.get_embedding(
                canonical_id=canonical_id,
                embedding_model_name=embedding_model_name,
            )
            if row is not None:
                out[canonical_id] = row
        return out


# ---- storage factory (pluggable registry) ----

_STORAGE_REGISTRY: dict[str, Callable[[str], StorageBackend]] = {}


def register_storage(scheme: str, factory: Callable[[str], StorageBackend]) -> None:
    """Register a storage backend factory for a URI scheme."""
    _STORAGE_REGISTRY[scheme.lower().strip()] = factory


def resolve_storage(path: str) -> StorageBackend:
    """Create a ``StorageBackend`` from a path/URI: a registered URI scheme first, then ``.db``/``.sqlite``/``.sqlite3`` → SQLite, else a YAML directory. Third-party backends register via ``register_storage()``."""
    if "://" in path:
        scheme, _, remainder = path.partition("://")
        scheme = scheme.lower()
        if scheme in _STORAGE_REGISTRY:
            return _STORAGE_REGISTRY[scheme](remainder)
        if scheme == "yaml":
            from slayer.storage.yaml_storage import YAMLStorage  # ALLOW(import-not-top): circular — backend modules import from this module

            return YAMLStorage(base_dir=remainder)
        if scheme == "sqlite":
            from slayer.storage.sqlite_storage import SQLiteStorage  # ALLOW(import-not-top): circular — backend modules import from this module

            # ///abs → "/abs" (absolute); //rel → "rel" (relative)
            db_path = remainder if remainder.startswith("/") else remainder.lstrip("/")
            return SQLiteStorage(db_path=db_path)
        raise ValueError(
            f"Unknown storage scheme '{scheme}'. "
            f"Built-in: yaml, sqlite. "
            f"Registered: {', '.join(_STORAGE_REGISTRY) or 'none'}. "
            f"Use register_storage() to add custom backends."
        )

    if path.endswith((".db", ".sqlite", ".sqlite3")):
        from slayer.storage.sqlite_storage import SQLiteStorage  # ALLOW(import-not-top): circular — backend modules import from this module

        return SQLiteStorage(db_path=path)

    from slayer.storage.yaml_storage import YAMLStorage  # ALLOW(import-not-top): circular — backend modules import from this module

    return YAMLStorage(base_dir=path)
