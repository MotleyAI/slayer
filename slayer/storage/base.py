"""Abstract storage protocol and factory."""

import asyncio
import logging
import os
import sys
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from collections.abc import Callable, Iterable

from slayer.core.enums import JoinCardinality, invert_cardinality
from slayer.core.errors import (
    AmbiguousModelError,
    IdCollisionError,
    MemoryNotFoundError,
)
from slayer.engine.column_dependency import validate_derived_columns
from slayer.core.join_walker import edges_between
from slayer.core.models import (
    DatasourceConfig,
    SlayerModel,
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
# Live type refinement repairs pre-v11 schemas only; later bumps are dict-only.
_LIVE_REFINEMENT_BELOW_VERSION = 11


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


class StorageBackend(ABC):
    """Abstract async storage backend keyed by ``(data_source, name)``. Concrete backends implement the composite-key CRUD; this class supplies shared validation and the priority-aware bare-name resolver."""

    #: True on filename-backed backends (YAML): saves reject ids differing only
    #: by case (they alias on case-insensitive filesystems). Wrappers copy it.
    _ids_collide_as_filenames = False

    # ---- model CRUD (composite key) ----------------------------------------

    async def save_model(
        self, model: SlayerModel, *, _validate: bool = True,
    ) -> None:
        """Persist a model: case-collision rejection (filename backends), then derived-column well-formedness (reference arity + cycles) and join-edge validation, before the backend write. ``_validate=False`` (migration write-back only) skips validation. Backends must NOT override this."""
        if _validate:
            if self._ids_collide_as_filenames:
                await self._check_model_identity_collision(model)
            await validate_derived_columns(model=model, storage=self)
            await self._validate_join_edges(model)
            await self._validate_aggregations(model)
        await self._save_model_impl(model)

    async def _validate_aggregations(self, model: SlayerModel) -> None:
        if not model.aggregations:
            return
        ds = await self.get_datasource(model.data_source) if model.data_source else None
        dialect = dialect_for_ds_type(ds.type).sqlglot_name if ds else ""
        for agg in model.aggregations:
            check_aggregation_definition(
                where=f"Model '{model.name}', aggregation '{agg.name}'", agg=agg, dialect=dialect,
            )

    async def _validate_join_edges(self, model: SlayerModel) -> None:
        """Save-time join validation: reject duplicate incident edge names, edge/model name collisions (both directions), and exact-inverse re-declarations; warn on unnamed parallel edges."""
        clash = await self._find_edge_named(
            name=model.name, data_source=model.data_source,
            exclude_model=model.name,
        )
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
        identities = await self._list_all_model_identities()
        ds_model_names = _checked_join_name_namespace(
            model=model, names=names, identities=identities,
        )
        peers = await self._load_join_peers(
            model, names=names, ds_model_names=ds_model_names,
        )
        _check_edges_against_peers(model=model, peers=peers)
        _warn_unnamed_parallel_edges(model=model, peers=peers)

    async def _load_join_peers(
        self, model: SlayerModel, *, names: list[str], ds_model_names: set[str],
    ) -> dict[str, SlayerModel]:
        """Load peer models only where needed: join targets always, all peers only when a named edge must be checked against incident edges."""
        peers: dict[str, SlayerModel] = {}
        wanted = {j.target_model for j in model.joins}
        if names:
            wanted = {n for n in ds_model_names if n != model.name}
        for peer_name in sorted(wanted):
            if peer_name == model.name or peer_name in peers:
                continue
            peer = await self.get_model(peer_name, data_source=model.data_source)
            if peer is not None:
                peers[peer.name] = peer
        return peers

    async def _find_edge_named(
        self, *, name: str, data_source: str, exclude_model: str,
    ) -> str | None:
        """The datasource model declaring a join named ``name``, or ``None``."""
        identities = await self._list_all_model_identities()
        for ds, peer_name in identities:
            if ds != data_source or peer_name == exclude_model:
                continue
            peer = await self.get_model(peer_name, data_source=data_source)
            if peer is not None and any(j.name == name for j in peer.joins):
                return peer_name
        return None

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
        pre_version = int(data.get("version", 1))
        if pre_version < _mig.CURRENT_VERSIONS["SlayerModel"]:
            data = _mig.migrate("SlayerModel", data)
            # Rewrite legacy ``__`` split-alias qualifiers to dotted on the RAW
            # dict, before validation (every forward migration may carry them).
            data = await self._rewrite_legacy_join_aliases(
                name=name, data=data, data_source=data_source,
            )
            await self._canonicalize_join_key_spellings(data=data, data_source=data_source)
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
            peer = await self._load_raw_model_dict(
                name=join["target_model"], data_source=data_source,
            )
            join["join_pairs"] = canonical_join_pairs(
                pairs=join.get("join_pairs"), source_columns=data.get("columns"),
                target_columns=peer.get("columns") if isinstance(peer, dict) else None,
            )

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
            cache[peer_name] = await self._load_raw_model_dict(
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
                cache[hop] = await self._load_raw_model_dict(
                    name=hop, data_source=data_source,
                )
            current = cache[hop]
        return True

    # ---- datasource CRUD ---------------------------------------------------

    @abstractmethod
    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        """Persist a datasource config (upsert by exact name); filename-backed backends should call ``check_datasource_id_collision`` first."""

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
    ) -> list[Memory]:
        """Every ``Memory`` whose entity set intersects ``entities``; ``None`` returns all rows, ``[]`` returns ``[]``."""

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
                ids = [m.id for m in await self._list_memories_rows(entities=None)]
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

    async def list_memories(
        self, *, entities: list[str] | None = None
    ) -> list[Memory]:
        return await self._list_memories_rows(entities=entities)

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
