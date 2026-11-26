"""Abstract storage protocol and factory."""

import asyncio
import contextlib
import copy
import functools
from functools import partial
import logging
import os
import sys
import uuid
import warnings

import yaml
from abc import ABC, abstractmethod
from contextvars import ContextVar
from pathlib import Path
from types import MappingProxyType
from typing import Any
from collections.abc import Awaitable, Callable, Generator, Hashable, Iterable, Mapping, Sequence, Set as AbstractSet

from pydantic import BaseModel
from typing_extensions import deprecated

from slayer.core.deprecation import outside_stacklevel
from slayer.core.enums import DataType, RESERVED_NAMES, TEMPORAL_DATATYPES, TimeGranularity
from slayer.core.granularity import granularity_key, resolve_granularity, unknown_granularity_message
from slayer.core.stored_rewrites import collecting_rewrites
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine_spelling, spine_dataset, spine_variant_warnings
from slayer.core.errors import (
    AmbiguousDatasetError,
    AmbiguousReferenceError,
    CaseCollisionError,
    ColonSpellingError,
    EnforcedKeyError,
    GranularityDefinitionError,
    IdCollisionError,
    NamespaceCollisionError,
    OntologiesUnsupportedError,
    OntologyDocumentInvalidError,
    OntologyMappingInvalidError,
    OssieDocumentInvalidError,
    RelationshipTargetCaseError,
    UndeterminedDatasourceError,
    UnreadableDocumentError,
    SemanticModelsUnsupportedError,
    MemoryNotFoundError,
    DefaultTimeDimensionTypeError,
    ReservedFunctionNameError,
    ReservedDatasetNameError,
    StoredDocumentLoadError,
    TimeSpineError,
    UnknownGranularityError,
)
from slayer.engine.definitions import check_portable_fields
from slayer.engine.field_dependency import (
    check_dataset_filters, check_evaluated_metrics, check_field_agg, validate_derived_fields, warn_broken_dependents,
)
from slayer.core.relationship_walker import edges_between, unique_key_sets
from slayer.core.names import folded_collisions
from slayer.core.legacy_spellings import LegacySpan, emit_deprecations
from slayer.core.field_expression import stored_definition_text
from slayer.core.models import (
    DatasourceConfig,
    DatasetField,
    DatasetRelationship,
    SlayerDataset,
    _validate_field_name,
    case_collisions,
    name_collision_warnings,
    is_base_field_sql,
    physical_column_sql,
    shadows_library_function,
)
from slayer.core.query import SlayerQuery, mode_a_texts
from slayer.core.semantic_model import OSSIE_VERSION, ModelScopedMetric, ossie_validity_errors
from slayer.core.syntax import parse_expr
from slayer.core.vocabulary import bridge_renamed_hooks, method_twin
from slayer.core.warnings import (
    DeprecatedSpellingWarning,
    LibraryNameShadowWarning,
    OntologyMismatchWarning,
    SlayerLibraryNameShadowWarning,
    SlayerOntologyMismatchWarning,
    SlayerWarning,
    SlayerNameCollisionWarning,
    SlayerSpineRelationshipDroppedWarning,
    SlayerStoredUpgradeNeededWarning,
    SpineRelationshipDroppedWarning,
    StoredUpgradeNeededWarning,
    caller_announces,
)
from slayer.embeddings.models import Embedding
from slayer.sql import connection_policy
from slayer.memories.models import (
    MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX,
    Memory,
    _validate_memory_id_charset,
)
from slayer.storage import migrations as _mig
from slayer.storage.date_only_migration import (
    UPGRADED_AT, StoredDatasets, holds_extension_date_literal, is_exact_inverse_relationship, needs_typing, stored_type,
    typing_with,
)
from slayer.storage.document_keys import (
    CURRENT_KEYS,
    LEGACY_KEYS,
    dataset_in,
    keys_for,
    relationship_pairs,
    with_relationship_pairs,
)
from slayer.storage.document_loading import DocumentLoadFailures, Loaded, stored_document_boundary
from slayer.storage.field_expression_migration import FIELD_EXPRESSION_VERSION, respell_source_paths
from slayer.storage.instant_field_migration import INSTANT_FIELD_VERSION, typed_instant_fields
from slayer.storage.granularity_rename import (
    Renames,
    datasource_renames,
    memory_source_dataset,
    pending_renames,
    rename_in_memory,
    rename_in_dataset,
    renamed_datasource,
)
from slayer.storage.mode_b_texts import (
    canonical_spellings,
    custom_buckets_as_date_trunc,
    rewrite_memory_doc,
    rewrite_dataset_doc,
)
from slayer.storage.legacy_alias_rewrite import (
    apply_dunder_rewrite_to_dataset_dict,
    extract_dunder_chains,
    mode_a_surface_texts,
)
from slayer.database.client import SqlClient
from slayer.sql.definitions import check_forms, with_converted_sql
from slayer.sql.dialects import dialect_for_ds_type, ossie_token
from slayer.sql.mode_a_variables import check_placeholders
from slayer.storage.extension_fields import holds_removed_forms, inline_extension_fields, without_extension_fields
from slayer.storage.qualified_references import qualified_stages, stage_readings
from slayer.storage.stored_datasource import memory_fallback_population, raw_entities, raw_memory_datasource
from slayer.storage.stored_literals import LiteralGates, typed_stage
from slayer.storage.document_mapping import as_expression, with_dataset, without_dataset
from slayer.storage.model_metric_rules import DocumentScope, check_model_metric, metric_cycle, warn_broken_model_metrics
from slayer.storage.semantic_model_edits import UNSET, MetricSpec, edited_document, metric_spec, with_datasets
from slayer.core.document_text import edit_text, parse_document_text
from slayer.core.portable_sql import portable_expression
from slayer.storage.semantic_models import (
    DocumentIndex, build_index, checked_document, decompose, expression_datasets, metric_graph, unevaluable_targets,
)
from slayer.sql.sql_template import check_aggregation_definition
from slayer.ontology.documents import (
    checked_mapping_shape, edited_mapping, edited_ontology, read_ontology, ref_name, stored_name_reasons,
)
from slayer.ontology.scope import MappingScope, check_mapping
from slayer.ontology.use import MappingDocument, StoredUse, UnresolvedMapping, model_findings
from slayer.storage.ontology_documents import (
    MAPPING_NAME, ONTOLOGY_NAME, StoredMapping, broken_mappings, report_broken, stored_data,
)
from slayer.live_schema.type_refinement import (
    has_refineable_fields,
    has_sqlite_widenable_fields,
    refine_dict_with_live_schema,
)

#: Supplies the loading query engine's client for a datasource.
ClientSource = Callable[[DatasourceConfig], SqlClient]


def report_save_warnings(warnings_out: list[SlayerWarning] | None, found: Sequence[SlayerWarning]) -> None:
    """``found`` into ``warnings_out``; without it, each ontology mismatch as a Python warning."""
    if warnings_out is not None:
        warnings_out.extend(found)
        return
    for warning in found:
        if isinstance(warning, OntologyMismatchWarning):
            warnings.warn(SlayerOntologyMismatchWarning(warning), stacklevel=outside_stacklevel())



_TO_ONE_CARDINALITIES = {"many_to_one", "one_to_one"}
# v8 is the first version written only by ingest that refined both DOUBLE and SQLite INT
# fields; older documents are refined against the live schema on load.
_LIVE_REFINEMENT_BELOW_VERSION = 8
_DATASET_LOAD_CONCURRENCY = 8
_MEMORY_FILTER_REPAIR_BELOW_VERSION = 4
# Below v25 a stored relationship target may reach its dataset only by case, or target the spine.
_RELATIONSHIP_TARGET_REPAIR_BELOW_VERSION = 25
#: The Memory version from which stored extension fields are inlined.
_EXTENSION_FIELDS_INLINED_AT = 10
# Raw document reads of the enclosing ``load_datasets`` pass, by ``(backend, data_source, name)``.
#: Set while reading the backend's own (standalone) datasets, bypassing the document layer.
_STANDALONE_ONLY: ContextVar[bool] = ContextVar("_STANDALONE_ONLY", default=False)
_RAW_PASS: ContextVar[dict[tuple[object, str, str], asyncio.Future[dict | None]] | None] = ContextVar(
    "_RAW_PASS", default=None)
#: Dialects supplied for datasources the store holds no config for (``slayer storage upgrade --datasource-type``).
DATASOURCE_TYPES: ContextVar[Mapping[str, str]] = ContextVar("DATASOURCE_TYPES", default=MappingProxyType({}))
# Upgraded datasets of the enclosing top-level load, by ``(backend, data_source, name)``: the raw read, the dataset
# and whether the upgrade changes it.
_UPGRADE_PASS: ContextVar[dict[tuple[object, str, str], tuple[Any, Any, bool]] | None] = ContextVar(
    "_UPGRADE_PASS", default=None)
# The granularity rename table of the enclosing top-level load, by backend.
_RENAME_PASS: ContextVar[dict[object, Mapping[str, Renames]] | None] = ContextVar("_RENAME_PASS", default=None)
# The stored datasets whose extension fields are being inlined (their own loads must not recurse into them).
_INLINING: ContextVar[frozenset[tuple[str, str]]] = ContextVar("_INLINING", default=frozenset())
#: The versions whose saves wrote qualified references.
_QUALIFIED_AT = {"SlayerModel": 28, "SlayerQuery": 17, "Memory": 13}


def _stage_below(query: Any, *, document_version: int, entity: str, at: Mapping[str, int]) -> bool:
    """Whether a raw stored query was written below the versions ``at`` (a version-less one, by its document)."""
    if not isinstance(query, dict):
        return False
    version = query.get("version")
    if version is not None:
        return int(version) < at["SlayerQuery"]
    return document_version < at[entity]


def _unqualified(query: Any, *, document_version: int, entity: str) -> bool:
    """Whether a raw stored query was written before qualified references."""
    return _stage_below(query, document_version=document_version, entity=entity, at=_QUALIFIED_AT)


def _stage_gates(query: Any, *, document_version: int, entity: str, legacy: bool) -> LiteralGates:
    """The literal rewrites a raw stored query calls for; ``legacy`` when its document's own gate repairs them."""
    return LiteralGates(legacy=legacy, whole_day=_stage_below(
        query, document_version=document_version, entity=entity, at=UPGRADED_AT))
class LiveRefinementRequiredError(ValueError):
    """A stored dataset below v8 cannot upgrade without refining its types against its live datasource."""


# The loading query engine's client source, set by its storage view around each call it delegates.
CLIENT_SOURCE: ContextVar[ClientSource | None] = ContextVar("CLIENT_SOURCE", default=None)


def _inverse_survivor(
    *, dataset_a: str, relationship_a: dict, dataset_b: str, relationship_b: dict,
) -> str:
    """Which dataset keeps its half of an exact-inverse pair: to-one side, else cardinality-carrying side, else lexicographic — a pure function of both halves, so load orders agree."""
    a_card, b_card = relationship_a.get("cardinality"), relationship_b.get("cardinality")
    a_to_one = a_card in _TO_ONE_CARDINALITIES
    b_to_one = b_card in _TO_ONE_CARDINALITIES
    if a_to_one != b_to_one:
        return dataset_a if a_to_one else dataset_b
    if (a_card is None) != (b_card is None):
        return dataset_a if a_card is not None else dataset_b
    return min((dataset_a, dataset_b), (dataset_b, dataset_a))[0]


def _canonical_key(*, key: Any, fields: Any) -> Any:
    """``key`` unless it names no declared field but is exactly one base field's physical rename."""
    if not isinstance(key, str) or not isinstance(fields, list):
        return key
    cols = [c for c in fields if isinstance(c, dict) and isinstance(c.get("name"), str)]
    if any(c["name"] == key for c in cols):
        return key
    renames = [
        c["name"] for c in cols
        if isinstance(text := stored_definition_text(c), str) and is_base_field_sql(text)
        and physical_column_sql(sql=text, name=c["name"]) == key
    ]
    return renames[0] if len(renames) == 1 else key


def canonical_relationship_keys(*, relationship: dict, source_fields: Any, target_fields: Any) -> dict:
    """A raw relationship with its stored physical key spellings rewritten to ``DatasetField.name``."""
    pairs = relationship_pairs(relationship)
    if not pairs:
        return relationship
    return with_relationship_pairs(relationship, [
        (_canonical_key(key=a, fields=source_fields), _canonical_key(key=b, fields=target_fields)) for a, b in pairs
    ])


def _undeclared_key(*, key: Any, fields: list) -> bool:
    """A valid field name that no field of ``fields`` declares or spells physically."""
    if not isinstance(key, str):
        return False
    try:
        _validate_field_name(key, "relationship key")
    except ValueError:
        return False
    return not any(
        isinstance(c, dict) and isinstance(c.get("name"), str) and (
            c["name"] == key
            or (isinstance(text := stored_definition_text(c), (str, type(None))) and is_base_field_sql(text)
                and physical_column_sql(sql=text, name=c["name"]) == key)
        )
        for c in fields
    )


def _stage_names(data: dict) -> frozenset[str]:
    """The names of a stored dataset's own stages."""
    queries = data.get("source_queries")
    return frozenset(q["name"] for q in queries if isinstance(q, dict) and isinstance(q.get("name"), str)) if isinstance(
        queries, list) else frozenset()


@contextlib.contextmanager
def one_upgrade_pass() -> Generator[None]:
    """Within the block (one query), each stored document read unchanged is upgraded in memory once, against one
    reading of the stored datasources' granularity renames."""
    token = _UPGRADE_PASS.set({}) if _UPGRADE_PASS.get() is None else None
    rename_token = _RENAME_PASS.set({}) if _RENAME_PASS.get() is None else None
    try:
        yield
    finally:
        if rename_token is not None:
            _RENAME_PASS.reset(rename_token)
        if token is not None:
            _UPGRADE_PASS.reset(token)


def _memory_in_datasource(data: dict, *, data_source: str, identities: Sequence[tuple[str, str]]) -> bool:
    """Whether a raw stored memory's query reads ``data_source`` (the granularity rename's scope)."""
    inline_data_source, source = memory_source_dataset(data)
    if inline_data_source is not None:
        return inline_data_source == data_source
    if memory_fallback_population(data, identities=identities)[0]:
        return raw_memory_datasource(data, identities=identities) == data_source
    if (data_source, source) not in identities:
        return False
    return all(d == data_source for d, n in identities if n == source) or (
        f"{data_source}.{source}" in raw_entities(data))


def _incoming_keys(*, sibling: dict, target: str, target_fields: list) -> list[tuple[Any, str | None]]:
    """``target``-side keys of ``sibling``'s relationships into ``target``, each typed like its source key."""
    relationships = sibling.get("relationships")
    out: list[tuple[Any, str | None]] = []
    for relationship in relationships if isinstance(relationships, list) else []:
        if not isinstance(relationship, dict) or relationship.get("to") != target:
            continue
        canonical = canonical_relationship_keys(
            relationship=relationship, source_fields=sibling.get("fields"), target_fields=target_fields,
        )
        out.extend((tgt, stored_type(key=src, fields=sibling.get("fields"), keys=CURRENT_KEYS))
                   for src, tgt in relationship_pairs(canonical))
    return out


def _stored_counterpart(*, relationship: dict, name: str, peer: dict | None, fields: Any):
    """The exact-inverse of ``relationship`` in ``peer``'s raw relationships (canonicalised against ``fields``, this document's), or ``None``."""
    peer_relationships = peer.get("relationships") if isinstance(peer, dict) else None
    if not isinstance(peer, dict) or not isinstance(peer_relationships, list):
        return None
    return next(
        (
            j for j in peer_relationships
            if isinstance(j, dict) and j.get("to") == name
            and is_exact_inverse_relationship(relationship, canonical_relationship_keys(
                relationship=j, source_fields=peer.get("fields"), target_fields=fields,
            ))
        ),
        None,
    )


def _repaired_target(target: str, *, names: set[str]) -> str:
    """``target`` respelled to the one dataset of ``names`` it reaches only by case, else unchanged."""
    if target in names:
        return target
    hits = [n for n in names if n.casefold() == target.casefold()]
    return hits[0] if len(hits) == 1 else target


def _with_repaired_targets(raw: Any, *, names: set[str]) -> Any:
    """A raw peer document's relationships as its pre-v23 target repair will read them."""
    relationships = raw.get("relationships") if isinstance(raw, dict) else None
    if not isinstance(relationships, list):
        return raw
    try:
        if int(raw.get("version", 1)) >= _RELATIONSHIP_TARGET_REPAIR_BELOW_VERSION:
            return raw
    except (TypeError, ValueError):  # a malformed version is reported at that document's own load
        return raw
    return {**raw, "relationships": [
        {**j, "to": _repaired_target(j["to"], names=names)}
        if isinstance(j, dict) and isinstance(j.get("to"), str) else j
        for j in relationships
    ]}


def _document_name(text: str) -> str:
    """The ``name`` an Ossie document's text declares; raises ``OssieDocumentInvalidError`` when it does not read."""
    try:
        data = parse_document_text(text)
    except UnreadableDocumentError as exc:
        raise OssieDocumentInvalidError(name=exc.name or "(unnamed)", reasons=exc.reasons) from exc
    name = data.get("name") if isinstance(data, dict) else None
    if not isinstance(name, str) or not name:
        raise ValueError("The semantic model's content declares no name; pass one")
    return name


def _checked_relationship_names(dataset: SlayerDataset) -> list[str]:
    """Names of ``dataset``'s named edges; raises on duplicates."""
    names = [j.name for j in dataset.relationships if j.name]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise ValueError(
            f"Dataset '{dataset.name}': duplicate relationship names {dupes}. Each "
            f"edge name incident to a dataset must be unique."
        )
    return names


def _check_edges_against_peers(
    *, dataset: SlayerDataset, peers: dict[str, SlayerDataset],
) -> None:
    """Reject edge-name reuse across incident edges and exact-inverse twins."""
    incident_to_self = {
        j.name for p in peers.values() for j in p.relationships
        if j.to == dataset.name and j.name
    }
    for relationship in dataset.relationships:
        target = peers.get(relationship.to)
        if relationship.name:
            _check_edge_name_free(
                dataset=dataset, relationship=relationship, target=target, peers=peers,
                incident_to_self=incident_to_self,
            )
        if target is not None:
            _check_not_exact_inverse(dataset=dataset, relationship=relationship, target=target)


def _check_edge_name_free(
    *, dataset: SlayerDataset, relationship: DatasetRelationship, target: SlayerDataset | None,
    peers: dict[str, SlayerDataset], incident_to_self: set,
) -> None:
    target_incident = set(incident_to_self)
    if target is not None:
        target_incident |= {j2.name for j2 in target.relationships if j2.name}
        target_incident |= {
            j3.name for p in peers.values() for j3 in p.relationships
            if j3.to == relationship.to and j3.name
        }
    if relationship.name in target_incident:
        raise ValueError(
            f"Dataset '{dataset.name}': relationship name '{relationship.name}' is "
            f"already used by another edge incident to "
            f"'{dataset.name}' or '{relationship.to}'."
        )


def _check_not_exact_inverse(
    *, dataset: SlayerDataset, relationship, target: SlayerDataset,
) -> None:
    raw = relationship.model_dump(mode="json")
    for j2 in target.relationships:
        if j2.to != dataset.name:
            continue
        if is_exact_inverse_relationship(raw, j2.model_dump(mode="json")):
            raise ValueError(
                f"Dataset '{dataset.name}': relationship to '{target.name}' is "
                f"the exact inverse of the edge already declared on "
                f"'{target.name}' — reverse traversal is automatic; "
                f"remove this declaration."
            )


def _check_list_hop_collisions(*, dataset: SlayerDataset, peers: dict[str, SlayerDataset]) -> None:
    """A list field ``L`` is the hop ``L`` to its elements, so no other hop from its dataset may share the name — checked
    on this dataset's list fields and on the peers its new edges reach (design D12)."""
    incident = [(j.name or j.to, j.to) for j in dataset.relationships] + [
        (j.name or p.name, p.name) for p in peers.values() for j in p.relationships if j.to == dataset.name
    ]
    for field in dataset.fields:
        hop = next((target for token, target in incident if token == field.name), None)
        if field.datatype is DataType.ARRAY and hop is not None:
            raise ValueError(
                f"Dataset '{dataset.name}': list field '{field.name}' has the name of its hop to '{hop}'; the list's "
                f"elements are reached as '{field.name}.value', so rename the field or name the edge."
            )
    for relationship in dataset.relationships:
        peer = peers.get(relationship.to)
        token = relationship.name or dataset.name
        field = peer.get_field(token) if peer is not None else None
        if field is not None and field.datatype is DataType.ARRAY:
            raise ValueError(
                f"Dataset '{dataset.name}': its relationship to '{relationship.to}' is the hop '{token}' from "
                f"'{relationship.to}', which collides with the list field '{relationship.to}.{token}'; "
                "name the edge differently."
            )


def check_relationship_target_spellings(
    dataset: SlayerDataset, *, names: Iterable[str], peers: dict[str, SlayerDataset],
) -> None:
    """Refuse a relationship of ``dataset`` whose target reaches a dataset of ``names`` (its datasource's) only by case or targets
    the spine, and a name of ``dataset`` that a relationship of ``peers`` reaches only by case."""
    declared = {*names, dataset.name}
    for relationship in dataset.relationships:
        if relationship.to not in declared:
            _check_relationship_target(dataset=dataset, relationship=relationship, declared=declared)
    for peer in sorted(peers.values(), key=lambda p: p.name):
        for relationship in peer.relationships:
            target = relationship.to
            if target not in declared and target.casefold() == dataset.name.casefold():
                raise RelationshipTargetCaseError(
                    dataset=peer.name, relationship=relationship.name, target=target, declared=dataset.name, saving=True,
                )


def _check_relationship_target(*, dataset: SlayerDataset, relationship: DatasetRelationship, declared: set[str]) -> None:
    """Refuse ``relationship``, whose target names no dataset of ``declared`` exactly, if it targets the spine or one by case."""
    target = relationship.to
    if is_spine_spelling(target):
        edge = f"relationship {relationship.name!r}" if relationship.name else f"the unnamed relationship to {target!r}"
        raise TimeSpineError(
            summary=f"Dataset {dataset.name!r}: {edge} targets the built-in time spine.",
            suggestion="Remove the relationship: the spine joins each dataset through its time axis, so a declared relationship "
            "to it is never needed.",
        )
    hits = sorted(n for n in declared if n.casefold() == target.casefold())
    if len(hits) > 1:
        raise AmbiguousReferenceError(target, hits, suggestion="Write the relationship target exactly as one of them.")
    if hits:
        raise RelationshipTargetCaseError(dataset=dataset.name, relationship=relationship.name, target=target, declared=hits[0])


def _warn_unnamed_parallel_edges(
    *, dataset: SlayerDataset, peers: dict[str, SlayerDataset],
) -> None:
    for peer_name in {j.to for j in dataset.relationships}:
        target = peers.get(peer_name)
        if target is None:
            continue
        edges = edges_between(source=dataset, target=target)
        unnamed = [e for e in edges if e.name is None]
        # An unnamed edge in a parallel set is unaddressable (bare token ambiguous).
        if len(edges) >= 2 and unnamed:
            warnings.warn(
                f"Dataset '{dataset.name}': {len(edges)} parallel edges connect "
                f"'{dataset.name}' and '{peer_name}' and {len(unnamed)} of "
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
    """Write ``sampled``/``sampled_values``/``distinct_count`` into a field dict in place (``None`` pops the key, non-None writes it); shared by every backend's ``update_field_sampled``."""
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


#: The versions below which a stored document's custom-granularity calls are bucket calls.
_CUSTOM_BUCKET_CALLS_BELOW = {"SlayerModel": 20, "Memory": 9}


def _raw_values(value: Any) -> Any:
    """``value`` with every nested model turned into its raw field values, runtime-only fields included."""
    if isinstance(value, BaseModel):
        return {name: _raw_values(value.__dict__.get(name)) for name in type(value).model_fields}
    if isinstance(value, list):
        return [_raw_values(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_raw_values(v) for v in value)
    if isinstance(value, dict):
        return {k: _raw_values(v) for k, v in value.items()}
    return value


def _pairs(held: Any, rebuilt: Any) -> list[tuple[Any, Any]]:
    """The (held, rebuilt) values one level down: list items, dict entries, or model fields (a dict may rebuild as one)."""
    if isinstance(rebuilt, BaseModel) and isinstance(held, (BaseModel, dict)):
        values = held.__dict__ if isinstance(held, BaseModel) else held
        return [(values.get(n), rebuilt.__dict__.get(n)) for n in type(rebuilt).model_fields if n in values]
    if isinstance(held, (list, tuple)) and isinstance(rebuilt, (list, tuple)):
        return list(zip(held, rebuilt))
    if isinstance(held, dict) and isinstance(rebuilt, dict):
        return [(held[k], rebuilt[k]) for k in held.keys() & rebuilt.keys()]
    return []


def _wrong_class(held: Any, rebuilt: Any) -> bool:
    """Whether ``held`` holds a model, directly or below plain values, whose class its rebuild does not give."""
    if isinstance(held, BaseModel):
        return isinstance(rebuilt, BaseModel) and type(held) is not type(rebuilt)  # a model where none is declared passes
    return any(_wrong_class(h, r) for h, r in _pairs(held, rebuilt))


def _check_same_classes(held: Any, rebuilt: Any) -> None:
    """Refuse a model in ``held`` whose class differs from the one its place declares (as ``rebuilt`` holds)."""
    if isinstance(held, BaseModel) and isinstance(rebuilt, BaseModel):
        cls = type(rebuilt)
        values = {name: held.__dict__.get(name) for name in cls.model_fields}
        if any(_wrong_class(h, r) for h, r in _pairs(held, rebuilt)):
            cls.model_validate(values)  # construction's refusal of the held objects
            raise TypeError(f"{cls.__name__} holds a model of another class than it declares")
    for h, r in _pairs(held, rebuilt):
        _check_same_classes(h, r)


def check_whole(obj: SlayerDataset | DatasourceConfig) -> None:
    """Refuse ``obj`` unless rebuilding it from its raw values passes every check construction applies."""
    _check_same_classes(obj, type(obj).model_validate(_raw_values(obj)))


def with_canonical_spellings(dataset: SlayerDataset) -> tuple[SlayerDataset, list[DeprecatedSpellingWarning]]:
    """``dataset`` with each deprecated spelling in its Mode-B texts replaced by its twin, and one warning per spelling."""
    found: dict[str, LegacySpan] = {}
    dumped = dataset.model_dump(mode="json", exclude_none=True)
    rewritten = rewrite_dataset_doc(dumped, lambda text: canonical_spellings(text, found=found), keys=CURRENT_KEYS)
    if not found:
        return dataset, []
    return SlayerDataset.model_validate(rewritten), emit_deprecations(found)


def with_portable_sql(dataset: SlayerDataset) -> SlayerDataset:
    """``dataset`` with each field and metric expression's portable text in the canonical writer's spelling."""
    fields = [f.model_copy(update={"expression": portable_expression(f.expression)}) if f.expression is not None else f
              for f in dataset.fields]
    metrics = [m.model_copy(update={"expression": portable_expression(m.expression)}) for m in dataset.metrics]
    if [f.expression for f in fields] == [f.expression for f in dataset.fields] and \
            [m.expression for m in metrics] == [m.expression for m in dataset.metrics]:
        return dataset
    return dataset.model_copy(update={"fields": fields, "metrics": metrics})


def warn_library_shadows(dataset: SlayerDataset) -> None:
    """One warning per aggregation of ``dataset`` that shadows a library function."""
    for agg in dataset.aggregations:
        if shadows_library_function(agg):
            warnings.warn(SlayerLibraryNameShadowWarning(LibraryNameShadowWarning(
                dataset=dataset.name, aggregation=agg.name, function=agg.name,
            )), stacklevel=outside_stacklevel())


def without_none(value: Any) -> Any:
    """``value`` with every ``None`` mapping entry dropped, recursively: an unset field is never persisted."""
    if isinstance(value, dict):
        return {k: without_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [without_none(v) for v in value]
    return value


def storage_base_dir(path: str) -> str:
    """The on-disk directory for a storage path: a SQLite file's parent, else the path itself (callers colocate auxiliary files beside storage)."""
    if path.endswith((".db", ".sqlite", ".sqlite3")):
        return os.path.dirname(path) or "."
    return path


def default_storage_path() -> str:
    """Default storage dir: $SLAYER_STORAGE, else the platform data dir (XDG on Linux, Application Support on macOS, LOCALAPPDATA on Windows)."""
    current = os.environ.get("SLAYER_STORAGE")
    if current:
        return current
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
    """Cascade match: ``memory:<id>`` refs exact-only; ``<ds>[.<dataset>[.<leaf>]]`` refs exact or strict dotted descendant."""
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


DATASOURCE_NAME = "datasource name"
SEMANTIC_MODEL_NAME = "semantic model name"


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


def _refuse_colon_metrics(dataset: SlayerDataset) -> None:
    """A metric expression in a removed colon spelling fails the save; other parse errors surface at binding."""
    for metric in dataset.metrics:
        try:
            parse_expr(metric.text)
        except ColonSpellingError:
            raise
        except Exception:  # noqa: BLE001, S112 — reported when a query binds it
            continue


def check_enforced_keys(dataset: SlayerDataset) -> None:
    """Every enforced key is a declared key of a table-backed dataset."""
    declared = [sorted(k) for k in unique_key_sets(dataset)]
    for key in dataset.enforced_keys:
        if not dataset.sql_table:
            raise EnforcedKeyError(dataset=dataset.name, key=key,
                                   reason="is recorded on a dataset not backed by a table (sql or a backing query)")
        if sorted(key) not in declared:
            raise EnforcedKeyError(dataset=dataset.name, key=key,
                                   reason="is not a declared key (its primary key or a unique field)")
        computed = [name for name in key if not (field := dataset.get_field(name)) or not field.is_base]
        if computed:
            raise EnforcedKeyError(dataset=dataset.name, key=key,
                                   reason=f"names fields that are not base columns: {computed}")


def _repointed_key_field(*, old: DatasetField | None, new: DatasetField | None) -> bool:
    """A stored base key field that is no longer a base column, or reads a different one."""
    if old is None or new is None or not old.is_base:
        return False
    return not new.is_base or new.physical_name != old.physical_name


def _validate_default_time_dimension(dataset: SlayerDataset) -> None:
    field = dataset.get_field(dataset.default_time_dimension) if dataset.default_time_dimension else None
    if field is not None and field.datatype is not None and field.datatype not in TEMPORAL_DATATYPES:
        raise DefaultTimeDimensionTypeError(dataset=dataset.name, field=field.name, field_type=field.datatype)


#: The 1.x methods and hooks a custom backend may still override, by their 2.0 names.
HOOK_RENAMES = {
    "get_model": "get_dataset", "save_model": "save_dataset", "delete_model": "delete_dataset",
    "list_models": "list_datasets", "get_model_or_builtin": "get_dataset_or_builtin",
    "resolve_model_identity": "resolve_dataset_identity", "load_models": "load_datasets",
    "builtin_models": "builtin_datasets", "update_column_sampled": "update_field_sampled",
    "_save_model_impl": "_save_dataset_impl", "_list_all_model_identities": "_list_all_dataset_identities",
    "_delete_model_row": "_delete_dataset_row",
}
#: A 1.x override's keywords, by the 2.0 name (new → old).
HOOK_ARGUMENTS = {
    "save_dataset": {"dataset": "model"}, "_save_dataset_impl": {"dataset": "model"},
    "update_field_sampled": {"dataset_name": "model_name", "field_name": "column_name"},
}


def _documents_first(get: Callable[..., Any]) -> Callable[..., Any]:
    """``get_dataset`` answering a document dataset first, then the backend's own."""

    @functools.wraps(get)
    async def get_dataset(self: StorageBackend, name: str, data_source: str | None = None) -> SlayerDataset | None:
        if not _STANDALONE_ONLY.get() and self._supports_semantic_models():
            found = await self._document_dataset(name, data_source=data_source)
            if found is not None:
                return found
        return await get(self, name, data_source=data_source)

    setattr(get_dataset, "__documents__", True)
    return get_dataset


def _with_document_identities(identities: Callable[..., Any]) -> Callable[..., Any]:
    """``_list_all_dataset_identities`` listing every loadable document dataset too."""

    @functools.wraps(identities)
    async def listed(self: StorageBackend) -> list[tuple[str, str]]:
        own = await identities(self)
        if _STANDALONE_ONLY.get() or not self._supports_semantic_models():
            return own
        out, seen = list(own), set(own)
        for ds in sorted({d for d, _ in await self._list_semantic_model_identities()}):
            for dataset in (await self._document_index(ds)).datasets.values():
                if (ds, dataset.name) not in seen:
                    seen.add((ds, dataset.name))
                    out.append((ds, dataset.name))
        return out

    setattr(listed, "__documents__", True)
    return listed


def _sampled_into_documents(update: Callable[..., Any]) -> Callable[..., Any]:
    """``update_field_sampled`` patching the owning document of a document dataset."""

    @functools.wraps(update)
    async def update_field_sampled(self: StorageBackend, *, data_source: str, dataset_name: str, field_name: str,
                                   sampled: str | None, sampled_values: list[str] | None,
                                   distinct_count: int | None) -> None:
        owner = None if _STANDALONE_ONLY.get() else await self._owner_of(dataset_name, data_source=data_source)
        if owner is None:
            await update(self, data_source=data_source, dataset_name=dataset_name, field_name=field_name,
                         sampled=sampled, sampled_values=sampled_values, distinct_count=distinct_count)
            return

        def sample(old: SlayerDataset | None) -> SlayerDataset:
            if old is None or old.get_field(field_name) is None:
                raise ValueError(f"Field {field_name!r} of dataset {dataset_name!r} not found")
            values = {"sampled": sampled, "sampled_values": sampled_values, "distinct_count": distinct_count}
            return old.model_copy(update={"fields": [f.model_copy(update=values) if f.name == field_name else f
                                                     for f in old.fields]})

        await self._write_document_dataset(data_source=data_source, document=owner, name=dataset_name, mutate=sample)

    setattr(update_field_sampled, "__documents__", True)
    return update_field_sampled


def _handed_out(dataset: SlayerDataset) -> SlayerDataset:
    """A copy of a document dataset remembering the form it was read as, the base its save merges from."""
    out = dataset.model_copy(deep=True)
    out._read_as = dataset.model_copy(deep=True)
    return out


def _reads(ossie: dict[str, Any]) -> set[str]:
    """The case-folded datasets an Ossie metric expression reads."""
    return expression_datasets(as_expression(ossie))[0] if ossie else set()


_DOCUMENT_WRAPPERS = {"get_dataset": _documents_first, "_list_all_dataset_identities": _with_document_identities,
                      "update_field_sampled": _sampled_into_documents}


class StorageBackend(ABC):
    """Abstract async storage backend keyed by ``(data_source, name)``. Concrete backends implement the composite-key CRUD; this class supplies shared validation and the priority-aware bare-name resolver."""

    #: True on filename-backed backends (YAML): saves reject ids differing only
    #: by case (they alias on case-insensitive filesystems). Wrappers copy it.
    _ids_collide_as_filenames = False
    _instance_identity: str | None = None
    #: A wrapper whose dataset reads already see its inner store's documents.
    _delegates_documents = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        bridge_renamed_hooks(cls, HOOK_RENAMES, arguments=HOOK_ARGUMENTS)
        if cls.__dict__.get("_delegates_documents", False):
            return
        for attr, wrap in _DOCUMENT_WRAPPERS.items():
            own = cls.__dict__.get(attr)
            if own is not None and not getattr(own, "__documents__", False):
                setattr(cls, attr, wrap(own))

    @method_twin("get_dataset")
    @deprecated("get_model is deprecated; use get_dataset", category=None)
    async def get_model(self, name: str, data_source: str | None = None) -> SlayerDataset | None: ...

    @method_twin("save_dataset", arguments={"model": "dataset"})
    @deprecated("save_model is deprecated; use save_dataset", category=None)
    async def save_model(
        self, dataset: SlayerDataset, *, _validate: bool = True, failures: DocumentLoadFailures | None = None,
    ) -> None: ...

    @method_twin("delete_dataset")
    @deprecated("delete_model is deprecated; use delete_dataset", category=None)
    async def delete_model(self, name: str, data_source: str | None = None) -> bool: ...

    @method_twin("list_datasets")
    @deprecated("list_models is deprecated; use list_datasets", category=None)
    async def list_models(self, data_source: str | None = None) -> list[str]: ...

    @method_twin("resolve_dataset_identity")
    @deprecated("resolve_model_identity is deprecated; use resolve_dataset_identity", category=None)
    async def resolve_model_identity(
        self, name: str, *, prefer_data_source: str | None = None,
    ) -> tuple[str, str] | None: ...

    @method_twin("get_dataset_or_builtin")
    @deprecated("get_model_or_builtin is deprecated; use get_dataset_or_builtin", category=None)
    async def get_model_or_builtin(self, name: str, data_source: str | None = None) -> SlayerDataset | None: ...

    @method_twin("load_datasets")
    @deprecated("load_models is deprecated; use load_datasets", category=None)
    async def load_models(
        self, *, data_source: str | None = None, exclude: str | None = None,
    ) -> Loaded[SlayerDataset]: ...

    @method_twin("builtin_datasets")
    @deprecated("builtin_models is deprecated; use builtin_datasets", category=None)
    async def builtin_models(
        self, data_source: str, *, detailed: bool = False, failures: DocumentLoadFailures | None = None,
    ) -> list[SlayerDataset]: ...

    @method_twin("update_field_sampled", arguments={"model_name": "dataset_name", "column_name": "field_name"})
    @deprecated("update_column_sampled is deprecated; use update_field_sampled", category=None)
    async def update_column_sampled(
        self, *, data_source: str, dataset_name: str, field_name: str, sampled: str | None,
        sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None: ...

    # ---- dataset CRUD (composite key) --------------------------------------

    async def save_dataset(
        self, dataset: SlayerDataset, *, _validate: bool = True, failures: DocumentLoadFailures | None = None,
    ) -> None:
        """Persist a dataset after ``validate_for_save`` (skipped by ``_validate=False``, migration write-back only), each
        deprecated Mode-B spelling stored as its twin and a deprecated field ``sql`` as its datasource's entry. Backends
        must NOT override this."""
        if _validate:
            check_whole(dataset)
            dataset = with_portable_sql(with_canonical_spellings(dataset)[0])
            datasource = await self.get_datasource(dataset.data_source) if dataset.data_source else None
            if datasource is not None and not dataset.source_queries:
                dataset = with_converted_sql(dataset, ds_type=datasource.type)
            dataset = await self._without_repointed_enforced_keys(dataset)
            await self.validate_for_save(dataset, failures=failures)
            warn_library_shadows(dataset)
            saved = dataset
            await self._guard_mappings(change=f"Saving dataset {dataset.name!r}", data_source=dataset.data_source,
                                       changed=lambda scope: scope.with_dataset(saved.name, saved))
        owner = await self._owner_of(dataset.name, data_source=dataset.data_source)
        moving = owner is None and dataset.semantic_model is not None
        owner = owner or dataset.semantic_model
        if owner is None:
            await self._save_dataset_impl(dataset)
        else:
            await self._write_document_dataset(data_source=dataset.data_source, document=owner, name=dataset.name,
                                               mutate=lambda _old: dataset)
            if moving:
                await self._delete_dataset_row(data_source=dataset.data_source, name=dataset.name)
        self._forget_raw(data_source=dataset.data_source, name=dataset.name)

    async def _without_repointed_enforced_keys(self, dataset: SlayerDataset) -> SlayerDataset:
        """``dataset`` without the enforced keys a changed source, or a re-pointed key field, leaves describing the old."""
        if not dataset.enforced_keys:
            return dataset
        stored = None
        with contextlib.suppress(Exception):
            stored = await self.get_dataset(dataset.name, data_source=dataset.data_source)
        if stored is None:
            return dataset
        if (stored.sql_table, stored.sql, stored.source_queries) != (
                dataset.sql_table, dataset.sql, dataset.source_queries):
            return dataset.model_copy(update={"enforced_keys": []})
        kept = [key for key in dataset.enforced_keys if not any(
            _repointed_key_field(old=stored.get_field(name), new=dataset.get_field(name)) for name in key)]
        return dataset if kept == dataset.enforced_keys else dataset.model_copy(update={"enforced_keys": kept})

    async def _owner_of(self, name: str, *, data_source: str) -> str | None:
        """The one owner lookup: the document declaring dataset ``name``, ``None`` for a standalone or new one."""
        if not self._supports_semantic_models() or not await self._document_data_sources():
            return None
        return (await self._document_index(data_source)).owners.get(name.casefold())

    async def save_dataset_extensions(self, dataset: SlayerDataset) -> None:
        """Write a document dataset's SLayer content as ``SLAYER`` entries, leaving its document's Ossie core as it is."""
        check_whole(dataset)
        dataset = await self._without_repointed_enforced_keys(dataset)
        check_enforced_keys(dataset)
        owner = await self._owner_of(dataset.name, data_source=dataset.data_source)
        if owner is None:
            raise ValueError(f"Dataset {dataset.name!r} is not declared by a semantic model")
        await self._write_document_dataset(data_source=dataset.data_source, document=owner, name=dataset.name,
                                           mutate=lambda _old: dataset, core=False)

    async def _write_document_dataset(self, *, data_source: str, document: str, name: str,
                                      mutate: Callable[[SlayerDataset | None], SlayerDataset | None],
                                      core: bool = True) -> None:
        """Patch document ``document``: dataset ``name`` becomes ``mutate(current)`` (``None`` removes it), read and
        written as one serialized step."""
        datasource = await self.get_datasource(data_source)
        ds_type, granularities = (datasource.type, datasource.granularities) if datasource is not None else (None, [])
        reader = dialect_for_ds_type(ds_type).sqlglot_name

        def patch(text: str) -> str:
            current = decompose(text=text, name=document, data_source=data_source, ds_type=ds_type,
                                granularities=granularities)
            old = next((d for d in current.datasets if d.name.casefold() == name.casefold()), None)
            new = mutate(old)
            base = (new._read_as if new is not None and new._read_as is not None else None) or old
            baseline = with_canonical_spellings(base)[0] if base is not None else None

            def edit(doc: dict[str, Any]) -> dict[str, Any]:
                out = without_dataset(doc, name=name, reads=_reads) if new is None else with_dataset(
                    doc, old=baseline, new=new, reader=reader, core=core)
                reasons = ossie_validity_errors(out)
                if reasons:
                    raise OssieDocumentInvalidError(name=document, reasons=reasons)
                return out

            return edit_text(text, edit)

        if self._supports_semantic_models() and await self.get_semantic_model_text(document, data_source=data_source) is None:
            raise ValueError(f"Semantic model {document!r} not found in datasource {data_source!r}")
        await self._patch_semantic_model_text(data_source=data_source, name=document, patch=patch)

    async def validate_for_save(self, dataset: SlayerDataset, *, failures: DocumentLoadFailures | None = None) -> None:
        """Reserved-name and case-collision rejection, derived-field and dataset-filter well-formedness (reference arity + cycles) and relationship validation against this store's peers; then a warning per dependent the save breaks, unless ``failures`` is unreported."""
        identities = await self._list_all_dataset_identities()
        if is_spine_spelling(dataset.name) and (dataset.data_source, dataset.name) not in identities:
            raise ReservedDatasetNameError(name=dataset.name)
        _refuse_colon_metrics(dataset)
        check_enforced_keys(dataset)
        datasource = await self.get_datasource(dataset.data_source) if dataset.data_source else None
        stored = await self.get_dataset(dataset.name, data_source=dataset.data_source)
        if datasource is not None and not dataset.source_queries:
            check_forms(dataset, ds_type=datasource.type, stored=stored)
        if self._ids_collide_as_filenames:
            await self._check_dataset_identity_collision(dataset)
        await self._check_case_collisions(dataset)
        # A skipped (unloadable) peer is invisible to these checks; its edge names go unchecked.
        failures = failures or DocumentLoadFailures()
        loaded, unloaded = await self.load_datasets(data_source=dataset.data_source, exclude=dataset.name)
        peers = {m.name: m for m in failures.skip((loaded, unloaded))}
        check_relationship_target_spellings(dataset, names=[n for ds, n in identities if ds == dataset.data_source], peers=peers)
        if datasource is not None and not dataset.source_queries:
            check_portable_fields(dataset, datasets=list(peers.values()), ds_type=datasource.type)
        await self._validate_mode_a_placeholders(dataset)
        check_field_agg(dataset)
        validate_derived_fields(dataset=dataset, peers=peers, unloaded=frozenset(e.name for e in unloaded))
        check_dataset_filters(dataset=dataset, reachable=peers,
                              dialect=ossie_token(datasource.type) if datasource is not None else None)
        check_evaluated_metrics(dataset=dataset, reachable=peers, stored=stored, model_metrics={
            m.name for m in await self.list_model_scoped_metrics(dataset.data_source)})
        self._validate_relationship_edges(dataset, peers=peers)
        await self._validate_aggregations(dataset)
        await self._validate_field_granularities(dataset)
        _validate_default_time_dimension(dataset)
        if failures.report:
            warn_broken_dependents(dataset=dataset, stored=stored, peers=peers)
            warn_broken_model_metrics(dataset=dataset, stored=stored,
                                      metrics=await self.list_model_scoped_metrics(dataset.data_source))

    async def _check_case_collisions(self, dataset: SlayerDataset) -> None:
        """Refuse a name that newly collides by case with another in its namespace (the datasource's dataset names
        included); a collision the stored document already holds is grandfathered, with a warning."""
        identities = await self._list_all_dataset_identities()
        peers = [n for d, n in identities if d == dataset.data_source]
        new = [g for g in folded_collisions([*peers, dataset.name]) if dataset.name in g] if dataset.name not in peers else []
        if new:
            raise CaseCollisionError(names=new[0], namespace=f"datasource {dataset.data_source!r}'s datasets")
        stored = None
        with contextlib.suppress(Exception):
            stored = await self.get_dataset(dataset.name, data_source=dataset.data_source)
        held = {(w, tuple(g)) for w, g in case_collisions(stored)} if stored is not None else set()
        for where, group in case_collisions(dataset):
            if (where, tuple(group)) not in held:
                raise CaseCollisionError(names=group, namespace=f"dataset {dataset.name!r}'s {where}")
        for payload in [*name_collision_warnings(dataset), *spine_variant_warnings(dataset)]:
            warnings.warn(SlayerNameCollisionWarning(payload), stacklevel=outside_stacklevel())

    async def _validate_mode_a_placeholders(self, dataset: SlayerDataset) -> None:
        ds = await self.get_datasource(dataset.data_source) if dataset.data_source else None
        check_placeholders(mode_a_texts(dataset), dialect=dialect_for_ds_type(ds.type if ds else None))

    async def _validate_field_granularities(self, dataset: SlayerDataset) -> None:
        named = [(c, c.granularity) for c in dataset.fields if c.granularity is not None and not isinstance(c.granularity, TimeGranularity)]
        if not named:
            return
        ds = await self.get_datasource(dataset.data_source) if dataset.data_source else None
        defined = ds.granularity_definitions if ds is not None else {}
        for field, granularity in named:
            if resolve_granularity(granularity, defined=defined) is None:
                raise UnknownGranularityError(summary=unknown_granularity_message(
                    name=str(granularity), defined=defined.values(),
                    where=f"field {field.name!r} of dataset {dataset.name!r}",
                ))

    async def _validate_aggregations(self, dataset: SlayerDataset) -> None:
        if not dataset.aggregations:
            return
        ds = await self.get_datasource(dataset.data_source) if dataset.data_source else None
        dialect = dialect_for_ds_type(ds.type).sqlglot_name if ds else ""
        for agg in dataset.aggregations:
            check_aggregation_definition(
                where=f"Dataset '{dataset.name}', aggregation '{agg.name}'", agg=agg, dialect=dialect,
            )
            if agg.name.lower() in RESERVED_NAMES:
                raise ReservedFunctionNameError(name=agg.name, where=f"Dataset '{dataset.name}', aggregation")

    @staticmethod
    def _validate_relationship_edges(dataset: SlayerDataset, *, peers: dict[str, SlayerDataset]) -> None:
        """Save-time relationship validation against the loaded datasource ``peers``: reject duplicate incident edge
        names and exact-inverse re-declarations; warn on unnamed parallel edges."""
        _check_list_hop_collisions(dataset=dataset, peers=peers)
        if not dataset.relationships:
            return
        names = _checked_relationship_names(dataset)
        # Relationship targets always; every peer when a named edge must be checked against incident edges.
        if not names:
            targets = {j.to for j in dataset.relationships}
            peers = {n: p for n, p in peers.items() if n in targets}
        _check_edges_against_peers(dataset=dataset, peers=peers)
        _warn_unnamed_parallel_edges(dataset=dataset, peers=peers)

    async def _check_dataset_identity_collision(self, dataset: SlayerDataset) -> None:
        """Reject a dataset whose ``data_source`` or ``name`` case-collides with an existing one (both are YAML filename components)."""
        identities = await self._list_all_dataset_identities()
        known_ds = {ds for ds, _ in identities}
        known_ds.update(await self.list_datasources())
        collide = _find_case_colliding_id(
            candidate=dataset.data_source, existing=known_ds,
        )
        if collide is not None:
            raise IdCollisionError(
                kind="datasource", new_id=dataset.data_source, existing_id=collide,
            )
        names_in_ds = [n for ds, n in identities if ds == dataset.data_source]
        collide = _find_case_colliding_id(
            candidate=dataset.name, existing=names_in_ds,
        )
        if collide is not None:
            raise IdCollisionError(
                kind="dataset",
                new_id=dataset.name,
                existing_id=collide,
                data_source=dataset.data_source,
            )

    async def builtin_datasets(
        self, data_source: str, *, detailed: bool = False, failures: DocumentLoadFailures | None = None,
    ) -> list[SlayerDataset]:
        """The datasource's built-in datasets (the time spine, unless a stored dataset shadows it);
        ``detailed`` describes its wiring, else its description points at ``inspect``. Empty for an unknown datasource."""
        if await self.get_datasource(data_source) is None:
            return []
        if (data_source, TIME_SPINE_DATASET) in await self._list_all_dataset_identities():
            return []
        if not detailed:
            return [spine_dataset(data_source=data_source)]
        peers = (failures or DocumentLoadFailures()).skip(await self.load_datasets(data_source=data_source))
        return [spine_dataset(data_source=data_source, wired=peers)]

    async def load_datasets(self, *, data_source: str | None = None, exclude: str | None = None) -> Loaded[SlayerDataset]:
        """Every stored dataset (of ``data_source`` when given, bar ``exclude``), and the ones that failed to load."""
        sem = asyncio.Semaphore(_DATASET_LOAD_CONCURRENCY)

        async def _load(ds: str, name: str) -> SlayerDataset | StoredDocumentLoadError | None:
            async with sem:
                try:
                    return await self.get_dataset(name, data_source=ds)
                except StoredDocumentLoadError as exc:
                    return exc

        indexes = {ds: await self._document_index(ds) for ds in await self._document_data_sources()
                   if data_source is None or ds == data_source}
        token = _RAW_PASS.set({}) if _RAW_PASS.get() is None else None
        try:
            with one_upgrade_pass():
                loaded = await asyncio.gather(*(
                    _load(ds, name) for ds, name in await self._standalone_identities()
                    if (data_source is None or ds == data_source) and name != exclude
                    and (ds not in indexes or name.casefold() not in indexes[ds].quarantined)
                ))
        finally:
            if token is not None:
                _RAW_PASS.reset(token)
        for index in indexes.values():
            loaded += [_handed_out(d) for d in index.datasets.values() if d.name != exclude]
            loaded += index.failures
        return (
            [m for m in loaded if isinstance(m, SlayerDataset)],
            [e for e in loaded if isinstance(e, StoredDocumentLoadError)],
        )

    # ---- semantic-model documents ------------------------------------------

    def _supports_semantic_models(self) -> bool:
        return type(self)._get_semantic_model_text_impl is not StorageBackend._get_semantic_model_text_impl

    def _unsupported(self) -> SemanticModelsUnsupportedError:
        return SemanticModelsUnsupportedError(backend=type(self).__name__)

    async def _save_semantic_model_text_impl(self, *, data_source: str, name: str, text: str) -> None:
        """Durably store a document's text verbatim (backends implement this to support documents)."""
        raise self._unsupported()

    async def _get_semantic_model_text_impl(self, *, data_source: str, name: str) -> str | None:
        """A stored document's text, ``None`` when absent."""
        raise self._unsupported()

    async def _list_semantic_model_identities(self) -> list[tuple[str, str]]:
        """Every stored ``(data_source, name)`` document; a backend without documents lists none."""
        await asyncio.sleep(0)
        return []

    async def _delete_semantic_model_row(self, *, data_source: str, name: str) -> bool:
        raise self._unsupported()

    async def _patch_semantic_model_text(self, *, data_source: str, name: str, patch: Callable[[str], str]) -> str:
        """Read, ``patch`` and write a document as one serialized step; the written text. Every document content or
        membership change must change ``graph_fingerprint``."""
        raise self._unsupported()

    async def _standalone_identities(self) -> list[tuple[str, str]]:
        token = _STANDALONE_ONLY.set(True)
        try:
            return await self._list_all_dataset_identities()
        finally:
            _STANDALONE_ONLY.reset(token)

    async def _document_data_sources(self) -> list[str]:
        if not self._supports_semantic_models():
            return []
        return sorted({ds for ds, _ in await self._list_semantic_model_identities()})

    async def _document_index(self, data_source: str) -> DocumentIndex:
        """``data_source``'s documents resolved against its namespace."""
        names = [n for ds, n in await self._list_semantic_model_identities() if ds == data_source] \
            if self._supports_semantic_models() else []
        if not names:
            return DocumentIndex(data_source=data_source)
        documents, unreadable = [], []
        for name in names:
            try:
                text = await self._get_semantic_model_text_impl(data_source=data_source, name=name)
            except UnreadableDocumentError as exc:
                unreadable.append(StoredDocumentLoadError(kind="semantic_model", name=name, data_source=data_source,
                                                          cause=exc))
                continue
            if text is not None:
                documents.append((name, text))
        standalone = [n for ds, n in await self._standalone_identities() if ds == data_source]
        datasource = await self.get_datasource(data_source)
        index = build_index(data_source=data_source, documents=documents, standalone=standalone,
                            ds_type=datasource.type if datasource is not None else None,
                            granularities=datasource.granularities if datasource is not None else [])
        index.failures.extend(unreadable)
        return index

    async def _readable_text(self, *, data_source: str, name: str) -> str | None:
        """A stored document's text; ``None`` when absent or unreadable."""
        try:
            return await self._get_semantic_model_text_impl(data_source=data_source, name=name)
        except UnreadableDocumentError:
            return None

    async def _document_dataset(self, name: str, *, data_source: str | None) -> SlayerDataset | None:
        """The document dataset ``name`` (case-insensitive), ``None`` when no document declares it; raises
        ``StoredDocumentLoadError`` when the name is quarantined."""
        if data_source is None:
            if not await self._document_data_sources():
                return None
            identity = await self.resolve_dataset_identity(name)
            if identity is None:
                return None
            data_source, name = identity
        index = await self._document_index(data_source)
        cause = index.quarantined.get(name.casefold())
        if cause is not None:
            raise StoredDocumentLoadError(kind="dataset", name=name, data_source=data_source, cause=ValueError(
                f"{name!r} is declared by {cause}; rename one of them"))
        found = index.datasets.get(name.casefold())
        return _handed_out(found) if found is not None else None

    async def _resolve_document(self, name: str, data_source: str | None) -> tuple[str, str] | None:
        _validate_path_component(name, kind=SEMANTIC_MODEL_NAME)
        if data_source is not None:
            _validate_path_component(data_source, kind=DATASOURCE_NAME)
            return data_source, name
        matches = [(ds, n) for ds, n in await self._list_semantic_model_identities() if n == name]
        if len(matches) > 1:
            raise AmbiguousDatasetError(name=name, candidates=[ds for ds, _ in matches])
        return matches[0] if matches else None

    async def list_semantic_models(self, data_source: str | None = None) -> list[tuple[str, str]]:
        """Every stored document as ``(data_source, name)``, of ``data_source`` when given."""
        if not self._supports_semantic_models():
            return []
        return sorted(i for i in await self._list_semantic_model_identities() if data_source in (None, i[0]))

    async def get_semantic_model_text(self, name: str, data_source: str | None = None) -> str | None:
        """A stored document's text as written; ``None`` when absent."""
        if not self._supports_semantic_models():
            raise self._unsupported()
        target = await self._resolve_document(name, data_source)
        return await self._get_semantic_model_text_impl(data_source=target[0], name=target[1]) if target else None

    async def save_semantic_model_text(self, *, data_source: str, name: str, text: str,
                                       warnings_out: list[SlayerWarning] | None = None) -> None:
        """Store ``text`` verbatim as document ``name`` of ``data_source`` after the Ossie validity and namespace checks;
        replaces a stored document of that name. Its datasets' ontology mismatches go to ``warnings_out``."""
        datasets = await self._store_semantic_model_text(data_source=data_source, name=name, text=text)
        report_save_warnings(warnings_out, await self.ontology_mismatches(data_source=data_source, datasets=datasets))

    async def _store_semantic_model_text(self, *, data_source: str, name: str, text: str) -> list[str]:
        """``save_semantic_model_text`` without its warnings; the stored document's dataset names."""
        _validate_path_component(data_source, kind=DATASOURCE_NAME)
        _validate_path_component(name, kind=SEMANTIC_MODEL_NAME)
        if not self._supports_semantic_models():
            raise self._unsupported()
        doc = checked_document(text=text, name=name)
        await self._check_document_namespace(
            name=name, data_source=data_source, datasets=[d.name for d in doc.datasets],
            metrics=[m.name for m in doc.metrics or []],
        )
        if await self.list_ontology_mappings(data_source):
            datasets = await self._document_datasets(data_source=data_source, name=name, text=text)
            await self._guard_mappings(change=f"Saving semantic model {name!r}", data_source=data_source,
                                       changed=lambda scope: scope.with_document(name, datasets))
        await self._save_semantic_model_text_impl(data_source=data_source, name=name, text=text)
        return [d.name for d in doc.datasets]

    async def adopt_semantic_model(self, *, data_source: str, name: str, text: str, replacing: list[str],
                                   warnings_out: list[SlayerWarning] | None = None) -> None:
        """Store document ``name`` in place of the standalone datasets ``replacing`` (same names, so references keep
        resolving); the standalone datasets are restored when the document is refused."""
        replaced = [d for n in replacing if (d := await self.get_dataset(n, data_source=data_source)) is not None]
        stored: list[str] = []

        async def write() -> None:
            stored.extend(await self._store_semantic_model_text(data_source=data_source, name=name, text=text))

        await self._moving_standalone(data_source=data_source, datasets=replaced, write=write)
        report_save_warnings(warnings_out, await self.ontology_mismatches(data_source=data_source, datasets=stored))

    async def _moving_standalone(self, *, data_source: str, datasets: list[SlayerDataset],
                                 write: Callable[[], Awaitable[Any]]) -> None:
        """Delete ``datasets``' standalone rows, then ``write`` the document taking them; any failure restores them."""
        try:
            for dataset in datasets:
                await self._delete_dataset_row(data_source=data_source, name=dataset.name)
            await write()
        except BaseException:  # cancellation too: the moved rows must come back
            for dataset in datasets:
                await self._save_dataset_impl(dataset)
            raise

    async def standalone_datasets_of(self, *, text: str, data_source: str, names: list[str]) -> list[SlayerDataset]:
        """Datasets ``names`` of document ``text`` (its ``SLAYER`` entries applied) as unsaved standalone datasets."""
        name = str((parse_document_text(text) or {}).get("name"))
        checked_document(text=text, name=name)
        datasource = await self.get_datasource(data_source)
        decomposed = decompose(text=text, name=name, data_source=data_source,
                               ds_type=datasource.type if datasource is not None else None,
                               granularities=datasource.granularities if datasource is not None else [])
        wanted = {n.casefold() for n in names}
        return [d.model_copy(deep=True, update={"semantic_model": None})
                for d in decomposed.datasets if d.name.casefold() in wanted]

    async def _check_document_namespace(self, *, name: str, data_source: str, datasets: list[str],
                                        metrics: list[str], moving: AbstractSet[str] = frozenset()) -> None:
        """Refuse a dataset or model-scoped metric name the datasource already uses elsewhere, in any case; ``moving``
        names standalone datasets this write moves into the document."""
        owner = f"semantic model {name!r}"
        taken: dict[str, str] = {n.casefold(): f"standalone dataset {n!r}"
                                 for ds, n in await self._standalone_identities()
                                 if ds == data_source and n.casefold() not in moving}
        others = [(n, t) for n, t in [
            (n, await self._readable_text(data_source=data_source, name=n))
            for ds, n in await self._list_semantic_model_identities() if ds == data_source and n != name] if t]
        datasource = await self.get_datasource(data_source)
        other_metrics: dict[str, str] = {}
        for other, text in others:
            with contextlib.suppress(Exception):
                index = build_index(data_source=data_source, documents=[(other, text)], standalone=[],
                                    ds_type=datasource.type if datasource else None,
                                    granularities=datasource.granularities if datasource else [])
                taken.update({k: f"dataset {d.name!r} of semantic model {other!r}" for k, d in index.datasets.items()})
                other_metrics.update({k: f"metric {m.name!r} of semantic model {other!r}"
                                      for k, m in index.metrics.items()})
        for dataset in datasets:
            if dataset.casefold() in taken:
                raise NamespaceCollisionError(name=dataset, owner=owner, existing=taken[dataset.casefold()],
                                              data_source=data_source)
        own = {d.casefold(): f"dataset {d!r} of {owner}" for d in datasets}
        for metric in metrics:
            existing = other_metrics.get(metric.casefold()) or taken.get(metric.casefold()) or own.get(metric.casefold())
            if existing is not None:
                raise NamespaceCollisionError(name=metric, owner=owner, existing=existing, data_source=data_source)

    async def create_semantic_model(self, *, data_source: str, name: str | None = None, content: str | dict[str, Any] | None = None,
                                    datasets: list[str] | None = None,
                                    warnings_out: list[SlayerWarning] | None = None) -> None:
        """Create document ``name`` (default: the content's own ``name``) from Ossie ``content`` (text or mapping), or
        from the standalone ``datasets``, which move into it under the same names."""
        if (content is None) == (datasets is None):
            raise ValueError("Give a semantic model either its Ossie content or the standalone datasets it takes")
        if content is not None:
            text = content if isinstance(content, str) else yaml.safe_dump(content, sort_keys=False, allow_unicode=True)
            name = name or _document_name(text)
            await self.save_semantic_model_text(data_source=data_source, name=name, text=text,
                                                warnings_out=warnings_out)
            return
        if name is None:
            raise ValueError("A semantic model made from standalone datasets needs a name")
        moved = await self._standalone_members(datasets or [], data_source=data_source)
        reader = await self._reader(data_source)
        doc = with_datasets({"version": OSSIE_VERSION, "name": name, "datasets": []}, moved, reader=reader)
        reasons = ossie_validity_errors(doc)
        if reasons:
            raise OssieDocumentInvalidError(name=name, reasons=reasons)
        await self.adopt_semantic_model(data_source=data_source, name=name, replacing=[d.name for d in moved],
                                        text=yaml.safe_dump(doc, sort_keys=False, allow_unicode=True),
                                        warnings_out=warnings_out)

    async def _standalone_members(self, names: list[str], *, data_source: str) -> list[SlayerDataset]:
        out = []
        for name in names:
            if await self._owner_of(name, data_source=data_source) is not None:
                raise ValueError(f"Dataset {name!r} already belongs to a semantic model")
            dataset = await self.get_dataset(name, data_source=data_source)
            if dataset is None:
                raise ValueError(f"Dataset {name!r} not found in datasource {data_source!r}")
            out.append(dataset)
        return out

    async def _reader(self, data_source: str) -> str:
        datasource = await self.get_datasource(data_source)
        return dialect_for_ds_type(datasource.type if datasource is not None else None).sqlglot_name

    async def edit_semantic_model(self, name: str, *, data_source: str, description: Any = UNSET,
                                  ai_context: Any = UNSET, metrics: list[dict[str, Any]] | None = None,
                                  remove_metrics: list[str] | None = None,
                                  relationships: list[dict[str, Any]] | None = None,
                                  remove_relationships: list[str] | None = None,
                                  add_datasets: list[str] | None = None,
                                  warnings_out: list[SlayerWarning] | None = None) -> None:
        """Edit document ``name`` in place: its description and AI context, upserted or removed model-scoped metrics and
        relationships, and standalone datasets moved into it. Every model-scoped metric written is checked first."""
        text = await self.get_semantic_model_text(name, data_source=data_source)
        if text is None:
            raise ValueError(f"Semantic model {name!r} not found in datasource {data_source!r}")
        moved = await self._standalone_members(add_datasets or [], data_source=data_source)
        datasource = await self.get_datasource(data_source)
        ds_type = datasource.type if datasource is not None else None
        reader = dialect_for_ds_type(ds_type).sqlglot_name
        specs = [metric_spec(m) for m in metrics or []]
        current = decompose(text=text, name=name, data_source=data_source, ds_type=ds_type,
                            granularities=datasource.granularities if datasource is not None else [])
        members = [*current.datasets, *moved]
        other_metrics = [m for m in await self.list_model_scoped_metrics(data_source) if m.semantic_model != name]
        others = [m.name for m in other_metrics]
        kept = [m.name for m in current.metrics if m.name.casefold() not in {
            n.casefold() for n in [*(remove_metrics or []), *(s["name"] for s in specs)]}]
        kept_metrics = [m for m in current.metrics if m.name in kept]
        scope = DocumentScope(
            fields={d.name.casefold(): {f.name.casefold() for f in d.fields} for d in members},
            dataset_metrics={d.name.casefold(): {m.name.casefold() for m in d.metrics} for d in members},
            datasource_datasets={n.casefold() for ds, n in await self._list_all_dataset_identities() if ds == data_source},
            model_metrics={n.casefold() for n in [*others, *kept, *(s["name"] for s in specs)]},
            # Chain warnings may run through a metric this edit replaces; the graph re-derives the live ones.
            unevaluable=unevaluable_targets(
                [*other_metrics, *(m for m in kept_metrics if m.name.casefold() not in current.chained)],
                datasets=members),
            metric_graph=metric_graph([*other_metrics, *kept_metrics]),
        )
        written = [MetricSpec(s, slayer_only=check_model_metric(name=s["name"], text=MetricSpec(s, slayer_only=False).text,
                                                                scope=scope)) for s in specs]
        dependencies = {m.name.casefold(): expression_datasets(m.expression)[1] for m in current.metrics
                        if m.name.casefold() in {k.casefold() for k in kept}}
        dependencies.update({w.name.casefold(): expression_datasets(w.text)[1] for w in written})
        metric_cycle({k: v & set(dependencies) for k, v in dependencies.items()})
        await self._check_document_namespace(
            name=name, data_source=data_source, datasets=[d.name for d in members],
            metrics=[*kept, *(w.name for w in written)], moving={d.name.casefold() for d in moved})

        def patch(stored: str) -> str:
            def edit(doc: dict[str, Any]) -> dict[str, Any]:
                out = edited_document(with_datasets(doc, moved, reader=reader), description=description,
                                      ai_context=ai_context, metrics=written, remove_metrics=remove_metrics or [],
                                      relationships=relationships or [],
                                      remove_relationships=remove_relationships or [])
                reasons = ossie_validity_errors(out)
                if reasons:
                    raise OssieDocumentInvalidError(name=name, reasons=reasons)
                return out

            return edit_text(stored, edit)

        await self._moving_standalone(data_source=data_source, datasets=moved, write=lambda: self._patch_semantic_model_text(
            data_source=data_source, name=name, patch=patch))
        report_save_warnings(warnings_out, await self.ontology_mismatches(
            data_source=data_source, datasets=[d.name for d in members]))

    async def delete_semantic_model(self, name: str, data_source: str | None = None) -> bool:
        """Delete a document, cascading every member's embeddings and memory entities; ``False`` when absent."""
        if not self._supports_semantic_models():
            raise self._unsupported()
        target = await self._resolve_document(name, data_source)
        if target is None:
            return False
        ds, name = target
        index = await self._document_index(ds)
        members = [d.name for k, d in index.datasets.items() if index.owners.get(k) == name]
        metrics = [m.name for m in index.metrics.values() if m.semantic_model == name]
        await self._guard_mappings(change=f"Deleting semantic model {name!r}", data_source=ds,
                                   changed=lambda scope: scope.with_document(name, None))
        if not await self._delete_semantic_model_row(data_source=ds, name=name):
            return False
        for entity in [*members, *metrics]:
            await self.delete_embeddings_for_canonical(canonical_id_prefix=f"{ds}.{entity}")
        for member in members:
            await self.strip_dangling_entities_from_memories(canonical_id=f"{ds}.{member}")
        return True

    async def semantic_model_owner(self, name: str, data_source: str) -> str | None:
        """The document declaring dataset ``name``, ``None`` for a standalone or absent one."""
        return (await self._document_index(data_source)).owners.get(name.casefold())

    async def list_model_scoped_metrics(self, data_source: str) -> list[ModelScopedMetric]:
        """``data_source``'s loadable model-scoped metrics, in document order."""
        return [m.model_copy(deep=True) for m in (await self._document_index(data_source)).metrics.values()]

    # ---- ontology and mapping documents ------------------------------------

    def _supports_ontologies(self) -> bool:
        return type(self)._get_ontology_text_impl is not StorageBackend._get_ontology_text_impl

    def _ontologies_unsupported(self) -> OntologiesUnsupportedError:
        return OntologiesUnsupportedError(backend=type(self).__name__)

    def _require_ontologies(self) -> None:
        if not self._supports_ontologies():
            raise self._ontologies_unsupported()

    async def _save_ontology_text_impl(self, *, name: str, text: str) -> None:
        """Durably store an ontology's text verbatim (backends implement this to support ontologies)."""
        raise self._ontologies_unsupported()

    async def _get_ontology_text_impl(self, *, name: str) -> str | None:
        raise self._ontologies_unsupported()

    async def _list_ontology_names(self) -> list[str]:
        """Every stored ontology name; a backend without ontologies lists none. Every ontology or mapping change must
        change ``graph_fingerprint``."""
        await asyncio.sleep(0)
        return []

    async def _delete_ontology_row(self, *, name: str) -> bool:
        raise self._ontologies_unsupported()

    async def _save_ontology_mapping_text_impl(self, *, data_source: str, name: str, text: str) -> None:
        raise self._ontologies_unsupported()

    async def _get_ontology_mapping_text_impl(self, *, data_source: str, name: str) -> str | None:
        raise self._ontologies_unsupported()

    async def _list_ontology_mapping_identities(self) -> list[tuple[str, str]]:
        await asyncio.sleep(0)
        return []

    async def _delete_ontology_mapping_row(self, *, data_source: str, name: str) -> bool:
        raise self._ontologies_unsupported()

    async def list_ontologies(self) -> list[str]:
        """Every stored ontology's name."""
        return sorted(await self._list_ontology_names()) if self._supports_ontologies() else []

    async def get_ontology_text(self, name: str) -> str | None:
        """A stored ontology's text as written; ``None`` when absent."""
        self._require_ontologies()
        _validate_path_component(name, kind=ONTOLOGY_NAME)
        return await self._get_ontology_text_impl(name=name)

    async def save_ontology_text(self, *, name: str, text: str,
                                 warnings_out: list[SlayerWarning] | None = None) -> None:
        """Store ontology ``name`` verbatim after its checks; refused when it would break a stored mapping. The
        ontology mismatches of the mappings using it go to ``warnings_out``."""
        _validate_path_component(name, kind=ONTOLOGY_NAME)
        self._require_ontologies()
        data, reasons = read_ontology(text)
        reasons = reasons or stored_name_reasons(data, name=name)
        if isinstance(data, dict) and "ontology" in data:
            await self._guard_mappings(change=f"Saving ontology {name!r}", own=reasons,
                                       changed=lambda scope: scope.with_ontology(name, data))
        if reasons:
            raise OntologyDocumentInvalidError(name=name, reasons=reasons)
        await self._save_ontology_text_impl(name=name, text=text)
        report_save_warnings(warnings_out, await self.ontology_mismatches(ontology=name))

    async def delete_ontology(self, name: str) -> bool:
        """Delete ontology ``name``; refused while a stored mapping resolves against it; ``False`` when absent."""
        self._require_ontologies()
        _validate_path_component(name, kind=ONTOLOGY_NAME)
        if await self._get_ontology_text_impl(name=name) is None:
            return False
        await self._guard_mappings(change=f"Deleting ontology {name!r}",
                                   changed=lambda scope: scope.with_ontology(name, None))
        return await self._delete_ontology_row(name=name)

    async def edit_ontology(self, name: str, *, warnings_out: list[SlayerWarning] | None = None, **edits: Any) -> None:
        """Apply ``edited_ontology``'s edits to ontology ``name`` and save it through every check."""
        text = await self.get_ontology_text(name)
        if text is None:
            raise ValueError(f"Ontology {name!r} not found")
        await self.save_ontology_text(name=name, text=edited_ontology(text, **edits), warnings_out=warnings_out)

    async def list_ontology_mappings(self, data_source: str | None = None) -> list[tuple[str, str]]:
        """Every stored mapping as ``(data_source, name)``, of ``data_source`` when given."""
        if not self._supports_ontologies():
            return []
        return sorted(i for i in await self._list_ontology_mapping_identities() if data_source in (None, i[0]))

    async def get_ontology_mapping_text(self, name: str, data_source: str) -> str | None:
        """A stored mapping's text as written; ``None`` when absent."""
        self._require_ontologies()
        _validate_path_component(data_source, kind=DATASOURCE_NAME)
        _validate_path_component(name, kind=MAPPING_NAME)
        return await self._get_ontology_mapping_text_impl(data_source=data_source, name=name)

    async def save_ontology_mapping_text(self, *, data_source: str, name: str, text: str,
                                         warnings_out: list[SlayerWarning] | None = None) -> list[str]:
        """Store mapping ``name`` of ``data_source`` verbatim once it resolves; the informational notes of the save.
        Its ontology mismatches go to ``warnings_out``."""
        _validate_path_component(data_source, kind=DATASOURCE_NAME)
        _validate_path_component(name, kind=MAPPING_NAME)
        self._require_ontologies()
        if await self.get_datasource(data_source) is None:
            raise ValueError(f"Datasource {data_source!r} not found; a mapping belongs to a stored datasource")
        data = checked_mapping_shape(text, name=name, data_source=data_source)
        resolution = check_mapping(data, scope=await self._mapping_scope(data_source))
        if resolution.problems:
            raise OntologyMappingInvalidError(name=name, data_source=data_source,
                                              reasons=[str(p) for p in resolution.problems])
        await self._save_ontology_mapping_text_impl(data_source=data_source, name=name, text=text)
        report_save_warnings(warnings_out, await self.ontology_mismatches(data_source=data_source, mapping=name))
        note = resolution.note(name)
        return [note] if note else []

    async def edit_ontology_mapping(self, name: str, *, data_source: str,
                                    warnings_out: list[SlayerWarning] | None = None, **edits: Any) -> list[str]:
        """Apply ``edited_mapping``'s edits to mapping ``name`` of ``data_source`` and save it through every check."""
        text = await self.get_ontology_mapping_text(name, data_source)
        if text is None:
            raise ValueError(f"Mapping {name!r} not found in datasource {data_source!r}")
        return await self.save_ontology_mapping_text(data_source=data_source, name=name,
                                                     text=edited_mapping(text, **edits), warnings_out=warnings_out)

    async def delete_ontology_mapping(self, name: str, data_source: str) -> bool:
        """Delete a stored mapping; ``False`` when absent."""
        self._require_ontologies()
        _validate_path_component(data_source, kind=DATASOURCE_NAME)
        _validate_path_component(name, kind=MAPPING_NAME)
        return await self._delete_ontology_mapping_row(data_source=data_source, name=name)

    async def ontology_mapping_problems(self, name: str, data_source: str) -> list[str]:
        """Why stored mapping ``name`` of ``data_source`` does not resolve now; empty when it does."""
        text = await self.get_ontology_mapping_text(name, data_source)
        if text is None:
            raise ValueError(f"Mapping {name!r} not found in datasource {data_source!r}")
        return await self._stored_mapping_problems(text, data_source=data_source, name=name)

    async def _stored_mapping_problems(self, text: str, *, data_source: str, name: str,
                                       scope: MappingScope | None = None) -> list[str]:
        try:
            data = checked_mapping_shape(text, name=name, data_source=data_source)
        except OntologyMappingInvalidError as exc:
            return exc.reasons
        resolution = check_mapping(data, scope=scope or await self._mapping_scope(data_source))
        return [str(p) for p in resolution.problems]

    async def load_ontology_mappings(self, *, data_source: str | None = None) -> Loaded[StoredMapping]:
        """Every stored mapping that resolves (of ``data_source`` when given), and the ones that do not."""
        loaded: list[StoredMapping] = []
        errors: list[StoredDocumentLoadError] = []
        scopes: dict[str, MappingScope] = {}
        for ds, name in await self.list_ontology_mappings(data_source):
            text = await self._get_ontology_mapping_text_impl(data_source=ds, name=name)
            if text is None:
                continue
            if ds not in scopes:
                scopes[ds] = await self._mapping_scope(ds)
            problems = await self._stored_mapping_problems(text, data_source=ds, name=name, scope=scopes[ds])
            if problems:
                errors.append(StoredDocumentLoadError(kind="ontology_mapping", name=name, data_source=ds,
                                                      cause=ValueError("; ".join(problems))))
            else:
                loaded.append(StoredMapping(name=name, data_source=ds, text=text))
        return loaded, errors

    async def ontology_use(self, data_source: str) -> StoredUse:
        """``data_source``'s mappings, the ontologies and semantic models they read, as this storage's caller sees
        them."""
        loaded, errors = await self.load_ontology_mappings(data_source=data_source)
        ontologies = {n: data for n in await self.list_ontologies()
                      if isinstance(data := stored_data(await self.get_ontology_text(n)), dict)}
        datasets, _ = await self.load_datasets(data_source=data_source)
        documents = {n: [d for d in datasets if d.semantic_model == n]
                     for _, n in await self.list_semantic_models(data_source)}
        unresolved = []
        for error in errors:
            data = stored_data(await self.get_ontology_mapping_text(error.name, data_source))
            unresolved.append(UnresolvedMapping(name=error.name, ontology=ref_name(data, "ontology_ref"),
                                                problems=[error.cause_text]))
        mappings = [MappingDocument(name=m.name, data=data) for m in loaded
                    if isinstance(data := stored_data(m.text), dict)]
        return StoredUse(data_source=data_source, ontologies=ontologies, documents=documents, datasets=datasets,
                         mappings=mappings, unresolved=unresolved)

    async def ontology_mismatches(self, *, data_source: str | None = None, mapping: str | None = None,
                                  ontology: str | None = None,
                                  datasets: Iterable[str] | None = None) -> list[OntologyMismatchWarning]:
        """The identifier and multiplicity mismatches of the stored mappings (of ``data_source``, named ``mapping``,
        of ``ontology``, on ``datasets``, when given), as save warnings."""
        identities = await self.list_ontology_mappings(data_source)
        wanted = {d.casefold() for d in datasets} if datasets is not None else None
        out: list[OntologyMismatchWarning] = []
        for ds in sorted({ds for ds, n in identities if mapping in (None, n)}):
            for finding in model_findings(await self.ontology_use(ds)):
                if (mapping is None or finding.mapping == mapping) and ontology in (None, finding.ontology) and (
                        wanted is None or (finding.dataset_name or "").casefold() in wanted):
                    out.append(finding.as_warning())
        return out

    async def _mapping_scope(self, data_source: str) -> MappingScope:
        """What a mapping of ``data_source`` resolves against now."""
        ontologies: dict[str, dict[str, Any]] = {}
        for name in await self._list_ontology_names():
            data = stored_data(await self._get_ontology_text_impl(name=name))
            if isinstance(data, dict):
                ontologies[name] = data
        index = await self._document_index(data_source)
        documents: dict[str, list[SlayerDataset]] = {
            n: [] for ds, n in (await self._list_semantic_model_identities() if self._supports_semantic_models() else [])
            if ds == data_source}
        for key, dataset in index.datasets.items():
            documents.setdefault(index.owners[key], []).append(dataset)
        loaded, _ = await self.load_datasets(data_source=data_source)
        return MappingScope(data_source=data_source, ontologies=ontologies, documents=documents, datasets=loaded)

    async def _guard_mappings(self, *, change: str, changed: Callable[[MappingScope], MappingScope],
                              data_source: str | None = None, own: list[str] | None = None) -> None:
        """Refuse ``change`` (``changed`` maps a scope to its state after it) when it breaks a stored mapping, of
        ``data_source`` when given, listing the changed document's ``own`` problems too; a database-driven change
        warns instead."""
        identities = await self.list_ontology_mappings(data_source)
        broken = []
        for ds in sorted({ds for ds, _ in identities}):
            mappings = [(n, data) for d, n in identities if d == ds and isinstance(
                data := stored_data(await self._get_ontology_mapping_text_impl(data_source=d, name=n)), dict)]
            before = await self._mapping_scope(ds)
            broken += broken_mappings(mappings, before=before, after=changed(before))
        report_broken(change=change, broken=broken, own=own)

    async def _document_datasets(self, *, data_source: str, name: str, text: str) -> list[SlayerDataset]:
        datasource = await self.get_datasource(data_source)
        return list(decompose(text=text, name=name, data_source=data_source,
                              ds_type=datasource.type if datasource is not None else None,
                              granularities=datasource.granularities if datasource is not None else []).datasets)

    async def get_dataset_or_builtin(self, name: str, data_source: str | None = None) -> SlayerDataset | None:
        """``get_dataset``, falling back to a built-in dataset of the datasource (the only one when unnamed)."""
        dataset = await self.get_dataset(name, data_source=data_source)
        if dataset is not None or name != TIME_SPINE_DATASET:
            return dataset
        datasources = [data_source] if data_source is not None else await self.list_datasources()
        if len(datasources) != 1:
            return None
        return next(iter(await self.builtin_datasets(datasources[0], detailed=True)), None)

    @abstractmethod
    async def _save_dataset_impl(self, dataset: SlayerDataset) -> None:
        """Backend-specific durable write of ``dataset`` (backends implement this, not ``save_dataset``)."""

    @abstractmethod
    async def _list_all_dataset_identities(self) -> list[tuple[str, str]]:
        """Every saved ``(data_source, name)`` pair (backends pick the cheapest enumeration)."""

    @abstractmethod
    async def get_dataset(
        self,
        name: str,
        data_source: str | None = None,
    ) -> SlayerDataset | None: ...

    async def _load_raw_dataset_dict(
        self, *, name: str, data_source: str,
    ) -> dict | None:
        """The persisted dataset dict verbatim — no migration, no validation; ``None`` when absent. The legacy-``__`` rewrite reads sibling datasets raw through this to avoid recursing their load pipeline; YAML/SQLite override, the default ``None`` limits the rewrite to first-hop walks."""
        await asyncio.sleep(0)  # awaited protocol hook; async impls override
        return None

    async def _raw_datasource_dicts(self) -> dict[str, dict]:
        """Every stored datasource dict verbatim, keyed by the name ``get_datasource`` reads it under; backends without
        raw access store none."""
        await asyncio.sleep(0)
        return {}

    async def _write_raw_datasource_dict(self, *, name: str, data: dict) -> None:
        raise NotImplementedError

    async def _write_raw_dataset_dict(self, *, name: str, data_source: str, data: dict) -> None:
        raise NotImplementedError

    async def _raw_memory_dicts(self) -> dict[str, dict]:
        """Every stored memory dict verbatim; backends without raw access store none."""
        await asyncio.sleep(0)
        return {}

    async def _write_raw_memory_dict(self, *, memory_id: str, data: dict) -> None:
        raise NotImplementedError

    async def _custom_bucket_calls(self, *, data_sources: list[str]) -> Callable[[str], str]:
        """The text rewrite spelling a call of one of ``data_sources``' granularities ``date_trunc``."""
        units: set[str] = set()
        for name in data_sources:
            with contextlib.suppress(Exception):
                ds = await self.get_datasource(name)
                units |= set(ds.granularity_definitions) if ds is not None else set()
        return partial(custom_buckets_as_date_trunc, units=frozenset(units))

    async def _memory_data_sources(self, data: dict) -> list[str]:
        """The datasources a stored memory's query reads: its inline dataset's, else (no stored population) its pinned
        one, else those holding its root dataset."""
        inline_data_source, source = memory_source_dataset(data)
        if inline_data_source is not None:
            return [inline_data_source]
        identities = await self._list_all_dataset_identities()
        if memory_fallback_population(data, identities=identities)[0]:
            pinned = raw_memory_datasource(data, identities=identities)
            return [pinned] if pinned is not None else []
        return sorted({d for d, n in identities if n == source})

    async def _granularity_renames(self) -> Mapping[str, Renames]:
        """Each stored datasource's library-name granularity renames (storage P3.2), a function of the stored
        datasources alone, read once per top-level load; every load applies them in memory."""
        memo = _RENAME_PASS.get()
        if memo is not None and self in memo:
            return memo[self]
        table: dict[str, Renames] = {}
        logged: set[tuple[str, str]] | None = getattr(self, "_logged_granularity_renames", None)
        if logged is None:
            logged = set()
            setattr(self, "_logged_granularity_renames", logged)
        for name, data in (await self._raw_datasource_dicts()).items():
            renames = datasource_renames(data)
            if renames:
                table[name] = renames
            for old, new in pending_renames(data).items():
                if (name, old) not in logged:
                    logged.add((name, old))
                    logging.getLogger(__name__).warning(
                        "datasource %r: custom granularity %r is now a reserved library name; read as %r with every "
                        "reference to it (`slayer storage upgrade` stores the rename)", name, old, new,
                    )
        if memo is not None:
            memo[self] = table
        return table

    def _forget_granularity_renames(self) -> None:
        memo = _RENAME_PASS.get()
        if memo is not None:
            memo.pop(self, None)

    async def _with_library_granularities_renamed(self, *, name: str, data: Any) -> Any:
        """Stored datasource ``name``'s dict with each granularity a library name reserves renamed."""
        del name  # the renames are a function of the document alone
        await asyncio.sleep(0)
        return renamed_datasource(data) if isinstance(data, dict) else data

    async def _memory_with_granularities_renamed(self, data: Any) -> Any:
        """A stored memory dict with the renamed granularities of the datasource its query reads respelled."""
        table = await self._granularity_renames()
        if not table or not isinstance(data, dict):
            return data
        identities = await self._list_all_dataset_identities()
        for data_source, renames in table.items():
            if _memory_in_datasource(data, data_source=data_source, identities=identities):
                data = rename_in_memory(data, renames=renames)
        return data

    def _warn_upgrade_needed(self, payload: StoredUpgradeNeededWarning) -> None:
        """One upgrade-needed warning per document per storage instance."""
        warned = getattr(self, "_upgrade_warned", None)
        if warned is None:
            warned = self._upgrade_warned = set()
        key = (payload.document_kind, payload.data_source, payload.name)
        if key not in warned:
            warned.add(key)
            warnings.warn(SlayerStoredUpgradeNeededWarning(payload), stacklevel=outside_stacklevel())

    async def _cached_raw_dataset_dict(self, *, name: str, data_source: str) -> dict | None:
        """``_load_raw_dataset_dict``, read once per ``load_datasets`` pass (callers must not mutate it)."""
        memo = _RAW_PASS.get()
        if memo is None:
            return await self._load_raw_dataset_dict(name=name, data_source=data_source)
        read = memo.get((self, data_source, name))
        if read is None:
            read = memo[(self, data_source, name)] = asyncio.ensure_future(
                self._load_raw_dataset_dict(name=name, data_source=data_source))
        return await asyncio.shield(read)  # one waiter's cancellation must not fail the others

    async def _peer_dataset_dict(self, *, name: str, data_source: str) -> dict | None:
        """A peer's raw stored dataset, read once per pass, spelled in the current vocabulary."""
        return dataset_in(await self._cached_raw_dataset_dict(name=name, data_source=data_source), CURRENT_KEYS)

    def _forget_raw(self, *, data_source: str, name: str) -> None:
        """Drop ``(data_source, name)`` from the enclosing pass's raw reads after a write to it."""
        memo = _RAW_PASS.get()
        if memo is not None:
            memo.pop((self, data_source, name), None)

    async def delete_dataset(
        self,
        name: str,
        data_source: str | None = None,
    ) -> bool:
        """Delete one dataset by ``(data_source, name)`` and cascade-delete its embeddings. Bare ``name`` resolves via the priority list; ``False`` (no cascade) when no dataset matches."""
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return False
        resolved_data_source, resolved_name = target
        await self._guard_mappings(change=f"Deleting dataset {resolved_name!r}", data_source=resolved_data_source,
                                   changed=lambda scope: scope.with_dataset(resolved_name, None))
        owner = await self._owner_of(resolved_name, data_source=resolved_data_source)
        if owner is not None:
            deleted = await self._delete_document_dataset(data_source=resolved_data_source, document=owner,
                                                          name=resolved_name)
        else:
            deleted = await self._delete_dataset_row(
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
            await self._drop_inbound_relationships(data_source=resolved_data_source, name=resolved_name)
        return deleted

    async def _drop_inbound_relationships(self, *, data_source: str, name: str) -> None:
        """Remove every relationship of the datasource's other datasets that targets the deleted dataset ``name``."""
        loaded, _ = await self.load_datasets(data_source=data_source)
        for peer in loaded:
            kept = [r for r in peer.relationships if r.to.casefold() != name.casefold()]
            if len(kept) != len(peer.relationships):
                await self.save_dataset(peer.model_copy(update={"relationships": kept}), _validate=False)

    async def _delete_document_dataset(self, *, data_source: str, document: str, name: str) -> bool:
        """Remove dataset ``name`` from ``document`` (the document itself with its last dataset), warning about each
        model-scoped metric that read it."""
        index = await self._document_index(data_source)
        readers = [m.name for m in index.metrics.values()
                   if name.casefold() in expression_datasets(m.expression)[0]]
        members = [d for k, d in index.owners.items() if d == document]
        if len(members) <= 1:
            deleted = await self._delete_semantic_model_row(data_source=data_source, name=document)
        else:
            await self._write_document_dataset(data_source=data_source, document=document, name=name,
                                               mutate=lambda _old: None)
            deleted = True
        for reader in readers:
            warnings.warn(f"Model-scoped metric {reader!r} of semantic model {document!r} reads the deleted dataset "
                          f"{name!r}; it no longer loads.", UserWarning, stacklevel=outside_stacklevel())
        return deleted

    @abstractmethod
    async def _delete_dataset_row(
        self, *, data_source: str, name: str,
    ) -> bool:
        """Delete the persisted row for ``(data_source, name)`` (``True`` if removed); the embedding cascade is the public ``delete_dataset`` wrapper's job."""

    @abstractmethod
    async def update_field_sampled(
        self,
        *,
        data_source: str,
        dataset_name: str,
        field_name: str,
        sampled: str | None,
        sampled_values: list[str] | None,
        distinct_count: int | None,
    ) -> None:
        """Patch a field's ``sampled``/``sampled_values``/``distinct_count`` as one read-modify-write (``None`` drops the key; other fields untouched). Raises ``ValueError`` when the dataset or field is absent."""

    # ---- shared dataset lookup / load helpers ------------------------------

    async def _resolve_target_or_none(
        self,
        name: str,
        *,
        data_source: str | None,
    ) -> tuple[str, str] | None:
        """Sanitize inputs and resolve a bare ``name`` to its ``(data_source, name)`` identity via the priority list; ``None`` when ``data_source`` is omitted and no such bare name exists."""
        _validate_path_component(name, kind="dataset name")
        if data_source is not None:
            _validate_path_component(data_source, kind="data_source")
            return (data_source, name)
        identity = await self.resolve_dataset_identity(name)
        if identity is None:
            return None
        return identity

    async def _apply_refinement_or_raise(
        self, *, name: str, data: dict, data_source: str,
    ) -> None:
        """Refine ``data`` against its live table through the loading engine's client (``CLIENT_SOURCE``).

        DOUBLE base fields require it: a missing datasource, or no loading engine, raises
        ``ValueError``. SQLite-INT widening is best-effort (warn and skip). No-op when neither applies.
        """
        needs_double = has_refineable_fields(data)
        needs_sqlite_int = has_sqlite_widenable_fields(data)
        if not (needs_double or needs_sqlite_int):
            return
        ds = await self.get_datasource(data_source)
        client_source = CLIENT_SOURCE.get()
        if ds is not None and client_source is None and needs_double:
            raise LiveRefinementRequiredError(
                f"Cannot migrate dataset {name!r}: its DOUBLE fields are refined against datasource "
                f"{data_source!r}, so the dataset must be loaded through a query engine "
                f"(SlayerQueryEngine(storage=...).storage)."
            )
        if ds is not None and client_source is not None:
            try:
                await refine_dict_with_live_schema(data, ds, client_source(ds))
            except Exception:
                # Unreachable DS: DOUBLE narrowing is required (propagate); INT
                # widening is advisory (persisted INT is safe) — warn and skip.
                if needs_double:
                    raise
                self._warn_skipped_int_probe(name=name, data_source=data_source)
            return
        if ds is None and needs_double:
            raise LiveRefinementRequiredError(
                f"Cannot migrate dataset {name!r}: datasource "
                f"{data_source!r} is unavailable for type "
                f"refinement. Restore the datasource entry or "
                f"remove the stale dataset file."
            )
        self._warn_skipped_int_probe(name=name, data_source=data_source)

    @staticmethod
    def _warn_skipped_int_probe(*, name: str, data_source: str) -> None:
        # Sanitize CR/LF before logging (log injection, S5145).
        safe_ds = data_source.replace("\r", "\\r").replace("\n", "\\n")
        safe_name = name.replace("\r", "\\r").replace("\n", "\\n")
        logging.getLogger(__name__).warning(
            "Datasource '%s' unavailable; skipping SQLite "
            "affinity probe for INT base fields on '%s'. "
            "Re-run `slayer ingest` once the datasource is "
            "back to widen any mis-typed fields.",
            safe_ds,
            safe_name,
        )

    async def _migrate_and_refine_on_load(
        self,
        *,
        name: str,
        data: Any,
        data_source: str,
    ) -> SlayerDataset:
        """A stored dataset dict upgraded in memory (``_loaded_dataset``) and validated; never written back."""
        return (await self._loaded_dataset(name=name, data=data, data_source=data_source))[0]

    async def _loaded_dataset(self, *, name: str, data: Any, data_source: str) -> tuple[SlayerDataset, bool]:
        """``_migrate_and_refine_on_load``, and whether the upgrade changes the stored document. Within one top-level
        load, a document read again unchanged (a peer of several loads) is upgraded once."""
        with one_upgrade_pass():
            dataset, changes = await self._passed_dataset(name=name, data=data, data_source=data_source)
        if not caller_announces():
            warn_library_shadows(dataset)
            for payload in [*name_collision_warnings(dataset), *spine_variant_warnings(dataset)]:
                warnings.warn(SlayerNameCollisionWarning(payload), stacklevel=2)
        if changes:
            self._warn_upgrade_needed(StoredUpgradeNeededWarning(
                document_kind="dataset", data_source=data_source, name=name))
        return dataset, changes

    async def _passed_dataset(self, *, name: str, data: Any, data_source: str) -> tuple[SlayerDataset, bool]:
        """``_upgraded_dataset`` through the enclosing load's memo."""
        memo = _UPGRADE_PASS.get()
        key = (self, data_source, name)
        kept = memo.get(key) if memo is not None else None
        if kept is not None and kept[0] == data:
            return kept[1].model_copy(deep=True), kept[2]
        read = copy.deepcopy(data)
        dataset, changes = await self._upgraded_dataset(name=name, data=data, data_source=data_source)
        if memo is not None:
            memo[key] = (read, dataset.model_copy(deep=True), changes)
        return dataset, changes

    async def _upgraded_dataset(self, *, name: str, data: Any, data_source: str) -> tuple[SlayerDataset, bool]:
        """A stored dataset dict migrated, renamed, repaired, converted and (below v8) refined against its live table
        through the loading engine; and whether the upgrade changes the stored document. Writes nothing."""
        if not isinstance(data, dict):
            # e.g. a zero-byte/corrupt YAML file (safe_load -> None): give the
            # remediation, not a bare Pydantic model_type error.
            raise ValueError(
                f"Dataset {name!r} in datasource {data_source!r} has an empty or "
                f"corrupt stored definition (got {type(data).__name__} instead "
                f"of a mapping). Delete the stored entry (YAML layout: "
                f"models/{data_source}/{name}.yaml) and re-run `slayer ingest` "
                f"to recreate it."
            )
        renames = (await self._granularity_renames()).get(data_source)
        renamed = rename_in_dataset(data, renames=renames) if renames else data
        settled = renamed != data
        data = _mig.stamp_stored(renamed)
        pre_version = int(data["version"])
        if pre_version < _CUSTOM_BUCKET_CALLS_BELOW["SlayerModel"]:
            data = rewrite_dataset_doc(
                data, await self._custom_bucket_calls(data_sources=[data_source]),
                keys=keys_for(entity="SlayerModel", version=pre_version),
            )
        typing = StoredDatasets()
        if needs_typing(data):
            typing = (await self._stored_datasets(data_source)).model_copy(update={"stages": _stage_names(data)})
        with typing_with(typing):
            dataset, changes = await self._migrated_dataset(name=name, data=data, data_source=data_source)
        return dataset, changes or settled

    async def _migrated_dataset(self, *, name: str, data: dict, data_source: str) -> tuple[SlayerDataset, bool]:
        """A stamped stored dataset dict migrated, repaired and validated; and whether that changed it."""
        changes = False
        pre_version = int(data["version"])
        queries = data.get("source_queries")
        pending = [_unqualified(q, document_version=pre_version, entity="SlayerModel")
                   for q in queries] if isinstance(queries, list) else []
        gates = [_stage_gates(q, document_version=pre_version, entity="SlayerModel", legacy=p)
                 for q, p in zip(queries, pending)] if isinstance(queries, list) else []
        if pre_version < _mig.CURRENT_VERSIONS["SlayerModel"]:
            data = _mig.migrate("SlayerModel", data)
            if pre_version < _RELATIONSHIP_TARGET_REPAIR_BELOW_VERSION:
                await self._repair_relationship_targets(name=name, data=data, data_source=data_source)
            # Rewrite legacy ``__`` split-alias qualifiers to dotted on the RAW
            # dict, before validation (every forward migration may carry them).
            data = await self._rewrite_legacy_join_aliases(
                name=name, data=data, data_source=data_source,
            )
            await self._canonicalize_relationship_key_spellings(data=data, data_source=data_source)
            await self._declare_relationship_keys(name=name, data=data, data_source=data_source)
            data = await self._converted_source_queries(name=name, data=data, data_source=data_source, gates=gates,
                                                        pending=pending)
            # Collapse stored exact-inverse mirror pairs.
            data = await self._dedup_exact_inverse_relationships(
                name=name, data=data, data_source=data_source,
            )
            if pre_version < FIELD_EXPRESSION_VERSION:
                data = respell_source_paths(data, peer_relationships=await self._peer_relationships(
                    data=data, data_source=data_source))
            if pre_version < INSTANT_FIELD_VERSION:
                data = typed_instant_fields(data, ds_type=await self._datasource_type(data_source))
            changes = True
            if pre_version < _LIVE_REFINEMENT_BELOW_VERSION:
                await self._apply_refinement_or_raise(
                    name=name, data=data, data_source=data_source,
                )
        elif any(pending) or any(g.any for g in gates) or any(map(holds_removed_forms, queries if isinstance(queries, list) else [])):
            # A current document can hold an older nested stage: its own version decides.
            data = await self._converted_source_queries(name=name, data=data, data_source=data_source, gates=gates,
                                                        pending=pending)
            changes = True
        with collecting_rewrites() as collected:
            validated = SlayerDataset.model_validate(data)
        dataset, converted = await self._with_dialect_sql(validated, data_source=data_source)
        return dataset, changes or converted or collected.rewritten

    async def _datasource_type(self, data_source: str) -> str | None:
        """The datasource's type: stored, else the enclosing upgrade's, else unknown."""
        datasource = await self.get_datasource(data_source)
        return datasource.type if datasource is not None else DATASOURCE_TYPES.get().get(data_source)

    async def _with_dialect_sql(self, dataset: SlayerDataset, *, data_source: str) -> tuple[SlayerDataset, bool]:
        """``dataset`` with its field ``sql`` converted once its datasource's dialect is known; whether it was."""
        if all(c.sql is None for c in dataset.fields):
            return dataset, False
        ds_type = await self._datasource_type(data_source)
        if ds_type is None:
            return dataset, False
        return with_converted_sql(dataset, ds_type=ds_type), True

    async def _peer_relationships(self, *, data: dict, data_source: str) -> dict[str, list]:
        """Each relationship target's own stored relationships, by target name."""
        out: dict[str, list] = {}
        for relationship in data.get("relationships") or []:
            target = relationship.get("to") if isinstance(relationship, dict) else None
            if isinstance(target, str) and target not in out:
                peer = await self._peer_dataset_dict(name=target, data_source=data_source)
                rels = peer.get("relationships") if isinstance(peer, dict) else None
                out[target] = rels if isinstance(rels, list) else []
        return out

    async def _dataset_names(self, data_source: str) -> set[str]:
        return {n for ds, n in await self._list_all_dataset_identities() if ds == data_source}

    async def _repair_relationship_targets(self, *, name: str, data: dict, data_source: str) -> None:
        """Respell each relationship target of ``data`` naming no dataset exactly but one by case to that dataset; drop each relationship
        into the spine, with a warning."""
        relationships = data.get("relationships")
        if not isinstance(relationships, list):
            return
        names = await self._dataset_names(data_source) | {name}
        kept: list = []
        for relationship in relationships:
            target = relationship.get("to") if isinstance(relationship, dict) else None
            if not isinstance(target, str) or target in names:
                kept.append(relationship)
                continue
            if is_spine_spelling(target):
                warnings.warn(SlayerSpineRelationshipDroppedWarning(SpineRelationshipDroppedWarning(
                    dataset=name, relationship=str(relationship.get("name") or target),
                )), stacklevel=2)
                continue
            relationship["to"] = _repaired_target(target, names=names)
            kept.append(relationship)
        data["relationships"] = kept

    async def _canonicalize_relationship_key_spellings(self, *, data: dict, data_source: str) -> None:
        """Rewrite stored physical relationship-key spellings in ``data`` to ``DatasetField.name``, both sides."""
        relationships = data.get("relationships")
        if not isinstance(relationships, list):
            return
        for i, relationship in enumerate(relationships):
            if not isinstance(relationship, dict) or not isinstance(relationship.get("to"), str):
                continue
            peer = await self._peer_dataset_dict(name=relationship["to"], data_source=data_source)
            relationships[i] = canonical_relationship_keys(
                relationship=relationship, source_fields=data.get("fields"),
                target_fields=peer.get("fields") if isinstance(peer, dict) else None,
            )

    async def _declare_relationship_keys(self, *, name: str, data: dict, data_source: str) -> None:
        """Declare each relationship key naming no field of this table- or SQL-backed document as its hidden base field:
        source keys from its own relationships, target keys from its siblings' relationships into it. Typed like the opposite key."""
        fields = data.get("fields")
        if data.get("source_queries") or not isinstance(fields, list):
            return
        keys = await self._outgoing_keys(relationships=data.get("relationships"), data_source=data_source)
        keys += await self._sibling_keys_into(name=name, fields=fields, data_source=data_source)
        for key, key_type in keys:
            if _undeclared_key(key=key, fields=fields):
                fields.append({"name": key, "hidden": True, **({"datatype": key_type} if key_type else {})})

    async def _outgoing_keys(self, *, relationships: Any, data_source: str) -> list[tuple[Any, str | None]]:
        """Source-side keys of ``relationships``, each typed like its target key."""
        keys: list[tuple[Any, str | None]] = []
        for relationship in relationships if isinstance(relationships, list) else []:
            target = relationship.get("to") if isinstance(relationship, dict) else None
            peer = await self._peer_dataset_dict(name=target, data_source=data_source) if isinstance(target, str) else None
            peer_fields = peer.get("fields") if isinstance(peer, dict) else None
            keys.extend((src, stored_type(key=tgt, fields=peer_fields, keys=CURRENT_KEYS)) for src, tgt in relationship_pairs(relationship))
        return keys

    async def _sibling_keys_into(self, *, name: str, fields: list, data_source: str) -> list[tuple[Any, str | None]]:
        """Target-side keys of every same-datasource sibling's relationships into ``name``."""
        keys: list[tuple[Any, str | None]] = []
        identities = await self._list_all_dataset_identities()
        names = {n for ds, n in identities if ds == data_source}
        for sibling_name in [n for ds, n in identities if ds == data_source and n != name]:
            sibling = _with_repaired_targets(
                await self._peer_dataset_dict(name=sibling_name, data_source=data_source), names=names,
            )
            if sibling is not None:
                keys.extend(_incoming_keys(sibling=sibling, target=name, target_fields=fields))
        return keys

    async def _converted_source_queries(
        self, *, name: str, data: dict, data_source: str, gates: list[LiteralGates], pending: list[bool],
    ) -> dict:
        """``data`` with its stages converted (``_converted_queries``), the document itself out of its own reads."""
        queries = data.get("source_queries")
        if not isinstance(queries, list) or not all(isinstance(q, dict) for q in queries) or len(queries) != len(pending):
            return data
        token = _INLINING.set(_INLINING.get() | {(data_source, name)})
        try:
            converted = await self._converted_queries(queries=queries, gates=gates, pending=pending,
                                                      data_source=data_source, document=name)
        finally:
            _INLINING.reset(token)
        return {**data, "source_queries": converted}

    async def _converted_queries(
        self, *, queries: list[dict], gates: list[LiteralGates], pending: list[bool], data_source: str | None,
        document: str,
    ) -> list:
        """Raw stored ``queries`` (one query list) migrated, then over one read of the datasets they read: their gated
        literals typed, their extension fields inlined, and each ``pending`` one's references qualified."""
        with _mig.migrating_document(document, stored=True):
            migrated = [_mig.migrate("SlayerQuery", _mig.stamp_stored(q)) for q in queries]
        removed = any(holds_removed_forms(q) for q in migrated)
        if not (removed or any(pending) or any(g.any for g in gates)):
            return migrated
        datasets_by_ds = await self._converting_datasets(data_source)
        priority = await self.get_datasource_priority()
        if any(g.any for g in gates):
            migrated = await self._typed_queries(migrated, gates=gates, datasets_by_ds=datasets_by_ds,
                                                 data_source=data_source, priority=priority, document=document)
        if removed:
            converted = inline_extension_fields(migrated, datasets_by_ds=datasets_by_ds, data_source=data_source,
                                                priority=priority, document=document)
            migrated = [without_none(q.model_dump(mode="json")) for q in converted]
        if not any(pending):
            return migrated
        return self._qualified_queries(queries=migrated, pending=pending, datasets_by_ds=datasets_by_ds,
                                       data_source=data_source, priority=priority, document=document)

    async def _typed_queries(
        self, queries: list[dict], *, gates: list[LiteralGates], datasets_by_ds: dict[str, dict[str, SlayerDataset]],
        data_source: str | None, priority: list[str], document: str,
    ) -> list[dict]:
        """Migrated raw stored ``queries`` with their gated literals typed as the binder reads them."""
        try:
            with _mig.migrating_document(document, stored=True):
                validated = [SlayerQuery.model_validate(without_extension_fields(q)) for q in queries]
        except ValueError:
            return queries  # the conversion after it refuses the document
        readings = stage_readings(validated, datasets_by_ds=datasets_by_ds, data_source=data_source, priority=priority)
        stages = _stage_names({"source_queries": queries})
        out: list[dict] = []
        for query, gate, reading in zip(queries, gates, readings):
            if not gate.any:
                out.append(query)
                continue
            types = None
            if gate.whole_day and holds_extension_date_literal(query):
                types = (await self._stored_datasets(reading.data_source)).model_copy(update={"stages": stages})
            label = query.get("name") if isinstance(query.get("name"), str) and query.get("name") else document
            out.append(typed_stage(query, gates=gate, reading=reading, extension_types=types, label=label))
        return out

    async def _converting_datasets(self, data_source: str | None) -> dict[str, dict[str, SlayerDataset]]:
        """The loadable datasets a stored query's conversion reads, by datasource (the documents converting excluded)."""
        datasets_by_ds: dict[str, dict[str, SlayerDataset]] = {}
        for ds, dataset_name in await self._list_all_dataset_identities():
            if (data_source is not None and ds != data_source) or (ds, dataset_name) in _INLINING.get():
                continue
            try:
                dataset = await self.get_dataset(dataset_name, data_source=ds)
            except StoredDocumentLoadError:
                continue
            if dataset is not None:
                datasets_by_ds.setdefault(ds, {})[dataset.name] = dataset
        return datasets_by_ds

    async def incident_edge_names(self, name: str, *, data_source: str) -> frozenset[str]:
        """The names of the stored edges incident to dataset ``name`` of ``data_source``, declared on either side."""
        return (await self._stored_datasets(data_source)).incident_names(name)

    async def qualified_stored_queries(
        self, queries: list[SlayerQuery], *, versions: list[int | None], data_source: str | None, document: str,
    ) -> list[SlayerQuery]:
        """``queries`` (stages read from a stored document, each written at ``versions``) with the pre-2.0 ones'
        references qualified, keeping their meaning; an unversioned stage counts as current."""
        pending = [v is not None and int(v) < _QUALIFIED_AT["SlayerQuery"] for v in versions]
        if len(pending) != len(queries) or not any(pending):
            return queries
        qualified = self._qualified_queries(
            queries=list(queries), pending=pending, datasets_by_ds=await self._converting_datasets(data_source),
            data_source=data_source, priority=await self.get_datasource_priority(), document=document)
        return [SlayerQuery.model_validate(q) for q in qualified]

    def _qualified_queries(
        self, *, queries: list[Any], pending: list[bool], datasets_by_ds: dict[str, dict[str, SlayerDataset]],
        data_source: str | None, priority: list[str], document: str,
    ) -> list[Any]:
        """Raw stored ``queries`` with each ``pending`` one's references qualified, keeping their meaning."""
        with _mig.migrating_document(document, stored=True):
            validated = [q if isinstance(q, SlayerQuery) else SlayerQuery.model_validate(
                _mig.stamp_stored(q) if isinstance(q, dict) else q) for q in queries]
        converted = qualified_stages(validated, pending=pending, datasets_by_ds=datasets_by_ds,
                                     data_source=data_source, priority=priority)
        return [without_none(q.model_dump(mode="json")) for q in converted]

    async def _stored_datasets(self, data_source: str | None) -> StoredDatasets:
        """Every raw stored dataset dict of ``data_source``."""
        if data_source is None:
            return StoredDatasets()
        datasets: dict[str, dict] = {}
        for ds, name in await self._list_all_dataset_identities():
            raw = await self._cached_raw_dataset_dict(name=name, data_source=ds) if ds == data_source else None
            if isinstance(raw, dict):
                datasets[name] = dataset_in(raw, LEGACY_KEYS)
        return StoredDatasets(data_source=data_source, datasets=datasets)

    async def _dedup_exact_inverse_relationships(
        self, *, name: str, data: dict, data_source: str,
    ) -> dict:
        """Drop this document's half of a stored exact-inverse relationship pair when the counterpart holds the surviving half. Load-order-independent (survivor reads only the two halves); a missing/drifted peer leaves the document untouched."""
        relationships = data.get("relationships")
        if not isinstance(relationships, list) or not relationships:
            return data
        cache: dict[str, dict | None] = {}
        names = await self._dataset_names(data_source)
        kept = [
            relationship for relationship in relationships
            if await self._survives_stored_inverse(
                relationship=relationship, name=name, data_source=data_source, cache=cache,
                fields=data.get("fields"), names=names,
            )
        ]
        if len(kept) != len(relationships):
            data["relationships"] = kept
        return data

    async def _survives_stored_inverse(
        self, *, relationship, name: str, data_source: str,
        cache: dict[str, dict | None], fields: Any, names: set[str],
    ) -> bool:
        """True when ``relationship`` has no stored exact-inverse counterpart, or wins
        against it."""
        peer_name = relationship.get("to") if isinstance(relationship, dict) else None
        if not isinstance(peer_name, str) or peer_name == name:
            return True
        if peer_name not in cache:
            cache[peer_name] = _with_repaired_targets(await self._peer_dataset_dict(
                name=peer_name, data_source=data_source,
            ), names=names)
        counterpart = _stored_counterpart(
            relationship=relationship, name=name, peer=cache[peer_name], fields=fields,
        )
        return counterpart is None or _inverse_survivor(
            dataset_a=name, relationship_a=relationship,
            dataset_b=peer_name, relationship_b=counterpart,
        ) == name

    async def _rewrite_legacy_join_aliases(
        self, *, name: str, data: dict, data_source: str,
    ) -> dict:
        """Rewrite legacy ``__`` split-alias qualifiers to dotted across every Mode-A surface on the raw dict. Each ``a__b__c`` is naive-split and resolved as a relationship walk (first hop against ``data``'s relationships, deeper hops against sibling raw dicts); only fully resolvable walks are rewritten, an unresolvable ``__`` is left verbatim. Mutates and returns ``data``."""
        all_chains: set[tuple[str, ...]] = set()
        for text in mode_a_surface_texts(data):
            all_chains |= extract_dunder_chains(text)
        if not all_chains:
            return data

        raw_cache: dict[str, dict | None] = {name: data}
        names = await self._dataset_names(data_source)
        resolvable: set[tuple[str, ...]] = set()
        for chain in all_chains:
            if await self._dunder_chain_is_relationship_walk(
                chain=chain, host=data, data_source=data_source, cache=raw_cache, names=names,
            ):
                resolvable.add(chain)
        if not resolvable:
            return data

        apply_dunder_rewrite_to_dataset_dict(data, resolvable=resolvable)
        return data

    async def _dunder_chain_is_relationship_walk(
        self,
        *,
        chain: tuple[str, ...],
        host: dict,
        data_source: str,
        cache: dict[str, dict | None],
        names: set[str],
    ) -> bool:
        """True iff every hop in ``chain`` is a relationship target on the preceding dataset (host first, then each hop's own dataset), a hop matching its target by case as a target repair would; sibling dicts loaded raw, target-repaired and memoised, a broken intermediate collapses the chain."""
        # An exact ``__``-named relationship target beats a split-alias reading: a host
        # relationship to a dataset literally named ``"__".join(chain)`` is exact, not a walk.
        host_relationships = host.get("relationships")
        host_targets = {
            j.get("to")
            for j in host_relationships if isinstance(j, dict)
        } if isinstance(host_relationships, list) else set()
        if "__".join(chain) in host_targets:
            return False

        current: dict | None = host
        for i, hop in enumerate(chain):
            if current is None:
                return False
            relationships = current.get("relationships")
            targets = {
                t for j in relationships if isinstance(j, dict) and isinstance(t := j.get("to"), str)
            } if isinstance(relationships, list) else set()
            target = _repaired_target(hop, names=targets)
            if target not in targets:
                return False
            if i == len(chain) - 1:
                return True
            if target not in cache:
                cache[target] = _with_repaired_targets(await self._peer_dataset_dict(
                    name=target, data_source=data_source,
                ), names=names)
            current = cache[target]
        return True

    # ---- datasource CRUD ---------------------------------------------------

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        """Persist a datasource config (upsert by exact name), keeping the stored granularity rename records, once it re-validates and its custom granularities pass the save-time rules. Backends must NOT override this."""
        check_whole(datasource)
        datasource = await self._with_stored_rename_records(datasource)
        datasource.check_granularities()
        connection_policy.check_transaction_pooler(datasource)
        await self._save_datasource_impl(datasource)
        self._forget_granularity_renames()

    async def _with_stored_rename_records(self, datasource: DatasourceConfig) -> DatasourceConfig:
        """``datasource`` with ``renamed_from`` on each new name of a stored rename (pending or recorded) it records
        nowhere; refused when that granularity records another former name."""
        renames = datasource_renames(await self._stored_raw(kind="datasource", data_source=None, name=datasource.name))
        recorded = {granularity_key(g.renamed_from) for g in datasource.granularities if g.renamed_from is not None}
        former_of = {
            granularity_key(new): former for former, new in reversed(list(renames.items()))
            if granularity_key(former) not in recorded
        }
        for g in datasource.granularities:
            if g.renamed_from is not None and granularity_key(g.name) in former_of:
                raise GranularityDefinitionError(name=g.name, rule=(
                    f"renamed_from {g.renamed_from!r} would drop the stored record of {former_of[granularity_key(g.name)]!r}"))
        granularities = [
            g.model_copy(update={"renamed_from": former_of[granularity_key(g.name)]})
            if granularity_key(g.name) in former_of else g
            for g in datasource.granularities
        ]
        if granularities == datasource.granularities:
            return datasource
        return datasource.model_copy(update={"granularities": granularities})

    async def _datasource_from_stored(self, *, name: str, data: Any) -> DatasourceConfig:
        """A stored datasource dict upgraded in memory and validated (env vars unresolved); never written back."""
        datasource, changes = await self._upgraded_datasource(name=name, data=data)
        if changes:
            self._warn_upgrade_needed(StoredUpgradeNeededWarning(document_kind="datasource", name=name))
        return datasource

    async def _upgraded_datasource(self, *, name: str, data: Any) -> tuple[DatasourceConfig, bool]:
        """A stored datasource dict upgraded and validated; and whether the upgrade changes the stored document."""
        renamed = await self._with_library_granularities_renamed(name=name, data=data)
        stamped = _mig.stamp_stored(renamed)
        changes = renamed != data or (
            isinstance(stamped, dict) and int(stamped["version"]) < _mig.CURRENT_VERSIONS["DatasourceConfig"])
        with collecting_rewrites() as collected:
            datasource = DatasourceConfig.model_validate(renamed)
        return datasource, changes or collected.rewritten

    async def _stored_raw(self, *, kind: str, data_source: str | None, name: str) -> dict | None:
        """One stored document's raw dict by ``kind`` (``dataset``, ``memory`` or ``datasource``)."""
        if kind == "dataset" and data_source is not None:
            return await self._load_raw_dataset_dict(name=name, data_source=data_source)
        if kind == "memory":
            return (await self._raw_memory_dicts()).get(name)
        return (await self._raw_datasource_dicts()).get(name)

    async def _write_raw_if_unchanged(
        self, *, kind: str, data_source: str | None, name: str, expected: dict, data: dict,
    ) -> bool:
        """Write ``data`` as a stored document's raw dict when it still reads ``expected``; ``False`` on a conflict."""
        if await self._stored_raw(kind=kind, data_source=data_source, name=name) != expected:
            return False
        if kind == "dataset" and data_source is not None:
            await self._write_raw_dataset_dict(name=name, data_source=data_source, data=data)
        elif kind == "memory":
            await self._write_raw_memory_dict(memory_id=name, data=data)
        else:  # the run's rename table stays: its datasource writes must not change what later decisions read
            await self._write_raw_datasource_dict(name=name, data=data)
        return True

    @abstractmethod
    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        """Backend-specific durable write; filename-backed backends should call ``check_datasource_id_collision`` first."""

    async def check_datasource_id_collision(self, name: str) -> None:
        """Raise :class:`IdCollisionError` when ``name`` case-collides with an existing datasource name or a saved dataset's ``data_source`` (public so backends call it from ``_save_datasource_impl``)."""
        existing = set(await self.list_datasources())
        existing.update(ds for ds, _ in await self._list_all_dataset_identities())
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
        """Delete the datasource config and cascade-delete every embedding under its canonical prefix. Datasets in the datasource are NOT deleted — they become orphans; re-creating the datasource and re-ingesting repopulates embeddings."""
        # Sanitize the raw name before it composes a path / cascade prefix.
        _validate_path_component(name, kind=DATASOURCE_NAME)
        deleted = await self._delete_datasource_row(name)
        self._forget_granularity_renames()
        for ds, mapping in await self.list_ontology_mappings(name):
            await self._delete_ontology_mapping_row(data_source=ds, name=mapping)
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

    async def list_datasets(self, data_source: str | None = None) -> list[str]:
        """Dataset names within one datasource. With ``data_source``: datasets stored under it (accepted if registered OR referenced by any saved dataset, so orphans list; unknown names raise ``ValueError``). Without it: the sole datasource's datasets, ``[]`` when empty, or a ``ValueError`` naming the datasources when ≥2 hold datasets."""
        identities = await self._list_all_dataset_identities()
        if data_source is not None:
            known = set(await self.list_datasources())
            existing_sources = {ds for ds, _ in identities}
            if data_source not in known and data_source not in existing_sources:
                raise ValueError(
                    f"list_datasets: unknown data_source {data_source!r}; "
                    f"known datasources: {sorted(known | existing_sources) or '[]'}."
                )
            return sorted(name for ds, name in identities if ds == data_source)
        distinct_sources = sorted({ds for ds, _ in identities})
        if not distinct_sources:
            return []
        if len(distinct_sources) == 1:
            return sorted(name for _, name in identities)
        raise ValueError(
            f"list_datasets: datasets exist in multiple datasources "
            f"{distinct_sources}; supply data_source=... to pick one."
        )

    # ---- bare-name resolver (priority-aware) ------------------------------

    async def resolve_dataset_identity(
        self,
        name: str,
        *,
        prefer_data_source: str | None = None,
    ) -> tuple[str, str] | None:
        """Resolve a bare dataset name to ``(data_source, name)``: ``None`` if no match, the sole match, ``prefer_data_source`` when it is a candidate, else the first priority-listed datasource holding it, else raise ``AmbiguousDatasetError``. ``prefer_data_source`` is the internal relationship-target hint; explicit callers use ``get_dataset(..., data_source=...)``."""
        identities = await self._list_all_dataset_identities()
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
        raise AmbiguousDatasetError(name=name, candidates=candidates)

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
        """Decode, upgrade in memory and validate one stored memory inside its load boundary; never written back."""
        with stored_document_boundary(kind="memory", name=memory_id), one_upgrade_pass():
            memory, changes = await self._upgraded_memory(memory_id=memory_id, data=decode())
        if changes:
            self._warn_upgrade_needed(StoredUpgradeNeededWarning(document_kind="memory", name=memory_id))
        return memory

    async def _upgraded_memory(self, *, memory_id: str, data: Any) -> tuple[Memory, bool]:
        """A stored memory dict upgraded and validated; and whether the upgrade changes the stored document."""
        renamed = await self._memory_with_granularities_renamed(data)
        changes = renamed != data
        data = _mig.stamp_stored(renamed)
        changes = changes or (isinstance(data, dict) and int(data["version"]) < _mig.CURRENT_VERSIONS["Memory"])
        if isinstance(data, dict) and int(data["version"]) < _CUSTOM_BUCKET_CALLS_BELOW["Memory"]:
            data = rewrite_memory_doc(data, await self._custom_bucket_calls(
                data_sources=await self._memory_data_sources(data)),
                keys=keys_for(entity="Memory", version=int(data["version"])))
        query = data.get("query") if isinstance(data, dict) else None
        if isinstance(query, dict):
            version = int(data["version"])
            pending = _unqualified(query, document_version=version, entity="Memory")
            gates = _stage_gates(query, document_version=version, entity="Memory",
                                 legacy=version < _MEMORY_FILTER_REPAIR_BELOW_VERSION)
            if pending or gates.any or version < _EXTENSION_FIELDS_INLINED_AT or holds_removed_forms(query):
                converted = await self._converted_queries(
                    queries=[query], gates=[gates], pending=[pending],
                    data_source=await self._memory_upgrade_data_source(data) if pending else None,
                    document=memory_id)
                data = {**data, "query": converted[0]}
                changes = True
        with collecting_rewrites() as collected:
            memory = Memory.model_validate(data)
        return memory, changes or collected.rewritten

    async def _memory_upgrade_data_source(self, data: dict) -> str | None:
        """The datasource a stored memory's qualification reads: pinned when its population names no stored dataset
        (refused when undetermined), else none."""
        identities = await self._list_all_dataset_identities()
        triggered, population = memory_fallback_population(data, identities=identities)
        if not triggered:
            return None
        pinned = raw_memory_datasource(data, identities=identities)
        if pinned is None:
            raise UndeterminedDatasourceError(population=population)
        return pinned

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
        with one_upgrade_pass():
            return await self._list_memories_rows(entities=entities)

    async def list_memories(
        self, *, entities: list[str] | None = None, failures: DocumentLoadFailures | None = None,
    ) -> list[Memory]:
        return (failures or DocumentLoadFailures()).skip(await self.load_memories(entities=entities))

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
        """Remove ``canonical_id`` from every memory's ``entities`` list; returns the count rewritten. Match: ``memory:<id>`` exact-only, ``<ds>[.<dataset>[.<leaf>]]`` exact or strict dotted descendant. Per-row read-modify-write via ``_save_memory_row`` (not the service); learning-only embeddings mean no refresh fires."""
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

    # ---- cache keys ----

    async def graph_fingerprint(self) -> str | None:
        """A string that changes whenever storage content changes; ``None`` (the default) = unknown, so no cache entry is ever reused. May raise ``OSError`` — callers force a rebuild."""
        await asyncio.sleep(0)
        return None

    async def cache_identity(self) -> Hashable:
        """Key under which caches derived from this store's content are kept (default: one per instance)."""
        await asyncio.sleep(0)
        if self._instance_identity is None:
            self._instance_identity = uuid.uuid4().hex
        return self._instance_identity

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
        """Cascade-delete embedding rows whose ``canonical_id`` is exactly ``canonical_id_prefix`` or a strict dotted descendant (never a character prefix — ``"orders"`` ≠ ``"orders_archive"``, ``"memory:4"`` ≠ ``"memory:42"``). Returns the count deleted; used by delete_dataset/_memory/_datasource."""

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
