"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → plan_query() → PlannedQuery → SQLGenerator → SQL → execute
"""

import asyncio
import contextlib
import copy
import decimal
import logging
import tempfile
import warnings as _warnings_module
from collections.abc import AsyncGenerator, Sequence
from datetime import datetime
from functools import partial
from typing import Any, Callable, Coroutine, Dict, Iterator, List, Optional, Set, Tuple, TypeVar, Unpack

from sqlglot import exp
from sqlglot.expressions.core import Expr
from typing_extensions import deprecated
from pydantic import (
    BaseModel,
    ConfigDict as PydanticConfigDict,
    Field as PydanticField,
    model_validator,
)

from slayer.async_utils import run_sync
from slayer.core import probes
from slayer.core.deprecation import outside_stacklevel
from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, RANKED_AGGREGATIONS, DataType, RelationshipCardinality, TimeGranularity
from slayer.core.errors import (
    AggregationArgumentError,
    AmbiguousDatasetError,
    AssociatedGrainWarning,
    BroadcastGrainWarning,
    CircularRelationshipPathError,
    DerivedFieldCircularError,
    ForcedFilterError,
    DatasetSqlValidationError,
    PopulationInferenceError,
    QueryBackedCycleError,
    SchemaDriftError,
    SlayerError,
    StoredDocumentLoadError,
)
from slayer.engine.cardinality.inference import (
    CardinalityVerdict,
    RelationshipCardinalityFinding,
    RelationshipCardinalityReport,
    SideStats,
    classify_cardinality,
    compute_verdict,
    declares_solo_unique,
)
from slayer.core.policy import SessionPolicy
from slayer.core.format import encode_text, format_number
from slayer.core.relationship_edges import RelationshipEdgeRef, resolve_relationship_refs, without_relationships
from slayer.core.models import (
    DatasourceConfig,
    name_collision_warnings,
    SlayerDataset,
    is_identifier,
    _check_relationship_keys,
    relationship_key_error,
)
from slayer.core.query import (
    OrderItem,
    QueryMetric,
    QueryRefinement,
    SlayerQuery,
    broadcast_deprecations,
    deprecated_inputs_of,
    extract_variable_refs,
    mode_a_texts,
    refine_query,
    render_probe_text,
)
from slayer.core.relationship_search import blocking_pairs, shortest_routes, unreachable_target
from slayer.engine.definitions import needs_live_types, resolved_dataset
from slayer.engine.field_dependency import check_definition_ambiguity, check_definition_reach, check_dataset_filters
from slayer.engine.population.inference import (
    DimensionAnchor,
    PopulationChoice,
    ambiguity_resolved_path,
    declared_head_spellings,
    determined_path,
    infer_rows,
    infer_spine_population,
    item_blockers,
    metric_anchor_names,
    spine_datasource,
    spine_factor_out,
)
from slayer.engine.population.row_supply import (
    RowPlan, anchored_rows, plan_rows, plan_spine_rows, refuse_product_split, stored_rows,
)
from slayer.core.time_points import host_clock
from slayer.core.names import name_taken
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine_query, respell_spine, spine_variant_warnings
from slayer.core.scope import StageDisplay, StageSchema, collect_stale_spellings
from slayer.core.legacy_spellings import collect_legacy_spellings, emit_deprecations
from slayer.core.vocabulary import method_twin, one_document
from slayer.core.warnings import (
    AnySlayerWarning,
    AssociatedWarningPayload,
    DegenerateReaggregationWarningPayload,
    BroadcastDimension,
    BroadcastGrainWarningPayload,
    broadcast_hint,
    DeprecatedInputWarning,
    LibraryNameShadowWarning,
    NameCollisionWarning,
    NarrowedAccessWarning,
    NormalizationWarning,
    SemiJoinPushedWarningPayload,
    SlayerLibraryNameShadowWarning,
    SlayerNameCollisionWarning,
    SlayerNarrowedAccessWarning,
    SlayerUnrelatedDatasetWarning,
    SlayerWarning,
    SlayerWholeListWarning,
    SlayerDateOnlyMidnightWarning,
    DateOnlyMidnightWarning,
    SlayerWholePeriodsWarning,
    UnrelatedDatasetWarningPayload,
    WholeListWarning,
    WholePeriodsNonNestingWarningPayload,
    announced_by_caller,
)
from slayer.core.refs import functional_agg_text
from slayer.core.recommend import (
    CandidateCoverage,
    ItemPath,
    PopulationRecommendation,
)
from slayer.core.syntax import AggCall, Ref, StarSource, parse_expr, split_entity_agg_ref
from slayer.engine.cache import (
    CacheConfig,
    NormalizedTable,
    QueryCache,
    RefreshError,
    RefreshKeyValue,
    RefreshResult,
    _CacheEntry,
)
from slayer.engine.normalization import normalize_query, stale_spelling_warnings
from slayer.engine.legacy_references import Conversions, anchor_names, convert_refinement, qualified_input
from slayer.engine.model_scoped import expand_model_scoped, model_scope
from slayer.engine.population.anchors import anchor_route, unknown_metric, unqualified_count
from slayer.core.names import qualified_reference
from slayer.core.keys import AggregateKey, CastKey, FieldKey, FieldSqlKey, ListSetKey, TimeTruncKey, ValueKey
from slayer.ir.planned import (
    PlannedQuery,
    _iter_plans_with_producers,
    _walk_regroup_attaches,
    emitted_plans,
    plan_has_semi_join_filters,
    plans_read_clock,
    planned_keys,
)
from slayer.live_schema import ingestion, schema_drift
from slayer.engine.dataset_reads import narrowing_datasets
from slayer.live_schema.schema_scope import IngestScopeArgs
from slayer.live_schema.type_refinement import live_table_location
from slayer.engine.unknown_types import UnknownTypes
from slayer.engine.response_meta import (
    FieldMetadata as FieldMetadata,  # re-export for slayer_client / tests
    ResponseAttributes,
    build_response_metadata,
    projection_result_keys,
)
from slayer.sql.definitions import with_converted_sql
from slayer.sql.field_expansion import expand_derived_refs_sync
from slayer.ir.source_bundle import (
    ROOT_STAGE,
    ResolvedSourceBundle,
    RowSupply,
    SplicedRows,
    absorb_inherited,
    dataset_from_stage_schema,
)
from slayer.engine.stage_ordering import localize_stages, topologically_order_stages
from slayer.engine.statement_reads import StatementReads, statement_reads
from slayer.engine.binding import collect_bind_records
from slayer.engine.elaborate_env import check_result_names_case, check_stage_flatten_collision, saved_date_only_notices
from slayer.engine.extensions import with_own_extension
from slayer.engine.plan import bound_supply, plan_stages
from slayer.ir.variables import (
    apply_variables_to_query,
    bind_extension_metrics,
    dataset_needs_substitution_pass,
    substitute_dataset_sql_surfaces,
)
from slayer.memories.resolver import (
    _all_datasets_in_datasource,
    resolve_entity,
)
from slayer.database.client import (
    ExecutionResult,
    ResultColumnType,
    SlayerSQLClient,
    SqlClient,
    _is_auth_failure,
    _is_transient_db_error,
    _is_unreachable_db_error,
    classify_dataset_sql,
    render_type_probe,
    sql_dataset_trial_query,
)
from slayer.sql.dialects import SQLGLOT_NAMES, SqlDialect, dialect_for_ds_type, get_dialect
from slayer.sql.mode_a_variables import bind_mode_a, check_mode_a_placeholders, check_placeholders
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.render.library import check_library_support
from slayer.sql.naming import key_identifier
from slayer.sql.render.parse import parse_table
from slayer.sql.sql_template import SqlTemplateError, sql_template
from slayer.sql.dialects.reflection import TableLocation
from slayer.database.engine_factory import ClientCacheKey, _sql_client_cache_key, client_cache_key
from slayer.sql.generator import (
    PlannedStatement,
    _finish_statement,
    _user_authored_exemptions,
    build_planned_statement,
    read_datasets,
)
from slayer.sql.physical_tables import physical_tables
from slayer.sql.session_policy import HasColumn, ScopedTable, apply_session_policy, policy_column_checks
from slayer.sql.stage_wrapper import build_flat_rename_wrapper
from slayer.storage.base import StorageBackend, with_canonical_spellings
from slayer.storage.engine_view import EngineStorage
from slayer.storage.document_loading import DocumentLoadFailures
from slayer.storage.yaml_storage import YAMLStorage
import slayer.engine.bundle_builder
from slayer.core.models import DatasetRelationship

logger = logging.getLogger(__name__)


_QueryT = TypeVar("_QueryT")
_T = TypeVar("_T")

#: Builds the client an engine uses for one datasource; the default builds ``SlayerSQLClient``.
ClientFactory = Callable[[DatasourceConfig], SqlClient]

REFINE_REQUIRES_NAME = (
    "refine applies only to a saved query run by name; put these clauses in the query object instead"
)


class SavedQueryRun(BaseModel):
    """A run-by-name input: a query-backed dataset's name and an optional refinement of its final stage."""

    name: str
    refinement: QueryRefinement | None = None


def _read_dataset_collisions(*, reads: StatementReads, bundle: ResolvedSourceBundle) -> List[NameCollisionWarning]:
    """A ``NameCollisionWarning`` per case collision of each dataset the statement reads."""
    read = {m for m, _ in (*reads.fields, *reads.metrics, *reads.aggregations)}
    datasets = {**bundle.datasets_by_name, **({bundle.population.name: bundle.population} if bundle.population else {})}
    variants = [w for m in datasets.values() if m.name in read or m is bundle.population for w in spine_variant_warnings(m)]
    return [*(w for name in sorted(read) if name in datasets for w in name_collision_warnings(datasets[name])), *variants]


def _validated_run_input(run_input: Any) -> Any:
    """``run_input`` with each query mapping validated once, so an input form it converts is reported once."""
    if isinstance(run_input, list):
        with one_document():
            return [SlayerQuery.model_validate(item) if isinstance(item, dict) else item for item in run_input]
    return SlayerQuery.model_validate(run_input) if isinstance(run_input, dict) else run_input


def _check_fresh_result_names(run_input: Any) -> None:
    """The result names a caller writes may not differ only in case; a stored query is never refused."""
    for query in run_input if isinstance(run_input, list) else [run_input]:
        if isinstance(query, SlayerQuery):
            check_result_names_case(names=[
                *(m.alias for m in query.metrics or [] if m.alias),
                *(d.name for d in query.dimensions or [] if d.is_computed),
                *(td.alias for td in query.time_dimensions or [] if td.alias),
            ])


def resolve_run_input(query: _QueryT, refine: "QueryRefinement | dict | None") -> "_QueryT | SavedQueryRun":
    """Pack a saved-query name with its refinement; a refinement carrying no clause and no deprecation is none."""
    if refine is None:
        return SavedQueryRun(name=query) if isinstance(query, str) else query
    if not isinstance(query, str):
        raise ValueError(REFINE_REQUIRES_NAME)
    refinement = QueryRefinement.model_validate(refine) if isinstance(refine, dict) else refine
    kept = refinement.model_fields_set or refinement.deprecated_inputs
    return SavedQueryRun(name=query, refinement=refinement if kept else None)


class _ResolvedItem(BaseModel):
    input_item: str
    data_source: str
    dataset: str
    leaf: str
    suffix: str | None = None
    # Metrics / aggregation-suffixed items attach (reachability-only)
    # and never steer the recommendation; fields are determination items.
    attachment: bool = False


def _anchored_route(
    *, root: SlayerDataset, dataset: str, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[Tuple[str, ...]]:
    """The route a query rooted at ``root`` takes to the anchor ``dataset``; ``None`` when it refuses it."""
    anchor = datasets_by_name.get(dataset)
    if anchor is None:
        return None
    try:
        return anchor_route(reader=root, anchor=anchor, datasets_by_name=datasets_by_name).route
    except SlayerError:
        return None


def _attachment_route(
    *, root: SlayerDataset, item: "_ResolvedItem", routed: Dict[str, Any], datasets_by_name: Dict[str, SlayerDataset],
) -> Tuple[str, ...]:
    """An attachment's route from ``root``: the anchor's route, else the tie-break-best route."""
    anchored = _anchored_route(root=root, dataset=item.dataset, datasets_by_name=datasets_by_name)
    return anchored if anchored is not None else routed[item.dataset].tokens


def _emit_recommend_path(
    hops: list[str], item: "_ResolvedItem", *, root: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> str:
    """``item`` as written when a query rooted at ``root`` reads it along ``hops``, else its explicit route from the
    root, re-aggregated functionally."""
    if _anchored_route(root=root, dataset=item.dataset, datasets_by_name=datasets_by_name) == tuple(hops):
        entity, _ = split_entity_agg_ref(item.input_item)
        if entity.split(".") != [item.data_source, item.dataset, item.leaf]:
            return item.input_item
        core = f"{item.dataset}.{item.leaf}"  # a datasource prefix is no query reference
        return core if item.suffix is None else functional_agg_text(source=core, suffix=item.suffix)
    core = ".".join((root.spelling, *hops, item.leaf))
    return core if item.suffix is None else functional_agg_text(source=core, suffix=item.suffix)


def _resolve_root_hint(
    raw_hint: str | None, *, data_source: str, all_names: list[str]
) -> tuple[str, str] | None:
    """Resolve ``root_hint`` to ``(dataset, display)`` (display verbatim in diagnostics); raises if malformed."""
    if raw_hint is None:
        return None
    display = raw_hint.strip()
    if not display:
        return None
    if "." in display:
        segs = display.split(".")
        if len(segs) == 2 and segs[0] == data_source:
            dataset = segs[1]
        else:
            raise ValueError(
                f"root_hint '{display}' must be a bare dataset name or "
                f"'{data_source}.<dataset>' within datasource '{data_source}'."
            )
    else:
        dataset = display
    if dataset not in all_names:
        raise ValueError(
            f"root_hint '{display}' is not a dataset in datasource '{data_source}'."
        )
    return dataset, display


def _build_recommend_coverage(
    all_names: list[str],
    items: list["_ResolvedItem"],
    reach_by_candidate: dict[str, dict[str, tuple[str, ...]]],
    *,
    force_include: set[str] | None = None,
) -> list[CandidateCoverage]:
    """Pareto frontier of partial-root candidates over ``items``.

    ``reach_by_candidate`` maps each candidate to input item → its path;
    ``force_include`` rows appear even if dominated.
    """
    forced = force_include or set()
    items_in_order = [r.input_item for r in items]

    candidates: list[tuple[str, set[str], dict[str, int]]] = []
    for name in all_names:
        paths = reach_by_candidate[name]
        reach = {it for it in items_in_order if it in paths}
        if not reach and name not in forced:
            continue
        candidates.append((name, reach, {it: len(paths[it]) for it in reach}))

    def dominates(a: tuple, b: tuple) -> bool:
        _an, ar, ah = a
        _bn, br, bh = b
        if ar > br:  # strict superset → covers strictly more
            return True
        if ar == br:  # same coverage, no path longer, at least one shorter
            return all(ah[m] <= bh[m] for m in ar) and any(ah[m] < bh[m] for m in ar)
        return False

    frontier = [
        c for c in candidates
        if c[0] in forced
        or not any(dominates(o, c) for o in candidates if o[0] != c[0])
    ]

    entries: list[tuple[CandidateCoverage, int]] = []
    for name, reach, hops in frontier:
        entries.append((
            CandidateCoverage(
                dataset_name=name,
                reachable_items=[it for it in items_in_order if it in reach],
                unreachable_items=[it for it in items_in_order if it not in reach],
            ),
            sum(hops.values()),
        ))
    entries.sort(key=lambda e: (-len(e[0].reachable_items), e[1], e[0].dataset_name))
    return [e[0] for e in entries]


def _merge_query_variables(
    *,
    outer: Optional[Dict[str, Any]],
    stage: Optional[Dict[str, Any]],
    runtime: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge variable layers per precedence ``runtime > stage > outer``."""
    return {**(outer or {}), **(stage or {}), **(runtime or {})}


def _spliced_rows(bundle: ResolvedSourceBundle) -> Dict[Tuple[str, str], SplicedRows]:
    """Each stored inferred stage the statement may splice, planned from its pinned anchor."""
    out: Dict[Tuple[str, str], SplicedRows] = {}
    for name, dataset in bundle.query_backed.items():
        for stage in dataset.source_queries or []:
            plan = stored_rows(stage, datasets_by_name=bundle.datasets_by_name)
            if plan is not None:
                out[(name, stage.name or ROOT_STAGE)] = SplicedRows(population=plan.population, supply=plan.supply)
    return out


def _with_default_order(query: SlayerQuery) -> SlayerQuery:
    """``query`` ordered by its projected dimensions, ascending, in result-column order, when it has no order.

    Each item is spelled as its dimension is declared, so it binds onto the projected value.
    """
    if query.order or query.distinct_dimension_values is False:
        return query
    dimensions = [
        ref.spelling if (ref := d.ref) is not None and not d.is_computed else d.name for d in query.dimensions or []
    ]
    time_dimensions = [
        f"date_trunc('{td.granularity.value if isinstance(td.granularity, TimeGranularity) else td.granularity}', "
        f"{td.field})"
        for td in query.time_dimensions or []
    ]
    fields = [*dimensions, *time_dimensions]
    if not fields:
        return query
    return query.model_copy(update={"order": [OrderItem.model_validate({"field": c, "implied": True}) for c in fields]})


def _sql_safety_reject_reason(safety: str) -> str | None:
    """Save-time reject reason for a classified ``dataset.sql``, or None to admit it."""
    if safety == "modifying":
        return "dataset SQL must be a read-only query; it is not a plain SELECT"
    if safety == "unparseable":
        return "dataset SQL could not be parsed for a read-only check"
    return None


def _substitute_dataset_sql_surfaces(
    *, dataset: SlayerDataset, variables: dict[str, Any], dialect: SqlDialect
) -> SlayerDataset:
    """Copy of ``dataset`` with ``{var}`` bound as literals into its four Mode-A surfaces (no-op when unneeded; never
    mutates input)."""
    return substitute_dataset_sql_surfaces(dataset=dataset, variables=variables, bind_mode_a=partial(bind_mode_a, dialect=dialect))


def _render_probe_dataset(dataset: SlayerDataset, *, dialect: SqlDialect) -> SlayerDataset:
    """Substitute a template dataset's own ``query_variables`` defaults for type-probing (raises on
    an undefaulted Mode-A variable); metrics are dropped, since the probe reads fields only."""
    dataset = dataset.model_copy(update={"metrics": []}) if dataset.metrics else dataset
    if dataset.source_dataset_origin is None and (
        dataset.query_variables or dataset_needs_substitution_pass(dataset)
    ):
        return _substitute_dataset_sql_surfaces(
            dataset=dataset, variables=dataset.query_variables, dialect=dialect
        )
    return dataset


def _stage_labels(*, stages, displays=None) -> List[str]:
    """Each stage's user-facing pointer; part of the dedup identity (distinguishes same-text stages)."""
    displays = displays or {}
    return [
        displays[q.name].label if q.name in displays
        else (f"stage {q.name!r}" if q.name else f"stages[{i}]")
        for i, q in enumerate(stages)
    ]


def _semi_join_filter_texts(planned_list) -> List[str]:
    """Distinct user filter texts pushed as semi-joins, for diagnostics."""
    texts: List[str] = []
    for plan in _iter_plans_with_producers(planned_list):
        for group in getattr(plan, "semi_join_filters", None) or ():
            for text in group.filter_texts:
                if text and text not in texts:
                    texts.append(text)
    return texts


def _collect_broadcast_warnings(
    *, planned_list, stages=None, displays=None, labels=None,
) -> List[BroadcastGrainWarningPayload]:
    """One broadcast payload per ``(stage location, metric label)``; dimensions unioned, the first attach's
    replacement named."""
    dims_by_key: "dict[tuple[str, str], list[tuple[str, str]]]" = {}
    replacement_of: "dict[tuple[str, str], tuple[Optional[str], list[str]]]" = {}
    if labels is None:
        labels = _stage_labels(stages=stages, displays=displays)
    for planned, location in zip(planned_list, labels):
        for attach in _walk_regroup_attaches(planned):
            metric = attach.broadcast_metric
            if not metric:
                continue
            dims = dims_by_key.setdefault((location, metric), [])
            replacement_of.setdefault((location, metric), (attach.broadcast_call, attach.broadcast_partition_by))
            seen = {d for d, _ in dims}
            for dim, reason in attach.broadcast_dimensions:
                if dim not in seen:
                    dims.append((dim, reason))
                    seen.add(dim)
    return [
        _broadcast_payload(
            metric=metric, location=location, dims=dims, replacement=replacement_of[(location, metric)],
        )
        for (location, metric), dims in dims_by_key.items()
    ]


def _broadcast_payload(
    *, metric: str, location: str, dims: "list[tuple[str, str]]", replacement: "tuple[Optional[str], list[str]]",
) -> BroadcastGrainWarningPayload:
    call, partition_by = replacement
    payload = BroadcastGrainWarningPayload(
        metric=metric, location=location, partition_by=partition_by,
        dimensions=[BroadcastDimension(dimension=d, reason=r) for d, r in dims],
    )
    if call is not None:
        payload.hint = broadcast_hint(call=call, partition_by=partition_by)
    return payload


def _collect_associated_warnings(
    *, planned_list, stages=None, displays=None, labels=None,
) -> List[AssociatedWarningPayload]:
    """One associated payload per ``(stage location, metric label)``; dimensions unioned."""
    dims_by_key: "dict[tuple[str, str], list[str]]" = {}
    if labels is None:
        labels = _stage_labels(stages=stages, displays=displays)
    for planned, location in zip(planned_list, labels):
        for attach in _walk_regroup_attaches(planned):
            metric = attach.associated_metric
            if not metric:
                continue
            dims = dims_by_key.setdefault((location, metric), [])
            for dim in attach.associated_dimensions:
                if dim not in dims:
                    dims.append(dim)
    return [
        AssociatedWarningPayload(metric=metric, location=location, dimensions=dims)
        for (location, metric), dims in dims_by_key.items()
    ]


def _collect_unrelated_warnings(*, planned_list, labels) -> List[UnrelatedDatasetWarningPayload]:
    """One unrelated-dataset payload per ``(stage location, dataset)``; metrics unioned."""
    found: "dict[tuple[str, str], tuple[str, list[str]]]" = {}
    for planned, location in zip(planned_list, labels):
        for attach in _walk_regroup_attaches(planned):
            if attach.unrelated_dataset is None:
                continue
            _population, metrics = found.setdefault(
                (location, attach.unrelated_dataset), (attach.unrelated_population or "", []),
            )
            metrics.extend(m for m in attach.unrelated_metrics if m not in metrics)
        _dimensionless_components(planned, location=location, found=found)
    return [
        UnrelatedDatasetWarningPayload(dataset=dataset, population=population or None, metrics=metrics,
                                       location=location)
        for (location, dataset), (population, metrics) in found.items()
    ]


def _dimensionless_components(
    planned: PlannedQuery, *, location: str, found: "dict[tuple[str, str], tuple[str, list[str]]]",
) -> None:
    """In an inferred stage without dimensions, unrelated measure homes are symmetric: one entry per home, none
    naming a population (the host is the planner's choice)."""
    supply = planned.stage_bundle.row_supply if planned.stage_bundle is not None else None
    host = planned.render_source_dataset
    entries = [k for k in found if k[0] == location]
    if supply is None or not supply.unit or not entries or host is None:
        return
    unrelated = {m for k in entries for m in found[k][1]}
    slots = {s.id: s for s in (*planned.row_slots, *planned.aggregate_slots, *planned.combined_expression_slots)}
    own = [name for sid in planned.projection if (name := slots[sid].public_name) and name not in unrelated]
    for k in entries:
        found[k] = ("", found[k][1])
    found[(location, host.name)] = ("", own)


def _collect_degenerate_warnings(
    *, planned_list, stages=None, displays=None, labels=None,
) -> List[DegenerateReaggregationWarningPayload]:
    """One degenerate-re-aggregation payload per ``(location, metric)``."""
    seen: set = set()
    out: List[DegenerateReaggregationWarningPayload] = []
    if labels is None:
        labels = _stage_labels(stages=stages, displays=displays)
    for planned, location in zip(planned_list, labels):
        for attach in _walk_regroup_attaches(planned):
            metric = attach.degenerate_metric
            if not metric:
                continue
            identity = (location, metric)
            if identity in seen:
                continue
            seen.add(identity)
            out.append(DegenerateReaggregationWarningPayload(
                metric=metric, location=location,
                operand_grain=list(attach.degenerate_operand_grain),
                outer_grain=list(attach.degenerate_outer_grain),
            ))
    return out


def _attach_semi_join_texts(attach) -> Iterator[str]:
    """Non-empty semi-join-pushed filter texts on an attach's producer plan, plus
    an association producer's inlined reachable-but-unsafe conjunct texts — both
    surface the same informational entry."""
    for group in getattr(attach.producer_plan, "semi_join_filters", None) or ():
        yield from (text for text in group.filter_texts if text)
    yield from (
        text
        for text in getattr(attach, "association_restricted_filter_texts", None) or ()
        if text
    )


def _population_pushed_entries(planned) -> Iterator[Tuple[Optional[str], str]]:
    """The population's own restriction: a top-level plan's semi-join groups name
    no aggregate (metric=None)."""
    for group in getattr(planned, "semi_join_filters", None) or ():
        for text in group.filter_texts:
            if text:
                yield None, text


def _attach_pushed_entries(planned) -> Iterator[Tuple[str, str]]:
    """Semi-join-pushed entries for each regroup attach, one per (public metric,
    filter text) — a producer may carry several public metrics under one push."""
    for attach in _walk_regroup_attaches(planned):
        metrics = attach.population_semi_join_metrics or [
            attach.semi_join_metric or attach.broadcast_metric
            or attach.associated_metric or attach.alias_hint or "<aggregate>"
        ]
        for text in _attach_semi_join_texts(attach):
            for metric in metrics:
                yield metric, text


def _collect_semi_join_pushed_warnings(
    *, planned_list, stages=None, displays=None, labels=None,
) -> List[SemiJoinPushedWarningPayload]:
    """Response-only informational entries for semi-join-pushed conjuncts; one per
    ``(location, aggregate, filter text)``."""
    seen: set = set()
    out: List[SemiJoinPushedWarningPayload] = []
    if labels is None:
        labels = _stage_labels(stages=stages, displays=displays)
    for planned, location in zip(planned_list, labels):
        entries = (
            *_population_pushed_entries(planned),
            *_attach_pushed_entries(planned),
        )
        for metric, text in entries:
            identity = (location, metric, text)
            if identity in seen:
                continue
            seen.add(identity)
            out.append(SemiJoinPushedWarningPayload(
                metric=metric, location=location, filter_text=text,
            ))
    return out


def _raise_on_error_events(
    *, broadcasts: List[BroadcastGrainWarningPayload],
) -> None:
    """``to_many_handling: "error"``: a broadcast is an error naming the metric + remedy."""
    if broadcasts:
        w = broadcasts[0]
        dims = ", ".join(d.dimension for d in w.dimensions)
        reason = w.dimensions[0].reason if w.dimensions else ""
        raise SlayerError(
            f"error mode: metric {w.metric!r} would broadcast across "
            f"unattributable dimension(s) {dims} ({reason}); declare relationship "
            f"cardinality, a covering unique key, omit to_many_handling to "
            f"associate, or remove the dimension."
        )


def _emit_python_warnings(response) -> None:
    """Emit one Python ``UserWarning`` per broadcast / associated metric."""

    for w in response.warnings or ():
        if isinstance(w, BroadcastGrainWarningPayload):
            reason = w.dimensions[0].reason if w.dimensions else ""
            _warnings_module.warn(
                BroadcastGrainWarning(metric=w.metric, reason=reason),
                stacklevel=outside_stacklevel(),
            )
        elif isinstance(w, AssociatedWarningPayload):
            _warnings_module.warn(
                AssociatedGrainWarning(
                    metric=w.metric, dimensions=", ".join(w.dimensions),
                ),
                stacklevel=outside_stacklevel(),
            )
        elif isinstance(w, WholePeriodsNonNestingWarningPayload):
            _warnings_module.warn(SlayerWholePeriodsWarning(w), stacklevel=outside_stacklevel())
        elif isinstance(w, WholeListWarning):
            _warnings_module.warn(SlayerWholeListWarning(w), stacklevel=outside_stacklevel())
        elif isinstance(w, UnrelatedDatasetWarningPayload):
            _warnings_module.warn(SlayerUnrelatedDatasetWarning(w), stacklevel=outside_stacklevel())
        elif isinstance(w, LibraryNameShadowWarning):
            _warnings_module.warn(SlayerLibraryNameShadowWarning(w), stacklevel=outside_stacklevel())
        elif isinstance(w, NameCollisionWarning):
            _warnings_module.warn(SlayerNameCollisionWarning(w), stacklevel=outside_stacklevel())
        elif isinstance(w, DateOnlyMidnightWarning):
            _warnings_module.warn(SlayerDateOnlyMidnightWarning(w), stacklevel=outside_stacklevel())


class SlayerResponse(BaseModel):
    """Response from a SLayer query."""

    data: List[Dict[str, Any]]
    columns: List[str] = PydanticField(default_factory=list)
    sql: Optional[str] = None
    attributes: ResponseAttributes = PydanticField(default_factory=ResponseAttributes)
    # Query advisories, discriminated on ``kind``; empty for a clean query.
    warnings: List[AnySlayerWarning] = PydanticField(default_factory=list)
    # The effective population dataset and whether it was inferred.
    population: Optional[str] = None
    population_inferred: bool = False
    # The datasets supplying the rows (``["unit"]`` with no dimensions).
    row_sources: List[str] = PydanticField(default_factory=list)
    # The datasource the query ran on; never serialized.
    datasource: Optional[DatasourceConfig] = PydanticField(default=None, exclude=True, repr=False)

    @model_validator(mode="after")
    def _populate_fields(self) -> "SlayerResponse":
        if not self.columns and self.data:
            self.columns = list(self.data[0].keys())
        return self

    @property
    def row_count(self) -> int:
        return len(self.data)

    def _format_value(self, column: str, value: Any) -> str:
        """Format a single cell value using column format metadata if available."""
        fm = self.attributes.get(column)
        if fm and fm.format and isinstance(value, (int, float, decimal.Decimal)):
            return format_number(value=value, format_spec=fm.format)
        return encode_text(value)

    def to_markdown(self) -> str:
        """Format data as a Markdown table with number formatting applied."""
        if not self.data:
            return "No results."
        header = "| " + " | ".join(self.columns) + " |"
        separator = "| " + " | ".join("---" for _ in self.columns) + " |"
        body_lines = []
        for row in self.data:
            cells = [self._format_value(column=c, value=row.get(c, "")) for c in self.columns]
            body_lines.append("| " + " | ".join(cells) + " |")
        return "\n".join([header, separator] + body_lines)


class _Prepared(BaseModel):
    """DB-free product of ``_prepare_pipeline``; ``sql`` is the final rewritten SQL, ds-fingerprint computed lazily."""

    model_config = PydanticConfigDict(arbitrary_types_allowed=True)

    sql: str
    dialect: str
    datasource: DatasourceConfig
    #: The physical tables the executed statement reads (cache invalidation).
    tables: List[NormalizedTable] = PydanticField(default_factory=list)
    resolved_data_source: Optional[str] = None
    attributes: Any
    expected_columns: List[str]
    # The persisted datasets the statement reads (drift-attribution scope).
    touched: Set[str]
    # The elements of those datasets it reads (drift attribution).
    reads: StatementReads = PydanticField(default_factory=StatementReads)
    dataset: SlayerDataset
    slack_warnings: List[Any] = PydanticField(default_factory=list)
    population: Optional[str] = None
    population_inferred: bool = False
    row_sources: List[str] = PydanticField(default_factory=list)
    #: The SQL evaluates ``now()`` / ``current_date()``: its result must never be cached.
    reads_clock: bool = False


class _Rendered(BaseModel):
    """One planned statement tree, before policy, response metadata and its render."""

    model_config = PydanticConfigDict(arbitrary_types_allowed=True)

    built: PlannedStatement
    dialect: str
    datasource: DatasourceConfig
    bundle: ResolvedSourceBundle
    planned_list: List[PlannedQuery]
    # The stages the SQL emits (a spliced stage nothing reads is pruned).
    emitted_list: List[PlannedQuery]
    dataset: SlayerDataset
    # Datasets whose relations the final statement contains, plus the spliced ones.
    reads: Set[str] = PydanticField(default_factory=set)
    # Datasets whose filters the statement restricts by a correlated subquery.
    associated: Set[str] = PydanticField(default_factory=set)
    element_reads: StatementReads = PydanticField(default_factory=StatementReads)
    warnings: List[Any] = PydanticField(default_factory=list)
    population: Optional[str] = None
    population_inferred: bool = False
    row_sources: List[str] = PydanticField(default_factory=list)

    @property
    def statement(self) -> exp.Select:
        return self.built.statement


class _StageInference(BaseModel):
    """One stage planned from its population; ``supply`` set when its rows are inferred (semantics 12.4)."""

    query: SlayerQuery
    data_source: Optional[str] = None
    reported: Optional[str] = None
    supply: Optional[RowSupply] = None
    row_sources: List[str] = PydanticField(default_factory=list)
    # The anchor a save pins as the stage's population.
    pin: Optional[str] = None
    # A named population planned from a metric's home (its rows stay the population's).
    anchored: bool = False


class _Inference(BaseModel):
    """A statement's stages planned from their populations, and what the main query reports."""

    query: SlayerQuery
    named_queries: Dict[str, SlayerQuery]
    population: Optional[str] = None
    population_inferred: bool = False
    data_source: Optional[str] = None
    row_sources: List[str] = PydanticField(default_factory=list)
    supplies: Dict[str, RowSupply] = PydanticField(default_factory=dict)
    pins: Dict[str, Optional[str]] = PydanticField(default_factory=dict)


class _QueryBackedExpansion(BaseModel):
    """A query-backed dataset planned as one statement tree, read as a virtual dataset over its root stage."""

    model_config = PydanticConfigDict(arbitrary_types_allowed=True)

    name: str
    rendered: _Rendered
    stage_schema: StageSchema
    #: Canonical field name → its length-fitted alias.
    fit_map: Dict[str, str]

    def virtual_dataset(self, *, sql: Optional[str] = None) -> SlayerDataset:
        """The virtual dataset over the root stage, reading ``sql`` (else the stage relation)."""
        source = self.rendered.bundle.population
        assert source is not None
        return dataset_from_stage_schema(
            name=self.name,
            schema=self.stage_schema,
            data_source=source.data_source,
            sql=sql,
            field_sql={c.name: self.fit_map.get(c.name, c.name) for c in self.stage_schema.fields},
            default_time_dimension=source.effective_default_time_dimension,
        )


def _granularity_conflict_warnings(
    *, planned_list: List[PlannedQuery], labels: List[str],
) -> List[WholePeriodsNonNestingWarningPayload]:
    """One payload per non-nesting ``whole_periods_only`` granularity pair, per stage."""
    return [
        WholePeriodsNonNestingWarningPayload(
            field=c.field, granularities=[str(g) for g in c.granularities], location=location,
        )
        for planned, location in zip(planned_list, labels)
        for c in planned.granularity_conflicts
    ]


def _plan_label(*, planned: PlannedQuery, index: int, root: Optional[SlayerQuery]) -> str:
    """A planned stage's user-facing pointer (``root`` given for the root plan)."""
    if root is not None:
        return f"stage {root.name!r}" if root.name else f"stages[{index}]"
    return planned.stage_schema.label if planned.stage_schema is not None else f"stages[{index}]"


def _bare_source_name(key: ValueKey) -> Optional[str]:
    """The field ``key``'s source reads directly (a bare field), else ``None``."""
    src = key.source if isinstance(key, (AggregateKey, TimeTruncKey, CastKey, ListSetKey)) else None
    if isinstance(src, FieldKey):
        return src.leaf or None
    return src.field_name if isinstance(src, FieldSqlKey) else None


def _reject_expressions_unparseable_everywhere(dataset: SlayerDataset) -> None:
    for agg in dataset.aggregations:
        if not agg.expression:
            continue
        if agg.name in RANKED_AGGREGATIONS:
            raise AggregationArgumentError(
                f"Dataset '{dataset.name}', aggregation '{agg.name}': a ranked aggregation cannot take an expression."
            )
        errors: list[SqlTemplateError] = []
        for dialect in SQLGLOT_NAMES:
            try:
                sql_template(text=agg.expression, dialect=dialect)
                break
            except SqlTemplateError as e:
                errors.append(e)
        else:
            raise SqlTemplateError(f"Dataset '{dataset.name}', aggregation '{agg.name}': {errors[0]}")


class SlayerQueryEngine:
    """Central orchestrator: resolves queries via storage, generates SQL, executes."""

    def __init__(
        self,
        storage: StorageBackend,
        *,
        policy: Optional[SessionPolicy] = None,
        cache_config: Optional[CacheConfig] = None,
        clock: Optional[Callable[[], datetime]] = None,
        client_factory: Optional[ClientFactory] = None,
    ):
        self.storage: StorageBackend = EngineStorage(storage, client_source=self.client_for)
        self._client_factory = client_factory
        # Read once per execution; relative time points resolve against it.
        self._clock = clock if clock is not None else host_clock()
        # Per-engine, opt-in query result cache (defaults to cache-indefinitely).
        self._cache = QueryCache(config=cache_config or CacheConfig())
        # One client per datasource name and connection identity, built only by ``client_for``.
        self._sql_clients: dict[ClientCacheKey, SqlClient] = {}
        self._policy = policy
        # Column-presence facts; an unconfirmable ``None`` is re-probed, never cached.
        self._column_presence_cache: dict[tuple, bool] = {}
        # Live-schema facts reused by query-time drift attribution.
        self._drift_snapshots = schema_drift.LiveSnapshotCache()
        # Live source-column types a portable definition's undeclared columns read, by table.
        self._source_column_types: dict[tuple, dict[str, DataType]] = {}
        self._unknown_types = UnknownTypes()

    async def _resolve_unknown_types(self, dataset: SlayerDataset) -> SlayerDataset:
        return await self._unknown_types.resolve(dataset, storage=self.storage, client_for=self.client_for)

    async def _live_column_types(self, *, datasource: DatasourceConfig, dataset: SlayerDataset) -> dict[str, DataType]:
        """``dataset``'s live source columns and their types, when a definition needs them; empty when unknown."""
        if not dataset.sql_table or not needs_live_types(dataset, ds_type=datasource.type):
            return {}
        key = (_sql_client_cache_key(datasource), dataset.sql_table)
        if key not in self._source_column_types:
            client = self.client_for(datasource)
            try:
                location = await live_table_location(dataset.sql_table, datasource, client)
                columns = await client.reflect_columns(location) if location is not None else []
            except Exception as exc:  # unreachable: source columns stay Opaque
                logger.warning("Could not read the columns of %s: %s", dataset.sql_table, exc)
                return {}
            self._source_column_types[key] = {c.name: c.narrowed_type for c in columns}
        return self._source_column_types[key]

    async def _resolved(
        self, dataset: SlayerDataset, *, datasource: DatasourceConfig, datasets: Sequence[SlayerDataset],
    ) -> SlayerDataset:
        """``dataset`` as it runs on ``datasource`` (each field carrying its native SQL)."""
        if dataset.source_queries:
            return dataset
        live = await self._live_column_types(datasource=datasource, dataset=dataset)
        return resolved_dataset(dataset, ds_type=datasource.type, datasets=datasets, live=live)

    async def resolved_datasets(
        self, datasets: Sequence[SlayerDataset], *, datasource: DatasourceConfig,
    ) -> dict[str, SlayerDataset | Exception]:
        """Each of ``datasets`` as it runs on ``datasource``, or why it does not resolve, by name."""
        out: dict[str, SlayerDataset | Exception] = {}
        for dataset in datasets:
            try:
                out[dataset.name] = await self._resolved(dataset, datasource=datasource, datasets=datasets)
            except Exception as exc:  # noqa: BLE001 — reported by the caller per dataset
                out[dataset.name] = exc
        return out

    async def _resolved_bundle(
        self, bundle: ResolvedSourceBundle, *, datasource: DatasourceConfig,
    ) -> ResolvedSourceBundle:
        """``bundle`` with every dataset's row definitions resolved for ``datasource``."""
        peers = list(bundle.referenced_datasets)
        done: Dict[str, SlayerDataset] = {}

        async def one(dataset: SlayerDataset) -> SlayerDataset:
            if dataset.name not in done:
                done[dataset.name] = await self._resolved(dataset, datasource=datasource, datasets=peers)
            return done[dataset.name]

        population = await one(bundle.population) if bundle.population is not None else None
        referenced = [population if population is not None and d.name == population.name else await one(d)
                      for d in bundle.referenced_datasets]
        stages = {name: await self._resolved(d, datasource=datasource, datasets=peers)
                  for name, d in bundle.stage_source_datasets.items()}
        resolved = bundle.model_copy(update={
            "population": population, "referenced_datasets": referenced, "stage_source_datasets": stages,
        })
        return resolved.model_copy(update={"spliced_rows": _spliced_rows(resolved)})

    @property
    def policy(self) -> Optional[SessionPolicy]:
        """Engine-global forced-filter policy; read-only, so engine-scoped caches are policy-scoped."""
        return self._policy

    @property
    def cache_config(self) -> CacheConfig:
        """The active :class:`CacheConfig` (read-only view of ``_cache``)."""
        return self._cache.config

    @cache_config.setter
    def cache_config(self, config: CacheConfig) -> None:
        """Reassign the cache policy; clears the cache (preserving the clock)."""
        self._cache = QueryCache(config=config, clock=self._cache._clock)

    @property
    def cache_size(self) -> int:
        return self._cache.size()

    def clear_cache(self) -> None:
        self._cache.clear()

    @probes.probe(name="query.policy")
    async def _apply_policy(
        self, *, statement: Expr, dialect: str, datasource: DatasourceConfig
    ) -> Expr:
        """``statement`` rewritten to enforce the forced-filter policy; unchanged when no policy."""
        if not self.policy:
            return statement
        has_column = await self._has_column(statement=statement, datasource=datasource, policy=self.policy)
        return apply_session_policy(
            statement, sql_dialect=get_dialect(dialect), policy=self.policy, has_column=has_column,
        )

    async def _has_column(
        self, *, statement: Expr, datasource: DatasourceConfig, policy: SessionPolicy,
    ) -> HasColumn:
        """The column presence facts the policy rewrite of ``statement`` asks for, read up front."""
        present = {
            check: await self._column_present(datasource=datasource, scoped_table=check[0], column=check[1])
            for check in policy_column_checks(statement, policy=policy)
        }
        return lambda scoped, column: present.get((scoped, column))

    async def _render_dialect(
        self, *, dialect: str, datasource: DatasourceConfig, planned_list,
    ) -> "str | SqlDialect":
        """The datasource's own dialect (a MariaDB source renders through ``MariadbDialect``, not its sqlglot
        name's), carrying the server profile when rendering the plan reads it; an unreadable profile renders
        as an unknown server. A library call the dialect does not translate is refused first."""
        sql_dialect = dialect_for_ds_type(datasource.type)
        if sql_dialect.sqlglot_name != dialect:
            sql_dialect = get_dialect(dialect)
        check_library_support(sql_dialect, planned_keys(planned_list))
        if not sql_dialect.profile_needed(planned_keys(planned_list)):
            return sql_dialect
        try:
            profile = await self.client_for(datasource).server_profile()
        except Exception:  # noqa: BLE001 — an unknown server gets the plain cast
            profile = ServerProfile()
        return sql_dialect.model_copy(update={"server_profile": profile})

    async def _require_correlated_subqueries(
        self, *, dialect: str, datasource: DatasourceConfig, planned_list, statement: Expr, associated: Set[str],
    ) -> None:
        """Fail closed when this user cannot run a correlated ``EXISTS``: a semi-join pushdown, or a
        dataset filter read across an unproven hop."""
        sql_dialect = get_dialect(dialect)
        if not sql_dialect.correlated_subqueries_gated:
            return
        texts = _semi_join_filter_texts(planned_list)
        named = f" (filter(s): {', '.join(repr(t) for t in texts)})" if texts else ""
        lead = (
            f"This query pushes a filter into a related dataset as a correlated EXISTS semi-join{named}."
            if texts or not associated else
            f"This query reads dataset(s) {sorted(associated)} whose filters read a joined dataset across a hop not "
            f"proven to-one, restricted by a correlated EXISTS."
        )
        try:
            profile = await self.client_for(datasource).server_profile()
        except Exception as exc:
            # An unreadable profile is refused like an empty one; the next query re-probes.
            reason = sql_dialect.correlated_subquery_refusal(ServerProfile(), statement=statement)
            raise SlayerError(f"{lead} {reason}") from exc
        reason = sql_dialect.correlated_subquery_refusal(profile, statement=statement)
        if reason is not None:
            raise SlayerError(f"{lead} {reason} Alternatively, restructure the filter.")

    async def _column_present(
        self,
        *,
        datasource: DatasourceConfig,
        scoped_table: ScopedTable,
        column: str,
    ) -> bool | None:
        """Whether ``column`` exists on ``scoped_table``: ``True``/``False``/``None`` (only confirmed results cached)."""
        schema = scoped_table.schema_name or datasource.schema_name
        # Cross-catalog refs can't be confirmed (introspection takes no catalog
        # argument) — fail closed rather than risk an under-filter.
        if scoped_table.catalog and (
            not datasource.database
            or scoped_table.catalog.casefold() != datasource.database.casefold()
        ):
            return None
        # Catalog in the key so BigQuery-style catalog twins never share a fact.
        key = (
            _sql_client_cache_key(datasource),
            scoped_table.catalog,
            schema,
            scoped_table.name,
            column,
        )
        if key in self._column_presence_cache:
            return self._column_presence_cache[key]
        try:
            # Carry the catalog so a same-named twin in another catalog never unions in.
            cols = await self.client_for(datasource).reflect_columns(
                TableLocation(name=scoped_table.name, schema_name=schema, catalog=scoped_table.catalog),
            )
        except Exception as exc:  # introspection failed -> cannot confirm
            logger.warning(
                "Forced filter: column-presence probe failed for %s.%s "
                "(column %r): %s",
                schema or "<default>",
                scoped_table.name,
                column,
                exc,
            )
            return None
        if not cols:
            return None  # no columns resolved -> cannot confirm
        names = {c.name.lower() for c in cols}
        present = column.lower() in names
        self._column_presence_cache[key] = present
        return present

    @classmethod
    def _topologically_order_queries(
        cls,
        queries: List["SlayerQuery"],
    ) -> List["SlayerQuery"]:
        """Shim delegating to :func:`topologically_order_stages`; reads populations and extensions only — the planner,
        with the datasets, orders by expressions too."""
        return topologically_order_stages(queries, expressions=False)

    async def aclose(self) -> None:
        """Release every client's loop-bound resources; the clients stay cached and usable."""
        self._drift_snapshots.clear()
        for client in list(self._sql_clients.values()):  # NOSONAR(S7504) — snapshot: a concurrent client_for may add a client during the await
            try:
                await client.aclose()
            except Exception:
                logger.warning("Failed to release a SQL client during engine aclose.", exc_info=True)

    async def release_local_connections(self) -> None:
        """Free the clients' idle connections holding a local database file; a client without that release is skipped."""
        for client in list(self._sql_clients.values()):  # NOSONAR(S7504) — snapshot: a concurrent client_for may add a client during the await
            release = getattr(client, "release_local_connections", None)
            if release is None:
                continue
            try:
                await release()
            except Exception:
                logger.warning("Failed to release a SQL client's local connections.", exc_info=True)

    def run_sync(self, workflow: Coroutine[Any, Any, _T]) -> _T:
        """Run one whole synchronous workflow on the bridge loop, releasing idle local-file connections at
        its end — also when it raises or is cancelled; networked connections stay open."""

        async def _run() -> _T:
            try:
                return await workflow
            finally:
                await asyncio.shield(self.release_local_connections())

        return run_sync(_run())

    def close(self) -> None:
        """Close every client this engine built and forget them; a later query rebuilds its clients.

        Idempotent; continues past a client that fails to close. Clients release their bridge-loop
        connections here; other loops' async resources are released by ``aclose()`` on their loop.
        """
        try:
            for client in list(self._sql_clients.values()):  # NOSONAR(S7504) — snapshot: another thread's client_for may add a client meanwhile
                try:
                    client.close()
                except Exception:
                    logger.warning(
                        "Failed to close a SQL client during engine close.",
                        exc_info=True,
                    )
        finally:
            self._sql_clients.clear()
            self._drift_snapshots.clear()

    @probes.probe(
        name="query",
        params={"query.dry_run": "dry_run", "query.explain": "explain"},
        result=lambda response: {"query.rows": len(response.data)},
    )
    async def execute(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        dry_run: bool = False,
        explain: bool = False,
        data_source: Optional[str] = None,
        cache: bool = False,
        refine: "QueryRefinement | dict | None" = None,
    ) -> SlayerResponse:
        runtime_kwarg = variables or {}
        deprecations: List[DeprecatedInputWarning] = []
        with collect_legacy_spellings() as legacy, announced_by_caller():
            run_input = _validated_run_input(resolve_run_input(query, refine))
            _check_fresh_result_names(run_input)
            main_query, named_queries, prefer_data_source, splice_chain = await self._normalize_input(
                run_input, runtime_kwarg=runtime_kwarg, prefer_data_source=data_source,
                deprecations_out=deprecations,
            )
            deprecations.extend(broadcast_deprecations([*named_queries.values(), main_query]))
            response = await self._execute_pipeline(
                query=main_query,
                named_queries=named_queries,
                splice_chain=splice_chain,
                runtime_kwarg=runtime_kwarg,
                dry_run=dry_run,
                explain=explain,
                prefer_data_source=prefer_data_source,
                cache=cache,
                original_input=run_input,
                original_data_source=data_source,
            )
        if deprecations:
            response = response.model_copy(update={"warnings": [*response.warnings, *deprecations]})
        # The one Python-warnings emission: after the response is built (under
        # ``-W error`` this raises) and path-independent (dry_run/explain/execute).
        response.warnings.extend(emit_deprecations(legacy))
        _emit_python_warnings(response)
        return response

    @probes.probe(name="query.normalize")
    async def _normalize_input(  # NOSONAR S3776 — public dispatch over str/dict/list/SlayerQuery; splitting hides the input-shape contract
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str | SavedQueryRun",
        *,
        runtime_kwarg: Dict[str, Any],
        prefer_data_source: Optional[str],
        deprecations_out: Optional[List[DeprecatedInputWarning]] = None,
    ) -> "tuple[SlayerQuery, Dict[str, SlayerQuery], Optional[str], Tuple[str, ...]]":
        """Resolve the user input union into ``(main_query, named_queries, prefer_data_source,
        splice_chain)``; running a query-backed dataset by name seeds the chain with it.
        The deprecations of the query objects and refinement it consumed go to ``deprecations_out``."""
        out = deprecations_out if deprecations_out is not None else []
        # Run-by-name: ``execute("dataset_name", ...)`` runs the backing query.
        if isinstance(query, str):
            query = SavedQueryRun(name=query)
        if isinstance(query, SavedQueryRun):
            return await self._normalize_by_name(
                run=query,
                runtime_kwarg=runtime_kwarg,
                prefer_data_source=prefer_data_source,
                deprecations_out=out,
            )

        if isinstance(query, list):
            if not query:
                raise ValueError(
                    "'query' must be a non-empty list when passing staged queries."
                )
            with one_document():
                queries = [SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in query]
            out.extend(deprecated_inputs_of(q for q in queries if isinstance(q, SlayerQuery)))
            queries = await qualified_input(queries, storage=self.storage, data_source=prefer_data_source, deprecations_out=out)
            # Reorder so each stage follows the siblings it references (last stays
            # the entry point); validates names / dups / self-refs / cycles.
            queries = self._topologically_order_queries(queries)
            main_query = queries[-1]
            named_queries = {q.name: q for q in queries[:-1] if q.name}
        else:
            if isinstance(query, dict):
                query = SlayerQuery.model_validate(query)
            out.extend(deprecated_inputs_of([query] if isinstance(query, SlayerQuery) else []))
            [main_query] = await qualified_input([query], storage=self.storage, data_source=prefer_data_source, deprecations_out=out)
            named_queries = {}

        # Merge ``variables=`` kwarg into query.variables (runtime wins).
        if runtime_kwarg:
            merged_top = {**(main_query.variables or {}), **runtime_kwarg}
            if merged_top != (main_query.variables or {}):
                main_query = main_query.model_copy(update={"variables": merged_top})

        return main_query, named_queries, prefer_data_source, ()

    async def _normalize_by_name(
        self,
        *,
        run: SavedQueryRun,
        runtime_kwarg: Dict[str, Any],
        prefer_data_source: Optional[str],
        deprecations_out: List[DeprecatedInputWarning],
    ) -> "tuple[SlayerQuery, Dict[str, SlayerQuery], Optional[str], Tuple[str, ...]]":
        """Normalize a run-by-name input into the shared prepare tuple (``prefer_data_source`` pins the lookup).
        The deprecations of the stored stages and the refinement go to ``deprecations_out``."""
        name = run.name
        dataset = await self.storage.get_dataset(name, data_source=prefer_data_source)
        if dataset is None:
            raise ValueError(f"Dataset '{name}' not found")
        if not dataset.source_queries:
            raise ValueError(
                f"Dataset '{name}' is not query-backed; pass a SlayerQuery "
                f"with population='{name}'."
            )
        deprecations_out.extend(deprecated_inputs_of([*dataset.source_queries, run.refinement]))
        prefer_data_source = dataset.data_source or prefer_data_source
        if run.refinement is not None and run.refinement.model_fields_set:
            # Legacy unpinned stages pin from the unrefined stage, so a refinement never moves the source.
            dataset = await self._pin_populations(dataset=dataset, prefer_data_source=prefer_data_source)
            *earlier, final = dataset.source_queries or []
            conversions = Conversions()
            refinement = convert_refinement(
                run.refinement, saved=final, conversions=conversions,
                anchors=await anchor_names(self.storage, dataset.source_queries or [], data_source=prefer_data_source,
                                           refinement=run.refinement),
            )
            deprecations_out.extend(conversions.payloads)
            dataset = dataset.model_copy(update={
                "source_queries": [*earlier, refine_query(saved=final, refinement=refinement)],
            })
        main_query, named_queries = self._stages_of_dataset(dataset=dataset, runtime_kwarg=runtime_kwarg)
        return main_query, named_queries, prefer_data_source, (dataset.name,)

    @staticmethod
    def _stages_of_dataset(
        *, dataset: SlayerDataset, runtime_kwarg: Dict[str, Any],
    ) -> "tuple[SlayerQuery, Dict[str, SlayerQuery]]":
        """A query-backed dataset's stages as ``(main_query, named_queries)``, variables layered."""
        # Stored ``source_queries`` may be non-topological for
        # ``relationships[].to`` deps; topo-sort to match save-time semantics.
        stages = topologically_order_stages(list(dataset.source_queries or []), expressions=False)
        if not stages:
            raise ValueError(f"Dataset {dataset.name!r} has no source_queries")
        main_query = stages[-1]
        named_queries = {q.name: q for q in stages[:-1] if q.name}
        # Precedence ``runtime > stage > model_defaults`` (no outer query here,
        # so ``dataset.query_variables`` is the lowest layer).
        merged = _merge_query_variables(
            outer=dataset.query_variables,
            stage=main_query.variables,
            runtime=runtime_kwarg,
        )
        if merged != (main_query.variables or {}):
            main_query = main_query.model_copy(update={"variables": merged})
        return main_query, named_queries

    @probes.probe(
        name="query.prepare",
        result=lambda prepared: {"datasource.name": prepared.datasource.name,
                                 "datasource.type": prepared.datasource.type or ""},
    )
    async def _prepare_pipeline(  # NOSONAR S3776 — linear pipeline (resolve→bind→generate→policy); breaking it up obscures the order of operations
        self,
        query: SlayerQuery,
        named_queries: Dict[str, SlayerQuery],
        runtime_kwarg: Dict[str, Any],
        *,
        prefer_data_source: Optional[str] = None,
        override_datasource: Optional[DatasourceConfig] = None,
        splice_chain: Tuple[str, ...] = (),
    ) -> _Prepared:
        """Prepare portion shared by execute / evict / refresh (resolve→…→response-metadata).

        Produces the final executed SQL; no SQL client on the no-policy path (so
        ``evict()`` recomputes a key without connecting).
        """
        rendered = await self._plan_and_render(
            query=query, named_queries=named_queries, runtime_kwarg=runtime_kwarg,
            prefer_data_source=prefer_data_source, override_datasource=override_datasource,
            splice_chain=splice_chain, result_order=True,
        )
        built, dialect, datasource = rendered.built, rendered.dialect, rendered.datasource
        planned_list = rendered.planned_list
        root_planned = planned_list[-1]
        if rendered.associated or any(plan_has_semi_join_filters(p) for p in rendered.emitted_list):
            await self._require_correlated_subqueries(
                dialect=dialect, datasource=datasource, planned_list=rendered.emitted_list,
                statement=built.statement, associated=rendered.associated,
            )
        # Forced-filter rewrite before dry-run / explain / execute so all three
        # (and the cache key) see the policy-rewritten SQL; no-op without a policy.
        statement = await self._apply_policy(statement=built.statement, dialect=dialect, datasource=datasource)
        attributes, expected_columns = build_response_metadata(
            root_planned=root_planned, bundle=root_planned.stage_bundle or rendered.bundle,
            statement=statement,
        )
        tables = [QueryCache._normalize_table_expr(t, dialect) for t in physical_tables(statement)]
        sql = built.model_copy(update={"statement": statement}).render(policy_scoped=self.policy is not None)
        logger.debug("Generated SQL:\n%s", sql)

        return _Prepared(
            sql=sql,
            dialect=dialect,
            datasource=datasource,
            tables=tables,
            resolved_data_source=datasource.name,
            attributes=attributes,
            expected_columns=list(expected_columns),
            touched=rendered.reads,
            reads=rendered.element_reads,
            dataset=rendered.dataset,
            slack_warnings=rendered.warnings,
            population=rendered.population,
            population_inferred=rendered.population_inferred,
            row_sources=rendered.row_sources,
            reads_clock=plans_read_clock(rendered.emitted_list),
        )

    async def _plan_and_render(self, **kwargs: Any) -> _Rendered:
        """Plan and render one statement (see ``_plan_and_render_statement``), with the elements it reads."""
        with collect_bind_records() as records:
            rendered = await self._plan_and_render_statement(**kwargs)
        reads = statement_reads(plans=rendered.emitted_list, bundle=rendered.bundle, metrics=records.metrics)
        # A pruned stage's shadowing call is not made by the statement.
        shadows = [w for w in records.shadows.values() if (w.dataset, w.aggregation) in reads.aggregations]
        collisions = _read_dataset_collisions(reads=reads, bundle=rendered.bundle)
        return rendered.model_copy(update={
            "element_reads": reads,
            "warnings": [*rendered.warnings, *shadows, *collisions, *records.notices.values()],
        })

    async def _plan_and_render_statement(  # NOSONAR S3776 — linear pipeline (localize→infer→bundle→normalize→variables→plan→render); splitting hides the order of operations
        self,
        *,
        query: SlayerQuery,
        named_queries: Dict[str, SlayerQuery],
        runtime_kwarg: Dict[str, Any],
        prefer_data_source: Optional[str],
        override_datasource: Optional[DatasourceConfig] = None,
        splice_chain: Tuple[str, ...] = (),
        result_order: bool = False,
        type_probe: bool = False,
    ) -> _Rendered:
        """Plan one statement and compose its tree, splicing the stored query-backed datasets
        it reads; warnings come from the stages the statement emits. ``result_order`` marks a
        request's result (default-ordered); ``type_probe`` bounds it as a type probe."""
        # Stage names are query-local: mint identities before anything resolves a name.
        localized, stage_displays = localize_stages([*named_queries.values(), query])
        localized, respelled_spine = await self._respell_spine(
            queries=localized, stage_names={d.name for d in stage_displays.values()}, data_source=prefer_data_source,
        )
        scope = await model_scope(self.storage, localized, data_source=prefer_data_source)
        localized = [expand_model_scoped(q, scope=scope) for q in localized]
        query = localized[-1]
        named_queries = {q.name: q for q in localized[:-1] if q.name}
        # Infer the population of any rootless stage (main + each named
        # stage independently) BEFORE prefix-strip, so the chosen dataset flows
        # through the untouched pipeline byte-identically to its explicit twin.
        inference = await self._infer_populations(
            query=query,
            named_queries=named_queries,
            prefer_data_source=prefer_data_source,
            stage_displays=stage_displays,
        )
        query, named_queries = inference.query, inference.named_queries
        population, population_inferred = inference.population, inference.population_inferred
        inferred_data_source = inference.data_source
        # Pin bundle resolution to the inferred datasource (anchor voting already
        # fixed exactly one), so the winning bare dataset name can't re-resolve to a
        # same-named dataset in another datasource.
        if inferred_data_source is not None:
            prefer_data_source = inferred_data_source

        # Build the resolved bundle once — the only storage consult; the binder
        # then reads from the bundle purely.
        failures = DocumentLoadFailures()
        bundle = await slayer.engine.bundle_builder.build_resolved_source_bundle(
            query=query,
            storage=self.storage,
            data_source=prefer_data_source,
            runtime_variables=runtime_kwarg,
            named_queries=named_queries,
            stage_displays=stage_displays,
            splice_chain=splice_chain,
            now=self._clock(),
            resolve_types=self._resolve_unknown_types,
            failures=failures,
            respelled_spine=respelled_spine,
        )
        # Plan the stages as the bundle resolved them (extension dataset names respelled).
        query, named_queries = bundle.query or query, bundle.named_queries
        if inference.supplies:
            bundle = bundle.model_copy(update={"row_supplies": inference.supplies})
        # ``build_resolved_source_bundle`` raises if unresolved, so it's populated.
        dataset = bundle.population
        assert dataset is not None

        # ``override_datasource`` pins the connection identity (refresh re-exec):
        # re-run against the exact datasource the entry was cached under, so a
        # same-name repoint can't migrate the entry. Resolved here (before Mode-A
        # substitution) because escaping is dialect-aware; safe since substitution
        # never touches ``dataset.data_source``.
        datasource = override_datasource or await self._resolve_datasource(dataset=dataset)
        dialect = self._dialect_for_type(datasource.type)
        bundle = bundle.model_copy(update={"dialect": dialect, "datasource_type": datasource.type})

        # Substitute {var} into the direct source dataset's Mode-A surfaces before
        # anything parses them; the substituted copy replaces the dataset as both
        # ``population`` and its ``referenced_datasets`` entry. Rendered virtual
        # datasets are skipped. Called unconditionally: a no-op for a variable-free,
        # block-free dataset, but a block-bearing dataset must run so its ``{? ?}``
        # collapse to ``(1=1)`` even on a zero-variable call.
        if dataset.source_dataset_origin is None and not dataset.source_queries:
            substituted = _substitute_dataset_sql_surfaces(
                dataset=dataset,
                variables=bundle.query_variables,
                dialect=dialect_for_ds_type(datasource.type),
            )
            bundle = bundle.model_copy(
                update={
                    "population": substituted,
                    "referenced_datasets": [
                        substituted if m.name == dataset.name else m
                        for m in bundle.referenced_datasets
                    ],
                }
            )
            dataset = substituted

        # Slack-normalization: rewrite slack-but-unambiguous input (func-style
        # aggs, misplaced metrics) to canonical form, each stage against its
        # own resolved dataset.
        sibling_names = set(named_queries)
        query, _norm_warnings = self._normalize_stage(
            query=query, bundle=bundle, sibling_names=sibling_names,
        )
        warnings: List[AnySlayerWarning] = [*failures.warnings, *_norm_warnings]
        normed_named: Dict[str, SlayerQuery] = {}
        for nm, nq in named_queries.items():
            nq2, nq_warnings = self._normalize_stage(
                query=nq, bundle=bundle, sibling_names=sibling_names,
            )
            normed_named[nm] = nq2
            warnings.extend(nq_warnings)

        # Substitute variables into every Mode-B surface. Root uses the bundle's
        # merged variables; each sibling re-merges its own stage layer.
        query = bind_extension_metrics(
            query=apply_variables_to_query(query=query, variables=bundle.query_variables),
            variables=bundle.query_variables,
        )
        root_vars = query.variables
        # Each stage's row supply reads the variables its stage does.
        supplies = dict(bundle.row_supplies)
        root_key = query.name or ROOT_STAGE
        if root_key in supplies:
            supplies[root_key] = bound_supply(supplies[root_key], variables=bundle.query_variables)
        # Sibling-sourced stages fall back to the root dataset's defaults (a
        # query-backed root's own defaults layer only its spliced stages).
        fallback_vars = {} if dataset.source_queries else dataset.query_variables
        stage_sources = dict(bundle.stage_source_datasets)
        for nm, nq in list(normed_named.items()):
            source = stage_sources.get(nm)
            # Lowest layer: the stage's own source-dataset defaults.
            stage_vars = {
                **((source.query_variables if source is not None else fallback_vars) or {}),
                **(root_vars or {}),
                **(nq.variables or {}),
                **(runtime_kwarg or {}),
            }
            normed_named[nm] = bind_extension_metrics(
                query=apply_variables_to_query(query=nq, variables=stage_vars), variables=stage_vars,
            )
            if nm in supplies:
                supplies[nm] = bound_supply(supplies[nm], variables=stage_vars)
            if source is not None:
                stage_sources[nm] = _substitute_dataset_sql_surfaces(
                    dataset=source, variables=stage_vars,
                    dialect=dialect_for_ds_type(datasource.type),
                )
        bundle = await self._resolved_bundle(
            bundle.model_copy(update={"stage_source_datasets": stage_sources, "row_supplies": supplies}),
            datasource=datasource,
        )

        if result_order:
            query = _with_default_order(query)

        # Plan the DAG (root last) and compose the whole chain as one statement.
        planned_list = plan_stages(queries=[*normed_named.values(), query], bundle=bundle)
        render_dialect = await self._render_dialect(dialect=dialect, datasource=datasource, planned_list=planned_list)
        with collect_stale_spellings() as render_stale_spellings:
            built = build_planned_statement(
                planned_list, bundle=bundle, dialect=render_dialect,
                # Plan-derived canonical projection keys drive the write-side length
                # fit; the read side decodes against the same set.
                projection_aliases=projection_result_keys(root_planned=planned_list[-1]),
                type_probe=type_probe,
            )
        kept = built.kept_stages
        reads: Set[str] = {*bundle.splice_chain, *read_datasets(built.statement)}

        # Warnings come from the stages the SQL emits (a spliced stage nothing reads is pruned).
        plans = emitted_plans(planned_list, kept_stages=kept)
        reads |= {
            p.stage_schema.display.dataset for p in plans
            if p.stage_schema is not None and p.stage_schema.display is not None
            and p.stage_schema.display.dataset is not None
        }
        emitted_ids = {id(p) for p in plans}
        labels = [
            _plan_label(planned=p, index=i, root=query if p is planned_list[-1] else None)
            for i, p in enumerate(planned_list) if id(p) in emitted_ids
        ]
        refusing = [(p, label) for p, label in zip(plans, labels) if p.mode_policy.refuses]
        _raise_on_error_events(broadcasts=_collect_broadcast_warnings(
            planned_list=[p for p, _ in refusing], labels=[label for _, label in refusing],
        ))
        warnings.extend(_collect_broadcast_warnings(planned_list=plans, labels=labels))
        warnings.extend(_collect_associated_warnings(planned_list=plans, labels=labels))
        warnings.extend(_collect_unrelated_warnings(planned_list=plans, labels=labels))
        warnings.extend(_collect_semi_join_pushed_warnings(planned_list=plans, labels=labels))
        warnings.extend(_collect_degenerate_warnings(planned_list=plans, labels=labels))
        warnings.extend(_granularity_conflict_warnings(planned_list=plans, labels=labels))
        warnings.extend(WholeListWarning(operation=r.operation, list=r.list)
                        for r in dict.fromkeys(r for p in plans for r in p.whole_list_reads))
        warnings.extend({n.message: n for p in plans for n in p.date_only_notices}.values())
        warnings.extend(stale_spelling_warnings(list(dict.fromkeys([
            *(s for p in plans for s in p.stale_spellings), *render_stale_spellings,
        ]))))
        # Stored datasets the statement reads that dropped keys SLayer does not read.
        warnings.extend(
            dropped for name in sorted(reads)
            if (read := bundle.datasets_by_name.get(name)) is not None and (dropped := read.unknown_stored_keys)
        )
        # Stored query-backed datasets the statement reads: their stages' input deprecations.
        read_stages = [
            stage for name in sorted(reads) if name in bundle.query_backed
            for stage in bundle.query_backed[name].source_queries or []
        ]
        warnings.extend(deprecated_inputs_of(read_stages))
        warnings.extend(broadcast_deprecations(read_stages))
        return _Rendered(
            built=built, dialect=dialect, datasource=datasource, bundle=bundle,
            planned_list=planned_list, emitted_list=plans, dataset=dataset, reads=reads, associated=built.associated,
            warnings=warnings, population=population, population_inferred=population_inferred,
            row_sources=list(inference.row_sources),
        )

    async def _respell_spine(
        self, *, queries: List[SlayerQuery], stage_names: Set[str], data_source: Optional[str],
    ) -> "tuple[List[SlayerQuery], frozenset[str]]":
        """Each query reaching the spine in any case; a stage's name or a stored dataset's exact name keeps its spelling
        (the pinned datasource's, else any's). Also the spellings respelled."""
        candidates: Set[str] = set()

        def collect(spelling: str) -> bool:
            candidates.add(spelling)
            return True

        for q in queries:
            respell_spine(q, keep=collect)
        if not candidates:
            return queries, frozenset()
        stored = {n for ds, n in await self.storage._list_all_dataset_identities() if data_source in (None, ds)}
        kept = {s for s in candidates if s in stored or name_taken(s, stage_names)}
        out = [respell_spine(q, keep=kept.__contains__) for q in queries]
        return [q for q, _ in out], frozenset().union(*(found for _, found in out))

    async def _infer_populations(
        self,
        *,
        query: SlayerQuery,
        named_queries: Dict[str, SlayerQuery],
        prefer_data_source: Optional[str],
        stage_displays: "Dict[str, StageDisplay]",
    ) -> "_Inference":
        """Plan the rows of the main query and of each named stage without a population (semantics 12).

        Runs before prefix-strip. Sibling checks compare user spellings; identities are minted, so an inferred
        dataset name never resolves to a stage. The inferred datasource pins bundle resolution, so the chosen
        dataset name can't resolve to a same-named dataset in another datasource."""
        stage_names = {*named_queries, *(d.name for d in stage_displays.values())}
        if prefer_data_source is None and any(map(is_spine_query, [*named_queries.values(), query])):
            prefer_data_source = await self._spine_list_datasource(
                queries=[*named_queries.values(), query], stage_names=stage_names,
            )
        main = await self._infer_population_of(
            query=query, data_source=prefer_data_source,
            sibling_stage_names={d.name for d in stage_displays.values()} - {query.name},
        )
        inferred_data_source = main.data_source
        supplies: Dict[str, RowSupply] = {}
        pins: Dict[str, Optional[str]] = {}
        if main.supply is not None:
            supplies[query.name or ROOT_STAGE] = main.supply
            pins[query.name or ROOT_STAGE] = main.pin
        rewritten: Dict[str, SlayerQuery] = {}
        for name, stage in named_queries.items():
            inferred = await self._infer_population_of(
                query=stage, data_source=prefer_data_source or inferred_data_source,
                sibling_stage_names={d.name for ident, d in stage_displays.items() if ident != name},
            )
            rewritten[name] = inferred.query
            if inferred.supply is not None:
                supplies[name] = inferred.supply
                pins[name] = inferred.pin
            # Pin bundle resolution even when only a stage was rootless, so its bare
            # dataset name can't resolve to a same-named dataset in another datasource.
            inferred_data_source = inferred_data_source or inferred.data_source

        population = main.reported
        if population in stage_displays:
            population = stage_displays[population].name
        if is_spine_query(query) and population != TIME_SPINE_DATASET:
            population = f"{TIME_SPINE_DATASET} × {population}"
        row_sources = [stage_displays[s].name if s in stage_displays else s for s in main.row_sources]
        return _Inference(
            query=main.query, named_queries=rewritten, population=population,
            population_inferred=main.supply is not None and not main.anchored, data_source=inferred_data_source,
            row_sources=row_sources, supplies=supplies, pins=pins,
        )

    async def _infer_population_of(
        self, *, query: SlayerQuery, data_source: Optional[str], sibling_stage_names: Set[str],
    ) -> "_StageInference":
        """``query`` planned from its population, with its row supply when it has none (or a pinned anchor); rows are
        planned from inference's spelling of it (each dataset head as declared), the stage keeps it as written."""
        if query.population is not None and not query.population_inferred:
            return await self._named_population_of(query=query, data_source=data_source,
                                                   sibling_stage_names=sibling_stage_names)
        free = query.model_copy(update={"population": None, "population_inferred": False})
        if is_spine_query(query):
            return await self._spine_population_of(query=query, free=free, data_source=data_source,
                                                   sibling_stage_names=sibling_stage_names)
        rows = await infer_rows(query=free, storage=self.storage, data_source=data_source,
                                sibling_stage_names=sibling_stage_names,
                                raw_rows=not query.distinct_dimension_values)
        anchor = DimensionAnchor(datasets=(query.population_name,)) if query.population_name else rows.anchor
        plan = plan_rows(rows.query, anchor=anchor, datasets_by_name=rows.datasets_by_name)
        return _StageInference(
            query=query.model_copy(update={"population": plan.population, "population_inferred": True}),
            data_source=rows.data_source, reported=plan.reported, supply=plan.supply,
            row_sources=list(plan.row_sources), pin=anchor.datasets[0] if len(anchor.datasets) == 1 else None,
        )

    async def _named_population_of(
        self, *, query: SlayerQuery, data_source: Optional[str], sibling_stage_names: Set[str],
    ) -> "_StageInference":
        """``query`` under the population it names; rows are planned from its heads as declared."""
        spine_ds = None
        if query.population_name == TIME_SPINE_DATASET:
            spine_ds = await spine_datasource(
                queries=[query], storage=self.storage, data_source=data_source,
                sibling_stage_names=sibling_stage_names,
            )
        name = query.population_name
        routed = None
        if spine_ds is None:
            spelled = await declared_head_spellings(query, storage=self.storage, data_source=data_source,
                                                    siblings=sibling_stage_names)
            routed = await self._anchored_rows(spelled, data_source=data_source)
        if routed is not None:
            plan, routed_ds = routed
            return _StageInference(
                query=query.model_copy(update={"population": plan.population}), data_source=routed_ds,
                reported=name, supply=plan.supply, row_sources=list(plan.row_sources), anchored=True,
            )
        return _StageInference(query=query, data_source=spine_ds, reported=name,
                               row_sources=[name] if name is not None else [])

    async def _spine_population_of(
        self, *, query: SlayerQuery, free: SlayerQuery, data_source: Optional[str], sibling_stage_names: Set[str],
    ) -> "_StageInference":
        """A spine-reading ``query`` planned from its pinned or inferred population; an inferred pin of the spine itself
        anchors nothing, so its anchor is inferred as unpinned."""
        spine_pin = query.population_inferred and query.population_name == TIME_SPINE_DATASET
        choice = (
            PopulationChoice(datasets=(query.population_name or "",), data_source=data_source or "")
            if query.population is not None and not spine_pin else
            await infer_spine_population(query=free, storage=self.storage, data_source=data_source,
                                         sibling_stage_names=sibling_stage_names)
        )
        p = choice.dataset_name
        data_source_p = choice.data_source or None
        if p == TIME_SPINE_DATASET and data_source_p is None:
            data_source_p = await spine_datasource(queries=[query], storage=self.storage, data_source=data_source,
                                                   sibling_stage_names=sibling_stage_names)
        product = len(choice.datasets) > 1
        loaded, _unloaded = await self.storage.load_datasets(data_source=data_source_p) \
            if data_source_p is not None else ([], [])
        datasets_by_name = {m.name: m for m in loaded}
        if product:
            refuse_product_split(spine_factor_out(free).model_copy(update={"metrics": free.metrics}),
                                 anchor_datasets=choice.datasets, datasets_by_name=datasets_by_name)
        anchor = DimensionAnchor(datasets=() if p == TIME_SPINE_DATASET else tuple(choice.datasets))
        plan = plan_spine_rows(free, anchor=anchor, datasets_by_name=datasets_by_name)
        return _StageInference(
            query=query.model_copy(update={"population": plan.population, "population_inferred": True}),
            data_source=data_source_p, reported=choice.display, supply=plan.supply,
            row_sources=list(plan.row_sources), pin=None if product else p,
        )

    async def _anchored_rows(
        self, query: SlayerQuery, *, data_source: Optional[str],
    ) -> Optional[Tuple[RowPlan, str]]:
        """A named population's row plan and its datasource when it cannot route to a metric's home (else ``None``)."""
        population = query.population_name
        if population is None or not query.metrics or is_spine_query(query):
            return None
        if metric_anchor_names(query.model_copy(update={"population": None})) <= {population}:
            return None
        dataset = await self.storage.get_dataset(population, data_source=data_source)
        if dataset is None or not dataset.data_source:
            return None
        loaded, _unloaded = await self.storage.load_datasets(data_source=dataset.data_source)
        plan = anchored_rows(query, datasets_by_name={m.name: m for m in loaded})
        return (plan, dataset.data_source) if plan is not None else None

    async def _spine_list_datasource(self, *, queries: List[SlayerQuery], stage_names: Set[str]) -> Optional[str]:
        """The one datasource every dataset a spine-reading query list names lives in, if there is one."""
        try:
            return await spine_datasource(
                queries=queries, storage=self.storage, data_source=None, sibling_stage_names=stage_names,
            )
        except PopulationInferenceError:
            return None

    @staticmethod
    def _ds_fingerprint(datasource: DatasourceConfig) -> str:
        """Cache-key datasource fingerprint (``connection_string|runtime_fingerprint``)."""
        return "|".join(_sql_client_cache_key(datasource))

    def client_for(self, datasource: DatasourceConfig) -> SqlClient:
        """The one construction point: the cached client for ``datasource``, built by the factory on a miss."""
        key = client_cache_key(datasource)
        client = self._sql_clients.get(key)
        if client is None:
            factory = self._client_factory
            client = factory(datasource) if factory is not None else SlayerSQLClient(datasource=datasource)
            self._sql_clients[key] = client
        return client

    async def ingest(
        self, datasource: DatasourceConfig, **scope: Unpack[IngestScopeArgs],
    ) -> List[SlayerDataset]:
        """The datasets a scan of ``datasource`` finds, read through this engine's client."""
        return await ingestion.ingest_datasource_async(datasource, client=self.client_for(datasource), **scope)

    def ingest_sync(self, datasource: DatasourceConfig, **scope: Unpack[IngestScopeArgs]) -> List[SlayerDataset]:
        return self.run_sync(self.ingest(datasource, **scope))

    async def ingest_idempotent(
        self, datasource: DatasourceConfig, **scope: Unpack[IngestScopeArgs],
    ) -> "schema_drift.IdempotentIngestResult":
        """Idempotent ingestion of ``datasource`` into this engine's storage, through this engine's client."""
        return await ingestion.ingest_datasource_idempotent(
            datasource=datasource, storage=self.storage, client=self.client_for(datasource), **scope,
        )

    def ingest_idempotent_sync(
        self, datasource: DatasourceConfig, **scope: Unpack[IngestScopeArgs],
    ) -> "schema_drift.IdempotentIngestResult":
        return self.run_sync(self.ingest_idempotent(datasource, **scope))

    async def _execute_pipeline(
        self,
        query: SlayerQuery,
        named_queries: Dict[str, SlayerQuery],
        runtime_kwarg: Dict[str, Any],
        *,
        splice_chain: Tuple[str, ...] = (),
        dry_run: bool = False,
        explain: bool = False,
        prefer_data_source: Optional[str] = None,
        cache: bool = False,
        original_input: Any = None,
        original_data_source: Optional[str] = None,
    ) -> SlayerResponse:
        """Prepare then dry-run / explain / cache-hook / execute (a miss scans baselines first, then stores)."""
        prepared = await self._prepare_pipeline(
            query=query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
            prefer_data_source=prefer_data_source,
            splice_chain=splice_chain,
        )

        # dry_run: return SQL without executing. NEVER cached.
        if dry_run:
            return SlayerResponse(
                data=[], columns=prepared.expected_columns, sql=prepared.sql,
                attributes=prepared.attributes, warnings=prepared.slack_warnings,
                population=prepared.population,
                population_inferred=prepared.population_inferred,
                row_sources=prepared.row_sources,
            )

        use_cache = cache and not dry_run and not explain and not prepared.reads_clock
        # Bind the cache once so a concurrent ``cache_config`` reassignment can't
        # split the get / put across two caches.
        cache_obj = self._cache
        key: Optional[str] = None
        if use_cache:
            key = QueryCache.make_key(prepared.sql, self._ds_fingerprint(prepared.datasource))
            entry = await cache_obj.get(key)
            if entry is not None:
                # Deep copy so caller mutation can't poison the cached response.
                # Population metadata and warnings are per-query, not per-SQL (an
                # explicit and an inferred twin, or two spellings, share a cache key),
                # so report the current query's.
                return entry.response.model_copy(deep=True, update={
                    "population": prepared.population,
                    "population_inferred": prepared.population_inferred,
                    "row_sources": prepared.row_sources,
                    "warnings": list(prepared.slack_warnings),
                })

        # A dialect without EXPLAIN refuses before any database access.
        explain_statements = get_dialect(prepared.dialect).explain_statements(prepared.sql) if explain else None

        # Miss (or cache=False) → a SQL client is required.
        client = self.client_for(prepared.datasource)

        # explain: run dialect-appropriate EXPLAIN on the query. NEVER cached.
        if explain_statements is not None:
            try:
                explained = await client.explain(explain_statements)
            except Exception as exc:
                await self._maybe_raise_schema_drift(
                    err=exc, datasource=prepared.datasource, read_set=prepared.touched,
                    reads=prepared.reads,
                )
                raise
            return SlayerResponse(
                data=explained.rows, sql=prepared.sql, attributes=prepared.attributes,
                warnings=[*prepared.slack_warnings, *explained.warnings],
                population=prepared.population,
                population_inferred=prepared.population_inferred,
                row_sources=prepared.row_sources,
                datasource=prepared.datasource,
            )

        # Capture refresh-key baselines before the data query (cached data then
        # reflects a state >= the baseline); a scan failure propagates.
        applicable: list[tuple[str, str]] = []
        refresh_key_values: list[RefreshKeyValue] = []
        if use_cache:
            applicable, refresh_key_values = await self._scan_refresh_key_baselines(
                prepared=prepared, client=client, cache=cache_obj
            )

        result = await self._run_data_query(prepared=prepared, client=client)
        columns = prepared.expected_columns if not result.rows else []  # [] triggers auto-derive
        response = SlayerResponse(
            data=result.rows, columns=columns, sql=prepared.sql,
            attributes=prepared.attributes, warnings=[*prepared.slack_warnings, *result.warnings],
            population=prepared.population,
            population_inferred=prepared.population_inferred,
            row_sources=prepared.row_sources,
            datasource=prepared.datasource,
        )

        if use_cache:
            assert key is not None
            entry = self._build_cache_entry(
                prepared=prepared,
                response=response,
                original_input=original_input,
                variables=runtime_kwarg,
                data_source=original_data_source,
                created_at=cache_obj.now(),
                applicable=applicable,
                refresh_key_values=refresh_key_values,
            )
            await cache_obj.put(key, entry)
        return response

    @probes.probe(name="query.run", result=lambda executed: {"query.rows": len(executed.rows)})
    async def _run_data_query(
        self, *, prepared: _Prepared, client: SqlClient
    ) -> ExecutionResult:
        """Run the prepared data query (schema-drift attribution on error); decodes result keys."""
        try:
            result = await client.execute(sql=prepared.sql)
        except Exception as exc:
            await self._maybe_raise_schema_drift(
                err=exc, datasource=prepared.datasource, read_set=prepared.touched,
                reads=prepared.reads,
            )
            raise
        # Pass canonical aliases so length-fitted keys are restored.
        rows = get_dialect(prepared.dialect).decode_result_keys(
            result.rows, aliases=prepared.expected_columns,
        )
        return result.model_copy(update={"rows": rows})

    async def _scan_one_table_values(
        self,
        *,
        table: str,
        exprs: "list[str]",
        dialect: str,
        datasource: DatasourceConfig,
        client: SqlClient,
    ) -> "dict[str, Any]":
        """One batched refresh-key scan → ``{expression: value}``; policy-rewritten like the data query."""
        scan = self._cache.build_refresh_key_sql(table, exprs, dialect)
        scan = await self._apply_policy(statement=scan, dialect=dialect, datasource=datasource)
        rows = (await client.execute(sql=scan.sql(dialect=dialect))).rows
        row0 = rows[0] if rows else {}
        return {e: row0.get(self._cache.rk_alias(i)) for i, e in enumerate(exprs)}

    async def _scan_refresh_key_baselines(
        self, *, prepared: _Prepared, client: SqlClient, cache: QueryCache
    ) -> "tuple[list[tuple[str, str]], list[RefreshKeyValue]]":
        """Capture write-time refresh-key baselines ``(applicable, refresh_key_values)`` (same order; failures propagate)."""
        applicable = cache.applicable_keys(tables=prepared.tables, dialect=prepared.dialect)
        if not applicable:
            return [], []
        by_table = self._group_expressions_by_table(applicable)
        scanned: dict[str, dict[str, Any]] = {}
        for table, exprs in by_table.items():
            scanned[table] = await self._scan_one_table_values(
                table=table, exprs=exprs, dialect=prepared.dialect,
                datasource=prepared.datasource, client=client,
            )
        values = [
            RefreshKeyValue(table=t, expression=e, value=scanned[t][e])
            for (t, e) in applicable
        ]
        return applicable, values

    @staticmethod
    def _group_expressions_by_table(
        applicable: "list[tuple[str, str]]",
    ) -> "dict[str, list[str]]":
        """Collate ``(table, expression)`` pairs into ``{table: [exprs]}``, deduped, order-stable."""
        by_table: dict[str, list[str]] = {}
        for table, expr in applicable:
            exprs = by_table.setdefault(table, [])
            if expr not in exprs:
                exprs.append(expr)
        return by_table

    def _build_cache_entry(
        self,
        *,
        prepared: _Prepared,
        response: SlayerResponse,
        original_input: Any,
        variables: Optional[Dict[str, Any]],
        data_source: Optional[str],
        created_at: float,
        applicable: "list[tuple[str, str]]",
        refresh_key_values: "list[RefreshKeyValue]",
    ) -> _CacheEntry:
        """Build a ``_CacheEntry`` with deep copies of the response and original input."""
        ds_key = _sql_client_cache_key(prepared.datasource)
        return _CacheEntry(
            response=response.model_copy(deep=True),
            sql=prepared.sql,
            ds_fingerprint="|".join(ds_key),
            dialect=prepared.dialect,
            ds_key=ds_key,
            resolved_data_source=prepared.resolved_data_source,
            original_input=copy.deepcopy(original_input),
            # Deep copy so nested values can't be mutated into a refresh replay.
            variables=copy.deepcopy(dict(variables)) if variables else None,
            data_source=data_source,
            created_at=created_at,
            applicable=list(applicable),
            refresh_key_values=list(refresh_key_values),
        )

    async def evict(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        data_source: Optional[str] = None,
        refine: "QueryRefinement | dict | None" = None,
    ) -> bool:
        """Remove one cached entry, recomputing its key DB-free; ``True`` if present."""
        runtime_kwarg = variables or {}
        main_query, named_queries, prefer_ds, splice_chain = await self._normalize_input(
            resolve_run_input(query, refine), runtime_kwarg=runtime_kwarg, prefer_data_source=data_source
        )
        prepared = await self._prepare_pipeline(
            query=main_query,
            named_queries=named_queries,
            splice_chain=splice_chain,
            runtime_kwarg=runtime_kwarg,
            prefer_data_source=prefer_ds,
        )
        key = QueryCache.make_key(prepared.sql, self._ds_fingerprint(prepared.datasource))
        return await self._cache.delete(key)

    def evict_sync(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        data_source: Optional[str] = None,
        refine: "QueryRefinement | dict | None" = None,
    ) -> bool:
        """Synchronous wrapper for :meth:`evict`."""

        return self.run_sync(self.evict(query, variables=variables, data_source=data_source, refine=refine))

    async def _reexecute_entry(self, entry: _CacheEntry, now: float) -> Optional[_CacheEntry]:
        """Re-prepare + re-execute a stale entry, pinning its connection identity via
        ``override_datasource`` so no config change migrates it; raises if the client is gone.
        ``None`` when the query now reads the clock (never cached)."""
        client = self._sql_clients.get(_entry_client_key(entry))
        if client is None:
            raise RuntimeError(
                f"no cached SQL client for datasource fingerprint {entry.ds_key!r}; "
                "cannot pin re-execution to the entry's connection identity"
            )
        main_query, named_queries, prefer_ds, splice_chain = await self._normalize_input(
            entry.original_input,
            runtime_kwarg=entry.variables or {},
            prefer_data_source=entry.resolved_data_source,
        )
        prepared = await self._prepare_pipeline(
            query=main_query,
            named_queries=named_queries,
            splice_chain=splice_chain,
            runtime_kwarg=entry.variables or {},
            prefer_data_source=prefer_ds,
            override_datasource=client.datasource,
        )
        if prepared.reads_clock:
            return None
        applicable, refresh_key_values = await self._scan_refresh_key_baselines(
            prepared=prepared, client=client, cache=self._cache
        )
        result = await self._run_data_query(prepared=prepared, client=client)
        columns = prepared.expected_columns if not result.rows else []
        response = SlayerResponse(
            data=result.rows, columns=columns, sql=prepared.sql,
            attributes=prepared.attributes, warnings=[*prepared.slack_warnings, *result.warnings],
            population=prepared.population,
            population_inferred=prepared.population_inferred,
            row_sources=prepared.row_sources,
            datasource=prepared.datasource,
        )
        return self._build_cache_entry(
            prepared=prepared,
            response=response,
            original_input=entry.original_input,
            variables=entry.variables,
            data_source=entry.data_source,
            created_at=now,
            applicable=applicable,
            refresh_key_values=refresh_key_values,
        )

    async def refresh(self) -> RefreshResult:  # NOSONAR S3776 — Cube-style refresh: snapshot → collate scans → per-entry TTL/refresh-key decision
        """Cube-style explicit refresh over all cached entries.

        Per entry: TTL-expired ⇒ re-exec (``expired_refreshed``); applicable scan
        failed ⇒ ``unchanged``; a refresh-key value moved ⇒ re-exec (``refreshed``);
        else ``unchanged``. Errors become :class:`RefreshError`, keeping the entry.
        """
        result = RefreshResult()
        snapshot = await self._cache.snapshot()
        if not snapshot:
            return result

        # Collate {client key: {table: ordered exprs}} so each entry scans through its own client.
        collate: dict[ClientCacheKey, dict[str, list[str]]] = {}
        for entry in snapshot.values():
            if not entry.applicable:
                continue
            tables = collate.setdefault(_entry_client_key(entry), {})
            for table, expr in entry.applicable:
                exprs = tables.setdefault(table, [])
                if expr not in exprs:
                    exprs.append(expr)

        # One batched scan per (ds_key, table), continue-on-error per table,
        # through the write-time client (no name re-resolution).
        scanned: dict[tuple[ClientCacheKey, str], dict[str, Any]] = {}
        failed: set[tuple[ClientCacheKey, str]] = set()
        for ds_key, tables in collate.items():
            client = self._sql_clients.get(ds_key)
            for table, exprs in tables.items():
                if client is None:
                    # Write-time client gone (shouldn't happen). Fail-soft: keep
                    # the entry rather than scan a re-resolved, wrong database.
                    failed.add((ds_key, table))
                    result.errors.append(RefreshError(
                        key=table, phase="refresh_key_scan",
                        message=f"no cached SQL client for datasource fingerprint {ds_key!r}",
                    ))
                    continue
                try:
                    datasource = client.datasource
                    dialect = self._dialect_for_type(datasource.type)
                    scanned[(ds_key, table)] = await self._scan_one_table_values(
                        table=table, exprs=exprs, dialect=dialect,
                        datasource=datasource, client=client,
                    )
                except Exception as exc:
                    failed.add((ds_key, table))
                    result.errors.append(RefreshError(
                        key=table, phase="refresh_key_scan", message=str(exc),
                    ))

        for key, entry in snapshot.items():
            now = self._cache.now()
            ttl = self._cache.config.ttl_seconds
            ttl_expired = ttl is not None and (now - entry.created_at) > ttl
            if ttl_expired:
                bucket = result.expired_refreshed
            else:
                dk = _entry_client_key(entry)
                if any((dk, t) in failed for (t, _e) in entry.applicable):
                    result.unchanged.append(key)
                    continue
                moved = any(
                    QueryCache.values_differ(
                        scanned.get((dk, rkv.table), {}).get(rkv.expression),
                        rkv.value,
                    )
                    for rkv in entry.refresh_key_values
                )
                if not moved:
                    result.unchanged.append(key)
                    continue
                bucket = result.refreshed

            # Bucket only after a successful re-exec AND a landed commit: a
            # failure or a guard-skipped commit must not report as refreshed.
            try:
                new_entry = await self._reexecute_entry(entry, now)
            except Exception as exc:
                result.errors.append(RefreshError(
                    key=key, phase="re_execute", message=str(exc),
                ))
                continue
            if new_entry is None:
                await self._cache.delete(key)
                continue
            new_key = QueryCache.make_key(new_entry.sql, new_entry.ds_fingerprint)
            replaced = await self._cache.commit_replace(
                old_key=key, expected=entry, new_key=new_key, new_entry=new_entry,
            )
            if replaced:
                bucket.append(key)

        return result

    def refresh_sync(self) -> RefreshResult:
        """Synchronous wrapper for :meth:`refresh`."""

        return self.run_sync(self.refresh())

    def _normalize_stage(
        self,
        *,
        query: SlayerQuery,
        bundle: ResolvedSourceBundle,
        sibling_names: "set[str]",
    ) -> "tuple[SlayerQuery, list[NormalizationWarning]]":
        """Slack-normalize one stage against its resolved dataset (sibling-sourced → ``dataset=None``)."""
        sm = query.population
        dataset: Optional[SlayerDataset] = None
        if query.name and query.name in bundle.stage_source_datasets:
            # A named non-root stage normalizes against its own source dataset, not
            # the root; sibling-sourced stages fall through to dataset=None.
            dataset = bundle.stage_source_datasets[query.name]
        elif isinstance(sm, str):
            if sm not in sibling_names:
                dataset = bundle.get_referenced_dataset(sm)
                if dataset is None and (
                    bundle.population is not None
                    and bundle.population.name == sm
                ):
                    dataset = bundle.population
        else:
            dataset = bundle.population
        if dataset is not None and dataset.source_queries:
            dataset = None  # a query-backed source is read as a spliced stage
        norm = normalize_query(query, dataset=with_own_extension(dataset, query))
        out = norm.query if norm.query is not None else query
        return out, list(norm.warnings)

    async def _validate_read_set(
        self, *, datasource: DatasourceConfig, read_set: "Set[str]",
        findings_out: "Optional[List[schema_drift.RetypeFinding | schema_drift.UntypedFinding]]" = None,
    ) -> "List[schema_drift.ToDeleteEntry]":
        """Drift entries for the read persisted datasets of ``datasource``, over reused live facts."""
        identities = await self.storage._list_all_dataset_identities()
        available = {n for d, n in identities if d == datasource.name}
        datasets: List[SlayerDataset] = []
        for name in sorted(read_set & available):
            m = await self.storage.get_dataset(name, data_source=datasource.name)
            if m is not None:
                datasets.append(m)
        async with self._drift_snapshots.acquire(datasource) as snapshot:
            return await schema_drift.validate_datasource(
                datasource=datasource,
                datasets=datasets,
                client=self.client_for(datasource),
                available_in_ds=available,
                snapshot=snapshot,
                findings_out=findings_out,
            )

    async def _maybe_raise_schema_drift(
        self,
        *,
        err: BaseException,
        datasource: DatasourceConfig,
        read_set: "Set[str]",
        reads: StatementReads,
    ) -> None:
        """Raise ``SchemaDriftError`` if ``err`` is attributable to drift touching what the
        statement reads; else return so the caller re-raises. Attribution errors are swallowed."""
        try:
            found: List[schema_drift.RetypeFinding | schema_drift.UntypedFinding] = []
            entries = await self._validate_read_set(
                datasource=datasource, read_set=read_set, findings_out=found,
            )
            # An "invalid_sql" entry restates the query failure itself, so it is no drift evidence.
            filtered = [
                e for e in entries
                if schema_drift.touches(reads, e) and getattr(e, "cause", "schema_drift") != "invalid_sql"
            ]
            findings = [f for f in found if schema_drift.touches(reads, f)]
            if filtered or findings:
                raise SchemaDriftError(
                    datasets=sorted({e.dataset_name for e in filtered} | {f.dataset_name for f in findings}),
                    to_delete=filtered,
                    original=err,
                    findings=findings,
                )
        except SchemaDriftError:
            raise
        except Exception as inner:
            logger.debug(
                "schema-drift attribution swallowed an internal error: %s",
                inner,
            )

    def _build_type_probe_query(self, dataset: SlayerDataset, *, population: Optional[str] = None) -> SlayerQuery:
        """SlayerQuery type-probing a dataset's fields (prefers ``max``, skips identifiers), read from ``population``
        (default: the dataset)."""
        population = population or dataset.name
        metrics: List[QueryMetric] = []
        for c in dataset.fields:
            if c.hidden or is_identifier(field=c, fields=dataset.fields):
                continue
            if c.allowed_aggregations is not None:
                allowed = list(c.allowed_aggregations)
            else:
                allowed = sorted(DEFAULT_AGGREGATIONS_BY_TYPE.get(c.datatype, frozenset()) if c.datatype else ())
            if not allowed:
                continue
            agg = "max" if "max" in allowed else allowed[0]
            metrics.append(QueryMetric(metric=f"{agg}({qualified_reference(population, c.name)})"))
        return SlayerQuery(population=population, metrics=metrics)

    async def _plan_type_probe(
        self, *, stored: SlayerDataset, dataset: SlayerDataset, datasource: DatasourceConfig,
    ) -> "tuple[PlannedStatement, str, PlannedQuery]":
        """The type probe of ``dataset`` as a statement tree, its dialect and root plan. A query-backed
        ``stored`` dataset is probed by one more stage over its own stages, as run-by-name splices them."""
        probe_query = self._build_type_probe_query(dataset=dataset)
        if stored.source_queries:
            main_query, named_queries = self._stages_of_dataset(dataset=stored, runtime_kwarg={})
            root_name = main_query.name or stored.name
            rendered = await self._plan_and_render(
                query=self._build_type_probe_query(dataset=dataset, population=root_name),
                named_queries={**named_queries, root_name: main_query.model_copy(update={"name": root_name})},
                runtime_kwarg={}, prefer_data_source=None, type_probe=True,
            )
            return rendered.built, rendered.dialect, rendered.planned_list[-1]
        # Render a template dataset's Mode-A {var} surfaces from its own
        # defaults before probe SQL; an undefaulted {var} degrades to {}.
        dataset = _render_probe_dataset(dataset, dialect=dialect_for_ds_type(datasource.type))
        bundle = await slayer.engine.bundle_builder.build_resolved_source_bundle(
            query=probe_query,
            storage=self.storage,
            data_source=dataset.data_source or None,
            runtime_variables={},
            named_queries={},
            now=self._clock(),
            resolve_types=self._resolve_unknown_types,
        )
        dialect = self._dialect_for_type(datasource.type)
        bundle = await self._resolved_bundle(bundle.model_copy(update={
            "dialect": dialect, "datasource_type": datasource.type, "population": dataset,
            "referenced_datasets": [dataset if m.name == dataset.name else m for m in bundle.referenced_datasets],
        }), datasource=datasource)
        planned = plan_stages(queries=[probe_query], bundle=bundle)
        root = planned[-1]
        built = build_planned_statement(
            planned, bundle=bundle, dialect=dialect,
            projection_aliases=projection_result_keys(root_planned=root), type_probe=True,
        )
        return built, dialect, root

    async def get_result_column_types(  # NOSONAR(S3776) — linear probe pipeline: query-backed prelude → bundle → expand-nested → plan → render → execute → result-key map-back. Splitting hides the order; each step is its own try/except + early-return so flatness is the easier read.
        self,
        dataset_name: str,
        data_source: Optional[str] = None,
    ) -> Dict[str, ResultColumnType]:
        """The probed SLayer type and database type name of each non-identifier field of a dataset."""
        stored = await self.storage.get_dataset(dataset_name, data_source=data_source)
        if stored is None:
            return {}

        # Expand query-backed datasets first so the resolved virtual dataset (fresh
        # data_source + inner fields) drives datasource + probeable-fields, not
        # a stale stored value.
        dataset = stored
        if stored.source_queries:
            try:
                # No caller variables here; an undefaulted {var} degrades to {} below.
                dataset = (await self._query_backed_expansion(dataset=stored)).virtual_dataset()
            except QueryBackedCycleError:
                raise
            except Exception:
                logger.warning(
                    "get_result_column_types: failed to resolve query-backed dataset '%s'",
                    dataset_name,
                )
                return {}

        probeable = [
            c for c in dataset.fields
            if not c.hidden and not is_identifier(field=c, fields=dataset.fields)
        ]
        if not probeable:
            return {}

        try:
            datasource = await self._resolve_datasource(dataset=dataset)
        except ValueError:
            return {}

        client = self.client_for(datasource)

        try:
            built, dialect, root = await self._plan_type_probe(
                stored=stored, dataset=dataset, datasource=datasource,
            )
            # Type probing honours the forced-filter policy too; a policy failure
            # degrades to {} rather than leaking an unscoped probe.
            statement = await self._apply_policy(statement=built.statement, dialect=dialect, datasource=datasource)
            probe = built.model_copy(update={"statement": statement}).mint_probe(policy_scoped=self.policy is not None)
        except Exception:
            logger.warning(
                "get_result_column_types plan/generate failed for dataset '%s'",
                dataset_name,
            )
            return {}

        try:
            raw_types = await client.get_result_column_types(probe)
        except Exception:
            logger.warning(
                "get_result_column_types probe failed for dataset '%s'", dataset_name,
            )
            return {}

        # Decode emitted (alias-mangled / length-fitted) keys back to canonical
        # dotted form for the ``full`` lookups below.
        raw_types = get_dialect(dialect).decode_result_keys(
            [raw_types], aliases=projection_result_keys(root_planned=root),
        )[0]

        # Map qualified aliases back to bare metric names.
        result: Dict[str, ResultColumnType] = {}
        source_relation = root.source_relation
        for slot in root.aggregate_slots:
            if slot.hidden:
                continue
            bare = _bare_source_name(slot.key)
            if bare is None:
                continue
            full = slot.public_name or f"{source_relation}.{slot.declared_name}"
            if full in raw_types:
                result[bare] = raw_types[full]
        # A metric answered by a regroup producer surfaces as a placeholder ROW
        # slot; its source lives on the substitution's original key.
        slot_by_key = {s.key: s for s in root.row_slots}
        for attach in root.regroup_attach_plans:
            for sub in attach.substitutions:
                ph_slot = slot_by_key.get(sub.placeholder)
                if ph_slot is None or ph_slot.hidden:
                    continue
                bare = _bare_source_name(sub.original_key)
                if bare is None or bare in result:
                    continue
                full = ph_slot.public_name or f"{source_relation}.{ph_slot.declared_name}"
                if full in raw_types:
                    result[bare] = raw_types[full]
        return result

    def execute_sync(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        *,
        dry_run: bool = False,
        explain: bool = False,
        data_source: Optional[str] = None,
        cache: bool = False,
        refine: "QueryRefinement | dict | None" = None,
    ) -> SlayerResponse:
        """Synchronous wrapper for execute(), run through ``run_sync``."""
        return self.run_sync(self.execute(
            query, variables=variables, dry_run=dry_run,
            explain=explain, data_source=data_source, cache=cache, refine=refine,
        ))

    async def _scope_bare_name_to_datasource(
        self, *, raw: str, name: str, data_source: str
    ) -> "tuple[str, str, str]":
        """Resolve a dotless item within one datasource → ``(ds, dataset, leaf)``."""
        datasets = await _all_datasets_in_datasource(self.storage, data_source)
        if any(m.name == name for m in datasets):
            raise ValueError(
                f"'{raw}' resolves to dataset '{name}' in '{data_source}', which "
                f"is not a field or metric. recommend_population needs "
                f"'dataset.field' / 'dataset.metric' items."
            )
        # Ownership is field/metric only; a same-named custom aggregation drives
        # only the aggregation-specific error below, never ambiguity.
        owners = [
            m for m in datasets
            if m.get_field(name) is not None or m.get_metric(name) is not None
        ]
        if len(owners) == 1:
            return data_source, owners[0].name, name
        if len(owners) > 1:
            names = sorted(m.name for m in owners)
            raise ValueError(
                f"'{raw}' is ambiguous in datasource '{data_source}' — matches "
                f"{names}. Qualify it as '<dataset>.{name}'."
            )
        if any(m.get_aggregation(name) is not None for m in datasets):
            raise ValueError(
                f"'{raw}' names a custom aggregation in datasource "
                f"'{data_source}'. Aggregations are operators applied to "
                f"fields, not relationship-path targets — pass the field."
            )
        raise ValueError(
            f"'{raw}' does not name a field or metric in datasource "
            f"'{data_source}'."
        )

    async def _resolve_recommend_item(
        self, *, raw: str, data_source: str | None
    ) -> "tuple[_ResolvedItem, list[str]]":
        """Resolve + validate one recommend_population item; must be a qualified field, metric or row count."""
        await self._refuse_unqualified_item(raw, data_source=data_source)
        entity_ref, suffix = split_entity_agg_ref(raw)
        warnings: list[str] = []
        if entity_ref.endswith(".*"):
            return await self._resolve_recommend_row_count(raw=raw, entity_ref=entity_ref, suffix=suffix,
                                                           data_source=data_source), warnings
        if data_source and "." not in entity_ref:
            ds, dataset_name, leaf = await self._scope_bare_name_to_datasource(
                raw=raw, name=entity_ref, data_source=data_source,
            )
        else:
            (ds, dataset_name, leaf), warnings = await self._resolve_dotted_recommend_item(
                raw=raw, entity_ref=entity_ref, data_source=data_source)
        owning = await self.storage.get_dataset(dataset_name, data_source=ds)
        if owning is None:
            raise ValueError(
                f"'{raw}' resolves to dataset '{dataset_name}' in '{ds}', which "
                f"is not a saved dataset."
            )
        if owning.get_field(leaf) is None and owning.get_metric(leaf) is None:
            if owning.get_aggregation(leaf) is not None:
                raise ValueError(
                    f"'{raw}' names the custom aggregation '{leaf}' on "
                    f"'{dataset_name}'. Aggregations are operators applied to "
                    f"fields, not relationship-path targets — pass the field."
                )
            raise ValueError(
                f"'{raw}' does not name a field or metric on '{dataset_name}'."
            )
        # An aggregation suffix or a saved-metric leaf makes this an
        # attachment (reachability-only); a plain field is a determination item.
        attachment = suffix is not None or (
            owning.get_field(leaf) is None and owning.get_metric(leaf) is not None
        )
        item = _ResolvedItem(
            input_item=raw, data_source=ds, dataset=dataset_name,
            leaf=leaf, suffix=suffix, attachment=attachment,
        )
        return item, warnings

    async def _resolve_dotted_recommend_item(
        self, *, raw: str, entity_ref: str, data_source: str | None,
    ) -> "tuple[tuple[str, str, str], list[str]]":
        """``(datasource, dataset, leaf)`` of a dotted item, and the resolution's warnings."""
        # Force a dotted item into ``data_source`` unless it already leads
        # with it, so a dataset colliding with a datasource name still resolves
        # and a foreign-datasource ref fails as cross-datasource.
        resolution_input = entity_ref
        if data_source and entity_ref.split(".")[0] != data_source:
            resolution_input = f"{data_source}.{entity_ref}"
        res = await resolve_entity(resolution_input, storage=self.storage)
        segs = res.canonical_forms[0].split(".")
        if len(segs) < 3:
            raise ValueError(
                f"'{raw}' resolves to '{res.canonical_forms[0]}', which is "
                f"not a field or metric. recommend_population needs "
                f"'dataset.field' / 'dataset.metric' items."
            )
        return (segs[0], segs[1], segs[2]), res.warnings

    async def _refuse_unqualified_item(self, raw: str, *, data_source: str | None) -> None:
        """A bare name or ``count(*)`` item is refused as a query refuses it, suggesting the qualified spellings."""
        try:
            node = parse_expr(raw)
        except ValueError:
            return
        source = node.source if isinstance(node, AggCall) else node
        if isinstance(source, StarSource):
            raise unqualified_count()
        if isinstance(source, Ref):
            datasets: list[SlayerDataset] = []
            for ds in [d for d in await self.storage.list_datasources() if data_source in (None, d)]:
                datasets.extend(await _all_datasets_in_datasource(self.storage, ds))
            raise unknown_metric(source.name, datasets=datasets)

    async def _resolve_recommend_row_count(
        self, *, raw: str, entity_ref: str, suffix: str | None, data_source: str | None,
    ) -> "_ResolvedItem":
        """``count(<dataset>.*)``: an attachment on its dataset."""
        dataset_ref = entity_ref[:-2]
        if data_source and dataset_ref.split(".")[0] != data_source:
            dataset_ref = f"{data_source}.{dataset_ref}"
        res = await resolve_entity(dataset_ref, storage=self.storage)
        segs = res.canonical_forms[0].split(".")
        if len(segs) != 2:
            raise ValueError(f"'{raw}' is not a row count of one dataset; write count(<dataset>.*).")
        return _ResolvedItem(input_item=raw, data_source=segs[0], dataset=segs[1], leaf="*", suffix=suffix,
                             attachment=True)

    async def _recommend_resolve_items(
        self, *, items: list[str], data_source: str | None
    ) -> "tuple[list[_ResolvedItem], list[str]]":
        """Resolve every input item, then enforce single-datasource + dedup."""
        resolved: list[_ResolvedItem] = []
        warnings: list[str] = []
        seen_inputs: set[str] = set()
        for original in items:
            raw = original.strip()
            if not raw or raw in seen_inputs:
                continue
            seen_inputs.add(raw)
            item, item_warnings = await self._resolve_recommend_item(
                raw=raw, data_source=data_source,
            )
            resolved.append(item)
            warnings.extend(item_warnings)

        if not resolved:
            raise ValueError("recommend_population requires at least one item.")
        datasources = {r.data_source for r in resolved}
        if len(datasources) > 1:
            raise ValueError(
                f"items span multiple datasources {sorted(datasources)}; "
                f"cross-datasource queries are not supported. Pass items from "
                f"a single datasource (optionally set data_source=...)."
            )
        deduped_warnings: list[str] = []
        seen_w: set[str] = set()
        for w in warnings:
            if w not in seen_w:
                seen_w.add(w)
                deduped_warnings.append(w)
        return resolved, deduped_warnings

    async def recommend_population(  # NOSONAR(S3776) — cohesive: small named reach/validity closures over shared state read clearer inline than hoisted
        self,
        items: list[str],
        *,
        data_source: str | None = None,
        root_hint: str | None = None,
    ) -> PopulationRecommendation:
        """Recommend the query root for ``dataset.field`` / ``dataset.metric`` items, plus each item's path.

        Selection uses the population rule — the root must *determine*
        every field item along to-one paths, an unproven hop by association (fewest total hops); saved
        metrics and aggregation-suffixed items are attachments, needing only
        (cardinality-blind) reachability, and never steer the choice. No common
        determiner ⇒ ``reachable=False`` with a Pareto ``coverage`` over the
        determination items. A feasible ``root_hint`` overrides the auto-pick; an
        infeasible one falls back with a warning, a malformed one raises.
        """
        scope = await model_scope(self.storage, [], data_source=data_source)
        scoped = {raw: m for raw in dict.fromkeys(i.strip() for i in items)
                  if raw and (m := scope.bare(raw, aliases=())) is not None}
        resolved, base_warnings = await self._recommend_resolve_items(
            items=[i for i in items if i.strip() not in scoped], data_source=data_source
        )
        ds = resolved[0].data_source
        if any(m.data_source != ds for m in scoped.values()):
            raise ValueError(
                f"items span multiple datasources {sorted({ds, *(m.data_source for m in scoped.values())})}; "
                f"pass items from a single datasource (optionally set data_source=...)."
            )
        scoped_paths = [ItemPath(input_item=raw, path=m.name, attachment=True) for raw, m in scoped.items()]
        datasets = await _all_datasets_in_datasource(self.storage, ds)
        datasets_by_name = {m.name: m for m in datasets}
        all_names = sorted(datasets_by_name)
        warnings = list(base_warnings)

        det = [r for r in resolved if not r.attachment]
        attach = [r for r in resolved if r.attachment]
        mentioned = {r.dataset for r in resolved}
        # Fields: the population probe; attachments: the shortest route. ``lenient`` adds the
        # fan-out-free path of an item blocked only by ambiguity (used when nothing is viable, or by a hint).
        strict: dict[str, dict[str, tuple[str, ...]]] = {}
        lenient: dict[str, dict[str, tuple[str, ...]]] = {}
        # With determination items, an attachment no relationship path reaches meets the root as a product.
        unrelated: dict[str, set[str]] = {}
        for name in all_names:
            routed = shortest_routes(root=datasets_by_name[name], datasets_by_name=datasets_by_name)
            paths = {r.input_item: _attachment_route(root=datasets_by_name[name], item=r, routed=routed,
                                                     datasets_by_name=datasets_by_name)
                     for r in attach if r.dataset in routed}
            unrelated[name] = {
                r.input_item for r in attach
                if det and r.dataset not in routed and unreachable_target(
                    root=datasets_by_name[name], token=r.dataset, datasets_by_name=datasets_by_name,
                ) is not None
            }
            paths.update({item: (datasets_by_name[r.dataset].spelling,) for r in attach
                          if (item := r.input_item) in unrelated[name]})
            fallback: dict[str, tuple[str, ...]] = {}
            for r in det:
                item = f"{r.dataset}.{r.leaf}"
                path = determined_path(root=name, item=item, datasets_by_name=datasets_by_name)
                if path is not None:
                    paths[r.input_item] = path
                elif (alt := ambiguity_resolved_path(root=name, item=item, datasets_by_name=datasets_by_name)) is not None:
                    fallback[r.input_item] = alt
            strict[name] = paths
            lenient[name] = {**paths, **fallback}

        def valid_under(reach: dict[str, dict[str, tuple[str, ...]]], root: str) -> bool:
            return all(r.input_item in reach[root] for r in resolved)

        reach = strict if any(valid_under(strict, n) for n in all_names) else lenient

        def det_hops(root: str) -> int:
            return sum(len(reach[root][r.input_item]) for r in det)

        def is_valid(root: str) -> bool:
            return valid_under(reach, root)

        def unmet(root: str) -> list[str]:
            return sorted({r.dataset for r in resolved if r.input_item not in lenient[root]})

        hint = _resolve_root_hint(root_hint, data_source=ds, all_names=all_names)
        hint_dataset = hint[0] if hint is not None else None
        hint_display = hint[1] if hint is not None else None

        valid = [n for n in all_names if is_valid(n)]
        if det and valid:
            # The population rule: a tie at the fewest determination hops picks no root, as inference fails.
            fewest = min(det_hops(n) for n in valid)
            if len([n for n in valid if det_hops(n) == fewest]) > 1 and not (
                    hint_dataset is not None and valid_under(lenient, hint_dataset)):
                valid = []
        if valid:
            # Objective: fewest determination hops; attachments never steer, so
            # they are absent from the key (only a mentioned-preference + name
            # break exact determination-hop ties).
            auto = min(
                valid,
                key=lambda n: (det_hops(n), 0 if n in mentioned else 1, n),
            )
            if hint_dataset is not None and valid_under(lenient, hint_dataset):
                root, reach = hint_dataset, lenient
                message = f"All items are reachable from '{root}' (requested via root_hint)."
            else:
                root = auto
                if hint_dataset is not None:
                    missing = ", ".join(unmet(hint_dataset))
                    warnings.append(
                        f"root_hint '{hint_display}' cannot reach {{{missing}}}; "
                        f"fell back to '{auto}'."
                    )
                message = f"All items are reachable from '{root}'."
            item_paths = [
                ItemPath(
                    input_item=r.input_item,
                    path=_emit_recommend_path(list(reach[root][r.input_item]), r, root=datasets_by_name[root],
                                              datasets_by_name=datasets_by_name),
                    attachment=r.attachment,
                    unrelated=r.input_item in unrelated[root],
                )
                for r in resolved
            ]
            return PopulationRecommendation(
                data_source=ds, population=root, reachable=True,
                item_paths=[*item_paths, *scoped_paths], warnings=warnings, message=message,
            )

        force_include = {hint_dataset} if hint_dataset is not None else None
        # An attachment-only request ranks partial roots by attachment reachability.
        coverage = _build_recommend_coverage(all_names, det or attach, reach, force_include=force_include)
        for entry in coverage:
            root_dataset = datasets_by_name[entry.dataset_name]
            for r in resolved:
                if r.input_item in entry.unreachable_items or r.attachment and r.input_item not in reach[root_dataset.name]:
                    pairs = (blocking_pairs(root=root_dataset, target=r.dataset, datasets_by_name=datasets_by_name)
                             if r.attachment else
                             item_blockers(root=root_dataset.name, item=f"{r.dataset}.{r.leaf}",
                                           datasets_by_name=datasets_by_name))
                    warnings.extend(p.describe() for p in pairs if p.describe() not in warnings)
        if hint_dataset is not None:
            missing = ", ".join(unmet(hint_dataset))
            warnings.append(
                f"root_hint '{hint_display}' cannot reach {{{missing}}}; "
                f"no single dataset reaches every item — see coverage."
            )
        return PopulationRecommendation(
            data_source=ds, population=None, reachable=False, item_paths=[],
            coverage=coverage, warnings=warnings,
            message=(
                "No single dataset determines every requested item. See 'coverage' "
                "for the best partial roots — split the request into a "
                "multi-stage query rooted at those datasets."
            ),
        )

    def recommend_population_sync(
        self,
        items: list[str],
        *,
        data_source: str | None = None,
        root_hint: str | None = None,
    ) -> PopulationRecommendation:
        """Synchronous wrapper for :meth:`recommend_population`."""
        return self.run_sync(self.recommend_population(items, data_source=data_source, root_hint=root_hint))


    async def edit_dataset_remove(
        self,
        *,
        dataset_name: str,
        data_source: Optional[str],
        remove_fields: Optional[List[str]] = None,
        remove_metrics: Optional[List[str]] = None,
        remove_aggregations: Optional[List[str]] = None,
        remove_relationships: Optional[List[str]] = None,
        remove_filters: Optional[List[str]] = None,
        remove_relationship_edges: Optional[List[RelationshipEdgeRef]] = None,
    ) -> SlayerDataset:
        """Apply surgical removals (by name; relationships by edge reference or exact ref; filters verbatim) to a persisted dataset."""
        existing = await self.storage.get_dataset(dataset_name, data_source=data_source)
        if existing is None:
            raise ValueError(
                f"Dataset {dataset_name!r} not found in datasource {data_source!r}."
            )
        if existing.source_queries:
            # Query-backed datasets manage fields as cache; bypassing save_dataset
            # would persist a stale, mismatched cache.
            raise ValueError(
                f"edit_dataset_remove() does not support query-backed datasets "
                f"({dataset_name!r}); edit source_queries via engine.save_dataset() "
                f"instead."
            )
        cols_to_remove = set(remove_fields or [])
        metrics_to_remove = set(remove_metrics or [])
        aggs_to_remove = set(remove_aggregations or [])
        relationships_to_remove = resolve_relationship_refs(
            dataset=existing, refs=[*(remove_relationships or []), *(remove_relationship_edges or [])],
        )
        filters_to_remove = list(remove_filters or [])

        new_fields = [c for c in existing.fields if c.name not in cols_to_remove]
        new_metrics = [
            m for m in existing.metrics if m.name not in metrics_to_remove
        ]
        new_aggs = [a for a in existing.aggregations if a.name not in aggs_to_remove]
        new_relationships = without_relationships(dataset=existing, relationships=relationships_to_remove)
        new_filters = [f for f in existing.filters if f not in filters_to_remove]

        updated = existing.model_copy(
            update={
                "fields": new_fields,
                "metrics": new_metrics,
                "aggregations": new_aggs,
                "relationships": new_relationships,
                "filters": new_filters,
            }
        )
        SlayerDataset.model_validate(updated.model_dump())
        await self.storage.save_dataset(updated)
        # Cascade-strip dropped leaves from memory entity tags. Relationships/filters
        # don't cascade (a joined-leaf ref canonicalizes to the target's own id).
        existing_ds = existing.data_source
        for removed in (
            list(cols_to_remove)
            + list(metrics_to_remove)
            + list(aggs_to_remove)
        ):
            await self.storage.strip_dangling_entities_from_memories(
                canonical_id=f"{existing_ds}.{dataset_name}.{removed}",
            )
        return updated

    async def delete_dataset_by_name(
        self, *, dataset_name: str, data_source: Optional[str]
    ) -> bool:
        """Delete a persisted dataset by name. Returns True if the dataset existed."""
        return await self.storage.delete_dataset(dataset_name, data_source=data_source)

    async def apply_untyped_findings(self, findings: "Sequence[schema_drift.UntypedFinding]") -> List[str]:
        """Write each untyped finding's live type into its document field; returns one message per failed write."""
        errors: List[str] = []
        for finding in findings:
            try:
                dataset = await self.storage.get_dataset(finding.dataset_name, data_source=finding.data_source)
                if dataset is None:
                    raise LookupError(f"dataset {finding.dataset_name!r} is gone")
                await self.storage.save_dataset(dataset.model_copy(update={"fields": [
                    f.model_copy(update={"datatype": finding.live_type, "element_type": finding.live_element_type})
                    if f.name == finding.field else f for f in dataset.fields]}))
            except Exception as exc:  # noqa: BLE001 — one failed write never stops the others
                errors.append(f"{finding.dataset_name}.{finding.field}: {exc}")
        return errors

    async def apply_drift_deletes(
        self,
        deletes: "Sequence[schema_drift.DriftReportEntry]",
    ) -> "schema_drift.ApplyDriftResult":
        """Apply each ``ToDeleteEntry`` → ``ApplyDriftResult``; per-entry failures land in ``errors``, ``residual`` from a re-validate."""
        applied: List[schema_drift.AppliedEntry] = []
        errors: List[schema_drift.ApplyError] = []
        touched_ds: set[str] = set()

        for entry in deletes:
            if isinstance(entry, schema_drift.REPORT_ONLY_FINDINGS):
                continue  # report-only
            # Track the datasource up front so re-validation runs even if every
            # mutation on it fails.
            touched_ds.add(entry.data_source)
            try:
                if entry.tool == "delete_dataset":
                    await self.delete_dataset_by_name(
                        dataset_name=entry.dataset_name,
                        data_source=entry.data_source,
                    )
                elif entry.tool == "edit_dataset":
                    await self.edit_dataset_remove(
                        dataset_name=entry.dataset_name,
                        data_source=entry.data_source,
                        remove_fields=list(entry.remove.fields),
                        remove_metrics=list(entry.remove.metrics),
                        remove_aggregations=list(entry.remove.aggregations),
                        remove_relationships=list(entry.remove.relationships),
                        remove_filters=list(entry.remove_filters),
                        remove_relationship_edges=list(entry.remove.relationship_edges),
                    )
                else:
                    raise ValueError(f"Unknown delete tool: {entry.tool!r}")
                applied.append(
                    schema_drift.AppliedEntry(
                        tool=entry.tool,
                        dataset_name=entry.dataset_name,
                        data_source=entry.data_source,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — best-effort per-entry isolation
                errors.append(
                    schema_drift.ApplyError(
                        tool=entry.tool,
                        dataset_name=entry.dataset_name,
                        data_source=entry.data_source,
                        error=str(exc),
                    )
                )

        # Re-validate the touched datasources to compute residual drift.
        residual: List[Any] = []
        for ds_name in touched_ds:
            try:
                residual.extend(
                    e for e in await self.validate_datasets(data_source=ds_name)
                    if not isinstance(e, schema_drift.REPORT_ONLY_FINDINGS)
                )
            except Exception as inner:
                logger.debug(
                    "post-apply validate_datasets failed for ds=%r: %s",
                    ds_name,
                    inner,
                )
        return schema_drift.ApplyDriftResult(
            applied=applied,
            errors=errors,
            residual=list(residual),
        )

    async def detect_relationship_cardinality(
        self,
        *,
        data_source: str | None = None,
        dataset: str | None = None,
        persist: bool = False,
    ) -> RelationshipCardinalityReport:
        """Profile each relationship's two sides and classify its cardinality (report-only unless ``persist``).

        Only ``sql_table`` datasets with bare-field relationship keys are profiled; the rest
        report ``SKIPPED_UNSUPPORTED``.
        """
        ds_names = (
            [data_source] if data_source
            else await self.storage.list_datasources()
        )
        findings: list[RelationshipCardinalityFinding] = []
        persist_map: dict[tuple[str, str], list[tuple]] = {}

        for ds_name in ds_names:
            ds_findings, ds_persist = await self._detect_datasource_relationships(
                ds_name=ds_name, dataset=dataset,
            )
            findings.extend(ds_findings)
            for dataset_name, signature, detected in ds_persist:
                persist_map.setdefault((ds_name, dataset_name), []).append(
                    (signature, detected)
                )

        if persist:
            for (ds_name, dataset_name), items in persist_map.items():
                await self._persist_relationship_cardinality(
                    data_source=ds_name, dataset_name=dataset_name, items=items,
                )
        return RelationshipCardinalityReport(findings=findings)

    async def _resolve_detection_scope(self, *, ds_name, dataset):
        """In-scope datasets + a whole-datasource name lookup (relationship targets must still resolve)."""
        all_datasets = await _all_datasets_in_datasource(self.storage, ds_name)
        by_name = {m.name: m for m in all_datasets}
        if dataset is None:
            return all_datasets, by_name
        return ([by_name[dataset]] if dataset in by_name else []), by_name

    async def _detect_datasource_relationships(
        self, *, ds_name, dataset,
    ) -> "tuple[list[RelationshipCardinalityFinding], list[tuple]]":
        """Profile every relationship of every in-scope dataset → ``(findings, persist_entries)``."""
        scope, by_name = await self._resolve_detection_scope(
            ds_name=ds_name, dataset=dataset,
        )
        if not scope:
            return [], []
        ds_cfg = await self.storage.get_datasource(ds_name)
        if ds_cfg is None:
            return [], []

        sqlglot_name = dialect_for_ds_type(ds_cfg.type).sqlglot_name
        findings: list[RelationshipCardinalityFinding] = []
        persist_entries: list[tuple] = []
        client = self.client_for(ds_cfg)
        for m in scope:
            for relationship in m.relationships:
                try:
                    finding, detected = await self._detect_one_relationship(
                        dataset=m, relationship=relationship, by_name=by_name,
                        client=client, sqlglot_name=sqlglot_name,
                        data_source=ds_name, datasource_cfg=ds_cfg,
                    )
                except ForcedFilterError:
                    # Fail-closed: a report line would leak unscoped stats.
                    raise
                except Exception as exc:  # noqa: BLE001
                    # Contain per relationship: one unreadable table mustn't fail all.
                    findings.append(
                        _scan_failed_finding(
                            data_source=ds_name, dataset=m, relationship=relationship, exc=exc,
                        )
                    )
                    continue
                findings.append(finding)
                if detected is not None:
                    persist_entries.append(
                        (m.name, _relationship_signature(relationship), detected)
                    )
        return findings, persist_entries

    async def _detect_one_relationship(
        self, *, dataset, relationship: DatasetRelationship, by_name, client, sqlglot_name, data_source,
        datasource_cfg,
    ) -> "tuple[RelationshipCardinalityFinding, RelationshipCardinality | None]":
        pairs = list(relationship.column_pairs)
        src_cols = [p[0] for p in relationship.column_pairs]
        tgt_cols = [p[1] for p in relationship.column_pairs]
        target = by_name.get(relationship.to)

        note = _detection_skip_reason(
            dataset=dataset, target=target, src_cols=src_cols, tgt_cols=tgt_cols,
        )
        if note is not None:
            return RelationshipCardinalityFinding(
                data_source=data_source, dataset=dataset.name,
                to=relationship.to, column_pairs=pairs,
                stored=relationship.cardinality, detected=None,
                verdict=CardinalityVerdict.SKIPPED_UNSUPPORTED, note=note,
            ), None

        src_side = await self._side_stats(
            client=client, table=dataset.sql_table,
            key_cols=_key_reads(dataset=dataset, keys=src_cols, dialect=sqlglot_name), sqlglot_name=sqlglot_name,
            datasource=datasource_cfg,
        )
        tgt_side = await self._side_stats(
            client=client, table=target.sql_table,
            key_cols=_key_reads(dataset=target, keys=tgt_cols, dialect=sqlglot_name), sqlglot_name=sqlglot_name,
            datasource=datasource_cfg,
        )
        # 0 == 0 reads as observed_unique, so an empty side would falsely detect
        # one_to_one; no rows is no evidence.
        empty_sides = [
            name
            for name, side in (("source", src_side), ("target", tgt_side))
            if side.row_count == 0
        ]
        if empty_sides:
            return RelationshipCardinalityFinding(
                data_source=data_source, dataset=dataset.name,
                to=relationship.to, column_pairs=pairs,
                stored=relationship.cardinality, detected=None,
                source_side=src_side, target_side=tgt_side,
                verdict=CardinalityVerdict.NO_EVIDENCE,
                note=(
                    f"no non-null key rows on the {' and '.join(empty_sides)} "
                    f"side; an empty scan is no evidence of arity"
                ),
            ), None

        detected = classify_cardinality(
            source_unique=src_side.observed_unique,
            target_unique=tgt_side.observed_unique,
        )
        verdict = compute_verdict(
            stored=relationship.cardinality, detected=detected,
            source_observed_unique=src_side.observed_unique,
            target_observed_unique=tgt_side.observed_unique,
        )
        contradictions = _unique_contradictions(
            dataset=dataset, target=target, src_cols=src_cols, tgt_cols=tgt_cols,
            src_side=src_side, tgt_side=tgt_side,
        )
        return RelationshipCardinalityFinding(
            data_source=data_source, dataset=dataset.name,
            to=relationship.to, column_pairs=pairs,
            stored=relationship.cardinality, detected=detected,
            source_side=src_side, target_side=tgt_side,
            verdict=verdict, unique_contradictions=contradictions,
        ), detected

    @staticmethod
    def _side_stats_sql(*, table, key_cols, sqlglot_name) -> tuple[exp.Select, exp.Select]:
        """(row-count, distinct-count) profiling statements reading the table and keys as SLayer reads them; NULL key
        rows excluded from both."""
        tbl = parse_table(name=table, dialect=get_dialect(sqlglot_name))
        cols = [exp.column(c) if isinstance(c, exp.Identifier) else c for c in key_cols]
        predicate = None
        for col in cols:
            term = exp.Not(this=exp.Is(this=col.copy(), expression=exp.null()))
            predicate = term if predicate is None else exp.and_(predicate, term)
        # A relationship's key lists are validated non-empty.
        assert predicate is not None

        count_star = exp.func("COUNT", exp.Star()).as_("c")
        rows_q = exp.select(count_star).from_(tbl.copy()).where(predicate)
        inner = (
            exp.select(*[c.copy() if isinstance(c, exp.Column) else exp.alias_(c.copy(), f"k{i}")
                         for i, c in enumerate(cols)])
            .from_(tbl.copy())
            .where(predicate.copy())
            .distinct()
        )
        dist_q = exp.select(count_star.copy()).from_(inner.subquery("d"))
        return (
            rows_q,
            dist_q,
        )

    async def _side_stats(
        self, *, client, table, key_cols, sqlglot_name, datasource,
    ) -> SideStats:
        """Full-scan one side of a relationship: non-null key rows vs distinct key-tuples."""
        rows_q, dist_q = self._side_stats_sql(
            table=table, key_cols=key_cols, sqlglot_name=sqlglot_name,
        )
        # Profile the tenant-scoped rows, like every execution path (no-op without a policy).
        rows_q = await self._apply_policy(statement=rows_q, dialect=sqlglot_name, datasource=datasource)
        dist_q = await self._apply_policy(statement=dist_q, dialect=sqlglot_name, datasource=datasource)
        rows_sql, dist_sql = rows_q.sql(dialect=sqlglot_name), dist_q.sql(dialect=sqlglot_name)
        row_rows = (await client.execute(sql=rows_sql)).rows
        dist_rows = (await client.execute(sql=dist_sql)).rows
        row_count = int(next(iter(row_rows[0].values())))
        distinct_count = int(next(iter(dist_rows[0].values())))
        return SideStats(
            row_count=row_count,
            distinct_count=distinct_count,
            observed_unique=(row_count == distinct_count),
        )

    async def _persist_relationship_cardinality(
        self, *, data_source, dataset_name, items,
    ) -> None:
        m = await self.storage.get_dataset(dataset_name, data_source=data_source)
        if m is None:
            return
        sig_to_detected = dict(items)
        changed = False
        new_relationships = []
        for j in m.relationships:
            d = sig_to_detected.get(_relationship_signature(j))
            if d is not None and j.cardinality != d:
                new_relationships.append(j.model_copy(update={"cardinality": d}))
                changed = True
            else:
                new_relationships.append(j)
        if changed:
            await self.storage.save_dataset(m.model_copy(update={"relationships": new_relationships}))

    async def validate_datasets(
        self, data_source: Optional[str] = None
    ) -> "List[schema_drift.DriftReportEntry]":
        """Diff persisted datasets against live schemas → minimal deletes (read-only; ``None`` = all datasources)."""
        ds_names = [data_source] if data_source is not None else await self.storage.list_datasources()
        if not ds_names:
            return []
        everything, _ = await self.storage.load_datasets()  # a reader's narrowing datasets may live elsewhere
        narrowed = narrowing_datasets(everything)
        if data_source is not None:
            return await self._validate_datasource(data_source, narrowed=narrowed)

        results = await asyncio.gather(
            *(self._validate_datasource(n, narrowed=narrowed) for n in ds_names), return_exceptions=True
        )
        out: List[schema_drift.DriftReportEntry] = []
        for r in results:
            if isinstance(r, BaseException):
                logger.warning("validate_datasets: per-DS validation failed: %s", r)
                continue
            out.extend(r)
        return out

    async def _validate_datasource(
        self, data_source: str, *, narrowed: Dict[Tuple[str, str], List[Tuple[str, str]]],
    ) -> "List[schema_drift.DriftReportEntry]":
        """One datasource's drift deletes, unloadable datasets and narrowed-access findings."""
        ds = await self.storage.get_datasource(data_source)
        if ds is None:
            return []
        datasets, unloadable = await self.storage.load_datasets(data_source=data_source)
        retypes: List[schema_drift.RetypeFinding | schema_drift.UntypedFinding] = []
        entries = await schema_drift.validate_datasource(
            datasource=ds, datasets=datasets, client=self.client_for(ds), findings_out=retypes,
        )
        findings = [
            schema_drift.NarrowedAccessFinding(dataset_name=name, data_source=ds_name, narrowed_by=[f"{d}.{n}" for d, n in by])
            for (ds_name, name), by in sorted(narrowed.items())
            if ds_name == data_source
        ]
        return [*entries, *(schema_drift.unloadable_dataset_delete(error=e) for e in unloadable), *retypes, *findings]

    def create_dataset_from_query_sync(
        self,
        query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]",
        name: str,
        description: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        save: bool = True,
        access_tags: Optional[List[str]] = None,
        warnings_out: Optional[List[SlayerWarning]] = None,
    ) -> SlayerDataset:
        """Synchronous wrapper for create_dataset_from_query()."""

        return self.run_sync(
            self.create_dataset_from_query(
                query=query,
                name=name,
                description=description,
                variables=variables,
                save=save,
                access_tags=access_tags,
                warnings_out=warnings_out,
            )
        )

    async def _query_backed_expansion(
        self,
        *,
        dataset: SlayerDataset,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        prefer_data_source: Optional[str] = None,
    ) -> "_QueryBackedExpansion":
        """Plan a query-backed ``dataset``'s stages as a run-by-name statement tree, read as a
        virtual dataset over its root stage. An undefaulted variable raises, exactly as execution does."""
        main_query, named_queries = self._stages_of_dataset(
            dataset=dataset, runtime_kwarg=runtime_kwarg or {},
        )
        # No hint by default, so inner resolution uses the priority list — letting
        # ``get_result_column_types`` recover from a stale persisted ``data_source``.
        rendered = await self._plan_and_render(
            query=main_query, named_queries=named_queries, runtime_kwarg=runtime_kwarg or {},
            prefer_data_source=prefer_data_source, splice_chain=(dataset.name,),
        )
        schema = rendered.planned_list[-1].stage_schema
        assert schema is not None
        if schema.flatten_collision is not None:
            flat, first, second = schema.flatten_collision
            check_stage_flatten_collision(flat_name=flat, collides=True, entries=(first, second))
        # ``DatasetField.sql`` carries the length-fitted alias; ``DatasetField.name`` stays canonical.
        fit_map = get_dialect(rendered.dialect).alias_rewrite_map([c.name for c in schema.fields])
        return _QueryBackedExpansion(name=dataset.name, rendered=rendered, stage_schema=schema, fit_map=fit_map)

    async def _expand_query_backed_dataset(
        self,
        *,
        dataset: SlayerDataset,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        prefer_data_source: Optional[str] = None,
        notices_out: Optional[List[DateOnlyMidnightWarning]] = None,
    ) -> SlayerDataset:
        """Render a query-backed ``dataset`` as a virtual ``sql``-mode dataset (save-time cache): its
        stages, wrapped in a flat-rename SELECT, rendered once through the statement validators."""
        expansion = await self._query_backed_expansion(
            dataset=dataset, runtime_kwarg=runtime_kwarg, prefer_data_source=prefer_data_source,
        )
        rendered = expansion.rendered
        if notices_out is not None:
            notices_out.extend(w for w in rendered.warnings if isinstance(w, DateOnlyMidnightWarning))
        root_planned = rendered.planned_list[-1]
        dialect, bundle = rendered.dialect, rendered.bundle
        # Wrap the stage AST with a flat-renamed SELECT over the root stage's output columns, then render once.
        wrapped_ast = build_flat_rename_wrapper(
            stage=dataset.name,
            source_relation=root_planned.stage_field_relation,
            inner=rendered.statement,
            expected_columns=[c.name for c in expansion.stage_schema.fields],
            dialect=dialect,
        )
        # A flat name equals the user-authored field name, which the finishing pass exempts, so the output
        # aliases are fitted here.
        for alias in wrapped_ast.expressions:
            fitted = expansion.fit_map.get(alias.alias)
            if fitted is not None:
                alias.set("alias", exp.to_identifier(fitted, quoted=True))
        wrapped_sql = _finish_statement(
            wrapped_ast,
            dialect=dialect,
            aliases=projection_result_keys(root_planned=root_planned),
            exempt=_user_authored_exemptions(bundle=bundle, dialect=dialect),
        )
        return expansion.virtual_dataset(sql=wrapped_sql)

    async def expand_query_backed_dataset(self, dataset: SlayerDataset) -> SlayerDataset:
        """The sql-mode dataset a query-backed ``dataset`` renders as, under its default variables, in its datasource."""
        return await self._expand_query_backed_dataset(dataset=dataset, prefer_data_source=dataset.data_source or None)

    async def relationships_as_read(self, dataset: SlayerDataset) -> Tuple[List[DatasetRelationship], Optional[str]]:
        """``dataset``'s relationships as a query reads them — a query-backed one's inherited edges merged in under its
        default variables — and why inherited edges could not be derived (``None`` when they were)."""
        if not dataset.source_queries:
            return list(dataset.relationships), None
        try:
            expansion = await self._query_backed_expansion(dataset=dataset, prefer_data_source=dataset.data_source or None)
            view = dataset_from_stage_schema(
                name=dataset.name, schema=expansion.stage_schema, data_source=dataset.data_source or "_stage",
                saved=dataset,
            )
        except ValueError as exc:  # SlayerError included
            return list(dataset.relationships), str(exc)
        # An identical edge a peer declares back absorbs the inherited one, as in execution.
        if not dataset.data_source:
            return list(view.relationships), None
        peers = await self._preload_relationship_targets(view, failures=DocumentLoadFailures())
        loaded = {n: d for n, d in peers.items() if d is not None}
        return list(absorb_inherited(loaded)[view.name].relationships), None

    async def create_dataset_from_query(
        self,
        query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]",
        name: str,
        description: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        save: bool = True,
        access_tags: Optional[List[str]] = None,
        warnings_out: Optional[List[SlayerWarning]] = None,
    ) -> SlayerDataset:
        """Create a query-backed dataset; cache fields come from a save-time dry-run, ``save=False`` skips persisting."""
        raw = query if isinstance(query, list) else [query]
        with one_document():
            stages = [SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in raw]
        if not save:
            stages = await qualified_input(stages, storage=self.storage, data_source=None, deprecations_out=[])
        dataset = SlayerDataset(
            name=name,
            description=description,
            source_queries=stages,
            query_variables=variables or {},
            access_tags=access_tags or [],
        )
        if save:
            return await self.save_dataset(dataset, warnings_out=warnings_out)
        return await self._validate_and_populate_cache(dataset)

    @method_twin("save_dataset", arguments={"model": "dataset"})
    @deprecated("save_model is deprecated; use save_dataset", category=None)
    async def save_model(
        self, dataset: SlayerDataset, *, warnings_out: Optional[List[SlayerWarning]] = None,
    ) -> SlayerDataset: ...

    @method_twin("create_dataset_from_query")
    @deprecated("create_model_from_query is deprecated; use create_dataset_from_query", category=None)
    async def create_model_from_query(
        self, query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]", name: str,
        description: Optional[str] = None, variables: Optional[Dict[str, Any]] = None, save: bool = True,
        access_tags: Optional[List[str]] = None, warnings_out: Optional[List[SlayerWarning]] = None,
    ) -> SlayerDataset: ...

    @method_twin("create_dataset_from_query_sync")
    @deprecated("create_model_from_query_sync is deprecated; use create_dataset_from_query_sync", category=None)
    def create_model_from_query_sync(
        self, query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]", name: str,
        description: Optional[str] = None, variables: Optional[Dict[str, Any]] = None, save: bool = True,
        access_tags: Optional[List[str]] = None, warnings_out: Optional[List[SlayerWarning]] = None,
    ) -> SlayerDataset: ...

    @method_twin("delete_dataset_by_name", arguments={"model_name": "dataset_name"})
    @deprecated("delete_model_by_name is deprecated; use delete_dataset_by_name", category=None)
    async def delete_model_by_name(self, *, dataset_name: str, data_source: Optional[str]) -> bool: ...

    @method_twin("detect_relationship_cardinality", arguments={"model": "dataset"})
    @deprecated("detect_join_cardinality is deprecated; use detect_relationship_cardinality", category=None)
    async def detect_join_cardinality(
        self, *, data_source: str | None = None, dataset: str | None = None, persist: bool = False,
    ) -> RelationshipCardinalityReport: ...

    @method_twin("edit_dataset_remove", arguments={
        "model_name": "dataset_name", "remove_columns": "remove_fields", "remove_measures": "remove_metrics",
        "remove_joins": "remove_relationships", "remove_join_edges": "remove_relationship_edges",
    })
    @deprecated("edit_model_remove is deprecated; use edit_dataset_remove", category=None)
    async def edit_model_remove(
        self, *, dataset_name: str, data_source: Optional[str], remove_fields: Optional[List[str]] = None,
        remove_metrics: Optional[List[str]] = None, remove_aggregations: Optional[List[str]] = None,
        remove_relationships: Optional[List[str]] = None, remove_filters: Optional[List[str]] = None,
        remove_relationship_edges: Optional[List[RelationshipEdgeRef]] = None,
    ) -> SlayerDataset: ...

    @method_twin("expand_query_backed_dataset", arguments={"model": "dataset"})
    @deprecated("expand_query_backed_model is deprecated; use expand_query_backed_dataset", category=None)
    async def expand_query_backed_model(self, dataset: SlayerDataset) -> SlayerDataset: ...

    @method_twin("recommend_population")
    @deprecated("recommend_root_model is deprecated; use recommend_population", category=None)
    async def recommend_root_model(
        self, items: list[str], *, data_source: str | None = None, root_hint: str | None = None,
    ) -> PopulationRecommendation: ...

    @method_twin("recommend_population_sync")
    @deprecated("recommend_root_model_sync is deprecated; use recommend_population_sync", category=None)
    def recommend_root_model_sync(
        self, items: list[str], *, data_source: str | None = None, root_hint: str | None = None,
    ) -> PopulationRecommendation: ...

    @method_twin("validate_datasets")
    @deprecated("validate_models is deprecated; use validate_datasets", category=None)
    async def validate_models(self, data_source: Optional[str] = None) -> "List[schema_drift.DriftReportEntry]": ...

    @method_twin("validate_sql_dataset_source", arguments={"model": "dataset"})
    @deprecated("validate_sql_model_source is deprecated; use validate_sql_dataset_source", category=None)
    async def validate_sql_model_source(self, dataset: SlayerDataset) -> None: ...

    async def save_dataset(
        self, dataset: SlayerDataset, *, warnings_out: Optional[List[SlayerWarning]] = None,
    ) -> SlayerDataset:
        """Persist a SlayerDataset, each deprecated spelling stored as its twin; query-backed datasets reject cache fields and validate via dry-run.
        The source queries' input deprecations, the spelling deprecations and narrowed-access warnings go to
        ``warnings_out`` when given; without it, narrowed-access warnings are emitted as Python warnings."""
        deprecated_inputs = [
            *deprecated_inputs_of([dataset, *(dataset.source_queries or [])]),
            *broadcast_deprecations(dataset.source_queries or []),
        ]
        dataset, spellings = with_canonical_spellings(dataset)
        if dataset.source_queries:
            stages = await qualified_input(
                list(dataset.source_queries), storage=self.storage, data_source=dataset.data_source, deprecations_out=deprecated_inputs,
            )
            dataset = dataset.model_copy(update={"source_queries": stages})
        if warnings_out is not None:
            warnings_out.extend([*deprecated_inputs, *spellings])
        with announced_by_caller():
            return await self._save_dataset(dataset, warnings_out=warnings_out)

    async def _save_dataset(
        self, dataset: SlayerDataset, *, warnings_out: Optional[List[SlayerWarning]],
    ) -> SlayerDataset:
        notices: List[DateOnlyMidnightWarning] = []
        # Save preserves the author's expression spelling — no slack
        # rewriting; both aggregation spellings are first-class parser input.
        prior_data_source = await self._prior_data_source(dataset)
        # Fail fast before any trial run / expansion; storage re-checks in the saved dialect.
        _reject_expressions_unparseable_everywhere(dataset)
        if dataset.source_queries:
            if dataset.fields:
                raise ValueError(
                    f"Dataset '{dataset.name}' is query-backed; fields are "
                    f"auto-generated and must not be supplied "
                    f"(got {len(dataset.fields)} fields)."
                )
            dataset = await self._validate_and_populate_cache(dataset, notices_out=notices)
        failures = DocumentLoadFailures()
        loaded = await self._preload_relationship_targets(dataset, failures=failures)
        ds = await self.storage.get_datasource(dataset.data_source) if dataset.data_source else None
        resolved = dataset
        if ds is not None and not dataset.source_queries:
            dataset = with_converted_sql(dataset, ds_type=ds.type)
            resolved = await self._resolved(dataset, datasource=ds,
                                            datasets=[d for d in loaded.values() if d is not None])
        notices.extend(saved_date_only_notices(dataset, datasets_by_name=loaded))
        _validate_relationship_keys(dataset=resolved, loaded=loaded)
        check_placeholders(mode_a_texts(dataset), dialect=dialect_for_ds_type(ds.type if ds else None))
        await self._validate_mode_a_relationship_paths(resolved, loaded=loaded)
        await self.validate_sql_dataset_source(dataset)
        try:
            prior = await self.storage.get_dataset(dataset.name, data_source=prior_data_source or dataset.data_source)
        except StoredDocumentLoadError:
            prior = None
        await self.storage.save_dataset(dataset, failures=failures)
        # Clean up the stale entry if the dataset moved datasource.
        if (
            prior_data_source is not None
            and prior_data_source != dataset.data_source
        ):
            await self.storage.delete_dataset(
                dataset.name, data_source=prior_data_source
            )
        payloads = await self._narrowed_access(dataset=dataset, prior=prior)
        unique = list({n.message: n for n in notices}.values())
        if warnings_out is not None:
            warnings_out.extend([*unique, *payloads])
            return dataset
        # Last, as in query(): under ``-W error`` this raises after the save.
        for notice in unique:
            _warnings_module.warn(SlayerDateOnlyMidnightWarning(notice), stacklevel=2)
        for payload in payloads:
            _warnings_module.warn(SlayerNarrowedAccessWarning(payload), stacklevel=2)
        return dataset

    async def _prior_data_source(self, dataset: SlayerDataset) -> Optional[str]:
        """A query-backed dataset's stored datasource, so a move cleans up its stale entry; ``None`` when ambiguous, so
        nothing is mass-deleted."""
        if not dataset.source_queries:
            return None
        try:
            identity = await self.storage.resolve_dataset_identity(dataset.name)
        except AmbiguousDatasetError:
            return None
        return identity[0] if identity is not None else None

    async def _narrowed_access(
        self, *, dataset: SlayerDataset, prior: Optional[SlayerDataset],
    ) -> List[NarrowedAccessWarning]:
        """The saved dataset, if narrowed, and — when its tags changed — each reader it newly narrows, in any datasource."""
        loaded, _ = await self.storage.load_datasets()
        retagged = prior if prior is not None and prior.access_tags != dataset.access_tags else None
        if retagged is None and not any(m.access_tags for m in loaded):
            return []
        key = (dataset.data_source, dataset.name)
        after = narrowing_datasets(loaded)
        flagged = {key} & set(after)
        if retagged is not None:
            replaced = {key, (retagged.data_source, retagged.name)}
            before = narrowing_datasets([*(m for m in loaded if (m.data_source, m.name) not in replaced), retagged])
            flagged |= set(after) - set(before)
        return [
            NarrowedAccessWarning(data_source=ds, dataset=name, narrowed_by=[f"{d}.{n}" for d, n in after[(ds, name)]])
            for ds, name in sorted(flagged)
        ]

    async def validate_sql_dataset_source(self, dataset: SlayerDataset) -> None:
        """Statically classify a raw-``sql`` source, then trial-execute it: a placeholder outside a literal
        position, a non-read-only or an unparseable source is rejected with no DB call; a reachable backend's
        rejection also blocks. Parameterized SQL is classified with its placeholders as literals, not trial-run."""
        if not dataset.sql or dataset.sql_table or dataset.source_queries:
            return
        ds = (
            await self.storage.get_datasource(dataset.data_source)
            if dataset.data_source
            else None
        )
        sqlglot_name = dialect_for_ds_type(ds.type).sqlglot_name if ds else None
        bare, blocked = extract_variable_refs(dataset.sql)
        parameterized = bool(bare or blocked)
        check_mode_a_placeholders(
            dataset.sql, dialect=dialect_for_ds_type(ds.type if ds else None), field=f"Dataset {dataset.name!r} sql",
        )
        # render_probe_text is SLayer's canonical Mode-A probe render (blocks → (1=1), {var} → 0).
        safety = classify_dataset_sql(render_probe_text(dataset.sql), dialect=sqlglot_name)
        reject_reason = _sql_safety_reject_reason(safety)
        if reject_reason:
            raise DatasetSqlValidationError(
                dataset_name=dataset.name,
                data_source=dataset.data_source or "",
                ds_type=ds.type if ds else None,
                reason=reject_reason,
            )
        if parameterized:
            logger.info(
                "Skipping save-time SQL trial-execute for dataset %r: dataset.sql "
                "carries %d placeholder(s), which cannot be trial-filled.",
                dataset.name, len(bare | blocked),
            )
            return
        if ds is None:
            logger.warning(
                "Skipping save-time SQL validation for dataset %r: datasource "
                "%r is unset or not configured.", dataset.name, dataset.data_source,
            )
            return
        await self._trial_execute_sql_source(dataset, ds)

    async def _trial_execute_sql_source(self, dataset: SlayerDataset, ds: DatasourceConfig) -> None:
        """Trial-execute read-only ``dataset.sql`` against ``ds``: raise on a
        reachable rejection, warn-and-return on an inconclusive verdict."""
        assert dataset.sql is not None
        try:
            dialect = dialect_for_ds_type(ds.type)
            await self.client_for(ds).get_result_column_types(
                render_type_probe(sql_dataset_trial_query(dataset.sql, dialect=dialect), dialect=dialect)
            )
        except Exception as exc:
            if (
                _is_transient_db_error(exc)
                or _is_auth_failure(exc)
                or _is_unreachable_db_error(exc)
            ):
                logger.warning(
                    "Save-time SQL validation for dataset %r was inconclusive "
                    "(datasource %r, type %r): %s. Saving anyway.",
                    dataset.name, dataset.data_source, ds.type,
                    getattr(exc, "orig", exc),
                )
                return
            raise DatasetSqlValidationError(
                dataset_name=dataset.name,
                data_source=dataset.data_source,
                ds_type=ds.type,
                reason=str(getattr(exc, "orig", exc)),
            ) from exc

    async def _validate_mode_a_relationship_paths(
        self, dataset: SlayerDataset, *, loaded: Dict[str, Optional[SlayerDataset]],
    ) -> None:
        """Reject a broken dotted chain / legacy ``__`` split-alias at save time via
        the generator's resolver; a revisiting field surface becomes the same
        ``DerivedFieldCircularError`` the storage door raises, while a dataset-level
        filter propagates the base ``CircularRelationshipPathError`` (it is not a field)."""

        # Parse with the datasource's own dialect (matching generation), else
        # valid non-Postgres Mode-A SQL could be mis-rejected. Missing → postgres.
        datasource = await self.storage.get_datasource(dataset.data_source)
        dialect = self._dialect_for_type(datasource.type if datasource else None)
        datasets_by_name = {k: v for k, v in loaded.items() if v is not None}
        check_definition_reach(dataset=dataset, reachable=datasets_by_name)
        check_definition_ambiguity(dataset=dataset, reachable=datasets_by_name)
        check_dataset_filters(dataset=dataset, reachable=datasets_by_name)

        def _expand(sql: str) -> None:
            expand_derived_refs_sync(
                sql=sql, dataset=dataset, alias_path=dataset.name,
                datasets_by_name=datasets_by_name, dialect=dialect,
            )

        for col in dataset.fields:
            for fragment in (col.definition_sql if col.resolved else None, col.filter):
                if not fragment:
                    continue
                try:
                    _expand(fragment)
                except CircularRelationshipPathError as exc:
                    # A revisit inside a referenced derived field names that inner
                    # field and its declaring dataset.
                    raise DerivedFieldCircularError(
                        field=exc.field or col.name,
                        reference=exc.reference, root_dataset=exc.root_dataset,
                        revisited=exc.revisited, hop=exc.hop, via=exc.via,
                    ) from exc
        for sql in dataset.filters or []:
            _expand(sql)

    async def _preload_relationship_targets(
        self, dataset: SlayerDataset, *, failures: DocumentLoadFailures,
    ) -> Dict[str, Optional[SlayerDataset]]:
        """The datasource's loadable datasets by name (``dataset`` itself in place of its stored copy) —
        the bidirectional closure is the connected component."""
        peers = failures.skip(await self.storage.load_datasets(data_source=dataset.data_source, exclude=dataset.name))
        return {**{m.name: m for m in peers}, dataset.name: dataset}

    async def _validate_and_populate_cache(
        self, dataset: SlayerDataset, *, notices_out: Optional[List[DateOnlyMidnightWarning]] = None,
    ) -> SlayerDataset:
        """Dry-run-validate a query-backed dataset → copy with cache fields populated (an undefaulted ``{var}`` refuses);
        its date-only notices go to ``notices_out``."""
        if not (dataset.source_queries or []):
            return dataset
        dataset = await self._pin_populations(dataset=dataset, prefer_data_source=None)
        virtual = await self._expand_query_backed_dataset(dataset=dataset, notices_out=notices_out)
        return dataset.model_copy(update={
            "fields": list(virtual.fields),
            # Refreshed from the resolved virtual dataset: the backing query may now
            # resolve through a different datasource, which downstream callers read
            # before expanding the dataset.
            "data_source": virtual.data_source,
        })

    async def _pin_populations(self, *, dataset: SlayerDataset, prefer_data_source: Optional[str]) -> SlayerDataset:
        """``dataset`` with each rootless stage's dimension anchor written as its ``population``, marked
        ``population_inferred`` (a unit or undefined anchor stays unpinned)."""
        stages: List[SlayerQuery] = list(dataset.source_queries or [])
        if all(stage.population is not None for stage in stages):
            return dataset
        ordered = topologically_order_stages(stages, expressions=False)
        localized, displays = localize_stages(ordered)
        localized, _ = await self._respell_spine(
            queries=localized, stage_names={d.name for d in displays.values()}, data_source=prefer_data_source,
        )
        inference = await self._infer_populations(
            query=localized[-1], named_queries={q.name: q for q in localized[:-1] if q.name},
            prefer_data_source=prefer_data_source, stage_displays=displays,
        )
        inferred = {id(ordered[-1]): inference.pins.get(localized[-1].name or ROOT_STAGE)}
        inferred.update(
            (id(stage), inference.pins.get(loc.name))
            for stage, loc in zip(ordered, localized[:-1]) if loc.name is not None
        )
        stage_names = {stage.name for stage in stages if stage.name}
        pinned: List[SlayerQuery] = []
        for stage in stages:
            if stage.population is None and (population := inferred.get(id(stage))) is not None:
                if population in stage_names:
                    raise ValueError(
                        f"Stage {stage.name or '(final)'!r} of saved query {dataset.name!r} reads dataset "
                        f"{population!r}, which is also a stage name in that query; rename the stage "
                        f"{population!r} so the population can be saved."
                    )
                stage = stage.model_copy(update={"population": population, "population_inferred": True})
            pinned.append(stage)
        return dataset.model_copy(update={"source_queries": pinned})

    async def _resolve_datasource(self, dataset: SlayerDataset) -> DatasourceConfig:
        ds_name = dataset.data_source
        if not ds_name:
            raise ValueError(
                f"Dataset '{dataset.name}' has no data_source configured. "
                f"Set data_source on the dataset or ensure the source dataset has one."
            )
        ds = await self.storage.get_datasource(ds_name)
        if ds is None:
            raise ValueError(f"Datasource '{ds_name}' not found for dataset '{dataset.name}'")
        return ds

    @staticmethod
    def _dialect_for_type(ds_type: Optional[str]) -> str:
        """Map a datasource ``type`` to its sqlglot dialect name (unknown/None → postgres)."""
        return dialect_for_ds_type(ds_type).sqlglot_name


@contextlib.asynccontextmanager
async def short_lived_engine(
    storage: Optional[StorageBackend] = None, *, client_factory: Optional[ClientFactory] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine for one call that brought no client (a temporary store when none is given), closed on exit."""
    with contextlib.ExitStack() as stack:
        if storage is None:
            storage = YAMLStorage(base_dir=stack.enter_context(tempfile.TemporaryDirectory()))
        engine = SlayerQueryEngine(storage=storage, client_factory=client_factory)
        try:
            yield engine
        finally:
            try:
                await asyncio.shield(engine.aclose())
            finally:
                engine.close()


def _entry_client_key(entry: _CacheEntry) -> ClientCacheKey:
    """The key of the client that wrote ``entry``."""
    return (entry.resolved_data_source or "", entry.ds_key)


# Relationship-cardinality detection helpers


def _relationship_signature(relationship: DatasetRelationship) -> tuple:
    """Stable identity for a relationship: (to, sorted key pairs)."""
    return (relationship.to, tuple(sorted((p[0], p[1]) for p in relationship.column_pairs)))


def _scan_failed_finding(*, data_source, dataset, relationship: DatasetRelationship, exc) -> RelationshipCardinalityFinding:
    """Report a relationship whose profiling scan raised, instead of aborting."""
    logger.warning(
        "detect_relationship_cardinality: %s -> %s failed: %s",
        dataset.name, relationship.to, exc,
    )
    return RelationshipCardinalityFinding(
        data_source=data_source,
        dataset=dataset.name,
        to=relationship.to,
        column_pairs=list(relationship.column_pairs),
        stored=relationship.cardinality,
        detected=None,
        verdict=CardinalityVerdict.SCAN_FAILED,
        note=f"profiling scan failed: {exc}",
    )


def _detection_skip_reason(
    *, dataset: SlayerDataset, target: SlayerDataset | None, src_cols: List[str], tgt_cols: List[str],
) -> str | None:
    """Why a relationship can't be profiled; ``None`` when it can."""
    if dataset.sql_table is None:
        return (
            f"dataset {dataset.name!r} is not table-backed (sql/query-backed); "
            f"cardinality profiling supports sql_table datasets only"
        )
    if target is None or target.sql_table is None:
        tn = target.name if target is not None else "?"
        return f"relationship target {tn!r} is not a table-backed dataset; skipped"
    for mdl, cols in ((dataset, src_cols), (target, tgt_cols)):
        for c in cols:
            err = relationship_key_error(dataset=dataset.name, target=target.name, key=c,
                                 side=mdl.name, fields=mdl.fields)
            if err is not None:
                return (
                    f"{str(err).splitlines()[0]}; cardinality profiling "
                    f"supports keys over each dataset's own source only"
                )
    return None


def _key_reads(*, dataset: SlayerDataset, keys: List[str], dialect: str) -> List[Expr]:
    """Each key as SLayer reads it from the dataset's table: its column, or a computed key's expression."""
    by_name = {c.name: c for c in dataset.fields}
    return [
        exp.column(key_identifier(f.physical_name, quoted=f.physical_quoted)) if f.is_base
        else exp.maybe_parse(f.definition_sql or f.name, dialect=dialect)
        for f in (by_name[k] for k in keys)
    ]


def _validate_relationship_keys(
    *, dataset: SlayerDataset, loaded: Dict[str, Optional[SlayerDataset]],
) -> None:
    """Every relationship's keys name valid key fields on both sides (source re-checked for a populated query-backed dataset)."""
    _check_relationship_keys(dataset_name=dataset.name, fields=dataset.fields, relationships=dataset.relationships)
    for relationship in dataset.relationships:
        target = loaded.get(relationship.to)
        if target is None:
            continue
        for _, key in relationship.column_pairs:
            err = relationship_key_error(dataset=dataset.name, target=relationship.to, key=key,
                                 side=target.name, fields=target.fields)
            if err is not None:
                raise err


def _unique_contradictions(
    *, dataset: SlayerDataset, target: SlayerDataset, src_cols: List[str], tgt_cols: List[str],
    src_side: SideStats, tgt_side: SideStats,
) -> list[str]:
    """Single-field relationship keys declared unique/PK but observed to have dups."""
    out: list[str] = []
    for mdl, cols, side in (
        (dataset, src_cols, src_side),
        (target, tgt_cols, tgt_side),
    ):
        if len(cols) != 1 or side.observed_unique:
            continue
        c = cols[0]
        col = next((x for x in mdl.fields if x.name == c), None)
        if col is not None and declares_solo_unique(fields=mdl.fields, field=col):
            out.append(
                f"{mdl.name}.{c} is declared unique but the data has duplicates"
            )
    return out
