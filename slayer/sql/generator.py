"""SQL generator — converts a ``PlannedQuery`` to SQL via sqlglot AST."""

import logging
import re
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Literal,
    Optional,
    Set,
    Tuple,
    cast,
)

from decimal import Decimal
from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    BUILTIN_AGGREGATION_PARAM_ORDER,
    DataType,
    JoinType,
    TimeGranularity,
)
from pydantic import BaseModel, ConfigDict, Field

from slayer.core.errors import (
    AggregationNotAllowedError,
    MaterialisationStageError,
    DatasetFilterCycleError,
    QueryBackedCycleError,
)
from slayer.core.enums import RANK_FAMILY_TRANSFORMS
from slayer.core.field_expression import portable_text
from slayer.core.granularity import CustomGranularity, Granularity, granularity_parts
from slayer.core.granularity import bucket_type as granularity_bucket_type
from slayer.core.time_spine import TIME_SPINE_DATASET, TIME_SPINE_FIELD, is_spine
from slayer.core.keys import restricts_own_rows, BOOL_CONNECTIVE_OPS, KIND_POLICY, REGROUP_LEAF_PREFIX, SLOT_COMPOSITE_KINDS, TEMPORAL_TYPES, VALUE_KEY_TYPES, AggregateKey, ArithmeticKey, CastKey, FieldKey, FieldSqlKey, FieldTypeFn, InKey, ListSetKey, LiteralKey, Phase, ScalarCallKey, StarKey, TimePointCmpKey, TimeTruncKey, TransformKey, ValueKey, aggregation_source_type, is_ranked, field_path, date_add_type, is_boolean_shaped, parameter_row_leaves, shift_offset_of, source_anchor_path, substitute_value_keys, temporal_type, list_element_type, list_valued, literal_type, value_type, walk_value_keys, TEXT_RESULT_SCALARS
from slayer.core.relationship_walker import (
    OrientedRelationship,
    aggregation_owner,
    follow,
    is_lateral_edge,
    is_unrelated_edge,
    relationship_key_fields,
    dataset_field_element,
    dataset_field_type,
    provably_to_one,
    resolved_terminal,
    trusted_to_one,
)
from slayer.core.models import (
    DatasetField, EmptyList, EmptyValue, aggregation_definition, empty_value, rendered_expression,
)
from slayer.core.refs import EXPRESSION_SOURCE_KINDS as _EXPRESSION_SOURCE_KINDS, key_display
from slayer.core.window_duration import WINDOW_UNIT_GRANULARITY
from slayer.sql.field_expansion import (
    is_trivial_base,
    collect_root_scope_joined_paths,
    expand_field_definition_parts_sync,
    wrap_field_filter,
)
from slayer.ir.planned import (
    AssociationProducerKernel,
    EntityDedup,
    MaskTyping,
    PlannedQuery,
    PlannedRowBranch,
    RankedGrainMember,
    RankedProducerKernel,
    RegroupAttachPlan,
    SemiJoinFilter,
    SemiJoinHop,
    SpineFactor,
    StageKind,
    TrailingWindowProducerKernel,
    ValueSlot,
    emitted_plans,
    is_spliced,
    plan_has_semi_join_filters,
    regroup_producer_identity,
    validate_grain_determination,
)
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql._identifier_fit import fit_identifier, overlimit_tokens
from slayer.sql import staged_plan
from slayer.sql.dialects import SqlDialect, get_dialect
from slayer.sql.dialects.base import LIST_POSITION, bare_comparands
from slayer.sql.untrusted_projection import mark_untrusted, project_untrusted_hops
from slayer.sql.naming import (
    OUTER_WRAP_ALIAS,
    ROW_IDENTITY_FIELD,
    AliasAllocator,
    identified_datasets,
    canonical_aggregate_alias,
    cte_name_from_alias,
    dialect_folds_case,
    expression_source_leaf,
    flat_name,
    key_identifier,
    maybe_quote_ident,
    internal_result_key,
    next_slot_result_key,
    result_key_from_alias,
    quote_mixed_case_identifiers,
)
from slayer.sql.render.cte_assembly import (
    CteEntry,
    assemble_with_chain,
    cte_entry,
    reachable_cte_entries,
    rename_embedded_ctes,
)
from slayer.sql.render.nodes import Node, NodePhase, fusion_blockers
from slayer.sql.render.joins import (
    AttachedValue,
    GrainPair,
    build_grain_joinback_condition,
    temporal_sides,
    grain_alias_field,
)
from slayer.sql.render.order_terms import (
    HOST_BASE_SCOPES,
    OrderEnv,
    OrderScope,
    OrderSlotNotMaterialisedError,
    ScopedOrder,
    resolve_order_term,
)
from slayer.sql.render.ranked import (
    RANK_KEY_FIELD,
    RANK_PASS_FIELD,
    RANK_VALUE_FIELD,
    RANKED_SOURCE_ALIAS,
    RankedGrainProjection,
    build_rank_field,
    build_rank_key,
    build_ranked_cte_select,
    build_ranked_pick,
    ranked_ordered,
)
from slayer.sql.render.aggregates import (
    AggregateSpec,
    aggregate_input,
    apply_aggregate,
    is_builtin_agg,
    render_aggregate,
    resolve_agg_entry,
)
from slayer.sql.render.parse import apply_ast_rewrites, parse_expression, parse_predicate, parse_table
from slayer.sql.sql_template import SqlTemplate, SqlTemplateError, sql_template
from slayer.sql.render.row_expr import _literal, bucket_operand, render_bucket
from slayer.sql.render.value_expr import (
    AliasFacilities,
    CompositeFacilities,
    FilterFacilities,
    RenderContext,
    _ranked_value_cast_type,
    _wrap_cast_for_type,
    contains_aggregate,
    keeps_native_type,
    render_value_key,
    rewrite_log_alias,
)
from slayer.sql.scope import ScopeFrame
from slayer.sql.scope_check import maybe_validate_scopes
from slayer.ir.bound import BoundExpr
from slayer.sql.type_probe import TypeProbe, mint_type_probe
from slayer.sql.stage_wrapper import (
    build_flat_rename_wrapper,
    unmangle_dotted_table_refs,
)
from slayer.core.models import SlayerDataset




class ResolvedAggKwarg(BaseModel):
    """A parameter value resolved to SQL (a literal, or a key rendered through a scope)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["expr"] = "expr"
    value: Expression
    input_type: DataType | None


class AggRenderSpec(BaseModel):
    """Typed input record for the dialect-aware aggregation builders; ``expression`` is
    the rendered template (``None``: a built-in builder), never the definition."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    sql: str | None

    name: str

    dataset_name: str

    aggregation: str

    alias: str

    expression: Optional[str] = None

    agg_kwargs: Dict[str, ResolvedAggKwarg] = {}

    time_field: Optional[str] = None

    type: Optional[DataType] = None

    field_type: Optional[DataType] = None

    #: The aggregated value's type as the aggregation reads it (a BOOLEAN is lowered to its integer).
    input_type: Optional[DataType] = None

    #: An expression source's rendered value, never re-parsed from text.
    value: Optional[Expression] = None

    #: A list aggregation's element type (the input's type).
    element_type: Optional[DataType] = None


def _is_spine_bucket(key: Any) -> bool:
    """A bucket of the spine field, as a spine stage's host reads it."""
    return isinstance(key, TimeTruncKey) and isinstance(key.source, FieldKey) \
        and key.source.leaf == TIME_SPINE_FIELD and source_anchor_path(key.source) in ((), (TIME_SPINE_DATASET,))


#: Window transforms stepping along the time axis one value per cell.
_ROW_STEP_TRANSFORMS = frozenset({"cumsum", "lag", "lead"})


def _key_equality(left: Expression, right: Expression) -> exp.EQ:
    left, right = bare_comparands(left, right)
    return exp.EQ(this=left, expression=right)


def _strip_declared_cast(expr: Expression) -> Expression:
    """Unwrap one declared-type ``CAST`` a derived-field expansion added."""
    return expr.this if isinstance(expr, exp.Cast) else expr


class _WindowedEmission(BaseModel):
    """Renderer-internal field bundle for one trailing-window emission"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    aggregate_slot_id: str
    agg: str
    window_parts: List[Tuple[int, str]]
    window_granularity: Granularity
    window_time_dimension_slot_id: str
    dimension_slot_ids: List[str]
    other_time_dimension_slot_ids: List[str]
    grain_slot_ids: List[str]
    where_filter_ids: List[str]
    src_filter_rewrites: List[Any]
    #: Set for a windowed first/last: the interval-row ranking key (_w_rank).
    ranking_time_key: Any = None
    #: Reference-bearing parameters read per interval row as _src._w_p<i>.
    picked_params: List[Any] = []


def _windowed_emission_from_kernel(*, planned_query, kernel) -> _WindowedEmission:
    """Derive the windowed emission from a trailing-window kernel producer."""
    if (
        len(planned_query.aggregate_slots) != 1
        or any(
            r.attach_phase != "row" for r in planned_query.regroup_attach_plans
        )
        or planned_query.combined_expression_slots
        or planned_query.transform_layers
        or any(m.typing == MaskTyping.METRIC for m in planned_query.masks)
        or planned_query.order
        or planned_query.limit is not None
        or planned_query.offset is not None
    ):
        raise RuntimeError(
            "Trailing-window kernel producer carries structure beyond its "
            "grain + windowed aggregate; synthesis and rendering disagree."
        )
    agg_slot = planned_query.aggregate_slots[0]
    bucket_sid = kernel.bucket_slot_id
    key_by_id = {s.id: s.key for s in planned_query.row_slots}
    dims: List[str] = []
    other_tds: List[str] = []
    for sid in planned_query.grain or ():
        if sid == bucket_sid:
            continue
        if isinstance(key_by_id[sid], TimeTruncKey):
            other_tds.append(sid)
        else:
            dims.append(sid)
    grain = [*dims, bucket_sid, *other_tds]
    if set(grain) != set(planned_query.grain or ()) or set(planned_query.projection) != {
        *grain, agg_slot.id,
    }:
        raise RuntimeError(
            "Trailing-window kernel producer's grain does not match its "
            "projection."
        )
    return _WindowedEmission(
        aggregate_slot_id=agg_slot.id,
        agg=agg_slot.key.agg,
        window_parts=kernel.window_parts,
        window_granularity=kernel.window_granularity,
        window_time_dimension_slot_id=bucket_sid,
        dimension_slot_ids=dims,
        other_time_dimension_slot_ids=other_tds,
        grain_slot_ids=grain,
        where_filter_ids=list(kernel.src_where_filter_ids),
        src_filter_rewrites=list(kernel.src_filter_rewrites),
        ranking_time_key=kernel.ranking_time_key,
        picked_params=list(kernel.picked_params),
    )


class _RankedEmission(BaseModel):
    """Renderer-internal field bundle for one ranked (first/last) emission"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    aggregate_slot_id: str
    agg: str
    ranking_time_key: Any
    grain: List[RankedGrainMember]
    where_filter_ids: List[str]


def _ranked_emission_from_kernel(*, planned_query, kernel) -> _RankedEmission:
    """Derive the ranked emission from a ranked-kernel producer."""
    if (
        len(planned_query.aggregate_slots) != 1
        or any(
            r.attach_phase != "row" for r in planned_query.regroup_attach_plans
        )
        or planned_query.combined_expression_slots
        or planned_query.transform_layers
        or any(m.typing == MaskTyping.METRIC for m in planned_query.masks)
        or planned_query.order
        or planned_query.limit is not None
        or planned_query.offset is not None
    ):
        raise RuntimeError(
            "Ranked kernel producer carries structure beyond its grain + "
            "ranked aggregate; synthesis and rendering disagree."
        )
    agg_slot = planned_query.aggregate_slots[0]
    key_by_id = {s.id: s.key for s in planned_query.row_slots}
    grain = [
        RankedGrainMember(host_slot_id=sid, ranked_key=key_by_id[sid])
        for sid in planned_query.grain or ()
    ]
    if list(planned_query.projection) != [
        *[m.host_slot_id for m in grain], agg_slot.id,
    ]:
        raise RuntimeError(
            "Ranked kernel producer's grain does not match its projection."
        )
    return _RankedEmission(
        aggregate_slot_id=agg_slot.id,
        agg=kernel.agg,
        ranking_time_key=kernel.ranking_time_key,
        grain=grain,
        where_filter_ids=[
            e.id for e in _lower_positions(planned_query).filters
            if e.phase == Phase.ROW
        ],
    )


# ---------------------------------------------------------------------------
# Mask / order lowering: the plan carries typed masks and scopeless
# order entries; placement (base WHERE / HAVING / outer WHERE / post wrapper)
# and the order scope are pure emission decisions reconstructed here.
# ---------------------------------------------------------------------------


#: Test hook: force metric masks onto the general materialize-and-filter path
#: (outer WHERE over hidden columns) instead of the HAVING lowering.
_FORCE_MASK_FALLBACK = False


class _LoweredFilter(BaseModel):
    """One mask with its render placement."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    phase: Phase
    expression: BoundExpr


class _LoweredPositions(BaseModel):
    """Per-plan lowering output: placed filter entries + scoped order entries."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    filters: List["_LoweredFilter"]
    #: Metric masks that resolve only after attachment (combined placeholders) —
    #: rendered at the outer WHERE, never HAVING in a ``_cm_*`` CTE (the LEFT JOIN
    #: would resurface a NULL-aggregate host row).
    outer_where_ids: List[str]
    order: List[ScopedOrder]


def _plan_slots(planned_query) -> List[ValueSlot]:
    return [
        *planned_query.row_slots,
        *planned_query.aggregate_slots,
        *planned_query.combined_expression_slots,
    ]


def _combined_attached_slot_ids(planned_query, slot_id_by_key) -> Set[str]:
    """Slot ids the combined SELECT reads from an attached producer CTE — an
    attach-plan fact, not a key-shape walk. Includes a dual-role placeholder
    (also row-attached, so BASE-staged: ``_base`` is the earliest relation it
    materialises in, but combined-level consumers read the producer CTE)."""
    out: Set[str] = set()
    for attach in planned_query.regroup_attach_plans:
        if attach.attach_phase != "combined":
            continue
        for sub in attach.substitutions:
            sid = slot_id_by_key.get(sub.placeholder)
            if sid is not None:
                out.add(sid)
    return out


def _lower_positions(planned_query) -> _LoweredPositions:
    """Placement from the planner stage (D5): field → base WHERE; metric →
    HAVING at BASE, the combined outer WHERE at PRODUCER / COMBINED (or reading
    a combined-attached dual-role value), the outer wrapper at DERIVED or when
    any transform is present."""
    slots_by_id = {s.id: s for s in _plan_slots(planned_query)}
    slot_id_by_key = {s.key: s.id for s in slots_by_id.values()}
    combined_attached = _combined_attached_slot_ids(planned_query, slot_id_by_key)
    outer_ids: List[str] = []
    # A HAVING would shrink the series every transform reads (Axiom 14).
    has_transform = any(
        s.stage is not None and s.stage.kind is StageKind.DERIVED
        for s in slots_by_id.values()
    )
    # The row supply's LEFT JOIN onto the host would resurface a key the host's HAVING removed.
    supplied = planned_query.row_supply is not None

    def _lower_mask(mask) -> _LoweredFilter:
        slot = slots_by_id[mask.slot_id]
        stage_kind = slot.stage.kind if slot.stage is not None else None
        if mask.typing == MaskTyping.FIELD:
            phase = Phase.ROW
        elif stage_kind is StageKind.DERIVED or has_transform:
            phase = Phase.POST
        else:
            phase = Phase.AGGREGATE
            outer = supplied or stage_kind in (
                StageKind.PRODUCER, StageKind.COMBINED,
            ) or any(
                slot_id_by_key.get(k) in combined_attached
                for k in walk_value_keys(slot.key)
            )
            if (outer or _FORCE_MASK_FALLBACK) and mask.slot_id not in outer_ids:
                outer_ids.append(mask.slot_id)
        return _LoweredFilter(
            id=mask.slot_id, phase=phase,
            expression=BoundExpr(value_key=slot.key),
        )

    entries = [_lower_mask(m) for m in planned_query.masks]

    order = _lower_order_entries(
        planned_query=planned_query,
        slots_by_id=slots_by_id,
        combined_attached=combined_attached,
    )
    return _LoweredPositions(
        filters=entries, outer_where_ids=outer_ids, order=order,
    )


def _lower_order_entries(
    planned_query,
    *,
    slots_by_id: Dict[str, ValueSlot],
    combined_attached: Set[str],
) -> List[ScopedOrder]:
    order: List[ScopedOrder] = []
    outer_composite_ids = staged_plan.combined_composite_slot_ids(planned_query)
    for entry in planned_query.order:
        slot = slots_by_id.get(entry.slot_id)
        if slot is None:
            raise OrderSlotNotMaterialisedError(
                f"ORDER BY references slot id={entry.slot_id!r}, which this "
                f"plan did not materialise (it carries "
                f"{sorted(slots_by_id)}).",
            )
        order.append(ScopedOrder(
            slot_id=entry.slot_id,
            direction=entry.direction,
            scope=_classify_order_scope(
                slot=slot,
                combined_attached=combined_attached,
                outer_composite_ids=outer_composite_ids,
                public_projection=list(planned_query.projection),
            ),
            nulls=entry.nulls,
        ))
    return order


def _classify_order_scope(
    *,
    slot: ValueSlot,
    combined_attached: Set[str],
    outer_composite_ids: Set[str],
    public_projection: List[str],
) -> OrderScope:
    """Name the scope that PRODUCES ``slot``'s value, from its planner stage plus the
    attach facts; a composite is OUTER_COMPOSITE exactly where the planner places it."""
    if slot.id in combined_attached:
        return OrderScope.CROSS_DATASET_CTE
    stage_kind = slot.stage.kind if slot.stage is not None else None
    if stage_kind is StageKind.PRODUCER:
        if isinstance(slot.key, AggregateKey) and any(
            kw == "window" for kw, _ in slot.key.kwargs
        ):
            return OrderScope.WINDOWED_CTE
        return OrderScope.CROSS_DATASET_CTE
    if isinstance(slot.key, TransformKey):
        return OrderScope.TRANSFORM_STEP
    if slot.id in outer_composite_ids:
        return OrderScope.OUTER_COMPOSITE
    if slot.hidden or slot.id not in public_projection:
        return OrderScope.HOST_BASE_HIDDEN
    return OrderScope.HOST_BASE


def _layer_batches_at_level(
    planned_query,
    *,
    slots_by_id: Dict[str, Any],
    level: int,
) -> Tuple[list, list, list]:
    """Restrict each transform layer to its slots staged at ``level``, split as
    (window, time_shift, consecutive_periods) batches in transform_layers order."""
    ready_window: list = []
    ready_time_shift: list = []
    ready_cp: list = []
    for layer in planned_query.transform_layers:
        slot_ids = [
            sid for sid in layer.slot_ids
            if slots_by_id[sid].stage.level == level
        ]
        if not slot_ids:
            continue
        batch = (
            layer if len(slot_ids) == len(layer.slot_ids)
            else layer.model_copy(update={"slot_ids": slot_ids})
        )
        if layer.op == "time_shift":
            ready_time_shift.append(batch)
        elif layer.op == "consecutive_periods":
            ready_cp.append(batch)
        else:
            ready_window.append(batch)
    return ready_window, ready_time_shift, ready_cp



logger = logging.getLogger(__name__)

# Consumer policy from the kind registry (core/keys.KIND_POLICY records intent).
_MATERIALISED_ORDER_KINDS = tuple(
    k for k in VALUE_KEY_TYPES if KIND_POLICY[k].materialised_order
)

_BUILTIN_BAREARG_AGGS_LOCAL_SLICE: frozenset[str] = BUILTIN_AGGREGATIONS

# sqlglot rewrites log10/log2 into 2-arg LOG(base,x), breaking dialects lacking 2-arg LOG; rewrite back to Anonymous.


def _grand_total_value(value: Expression) -> Expression:  # pyright: ignore[reportPrivateImportUsage]
    """``value`` as an aggregate over the whole population: one row, even when empty."""
    always = exp.GTE(this=exp.Count(this=exp.Star()), expression=exp.Literal.number(0))
    return exp.Case(ifs=[exp.If(this=always, true=value)])


def _grouped(predicate: Expression) -> Expression:  # pyright: ignore[reportPrivateImportUsage]
    """``predicate`` parenthesised when it is an ``AND`` / ``OR`` (safe as a conjunct)."""
    return exp.Paren(this=predicate) if isinstance(predicate, (exp.And, exp.Or)) else predicate


def _conjunction(parts: Sequence[Expression]) -> Optional[Expression]:  # pyright: ignore[reportPrivateImportUsage]
    """The ``AND`` of already-grouped ``parts``; ``None`` when empty."""
    if not parts:
        return None
    out = parts[0]
    for part in parts[1:]:
        out = exp.And(this=out, expression=part)
    return out

# Parameter values rendered through a scope; scalars render as literals, markers never.
def _is_row_param(value: object) -> bool:
    """A row-valued aggregation parameter: a row-level key carrying no aggregate or transform."""
    return isinstance(value, VALUE_KEY_TYPES) and not any(
        isinstance(k, (AggregateKey, TransformKey, StarKey)) for k in walk_value_keys(cast(ValueKey, value))
    )

# Shift units whose whole-unit offsets map each bucket start onto another, making the outer re-trunc a per-row no-op.
_BUCKET_ALIGNED_SHIFT_UNITS: dict[str, frozenset[str]] = {
    "month": frozenset({"month", "quarter", "year"}),
    "quarter": frozenset({"quarter", "year"}),
    "year": frozenset({"year"}),
    "week": frozenset({"week", "week_sunday"}),
    "week_sunday": frozenset({"week", "week_sunday"}),
    "day": frozenset({"day", "week", "week_sunday", "month", "quarter", "year"}),
    "hour": frozenset({"hour", "day", "week", "week_sunday", "month", "quarter", "year"}),
    "minute": frozenset({"minute", "hour", "day", "week", "week_sunday",
                         "month", "quarter", "year"}),
    "second": frozenset({"second", "minute", "hour", "day", "week", "week_sunday",
                         "month", "quarter", "year"}),
}


def _no_field_type(_key: Any) -> Optional[DataType]:
    return None


def _shift_preserves_bucket_starts(*, bucket: Granularity, shift: Granularity) -> bool:
    """A built-in shift that maps every built-in bucket start onto a bucket start (a custom one re-buckets)."""
    if isinstance(bucket, CustomGranularity) or isinstance(shift, CustomGranularity):
        return False
    return shift.value in _BUCKET_ALIGNED_SHIFT_UNITS.get(bucket.value, frozenset())


def _is_host_grain(key) -> bool:
    """True for an ``AggregateKey`` marked ``locus="host"``."""
    return getattr(key, "locus", "target") == "host"


# --- Transform-input shape classification, shared by the series-regime
# time_shift selector and the consecutive_periods emitter. ---

#: Argument-less rank-family window functions (``ntile`` carries its bucket count).
_RANK_WINDOW_FNS = {"rank": exp.Rank, "dense_rank": exp.DenseRank, "percent_rank": exp.PercentRank}

# Real ValueKey args (a ScalarCallKey / iif may also carry raw scalar literals).
_COMPOUND_VALUE_KEYS = (
    FieldKey, FieldSqlKey, TimeTruncKey, StarKey,
    AggregateKey, TransformKey, ArithmeticKey, ScalarCallKey, InKey, CastKey, ListSetKey,
)


def _validate_consecutive_periods_input(*, op: str, inner) -> None:
    """Enforce the ``consecutive_periods`` predicate typing contract: a top-level string-valued
    scalar call has no truthiness, and ``and`` / ``or`` / ``not`` take boolean-shaped operands
    (a boolean anywhere a number is needed is its integer)."""
    if (
        isinstance(inner, ScalarCallKey)
        and inner.name.lower() in TEXT_RESULT_SCALARS
    ):
        raise ValueError(
            f"{op!r} cannot use a string-valued predicate ({inner.name}(...)): a "
            f"string has no truthiness. Compare it explicitly (e.g. "
            f"`length(...) > 0`) to form a predicate."
        )
    _walk_cp_predicate(op=op, key=inner, connective_operand=False)


def _walk_cp_predicate(*, op: str, key, connective_operand: bool) -> None:
    """Recursively require boolean-shaped operands under ``and`` / ``or`` / ``not``."""
    if connective_operand and not is_boolean_shaped(key):
        raise ValueError(
            f"{op!r}: 'and' / 'or' / 'not' require boolean-shaped operands (a "
            f"comparison, a null test, IN, or another connective); got "
            f"{type(key).__name__}."
        )
    if isinstance(key, ArithmeticKey):
        for o in key.operands:
            _walk_cp_predicate(op=op, key=o, connective_operand=key.op in BOOL_CONNECTIVE_OPS)
    elif isinstance(key, ScalarCallKey):
        for a in key.args:
            if isinstance(a, _COMPOUND_VALUE_KEYS):
                _walk_cp_predicate(op=op, key=a, connective_operand=False)
    elif isinstance(key, InKey):
        _walk_cp_predicate(op=op, key=key.field, connective_operand=False)
    elif isinstance(key, (CastKey, ListSetKey)):
        _walk_cp_predicate(op=op, key=key.source, connective_operand=False)


def _effective_src_filters(*, lowered_filters, plan) -> list:
    """The lowered filter entries as the windowed ``_src`` scope sees them"""
    rewrites = getattr(plan, "src_filter_rewrites", None)
    if not rewrites:
        return list(lowered_filters)
    by_id = {r.filter_id: r.expression for r in rewrites}
    return [
        fp if fp.id not in by_id
        else fp.model_copy(update={"expression": by_id[fp.id]})
        for fp in lowered_filters
    ]











# A bare-identifier DatasetField.sql renames a physical column; dots are rejected (a dotted ref is a crossing, not a column
# here).


def _apply_joins(*, select: exp.Select, joins) -> exp.Select:
    """Apply ``(join_expr, on_expr, join_type)`` triples to ``select`` in order (``ARRAY``: ClickHouse's ARRAY JOIN)."""
    for join_expr, on_expr, join_type in joins:
        if join_type == "ARRAY":
            select = select.copy()
            select.append("joins", exp.Join(this=join_expr, kind="ARRAY"))
            continue
        select = select.join(join_expr, on=on_expr, join_type=join_type)
    return select


def _cycle_public_aliases_in_projection_order(
    *, planned_query, slots_by_id, aliases_by_slot_id,
):
    """Public projection aliases in query order, cycling each slot's alias list"""
    public_aliases: list[str] = []
    outer_alias_index: Dict[str, int] = {}
    for sid in planned_query.projection:
        slot = slots_by_id[sid]
        if slot.hidden:
            continue
        all_aliases = aliases_by_slot_id.get(sid, [])
        if not all_aliases:
            raise ValueError(
                f"public slot {sid!r} ({type(slot.key).__name__}) rendered no "
                f"column; it would be silently dropped from the result",
            )
        idx = outer_alias_index.setdefault(sid, 0)
        alias = (
            all_aliases[idx] if idx < len(all_aliases) else all_aliases[-1]
        )
        outer_alias_index[sid] = idx + 1
        public_aliases.append(alias)
    return public_aliases


class _SemiJoinOps:
    """Alias / column / table callbacks of one semi-join EXISTS; a list hop is its parent row's elements."""

    def __init__(
        self, *, alias: Callable[[Tuple[str, ...]], str],
        col: Callable[[str, str, bool], exp.Column],  # (table, name, quoted)
        table: Callable[[Any, bool], Tuple[Expression, str]],  # pyright: ignore[reportPrivateImportUsage] — sqlglot exports it
        attached: Set[Tuple[str, ...]],
        computed: Callable[[str, str], Expression],  # (table, a computed key's expression)
    ) -> None:
        self.alias = alias
        self.col = col
        self.table = table
        self.attached = attached
        self.computed = computed

    def key(self, *, table: str, name: str, quoted: bool, computed: Optional[str]) -> Expression:
        """One side's key read from ``table``: its column, or a computed key's expression."""
        return self.col(table, name, quoted) if computed is None else self.computed(table, computed)

    def is_attached(self, hop: SemiJoinHop) -> bool:
        return tuple(hop.node_path) in self.attached

    def eqs(self, *, hop: SemiJoinHop, parent: str, parent_name=None) -> List[Any]:
        """``parent_name`` maps a parent key and its quoting to the spine column projecting it."""
        alias = self.alias(tuple(hop.node_path))
        return [
            exp.EQ(this=(self.col(parent, parent_name(pc, pq), pq) if parent_name
                         else self.key(table=parent, name=pc, quoted=pq, computed=pk)),
                   expression=self.key(table=alias, name=hc, quoted=hq, computed=hk))
            for (pc, hc), (pq, hq), (pk, hk) in zip(hop.join_pairs, _hop_quoting(hop), _hop_computed(hop))
        ]

    def join(self, *, inner: exp.Select, hop: SemiJoinHop, eqs: List[Any]) -> exp.Select:
        relation, kind = self.table(hop, False)
        if hop.lateral is not None:
            return _apply_joins(select=inner, joins=[(relation, None, kind)])
        on = exp.and_(*eqs) if len(eqs) > 1 else eqs[0]
        return inner.join(relation, on=on, join_type="left" if hop.null_extended else "inner")


def _hop_quoting(hop: SemiJoinHop) -> List[Tuple[bool, bool]]:
    """Per key pair, whether each side's stored SQL quotes it."""
    return list(hop.quoted) or [(False, False)] * len(hop.join_pairs)


def _hop_computed(hop: SemiJoinHop) -> List[Tuple[Optional[str], Optional[str]]]:
    """Per key pair, each side's computed-key expression (``None``: a column)."""
    return list(hop.computed) or [(None, None)] * len(hop.join_pairs)


def _spine_projection(*, ops: _SemiJoinOps, hops: Sequence[SemiJoinHop]):
    """The spine's distinct outer correlation columns: ``{(parent, col, quoted): name}``
    and their projection list; a column's alias is quoted as its key is."""
    spine_cols: Dict[Tuple[str, str, bool], str] = {}
    projection: List[Any] = []
    for hop in hops:
        if not ops.is_attached(hop):
            continue
        parent_path = tuple(hop.node_path[:-1])
        parent = ops.alias(parent_path)
        prefix = f"{parent}__" if parent_path else ""
        for (parent_col, _hop_col), (quoted, _), (computed, _) in zip(
                hop.join_pairs, _hop_quoting(hop), _hop_computed(hop)):
            if (parent, parent_col, quoted) in spine_cols:
                continue
            spine_cols[(parent, parent_col, quoted)] = prefix + parent_col
            projection.append(exp.alias_(ops.key(table=parent, name=parent_col, quoted=quoted, computed=computed),
                                         key_identifier(prefix + parent_col, quoted=quoted)))
    return spine_cols, projection


def _semi_join_spine(*, ops: _SemiJoinOps, hops: Sequence[SemiJoinHop], inner: exp.Select) -> exp.Select:
    """Spine shape: FROM a one-row derived table projecting the outer correlation
    keys; attached hops LEFT/INNER join ON it, deeper hops ON their parent."""
    spine_alias = ops.alias(("__slayer_spine",))
    spine_cols, projection = _spine_projection(ops=ops, hops=hops)
    inner = inner.from_(exp.Subquery(
        this=exp.select(*projection), alias=exp.to_identifier(spine_alias),
    ))
    for hop in hops:
        parent = ops.alias(tuple(hop.node_path[:-1]))
        if ops.is_attached(hop):
            eqs = ops.eqs(
                hop=hop, parent=spine_alias,
                parent_name=lambda c, q, p=parent: spine_cols[(p, c, q)],
            )
        else:
            eqs = ops.eqs(hop=hop, parent=parent)
        inner = ops.join(inner=inner, hop=hop, eqs=eqs)
    return inner


def _leaf_list_predicates(
    *, group, conjuncts: List[Expression], alias,
) -> Tuple[List[Tuple[Any, List[Expression]]], List[Expression]]:
    """The group's leaf list hops, each with the conjuncts that read its elements, and the other conjuncts.

    A conjunct reading two leaf lists keeps every list hop in the subquery's FROM."""
    parents = {tuple(h.node_path[:-1]) for h in group.hops}
    leaves = [h for h in group.hops
              if h.lateral is not None and not h.null_extended and tuple(h.node_path) not in parents]
    names = {alias(h.node_path): h for h in leaves}
    by_hop: Dict[Tuple[str, ...], List[Expression]] = {tuple(h.node_path): [] for h in leaves}
    plain: List[Expression] = []
    for conjunct in conjuncts:
        read = {tuple(names[c.table].node_path) for c in conjunct.find_all(exp.Column) if c.table in names}
        if len(read) > 1:
            return [], conjuncts
        if read:
            by_hop[next(iter(read))].append(conjunct)
        else:
            plain.append(conjunct)
    return [(h, by_hop[tuple(h.node_path)]) for h in leaves], plain


def _semi_join_flat(*, ops: _SemiJoinOps, hops, inner):
    """Flat shape: FROM the first attached hop, further attached hops CROSS-joined
    with their correlations in WHERE, deeper hops ON their parent."""
    correlation: List[Any] = []
    first_seen = False
    for hop in hops:
        eqs = ops.eqs(hop=hop, parent=ops.alias(tuple(hop.node_path[:-1])))
        if not ops.is_attached(hop):
            inner = ops.join(inner=inner, hop=hop, eqs=eqs)
            continue
        table, kind = ops.table(hop, not first_seen)
        if first_seen:
            inner = _apply_joins(select=inner, joins=[(table, None, kind if hop.lateral else "CROSS")])
        else:
            inner = inner.from_(table)
        first_seen = True
        correlation.extend(eqs)
    for eq in correlation:
        inner = inner.where(eq)
    return inner


class RenderState(BaseModel):
    """Frozen per-render constants threaded through the transform-chain emitters"""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    planned_query: Any
    bundle: Any
    regroup_env: Any = None
    regroup_join_specs: Any = None


class ChainState(BaseModel):
    """Per-chain-layer accumulators + the layer's source root."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    ctes: Any
    cte_allocator: Any
    slots_by_id: Any
    slot_id_by_key: Any
    available_alias_by_slot_id: Any
    aliases_by_slot_id: Any
    source_dataset: Any
    source_relation: Any


class SQLGenerator:
    """Generates SQL from a typed ``PlannedQuery`` (from ``stage_planner``)."""

    def __init__(
        self, dialect: "str | SqlDialect" = "postgres",
        force_unfused: bool = False,
        force_association_filters: bool = False,
    ):
        if isinstance(dialect, SqlDialect):
            self._dialect: SqlDialect = dialect
        else:
            self._dialect = get_dialect(dialect)
        # Synthetic fusion blocker for lowering-soundness parity tests.
        self._force_unfused = force_unfused
        # Never inline a dataset filter's joined reads (lowering-soundness parity tests).
        self._force_association_filters = force_association_filters
        # Datasets whose filtered datasets are being emitted (innermost last).
        self._gen_filter_stack: Tuple[str, ...] = ()
        self._gen_allocator: Optional[AliasAllocator] = None
        self._gen_rendered_producers: Optional[
            Dict[Any, Tuple[str, Dict[str, str]]]
        ] = None
        self._gen_split_consumers: List[str] = []
        self._gen_reuse_deps: Dict[str, Set[str]] = {}
        #: Statement-scoped CTE-dependency registry (sql P6): a stack of
        #: {cte name -> declared deps}, one per statement being rendered, so a
        #: later split recovers a hoisted producer's edges (never AST-scanned).
        self._gen_dep_stack: List[Dict[str, List[str]]] = []
        #: Field-type resolvers of the planned queries being rendered (innermost last).
        self._gen_field_types: List[FieldTypeFn] = []
        #: Per planned query: its placeholders' types and their list element types.
        self._gen_placeholder_types: List[Tuple[Dict[Any, DataType], Dict[Any, DataType]]] = []
        #: Multi-stage statement state: every stage relation, the current
        #: statement's declared reads and its in-flight splice chain.
        # Spine factors of the plans being rendered; the innermost reads its spine relation.
        self._gen_spines: List[SpineFactor] = []
        # CTEs a rendered spine series reads (a recursive integer sequence), awaiting ``_base``.
        self._gen_spine_ctes: List[CteEntry] = []
        self._gen_stage_relations: FrozenSet[str] = frozenset()
        self._gen_stage_reads: FrozenSet[str] = frozenset()
        self._gen_splice_chain: Tuple[str, ...] = ()
        self._gen_splice_failures: Dict[str, Exception] = {}

    def install_generation(self, *, reserve: "Iterable[str]" = ()) -> None:
        """Open one generation scope spanning SEVERAL ``reuse_allocator=True``"""
        allocator = self._new_allocator()
        allocator.reserve(*reserve)
        self._gen_allocator = allocator
        self._gen_rendered_producers = {}
        self._gen_split_consumers = []
        self._gen_reuse_deps = {}
        self._gen_dep_stack = []

    @property
    def dialect(self) -> str:
        """The sqlglot dialect name. Read-only — derived from"""
        return self._dialect.sqlglot_name

    def _slot_cast_type(self, slot: ValueSlot) -> Optional[DataType]:
        """The declared CAST target for ``slot`` — the single funnel for every slot cast site. Without native exact decimals (SQLite), preservation is a no-op; the dialect's declared-cast policy then suppresses temporal casts it cannot store (P2)."""
        if slot.preserve_native_type and not self._dialect.exact_decimal_native:
            dt = slot.model_copy(update={"preserve_native_type": False}).cast_type
        else:
            dt = slot.cast_type
        return self._dialect.declared_cast_type(dt)

    def _typed_dimension(self, expr: Expression, *, slot: ValueSlot) -> Expression:
        """Computed dimension ``slot``'s value as grouped, displayed and joined back: cast to its type (a cast
        dimension already is its declared type, and a wide unsigned target must stay wide)."""
        if isinstance(slot.key, CastKey):
            return expr
        return _wrap_cast_for_type(dialect=self._dialect, expr=expr, dt=self._slot_cast_type(slot))

    def _field_cast_type(self, col: DatasetField) -> Optional[DataType]:
        """The CAST target for derived field ``col``; none when it keeps its native type."""
        if keeps_native_type(col=col, dialect=self._dialect):
            return None
        return self._dialect.declared_cast_type(col.datatype)

    def _new_allocator(self) -> AliasAllocator:
        """Build an ``AliasAllocator`` carrying this generator's dialect"""
        allocator = AliasAllocator(folds_case=dialect_folds_case(self.dialect))
        # Reserve the hardcoded base CTE names so a hoisted producer's renamed base never lands on the consumer's _base.
        allocator.reserve("_base", "base")
        return allocator

    def _join_alias(self, *, root: str, path: Tuple[str, ...]) -> str:
        """Mint the internal JOIN alias for cumulative ``path`` under ``root``"""
        alloc = self._gen_allocator
        if alloc is None:
            return root if not path else "__".join(path)
        return alloc.alias_for(
            root=root, path=path, limit=self._dialect.max_identifier_bytes,
        )

    def _join_alias_resolver(self, root: str) -> "Callable[[Tuple[str, ...]], str]":
        """A root-bound alias resolver for ``expand_derived_refs_sync`` so a"""
        return lambda path: self._join_alias(root=root, path=path)

    def _scope_frame(self, *, dataset, relation, bundle, allocator, attached_fields=None):
        """Build a ``ScopeFrame`` rooted at ``dataset`` / ``relation`` on the"""
        return ScopeFrame(
            scope_id=allocator.next_scope_id(relation),
            root_dataset=dataset,
            root_relation=relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=allocator,
            attached_fields=dict(attached_fields or {}),
            placeholder_types=self._placeholder_types(),
            placeholder_elements=self._placeholder_elements(),
        )

    def _current_field_types(self) -> Optional[FieldTypeFn]:
        """The active ``generate()`` call's field types; ``None`` outside one (untyped)."""
        return self._gen_field_types[-1] if self._gen_field_types else None

    def _alias_render_ctx(self, *, slot_id_by_key, available_alias_by_slot_id):
        """RenderContext carrying only the plain slot-alias facilities."""
        return RenderContext(
            dialect=self._dialect,
            field_type=self._current_field_types(),
            aliases=AliasFacilities(
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            ),
        )

    def _outer_wrapper_render_ctx(
        self, *, slot_by_key, cross_dataset_agg_slot_to_cm, aliases_by_slot_id,
    ):
        """RenderContext for the outer-wrapper composite/filter render pass."""
        return RenderContext(
            dialect=self._dialect,
            field_type=self._current_field_types(),
            aliases=self._outer_wrapper_alias_facilities(
                slot_by_key=slot_by_key,
                cross_dataset_agg_slot_to_cm=cross_dataset_agg_slot_to_cm,
                aliases_by_slot_id=aliases_by_slot_id,
            ),
            filters=FilterFacilities(paren_comparison_operands=True),
        )

    @staticmethod
    def _reserve_dataset_field_names(allocator: AliasAllocator, dataset: SlayerDataset) -> None:
        """Reserve every name a ``<relation>.*`` projection of ``dataset`` can"""
        names: List[str] = []
        for c in dataset.fields:
            names.append(c.name)
            if c.is_base and c.physical_name != c.name:
                names.append(c.physical_name)
        allocator.reserve(*names)

    @staticmethod
    def _maybe_quote_ident(ident: Optional[Expression]) -> None:
        """Thin delegator to :func:`slayer.sql.naming.maybe_quote_ident`"""
        maybe_quote_ident(ident)

    @staticmethod
    def _quote_mixed_case_identifiers(node: Expression) -> Expression:
        """Thin delegator to"""
        return quote_mixed_case_identifiers(node)

    def _to_ident(self, name: str) -> exp.Identifier:
        """Build a column/table-name identifier, quoting it when mixed-case"""
        ident = exp.to_identifier(name)
        self._maybe_quote_ident(ident)
        return ident

    def _to_table(self, name: str, alias: Optional[str] = None) -> Expression:
        """A (possibly qualified) table reference read with this dialect's identifier rules."""
        table = parse_table(name=name, dialect=self._dialect)
        if alias is not None:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
        return table

    def _parse(self, sql: str, *, dialect: Optional[str] = None) -> Expression:
        """Parse ``sql`` via sqlglot, applying SLayer-specific AST rewrites."""
        return parse_expression(
            sql=sql, target_dialect=self._dialect, parse_dialect=self._parse_dialect(dialect),
        )

    def _parse_predicate(self, sql: str, *, dialect: Optional[str] = None) -> Expression:
        """Parse a bare WHERE/HAVING predicate expression."""
        return parse_predicate(
            sql=sql, target_dialect=self._dialect, parse_dialect=self._parse_dialect(dialect),
        )

    def _parse_dialect(self, dialect: Optional[str]) -> SqlDialect:
        return self._dialect if dialect in (None, self.dialect) else get_dialect(dialect)




    def _embed_dataset_sql(self, *, sql: str, alias: str) -> Expression:
        """A dataset's own ``sql`` as an aliased subquery, its CTEs renamed through the
        generation allocator so a later hoist never meets another statement's names."""
        statement = self._parse(sql)
        allocator = self._gen_allocator or self._new_allocator()
        limit = self._dialect.max_identifier_bytes
        rename_embedded_ctes(statement, allocate=lambda name: allocator.allocate_cte(
            fit_identifier(name=f"_w_{name}", limit=limit),
        ))
        return exp.Subquery(this=statement, alias=exp.to_identifier(alias))

    def _quote_ident(self, name: str) -> str:
        """Render ``name`` as ONE dialect-quoted identifier string."""
        return exp.to_identifier(name, quoted=True).sql(dialect=self.dialect)

    @staticmethod
    def _carry_aliases_in_plan_order(
        aliases_by_slot_id: Dict[str, List[str]],
    ) -> List[str]:
        """Aliases an inner stage carries forward, in PLAN order (B8)."""
        out: List[str] = []
        owner_of: Dict[str, str] = {}
        for sid, aliases in aliases_by_slot_id.items():
            for alias in aliases:
                owner = owner_of.get(alias)
                if owner == sid:
                    raise ValueError(
                        f"slot {sid!r} renders the alias {alias!r} more than "
                        f"once; an inner stage cannot carry the same output "
                        f"name twice",
                    )
                if owner is not None:
                    raise ValueError(
                        f"slots {owner!r} and {sid!r} both render the alias "
                        f"{alias!r}; an inner stage cannot carry the same "
                        f"output name twice",
                    )
                owner_of[alias] = sid
                out.append(alias)
        return out





    def _date_offset(
        self, expr: Expression, *, count: int, unit: TimeGranularity, operand: Optional[DataType],
    ) -> Expression:
        """``expr`` moved by ``count`` ``unit``s through the dialect's one date-arithmetic primitive."""
        typed = DataType.DATETIME if operand is None or operand not in TEMPORAL_TYPES else operand
        return self._dialect.build_date_add(
            expr=expr, count=exp.Literal.number(count), unit=unit, operand=typed,
        )

    def _calendar_offset_bucket(
        self, *, bucket_expr: Expression, bucket_type: DataType, periods: int, shift_granularity: Granularity,
        bucket_granularity: Granularity,
    ) -> Tuple[Expression, DataType]:
        """The bucket ``periods`` steps of ``shift_granularity`` from ``bucket_expr`` (a ``bucket_type``), and its
        type."""
        base = granularity_parts(shift_granularity)[0]
        shifted = self._dialect.bucket_step(
            bucket=bucket_expr, count=int(periods), unit=shift_granularity, operand=bucket_type,
        )
        shifted_type = date_add_type(bucket_type, base)
        if _shift_preserves_bucket_starts(bucket=bucket_granularity, shift=shift_granularity):
            return shifted, shifted_type
        rebucketed = self._dialect.build_bucket(
            col_expr=shifted, granularity=bucket_granularity, operand=shifted_type,
        )
        return rebucketed, granularity_bucket_type(shifted_type, bucket_granularity)

    def _temporal_operands(
        self, time: Expression, time_type: DataType, bound: Expression, bound_type: DataType,
    ) -> Dict[str, Expression]:
        """A comparison's two temporal operands as the dialect compares them."""
        left, right = temporal_sides((time, time_type, bound, bound_type), dialect=self._dialect)
        return {"this": left, "expression": right}

    def _build_date_trunc(self, *, col_expr: Expression, key: TimeTruncKey) -> Expression:
        """The bucket ``key`` of ``col_expr`` (its rendered source), through the dialect strategy."""
        return render_bucket(key=key, source=col_expr, dialect=self._dialect, field_type=self._field_types())

    def _bucket_type(self, key: TimeTruncKey) -> DataType:
        """The logical type of bucket ``key`` under the active root's column types."""
        return granularity_bucket_type(bucket_operand(key, field_type=self._field_types()), key.granularity)

    def _rewrite_log_aliases(self, node: Expression) -> Expression:
        """Thin delegator to the shared log-alias policy in"""
        return rewrite_log_alias(node, dialect=self._dialect)

    def _resolve_sql(
        self,
        sql: Optional[str],
        name: str,
        dataset_name: str,
        type: Optional[DataType] = None,
    ) -> Expression:
        """Resolve an enriched SQL expression to a sqlglot AST node."""
        if sql is None:
            return exp.Column(this=self._to_ident(name), table=exp.to_identifier(dataset_name))
        if sql.isidentifier():
            return exp.Column(this=self._to_ident(sql), table=exp.to_identifier(dataset_name))
        return _wrap_cast_for_type(
            dialect=self._dialect, expr=self._parse(sql), dt=self._dialect.declared_cast_type(type),
        )

    def _resolve_value_ast(self, spec: AggRenderSpec) -> Expression:
        """The spec's value: its rendered AST, else ``spec.sql`` (or ``spec.name``) resolved."""
        if spec.value is not None:
            return spec.value.copy()
        return self._resolve_sql(
            sql=spec.sql,
            name=spec.name,
            dataset_name=spec.dataset_name,
            type=spec.field_type,
        )

    def _build_agg(
        self,
        spec: "AggRenderSpec | None" = None,
    ) -> tuple[Expression, bool]:
        """Build an aggregation expression from an ``AggRenderSpec`` through the shared dispatch."""
        if spec is None:  # pragma: no cover — defensive
            raise ValueError("_build_agg requires a 'spec'.")
        if not spec.aggregation:
            return self._resolve_value_ast(spec), False
        return render_aggregate(self._aggregate_spec(spec), dialect=self._dialect), True

    def _aggregate_spec(self, spec: AggRenderSpec) -> AggregateSpec:
        """``spec`` fully resolved: its value AST (``None`` for a star) and its parameters' SQL."""
        custom = not is_builtin_agg(spec.aggregation) or bool(spec.expression)
        unvalued = spec.sql is None and spec.value is None
        is_star = unvalued and (not spec.name if custom else spec.aggregation == "count")
        return AggregateSpec(
            aggregation=spec.aggregation,
            value=None if is_star else self._resolve_value_ast(spec),
            # A parameter is read like the aggregated value (a boolean as its integer).
            bindings={
                name: aggregate_input(value=kwarg.value, aggregation=spec.aggregation, input_type=kwarg.input_type)
                for name, kwarg in spec.agg_kwargs.items()
            },
            input_type=spec.input_type,
            element_type=spec.element_type,
            expression=spec.expression,
        )

    def _build_median(self, inner: Expression) -> Expression:
        return self._dialect.build_median(inner=inner)

    def _build_percentile(self, spec: AggRenderSpec) -> Expression:
        return render_aggregate(self._aggregate_spec(spec), dialect=self._dialect)

    def _empty_value_expr(self, empty: EmptyValue) -> Optional[Expression]:
        """An aggregate's empty value as SQL (``None``: NULL)."""
        if isinstance(empty, EmptyList):
            return self._dialect.empty_list()
        return None if empty is None else exp.Literal.number(empty)

    def _pick_once(self, value: Expression, *, input_type: Optional[DataType]) -> Expression:
        """An association producer's per-entity pick of a constant ``value`` (MAX)."""
        return apply_aggregate(
            entry=resolve_agg_entry("max"), value=value, input_type=input_type, dialect=self._dialect,
        )

    def _expression_template(self, *, agg_name: str, expression: str) -> SqlTemplate:
        try:
            return sql_template(text=expression, dialect=self.dialect)
        except SqlTemplateError as e:
            raise SqlTemplateError(f"Aggregation '{agg_name}': {e}") from e

    def generate_from_planned(self, planned_query, *, bundle) -> str:
        """Render a typed ``PlannedQuery`` to finished SQL (public entry)."""
        return _finish_statement(
            self._build_from_planned(planned_query, bundle=bundle),
            dialect=self._dialect,
            exempt=_user_authored_exemptions(bundle=bundle, dialect=self._dialect),
        )

    def _build_from_planned(
        self, planned_query, *, bundle, as_cte_body: bool = False,
        reuse_allocator: bool = False, producer_kernel=None,
    ) -> exp.Select:
        """Compose a typed ``PlannedQuery`` as AST, in a fresh generation scope unless ``reuse_allocator``."""
        self._assert_projection_is_public(planned_query)
        if reuse_allocator and self._gen_allocator is not None:
            return self._generate_from_planned_impl(
                planned_query, bundle=bundle, as_cte_body=as_cte_body,
                producer_kernel=producer_kernel,
            )
        else:
            prev_allocator = getattr(self, "_gen_allocator", None)
            prev_rendered = getattr(self, "_gen_rendered_producers", None)
            prev_split_consumers = self._gen_split_consumers
            prev_reuse_deps = self._gen_reuse_deps
            prev_dep_stack = self._gen_dep_stack
            self._gen_allocator = self._new_allocator()
            self._gen_rendered_producers = {}
            self._gen_split_consumers = []
            self._gen_reuse_deps = {}
            self._gen_dep_stack = []
            try:
                return self._generate_from_planned_impl(
                    planned_query, bundle=bundle, as_cte_body=as_cte_body,
                    producer_kernel=producer_kernel,
                )
            finally:
                self._gen_allocator = prev_allocator
                self._gen_rendered_producers = prev_rendered
                self._gen_split_consumers = prev_split_consumers
                self._gen_reuse_deps = prev_reuse_deps
                self._gen_dep_stack = prev_dep_stack

    @staticmethod
    def _assert_projection_is_public(planned_query) -> None:
        """The renderer-side belt for the public-projection invariant (§5.2)."""
        slots = {
            slot.id: slot
            for slot in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }
        for sid in planned_query.projection:
            slot = slots.get(sid)
            if slot is not None and slot.hidden:
                raise ValueError(
                    f"hidden slot {sid!r} reached the public projection; the "
                    f"plan's projection must contain only public slots "
                    f"(a model_copy that skips validation is the usual cause)",
                )

    def _generate_from_planned_impl(
        self, planned_query, *, bundle: ResolvedSourceBundle, as_cte_body: bool = False, producer_kernel=None,
    ) -> exp.Select:
        """Compose one planned query with its root's field types in reach of alias-mode renders."""
        source_dataset = bundle.population
        if source_dataset is None:
            raise ValueError(
                "generate_from_planned requires bundle.source_model to be set",
            )
        dataset_types = dataset_field_type(dataset=source_dataset, datasets_by_name=bundle.datasets_by_name)
        placeholders = {
            sub.placeholder: dt
            for attach in planned_query.regroup_attach_plans
            for sub in attach.substitutions
            if (dt := temporal_type(sub.original_key, field_type=dataset_types) or (
                DataType.ARRAY if list_valued(sub.original_key, field_type=dataset_types) else None
            )) is not None
        }
        dataset_elements = dataset_field_element(dataset=source_dataset, datasets_by_name=bundle.datasets_by_name)
        elements = {
            sub.placeholder: element
            for attach in planned_query.regroup_attach_plans
            for sub in attach.substitutions
            if (element := list_element_type(sub.original_key, field_type=dataset_types,
                                             field_element=dataset_elements)) is not None
        }
        self._gen_placeholder_types.append((placeholders, elements))
        self._gen_field_types.append(lambda key: placeholders.get(key) or dataset_types(key))
        # The outermost spine render declares every sequence CTE rendered within it (nested producers included).
        owns_spine = planned_query.spine is not None and not self._gen_spines
        outer_spine_ctes = self._gen_spine_ctes
        if planned_query.spine is not None:
            self._gen_spines.append(planned_query.spine)
        if owns_spine:
            self._gen_spine_ctes = []
        try:
            out = self._generate_from_planned_body(
                planned_query, bundle=bundle, as_cte_body=as_cte_body,
                producer_kernel=producer_kernel,
            )
            if owns_spine and self._gen_spine_ctes:
                raise ValueError("a spine sequence CTE was rendered but never declared")
            return out
        finally:
            self._gen_field_types.pop()
            self._gen_placeholder_types.pop()
            if planned_query.spine is not None:
                self._gen_spines.pop()
            if owns_spine:
                self._gen_spine_ctes = outer_spine_ctes

    def _placeholder_types(self) -> Dict[Any, DataType]:
        return dict(self._gen_placeholder_types[-1][0]) if self._gen_placeholder_types else {}

    def _placeholder_elements(self) -> Dict[Any, DataType]:
        return dict(self._gen_placeholder_types[-1][1]) if self._gen_placeholder_types else {}

    def _generate_from_planned_body(  # NOSONAR(S3776) — top-level dispatch over cross-dataset / transform-chain / plain branches plus the conditional outer-trim wrap. Each branch is a coherent compilation strategy; extracting would scatter the shared planned_query / slots_by_id / aliases_by_slot_id state across helpers without simplifying anything.
        self,
        planned_query,
        *,
        bundle: ResolvedSourceBundle,
        as_cte_body: bool = False,
        producer_kernel=None,
    ) -> exp.Select:
        """Compose a typed ``PlannedQuery`` as one statement AST."""

        source_dataset = bundle.population
        source_relation = planned_query.source_relation
        spine_mark = len(self._gen_spine_ctes)

        _row_attaches = [
            r for r in planned_query.regroup_attach_plans
            if r.attach_phase == "row"
        ]
        _combined_attaches = [
            r for r in planned_query.regroup_attach_plans
            if r.attach_phase == "combined"
        ]
        # One hoisted gate: above the kernel-body / combined-attaches early
        # returns so every render path raises the same shape error.
        self._validate_transform_input_shapes(planned_query=planned_query)
        # Belts against a model_copy that bypassed the plan-time validators.
        self._assert_stages_assigned(planned_query=planned_query)
        validate_grain_determination(planned_query)

        if (
            as_cte_body
            and producer_kernel is not None
            and producer_kernel.kind != "plain"
        ):
            return self._render_kernel_producer_body(
                planned_query=planned_query, bundle=bundle,
                kernel=producer_kernel,
            )

        if _combined_attaches or planned_query.row_supply is not None:
            return self._render_with_combined_attaches(
                planned_query=planned_query, bundle=bundle, spine_mark=spine_mark,
            )

        slots_by_id = {
            s.id: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }

        # _base projects every BASE ∧ needs_field value in plan order;
        # this path carries no combined attaches, so nothing is isolated.
        base_render_order = staged_plan.base_render_order(planned_query)

        regroup_ctes, regroup_env, regroup_join_specs, reused_row_ctes = (
            self._prepare_regroup_attaches(planned_query=planned_query, bundle=bundle)
            if _row_attaches
            else ([], {}, [], [])
        )

        (
            base_select,
            aliases_by_slot_id,
            has_aggregation,
            group_by_keys,
            dimension_values,
        ) = self._build_base_select_for_planned(
            planned_query=planned_query,
            bundle=bundle,
            source_dataset=source_dataset,
            source_relation=source_relation,
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            regroup_env=regroup_env,
            regroup_join_specs=regroup_join_specs,
        )

        where_clause, having_clause = self._build_where_having_from_planned(
            planned_query=planned_query,
            source_relation=source_relation,
            source_dataset=source_dataset,
            bundle=bundle,
            aliases_by_slot_id=aliases_by_slot_id,
            regroup_env=regroup_env,
            dimension_values=dimension_values,
        )

        if where_clause is not None:
            base_select = base_select.where(where_clause)
        for cond in self._semi_join_exists_conditions(
            planned_query=planned_query, source_dataset=source_dataset,
            source_relation=source_relation, bundle=bundle,
        ):
            base_select = base_select.where(cond)

        # dim-only dedup emits GROUP BY before LIMIT so unique dim tuples aren't dropped past row N;
        # raw-row mode opts out.
        dim_only_dedup = (
            planned_query.grain is not None
            and bool(group_by_keys)
            and not has_aggregation
        )
        needs_group_by = has_aggregation or dim_only_dedup
        if needs_group_by and group_by_keys:
            for gb in group_by_keys.values():
                base_select = base_select.group_by(gb)

        if having_clause is not None:
            base_select = base_select.having(having_clause)
        prelude = [*self._take_spine_ctes(since=spine_mark), *regroup_ctes]

        # With no blocker the pipeline collapses to one SELECT; the only blocker here is materialised hidden
        # order/filter slots, forcing an outer trim wrap.
        if not planned_query.transform_layers:
            public_slot_ids = set(planned_query.projection)
            blockers = fusion_blockers(
                has_combined_phase=False,
                has_transform_steps=False,
                trims_hidden_fields=any(
                    sid not in public_slot_ids for sid in base_render_order
                ),
            )
            if self._force_unfused:
                blockers.append("forced unfused (test seam)")
            if blockers:
                final_select = self._build_outer_trim_wrap_select(
                    base_select=base_select,
                    planned_query=planned_query,
                    source_relation=source_relation,
                    aliases_by_slot_id=aliases_by_slot_id,
                    slots_by_id=slots_by_id,
                    bundle=bundle,
                )
            else:
                final_select = self._apply_planned_order_limit(
                    select=base_select,
                    planned_query=planned_query,
                    source_relation=source_relation,
                    slots_by_id=slots_by_id,
                    source_dataset=source_dataset,
                    bundle=bundle,
                    aliases_by_slot_id=aliases_by_slot_id,
                )
            if prelude:
                final_select = self._assemble_with_chain(
                    entries=prelude, final=final_select,
                    external_names=self._external_cte_names(),
                )
            return final_select

        # Chain bodies stay exp.Select end-to-end: render-to-text-and-reparse would mis-split the dotted
        # <relation>.<alias> names on dot-path dialects.
        return self._render_steps_and_post(
            prelude_nodes=prelude,
            tail_select=base_select,
            tail_schema=aliases_by_slot_id,
            tail_phase="base",
            planned_query=planned_query,
            bundle=bundle,
            source_dataset=source_dataset,
            source_relation=source_relation,
            slots_by_id=slots_by_id,
            regroup_env=regroup_env,
            regroup_join_specs=regroup_join_specs,
            reserve_bare_aliases=True,
            reused_names=reused_row_ctes,
        )


    def _run_transform_chain(
        self,
        *,
        chain: ChainState,
        render: RenderState,
        chain_tail: str,
    ) -> str:
        """The one driver for the transform-step phase: level-ascending batches,
        then the fused trailing derived-composite step (D8)."""
        planned_query = render.planned_query
        # Batches by planner-assigned derived level, ascending (D8): within a
        # level, window batch, then time_shift, then cp, in transform_layers
        # order — the exact sequence the retired Kahn readiness rounds produced.
        levels = sorted({
            chain.slots_by_id[sid].stage.level
            for layer in planned_query.transform_layers
            for sid in layer.slot_ids
        })
        step_num = 0
        for level in levels:
            ready_window, ready_time_shift, ready_cp = _layer_batches_at_level(
                planned_query, slots_by_id=chain.slots_by_id, level=level,
            )
            if ready_window:
                chain_tail, step_num = self._emit_window_batch_step(
                    ready_window=ready_window,
                    ctes=chain.ctes,
                    chain_tail=chain_tail,
                    cte_allocator=chain.cte_allocator,
                    step_num=step_num,
                    slots_by_id=chain.slots_by_id,
                    slot_id_by_key=chain.slot_id_by_key,
                    available_alias_by_slot_id=chain.available_alias_by_slot_id,
                    aliases_by_slot_id=chain.aliases_by_slot_id,
                    source_relation=chain.source_relation,
                    planned_query=planned_query,
                )
            chain_tail = self._emit_time_shift_layers(
                ready_time_shift=ready_time_shift,
                chain=chain,
                render=render,
                chain_tail=chain_tail,
            )
            chain_tail = self._emit_cp_layers(
                ready_cp=ready_cp,
                chain=chain,
                render=render,
                chain_tail=chain_tail,
            )

        chain_tail, step_num = self._emit_unmaterialised_post_phase_step(
            ctes=chain.ctes,
            chain_tail=chain_tail,
            cte_allocator=chain.cte_allocator,
            step_num=step_num,
            slot_id_by_key=chain.slot_id_by_key,
            available_alias_by_slot_id=chain.available_alias_by_slot_id,
            aliases_by_slot_id=chain.aliases_by_slot_id,
            source_relation=chain.source_relation,
            planned_query=planned_query,
        )
        return chain_tail

    def _render_steps_and_post(
        self,
        *,
        prelude_nodes: List["CteEntry"],
        tail_select: exp.Select,
        tail_schema: Dict[str, List[str]],
        tail_phase: NodePhase,
        planned_query,
        bundle,
        source_dataset,
        source_relation: str,
        slots_by_id: Dict[str, Any],
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str, Optional[DataType]]]]]] = None,
        reserve_bare_aliases: bool = False,
        reused_names: Sequence[str] = (),
    ) -> exp.Select:
        """Steps + post phases over a built relation tail (D1) — shared by the"""
        # The base relation joins the prelude producers AND any reused producer its
        # tail_select reads (a dual-role producer shared with the combined/row phase);
        # both are declared so the split re-keys the edge onto the hoisted base (P6).
        base_node = Node(
            name="base",
            phase=tail_phase,
            query=tail_select,
            depends_on=[*[e.name for e in prelude_nodes], *reused_names],
            schema_by_slot={sid: list(a) for sid, a in tail_schema.items()},
        )
        ctes: List[CteEntry] = [*prelude_nodes, base_node]
        cte_allocator = self._gen_allocator or self._new_allocator()
        cte_allocator.reserve(*(entry.name for entry in ctes))
        if reserve_bare_aliases:
            _alias_prefix = f"{source_relation}."
            cte_allocator.reserve(*(
                a[len(_alias_prefix):] if a.startswith(_alias_prefix) else a
                for aliases in base_node.schema_by_slot.values()
                for a in aliases
            ))
        aliases_by_slot_id: Dict[str, List[str]] = {
            sid: list(a) for sid, a in base_node.schema_by_slot.items()
        }
        slot_id_by_key: Dict[Any, str] = {
            s.key: s.id for s in slots_by_id.values()
        }
        available_alias_by_slot_id: Dict[str, str] = {
            sid: aliases[0]
            for sid, aliases in aliases_by_slot_id.items()
            if aliases
        }
        chain_state = ChainState(
            ctes=ctes,
            cte_allocator=cte_allocator,
            slots_by_id=slots_by_id,
            slot_id_by_key=slot_id_by_key,
            available_alias_by_slot_id=available_alias_by_slot_id,
            aliases_by_slot_id=aliases_by_slot_id,
            source_dataset=source_dataset,
            source_relation=source_relation,
        )
        render_state = RenderState(
            planned_query=planned_query, bundle=bundle,
            regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
        )
        chain_tail = self._run_transform_chain(
            chain=chain_state,
            render=render_state,
            chain_tail=base_node.name,
        )
        return self._finalize_planned_transform_chain(
            ctes=ctes,
            chain_tail=chain_tail,
            slots_by_id=slots_by_id,
            slot_id_by_key=slot_id_by_key,
            available_alias_by_slot_id=available_alias_by_slot_id,
            aliases_by_slot_id=aliases_by_slot_id,
            planned_query=planned_query,
        )

    def _emit_step_cte(
        self,
        *,
        ctes: List["CteEntry"],
        chain_tail: str,
        step_num: int,
        cte_allocator,
        aliases_by_slot_id: Dict[str, List[str]],
        available_alias_by_slot_id: Dict[str, str],
        source_relation: str,
        slot_entries: Iterable[Tuple[str, ValueSlot]],
        render: Callable[[ValueSlot], Expression],
    ) -> Tuple[str, int]:
        """Emit one transform-chain step CTE and advance the chain."""
        step_num += 1
        step_name = cte_allocator.allocate_cte(f"step{step_num}")
        prev_cte = chain_tail
        carry_aliases = self._carry_aliases_in_plan_order(aliases_by_slot_id)
        step_parts: List[Expr] = [exp.column(a, quoted=True) for a in carry_aliases]
        # Siblings render against prev_cte only: a step's own aliases publish after its SELECT.
        published: List[Tuple[str, str]] = []
        for map_key, slot in slot_entries:
            names = list(slot.public_aliases) or [slot.declared_name]
            rendered = render(slot)
            if slot.type is not None:
                rendered = _wrap_cast_for_type(dialect=self._dialect, expr=rendered, dt=self._slot_cast_type(slot))
            # One column per declared name (C13); as_ copies its child, so the rendered node is safely reused.
            for alias in names:
                full_alias = (result_key_from_alias(source_relation=source_relation, alias=alias) if slot.public_aliases
                              else internal_result_key(source_relation=source_relation, name=alias))
                step_parts.append(rendered.as_(full_alias, quoted=True))
                published.append((map_key, full_alias))
        for map_key, full_alias in published:
            aliases_by_slot_id.setdefault(map_key, []).append(full_alias)
            available_alias_by_slot_id.setdefault(map_key, full_alias)
        ctes.append(CteEntry(
            name=step_name,
            query=exp.Select().select(*step_parts).from_(prev_cte),
            depends_on=[prev_cte],
        ))
        return step_name, step_num

    @staticmethod
    def _unmaterialised_post_slots(
        planned_query: PlannedQuery, aliases_by_slot_id: Dict[str, List[str]],
    ) -> List[ValueSlot]:
        """DERIVED composite slots needing a column, not yet materialised —
        every level fused into the one trailing step (D8, sql P11). A mask-only
        value has ``needs_field=False`` and renders as a predicate."""
        return [
            cslot
            for cslot in planned_query.combined_expression_slots
            if isinstance(cslot.key, SLOT_COMPOSITE_KINDS)
            and cslot.id not in aliases_by_slot_id
            and cslot.needs_field
            and cslot.stage is not None
            and cslot.stage.kind is StageKind.DERIVED
        ]

    def _emit_window_batch_step(
        self,
        *,
        ready_window,
        ctes,
        chain_tail,
        cte_allocator,
        step_num,
        slots_by_id,
        slot_id_by_key,
        available_alias_by_slot_id,
        aliases_by_slot_id,
        source_relation,
        planned_query,
    ) -> tuple:
        """One ``step<n>`` CTE for a Kahn batch of window layers, carrying every"""
        window_entries = [
            (slot_id, slots_by_id[slot_id])
            for layer in ready_window
            for slot_id in layer.slot_ids
        ]
        return self._emit_step_cte(
            ctes=ctes,
            chain_tail=chain_tail,
            step_num=step_num,
            cte_allocator=cte_allocator,
            aliases_by_slot_id=aliases_by_slot_id,
            available_alias_by_slot_id=available_alias_by_slot_id,
            source_relation=source_relation,
            slot_entries=window_entries,
            render=lambda slot: self._render_window_transform_sql(
                slot=slot,
                slots_by_id=slots_by_id,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
                planned_query=planned_query,
            ),
        )

    def _emit_time_shift_layers(
        self,
        *,
        ready_time_shift,
        chain: ChainState,
        render: RenderState,
        chain_tail,
    ):
        """Emit the ``shifted_`` + ``sjoin_`` CTE pair for each ready"""
        for layer in ready_time_shift:
            for slot_id in layer.slot_ids:
                slot = chain.slots_by_id[slot_id]
                chain_tail = self._emit_time_shift_ctes_for_planned(
                    slot=slot,
                    chain=chain,
                    render=render,
                    chain_tail=chain_tail,
                )
        return chain_tail

    def _emit_cp_layers(
        self,
        *,
        ready_cp,
        chain: ChainState,
        render: RenderState,
        chain_tail,
    ):
        """Emit the ``cp_reset_`` + ``cp_value_`` CTE pair for each ready"""
        for layer in ready_cp:
            for slot_id in layer.slot_ids:
                slot = chain.slots_by_id[slot_id]
                chain_tail = self._emit_consecutive_periods_ctes_for_planned(
                    slot=slot,
                    chain=chain,
                    render=render,
                    chain_tail=chain_tail,
                )
        return chain_tail

    def _emit_unmaterialised_post_phase_step(
        self,
        *,
        ctes,
        chain_tail,
        cte_allocator,
        step_num,
        slot_id_by_key,
        available_alias_by_slot_id,
        aliases_by_slot_id,
        source_relation,
        planned_query,
    ) -> tuple:
        """Materialise projected POST-phase composite (``SLOT_COMPOSITE_KINDS``) slots"""
        unmaterialised = self._unmaterialised_post_slots(
            planned_query, aliases_by_slot_id,
        )
        if not unmaterialised:
            return chain_tail, step_num
        return self._emit_step_cte(
            ctes=ctes,
            chain_tail=chain_tail,
            step_num=step_num,
            cte_allocator=cte_allocator,
            aliases_by_slot_id=aliases_by_slot_id,
            available_alias_by_slot_id=available_alias_by_slot_id,
            source_relation=source_relation,
            slot_entries=[(cslot.id, cslot) for cslot in unmaterialised],
            render=lambda cslot: render_value_key(
                key=cslot.key,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                ),
            ),
        )

    def _finalize_planned_transform_chain(
        self,
        *,
        ctes,
        chain_tail,
        slots_by_id,
        slot_id_by_key,
        available_alias_by_slot_id,
        aliases_by_slot_id,
        planned_query,
    ) -> exp.Select:
        """The ``WITH`` chain whose final select (POST filters as its ``WHERE``) sits under the public outer wrap."""
        projected = self._carry_aliases_in_plan_order(aliases_by_slot_id)
        final_select = exp.Select().select(
            *(exp.column(a, quoted=True) for a in projected),
        ).from_(chain_tail)
        post_where = _conjunction(self._render_post_phase_filter_conditions(
            planned_query=planned_query,
            slot_id_by_key=slot_id_by_key,
            available_alias_by_slot_id=available_alias_by_slot_id,
        ))
        if post_where is not None:
            final_select = final_select.where(post_where)
        chain = self._assemble_with_chain(
            entries=ctes, final=final_select,
            external_names=self._external_cte_names(),
        )
        return self._public_outer_wrap(
            inner=chain,
            public_aliases=_cycle_public_aliases_in_projection_order(
                planned_query=planned_query,
                slots_by_id=slots_by_id,
                aliases_by_slot_id=aliases_by_slot_id,
            ),
            order_terms=self._planned_order_terms(
                planned_query=planned_query,
                slots_by_id=slots_by_id,
                available_alias_by_slot_id=available_alias_by_slot_id,
            ),
            planned_query=planned_query,
        )


    @staticmethod
    def _assert_stages_assigned(*, planned_query) -> None:
        """Refuse a plan any of whose values is unstaged; the
        plan-time validator already enforces this, but ``model_copy`` bypasses it."""
        for slot in (
            *planned_query.row_slots,
            *planned_query.aggregate_slots,
            *planned_query.combined_expression_slots,
        ):
            if slot.stage is None:
                raise MaterialisationStageError(
                    f"value {slot.id!r} reached SQL generation unstaged; the "
                    f"generator refuses a plan the staging pass has not run over.",
                )

    @staticmethod
    def _validate_transform_input_shapes(*, planned_query) -> None:
        """Reject unsupported ``consecutive_periods`` input shapes before any
        render path branches (``time_shift`` inputs are checker-typed at plan
        time — engine P9)."""
        slots_map = {
            s.id: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }
        for layer in planned_query.transform_layers:
            if layer.op != "consecutive_periods":
                continue
            for sid in layer.slot_ids:
                slot = slots_map.get(sid)
                if slot is None or not isinstance(slot.key, TransformKey):
                    continue
                _validate_consecutive_periods_input(
                    op=layer.op, inner=slot.key.input,
                )

    def _resolve_agg_inputs_via_scope(  # NOSONAR(S3776) — one cohesive Law-1 discovery pass: three ordered sub-passes (DatasetField.filter → source → kwargs) over the local aggregates via small closures sharing scope/resolved. Extracting them would scatter the ordered-registration contract that keeps the base FROM byte-identical.
        self, *, base_render_order, slots_by_id, scope: ScopeFrame,
        skip_cross_dataset_aggs: bool = False,
    ) -> "Dict[Any, Dict[str, ResolvedAggKwarg]]":
        """Resolve every LOCAL aggregate's relationship-crossing inputs through the host"""

        resolved: "Dict[Any, Dict[str, ResolvedAggKwarg]]" = {}

        def _walk(key, fn) -> None:
            if isinstance(key, AggregateKey):
                # A host-grain aggregate renders inline and must register its source relationship path (Law 1) — but
                # not when a _cm_ CTE owns it (would add an unused, cardinality-changing join).
                if not source_anchor_path(key.source) or (
                    _is_host_grain(key) and not skip_cross_dataset_aggs
                ):
                    fn(key)
            elif isinstance(key, ArithmeticKey):
                for o in key.operands:
                    _walk(o, fn)
            elif isinstance(key, ScalarCallKey):
                for a in key.args:
                    _walk(a, fn)
            elif isinstance(key, (CastKey, ListSetKey)):
                _walk(key.source, fn)

        def _for_each_local_agg(fn) -> None:
            for sid in base_render_order:
                slot = slots_by_id.get(sid)
                if slot is not None and slot.phase == Phase.AGGREGATE:
                    _walk(slot.key, fn)

        def _resolve_source(key) -> None:
            # Expression sources resolve too: a FieldSqlKey operand
            # whose derived SQL crosses relationships must register them (Law 1).
            if isinstance(
                key.source, (FieldSqlKey, *_EXPRESSION_SOURCE_KINDS),
            ) or source_anchor_path(key.source):
                assert not isinstance(key.source, TimePointCmpKey)  # lowered by the checker before planning
                scope.resolve(key.source)

        def _resolve_kwargs(key) -> None:
            kw = self._resolve_agg_kwargs_for_key(key=key, scope=scope)
            if kw:
                resolved[key] = kw

        def _resolve_first_last_time_arg(key) -> None:
            arg = self._explicit_time_arg_of(key)
            if arg is None:
                return
            # A path-bearing derived time arg is a hop past the target; skip it — anchoring against source_relation
            # would register a bogus relationship path.
            if isinstance(arg, FieldSqlKey) and arg.path:
                return
            scope.resolve(arg)

        def _resolve_filter(key) -> None:
            if key.filter is not None:
                scope.resolve(key.filter)

        _for_each_local_agg(_resolve_source)
        _for_each_local_agg(_resolve_kwargs)
        _for_each_local_agg(_resolve_first_last_time_arg)
        _for_each_local_agg(_resolve_filter)
        return resolved

    def _throwaway_frame(
        self, *, dataset, relation: str, bundle, attached_fields=None,
    ) -> ScopeFrame:
        """A target-rooted ``ScopeFrame`` built purely to reproduce an anchored"""
        allocator = self._new_allocator()
        return ScopeFrame(
            scope_id=allocator.next_scope_id(relation),
            root_dataset=dataset,
            root_relation=relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=allocator,
            attached_fields=dict(attached_fields or {}),
            placeholder_types=self._placeholder_types(),
            placeholder_elements=self._placeholder_elements(),
        )

    def _render_computed_dims_via_scope(
        self, *, base_render_order, slots_by_id, scope,
    ) -> Dict[str, Expression]:
        """Render ROW-phase computed (expression) dimensions through the HOST"""

        out: Dict[str, Expression] = {}
        for sid in base_render_order:
            slot = slots_by_id[sid]
            if (
                slot.phase == Phase.ROW
                and slot.is_dimension
                and isinstance(slot.key, SLOT_COMPOSITE_KINDS)
            ):
                out[sid] = render_value_key(
                    key=slot.key,
                    ctx=RenderContext(scope=scope, dialect=self._dialect),
                )
        return out

    def _resolve_regroup_attach_conditions(
        self, *, regroup_join_specs, scope,
    ) -> List[Tuple[str, Optional[Expression]]]:
        """One ``(cte_name, condition)`` per regroup producer. Each host"""
        out: List[Tuple[str, Optional[Expression]]] = []
        for cte_name, pairs in (regroup_join_specs or []):
            operands = self._keyed_pairs([
                (
                    render_value_key(
                        key=host_key,
                        ctx=RenderContext(scope=scope, dialect=self._dialect),
                    ),
                    grain_alias_field(alias=producer_alias, table=cte_name),
                    host_key,
                )
                for host_key, producer_alias, _ in pairs
            ], right_types=[producer_type for _, _, producer_type in pairs])
            out.append((
                cte_name,
                build_grain_joinback_condition(pairs=operands, dialect=self._dialect),
            ))
        return out

    def _resolve_agg_kwargs_for_key(
        self, *, key, scope: ScopeFrame,
    ) -> "Optional[Dict[str, ResolvedAggKwarg]]":
        """Resolve a LOCAL aggregate's row-valued parameters through ``scope``, registering
        the relationship paths they cross (a row-attached placeholder resolves to its producer column)."""
        resolved = {
            kname: ResolvedAggKwarg(
                value=scope.resolve(kval),
                input_type=aggregation_source_type(kval, field_type=scope.field_type),
            )
            for kname, kval in getattr(key, "kwargs", ())
            if _is_row_param(kval)
        }
        return resolved or None

    def _build_base_select_for_planned(  # NOSONAR(S3776) — relationship-path collection and derived-dim expansion are extracted to helpers; the residual is the one cohesive per-slot ROW/AGGREGATE projection + GROUP-BY assembly pass.
        self,
        *,
        planned_query,
        bundle,
        source_dataset,
        source_relation: str,
        base_render_order: List[str],
        slots_by_id: Dict[str, Any],
        skip_cross_dataset_aggs: bool = False,
        skip_filter_ids: Optional[Set[str]] = None,
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str, Optional[DataType]]]]]] = None,
    ):
        """Build the base SELECT (sqlglot ``Select``) for ``generate_from_planned``."""

        # Every relationship-crossing ref registers its path into host_scope.relationship_paths as it resolves (Law 1);
        # first-seen order keeps the FROM byte-identical to the legacy collectors'.
        needed_relationship_paths = self._collect_joined_paths_for_base(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            order_slot_ids=[e.slot_id for e in planned_query.order],
        )
        host_allocator = self._gen_allocator or self._new_allocator()
        host_scope = self._scope_frame(
            dataset=source_dataset, relation=source_relation,
            bundle=bundle, allocator=host_allocator,
            attached_fields=regroup_env,
        )
        derived_expr_by_sid = self._expand_derived_row_dims(
            base_render_order=base_render_order, slots_by_id=slots_by_id,
            source_relation=source_relation, source_dataset=source_dataset,
            bundle=bundle, scope=host_scope,
            order_slot_ids=[e.slot_id for e in planned_query.order],
        )
        # Pre-render computed dimensions through the host scope so a relationship their expression crosses registers
        # before the FROM is built.
        computed_dim_expr_by_sid = self._render_computed_dims_via_scope(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            scope=host_scope,
        )
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=host_scope,
        )
        # WHERE filters register their relationship paths too; skip those routed to a _cm_ CTE (registering here would
        # add an unused, cardinality-changing LEFT JOIN).
        self._resolve_where_filter_relationships_via_scope(
            planned_query=planned_query, scope=host_scope,
            skip_filter_ids=skip_filter_ids,
        )
        resolved_agg_kwargs = self._resolve_agg_inputs_via_scope(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            scope=host_scope,
            skip_cross_dataset_aggs=skip_cross_dataset_aggs,
        )
        for p in host_scope.relationship_paths.as_list():
            if p not in needed_relationship_paths:
                needed_relationship_paths.append(p)
        from_clause, base_joins = self._build_from_and_joins(
            source_dataset=source_dataset,
            source_relation=source_relation,
            joined_paths=needed_relationship_paths,
            bundle=bundle,
        )

        select_columns: list[Expr] = []
        group_by_keys: Dict[str, Expression] = {}
        # A grouped dimension's value where it differs from its GROUP BY key.
        display_by_sid: Dict[str, Expression] = {}
        has_aggregation = False
        alias_index: Dict[str, int] = {}
        aliases_by_slot_id: Dict[str, List[str]] = {}
        # Non-dimension composites render once every GROUP BY expression is known.
        deferred_composites: List[Tuple[int, Any, str]] = []

        def _record_alias(sid: str, full_alias: str) -> None:
            aliases_by_slot_id.setdefault(sid, []).append(full_alias)

        def _defer_composite(sid: str, slot, full_alias: str) -> None:
            deferred_composites.append((len(select_columns), slot, full_alias))
            select_columns.append(exp.null())
            _record_alias(sid, full_alias)

        for sid in base_render_order:
            slot = slots_by_id[sid]
            # Joined ROW slots project the full dotted result key, not the planner's flat declared_name, per the
            # result-key contract.
            full_alias = self._full_alias_for_slot(
                slot=slot,
                source_relation=source_relation,
                alias_index=alias_index,
            )

            if slot.phase == Phase.ROW:
                key = slot.key
                if isinstance(key, FieldKey):
                    attached = regroup_env.get(key) if regroup_env else None
                    if attached is not None:
                        col_expr = attached.copy()
                    else:
                        col_expr = self._joined_or_local_dim_expr(
                            path=key.path,
                            leaf=key.leaf,
                            source_dataset=source_dataset,
                            source_relation=source_relation,
                            bundle=bundle,
                        )
                    select_columns.append(col_expr.copy().as_(full_alias, quoted=True))
                    group_by_keys.setdefault(sid, col_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, TimeTruncKey):
                    col_expr = derived_expr_by_sid.get(sid) or self._raw_time_col_expr_for_planned(
                        time_field=key.source,
                        source_dataset=source_dataset,
                        source_relation=source_relation,
                        bundle=bundle,
                    )
                    trunc_expr = self._build_date_trunc(col_expr=col_expr, key=key)
                    select_columns.append(trunc_expr.copy().as_(full_alias, quoted=True))
                    group_by_keys.setdefault(sid, trunc_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, FieldSqlKey):
                    col_expr = derived_expr_by_sid.get(sid)
                    if col_expr is None:
                        col_expr = self._dim_field_expr_from_planned(
                            source_dataset=source_dataset,
                            source_relation=source_relation,
                            leaf=key.field_name,
                        )
                    select_columns.append(col_expr.copy().as_(full_alias, quoted=True))
                    group_by_keys.setdefault(sid, col_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, SLOT_COMPOSITE_KINDS) and slot.is_dimension:
                    # Grouped as displayed: MySQL refuses a SELECT CAST(e) grouped by e.
                    dim_expr = self._typed_dimension(computed_dim_expr_by_sid[sid], slot=slot)
                    select_columns.append(self._grouped_display(dim_expr, key=key).as_(full_alias, quoted=True))
                    group_by_keys.setdefault(sid, self._grain_key(dim_expr, key=key))
                    display_by_sid[sid] = dim_expr
                    _record_alias(sid, full_alias)
                elif isinstance(key, LiteralKey) and slot.is_dimension:
                    # A constant needs no GROUP BY entry (a constant one is refused or read as a position).
                    shown = _wrap_cast_for_type(
                        dialect=self._dialect, expr=_literal(key.value, dialect=self._dialect),
                        dt=self._slot_cast_type(slot),
                    )
                    select_columns.append(shown.as_(full_alias, quoted=True))
                    _record_alias(sid, full_alias)
                elif isinstance(key, SLOT_COMPOSITE_KINDS) or (
                    isinstance(key, LiteralKey) and not slot.is_dimension
                ):
                    _defer_composite(sid, slot, full_alias)
                else:
                    raise NotImplementedError(
                        f"row-phase key type "
                        f"{type(key).__name__} not supported in the "
                        f"local-only / time-dim slice."
                    )

            elif slot.phase == Phase.AGGREGATE:
                key = slot.key
                if not isinstance(key, AggregateKey):
                    has_aggregation = has_aggregation or contains_aggregate(key)
                    _defer_composite(sid, slot, full_alias)
                    continue
                agg_path = source_anchor_path(key.source)
                if agg_path:
                    if skip_cross_dataset_aggs:
                        continue
                    if not _is_host_grain(key):
                        raise NotImplementedError(
                            f"cross-dataset aggregate (source.path={agg_path!r}) "
                            f"reached the local base SELECT path; the regroup "
                            f"desugar should have isolated it into a producer "
                            f"CTE."
                        )
                synth = self._build_agg_render_spec_from_planned(
                    slot=slot,
                    key=key,
                    source_dataset=source_dataset,
                    source_relation=source_relation,
                    full_alias=full_alias,
                    bundle=bundle,
                    resolved_agg_kwargs=resolved_agg_kwargs.get(key),
                    scope=host_scope,
                )
                agg_expr, is_agg = self._build_agg(synth)
                if is_agg:
                    agg_expr = _wrap_cast_for_type(dialect=self._dialect, expr=agg_expr, dt=self._slot_cast_type(slot))
                    has_aggregation = True
                select_columns.append(agg_expr.copy().as_(full_alias, quoted=True))
                _record_alias(sid, full_alias)
            else:
                raise MaterialisationStageError(
                    f"value {sid!r} (phase {slot.phase!r}) reached the base "
                    f"SELECT; a BASE-staged value is ROW or AGGREGATE by "
                    f"construction.",
                )

        dimension_values = {slots_by_id[sid].key: display_by_sid.get(sid, gb) for sid, gb in group_by_keys.items()}
        # No dimension and no aggregate: every metric is the single grand-total cell.
        grand_total = not group_by_keys and not has_aggregation and bool(deferred_composites)
        for idx, slot, full_alias in deferred_composites:
            composite = render_value_key(
                key=slot.key,
                ctx=RenderContext(
                    dialect=self._dialect,
                    field_type=host_scope.field_type,
                    dimension_values=dimension_values,
                    composites=CompositeFacilities(
                        agg_builder=self._composite_agg_builder(
                            slot=slot,
                            source_dataset=source_dataset,
                            source_relation=source_relation,
                            bundle=bundle,
                            resolved_agg_kwargs=resolved_agg_kwargs,
                            scope=host_scope,
                        ),
                    ),
                ),
            )
            if contains_aggregate(slot.key):
                composite = _wrap_cast_for_type(dialect=self._dialect, expr=composite, dt=self._slot_cast_type(slot))
            elif grand_total:
                composite = _grand_total_value(composite)
            select_columns[idx] = composite.as_(full_alias, quoted=True)
        has_aggregation = has_aggregation or grand_total

        base_select = exp.Select()
        for col in select_columns:
            base_select = base_select.select(col)
        base_select = base_select.from_(from_clause)
        base_select = _apply_joins(select=base_select, joins=base_joins)
        # Attach each regroup producer on its partition grain (null-safe LEFT, or CROSS for a grand total);
        # cardinality-preserving.
        for cte_name, condition in regroup_attach_conditions:
            if condition is None:
                base_select = base_select.join(
                    exp.to_identifier(cte_name), join_type="CROSS",
                )
            else:
                base_select = base_select.join(
                    exp.to_identifier(cte_name), on=condition, join_type="LEFT",
                )
        # A regroup value carrying a dotted producer alias (`_cm_x.`a.b``) round-trips
        # through `_resolve_sql` and BigQuery/T-SQL re-parse it as `_cm_x.a.b`; repair
        # it here, where the base select's FROM/JOIN sources are complete.
        unmangle_dotted_table_refs(base_select)
        return (
            base_select, aliases_by_slot_id, has_aggregation, group_by_keys,
            dimension_values,
        )

    @staticmethod
    def _explicit_time_arg_of(key):
        """The explicit positional ranking-time arg of a ``first`` / ``last``"""

        if key.agg not in ("first", "last"):
            return None
        for a in key.args:
            return a if isinstance(a, (FieldKey, FieldSqlKey)) else None
        return None

    def _composite_agg_builder(
        self, *, slot, source_dataset, source_relation: str, bundle,
        resolved_agg_kwargs, scope: ScopeFrame,
    ):
        """The AGGREGATE-phase composite seam: render one"""

        def build(agg_key) -> Expression:
            if source_anchor_path(agg_key.source):
                # Internal invariant: cross-dataset operands desugar to regroup
                # placeholders before phase classification, so none reaches
                # this seam.
                raise RuntimeError(
                    f"cross-dataset aggregate operand {agg_key!r} reached the "
                    f"AGGREGATE-phase composite seam; the regroup desugar "
                    f"should have replaced it with a placeholder."
                )
            synth = self._build_agg_render_spec_from_planned(
                slot=slot, key=agg_key, source_dataset=source_dataset,
                source_relation=source_relation, full_alias="__op__",
                bundle=bundle,
                resolved_agg_kwargs=(resolved_agg_kwargs or {}).get(agg_key),
                scope=scope,
            )
            agg_expr, _is_agg = self._build_agg(synth)
            return agg_expr

        return build

    def _render_window_metric_cte_from_planned(  # NOSONAR(S3776) — one cohesive host-rooted range-join CTE build: ``_src`` projection (dims / other-time-dims / raw-window-time / value) with Law-1 relationship-path discovery, WHERE inheritance minus date_range, and the ``_base LEFT JOIN _src`` interval range join. Splitting scatters the shared scope / grain-alias / join-eq state.
        self,
        *,
        plan,
        agg_slot: ValueSlot,
        source_dataset,
        source_relation: str,
        bundle,
        planned_query,
        slots_by_id: Dict[str, Any],
        aliases_by_slot_id: Dict[str, List[str]],
        full_agg_alias: str,
        base_relation: Optional[Expression] = None,
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str, Optional[DataType]]]]]] = None,
        association: Optional[EntityDedup] = None,
    ) -> Tuple[exp.Select, List[str]]:
        """Render one duration-windowed-metric CTE; associating, each frame counts an entity once."""

        key = agg_slot.key
        assert isinstance(key, AggregateKey)

        allocator = self._gen_allocator or self._new_allocator()
        src_scope = self._scope_frame(
            dataset=source_dataset, relation=source_relation,
            bundle=bundle, allocator=allocator,
            attached_fields=regroup_env,
        )

        def _base_col(alias: str) -> exp.Column:
            return exp.Column(
                this=exp.to_identifier(alias, quoted=True),
                table=exp.to_identifier("_base"),
            )

        def _src_col(name: str) -> exp.Column:
            return exp.Column(
                this=exp.to_identifier(name), table=exp.to_identifier("_src"),
            )

        def _alias_of(sid: str) -> str:
            al = aliases_by_slot_id.get(sid) or []
            return al[0] if al else sid

        src_cols: List[Expr] = []
        grain_pairs: List[GrainPair] = []
        grain_aliases: List[str] = []
        grain_keys: Dict[str, Any] = {}

        for idx, sid in enumerate(plan.dimension_slot_ids):
            dslot = slots_by_id.get(sid)
            assert dslot is not None
            base_alias = _alias_of(sid)
            expr = render_value_key(
                key=dslot.key,
                ctx=RenderContext(scope=src_scope, dialect=self._dialect),
            )
            src_cols.append(expr.as_(f"_w_dim_{idx}"))
            grain_pairs.extend(self._keyed_pairs([(_src_col(f"_w_dim_{idx}"), _base_col(base_alias), dslot.key)]))
            grain_aliases.append(base_alias)
            grain_keys[base_alias] = dslot.key

        # Non-window time dimensions are equality-joined so the trailing window doesn't fan out across their values.
        for idx, sid in enumerate(plan.other_time_dimension_slot_ids):
            tslot = slots_by_id.get(sid)
            assert tslot is not None
            base_alias = _alias_of(sid)
            raw = self._bucket_source_expr(
                source=tslot.key.source, scope=src_scope, source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
            )
            trunc = self._build_date_trunc(col_expr=raw, key=tslot.key)
            src_cols.append(trunc.as_(f"_w_td_{idx}"))
            grain_pairs.extend(self._keyed_pairs([(_src_col(f"_w_td_{idx}"), _base_col(base_alias), tslot.key)]))
            grain_aliases.append(base_alias)

        wtd_slot = slots_by_id.get(plan.window_time_dimension_slot_id)
        assert wtd_slot is not None
        wtd_alias = _alias_of(plan.window_time_dimension_slot_id)
        raw_time = self._bucket_source_expr(
            source=wtd_slot.key.source, scope=src_scope, source_dataset=source_dataset,
            source_relation=source_relation, bundle=bundle,
        )
        src_cols.append(raw_time.copy().as_("_w_time"))
        grain_aliases.append(wtd_alias)

        # A DatasetField.filter on the source is baked into its FieldSqlKey (CASE WHEN),
        # so a first/last picked value is masked while ranking spans all rows:
        # the latest row's value may be NULL if it fails the filter.
        # ``count(*)`` projects a literal so the outer COUNT(_w_value) counts interval
        # rows (0, not 1, on an empty interval) — the star never enters resolve.
        # The custom-aggregation definition lives on the source's owning dataset,
        # which a parameter may widen the home above (D4) — resolve it there.
        definition = self._resolve_aggregation_def(
            key=key,
            source_dataset=(
                self._walk_relationship_path_dataset(
                    source_dataset=source_dataset,
                    path=source_anchor_path(key.source), bundle=bundle,
                ) or source_dataset
            ),
            src_leaf="_w_value",
        )
        expression = rendered_expression(agg=key.agg, definition=definition)
        if isinstance(key.source, StarKey):
            # ``*`` is only legal with count (as in the plain path); any other
            # aggregation over the star would silently become ``<agg>(1)``.
            if key.agg != "count":
                raise ValueError(
                    f"Aggregation {key.agg!r} not allowed with metric "
                    f"'*' — use 'count(<dataset>.*)' for COUNT(*)."
                )
            # ``count(*)`` takes no inputs but its own ``window=`` (plain-path guard);
            # a stray arg/kwarg would otherwise be projected and silently ignored.
            extra_kwargs = [(k, v) for k, v in key.kwargs if k != "window"]
            if key.args or (extra_kwargs and not expression):
                raise ValueError(
                    f"'count(*)' takes no args or kwargs other than window; got "
                    f"args={key.args!r}, kwargs={extra_kwargs!r}."
                )
            src_cols.append(exp.Literal.number("1").as_("_w_value"))
        else:
            assert not isinstance(key.source, (AggregateKey, TransformKey, TimePointCmpKey))  # sources are row-level here
            src_cols.append(src_scope.resolve(key.source).as_("_w_value"))

        # Reference-bearing parameters read per interval row (D4): each _w_p<i> is
        # rendered through the _src scope so its relationship paths register for discovery.
        picked_kwarg_exprs: Dict[str, ResolvedAggKwarg] = {}
        for _i, _pp in enumerate(plan.picked_params):
            _p_alias = f"_w_p{_i}"
            _picked = render_value_key(
                key=_pp.key, ctx=RenderContext(scope=src_scope, dialect=self._dialect),
            )
            src_cols.append(_picked.as_(_p_alias))
            picked_kwarg_exprs[_pp.name] = ResolvedAggKwarg(
                value=_src_col(_p_alias),
                input_type=aggregation_source_type(_pp.key, field_type=src_scope.field_type),
            )

        # A windowed first/last ranks the interval rows by this key, projected as
        # _w_rank (uncast, like the plain ranked path).
        if plan.ranking_time_key is not None:
            src_cols.append(self._ranked_scope_expr(
                key=plan.ranking_time_key, root_dataset=source_dataset,
                root_relation=source_relation, bundle=bundle, scope=src_scope,
                cast_derived=False,
            ).as_("_w_rank"))
        n_entity_keys = 0 if association is None else len(association.entity_keys)
        for i, ekey in enumerate(association.entity_keys if association is not None else []):
            src_cols.append(render_value_key(
                key=ekey, ctx=RenderContext(scope=src_scope, dialect=self._dialect),
            ).as_(f"_w_ek{i}"))

        # _src inherits row filters minus their frame bounds; one effective list feeds both relationship-path discovery
        # and rendering so they can't disagree.
        lowered_src = _lower_positions(planned_query).filters
        all_filter_ids = {fp.id for fp in lowered_src}
        skip_for_src = all_filter_ids - set(plan.where_filter_ids)
        src_filters = _effective_src_filters(lowered_filters=lowered_src, plan=plan)
        self._resolve_where_filter_relationships_via_scope(
            planned_query=planned_query, scope=src_scope,
            skip_filter_ids=skip_for_src, filters_override=src_filters,
        )
        src_where, _src_having = self._build_where_having_from_planned(
            planned_query=planned_query, source_relation=source_relation,
            source_dataset=source_dataset, bundle=bundle,
            skip_filter_ids=skip_for_src, filters_override=src_filters,
        )

        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=src_scope,
        )

        from_expr, src_joins = self._build_from_and_joins(
            source_dataset=source_dataset, source_relation=source_relation,
            joined_paths=src_scope.relationship_paths.as_list(), bundle=bundle,
        )
        src_select = exp.Select().select(*src_cols).from_(from_expr)
        src_select = _apply_joins(select=src_select, joins=src_joins)
        for _cte_name, _condition in regroup_attach_conditions:
            if _condition is None:
                src_select = src_select.join(
                    exp.to_identifier(_cte_name), join_type="CROSS",
                )
            else:
                src_select = src_select.join(
                    exp.to_identifier(_cte_name), on=_condition, join_type="LEFT",
                )
        if src_where is not None:
            src_select = src_select.where(src_where)
        for cond in self._semi_join_exists_conditions(
            planned_query=planned_query, source_dataset=source_dataset,
            source_relation=source_relation, bundle=bundle,
        ):
            src_select = src_select.where(cond)
        src_subq = exp.Subquery(
            this=src_select, alias=exp.TableAlias(this=exp.to_identifier("_src")),
        )

        # Trailing-window range: _src._w_time in [bucket_end - window, bucket_end), bucket_end being the host bucket's
        # exclusive upper edge.
        # Calendar parts apply one by one in written order, clamping like date_add.
        assert wtd_slot is not None
        time_type = bucket_operand(wtd_slot.key, field_type=src_scope.field_type)
        operand = granularity_bucket_type(time_type, wtd_slot.key.granularity)
        unit, step, _ = granularity_parts(plan.window_granularity)
        bucket_end = self._date_offset(_base_col(wtd_alias), count=step, unit=unit, operand=operand)
        lower_bound, operand = bucket_end, date_add_type(operand, unit)
        end_type = operand
        for amount, letter in plan.window_parts:
            unit = WINDOW_UNIT_GRANULARITY[letter]
            lower_bound = self._date_offset(lower_bound, count=-amount, unit=unit, operand=operand)
            operand = date_add_type(operand, unit)
        # The frame bounds are dialect-built timestamps; the source time operand is
        # normalised to the same type by the dialect (identity except SQLite, whose
        # bare-date affinity would leak the exclusive bucket_end row — sql P2).
        src_w_time = self._dialect.frame_time_operand(_src_col("_w_time"))
        # An empty grain yields None here, but the range predicates still correlate the sides, so this stays a LEFT
        # JOIN, not a CROSS JOIN.
        grain_condition = build_grain_joinback_condition(
            pairs=grain_pairs, dialect=self._dialect,
        )
        on_range = exp.and_(
            *([grain_condition] if grain_condition is not None else []),
            exp.GTE(**self._temporal_operands(src_w_time, time_type, lower_bound, operand)),
            exp.LT(**self._temporal_operands(src_w_time.copy(), time_type, bucket_end.copy(), end_type)),
        )

        base_from = (
            base_relation if base_relation is not None
            else exp.Table(this=exp.to_identifier("_base"))
        )
        value_type = aggregation_source_type(key.source, field_type=src_scope.field_type)
        # Association's level 1: one framed row per (bucket, cell, entity); the reducer runs over those.
        entity_rows: Optional[exp.Subquery] = None
        if association is not None:
            picked: List[Tuple[str, Optional[DataType]]] = [
                ("_w_value", value_type),
                *((f"_w_p{i}", picked_kwarg_exprs[pp.name].input_type) for i, pp in enumerate(plan.picked_params)),
            ]
            if plan.ranking_time_key is not None:
                picked.append(("_w_rank", None))
            picked.append(("_w_time", time_type))
            entity_rows = self._window_entity_rows(
                framed=exp.Select().from_(base_from).join(src_subq, on=on_range, join_type="LEFT"),
                grain_aliases=grain_aliases, grain_keys=grain_keys,
                n_entity_keys=n_entity_keys, picked=picked,
            )

        def _framed(select: exp.Select) -> exp.Select:
            if entity_rows is None:
                return select.from_(base_from).join(src_subq, on=on_range, join_type="LEFT")
            return select.from_(entity_rows.copy())

        def grain_col(alias: str) -> exp.Column:
            return _base_col(alias) if entity_rows is None else exp.Column(
                this=exp.to_identifier(alias, quoted=True), table=exp.to_identifier("_src"),
            )

        # first/last: rank the range-joined interval rows and pick rank 1 per bucket
        # through the one ranked shape — the LEFT JOIN's NULL row of an empty
        # interval ranks 1 and yields NULL, matching sum (D3).
        if plan.ranking_time_key is not None:
            rank_inner = exp.Select()
            for ga in grain_aliases:
                rank_inner = rank_inner.select(
                    grain_col(ga).as_(exp.to_identifier(ga, quoted=True)))
            rank_inner = rank_inner.select(_src_col("_w_value").as_("_w_value"))
            rank_inner = rank_inner.select(build_rank_key(_src_col("_w_rank")))
            rank_inner = rank_inner.select(build_rank_field(
                partition_by=[grain_col(ga) for ga in grain_aliases],
                ranking_time=ranked_ordered(
                    ranking_time=_src_col("_w_rank"), agg=plan.agg,
                    native_nulls_first=self._dialect.native_nulls_first(
                        descending=plan.agg == "last",
                    ),
                ),
            ))
            rank_inner = _framed(rank_inner)
            pick = _wrap_cast_for_type(
                dialect=self._dialect, expr=build_ranked_pick(
                    value_ref=exp.Column(
                        this=exp.to_identifier("_w_value"),
                        table=exp.to_identifier(RANKED_SOURCE_ALIAS),
                    ),
                    input_type=value_type,
                    dialect=self._dialect,
                ),
                dt=_ranked_value_cast_type(self._slot_cast_type(agg_slot)),
            )
            grain_proj = [
                RankedGrainProjection(
                    output_alias=ga,
                    inner_ref=exp.Column(
                        this=exp.to_identifier(ga, quoted=True),
                        table=exp.to_identifier(RANKED_SOURCE_ALIAS),
                    ),
                )
                for ga in grain_aliases
            ]
            outer, _ = build_ranked_cte_select(
                inner=rank_inner, grain=grain_proj, pick=pick,
                agg_alias=full_agg_alias,
            )
            return outer, grain_aliases

        # Every other aggregation renders through the one aggregate builder over
        # the _src value column (sql P5) — no per-name branch (engine P6). ``count``
        # names _w_value (COUNT(*) would count an empty interval's unmatched grain
        # row as 1); picked parameters read _src._w_p<i> (D2, D4).
        level2_spec = AggRenderSpec(
            name="_w_value",
            sql="_w_value" if plan.agg == "count" else None,
            aggregation=plan.agg,
            alias=full_agg_alias,
            dataset_name="_src",
            type=agg_slot.type,
            expression=expression,
            agg_kwargs=self._agg_param_exprs(
                key=key, expression=expression, resolved=picked_kwarg_exprs, scope=src_scope,
            ),
            input_type=value_type,
        )
        agg_expr, _ = self._build_agg(level2_spec)
        if definition is not None and definition.expression is not None:
            # A defined aggregation's empty value is NULL (semantics Axiom 4), whatever its expression yields.
            agg_expr = exp.Case(ifs=[exp.If(
                this=exp.EQ(this=exp.Count(this=_src_col("_w_time")), expression=exp.Literal.number(0)),
                true=exp.Null(),
            )], default=agg_expr)
        agg_expr = _wrap_cast_for_type(
            dialect=self._dialect, expr=agg_expr, dt=self._slot_cast_type(agg_slot),
        )

        outer = exp.Select()
        for ga in grain_aliases:
            shown = self._grouped_display(grain_col(ga), key=grain_keys.get(ga))
            outer = outer.select(shown.as_(exp.to_identifier(ga, quoted=True)) if isinstance(shown, exp.AnyValue)
                                 else shown)
        outer = outer.select(agg_expr.as_(exp.to_identifier(full_agg_alias, quoted=True)))
        outer = _framed(outer)
        for ga in grain_aliases:
            outer = outer.group_by(self._grain_key(grain_col(ga), key=grain_keys.get(ga)))

        # Return AST: rendering here and re-parsing later would re-introduce the dotted-alias corruption on BigQuery.
        return outer, grain_aliases

    def _window_entity_rows(
        self, *, framed: exp.Select, grain_aliases: Sequence[str], grain_keys: Mapping[str, Any],
        n_entity_keys: int, picked: Sequence[Tuple[str, Optional[DataType]]],
    ) -> exp.Subquery:
        """The range-joined rows as ``_src``, one per (bucket, cell, entity), each ``picked`` column read once; an
        empty frame's NULL row stays one entity-less row."""
        def base(alias: str) -> exp.Column:
            return exp.Column(this=exp.to_identifier(alias, quoted=True), table=exp.to_identifier("_base"))

        def src(name: str) -> exp.Column:
            return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier("_src"))

        select = framed
        for ga in grain_aliases:
            shown = self._grouped_display(base(ga), key=grain_keys.get(ga))
            select = select.select(shown.as_(exp.to_identifier(ga, quoted=True)))
        for name, input_type in picked:
            select = select.select(self._pick_once(src(name), input_type=input_type).as_(exp.to_identifier(name)))
        for ga in grain_aliases:
            select = select.group_by(self._grain_key(base(ga), key=grain_keys.get(ga)))
        for i in range(n_entity_keys):
            select = select.group_by(src(f"_w_ek{i}"))
        return exp.Subquery(this=select, alias=exp.TableAlias(this=exp.to_identifier("_src")))

    def _ranked_scope_expr(
        self,
        *,
        key,
        root_dataset,
        root_relation: str,
        bundle,
        scope: ScopeFrame,
        cast_derived: bool = True,
    ) -> Expression:
        """One value expression anchored in a ranked CTE's own scope."""
        if isinstance(key, TimeTruncKey):
            if isinstance(key.source, (FieldKey, FieldSqlKey)):
                raw = self._raw_time_col_expr_for_planned(
                    time_field=key.source, source_dataset=root_dataset,
                    source_relation=root_relation, bundle=bundle,
                )
                self._register_ranked_paths(
                    raw, field_path(key.source), root_dataset=root_dataset, root_relation=root_relation,
                    bundle=bundle, scope=scope,
                )
            else:
                raw = self._ranked_scope_expr(
                    key=key.source, root_dataset=root_dataset, root_relation=root_relation,
                    bundle=bundle, scope=scope, cast_derived=False,
                )
            return self._build_date_trunc(col_expr=raw, key=key)
        if isinstance(key, FieldKey):
            expr = self._joined_or_local_dim_expr(
                path=key.path, leaf=key.leaf, source_dataset=root_dataset,
                source_relation=root_relation, bundle=bundle,
            )
            self._register_ranked_paths(
                expr, key.path, root_dataset=root_dataset, root_relation=root_relation, bundle=bundle, scope=scope,
            )
            return expr
        if isinstance(key, FieldSqlKey):
            return self._ranked_derived_field_expr(
                key=key, root_dataset=root_dataset, root_relation=root_relation, bundle=bundle, scope=scope,
                cast_derived=cast_derived,
            )
        if isinstance(key, (*SLOT_COMPOSITE_KINDS, LiteralKey)):
            return render_value_key(key=key, ctx=RenderContext(scope=scope, dialect=self._dialect))
        raise NotImplementedError(
            f"Ranked CTE cannot anchor a {type(key).__name__} — the grain and "
            f"the ranking key are row-level values.",
        )

    def _ranked_derived_field_expr(
        self, *, key: FieldSqlKey, root_dataset, root_relation: str, bundle, scope: ScopeFrame, cast_derived: bool,
    ) -> Expression:
        expr = self._derived_field_expr(
            key=key, source_dataset=root_dataset,
            source_relation=root_relation, bundle=bundle,
        )
        if expr is None:
            raise ValueError(
                f"Derived field {key.field_name!r} on dataset "
                f"{key.path[-1] if key.path else root_dataset.name!r} is not "
                f"in the resolved source bundle.",
            )
        self._register_ranked_paths(
            expr, key.path, root_dataset=root_dataset, root_relation=root_relation, bundle=bundle, scope=scope,
        )
        return expr if cast_derived else _strip_declared_cast(expr)

    def _register_ranked_paths(
        self, expr: Expression, path: Tuple[str, ...], *, root_dataset, root_relation: str, bundle, scope: ScopeFrame,
    ) -> None:
        """Record the relationship paths ``expr`` reads (``path`` included) in the ranked CTE's scope."""
        if path:
            scope.relationship_paths.add(path)
        for p in self._joined_paths_in_sql(
            sql_expr=expr, source_relation=root_relation,
            source_dataset=root_dataset, bundle=bundle,
        ):
            scope.relationship_paths.add(p)

    def _ranked_value_expr(
        self, *, key, root_dataset: SlayerDataset, root_relation: str, bundle, scope: ScopeFrame,
    ) -> Expression:
        """The value a ranked aggregate picks, anchored in its own scope."""

        source = key.source
        if isinstance(source, StarKey):
            raise ValueError(
                f"Aggregation {key.agg!r} not allowed with metric "
                f"'*' — use 'count(<dataset>.*)' for COUNT(*)."
            )
        if not isinstance(source, (FieldKey, FieldSqlKey)):
            # An expression (a mixed one's attached values row-attached in ``scope``).
            return render_value_key(key=source, ctx=RenderContext(scope=scope, dialect=self._dialect))
        leaf = (
            source.leaf if isinstance(source, FieldKey) else source.field_name
        )
        col = next((c for c in root_dataset.fields if c.name == leaf), None)
        if col is None:
            raise ValueError(
                f"Aggregate source field {leaf!r} not found on dataset "
                f"{root_dataset.name!r}",
            )
        if isinstance(source, FieldSqlKey):
            # The value (and any DatasetField.filter CASE) with its type CAST baked in —
            # parse without re-casting the whole expression.
            expr = self._parse(self._expand_derived_field_sql(
                source_dataset=root_dataset, source_relation=root_relation,
                field_name=col.name, bundle=bundle,
            ))
        else:
            expr = self._dialect.read_column(self._resolve_sql(
                sql=col.definition_sql or col.name, name=col.name,
                dataset_name=root_relation, type=col.datatype,
            ), zoned=self._dialect.is_zoned_db_type(col.db_type))
        for p in self._joined_paths_in_sql(
            sql_expr=expr, source_relation=root_relation,
            source_dataset=root_dataset, bundle=bundle,
        ):
            scope.relationship_paths.add(p)
        return expr

    def _render_ranked_cte_from_planned(  # NOSONAR(S3776) — single linear ranked-CTE assembly (src → ROW_NUMBER → collapse); the branches are sequential dialect/shape guards, not nested logic
        self,
        *,
        plan: "_RankedEmission",
        agg_slot,
        bundle,
        planned_query,
        slots_by_id: Dict[str, Any],
        host_source_dataset,
        host_source_relation: str,
        full_agg_alias: str,
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str, Optional[DataType]]]]]] = None,
        association: Optional[EntityDedup] = None,
    ) -> Tuple[exp.Select, List[str]]:
        """Render one ranked (``first`` / ``last``) CTE; associating, it ranks the deduplicated entity rows."""

        key = agg_slot.key
        if not isinstance(key, AggregateKey):
            raise RuntimeError(
                f"Ranked emission {plan.aggregate_slot_id!r} references a "
                f"non-aggregate slot.",
            )

        root_dataset = host_source_dataset
        root_relation = host_source_relation

        allocator = self._gen_allocator or self._new_allocator()
        self._reserve_dataset_field_names(allocator, root_dataset)

        def _frame(*, attached=None) -> ScopeFrame:
            return self._scope_frame(
                dataset=root_dataset, relation=root_relation,
                bundle=bundle, allocator=allocator, attached_fields=attached,
            )

        ranked_scope = _frame(attached=regroup_env)
        cte_scope = _frame()

        local_key = key

        grain: List[RankedGrainProjection] = []
        partition_by: List[Expression] = []
        for member in plan.grain:
            host_slot = slots_by_id.get(member.host_slot_id)
            if host_slot is None:
                raise RuntimeError(
                    f"Ranked emission grain references host slot "
                    f"{member.host_slot_id!r}, which this plan does not carry.",
                )
            if isinstance(
                member.ranked_key, (*SLOT_COMPOSITE_KINDS, TransformKey),
            ) or (
                isinstance(member.ranked_key, FieldKey)
                and member.ranked_key.leaf.startswith(REGROUP_LEAF_PREFIX)
            ):
                expr = render_value_key(
                    key=member.ranked_key,
                    ctx=RenderContext(scope=ranked_scope, dialect=self._dialect),
                )
                if isinstance(member.ranked_key, SLOT_COMPOSITE_KINDS) and host_slot.is_dimension:
                    # The join-back key equals the host's grain value.
                    expr = self._typed_dimension(expr, slot=host_slot)
            else:
                expr = self._ranked_scope_expr(
                    key=member.ranked_key, root_dataset=root_dataset,
                    root_relation=root_relation, bundle=bundle, scope=ranked_scope,
                )
            # PARTITION BY takes the raw expression (evaluated in the ranked scope where its joins bind); the outer
            # SELECT takes the materialised alias.
            partition_by.append(expr.copy())
            grain.append(RankedGrainProjection(
                output_alias=self._full_alias_for_slot(
                    slot=host_slot,
                    source_relation=host_source_relation,
                    alias_index={},
                ),
                inner_ref=ranked_scope.materialize_for(
                    expr, consumer=cte_scope,
                ),
            ))

        value_expr = self._ranked_value_expr(
            key=local_key, root_dataset=root_dataset,
            root_relation=root_relation, bundle=bundle, scope=ranked_scope,
        )
        # A FILTER ranks the passing rows first and picks only a passing row's value.
        passing_first: List[exp.Ordered] = []
        if local_key.filter is not None:
            value_expr = self._passing_value(local_key.filter, value=value_expr, scope=ranked_scope)
            passing_first.append(exp.Ordered(this=exp.Case(
                ifs=[exp.If(this=value_expr.args["ifs"][0].this.copy(), true=exp.Literal.number(0))],
                default=exp.Literal.number(1),
            )))
        value_ref = ranked_scope.materialize_for(value_expr, consumer=cte_scope)
        ranking_time = self._ranked_scope_expr(
            key=plan.ranking_time_key, root_dataset=root_dataset,
            root_relation=root_relation, bundle=bundle, scope=ranked_scope,
            cast_derived=False,
        )

        where_parts = self._ranked_cte_where(
            plan=plan, planned_query=planned_query,
            bundle=bundle, root_dataset=root_dataset, root_relation=root_relation,
            scope=ranked_scope,
        )

        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=ranked_scope,
        )
        entity_exprs = [
            render_value_key(key=ek, ctx=RenderContext(scope=ranked_scope, dialect=self._dialect))
            for ek in (association.entity_keys if association is not None else [])
        ]
        entity_refs = [ranked_scope.materialize_for(e, consumer=cte_scope) for e in entity_exprs]

        from_expr, joins = self._build_from_and_joins(
            source_dataset=root_dataset, source_relation=root_relation,
            joined_paths=ranked_scope.relationship_paths.as_list(), bundle=bundle,
        )
        inner = exp.Select()
        # A named projection list, never <relation>.*: the projection boundary keeps the rank column's name private.
        ranked_scope.apply_materializations(inner)
        inner = inner.select(build_rank_key(ranking_time))
        if association is not None:
            if passing_first:
                inner = inner.select(passing_first[0].this.copy().as_(RANK_PASS_FIELD))
        else:
            inner = inner.select(build_rank_field(
                partition_by=partition_by,
                ranking_time=ranked_ordered(
                    ranking_time=ranking_time,
                    agg=plan.agg,
                    native_nulls_first=self._dialect.native_nulls_first(
                        descending=plan.agg == "last",
                    ),
                ),
                leading=passing_first,
            ))
        inner = inner.from_(from_expr)
        inner = _apply_joins(select=inner, joins=joins)
        for _cte_name, _condition in regroup_attach_conditions:
            if _condition is None:
                inner = inner.join(
                    exp.to_identifier(_cte_name), join_type="CROSS",
                )
            else:
                inner = inner.join(
                    exp.to_identifier(_cte_name), on=_condition, join_type="LEFT",
                )
        if where_parts:
            inner = inner.where(
                exp.and_(*where_parts) if len(where_parts) > 1 else where_parts[0],
            )
        value_type = aggregation_source_type(local_key.source, field_type=ranked_scope.field_type)
        if association is not None:
            for e in entity_exprs:
                inner = inner.where(exp.Not(this=exp.Is(this=e.copy(), expression=exp.Null())))
            inner, value_ref = self._ranked_entity_rows(
                inner=inner, plan=plan, grain=grain, entity_refs=entity_refs, value_ref=value_ref,
                value_type=value_type, passing=bool(passing_first),
            )

        # A first/last value is the raw picked column, so its temporal type needs no CAST (SQLite would give numeric
        # affinity, truncating a date to its year).
        pick = _wrap_cast_for_type(
            dialect=self._dialect, expr=build_ranked_pick(
                value_ref=value_ref, dialect=self._dialect, input_type=value_type,
            ),
            dt=_ranked_value_cast_type(self._slot_cast_type(agg_slot)),
        )
        return build_ranked_cte_select(
            inner=inner, grain=grain, pick=pick, agg_alias=full_agg_alias,
        )

    def _ranked_entity_rows(
        self, *, inner: exp.Select, plan, grain: Sequence[RankedGrainProjection], entity_refs: Sequence[Expression],
        value_ref: Expression, value_type: Optional[DataType], passing: bool,
    ) -> Tuple[exp.Select, Expression]:
        """Association's level 1 under ``first``/``last``: one row per (cell, entity) picking its value, ranking key
        and FILTER pass once, then ranked per cell; returns the ranked select and its value column."""
        ent = exp.to_identifier("_ranked_ent")
        members: Dict[str, Any] = {}
        for g, member in zip(grain, plan.grain):
            members.setdefault(g.inner_ref.name, member.ranked_key)
        cols = [self._grouped_display(exp.column(n), key=k).as_(n) for n, k in members.items()]
        cols.append(self._pick_once(value_ref.copy(), input_type=value_type).as_(RANK_VALUE_FIELD))
        cols.append(exp.Max(this=exp.column(RANK_KEY_FIELD)).as_(RANK_KEY_FIELD))
        if passing:
            cols.append(exp.Min(this=exp.column(RANK_PASS_FIELD)).as_(RANK_PASS_FIELD))
        deduped = exp.Select().select(*cols).from_(exp.Subquery(this=inner, alias=exp.TableAlias(this=ent)))
        for n, k in members.items():
            deduped = deduped.group_by(self._grain_key(exp.column(n), key=k))
        for ref in entity_refs:
            deduped = deduped.group_by(ref.copy())
        rank_key = exp.column(RANK_KEY_FIELD)
        ranked = exp.Select().select(
            *(exp.column(n) for n in members), exp.column(RANK_VALUE_FIELD), rank_key.copy(),
            build_rank_field(
                partition_by=[self._grain_key(exp.column(n), key=k) for n, k in members.items()],
                ranking_time=ranked_ordered(
                    ranking_time=rank_key, agg=plan.agg,
                    native_nulls_first=self._dialect.native_nulls_first(descending=plan.agg == "last"),
                ),
                leading=[exp.Ordered(this=exp.column(RANK_PASS_FIELD))] if passing else (),
            ),
        ).from_(exp.Subquery(this=deduped, alias=exp.TableAlias(this=ent.copy())))
        return ranked, exp.column(RANK_VALUE_FIELD)

    def _ranked_cte_where(
        self,
        *,
        plan,
        planned_query,
        bundle,
        root_dataset,
        root_relation: str,
        scope: ScopeFrame,
    ) -> List[Expression]:
        """Every predicate a ranked CTE applies to the rows it ranks.

        A ``DatasetField.filter`` on the ranked field is NOT a WHERE here — it masks the
        picked value (baked into the source's FieldSqlKey CASE), so ranking spans
        every row and the latest row's value may be NULL."""
        parts: List[Expression] = []
        skip_ids = {
            fp.id for fp in _lower_positions(planned_query).filters
        } - set(plan.where_filter_ids)
        self._resolve_where_filter_relationships_via_scope(
            planned_query=planned_query, scope=scope,
            skip_filter_ids=skip_ids,
        )
        routed, _having = self._build_where_having_from_planned(
            planned_query=planned_query,
            source_relation=root_relation,
            source_dataset=root_dataset,
            bundle=bundle,
            skip_filter_ids=skip_ids,
        )
        if routed is not None:
            parts.append(routed)
        parts.extend(self._semi_join_exists_conditions(
            planned_query=planned_query, source_dataset=root_dataset,
            source_relation=root_relation, bundle=bundle,
        ))
        return parts

    def _render_kernel_producer_body(self, *, planned_query, bundle: ResolvedSourceBundle, kernel) -> exp.Select:
        """The aggregate phase of a ranked / trailing-window kernel producer"""
        source_dataset = bundle.population
        source_relation = planned_query.source_relation
        slots_by_id = {
            s.id: s
            for s in (
                list(planned_query.row_slots) + list(planned_query.aggregate_slots)
            )
        }
        regroup_ctes, regroup_env, regroup_join_specs, _ = (
            self._prepare_regroup_attaches(planned_query=planned_query, bundle=bundle)
            if any(r.attach_phase == "row" for r in planned_query.regroup_attach_plans)
            else ([], {}, [], [])
        )

        if kernel.kind == "association":
            body = self._render_association_producer_body(
                planned_query=planned_query, bundle=bundle, kernel=kernel,
                source_dataset=source_dataset, source_relation=source_relation,
                slots_by_id=slots_by_id, regroup_env=regroup_env,
                regroup_join_specs=regroup_join_specs,
            )
        elif kernel.kind == "ranked":
            plan = _ranked_emission_from_kernel(
                planned_query=planned_query, kernel=kernel,
            )
            agg_slot = slots_by_id[plan.aggregate_slot_id]
            body, _grain_aliases = self._render_ranked_cte_from_planned(
                plan=plan,
                agg_slot=agg_slot,
                bundle=bundle,
                planned_query=planned_query,
                slots_by_id=slots_by_id,
                host_source_dataset=source_dataset,
                host_source_relation=source_relation,
                full_agg_alias=self._full_alias_for_slot(
                    slot=agg_slot, source_relation=source_relation, alias_index={},
                ),
                regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
                association=kernel.association,
            )
        else:
            plan = _windowed_emission_from_kernel(
                planned_query=planned_query, kernel=kernel,
            )
            agg_slot = slots_by_id[plan.aggregate_slot_id]
            aliases_by_slot_id = {
                s.id: [self._full_alias_for_slot(
                    slot=s, source_relation=source_relation, alias_index={},
                )]
                for s in slots_by_id.values()
            }
            grain_base = (
                self._window_endpoints_base(
                    kernel=kernel, plan=plan, slots_by_id=slots_by_id,
                    aliases_by_slot_id=aliases_by_slot_id,
                )
                if kernel.endpoints is not None else self._build_windowed_grain_base(
                    planned_query=planned_query, plan=plan, slots_by_id=slots_by_id,
                    aliases_by_slot_id=aliases_by_slot_id, source_dataset=source_dataset,
                    source_relation=source_relation, bundle=bundle,
                    regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
                )
            )
            base_subq = exp.Subquery(
                this=grain_base,
                alias=exp.TableAlias(this=exp.to_identifier("_base")),
            )
            body, _ = self._render_window_metric_cte_from_planned(
                plan=plan, agg_slot=agg_slot, source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
                planned_query=planned_query, slots_by_id=slots_by_id,
                aliases_by_slot_id=aliases_by_slot_id,
                full_agg_alias=self._full_alias_for_slot(
                    slot=agg_slot, source_relation=source_relation, alias_index={},
                ),
                base_relation=base_subq,
                regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
                association=kernel.association,
            )

        if regroup_ctes:
            body = self._assemble_with_chain(
                entries=regroup_ctes, final=body,
                external_names=self._external_cte_names(),
            )
        return body

    def _association_level_one_from_host(
        self, *, inner_cols: List[Expr], planned_query: PlannedQuery, scope: ScopeFrame, bundle, kernel,
        source_dataset, source_relation, entity_exprs: List[Expression], regroup_join_specs,
    ) -> exp.Select:
        """Level 1 of an association producer over its root's rows, every row attach joined on its grain."""
        self._resolve_where_filter_relationships_via_scope(
            planned_query=planned_query, scope=scope, skip_filter_ids=set(),
        )
        where, _having = self._build_where_having_from_planned(
            planned_query=planned_query, source_relation=source_relation,
            source_dataset=source_dataset, bundle=bundle, skip_filter_ids=set(),
        )
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs or [], scope=scope,
        )
        from_expr, joins = self._build_from_and_joins(
            source_dataset=source_dataset, source_relation=source_relation,
            joined_paths=scope.relationship_paths.as_list(), bundle=bundle,
        )
        inner = exp.Select().select(*inner_cols).from_(from_expr)
        inner = _apply_joins(select=inner, joins=joins)
        for _cte_name, _condition in regroup_attach_conditions:
            if _condition is None:
                inner = inner.join(exp.to_identifier(_cte_name), join_type="CROSS")
            else:
                inner = inner.join(
                    exp.to_identifier(_cte_name), on=_condition, join_type="LEFT",
                )
        if where is not None:
            inner = inner.where(where)
        # A host row with no associated entity (a NULL key from the LEFT JOIN) is
        # not a distinct entity — exclude it so ``count(*)`` never counts it. A
        # null-safe re-aggregation keeps a NULL grain cell as its own
        # cell instead.
        if not getattr(kernel, "null_safe", False):
            for eexpr in entity_exprs:
                inner = inner.where(
                    exp.Not(this=exp.Is(this=eexpr, expression=exp.Null())))
        for cond in self._semi_join_exists_conditions(
            planned_query=planned_query, source_dataset=source_dataset,
            source_relation=source_relation, bundle=bundle,
        ):
            inner = inner.where(cond)
        return inner

    def _render_association_producer_body(  # NOSONAR(S3776) — one cohesive two-level association body: level-1 dedup SELECT (grain × entity key, picked value) wrapped as ``_base``, level-2 aggregate over the picked rows. The two arms share the grain-alias / scope state.
        self, *, planned_query: PlannedQuery, bundle, kernel, source_dataset, source_relation,
        slots_by_id, regroup_env=None, regroup_join_specs=None,
    ) -> exp.Select:
        """The distinct-entity association producer: level 1 groups by
        (grain × the root's entity key) picking each input once per entity; level
        2 aggregates over the picked rows per grain."""
        agg_slot = planned_query.aggregate_slots[0]
        assert isinstance(agg_slot.key, AggregateKey)
        grain_slots = [
            slots_by_id[sid]
            for sid in planned_query.projection
            if sid != agg_slot.id
        ]
        alias_index: Dict[str, int] = {}
        grain_aliases = [
            self._full_alias_for_slot(
                slot=s, source_relation=source_relation, alias_index=alias_index,
            )
            for s in grain_slots
        ]
        agg_alias = self._full_alias_for_slot(
            slot=agg_slot, source_relation=source_relation, alias_index=alias_index,
        )
        allocator = self._gen_allocator or self._new_allocator()
        scope = self._scope_frame(
            dataset=source_dataset, relation=source_relation,
            bundle=bundle, allocator=allocator, attached_fields=regroup_env,
        )
        ctx = RenderContext(scope=scope, dialect=self._dialect)

        inner_cols: List[Expr] = []
        group: List[Expression] = []
        for slot, alias in zip(grain_slots, grain_aliases):
            expr = render_value_key(key=slot.key, ctx=ctx)
            inner_cols.append(self._grouped_display(expr, key=slot.key).as_(exp.to_identifier(alias, quoted=True)))
            group.append(self._grain_key(expr.copy(), key=slot.key))
        entity_exprs: List[Expression] = []
        for idx, ekey in enumerate(kernel.entity_keys):
            eexpr = render_value_key(key=ekey, ctx=ctx)
            ek_alias = f"_ek{idx}"
            inner_cols.append(eexpr.copy().as_(exp.to_identifier(ek_alias)))
            group.append(eexpr.copy())
            entity_exprs.append(eexpr.copy())

        # Level 1 picks each input once per entity (MAX is arbitrary-but-correct:
        # the input is root-determined, constant per entity); ``count(*)`` keeps no
        # value column — level 2 counts the entity rows.
        def pick(value: Expression, *, input_type: Optional[DataType]) -> Expression:
            return self._pick_once(value, input_type=input_type)

        is_star = isinstance(agg_slot.key.source, StarKey)
        if agg_slot.key.filter is not None:
            raise RuntimeError(
                f"{key_display(agg_slot.key)}: a key-carried FILTER reached an association producer; it renders "
                f"only on a local star count or a ranked pick, and restricts a non-built-in's producer rows.",
            )
        picked_alias = "_v"
        # Parameters the grain determines are picked once per cell as
        # _p<i> (below) and read by level 2 as _base._p<i>; the level-1 value pick
        # is the aggregate's own source, stripped of those parameters.
        picked_params = list(getattr(kernel, "picked_params", []) or [])
        picked_names = {pp.name for pp in picked_params}
        spec: Optional[AggRenderSpec] = None
        if not is_star and getattr(kernel, "null_safe", False):
            # Re-aggregation: the per-cell value is the carrier's
            # attached composite; render it through the scope (its placeholders
            # resolve to the carrier columns) and pick it once per cell.
            value_expr = render_value_key(key=agg_slot.key.source, ctx=ctx)
            expression = rendered_expression(agg=agg_slot.key.agg, definition=self._resolve_aggregation_def(
                key=agg_slot.key, source_dataset=source_dataset, src_leaf=picked_alias,
            ))
            spec = AggRenderSpec(
                name=picked_alias, sql=None, aggregation=agg_slot.key.agg,
                alias=agg_alias, dataset_name="_base", type=agg_slot.type,
                expression=expression,
                agg_kwargs=self._agg_param_exprs(
                    key=agg_slot.key, expression=expression, scope=scope,
                    skip=frozenset(picked_names),
                ),
                input_type=aggregation_source_type(agg_slot.key.source, field_type=scope.field_type),
            )
            inner_cols.append(exp.Alias(
                this=pick(value_expr.copy(), input_type=spec.input_type),
                alias=exp.to_identifier(picked_alias),
            ))
        elif not is_star:
            # Discovery runs over the FULL key so a parameter's relationship path (e.g.
            # weight=customers.regions.pop) is registered in the scope; the source
            # spec is built from the parameter-stripped key so a parameter path
            # that extends the source path is not rejected.
            resolved = self._resolve_agg_inputs_via_scope(
                base_render_order=[agg_slot.id], slots_by_id={agg_slot.id: agg_slot},
                scope=scope,
            )
            source_key = agg_slot.key.model_copy(update={
                "kwargs": tuple((k, v) for k, v in agg_slot.key.kwargs
                                if k not in picked_names),
            })
            spec = self._build_agg_render_spec_from_planned(
                slot=agg_slot, key=source_key, source_dataset=source_dataset,
                source_relation=source_relation, full_alias=picked_alias,
                bundle=bundle,
                resolved_agg_kwargs={
                    k: v for k, v in (resolved.get(agg_slot.key) or {}).items()
                    if k not in picked_names
                },
                scope=scope,
            )
            inner_cols.append(
                pick(self._resolve_value_ast(spec), input_type=spec.input_type).as_(
                    exp.to_identifier(picked_alias),
                ),
            )

        # Pick each legal parameter once per cell as _p<i>. A DatasetField.filter masks
        # only its own field (baked into that field's FieldSqlKey CASE), never
        # the source's filter.
        picked_kwarg_exprs: Dict[str, ResolvedAggKwarg] = {}
        for _i, _pp in enumerate(picked_params):
            _p_alias = f"_p{_i}"
            _picked = render_value_key(key=_pp.key, ctx=ctx)
            _picked_type = aggregation_source_type(_pp.key, field_type=scope.field_type)
            inner_cols.append(exp.Alias(
                this=pick(_picked, input_type=_picked_type),
                alias=exp.to_identifier(_p_alias),
            ))
            picked_kwarg_exprs[_pp.name] = ResolvedAggKwarg(
                value=exp.Column(
                    this=exp.to_identifier(_p_alias),
                    table=exp.to_identifier("_base"),
                ),
                input_type=_picked_type,
            )

        row_attaches = [a for a in planned_query.regroup_attach_plans if a.attach_phase == "row"]
        driving = next((spec[0] for a, spec in zip(row_attaches, regroup_join_specs or []) if a.drives), None)
        if driving is not None:
            # A re-aggregation reads its operand's cells as its own rows (semantics Axiom 2.4).
            inner = exp.Select().select(*inner_cols).from_(exp.to_identifier(driving))
        else:
            inner = self._association_level_one_from_host(
                inner_cols=inner_cols, planned_query=planned_query, scope=scope, bundle=bundle, kernel=kernel,
                source_dataset=source_dataset, source_relation=source_relation, entity_exprs=entity_exprs,
                regroup_join_specs=regroup_join_specs,
            )
        for g in group:
            inner = inner.group_by(g)

        base_subq = exp.Subquery(
            this=inner, alias=exp.TableAlias(this=exp.to_identifier("_base")),
        )

        def _base_col(alias: str, *, quoted: bool = True) -> exp.Column:
            return exp.Column(
                this=exp.to_identifier(alias, quoted=quoted),
                table=exp.to_identifier("_base"),
            )

        # Level 2 aggregates over the picked rows per grain; ``count(*)`` counts
        # the entity rows (COUNT(*)), every other family runs over ``_v``.
        if is_star:
            expression = rendered_expression(
                agg=agg_slot.key.agg, definition=self._resolve_aggregation_def(
                    key=agg_slot.key, source_dataset=source_dataset, src_leaf="*",
                ))
            level2_spec = AggRenderSpec(
                name="", sql=None, aggregation=agg_slot.key.agg,
                alias=agg_alias, dataset_name="_base", type=agg_slot.type,
                expression=expression,
                agg_kwargs=self._agg_param_exprs(
                    key=agg_slot.key, expression=expression, resolved=picked_kwarg_exprs,
                    scope=scope,
                ),
            )
        else:
            assert spec is not None  # set in both non-star arms above
            level2_spec = AggRenderSpec(
                # ``count`` counts cells with a NON-NULL picked value (COUNT(_v)),
                # never the cells (COUNT(*)): a DatasetField.filter masks non-matching
                # rows to NULL, so a filtered association count must skip them.
                # Other families already read _base._v via the
                # sql=None branch, so only count must name _v here.
                name=picked_alias,
                sql=(
                    picked_alias
                    if getattr(kernel, "null_safe", False)
                    or agg_slot.key.agg == "count"
                    else None
                ),
                aggregation=agg_slot.key.agg,
                alias=agg_alias, dataset_name="_base", type=agg_slot.type,
                field_type=spec.field_type,
                input_type=spec.input_type,
                # A picked parameter reads from _base._p<i>, overriding its
                # explicit-kwarg / definition-default resolution.
                agg_kwargs={**spec.agg_kwargs, **picked_kwarg_exprs},
                expression=spec.expression,
            )
        agg_expr, _ = self._build_agg(level2_spec)
        agg_expr = _wrap_cast_for_type(dialect=self._dialect, expr=agg_expr, dt=self._slot_cast_type(agg_slot))
        grain_key_of = {alias: slot.key for slot, alias in zip(grain_slots, grain_aliases)}
        outer_cols: List[Expr] = [
            self._grouped_display(_base_col(alias), key=grain_key_of[alias]).as_(exp.to_identifier(alias, quoted=True))
            for alias in grain_aliases
        ]
        outer_cols.append(agg_expr.as_(exp.to_identifier(agg_alias, quoted=True)))
        outer = exp.Select().select(*outer_cols).from_(base_subq)
        for alias in grain_aliases:
            outer = outer.group_by(self._grain_key(_base_col(alias), key=grain_key_of[alias]))
        return outer

    def _window_endpoints_base(self, *, kernel, plan, slots_by_id, aliases_by_slot_id) -> exp.Select:
        """The windowed producer's cells: the population's, each named as the producer's grain column."""
        endpoints = kernel.endpoints
        inner = self._build_from_planned(endpoints, bundle=endpoints.stage_bundle, reuse_allocator=True)
        outputs = inner.named_selects
        grain_sids = [
            *plan.dimension_slot_ids, *plan.other_time_dimension_slot_ids, plan.window_time_dimension_slot_id,
        ]
        cols = [
            exp.column(outputs[kernel.endpoint_keys.index(slots_by_id[sid].key)], table="_e", quoted=True).as_(
                exp.to_identifier(aliases_by_slot_id[sid][0], quoted=True),
            )
            for sid in grain_sids
        ]
        return exp.select(*cols).from_(exp.Subquery(this=inner, alias=exp.to_identifier("_e")))

    def _build_windowed_grain_base(
        self, *, planned_query, plan, slots_by_id, aliases_by_slot_id,
        source_dataset, source_relation, bundle,
        regroup_env=None, regroup_join_specs=None,
    ) -> exp.Select:
        """The grain-rows relation for a collapsed windowed producer"""
        allocator = self._gen_allocator or self._new_allocator()
        scope = self._scope_frame(
            dataset=source_dataset, relation=source_relation,
            bundle=bundle, allocator=allocator, attached_fields=regroup_env,
        )
        cols: List[Expr] = []
        group: List[Expression] = []

        def _emit(slot, expr: Expression) -> None:
            alias = aliases_by_slot_id[slot.id][0]
            cols.append(self._grouped_display(expr, key=slot.key).as_(exp.to_identifier(alias, quoted=True)))
            group.append(self._grain_key(expr.copy(), key=slot.key))

        for sid in plan.dimension_slot_ids:
            dslot = slots_by_id[sid]
            _emit(dslot, render_value_key(
                key=dslot.key,
                ctx=RenderContext(scope=scope, dialect=self._dialect),
            ))
        for sid in (*plan.other_time_dimension_slot_ids,
                    plan.window_time_dimension_slot_id):
            tslot = slots_by_id[sid]
            raw = self._bucket_source_expr(
                source=tslot.key.source, scope=scope, source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
            )
            _emit(tslot, self._build_date_trunc(col_expr=raw, key=tslot.key))
        # ROW filters gate the visible grain; the trailing window in the _src join reaches rows before them via
        # plan.where_filter_ids (a strict subset).
        self._resolve_where_filter_relationships_via_scope(
            planned_query=planned_query, scope=scope, skip_filter_ids=set(),
        )
        where, _having = self._build_where_having_from_planned(
            planned_query=planned_query, source_relation=source_relation,
            source_dataset=source_dataset, bundle=bundle, skip_filter_ids=set(),
        )
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=scope,
        )
        from_expr, joins = self._build_from_and_joins(
            source_dataset=source_dataset, source_relation=source_relation,
            joined_paths=scope.relationship_paths.as_list(), bundle=bundle,
        )
        base = exp.Select().select(*cols).from_(from_expr)
        base = _apply_joins(select=base, joins=joins)
        for _cte_name, _condition in regroup_attach_conditions:
            if _condition is None:
                base = base.join(exp.to_identifier(_cte_name), join_type="CROSS")
            else:
                base = base.join(
                    exp.to_identifier(_cte_name), on=_condition, join_type="LEFT",
                )
        if where is not None:
            base = base.where(where)
        for cond in self._semi_join_exists_conditions(
            planned_query=planned_query, source_dataset=source_dataset,
            source_relation=source_relation, bundle=bundle,
        ):
            base = base.where(cond)
        for g in group:
            base = base.group_by(g)
        return base

    def _render_with_combined_attaches(  # NOSONAR(S3776) — orchestration of host ``_base`` CTE + per-plan ``_cm_*`` CTEs + combined SELECT + transform-chain step CTEs + outer ORDER BY/LIMIT wrap. Each block is a coherent compilation stage sharing planned_query / slots_by_id / cma_slot_ids / seen_base_ids state; extracting per-stage helpers would scatter the cross-cutting state.
        self,
        *,
        planned_query,
        bundle: ResolvedSourceBundle,
        spine_mark: int,
    ) -> exp.Select:
        """Render a ``PlannedQuery`` that carries one or more COMBINED"""

        source_dataset = bundle.population
        source_relation = planned_query.source_relation

        slots_by_id = {
            s.id: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }


        # One lowering per render: filter placements + scoped order entries.
        lowered = _lower_positions(planned_query)
        slot_by_key = {s.key: s for s in slots_by_id.values()}

        # Combined regroup producers render as _cm_ CTEs joined at the combined SELECT; prepared before the ROW
        # producers so a dual-role aggregate dedups onto them.
        (
            cm_regroup_ctes,
            regroup_placeholder_to_cm,
            regroup_placeholder_slot_ids,
            regroup_joinbacks,
            regroup_shift_specs,
            reused_combined_ctes,
        ) = self._prepare_combined_regroup_attaches(
            planned_query=planned_query, bundle=bundle,
            source_relation=source_relation, slot_by_key=slot_by_key,
        )

        _combined_attaches = [
            a for a in planned_query.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        combined_dedup_index: Dict[Any, Any] = {}
        for _attach, (_cte_name, _grain_pairs) in zip(
            _combined_attaches, regroup_shift_specs,
        ):
            _okey_to_col = {
                sub.original_key: regroup_placeholder_to_cm[sub.placeholder].field_name
                for sub in _attach.substitutions
                if sub.placeholder in regroup_placeholder_to_cm
            }
            combined_dedup_index[self._regroup_attach_identity(_attach)] = (
                _cte_name, _okey_to_col, _grain_pairs,
            )

        row_regroup_ctes, row_regroup_env, row_regroup_join_specs, reused_cm_ctes = (
            self._prepare_regroup_attaches(
                planned_query=planned_query, bundle=bundle,
                dedup_producers=combined_dedup_index,
            )
            if any(r.attach_phase == "row" for r in planned_query.regroup_attach_plans)
            else ([], {}, [], [])
        )
        isolated_slot_ids = set(regroup_placeholder_slot_ids)
        outer_where_filter_ids: Set[str] = set(lowered.outer_where_ids)
        outer_where_filters: List = [
            fp for fp in lowered.filters
            if fp.id in outer_where_filter_ids
        ]
        # Placement is planner-owned: a COMBINED composite renders at
        # the combined SELECT; a DERIVED composite (transform-reading) renders in
        # the transform chain; a computed dimension groups in _base. Only COMBINED
        # composites route outward here.
        outer_composite_slot_ids: Set[str] = staged_plan.combined_composite_slot_ids(
            planned_query,
        )
        # _base projects every BASE ∧ needs_field value in plan order; a dual-role
        # placeholder joined at the combined SELECT is read from its _cm_ CTE.
        base_render_order = staged_plan.base_render_order(
            planned_query, isolated_slot_ids=isolated_slot_ids,
        )
        base_id_set = set(base_render_order)
        base_projection = [
            sid for sid in planned_query.projection if sid in base_id_set
        ]
        proj_set = set(planned_query.projection)
        order_target_ids = {e.slot_id for e in planned_query.order}
        order_only_local_ids = [
            sid for sid in base_render_order
            if sid not in proj_set and sid in order_target_ids
        ]

        # With no host rows or local aggs, _base is a one-row placeholder emitted WITHOUT the host FROM — a host FROM
        # would make it N rows and the scalar-_cm_ CROSS JOIN would duplicate the result N times.
        empty_base_plan = planned_query.empty_base_plan
        if empty_base_plan is not None:
            host_filter_ids = set(empty_base_plan.host_filter_ids)
            # A population semi-join gates the spine even with no field mask.
            host_gated = host_filter_ids or empty_base_plan.host_gated
            placeholder_skip_ids = {
                fp.id
                for fp in lowered.filters
                if fp.id not in host_filter_ids
            }
            if host_gated:
                # LIMIT 1 collapses the host to one row so the CROSS JOIN doesn't duplicate aggregates, while WHERE
                # and the correlated EXISTS still gate the whole result (no matching host row -> 0 rows).
                placeholder_allocator = self._gen_allocator or self._new_allocator()
                placeholder_scope = self._scope_frame(
                    dataset=source_dataset, relation=source_relation,
                    bundle=bundle, allocator=placeholder_allocator,
                )
                self._resolve_where_filter_relationships_via_scope(
                    planned_query=planned_query,
                    scope=placeholder_scope,
                    skip_filter_ids=placeholder_skip_ids,
                )
                assert source_dataset is not None
                placeholder_from, placeholder_joins = self._build_from_and_joins(
                    source_dataset=source_dataset,
                    source_relation=source_relation,
                    joined_paths=placeholder_scope.relationship_paths.as_list(),
                    bundle=bundle,
                )
                base_select = exp.Select().select(
                    exp.Alias(
                        this=exp.Literal.number("1"),
                        alias=exp.to_identifier("_placeholder"),
                    ),
                ).from_(placeholder_from)
                base_select = _apply_joins(select=base_select, joins=placeholder_joins)
                base_where, _base_having = self._build_where_having_from_planned(
                    planned_query=planned_query,
                    source_relation=source_relation,
                    source_dataset=source_dataset,
                    bundle=bundle,
                    skip_filter_ids=placeholder_skip_ids,
                )
                if base_where is not None:
                    base_select = base_select.where(base_where)
                for cond in self._semi_join_exists_conditions(
                    planned_query=planned_query, source_dataset=source_dataset,
                    source_relation=source_relation, bundle=bundle,
                ):
                    base_select = base_select.where(cond)
                base_select = base_select.limit(1)
            else:
                base_select = exp.Select().select(
                    exp.Alias(
                        this=exp.Literal.number("1"),
                        alias=exp.to_identifier("_placeholder"),
                    ),
                )
            aliases_by_slot_id: Dict[str, List[str]] = {}
            base_has_agg = False
            base_group_by: Dict[str, Expression] = {}
        else:
            # Skip only the host filters _base cannot apply: a _cm_ CTE is LEFT-joined on grain, so a predicate applied
            # only there blanks a row's metric instead of excluding the row; ROW-phase filters apply in both places.
            routed_ids: Set[str] = set(outer_where_filter_ids)
            (
                base_select,
                aliases_by_slot_id,
                base_has_agg,
                base_group_by,
                base_dimension_values,
            ) = self._build_base_select_for_planned(
                planned_query=planned_query,
                bundle=bundle,
                source_dataset=source_dataset,
                source_relation=source_relation,
                base_render_order=base_render_order,
                slots_by_id=slots_by_id,
                skip_cross_dataset_aggs=True,
                skip_filter_ids=routed_ids,
                regroup_env=row_regroup_env,
                regroup_join_specs=row_regroup_join_specs,
            )

            base_where, base_having = self._build_where_having_from_planned(
                planned_query=planned_query,
                source_relation=source_relation,
                source_dataset=source_dataset,
                bundle=bundle,
                skip_filter_ids=routed_ids,
                aliases_by_slot_id=aliases_by_slot_id,
                regroup_env=row_regroup_env,
                dimension_values=base_dimension_values,
            )
            if base_where is not None:
                base_select = base_select.where(base_where)
            for cond in self._semi_join_exists_conditions(
                planned_query=planned_query, source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
            ):
                base_select = base_select.where(cond)
            base_dim_only_dedup = (
                planned_query.grain is not None
                and bool(base_group_by)
                and not base_has_agg
            )
            if (base_has_agg or base_dim_only_dedup) and base_group_by:
                for gb in base_group_by.values():
                    base_select = base_select.group_by(gb)
            if base_having is not None:
                base_select = base_select.having(base_having)



        proj_exprs: Dict[str, List[Expr]] = {}
        combined_aliases_by_slot_id: Dict[str, List[str]] = {}

        def _emit(sid: str, expr: Expr) -> None:
            proj_exprs.setdefault(sid, []).append(expr)
        host_combined_ids = (
            base_render_order
            if planned_query.transform_layers
            else base_projection
        )
        # Dedup by declared name: a C13 slot's alias list already has one entry per name, so visiting it twice and
        # emitting the whole list renders N^2 columns.
        _seen_host_ids: Set[str] = set()
        for sid in host_combined_ids:
            if sid in _seen_host_ids:
                continue
            _seen_host_ids.add(sid)
            aliases = aliases_by_slot_id.get(sid, [])
            for full_alias in aliases:
                _emit(sid, grain_alias_field(alias=full_alias, table="_base"))
            if aliases:
                combined_aliases_by_slot_id[sid] = list(aliases)
        outer_composite_order_alias_by_sid: Dict[str, str] = {}
        outer_composite_order_expressions: Dict[str, Expression] = {}
        if outer_composite_slot_ids:
            outer_composite_cm_map: Dict[str, AttachedValue] = {}
            for _ph_key, _cm in regroup_placeholder_to_cm.items():
                _ph_slot = slot_by_key.get(_ph_key)
                if _ph_slot is not None:
                    outer_composite_cm_map[_ph_slot.id] = _cm

            def _render_outer_composite(cslot: ValueSlot) -> Expression:
                rendered = render_value_key(
                    key=cslot.key,
                    ctx=self._outer_wrapper_render_ctx(
                        slot_by_key=slot_by_key,
                        cross_dataset_agg_slot_to_cm=outer_composite_cm_map,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ),
                )
                if cslot.type is not None:
                    rendered = _wrap_cast_for_type(dialect=self._dialect, expr=rendered, dt=self._slot_cast_type(cslot))
                return rendered

            # Cycle public_aliases per projection occurrence: emitting public_aliases[0] twice would drop the second C13
            # name.
            outer_emission_count: Dict[str, int] = {}
            for sid in planned_query.projection:
                if sid not in outer_composite_slot_ids:
                    continue
                cslot = slots_by_id.get(sid)
                if cslot is None:
                    continue
                aliases_for_slot = list(cslot.public_aliases) or [
                    cslot.declared_name,
                ]
                idx = outer_emission_count.get(sid, 0)
                public_alias = (
                    aliases_for_slot[idx]
                    if idx < len(aliases_for_slot)
                    else aliases_for_slot[-1]
                )
                outer_emission_count[sid] = idx + 1
                full_alias = (result_key_from_alias(source_relation=source_relation, alias=public_alias)
                              if cslot.public_aliases
                              else internal_result_key(source_relation=source_relation, name=public_alias))
                _emit(
                    sid,
                    _render_outer_composite(cslot).as_(full_alias, quoted=True),
                )
                combined_aliases_by_slot_id.setdefault(sid, []).append(
                    full_alias,
                )
                outer_composite_order_alias_by_sid.setdefault(sid, full_alias)

            # Order-only outer composites render inline in the combined ORDER BY (not as a hidden column) so no
            # synthetic column leaks into the public projection.
            projection_set_for_outer = set(planned_query.projection)
            for entry in planned_query.order:
                sid = entry.slot_id
                if sid not in outer_composite_slot_ids:
                    continue
                if sid in projection_set_for_outer:
                    continue
                cslot = slots_by_id.get(sid)
                if cslot is None:
                    continue
                outer_composite_order_expressions[sid] = (
                    _render_outer_composite(cslot)
                )
        for _ph_key, _attached in regroup_placeholder_to_cm.items():
            ph_slot = slot_by_key.get(_ph_key)
            if ph_slot is None or ph_slot.id not in regroup_placeholder_slot_ids:
                continue
            _agg_col = _attached.field_name
            if ph_slot.hidden and planned_query.transform_layers:
                carried = self._hidden_carry_name(ph_slot=ph_slot, column=_agg_col, cte=_attached.cte_name,
                                                  source_relation=source_relation, taken=aliases_by_slot_id)
                _emit(ph_slot.id, _attached.value().as_(carried, quoted=True))
                combined_aliases_by_slot_id[ph_slot.id] = [carried]
                continue
            public_aliases = (
                []
                if ph_slot.hidden
                else self._public_aliases_for_cross_dataset_agg(
                    slot=ph_slot,
                    source_relation=source_relation,
                    canonical_alias=_agg_col,
                )
            )
            for pub in public_aliases:
                value = _attached.value()
                bare = pub == _agg_col and isinstance(value, exp.Column)
                _emit(ph_slot.id, value if bare else value.as_(pub, quoted=True))
            combined_aliases_by_slot_id[ph_slot.id] = list(public_aliases)

        # Both plan kinds join back on the shared grain null-safely (a NULL dim value keeps its aggregate); an empty
        # grain becomes a CROSS JOIN.
        # Every renderer consumes planned_query.projection verbatim; a slot appears once per declared name and each
        # occurrence consumes the next of its rendered columns.
        combined_select_exprs: List[Expr] = []
        consumed: Dict[str, int] = {}
        for sid in planned_query.projection:
            exprs = proj_exprs.get(sid)
            if not exprs:
                if planned_query.transform_layers:
                    continue  # rendered later in the transform chain
                raise ValueError(
                    f"public slot {sid!r} rendered no column at the combined "
                    f"SELECT; it would be silently dropped from the result",
                )
            idx = consumed.get(sid, 0)
            if idx >= len(exprs):
                raise ValueError(
                    f"slot {sid!r} appears {idx + 1} times in the public "
                    f"projection but rendered only {len(exprs)} column(s); the "
                    f"occurrence would be dropped from the result",
                )
            combined_select_exprs.append(exprs[idx])
            consumed[sid] = idx + 1
        # Carry only slots the projection never mentions; a leftover for a published slot means rendered columns and
        # declared names disagree — fail rather than emit an extra public column.
        for sid, exprs in proj_exprs.items():
            if sid not in consumed:
                combined_select_exprs.extend(exprs)
            elif consumed[sid] < len(exprs):
                raise ValueError(
                    f"slot {sid!r} rendered {len(exprs)} column(s) but the "
                    f"projection consumed only {consumed[sid]}; the plan's "
                    f"declared names and the rendered columns disagree",
                )

        combined_select = exp.Select().select(*combined_select_exprs)
        combined_select = combined_select.from_("_base")

        joined_cte_names: set = set()
        joinback_specs = list(regroup_joinbacks)  # Combined regroup producers
        for cte_name, joinback_pairs in joinback_specs:
            if cte_name in joined_cte_names:
                continue
            joined_cte_names.add(cte_name)
            key_by_alias = {a: slots_by_id[sid].key for sid, als in aliases_by_slot_id.items() for a in als}
            on_condition = build_grain_joinback_condition(
                pairs=self._keyed_pairs([
                    (
                        grain_alias_field(alias=host, table="_base"),
                        grain_alias_field(alias=cte_col, table=cte_name),
                        key_by_alias.get(host),
                    )
                    for host, cte_col, _ in joinback_pairs
                ], right_types=[producer_type for _, _, producer_type in joinback_pairs]),
                dialect=self._dialect,
            )
            if on_condition is None:
                combined_select = combined_select.join(
                    cte_name, join_type="CROSS",
                )
            else:
                combined_select = combined_select.join(
                    cte_name, on=on_condition, join_type="LEFT",
                )

        if outer_where_filters:
            # Map every cross-dataset aggregate slot (filtered-local AND forward) to its _cm_ column: a mixed AGGREGATE
            # filter resolves both operands outer, so mapping only filtered-local ones makes the forward operand raise.
            cross_dataset_agg_slot_to_cm: Dict[str, AttachedValue] = {}
            for _ph_key, _cm in regroup_placeholder_to_cm.items():
                _ph_slot = slot_by_key.get(_ph_key)
                if _ph_slot is not None:
                    cross_dataset_agg_slot_to_cm[_ph_slot.id] = _cm
            for fp in outer_where_filters:
                rendered = render_value_key(
                    key=fp.expression.value_key,
                    ctx=self._outer_wrapper_render_ctx(
                        slot_by_key=slot_by_key,
                        cross_dataset_agg_slot_to_cm=cross_dataset_agg_slot_to_cm,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ),
                )
                combined_select = combined_select.where(_grouped(rendered))

        base_deps = [*[e.name for e in row_regroup_ctes], *reused_cm_ctes,
                     *(e.name for e in self._gen_spine_ctes[spine_mark:])]
        supply_ctes: List[CteEntry] = []
        if planned_query.row_supply is not None:
            base_select, supply_ctes, base_deps = self._row_supply_base(
                planned_query=planned_query, base_select=base_select, aliases_by_slot_id=aliases_by_slot_id,
                slots_by_id=slots_by_id, bundle=bundle, base_deps=base_deps,
            )
        elif planned_query.domain is not None:
            base_select, supply_ctes, base_deps = self._domain_base(
                planned_query=planned_query, base_select=base_select, aliases_by_slot_id=aliases_by_slot_id,
                slots_by_id=slots_by_id, bundle=bundle, base_deps=base_deps,
                joinbacks=dict(regroup_joinbacks),
                cte_of={ph: attached.cte_name for ph, attached in regroup_placeholder_to_cm.items()},
            )
        spine_ctes = self._take_spine_ctes(since=spine_mark)
        if planned_query.transform_layers:
            return self._render_steps_and_post(
                prelude_nodes=[
                    *row_regroup_ctes,
                    *(Node(name=e.name, phase="base", query=e.query, depends_on=[], recursive=e.recursive)
                      for e in spine_ctes),
                    *(Node(name=e.name, phase="base", query=e.query, depends_on=list(e.depends_on),
                           recursive=e.recursive) for e in supply_ctes),
                    Node(
                        name="_base", phase="base", query=base_select,
                        depends_on=base_deps,
                        schema_by_slot=dict(aliases_by_slot_id),
                    ),
                    *cm_regroup_ctes,
                ],
                tail_select=combined_select,
                tail_schema=combined_aliases_by_slot_id,
                tail_phase="combined",
                reused_names=reused_combined_ctes,
                planned_query=planned_query,
                bundle=bundle,
                source_dataset=source_dataset,
                source_relation=source_relation,
                slots_by_id=slots_by_id,
                regroup_env={
                    **row_regroup_env,
                    **{
                        ph: attached.value()
                        for ph, attached in regroup_placeholder_to_cm.items()
                    },
                },
                regroup_join_specs=[
                    *row_regroup_join_specs, *regroup_shift_specs,
                ],
            )

        # WITH dependencies are declared, not discovered by scanning the statement; the assembler emits a stable
        # topological order with declaration order as tiebreak.
        cte_entries = [
            *spine_ctes,
            *supply_ctes,
            CteEntry(name="_base", query=base_select, depends_on=base_deps),
            *row_regroup_ctes,
        ]
        cte_entries += cm_regroup_ctes
        combined_statement = self._assemble_with_chain(
            entries=cte_entries, final=combined_select,
            external_names=self._external_cte_names(),
        )

        # Emit ORDER BY/LIMIT/OFFSET at the combined level through one resolver: naming a projected cross-dataset
        # aggregate by its CTE column picks the wrong column once two scopes project the same name.
        order_env = OrderEnv(dialect=self._dialect)
        for _ph_key, _attached in regroup_placeholder_to_cm.items():
            _ph_slot = slot_by_key.get(_ph_key)
            if _ph_slot is not None:
                order_env.cross_dataset_cte[_ph_slot.id] = _attached.value()
        for _sid, _alias in outer_composite_order_alias_by_sid.items():
            order_env.outer_composite[_sid] = exp.column(_alias, quoted=True)
        for _sid, _expr in outer_composite_order_expressions.items():
            order_env.outer_composite.setdefault(_sid, _expr)
        # An order-only local slot is named BARE: a _base. qualifier would dangle under the outer projection-trim
        # wrapper, while the bare name still resolves against _base.
        _local_bare_ids = set(order_only_local_ids)
        for entry in lowered.order:
            if entry.scope not in HOST_BASE_SCOPES:
                continue
            slot = slots_by_id.get(entry.slot_id)
            if slot is None:
                continue
            _full_alias = self._full_alias_for_slot(
                slot=slot, source_relation=source_relation, alias_index={},
            )
            getattr(order_env, entry.scope.value)[entry.slot_id] = (
                exp.column(_full_alias, quoted=True)
                if entry.slot_id in _local_bare_ids
                else grain_alias_field(alias=_full_alias, table="_base")
            )
        order_terms = [
            term for entry in lowered.order for term in resolve_order_term(entry=entry, env=order_env)
        ]
        if order_terms:
            combined_statement.set("order", exp.Order(expressions=order_terms))

        combined_statement = self._dialect.apply_pagination(
            combined_statement,
            limit=planned_query.limit,
            offset=planned_query.offset,
        )

        return combined_statement

    def _canonical_cross_dataset_alias(
        self,
        *,
        source_relation: str,
        key,
    ) -> str:
        """Build the canonical result-key alias for a cross-dataset"""
        # The kwarg suffix is part of the CTE name so two parametric aggregates (p=0.5 vs p=0.95) get distinct
        # names/aliases instead of colliding.
        alias = canonical_aggregate_alias(
            key, profile="cross_dataset_cte", source_relation=source_relation,
        )
        assert alias is not None  # the cross_dataset_cte profile never declines
        return alias

    @staticmethod
    def _hidden_carry_name(
        *, ph_slot: ValueSlot, column: str, cte: str, source_relation: str, taken: Mapping[str, List[str]],
    ) -> str:
        """A hidden regroup value's carried column: a dotted name, which never equals a (bare) query alias, and no
        other slot's — its producer column, else its placeholder's key, else the column under the population."""
        seed = re.sub(rf"^{re.escape(REGROUP_LEAF_PREFIX)}\d+__", "", ph_slot.declared_name)
        used = {a for sid, aliases in taken.items() if sid != ph_slot.id for a in aliases}
        for name in (column, seed, internal_result_key(source_relation=source_relation, name=column)):
            if "." in name and name not in used:
                return name
        return f"{cte}.{column}"

    def _public_aliases_for_cross_dataset_agg(
        self,
        *,
        slot,
        source_relation: str,
        canonical_alias: str,
    ) -> List[str]:
        """User-facing combined-SELECT aliases for this cross-dataset slot."""
        if not slot.public_aliases:
            return [canonical_alias]
        return [result_key_from_alias(source_relation=source_relation, alias=a) for a in slot.public_aliases]

    @staticmethod
    def _producer_render_bundle(attach: RegroupAttachPlan, bundle: ResolvedSourceBundle) -> ResolvedSourceBundle:
        """The bundle a target-rooted producer renders against."""
        root_name = getattr(attach, "producer_root_dataset", None)
        if not root_name:
            return bundle
        # A home stamped with the datasets it meets as a product renders over its own bundle.
        bundle = attach.producer_plan.stage_bundle or bundle
        root = bundle.get_referenced_dataset(root_name)
        if root is None:
            raise RuntimeError(
                f"Target-rooted regroup producer names root dataset {root_name!r}, "
                f"absent from the bundle's referenced datasets."
            )
        kernel = getattr(attach, "kernel", None)
        if isinstance(kernel, AssociationProducerKernel):
            entity_keys = kernel.entity_keys
        elif isinstance(kernel, (RankedProducerKernel, TrailingWindowProducerKernel)) and kernel.association:
            entity_keys = kernel.association.entity_keys
        else:
            return bundle.rerooted(root)
        root, numbered = identified_datasets(
            root=root, entity_keys=entity_keys, datasets_by_name=bundle.datasets_by_name,
        )
        return bundle.rerooted(root, replacing=numbered)

    def _assemble_with_chain(
        self, *, entries, final, external_names=frozenset(),
    ):
        """``assemble_with_chain`` recording each entry's declared deps into the
        top statement-scoped registry, so a later split recovers a hoisted
        producer's edges without AST-scanning (sql P6)."""
        if self._gen_dep_stack:
            top = self._gen_dep_stack[-1]
            for e in entries:
                deps = top.setdefault(e.name, [])
                for d in e.depends_on:
                    if d not in deps:
                        deps.append(d)
        return assemble_with_chain(
            entries=entries, final=final, external_names=external_names,
        )

    def _row_supply_base(
        self, *, planned_query, base_select: exp.Select, aliases_by_slot_id: Dict[str, List[str]],
        slots_by_id: Dict[str, ValueSlot], bundle: ResolvedSourceBundle, base_deps: List[str],
    ) -> Tuple[exp.Select, List[CteEntry], List[str]]:
        """``_base`` over an inferred stage's row supply: the null-safe union of its branches' cells (each cast to
        its dimension's type, never chained FULL JOINs), left-joined to the host's own base, a local aggregate
        absent from a cell reading its empty value (semantics 12.4, Axiom 4). Returns ``(base, ctes, base deps)``."""
        allocator = self._gen_allocator or self._new_allocator()
        dims = [sid for sid in planned_query.grain or [] if aliases_by_slot_id.get(sid)]
        key_names = {sid: aliases_by_slot_id[sid][0] for sid in dims}
        # A cell plan spells a dimension by its alias, or by its field when it is a coarser grain's restriction.
        names = {
            name: sid for sid in dims
            for name in (slots_by_id[sid].declared_name, *slots_by_id[sid].public_aliases, slots_by_id[sid].public_name)
            if name
        }
        hoisted: List[CteEntry] = []

        def cells(plan) -> Tuple[Expression, Dict[str, str]]:
            """``plan`` as a derived table, and its column per main dimension slot."""
            ctes, body = self._render_producer_split(producer=plan, bundle=plan.stage_bundle or bundle)
            hoisted.extend(ctes)
            plan_slots = {s.id: s for s in (*plan.row_slots, *plan.aggregate_slots, *plan.combined_expression_slots)}
            outputs = [e.alias_or_name for e in body.expressions]
            if len(outputs) != len(plan.projection):
                raise ValueError("a row-source plan projects other values than its dimensions")
            public = {sid: plan_slots[sid].public_name or plan_slots[sid].declared_name for sid in plan.projection}
            if any(public[sid] not in names for sid in plan.projection):
                raise ValueError("a row-source plan projects a column that is no dimension of its stage")
            return body, {names[public[sid]]: col for sid, col in zip(plan.projection, outputs)}

        # A spine stage's rows are every bucket × its tuples' union (semantics Axiom 15).
        spine_sids = [sid for sid in dims if planned_query.spine is not None and _is_spine_bucket(slots_by_id[sid].key)]
        tuple_sids = [sid for sid in dims if sid not in spine_sids]
        branches = [
            self._row_supply_branch(index=i, branch=branch, cells=cells, dims=tuple_sids, key_names=key_names,
                                    slots_by_id=slots_by_id)
            for i, branch in enumerate(planned_query.row_supply.branches)
        ]
        union: Expression = branches[0]
        for b in branches[1:]:
            union = exp.union(union, b, distinct=True)
        series_mark = len(self._gen_spine_ctes)
        if spine_sids:
            union = self._spine_crossed(union=union, tuple_names=[key_names[sid] for sid in tuple_sids],
                                        buckets=[(key_names[sid], slots_by_id[sid].key) for sid in spine_sids])
        series_ctes = [e.name for e in self._gen_spine_ctes[series_mark:]]
        host, keys = allocator.allocate("_base_host"), allocator.allocate("_keys")
        local = [
            (sid, alias) for sid, als in aliases_by_slot_id.items() if sid not in key_names for alias in als
        ]
        on = build_grain_joinback_condition(pairs=self._keyed_pairs([
            (exp.column(key_names[sid], table=keys, quoted=True), exp.column(key_names[sid], table=host, quoted=True),
             slots_by_id[sid].key)
            for sid in dims
        ]), dialect=self._dialect)
        wrapped = exp.Select().select(
            *(exp.column(key_names[sid], table=keys, quoted=True).as_(alias, quoted=True)
              for sid in dims for alias in aliases_by_slot_id[sid]),
            *(self._with_empty(exp.column(alias, table=host, quoted=True), slot=slots_by_id[sid], bundle=bundle)
              .as_(alias, quoted=True) for sid, alias in local),
        ).from_(keys)
        wrapped = wrapped.join(host, on=on, join_type="LEFT") if on is not None else wrapped.join(host,
                                                                                                    join_type="CROSS")
        ctes = [
            *hoisted,
            CteEntry(name=host, query=base_select, depends_on=list(base_deps)),
            CteEntry(name=keys, query=union, depends_on=[*(e.name for e in hoisted), *series_ctes]),
        ]
        return wrapped, ctes, [host, keys]

    def _domain_base(
        self, *, planned_query, base_select: exp.Select, aliases_by_slot_id: Dict[str, List[str]],
        slots_by_id: Dict[str, ValueSlot], bundle: ResolvedSourceBundle, base_deps: List[str],
        joinbacks: Dict[str, List[Tuple[str, str, Optional[DataType]]]], cte_of: Dict[Any, str],
    ) -> Tuple[exp.Select, List[CteEntry], List[str]]:
        """``_base`` over a transform producer's domain: the union of its supplying constituents' cells (each cast to
        its dimension's type), left-joined to the host's own base (semantics Axiom 11.6). Returns ``(base, ctes, deps)``."""
        domain = planned_query.domain
        allocator = self._gen_allocator or self._new_allocator()
        dims = [sid for sid in planned_query.grain or [] if aliases_by_slot_id.get(sid)]
        key_names = {sid: aliases_by_slot_id[sid][0] for sid in dims}
        host, keys = allocator.allocate("_base_host"), allocator.allocate("_keys")

        def branch(table: str, key_of: Callable[[str], str]) -> exp.Select:
            return exp.Select().select(*(
                exp.Alias(
                    this=_wrap_cast_for_type(
                        dialect=self._dialect, expr=exp.column(key_of(sid), table=table, quoted=True),
                        dt=self._dialect.declared_cast_type(slots_by_id[sid].type),
                    ),
                    alias=exp.to_identifier(key_names[sid], quoted=True),
                )
                for sid in dims
            )).from_(table)

        branches: List[exp.Select] = [branch(host, key_names.__getitem__)] if domain.host else []
        sources: List[str] = []
        for ph in domain.placeholders:
            cte = cte_of[ph]
            by_host = {pair[0]: pair[1] for pair in joinbacks[cte]}
            branches.append(branch(cte, lambda sid, by_host=by_host: next(
                by_host[a] for a in aliases_by_slot_id[sid] if a in by_host)))
            sources.append(cte)
        union: Expression = branches[0]
        for b in branches[1:]:
            union = exp.union(union, b, distinct=True)
        if len(branches) == 1:
            union = cast(exp.Select, union).distinct()
        # Over the spine the domain is its tuples × every bucket in range (Axiom 11.6).
        spine_sids = [sid for sid in dims if planned_query.spine is not None and _is_spine_bucket(slots_by_id[sid].key)]
        series_mark = len(self._gen_spine_ctes)
        if spine_sids:
            union = self._spine_crossed(
                union=union, tuple_names=[key_names[sid] for sid in dims if sid not in spine_sids],
                buckets=[(key_names[sid], slots_by_id[sid].key) for sid in spine_sids])
        sources.extend(e.name for e in self._gen_spine_ctes[series_mark:])
        local = [(sid, alias) for sid, als in aliases_by_slot_id.items() if sid not in key_names for alias in als]
        wrapped = exp.Select().select(
            *(exp.column(key_names[sid], table=keys, quoted=True).as_(alias, quoted=True)
              for sid in dims for alias in aliases_by_slot_id[sid]),
            *(self._with_empty(exp.column(alias, table=host, quoted=True), slot=slots_by_id[sid], bundle=bundle)
              .as_(alias, quoted=True) for sid, alias in local),
        ).from_(keys)
        keys_entry = CteEntry(name=keys, query=union, depends_on=[*([host] if domain.host else []), *sources])
        if not local and not domain.host:
            # The host contributes neither cells nor values.
            return wrapped, [keys_entry], [keys]
        on = build_grain_joinback_condition(pairs=self._keyed_pairs([
            (exp.column(key_names[sid], table=keys, quoted=True), exp.column(key_names[sid], table=host, quoted=True),
             slots_by_id[sid].key)
            for sid in dims
        ]), dialect=self._dialect)
        wrapped = wrapped.join(host, on=on, join_type="LEFT")
        return wrapped, [CteEntry(name=host, query=base_select, depends_on=list(base_deps)), keys_entry], [host, keys]

    def _row_supply_branch(
        self, *, index: int, branch: PlannedRowBranch, cells: Callable[[Any], Tuple[Expression, Dict[str, str]]],
        dims: List[str], key_names: Dict[str, str], slots_by_id: Dict[str, ValueSlot],
    ) -> exp.Select:
        """One branch's cells, each dimension cast to its type, kept where every ``restrict`` plan has the cell."""
        body, by_dim = cells(branch.cells)
        if set(dims) - set(by_dim):
            raise ValueError("a row-source plan does not project every dimension of its stage")
        alias = f"_cells{index}"
        select = exp.Select().select(*(
            exp.Alias(
                this=_wrap_cast_for_type(
                    dialect=self._dialect, expr=exp.column(by_dim[sid], table=alias, quoted=True),
                    dt=self._dialect.declared_cast_type(slots_by_id[sid].type),
                ),
                alias=exp.to_identifier(key_names[sid], quoted=True),
            )
            for sid in dims
        )).from_(exp.Subquery(this=body, alias=exp.TableAlias(this=exp.to_identifier(alias))))
        for j, restrict in enumerate(branch.restrict):
            r_body, r_dim = cells(restrict)
            r_alias = f"_agree{index}_{j}"
            on = build_grain_joinback_condition(pairs=self._keyed_pairs([
                (exp.column(col, table=r_alias, quoted=True), exp.column(by_dim[sid], table=alias, quoted=True),
                 slots_by_id[sid].key)
                for sid, col in r_dim.items() if sid in by_dim
            ]), dialect=self._dialect)
            exists = exp.Select().select(exp.Literal.number(1)).from_(
                exp.Subquery(this=r_body, alias=exp.TableAlias(this=exp.to_identifier(r_alias))))
            select = select.where(exp.Exists(this=exists.where(on) if on is not None else exists))
        return select

    def _spine_crossed(
        self, *, union: Expression, tuple_names: List[str], buckets: List[Tuple[str, Any]],
    ) -> exp.Select:
        """Each distinct tuple of ``union`` crossed with every bucket of the spine series."""
        tuples = exp.Subquery(this=union, alias=exp.TableAlias(this=exp.to_identifier("_tuples")))
        series = exp.Select().select(*(
            self._build_date_trunc(col_expr=exp.column(TIME_SPINE_FIELD, table="_series", quoted=True), key=key)
            .as_(name, quoted=True)
            for name, key in buckets
        )).distinct().from_(self._spine_relation(alias="_series"))
        return exp.Select().select(
            *(exp.column(n, table="_tuples", quoted=True).as_(n, quoted=True) for n in tuple_names),
            *(exp.column(n, table="_buckets", quoted=True).as_(n, quoted=True) for n, _ in buckets),
        ).distinct().from_(tuples).join(
            exp.Subquery(this=series, alias=exp.TableAlias(this=exp.to_identifier("_buckets"))), join_type="CROSS")

    def _with_empty(self, value: Expression, *, slot: ValueSlot, bundle: ResolvedSourceBundle) -> Expression:
        """``value`` with a missing cell read as the slot's empty value (Axiom 4)."""
        key = slot.key
        if not isinstance(key, AggregateKey):
            return value
        owner = aggregation_owner(root=bundle.population, source=key.source, datasets_by_name=bundle.datasets_by_name)
        empty = self._empty_value_expr(empty_value(
            agg=key.agg, definition=aggregation_definition(owner=owner, agg=key.agg),
        ))
        return value if empty is None else exp.Coalesce(this=value, expressions=[empty])

    def _render_producer_split(
        self, *, producer, bundle, kernel=None,
    ) -> Tuple[List[CteEntry], exp.Select]:
        """Render a regroup producer as AST, split into (hoisted CTEs, body) — D2.
        A pushed registry scope captures the producer statement's declared CTE
        deps for :meth:`_split_ast_ctes`."""
        self._gen_dep_stack.append({})
        try:
            return self._split_ast_ctes(self._build_from_planned(
                producer, bundle=bundle, as_cte_body=True,
                reuse_allocator=True, producer_kernel=kernel,
            ))
        finally:
            self._gen_dep_stack.pop()

    @contextmanager
    def _stage_scope(self, relation: Optional[str]) -> Generator[None, None, None]:
        """One multi-stage statement's dep-registry frame; a non-root stage is also a split consumer."""
        self._gen_dep_stack.append({})
        if relation is not None:
            self._gen_split_consumers.append(relation)
        try:
            yield
        finally:
            if relation is not None:
                self._gen_split_consumers.pop()
            self._gen_dep_stack.pop()

    def _split_ast_ctes(
        self, parsed: exp.Select, *, root: bool = False,
    ) -> Tuple[List[CteEntry], exp.Select]:
        """Split a composed statement into (hoisted CTE entries, de-WITHed body).

        Each entry's ``depends_on`` comes from the top statement-scoped registry
        (declared at assembly time), re-keyed through the ``_base`` rename map so
        an edge onto a renamed base still resolves. The multi-stage ``root`` is the
        outermost consumer: its own top-level CTEs keep their names. Fails closed if
        one CTE name appears in two ``WITH`` nodes of the statement."""
        top = parsed.args.get("with_") if root else None
        with_nodes = list(parsed.find_all(exp.With))
        if not with_nodes:
            return [], parsed
        allocator = self._gen_allocator or self._new_allocator()
        captured = self._gen_dep_stack[-1] if self._gen_dep_stack else {}
        entries: List[CteEntry] = []
        seen: Set[str] = set()
        for with_node in with_nodes:
            rename = {} if with_node is top else self._uniquify_producer_base_ctes(
                with_node=with_node, allocator=allocator,
            )
            reverse = {new: old for old, new in rename.items()}
            for cte in with_node.expressions:
                name = cast(str, cte.alias_or_name)
                if name in seen:
                    raise ValueError(
                        f"CTE name {name!r} appears in two WITH nodes of one "
                        f"producer statement; cannot order it deterministically",
                    )
                seen.add(name)
                orig = reverse.get(name, name)
                deps = [rename.get(d, d) for d in captured.get(orig, ())]
                entries.append(cte_entry(cte=cte, name=name, depends_on=deps))
            with_node.pop()
        return entries, parsed

    def _split_root_ctes(
        self, parsed: exp.Select,
    ) -> Tuple[List[CteEntry], exp.Select]:
        """Split the multi-stage ROOT statement into (its CTE entries, de-WITHed body)."""
        return self._split_ast_ctes(parsed, root=True)

    @staticmethod
    def _uniquify_producer_base_ctes(*, with_node, allocator) -> Dict[str, str]:  # NOSONAR(S3776) — one rename pass; the collect / table-ref / column-qualifier / cte-alias rewrites share the rename map.
        """Rename a hoisted producer's hardcoded base CTE(s) (``_base``/``base``),
        returning the ``{old: new}`` rename map so a caller can re-key declared deps."""
        parsed = with_node.parent
        rename: Dict[str, str] = {}
        for cte in with_node.expressions:
            name = cte.alias_or_name
            if name in ("_base", "base") and name not in rename:
                rename[name] = allocator.allocate_cte(name)
        if not rename:
            return rename
        # A nested windowed producer aliases its inline grain subquery _base, shadowing a same-named hoisted CTE; the
        # CTE rename must skip refs bound to the local subquery.
        shadow: Dict[str, set] = {name: set() for name in rename}
        for subq in parsed.find_all(exp.Subquery):
            if subq.alias in rename and subq.parent_select is not None:
                shadow[subq.alias].add(id(subq.parent_select))

        def _shadowed(node, name: str) -> bool:
            sel = node.parent_select
            return sel is not None and id(sel) in shadow[name]

        for tbl in parsed.find_all(exp.Table):
            ident = tbl.this
            if (
                isinstance(ident, exp.Identifier)
                and tbl.name in rename
                and tbl.args.get("db") is None
                and not _shadowed(tbl, tbl.name)
            ):
                tbl.set("this", exp.to_identifier(
                    rename[tbl.name], quoted=ident.quoted,
                ))
        for col in parsed.find_all(exp.Column):
            tref = col.args.get("table")
            if (
                isinstance(tref, exp.Identifier)
                and tref.name in rename
                and not _shadowed(col, tref.name)
            ):
                col.set("table", exp.to_identifier(
                    rename[tref.name], quoted=tref.quoted,
                ))
        for cte in with_node.expressions:
            alias = cte.args.get("alias")
            ident = alias.this if isinstance(alias, exp.TableAlias) else None
            if isinstance(ident, exp.Identifier) and ident.name in rename:
                alias.set("this", exp.to_identifier(
                    rename[ident.name], quoted=ident.quoted,
                ))
        return rename

    def _prepare_combined_regroup_attaches(  # NOSONAR(S3776) — one cohesive combined-attach render (producer CTE → placeholder env → join-back); the phases share local state and reads clearer inline.
        self, *, planned_query: PlannedQuery, bundle, source_relation, slot_by_key,
    ):
        """Render each combined regroup producer as a ``_cm_*`` CTE."""
        ctes: List[Node] = []
        placeholder_to_cm: Dict[Any, AttachedValue] = {}
        placeholder_slot_ids: Set[str] = set()
        joinbacks: List[Tuple[str, List[Tuple[str, str, Optional[DataType]]]]] = []
        shift_specs: List[Tuple[str, List[Tuple[Any, str, Optional[DataType]]]]] = []
        reused_cte_names: List[str] = []
        allocator = self._gen_allocator or self._new_allocator()
        for attach in planned_query.regroup_attach_plans:
            if attach.attach_phase != "combined":
                continue
            # A producer already rendered in any scope of this generation reuses its CTE; checked before minting so no
            # _cm_ suffix is burned.
            rendered_map = self._gen_rendered_producers
            ident = (
                regroup_producer_identity(attach)
                if rendered_map is not None else None
            )
            rec = rendered_map.get(ident) if rendered_map is not None else None
            if rec is not None:
                cte_name, col_by_sid = rec
                reused_cte_names.append(cte_name)
                self._record_reuse_edges(cte_name)
            else:
                seed_key = attach.substitutions[0].original_key
                cte_name = cte_name_from_alias(
                    prefix="_cm_",
                    alias=self._canonical_cross_dataset_alias(
                        source_relation=source_relation, key=seed_key,
                    ) if isinstance(seed_key, AggregateKey) else attach.alias_hint,
                    allocator=allocator, dialect=self.dialect,
                    limit=self._dialect.max_identifier_bytes,
                )
                entries, col_by_sid = self._producer_ctes(
                    attach=attach, bundle=bundle, cte_name=cte_name,
                )
                ctes.extend(entries)
                if rendered_map is not None:
                    rendered_map[ident] = (cte_name, col_by_sid)
            for sub in attach.substitutions:
                agg_col = col_by_sid.get(sub.producer_slot_id)
                if agg_col is None:
                    raise RuntimeError(
                        f"Combined regroup producer is missing aggregate slot "
                        f"{sub.producer_slot_id!r}.",
                    )
                placeholder_to_cm[sub.placeholder] = AttachedValue(
                    cte_name=cte_name, field_name=agg_col, empty=self._empty_value_expr(sub.empty_value),
                )
                ph_slot = slot_by_key.get(sub.placeholder)
                if ph_slot is not None:
                    placeholder_slot_ids.add(ph_slot.id)
            # A combined attach carries the RAW grain key while the host slot carries the desugared one; desugar
            # host_key with the same map so slot_by_key finds it.
            row_desugar_map = {
                sub.original_key: sub.placeholder
                for a in planned_query.regroup_attach_plans
                if a.attach_phase == "row"
                for sub in a.substitutions
            }
            pairs: List[Tuple[str, str, Optional[DataType]]] = []
            shift_pairs: List[Tuple[Any, str, Optional[DataType]]] = []
            producer_types = self._producer_slot_types(attach)
            for host_key, producer_slot_id in attach.join_pairs:
                grain_alias = col_by_sid.get(producer_slot_id)
                host_slot = slot_by_key.get(host_key)
                if host_slot is None and row_desugar_map:
                    host_slot = slot_by_key.get(
                        substitute_value_keys(key=host_key, mapping=row_desugar_map),
                    )
                if grain_alias is None or host_slot is None:
                    raise RuntimeError(
                        "Combined regroup attach is missing a host / producer "
                        "grain slot for its join-back.",
                    )
                host_alias = self._full_alias_for_slot(
                    slot=host_slot, source_relation=source_relation, alias_index={},
                )
                producer_type = producer_types.get(producer_slot_id)
                pairs.append((host_alias, grain_alias, producer_type))
                shift_pairs.append((host_slot.key, grain_alias, producer_type))
            joinbacks.append((cte_name, pairs))
            shift_specs.append((cte_name, shift_pairs))
        return (
            ctes, placeholder_to_cm, placeholder_slot_ids, joinbacks, shift_specs,
            reused_cte_names,
        )

    def _record_reuse_edges(self, shared_cte: str) -> None:
        """A reuse under an active producer split: every enclosing"""
        for consumer in self._gen_split_consumers:
            self._gen_reuse_deps.setdefault(consumer, set()).add(shared_cte)

    def _reuse_deps_of(self, cte_name: str) -> List[str]:
        return sorted(self._gen_reuse_deps.get(cte_name, ()))

    def _external_cte_names(self) -> frozenset:
        """Shared producer CTE names of this generation — legal dependency"""
        if not self._gen_rendered_producers:
            return frozenset()
        return frozenset(
            name for name, _ in self._gen_rendered_producers.values()
        )

    @staticmethod
    def _regroup_attach_identity(attach: RegroupAttachPlan):
        """Structural identity of a regroup attach's producer: the"""
        return (
            frozenset(sub.original_key for sub in attach.substitutions),
            frozenset(host_key for host_key, _ in attach.join_pairs),
        )

    def _prepare_regroup_attaches(  # NOSONAR(S3776) — one linear pass over the planned regroup producers (dedup → render → hoist); splitting would thread the CTE registry through every helper
        self, *, planned_query: PlannedQuery, bundle, dedup_producers=None,
    ):
        """Render each regroup producer as a ``_cm_*`` CTE."""
        dedup_producers = dedup_producers or {}
        ctes: List[CteEntry] = []
        attached_env: Dict[Any, Expression] = {}
        join_specs: List[Tuple[str, List[Tuple[Any, str, Optional[DataType]]]]] = []
        reused_cte_names: List[str] = []
        allocator = self._gen_allocator or self._new_allocator()
        for attach in planned_query.regroup_attach_plans:
            if attach.attach_phase != "row":
                continue
            dedup = dedup_producers.get(self._regroup_attach_identity(attach))
            if dedup is not None:
                dedup_cte, okey_to_col, grain_pairs = dedup
                for sub in attach.substitutions:
                    attached_env[sub.placeholder] = AttachedValue(
                        cte_name=dedup_cte, field_name=okey_to_col[sub.original_key],
                        empty=self._empty_value_expr(sub.empty_value),
                    ).value()
                join_specs.append((dedup_cte, list(grain_pairs)))
                reused_cte_names.append(dedup_cte)
                continue
            rendered_map = self._gen_rendered_producers
            ident = (
                regroup_producer_identity(attach)
                if rendered_map is not None else None
            )
            rec = rendered_map.get(ident) if rendered_map is not None else None
            if rec is not None:
                shared_cte, col_by_sid = rec
                self._record_reuse_edges(shared_cte)
                for sub in attach.substitutions:
                    attached_env[sub.placeholder] = AttachedValue(
                        cte_name=shared_cte, field_name=col_by_sid[sub.producer_slot_id],
                        empty=self._empty_value_expr(sub.empty_value),
                    ).value()
                producer_types = self._producer_slot_types(attach)
                join_specs.append((shared_cte, [
                    (host_key, col_by_sid[producer_slot_id], producer_types.get(producer_slot_id))
                    for host_key, producer_slot_id in attach.join_pairs
                ]))
                reused_cte_names.append(shared_cte)
                continue
            cte_name = cte_name_from_alias(
                prefix="_cm_", alias=attach.alias_hint, allocator=allocator,
                dialect=self.dialect, limit=self._dialect.max_identifier_bytes,
            )
            producer = attach.producer_plan
            relation = producer.source_relation
            sub_slots = {
                s.id: s
                for s in (
                    list(producer.row_slots)
                    + list(producer.aggregate_slots)
                    + list(producer.combined_expression_slots)
                )
            }
            def _flat(slot) -> str:
                return flat_name(
                    self._full_alias_for_slot(slot=slot, source_relation=relation, alias_index={}),
                    strip_relation=relation,
                )

            self._gen_split_consumers.append(cte_name)
            try:
                producer_hoisted, producer_body = self._render_producer_split(
                    producer=producer,
                    bundle=self._producer_render_bundle(
                        attach=attach, bundle=bundle,
                    ),
                    kernel=attach.kernel,
                )
            finally:
                self._gen_split_consumers.pop()
            expected = [
                _flat(sub_slots[sid]) for sid in producer.projection
                if sid in sub_slots
            ]
            wrapped = build_flat_rename_wrapper(
                stage=cte_name, source_relation=relation, inner=producer_body,
                expected_columns=expected, dialect=self.dialect,
            )
            for h in producer_hoisted:
                ctes.append(Node(
                    name=h.name, phase="producer", query=h.query,
                    depends_on=[*h.depends_on, *self._reuse_deps_of(h.name)],
                ))
            exposed_by_sid = {sid: _flat(slot) for sid, slot in sub_slots.items()}
            ctes.append(Node(
                name=cte_name, phase="producer", query=wrapped,
                depends_on=[
                    *[h.name for h in producer_hoisted],
                    *self._reuse_deps_of(cte_name),
                ],
                schema_by_slot={sid: [col] for sid, col in exposed_by_sid.items()},
            ))
            for sub in attach.substitutions:
                agg_slot = sub_slots.get(sub.producer_slot_id)
                if agg_slot is None:
                    raise RuntimeError(
                        f"Regroup producer is missing aggregate slot "
                        f"{sub.producer_slot_id!r}.",
                    )
                attached_env[sub.placeholder] = AttachedValue(
                    cte_name=cte_name, field_name=_flat(agg_slot),
                    empty=self._empty_value_expr(sub.empty_value),
                ).value()
            pairs: List[Tuple[Any, str, Optional[DataType]]] = []
            for host_key, producer_slot_id in attach.join_pairs:
                grain_slot = sub_slots.get(producer_slot_id)
                if grain_slot is None:
                    raise RuntimeError(
                        f"Regroup producer is missing grain slot "
                        f"{producer_slot_id!r}.",
                    )
                pairs.append((host_key, _flat(grain_slot), grain_slot.type))
            join_specs.append((cte_name, pairs))
            if rendered_map is not None:
                rendered_map[ident] = (cte_name, exposed_by_sid)
        return ctes, attached_env, join_specs, reused_cte_names

    @staticmethod
    def _producer_slot_types(attach: Any) -> Dict[str, Optional[DataType]]:
        """Each producer slot's recorded type."""
        plan = attach.producer_plan
        return {
            s.id: s.type
            for s in (*plan.row_slots, *plan.aggregate_slots, *plan.combined_expression_slots)
        }

    def _grain_key(self, expr: Expression, *, key: Any) -> Expression:
        """What grouping, partitioning and join-backs read of grain member ``key``: an element set's list key."""
        return self._dialect.build_list_key(expr.copy()) if isinstance(key, ListSetKey) else expr

    def _keyed_pairs(
        self, pairs: Sequence[Tuple[Expression, Expression, Any]], *,
        right_types: Optional[Sequence[Optional[DataType]]] = None,
    ) -> List[GrainPair]:
        """Grain join-back operands ``(left, right, member key)`` typed by the key (``right_types``: the right
        side's own recorded types, where known), an element set read through its list key."""
        out: List[GrainPair] = []
        for i, (left, right, k) in enumerate(pairs):
            left_type = self._key_type(k)
            right_type = right_types[i] if right_types is not None and right_types[i] is not None else left_type
            out.append((self._grain_key(left, key=k), left_type, self._grain_key(right, key=k), right_type))
        return out

    def _key_type(self, key: Any) -> Optional[DataType]:
        """``key``'s type under the active root's column types."""
        if key is None:
            return None
        return value_type(key, field_type=self._current_field_types() or _no_field_type)

    def _field_types(self) -> FieldTypeFn:
        """The active root's column types; rendering a bucket outside a ``generate()`` call is a wiring bug."""
        field_type = self._current_field_types()
        if field_type is None:
            raise RuntimeError("A bucket rendered outside a generate() call has no column types.")
        return field_type

    def _grouped_display(self, expr: Expression, *, key: Any) -> Expression:
        """A grouped member's projection: its value, any one of its cell's where its key is another expression."""
        if not isinstance(key, ListSetKey) or self._dialect.build_list_key(expr) is expr:
            return expr.copy()
        return exp.AnyValue(this=expr.copy())

    def _full_alias_for_slot(
        self,
        *,
        slot,
        source_relation: str,
        alias_index: Dict[str, int],
    ) -> str:
        """Build the SQL public alias for one ``ValueSlot``."""
        return next_slot_result_key(
            slot=slot, alias_index=alias_index, source_relation=source_relation,
        )

    def _collect_joined_paths_for_base(
        self,
        *,
        base_render_order: List[str],
        slots_by_id: Dict[str, Any],
        order_slot_ids: Optional[List[str]] = None,
    ) -> List[Tuple[str, ...]]:
        """Walk ROW slots in render order to collect unique joined DIMENSION"""

        seen: set = set()
        ordered: List[Tuple[str, ...]] = []

        def _add(path: Tuple[str, ...]) -> None:
            if not path or path in seen:
                return
            seen.add(path)
            ordered.append(path)

        def _add_row_slot(sid: str) -> None:
            slot = slots_by_id.get(sid)
            if slot is None or slot.phase != Phase.ROW:
                return
            key = slot.key
            if isinstance(key, FieldKey):
                _add(key.path)
            elif isinstance(key, TimeTruncKey) and isinstance(key.source, (FieldKey, FieldSqlKey)):
                _add(field_path(key.source))

        for sid in base_render_order:
            _add_row_slot(sid)
        for sid in order_slot_ids or ():
            _add_row_slot(sid)
        return ordered

    def _oriented_hop_chain(
        self, *, source_dataset: SlayerDataset, path, bundle: ResolvedSourceBundle,
    ) -> List[Tuple[OrientedRelationship, SlayerDataset]]:
        """``(OrientedRelationship, next_dataset)`` hops of a bound (possibly composed) relationship ``path``;
        ``ValueError`` when it does not resolve, ``AmbiguousRelationshipPathError`` on an ambiguous hop."""
        datasets_by_name = bundle.datasets_by_name
        datasets_by_name.setdefault(source_dataset.name, source_dataset)
        chain = follow(root=source_dataset, path=tuple(path), datasets_by_name=datasets_by_name)
        if chain is None:
            raise ValueError(
                f"Joined path {tuple(path)!r} does not resolve from dataset {source_dataset.name!r}.",
            )
        return [(edge, datasets_by_name[edge.to]) for edge in chain]

    def _build_from_and_joins(
        self,
        *,
        source_dataset: SlayerDataset,
        source_relation: str,
        joined_paths: List[Tuple[str, ...]],
        bundle: ResolvedSourceBundle,
    ):
        """Build ``(from_expr, joins)`` for a base SELECT; a list child root reads its parent's own rows."""
        joins: List = []
        emitted_aliases: set = {source_relation}
        if source_dataset.list_of is not None:
            parent_name, field = source_dataset.list_of
            parent = bundle.datasets_by_name[parent_name]
            parent_alias = self._join_alias(root=source_relation, path=(field,))
            base_from = self._emit_relation(dataset=parent, alias=parent_alias, bundle=bundle)
            joins.append(self._list_join(
                parent=parent, field=field, parent_alias=parent_alias, alias=source_relation, bundle=bundle,
                positioned=source_dataset.get_field(LIST_POSITION) is not None,
            ))
            emitted_aliases.add(parent_alias)
        else:
            base_from = self._emit_relation(dataset=source_dataset, alias=source_relation, bundle=bundle)
        if not joined_paths:
            return base_from, joins
        for path in joined_paths:
            current_alias = source_relation
            chain = self._oriented_hop_chain(
                source_dataset=source_dataset, path=path, bundle=bundle,
            )
            prev_dataset = source_dataset
            for hop_idx, (edge, next_dataset) in enumerate(chain):
                next_alias = self._join_alias(
                    root=source_relation, path=path[: hop_idx + 1],
                )
                if next_alias not in emitted_aliases:
                    joins.append(self._hop_join(
                        edge=edge, prev_dataset=prev_dataset, next_dataset=next_dataset,
                        current_alias=current_alias, next_alias=next_alias, bundle=bundle,
                    ))
                    emitted_aliases.add(next_alias)
                current_alias = next_alias
                prev_dataset = next_dataset
        return base_from, joins

    def _list_relation(
        self, *, parent: SlayerDataset, field: str, parent_alias: str, alias: str, bundle, leading: bool,
        positioned: bool = False,
    ):
        """``parent``'s list ``field`` (read at ``parent_alias``) as its element relation ``alias``."""
        col = parent.get_field(field)
        assert col is not None and col.datatype is DataType.ARRAY, (parent.name, field)
        frame = self._throwaway_frame(dataset=parent, relation=parent_alias, bundle=bundle)
        values = frame.resolve(
            FieldSqlKey(dataset=parent.name, field_name=field) if col.needs_expansion else FieldKey(leaf=field),
        )
        return self._dialect.list_relation(
            values, alias=exp.to_identifier(alias), element=col.element_type, leading=leading, positioned=positioned,
        )

    def _list_join(
        self, *, parent, field: str, parent_alias: str, alias: str, bundle, positioned: bool = False,
    ):
        relation = self._list_relation(
            parent=parent, field=field, parent_alias=parent_alias, alias=alias, bundle=bundle, leading=False,
            positioned=positioned,
        )
        return relation.relation, None, relation.join_kind

    def _join_key(self, *, field: DatasetField, alias: str) -> Expression:
        # Table qualifiers are internal aliases.
        if not field.is_base:
            return self._computed_key(text=field.definition_sql or field.name, alias=alias)
        key = exp.Column(this=key_identifier(field.physical_name, quoted=field.physical_quoted),
                         table=exp.to_identifier(alias))
        return self._dialect.read_column(key, zoned=self._dialect.is_zoned_db_type(field.db_type))

    def _computed_key(self, *, text: str, alias: str) -> Expression:
        """A computed relationship key: its expression over ``alias``'s source columns."""
        key = self._parse(text)
        for column in key.find_all(exp.Column):
            if not column.table:
                column.set("table", exp.to_identifier(alias))
        return key

    def _hop_join(
        self, *, edge: OrientedRelationship, prev_dataset: SlayerDataset, next_dataset: SlayerDataset, current_alias: str,
        next_alias: str, bundle,
    ) -> Tuple[Expression, Optional[exp.Condition], str]:
        """``(join_expr, on_expr, join_type)`` for one oriented hop; an unrelated product hop is crossed only under a
        product anchor (semantics 12.2), else fails closed."""
        if is_unrelated_edge(edge):
            if bundle.row_supply is not None and bundle.row_supply.product:
                return self._emit_relation(dataset=next_dataset, alias=next_alias, bundle=bundle), None, "CROSS"
            raise ValueError(f"The product hop {edge.source_dataset} × {edge.to} cannot be joined.")
        if edge.lateral is not None:
            if next_dataset.list_of is None:
                raise ValueError(f"The hop from list child {prev_dataset.name!r} back to its parent is its own row.")
            return self._list_join(
                parent=prev_dataset, field=edge.lateral, parent_alias=current_alias, alias=next_alias, bundle=bundle,
            )
        join_on_parts = [
            _key_equality(
                self._join_key(field=src_col, alias=current_alias),
                self._join_key(field=tgt_col, alias=next_alias),
            )
            for src_col, tgt_col in relationship_key_fields(
                edge=edge, source=prev_dataset, target=next_dataset,
            )
        ]
        join_expr = self._emit_relation(dataset=next_dataset, alias=next_alias, bundle=bundle)
        if not join_on_parts:
            return join_expr, None, "CROSS"  # the population's product with its spine
        if provably_to_one(edge=edge, target_dataset=next_dataset) and not trusted_to_one(
                edge=edge, target_dataset=next_dataset):
            mark_untrusted(join_expr)
        on_expr = exp.and_(*join_on_parts) if len(join_on_parts) > 1 else join_on_parts[0]
        # Root-relative join type: LEFT keeps the querying root whole in the traversal
        # direction, INNER is symmetric; RIGHT is never emitted.
        return join_expr, on_expr, edge.join_type.value.upper()

    def _joined_or_local_dim_expr(
        self,
        *,
        path: Tuple[str, ...],
        leaf: str,
        source_dataset: SlayerDataset,
        source_relation: str,
        bundle,
    ) -> Expression:
        """Resolve a dimension field expression on either the host"""
        if not path:
            return self._dim_field_expr_from_planned(
                source_dataset=source_dataset,
                source_relation=source_relation,
                leaf=leaf,
            )
        current_alias = source_relation
        chain = self._oriented_hop_chain(
            source_dataset=source_dataset, path=path, bundle=bundle,
        )
        current_dataset = source_dataset
        for hop_idx, (_edge, next_dataset) in enumerate(chain):
            current_alias = self._join_alias(
                root=source_relation, path=path[: hop_idx + 1],
            )
            current_dataset = next_dataset
        col_def = next(
            (c for c in current_dataset.fields if c.name == leaf), None,
        )
        if col_def is None:
            raise ValueError(
                f"Field {leaf!r} not found on joined dataset "
                f"{current_dataset.name!r}.",
            )
        column = exp.Column(this=exp.to_identifier(leaf), table=exp.to_identifier(current_alias))
        return self._dialect.read_column(column, zoned=self._dialect.is_zoned_db_type(col_def.db_type))

    def _rank_family_window(
        self, *, fn: Expression, metric: Expression, partition_by: Iterable[Expression], descending: bool,
        ties: Sequence[Expression] = (),
    ) -> Expression:
        """``CASE WHEN v IS NULL THEN NULL ELSE fn OVER (PARTITION BY …, <v-is-null flag> ORDER BY v) END``: NULL rows rank NULL, outside the others' window."""
        null_flag = exp.Case(
            ifs=[exp.If(this=exp.Is(this=metric.copy(), expression=exp.Null()), true=exp.Literal.number(1))],
            default=exp.Literal.number(0),
        )
        window = exp.Window(
            this=fn,
            partition_by=[*(c.copy() for c in partition_by), null_flag],
            order=exp.Order(expressions=[self._window_ordered(metric.copy(), descending=descending),
                                         *(self._window_ordered(t.copy()) for t in ties)]),
        )
        return exp.Case(
            ifs=[exp.If(this=exp.Is(this=metric.copy(), expression=exp.Null()), true=exp.Null())],
            default=window,
        )

    def _window_ordered(self, col: Expression, *, descending: bool = False) -> exp.Ordered:
        """One ``ORDER BY`` term INSIDE an ``OVER (…)`` clause."""
        args: Dict[str, Any] = {
            "this": col,
            "nulls_first": self._dialect.native_nulls_first(
                descending=descending,
            ),
        }
        if descending:
            args["desc"] = True
        return exp.Ordered(**args)

    @staticmethod
    def _transform_grain_slot_ids(*, planned_query, slots_by_id, axis: object = None) -> List[str]:
        """The transform auto-grain: every grain dimension, and every grain time dimension off the axis's field."""
        axis_field = axis.source if isinstance(axis, TimeTruncKey) else None
        combined_placeholders = {
            sub.placeholder
            for plan in planned_query.regroup_attach_plans
            if plan.attach_phase == "combined"
            for sub in plan.substitutions
        }
        return [
            sid for sid in planned_query.grain or ()
            if (slot := slots_by_id.get(sid)) is not None and slot.phase == Phase.ROW
            and slot.key not in combined_placeholders
            and SQLGenerator._in_transform_grain(slot=slot, axis_field=axis_field)
        ]

    @staticmethod
    def _in_transform_grain(*, slot, axis_field: object) -> bool:
        """A row slot's grain membership: a dimension, or a time dimension off the axis's field."""
        key = slot.key
        if isinstance(key, TimeTruncKey):
            return axis_field is not None and key.source != axis_field
        return isinstance(key, (FieldKey, FieldSqlKey)) or slot.is_dimension

    def _render_window_transform_sql(  # NOSONAR(S3776) — one per-op dispatch over the window-transform vocabulary, sharing the resolved metric / frame / partition state every arm reads. Each arm is one line; splitting the dispatch scatters that state without simplifying it.
        self,
        *,
        slot,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
        planned_query,
    ) -> Expression:
        """Render one window-transform slot as an ``OVER()`` expression."""

        key = slot.key
        if not isinstance(key, TransformKey):
            raise ValueError(
                f"_render_window_transform_sql expected TransformKey, "
                f"got {type(key).__name__}",
            )

        metric = render_value_key(
            key=key.input,
            ctx=self._alias_render_ctx(
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            ),
        )

        time_col: Optional[Expression] = None
        if key.time_key is not None:
            tk_sid = slot_id_by_key.get(key.time_key)
            if tk_sid is None or tk_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"transform time_key not materialised: "
                    f"slot id={slot.id!r}, op={key.op!r}, "
                    f"time_key={key.time_key!r}.",
                )
            time_col = exp.column(
                available_alias_by_slot_id[tk_sid], quoted=True,
            )

        # Explicit partition_keys win; otherwise auto-partition by query dimension slots (FieldKey row-phase), never
        # TimeTruncKey.
        partition_members: list[Tuple[str, Any]] = []
        if key.partition_keys:
            for pk in sorted(
                key.partition_keys, key=lambda k: repr(k),
            ):
                pk_sid = slot_id_by_key.get(pk)
                if pk_sid is None or pk_sid not in available_alias_by_slot_id:
                    raise RuntimeError(
                        f"transform partition_key not materialised: "
                        f"slot id={slot.id!r}, op={key.op!r}, "
                        f"partition_key={pk!r}.",
                    )
                partition_members.append((available_alias_by_slot_id[pk_sid], pk))
        elif key.op not in RANK_FAMILY_TRANSFORMS:
            for sid in self._transform_grain_slot_ids(
                planned_query=planned_query, slots_by_id=slots_by_id, axis=key.time_key,
            ):
                alias = available_alias_by_slot_id.get(sid)
                if alias is not None:
                    partition_members.append((alias, slots_by_id[sid].key))

        partition_by = [self._grain_key(exp.column(a, quoted=True), key=k) for a, k in partition_members]

        def _over(
            fn: Expression,
            *,
            order: Optional[exp.Order] = None,
            spec: Optional[exp.WindowSpec] = None,
        ) -> exp.Window:
            """``fn OVER (PARTITION BY … ORDER BY … <frame>)``."""
            args: Dict[str, Any] = {"this": fn}
            if partition_by:
                args["partition_by"] = [c.copy() for c in partition_by]
            if order is not None:
                args["order"] = order
            if spec is not None:
                args["spec"] = spec
            return exp.Window(**args)

        # A NULL time bucket has no position on the axis (Axiom 11.3): a series of its own, skipped by a pick.
        null_bucket = (
            exp.Case(ifs=[exp.If(this=exp.Is(this=time_col.copy(), expression=exp.Null()),
                                 true=exp.Literal.number(1))], default=exp.Literal.number(0))
            if time_col is not None else None
        )
        if null_bucket is not None and key.op in _ROW_STEP_TRANSFORMS:
            partition_by = [*partition_by, null_bucket]
        time_order = (
            exp.Order(expressions=[self._window_ordered(time_col.copy())])
            if time_col is not None
            else None
        )
        unbounded_frame = exp.WindowSpec(
            kind="ROWS",
            start="UNBOUNDED", start_side="PRECEDING",
            end="UNBOUNDED", end_side="FOLLOWING",
        )

        kwarg_map = dict(key.kwargs)
        op = key.op

        def _normalise_periods(raw: Any, *, kw: str = "periods") -> int:
            """Reject bool / non-integral periods; accept int / integral"""
            if isinstance(raw, bool):
                raise ValueError(
                    f"transform {op!r} kwarg {kw!r} must be an integer; "
                    f"got bool {raw!r}.",
                )
            if isinstance(raw, int):
                return int(raw)
            if isinstance(raw, Decimal):
                if raw != raw.to_integral_value():
                    raise ValueError(
                        f"transform {op!r} kwarg {kw!r} must be an "
                        f"integer; got {raw!r}.",
                    )
                return int(raw)
            raise ValueError(
                f"transform {op!r} kwarg {kw!r} must be an integer; "
                f"got {type(raw).__name__} {raw!r}.",
            )

        if op == "cumsum":
            return _over(exp.Sum(this=metric), order=time_order)
        if op == "lag":
            n = abs(_normalise_periods(kwarg_map.get("periods", 1)))
            return _over(
                exp.Lag(this=metric, offset=exp.Literal.number(n)),
                order=time_order,
            )
        if op == "lead":
            n = abs(_normalise_periods(kwarg_map.get("periods", 1)))
            return _over(
                exp.Lead(this=metric, offset=exp.Literal.number(n)),
                order=time_order,
            )
        if op == "ntile":
            # Route through the shared normaliser (like lag/lead) so bool is rejected and a non-integral Decimal raises
            # rather than truncating; render-side defense.
            n = _normalise_periods(raw=kwarg_map.get("n"), kw="n")
            if n <= 0:
                raise ValueError(
                    f"ntile requires a positive integer n, got {n!r}",
                )
            # ntile splits ties: ordered by the cell too, so a cell's tile never depends on the plan.
            return self._rank_family_window(
                fn=exp.Ntile(this=exp.Literal.number(n)), metric=metric,
                partition_by=partition_by, descending=False,
                ties=[exp.column(available_alias_by_slot_id[sid], quoted=True) for sid in planned_query.grain or ()
                      if sid in available_alias_by_slot_id and slots_by_id[sid].phase == Phase.ROW],
            )
        if op in _RANK_WINDOW_FNS:
            return self._rank_family_window(
                fn=_RANK_WINDOW_FNS[op](), metric=metric, partition_by=partition_by,
                descending=kwarg_map.get("direction") == "desc",
            )
        # A pick never takes a NULL bucket's value: with no positioned candidate it reads NULL.
        positioned = (
            exp.Case(ifs=[exp.If(this=exp.Is(this=time_col.copy(), expression=exp.Null()), true=exp.Null())],
                     default=metric)
            if time_col is not None else metric
        )
        if op == "first":
            assert null_bucket is not None and time_col is not None
            return _over(
                exp.FirstValue(this=positioned),
                order=exp.Order(expressions=[self._window_ordered(null_bucket.copy()),
                                             self._window_ordered(time_col.copy())]),
                spec=unbounded_frame,
            )
        if op == "last":
            if time_col is None:
                raise ValueError(
                    f"Transform 'last' requires an unambiguous time "
                    f"dimension (binder/planner gap; slot id={slot.id!r}).",
                )
            # last is first over the reversed time axis, so it takes the descending order.
            assert null_bucket is not None
            return _over(
                exp.FirstValue(this=positioned),
                order=exp.Order(expressions=[
                    self._window_ordered(null_bucket.copy()),
                    self._window_ordered(time_col.copy(), descending=True),
                ]),
                spec=unbounded_frame,
            )
        # Total-dispatch backstop: the 9 window ops above exhaust the vocabulary
        # minus the desugared change/change_pct and the two dedicated emitters.
        raise NotImplementedError(
            f"window-transform dispatch has no arm for op {op!r}.",
        )

    def _render_post_phase_filter_conditions(  # NOSONAR(S3776) — one cohesive walk of every POST-phase filter producing the outer-WHERE conditions: per-filter slot-id lookup, expr rebuild (Compare / BoolOp / UnaryOp / scalar wraps), alias resolution. Splitting hides the shared registry / alias-map state both wrap-CTE and outer-WHERE emission depend on.
        self,
        *,
        planned_query,
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
    ) -> List[Expression]:  # pyright: ignore[reportPrivateImportUsage]
        """Each POST-phase lowered mask as a grouped conjunct over the chain's carried aliases."""

        out: List[Expression] = []
        for fp in _lower_positions(planned_query).filters:
            if fp.phase != Phase.POST:
                continue
            rendered = render_value_key(
                key=fp.expression.value_key,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                ),
            )
            out.append(_grouped(rendered))
        return out

    def _planned_order_terms(
        self,
        *,
        planned_query,
        slots_by_id: Dict[str, Any],
        available_alias_by_slot_id: Dict[str, str],
    ) -> List[exp.Ordered]:
        """ORDER BY terms for a plan whose sort keys resolve to CTE-chain"""
        env = OrderEnv.uniform(
            {
                sid: exp.column(alias, quoted=True)
                for sid, alias in available_alias_by_slot_id.items()
                if sid in slots_by_id
            },
            dialect=self._dialect,
        )
        return [
            term for entry in _lower_positions(planned_query).order
            for term in resolve_order_term(entry=entry, env=env)
        ]

    def _series_shift_cte(
        self, *, slot, chain: ChainState, render: RenderState, chain_tail: str,
        time_alias: str, cte_name_alias: str,
    ) -> Tuple[str, str, List[Tuple[str, str]]]:
        """The series regime's shifted relation: the input's materialised series read
        off the chain tail, keyed by the unshifted bucket and projected grain."""
        planned_query = render.planned_query
        slots_by_id = chain.slots_by_id
        available = chain.available_alias_by_slot_id
        grain_sids = set(self._transform_grain_slot_ids(
            planned_query=planned_query, slots_by_id=slots_by_id,
        ))
        pk_aliases: List[str] = []
        for sid in planned_query.projection:
            dim_slot = slots_by_id.get(sid)
            if dim_slot is None or dim_slot.phase != Phase.ROW:
                continue
            if sid not in grain_sids and not isinstance(dim_slot.key, TimeTruncKey):
                continue
            alias = available.get(sid)
            if alias is None:
                raise RuntimeError(
                    f"time_shift query dimension not materialised: slot id={slot.id!r}.",
                )
            if alias != time_alias and alias not in pk_aliases:
                pk_aliases.append(alias)
        input_sid = chain.slot_id_by_key.get(slot.key.input)
        value_alias = (
            available.get(input_sid) if input_sid is not None else None
        ) or chain.cte_allocator.allocate_cte(f"{slot.declared_name}__ts")
        value_expr = render_value_key(
            key=slot.key.input,
            ctx=self._alias_render_ctx(
                slot_id_by_key=chain.slot_id_by_key,
                available_alias_by_slot_id=available,
            ),
        )
        select = exp.Select().select(
            exp.column(time_alias, quoted=True).as_(time_alias, quoted=True),
            *[exp.column(a, quoted=True).as_(a, quoted=True) for a in pk_aliases],
            value_expr.as_(value_alias, quoted=True),
        ).from_(chain_tail)
        cte_name = cte_name_from_alias(
            prefix="shifted_", alias=cte_name_alias, allocator=chain.cte_allocator,
            dialect=self.dialect, limit=self._dialect.max_identifier_bytes,
        )
        chain.ctes.append(CteEntry(name=cte_name, query=select, depends_on=[chain_tail]))
        return cte_name, value_alias, [(a, a) for a in [time_alias, *pk_aliases]]

    def _shifted_producer_cte(
        self, *, slot, chain: ChainState, render: RenderState, cte_name_alias: str,
    ) -> Tuple[str, str, List[Tuple[str, str]]]:
        """The re-aggregation regime's shifted relation: the slot's shifted producer,
        rendered once per identity (a base producer it interns with is read as is)."""
        planned_query: PlannedQuery = render.planned_query
        attach = next(
            (a for a in planned_query.regroup_attach_plans
             if a.attach_phase == "shifted" and a.shift_of == slot.id),
            None,
        )
        if attach is None:
            raise RuntimeError(
                f"time_shift slot {slot.id!r} has no shifted producer attach.",
            )
        rendered_map = self._gen_rendered_producers
        ident = regroup_producer_identity(attach)
        rec = rendered_map.get(ident) if rendered_map is not None else None
        if rec is not None:
            cte_name, col_by_sid = rec
            self._record_reuse_edges(cte_name)
        else:
            cte_name = cte_name_from_alias(
                prefix="shifted_", alias=cte_name_alias,
                allocator=chain.cte_allocator, dialect=self.dialect,
                limit=self._dialect.max_identifier_bytes,
            )
            entries, col_by_sid = self._producer_ctes(
                attach=attach, bundle=render.bundle, cte_name=cte_name,
            )
            chain.ctes.extend(entries)
            if rendered_map is not None:
                rendered_map[ident] = (cte_name, col_by_sid)
        pairs: List[Tuple[str, str]] = []
        for host_key, producer_sid in attach.join_pairs:
            host_sid = chain.slot_id_by_key.get(host_key)
            host_alias = (
                chain.available_alias_by_slot_id.get(host_sid)
                if host_sid is not None else None
            )
            if host_alias is None or producer_sid not in col_by_sid:
                raise RuntimeError(
                    f"time_shift slot {slot.id!r}: shifted join-back grain member "
                    f"{host_key!r} is not materialised.",
                )
            pairs.append((host_alias, col_by_sid[producer_sid]))
        assert attach.answer_slot_id is not None
        return cte_name, col_by_sid[attach.answer_slot_id], pairs

    def _producer_ctes(
        self, *, attach, bundle, cte_name: str,
    ) -> Tuple[List[Node], Dict[str, str]]:
        """Render ``attach``'s producer as ``cte_name`` after its hoisted CTEs, and map
        each producer slot id to its output column (sql P6: declared edges only)."""
        producer = attach.producer_plan
        self._gen_split_consumers.append(cte_name)
        try:
            hoisted, body = self._render_producer_split(
                producer=producer,
                bundle=self._producer_render_bundle(attach=attach, bundle=bundle),
                kernel=attach.kernel,
            )
        finally:
            self._gen_split_consumers.pop()
        entries: List[Node] = [
            Node(name=h.name, phase="producer", query=h.query,
                 depends_on=[*h.depends_on, *self._reuse_deps_of(h.name)])
            for h in hoisted
        ]
        col_by_sid = {
            s.id: self._full_alias_for_slot(
                slot=s, source_relation=producer.source_relation, alias_index={},
            )
            for s in (*producer.row_slots, *producer.aggregate_slots,
                      *producer.combined_expression_slots)
        }
        if attach.collapsed:
            # A collapsing pick is constant along its axis: one row per remaining grain cell.
            kept = [col_by_sid[sid] for sid in producer.projection if sid not in attach.collapsed]
            body = exp.Select().select(*(exp.column(c, quoted=True) for c in kept)).distinct().from_(
                exp.Subquery(this=body, alias=exp.TableAlias(this=exp.to_identifier("_picked"))))
        entries.append(Node(
            name=cte_name, phase="producer", query=body,
            depends_on=[*[h.name for h in hoisted], *self._reuse_deps_of(cte_name)],
        ))
        return entries, col_by_sid

    def _emit_time_shift_ctes_for_planned(
        self,
        *,
        slot,
        chain: ChainState,
        render: RenderState,
        chain_tail: str,
    ) -> str:
        """Emit the shifted relation + ``sjoin_<alias>`` for one ``time_shift`` slot:
        each row reads the relation at the bucket containing ``bucket + offset``."""
        key = slot.key
        if not isinstance(key, TransformKey) or key.op != "time_shift":
            raise ValueError(
                f"expected time_shift TransformKey, got "
                f"{type(key).__name__} (op={getattr(key, 'op', None)!r})",
            )
        time_key = key.time_key
        if not isinstance(time_key, TimeTruncKey):
            raise ValueError(
                f"time_shift requires a TimeTruncKey time_key; got "
                f"{type(time_key).__name__} (slot id={slot.id!r}).",
            )
        periods, shift_gran = shift_offset_of(key)
        # Explicit granularity else the bucket's: a year shift over month buckets is YoY.
        shift_granularity = shift_gran or time_key.granularity
        time_sid = chain.slot_id_by_key.get(time_key)
        time_alias = (
            chain.available_alias_by_slot_id.get(time_sid)
            if time_sid is not None else None
        )
        if time_alias is None:
            raise RuntimeError(
                f"time_shift time_key not materialised in base CTE: "
                f"slot id={slot.id!r}, time_key={time_key!r}.",
            )
        # A hidden inner slot's declared_name repeats across offsets; allocate a unique alias.
        slot_aliases: List[str] = (
            list(slot.public_aliases) if slot.public_aliases
            else [chain.cte_allocator.allocate_cte(slot.declared_name)]
        )
        if slot.series:
            shifted_cte_name, value_alias, pairs = self._series_shift_cte(
                slot=slot, chain=chain, render=render, chain_tail=chain_tail,
                time_alias=time_alias, cte_name_alias=slot_aliases[0],
            )
        else:
            shifted_cte_name, value_alias, pairs = self._shifted_producer_cte(
                slot=slot, chain=chain, render=render, cte_name_alias=slot_aliases[0],
            )

        # Consumer-side lookup: total even when the calendar shift is many-to-one.
        time_type = self._bucket_type(time_key)
        lookup_expr, lookup_type = self._calendar_offset_bucket(
            bucket_expr=grain_alias_field(alias=time_alias, table=chain_tail), bucket_type=time_type,
            periods=periods, shift_granularity=shift_granularity,
            bucket_granularity=time_key.granularity,
        )
        key_by_alias = {chain.available_alias_by_slot_id.get(sid): k for k, sid in chain.slot_id_by_key.items()}
        sjoin_on = build_grain_joinback_condition(
            pairs=[
                (
                    lookup_expr, lookup_type,
                    grain_alias_field(alias=shifted, table=shifted_cte_name), time_type,
                ) if host == time_alias else self._keyed_pairs([(
                    grain_alias_field(alias=host, table=chain_tail),
                    grain_alias_field(alias=shifted, table=shifted_cte_name),
                    key_by_alias.get(host),
                )])[0]
                for host, shifted in pairs
            ],
            dialect=self._dialect,
        )
        source_relation = chain.source_relation
        slot_full_aliases = [result_key_from_alias(source_relation=source_relation, alias=a) for a in slot_aliases]
        sjoin_select = exp.Select().select(
            *[grain_alias_field(alias=a, table=chain_tail)
              for a in self._carry_aliases_in_plan_order(chain.aliases_by_slot_id)],
            *[grain_alias_field(alias=value_alias, table=shifted_cte_name).as_(
                full, quoted=True) for full in slot_full_aliases],
        ).from_(chain_tail).join(shifted_cte_name, on=sjoin_on, join_type="LEFT")
        sjoin_cte_name = cte_name_from_alias(
            prefix="sjoin_", alias=slot_aliases[0], allocator=chain.cte_allocator,
            dialect=self.dialect, limit=self._dialect.max_identifier_bytes,
        )
        chain.ctes.append(CteEntry(
            name=sjoin_cte_name, query=sjoin_select,
            depends_on=[chain_tail, shifted_cte_name],
        ))
        chain.aliases_by_slot_id.setdefault(slot.id, []).extend(slot_full_aliases)
        chain.available_alias_by_slot_id.setdefault(slot.id, slot_full_aliases[0])
        return sjoin_cte_name

    def _emit_consecutive_periods_ctes_for_planned(  # NOSONAR(S3776) — one cohesive per-slot consecutive_periods emission: predicate-shape decision, unique hidden alias plus collision-safe reset and value CTE names, the reset-group window layer, then the count-within-group window layer. Each block shares the slot registry and alias maps and cte_allocator; extracting helpers would scatter that contract without simplifying it.
        self,
        *,
        slot,
        chain: ChainState,
        render: RenderState,
        chain_tail: str,
    ) -> str:
        """Emit ``cp_prev_`` / ``cp_reset_`` / ``cp_value_<alias>`` CTEs for one slot."""
        ctes = chain.ctes
        cte_allocator = chain.cte_allocator
        slots_by_id = chain.slots_by_id
        slot_id_by_key = chain.slot_id_by_key
        available_alias_by_slot_id = chain.available_alias_by_slot_id
        aliases_by_slot_id = chain.aliases_by_slot_id
        source_relation = chain.source_relation
        planned_query = render.planned_query

        key = slot.key
        if not isinstance(key, TransformKey) or key.op != "consecutive_periods":
            raise ValueError(
                f"expected consecutive_periods TransformKey, got "
                f"{type(key).__name__} (op={getattr(key, 'op', None)!r})",
            )
        inner_key = key.input
        time_key = key.time_key
        if not isinstance(time_key, TimeTruncKey):
            raise ValueError(
                f"consecutive_periods requires a TimeTruncKey time_key; "
                f"got {type(time_key).__name__} (slot id={slot.id!r}).",
            )

        time_sid = slot_id_by_key.get(time_key)
        if time_sid is None or time_sid not in available_alias_by_slot_id:
            raise RuntimeError(
                f"consecutive_periods time_key not materialised: "
                f"slot id={slot.id!r}.",
            )
        time_alias = available_alias_by_slot_id[time_sid]

        # One render path for every input shape (the gate already rejected the
        # unsupported ones). A boolean-shaped tree IS the predicate; a
        # value-shaped tree drives the streak by non-NULL / non-zero truthiness.
        predicate_is_boolean = is_boolean_shaped(inner_key)
        rendered = render_value_key(
            key=inner_key,
            ctx=self._alias_render_ctx(
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            ),
        )
        if predicate_is_boolean:
            predicate = rendered
        else:
            predicate = exp.And(
                this=exp.Is(this=rendered.copy(), expression=exp.Null()).not_(),
                expression=exp.NEQ(
                    this=rendered.copy(), expression=exp.Literal.number(0),
                ),
            )

        partition_aliases: list[str] = []
        for sid in self._transform_grain_slot_ids(
            planned_query=planned_query, slots_by_id=slots_by_id, axis=key.time_key,
        ):
            alias = available_alias_by_slot_id.get(sid)
            if alias is not None:
                partition_aliases.append(alias)

        if slot.public_aliases:
            slot_alias = slot.public_aliases[0]
        else:
            slot_alias = cte_allocator.allocate_cte(slot.declared_name)
        full_slot_alias = (result_key_from_alias(source_relation=source_relation, alias=slot_alias) if slot.public_aliases
                           else internal_result_key(source_relation=source_relation, name=slot_alias))
        cp_reset_alias = f"_cp_reset_{full_slot_alias}"
        cp_prev_alias = f"_cp_prev_{full_slot_alias}"

        carry_aliases = self._carry_aliases_in_plan_order(
            aliases_by_slot_id,
        )
        carry_cols = [exp.column(a, quoted=True) for a in carry_aliases]
        running_frame = exp.WindowSpec(
            kind="ROWS",
            start="UNBOUNDED", start_side="PRECEDING", end="CURRENT ROW",
        )

        def _window(*, this: Expression, partitions: List[str], **extra) -> exp.Window:
            args: Dict[str, Any] = {
                "this": this,
                "order": exp.Order(expressions=[
                    self._window_ordered(exp.column(time_alias, quoted=True)),
                ]),
                **extra,
            }
            if partitions:
                args["partition_by"] = [
                    exp.column(a, quoted=True) for a in partitions
                ]
            return exp.Window(**args)

        def _running_sum(
            *, condition: Expression, then: int, other: int, partitions: List[str],
        ) -> exp.Window:
            """``SUM(CASE WHEN <condition> THEN … ELSE … END)`` over the running frame."""
            return _window(
                this=exp.Sum(this=exp.Case(
                    ifs=[exp.If(this=condition, true=exp.Literal.number(then))],
                    default=exp.Literal.number(other),
                )),
                partitions=partitions, spec=running_frame.copy(),
            )

        # Previous present bucket of the series; a run continues only onto its calendar successor.
        cp_prev_cte_name = cte_allocator.allocate_cte(f"cp_prev_{slot_alias}")
        ctes.append(CteEntry(
            name=cp_prev_cte_name,
            query=exp.Select().select(
                *(c.copy() for c in carry_cols),
                _window(
                    this=exp.Lag(this=exp.column(time_alias, quoted=True)),
                    partitions=partition_aliases,
                ).as_(cp_prev_alias, quoted=True),
            ).from_(chain_tail),
            depends_on=[chain_tail],
        ))
        time_type = self._bucket_type(time_key)
        predecessor, predecessor_type = self._calendar_offset_bucket(
            bucket_expr=exp.column(time_alias, quoted=True), bucket_type=time_type, periods=-1,
            shift_granularity=time_key.granularity,
            bucket_granularity=time_key.granularity,
        )
        continues_run = exp.And(
            this=predicate.copy(),
            expression=exp.EQ(**self._temporal_operands(
                exp.column(cp_prev_alias, quoted=True), time_type, predecessor, predecessor_type,
            )),
        )

        cp_reset_cte_name = cte_allocator.allocate_cte(f"cp_reset_{slot_alias}")
        ctes.append(CteEntry(
            name=cp_reset_cte_name,
            query=exp.Select().select(
                *(c.copy() for c in carry_cols),
                _running_sum(
                    condition=continues_run, then=0, other=1,
                    partitions=partition_aliases,
                ).as_(cp_reset_alias, quoted=True),
            ).from_(cp_prev_cte_name),
            depends_on=[cp_prev_cte_name],
        ))

        value_outer_case = exp.Case(
            ifs=[exp.If(
                this=predicate.copy(),
                true=_running_sum(
                    condition=predicate.copy(), then=1, other=0,
                    partitions=partition_aliases + [cp_reset_alias],
                ),
            )],
            default=exp.Literal.number(0),
        )
        cp_value_cte_name = cte_allocator.allocate_cte(f"cp_value_{slot_alias}")
        output_aliases = [
            result_key_from_alias(source_relation=source_relation, alias=a) for a in slot.public_aliases
        ] or [full_slot_alias]
        ctes.append(CteEntry(
            name=cp_value_cte_name,
            query=exp.Select().select(
                *(c.copy() for c in carry_cols),
                *(value_outer_case.copy().as_(oa, quoted=True)
                  for oa in output_aliases),
            ).from_(cp_reset_cte_name),
            depends_on=[cp_reset_cte_name],
        ))

        for oa in output_aliases:
            aliases_by_slot_id.setdefault(slot.id, []).append(oa)
            available_alias_by_slot_id.setdefault(slot.id, oa)
        return cp_value_cte_name

    def _expand_derived_row_dims(  # NOSONAR(S3776) — one cohesive per-slot pass expanding derived ROW/TIME dimensions and registering the relationship paths they cross.
        self, *, base_render_order, slots_by_id, source_relation: str,
        source_dataset, bundle, scope: ScopeFrame,
        order_slot_ids: Optional[List[str]] = None,
    ) -> Dict[str, Expression]:
        """Pre-expand derived (``FieldSqlKey``) ROW dimensions and derived TIME"""

        def _add(path: Tuple[str, ...]) -> None:
            if path:
                scope.relationship_paths.add(path)

        derived_expr_by_sid: Dict[str, Expression] = {}
        seen_sids: Set[str] = set()
        for sid in [*base_render_order, *(order_slot_ids or ())]:
            if sid in seen_sids:
                continue
            seen_sids.add(sid)
            slot = slots_by_id.get(sid)
            if slot is None or slot.phase != Phase.ROW:
                continue
            key = slot.key
            if isinstance(key, TimeTruncKey) and not isinstance(key.source, (FieldKey, FieldSqlKey)):
                # The bucket's untruncated value, its relationship paths registered before the FROM is built.
                derived_expr_by_sid[sid] = render_value_key(
                    key=key.source, ctx=RenderContext(scope=scope, dialect=self._dialect),
                )
                continue
            if isinstance(key, TimeTruncKey) and isinstance(key.source, FieldSqlKey):
                raw = self._raw_time_col_expr_for_planned(
                    time_field=key.source, source_dataset=source_dataset,
                    source_relation=source_relation, bundle=bundle,
                )
                _add(field_path(key.source))
                for p in self._joined_paths_in_sql(
                    sql_expr=raw, source_relation=source_relation,
                    source_dataset=source_dataset, bundle=bundle,
                ):
                    _add(p)
                continue
            if not isinstance(key, FieldSqlKey):
                continue
            # A cross-dataset derived dim expands rooted at the owning join's __ alias with is_root=False, so a
            # further-joined ref carries the full prefix (B reaching C -> B__C).
            crossed: Set[Tuple[str, ...]] = set()
            expr = self._derived_field_expr(
                key=key, source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
                crossed_paths=crossed,
            )
            if expr is None:
                continue
            derived_expr_by_sid[sid] = expr
            _add(key.path)  # the relationship path to the owning dataset itself (cross-dataset)
            for p in sorted(crossed, key=lambda t: (len(t), t)):
                _add(p)
        return derived_expr_by_sid

    def _derived_field_expr(
        self, *, key: FieldSqlKey, source_dataset: SlayerDataset, source_relation: str, bundle: ResolvedSourceBundle,
        crossed_paths: "Optional[Set[Tuple[str, ...]]]" = None,
    ) -> "Optional[Expression]":
        """The rendered expression for a derived (``FieldSqlKey``) field."""
        if key.path:
            owner_dataset = self._walk_relationship_path_dataset(
                source_dataset=source_dataset, path=key.path, bundle=bundle,
            )
            if owner_dataset is None:
                return None
            owner_relation = "__".join(key.path)
        else:
            owner_dataset = source_dataset
            owner_relation = source_relation
        expanded_sql = self._expand_derived_field_sql(
            source_dataset=owner_dataset, source_relation=owner_relation,
            field_name=key.field_name, bundle=bundle,
            owner_path=tuple(key.path), root_relation=source_relation,
            crossed_paths=crossed_paths,
        )
        col = next(
            (c for c in owner_dataset.fields if c.name == key.field_name), None,
        )
        return _wrap_cast_for_type(
            dialect=self._dialect, expr=self._parse(expanded_sql),
            dt=self._field_cast_type(col) if col is not None else None,
        )


    def _emit_relation(self, *, dataset: SlayerDataset, alias: str, bundle) -> Expression:
        """The one door a dataset reaches the SQL through (FROM, join target, semi-join
        hop, list parent), always as its filtered dataset (python.sql P13)."""
        if is_spine(dataset):
            return self._spine_relation(alias=alias)
        filters = list(dataset.filters or [])
        if not filters:
            relation = self._backing_relation(dataset=dataset, alias=alias)
        else:
            if dataset.name in self._gen_filter_stack:
                stack = self._gen_filter_stack
                raise DatasetFilterCycleError([*stack[stack.index(dataset.name):], dataset.name])
            allocator = self._gen_allocator or self._new_allocator()
            # Scoped to the derived table, so a filter naming its host (even in a subquery) reads this row.
            rows = dataset.name
            backing = self._backing_relation(dataset=dataset, alias=rows)
            self._gen_filter_stack = (*self._gen_filter_stack, dataset.name)
            try:
                relation = self._filtered_dataset(
                    dataset=dataset, filters=filters, backing=backing, rows=rows, alias=alias,
                    allocator=allocator, bundle=bundle,
                )
            finally:
                self._gen_filter_stack = self._gen_filter_stack[:-1]
        if dataset.sql_table not in self._gen_stage_relations:
            relation = _stamp_dataset(relation, dataset=dataset)
        if any(c.name == ROW_IDENTITY_FIELD for c in dataset.fields):
            relation = self._numbered_relation(relation, alias=alias)
        return relation

    def _numbered_relation(self, relation: Expression, *, alias: str) -> Expression:
        """``(SELECT alias.*, <row number> AS row_id FROM relation) AS alias``: the filtered rows, each numbered once."""
        rows = exp.select(
            exp.Column(this=exp.Star(), table=exp.to_identifier(alias)),
            self._dialect.row_number().as_(exp.to_identifier(ROW_IDENTITY_FIELD)),
        ).from_(relation)
        return exp.Subquery(this=rows, alias=exp.TableAlias(this=exp.to_identifier(alias)))

    def _backing_relation(self, *, dataset, alias: str) -> Expression:
        """``dataset``'s rows before its filters; a stage relation must be one the statement declared it reads."""
        if dataset.list_of is not None:
            raise ValueError(f"List child {dataset.name!r} is read inside its parent's rows, never as a relation.")
        if dataset.sql_table:
            if dataset.sql_table in self._gen_stage_relations and dataset.sql_table not in self._gen_stage_reads:
                raise ValueError(
                    f"stage relation {dataset.sql_table!r} is emitted by a statement that "
                    f"does not declare reading it (declared: {sorted(self._gen_stage_reads)})"
                )
            return self._to_table(dataset.sql_table, alias=alias)
        if dataset.sql:
            return self._embed_dataset_sql(sql=dataset.sql, alias=alias)
        if dataset.name in self._gen_splice_failures:
            raise self._gen_splice_failures[dataset.name]
        if dataset.name in self._gen_splice_chain:
            chain = self._gen_splice_chain
            raise QueryBackedCycleError(path=[*chain[chain.index(dataset.name):], dataset.name])
        raise ValueError(
            f"Dataset {dataset.name!r} has neither sql_table nor sql: a query-backed "
            f"dataset reaches the renderer only as its spliced stages."
        )

    def _filtered_dataset(
        self, *, dataset, filters: List[str], backing: Expression, rows: str, alias: str,
        allocator: AliasAllocator, bundle,
    ) -> Expression:
        """``(SELECT rows.* FROM backing AS rows [joins] WHERE …) AS alias``: an entry crossing only
        proven to-one hops inlines its joins; the others restrict together by one correlated ``EXISTS``."""
        limit = self._dialect.max_identifier_bytes

        def path_alias(path: Tuple[str, ...]) -> str:
            return allocator.alias_for(root=rows, path=path, limit=limit)

        inlined: Dict[Tuple[str, ...], list] = {}
        associated: Dict[Tuple[str, ...], list] = {}
        conditions: List[Expression] = []
        association: List[Expression] = []
        for text in filters:
            frame = self._scope_frame(dataset=dataset, relation=rows, bundle=bundle, allocator=allocator)
            predicate = frame.enter_predicate(text, location=f"dataset filter {text!r} on dataset {dataset.name!r}")
            paths = _prefix_closed(frame.relationship_paths.as_list())
            chains = {p: self._oriented_hop_chain(source_dataset=dataset, path=p, bundle=bundle) for p in paths}
            proven = all(
                not is_lateral_edge(edge) and trusted_to_one(edge=edge, target_dataset=target)
                for chain in chains.values() for edge, target in chain
            )
            if paths and (self._force_association_filters or not proven):
                associated.update(chains)
                association.append(_grouped(predicate))
                continue
            inlined.update(chains)
            conditions.append(_grouped(predicate))
        associated_predicate = _conjunction(association)
        if associated_predicate is not None:
            conditions.append(_stamp_association(self._association_exists(
                dataset=dataset, chains=associated, predicate=associated_predicate,
                path_alias=path_alias, bundle=bundle,
            ), dataset=dataset))
        joins = []
        for path, chain in inlined.items():
            edge, target = chain[-1]
            joins.append(self._hop_join(
                edge=edge, prev_dataset=chain[-2][1] if len(chain) > 1 else dataset, next_dataset=target,
                current_alias=path_alias(path[:-1]), next_alias=path_alias(path), bundle=bundle,
            ))
        select = exp.select(exp.Column(this=exp.Star(), table=exp.to_identifier(rows))).from_(backing)
        select = _apply_joins(select=select, joins=joins).where(_conjunction(conditions))
        return exp.Subquery(this=select, alias=exp.to_identifier(alias))

    def _association_exists(
        self, *, dataset: SlayerDataset, chains: Dict[Tuple[str, ...], List[Tuple[OrientedRelationship, SlayerDataset]]],
        predicate: Expression, path_alias: Callable[[Tuple[str, ...]], str], bundle,
    ) -> Expression:
        """``EXISTS`` over the null-extending joins the associated filter entries read, keyed on the row's own columns."""
        hops = []
        for path, chain in chains.items():
            edge, target = chain[-1]
            if is_unrelated_edge(edge):
                raise ValueError(f"The product hop {edge.source_dataset} × {edge.to} cannot be joined.")
            prev = chain[-2][1] if len(chain) > 1 else dataset
            lateral = is_lateral_edge(edge)
            declared_left = edge.join_type is JoinType.LEFT
            keys = [] if lateral else relationship_key_fields(edge=edge, source=prev, target=target)
            hops.append(SemiJoinHop(
                to=target.name, node_path=path, declared_left=declared_left,
                null_extended=declared_left, lateral=edge.lateral if lateral else None,
                **SemiJoinHop.key_args(keys),
            ))
        node_datasets = {(): dataset, **{p: chain[-1][1] for p, chain in chains.items()}}

        def table(hop, leading: bool) -> Tuple[Expression, str]:
            if hop.lateral is None:
                return self._emit_relation(
                    dataset=node_datasets[tuple(hop.node_path)], alias=path_alias(hop.node_path), bundle=bundle,
                ), "CROSS"
            parent_path = tuple(hop.node_path[:-1])
            relation = self._list_relation(
                parent=node_datasets[parent_path], field=hop.lateral, parent_alias=path_alias(parent_path),
                alias=path_alias(hop.node_path), bundle=bundle, leading=leading,
            )
            return relation.relation, relation.join_kind

        ops = _SemiJoinOps(
            alias=path_alias,
            col=lambda table_alias, name, quoted: exp.Column(
                this=key_identifier(name, quoted=quoted), table=exp.to_identifier(table_alias)),
            table=table,
            attached={tuple(h.node_path) for h in hops if len(h.node_path) == 1},
            computed=lambda table_alias, text: self._computed_key(text=text, alias=table_alias),
        )
        inner = _semi_join_spine(ops=ops, hops=hops, inner=exp.select(exp.Literal.number(1)))
        return exp.Exists(this=inner.where(predicate))

    def _spine_relation(self, *, alias: str) -> Expression:
        """The spine as its bucket series: one row per bucket overlapping the bounds."""
        if not self._gen_spines:
            raise ValueError("time_spine reached the renderer outside a spine population")
        factor = self._gen_spines[-1]
        d = self._dialect
        base, multiple, _ = granularity_parts(factor.granularity)
        first = d.build_bucket(
            col_expr=d.build_temporal_literal(value=factor.lower, dt=DataType.DATETIME),
            granularity=factor.granularity, operand=DataType.DATETIME,
        )

        def at(count: Expression) -> Expression:
            return d.build_date_add(expr=first, count=count, unit=base, operand=DataType.DATETIME)

        def steps(n: Expression) -> Expression:
            return n if multiple == 1 else exp.Mul(this=exp.Paren(this=n), expression=exp.Literal.number(multiple))

        i = exp.column("i", table="_seq")
        start = at(steps(i))
        end = at(steps(exp.Add(this=i.copy(), expression=exp.Literal.number(1))))
        sequence = d.build_integer_sequence(size=factor.size)
        allocator = self._gen_allocator or self._new_allocator()
        limit = d.max_identifier_bytes
        rename_embedded_ctes(sequence, allocate=lambda name: allocator.allocate_cte(
            fit_identifier(name=f"_w_{name}", limit=limit),
        ))
        with_node = sequence.args.get("with_")
        if with_node is not None:
            for cte in with_node.expressions:
                self._gen_spine_ctes.append(cte_entry(cte=cte, name=cte.alias_or_name, depends_on=[]))
            with_node.pop()
        series = exp.select(start.as_(exp.to_identifier(TIME_SPINE_FIELD, quoted=True))).from_(
            exp.Subquery(this=sequence, alias=exp.to_identifier("_seq")),
        ).where(exp.and_(
            d.build_temporal_comparison(op="<", operand=start.copy(), value=factor.upper),
            d.build_temporal_comparison(op=">", operand=end, value=factor.lower),
        ))
        return exp.Subquery(this=series, alias=exp.to_identifier(alias))

    def _take_spine_ctes(self, *, since: int) -> List[CteEntry]:
        """The sequence CTEs recorded since ``since``: the ones this render reads."""
        taken, self._gen_spine_ctes = self._gen_spine_ctes[since:], self._gen_spine_ctes[:since]
        return taken

    def _dim_field_expr_from_planned(
        self, *, source_dataset: SlayerDataset, source_relation: str, leaf: str,
    ) -> Expression:
        col = next(
            (c for c in source_dataset.fields if c.name == leaf), None,
        )
        if col is None:
            raise ValueError(
                f"Field {leaf!r} not found on dataset "
                f"{source_dataset.name!r}",
            )
        resolved = self._resolve_sql(sql=col.definition_sql, name=col.name, dataset_name=source_relation,
                                     type=col.datatype)
        return self._dialect.read_column(resolved, zoned=self._dialect.is_zoned_db_type(col.db_type))

    def _bucket_source_expr(
        self, *, source: ValueKey, scope, source_dataset, source_relation: str, bundle,
    ) -> Expression:
        """Untruncated value of a bucket's ``source`` in ``scope``, registering the relationship paths it reads."""
        if not isinstance(source, (FieldKey, FieldSqlKey)):
            return render_value_key(key=source, ctx=RenderContext(scope=scope, dialect=self._dialect))
        scope.resolve(source)
        return self._raw_time_col_expr_for_planned(
            time_field=source, source_dataset=source_dataset,
            source_relation=source_relation, bundle=bundle,
        )

    def _raw_time_col_expr_for_planned(
        self, *, time_field, source_dataset: SlayerDataset, source_relation: str, bundle: ResolvedSourceBundle,
    ) -> Expression:
        """Untruncated value of a field bucket source."""

        if isinstance(time_field, FieldKey):
            return self._joined_or_local_dim_expr(
                path=time_field.path,
                leaf=time_field.leaf,
                source_dataset=source_dataset,
                source_relation=source_relation,
                bundle=bundle,
            )
        if isinstance(time_field, FieldSqlKey):
            if time_field.path:
                joined_dataset = self._walk_relationship_path_dataset(
                    source_dataset=source_dataset, path=time_field.path,
                    bundle=bundle,
                )
                if joined_dataset is None:
                    raise ValueError(
                        f"Time dimension references derived field "
                        f"{time_field.field_name!r} over relationship path "
                        f"{'.'.join(time_field.path)!r} which does not "
                        f"resolve from the source bundle.",
                    )
                # A joined derived TIME dim whose sql crosses a further relationship must anchor inner refs at the
                # host-path alias, not the bare direct-join alias, or the FROM references an unjoined table.
                expanded_sql = self._expand_derived_field_sql(
                    source_dataset=joined_dataset,
                    source_relation="__".join(time_field.path),
                    field_name=time_field.field_name,
                    bundle=bundle,
                    owner_path=tuple(time_field.path),
                    root_relation=source_relation,
                    cast=False,
                )
                owner = joined_dataset
            else:
                expanded_sql = self._expand_derived_field_sql(
                    source_dataset=source_dataset,
                    source_relation=source_relation,
                    field_name=time_field.field_name,
                    bundle=bundle,
                    cast=False,
                )
                owner = source_dataset
            declared = owner.get_field(time_field.field_name)
            return self._dialect.read_column(
                self._parse(expanded_sql), zoned=self._dialect.is_zoned_db_type(getattr(declared, "db_type", None)),
            )
        raise NotImplementedError(
            f"Unsupported TimeTruncKey field type: {type(time_field).__name__}",
        )

    def _expand_derived_field_sql(
        self, *, source_dataset: SlayerDataset, source_relation: str, field_name: str, bundle: ResolvedSourceBundle,
        owner_path: Tuple[str, ...] = (),
        root_relation: "Optional[str]" = None,
        crossed_paths: "Optional[Set[Tuple[str, ...]]]" = None,
        cast: bool = True,
    ) -> str:
        """Expand a ``FieldSqlKey`` target to SQL, casting the value to its
        declared type (a bare field is skipped) and desugaring any ``DatasetField.filter``
        to ``CASE WHEN <filter> THEN <value> END``. The type CAST rides
        the VALUE, so the filter CASE needs no outer cast. ``cast=False`` skips the
        type CAST for a raw-time expression (``DATE_TRUNC`` needs the untruncated
        timestamp, not a lossy ``CAST(... AS TIMESTAMP)``)."""
        col = next(
            (c for c in source_dataset.fields if c.name == field_name), None,
        )
        if col is None:
            raise ValueError(
                f"Derived field {field_name!r} not found on dataset "
                f"{source_dataset.name!r}",
            )
        resolver_root = root_relation if root_relation is not None else source_relation
        value, filter_sql = expand_field_definition_parts_sync(
            field=col, dataset=source_dataset, alias_path=source_relation,
            datasets_by_name=bundle.datasets_by_name, dialect=self.dialect,
            owner_path=owner_path, alias_resolver=self._join_alias_resolver(resolver_root),
            crossed_paths=crossed_paths,
        )
        value_ast = self._parse(value)
        value_sql = (
            _wrap_cast_for_type(dialect=self._dialect, expr=value_ast, dt=self._field_cast_type(col)) if cast else value_ast
        ).sql(dialect=self.dialect)
        return wrap_field_filter(value_sql=value_sql, filter_sql=filter_sql)

    def _joined_paths_in_sql(
        self, *, sql_expr: Expression, source_relation: str, source_dataset,
        bundle,
    ) -> List[Tuple[str, ...]]:
        """Collect the relationship paths referenced by table qualifiers inside an"""
        return collect_root_scope_joined_paths(
            parsed=sql_expr,
            source_dataset=source_dataset,
            source_relation=source_relation,
            bundle=bundle,
        )

    def _resolve_where_filter_relationships_via_scope(
        self, *, planned_query, scope: ScopeFrame,
        skip_filter_ids: Optional[Set[str]] = None,
        filters_override: "Optional[List[Any]]" = None,
    ) -> None:
        """Register into ``scope.relationship_paths`` the relationship paths every WHERE-phase filter"""

        skip = skip_filter_ids or set()
        filters = (
            _lower_positions(planned_query).filters
            if filters_override is None else filters_override
        )
        for fp in filters:
            if fp.phase != Phase.ROW or fp.id in skip:
                continue
            for p in self._value_key_relationship_paths(
                key=fp.expression.value_key, source_dataset=scope.root_dataset,
                source_relation=scope.root_relation, bundle=scope.bundle,
            ):
                scope.relationship_paths.add(p)

    def _value_key_relationship_paths(  # NOSONAR(S3776) — one cohesive recursive ValueKey-tree walk; complexity is the per-key-type dispatch.
        self, *, key, source_dataset, source_relation: str, bundle,
    ) -> List[Tuple[str, ...]]:
        """Relationship paths a typed filter ``ValueKey`` tree references"""

        out: List[Tuple[str, ...]] = []

        def _add(path: Tuple[str, ...]) -> None:
            for i in range(1, len(path) + 1):
                prefix = tuple(path[:i])
                if prefix and prefix not in out:
                    out.append(prefix)

        def _derived_paths(*, dataset, relation, field_name, owner_path) -> None:
            crossed: Set[Tuple[str, ...]] = set()
            self._expand_derived_field_sql(
                source_dataset=dataset, source_relation=relation,
                field_name=field_name, bundle=bundle, owner_path=owner_path,
                crossed_paths=crossed,
            )
            for p in sorted(crossed, key=lambda t: (len(t), t)):
                if p not in out:
                    out.append(p)

        def _walk(k) -> None:
            if isinstance(k, FieldKey):
                _add(k.path)
            elif isinstance(k, FieldSqlKey):
                _add(k.path)
                dataset = (
                    self._walk_relationship_path_dataset(
                        source_dataset=source_dataset, path=k.path, bundle=bundle,
                    ) if k.path
                    else source_dataset
                )
                if dataset is not None:
                    _derived_paths(
                        dataset=dataset,
                        relation="__".join(k.path) if k.path else source_relation,
                        field_name=k.field_name,
                        owner_path=tuple(k.path),
                    )
            elif isinstance(k, ArithmeticKey):
                for o in k.operands:
                    _walk(o)
            elif isinstance(k, ScalarCallKey):
                for a in k.args:
                    _walk(a)
            elif isinstance(k, InKey):
                _walk(k.field)
            elif isinstance(k, (CastKey, ListSetKey)):
                _walk(k.source)

        _walk(key)
        return out

    def _resolve_aggregation_def(
        self,
        *,
        key,
        source_dataset,
        src_leaf: str,
    ):
        """Look up the dataset-level ``Aggregation`` definition for ``key.agg``,"""
        agg_def = aggregation_definition(owner=source_dataset, agg=key.agg)
        if agg_def is None and key.agg not in _BUILTIN_BAREARG_AGGS_LOCAL_SLICE:
            raise AggregationNotAllowedError(
                field=src_leaf,
                agg=key.agg,
                reason=(
                    f"unknown aggregation {key.agg!r} — not a built-in "
                    f"and not defined in {source_dataset.name!r}."
                    f"aggregations."
                ),
            )
        return agg_def

    def _validate_aggregate_kwarg_paths(
        self,
        *,
        key,
        source,
        src_leaf: str,
    ) -> None:
        """Reject CROSS-DATASET aggregates' kwarg field refs whose relationship path"""

        # A host-locus aggregate's inputs are certified from the home by the
        # compiler and the scope registers each kwarg's relationship path; the gate keeps
        # guarding target-rooted producers.
        if not source.path or _is_host_grain(key):
            return
        for kname, kval in key.kwargs:
            leaf = next((
                r for r in parameter_row_leaves(kval)
                if isinstance(r, (FieldKey, FieldSqlKey)) and r.path != source.path
            ), None)
            if leaf is not None:
                raise AggregationNotAllowedError(
                    field=src_leaf,
                    agg=key.agg,
                    reason=(
                        f"kwarg {kname!r} references "
                        f"{type(leaf).__name__} with path {leaf.path!r}; "
                        f"aggregate source path is {source.path!r}. "
                        f"Cross-dataset kwargs must share the source's "
                        f"relationship path."
                    ),
                )

    def _agg_param_exprs(
        self, *, key, expression: Optional[str], scope: ScopeFrame,
        resolved: "Optional[Dict[str, ResolvedAggKwarg]]" = None,
        skip: frozenset = frozenset(),
    ) -> "Dict[str, ResolvedAggKwarg]":
        """The one parameter render path: a pre-resolved value wins, a scalar renders as a
        literal, a row value key through ``scope``; a marker string is never SQL."""
        reads = self._expression_reads(agg=key.agg, expression=expression)
        out: "Dict[str, ResolvedAggKwarg]" = {}
        for name, value in key.kwargs:
            if name in skip:
                continue
            if resolved and name in resolved:
                out[name] = resolved[name]
            elif isinstance(value, str):
                if name in reads:
                    raise TypeError(
                        f"Aggregation {key.agg!r} parameter {name!r} is unbound text "
                        f"{value!r}; a parameter binds at query binding, never as SQL text."
                    )
            elif _is_row_param(value):
                out[name] = ResolvedAggKwarg(
                    value=scope.resolve(value),
                    input_type=aggregation_source_type(value, field_type=scope.field_type),
                )
            elif isinstance(value, VALUE_KEY_TYPES):
                raise RuntimeError(
                    f"Aggregation {key.agg!r} parameter {name!r} ({type(value).__name__}) "
                    f"reached the renderer unattached."
                )
            else:
                out[name] = ResolvedAggKwarg(value=_literal(value), input_type=literal_type(value))
        return out

    def _expression_reads(self, *, agg: str, expression: Optional[str]) -> frozenset:
        """The parameter names ``agg`` reads: its expression's placeholders, else a built-in's own."""
        if expression is None:
            return frozenset(BUILTIN_AGGREGATION_PARAM_ORDER.get(agg, ()))
        return self._expression_template(agg_name=agg, expression=expression).placeholder_names

    def _walk_relationship_path_dataset(
        self, *, source_dataset: SlayerDataset, path: Sequence[str], bundle: ResolvedSourceBundle,
    ) -> Optional[SlayerDataset]:
        """The terminal dataset of a relationship ``path`` walked from ``source_dataset``
        via the shared walker (tokens may be edge names or reverse hops)."""
        return resolved_terminal(
            root=source_dataset, path=tuple(path),
            datasets_by_name=bundle.datasets_by_name,
        )

    def _build_agg_render_spec_from_planned(  # NOSONAR(S3776) — sequential isinstance dispatch over StarKey / FieldKey / FieldSqlKey with helper extractions for aggregation-def lookup, kwarg path validation, and explicit-time-arg resolution. Further splitting would scatter the per-source-kind contract.
        self,
        *,
        slot: Optional[ValueSlot],
        key,
        source_dataset: SlayerDataset,
        source_relation: str,
        full_alias: str,
        bundle: Optional[ResolvedSourceBundle] = None,
        resolved_agg_kwargs: "Optional[Dict[str, ResolvedAggKwarg]]" = None,
        scope: Optional[ScopeFrame] = None,
    ) -> AggRenderSpec:
        """Build an ``AggRenderSpec`` from a planned aggregate slot so"""
        if restricts_own_rows(key):
            raise RuntimeError(
                f"{key_display(key)}: a FILTER on a non-built-in aggregation restricts its producer's rows; "
                f"it never renders inline.",
            )
        # slot may be None for a HAVING term whose aggregate isn't a projection slot; the result type is then unknown
        # (no outer CAST).
        slot_type = slot.type if slot is not None else None
        source = key.source
        if isinstance(source, StarKey):
            # Reject any non-count aggregation on * (sum(*) would render as SUM(*)); enforce here so invalid SQL can't be
            # emitted.
            if key.agg != "count":
                raise ValueError(
                    f"Aggregation {key.agg!r} not allowed with metric "
                    f"'*' — use 'count(<dataset>.*)' for COUNT(*)."
                )
            owner = (
                aggregation_owner(
                    root=source_dataset, source=source, datasets_by_name=bundle.datasets_by_name,
                ) if bundle is not None else None
            ) or source_dataset
            agg_def = self._resolve_aggregation_def(key=key, source_dataset=owner, src_leaf="*")
            expression = rendered_expression(agg=key.agg, definition=agg_def)
            if key.args or (key.kwargs and not expression):
                raise ValueError(
                    f"'count(*)' takes no args or kwargs; got "
                    f"args={key.args!r}, kwargs={key.kwargs!r}."
                )
            star_scope = scope or self._throwaway_frame(
                dataset=source_dataset, relation=source_relation,
                bundle=bundle or ResolvedSourceBundle(dialect=self.dialect, population=source_dataset),
            )
            return AggRenderSpec(
                name="",
                sql=None,
                # A key-carried FILTER counts the passing rows.
                value=None if key.filter is None else self._passing_value(
                    key.filter, value=exp.Literal.number(1), scope=star_scope,
                ),
                aggregation=key.agg,
                alias=full_alias,
                dataset_name=source_relation,
                type=slot_type,
                expression=expression,
                agg_kwargs=self._agg_param_exprs(
                    key=key, expression=expression, resolved=resolved_agg_kwargs, scope=star_scope,
                ),
            )
        param_scope = scope or self._throwaway_frame(
            dataset=source_dataset, relation=source_relation,
            bundle=bundle or ResolvedSourceBundle(dialect=self.dialect, population=source_dataset),
        )
        input_type = aggregation_source_type(source, field_type=param_scope.field_type)
        element_type = (
            value_type(source, field_type=param_scope.field_type)
            if list_valued(key, field_type=param_scope.field_type) else None
        )
        if isinstance(source, (FieldKey, FieldSqlKey)):
            host_grain_root: Optional[str] = None
            if source.path and _is_host_grain(key) and bundle is not None:
                terminal = self._walk_relationship_path_dataset(
                    source_dataset=source_dataset, path=source.path, bundle=bundle,
                )
                if terminal is not None:
                    source_dataset = terminal
                    # Qualify through the generation AliasAllocator, not a raw __.join, so the alias matches what
                    # _build_from_and_joins emitted.
                    host_grain_root = source_relation
                    source_relation = self._join_alias(
                        root=source_relation, path=source.path,
                    )
            src_leaf = (
                source.leaf
                if isinstance(source, FieldKey)
                else source.field_name
            )
            # first/last render through a ranked-kernel producer, never this spec builder — a plain render would
            # silently drop the ranking.
            if is_ranked(key):
                raise RuntimeError(
                    f"first/last aggregate {key!r} reached the plain aggregate "
                    f"renderer; it must render through a ranked-kernel "
                    f"producer."
                )
            agg_def = self._resolve_aggregation_def(
                key=key, source_dataset=source_dataset, src_leaf=src_leaf,
            )
            self._validate_aggregate_kwarg_paths(
                key=key, source=source, src_leaf=src_leaf,
            )
            col = next(
                (c for c in source_dataset.fields if c.name == src_leaf),
                None,
            )
            if col is None:
                raise ValueError(
                    f"Aggregate source field {src_leaf!r} not found "
                    f"on dataset {source_dataset.name!r}",
                )
            # A FieldSqlKey source expands its value (inner bare refs qualify to
            # source_relation) and desugars any DatasetField.filter to CASE WHEN here;
            # a plain physical column stays bare for the aggregate.
            if isinstance(source, FieldSqlKey) and bundle is not None:
                sql_text = self._expand_derived_field_sql(
                    source_dataset=source_dataset,
                    source_relation=source_relation,
                    field_name=col.name,
                    bundle=bundle,
                    owner_path=source.path if host_grain_root is not None else (),
                    root_relation=host_grain_root,
                )
                # The declared-type CAST is baked into sql_text (on the value), so
                # the aggregate must not re-cast the whole expression.
                field_type = None
                zoned_value = None
            else:
                sql_text = col.definition_sql or col.name
                field_type = col.datatype
                # A zoned physical column reads as the dialect's naive type inside the aggregate too.
                zoned_value = self._dialect.read_column(
                    self._resolve_sql(sql=sql_text, name=col.name, dataset_name=source_relation, type=field_type),
                    zoned=True,
                ) if self._dialect.is_zoned_db_type(col.db_type) else None
            expression = rendered_expression(agg=key.agg, definition=agg_def)
            return AggRenderSpec(
                name=col.name,
                sql=sql_text,
                aggregation=key.agg,
                alias=full_alias,
                dataset_name=source_relation,
                type=slot_type,
                field_type=field_type,
                input_type=input_type,
                element_type=element_type,
                value=zoned_value,
                agg_kwargs=self._agg_param_exprs(
                    key=key, expression=expression, resolved=resolved_agg_kwargs, scope=param_scope,
                ),
                expression=expression,
                time_field=None,
            )
        if isinstance(source, _EXPRESSION_SOURCE_KINDS):
            # Same-dataset expression source — render the row-level
            # expression to SQL text; every dispatch kind downstream
            # (simple / distinct / percentile / dialect hook / expression
            # ``{value}``) receives it exactly like a derived-field body.
            assert not isinstance(source, TimePointCmpKey)  # lowered by the checker before planning
            expr_leaf = expression_source_leaf(source)
            agg_def = self._resolve_aggregation_def(
                key=key, source_dataset=source_dataset, src_leaf=expr_leaf,
            )
            # One resolver for leaves, attached placeholders, derived fields and relationship-path registration.
            value = apply_ast_rewrites(
                tree=param_scope.resolve(source), target_dialect=self._dialect, parse_dialect=self._dialect,
            )
            expression = rendered_expression(agg=key.agg, definition=agg_def)
            return AggRenderSpec(
                name=expr_leaf,
                sql=None,
                value=value,
                aggregation=key.agg,
                alias=full_alias,
                dataset_name=source_relation,
                type=slot_type,
                field_type=None,
                input_type=input_type,
                element_type=element_type,
                agg_kwargs=self._agg_param_exprs(
                    key=key, expression=expression, resolved=resolved_agg_kwargs, scope=param_scope,
                ),
                expression=expression,
                time_field=None,
            )
        raise NotImplementedError(
            f"AggregateKey source {type(source).__name__} not supported.",
        )

    def _passing_value(self, condition, *, value: Expression, scope: ScopeFrame) -> exp.Case:
        """``CASE WHEN <condition> THEN <value> END``: ``value`` on the rows a FILTER keeps."""
        passing = render_value_key(key=condition, ctx=RenderContext(scope=scope, dialect=self._dialect))
        return exp.Case(ifs=[exp.If(this=passing, true=value)])

    def _semi_join_exists_conditions(
        self, *, planned_query, source_dataset, source_relation: str, bundle,
    ) -> "List[Expression]":
        """One correlated ``EXISTS`` per semi-join group pushed into this
        producer plan; empty for plans without pushdown."""
        groups = getattr(planned_query, "semi_join_filters", None) or []
        if not groups:
            return []
        allocator = self._gen_allocator or self._new_allocator()
        return [
            self._build_semi_join_exists(
                group=group, source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
                allocator=allocator,
            )
            for group in groups
        ]

    def _hop_dataset(self, *, name: str, bundle: ResolvedSourceBundle) -> SlayerDataset:
        dataset = bundle.get_referenced_dataset(name)
        if dataset is None and bundle.population is not None \
                and bundle.population.name == name:
            dataset = bundle.population
        if dataset is None:
            raise ValueError(
                f"Semi-join hop target {name!r} is not in the resolved "
                f"source bundle."
            )
        return dataset


    def _build_semi_join_exists(
        self, *, group: SemiJoinFilter, source_dataset: SlayerDataset, source_relation: str,
        bundle: ResolvedSourceBundle, allocator,
    ) -> Expression:
        """Build one correlated EXISTS lowering the group's join product:
        outer-attached hops (parent outside the group) correlate to the outer
        body — the flat shape when none is null-extended, else the spine shape (a
        correlated LEFT JOIN ON is rejected by DuckDB; a derived table carrying the
        outer keys is not)."""
        limit = self._dialect.max_identifier_bytes

        def _alias(path) -> str:
            return allocator.alias_for(
                root=source_relation, path=tuple(path), limit=limit,
            )

        in_group = {tuple(h.node_path) for h in group.hops}
        node_datasets = {(): source_dataset.name, **{tuple(h.node_path): h.to for h in group.hops}}

        def _table(hop: SemiJoinHop, leading: bool) -> Tuple[Expression, str]:
            if hop.lateral is None:
                relation = self._emit_relation(
                    dataset=self._hop_dataset(name=hop.to, bundle=bundle), alias=_alias(hop.node_path),
                    bundle=bundle,
                )
                return (mark_untrusted(relation) if hop.untrusted else relation), "CROSS"
            parent_path = tuple(hop.node_path[:-1])
            relation = self._list_relation(
                parent=bundle.datasets_by_name[node_datasets[parent_path]], field=hop.lateral,
                parent_alias=_alias(parent_path), alias=_alias(hop.node_path), bundle=bundle, leading=leading,
            )
            return relation.relation, relation.join_kind

        # Conjunct refs carry tree-node paths, so the same allocator resolves
        # them to the hop aliases; root-local refs correlate to the outer body.
        ctx = RenderContext(
            scope=self._scope_frame(
                dataset=source_dataset, relation=source_relation,
                bundle=bundle, allocator=allocator,
            ),
            dialect=self._dialect,
            filters=FilterFacilities(
                cast_field_sql=True, paren_comparison_operands=True,
            ),
        )
        conjuncts = []
        for key in group.conjuncts:
            rendered = render_value_key(key=key, ctx=ctx)
            conjuncts.append(exp.Paren(this=rendered) if isinstance(rendered, (exp.And, exp.Or)) else rendered)
        leaves, plain = _leaf_list_predicates(group=group, conjuncts=conjuncts, alias=_alias)
        conditions = [*plain, *(
            self._element_test(hop=hop, predicates=predicates, node_datasets=node_datasets,
                               source_relation=source_relation, bundle=bundle, alias=_alias)
            for hop, predicates in leaves
        )]
        rest = [h for h in group.hops if all(h is not hop for hop, _ in leaves)]
        if not rest:
            combined = conditions[0]
            for condition in conditions[1:]:
                combined = exp.And(this=combined, expression=condition)
            return combined
        ops = _SemiJoinOps(
            alias=_alias,
            col=lambda table, name, quoted: exp.Column(
                this=key_identifier(name, quoted=quoted), table=exp.to_identifier(table)),
            table=_table,
            attached={
                tuple(h.node_path) for h in rest
                if tuple(h.node_path[:-1]) not in in_group
            },
            computed=lambda table, text: self._computed_key(text=text, alias=table),
        )
        inner = exp.select(exp.Literal.number(1))
        if any(h.null_extended for h in rest if ops.is_attached(h)):
            inner = _semi_join_spine(
                ops=ops, hops=rest, inner=inner)
        else:
            inner = _semi_join_flat(ops=ops, hops=rest, inner=inner)
        for condition in conditions:
            inner = inner.where(condition)
        return exp.Exists(this=inner)

    def _element_test(
        self, *, hop, predicates: List[Expression], node_datasets, source_relation: str, bundle: ResolvedSourceBundle, alias,
    ) -> Expression:
        """Some non-NULL element of the leaf list hop satisfies every predicate (read on its parent's row)."""
        parent_path = tuple(hop.node_path[:-1])
        parent = bundle.datasets_by_name[node_datasets[parent_path]]
        col = parent.get_field(hop.lateral)
        assert col is not None
        relation = alias(parent_path) if parent_path else source_relation
        values = self._throwaway_frame(dataset=parent, relation=relation, bundle=bundle).resolve(
            FieldSqlKey(dataset=parent.name, field_name=hop.lateral) if col.needs_expansion else FieldKey(leaf=hop.lateral),
        )
        return self._dialect.list_exists(
            values, alias=exp.to_identifier(alias(hop.node_path)), element=col.element_type,
            predicate=exp.and_(*predicates) if predicates else exp.true(),
        )

    def _build_where_having_from_planned(  # NOSONAR(S3776) — one cohesive pass over the lowered entries routing each to WHERE / HAVING / POST by phase, with the per-carrier (typed vs Mode-A text) rendering inline. Splitting the phase routing from the rendering would thread slot_by_key / first_last_state / where_parts / having_parts through helpers without simplifying anything.
        self,
        *,
        planned_query,
        source_relation: str,
        source_dataset,
        bundle,
        skip_filter_ids: Optional[Set[str]] = None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
        filters_override: "Optional[List[Any]]" = None,
        regroup_env: Optional[Dict[Any, Expression]] = None,
        dimension_values: Optional[Dict[Any, Expression]] = None,
    ):
        """``filters_override`` replaces the plan's lowered entries; HAVING renders
        over the grouped SELECT's ``dimension_values`` (``None``: no HAVING)."""

        skip = skip_filter_ids or set()
        slot_by_key: Dict[Any, Any] = {
            s.key: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }
        where_parts: List[Expression] = []
        having_parts: List[Expression] = []
        filters = (
            _lower_positions(planned_query).filters
            if filters_override is None else filters_override
        )
        for fp in filters:
            if fp.id in skip:
                continue
            if fp.phase == Phase.POST:
                continue
            if fp.phase not in (Phase.ROW, Phase.AGGREGATE):
                raise NotImplementedError(
                    f"unsupported filter phase "
                    f"{fp.phase!r}. filter id={fp.id!r}."
                )
            # An AGGREGATE-phase filter on a LOCAL aggregate renders as HAVING; a cross-dataset ref raises in the walker
            # (it routes via the per-plan CTE).
            having = fp.phase == Phase.AGGREGATE
            if having and dimension_values is None:
                continue
            target_parts = having_parts if having else where_parts
            ctx = self._filter_render_context(
                source_dataset=source_dataset,
                source_relation=source_relation,
                bundle=bundle,
                slot_by_key=slot_by_key,
                aliases_by_slot_id=aliases_by_slot_id,
                regroup_env=regroup_env,
            )
            if having:
                ctx = ctx.model_copy(update={
                    "scope": None, "dimension_values": dimension_values,
                })
            target_parts.append(_grouped(render_value_key(key=fp.expression.value_key, ctx=ctx)))

        return _conjunction(where_parts), _conjunction(having_parts)

    @staticmethod
    def _is_nontrivial_derived(dataset: SlayerDataset, name: str) -> bool:
        """True iff ``name`` is a field on ``dataset`` whose ``DatasetField.sql`` is a"""
        col = next((c for c in dataset.fields if c.name == name), None)
        return col is not None and col.definition_sql is not None and not is_trivial_base(
            field=col,
        )

    def _filter_agg_builder(
        self, *, source_dataset, source_relation: str, bundle, scope: ScopeFrame,
    ):
        """The WHERE/HAVING aggregate seam: render a local"""

        def build(agg_key, slot, having_full_alias) -> Expression:
            anchor = source_anchor_path(agg_key.source)
            if anchor:
                raise NotImplementedError(
                    f"cross-dataset aggregate ref in filter (path={anchor!r}) "
                    f"routes via the per-plan CTE, not inline HAVING."
                )
            having_kwargs = self._resolve_agg_kwargs_for_key(key=agg_key, scope=scope)
            synth = self._build_agg_render_spec_from_planned(
                slot=slot, key=agg_key, source_dataset=source_dataset,
                source_relation=source_relation, full_alias=having_full_alias,
                bundle=bundle, resolved_agg_kwargs=having_kwargs, scope=scope,
            )
            agg_expr, _is_agg = self._build_agg(synth)
            return agg_expr

        return build

    def _filter_render_context(
        self, *, source_dataset, source_relation: str, bundle,
        slot_by_key=None, aliases_by_slot_id=None, regroup_env=None,
    ) -> RenderContext:
        """A ``RenderContext`` for the WHERE/HAVING filter family, over a"""
        scope = self._throwaway_frame(
            dataset=source_dataset, relation=source_relation, bundle=bundle,
            attached_fields=regroup_env,
        )
        return RenderContext(
            scope=scope,
            dialect=self._dialect,
            # Kept when HAVING drops the scope: booleans read as numbers need declared types.
            field_type=scope.field_type,
            filters=FilterFacilities(
                slot_by_key=slot_by_key or {},
                aliases_by_slot_id=aliases_by_slot_id or {},
                agg_builder=self._filter_agg_builder(
                    source_dataset=source_dataset,
                    source_relation=source_relation,
                    bundle=bundle,
                    scope=scope,
                ),
                cast_field_sql=True,
                paren_comparison_operands=True,
            ),
        )

    def _outer_wrapper_alias_facilities(
        self, *, slot_by_key, cross_dataset_agg_slot_to_cm: Dict[str, AttachedValue], aliases_by_slot_id,
    ) -> AliasFacilities:
        """Precompute the outer-WHERE slot→qualified-column map as an"""
        slot_id_by_key: Dict[Any, str] = {}
        available_alias_by_slot_id: Dict[str, str] = {}
        table_by_slot_id: Dict[str, str] = {}
        value_by_slot_id: Dict[str, Expression] = {}
        for key, slot in slot_by_key.items():
            sid = slot.id
            cm_entry = cross_dataset_agg_slot_to_cm.get(sid)
            if cm_entry is not None:
                alias, table = cm_entry.field_name, cm_entry.cte_name
                value_by_slot_id[sid] = cm_entry.value()
            else:
                aliases = aliases_by_slot_id.get(sid) or []
                if not aliases:
                    continue
                alias, table = aliases[0], "_base"
            slot_id_by_key[key] = sid
            available_alias_by_slot_id[sid] = alias
            table_by_slot_id[sid] = table
        return AliasFacilities(
            slot_id_by_key=slot_id_by_key,
            available_alias_by_slot_id=available_alias_by_slot_id,
            table_by_slot_id=table_by_slot_id,
            value_by_slot_id=value_by_slot_id,
        )

    def _build_outer_trim_wrap_select(
        self,
        *,
        base_select: exp.Select,
        planned_query,
        source_relation: str,
        aliases_by_slot_id: Dict[str, List[str]],
        slots_by_id: Dict[str, Any],
        bundle,
    ) -> exp.Select:
        """Wrap a no-transform base SELECT in the public outer wrap, trimming hidden columns."""
        return self._public_outer_wrap(
            inner=base_select,
            public_aliases=_cycle_public_aliases_in_projection_order(
                planned_query=planned_query,
                slots_by_id=slots_by_id,
                aliases_by_slot_id=aliases_by_slot_id,
            ),
            order_terms=self._host_order_terms(
                planned_query=planned_query,
                source_relation=source_relation,
                slots_by_id=slots_by_id,
                bundle=bundle,
                aliases_by_slot_id=aliases_by_slot_id,
            ),
            planned_query=planned_query,
        )

    def _public_outer_wrap(
        self,
        *,
        inner: exp.Select,
        public_aliases: Sequence[str],
        order_terms: Sequence[exp.Ordered],
        planned_query,
    ) -> exp.Select:
        """``SELECT <public> FROM (<inner>) AS _outer``: the inner's ``WITH`` hoisted, then ordered and paginated."""
        with_ = inner.args.get("with_")
        if with_ is not None:
            inner.set("with_", None)
        outer = exp.Select().select(*(
            exp.Column(this=exp.to_identifier(alias, quoted=True))
            for alias in public_aliases
        )).from_(
            exp.Subquery(this=inner, alias=exp.to_identifier(OUTER_WRAP_ALIAS)),
        )
        if with_ is not None:
            outer.set("with_", with_)
        return self._order_and_paginate(
            select=outer, order_terms=order_terms, planned_query=planned_query,
        )

    def _order_and_paginate(
        self, *, select: exp.Select, order_terms: Sequence[exp.Ordered], planned_query,
    ) -> exp.Select:
        """``select`` with ``order_terms`` and the plan's dialect pagination applied."""
        for term in order_terms:
            select = select.order_by(term)
        return self._dialect.apply_pagination(
            select, limit=planned_query.limit, offset=planned_query.offset,
        )

    def _host_order_terms(
        self,
        *,
        planned_query,
        source_relation: str,
        slots_by_id: dict,
        source_dataset=None,
        bundle=None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
        select: Optional[exp.Select] = None,
    ) -> List[exp.Ordered]:
        """ORDER BY terms resolved against a base SELECT with no CTE chain."""
        scoped_order = _lower_positions(planned_query).order
        env = self._host_base_order_env(
            scoped_order=scoped_order,
            source_relation=source_relation,
            slots_by_id=slots_by_id,
            source_dataset=source_dataset,
            bundle=bundle,
            aliases_by_slot_id=aliases_by_slot_id,
        )
        if select is not None:
            env.projection = {
                a.alias: a.this for a in select.expressions if isinstance(a, exp.Alias) and a.alias
            }
        return [term for entry in scoped_order for term in resolve_order_term(entry=entry, env=env)]

    def _apply_planned_order_limit(
        self,
        *,
        select: exp.Select,
        planned_query,
        source_relation: str,
        slots_by_id: dict,
        source_dataset=None,
        bundle=None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
    ) -> exp.Select:
        """ORDER BY / LIMIT / OFFSET for a base SELECT with no CTE chain."""
        return self._order_and_paginate(
            select=select,
            order_terms=self._host_order_terms(
                planned_query=planned_query,
                source_relation=source_relation,
                slots_by_id=slots_by_id,
                source_dataset=source_dataset,
                bundle=bundle,
                aliases_by_slot_id=aliases_by_slot_id,
                select=select,
            ),
            planned_query=planned_query,
        )

    def _host_base_order_env(
        self,
        *,
        scoped_order: List[ScopedOrder],
        source_relation: str,
        slots_by_id: dict,
        source_dataset,
        bundle,
        aliases_by_slot_id: Optional[Dict[str, List[str]]],
    ) -> OrderEnv:
        """Name every order slot the base SELECT produces, under the scope the"""
        env = OrderEnv(dialect=self._dialect)
        for order_entry in scoped_order:
            slot = slots_by_id.get(order_entry.slot_id)
            if slot is None:
                # Deliberately not an early raise: leaving the slot absent makes the resolver report it, so every path
                # reports it the same way.
                continue
            getattr(env, order_entry.scope.value)[order_entry.slot_id] = (
                self._host_base_order_ref(
                    slot=slot,
                    source_relation=source_relation,
                    source_dataset=source_dataset,
                    bundle=bundle,
                    aliases_by_slot_id=aliases_by_slot_id,
                )
            )
        return env

    def _host_base_order_ref(  # NOSONAR(S3776) — per-key-kind resolution of ONE hidden slot to a base-SELECT reference (materialised alias vs split row emission vs local derived expansion). Each branch is a distinct contract with its own invariant; splitting them scatters the chain that makes their order meaningful.
        self,
        *,
        slot,
        source_relation: str,
        source_dataset,
        bundle,
        aliases_by_slot_id: Optional[Dict[str, List[str]]],
    ) -> Expression:
        """How one slot's value is NAMED in the base SELECT."""

        if not slot.hidden:
            # Order on the SAME full dotted alias the projection emits, not the flat declared_name, which names a column
            # the SELECT never projects.
            return exp.Column(
                this=exp.to_identifier(
                    self._full_alias_for_slot(
                        slot=slot, source_relation=source_relation,
                        alias_index={},
                    ),
                    quoted=True,
                ),
            )

        aliases = (
            aliases_by_slot_id.get(slot.id, [])
            if aliases_by_slot_id is not None
            else []
        )
        if aliases and isinstance(slot.key, _MATERIALISED_ORDER_KINDS):
            return exp.Column(this=exp.to_identifier(aliases[0], quoted=True))

        # A hidden ROW column ordered in an UNGROUPED query emits a split <relation>.<column> reference (the row is the
        # grain, so the bare ref is legal and Law 1 pulls the join in).
        key = slot.key
        row_key = key.source if isinstance(key, TimeTruncKey) else key
        if source_dataset is not None and isinstance(row_key, FieldKey):
            return self._joined_or_local_dim_expr(
                path=row_key.path, leaf=row_key.leaf,
                source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
            )

        # A local derived ORDER BY column renders through the same expansion a projected derived dimension gets, so both
        # spellings sort identically.
        if (
            source_dataset is not None
            and bundle is not None
            and isinstance(row_key, FieldSqlKey)
            and not row_key.path
        ):
            expr = self._derived_field_expr(
                key=row_key, source_dataset=source_dataset,
                source_relation=source_relation, bundle=bundle,
            )
            if expr is not None:
                return expr

        # A hidden row-level expression in an ungrouped query renders in place over the host's
        # rows; its relationship paths were collected with the order slots.
        if (
            source_dataset is not None and bundle is not None and slot.phase == Phase.ROW
            and isinstance(key, SLOT_COMPOSITE_KINDS)
        ):
            frame = self._scope_frame(
                dataset=source_dataset, relation=source_relation, bundle=bundle,
                allocator=self._gen_allocator or self._new_allocator(),
            )
            return render_value_key(key=key, ctx=RenderContext(scope=frame, dialect=self._dialect))

        raise NotImplementedError(
            f"ORDER BY references a hidden slot (id={slot.id!r}, key="
            f"{type(slot.key).__name__}) that was not resolved at plan "
            f"time — this is an internal invariant violation."
        )




def generate_from_planned(
    planned_query,
    *,
    bundle,
    dialect: str = "postgres",
) -> str:
    """Render a ``PlannedQuery`` to SQL."""
    generator = SQLGenerator(dialect=dialect)
    generator._gen_splice_chain = getattr(bundle, "splice_chain", ())
    generator._gen_splice_failures = getattr(bundle, "splice_failures", {})
    return cast(str, generator.generate_from_planned(planned_query, bundle=bundle))


def _finish_statement(
    statement: exp.Select,
    *,
    dialect: "str | SqlDialect",
    aliases: Sequence[str] = (),
    exempt: frozenset[str] = frozenset(),
    policy_scoped: bool = False,
    pretty: bool = True,
) -> str:
    """The one render of a composed statement: text, then identifier fitting, scope validation and the limit check."""
    d = _as_dialect(dialect)
    sql = d.rewrite_emitted_sql(
        d.rewrite_statement(project_untrusted_hops(statement)).sql(dialect=d.sqlglot_name, pretty=pretty), aliases=aliases,
        exempt=exempt,
    )
    maybe_validate_scopes(sql, dialect=d.sqlglot_name, allow_rls_correlation=policy_scoped)
    d.assert_no_overlimit_identifiers(sql, exempt=exempt)
    return sql


def _user_authored_exemptions(
    *, bundle: ResolvedSourceBundle, dialect: "str | SqlDialect",
) -> frozenset[str]:
    """Over-limit identifier-shaped tokens from every user-authored raw-SQL surface
    in ``bundle`` — dataset ``sql``/``sql_table``/``filters`` and per-field
    ``name``/``sql``/``filter`` across the source, referenced, per-stage source and
    inline-extension datasets. These pass through emission
    unfitted; SLayer-generated query-backed dataset SQL and synthetic stage-schema
    datasets (built later) are deliberately excluded."""
    d = dialect if isinstance(dialect, SqlDialect) else get_dialect(dialect)
    limit = d.max_identifier_bytes
    if limit is None:
        return frozenset()

    def _col_surfaces(col) -> list[str]:
        # ``fields`` entries may still be raw dicts (not yet coerced to DatasetField).
        if not isinstance(col, dict):
            texts = [col.name, col.definition_sql, col.filter, portable_text(col.expression), *col.native_texts]
            return [s for s in texts if s]
        expression = col.get("expression")
        texts = [col.get("name"), col.get("sql"), col.get("filter")]
        if isinstance(expression, str):
            texts.append(expression)
        elif isinstance(expression, dict):
            texts += [expression.get("sql"),
                      *(e.get("expression") for e in expression.get("dialects") or [] if isinstance(e, dict))]
        return [s for s in texts if isinstance(s, str) and s]

    surfaces: List[str] = []
    datasets = [
        *([bundle.population] if bundle.population is not None else []),
        *bundle.referenced_datasets,
        *bundle.stage_source_datasets.values(),
    ]
    for dataset in datasets:
        surfaces.extend(s for s in (dataset.sql, dataset.sql_table) if s)
        surfaces.extend(dataset.filters)
        for col in dataset.fields:
            surfaces.extend(_col_surfaces(col))
    tokens: set[str] = set()
    quote_styles = [d._identifier_quote_anchors()]
    for text in surfaces:
        tokens.update(
            overlimit_tokens(
                text, limit=limit, quote_styles=quote_styles,
                lexis=d.identifier_masking_lexis,
            )
        )
    return frozenset(tokens)


def _stage_relation(*, planned, is_root: bool) -> Optional[str]:
    """A stage's CTE name; ``None`` for the root."""
    if is_root:
        return None
    if planned.stage_schema is None:
        raise ValueError(
            "non-root stage must carry a stage_schema for CTE chaining; "
            f"source_relation={planned.source_relation!r}",
        )
    return planned.stage_schema.relation_name


_DATASET_STAMP = "slayer_model"


def _stamp_dataset(relation: Expression, *, dataset) -> Expression:
    """Mark ``relation`` as reading persisted ``dataset``."""
    relation.meta[_DATASET_STAMP] = dataset.name
    return relation


def read_datasets(statement: Expression) -> Set[str]:
    """Names of the datasets whose relations ``statement`` contains."""
    return {
        stamp for node in statement.find_all(exp.Table, exp.Subquery)
        if (stamp := node.meta.get(_DATASET_STAMP)) is not None
    }


_ASSOCIATION_STAMP = "slayer_association"


def _stamp_association(exists: Expression, *, dataset) -> Expression:
    """Mark ``exists`` as restricting ``dataset``'s rows by a correlated subquery."""
    exists.meta[_ASSOCIATION_STAMP] = dataset.name
    return exists


def association_datasets(statement: Expression) -> Set[str]:
    """Datasets whose filters ``statement`` restricts by a correlated ``EXISTS``."""
    return {
        stamp for node in statement.find_all(exp.Exists)
        if (stamp := node.meta.get(_ASSOCIATION_STAMP)) is not None
    }


def _prefix_closed(paths: Iterable[Tuple[str, ...]]) -> List[Tuple[str, ...]]:
    """``paths`` with every prefix, each parent before its children."""
    return list(dict.fromkeys(path[:i] for path in paths for i in range(1, len(path) + 1)))


def _as_dialect(dialect: "str | SqlDialect") -> SqlDialect:
    return dialect if isinstance(dialect, SqlDialect) else get_dialect(dialect)


class PlannedStatement(BaseModel):
    """A composed statement before its single render, with what that render fits and exempts."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    statement: exp.Select
    dialect: "str | SqlDialect"
    aliases: List[str] = Field(default_factory=list)
    exempt: FrozenSet[str] = frozenset()
    kept_stages: Set[str] = Field(default_factory=set)
    #: Datasets whose filters the statement restricts by a correlated subquery.
    associated: Set[str] = Field(default_factory=set)

    def render(self, *, policy_scoped: bool = False) -> str:
        """The statement's one render (``policy_scoped``: a session policy rewrote it)."""
        return _finish_statement(
            self.statement, dialect=self.dialect, aliases=self.aliases, exempt=self.exempt,
            policy_scoped=policy_scoped,
        )

    def mint_probe(self, *, policy_scoped: bool = False) -> TypeProbe:
        """The statement (built with ``type_probe``) as a :class:`TypeProbe`, minted at its one render."""

        def render(tree: Expr) -> str:
            assert isinstance(tree, exp.Select)
            return _finish_statement(
                tree, dialect=self.dialect, aliases=self.aliases, exempt=self.exempt, policy_scoped=policy_scoped,
                pretty=False,
            )

        return mint_type_probe(self.statement, dialect=_as_dialect(self.dialect), render=render)


def build_planned_statement(
    planned_queries,
    *,
    bundle,
    dialect: "str | SqlDialect" = "postgres",
    projection_aliases: "Sequence[str]" = (),
    type_probe: bool = False,
) -> PlannedStatement:
    """Compose a multi-stage DAG (``plan_stages`` output) as one statement, its settings attached;
    spliced stages nothing reaches are pruned. ``type_probe`` bounds it by the dialect's type probe."""
    kept: Set[str] = set()
    statement = project_untrusted_hops(_build_planned_stages_ast(
        planned_queries, bundle=bundle, dialect=dialect, kept_stages=kept,
    ))
    filter_associations = association_datasets(statement)
    if type_probe:
        statement = _as_dialect(dialect).type_probe(statement)
    spine_sizes = [p.spine.size for p in planned_queries if p.spine is not None]
    if spine_sizes:
        _as_dialect(dialect).attach_sequence_setting(statement, size=max(spine_sizes))
    # Semi-join pushdown emits a correlated EXISTS; attached unconditionally so SQL never depends on server state.
    if filter_associations or any(
        plan_has_semi_join_filters(p) for p in emitted_plans(planned_queries, kept_stages=kept)
    ):
        _as_dialect(dialect).attach_correlated_setting(statement)
    # Length-fit over-limit projection aliases from the plan-derived canonical keys, not parsed off the SQL —
    # BigQuery can't parse a backticked dotted alias.
    return PlannedStatement(
        statement=statement, dialect=dialect, aliases=list(projection_aliases),
        exempt=_user_authored_exemptions(bundle=bundle, dialect=dialect), kept_stages=kept,
        associated=filter_associations,
    )


def generate_planned_stages(
    planned_queries,
    *,
    bundle,
    dialect: "str | SqlDialect" = "postgres",
    projection_aliases: "Sequence[str]" = (),
    kept_stages: "Optional[Set[str]]" = None,
    read_set: "Optional[Set[str]]" = None,
    associated: "Optional[Set[str]]" = None,
) -> str:
    """Render a multi-stage DAG (``plan_stages`` output) to one SQL string; the stage relations
    emitted land in ``kept_stages``, the datasets the statement reads in ``read_set``, those whose
    filters it restricts by a correlated subquery in ``associated``."""
    built = build_planned_statement(
        planned_queries, bundle=bundle, dialect=dialect, projection_aliases=projection_aliases,
    )
    if kept_stages is not None:
        kept_stages.update(built.kept_stages)
    if read_set is not None:
        read_set.update(read_datasets(built.statement))
    if associated is not None:
        associated.update(built.associated)
    return built.render()


def _build_planned_stages_ast(
    planned_queries: List[PlannedQuery], *, bundle, dialect: "str | SqlDialect", kept_stages: "Optional[Set[str]]" = None,
) -> exp.Select:
    """Compose a multi-stage DAG (``plan_stages`` output) as one statement AST."""
    if not planned_queries:
        raise ValueError("generate_planned_stages requires at least one stage")
    if len(planned_queries) == 1:
        single = SQLGenerator(dialect=dialect)
        single._gen_splice_chain = getattr(bundle, "splice_chain", ())
        single._gen_splice_failures = getattr(bundle, "splice_failures", {})
        # The stage's own universe: its query extensions overlaid.
        return single._build_from_planned(planned_queries[0], bundle=planned_queries[0].stage_bundle or bundle)

    schema_by_name = {
        p.stage_schema.relation_name: p.stage_schema
        for p in planned_queries
        if p.stage_schema is not None
    }

    # One generation scope spans every stage (shared allocator + rendered-producer map) so hoisted internal CTEs stay
    # globally unique and a shared producer renders once.
    generator = SQLGenerator(dialect=dialect)
    generator.install_generation(reserve=schema_by_name.keys())

    # Hoist each stage's internal CTEs and de-WITH its body into one flat WITH
    # assembled by declared edges (sql P6) — a nested WITH inside a stage CTE is
    # invalid on T-SQL. Every entry of a stage's statement declares that
    # statement's sibling reads; stage relations also declare their hoisted CTEs
    # and body reuses. Plan order is the insertion tiebreak (byte-stable output).
    _check_stage_order(planned_queries)
    generator._gen_stage_relations = frozenset(
        p.stage_schema.relation_name for p in planned_queries[:-1] if p.stage_schema is not None
    )
    stage_entries: List[CteEntry] = []
    root_entries: List[CteEntry] = []
    root_final: Optional[exp.Select] = None
    seeds: Set[str] = set(planned_queries[-1].stage_reads)
    for planned in planned_queries:
        relation = _stage_relation(planned=planned, is_root=planned is planned_queries[-1])
        stage_bundle = planned.stage_bundle
        if stage_bundle is None:
            raise ValueError(
                f"stage {planned.source_relation!r} carries no stamped per-stage bundle; "
                "multi-stage plans come from plan_stages",
            )
        generator._gen_stage_reads = frozenset(planned.stage_reads)
        generator._gen_splice_chain = stage_bundle.splice_chain
        generator._gen_splice_failures = stage_bundle.splice_failures
        spliced = is_spliced(planned)
        with generator._stage_scope(relation):
            statement = generator._build_from_planned(
                planned, bundle=stage_bundle, reuse_allocator=True,
            )
            if relation is None:
                root_entries, root_final = generator._split_root_ctes(statement)
                root_entries = _with_stage_reads(entries=root_entries, reads=planned.stage_reads)
                continue
            hoisted, body = generator._split_ast_ctes(statement)
        if not spliced:
            seeds.update([relation, *(h.name for h in hoisted)])
        stage_entries.extend(_with_stage_reads(entries=hoisted, reads=planned.stage_reads))
        assert planned.stage_schema is not None  # _stage_relation guarantees it
        stage_entries.append(CteEntry(
            name=relation,
            query=build_flat_rename_wrapper(
                stage=planned.stage_schema.display_name,
                source_relation=planned.stage_field_relation,
                inner=body,
                expected_columns=[c.name for c in planned.stage_schema.fields],
                dialect=_as_dialect(dialect).sqlglot_name,
            ),
            depends_on=_merged_deps(
                [h.name for h in hoisted],
                generator._reuse_deps_of(relation),
                planned.stage_reads,
            ),
        ))

    assert root_final is not None
    seeds.update(e.name for e in root_entries)
    entries = reachable_cte_entries(entries=[*stage_entries, *root_entries], seeds=seeds)
    kept = {e.name for e in entries} & generator._gen_stage_relations
    for planned in planned_queries[:-1]:
        assert planned.stage_schema is not None
        if planned.splice_conflict and planned.stage_schema.relation_name in kept:
            raise ValueError(planned.splice_conflict)
    if kept_stages is not None:
        kept_stages.update(kept)
    combined = assemble_with_chain(
        entries=entries, final=root_final,
        external_names=generator._external_cte_names(),
    )
    _fit_overlimit_identifiers(
        combined, dialect=dialect, exempt=_user_authored_exemptions(bundle=bundle, dialect=dialect),
        stage_fields={
            c.name for p in planned_queries[:-1] if p.stage_schema is not None
            for c in p.stage_schema.fields
        },
    )
    return combined


def _fit_overlimit_identifiers(
    tree, *, dialect: "str | SqlDialect", exempt: AbstractSet[str], stage_fields: AbstractSet[str],
) -> None:
    """Length-fit over-limit SLayer-minted identifiers in place the way the emission
    pass fits quoted ones: every unquoted one (stage identities, stage column
    references) and the stage column names wherever they occur, so a definition and
    its references agree; user-authored ``exempt`` names pass unless they name a
    stage column."""
    d = _as_dialect(dialect)
    if d.max_identifier_bytes is None:
        return
    for ident in tree.find_all(exp.Identifier):
        name = ident.name
        if name not in stage_fields and (ident.quoted or name in exempt):
            continue
        fitted = d.fit_alias(name)
        if fitted != name:
            ident.set("this", fitted)


def _check_stage_order(planned_queries) -> None:
    """Fail closed unless every stage's ``stage_reads`` names an earlier stage."""
    spelling = {
        p.stage_schema.relation_name: p.stage_schema.display_name
        for p in planned_queries if p.stage_schema is not None
    }
    earlier: Set[str] = set()
    for planned in planned_queries:
        name = (
            planned.stage_schema.relation_name
            if planned.stage_schema is not None else "<root>"
        )
        late = [r for r in planned.stage_reads if r not in earlier]
        if late:
            raise ValueError(
                f"stage {spelling.get(name, name)!r} reads sibling(s) "
                f"{[spelling.get(r, r) for r in late]!r} not planned before it; "
                "planned stages must be in dependency order",
            )
        earlier.add(name)


def _merged_deps(*groups: Sequence[str]) -> List[str]:
    """Concatenate dependency lists, dropping repeats (first occurrence wins)."""
    return list(dict.fromkeys(d for group in groups for d in group))


def _with_stage_reads(*, entries: List[CteEntry], reads: Sequence[str]) -> List[CteEntry]:
    """``entries`` with their statement's sibling reads added as prerequisites."""
    if not reads:
        return entries
    return [
        e.model_copy(update={"depends_on": _merged_deps(e.depends_on, reads)})
        for e in entries
    ]
