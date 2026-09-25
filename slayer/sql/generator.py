"""SQL generator — converts a ``PlannedQuery`` to SQL via sqlglot AST."""

import logging
import re
from collections.abc import Sequence
from contextlib import contextmanager
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from decimal import Decimal
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    BUILTIN_AGGREGATION_REQUIRED_PARAMS,
    DataType,
    TimeGranularity,
)
from pydantic import BaseModel, ConfigDict, field_validator

from slayer.core.errors import AggregationNotAllowedError, MaterialisationStageError
from slayer.core.enums import RANK_FAMILY_TRANSFORMS
from slayer.core.keys import BOOL_CONNECTIVE_OPS, KIND_POLICY, REGROUP_LEAF_PREFIX, VALUE_KEY_TYPES, AggregateKey, ArithmeticKey, BetweenKey, ColumnKey, ColumnSqlKey, InKey, Phase, ScalarCallKey, StarKey, TimeTruncKey, TransformKey, column_leaf, column_path, is_boolean_shaped, shift_offset_of, source_anchor_path, substitute_value_keys, walk_value_keys
from slayer.core.join_walker import physical_join_pairs, resolve_hop, terminal_model
from slayer.core.models import VALUE_PLACEHOLDER, Aggregation, rendered_formula, reserved_value_param_message
from slayer.core.refs import (
    EXPRESSION_SOURCE_KINDS as _EXPRESSION_SOURCE_KINDS,
    agg_kwarg_canonical_str,
    expression_source_leaf,
)
from slayer.core.window_duration import parse_window_duration as _parse_window_duration
from slayer.sql.column_expansion import (
    is_trivial_base,
    collect_root_scope_joined_paths,
    expand_column_definition_parts_sync,
    requalify_default_references,
    resolve_default_reference_paths,
    wrap_column_filter,
)
from slayer.ir.planned import MaskTyping, RankedGrainMember, StageKind, ValueSlot, regroup_producer_identity
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    model_from_stage_schema,
    stage_bundle_with_siblings,
)
from slayer.sql._identifier_fit import overlimit_tokens
from slayer.sql import staged_plan
from slayer.sql.dialects import SqlDialect, get_dialect
from slayer.sql.dialects.base import TimeUnit, is_stat_agg1, is_stat_agg2
from slayer.sql.naming import (
    OUTER_WRAP_ALIAS,
    AliasAllocator,
    canonical_aggregate_alias,
    cte_name_from_alias,
    dialect_folds_case,
    maybe_quote_ident,
    quote_mixed_case_identifiers,
    result_key,
    result_key_from_alias,
    time_trunc_result_key,
)
from slayer.sql.render.cte_assembly import CteEntry, assemble_with_chain
from slayer.sql.render.nodes import Node, fusion_blockers
from slayer.sql.render.joins import (
    build_grain_joinback_condition,
    grain_alias_column,
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
    RANKED_SOURCE_ALIAS,
    RankedGrainProjection,
    build_rank_column,
    build_ranked_cte_select,
    build_ranked_pick,
    ranked_ordered,
)
from slayer.sql.render.aggregates import (
    DISPATCH_DISTINCT,
    DISPATCH_STAT,
    is_builtin_agg,
    resolve_agg_entry,
)
from slayer.sql.render.parse import parse_expression, parse_predicate
from slayer.sql.sql_template import SqlTemplate, SqlTemplateError, sql_template
from slayer.sql.render.value_expr import (
    AliasFacilities,
    CompositeFacilities,
    FilterFacilities,
    RenderContext,
    _ALIAS_SLOTTED_KINDS as _ALIAS_SLOTTED_RENDER_KINDS,
    _ranked_value_cast_type,
    _wrap_cast_for_type,
    contains_aggregate,
    render_value_key,
    rewrite_log_alias,
)
from slayer.sql.scope import ScopeFrame
from slayer.sql.scope_check import maybe_validate_scopes
from slayer.ir.bound import BoundExpr
from slayer.sql.stage_wrapper import (
    build_flat_rename_wrapper,
    unmangle_dotted_table_refs,
)




class ResolvedAggKwarg(BaseModel):
    """A resolved parametric-aggregation kwarg value (2-kind tag)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["expr", "str"]
    value: Union[Expression, str]


class AggRenderSpec(BaseModel):
    """Typed input record for the dialect-aware aggregation"""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    sql: str | None

    name: str

    model_name: str

    aggregation: str

    alias: str

    aggregation_def: Optional[Aggregation] = None

    agg_kwargs: Dict[str, ResolvedAggKwarg] = {}

    @field_validator("agg_kwargs", mode="before")
    @classmethod
    def _coerce_agg_kwargs(cls, v: Any) -> Any:
        """Coerce bare ``str`` kwarg values to ``ResolvedAggKwarg(kind="str")``;"""
        if not isinstance(v, dict):
            return v
        coerced: Dict[str, Any] = {}
        for key, val in v.items():
            if isinstance(val, (ResolvedAggKwarg, dict)):
                coerced[key] = val
            elif isinstance(val, str):
                coerced[key] = ResolvedAggKwarg(kind="str", value=val)
            else:
                coerced[key] = val  # bool / None / other → Pydantic rejects
        return coerced

    time_column: Optional[str] = None

    type: Optional[DataType] = None

    column_type: Optional[DataType] = None


def _strip_declared_cast(expr: Expression) -> Expression:
    """Unwrap one declared-type ``CAST`` a derived-column expansion added."""
    return expr.this if isinstance(expr, exp.Cast) else expr


class _WindowedEmission(BaseModel):
    """Renderer-internal field bundle for one trailing-window emission"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    aggregate_slot_id: str
    agg: str
    window_parts: List[Tuple[int, str]]
    window_granularity: str
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
        or any(m.typing == MaskTyping.MEASURE for m in planned_query.masks)
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
    dims: List[str] = []
    other_tds: List[str] = []
    for rs in planned_query.row_slots:
        if rs.hidden or rs.id == bucket_sid:
            continue
        if isinstance(rs.key, TimeTruncKey):
            other_tds.append(rs.id)
        else:
            dims.append(rs.id)
    grain = [*dims, bucket_sid, *other_tds]
    visible_row_ids = {s.id for s in planned_query.row_slots if not s.hidden}
    if set(grain) != visible_row_ids or set(planned_query.projection) != {
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
        or any(m.typing == MaskTyping.MEASURE for m in planned_query.masks)
        or planned_query.order
        or planned_query.limit is not None
        or planned_query.offset is not None
    ):
        raise RuntimeError(
            "Ranked kernel producer carries structure beyond its grain + "
            "ranked aggregate; synthesis and rendering disagree."
        )
    agg_slot = planned_query.aggregate_slots[0]
    grain = [
        RankedGrainMember(host_slot_id=s.id, ranked_key=s.key)
        for s in planned_query.row_slots
        if not s.hidden
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


#: Test hook: force measure masks onto the general materialize-and-filter path
#: (outer WHERE over hidden columns) instead of the HAVING lowering.
_FORCE_MASK_FALLBACK = False


class _LoweredFilter(BaseModel):
    """One mask (typed expression) or Mode-A text with its render placement."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    phase: Phase
    text: Optional[str] = None
    expression: Optional[BoundExpr] = None


class _LoweredPositions(BaseModel):
    """Per-plan lowering output: placed filter entries + scoped order entries."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    filters: List["_LoweredFilter"]
    #: Measure masks that resolve only after attachment (combined placeholders) —
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
    """Placement from the planner stage (D5): field → base WHERE; measure →
    HAVING at BASE, the combined outer WHERE at PRODUCER / COMBINED (or reading
    a combined-attached dual-role value), the outer wrapper at DERIVED. Mode-A
    texts render in the base WHERE between the date-range and user masks."""
    slots_by_id = {s.id: s for s in _plan_slots(planned_query)}
    slot_id_by_key = {s.key: s.id for s in slots_by_id.values()}
    combined_attached = _combined_attached_slot_ids(planned_query, slot_id_by_key)
    outer_ids: List[str] = []

    def _lower_mask(mask) -> _LoweredFilter:
        slot = slots_by_id[mask.slot_id]
        stage_kind = slot.stage.kind if slot.stage is not None else None
        if mask.typing == MaskTyping.FIELD:
            phase = Phase.ROW
        elif stage_kind is StageKind.DERIVED:
            phase = Phase.POST
        else:
            phase = Phase.AGGREGATE
            outer = stage_kind in (
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

    n_date = planned_query.n_date_range_masks
    entries: List[_LoweredFilter] = [
        _lower_mask(m) for m in planned_query.masks[:n_date]
    ]
    entries.extend(
        _LoweredFilter(id=mf.id, phase=Phase.ROW, text=mf.text)
        for mf in planned_query.mode_a_filters
    )
    entries.extend(_lower_mask(m) for m in planned_query.masks[n_date:])

    order = _lower_order_entries(
        planned_query=planned_query,
        slots_by_id=slots_by_id,
        slot_id_by_key=slot_id_by_key,
        combined_attached=combined_attached,
    )
    return _LoweredPositions(
        filters=entries, outer_where_ids=outer_ids, order=order,
    )


def _lower_order_entries(
    planned_query,
    *,
    slots_by_id: Dict[str, ValueSlot],
    slot_id_by_key: Dict[Any, str],
    combined_attached: Set[str],
) -> List[ScopedOrder]:
    order: List[ScopedOrder] = []
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
                slots_by_id=slots_by_id,
                slot_by_key=slot_id_by_key,
                combined_attached=combined_attached,
                public_projection=list(planned_query.projection),
            ),
            nulls=entry.nulls,
        ))
    return order


def _composite_operand_in_isolated_cte(
    slot: ValueSlot,
    *,
    slots_by_id: Dict[str, ValueSlot],
    slot_by_key: Dict[Any, str],
    combined_attached: Set[str],
    ranked_slot_ids: "AbstractSet[str]",
) -> bool:
    for dep in walk_value_keys(slot.key):
        dep_sid = slot_by_key.get(dep)
        if dep_sid is None or dep_sid == slot.id:
            continue
        dep_stage = slots_by_id[dep_sid].stage
        if (
            dep_sid in ranked_slot_ids
            or dep_sid in combined_attached
            or (dep_stage is not None and dep_stage.kind is StageKind.PRODUCER)
        ):
            return True
    return False


def _classify_order_scope(
    *,
    slot: ValueSlot,
    slots_by_id: Dict[str, ValueSlot],
    slot_by_key: Dict[Any, str],
    combined_attached: Set[str],
    public_projection: List[str],
    ranked_slot_ids: "AbstractSet[str]" = frozenset(),
) -> OrderScope:
    """Name the scope that PRODUCES ``slot``'s value, from its planner stage
    plus the attach facts (ranked kernel, combined-attached dual-role values);
    a composite is OUTER_COMPOSITE when any operand lives in an isolated CTE."""
    if slot.id in ranked_slot_ids:
        return OrderScope.RANKED_CTE
    if slot.id in combined_attached:
        return OrderScope.CROSS_MODEL_CTE
    stage_kind = slot.stage.kind if slot.stage is not None else None
    if stage_kind is StageKind.PRODUCER:
        if isinstance(slot.key, AggregateKey) and any(
            kw == "window" for kw, _ in slot.key.kwargs
        ):
            return OrderScope.WINDOWED_CTE
        return OrderScope.CROSS_MODEL_CTE
    if isinstance(slot.key, TransformKey):
        return OrderScope.TRANSFORM_STEP
    if isinstance(slot.key, (ArithmeticKey, ScalarCallKey)) and _composite_operand_in_isolated_cte(
        slot,
        slots_by_id=slots_by_id,
        slot_by_key=slot_by_key,
        combined_attached=combined_attached,
        ranked_slot_ids=ranked_slot_ids,
    ):
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
_SLOT_COMPOSITE_KINDS = tuple(
    k for k in VALUE_KEY_TYPES if KIND_POLICY[k].slot_composite
)
_MATERIALISED_ORDER_KINDS = tuple(
    k for k in VALUE_KEY_TYPES if KIND_POLICY[k].materialised_order
)

_BUILTIN_BAREARG_AGGS_LOCAL_SLICE: frozenset[str] = BUILTIN_AGGREGATIONS

# sqlglot rewrites log10/log2 into 2-arg LOG(base,x), breaking dialects lacking 2-arg LOG; rewrite back to Anonymous.


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

# Safe agg-param values: identifiers, qualified names, numeric literals.
_SAFE_AGG_PARAM_RE = re.compile(
    r'^(?:'
    r'[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*'  # identifier or qualified name
    r'|'
    r'-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?'  # numeric literal
    r'|'
    r'\(-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?\)'  # parenthesised numeric literal
    r')$'
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


def _is_fragment(*, name: str, placeholders: Optional[frozenset[str]]) -> bool:
    """A formula-less aggregation treats every string param as a fragment."""
    return placeholders is None or name in placeholders


def _percentile_literal(p: Expression) -> Expression:
    """The numeric literal inside ``p`` (optionally signed / parenthesised), validated to [0, 1]."""
    node, negative = p, False
    while isinstance(node, (exp.Paren, exp.Neg)):
        negative ^= isinstance(node, exp.Neg)
        node = node.this
    if not isinstance(node, exp.Literal) or node.is_string:
        raise ValueError(
            f"Aggregation 'percentile' parameter 'p' must be a numeric literal "
            f"in [0, 1]; got {p.sql()!r}."
        )
    try:
        value = Decimal(node.this)
    except ArithmeticError:
        value = Decimal("NaN")
    if negative:
        value = -value
    if not value.is_finite() or not 0 <= value <= 1:
        raise ValueError(
            f"Aggregation 'percentile' parameter 'p' must be in [0, 1]; got {p.sql()}."
        )
    return node.copy()


def _shift_preserves_bucket_starts(bucket: "TimeGranularity", shift: str) -> bool:
    return shift.lower() in _BUCKET_ALIGNED_SHIFT_UNITS.get(str(bucket), frozenset())


def _is_host_grain(key) -> bool:
    """True for an ``AggregateKey`` marked ``locus="host"``."""
    return getattr(key, "locus", "target") == "host"


def _first_bare_column_name(key) -> Optional[str]:
    """Return the leaf name of the first bare column reference inside a"""

    if isinstance(key, ColumnKey):
        return key.leaf
    if isinstance(key, ColumnSqlKey):
        return key.column_name
    if isinstance(key, ArithmeticKey):
        children = key.operands
    elif isinstance(key, ScalarCallKey):
        children = key.args
    elif isinstance(key, TransformKey):
        children = [key.input]
    else:
        return None
    for child in children:
        name = _first_bare_column_name(child)
        if name is not None:
            return name
    return None


# --- Transform-input shape classification, shared by the series-regime
# time_shift selector and the consecutive_periods emitter. ---

# SCALAR_PASSTHROUGH members whose result is a string (no defined truthiness);
# length / instr return numbers and are intentionally excluded.
_STRING_VALUED_SCALARS = frozenset({
    "lower", "upper", "trim", "ltrim", "rtrim",
    "replace", "substr", "substring", "concat",
})

# Real ValueKey args (a ScalarCallKey / iif may also carry raw scalar literals).
_COMPOUND_VALUE_KEYS = (
    ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey,
    AggregateKey, TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey, InKey,
)


def _validate_consecutive_periods_input(*, op: str, inner) -> None:
    """Enforce the ``consecutive_periods`` predicate typing contract: a
    top-level string-valued scalar call has no truthiness; a boolean-shaped node
    is legal only at the predicate top level or in an ``iif`` condition."""
    if (
        isinstance(inner, ScalarCallKey)
        and inner.name.lower() in _STRING_VALUED_SCALARS
    ):
        raise ValueError(
            f"{op!r} cannot use a string-valued predicate ({inner.name}(...)): a "
            f"string has no truthiness. Compare it explicitly (e.g. "
            f"`length(...) > 0`) to form a predicate."
        )
    _walk_cp_predicate(op=op, key=inner, expect="either")


def _assert_cp_shape(*, op: str, key, expect: str, node_is_bool: bool) -> None:
    """Enforce the boolean-vs-value expectation at one node; raise on mismatch."""
    if expect == "bool" and not node_is_bool:
        raise ValueError(
            f"{op!r}: 'and' / 'or' / 'not' require boolean-shaped operands (a "
            f"comparison, a null test, BETWEEN, IN, or another connective); got "
            f"{type(key).__name__}."
        )
    if expect == "value" and node_is_bool:
        raise ValueError(
            f"{op!r}: a boolean-shaped predicate cannot appear in a value "
            f"position (arithmetic operand, scalar-call argument, or IN / BETWEEN "
            f"operand); only iif's condition and the top-level predicate accept a "
            f"boolean. Got {type(key).__name__}."
        )


def _walk_cp_scalar_call(*, op: str, key) -> None:
    """Recurse into a scalar call: an ``iif`` condition accepts either shape;
    every remaining compound argument must be value-shaped."""
    if key.name == "iif" and key.args:
        _walk_cp_predicate(op=op, key=key.args[0], expect="either")
        rest = key.args[1:]
    else:
        rest = key.args
    for a in rest:
        if isinstance(a, _COMPOUND_VALUE_KEYS):
            _walk_cp_predicate(op=op, key=a, expect="value")


def _cp_value_operands(key) -> list:
    """Value-position sub-keys of a BETWEEN / IN predicate (its column, bounds,
    and IN set) — each must be value-shaped, never a nested boolean."""
    if isinstance(key, BetweenKey):
        return [key.column, key.low, key.high]
    return [key.column, *key.values]  # InKey


def _walk_cp_predicate(*, op: str, key, expect: str) -> None:
    """Recursively check the boolean-vs-value contract. ``expect`` is 'bool'
    (must be boolean-shaped), 'value' (must not be), or 'either' (predicate top
    level / iif condition)."""
    _assert_cp_shape(
        op=op, key=key, expect=expect, node_is_bool=is_boolean_shaped(key),
    )
    if isinstance(key, ArithmeticKey):
        child_expect = "bool" if key.op in BOOL_CONNECTIVE_OPS else "value"
        for o in key.operands:
            _walk_cp_predicate(op=op, key=o, expect=child_expect)
    elif isinstance(key, ScalarCallKey):
        _walk_cp_scalar_call(op=op, key=key)
    elif isinstance(key, (BetweenKey, InKey)):
        for sub in _cp_value_operands(key):
            _walk_cp_predicate(op=op, key=sub, expect="value")


_WINDOW_UNIT_SQL = {
    "y": "year",
    "m": "month",
    "w": "week",
    "d": "day",
    "h": "hour",
    "min": "minute",
    "s": "second",
}
_WINDOW_UNIT_SQLITE = {
    "y": "years",
    "m": "months",
    "w": "days",
    "d": "days",
    "h": "hours",
    "min": "minutes",
    "s": "seconds",
}


def _validate_agg_param_value(value: str, param_name: str, agg_name: str) -> None:
    """Validate that a query-time aggregation parameter value is safe for substitution."""
    if not _SAFE_AGG_PARAM_RE.match(value):
        raise ValueError(
            f"Unsafe value '{value}' for parameter '{param_name}' in "
            f"aggregation '{agg_name}'. Parameter values must be column names "
            f"(e.g., 'quantity') or numeric literals (e.g., '0.95')."
        )






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











# A bare-identifier Column.sql renames a physical column; dots are rejected (a dotted ref is a crossing, not a column
# here).
_BARE_IDENT_RE = re.compile(r"[A-Za-z_]\w*")


def _apply_joins(*, select, joins):
    """Apply ``(join_expr, on_expr, join_type)`` triples to ``select`` in order,"""
    for join_expr, on_expr, join_type in joins:
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
            continue
        idx = outer_alias_index.setdefault(sid, 0)
        alias = (
            all_aliases[idx] if idx < len(all_aliases) else all_aliases[-1]
        )
        outer_alias_index[sid] = idx + 1
        public_aliases.append(alias)
    return public_aliases


class _SemiJoinOps:
    """Alias / column / table callbacks of one semi-join EXISTS."""

    def __init__(
        self, *, alias: Callable[[Tuple[str, ...]], str],
        col: Callable[[str, str], exp.Column],
        table: Callable[[Any], Expression],  # pyright: ignore[reportPrivateImportUsage] — sqlglot exports it
        attached: Set[Tuple[str, ...]],
    ) -> None:
        self.alias = alias
        self.col = col
        self.table = table
        self.attached = attached

    def is_attached(self, hop) -> bool:
        return tuple(hop.node_path) in self.attached

    def eqs(self, *, hop, parent: str, parent_name=lambda c: c) -> List[Any]:
        alias = self.alias(tuple(hop.node_path))
        return [
            exp.EQ(this=self.col(parent, parent_name(pc)), expression=self.col(alias, hc))
            for pc, hc in hop.join_pairs
        ]

    def join(self, *, inner, hop, eqs: List[Any]):
        on = exp.and_(*eqs) if len(eqs) > 1 else eqs[0]
        return inner.join(
            self.table(hop), on=on, join_type="left" if hop.null_extended else "inner",
        )


def _spine_projection(*, ops: _SemiJoinOps, hops, ident):
    """The spine's distinct outer correlation columns: ``{(parent, col): name}``
    and their projection list."""
    spine_cols: Dict[Tuple[str, str], str] = {}
    projection: List[Any] = []
    for hop in hops:
        if not ops.is_attached(hop):
            continue
        parent_path = tuple(hop.node_path[:-1])
        parent = ops.alias(parent_path)
        prefix = f"{parent}__" if parent_path else ""
        for parent_col, _hop_col in hop.join_pairs:
            if (parent, parent_col) in spine_cols:
                continue
            spine_cols[(parent, parent_col)] = prefix + parent_col
            projection.append(
                exp.alias_(ops.col(parent, parent_col), ident(prefix + parent_col)))
    return spine_cols, projection


def _semi_join_spine(*, ops: _SemiJoinOps, hops, inner, ident):
    """Spine shape: FROM a one-row derived table projecting the outer correlation
    keys; attached hops LEFT/INNER join ON it, deeper hops ON their parent."""
    spine_alias = ops.alias(("__slayer_spine",))
    spine_cols, projection = _spine_projection(ops=ops, hops=hops, ident=ident)
    inner = inner.from_(exp.Subquery(
        this=exp.select(*projection), alias=exp.to_identifier(spine_alias),
    ))
    for hop in hops:
        parent = ops.alias(tuple(hop.node_path[:-1]))
        if ops.is_attached(hop):
            eqs = ops.eqs(
                hop=hop, parent=spine_alias,
                parent_name=lambda c, p=parent: spine_cols[(p, c)],
            )
        else:
            eqs = ops.eqs(hop=hop, parent=parent)
        inner = ops.join(inner=inner, hop=hop, eqs=eqs)
    return inner


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
        table = ops.table(hop)
        inner = inner.join(table, join_type="cross") if first_seen else inner.from_(table)
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
    source_model: Any
    source_relation: Any


class SQLGenerator:
    """Generates SQL from a typed ``PlannedQuery`` (from ``stage_planner``)."""

    def __init__(
        self, dialect: "str | SqlDialect" = "postgres",
        force_unfused: bool = False,
    ):
        if isinstance(dialect, SqlDialect):
            self._dialect: SqlDialect = dialect
        else:
            self._dialect = get_dialect(dialect)
        # Synthetic fusion blocker for lowering-soundness parity tests.
        self._force_unfused = force_unfused
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

    def _scope_frame(self, *, model, relation, bundle, allocator, attached_columns=None):
        """Build a ``ScopeFrame`` rooted at ``model`` / ``relation`` on the"""
        return ScopeFrame(
            scope_id=allocator.next_scope_id(relation),
            root_model=model,
            root_relation=relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=allocator,
            attached_columns=dict(attached_columns or {}),
        )

    def _alias_render_ctx(
        self, *, slot_id_by_key, available_alias_by_slot_id,
        composite_alias_slot_ids: Optional[Set[str]] = None,
    ):
        """RenderContext carrying only the plain slot-alias facilities."""
        return RenderContext(
            dialect=self._dialect,
            aliases=AliasFacilities(
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
                composite_alias_slot_ids=composite_alias_slot_ids or set(),
            ),
        )

    @staticmethod
    def _dimension_composite_slot_ids(planned_query) -> Set[str]:
        """Computed-dimension slots keyed by a composite: post-aggregation scopes
        must reference their grouped alias, never re-render the expression."""
        return {
            s.id
            for name in ("row_slots", "aggregate_slots", "combined_expression_slots")
            for s in getattr(planned_query, name, None) or []
            if s.is_dimension and not isinstance(s.key, _ALIAS_SLOTTED_RENDER_KINDS)
        }

    def _outer_wrapper_render_ctx(
        self, *, slot_by_key, cross_model_agg_slot_to_cm, aliases_by_slot_id,
    ):
        """RenderContext for the outer-wrapper composite/filter render pass."""
        return RenderContext(
            dialect=self._dialect,
            aliases=self._outer_wrapper_alias_facilities(
                slot_by_key=slot_by_key,
                cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                aliases_by_slot_id=aliases_by_slot_id,
            ),
            filters=FilterFacilities(paren_comparison_operands=True),
        )

    @staticmethod
    def _reserve_model_column_names(allocator: AliasAllocator, model) -> None:
        """Reserve every name a ``<relation>.*`` projection of ``model`` can"""
        names: List[str] = []
        for c in model.columns:
            names.append(c.name)
            sql = getattr(c, "sql", None)
            if sql and _BARE_IDENT_RE.fullmatch(sql.strip()):
                names.append(sql.strip())
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
        """Build a (possibly schema-qualified) table reference with mixed-case"""
        table = exp.to_table(name).transform(self._quote_mixed_case_identifiers)
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

    def _ordered(
        self, order_col: Expression, *, ascending: bool,
        nulls: str = "default",
    ) -> exp.Ordered:
        """Build an ``exp.Ordered`` node via the dialect strategy."""
        return self._dialect.build_ordered(
            order_col, descending=not ascending, nulls=nulls,
        )





    def _build_time_offset_expr(self, col_expr: Expression, offset: int,
                                granularity: TimeGranularity | TimeUnit) -> Expression:
        """Apply a time offset to a column expression (dialect-aware)."""
        return self._dialect.build_time_offset_expr(
            col_expr=col_expr, offset=offset, granularity=granularity,
        )

    def _duration_interval_exprs(self, duration: str, sign: int = 1) -> list[Expression]:
        """Return per-unit AST nodes that `_add_intervals_expr` will chain."""
        parts = _parse_window_duration(duration)
        return self._dialect.duration_interval_exprs(parts=parts, sign=sign)

    def _granularity_interval_expr(self, granularity: TimeGranularity, sign: int = 1) -> list[Expression]:
        if granularity == TimeGranularity.QUARTER:
            duration = "3m"
        elif granularity in (TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY):
            # A WEEK_SUNDAY shift spans one calendar week, same as WEEK (only the anchor differs).
            duration = "1w"
        else:
            unit_to_duration = {
                TimeGranularity.YEAR: "1y",
                TimeGranularity.MONTH: "1m",
                TimeGranularity.DAY: "1d",
                TimeGranularity.HOUR: "1h",
                TimeGranularity.MINUTE: "1min",
                TimeGranularity.SECOND: "1s",
            }
            duration = unit_to_duration[granularity]
        return self._duration_interval_exprs(duration, sign=sign)

    def _add_intervals_expr(self, expr: Expression, intervals: list[Expression],
                            sign: int = 1) -> Expression:
        """Compose `expr ± interval [± interval ...]` as AST."""
        return self._dialect.add_intervals_expr(
            expr=expr, intervals=intervals, sign=sign,
        )

    def _build_date_trunc(self, col_expr: Expression, granularity: TimeGranularity) -> Expression:
        """Build a DATE_TRUNC expression. Dispatches to the dialect strategy"""
        return self._dialect.build_date_trunc(
            col_expr=col_expr, granularity=granularity,
        )

    def _rewrite_log_aliases(self, node: Expression) -> Expression:
        """Thin delegator to the shared log-alias policy in"""
        return rewrite_log_alias(node, dialect=self._dialect)

    def _resolve_sql(
        self,
        sql: Optional[str],
        name: str,
        model_name: str,
        type: Optional[DataType] = None,
    ) -> Expression:
        """Resolve an enriched SQL expression to a sqlglot AST node."""
        if sql is None:
            return exp.Column(this=self._to_ident(name), table=exp.to_identifier(model_name))
        if sql.isidentifier():
            return exp.Column(this=self._to_ident(sql), table=exp.to_identifier(model_name))
        return _wrap_cast_for_type(
            expr=self._parse(sql), dt=self._dialect.declared_cast_type(type),
        )

    def _resolve_value_ast(self, spec: AggRenderSpec) -> Expression:
        """Resolve ``spec.sql`` (or ``spec.name``) into a fully-qualified AST."""
        return self._resolve_sql(
            sql=spec.sql,
            name=spec.name,
            model_name=spec.model_name,
            type=spec.column_type,
        )

    def _agg_param_ast(
        self, value: "ResolvedAggKwarg | str", *, model_name: str,
    ) -> Expression:
        """Resolve a parametric-agg param value to a sqlglot AST."""
        if isinstance(value, ResolvedAggKwarg):
            if value.kind == "expr":
                # Return a copy: sqlglot re-parents a node on attach, so sharing one kwarg AST across trees corrupts the
                # first.
                return value.value.copy() if isinstance(value.value, Expression) \
                    else self._parse(value.value)
            raw = value.value
        else:
            raw = value
        return self._resolve_sql(sql=raw, name=raw, model_name=model_name)

    def _resolve_agg_param(
        self,
        spec: AggRenderSpec,
        *,
        name: str,
        agg_name: str,
    ) -> Expression:
        """Pull a named aggregation parameter, with query-time SQL-injection"""
        value: "ResolvedAggKwarg | str | None" = None
        if name in spec.agg_kwargs:
            value = spec.agg_kwargs[name]
            # Guard only the untrusted str forms; kind="expr" is a trusted bind-time-resolved expression, embedded
            # verbatim.
            if isinstance(value, ResolvedAggKwarg):
                if value.kind == "str":
                    _validate_agg_param_value(value.value, name, agg_name)
            elif isinstance(value, str):
                _validate_agg_param_value(value, name, agg_name)
        elif spec.aggregation_def:
            for param in spec.aggregation_def.params:
                if param.name == name:
                    value = param.sql
                    break
        if value is None:
            raise ValueError(
                f"Aggregation '{agg_name}' requires parameter '{name}'. "
                f"Set it in the model's aggregation definition or at query time "
                f"(e.g., 'measure:{agg_name}({name}=column)')."
            )
        return self._agg_param_ast(value, model_name=spec.model_name)

    def _build_agg(
        self,
        spec: "AggRenderSpec | None" = None,
    ) -> tuple[Expression, bool]:
        """Build an aggregation expression from an ``AggRenderSpec``."""
        if spec is None:  # pragma: no cover — defensive
            raise ValueError("_build_agg requires a 'spec'.")
        agg_name = spec.aggregation
        if not agg_name:
            if spec.sql:
                return self._resolve_sql(
                    sql=spec.sql,
                    name=spec.name,
                    model_name=spec.model_name,
                    type=spec.column_type,
                ), False
            return exp.Column(
                this=exp.to_identifier(spec.name),
                table=exp.to_identifier(spec.model_name),
            ), False

        if not is_builtin_agg(agg_name) or rendered_formula(agg=agg_name, definition=spec.aggregation_def):
            return self._build_formula_agg(spec, agg_name), True

        entry = resolve_agg_entry(agg_name)
        dispatch = entry.dispatch

        # These builders resolve+filter-wrap their own inner and must run before the plain inner resolution (a
        # join-discovery side effect).
        if dispatch == DISPATCH_STAT:
            return self._build_stat_agg(spec), True
        if agg_name == "percentile":
            return self._build_percentile(spec), True
        if agg_name == "count_distinct_approx":
            return self._dialect.build_approx_count_distinct(
                col_expr=self._resolve_value_ast(spec),
            ), True

        if agg_name == "count" and spec.sql is None:
            inner = exp.Star()
        elif spec.sql:
            inner = self._resolve_sql(
                sql=spec.sql,
                name=spec.name,
                model_name=spec.model_name,
                type=spec.column_type,
            )
        else:
            inner = exp.Column(
                this=exp.to_identifier(spec.name),
                table=exp.to_identifier(spec.model_name),
            )

        if dispatch == DISPATCH_DISTINCT:
            return exp.Count(this=exp.Distinct(expressions=[inner])), True

        if agg_name == "median":
            return self._build_median(inner), True

        return entry.node_class(this=inner), True

    def _build_formula_agg(self, spec: AggRenderSpec, agg_name: str) -> Expression:
        """Build SQL for formula-based aggregations (weighted_avg, custom)."""
        formula = rendered_formula(agg=agg_name, definition=spec.aggregation_def)
        if formula is None:
            raise ValueError(
                f"Aggregation '{agg_name}' has no formula. "
                f"Custom aggregations must define a formula."
            )

        param_defaults = {}
        if spec.aggregation_def:
            param_defaults = {p.name: p.sql for p in spec.aggregation_def.params}
        params = {**param_defaults, **spec.agg_kwargs}

        # Guard only the untrusted kind="str" form against injection; kind="expr" is bind-resolved and trusted.
        for pname, pval in spec.agg_kwargs.items():
            if isinstance(pval, ResolvedAggKwarg) and pval.kind == "str":
                _validate_agg_param_value(pval.value, pname, agg_name)

        template = self._formula_template(agg_name=agg_name, formula=formula)
        for req in BUILTIN_AGGREGATION_REQUIRED_PARAMS.get(agg_name, []):
            if req in template.placeholder_names and req not in params:
                raise ValueError(
                    f"Aggregation '{agg_name}' requires parameter '{req}'. "
                    f"Set it in the model's aggregation definition or at query time "
                    f"(e.g., 'measure:{agg_name}({req}=column)')."
                )

        if VALUE_PLACEHOLDER in params:
            raise SqlTemplateError(reserved_value_param_message(agg_name))
        bindings = {
            name: self._agg_param_ast(val, model_name=spec.model_name)
            for name, val in params.items() if name in template.placeholder_names
        }
        # Bound last: the aggregated column always wins. A source ``Column.filter`` is already baked in.
        bindings[VALUE_PLACEHOLDER] = (
            exp.Star() if spec.sql is None and not spec.name else self._resolve_value_ast(spec)
        )
        try:
            return template.render(bindings)
        except SqlTemplateError as e:
            raise SqlTemplateError(f"Aggregation '{agg_name}': {e}") from e

    def _formula_template(self, *, agg_name: str, formula: str) -> SqlTemplate:
        try:
            return sql_template(text=formula, dialect=self.dialect)
        except SqlTemplateError as e:
            raise SqlTemplateError(f"Aggregation '{agg_name}': {e}") from e

    def _build_median(self, inner: Expression) -> Expression:
        """Build a median aggregation expression. Dispatches to the dialect"""
        return self._dialect.build_median(inner=inner)

    def _build_percentile(self, spec: AggRenderSpec) -> Expression:
        """Build a PERCENTILE_CONT(p) aggregation expression (dialect-dependent)."""
        p = _percentile_literal(self._resolve_agg_param(spec, name="p", agg_name="percentile"))
        return self._dialect.build_percentile(p=p, col_expr=self._resolve_value_ast(spec))

    def _build_stat_agg(self, spec: AggRenderSpec) -> Expression:
        """Build SQL for the statistical aggregations."""
        agg_name = spec.aggregation
        if is_stat_agg2(agg_name):
            # Resolve other= first so a missing-required-param error outranks the dialect-unsupported one.
            other_expr = self._resolve_agg_param(spec, name="other", agg_name=agg_name)
            return self._dialect.build_covar_2arg(
                agg_name=agg_name, col_expr=self._resolve_value_ast(spec), other_expr=other_expr,
            )
        if is_stat_agg1(agg_name):
            return self._dialect.build_stat_agg_1arg(
                agg_name=agg_name, col_expr=self._resolve_value_ast(spec),
            )
        raise ValueError(f"Unknown statistical aggregation {agg_name!r}.")

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

    def _generate_from_planned_impl(  # NOSONAR(S3776) — top-level dispatch over cross-model / transform-chain / plain branches plus the conditional outer-trim wrap. Each branch is a coherent compilation strategy; extracting would scatter the shared planned_query / slots_by_id / aliases_by_slot_id state across helpers without simplifying anything.
        self,
        planned_query,
        *,
        bundle,
        as_cte_body: bool = False,
        producer_kernel=None,
    ) -> exp.Select:
        """Compose a typed ``PlannedQuery`` as one statement AST."""

        source_model = bundle.source_model
        if source_model is None:
            raise ValueError(
                "generate_from_planned requires bundle.source_model to be set",
            )
        source_relation = planned_query.source_relation

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
        # Belt against a model_copy that bypassed the plan-time staging validator.
        self._assert_stages_assigned(planned_query=planned_query)

        if (
            as_cte_body
            and producer_kernel is not None
            and producer_kernel.kind != "plain"
        ):
            return self._render_kernel_producer_body(
                planned_query=planned_query, bundle=bundle,
                kernel=producer_kernel,
            )

        if _combined_attaches:
            return self._render_with_combined_attaches(
                planned_query=planned_query, bundle=bundle,
            )

        slots_by_id = {
            s.id: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }

        # _base projects every BASE ∧ needs_column value in plan order;
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
        ) = self._build_base_select_for_planned(
            planned_query=planned_query,
            bundle=bundle,
            source_model=source_model,
            source_relation=source_relation,
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            regroup_env=regroup_env,
            regroup_join_specs=regroup_join_specs,
        )

        where_clause, having_clause = self._build_where_having_from_planned(
            planned_query=planned_query,
            source_relation=source_relation,
            source_model=source_model,
            bundle=bundle,
            aliases_by_slot_id=aliases_by_slot_id,
            regroup_env=regroup_env,
        )

        if where_clause is not None:
            base_select = base_select.where(where_clause)
        for cond in self._semi_join_exists_conditions(
            planned_query=planned_query, source_model=source_model,
            source_relation=source_relation, bundle=bundle,
        ):
            base_select = base_select.where(cond)

        # dim-only dedup emits GROUP BY before LIMIT so unique dim tuples aren't dropped past row N;
        # distinct_dimension_values=False opts out.
        dim_only_dedup = (
            planned_query.distinct_dimension_values
            and bool(group_by_keys)
            and not has_aggregation
        )
        needs_group_by = has_aggregation or dim_only_dedup
        if needs_group_by and group_by_keys:
            for gb in group_by_keys.values():
                base_select = base_select.group_by(gb)

        if having_clause is not None:
            base_select = base_select.having(having_clause)

        # With no blocker the pipeline collapses to one SELECT; the only blocker here is materialised hidden
        # order/filter slots, forcing an outer trim wrap.
        if not planned_query.transform_layers:
            public_slot_ids = set(planned_query.projection)
            blockers = fusion_blockers(
                has_combined_phase=False,
                has_transform_steps=False,
                trims_hidden_columns=any(
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
                    source_model=source_model,
                    bundle=bundle,
                    aliases_by_slot_id=aliases_by_slot_id,
                )
            if regroup_ctes:
                final_select = self._assemble_with_chain(
                    entries=regroup_ctes, final=final_select,
                    external_names=self._external_cte_names(),
                )
            return final_select

        # Chain bodies stay exp.Select end-to-end: render-to-text-and-reparse would mis-split the dotted
        # <relation>.<alias> names on dot-path dialects.
        return self._render_steps_and_post(
            prelude_nodes=regroup_ctes,
            tail_select=base_select,
            tail_schema=aliases_by_slot_id,
            tail_phase="base",
            planned_query=planned_query,
            bundle=bundle,
            source_model=source_model,
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
        tail_phase: str,
        planned_query,
        bundle,
        source_model,
        source_relation: str,
        slots_by_id: Dict[str, Any],
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
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
            source_model=source_model,
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
        slot_entries: Iterable[Tuple[str, Any]],
        render: Callable[[Any], Expression],
    ) -> Tuple[str, int]:
        """Emit one transform-chain step CTE and advance the chain."""
        step_num += 1
        step_name = cte_allocator.allocate_cte(f"step{step_num}")
        prev_cte = chain_tail
        carry_aliases = self._carry_aliases_in_plan_order(aliases_by_slot_id)
        step_parts = [exp.column(a, quoted=True) for a in carry_aliases]
        for map_key, slot in slot_entries:
            names = list(slot.public_aliases) or [slot.declared_name]
            rendered = render(slot)
            if slot.type is not None:
                rendered = _wrap_cast_for_type(expr=rendered, dt=self._slot_cast_type(slot))
            # One column per declared name (C13); as_ copies its child, so the rendered node is safely reused.
            for alias in names:
                full_alias = f"{source_relation}.{alias}"
                step_parts.append(rendered.as_(full_alias, quoted=True))
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
        planned_query, aliases_by_slot_id: Dict[str, List[str]],
    ) -> List[Any]:
        """DERIVED composite slots needing a column, not yet materialised —
        every level fused into the one trailing step (D8, sql P11). A mask-only
        value has ``needs_column=False`` and renders as a predicate."""
        return [
            cslot
            for cslot in planned_query.combined_expression_slots
            if isinstance(cslot.key, (ArithmeticKey, ScalarCallKey))
            and cslot.id not in aliases_by_slot_id
            and cslot.needs_column
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
        """Materialise projected POST-phase ``ArithmeticKey`` / ``ScalarCallKey``"""
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

    def _resolve_agg_inputs_via_scope(  # NOSONAR(S3776) — one cohesive Law-1 discovery pass: three ordered sub-passes (Column.filter → source → kwargs) over the local aggregates via small closures sharing scope/resolved. Extracting them would scatter the ordered-registration contract that keeps the base FROM byte-identical.
        self, *, base_render_order, slots_by_id, scope: ScopeFrame,
        skip_cross_model_aggs: bool = False,
    ) -> "Dict[Any, Dict[str, ResolvedAggKwarg]]":
        """Resolve every LOCAL aggregate's join-crossing inputs through the host"""

        resolved: "Dict[Any, Dict[str, ResolvedAggKwarg]]" = {}

        def _walk(key, fn) -> None:
            if isinstance(key, AggregateKey):
                # A host-grain aggregate renders inline and must register its source join (Law 1) — but not when a _cm_
                # CTE owns it (would add an unused, cardinality-changing join).
                if not source_anchor_path(key.source) or (
                    _is_host_grain(key) and not skip_cross_model_aggs
                ):
                    fn(key)
            elif isinstance(key, ArithmeticKey):
                for o in key.operands:
                    _walk(o, fn)
            elif isinstance(key, ScalarCallKey):
                for a in key.args:
                    _walk(a, fn)

        def _for_each_local_agg(fn) -> None:
            for sid in base_render_order:
                slot = slots_by_id.get(sid)
                if slot is not None and slot.phase == Phase.AGGREGATE:
                    _walk(slot.key, fn)

        def _resolve_source(key) -> None:
            # Expression sources resolve too: a ColumnSqlKey operand
            # whose derived SQL crosses joins must register them (Law 1).
            if isinstance(
                key.source, (ColumnSqlKey, *_EXPRESSION_SOURCE_KINDS),
            ) or source_anchor_path(key.source):
                scope.resolve(key.source)

        def _resolve_kwargs(key) -> None:
            kw: Dict[str, ResolvedAggKwarg] = {}
            for kname, kval in key.kwargs:
                if isinstance(kval, (ColumnKey, ColumnSqlKey)):
                    kw[kname] = ResolvedAggKwarg(kind="expr", value=scope.resolve(kval))
            if kw:
                resolved[key] = kw

        def _resolve_fragment_kwargs(key) -> None:
            # Template-fragment kwargs are substituted as qualified SQL, so their crossed joins must register like
            # Column.filter; keep the resolved (alias-rewritten) fragment. A host-locus source beyond the root carries
            # its aggregation definition on the source model, so look the params up there.
            # A definition default resolves per its reference frame: the source
            # owner for a source-relative default (regions.pop), the root for a home-frame
            # default naming the widened home (customers.spend) — the reverse hop back to it.
            frag_model, source_owner_path = scope.root_model, None
            src_path = source_anchor_path(key.source)
            if src_path and _is_host_grain(key):
                walked = self._walk_join_path_model(
                    source_model=scope.root_model, path=src_path, bundle=scope.bundle,
                )
                if walked is not None:
                    frag_model, source_owner_path = walked, src_path
            frags = self._register_fragment_kwarg_joins(
                key=key, scope=scope, model=frag_model,
                source_owner_path=source_owner_path,
            )
            if frags:
                bucket = resolved.setdefault(key, {})
                for name, ast in frags.items():
                    bucket.setdefault(name, ResolvedAggKwarg(kind="expr", value=ast))

        def _resolve_first_last_time_arg(key) -> None:
            arg = self._explicit_time_arg_of(key)
            if arg is None:
                return
            # A path-bearing derived time arg is a hop past the target; skip it — anchoring against source_relation
            # would register a bogus join.
            if isinstance(arg, ColumnSqlKey) and arg.path:
                return
            scope.resolve(arg)

        _for_each_local_agg(_resolve_source)
        _for_each_local_agg(_resolve_kwargs)
        _for_each_local_agg(_resolve_fragment_kwargs)
        _for_each_local_agg(_resolve_first_last_time_arg)
        return resolved

    def _throwaway_frame(
        self, *, model, relation: str, bundle, attached_columns=None,
    ) -> ScopeFrame:
        """A target-rooted ``ScopeFrame`` built purely to reproduce an anchored"""
        allocator = self._new_allocator()
        return ScopeFrame(
            scope_id=allocator.next_scope_id(relation),
            root_model=model,
            root_relation=relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=allocator,
            attached_columns=dict(attached_columns or {}),
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
                and isinstance(slot.key, (ScalarCallKey, ArithmeticKey))
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
            operands = [
                (
                    render_value_key(
                        key=host_key,
                        ctx=RenderContext(scope=scope, dialect=self._dialect),
                    ),
                    grain_alias_column(alias=producer_alias, table=cte_name),
                )
                for host_key, producer_alias in pairs
            ]
            out.append((
                cte_name,
                build_grain_joinback_condition(pairs=operands, dialect=self._dialect),
            ))
        return out

    def _resolve_agg_kwargs_for_key(
        self, *, key, scope: ScopeFrame,
    ) -> "Optional[Dict[str, ResolvedAggKwarg]]":
        """Resolve a single LOCAL aggregate's column-ref kwargs through ``scope`` (a row-attached placeholder resolves to its producer join column)."""
        kwargs = getattr(key, "kwargs", None)
        if not kwargs:
            return None
        resolved = {
            kname: ResolvedAggKwarg(kind="expr", value=scope.resolve(kval))
            for kname, kval in kwargs
            if isinstance(kval, (ColumnKey, ColumnSqlKey))
        }
        return resolved or None

    def _build_base_select_for_planned(  # NOSONAR(S3776) — join-path collection and derived-dim expansion are extracted to helpers; the residual is the one cohesive per-slot ROW/AGGREGATE projection + GROUP-BY assembly pass.
        self,
        *,
        planned_query,
        bundle,
        source_model,
        source_relation: str,
        base_render_order: List[str],
        slots_by_id: Dict[str, Any],
        skip_cross_model_aggs: bool = False,
        skip_filter_ids: Optional[Set[str]] = None,
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
    ):
        """Build the base SELECT (sqlglot ``Select``) for ``generate_from_planned``."""

        # Every join-crossing ref registers its path into host_scope.join_paths as it resolves (Law 1); first-seen order
        # keeps the FROM byte-identical to the legacy collectors'.
        needed_join_paths = self._collect_joined_paths_for_base(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            order_slot_ids=[e.slot_id for e in planned_query.order],
        )
        host_allocator = self._gen_allocator or self._new_allocator()
        host_scope = self._scope_frame(
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=host_allocator,
            attached_columns=regroup_env,
        )
        derived_expr_by_sid = self._expand_derived_row_dims(
            base_render_order=base_render_order, slots_by_id=slots_by_id,
            source_relation=source_relation, source_model=source_model,
            bundle=bundle, scope=host_scope,
            order_slot_ids=[e.slot_id for e in planned_query.order],
        )
        # Pre-render computed dimensions through the host scope so a join their expression crosses registers before the
        # FROM is built.
        computed_dim_expr_by_sid = self._render_computed_dims_via_scope(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            scope=host_scope,
        )
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=host_scope,
        )
        # WHERE filters register their joins too; skip those routed to a _cm_ CTE (registering here would add an unused,
        # cardinality-changing LEFT JOIN).
        self._resolve_where_filter_joins_via_scope(
            planned_query=planned_query, scope=host_scope,
            skip_filter_ids=skip_filter_ids,
        )
        resolved_agg_kwargs = self._resolve_agg_inputs_via_scope(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            scope=host_scope,
            skip_cross_model_aggs=skip_cross_model_aggs,
        )
        for p in host_scope.join_paths.as_list():
            if p not in needed_join_paths:
                needed_join_paths.append(p)
        from_clause, base_joins = self._build_from_and_joins(
            source_model=source_model,
            source_relation=source_relation,
            joined_paths=needed_join_paths,
            bundle=bundle,
        )

        select_columns: list[Expression] = []
        group_by_keys: Dict[str, Expression] = {}
        has_aggregation = False
        alias_index: Dict[str, int] = {}
        aliases_by_slot_id: Dict[str, List[str]] = {}

        def _record_alias(sid: str, full_alias: str) -> None:
            aliases_by_slot_id.setdefault(sid, []).append(full_alias)

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
                if isinstance(key, ColumnKey):
                    attached = regroup_env.get(key) if regroup_env else None
                    if attached is not None:
                        col_expr = attached.copy()
                    else:
                        col_expr = self._joined_or_local_dim_expr(
                            path=key.path,
                            leaf=key.leaf,
                            source_model=source_model,
                            source_relation=source_relation,
                            bundle=bundle,
                        )
                    select_columns.append(col_expr.copy().as_(full_alias))
                    group_by_keys.setdefault(sid, col_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, TimeTruncKey):
                    col_expr = self._raw_time_col_expr_for_planned(
                        time_column=key.column,
                        source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
                    )
                    trunc_expr = self._build_date_trunc(
                        col_expr=col_expr,
                        granularity=TimeGranularity(key.granularity),
                    )
                    select_columns.append(trunc_expr.copy().as_(full_alias))
                    group_by_keys.setdefault(sid, trunc_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, ColumnSqlKey):
                    col_expr = derived_expr_by_sid.get(sid)
                    if col_expr is None:
                        col_expr = self._dim_column_expr_from_planned(
                            source_model=source_model,
                            source_relation=source_relation,
                            leaf=key.column_name,
                        )
                    select_columns.append(col_expr.copy().as_(full_alias))
                    group_by_keys.setdefault(sid, col_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, (ScalarCallKey, ArithmeticKey)) and slot.is_dimension:
                    dim_expr = computed_dim_expr_by_sid[sid]
                    select_columns.append(dim_expr.copy().as_(full_alias))
                    group_by_keys.setdefault(sid, dim_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, (ScalarCallKey, ArithmeticKey)):
                    # A ROW-phase composite here is a measure that never aggregates; raise the actionable 'Bare measure
                    # name' error rather than leaking NotImplementedError.
                    bare = _first_bare_column_name(key) or full_alias
                    raise ValueError(
                        f"'{bare}' needs an aggregation inside an expression. "
                        f"Wrap it in an aggregation (e.g., 'sum({bare})', 'avg({bare})'). "
                        f"For COUNT(*), use 'count(*)'."
                    )
                else:
                    raise NotImplementedError(
                        f"row-phase key type "
                        f"{type(key).__name__} not supported in the "
                        f"local-only / time-dim slice."
                    )

            elif slot.phase == Phase.AGGREGATE:
                key = slot.key
                if not isinstance(key, AggregateKey):
                    composite = render_value_key(
                        key=key,
                        ctx=RenderContext(
                            dialect=self._dialect,
                            composites=CompositeFacilities(
                                agg_builder=self._composite_agg_builder(
                                    slot=slot,
                                    source_model=source_model,
                                    source_relation=source_relation,
                                    bundle=bundle,
                                    resolved_agg_kwargs=resolved_agg_kwargs,
                                    scope=host_scope,
                                ),
                            ),
                        ),
                    )
                    if contains_aggregate(key):
                        composite = _wrap_cast_for_type(expr=composite, dt=self._slot_cast_type(slot))
                        has_aggregation = True
                    select_columns.append(composite.copy().as_(full_alias))
                    _record_alias(sid, full_alias)
                    continue
                agg_path = source_anchor_path(key.source)
                if agg_path:
                    if skip_cross_model_aggs:
                        continue
                    if not _is_host_grain(key):
                        raise NotImplementedError(
                            f"cross-model aggregate (source.path={agg_path!r}) "
                            f"reached the local base SELECT path; the regroup "
                            f"desugar should have isolated it into a producer "
                            f"CTE."
                        )
                synth = self._build_agg_render_spec_from_planned(
                    slot=slot,
                    key=key,
                    source_model=source_model,
                    source_relation=source_relation,
                    full_alias=full_alias,
                    bundle=bundle,
                    resolved_agg_kwargs=resolved_agg_kwargs.get(key),
                    scope=host_scope,
                )
                agg_expr, is_agg = self._build_agg(synth)
                if is_agg:
                    agg_expr = _wrap_cast_for_type(expr=agg_expr, dt=self._slot_cast_type(slot))
                    has_aggregation = True
                select_columns.append(agg_expr.copy().as_(full_alias))
                _record_alias(sid, full_alias)
            else:
                raise MaterialisationStageError(
                    f"value {sid!r} (phase {slot.phase!r}) reached the base "
                    f"SELECT; a BASE-staged value is ROW or AGGREGATE by "
                    f"construction.",
                )

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
        )

    @staticmethod
    def _explicit_time_arg_of(key):
        """The explicit positional ranking-time arg of a ``first`` / ``last``"""

        if key.agg not in ("first", "last"):
            return None
        for a in key.args:
            return a if isinstance(a, (ColumnKey, ColumnSqlKey)) else None
        return None

    def _composite_agg_builder(
        self, *, slot, source_model, source_relation: str, bundle,
        resolved_agg_kwargs, scope: ScopeFrame,
    ):
        """The AGGREGATE-phase composite seam: render one"""

        def build(agg_key) -> Expression:
            if source_anchor_path(agg_key.source):
                # Internal invariant: cross-model operands desugar to regroup
                # placeholders before phase classification, so none reaches
                # this seam.
                raise RuntimeError(
                    f"cross-model aggregate operand {agg_key!r} reached the "
                    f"AGGREGATE-phase composite seam; the regroup desugar "
                    f"should have replaced it with a placeholder."
                )
            synth = self._build_agg_render_spec_from_planned(
                slot=slot, key=agg_key, source_model=source_model,
                source_relation=source_relation, full_alias="__op__",
                bundle=bundle,
                resolved_agg_kwargs=(resolved_agg_kwargs or {}).get(agg_key),
                scope=scope,
            )
            agg_expr, _is_agg = self._build_agg(synth)
            return agg_expr

        return build

    def _render_window_measure_cte_from_planned(  # NOSONAR(S3776) — one cohesive host-rooted range-join CTE build: ``_src`` projection (dims / other-time-dims / raw-window-time / value) with Law-1 join discovery, WHERE inheritance minus date_range, and the ``_base LEFT JOIN _src`` interval range join. Splitting scatters the shared scope / grain-alias / join-eq state.
        self,
        *,
        plan,
        agg_slot,
        source_model,
        source_relation: str,
        bundle,
        planned_query,
        slots_by_id: Dict[str, Any],
        aliases_by_slot_id: Dict[str, List[str]],
        full_agg_alias: str,
        base_relation: Optional[Expression] = None,
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
    ) -> Tuple[exp.Select, List[str]]:
        """Render one duration-windowed-measure CTE."""

        key = agg_slot.key
        assert isinstance(key, AggregateKey)

        allocator = self._gen_allocator or self._new_allocator()
        src_scope = self._scope_frame(
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=allocator,
            attached_columns=regroup_env,
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

        src_cols: List[Expression] = []
        grain_pairs: List[Tuple[Expression, Expression]] = []
        grain_aliases: List[str] = []

        for idx, sid in enumerate(plan.dimension_slot_ids):
            dslot = slots_by_id.get(sid)
            base_alias = _alias_of(sid)
            expr = render_value_key(
                key=dslot.key,
                ctx=RenderContext(scope=src_scope, dialect=self._dialect),
            )
            src_cols.append(expr.as_(f"_w_dim_{idx}"))
            grain_pairs.append(
                (_src_col(f"_w_dim_{idx}"), _base_col(base_alias)),
            )
            grain_aliases.append(base_alias)

        # Non-window time dimensions are equality-joined so the trailing window doesn't fan out across their values.
        for idx, sid in enumerate(plan.other_time_dimension_slot_ids):
            tslot = slots_by_id.get(sid)
            base_alias = _alias_of(sid)
            src_scope.resolve(tslot.key.column)
            raw = self._raw_time_col_expr_for_planned(
                time_column=tslot.key.column, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            )
            trunc = self._build_date_trunc(
                col_expr=raw, granularity=TimeGranularity(tslot.key.granularity),
            )
            src_cols.append(trunc.as_(f"_w_td_{idx}"))
            grain_pairs.append(
                (_src_col(f"_w_td_{idx}"), _base_col(base_alias)),
            )
            grain_aliases.append(base_alias)

        wtd_slot = slots_by_id.get(plan.window_time_dimension_slot_id)
        wtd_alias = _alias_of(plan.window_time_dimension_slot_id)
        src_scope.resolve(wtd_slot.key.column)
        raw_time = self._raw_time_col_expr_for_planned(
            time_column=wtd_slot.key.column, source_model=source_model,
            source_relation=source_relation, bundle=bundle,
        )
        src_cols.append(raw_time.copy().as_("_w_time"))
        grain_aliases.append(wtd_alias)

        # A Column.filter on the source is baked into its ColumnSqlKey (CASE WHEN),
        # so a first/last picked value is masked while ranking spans all rows:
        # the latest row's value may be NULL if it fails the filter.
        # ``count(*)`` projects a literal so the outer COUNT(_w_value) counts interval
        # rows (0, not 1, on an empty interval) — the star never enters resolve.
        if isinstance(key.source, StarKey):
            # ``*`` is only legal with count (as in the plain path); any other
            # aggregation over the star would silently become ``<agg>(1)``.
            if key.agg != "count":
                raise ValueError(
                    f"Aggregation {key.agg!r} not allowed with measure "
                    f"'*' — use 'count(*)' for COUNT(*)."
                )
            # ``count(*)`` takes no inputs but its own ``window=`` (plain-path guard);
            # a stray arg/kwarg would otherwise be projected and silently ignored.
            extra_kwargs = [(k, v) for k, v in key.kwargs if k != "window"]
            if key.args or extra_kwargs:
                raise ValueError(
                    f"'count(*)' takes no args or kwargs other than window; got "
                    f"args={key.args!r}, kwargs={extra_kwargs!r}."
                )
            src_cols.append(exp.Literal.number("1").as_("_w_value"))
        else:
            src_cols.append(src_scope.resolve(key.source).as_("_w_value"))

        # Reference-bearing parameters read per interval row (D4): each _w_p<i> is
        # rendered through the _src scope so its join paths register for discovery.
        picked_kwarg_exprs: Dict[str, ResolvedAggKwarg] = {}
        for _i, _pp in enumerate(plan.picked_params):
            _p_alias = f"_w_p{_i}"
            _picked = self._render_picked_param_value(
                pp=_pp, ctx=RenderContext(scope=src_scope, dialect=self._dialect),
            )
            src_cols.append(_picked.as_(_p_alias))
            picked_kwarg_exprs[_pp.name] = ResolvedAggKwarg(
                kind="expr", value=_src_col(_p_alias),
            )

        # A windowed first/last ranks the interval rows by this key, projected as
        # _w_rank (uncast, like the plain ranked path).
        if plan.ranking_time_key is not None:
            src_cols.append(self._ranked_scope_expr(
                key=plan.ranking_time_key, root_model=source_model,
                root_relation=source_relation, bundle=bundle, scope=src_scope,
                cast_derived=False,
            ).as_("_w_rank"))

        # _src inherits row filters minus their frame bounds; one effective list feeds both join discovery and rendering
        # so they can't disagree.
        lowered_src = _lower_positions(planned_query).filters
        all_filter_ids = {fp.id for fp in lowered_src}
        skip_for_src = all_filter_ids - set(plan.where_filter_ids)
        src_filters = _effective_src_filters(lowered_filters=lowered_src, plan=plan)
        self._resolve_where_filter_joins_via_scope(
            planned_query=planned_query, scope=src_scope,
            skip_filter_ids=skip_for_src, filters_override=src_filters,
        )
        src_where, _src_having = self._build_where_having_from_planned(
            planned_query=planned_query, source_relation=source_relation,
            source_model=source_model, bundle=bundle,
            skip_filter_ids=skip_for_src, filters_override=src_filters,
        )

        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=src_scope,
        )

        from_expr, src_joins = self._build_from_and_joins(
            source_model=source_model, source_relation=source_relation,
            joined_paths=src_scope.join_paths.as_list(), bundle=bundle,
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
            planned_query=planned_query, source_model=source_model,
            source_relation=source_relation, bundle=bundle,
        ):
            src_select = src_select.where(cond)
        src_subq = exp.Subquery(
            this=src_select, alias=exp.TableAlias(this=exp.to_identifier("_src")),
        )

        # Trailing-window range: _src._w_time in [bucket_end - window, bucket_end), bucket_end being the host bucket's
        # exclusive upper edge.
        frame_time = _base_col(wtd_alias)
        bucket_end = self._add_intervals_expr(
            frame_time,
            self._granularity_interval_expr(
                TimeGranularity(plan.window_granularity), sign=1,
            ),
            sign=1,
        )
        lower_bound = self._add_intervals_expr(
            bucket_end,
            self._dialect.duration_interval_exprs(
                parts=[tuple(p) for p in plan.window_parts], sign=-1,
            ),
            sign=-1,
        )
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
            exp.GTE(this=src_w_time, expression=lower_bound),
            exp.LT(this=src_w_time.copy(), expression=bucket_end.copy()),
        )

        base_from = (
            base_relation if base_relation is not None
            else exp.Table(this=exp.to_identifier("_base"))
        )

        # first/last: rank the range-joined interval rows and pick rank 1 per bucket
        # through the one ranked shape — the LEFT JOIN's NULL row of an empty
        # interval ranks 1 and yields NULL, matching sum (D3).
        if plan.ranking_time_key is not None:
            rank_inner = exp.Select()
            for ga in grain_aliases:
                rank_inner = rank_inner.select(
                    _base_col(ga).as_(exp.to_identifier(ga, quoted=True)))
            rank_inner = rank_inner.select(_src_col("_w_value").as_("_w_value"))
            rank_inner = rank_inner.select(build_rank_column(
                partition_by=[_base_col(ga) for ga in grain_aliases],
                ranking_time=ranked_ordered(
                    ranking_time=_src_col("_w_rank"), agg=plan.agg,
                    native_nulls_first=self._dialect.native_nulls_first(
                        descending=plan.agg == "last",
                    ),
                ),
            ))
            rank_inner = rank_inner.from_(base_from).join(
                src_subq, on=on_range, join_type="LEFT")
            pick = _wrap_cast_for_type(
                expr=build_ranked_pick(value_ref=exp.Column(
                    this=exp.to_identifier("_w_value"),
                    table=exp.to_identifier(RANKED_SOURCE_ALIAS),
                )),
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
            model_name="_src",
            type=agg_slot.type,
            # The custom-aggregation definition lives on the source's owning model,
            # which a parameter may widen the home above (D4) — resolve it there,
            # not on the producer root, mirroring _trailing_window_kernel.
            aggregation_def=self._resolve_aggregation_def(
                key=key,
                source_model=(
                    self._walk_join_path_model(
                        source_model=source_model,
                        path=source_anchor_path(key.source), bundle=bundle,
                    ) or source_model
                ),
                src_leaf="_w_value",
            ),
            agg_kwargs={
                **{
                    k: ResolvedAggKwarg(kind="str", value=agg_kwarg_canonical_str(v))
                    for k, v in key.kwargs
                    if k not in ("window", "partition_by")
                    and k not in picked_kwarg_exprs
                },
                **picked_kwarg_exprs,
            },
        )
        agg_expr, _ = self._build_agg(level2_spec)
        agg_expr = _wrap_cast_for_type(
            expr=agg_expr, dt=self._slot_cast_type(agg_slot),
        )

        outer = exp.Select()
        for ga in grain_aliases:
            outer = outer.select(_base_col(ga))
        outer = outer.select(agg_expr.as_(exp.to_identifier(full_agg_alias, quoted=True)))
        outer = outer.from_(base_from)
        outer = outer.join(src_subq, on=on_range, join_type="LEFT")
        for ga in grain_aliases:
            outer = outer.group_by(_base_col(ga))

        # Return AST: rendering here and re-parsing later would re-introduce the dotted-alias corruption on BigQuery.
        return outer, grain_aliases

    def _ranked_scope_expr(
        self,
        *,
        key,
        root_model,
        root_relation: str,
        bundle,
        scope: ScopeFrame,
        cast_derived: bool = True,
    ) -> Expression:
        """One value expression anchored in a ranked CTE's own scope."""

        def _register(expr: Expression, path: Tuple[str, ...]) -> None:
            if path:
                scope.join_paths.add(path)
            for p in self._joined_paths_in_sql(
                sql_expr=expr, source_relation=root_relation,
                source_model=root_model, bundle=bundle,
            ):
                scope.join_paths.add(p)

        if isinstance(key, TimeTruncKey):
            raw = self._raw_time_col_expr_for_planned(
                time_column=key.column, source_model=root_model,
                source_relation=root_relation, bundle=bundle,
            )
            _register(raw, column_path(key.column))
            return self._build_date_trunc(
                col_expr=raw, granularity=TimeGranularity(key.granularity),
            )
        if isinstance(key, ColumnKey):
            expr = self._joined_or_local_dim_expr(
                path=key.path, leaf=key.leaf, source_model=root_model,
                source_relation=root_relation, bundle=bundle,
            )
            _register(expr, key.path)
            return expr
        if isinstance(key, ColumnSqlKey):
            expr = self._derived_column_expr(
                key=key, source_model=root_model,
                source_relation=root_relation, bundle=bundle,
            )
            if expr is None:
                raise ValueError(
                    f"Derived column {key.column_name!r} on model "
                    f"{key.path[-1] if key.path else root_model.name!r} is not "
                    f"in the resolved source bundle.",
                )
            _register(expr, key.path)
            return expr if cast_derived else _strip_declared_cast(expr)
        raise NotImplementedError(
            f"Ranked CTE cannot anchor a {type(key).__name__} — the grain and "
            f"the ranking column are columns, truncated time columns, or "
            f"derived columns.",
        )

    def _ranked_value_expr(
        self, *, key, root_model, root_relation: str, bundle, scope: ScopeFrame,
    ) -> Expression:
        """The value a ranked aggregate picks, anchored in its own scope."""

        source = key.source
        if isinstance(source, StarKey):
            raise ValueError(
                f"Aggregation {key.agg!r} not allowed with measure "
                f"'*' — use 'count(*)' for COUNT(*)."
            )
        if not isinstance(source, (ColumnKey, ColumnSqlKey)):
            raise NotImplementedError(
                f"AggregateKey source {type(source).__name__} not supported.",
            )
        leaf = (
            source.leaf if isinstance(source, ColumnKey) else source.column_name
        )
        col = next((c for c in root_model.columns if c.name == leaf), None)
        if col is None:
            raise ValueError(
                f"Aggregate source column {leaf!r} not found on model "
                f"{root_model.name!r}",
            )
        if isinstance(source, ColumnSqlKey):
            # The value (and any Column.filter CASE) with its type CAST baked in —
            # parse without re-casting the whole expression.
            expr = self._parse(self._expand_derived_column_sql(
                source_model=root_model, source_relation=root_relation,
                column_name=col.name, bundle=bundle,
            ))
        else:
            expr = self._resolve_sql(
                sql=col.sql if col.sql else col.name, name=col.name,
                model_name=root_relation, type=col.type,
            )
        for p in self._joined_paths_in_sql(
            sql_expr=expr, source_relation=root_relation,
            source_model=root_model, bundle=bundle,
        ):
            scope.join_paths.add(p)
        return expr

    def _render_ranked_cte_from_planned(  # NOSONAR(S3776) — single linear ranked-CTE assembly (src → ROW_NUMBER → collapse); the branches are sequential dialect/shape guards, not nested logic
        self,
        *,
        plan: "_RankedEmission",
        agg_slot,
        bundle,
        planned_query,
        slots_by_id: Dict[str, Any],
        host_source_model,
        host_source_relation: str,
        full_agg_alias: str,
        regroup_env: Optional[Dict[Any, Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
    ) -> Tuple[exp.Select, List[str]]:
        """Render one ranked (``first`` / ``last``) CTE."""

        key = agg_slot.key
        if not isinstance(key, AggregateKey):
            raise RuntimeError(
                f"Ranked emission {plan.aggregate_slot_id!r} references a "
                f"non-aggregate slot.",
            )

        root_model = host_source_model
        root_relation = host_source_relation

        allocator = self._gen_allocator or self._new_allocator()
        self._reserve_model_column_names(allocator, root_model)

        def _frame(*, attached=None) -> ScopeFrame:
            return self._scope_frame(
                model=root_model, relation=root_relation,
                bundle=bundle, allocator=allocator, attached_columns=attached,
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
                member.ranked_key, (ScalarCallKey, ArithmeticKey, TransformKey),
            ) or (
                isinstance(member.ranked_key, ColumnKey)
                and member.ranked_key.leaf.startswith(REGROUP_LEAF_PREFIX)
            ):
                expr = render_value_key(
                    key=member.ranked_key,
                    ctx=RenderContext(scope=ranked_scope, dialect=self._dialect),
                )
            else:
                expr = self._ranked_scope_expr(
                    key=member.ranked_key, root_model=root_model,
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

        value_ref = ranked_scope.materialize_for(
            self._ranked_value_expr(
                key=local_key, root_model=root_model,
                root_relation=root_relation, bundle=bundle, scope=ranked_scope,
            ),
            consumer=cte_scope,
        )
        ranking_time = self._ranked_scope_expr(
            key=plan.ranking_time_key, root_model=root_model,
            root_relation=root_relation, bundle=bundle, scope=ranked_scope,
            cast_derived=False,
        )

        where_parts = self._ranked_cte_where(
            plan=plan, planned_query=planned_query,
            bundle=bundle, root_model=root_model, root_relation=root_relation,
            scope=ranked_scope,
        )

        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=ranked_scope,
        )

        from_expr, joins = self._build_from_and_joins(
            source_model=root_model, source_relation=root_relation,
            joined_paths=ranked_scope.join_paths.as_list(), bundle=bundle,
        )
        inner = exp.Select()
        # A named projection list, never <relation>.*: the projection boundary keeps the rank column's name private.
        ranked_scope.apply_materializations(inner)
        inner = inner.select(build_rank_column(
            partition_by=partition_by,
            ranking_time=ranked_ordered(
                ranking_time=ranking_time,
                agg=plan.agg,
                native_nulls_first=self._dialect.native_nulls_first(
                    descending=plan.agg == "last",
                ),
            ),
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

        # A first/last value is the raw picked column, so its temporal type needs no CAST (SQLite would give numeric
        # affinity, truncating a date to its year).
        pick = _wrap_cast_for_type(
            expr=build_ranked_pick(value_ref=value_ref),
            dt=_ranked_value_cast_type(self._slot_cast_type(agg_slot)),
        )
        return build_ranked_cte_select(
            inner=inner, grain=grain, pick=pick, agg_alias=full_agg_alias,
        )

    def _ranked_cte_where(
        self,
        *,
        plan,
        planned_query,
        bundle,
        root_model,
        root_relation: str,
        scope: ScopeFrame,
    ) -> List[Expression]:
        """Every predicate a ranked CTE applies to the rows it ranks.

        A ``Column.filter`` on the ranked column is NOT a WHERE here — it masks the
        picked value (baked into the source's ColumnSqlKey CASE), so ranking spans
        every row and the latest row's value may be NULL."""
        parts: List[Expression] = []
        skip_ids = {
            fp.id for fp in _lower_positions(planned_query).filters
        } - set(plan.where_filter_ids)
        self._resolve_where_filter_joins_via_scope(
            planned_query=planned_query, scope=scope,
            skip_filter_ids=skip_ids,
        )
        routed, _having = self._build_where_having_from_planned(
            planned_query=planned_query,
            source_relation=root_relation,
            source_model=root_model,
            bundle=bundle,
            skip_filter_ids=skip_ids,
        )
        if routed is not None:
            parts.append(routed)
        parts.extend(self._semi_join_exists_conditions(
            planned_query=planned_query, source_model=root_model,
            source_relation=root_relation, bundle=bundle,
        ))
        return parts

    def _render_kernel_producer_body(self, *, planned_query, bundle, kernel) -> exp.Select:
        """The aggregate phase of a ranked / trailing-window kernel producer"""
        source_model = bundle.source_model
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
                source_model=source_model, source_relation=source_relation,
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
                host_source_model=source_model,
                host_source_relation=source_relation,
                full_agg_alias=self._full_alias_for_slot(
                    slot=agg_slot, source_relation=source_relation, alias_index={},
                ),
                regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
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
            grain_base = self._build_windowed_grain_base(
                planned_query=planned_query, plan=plan, slots_by_id=slots_by_id,
                aliases_by_slot_id=aliases_by_slot_id, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
            )
            base_subq = exp.Subquery(
                this=grain_base,
                alias=exp.TableAlias(this=exp.to_identifier("_base")),
            )
            body, _ = self._render_window_measure_cte_from_planned(
                plan=plan, agg_slot=agg_slot, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                planned_query=planned_query, slots_by_id=slots_by_id,
                aliases_by_slot_id=aliases_by_slot_id,
                full_agg_alias=self._full_alias_for_slot(
                    slot=agg_slot, source_relation=source_relation, alias_index={},
                ),
                base_relation=base_subq,
                regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
            )

        if regroup_ctes:
            body = self._assemble_with_chain(
                entries=regroup_ctes, final=body,
                external_names=self._external_cte_names(),
            )
        return body

    def _render_picked_param_value(self, *, pp, ctx) -> Expression:  # pyright: ignore[reportPrivateImportUsage]
        """The level-1 SQL for a picked parameter: a canonical expression default
        entered at the producer root (the kernel already rerooted it
        into producer coordinates), else the parameter's value key rendered through
        the scope (a column / placeholder / composite; a derived ``Column.sql``
        expands, a carrier placeholder resolves to its carrier column)."""
        if pp.sql is not None:
            return ctx.scope.enter_expression(
                pp.sql, owner_path=(),
                location=f"parameter default {pp.name!r}",
            )
        return render_value_key(key=pp.key, ctx=ctx)

    def _render_association_producer_body(  # NOSONAR(S3776) — one cohesive two-level association body: level-1 dedup SELECT (grain × entity key, picked value) wrapped as ``_base``, level-2 aggregate over the picked rows. The two arms share the grain-alias / scope state.
        self, *, planned_query, bundle, kernel, source_model, source_relation,
        slots_by_id, regroup_env=None, regroup_join_specs=None,
    ) -> exp.Select:
        """The distinct-entity association producer: level 1 groups by
        (grain × the root's entity key) picking each input once per entity; level
        2 aggregates over the picked rows per grain."""
        agg_slot = planned_query.aggregate_slots[0]
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
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=allocator, attached_columns=regroup_env,
        )
        ctx = RenderContext(scope=scope, dialect=self._dialect)

        inner_cols: List[Expression] = []
        group: List[Expression] = []
        for slot, alias in zip(grain_slots, grain_aliases):
            expr = render_value_key(key=slot.key, ctx=ctx)
            inner_cols.append(expr.copy().as_(exp.to_identifier(alias, quoted=True)))
            group.append(expr.copy())
        entity_exprs: List[Expression] = []
        for idx, ekey in enumerate(kernel.entity_keys):
            eexpr = render_value_key(key=ekey, ctx=ctx)
            ek_alias = f"_ek{idx}"
            inner_cols.append(eexpr.copy().as_(exp.to_identifier(ek_alias)))
            group.append(eexpr.copy())
            entity_exprs.append(eexpr.copy())
        # The reverse hop's host-side join columns — rendered here so their join
        # path registers in the scope — are guarded NOT NULL below.
        present_exprs = [
            render_value_key(key=pkey, ctx=ctx)
            for pkey in getattr(kernel, "present_keys", None) or ()
        ]

        # Level 1 picks each input once per entity (MAX is arbitrary-but-correct:
        # the input is root-determined, constant per entity); ``count(*)`` keeps no
        # value column — level 2 counts the entity rows.
        is_star = isinstance(agg_slot.key.source, StarKey)
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
            agg_def = self._resolve_aggregation_def(
                key=agg_slot.key, source_model=source_model, src_leaf=picked_alias,
            )
            spec = AggRenderSpec(
                name=picked_alias, sql=None, aggregation=agg_slot.key.agg,
                alias=agg_alias, model_name="_base", type=agg_slot.type,
                aggregation_def=agg_def,
                agg_kwargs={
                    k: ResolvedAggKwarg(kind="str", value=agg_kwarg_canonical_str(v))
                    for k, v in agg_slot.key.kwargs if k not in picked_names
                },
            )
            inner_cols.append(exp.Alias(
                this=exp.Max(this=value_expr.copy()),
                alias=exp.to_identifier(picked_alias),
            ))
        elif not is_star:
            # Discovery runs over the FULL key so a parameter's join path (e.g.
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
                slot=agg_slot, key=source_key, source_model=source_model,
                source_relation=source_relation, full_alias=picked_alias,
                bundle=bundle,
                resolved_agg_kwargs={
                    k: v for k, v in (resolved.get(agg_slot.key) or {}).items()
                    if k not in picked_names
                },
                scope=scope,
            )
            inner_cols.append(
                exp.Max(this=self._resolve_value_ast(spec)).as_(
                    exp.to_identifier(picked_alias),
                ),
            )

        # Pick each legal parameter once per cell as _p<i>. A Column.filter masks
        # only its own column (baked into that column's ColumnSqlKey CASE), never
        # the source's filter.
        picked_kwarg_exprs: Dict[str, ResolvedAggKwarg] = {}
        for _i, _pp in enumerate(picked_params):
            _p_alias = f"_p{_i}"
            _picked = self._render_picked_param_value(pp=_pp, ctx=ctx)
            inner_cols.append(exp.Alias(
                this=exp.Max(this=_picked),
                alias=exp.to_identifier(_p_alias),
            ))
            picked_kwarg_exprs[_pp.name] = ResolvedAggKwarg(
                kind="expr",
                value=exp.Column(
                    this=exp.to_identifier(_p_alias),
                    table=exp.to_identifier("_base"),
                ),
            )

        self._resolve_where_filter_joins_via_scope(
            planned_query=planned_query, scope=scope, skip_filter_ids=set(),
        )
        where, _having = self._build_where_having_from_planned(
            planned_query=planned_query, source_relation=source_relation,
            source_model=source_model, bundle=bundle, skip_filter_ids=set(),
        )
        # A re-aggregation attaches its carrier (the inner producer)
        # as a row producer supplying the per-cell value; join it into level 1.
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs or [], scope=scope,
        )
        from_expr, joins = self._build_from_and_joins(
            source_model=source_model, source_relation=source_relation,
            joined_paths=scope.join_paths.as_list(), bundle=bundle,
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
        # A dimension reached only back through the population root associates an
        # entity only when a population row carries it: guard every host-side
        # join column of the reverse hop NOT NULL (all-components rule), so an
        # entity absent from the population is in no such cell.
        for pexpr in present_exprs:
            inner = inner.where(
                exp.Not(this=exp.Is(this=pexpr, expression=exp.Null())))
        for cond in self._semi_join_exists_conditions(
            planned_query=planned_query, source_model=source_model,
            source_relation=source_relation, bundle=bundle,
        ):
            inner = inner.where(cond)
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
            level2_spec = AggRenderSpec(
                name="", sql=None, aggregation=agg_slot.key.agg,
                alias=agg_alias, model_name="_base", type=agg_slot.type,
                aggregation_def=self._resolve_aggregation_def(
                    key=agg_slot.key, source_model=source_model, src_leaf="*",
                ),
                agg_kwargs={
                    **{k: ResolvedAggKwarg(kind="str", value=agg_kwarg_canonical_str(v))
                       for k, v in agg_slot.key.kwargs if k not in picked_names},
                    **picked_kwarg_exprs,
                },
            )
        else:
            assert spec is not None  # set in both non-star arms above
            level2_spec = AggRenderSpec(
                # ``count`` counts cells with a NON-NULL picked value (COUNT(_v)),
                # never the cells (COUNT(*)): a Column.filter masks non-matching
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
                alias=agg_alias, model_name="_base", type=agg_slot.type,
                column_type=spec.column_type,
                # A picked parameter reads from _base._p<i>, overriding its
                # explicit-kwarg / definition-default resolution.
                agg_kwargs={**spec.agg_kwargs, **picked_kwarg_exprs},
                aggregation_def=spec.aggregation_def,
            )
        agg_expr, _ = self._build_agg(level2_spec)
        agg_expr = _wrap_cast_for_type(expr=agg_expr, dt=self._slot_cast_type(agg_slot))
        outer_cols: List[Expr] = [
            _base_col(alias).as_(exp.to_identifier(alias, quoted=True))
            for alias in grain_aliases
        ]
        outer_cols.append(agg_expr.as_(exp.to_identifier(agg_alias, quoted=True)))
        outer = exp.Select().select(*outer_cols).from_(base_subq)
        for alias in grain_aliases:
            outer = outer.group_by(_base_col(alias))
        return outer

    def _build_windowed_grain_base(
        self, *, planned_query, plan, slots_by_id, aliases_by_slot_id,
        source_model, source_relation, bundle,
        regroup_env=None, regroup_join_specs=None,
    ) -> exp.Select:
        """The grain-rows relation for a collapsed windowed producer"""
        allocator = self._gen_allocator or self._new_allocator()
        scope = self._scope_frame(
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=allocator, attached_columns=regroup_env,
        )
        cols: List[Expression] = []
        group: List[Expression] = []

        def _emit(slot, expr: Expression) -> None:
            alias = aliases_by_slot_id[slot.id][0]
            cols.append(expr.copy().as_(exp.to_identifier(alias, quoted=True)))
            group.append(expr.copy())

        for sid in plan.dimension_slot_ids:
            dslot = slots_by_id[sid]
            _emit(dslot, render_value_key(
                key=dslot.key,
                ctx=RenderContext(scope=scope, dialect=self._dialect),
            ))
        for sid in (*plan.other_time_dimension_slot_ids,
                    plan.window_time_dimension_slot_id):
            tslot = slots_by_id[sid]
            scope.resolve(tslot.key.column)
            raw = self._raw_time_col_expr_for_planned(
                time_column=tslot.key.column, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            )
            _emit(tslot, self._build_date_trunc(
                col_expr=raw, granularity=TimeGranularity(tslot.key.granularity),
            ))
        # ROW filters gate the visible grain; the trailing window in the _src join reaches rows before them via
        # plan.where_filter_ids (a strict subset).
        self._resolve_where_filter_joins_via_scope(
            planned_query=planned_query, scope=scope, skip_filter_ids=set(),
        )
        where, _having = self._build_where_having_from_planned(
            planned_query=planned_query, source_relation=source_relation,
            source_model=source_model, bundle=bundle, skip_filter_ids=set(),
        )
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=scope,
        )
        from_expr, joins = self._build_from_and_joins(
            source_model=source_model, source_relation=source_relation,
            joined_paths=scope.join_paths.as_list(), bundle=bundle,
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
            planned_query=planned_query, source_model=source_model,
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
        bundle,
    ) -> exp.Select:
        """Render a ``PlannedQuery`` that carries one or more COMBINED"""

        source_model = bundle.source_model
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
                sub.original_key: regroup_placeholder_to_cm[sub.placeholder][1]
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
        # _base projects every BASE ∧ needs_column value in plan order; a dual-role
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
                    model=source_model, relation=source_relation,
                    bundle=bundle, allocator=placeholder_allocator,
                )
                self._resolve_where_filter_joins_via_scope(
                    planned_query=planned_query,
                    scope=placeholder_scope,
                    skip_filter_ids=placeholder_skip_ids,
                )
                placeholder_from, placeholder_joins = self._build_from_and_joins(
                    source_model=source_model,
                    source_relation=source_relation,
                    joined_paths=placeholder_scope.join_paths.as_list(),
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
                    source_model=source_model,
                    bundle=bundle,
                    skip_filter_ids=placeholder_skip_ids,
                )
                if base_where is not None:
                    base_select = base_select.where(base_where)
                for cond in self._semi_join_exists_conditions(
                    planned_query=planned_query, source_model=source_model,
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
            # only there blanks a row's measure instead of excluding the row; ROW-phase filters apply in both places.
            routed_ids: Set[str] = set(outer_where_filter_ids)
            (
                base_select,
                aliases_by_slot_id,
                base_has_agg,
                base_group_by,
            ) = self._build_base_select_for_planned(
                planned_query=planned_query,
                bundle=bundle,
                source_model=source_model,
                source_relation=source_relation,
                base_render_order=base_render_order,
                slots_by_id=slots_by_id,
                skip_cross_model_aggs=True,
                skip_filter_ids=routed_ids,
                regroup_env=row_regroup_env,
                regroup_join_specs=row_regroup_join_specs,
            )

            base_where, base_having = self._build_where_having_from_planned(
                planned_query=planned_query,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                skip_filter_ids=routed_ids,
                aliases_by_slot_id=aliases_by_slot_id,
                regroup_env=row_regroup_env,
            )
            if base_where is not None:
                base_select = base_select.where(base_where)
            for cond in self._semi_join_exists_conditions(
                planned_query=planned_query, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            ):
                base_select = base_select.where(cond)
            base_dim_only_dedup = (
                planned_query.distinct_dimension_values
                and bool(base_group_by)
                and not base_has_agg
            )
            if (base_has_agg or base_dim_only_dedup) and base_group_by:
                for gb in base_group_by.values():
                    base_select = base_select.group_by(gb)
            if base_having is not None:
                base_select = base_select.having(base_having)



        proj_exprs: Dict[str, List[Expression]] = {}
        combined_aliases_by_slot_id: Dict[str, List[str]] = {}

        def _emit(sid: str, expr: Expression) -> None:
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
                _emit(sid, grain_alias_column(alias=full_alias, table="_base"))
            if aliases:
                combined_aliases_by_slot_id[sid] = list(aliases)
        outer_composite_order_alias_by_sid: Dict[str, str] = {}
        outer_composite_order_expressions: Dict[str, Expression] = {}
        if outer_composite_slot_ids:
            outer_composite_cm_map: Dict[str, Tuple[str, str]] = {}
            for _ph_key, _cm in regroup_placeholder_to_cm.items():
                _ph_slot = slot_by_key.get(_ph_key)
                if _ph_slot is not None:
                    outer_composite_cm_map[_ph_slot.id] = _cm

            def _render_outer_composite(cslot) -> Expression:
                rendered = render_value_key(
                    key=cslot.key,
                    ctx=self._outer_wrapper_render_ctx(
                        slot_by_key=slot_by_key,
                        cross_model_agg_slot_to_cm=outer_composite_cm_map,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ),
                )
                if cslot.type is not None:
                    rendered = _wrap_cast_for_type(expr=rendered, dt=self._slot_cast_type(cslot))
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
                full_alias = f"{source_relation}.{public_alias}"
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
        for _ph_key, (_cte_name, _agg_col) in regroup_placeholder_to_cm.items():
            ph_slot = slot_by_key.get(_ph_key)
            if ph_slot is None or ph_slot.id not in regroup_placeholder_slot_ids:
                continue
            if ph_slot.hidden and planned_query.transform_layers:
                _emit(
                    ph_slot.id,
                    grain_alias_column(alias=_agg_col, table=_cte_name).as_(
                        _agg_col, quoted=True,
                    ),
                )
                combined_aliases_by_slot_id[ph_slot.id] = [_agg_col]
                continue
            public_aliases = (
                []
                if ph_slot.hidden
                else self._public_aliases_for_cross_model_agg(
                    slot=ph_slot,
                    source_relation=source_relation,
                    canonical_alias=_agg_col,
                )
            )
            for pub in public_aliases:
                col = grain_alias_column(alias=_agg_col, table=_cte_name)
                _emit(
                    ph_slot.id,
                    col if pub == _agg_col else col.as_(pub, quoted=True),
                )
            combined_aliases_by_slot_id[ph_slot.id] = list(public_aliases)

        # Both plan kinds join back on the shared grain null-safely (a NULL dim value keeps its aggregate); an empty
        # grain becomes a CROSS JOIN.
        # Every renderer consumes planned_query.projection verbatim; a slot appears once per declared name and each
        # occurrence consumes the next of its rendered columns.
        combined_select_exprs: List[Expression] = []
        consumed: Dict[str, int] = {}
        for sid in planned_query.projection:
            exprs = proj_exprs.get(sid)
            if not exprs:
                continue
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
            on_condition = build_grain_joinback_condition(
                pairs=[
                    (
                        grain_alias_column(alias=host, table="_base"),
                        grain_alias_column(alias=cte_col, table=cte_name),
                    )
                    for host, cte_col in joinback_pairs
                ],
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
            # Map every cross-model aggregate slot (filtered-local AND forward) to its _cm_ column: a mixed AGGREGATE
            # filter resolves both operands outer, so mapping only filtered-local ones makes the forward operand raise.
            cross_model_agg_slot_to_cm: Dict[str, Tuple[str, str]] = {}
            for _ph_key, _cm in regroup_placeholder_to_cm.items():
                _ph_slot = slot_by_key.get(_ph_key)
                if _ph_slot is not None:
                    cross_model_agg_slot_to_cm[_ph_slot.id] = _cm
            for fp in outer_where_filters:
                rendered = render_value_key(
                    key=fp.expression.value_key,
                    ctx=self._outer_wrapper_render_ctx(
                        slot_by_key=slot_by_key,
                        cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ),
                )
                combined_select = combined_select.where(_grouped(rendered))

        if planned_query.transform_layers:
            return self._render_steps_and_post(
                prelude_nodes=[
                    *row_regroup_ctes,
                    Node(
                        name="_base", phase="base", query=base_select,
                        depends_on=[
                            *[e.name for e in row_regroup_ctes], *reused_cm_ctes,
                        ],
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
                source_model=source_model,
                source_relation=source_relation,
                slots_by_id=slots_by_id,
                regroup_env={
                    **row_regroup_env,
                    **{
                        ph: grain_alias_column(alias=agg_col, table=cte_name)
                        for ph, (cte_name, agg_col)
                        in regroup_placeholder_to_cm.items()
                    },
                },
                regroup_join_specs=[
                    *row_regroup_join_specs, *regroup_shift_specs,
                ],
            )

        # WITH dependencies are declared, not discovered by scanning the statement; the assembler emits a stable
        # topological order with declaration order as tiebreak.
        cte_entries = [
            CteEntry(
                name="_base", query=base_select,
                depends_on=[
                    *[e.name for e in row_regroup_ctes], *reused_cm_ctes,
                ],
            ),
            *row_regroup_ctes,
        ]
        cte_entries += cm_regroup_ctes
        combined_statement = self._assemble_with_chain(
            entries=cte_entries, final=combined_select,
            external_names=self._external_cte_names(),
        )

        # Emit ORDER BY/LIMIT/OFFSET at the combined level through one resolver: naming a projected cross-model
        # aggregate by its CTE column picks the wrong column once two scopes project the same name.
        order_env = OrderEnv(dialect=self._dialect)
        for _ph_key, (_cte_name, _agg_col) in regroup_placeholder_to_cm.items():
            _ph_slot = slot_by_key.get(_ph_key)
            if _ph_slot is not None:
                order_env.cross_model_cte[_ph_slot.id] = grain_alias_column(
                    alias=_agg_col, table=_cte_name,
                )
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
                else grain_alias_column(alias=_full_alias, table="_base")
            )
        order_terms = [
            resolve_order_term(entry=entry, env=order_env)
            for entry in lowered.order
        ]
        if order_terms:
            combined_statement.set("order", exp.Order(expressions=order_terms))

        combined_statement = self._dialect.apply_pagination(
            combined_statement,
            limit=planned_query.limit,
            offset=planned_query.offset,
        )

        return combined_statement

    def _canonical_cross_model_alias(
        self,
        *,
        source_relation: str,
        key,
    ) -> str:
        """Build the canonical result-key alias for a cross-model"""
        # The kwarg suffix is part of the CTE name so two parametric aggregates (p=0.5 vs p=0.95) get distinct
        # names/aliases instead of colliding.
        alias = canonical_aggregate_alias(
            key, profile="cross_model_cte", source_relation=source_relation,
        )
        assert alias is not None  # the cross_model_cte profile never declines
        return alias

    def _public_aliases_for_cross_model_agg(
        self,
        *,
        slot,
        source_relation: str,
        canonical_alias: str,
    ) -> List[str]:
        """User-facing combined-SELECT aliases for this cross-model slot."""
        if not slot.public_aliases:
            return [canonical_alias]
        return [f"{source_relation}.{a}" for a in slot.public_aliases]

    @staticmethod
    def _producer_render_bundle(attach, bundle):
        """The bundle a target-rooted producer renders against."""
        root_name = getattr(attach, "producer_root_model", None)
        if not root_name:
            return bundle
        root = bundle.get_referenced_model(root_name)
        if root is None:
            raise RuntimeError(
                f"Target-rooted regroup producer names root model {root_name!r}, "
                f"absent from the bundle's referenced models."
            )
        return bundle.rerooted(root)

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
    def _stage_scope(self, relation: Optional[str]) -> Iterator[None]:
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
        self, parsed: exp.Select,
    ) -> Tuple[List[CteEntry], exp.Select]:
        """Split a composed statement into (hoisted CTE entries, de-WITHed body).

        Each entry's ``depends_on`` comes from the top statement-scoped registry
        (declared at assembly time), re-keyed through the ``_base`` rename map so
        an edge onto a renamed base still resolves. Fails closed if one CTE name
        appears in two ``WITH`` nodes of the statement."""
        with_nodes = list(parsed.find_all(exp.With))
        if not with_nodes:
            return [], parsed
        allocator = self._gen_allocator or self._new_allocator()
        captured = self._gen_dep_stack[-1] if self._gen_dep_stack else {}
        entries: List[CteEntry] = []
        seen: Set[str] = set()
        for with_node in with_nodes:
            rename = self._uniquify_producer_base_ctes(
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
                entries.append(CteEntry(
                    name=name, query=cte.this.copy(), depends_on=deps,
                ))
            with_node.pop()
        return entries, parsed

    def _split_root_ctes(
        self, parsed: exp.Select,
    ) -> Tuple[List[CteEntry], exp.Select]:
        """Split the multi-stage ROOT statement into (its own CTE entries, de-WITHed
        body). The root is the outermost consumer, so its base CTE is NOT renamed;
        each entry takes its declared deps from the top registry (identity re-key)."""
        with_node = parsed.args.get("with_")
        if with_node is None:
            return [], parsed
        captured = self._gen_dep_stack[-1] if self._gen_dep_stack else {}
        entries = [
            CteEntry(
                name=cast(str, cte.alias_or_name), query=cte.this.copy(),
                depends_on=list(captured.get(cast(str, cte.alias_or_name), ())),
            )
            for cte in with_node.expressions
        ]
        parsed.set("with_", None)
        return entries, parsed

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
        self, *, planned_query, bundle, source_relation, slot_by_key,
    ):
        """Render each combined regroup producer as a ``_cm_*`` CTE."""
        ctes: List[Node] = []
        placeholder_to_cm: Dict[Any, Tuple[str, str]] = {}
        placeholder_slot_ids: Set[str] = set()
        joinbacks: List[Tuple[str, List[Tuple[str, str]]]] = []
        shift_specs: List[Tuple[str, List[Tuple[Any, str]]]] = []
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
                    alias=self._canonical_cross_model_alias(
                        source_relation=source_relation, key=seed_key,
                    ),
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
                placeholder_to_cm[sub.placeholder] = (cte_name, agg_col)
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
            pairs: List[Tuple[str, str]] = []
            shift_pairs: List[Tuple[Any, str]] = []
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
                pairs.append((host_alias, grain_alias))
                shift_pairs.append((host_slot.key, grain_alias))
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
    def _regroup_attach_identity(attach):
        """Structural identity of a regroup attach's producer: the"""
        return (
            frozenset(sub.original_key for sub in attach.substitutions),
            frozenset(host_key for host_key, _ in attach.join_pairs),
        )

    def _prepare_regroup_attaches(  # NOSONAR(S3776) — one linear pass over the planned regroup producers (dedup → render → hoist); splitting would thread the CTE registry through every helper
        self, *, planned_query, bundle, dedup_producers=None,
    ):
        """Render each regroup producer as a ``_cm_*`` CTE."""
        dedup_producers = dedup_producers or {}
        ctes: List[CteEntry] = []
        attached_env: Dict[Any, Expression] = {}
        join_specs: List[Tuple[str, List[Tuple[Any, str]]]] = []
        reused_cte_names: List[str] = []
        allocator = self._gen_allocator or self._new_allocator()
        for attach in planned_query.regroup_attach_plans:
            if attach.attach_phase != "row":
                continue
            dedup = dedup_producers.get(self._regroup_attach_identity(attach))
            if dedup is not None:
                dedup_cte, okey_to_col, grain_pairs = dedup
                for sub in attach.substitutions:
                    attached_env[sub.placeholder] = grain_alias_column(
                        alias=okey_to_col[sub.original_key], table=dedup_cte,
                    )
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
                    attached_env[sub.placeholder] = grain_alias_column(
                        alias=col_by_sid[sub.producer_slot_id], table=shared_cte,
                    )
                join_specs.append((shared_cte, [
                    (host_key, col_by_sid[producer_slot_id])
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
            # Flatten each producer output column to a dot-free name.
            def _flat(slot) -> str:
                dotted = self._full_alias_for_slot(
                    slot=slot, source_relation=relation, alias_index={},
                )
                prefix = f"{relation}."
                stripped = dotted[len(prefix):] if dotted.startswith(prefix) else dotted
                return stripped.replace(".", "__")

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
                source_relation=relation, inner=producer_body,
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
                attached_env[sub.placeholder] = grain_alias_column(
                    alias=_flat(agg_slot), table=cte_name,
                )
            pairs: List[Tuple[Any, str]] = []
            for host_key, producer_slot_id in attach.join_pairs:
                grain_slot = sub_slots.get(producer_slot_id)
                if grain_slot is None:
                    raise RuntimeError(
                        f"Regroup producer is missing grain slot "
                        f"{producer_slot_id!r}.",
                    )
                pairs.append((host_key, _flat(grain_slot)))
            join_specs.append((cte_name, pairs))
            if rendered_map is not None:
                rendered_map[ident] = (cte_name, exposed_by_sid)
        return ctes, attached_env, join_specs, reused_cte_names

    def _full_alias_for_slot(
        self,
        *,
        slot,
        source_relation: str,
        alias_index: Dict[str, int],
    ) -> str:
        """Build the SQL public alias for one ``ValueSlot``."""

        if slot.phase == Phase.ROW:
            key = slot.key
            path: Tuple[str, ...] = ()
            leaf: Optional[str] = None
            if isinstance(key, ColumnKey):
                path, leaf = key.path, key.leaf
            elif isinstance(key, ColumnSqlKey):
                path, leaf = key.path, key.column_name
            elif isinstance(key, TimeTruncKey):
                path, leaf = column_path(key.column), column_leaf(key.column)
            if path and leaf is not None:
                if isinstance(key, TimeTruncKey):
                    return time_trunc_result_key(
                        source_relation=source_relation, path=path, leaf=leaf,
                        granularity=key.granularity, declared_name=slot.declared_name,
                    )
                return result_key(
                    source_relation=source_relation, path=path, leaf=leaf,
                )
        if slot.public_aliases:
            alias = self._pick_alias_for_planned_slot(
                slot=slot, alias_index=alias_index,
            )
        else:
            alias = slot.declared_name
        return result_key_from_alias(source_relation=source_relation, alias=alias)

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
            if isinstance(key, ColumnKey):
                _add(key.path)
            elif isinstance(key, TimeTruncKey):
                _add(key.column.path)

        for sid in base_render_order:
            _add_row_slot(sid)
        for sid in order_slot_ids or ():
            _add_row_slot(sid)
        return ordered

    def _oriented_hop_chain(self, *, source_model, path, bundle):
        """Resolve ``path`` tokens into ``(OrientedJoin, next_model)`` hops through
        the shared bidirectional walker — reverse hops and edge-name tokens
        included. Raises ``ValueError`` on a missing hop (its own message) and
        propagates ``AmbiguousJoinPathError`` on an ambiguous one."""
        models_by_name = bundle.models_by_name
        models_by_name.setdefault(source_model.name, source_model)
        current = source_model
        chain = []
        for hop in path:
            edge = resolve_hop(
                current=current, token=hop, models_by_name=models_by_name
            )
            if edge is None:
                raise ValueError(
                    f"Model {current.name!r} has no join to "
                    f"{hop!r}; needed for joined path {path!r}.",
                )
            next_model = models_by_name.get(edge.target_model)
            if next_model is None:
                raise ValueError(
                    f"Join target {edge.target_model!r} not in resolved "
                    f"source bundle.",
                )
            chain.append((edge, next_model))
            current = next_model
        return chain

    def _build_from_and_joins(
        self,
        *,
        source_model,
        source_relation: str,
        joined_paths: List[Tuple[str, ...]],
        bundle,
    ):
        """Build ``(from_expr, joins)`` for a base SELECT."""
        base_from = self._build_from_clause_from_planned(
            source_model=source_model, source_relation=source_relation,
        )
        joins: List = []
        if not joined_paths:
            return base_from, joins
        emitted_aliases: set = {source_relation}
        for path in joined_paths:
            current_alias = source_relation
            chain = self._oriented_hop_chain(
                source_model=source_model, path=path, bundle=bundle,
            )
            prev_model = source_model
            for hop_idx, (edge, next_model) in enumerate(chain):
                next_alias = self._join_alias(
                    root=source_relation, path=path[: hop_idx + 1],
                )
                if next_alias not in emitted_aliases:
                    join_on_parts = []
                    for src_col, tgt_col in physical_join_pairs(
                        edge=edge, source=prev_model, target=next_model,
                    ):
                        # _to_ident quotes mixed-case keys; table qualifiers are internal aliases.
                        join_on_parts.append(exp.EQ(
                            this=exp.Column(
                                this=self._to_ident(src_col),
                                table=exp.to_identifier(current_alias),
                            ),
                            expression=exp.Column(
                                this=self._to_ident(tgt_col),
                                table=exp.to_identifier(next_alias),
                            ),
                        ))
                    target_table = (
                        next_model.sql_table or next_model.name
                    )
                    if next_model.sql and not next_model.sql_table:
                        join_expr = exp.Subquery(
                            this=self._parse(next_model.sql),
                            alias=exp.to_identifier(next_alias),
                        )
                    else:
                        join_expr = self._to_table(target_table, alias=next_alias)
                    on_expr = (
                        exp.and_(*join_on_parts)
                        if len(join_on_parts) > 1
                        else join_on_parts[0]
                    )
                    # Root-relative join type: LEFT keeps the querying root whole
                    # in the traversal direction, INNER is symmetric; RIGHT is
                    # never emitted. The oriented edge carries the
                    # declared type unchanged.
                    joins.append((
                        join_expr, on_expr, edge.join_type.value.upper(),
                    ))
                    emitted_aliases.add(next_alias)
                current_alias = next_alias
                prev_model = next_model
        return base_from, joins

    def _joined_or_local_dim_expr(
        self,
        *,
        path: Tuple[str, ...],
        leaf: str,
        source_model,
        source_relation: str,
        bundle,
    ) -> Expression:
        """Resolve a dimension column expression on either the host"""
        if not path:
            return self._dim_column_expr_from_planned(
                source_model=source_model,
                source_relation=source_relation,
                leaf=leaf,
            )
        current_alias = source_relation
        chain = self._oriented_hop_chain(
            source_model=source_model, path=path, bundle=bundle,
        )
        current_model = source_model
        for hop_idx, (_edge, next_model) in enumerate(chain):
            current_alias = self._join_alias(
                root=source_relation, path=path[: hop_idx + 1],
            )
            current_model = next_model
        col_def = next(
            (c for c in current_model.columns if c.name == leaf), None,
        )
        if col_def is None:
            raise ValueError(
                f"Column {leaf!r} not found on joined model "
                f"{current_model.name!r}.",
            )
        return exp.Column(
            this=exp.to_identifier(leaf),
            table=exp.to_identifier(current_alias),
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
    def _transform_grain_slot_ids(*, planned_query, slots_by_id) -> List[str]:
        """The transform auto-grain: every projected"""
        combined_placeholders = {
            sub.placeholder
            for plan in planned_query.regroup_attach_plans
            if plan.attach_phase == "combined"
            for sub in plan.substitutions
        }
        out: List[str] = []
        for sid in planned_query.projection:
            slot = slots_by_id.get(sid)
            if slot is None or slot.phase != Phase.ROW:
                continue
            key = slot.key
            if isinstance(key, TimeTruncKey) or key in combined_placeholders:
                continue
            if isinstance(key, (ColumnKey, ColumnSqlKey)) or slot.is_dimension:
                out.append(sid)
        return out

    def _render_window_transform_sql(  # NOSONAR(S3776) — one per-op dispatch over the window-transform vocabulary, sharing the resolved measure / frame / partition state every arm reads. Each arm is one line; splitting the dispatch scatters that state without simplifying it.
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

        # A composite transform input renders inline against operands' already-materialised aliases; the Kahn readiness
        # check guarantees they're in a prior CTE.

        if isinstance(key.input, (ArithmeticKey, ScalarCallKey)):
            # A composite input that IS a projected computed dimension reads its
            # grouped alias, never re-renders the expression over base columns.
            measure = render_value_key(
                key=key.input,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                    composite_alias_slot_ids=(
                        self._dimension_composite_slot_ids(planned_query)
                    ),
                ),
            )
        else:
            input_sid = slot_id_by_key.get(key.input)
            if input_sid is None or input_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"transform input not materialised: slot id={slot.id!r}, "
                    f"op={key.op!r}, input_key={key.input!r}.",
                )
            measure = exp.column(
                available_alias_by_slot_id[input_sid], quoted=True,
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

        # Explicit partition_keys win; otherwise auto-partition by query dimension slots (ColumnKey row-phase), never
        # TimeTruncKey.
        if key.partition_keys:
            partition_aliases: list[str] = []
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
                partition_aliases.append(
                    available_alias_by_slot_id[pk_sid],
                )
        elif key.op in RANK_FAMILY_TRANSFORMS:
            partition_aliases = []
        else:
            partition_aliases = []
            for sid in self._transform_grain_slot_ids(
                planned_query=planned_query, slots_by_id=slots_by_id,
            ):
                alias = available_alias_by_slot_id.get(sid)
                if alias is not None:
                    partition_aliases.append(alias)

        partition_by = [exp.column(a, quoted=True) for a in partition_aliases]

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

        time_order = (
            exp.Order(expressions=[self._window_ordered(time_col.copy())])
            if time_col is not None
            else None
        )
        # Rank has no frame; pin uniform NULLS LAST (not frame-safe native) for cross-dialect parity.
        rank_order = exp.Order(
            expressions=[self._dialect.build_ordered(measure.copy(), descending=True)],
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
            return _over(exp.Sum(this=measure), order=time_order)
        if op == "lag":
            n = abs(_normalise_periods(kwarg_map.get("periods", 1)))
            return _over(
                exp.Lag(this=measure, offset=exp.Literal.number(n)),
                order=time_order,
            )
        if op == "lead":
            n = abs(_normalise_periods(kwarg_map.get("periods", 1)))
            return _over(
                exp.Lead(this=measure, offset=exp.Literal.number(n)),
                order=time_order,
            )
        if op == "rank":
            return _over(exp.Rank(), order=rank_order)
        if op == "percent_rank":
            return _over(exp.PercentRank(), order=rank_order)
        if op == "dense_rank":
            return _over(exp.DenseRank(), order=rank_order)
        if op == "ntile":
            # Route through the shared normaliser (like lag/lead) so bool is rejected and a non-integral Decimal raises
            # rather than truncating; render-side defense.
            n = _normalise_periods(raw=kwarg_map.get("n"), kw="n")
            if n <= 0:
                raise ValueError(
                    f"ntile requires a positive integer n, got {n!r}",
                )
            return _over(
                exp.Ntile(this=exp.Literal.number(n)), order=rank_order,
            )
        if op == "first":
            return _over(
                exp.FirstValue(this=measure),
                order=time_order, spec=unbounded_frame,
            )
        if op == "last":
            if time_col is None:
                raise ValueError(
                    f"Transform 'last' requires an unambiguous time "
                    f"dimension (binder/planner gap; slot id={slot.id!r}).",
                )
            # last is first over the reversed time axis, so it takes the descending order.
            return _over(
                exp.FirstValue(this=measure),
                order=exp.Order(expressions=[
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
            if fp.expression is None:
                raise ValueError(
                    f"POST-phase mask id={fp.id!r} has no typed "
                    f"expression; text-only POST filters are not supported.",
                )
            rendered = render_value_key(
                key=fp.expression.value_key,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                    composite_alias_slot_ids=(
                        self._dimension_composite_slot_ids(planned_query)
                    ),
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
            resolve_order_term(entry=entry, env=env)
            for entry in _lower_positions(planned_query).order
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
        planned_query = render.planned_query
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
        entries.append(Node(
            name=cte_name, phase="producer", query=body,
            depends_on=[*[h.name for h in hoisted], *self._reuse_deps_of(cte_name)],
        ))
        col_by_sid = {
            s.id: self._full_alias_for_slot(
                slot=s, source_relation=producer.source_relation, alias_index={},
            )
            for s in (*producer.row_slots, *producer.aggregate_slots,
                      *producer.combined_expression_slots)
        }
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
        bucket_granularity = TimeGranularity(time_key.granularity)
        lookup_expr = self._build_time_offset_expr(
            col_expr=grain_alias_column(alias=time_alias, table=chain_tail),
            offset=periods, granularity=TimeGranularity(shift_granularity),
        )
        if not _shift_preserves_bucket_starts(
            bucket=bucket_granularity, shift=shift_granularity,
        ):
            lookup_expr = self._build_date_trunc(
                col_expr=lookup_expr, granularity=bucket_granularity,
            )
        sjoin_on = build_grain_joinback_condition(
            pairs=[
                (
                    lookup_expr if host == time_alias
                    else grain_alias_column(alias=host, table=chain_tail),
                    grain_alias_column(alias=shifted, table=shifted_cte_name),
                )
                for host, shifted in pairs
            ],
            dialect=self._dialect,
        )
        source_relation = chain.source_relation
        slot_full_aliases = [f"{source_relation}.{a}" for a in slot_aliases]
        sjoin_select = exp.Select().select(
            *[grain_alias_column(alias=a, table=chain_tail)
              for a in self._carry_aliases_in_plan_order(chain.aliases_by_slot_id)],
            *[grain_alias_column(alias=value_alias, table=shifted_cte_name).as_(
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
        """Emit ``cp_reset_<alias>`` + ``cp_value_<alias>`` CTEs for one"""
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
            planned_query=planned_query, slots_by_id=slots_by_id,
        ):
            alias = available_alias_by_slot_id.get(sid)
            if alias is not None:
                partition_aliases.append(alias)

        if slot.public_aliases:
            slot_alias = slot.public_aliases[0]
        else:
            slot_alias = cte_allocator.allocate_cte(slot.declared_name)
        full_slot_alias = f"{source_relation}.{slot_alias}"
        cp_reset_alias = f"_cp_reset_{full_slot_alias}"

        prev_cte = chain_tail
        carry_aliases = self._carry_aliases_in_plan_order(
            aliases_by_slot_id,
        )
        carry_cols = [exp.column(a, quoted=True) for a in carry_aliases]
        running_frame = exp.WindowSpec(
            kind="ROWS",
            start="UNBOUNDED", start_side="PRECEDING", end="CURRENT ROW",
        )

        def _running_sum(
            *, then: int, other: int, partitions: List[str],
        ) -> exp.Window:
            """``SUM(CASE WHEN <pred> THEN … ELSE … END) OVER (… ROWS BETWEEN"""
            args: Dict[str, Any] = {
                "this": exp.Sum(this=exp.Case(
                    ifs=[exp.If(
                        this=predicate.copy(),
                        true=exp.Literal.number(then),
                    )],
                    default=exp.Literal.number(other),
                )),
                "order": exp.Order(expressions=[
                    self._window_ordered(exp.column(time_alias, quoted=True)),
                ]),
                "spec": running_frame.copy(),
            }
            if partitions:
                args["partition_by"] = [
                    exp.column(a, quoted=True) for a in partitions
                ]
            return exp.Window(**args)

        cp_reset_cte_name = cte_allocator.allocate_cte(f"cp_reset_{slot_alias}")
        ctes.append(CteEntry(
            name=cp_reset_cte_name,
            query=exp.Select().select(
                *(c.copy() for c in carry_cols),
                _running_sum(
                    then=0, other=1, partitions=partition_aliases,
                ).as_(cp_reset_alias, quoted=True),
            ).from_(prev_cte),
            depends_on=[prev_cte],
        ))

        value_outer_case = exp.Case(
            ifs=[exp.If(
                this=predicate.copy(),
                true=_running_sum(
                    then=1, other=0,
                    partitions=partition_aliases + [cp_reset_alias],
                ),
            )],
            default=exp.Literal.number(0),
        )
        cp_value_cte_name = cte_allocator.allocate_cte(f"cp_value_{slot_alias}")
        output_aliases = [
            f"{source_relation}.{a}" for a in slot.public_aliases
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

    @staticmethod
    def _pick_alias_for_planned_slot(*, slot, alias_index: dict) -> str:
        """Pick the next alias for a slot in projection order."""
        idx = alias_index.setdefault(slot.id, 0)
        if idx < len(slot.public_aliases):
            alias = slot.public_aliases[idx]
        else:
            alias = slot.declared_name
        alias_index[slot.id] = idx + 1
        return alias

    def _mode_a_scope(
        self, *, source_model, source_relation: str, bundle,
    ) -> ScopeFrame:
        """An ephemeral :class:`ScopeFrame` for a Mode-A entry whose call site"""
        return ScopeFrame(
            scope_id=f"_modea_{source_relation}",
            root_model=source_model,
            root_relation=source_relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=self._new_allocator(),
        )

    def _enter_mode_a_predicate(
        self,
        *,
        sql: str,
        scope: Optional[ScopeFrame] = None,
        source_model=None,
        source_relation: Optional[str] = None,
        bundle=None,
        location: Optional[str] = None,
        owner_path: Tuple[str, ...] = (),
    ) -> Expression:
        """Enter a Mode-A PREDICATE through the door and hand back its AST."""
        frame = scope or self._mode_a_scope(
            source_model=source_model,
            source_relation=source_relation,
            bundle=bundle,
        )
        return frame.enter_predicate(sql, location=location, owner_path=tuple(owner_path))

    def _enter_mode_a_expression(
        self,
        *,
        sql: str,
        scope: ScopeFrame,
        location: Optional[str] = None,
        owner_path: Tuple[str, ...] = (),
    ) -> Expression:
        """Enter a Mode-A scalar EXPRESSION (a ``Column.sql`` / aggregation"""
        return scope.enter_expression(sql, location=location, owner_path=tuple(owner_path))

    def _default_frag_entry(
        self, *, frag: str, scope: ScopeFrame, source_owner_path: Tuple[str, ...],
    ) -> Tuple[str, Tuple[str, ...]]:
        """The (fragment, owner_path) to enter for a definition default on a
        host-locus aggregate. Every reference resolves owner-first
        with reverse-hop cancellation and a root fallback. A fragment whose every
        reference is owner-forward (its absolute path extends the source owner
        path) enters raw at the owner path — byte-identical by construction. Any
        cancelled or root-anchored reference makes the fragment MIXED: it is
        requalified per reference to its absolute path and entered at the root, so
        each reference resolves in its own frame, never as a reverse join."""
        try:
            parsed = sqlglot.parse_one(frag, dialect=self.dialect)
            abs_refs = resolve_default_reference_paths(
                parsed=parsed,  # pyright: ignore[reportArgumentType] — parse_one's Expr TypeVar
                owner_path=source_owner_path,
                root_model=scope.root_model, root_path=(), bundle=scope.bundle,
            )
        except Exception:
            return frag, source_owner_path  # unanalysable: raw at the source owner
        n = len(source_owner_path)
        if all(
            a is not None and tuple(a[:n]) == tuple(source_owner_path)
            for a, _ in abs_refs
        ):
            return frag, tuple(source_owner_path)  # owner-forward: raw at the owner
        return requalify_default_references(
            parsed=parsed,  # pyright: ignore[reportArgumentType] — parse_one's Expr TypeVar
            abs_refs=abs_refs, dialect=self.dialect,
        ), ()

    def _fragment_placeholders(self, *, key, agg_def) -> Optional[frozenset[str]]:
        """Placeholder names of the rendered formula; None when it has none (e.g. corr, percentile)."""
        formula = rendered_formula(agg=key.agg, definition=agg_def)
        if not formula:
            return None
        return self._formula_template(agg_name=key.agg, formula=formula).placeholder_names

    def _register_fragment_kwarg_joins(
        self, *, key, scope: ScopeFrame, model, owner_path: Tuple[str, ...] = (),
        source_owner_path: Optional[Tuple[str, ...]] = None,
    ) -> "Dict[str, Expression]":
        """Resolve an aggregation's template FRAGMENTS through the Mode-A door,"""
        agg_def = next(
            (a for a in (model.aggregations or []) if a.name == key.agg), None,
        )
        placeholders = self._fragment_placeholders(key=key, agg_def=agg_def)
        overridden = {name for name, _ in key.kwargs}
        # (name, fragment, owner_path). Explicit string kwargs keep the caller's
        # owner_path; a definition default on a host-locus aggregate resolves at
        # the root or the source owner per its reference frame.
        named_fragments: List[Tuple[str, str, Tuple[str, ...]]] = [
            (name, v, tuple(owner_path)) for name, v in key.kwargs
            if isinstance(v, str) and _is_fragment(name=name, placeholders=placeholders)
        ]
        for p in (agg_def.params if agg_def else []):
            if p.name in overridden or not _is_fragment(name=p.name, placeholders=placeholders):
                continue
            if source_owner_path is not None:
                frag_sql, frag_owner_path = self._default_frag_entry(
                    frag=p.sql, scope=scope,
                    source_owner_path=source_owner_path,
                )
            else:
                frag_sql, frag_owner_path = p.sql, tuple(owner_path)
            named_fragments.append((p.name, frag_sql, frag_owner_path))
        resolved: "Dict[str, Expression]" = {}
        for name, frag, frag_owner_path in named_fragments:
            resolved[name] = self._enter_mode_a_expression(
                sql=frag, scope=scope, owner_path=frag_owner_path,
                location=(
                    f"aggregation {key.agg!r} template fragment on model "
                    f"{model.name!r}"
                ),
            )
        return resolved

    def _expand_derived_row_dims(  # NOSONAR(S3776) — one cohesive per-slot pass expanding derived ROW/TIME dimensions and registering the joins they cross.
        self, *, base_render_order, slots_by_id, source_relation: str,
        source_model, bundle, scope: ScopeFrame,
        order_slot_ids: Optional[List[str]] = None,
    ) -> Dict[str, Expression]:
        """Pre-expand derived (``ColumnSqlKey``) ROW dimensions and derived TIME"""

        def _add(path: Tuple[str, ...]) -> None:
            if path:
                scope.join_paths.add(path)

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
            if isinstance(key, TimeTruncKey) and isinstance(key.column, ColumnSqlKey):
                raw = self._raw_time_col_expr_for_planned(
                    time_column=key.column, source_model=source_model,
                    source_relation=source_relation, bundle=bundle,
                )
                _add(key.column.path)
                for p in self._joined_paths_in_sql(
                    sql_expr=raw, source_relation=source_relation,
                    source_model=source_model, bundle=bundle,
                ):
                    _add(p)
                continue
            if not isinstance(key, ColumnSqlKey):
                continue
            # A cross-model derived dim expands rooted at the owning join's __ alias with is_root=False, so a
            # further-joined ref carries the full prefix (B reaching C -> B__C).
            crossed: Set[Tuple[str, ...]] = set()
            expr = self._derived_column_expr(
                key=key, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                crossed_paths=crossed,
            )
            if expr is None:
                continue
            derived_expr_by_sid[sid] = expr
            _add(key.path)  # the join to the owning model itself (cross-model)
            for p in sorted(crossed, key=lambda t: (len(t), t)):
                _add(p)
        return derived_expr_by_sid

    def _derived_column_expr(
        self, *, key, source_model, source_relation: str, bundle,
        crossed_paths: "Optional[Set[Tuple[str, ...]]]" = None,
    ) -> "Optional[Expression]":
        """The rendered expression for a derived (``ColumnSqlKey``) column."""
        if key.path:
            owner_model = self._walk_join_path_model(
                source_model=source_model, path=key.path, bundle=bundle,
            )
            if owner_model is None:
                return None
            owner_relation = "__".join(key.path)
        else:
            owner_model = source_model
            owner_relation = source_relation
        expanded_sql = self._expand_derived_column_sql(
            source_model=owner_model, source_relation=owner_relation,
            column_name=key.column_name, bundle=bundle,
            owner_path=tuple(key.path), root_relation=source_relation,
            crossed_paths=crossed_paths,
        )
        col = next(
            (c for c in owner_model.columns if c.name == key.column_name), None,
        )
        return _wrap_cast_for_type(
            expr=self._parse(expanded_sql),
            dt=self._dialect.declared_cast_type(col.type if col is not None else None),
        )


    def _build_from_clause_from_planned(
        self,
        *,
        source_model,
        source_relation: str,
    ) -> Expression:
        if source_model.sql_table:
            return self._to_table(source_model.sql_table, alias=source_relation)
        if source_model.sql:
            return exp.Subquery(
                this=self._parse(source_model.sql),
                alias=exp.to_identifier(source_relation),
            )
        raise NotImplementedError(
            f"Model {source_model.name!r} has neither sql_table nor sql set; "
            f"query-backed models (source_queries) deferred to multi-stage "
            f"slices (DEV-1878)."
        )

    def _dim_column_expr_from_planned(
        self, *, source_model, source_relation: str, leaf: str,
    ) -> Expression:
        col = next(
            (c for c in source_model.columns if c.name == leaf), None,
        )
        if col is None:
            raise ValueError(
                f"Column {leaf!r} not found on model "
                f"{source_model.name!r}",
            )
        return self._resolve_sql(
            sql=col.sql, name=col.name, model_name=source_relation,
            type=col.type,
        )

    def _raw_time_col_expr_for_planned(
        self, *, time_column, source_model, source_relation: str, bundle,
    ) -> Expression:
        """Untruncated time expression for a ``TimeTruncKey.column``"""

        if isinstance(time_column, ColumnKey):
            return self._joined_or_local_dim_expr(
                path=time_column.path,
                leaf=time_column.leaf,
                source_model=source_model,
                source_relation=source_relation,
                bundle=bundle,
            )
        if isinstance(time_column, ColumnSqlKey):
            if time_column.path:
                joined_model = self._walk_join_path_model(
                    source_model=source_model, path=time_column.path,
                    bundle=bundle,
                )
                if joined_model is None:
                    raise ValueError(
                        f"Time dimension references derived column "
                        f"{time_column.column_name!r} over join path "
                        f"{'.'.join(time_column.path)!r} which does not "
                        f"resolve from the source bundle.",
                    )
                # A joined derived TIME dim whose sql crosses a further join must anchor inner refs at the host-path
                # alias, not the bare direct-join alias, or the FROM references an unjoined table.
                expanded_sql = self._expand_derived_column_sql(
                    source_model=joined_model,
                    source_relation="__".join(time_column.path),
                    column_name=time_column.column_name,
                    bundle=bundle,
                    owner_path=tuple(time_column.path),
                    cast=False,
                )
            else:
                expanded_sql = self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=time_column.column_name,
                    bundle=bundle,
                    cast=False,
                )
            return self._parse(expanded_sql)
        raise NotImplementedError(
            f"Unsupported TimeTruncKey column type: {type(time_column).__name__}",
        )

    def _expand_derived_column_sql(
        self, *, source_model, source_relation: str, column_name: str, bundle,
        owner_path: Tuple[str, ...] = (),
        root_relation: "Optional[str]" = None,
        crossed_paths: "Optional[Set[Tuple[str, ...]]]" = None,
        cast: bool = True,
    ) -> str:
        """Expand a ``ColumnSqlKey`` target to SQL, casting the value to its
        declared type (a bare column is skipped) and desugaring any ``Column.filter``
        to ``CASE WHEN <filter> THEN <value> END``. The type CAST rides
        the VALUE, so the filter CASE needs no outer cast. ``cast=False`` skips the
        type CAST for a raw-time expression (``DATE_TRUNC`` needs the untruncated
        timestamp, not a lossy ``CAST(... AS TIMESTAMP)``)."""
        col = next(
            (c for c in source_model.columns if c.name == column_name), None,
        )
        if col is None:
            raise ValueError(
                f"Derived column {column_name!r} not found on model "
                f"{source_model.name!r}",
            )
        resolver_root = root_relation if root_relation is not None else source_relation
        value, filter_sql = expand_column_definition_parts_sync(
            column=col, model=source_model, alias_path=source_relation,
            models_by_name=bundle.models_by_name, dialect=self.dialect,
            owner_path=owner_path, alias_resolver=self._join_alias_resolver(resolver_root),
            crossed_paths=crossed_paths,
        )
        value_ast = self._parse(value)
        value_sql = (
            _wrap_cast_for_type(
                expr=value_ast, dt=self._dialect.declared_cast_type(col.type),
            ) if cast else value_ast
        ).sql(dialect=self.dialect)
        return wrap_column_filter(value_sql=value_sql, filter_sql=filter_sql)

    def _render_expression_source_sql(self, *, source, scope: ScopeFrame) -> str:
        """Render an aggregate's row-level expression source through ``scope`` — one resolver for leaves, attached placeholders, derived columns and join registration."""
        return scope.resolve(source).sql(dialect=self.dialect)

    def _joined_paths_in_sql(
        self, *, sql_expr: Expression, source_relation: str, source_model,
        bundle,
    ) -> List[Tuple[str, ...]]:
        """Collect the join paths referenced by table qualifiers inside an"""
        return collect_root_scope_joined_paths(
            parsed=sql_expr,
            source_model=source_model,
            source_relation=source_relation,
            bundle=bundle,
        )

    def _resolve_where_filter_joins_via_scope(
        self, *, planned_query, scope: ScopeFrame,
        skip_filter_ids: Optional[Set[str]] = None,
        filters_override: "Optional[List[Any]]" = None,
    ) -> None:
        """Register into ``scope.join_paths`` the joins every WHERE-phase filter"""

        skip = skip_filter_ids or set()
        filters = (
            _lower_positions(planned_query).filters
            if filters_override is None else filters_override
        )
        for fp in filters:
            if fp.phase != Phase.ROW or fp.id in skip:
                continue
            if fp.expression is not None:
                for p in self._value_key_join_paths(
                    key=fp.expression.value_key, source_model=scope.root_model,
                    source_relation=scope.root_relation, bundle=scope.bundle,
                ):
                    scope.join_paths.add(p)
            elif fp.text is not None:
                # Discover joins from BOTH the un-inlined placeholder text and the inline-expanded text — each surfaces
                # joins the other hides (see ScopeFrame._enter's dual-scan).
                self._enter_mode_a_predicate(
                    sql=fp.text, scope=scope,
                    location=(
                        f"SlayerModel.filters on model "
                        f"{scope.root_model.name!r}"
                    ),
                )

    def _value_key_join_paths(  # NOSONAR(S3776) — one cohesive recursive ValueKey-tree walk; complexity is the per-key-type dispatch.
        self, *, key, source_model, source_relation: str, bundle,
    ) -> List[Tuple[str, ...]]:
        """Join paths a typed filter ``ValueKey`` tree references"""

        out: List[Tuple[str, ...]] = []

        def _add(path: Tuple[str, ...]) -> None:
            for i in range(1, len(path) + 1):
                prefix = tuple(path[:i])
                if prefix and prefix not in out:
                    out.append(prefix)

        def _derived_paths(*, model, relation, column_name, owner_path) -> None:
            crossed: Set[Tuple[str, ...]] = set()
            self._expand_derived_column_sql(
                source_model=model, source_relation=relation,
                column_name=column_name, bundle=bundle, owner_path=owner_path,
                crossed_paths=crossed,
            )
            for p in sorted(crossed, key=lambda t: (len(t), t)):
                if p not in out:
                    out.append(p)

        def _walk(k) -> None:
            if isinstance(k, ColumnKey):
                _add(k.path)
            elif isinstance(k, ColumnSqlKey):
                _add(k.path)
                model = (
                    self._walk_join_path_model(
                        source_model=source_model, path=k.path, bundle=bundle,
                    ) if k.path
                    else source_model
                )
                if model is not None:
                    _derived_paths(
                        model=model,
                        relation="__".join(k.path) if k.path else source_relation,
                        column_name=k.column_name,
                        owner_path=tuple(k.path),
                    )
            elif isinstance(k, ArithmeticKey):
                for o in k.operands:
                    _walk(o)
            elif isinstance(k, ScalarCallKey):
                for a in k.args:
                    _walk(a)
            elif isinstance(k, BetweenKey):
                _walk(k.column)
                _walk(k.low)
                _walk(k.high)
            elif isinstance(k, InKey):
                _walk(k.column)

        _walk(key)
        return out

    def _resolve_aggregation_def(
        self,
        *,
        key,
        source_model,
        src_leaf: str,
    ):
        """Look up the model-level ``Aggregation`` definition for ``key.agg``,"""
        agg_def = next(
            (a for a in (source_model.aggregations or []) if a.name == key.agg),
            None,
        )
        if agg_def is None and key.agg not in _BUILTIN_BAREARG_AGGS_LOCAL_SLICE:
            raise AggregationNotAllowedError(
                column=src_leaf,
                agg=key.agg,
                reason=(
                    f"unknown aggregation {key.agg!r} — not a built-in "
                    f"and not defined in {source_model.name!r}."
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
        """Reject CROSS-MODEL aggregates' kwarg column refs whose join path"""

        # A host-locus aggregate's inputs are certified from the home by the
        # compiler and the scope registers each kwarg's join; the gate keeps
        # guarding target-rooted producers.
        if not source.path or _is_host_grain(key):
            return
        for kname, kval in key.kwargs:
            if isinstance(kval, (ColumnKey, ColumnSqlKey)) and kval.path != source.path:
                raise AggregationNotAllowedError(
                    column=src_leaf,
                    agg=key.agg,
                    reason=(
                        f"kwarg {kname!r} references "
                        f"{type(kval).__name__} with path {kval.path!r}; "
                        f"aggregate source path is {source.path!r}. "
                        f"Cross-model kwargs must share the source's "
                        f"join path."
                    ),
                )

    def _walk_join_path_model(self, *, source_model, path, bundle):
        """The terminal model of a join ``path`` walked from ``source_model``
        via the shared walker (tokens may be edge names or reverse hops)."""
        return terminal_model(
            root=source_model, path=tuple(path),
            models_by_name=bundle.models_by_name,
        )

    def _build_agg_render_spec_from_planned(  # NOSONAR(S3776) — sequential isinstance dispatch over StarKey / ColumnKey / ColumnSqlKey with helper extractions for aggregation-def lookup, kwarg path validation, and explicit-time-arg resolution. Further splitting would scatter the per-source-kind contract.
        self,
        *,
        slot,
        key,
        source_model,
        source_relation: str,
        full_alias: str,
        bundle=None,
        resolved_agg_kwargs: "Optional[Dict[str, ResolvedAggKwarg]]" = None,
        scope: Optional[ScopeFrame] = None,
    ) -> AggRenderSpec:
        """Build an ``AggRenderSpec`` from a planned aggregate slot so"""

        # slot may be None for a HAVING term whose aggregate isn't a projection slot; the result type is then unknown
        # (no outer CAST).
        slot_type = slot.type if slot is not None else None
        source = key.source
        if isinstance(source, StarKey):
            # Reject any non-count aggregation on * (sum(*) would render as SUM(*)); enforce here so invalid SQL can't be
            # emitted.
            if key.agg != "count":
                raise ValueError(
                    f"Aggregation {key.agg!r} not allowed with measure "
                    f"'*' — use 'count(*)' for COUNT(*)."
                )
            owner = (
                self._walk_join_path_model(source_model=source_model, path=source_anchor_path(source), bundle=bundle)
                if source_anchor_path(source) and bundle is not None else source_model
            ) or source_model
            agg_def = self._resolve_aggregation_def(key=key, source_model=owner, src_leaf="*")
            if key.args or (key.kwargs and not rendered_formula(agg=key.agg, definition=agg_def)):
                raise ValueError(
                    f"'count(*)' takes no args or kwargs; got "
                    f"args={key.args!r}, kwargs={key.kwargs!r}."
                )
            resolved_kw = resolved_agg_kwargs or {}
            return AggRenderSpec(
                name="",
                sql=None,
                aggregation=key.agg,
                alias=full_alias,
                model_name=source_relation,
                type=slot_type,
                aggregation_def=agg_def,
                agg_kwargs={
                    **resolved_kw,
                    **{
                        k: ResolvedAggKwarg(kind="str", value=agg_kwarg_canonical_str(v))
                        for k, v in key.kwargs if k not in resolved_kw
                    },
                },
            )
        if isinstance(source, (ColumnKey, ColumnSqlKey)):
            host_grain_root: Optional[str] = None
            if source.path and _is_host_grain(key) and bundle is not None:
                terminal = self._walk_join_path_model(
                    source_model=source_model, path=source.path, bundle=bundle,
                )
                if terminal is not None:
                    source_model = terminal
                    # Qualify through the generation AliasAllocator, not a raw __.join, so the alias matches what
                    # _build_from_and_joins emitted.
                    host_grain_root = source_relation
                    source_relation = self._join_alias(
                        root=source_relation, path=source.path,
                    )
            src_leaf = (
                source.leaf
                if isinstance(source, ColumnKey)
                else source.column_name
            )
            # first/last render through a ranked-kernel producer, never this spec builder — a plain render would
            # silently drop the ranking.
            if key.agg in ("first", "last"):
                raise RuntimeError(
                    f"first/last aggregate {key!r} reached the plain aggregate "
                    f"renderer; it must render through a ranked-kernel "
                    f"producer."
                )
            agg_def = self._resolve_aggregation_def(
                key=key, source_model=source_model, src_leaf=src_leaf,
            )
            self._validate_aggregate_kwarg_paths(
                key=key, source=source, src_leaf=src_leaf,
            )
            col = next(
                (c for c in source_model.columns if c.name == src_leaf),
                None,
            )
            if col is None:
                raise ValueError(
                    f"Aggregate source column {src_leaf!r} not found "
                    f"on model {source_model.name!r}",
                )
            # A ColumnSqlKey source expands its value (inner bare refs qualify to
            # source_relation) and desugars any Column.filter to CASE WHEN here;
            # a plain physical column stays bare for the aggregate.
            if isinstance(source, ColumnSqlKey) and bundle is not None:
                sql_text = self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=col.name,
                    bundle=bundle,
                    owner_path=source.path if host_grain_root is not None else (),
                    root_relation=host_grain_root,
                )
                # The declared-type CAST is baked into sql_text (on the value), so
                # the aggregate must not re-cast the whole expression.
                column_type = None
            else:
                sql_text = col.sql if col.sql else col.name
                column_type = col.type
            resolved_kw = resolved_agg_kwargs or {}
            agg_kwargs_str = {
                k: (resolved_kw[k] if k in resolved_kw else agg_kwarg_canonical_str(v))
                for k, v in key.kwargs
            }
            key_kwarg_names = {k for k, _ in key.kwargs}
            for _name, _resolved in resolved_kw.items():
                if _name not in key_kwarg_names:
                    agg_kwargs_str.setdefault(_name, _resolved)
            return AggRenderSpec(
                name=col.name,
                sql=sql_text,
                aggregation=key.agg,
                alias=full_alias,
                model_name=source_relation,
                type=slot_type,
                column_type=column_type,
                agg_kwargs=agg_kwargs_str,
                aggregation_def=agg_def,
                time_column=None,
            )
        if isinstance(source, _EXPRESSION_SOURCE_KINDS):
            # Same-model expression source — render the row-level
            # expression to SQL text; every dispatch kind downstream
            # (simple / distinct / percentile / dialect hook / formula
            # ``{value}``) receives it exactly like a derived-column body.
            expr_leaf = expression_source_leaf(source)
            agg_def = self._resolve_aggregation_def(
                key=key, source_model=source_model, src_leaf=expr_leaf,
            )
            if scope is None:
                scope = self._throwaway_frame(
                    model=source_model, relation=source_relation, bundle=bundle,
                )
            sql_text = self._render_expression_source_sql(source=source, scope=scope)
            resolved_kw = resolved_agg_kwargs or {}
            agg_kwargs_str = {
                k: (resolved_kw[k] if k in resolved_kw else agg_kwarg_canonical_str(v))
                for k, v in key.kwargs
            }
            for _name, _resolved in resolved_kw.items():
                agg_kwargs_str.setdefault(_name, _resolved)
            return AggRenderSpec(
                name=expr_leaf,
                sql=sql_text,
                aggregation=key.agg,
                alias=full_alias,
                model_name=source_relation,
                type=slot_type,
                column_type=None,
                agg_kwargs=agg_kwargs_str,
                aggregation_def=agg_def,
                time_column=None,
            )
        raise NotImplementedError(
            f"AggregateKey source {type(source).__name__} not supported.",
        )

    def _semi_join_exists_conditions(
        self, *, planned_query, source_model, source_relation: str, bundle,
    ) -> "List[Expression]":
        """One correlated ``EXISTS`` per semi-join group pushed into this
        producer plan; empty for plans without pushdown."""
        groups = getattr(planned_query, "semi_join_filters", None) or []
        if not groups:
            return []
        allocator = self._gen_allocator or self._new_allocator()
        return [
            self._build_semi_join_exists(
                group=group, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                allocator=allocator,
            )
            for group in groups
        ]

    def _hop_model(self, *, name: str, bundle):
        model = bundle.get_referenced_model(name)
        if model is None and bundle.source_model is not None \
                and bundle.source_model.name == name:
            model = bundle.source_model
        if model is None:
            raise ValueError(
                f"Semi-join hop target {name!r} is not in the resolved "
                f"source bundle."
            )
        return model

    def _hop_table_expr(self, *, hop_model, alias: str) -> Expression:
        if hop_model.sql and not hop_model.sql_table:
            return exp.Subquery(
                this=self._parse(hop_model.sql),
                alias=exp.to_identifier(alias),
            )
        return self._to_table(
            name=hop_model.sql_table or hop_model.name, alias=alias,
        )

    def _build_semi_join_exists(
        self, *, group, source_model, source_relation: str, bundle, allocator,
    ) -> exp.Exists:
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
        ops = _SemiJoinOps(
            alias=_alias,
            col=lambda table, name: exp.Column(
                this=self._to_ident(name), table=exp.to_identifier(table)),
            table=lambda hop: self._hop_table_expr(
                hop_model=self._hop_model(name=hop.target_model, bundle=bundle),
                alias=_alias(hop.node_path)),
            attached={
                tuple(h.node_path) for h in group.hops
                if tuple(h.node_path[:-1]) not in in_group
            },
        )
        inner = exp.select(exp.Literal.number(1))
        if any(h.null_extended for h in group.hops if ops.is_attached(h)):
            inner = _semi_join_spine(
                ops=ops, hops=group.hops, inner=inner, ident=self._to_ident)
        else:
            inner = _semi_join_flat(ops=ops, hops=group.hops, inner=inner)
        # Conjunct refs carry tree-node paths, so the same allocator resolves
        # them to the hop aliases; root-local refs correlate to the outer body.
        ctx = RenderContext(
            scope=self._scope_frame(
                model=source_model, relation=source_relation,
                bundle=bundle, allocator=allocator,
            ),
            dialect=self._dialect,
            filters=FilterFacilities(
                cast_column_sql=True, paren_comparison_operands=True,
            ),
        )
        for key in group.conjuncts:
            rendered = render_value_key(key=key, ctx=ctx)
            if isinstance(rendered, (exp.And, exp.Or)):
                rendered = exp.Paren(this=rendered)
            inner = inner.where(rendered)
        return exp.Exists(this=inner)

    def _build_where_having_from_planned(  # NOSONAR(S3776) — one cohesive pass over the lowered entries routing each to WHERE / HAVING / POST by phase, with the per-carrier (typed vs Mode-A text) rendering and the HAVING grouped-column guard inline. The complexity is pre-existing; `filters_override` only adds a list selection. Splitting the phase routing from the rendering would thread slot_by_key / first_last_state / where_parts / having_parts through helpers without simplifying anything.
        self,
        *,
        planned_query,
        source_relation: str,
        source_model,
        bundle,
        skip_filter_ids: Optional[Set[str]] = None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
        filters_override: "Optional[List[Any]]" = None,
        regroup_env: Optional[Dict[Any, Expression]] = None,
    ):
        """``filters_override`` replaces the plan's lowered entries as the"""

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
            # An AGGREGATE-phase filter on a LOCAL aggregate renders as HAVING; a cross-model ref raises in the walker
            # (it routes via the per-plan CTE).
            target_parts = (
                having_parts if fp.phase == Phase.AGGREGATE else where_parts
            )
            if fp.phase == Phase.AGGREGATE and fp.expression is not None:
                # A HAVING referencing a bare row column not in GROUP BY would emit invalid SQL; reject early.
                grouped = {
                    s.key
                    for s in planned_query.row_slots
                    if s.id in set(planned_query.projection)
                }
                for ck in self._direct_local_column_keys(fp.expression.value_key):
                    if ck not in grouped:
                        raise ValueError(
                            f"Filter references column {ck.leaf!r} in a HAVING "
                            f"(aggregate) predicate, but it is not in the "
                            f"query's dimensions / GROUP BY."
                        )
            if fp.expression is not None:
                rendered = render_value_key(
                    key=fp.expression.value_key,
                    ctx=self._filter_render_context(
                        source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
                        slot_by_key=slot_by_key,
                        aliases_by_slot_id=aliases_by_slot_id,
                        regroup_env=regroup_env,
                    ),
                )
                target_parts.append(_grouped(rendered))
            elif fp.text is not None:
                # Mode-A filter: qualify bare refs with the source relation; a non-trivial derived reference is
                # inline-expanded and pulls its crossed joins into the FROM.
                target_parts.append(_grouped(self._enter_mode_a_predicate(
                    sql=fp.text,
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                    location=(
                        f"SlayerModel.filters on model {source_model.name!r}"
                    ),
                )))
            else:
                raise ValueError(
                    f"Lowered filter id={fp.id!r} has neither expression "
                    f"nor text (planner gap).",
                )

        return _conjunction(where_parts), _conjunction(having_parts)

    @staticmethod
    def _is_nontrivial_derived(model, name: str) -> bool:
        """True iff ``name`` is a column on ``model`` whose ``Column.sql`` is a"""
        col = next((c for c in model.columns if c.name == name), None)
        return col is not None and col.sql is not None and not is_trivial_base(
            column=col,
        )

    def _filter_agg_builder(
        self, *, source_model, source_relation: str, bundle, scope: ScopeFrame,
    ):
        """The WHERE/HAVING aggregate seam: render a local"""

        def build(agg_key, slot, having_full_alias) -> Expression:
            anchor = source_anchor_path(agg_key.source)
            if anchor:
                raise NotImplementedError(
                    f"cross-model aggregate ref in filter (path={anchor!r}) "
                    f"routes via the per-plan CTE, not inline HAVING."
                )
            having_kwargs = self._resolve_agg_kwargs_for_key(key=agg_key, scope=scope)
            synth = self._build_agg_render_spec_from_planned(
                slot=slot, key=agg_key, source_model=source_model,
                source_relation=source_relation, full_alias=having_full_alias,
                bundle=bundle, resolved_agg_kwargs=having_kwargs, scope=scope,
            )
            agg_expr, _is_agg = self._build_agg(synth)
            return agg_expr

        return build

    def _filter_render_context(
        self, *, source_model, source_relation: str, bundle,
        slot_by_key=None, aliases_by_slot_id=None, regroup_env=None,
    ) -> RenderContext:
        """A ``RenderContext`` for the WHERE/HAVING filter family, over a"""
        scope = self._throwaway_frame(
            model=source_model, relation=source_relation, bundle=bundle,
            attached_columns=regroup_env,
        )
        return RenderContext(
            scope=scope,
            dialect=self._dialect,
            filters=FilterFacilities(
                slot_by_key=slot_by_key or {},
                aliases_by_slot_id=aliases_by_slot_id or {},
                agg_builder=self._filter_agg_builder(
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                    scope=scope,
                ),
                cast_column_sql=True,
                paren_comparison_operands=True,
            ),
        )

    def _outer_wrapper_alias_facilities(
        self, *, slot_by_key, cross_model_agg_slot_to_cm, aliases_by_slot_id,
    ) -> AliasFacilities:
        """Precompute the outer-WHERE slot→qualified-column map as an"""
        slot_id_by_key: Dict[Any, str] = {}
        available_alias_by_slot_id: Dict[str, str] = {}
        table_by_slot_id: Dict[str, str] = {}
        for key, slot in slot_by_key.items():
            sid = slot.id
            cm_entry = cross_model_agg_slot_to_cm.get(sid)
            if cm_entry is not None:
                cte_name, agg_col_alias = cm_entry
                alias, table = agg_col_alias, cte_name
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
        )

    @staticmethod
    def _direct_local_column_keys(key) -> "List[Any]":
        """Local ``ColumnKey``s that appear as DIRECT (non-aggregated) operands"""

        out: List[Any] = []

        def _walk(k) -> None:
            if isinstance(k, ColumnKey):
                if k.path == ():
                    out.append(k)
                return
            if isinstance(k, (AggregateKey, TransformKey, TimeTruncKey)):
                # Aggregated / windowed inner refs aren't grouped; a
                # TimeTruncKey IS the grouped slot, not its wrapped column.
                return
            for child in k.children():
                _walk(child)

        _walk(key)
        return out

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
        source_model=None,
        bundle=None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
    ) -> List[exp.Ordered]:
        """ORDER BY terms resolved against a base SELECT with no CTE chain."""
        scoped_order = _lower_positions(planned_query).order
        env = self._host_base_order_env(
            scoped_order=scoped_order,
            source_relation=source_relation,
            slots_by_id=slots_by_id,
            source_model=source_model,
            bundle=bundle,
            aliases_by_slot_id=aliases_by_slot_id,
        )
        return [resolve_order_term(entry=entry, env=env) for entry in scoped_order]

    def _apply_planned_order_limit(
        self,
        *,
        select: exp.Select,
        planned_query,
        source_relation: str,
        slots_by_id: dict,
        source_model=None,
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
                source_model=source_model,
                bundle=bundle,
                aliases_by_slot_id=aliases_by_slot_id,
            ),
            planned_query=planned_query,
        )

    def _host_base_order_env(
        self,
        *,
        scoped_order: List[ScopedOrder],
        source_relation: str,
        slots_by_id: dict,
        source_model,
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
                    source_model=source_model,
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
        source_model,
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
        row_key = key.column if isinstance(key, TimeTruncKey) else key
        if source_model is not None and isinstance(row_key, ColumnKey):
            return self._joined_or_local_dim_expr(
                path=row_key.path, leaf=row_key.leaf,
                source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            )

        # A local derived ORDER BY column renders through the same expansion a projected derived dimension gets, so both
        # spellings sort identically.
        if (
            source_model is not None
            and bundle is not None
            and isinstance(row_key, ColumnSqlKey)
            and not row_key.path
        ):
            expr = self._derived_column_expr(
                key=row_key, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            )
            if expr is not None:
                return expr

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
    return SQLGenerator(dialect=dialect).generate_from_planned(
        planned_query, bundle=bundle,
    )


def _finish_statement(
    statement: exp.Select,
    *,
    dialect: "str | SqlDialect",
    aliases: Sequence[str] = (),
    exempt: frozenset[str] = frozenset(),
) -> str:
    """The one render of a composed statement: text, then identifier fitting, scope validation and the limit check."""
    d = dialect if isinstance(dialect, SqlDialect) else get_dialect(dialect)
    sql = d.rewrite_emitted_sql(
        statement.sql(dialect=d.sqlglot_name, pretty=True), aliases=aliases, exempt=exempt,
    )
    maybe_validate_scopes(sql, dialect=d.sqlglot_name)
    d.assert_no_overlimit_identifiers(sql, exempt=exempt)
    return sql


def _bundle_for_stage(*, planned_query, bundle, schema_by_name):
    """Pick the per-stage bundle a single DAG stage renders against."""
    ds = (bundle.source_model.data_source if bundle.source_model else "") or "_stage"
    relation = planned_query.source_relation
    if planned_query.render_source_model is not None:
        source = planned_query.render_source_model
    elif relation in schema_by_name:
        source = model_from_stage_schema(
            name=relation, schema=schema_by_name[relation], data_source=ds,
        )
    else:
        return bundle
    sibling_schemas = {n: s for n, s in schema_by_name.items() if n != relation}
    return stage_bundle_with_siblings(
        bundle=bundle, source_model=source,
        sibling_schemas=sibling_schemas, data_source=ds,
    )


def _user_authored_exemptions(
    *, bundle: ResolvedSourceBundle, dialect: "str | SqlDialect",
) -> frozenset[str]:
    """Over-limit identifier-shaped tokens from every user-authored raw-SQL surface
    in ``bundle`` — model ``sql``/``sql_table``/``filters`` and per-column
    ``name``/``sql``/``filter`` across the source, referenced, per-stage source and
    inline-extension models. These pass through emission
    unfitted; SLayer-generated ``backing_query_sql`` and synthetic stage-schema
    models (built later) are deliberately excluded."""
    d = dialect if isinstance(dialect, SqlDialect) else get_dialect(dialect)
    limit = d.max_identifier_bytes
    if limit is None:
        return frozenset()

    def _col_surfaces(col) -> list[str]:
        # ext.columns may still be raw dicts (not yet coerced to Column).
        get = col.get if isinstance(col, dict) else lambda k: getattr(col, k, None)
        return [s for s in (get("name"), get("sql"), get("filter")) if s]

    surfaces: List[str] = []
    models = [
        *([bundle.source_model] if bundle.source_model is not None else []),
        *bundle.referenced_models,
        *bundle.stage_source_models.values(),
    ]
    for model in models:
        surfaces.extend(s for s in (model.sql, model.sql_table) if s)
        surfaces.extend(model.filters)
        for col in model.columns:
            surfaces.extend(_col_surfaces(col))
    for ext in bundle.inline_extensions:
        for col in ext.columns or []:
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


def generate_planned_stages(
    planned_queries,
    *,
    bundle,
    dialect: str = "postgres",
    projection_aliases: "Sequence[str]" = (),
) -> str:
    """Render a multi-stage DAG (``plan_stages`` output) to one SQL string."""
    # Length-fit over-limit projection aliases from the plan-derived canonical keys, not parsed off the SQL —
    # BigQuery can't parse a backticked dotted alias.
    return _finish_statement(
        _build_planned_stages_ast(planned_queries, bundle=bundle, dialect=dialect),
        dialect=dialect,
        aliases=projection_aliases,
        exempt=_user_authored_exemptions(bundle=bundle, dialect=dialect),
    )


def _build_planned_stages_ast(planned_queries, *, bundle, dialect: str) -> exp.Select:
    """Compose a multi-stage DAG (``plan_stages`` output) as one statement AST."""
    if not planned_queries:
        raise ValueError("generate_planned_stages requires at least one stage")
    if len(planned_queries) == 1:
        return SQLGenerator(dialect=dialect)._build_from_planned(
            planned_queries[0], bundle=bundle,
        )

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
    stage_entries: List[CteEntry] = []
    root_entries: List[CteEntry] = []
    root_final: Optional[exp.Select] = None
    for planned in planned_queries:
        relation = _stage_relation(planned=planned, is_root=planned is planned_queries[-1])
        stage_bundle = _bundle_for_stage(
            planned_query=planned, bundle=bundle, schema_by_name=schema_by_name,
        )
        with generator._stage_scope(relation):
            statement = generator._build_from_planned(
                planned, bundle=stage_bundle, reuse_allocator=True,
            )
            if relation is None:
                root_entries, root_final = generator._split_root_ctes(statement)
                root_entries = _with_stage_reads(entries=root_entries, reads=planned.stage_reads)
                continue
            hoisted, body = generator._split_ast_ctes(statement)
        stage_entries.extend(_with_stage_reads(entries=hoisted, reads=planned.stage_reads))
        stage_entries.append(CteEntry(
            name=relation,
            query=build_flat_rename_wrapper(
                source_relation=planned.source_relation,
                inner=body,
                expected_columns=[c.name for c in planned.stage_schema.columns],
                dialect=dialect,
            ),
            depends_on=_merged_deps(
                [h.name for h in hoisted],
                generator._reuse_deps_of(relation),
                planned.stage_reads,
            ),
        ))

    assert root_final is not None
    return assemble_with_chain(
        entries=[*stage_entries, *root_entries], final=root_final,
        external_names=generator._external_cte_names(),
    )


def _check_stage_order(planned_queries) -> None:
    """Fail closed unless every stage's ``stage_reads`` names an earlier stage."""
    earlier: Set[str] = set()
    for planned in planned_queries:
        name = (
            planned.stage_schema.relation_name
            if planned.stage_schema is not None else "<root>"
        )
        late = [r for r in planned.stage_reads if r not in earlier]
        if late:
            raise ValueError(
                f"stage {name!r} reads sibling(s) {late!r} not planned before it; "
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
