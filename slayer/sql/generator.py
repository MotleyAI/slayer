"""SQL generator — converts a ``PlannedQuery`` to SQL via sqlglot AST."""

import logging
import re
from collections.abc import Sequence
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)

from decimal import Decimal
import sqlglot
from sqlglot import exp

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    BUILTIN_AGGREGATION_FORMULAS,
    BUILTIN_AGGREGATION_REQUIRED_PARAMS,
    DataType,
    TimeGranularity,
)
from pydantic import BaseModel, ConfigDict, field_validator

from slayer.core.errors import AggregationNotAllowedError
from slayer.core.formula import RANK_FAMILY_TRANSFORMS
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    Phase,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    column_leaf,
    column_path,
    substitute_value_keys,
)
from slayer.core.models import Aggregation
from slayer.core.refs import (
    EXPRESSION_SOURCE_KINDS as _EXPRESSION_SOURCE_KINDS,
    agg_kwarg_canonical_str,
    expression_source_leaf,
)
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration as _parse_window_duration
from slayer.engine.binding import walk_value_keys
from slayer.engine.column_expansion import (
    _is_trivial_base,
    collect_root_scope_joined_paths,
    expand_derived_refs_sync,
)
from slayer.engine.planned import RankedGrainMember
from slayer.engine.stage_planner import regroup_producer_identity
from slayer.engine.source_bundle import (
    stage_bundle_with_siblings,
    synthetic_model_from_stage_schema,
)
from slayer.sql.dialects import SqlDialect, get_dialect
from slayer.sql.naming import (
    FILTERED_ALIAS,
    OUTER_WRAP_ALIAS,
    AliasAllocator,
    canonical_aggregate_alias,
    cte_name_from_alias,
    dialect_folds_case,
    maybe_quote_ident,
    quote_mixed_case_identifiers,
    result_key,
    result_key_from_alias,
)
from slayer.sql.render.aggregates import window_agg_class
from slayer.sql.render.cte_assembly import CteEntry, assemble_with_chain
from slayer.sql.render.nodes import Node, fusion_blockers
from slayer.sql.render.joins import (
    build_grain_joinback_condition,
    grain_alias_column,
)
from slayer.sql.render.order_terms import (
    HOST_BASE_SCOPES,
    OrderEnv,
    resolve_order_term,
)
from slayer.sql.render.ranked import (
    RankedGrainProjection,
    build_rank_column,
    build_ranked_cte_select,
    build_ranked_pick,
    ranked_ordered,
)
from slayer.sql.render.aggregates import (
    DISPATCH_DISTINCT,
    DISPATCH_FORMULA,
    DISPATCH_STAT,
    is_builtin_agg,
    resolve_agg_entry,
)
from slayer.sql.render.value_expr import (
    AliasFacilities,
    CompositeFacilities,
    FilterFacilities,
    RenderContext,
    _ranked_value_cast_type,
    _wrap_cast_for_type,
    contains_aggregate,
    render_value_key,
    rewrite_log_alias,
)
from slayer.sql.render.row_expr import render_row_expression
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.scope import ScopeFrame
from slayer.sql.scope_check import maybe_validate_scopes
from slayer.sql.stage_wrapper import (
    build_flat_rename_wrapper,
    unmangle_dotted_table_refs,
)




class ResolvedAggKwarg(BaseModel):
    """DEV-1706 — a resolved parametric-aggregation kwarg value (2-kind tag)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["expr", "str"]
    value: Union[exp.Expression, str]


class AggRenderSpec(BaseModel):
    """DEV-1452 — typed input record for the dialect-aware aggregation"""

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

    filter_sql: Optional[str] = None

    time_column: Optional[str] = None

    type: Optional[DataType] = None

    column_type: Optional[DataType] = None


def _strip_declared_cast(expr: exp.Expression) -> exp.Expression:
    """Unwrap one declared-type ``CAST`` a derived-column expansion added."""
    return expr.this if isinstance(expr, exp.Cast) else expr


class _WindowedEmission(BaseModel):
    """Renderer-internal field bundle for one trailing-window emission (DEV-1838"""

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


def _windowed_emission_from_kernel(*, planned_query, kernel) -> _WindowedEmission:
    """Derive the windowed emission from a trailing-window kernel producer."""
    if (
        len(planned_query.aggregate_slots) != 1
        or any(
            r.attach_phase != "row" for r in planned_query.regroup_attach_plans
        )
        or planned_query.combined_expression_slots
        or planned_query.transform_layers
        or planned_query.outer_where_filter_ids
        or planned_query.order
        or planned_query.limit is not None
        or planned_query.offset is not None
    ):
        raise RuntimeError(
            "Trailing-window kernel producer carries structure beyond its "
            "grain + windowed aggregate; synthesis and rendering disagree "
            "(DEV-1838)."
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
            "projection (DEV-1838)."
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
        or planned_query.outer_where_filter_ids
        or planned_query.order
        or planned_query.limit is not None
        or planned_query.offset is not None
    ):
        raise RuntimeError(
            "Ranked kernel producer carries structure beyond its grain + "
            "ranked aggregate; synthesis and rendering disagree (DEV-1838)."
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
            "Ranked kernel producer's grain does not match its projection "
            "(DEV-1838)."
        )
    return _RankedEmission(
        aggregate_slot_id=agg_slot.id,
        agg=kernel.agg,
        ranking_time_key=kernel.ranking_time_key,
        grain=grain,
        where_filter_ids=[
            fp.id for fp in planned_query.filters_by_phase
            if fp.phase == Phase.ROW
        ],
    )



logger = logging.getLogger(__name__)

# corr/covar_samp/covar_pop are 2-arg (other= kwarg); MySQL has no native form, so _build_stat_agg raises there.
_TWO_ARG_STAT_AGGS: frozenset[str] = frozenset({"corr", "covar_samp", "covar_pop"})

_BUILTIN_BAREARG_AGGS_LOCAL_SLICE: frozenset[str] = BUILTIN_AGGREGATIONS

# sqlglot rewrites log10/log2 into 2-arg LOG(base,x), breaking dialects lacking 2-arg LOG; rewrite back to Anonymous.


_SQL_AND_JOINER = " AND "

_SQL_COL_SEP = ",\n    "

_SQL_WITH = "WITH "
_SQL_PARTITION_BY = "PARTITION BY "
_SQL_SELECT_HEAD = "SELECT\n  "

# Safe agg-param values: identifiers, qualified names, numeric literals.
_SAFE_AGG_PARAM_RE = re.compile(
    r'^(?:'
    r'[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*'  # identifier or qualified name
    r'|'
    r'-?\d+(?:\.\d+)?'  # numeric literal
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


def _shift_preserves_bucket_starts(bucket: "TimeGranularity", shift: str) -> bool:
    return shift.lower() in _BUCKET_ALIGNED_SHIFT_UNITS.get(str(bucket), frozenset())


def _wrap_filter(sql_str: str, filter_sql: Optional[str]) -> str:
    """Wrap ``sql_str`` in ``CASE WHEN filter_sql THEN ... END`` if a row-level"""
    if not filter_sql:
        return sql_str
    return f"(CASE WHEN {filter_sql} THEN {sql_str} END)"


def _is_host_grain(key) -> bool:
    """True for an ``AggregateKey`` marked ``grain="host"`` (DEV-1747 D2)."""
    return getattr(key, "grain", "target") == "host"


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


# --- Transform-input shape classification, shared by the hoisted validation
# gate and the consecutive_periods emitter. ---

_PREDICATE_COMPARISON_OPS = frozenset(
    {"==", "=", "!=", "<>", "<", "<=", ">", ">="}
)
_BOOL_CONNECTIVE_OPS = frozenset({"and", "or", "not"})

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


def _is_boolean_shaped(key) -> bool:
    """Whether ``key`` renders as a SQL predicate (truth value) rather than a
    numeric/text value: a comparison, BETWEEN, IN, or an ``and`` / ``or`` /
    ``not`` connective. Recursive by construction — a connective's operands are
    themselves boolean-shaped (enforced by the typing contract)."""
    if isinstance(key, ArithmeticKey):
        return (
            key.op in _PREDICATE_COMPARISON_OPS
            or key.op in _BOOL_CONNECTIVE_OPS
        )
    return isinstance(key, (BetweenKey, InKey))


def _regroup_placeholder_map(planned_query):
    """Recover the aggregate leaves the planner isolated into ``_cm_*`` producer
    CTEs. Returns ``(placeholder_key -> original aggregate key, original key ->
    producer slot)`` — a join-crossing fragment aggregation is regrouped this way
    and must re-aggregate in the shifted CTE rather than read its current value."""
    to_original: Dict[Any, Any] = {}
    to_slot: Dict[Any, Any] = {}
    for plan in planned_query.regroup_attach_plans:
        prod = plan.producer_plan
        prod_slots = {
            s.id: s
            for s in (
                list(prod.row_slots)
                + list(prod.aggregate_slots)
                + list(prod.combined_expression_slots)
            )
        }
        for sub in plan.substitutions:
            to_original[sub.placeholder] = sub.original_key
            slot = prod_slots.get(sub.producer_slot_id)
            if slot is not None:
                to_slot[sub.original_key] = slot
    return to_original, to_slot


def _composite_operand_children(node) -> list:
    """Sub-keys a composite / predicate node recurses into; ``[]`` for a leaf."""
    if isinstance(node, ArithmeticKey):
        return list(node.operands)
    if isinstance(node, ScalarCallKey):
        return list(node.args)
    if isinstance(node, BetweenKey):
        return [node.column, node.low, node.high]
    if isinstance(node, InKey):
        return [node.column]
    return []


def _classify_walk(node, *, flags, placeholder_to_original) -> None:
    """One node of the composite walk; mutates ``flags`` (transform, row_leaf,
    cross_model). AggregateKey nodes are opaque leaves; a regroup placeholder
    resolves to its original aggregate (host-grain fine, cross-model not)."""
    if isinstance(node, TransformKey):
        flags[0] = True
    elif isinstance(node, AggregateKey):
        if getattr(node.source, "path", ()):
            flags[2] = True
    elif isinstance(node, ColumnKey) and node.leaf.startswith(REGROUP_LEAF_PREFIX):
        original = placeholder_to_original.get(node)
        if original is None:
            flags[2] = True  # unknown placeholder — fail closed
        else:
            _classify_walk(
                original, flags=flags,
                placeholder_to_original=placeholder_to_original,
            )
    elif isinstance(node, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        flags[1] = True
    else:
        for child in _composite_operand_children(node):
            _classify_walk(
                child, flags=flags,
                placeholder_to_original=placeholder_to_original,
            )


def _classify_time_shift_composite(key, *, placeholder_to_original) -> Tuple[bool, bool, bool]:
    """Walk a composite ``time_shift`` input, returning
    ``(has_transform, has_row_leaf, has_cross_model_agg)``."""
    flags = [False, False, False]
    _classify_walk(key, flags=flags, placeholder_to_original=placeholder_to_original)
    return tuple(flags)  # type: ignore[return-value]


def _nested_transform_msg(op: str) -> str:
    return (
        f"Nesting a transform inside {op!r} is not supported. Compute the inner "
        f"transform in an earlier stage of a multi-stage `source_queries` model "
        f"and reference its output in this stage."
    )


def _validate_time_shift_input(*, op: str, inner, placeholder_to_original) -> None:
    """Fail closed on an unsupported ``time_shift`` input shape. Only a bare
    aggregate/column leaf or an aggregate-only composite passes; a predicate
    (IN / BETWEEN) or other non-leaf raises rather than leaking a RuntimeError."""
    if isinstance(inner, TransformKey):
        raise ValueError(_nested_transform_msg(op))
    if not isinstance(inner, (ArithmeticKey, ScalarCallKey)):
        if isinstance(inner, (AggregateKey, ColumnKey, ColumnSqlKey)):
            return  # bare aggregate → re-aggregate; column → read-and-rebucket
        raise ValueError(
            f"{op!r} does not support a {type(inner).__name__} input; only a bare "
            f"aggregate or column leaf, or an aggregate-only composite, is "
            f"supported. Compute this value in an earlier stage of a multi-stage "
            f"`source_queries` model and reference its aggregate here."
        )
    has_transform, has_row_leaf, has_cross_model = _classify_time_shift_composite(
        inner, placeholder_to_original=placeholder_to_original,
    )
    if has_transform:
        raise ValueError(_nested_transform_msg(op))
    if has_row_leaf:
        raise ValueError(
            f"{op!r} does not support a row-level (non-aggregate) leaf inside a "
            f"composite input; every leaf must be an aggregate. Compute the "
            f"row-level value in an earlier stage of a multi-stage "
            f"`source_queries` model and reference its aggregate here."
        )
    if has_cross_model:
        raise ValueError(
            f"{op!r} does not support a cross-model aggregate leaf inside a "
            f"composite input; compute it in an earlier stage of a multi-stage "
            f"`source_queries` model and reference its output here."
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
            f"comparison, BETWEEN, IN, or another connective); got "
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
        op=op, key=key, expect=expect, node_is_bool=_is_boolean_shaped(key),
    )
    if isinstance(key, ArithmeticKey):
        child_expect = "bool" if key.op in _BOOL_CONNECTIVE_OPS else "value"
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


_GRANULARITY_MAP = {
    TimeGranularity.SECOND: "second",
    TimeGranularity.MINUTE: "minute",
    TimeGranularity.HOUR: "hour",
    TimeGranularity.DAY: "day",
    TimeGranularity.WEEK: "week",
    TimeGranularity.MONTH: "month",
    TimeGranularity.QUARTER: "quarter",
    TimeGranularity.YEAR: "year",
}






def _effective_src_filters(*, planned_query, plan) -> list:
    """``planned_query.filters_by_phase`` as the windowed ``_src`` scope sees it"""
    rewrites = getattr(plan, "src_filter_rewrites", None)
    if not rewrites:
        return planned_query.filters_by_phase
    by_id = {r.filter_id: r.expression for r in rewrites}
    return [
        fp if fp.id not in by_id
        else fp.model_copy(update={"expression": by_id[fp.id]})
        for fp in planned_query.filters_by_phase
    ]










_TRAILING_OFFSET_RE = re.compile(r"(?is)\s*OFFSET\s+\d+\s*\Z")
_TRAILING_LIMIT_OFFSET_RE = re.compile(
    r"(?is)\s*LIMIT\s+\d+\s+OFFSET\s+\d+\s*\Z"
)
_TRAILING_LIMIT_RE = re.compile(r"(?is)\s*LIMIT\s+\d+\s*\Z")

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


class RenderState(BaseModel):
    """Frozen per-render constants threaded through the transform-chain emitters"""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    planned_query: Any
    bundle: Any
    regroup_env: Any = None
    regroup_join_specs: Any = None


class ChainState(BaseModel):
    """Per-chain-layer accumulators + the layer's source root (DEV-1817)."""

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

    def __init__(self, dialect: "str | SqlDialect" = "postgres"):
        if isinstance(dialect, SqlDialect):
            self._dialect: SqlDialect = dialect
        else:
            self._dialect = get_dialect(dialect)
        self._gen_allocator: Optional[AliasAllocator] = None
        self._gen_rendered_producers: Optional[
            Dict[Any, Tuple[str, Dict[str, str]]]
        ] = None
        self._gen_split_consumers: List[str] = []
        self._gen_reuse_deps: Dict[str, Set[str]] = {}

    def install_generation(self, *, reserve: "Iterable[str]" = ()) -> None:
        """Open one generation scope spanning SEVERAL ``reuse_allocator=True``"""
        allocator = self._new_allocator()
        allocator.reserve(*reserve)
        self._gen_allocator = allocator
        self._gen_rendered_producers = {}
        self._gen_split_consumers = []
        self._gen_reuse_deps = {}

    @property
    def dialect(self) -> str:
        """The sqlglot dialect name. Read-only — derived from"""
        return self._dialect.sqlglot_name

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

    def _alias_render_ctx(self, *, slot_id_by_key, available_alias_by_slot_id):
        """RenderContext carrying only the plain slot-alias facilities."""
        return RenderContext(
            dialect=self._dialect,
            aliases=AliasFacilities(
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            ),
        )

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
    def _maybe_quote_ident(ident: Optional[exp.Expression]) -> None:
        """Thin delegator to :func:`slayer.sql.naming.maybe_quote_ident`"""
        maybe_quote_ident(ident)

    @staticmethod
    def _quote_mixed_case_identifiers(node: exp.Expression) -> exp.Expression:
        """Thin delegator to"""
        return quote_mixed_case_identifiers(node)

    def _to_ident(self, name: str) -> exp.Identifier:
        """Build a column/table-name identifier, quoting it when mixed-case"""
        ident = exp.to_identifier(name)
        self._maybe_quote_ident(ident)
        return ident

    def _to_table(self, name: str, alias: Optional[str] = None) -> exp.Expression:
        """Build a (possibly schema-qualified) table reference with mixed-case"""
        table = exp.to_table(name).transform(self._quote_mixed_case_identifiers)
        if alias is not None:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
        return table

    def _parse(self, sql: str, *, dialect: Optional[str] = None) -> exp.Expression:
        """Parse ``sql`` via sqlglot, applying SLayer-specific AST rewrites."""
        d = dialect or self.dialect
        active = self._dialect if d == self.dialect else get_dialect(d)
        sql = prequote_reserved_identifiers(sql, dialect=d)
        tree = sqlglot.parse_one(sql, dialect=d)
        tree = active.rewrite_parsed_ast(tree)
        tree = tree.transform(self._rewrite_log_aliases)
        tree = tree.transform(self._quote_mixed_case_identifiers)
        return self._dialect.rewrite_target_ast(tree)

    def _parse_predicate(self, sql: str, *, dialect: Optional[str] = None) -> exp.Expression:
        """Parse a bare WHERE/HAVING predicate expression (DEV-1378)."""
        d = dialect or self.dialect
        active = self._dialect if d == self.dialect else get_dialect(d)
        sql = prequote_reserved_identifiers(sql, dialect=d)
        wrapped = sqlglot.parse_one(f"SELECT 1 WHERE {sql}", dialect=d)
        where = wrapped.args.get("where")
        if where is None or where.this is None:  # pragma: no cover — defensive
            raise ValueError(
                f"Could not extract WHERE predicate from {sql!r} (dialect={d!r})"
            )
        tree = active.rewrite_parsed_ast(where.this)
        tree = tree.transform(self._rewrite_log_aliases)
        tree = tree.transform(self._quote_mixed_case_identifiers)
        return self._dialect.rewrite_target_ast(tree)




    def _quote_ident(self, name: str) -> str:
        """Render ``name`` as ONE dialect-quoted identifier string (DEV-1716)."""
        return exp.to_identifier(name, quoted=True).sql(dialect=self.dialect)

    def _parse_cte_body(self, sql: str) -> exp.Expression:
        """Parse a rendered CTE body back into AST for the WITH assembler."""
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        unmangle_dotted_table_refs(parsed)
        return parsed

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
        self, order_col: exp.Expression, *, ascending: bool,
        nulls: str = "default",
    ) -> exp.Ordered:
        """Build an ``exp.Ordered`` node via the dialect strategy."""
        return self._dialect.build_ordered(
            order_col, descending=not ascending, nulls=nulls,
        )





    def _build_time_offset_expr(self, col_expr: exp.Expression, offset: int,
                                granularity: str) -> exp.Expression:
        """Apply a time offset to a column expression (dialect-aware)."""
        return self._dialect.build_time_offset_expr(
            col_expr=col_expr, offset=offset, granularity=granularity,
        )

    def _duration_interval_exprs(self, duration: str, sign: int = 1) -> list[exp.Expression]:
        """Return per-unit AST nodes that `_add_intervals_expr` will chain."""
        parts = _parse_window_duration(duration)
        return self._dialect.duration_interval_exprs(parts=parts, sign=sign)

    def _granularity_interval_expr(self, granularity: TimeGranularity, sign: int = 1) -> list[exp.Expression]:
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

    def _add_intervals_expr(self, expr: exp.Expression, intervals: list[exp.Expression],
                            sign: int = 1) -> exp.Expression:
        """Compose `expr ± interval [± interval ...]` as AST."""
        return self._dialect.add_intervals_expr(
            expr=expr, intervals=intervals, sign=sign,
        )

    def _build_date_trunc(self, col_expr: exp.Expression, granularity: TimeGranularity) -> exp.Expression:
        """Build a DATE_TRUNC expression. Dispatches to the dialect strategy"""
        return self._dialect.build_date_trunc(
            col_expr=col_expr, granularity=granularity, parse=self._parse,
        )

    def _rewrite_log_aliases(self, node: exp.Expression) -> exp.Expression:
        """Thin delegator to the shared log-alias policy in"""
        return rewrite_log_alias(node, dialect=self._dialect)

    def _resolve_sql(
        self,
        sql: Optional[str],
        name: str,
        model_name: str,
        type: Optional[DataType] = None,
    ) -> exp.Expression:
        """Resolve an enriched SQL expression to a sqlglot AST node."""
        if sql is None:
            return exp.Column(this=self._to_ident(name), table=exp.to_identifier(model_name))
        if sql.isidentifier():
            return exp.Column(this=self._to_ident(sql), table=exp.to_identifier(model_name))
        return _wrap_cast_for_type(self._parse(sql), type)

    def _resolve_value_sql(self, spec: AggRenderSpec) -> str:
        """Resolve ``spec.sql`` (or ``spec.name``) into a fully-qualified"""
        return self._resolve_sql(
            sql=spec.sql,
            name=spec.name,
            model_name=spec.model_name,
            type=spec.column_type,
        ).sql(dialect=self.dialect)

    def _agg_param_ast(
        self, value: "ResolvedAggKwarg | str", *, model_name: str,
    ) -> exp.Expression:
        """Resolve a parametric-agg param value to a sqlglot AST."""
        if isinstance(value, ResolvedAggKwarg):
            if value.kind == "expr":
                # Return a copy: sqlglot re-parents a node on attach, so sharing one kwarg AST across trees corrupts the
                # first.
                return value.value.copy() if isinstance(value.value, exp.Expression) \
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
    ) -> str:
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
        return self._agg_param_ast(
            value, model_name=spec.model_name,
        ).sql(dialect=self.dialect)

    def _build_agg(
        self,
        spec: "AggRenderSpec | None" = None,
    ) -> tuple[exp.Expression, bool]:
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

        if not is_builtin_agg(agg_name):
            return self._build_formula_agg(spec, agg_name), True

        entry = resolve_agg_entry(agg_name)
        dispatch = entry.dispatch

        # These builders resolve+filter-wrap their own inner and must run before the plain inner resolution (a
        # join-discovery side effect).
        if dispatch == DISPATCH_STAT:
            return self._build_stat_agg(spec), True
        if dispatch == DISPATCH_FORMULA:
            return self._build_formula_agg(spec, agg_name), True
        if agg_name == "percentile":
            return self._build_percentile(spec), True
        if agg_name == "count_distinct_approx":
            col_expr = _wrap_filter(
                self._resolve_value_sql(spec), spec.filter_sql
            )
            return self._dialect.build_approx_count_distinct(
                col_sql=col_expr, parse=self._parse
            ), True

        if agg_name == "count" and spec.sql is None:
            if spec.filter_sql:
                case_sql = f"CASE WHEN {spec.filter_sql} THEN 1 END"
                inner = self._parse(case_sql)
            else:
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

        if spec.filter_sql and not (agg_name == "count" and spec.sql is None):
            inner_sql = inner.sql(dialect=self.dialect)
            case_sql = f"CASE WHEN {spec.filter_sql} THEN {inner_sql} END"
            inner = self._parse(case_sql)

        if dispatch == DISPATCH_DISTINCT:
            return exp.Count(this=exp.Distinct(expressions=[inner])), True

        if agg_name == "median":
            return self._build_median(inner), True

        return entry.node_class(this=inner), True

    def _build_formula_agg(self, spec: AggRenderSpec, agg_name: str) -> exp.Expression:  # NOSONAR(S3776) — sequential dispatch over formula source (aggregation_def vs built-in) and per-kind ResolvedAggKwarg substitution (DEV-1527); one cohesive template-substitution contract.
        """Build SQL for formula-based aggregations (weighted_avg, custom)."""
        formula = None
        if spec.aggregation_def and spec.aggregation_def.formula:
            formula = spec.aggregation_def.formula
        elif agg_name in BUILTIN_AGGREGATION_FORMULAS:
            formula = BUILTIN_AGGREGATION_FORMULAS[agg_name]

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

        required = BUILTIN_AGGREGATION_REQUIRED_PARAMS.get(agg_name, [])
        for req in required:
            if req not in params:
                raise ValueError(
                    f"Aggregation '{agg_name}' requires parameter '{req}'. "
                    f"Set it in the model's aggregation definition or at query time "
                    f"(e.g., 'measure:{agg_name}({req}=column)')."
                )

        # Filter-wrap column refs in CASE WHEN so non-matching rows go NULL, but leave literal-default params unwrapped
        # (wrapping a constant makes it a row expression).
        col_expr = _wrap_filter(self._resolve_value_sql(spec), spec.filter_sql)
        substituted = formula.replace("{value}", col_expr)
        for param_name, param_val in params.items():
            param_ast = self._agg_param_ast(
                param_val, model_name=spec.model_name,
            )
            param_expr = param_ast.sql(dialect=self.dialect)
            if spec.filter_sql and not isinstance(param_ast, exp.Literal):
                param_expr = _wrap_filter(param_expr, spec.filter_sql)
            substituted = substituted.replace(f"{{{param_name}}}", param_expr)

        return self._parse(substituted)

    def _build_median(self, inner: exp.Expression) -> exp.Expression:
        """Build a median aggregation expression. Dispatches to the dialect"""
        return self._dialect.build_median(inner=inner, parse=self._parse)

    def _build_percentile(self, spec: AggRenderSpec) -> exp.Expression:
        """Build a PERCENTILE_CONT(p) aggregation expression (dialect-dependent)."""
        p = self._resolve_agg_param(spec, name="p", agg_name="percentile")
        # p must be a numeric literal in [0,1]; guards a column-ref or function default from reaching PERCENTILE_CONT's
        # direct-arg slot.
        try:
            p_float = float(p)
        except ValueError:
            raise ValueError(
                f"Aggregation 'percentile' parameter 'p' must be a numeric literal "
                f"in [0, 1]; got {p!r}."
            ) from None
        if not 0.0 <= p_float <= 1.0:
            raise ValueError(
                f"Aggregation 'percentile' parameter 'p' must be in [0, 1]; got {p_float}."
            )

        # Pass the original string p (not the float) so user literals like 0.50 / 5e-2 survive verbatim.
        col_expr = _wrap_filter(self._resolve_value_sql(spec), spec.filter_sql)
        return self._dialect.build_percentile(
            p_str=p, col_sql=col_expr, parse=self._parse,
        )

    def _build_stat_agg(self, spec: AggRenderSpec) -> exp.Expression:
        """Build SQL for the statistical aggregations added in DEV-1317."""
        agg_name = spec.aggregation

        # Resolve other= before any dialect guard so a missing-required-param error outranks the dialect-unsupported
        # one.
        other_expr: Optional[str] = None
        if agg_name in _TWO_ARG_STAT_AGGS:
            other_expr = _wrap_filter(
                self._resolve_agg_param(spec, name="other", agg_name=agg_name),
                spec.filter_sql,
            )

        col_expr = _wrap_filter(self._resolve_value_sql(spec), spec.filter_sql)

        if agg_name in _TWO_ARG_STAT_AGGS:
            assert other_expr is not None  # set above when two-arg
            return self._dialect.build_covar_2arg(
                agg_name=agg_name,
                col_sql=col_expr,
                other_sql=other_expr,
                parse=self._parse,
            )
        return self._dialect.build_stat_agg_1arg(
            agg_name=agg_name, col_expr=col_expr, parse=self._parse,
        )




    def generate_from_planned(
        self, planned_query, *, bundle, as_cte_body: bool = False,
        reuse_allocator: bool = False, as_ast: bool = False,
        producer_kernel=None,
    ):
        """Render a typed ``PlannedQuery`` to SQL (public entry)."""
        self._assert_projection_is_public(planned_query)
        if reuse_allocator and self._gen_allocator is not None:
            result = self._generate_from_planned_impl(
                planned_query, bundle=bundle, as_cte_body=as_cte_body,
                as_ast=as_ast, producer_kernel=producer_kernel,
            )
        else:
            prev_allocator = getattr(self, "_gen_allocator", None)
            prev_rendered = getattr(self, "_gen_rendered_producers", None)
            prev_split_consumers = self._gen_split_consumers
            prev_reuse_deps = self._gen_reuse_deps
            self._gen_allocator = self._new_allocator()
            self._gen_rendered_producers = {}
            self._gen_split_consumers = []
            self._gen_reuse_deps = {}
            try:
                result = self._generate_from_planned_impl(
                    planned_query, bundle=bundle, as_cte_body=as_cte_body,
                    as_ast=as_ast, producer_kernel=producer_kernel,
                )
            finally:
                self._gen_allocator = prev_allocator
                self._gen_rendered_producers = prev_rendered
                self._gen_split_consumers = prev_split_consumers
                self._gen_reuse_deps = prev_reuse_deps
        # Hoist consumes the producer AST, not re-parsed SQL text (a round-trip mis-binds a dotted result-key column on
        # BigQuery / T-SQL).
        if as_ast and not isinstance(result, exp.Expression):
            result = sqlglot.parse_one(result, dialect=self.dialect)
            unmangle_dotted_table_refs(result)
        return result

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
        as_ast: bool = False,
        producer_kernel=None,
    ):
        """Render a typed ``PlannedQuery`` to SQL."""

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

        slot_id_by_key: Dict[Any, str] = {
            s.key: s.id for s in slots_by_id.values()
        }

        public_proj_set: Set[str] = set(planned_query.projection)
        # aggregates_only pulls only AggregateKey leaves from order/filter walks; a hidden ROW order target would
        # otherwise land in GROUP BY and change grain.
        no_transform = not bool(planned_query.transform_layers)
        extra_materialize_ids = self._collect_base_aux_slot_ids(
            planned_query=planned_query,
            slot_id_by_key=slot_id_by_key,
            slots_by_id=slots_by_id,
            include_order=True,
            aggregates_only=no_transform,
        )
        base_render_order = list(planned_query.projection) + [
            sid for sid in extra_materialize_ids if sid not in public_proj_set
        ]

        regroup_ctes, regroup_env, regroup_join_specs, _reused = (
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
                final_select = assemble_with_chain(
                    entries=regroup_ctes, final=final_select,
                    external_names=self._external_cte_names(),
                )
            return final_select if as_ast else final_select.sql(
                dialect=self.dialect, pretty=True,
            )

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
        )


    def _run_transform_chain(
        self,
        *,
        chain: ChainState,
        render: RenderState,
        chain_tail: str,
    ) -> str:
        """The one Kahn driver for the transform-step phase (D7)."""
        planned_query = render.planned_query
        if any(
            layer.op == "time_shift" for layer in planned_query.transform_layers
        ):
            shifted_where_parts, shifted_where_join_paths = (
                self._build_shifted_cte_where_parts(
                    planned_query=planned_query,
                    source_relation=chain.source_relation,
                    source_model=chain.source_model,
                    bundle=render.bundle,
                    regroup_env=render.regroup_env,
                )
            )
        else:
            shifted_where_parts, shifted_where_join_paths = [], []

        pending_layers = list(planned_query.transform_layers)
        step_num = 0
        while pending_layers:
            (ready_window, ready_time_shift, ready_cp, not_ready) = (
                self._classify_ready_transform_layers(
                    pending_layers=pending_layers,
                    slots_by_id=chain.slots_by_id,
                    slot_id_by_key=chain.slot_id_by_key,
                    available_alias_by_slot_id=chain.available_alias_by_slot_id,
                )
            )
            if not (ready_window or ready_time_shift or ready_cp):
                pending_ops = [layer.op for layer in pending_layers]
                raise RuntimeError(
                    f"transform layer dependencies could not be resolved; "
                    f"pending ops: {pending_ops!r}.",
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
                shifted_where_parts=shifted_where_parts,
                shifted_where_join_paths=shifted_where_join_paths,
                chain_tail=chain_tail,
            )
            chain_tail = self._emit_cp_layers(
                ready_cp=ready_cp,
                chain=chain,
                render=render,
                chain_tail=chain_tail,
            )
            pending_layers = not_ready

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
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
        reserve_bare_aliases: bool = False,
    ) -> str:
        """Steps + post phases over a built relation tail (D1) — shared by the"""
        base_node = Node(
            name="base",
            phase=tail_phase,
            query=tail_select,
            depends_on=[e.name for e in prelude_nodes],
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

    def _classify_ready_transform_layers(
        self,
        *,
        pending_layers,
        slots_by_id,
        slot_id_by_key,
        available_alias_by_slot_id,
    ) -> tuple:
        """Kahn split of ``pending_layers`` into"""
        ready_window: list = []
        ready_time_shift: list = []
        ready_cp: list = []
        not_ready: list = []
        for layer in pending_layers:
            if not self._transform_layer_deps_ready(
                layer=layer,
                slots_by_id=slots_by_id,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            ):
                not_ready.append(layer)
            elif layer.op == "time_shift":
                ready_time_shift.append(layer)
            elif layer.op == "consecutive_periods":
                ready_cp.append(layer)
            else:
                ready_window.append(layer)
        return ready_window, ready_time_shift, ready_cp, not_ready

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
        render: Callable[[Any], exp.Expression],
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
                rendered = _wrap_cast_for_type(expr=rendered, dt=slot.cast_type)
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
        """Projected POST-phase Arithmetic / ScalarCall slots no transform"""
        unmaterialised: List[Any] = []
        for cslot in planned_query.combined_expression_slots:
            if isinstance(cslot.key, TransformKey):
                continue
            if cslot.id in aliases_by_slot_id:
                continue
            if isinstance(cslot.key, (ArithmeticKey, ScalarCallKey)):
                unmaterialised.append(cslot)
        return unmaterialised

    def _inner_select_from_final_cte(
        self, *, chain_tail: str, aliases_by_slot_id: Dict[str, List[str]],
    ) -> exp.Select:
        """Inner SELECT over the final chain CTE: all carried aliases in PLAN"""
        inner_aliases = self._carry_aliases_in_plan_order(aliases_by_slot_id)
        return exp.Select().select(
            *(exp.column(a, quoted=True) for a in inner_aliases),
        ).from_(chain_tail)

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
        shifted_where_parts,
        shifted_where_join_paths,
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
                    shifted_where_parts=shifted_where_parts,
                    shifted_where_join_paths=shifted_where_join_paths,
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
    ) -> str:
        """Assemble the ``WITH`` chain, apply the POST-phase filter wrap, and emit"""
        inner_select = self._inner_select_from_final_cte(
            chain_tail=chain_tail, aliases_by_slot_id=aliases_by_slot_id,
        )
        chain_sql = assemble_with_chain(
            entries=ctes, final=inner_select,
            external_names=self._external_cte_names(),
        ).sql(dialect=self.dialect, pretty=True)

        post_filter_conditions = self._render_post_phase_filter_conditions(
            planned_query=planned_query,
            slot_id_by_key=slot_id_by_key,
            available_alias_by_slot_id=available_alias_by_slot_id,
        )
        if post_filter_conditions:
            chain_sql = (
                f"SELECT *\nFROM (\n{chain_sql}\n) AS {FILTERED_ALIAS}"
                f"\nWHERE {_SQL_AND_JOINER.join(post_filter_conditions)}"
            )

        public_aliases_user_order = _cycle_public_aliases_in_projection_order(
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            aliases_by_slot_id=aliases_by_slot_id,
        )
        return self._emit_planned_outer_wrap(
            chain_sql=chain_sql,
            public_aliases=public_aliases_user_order,
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            available_alias_by_slot_id=available_alias_by_slot_id,
        )


    @staticmethod
    def _validate_transform_input_shapes(*, planned_query) -> None:
        """Reject unsupported ``time_shift`` / ``consecutive_periods`` input
        shapes uniformly, before any render path (plain, combined-attaches,
        kernel body) branches. The message names the transform, the offending
        shape, and the multi-stage ``source_queries`` remedy."""
        slots_map = {
            s.id: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }
        placeholder_to_original, _ = _regroup_placeholder_map(planned_query)
        for layer in planned_query.transform_layers:
            if layer.op not in ("time_shift", "consecutive_periods"):
                continue
            for sid in layer.slot_ids:
                slot = slots_map.get(sid)
                if slot is None or not isinstance(slot.key, TransformKey):
                    continue
                inner = slot.key.input
                if layer.op == "time_shift":
                    _validate_time_shift_input(
                        op=layer.op, inner=inner,
                        placeholder_to_original=placeholder_to_original,
                    )
                else:
                    _validate_consecutive_periods_input(op=layer.op, inner=inner)

    @staticmethod
    def _composite_has_remote_operand(
        *,
        key,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        planned_query,
    ) -> bool:
        """Whether any operand of ``key`` is materialised OUTSIDE the base CTE."""

        remote_slot_ids: Set[str] = set()
        for node in walk_value_keys(key):
            if not isinstance(node, AggregateKey):
                continue
            if getattr(node.source, "path", ()):
                return True  # cross-model source, even without a plan yet
            sid = slot_id_by_key.get(node)
            if sid is not None and sid in remote_slot_ids:
                return True
        return False

    @staticmethod
    def _collect_base_aux_slot_ids(  # NOSONAR(S3776) — recursive ValueKey walker (nested ``_collect_from``) over the closed key union plus three top-level passes (transform layers / phase-gated filter deps / order deps). Each pass is one decision; extracting them would scatter the slot-dep contract.
        *,
        planned_query,
        slot_id_by_key: Dict[Any, str],
        slots_by_id: Dict[str, Any],
        include_order: bool = True,
        aggregates_only: bool = False,
    ) -> List[str]:
        """Return slot ids the base CTE must project beyond the public"""

        if aggregates_only:
            base_kinds: Tuple[type, ...] = (AggregateKey,)
        else:
            base_kinds = (ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey)
        # Insertion-ordered dedup, not a set: a set would surface same-grain inner aggregates in hash-seed order, making
        # emitted SQL non-deterministic.
        out: List[str] = []
        seen: Set[str] = set()

        def _add(sid: str) -> None:
            if sid not in seen:
                seen.add(sid)
                out.append(sid)

        def _collect_from(key) -> None:
            if isinstance(key, base_kinds):
                sid = slot_id_by_key.get(key)
                if sid is not None:
                    _add(sid)
                return
            if aggregates_only and isinstance(
                key, (ColumnKey, ColumnSqlKey, TimeTruncKey),
            ):
                return
            if isinstance(key, TransformKey):
                _collect_from(key.input)
                for p in key.partition_keys:
                    _collect_from(p)
                if key.time_key is not None:
                    _collect_from(key.time_key)
                return
            if isinstance(key, ArithmeticKey):
                for o in key.operands:
                    _collect_from(o)
                return
            if isinstance(key, ScalarCallKey):
                for a in key.args:
                    if isinstance(
                        a,
                        (
                            TransformKey, ArithmeticKey, ScalarCallKey,
                            BetweenKey, InKey, ColumnKey,
                            ColumnSqlKey, TimeTruncKey, AggregateKey,
                        ),
                    ):
                        _collect_from(a)
                return
            if isinstance(key, BetweenKey):
                _collect_from(key.column)
                _collect_from(key.low)
                _collect_from(key.high)
                return
            if isinstance(key, InKey):
                _collect_from(key.column)
                return

        for layer in planned_query.transform_layers:
            for slot_id in layer.slot_ids:
                slot = slots_by_id.get(slot_id)
                if slot is None:
                    continue
                key = slot.key
                if isinstance(key, TransformKey):
                    _collect_from(key.input)
                    for p in key.partition_keys:
                        _collect_from(p)
                    if key.time_key is not None:
                        _collect_from(key.time_key)

        # Walk AGGREGATE (HAVING) and POST filter deps; POST is gated on transforms present, else its operands
        # materialise without the filter applying.
        has_transforms = bool(planned_query.transform_layers)
        for fp in planned_query.filters_by_phase:
            if fp.phase == Phase.AGGREGATE:
                pass  # walk
            elif fp.phase == Phase.POST and has_transforms:
                pass  # walk
            else:
                continue
            if fp.expression is not None:
                _collect_from(fp.expression.value_key)

        if include_order:
            for oe in planned_query.order:
                slot = slots_by_id.get(oe.slot_id)
                if slot is None:
                    continue
                _collect_from(slot.key)
                # An order-only composite needs its own materialised column; cross-model/windowed composites are
                # excluded (their operands live in _cm_/_wm_ CTEs).
                if isinstance(slot.key, (ArithmeticKey, ScalarCallKey)):
                    if not SQLGenerator._composite_has_remote_operand(
                        key=slot.key, slots_by_id=slots_by_id,
                        slot_id_by_key=slot_id_by_key,
                        planned_query=planned_query,
                    ):
                        _add(oe.slot_id)

        return out

    @staticmethod
    def _transform_layer_deps_ready(
        *,
        layer,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
    ) -> bool:
        """A layer is ready when every slot-worthy dep its TransformKeys"""

        slotted_kinds = (
            ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey, TransformKey,
        )

        def _ready(key) -> bool:
            if isinstance(key, slotted_kinds):
                sid = slot_id_by_key.get(key)
                if sid is None:
                    return True
                return sid in available_alias_by_slot_id
            if isinstance(key, ArithmeticKey):
                return all(_ready(o) for o in key.operands)
            if isinstance(key, ScalarCallKey):
                for a in key.args:
                    if isinstance(
                        a,
                        (
                            TransformKey, ArithmeticKey, ScalarCallKey,
                            BetweenKey, InKey, ColumnKey, ColumnSqlKey,
                            TimeTruncKey, AggregateKey,
                        ),
                    ) and not _ready(a):
                        return False
                return True
            if isinstance(key, BetweenKey):
                return all(
                    _ready(k) for k in (key.column, key.low, key.high)
                )
            if isinstance(key, InKey):
                return _ready(key.column)
            return True

        for slot_id in layer.slot_ids:
            slot = slots_by_id.get(slot_id)
            if slot is None or not isinstance(slot.key, TransformKey):
                continue
            tk = slot.key
            if not _ready(tk.input):
                return False
            for p in tk.partition_keys:
                if not _ready(p):
                    return False
            if tk.time_key is not None and not _ready(tk.time_key):
                return False
        return True

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
                if not getattr(key.source, "path", ()) or (
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

        def _resolve_column_filter(key) -> None:
            cfk = key.column_filter_key
            if cfk is None or not cfk.canonical_sql:
                return
            self._enter_mode_a_predicate(
                sql=cfk.canonical_sql, scope=scope,
                location=f"Column.filter on model {scope.root_model.name!r}",
            )

        def _resolve_source(key) -> None:
            # Expression sources (DEV-1826) resolve too: a ColumnSqlKey operand
            # whose derived SQL crosses joins must register them (Law 1).
            if isinstance(
                key.source, (ColumnSqlKey, *_EXPRESSION_SOURCE_KINDS),
            ) or getattr(key.source, "path", ()):
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
            # Column.filter; keep the resolved (alias-rewritten) fragment.
            frags = self._register_fragment_kwarg_joins(
                key=key, scope=scope, model=scope.root_model,
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

        _for_each_local_agg(_resolve_column_filter)
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
    ) -> Dict[str, exp.Expression]:
        """Render ROW-phase computed (expression) dimensions through the HOST"""

        out: Dict[str, exp.Expression] = {}
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
    ) -> List[Tuple[str, Optional[exp.Expression]]]:
        """One ``(cte_name, condition)`` per regroup producer. Each host"""
        out: List[Tuple[str, Optional[exp.Expression]]] = []
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
        self, *, key, source_model, source_relation: str, bundle,
    ) -> "Optional[Dict[str, ResolvedAggKwarg]]":
        """Resolve a single LOCAL aggregate's column-ref kwargs"""

        kwargs = getattr(key, "kwargs", None)
        if bundle is None or not kwargs:
            return None
        scope = self._throwaway_frame(
            model=source_model, relation=source_relation, bundle=bundle,
        )
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
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
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

        select_columns: list[exp.Expression] = []
        group_by_keys: Dict[str, exp.Expression] = {}
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
                        f"Use colon syntax (e.g., '{bare}:sum', '{bare}:avg'). "
                        f"For COUNT(*), use '*:count'."
                    )
                else:
                    raise NotImplementedError(
                        f"DEV-1450 stage 7b.10+: row-phase key type "
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
                                ),
                            ),
                        ),
                    )
                    if contains_aggregate(key):
                        composite = _wrap_cast_for_type(composite, slot.cast_type)
                        has_aggregation = True
                    select_columns.append(composite.copy().as_(full_alias))
                    _record_alias(sid, full_alias)
                    continue
                agg_path = getattr(key.source, "path", ())
                if agg_path:
                    if skip_cross_model_aggs:
                        continue
                    if not _is_host_grain(key):
                        raise NotImplementedError(
                            f"DEV-1450 stage 7b.12: cross-model aggregate "
                            f"(source.path={agg_path!r}) reached the local "
                            f"base SELECT path. The cross-model orchestrator "
                            f"should have routed this through `_render_with_"
                            f"cross_model_plans`."
                        )
                synth = self._build_agg_render_spec_from_planned(
                    slot=slot,
                    key=key,
                    source_model=source_model,
                    source_relation=source_relation,
                    full_alias=full_alias,
                    bundle=bundle,
                    resolved_agg_kwargs=resolved_agg_kwargs.get(key),
                )
                agg_expr, is_agg = self._build_agg(synth)
                if is_agg:
                    agg_expr = _wrap_cast_for_type(agg_expr, slot.cast_type)
                    has_aggregation = True
                select_columns.append(agg_expr.copy().as_(full_alias))
                _record_alias(sid, full_alias)
            else:
                continue

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
        resolved_agg_kwargs,
    ):
        """The AGGREGATE-phase composite seam (DEV-1763 P-G): render one"""

        def build(agg_key) -> exp.Expression:
            if getattr(agg_key.source, "path", ()):
                raise NotImplementedError(
                    "DEV-1450: cross-model aggregate operand inside an "
                    "AGGREGATE-phase composite is not yet supported; factor it "
                    "into a multi-stage source_queries model."
                )
            synth = self._build_agg_render_spec_from_planned(
                slot=slot, key=agg_key, source_model=source_model,
                source_relation=source_relation, full_alias="__op__",
                bundle=bundle,
                resolved_agg_kwargs=(resolved_agg_kwargs or {}).get(agg_key),
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
        base_relation: Optional[exp.Expression] = None,
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
    ) -> Tuple[exp.Select, List[str]]:
        """Render one duration-windowed-measure CTE (DEV-1714 Stage 10)."""

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

        src_cols: List[exp.Expression] = []
        grain_pairs: List[Tuple[exp.Expression, exp.Expression]] = []
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

        val_expr = src_scope.resolve(key.source)
        if key.column_filter_key is not None:
            pred_sql = src_scope.resolve_predicate_sql(
                key.column_filter_key.canonical_sql,
            )
            val_expr = exp.Case(
                ifs=[exp.If(this=self._parse_predicate(pred_sql), true=val_expr)],
            )
        src_cols.append(val_expr.as_("_w_value"))

        # _src inherits row filters minus their frame bounds; one effective list feeds both join discovery and rendering
        # so they can't disagree.
        all_filter_ids = {fp.id for fp in planned_query.filters_by_phase}
        skip_for_src = all_filter_ids - set(plan.where_filter_ids)
        src_filters = _effective_src_filters(planned_query=planned_query, plan=plan)
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
        src_w_time = _src_col("_w_time")
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

        # Registry lookup, not a catch-all: the old 'Sum if sum else Avg' rendered every other agg as AVG; raise
        # instead.
        agg_cls = window_agg_class(plan.agg)
        agg_expr = _wrap_cast_for_type(
            agg_cls(this=_src_col("_w_value")), agg_slot.cast_type,
        )

        outer = exp.Select()
        for ga in grain_aliases:
            outer = outer.select(_base_col(ga))
        outer = outer.select(agg_expr.as_(exp.to_identifier(full_agg_alias, quoted=True)))
        if base_relation is not None:
            outer = outer.from_(base_relation)
        else:
            outer = outer.from_(exp.Table(this=exp.to_identifier("_base")))
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
    ) -> exp.Expression:
        """One value expression anchored in a ranked CTE's own scope."""

        def _register(expr: exp.Expression, path: Tuple[str, ...]) -> None:
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
    ) -> exp.Expression:
        """The value a ranked aggregate picks, anchored in its own scope."""

        source = key.source
        if isinstance(source, StarKey):
            raise ValueError(
                f"Aggregation {key.agg!r} not allowed with measure "
                f"'*' — use '*:count' for COUNT(*)."
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
        if isinstance(source, ColumnSqlKey) and col.sql is not None:
            sql_text = self._expand_derived_column_sql(
                source_model=root_model, source_relation=root_relation,
                column_name=col.name, bundle=bundle,
            )
        else:
            sql_text = col.sql if col.sql else col.name
        expr = self._resolve_sql(
            sql=sql_text, name=col.name, model_name=root_relation,
            type=col.type,
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
        plan,
        agg_slot,
        bundle,
        planned_query,
        slots_by_id: Dict[str, Any],
        host_source_model,
        host_source_relation: str,
        full_agg_alias: str,
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
    ) -> Tuple[exp.Select, List[str]]:
        """Render one ranked (``first`` / ``last``) CTE (DEV-1748, B9)."""

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
        partition_by: List[exp.Expression] = []
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
            plan=plan, local_key=local_key, planned_query=planned_query,
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
            build_ranked_pick(value_ref=value_ref),
            _ranked_value_cast_type(agg_slot.cast_type),
        )
        return build_ranked_cte_select(
            inner=inner, grain=grain, pick=pick, agg_alias=full_agg_alias,
        )

    def _ranked_cte_where(
        self,
        *,
        plan,
        local_key,
        planned_query,
        bundle,
        root_model,
        root_relation: str,
        scope: ScopeFrame,
    ) -> List[exp.Expression]:
        """Every predicate a ranked CTE applies to the rows it ranks."""
        parts: List[exp.Expression] = []
        if local_key.column_filter_key is not None:
            cfk_sql = local_key.column_filter_key.canonical_sql
            if cfk_sql:
                parts.append(self._enter_mode_a_predicate(
                    sql=cfk_sql, scope=scope,
                    location=f"Column.filter on model {root_model.name!r}",
                ))
        skip_ids = {
            fp.id for fp in planned_query.filters_by_phase
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
        return parts

    def _render_kernel_producer_body(self, *, planned_query, bundle, kernel) -> str:
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

        if kernel.kind == "ranked":
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
            body = assemble_with_chain(
                entries=regroup_ctes, final=body,
                external_names=self._external_cte_names(),
            )
        return body.sql(dialect=self.dialect, pretty=True)

    def _build_windowed_grain_base(
        self, *, planned_query, plan, slots_by_id, aliases_by_slot_id,
        source_model, source_relation, bundle,
        regroup_env=None, regroup_join_specs=None,
    ) -> exp.Select:
        """The grain-rows relation for a collapsed windowed producer (DEV-1835"""
        allocator = self._gen_allocator or self._new_allocator()
        scope = self._scope_frame(
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=allocator, attached_columns=regroup_env,
        )
        cols: List[exp.Expression] = []
        group: List[exp.Expression] = []

        def _emit(slot, expr: exp.Expression) -> None:
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
        for g in group:
            base = base.group_by(g)
        return base

    def _render_with_combined_attaches(  # NOSONAR(S3776) — orchestration of host ``_base`` CTE + per-plan ``_cm_*`` CTEs + combined SELECT + transform-chain step CTEs + outer ORDER BY/LIMIT wrap. Each block is a coherent compilation stage sharing planned_query / slots_by_id / cma_slot_ids / seen_base_ids state; extracting per-stage helpers would scatter the cross-cutting state.
        self,
        *,
        planned_query,
        bundle,
    ) -> str:
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


        # The outer WHERE wrapper is routed by the planner; re-walking filters_by_phase here could disagree with the
        # emission-time decision.
        slot_by_key = {s.key: s for s in slots_by_id.values()}

        # Combined regroup producers render as _cm_ CTEs joined at the combined SELECT; prepared before the ROW
        # producers so a dual-role aggregate dedups onto them.
        (
            cm_regroup_ctes,
            regroup_placeholder_to_cm,
            regroup_placeholder_slot_ids,
            regroup_joinbacks,
            regroup_shift_specs,
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
        outer_where_filter_ids: Set[str] = set(
            planned_query.outer_where_filter_ids,
        )
        outer_where_filters: List = [
            fp for fp in planned_query.filters_by_phase
            if fp.id in outer_where_filter_ids
        ]
        # A composite whose tree walks an isolated cross-model aggregate must not render in _base (inline rendering
        # pulls filter-target joins in and corrupts both aggregates); route it outward.
        composite_kinds = (ArithmeticKey, ScalarCallKey)
        outer_composite_slot_ids: Set[str] = set()
        # Route a composite outward when the projection OR an ORDER BY entry references it — a hidden ORDER BY composite
        # would otherwise render inline in _base.
        composite_candidate_ids: Set[str] = set(planned_query.projection)
        for order_entry in planned_query.order:
            composite_candidate_ids.add(order_entry.slot_id)
        for slot in (
            list(planned_query.combined_expression_slots)
            + list(planned_query.aggregate_slots)
            + list(planned_query.row_slots)
        ):
            if slot.id not in composite_candidate_ids:
                continue
            if not isinstance(slot.key, composite_kinds):
                continue
            # A computed dimension (composite over a regroup placeholder) is grouped in _base, never routed outward;
            # only measure/order composites route out.
            if slot.is_dimension:
                continue
            _keys = list(walk_value_keys(slot.key))
            if (
                planned_query.transform_layers
                and any(isinstance(k, TransformKey) for k in _keys)
                and any(
                    isinstance(k, ColumnKey) and k in regroup_placeholder_to_cm
                    for k in _keys
                )
            ):
                continue
            for k in walk_value_keys(slot.key):
                if isinstance(k, ColumnKey) and k in regroup_placeholder_to_cm:
                    outer_composite_slot_ids.add(slot.id)
                    break
                if isinstance(k, AggregateKey):
                    s = slot_by_key.get(k)
                    # A windowed operand routes the composite outward (its value lives in a _wm_ CTE); rendering inside
                    # _base would substitute a plain aggregate for the rolling one.
                    if s is not None and s.id in isolated_slot_ids:
                        outer_composite_slot_ids.add(slot.id)
                        break
        base_projection = [
            sid for sid in planned_query.projection
            if sid not in isolated_slot_ids
            and sid not in outer_composite_slot_ids
        ]

        # Hidden ORDER-BY-only local slots are materialised in _base but stay out of the combined public projection
        # (trimmed).
        seen_base_ids = set(base_projection)
        order_only_local_ids: List[str] = []
        for order_entry in planned_query.order:
            sid = order_entry.slot_id
            if (
                sid in isolated_slot_ids
                or sid in outer_composite_slot_ids
                or sid in seen_base_ids
            ):
                continue
            slot = slots_by_id.get(sid)
            if slot is None:
                continue
            if getattr(getattr(slot.key, "source", None), "path", ()):
                continue
            order_only_local_ids.append(sid)
            seen_base_ids.add(sid)
        base_render_order = base_projection + order_only_local_ids

        aux_slot_id_by_key = {s.key: s.id for s in slots_by_id.values()}

        def _add_local_aux_slots(
            *,
            include_order: bool,
            aggregates_only: bool,
        ) -> None:
            """Pull local (non-cross-model) aux slot ids into"""
            for sid in self._collect_base_aux_slot_ids(
                planned_query=planned_query,
                slot_id_by_key=aux_slot_id_by_key,
                slots_by_id=slots_by_id,
                include_order=include_order,
                aggregates_only=aggregates_only,
            ):
                if sid in isolated_slot_ids or sid in seen_base_ids:
                    continue
                slot = slots_by_id.get(sid)
                if slot is None:
                    continue
                if getattr(getattr(slot.key, "source", None), "path", ()):
                    continue  # cross-model leaf dep → owned by a _cm_* CTE
                base_render_order.append(sid)
                seen_base_ids.add(sid)

        # Non-isolated local aggregate operands of an outer-rendered composite must still materialise in _base so the
        # outer SELECT can reference them via _base.<alias>.
        if outer_composite_slot_ids:
            for cid in outer_composite_slot_ids:
                cslot = slots_by_id.get(cid)
                if cslot is None:
                    continue
                for k in walk_value_keys(cslot.key):
                    if not isinstance(k, AggregateKey):
                        continue
                    dep = slot_by_key.get(k)
                    if dep is None:
                        continue
                    if dep.id in isolated_slot_ids or dep.id in seen_base_ids:
                        continue
                    base_render_order.append(dep.id)
                    seen_base_ids.add(dep.id)

        if planned_query.transform_layers:
            _add_local_aux_slots(include_order=True, aggregates_only=False)
        # A HAVING filter on a hidden local first/last must reach base_render_order so _base builds the ranked subquery,
        # else HAVING references a dangling _last_rn.
        _add_local_aux_slots(include_order=False, aggregates_only=True)

        # With no host rows or local aggs, _base is a one-row placeholder emitted WITHOUT the host FROM — a host FROM
        # would make it N rows and the scalar-_cm_ CROSS JOIN would duplicate the result N times.
        empty_base_plan = planned_query.empty_base_plan
        if empty_base_plan is not None:
            host_filter_ids = set(empty_base_plan.host_filter_ids)
            placeholder_skip_ids = {
                fp.id
                for fp in planned_query.filters_by_phase
                if fp.id not in host_filter_ids
            }
            if host_filter_ids:
                # LIMIT 1 collapses the host to one row so the CROSS JOIN doesn't duplicate aggregates, while WHERE
                # still gates the whole result (no matching host row -> 0 rows).
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
            base_group_by: Dict[str, exp.Expression] = {}
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



        proj_exprs: Dict[str, List[exp.Expression]] = {}
        combined_aliases_by_slot_id: Dict[str, List[str]] = {}

        def _emit(sid: str, expr: exp.Expression) -> None:
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
        outer_composite_order_expressions: Dict[str, exp.Expression] = {}
        if outer_composite_slot_ids:
            outer_composite_cm_map: Dict[str, Tuple[str, str]] = {}
            for _ph_key, _cm in regroup_placeholder_to_cm.items():
                _ph_slot = slot_by_key.get(_ph_key)
                if _ph_slot is not None:
                    outer_composite_cm_map[_ph_slot.id] = _cm

            def _render_outer_composite(cslot) -> exp.Expression:
                rendered = render_value_key(
                    key=cslot.key,
                    ctx=self._outer_wrapper_render_ctx(
                        slot_by_key=slot_by_key,
                        cross_model_agg_slot_to_cm=outer_composite_cm_map,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ),
                )
                if cslot.type is not None:
                    rendered = _wrap_cast_for_type(rendered, cslot.cast_type)
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
        combined_select_exprs: List[exp.Expression] = []
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
        joinback_specs = list(regroup_joinbacks)  # DEV-1829 — combined regroup producers
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
                if isinstance(rendered, (exp.And, exp.Or)):
                    rendered = exp.Paren(this=rendered)
                combined_select = combined_select.where(rendered)

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
                    *[
                        Node(
                            name=n, phase="producer", query=q,
                            depends_on=self._reuse_deps_of(n),
                        )
                        for n, q in cm_regroup_ctes
                    ],
                ],
                tail_select=combined_select,
                tail_schema=combined_aliases_by_slot_id,
                tail_phase="combined",
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
        cte_entries += [
            CteEntry(
                name=name, query=query,
                depends_on=self._reuse_deps_of(name),
            )
            for name, query in cm_regroup_ctes
        ]
        combined_statement = assemble_with_chain(
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
        for entry in planned_query.order:
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
            for entry in planned_query.order
        ]
        if order_terms:
            combined_statement.set("order", exp.Order(expressions=order_terms))

        combined_statement = self._dialect.apply_pagination(
            combined_statement,
            limit=planned_query.limit,
            offset=planned_query.offset,
        )

        return combined_statement.sql(dialect=self.dialect, pretty=True)

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
        """The bundle a producer renders against. DEV-1836 — a target-rooted"""
        root_name = getattr(attach, "producer_root_model", None)
        if not root_name:
            return bundle
        root = bundle.get_referenced_model(root_name)
        if root is None:
            raise RuntimeError(
                f"Target-rooted regroup producer names root model {root_name!r}, "
                f"absent from the bundle's referenced models."
            )
        return bundle.model_copy(update={"source_model": root})

    def _render_producer_split(
        self, *, producer, bundle, kernel=None,
    ) -> Tuple[List[Tuple[str, exp.Expression]], str]:
        """Render a regroup producer, split into (hoisted CTEs, body SQL) — D2."""
        producer_sql = self.generate_from_planned(
            planned_query=producer, bundle=bundle, as_cte_body=True,
            reuse_allocator=True, producer_kernel=kernel,
        )
        return self._split_statement_ctes(producer_sql)

    def _split_statement_ctes(
        self, sql: str,
    ) -> Tuple[List[Tuple[str, exp.Expression]], str]:
        """Split a rendered statement into (hoisted CTEs, de-WITHed body SQL)."""
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        self._unmangle_dotted_table_refs(parsed)
        with_nodes = list(parsed.find_all(exp.With))
        if not with_nodes:
            return [], sql
        allocator = self._gen_allocator or self._new_allocator()
        hoisted: List[Tuple[str, exp.Expression]] = []
        for with_node in with_nodes:
            self._uniquify_producer_base_ctes(with_node=with_node, allocator=allocator)
            hoisted.extend(
                (cte.alias_or_name, cte.this.copy()) for cte in with_node.expressions
            )
            with_node.pop()
        return hoisted, parsed.sql(dialect=self.dialect, pretty=True)

    _unmangle_dotted_table_refs = staticmethod(unmangle_dotted_table_refs)

    @staticmethod
    def _uniquify_producer_base_ctes(*, with_node, allocator) -> None:  # NOSONAR(S3776) — one rename pass; the collect / table-ref / column-qualifier / cte-alias rewrites share the rename map.
        """Rename a hoisted producer's hardcoded base CTE(s) (``_base``/``base``)"""
        parsed = with_node.parent
        rename: Dict[str, str] = {}
        for cte in with_node.expressions:
            name = cte.alias_or_name
            if name in ("_base", "base") and name not in rename:
                rename[name] = allocator.allocate_cte(name)
        if not rename:
            return
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

    def _prepare_combined_regroup_attaches(  # NOSONAR(S3776) — one cohesive combined-attach render (producer CTE → placeholder env → join-back); the phases share local state and reads clearer inline.
        self, *, planned_query, bundle, source_relation, slot_by_key,
    ):
        """Render each DEV-1829 combined regroup producer as a ``_cm_*`` CTE."""
        ctes: List[Tuple[str, exp.Expression]] = []
        placeholder_to_cm: Dict[Any, Tuple[str, str]] = {}
        placeholder_slot_ids: Set[str] = set()
        joinbacks: List[Tuple[str, List[Tuple[str, str]]]] = []
        shift_specs: List[Tuple[str, List[Tuple[Any, str]]]] = []
        allocator = self._gen_allocator or self._new_allocator()
        for attach in planned_query.regroup_attach_plans:
            if attach.attach_phase != "combined":
                continue
            producer = attach.producer_plan
            sub_slots = {
                s.id: s
                for s in (
                    list(producer.row_slots)
                    + list(producer.aggregate_slots)
                    + list(producer.combined_expression_slots)
                )
            }
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
                self._gen_split_consumers.append(cte_name)
                try:
                    producer_hoisted, producer_body_sql = (
                        self._render_producer_split(
                            producer=producer,
                            bundle=self._producer_render_bundle(
                                attach=attach, bundle=bundle,
                            ),
                            kernel=attach.kernel,
                        )
                    )
                finally:
                    self._gen_split_consumers.pop()
                ctes.extend(producer_hoisted)
                ctes.append((cte_name, self._parse_cte_body(producer_body_sql)))
                producer_relation = producer.source_relation
                col_by_sid = {
                    sid: self._full_alias_for_slot(
                        slot=slot, source_relation=producer_relation,
                        alias_index={},
                    )
                    for sid, slot in sub_slots.items()
                }
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
        )

    def _record_reuse_edges(self, shared_cte: str) -> None:
        """DEV-1838 D3 — a reuse under an active producer split: every enclosing"""
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
        """Structural identity of a regroup attach's producer (DEV-1835 D10): the"""
        return (
            frozenset(sub.original_key for sub in attach.substitutions),
            frozenset(host_key for host_key, _ in attach.join_pairs),
        )

    def _prepare_regroup_attaches(  # NOSONAR(S3776) — one linear pass over the planned regroup producers (dedup → render → hoist); splitting would thread the CTE registry through every helper
        self, *, planned_query, bundle, dedup_producers=None,
    ):
        """Render each DEV-1825 regroup producer as a ``_cm_*`` CTE."""
        dedup_producers = dedup_producers or {}
        ctes: List[CteEntry] = []
        attached_env: Dict[Any, exp.Expression] = {}
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
            # Flatten each producer output column to a dot-free name: a dotted alias in a WHERE predicate is stringified
            # and re-parsed, and BigQuery mis-splits it.
            def _flat(slot) -> str:
                dotted = self._full_alias_for_slot(
                    slot=slot, source_relation=relation, alias_index={},
                )
                prefix = f"{relation}."
                stripped = dotted[len(prefix):] if dotted.startswith(prefix) else dotted
                return stripped.replace(".", "__")

            self._gen_split_consumers.append(cte_name)
            try:
                producer_hoisted, producer_body_sql = self._render_producer_split(
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
                source_relation=relation, stage_sql=producer_body_sql,
                expected_columns=expected, dialect=self.dialect,
            )
            reuse_deps = self._reuse_deps_of(cte_name)
            for hoisted_name, hoisted_body in producer_hoisted:
                ctes.append(Node(
                    name=hoisted_name, phase="producer", query=hoisted_body,
                    depends_on=list(reuse_deps),
                ))
            exposed_by_sid = {sid: _flat(slot) for sid, slot in sub_slots.items()}
            ctes.append(Node(
                name=cte_name, phase="producer", query=wrapped,
                depends_on=[
                    *[name for name, _ in producer_hoisted], *reuse_deps,
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
            current_model = source_model
            current_alias = source_relation
            for hop_idx, hop in enumerate(path):
                join_def = next(
                    (j for j in current_model.joins if j.target_model == hop),
                    None,
                )
                if join_def is None:
                    raise ValueError(
                        f"Model {current_model.name!r} has no join to "
                        f"{hop!r}; needed for joined path {path!r}.",
                    )
                next_model = bundle.get_referenced_model(hop)
                if next_model is None:
                    raise ValueError(
                        f"Join target {hop!r} not in resolved source bundle.",
                    )
                next_alias = self._join_alias(
                    root=source_relation, path=path[: hop_idx + 1],
                )
                if next_alias not in emitted_aliases:
                    join_on_parts = []
                    for src_col, tgt_col in join_def.join_pairs:
                        # Join keys are physical DB columns — quote them when mixed-case via _to_ident so a case-folding
                        # backend resolves them; table qualifiers are internal aliases.
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
                    # Honor the model's declared join_type (default LEFT so a measure never changes cardinality;
                    # explicit INNER only when declared).
                    joins.append((
                        join_expr, on_expr, join_def.join_type.value.upper(),
                    ))
                    emitted_aliases.add(next_alias)
                current_model = next_model
                current_alias = next_alias
        return base_from, joins

    def _joined_or_local_dim_expr(
        self,
        *,
        path: Tuple[str, ...],
        leaf: str,
        source_model,
        source_relation: str,
        bundle,
    ) -> exp.Expression:
        """Resolve a dimension column expression on either the host"""
        if not path:
            return self._dim_column_expr_from_planned(
                source_model=source_model,
                source_relation=source_relation,
                leaf=leaf,
            )
        current_alias = source_relation
        current_model = source_model
        for hop_idx, hop in enumerate(path):
            target_alias = self._join_alias(
                root=source_relation, path=path[: hop_idx + 1],
            )
            current_alias = target_alias
            target_model = bundle.get_referenced_model(hop)
            if target_model is None:
                raise ValueError(
                    f"Joined dim path {path!r}: target {hop!r} missing "
                    f"from the resolved source bundle.",
                )
            current_model = target_model
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

    def _window_ordered(self, col: exp.Expression, *, descending: bool = False) -> exp.Ordered:
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
        """The transform auto-grain (DEV-1837 D1/D2, Option A): every projected"""
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
    ) -> exp.Expression:
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
            measure = render_value_key(
                key=key.input,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
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

        time_col: Optional[exp.Expression] = None
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
            fn: exp.Expression,
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
        # The rank family orders by the MEASURE descending, not by time.
        rank_order = exp.Order(
            expressions=[self._window_ordered(measure.copy(), descending=True)],
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
    ) -> List[str]:
        """Render each POST-phase ``FilterPhase.expression`` to a SQL"""

        out: List[str] = []
        for fp in planned_query.filters_by_phase:
            if fp.phase != Phase.POST:
                continue
            if fp.expression is None:
                raise ValueError(
                    f"POST-phase FilterPhase id={fp.id!r} has no typed "
                    f"expression; text-only POST filters are not supported.",
                )
            rendered = render_value_key(
                key=fp.expression.value_key,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                ),
            )
            out.append(rendered.sql(dialect=self.dialect))
        return out

    def _emit_planned_outer_wrap(
        self,
        *,
        chain_sql: str,
        public_aliases: List[str],
        planned_query,
        slots_by_id: Dict[str, Any],
        available_alias_by_slot_id: Dict[str, str],
    ) -> str:
        """Wrap ``chain_sql`` in the public-projection outer SELECT, through"""
        order_terms = self._planned_order_terms(
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            available_alias_by_slot_id=available_alias_by_slot_id,
        )
        order_expr = exp.Order(expressions=order_terms) if order_terms else None
        limit_expr = (
            exp.Limit(expression=exp.Literal.number(planned_query.limit))
            if planned_query.limit is not None
            else None
        )
        offset_expr = (
            exp.Offset(expression=exp.Literal.number(planned_query.offset))
            if planned_query.offset is not None
            else None
        )
        return self._dialect.emit_outer_wrap(
            inner_sql=chain_sql,
            public=public_aliases,
            order=order_expr,
            limit=limit_expr,
            offset_arg=offset_expr,
            parse=self._parse,
        )

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
            for entry in planned_query.order
        ]

    def _build_shifted_cte_where_parts(
        self,
        *,
        planned_query,
        source_relation: str,
        source_model,
        bundle,
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
    ) -> Tuple[List[str], List[Tuple[str, ...]]]:
        """Build the WHERE clauses for the shifted CTE that re-aggregates"""

        out: List[str] = []
        crossed_paths: List[Tuple[str, ...]] = []
        time_cols = frozenset(planned_query.frame_bound_columns)
        for fp in planned_query.filters_by_phase:
            if fp.phase != Phase.ROW:
                continue
            rendered = self._shifted_where_part(
                fp=fp, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
                time_columns=time_cols, regroup_env=regroup_env,
            )
            if rendered is None:
                continue
            part, paths = rendered
            out.append(part)
            for p in paths:
                if p not in crossed_paths:
                    crossed_paths.append(p)
        return out, crossed_paths

    def _shifted_where_part(
        self, *, fp, source_relation: str, source_model, bundle,
        time_columns: "AbstractSet[Any]",
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
    ) -> "Optional[Tuple[str, List[Tuple[str, ...]]]]":
        """Render one ROW-phase filter for the shifted CTE, returning its SQL"""
        if fp.expression is not None:
            residual = strip_frame_bounds(
                key=fp.expression.value_key, time_columns=time_columns,
            )
            if residual is None:
                return None  # wholly a frame bound — omit from the shifted CTE.
            rendered = render_value_key(
                key=residual,
                ctx=self._filter_render_context(
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                    regroup_env=regroup_env,
                ),
            )
            if isinstance(rendered, (exp.And, exp.Or)):
                rendered = exp.Paren(this=rendered)
            paths = self._joined_paths_in_sql(
                sql_expr=rendered, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
            )
            return rendered.sql(dialect=self.dialect), paths
        if fp.text is not None:
            frame = self._mode_a_scope(
                source_model=source_model,
                source_relation=source_relation,
                bundle=bundle,
            )
            rendered = frame.enter_predicate(
                fp.text,
                location=f"SlayerModel.filters on model {source_model.name!r}",
            )
            return rendered.sql(dialect=self.dialect), frame.join_paths.as_list()
        return None

    def _emit_time_shift_ctes_for_planned(  # NOSONAR(S3776) — single conceptual unit for one time_shift slot: partition/time resolution through the shifted ScopeFrame + shifted-CTE body assembly + collision-safe CTE naming (cte_allocator) + sjoin grain join-back, all sharing tightly-coupled per-slot state (time_alias / input_alias / partition_specs / shifted_cte_name / carry aliases). Splitting forces that cross-cutting state through many-argument helpers without simplifying anything — same shape as the sibling producer-CTE renderers' suppression.
        self,
        *,
        slot,
        chain: ChainState,
        render: RenderState,
        shifted_where_parts: List[str],
        shifted_where_join_paths: List[Tuple[str, ...]],
        chain_tail: str,
    ) -> str:
        """Emit a ``shifted_<alias>`` + ``sjoin_<alias>`` CTE pair for"""
        ctes = chain.ctes
        cte_allocator = chain.cte_allocator
        slots_by_id = chain.slots_by_id
        slot_id_by_key = chain.slot_id_by_key
        available_alias_by_slot_id = chain.available_alias_by_slot_id
        aliases_by_slot_id = chain.aliases_by_slot_id
        source_model = chain.source_model
        source_relation = chain.source_relation
        planned_query = render.planned_query
        bundle = render.bundle

        key = slot.key
        if not isinstance(key, TransformKey) or key.op != "time_shift":
            raise ValueError(
                f"expected time_shift TransformKey, got "
                f"{type(key).__name__} (op={getattr(key, 'op', None)!r})",
            )
        inner_key = key.input
        time_key = key.time_key
        # A COMPOSITE re-aggregates each aggregate leaf in the shifted CTE: any
        # regroup-isolated leaf (a join-crossing fragment aggregation the planner
        # moved into a _cm_* CTE) is substituted back to its original aggregate so
        # the builder can re-aggregate it. A BARE leaf keeps its DEV-1750 path
        # (aggregate → re-aggregate; column / regroup-placeholder → read-and-
        # rebucket its base-slot value), leaving single-leaf SQL byte-identical.
        is_composite = isinstance(inner_key, (ArithmeticKey, ScalarCallKey))
        placeholder_to_original, regroup_slot_by_key = _regroup_placeholder_map(
            planned_query,
        )
        shifted_input_key = (
            substitute_value_keys(key=inner_key, mapping=placeholder_to_original)
            if is_composite else inner_key
        )
        if not isinstance(time_key, TimeTruncKey):
            raise ValueError(
                f"time_shift requires a TimeTruncKey time_key; got "
                f"{type(time_key).__name__} (slot id={slot.id!r}).",
            )

        periods_raw = next(
            (v for k, v in key.kwargs if k == "periods"), None,
        )
        if periods_raw is None:
            raise ValueError(
                f"time_shift requires 'periods' kwarg; planner gap "
                f"(slot id={slot.id!r}).",
            )
        if isinstance(periods_raw, bool):
            raise ValueError(
                f"time_shift periods must be an integer; got bool {periods_raw!r}",
            )
        if isinstance(periods_raw, Decimal):
            if periods_raw != periods_raw.to_integral_value():
                raise ValueError(
                    f"time_shift periods must be an integer; got {periods_raw!r}",
                )
            periods = int(periods_raw)
        elif isinstance(periods_raw, int):
            periods = int(periods_raw)
        else:
            raise ValueError(
                f"time_shift periods must be an integer; got "
                f"{type(periods_raw).__name__} {periods_raw!r}",
            )

        time_sid = slot_id_by_key.get(time_key)
        if time_sid is None or time_sid not in available_alias_by_slot_id:
            raise RuntimeError(
                f"time_shift time_key not materialised in base CTE: "
                f"slot id={slot.id!r}, time_key={time_key!r}.",
            )
        time_alias = available_alias_by_slot_id[time_sid]

        # A bare leaf has its own base slot (alias reused, keeping N=1 SQL
        # byte-identical); a composite has none, so its value gets an allocated
        # internal alias below.
        input_sid = slot_id_by_key.get(inner_key)
        input_alias = (
            available_alias_by_slot_id.get(input_sid)
            if input_sid is not None else None
        )
        if not is_composite and input_alias is None:
            raise RuntimeError(
                f"time_shift input not materialised in base CTE: "
                f"slot id={slot.id!r}, input={inner_key!r}.",
            )

        shifted_allocator = self._gen_allocator or self._new_allocator()
        shifted_scope = self._scope_frame(
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=shifted_allocator,
            attached_columns=render.regroup_env,
        )

        # Auto-include every projected dimension in the sjoin grain, else a prior-period total broadcasts across every
        # value of an ungrouped dimension.
        partition_specs: list[tuple[str, str, exp.Expression]] = []
        seen_partition_sids: set = set()

        def _resolve_partition_expr(pk_obj) -> exp.Expression:
            if isinstance(pk_obj, TimeTruncKey):
                raw = shifted_scope.resolve(pk_obj.column)
                return self._build_date_trunc(
                    col_expr=raw,
                    granularity=TimeGranularity(pk_obj.granularity),
                )
            if isinstance(pk_obj, (ColumnKey, ColumnSqlKey)):
                return shifted_scope.resolve(pk_obj)
            if isinstance(pk_obj, (ScalarCallKey, ArithmeticKey)):
                return render_value_key(
                    key=pk_obj,
                    ctx=RenderContext(scope=shifted_scope, dialect=self._dialect),
                )
            # Unreachable: auto-partitions are query-dimension slots, always one
            # of the five kinds above. A RuntimeError invariant, not a user error.
            raise RuntimeError(
                f"time_shift partition on {type(pk_obj).__name__} reached the "
                f"shifted CTE (slot id={slot.id!r}); only query-dimension "
                f"partition kinds are expected here.",
            )

        def _add_partition(pk_obj, *, where: str) -> None:
            pk_sid = slot_id_by_key.get(pk_obj)
            if pk_sid is None or pk_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"time_shift {where} not materialised: "
                    f"slot id={slot.id!r}, key={pk_obj!r}.",
                )
            if pk_sid == time_sid or pk_sid in seen_partition_sids:
                return
            pk_alias = available_alias_by_slot_id[pk_sid]
            partition_specs.append((pk_sid, pk_alias, _resolve_partition_expr(pk_obj)))
            seen_partition_sids.add(pk_sid)

        grain_sids = set(self._transform_grain_slot_ids(
            planned_query=planned_query, slots_by_id=slots_by_id,
        ))
        for sid in planned_query.projection:
            dim_slot = slots_by_id.get(sid)
            if dim_slot is None or dim_slot.phase != Phase.ROW:
                continue
            if sid in grain_sids or isinstance(dim_slot.key, TimeTruncKey):
                _add_partition(dim_slot.key, where="query dimension")

        # partition_by is binder-rejected on non-rank transforms (DEV-1739) and
        # rank never routes here, so explicit partition_keys are always empty.
        if key.partition_keys:
            raise RuntimeError(
                f"time_shift unexpectedly carries partition_keys "
                f"{key.partition_keys!r} (slot id={slot.id!r}).",
            )

        def _build_shifted_leaf_agg(leaf_key) -> exp.Expression:
            """Re-aggregate one aggregate leaf over the shifted bucket: register
            its joins / fragment kwargs / column filter into ``shifted_scope``,
            build the aggregate, and cast per the leaf's own base slot. The
            composite renderer recomposes the arithmetic on top."""
            leaf_sid = slot_id_by_key.get(leaf_key)
            leaf_slot = (
                slots_by_id.get(leaf_sid) if leaf_sid is not None
                else regroup_slot_by_key.get(leaf_key)
            )
            if leaf_slot is None:
                raise RuntimeError(
                    f"time_shift composite leaf not materialised: "
                    f"slot id={slot.id!r}, leaf={leaf_key!r}.",
                )
            if getattr(leaf_key.source, "path", ()):  # gate rejects this shape
                raise RuntimeError(
                    f"time_shift reached a cross-model aggregate leaf "
                    f"{leaf_key!r} in the shifted CTE (slot id={slot.id!r}).",
                )
            leaf_frag_kwargs: "Dict[str, ResolvedAggKwarg]" = {}
            if isinstance(leaf_key.source, ColumnSqlKey):
                shifted_scope.resolve(leaf_key.source)
            for _arg in leaf_key.args:
                if isinstance(_arg, ColumnSqlKey) and _arg.path:
                    continue
                if isinstance(_arg, (ColumnKey, ColumnSqlKey)):
                    shifted_scope.resolve(_arg)
            for _kname, _kval in leaf_key.kwargs:
                if isinstance(_kval, (ColumnKey, ColumnSqlKey)):
                    shifted_scope.resolve(_kval)
            for _fname, _fast in self._register_fragment_kwarg_joins(
                key=leaf_key, scope=shifted_scope, model=source_model,
            ).items():
                leaf_frag_kwargs.setdefault(
                    _fname, ResolvedAggKwarg(kind="expr", value=_fast),
                )
            if (
                leaf_key.column_filter_key is not None
                and leaf_key.column_filter_key.canonical_sql
            ):
                self._enter_mode_a_predicate(
                    sql=leaf_key.column_filter_key.canonical_sql,
                    scope=shifted_scope,
                    location=f"Column.filter on model {source_model.name!r}",
                )
            synth = self._build_agg_render_spec_from_planned(
                slot=leaf_slot, key=leaf_key, source_model=source_model,
                source_relation=source_relation,
                full_alias=input_alias or "__op__", bundle=bundle,
                resolved_agg_kwargs=leaf_frag_kwargs or None,
            )
            agg_expr, _ = self._build_agg(synth)
            return _wrap_cast_for_type(agg_expr, leaf_slot.cast_type)

        # Shift granularity is the explicit 3rd arg else the TD granularity, so a year-shift over a month bucket yields
        # 'same month, previous year' (YoY).
        shift_gran_raw = next(
            (v for k, v in key.kwargs if k == "granularity"), None,
        )
        shift_granularity = (
            str(shift_gran_raw) if shift_gran_raw is not None
            else time_key.granularity
        )
        raw_time_col_expr = shifted_scope.resolve(time_key.column)
        # Truncate BEFORE shifting: offsetting a raw timestamp overflows on non-clamping dialects (SQLite: Jan 31 + 1
        # month = Mar 2), dropping period-tail rows; the outer re-trunc is skipped only for bucket-aligned offsets.
        bucket_granularity = TimeGranularity(time_key.granularity)
        bucketed_time_expr = self._build_date_trunc(
            col_expr=raw_time_col_expr,
            granularity=bucket_granularity,
        )
        shifted_raw_expr = self._build_time_offset_expr(
            col_expr=bucketed_time_expr,
            offset=-periods,
            granularity=shift_granularity,
        )
        if _shift_preserves_bucket_starts(
            bucket=bucket_granularity, shift=shift_granularity,
        ):
            shifted_trunc_expr = shifted_raw_expr
        else:
            shifted_trunc_expr = self._build_date_trunc(
                col_expr=shifted_raw_expr,
                granularity=bucket_granularity,
            )

        shifted_select_parts: List[exp.Expression] = []
        shifted_group_by: List[exp.Expression] = []

        shifted_select_parts.append(
            shifted_trunc_expr.as_(time_alias, quoted=True),
        )
        shifted_group_by.append(shifted_trunc_expr.copy())

        for _, pk_alias, pk_expr in partition_specs:
            shifted_select_parts.append(pk_expr.as_(pk_alias, quoted=True))
            shifted_group_by.append(pk_expr.copy())

        if not is_composite and isinstance(inner_key, (ColumnKey, ColumnSqlKey)):
            # Bare column or regroup placeholder: grouped and projected directly
            # (read-and-rebucket of its base-slot value — DEV-1750, unchanged).
            shifted_value_expr = shifted_scope.resolve(inner_key)
            shifted_group_by.append(shifted_value_expr.copy())
            shifted_value_alias = input_alias
        else:
            # Bare aggregate (N=1) or aggregate-only composite (N>1): the same
            # door — each aggregate leaf re-aggregates in the shifted bucket and
            # the composite recomposes on top. No whole-composite cast (leaves
            # cast). A bare aggregate reuses its base-slot alias (byte-identical).
            shifted_value_expr = render_value_key(
                key=shifted_input_key,
                ctx=RenderContext(
                    scope=shifted_scope, dialect=self._dialect,
                    composites=CompositeFacilities(
                        agg_builder=_build_shifted_leaf_agg,
                    ),
                ),
            )
            shifted_value_alias = input_alias or cte_allocator.allocate_cte(
                f"{slot.declared_name}__ts",
            )
        shifted_select_parts.append(
            shifted_value_expr.as_(shifted_value_alias, quoted=True),
        )

        # A composite re-aggregates every leaf from source, so it reads no _cm_*
        # value — omit the regroup attaches (a bare read-and-rebucket still needs
        # them to resolve its placeholder column).
        regroup_attach_conditions = (
            []
            if is_composite
            else self._resolve_regroup_attach_conditions(
                regroup_join_specs=render.regroup_join_specs, scope=shifted_scope,
            )
        )
        for _p in shifted_where_join_paths:
            shifted_scope.join_paths.add(_p)
        shifted_join_paths = shifted_scope.join_paths.as_list()
        if shifted_join_paths:
            from_clause, shifted_joins = self._build_from_and_joins(
                source_model=source_model,
                source_relation=source_relation,
                joined_paths=shifted_join_paths,
                bundle=bundle,
            )
        else:
            from_clause = self._build_from_clause_from_planned(
                source_model=source_model, source_relation=source_relation,
            )
            shifted_joins = []

        shifted_select = exp.Select().select(*shifted_select_parts).from_(
            from_clause,
        )
        shifted_select = _apply_joins(select=shifted_select, joins=shifted_joins)
        for _cte_name, _condition in regroup_attach_conditions:
            if _condition is None:
                shifted_select = shifted_select.join(
                    exp.to_identifier(_cte_name), join_type="CROSS",
                )
            else:
                shifted_select = shifted_select.join(
                    exp.to_identifier(_cte_name), on=_condition, join_type="LEFT",
                )
        for _where_part in shifted_where_parts:
            shifted_select = shifted_select.where(
                self._parse_predicate(_where_part),
            )
        for _gb in shifted_group_by:
            shifted_select = shifted_select.group_by(_gb)

        # A hidden inner time_shift slot's declared_name isn't unique across sibling shifts with different offsets;
        # allocate a unique internal alias so growth_2m doesn't collapse onto growth_1m.
        if slot.public_aliases:
            slot_aliases: List[str] = list(slot.public_aliases)
        else:
            slot_aliases = [cte_allocator.allocate_cte(slot.declared_name)]
        cte_name_alias = slot_aliases[0]
        # Length-fit the shifted_/sjoin_ CTE names so a long transform name can't exceed the dialect's identifier limit
        # and silently truncate.
        _fit_kw = dict(
            allocator=cte_allocator, dialect=self.dialect,
            limit=self._dialect.max_identifier_bytes,
        )
        shifted_cte_name = cte_name_from_alias(
            prefix="shifted_", alias=cte_name_alias, **_fit_kw,
        )
        sjoin_cte_name = cte_name_from_alias(
            prefix="sjoin_", alias=cte_name_alias, **_fit_kw,
        )

        ctes.append(CteEntry(
            name=shifted_cte_name, query=shifted_select,
            depends_on=[name for name, _ in regroup_attach_conditions],
        ))

        prev_cte = chain_tail  # the chain tail this pair extends
        carry_aliases = self._carry_aliases_in_plan_order(
            aliases_by_slot_id,
        )
        sjoin_select_parts: List[exp.Expression] = [
            grain_alias_column(alias=a, table=prev_cte) for a in carry_aliases
        ]
        slot_full_aliases: List[str] = []
        for slot_alias in slot_aliases:
            full_slot_alias = f"{source_relation}.{slot_alias}"
            slot_full_aliases.append(full_slot_alias)
            sjoin_select_parts.append(
                grain_alias_column(
                    alias=shifted_value_alias, table=shifted_cte_name,
                ).as_(full_slot_alias, quoted=True),
            )

        grain_alias_names = [time_alias] + [
            pk_alias for _, pk_alias, _ in partition_specs
        ]
        sjoin_on = build_grain_joinback_condition(
            pairs=[
                (
                    grain_alias_column(alias=a, table=prev_cte),
                    grain_alias_column(alias=a, table=shifted_cte_name),
                )
                for a in grain_alias_names
            ],
            dialect=self._dialect,
        )
        sjoin_select = exp.Select().select(*sjoin_select_parts).from_(
            prev_cte,
        ).join(shifted_cte_name, on=sjoin_on, join_type="LEFT")
        ctes.append(CteEntry(
            name=sjoin_cte_name,
            query=sjoin_select,
            depends_on=[prev_cte, shifted_cte_name],
        ))

        for full_slot_alias in slot_full_aliases:
            aliases_by_slot_id.setdefault(slot.id, []).append(full_slot_alias)
        available_alias_by_slot_id.setdefault(slot.id, slot_full_aliases[0])
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
        predicate_is_boolean = _is_boolean_shaped(inner_key)
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

        if predicate_is_boolean:
            pred_in_case: exp.Expression = exp.Coalesce(
                this=predicate, expressions=[exp.false()],
            )
        else:
            pred_in_case = predicate

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
                        this=pred_in_case.copy(),
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
                this=pred_in_case.copy(),
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

    def _qualify_column_filter_sql(
        self,
        *,
        canonical_sql: Optional[str],
        source_relation: str,
        source_model,
    ) -> Optional[str]:
        """Qualify bare-identifier column refs in a Mode-A filter fragment."""
        if not canonical_sql:
            return None
        try:
            ast = self._parse_predicate(canonical_sql)
        except Exception:
            return canonical_sql
        known_names = {c.name for c in source_model.columns}
        for col in ast.find_all(exp.Column):
            if col.args.get("table") is not None:
                continue
            ident = col.this
            if not isinstance(ident, exp.Identifier):
                continue
            if ident.name in known_names:
                col.set("table", exp.to_identifier(source_relation))
        return ast.sql(dialect=self.dialect)

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
    ) -> exp.Expression:
        """Enter a Mode-A PREDICATE through the door and hand back its AST."""
        frame = scope or self._mode_a_scope(
            source_model=source_model,
            source_relation=source_relation,
            bundle=bundle,
        )
        return frame.enter_predicate(sql, location=location)

    def _enter_mode_a_expression(
        self,
        *,
        sql: str,
        scope: ScopeFrame,
        location: Optional[str] = None,
    ) -> exp.Expression:
        """Enter a Mode-A scalar EXPRESSION (a ``Column.sql`` / aggregation"""
        return scope.enter_expression(sql, location=location)

    def _register_fragment_kwarg_joins(
        self, *, key, scope: ScopeFrame, model,
    ) -> "Dict[str, exp.Expression]":
        """Resolve an aggregation's template FRAGMENTS through the Mode-A door,"""
        agg_def = next(
            (a for a in (model.aggregations or []) if a.name == key.agg), None,
        )
        if agg_def is None:
            return {}
        formula = agg_def.formula or ""
        overridden = {name for name, _ in key.kwargs}
        named_fragments: List[Tuple[str, str]] = [
            (name, v) for name, v in key.kwargs
            if isinstance(v, str) and f"{{{name}}}" in formula
        ]
        named_fragments.extend(
            (p.name, p.sql) for p in (agg_def.params or [])
            if p.name not in overridden and p.sql
        )
        resolved: "Dict[str, exp.Expression]" = {}
        for name, frag in named_fragments:
            resolved[name] = self._enter_mode_a_expression(
                sql=frag, scope=scope,
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
    ) -> Dict[str, exp.Expression]:
        """Pre-expand derived (``ColumnSqlKey``) ROW dimensions and derived TIME"""

        def _add(path: Tuple[str, ...]) -> None:
            if path:
                scope.join_paths.add(path)

        derived_expr_by_sid: Dict[str, exp.Expression] = {}
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
    ) -> "Optional[exp.Expression]":
        """The rendered expression for a derived (``ColumnSqlKey``) column."""
        if key.path:
            owner_model = bundle.get_referenced_model(key.path[-1])
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
            self._parse(expanded_sql), col.type if col is not None else None,
        )

    def _expand_column_filter_sql(
        self,
        *,
        canonical_sql: Optional[str],
        source_relation: str,
        source_model,
        bundle=None,
    ) -> Optional[str]:
        """Render a ``Column.filter`` Mode-A predicate for the aggregation-time"""
        if not canonical_sql:
            return None
        if bundle is None:
            return self._qualify_column_filter_sql(
                canonical_sql=canonical_sql,
                source_relation=source_relation,
                source_model=source_model,
            )
        return self._enter_mode_a_predicate(
            sql=canonical_sql,
            source_model=source_model,
            source_relation=source_relation,
            bundle=bundle,
            location=f"Column.filter on model {source_model.name!r}",
        ).sql(dialect=self.dialect)

    def _build_from_clause_from_planned(
        self,
        *,
        source_model,
        source_relation: str,
    ) -> exp.Expression:
        if source_model.sql_table:
            return self._to_table(source_model.sql_table, alias=source_relation)
        if source_model.sql:
            return exp.Subquery(
                this=self._parse(source_model.sql),
                alias=exp.to_identifier(source_relation),
            )
        raise NotImplementedError(
            f"DEV-1450 stage 7b.12+: query-backed models (source_queries) "
            f"deferred to multi-stage slices. Model "
            f"{source_model.name!r} has neither sql_table nor sql set."
        )

    def _dim_column_expr_from_planned(
        self, *, source_model, source_relation: str, leaf: str,
    ) -> exp.Expression:
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
    ) -> exp.Expression:
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
                joined_model = bundle.get_referenced_model(time_column.path[-1])
                if joined_model is None:
                    raise ValueError(
                        f"Time dimension references derived column "
                        f"{time_column.column_name!r} on joined model "
                        f"{time_column.path[-1]!r} which is not in the resolved "
                        f"source bundle.",
                    )
                # A joined derived TIME dim whose sql crosses a further join must anchor inner refs at the host-path
                # alias, not the bare direct-join alias, or the FROM references an unjoined table.
                expanded_sql = self._expand_derived_column_sql(
                    source_model=joined_model,
                    source_relation="__".join(time_column.path),
                    column_name=time_column.column_name,
                    bundle=bundle,
                    owner_path=tuple(time_column.path),
                )
            else:
                expanded_sql = self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=time_column.column_name,
                    bundle=bundle,
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
    ) -> str:
        """Expand a derived ``Column.sql`` (a ``ColumnSqlKey`` target) into a"""
        col = next(
            (c for c in source_model.columns if c.name == column_name), None,
        )
        if col is None:
            raise ValueError(
                f"Derived column {column_name!r} not found on model "
                f"{source_model.name!r}",
            )
        if col.sql is None:
            return col.name
        resolver_root = root_relation if root_relation is not None else source_relation
        expanded = expand_derived_refs_sync(
            sql=col.sql,
            model=source_model,
            alias_path=source_relation,
            resolve_model=bundle.get_referenced_model,
            dialect=self.dialect,
            owner_path=owner_path,
            alias_resolver=self._join_alias_resolver(resolver_root),
            crossed_paths=crossed_paths,
        )
        return expanded if expanded is not None else col.sql

    def _render_expression_source_sql(
        self, *, source, source_model, source_relation: str, bundle,
    ) -> str:
        """Render an aggregate's row-level expression source (DEV-1826) to
        qualified SQL text: plain columns anchor at ``source_relation``,
        derived columns expand through ``_expand_derived_column_sql``."""
        def _column_ast(ref) -> exp.Expression:
            # Fail closed for a joined operand before any dispatch, so a pathed
            # ColumnSqlKey can't expand against the host relation (DEV-1832).
            if getattr(ref, "path", ()):
                raise NotImplementedError(
                    f"Cross-model operand {ref!r} inside an aggregated "
                    f"expression is not supported (DEV-1832)."
                )
            if isinstance(ref, ColumnSqlKey):
                return self._parse(self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=ref.column_name,
                    bundle=bundle,
                ))
            return exp.Column(
                this=self._to_ident(ref.leaf),
                table=exp.to_identifier(source_relation),
            )

        node = render_row_expression(
            key=source, dialect=self._dialect, resolve_column=_column_ast,
        )
        return node.sql(dialect=self.dialect)

    def _joined_paths_in_sql(
        self, *, sql_expr: exp.Expression, source_relation: str, source_model,
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
            planned_query.filters_by_phase
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
        """Join paths a typed filter ``ValueKey`` tree references (DEV-1450 /"""

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
                    bundle.get_referenced_model(k.path[-1]) if k.path
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

        if not source.path:
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
        """The terminal model of a join ``path`` walked from ``source_model``,"""
        current = source_model
        for hop in path:
            if not any(j.target_model == hop for j in current.joins):
                return None
            nxt = bundle.get_referenced_model(hop)
            if nxt is None:
                return None
            current = nxt
        return current

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
    ) -> AggRenderSpec:
        """Build an ``AggRenderSpec`` from a planned aggregate slot so"""

        # slot may be None for a HAVING term whose aggregate isn't a projection slot; the result type is then unknown
        # (no outer CAST).
        slot_type = slot.type if slot is not None else None
        source = key.source
        if isinstance(source, StarKey):
            # Reject any non-count aggregation on * (*:sum would render as SUM(*)); enforce here so invalid SQL can't be
            # emitted.
            if key.agg != "count":
                raise ValueError(
                    f"Aggregation {key.agg!r} not allowed with measure "
                    f"'*' — use '*:count' for COUNT(*)."
                )
            if key.args or key.kwargs:
                raise ValueError(
                    f"'*:count' takes no args or kwargs; got "
                    f"args={key.args!r}, kwargs={key.kwargs!r}."
                )
            return AggRenderSpec(
                name="",
                sql=None,
                aggregation=key.agg,
                alias=full_alias,
                model_name=source_relation,
                type=slot_type,
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
                    f"producer (DEV-1838)."
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
            # Inner bare refs in a derived aggregate's Column.sql must qualify to source_relation, else the SQL keeps
            # bare 'amount' where it needs 'orders.amount'.
            if (
                isinstance(source, ColumnSqlKey)
                and col.sql is not None
                and bundle is not None
            ):
                sql_text = self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=col.name,
                    bundle=bundle,
                    owner_path=source.path if host_grain_root is not None else (),
                    root_relation=host_grain_root,
                )
            else:
                sql_text = col.sql if col.sql else col.name
            resolved_kw = resolved_agg_kwargs or {}
            agg_kwargs_str = {
                k: (resolved_kw[k] if k in resolved_kw else agg_kwarg_canonical_str(v))
                for k, v in key.kwargs
            }
            key_kwarg_names = {k for k, _ in key.kwargs}
            for _name, _resolved in resolved_kw.items():
                if _name not in key_kwarg_names:
                    agg_kwargs_str.setdefault(_name, _resolved)
            # Propagate column_filter_key into filter_sql (SUM(CASE WHEN <filter> THEN col END)) and qualify the
            # filter's bare refs with the host model name.
            filter_sql = self._expand_column_filter_sql(
                canonical_sql=(
                    key.column_filter_key.canonical_sql
                    if key.column_filter_key is not None
                    else None
                ),
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
            )
            return AggRenderSpec(
                name=col.name,
                sql=sql_text,
                aggregation=key.agg,
                alias=full_alias,
                model_name=source_relation,
                type=slot_type,
                column_type=col.type,
                filter_sql=filter_sql,
                agg_kwargs=agg_kwargs_str,
                aggregation_def=agg_def,
                time_column=None,
            )
        if isinstance(source, _EXPRESSION_SOURCE_KINDS):
            # DEV-1826: same-model expression source — render the row-level
            # expression to SQL text; every dispatch kind downstream
            # (simple / distinct / percentile / dialect hook / formula
            # ``{value}``) receives it exactly like a derived-column body.
            expr_leaf = expression_source_leaf(source)
            agg_def = self._resolve_aggregation_def(
                key=key, source_model=source_model, src_leaf=expr_leaf,
            )
            sql_text = self._render_expression_source_sql(
                source=source,
                source_model=source_model,
                source_relation=source_relation,
                bundle=bundle,
            )
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
                filter_sql=None,
                agg_kwargs=agg_kwargs_str,
                aggregation_def=agg_def,
                time_column=None,
            )
        raise NotImplementedError(
            f"AggregateKey source {type(source).__name__} not supported.",
        )

    def _build_where_having_from_planned(  # NOSONAR(S3776) — one cohesive pass over filters_by_phase routing each entry to WHERE / HAVING / POST by phase, with the per-carrier (typed vs Mode-A text) rendering and the HAVING grouped-column guard inline. The complexity is pre-existing; DEV-1732 added only the `filters_override` list selection. Splitting the phase routing from the rendering would thread slot_by_key / first_last_state / where_parts / having_parts through helpers without simplifying anything.
        self,
        *,
        planned_query,
        source_relation: str,
        source_model,
        bundle,
        skip_filter_ids: Optional[Set[str]] = None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
        filters_override: "Optional[List[Any]]" = None,
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
    ):
        """``filters_override`` (DEV-1732) replaces ``filters_by_phase`` as the"""

        skip = skip_filter_ids or set()
        slot_by_key: Dict[Any, Any] = {
            s.key: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }
        where_parts: list[str] = []
        having_parts: list[str] = []
        filters = (
            planned_query.filters_by_phase
            if filters_override is None else filters_override
        )
        for fp in filters:
            if fp.id in skip:
                continue
            if fp.phase == Phase.POST:
                continue
            if fp.phase not in (Phase.ROW, Phase.AGGREGATE):
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.10+: unsupported filter phase "
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
                if isinstance(rendered, (exp.And, exp.Or)):
                    rendered = exp.Paren(this=rendered)
                target_parts.append(rendered.sql(dialect=self.dialect))
            elif fp.text is not None:
                # Mode-A filter: qualify bare refs with the source relation; a non-trivial derived reference is
                # inline-expanded and pulls its crossed joins into the FROM.
                target_parts.append(self._enter_mode_a_predicate(
                    sql=fp.text,
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                    location=(
                        f"SlayerModel.filters on model {source_model.name!r}"
                    ),
                ).sql(dialect=self.dialect))
            else:
                raise ValueError(
                    f"FilterPhase id={fp.id!r} has neither expression "
                    f"nor text (planner gap).",
                )

        where_clause = None
        if where_parts:
            where_clause = self._parse_predicate(_SQL_AND_JOINER.join(where_parts))
        having_clause = None
        if having_parts:
            having_clause = self._parse_predicate(_SQL_AND_JOINER.join(having_parts))
        return where_clause, having_clause

    @staticmethod
    def _is_nontrivial_derived(model, name: str) -> bool:
        """True iff ``name`` is a column on ``model`` whose ``Column.sql`` is a"""
        col = next((c for c in model.columns if c.name == name), None)
        return col is not None and col.sql is not None and not _is_trivial_base(
            column=col,
        )

    def _filter_agg_builder(
        self, *, source_model, source_relation: str, bundle,
    ):
        """The WHERE/HAVING aggregate seam (DEV-1763 P-G): render a local"""

        def build(agg_key, slot, having_full_alias) -> exp.Expression:
            if getattr(agg_key.source, "path", ()):
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.12: cross-model aggregate ref in "
                    f"filter (path={agg_key.source.path!r}) routes via the "
                    f"per-plan CTE, not inline HAVING."
                )
            having_kwargs = self._resolve_agg_kwargs_for_key(
                key=agg_key, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            )
            synth = self._build_agg_render_spec_from_planned(
                slot=slot, key=agg_key, source_model=source_model,
                source_relation=source_relation, full_alias=having_full_alias,
                bundle=bundle, resolved_agg_kwargs=having_kwargs,
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
                ),
                cast_column_sql=True,
                paren_comparison_operands=True,
            ),
        )

    def _outer_wrapper_alias_facilities(
        self, *, slot_by_key, cross_model_agg_slot_to_cm, aliases_by_slot_id,
    ) -> AliasFacilities:
        """Precompute the DEV-1503 outer-WHERE slot→qualified-column map as an"""
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
            if isinstance(k, (AggregateKey, TransformKey)):
                return  # inner refs are aggregated / windowed, not grouped
            if isinstance(k, ArithmeticKey):
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
        """DEV-1501 — wrap a no-transform base SELECT in an outer SELECT"""
        public_aliases = _cycle_public_aliases_in_projection_order(
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            aliases_by_slot_id=aliases_by_slot_id,
        )

        outer_select = exp.Select()
        for alias in public_aliases:
            outer_select = outer_select.select(
                exp.Column(this=exp.to_identifier(alias, quoted=True)),
            )
        outer_select = outer_select.from_(
            exp.Subquery(this=base_select, alias=exp.to_identifier(OUTER_WRAP_ALIAS)),
        )

        return self._apply_planned_order_limit(
            select=outer_select,
            planned_query=planned_query,
            source_relation=source_relation,
            slots_by_id=slots_by_id,
            source_model=None,
            bundle=bundle,
            aliases_by_slot_id=aliases_by_slot_id,
        )

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
        env = self._host_base_order_env(
            planned_query=planned_query,
            source_relation=source_relation,
            slots_by_id=slots_by_id,
            source_model=source_model,
            bundle=bundle,
            aliases_by_slot_id=aliases_by_slot_id,
        )
        for order_entry in planned_query.order:
            select = select.order_by(
                resolve_order_term(entry=order_entry, env=env),
            )
        return self._dialect.apply_pagination(
            select, limit=planned_query.limit, offset=planned_query.offset,
        )

    def _host_base_order_env(
        self,
        *,
        planned_query,
        source_relation: str,
        slots_by_id: dict,
        source_model,
        bundle,
        aliases_by_slot_id: Optional[Dict[str, List[str]]],
    ) -> OrderEnv:
        """Name every order slot the base SELECT produces, under the scope the"""
        env = OrderEnv(dialect=self._dialect)
        for order_entry in planned_query.order:
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
    ) -> exp.Expression:
        """How one slot's value is NAMED in the base SELECT."""

        # The hidden key kinds that resolve to a materialised alias are enumerated deliberately, so a hidden aliased ROW
        # slot hits the split-emission branch instead of ordering as an ungrouped bare column.
        _MATERIALISED_ORDER_KINDS = (
            AggregateKey, ArithmeticKey, ScalarCallKey, TransformKey,
        )

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


def _bundle_for_stage(planned_query, bundle, schema_by_name):
    """Pick the per-stage bundle a single DAG stage renders against."""
    ds = (bundle.source_model.data_source if bundle.source_model else "") or "_stage"
    relation = planned_query.source_relation
    if planned_query.render_source_model is not None:
        source = planned_query.render_source_model
    elif relation in schema_by_name:
        source = synthetic_model_from_stage_schema(
            name=relation, schema=schema_by_name[relation], data_source=ds,
        )
    else:
        return bundle
    sibling_schemas = {n: s for n, s in schema_by_name.items() if n != relation}
    return stage_bundle_with_siblings(
        bundle=bundle, source_model=source,
        sibling_schemas=sibling_schemas, data_source=ds,
    )


def generate_planned_stages(
    planned_queries,
    *,
    bundle,
    dialect: str = "postgres",
    projection_aliases: "Sequence[str]" = (),
) -> str:
    """Render a multi-stage DAG (``plan_stages`` output) to one SQL string."""
    if not planned_queries:
        raise ValueError("generate_planned_stages requires at least one stage")
    if len(planned_queries) == 1:
        sql = generate_from_planned(
            planned_queries[0], bundle=bundle, dialect=dialect,
        )
        # Length-fit over-limit projection aliases from the plan-derived canonical keys, not parsed off the SQL —
        # BigQuery can't parse a backticked dotted alias.
        sql = get_dialect(dialect).rewrite_emitted_sql(sql, aliases=projection_aliases)
        maybe_validate_scopes(sql, dialect=dialect)
        return sql

    schema_by_name = {
        p.stage_schema.relation_name: p.stage_schema
        for p in planned_queries
        if p.stage_schema is not None
    }

    # One generation scope spans every stage (shared allocator + rendered-producer map) so hoisted internal CTEs stay
    # globally unique and a shared producer renders once.
    generator = SQLGenerator(dialect=dialect)
    generator.install_generation(reserve=schema_by_name.keys())

    # Hoist each stage's internal CTEs and de-WITH its body into one flat WITH — a nested WITH inside a stage CTE is
    # invalid on T-SQL.
    stage_ctes: List[Tuple[str, exp.Expression]] = []
    root_sql: Optional[str] = None
    for planned in planned_queries:
        stage_bundle = _bundle_for_stage(planned, bundle, schema_by_name)
        stage_sql = generator.generate_from_planned(
            planned, bundle=stage_bundle, reuse_allocator=True,
        )
        if planned is planned_queries[-1]:
            root_sql = stage_sql
            continue
        if planned.stage_schema is None:
            raise ValueError(
                "non-root stage must carry a stage_schema for CTE chaining; "
                f"source_relation={planned.source_relation!r}",
            )
        hoisted, body_sql = generator._split_statement_ctes(stage_sql)
        stage_ctes.extend(hoisted)
        stage_ctes.append((
            planned.stage_schema.relation_name,
            _stage_rename_wrapper(
                planned=planned, stage_sql=body_sql, dialect=dialect,
            ),
        ))

    assert root_sql is not None
    root_ast = sqlglot.parse_one(root_sql, dialect=dialect)

    # The root's own CTEs read FROM the stage relations, so clear them, add the stage CTEs first (dependency order),
    # then re-append the root's.
    existing_with = root_ast.args.get("with_")
    existing_ctes = (
        list(existing_with.expressions) if existing_with is not None else []
    )
    if existing_with is not None:
        root_ast.set("with_", None)

    for name, wrapped in stage_ctes:
        root_ast = root_ast.with_(name, as_=wrapped, dialect=dialect)
    for cte in existing_ctes:
        root_ast = root_ast.with_(cte.args["alias"], as_=cte.this, dialect=dialect)

    sql = root_ast.sql(dialect=dialect, pretty=True)
    sql = get_dialect(dialect).rewrite_emitted_sql(sql, aliases=projection_aliases)
    maybe_validate_scopes(sql, dialect=dialect)
    return sql


def _stage_rename_wrapper(*, planned, stage_sql, dialect):
    """Wrap a rendered intermediate-stage SQL so its output columns are the"""
    return build_flat_rename_wrapper(
        source_relation=planned.source_relation,
        stage_sql=stage_sql,
        expected_columns=[c.name for c in planned.stage_schema.columns],
        dialect=dialect,
    )
