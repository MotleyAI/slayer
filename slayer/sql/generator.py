"""SQL generator — converts a ``PlannedQuery`` to SQL via sqlglot AST.

The generator works exclusively with ``PlannedQuery`` objects (typed value
keys interned into slots, each carrying its resolved expression, join path
and phase). It never looks up model definitions — every referenced model is
already loaded on the ``ResolvedSourceBundle`` it is handed.

Entry points: ``generate_from_planned`` (one stage) and
``generate_planned_stages`` (a multi-stage DAG rendered to one statement).
"""

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
    _FrozenKey,
    _reroot_path_ref,
    column_leaf,
    column_path,
    reroot_aggregate_key,
    substitute_value_keys,
)
from slayer.core.models import Aggregation
from slayer.core.refs import agg_kwarg_canonical_str
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration as _parse_window_duration
from slayer.engine.binding import walk_value_keys
from slayer.engine.column_expansion import (
    _is_trivial_base,
    collect_root_scope_joined_paths,
    expand_derived_refs_sync,
)
from slayer.engine.planned import ValueSlot
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
    RANKED_CTE_PREFIX,
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
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.scope import ScopeFrame
from slayer.sql.scope_check import maybe_validate_scopes
from slayer.sql.stage_wrapper import (
    build_flat_rename_wrapper,
    unmangle_dotted_table_refs,
)




class ResolvedAggKwarg(BaseModel):
    """DEV-1706 — a resolved parametric-aggregation kwarg value (2-kind tag).

    * ``kind="expr"`` — a trusted, scope-resolved sqlglot expression for a
      column-ref kwarg (``ColumnKey`` / ``ColumnSqlKey``). Embedded directly;
      the crossed join registered at spec-build (the DEV-1527 fix).
    * ``kind="str"`` — the legacy canonical-string form (scalars via
      ``agg_kwarg_canonical_str``, existing strings), consumed exactly as
      before: ``_SAFE_AGG_PARAM_RE`` guard + ``_resolve_sql`` (percentile /
      stat) or formula substitution (custom aggregations).
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["expr", "str"]
    value: Union[exp.Expression, str]


class AggRenderSpec(BaseModel):
    """DEV-1452 — typed input record for the dialect-aware aggregation
    helpers (``_build_agg``, ``_build_percentile``, ``_build_stat_agg``,
    ``_build_formula_agg``, ``_resolve_value_sql``, ``_resolve_agg_param``).

    Gives dialect SQL emission a single typed input, decoupled from the
    query representation. Carries exactly the 11 fields the helpers
    empirically read; other measure attributes (``agg_args``,
    ``source_measure_name``, ``distinct``, ``window``, ``user_declared``,
    ``label``, ``filter_columns``) are deliberately NOT carried —
    ``count_distinct`` dispatches on the agg name, and the positional time
    arg for ``first`` / ``last`` is pre-resolved into ``time_column`` at
    spec-build time.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    sql: str | None
    """Column SQL expression (``Column.sql`` or its bare name); ``None`` for
    ``*:count`` (renders as ``COUNT(*)``).

    Typed as ``str | None`` (not ``Optional[str]``) deliberately — the
    field is **required** at construction; the explicit nullable form
    documents that and dodges Sonar's S8396 false-positive on the
    Pydantic-v2 ``Optional[X]``-implies-default-None misconception."""

    name: str
    """Source column name — qualified under ``model_name`` when ``sql`` is
    None or a bare identifier. Empty for star-source aggregates."""

    model_name: str
    """Qualifier for unqualified column refs in ``sql`` / ``filter_sql`` /
    aggregation params — the source relation."""

    aggregation: str
    """Aggregation name (``sum`` / ``count`` / ``percentile`` / …). Empty
    string for the non-aggregation bare-column branch."""

    alias: str
    """Result-column alias used by the filtered first/last ranked-subquery
    bookkeeping (``filtered_rn_map``, ``filtered_match_map`` lookups)."""

    aggregation_def: Optional[Aggregation] = None
    """Custom-aggregation definition (formula + params) for aggregations
    outside the built-in set. ``None`` for built-ins."""

    agg_kwargs: Dict[str, ResolvedAggKwarg] = {}
    """Query-time aggregation parameter overrides as typed 2-kind values
    (DEV-1706 D-I). Column-ref kwargs arrive as ``kind="expr"`` (scope-resolved
    at spec-build); everything else as ``kind="str"``. A bare ``str`` value is
    coerced to ``kind="str"`` by ``_coerce_agg_kwargs`` so direct-construction
    call sites keep working (this also carried the retired ``EnrichedMeasure``
    shim before DEV-1485 deleted it)."""

    @field_validator("agg_kwargs", mode="before")
    @classmethod
    def _coerce_agg_kwargs(cls, v: Any) -> Any:
        """Coerce bare ``str`` kwarg values to ``ResolvedAggKwarg(kind="str")``;
        pass ``ResolvedAggKwarg`` through; leave anything else for Pydantic to
        reject (``bool`` / ``None`` never reach here from spec-build — they raise
        earlier in ``agg_kwarg_canonical_str``)."""
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
    """Column-filter predicate (``Column.filter``) wired in at aggregation
    time; the helpers wrap the aggregate as ``SUM(CASE WHEN <filter> THEN
    <col> END)``."""

    time_column: Optional[str] = None
    """Explicit time column for first/last ranking (overrides the query's
    default). Pre-resolved from ``AggregateKey.args`` for the planner path."""

    type: Optional[DataType] = None
    """Declared outer-result type — when set, callers wrap the final
    aggregate expression in ``CAST AS <type>`` via ``_wrap_cast_for_type``."""

    column_type: Optional[DataType] = None
    """Source column's declared type — wraps the inner (pre-aggregation)
    expression in CAST when the column.sql is a non-bare expression (e.g.
    ``json_extract(...)``). Distinct from ``type`` which wraps the outer
    aggregate."""


def _strip_declared_cast(expr: exp.Expression) -> exp.Expression:
    """Unwrap one declared-type ``CAST`` a derived-column expansion added.

    Used for a ranked aggregate's ORDER BY column. The CAST exists to make a
    PROJECTED value match its declared type; an ordering key is compared only
    to itself, and on SQLite the cast is not merely redundant — ``TIMESTAMP``
    carries numeric affinity, so it truncates every date to its year and ties
    the partition.
    """
    return expr.this if isinstance(expr, exp.Cast) else expr


def _collapses_to_windowed_cte(planned_query) -> bool:
    """Whether this whole plan IS one windowed CTE (DEV-1835 D3).

    The windowed mirror of :func:`_collapses_to_ranked_cte`: True when the only
    thing the plan computes is one duration-windowed aggregate at exactly the
    grain it groups by, with nothing layered on top. A regroup producer for a
    bare or partitioned windowed measure meets this, so it renders as one
    self-contained ``_cm_`` CTE (its grain rows derived inline) instead of a
    ``_base`` + ``_wm_`` + wrapper trio — eliding the deleted ``_wm_`` relation.
    """
    plans = planned_query.windowed_aggregate_plans
    if len(plans) != 1:
        return False
    if (
        planned_query.cross_model_aggregate_plans
        or planned_query.ranked_aggregate_plans
        # DEV-1835 D4 — ROW attaches (a computed-dimension grain) collapse inline:
        # they render as a WITH prelude the hoister lifts flat. A COMBINED attach
        # (union-grain broadcast) still needs the full machinery, so refuse it.
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
        return False
    plan = plans[0]
    if plan.hidden:
        return False
    if len(planned_query.aggregate_slots) != 1:
        return False
    visible_row_ids = {s.id for s in planned_query.row_slots if not s.hidden}
    if set(plan.grain_slot_ids) != visible_row_ids:
        return False
    return set(planned_query.projection) == {
        *plan.grain_slot_ids, plan.aggregate_slot_id,
    }


def _collapses_to_ranked_cte(planned_query) -> bool:
    """Whether this whole plan IS one ranked CTE (DEV-1748 D9).

    True when the ONLY thing the plan computes is one ranked aggregate at
    exactly the grain the plan groups by, with nothing layered on top: no other
    isolated aggregate, no host-local aggregate that would need a ``_base`` of
    its own, no combined expression, no transform, no outer WHERE, no
    pagination, and a projection that is precisely the grain plus the aggregate.

    Under those conditions the ranked CTE's body already produces the plan's
    rows under the plan's names, so emitting ``_base`` + a combined SELECT
    around it adds a ``WITH`` and nothing else. Every clause here is a case
    where it would add something more, and the collapse would silently drop it.
    """
    plans = planned_query.ranked_aggregate_plans
    if len(plans) != 1:
        return False
    if (
        planned_query.cross_model_aggregate_plans
        or planned_query.windowed_aggregate_plans
        # DEV-1835 D4 — ROW attaches (a computed-dimension grain) collapse inline:
        # the nested producers render as a WITH prelude the hoister lifts flat, so
        # no ``_rk_`` relation survives. A COMBINED attach (union-grain broadcast)
        # still needs the full machinery, so refuse it.
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
        return False
    plan = plans[0]
    if plan.hidden or plan.having_filter_ids:
        return False
    if len(planned_query.aggregate_slots) != 1:
        return False
    grain_ids = [m.host_slot_id for m in plan.grain]
    visible_row_ids = [s.id for s in planned_query.row_slots if not s.hidden]
    if grain_ids != visible_row_ids:
        return False
    # HIDDEN row slots are deliberately not a reason to refuse. They are filter
    # scaffolding — a ``WHERE customers.tier = 'gold'`` interns ``tier`` as a
    # hidden ROW slot — and ``_base`` does not project or group by them either:
    # the no-transform aux pass is ``aggregates_only``, and a transform layer is
    # already excluded above. The ranked CTE applies the very same ROW filters
    # (``where_filter_ids``), so the two renderings agree.
    return list(planned_query.projection) == [
        *grain_ids, plan.aggregate_slot_id,
    ]


# ``_wrap_cast_for_type`` / ``_filter_cast_type`` moved to
# ``slayer.sql.render.value_expr`` (DEV-1763 P-G): the filter-CAST policy is now
# renderer-visible, and they are re-exported above so this module's call sites
# and their pinning tests are unchanged.

logger = logging.getLogger(__name__)

# DEV-1317: statistical aggregations (``DISPATCH_STAT`` in ``AGG_REGISTRY``) are
# routed through _build_stat_agg. stddev_samp/_pop and var_samp/_pop are 1-arg;
# corr / covar_samp / covar_pop are 2-arg via the `other=` kwarg. SQLite gets
# these through registered Python UDFs; Postgres/DuckDB/MySQL/ClickHouse use the
# native function emitted via sqlglot transpilation. MySQL has no native CORR /
# COVAR_SAMP / COVAR_POP — _build_stat_agg raises NotImplementedError there,
# mirroring _build_median.
#
# The two-column subset (LHS + `other=` kwarg).
_TWO_ARG_STAT_AGGS: frozenset[str] = frozenset({"corr", "covar_samp", "covar_pop"})

# DEV-1450 stage 7b.13: aggregations dispatched through the built-in
# path (``_build_agg`` -> ``_build_*`` family). A name in this set always
# resolves to a built-in renderer; a name NOT in the set MUST resolve to
# a model-level ``Aggregation`` definition (``SlayerModel.aggregations``)
# or it's a hard error. Model-level overrides for built-in names ARE
# permitted and get threaded into ``AggRenderSpec.aggregation_def`` so
# ``_resolve_agg_param`` honours their default params (CodeRabbit
# fold-in on DEV-1452 PR #144 — the prior "synth adapter doesn't
# propagate aggregation_def for built-ins" TODO is now done).
#
# Name kept as ``_LOCAL_SLICE`` for grep continuity with 7b.8-7b.12
# call sites and tests; the set is no longer local-only.
#
# DEV-1717: bound to the canonical ``BUILTIN_AGGREGATIONS`` enum rather than a
# hand-maintained duplicate. The two allowlists must stay byte-identical — a
# new built-in aggregation added to the enum is dispatched here automatically,
# so they can never silently desync (a lockstep-edit hazard CodeRabbit flagged
# when ``count_distinct_approx`` had to be added to both).
_BUILTIN_BAREARG_AGGS_LOCAL_SLICE: frozenset[str] = BUILTIN_AGGREGATIONS

# DEV-1337: dialects with native single-arg `log10(x)` / `log2(x)`. sqlglot
# normalises both into a generic ``Log(this=Literal(base), expression=arg)``
# AST and re-emits as ``LOG(base, x)`` for almost every dialect, which
# diverges from the recipe formula text and (on dialects without 2-arg
# ``LOG``) can break a previously working call. We rewrite the AST back
# to ``Anonymous(this='log10'|'log2', ...)``; the per-dialect native-alias
# decision is delegated to ``SqlDialect.should_use_native_log`` (DEV-1716).


# Separator used when joining pre-rendered SQL fragments into a conjunctive
# WHERE/HAVING clause; extracted as a constant so Sonar S1192 doesn't flag it
# at every join site.
_SQL_AND_JOINER = " AND "

# DEV-1444: separator used between pretty-printed SELECT projection columns
# (",\n    "). Extracted as a constant so Sonar S1192 doesn't flag every
# join site that follows the same pattern.
_SQL_COL_SEP = ",\n    "

# Repeated SQL keyword fragments — extracted so the same literal isn't
# duplicated across CTE / window emission sites (Sonar S1192).
_SQL_WITH = "WITH "
_SQL_PARTITION_BY = "PARTITION BY "
# Two-space-indented ``SELECT`` head for hand-assembled CTE bodies (shifted /
# consecutive-periods pairs), extracted so the literal isn't duplicated (S1192).
_SQL_SELECT_HEAD = "SELECT\n  "

# Matches safe aggregation parameter values: identifiers, qualified names, numeric literals.
_SAFE_AGG_PARAM_RE = re.compile(
    r'^(?:'
    r'[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*'  # identifier or qualified name
    r'|'
    r'-?\d+(?:\.\d+)?'  # numeric literal
    r')$'
)


# Per bucket granularity: the shift units whose whole-unit offsets map every
# bucket START onto another bucket start, making the outer re-trunc of the
# shifted expression a per-row no-op (DEV-1811 period-boundary fix follow-up).
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
    """Wrap ``sql_str`` in ``CASE WHEN filter_sql THEN ... END`` if a row-level
    filter is set; otherwise pass through unchanged. Used by the dialect-aware
    aggregate builders (``_build_percentile``, ``_build_stat_agg``,
    ``_build_formula_agg``) so that non-matching rows contribute NULL and the
    aggregate skips them.
    """
    if not filter_sql:
        return sql_str
    return f"(CASE WHEN {filter_sql} THEN {sql_str} END)"


def _is_host_grain(key) -> bool:
    """True for an ``AggregateKey`` marked ``grain="host"`` (DEV-1747 D2).

    The marker separates WHERE a value is READ from WHERE it is GROUPED: the
    source ``path`` says the value comes through a join, ``grain="host"`` says
    the aggregate is nonetheless computed per HOST row-group. Such a key
    renders INLINE over the joined relation inside its own scope, rather than
    in a target-rooted CTE that would collapse it to one global value.
    """
    return getattr(key, "grain", "target") == "host"


def _first_bare_column_name(key) -> Optional[str]:
    """Return the leaf name of the first bare column reference inside a
    ROW-phase composite key (DEV-1576 / DEV-1717 error messages).

    Walks ``ArithmeticKey`` operands / ``ScalarCallKey`` args / a
    ``TransformKey`` input for a ``ColumnKey`` / ``ColumnSqlKey`` leaf so the
    "Bare measure name '<col>'" error names the offending column. Returns
    ``None`` when no column ref is found (caller falls back to the alias).
    """

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
    """Validate that a query-time aggregation parameter value is safe for substitution.

    Only allows column names (optionally table-qualified) and numeric literals.
    Rejects arbitrary SQL to prevent injection via formula string substitution.
    """
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






def _cm_plan_identity(*, source_relation: str, plan, agg_slot) -> tuple:
    """The dedup identity for a cross-model CTE.

    Structural, never the sanitised name string: the canonical alias omits the
    aggregate's column filter, and the name is doubly lossy, so either would
    merge plans that must render separately.

    The reroot shape is part of the identity because the two render paths
    produce DIFFERENT join-back pairs and a different aggregate column alias —
    forward uses the canonical alias, rerooted uses the sub-plan's. Sharing a
    CTE across them would join at the wrong grain or read the wrong column.
    The planner interns each key to one slot and emits one plan per slot, so
    two plans cannot collide here today; keeping the shape in the identity
    means a future planner change cannot make that silently wrong.
    """
    return (
        source_relation,
        agg_slot.key,
        plan.rerooted_plan is not None,
    )


def _effective_src_filters(*, planned_query, plan) -> list:
    """``planned_query.filters_by_phase`` as the windowed ``_src`` scope sees it
    (DEV-1732): frame-bound residuals substituted for the host's predicates.

    Returned as ONE list that both ``_resolve_where_filter_joins_via_scope`` and
    ``_build_where_having_from_planned`` consume, so join discovery and
    rendering are structurally guaranteed to agree. Entries whose filter is
    wholly a frame bound need no substitution here — the planner already left
    their ids out of ``plan.where_filter_ids``, and the caller's
    ``skip_filter_ids`` drops them.

    Returns the original list unchanged when the plan carries no rewrites, so a
    query without a split conjunction emits byte-identical SQL.
    """
    rewrites = getattr(plan, "src_filter_rewrites", None)
    if not rewrites:
        return planned_query.filters_by_phase
    by_id = {r.filter_id: r.expression for r in rewrites}
    return [
        fp if fp.id not in by_id
        else fp.model_copy(update={"expression": by_id[fp.id]})
        for fp in planned_query.filters_by_phase
    ]










# DEV-1444: digit-suffix tail patterns for OFFSET / LIMIT, each bounded
# (`\d+`) so neither matches an unbounded run of arbitrary characters.
# LIMIT and LIMIT-OFFSET are split into two separate regexes (rather
# than one with an optional group) so Sonar's S5852 analyzer can
# clearly bound each — the analyzer flags optional-group + greedy-
# quantifier combinations even when both quantifiers are over `\d+`.
# ORDER BY uses a non-regex ``rfind`` strategy below — its tail can
# include arbitrary expressions and a regex would either need an
# unbounded character class (Sonar S5852 polynomial backtracking
# warning) or an artificial length cap.
_TRAILING_OFFSET_RE = re.compile(r"(?is)\s*OFFSET\s+\d+\s*\Z")
_TRAILING_LIMIT_OFFSET_RE = re.compile(
    r"(?is)\s*LIMIT\s+\d+\s+OFFSET\s+\d+\s*\Z"
)
_TRAILING_LIMIT_RE = re.compile(r"(?is)\s*LIMIT\s+\d+\s*\Z")

# A ``Column.sql`` that is just an unqualified identifier — i.e. the column
# renames a physical column rather than computing an expression. Used to
# reserve star-exported physical names against ``_val_<n>`` collisions
# (DEV-1728). Deliberately rejects dots: ``regions.population`` is a crossing
# reference, not a column of the star-projected relation.
_BARE_IDENT_RE = re.compile(r"[A-Za-z_]\w*")


def _apply_joins(*, select, joins):
    """Apply ``(join_expr, on_expr, join_type)`` triples to ``select`` in order,
    returning the joined ``exp.Select``."""
    for join_expr, on_expr, join_type in joins:
        select = select.join(join_expr, on=on_expr, join_type=join_type)
    return select


def _cycle_public_aliases_in_projection_order(
    *, planned_query, slots_by_id, aliases_by_slot_id,
):
    """Public projection aliases in query order, cycling each slot's alias list
    (a slot materialised under several aliases is consumed in order; the last
    repeats once exhausted). Hidden and alias-less slots are skipped."""
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
    """Frozen per-render constants threaded through the transform-chain emitters
    (DEV-1817). ``planned_query`` / ``bundle`` never change within one
    ``generate_from_planned`` call. Held by reference (``Any`` fields, no copy).

    ``regroup_env`` / ``regroup_join_specs`` (DEV-1837 D3) carry the ROW
    regroup producers' placeholder registry and grain-join specs into the
    shifted-CTE emitter, so a computed dimension in the shifted grain resolves
    to its producer column and the shifted FROM carries the producer join."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    planned_query: Any
    bundle: Any
    regroup_env: Any = None
    regroup_join_specs: Any = None


class ChainState(BaseModel):
    """Per-chain-layer accumulators + the layer's source root (DEV-1817).

    Frozen with ``Any`` fields so the contained collections keep their identity:
    ``ctes`` (append), the alias maps (``setdefault``) and ``cte_allocator``
    (``allocate_cte``) are mutated IN PLACE and the driver reads the results
    back. A fresh ``ChainState`` is built at each chain-layer boundary; the
    allocator is INJECTED (plain and cross-model chains use different ones).
    ``source_model`` / ``source_relation`` are the layer root — NOT a per-query
    constant (isolated-plan CTEs re-root to their target)."""

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
        # DEV-1708 (D-E): the generation-wide alias allocator, installed by
        # ``generate_from_planned`` for the duration of one render so inline
        # forward ``_cm_*`` CTEs and the host base share ``_val_<n>`` naming.
        # ``None`` outside a render; direct-call helpers fall back to a local
        # allocator.
        self._gen_allocator: Optional[AliasAllocator] = None

    @property
    def dialect(self) -> str:
        """The sqlglot dialect name. Read-only — derived from
        ``self._dialect.sqlglot_name``. Mutating it would desync the
        strategy object from the string sqlglot consumes (DEV-1716)."""
        return self._dialect.sqlglot_name

    def _new_allocator(self) -> AliasAllocator:
        """Build an ``AliasAllocator`` carrying this generator's dialect
        case-folding policy (DEV-1726): on case-folding dialects the
        ``_taken`` comparison folds, so minted CTE / materialisation names can
        never collide after the backend folds them. The ONLY construction
        site in this module — pinned by test_dev1726_cte_case_folding — so a
        new allocation path cannot silently lose dialect awareness."""
        allocator = AliasAllocator(folds_case=dialect_folds_case(self.dialect))
        # DEV-1824 — the base CTE literals (``_base`` cross-model path, ``base``
        # transform path) are hardcoded, not minted here; reserve them so a
        # hoisted producer's own base (renamed via ``allocate_cte`` in
        # ``_render_producer_split``) never lands back on the consumer's ``_base``.
        allocator.reserve("_base", "base")
        return allocator

    def _join_alias(self, *, root: str, path: Tuple[str, ...]) -> str:
        """Mint the internal JOIN alias for cumulative ``path`` under ``root``
        via the generation-wide registry (DEV-1743). The registry keeps the
        JOIN clause, the dim-column qualifier and the scope anchor agreed on
        one alias per path, length-fits it, and uniquifies a chain leaf vs a
        literal ``__``-named model. Falls back to the legacy ``__``-join only
        when no generation allocator is installed (isolated direct calls)."""
        alloc = self._gen_allocator
        if alloc is None:
            return root if not path else "__".join(path)
        return alloc.alias_for(
            root=root, path=path, limit=self._dialect.max_identifier_bytes,
        )

    def _join_alias_resolver(self, root: str) -> "Callable[[Tuple[str, ...]], str]":
        """A root-bound alias resolver for ``expand_derived_refs_sync`` so a
        derived column's qualifiers match the emitted JOIN aliases (DEV-1743)."""
        return lambda path: self._join_alias(root=root, path=path)

    def _scope_frame(self, *, model, relation, bundle, allocator, attached_columns=None):
        """Build a ``ScopeFrame`` rooted at ``model`` / ``relation`` on the
        injected ``allocator`` (its ``next_scope_id`` mints the scope id).

        ``attached_columns`` (DEV-1825) seeds the regroup placeholder registry so
        a computed dimension over a partitioned aggregate resolves the aggregate
        to its attached producer-CTE column."""
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
        """Reserve every name a ``<relation>.*`` projection of ``model`` can
        export, so a minted ``_val_<n>`` (Law-2 materialisation) never shadows a
        real column (DEV-1728 / Codex F6).

        Both the SEMANTIC name and — when ``Column.sql`` is a bare identifier —
        the PHYSICAL column name are reserved: a star-projection exports the
        physical names, and the two differ whenever a column renames its source
        (``Column(name="value", sql="_val_0")``). A non-bare ``Column.sql`` is an
        expression, not a star-exported column, so it contributes nothing.

        Columns that exist in the database but not on the model are outside what
        SLayer can see without reflection; a physical column literally named
        ``_val_<n>`` is the only way to hit that residual, which the underscore
        prefix makes vanishingly unlikely.
        """
        names: List[str] = []
        for c in model.columns:
            names.append(c.name)
            sql = getattr(c, "sql", None)
            if sql and _BARE_IDENT_RE.fullmatch(sql.strip()):
                names.append(sql.strip())
        allocator.reserve(*names)

    @staticmethod
    def _maybe_quote_ident(ident: Optional[exp.Expression]) -> None:
        """Thin delegator to :func:`slayer.sql.naming.maybe_quote_ident`
        (DEV-1713 D-b: the mixed-case quoting policy is owned by the naming
        module). Kept as a method so existing ``gen._maybe_quote_ident`` call
        sites / tests are unchanged."""
        maybe_quote_ident(ident)

    @staticmethod
    def _quote_mixed_case_identifiers(node: exp.Expression) -> exp.Expression:
        """Thin delegator to
        :func:`slayer.sql.naming.quote_mixed_case_identifiers` (DEV-1713 D-b).
        Kept as a method so ``tree.transform(gen._quote_mixed_case_identifiers)``
        call sites / tests are unchanged. See the naming module for the policy
        (DEV-1645 mixed-case quoting; DEV-1686 reserved-word dependency)."""
        return quote_mixed_case_identifiers(node)

    def _to_ident(self, name: str) -> exp.Identifier:
        """Build a column/table-name identifier, quoting it when mixed-case
        (DEV-1645). Use for real DB column/table names — NOT for aliases or
        qualifiers (those stay unquoted via plain ``exp.to_identifier``, and
        reserved-word aliases quote at emit via ``RESERVED_KEYWORDS``)."""
        ident = exp.to_identifier(name)
        self._maybe_quote_ident(ident)
        return ident

    def _to_table(self, name: str, alias: Optional[str] = None) -> exp.Expression:
        """Build a (possibly schema-qualified) table reference with mixed-case
        physical-name parts quoted (DEV-1645). The ``alias`` is SLayer-internal
        and stays unquoted (a reserved-word alias still quotes at emit through
        ``RESERVED_KEYWORDS`` — DEV-1686)."""
        table = exp.to_table(name).transform(self._quote_mixed_case_identifiers)
        if alias is not None:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
        return table

    def _parse(self, sql: str, *, dialect: Optional[str] = None) -> exp.Expression:
        """Parse ``sql`` via sqlglot, applying SLayer-specific AST rewrites.

        On SQLite, rewrites ``exp.JSONExtract`` to the function-call form so
        ``json_extract(...)`` is preserved (DEV-1331); the default sqlglot
        SQLite emit is ``col -> '$.path'``, which returns the JSON-quoted
        form and silently breaks CASE WHEN / equality matches.

        On every dialect, rewrites ``Log(this=Literal(10|2), expression=X)``
        to ``Anonymous(this='log10'|'log2', ...)`` for backends with native
        single-arg aliases (DEV-1337); sqlglot otherwise canonicalises both
        to ``LOG(base, x)`` and the emitted SQL stops matching the recipe
        formula text.

        Use this in place of ``sqlglot.parse_one(...)`` everywhere inside
        ``SQLGenerator`` so the rewrites fire uniformly across every parse
        site.
        """
        d = dialect or self.dialect
        active = self._dialect if d == self.dialect else get_dialect(d)
        # DEV-1686: quote any reserved-word qualifier/leaf (``grant.id`` →
        # ``"grant".id``) before re-parsing a SLayer-built string, so a bare
        # reserved word does not fail at parse time. No-op for ordinary SQL
        # (only dot-adjacent reserved words are touched) and idempotent on
        # already-quoted identifiers.
        sql = prequote_reserved_identifiers(sql, dialect=d)
        tree = sqlglot.parse_one(sql, dialect=d)
        # DEV-1716: PARSE-dialect keyed AST rewrite (SQLite rewrites
        # JSONExtract to the function-call form — DEV-1331). Default identity.
        tree = active.rewrite_parsed_ast(tree)
        # Log-alias rewrite is multi-dialect; the per-base allowlist check
        # lives inside ``_rewrite_log_aliases`` so unsupported dialects
        # (oracle; tsql for log2) keep the canonical 2-arg LOG form.
        tree = tree.transform(self._rewrite_log_aliases)
        # DEV-1645: quote mixed-case column/table identifiers so case-folding
        # dialects reach the right physical object (see the method docstring
        # for the DEV-1706 pull-forward rationale).
        tree = tree.transform(self._quote_mixed_case_identifiers)
        # DEV-1716: TARGET-dialect keyed AST rewrite (Postgres wraps the first
        # arg of a 2-arg ROUND in a numeric CAST — DEV-1576). Keyed to the
        # generator's target dialect, not the parse dialect.
        return self._dialect.rewrite_target_ast(tree)

    def _parse_predicate(self, sql: str, *, dialect: Optional[str] = None) -> exp.Expression:
        """Parse a bare WHERE/HAVING predicate expression (DEV-1378).

        ``sqlglot.parse_one(sql, dialect=...)`` falls back to a ``Command``
        statement parse when an expression starts with a function name that
        is also a SQL statement keyword in the target dialect — e.g.
        ``replace(x, ',', '')`` on SQLite or MySQL is misinterpreted as
        the ``REPLACE INTO`` statement form. To dodge this, wrap the
        expression in ``SELECT 1 WHERE ...`` and extract the WHERE body —
        sqlglot's expression-context parser then reads ``replace`` as a
        function call.

        Use this in place of :meth:`_parse` for parsing bare expressions
        derived from user-supplied SQL fragments (filter SQL, measure
        ``filter_sql``, etc.) — paths where statement-keyword shadowing is
        possible.
        """
        d = dialect or self.dialect
        active = self._dialect if d == self.dialect else get_dialect(d)
        # DEV-1686: quote reserved qualifiers/leaves before the re-parse (see
        # ``_parse``). No-op for ordinary predicates; idempotent when quoted.
        sql = prequote_reserved_identifiers(sql, dialect=d)
        wrapped = sqlglot.parse_one(f"SELECT 1 WHERE {sql}", dialect=d)
        where = wrapped.args.get("where")
        if where is None or where.this is None:  # pragma: no cover — defensive
            raise ValueError(
                f"Could not extract WHERE predicate from {sql!r} (dialect={d!r})"
            )
        tree = active.rewrite_parsed_ast(where.this)
        tree = tree.transform(self._rewrite_log_aliases)
        # DEV-1645: mixed-case identifier quoting (see ``_parse``).
        tree = tree.transform(self._quote_mixed_case_identifiers)
        return self._dialect.rewrite_target_ast(tree)




    def _quote_ident(self, name: str) -> str:
        """Render ``name`` as ONE dialect-quoted identifier string (DEV-1716).

        Backticks on MySQL/BigQuery, brackets on T-SQL, ANSI double quotes on
        Postgres/SQLite/DuckDB. Replaces raw ``f'"{name}"'`` sites in the
        string-assembled CTE/projection paths so non-ANSI dialects get correct
        quoting in the first place (a terminal string-rewrite can't fix ANSI
        quotes — MySQL re-parses them as string literals). The BigQuery / T-SQL
        alias-mangling ``rewrite_emitted_sql`` post-pass then fires on the
        dotted quoted identifier. Identity round-trip on Postgres/SQLite (still
        ``"name"``), so those emissions are unchanged.
        """
        return exp.to_identifier(name, quoted=True).sql(dialect=self.dialect)

    def _parse_cte_body(self, sql: str) -> exp.Expression:
        """Parse a rendered CTE body back into AST for the WITH assembler.

        The deliberate seam. The CTE renderers still return SQL text, and one of
        them (the re-rooted cross-model CTE) returns a COMPLETE ``WITH … SELECT``
        statement produced by a nested ``generate_from_planned`` — threading AST
        out through that whole pipeline is a larger change than this PR takes on.
        Parsing once here keeps the assembly between scopes on AST, which is
        what the doctrine is about; the alternative was splicing statement text
        into an f-string, which is what it replaced.

        ``sqlglot.parse_one`` rather than :meth:`_parse`: this text is our own
        freshly-emitted output, so it needs no prequoting or derived-ref
        expansion — only structure.
        """
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        # DEV-1824 — repair a BigQuery / T-SQL dotted-alias re-parse so a hoisted
        # producer's ``_base.`orders.region``` references stay bound.
        unmangle_dotted_table_refs(parsed)
        return parsed

    @staticmethod
    def _carry_aliases_in_plan_order(
        aliases_by_slot_id: Dict[str, List[str]],
    ) -> List[str]:
        """Aliases an inner stage carries forward, in PLAN order (B8).

        These lists used to be ``sorted(...)`` — one site still carried the
        comment "matches legacy ``_generate_with_computed:1607``", i.e. it was
        byte-parity ballast rather than a requirement. Alphabetical order is
        unrelated to anything the query means, and it made a step CTE project
        its columns in a different order from the base it selects them from.

        ``aliases_by_slot_id`` is populated as slots are rendered, so its
        insertion order IS the plan's render order; iterating it directly is
        what "plan order" means here.

        A duplicate alias RAISES. Two slots sharing a rendered alias is an
        allocator invariant violation: the old ``sorted(...)`` emitted the
        column twice, which leaves the downstream ``SELECT "x" FROM step1``
        ambiguous, and silently collapsing it instead would change the stage's
        arity while hiding the violation that caused it.
        """
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
        """Build an ``exp.Ordered`` node via the dialect strategy.

        DEV-1747 D5 — the T-SQL ``nulls_first`` pin used to live here, which
        left the combined and transform-chain paths (which build their own
        ``exp.Ordered``) without it. It now lives in ``SqlDialect.build_ordered``
        so every render site gets identical null ordering (P-H).
        """
        return self._dialect.build_ordered(
            order_col, descending=not ascending, nulls=nulls,
        )





    def _build_time_offset_expr(self, col_expr: exp.Expression, offset: int,
                                granularity: str) -> exp.Expression:
        """Apply a time offset to a column expression (dialect-aware).

        In shifted CTEs the caller truncates first, then calls this to offset the
        already-truncated bucket-start by whole calendar units (DEV-1811), and
        re-truncates only when the shift unit may not preserve bucket alignment.
        """
        return self._dialect.build_time_offset_expr(
            col_expr=col_expr, offset=offset, granularity=granularity,
        )

    def _duration_interval_exprs(self, duration: str, sign: int = 1) -> list[exp.Expression]:
        """Return per-unit AST nodes that `_add_intervals_expr` will chain.

        Delegates to the dialect strategy (DEV-1716) — Postgres-shape returns
        ``exp.Interval`` nodes; SQLite returns DATETIME-modifier string
        literals with sign baked in.
        """
        parts = _parse_window_duration(duration)
        return self._dialect.duration_interval_exprs(parts=parts, sign=sign)

    def _granularity_interval_expr(self, granularity: TimeGranularity, sign: int = 1) -> list[exp.Expression]:
        if granularity == TimeGranularity.QUARTER:
            duration = "3m"
        elif granularity in (TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY):
            # DEV-1572: a WEEK_SUNDAY shift spans one calendar week, same as WEEK
            # (only the bucket anchor differs — Sunday vs Monday).
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
        """Compose `expr ± interval [± interval ...]` as AST.

        Delegates to the dialect strategy (DEV-1716) — defaults to chained
        Add/Sub with ``exp.Interval`` nodes; SQLite wraps as ``DATETIME(...)``;
        T-SQL chains ``DATEADD(...)`` calls.
        """
        return self._dialect.add_intervals_expr(
            expr=expr, intervals=intervals, sign=sign,
        )

    def _build_date_trunc(self, col_expr: exp.Expression, granularity: TimeGranularity) -> exp.Expression:
        """Build a DATE_TRUNC expression. Dispatches to the dialect strategy
        (DEV-1716).

        The dialect determines the wire form — DATE_TRUNC for
        Postgres/DuckDB/ClickHouse, STRFTIME for SQLite (with CASE WHEN for
        quarter and weekday-modifier for week), DATETRUNC for T-SQL, native
        Sunday-week for BigQuery. Cast-wrapping of non-column operands and the
        WEEK_SUNDAY day-shift are handled inside the dialect (base) impl.
        """
        return self._dialect.build_date_trunc(
            col_expr=col_expr, granularity=granularity, parse=self._parse,
        )

    def _rewrite_log_aliases(self, node: exp.Expression) -> exp.Expression:
        """Thin delegator to the shared log-alias policy in
        ``slayer.sql.render.value_expr``.

        Kept as a method so the existing ``tree.transform(...)`` call sites,
        which walk every parsed AST so the rewrite survives sqlglot's re-parse
        passes, stay unchanged.
        """
        return rewrite_log_alias(node, dialect=self._dialect)

    def _resolve_sql(
        self,
        sql: Optional[str],
        name: str,
        model_name: str,
        type: Optional[DataType] = None,
    ) -> exp.Expression:
        """Resolve an enriched SQL expression to a sqlglot AST node.

        DEV-1361: when the caller has a typed object in scope (a typed
        slot, a ``Column``), it passes ``type=`` so the
        generator wraps non-trivial expressions in ``CAST(... AS <type>)``.
        Bare identifiers (``sql=None`` or ``sql`` is a single identifier)
        trust the DB schema and sqlglot — no CAST is emitted regardless of
        ``type``.
        """
        if sql is None:
            # DEV-1645: quote the mixed-case column leaf; the model qualifier is
            # a SLayer-internal alias and stays unquoted (reserved names quote
            # at emit).
            return exp.Column(this=self._to_ident(name), table=exp.to_identifier(model_name))
        # Bare column name → qualify with model name
        # Use isidentifier() to distinguish column names from literals (e.g. "1")
        if sql.isidentifier():
            return exp.Column(this=self._to_ident(sql), table=exp.to_identifier(model_name))
        return _wrap_cast_for_type(self._parse(sql), type)

    def _resolve_value_sql(self, spec: AggRenderSpec) -> str:
        """Resolve ``spec.sql`` (or ``spec.name``) into a fully-qualified
        SQL string for the value column. Mirrors what ``_build_agg`` does for
        the standard sum/avg/min/max path so the dialect-aware builders
        (median/percentile/stat-aggs/formula) emit the same qualified
        identifiers.
        """
        return self._resolve_sql(
            sql=spec.sql,
            name=spec.name,
            model_name=spec.model_name,
            type=spec.column_type,
        ).sql(dialect=self.dialect)

    def _agg_param_ast(
        self, value: "ResolvedAggKwarg | str", *, model_name: str,
    ) -> exp.Expression:
        """Resolve a parametric-agg param value to a sqlglot AST.

        DEV-1706 (D-I): a ``ResolvedAggKwarg`` with ``kind="expr"`` is a trusted,
        scope-resolved expression embedded directly; ``kind="str"`` (and a plain
        model-level default ``str``) resolve through ``_resolve_sql`` so bare
        identifiers qualify under ``model_name`` — the pre-DEV-1706 behaviour.
        ``_SAFE_AGG_PARAM_RE`` guarding of ``kind="str"`` query values is applied
        by the callers before this point.
        """
        if isinstance(value, ResolvedAggKwarg):
            if value.kind == "expr":
                # Return a COPY: the same ResolvedAggKwarg (keyed by AggregateKey)
                # is embedded into more than one AST when a C13 slot with two
                # declared aliases visits the same key twice in base_render_order.
                # sqlglot re-parents a node on attach, so sharing the node would
                # corrupt the first tree — mirror ScopeFrame.resolve's .copy()
                # discipline (slayer/sql/scope.py).
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
        """Pull a named aggregation parameter, with query-time SQL-injection
        validation and model-level-default fallback. Returns the SQL string
        with bare identifiers qualified under ``spec.model_name`` (via
        ``_resolve_sql``); qualified names and numeric literals pass
        through unchanged. Raises ``ValueError`` if neither source supplies
        the parameter — reused by ``_build_percentile`` (``p=``) and
        ``_build_stat_agg`` (``other=``); mirrors ``weighted_avg``'s
        ``weight=`` flow.
        """
        value: "ResolvedAggKwarg | str | None" = None
        if name in spec.agg_kwargs:
            value = spec.agg_kwargs[name]
            # Guard the untrusted string forms: a ``kind="str"`` wrapper OR a
            # bare ``str`` (model-level defaults and direct-construction call
            # sites reach here unwrapped). ``kind="expr"`` is a trusted,
            # bind-time-resolved expression and is embedded verbatim.
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
        """Build an aggregation expression from an ``AggRenderSpec``.

        First/last aggregates never reach here — they render as a plan-shaped
        ``RankedAggregatePlan`` CTE (DEV-1748 B9), not through this emitter."""
        if spec is None:  # pragma: no cover — defensive
            raise ValueError("_build_agg requires a 'spec'.")
        agg_name = spec.aggregation
        if not agg_name:
            # Not an aggregation — raw expression
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

        # Classification comes from the single ``AGG_REGISTRY`` table (DEV-1744):
        # a name not registered is a model-level custom aggregation and takes the
        # formula-template path.
        if not is_builtin_agg(agg_name):
            return self._build_formula_agg(spec, agg_name), True

        entry = resolve_agg_entry(agg_name)
        dispatch = entry.dispatch

        # --- Builders that resolve (and filter-wrap) their OWN inner ---
        # These are dialect-dependent or template-based, so they cannot share the
        # plain inner resolution below and run BEFORE it (which also keeps them
        # from triggering its join-discovery side effect).
        if dispatch == DISPATCH_STAT:
            # DEV-1317: SQLite-UDF / native-function / NotImplementedError split.
            return self._build_stat_agg(spec), True
        if dispatch == DISPATCH_FORMULA:
            # ``weighted_avg`` and any other {value}/{param} template built-in.
            return self._build_formula_agg(spec, agg_name), True
        if agg_name == "percentile":
            # Dialect-dependent (no static formula works on
            # SQLite/ClickHouse/MySQL) so it gets its own builder.
            return self._build_percentile(spec), True
        if agg_name == "count_distinct_approx":
            # DEV-1595: native approx-distinct or the exact COUNT(DISTINCT)
            # fallback. A row-level filter wraps as COUNT(DISTINCT (CASE ...)).
            col_expr = _wrap_filter(
                self._resolve_value_sql(spec), spec.filter_sql
            )
            return self._dialect.build_approx_count_distinct(
                col_sql=col_expr, parse=self._parse
            ), True

        # --- Resolve inner expression (SIMPLE / DISTINCT / median paths) ---
        if agg_name == "count" and spec.sql is None:
            # COUNT(*) — if filtered, use COUNT(CASE WHEN filter THEN 1 END)
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

        # --- Apply spec-level filter as CASE WHEN wrapper ---
        if spec.filter_sql and not (agg_name == "count" and spec.sql is None):
            inner_sql = inner.sql(dialect=self.dialect)
            case_sql = f"CASE WHEN {spec.filter_sql} THEN {inner_sql} END"
            inner = self._parse(case_sql)

        # --- count_distinct ---
        if dispatch == DISPATCH_DISTINCT:
            return exp.Count(this=exp.Distinct(expressions=[inner])), True

        # --- median (dialect-dependent) ---
        if agg_name == "median":
            return self._build_median(inner), True

        # --- Standard aggregations (sum, avg, min, max, count) ---
        # ``node_class`` is the sqlglot class the registry entry carries.
        return entry.node_class(this=inner), True

    def _build_formula_agg(self, spec: AggRenderSpec, agg_name: str) -> exp.Expression:  # NOSONAR(S3776) — sequential dispatch over formula source (aggregation_def vs built-in) and per-kind ResolvedAggKwarg substitution (DEV-1527); one cohesive template-substitution contract.
        """Build SQL for formula-based aggregations (weighted_avg, custom)."""
        # Get formula: from aggregation_def or built-in
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

        # Collect param values: query-time overrides > aggregation_def defaults
        param_defaults = {}
        if spec.aggregation_def:
            param_defaults = {p.name: p.sql for p in spec.aggregation_def.params}
        params = {**param_defaults, **spec.agg_kwargs}

        # Validate query-time parameter values to prevent SQL injection. Only the
        # untrusted ``kind="str"`` form is guarded; ``kind="expr"`` is a trusted,
        # bind-time-resolved expression (DEV-1706 D-I).
        for pname, pval in spec.agg_kwargs.items():
            if isinstance(pval, ResolvedAggKwarg) and pval.kind == "str":
                _validate_agg_param_value(pval.value, pname, agg_name)

        # Validate required params
        required = BUILTIN_AGGREGATION_REQUIRED_PARAMS.get(agg_name, [])
        for req in required:
            if req not in params:
                raise ValueError(
                    f"Aggregation '{agg_name}' requires parameter '{req}'. "
                    f"Set it in the model's aggregation definition or at query time "
                    f"(e.g., 'measure:{agg_name}({req}=column)')."
                )

        # Resolve {value} and {param_name} via _resolve_sql so bare identifiers
        # are qualified under spec.model_name (matching the standard
        # sum/avg/min/max path). When the spec carries a row-level filter,
        # wrap row-level references (the value AND any column-ref params) in
        # CASE WHEN so non-matching rows contribute NULL to all terms — but
        # leave literal-default params unwrapped, since `(CASE WHEN ... THEN
        # 100 END)` for a constant `scale=100` would turn it into a row
        # expression and break grouped SQL semantics.
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
        """Build a median aggregation expression. Dispatches to the dialect
        (DEV-1716) — MySQL/T-SQL raise NotImplementedError, SQLite/ClickHouse
        emit ``median()``, others ``PERCENTILE_CONT(0.5)``."""
        return self._dialect.build_median(inner=inner, parse=self._parse)

    def _build_percentile(self, spec: AggRenderSpec) -> exp.Expression:
        """Build a PERCENTILE_CONT(p) aggregation expression (dialect-dependent).

        ``p`` comes from ``spec.agg_kwargs['p']`` (validated against
        SQL injection) or from a model-level ``Aggregation`` default.
        Filter handling mirrors ``_build_formula_agg``: when the spec
        carries a row-level filter, the value column is wrapped in
        ``CASE WHEN ... END`` so non-matching rows contribute NULL and
        are ignored by the aggregate. Both the value column and ``p``
        flow through ``_resolve_sql`` so bare identifiers are qualified
        under ``spec.model_name`` and numeric literals pass through
        unchanged.
        """
        p = self._resolve_agg_param(spec, name="p", agg_name="percentile")
        # `p` must be a numeric literal in [0, 1]. Without this guard a
        # caller could pass `measure:percentile(p=quantity)` (or a model-
        # level default like `p=pg_sleep(10)` that bypasses
        # `_validate_agg_param_value`) and have it flow into
        # PERCENTILE_CONT(p)'s direct-arg slot as a column ref or function
        # call — failing at the backend with a dialect-specific error
        # rather than at SLayer's validation boundary. Closes Codex #3 on
        # PR #82 by catching non-numeric model-level defaults here.
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

        # Pass the **original string** ``p`` (not ``p_float``) to the dialect so
        # user literals like ``0.50`` / ``1`` / ``5e-2`` survive verbatim.
        # DEV-1716: dialect owns the wire form (MySQL/T-SQL raise, SQLite UDF,
        # ClickHouse parametric ``quantile(p)(x)``, others ``PERCENTILE_CONT``).
        col_expr = _wrap_filter(self._resolve_value_sql(spec), spec.filter_sql)
        return self._dialect.build_percentile(
            p_str=p, col_sql=col_expr, parse=self._parse,
        )

    def _build_stat_agg(self, spec: AggRenderSpec) -> exp.Expression:
        """Build SQL for the statistical aggregations added in DEV-1317.

        Handles ``stddev_samp``, ``stddev_pop``, ``var_samp``, ``var_pop``
        (1-arg) and ``corr`` / ``covar_samp`` / ``covar_pop`` (2-arg via
        ``other=`` kwarg). All seven are native on Postgres / DuckDB /
        ClickHouse; ``stddev*`` / ``var*`` are also native on MySQL but
        ``corr`` / ``covar_*`` are not. SQLite gets them via Python UDFs
        registered in ``slayer.sql.sqlite_udfs`` — the UDFs alias
        sqlglot's transpiled names (e.g. ``var_samp`` → ``VARIANCE`` on
        SQLite) so generator output resolves at runtime.

        Both legs flow through ``_resolve_sql`` so bare identifiers are
        qualified under ``spec.model_name`` (matches the standard
        sum/avg/min/max path). Filter handling mirrors
        ``_build_percentile`` / ``_build_formula_agg``: a row-level
        filter wraps the value AND the ``other`` column in
        ``CASE WHEN filter THEN col END`` so non-matching rows
        contribute NULL — which the aggregates skip.
        """
        agg_name = spec.aggregation

        # Resolve the `other=` kwarg before the MySQL guard so that a
        # missing-required-param error takes priority over the
        # MySQL-not-supported error when both conditions hold — the
        # missing-param message points at the actual user mistake. Closes
        # Codex #5 on PR #82.
        # Resolve the `other=` kwarg BEFORE any dialect guard so a
        # missing-required-param error takes priority over a dialect-specific
        # error (the missing-param message points at the actual user mistake).
        other_expr: Optional[str] = None
        if agg_name in _TWO_ARG_STAT_AGGS:
            other_expr = _wrap_filter(
                self._resolve_agg_param(spec, name="other", agg_name=agg_name),
                spec.filter_sql,
            )

        col_expr = _wrap_filter(self._resolve_value_sql(spec), spec.filter_sql)

        # DEV-1716: the dialect owns the wire form — native CORR/COVAR on
        # Postgres/DuckDB/ClickHouse, variance-decomposition formula on
        # MySQL/T-SQL; canonical stddev/var name (sqlglot-transpiled) with the
        # MySQL ``exp.Anonymous`` var_samp/var_pop bypass in the dialect class.
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

    # ------------------------------------------------------------------
    # WHERE / HAVING (filters still use ColumnRef for member resolution)
    # ------------------------------------------------------------------


    # ======================================================================
    # DEV-1450 stage 7b.8 — PlannedQuery → SQL.
    #
    # This entry point consumes the typed PlannedQuery from
    # slayer/engine/stage_planner.py and renders the full pipeline:
    # row-phase dims, local aggregates, Mode-B row filters, ORDER BY /
    # LIMIT / OFFSET, dim-only dedup, plus cross-model, time dimensions,
    # transforms, and aggregate filtering.
    # ======================================================================

    def generate_from_planned(
        self, planned_query, *, bundle, as_cte_body: bool = False,
        reuse_allocator: bool = False, as_ast: bool = False,
        as_hoistable_producer: bool = False,
    ):
        """Render a typed ``PlannedQuery`` to SQL (public entry).

        DEV-1708 (D-E): installs a fresh generation-wide ``AliasAllocator`` for
        the duration of this call and restores the caller's on exit. Inline
        forward ``_cm_*`` CTEs and the host base share this one allocator, so
        their ``_val_<n>`` materialisation names never collide; a recursive
        rerooted sub-generation (``_render_rerooted_cross_model_cte`` →
        ``generate_from_planned``) is a self-contained statement and gets its
        own allocator, with the parent's restored afterwards.

        ``reuse_allocator`` (DEV-1824 / D2) renders against THIS generation's
        allocator instead of a fresh one, so a nested regroup producer's own
        base / step / ``_cm_`` names are globally unique with the parent's — the
        precondition for hoisting the producer's internal CTEs into one flat
        WITH. Byte-identity holds for a producer that mints no names (a plain
        grouped aggregate has no projection boundary and crosses no join), which
        is every pre-DEV-1824 producer.

        ``as_cte_body`` says the result is about to become a CTE DEFINITION
        rather than a statement, which forbids a ``WITH`` of its own (SQL Server
        rejects a nested one outright). Only the caller knows that, so only the
        caller can say it — see :func:`_collapses_to_ranked_cte` for the one
        shape that currently needs it.
        """
        self._assert_projection_is_public(planned_query)
        if reuse_allocator and self._gen_allocator is not None:
            result = self._generate_from_planned_impl(
                planned_query, bundle=bundle, as_cte_body=as_cte_body,
                as_ast=as_ast, as_hoistable_producer=as_hoistable_producer,
            )
        else:
            prev_allocator = getattr(self, "_gen_allocator", None)
            self._gen_allocator = self._new_allocator()
            try:
                result = self._generate_from_planned_impl(
                    planned_query, bundle=bundle, as_cte_body=as_cte_body,
                    as_ast=as_ast, as_hoistable_producer=as_hoistable_producer,
                )
            finally:
                self._gen_allocator = prev_allocator
        # DEV-1824 — the hoist wants the producer AST, not re-parsed SQL text (a
        # round-trip mis-binds a dotted result-key column on BigQuery / T-SQL).
        # Paths that already return an AST hand it back verbatim; a string path
        # is parsed here and repaired defensively.
        if as_ast and not isinstance(result, exp.Expression):
            result = sqlglot.parse_one(result, dialect=self.dialect)
            unmangle_dotted_table_refs(result)
        return result

    @staticmethod
    def _assert_projection_is_public(planned_query) -> None:
        """The renderer-side belt for the public-projection invariant (§5.2).

        ``PlannedQuery`` validates this at construction, but pydantic's
        ``model_copy(update=...)`` skips validators — and rerooting a plan uses
        exactly that. So the ONE place every render path passes through checks
        it again. It RAISES rather than skipping the offending slot: silently
        dropping a column the plan asked for is how a wrong answer reaches a
        user, whereas a raise names the slot.

        This is the only such check left; the defensive ``if slot.hidden:
        continue`` guards the renderers used to carry are redundant now that
        the projection is authoritative.
        """
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
        as_hoistable_producer: bool = False,
    ):
        """Render a typed ``PlannedQuery`` to SQL.

        NOTE (DEV-1716): this is a STAGE renderer — its output feeds
        ``generate_planned_stages``' flat-column stage-schema wrapper, so the
        dialect ``rewrite_emitted_sql`` alias-mangling post-pass is applied by
        the DB-bound terminal (``generate_planned_stages``), NOT here. Mangling
        a stage's column names would break the downstream flat-name binding.

        Reads from typed PlannedQuery fields (``row_slots`` /
        ``aggregate_slots`` / ``filters_by_phase`` / ``order`` /
        ``transform_layers``) and renders through the dialect helpers
        (``_resolve_sql`` / ``_build_agg`` / ``_wrap_cast_for_type`` /
        ``_parse_predicate`` / ``_build_date_trunc``) so dialect-specific
        behavior is emitted consistently across the pipeline.

        Stage 7b.10 adds window-transform rendering: when
        ``planned_query.transform_layers`` is non-empty, the base SELECT
        is emitted as ``WITH base AS (...)``, Kahn-batched step CTEs
        carry the window functions, and an outer wrap projects in
        user-spec order. POST-phase filters that reference transform
        slots wrap as ``SELECT * FROM (...) AS _filtered WHERE ...``.
        ``time_shift`` / ``consecutive_periods`` layers raise
        ``NotImplementedError`` with a ``7b.11`` marker.
        """

        source_model = bundle.source_model
        if source_model is None:
            raise ValueError(
                "generate_from_planned requires bundle.source_model to be set",
            )
        source_relation = planned_query.source_relation

        if as_cte_body and _collapses_to_ranked_cte(planned_query):
            # D9. A re-rooted cross-model CTE renders its sub-plan as a COMPLETE
            # statement and splices it into a CTE body, so a sub-plan that
            # emitted ``_base`` plus a combined SELECT would put a ``WITH``
            # inside a CTE — which SQL Server rejects outright. It never
            # happened before because no sub-plan ever contained an isolated
            # aggregate; a re-rooted first/last is the first one that does.
            #
            # It also never needs to: when the sub-plan's only isolated
            # aggregate IS its answer, at its own grain, the ranked CTE's body
            # and the statement the long way round would produce are the same
            # rows under the same names. So emit it directly.
            return self._render_collapsed_ranked_plan(
                planned_query=planned_query, bundle=bundle,
            )
        if as_cte_body and _collapses_to_windowed_cte(planned_query):
            # DEV-1835 D3 — the windowed mirror of the ranked collapse: a bare /
            # partitioned windowed producer renders as one self-contained ``_cm_``
            # CTE (its grain rows derived inline) instead of a ``_base`` + ``_wm_``
            # + wrapper trio, so no ``_wm_`` relation survives.
            return self._render_collapsed_windowed_plan(
                planned_query=planned_query, bundle=bundle,
            )
        if (
            as_cte_body
            and planned_query.ranked_aggregate_plans
            # DEV-1835 D4 — a ranked producer whose GRAIN is a computed dimension
            # carries a nested LOCAL row attach and renders as a HOISTABLE producer
            # (its internal ``_base`` / ``_rk_`` / row-producer CTEs hoisted flat by
            # ``_render_producer_split``). That is not the re-rooted cross-model
            # sub-plan this residual forbids, so let it through to
            # ``_render_with_cross_model_plans``.
            and not (
                as_hoistable_producer
                and any(
                    r.attach_phase == "row"
                    for r in planned_query.regroup_attach_plans
                )
            )
        ):
            # The residual, made loud. Anything a ranked sub-plan cannot collapse
            # would go through ``_render_with_cross_model_plans`` and emit its own
            # ``WITH`` — a nested one, which SQL Server rejects, and which sqlglot
            # otherwise FLATTENS into the parent chain where the two ``_base``
            # CTEs then collide. Both outcomes are invalid SQL that no unit test
            # reads, so a shape that escapes the collapse must stop here rather
            # than reach a database.
            raise NotImplementedError(
                "A re-rooted cross-model first/last whose sub-plan needs more "
                "than the ranked CTE itself is not yet supported: the sub-plan "
                "renders into a CTE body, which cannot carry a WITH of its own. "
                "Split the measure into an earlier stage, or drop the part of "
                "the query the sub-plan cannot express in one SELECT.",
            )

        # DEV-1825 / DEV-1829 — a ROW regroup attach (computed dimension) renders
        # in the plain base path; a COMBINED regroup attach (partitioned measure)
        # renders through ``_render_with_cross_model_plans`` (the position the
        # DEV-1739 ``CrossModelAggregatePlan`` occupied).
        _row_attaches = [
            r for r in planned_query.regroup_attach_plans
            if r.attach_phase == "row"
        ]
        _combined_attaches = [
            r for r in planned_query.regroup_attach_plans
            if r.attach_phase == "combined"
        ]
        # DEV-1837 (stage 1a) — a ROW attach composes with transform measures in
        # both chains: the plain path joins the producers into ``base``, the
        # cross-model path into ``_base`` (task 3.2), and the step CTEs layer
        # above either. The remaining coexistence deferrals fail closed, each
        # owned by its stage.
        # DEV-1835/1836 — the row-attach × windowed/ranked/cross-model
        # coexistence deferral lifted: a bare windowed / first-last / cross-model
        # measure desugars onto the regroup primitive (its own producer), so a
        # computed dimension coexists with it in one flat WITH.
        if _row_attaches and as_cte_body and not as_hoistable_producer:
            # DEV-1835 D4 — a windowed / ranked producer whose GRAIN is a computed
            # dimension (a band / scalar-expr / rank) carries a nested ROW attach
            # (the dimension's own aggregate) and renders as a hoistable producer
            # body, its internal WITH hoisted into the one flat chain. A genuinely
            # non-hoistable CTE body (a re-rooted cross-model sub-plan) still fails
            # closed (DEV-1838).
            raise NotImplementedError(
                "A row regroup attach (computed dimension) nested in a CTE body "
                "is not yet supported (DEV-1838)."
            )
        _local_combined = [a for a in _combined_attaches if not a.producer_root_model]
        if _local_combined and as_cte_body and not as_hoistable_producer:
            # DEV-1839 D5 — a GENUINELY non-hoistable CTE body (a re-rooted
            # cross-model sub-plan spliced into a CTE, SQL Server's nested-WITH
            # restriction) still fails closed. A union-grain PRODUCER body whose
            # internal WITH is about to be hoisted by ``_render_producer_split``
            # is exempt (``as_hoistable_producer``). DEV-1836 — a target-rooted
            # (cross-model) combined attach renders as before the migration (its
            # producer hoists like the DEV-1739 forward CTE), so it too is exempt.
            raise NotImplementedError(
                "A partitioned-aggregate regroup attach nested in a CTE body is "
                "not yet supported (DEV-1838)."
            )

        if (
            planned_query.cross_model_aggregate_plans
            or planned_query.windowed_aggregate_plans
            or planned_query.ranked_aggregate_plans
            or _combined_attaches
        ):
            return self._render_with_cross_model_plans(
                planned_query=planned_query, bundle=bundle,
            )

        # 7b.10 — fail fast on transform ops this slice does not render
        # (time_shift / consecutive_periods belong to 7b.11). Walks
        # ``transform_layers`` for an explicit op match AND walks every
        # ``TransformKey.input`` reachable from public slots so a
        # ``change`` desugared into ``time_shift`` raises with the same
        # marker.
        self._validate_window_transform_ops_for_7b10(
            planned_query=planned_query,
        )

        slots_by_id = {
            s.id: s
            for s in (
                list(planned_query.row_slots)
                + list(planned_query.aggregate_slots)
                + list(planned_query.combined_expression_slots)
            )
        }

        # 7b.10 — slot key -> id lookup. ``PlannedQuery`` does not carry
        # the ``ValueRegistry``, so the generator builds its own map.
        # Used for resolving ``TransformKey.input`` / ``partition_keys`` /
        # ``time_key`` references to step-CTE aliases.
        slot_id_by_key: Dict[Any, str] = {
            s.key: s.id for s in slots_by_id.values()
        }

        public_proj_set: Set[str] = set(planned_query.projection)
        # 7b.10 / DEV-1501 — base CTE projects hidden slots referenced as
        # transform inputs / partition_keys / time_key / filter operands
        # (AGGREGATE + POST phase) / order targets so step CTEs, HAVING,
        # and the outer ORDER BY can name them. In the NO-transform path
        # we additionally pass ``aggregates_only=True`` so only
        # AggregateKey leaves get pulled in from order/filter walks — a
        # hidden ROW order target (e.g. ``ORDER BY customer_id`` with
        # ``customer_id`` not projected) would otherwise materialise into
        # GROUP BY and silently change query grain. Hidden ROW order
        # targets in the no-transform path keep raising NotImplementedError
        # at the inline ORDER BY render path.
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

        # Build the base SELECT body. ``aliases_by_slot_id`` is a list
        # of full aliases per slot, in projection visit order — needed
        # so duplicate public_aliases on a single interned slot (DEV-1450
        # C13: two declared measures with the same key + different names)
        # survive the CTE chain. ``available_alias_by_slot_id`` is the
        # canonical "pick one" map used by transform-input / time-key /
        # partition-key / order-entry lookups (any alias of the slot
        # refers to the same column value, so any will do).
        # DEV-1825 — render the regroup producers as _cm_ CTEs and their attach
        # registry / join specs before the base SELECT, so the base scope can
        # resolve the substituted placeholders and add the null-safe join.
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

        # Match legacy _generate_base:1375 — dim-only-dedup OR
        # has_aggregation triggers GROUP BY (dim-only emits GROUP BY
        # before LIMIT so unique dim tuples can't silently drop past
        # row N).
        # DEV-1543: distinct_dimension_values=False opts out of the dim-only
        # dedup GROUP BY, emitting raw rows instead of distinct tuples.
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

        # No transforms → existing pre-7b.10 path: apply ORDER/LIMIT
        # directly on the base select. DEV-1501: when the base
        # materialised hidden order/filter aggregate slots (slot ids in
        # ``base_render_order`` not in ``planned_query.projection``),
        # wrap the base in an outer SELECT that trims to the public
        # projection and moves ORDER BY / LIMIT / OFFSET to the outer
        # level — mirrors the transform path's outer wrap shape, minus
        # the step CTE chain.
        if not planned_query.transform_layers:
            public_slot_ids = set(planned_query.projection)
            has_hidden_materialised = any(
                sid not in public_slot_ids for sid in base_render_order
            )
            if has_hidden_materialised:
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
            # DEV-1825 — prepend the regroup producer CTEs (the base FROM reads
            # from them). ``assemble_with_chain`` owns the WITH clause.
            if regroup_ctes:
                final_select = assemble_with_chain(
                    entries=regroup_ctes, final=final_select,
                )
            return final_select if as_ast else final_select.sql(
                dialect=self.dialect, pretty=True,
            )

        # 7b.10 — transform layers present. Build the CTE chain. Bodies stay
        # ``exp.Select`` from renderer to assembler (D8): this chain carries
        # dotted ``<relation>.<alias>`` names throughout, and a render-to-text-
        # and-re-parse seam re-reads one as a multi-part reference on any
        # dialect that mangles dots at emission.
        # DEV-1837 (D4) — ROW regroup producers (and their hoisted internals)
        # join into ``base``, so they enter the chain as real ``CteEntry``s
        # ahead of it and ``base`` declares them as dependencies.
        ctes: List[CteEntry] = [
            *regroup_ctes,
            CteEntry(
                name="base", query=base_select,
                depends_on=[e.name for e in regroup_ctes],
            ),
        ]
        # DEV-1692: collision-safe CTE-name allocator for the whole transform
        # chain. The hoisted time_shift slot alias (``_time_shift_inner``)
        # repeats across arithmetic-wrapped shifts, so two ``shifted_`` /
        # ``sjoin_`` pairs would otherwise share a name (duplicate WITH). Every
        # CTE name is reserved/allocated through this one allocator so the
        # ``step`` / ``shifted_`` / ``sjoin_`` / ``cp_`` families never collide.
        # DEV-1839 — share the generation-wide allocator when one is installed
        # (a hoisted producer render, ``reuse_allocator``) so a nested transform
        # producer's ``step`` names never collide with the parent chain's once
        # both hoist into one flat WITH. A fresh allocator otherwise (top level),
        # byte-identical since ``step`` names are unused before this point.
        cte_allocator = self._gen_allocator or self._new_allocator()
        cte_allocator.reserve(*(entry.name for entry in ctes))
        # Codex (PR #269): also reserve every already-projected column alias's
        # BARE form so a hidden transform alias minted below
        # (``_time_shift_inner`` / ``_consecutive_periods_inner``) can never
        # shadow a real user column of that name — mirrors the legacy path
        # seeding ``base_aliases`` into its allocator.
        _alias_prefix = f"{source_relation}."
        cte_allocator.reserve(*(
            a[len(_alias_prefix):] if a.startswith(_alias_prefix) else a
            for aliases in aliases_by_slot_id.values()
            for a in aliases
        ))
        # "Pick one" map for transform-input / time-key / partition-key /
        # order-entry / POST-filter lookups. Initialised from the first
        # alias of every materialised slot.
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

        pending_layers = list(planned_query.transform_layers)
        step_num = 0
        # 7b.11 — gather a global view of WHERE-able row-phase filters
        # for the shifted CTE (which re-aggregates the source and needs
        # the same WHERE minus BetweenKey date_range filters). Built
        # once outside the loop since the source filters don't change
        # across layers.
        shifted_where_parts, shifted_where_join_paths = (
            self._build_shifted_cte_where_parts(
                planned_query=planned_query,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                regroup_env=regroup_env,
            )
        )
        # Explicit chain tail: the CTE the next transform step reads from,
        # tracked directly rather than as ``ctes[-1]`` so an append elsewhere in
        # the list can never silently retarget where the chain continues.
        chain_tail = ctes[-1].name
        while pending_layers:
            (ready_window, ready_time_shift, ready_cp, not_ready) = (
                self._classify_ready_transform_layers(
                    pending_layers=pending_layers,
                    slots_by_id=slots_by_id,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                )
            )
            if not (ready_window or ready_time_shift or ready_cp):
                pending_ops = [layer.op for layer in pending_layers]
                raise RuntimeError(
                    f"DEV-1450 stage 7b.11: transform layer dependencies "
                    f"could not be resolved; pending ops: {pending_ops!r}.",
                )
            # This chain dispatches window batch first, then the temporal ops
            # (the cross-model chain reverses that order — DEV-1799 unifies it).
            if ready_window:
                chain_tail, step_num = self._emit_window_batch_step(
                    ready_window=ready_window,
                    ctes=ctes,
                    chain_tail=chain_tail,
                    cte_allocator=cte_allocator,
                    step_num=step_num,
                    slots_by_id=slots_by_id,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                    aliases_by_slot_id=aliases_by_slot_id,
                    source_relation=source_relation,
                    planned_query=planned_query,
                )
            chain_tail = self._emit_time_shift_layers(
                ready_time_shift=ready_time_shift,
                chain=chain_state,
                render=render_state,
                shifted_where_parts=shifted_where_parts,
                shifted_where_join_paths=shifted_where_join_paths,
                chain_tail=chain_tail,
            )
            chain_tail = self._emit_cp_layers(
                ready_cp=ready_cp,
                chain=chain_state,
                render=render_state,
                chain_tail=chain_tail,
            )
            pending_layers = not_ready

        # 7b.11 — materialise POST-phase ArithmeticKey / ScalarCallKey slots the
        # user projected but no transform layer rendered (``change`` /
        # ``change_pct`` desugarings), then assemble the chain and outer wrap.
        chain_tail, step_num = self._emit_unmaterialised_post_phase_step(
            ctes=ctes,
            chain_tail=chain_tail,
            cte_allocator=cte_allocator,
            step_num=step_num,
            slot_id_by_key=slot_id_by_key,
            available_alias_by_slot_id=available_alias_by_slot_id,
            aliases_by_slot_id=aliases_by_slot_id,
            source_relation=source_relation,
            planned_query=planned_query,
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

    # -----------------------------------------------------------------
    # Shared transform-chain steps (DEV-1750)
    #
    # The local (``generate_from_planned``) and cross-model
    # (``_render_cross_model_transform_chain``) chains layer the SAME step CTEs
    # over their base: a Kahn batch split, a per-batch window step, per-op
    # temporal emitters, a POST-phase materialisation step, and an identical
    # finalise/outer-wrap tail. These helpers hold those step BODIES once; each
    # chain keeps only its own loop skeleton (they differ solely in the order
    # they dispatch window vs temporal within a batch — DEV-1799 unifies that,
    # which moves cross-model CTE order and re-blesses golden). Extracting the
    # bodies verbatim leaves emitted SQL byte-identical.
    # -----------------------------------------------------------------

    def _classify_ready_transform_layers(
        self,
        *,
        pending_layers,
        slots_by_id,
        slot_id_by_key,
        available_alias_by_slot_id,
    ) -> tuple:
        """Kahn split of ``pending_layers`` into
        ``(ready_window, ready_time_shift, ready_cp, not_ready)`` by dependency
        readiness then op. A dep-blocked layer is ``not_ready`` regardless of op.
        """
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
        """Emit one transform-chain step CTE and advance the chain.

        ``render`` is invoked once per ``slot_entries`` element, in order, and
        each element's alias-map updates happen AFTER its render — so a window
        render sees earlier same-step aliases. ``render`` must not mutate the
        alias maps. Appends the CTE to ``ctes``, updates both alias maps in
        place, and returns ``(new_chain_tail, step_num)``.
        """
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
            # One column per declared name (C13, DEV-1798); the first stays
            # the canonical handle. ``as_`` copies its child, so the rendered
            # node is safely reused.
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
        """Projected POST-phase Arithmetic / ScalarCall slots no transform
        layer rendered.

        ``change(amount:sum)`` lowers to ``amount:sum - time_shift(...)``: the
        time_shift slot is a self-join CTE pair, but the outer ArithmeticKey
        that subtracts them needs its own step CTE. Same shape covers
        ``change_pct`` and any future POST-phase non-transform slot.
        """
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
        """Inner SELECT over the final chain CTE: all carried aliases in PLAN
        order (B8)."""
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
        """One ``step<n>`` CTE for a Kahn batch of window layers, carrying every
        prior alias forward. Delegates to the shared ``_emit_step_cte`` shell.
        Returns ``(new_chain_tail, new_step_num)``."""
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
        """Emit the ``shifted_`` + ``sjoin_`` CTE pair for each ready
        ``time_shift`` layer's slot. Returns the advanced ``chain_tail``."""
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
        """Emit the ``cp_reset_`` + ``cp_value_`` CTE pair for each ready
        ``consecutive_periods`` layer's slot. Returns the advanced ``chain_tail``.
        """
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
        """Materialise projected POST-phase ``ArithmeticKey`` / ``ScalarCallKey``
        slots no transform layer rendered (``change`` / ``change_pct`` desugar to
        an outer subtraction/division over a ``time_shift`` slot; ``cumsum(x)+1``
        is the window analogue). Transform-key slots are materialised by their
        layers, so they are skipped. Delegates to the shared ``_emit_step_cte``
        shell. Returns ``(new_chain_tail, new_step_num)``.
        """
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
        """Assemble the ``WITH`` chain, apply the POST-phase filter wrap, and emit
        the outer projection wrap in user-projection order (per-slot index walks
        C13 duplicate aliases). Returns the finished statement SQL."""
        inner_select = self._inner_select_from_final_cte(
            chain_tail=chain_tail, aliases_by_slot_id=aliases_by_slot_id,
        )
        chain_sql = assemble_with_chain(
            entries=ctes, final=inner_select,
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

        # Outer SELECT in user-projection order (public slots only). Per-slot
        # index walks each slot's public_aliases so duplicate interned names
        # (DEV-1450 C13) both surface in the result.
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

    # -----------------------------------------------------------------
    # Stage 7b.10 helpers
    # -----------------------------------------------------------------

    @staticmethod
    def _validate_window_transform_ops_for_7b10(*, planned_query) -> None:
        """Validate transform-layer op scope.

        7b.11 lifted ``time_shift`` and ``consecutive_periods`` from
        the deferred set — both render through dedicated self-join /
        staged-window CTE pairs. The deferred set is now empty; the
        function stays in place as a safety net for follow-up ops
        added by later slices.

        It also enforces the **composite-input** rule that survives
        from 7b.10:

        * ``time_shift`` requires a slottable leaf input (the legacy
          self-join CTE re-aggregates the source — composite expressions
          would need an inner expression layer).
        * ``consecutive_periods`` accepts a slottable leaf OR a top-level
          comparison ``ArithmeticKey`` (the boolean predicate shape
          ``amount:sum > 0`` is the canonical user form). Other
          composite shapes (numeric subtraction, scalar calls) are
          rejected with a ``composite-input transforms`` marker so the
          test suite's per-op composite assertions pin a unified message.
        """

        # 7b.11 lifted these — placeholder set for future slices.
        deferred: set = set()

        leaf_kinds = (ColumnKey, ColumnSqlKey, AggregateKey, TimeTruncKey)
        # Keep aligned with _emit_consecutive_periods_ctes_for_planned —
        # the renderer dispatches arithmetic ops via render_arithmetic
        # which supports these binary comparisons only.
        _COMPARISON_OPS = {"==", "!=", "<", "<=", ">", ">="}

        def _walk(key) -> Optional[str]:
            if isinstance(key, TransformKey):
                if key.op in deferred:
                    return key.op
                return _walk(key.input)
            if isinstance(key, ArithmeticKey):
                for o in key.operands:
                    found = _walk(o)
                    if found:
                        return found
                return None
            if isinstance(key, ScalarCallKey):
                for a in key.args:
                    if isinstance(
                        a,
                        (TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey, InKey),
                    ):
                        found = _walk(a)
                        if found:
                            return found
                return None
            if isinstance(key, BetweenKey):
                for k in (key.column, key.low, key.high):
                    found = _walk(k)
                    if found:
                        return found
                return None
            if isinstance(key, InKey):
                # DEV-1475: only LHS column can host a deferred transform.
                return _walk(key.column)
            return None

        # Explicit layer ops + composite-input enforcement.
        for layer in planned_query.transform_layers:
            if layer.op in deferred:
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.11: transform op {layer.op!r} "
                    f"(self-join CTE) deferred to a follow-up slice.",
                )
            if layer.op in ("time_shift", "consecutive_periods"):
                # Walk the layer's slot ids and assert their TransformKey
                # inputs satisfy the per-op composite-input rule.
                slots_map = {
                    s.id: s
                    for s in (
                        list(planned_query.row_slots)
                        + list(planned_query.aggregate_slots)
                        + list(planned_query.combined_expression_slots)
                    )
                }
                for sid in layer.slot_ids:
                    slot = slots_map.get(sid)
                    if slot is None or not isinstance(slot.key, TransformKey):
                        continue
                    inner = slot.key.input
                    if isinstance(inner, leaf_kinds):
                        continue
                    if (
                        layer.op == "consecutive_periods"
                        and isinstance(inner, ArithmeticKey)
                        and inner.op in _COMPARISON_OPS
                    ):
                        # Boolean predicate shape — accepted.
                        continue
                    raise ValueError(
                        f"Nesting a transform inside {layer.op!r} "
                        f"(input={type(inner).__name__}) is not supported. "
                        f"Compute the inner transform in an earlier stage of "
                        f"a multi-stage `source_queries` model and reference "
                        f"its output in this stage."
                    )

        # Reachable trees of every slot we'll need to render.
        slots = (
            list(planned_query.row_slots)
            + list(planned_query.aggregate_slots)
            + list(planned_query.combined_expression_slots)
        )
        for slot in slots:
            found_op = _walk(slot.key)
            if found_op is not None:
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.11: transform op {found_op!r} "
                    f"(reached via slot id={slot.id!r}, key="
                    f"{type(slot.key).__name__}) deferred to a follow-up "
                    f"slice.",
                )

    @staticmethod
    def _composite_has_remote_operand(
        *,
        key,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        planned_query,
    ) -> bool:
        """Whether any operand of ``key`` is materialised OUTSIDE the base CTE.

        DEV-1733: a composite whose operands include a CROSS-MODEL aggregate
        (``_cm_`` CTE) or a WINDOWED aggregate (``_wm_`` CTE) cannot render in
        ``_base`` — the operand column is not in that scope. Such composites
        are owned by the combined SELECT instead, which resolves each operand
        to its CTE-qualified column.
        """

        remote_slot_ids = {
            p.aggregate_slot_id
            for p in planned_query.cross_model_aggregate_plans
        } | {
            p.aggregate_slot_id
            for p in planned_query.windowed_aggregate_plans
        }
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
        """Return slot ids the base CTE must project beyond the public
        projection, in deterministic (walk) order (DEV-1839).

        Walks every ``TransformKey`` in ``transform_layers`` for its
        ``input`` / ``partition_keys`` / ``time_key`` deps; walks every
        AGGREGATE- and POST-phase ``FilterPhase.expression`` for
        slot-worthy deps; walks ``OrderEntry.slot_id`` keys when
        ``include_order`` is True. Only ``ColumnKey`` / ``ColumnSqlKey``
        / ``TimeTruncKey`` / ``AggregateKey`` slot ids are returned
        (those that the base CTE renders); transform slot ids are
        excluded since they're materialised in step CTEs.

        DEV-1501: ``aggregates_only=True`` narrows leaf collection to
        ``AggregateKey`` slots ONLY (row leaves on order/filter paths are
        skipped). Used by the no-transform path so that materialising a
        hidden order/filter aggregate does NOT accidentally pull a hidden
        ROW dep into ``base_render_order`` (which would add it to GROUP
        BY and silently change query grain). Composites still recurse so
        their AggregateKey operands surface.
        """

        if aggregates_only:
            base_kinds: Tuple[type, ...] = (AggregateKey,)
        else:
            base_kinds = (ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey)
        # DEV-1839 — insertion-ordered dedup (the ``_collect_from`` walk visits
        # operands in a deterministic order), NOT a set: two same-grain inner
        # aggregates of one transform (``rank(a:sum(pby=x) + b:sum(pby=x))``)
        # would otherwise surface in hash-seed order, making the emitted SQL
        # non-deterministic across processes. Callers only iterate / membership-
        # test against OTHER sets, never set-algebra this result.
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
            # ``aggregates_only`` mode: still SKIP non-aggregate row leaves
            # at the leaf level — but the composite/walker branches below
            # continue to recurse so their nested AggregateKey operands
            # surface.
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
                # DEV-1475: only the LHS column references a slot; the
                # RHS values are bare literals with no slot identity.
                _collect_from(key.column)
                return
            # LiteralKey / StarKey / unknown: nothing to materialise.

        # Transform layer deps.
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

        # Filter deps for AGGREGATE-phase (HAVING) and POST-phase filters
        # (the latter only in the transform path, where
        # ``_render_post_phase_filter_conditions`` actually applies them).
        # A hidden ``revenue:last(...) > 100`` HAVING aggregate needs the
        # same ranked-subquery materialisation as the ORDER BY path, so
        # its AggregateKey must reach ``base_render_order`` alongside the
        # projected and order-only ones (DEV-1501). POST-phase walk is
        # gated on the presence of transforms — POST filters reference
        # ``TransformKey`` and so are planner-unreachable in no-transform
        # queries; walking them anyway would silently materialise their
        # operands without applying the filter (CodeRabbit DEV-1501 PR
        # #159 Group B).
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

        # 7b.10 / DEV-1501 — order hidden refs reach the base CTE so
        # ORDER BY can resolve via materialised aliases. Walk
        # ``OrderEntry.slot_id`` → that slot's key (so any transform /
        # arithmetic inside also surfaces its base deps). In the
        # no-transform path the caller passes ``aggregates_only=True``
        # so hidden ROW order targets are NOT pulled in (they stay
        # inline-rendered or raise NotImplementedError downstream).
        if include_order:
            for oe in planned_query.order:
                slot = slots_by_id.get(oe.slot_id)
                if slot is None:
                    continue
                _collect_from(slot.key)
                # DEV-1733: an order-only COMPOSITE (``a:sum / b:sum``,
                # ``abs(a:sum)``) needs its OWN materialised column, not just
                # its operands — the outer trim wrap orders on a plain quoted
                # alias, so the composite has to exist as a column of the inner
                # SELECT. ``_collect_from`` deliberately recurses past
                # composite nodes (the generator inlines them elsewhere), so
                # the slot id is added here explicitly.
                #
                # Cross-model / windowed composites are EXCLUDED: their
                # operands live in ``_cm_`` / ``_wm_`` CTEs, so the composite
                # is owned by the combined SELECT and rendering it in ``_base``
                # would reference an out-of-scope column. The cross-model
                # renderer routes them via ``outer_composite_slot_ids``.
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
        """A layer is ready when every slot-worthy dep its TransformKeys
        reference (``input`` + ``partition_keys`` + ``time_key``) has
        an alias materialised in a prior CTE.
        """

        slotted_kinds = (
            ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey, TransformKey,
        )

        def _ready(key) -> bool:
            if isinstance(key, slotted_kinds):
                sid = slot_id_by_key.get(key)
                if sid is None:
                    # Not interned as a slot — can be inlined.
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
                # DEV-1475: only LHS column needs slot readiness; RHS
                # values are literals (always ready).
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
        """Resolve every LOCAL aggregate's join-crossing inputs through the host
        ``scope`` (Law 1) — ``scope.resolve`` anchors each ref and registers the
        joins it crosses into ``scope.join_paths``, the side effect that
        base-pulls the crossed LEFT JOIN.

        Three ordered sub-passes over ``base_render_order`` preserve the pre-
        resolver join-registration order (Column.filter → source → kwargs):

        1. **``Column.filter`` predicates** (DEV-1494; replaces
           ``_collect_column_filter_join_paths``). The Mode-A predicate enters
           through the door (``ScopeFrame.enter_predicate``), whose dual-scan
           (raw + inline-expanded, so a placeholder dotted ref that inlines to
           a constant still pulls its join) registers the crossed paths into
           the scope as a side effect.
        2. **derived aggregate SOURCES** (``ColumnSqlKey`` whose ``Column.sql``
           crosses a join — DEV-1502; replaces ``_collect_aggregate_source_
           join_paths``). Discovery only; the render spec re-expands the source.
        3. **column-ref KWARGS** (``weight=<col>`` / ``other=<col>`` — DEV-1527).
           The resolved expression is returned, keyed by ``AggregateKey``
           (frozen/hashable) → ``{kwarg_name: ResolvedAggKwarg(kind="expr")}``,
           for ``_build_agg_render_spec_from_planned`` to embed. Scalar
           kwargs are left out (the spec builder canonical-stringifies them).
        3b. **template-fragment KWARGS** (DEV-1709): user-supplied string
           kwargs and non-overridden model-default ``AggregationParam.sql``
           fragments are scanned for crossed paths (register-only) — the
           fragment text substitutes verbatim into the aggregation template,
           so its joins must be in the FROM.
        4. **first/last explicit TIME ARGS** (``amount:last(customers.signup_at)``
           — DEV-1710). Discovery only; the ranked subquery's ORDER BY re-renders
           the arg from the plan (``RankedAggregatePlan.ranking_time_key``).
           Replaces the legacy ``_collect_joined_paths_for_base`` AGGREGATE arm.
           A path-bearing derived (``ColumnSqlKey``) arg — the DEV-1526 residual
           — is skipped.

        Cross-model aggregates (non-empty ``source.path``) are skipped in every
        sub-pass: their inputs are owned by the per-plan ``_cm_*`` CTE
        (Stage 4 / DEV-1708). Recurses into composite AGGREGATE keys.
        """

        resolved: "Dict[Any, Dict[str, ResolvedAggKwarg]]" = {}

        def _walk(key, fn) -> None:
            if isinstance(key, AggregateKey):
                # DEV-1747 D2 — a HOST-GRAIN aggregate reads through a join but
                # is grouped at the host grain, so it renders INLINE here and
                # its source join has to register like any other crossing input
                # (Law 1). When the caller owns it in a ``_cm_*`` CTE
                # (``skip_cross_model_aggs``) the join belongs to that CTE, not
                # to this base — registering it here would add an unused, and
                # for a one-to-many join cardinality-changing, LEFT JOIN.
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
            # Entering registers the crossed joins on ``scope`` as a side
            # effect (Law 1 / P-A); the AST itself is re-rendered later by the
            # aggregate CASE-WHEN wrapper, so it is discarded here.
            self._enter_mode_a_predicate(
                sql=cfk.canonical_sql, scope=scope,
                location=f"Column.filter on model {scope.root_model.name!r}",
            )

        def _resolve_source(key) -> None:
            # Two shapes, one action. A DERIVED source (``ColumnSqlKey``) may
            # cross inside its ``Column.sql``; a PATH-BEARING one crosses by
            # the path itself, which is what a host-grain aggregate's source
            # does (DEV-1747 D2). Either way the scope only needs the
            # register-only resolve — the render re-expands.
            if isinstance(key.source, ColumnSqlKey) or getattr(
                key.source, "path", (),
            ):
                scope.resolve(key.source)

        def _resolve_kwargs(key) -> None:
            kw: Dict[str, ResolvedAggKwarg] = {}
            for kname, kval in key.kwargs:
                if isinstance(kval, (ColumnKey, ColumnSqlKey)):
                    kw[kname] = ResolvedAggKwarg(kind="expr", value=scope.resolve(kval))
            if kw:
                resolved[key] = kw

        def _resolve_fragment_kwargs(key) -> None:
            # DEV-1709 (PR #271 Codex review): template-fragment kwargs —
            # user-supplied str values and non-overridden model-default
            # ``AggregationParam.sql`` fragments — are substituted into the
            # aggregation template as qualified SQL text, so their crossed
            # joins must register exactly like ``Column.filter`` predicates
            # do (the widened Law-3 trigger isolates on them, and the CTE
            # sub-render lands here). Shared with the ``_cm_*`` CTE path so the
            # two cannot drift apart again (DEV-1745 W2).
            #
            # DEV-1743: keep the RESOLVED (alias-rewritten) fragment per name so
            # the render embeds it (a multi-hop dotted fragment must become its
            # ``__`` join alias, not stay dotted-unbound).
            frags = self._register_fragment_kwarg_joins(
                key=key, scope=scope, model=scope.root_model,
            )
            if frags:
                bucket = resolved.setdefault(key, {})
                for name, ast in frags.items():
                    bucket.setdefault(name, ResolvedAggKwarg(kind="expr", value=ast))

        def _resolve_first_last_time_arg(key) -> None:
            # DEV-1710 Stage 6 — a first/last explicit ranking-time arg
            # (``amount:last(customers.signup_at)``) crosses a join exactly like
            # a source / kwarg does; resolving it through the scope registers
            # that join (Law 1), so the ranked subquery's ORDER BY ref is in the
            # base FROM. Replaces the legacy ``_collect_joined_paths_for_base``
            # AGGREGATE arm. Register-only: the ranked plan carries the resolved
            # ranking time column (``RankedAggregatePlan.ranking_time_key``).
            arg = self._explicit_time_arg_of(key)
            if arg is None:
                return
            # A path-bearing derived (ColumnSqlKey) arg is a hop PAST the target
            # (the DEV-1526 residual the render seam raises on) — skip it here;
            # anchoring against ``source_relation`` would register a bogus join.
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
        """A target-rooted ``ScopeFrame`` built purely to reproduce an anchored
        expression. Its ``join_paths`` are inert — join discovery is owned by a
        separate pass — so every caller discards them; only the re-anchored SQL
        is used. A fresh allocator per frame keeps its scope id generation-local.

        ``attached_columns`` (DEV-1825) seeds the regroup placeholder registry so
        a WHERE/HAVING predicate over a computed dimension resolves its
        partitioned aggregate to the attached producer column."""
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
        """Render ROW-phase computed (expression) dimensions through the HOST
        scope BEFORE the FROM is built, so a join the expression crosses
        registers into ``scope.join_paths`` (DEV-1740). Returns the rendered
        expr per slot id; the base-SELECT branch reads it instead of
        re-rendering."""

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
        """One ``(cte_name, condition)`` per regroup producer. Each host
        partition key renders through ``scope`` (registering any join it crosses)
        and pairs null-safely (P-I) with the producer's grain column.
        ``condition`` is ``None`` for a grand-total producer — an empty grain the
        caller attaches as a CROSS JOIN."""
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
        """Resolve a single LOCAL aggregate's column-ref kwargs
        (``weighted_avg(weight=<col>)`` / ``corr(other=<col>)``) through a fresh
        host ``ScopeFrame`` → ``{name: ResolvedAggKwarg(kind="expr")}`` or ``None``.

        The base SELECT uses the batch ``_resolve_agg_inputs_via_scope`` pass over
        a shared host scope (which also registers the crossed joins). The HAVING
        render path (``render_value_key``) has no such scope, so it
        builds a throwaway one here purely to reproduce the SAME anchored kwarg
        expression the SELECT emits — the crossed join is already base-pulled
        (the HAVING aggregate is also a ``base_render_order`` slot), so the
        throwaway scope's own ``join_paths`` are intentionally discarded.
        """

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
        """Build the base SELECT (sqlglot ``Select``) for ``generate_from_planned``.

        Iterates ``base_render_order`` (public projection followed by
        aux materialisation slot ids), rendering each ROW / AGGREGATE
        slot. POST-phase slots are skipped — step CTEs render them.

        Returns ``(base_select, aliases_by_slot_id, has_aggregation,
        group_by_keys)``. ``aliases_by_slot_id`` is a list per slot to
        preserve duplicate public aliases (DEV-1450 C13).

        DEV-1450 stage 7b.12: joined ROW slots (ColumnKey.path != ()
        and TimeTruncKey.column.path != ()) are rendered by walking
        the bundle's join graph and emitting ``LEFT JOIN`` clauses in
        the FROM. ``skip_cross_model_aggs=True`` is passed by the
        cross-model orchestrator so the ``_base`` CTE omits AGGREGATE
        slots that live in a per-plan ``_cm_*`` CTE.
        """

        # DEV-1706 Stage 2: the host base is a single scope; every join-crossing
        # ref registers its path into ``host_scope.join_paths`` as a side effect
        # of being resolved through the scope (Law 1 — discovery can never be
        # forgotten). The legacy join collectors are gone: their work is now the
        # scope passes below. The scope's ordered ``join_paths`` reproduce the
        # collectors' first-seen registration order — derived dims → WHERE filters
        # → Column.filter → source → kwargs → first/last time args — so the base
        # FROM is byte-identical.
        #
        # Stage 2's host base has no projection boundary, so the allocator mints
        # no ``_val_`` names here and a local instance suffices; the generation-
        # wide allocator (D-E) arrives with the CTE scopes in Stage 4.
        #
        # Walk row slots for every joined DIMENSION path first (join-order
        # position 1); the scope's paths (derived dims, filters, aggregate
        # inputs, and — DEV-1710 Stage 6 — first/last time args) append after it.
        needed_join_paths = self._collect_joined_paths_for_base(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            order_slot_ids=[e.slot_id for e in planned_query.order],
        )
        # DEV-1708 (D-E): share the generation-wide allocator so host-base and
        # per-plan ``_cm_*`` CTE ``_val_<n>`` names are globally unique.
        host_allocator = self._gen_allocator or self._new_allocator()
        host_scope = self._scope_frame(
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=host_allocator,
            attached_columns=regroup_env,
        )
        # Pre-expand derived (ColumnSqlKey) ROW + TIME dimensions: inline
        # sibling/joined derived refs (DEV-1333 / DEV-1410) and register any
        # joins their SQL crosses into the scope (position 2). Returns the
        # expanded-expr-by-slot-id map the render branch reads.
        derived_expr_by_sid = self._expand_derived_row_dims(
            base_render_order=base_render_order, slots_by_id=slots_by_id,
            source_relation=source_relation, source_model=source_model,
            bundle=bundle, scope=host_scope,
            order_slot_ids=[e.slot_id for e in planned_query.order],
        )
        # DEV-1740: pre-render computed (expression) dimensions through the
        # HOST scope (position 2.5) so a join their expression crosses is
        # registered before the FROM is built — a throwaway frame here dropped
        # the customers join for ``upper(customers.tier)``.
        computed_dim_expr_by_sid = self._render_computed_dims_via_scope(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            scope=host_scope,
        )
        # DEV-1825 (position 2.6) — resolve each regroup producer's host-side
        # partition keys through the scope so any join they cross (e.g.
        # partition_by=customers.region_id) is registered before the FROM is
        # built. The rendered host expressions become the attach join ON operands.
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=host_scope,
        )
        # WHERE-phase filters referencing joined columns (direct, derived, or
        # Mode-A ``__`` paths) register their joins into the scope too (position
        # 3). Filters routed to a cross-model ``_cm_*`` CTE (``skip_filter_ids``)
        # are applied there, not on ``_base`` — registering their join here would
        # add an unused (and, for one-to-many joins, cardinality-changing) LEFT
        # JOIN.
        self._resolve_where_filter_joins_via_scope(
            planned_query=planned_query, scope=host_scope,
            skip_filter_ids=skip_filter_ids,
        )
        # Every LOCAL aggregate's join-crossing inputs resolve through the scope
        # next: ``Column.filter`` (position 4; DEV-1494), derived aggregate SOURCE
        # (position 5; DEV-1502), column-ref KWARGS (position 6; DEV-1527 —
        # ``weighted_avg(weight=<col>)`` / ``corr(other=<col>)``, whose resolved
        # expression is embedded verbatim (``kind="expr"``) into the render spec,
        # replacing the ``agg_kwarg_canonical_str`` round-trip that collapsed a
        # derived column to a bare, non-existent name), and first/last explicit
        # TIME ARGS (position 7; DEV-1710 — ``amount:last(customers.signup_at)``).
        resolved_agg_kwargs = self._resolve_agg_inputs_via_scope(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            scope=host_scope,
            skip_cross_model_aggs=skip_cross_model_aggs,
        )
        # Merge the scope's registered paths (positions 2-7, in first-seen order)
        # after the dimension paths (position 1) → byte-identical FROM.
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
            # DEV-1450 stage 7b.12: joined ROW slots emit the FULL
            # dotted result-key form (``orders.customers.region_id``).
            # The planner emits a flat ``customers__region_id``
            # declared_name for downstream stage binding (DEV-1449 / C4
            # contract), but the public projection alias must preserve
            # the dotted path for the result-key contract (P10). Local
            # slots keep the existing ``<source_relation>.<alias>``
            # form.
            full_alias = self._full_alias_for_slot(
                slot=slot,
                source_relation=source_relation,
                alias_index=alias_index,
            )

            if slot.phase == Phase.ROW:
                key = slot.key
                if isinstance(key, ColumnKey):
                    # DEV-1824 — a bare partitioned aggregate used as a computed
                    # dimension (``amount:last(partition_by=region)``) substitutes
                    # to a bare placeholder ColumnKey ROW slot; it resolves to its
                    # row-attach producer column (``regroup_env``), not a model
                    # column, and the query GROUPs BY that attached value.
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
                    # A derived column (``Column.sql`` set) used as a dimension,
                    # e.g. ``ratio = A.bar / B.foo_normalized`` (cross-table) or
                    # ``c2 = c1 * 2`` (sibling-derived chain). Local
                    # (``path == ()``) derived columns are pre-expanded above
                    # (sibling/joined refs inlined, joins pulled in); fall back
                    # to the non-expanded resolution for any other shape.
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
                    # DEV-1740: a computed (expression) dimension — rendered
                    # through the host scope in the pre-pass above (so a
                    # crossed join reached the FROM); GROUP BY the expression.
                    dim_expr = computed_dim_expr_by_sid[sid]
                    select_columns.append(dim_expr.copy().as_(full_alias))
                    group_by_keys.setdefault(sid, dim_expr)
                    _record_alias(sid, full_alias)
                elif isinstance(key, (ScalarCallKey, ArithmeticKey)):
                    # DEV-1576 / DEV-1717: a ROW-phase composite here is a
                    # non-aggregating measure expression (a bare column, or
                    # arithmetic / scalar-call over bare columns such as
                    # ``round(amount, 2)`` / ``abs(amount)`` / ``amount + 1``).
                    # Dimensions are ColumnKey / TimeTruncKey / ColumnSqlKey /
                    # a computed dimension (handled above); the only way to reach
                    # here with a composite key is a measure that never
                    # aggregates. Raise the same actionable "Bare measure name"
                    # error the enrich_query path raises rather than leaking an
                    # internal NotImplementedError.
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
                    # AGGREGATE-phase composite (arithmetic / scalar-call of
                    # aggregates, e.g. ``expensenet:avg + benchmarkexp:avg``).
                    # Render inline; cast the whole composite once. DEV-1527:
                    # thread the host scope's resolved column-ref kwargs so a
                    # crossing derived kwarg inside a composite operand
                    # (``amount:weighted_avg(weight=<derived>) + quantity:sum``)
                    # embeds its expanded join-anchored expression instead of a
                    # bare, non-existent name.
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
                        # Owned by a per-plan ``_cm_*`` CTE — target-rooted or
                        # (DEV-1747 D2) host-rooted. Skip in the host base.
                        continue
                    if not _is_host_grain(key):
                        raise NotImplementedError(
                            f"DEV-1450 stage 7b.12: cross-model aggregate "
                            f"(source.path={agg_path!r}) reached the local "
                            f"base SELECT path. The cross-model orchestrator "
                            f"should have routed this through `_render_with_"
                            f"cross_model_plans`."
                        )
                    # DEV-1747 D2 — a HOST-GRAIN aggregate inside its own CTE:
                    # the crossed join is already in this scope's FROM, so the
                    # aggregate renders inline over the joined relation and
                    # GROUPs at the query grain. This is the base-pull the
                    # recursion guard exists to reach.
                # DEV-1450 stage 7b.12: ``column_filter_key`` is now
                # propagated into the synthetic ``AggRenderSpec``'s
                # ``filter_sql`` field so ``_build_agg`` wraps the
                # aggregate as ``SUM(CASE WHEN <filter> THEN col END)``.
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
                # POST-phase slot in projection — handled by step CTEs.
                # Don't add to base select; step CTE will materialise.
                continue

        base_select = exp.Select()
        for col in select_columns:
            base_select = base_select.select(col)
        base_select = base_select.from_(from_clause)
        base_select = _apply_joins(select=base_select, joins=base_joins)
        # DEV-1825 — attach each regroup producer CTE on its partition grain
        # (null-safe LEFT JOIN, or a single-row CROSS JOIN for a grand-total
        # producer). Cardinality-preserving: the producer is grouped by exactly
        # these keys, so at most one row joins per null-safe key tuple.
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
        """The explicit positional ranking-time arg of a ``first`` / ``last``
        aggregate, or ``None``.

        The SINGLE arg-selection contract shared by the two sites that must
        never disagree on WHICH positional arg is the time column (DEV-1710 /
        Codex F1): the ranked-plan builder in ``slayer/engine/ranked_planner.py``
        and the join-discovery pass in ``_resolve_agg_inputs_via_scope``. Returns
        the FIRST positional arg iff it is a ``ColumnKey`` / ``ColumnSqlKey``;
        ``None`` for a non-first/last agg, empty args, or a first positional arg
        of any other type (first/last never takes a leading non-column
        positional).
        """

        if key.agg not in ("first", "last"):
            return None
        for a in key.args:
            return a if isinstance(a, (ColumnKey, ColumnSqlKey)) else None
        return None

    def _composite_agg_builder(
        self, *, slot, source_model, source_relation: str, bundle,
        resolved_agg_kwargs,
    ):
        """The AGGREGATE-phase composite seam (DEV-1763 P-G): render one
        aggregate LEAF of a composite inline via the same synth + ``_build_agg``
        path the single-aggregate branch uses. ``render_value_key`` owns the
        composite STRUCTURE (arithmetic / scalar calls); only the aggregate leaf
        needs the generator's spec builder + resolved column-ref kwargs. The
        live base-SELECT site threads no rn-state (that is the dead first/last
        path); the ``__op__`` placeholder alias is inert without it."""

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
        """Render one duration-windowed-measure CTE (DEV-1714 Stage 10).

        ``base_relation`` (DEV-1835 D3) supplies the grain-rows relation aliased
        ``_base`` — a self-contained producer collapse passes its own grain
        subquery so no separate ``_base`` / ``_wm_`` CTE pair is emitted; the
        default ``exp.Table('_base')`` is the pre-migration host-rooted form.

        ``regroup_env`` / ``regroup_join_specs`` (DEV-1835 D4) make the ``_src``
        subquery regroup-aware: a computed-dimension grain key resolves through
        ``render_value_key`` (so a ``__regroup__`` placeholder inside it anchors
        to its producer column) and the nested ROW producers LEFT JOIN into
        ``_src``, exactly as the shifted-CTE emitter does.

        The CTE is host-rooted: ``FROM _base LEFT JOIN (<_src>) AS _src`` where
        ``_src`` self-selects the host rows (dims → ``_w_dim_<n>``, other time
        dims date-trunc'd → ``_w_td_<n>``, the raw window time column →
        ``_w_time``, the value → ``_w_value``), and the join predicate pairs the
        grain equalities with the trailing ``INTERVAL`` range
        (``_src._w_time >= bucket_end - window`` / ``< bucket_end``). The result
        is grouped at the host grain and LEFT-JOINed back to ``_base`` by the
        caller. Returns ``(cte_sql, grain_aliases)``.
        """

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
        # Grain operand pairs for the inner ``_base``↔``_src`` correlation. They
        # go through the same builder as the outer join-back (P-I) rather than
        # calling ``build_null_safe_eq`` per pair here, so both sites share one
        # answer to "how is a grain compared" — including whatever a dialect
        # later needs that a bare per-pair equality could not express.
        grain_pairs: List[Tuple[exp.Expression, exp.Expression]] = []
        grain_aliases: List[str] = []

        # Query dimensions → ``_w_dim_<n>`` (Law-1 resolve registers crossed
        # joins into ``src_scope``).
        for idx, sid in enumerate(plan.dimension_slot_ids):
            dslot = slots_by_id.get(sid)
            base_alias = _alias_of(sid)
            # DEV-1835 D4 — a computed-dimension grain (band / scalar-expr /
            # rank) is a ScalarCallKey / ArithmeticKey / TransformKey, not a bare
            # column: render it through the full value-key renderer so a
            # ``__regroup__`` placeholder inside it resolves via ``regroup_env``.
            expr = render_value_key(
                key=dslot.key,
                ctx=RenderContext(scope=src_scope, dialect=self._dialect),
            )
            src_cols.append(expr.as_(f"_w_dim_{idx}"))
            grain_pairs.append(
                (_src_col(f"_w_dim_{idx}"), _base_col(base_alias)),
            )
            grain_aliases.append(base_alias)

        # Non-window time dimensions → ``_w_td_<n>`` (date-trunc'd), equality-
        # joined so the trailing window does not fan out across their values.
        for idx, sid in enumerate(plan.other_time_dimension_slot_ids):
            tslot = slots_by_id.get(sid)
            base_alias = _alias_of(sid)
            # Codex#1: register any join the time column crosses into the scope
            # (a joined time dimension would otherwise reference an unbound alias
            # in _src); the expression itself comes from the is_root/derived-aware
            # helper below.
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

        # The window time dimension's RAW column → ``_w_time`` (the range axis).
        wtd_slot = slots_by_id.get(plan.window_time_dimension_slot_id)
        wtd_alias = _alias_of(plan.window_time_dimension_slot_id)
        # Codex#1: register the window time column's crossed join (if any) too.
        src_scope.resolve(wtd_slot.key.column)
        raw_time = self._raw_time_col_expr_for_planned(
            time_column=wtd_slot.key.column, source_model=source_model,
            source_relation=source_relation, bundle=bundle,
        )
        src_cols.append(raw_time.copy().as_("_w_time"))
        grain_aliases.append(wtd_alias)

        # The measure value → ``_w_value`` (CASE-wrapped by ``Column.filter``).
        val_expr = src_scope.resolve(key.source)
        if key.column_filter_key is not None:
            pred_sql = src_scope.resolve_predicate_sql(
                key.column_filter_key.canonical_sql,
            )
            val_expr = exp.Case(
                ifs=[exp.If(this=self._parse_predicate(pred_sql), true=val_expr)],
            )
        src_cols.append(val_expr.as_("_w_value"))

        # WHERE-phase row filters (model + user) inherited into ``_src``, minus
        # their frame bounds (``plan.where_filter_ids`` /
        # ``plan.src_filter_rewrites``, DEV-1714 + DEV-1732). ONE effective list
        # feeds both join discovery (Law 1) and rendering, so the two can never
        # disagree about what this CTE contains.
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

        # DEV-1835 D4 — resolve the nested ROW producer join conditions through
        # ``src_scope`` (registering any join a host partition key crosses)
        # BEFORE the FROM is built, then LEFT / CROSS JOIN the producers into
        # ``_src`` so a placeholder grain key anchors to its producer column.
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=src_scope,
        )

        # ``_src`` FROM + joins from the scope's discovered paths.
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

        # Trailing-window range predicate: ``_src._w_time`` in
        # ``[bucket_end - window, bucket_end)`` where ``bucket_end`` is the
        # host bucket's exclusive upper edge (grain + 1 grain).
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
        # An empty grain yields ``None`` here — the windowed measure is scalar
        # over the whole host, so the ON carries only the range bounds. That is
        # NOT the builder's CROSS-JOIN case: the range predicates still
        # correlate the two sides, so this stays a LEFT JOIN either way.
        grain_condition = build_grain_joinback_condition(
            pairs=grain_pairs, dialect=self._dialect,
        )
        on_range = exp.and_(
            *([grain_condition] if grain_condition is not None else []),
            exp.GTE(this=src_w_time, expression=lower_bound),
            exp.LT(this=src_w_time.copy(), expression=bucket_end.copy()),
        )

        # Registry lookup, not a silent catch-all: the previous
        # ``exp.Sum if agg == "sum" else exp.Avg`` rendered ANY other
        # aggregation as AVG. Unreachable through the planner, which gates
        # windowed measures to sum/avg — which is exactly why it would have
        # stayed wrong. Now it raises.
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

        # Returned as AST: the caller assembles the WITH chain structurally.
        # Rendering here and re-parsing later would re-introduce the very
        # corruption B2 removed — a dotted public alias round-trips through
        # text as a multi-part reference on BigQuery.
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
        """One value expression anchored in a ranked CTE's own scope.

        Built through the SAME helpers the host ``_base`` uses, which is what
        makes a grain member compare equal to the ``_base`` column it joins back
        to: two spellings of "the same dimension" that differ only in a CAST
        stop being the same value the moment a dialect rounds them differently.

        ``cast_derived=False`` for the RANKING column, which is compared only to
        itself and so needs no agreement with anything. It also must not carry
        the declared-type CAST: SQLite's ``TIMESTAMP`` has numeric affinity, so
        ``CAST(DATE(created_at) AS TIMESTAMP)`` truncates every date to its year
        and ties the whole partition.

        Registering the joins the expression crosses into ``scope`` is a side
        effect here rather than a separate pass (Law 1), so the CTE's FROM can
        never be missing one.
        """

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
        """The value a ranked aggregate picks, anchored in its own scope.

        Deliberately NOT ``_build_agg_render_spec_from_planned``: that builder
        resolves an explicit time arg on the way past, and the ranking column is
        plan data now (``RankedAggregatePlan.ranking_time_key``). Going through
        it would re-derive at render time the one thing the plan exists to
        decide — and would keep the residual-path raise alive on a path that no
        longer has the limitation it describes.
        """

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
        """Render one ``_rk_`` ranked (``first`` / ``last``) CTE (DEV-1748, B9).

        Two SELECTs: an inner one that projects the grain, the value and one
        ``ROW_NUMBER`` over the rows this aggregate is allowed to see, and an
        outer one that picks rank 1 per grain. Returns ``(cte_query,
        grain_aliases)`` — the aliases the caller joins back on.

        The whole aggregate lives here, so the host base is untouched: adding a
        ``first`` to a query cannot change what its siblings compute, and the
        rn-suffix scheme that used to disambiguate several rankings sharing one
        scope has nothing left to disambiguate.
        """

        key = agg_slot.key
        if not isinstance(key, AggregateKey):
            raise RuntimeError(
                f"RankedAggregatePlan {plan.aggregate_slot_id!r} references a "
                f"non-aggregate slot.",
            )

        if plan.target_path:
            root_model = bundle.get_referenced_model(plan.root_model)
            if root_model is None:
                raise ValueError(
                    f"Ranked CTE root {plan.root_model!r} is not in the "
                    f"resolved source bundle.",
                )
            root_relation = plan.root_model
        else:
            root_model = host_source_model
            root_relation = host_source_relation

        allocator = self._gen_allocator or self._new_allocator()
        self._reserve_model_column_names(allocator, root_model)

        def _frame(*, attached=None) -> ScopeFrame:
            return self._scope_frame(
                model=root_model, relation=root_relation,
                bundle=bundle, allocator=allocator, attached_columns=attached,
            )

        # Law 2's producer/consumer pair. The inner scope PRODUCES every value
        # the outer one reads, so a value that is both the grain and the ranked
        # expression is materialised once — the two used to keep separate alias
        # maps and project it twice. DEV-1835 D4 — the inner (producing) scope
        # carries the nested ROW producers' env so a computed-dimension grain
        # key resolves its ``__regroup__`` placeholder; the outer scope reads the
        # materialised alias and needs none.
        ranked_scope = _frame(attached=regroup_env)
        cte_scope = _frame()

        # The aggregate in the ranked scope's coordinates. For a target-rooted
        # plan that means stripping the host prefix from the source (and from
        # every embedded ref) in one pass, exactly as the cross-model CTE does.
        local_key = reroot_aggregate_key(key, target_path=plan.target_path)

        grain: List[RankedGrainProjection] = []
        partition_by: List[exp.Expression] = []
        for member in plan.grain:
            host_slot = slots_by_id.get(member.host_slot_id)
            if host_slot is None:
                raise RuntimeError(
                    f"RankedAggregatePlan grain references host slot "
                    f"{member.host_slot_id!r}, which this plan does not carry.",
                )
            if isinstance(
                member.ranked_key, (ScalarCallKey, ArithmeticKey, TransformKey),
            ) or (
                isinstance(member.ranked_key, ColumnKey)
                and member.ranked_key.leaf.startswith(REGROUP_LEAF_PREFIX)
            ):
                # DEV-1835 D4 — a computed-dimension grain (band / scalar-expr /
                # rank) OR a bare partitioned-aggregate placeholder (``ct``)
                # renders through the value-key renderer so a ``__regroup__``
                # placeholder anchors to its nested producer column.
                expr = render_value_key(
                    key=member.ranked_key,
                    ctx=RenderContext(scope=ranked_scope, dialect=self._dialect),
                )
            else:
                expr = self._ranked_scope_expr(
                    key=member.ranked_key, root_model=root_model,
                    root_relation=root_relation, bundle=bundle, scope=ranked_scope,
                )
            # PARTITION BY takes the RAW expression: it is evaluated inside the
            # ranked scope, where the joins it crosses are bound. The outer
            # SELECT takes the materialised alias, because out there they are
            # not.
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

        # DEV-1835 D4 — resolve the nested ROW producer join conditions through
        # the ranked scope (registering any join a host key crosses) BEFORE the
        # FROM is built, then LEFT / CROSS JOIN the producers into the inner
        # select so a computed-dimension grain placeholder anchors to its column.
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=regroup_join_specs, scope=ranked_scope,
        )

        from_expr, joins = self._build_from_and_joins(
            source_model=root_model, source_relation=root_relation,
            joined_paths=ranked_scope.join_paths.as_list(), bundle=bundle,
        )
        inner = exp.Select()
        # A NAMED projection list, never ``<relation>.*`` — the projection
        # boundary (P-B) is what keeps the rank column's name private and what
        # removes the need for a materialiser bolted on outside the scope.
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

        # DEV-1824 — a first/last VALUE is the raw picked column, so its declared
        # temporal type needs no CAST (which SQLite would give numeric affinity,
        # truncating a date to its year); ``_ranked_value_cast_type`` suppresses
        # it while keeping the enforcing cast for a type-changing aggregate.
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
        """Every predicate a ranked CTE applies to the rows it ranks.

        All of them narrow the row set BEFORE the ranking, which is the whole
        difference from the shape this replaces: a filtered first/last used to
        rank every row and mask the non-matching ones with a sentinel rank
        column plus a match flag, because the ranking was shared with the rest
        of the query and could not simply drop rows.

        Entering each through ``scope`` registers the joins it crosses (Law 1),
        so the CTE's FROM is assembled from a set nothing can be missing from.
        """
        parts: List[exp.Expression] = []
        if local_key.column_filter_key is not None:
            cfk_sql = local_key.column_filter_key.canonical_sql
            if cfk_sql:
                parts.append(self._enter_mode_a_predicate(
                    sql=cfk_sql, scope=scope,
                    location=f"Column.filter on model {root_model.name!r}",
                ))
        for filter_text in plan.target_model_filters:
            if not filter_text:
                continue
            parts.append(self._enter_mode_a_predicate(
                sql=filter_text, scope=scope,
                location=f"SlayerModel.filters on model {root_model.name!r}",
            ))

        # Host filters this CTE also evaluates. The two roots need different
        # renderers for the same reason ``_wm_`` and ``_cm_`` do: a host-rooted
        # CTE binds them in the host's own scope (so they render
        # byte-identically to the copy ``_base`` keeps), while a target-rooted
        # one re-anchors each leaf against the target.
        if plan.target_path:
            self._register_routed_filter_joins(
                planned_query=planned_query,
                filter_ids=list(plan.where_filter_ids),
                scope=scope,
                target_path=plan.target_path,
            )
            routed = self._collect_routed_filters(
                planned_query=planned_query,
                filter_ids=plan.where_filter_ids,
                target_relation=root_relation,
                target_model=root_model,
                bundle=bundle,
            )
        else:
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

    def _render_collapsed_ranked_plan(self, *, planned_query, bundle) -> str:
        """Emit a whole plan AS its single ranked CTE body (D9).

        The collapse is not an optimisation. It is what keeps a re-rooted
        cross-model first/last emitting valid SQL Server, where a ``WITH``
        nested inside a CTE definition is rejected outright — see the caller.
        :func:`_collapses_to_ranked_cte` owns the precondition.
        """
        source_model = bundle.source_model
        source_relation = planned_query.source_relation
        plan = planned_query.ranked_aggregate_plans[0]
        slots_by_id = {
            s.id: s
            for s in (
                list(planned_query.row_slots) + list(planned_query.aggregate_slots)
            )
        }
        agg_slot = slots_by_id[plan.aggregate_slot_id]
        # DEV-1835 D4 — nested ROW producers for a computed-dimension grain render
        # as a WITH prelude the hoister lifts flat, so no ``_rk_`` relation
        # survives; a plain ranked plan has none and stays byte-stable.
        regroup_ctes, regroup_env, regroup_join_specs, _ = (
            self._prepare_regroup_attaches(planned_query=planned_query, bundle=bundle)
            if any(r.attach_phase == "row" for r in planned_query.regroup_attach_plans)
            else ([], {}, [], [])
        )
        cte_query, _grain_aliases = self._render_ranked_cte_from_planned(
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
        if regroup_ctes:
            cte_query = assemble_with_chain(entries=regroup_ctes, final=cte_query)
        return cte_query.sql(dialect=self.dialect, pretty=True)

    def _build_windowed_grain_base(
        self, *, planned_query, plan, slots_by_id, aliases_by_slot_id,
        source_model, source_relation, bundle,
        regroup_env=None, regroup_join_specs=None,
    ) -> exp.Select:
        """The grain-rows relation for a collapsed windowed producer (DEV-1835
        D3): ``SELECT <grain> FROM source [joins] [WHERE row filters] GROUP BY
        <grain>`` aliased ``_base``. Derives the visible grain directly from the
        source (all ROW filters, frame bounds included) instead of referencing a
        separate ``_base`` CTE. ``regroup_env`` / ``regroup_join_specs`` (D4) make
        a computed-dimension grain key resolve its ``__regroup__`` placeholder and
        LEFT JOIN the nested ROW producers, so the collapse carries them too."""
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
            # DEV-1835 D4 — a computed-dimension grain (scalar-expr) renders
            # through the value-key renderer, not the bare column resolver.
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
        # All ROW filters gate the visible grain (frame bounds define which
        # buckets exist); the trailing window in the ``_src`` join reaches rows
        # before them via ``plan.where_filter_ids`` (a strict subset).
        self._resolve_where_filter_joins_via_scope(
            planned_query=planned_query, scope=scope, skip_filter_ids=set(),
        )
        where, _having = self._build_where_having_from_planned(
            planned_query=planned_query, source_relation=source_relation,
            source_model=source_model, bundle=bundle, skip_filter_ids=set(),
        )
        # DEV-1835 D4 — resolve the nested ROW producer join conditions (any join
        # a host key crosses registers into ``scope``) before the FROM is built.
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

    def _render_collapsed_windowed_plan(self, *, planned_query, bundle) -> str:
        """Emit a whole plan AS one self-contained windowed CTE body (DEV-1835
        D3) — the windowed mirror of :func:`_render_collapsed_ranked_plan`. The
        grain rows are derived inline (:meth:`_build_windowed_grain_base`), so no
        separate ``_base`` / ``_wm_`` pair is emitted. A computed-dimension grain
        (D4) carries nested ROW producers: they render as a WITH prelude the
        hoister lifts flat, so no ``_wm_`` relation survives. Precondition:
        :func:`_collapses_to_windowed_cte`."""
        source_model = bundle.source_model
        source_relation = planned_query.source_relation
        plan = planned_query.windowed_aggregate_plans[0]
        slots_by_id = {
            s.id: s
            for s in (list(planned_query.row_slots)
                      + list(planned_query.aggregate_slots))
        }
        agg_slot = slots_by_id[plan.aggregate_slot_id]
        aliases_by_slot_id = {
            s.id: [self._full_alias_for_slot(
                slot=s, source_relation=source_relation, alias_index={},
            )]
            for s in slots_by_id.values()
        }
        # DEV-1835 D4 — nested ROW producers for a computed-dimension grain.
        regroup_ctes, regroup_env, regroup_join_specs, _ = (
            self._prepare_regroup_attaches(planned_query=planned_query, bundle=bundle)
            if any(r.attach_phase == "row" for r in planned_query.regroup_attach_plans)
            else ([], {}, [], [])
        )
        grain_base = self._build_windowed_grain_base(
            planned_query=planned_query, plan=plan, slots_by_id=slots_by_id,
            aliases_by_slot_id=aliases_by_slot_id, source_model=source_model,
            source_relation=source_relation, bundle=bundle,
            regroup_env=regroup_env, regroup_join_specs=regroup_join_specs,
        )
        base_subq = exp.Subquery(
            this=grain_base, alias=exp.TableAlias(this=exp.to_identifier("_base")),
        )
        outer, _ = self._render_window_measure_cte_from_planned(
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
            outer = assemble_with_chain(entries=regroup_ctes, final=outer)
        return outer.sql(dialect=self.dialect, pretty=True)

    def _render_with_cross_model_plans(  # NOSONAR(S3776) — orchestration of host ``_base`` CTE + per-plan ``_cm_*`` CTEs + combined SELECT + transform-chain step CTEs + outer ORDER BY/LIMIT wrap. Each block is a coherent compilation stage sharing planned_query / slots_by_id / cma_slot_ids / seen_base_ids state; extracting per-stage helpers would scatter the cross-cutting state.
        self,
        *,
        planned_query,
        bundle,
    ) -> str:
        """Render a ``PlannedQuery`` that carries one or more
        ``CrossModelAggregatePlan`` entries.

        Mirrors the legacy ``_build_combined`` + ``_assemble_combined_sql``
        shape:

        * ``_base`` CTE: host's local row/aggregate slots (joined ROW
          slots LEFT JOINed; cross-model AGGREGATE slots skipped).
        * One ``_cm_<sanitized_alias>`` CTE per plan, rooted at the
          terminal target model (``FROM <target> AS <target>``), with
          target-model filters as WHERE, host-routed filters as WHERE /
          HAVING per ``where_filter_ids`` / ``having_filter_ids``, and
          GROUP BY over the shared-grain slots whose key path matches
          the agg's target path.
        * A ``_combined`` SELECT joining ``_base`` to every ``_cm_*``
          via ``LEFT JOIN`` on the shared-grain aliases (or ``CROSS
          JOIN`` when no shared grain is in play).
        * Outer wrap: ORDER BY / LIMIT / OFFSET applied at the combined
          SELECT, then ``_apply_outer_projection_trim`` reshapes the
          public alias projection to exactly ``planned_query.projection``
          order.

        Transform layers over the combined result render via
        ``_render_cross_model_transform_chain`` (the combined SELECT becomes
        the chain's ``base``).
        """

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

        # The ``_base`` CTE projects host-local ROW slots, joined ROW
        # slots (LEFT JOIN walk), and any LOCAL aggregate slots. Cross-
        # model AGGREGATE slots are skipped — the per-plan ``_cm_*`` CTE
        # owns them. POST-phase slots aren't in scope (no transforms).
        cma_slot_ids = {
            p.aggregate_slot_id for p in planned_query.cross_model_aggregate_plans
        }
        # DEV-1714 Stage 10 — windowed aggregate slots render via their own
        # host-rooted ``_wm_`` range-join CTEs (below); like cross-model slots
        # they are excluded from ``_base`` and joined back on the shared grain.
        windowed_slot_ids = {
            p.aggregate_slot_id for p in planned_query.windowed_aggregate_plans
        }
        # DEV-1748 (B9) — ranked (``first`` / ``last``) aggregate slots render
        # via their own ``_rk_`` CTEs. Same treatment as the two above: out of
        # ``_base``, joined back on the grain. One first/last used to wrap the
        # ENTIRE base in a ranking, which is what made a sibling aggregate's
        # value depend on whether a first/last was in the query at all.
        ranked_slot_ids = {
            p.aggregate_slot_id for p in planned_query.ranked_aggregate_plans
        }

        # DEV-1503 / DEV-1745 (P-D) — the outer combined-SELECT WHERE wrapper is
        # routed by the PLANNER (``_plan_outer_where_filters``), which knows
        # which aggregates were isolated into a CTE with its own root. The
        # generator consumes that decision verbatim: re-walking
        # ``filters_by_phase`` here to rediscover it would be routing policy
        # chosen during emission, and the two could disagree.
        slot_by_key = {s.key: s for s in slots_by_id.values()}

        # DEV-1829 — combined regroup producers. Rendered here as ``_cm_`` CTEs
        # and joined back at the combined SELECT, substituting for the DEV-1739
        # partitioned-measure ``CrossModelAggregatePlan``. Their placeholder
        # slots are excluded from ``_base`` (like isolated aggregate slots) and
        # projected from the producer column instead. Prepared BEFORE the ROW
        # producers so a dual-role aggregate (D10) dedups onto the combined CTE.
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

        # DEV-1835 D10 — index the combined producers by structural identity so a
        # ROW attach computing the same aggregate at the same grain reuses the
        # combined CTE (one producer, both roles) instead of shipping a twin.
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

        # DEV-1824 (task 3.2) — ROW regroup producers (computed dimensions) join
        # into ``_base`` BEFORE aggregation, so their ``_cm_`` CTEs, placeholder
        # env, and grain-join specs are prepared here and threaded into the base
        # build (like the plain-base path does). ``[]/{}/[]`` when the query has
        # no row attach, keeping the pure-combined path unchanged.
        row_regroup_ctes, row_regroup_env, row_regroup_join_specs, reused_cm_ctes = (
            self._prepare_regroup_attaches(
                planned_query=planned_query, bundle=bundle,
                dedup_producers=combined_dedup_index,
            )
            if any(r.attach_phase == "row" for r in planned_query.regroup_attach_plans)
            else ([], {}, [], [])
        )
        isolated_slot_ids = (
            cma_slot_ids | windowed_slot_ids | ranked_slot_ids
            | regroup_placeholder_slot_ids
        )
        outer_where_filter_ids: Set[str] = set(
            planned_query.outer_where_filter_ids,
        )
        outer_where_filters: List = [
            fp for fp in planned_query.filters_by_phase
            if fp.id in outer_where_filter_ids
        ]
        # DEV-1503 (Codex round 2 #1) — composite projection slots whose
        # value-key tree walks an ISOLATED cross-model aggregate must NOT
        # render in ``_base``. Inline rendering pulls the filter-target
        # joins back into the host CTE (the host scope's Column.filter
        # resolve pass) and computes the formula against the host rowset —
        # silently corrupting both aggregates when two filter-target INNER joins
        # intersect to different rows. Route them to the outer combined
        # SELECT where the joined-back ``_cm_*`` columns resolve.
        #
        # Composite (``ArithmeticKey`` / ``ScalarCallKey``) projection
        # slots live in EITHER ``aggregate_slots`` (when the composite
        # contains an aggregate and the planner buckets it as
        # AGGREGATE-phase) OR ``combined_expression_slots`` (transform-
        # adjacent composites). Walk both, but skip pure-leaf
        # ``AggregateKey`` slots — those have their own routing via
        # ``cma_slot_ids`` and ``_cm_*`` CTEs.
        composite_kinds = (ArithmeticKey, ScalarCallKey)
        outer_composite_slot_ids: Set[str] = set()
        # A composite slot routes to the outer combined SELECT when it is
        # referenced by EITHER the public projection OR an ORDER BY entry —
        # a hidden ``ORDER BY <isolated_agg> + <other>`` composite would
        # otherwise fall through to the local order-only path and render
        # inline in ``_base``, re-pulling filter-target joins into the
        # host spine (Codex round 3 #1).
        composite_candidate_ids: Set[str] = set(planned_query.projection)
        for order_entry in planned_query.order:
            composite_candidate_ids.add(order_entry.slot_id)
        for slot in (
            list(planned_query.combined_expression_slots)
            + list(planned_query.aggregate_slots)
            # DEV-1836 — an order-only composite whose ONLY aggregates are
            # cross-model buckets as ROW (its regroup-placeholder operands carry
            # no AggregateKey), so it must be scanned here too or it falls through
            # to the base ROW arm and rejects the placeholder as a bare column.
            + list(planned_query.row_slots)
        ):
            if slot.id not in composite_candidate_ids:
                continue
            if not isinstance(slot.key, composite_kinds):
                continue
            # A computed DIMENSION (its own composite over a regroup placeholder,
            # e.g. a band) is GROUPED in ``_base`` — it is never an outer
            # composite. Only measure / order-target composites route outward.
            if slot.is_dimension:
                continue
            # DEV-1835 D7 — a composite that WRAPS a transform (``change`` /
            # ``change_pct`` desugar to ``x - time_shift(x)``) over a LOCAL
            # combined placeholder is owned by the transform chain: its
            # ``time_shift`` needs a step CTE the inline outer-composite path
            # cannot build. The chain materialises the placeholder in ``base`` and
            # computes the arithmetic in its outer wrap. A transform over a
            # CROSS-MODEL inner stays out of scope (DEV-1836) and must still hit
            # the loud RenderContextMissingFacilityError, so only skip when a
            # combined regroup placeholder is actually present.
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
                # DEV-1829 — a combined regroup placeholder operand routes the
                # composite outward like a cross-model one: its value lives in a
                # ``_cm_`` producer joined back to ``_base``.
                if isinstance(k, ColumnKey) and k in regroup_placeholder_to_cm:
                    outer_composite_slot_ids.add(slot.id)
                    break
                if isinstance(k, AggregateKey):
                    s = slot_by_key.get(k)
                    # DEV-1733: a WINDOWED operand routes the composite outward
                    # for the same reason a cross-model one does — the value
                    # lives in a ``_wm_`` CTE joined back to ``_base``, so
                    # rendering the composite inside ``_base`` would silently
                    # substitute a PLAIN aggregate for the rolling one.
                    if s is not None and s.id in isolated_slot_ids:
                        outer_composite_slot_ids.add(slot.id)
                        break
        base_projection = [
            sid for sid in planned_query.projection
            if sid not in isolated_slot_ids
            and sid not in outer_composite_slot_ids
        ]

        # Hidden ORDER-BY-only LOCAL slots (``ORDER BY revenue:sum`` with
        # no declared measure, or an unprojected host dimension) must be
        # MATERIALISED in ``_base`` so the combined-level ORDER BY can
        # reference them — but they stay OUT of the combined public
        # projection (trimmed). Cross-model order slots are handled by
        # the per-plan ``_cm_*`` branch, never here.
        seen_base_ids = set(base_projection)
        order_only_local_ids: List[str] = []
        for order_entry in planned_query.order:
            sid = order_entry.slot_id
            if (
                sid in isolated_slot_ids
                or sid in outer_composite_slot_ids
                or sid in seen_base_ids
            ):
                # DEV-1714: a windowed slot lives in its ``_wm_`` CTE, never
                # ``_base`` — materialising it here as an order-only local slot
                # would emit a dead plain aggregate in ``_base``. It resolves in
                # the combined ORDER BY via its bare projected alias instead.
                continue
            slot = slots_by_id.get(sid)
            if slot is None:
                continue
            # Local-only: a cross-model aggregate carries a non-empty
            # ``source.path``; those never materialise in ``_base``.
            if getattr(getattr(slot.key, "source", None), "path", ()):
                continue
            order_only_local_ids.append(sid)
            seen_base_ids.add(sid)
        base_render_order = base_projection + order_only_local_ids

        # When a transform layer is present, the ``_base`` CTE (and the
        # combined SELECT that becomes the transform base) must also carry
        # hidden LOCAL transform deps the public projection omits — a local
        # aggregate feeding a transform (``cumsum(amount:sum)`` alongside a
        # cross-model agg), partition-by dims, or a hidden time_key. Cross-
        # model agg deps stay in the per-plan ``_cm_*`` CTEs. Mirrors
        # ``_collect_base_aux_slot_ids`` used by the local transform path.
        aux_slot_id_by_key = {s.key: s.id for s in slots_by_id.values()}

        def _add_local_aux_slots(
            *,
            include_order: bool,
            aggregates_only: bool,
        ) -> None:
            """Pull local (non-cross-model) aux slot ids into
            ``base_render_order``. Shared body for the transform-deps
            pass (include_order=True) and the AGG-phase filter pass
            (aggregates_only=True), to keep the duplication-density
            metric below the gate.
            """
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

        # DEV-1503 (Codex round 2 #1) — non-isolated local AggregateKey
        # operands of an outer-rendered composite (e.g. ``total_amount:sum``
        # in ``loss_payment_amt:sum + total_amount:sum``) must still
        # materialise in ``_base`` so the outer combined SELECT can
        # reference them via ``_base."<alias>"``. Walk each outer
        # composite's key tree once, promote its non-isolated agg deps
        # to ``base_render_order`` as hidden aux slots.
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
                    # DEV-1733: a WINDOWED operand is owned by its ``_wm_`` CTE,
                    # exactly like a cross-model one is owned by ``_cm_``.
                    # Promoting it into ``_base`` would emit a dead PLAIN
                    # aggregate under the windowed slot's alias, which the outer
                    # composite would then read instead of the rolling value.
                    if dep.id in isolated_slot_ids or dep.id in seen_base_ids:
                        continue
                    base_render_order.append(dep.id)
                    seen_base_ids.add(dep.id)

        if planned_query.transform_layers:
            _add_local_aux_slots(include_order=True, aggregates_only=False)
        # DEV-1501 (Codex round 6): host AGG-phase filter operand
        # AggregateKey slots (a HAVING filter on a hidden local first/
        # last) must also reach ``base_render_order`` so the host
        # ``_base`` CTE builds the ranked subquery — otherwise HAVING
        # references a dangling ``_last_rn``. ``aggregates_only=True``
        # keeps row deps out of GROUP BY; ``include_order=False`` since
        # order is already covered by ``order_only_local_ids`` above.
        _add_local_aux_slots(include_order=False, aggregates_only=True)

        # Hidden grain materialisation: when the user query has neither
        # host row slots NOR local aggs (and no hidden order targets),
        # ``base_render_order`` is empty. ``_base`` becomes a one-row
        # placeholder so the combined CROSS JOIN to the scalar ``_cm_*``
        # CTEs has a left side to join against.
        #
        # DEV-1503 (Codex round 2 #2): emit the placeholder WITHOUT the
        # host FROM. With ``FROM <host>`` the placeholder is N rows (one
        # per host row), and a CROSS JOIN to a 1-row ``_cm_*`` scalar
        # aggregate duplicates the result N times — for a no-dim
        # aggregate-only query the user expects ONE row, not one per
        # host row. The host rowset doesn't contribute to the result
        # here (every projected slot is a cross-model aggregate that
        # owns its own ``FROM <host>`` inside the ``_cm_*`` CTE), so
        # dropping the host FROM is safe.
        # DEV-1746 (§5.12): the shape, and which filters stay host-local, are
        # decided at plan time and consumed here (P-D). The generator used to
        # re-derive both from its own render order and a re-walk of
        # ``filters_by_phase``.
        empty_base_plan = planned_query.empty_base_plan
        if empty_base_plan is not None:
            # Apply exactly the filters the plan marked host-local; every
            # other filter is applied somewhere else (a ``_cm_*`` CTE or the
            # outer WHERE) and must be skipped here.
            host_filter_ids = set(empty_base_plan.host_filter_ids)
            placeholder_skip_ids = {
                fp.id
                for fp in planned_query.filters_by_phase
                if fp.id not in host_filter_ids
            }
            if host_filter_ids:
                # Build the placeholder over the host with WHERE + LIMIT 1.
                # LIMIT 1 collapses the host rowset to a single row so the
                # combined CROSS JOIN to the scalar ``_cm_*`` does not
                # duplicate aggregates (round 2 invariant), while WHERE
                # still gates the entire result — if no host row matches,
                # ``_base`` is empty and the combined query returns 0
                # rows (correct host-filter semantics).
                #
                # Round 6 (Codex): register the non-routed filters' join paths
                # into a host ScopeFrame (Law 1 — same single resolver as the
                # main host base, D-J) and pull them in via
                # ``_build_from_and_joins`` — a filter like
                # ``claim.claim_number = '...'`` references a joined alias
                # that must be in scope; without the join, the WHERE
                # references an undefined alias.
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
            # Which host filters ``_base`` must NOT apply.
            #
            # Only the ones it CANNOT apply. A ``_cm_`` CTE is joined back with
            # a LEFT JOIN on the query grain, which propagates a value but not
            # an EXCLUSION: a host row whose group the CTE filtered away does
            # not disappear, it arrives with a NULL measure. So a predicate
            # applied only in the CTE silently turns "exclude these rows" into
            # "blank out their measure", and the user gets rows they asked not
            # to see (DEV-1747 B6, second instance).
            #
            # ROW-phase (``where_filter_ids``) predicates are therefore applied
            # in BOTH places. They are host-evaluable by construction — they
            # were bound against the host — and applying one at the host is
            # exactly what the same query does when it carries no cross-model
            # measure at all, so this is also what makes those two agree.
            # Double-applying is free: the CTE's copy narrows the aggregate,
            # the host's copy narrows the rows.
            #
            # AGGREGATE-phase (``having_filter_ids``) predicates are the real
            # exclusion. They reference the isolated aggregate, which does not
            # live in ``_base`` at all, so the host cannot evaluate them —
            # trying raises ``NotImplementedError`` (stage 7b.12).
            #
            # DEV-1503: outer-WHERE filters (AGGREGATE-phase host filters
            # referencing a filtered-local isolated aggregate) join them, so
            # ``_base`` does not double-apply them as HAVING on a bare local
            # aggregate expression that no longer lives there.
            #
            # The union runs across EVERY plan, which is what made this a
            # cross-plan defect rather than a per-plan one: a forward plan
            # routing its filter used to make the host skip it for a re-rooted
            # sibling that needed it.
            routed_ids: Set[str] = set(outer_where_filter_ids)
            for plan in planned_query.cross_model_aggregate_plans:
                routed_ids.update(plan.having_filter_ids)
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

        # ``base_select`` stays AST: the WITH assembler takes the query
        # structurally, and the transform-chain branch renders it on demand.

        # Per-plan ``_cm_*`` CTEs. The CTE name and projection use the
        # CANONICAL aggregate alias (path + canonical_agg_name); user-
        # declared ``name``s surface at the combined SELECT level via
        # ``... AS "<public_alias>"`` so:
        #   * legacy parity holds for non-renamed cases (canonical
        #     stays as the only emitted alias);
        #   * C13 multi-alias same-key slots collapse to ONE CTE +
        #     N combined-level projections;
        #   * renamed measures (DEV-1445 C1) produce one CTE under the
        #     canonical alias plus an ``AS`` remap at the combined
        #     SELECT — matches the result-key contract while keeping
        #     legacy parity for the unaliased shape.
        cm_ctes: List[Tuple[str, exp.Expression]] = []
        # Dedup identity is the STRUCTURAL key (the typed AggregateKey plus the
        # source relation), never the sanitised CTE-name string. The canonical
        # alias omits the aggregate's column filter, so a filtered and an
        # unfiltered aggregate over one column share an alias while needing two
        # CTEs; and the name is doubly lossy (path flattening, then
        # non-identifier sanitisation), so unrelated aggregates can collide on
        # it. Keying on the name silently merged both cases.
        cm_cte_name_by_identity: Dict[Any, str] = {}
        cm_cte_name_for_plan: Dict[str, str] = {}
        cm_allocator = self._gen_allocator or self._new_allocator()
        canonical_alias_for_plan: Dict[str, str] = {}
        # join-back pairs are ``(host_base_alias, cte_column_alias)`` — the two
        # sides need not match (re-rooted CTEs alias dims under the target's
        # relation). ``agg_col_alias_for_plan`` is the CTE's emitted column
        # name for the aggregate (canonical for the forward path; the sub-plan
        # alias for the re-rooted path).
        joinback_pairs_for_plan: Dict[str, List[Tuple[str, str]]] = {}
        agg_col_alias_for_plan: Dict[str, str] = {}
        joinback_pairs_for_identity: Dict[Any, List[Tuple[str, str]]] = {}
        agg_col_alias_for_identity: Dict[Any, str] = {}
        for plan in planned_query.cross_model_aggregate_plans:
            agg_slot = slots_by_id.get(plan.aggregate_slot_id)
            if agg_slot is None or not isinstance(agg_slot.key, AggregateKey):
                raise RuntimeError(
                    f"CrossModelAggregatePlan {plan.aggregate_slot_id!r} "
                    f"references a missing or non-aggregate slot.",
                )
            canonical_alias = self._canonical_cross_model_alias(
                source_relation=source_relation,
                key=agg_slot.key,
            )
            canonical_alias_for_plan[plan.aggregate_slot_id] = canonical_alias
            identity = _cm_plan_identity(
                source_relation=source_relation, plan=plan, agg_slot=agg_slot,
            )
            existing = cm_cte_name_by_identity.get(identity)
            if existing is not None:
                # Same aggregate under another public name: share the one CTE,
                # but still record THIS slot's maps. The old code skipped the
                # whole iteration, leaving the join-back and column-alias maps
                # unwritten for the skipped slot id.
                cm_cte_name_for_plan[plan.aggregate_slot_id] = existing
                joinback_pairs_for_plan[plan.aggregate_slot_id] = (
                    joinback_pairs_for_identity[identity]
                )
                agg_col_alias_for_plan[plan.aggregate_slot_id] = (
                    agg_col_alias_for_identity[identity]
                )
                continue
            cte_name = cte_name_from_alias(
                prefix="_cm_", alias=canonical_alias, allocator=cm_allocator,
                dialect=self.dialect, limit=self._dialect.max_identifier_bytes,
            )
            cm_cte_name_by_identity[identity] = cte_name
            cm_cte_name_for_plan[plan.aggregate_slot_id] = cte_name

            if plan.rerooted_plan is not None:
                # C1: nested re-rooted PlannedQuery rooted at the target,
                # preserving host dimension grain.
                rerooted_sql, joinback_pairs, agg_col_alias = (
                    self._render_rerooted_cross_model_cte(
                        plan=plan,
                        bundle=bundle,
                        host_slots_by_id=slots_by_id,
                        host_source_relation=source_relation,
                    )
                )
                # The ONE parse seam: this branch renders a complete nested
                # ``WITH … SELECT`` through ``generate_from_planned``, so it
                # arrives as text. Everything else in the chain is already AST.
                cte_query = self._parse_cte_body(rerooted_sql)
            else:
                cte_query, shared_grain_aliases = self._render_cross_model_cte(
                    plan=plan,
                    agg_slot=agg_slot,
                    full_agg_alias=canonical_alias,
                    bundle=bundle,
                    planned_query=planned_query,
                    slots_by_id=slots_by_id,
                    base_projection_ids=set(base_projection),
                )
                # Forward path: host alias == cte alias; agg under canonical.
                joinback_pairs = [(a, a) for a in shared_grain_aliases]
                agg_col_alias = canonical_alias
            cm_ctes.append((cte_name, cte_query))
            joinback_pairs_for_plan[plan.aggregate_slot_id] = joinback_pairs
            agg_col_alias_for_plan[plan.aggregate_slot_id] = agg_col_alias
            joinback_pairs_for_identity[identity] = joinback_pairs
            agg_col_alias_for_identity[identity] = agg_col_alias

        # DEV-1714 Stage 10 — per-plan ``_wm_`` windowed range-join CTEs. Each
        # is host-rooted (``FROM _base LEFT JOIN _src``), grouped at the query
        # grain, and joined back to ``_base`` on that grain (host alias == cte
        # column alias, since the CTE projects the grain under the same alias).
        wm_ctes: List[Tuple[str, exp.Expression]] = []
        wm_cte_name_for_plan: Dict[str, str] = {}
        wm_agg_col_for_plan: Dict[str, str] = {}
        wm_joinback_pairs_for_plan: Dict[str, List[Tuple[str, str]]] = {}
        # Codex round 4: mint ``_wm_`` CTE names through the DEV-1726 collision-
        # aware allocator so two measures whose aliases lossy-sanitise to the
        # same name (``rev-a`` / ``rev_a``), or case-only variants on a
        # case-folding dialect, get distinct auto-numbered names instead of
        # tripping the CTE-name-collision belt.
        wm_allocator = self._gen_allocator or self._new_allocator()
        for plan in planned_query.windowed_aggregate_plans:
            agg_slot = slots_by_id.get(plan.aggregate_slot_id)
            if agg_slot is None or not isinstance(agg_slot.key, AggregateKey):
                raise RuntimeError(
                    f"WindowedAggregatePlan {plan.aggregate_slot_id!r} references "
                    f"a missing or non-aggregate slot.",
                )
            full_agg_alias = self._full_alias_for_slot(
                slot=agg_slot, source_relation=source_relation, alias_index={},
            )
            cte_name = cte_name_from_alias(
                prefix="_wm_", alias=full_agg_alias, allocator=wm_allocator,
                dialect=self.dialect, limit=self._dialect.max_identifier_bytes,
            )
            cte_query, grain_aliases = self._render_window_measure_cte_from_planned(
                plan=plan, agg_slot=agg_slot, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                planned_query=planned_query, slots_by_id=slots_by_id,
                aliases_by_slot_id=aliases_by_slot_id, full_agg_alias=full_agg_alias,
                regroup_env=row_regroup_env,
                regroup_join_specs=row_regroup_join_specs,
            )
            wm_ctes.append((cte_name, cte_query))
            wm_cte_name_for_plan[plan.aggregate_slot_id] = cte_name
            wm_agg_col_for_plan[plan.aggregate_slot_id] = full_agg_alias
            wm_joinback_pairs_for_plan[plan.aggregate_slot_id] = [
                (a, a) for a in grain_aliases
            ]

        # DEV-1748 (B9) — per-plan ``_rk_`` ranked first/last CTEs. Rooted where
        # the ranked rows live (the host, or the join target), grouped at the
        # query grain, joined back on it. Names are minted through the same
        # collision-aware allocator the other two prefixes use, so two measures
        # whose aliases lossy-sanitise alike get distinct CTEs (P-F).
        rk_ctes: List[Tuple[str, exp.Expression]] = []
        rk_cte_name_for_plan: Dict[str, str] = {}
        rk_agg_col_for_plan: Dict[str, str] = {}
        rk_joinback_pairs_for_plan: Dict[str, List[Tuple[str, str]]] = {}
        rk_allocator = self._gen_allocator or self._new_allocator()
        for plan in planned_query.ranked_aggregate_plans:
            agg_slot = slots_by_id.get(plan.aggregate_slot_id)
            if agg_slot is None or not isinstance(agg_slot.key, AggregateKey):
                raise RuntimeError(
                    f"RankedAggregatePlan {plan.aggregate_slot_id!r} references "
                    f"a missing or non-aggregate slot.",
                )
            full_agg_alias = self._full_alias_for_slot(
                slot=agg_slot, source_relation=source_relation, alias_index={},
            )
            cte_name = cte_name_from_alias(
                prefix=RANKED_CTE_PREFIX, alias=full_agg_alias, allocator=rk_allocator,
                dialect=self.dialect, limit=self._dialect.max_identifier_bytes,
            )
            cte_query, grain_aliases = self._render_ranked_cte_from_planned(
                plan=plan, agg_slot=agg_slot, bundle=bundle,
                planned_query=planned_query, slots_by_id=slots_by_id,
                host_source_model=source_model,
                host_source_relation=source_relation,
                full_agg_alias=full_agg_alias,
                regroup_env=row_regroup_env,
                regroup_join_specs=row_regroup_join_specs,
            )
            rk_ctes.append((cte_name, cte_query))
            rk_cte_name_for_plan[plan.aggregate_slot_id] = cte_name
            rk_agg_col_for_plan[plan.aggregate_slot_id] = full_agg_alias
            rk_joinback_pairs_for_plan[plan.aggregate_slot_id] = [
                (a, a) for a in grain_aliases
            ]

        # DEV-1745 (W5): dropped-filter warnings are NOT emitted here. This
        # emission fired once per cross-model plan — so nested subplans
        # double-fired for one user filter — and never fired at all on a path
        # that did not reach this render step. It is now collected across every
        # plan at the ENGINE boundary, deduped per user filter, and emitted
        # exactly once per execute. The plans still carry the payloads.

        # Build the combined SELECT: SELECT _base.<all_local>,
        # _cm_*.<canonical> [AS "<user_alias>"] FROM _base [LEFT JOIN |
        # CROSS JOIN] _cm_* [ON ...].
        # Projection expressions per slot, emitted below in the PLAN's declared
        # order (B7). Collecting per slot first is what allows one ordered pass:
        # the host, outer-composite, cross-model and windowed sides each know how
        # to render their own columns, but none of them knows where those columns
        # belong relative to the others — only ``planned_query.projection`` does.
        proj_exprs: Dict[str, List[exp.Expression]] = {}
        # ``combined_aliases_by_slot_id`` records the output column alias each
        # slot surfaces in the combined SELECT — the input the transform chain
        # (when present) binds against (the combined result is its base CTE).
        combined_aliases_by_slot_id: Dict[str, List[str]] = {}

        def _emit(sid: str, expr: exp.Expression) -> None:
            proj_exprs.setdefault(sid, []).append(expr)
        # Host-side projection: every slot in base_projection surfaces
        # its picked alias(es). Multi-alias slots emit one entry per
        # alias (C13). With a transform chain on top, the combined SELECT is
        # that chain's base CTE, so it must ALSO surface hidden local deps
        # materialised in ``_base`` (transform inputs / order-only slots) —
        # the outer wrap trims them back to the public projection.
        host_combined_ids = (
            base_render_order
            if planned_query.transform_layers
            else base_projection
        )
        # Deduped, because a C13 slot appears once per DECLARED NAME in the
        # projection and its alias list already carries one entry per name.
        # Visiting it twice and emitting the whole list each time renders N²
        # columns, which the projection-consumption check below then rejects —
        # a query mixing a two-name measure with any isolated aggregate used to
        # fail outright.
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
        # DEV-1503 (Codex round 2 #1) — composite slots routed to the outer
        # combined SELECT. Render via the same substitution renderer the
        # outer-WHERE wrapper uses: isolated AggregateKey → ``_cm_*."col"``,
        # non-isolated local agg / ColumnKey → ``_base."<alias>"`` (already
        # promoted as aux above). Wrap with ``AS "<public_alias>"`` so the
        # composite surfaces under the user-declared name.
        outer_composite_order_alias_by_sid: Dict[str, str] = {}
        outer_composite_order_expressions: Dict[str, exp.Expression] = {}
        if outer_composite_slot_ids:
            outer_composite_cm_map: Dict[str, Tuple[str, str]] = {}
            for plan in planned_query.cross_model_aggregate_plans:
                cte_name = cm_cte_name_for_plan[plan.aggregate_slot_id]
                agg_col_alias = agg_col_alias_for_plan[plan.aggregate_slot_id]
                outer_composite_cm_map[plan.aggregate_slot_id] = (
                    cte_name, agg_col_alias,
                )
            # DEV-1733: windowed operands resolve the same way — the renderer
            # substitutes ``<cte>."<col>"`` for any slot id in this map, and a
            # ``_wm_`` CTE is joined into the combined FROM exactly as a
            # ``_cm_`` one is. Without these entries the operand would fall
            # through to the ``_base.<alias>`` fallback and read a plain
            # aggregate (or dangle).
            for plan in planned_query.windowed_aggregate_plans:
                outer_composite_cm_map[plan.aggregate_slot_id] = (
                    wm_cte_name_for_plan[plan.aggregate_slot_id],
                    wm_agg_col_for_plan[plan.aggregate_slot_id],
                )
            # A ranked operand resolves the same way (DEV-1748): its value is a
            # column of a joined-in CTE, so a composite over it evaluates in the
            # combined SELECT, never in ``_base``.
            for plan in planned_query.ranked_aggregate_plans:
                outer_composite_cm_map[plan.aggregate_slot_id] = (
                    rk_cte_name_for_plan[plan.aggregate_slot_id],
                    rk_agg_col_for_plan[plan.aggregate_slot_id],
                )
            # DEV-1829 — a combined regroup placeholder resolves to its producer
            # column exactly like a cross-model aggregate does, keyed by the
            # placeholder slot's id so the outer-wrapper facility resolves it.
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

            # Projected outer composites: cycle through ``public_aliases``
            # for each occurrence in ``planned_query.projection``. C13 lets
            # the same composite slot project under multiple user-declared
            # names; emitting ``public_aliases[0]`` twice (and overwriting
            # ``combined_aliases_by_slot_id[sid]``) would drop the second
            # alias (CodeRabbit thread 2).
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
                # The first emitted alias is the canonical handle the
                # combined-level ORDER BY references.
                outer_composite_order_alias_by_sid.setdefault(sid, full_alias)

            # Order-only outer composites (Codex round 3 #1 / round 8):
            # not in public projection but referenced by
            # ``planned_query.order``. Rather than materialise as a
            # hidden combined-SELECT column (which would leak as an
            # extra public-result column on the no-transform path AND
            # disappear from the cross-model transform chain's
            # carry-forward dict), render the expression INLINE in the
            # combined ORDER BY. ``outer_composite_order_expressions``
            # carries the rendered SQL for each order-only slot; the
            # order-by builder emits ``<expr> {direction}`` bare.
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
        # Cross-model side: one entry per declared user alias, all
        # referencing the CTE's aggregate column (canonical for the forward
        # path; the sub-plan alias for the re-rooted path). When the public
        # alias matches the CTE column name, no ``AS`` remap fires.
        for plan in planned_query.cross_model_aggregate_plans:
            agg_slot = slots_by_id[plan.aggregate_slot_id]
            agg_col_alias = agg_col_alias_for_plan[plan.aggregate_slot_id]
            cte_name = cm_cte_name_for_plan[plan.aggregate_slot_id]
            # Read THIS plan's canonical alias. It feeds
            # ``_public_aliases_for_cross_model_agg``, which falls back to it
            # when the slot declares no public alias, so a stale value projects
            # one measure under another measure's name.
            canonical_alias = canonical_alias_for_plan[plan.aggregate_slot_id]
            # DEV-1495 bug 2 / DEV-1712: an order-by-only (hidden) cross-model
            # aggregate never surfaces in the combined projection — its CTE is
            # still joined below, and the ORDER BY references it CTE-qualified
            # (``hidden_cte_order_refs``). Trimming it keeps the outer SELECT to
            # the user-declared columns (Law 2 projection boundary). Only when
            # there is NO transform chain: a hidden CMA feeding a transform
            # layer (``cumsum(customers.revenue:sum)``) must stay projected so
            # the step CTE can consume it — the transform outer wrap does the
            # public-vs-hidden trim in that path.
            trim_hidden = plan.hidden and not planned_query.transform_layers
            public_aliases = (
                []
                if trim_hidden
                else self._public_aliases_for_cross_model_agg(
                    slot=agg_slot,
                    source_relation=source_relation,
                    canonical_alias=canonical_alias,
                )
            )
            for pub in public_aliases:
                col = grain_alias_column(alias=agg_col_alias, table=cte_name)
                _emit(
                    plan.aggregate_slot_id,
                    col if pub == agg_col_alias else col.as_(pub, quoted=True),
                )
            combined_aliases_by_slot_id[plan.aggregate_slot_id] = list(
                public_aliases,
            )

        # DEV-1829 — regroup side: a directly-projected combined placeholder
        # (a partitioned measure) surfaces the producer's aggregate column under
        # the consumer's public alias(es), remapping ``AS`` for extra names
        # (C13). A hidden order-only placeholder is trimmed — its ``_cm_`` CTE is
        # still joined below and its ORDER BY reference resolves CTE-qualified.
        for _ph_key, (_cte_name, _agg_col) in regroup_placeholder_to_cm.items():
            ph_slot = slot_by_key.get(_ph_key)
            if ph_slot is None or ph_slot.id not in regroup_placeholder_slot_ids:
                continue
            # DEV-1824 — a hidden placeholder is the INPUT of a transform layer
            # (``cumsum(amount:sum(partition_by=…))``): with a transform chain on
            # top it must stay projected under its canonical ``_cm_`` column so the
            # step CTE can read it (mirrors the windowed side below). Without a
            # transform chain a hidden placeholder is order-only and trims to
            # nothing — its ``_cm_`` CTE is still joined and ORDER BY resolves
            # CTE-qualified.
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

        # DEV-1714 Stage 10 — windowed side: project each ``_wm_`` CTE's
        # aggregate column. Codex#2: one occurrence per declared user alias (C13
        # lets the same windowed key be selected under multiple names — the CTE
        # holds one aggregate column, remapped ``AS`` each public alias). The
        # column name already IS the primary dotted result key, so that occurrence
        # needs no remap. Like the cross-model ``_cm_`` columns above, windowed
        # columns are grouped after the ``_base`` projection rather than woven
        # into ``planned_query.projection`` order — deterministic (measure
        # declaration order) and harmless because results are keyed by name, not
        # position.
        for plan in planned_query.windowed_aggregate_plans:
            agg_slot = slots_by_id[plan.aggregate_slot_id]
            cte_name = wm_cte_name_for_plan[plan.aggregate_slot_id]
            agg_col = wm_agg_col_for_plan[plan.aggregate_slot_id]
            # DEV-1733: an order-only (hidden) windowed aggregate never surfaces
            # in the combined projection — its ``_wm_`` CTE is still joined
            # below and the ORDER BY references it CTE-qualified
            # (``hidden_wm_order_ref``). Same trim predicate the hidden
            # cross-model aggregate uses: with a transform chain on top the
            # column must stay projected so the step CTE can consume it, and
            # the transform outer wrap does the public-vs-hidden trim there.
            if plan.hidden and not planned_query.transform_layers:
                combined_aliases_by_slot_id[plan.aggregate_slot_id] = []
                continue
            public_names = list(agg_slot.public_aliases) or (
                [agg_slot.public_name] if agg_slot.public_name else []
            )
            full_aliases = [f"{source_relation}.{p}" for p in public_names] or [agg_col]
            for full in full_aliases:
                col = grain_alias_column(alias=agg_col, table=cte_name)
                _emit(
                    plan.aggregate_slot_id,
                    col if full == agg_col else col.as_(full, quoted=True),
                )
            combined_aliases_by_slot_id[plan.aggregate_slot_id] = list(full_aliases)

        # DEV-1748 (B9) — ranked side. Identical to the windowed one above: one
        # occurrence per declared user alias (C13), the hidden order-only case
        # trimmed from the projection while its CTE stays joined.
        for plan in planned_query.ranked_aggregate_plans:
            agg_slot = slots_by_id[plan.aggregate_slot_id]
            cte_name = rk_cte_name_for_plan[plan.aggregate_slot_id]
            agg_col = rk_agg_col_for_plan[plan.aggregate_slot_id]
            if plan.hidden and not planned_query.transform_layers:
                combined_aliases_by_slot_id[plan.aggregate_slot_id] = []
                continue
            public_names = list(agg_slot.public_aliases) or (
                [agg_slot.public_name] if agg_slot.public_name else []
            )
            full_aliases = [f"{source_relation}.{p}" for p in public_names] or [agg_col]
            for full in full_aliases:
                col = grain_alias_column(alias=agg_col, table=cte_name)
                _emit(
                    plan.aggregate_slot_id,
                    col if full == agg_col else col.as_(full, quoted=True),
                )
            combined_aliases_by_slot_id[plan.aggregate_slot_id] = list(full_aliases)

        # Grain join-backs (P-I). Both plan kinds join back identically — on the
        # shared grain, null-safely, so a NULL dimension value or a nullable
        # truncated time bucket keeps its aggregate instead of dropping it. An
        # EMPTY grain (a scalar aggregate) has nothing to join on and becomes a
        # CROSS JOIN; the builder signals that by returning ``None``.
        # The public projection, in the plan's declared order (B7). Every
        # renderer consumes ``planned_query.projection`` verbatim rather than
        # reconstructing an order from the separate host / composite / cross-
        # model / windowed lists — which is why a cross-model measure declared
        # first used to be emitted last. Hidden slots are simply absent from the
        # plan's list, so trimming them is not a step: it is the absence of one.
        # One slot can appear in the projection more than once: C13 lets the
        # same key be selected under several user-declared names, and the plan
        # lists it once per name. Each occurrence therefore consumes the NEXT of
        # that slot's rendered columns — emitting the whole list per occurrence
        # would project every alias once per name.
        combined_select_exprs: List[exp.Expression] = []
        consumed: Dict[str, int] = {}
        for sid in planned_query.projection:
            exprs = proj_exprs.get(sid)
            if not exprs:
                # Not rendered by THIS scope at all. A transform slot is in the
                # plan's projection but is computed by a later step CTE and
                # projected there, so its absence here is correct — unlike a
                # slot that renders some columns but fewer than the projection
                # asks for, which is caught below.
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
        # Columns the plan does not publish but the statement still needs: with
        # a transform chain the combined SELECT is that chain's base CTE, so it
        # must also carry hidden inputs (transform operands, order-only slots)
        # for the step CTEs to read. The outer wrap trims them back afterwards.
        #
        # Only slots the projection never mentions are carried. A slot the plan
        # DID publish has exactly as many occurrences as it has declared names,
        # so a leftover would mean the two disagree — and appending it would
        # emit an extra public column, at the end, under a name the caller did
        # not ask for. Fail instead. Both directions of that disagreement fail:
        # too FEW rendered columns is caught in the loop above, where the
        # occurrence would otherwise be dropped from the result.
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
        joinback_specs = [
            (
                cm_cte_name_for_plan[plan.aggregate_slot_id],
                joinback_pairs_for_plan.get(plan.aggregate_slot_id, []),
            )
            for plan in planned_query.cross_model_aggregate_plans
        ] + [
            (
                wm_cte_name_for_plan[plan.aggregate_slot_id],
                wm_joinback_pairs_for_plan.get(plan.aggregate_slot_id, []),
            )
            for plan in planned_query.windowed_aggregate_plans
        ] + [
            (
                rk_cte_name_for_plan[plan.aggregate_slot_id],
                rk_joinback_pairs_for_plan.get(plan.aggregate_slot_id, []),
            )
            for plan in planned_query.ranked_aggregate_plans
        ] + regroup_joinbacks  # DEV-1829 — combined regroup producers
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

        # DEV-1503 — outer combined-SELECT WHERE wrapper. AGGREGATE-phase
        # host filters routed here in the classification pass above
        # (``outer_where_filters``) render now against the joined-back
        # ``_cm_*`` column for isolated-aggregate refs and ``_base.<alias>``
        # for any local operand (which the ``_add_local_aux_slots`` pass
        # has materialised in ``_base``).
        if outer_where_filters:
            # Map EVERY cross-model aggregate slot (filtered-local AND
            # forward / re-rooted) to its ``_cm_*`` CTE column — a mixed
            # AGGREGATE filter like ``loss_payment_amt:sum >
            # customers.revenue:sum`` triggers the outer wrapper through
            # the filtered-local operand but ALSO has to resolve the
            # forward cross-model operand on the same outer scope. If
            # only filtered-local plans were mapped, the forward operand
            # would fall through to the ``_base`` fallback and the
            # renderer would raise (CodeRabbit thread 2).
            cross_model_agg_slot_to_cm: Dict[str, Tuple[str, str]] = {}
            for plan in planned_query.cross_model_aggregate_plans:
                cte_name = cm_cte_name_for_plan[plan.aggregate_slot_id]
                agg_col_alias = agg_col_alias_for_plan[plan.aggregate_slot_id]
                cross_model_agg_slot_to_cm[plan.aggregate_slot_id] = (
                    cte_name, agg_col_alias,
                )
            # DEV-1748: a ranked aggregate is isolated for the same reason and
            # resolves the same way. Its filter is HERE rather than a HAVING
            # inside the CTE precisely because the join back is a LEFT JOIN —
            # dropping the CTE row would resurrect the host row with a NULL.
            for plan in planned_query.ranked_aggregate_plans:
                cross_model_agg_slot_to_cm[plan.aggregate_slot_id] = (
                    rk_cte_name_for_plan[plan.aggregate_slot_id],
                    rk_agg_col_for_plan[plan.aggregate_slot_id],
                )
            # DEV-1824 — a filter referencing a combined regroup placeholder
            # (a partitioned aggregate in a query filter) resolves to its
            # producer column exactly like a cross-model aggregate, keyed by the
            # placeholder slot's id.
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

        # DEV-1714 Stage 10 — POST-phase filters referencing a windowed measure
        # render as an outer WHERE on the combined SELECT (never HAVING on the
        # plain base aggregate), resolving each windowed slot to its ``_wm_``
        # CTE's joined-back aggregate column.
        if planned_query.windowed_aggregate_plans:
            wm_slot_to_cte: Dict[str, Tuple[str, str]] = {
                p.aggregate_slot_id: (
                    wm_cte_name_for_plan[p.aggregate_slot_id],
                    wm_agg_col_for_plan[p.aggregate_slot_id],
                )
                for p in planned_query.windowed_aggregate_plans
            }
            # ``Select.where`` conjoins, so a POST-phase windowed filter composes
            # with any outer-WHERE filter above without the caller choosing
            # between ``WHERE`` and ``AND`` — the hand-rolled connector this
            # replaces glued an ``AND`` onto a predicate built elsewhere, with no
            # parenthesisation of the union.
            for fp in planned_query.filters_by_phase:
                if fp.phase != Phase.POST or fp.expression is None:
                    continue
                rendered = render_value_key(
                    key=fp.expression.value_key,
                    ctx=self._outer_wrapper_render_ctx(
                        slot_by_key=slot_by_key,
                        cross_model_agg_slot_to_cm=wm_slot_to_cte,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ),
                )
                if isinstance(rendered, (exp.And, exp.Or)):
                    rendered = exp.Paren(this=rendered)
                combined_select = combined_select.where(rendered)

        # DEV-1450 stage 7b.15e (C2): a transform layer over a cross-model
        # aggregate (``cumsum(customers.avg_score:avg)``) runs on TOP of the
        # combined cross-model result — the combined SELECT becomes the base
        # CTE and the window step CTEs / outer wrap are layered above it.
        if planned_query.transform_layers:
            if wm_ctes:
                # Unreachable today — guard G4 rejects a windowed measure that
                # coexists with a transform — but the combined SELECT already
                # projects/joins the _wm_ CTEs, which this prelude omits. Fail
                # loudly so lifting G4 (DEV-1504) can't silently emit a statement
                # referencing undefined _wm_ CTEs.
                raise NotImplementedError(
                    "DEV-1714 Stage 10: a windowed measure combined with a "
                    "transform layer is not supported (guarded at plan time by "
                    "G4); the cross-model transform chain does not carry `_wm_` "
                    "CTEs.",
                )
            return self._render_cross_model_transform_chain(
                # ``_rk_`` CTEs join into the combined SELECT, which becomes the
                # chain's base, so they belong in the prelude alongside the
                # ``_cm_`` ones. Like those they are rooted at a real relation
                # and depend on nothing. DEV-1824 — the combined regroup
                # producers (``cm_regroup_ctes``) join into that same combined
                # SELECT, so they belong in the prelude too; omitting them left
                # the SELECT referencing an undefined ``_cm_`` producer.
                # DEV-1837 (D4) — real ``CteEntry``s, not ``(name, query)``
                # tuples: the ROW producers' hoisted internals keep their
                # dependency edges, and ``_base`` declares the producers it
                # LEFT JOINs.
                prelude_ctes=[
                    *row_regroup_ctes,
                    CteEntry(
                        name="_base", query=base_select,
                        depends_on=[
                            *[e.name for e in row_regroup_ctes], *reused_cm_ctes,
                        ],
                    ),
                    *[CteEntry(name=n, query=q) for n, q in cm_ctes],
                    *[CteEntry(name=n, query=q) for n, q in cm_regroup_ctes],
                    *[CteEntry(name=n, query=q) for n, q in rk_ctes],
                ],
                combined_select=combined_select,
                planned_query=planned_query,
                slots_by_id=slots_by_id,
                combined_aliases_by_slot_id=combined_aliases_by_slot_id,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                # DEV-1835 D7 — a ``time_shift`` over a COMBINED placeholder
                # re-reads the source in its shifted CTE, so the combined
                # producers join there too: merge their placeholder env and
                # source-scope join specs with the ROW ones. The ROW-only base /
                # step CTEs read the already-materialised column, so the extra
                # env entries are inert for them.
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

        # Assemble the WITH chain (§5.6). Dependencies are DECLARED, not
        # discovered by scanning the rendered statement: ``_wm_`` CTEs select
        # FROM ``_base``, the cross-model CTEs are rooted at their own targets
        # and depend on nothing. The assembler emits a stable topological order
        # with declaration order as the tiebreak.
        # DEV-1824 (task 3.2) — ROW regroup producers join INTO ``_base`` (before
        # aggregation), so ``_base`` declares them as dependencies and they emit
        # ahead of it.
        cte_entries = [
            CteEntry(
                name="_base", query=base_select,
                # DEV-1835 D10 — also depend on any COMBINED producer a dual-role
                # row attach reused, so it emits ahead of ``_base``.
                depends_on=[
                    *[e.name for e in row_regroup_ctes], *reused_cm_ctes,
                ],
            ),
            *row_regroup_ctes,
        ]
        cte_entries += [
            CteEntry(name=name, query=query) for name, query in cm_ctes
        ]
        # DEV-1829 — combined regroup producers are host-rooted single SELECTs
        # (``FROM <host>``), rooted at a real relation like a ``_cm_`` CTE, so
        # they declare no ``_base`` dependency.
        cte_entries += [
            CteEntry(name=name, query=query) for name, query in cm_regroup_ctes
        ]
        cte_entries += [
            CteEntry(
                name=name, query=query,
                # DEV-1835 D4 — a windowed producer whose grain is a computed
                # dimension LEFT JOINs the ROW producers inside ``_src``, so it
                # depends on them as well as ``_base``.
                depends_on=["_base", *[e.name for e in row_regroup_ctes]],
            )
            for name, query in wm_ctes
        ]
        # A ``_rk_`` CTE is rooted at a real relation, never at ``_base``. DEV-1835
        # D4 — a ranked producer whose grain is a computed dimension LEFT JOINs the
        # ROW producers inside its inner select, so it declares them as
        # dependencies (an empty list keeps the byte-stable no-row-attach case).
        cte_entries += [
            CteEntry(
                name=name, query=query,
                depends_on=[e.name for e in row_regroup_ctes],
            )
            for name, query in rk_ctes
        ]
        combined_statement = assemble_with_chain(
            entries=cte_entries, final=combined_select,
        )

        # ORDER BY / LIMIT / OFFSET: emitted at the combined SELECT level,
        # through the one resolver (§5.10). Each scope names its own value and
        # nothing else does — the superseded chain ran a five-way precedence
        # over four alias maps and put a projected cross-model aggregate under
        # its CTE COLUMN name while the SELECT projected it under the user's
        # alias, which resolves only by falling through to an input column of
        # the FROM (Postgres permits that; other engines do not, and it picks
        # the wrong column the moment two scopes project the same name).
        order_env = OrderEnv(dialect=self._dialect)
        # An isolated aggregate is CTE-qualified whether or not it is ALSO
        # projected: hidden it has no combined-SELECT alias to name, projected
        # its alias is the user's, not the CTE column's. One form, both cases.
        for plan in planned_query.cross_model_aggregate_plans:
            order_env.cross_model_cte[plan.aggregate_slot_id] = grain_alias_column(
                alias=agg_col_alias_for_plan[plan.aggregate_slot_id],
                table=cm_cte_name_for_plan[plan.aggregate_slot_id],
            )
        for plan in planned_query.windowed_aggregate_plans:
            order_env.windowed_cte[plan.aggregate_slot_id] = grain_alias_column(
                alias=wm_agg_col_for_plan[plan.aggregate_slot_id],
                table=wm_cte_name_for_plan[plan.aggregate_slot_id],
            )
        for plan in planned_query.ranked_aggregate_plans:
            order_env.ranked_cte[plan.aggregate_slot_id] = grain_alias_column(
                alias=rk_agg_col_for_plan[plan.aggregate_slot_id],
                table=rk_cte_name_for_plan[plan.aggregate_slot_id],
            )
        # DEV-1829 — an ORDER BY over a combined regroup placeholder resolves to
        # its producer column, exactly like a cross-model aggregate (the planner
        # classifies it CROSS_MODEL_CTE).
        for _ph_key, (_cte_name, _agg_col) in regroup_placeholder_to_cm.items():
            _ph_slot = slot_by_key.get(_ph_key)
            if _ph_slot is not None:
                order_env.cross_model_cte[_ph_slot.id] = grain_alias_column(
                    alias=_agg_col, table=_cte_name,
                )
        # A PROJECTED outer composite orders on its combined-SELECT alias; an
        # order-only one has no alias and renders INLINE, so no synthetic
        # column leaks into the public projection.
        for _sid, _alias in outer_composite_order_alias_by_sid.items():
            order_env.outer_composite[_sid] = exp.column(_alias, quoted=True)
        for _sid, _expr in outer_composite_order_expressions.items():
            order_env.outer_composite.setdefault(_sid, _expr)
        # Local slots live in ``_base``. One trimmed from the combined
        # projection (order-only) is named BARE — a ``_base.`` qualifier would
        # dangle under an outer projection-trim wrapper, which exposes only the
        # public aliases — and the bare name still resolves unambiguously
        # against ``_base`` in the combined FROM. A projected one keeps the
        # qualifier.
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

        # Pagination through the dialect strategy (B3). This path used to append
        # raw ``LIMIT``/``OFFSET`` text, which emitted a literal ``LIMIT`` on
        # SQL Server — while the same query carrying a transform layer went
        # through the outer wrap and came out correct.
        combined_statement = self._dialect.apply_pagination(
            combined_statement,
            limit=planned_query.limit,
            offset=planned_query.offset,
        )

        # Outer projection trim — the inner already projects the public
        # list in declared order, so the trim is normally a no-op and is
        # skipped on this cross-model transform-chain path. Future slices
        # may re-enable it.
        return combined_statement.sql(dialect=self.dialect, pretty=True)

    def _guard_target_grain_time_shift(
        self, *, planned_query, slots_by_id, slot_id_by_key,
    ) -> None:
        """Raise the narrowed 7b.15e guard for a ``time_shift`` over a
        target-grain cross-model aggregate — re-aggregating it host-rooted in the
        shifted CTE would multiply target rows through the 1:N join (DEV-1750).

        Target-grain is read from plan ownership: the inner aggregate's
        ``CrossModelAggregatePlan.cte_root_model is None`` (host-rooted isolation
        sets it to the host name). A local inner (no plan) or host-rooted inner
        renders; ``consecutive_periods`` never re-aggregates and is exempt.
        """

        target_rooted_agg_slot_ids = {
            p.aggregate_slot_id
            for p in planned_query.cross_model_aggregate_plans
            if p.cte_root_model is None
        }
        if not target_rooted_agg_slot_ids:
            return
        for layer in planned_query.transform_layers:
            if layer.op != "time_shift":
                continue
            for sid in layer.slot_ids:
                slot = slots_by_id.get(sid)
                if slot is None or not isinstance(slot.key, TransformKey):
                    continue
                inner_sid = slot_id_by_key.get(slot.key.input)
                if inner_sid in target_rooted_agg_slot_ids:
                    raise NotImplementedError(
                        "DEV-1450 stage 7b.15e: time_shift over a TARGET-GRAIN "
                        "cross-model aggregate (its inner aggregate is grouped "
                        "at a joined target's grain, not the host's) is not yet "
                        "rendered — the shifted CTE would re-aggregate it "
                        "host-rooted and multiply target rows through the 1:N "
                        "join. Local and host-grain inner aggregates DO render. "
                        "Factor the temporal transform into an earlier stage, or "
                        "drop the cross-grain part.",
                    )

    def _render_cross_model_transform_chain(  # NOSONAR(S3776) — pre-existing complexity in the window-layer chain; this PR only threaded the CTE-name allocator through it, which re-attributed the function as new code. The chain is rebuilt as sqlglot AST in the scope-assembly PR, where the layering is what gets simplified.
        self,
        *,
        prelude_ctes: List["CteEntry"],
        combined_select: exp.Select,
        planned_query,
        slots_by_id: Dict[str, Any],
        combined_aliases_by_slot_id: Dict[str, List[str]],
        source_relation: str,
        source_model,
        bundle,
        regroup_env: Optional[Dict[Any, exp.Expression]] = None,
        regroup_join_specs: Optional[List[Tuple[str, List[Tuple[Any, str]]]]] = None,
    ) -> str:
        """Render transform layers over a cross-model combined result.

        DEV-1450 stage 7b.15e (C2). The combined cross-model SELECT becomes the
        ``base`` CTE; step CTEs are layered above it exactly like the local
        transform path in ``generate_from_planned``, then an outer wrap projects
        the public slots in user order and applies ORDER BY / LIMIT / OFFSET.

        DEV-1750: window (``cumsum`` / ``lag`` / ``lead`` / ``rank``),
        ``time_shift`` and ``consecutive_periods`` layers all render here — the
        Kahn loop dispatches each op to the SAME per-op emitter the local chain
        uses, so the shifted-CTE join discovery (Part 1) serves both chains.
        Only one shape stays guarded: a ``time_shift`` whose inner aggregate is a
        TARGET-GRAIN cross-model aggregate (``cte_root_model is None``) —
        re-aggregating it host-rooted in the shifted CTE would multiply target
        rows through the 1:N join. ``consecutive_periods`` reads a materialised
        alias and never re-aggregates, so it has no such failure mode.
        """

        ctes: List[CteEntry] = [
            *prelude_ctes,
            CteEntry(
                name="base",
                query=combined_select,
                # The combined SELECT reads ``_base`` and every ``_cm_`` CTE the
                # prelude carries; declaring that is what keeps the assembler
                # from emitting it before them.
                depends_on=[e.name for e in prelude_ctes],
            ),
        ]
        # P-F: this chain previously minted ``step<n>`` names with a
        # bare f-string and held no allocator at all, so nothing connected its
        # names to the ``_cm_*`` CTEs already in ``prelude_ctes`` or to the
        # literal ``base``. Take the generation-scoped allocator (the SAME
        # instance that minted the ``_cm_`` names, so its used-set already
        # covers them) and reserve the inherited literals before allocating.
        cte_allocator = self._gen_allocator or self._new_allocator()
        cte_allocator.reserve(*(entry.name for entry in ctes))
        aliases_by_slot_id: Dict[str, List[str]] = {
            sid: list(a) for sid, a in combined_aliases_by_slot_id.items()
        }
        slot_id_by_key: Dict[Any, str] = {
            s.key: s.id for s in slots_by_id.values()
        }
        available_alias_by_slot_id: Dict[str, str] = {
            sid: a[0] for sid, a in aliases_by_slot_id.items() if a
        }
        # DEV-1817 carriers for the shared per-op emitters (this chain re-roots
        # to the cross-model source, so it builds its own).
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

        # DEV-1750 narrowed 7b.15e guard — runs before the emitter loop so a
        # guarded shape never reaches the shifted CTE.
        self._guard_target_grain_time_shift(
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            slot_id_by_key=slot_id_by_key,
        )

        # 7b.11 — WHERE-able row-phase filters for the shifted CTE (source minus
        # BetweenKey date_range bounds), built once like the local chain. Only a
        # time_shift layer consumes them, so skip the work (and its filter-parse
        # failure surface) for a window-only / cp-only chain.
        if any(
            layer.op == "time_shift" for layer in planned_query.transform_layers
        ):
            shifted_where_parts, shifted_where_join_paths = (
                self._build_shifted_cte_where_parts(
                    planned_query=planned_query,
                    source_relation=source_relation,
                    source_model=source_model,
                    bundle=bundle,
                    regroup_env=regroup_env,
                )
            )
        else:
            shifted_where_parts, shifted_where_join_paths = [], []

        # Transform Kahn batches (one step CTE per ready batch). Ready layers are
        # split by op so each dispatches to the SAME per-op emitter the local
        # chain uses (DEV-1750): window layers to the batch step below, temporal
        # ops to their shifted / cp CTE emitters.
        pending_layers = list(planned_query.transform_layers)
        step_num = 0
        # Explicit chain tail (see the host transform chain above): the CTE the
        # next step reads from, tracked directly instead of as ``ctes[-1]``.
        chain_tail = ctes[-1].name
        while pending_layers:
            (ready_window, ready_time_shift, ready_cp, not_ready) = (
                self._classify_ready_transform_layers(
                    pending_layers=pending_layers,
                    slots_by_id=slots_by_id,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                )
            )
            if not (ready_window or ready_time_shift or ready_cp):
                pending_ops = [layer.op for layer in pending_layers]
                raise RuntimeError(
                    f"DEV-1450 stage 7b.15e: cross-model transform layer "
                    f"dependencies could not be resolved; pending ops: "
                    f"{pending_ops!r}.",
                )
            # This chain dispatches the temporal ops first, then the window batch
            # (the local chain reverses that order — DEV-1799 unifies it).
            chain_tail = self._emit_time_shift_layers(
                ready_time_shift=ready_time_shift,
                chain=chain_state,
                render=render_state,
                shifted_where_parts=shifted_where_parts,
                shifted_where_join_paths=shifted_where_join_paths,
                chain_tail=chain_tail,
            )
            chain_tail = self._emit_cp_layers(
                ready_cp=ready_cp,
                chain=chain_state,
                render=render_state,
                chain_tail=chain_tail,
            )
            if ready_window:
                chain_tail, step_num = self._emit_window_batch_step(
                    ready_window=ready_window,
                    ctes=ctes,
                    chain_tail=chain_tail,
                    cte_allocator=cte_allocator,
                    step_num=step_num,
                    slots_by_id=slots_by_id,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                    aliases_by_slot_id=aliases_by_slot_id,
                    source_relation=source_relation,
                    planned_query=planned_query,
                )
            pending_layers = not_ready

        # Materialise any projected POST-phase arith/scalar slot no window layer
        # rendered (``cumsum(x) + 1``-style combos), then assemble and wrap.
        chain_tail, step_num = self._emit_unmaterialised_post_phase_step(
            ctes=ctes,
            chain_tail=chain_tail,
            cte_allocator=cte_allocator,
            step_num=step_num,
            slot_id_by_key=slot_id_by_key,
            available_alias_by_slot_id=available_alias_by_slot_id,
            aliases_by_slot_id=aliases_by_slot_id,
            source_relation=source_relation,
            planned_query=planned_query,
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

    def _canonical_cross_model_alias(
        self,
        *,
        source_relation: str,
        key,
    ) -> str:
        """Build the canonical result-key alias for a cross-model
        aggregate, IGNORING any user-declared ``name``.

        Used for CTE name + CTE projection alias so per-plan CTEs are
        stable under renames and so multi-alias same-key slots (C13)
        produce ONE shared CTE. The user-facing alias remapping
        happens at the combined SELECT level via ``... AS
        "<public_alias>"``.

        Format: ``<source_relation>.<path>.<canonical_agg_name>``.
        ``canonical_agg_name`` collapses ``*`` to a leading ``_``
        (``*:count`` → ``_count``) per the result-key contract.
        """
        # The derivation lives in ``slayer.sql.naming`` (P-F, one naming
        # authority) — this was one of four drifted copies. The
        # ``cross_model_cte`` profile prefixes BOTH the source relation and the
        # join path, and collapses a source with neither ``leaf`` nor
        # ``column_name`` to the star form.
        #
        # The kwarg suffix is included so two parametric aggregates
        # (``percentile(p=0.5)`` vs ``p=0.95``) get distinct CTE names and
        # column aliases. The deleted legacy pipeline dropped it and thereby
        # collided them — a ratified divergence, pinned by
        # tests/test_dev1744_result_key_contract.py.
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
        """User-facing combined-SELECT aliases for this cross-model slot.

        Each declared ``name`` on the slot (P4 / C13) surfaces as one
        entry. When no user names are declared we return a single
        entry equal to ``canonical_alias`` so the combined SELECT
        projects exactly once. The result is always ``<source_relation>.
        <user_or_canonical>``.
        """
        if not slot.public_aliases:
            return [canonical_alias]
        return [f"{source_relation}.{a}" for a in slot.public_aliases]

    def _render_rerooted_cross_model_cte(
        self,
        *,
        plan,
        bundle,
        host_slots_by_id: Dict[str, Any],
        host_source_relation: str,
    ) -> Tuple[str, List[Tuple[str, str]], str]:
        """Render a cross-model CTE from a nested re-rooted ``PlannedQuery``.

        DEV-1450 stage 7b.15e (C1). The sub-plan is rooted at the TARGET
        model (``FROM target + joins``) so it preserves the host dimension
        grain — the legacy ``_build_rerooted_enriched`` shape, now driven by
        the typed pipeline. Reuses ``generate_from_planned`` to render the
        sub-plan exactly like any base query.

        Returns ``(cte_sql, joinback_pairs, agg_col_alias)``:
        * ``joinback_pairs`` — ``(host_base_alias, cte_column_alias)`` for the
          combined ``LEFT JOIN ON`` (the two sides differ — the host aliases
          dims under its own relation; the CTE under the target relation),
        * ``agg_col_alias`` — the sub-plan's emitted alias for the aggregate.
        """
        sub_plan = plan.rerooted_plan
        # DEV-1503 — filtered-local (host-rooted) plans don't change the
        # source_model: the sub-plan is rooted at the SAME host the outer
        # plan binds against, and ``bundle.source_model`` already IS the
        # host. The existing cross-model re-rooted path swaps ``source_model``
        # to the join target (which lives in ``referenced_models``); a
        # filtered-local host name won't resolve there, so guard on
        # ``cte_root_model`` first.
        if plan.cte_root_model is not None:
            host_model = bundle.source_model
            if host_model is None or host_model.name != plan.cte_root_model:
                raise ValueError(
                    f"Filtered-local CrossModelAggregatePlan "
                    f"cte_root_model={plan.cte_root_model!r} does not match "
                    f"the bundle's source model — planner/renderer drift.",
                )
            rerooted_bundle = bundle
        else:
            target_model = bundle.get_referenced_model(plan.target_model)
            if target_model is None:
                raise ValueError(
                    f"Re-rooted CrossModelAggregatePlan target "
                    f"{plan.target_model!r} not in resolved source bundle.",
                )
            rerooted_bundle = bundle.model_copy(
                update={"source_model": target_model},
            )
        cte_sql = self.generate_from_planned(
            sub_plan, bundle=rerooted_bundle, as_cte_body=True,
        )

        sub_slots_by_id = {
            s.id: s
            for s in (
                list(sub_plan.row_slots)
                + list(sub_plan.aggregate_slots)
                + list(sub_plan.combined_expression_slots)
            )
        }
        target_relation = sub_plan.source_relation

        joinback_pairs: List[Tuple[str, str]] = []
        for host_sid, sub_sid in plan.rerooted_grain_pairs:
            host_slot = host_slots_by_id.get(host_sid)
            sub_slot = sub_slots_by_id.get(sub_sid)
            if host_slot is None or sub_slot is None:
                continue
            host_alias = self._full_alias_for_slot(
                slot=host_slot,
                source_relation=host_source_relation,
                alias_index={},
            )
            cte_alias = self._full_alias_for_slot(
                slot=sub_slot,
                source_relation=target_relation,
                alias_index={},
            )
            joinback_pairs.append((host_alias, cte_alias))

        agg_slot = sub_slots_by_id.get(plan.rerooted_agg_slot_id)
        if agg_slot is None:
            raise RuntimeError(
                f"Re-rooted plan aggregate slot "
                f"{plan.rerooted_agg_slot_id!r} not found in sub-plan.",
            )
        agg_col_alias = self._full_alias_for_slot(
            slot=agg_slot,
            source_relation=target_relation,
            alias_index={},
        )
        return cte_sql, joinback_pairs, agg_col_alias

    @staticmethod
    def _producer_render_bundle(attach, bundle):
        """The bundle a producer renders against. DEV-1836 — a target-rooted
        (cross-model) producer roots its FROM at the aggregate's source model,
        so its render bundle swaps ``source_model`` to that root; a local
        producer keeps the consumer's bundle."""
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
        self, *, producer, bundle,
    ) -> Tuple[List[Tuple[str, exp.Expression]], str]:
        """Render a regroup producer, split into (hoisted CTEs, body SQL) — D2.

        The producer shares THIS generation's allocator (``reuse_allocator``) so
        its own base / step / ``_cm_`` names are globally unique with the parent
        and its internal CTEs can be hoisted into one flat WITH. When the
        producer carries an internal WITH (a windowed / ranked / transform
        producer), those CTEs are returned as ``(name, ast)`` pairs to hoist and
        the body SQL is the bare final SELECT; a producer with no internal WITH
        returns ``([], its verbatim SQL)`` — byte-identical to the pre-hoist
        single-SELECT render.
        """
        producer_sql = self.generate_from_planned(
            planned_query=producer, bundle=bundle, as_cte_body=True,
            reuse_allocator=True, as_hoistable_producer=True,
        )
        parsed = sqlglot.parse_one(producer_sql, dialect=self.dialect)
        self._unmangle_dotted_table_refs(parsed)
        # A producer's internal WITH may be top-level (windowed / ranked path) or
        # nested inside an outer projection-trim SELECT (transform path). Hoist
        # every WITH block into the flat outer chain, leaving the de-WITHed body.
        with_nodes = list(parsed.find_all(exp.With))
        if not with_nodes:
            return [], producer_sql
        # DEV-1824 — the producer's hardcoded base CTE (``_base``/``base``) is not
        # allocator-minted, so hoisting two producers (or a producer + the
        # consumer's own ``_base``) would collide. Rename it to a fresh
        # allocator-minted name (reserved in ``_new_allocator``) and rewrite every
        # reference; the ``_cm_``/``_wm_``/``_rk_``/``step`` names are already
        # globally unique via ``reuse_allocator``.
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
        """Rename a hoisted producer's hardcoded base CTE(s) (``_base``/``base``)
        to fresh allocator-minted names, rewriting every table / column-qualifier
        / CTE-alias reference so the hoisted CTEs remain self-consistent."""
        parsed = with_node.parent
        rename: Dict[str, str] = {}
        for cte in with_node.expressions:
            name = cte.alias_or_name
            if name in ("_base", "base") and name not in rename:
                rename[name] = allocator.allocate_cte(name)
        if not rename:
            return
        # DEV-1835 D9 — a nested windowed producer renders its own grain base as an
        # inline subquery aliased ``_base`` (the collapse form), which SHADOWS a
        # same-named hoisted CTE inside its SELECT. Its ``_base.col`` refs resolve
        # to that local subquery, so the CTE rename must skip them — else the
        # inline alias stays ``_base`` while its refs point at the renamed CTE, an
        # out-of-scope reference. Track the SELECTs that locally define a name.
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
        """Render each DEV-1829 combined regroup producer as a ``_cm_*`` CTE.

        The combined attach substitutes for the DEV-1739 partitioned-measure
        ``CrossModelAggregatePlan``, so its producer renders through the SAME
        machinery: a host-rooted single SELECT with dotted output aliases and
        public / canonical aggregate names, joined back at the combined SELECT.

        Returns ``(ctes, placeholder_to_cm, placeholder_slot_ids, joinbacks)``:
        * ``ctes`` — ``(cte_name, parsed_body)`` per producer;
        * ``placeholder_to_cm`` — placeholder ``ColumnKey`` → ``(cte_name,
          agg_col_alias)`` for combined-scope resolution (projection / composite
          / order);
        * ``placeholder_slot_ids`` — consumer slot ids whose key is a combined
          placeholder (excluded from ``_base``, projected from the producer);
        * ``joinbacks`` — ``(cte_name, [(host_base_alias, producer_grain_alias)])``
          per producer; an empty pair list attaches as a single-row CROSS JOIN.
        * ``shift_specs`` — ``(cte_name, [(host_KEY, producer_grain_alias)])`` in
          the ValueKey form ``_resolve_regroup_attach_conditions`` consumes
          (DEV-1835 D7): a ``time_shift`` over a combined placeholder re-reads the
          SOURCE in its shifted CTE, so the producer must LEFT JOIN there too, its
          host grain resolved against the source scope rather than ``_base``.
        """
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
            # CTE name from the canonical (host-prefixed) alias of the first
            # consumed aggregate — reproduces the DEV-1739 ``_cm_`` seed.
            seed_key = attach.substitutions[0].original_key
            cte_name = cte_name_from_alias(
                prefix="_cm_",
                alias=self._canonical_cross_model_alias(
                    source_relation=source_relation, key=seed_key,
                ),
                allocator=allocator, dialect=self.dialect,
                limit=self._dialect.max_identifier_bytes,
            )
            producer_hoisted, producer_body_sql = self._render_producer_split(
                producer=producer,
                bundle=self._producer_render_bundle(attach=attach, bundle=bundle),
            )
            ctes.extend(producer_hoisted)
            ctes.append((cte_name, self._parse_cte_body(producer_body_sql)))
            producer_relation = producer.source_relation
            for sub in attach.substitutions:
                agg_slot = sub_slots.get(sub.producer_slot_id)
                if agg_slot is None:
                    raise RuntimeError(
                        f"Combined regroup producer is missing aggregate slot "
                        f"{sub.producer_slot_id!r}.",
                    )
                agg_col = self._full_alias_for_slot(
                    slot=agg_slot, source_relation=producer_relation, alias_index={},
                )
                placeholder_to_cm[sub.placeholder] = (cte_name, agg_col)
                ph_slot = slot_by_key.get(sub.placeholder)
                if ph_slot is not None:
                    placeholder_slot_ids.add(ph_slot.id)
            # DEV-1835 D4 — a combined attach whose grain includes a computed
            # dimension carries the RAW grain key (pre-desugar); the host slot
            # carries the DESUGARED one (its inner aggregate replaced by a
            # ``__regroup__`` placeholder). Desugar the host_key with the same
            # ROW-attach substitution map so ``slot_by_key`` finds the host slot.
            row_desugar_map = {
                sub.original_key: sub.placeholder
                for a in planned_query.regroup_attach_plans
                if a.attach_phase == "row"
                for sub in a.substitutions
            }
            pairs: List[Tuple[str, str]] = []
            shift_pairs: List[Tuple[Any, str]] = []
            for host_key, producer_slot_id in attach.join_pairs:
                grain_slot = sub_slots.get(producer_slot_id)
                host_slot = slot_by_key.get(host_key)
                if host_slot is None and row_desugar_map:
                    host_slot = slot_by_key.get(
                        substitute_value_keys(key=host_key, mapping=row_desugar_map),
                    )
                if grain_slot is None or host_slot is None:
                    raise RuntimeError(
                        "Combined regroup attach is missing a host / producer "
                        "grain slot for its join-back.",
                    )
                host_alias = self._full_alias_for_slot(
                    slot=host_slot, source_relation=source_relation, alias_index={},
                )
                grain_alias = self._full_alias_for_slot(
                    slot=grain_slot, source_relation=producer_relation, alias_index={},
                )
                pairs.append((host_alias, grain_alias))
                # The shifted CTE resolves the host key against the SOURCE, so it
                # takes the ValueKey (``host_slot.key``), not the ``_base`` alias.
                shift_pairs.append((host_slot.key, grain_alias))
            joinbacks.append((cte_name, pairs))
            shift_specs.append((cte_name, shift_pairs))
        return (
            ctes, placeholder_to_cm, placeholder_slot_ids, joinbacks, shift_specs,
        )

    @staticmethod
    def _regroup_attach_identity(attach):
        """Structural identity of a regroup attach's producer (DEV-1835 D10): the
        aggregates it computes and the grain it joins on. A ROW attach and a
        COMBINED attach that share this identity are the SAME producer."""
        return (
            frozenset(sub.original_key for sub in attach.substitutions),
            frozenset(host_key for host_key, _ in attach.join_pairs),
        )

    def _prepare_regroup_attaches(  # NOSONAR(S3776) — one linear pass over the planned regroup producers (dedup → render → hoist); splitting would thread the CTE registry through every helper
        self, *, planned_query, bundle, dedup_producers=None,
    ):
        """Render each DEV-1825 regroup producer as a ``_cm_*`` CTE.

        Returns ``(ctes, attached_env, join_specs, reused_cte_names)``:
        * ``ctes`` — one ``CteEntry`` per producer, parsed from its rendered
          single-SELECT body (the producer is guarded to carry no WITH);
        * ``attached_env`` — placeholder ``ColumnKey`` → its producer-CTE column,
          seeded into the base scope so the computed dimension resolves;
        * ``join_specs`` — ``(cte_name, [(host_partition_key, producer_alias)])``
          per producer; an empty pair list attaches as a single-row CROSS JOIN.
        * ``reused_cte_names`` — the COMBINED producer CTEs a dual-role row attach
          reused instead of emitting its own (so ``_base`` declares them as deps).

        ``dedup_producers`` (DEV-1835 D10) maps a producer identity
        (:meth:`_regroup_attach_identity`) to ``(cte_name, {original_key:
        agg_col}, grain_pairs)`` for the COMBINED producers already rendered; a
        ROW attach whose identity matches reuses that CTE — one producer serves
        both roles — instead of shipping a structural duplicate.

        The PARENT allocator mints the ``_cm_`` name before the recursive
        producer render (which installs and restores its own allocator), so
        naming is parent-owned and the producer render is allocator-isolated.
        """
        dedup_producers = dedup_producers or {}
        ctes: List[CteEntry] = []
        attached_env: Dict[Any, exp.Expression] = {}
        join_specs: List[Tuple[str, List[Tuple[Any, str]]]] = []
        reused_cte_names: List[str] = []
        allocator = self._gen_allocator or self._new_allocator()
        for attach in planned_query.regroup_attach_plans:
            # ROW attaches only — a combined attach renders through
            # ``_render_with_cross_model_plans``. Explicit so a future DEV-1824
            # row+combined query can't double-render a combined producer here.
            if attach.attach_phase != "row":
                continue
            # DEV-1835 D10 — a row attach whose producer already exists as a
            # combined producer reuses it: redirect the placeholder env and join
            # spec at the combined CTE, emit no duplicate.
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
            # Flatten each producer output column (``orders.amount_sum`` ->
            # ``amount_sum``) so the CTE exposes DOT-FREE names. A dotted alias
            # inside a WHERE predicate is stringified and re-parsed
            # (``_build_where_having_from_planned``), and a dialect that treats
            # dots as path separators (BigQuery) then mis-splits it — the flat
            # rename is the same fix the user-stage CTEs use.
            def _flat(slot) -> str:
                dotted = self._full_alias_for_slot(
                    slot=slot, source_relation=relation, alias_index={},
                )
                prefix = f"{relation}."
                stripped = dotted[len(prefix):] if dotted.startswith(prefix) else dotted
                return stripped.replace(".", "__")

            producer_hoisted, producer_body_sql = self._render_producer_split(
                producer=producer,
                bundle=self._producer_render_bundle(attach=attach, bundle=bundle),
            )
            expected = [
                _flat(sub_slots[sid]) for sid in producer.projection
                if sid in sub_slots
            ]
            wrapped = build_flat_rename_wrapper(
                source_relation=relation, stage_sql=producer_body_sql,
                expected_columns=expected, dialect=self.dialect,
            )
            # D2 — the producer's own internal CTEs hoist to the flat WITH before
            # its wrapper (which reads them). Empty for a plain grouped-aggregate
            # producer, so the byte-stable single-SELECT case is unchanged.
            for hoisted_name, hoisted_body in producer_hoisted:
                ctes.append(CteEntry(name=hoisted_name, query=hoisted_body))
            ctes.append(CteEntry(
                name=cte_name, query=wrapped,
                depends_on=[name for name, _ in producer_hoisted],
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
        return ctes, attached_env, join_specs, reused_cte_names

    def _render_cross_model_cte(  # NOSONAR(S3776) — single conceptual unit: shared-grain projection + GROUP BY classification + aggregate reroot (source / args / kwargs) + first/last ranked-subquery wrap + target-model-filter qualification + WHERE/HAVING routing. Each block is interdependent state for the same CTE; splitting forces the same cross-cutting state through helpers without simplifying anything.
        self,
        *,
        plan,
        agg_slot,
        full_agg_alias: str,
        bundle,
        planned_query,
        slots_by_id: Dict[str, Any],
        base_projection_ids: Set[str],
    ) -> Tuple[exp.Select, List[str]]:
        """Render one ``_cm_<...>`` CTE body and return it as AST, plus the
        shared-grain alias list (for the outer ``LEFT JOIN ON`` clause).

        AST rather than SQL text: the caller assembles the WITH chain
        structurally, and rendering here only to re-parse there would re-read a
        dotted public alias as a multi-part reference on BigQuery.

        The CTE is rooted at the terminal target model (legacy
        rerooted shape). Shared-grain slots whose key path is a prefix
        of the target_path participate as both projection and GROUP BY
        keys; slots with empty path (host-local dims) are excluded
        since the legacy CROSS JOINs in that case.

        Filter routing reads ``plan.where_filter_ids`` /
        ``plan.having_filter_ids`` / ``plan.target_model_filters`` so
        the CTE renders each route without re-classifying.
        """

        target_model_name = plan.target_model
        target_model = bundle.get_referenced_model(target_model_name)
        if target_model is None:
            raise ValueError(
                f"CrossModelAggregatePlan target {target_model_name!r} "
                f"not in resolved source bundle.",
            )
        target_relation = target_model_name

        target_path = tuple(getattr(agg_slot.key.source, "path", ()))

        # Shared grain: project + GROUP BY any host slot whose key path
        # matches a prefix of target_path. Local-only slots (path=())
        # don't participate at the CTE level; the legacy CROSS JOINs in
        # that case so the host's GROUP BY broadcasts the global agg.
        #
        # Codex HIGH fold-in: the planner's ``shared_grain_slots``
        # currently includes ANY host ROW slot on the target path,
        # including FILTER-ONLY slots that exist in the registry but
        # are not in the host's public projection. A filter-only slot
        # would over-GROUP the CTE and produce a join-back key that
        # ``_base`` never projects (so the outer ``LEFT JOIN _cm_* ON
        # _base."<alias>" = _cm_*."<alias>"`` references a missing
        # column on the left side). Intersect with the host's actual
        # projection ids so only projected slots flow into the CTE.
        cte_select_columns: List[exp.Expression] = []
        cte_group_by: List[exp.Expression] = []
        shared_grain_aliases: List[str] = []
        # DEV-1701: join paths crossed by a shared-grain derived TIME dimension's
        # expanded ``Column.sql``. Collected during the loop (which runs before
        # the CTE scope's join set is assembled) and merged into it below.
        shared_grain_join_paths: List[Tuple[str, ...]] = []
        # DEV-1728: reserve the target's physical column names in the shared
        # generation-wide allocator (Codex F6) so any minted ``_val_<n>``
        # materialisation never collides with a real target column of that name.
        cte_allocator = self._gen_allocator or self._new_allocator()
        self._reserve_model_column_names(cte_allocator, target_model)
        # The CTE's own scope, created before the grain loop so shared-grain
        # refs resolve (and any crossing value materialises) through it.
        cte_scope = self._scope_frame(
            model=target_model, relation=target_relation,
            bundle=bundle, allocator=cte_allocator,
        )
        for sid in plan.shared_grain_slots:
            if sid not in base_projection_ids:
                continue
            slot = slots_by_id.get(sid)
            if slot is None or slot.phase != Phase.ROW:
                continue
            key = slot.key
            path: Tuple[str, ...] = ()
            if isinstance(key, ColumnKey):
                path = key.path
            elif isinstance(key, TimeTruncKey):
                path = key.column.path
            elif isinstance(key, ColumnSqlKey):
                # DEV-1708 / DEV-1728: a plain derived (non-time) dim carries its
                # own path; a path-bearing one renders here like any grain, a
                # host-local ``path == ()`` one falls through to the CROSS-JOIN
                # broadcast below (unchanged).
                path = key.path
            if not path:
                # Local-only host dim — broadcast via CROSS JOIN.
                continue
            if path != target_path[: len(path)]:
                # Off the join path; cross-branch dim doesn't share grain.
                continue
            # Build the column expression rooted at the target model.
            # Single-hop case (path == target_path): bare leaf on target.
            # Multi-hop intermediate case (path < target_path): would
            # need an inner JOIN on the CTE's body. For 7b.12 we accept
            # the single-hop common case and leave intermediate-hop
            # shared grain as a follow-up.
            if path != target_path:
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.12: shared-grain dimension on an "
                    f"intermediate hop ({path!r}) of cross-model agg "
                    f"target_path={target_path!r} not yet rendered in "
                    f"the typed pipeline. Use the terminal-target path "
                    f"or pull the dimension to the host base.",
                )
            # Build the (untruncated) shared-grain column expression rooted at
            # the target relation. A derived (ColumnSqlKey) column — base dim
            # or time dimension — expands its Column.sql rooted at the target;
            # a base column emits the bare ``target.leaf``.

            grain_column = key.column if isinstance(key, TimeTruncKey) else key
            if isinstance(grain_column, ColumnSqlKey):
                # DEV-1728: a derived (ColumnSqlKey) grain — plain dimension OR
                # time dimension — expands its Column.sql rooted at the target and
                # renders here. (The DEV-1708 raise for a PLAIN derived grain is
                # gone: DEV-1713 fixed the naming half, so the host's dotted alias
                # and the CTE join-back now agree.)
                expanded_grain_sql = self._expand_derived_column_sql(
                    source_model=target_model,
                    source_relation=target_relation,
                    column_name=grain_column.column_name,
                    bundle=bundle,
                )
                col_expr = self._parse(expanded_grain_sql)
                leaf = grain_column.column_name
                if not isinstance(key, TimeTruncKey):
                    # DEV-1728: a PLAIN derived grain is CAST to its declared type
                    # to match the host base's ``_wrap_cast_for_type`` (a bare
                    # column ref / TEXT is skipped there and here identically), so
                    # the join-back compares identically-typed values. A
                    # TimeTrunc-wrapped grain keeps ``_build_date_trunc``'s own
                    # temporal shape (no extra cast — parity with the host base).
                    grain_col = next(
                        (c for c in target_model.columns
                         if c.name == grain_column.column_name),
                        None,
                    )
                    col_expr = _wrap_cast_for_type(
                        col_expr, grain_col.type if grain_col else None,
                    )
                # DEV-1701: register every further join the derived grain's
                # expanded sql crosses (rooted at the target relation), so the
                # CTE's FROM pulls it. Merged into the CTE join set below.
                for _p in self._joined_paths_in_sql(
                    sql_expr=col_expr, source_relation=target_relation,
                    source_model=target_model, bundle=bundle,
                ):
                    if _p not in shared_grain_join_paths:
                        shared_grain_join_paths.append(_p)
            else:
                leaf = grain_column.leaf
                col_expr = exp.Column(
                    this=exp.to_identifier(leaf),
                    table=exp.to_identifier(target_relation),
                )
            if isinstance(key, TimeTruncKey):
                col_expr = self._build_date_trunc(
                    col_expr=col_expr,
                    granularity=TimeGranularity(key.granularity),
                )
            # Host-side join-back uses the SAME alias as the host's
            # base projection. For path-bearing slots that's the dotted
            # form (e.g. ``orders.customers.created_at``); the host's
            # ``_build_base_select_for_planned`` already aliases that
            # way for joined ROW slots.
            host_alias = planned_query.source_relation + "." + ".".join(path) + f".{leaf}"
            cte_select_columns.append(col_expr.copy().as_(host_alias))
            cte_group_by.append(col_expr.copy())
            shared_grain_aliases.append(host_alias)

        # Aggregate column: synthesise an ``AggRenderSpec`` ROOTED at the
        # target so ``_build_agg`` resolves the source column on the
        # right model (including ``column_filter_key`` CASE-WHEN).
        # Mutate a copy of the key with ``source.path=()`` so the
        # synthesise helper's local branch fires without re-checking
        # path-based deferrals. DEV-1450 stage 7b.13: also reroot
        # ``ColumnKey`` kwargs whose path matches the source's join path
        # -- a user-qualified kwarg like
        # ``customers.revenue:corr(other=customers.region_id)`` arrives
        # here with both source and ``other`` rooted at ``("customers",)``.
        # Stripping the prefix in lockstep means the synth helper's
        # path-validation invariant (``kwarg.path == source.path``) holds.
        # Re-root the aggregate SOURCE and ALL embedded refs (positional args
        # AND column-valued kwargs) from the host's coordinate system into the
        # target's local scope in one symmetric pass (DEV-1707). Covers a
        # derived (ColumnSqlKey) source like ``customers.net:sum`` — otherwise
        # the host-rooted derived key renders against the wrong alias inside
        # the CTE — and the DEV-1476(c) explicit time arg
        # ``customers.amount:last(customers.signup_at)``, whose positional arg
        # must strip the host prefix in lockstep with the source so the ranked
        # plan qualifies the time column under the target relation.
        # ``column_filter_key`` rides through unchanged
        # (owner-anchored, invariant under reroot).
        cross_model_path = getattr(agg_slot.key.source, "path", ())
        local_agg_key = reroot_aggregate_key(
            agg_slot.key, target_path=cross_model_path,
        )
        # The local_agg_key was built from the target's own column.
        # column_filter_key (if set) carries the canonical filter SQL
        # from the target's Column.filter — the synth helper qualifies
        # bare refs against target_model.
        local_slot = agg_slot.model_copy(update={"key": local_agg_key})

        # DEV-1708 Law 1: every expression rendered into this CTE enters through
        # a single ScopeFrame rooted at the target relation. ``resolve`` anchors
        # each ref and REGISTERS the joins it crosses into ``cte_scope.join_paths``
        # as a side effect — the CTE's FROM is built from that set below, so a
        # crossed join can never be forgotten (replaces the ad-hoc
        # ``_add_cte_join_paths`` closure + per-carrier collectors). The scope
        # shares the generation-wide allocator (``cte_allocator``, hoisted above
        # the grain loop) so ``_val_<n>`` materialisation names (Law 2) are
        # unique across the host base, the grain projections, and every CTE.
        # DEV-1701: merge the shared-grain derived-TIME-dim crossed joins
        # collected in the loop above.
        for _p in shared_grain_join_paths:
            cte_scope.join_paths.add(_p)
        # DEV-1526: register the rerooted aggregate SOURCE's crossed joins (a
        # derived ``ColumnSqlKey`` source like ``customers_v2.deep_pop:sum`` whose
        # ``Column.sql`` = ``regions.population`` must pull the customers_v2 →
        # regions join into the CTE). Registration only — the render spec
        # re-expands the source itself.
        if isinstance(local_agg_key.source, ColumnSqlKey):
            cte_scope.resolve(local_agg_key.source)
        # DEV-1476(c) / Codex F1: register every positional ARG's crossed joins
        # (the explicit first/last time arg may itself be a derived column whose
        # sql crosses a further join — its ranking ORDER BY needs that join).
        for _arg in local_agg_key.args:
            if isinstance(_arg, (ColumnKey, ColumnSqlKey)):
                cte_scope.resolve(_arg)
        # DEV-1527 (cross-model remainder): resolve each column-ref KWARG through
        # the scope — anchors the expanded expression AND registers its join —
        # then embed it as a trusted ``kind="expr"`` into the render spec so a
        # derived kwarg emits its expanded sql (``regions.weight``) instead of a
        # bare, non-existent ``customers_v2.deep_weight``.
        cte_resolved_kwargs: "Dict[str, ResolvedAggKwarg]" = {}
        for _kname, _kval in local_agg_key.kwargs:
            if isinstance(_kval, (ColumnKey, ColumnSqlKey)):
                cte_resolved_kwargs[_kname] = ResolvedAggKwarg(
                    kind="expr", value=cte_scope.resolve(_kval),
                )
        # DEV-1745 (W2) + DEV-1743: the aggregation's template FRAGMENTS — string
        # kwargs plus non-overridden ``AggregationParam.sql`` defaults — substitute
        # into the CTE's aggregate expression, so the joins they cross belong in
        # this CTE's FROM AND their alias-rewritten form must be embedded (a
        # multi-hop dotted fragment ``customers.regions.weight`` becomes the
        # ``customers__regions`` alias, not a dotted-unbound ``regions``). Resolve
        # them here — BEFORE the spec is built — through the same door the host
        # path uses, so the two cannot drift apart again.
        for _fname, _fast in self._register_fragment_kwarg_joins(
            key=local_agg_key, scope=cte_scope, model=target_model,
        ).items():
            cte_resolved_kwargs.setdefault(
                _fname, ResolvedAggKwarg(kind="expr", value=_fast),
            )

        synth = self._build_agg_render_spec_from_planned(
            slot=local_slot,
            key=local_agg_key,
            source_model=target_model,
            source_relation=target_relation,
            full_alias=full_agg_alias,
            bundle=bundle,
            resolved_agg_kwargs=cte_resolved_kwargs or None,
        )

        # WHERE: target-model-filters (qualified bare-identifier refs
        # so ``deleted_at IS NULL`` becomes ``customers.deleted_at IS
        # NULL`` so they resolve against the target model, via the
        # Mode-A door) + host filters routed to WHERE. Computed up-front
        # so the first/last branch can push them INSIDE the ranked
        # subquery — otherwise rows excluded by a filter could still
        # win ``_last_rn = 1`` and yield NULL aggregates.
        # DEV-1494: join paths the CTE's own filters cross — the target measure's
        # ``Column.filter`` and the target-model filters — registered into the
        # CTE scope (Law 1). Each ``_cm_*`` CTE is an isolated per-(target, grain)
        # computation, so adding these joins to ITS FROM affects only this
        # measure (not siblings) — it resolves the filter's refs without the
        # cross-measure cardinality concern DEV-1503 owns. Free-SQL predicates
        # keep the quote-tolerant dual-scan (raw + inline-expanded — the
        # DEV-1494/dedup contract) now performed by the Mode-A door
        # (``ScopeFrame.enter_predicate``), writing into the single
        # ``cte_scope.join_paths`` set.
        def _register_filter_join_paths(sql_text: Optional[str]) -> None:
            if not sql_text:
                return
            self._enter_mode_a_predicate(
                sql=sql_text, scope=cte_scope,
                location=(
                    f"Column.filter on cross-model target "
                    f"{target_model_name!r}"
                ),
            )

        if local_agg_key.column_filter_key is not None:
            _register_filter_join_paths(local_agg_key.column_filter_key.canonical_sql)

        # The aggregation's template fragments are resolved (and their joins
        # registered into ``cte_scope``) ABOVE, before the render spec is built,
        # so the resolved fragment can be embedded into it (DEV-1743).

        where_parts: List[exp.Expression] = []
        for filter_text in plan.target_model_filters:
            # DEV-1450 #4b / DEV-1494: a target model filter referencing a
            # non-trivial derived column on the target (bare OR a dotted ref to a
            # derived column on a joined model) is inline-expanded; base-only
            # filters keep the AST bare-ref qualification. The crossed join is
            # pulled into this CTE's FROM via ``cte_scope.join_paths``.
            # An empty entry contributes no predicate. Skipped rather than
            # entered: the door raises on text it cannot parse, and "" is not a
            # predicate — the path this replaced skipped it too.
            if not filter_text:
                continue
            where_parts.append(self._enter_mode_a_predicate(
                sql=filter_text, scope=cte_scope,
                location=(
                    f"SlayerModel.filters on cross-model target "
                    f"{target_model_name!r}"
                ),
            ))
        # DEV-1708 / Codex F4: pre-pass — walk the FULL ValueKey tree of every
        # routed WHERE and HAVING filter (nested arithmetic / boolean / IN
        # operands, aggregate leaves' source + args + kwargs + column_filter,
        # derived ColumnSqlKey refs) and register the joins they cross into the
        # CTE scope BEFORE the FROM is built. HAVING is rendered later (it needs
        # the ranked-subquery rn maps), so its joins would otherwise register
        # too late to reach the FROM.
        self._register_routed_filter_joins(
            planned_query=planned_query,
            filter_ids=list(plan.where_filter_ids) + list(plan.having_filter_ids),
            scope=cte_scope,
            target_path=target_path,
        )
        cte_where = self._collect_routed_filters(
            planned_query=planned_query,
            filter_ids=plan.where_filter_ids,
            target_relation=target_relation,
            target_model=target_model,
            bundle=bundle,
        )
        if cte_where is not None:
            where_parts.append(cte_where)
        combined_where: Optional[exp.Expression] = None
        if where_parts:
            combined_where = (
                exp.and_(*where_parts) if len(where_parts) > 1 else where_parts[0]
            )

        # FROM: target table directly, OR a ROW_NUMBER-ranked subquery for
        # first/last. Build the ranked subquery FIRST so its rank-column
        # maps — including the filtered ``_last_rn_fN`` / ``_match_fN``
        # columns emitted when the measure's source column carries a
        # ``Column.filter`` — can be threaded into ``_build_agg``. Without
        # the filtered maps the agg references a bare ``_last_rn`` the
        # subquery never projects. WHERE is pushed INSIDE so RN is computed
        # over the filtered row set; otherwise a filtered-out row could win
        # ``_last_rn = 1`` and the ``MAX(CASE WHEN _last_rn = 1 ...)``
        # aggregate would return NULL.
        cte_join_paths = cte_scope.join_paths.as_list()
        if cte_join_paths:
            target_from, cte_base_joins = self._build_from_and_joins(
                source_model=target_model, source_relation=target_relation,
                joined_paths=cte_join_paths, bundle=bundle,
            )
        else:
            target_from = self._build_from_clause_from_planned(
                source_model=target_model, source_relation=target_relation,
            )
            cte_base_joins = []
        agg_expr, is_agg = self._build_agg(synth)
        if is_agg:
            agg_expr = _wrap_cast_for_type(agg_expr, agg_slot.cast_type)
        cte_select_columns.append(agg_expr.copy().as_(full_agg_alias))

        # Assemble the CTE Select now that every projected column (shared
        # grain + aggregate) is in ``cte_select_columns``.
        cte_select = exp.Select()
        for col in cte_select_columns:
            cte_select = cte_select.select(col)
        cte_select = cte_select.from_(target_from)
        cte_select = _apply_joins(select=cte_select, joins=cte_base_joins)
        if combined_where is not None:
            cte_select = cte_select.where(combined_where)

        if cte_group_by:
            for gb in cte_group_by:
                cte_select = cte_select.group_by(gb)

        cte_having = self._collect_routed_filters(
            planned_query=planned_query,
            filter_ids=plan.having_filter_ids,
            target_relation=target_relation,
            target_model=target_model,
            bundle=bundle,
        )
        if cte_having is not None:
            cte_select = cte_select.having(cte_having)

        return cte_select, shared_grain_aliases

    def _register_routed_filter_joins(  # NOSONAR(S3776) — a cohesive recursive ValueKey tree-walk dispatcher (the heavy AggregateKey arm is already extracted to _register_agg_key_joins); the remaining branches are the closed-union dispatch contract, mirroring the sibling walkers _value_key_join_paths / _collect_base_aux_slot_ids in this file.
        self,
        *,
        planned_query,
        filter_ids: List[str],
        scope: ScopeFrame,
        target_path: Tuple[str, ...],
    ) -> None:
        """DEV-1708 / Codex F4 — register the joins crossed by every routed
        WHERE/HAVING filter into ``scope.join_paths``, walking the FULL
        ``ValueKey`` tree so nested arithmetic/boolean/IN operands and aggregate
        leaves (source + positional args + column-ref kwargs + ``column_filter``)
        all contribute BEFORE the CTE FROM is assembled.

        Registration only — the render passes (``_collect_routed_filters`` for
        WHERE, and the HAVING render below) emit the SQL themselves. The scope's
        ``resolve`` anchors each typed leaf at the target relation and records
        the path it crosses; free-SQL ``column_filter`` predicates keep the
        quote-tolerant dual-scan of the Mode-A door (``ScopeFrame.enter_predicate``).
        """

        if not filter_ids:
            return
        wanted = set(filter_ids)

        def _walk(vk) -> None:
            if isinstance(vk, (ColumnKey, ColumnSqlKey)):
                # Reroot a path-qualified leaf into the target's local scope by
                # stripping the CTE's ``target_path`` prefix (NOT the ref's own
                # path — a ref one hop past the target keeps its residual so the
                # deeper join still registers), then resolve.
                local = (
                    _reroot_path_ref(vk, target_path=target_path)
                    if vk.path else vk
                )
                scope.resolve(local)
            elif isinstance(vk, AggregateKey):
                self._register_agg_key_joins(agg_key=vk, scope=scope)
            elif isinstance(vk, ArithmeticKey):
                for op in vk.operands:
                    _walk(op)
            elif isinstance(vk, ScalarCallKey):
                for a in vk.args:
                    _walk(a)
            elif isinstance(vk, BetweenKey):
                _walk(vk.column)
                _walk(vk.low)
                _walk(vk.high)
            elif isinstance(vk, InKey):
                _walk(vk.column)

        for fp in planned_query.filters_by_phase:
            if fp.id in wanted and fp.expression is not None:
                _walk(fp.expression.value_key)

    def _register_agg_key_joins(
        self, *, agg_key, scope: ScopeFrame,
    ) -> None:
        """Register the joins an aggregate leaf crosses (source + positional
        args + column-ref kwargs + ``column_filter``) into ``scope.join_paths``
        — the ``AggregateKey`` arm of ``_register_routed_filter_joins``'s tree
        walk, extracted so the walker stays a thin dispatcher (DEV-1708).

        Takes ONLY the scope. It used to also receive ``target_relation`` /
        ``target_model`` / ``bundle`` and scan the column filter against those
        while registering the result on ``scope`` — two sources of truth for one
        fact. They agree at the single call site (the CTE scope is built with
        ``root_model=target_model, root_relation=target_relation``), but nothing
        enforced that, so a future caller could have passed a scope rooted
        elsewhere and had its refs resolved in the wrong namespace. Now the
        scope is the only namespace, and the question cannot arise.
        """

        cross_model_path = getattr(agg_key.source, "path", ())
        local_agg = reroot_aggregate_key(agg_key, target_path=cross_model_path)
        if isinstance(local_agg.source, ColumnSqlKey):
            scope.resolve(local_agg.source)
        for a in local_agg.args:
            if isinstance(a, (ColumnKey, ColumnSqlKey)):
                scope.resolve(a)
        for _k, v in local_agg.kwargs:
            if isinstance(v, (ColumnKey, ColumnSqlKey)):
                scope.resolve(v)
        cfk = local_agg.column_filter_key
        if cfk is not None and cfk.canonical_sql:
            self._enter_mode_a_predicate(
                sql=cfk.canonical_sql, scope=scope,
                location=f"Column.filter on model {scope.root_model.name!r}",
            )

    def _reroot_routed_leaf(self, key, *, target_relation: str, target_model):
        """Re-root a routed COLUMN leaf (``ColumnKey`` / ``ColumnSqlKey``) to the
        CTE-local scope, or return ``None`` when ``key`` is not a column leaf.

        Host-rooted refs carry a ``__``-path; inside the CTE the join to the
        target is direct, so the path is stripped and the ref anchors at
        ``target_relation``. Both leaf kinds reject an intermediate-hop path
        (not ending at the target) symmetrically (DEV-1769); the ColumnSqlKey
        guard is unreachable for binder keys (``model == path[-1]``,
        ``target_relation == target_model.name``) so it fails closed on
        inconsistent hand-built / deserialized ones."""

        if isinstance(key, ColumnKey):
            if key.path and key.path[-1] != target_relation:
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.12: cross-model filter on an "
                    f"intermediate hop ({key.path!r}) not yet rendered in "
                    f"the typed pipeline.",
                )
            return ColumnKey(path=(), leaf=key.leaf) if key.path else key
        if isinstance(key, ColumnSqlKey):
            if key.model != target_model.name:
                raise NotImplementedError(
                    f"DEV-1450: cross-model filter on derived column "
                    f"{key.column_name!r} owned by {key.model!r} "
                    f"(not the CTE target {target_model.name!r}) is not yet "
                    f"rendered in the typed pipeline.",
                )
            if key.path and key.path[-1] != target_relation:
                raise NotImplementedError(
                    f"DEV-1769: cross-model filter on derived column "
                    f"{key.column_name!r} via an intermediate-hop path "
                    f"({key.path!r}) not yet rendered in the typed pipeline.",
                )
            return (
                ColumnSqlKey(path=(), model=key.model, column_name=key.column_name)
                if key.path else key
            )
        return None

    def _reroot_routed_filter_key(self, key, *, target_relation: str, target_model):
        """Re-root a routed filter key to the cross-model CTE's LOCAL scope so
        ``render_value_key`` resolves each leaf against the target relation the
        way the legacy target-scope renderer did (DEV-1763 P-G).

        Column leaves reroot via :meth:`_reroot_routed_leaf`; composites rebuild
        with rerooted children. ``AggregateKey`` leaves are left intact — the
        HAVING seam reroots them via ``reroot_aggregate_key``."""

        leaf = self._reroot_routed_leaf(
            key, target_relation=target_relation, target_model=target_model,
        )
        if leaf is not None:
            return leaf
        if isinstance(key, ArithmeticKey):
            return ArithmeticKey(
                op=key.op,
                operands=tuple(
                    self._reroot_routed_filter_key(
                        o, target_relation=target_relation, target_model=target_model,
                    )
                    for o in key.operands
                ),
            )
        if isinstance(key, ScalarCallKey):
            return ScalarCallKey(
                name=key.name,
                args=tuple(
                    self._reroot_routed_filter_key(
                        a, target_relation=target_relation, target_model=target_model,
                    )
                    if isinstance(a, _FrozenKey) else a
                    for a in key.args
                ),
            )
        if isinstance(key, BetweenKey):
            return BetweenKey(
                column=self._reroot_routed_filter_key(
                    key.column, target_relation=target_relation, target_model=target_model,
                ),
                low=self._reroot_routed_filter_key(
                    key.low, target_relation=target_relation, target_model=target_model,
                ),
                high=self._reroot_routed_filter_key(
                    key.high, target_relation=target_relation, target_model=target_model,
                ),
            )
        if isinstance(key, InKey):
            return InKey(
                column=self._reroot_routed_filter_key(
                    key.column, target_relation=target_relation, target_model=target_model,
                ),
                values=key.values,
                negated=key.negated,
            )
        # LiteralKey / AggregateKey (rerooted by the HAVING seam) pass through.
        return key

    def _target_scope_agg_builder(self, *, target_model, target_relation: str, bundle):
        """The routed-HAVING seam (DEV-1763 P-G): render a cross-model aggregate
        against the CTE's LOCAL scope. Symmetric reroot (DEV-1707) qualifies the
        aggregate's source / args / kwargs under ``target_relation`` rather than
        the host ``__``-path alias, then the same synth + ``_build_agg`` path the
        projection uses emits it. The LIVE path (``first_last_state is None``)
        threads no rn-state and uses the ``{target_relation}._having_agg`` alias;
        the first/last variant stays on the legacy renderer (escape hatch)."""

        def build(agg_key, _slot, _having_full_alias) -> exp.Expression:

            cross_model_path = getattr(agg_key.source, "path", ())
            local_agg = reroot_aggregate_key(agg_key, target_path=cross_model_path)
            tmp_slot = ValueSlot(
                id="_cte_having_tmp", key=local_agg, declared_name="_having_agg",
                phase=agg_key.phase, type=None,
            )
            synth = self._build_agg_render_spec_from_planned(
                slot=tmp_slot, key=local_agg, source_model=target_model,
                source_relation=target_relation,
                full_alias=f"{target_relation}._having_agg", bundle=bundle,
            )
            expr, _ = self._build_agg(synth)
            return expr

        return build

    def _collect_routed_filters(
        self,
        *,
        planned_query,
        filter_ids: List[str],
        target_relation: str,
        target_model,
        bundle,
    ) -> Optional[exp.Expression]:
        """Build a conjunction of bound filter predicates by ID.

        Filters routed into a cross-model CTE bind in the CTE's local
        scope (``customers.status`` resolves to the target's table).
        Each filter renders through ``render_value_key`` on a
        target-rooted scope (DEV-1763 P-G).

        Returns ``None`` when the requested filter set is empty so the
        caller can skip emitting WHERE / HAVING.
        """
        if not filter_ids:
            return None
        wanted = set(filter_ids)

        scope = self._throwaway_frame(
            model=target_model, relation=target_relation, bundle=bundle,
        )
        ctx = RenderContext(
            scope=scope,
            dialect=self._dialect,
            filters=FilterFacilities(
                agg_builder=self._target_scope_agg_builder(
                    target_model=target_model,
                    target_relation=target_relation,
                    bundle=bundle,
                ),
                cast_column_sql=False,
                paren_comparison_operands=False,
            ),
        )
        parts: List[exp.Expression] = []
        for fp in planned_query.filters_by_phase:
            if fp.id not in wanted or fp.expression is None:
                continue
            local_key = self._reroot_routed_filter_key(
                fp.expression.value_key,
                target_relation=target_relation,
                target_model=target_model,
            )
            parts.append(render_value_key(key=local_key, ctx=ctx))
        if not parts:
            return None
        return exp.and_(*parts) if len(parts) > 1 else parts[0]

    def _full_alias_for_slot(
        self,
        *,
        slot,
        source_relation: str,
        alias_index: Dict[str, int],
    ) -> str:
        """Build the SQL public alias for one ``ValueSlot``.

        Local slots use the legacy ``<source_relation>.<alias>`` form
        where ``alias`` is the user-declared name (cycled via
        ``_pick_alias_for_planned_slot`` for C13 multi-alias slots) or
        the planner's canonical ``declared_name``.

        DEV-1450 stage 7b.12: joined ROW slots emit the FULL dotted
        result-key form (``orders.customers.region_id``), preserving
        the result-key contract (P10). The planner's flat
        ``declared_name`` is the DEV-1449 / C4 downstream-stage binding
        name and remains untouched on the slot for stage-2 references;
        only the public SQL alias differs.

        DEV-1713 (D3 / DEV-1495 bug 1): the ROW branch covers all three
        row key shapes — ``ColumnKey``, ``ColumnSqlKey`` (a joined DERIVED
        column, which previously fell through to the flat ``declared_name``
        and surfaced as ``orders.customers__revenue``), and ``TimeTruncKey``
        over either. All route through :func:`slayer.sql.naming.result_key`,
        the single owner of the dotted form; response_meta mirrors this
        via the same builder so the two producers cannot drift.
        """

        if slot.phase == Phase.ROW:
            key = slot.key
            path: Tuple[str, ...] = ()
            leaf: Optional[str] = None
            if isinstance(key, ColumnKey):
                path, leaf = key.path, key.leaf
            elif isinstance(key, ColumnSqlKey):
                # DEV-1713: a joined derived column's leaf is its column_name.
                path, leaf = key.path, key.column_name
            elif isinstance(key, TimeTruncKey):
                # DEV-1450 #4a: a derived TD's leaf is its column_name, so the
                # public result-key shape matches the base-column TD.
                path, leaf = column_path(key.column), column_leaf(key.column)
            if path and leaf is not None:
                return result_key(
                    source_relation=source_relation, path=path, leaf=leaf,
                )
        # Local + AGGREGATE / POST slots: existing alias selection. The alias
        # may embed hop dots (a cross-model measure alias such as
        # ``customers.revenue_sum``), so use the canonical-alias builder.
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
        """Walk ROW slots in render order to collect unique joined DIMENSION
        paths needed for projection / GROUP BY.

        Cross-model aggregate slots are NEVER walked — their joins live in
        ``CrossModelAggregatePlan.join_chain`` and render inside the per-plan
        ``_cm_*`` CTE. Local ``first`` / ``last`` explicit-time-arg joins are no
        longer collected here either: DEV-1710 Stage 6 moved that discovery into
        ``_resolve_agg_inputs_via_scope`` (sub-pass 4), where anchoring the arg
        through the host ``ScopeFrame`` registers its crossed join as a Law-1
        side effect (bare, derived, and multi-hop args alike).

        ``order_slot_ids`` (DEV-1703 Phase 1) walks ORDER BY targets for the
        same paths. An order-only joined row column is deliberately NOT in
        ``base_render_order`` (materialising it there would project it and add
        it to GROUP BY, changing the result grain), but the split reference the
        ORDER BY emits still needs its join bound in the base FROM — Law 1
        applies to a sort key exactly as it does to a filter ref.
        """

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
        """Build ``(from_expr, joins)`` for a base SELECT.

        ``from_expr`` is the single-source Table/Subquery (same shape
        ``_build_from_clause_from_planned`` would return). ``joins`` is
        a list of ``(join_expr, on_expr, join_type)`` tuples the caller
        attaches via ``Select.join`` after constructing the SELECT.

        Single-hop paths use the target's bare name as the table alias
        (matching legacy: ``LEFT JOIN customers AS customers ON ...``);
        multi-hop paths use the ``__``-delimited path alias for non-
        leading hops (``LEFT JOIN regions AS customers__regions ON
        ...``). The cross-model rerooted CTE re-uses this helper rooted
        at the terminal target model with an empty join list, so the
        same FROM shape applies.
        """
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
                        # DEV-1645: the join keys are physical DB columns —
                        # quote them when mixed-case (``merchantId``) via
                        # ``_to_ident`` so a case-folding backend resolves them;
                        # the table qualifiers are SLayer-internal aliases
                        # (reserved names quote at emit via RESERVED_KEYWORDS).
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
                        # DEV-1686 reserved-word alias + DEV-1645 mixed-case
                        # physical-name quoting: ``FROM "Order" AS "order"``.
                        join_expr = self._to_table(target_table, alias=next_alias)
                    on_expr = (
                        exp.and_(*join_on_parts)
                        if len(join_on_parts) > 1
                        else join_on_parts[0]
                    )
                    # Honor the model's declared join_type (default LEFT so a
                    # measure never changes cardinality; explicit INNER when the
                    # user declared it — e.g. existence-filter joins). Legacy
                    # rendered ``jtype.upper()`` here (generator.py:835/1242).
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
        """Resolve a dimension column expression on either the host
        model (empty path) or a joined target (non-empty path).

        For empty paths this delegates to ``_dim_column_expr_from_planned``
        which respects ``Column.sql`` for derived columns. For joined
        paths the legacy emits a bare ``<target_alias>.<leaf>`` column
        ref — matching that shape so parity comparisons hold.
        """
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
        # Legacy emits the bare-table.column form for joined dims even
        # when the column has a ``Column.sql`` override on the target;
        # mirror that for parity.
        return exp.Column(
            this=exp.to_identifier(leaf),
            table=exp.to_identifier(current_alias),
        )

    def _window_ordered(self, col: exp.Expression, *, descending: bool = False) -> exp.Ordered:
        """One ``ORDER BY`` term INSIDE an ``OVER (…)`` clause.

        Not :meth:`SqlDialect.build_ordered`: a window's frame ordering is
        internal machinery, not a user-visible sort, so it takes the emitter's
        own null ordering rather than SLayer's nulls-last policy — which on a
        dialect without NULLS syntax would expand into a ``CASE WHEN … IS
        NULL`` term inside the frame and change which rows the frame covers.
        """
        args: Dict[str, Any] = {
            "this": col,
            "nulls_first": self._dialect.native_nulls_first(
                descending=descending,
            ),
        }
        if descending:
            # ``desc=False`` would emit an explicit ``ASC``; leaving the key
            # off emits the bare column, which is what ascending means.
            args["desc"] = True
        return exp.Ordered(**args)

    @staticmethod
    def _transform_grain_slot_ids(*, planned_query, slots_by_id) -> List[str]:
        """The transform auto-grain (DEV-1837 D1/D2, Option A): every projected
        ROW-phase dimension slot — plain / derived columns, computed dims, and
        row-attach placeholder dims — excluding time buckets (the ordering axis)
        and combined-attach placeholder slots (an attached measure value must
        never widen a grain). Placeholder roles are read structurally from the
        attach plans' substitutions, never from leaf text."""
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
        """Render one window-transform slot as an ``OVER()`` expression.

        Returns AST (DEV-1747 D8). It used to return a SQL string, which the
        caller then spliced into an f-string CTE body — so every dotted public
        alias this path carries (``orders.rev``) made a round trip through text
        before reaching the assembler, and on a dialect that mangles dots at
        emission a re-parse reads such an alias as a multi-part reference.

        Auto-partition is the shared transform grain
        (``_transform_grain_slot_ids``) for non-rank ops; rank-family defaults
        to no PARTITION BY.
        """

        key = slot.key
        if not isinstance(key, TransformKey):
            raise ValueError(
                f"_render_window_transform_sql expected TransformKey, "
                f"got {type(key).__name__}",
            )

        # Composite transform inputs — a transform whose ``input`` is an
        # arithmetic / scalar-call expression rather than a slotted leaf
        # (``cumsum(amount:sum / qty:sum)``; ``cumsum(change(x))`` which
        # lowers to ``cumsum(x - time_shift(x))``). Render the input
        # expression INLINE against the operands' already-materialised
        # aliases — the Kahn readiness check (``_transform_layer_deps_ready``
        # → ``_ready(tk.input)``) guarantees every operand slot is in a
        # prior CTE before this layer runs, so no extra inner CTE is needed.

        if isinstance(key.input, (ArithmeticKey, ScalarCallKey)):
            measure = render_value_key(
                key=key.input,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                ),
            )
        else:
            # Resolve input alias (slotted leaf).
            input_sid = slot_id_by_key.get(key.input)
            if input_sid is None or input_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"transform input not materialised: slot id={slot.id!r}, "
                    f"op={key.op!r}, input_key={key.input!r}.",
                )
            measure = exp.column(
                available_alias_by_slot_id[input_sid], quoted=True,
            )

        # Resolve time-key alias (None for rank-family without time).
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

        # Resolve partition aliases. Explicit partition_keys take
        # precedence; otherwise auto-partition by query dimension slots
        # (ColumnKey row-phase, hidden==False) — NOT TimeTruncKey slots
        # (partition by the query's dimension aliases).
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
            """``fn OVER (PARTITION BY … ORDER BY … <frame>)``.

            Built rather than formatted so the partition and order columns stay
            single quoted identifiers all the way to emission — the dotted
            public aliases here (``orders.rev``) are exactly the shape a text
            round trip re-reads as a multi-part reference.
            """
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
        #: The rank family orders by the MEASURE descending, not by time.
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
            """Reject bool / non-integral periods; accept int / integral
            Decimal. Mirrors the strict validation the binder applies to
            ``ntile.n`` and ``time_shift.periods``."""
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
            # Route through the shared normaliser (as lag / lead do) so bool is
            # rejected and a non-integral Decimal raises rather than truncating
            # (DEV-1783). The binder gates this too; this is the render-side
            # defense-in-depth.
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
            # ``last`` is ``first`` over the REVERSED time axis, so it takes
            # the descending order rather than ``time_order``.
            return _over(
                exp.FirstValue(this=measure),
                order=exp.Order(expressions=[
                    self._window_ordered(time_col.copy(), descending=True),
                ]),
                spec=unbounded_frame,
            )
        raise NotImplementedError(
            f"DEV-1450 stage 7b.10: transform op {op!r} not in the "
            f"window-transform slice scope.",
        )

    def _render_post_phase_filter_conditions(  # NOSONAR(S3776) — one cohesive walk of every POST-phase filter producing the outer-WHERE conditions: per-filter slot-id lookup, expr rebuild (Compare / BoolOp / UnaryOp / scalar wraps), alias resolution. Splitting hides the shared registry / alias-map state both wrap-CTE and outer-WHERE emission depend on.
        self,
        *,
        planned_query,
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
    ) -> List[str]:
        """Render each POST-phase ``FilterPhase.expression`` to a SQL
        string suitable for the outer ``WHERE`` after the CTE chain.

        Walks the typed value-key tree. Slot-worthy keys
        (``AggregateKey`` / ``TransformKey`` / row-phase columns) are
        replaced with quoted alias refs (``"orders.cumsum_amount_sum"``)
        looked up through ``slot_id_by_key`` /
        ``available_alias_by_slot_id``. Arithmetic / scalar-call
        composition uses the same operator dispatch as the WHERE
        renderer in ``render_value_key``.
        """

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
        """Wrap ``chain_sql`` in the public-projection outer SELECT, through
        the dialect strategy (DEV-1716).

        Delegates to ``SqlDialect.emit_outer_wrap`` rather than string-building
        ``FROM (<chain>) AS _outer`` inline. T-SQL overrides that hook to hoist
        the inner top-level CTEs onto the outer statement, because SQL Server
        accepts ``WITH`` only as a statement prefix and rejects
        ``FROM (WITH ... SELECT ...) AS _outer`` with "Incorrect syntax near
        the keyword 'WITH'" (DEV-1571 Bug 1). Every other dialect gets the
        base impl, whose output is byte-identical to the previous inline
        string.

        ORDER BY / LIMIT / OFFSET are resolved here from the typed plan (slot
        id -> materialised alias) and handed to the hook as AST, since the
        T-SQL override also transposes pagination to ``TOP`` /
        ``FETCH NEXT n ROWS ONLY``.
        """
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
        """ORDER BY terms for a plan whose sort keys resolve to CTE-chain
        aliases (§5.10).

        Every value the chain materialised is one column of the wrapped
        subquery by the time the outer wrap is emitted, so the producing scope
        no longer distinguishes anything here — hence
        :meth:`OrderEnv.uniform`. Built as AST rather than rendered to text and
        re-parsed: SLayer's aliases are dotted, and a re-parse re-reads
        ``"orders.cs"`` as a multi-part reference on a dialect that mangles
        dots at emission.
        """
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
        """Build the WHERE clauses for the shifted CTE that re-aggregates
        the source relation, plus the join paths those clauses cross.

        7b.3c invariant, generalised by DEV-1732: a FRAME BOUND must be omitted
        from the shifted inner CTE so the earliest visible bucket can still
        carry a non-null shifted value. That covers the ``BetweenKey`` a
        ``date_range`` produces AND the explicit relational spelling of the same
        intent (``created_at >= '2024-01-01'``), which used to be propagated —
        so the two spellings gave different numbers. A filter that is only
        PARTLY a frame bound propagates as its residual population predicate.

        Other ROW-phase filters (e.g. ``status = 'active'``) are propagated
        unchanged so the shifted aggregation runs over the same row population.
        AGGREGATE / POST phase filters never apply to the shifted CTE
        (they're outer-projection concerns).

        DEV-1711: a ROW filter referencing a JOINED column
        (``stores.name = 'North'``) is now supported — the shifted CTE is a
        real ``ScopeFrame`` whose FROM pulls the join, so the guard that used
        to raise on joined refs is gone. The returned ``crossed_paths`` list is
        registered into the caller's shifted scope so the LEFT JOIN the filter
        needs is emitted. Filters over the same join set the base already
        applies keep population parity between ``_base`` and the shifted CTE.
        """

        out: List[str] = []
        crossed_paths: List[Tuple[str, ...]] = []
        # DEV-1732: the frame-bound column set is computed once by the planner
        # and carried on the plan, so this path and the windowed ``_src`` path
        # cannot drift apart.
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
        """Render one ROW-phase filter for the shifted CTE, returning its SQL
        plus the join paths it crosses — or ``None`` to omit it entirely.

        A filter that is wholly a FRAME BOUND on one of ``time_columns`` is
        omitted; one that is partly a frame bound renders as its residual
        population predicate (DEV-1732). This subsumes the old
        ``isinstance(..., BetweenKey)`` special case: a ``date_range``'s
        ``BetweenKey`` column is always a query time dimension's raw column, so
        ``strip_frame_bounds`` returns ``None`` for it — same behaviour, one
        rule.

        Mode-A ``text`` filters are exempt from the analysis and always
        propagate (a model filter defines which rows EXIST, not the frame).

        ``time_columns`` is REQUIRED, deliberately (Codex): ``strip_frame_bounds``
        returns its input unchanged for an empty set, so a default would let a
        future caller silently start rendering every ``date_range`` into the
        shifted CTE — the exact 7b.3c regression this method exists to prevent.

        The join paths are collected per carrier kind (CodeRabbit): a TYPED
        filter is scanned STRUCTURALLY on its already-rendered AST via
        ``_joined_paths_in_sql`` — the expression is fully qualified/expanded,
        so its crossed joins are visible directly and there is no text
        round-trip that could silently swallow a parse failure. A Mode-A
        ``text`` filter has only its string form, so it keeps the Mode-A
        door's dual raw + inline-expanded scan (the DEV-1494 contract that
        surfaces a derived ref's expansion joins).

        Note the scan runs on the RESIDUAL, so the shifted CTE's join set
        follows what it actually renders.
        """
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
            # One entry does both jobs: the returned AST is what the shifted
            # CTE renders, and the paths it crossed were registered on the
            # frame while entering (P-A — discovery cannot be forgotten).
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

    def _emit_time_shift_ctes_for_planned(  # NOSONAR(S3776) — single conceptual unit for one time_shift slot: partition/time resolution through the shifted ScopeFrame + shifted-CTE body assembly + collision-safe CTE naming (cte_allocator) + sjoin grain join-back, all sharing tightly-coupled per-slot state (time_alias / input_alias / partition_specs / shifted_cte_name / carry aliases). Splitting forces that cross-cutting state through many-argument helpers without simplifying anything — same shape as the sibling _render_cross_model_cte's suppression.
        self,
        *,
        slot,
        chain: ChainState,
        render: RenderState,
        shifted_where_parts: List[str],
        shifted_where_join_paths: List[Tuple[str, ...]],
        chain_tail: str,
    ) -> str:
        """Emit a ``shifted_<alias>`` + ``sjoin_<alias>`` CTE pair for
        one time_shift transform slot.

        Returns the name of the ``sjoin_`` CTE — the new chain tail the caller
        continues from.

        Legacy reference: ``slayer/sql/generator.py::_generate_shifted_base``
        and the sjoin assembly inside ``_generate_with_computed:1546``.
        The typed implementation differs from legacy in two principled
        ways:

        * **Inner reads raw data**: ``BetweenKey`` filters from
          ``TimeDimension.date_range`` are omitted from the shifted CTE
          (the 7b.3c invariant). Legacy instead substituted the time
          column inside WHERE filters with a shifted expression to read
          adjacent periods; the typed pipeline reads raw and lets the
          outer projection re-apply the BETWEEN.
        * **partition_keys**: DEV-1450 C6 — explicit ``partition_by`` on
          ``change`` / ``time_shift`` threads through as additional
          equality keys in the LEFT JOIN (not just query dimensions).

        DEV-1711 (Stage 7): the shifted CTE is a ``ScopeFrame`` (Laws 1 & 2).
        Every partition key and the shift-axis time expression enters through
        ``scope.resolve`` — anchoring the ref AND registering the join it
        crosses in one call — so the shifted CTE's FROM (built from
        ``scope.join_paths``) pulls exactly the LEFT JOINs the shifted
        projection references. This makes CROSS-MODEL partitions (``stores.
        name``), DERIVED dim partitions (local ``upper(status)`` or joined
        ``stores.tier``), SECONDARY time-dimension partitions, and joined-column
        ROW filters all work, and removes the joinless-CTE guards. The sjoin
        grain join-back (time axis + every partition) is dialect-aware
        null-safe (Codex F2) so NULL dim / NULL time-bucket groups keep their
        shifted value instead of silently dropping.
        """
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
        if not isinstance(inner_key, (AggregateKey, ColumnKey, ColumnSqlKey)):
            raise NotImplementedError(
                f"DEV-1450 stage 7b.11: composite-input transforms "
                f"(layer op='time_shift' input={type(inner_key).__name__}) "
                f"are deferred to a follow-up slice. slot id={slot.id!r}."
            )
        if not isinstance(time_key, TimeTruncKey):
            raise ValueError(
                f"time_shift requires a TimeTruncKey time_key; got "
                f"{type(time_key).__name__} (slot id={slot.id!r}).",
            )

        # Resolve periods kwarg (binder defaulted to None if missing —
        # validation raised already in that case).
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

        # The aliases the shifted CTE needs to project.
        # 1. The time-trunc column (shifted, then DATE_TRUNC'd) AS its
        #    own alias matching the base CTE.
        time_sid = slot_id_by_key.get(time_key)
        if time_sid is None or time_sid not in available_alias_by_slot_id:
            raise RuntimeError(
                f"time_shift time_key not materialised in base CTE: "
                f"slot id={slot.id!r}, time_key={time_key!r}.",
            )
        time_alias = available_alias_by_slot_id[time_sid]

        # 2. The aggregate / column input under its base alias.
        input_sid = slot_id_by_key.get(inner_key)
        if input_sid is None or input_sid not in available_alias_by_slot_id:
            raise RuntimeError(
                f"time_shift input not materialised in base CTE: "
                f"slot id={slot.id!r}, input={inner_key!r}.",
            )
        input_alias = available_alias_by_slot_id[input_sid]

        # DEV-1711 (Law 1): the shifted CTE is a ScopeFrame. Every partition
        # key and the shift-axis time expression enters through ``resolve``,
        # which anchors the ref AND registers the join it crosses. The FROM
        # (built below from ``shifted_scope.join_paths``) then pulls exactly
        # those LEFT JOINs — a cross-model / derived / secondary-time partition
        # can never reference an unjoined table. The scope shares the
        # generation-wide allocator so any ``_val_<n>`` names stay unique
        # across the base and every CTE.
        shifted_allocator = self._gen_allocator or self._new_allocator()
        # DEV-1837 (D3): seed the ROW regroup placeholder registry so a computed
        # dimension in the shifted grain — and a row-lowered predicate over it —
        # resolves to the producer column, exactly as in ``base``.
        shifted_scope = self._scope_frame(
            model=source_model, relation=source_relation,
            bundle=bundle, allocator=shifted_allocator,
            attached_columns=render.regroup_env,
        )

        # 3. partition_keys (DEV-1450 C6) + auto-include query dimensions.
        #
        # Legacy auto-joins on EVERY query dimension regardless of
        # partition_by (``_generate_with_computed:1559``). Without this,
        # ``time_shift(amount:sum, periods=-1)`` with ``status`` in
        # ``dimensions`` would broadcast the prior-period total across
        # every status value. The typed pipeline mirrors this AND extends it
        # (DEV-1711): the sjoin grain is EVERY projected dimension — joined
        # ``ColumnKey``, derived ``ColumnSqlKey``, and SECONDARY ``TimeTruncKey``
        # (a second time dim, distinct from the shift axis) — plus any explicit
        # ``partition_keys`` (C6). The shift axis itself is the time-join
        # column, excluded by slot id.
        partition_specs: list[tuple[str, str, exp.Expression]] = []
        # entries: (slot_id, base_alias, resolved_expr_for_select_and_group_by)
        seen_partition_sids: set = set()

        def _resolve_partition_expr(pk_obj) -> exp.Expression:
            # A SECONDARY time dimension renders as DATE_TRUNC over its resolved
            # (possibly joined / derived) raw column; a plain / derived column
            # renders as its resolved expression. ``resolve`` registers the
            # crossed join in both cases (Law 1).
            if isinstance(pk_obj, TimeTruncKey):
                raw = shifted_scope.resolve(pk_obj.column)
                return self._build_date_trunc(
                    col_expr=raw,
                    granularity=TimeGranularity(pk_obj.granularity),
                )
            if isinstance(pk_obj, (ColumnKey, ColumnSqlKey)):
                return shifted_scope.resolve(pk_obj)
            if isinstance(pk_obj, (ScalarCallKey, ArithmeticKey)):
                # DEV-1740: a computed (expression) dimension — render through
                # the shifted scope so its leaf columns resolve (and any
                # crossed join registers) inside the shifted CTE.
                return render_value_key(
                    key=pk_obj,
                    ctx=RenderContext(scope=shifted_scope, dialect=self._dialect),
                )
            raise NotImplementedError(
                f"time_shift partition on {type(pk_obj).__name__} is not "
                f"supported (only column / derived-column / time-dimension / "
                f"computed-dimension partitions render in the shifted CTE). "
                f"slot id={slot.id!r}.",
            )

        def _add_partition(pk_obj, *, where: str) -> None:
            pk_sid = slot_id_by_key.get(pk_obj)
            if pk_sid is None or pk_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"time_shift {where} not materialised: "
                    f"slot id={slot.id!r}, key={pk_obj!r}.",
                )
            # The shift axis is the time-join column, never a partition pair.
            if pk_sid == time_sid or pk_sid in seen_partition_sids:
                return
            pk_alias = available_alias_by_slot_id[pk_sid]
            partition_specs.append((pk_sid, pk_alias, _resolve_partition_expr(pk_obj)))
            seen_partition_sids.add(pk_sid)

        # Auto-include the shared transform grain (DEV-1837 D1 — plain /
        # derived / computed / row-placeholder dims, combined placeholders
        # excluded so an attached measure value never widens the shifted
        # grain) plus any SECONDARY time dimension; the shift axis is skipped
        # by slot id above.
        grain_sids = set(self._transform_grain_slot_ids(
            planned_query=planned_query, slots_by_id=slots_by_id,
        ))
        for sid in planned_query.projection:
            dim_slot = slots_by_id.get(sid)
            if dim_slot is None or dim_slot.phase != Phase.ROW:
                continue
            if sid in grain_sids or isinstance(dim_slot.key, TimeTruncKey):
                _add_partition(dim_slot.key, where="query dimension")

        # Explicit partition_keys (DEV-1450 C6) may add more (deduped by slot id
        # against the auto-included dims — see the DEV-1711 dedup test).
        for pk in sorted(key.partition_keys, key=lambda k: repr(k)):
            _add_partition(pk, where="partition_key")

        # DEV-1750: the shifted CTE re-aggregates the inner aggregate host-rooted,
        # so every crossing input registers into ``shifted_scope`` exactly as the
        # base SELECT (``_resolve_agg_inputs_via_scope``) and the ``_cm_`` CTE do
        # — otherwise a crossing default param (``w='customers__regions.weight'``)
        # re-aggregates with no join to regions, which no database binds.
        shifted_frag_kwargs: "Dict[str, ResolvedAggKwarg]" = {}
        if isinstance(inner_key, AggregateKey):
            # source: derived (crosses inside Column.sql) or path-bearing.
            if isinstance(inner_key.source, ColumnSqlKey) or getattr(
                inner_key.source, "path", (),
            ):
                shifted_scope.resolve(inner_key.source)
            # positional args (first/last time arg); skip the DEV-1526
            # path-bearing ColumnSqlKey residual (bogus join if anchored here).
            for _arg in inner_key.args:
                if isinstance(_arg, ColumnSqlKey) and _arg.path:
                    continue
                if isinstance(_arg, (ColumnKey, ColumnSqlKey)):
                    shifted_scope.resolve(_arg)
            for _kname, _kval in inner_key.kwargs:
                if isinstance(_kval, (ColumnKey, ColumnSqlKey)):
                    shifted_scope.resolve(_kval)
            # template fragments + non-overridden default AggregationParam.sql —
            # the one door shared with the host + ``_cm_`` paths (DEV-1745 W2).
            # DEV-1743: keep the resolved (alias-rewritten) fragments so the
            # shifted re-aggregation spec embeds them (multi-hop dotted →
            # ``__`` alias), not the raw dotted text.
            for _fname, _fast in self._register_fragment_kwarg_joins(
                key=inner_key, scope=shifted_scope, model=source_model,
            ).items():
                shifted_frag_kwargs.setdefault(
                    _fname, ResolvedAggKwarg(kind="expr", value=_fast),
                )
            if (
                inner_key.column_filter_key is not None
                and inner_key.column_filter_key.canonical_sql
            ):
                self._enter_mode_a_predicate(
                    sql=inner_key.column_filter_key.canonical_sql,
                    scope=shifted_scope,
                    location=f"Column.filter on model {source_model.name!r}",
                )

        # Build the shifted time-column expression. Calendar offset is
        # ``-periods`` units in the SHIFT granularity (periods=-1 -> +1 unit).
        # The shift granularity is the explicit 3rd arg
        # (``time_shift(x, -1, 'year')``) when given, else the query time
        # dimension's granularity — so a year-shift over a month bucket
        # yields "same month, previous year" (YoY). The DATE_TRUNC below
        # always uses the TD granularity (the join/bucket axis).
        shift_gran_raw = next(
            (v for k, v in key.kwargs if k == "granularity"), None,
        )
        shift_granularity = (
            str(shift_gran_raw) if shift_gran_raw is not None
            else time_key.granularity
        )
        # DEV-1450 #4a / DEV-1711: the shift-axis raw time expression resolves
        # through the SAME scope (Law 1) — a derived (ColumnSqlKey) time column
        # yields its EXPANDED expression, and a JOINED time axis (``stores.
        # opened_at``) registers its join so the shifted FROM binds it. The
        # calendar offset and DATE_TRUNC then apply over that expression.
        raw_time_col_expr = shifted_scope.resolve(time_key.column)
        # Truncate BEFORE shifting: offsetting a raw timestamp overflows on
        # non-clamping dialects (SQLite: Jan 31 + 1 month = Mar 2), silently
        # dropping period-tail rows from the shifted bucket (DEV-1811 audit).
        # A period START never overflows. When shifting a bucket start by
        # whole shift-units always lands on a bucket start, the outer
        # re-trunc is a per-row no-op and is skipped; non-aligned offsets
        # (e.g. a day shift over month buckets) keep it.
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

        # Build the shifted CTE — as AST, so the dotted base aliases it
        # projects reach the assembler as single identifiers (D8).
        shifted_select_parts: List[exp.Expression] = []
        shifted_group_by: List[exp.Expression] = []

        # Projected: time-trunc shifted under the base time alias.
        shifted_select_parts.append(
            shifted_trunc_expr.as_(time_alias, quoted=True),
        )
        shifted_group_by.append(shifted_trunc_expr.copy())

        # partition_keys: SELECT + GROUP BY under their base aliases.
        for _, pk_alias, pk_expr in partition_specs:
            shifted_select_parts.append(pk_expr.as_(pk_alias, quoted=True))
            shifted_group_by.append(pk_expr.copy())

        # Aggregate: re-emit the AggregateKey using the same synth /
        # _build_agg dance the base CTE uses.
        if isinstance(inner_key, AggregateKey):
            # DEV-1835 — the time_shift-over-ranked guard dissolved: a local bare
            # first/last is a combined regroup placeholder by the time the shifted
            # CTE renders (the ranking happens inside its producer), so no ranked
            # AggregateKey reaches this flat re-aggregation.
            # Build a synth ``AggRenderSpec`` for _build_agg.
            #
            # The renderer needs a slot-like input with declared_name +
            # type. Pull from the inner aggregate's slot to keep typed
            # CAST behavior aligned with the base.
            inner_slot = slots_by_id.get(input_sid)
            if inner_slot is None:
                raise RuntimeError(
                    f"inner aggregate slot {input_sid!r} not found",
                )
            synth = self._build_agg_render_spec_from_planned(
                slot=inner_slot,
                key=inner_key,
                source_model=source_model,
                source_relation=source_relation,
                full_alias=input_alias,
                bundle=bundle,
                resolved_agg_kwargs=shifted_frag_kwargs or None,
            )
            agg_expr, _ = self._build_agg(synth)
            agg_expr = _wrap_cast_for_type(agg_expr, inner_slot.cast_type)
            shifted_select_parts.append(
                agg_expr.as_(input_alias, quoted=True),
            )
        else:
            # Row-level column input (not aggregated). Resolve through the scope
            # so a joined / derived input registers its join and anchors
            # correctly (Law 1), same as every other ref in this CTE.
            col_expr = shifted_scope.resolve(inner_key)
            shifted_select_parts.append(
                col_expr.as_(input_alias, quoted=True),
            )
            shifted_group_by.append(col_expr.copy())

        # DEV-1837 (D3): resolve the ROW regroup attach conditions through the
        # shifted scope BEFORE the FROM is built (a host partition key may cross
        # a join), mirroring the base build's position 2.6. The producer LEFT
        # JOINs are applied below, keeping the shifted row population at parity
        # with ``base``.
        regroup_attach_conditions = self._resolve_regroup_attach_conditions(
            regroup_join_specs=render.regroup_join_specs, scope=shifted_scope,
        )
        # DEV-1711: register the join paths the shifted WHERE filters cross
        # (computed once by ``_build_shifted_cte_where_parts``) so a joined-column
        # ROW filter (``stores.name = 'North'``) pulls its LEFT JOIN into this
        # CTE. Then build the FROM from the scope's full registered set — a
        # crossed join can never be forgotten because discovery is a side effect
        # of resolving each ref above.
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
        # ``shifted_where_parts`` is the one text input left on this path: the
        # WHERE builder renders Mode-A predicates to SQL. Parsed once here
        # rather than concatenated into a body string, so the surrounding CTE
        # stays AST.
        for _where_part in shifted_where_parts:
            shifted_select = shifted_select.where(
                self._parse_predicate(_where_part),
            )
        for _gb in shifted_group_by:
            shifted_select = shifted_select.group_by(_gb)

        # Pick the slot's user-facing alias(es). DEV-1450 C13: two
        # declared measures sharing a structural key intern to ONE
        # slot with multiple ``public_aliases``; the sjoin CTE projects
        # the shifted measure under EACH alias so the outer SELECT
        # carries both.
        # DEV-1692: a HIDDEN inner time_shift slot's declared_name
        # (``_time_shift_inner``) is NOT unique across sibling shifts with
        # different offsets — two would project + resolve downstream under the
        # same column, silently collapsing ``growth_2m`` onto ``growth_1m``'s
        # shift. Allocate a unique internal alias for the hidden case; USER
        # aliases (public_aliases, already unique) are left untouched.
        if slot.public_aliases:
            slot_aliases: List[str] = list(slot.public_aliases)
        else:
            slot_aliases = [cte_allocator.allocate_cte(slot.declared_name)]
        cte_name_alias = slot_aliases[0]
        # DEV-1692: allocate collision-free CTE names too. DEV-1756: length-fit
        # them so a long transform name can't push ``shifted_``/``sjoin_`` past
        # the dialect's identifier limit and silently truncate.
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

        # The shifted CTE reads the SOURCE table, not the chain; its only CTE
        # dependencies are the ROW regroup producers it LEFT JOINs (D3).
        ctes.append(CteEntry(
            name=shifted_cte_name, query=shifted_select,
            depends_on=[name for name, _ in regroup_attach_conditions],
        ))

        # Build the sjoin CTE: LEFT JOIN prev_cte + shifted on time +
        # partition equalities. Carry every prev_cte alias forward,
        # then add the shifted measure under EACH of the slot's public
        # aliases (DEV-1450 C13).
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
                    alias=input_alias, table=shifted_cte_name,
                ).as_(full_slot_alias, quoted=True),
            )

        # JOIN conditions: time equality + every partition equality. The sjoin is
        # a grain join-back like any other, so it goes through the shared
        # null-safe builder — a NULL dimension value (e.g. a LEFT-joined
        # ``stores.name`` with no matching store) or a NULL time bucket must
        # match its own group instead of dropping to a NULL shifted value.
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

        # Record EACH alias in both the per-slot list (C13 carry-forward
        # in the outer SELECT) and the "pick one" map (transform input /
        # filter / order lookups by downstream layers).
        for full_slot_alias in slot_full_aliases:
            aliases_by_slot_id.setdefault(slot.id, []).append(full_slot_alias)
        # ``available_alias_by_slot_id`` is "pick one" — first alias wins.
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
        """Emit ``cp_reset_<alias>`` + ``cp_value_<alias>`` CTEs for one
        consecutive_periods transform slot.

        Returns the name of the ``cp_value_`` CTE — the new chain tail the
        caller continues from.

        Supersedes the legacy ``_build_consecutive_periods_ctes`` (deleted
        with the enrichment stack in DEV-1485). The typed implementation
        differs from it in two principled ways:

        * The predicate-shape decision (boolean vs numeric) is read
          from the TransformKey input shape (validated by
          ``_validate_window_transform_ops_for_7b10``) rather than the
          legacy ``predicate_is_boolean`` field.
        * The inner aggregate is materialised in the base CTE as a
          hidden slot (via the planner's ``_iter_slot_deps`` walk), so
          the predicate text references that base alias directly — no
          legacy ``_inner_<name>`` step CTE needed.
        """
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

        # Resolve the time-key alias.
        time_sid = slot_id_by_key.get(time_key)
        if time_sid is None or time_sid not in available_alias_by_slot_id:
            raise RuntimeError(
                f"consecutive_periods time_key not materialised: "
                f"slot id={slot.id!r}.",
            )
        time_alias = available_alias_by_slot_id[time_sid]

        # Build the predicate SQL referencing already-materialised base
        # CTE aliases. Two shapes accepted by the validator:
        #   * Slottable leaf: numeric truthiness via IS NOT NULL AND <> 0.
        #   * Comparison ArithmeticKey: rendered + wrapped in COALESCE(<expr>, FALSE).
        leaf_kinds = (ColumnKey, ColumnSqlKey, AggregateKey, TimeTruncKey)
        if isinstance(inner_key, leaf_kinds):
            input_sid = slot_id_by_key.get(inner_key)
            if input_sid is None or input_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"consecutive_periods input not materialised: "
                    f"slot id={slot.id!r}, input={inner_key!r}.",
                )
            input_col = exp.column(
                available_alias_by_slot_id[input_sid], quoted=True,
            )
            predicate = exp.And(
                this=exp.Is(this=input_col.copy(), expression=exp.Null()).not_(),
                expression=exp.NEQ(
                    this=input_col.copy(), expression=exp.Literal.number(0),
                ),
            )
            predicate_is_boolean = False
        elif isinstance(inner_key, ArithmeticKey):
            comparison_ops = {"==", "!=", "<", "<=", ">", ">=", "=", "<>"}
            if inner_key.op not in comparison_ops:
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.11: composite-input transforms "
                    f"(layer op='consecutive_periods' input="
                    f"ArithmeticKey op={inner_key.op!r}) are deferred to "
                    f"a follow-up slice (slot id={slot.id!r}).",
                )
            predicate = render_value_key(
                key=inner_key,
                ctx=self._alias_render_ctx(
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                ),
            )
            predicate_is_boolean = True
        else:
            raise NotImplementedError(
                f"DEV-1450 stage 7b.11: consecutive_periods input "
                f"{type(inner_key).__name__} not supported.",
            )

        # COALESCE / numeric wrap.
        if predicate_is_boolean:
            pred_in_case: exp.Expression = exp.Coalesce(
                this=predicate, expressions=[exp.false()],
            )
        else:
            pred_in_case = predicate

        # Auto-partition by the shared transform grain (DEV-1837 D1).
        partition_aliases: list[str] = []
        for sid in self._transform_grain_slot_ids(
            planned_query=planned_query, slots_by_id=slots_by_id,
        ):
            alias = available_alias_by_slot_id.get(sid)
            if alias is not None:
                partition_aliases.append(alias)

        # DEV-1692: a HIDDEN inner consecutive_periods slot's declared_name
        # (``_consecutive_periods_inner``) is NOT unique across sibling slots —
        # two would collide on ``full_slot_alias`` / ``cp_reset_alias`` and
        # collapse downstream, the same failure mode fixed for time_shift.
        # Allocate a unique internal alias for the hidden case; USER aliases
        # (already unique) are left untouched.
        if slot.public_aliases:
            slot_alias = slot.public_aliases[0]
        else:
            slot_alias = cte_allocator.allocate_cte(slot.declared_name)
        full_slot_alias = f"{source_relation}.{slot_alias}"
        cp_reset_alias = f"_cp_reset_{full_slot_alias}"

        # Build the reset CTE.
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
            """``SUM(CASE WHEN <pred> THEN … ELSE … END) OVER (… ROWS BETWEEN
            UNBOUNDED PRECEDING AND CURRENT ROW)`` — the shape both layers use,
            differing only in the CASE arms and the partition set."""
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

        # Build the value CTE — references the cp_reset CTE's added
        # column in PARTITION BY so each run of true predicate is
        # counted within its own reset group. The outer CASE WHEN
        # guarantees rows where the predicate is false surface as 0.
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
        # One value column per declared name (C13, DEV-1798); the first stays
        # canonical. The internal cp_reset alias stays single (not user-facing).
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

        # Record the slot's aliases for downstream lookups.
        for oa in output_aliases:
            aliases_by_slot_id.setdefault(slot.id, []).append(oa)
            available_alias_by_slot_id.setdefault(slot.id, oa)
        return cp_value_cte_name

    @staticmethod
    def _pick_alias_for_planned_slot(*, slot, alias_index: dict) -> str:
        """Pick the next alias for a slot in projection order.

        Mirrors ``stage_planner._emit_stage_schema``: per-slot index
        picks the next ``public_aliases`` entry; falls back to
        ``declared_name`` when the alias list is exhausted (kept
        symmetric with the planner; unreachable for properly-interned
        slots but defensive).
        """
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
        """Qualify bare-identifier column refs in a Mode-A filter fragment.

        ``Column.filter`` is Mode-A SQL like ``"status = 'paid'"``;
        ``_build_agg`` wraps the aggregate argument as ``SUM(CASE WHEN
        <filter> THEN col END)`` and inserts the filter text verbatim.
        Without qualification, ``status`` resolves against the implicit
        outermost scope at the agg-rendering site, which differs between
        the host base CTE and a re-rooted cross-model CTE. Legacy
        ``resolve_filter_columns`` qualifies bare refs to
        ``<model_name>.<col>``; mirror that on the parsed AST so a
        rerooted CTE renders the same ``customers.status = 'active'``
        the host base would render.

        Only bare ``exp.Column`` nodes (no table qualifier) whose name
        matches a column on ``source_model`` get qualified. Already-
        qualified refs (``other.col``) and function-call AST nodes pass
        through unchanged.
        """
        if not canonical_sql:
            return None
        try:
            ast = self._parse_predicate(canonical_sql)
        except Exception:
            # Unparseable filter SQL — fall back to the raw text. The
            # legacy path bubbled up the same shape (the enrichment
            # parse failure surfaces at query time).
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
        """An ephemeral :class:`ScopeFrame` for a Mode-A entry whose call site
        holds no scope.

        Two kinds of caller. The pure RENDER paths (the aggregate CASE-WHEN
        wrapper, the WHERE/HAVING assembler) run after the corresponding
        registration pass has already put the crossed joins into the real
        scope, so for them the frame exists only to give the text one
        consistent door to come through. The shifted-CTE residual path
        (``_shifted_filter_sql``) instead READS ``frame.join_paths`` back and
        hands it to its caller, which registers those paths on the shifted
        scope — so the frame is the discovery vehicle there, not a byproduct.
        Every site that already owns a real scope passes it instead.
        """
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
        """Enter a Mode-A PREDICATE through the door and hand back its AST.

        The grammar is fixed by the caller's surface (``Column.filter`` and
        ``SlayerModel.filters`` are predicates), never sniffed from the text.
        """
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
        """Enter a Mode-A scalar EXPRESSION (a ``Column.sql`` / aggregation
        template fragment) through the door."""
        return scope.enter_expression(sql, location=location)

    def _register_fragment_kwarg_joins(
        self, *, key, scope: ScopeFrame, model,
    ) -> "Dict[str, exp.Expression]":
        """Resolve an aggregation's template FRAGMENTS through the Mode-A door,
        registering the joins they cross AND returning the alias-rewritten AST
        keyed by param name.

        The sources are the aggregate's string kwargs plus the non-overridden
        ``AggregationParam.sql`` defaults of the aggregation named by
        ``key.agg`` on ``model``. Both substitute into the rendered aggregate
        expression, so whatever they reach has to be in the FROM.

        DEV-1743: the resolved AST is RETURNED (not discarded) so the render
        path embeds it. A multi-hop dotted fragment (``customers.regions.weight``)
        must be rewritten to its internal join alias (``customers__regions.weight``)
        exactly like ``Column.sql`` is — re-parsing the raw dotted text would
        emit an unbound ``regions`` qualifier. (A single-hop fragment already
        happened to work because the alias IS the bare target name.)

        Shared by the host base SELECT and the ``_cm_*`` cross-model CTE. Only
        values the aggregation's formula actually SUBSTITUTES are treated as SQL:
        a string kwarg whose ``{name}`` never appears in the template is a
        marker, not a fragment (``revenue:sum(window='90d')``), and handing it
        to a SQL parser is meaningless.
        """
        agg_def = next(
            (a for a in (model.aggregations or []) if a.name == key.agg), None,
        )
        if agg_def is None:
            # A built-in aggregation has no template, so no kwarg of it is a
            # SQL fragment.
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
        """Pre-expand derived (``ColumnSqlKey``) ROW dimensions and derived TIME
        dimensions for the base SELECT: inline sibling/joined derived refs
        (DEV-1333 / DEV-1410), register any joins their SQL crosses into
        ``scope.join_paths`` (Law 1 — the join-discovery side effect), and return
        the expanded-expr-by-slot-id map the render branch reads from. Extracted
        from ``_build_base_select_for_planned``.

        ``order_slot_ids`` extends the pass to ORDER-BY-only targets, which are
        deliberately NOT in ``base_render_order`` — materialising one there
        would project it and add it to GROUP BY, changing the grain. A hidden
        derived sort key still crosses whatever its ``Column.sql`` crosses, and
        Law 1 does not care that ORDER BY is the only thing referencing it: the
        join has to be in the base FROM or the sort term is unbound. Only ROW
        slots reach here, so a GROUPED query contributes nothing — the planner
        has already rewritten its sort key to an aggregate wrap, which is
        isolated rather than pulled (DEV-1735 / D9).
        """

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
            # DEV-1450 #4a: a derived (ColumnSqlKey) TIME dimension expands the
            # same way; pull any joins its SQL crosses into the FROM so the
            # DATE_TRUNC over the expanded expression resolves.
            if isinstance(key, TimeTruncKey) and isinstance(key.column, ColumnSqlKey):
                raw = self._raw_time_col_expr_for_planned(
                    time_column=key.column, source_model=source_model,
                    source_relation=source_relation, bundle=bundle,
                )
                # DEV-1701: register the join to the derived TD's OWNING model
                # (``key.column.path``) plus every further join its expanded sql
                # crosses — parity with the plain joined-derived-dimension branch
                # below. ``is_root=False`` (in ``_raw_time_col_expr_for_planned``)
                # already anchored the inner refs at the host-path alias, so the
                # scan and the render agree.
                _add(key.column.path)
                for p in self._joined_paths_in_sql(
                    sql_expr=raw, source_relation=source_relation,
                    source_model=source_model, bundle=bundle,
                ):
                    _add(p)
                continue
            if not isinstance(key, ColumnSqlKey):
                continue
            # Local refs (``path == ()``) expand rooted at the source relation; a
            # CROSS-MODEL derived dim (``B.foo``, ``path == ("B",)``) expands
            # rooted at the ``__``-path alias of the owning joined model, with
            # ``is_root=False`` so a further-joined ref carries the full prefix
            # (``B`` reaching ``C`` → ``B__C``).
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
            # DEV-1743: register the joins the derived column crosses
            # STRUCTURALLY (a chain leaf and a literal ``__``-named model must
            # not collapse onto one path when re-scanning the internal alias).
            for p in sorted(crossed, key=lambda t: (len(t), t)):
                _add(p)
        return derived_expr_by_sid

    def _derived_column_expr(
        self, *, key, source_model, source_relation: str, bundle,
        crossed_paths: "Optional[Set[Tuple[str, ...]]]" = None,
    ) -> "Optional[exp.Expression]":
        """The rendered expression for a derived (``ColumnSqlKey``) column.

        The ONE expansion, so a derived column renders identically wherever it
        appears (P-G). A derived column's ``Column.sql`` may reference ANOTHER
        derived column on the same model (``amount_x4 = amount_x2 * 2``), which
        only :meth:`_expand_derived_column_sql` inlines — resolving the raw
        ``Column.sql`` instead emits the sibling's NAME, and no such database
        column exists. The projection path always expanded; the ORDER BY path
        resolved raw, so an unprojected derived sort key emitted SQL that
        failed at the database (CodeRabbit, DEV-1747).

        ``None`` when the owning model is not in the bundle — the caller
        decides whether that is a skip or an error.
        """
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
        """Render a ``Column.filter`` Mode-A predicate for the aggregation-time
        CASE-WHEN wrapper (``SUM(CASE WHEN <filter> THEN col END)``). Inlines
        derived refs (bare or dotted-to-joined-derived) so the crossed joins
        resolve; otherwise qualifies bare refs. DEV-1494; see the Mode-A door
        (``_enter_mode_a_predicate``).
        """
        if not canonical_sql:
            return None
        if bundle is None:
            # No bundle means no join graph to expand or scan against — the
            # AST bare-ref qualification is all that is available.
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
            # DEV-1686 reserved-word alias + DEV-1645 mixed-case physical-name
            # quoting via ``_to_table``.
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
        """Untruncated time expression for a ``TimeTruncKey.column``
        (DEV-1450 #4a), agnostic to base vs derived.

        * ``ColumnKey`` → the (possibly joined) bare column expression.
        * ``ColumnSqlKey`` → the EXPANDED ``Column.sql``, rooted at the host
          relation for a local derived column, or at the ``__``-path alias
          for a joined one. The DATE_TRUNC is applied by the caller.
        """

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
                # DEV-1701: a JOINED derived TIME dimension whose ``Column.sql``
                # crosses a FURTHER join must anchor its inner refs at the
                # host-path alias (``customers_v2__regions``), not the bare
                # direct-join alias (``regions``) — otherwise the host base
                # SELECT references a table its FROM never joins. ``is_root=
                # False`` carries the full ``__`` prefix, exactly as the plain
                # joined-derived-dimension branch in ``_expand_derived_row_dims``
                # does. The ``continue``-less callers (base render, ranked
                # subquery, default-time-col) all render in the host frame.
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
        """Expand a derived ``Column.sql`` (a ``ColumnSqlKey`` target) into a
        fully-qualified SQL string, recursively inlining references to other
        derived columns on the same model or on joined models (DEV-1333 /
        DEV-1410). Bare identifiers qualify to ``source_relation``; joined
        refs qualify to their ``__``-canonical path alias.

        ``owner_path`` is the ROOT-relative join path of ``source_model`` (empty
        when the derived column is local to the query root; the ``__``-path
        tuple when it lives on a JOINED model). A further-joined reference
        inside the column's sql then resolves to the full path. When
        ``crossed_paths`` is supplied, every join the fragment (recursively)
        crosses is added to it STRUCTURALLY (DEV-1743) — the caller no longer
        re-scans the internal-alias output.

        Synchronous: resolves join targets through ``bundle.get_referenced_
        model`` (every model is already loaded — P11). Returns the column's
        own ``name`` when ``sql`` is unset (bare base column).
        """
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

    def _joined_paths_in_sql(
        self, *, sql_expr: exp.Expression, source_relation: str, source_model,
        bundle,
    ) -> List[Tuple[str, ...]]:
        """Collect the join paths referenced by table qualifiers inside an
        (already-expanded) SQL expression.

        Each ROOT-scope ``<alias>.<col>`` whose ``alias`` is not the source
        relation and fully resolves as a join walk on ``source_model``
        contributes its path prefixes (``a__b`` → ``("a",)`` and
        ``("a", "b")``) so ``_build_from_and_joins`` pulls the LEFT JOINs into
        the FROM. Aliases that don't resolve as a join path (CTE / subquery
        aliases) are skipped, as are refs inside a nested scope (subquery /
        set-op branch) — those belong to the inner rowset, not the outer FROM.
        Prefixes are only emitted once the FULL alias path resolves, so a
        partially-matching alias never injects a spurious outer join.

        Thin shim over the shared ``collect_root_scope_joined_paths`` helper
        — see ``column_filter_paths._walk_root_scope_paths`` for the planner
        side using the same primitive.
        """
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
        """Register into ``scope.join_paths`` the joins every WHERE-phase filter
        references (Law 1 — discovery is a side effect of resolving the filter
        through the scope). A 1:1 replacement for the former
        ``_collect_filter_join_paths`` (wrap-and-reuse, D-G): it delegates to the
        same ``_value_key_join_paths`` sub-scanner for typed filters and to the
        Mode-A door (``_enter_mode_a_predicate``) for text filters, in the same
        ``filters_by_phase`` order, so the base FROM stays byte-identical.

        Covers three shapes:
        * typed joined column ref (``customers.regions.name == 'US'``) —
          ``ColumnKey.path``;
        * typed derived column whose ``Column.sql`` crosses a join
          (``is_eu = 1`` where ``is_eu`` references ``customers.region``) —
          ``ColumnSqlKey``, expanded then scanned;
        * Mode-A ``SlayerModel.filters`` text with a ``__`` join path
          (``customers__regions.name = 'EU'``) — parsed and scanned.

        Filters routed to a per-plan ``_cm_*`` CTE (``skip_filter_ids``) are
        applied there, not on the host base, so their joins are not registered
        here.

        ``filters_override`` (DEV-1732) replaces the filter list being scanned —
        the windowed ``_src`` scope passes the SAME rewritten list it renders, so
        discovery and rendering can never disagree about what the CTE contains.
        """

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
                # DEV-1450 #4b / DEV-1494: discover joins from BOTH the
                # un-inlined text (a placeholder dotted ref like
                # ``loss_payment.has_flag`` keeps its alias even when it inlines
                # to a constant) AND the inline-expanded text (a bare/dotted
                # DERIVED ref like ``is_eu`` surfaces the join its expansion
                # crosses). See the Mode-A door's dual-scan (``ScopeFrame._enter``).
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
        """Join paths a typed filter ``ValueKey`` tree references (DEV-1450 /
        DEV-1475): a direct ``ColumnKey.path``; a derived ``ColumnSqlKey``
        (local or joined — expanded then scanned for the joins its ``sql``
        crosses); and recursively through ``ArithmeticKey`` / ``ScalarCallKey`` /
        ``BetweenKey`` / ``InKey`` operands. Sub-scanner shared by
        ``_resolve_where_filter_joins_via_scope``; ``_joined_paths_in_sql``
        already emits path prefixes, and ``ColumnKey.path`` prefixes are
        expanded here.
        """

        out: List[Tuple[str, ...]] = []

        def _add(path: Tuple[str, ...]) -> None:
            for i in range(1, len(path) + 1):
                prefix = tuple(path[:i])
                if prefix and prefix not in out:
                    out.append(prefix)

        def _derived_paths(*, model, relation, column_name, owner_path) -> None:
            # DEV-1743: collect the joins the derived column crosses
            # STRUCTURALLY as it expands, rather than re-scanning the
            # internal-alias output (which cannot tell a chain alias from a
            # literal ``__``-named model once serialized).
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
                # Joined derived ref also pulls the walk to its owning model.
                _add(k.path)
                model = (
                    bundle.get_referenced_model(k.path[-1]) if k.path
                    else source_model
                )
                if model is not None:
                    # A JOINED derived column (``k.path`` non-empty) roots its
                    # inner refs at its own ``__``-path alias so a further-joined
                    # ref resolves to the full path; a local one roots at the
                    # query root.
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
                # DEV-1475: only the LHS column of an IN can carry a join path.
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
        """Look up the model-level ``Aggregation`` definition for ``key.agg``,
        if any. Returns the matched ``Aggregation`` or ``None``.

        The lookup runs for built-ins too (a user model is allowed to
        override default params for a built-in, e.g. supply a default
        ``weight=`` for ``weighted_avg``), and ``_resolve_agg_param``
        relies on that override surfacing in
        ``AggRenderSpec.aggregation_def``. Only when the name is NOT a
        built-in does a lookup miss raise — an unknown non-built-in is a
        hard error.
        """
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
        """Reject CROSS-MODEL aggregates' kwarg column refs whose join path
        disagrees with the aggregate source path.

        For a target-rooted aggregate, a kwarg path that doesn't match the
        source path after reroot prefix-stripping would silently bind the
        kwarg to a different model than the aggregate value column —
        meaningless SQL semantically; any residual mismatch surfaces here.

        DEV-1709: LOCAL aggregates (``source.path == ()``) are exempt — a
        structurally-crossing kwarg (``weighted_avg(weight=customers.w)``)
        is now a supported crossing INPUT: the widened Law-3 trigger
        isolates the aggregate host-rooted, and inside that CTE's
        sub-render the kwarg resolves through the host scope (join
        registration + path-aliased emission). Both bare-column
        (``ColumnKey``) and derived-column (``ColumnSqlKey``) kwarg
        refs go through this gate (CodeRabbit fold-in on PR #144).
        """

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
        """The terminal model of a join ``path`` walked from ``source_model``,
        or ``None`` if any hop is missing. Non-raising: callers use it to
        re-anchor a reference that the planner has already validated."""
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
        """Build an ``AggRenderSpec`` from a planned aggregate slot so
        ``_build_agg`` / ``_resolve_sql`` / ``_wrap_cast_for_type`` emit
        dialect-correct SQL without forking the agg-emission codebase.

        Uses ``sql = column.sql or column.name`` so ``COUNT(*)`` (StarKey
        source) and ``COUNT(col)`` (ColumnKey source with sql=None on a bare
        column) take their distinct branches inside ``_build_agg``.
        """

        # ``slot`` may be ``None`` when this spec is built for a HAVING term
        # whose aggregate isn't a declared projection slot; the result type is
        # then unknown (no outer CAST needed for a comparison operand).
        slot_type = slot.type if slot is not None else None
        source = key.source
        if isinstance(source, StarKey):
            # Reject any non-count aggregation on ``*`` — e.g.
            # ``*:sum`` or ``*:median`` would otherwise plan and render
            # as ``SUM(*)`` / ``MEDIAN(*)``, which is meaningless. The
            # typed pipeline enforces it here so it can't silently emit
            # invalid SQL (Codex MEDIUM fold-in).
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
            # DEV-1747 D2 — a HOST-GRAIN aggregate reads its source THROUGH a
            # join, so the column lives on the terminal model and qualifies to
            # that join's FROM alias. Re-anchor before the lookup below; the
            # join itself is already in this scope's FROM, registered by the
            # aggregate-input scope pass.
            host_grain_root: Optional[str] = None
            if source.path and _is_host_grain(key) and bundle is not None:
                terminal = self._walk_join_path_model(
                    source_model=source_model, path=source.path, bundle=bundle,
                )
                if terminal is not None:
                    source_model = terminal
                    # Qualify through the generation AliasAllocator (NOT a raw
                    # "__".join) so the alias matches the one _build_from_and_
                    # joins() emitted — the allocator uniquifies a chain leaf vs
                    # a literal __-named model (D4). Retain the query root for
                    # derived-ref expansion below.
                    host_grain_root = source_relation
                    source_relation = self._join_alias(
                        root=source_relation, path=source.path,
                    )
            # ColumnKey is a bare / trivial column (``sql`` None or a bare
            # identifier remap); ColumnSqlKey is a derived column (``Column.sql``
            # set to a non-trivial expression — ``amount * 2``). Both resolve
            # the same way: look up the column on the model and aggregate
            # ``col.sql`` (the derived expression) or ``col.name`` (bare).
            src_leaf = (
                source.leaf
                if isinstance(source, ColumnKey)
                else source.column_name
            )
            # ``first`` / ``last`` render through RankedAggregatePlan (see
            # ``_ranked_value_expr``), never through this spec builder, so no
            # explicit ranking time column is resolved here.
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
            # DEV-1452 Stage B — for derived (``ColumnSqlKey``) aggregate
            # sources, the inner bare refs in ``Column.sql`` must qualify
            # to ``source_relation`` (legacy enrichment did this pre-CAST
            # via ``_enrich``'s derived-ref expansion; the typed pipeline
            # never invoked the expander on aggregate sources, so the
            # rendered SQL kept bare ``amount`` where it should be
            # ``orders.amount``).
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
                    # Host-grain re-anchor: expand the inner refs in the
                    # allocator namespace of the EMITTED joins (the query root),
                    # not the re-anchored terminal alias.
                    owner_path=source.path if host_grain_root is not None else (),
                    root_relation=host_grain_root,
                )
            else:
                sql_text = col.sql if col.sql else col.name
            # DEV-1527: a column-ref kwarg (``weight=<col>`` / ``other=<col>``)
            # that the pre-FROM scope pass resolved is embedded as a trusted
            # ``kind="expr"`` expression (its join already base-pulled); every
            # other kwarg (scalar / string / a column-ref on a path this call
            # has no scope for — e.g. the cross-model CTE build) canonical-
            # stringifies as before and coerces to ``kind="str"`` via the
            # ``AggRenderSpec`` before-validator (guarded downstream by
            # ``_validate_agg_param_value`` / ``_SAFE_AGG_PARAM_RE``).
            resolved_kw = resolved_agg_kwargs or {}
            agg_kwargs_str = {
                k: (resolved_kw[k] if k in resolved_kw else agg_kwarg_canonical_str(v))
                for k, v in key.kwargs
            }
            # DEV-1743: a non-overridden model-default ``AggregationParam.sql``
            # fragment is not a ``key.kwargs`` entry, so its resolved
            # (alias-rewritten) form is surfaced here under the param name — the
            # render's ``if name in spec.agg_kwargs`` branch then embeds it
            # instead of re-parsing the raw dotted ``param.sql``.
            key_kwarg_names = {k for k, _ in key.kwargs}
            for _name, _resolved in resolved_kw.items():
                if _name not in key_kwarg_names:
                    agg_kwargs_str.setdefault(_name, _resolved)
            # DEV-1450 stage 7b.12: propagate ``AggregateKey.column_filter_key``
            # into ``AggRenderSpec.filter_sql`` so ``_build_agg`` wraps the
            # aggregate argument as ``SUM(CASE WHEN <filter> THEN col END)``.
            # Legacy ``resolve_filter_columns`` qualifies bare-identifier refs
            # in the filter with the host model name (so ``status = 'paid'``
            # becomes ``orders.status = 'paid'``); mirror that here on the
            # parsed AST so dialect-independent wiring works in the new
            # pipeline.
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
        """``filters_override`` (DEV-1732) replaces ``filters_by_phase`` as the
        list being rendered — see ``_effective_src_filters``. ``regroup_env``
        (DEV-1825) resolves computed-dimension placeholders in a predicate."""

        skip = skip_filter_ids or set()
        # key -> slot map so a HAVING term's local AggregateKey renders as the
        # same aggregate expression the base SELECT emits.
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
                # DEV-1450 stage 7b.12: filters routed into a per-plan
                # cross-model CTE (where_filter_ids / having_filter_ids)
                # are rendered there; the host base must not double-
                # apply them.
                continue
            if fp.phase == Phase.POST:
                # 7b.10: POST-phase filters are handled in the outer
                # wrapper by ``_render_post_phase_filter_conditions``
                # (after the CTE chain, before pagination). Skip them
                # here so the base WHERE doesn't try to render them.
                continue
            if fp.phase not in (Phase.ROW, Phase.AGGREGATE):
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.10+: unsupported filter phase "
                    f"{fp.phase!r}. filter id={fp.id!r}."
                )
            # AGGREGATE-phase filters referencing a LOCAL aggregate render as a
            # HAVING clause; a cross-model aggregate ref raises inside the
            # value-key walker (it routes via the per-plan CTE instead).
            target_parts = (
                having_parts if fp.phase == Phase.AGGREGATE else where_parts
            )
            if fp.phase == Phase.AGGREGATE and fp.expression is not None:
                # A HAVING that references a bare (non-aggregated) row column
                # which is NOT in the query's GROUP BY would emit invalid SQL
                # (``HAVING orders.status = 'x'`` with status ungrouped). Reject
                # early with the legacy phrasing.
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
                # Typed predicate (Mode-B DSL or planner-emitted
                # BetweenKey) — render through the value-key walker.
                # Thread ``aliases_by_slot_id`` so the synth's ``full_alias``
                # matches the materialised spec's alias.
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
                # Match the legacy DSL parser, which wraps top-level
                # boolean expressions in parens — legacy WHERE for a
                # compound filter emits ``WHERE (a AND b)`` rather than
                # ``WHERE a AND b``. Wrapping at the top level only (not
                # recursively) reproduces legacy output without affecting
                # single-comparison or single-BETWEEN filters.
                if isinstance(rendered, (exp.And, exp.Or)):
                    rendered = exp.Paren(this=rendered)
                target_parts.append(rendered.sql(dialect=self.dialect))
            elif fp.text is not None:
                # Mode-A SQL filter (SlayerModel.filters) — qualify bare
                # column refs with the source relation, mirroring
                # legacy `_build_where_and_having` at generator.py:2566.
                # DEV-1450 #4b: a reference to a non-trivial derived column
                # is inline-expanded (and pulls its crossed joins into the
                # FROM via _resolve_where_filter_joins_via_scope).
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
        """True iff ``name`` is a column on ``model`` whose ``Column.sql`` is a
        non-trivial expression (set, and not just a bare-identifier remap)."""
        col = next((c for c in model.columns if c.name == name), None)
        return col is not None and col.sql is not None and not _is_trivial_base(
            column=col,
        )

    def _filter_agg_builder(
        self, *, source_model, source_relation: str, bundle,
    ):
        """The WHERE/HAVING aggregate seam (DEV-1763 P-G): render a local
        aggregate as its EXPRESSION (``SUM(x)``) rather than by output alias so
        HAVING works on backends that reject SELECT aliases there. The renderer
        supplies the slot and the recovered ``having_full_alias``; this resolves
        the aggregate's column-ref kwargs through a host scope (matching the base
        SELECT). Cross-model aggregates route via the per-plan CTE."""

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
        """A ``RenderContext`` for the WHERE/HAVING filter family, over a
        render-scoped host ``ScopeFrame`` from ``_throwaway_frame`` (the crossed
        joins are pulled into the FROM by a separate pass, so this scope's
        ``join_paths`` are inert). Carries the filter-side CAST policy and the
        DEV-1539 comparison grouping. ``regroup_env`` (DEV-1825) seeds the
        placeholder registry so a WHERE over a computed dimension resolves."""
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
        """Precompute the DEV-1503 outer-WHERE slot→qualified-column map as an
        :class:`AliasFacilities` for ``render_value_key`` (DEV-1763 P-G).

        Each slotted leaf resolves to a table-qualified alias: an isolated
        aggregate slot to ``<cte_name>."<agg_col_alias>"``
        (``cross_model_agg_slot_to_cm``), every other slot to
        ``_base."<first_alias>"`` (``aliases_by_slot_id``). A slot with neither
        is left out of the maps, so alias-exclusive resolution fails closed on
        it — the operand-promotion pass makes that unreachable in production."""
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
        """Local ``ColumnKey``s that appear as DIRECT (non-aggregated) operands
        of a predicate tree — used to reject a HAVING that compares an
        ungrouped row column. The walk stops at ``AggregateKey`` /
        ``TransformKey`` (their inner columns are aggregated, not grouped).
        """

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
                # DEV-1475: only the LHS column can be a direct local
                # row-column; literal RHS values aren't grouped against.
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
        """DEV-1501 — wrap a no-transform base SELECT in an outer SELECT
        that projects ONLY the public projection slots (trimming hidden
        materialised aggregates from the result), then moves ORDER BY /
        LIMIT / OFFSET to the outer level so they reference the full
        materialised aliases.

        Same shape as the transform path's outer wrap minus the step CTE
        chain. Preserves C13 duplicate-public-alias semantics by walking
        ``planned_query.projection`` slot-by-slot and cycling aliases per
        slot (mirroring the transform path's ``outer_alias_index``).

        Built via sqlglot AST + ``.sql(dialect=…)`` so identifier quoting
        is dialect-correct (Postgres / SQLite / DuckDB / ClickHouse use
        ``"…"``; MySQL uses backticks). String-built quoted identifiers
        would silently degrade to string literals on MySQL.
        """
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

        # Outer ORDER BY references each order entry's materialised alias
        # — the first alias per slot is canonical (C13-duplicate aliases
        # of a single slot share the same column value). Reuse
        # ``_apply_planned_order_limit`` to apply ORDER BY / LIMIT /
        # OFFSET so the dialect-aware sqlglot emission path is shared.
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
        """ORDER BY / LIMIT / OFFSET for a base SELECT with no CTE chain.

        The per-entry resolution is :func:`resolve_order_term`; this method
        only builds the environment it reads. An entry whose slot the base
        never materialised raises there rather than being skipped — the
        superseded method ``continue``d past it and returned unsorted rows.
        """
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
        """Name every order slot the base SELECT produces, under the scope the
        PLANNER assigned it (P-D).

        Both host-base scopes reference a column of the same SELECT, so the
        reference form is the same; what differs is only whether the alias
        survives an outer projection trim, which is the planner's
        ``HOST_BASE`` / ``HOST_BASE_HIDDEN`` distinction and not something
        re-derived here.
        """
        env = OrderEnv(dialect=self._dialect)
        for order_entry in planned_query.order:
            slot = slots_by_id.get(order_entry.slot_id)
            if slot is None:
                # Deliberately not an early raise: leaving the slot absent is
                # what makes the resolver report it, so every path reports it
                # the same way.
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
        """How one slot's value is NAMED in the base SELECT.

        A public slot is its projected alias. A hidden slot is one of three
        shapes: an aggregate materialised for ordering only (its materialised
        alias), a bare ROW column in an ungrouped query (split
        ``<relation>.<column>`` emission, Law 2), or a local derived column
        (its expansion, provided it crosses no join — a hidden derived column
        never had its join pulled into the base FROM).
        """

        # DEV-1733: the EXACT set of hidden key kinds that resolve to a
        # materialised alias. Deliberately enumerated rather than "any hidden
        # slot that happens to carry an alias" — a hidden ROW slot with an
        # alias must still hit the split-emission / invariant branches below,
        # never be ordered on as a bare column that is not in the GROUP BY.
        _MATERIALISED_ORDER_KINDS = (
            AggregateKey, ArithmeticKey, ScalarCallKey, TransformKey,
        )

        if not slot.hidden:
            # DEV-1713: resolve to the SAME full alias the projection emits —
            # a joined ROW dimension projects under the DOTTED result key
            # (``orders.customers.regions.name``), so the ORDER BY must match
            # it, not the flat ``declared_name`` (``customers__regions__name``),
            # which would name a column the SELECT never projects.
            return exp.Column(
                this=exp.to_identifier(
                    self._full_alias_for_slot(
                        slot=slot, source_relation=source_relation,
                        alias_index={},
                    ),
                    quoted=True,
                ),
            )

        # DEV-1501: hidden AGGREGATE slots are materialised in the base SELECT.
        # Resolve to the materialised full alias — identical shape to the
        # public-alias branch above; the inner subquery exposes it as a column
        # the outer wrap can reference by quoted identifier.
        aliases = (
            aliases_by_slot_id.get(slot.id, [])
            if aliases_by_slot_id is not None
            else []
        )
        if aliases and isinstance(slot.key, _MATERIALISED_ORDER_KINDS):
            return exp.Column(this=exp.to_identifier(aliases[0], quoted=True))

        # DEV-1712 (Law 2, split emission): a hidden ROW column ordered in an
        # UNGROUPED query. Plan-time order validation guarantees the only
        # hidden ROW slot that reaches here is a bare column in a query with no
        # GROUP BY — grouped row columns are rejected or wrapped up front, and
        # aggregates took the branch above. Emit a SPLIT
        # ``<relation>.<column>`` reference (mixed-case-aware) against the base
        # FROM scope, identical to how the column would render if it were a
        # projected dimension.
        #
        # DEV-1703 Phase 1: a JOINED column is emitted the same way, under its
        # ``__`` path alias (``customers__regions.name``). The row IS the grain
        # in an ungrouped query, so the bare reference is legal; Law 1 pulls
        # the crossed join into the base FROM.
        key = slot.key
        row_key = key.column if isinstance(key, TimeTruncKey) else key
        if source_model is not None and isinstance(row_key, ColumnKey):
            return self._joined_or_local_dim_expr(
                path=row_key.path, leaf=row_key.leaf,
                source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            )

        # A LOCAL DERIVED column (``ColumnSqlKey``, path empty). Rendered
        # through the SAME expansion a projected derived dimension gets, which
        # is what makes the two spellings of one column sort identically (D9)
        # — and what stops a derived column defined over ANOTHER derived column
        # from emitting the sibling's name, which is not a database column at
        # all (CodeRabbit).
        #
        # This used to build a throwaway ``ScopeFrame`` here purely to DETECT
        # the crossing at render time, and raised when it found one, because
        # the join had not been pulled into the base FROM. It is pulled now —
        # ``_expand_derived_row_dims`` walks ORDER BY targets, so Law 1 applies
        # to a sort key exactly as it does to a projected dimension.
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

        # Defensive: any other hidden shape should have been rejected at plan
        # time (transform / composite / joined / grouped-row).
        raise NotImplementedError(
            f"ORDER BY references a hidden slot (id={slot.id!r}, key="
            f"{type(slot.key).__name__}) that was not resolved at plan "
            f"time — this is an internal invariant violation."
        )


# ===========================================================================
# DEV-1450 stage 7b.8 — module-level shim entry point.
# ===========================================================================


def generate_from_planned(
    planned_query,
    *,
    bundle,
    dialect: str = "postgres",
) -> str:
    """Render a ``PlannedQuery`` to SQL.

    Module-level entry point: constructs an ``SQLGenerator`` for the
    requested dialect and delegates to the instance method, which uses
    the dialect helpers (``_resolve_sql`` / ``_build_agg`` /
    ``_wrap_cast_for_type`` / ``_parse_predicate``) so dialect-specific
    behavior is emitted consistently.
    """
    return SQLGenerator(dialect=dialect).generate_from_planned(
        planned_query, bundle=bundle,
    )


def _bundle_for_stage(planned_query, bundle, schema_by_name):
    """Pick the per-stage bundle a single DAG stage renders against.

    The stage's host model comes from the planner (``render_source_model`` —
    the stage's OWN source / overlay / synthetic-over-sibling) so the
    generator's FROM / joins bind against exactly what the binder used. A
    StageSchema chain stage carries no ``render_source_model``; the generator
    builds a synthetic model over the upstream CTE. Either way, synthetic
    models for the OTHER sibling stages are threaded into ``referenced_models``
    so a join / cross-model ref that targets a sibling resolves to its CTE.

    A plain single-model query (no upstream schema, no render model) renders
    against the original bundle unchanged.
    """
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
    """Render a multi-stage DAG (``plan_stages`` output) to one SQL string.

    Each non-root stage becomes a CTE ``<name>(<flat cols>) AS (<stage sql>)``;
    the column-alias list flattens the stage's result-key projection
    (``orders.amount_sum``) to the flat names downstream stages bound against
    (``amount_sum``), so no per-stage rename wrapper is needed. The root
    stage is the outer SELECT and carries the public result keys. Stage CTEs
    are prepended to any CTEs the root already emits (cross-model / transform
    stages), since the root reads ``FROM <stage>``.

    ``planned_queries`` is the topo-ordered list from ``plan_stages`` (root
    last). A single-stage list delegates straight to ``generate_from_planned``.
    """
    if not planned_queries:
        raise ValueError("generate_planned_stages requires at least one stage")
    if len(planned_queries) == 1:
        # DEV-1716: single-stage DB-bound terminal — apply the dialect alias
        # mangling post-pass (BigQuery / T-SQL; identity otherwise).
        sql = generate_from_planned(
            planned_queries[0], bundle=bundle, dialect=dialect,
        )
        # DEV-1756: length-fit over-limit projection aliases. ``projection_
        # aliases`` are the plan-derived canonical result keys (same source the
        # read side decodes against), passed in rather than parsed off the
        # pre-mangle SQL — BigQuery can't parse a backticked dotted alias.
        sql = get_dialect(dialect).rewrite_emitted_sql(sql, aliases=projection_aliases)
        # DEV-1705: validate the final POST-mangle, pre-RLS statement (env-gated).
        maybe_validate_scopes(sql, dialect=dialect)
        return sql

    schema_by_name = {
        p.stage_schema.relation_name: p.stage_schema
        for p in planned_queries
        if p.stage_schema is not None
    }

    # (cte_name, rename-wrapped stage AST) in dependency order.
    stage_ctes: List[Tuple[str, exp.Expression]] = []
    root_sql: Optional[str] = None
    for planned in planned_queries:
        stage_bundle = _bundle_for_stage(planned, bundle, schema_by_name)
        stage_sql = generate_from_planned(
            planned, bundle=stage_bundle, dialect=dialect,
        )
        if planned is planned_queries[-1]:
            root_sql = stage_sql
            continue
        if planned.stage_schema is None:
            raise ValueError(
                "non-root stage must carry a stage_schema for CTE chaining; "
                f"source_relation={planned.source_relation!r}",
            )
        stage_ctes.append((
            planned.stage_schema.relation_name,
            _stage_rename_wrapper(
                planned=planned, stage_sql=stage_sql, dialect=dialect,
            ),
        ))

    assert root_sql is not None
    root_ast = sqlglot.parse_one(root_sql, dialect=dialect)

    # The root may already carry CTEs (cross-model / transform stages emit
    # ``WITH base AS ...``). Those read FROM the stage relations, so the
    # stage CTEs must come FIRST. ``Select.with_`` appends; build the order
    # explicitly: clear the root's own CTEs, add the stage CTEs (dependency
    # order), then re-append the root's original CTEs.
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

    # DEV-1716: terminal emit of the multi-stage root — apply the dialect
    # rewrite_emitted_sql post-pass (BigQuery / T-SQL alias mangling; identity
    # otherwise). The re-parse/with_ grafting above can surface dotted aliases
    # the per-stage emits already mangled, so mangle once more here
    # (idempotent) to catch the root's own projection.
    # DEV-1756: length-fit over-limit projection aliases (plan-derived, passed
    # in — see the single-stage branch above).
    sql = root_ast.sql(dialect=dialect, pretty=True)
    sql = get_dialect(dialect).rewrite_emitted_sql(sql, aliases=projection_aliases)
    # DEV-1705: validate the final POST-mangle, pre-RLS multi-stage root
    # (env-gated). One validation per final terminal (single- vs multi-stage).
    maybe_validate_scopes(sql, dialect=dialect)
    return sql


def _stage_rename_wrapper(*, planned, stage_sql, dialect):
    """Wrap a rendered intermediate-stage SQL so its output columns are the
    flat names downstream stages bound against.

    Thin adapter around :func:`slayer.sql.stage_wrapper.build_flat_rename_wrapper`
    (DEV-1452 Stage B decision B) — pulls ``source_relation`` and the
    expected StageSchema column names off the ``PlannedQuery`` and forwards
    to the shared helper. The migrated ``_expand_query_backed_model`` path
    calls the helper directly with names derived from the typed plan.
    """
    return build_flat_rename_wrapper(
        source_relation=planned.source_relation,
        stage_sql=stage_sql,
        expected_columns=[c.name for c in planned.stage_schema.columns],
        dialect=dialect,
    )
