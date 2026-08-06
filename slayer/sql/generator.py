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
from typing import AbstractSet, Any, Dict, List, Literal, Optional, Set, Tuple, Union

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

from slayer.core.errors import AggregationNotAllowedError, UnresolvableOrderColumnError
from slayer.core.keys import _FrozenKey, _reroot_path_ref, reroot_aggregate_key
from slayer.core.models import Aggregation
from slayer.core.refs import agg_kwarg_canonical_str
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration as _parse_window_duration
from slayer.engine.column_expansion import (
    _is_trivial_base,
    _walk_path_to_target_sync,
    collect_root_scope_joined_paths,
    expand_derived_refs_sync,
)
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
    flat_name,
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
from slayer.sql.render.value_expr import (
    render_arithmetic,
    render_scalar_call,
    rewrite_log_alias,
)
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.scope import ScopeFrame
from slayer.sql.scope_check import maybe_validate_scopes
from slayer.sql.stage_wrapper import build_flat_rename_wrapper




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
    ``_build_formula_agg``, ``_resolve_value_sql``, ``_resolve_agg_param``,
    ``_build_ranked_subquery_from_planned``).

    Decouples the helpers from ``EnrichedMeasure`` so the legacy enrichment
    pipeline can be deleted without forking dialect SQL emission. Carries
    exactly the 11 fields the helpers empirically read; ``EnrichedMeasure``
    fields outside this set (``agg_args``, ``source_measure_name``,
    ``distinct``, ``window``, ``user_declared``, ``label``,
    ``filter_columns``) are deliberately NOT carried — ``count_distinct``
    dispatches on the agg name, and the positional time arg for
    ``first`` / ``last`` is pre-resolved into ``time_column`` at spec-build
    time.
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


class FirstLastRenderState(BaseModel):
    """DEV-1501 — bundle of maps produced by
    ``_build_first_last_base_select`` (host base) or
    ``_render_cross_model_cte`` (cross-model CTE) that the HAVING render
    path needs to thread into ``_build_agg`` so a HAVING aggregate
    references the same ``_first_rn`` / ``_last_rn{suffix}`` column the
    SELECT projects (instead of bare ``_last_rn``, which collapses
    distinct time-column specs).
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    rn_suffix_map: Dict[str, str] = {}
    """Effective-time-column → rn suffix (``""`` / ``"_2"`` / …). Empty
    when no first/last aggregates are in scope."""

    default_time_col_sql: Optional[str] = None
    """Fallback time column when a spec has no explicit ``time_column``.
    ``None`` when every first/last spec carries an explicit arg."""

    filtered_rn_map: Dict[str, str] = {}
    """Per-spec-alias → dedicated rn column for filtered first/last
    aggregates (Column.filter wired in at aggregation time)."""

    filtered_match_map: Dict[str, str] = {}
    """Per-spec-alias → match-flag column for filtered first/last."""

    agg_synth_alias: Optional[str] = None
    """DEV-1501 Group A.3 — only set for the cross-model CTE single-agg
    case. The cross-model CTE projects exactly one aggregate; if HAVING
    references the same key, ``_render_filter_value_key_in_target_scope``
    must rebuild the synth with THIS alias so the ``filtered_rn_map`` /
    ``filtered_match_map`` lookups (keyed by the synth alias) hit. Host-
    base callers leave this ``None`` and rely on ``aliases_by_slot_id``
    threaded through ``_build_where_having_from_planned`` instead."""

    value_alias_by_sql: Dict[str, str] = {}
    """DEV-1708 Law 2: RESOLVED value text → ``_val_<n>`` materialisation alias
    for an aggregate whose SOURCE crosses a join. Keyed by the resolved
    (qualified + ``Column.type`` inner-CAST) value emission — DEV-1709 — so
    same-sql-different-type aggregates map to distinct materialisations. The
    projected aggregate materialises the crossing value inside the ranked
    subquery; a HAVING referencing the same aggregate must bind to the SAME
    alias (not re-emit the raw crossing ref, which is out of scope in the
    outer SELECT). Empty when every source is local."""


def _iter_first_last_leaves(key) -> "list":  # NOSONAR(S3776) — sequential isinstance dispatch over the closed ValueKey union; each branch is the per-type recursion contract for surfacing first/last AggregateKey leaves. Extracting per-type helpers would scatter the contract.
    """DEV-1501 (Codex round 3): walk a composite ValueKey for first /
    last ``AggregateKey`` leaves.

    Composite aggregate slots (``ArithmeticKey`` / ``ScalarCallKey``)
    aren't separately materialised — their operand AggregateKeys are
    inlined at the composite render path. Without surfacing the leaves
    here, the ranked-subquery builder wouldn't see their distinct time
    columns and the composite render would resolve every operand to
    bare ``_last_rn``.

    Returns local first/last leaves only (cross-model operands raise in
    the composite render path; row / literal / scalar-call /
    transform / between / in branches recurse into operands without
    surfacing themselves).
    """
    from slayer.core.keys import (
        AggregateKey,
        ArithmeticKey,
        BetweenKey,
        InKey,
        ScalarCallKey,
    )

    out: list = []

    def _walk(k) -> None:
        if isinstance(k, AggregateKey):
            if k.agg in ("first", "last") and not getattr(
                k.source, "path", (),
            ):
                out.append(k)
            return
        if isinstance(k, ArithmeticKey):
            for o in k.operands:
                _walk(o)
            return
        if isinstance(k, ScalarCallKey):
            for a in k.args:
                _walk(a)
            return
        if isinstance(k, BetweenKey):
            _walk(k.column)
            _walk(k.low)
            _walk(k.high)
            return
        if isinstance(k, InKey):
            _walk(k.column)
        # LiteralKey / ColumnKey / TimeTruncKey / TransformKey / etc.:
        # not a first/last operand carrier; stop recursing.

    _walk(key)
    return out




def _render_scalar_literal(v: Any) -> exp.Expression:
    """Render a Python scalar (None / bool / int / float / Decimal / str)
    as a bare sqlglot literal node. Used by the POST-phase filter renderer
    for ``LiteralKey.value`` AND any non-key arg inside ``ScalarCallKey``.
    """
    from decimal import Decimal
    if v is None:
        return exp.Null()
    if isinstance(v, bool):
        return exp.true() if v else exp.false()
    if isinstance(v, (int, float, Decimal)):
        return exp.Literal.number(str(v))
    return exp.Literal.string(str(v))


def _wrap_cast_for_type(expr: exp.Expression, dt: Optional[DataType]) -> exp.Expression:
    """DEV-1361: wrap ``expr`` in ``CAST(expr AS <dialect-rendered dt>)`` so the
    declared SLayer ``DataType`` is enforced in emitted SQL.

    Skipped when ``dt`` is ``None`` (no declared type) or ``DataType.TEXT``
    (cosmetic — SQL TEXT/VARCHAR roundtripping is already a no-op for our
    purposes and ``CAST(... AS TEXT)`` does not unwrap SQLite's
    JSON-quoted-string return values anyway). Also skipped when ``dt`` is
    opaque (``DataType.UNKNOWN``) — there is no such SQL type, so
    ``CAST(x AS UNKNOWN)`` is invalid in every dialect. Skipped when ``expr`` is a
    plain ``exp.Column`` (possibly qualified ``model.col``) — those are
    bare column references whose runtime type already matches the declared
    type by definition; wrapping them in CAST is dead noise and on SQLite
    can be lossy (e.g. ``CAST(text_timestamp AS TIMESTAMP)`` truncating
    to a year). Idempotent: if ``expr`` is already a CAST to the same
    target, return it unchanged.
    """
    if dt is None or dt == DataType.TEXT or dt.is_opaque:
        return expr
    if isinstance(expr, exp.Column):
        return expr
    target = exp.DataType.Type(dt.value)
    if isinstance(expr, exp.Cast):
        existing = expr.args.get("to")
        if isinstance(existing, exp.DataType) and existing.this == target:
            return expr
    return exp.Cast(this=expr, to=exp.DataType(this=target))


def _filter_cast_type(dt: Optional[DataType]) -> Optional[DataType]:
    """The CAST target to use when rendering a derived column inside a
    WHERE / HAVING predicate (DEV-1450 #4a).

    Temporal types (``DATE`` / ``TIMESTAMP``) are suppressed: in a filter
    the derived expression is COMPARED, not type-enforced, and
    ``CAST(text AS TIMESTAMP)`` on SQLite gives the expression NUMERIC
    affinity — truncating a string timestamp to its leading year and
    breaking ``BETWEEN`` / comparison. A base temporal column in the same
    position is never cast (it renders as a bare ``exp.Column``), so this
    keeps the derived form on par. Non-temporal types pass through so a
    derived numeric / boolean column still gets its enforcing CAST.
    """
    if dt in (DataType.DATE, DataType.TIMESTAMP):
        return None
    return dt

logger = logging.getLogger(__name__)

# Maps aggregation name (string) → SQL function name.
_AGG_FUNCTION_MAP: dict[str, str] = {
    "count": "COUNT",
    "count_distinct": "COUNT_DISTINCT",
    "sum": "SUM",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "median": "MEDIAN",
    # "first", "last" use special ROW_NUMBER + conditional aggregate
    # "weighted_avg" and custom aggregations use formula substitution
    # "percentile", "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    # "corr" are dialect-dependent and routed through dedicated builders
    # (_build_percentile / _build_stat_agg) — they are intentionally
    # absent from this map.
}

# DEV-1317: statistical aggregations routed through _build_stat_agg.
# stddev_samp/_pop and var_samp/_pop are 1-arg; corr / covar_samp /
# covar_pop are 2-arg via the `other=` kwarg. SQLite gets these through
# registered Python UDFs; Postgres/DuckDB/MySQL/ClickHouse use the
# native function emitted via sqlglot transpilation. MySQL has no
# native CORR / COVAR_SAMP / COVAR_POP — _build_stat_agg raises
# NotImplementedError there, mirroring _build_median.
_STAT_AGG_NAMES: frozenset[str] = frozenset({
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
})

# Subset of _STAT_AGG_NAMES that take two columns (LHS + `other=` kwarg).
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

# Transforms that use self-join CTEs instead of window functions.
# This gives correct results at result-set edges (no NULLs when the DB has the data)
# and handles gaps in time series correctly.
_SELF_JOIN_TRANSFORMS = {"time_shift"}

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


def _first_bare_column_name(key) -> Optional[str]:
    """Return the leaf name of the first bare column reference inside a
    ROW-phase composite key (DEV-1576 / DEV-1717 error messages).

    Walks ``ArithmeticKey`` operands / ``ScalarCallKey`` args / a
    ``TransformKey`` input for a ``ColumnKey`` / ``ColumnSqlKey`` leaf so the
    "Bare measure name '<col>'" error names the offending column. Returns
    ``None`` when no column ref is found (caller falls back to the alias).
    """
    from slayer.core.keys import (
        ArithmeticKey,
        ColumnKey,
        ColumnSqlKey,
        ScalarCallKey,
        TransformKey,
    )

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






def _cte_name_from_alias(prefix: str, alias: str) -> str:
    """Build a unique CTE name from a measure alias.

    Dots are replaced with ``__`` (double underscore) to avoid collision
    with aliases that already contain underscores. E.g.:
    - ``orders.revenue_sum``  -> ``_fm_orders__revenue_sum``
    - ``orders_v2.revenue_sum`` -> ``_fm_orders_v2__revenue_sum``

    DEV-1713: the ``.`` -> ``__`` flatten delegates to
    :func:`slayer.sql.naming.flat_name` (single owner); this adds only the
    non-identifier-character sanitisation on top.
    """
    sanitized = flat_name(alias)
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized)
    return prefix + sanitized


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


def _strip_trailing_pagination(sql: str) -> str:
    """DEV-1444: remove trailing ORDER BY / LIMIT / OFFSET clauses that
    SLayer's generator appends as raw string segments after the inner
    SELECT body. Used by ``_apply_outer_projection_trim`` so the outer
    wrapper owns pagination without it appearing twice.

    Works on the trailing tail only — preserves any ORDER BY / LIMIT /
    OFFSET that appears inside nested CTEs or sub-queries (they have a
    closing ``)`` after them).
    """
    s = sql.rstrip()
    # OFFSET / LIMIT use narrow digit-bounded regexes. LIMIT-OFFSET is
    # checked before bare OFFSET / LIMIT so the combined form is peeled
    # in a single pass.
    for pattern in (
        _TRAILING_LIMIT_OFFSET_RE,
        _TRAILING_OFFSET_RE,
        _TRAILING_LIMIT_RE,
    ):
        m = pattern.search(s)
        if not m or m.start() == 0:
            continue
        tail = s[m.start():]
        if tail.count("(") != tail.count(")"):
            continue
        s = s[:m.start()].rstrip()
    # ORDER BY: use rfind on the upper-cased copy (case-insensitive
    # match) instead of a regex with an unbounded character class. Same
    # paren-balance check confirms the clause is at the outermost
    # nesting level.
    upper = s.upper()
    pos = upper.rfind("ORDER BY")
    if pos > 0:
        # Word-boundary on the left (preceding whitespace or newline)
        # and after (the BY must be followed by whitespace or end).
        left_ok = upper[pos - 1] in " \t\n\r"
        right_idx = pos + len("ORDER BY")
        right_ok = right_idx >= len(upper) or upper[right_idx] in " \t\n\r"
        if left_ok and right_ok:
            tail = s[pos:]
            if tail.count("(") == tail.count(")"):
                s = s[:pos].rstrip()
    return s


class SQLGenerator:
    """Generates SQL from an EnrichedQuery."""

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
        return AliasAllocator(folds_case=dialect_folds_case(self.dialect))

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

    def _finalize_scalar_call(self, expr: exp.Expression) -> exp.Expression:
        """Apply the target-dialect AST rewrite to a scalar-call expression
        (DEV-1576 / DEV-1717).

        Scalar calls (``round``/``abs``/``coalesce``/…) in formulas are
        assembled directly as ``exp.func(...)`` AST, never string-parsed, so
        the ``rewrite_target_ast`` applied inside ``_parse`` never sees them.
        Routing them through the same dialect hook here keeps the 2-arg
        Postgres ``ROUND`` numeric-cast (and any future target rewrite)
        consistent between parsed and AST-built expressions. Identity for
        dialects whose ``rewrite_target_ast`` is a no-op.
        """
        return self._dialect.rewrite_target_ast(expr)

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




    def _build_outer_wrap(
        self,
        *,
        inner_sql: str,
        public: List[str],
        order,
        limit,
        offset_arg,
    ) -> str:
        """Thin delegate to ``self._dialect.emit_outer_wrap`` (DEV-1716).

        Strips trailing ORDER BY / LIMIT / OFFSET from ``inner_sql``
        (text-level) before handing off to the dialect hook, then passes
        the detached AST nodes for re-emission on the outer statement. The
        hook owns the wrap shape (base derived-table form; ``TsqlDialect``
        hoists inner CTEs) AND the dialect-correct identifier quoting of the
        public-alias list (backticks / brackets / ANSI double quotes).
        """
        if order is None and limit is None and offset_arg is None:
            stripped = inner_sql
        else:
            stripped = _strip_trailing_pagination(inner_sql)
        return self._dialect.emit_outer_wrap(
            inner_sql=stripped,
            public=public,
            order=order,
            limit=limit,
            offset_arg=offset_arg,
            parse=self._parse,
        )

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
        return sqlglot.parse_one(sql, dialect=self.dialect)

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
        what "plan order" means here. Duplicates are dropped (two slots can
        share an alias) while preserving first appearance.
        """
        out: List[str] = []
        seen: Set[str] = set()
        for aliases in aliases_by_slot_id.values():
            for alias in aliases:
                if alias not in seen:
                    seen.add(alias)
                    out.append(alias)
        return out

    def _null_safe_join_pair_sql(self, *, left_sql: str, right_sql: str) -> str:
        """Render one dialect-aware null-safe equality (DEV-1708 / Codex F2) for
        a grain join-back ``ON`` clause. ``left_sql`` / ``right_sql`` are the
        already-quoted qualified column strings (``_base."x"`` / ``_cm."x"``);
        they are parsed back to AST so the dialect strategy's
        ``build_null_safe_eq`` can wrap them (native ``IS NOT DISTINCT FROM`` /
        ``<=>`` / ``IS``, or the expanded ``= … OR (… IS NULL AND … IS NULL)``)."""
        left = self._parse(left_sql)
        right = self._parse(right_sql)
        return self._dialect.build_null_safe_eq(left, right).sql(dialect=self.dialect)

    def _ordered(self, order_col: exp.Expression, *, ascending: bool) -> exp.Ordered:
        """Build an ``exp.Ordered`` node, suppressing sqlglot's NULLS-emulation
        ``CASE WHEN`` on T-SQL (DEV-1571 Bug 2 / DEV-1716).

        On T-SQL, sqlglot emits ``CASE WHEN <alias> IS NULL THEN 1 ELSE 0 END,
        <alias>`` to emulate NULLS ordering whenever ``nulls_first`` is unset;
        the bracketed alias INSIDE the CASE WHEN mis-resolves against the FROM
        scope (``Invalid column name``). Pinning ``nulls_first`` to T-SQL's
        native default for the direction (FIRST on ASC, LAST on DESC)
        suppresses the wrapper. No-op on every other dialect.
        """
        kwargs: dict = {"this": order_col, "desc": not ascending}
        if self.dialect == "tsql":
            kwargs["nulls_first"] = ascending
        return exp.Ordered(**kwargs)





    def _build_time_offset_expr(self, col_expr: exp.Expression, offset: int,
                                granularity: str) -> exp.Expression:
        """Apply a time offset to a column expression (dialect-aware).

        Used to shift raw timestamps before DATE_TRUNC in shifted CTEs so that
        aggregated time buckets align with the base query's buckets.
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

    def _build_transform_sql(self, t) -> str:  # NOSONAR S3776 — flat dispatch over transform names; per-transform SQL forms read better as one if/elif tree than as named helpers
        """Build a window function SQL expression for a transform.

        DEV-1716: identifier refs are dialect-quoted (``_quote_ident``) so
        MySQL/T-SQL/BigQuery get correct quotes; the subsequent
        ``self._parse(window_sql)`` reads them back as identifiers (backticks
        on MySQL, brackets on T-SQL) rather than string literals.
        """
        measure = self._quote_ident(t.measure_alias)
        time_col = self._quote_ident(t.time_alias) if t.time_alias else None
        partition_cols = getattr(t, "partition_aliases", []) or []
        partition_clause = (
            _SQL_PARTITION_BY + ", ".join(self._quote_ident(a) for a in partition_cols)
            if partition_cols
            else ""
        )
        order_clause = f"ORDER BY {time_col}" if time_col else ""
        over_parts = " ".join(p for p in (partition_clause, order_clause) if p)

        # Rank-family OVER clauses always order by the inner measure DESC; their
        # partition is empty unless the user passed partition_by= on the call.
        rank_order = f"ORDER BY {measure} DESC"
        rank_over = " ".join(p for p in (partition_clause, rank_order) if p)

        if t.transform == "cumsum":
            return f"SUM({measure}) OVER ({over_parts})"
        elif t.transform == "consecutive_periods":
            raise ValueError("consecutive_periods should be materialized with staged CTEs")
        elif t.transform in _SELF_JOIN_TRANSFORMS:
            raise ValueError(f"{t.transform} should not reach _build_transform_sql; it uses self-join CTE")
        elif t.transform == "lag":
            return f"LAG({measure}, {abs(t.offset)}) OVER ({over_parts})"
        elif t.transform == "lead":
            return f"LEAD({measure}, {abs(t.offset)}) OVER ({over_parts})"
        elif t.transform == "rank":
            return f"RANK() OVER ({rank_over})"
        elif t.transform == "percent_rank":
            return f"PERCENT_RANK() OVER ({rank_over})"
        elif t.transform == "dense_rank":
            return f"DENSE_RANK() OVER ({rank_over})"
        elif t.transform == "ntile":
            n = getattr(t, "n", None)
            if not isinstance(n, int) or n <= 0:
                raise ValueError(f"ntile requires a positive integer n, got {n!r}")
            return f"NTILE({n}) OVER ({rank_over})"
        elif t.transform == "first":
            return (
                f"FIRST_VALUE({measure}) OVER ({over_parts} "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            )
        elif t.transform == "last":
            return (
                f"FIRST_VALUE({measure}) OVER ({partition_clause} ORDER BY {time_col} DESC "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            )
        else:
            raise ValueError(f"Unsupported transform: {t.transform}")





    # ------------------------------------------------------------------
    # FROM / JOIN building
    # ------------------------------------------------------------------



    # ------------------------------------------------------------------
    # Column / measure resolution (from enriched SQL expressions)
    # ------------------------------------------------------------------

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

        DEV-1361: when the caller has a typed object in scope (an
        ``EnrichedDimension``, a ``Column``), it passes ``type=`` so the
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
            # bare ``str`` (the legacy ``EnrichedMeasure`` adapter and model-level
            # defaults reach here unwrapped). ``kind="expr"`` is a trusted,
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
        rn_suffix_map: Optional[dict[str, str]] = None,
        default_time_col: Optional[str] = None,
        filtered_rn_map: Optional[dict[str, str]] = None,
        filtered_match_map: Optional[dict[str, str]] = None,
    ) -> tuple[exp.Expression, bool]:
        """Build an aggregation expression from an ``AggRenderSpec``."""
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

        # --- first/last: MAX(CASE WHEN _rn = 1 THEN col END) ---
        if agg_name in ("first", "last"):
            col_expr = self._resolve_sql(
                sql=spec.sql,
                name=spec.name,
                model_name=spec.model_name,
                type=spec.column_type,
            )
            col = col_expr.sql(dialect=self.dialect)
            suffix = ""
            if rn_suffix_map is not None:
                # DEV-1501: when no default ranking time column is in scope,
                # every first/last spec is guaranteed to carry an explicit
                # ``time_column`` (validated in
                # ``_build_first_last_base_select``); so the suffix lookup
                # must not gate on ``default_time_col`` being truthy, else
                # distinct-time-column specs all collapse to ``_last_rn``.
                effective_tc = spec.time_column or default_time_col
                if effective_tc is not None:
                    suffix = rn_suffix_map.get(effective_tc, "")
            rn_col = f"_first_rn{suffix}" if agg_name == "first" else f"_last_rn{suffix}"
            # For filtered first/last, use the dedicated ROW_NUMBER column
            # that pushes non-matching rows to the bottom of the ranking.
            # Look up by alias (unique per spec) so two filtered specs
            # sharing source/agg but with different filters map to their
            # own respective rank columns. Use the per-spec match flag
            # (also projected by the ranked subquery) instead of
            # re-emitting spec.filter_sql here — the filter can reference
            # joined-table columns that are not in scope outside the
            # subquery.
            if spec.filter_sql and filtered_rn_map:
                filtered_rn = filtered_rn_map.get(spec.alias, rn_col)
                match_col = (
                    filtered_match_map.get(spec.alias)
                    if filtered_match_map
                    else None
                )
                # Fall back to the raw filter expression only if no match flag
                # was projected (legacy callers); accepts the leak risk.
                filter_clause = f"{match_col} = 1" if match_col else spec.filter_sql
                case_sql = (
                    f"MAX(CASE WHEN {filtered_rn} = 1 AND {filter_clause} "
                    f"THEN {col} END)"
                )
            else:
                # ``col`` is already a fully-qualified SQL expression resolved
                # via ``_resolve_sql`` earlier in this branch, so we don't need
                # to re-prefix ``spec.model_name``. (DEV-1333.)
                case_sql = f"MAX(CASE WHEN {rn_col} = 1 THEN {col} END)"
            return self._parse(case_sql), True

        # --- Custom or parameterized aggregation (formula-based) ---
        if agg_name not in _AGG_FUNCTION_MAP:
            # percentile is dialect-dependent (no static formula works on
            # SQLite/ClickHouse/MySQL) so it gets its own builder rather than
            # going through the BUILTIN_AGGREGATION_FORMULAS path.
            if agg_name == "percentile":
                return self._build_percentile(spec), True
            # Statistical aggregates also dispatch to a dedicated builder so
            # the SQLite-UDF / native-function / NotImplementedError split
            # mirrors _build_median.
            if agg_name in _STAT_AGG_NAMES:
                return self._build_stat_agg(spec), True
            # count_distinct_approx (DEV-1595): dialect-aware approximate-
            # distinct — native function (DuckDB/ClickHouse/BigQuery/…) or the
            # exact COUNT(DISTINCT) fallback (Postgres/SQLite/MySQL). Built like
            # percentile/stat-agg (via _wrap_filter + _resolve_value_sql) so a
            # row-level filter wraps as COUNT(DISTINCT (CASE WHEN ... END)).
            if agg_name == "count_distinct_approx":
                col_expr = _wrap_filter(
                    self._resolve_value_sql(spec), spec.filter_sql
                )
                return self._dialect.build_approx_count_distinct(
                    col_sql=col_expr, parse=self._parse
                ), True
            return self._build_formula_agg(spec, agg_name), True

        # --- Resolve inner expression ---
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
        if agg_name == "count_distinct":
            return exp.Count(this=exp.Distinct(expressions=[inner])), True

        # --- median (dialect-dependent) ---
        if agg_name == "median":
            return self._build_median(inner), True

        # --- Standard aggregations (sum, avg, min, max, count) ---
        agg_class_map = {
            "COUNT": exp.Count,
            "SUM": exp.Sum,
            "AVG": exp.Avg,
            "MIN": exp.Min,
            "MAX": exp.Max,
        }
        agg_func = _AGG_FUNCTION_MAP[agg_name]
        agg_class = agg_class_map[agg_func]
        return agg_class(this=inner), True

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
    # The legacy generator (everything above) consumes EnrichedQuery. This
    # new entry point consumes the typed PlannedQuery from
    # slayer/engine/stage_planner.py. The two paths coexist until the
    # engine cutover (stage 7b.15) flips the default path.
    #
    # 7b.8 scope: local-only single-model queries — row-phase dims, local
    # aggregates, Mode-B row filters, ORDER BY / LIMIT / OFFSET, dim-only
    # dedup. Cross-model, time dimensions, transforms, and aggregate
    # filtering raise NotImplementedError with an explicit stage marker
    # so silent parity drift is impossible.
    # ======================================================================

    def generate_from_planned(self, planned_query, *, bundle) -> str:
        """Render a typed ``PlannedQuery`` to SQL (public entry).

        DEV-1708 (D-E): installs a fresh generation-wide ``AliasAllocator`` for
        the duration of this call and restores the caller's on exit. Inline
        forward ``_cm_*`` CTEs and the host base share this one allocator, so
        their ``_val_<n>`` materialisation names never collide; a recursive
        rerooted sub-generation (``_render_rerooted_cross_model_cte`` →
        ``generate_from_planned``) is a self-contained statement and gets its
        own allocator, with the parent's restored afterwards.
        """
        self._assert_projection_is_public(planned_query)
        prev_allocator = getattr(self, "_gen_allocator", None)
        self._gen_allocator = self._new_allocator()
        try:
            return self._generate_from_planned_impl(
                planned_query, bundle=bundle,
            )
        finally:
            self._gen_allocator = prev_allocator

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
    ) -> str:
        """Render a typed ``PlannedQuery`` to SQL.

        NOTE (DEV-1716): this is a STAGE renderer — its output feeds
        ``generate_planned_stages``' flat-column stage-schema wrapper, so the
        dialect ``rewrite_emitted_sql`` alias-mangling post-pass is applied by
        the DB-bound terminal (``generate_planned_stages``), NOT here. Mangling
        a stage's column names would break the downstream flat-name binding.

        Mirrors the local-only branch of ``_generate_base`` but reads
        from typed PlannedQuery fields (``row_slots`` / ``aggregate_slots``
        / ``filters_by_phase`` / ``order`` / ``transform_layers``)
        instead of ``EnrichedQuery``. Reuses legacy dialect helpers
        (``_resolve_sql`` / ``_build_agg`` / ``_wrap_cast_for_type`` /
        ``_parse_predicate`` / ``_build_date_trunc``) so dialect-specific
        behavior is rendered identically to the legacy ``generate()``
        path — the parity oracle in ``tests/parity_oracle.py`` pins
        this contract.

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

        if (
            planned_query.cross_model_aggregate_plans
            or planned_query.windowed_aggregate_plans
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
        (
            base_select,
            aliases_by_slot_id,
            has_aggregation,
            group_by_keys,
            where_consumed,
            first_last_state,
        ) = self._build_base_select_for_planned(
            planned_query=planned_query,
            bundle=bundle,
            source_model=source_model,
            source_relation=source_relation,
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
        )

        where_clause, having_clause = self._build_where_having_from_planned(
            planned_query=planned_query,
            source_relation=source_relation,
            source_model=source_model,
            bundle=bundle,
            first_last_state=first_last_state,
            aliases_by_slot_id=aliases_by_slot_id,
        )

        # ``where_consumed`` is True for the first/last ranked-subquery path:
        # the WHERE is applied INSIDE the ranked subquery (it must filter raw
        # rows before ranking), so re-applying it on the outer SELECT would be
        # both redundant and — for filters that should narrow the ranked set —
        # semantically wrong.
        if where_clause is not None and not where_consumed:
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
                return self._build_outer_trim_wrap_sql(
                    base_select=base_select,
                    planned_query=planned_query,
                    source_relation=source_relation,
                    aliases_by_slot_id=aliases_by_slot_id,
                    slots_by_id=slots_by_id,
                    bundle=bundle,
                )
            base_select = self._apply_order_limit_from_planned(
                select=base_select,
                planned_query=planned_query,
                source_relation=source_relation,
                slots_by_id=slots_by_id,
                source_model=source_model,
                bundle=bundle,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            return base_select.sql(dialect=self.dialect, pretty=True)

        # 7b.10 — transform layers present. Build the CTE chain.
        base_cte_sql = base_select.sql(dialect=self.dialect, pretty=True)
        ctes: list[tuple[str, str]] = [("base", base_cte_sql)]
        # DEV-1692: collision-safe CTE-name allocator for the whole transform
        # chain. The hoisted time_shift slot alias (``_time_shift_inner``)
        # repeats across arithmetic-wrapped shifts, so two ``shifted_`` /
        # ``sjoin_`` pairs would otherwise share a name (duplicate WITH). Every
        # CTE name is reserved/allocated through this one allocator so the
        # ``step`` / ``shifted_`` / ``sjoin_`` / ``cp_`` families never collide.
        cte_allocator = self._new_allocator()
        cte_allocator.reserve(*(name for name, _ in ctes))
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
            )
        )
        while pending_layers:
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
            if not (ready_window or ready_time_shift or ready_cp):
                pending_ops = [layer.op for layer in pending_layers]
                raise RuntimeError(
                    f"DEV-1450 stage 7b.11: transform layer dependencies "
                    f"could not be resolved; pending ops: {pending_ops!r}.",
                )
            # --- Window batch (one step CTE per Kahn batch) ----------
            if ready_window:
                step_num += 1
                step_name = cte_allocator.allocate_cte(f"step{step_num}")
                prev_cte = ctes[-1][0]
                carry_aliases_sorted = self._carry_aliases_in_plan_order(
                    aliases_by_slot_id,
                )
                step_parts = [self._quote_ident(a) for a in carry_aliases_sorted]
                for layer in ready_window:
                    for slot_id in layer.slot_ids:
                        slot = slots_by_id[slot_id]
                        alias = (
                            slot.public_aliases[0]
                            if slot.public_aliases
                            else slot.declared_name
                        )
                        full_alias = f"{source_relation}.{alias}"
                        window_sql = self._render_window_transform_sql(
                            slot=slot,
                            slots_by_id=slots_by_id,
                            slot_id_by_key=slot_id_by_key,
                            available_alias_by_slot_id=available_alias_by_slot_id,
                            planned_query=planned_query,
                        )
                        if slot.type is not None:
                            wrapped = _wrap_cast_for_type(
                                self._parse(window_sql), slot.type,
                            )
                            window_sql = wrapped.sql(dialect=self.dialect)
                        step_parts.append(f'{window_sql} AS {self._quote_ident(full_alias)}')
                        aliases_by_slot_id.setdefault(slot_id, []).append(
                            full_alias,
                        )
                        available_alias_by_slot_id.setdefault(
                            slot_id, full_alias,
                        )
                step_sql = (
                    "SELECT\n    "
                    + _SQL_COL_SEP.join(step_parts)
                    + f"\nFROM {prev_cte}"
                )
                ctes.append((step_name, step_sql))
            # --- time_shift layers (each gets shifted_ + sjoin_ pair) -
            for layer in ready_time_shift:
                for slot_id in layer.slot_ids:
                    slot = slots_by_id[slot_id]
                    self._emit_time_shift_ctes_for_planned(
                        slot=slot,
                        ctes=ctes,
                        cte_allocator=cte_allocator,
                        slots_by_id=slots_by_id,
                        slot_id_by_key=slot_id_by_key,
                        available_alias_by_slot_id=available_alias_by_slot_id,
                        aliases_by_slot_id=aliases_by_slot_id,
                        source_model=source_model,
                        source_relation=source_relation,
                        shifted_where_parts=shifted_where_parts,
                        shifted_where_join_paths=shifted_where_join_paths,
                        planned_query=planned_query,
                        bundle=bundle,
                    )
            # --- consecutive_periods layers (cp_reset_ + cp_value_ pair)
            for layer in ready_cp:
                for slot_id in layer.slot_ids:
                    slot = slots_by_id[slot_id]
                    self._emit_consecutive_periods_ctes_for_planned(
                        slot=slot,
                        ctes=ctes,
                        cte_allocator=cte_allocator,
                        slots_by_id=slots_by_id,
                        slot_id_by_key=slot_id_by_key,
                        available_alias_by_slot_id=available_alias_by_slot_id,
                        aliases_by_slot_id=aliases_by_slot_id,
                        planned_query=planned_query,
                        source_relation=source_relation,
                    )
            pending_layers = not_ready

        # 7b.11 — materialise POST-phase ArithmeticKey / ScalarCallKey
        # slots that the user projected but no transform layer rendered.
        # ``change(amount:sum)`` lowers to ``amount:sum - time_shift(...)``;
        # the time_shift slot is rendered as a self-join CTE pair, but
        # the outer ArithmeticKey slot that subtracts them needs its
        # own step CTE. Same shape covers ``change_pct`` (division of
        # arithmetic operands) and any future POST-phase non-transform
        # slot the planner emits.
        from slayer.core.keys import (
            ArithmeticKey as _ArithKey,
            ScalarCallKey as _ScalarKey,
            TransformKey as _TKey,
        )
        unmaterialised: list = []
        for cslot in planned_query.combined_expression_slots:
            if isinstance(cslot.key, _TKey):
                # Transform-key slots are materialised by transform_layers.
                continue
            if cslot.id in aliases_by_slot_id:
                continue
            if isinstance(cslot.key, (_ArithKey, _ScalarKey)):
                unmaterialised.append(cslot)
        if unmaterialised:
            step_num += 1
            step_name = cte_allocator.allocate_cte(f"step{step_num}")
            prev_cte = ctes[-1][0]
            carry_aliases_sorted = self._carry_aliases_in_plan_order(
                aliases_by_slot_id,
            )
            step_parts = [self._quote_ident(a) for a in carry_aliases_sorted]
            for cslot in unmaterialised:
                alias = (
                    cslot.public_aliases[0]
                    if cslot.public_aliases
                    else cslot.declared_name
                )
                full_alias = f"{source_relation}.{alias}"
                rendered = self._render_value_key_against_aliases(
                    key=cslot.key,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                )
                expr_sql = rendered.sql(dialect=self.dialect)
                if cslot.type is not None:
                    wrapped = _wrap_cast_for_type(
                        self._parse(expr_sql), cslot.type,
                    )
                    expr_sql = wrapped.sql(dialect=self.dialect)
                step_parts.append(f'{expr_sql} AS {self._quote_ident(full_alias)}')
                aliases_by_slot_id.setdefault(cslot.id, []).append(
                    full_alias,
                )
                available_alias_by_slot_id.setdefault(
                    cslot.id, full_alias,
                )
            step_sql = (
                "SELECT\n    "
                + _SQL_COL_SEP.join(step_parts)
                + f"\nFROM {prev_cte}"
            )
            ctes.append((step_name, step_sql))

        # Inner SELECT inside _outer wrap: ALL carried aliases sorted
        # in PLAN order (B8 — this list used to be sorted alphabetically to
        # match the legacy renderer byte-for-byte).
        final_cte = ctes[-1][0]
        inner_sorted = self._carry_aliases_in_plan_order(aliases_by_slot_id)
        inner_sql = (
            "SELECT\n    "
            + _SQL_COL_SEP.join(self._quote_ident(a) for a in inner_sorted)
            + f"\nFROM {final_cte}"
        )

        cte_clause = (
            _SQL_WITH
            + ",\n".join(f"{name} AS (\n{sql}\n)" for name, sql in ctes)
        )
        chain_sql = f"{cte_clause}\n{inner_sql}"

        # POST-phase filter wrap (filters referencing transform / arith
        # slots). Mirrors legacy _generate_with_computed:1627-1648 —
        # ``SELECT * FROM (<chain>) AS _filtered WHERE <conditions>``.
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

        # Outer SELECT in user-projection order (public slots only).
        # Per-slot index walks each slot's public_aliases so duplicate
        # interned names (DEV-1450 C13) both surface in the result.
        public_aliases_user_order: list[str] = []
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
            public_aliases_user_order.append(alias)
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
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            ScalarCallKey,
            TimeTruncKey,
            TransformKey,
        )

        # 7b.11 lifted these — placeholder set for future slices.
        deferred: set = set()

        leaf_kinds = (ColumnKey, ColumnSqlKey, AggregateKey, TimeTruncKey)
        # Keep aligned with _emit_consecutive_periods_ctes_for_planned —
        # the renderer dispatches arithmetic ops via _compose_arithmetic_op
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
        from slayer.core.keys import AggregateKey as _AggKey
        from slayer.engine.binding import walk_value_keys

        remote_slot_ids = {
            p.aggregate_slot_id
            for p in planned_query.cross_model_aggregate_plans
        } | {
            p.aggregate_slot_id
            for p in planned_query.windowed_aggregate_plans
        }
        for node in walk_value_keys(key):
            if not isinstance(node, _AggKey):
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
    ) -> Set[str]:
        """Return slot ids the base CTE must project beyond the public
        projection.

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
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            Phase,
            ScalarCallKey,
            TimeTruncKey,
            TransformKey,
        )

        if aggregates_only:
            base_kinds: Tuple[type, ...] = (AggregateKey,)
        else:
            base_kinds = (ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey)
        out: Set[str] = set()

        def _collect_from(key) -> None:
            if isinstance(key, base_kinds):
                sid = slot_id_by_key.get(key)
                if sid is not None:
                    out.add(sid)
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
                            BetweenKey, InKey, ColumnKey, ColumnSqlKey,
                            TimeTruncKey, AggregateKey,
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
                        out.add(oe.slot_id)

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
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            ScalarCallKey,
            TimeTruncKey,
            TransformKey,
        )

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
    ) -> "Dict[Any, Dict[str, ResolvedAggKwarg]]":
        """Resolve every LOCAL aggregate's join-crossing inputs through the host
        ``scope`` (Law 1) — ``scope.resolve`` anchors each ref and registers the
        joins it crosses into ``scope.join_paths``, the side effect that
        base-pulls the crossed LEFT JOIN.

        Three ordered sub-passes over ``base_render_order`` preserve the pre-
        resolver join-registration order (Column.filter → source → kwargs):

        1. **``Column.filter`` predicates** (DEV-1494; replaces
           ``_collect_column_filter_join_paths``). The Mode-A predicate is
           dual-scanned via ``_filter_join_paths`` (raw + inline-expanded, so a
           placeholder dotted ref that inlines to a constant still pulls its
           join) and the paths registered into the scope.
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
           the arg via ``_resolve_explicit_time_col``. Replaces the legacy
           ``_collect_joined_paths_for_base`` AGGREGATE arm. A path-bearing
           derived (``ColumnSqlKey``) arg — the DEV-1526 residual — is skipped.

        Cross-model aggregates (non-empty ``source.path``) are skipped in every
        sub-pass: their inputs are owned by the per-plan ``_cm_*`` CTE
        (Stage 4 / DEV-1708). Recurses into composite AGGREGATE keys.
        """
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            ColumnKey,
            ColumnSqlKey,
            Phase,
            ScalarCallKey,
        )

        resolved: "Dict[Any, Dict[str, ResolvedAggKwarg]]" = {}

        def _walk(key, fn) -> None:
            if isinstance(key, AggregateKey):
                if not getattr(key.source, "path", ()):
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
            if isinstance(key.source, ColumnSqlKey):
                scope.resolve(key.source)  # register-only; render re-expands

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
            self._register_fragment_kwarg_joins(
                key=key, scope=scope, model=scope.root_model,
            )

        def _resolve_first_last_time_arg(key) -> None:
            # DEV-1710 Stage 6 — a first/last explicit ranking-time arg
            # (``amount:last(customers.signup_at)``) crosses a join exactly like
            # a source / kwarg does; resolving it through the scope registers
            # that join (Law 1), so the ranked subquery's ORDER BY ref is in the
            # base FROM. Replaces the legacy ``_collect_joined_paths_for_base``
            # AGGREGATE arm. Register-only: the render spec re-resolves via
            # ``_resolve_explicit_time_col``.
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

    def _resolve_agg_kwargs_for_key(
        self, *, key, source_model, source_relation: str, bundle,
    ) -> "Optional[Dict[str, ResolvedAggKwarg]]":
        """Resolve a single LOCAL aggregate's column-ref kwargs
        (``weighted_avg(weight=<col>)`` / ``corr(other=<col>)``) through a fresh
        host ``ScopeFrame`` → ``{name: ResolvedAggKwarg(kind="expr")}`` or ``None``.

        The base SELECT uses the batch ``_resolve_agg_inputs_via_scope`` pass over
        a shared host scope (which also registers the crossed joins). The HAVING
        render path (``_render_value_key_for_filter``) has no such scope, so it
        builds a throwaway one here purely to reproduce the SAME anchored kwarg
        expression the SELECT emits — the crossed join is already base-pulled
        (the HAVING aggregate is also a ``base_render_order`` slot), so the
        throwaway scope's own ``join_paths`` are intentionally discarded.
        """
        from slayer.core.keys import ColumnKey, ColumnSqlKey

        kwargs = getattr(key, "kwargs", None)
        if bundle is None or not kwargs:
            return None
        allocator = self._new_allocator()
        scope = ScopeFrame(
            scope_id=allocator.next_scope_id(source_relation),
            root_model=source_model,
            root_relation=source_relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=allocator,
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
        from slayer.core.enums import TimeGranularity
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            ColumnKey,
            ColumnSqlKey,
            Phase,
            ScalarCallKey,
            TimeTruncKey,
        )

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
        host_scope = ScopeFrame(
            scope_id=host_allocator.next_scope_id(source_relation),
            root_model=source_model,
            root_relation=source_relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=host_allocator,
        )
        # Pre-expand derived (ColumnSqlKey) ROW + TIME dimensions: inline
        # sibling/joined derived refs (DEV-1333 / DEV-1410) and register any
        # joins their SQL crosses into the scope (position 2). Returns the
        # expanded-expr-by-slot-id map the render branch reads.
        derived_expr_by_sid = self._expand_derived_row_dims(
            base_render_order=base_render_order, slots_by_id=slots_by_id,
            source_relation=source_relation, source_model=source_model,
            bundle=bundle, scope=host_scope,
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

        # DEV-1450: first/last AGGREGATIONS rank rows via a ROW_NUMBER
        # subquery (mirrors legacy ``_generate_base`` + ``_build_last_
        # ranked_from``).
        if self._has_first_last_aggregate(
            base_render_order=base_render_order, slots_by_id=slots_by_id,
        ):
            # DEV-1503 — local first/last in ``_base`` alongside cross-model
            # CTEs is supported: the ranked subquery wraps ``_base`` for the
            # local measures; cross-model / filtered-local aggregates are
            # deferred to their per-plan ``_cm_*`` CTEs (their slot ids are
            # excluded from ``base_render_order`` by the caller, so
            # ``_build_first_last_base_select`` never sees them and emits no
            # dangling references).
            return self._build_first_last_base_select(
                planned_query=planned_query,
                bundle=bundle,
                source_model=source_model,
                source_relation=source_relation,
                base_render_order=base_render_order,
                slots_by_id=slots_by_id,
                from_clause=from_clause,
                base_joins=base_joins,
                skip_filter_ids=skip_filter_ids,
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
                elif isinstance(key, (ScalarCallKey, ArithmeticKey)):
                    # DEV-1576 / DEV-1717: a ROW-phase composite here is a
                    # non-aggregating measure expression (a bare column, or
                    # arithmetic / scalar-call over bare columns such as
                    # ``round(amount, 2)`` / ``abs(amount)`` / ``amount + 1``).
                    # Dimensions are ColumnKey / TimeTruncKey / ColumnSqlKey,
                    # already handled above; the only way to reach here with a
                    # composite key is a measure that never aggregates. Raise
                    # the same actionable "Bare measure name" error the
                    # enrich_query path raises rather than leaking an internal
                    # NotImplementedError.
                    bare = _first_bare_column_name(key) or full_alias
                    raise ValueError(
                        f"Bare measure name '{bare}' is not valid. "
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
                    composite, any_agg = self._render_aggregate_composite_expr(
                        key=key,
                        slot=slot,
                        source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
                        resolved_agg_kwargs=resolved_agg_kwargs,
                    )
                    if any_agg:
                        composite = _wrap_cast_for_type(composite, slot.type)
                        has_aggregation = True
                    select_columns.append(composite.copy().as_(full_alias))
                    _record_alias(sid, full_alias)
                    continue
                agg_path = getattr(key.source, "path", ())
                if agg_path:
                    if skip_cross_model_aggs:
                        # Cross-model aggregate; rendered by the per-plan
                        # ``_cm_*`` CTE. Skip in the host base.
                        continue
                    raise NotImplementedError(
                        f"DEV-1450 stage 7b.12: cross-model aggregate "
                        f"(source.path={agg_path!r}) reached the local "
                        f"base SELECT path. The cross-model orchestrator "
                        f"should have routed this through `_render_with_"
                        f"cross_model_plans`."
                    )
                # DEV-1450 stage 7b.12: ``column_filter_key`` is now
                # propagated into the synthetic EnrichedMeasure's
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
                    agg_expr = _wrap_cast_for_type(agg_expr, slot.type)
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
        for join_expr, on_expr, join_type in base_joins:
            base_select = base_select.join(
                join_expr, on=on_expr, join_type=join_type,
            )
        return (
            base_select, aliases_by_slot_id, has_aggregation, group_by_keys,
            False, None,
        )

    def _has_first_last_aggregate(
        self, *, base_render_order: List[str], slots_by_id: Dict[str, Any],
    ) -> bool:
        """True if any LOCAL ``first`` / ``last`` AGGREGATE slot appears in
        the base render order — directly as an ``AggregateKey`` slot OR
        as an operand inside a composite (``ArithmeticKey`` /
        ``ScalarCallKey``) aggregate slot.

        Cross-model first/last (non-empty ``source.path``) is excluded —
        it is not rendered by the ranked-subquery path (each cross-model
        aggregate has its own CTE). DEV-1501 (Codex round 4): composite-
        only first/last (e.g. ``last(created_at) + last(updated_at)``
        with no direct sibling) must still trigger the ranked-subquery
        path; without composite-aware detection the composite render
        would emit ``MAX(CASE WHEN _last_rn = 1 …)`` referencing a
        column the bare-FROM never projects.
        """
        from slayer.core.keys import AggregateKey, Phase

        for sid in base_render_order:
            slot = slots_by_id.get(sid)
            if slot is None or slot.phase != Phase.AGGREGATE:
                continue
            key = slot.key
            if (
                isinstance(key, AggregateKey)
                and key.agg in ("first", "last")
                and not getattr(key.source, "path", ())
            ):
                return True
            # Composite slot (no direct AggregateKey): walk for first/
            # last AggregateKey leaves. The composite render needs the
            # ranked subquery so each operand's ``_first_rn`` /
            # ``_last_rn{suffix}`` column exists.
            if not isinstance(key, AggregateKey) and _iter_first_last_leaves(key):
                return True
        return False

    def _resolve_ranking_time_column_from_planned(
        self,
        *,
        base_render_order: List[str],
        slots_by_id: Dict[str, Any],
        source_model,
        source_relation: str,
        bundle,
    ) -> Optional[str]:
        """Resolve the default ORDER-BY time column for first/last
        ROW_NUMBER ranking (mirrors legacy ``_resolve_last_agg_time``).

        Precedence (matching legacy): the first ``DATE`` / ``TIMESTAMP``
        regular dimension, then the first time-dimension slot's raw column,
        then the model's ``default_time_dimension``. Returns the qualified
        SQL string (e.g. ``"orders.created_at"`` / ``"stores.opened_at"``),
        or ``None`` when nothing temporal is in scope.

        (The legacy ``main_time_dimension`` short-circuit and the
        filter-referenced-date fallback are corner cases the spec permits
        diverging on; they are not reproduced here.)
        """
        from slayer.core.keys import ColumnKey, Phase, TimeTruncKey

        for sid in base_render_order:
            slot = slots_by_id[sid]
            if slot.phase == Phase.ROW and isinstance(slot.key, ColumnKey):
                model = source_model
                for hop in slot.key.path:
                    nxt = bundle.get_referenced_model(hop)
                    if nxt is None:
                        model = None
                        break
                    model = nxt
                if model is None:
                    continue
                col_def = next(
                    (c for c in model.columns if c.name == slot.key.leaf), None,
                )
                if col_def is not None and col_def.type in (
                    DataType.DATE, DataType.TIMESTAMP,
                ):
                    return self._joined_or_local_dim_expr(
                        path=slot.key.path, leaf=slot.key.leaf,
                        source_model=source_model,
                        source_relation=source_relation, bundle=bundle,
                    ).sql(dialect=self.dialect)
        for sid in base_render_order:
            slot = slots_by_id[sid]
            if slot.phase == Phase.ROW and isinstance(slot.key, TimeTruncKey):
                return self._raw_time_col_expr_for_planned(
                    time_column=slot.key.column, source_model=source_model,
                    source_relation=source_relation, bundle=bundle,
                ).sql(dialect=self.dialect)
        if source_model.default_time_dimension:
            return f"{source_relation}.{source_model.default_time_dimension}"
        return None

    @staticmethod
    def _explicit_time_arg_of(key):
        """The explicit positional ranking-time arg of a ``first`` / ``last``
        aggregate, or ``None``.

        The SINGLE arg-selection contract shared by the three sites that must
        never disagree on WHICH positional arg is the time column (DEV-1710 /
        Codex F1): the raise-gate in ``_build_first_last_base_select``, the
        join-discovery pass in ``_resolve_agg_inputs_via_scope``, and the render
        seam ``_resolve_explicit_time_col``. Returns the FIRST positional arg
        iff it is a ``ColumnKey`` / ``ColumnSqlKey``; ``None`` for a
        non-first/last agg, empty args, or a first positional arg of any other
        type (first/last never takes a leading non-column positional).
        """
        from slayer.core.keys import ColumnKey, ColumnSqlKey

        if key.agg not in ("first", "last"):
            return None
        for a in key.args:
            return a if isinstance(a, (ColumnKey, ColumnSqlKey)) else None
        return None

    def _resolve_explicit_time_col(
        self,
        *,
        key,
        source_model,
        source_relation: str,
        bundle=None,
    ) -> Optional[str]:
        """Resolve the explicit positional time arg on a ``first`` / ``last``
        aggregate into a SQL string suitable for ``ORDER BY`` inside the
        ranked subquery.

        Handles both bare-column refs (``ColumnKey`` —
        ``amount:last(created_at)``) and derived-column refs (``ColumnSqlKey``
        — ``amount:last(net_amount_date)`` where ``net_amount_date`` has a
        non-trivial ``Column.sql``). DEV-1710 Stage 6: when a ``bundle`` is
        available the arg is anchored through a ``ScopeFrame`` (Law 1) — the
        same resolver the host base / kwargs passes use — so a bare joined ref
        qualifies to its ``__``-path alias, a derived expression's inner bare
        refs qualify to ``source_relation`` (never ambiguous against a
        same-named joined column), and reserved-word relations are quoted
        (DEV-1686). Without a ``bundle`` (the render-spec unit path) it falls
        back to bare-ident qualification / verbatim emit.

        Returns ``None`` for non-first/last aggs and when ``key.args`` is empty
        or its first element is neither a ``ColumnKey`` nor a ``ColumnSqlKey``
        (see ``_explicit_time_arg_of``). A derived time arg (``ColumnSqlKey``)
        whose ``path`` is non-empty AFTER the DEV-1707 cross-model reroot — a
        column a hop PAST the target — raises ``NotImplementedError`` rather
        than silently emitting against a relation the isolated CTE does not
        join; that residual-hop case is tracked as DEV-1526 (Stage 4). The
        analogous residual ``ColumnKey`` arg is caught loudly by the
        scope-closure validator (``SLAYER_VALIDATE_SCOPES``) instead.
        """
        from slayer.core.keys import ColumnKey, ColumnSqlKey

        arg = self._explicit_time_arg_of(key)
        if arg is None:
            return None
        if isinstance(arg, ColumnSqlKey) and arg.path:
            raise NotImplementedError(
                f"Derived time column with a residual join path "
                f"(path={arg.path!r}, column={arg.column_name!r}) on a "
                f"first/last positional arg is not yet supported by "
                f"the ranked-subquery builder: the isolated CTE does "
                f"not pull the residual join. Post-DEV-1707 the "
                f"cross-model reroot strips the target prefix, so this "
                f"fires only for a time arg a hop PAST the target; "
                f"tracked as DEV-1526 (Stage 4)."
            )
        # Validate a derived arg's existence up front so the not-found case is a
        # clear error rather than the resolver silently anchoring the bare name.
        col = None
        if isinstance(arg, ColumnSqlKey):
            col = next(
                (c for c in source_model.columns if c.name == arg.column_name),
                None,
            )
            if col is None:
                raise ValueError(
                    f"Derived time column {arg.column_name!r} (positional "
                    f"arg of {key.agg!r}) not found on model "
                    f"{source_model.name!r}."
                )
        if bundle is not None:
            # Law 1 — anchor the arg through a throwaway host-rooted scope. Its
            # ``join_paths`` are discarded (discovery is owned by the base
            # aggregate-input pass, which registers the same join); this call is
            # purely to reproduce the SAME anchored SQL the ORDER BY needs. Same
            # throwaway-frame pattern as ``_resolve_agg_kwargs_for_key``.
            allocator = self._new_allocator()
            scope = ScopeFrame(
                scope_id=allocator.next_scope_id(source_relation),
                root_model=source_model,
                root_relation=source_relation,
                bundle=bundle,
                dialect=self._dialect,
                allocator=allocator,
            )
            return scope.resolve(arg).sql(dialect=self.dialect)
        # No bundle (defensive; the render-spec unit path): bare ColumnKey
        # qualifies to its ``__``-path alias / source relation, a derived
        # bare-ident qualifies to the source relation, else emit verbatim.
        if isinstance(arg, ColumnKey):
            relation = "__".join(arg.path) if arg.path else source_relation
            return f"{relation}.{arg.leaf}"
        col_sql = col.sql if col.sql else col.name
        if col_sql.isidentifier():
            return f"{source_relation}.{col_sql}"
        return self._parse(col_sql).sql(dialect=self.dialect)

    def _build_ranked_subquery_from_planned(  # NOSONAR(S3776) — Group 2 already factored the per-spec ROW_NUMBER passes into _build_unfiltered_rn_columns / _build_filtered_rn_columns; what's left is exp.Select / from / joins / where assembly that has to live in one place.
        self,
        *,
        source_relation: str,
        default_time_col_sql: str,
        partition_exprs: List[exp.Expression],
        extra_projections: List[Tuple[str, exp.Expression]],
        synth_specs: List[AggRenderSpec],
        from_clause: exp.Expression,
        base_joins: List,
        where_clause: Optional[exp.Expression],
    ) -> Tuple[exp.Expression, dict, dict, dict]:
        """Build the ROW_NUMBER-ranked subquery that wraps the source for
        first/last aggregation (planned-native port of
        ``_build_last_ranked_from``).

        Projects ``source_relation.*`` plus the supplied ``extra_projections``
        (truncated time dimensions / joined dimensions referenced by the
        outer SELECT) plus one ``ROW_NUMBER`` column per distinct
        (effective-time-column, agg) pair. Filtered first/last measures get
        a dedicated ranking column (non-matching rows pushed to the bottom)
        and a boolean match flag. WHERE is applied INSIDE so it filters raw
        rows before ranking. Returns ``(subquery, rn_suffix_map,
        filtered_rn_map, filtered_match_map)``.
        """
        partition_clause = ""
        if partition_exprs:
            partition_clause = _SQL_PARTITION_BY + ", ".join(
                p.sql(dialect=self.dialect) for p in partition_exprs
            )

        select_exprs: List[exp.Expression] = [
            exp.Column(this=exp.Star(), table=exp.to_identifier(source_relation)),
        ]
        for alias, e in extra_projections:
            select_exprs.append(e.copy().as_(alias))

        unfiltered_exprs, rn_suffix_map = self._build_unfiltered_rn_columns(
            synth_specs=synth_specs,
            default_time_col_sql=default_time_col_sql,
            partition_clause=partition_clause,
        )
        select_exprs.extend(unfiltered_exprs)

        filtered_exprs, filtered_rn_map, filtered_match_map = (
            self._build_filtered_rn_columns(
                synth_specs=synth_specs,
                default_time_col_sql=default_time_col_sql,
                partition_clause=partition_clause,
            )
        )
        select_exprs.extend(filtered_exprs)

        inner = exp.Select()
        for e in select_exprs:
            inner = inner.select(e)
        inner = inner.from_(from_clause)
        for join_expr, on_expr, join_type in base_joins:
            inner = inner.join(join_expr, on=on_expr, join_type=join_type)
        if where_clause is not None:
            inner = inner.where(where_clause)
        subquery = exp.Subquery(
            this=inner, alias=exp.to_identifier(source_relation),
        )
        return subquery, rn_suffix_map, filtered_rn_map, filtered_match_map

    def _build_unfiltered_rn_columns(
        self,
        *,
        synth_specs: List[AggRenderSpec],
        default_time_col_sql: str,
        partition_clause: str,
    ) -> Tuple[List[exp.Expression], Dict[str, str]]:
        """One ``ROW_NUMBER`` projection per distinct effective time column
        for the unfiltered ``first`` / ``last`` specs.

        Each unique effective time column gets a stable suffix in render
        order (first sorted gets ``""``, then ``"_2"``, ...); the same
        time column shared by both ``first`` and ``last`` produces two
        projections (`_first_rn{suffix}` ASC, `_last_rn{suffix}` DESC).
        Returns ``(rn_select_exprs, rn_suffix_map)``.
        """
        time_col_agg_types: Dict[str, set] = {}
        for m in synth_specs:
            if m.aggregation in ("first", "last") and not m.filter_sql:
                eff = m.time_column or default_time_col_sql
                time_col_agg_types.setdefault(eff, set()).add(m.aggregation)
        sorted_tcs = sorted(time_col_agg_types)
        rn_suffix_map: Dict[str, str] = {
            tc: ("" if i == 0 else f"_{i + 1}")
            for i, tc in enumerate(sorted_tcs)
        }
        rn_exprs: List[exp.Expression] = []
        for tc in sorted_tcs:
            suffix = rn_suffix_map[tc]
            if "last" in time_col_agg_types[tc]:
                rn_exprs.append(
                    self._parse(
                        f"ROW_NUMBER() OVER ({partition_clause} "
                        f"ORDER BY {tc} DESC)"
                    ).as_(f"_last_rn{suffix}")
                )
            if "first" in time_col_agg_types[tc]:
                rn_exprs.append(
                    self._parse(
                        f"ROW_NUMBER() OVER ({partition_clause} "
                        f"ORDER BY {tc} ASC)"
                    ).as_(f"_first_rn{suffix}")
                )
        return rn_exprs, rn_suffix_map

    def _build_filtered_rn_columns(
        self,
        *,
        synth_specs: List[AggRenderSpec],
        default_time_col_sql: str,
        partition_clause: str,
    ) -> Tuple[List[exp.Expression], Dict[str, str], Dict[str, str]]:
        """One dedicated ``ROW_NUMBER`` + match-flag projection per distinct
        ``(filter, time, agg)`` triple for the filtered ``first`` / ``last``
        specs.

        Filtered first/last needs to push non-matching rows past the
        winners; emits ``ROW_NUMBER() OVER (... ORDER BY CASE WHEN
        <filter> THEN 0 ELSE 1 END, <time> <dir>)`` alongside a boolean
        match-flag column so the outer SELECT can ``MAX(CASE WHEN _rn = 1
        AND _match = 1 THEN col END)``. Triples that repeat across specs
        share a single (rn, match) pair; per-spec ``alias`` keys map onto
        those.
        """
        filtered_rn_map: Dict[str, str] = {}
        filtered_match_map: Dict[str, str] = {}
        seen_filters: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
        rn_exprs: List[exp.Expression] = []
        filter_idx = 0
        for m in synth_specs:
            if not (m.aggregation in ("first", "last") and m.filter_sql):
                continue
            eff = m.time_column or default_time_col_sql
            cache_key = (m.filter_sql, eff, m.aggregation)
            cached = seen_filters.get(cache_key)
            if cached is not None:
                rn_alias, match_alias = cached
            else:
                kind = "first" if m.aggregation == "first" else "last"
                rn_alias = f"_{kind}_rn_f{filter_idx}"
                match_alias = f"_match_f{filter_idx}"
                order_dir = "ASC" if m.aggregation == "first" else "DESC"
                rn_exprs.append(
                    self._parse(
                        f"ROW_NUMBER() OVER ({partition_clause} ORDER BY "
                        f"CASE WHEN {m.filter_sql} THEN 0 ELSE 1 END, "
                        f"{eff} {order_dir})"
                    ).as_(rn_alias)
                )
                rn_exprs.append(
                    self._parse(
                        f"CASE WHEN {m.filter_sql} THEN 1 ELSE 0 END"
                    ).as_(match_alias)
                )
                seen_filters[cache_key] = (rn_alias, match_alias)
                filter_idx += 1
            filtered_rn_map[m.alias] = rn_alias
            filtered_match_map[m.alias] = match_alias
        return rn_exprs, filtered_rn_map, filtered_match_map

    def _build_first_last_base_select(  # NOSONAR(S3776) — single conceptual unit: dimension/td/derived-dim classification pass + agg-spec synth + ranked-subquery wrap + outer SELECT/GROUP BY assembly. Splitting forces shared mutable state (partition_exprs / extra_projections / outer_ref_by_sid / synth_by_sid) across helpers without simplifying anything.
        self,
        *,
        planned_query,
        bundle,
        source_model,
        source_relation: str,
        base_render_order: List[str],
        slots_by_id: Dict[str, Any],
        from_clause: exp.Expression,
        base_joins: List,
        skip_filter_ids: Optional[Set[str]] = None,
    ):
        """Render the base SELECT for a query containing LOCAL first/last
        AGGREGATES (planned-native port of legacy ``_generate_base``'s
        ``has_first_or_last`` branch).

        The FROM (+ joins + WHERE) is wrapped in a ROW_NUMBER-ranked
        subquery; dimensions / time-dimensions are materialised inside it
        (``source_relation.*`` plus ``_td_*`` / ``_dim_*`` projections) and
        referenced bare by the outer SELECT, which GROUPs BY them and emits
        each first/last aggregate as ``MAX(CASE WHEN _rn = 1 THEN col END)``.
        WHERE goes inside the subquery (raw-row filtering before ranking), so
        ``where_consumed=True`` is returned to suppress the outer WHERE.

        Returns ``(base_select, aliases_by_slot_id, has_aggregation,
        group_by_keys, where_consumed)``.
        """
        from slayer.core.enums import TimeGranularity
        from slayer.core.keys import (
            AggregateKey,
            ColumnKey,
            ColumnSqlKey,
            Phase,
            TimeTruncKey,
        )

        default_time_col_sql = self._resolve_ranking_time_column_from_planned(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
            source_model=source_model,
            source_relation=source_relation,
            bundle=bundle,
        )
        # DEV-1476 bug (b): the raise must gate on whether ANY first/last
        # aggregate slot lacks an explicit positional time arg. When every
        # first/last spec carries its own ``key.args`` time column, no
        # default is needed and the helper should not raise.
        if default_time_col_sql is None:
            needs_default = False
            # DEV-1501 (Codex round 5): walk both top-level AggregateKey
            # slots AND first/last leaves inside composite slots
            # (ArithmeticKey / ScalarCallKey). A composite like
            # ``revenue:last + 1`` with no explicit time arg and no
            # default time dim would otherwise bypass the validation,
            # then ``_build_unfiltered_rn_columns`` would emit
            # ``ORDER BY None``.
            for sid in base_render_order:
                if needs_default:
                    break
                slot = slots_by_id[sid]
                if slot.phase != Phase.AGGREGATE:
                    continue
                key = slot.key
                fl_keys: list = []
                if isinstance(key, AggregateKey):
                    if key.agg in ("first", "last"):
                        fl_keys = [key]
                else:
                    fl_keys = _iter_first_last_leaves(key)
                for fl in fl_keys:
                    # Whether this leaf carries an explicit ranking-time arg is
                    # the shared ``_explicit_time_arg_of`` contract — the SAME
                    # selection the render seam uses, so the gate and the render
                    # can never disagree (DEV-1710 / Codex F1).
                    if self._explicit_time_arg_of(fl) is None:
                        needs_default = True
                        break
            if needs_default:
                raise ValueError(
                    "first/last aggregation requires a ranking time column "
                    "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
                    "model's default_time_dimension); none is resolvable for "
                    f"model {source_model.name!r}."
                )

        # Pass 1: full aliases (in render order, for C13 cycling), ROW-slot
        # classification (partition / subquery projection / outer ref), and
        # synth measures for aggregate slots.
        #
        # DEV-1501: C13 multi-public-alias support — a slot can appear in
        # ``base_render_order`` multiple times (one per declared public
        # name on a shared key). Track aliases as a per-sid list so pass 2
        # can project the same aggregate expression once per declared
        # alias (instead of overwriting and emitting the same alias N
        # times). The synth spec is computed once per sid; the aggregate
        # value is identical across C13 visits so a single computation
        # suffices.
        alias_index: Dict[str, int] = {}
        full_aliases_by_sid: Dict[str, List[str]] = {}
        partition_exprs: List[exp.Expression] = []
        extra_projections: List[Tuple[str, exp.Expression]] = []
        outer_ref_by_sid: Dict[str, exp.Expression] = {}
        synth_by_sid: Dict[str, "AggRenderSpec"] = {}
        td_counter = 0
        dim_counter = 0

        for sid in base_render_order:
            slot = slots_by_id[sid]
            full_alias = self._full_alias_for_slot(
                slot=slot, source_relation=source_relation,
                alias_index=alias_index,
            )
            full_aliases_by_sid.setdefault(sid, []).append(full_alias)
            if slot.phase == Phase.ROW:
                key = slot.key
                if isinstance(key, TimeTruncKey):
                    raw = self._raw_time_col_expr_for_planned(
                        time_column=key.column,
                        source_model=source_model,
                        source_relation=source_relation, bundle=bundle,
                    )
                    trunc = self._build_date_trunc(
                        col_expr=raw,
                        granularity=TimeGranularity(key.granularity),
                    )
                    alias = f"_td_{td_counter}"
                    td_counter += 1
                    extra_projections.append((alias, trunc))
                    partition_exprs.append(trunc.copy())
                    outer_ref_by_sid[sid] = exp.Column(
                        this=exp.to_identifier(alias),
                    )
                elif isinstance(key, ColumnKey) and key.path:
                    joined = self._joined_or_local_dim_expr(
                        path=key.path, leaf=key.leaf,
                        source_model=source_model,
                        source_relation=source_relation, bundle=bundle,
                    )
                    alias = f"_dim_{dim_counter}"
                    dim_counter += 1
                    extra_projections.append((alias, joined))
                    partition_exprs.append(joined.copy())
                    outer_ref_by_sid[sid] = exp.Column(
                        this=exp.to_identifier(alias),
                    )
                elif isinstance(key, ColumnKey):
                    # Local dimension — available via ``source_relation.*``.
                    # ``_to_ident`` (not bare ``to_identifier``) so a mixed-case
                    # physical column is double-quoted: this expression is
                    # emitted in the ranked subquery's PARTITION BY *and* in the
                    # outer SELECT / GROUP BY, and an unquoted ``events.RegionCode``
                    # out there folds to lowercase on Postgres -> UndefinedColumn
                    # (DEV-1645 Flavor B).
                    local = exp.Column(
                        this=self._to_ident(key.leaf),
                        table=exp.to_identifier(source_relation),
                    )
                    partition_exprs.append(local.copy())
                    outer_ref_by_sid[sid] = local
                elif isinstance(key, ColumnSqlKey):
                    derived = self._dim_column_expr_from_planned(
                        source_model=source_model,
                        source_relation=source_relation,
                        leaf=key.column_name,
                    )
                    alias = f"_dim_{dim_counter}"
                    dim_counter += 1
                    extra_projections.append((alias, derived))
                    partition_exprs.append(derived.copy())
                    outer_ref_by_sid[sid] = exp.Column(
                        this=exp.to_identifier(alias),
                    )
                else:
                    raise NotImplementedError(
                        f"DEV-1450: first/last with row key "
                        f"{type(key).__name__} not supported."
                    )
            elif slot.phase == Phase.AGGREGATE and isinstance(
                slot.key, AggregateKey,
            ):
                # Single aggregate (incl. first/last) — synth + ``_build_agg``.
                # Composite aggregates (ArithmeticKey / ScalarCallKey of
                # aggregates) have no ``key.source``; they render in pass 2
                # via ``_render_aggregate_composite_expr`` (reading the
                # subquery's ``source_relation.*``), matching the normal path.
                # DEV-1501: a C13 sid appears multiple times in
                # ``base_render_order``; the aggregate value is identical
                # across visits so synthesise once per sid, keyed by the
                # first alias.
                #
                # DEV-1709 (closes the DEV-1527/DEV-1476 first/last kwarg
                # deferral): column-ref kwargs are resolved here so the
                # spec embeds the expanded, join-anchored expression; a
                # CROSSING kwarg expression is then Law-2-materialised in
                # the pass below (the outer aggregate body may only
                # reference the ranked subquery's projections).
                if sid not in synth_by_sid:
                    synth_by_sid[sid] = (
                        self._build_agg_render_spec_from_planned(
                            slot=slot, key=slot.key, source_model=source_model,
                            source_relation=source_relation,
                            full_alias=full_alias,
                            bundle=bundle,
                            resolved_agg_kwargs=self._resolve_agg_kwargs_for_key(
                                key=slot.key, source_model=source_model,
                                source_relation=source_relation, bundle=bundle,
                            ),
                        )
                    )

        # DEV-1501 (Codex round 3): composite aggregate slots (ArithmeticKey
        # / ScalarCallKey of aggregates) carry first/last AggregateKey
        # operands that are not separately slotted but DO need their time
        # columns to contribute ``_first_rn`` / ``_last_rn{suffix}`` columns
        # in the ranked subquery — otherwise the composite render's
        # ``MAX(CASE WHEN _last_rn{suffix} = 1 ...)`` references a column
        # the subquery never projects. Walk every composite-aggregate slot
        # in base_render_order for first/last AggregateKey leaves and
        # synthesise specs for them (NOT projected as columns — they are
        # inlined inside the composite render). Keyed by the AggregateKey
        # itself so two composites sharing the same operand dedupe.
        composite_synth_by_key: Dict[Any, "AggRenderSpec"] = {}
        composite_resolved_kwargs: Dict[Any, Dict[str, ResolvedAggKwarg]] = {}
        for sid in base_render_order:
            slot = slots_by_id[sid]
            if slot.phase != Phase.AGGREGATE:
                continue
            if isinstance(slot.key, AggregateKey):
                continue  # handled by synth_by_sid above
            for agg_leaf in _iter_first_last_leaves(slot.key):
                if agg_leaf in composite_synth_by_key:
                    continue
                composite_synth_by_key[agg_leaf] = (
                    self._build_agg_render_spec_from_planned(
                        slot=slot,
                        key=agg_leaf,
                        source_model=source_model,
                        source_relation=source_relation,
                        # Per-leaf alias must be distinct so the filtered
                        # rn-map lookup (keyed by alias) hits the right
                        # column when multiple composite operands share
                        # a Column.filter.
                        full_alias=(
                            f"{source_relation}._composite_op_"
                            f"{len(composite_synth_by_key)}"
                        ),
                        bundle=bundle,
                    )
                )
                leaf_kw = self._resolve_agg_kwargs_for_key(
                    key=agg_leaf, source_model=source_model,
                    source_relation=source_relation, bundle=bundle,
                )
                if leaf_kw:
                    composite_resolved_kwargs[agg_leaf] = leaf_kw

        # DEV-1709 / DEV-1531 — Law 2 in the ranked subquery. The subquery
        # re-exports only ``source_relation.*`` + rank / ``_td`` / ``_dim``
        # columns, so ANY crossing expression the OUTER SELECT consumes —
        # an aggregate SOURCE sql or a column-ref KWARG value — must be
        # materialised as a ``_val_<n>`` projection inside the subquery and
        # the outer consumer rewritten to the bare alias. Applies to
        # first/last AND regular aggregates alike (DEV-1702-B1). After the
        # widened Law-3 trigger, crossing inputs reach this path only
        # inside a host-rooted isolation CTE's sub-render (where inline
        # joins are legal but the ranked-scope boundary still applies) or
        # under ``disable_host_rooted_isolation``.
        #
        # The materialised projection is the RESOLVED value — qualified,
        # with the ``Column.type`` inner CAST for non-bare expressions
        # (``_resolve_value_sql``'s rule) — so the outer aggregate consumes
        # exactly the value the inline path would have aggregated
        # (``SUM(CAST(x * 2 AS INT))`` semantics preserved). Dedupe is by
        # that resolved text: same sql + different type differ by the CAST
        # and never collapse onto one ``_val``; bare refs are type-agnostic
        # (no CAST) and sharing IS correct.
        allocator = self._gen_allocator or self._new_allocator()
        self._reserve_model_column_names(allocator, source_model)
        value_alias_by_sql: Dict[str, str] = {}

        def _materialize_if_crossing(
            spec: "AggRenderSpec",
        ) -> Optional[str]:
            if not spec.sql:
                return None
            value_expr = self._resolve_sql(
                sql=spec.sql, name=spec.name,
                model_name=spec.model_name, type=spec.column_type,
            )
            resolved_key = value_expr.sql(dialect=self.dialect)
            if resolved_key in value_alias_by_sql:
                return value_alias_by_sql[resolved_key]
            if not self._joined_paths_in_sql(
                sql_expr=value_expr, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
            ):
                return None
            val_alias = allocator.allocate_val()
            extra_projections.append((val_alias, value_expr))
            value_alias_by_sql[resolved_key] = val_alias
            return val_alias

        def _materialize_spec_kwargs(
            kw: "Optional[Dict[str, ResolvedAggKwarg]]",
        ) -> "Optional[Dict[str, ResolvedAggKwarg]]":
            """Rewrite crossing ``kind="expr"`` kwarg values to their
            materialised ``_val`` aliases; local values pass through."""
            if not kw:
                return kw
            new_kw: Dict[str, ResolvedAggKwarg] = {}
            for name, rk in kw.items():
                if rk.kind == "expr" and self._joined_paths_in_sql(
                    sql_expr=rk.value, source_relation=source_relation,
                    source_model=source_model, bundle=bundle,
                ):
                    kw_sql = rk.value.sql(dialect=self.dialect)
                    val_alias = value_alias_by_sql.get(kw_sql)
                    if val_alias is None:
                        val_alias = allocator.allocate_val()
                        extra_projections.append((val_alias, rk.value))
                        value_alias_by_sql[kw_sql] = val_alias
                    new_kw[name] = ResolvedAggKwarg(
                        kind="expr",
                        value=exp.column(val_alias, table=source_relation),
                    )
                else:
                    new_kw[name] = rk
            return new_kw

        outer_synth_by_sid: Dict[str, "AggRenderSpec"] = {}
        for sid, spec in synth_by_sid.items():
            updates: Dict[str, Any] = {}
            src_alias = _materialize_if_crossing(spec)
            if src_alias is not None:
                updates["sql"] = src_alias
            new_kw = _materialize_spec_kwargs(spec.agg_kwargs)
            if new_kw is not spec.agg_kwargs:
                updates["agg_kwargs"] = new_kw
            outer_synth_by_sid[sid] = (
                spec.model_copy(update=updates) if updates else spec
            )
        for leaf_key, spec in composite_synth_by_key.items():
            # Composite leaves re-synthesise inside the pass-2 composite
            # render; registering their crossing sql here lets that render
            # swap in the alias via ``value_alias_by_sql``.
            _materialize_if_crossing(spec)
            leaf_kw = composite_resolved_kwargs.get(leaf_key)
            new_leaf_kw = _materialize_spec_kwargs(leaf_kw)
            if new_leaf_kw is not leaf_kw and new_leaf_kw is not None:
                composite_resolved_kwargs[leaf_key] = new_leaf_kw

        # WHERE goes inside the ranked subquery (raw-row filtering before
        # ranking). HAVING is recomputed and applied by the caller.
        # ``skip_filter_ids`` carries the cross-model-routed filter ids so
        # filters applied inside a per-plan ``_cm_*`` CTE don't double-apply
        # inside the host ranked subquery (Codex round 5).
        where_clause, _having = self._build_where_having_from_planned(
            planned_query=planned_query,
            source_relation=source_relation,
            source_model=source_model,
            bundle=bundle,
            skip_filter_ids=skip_filter_ids,
        )

        (
            ranked_from,
            rn_suffix_map,
            filtered_rn_map,
            filtered_match_map,
        ) = self._build_ranked_subquery_from_planned(
            source_relation=source_relation,
            default_time_col_sql=default_time_col_sql,
            partition_exprs=partition_exprs,
            extra_projections=extra_projections,
            # Project AggregateKey synth_specs (single-key slot
            # aggregates) PLUS composite-operand first/last synth specs
            # so their distinct time columns each contribute an rn
            # column. The composite operands aren't projected as base
            # SELECT columns — they're inlined inside the composite
            # render in pass 2 — but their time columns must still
            # participate in the ranked-subquery rn-column set.
            synth_specs=(
                list(synth_by_sid.values())
                + list(composite_synth_by_key.values())
            ),
            from_clause=from_clause,
            base_joins=base_joins,
            where_clause=where_clause,
        )

        # Pass 2: outer SELECT columns + GROUP BY, in render order.
        # DEV-1501: cycle through each sid's ``full_aliases_by_sid`` list
        # so a C13 slot (one key, N declared names) projects once per
        # alias rather than re-emitting the same alias N times.
        select_columns: List[exp.Expression] = []
        group_by_keys: Dict[str, exp.Expression] = {}
        aliases_by_slot_id: Dict[str, List[str]] = {}
        has_aggregation = False
        visit_idx: Dict[str, int] = {}

        for sid in base_render_order:
            slot = slots_by_id[sid]
            aliases_for_sid = full_aliases_by_sid[sid]
            idx = visit_idx.get(sid, 0)
            full_alias = (
                aliases_for_sid[idx]
                if idx < len(aliases_for_sid)
                else aliases_for_sid[-1]
            )
            visit_idx[sid] = idx + 1
            if slot.phase == Phase.ROW:
                ref = outer_ref_by_sid[sid]
                select_columns.append(ref.copy().as_(full_alias))
                group_by_keys[sid] = ref.copy()
                aliases_by_slot_id.setdefault(sid, []).append(full_alias)
            elif slot.phase == Phase.AGGREGATE:
                if sid in synth_by_sid:
                    # DEV-1709: the OUTER aggregate consumes the Law-2
                    # rewritten spec (crossing source / kwarg expressions
                    # swapped for their ``_val`` aliases); the ranked
                    # subquery consumed the originals.
                    agg_expr, is_agg = self._build_agg(
                        outer_synth_by_sid[sid],
                        rn_suffix_map=rn_suffix_map,
                        default_time_col=default_time_col_sql,
                        filtered_rn_map=filtered_rn_map,
                        filtered_match_map=filtered_match_map,
                    )
                else:
                    # Composite aggregate (no single ``AggregateKey``).
                    # DEV-1501 (Codex round 3 + 6): thread rn state so a
                    # composite expression containing first/last operands
                    # binds each operand to its own ``_first_rn`` /
                    # ``_last_rn{suffix}`` column instead of bare
                    # ``_last_rn``; AND pass per-leaf alias map so a
                    # FILTERED first/last operand's synth matches the
                    # alias the ranked subquery used to key
                    # ``filtered_rn_map`` / ``filtered_match_map``
                    # (otherwise the lookup misses and the operand falls
                    # back to bare ``_last_rn`` + raw filter_sql).
                    composite_alias_by_key = {
                        agg_key: spec.alias
                        for agg_key, spec in composite_synth_by_key.items()
                    }
                    # DEV-1709 (closes the DEV-1527/DEV-1476 first/last kwarg
                    # deferral): thread the per-leaf resolved kwargs (crossing
                    # values already Law-2-rewritten to their ``_val`` aliases)
                    # and the source-value alias map so composite leaves bind
                    # to the ranked subquery's projections instead of leaking
                    # crossing refs into the outer scope.
                    agg_expr, is_agg = self._render_aggregate_composite_expr(
                        key=slot.key, slot=slot, source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
                        rn_suffix_map=rn_suffix_map,
                        default_time_col=default_time_col_sql,
                        filtered_rn_map=filtered_rn_map,
                        filtered_match_map=filtered_match_map,
                        composite_alias_by_key=composite_alias_by_key,
                        resolved_agg_kwargs=composite_resolved_kwargs,
                        value_alias_by_sql=value_alias_by_sql,
                    )
                if is_agg:
                    agg_expr = _wrap_cast_for_type(agg_expr, slot.type)
                    has_aggregation = True
                select_columns.append(agg_expr.copy().as_(full_alias))
                aliases_by_slot_id.setdefault(sid, []).append(full_alias)

        base_select = exp.Select()
        for col in select_columns:
            base_select = base_select.select(col)
        base_select = base_select.from_(ranked_from)
        # DEV-1501: surface the per-time-column rn maps + default time
        # column so the outer HAVING render can resolve hidden first/last
        # aggregate references to the same ``_first_rn`` / ``_last_rn
        # {suffix}`` columns the base SELECT projects.
        first_last_state = FirstLastRenderState(
            rn_suffix_map=dict(rn_suffix_map),
            default_time_col_sql=default_time_col_sql,
            filtered_rn_map=dict(filtered_rn_map),
            filtered_match_map=dict(filtered_match_map),
            # DEV-1709: HAVING re-synths of a crossing-source aggregate bind
            # to the materialised alias instead of the raw crossing ref.
            value_alias_by_sql=dict(value_alias_by_sql),
        )
        return (
            base_select, aliases_by_slot_id, has_aggregation,
            group_by_keys, True, first_last_state,
        )

    def _render_aggregate_composite_expr(  # NOSONAR(S3776) — sequential isinstance dispatch over ValueKey union (AggregateKey / ArithmeticKey / ScalarCallKey / LiteralKey) with rn-state + composite-alias-by-key threading. Each branch carries the per-type recursion contract; extracting helpers would scatter the rn-state forwarding chain.
        self,
        *,
        key,
        slot,
        source_model,
        source_relation: str,
        bundle=None,
        rn_suffix_map: Optional[Dict[str, str]] = None,
        default_time_col: Optional[str] = None,
        filtered_rn_map: Optional[Dict[str, str]] = None,
        filtered_match_map: Optional[Dict[str, str]] = None,
        composite_alias_by_key: Optional[Dict[Any, str]] = None,
        resolved_agg_kwargs: "Optional[Dict[Any, Dict[str, ResolvedAggKwarg]]]" = None,
        value_alias_by_sql: Optional[Dict[str, str]] = None,
    ) -> "tuple[exp.Expression, bool]":
        """Render an AGGREGATE-phase composite key (``ArithmeticKey`` /
        ``ScalarCallKey`` of aggregates, e.g. ``expensenet:avg +
        benchmarkexp:avg``) to one inline sqlglot expr.

        Operand ``AggregateKey``s render inline via the same synth +
        ``_build_agg`` path the single-aggregate branch uses (no per-operand
        cast — the caller casts the composite once). Returns ``(expr,
        contains_aggregate)``. Cross-model operand aggregates (non-empty
        ``source.path``) are not yet handled here — they need CTE routing.

        DEV-1527 (composite local half): ``resolved_agg_kwargs`` is the host
        scope's per-``AggregateKey`` column-ref kwarg map (``weight=<derived>`` /
        ``other=<derived>``); each operand leaf looks up its own entry so a
        crossing derived kwarg embeds its expanded, join-anchored expression
        instead of collapsing to a bare (non-existent) name. Threaded only from
        the non-first/last base path; the first/last ranked-subquery caller passes
        ``None`` (that path's derived-kwarg support is deferred — see DEV-1476).

        DEV-1501 (Codex round 3): when the host base is built via the
        first/last ranked-subquery path, the caller threads the rn maps
        here so a composite expression like ``last(amount, created_at) +
        last(amount, updated_at)`` renders each operand with its OWN
        ``_last_rn{suffix}`` column instead of collapsing to bare
        ``_last_rn``.
        """
        from decimal import Decimal

        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            LiteralKey,
            ScalarCallKey,
        )

        if isinstance(key, AggregateKey):
            if getattr(key.source, "path", ()):
                raise NotImplementedError(
                    "DEV-1450: cross-model aggregate operand inside an "
                    "AGGREGATE-phase composite is not yet supported; factor it "
                    "into a multi-stage source_queries model."
                )
            # DEV-1501 (Codex round 6): for FILTERED composite operands,
            # the ranked subquery's ``filtered_rn_map`` /
            # ``filtered_match_map`` were keyed by the per-leaf alias
            # ``_build_first_last_base_select`` minted at synth time
            # (e.g. ``orders._composite_op_0``). Rebuilding the synth
            # with the fixed placeholder ``__op__`` would miss those
            # lookups and fall back to bare ``_last_rn`` + raw
            # ``filter_sql``. Use the per-leaf alias when supplied.
            op_alias = (
                composite_alias_by_key.get(key)
                if composite_alias_by_key is not None
                else None
            ) or "__op__"
            synth = self._build_agg_render_spec_from_planned(
                slot=slot, key=key, source_model=source_model,
                source_relation=source_relation, full_alias=op_alias,
                bundle=bundle,
                resolved_agg_kwargs=(resolved_agg_kwargs or {}).get(key),
            )
            # DEV-1709 Law 2: inside the first/last ranked path, a crossing
            # composite-leaf SOURCE was materialised as a ``_val_<n>``
            # projection in the ranked subquery — rebind the re-synthesised
            # leaf to that alias (keyed by the RESOLVED value text, so
            # same-sql-different-type leaves bind to their own casts) so
            # the outer composite never references the crossing expression
            # out of scope.
            if value_alias_by_sql and synth.sql is not None:
                resolved_key = self._resolve_value_sql(synth)
                if resolved_key in value_alias_by_sql:
                    synth = synth.model_copy(
                        update={"sql": value_alias_by_sql[resolved_key]},
                    )
            agg_expr, is_agg = self._build_agg(
                synth,
                rn_suffix_map=rn_suffix_map,
                default_time_col=default_time_col,
                filtered_rn_map=filtered_rn_map,
                filtered_match_map=filtered_match_map,
            )
            return agg_expr, is_agg
        if isinstance(key, ArithmeticKey):
            operands = []
            any_agg = False
            for o in key.operands:
                e, a = self._render_aggregate_composite_expr(
                    key=o, slot=slot, source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                    rn_suffix_map=rn_suffix_map,
                    default_time_col=default_time_col,
                    filtered_rn_map=filtered_rn_map,
                    filtered_match_map=filtered_match_map,
                    composite_alias_by_key=composite_alias_by_key,
                    resolved_agg_kwargs=resolved_agg_kwargs,
                    value_alias_by_sql=value_alias_by_sql,
                )
                operands.append(e)
                any_agg = any_agg or a
            return self._compose_arithmetic_op(op=key.op, operands=operands), any_agg
        if isinstance(key, ScalarCallKey):
            args = []
            any_agg = False
            for a in key.args:
                # DEV-1733: dispatch on the KEY BASE, not a hand-listed subset.
                # The trailing ``else`` below stringifies whatever it does not
                # recognise, so a row-column argument
                # (``coalesce(revenue:sum, quantity)``) used to render as the
                # SQL string literal ``'path=() leaf=''quantity'''`` — valid
                # SQL, silently wrong results. Routing every ValueKey through
                # the recursive renderer makes an unsupported operand hit the
                # same terminal NotImplementedError the arithmetic path raises,
                # and leaves the ``else`` for genuine Python literals only.
                if isinstance(a, _FrozenKey):
                    e, ag = self._render_aggregate_composite_expr(
                        key=a, slot=slot, source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
                        rn_suffix_map=rn_suffix_map,
                        default_time_col=default_time_col,
                        filtered_rn_map=filtered_rn_map,
                        filtered_match_map=filtered_match_map,
                        composite_alias_by_key=composite_alias_by_key,
                        resolved_agg_kwargs=resolved_agg_kwargs,
                        value_alias_by_sql=value_alias_by_sql,
                    )
                    args.append(e)
                    any_agg = any_agg or ag
                elif a is None:
                    args.append(exp.Null())
                elif isinstance(a, bool):
                    args.append(exp.true() if a else exp.false())
                elif isinstance(a, (int, float, Decimal)):
                    args.append(exp.Literal.number(str(a)))
                else:
                    args.append(exp.Literal.string(str(a)))
            return render_scalar_call(
                name=key.name, args=args, dialect=self._dialect,
            ), any_agg
        if isinstance(key, LiteralKey):
            v = key.value
            if v is None:
                return exp.Null(), False
            if isinstance(v, bool):
                return (exp.true() if v else exp.false()), False
            if isinstance(v, (int, float, Decimal)):
                return exp.Literal.number(str(v)), False
            return exp.Literal.string(str(v)), False
        raise NotImplementedError(
            f"DEV-1450: AGGREGATE-phase composite operand "
            f"{type(key).__name__} not supported."
        )

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
    ) -> Tuple[str, List[str]]:
        """Render one ``_wm_`` duration-windowed-measure CTE (DEV-1714 Stage 10).

        The CTE is host-rooted: ``FROM _base LEFT JOIN (<_src>) AS _src`` where
        ``_src`` self-selects the host rows (dims → ``_w_dim_<n>``, other time
        dims date-trunc'd → ``_w_td_<n>``, the raw window time column →
        ``_w_time``, the value → ``_w_value``), and the join predicate pairs the
        grain equalities with the trailing ``INTERVAL`` range
        (``_src._w_time >= bucket_end - window`` / ``< bucket_end``). The result
        is grouped at the host grain and LEFT-JOINed back to ``_base`` by the
        caller. Returns ``(cte_sql, grain_aliases)``.
        """
        from slayer.core.keys import AggregateKey

        key = agg_slot.key
        assert isinstance(key, AggregateKey)

        allocator = self._gen_allocator or self._new_allocator()
        src_scope = ScopeFrame(
            scope_id=allocator.next_scope_id(source_relation),
            root_model=source_model,
            root_relation=source_relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=allocator,
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
        join_eqs: List[exp.Expression] = []
        grain_aliases: List[str] = []

        # Query dimensions → ``_w_dim_<n>`` (Law-1 resolve registers crossed
        # joins into ``src_scope``).
        for idx, sid in enumerate(plan.dimension_slot_ids):
            dslot = slots_by_id.get(sid)
            base_alias = _alias_of(sid)
            expr = src_scope.resolve(dslot.key)
            src_cols.append(expr.as_(f"_w_dim_{idx}"))
            join_eqs.append(self._dialect.build_null_safe_eq(
                _src_col(f"_w_dim_{idx}"), _base_col(base_alias),
            ))
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
            join_eqs.append(self._dialect.build_null_safe_eq(
                _src_col(f"_w_td_{idx}"), _base_col(base_alias),
            ))
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

        # ``_src`` FROM + joins from the scope's discovered paths.
        from_expr, src_joins = self._build_from_and_joins(
            source_model=source_model, source_relation=source_relation,
            joined_paths=src_scope.join_paths.as_list(), bundle=bundle,
        )
        src_select = exp.Select().select(*src_cols).from_(from_expr)
        for join_expr, on_expr, join_type in src_joins:
            src_select = src_select.join(
                join_expr, on=on_expr, join_type=join_type,
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
        on_range = exp.and_(
            *join_eqs,
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
            agg_cls(this=_src_col("_w_value")), agg_slot.type,
        )

        outer = exp.Select()
        for ga in grain_aliases:
            outer = outer.select(_base_col(ga))
        outer = outer.select(agg_expr.as_(exp.to_identifier(full_agg_alias, quoted=True)))
        outer = outer.from_(exp.Table(this=exp.to_identifier("_base")))
        outer = outer.join(src_subq, on=on_range, join_type="LEFT")
        for ga in grain_aliases:
            outer = outer.group_by(_base_col(ga))

        # Returned as AST: the caller assembles the WITH chain structurally.
        # Rendering here and re-parsing later would re-introduce the very
        # corruption B2 removed — a dotted public alias round-trips through
        # text as a multi-part reference on BigQuery.
        return outer, grain_aliases

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

        Transform layers + cross-model plans together are out of this
        slice — the renderer rejects with a stage marker so the failure
        mode is loud. Most acceptance / parity tests don't exercise that
        combination.
        """
        from slayer.core.keys import AggregateKey, Phase
        from slayer.engine.binding import walk_value_keys

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

        # DEV-1503 / DEV-1745 (P-D) — the outer combined-SELECT WHERE wrapper is
        # routed by the PLANNER (``_plan_outer_where_filters``), which knows
        # which aggregates were isolated into a CTE with its own root. The
        # generator consumes that decision verbatim: re-walking
        # ``filters_by_phase`` here to rediscover it would be routing policy
        # chosen during emission, and the two could disagree.
        slot_by_key = {s.key: s for s in slots_by_id.values()}
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
        from slayer.core.keys import (
            ArithmeticKey as _ArithKey,
            ScalarCallKey as _ScalarKey,
        )
        composite_kinds = (_ArithKey, _ScalarKey)
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
        ):
            if slot.id not in composite_candidate_ids:
                continue
            if not isinstance(slot.key, composite_kinds):
                continue
            for k in walk_value_keys(slot.key):
                if isinstance(k, AggregateKey):
                    s = slot_by_key.get(k)
                    # DEV-1733: a WINDOWED operand routes the composite outward
                    # for the same reason a cross-model one does — the value
                    # lives in a ``_wm_`` CTE joined back to ``_base``, so
                    # rendering the composite inside ``_base`` would silently
                    # substitute a PLAIN aggregate for the rolling one.
                    if s is not None and (
                        s.id in cma_slot_ids or s.id in windowed_slot_ids
                    ):
                        outer_composite_slot_ids.add(slot.id)
                        break
        base_projection = [
            sid for sid in planned_query.projection
            if sid not in cma_slot_ids
            and sid not in outer_composite_slot_ids
            and sid not in windowed_slot_ids
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
                sid in cma_slot_ids
                or sid in outer_composite_slot_ids
                or sid in windowed_slot_ids
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
                if sid in cma_slot_ids or sid in seen_base_ids:
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
                    if (
                        dep.id in cma_slot_ids
                        or dep.id in windowed_slot_ids
                        or dep.id in seen_base_ids
                    ):
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
                placeholder_scope = ScopeFrame(
                    scope_id=placeholder_allocator.next_scope_id(source_relation),
                    root_model=source_model,
                    root_relation=source_relation,
                    bundle=bundle,
                    dialect=self._dialect,
                    allocator=placeholder_allocator,
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
                for join_expr, on_expr, join_type in placeholder_joins:
                    base_select = base_select.join(
                        join_expr, on=on_expr, join_type=join_type,
                    )
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
            # Filters routed to any CTE (WHERE or HAVING) must NOT
            # double-apply at the host base — nor pull their joins into
            # ``_base`` (the predicate runs in the ``_cm_*`` CTE).
            # ``applied_filter_ids`` is the audit union of where + having
            # on each plan.
            #
            # DEV-1503: outer-WHERE filters (AGGREGATE-phase host filters
            # referencing a filtered-local isolated aggregate) also go in
            # here so ``_base`` does not double-apply them as HAVING on
            # the bare local aggregate expression (which would reference
            # an aggregate that no longer lives in ``_base``).
            routed_ids: Set[str] = set(outer_where_filter_ids)
            for plan in planned_query.cross_model_aggregate_plans:
                routed_ids.update(plan.where_filter_ids)
                routed_ids.update(plan.having_filter_ids)
            (
                base_select,
                aliases_by_slot_id,
                base_has_agg,
                base_group_by,
                _base_where_consumed,
                base_first_last_state,
            ) = self._build_base_select_for_planned(
                planned_query=planned_query,
                bundle=bundle,
                source_model=source_model,
                source_relation=source_relation,
                base_render_order=base_render_order,
                slots_by_id=slots_by_id,
                skip_cross_model_aggs=True,
                skip_filter_ids=routed_ids,
            )

            base_where, base_having = self._build_where_having_from_planned(
                planned_query=planned_query,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                skip_filter_ids=routed_ids,
                first_last_state=base_first_last_state,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            # CodeRabbit thread: the first/last ranked-subquery path
            # consumes WHERE inside the ranked subquery and returns
            # ``where_consumed=True``. Re-applying ``base_where`` to the
            # outer SELECT here would double-filter (and change
            # first/last semantics by filtering AFTER ranking) and can
            # dangle joined-column aliases on the outer SELECT scope.
            if base_where is not None and not _base_where_consumed:
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
        cm_ctes: List[Tuple[str, str]] = []
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
                "_cm_", canonical_alias, allocator=cm_allocator,
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
        wm_ctes: List[Tuple[str, str]] = []
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
                "_wm_", full_agg_alias, allocator=wm_allocator,
            )
            cte_query, grain_aliases = self._render_window_measure_cte_from_planned(
                plan=plan, agg_slot=agg_slot, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                planned_query=planned_query, slots_by_id=slots_by_id,
                aliases_by_slot_id=aliases_by_slot_id, full_agg_alias=full_agg_alias,
            )
            wm_ctes.append((cte_name, cte_query))
            wm_cte_name_for_plan[plan.aggregate_slot_id] = cte_name
            wm_agg_col_for_plan[plan.aggregate_slot_id] = full_agg_alias
            wm_joinback_pairs_for_plan[plan.aggregate_slot_id] = [
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
        for sid in host_combined_ids:
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

            def _render_outer_composite(cslot) -> exp.Expression:
                rendered = self._render_filter_for_outer_wrapper(
                    key=cslot.key,
                    slot_by_key=slot_by_key,
                    cross_model_agg_slot_to_cm=outer_composite_cm_map,
                    aliases_by_slot_id=aliases_by_slot_id,
                )
                if cslot.type is not None:
                    rendered = _wrap_cast_for_type(rendered, cslot.type)
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
                continue
            idx = consumed.get(sid, 0)
            if idx < len(exprs):
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
        # not ask for. Fail instead: a silent extra column is the harder bug.
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
        ]
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
            for fp in outer_where_filters:
                rendered = self._render_filter_for_outer_wrapper(
                    key=fp.expression.value_key,
                    slot_by_key=slot_by_key,
                    cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                    aliases_by_slot_id=aliases_by_slot_id,
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
                rendered = self._render_filter_for_outer_wrapper(
                    key=fp.expression.value_key,
                    slot_by_key=slot_by_key,
                    cross_model_agg_slot_to_cm=wm_slot_to_cte,
                    aliases_by_slot_id=aliases_by_slot_id,
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
            # The transform chain is still string-assembled (it adopts the
            # shared assembler in PR 4, with the local chain), so render its
            # prelude CTEs here rather than threading AST into it.
            return self._render_cross_model_transform_chain(
                prelude_ctes=[
                    ("_base", base_select.sql(dialect=self.dialect, pretty=True)),
                ] + [
                    (name, query.sql(dialect=self.dialect, pretty=True))
                    for name, query in cm_ctes
                ],
                combined_select_sql=combined_select.sql(
                    dialect=self.dialect, pretty=True,
                ),
                planned_query=planned_query,
                slots_by_id=slots_by_id,
                combined_aliases_by_slot_id=combined_aliases_by_slot_id,
                source_relation=source_relation,
            )

        # Assemble the WITH chain (§5.6). Dependencies are DECLARED, not
        # discovered by scanning the rendered statement: ``_wm_`` CTEs select
        # FROM ``_base``, the cross-model CTEs are rooted at their own targets
        # and depend on nothing. The assembler emits a stable topological order
        # with declaration order as the tiebreak.
        cte_entries = [CteEntry(name="_base", query=base_select)]
        cte_entries += [
            CteEntry(name=name, query=query) for name, query in cm_ctes
        ]
        cte_entries += [
            CteEntry(name=name, query=query, depends_on=["_base"])
            for name, query in wm_ctes
        ]
        combined_statement = assemble_with_chain(
            entries=cte_entries, final=combined_select,
        )

        # ORDER BY / LIMIT / OFFSET: emitted at the combined SELECT
        # level. ORDER BY columns must be qualified — ``_base`` columns
        # use ``_base."..."``, cross-model columns use the bare alias
        # (only present on one side).
        # DEV-1712 / DEV-1495 bug 2: hidden (order-only) cross-model aggregates
        # are trimmed from the projection above, so their ORDER BY term must be
        # CTE-qualified (``_cm_*."<agg_col_alias>"``) rather than the bare
        # combined-SELECT alias.
        hidden_cte_order_refs: Dict[str, exp.Expression] = {}
        for plan in planned_query.cross_model_aggregate_plans:
            # Only CMAs actually trimmed from the projection (hidden + no
            # transform chain) need the CTE-qualified ORDER BY reference.
            if not (plan.hidden and not planned_query.transform_layers):
                continue
            _agg_col = agg_col_alias_for_plan[plan.aggregate_slot_id]
            _cte = cm_cte_name_for_plan[plan.aggregate_slot_id]
            hidden_cte_order_refs[plan.aggregate_slot_id] = (
                grain_alias_column(alias=_agg_col, table=_cte)
            )
        # DEV-1733: same treatment for a hidden (order-only) WINDOWED aggregate
        # trimmed from the combined projection above — reference its ``_wm_``
        # CTE column rather than a bare alias the SELECT no longer emits.
        for plan in planned_query.windowed_aggregate_plans:
            if not (plan.hidden and not planned_query.transform_layers):
                continue
            hidden_cte_order_refs[plan.aggregate_slot_id] = grain_alias_column(
                alias=wm_agg_col_for_plan[plan.aggregate_slot_id],
                table=wm_cte_name_for_plan[plan.aggregate_slot_id],
            )
        order_terms = self._build_combined_order_by_sql(
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            cma_slot_ids=cma_slot_ids,
            cm_alias_for_plan=canonical_alias_for_plan,
            # DEV-1714: windowed slots are referenced bare in the combined ORDER
            # BY — they surface as a projected combined-SELECT column (from their
            # ``_wm_`` CTE), so a ``_base.`` qualifier would dangle.
            bare_order_slot_ids=set(order_only_local_ids) | windowed_slot_ids,
            outer_composite_aliases=outer_composite_order_alias_by_sid,
            outer_composite_expressions=outer_composite_order_expressions,
            hidden_cte_order_refs=hidden_cte_order_refs,
        )
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
        # list in declared order, so the trim is normally a no-op. Skip
        # the trim machinery here because the legacy path goes through
        # an EnrichedQuery-driven ``_apply_outer_projection_trim`` that
        # we don't have on the new side. Future slices may re-enable.
        return combined_statement.sql(dialect=self.dialect, pretty=True)

    def _render_cross_model_transform_chain(  # NOSONAR(S3776) — pre-existing complexity in the window-layer chain; this PR only threaded the CTE-name allocator through it, which re-attributed the function as new code. The chain is rebuilt as sqlglot AST in the scope-assembly PR, where the layering is what gets simplified.
        self,
        *,
        prelude_ctes: List[Tuple[str, str]],
        combined_select_sql: str,
        planned_query,
        slots_by_id: Dict[str, Any],
        combined_aliases_by_slot_id: Dict[str, List[str]],
        source_relation: str,
    ) -> str:
        """Render window-transform layers over a cross-model combined result.

        DEV-1450 stage 7b.15e (C2). The combined cross-model SELECT becomes the
        ``base`` CTE; window step CTEs (``cumsum`` / ``lag`` / ``lead`` /
        ``rank`` …) are layered above it exactly like the local transform path
        in ``generate_from_planned``, then an outer wrap projects the public
        slots in user order and applies ORDER BY / LIMIT / OFFSET.

        ``time_shift`` / ``consecutive_periods`` over a cross-model aggregate
        re-aggregate the *source* and are out of slice scope — they raise.
        """
        for layer in planned_query.transform_layers:
            if layer.op in ("time_shift", "consecutive_periods"):
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.15e: self-join transform op "
                    f"{layer.op!r} is not yet rendered in a query that also has "
                    f"a cross-model aggregate (window transforms such as cumsum "
                    f"/ lag / lead / rank are). Factor the temporal transform "
                    f"(or change / change_pct, which desugar to time_shift) "
                    f"into an earlier stage.",
                )

        ctes: List[Tuple[str, str]] = list(prelude_ctes) + [
            ("base", combined_select_sql),
        ]
        # P-F: this chain previously minted ``step<n>`` names with a
        # bare f-string and held no allocator at all, so nothing connected its
        # names to the ``_cm_*`` CTEs already in ``prelude_ctes`` or to the
        # literal ``base``. Take the generation-scoped allocator (the SAME
        # instance that minted the ``_cm_`` names, so its used-set already
        # covers them) and reserve the inherited literals before allocating.
        cte_allocator = self._gen_allocator or self._new_allocator()
        cte_allocator.reserve(*(name for name, _ in ctes))
        aliases_by_slot_id: Dict[str, List[str]] = {
            sid: list(a) for sid, a in combined_aliases_by_slot_id.items()
        }
        slot_id_by_key: Dict[Any, str] = {
            s.key: s.id for s in slots_by_id.values()
        }
        available_alias_by_slot_id: Dict[str, str] = {
            sid: a[0] for sid, a in aliases_by_slot_id.items() if a
        }

        # Window-transform Kahn batches (one step CTE per ready batch).
        pending_layers = list(planned_query.transform_layers)
        step_num = 0
        while pending_layers:
            ready: list = []
            not_ready: list = []
            for layer in pending_layers:
                if self._transform_layer_deps_ready(
                    layer=layer,
                    slots_by_id=slots_by_id,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                ):
                    ready.append(layer)
                else:
                    not_ready.append(layer)
            if not ready:
                pending_ops = [layer.op for layer in pending_layers]
                raise RuntimeError(
                    f"DEV-1450 stage 7b.15e: cross-model transform layer "
                    f"dependencies could not be resolved; pending ops: "
                    f"{pending_ops!r}.",
                )
            step_num += 1
            step_name = cte_allocator.allocate_cte(f"step{step_num}")
            prev_cte = ctes[-1][0]
            carry_aliases_sorted = self._carry_aliases_in_plan_order(
                aliases_by_slot_id,
            )
            step_parts = [self._quote_ident(a) for a in carry_aliases_sorted]
            for layer in ready:
                for slot_id in layer.slot_ids:
                    slot = slots_by_id[slot_id]
                    alias = (
                        slot.public_aliases[0]
                        if slot.public_aliases
                        else slot.declared_name
                    )
                    full_alias = f"{source_relation}.{alias}"
                    window_sql = self._render_window_transform_sql(
                        slot=slot,
                        slots_by_id=slots_by_id,
                        slot_id_by_key=slot_id_by_key,
                        available_alias_by_slot_id=available_alias_by_slot_id,
                        planned_query=planned_query,
                    )
                    if slot.type is not None:
                        window_sql = _wrap_cast_for_type(
                            self._parse(window_sql), slot.type,
                        ).sql(dialect=self.dialect)
                    step_parts.append(f'{window_sql} AS {self._quote_ident(full_alias)}')
                    aliases_by_slot_id.setdefault(slot_id, []).append(full_alias)
                    available_alias_by_slot_id.setdefault(slot_id, full_alias)
            step_sql = (
                "SELECT\n    "
                + _SQL_COL_SEP.join(step_parts)
                + f"\nFROM {prev_cte}"
            )
            ctes.append((step_name, step_sql))
            pending_layers = not_ready

        # Materialise any projected POST-phase ArithmeticKey / ScalarCallKey
        # slot a window layer didn't render (``cumsum(x) + 1``-style combos).
        from slayer.core.keys import (
            ArithmeticKey as _ArithKey,
            ScalarCallKey as _ScalarKey,
            TransformKey as _TKey,
        )
        unmaterialised: list = []
        for cslot in planned_query.combined_expression_slots:
            if isinstance(cslot.key, _TKey):
                continue
            if cslot.id in aliases_by_slot_id:
                continue
            if isinstance(cslot.key, (_ArithKey, _ScalarKey)):
                unmaterialised.append(cslot)
        if unmaterialised:
            step_num += 1
            step_name = cte_allocator.allocate_cte(f"step{step_num}")
            prev_cte = ctes[-1][0]
            carry_aliases_sorted = self._carry_aliases_in_plan_order(
                aliases_by_slot_id,
            )
            step_parts = [self._quote_ident(a) for a in carry_aliases_sorted]
            for cslot in unmaterialised:
                alias = (
                    cslot.public_aliases[0]
                    if cslot.public_aliases
                    else cslot.declared_name
                )
                full_alias = f"{source_relation}.{alias}"
                rendered = self._render_value_key_against_aliases(
                    key=cslot.key,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                )
                expr_sql = rendered.sql(dialect=self.dialect)
                if cslot.type is not None:
                    expr_sql = _wrap_cast_for_type(
                        self._parse(expr_sql), cslot.type,
                    ).sql(dialect=self.dialect)
                step_parts.append(f'{expr_sql} AS {self._quote_ident(full_alias)}')
                aliases_by_slot_id.setdefault(cslot.id, []).append(full_alias)
                available_alias_by_slot_id.setdefault(cslot.id, full_alias)
            step_sql = (
                "SELECT\n    "
                + _SQL_COL_SEP.join(step_parts)
                + f"\nFROM {prev_cte}"
            )
            ctes.append((step_name, step_sql))

        final_cte = ctes[-1][0]
        inner_sorted = self._carry_aliases_in_plan_order(aliases_by_slot_id)
        inner_sql = (
            "SELECT\n    "
            + _SQL_COL_SEP.join(self._quote_ident(a) for a in inner_sorted)
            + f"\nFROM {final_cte}"
        )
        cte_clause = (
            _SQL_WITH
            + ",\n".join(f"{name} AS (\n{sql}\n)" for name, sql in ctes)
        )
        chain_sql = f"{cte_clause}\n{inner_sql}"

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

        public_aliases_user_order: list[str] = []
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
            public_aliases_user_order.append(alias)
        return self._emit_planned_outer_wrap(
            chain_sql=chain_sql,
            public_aliases=public_aliases_user_order,
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            available_alias_by_slot_id=available_alias_by_slot_id,
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
        cte_sql = self.generate_from_planned(sub_plan, bundle=rerooted_bundle)

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
    ) -> Tuple[str, List[str]]:
        """Render one ``_cm_<...>`` CTE body and return its SQL +
        shared-grain alias list (for the outer ``LEFT JOIN ON`` clause).

        The CTE is rooted at the terminal target model (legacy
        rerooted shape). Shared-grain slots whose key path is a prefix
        of the target_path participate as both projection and GROUP BY
        keys; slots with empty path (host-local dims) are excluded
        since the legacy CROSS JOINs in that case.

        Filter routing reads ``plan.where_filter_ids`` /
        ``plan.having_filter_ids`` / ``plan.target_model_filters`` so
        the CTE renders each route without re-classifying.
        """
        from slayer.core.enums import TimeGranularity
        from slayer.core.keys import (
            ColumnKey,
            ColumnSqlKey,
            Phase,
            TimeTruncKey,
        )

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
        # DEV-1728: two GROUP-BY lists — ``cte_group_by`` is the OUTER GROUP BY
        # (an alias ref ``_val_<n>`` for a first/last-materialised crossing
        # grain, the raw expression otherwise); ``cte_partition_exprs`` is the
        # ranked-subquery PARTITION BY, always the RAW expression (valid inside
        # the subquery where the crossed join is bound). They are identical for
        # every non-first/last query and every non-crossing grain.
        cte_group_by: List[exp.Expression] = []
        cte_partition_exprs: List[exp.Expression] = []
        shared_grain_aliases: List[str] = []
        # DEV-1701: join paths crossed by a shared-grain derived TIME dimension's
        # expanded ``Column.sql``. Collected during the loop (which runs before
        # the CTE scope's join set is assembled) and merged into it below.
        shared_grain_join_paths: List[Tuple[str, ...]] = []
        # DEV-1728: first/last grain materialisations (``_val_<n>`` projections
        # to inject INTO the ranked subquery for crossing derived grains). The
        # generation-wide allocator is hoisted here so grain + source ``_val``s
        # share one monotonic sequence. Reserve the target's physical column
        # names (Codex F6): the ranked subquery re-exports ``target.*``, so a
        # minted ``_val_<n>`` must never collide with a real target column of
        # that name — mirrors the host-path reservation in
        # ``_build_first_last_base_select``.
        cte_allocator = self._gen_allocator or self._new_allocator()
        self._reserve_model_column_names(cte_allocator, target_model)
        is_first_or_last = agg_slot.key.agg in ("first", "last")
        # The CTE's own scope, and the scope of the ranked subquery it may
        # select FROM. Both are created here, before the grain loop, because a
        # crossing grain is materialised in the RANKED scope for the CTE scope
        # to consume — Law 2's producer/consumer pair.
        cte_scope = ScopeFrame(
            scope_id=cte_allocator.next_scope_id(target_relation),
            root_model=target_model,
            root_relation=target_relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=cte_allocator,
        )
        ranked_scope = ScopeFrame(
            scope_id=cte_allocator.next_scope_id(target_relation),
            root_model=target_model,
            root_relation=target_relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=cte_allocator,
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
            from slayer.core.keys import ColumnSqlKey as _ColumnSqlKey

            grain_column = key.column if isinstance(key, TimeTruncKey) else key
            if isinstance(grain_column, _ColumnSqlKey):
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
            # DEV-1728 Law 2: for a first/last aggregate the CTE's FROM is a
            # ROW_NUMBER-ranked subquery that re-exports only ``target.*`` + rank
            # columns. A grain whose expression CROSSES a join references a table
            # bound ONLY inside that subquery, so the outer SELECT / GROUP BY
            # cannot name it — materialise it as a ``_val_<n>`` projection inside
            # the subquery, group the outer SELECT on the alias, and keep the RAW
            # expression for PARTITION BY (evaluated where the join is bound). A
            # target-local grain (no crossing) needs no materialisation — it is
            # re-exported by ``target.*``.
            grain_crosses = is_first_or_last and bool(
                self._joined_paths_in_sql(
                    sql_expr=col_expr, source_relation=target_relation,
                    source_model=target_model, bundle=bundle,
                )
            )
            if grain_crosses:
                # Law 2 through the SCOPE (§5.1): the ranked subquery projects
                # the crossing expression and the CTE consumes the alias. The
                # scope owns the dedup table, so a value materialised once for
                # the grain is not materialised again for the aggregate.
                alias_ref = ranked_scope.materialize_for(
                    col_expr.copy(), consumer=cte_scope,
                )
                cte_select_columns.append(alias_ref.copy().as_(host_alias))
                cte_group_by.append(alias_ref.copy())
                cte_partition_exprs.append(col_expr.copy())
            else:
                cte_select_columns.append(col_expr.copy().as_(host_alias))
                cte_group_by.append(col_expr.copy())
                cte_partition_exprs.append(col_expr.copy())
            shared_grain_aliases.append(host_alias)

        # Aggregate column: synthesise an EnrichedMeasure ROOTED at the
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
        # must strip the host prefix in lockstep with the source so
        # ``_resolve_explicit_time_col`` qualifies the time column under the
        # target relation. ``column_filter_key`` rides through unchanged
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

        synth = self._build_agg_render_spec_from_planned(
            slot=local_slot,
            key=local_agg_key,
            source_model=target_model,
            source_relation=target_relation,
            full_alias=full_agg_alias,
            bundle=bundle,
            resolved_agg_kwargs=cte_resolved_kwargs or None,
        )

        # DEV-1476 bug (c): for first/last aggregates the FROM must be a
        # ROW_NUMBER-ranked subquery so the ``MAX(CASE WHEN _last_rn = 1
        # THEN col END)`` expression has a ranking column. The local
        # first/last path (``_build_first_last_base_select``) wraps via
        # ``_build_ranked_subquery_from_planned``; mirror that here for
        # the cross-model CTE.
        #
        # Codex round 2: when no explicit positional time arg was
        # supplied, fall back to the target model's
        # ``default_time_dimension`` (qualified under the target
        # relation). If even that is unset, raise the standard
        # "first/last requires a ranking time column" error rather than
        # silently emitting an agg_expr that references a non-existent
        # ``_first_rn`` / ``_last_rn`` column. (``is_first_or_last`` is computed
        # once above the grain loop from ``agg_slot.key.agg`` — reroot preserves
        # the aggregation name — so the grain materialisation and this branch
        # agree.)
        time_col_sql: Optional[str] = synth.time_column
        if is_first_or_last and time_col_sql is None:
            if target_model.default_time_dimension:
                time_col_sql = (
                    f"{target_relation}.{target_model.default_time_dimension}"
                )
            else:
                raise ValueError(
                    f"first/last aggregation requires a ranking time column "
                    f"(an explicit positional time arg, or the target "
                    f"model's default_time_dimension); none is resolvable "
                    f"for cross-model aggregate on target "
                    f"{target_model_name!r}."
                )
        # WHERE: target-model-filters (qualified bare-identifier refs
        # so ``deleted_at IS NULL`` becomes ``customers.deleted_at IS
        # NULL`` to match the legacy enrichment's filter-column
        # resolution) + host filters routed to WHERE. Computed up-front
        # so the first/last branch can push them INSIDE the ranked
        # subquery — otherwise rows excluded by a filter could still
        # win ``_last_rn = 1`` and yield NULL aggregates.
        # DEV-1494: join paths the CTE's own filters cross — the target measure's
        # ``Column.filter`` and the target-model filters — registered into the
        # CTE scope (Law 1). Each ``_cm_*`` CTE is an isolated per-(target, grain)
        # computation, so adding these joins to ITS FROM affects only this
        # measure (not siblings) — it resolves the filter's refs without the
        # cross-measure cardinality concern DEV-1503 owns. Free-SQL predicates
        # keep the quote-tolerant dual-scan of ``_filter_join_paths`` (raw +
        # inline-expanded — the DEV-1494/dedup contract) while writing into the
        # single ``cte_scope.join_paths`` set.
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

        # DEV-1745 (W2): the aggregation's template FRAGMENTS — string kwargs
        # plus the non-overridden ``AggregationParam.sql`` defaults — substitute
        # verbatim into the CTE's aggregate expression, so the joins they cross
        # belong in this CTE's FROM. The host path has always registered them;
        # this path never did, which is why a default like ``w='regions.weight'``
        # emitted ``SUM(customers.spend * regions.weight) FROM customers`` with
        # no join to regions. Routing the fragments through the door makes the
        # registration a side effect of resolving them, so the two paths cannot
        # drift apart again.
        self._register_fragment_kwarg_joins(
            key=local_agg_key, scope=cte_scope, model=target_model,
        )

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
        ranked_from: Optional[exp.Expression] = None
        cte_value_alias_by_sql: Dict[str, str] = {}
        if is_first_or_last:
            assert time_col_sql is not None  # narrowed by the guard above
            # DEV-1708 Law 2 (DEV-1702 B2, forward variant): the ranked subquery
            # re-exports only ``target.*`` + rank columns. If the first/last
            # SOURCE value crosses a join, the crossing ref must be materialised
            # as a ``_val_<n>`` projection INSIDE the subquery and the outer
            # aggregate rewritten to reference the alias — otherwise the outer
            # ``MAX(CASE WHEN _last_rn = 1 THEN <crossing ref> END)`` references a
            # table bound only inside the subquery (out of scope). The FILTER
            # refs are consumed inside the subquery (the ``_last_rn_fN`` /
            # ``_match_fN`` rank columns) and need no outer alias. A LOCAL source
            # value is already covered by ``target.*`` — no materialisation.
            outer_synth = synth
            # DEV-1728: seed with the crossing-grain ``_val_<n>`` projections
            # collected in the grain loop, so a first/last aggregate grouped by a
            # crossing derived grain materialises that grain INSIDE the subquery
            # too (the outer SELECT / GROUP BY reference the alias).
            extra_projections: List[Tuple[str, exp.Expression]] = [
                (m.alias, m.expr) for m in ranked_scope.materializations
            ]
            if synth.sql:
                # DEV-1709: materialise the RESOLVED value (qualified +
                # ``Column.type`` inner CAST for non-bare expressions) and
                # key the alias map by that resolved text — mirrors the
                # host-path materialisation in
                # ``_build_first_last_base_select`` so typed non-bare
                # sources keep ``MAX(CASE ... THEN CAST(x AS t) END)``
                # semantics inside the CTE too.
                value_expr = self._resolve_sql(
                    sql=synth.sql, name=synth.name,
                    model_name=synth.model_name, type=synth.column_type,
                )
                if self._joined_paths_in_sql(
                    sql_expr=value_expr, source_relation=target_relation,
                    source_model=target_model, bundle=bundle,
                ):
                    # Law 2 through the SCOPE, same as the crossing grain above.
                    # Because both go through ONE dedup table, grouping a
                    # first/last aggregate by the very expression it aggregates
                    # now projects that expression once instead of twice — the
                    # two sites used to keep separate alias maps.
                    val_alias = ranked_scope.materialize_for(
                        value_expr, consumer=cte_scope,
                    ).name
                    extra_projections = [
                        (m.alias, m.expr) for m in ranked_scope.materializations
                    ]
                    outer_synth = synth.model_copy(update={"sql": val_alias})
                    # A HAVING on this same aggregate must reference the alias,
                    # not the raw crossing ref (out of scope in the outer SELECT).
                    cte_value_alias_by_sql[
                        value_expr.sql(dialect=self.dialect)
                    ] = val_alias
            ranked_from, rn_suffix_map, filtered_rn_map, filtered_match_map = (
                self._build_ranked_subquery_from_planned(
                    source_relation=target_relation,
                    default_time_col_sql=time_col_sql,
                    partition_exprs=list(cte_partition_exprs),
                    extra_projections=extra_projections,
                    synth_specs=[synth],
                    from_clause=target_from,
                    base_joins=cte_base_joins,
                    where_clause=combined_where,
                )
            )
            agg_expr, is_agg = self._build_agg(
                outer_synth,
                rn_suffix_map=rn_suffix_map,
                default_time_col=time_col_sql,
                filtered_rn_map=filtered_rn_map,
                filtered_match_map=filtered_match_map,
            )
        else:
            agg_expr, is_agg = self._build_agg(synth)
        if is_agg:
            agg_expr = _wrap_cast_for_type(agg_expr, agg_slot.type)
        cte_select_columns.append(agg_expr.copy().as_(full_agg_alias))

        # Assemble the CTE Select now that every projected column (shared
        # grain + aggregate) is in ``cte_select_columns``.
        cte_select = exp.Select()
        for col in cte_select_columns:
            cte_select = cte_select.select(col)
        if is_first_or_last:
            assert ranked_from is not None
            cte_select = cte_select.from_(ranked_from)
        else:
            cte_select = cte_select.from_(target_from)
            for join_expr, on_expr, join_type in cte_base_joins:
                cte_select = cte_select.join(
                    join_expr, on=on_expr, join_type=join_type,
                )
            if combined_where is not None:
                cte_select = cte_select.where(combined_where)

        if cte_group_by:
            for gb in cte_group_by:
                cte_select = cte_select.group_by(gb)

        # DEV-1501 Group A.3: routed HAVING for cross-model first/last
        # must use the SAME rn-based aggregate the CTE projects. Build a
        # ``FirstLastRenderState`` carrying the rn maps + the single
        # projected aggregate's full alias so HAVING's synth rebuild
        # binds to the right ``_first_rn`` / ``_last_rn{suffix}`` /
        # ``_last_rn_fN`` column (instead of a placeholder alias whose
        # ``filtered_rn_map`` lookup misses and silently degrades to
        # bare ``_last_rn`` + raw ``filter_sql``).
        cm_first_last_state: Optional[FirstLastRenderState] = None
        if is_first_or_last:
            cm_first_last_state = FirstLastRenderState(
                rn_suffix_map=dict(rn_suffix_map),
                default_time_col_sql=time_col_sql,
                filtered_rn_map=dict(filtered_rn_map),
                filtered_match_map=dict(filtered_match_map),
                agg_synth_alias=full_agg_alias,
                value_alias_by_sql=dict(cte_value_alias_by_sql),
            )
        cte_having = self._collect_routed_filters(
            planned_query=planned_query,
            filter_ids=plan.having_filter_ids,
            target_relation=target_relation,
            target_model=target_model,
            bundle=bundle,
            first_last_state=cm_first_last_state,
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
        quote-tolerant dual-scan of ``_filter_join_paths``.
        """
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            ScalarCallKey,
        )

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
        from slayer.core.keys import ColumnKey, ColumnSqlKey

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

    def _collect_routed_filters(
        self,
        *,
        planned_query,
        filter_ids: List[str],
        target_relation: str,
        target_model,
        bundle,
        first_last_state: Optional[FirstLastRenderState] = None,
    ) -> Optional[exp.Expression]:
        """Build a conjunction of bound filter predicates by ID.

        Filters routed into a cross-model CTE bind in the CTE's local
        scope (``customers.status`` resolves to the target's table).
        For row-phase filters whose typed ``value_key`` already encodes
        the join-target columns, ``_render_filter_value_key`` resolves
        each leaf against the target model.

        Returns ``None`` when the requested filter set is empty so the
        caller can skip emitting WHERE / HAVING.
        """
        if not filter_ids:
            return None
        wanted = set(filter_ids)
        parts: List[exp.Expression] = []
        for fp in planned_query.filters_by_phase:
            if fp.id not in wanted:
                continue
            if fp.expression is None:
                continue
            ast = self._render_filter_value_key_in_target_scope(
                value_key=fp.expression.value_key,
                target_relation=target_relation,
                target_model=target_model,
                planned_query=planned_query,
                bundle=bundle,
                first_last_state=first_last_state,
            )
            if ast is not None:
                parts.append(ast)
        if not parts:
            return None
        return exp.and_(*parts) if len(parts) > 1 else parts[0]

    def _render_filter_value_key_in_target_scope(  # NOSONAR(S3776) — sequential isinstance dispatch over the closed ValueKey union with per-type cross-model target-scope rules (joined-column qualification, derived-column expansion, rn-state aware aggregate synth). Each branch carries the per-type cross-model render contract; extracting helpers would scatter the contract.
        self,
        *,
        value_key,
        target_relation: str,
        target_model,
        planned_query,
        bundle,
        first_last_state: Optional[FirstLastRenderState] = None,
    ) -> Optional[exp.Expression]:
        """Render a bound filter's value key as SQL with bare column
        refs qualified against the cross-model CTE's local scope.

        The typed pipeline carries filter ASTs as ``ValueKey``-rooted
        trees (``ArithmeticKey`` / ``AggregateKey`` / ``ColumnKey`` /
        ``ColumnSqlKey`` / scalars). The CTE renderer reuses the legacy
        ``_build_agg`` / column-resolution helpers via a small local
        recursion that binds each leaf to the target model's relation alias.
        """
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            LiteralKey,
            ScalarCallKey,
        )

        if isinstance(value_key, ColumnSqlKey):
            # DEV-1450 #4b: a routed filter on a DERIVED column owned by the
            # CTE target — expand its Column.sql rooted at the target so it
            # emits real SQL instead of falling through to a bogus literal.
            if value_key.model != target_model.name:
                raise NotImplementedError(
                    f"DEV-1450: cross-model filter on derived column "
                    f"{value_key.column_name!r} owned by {value_key.model!r} "
                    f"(not the CTE target {target_model.name!r}) is not yet "
                    f"rendered in the typed pipeline.",
                )
            expanded = self._expand_derived_column_sql(
                source_model=target_model,
                source_relation=target_relation,
                column_name=value_key.column_name,
                bundle=bundle,
            )
            return self._parse(expanded)

        if isinstance(value_key, ColumnKey):
            # Cross-model filter on the joined-target path: the column
            # lives on the target (single-hop) or on an intermediate
            # hop. For 7b.12 we expect target-rooted refs only.
            path = value_key.path
            # ``value_key.path`` is a tuple of hop names ending at the
            # target. The cross-model planner routes filters to the
            # CTE only when the path == target_path (single-hop) or is
            # a prefix (multi-hop). Both forms render against the
            # target's local relation alias by leaf name.
            if path and path[-1] != target_relation:
                # Intermediate hop ref — not yet rendered.
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.12: cross-model filter on an "
                    f"intermediate hop ({path!r}) not yet rendered in "
                    f"the typed pipeline.",
                )
            return exp.Column(
                this=exp.to_identifier(value_key.leaf),
                table=exp.to_identifier(target_relation),
            )
        if isinstance(value_key, LiteralKey):
            return self._literal_key_to_exp(value_key)
        if isinstance(value_key, AggregateKey):
            # HAVING-route: render the aggregate against the target.
            # Reuse the synthesise helper with target_model as scope.
            # DEV-1501 (Codex round 9): the routed AggregateKey carries
            # source / args / kwargs still rooted at the cross-model path
            # (``customers.regions.amount:last(customers.regions.opened_at)``
            # arrives with ``args=(ColumnKey(path=("customers","regions"),
            # leaf="opened_at"),)``). Inside the target CTE scope every ref
            # must qualify under the local relation, not the host-rooted
            # ``__``-path alias — the SAME symmetric reroot the projection
            # path applies (DEV-1707). Without it, the ranked subquery's
            # ``ORDER BY`` qualifies the time column under a non-existent
            # alias inside the CTE.
            cross_model_path = getattr(value_key.source, "path", ())
            local_agg = reroot_aggregate_key(
                value_key, target_path=cross_model_path,
            )
            from slayer.engine.planned import ValueSlot as _Slot
            tmp_slot = _Slot(
                id="_cte_having_tmp",
                key=local_agg,
                declared_name="_having_agg",
                phase=value_key.phase,
                type=None,
            )
            # DEV-1501 Group A.3: when the CTE projects a first/last
            # aggregate, the projected spec's alias is the key for
            # ``filtered_rn_map`` / ``filtered_match_map``. Reusing the
            # SAME alias here lets ``_build_agg``'s lookup hit, binding
            # the HAVING aggregate to the dedicated ``_last_rn_fN`` (and
            # match-flag) instead of bare ``_last_rn`` + raw filter_sql.
            having_full_alias = (
                first_last_state.agg_synth_alias
                if first_last_state is not None and first_last_state.agg_synth_alias
                else f"{target_relation}._having_agg"
            )
            synth = self._build_agg_render_spec_from_planned(
                slot=tmp_slot,
                key=local_agg,
                source_model=target_model,
                source_relation=target_relation,
                full_alias=having_full_alias,
                bundle=bundle,
            )
            # DEV-1708 Law 2: if the projected first/last materialised its
            # crossing SOURCE value as a ``_val_<n>`` column inside the ranked
            # subquery, the HAVING aggregate must bind to that SAME alias — the
            # outer ``MAX(CASE WHEN _last_rn = 1 THEN <raw crossing ref> END)``
            # would otherwise reference a table bound only inside the subquery.
            # DEV-1709: the alias map is keyed by the RESOLVED value text
            # (qualified + typed inner CAST) so same-sql-different-type
            # aggregates bind to their own materialisations.
            if first_last_state is not None and synth.sql:
                resolved_key = self._resolve_value_sql(synth)
                if resolved_key in first_last_state.value_alias_by_sql:
                    synth = synth.model_copy(update={
                        "sql": first_last_state.value_alias_by_sql[resolved_key],
                    })
            # Thread the cross-model CTE's rn maps so the HAVING
            # aggregate uses the same ``_first_rn`` / ``_last_rn{suffix}``
            # / ``_last_rn_fN`` column the CTE SELECT projects.
            rn_suffix_map = (
                first_last_state.rn_suffix_map if first_last_state else None
            )
            default_time_col = (
                first_last_state.default_time_col_sql
                if first_last_state else None
            )
            filtered_rn_map = (
                first_last_state.filtered_rn_map if first_last_state else None
            )
            filtered_match_map = (
                first_last_state.filtered_match_map if first_last_state else None
            )
            expr, _ = self._build_agg(
                synth,
                rn_suffix_map=rn_suffix_map,
                default_time_col=default_time_col,
                filtered_rn_map=filtered_rn_map,
                filtered_match_map=filtered_match_map,
            )
            return expr
        if isinstance(value_key, ArithmeticKey):
            op = value_key.op
            rendered_operands = [
                self._render_filter_value_key_in_target_scope(
                    value_key=op_key,
                    target_relation=target_relation,
                    target_model=target_model,
                    planned_query=planned_query,
                    bundle=bundle,
                    first_last_state=first_last_state,
                )
                for op_key in value_key.operands
            ]
            return self._build_arith_or_cmp_ast(op=op, operands=rendered_operands)
        if isinstance(value_key, ScalarCallKey):
            # DEV-1708 (Codex): a routed filter wrapping a target ref in a scalar
            # call (``abs(customers.deep_pop) > 5``) — render each arg in the
            # target scope (a derived arg expands + pulls its join) and rebuild
            # the call, mirroring the local filter-render path's ScalarCallKey
            # branch (``like`` → ``exp.Like``; else ``func(NAME, *args)`` through
            # the dialect rewrite). Without this the key falls through to the
            # scalar fallback and emits its repr as a bogus string literal.
            rendered_args = [
                self._render_filter_value_key_in_target_scope(
                    value_key=a,
                    target_relation=target_relation,
                    target_model=target_model,
                    planned_query=planned_query,
                    bundle=bundle,
                    first_last_state=first_last_state,
                )
                for a in value_key.args
            ]
            return render_scalar_call(
                name=value_key.name, args=rendered_args, dialect=self._dialect,
            )
        if isinstance(value_key, BetweenKey):
            # DEV-1708: a routed ``date_range``-derived BETWEEN over a target
            # (possibly crossing) column — render each operand in the target
            # scope, mirroring the local filter path's ``exp.Between``.
            def _render(k):
                return self._render_filter_value_key_in_target_scope(
                    value_key=k,
                    target_relation=target_relation,
                    target_model=target_model,
                    planned_query=planned_query,
                    bundle=bundle,
                    first_last_state=first_last_state,
                )
            return exp.Between(
                this=_render(value_key.column),
                low=_render(value_key.low),
                high=_render(value_key.high),
            )
        if isinstance(value_key, InKey):
            # DEV-1475: cross-model IN filter — render the LHS column
            # rooted at the CTE's target relation (so a bare ``name`` on
            # ``stores`` becomes ``stores.name``), and the RHS literals
            # inline. The cross-model routing path lands here only when
            # the InKey's LHS column lives on the CTE target.
            col_expr = self._render_filter_value_key_in_target_scope(
                value_key=value_key.column,
                target_relation=target_relation,
                target_model=target_model,
                planned_query=planned_query,
                bundle=bundle,
                first_last_state=first_last_state,
            )
            value_exprs = [
                self._literal_key_to_exp(lit) for lit in value_key.values
            ]
            in_expr = exp.In(this=col_expr, expressions=value_exprs)
            return exp.Not(this=in_expr) if value_key.negated else in_expr
        # Scalars stored inline (Decimal / str / bool / None).
        return self._literal_key_to_exp(value_key)

    def _literal_key_to_exp(self, value) -> exp.Expression:
        """Convert a scalar / LiteralKey value to a sqlglot literal."""
        from slayer.core.keys import LiteralKey
        from decimal import Decimal

        if isinstance(value, LiteralKey):
            inner = value.value
        else:
            inner = value
        if isinstance(inner, bool):
            return exp.Boolean(this=inner)
        if isinstance(inner, (int, float, Decimal)):
            return exp.Literal.number(str(inner))
        if inner is None:
            return exp.Null()
        return exp.Literal.string(str(inner))

    def _build_arith_or_cmp_ast(
        self,
        *,
        op: str,
        operands: List[exp.Expression],
    ) -> exp.Expression:
        """Build a sqlglot expression for a binary or unary op.

        Delegates to the one composer in ``slayer.sql.render.value_expr``.
        The hand-rolled version here applied NO precedence pass at all, so
        ``(a + b) * c`` emitted ``a + b * c`` and ``(a > b) + 1`` emitted
        ``a > b + 1`` — both parse, both mean something else.
        """
        return render_arithmetic(op, list(operands))

    def _build_combined_order_by_sql(
        self,
        *,
        planned_query,
        slots_by_id: Dict[str, Any],
        cma_slot_ids: Set[str],
        cm_alias_for_plan: Dict[str, str],
        bare_order_slot_ids: Optional[Set[str]] = None,
        outer_composite_aliases: Optional[Dict[str, str]] = None,
        outer_composite_expressions: Optional[Dict[str, exp.Expression]] = None,
        hidden_cte_order_refs: Optional[Dict[str, exp.Expression]] = None,
    ) -> List[exp.Ordered]:
        """Build the combined SELECT's ORDER BY terms, as AST.

        Terms are AST rather than text because they reference DOTTED public
        aliases, and rendering them to a string only to re-parse it re-reads
        such an alias as a multi-part reference on BigQuery — the same
        corruption the grain join-back suffered.

        PROJECTED local slots are referenced as ``_base."<full_alias>"``
        (legacy parity); cross-model slots are referenced as bare
        ``"<full_alias>"`` (they live in a single column projected from
        the cross-model CTE). HIDDEN order-only local slots
        (``bare_order_slot_ids``) are also referenced bare: they are
        materialised in ``_base`` but TRIMMED from the combined public
        projection, so the outermost ORDER BY must use the unqualified
        alias — the ``_base.`` qualifier would dangle if an outer
        projection-trim wrapper (which exposes only the bare public
        aliases) is ever layered on top. The bare alias still resolves
        unambiguously against ``_base`` in the combined FROM.

        DEV-1503 (Codex round 3 #2): outer-routed composite slots
        (``outer_composite_aliases``) live ONLY in the combined SELECT's
        projection — they are not materialised in ``_base``. Reference
        them as bare aliases so the ORDER BY resolves against the outer
        SELECT's own column list rather than ``_base.<alias>`` (which
        would dangle).
        """
        if not planned_query.order:
            return []
        bare_ids = bare_order_slot_ids or set()
        outer_aliases = outer_composite_aliases or {}
        outer_expressions = outer_composite_expressions or {}
        hidden_cte_refs = hidden_cte_order_refs or {}
        parts: List[exp.Ordered] = []
        for entry in planned_query.order:
            slot = slots_by_id.get(entry.slot_id)
            if slot is None:
                continue
            term = self._resolve_combined_order_term(
                entry=entry,
                slot=slot,
                source_relation=planned_query.source_relation,
                cma_slot_ids=cma_slot_ids,
                cm_alias_for_plan=cm_alias_for_plan,
                bare_ids=bare_ids,
                outer_aliases=outer_aliases,
                outer_expressions=outer_expressions,
                hidden_cte_refs=hidden_cte_refs,
            )
            if term is not None:
                parts.append(term)
        return parts

    def _resolve_combined_order_term(
        self,
        *,
        entry,
        slot,
        source_relation: str,
        cma_slot_ids: Set[str],
        cm_alias_for_plan: Dict[str, str],
        bare_ids: Set[str],
        outer_aliases: Dict[str, str],
        outer_expressions: Optional[Dict[str, exp.Expression]] = None,
        hidden_cte_refs: Optional[Dict[str, exp.Expression]] = None,
    ) -> Optional[exp.Ordered]:
        """Resolve one ``OrderEntry`` to an ``exp.Ordered`` term.

        Cross-model agg slot → bare CTE alias; projected outer-composite
        slot → bare combined-SELECT alias; order-only outer composite
        (Codex round 8 / CodeRabbit) → inline ``<expression> <dir>`` so
        no synthetic alias leaks into the combined projection and the
        transform-chain carry-forward doesn't lose track; hidden
        order-only local → bare ``_base`` alias; everything else →
        qualified ``_base."<alias>"``. Returns ``None`` when the
        cross-model alias map has no entry (the order slot can't be
        rendered).
        """
        descending = entry.direction != "asc"

        def _ordered(col: exp.Expression) -> exp.Ordered:
            return exp.Ordered(this=col, desc=descending)

        # DEV-1712 / DEV-1733: a HIDDEN (order-only) aggregate that lives in its
        # own CTE — cross-model (``_cm_``) or windowed (``_wm_``) — is trimmed
        # from the combined projection, so the bare alias no longer names a
        # projected column. Reference the CTE-qualified column instead. Checked
        # BEFORE the ``cma_slot_ids`` gate because a windowed slot is not a
        # cross-model slot and would otherwise fall through to the bare-alias
        # branch below and dangle.
        hidden_ref = (hidden_cte_refs or {}).get(entry.slot_id)
        if hidden_ref is not None:
            return _ordered(hidden_ref.copy())
        if entry.slot_id in cma_slot_ids:
            alias = cm_alias_for_plan.get(entry.slot_id)
            if alias is None:
                return None
            return _ordered(exp.column(alias, quoted=True))
        if entry.slot_id in outer_aliases:
            return _ordered(exp.column(outer_aliases[entry.slot_id], quoted=True))
        if outer_expressions and entry.slot_id in outer_expressions:
            return _ordered(outer_expressions[entry.slot_id].copy())
        full_alias = self._full_alias_for_slot(
            slot=slot,
            source_relation=source_relation,
            alias_index={},
        )
        if entry.slot_id in bare_ids:
            return _ordered(exp.column(full_alias, quoted=True))
        return _ordered(grain_alias_column(alias=full_alias, table="_base"))

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
        from slayer.core.keys import (
            ColumnKey,
            ColumnSqlKey,
            Phase,
            TimeTruncKey,
            column_leaf,
            column_path,
        )

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
        from slayer.core.keys import ColumnKey, Phase, TimeTruncKey

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
                next_alias = (
                    hop if hop_idx == 0
                    else f"{current_alias}__{hop}"
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
            target_alias = (
                hop if hop_idx == 0
                else f"{current_alias}__{hop}"
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

    def _render_window_transform_sql(
        self,
        *,
        slot,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
        planned_query,
    ) -> str:
        """Render one window-transform slot as an OVER() expression.

        Direct port of ``_build_transform_sql:1794`` but reads from the
        typed ``TransformKey`` instead of legacy ``EnrichedTransform``.
        Auto-partition matches legacy: ``partition_aliases = query
        dimensions only`` (NOT time dimensions) for non-rank ops;
        rank-family defaults to no PARTITION BY.
        """
        from slayer.core.keys import (
            ColumnKey,
            Phase,
            TransformKey,
        )

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
        from slayer.core.keys import (
            ArithmeticKey as _ArithKey,
            ScalarCallKey as _ScalarKey,
        )

        if isinstance(key.input, (_ArithKey, _ScalarKey)):
            measure = self._render_value_key_against_aliases(
                key=key.input,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            ).sql(dialect=self.dialect)
        else:
            # Resolve input alias (slotted leaf).
            input_sid = slot_id_by_key.get(key.input)
            if input_sid is None or input_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"transform input not materialised: slot id={slot.id!r}, "
                    f"op={key.op!r}, input_key={key.input!r}.",
                )
            input_alias = available_alias_by_slot_id[input_sid]
            measure = self._quote_ident(input_alias)

        # Resolve time-key alias (None for rank-family without time).
        time_alias: Optional[str] = None
        if key.time_key is not None:
            tk_sid = slot_id_by_key.get(key.time_key)
            if tk_sid is None or tk_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"transform time_key not materialised: "
                    f"slot id={slot.id!r}, op={key.op!r}, "
                    f"time_key={key.time_key!r}.",
                )
            time_alias = self._quote_ident(available_alias_by_slot_id[tk_sid])

        # Resolve partition aliases. Explicit partition_keys take
        # precedence; otherwise auto-partition by query dimension slots
        # (ColumnKey row-phase, hidden==False) — NOT TimeTruncKey slots
        # (matches legacy enrichment.py:584 ``[d.alias for d in
        # dimensions]``).
        rank_family = {"rank", "percent_rank", "dense_rank", "ntile"}
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
        elif key.op in rank_family:
            partition_aliases = []
        else:
            partition_aliases = []
            for sid in planned_query.projection:
                row_slot = slots_by_id.get(sid)
                if row_slot is None or row_slot.phase != Phase.ROW:
                    continue
                if not isinstance(row_slot.key, ColumnKey):
                    # Skip TimeTruncKey row slots — matches legacy
                    # ``[d.alias for d in dimensions]``.
                    continue
                alias = available_alias_by_slot_id.get(sid)
                if alias is not None:
                    partition_aliases.append(alias)

        partition_clause = (
            _SQL_PARTITION_BY + ", ".join(self._quote_ident(a) for a in partition_aliases)
            if partition_aliases
            else ""
        )
        order_clause = (
            f"ORDER BY {time_alias}" if time_alias else ""
        )
        over_parts = " ".join(p for p in (partition_clause, order_clause) if p)
        rank_order = f"ORDER BY {measure} DESC"
        rank_over = " ".join(p for p in (partition_clause, rank_order) if p)

        kwarg_map = dict(key.kwargs)
        op = key.op

        def _normalise_periods(raw: Any, *, kw: str = "periods") -> int:
            """Reject bool / non-integral periods; accept int / integral
            Decimal. Mirrors the strict validation the binder applies to
            ``ntile.n`` and ``time_shift.periods``."""
            from decimal import Decimal
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
            return f"SUM({measure}) OVER ({over_parts})"
        if op == "lag":
            n = abs(_normalise_periods(kwarg_map.get("periods", 1)))
            return f"LAG({measure}, {n}) OVER ({over_parts})"
        if op == "lead":
            n = abs(_normalise_periods(kwarg_map.get("periods", 1)))
            return f"LEAD({measure}, {n}) OVER ({over_parts})"
        if op == "rank":
            return f"RANK() OVER ({rank_over})"
        if op == "percent_rank":
            return f"PERCENT_RANK() OVER ({rank_over})"
        if op == "dense_rank":
            return f"DENSE_RANK() OVER ({rank_over})"
        if op == "ntile":
            n = kwarg_map.get("n")
            if not isinstance(n, int):
                # Decimal-normalised int.
                try:
                    n_int = int(n)
                except (TypeError, ValueError):
                    raise ValueError(
                        f"ntile requires a positive integer n, got {n!r}",
                    )
                n = n_int
            if n <= 0:
                raise ValueError(
                    f"ntile requires a positive integer n, got {n!r}",
                )
            return f"NTILE({n}) OVER ({rank_over})"
        if op == "first":
            return (
                f"FIRST_VALUE({measure}) OVER ({over_parts} "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            )
        if op == "last":
            if time_alias is None:
                raise ValueError(
                    f"Transform 'last' requires an unambiguous time "
                    f"dimension (binder/planner gap; slot id={slot.id!r}).",
                )
            return (
                f"FIRST_VALUE({measure}) OVER "
                f"({partition_clause} ORDER BY {time_alias} DESC "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
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
        renderer in ``_render_value_key_for_filter``.
        """
        from slayer.core.keys import Phase

        out: List[str] = []
        for fp in planned_query.filters_by_phase:
            if fp.phase != Phase.POST:
                continue
            if fp.expression is None:
                raise ValueError(
                    f"POST-phase FilterPhase id={fp.id!r} has no typed "
                    f"expression; text-only POST filters are not supported.",
                )
            rendered = self._render_value_key_against_aliases(
                key=fp.expression.value_key,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            )
            out.append(rendered.sql(dialect=self.dialect))
        return out

    def _render_value_key_against_aliases(
        self,
        *,
        key,
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
    ) -> exp.Expression:
        """Render a typed ValueKey tree against already-materialised
        aliases (used inside the ``_filtered`` wrapper).

        Slot-worthy keys → quoted ``exp.Column`` refs to their aliases.
        ``ArithmeticKey`` / ``ScalarCallKey`` / ``BetweenKey`` /
        ``InKey`` / ``LiteralKey`` compose recursively.
        """
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            LiteralKey,
            ScalarCallKey,
            TimeTruncKey,
            TransformKey,
        )

        slotted_kinds = (
            ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey, TransformKey,
        )
        all_key_kinds = (
            TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey, InKey,
            ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey, LiteralKey,
        )

        def recurse(k) -> exp.Expression:
            return self._render_value_key_against_aliases(
                key=k,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            )

        if isinstance(key, slotted_kinds):
            sid = slot_id_by_key.get(key)
            if sid is None or sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"POST-phase filter references a key not materialised "
                    f"as a slot: {type(key).__name__} -> {key!r}.",
                )
            alias = available_alias_by_slot_id[sid]
            return exp.Column(this=exp.to_identifier(alias, quoted=True))

        if isinstance(key, LiteralKey):
            return _render_scalar_literal(key.value)

        if isinstance(key, ArithmeticKey):
            return self._compose_arithmetic_op(
                op=key.op, operands=[recurse(o) for o in key.operands],
            )

        if isinstance(key, ScalarCallKey):
            args = [
                recurse(a) if isinstance(a, all_key_kinds)
                else _render_scalar_literal(a)
                for a in key.args
            ]
            return render_scalar_call(
                name=key.name, args=args, dialect=self._dialect,
            )

        if isinstance(key, BetweenKey):
            return exp.Between(
                this=recurse(key.column),
                low=recurse(key.low),
                high=recurse(key.high),
            )

        if isinstance(key, InKey):
            # DEV-1475: POST-phase IN filter — LHS column resolves to a
            # quoted alias materialised in the ``_filtered`` wrapper; RHS
            # literals are inlined as bare sqlglot scalars.
            in_expr = exp.In(
                this=recurse(key.column),
                expressions=[recurse(lit) for lit in key.values],
            )
            return exp.Not(this=in_expr) if key.negated else in_expr

        raise NotImplementedError(
            f"DEV-1450 stage 7b.10: POST-phase filter key type "
            f"{type(key).__name__} not yet supported.",
        )

    @staticmethod
    def _compose_arithmetic_op(
        *, op: str, operands: List[exp.Expression],
    ) -> exp.Expression:
        """Compose an arithmetic / comparison / boolean operator over
        already-rendered operands.

        Accepts the operator aliases ``=``/``==``, ``<>``/``!=`` so the
        rendered SQL surfaces the canonical SQL spellings for POST filters.

        Delegates to the one composer in ``slayer.sql.render.value_expr``.
        The hand-rolled version's precedence table knew only ``+ - * /``, so
        a comparison nested in arithmetic — ``(a > b) + 1`` — emitted
        ``a > b + 1``, which reads as ``a > (b + 1)``.
        """
        return render_arithmetic(op, list(operands))

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
        order_sql = self._planned_order_by_sql(
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            available_alias_by_slot_id=available_alias_by_slot_id,
        )
        order_expr = (
            self._parse(f"SELECT 1 ORDER BY {order_sql}").args.get("order")
            if order_sql
            else None
        )
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

    def _planned_order_by_sql(
        self,
        *,
        planned_query,
        slots_by_id: Dict[str, Any],
        available_alias_by_slot_id: Dict[str, str],
    ) -> str:
        """Render the ORDER BY term list (without the keyword) for a planned
        query whose sort keys resolve to CTE-chain aliases."""
        order_parts: list[str] = []
        for order_entry in planned_query.order:
            slot = slots_by_id.get(order_entry.slot_id)
            alias = available_alias_by_slot_id.get(order_entry.slot_id)
            if slot is None or alias is None:
                raise RuntimeError(
                    f"ORDER BY references slot id={order_entry.slot_id!r} "
                    f"not materialised in the CTE chain.",
                )
            direction = (
                "ASC" if order_entry.direction == "asc" else "DESC"
            )
            order_parts.append(f'{self._quote_ident(alias)} {direction}')
        return ", ".join(order_parts)


    # -----------------------------------------------------------------
    # Stage 7b.11 helpers — self-join CTE transforms (time_shift,
    # consecutive_periods). change / change_pct desugar at plan time to
    # time_shift + arithmetic, so the renderer only needs the two
    # primitive shapes below.
    # -----------------------------------------------------------------

    def _build_shifted_cte_where_parts(
        self,
        *,
        planned_query,
        source_relation: str,
        source_model,
        bundle,
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
        from slayer.core.keys import Phase

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
                time_columns=time_cols,
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
        ``text`` filter has only its string form, so it keeps the
        ``_filter_join_paths`` dual raw + inline-expanded scan (the DEV-1494
        contract that surfaces a derived ref's expansion joins).

        Note the scan runs on the RESIDUAL, so the shifted CTE's join set
        follows what it actually renders.
        """
        if fp.expression is not None:
            residual = strip_frame_bounds(
                key=fp.expression.value_key, time_columns=time_columns,
            )
            if residual is None:
                return None  # wholly a frame bound — omit from the shifted CTE.
            rendered = self._render_value_key_for_filter(
                key=residual,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
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
        ctes: list,
        cte_allocator: AliasAllocator,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
        aliases_by_slot_id: Dict[str, List[str]],
        source_model,
        source_relation: str,
        shifted_where_parts: List[str],
        shifted_where_join_paths: List[Tuple[str, ...]],
        planned_query,
        bundle,
    ) -> None:
        """Emit a ``shifted_<alias>`` + ``sjoin_<alias>`` CTE pair for
        one time_shift transform slot.

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
        from slayer.core.enums import TimeGranularity
        from slayer.core.keys import (
            AggregateKey,
            ColumnKey,
            ColumnSqlKey,
            TimeTruncKey,
            TransformKey,
        )

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
        from decimal import Decimal
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
        shifted_scope = ScopeFrame(
            scope_id=shifted_allocator.next_scope_id(source_relation),
            root_model=source_model,
            root_relation=source_relation,
            bundle=bundle,
            dialect=self._dialect,
            allocator=shifted_allocator,
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
        from slayer.core.keys import Phase as _Phase
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
            raise NotImplementedError(
                f"time_shift partition on {type(pk_obj).__name__} is not "
                f"supported (only column / derived-column / time-dimension "
                f"partitions render in the shifted CTE). slot id={slot.id!r}.",
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

        # Auto-include EVERY projected row dimension (column, derived column, or
        # secondary time dimension); the shift axis is skipped by slot id above.
        for sid in planned_query.projection:
            dim_slot = slots_by_id.get(sid)
            if dim_slot is None or dim_slot.phase != _Phase.ROW:
                continue
            if not isinstance(dim_slot.key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
                continue
            _add_partition(dim_slot.key, where="query dimension")

        # Explicit partition_keys (DEV-1450 C6) may add more (deduped by slot id
        # against the auto-included dims — see the DEV-1711 dedup test).
        for pk in sorted(key.partition_keys, key=lambda k: repr(k)):
            _add_partition(pk, where="partition_key")

        # DEV-1711 defensive completeness: a LOCAL aggregate whose source /
        # column-filter / kwargs cross a join is isolated upstream (Stage 5) and
        # would have raised 7b.15e before reaching a time_shift CTE, so these
        # registrations are provably no-ops today — but routing them through the
        # scope keeps Law 1 total (no render path skips join discovery).
        if isinstance(inner_key, AggregateKey):
            if isinstance(inner_key.source, ColumnSqlKey):
                shifted_scope.resolve(inner_key.source)
            for _kname, _kval in inner_key.kwargs:
                if isinstance(_kval, (ColumnKey, ColumnSqlKey)):
                    shifted_scope.resolve(_kval)
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
        shifted_raw_expr = self._build_time_offset_expr(
            col_expr=raw_time_col_expr,
            offset=-periods,
            granularity=shift_granularity,
        )
        shifted_trunc_expr = self._build_date_trunc(
            col_expr=shifted_raw_expr,
            granularity=TimeGranularity(time_key.granularity),
        )

        # Build the shifted CTE.
        shifted_select_parts: list[str] = []
        shifted_group_by: list[str] = []

        # Projected: time-trunc shifted under the base time alias.
        shifted_trunc_sql = shifted_trunc_expr.sql(dialect=self.dialect)
        shifted_select_parts.append(
            f'{shifted_trunc_sql} AS {self._quote_ident(time_alias)}',
        )
        shifted_group_by.append(shifted_trunc_sql)

        # partition_keys: SELECT + GROUP BY under their base aliases.
        for _, pk_alias, pk_expr in partition_specs:
            pk_sql = pk_expr.sql(dialect=self.dialect)
            shifted_select_parts.append(f'{pk_sql} AS {self._quote_ident(pk_alias)}')
            shifted_group_by.append(pk_sql)

        # Aggregate: re-emit the AggregateKey using the same synth /
        # _build_agg dance the base CTE uses.
        if isinstance(inner_key, AggregateKey):
            # Build a synth EnrichedMeasure for _build_agg.
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
            )
            agg_expr, _ = self._build_agg(synth)
            agg_expr = _wrap_cast_for_type(agg_expr, inner_slot.type)
            shifted_select_parts.append(
                f'{agg_expr.sql(dialect=self.dialect)} AS {self._quote_ident(input_alias)}',
            )
        else:
            # Row-level column input (not aggregated). Resolve through the scope
            # so a joined / derived input registers its join and anchors
            # correctly (Law 1), same as every other ref in this CTE.
            col_expr = shifted_scope.resolve(inner_key)
            shifted_select_parts.append(
                f'{col_expr.sql(dialect=self.dialect)} AS {self._quote_ident(input_alias)}',
            )
            shifted_group_by.append(col_expr.sql(dialect=self.dialect))

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

        from_parts = [f"FROM {from_clause.sql(dialect=self.dialect)}"]
        for join_expr, on_expr, join_type in shifted_joins:
            from_parts.append(
                f"{join_type} JOIN {join_expr.sql(dialect=self.dialect)} "
                f"ON {on_expr.sql(dialect=self.dialect)}"
            )

        shifted_sql_parts = [_SQL_SELECT_HEAD + ",\n  ".join(shifted_select_parts)]
        shifted_sql_parts.extend(from_parts)
        if shifted_where_parts:
            shifted_sql_parts.append(
                "WHERE " + _SQL_AND_JOINER.join(shifted_where_parts),
            )
        if shifted_group_by:
            shifted_sql_parts.append(
                "GROUP BY\n  " + ",\n  ".join(shifted_group_by),
            )
        shifted_sql = "\n".join(shifted_sql_parts)

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
        # DEV-1692: allocate collision-free CTE names too.
        shifted_cte_name = cte_allocator.allocate_cte(f"shifted_{cte_name_alias}")
        sjoin_cte_name = cte_allocator.allocate_cte(f"sjoin_{cte_name_alias}")

        ctes.append((shifted_cte_name, shifted_sql))

        # Build the sjoin CTE: LEFT JOIN prev_cte + shifted on time +
        # partition equalities. Carry every prev_cte alias forward,
        # then add the shifted measure under EACH of the slot's public
        # aliases (DEV-1450 C13).
        prev_cte = ctes[-2][0]  # the CTE just before the shifted CTE
        carry_aliases_sorted = self._carry_aliases_in_plan_order(
            aliases_by_slot_id,
        )
        sjoin_select_parts = [
            f'{prev_cte}.{self._quote_ident(a)}' for a in carry_aliases_sorted
        ]
        slot_full_aliases: List[str] = []
        for slot_alias in slot_aliases:
            full_slot_alias = f"{source_relation}.{slot_alias}"
            slot_full_aliases.append(full_slot_alias)
            sjoin_select_parts.append(
                f'{shifted_cte_name}.{self._quote_ident(input_alias)} AS {self._quote_ident(full_slot_alias)}',
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
        sjoin_sql = (
            "SELECT " + ", ".join(sjoin_select_parts)
            + f"\nFROM {prev_cte}"
            + f"\nLEFT JOIN {shifted_cte_name}"
            + "\n    ON " + sjoin_on.sql(dialect=self.dialect)
        )
        ctes.append((sjoin_cte_name, sjoin_sql))

        # Record EACH alias in both the per-slot list (C13 carry-forward
        # in the outer SELECT) and the "pick one" map (transform input /
        # filter / order lookups by downstream layers).
        for full_slot_alias in slot_full_aliases:
            aliases_by_slot_id.setdefault(slot.id, []).append(full_slot_alias)
        # ``available_alias_by_slot_id`` is "pick one" — first alias wins.
        available_alias_by_slot_id.setdefault(slot.id, slot_full_aliases[0])

    def _emit_consecutive_periods_ctes_for_planned(  # NOSONAR(S3776) — one cohesive per-slot consecutive_periods emission: predicate-shape decision, unique hidden alias plus collision-safe reset and value CTE names, the reset-group window layer, then the count-within-group window layer. Each block shares the slot registry and alias maps and cte_allocator; extracting helpers would scatter that contract without simplifying it.
        self,
        *,
        slot,
        ctes: list,
        cte_allocator: AliasAllocator,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
        aliases_by_slot_id: Dict[str, List[str]],
        planned_query,
        source_relation: str,
    ) -> None:
        """Emit ``cp_reset_<alias>`` + ``cp_value_<alias>`` CTEs for one
        consecutive_periods transform slot.

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
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            ColumnKey,
            ColumnSqlKey,
            Phase,
            TimeTruncKey,
            TransformKey,
        )

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
            input_alias = available_alias_by_slot_id[input_sid]
            predicate_sql = (
                f'{self._quote_ident(input_alias)} IS NOT NULL AND {self._quote_ident(input_alias)} <> 0'
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
            rendered = self._render_value_key_against_aliases(
                key=inner_key,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            )
            predicate_sql = rendered.sql(dialect=self.dialect)
            predicate_is_boolean = True
        else:
            raise NotImplementedError(
                f"DEV-1450 stage 7b.11: consecutive_periods input "
                f"{type(inner_key).__name__} not supported.",
            )

        # COALESCE / numeric wrap.
        if predicate_is_boolean:
            pred_in_case = f"COALESCE({predicate_sql}, FALSE)"
        else:
            pred_in_case = predicate_sql

        # Auto-partition by query dimensions (ColumnKey row-phase slots
        # only — NOT TimeTruncKey, matching legacy).
        partition_aliases: list[str] = []
        for sid in planned_query.projection:
            row_slot = slots_by_id.get(sid)
            if row_slot is None or row_slot.phase != Phase.ROW:
                continue
            if not isinstance(row_slot.key, ColumnKey):
                continue
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
        prev_cte = ctes[-1][0]
        carry_aliases_sorted = self._carry_aliases_in_plan_order(
            aliases_by_slot_id,
        )
        carry_select = ",\n  ".join(self._quote_ident(a) for a in carry_aliases_sorted)
        partition_clause = (
            _SQL_PARTITION_BY + ", ".join(self._quote_ident(a) for a in partition_aliases)
            if partition_aliases
            else ""
        )
        over_reset = " ".join(p for p in (
            partition_clause,
            f'ORDER BY {self._quote_ident(time_alias)}',
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        ) if p)
        reset_window_sql = (
            f'SUM(CASE WHEN {pred_in_case} THEN 0 ELSE 1 END) '
            f'OVER ({over_reset}) AS {self._quote_ident(cp_reset_alias)}'
        )
        cp_reset_cte_name = cte_allocator.allocate_cte(f"cp_reset_{slot_alias}")
        cp_reset_sql = (
            _SQL_SELECT_HEAD + carry_select
            + ",\n  " + reset_window_sql
            + f"\nFROM {prev_cte}"
        )
        ctes.append((cp_reset_cte_name, cp_reset_sql))

        # Build the value CTE — references the cp_reset CTE's added
        # column in PARTITION BY so each run of true predicate is
        # counted within its own reset group.
        value_partition_aliases = partition_aliases + [cp_reset_alias]
        value_partition_clause = _SQL_PARTITION_BY + ", ".join(
            self._quote_ident(a) for a in value_partition_aliases
        )
        over_value = " ".join((
            value_partition_clause,
            f'ORDER BY {self._quote_ident(time_alias)}',
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        ))
        # Outer CASE WHEN guarantees rows where the predicate is false
        # surface as 0 (legacy parity).
        value_inner_window_sql = (
            f'SUM(CASE WHEN {pred_in_case} THEN 1 ELSE 0 END) '
            f'OVER ({over_value})'
        )
        value_outer_case = (
            f'CASE WHEN {pred_in_case} '
            f'THEN {value_inner_window_sql} ELSE 0 END '
            f'AS {self._quote_ident(full_slot_alias)}'
        )
        cp_value_cte_name = cte_allocator.allocate_cte(f"cp_value_{slot_alias}")
        cp_value_sql = (
            _SQL_SELECT_HEAD + carry_select
            + ",\n  " + value_outer_case
            + f"\nFROM {cp_reset_cte_name}"
        )
        ctes.append((cp_value_cte_name, cp_value_sql))

        # Record the slot's alias for downstream lookups.
        aliases_by_slot_id.setdefault(slot.id, []).append(full_slot_alias)
        available_alias_by_slot_id.setdefault(slot.id, full_slot_alias)

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

    def _column_ref_is_derived(
        self, *, col: exp.Column, source_model, source_relation: str, bundle,
        include_dotted: bool,
    ) -> bool:
        """True iff a single ``exp.Column`` ref resolves to a non-trivial DERIVED
        column — bare on ``source_model``, or (when ``include_dotted``) a dotted
        ``__``-path alias resolving through ``bundle`` to a derived column on a
        joined model. Dotted resolution starts from ``source_relation`` (the
        alias the rest of the Mode-A path uses), not ``source_model.name``.
        """
        if col.args.get("db") or col.args.get("catalog"):
            return False
        ident = col.this
        if not isinstance(ident, exp.Identifier):
            return False
        tbl = col.args.get("table")
        if tbl is None:
            return self._is_nontrivial_derived(source_model, ident.name)
        if not include_dotted:
            return False
        target, _ = _walk_path_to_target_sync(
            source_model=source_model,
            source_alias=source_relation,
            table_alias=tbl.name,
            resolve_model=bundle.get_referenced_model,
            is_root=True,
        )
        return target is not None and self._is_nontrivial_derived(target, ident.name)

    def _predicate_references_derived(
        self, *, parsed: exp.Expression, source_model, source_relation: str,
        bundle, include_dotted: bool,
    ) -> bool:
        """True iff the parsed Mode-A predicate references a non-trivial DERIVED
        column (see :meth:`_column_ref_is_derived`). Drives whether the predicate
        must be inline-expanded (vs. cheaply qualified). DEV-1494.
        """
        return any(
            self._column_ref_is_derived(
                col=col, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                include_dotted=include_dotted,
            )
            for col in parsed.find_all(exp.Column)
        )

    def _render_mode_a_predicate(
        self,
        *,
        sql: Optional[str],
        source_model,
        source_relation: str,
        bundle,
        qualify_fallback,
        include_dotted_derived: bool = True,
    ) -> Optional[str]:
        """Render a Mode-A predicate (``Column.filter`` / ``SlayerModel.filters``)
        with DERIVED refs inline-expanded and base refs qualified — the shared
        core for the column-filter and model-filter render paths (DEV-1494).

        If the predicate references a non-trivial derived column (bare, or — when
        ``include_dotted_derived`` — a dotted ref to a derived column on a joined
        model), it is inline-expanded via ``expand_derived_refs_sync`` so the
        crossed joins resolve and no dangling ``<alias>.<derived_col>`` (a
        non-physical column) survives — mirroring the query-level filter path.
        Otherwise ``qualify_fallback(sql)`` does the cheap bare-ref qualification,
        preserving each caller's exact non-derived output (regex for model
        filters, AST for column filters). On sqlglot parse failure the predicate
        falls through to ``qualify_fallback`` unchanged, so a dialect-specific
        fragment never raises earlier than today. ``include_dotted_derived`` is
        ``False`` for the cross-model ``_cm_*`` CTE target-filter path, which has
        no mechanism to add a deeper join an expansion would cross (DEV-1503).
        """
        if not sql:
            return None
        if bundle is not None:
            try:
                parsed = self._parse_predicate(sql)
            except Exception:
                parsed = None
            if parsed is not None and self._predicate_references_derived(
                parsed=parsed, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
                include_dotted=include_dotted_derived,
            ):
                root = self._expand_degenerate_derived_root(
                    parsed=parsed, source_model=source_model,
                    source_relation=source_relation, bundle=bundle,
                    include_dotted_derived=include_dotted_derived,
                )
                if root is not None:
                    return root
                expanded = expand_derived_refs_sync(
                    sql=sql,
                    model=source_model,
                    alias_path=source_relation,
                    resolve_model=bundle.get_referenced_model,
                    dialect=self.dialect,
                )
                if expanded is not None:
                    return expanded
        return qualify_fallback(sql)

    def _expand_degenerate_derived_root(
        self, *, parsed: exp.Expression, source_model, source_relation: str,
        bundle, include_dotted_derived: bool,
    ) -> Optional[str]:
        """When the whole predicate IS a single derived-column ref
        (``filter="is_eu"`` / ``filter="loss_payment.has_flag"``), expand it
        directly — ``expand_derived_refs_sync`` rewrites refs via in-place
        ``col.replace``, a no-op on the AST root. A dotted root is walked to its
        target model + canonical ``__`` alias and expanded with ``is_root=False``
        so further-joined refs prefix correctly. Returns the expanded SQL, or
        ``None`` when ``parsed`` is not such a derived single-column root.
        """
        if not isinstance(parsed, exp.Column) or (
            parsed.args.get("db") or parsed.args.get("catalog")
        ):
            return None
        tbl = parsed.args.get("table")
        if tbl is None:
            if self._is_nontrivial_derived(source_model, parsed.name):
                return self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=parsed.name,
                    bundle=bundle,
                )
            return None
        if not include_dotted_derived:
            return None
        target, canonical = _walk_path_to_target_sync(
            source_model=source_model,
            source_alias=source_relation,
            table_alias=tbl.name,
            resolve_model=bundle.get_referenced_model,
            is_root=True,
        )
        if (
            target is not None
            and canonical is not None
            and self._is_nontrivial_derived(target, parsed.name)
        ):
            return self._expand_derived_column_sql(
                source_model=target,
                source_relation=canonical,
                column_name=parsed.name,
                bundle=bundle,
                is_root=False,
            )
        return None

    def _filter_join_paths(
        self, *, sql: Optional[str], source_relation: str, source_model, bundle,
    ) -> List[Tuple[str, ...]]:
        """Join paths a Mode-A filter (``Column.filter`` / ``SlayerModel.filters``)
        needs (DEV-1494).

        Scans BOTH the un-inlined predicate — so a placeholder dotted ref
        (``loss_payment.has_flag``, the dbt join-trigger idiom) keeps its alias
        even when it inlines to a constant — AND the inline-expanded predicate —
        so a bare/dotted DERIVED ref surfaces the joins its expansion crosses
        (``is_eu`` → ``customers``; ``loss_payment.deep_flag`` →
        ``loss_payment__claim``). The union is required because inlining drops the
        placeholder alias while the raw form can't see a derived expansion's
        crossed joins. Each parse is tolerant — an unparseable side yields no
        paths rather than raising earlier than today.
        """
        if not sql:
            return []
        seen: set = set()
        ordered: List[Tuple[str, ...]] = []

        def _scan(text: Optional[str]) -> None:
            if not text:
                return
            try:
                parsed = self._parse_predicate(text)
            except Exception:
                return
            for p in self._joined_paths_in_sql(
                sql_expr=parsed, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
            ):
                if p not in seen:
                    seen.add(p)
                    ordered.append(p)

        _scan(sql)
        rendered = self._render_mode_a_predicate(
            sql=sql, source_model=source_model, source_relation=source_relation,
            bundle=bundle, qualify_fallback=lambda s: s,
        )
        if rendered is not None and rendered != sql:
            _scan(rendered)
        return ordered

    # ---- The one Mode-A door, generator side (P-A) -------------------------
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
    ) -> None:
        """Register the joins an aggregation's template FRAGMENTS cross.

        The sources are the aggregate's string kwargs plus the non-overridden
        ``AggregationParam.sql`` defaults of the aggregation named by
        ``key.agg`` on ``model``. Both substitute verbatim into the rendered
        aggregate expression, so whatever they reach has to be in the FROM.

        Shared by the host base SELECT and the ``_cm_*`` cross-model CTE. The
        host path always did this; the CTE path did not, and emitted
        ``SUM(customers.spend * regions.weight) FROM customers`` — SQL no
        database accepts. One implementation, entered through the one door, is
        what stops that from recurring (DEV-1745 W2).

        Only values the aggregation's formula actually SUBSTITUTES are treated
        as SQL. A string kwarg whose ``{name}`` never appears in the template is
        a marker, not a fragment — ``revenue:sum(window='90d')`` being the
        standing example — and handing it to a SQL parser is meaningless. It
        was harmless while the scan swallowed parse errors; now that the door
        raises, a marker that does not happen to parse would take the query
        down with it.
        """
        agg_def = next(
            (a for a in (model.aggregations or []) if a.name == key.agg), None,
        )
        if agg_def is None:
            # A built-in aggregation has no template, so no kwarg of it is a
            # SQL fragment.
            return
        formula = agg_def.formula or ""
        overridden = {name for name, _ in key.kwargs}
        fragments = [
            v for name, v in key.kwargs
            if isinstance(v, str) and f"{{{name}}}" in formula
        ]
        fragments.extend(
            p.sql for p in (agg_def.params or [])
            if p.name not in overridden and p.sql
        )
        for frag in fragments:
            self._enter_mode_a_expression(
                sql=frag, scope=scope,
                location=(
                    f"aggregation {key.agg!r} template fragment on model "
                    f"{model.name!r}"
                ),
            )

    def _expand_derived_row_dims(  # NOSONAR(S3776) — one cohesive per-slot pass expanding derived ROW/TIME dimensions and registering the joins they cross.
        self, *, base_render_order, slots_by_id, source_relation: str,
        source_model, bundle, scope: ScopeFrame,
    ) -> Dict[str, exp.Expression]:
        """Pre-expand derived (``ColumnSqlKey``) ROW dimensions and derived TIME
        dimensions for the base SELECT: inline sibling/joined derived refs
        (DEV-1333 / DEV-1410), register any joins their SQL crosses into
        ``scope.join_paths`` (Law 1 — the join-discovery side effect), and return
        the expanded-expr-by-slot-id map the render branch reads from. Extracted
        from ``_build_base_select_for_planned``.
        """
        from slayer.core.keys import ColumnSqlKey, Phase, TimeTruncKey

        def _add(path: Tuple[str, ...]) -> None:
            if path:
                scope.join_paths.add(path)

        derived_expr_by_sid: Dict[str, exp.Expression] = {}
        for sid in base_render_order:
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
            if key.path:
                owner_model = bundle.get_referenced_model(key.path[-1])
                if owner_model is None:
                    continue
                owner_relation = "__".join(key.path)
            else:
                owner_model = source_model
                owner_relation = source_relation
            expanded_sql = self._expand_derived_column_sql(
                source_model=owner_model, source_relation=owner_relation,
                column_name=key.column_name, bundle=bundle, is_root=not key.path,
            )
            col = next(
                (c for c in owner_model.columns if c.name == key.column_name), None,
            )
            expr = _wrap_cast_for_type(
                self._parse(expanded_sql), col.type if col is not None else None,
            )
            derived_expr_by_sid[sid] = expr
            _add(key.path)  # the join to the owning model itself (cross-model)
            for p in self._joined_paths_in_sql(
                sql_expr=expr, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
            ):
                _add(p)
        return derived_expr_by_sid

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
        resolve; otherwise qualifies bare refs. DEV-1494; see
        ``_render_mode_a_predicate``.
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
        from slayer.core.keys import ColumnKey, ColumnSqlKey

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
                    is_root=False,
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
        is_root: bool = True,
    ) -> str:
        """Expand a derived ``Column.sql`` (a ``ColumnSqlKey`` target) into a
        fully-qualified SQL string, recursively inlining references to other
        derived columns on the same model or on joined models (DEV-1333 /
        DEV-1410). Bare identifiers qualify to ``source_relation``; joined
        refs qualify to their ``__``-canonical path alias.

        ``is_root`` is ``False`` when the derived column lives on a JOINED
        model (a cross-model derived dimension, ``source_relation`` being the
        ``__``-path alias). A further-joined reference inside that column's
        sql then resolves to the full path (``B`` reaching ``C`` →
        ``B__C``), not the bare child alias.

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
        expanded = expand_derived_refs_sync(
            sql=col.sql,
            model=source_model,
            alias_path=source_relation,
            resolve_model=bundle.get_referenced_model,
            dialect=self.dialect,
            is_root=is_root,
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
        same ``_value_key_join_paths`` / ``_filter_join_paths`` sub-scanners in
        the same ``filters_by_phase`` order, so the base FROM stays byte-identical.

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
        from slayer.core.keys import Phase

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
                # crosses). See ``_filter_join_paths``.
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
        from slayer.core.keys import (
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            ScalarCallKey,
        )

        out: List[Tuple[str, ...]] = []

        def _add(path: Tuple[str, ...]) -> None:
            for i in range(1, len(path) + 1):
                prefix = tuple(path[:i])
                if prefix and prefix not in out:
                    out.append(prefix)

        def _scan(parsed: exp.Expression) -> None:
            for p in self._joined_paths_in_sql(
                sql_expr=parsed, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
            ):
                if p not in out:
                    out.append(p)

        def _derived_paths(*, model, relation, column_name, is_root: bool) -> None:
            _scan(self._parse(self._expand_derived_column_sql(
                source_model=model, source_relation=relation,
                column_name=column_name, bundle=bundle, is_root=is_root,
            )))

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
                    # ``is_root=False`` for a JOINED derived column: a further
                    # -joined ref inside its ``sql`` must resolve to the full
                    # path (``customers_v2`` reaching ``regions`` →
                    # ``customers_v2__regions``). Rooting it here instead left
                    # the ref bare, so the scan found no path and the hop was
                    # never joined — the filter then referenced a table that
                    # is not in the FROM.
                    _derived_paths(
                        model=model,
                        relation="__".join(k.path) if k.path else source_relation,
                        column_name=k.column_name,
                        is_root=not k.path,
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
        from slayer.core.keys import ColumnKey, ColumnSqlKey

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

        Replaces the legacy ``_build_agg_render_spec_from_planned``
        adapter (DEV-1452 Stage A). Mirrors ``enrichment.py:431``
        ``sql = column.sql or column.name`` so ``COUNT(*)`` (StarKey source)
        and ``COUNT(col)`` (ColumnKey source with sql=None on a bare column)
        take their distinct branches inside ``_build_agg``.
        """
        from slayer.core.keys import ColumnKey, ColumnSqlKey, StarKey

        # ``slot`` may be ``None`` when this spec is built for a HAVING term
        # whose aggregate isn't a declared projection slot; the result type is
        # then unknown (no outer CAST needed for a comparison operand).
        slot_type = slot.type if slot is not None else None
        source = key.source
        if isinstance(source, StarKey):
            # Legacy enrichment (enrichment.py:~388) rejects any
            # non-count aggregation on ``*`` — e.g. ``*:sum`` or
            # ``*:median`` would otherwise plan and render as
            # ``SUM(*)`` / ``MEDIAN(*)``, which is meaningless.
            # Mirror that rejection here so the typed pipeline can't
            # silently emit invalid SQL (Codex MEDIUM fold-in).
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
            # ``first`` / ``last`` aggregations rank rows via a ROW_NUMBER
            # subquery (built in ``_build_ranked_subquery_from_planned``) and
            # pick ``rn = 1`` through ``MAX(CASE WHEN _rn = 1 THEN col END)``.
            # An explicit positional arg (``latest_amount:last(created_at)``
            # or ``…:last(derived_time_col)``) overrides the query's default
            # ranking time column; the helper handles both bare-column
            # (``ColumnKey``) and derived-column (``ColumnSqlKey``) args.
            explicit_time_col = self._resolve_explicit_time_col(
                key=key,
                source_model=source_model,
                source_relation=source_relation,
                bundle=bundle,
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
                time_column=explicit_time_col,
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
        first_last_state: Optional[FirstLastRenderState] = None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
        filters_override: "Optional[List[Any]]" = None,
    ):
        """``filters_override`` (DEV-1732) replaces ``filters_by_phase`` as the
        list being rendered — see ``_effective_src_filters``."""
        from slayer.core.keys import Phase

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
                # DEV-1501: thread ``first_last_state`` so HAVING
                # aggregates reference the same ``_first_rn`` /
                # ``_last_rn{suffix}`` columns the base SELECT projects,
                # AND thread ``aliases_by_slot_id`` so the synth's
                # ``full_alias`` matches the materialised spec's alias —
                # required for ``filtered_rn_map`` / ``filtered_match_map``
                # lookups (which are keyed by the full alias the
                # ranked-subquery builder used).
                rendered = self._render_value_key_for_filter(
                    key=fp.expression.value_key,
                    source_relation=source_relation,
                    source_model=source_model,
                    bundle=bundle,
                    slot_by_key=slot_by_key,
                    first_last_state=first_last_state,
                    aliases_by_slot_id=aliases_by_slot_id,
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
    def _qualify_mode_a_sql_filter(
        *,
        sql: str,
        columns,
        source_model,
        source_relation: str,
    ) -> str:
        """Qualify bare-identifier column references in a Mode-A SQL
        filter — mirrors legacy ``_build_where_and_having`` at
        ``slayer/sql/generator.py:2566-2580``.

        For each name in ``columns``:
        * Already-dotted refs are left alone (``orders.id`` stays).
        * Non-identifier tokens (SQL keywords picked up by the regex
          extractor) are left alone.
        * Bare identifiers matching a model column name are rewritten
          to ``<source_relation>.<col>``. The negative lookbehind
          ``(?<!\\.)(?<!\\w)`` prevents touching already-qualified or
          substring-of-another-identifier matches.

        Note: ``columns`` is the column list ``parse_sql_predicate``
        returned at planner time. The bare-identifier filter is
        permissive (matches more than just column names) — only those
        also present in the regex's ``\\b...\\b`` match get rewritten,
        which mirrors legacy behavior exactly.
        """
        import re
        out = sql
        for col_name in dict.fromkeys(columns):
            if "." in col_name:
                continue
            if not col_name.isidentifier():
                continue
            out = re.sub(
                rf"(?<!\.)(?<!\w)\b{re.escape(col_name)}\b",
                f"{source_relation}.{col_name}",
                out,
            )
        return out

    @staticmethod
    def _is_nontrivial_derived(model, name: str) -> bool:
        """True iff ``name`` is a column on ``model`` whose ``Column.sql`` is a
        non-trivial expression (set, and not just a bare-identifier remap)."""
        col = next((c for c in model.columns if c.name == name), None)
        return col is not None and col.sql is not None and not _is_trivial_base(
            column=col,
        )

    def _render_model_filter_sql(
        self,
        *,
        sql: str,
        columns,
        source_model,
        source_relation: str,
        bundle,
    ) -> str:
        """Render a ``SlayerModel.filters`` Mode-A SQL predicate (DEV-1450 #4b /
        DEV-1494).

        Inlines references to non-trivial derived columns — bare on
        ``source_model`` or dotted-to-a-derived-column-on-a-joined-model — so the
        crossed joins resolve; otherwise qualifies bare base refs via the regex
        path (``_qualify_mode_a_sql_filter``), byte-identical to legacy. Thin
        wrapper over ``_render_mode_a_predicate``.
        """
        rendered = self._render_mode_a_predicate(
            sql=sql,
            source_model=source_model,
            source_relation=source_relation,
            bundle=bundle,
            qualify_fallback=lambda s: self._qualify_mode_a_sql_filter(
                sql=s,
                columns=columns,
                source_model=source_model,
                source_relation=source_relation,
            ),
        )
        return rendered if rendered is not None else sql

    def _render_value_key_for_filter(  # NOSONAR(S3776) — sequential isinstance dispatch over the closed filter-ValueKey union. Each branch carries the per-type filter-render contract (local vs joined column qualification, derived-column expansion, aggregate-with-rn-state synth, etc.); extracting per-branch helpers would scatter the contract.
        self,
        *,
        key,
        source_relation: str,
        source_model,
        bundle,
        slot_by_key: Optional[Dict[Any, Any]] = None,
        first_last_state: Optional[FirstLastRenderState] = None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
    ) -> exp.Expression:
        """Render a ValueKey tree to sqlglot for WHERE / HAVING rendering.

        Supports ``ColumnKey`` (local AND joined ``path != ()`` — emitted as
        ``<__path_alias>.<leaf>``; the join is pulled into the FROM by
        ``_resolve_where_filter_joins_via_scope``), ``ColumnSqlKey`` (derived column —
        expanded inline, sibling/joined refs resolved), ``LiteralKey``,
        ``ArithmeticKey``, ``ScalarCallKey``, ``BetweenKey``, and a LOCAL
        ``AggregateKey`` (for HAVING — rendered as the bare aggregate
        expression so it works on dialects that reject SELECT aliases in
        HAVING). Cross-model aggregate refs (``path != ()``) and
        ``TransformKey`` / ``TimeTruncKey`` are deferred to later slices.
        """
        from decimal import Decimal

        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            LiteralKey,
            ScalarCallKey,
            StarKey,
            TimeTruncKey,
            TransformKey,
        )

        if isinstance(key, AggregateKey):
            # HAVING term: render the aggregate as its expression (``COUNT(*)``,
            # ``SUM(amount)``), not the SELECT alias — Postgres rejects output
            # aliases in HAVING. Cross-model aggregates (non-empty source path)
            # are routed into a per-plan CTE instead (handled by the caller).
            if getattr(key.source, "path", ()):
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.12: cross-model aggregate ref in "
                    f"filter (path={key.source.path!r}) routes via the "
                    f"per-plan CTE, not inline HAVING."
                )
            slot = (slot_by_key or {}).get(key)
            # DEV-1501 Group A.2: when the slot was materialised in the
            # base SELECT, ``_build_filtered_rn_columns`` keyed its
            # ``filtered_rn_map`` / ``filtered_match_map`` by the FULL
            # ALIAS the materialised spec used. The HAVING synth must
            # reuse the same alias — bare placeholder ``__having_ref__``
            # would miss the lookup and fall back to the unfiltered
            # ``_last_rn`` + raw ``filter_sql``.
            having_full_alias = "__having_ref__"
            if (
                aliases_by_slot_id is not None
                and slot is not None
                and aliases_by_slot_id.get(slot.id)
            ):
                having_full_alias = aliases_by_slot_id[slot.id][0]
            # DEV-1527: resolve this local aggregate's column-ref kwargs
            # (``weighted_avg(weight=<col>)`` / ``corr(other=<col>)``) through a
            # host scope so a derived/crossing kwarg renders its expanded, join-
            # anchored expression HERE too — matching the base SELECT — instead of
            # collapsing to a bare, non-existent name. The crossed join is already
            # base-pulled by ``_resolve_agg_inputs_via_scope`` (this HAVING
            # aggregate is also a ``base_render_order`` slot), so the throwaway
            # scope is used only to reproduce the same anchored expression.
            having_kwargs = self._resolve_agg_kwargs_for_key(
                key=key, source_model=source_model,
                source_relation=source_relation, bundle=bundle,
            )
            synth = self._build_agg_render_spec_from_planned(
                slot=slot,
                key=key,
                source_model=source_model,
                source_relation=source_relation,
                full_alias=having_full_alias,
                bundle=bundle,
                resolved_agg_kwargs=having_kwargs,
            )
            # DEV-1501: thread the rn suffix maps from the base SELECT
            # so a HAVING reference to a hidden first/last aggregate
            # binds to the same ``_first_rn`` / ``_last_rn{suffix}``
            # column the base projects (instead of bare ``_last_rn``,
            # which collapses distinct time-column specs).
            rn_suffix_map = (
                first_last_state.rn_suffix_map if first_last_state else None
            )
            default_time_col = (
                first_last_state.default_time_col_sql
                if first_last_state
                else None
            )
            filtered_rn_map = (
                first_last_state.filtered_rn_map if first_last_state else None
            )
            filtered_match_map = (
                first_last_state.filtered_match_map if first_last_state else None
            )
            agg_expr, _is_agg = self._build_agg(
                synth,
                rn_suffix_map=rn_suffix_map,
                default_time_col=default_time_col,
                filtered_rn_map=filtered_rn_map,
                filtered_match_map=filtered_match_map,
            )
            return agg_expr

        if isinstance(key, ColumnKey):
            if key.path != ():
                # Joined column ref (``customers.regions.name``) — emit the
                # ``__``-canonical path alias (``customers__regions.name``).
                # The join is pulled into the FROM by
                # ``_resolve_where_filter_joins_via_scope``.
                return exp.Column(
                    this=exp.to_identifier(key.leaf),
                    table=exp.to_identifier("__".join(key.path)),
                )
            col = next(
                (c for c in source_model.columns if c.name == key.leaf),
                None,
            )
            if col is None:
                raise ValueError(
                    f"Filter references column {key.leaf!r} which is "
                    f"not found on model {source_model.name!r}",
                )
            return self._resolve_sql(
                sql=col.sql,
                name=col.name,
                model_name=source_relation,
                type=col.type,
            )
        if isinstance(key, ColumnSqlKey):
            if key.path != ():
                # Joined derived-column ref (``policy_amount.premium.has_premium``).
                # Expand the column's ``sql`` rooted at the JOINED model,
                # qualifying bare refs to the ``__``-canonical path alias; the
                # join itself is pulled into the FROM by
                # ``_resolve_where_filter_joins_via_scope`` (which adds ``key.path``).
                joined_model = bundle.get_referenced_model(key.path[-1])
                if joined_model is None:
                    raise ValueError(
                        f"Filter references derived column {key.column_name!r} "
                        f"on joined model {key.path[-1]!r} which is not in the "
                        f"resolved source bundle.",
                    )
                path_alias = "__".join(key.path)
                # ``is_root=False`` — the column lives on a JOINED model, so a
                # further-joined ref inside its ``sql`` resolves to the full
                # path alias rather than the bare child relation. Must match
                # ``_value_key_join_paths``, which registers the joins by
                # scanning this same expansion.
                expanded_sql = self._expand_derived_column_sql(
                    source_model=joined_model,
                    source_relation=path_alias,
                    column_name=key.column_name,
                    bundle=bundle,
                    is_root=False,
                )
                col = next(
                    (c for c in joined_model.columns if c.name == key.column_name),
                    None,
                )
                return _wrap_cast_for_type(
                    self._parse(expanded_sql),
                    _filter_cast_type(col.type if col is not None else None),
                )
            # Derived column (``Column.sql`` set) — expand inline, resolving
            # sibling / joined derived refs and pulling crossed joins into the
            # FROM (via ``_resolve_where_filter_joins_via_scope``).
            expanded_sql = self._expand_derived_column_sql(
                source_model=source_model,
                source_relation=source_relation,
                column_name=key.column_name,
                bundle=bundle,
            )
            col = next(
                (c for c in source_model.columns if c.name == key.column_name),
                None,
            )
            return _wrap_cast_for_type(
                self._parse(expanded_sql),
                _filter_cast_type(col.type if col is not None else None),
            )
        if isinstance(key, LiteralKey):
            return self._scalar_to_sqlglot(key.value)
        if isinstance(key, ArithmeticKey):
            operands = [
                self._render_value_key_for_filter(
                    key=o,
                    source_relation=source_relation,
                    source_model=source_model,
                    bundle=bundle,
                    slot_by_key=slot_by_key,
                    first_last_state=first_last_state,
                    aliases_by_slot_id=aliases_by_slot_id,
                )
                for o in key.operands
            ]
            return self._build_arithmetic_for_filter(
                op=key.op, operands=operands,
            )
        if isinstance(key, ScalarCallKey):
            args = []
            for a in key.args:
                if isinstance(a, (Decimal, str, bool)) or a is None:
                    args.append(self._scalar_to_sqlglot(a))
                else:
                    args.append(self._render_value_key_for_filter(
                        key=a,
                        source_relation=source_relation,
                        source_model=source_model,
                        bundle=bundle,
                        slot_by_key=slot_by_key,
                        first_last_state=first_last_state,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ))
            # One ScalarCall policy everywhere (B5): typed node, dialect
            # rewrite, then the log-alias fix-up. This branch used to return an
            # ``exp.Anonymous`` passthrough for everything but ROUND, so a
            # filter emitted ``IFNULL(...)`` — which Postgres does not have —
            # while the same key emitted ``COALESCE(...)`` from a projection.
            #
            # The log fix-up is load-bearing: ``exp.func("LOG10", x)``
            # normalises to a generic ``Log(10, x)`` that re-emits as
            # ``LOG(10, x)``, wrong for dialects with a native single-arg
            # ``LOG10``. Transpiling alone fixes ifnull and breaks log10.
            return render_scalar_call(
                name=key.name, args=args, dialect=self._dialect,
            )
        if isinstance(key, BetweenKey):
            col_expr = self._render_value_key_for_filter(
                key=key.column,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                slot_by_key=slot_by_key,
                first_last_state=first_last_state,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            low_expr = self._render_value_key_for_filter(
                key=key.low,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                slot_by_key=slot_by_key,
                first_last_state=first_last_state,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            high_expr = self._render_value_key_for_filter(
                key=key.high,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                slot_by_key=slot_by_key,
                first_last_state=first_last_state,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            return exp.Between(this=col_expr, low=low_expr, high=high_expr)
        if isinstance(key, InKey):
            # DEV-1475: render the LHS column through the normal filter
            # path (local + joined paths both supported via ColumnKey /
            # ColumnSqlKey), and the RHS as a sequence of scalar
            # literals. Wrap in ``exp.Not`` for ``not in``.
            col_expr = self._render_value_key_for_filter(
                key=key.column,
                source_relation=source_relation,
                source_model=source_model,
                bundle=bundle,
                slot_by_key=slot_by_key,
                first_last_state=first_last_state,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            value_exprs = [
                self._scalar_to_sqlglot(lit.value) for lit in key.values
            ]
            in_expr = exp.In(this=col_expr, expressions=value_exprs)
            return exp.Not(this=in_expr) if key.negated else in_expr
        if isinstance(key, (
            AggregateKey, TransformKey, TimeTruncKey, StarKey,
        )):
            raise NotImplementedError(
                f"DEV-1450 stage 7b.10+: filter rendering for "
                f"{type(key).__name__} deferred to later slice."
            )
        raise NotImplementedError(
            f"Unsupported ValueKey type in filter: {type(key).__name__}",
        )

    def _render_filter_for_outer_wrapper(  # NOSONAR(S3776) — sequential isinstance dispatch over the closed filter-ValueKey union for the DEV-1503 outer-WHERE wrapper. Mirrors ``_render_value_key_for_filter`` shape but substitutes slot refs with the combined-SELECT's table-qualified columns (``_cm_*`` / ``_base``); per-branch helpers would scatter the substitution contract.
        self,
        *,
        key,
        slot_by_key: Dict[Any, Any],
        cross_model_agg_slot_to_cm: Dict[str, Tuple[str, str]],
        aliases_by_slot_id: Dict[str, List[str]],
    ) -> exp.Expression:
        """Render a ValueKey tree for the DEV-1503 outer combined-SELECT WHERE.

        Used when an AGGREGATE-phase host filter references a filtered-local
        isolated aggregate (``loss_payment_amt:sum > 1000`` where
        ``loss_payment_amt`` has a join-crossing ``Column.filter``). The
        filtered aggregate lives in a ``_cm_*`` CTE that LEFT JOINs back to
        ``_base``; the outer combined SELECT is non-aggregating, so the
        comparison renders as plain WHERE on the joined-back column rather
        than HAVING-into-the-CTE (which would surface host rows as NULL
        instead of dropping them).

        Slot-bearing leaves resolve via:

        * Isolated aggregate slot → ``<cte_name>."<agg_col_alias>"`` from
          ``cross_model_agg_slot_to_cm`` (the CTE's emitted aggregate column).
        * Any other slot (row column, joined dim, local aggregate operand)
          → ``_base."<first_alias>"`` from ``aliases_by_slot_id``. The
          generator's aux-slot pass (``_add_local_aux_slots(aggregates_only=
          True)``) promotes non-isolated aggregate operands into
          ``base_render_order`` so this lookup always succeeds.

        Cross-model aggregates with ``path != ()`` (forward-path) are not
        expected here — the planner routes those via plan-level
        ``where_filter_ids`` / ``having_filter_ids`` instead.
        """
        from decimal import Decimal

        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            LiteralKey,
            ScalarCallKey,
            StarKey,
            TimeTruncKey,
            TransformKey,
        )

        def _slot_alias_column(slot) -> Optional[exp.Expression]:
            sid = slot.id
            cm_entry = cross_model_agg_slot_to_cm.get(sid)
            if cm_entry is not None:
                cte_name, agg_col_alias = cm_entry
                return exp.Column(
                    this=exp.to_identifier(agg_col_alias, quoted=True),
                    table=exp.to_identifier(cte_name),
                )
            aliases = aliases_by_slot_id.get(sid) or []
            if not aliases:
                return None
            return exp.Column(
                this=exp.to_identifier(aliases[0], quoted=True),
                table=exp.to_identifier("_base"),
            )

        if isinstance(key, AggregateKey):
            slot = slot_by_key.get(key)
            if slot is not None:
                resolved = _slot_alias_column(slot)
                if resolved is not None:
                    return resolved
            raise NotImplementedError(
                f"DEV-1503 outer-WHERE wrapper: AggregateKey "
                f"{key!r} has no slot/alias resolution. "
                f"Forward-path cross-model aggregates route via plan "
                f"where/having ids instead.",
            )
        if isinstance(key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            slot = slot_by_key.get(key)
            if slot is not None:
                resolved = _slot_alias_column(slot)
                if resolved is not None:
                    return resolved
            raise NotImplementedError(
                f"DEV-1503 outer-WHERE wrapper: {type(key).__name__} "
                f"{key!r} has no base alias — operand promotion did not "
                f"materialise it in ``_base``.",
            )
        if isinstance(key, LiteralKey):
            return self._scalar_to_sqlglot(key.value)
        if isinstance(key, ArithmeticKey):
            operands = [
                self._render_filter_for_outer_wrapper(
                    key=o,
                    slot_by_key=slot_by_key,
                    cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                    aliases_by_slot_id=aliases_by_slot_id,
                )
                for o in key.operands
            ]
            return self._build_arithmetic_for_filter(
                op=key.op, operands=operands,
            )
        if isinstance(key, ScalarCallKey):
            args = []
            for a in key.args:
                if isinstance(a, (Decimal, str, bool)) or a is None:
                    args.append(self._scalar_to_sqlglot(a))
                else:
                    args.append(self._render_filter_for_outer_wrapper(
                        key=a,
                        slot_by_key=slot_by_key,
                        cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                        aliases_by_slot_id=aliases_by_slot_id,
                    ))
            # One ScalarCall policy everywhere (B5): typed node, dialect
            # rewrite, then the log-alias fix-up. This branch used to return an
            # ``exp.Anonymous`` passthrough for everything but ROUND, so a
            # filter emitted ``IFNULL(...)`` — which Postgres does not have —
            # while the same key emitted ``COALESCE(...)`` from a projection.
            #
            # The log fix-up is load-bearing: ``exp.func("LOG10", x)``
            # normalises to a generic ``Log(10, x)`` that re-emits as
            # ``LOG(10, x)``, wrong for dialects with a native single-arg
            # ``LOG10``. Transpiling alone fixes ifnull and breaks log10.
            return render_scalar_call(
                name=key.name, args=args, dialect=self._dialect,
            )
        if isinstance(key, BetweenKey):
            col_expr = self._render_filter_for_outer_wrapper(
                key=key.column,
                slot_by_key=slot_by_key,
                cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            low_expr = self._render_filter_for_outer_wrapper(
                key=key.low,
                slot_by_key=slot_by_key,
                cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            high_expr = self._render_filter_for_outer_wrapper(
                key=key.high,
                slot_by_key=slot_by_key,
                cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            return exp.Between(this=col_expr, low=low_expr, high=high_expr)
        if isinstance(key, InKey):
            col_expr = self._render_filter_for_outer_wrapper(
                key=key.column,
                slot_by_key=slot_by_key,
                cross_model_agg_slot_to_cm=cross_model_agg_slot_to_cm,
                aliases_by_slot_id=aliases_by_slot_id,
            )
            value_exprs = [
                self._scalar_to_sqlglot(lit.value) for lit in key.values
            ]
            in_expr = exp.In(this=col_expr, expressions=value_exprs)
            return exp.Not(this=in_expr) if key.negated else in_expr
        if isinstance(key, (TransformKey, StarKey)):
            raise NotImplementedError(
                f"DEV-1503 outer-WHERE wrapper: filter rendering for "
                f"{type(key).__name__} not supported on outer wrapper.",
            )
        raise NotImplementedError(
            f"DEV-1503 outer-WHERE wrapper: unsupported ValueKey type "
            f"{type(key).__name__}",
        )

    @staticmethod
    def _direct_local_column_keys(key) -> "List[Any]":
        """Local ``ColumnKey``s that appear as DIRECT (non-aggregated) operands
        of a predicate tree — used to reject a HAVING that compares an
        ungrouped row column. The walk stops at ``AggregateKey`` /
        ``TransformKey`` (their inner columns are aggregated, not grouped).
        """
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            InKey,
            ScalarCallKey,
            TransformKey,
        )

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

    @staticmethod
    def _scalar_to_sqlglot(v) -> exp.Expression:
        from decimal import Decimal

        if v is None:
            return exp.Null()
        if isinstance(v, bool):
            return exp.Boolean(this=v)
        if isinstance(v, Decimal):
            return exp.Literal.number(str(v))
        if isinstance(v, str):
            return exp.Literal.string(v)
        raise NotImplementedError(
            f"Unsupported scalar in filter: type={type(v).__name__} "
            f"value={v!r}",
        )

    @staticmethod
    def _paren_if_binary(node: exp.Expression) -> exp.Expression:
        """DEV-1539: wrap a multi-term operand in ``(...)`` when it is a
        ``Binary`` (arithmetic ``a + b``, or an ``AND``/``OR`` connector) so a
        surrounding comparator's precedence is explicit by inspection, not only
        by SQL operator-precedence rules — ``(a + b) > 7``, not ``a + b > 7``.
        Bare columns, literals, function calls, and already-enclosed forms
        (``CAST(...)`` / ``Paren``) are not ``Binary`` and pass through."""
        return exp.Paren(this=node) if isinstance(node, exp.Binary) else node

    @staticmethod
    def _build_arithmetic_for_filter(
        *, op: str, operands: list,
    ) -> exp.Expression:
        """Compose a WHERE / HAVING operator.

        Every operator but the comparisons delegates to the one composer in
        ``slayer.sql.render.value_expr``; the hand-rolled versions here emitted
        ``a + (b - c)`` as ``a + b - c``, a different number.

        Comparisons keep :meth:`_paren_if_binary` (DEV-1539): it parenthesises
        EVERY multi-term operand, so ``(a + b) > 7`` stays explicit by
        inspection rather than by precedence rules. That is strictly more
        grouping than the shared policy derives, never less, so it is a
        readability choice rather than a second correctness policy.
        """
        # DSL ``==``/``!=`` map to sqlglot EQ/NEQ; sqlglot then emits the
        # dialect-correct SQL operator (postgres ``=``/``!=``).
        _cmp = {
            "==": exp.EQ, "=": exp.EQ, "!=": exp.NEQ, "<>": exp.NEQ,
            "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
        }
        cmp_cls = _cmp.get(op)
        if cmp_cls is not None:
            return cmp_cls(
                this=SQLGenerator._paren_if_binary(operands[0]),
                expression=SQLGenerator._paren_if_binary(operands[1]),
            )
        return render_arithmetic(op, list(operands))

    def _build_outer_trim_wrap_sql(
        self,
        *,
        base_select: exp.Select,
        planned_query,
        source_relation: str,
        aliases_by_slot_id: Dict[str, List[str]],
        slots_by_id: Dict[str, Any],
        bundle,
    ) -> str:
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
        # ``_apply_order_limit_from_planned`` to apply ORDER BY / LIMIT /
        # OFFSET so the dialect-aware sqlglot emission path is shared.
        return self._apply_order_limit_from_planned(
            select=outer_select,
            planned_query=planned_query,
            source_relation=source_relation,
            slots_by_id=slots_by_id,
            source_model=None,
            bundle=bundle,
            aliases_by_slot_id=aliases_by_slot_id,
        ).sql(dialect=self.dialect, pretty=True)

    def _apply_order_limit_from_planned(  # NOSONAR(S3776) — per-order-entry slot-kind dispatch (hidden materialised aggregate vs hidden NYI vs declared public alias) plus LIMIT/OFFSET tail. Each branch is the per-kind resolution contract; extracting helpers would scatter the alias-lookup chain.
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
        """ORDER BY entries reference slot ids — resolve to the slot's
        public or materialised alias and emit ``ORDER BY
        "source_relation.alias" ASC|DESC`` (quoted-identifier form).

        DEV-1501: hidden AggregateKey slots that have been MATERIALISED
        in the base SELECT (via Change 2's aggregate-only walk over
        order/filter deps) resolve to their materialised full alias from
        ``aliases_by_slot_id``. This is called either on the inner base
        SELECT (when no outer wrap is needed) or on the outer wrap (when
        hidden materialised columns are trimmed). In the outer-wrap
        path, the inner subquery exposes the materialised alias as a
        column the outer SELECT can reference by quoted identifier.

        Hidden ROW / TransformKey / cross-model targets remain
        unsupported (``Change 2``'s ``aggregates_only=True`` keeps row
        targets out of ``base_render_order``, preserving today's
        ``NotImplementedError``).
        """
        from slayer.core.keys import (
            AggregateKey,
            ArithmeticKey,
            ColumnKey,
            ColumnSqlKey,
            ScalarCallKey,
            TimeTruncKey,
            TransformKey,
        )

        # DEV-1733: the EXACT set of hidden key kinds that resolve to a
        # materialised alias. Deliberately enumerated rather than "any hidden
        # slot that happens to carry an alias" — a hidden ROW slot with an
        # alias must still hit the split-emission / invariant branches below,
        # never be ordered on as a bare column that is not in the GROUP BY.
        _MATERIALISED_ORDER_KINDS = (
            AggregateKey, ArithmeticKey, ScalarCallKey, TransformKey,
        )

        for order_entry in planned_query.order:
            slot = slots_by_id.get(order_entry.slot_id)
            if slot is None:
                continue
            if slot.hidden:
                # DEV-1501: hidden AGGREGATE slots are now materialised
                # in the base SELECT (Change 2). Resolve to the
                # materialised full alias from ``aliases_by_slot_id`` and
                # reference it by quoted identifier — identical shape to
                # the non-hidden public-alias branch below.
                aliases = (
                    aliases_by_slot_id.get(slot.id, [])
                    if aliases_by_slot_id is not None
                    else []
                )
                if aliases and isinstance(slot.key, _MATERIALISED_ORDER_KINDS):
                    full_alias = aliases[0]
                    order_col = exp.Column(
                        this=exp.to_identifier(full_alias, quoted=True),
                    )
                    ascending = order_entry.direction == "asc"
                    select = select.order_by(
                        self._ordered(order_col, ascending=ascending),
                    )
                    continue
                # DEV-1712 (Law 2, split emission): a hidden ROW column ordered
                # in an UNGROUPED query. The plan-time order validation
                # (``plan_query``) guarantees the only hidden ROW slot that
                # reaches here is a bare column in a query with no GROUP BY —
                # grouped row columns are rejected or MAX-wrapped up front, and
                # aggregates take the branch above. Emit a SPLIT
                # ``<relation>.<column>`` reference (mixed-case-aware) against
                # the base FROM scope, identical to how the column would render
                # if it were a projected dimension.
                #
                # DEV-1703 Phase 1: a JOINED column is emitted the same way,
                # under its ``__`` path alias (``customers__regions.name``). The
                # row IS the grain in an ungrouped query, so the bare reference
                # is legal; Law 1 pulls the crossed join into the base FROM (see
                # ``_collect_joined_paths_for_base``, which walks order targets).
                key = slot.key
                row_key = key.column if isinstance(key, TimeTruncKey) else key
                if (
                    source_model is not None
                    and isinstance(row_key, ColumnKey)
                ):
                    order_col = self._joined_or_local_dim_expr(
                        path=row_key.path, leaf=row_key.leaf,
                        source_model=source_model,
                        source_relation=source_relation, bundle=bundle,
                    )
                    ascending = order_entry.direction == "asc"
                    select = select.order_by(
                        self._ordered(order_col, ascending=ascending),
                    )
                    continue
                # A LOCAL DERIVED column (``ColumnSqlKey``, path empty): resolve
                # its ``Column.sql`` through a throwaway host scope. That both
                # anchors the expansion AND surfaces whether the SQL crosses a
                # join. A hidden order-only derived column is NOT projected, so
                # its join was never pulled into the base FROM — ordering on it
                # would reference an unbound table. Reject that (project it),
                # rather than emit invalid SQL; a non-crossing derived column
                # (e.g. a bare mixed-case identifier) orders on its expression.
                if (
                    source_model is not None
                    and bundle is not None
                    and isinstance(row_key, ColumnSqlKey)
                    and not row_key.path
                ):
                    # Detect join crossing via a throwaway scope (register-only);
                    # the resolved expr is discarded — its expansion lacks the
                    # DEV-1645 mixed-case quoting the planned-dim helper applies.
                    allocator = self._new_allocator()
                    scope = ScopeFrame(
                        scope_id=allocator.next_scope_id(source_relation),
                        root_model=source_model,
                        root_relation=source_relation,
                        bundle=bundle,
                        dialect=self._dialect,
                        allocator=allocator,
                    )
                    scope.resolve(row_key)
                    if scope.join_paths:
                        # The derived column IS local (``orders.cust_region``);
                        # it merely depends on an unpulled join. Report its own
                        # qualified name, not a fabricated ``customers.cust_region``.
                        raise UnresolvableOrderColumnError(
                            column=row_key.column_name, qualifier=source_relation,
                        )
                    # Non-crossing local derived column — emit through the
                    # planned-dim helper so the expansion is quoted identically
                    # to a projected dimension (mixed-case-safe).
                    order_col = self._joined_or_local_dim_expr(
                        path=(), leaf=row_key.column_name,
                        source_model=source_model,
                        source_relation=source_relation, bundle=bundle,
                    )
                    ascending = order_entry.direction == "asc"
                    select = select.order_by(
                        self._ordered(order_col, ascending=ascending),
                    )
                    continue
                # Defensive: any other hidden shape should have been rejected at
                # plan time (transform / composite / joined / grouped-row).
                raise NotImplementedError(
                    f"ORDER BY references a hidden slot (id={slot.id!r}, key="
                    f"{type(slot.key).__name__}) that was not resolved at plan "
                    f"time — this is an internal invariant violation."
                )
            # DEV-1713: resolve to the SAME full alias the projection emits —
            # a joined ROW dimension projects under the DOTTED result key
            # (``orders.customers.regions.name``), so the ORDER BY must match
            # it, not the flat ``declared_name`` (``customers__regions__name``),
            # which would name a column the SELECT never projects.
            full_alias = self._full_alias_for_slot(
                slot=slot, source_relation=source_relation, alias_index={},
            )
            order_col = exp.Column(
                this=exp.to_identifier(full_alias, quoted=True),
            )
            ascending = order_entry.direction == "asc"
            select = select.order_by(
                self._ordered(order_col, ascending=ascending),
            )

        return self._dialect.apply_pagination(
            select, limit=planned_query.limit, offset=planned_query.offset,
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
    requested dialect and delegates to the instance method, which
    reuses the legacy dialect helpers (``_resolve_sql`` /
    ``_build_agg`` / ``_wrap_cast_for_type`` / ``_parse_predicate``)
    so dialect-specific behavior is rendered identically to the
    legacy ``SQLGenerator.generate()`` path.

    Stage 7b.8 scope: single-model queries with dimensions, local
    aggregates, Mode-B row filters, ORDER BY, LIMIT/OFFSET, and dim-
    only deduplication. Cross-model aggregates, time dimensions,
    window transforms, self-join CTE transforms, and HAVING-phase
    filters raise ``NotImplementedError`` with a stage marker so
    silent parity drift is impossible (slices 7b.9–7b.13 land each
    behavior in turn).
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
        sql = get_dialect(dialect).rewrite_emitted_sql(sql)
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
    sql = root_ast.sql(dialect=dialect, pretty=True)
    sql = get_dialect(dialect).rewrite_emitted_sql(sql)
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
