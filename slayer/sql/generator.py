"""SQL generator — converts EnrichedQuery to SQL via sqlglot AST.

The generator works exclusively with EnrichedQuery objects (fully resolved
SQL expressions). It never looks up model definitions — that's done by the
query engine's _enrich() step.
"""

import copy
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from slayer.core.enums import (
    BUILTIN_AGGREGATION_FORMULAS,
    BUILTIN_AGGREGATION_REQUIRED_PARAMS,
    DataType,
    TimeGranularity,
)
from pydantic import BaseModel, ConfigDict

from slayer.core.errors import AggregationNotAllowedError
from slayer.core.models import Aggregation
from slayer.core.refs import agg_kwarg_canonical_str
from slayer.engine.column_expansion import (
    _is_trivial_base,
    _root_scope_column_ids,
    expand_derived_refs_sync,
)
from slayer.engine.enriched import EnrichedMeasure, EnrichedQuery, public_projection_aliases
from slayer.engine.source_bundle import (
    stage_bundle_with_siblings,
    synthetic_model_from_stage_schema,
)
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.sql.sqlite_dialect import rewrite_sqlite_json_extract
from slayer.sql.stage_wrapper import build_flat_rename_wrapper


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

    agg_kwargs: Dict[str, str] = {}
    """Query-time aggregation parameter overrides (already stringified via
    ``agg_kwarg_canonical_str`` at spec-build time)."""

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


def _agg_render_spec_from_enriched(em: "EnrichedMeasure") -> AggRenderSpec:
    """Adapt a legacy ``EnrichedMeasure`` to the typed ``AggRenderSpec`` for
    the refactored dialect helpers (DEV-1452 Stage A).

    Pure field-mapping shim — drops the fields the helpers don't consume
    (``agg_args``, ``source_measure_name``, ``distinct``, ``window``,
    ``user_declared``, ``label``, ``filter_columns``). The legacy
    ``SQLGenerator.generate(enriched=...)`` path uses this to keep emitting
    byte-identical SQL through the refactored helpers; the shim is deleted
    in Stage D along with the rest of the legacy pipeline.
    """
    return AggRenderSpec(
        sql=em.sql,
        name=em.name,
        model_name=em.model_name,
        aggregation=em.aggregation,
        alias=em.alias,
        aggregation_def=em.aggregation_def,
        agg_kwargs=dict(em.agg_kwargs),
        filter_sql=em.filter_sql,
        time_column=em.time_column,
        type=em.type,
        column_type=em.column_type,
    )


def _wrap_cast_for_type(expr: exp.Expression, dt: Optional[DataType]) -> exp.Expression:
    """DEV-1361: wrap ``expr`` in ``CAST(expr AS <dialect-rendered dt>)`` so the
    declared SLayer ``DataType`` is enforced in emitted SQL.

    Skipped when ``dt`` is ``None`` (no declared type) or ``DataType.TEXT``
    (cosmetic — SQL TEXT/VARCHAR roundtripping is already a no-op for our
    purposes and ``CAST(... AS TEXT)`` does not unwrap SQLite's
    JSON-quoted-string return values anyway). Skipped when ``expr`` is a
    plain ``exp.Column`` (possibly qualified ``model.col``) — those are
    bare column references whose runtime type already matches the declared
    type by definition; wrapping them in CAST is dead noise and on SQLite
    can be lossy (e.g. ``CAST(text_timestamp AS TIMESTAMP)`` truncating
    to a year). Idempotent: if ``expr`` is already a CAST to the same
    target, return it unchanged.
    """
    if dt is None or dt == DataType.TEXT:
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
_BUILTIN_BAREARG_AGGS_LOCAL_SLICE: frozenset[str] = frozenset({
    "sum", "avg", "min", "max", "count", "count_distinct", "median",
    "percentile", "weighted_avg",
    "corr", "covar_samp", "covar_pop",
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "first", "last",
})

# DEV-1337: dialects with native single-arg `log10(x)` / `log2(x)`. sqlglot
# normalises both into a generic ``Log(this=Literal(base), expression=arg)``
# AST and re-emits as ``LOG(base, x)`` for almost every dialect, which
# diverges from the recipe formula text and (on dialects without 2-arg
# ``LOG``) can break a previously working call. We rewrite the AST back
# to ``Anonymous(this='log10'|'log2', ...)`` for the dialects below;
# unsupported dialects (oracle; tsql for log2) keep the canonical 2-arg
# form. Mirrored in tests/test_sql_generator.py — keep in sync.
_LOG10_NATIVE_DIALECTS: frozenset[str] = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "snowflake", "bigquery", "redshift",
    "trino", "presto", "databricks", "spark", "tsql",
})
_LOG2_NATIVE_DIALECTS: frozenset[str] = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "bigquery", "trino", "presto", "databricks", "spark",
})

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

_WINDOW_DURATION_RE = re.compile(r"(?P<num>\d+)(?P<unit>min|[ymwdhs])")
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


def _has_cross_model_filter(m: EnrichedMeasure) -> bool:
    """Check if a measure's filter references a cross-model dimension.

    Local columns are qualified as "model.column" by resolve_filter_columns.
    Cross-model columns have a different prefix (e.g., "loss_payment.has_flag").
    We detect cross-model by checking if any dotted column's prefix differs
    from the measure's own model_name.
    """
    if not m.filter_columns:
        return False
    for col in m.filter_columns:
        if "." not in col:
            continue
        prefix = col.rsplit(".", 1)[0]
        # "__" in prefix means a multi-hop join path (always cross-model)
        if "__" in prefix:
            return True
        # Single segment prefix: cross-model if it's not the measure's model
        if prefix != m.model_name:
            return True
    return False


def _is_windowed_measure(m: EnrichedMeasure) -> bool:
    return bool(m.window)


def _parse_window_duration(value: str) -> list[tuple[int, str]]:
    """Parse compact durations like 1y2m3w5d6h7min8s."""
    if not value:
        raise ValueError("Window duration cannot be empty")
    pos = 0
    parts: list[tuple[int, str]] = []
    for match in _WINDOW_DURATION_RE.finditer(value):
        if match.start() != pos:
            raise ValueError(
                f"Invalid window duration '{value}'. Use syntax like '1y2m3w5d6h7min8s'."
            )
        amount = int(match.group("num"))
        unit = match.group("unit")
        if amount <= 0:
            raise ValueError(f"Window duration parts must be positive in '{value}'")
        parts.append((amount, unit))
        pos = match.end()
    if pos != len(value) or not parts:
        raise ValueError(
            f"Invalid window duration '{value}'. Use syntax like '1y2m3w5d6h7min8s'."
        )
    return parts


def _cte_name_from_alias(prefix: str, alias: str) -> str:
    """Build a unique CTE name from a measure alias.

    Dots are replaced with ``__`` (double underscore) to avoid collision
    with aliases that already contain underscores. E.g.:
    - ``orders.revenue_sum``  -> ``_fm_orders__revenue_sum``
    - ``orders_v2.revenue_sum`` -> ``_fm_orders_v2__revenue_sum``
    """
    sanitized = alias.replace(".", "__")
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized)
    return prefix + sanitized


def _alias_prefixes(model_name: str) -> list:
    """'a__b__c' → ['a', 'a__b', 'a__b__c']"""
    parts = model_name.split("__")
    return ["__".join(parts[: i + 1]) for i in range(len(parts))]


def _filter_dotted_columns(filters) -> list[str]:
    """Yield each "__"-joined path-alias prefix referenced by every non-post
    filter's dotted column.

    A filter on `a.b.c` produces ['a', 'a__b'] — the path-alias forms that
    correspond to the joins required to evaluate the filter. Used by window
    CTE pruning to keep filter-driven joins.
    """
    out: list[str] = []
    for f in filters:
        if getattr(f, "is_post_filter", False):
            continue
        for col in f.columns:
            if "." not in col:
                continue
            parts = col.split(".")
            for i in range(1, len(parts)):
                out.append("__".join(parts[:i]))
    return out


def _needed_join_aliases(enriched: EnrichedQuery, extra_columns: list = ()) -> set:
    """Compute which resolved_join aliases are needed for dimensions + extra dotted columns."""
    aliases: set = set()
    for dim in enriched.dimensions:
        if dim.model_name != enriched.model_name:
            aliases.update(_alias_prefixes(dim.model_name))
    for td in enriched.time_dimensions:
        if td.model_name != enriched.model_name:
            aliases.update(_alias_prefixes(td.model_name))
    for col in extra_columns:
        if "." in col:
            parts = col.split(".")
            for i in range(1, len(parts)):
                aliases.add("__".join(parts[:i]))
    return aliases


def _filter_references_available(f, available_aliases: set) -> bool:
    """Check if all table references in a filter's columns are within a CTE's join set.

    Non-dotted columns (local to the base model) are always available.
    Dotted columns like "warehouse.status" produce alias "warehouse" which
    must be in available_aliases.
    """
    for col in f.columns:
        if "." not in col:
            continue
        parts = col.split(".")
        table_alias = "__".join(parts[:-1])
        if table_alias not in available_aliases:
            return False
    return True


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

    def __init__(self, dialect: str = "postgres"):
        self.dialect = dialect

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
        tree = sqlglot.parse_one(sql, dialect=d)
        if d == "sqlite":
            tree = rewrite_sqlite_json_extract(tree)
        # Log-alias rewrite is multi-dialect; the per-base allowlist check
        # lives inside ``_rewrite_log_aliases`` so unsupported dialects
        # (oracle; tsql for log2) keep the canonical 2-arg LOG form.
        return tree.transform(self._rewrite_log_aliases)

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
        wrapped = sqlglot.parse_one(f"SELECT 1 WHERE {sql}", dialect=d)
        where = wrapped.args.get("where")
        if where is None or where.this is None:  # pragma: no cover — defensive
            raise ValueError(
                f"Could not extract WHERE predicate from {sql!r} (dialect={d!r})"
            )
        tree = where.this
        if d == "sqlite":
            tree = rewrite_sqlite_json_extract(tree)
        return tree.transform(self._rewrite_log_aliases)

    def generate(
        self,
        enriched: EnrichedQuery,
        *,
        render_mode: str = "outer",
    ) -> str:
        """Generate SQL from a fully resolved EnrichedQuery.

        Architecture:
        1. Base CTE: simple (non-isolated) measures + dimensions
        2. Per-measure CTEs: cross-model measures + cross-model-filtered measures
        3. Combined: LEFT JOIN base + measure CTEs on shared dimensions
        4. Expressions/transforms stacked on top of combined

        Args:
            enriched: Fully resolved query.
            render_mode: ``"outer"`` (default) — SQL will be executed and
                shown to the user; the outermost SELECT is trimmed to
                ``public_projection_aliases(enriched)`` (DEV-1444).
                ``"wrapped"`` — SQL is embedded into a larger structure
                (``_query_as_model`` inner_sql, inner stages of
                ``source_queries``); the outer SELECT keeps every alias
                downstream references can reach.
        """
        if render_mode not in ("outer", "wrapped"):
            raise ValueError(
                f"render_mode must be 'outer' or 'wrapped', got {render_mode!r}"
            )
        has_isolated = any(_has_cross_model_filter(m) for m in enriched.measures)
        has_windowed = any(_is_windowed_measure(m) for m in enriched.measures)
        has_cross_model = bool(enriched.cross_model_measures)
        has_measure_ctes = has_isolated or has_cross_model or has_windowed
        has_computed = bool(enriched.expressions or enriched.transforms)
        # DEV-1336: a post-filter on a windowed `Column.sql` (or any other
        # post-classified filter) requires the outer `_filtered` wrap from
        # `_generate_with_computed`, even when there are no expressions or
        # transforms to layer.
        has_post_filters = any(getattr(f, "is_post_filter", False) for f in enriched.filters)

        base_sql = self._generate_base(enriched=enriched, skip_isolated=has_measure_ctes)

        if not has_measure_ctes and not has_computed and not has_post_filters:
            sql = base_sql
        elif has_measure_ctes:
            # Get structured CTE definitions (no WITH wrapper)
            measure_ctes = self._build_combined(enriched=enriched, base_sql=base_sql)
            if has_computed or has_post_filters:
                # Pass CTE list to computed layer — it merges into a flat WITH
                sql = self._generate_with_computed(enriched=enriched, prefix_ctes=measure_ctes)
            else:
                # No expressions: assemble CTEs + outer SELECT + pagination
                sql = self._assemble_combined_sql(enriched=enriched, measure_ctes=measure_ctes)
        else:
            # No measure CTEs, just computed columns or post-filters
            sql = self._generate_with_computed(enriched=enriched, base_sql=base_sql)

        if render_mode == "outer":
            sql = self._apply_outer_projection_trim(sql=sql, enriched=enriched)
        return sql

    def _apply_outer_projection_trim(
        self, *, sql: str, enriched: EnrichedQuery,
    ) -> str:
        """DEV-1444: wrap ``sql`` so its outermost SELECT projects exactly
        the user-declared ``public_projection_aliases`` of ``enriched``,
        in declared order.

        When the inner SELECT already projects exactly the public list,
        the trim is a no-op (``sql`` returned unchanged). Otherwise an
        outer wrapper is emitted::

            SELECT <public_aliases>
            FROM   (<inner sql, with ORDER/LIMIT/OFFSET moved out>) AS _outer
            ORDER BY ... LIMIT N OFFSET M

        Moving ORDER BY / LIMIT / OFFSET to the outer wrapper preserves the
        rendered SQL's row-ordering contract while keeping every hoisted
        intermediate accessible inside the subquery scope (so the ORDER BY
        can still reference a hidden alias like ``"orders.revenue_sum"``
        when no matching declared measure exists).
        """
        public = public_projection_aliases(enriched)
        if not public:
            return sql
        parsed = self._safe_parse_outer(sql)
        if parsed is None:
            return sql
        # Fast path: when the inner SELECT already projects exactly the
        # public alias list (in order), no wrapper is needed.
        inner_aliases = [n.alias_or_name for n in parsed.expressions]
        if inner_aliases == public:
            return sql
        # Detach ORDER BY / LIMIT / OFFSET from the inner so the outer
        # wrapper can own them; the FROM-subquery scope exposes every
        # alias their references may need.
        order = parsed.args.pop("order", None)
        limit = parsed.args.pop("limit", None)
        offset_arg = parsed.args.pop("offset", None)
        return self._build_outer_wrap(
            inner_sql=sql,
            public=public,
            order=order,
            limit=limit,
            offset_arg=offset_arg,
        )

    def _safe_parse_outer(self, sql: str):
        """Parse ``sql`` via the generator's ``_parse`` (so AST rewrites
        like LOG10/LOG2 alias preservation survive a round-trip).
        Returns the ``exp.Select`` root or ``None`` when parsing fails
        or the root isn't a Select — both signals tell the trim caller
        to leave ``sql`` untouched.
        """
        try:
            parsed = self._parse(sql)
        except Exception:
            return None
        if not isinstance(parsed, exp.Select):
            return None
        return parsed

    def _build_outer_wrap(
        self,
        *,
        inner_sql: str,
        public: List[str],
        order,
        limit,
        offset_arg,
    ) -> str:
        """Emit ``SELECT <public> FROM (<inner>) AS _outer [ORDER/LIMIT/OFFSET]``.

        ``inner_sql`` is used as-is to preserve its formatting (callers
        diff against literal ``OVER (...)`` substrings). Trailing
        ORDER/LIMIT/OFFSET segments are stripped from ``inner_sql`` and
        re-emitted on the outer wrapper.
        """
        outer_select = _SQL_COL_SEP.join(f'"{a}"' for a in public)
        if order is None and limit is None and offset_arg is None:
            return (
                f"SELECT\n    {outer_select}\n"
                f"FROM (\n{inner_sql.rstrip()}\n) AS _outer"
            )
        inner_no_pag = _strip_trailing_pagination(inner_sql)
        out = (
            f"SELECT\n    {outer_select}\n"
            f"FROM (\n{inner_no_pag.rstrip()}\n) AS _outer"
        )
        if order is not None:
            # DEV-1444 (Codex review on PR #134): the detached ORDER BY
            # may carry inner-CTE qualifiers like ``_base."col"`` from
            # ``_assemble_combined_sql``; those don't resolve at the
            # outer wrapper level (only ``_outer`` is in scope). Strip
            # every Column's table qualifier — the outer scope exposes
            # each column by its bare alias name.
            for col in order.find_all(exp.Column):
                if col.args.get("table") is not None:
                    col.set("table", None)
            out += "\n" + order.sql(dialect=self.dialect, pretty=True)
        if limit is not None:
            out += "\n" + limit.sql(dialect=self.dialect, pretty=True)
        if offset_arg is not None:
            out += "\n" + offset_arg.sql(dialect=self.dialect, pretty=True)
        return out

    def _build_combined(self, enriched: EnrichedQuery,
                         base_sql: str) -> list[tuple[str, str]]:
        """Build CTE definitions for per-measure isolation.

        Returns a list of (name, sql) tuples. The last entry is ("_combined", select)
        which joins _base with all measure CTEs on shared dimensions. The caller
        decides how to assemble these — either as a standalone WITH query or as
        prefix CTEs for _generate_with_computed().
        """
        ctes = [("_base", base_sql)]

        # Collect dimension aliases for JOIN conditions
        dim_aliases = [d.alias for d in enriched.dimensions]
        td_aliases = [td.alias for td in enriched.time_dimensions]
        join_aliases = dim_aliases + td_aliases

        # Track all CTEs and their measure aliases
        # Each entry: (cte_name, measure_alias, cte_join_aliases)
        # cte_join_aliases is None to use the default join_aliases, or a list
        # of surviving aliases when the CTE has fewer dimensions.
        measure_cte_refs = []

        # --- Cross-model measure CTEs ---
        seen_cm_ctes: set = set()
        for cm in enriched.cross_model_measures:
            cte_name = _cte_name_from_alias("_cm_", cm.alias)
            if cte_name in seen_cm_ctes:
                measure_cte_refs.append((cte_name, cm.alias, None))
                continue
            seen_cm_ctes.add(cte_name)

            if cm.rerooted_enriched is not None:
                # Re-rooted subquery: full query with target model as source,
                # all joins/filters resolved from the target's join graph.
                cte_sql = self._generate_base(enriched=cm.rerooted_enriched)
                ctes.append((cte_name, cte_sql))
                # Surviving dims may be fewer than shared dims (unreachable dropped)
                surviving = (
                    [d.alias for d in cm.rerooted_enriched.dimensions]
                    + [td.alias for td in cm.rerooted_enriched.time_dimensions]
                )
                measure_cte_refs.append((cte_name, cm.alias, surviving))
                continue
            else:
                # Fallback: minimal source→target CTE (legacy path)
                select = exp.Select()
                group_exprs = []

                for dim in cm.shared_dimensions:
                    col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=cm.source_model_name, type=dim.type)
                    select = select.select(col_expr.as_(dim.alias))
                    group_exprs.append(col_expr)
                for td in cm.shared_time_dimensions:
                    col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=cm.source_model_name)
                    td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
                    select = select.select(td_expr.as_(td.alias))
                    group_exprs.append(td_expr)

                agg_expr, _ = self._build_agg(_agg_render_spec_from_enriched(cm.measure))
                # DEV-1361: cast the cross-model agg result if a result type
                # was declared on the source ModelMeasure.
                agg_expr = _wrap_cast_for_type(agg_expr, cm.measure.type)
                select = select.select(agg_expr.as_(cm.alias))

                # FROM source model
                if cm.source_sql:
                    source_from = exp.Subquery(
                        this=self._parse(cm.source_sql),
                        alias=exp.to_identifier(cm.source_model_name),
                    )
                else:
                    source_from = exp.to_table(cm.source_sql_table, alias=cm.source_model_name)
                select = select.from_(source_from)

                # JOIN target model
                if cm.target_model_sql:
                    target_join = exp.Subquery(
                        this=self._parse(cm.target_model_sql),
                        alias=exp.to_identifier(cm.target_model_name),
                    )
                else:
                    target_join = exp.to_table(cm.target_model_sql_table, alias=cm.target_model_name)
                join_on = exp.and_(*(
                    exp.EQ(
                        this=exp.Column(this=exp.to_identifier(src), table=exp.to_identifier(cm.source_model_name)),
                        expression=exp.Column(this=exp.to_identifier(tgt), table=exp.to_identifier(cm.target_model_name)),
                    )
                    for src, tgt in cm.join_pairs
                ))
                select = select.join(target_join, on=join_on, join_type=cm.join_type.upper())

                # Only include WHERE conditions whose tables are in this CTE
                cm_available = {cm.source_model_name, cm.target_model_name}
                original_filters = enriched.filters
                enriched.filters = [f for f in original_filters
                                    if _filter_references_available(f, cm_available)]
                where_clause, _ = self._build_where_and_having(enriched=enriched)
                enriched.filters = original_filters
                if where_clause is not None:
                    select = select.where(where_clause)
                for gb in group_exprs:
                    select = select.group_by(gb)

                ctes.append((cte_name, select.sql(dialect=self.dialect)))
                measure_cte_refs.append((cte_name, cm.alias, None))

        # --- Windowed aggregation CTEs ---
        for measure in enriched.measures:
            if not _is_windowed_measure(measure):
                continue
            cte_name = _cte_name_from_alias("_wm_", measure.alias)
            ctes.append((cte_name, self._generate_window_measure_cte(enriched=enriched, measure=measure)))
            measure_cte_refs.append((cte_name, measure.alias, None))

        # --- Isolated filtered-measure CTEs ---
        for measure in enriched.measures:
            if not _has_cross_model_filter(measure):
                continue
            cte_name = _cte_name_from_alias("_fm_", measure.alias)

            # Measure aggregation without CASE WHEN (the join IS the filter)
            unfiltered = copy.copy(measure)
            unfiltered.filter_sql = None
            unfiltered.filter_columns = []

            # Only include dimension joins + this measure's filter joins
            needed = _needed_join_aliases(enriched, extra_columns=measure.filter_columns)

            is_first_or_last = measure.aggregation in ("first", "last")

            if is_first_or_last and enriched.last_agg_time_column:
                # Build a ranked subquery within this CTE so _last_rn/_first_rn
                # columns exist for the MAX(CASE WHEN _rn = 1 ...) aggregate.
                scoped = copy.copy(enriched)
                scoped.measures = [unfiltered]
                scoped.resolved_joins = [
                    (t, a, c, j) for t, a, c, j in enriched.resolved_joins
                    if a in needed
                ]
                fm_available = needed | {enriched.model_name}
                scoped.filters = [
                    f for f in enriched.filters
                    if not f.is_post_filter and _filter_references_available(f, fm_available)
                ]

                from_clause = self._build_from_clause(enriched=enriched)
                (
                    ranked_from,
                    rn_suffix_map,
                    _filtered_rn_map,
                    _filtered_match_map,
                ) = self._build_last_ranked_from(
                    enriched=scoped, base_from=from_clause,
                )

                select = exp.Select()
                group_exprs: list[exp.Expression] = []
                # Dimensions are already resolved inside the ranked subquery
                for dim in enriched.dimensions:
                    col_expr = exp.Column(this=exp.to_identifier(dim.name))
                    select = select.select(col_expr.as_(dim.alias))
                    group_exprs.append(col_expr)
                for td in enriched.time_dimensions:
                    col_expr = exp.Column(this=exp.to_identifier(f"_td_{td.name}"))
                    select = select.select(col_expr.as_(td.alias))
                    group_exprs.append(col_expr)

                agg_expr, _ = self._build_agg(
                    _agg_render_spec_from_enriched(unfiltered),
                    rn_suffix_map=rn_suffix_map,
                    default_time_col=enriched.last_agg_time_column,
                )
                agg_expr = _wrap_cast_for_type(agg_expr, measure.type)
                select = select.select(agg_expr.as_(measure.alias))
                select = select.from_(ranked_from)
                # WHERE already inside ranked subquery
            else:
                # Standard aggregation (sum, avg, etc.)
                select = exp.Select()
                group_exprs = []
                for dim in enriched.dimensions:
                    col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name, type=dim.type)
                    select = select.select(col_expr.as_(dim.alias))
                    group_exprs.append(col_expr)
                for td in enriched.time_dimensions:
                    col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
                    td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
                    select = select.select(td_expr.as_(td.alias))
                    group_exprs.append(td_expr)

                agg_expr, _ = self._build_agg(_agg_render_spec_from_enriched(unfiltered))
                agg_expr = _wrap_cast_for_type(agg_expr, measure.type)
                select = select.select(agg_expr.as_(measure.alias))

                from_clause = self._build_from_clause(enriched=enriched)
                select = select.from_(from_clause)

                for target_table, target_alias, join_cond, jtype in enriched.resolved_joins:
                    if target_alias in needed:
                        if target_table.startswith("("):
                            join_target = exp.Subquery(
                                this=self._parse(target_table),
                                alias=exp.to_identifier(target_alias),
                            )
                        else:
                            join_target = exp.to_table(target_table, alias=target_alias)
                        join_on = self._parse(join_cond)
                        select = select.join(join_target, on=join_on, join_type=jtype.upper())

                # Only include WHERE conditions whose tables are in this CTE
                fm_available = needed | {enriched.model_name}
                original_filters = enriched.filters
                enriched.filters = [f for f in original_filters
                                    if _filter_references_available(f, fm_available)]
                where_clause, _ = self._build_where_and_having(enriched=enriched)
                enriched.filters = original_filters
                if where_clause is not None:
                    select = select.where(where_clause)

            for gb in group_exprs:
                select = select.group_by(gb)

            ctes.append((cte_name, select.sql(dialect=self.dialect)))
            measure_cte_refs.append((cte_name, measure.alias, None))

        # --- Build combined SELECT: _base LEFT JOIN measure CTEs ---
        base_cols = list(dim_aliases) + list(td_aliases)
        for m in enriched.measures:
            if not _has_cross_model_filter(m) and not _is_windowed_measure(m):
                base_cols.append(m.alias)
        final_parts = [f'_base."{a}"' for a in base_cols]
        for cte_name, alias, _ in measure_cte_refs:
            final_parts.append(f'{cte_name}."{alias}"')

        from_clause_str = "FROM _base"
        joined_ctes: set = set()
        for cte_name, _, cte_join_aliases in measure_cte_refs:
            if cte_name in joined_ctes:
                continue
            joined_ctes.add(cte_name)

            # Use per-CTE join aliases when available (re-rooted CTEs may
            # have fewer dims than the main query if some were unreachable).
            effective_aliases = cte_join_aliases if cte_join_aliases is not None else join_aliases
            join_on_parts = []
            for a in effective_aliases:
                join_on_parts.append(f'_base."{a}" = {cte_name}."{a}"')
            if join_on_parts:
                from_clause_str += f"\nLEFT JOIN {cte_name} ON {' AND '.join(join_on_parts)}"
            else:
                from_clause_str += f"\nCROSS JOIN {cte_name}"

        combined_select = (
            f"SELECT {', '.join(final_parts)}\n"
            f"{from_clause_str}"
        )
        ctes.append(("_combined", combined_select))
        return ctes

    def _assemble_combined_sql(self, enriched: EnrichedQuery,
                                measure_ctes: list[tuple[str, str]]) -> str:
        """Assemble measure CTEs into final SQL with pagination.

        The last entry in measure_ctes is the combined SELECT that joins _base
        with measure CTEs. Earlier entries become WITH clauses.
        """
        inner_ctes = measure_ctes[:-1]
        combined_select = measure_ctes[-1][1]

        cte_strs = [f"{name} AS (\n{sql}\n)" for name, sql in inner_ctes]
        sql = f"WITH {', '.join(cte_strs)}\n{combined_select}"

        # ORDER BY: use _base. for dimensions (ambiguous across CTEs),
        # bare alias for measure CTE columns (not in _base)
        if enriched.order:
            order_parts = []
            base_cols = set(d.alias for d in enriched.dimensions) | set(td.alias for td in enriched.time_dimensions)
            base_cols |= {
                m.alias for m in enriched.measures
                if not _has_cross_model_filter(m) and not _is_windowed_measure(m)
            }
            for order_item in enriched.order:
                col = order_item.column
                col_name = self._resolve_order_column(col=col, enriched=enriched)
                direction = "ASC" if order_item.direction == "asc" else "DESC"
                if col_name in base_cols:
                    order_parts.append(f'_base."{col_name}" {direction}')
                else:
                    order_parts.append(f'"{col_name}" {direction}')
            sql += "\nORDER BY " + ", ".join(order_parts)
        if enriched.limit is not None:
            sql += f"\nLIMIT {enriched.limit}"
        if enriched.offset is not None:
            sql += f"\nOFFSET {enriched.offset}"

        return sql

    @staticmethod
    def _apply_pagination_to_sql(enriched: EnrichedQuery, sql: str) -> str:
        """Apply ORDER BY, LIMIT, OFFSET to a raw SQL string."""
        if enriched.order:
            order_parts = []
            for order_item in enriched.order:
                col = order_item.column
                col_name = SQLGenerator._resolve_order_column(col=col, enriched=enriched)
                direction = "ASC" if order_item.direction == "asc" else "DESC"
                order_parts.append(f'"{col_name}" {direction}')
            sql += "\nORDER BY " + ", ".join(order_parts)
        if enriched.limit is not None:
            sql += f"\nLIMIT {enriched.limit}"
        if enriched.offset is not None:
            sql += f"\nOFFSET {enriched.offset}"
        return sql

    def _generate_shifted_base(self, enriched: EnrichedQuery, transform) -> str:
        """Generate a shifted sub-query for a time_shift transform.

        Shifts the time dimension column expression by -offset so that the
        WHERE, SELECT, and GROUP BY all reference shifted time. Only includes
        the target measure (not all measures).

        For example, time_shift(revenue:sum, -1, 'month') with date_range
        [2024-03-01, 2024-03-31] produces a sub-query where the time column
        is (created_at + INTERVAL '1' MONTH). This makes the WHERE fetch
        February data and the GROUP BY bucket it into March, aligning with
        the base query for a simple equality join.
        """
        # Determine granularity: explicit or from time dim
        gran = transform.granularity
        if not gran:
            for td in enriched.time_dimensions:
                if td.alias == transform.time_alias:
                    gran = td.granularity.value
                    break
            if not gran:
                gran = "month"

        # Find target measure
        target_measure = next(
            (m for m in enriched.measures if m.alias == transform.measure_alias),
            None,
        )
        if target_measure is None:
            raise ValueError(
                f"time_shift target measure '{transform.measure_alias}' not found "
                f"in enriched query measures"
            )

        # Create shifted time dimensions with offset baked into td.sql
        shifted_tds = []
        time_col_map: dict[str, str] = {}  # original_qualified → shifted_sql
        for td in enriched.time_dimensions:
            shifted_td = copy.copy(td)
            raw_sql = td.sql or td.name
            raw_expr = self._resolve_sql(sql=raw_sql, name=td.name, model_name=td.model_name)
            shifted_expr = self._build_time_offset_expr(
                col_expr=raw_expr, offset=-transform.offset, granularity=gran,
            )
            shifted_td.sql = shifted_expr.sql(dialect=self.dialect)
            shifted_tds.append(shifted_td)
            # Track for filter substitution
            original_qualified = f"{enriched.model_name}.{td.name}"
            time_col_map[original_qualified] = shifted_td.sql

        # Substitute time column references in filter SQL strings
        shifted_filters = []
        for f in enriched.filters:
            if f.is_post_filter:
                continue
            sf = copy.copy(f)
            for orig, shifted_sql in time_col_map.items():
                sf.sql = sf.sql.replace(orig, f"({shifted_sql})")
            shifted_filters.append(sf)

        # Build minimal enriched query with only the target measure
        shifted_enriched = EnrichedQuery(
            model_name=enriched.model_name,
            sql_table=enriched.sql_table,
            sql=enriched.sql,
            resolved_joins=enriched.resolved_joins,
            dimensions=list(enriched.dimensions),
            measures=[target_measure],
            time_dimensions=shifted_tds,
            filters=shifted_filters,
        )
        return self._generate_base(enriched=shifted_enriched)

    def _build_time_offset_expr(self, col_expr: exp.Expression, offset: int,
                                granularity: str) -> exp.Expression:
        """Apply a time offset to a column expression (dialect-aware).

        Used to shift raw timestamps before DATE_TRUNC in shifted CTEs so that
        aggregated time buckets align with the base query's buckets.
        """
        unit_map = {"year": "YEAR", "month": "MONTH", "day": "DAY",
                    "quarter": "MONTH", "week": "WEEK", "hour": "HOUR",
                    "minute": "MINUTE", "second": "SECOND"}
        unit = unit_map.get(granularity, granularity.upper())
        val = offset * 3 if granularity == "quarter" else offset

        if self.dialect == "sqlite":
            sqlite_units = {"YEAR": "years", "MONTH": "months", "DAY": "days",
                            "WEEK": "days", "HOUR": "hours", "MINUTE": "minutes",
                            "SECOND": "seconds"}
            sqlite_unit = sqlite_units.get(unit, unit.lower() + "s")
            sqlite_val = val * 7 if granularity == "week" else val
            return exp.Anonymous(
                this="DATE",
                expressions=[col_expr, exp.Literal.string(f"{sqlite_val} {sqlite_unit}")],
            )

        # Standard SQL: col ± INTERVAL N UNIT (single-unit; sqlglot transpiles
        # to the dialect-correct form, e.g. MySQL `INTERVAL N UNIT`,
        # ClickHouse same, BigQuery same).
        if val >= 0:
            return exp.Add(this=col_expr, expression=exp.Interval(
                this=exp.Literal.number(val), unit=exp.Var(this=unit),
            ))
        return exp.Sub(this=col_expr, expression=exp.Interval(
            this=exp.Literal.number(-val), unit=exp.Var(this=unit),
        ))

    def _duration_interval_exprs(self, duration: str, sign: int = 1) -> list[exp.Expression]:
        """Return per-unit AST nodes that `_add_intervals_expr` will chain.

        Non-SQLite: one positive `exp.Interval` per parsed (amount, unit) pair.
        The Add-vs-Sub direction is decided by `_add_intervals_expr` from its
        own `sign` arg, not baked into the Interval — sqlglot transpiles each
        single-unit interval per dialect (MySQL: `INTERVAL N UNIT`;
        ClickHouse: same; BigQuery: same), avoiding the broken Postgres-shape
        multi-unit literal `INTERVAL '1 year 2 month 3 day'` that fails on
        every Tier-1+ non-SQLite/non-Postgres dialect.

        SQLite: one DATETIME-modifier string literal per pair, sign baked in.
        Week is converted to `N*7 days` (SQLite has no week unit).
        """
        parts = _parse_window_duration(duration)
        if self.dialect == "sqlite":
            prefix = "+" if sign >= 0 else "-"
            return [
                exp.Literal.string(
                    f"{prefix}{(amount * 7 if unit == 'w' else amount)} "
                    f"{_WINDOW_UNIT_SQLITE[unit]}"
                )
                for amount, unit in parts
            ]
        return [
            exp.Interval(
                this=exp.Literal.number(amount),
                unit=exp.Var(this=_WINDOW_UNIT_SQL[unit].upper()),
            )
            for amount, unit in parts
        ]

    def _granularity_interval_expr(self, granularity: TimeGranularity, sign: int = 1) -> list[exp.Expression]:
        if granularity == TimeGranularity.QUARTER:
            duration = "3m"
        elif granularity == TimeGranularity.WEEK:
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

        SQLite: wraps as `DATETIME(expr, mod1, mod2, ...)` (sign baked into
        each modifier by `_duration_interval_exprs`); the `sign` arg is
        ignored on SQLite.
        Other dialects: chains `exp.Add` (sign>=0) or `exp.Sub` (sign<0). The
        result transpiles per dialect via sqlglot — MySQL renders
        `INTERVAL N UNIT` clauses unquoted, ClickHouse same, etc.
        """
        if self.dialect == "sqlite":
            return exp.Anonymous(this="DATETIME", expressions=[expr, *intervals])
        op_cls = exp.Add if sign >= 0 else exp.Sub
        result = expr
        for iv in intervals:
            result = op_cls(this=result, expression=iv)
        return result

    def _build_window_source_cols(
        self,
        *,
        enriched: EnrichedQuery,
        td,
        measure: EnrichedMeasure,
    ) -> tuple[list[exp.Alias], list[exp.Condition]]:
        """Build the SELECT columns and base equality predicates for the _src subquery.

        The trailing-window range predicate (`_src._w_time >= ...`) is added later
        by the caller; only the equality joins on dims and other time dims are
        produced here.

        Returns (source_cols, join_eqs) where source_cols are alias-wrapped
        expressions ready to feed `exp.Select.select(...)` and join_eqs are
        `exp.EQ` predicates ready to combine with `exp.and_`.
        """
        source_cols: list[exp.Alias] = []
        join_eqs: list[exp.Condition] = []

        def _src_col(name: str) -> exp.Column:
            return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier("_src"))

        def _base_col(alias: str) -> exp.Column:
            return exp.Column(this=exp.to_identifier(alias), table=exp.to_identifier("_base"))

        for idx, dim in enumerate(enriched.dimensions):
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name, type=dim.type)
            src_alias = f"_w_dim_{idx}"
            source_cols.append(col_expr.as_(src_alias))
            join_eqs.append(exp.EQ(this=_src_col(src_alias), expression=_base_col(dim.alias)))

        # Equality-join on every other time dim so the trailing window does not
        # fan out across their values when the query has 2+ time dimensions.
        for idx, other_td in enumerate(enriched.time_dimensions):
            if other_td.alias == td.alias:
                continue
            other_expr = self._resolve_sql(
                sql=other_td.sql or other_td.name,
                name=other_td.name,
                model_name=other_td.model_name,
            )
            other_bucket = self._build_date_trunc(
                col_expr=other_expr,
                granularity=other_td.granularity,
            )
            other_alias = f"_w_td_{idx}"
            source_cols.append(other_bucket.as_(other_alias))
            join_eqs.append(exp.EQ(this=_src_col(other_alias), expression=_base_col(other_td.alias)))

        raw_time_expr = self._resolve_sql(sql=td.sql or td.name, name=td.name, model_name=td.model_name)
        source_cols.append(raw_time_expr.as_("_w_time"))

        value_expr = self._resolve_sql(sql=measure.sql or measure.name, name=measure.name, model_name=measure.model_name)
        if measure.filter_sql:
            # measure.filter_sql is a user-supplied predicate (originates from
            # ``Column.filter`` / ``SlayerQuery.filters``); parse it via
            # ``_parse_predicate`` so dialects whose statement keywords
            # shadow function calls at expression start (SQLite / MySQL
            # ``REPLACE``) don't fall back to a Command parse — DEV-1378.
            filter_ast = self._parse_predicate(measure.filter_sql)
            value_expr = exp.Case(ifs=[exp.If(this=filter_ast, true=value_expr)])
        source_cols.append(value_expr.as_("_w_value"))

        return source_cols, join_eqs

    def _window_referenced_aliases(
        self,
        *,
        source_cols: list[exp.Alias],
        measure: EnrichedMeasure,
        filters,
    ) -> set[str]:
        """Aliases the windowed-CTE actually references; drives join pruning.

        Scans rendered `source_cols` SQL, the measure's filter_sql, and column
        paths of every non-post query filter (so a WHERE on customers.x keeps
        the customers join even if no other thing references it). Path aliases
        use "__" so each is one identifier token; for multi-hop aliases like
        "customers__regions" we also include every "__"-split prefix
        ("customers") via `_alias_prefixes` so the transitive joins those
        reference are kept too.
        """
        rendered_cols = " ".join(c.sql(dialect=self.dialect) for c in source_cols)
        referenced_text = rendered_cols
        if measure.filter_sql:
            referenced_text += " " + measure.filter_sql
        referenced: set[str] = set()
        for tok in re.findall(r'(?:^|[^\w."\'])([A-Za-z_]\w*)\.', referenced_text):
            referenced.update(_alias_prefixes(tok))
        for col in _filter_dotted_columns(filters):
            referenced.update(_alias_prefixes(col))
        return referenced

    def _build_window_source_select(
        self,
        *,
        enriched: EnrichedQuery,
        source_cols: list[exp.Alias],
        measure: EnrichedMeasure,
    ) -> exp.Select:
        """Build the _src subquery: SELECT ... FROM ... [filtered JOINs] [WHERE ...] as AST.

        Only joins whose target_alias is referenced by source_cols (or by the
        measure's filter SQL) are included — pulling in unrelated joins can
        change row multiplicity for the windowed aggregation, breaking the
        "adding a measure must not affect cardinality" core principle.
        """
        select = exp.Select().select(*source_cols).from_(self._build_from_clause(enriched=enriched))
        referenced = self._window_referenced_aliases(
            source_cols=source_cols, measure=measure, filters=enriched.filters,
        )

        for target_table, target_alias, join_cond, jtype in enriched.resolved_joins:
            if target_alias not in referenced:
                continue
            if target_table.startswith("("):
                join_target = exp.Subquery(
                    this=self._parse(target_table),
                    alias=exp.to_identifier(target_alias),
                )
            else:
                join_target = exp.to_table(target_table, alias=target_alias)
            join_on = self._parse(join_cond)
            select = select.join(join_target, on=join_on, join_type=jtype.upper())

        scoped = copy.copy(enriched)
        scoped.time_dimensions = [
            t.model_copy(update={"date_range": None}) for t in enriched.time_dimensions
        ]
        where_clause, _ = self._build_where_and_having(enriched=scoped)
        if where_clause is not None:
            select = select.where(where_clause)

        return select

    def _generate_window_measure_cte(self, enriched: EnrichedQuery, measure: EnrichedMeasure) -> str:
        if measure.aggregation not in ("sum", "avg"):
            raise ValueError("Windowed aggregations are only supported for sum and avg")
        if not measure.window or not measure.window_time_alias:
            raise ValueError(f"Windowed measure '{measure.alias}' is missing window metadata")

        td = next((t for t in enriched.time_dimensions if t.alias == measure.window_time_alias), None)
        if td is None:
            raise ValueError(f"Windowed measure '{measure.alias}' could not resolve its time dimension")

        group_aliases = [d.alias for d in enriched.dimensions] + [t.alias for t in enriched.time_dimensions]
        source_cols, join_eqs = self._build_window_source_cols(
            enriched=enriched, td=td, measure=measure,
        )
        src_select = self._build_window_source_select(
            enriched=enriched, source_cols=source_cols, measure=measure,
        )
        src_subq = exp.Subquery(this=src_select, alias=exp.TableAlias(this=exp.to_identifier("_src")))

        frame_time = exp.Column(this=exp.to_identifier(td.alias), table=exp.to_identifier("_base"))
        bucket_end = self._add_intervals_expr(
            frame_time,
            self._granularity_interval_expr(td.granularity, sign=1),
            sign=1,
        )
        lower_bound = self._add_intervals_expr(
            bucket_end,
            self._duration_interval_exprs(measure.window, sign=-1),
            sign=-1,
        )
        src_w_time = exp.Column(this=exp.to_identifier("_w_time"), table=exp.to_identifier("_src"))
        # bucket_end may be referenced both as upper bound and as base for the
        # lower bound — clone so the AST has independent subtrees.
        on_expr = exp.and_(
            *join_eqs,
            exp.GTE(this=src_w_time, expression=lower_bound),
            exp.LT(this=src_w_time.copy(), expression=bucket_end.copy()),
        )

        agg_cls = exp.Sum if measure.aggregation == "sum" else exp.Avg
        agg_input = exp.Column(this=exp.to_identifier("_w_value"), table=exp.to_identifier("_src"))

        outer = exp.Select()
        for a in group_aliases:
            outer = outer.select(exp.Column(this=exp.to_identifier(a), table=exp.to_identifier("_base")))
        agg_expr = _wrap_cast_for_type(agg_cls(this=agg_input), measure.type)
        outer = outer.select(agg_expr.as_(measure.alias))
        outer = outer.from_(exp.Table(this=exp.to_identifier("_base")))
        outer = outer.join(src_subq, on=on_expr, join_type="LEFT")
        for a in group_aliases:
            outer = outer.group_by(exp.Column(this=exp.to_identifier(a), table=exp.to_identifier("_base")))

        return outer.sql(dialect=self.dialect, pretty=True)

    def _generate_base(self, enriched: EnrichedQuery,
                        skip_isolated: bool = False) -> str:
        """Generate the base SELECT (measures, dimensions, filters)."""
        from_clause = self._build_from_clause(enriched=enriched)

        # If any measure has first/last aggregation, prepend a ROW_NUMBER CTE
        # to mark the latest (or earliest) row per group.
        # When skip_isolated is set, only consider non-isolated measures — isolated
        # first/last measures get their own ranked subquery in their CTE.
        if skip_isolated:
            has_first_or_last = any(
                m.aggregation in ("first", "last") and not _has_cross_model_filter(m)
                for m in enriched.measures
            )
        else:
            has_first_or_last = any(m.aggregation in ("first", "last") for m in enriched.measures)
        rn_suffix_map: dict[str, str] = {}
        filtered_rn_map: dict[str, str] = {}
        filtered_match_map: dict[str, str] = {}
        if has_first_or_last and enriched.last_agg_time_column:
            (
                from_clause,
                rn_suffix_map,
                filtered_rn_map,
                filtered_match_map,
            ) = self._build_last_ranked_from(
                enriched=enriched, base_from=from_clause,
            )

        select_columns = []
        group_by_columns = []

        for dim in enriched.dimensions:
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name, type=dim.type)
            if has_first_or_last:
                # In ranked subquery, dimensions are already columns — reference directly
                col_expr = exp.Column(this=exp.to_identifier(dim.name))
            select_columns.append(col_expr.as_(dim.alias))
            group_by_columns.append(col_expr)

        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
            if has_first_or_last:
                # Time dimension is already truncated in the ranked subquery
                col_expr = exp.Column(this=exp.to_identifier(f"_td_{td.name}"))
            else:
                col_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
            select_columns.append(col_expr.as_(td.alias))
            group_by_columns.append(col_expr)

        has_aggregation = False
        for measure in enriched.measures:
            if skip_isolated and (_has_cross_model_filter(measure) or _is_windowed_measure(measure)):
                continue  # Will be handled in its own CTE
            agg_expr, is_agg = self._build_agg(
                _agg_render_spec_from_enriched(measure),
                rn_suffix_map=rn_suffix_map,
                default_time_col=enriched.last_agg_time_column,
                filtered_rn_map=filtered_rn_map,
                filtered_match_map=filtered_match_map,
            )
            # DEV-1361: wrap the aggregation result in CAST when the measure
            # has a declared result type.
            if is_agg:
                agg_expr = _wrap_cast_for_type(agg_expr, measure.type)
            select_columns.append(agg_expr.as_(measure.alias))
            if is_agg:
                has_aggregation = True

        # When all measures are isolated/cross-model and there are no dimensions,
        # the base SELECT would be empty. Add a placeholder to produce valid SQL.
        if not select_columns and skip_isolated:
            select_columns.append(exp.Literal.number(1).as_("_placeholder"))

        where_clause, having_clause = self._build_where_and_having(
            enriched=enriched,
            rn_suffix_map=rn_suffix_map,
            filtered_rn_map=filtered_rn_map,
        )

        select = exp.Select()
        for col in select_columns:
            select = select.select(col)

        select = select.from_(from_clause)

        # When using ranked subquery for type=last, WHERE is already inside the subquery
        if where_clause is not None and not has_first_or_last:
            select = select.where(where_clause)

        # Group by when there are aggregations, cross-model measures exist,
        # isolated measures were skipped (to deduplicate the dimension spine),
        # or the query is dim-only (auto-dedup distinct dim/time-dim tuples
        # — applied before LIMIT so a row cap can't drop unique tuples).
        dim_only_dedup = bool(group_by_columns) and not enriched.measures
        needs_group_by = (
            has_aggregation
            or bool(enriched.cross_model_measures)
            or skip_isolated
            or dim_only_dedup
        )
        if needs_group_by and group_by_columns:
            for gb in group_by_columns:
                select = select.group_by(gb)

        if having_clause is not None:
            select = select.having(having_clause)

        # When no computed columns and no measure CTEs, apply order/limit/offset
        # to the base query. Otherwise, they'll be applied to the outer query.
        # DEV-1336: a post-filter requires the outer `_filtered` wrap from
        # `_generate_with_computed`; pagination must apply to the filtered
        # result, not to the unfiltered base.
        has_post_filters = any(getattr(f, "is_post_filter", False) for f in enriched.filters)
        if (
            not enriched.expressions
            and not enriched.transforms
            and not skip_isolated
            and not has_post_filters
        ):
            select = self._apply_order_limit(select=select, enriched=enriched)

        # Append LEFT JOINs from resolved joins via sqlglot AST (works for both
        # sql_table and inline-SQL models).
        # When has_first_or_last is true, the joins were already injected inside the
        # ranked subquery by _build_last_ranked_from — skip here to avoid duplicating.
        # When skip_isolated, only include joins needed for dimensions (not filter-target
        # joins of isolated measures, which would cause conflicting INNER JOIN intersections).
        dim_only_aliases = _needed_join_aliases(enriched) if skip_isolated else None
        if dim_only_aliases is not None:
            # Also include aliases needed by WHERE-clause filters
            for f in enriched.filters:
                if not f.is_post_filter:
                    for col in f.columns:
                        if "." in col:
                            parts = col.split(".")
                            for i in range(1, len(parts)):
                                dim_only_aliases.add("__".join(parts[:i]))
        resolved_joins = enriched.resolved_joins
        if dim_only_aliases is not None:
            resolved_joins = [(t, a, c, j) for t, a, c, j in resolved_joins if a in dim_only_aliases]
        if resolved_joins and not has_first_or_last:
            for target_table, target_alias, join_cond, jtype in resolved_joins:
                if target_table.startswith("("):
                    # Inline-SQL target: parse as subquery
                    parsed_target = self._parse(target_table)
                    join_target = exp.Subquery(
                        this=parsed_target, alias=exp.to_identifier(target_alias),
                    )
                else:
                    join_target = exp.to_table(target_table, alias=target_alias)
                join_on = self._parse(join_cond)
                select = select.join(join_target, on=join_on, join_type=jtype.upper())

        sql = select.sql(dialect=self.dialect, pretty=True)

        return sql

    def _generate_with_computed(self, enriched: EnrichedQuery,
                                base_sql: str | None = None,
                                prefix_ctes: list[tuple[str, str]] | None = None) -> str:
        """Wrap the base query as a CTE and add expressions/transforms as stacked CTE layers.

        Transforms that reference other transforms' outputs get their own CTE layer.
        This handles arbitrary nesting like change(cumsum(revenue)).

        Args:
            base_sql: Base SQL to wrap as "base" CTE (simple case, no measure CTEs).
            prefix_ctes: Pre-built CTE list from _build_combined(). When provided,
                these are used as the initial CTE stack instead of wrapping base_sql.
                The last entry is the "combined" CTE with all measure values available.
        """
        # Collect base aliases (includes all measures — combined SQL has them all)
        base_aliases = []
        for dim in enriched.dimensions:
            base_aliases.append(dim.alias)
        for td in enriched.time_dimensions:
            base_aliases.append(td.alias)
        for m in enriched.measures:
            base_aliases.append(m.alias)
        for cm in enriched.cross_model_measures:
            base_aliases.append(cm.alias)
        # Build stacked CTEs. Each layer can reference aliases from previous layers.
        if prefix_ctes is not None:
            ctes = list(prefix_ctes)
        else:
            ctes = [("base", base_sql)]
        available_aliases = set(base_aliases)  # Aliases available in the current layer

        # All transforms go into a unified layering loop. Each iteration tries
        # to resolve transforms whose inputs are available. Self-join transforms
        # (time_shift, change, change_pct) get their own CTE with a LEFT JOIN.
        # Window transforms (cumsum, lag, lead, rank, last) are batched into a
        # single CTE layer with OVER() expressions.
        # All measure aliases are available in base_sql (combined CTE includes
        # cross-model and isolated filtered measures via LEFT JOIN).
        pending_expressions = list(enriched.expressions)
        pending_transforms = list(enriched.transforms)
        layer_num = 0
        while pending_expressions or pending_transforms:
            layer_num += 1
            prev_cte = ctes[-1][0]
            added_this_layer = []
            remaining_expressions = []
            remaining_transforms = []

            # Collect window transforms and expressions that can go in one layer
            layer_parts = [f'"{a}"' for a in sorted(available_aliases)]

            for expr in pending_expressions:
                if self._deps_available(expr.sql, available_aliases):
                    # DEV-1361: when the source ModelMeasure declared a
                    # result type, wrap the expression in CAST so the outer
                    # SELECT yields the typed value.
                    expr_sql = expr.sql
                    if expr.type is not None:
                        wrapped = _wrap_cast_for_type(self._parse(expr_sql), expr.type)
                        expr_sql = wrapped.sql(dialect=self.dialect)
                    layer_parts.append(f'{expr_sql} AS "{expr.alias}"')
                    added_this_layer.append(expr.alias)
                else:
                    remaining_expressions.append(expr)

            # Batch window-function transforms into this layer
            deferred_self_joins = []
            deferred_consecutive_periods = []
            for t in pending_transforms:
                if t.measure_alias not in available_aliases:
                    remaining_transforms.append(t)
                elif t.transform in _SELF_JOIN_TRANSFORMS:
                    deferred_self_joins.append(t)  # Handle after window layer
                elif t.transform == "consecutive_periods":
                    deferred_consecutive_periods.append(t)
                else:
                    window_sql = self._build_transform_sql(t)
                    # DEV-1361: wrap in CAST when the source ModelMeasure
                    # declared a result type (propagated to t.type at
                    # enrichment time).
                    if t.type is not None:
                        wrapped = _wrap_cast_for_type(self._parse(window_sql), t.type)
                        window_sql = wrapped.sql(dialect=self.dialect)
                    layer_parts.append(f'{window_sql} AS "{t.alias}"')
                    added_this_layer.append(t.alias)

            # Emit window layer CTE if anything was added
            if added_this_layer:
                layer_name = f"step{layer_num}"
                layer_select = "SELECT\n    " + _SQL_COL_SEP.join(layer_parts)
                ctes.append((layer_name, f"{layer_select}\nFROM {prev_cte}"))
                available_aliases.update(added_this_layer)

            # Now emit each self-join transform as its own CTE layer.
            # The shifted sub-query has the time offset baked into td.sql,
            # so we always join on time column equality (calendar-based).
            for t in deferred_self_joins:
                src_cte = ctes[-1][0]

                shift_name = f"shifted_{t.name}"
                shifted_sql = self._generate_shifted_base(
                    enriched=enriched, transform=t,
                )
                ctes.append((shift_name, shifted_sql))

                # Build the self-join CTE: src LEFT JOIN shifted ON time equality
                time_col = f'"{t.time_alias}"'
                join_cond = f'{src_cte}.{time_col} = {shift_name}.{time_col}'
                # Also join on all dimension columns for correct matching
                for dim in enriched.dimensions:
                    join_cond += f' AND {src_cte}."{dim.alias}" = {shift_name}."{dim.alias}"'
                col_sql = self._build_self_join_column(
                    transform=t.transform, right_table=shift_name,
                    measure_alias=t.measure_alias,
                )
                join_cols = ", ".join(f'{src_cte}."{a}"' for a in sorted(available_aliases))
                join_layer = f"sjoin_{t.name}"
                join_sql = (
                    f"SELECT {join_cols}, {col_sql} AS \"{t.alias}\"\n"
                    f"FROM {src_cte}\n"
                    f"LEFT JOIN {shift_name}\n"
                    f"    ON {join_cond}"
                )
                ctes.append((join_layer, join_sql))
                available_aliases.add(t.alias)
                added_this_layer.append(t.alias)

            # consecutive_periods needs two window layers: one to compute the
            # reset group, then one to count within that group. Most SQL
            # engines reject nested window functions in a single SELECT.
            for t in deferred_consecutive_periods:
                reset_layer, value_layer = self._build_consecutive_periods_ctes(
                    transform=t,
                    source_cte=ctes[-1][0],
                    available_aliases=available_aliases,
                    layer_num=layer_num,
                )
                ctes.extend(reset_layer)
                ctes.extend(value_layer)
                available_aliases.add(t.alias)
                added_this_layer.append(t.alias)

            if not added_this_layer:
                remaining_transforms.extend(deferred_self_joins)
                remaining_transforms.extend(deferred_consecutive_periods)
                break  # Nothing could be added — remaining items have unresolved deps

            pending_expressions = remaining_expressions
            pending_transforms = remaining_transforms

        # Build final CTE clause
        cte_strs = [f"{name} AS (\n{sql}\n)" for name, sql in ctes]
        cte_clause = _SQL_WITH + ",\n".join(cte_strs)

        final_cte = ctes[-1][0]

        # Build final SELECT
        final_parts = [f'"{a}"' for a in sorted(available_aliases)]

        # Add any remaining expressions/transforms that couldn't be layered
        for expr in pending_expressions:
            final_parts.append(f'{expr.sql} AS "{expr.alias}"')
        for t in pending_transforms:
            if t.transform in _SELF_JOIN_TRANSFORMS:
                continue  # Should not happen — self-joins are always materialized
            if t.transform == "consecutive_periods":
                raise ValueError("consecutive_periods could not be materialized")
            window_sql = self._build_transform_sql(t)
            if t.type is not None:
                wrapped = _wrap_cast_for_type(self._parse(window_sql), t.type)
                window_sql = wrapped.sql(dialect=self.dialect)
            final_parts.append(f'{window_sql} AS "{t.alias}"')

        outer_select = "SELECT\n    " + _SQL_COL_SEP.join(final_parts)

        sql = f"{cte_clause}\n{outer_select}\nFROM {final_cte}"

        # Apply post-filters (filters referencing computed columns) BEFORE
        # pagination, so LIMIT/OFFSET operate on the filtered result.
        post_filters = [f for f in enriched.filters if f.is_post_filter]
        if post_filters:
            import re
            model = enriched.model_name
            conditions = []
            for f in post_filters:
                qualified_sql = f.sql
                for col_name in dict.fromkeys(f.columns):
                    qualified_sql = re.sub(
                        rf'(?<!\.)(?<!\w)\b{re.escape(col_name)}\b',
                        f"{model}.{col_name}",
                        qualified_sql,
                    )
                # Wrap qualified names in quotes for alias references
                for col_name in dict.fromkeys(f.columns):
                    qualified = f"{model}.{col_name}"
                    qualified_sql = qualified_sql.replace(qualified, f'"{qualified}"')
                conditions.append(qualified_sql)
            where_clause = _SQL_AND_JOINER.join(conditions)
            sql = f"SELECT *\nFROM (\n{sql}\n) AS _filtered\nWHERE {where_clause}"

        # Apply order/limit/offset as the outermost wrapper.
        return self._apply_pagination_to_sql(enriched=enriched, sql=sql)

    @staticmethod
    def _deps_available(sql: str, available: set[str]) -> bool:
        """Check if all quoted aliases referenced in SQL are in the available set."""
        import re
        refs = re.findall(r'"([^"]+)"', sql)
        return all(ref in available for ref in refs)

    def _build_consecutive_periods_ctes(
        self,
        transform,
        source_cte: str,
        available_aliases: set[str],
        layer_num: int,
    ) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        partition_aliases = getattr(transform, "partition_aliases", []) or []
        reset_alias = _cte_name_from_alias("_cp_reset_", transform.alias)
        reset_cte = _cte_name_from_alias(f"cp_reset_{layer_num}_", transform.alias)
        value_cte = _cte_name_from_alias(f"cp_value_{layer_num}_", transform.alias)

        def _quoted_col(name: str) -> exp.Column:
            return exp.Column(this=exp.to_identifier(name, quoted=True))

        measure_col = _quoted_col(transform.measure_alias)
        time_col = _quoted_col(transform.time_alias)
        # Bare column inside exp.Order, NOT wrapped in exp.Ordered — sqlglot
        # otherwise injects `NULLS LAST` on SQLite (and Spark/Databricks),
        # changing streak/reset semantics for any NULL time values vs the
        # pre-AST string-built `ORDER BY <t>` output.
        order = exp.Order(expressions=[time_col])
        spec = exp.WindowSpec(
            kind="ROWS",
            start="UNBOUNDED",
            start_side="PRECEDING",
            end="CURRENT ROW",
        )

        # Wrap measure in an explicit boolean predicate so non-boolean argument
        # expressions don't rely on dialect-specific truthiness coercion in
        # CASE WHEN. Postgres rejects non-boolean WHEN outright; SQLite/MySQL
        # coerce non-zero to true; ClickHouse has its own rules.
        # When the inner expression is already boolean (e.g.
        # `consecutive_periods(revenue:sum > 0)`), the numeric `<> 0` form
        # is itself rejected by Postgres ("operator does not exist:
        # boolean <> integer"), so we use the column directly inside CASE WHEN.
        def _predicate() -> exp.Expression:
            if getattr(transform, "predicate_is_boolean", False):
                return exp.func("COALESCE", measure_col.copy(), exp.false())
            return exp.and_(
                exp.Is(this=measure_col.copy(), expression=exp.Not(this=exp.Null())),
                exp.NEQ(this=measure_col.copy(), expression=exp.Literal.number(0)),
            )

        source_col_exprs = [_quoted_col(a) for a in sorted(available_aliases)]

        # reset CTE: SELECT <available>, SUM(CASE WHEN pred THEN 0 ELSE 1 END)
        #   OVER (PARTITION BY ... ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        #   AS "<reset_alias>" FROM source_cte
        reset_case = exp.Case(
            ifs=[exp.If(this=_predicate(), true=exp.Literal.number(0))],
            default=exp.Literal.number(1),
        )
        reset_window = exp.Window(
            this=exp.Sum(this=reset_case),
            partition_by=[_quoted_col(a) for a in partition_aliases] or None,
            order=order,
            spec=spec,
        )
        reset_select = (
            exp.Select()
            .select(*[c.copy() for c in source_col_exprs])
            .select(reset_window.as_(reset_alias, quoted=True))
            .from_(exp.Table(this=exp.to_identifier(source_cte)))
        )

        # value CTE: SELECT <available>,
        #   CASE WHEN pred THEN SUM(CASE WHEN pred THEN 1 ELSE 0 END)
        #     OVER (PARTITION BY ..., "<reset_alias>" ORDER BY t ROWS ...) ELSE 0 END
        #   AS "<transform.alias>" FROM reset_cte
        value_inner_case = exp.Case(
            ifs=[exp.If(this=_predicate(), true=exp.Literal.number(1))],
            default=exp.Literal.number(0),
        )
        value_partition = (
            [_quoted_col(a) for a in partition_aliases] + [_quoted_col(reset_alias)]
        )
        value_window = exp.Window(
            this=exp.Sum(this=value_inner_case),
            partition_by=value_partition,
            order=order.copy(),
            spec=spec.copy(),
        )
        value_outer_case = exp.Case(
            ifs=[exp.If(this=_predicate(), true=value_window)],
            default=exp.Literal.number(0),
        )
        value_select = (
            exp.Select()
            .select(*[c.copy() for c in source_col_exprs])
            .select(value_outer_case.as_(transform.alias, quoted=True))
            .from_(exp.Table(this=exp.to_identifier(reset_cte)))
        )

        reset_sql = reset_select.sql(dialect=self.dialect, pretty=True)
        value_sql = value_select.sql(dialect=self.dialect, pretty=True)
        return [(reset_cte, reset_sql)], [(value_cte, value_sql)]

    def _build_date_trunc(self, col_expr: exp.Expression, granularity: TimeGranularity) -> exp.Expression:
        """Build a DATE_TRUNC expression, with SQLite STRFTIME fallback."""
        gran_str = _GRANULARITY_MAP.get(granularity, granularity.value)
        if self.dialect == "sqlite":
            # SQLite has no DATE_TRUNC — use STRFTIME
            fmt_map = {
                "year": "%Y-01-01",
                "month": "%Y-%m-01",
                "day": "%Y-%m-%d",
                "hour": "%Y-%m-%d %H:00:00",
                "minute": "%Y-%m-%d %H:%M:00",
                "second": "%Y-%m-%d %H:%M:%S",
            }
            # Week: SQLite weekday 0=Sunday, use date() with weekday modifier
            if gran_str == "week":
                return self._parse(f"DATE({col_expr.sql(dialect='sqlite')}, 'weekday 0', '-6 days')", dialect="sqlite")
            if gran_str == "quarter":
                # Quarter start: derive from month
                col_sql = col_expr.sql(dialect="sqlite")
                return self._parse(
                    f"STRFTIME('%Y-', {col_sql}) || CASE "
                    f"WHEN CAST(STRFTIME('%m', {col_sql}) AS INTEGER) <= 3 THEN '01-01' "
                    f"WHEN CAST(STRFTIME('%m', {col_sql}) AS INTEGER) <= 6 THEN '04-01' "
                    f"WHEN CAST(STRFTIME('%m', {col_sql}) AS INTEGER) <= 9 THEN '07-01' "
                    f"ELSE '10-01' END",
                    dialect="sqlite",
                )
            fmt = fmt_map.get(gran_str, "%Y-%m-%d")
            return exp.Anonymous(
                this="STRFTIME",
                expressions=[exp.Literal.string(fmt), col_expr],
            )
        return exp.DateTrunc(this=col_expr, unit=exp.Literal.string(gran_str))

    @staticmethod
    def _build_transform_sql(t) -> str:  # NOSONAR S3776 — flat dispatch over transform names; per-transform SQL forms read better as one if/elif tree than as named helpers
        """Build a window function SQL expression for a transform."""
        measure = f'"{t.measure_alias}"'
        time_col = f'"{t.time_alias}"' if t.time_alias else None
        partition_cols = getattr(t, "partition_aliases", []) or []
        partition_clause = (
            _SQL_PARTITION_BY + ", ".join(f'"{a}"' for a in partition_cols)
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

    @staticmethod
    def _build_self_join_column(transform: str, right_table: str,
                                measure_alias: str) -> str:
        """Build the SELECT expression for a self-join transform."""
        prev = f'{right_table}."{measure_alias}"'
        if transform == "time_shift":
            return prev
        raise ValueError(f"Unknown self-join transform: {transform}")

    def _apply_order_limit(self, select: exp.Select, enriched: EnrichedQuery) -> exp.Select:
        """Apply ORDER BY, LIMIT, OFFSET to a select expression."""
        if enriched.order:
            for order_item in enriched.order:
                col = order_item.column
                col_name = self._resolve_order_column(col=col, enriched=enriched)
                order_col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
                ascending = order_item.direction == "asc"
                select = select.order_by(exp.Ordered(this=order_col, desc=not ascending))

        if enriched.limit is not None:
            select = select.limit(enriched.limit)

        if enriched.offset is not None:
            select = select.offset(enriched.offset)

        return select

    @staticmethod
    def _resolve_order_column(col, enriched: EnrichedQuery) -> str:
        """Resolve an order column reference to the correct enriched alias.

        Users refer to columns by their short name (e.g., ``count``,
        ``revenue_sum``).  The enriched query stores fully qualified aliases
        (e.g., ``orders._count``, ``orders.revenue_sum``).  This method
        matches the user-provided name against all enriched columns and
        returns the matching alias.  If no match is found, the name is
        qualified with the model name as a fallback.

        For ``*:count`` results, the internal name is ``_count`` but users
        refer to it as ``count``.  A fallback check for ``_name`` handles
        this case.
        """
        user_name = col.name
        model_prefix = col.model or enriched.model_name

        # Build a lookup: short name → alias for all enriched columns
        alias_lookup: dict[str, str] = {}
        for d in enriched.dimensions:
            alias_lookup[d.name] = d.alias
        for td in enriched.time_dimensions:
            alias_lookup[td.name] = td.alias
        for m in enriched.measures:
            alias_lookup[m.name] = m.alias
        for e in enriched.expressions:
            alias_lookup[e.name] = e.alias
        for t in enriched.transforms:
            alias_lookup[t.name] = t.alias
        for cm in enriched.cross_model_measures:
            alias_lookup[cm.name] = cm.alias
        # Custom field names (e.g., {"formula": "x:count_distinct", "name": "my_name"})
        alias_lookup.update(enriched.field_name_aliases)

        # Direct match on the user-provided name
        if user_name in alias_lookup:
            return alias_lookup[user_name]

        # Qualified match for cross-model measures:
        # col.model="customers", col.name="revenue_sum" → "customers.revenue_sum"
        if col.model:
            qualified = f"{col.model}.{col.name}"
            if qualified in alias_lookup:
                return alias_lookup[qualified]

        # Fallback for *:count → _count: user says "count", internal is "_count"
        prefixed = f"_{user_name}"
        if prefixed in alias_lookup:
            return alias_lookup[prefixed]

        # Fallback: qualify with model prefix
        return f"{model_prefix}.{user_name}"

    # ------------------------------------------------------------------
    # FROM / JOIN building
    # ------------------------------------------------------------------

    def _build_from_clause(self, enriched: EnrichedQuery) -> exp.Expression:
        if enriched.sql_table:
            return exp.to_table(enriched.sql_table, alias=enriched.model_name)
        elif enriched.sql:
            parsed = self._parse(enriched.sql)
            return exp.Subquery(this=parsed, alias=exp.to_identifier(enriched.model_name))
        else:
            raise ValueError(f"Model '{enriched.model_name}' has neither sql_table nor sql defined")

    def _build_last_ranked_from(
        self,
        enriched: EnrichedQuery,
        base_from: exp.Expression,
    ) -> tuple[exp.Expression, dict[str, str], dict[str, str], dict[str, str]]:
        """Build a ranked subquery for first/last aggregation.

        Wraps the source table in a subquery that adds ROW_NUMBER columns
        for each distinct time column used by first/last measures.
        Returns (subquery, rn_suffix_map, filtered_rn_map, filtered_match_map):
        rn_suffix_map maps each effective time column to its ROW_NUMBER alias
        suffix; filtered_rn_map and filtered_match_map both key by
        EnrichedMeasure.alias and map to the dedicated ROW_NUMBER column and
        boolean match-flag column for filtered first/last measures. The match
        flag is needed by the outer aggregate so it doesn't have to re-emit
        measure.filter_sql (which can reference joined-table columns that
        aren't in scope outside this subquery).
        """
        model = enriched.model_name
        default_time_col = enriched.last_agg_time_column

        # Build SELECT * plus ROW_NUMBER
        parts = [f"{model}.*"]

        # Add pre-computed time dimension expressions (DATE_TRUNC)
        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
            td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
            parts.append(f"{td_expr.sql(dialect=self.dialect)} AS _td_{td.name}")

        # Build PARTITION BY from query dimensions + time dimensions
        # Must use full expressions (not aliases) since aliases aren't visible in OVER()
        partition_parts = []
        for dim in enriched.dimensions:
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name, type=dim.type)
            partition_parts.append(col_expr.sql(dialect=self.dialect))
        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
            td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
            partition_parts.append(td_expr.sql(dialect=self.dialect))

        partition_clause = f"PARTITION BY {', '.join(partition_parts)}" if partition_parts else ""

        # Collect distinct effective time columns from UNFILTERED first/last
        # measures only — filtered ones get their own dedicated ROW_NUMBER
        # columns later (so we'd otherwise emit a redundant _last_rn that
        # nothing references).
        # default_time_col is guaranteed non-None here (checked at call site)
        assert default_time_col is not None
        time_col_agg_types: dict[str, set[str]] = {}
        for m in enriched.measures:
            if m.aggregation in ("first", "last") and not m.filter_sql:
                effective = m.time_column or default_time_col
                if effective not in time_col_agg_types:
                    time_col_agg_types[effective] = set()
                time_col_agg_types[effective].add(m.aggregation)

        # Assign stable suffixes: first sorted gets "", second gets "_2", etc.
        sorted_time_cols = sorted(time_col_agg_types.keys())
        rn_suffix_map: dict[str, str] = {}
        for i, tc in enumerate(sorted_time_cols):
            rn_suffix_map[tc] = "" if i == 0 else f"_{i + 1}"

        # Generate ROW_NUMBER columns per distinct time column
        for tc in sorted_time_cols:
            tc_expr = self._resolve_sql(sql=tc, name=tc, model_name=model)
            order_sql = tc_expr.sql(dialect=self.dialect)
            suffix = rn_suffix_map[tc]
            agg_types = time_col_agg_types[tc]
            if "last" in agg_types:
                parts.append(f"ROW_NUMBER() OVER ({partition_clause} ORDER BY {order_sql} DESC) AS _last_rn{suffix}")
            if "first" in agg_types:
                parts.append(f"ROW_NUMBER() OVER ({partition_clause} ORDER BY {order_sql} ASC) AS _first_rn{suffix}")

        # Generate dedicated ROW_NUMBER columns for filtered first/last measures.
        # These push non-matching rows to the bottom of the ranking so that
        # rn=1 picks the first matching row, not the globally first row.
        # Also project a per-filter boolean *match flag* so the outer aggregate
        # doesn't have to re-emit `measure.filter_sql` (which can reference
        # joined-table columns that aren't visible outside the ranked subquery).
        filtered_rn_map: dict[str, str] = {}
        filtered_match_map: dict[str, str] = {}
        filter_idx = 0
        # cache_key -> (rn_alias, match_alias)
        seen_filters: dict[tuple[str, str, str], tuple[str, str]] = {}
        for m in enriched.measures:
            if m.aggregation in ("first", "last") and m.filter_sql:
                effective_tc = m.time_column or default_time_col
                tc_expr = self._resolve_sql(sql=effective_tc, name=effective_tc, model_name=model)
                order_sql = tc_expr.sql(dialect=self.dialect)
                cache_key = (m.filter_sql, effective_tc, m.aggregation)
                if cache_key in seen_filters:
                    # Reuse existing columns for identical filter+time_col+agg
                    rn_alias, match_alias = seen_filters[cache_key]
                else:
                    rn_alias = f"_{'first' if m.aggregation == 'first' else 'last'}_rn_f{filter_idx}"
                    match_alias = f"_match_f{filter_idx}"
                    order_dir = "ASC" if m.aggregation == "first" else "DESC"
                    parts.append(
                        f"ROW_NUMBER() OVER ({partition_clause} ORDER BY "
                        f"CASE WHEN {m.filter_sql} THEN 0 ELSE 1 END, "
                        f"{order_sql} {order_dir}) AS {rn_alias}"
                    )
                    parts.append(
                        f"CASE WHEN {m.filter_sql} THEN 1 ELSE 0 END AS {match_alias}"
                    )
                    seen_filters[cache_key] = (rn_alias, match_alias)
                    filter_idx += 1
                # Key by alias (unique per enriched measure) so two filtered
                # measures that share source/agg but differ in filter or time
                # column don't clobber each other.
                filtered_rn_map[m.alias] = rn_alias
                filtered_match_map[m.alias] = match_alias

        select_sql = ", ".join(parts)
        from_sql = base_from.sql(dialect=self.dialect)
        ranked_sql = f"SELECT {select_sql} FROM {from_sql}"

        # Apply LEFT JOINs from resolved_joins INSIDE the subquery so that
        # filter expressions (and ORDER BY columns) referencing joined
        # tables resolve. The outer query's join injection only matches
        # `FROM <table> AS <model>` and would miss this subquery wrapper.
        if enriched.resolved_joins:
            join_sql_parts = [
                f"{jtype.upper()} JOIN {target_table} AS {target_alias} ON {join_cond}"
                for target_table, target_alias, join_cond, jtype in enriched.resolved_joins
            ]
            ranked_sql += " " + " ".join(join_sql_parts)

        # Apply WHERE filters to the subquery (they filter raw data before ranking)
        where_clause, _ = self._build_where_and_having(enriched=enriched)
        if where_clause is not None:
            ranked_sql += f" WHERE {where_clause.sql(dialect=self.dialect)}"

        parsed = self._parse(ranked_sql)
        return (
            exp.Subquery(this=parsed, alias=exp.to_identifier(model)),
            rn_suffix_map,
            filtered_rn_map,
            filtered_match_map,
        )

    # ------------------------------------------------------------------
    # Column / measure resolution (from enriched SQL expressions)
    # ------------------------------------------------------------------

    def _rewrite_log_aliases(self, node: exp.Expression) -> exp.Expression:
        """DEV-1337: rewrite ``Log(this=Literal(10|2), expression=X)`` back to
        ``Anonymous(this='log10'|'log2', expressions=[X])`` for dialects with
        native single-arg aliases. Walked over every parsed AST so the
        rewrite survives sqlglot's re-parse passes (which would otherwise
        turn ``LOG10(x)`` back into a generic ``Log`` node and re-emit as
        ``LOG(10, x)``). No-op on non-``Log`` nodes and on ``Log`` nodes
        with a non-literal or non-{10,2} base.
        """
        if not isinstance(node, exp.Log):
            return node
        base = node.args.get("this")
        arg = node.args.get("expression")
        if arg is None or not isinstance(base, exp.Literal) or base.is_string:
            return node
        try:
            base_val = float(base.this)
        except (TypeError, ValueError):
            return node
        if base_val == 10 and self.dialect in _LOG10_NATIVE_DIALECTS:
            return exp.Anonymous(this="log10", expressions=[arg.copy()])
        if base_val == 2 and self.dialect in _LOG2_NATIVE_DIALECTS:
            return exp.Anonymous(this="log2", expressions=[arg.copy()])
        return node

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
            return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(model_name))
        # Bare column name → qualify with model name
        # Use isidentifier() to distinguish column names from literals (e.g. "1")
        if sql.isidentifier():
            return exp.Column(this=exp.to_identifier(sql), table=exp.to_identifier(model_name))
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
        raw: Optional[str] = None
        if name in spec.agg_kwargs:
            raw = spec.agg_kwargs[name]
            _validate_agg_param_value(raw, name, agg_name)
        elif spec.aggregation_def:
            for param in spec.aggregation_def.params:
                if param.name == name:
                    raw = param.sql
                    break
        if raw is None:
            raise ValueError(
                f"Aggregation '{agg_name}' requires parameter '{name}'. "
                f"Set it in the model's aggregation definition or at query time "
                f"(e.g., 'measure:{agg_name}({name}=column)')."
            )
        return self._resolve_sql(
            sql=raw, name=raw, model_name=spec.model_name,
        ).sql(dialect=self.dialect)

    def _build_agg(
        self,
        spec: AggRenderSpec,
        rn_suffix_map: Optional[dict[str, str]] = None,
        default_time_col: Optional[str] = None,
        filtered_rn_map: Optional[dict[str, str]] = None,
        filtered_match_map: Optional[dict[str, str]] = None,
    ) -> tuple[exp.Expression, bool]:
        """Build an aggregation expression from an AggRenderSpec."""
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

    def _build_formula_agg(self, spec: AggRenderSpec, agg_name: str) -> exp.Expression:
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

        # Validate query-time parameter values to prevent SQL injection
        for pname, pval in spec.agg_kwargs.items():
            _validate_agg_param_value(pval, pname, agg_name)

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
            param_ast = self._resolve_sql(
                sql=param_val, name=param_val, model_name=spec.model_name,
            )
            param_expr = param_ast.sql(dialect=self.dialect)
            if spec.filter_sql and not isinstance(param_ast, exp.Literal):
                param_expr = _wrap_filter(param_expr, spec.filter_sql)
            substituted = substituted.replace(f"{{{param_name}}}", param_expr)

        return self._parse(substituted)

    def _build_median(self, inner: exp.Expression) -> exp.Expression:
        """Build a median aggregation expression (dialect-dependent)."""
        inner_sql = inner.sql(dialect=self.dialect)
        if self.dialect == "mysql":
            raise NotImplementedError(
                "Aggregation 'median' is not supported on MySQL: MySQL has no native "
                "MEDIAN/PERCENTILE_CONT function and no Python UDF mechanism. "
                "Use MariaDB (has MEDIAN()) or compute the value client-side."
            )
        if self.dialect in ("sqlite", "clickhouse"):
            # SQLite: provided by the median() UDF registered on connect.
            # ClickHouse: native median() aggregate.
            return self._parse(f"median({inner_sql})")
        # Postgres, DuckDB, and most others: PERCENTILE_CONT
        return self._parse(f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {inner_sql})")

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

        if self.dialect == "mysql":
            raise NotImplementedError(
                "Aggregation 'percentile' is not supported on MySQL: MySQL has no native "
                "PERCENTILE_CONT function and no Python UDF mechanism. "
                "Use MariaDB or compute the value client-side."
            )

        col_expr = _wrap_filter(self._resolve_value_sql(spec), spec.filter_sql)

        if self.dialect == "sqlite":
            # Provided by the percentile_cont(value, p) UDF registered on connect.
            sql_str = f"percentile_cont({col_expr}, {p})"
        elif self.dialect == "clickhouse":
            # ClickHouse parametric aggregate syntax.
            sql_str = f"quantile({p})({col_expr})"
        else:
            sql_str = f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {col_expr})"

        return self._parse(sql_str)

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
        other_expr: Optional[str] = None
        if agg_name in _TWO_ARG_STAT_AGGS:
            other_expr = _wrap_filter(
                self._resolve_agg_param(spec, name="other", agg_name=agg_name),
                spec.filter_sql,
            )

        if agg_name in _TWO_ARG_STAT_AGGS and self.dialect == "mysql":
            raise NotImplementedError(
                f"Aggregation '{agg_name}' is not supported on MySQL: MySQL has no "
                f"native {agg_name.upper()} function and no Python UDF mechanism. "
                f"Use MariaDB or compute the value client-side."
            )

        col_expr = _wrap_filter(self._resolve_value_sql(spec), spec.filter_sql)

        if agg_name in _TWO_ARG_STAT_AGGS:
            sql_str = f"{agg_name.upper()}({col_expr}, {other_expr})"
        else:
            # stddev_samp, stddev_pop, var_samp, var_pop: emit the
            # canonical Postgres-style name and let sqlglot transpile per
            # dialect (e.g., var_samp → VARIANCE on SQLite/DuckDB/MySQL,
            # var_pop → VARIANCE_POP on SQLite/MySQL). Both spellings
            # resolve via the SQLite UDF aliases.
            #
            # MySQL exception: sqlglot's MySQL dialect rewrites
            # ``VAR_POP`` → ``VARIANCE_POP`` (no such function in MySQL —
            # only VAR_POP / VARIANCE exist) and ``VAR_SAMP`` →
            # ``VARIANCE`` (silently wrong, since MySQL's ``VARIANCE``
            # equals ``VAR_POP`` — sample variance gets aliased to
            # population variance). Bypass both by emitting the
            # MySQL-native names through ``exp.Anonymous``, which
            # sqlglot leaves verbatim.
            if self.dialect == "mysql" and agg_name in {"var_samp", "var_pop"}:
                return exp.Anonymous(
                    this=agg_name.upper(),
                    expressions=[self._parse(col_expr)],
                )
            sql_str = f"{agg_name.upper()}({col_expr})"

        return self._parse(sql_str)

    # ------------------------------------------------------------------
    # WHERE / HAVING (filters still use ColumnRef for member resolution)
    # ------------------------------------------------------------------

    def _build_where_and_having(
        self,
        enriched: EnrichedQuery,
        rn_suffix_map: Optional[dict[str, str]] = None,
        filtered_rn_map: Optional[dict[str, str]] = None,
    ) -> tuple[Optional[exp.Expression], Optional[exp.Expression]]:
        """Build WHERE and HAVING clauses from parsed filters.

        ParsedFilter objects have pre-built SQL strings. Column names are
        qualified with the model name for the WHERE clause.
        """
        where_parts: list[str] = []
        having_parts: list[str] = []

        # Time dimension date ranges — use the resolved SQL expression
        # (which may include a time offset for shifted sub-queries)
        for td in enriched.time_dimensions:
            if td.date_range and len(td.date_range) == 2:
                col_expr = self._resolve_sql(
                    sql=td.sql or td.name, name=td.name, model_name=td.model_name,
                )
                col = col_expr.sql(dialect=self.dialect)
                where_parts.append(
                    f"{col} BETWEEN '{td.date_range[0]}' AND '{td.date_range[1]}'"
                )

        # Parsed filters
        import re
        model = enriched.model_name
        for f in enriched.filters:
            # Post-filters are applied later, on the outer wrapper
            if f.is_post_filter:
                continue
            if f.is_having:
                # HAVING: reference the aggregate by looking up the measure's
                # aggregation expression from the enriched query
                having_sql = f.sql
                for col_name in dict.fromkeys(f.columns):
                    # Find the measure and build its aggregate expression
                    for m in enriched.measures:
                        if m.name == col_name:
                            agg_expr, _ = self._build_agg(
                                _agg_render_spec_from_enriched(m),
                                rn_suffix_map=rn_suffix_map,
                                default_time_col=enriched.last_agg_time_column,
                                filtered_rn_map=filtered_rn_map,
                            )
                            agg_sql = agg_expr.sql(dialect=self.dialect)
                            having_sql = re.sub(
                                rf'(?<!\.)(?<!\w)\b{re.escape(col_name)}\b',
                                agg_sql,
                                having_sql,
                            )
                            break
                having_parts.append(having_sql)
            else:
                # WHERE: qualify column names with model name
                # Dotted names (joined columns) are already table-qualified
                qualified_sql = f.sql
                for col_name in dict.fromkeys(f.columns):
                    if "." in col_name:
                        # Already qualified (e.g., "customers.name") — keep as-is
                        pass
                    elif col_name.isidentifier():
                        qualified_sql = re.sub(
                            rf'(?<!\.)(?<!\w)\b{re.escape(col_name)}\b',
                            f"{model}.{col_name}",
                            qualified_sql,
                        )
                where_parts.append(qualified_sql)

        where_clause = None
        if where_parts:
            where_sql = _SQL_AND_JOINER.join(where_parts)
            # DEV-1378: ``_parse_predicate`` wraps in SELECT context so a
            # filter starting with ``replace(...)`` (a SQLite/MySQL
            # statement keyword) is parsed as a function call rather
            # than the REPLACE INTO statement form.
            where_clause = self._parse_predicate(where_sql)

        having_clause = None
        if having_parts:
            having_sql = _SQL_AND_JOINER.join(having_parts)
            having_clause = self._parse_predicate(having_sql)

        return where_clause, having_clause

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

    def generate_from_planned(  # NOSONAR(S3776) — top-level dispatch over cross-model / transform-chain / plain branches plus the conditional outer-trim wrap. Each branch is a coherent compilation strategy; extracting would scatter the shared planned_query / slots_by_id / aliases_by_slot_id state across helpers without simplifying anything.
        self,
        planned_query,
        *,
        bundle,
    ) -> str:
        """Render a typed ``PlannedQuery`` to SQL.

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

        if planned_query.cross_model_aggregate_plans:
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
        dim_only_dedup = bool(group_by_keys) and not has_aggregation
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
        shifted_where_parts = self._build_shifted_cte_where_parts(
            planned_query=planned_query,
            source_relation=source_relation,
            source_model=source_model,
            bundle=bundle,
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
                step_name = f"step{step_num}"
                prev_cte = ctes[-1][0]
                carry_aliases_sorted = sorted(
                    a for aliases in aliases_by_slot_id.values() for a in aliases
                )
                step_parts = [f'"{a}"' for a in carry_aliases_sorted]
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
                        step_parts.append(f'{window_sql} AS "{full_alias}"')
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
                        slots_by_id=slots_by_id,
                        slot_id_by_key=slot_id_by_key,
                        available_alias_by_slot_id=available_alias_by_slot_id,
                        aliases_by_slot_id=aliases_by_slot_id,
                        source_model=source_model,
                        source_relation=source_relation,
                        shifted_where_parts=shifted_where_parts,
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
            step_name = f"step{step_num}"
            prev_cte = ctes[-1][0]
            carry_aliases_sorted = sorted(
                a for aliases in aliases_by_slot_id.values() for a in aliases
            )
            step_parts = [f'"{a}"' for a in carry_aliases_sorted]
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
                step_parts.append(f'{expr_sql} AS "{full_alias}"')
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
        # (matches legacy _generate_with_computed:1607).
        final_cte = ctes[-1][0]
        inner_sorted = sorted(
            a for aliases in aliases_by_slot_id.values() for a in aliases
        )
        inner_sql = (
            "SELECT\n    "
            + _SQL_COL_SEP.join(f'"{a}"' for a in inner_sorted)
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
                f"SELECT *\nFROM (\n{chain_sql}\n) AS _filtered"
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
        outer_sql = (
            "SELECT\n    "
            + _SQL_COL_SEP.join(f'"{a}"' for a in public_aliases_user_order)
            + f"\nFROM (\n{chain_sql}\n) AS _outer"
        )

        # ORDER BY / LIMIT / OFFSET on the outermost wrap.
        return self._apply_order_limit_to_planned_sql_string(
            sql=outer_sql,
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

    def _build_base_select_for_planned(
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
            ColumnKey,
            ColumnSqlKey,
            Phase,
            TimeTruncKey,
        )

        # Walk row slots to collect every joined path so the FROM
        # clause carries the needed LEFT JOINs in one pass.
        needed_join_paths = self._collect_joined_paths_for_base(
            base_render_order=base_render_order,
            slots_by_id=slots_by_id,
        )
        # Pre-expand local derived (ColumnSqlKey) ROW dimensions: inline
        # sibling/joined derived refs (DEV-1333 / DEV-1410) and pull any
        # joins their SQL crosses into the FROM.
        derived_expr_by_sid: Dict[str, exp.Expression] = {}
        for sid in base_render_order:
            slot = slots_by_id.get(sid)
            if slot is None or slot.phase != Phase.ROW:
                continue
            key = slot.key
            # DEV-1450 #4a: a derived (ColumnSqlKey) TIME dimension expands the
            # same way; pull any joins its SQL crosses into the FROM so the
            # DATE_TRUNC over the expanded expression resolves. The render
            # branch re-derives the (un-cached) raw expr and wraps DATE_TRUNC.
            if isinstance(key, TimeTruncKey) and isinstance(
                key.column, ColumnSqlKey,
            ):
                raw = self._raw_time_col_expr_for_planned(
                    time_column=key.column,
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                )
                for p in self._joined_paths_in_sql(
                    sql_expr=raw, source_relation=source_relation,
                    source_model=source_model, bundle=bundle,
                ):
                    if p not in needed_join_paths:
                        needed_join_paths.append(p)
                continue
            if not isinstance(key, ColumnSqlKey):
                continue
            # A derived (``Column.sql``) dimension. Local refs (``path == ()``)
            # expand rooted at the source relation; a CROSS-MODEL derived dim
            # (``B.foo_normalized``, ``path == ("B",)``) expands rooted at the
            # ``__``-path alias of the owning joined model — mirrors the
            # ColumnSqlKey arm of ``_raw_time_col_expr_for_planned``. Without
            # this the render branch falls back to ``_dim_column_expr_from_
            # planned`` which only looks at the source model and raises
            # "Column not found".
            if key.path:
                owner_model = bundle.get_referenced_model(key.path[-1])
                if owner_model is None:
                    continue
                owner_relation = "__".join(key.path)
            else:
                owner_model = source_model
                owner_relation = source_relation
            expanded_sql = self._expand_derived_column_sql(
                source_model=owner_model,
                source_relation=owner_relation,
                column_name=key.column_name,
                bundle=bundle,
                # Cross-model derived dim: the owner is a joined model, so a
                # further-joined ref inside its sql must carry the full path
                # prefix (``B`` reaching ``C`` → ``B__C``).
                is_root=not key.path,
            )
            col = next(
                (c for c in owner_model.columns if c.name == key.column_name),
                None,
            )
            expr = _wrap_cast_for_type(
                self._parse(expanded_sql),
                col.type if col is not None else None,
            )
            derived_expr_by_sid[sid] = expr
            # Pull the join to the owning model itself (cross-model case) plus
            # any joins the expanded SQL crosses into the FROM clause.
            if key.path and key.path not in needed_join_paths:
                needed_join_paths.append(key.path)
            for p in self._joined_paths_in_sql(
                sql_expr=expr, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
            ):
                if p not in needed_join_paths:
                    needed_join_paths.append(p)
        # WHERE-phase filters referencing joined columns (direct, derived, or
        # Mode-A ``__`` paths) pull their joins into the FROM too. Filters
        # routed to a cross-model ``_cm_*`` CTE (``skip_filter_ids``) are
        # applied there, not on ``_base`` — pulling their join into ``_base``
        # would add an unused (and, for one-to-many joins, cardinality-
        # changing) LEFT JOIN.
        for p in self._collect_filter_join_paths(
            planned_query=planned_query, source_model=source_model,
            source_relation=source_relation, bundle=bundle,
            skip_filter_ids=skip_filter_ids,
        ):
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
            if skip_cross_model_aggs:
                # The cross-model orchestrator builds ``_base`` with
                # ``skip_cross_model_aggs=True``; a local first/last there
                # would need the ranked subquery wrapped around ``_base``
                # while still deferring cross-model aggregates to their
                # ``_cm_*`` CTEs. Raise loudly rather than emit a SELECT
                # that references ROW_NUMBER columns it never projected.
                raise NotImplementedError(
                    "DEV-1450: local first/last aggregation combined with "
                    "cross-model aggregates is not yet supported; factor the "
                    "first/last measure into a multi-stage source_queries "
                    "model."
                )
            return self._build_first_last_base_select(
                planned_query=planned_query,
                bundle=bundle,
                source_model=source_model,
                source_relation=source_relation,
                base_render_order=base_render_order,
                slots_by_id=slots_by_id,
                from_clause=from_clause,
                base_joins=base_joins,
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
                    # Render inline; cast the whole composite once.
                    composite, any_agg = self._render_aggregate_composite_expr(
                        key=key,
                        slot=slot,
                        source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
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

    def _resolve_explicit_time_col(  # NOSONAR(S3776) — sequential isinstance dispatch over ColumnKey (bare ref → ``__``-joined path alias) and ColumnSqlKey (derived column → bare-ident-qualify vs complex-emit-verbatim). Extracting the per-shape branches would scatter the time-arg resolution contract; each branch is one decision.
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
        ``amount:last(created_at)``) and derived-column refs
        (``ColumnSqlKey`` — ``amount:last(net_amount_date)`` where
        ``net_amount_date`` has a non-trivial ``Column.sql``). For derived
        columns the column's ``Column.sql`` is materialised through
        ``_expand_derived_column_sql`` (when ``bundle`` is available) so
        inner bare refs qualify to ``source_relation`` and joined refs to
        their ``__``-path alias — a complex expression like
        ``date(created_at)`` can't go ambiguous against a same-named column
        on a joined table inside the ranked subquery. Without a ``bundle``
        it falls back to bare-ident qualification / verbatim emit.

        Returns ``None`` for non-first/last aggs and when ``key.args`` is
        empty or its first element is neither a ``ColumnKey`` nor a
        ``ColumnSqlKey``. Cross-model paths on derived time args
        (``ColumnSqlKey`` with non-empty ``path``) raise
        ``NotImplementedError`` rather than silently emitting against the
        wrong relation alias — that case is tracked alongside bug (c) of
        the four-bug Stage B package in DEV-1476.
        """
        from slayer.core.keys import ColumnKey, ColumnSqlKey

        if key.agg not in ("first", "last"):
            return None
        for a in key.args:
            if isinstance(a, ColumnKey):
                relation = "__".join(a.path) if a.path else source_relation
                return f"{relation}.{a.leaf}"
            if isinstance(a, ColumnSqlKey):
                if a.path:
                    raise NotImplementedError(
                        f"Cross-model derived time column "
                        f"(path={a.path!r}, column={a.column_name!r}) on "
                        f"first/last positional arg is not yet supported "
                        f"by the ranked-subquery builder; tracked as "
                        f"DEV-1476."
                    )
                col = next(
                    (c for c in source_model.columns if c.name == a.column_name),
                    None,
                )
                if col is None:
                    raise ValueError(
                        f"Derived time column {a.column_name!r} (positional "
                        f"arg of {key.agg!r}) not found on model "
                        f"{source_model.name!r}."
                    )
                if bundle is not None:
                    # Qualify inner bare refs against ``source_relation`` (and
                    # joined refs to their ``__``-path alias) so a complex
                    # derived time expression can't bind to the wrong table
                    # inside the ranked subquery's joins — same expansion the
                    # aggregate-source path uses.
                    return self._expand_derived_column_sql(
                        source_model=source_model,
                        source_relation=source_relation,
                        column_name=a.column_name,
                        bundle=bundle,
                    )
                # No bundle (defensive): bare-ident qualify, else emit verbatim.
                col_sql = col.sql if col.sql else col.name
                if col_sql.isidentifier():
                    return f"{source_relation}.{col_sql}"
                return self._parse(col_sql).sql(dialect=self.dialect)
            # Unrecognised positional arg type — leave time_column unset and
            # let _build_ranked_subquery_from_planned fall back to the
            # query's default ranking column.
            break
        return None

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
                    # An explicit time arg is the first ColumnKey /
                    # ColumnSqlKey in ``key.args``.
                    has_explicit = any(
                        isinstance(a, (ColumnKey, ColumnSqlKey))
                        for a in fl.args
                    )
                    if not has_explicit:
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
        synth_by_sid: Dict[str, "EnrichedMeasure"] = {}
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
                    local = exp.Column(
                        this=exp.to_identifier(key.leaf),
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
                if sid not in synth_by_sid:
                    synth_by_sid[sid] = (
                        self._build_agg_render_spec_from_planned(
                            slot=slot, key=slot.key, source_model=source_model,
                            source_relation=source_relation,
                            full_alias=full_alias,
                            bundle=bundle,
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
        composite_synth_by_key: Dict[Any, "EnrichedMeasure"] = {}
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

        # WHERE goes inside the ranked subquery (raw-row filtering before
        # ranking). HAVING is recomputed and applied by the caller.
        where_clause, _having = self._build_where_having_from_planned(
            planned_query=planned_query,
            source_relation=source_relation,
            source_model=source_model,
            bundle=bundle,
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
                    agg_expr, is_agg = self._build_agg(
                        synth_by_sid[sid],
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
                    agg_expr, is_agg = self._render_aggregate_composite_expr(
                        key=slot.key, slot=slot, source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
                        rn_suffix_map=rn_suffix_map,
                        default_time_col=default_time_col_sql,
                        filtered_rn_map=filtered_rn_map,
                        filtered_match_map=filtered_match_map,
                        composite_alias_by_key=composite_alias_by_key,
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
        )
        return (
            base_select, aliases_by_slot_id, has_aggregation,
            group_by_keys, True, first_last_state,
        )

    def _render_aggregate_composite_expr(
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
    ) -> "tuple[exp.Expression, bool]":
        """Render an AGGREGATE-phase composite key (``ArithmeticKey`` /
        ``ScalarCallKey`` of aggregates, e.g. ``expensenet:avg +
        benchmarkexp:avg``) to one inline sqlglot expr.

        Operand ``AggregateKey``s render inline via the same synth +
        ``_build_agg`` path the single-aggregate branch uses (no per-operand
        cast — the caller casts the composite once). Returns ``(expr,
        contains_aggregate)``. Cross-model operand aggregates (non-empty
        ``source.path``) are not yet handled here — they need CTE routing.

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
                )
                operands.append(e)
                any_agg = any_agg or a
            return self._compose_arithmetic_op(op=key.op, operands=operands), any_agg
        if isinstance(key, ScalarCallKey):
            args = []
            any_agg = False
            for a in key.args:
                if isinstance(a, (AggregateKey, ArithmeticKey, ScalarCallKey, LiteralKey)):
                    e, ag = self._render_aggregate_composite_expr(
                        key=a, slot=slot, source_model=source_model,
                        source_relation=source_relation,
                        bundle=bundle,
                        rn_suffix_map=rn_suffix_map,
                        default_time_col=default_time_col,
                        filtered_rn_map=filtered_rn_map,
                        filtered_match_map=filtered_match_map,
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
            if key.name == "like":
                return exp.Like(this=args[0], expression=args[1]), any_agg
            return exp.func(key.name.upper(), *args), any_agg
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

    def _render_with_cross_model_plans(
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
        from slayer.core.keys import AggregateKey

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
        base_projection = [
            sid for sid in planned_query.projection if sid not in cma_slot_ids
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
            if sid in cma_slot_ids or sid in seen_base_ids:
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
        if planned_query.transform_layers:
            aux_slot_id_by_key = {s.key: s.id for s in slots_by_id.values()}
            for sid in self._collect_base_aux_slot_ids(
                planned_query=planned_query,
                slot_id_by_key=aux_slot_id_by_key,
                slots_by_id=slots_by_id,
                include_order=True,
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

        # DEV-1501 (Codex round 6): host AGG-phase filter operand
        # AggregateKey slots (a HAVING filter on a hidden local first/
        # last like ``revenue:last(created_at) > 100``) must also reach
        # ``base_render_order`` so the host ``_base`` CTE builds the
        # ranked subquery — otherwise HAVING references a dangling
        # ``_last_rn``. The transform-gated block above misses this case
        # (no transforms). ``aggregates_only=True`` keeps row deps out
        # of GROUP BY; ``include_order=False`` since order is already
        # covered by ``order_only_local_ids`` above.
        aux_slot_id_by_key_agg = {s.key: s.id for s in slots_by_id.values()}
        for sid in self._collect_base_aux_slot_ids(
            planned_query=planned_query,
            slot_id_by_key=aux_slot_id_by_key_agg,
            slots_by_id=slots_by_id,
            include_order=False,
            aggregates_only=True,
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

        # Hidden grain materialisation: when the user query has neither
        # host row slots NOR local aggs (and no hidden order targets),
        # ``base_render_order`` is empty and the ``_base`` CTE would be a
        # bare ``FROM orders`` — legacy emits ``SELECT 1 AS _placeholder
        # FROM orders`` so the combined CROSS JOIN has a left side to
        # join against. Mirror that shape.
        empty_base = not base_render_order
        if empty_base:
            base_select = exp.Select().select(
                exp.Alias(this=exp.Literal.number("1"), alias=exp.to_identifier("_placeholder")),
            ).from_(
                self._build_from_clause_from_planned(
                    source_model=source_model, source_relation=source_relation,
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
            routed_ids: Set[str] = set()
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
            if base_where is not None:
                base_select = base_select.where(base_where)
            base_dim_only_dedup = bool(base_group_by) and not base_has_agg
            if (base_has_agg or base_dim_only_dedup) and base_group_by:
                for gb in base_group_by.values():
                    base_select = base_select.group_by(gb)
            if base_having is not None:
                base_select = base_select.having(base_having)

        base_cte_sql = base_select.sql(dialect=self.dialect, pretty=True)

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
        seen_cm: set = set()
        canonical_alias_for_plan: Dict[str, str] = {}
        # join-back pairs are ``(host_base_alias, cte_column_alias)`` — the two
        # sides need not match (re-rooted CTEs alias dims under the target's
        # relation). ``agg_col_alias_for_plan`` is the CTE's emitted column
        # name for the aggregate (canonical for the forward path; the sub-plan
        # alias for the re-rooted path).
        joinback_pairs_for_plan: Dict[str, List[Tuple[str, str]]] = {}
        agg_col_alias_for_plan: Dict[str, str] = {}
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
            cte_name = _cte_name_from_alias("_cm_", canonical_alias)
            if cte_name in seen_cm:
                continue
            seen_cm.add(cte_name)

            if plan.rerooted_plan is not None:
                # C1: nested re-rooted PlannedQuery rooted at the target,
                # preserving host dimension grain.
                cte_sql, joinback_pairs, agg_col_alias = (
                    self._render_rerooted_cross_model_cte(
                        plan=plan,
                        bundle=bundle,
                        host_slots_by_id=slots_by_id,
                        host_source_relation=source_relation,
                    )
                )
            else:
                cte_sql, shared_grain_aliases = self._render_cross_model_cte(
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
            cm_ctes.append((cte_name, cte_sql))
            joinback_pairs_for_plan[plan.aggregate_slot_id] = joinback_pairs
            agg_col_alias_for_plan[plan.aggregate_slot_id] = agg_col_alias

        # Codex MED fold-in: surface dropped-filter warnings from each
        # plan via Python ``warnings`` so callers using
        # ``warnings.catch_warnings()`` see what was dropped. The
        # generator is the boundary that "renders" the plan; warnings
        # are inert until something is actually compiled.
        import warnings as _warnings_mod
        for plan in planned_query.cross_model_aggregate_plans:
            for w in plan.dropped_filter_warnings:
                _warnings_mod.warn(
                    str(w),
                    UserWarning,
                    stacklevel=2,
                )

        # Build the combined SELECT: SELECT _base.<all_local>,
        # _cm_*.<canonical> [AS "<user_alias>"] FROM _base [LEFT JOIN |
        # CROSS JOIN] _cm_* [ON ...].
        combined_parts: List[str] = []
        # ``combined_aliases_by_slot_id`` records the output column alias each
        # slot surfaces in the combined SELECT — the input the transform chain
        # (when present) binds against (the combined result is its base CTE).
        combined_aliases_by_slot_id: Dict[str, List[str]] = {}
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
                combined_parts.append(f'_base."{full_alias}"')
            if aliases:
                combined_aliases_by_slot_id[sid] = list(aliases)
        # Cross-model side: one entry per declared user alias, all
        # referencing the CTE's aggregate column (canonical for the forward
        # path; the sub-plan alias for the re-rooted path). When the public
        # alias matches the CTE column name, no ``AS`` remap fires.
        for plan in planned_query.cross_model_aggregate_plans:
            agg_slot = slots_by_id[plan.aggregate_slot_id]
            canonical_alias = canonical_alias_for_plan[plan.aggregate_slot_id]
            agg_col_alias = agg_col_alias_for_plan[plan.aggregate_slot_id]
            cte_name = _cte_name_from_alias("_cm_", canonical_alias)
            public_aliases = self._public_aliases_for_cross_model_agg(
                slot=agg_slot,
                source_relation=source_relation,
                canonical_alias=canonical_alias,
            )
            for pub in public_aliases:
                if pub == agg_col_alias:
                    combined_parts.append(f'{cte_name}."{agg_col_alias}"')
                else:
                    combined_parts.append(
                        f'{cte_name}."{agg_col_alias}" AS "{pub}"',
                    )
            combined_aliases_by_slot_id[plan.aggregate_slot_id] = list(
                public_aliases,
            )

        from_clause_str = "FROM _base"
        joined_cte_names: set = set()
        for plan in planned_query.cross_model_aggregate_plans:
            canonical_alias = canonical_alias_for_plan[plan.aggregate_slot_id]
            cte_name = _cte_name_from_alias("_cm_", canonical_alias)
            if cte_name in joined_cte_names:
                continue
            joined_cte_names.add(cte_name)
            joinback_pairs = joinback_pairs_for_plan.get(
                plan.aggregate_slot_id, [],
            )
            if joinback_pairs:
                join_parts = [
                    f'_base."{host}" = {cte_name}."{cte_col}"'
                    for host, cte_col in joinback_pairs
                ]
                from_clause_str += (
                    f"\nLEFT JOIN {cte_name} ON " + _SQL_AND_JOINER.join(join_parts)
                )
            else:
                from_clause_str += f"\nCROSS JOIN {cte_name}"

        combined_select_sql = (
            f"SELECT {', '.join(combined_parts)}\n{from_clause_str}"
        )

        # DEV-1450 stage 7b.15e (C2): a transform layer over a cross-model
        # aggregate (``cumsum(customers.avg_score:avg)``) runs on TOP of the
        # combined cross-model result — the combined SELECT becomes the base
        # CTE and the window step CTEs / outer wrap are layered above it.
        if planned_query.transform_layers:
            return self._render_cross_model_transform_chain(
                prelude_ctes=[("_base", base_cte_sql)] + cm_ctes,
                combined_select_sql=combined_select_sql,
                planned_query=planned_query,
                slots_by_id=slots_by_id,
                combined_aliases_by_slot_id=combined_aliases_by_slot_id,
                source_relation=source_relation,
            )

        all_ctes = [("_base", base_cte_sql)] + cm_ctes + [("_combined", combined_select_sql)]

        # Stitch the WITH chain together. Inner CTEs first; the final
        # ``_combined`` is the outermost FROM target.
        cte_strs = [f"{name} AS (\n{sql}\n)" for name, sql in all_ctes[:-1]]
        sql = f"WITH {', '.join(cte_strs)}\n{combined_select_sql}"

        # ORDER BY / LIMIT / OFFSET: emitted at the combined SELECT
        # level. ORDER BY columns must be qualified — ``_base`` columns
        # use ``_base."..."``, cross-model columns use the bare alias
        # (only present on one side).
        order_sql = self._build_combined_order_by_sql(
            planned_query=planned_query,
            slots_by_id=slots_by_id,
            cma_slot_ids=cma_slot_ids,
            cm_alias_for_plan=canonical_alias_for_plan,
            bare_order_slot_ids=set(order_only_local_ids),
        )
        if order_sql:
            sql += "\n" + order_sql
        if planned_query.limit is not None:
            sql += f"\nLIMIT {planned_query.limit}"
        if planned_query.offset is not None:
            sql += f"\nOFFSET {planned_query.offset}"

        # Outer projection trim — the inner already projects the public
        # list in declared order, so the trim is normally a no-op. Skip
        # the trim machinery here because the legacy path goes through
        # an EnrichedQuery-driven ``_apply_outer_projection_trim`` that
        # we don't have on the new side. Future slices may re-enable.
        return sql

    def _render_cross_model_transform_chain(
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
            step_name = f"step{step_num}"
            prev_cte = ctes[-1][0]
            carry_aliases_sorted = sorted(
                a for aliases in aliases_by_slot_id.values() for a in aliases
            )
            step_parts = [f'"{a}"' for a in carry_aliases_sorted]
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
                    step_parts.append(f'{window_sql} AS "{full_alias}"')
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
            step_name = f"step{step_num}"
            prev_cte = ctes[-1][0]
            carry_aliases_sorted = sorted(
                a for aliases in aliases_by_slot_id.values() for a in aliases
            )
            step_parts = [f'"{a}"' for a in carry_aliases_sorted]
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
                step_parts.append(f'{expr_sql} AS "{full_alias}"')
                aliases_by_slot_id.setdefault(cslot.id, []).append(full_alias)
                available_alias_by_slot_id.setdefault(cslot.id, full_alias)
            step_sql = (
                "SELECT\n    "
                + _SQL_COL_SEP.join(step_parts)
                + f"\nFROM {prev_cte}"
            )
            ctes.append((step_name, step_sql))

        final_cte = ctes[-1][0]
        inner_sorted = sorted(
            a for aliases in aliases_by_slot_id.values() for a in aliases
        )
        inner_sql = (
            "SELECT\n    "
            + _SQL_COL_SEP.join(f'"{a}"' for a in inner_sorted)
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
                f"SELECT *\nFROM (\n{chain_sql}\n) AS _filtered"
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
        outer_sql = (
            "SELECT\n    "
            + _SQL_COL_SEP.join(f'"{a}"' for a in public_aliases_user_order)
            + f"\nFROM (\n{chain_sql}\n) AS _outer"
        )

        return self._apply_order_limit_to_planned_sql_string(
            sql=outer_sql,
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
        from slayer.core.refs import canonical_agg_name

        path = getattr(key.source, "path", ())
        measure_name = (
            key.source.leaf if hasattr(key.source, "leaf") else "*"
        )
        # DEV-1450 stage 7b.13: include kwarg suffix in cross-model
        # alias so two distinct parametric aggs (``percentile(p=0.5)``
        # vs ``p=0.95``) produce distinct CTE names and column aliases.
        # Legacy enrichment at ``query_engine.py:2160`` drops the
        # signature suffix entirely -- a known legacy bug that
        # produces ALIAS COLLISION when the same query has multiple
        # parametric aggs against the same target.column. The new
        # pipeline preserves slot identity here for correctness;
        # parity tests for parametric cross-model aggs assert
        # structural shape rather than bit-identical SQL.
        canonical = canonical_agg_name(
            measure_name=measure_name,
            aggregation_name=key.agg,
            agg_args=[agg_kwarg_canonical_str(a) for a in key.args] or None,
            agg_kwargs={
                k: agg_kwarg_canonical_str(v) for k, v in key.kwargs
            } or None,
        )
        if path:
            return f"{source_relation}." + ".".join(path) + f".{canonical}"
        return f"{source_relation}.{canonical}"

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
            AggregateKey,
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
        cte_group_by: List[exp.Expression] = []
        shared_grain_aliases: List[str] = []
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
                col_expr = self._parse(self._expand_derived_column_sql(
                    source_model=target_model,
                    source_relation=target_relation,
                    column_name=grain_column.column_name,
                    bundle=bundle,
                ))
                leaf = grain_column.column_name
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
            cte_group_by.append(col_expr)
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
        # Re-root the aggregate SOURCE and any column-valued kwargs to the
        # target's local scope (path=()). Covers a derived (ColumnSqlKey)
        # source like ``customers.net:sum`` — Codex: otherwise the
        # host-rooted derived key renders against the wrong alias inside the
        # CTE. StarKey ignores path (COUNT(*)), so leave it as-is.
        _src = agg_slot.key.source
        cross_model_path = getattr(_src, "path", ())
        if isinstance(_src, ColumnKey):
            local_source_key = ColumnKey(path=(), leaf=_src.leaf)
        elif isinstance(_src, ColumnSqlKey):
            local_source_key = ColumnSqlKey(
                path=(), model=_src.model, column_name=_src.column_name,
            )
        else:
            local_source_key = _src

        def _reroot_kwarg(kval):
            if isinstance(kval, ColumnKey) and kval.path == cross_model_path:
                return ColumnKey(path=(), leaf=kval.leaf)
            if isinstance(kval, ColumnSqlKey) and kval.path == cross_model_path:
                return ColumnSqlKey(
                    path=(), model=kval.model, column_name=kval.column_name,
                )
            return kval

        local_kwargs = tuple(
            (k, _reroot_kwarg(v)) for k, v in agg_slot.key.kwargs
        )
        # DEV-1476 bug (c): symmetric reroot of positional args. An explicit
        # time arg ``customers.amount:last(customers.signup_at)`` arrives
        # here with ``key.args=(ColumnKey(path=("customers",),
        # leaf="signup_at"),)``. Without rerooting, ``_resolve_explicit_
        # time_col`` qualifies the time column under the wrong alias inside
        # the target-rooted CTE. ``_reroot_kwarg`` already does the right
        # thing for ``ColumnKey`` / ``ColumnSqlKey``; reuse it.
        local_args = tuple(
            _reroot_kwarg(a) if isinstance(a, (ColumnKey, ColumnSqlKey)) else a
            for a in agg_slot.key.args
        )
        local_agg_key = AggregateKey(
            source=local_source_key,
            agg=agg_slot.key.agg,
            args=local_args,
            kwargs=local_kwargs,
            column_filter_key=agg_slot.key.column_filter_key,
        )
        # The local_agg_key was built from the target's own column.
        # column_filter_key (if set) carries the canonical filter SQL
        # from the target's Column.filter — the synth helper qualifies
        # bare refs against target_model.
        local_slot = agg_slot.model_copy(update={"key": local_agg_key})
        synth = self._build_agg_render_spec_from_planned(
            slot=local_slot,
            key=local_agg_key,
            source_model=target_model,
            source_relation=target_relation,
            full_alias=full_agg_alias,
            bundle=bundle,
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
        # ``_first_rn`` / ``_last_rn`` column.
        is_first_or_last = local_agg_key.agg in ("first", "last")
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
        where_parts: List[exp.Expression] = []
        for filter_text in plan.target_model_filters:
            # DEV-1450 #4b: a target model filter that references a
            # non-trivial derived column must be inline-expanded (it would
            # otherwise emit ``target.derived_col`` — a non-existent column);
            # base-only filters keep the AST bare-ref qualification.
            cols = parse_sql_predicate(filter_text).columns
            if any(
                self._is_nontrivial_derived(target_model, c) for c in cols
            ):
                qualified = self._render_model_filter_sql(
                    sql=filter_text,
                    columns=cols,
                    source_model=target_model,
                    source_relation=target_relation,
                    bundle=bundle,
                )
            else:
                qualified = self._qualify_column_filter_sql(
                    canonical_sql=filter_text,
                    source_relation=target_relation,
                    source_model=target_model,
                )
            if not qualified:
                continue
            try:
                where_parts.append(self._parse_predicate(qualified))
            except Exception:
                raise ValueError(
                    f"Target model filter on {target_model_name!r} could "
                    f"not be parsed: {filter_text!r}",
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
        target_from = self._build_from_clause_from_planned(
            source_model=target_model, source_relation=target_relation,
        )
        ranked_from: Optional[exp.Expression] = None
        if is_first_or_last:
            assert time_col_sql is not None  # narrowed by the guard above
            ranked_from, rn_suffix_map, filtered_rn_map, filtered_match_map = (
                self._build_ranked_subquery_from_planned(
                    source_relation=target_relation,
                    default_time_col_sql=time_col_sql,
                    partition_exprs=list(cte_group_by),
                    extra_projections=[],
                    synth_specs=[synth],
                    from_clause=target_from,
                    base_joins=[],
                    where_clause=combined_where,
                )
            )
            agg_expr, is_agg = self._build_agg(
                synth,
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

        cte_sql = cte_select.sql(dialect=self.dialect, pretty=True)
        return cte_sql, shared_grain_aliases

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
            ColumnKey,
            ColumnSqlKey,
            InKey,
            LiteralKey,
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
            from slayer.core.keys import AggregateKey as _AggKey
            local_source = ColumnKey(path=(), leaf=value_key.source.leaf) \
                if isinstance(value_key.source, ColumnKey) else value_key.source
            local_agg = _AggKey(
                source=local_source,
                agg=value_key.agg,
                args=value_key.args,
                kwargs=value_key.kwargs,
                column_filter_key=value_key.column_filter_key,
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

        Mirrors the small subset of operators the bound-filter renderer
        emits: comparisons (``==``, ``!=``, ``<``, ``<=``, ``>``,
        ``>=``, ``is``, ``is not``), boolean (``and``, ``or``, ``not``),
        arithmetic (``+``, ``-``, ``*``, ``/``).
        """
        if op == "not":
            return exp.Not(this=operands[0])
        # ``and`` / ``or`` (Codex round 2): the binder produces n-ary
        # boolean ``ArithmeticKey`` for ``a AND b AND c`` (three operands);
        # the prior implementation took only ``operands[0]`` / ``[1]`` and
        # silently dropped the third predicate from cross-model HAVING/
        # WHERE, broadening results. Fold over every operand the same
        # way ``_compose_arithmetic_op`` and ``_build_arithmetic_for_filter``
        # already do.
        if op in ("and", "or"):
            node_cls = exp.And if op == "and" else exp.Or
            acc = operands[0]
            for o in operands[1:]:
                acc = node_cls(this=acc, expression=o)
            return acc
        left, right = operands[0], operands[1]
        # ``IS`` / ``IS NOT`` (Codex review): the typed pipeline's filter
        # normalizer lowers SQL ``IS NULL`` / ``IS NOT NULL`` to Python
        # ``is None`` / ``is not None``. Render against a ``Null`` literal
        # as the standard SQL forms.
        if op == "is":
            return exp.Is(this=left, expression=right)
        if op == "is not":
            return exp.Not(this=exp.Is(this=left, expression=right))
        op_map = {
            "==": exp.EQ,
            "!=": exp.NEQ,
            "<": exp.LT,
            "<=": exp.LTE,
            ">": exp.GT,
            ">=": exp.GTE,
            "+": exp.Add,
            "-": exp.Sub,
            "*": exp.Mul,
            "/": exp.Div,
        }
        cls = op_map.get(op)
        if cls is None:
            raise NotImplementedError(
                f"DEV-1450 stage 7b.12: arithmetic operator {op!r} not "
                f"supported in cross-model filter rendering.",
            )
        return cls(this=left, expression=right)

    def _build_combined_order_by_sql(
        self,
        *,
        planned_query,
        slots_by_id: Dict[str, Any],
        cma_slot_ids: Set[str],
        cm_alias_for_plan: Dict[str, str],
        bare_order_slot_ids: Optional[Set[str]] = None,
    ) -> Optional[str]:
        """Build the ORDER BY clause for the combined SELECT.

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
        """
        if not planned_query.order:
            return None
        bare_ids = bare_order_slot_ids or set()
        parts: List[str] = []
        for entry in planned_query.order:
            direction = "ASC" if entry.direction == "asc" else "DESC"
            slot = slots_by_id.get(entry.slot_id)
            if slot is None:
                continue
            if entry.slot_id in cma_slot_ids:
                alias = cm_alias_for_plan.get(entry.slot_id)
                if alias is None:
                    continue
                parts.append(f'"{alias}" {direction}')
            else:
                full_alias = self._full_alias_for_slot(
                    slot=slot,
                    source_relation=planned_query.source_relation,
                    alias_index={},
                )
                if entry.slot_id in bare_ids:
                    parts.append(f'"{full_alias}" {direction}')
                else:
                    parts.append(f'_base."{full_alias}" {direction}')
        if not parts:
            return None
        return "ORDER BY " + ", ".join(parts)

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

        DEV-1450 stage 7b.12: joined ROW slots (``ColumnKey.path != ()``
        / ``TimeTruncKey.column.path != ()``) emit the FULL dotted
        result-key form (``orders.customers.region_id``), preserving
        the result-key contract (P10). The planner's flat
        ``declared_name`` is the DEV-1449 / C4 downstream-stage binding
        name and remains untouched on the slot for stage-2 references;
        only the public SQL alias differs.
        """
        from slayer.core.keys import (
            ColumnKey,
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
            elif isinstance(key, TimeTruncKey):
                # DEV-1450 #4a: a derived TD's leaf is its column_name, so the
                # public result-key shape matches the base-column TD.
                path, leaf = column_path(key.column), column_leaf(key.column)
            if path and leaf is not None:
                return f"{source_relation}." + ".".join(path) + f".{leaf}"
        # Local + AGGREGATE / POST slots: existing alias selection.
        if slot.public_aliases:
            alias = self._pick_alias_for_planned_slot(
                slot=slot, alias_index=alias_index,
            )
        else:
            alias = slot.declared_name
        return f"{source_relation}.{alias}"

    def _collect_joined_paths_for_base(  # NOSONAR(S3776) — sequential per-slot dispatch over ROW (ColumnKey / TimeTruncKey path) vs AGGREGATE (top-level AggregateKey first/last + composite first/last leaves) classification. Each branch is the per-slot join-discovery contract; extracting per-shape helpers would scatter the contract.
        self,
        *,
        base_render_order: List[str],
        slots_by_id: Dict[str, Any],
    ) -> List[Tuple[str, ...]]:
        """Walk ROW slots in render order to collect unique joined paths.

        Only paths needed for projection / GROUP BY surface here. Cross-
        model aggregate slots are NEVER walked — their joins live in
        ``CrossModelAggregatePlan.join_chain`` and are rendered inside
        the per-plan ``_cm_*`` CTE.

        Local ``first`` / ``last`` AGGREGATE slots additionally contribute
        any joined path named by an explicit ranking-time arg
        (``amount:last(stores.opened_at)``) — the ranked subquery's
        ``ORDER BY`` references that column, so the join must be in scope.
        """
        from slayer.core.keys import AggregateKey, ColumnKey, Phase, TimeTruncKey

        seen: set = set()
        ordered: List[Tuple[str, ...]] = []

        def _add(path: Tuple[str, ...]) -> None:
            if not path or path in seen:
                return
            seen.add(path)
            ordered.append(path)

        for sid in base_render_order:
            slot = slots_by_id.get(sid)
            if slot is None:
                continue
            key = slot.key
            if slot.phase == Phase.ROW:
                if isinstance(key, ColumnKey):
                    _add(key.path)
                elif isinstance(key, TimeTruncKey):
                    _add(key.column.path)
            elif slot.phase == Phase.AGGREGATE:
                # DEV-1501 (Codex round 5): walk top-level AggregateKey
                # slots AND first/last leaves inside composite slots
                # (ArithmeticKey / ScalarCallKey). A composite operand
                # ``amount:last(stores.opened_at) + 1`` orders the ranked
                # subquery by ``stores.opened_at`` and requires the
                # ``stores`` join to be in scope.
                fl_keys: list = []
                if (
                    isinstance(key, AggregateKey)
                    and key.agg in ("first", "last")
                    and not getattr(key.source, "path", ())
                ):
                    fl_keys = [key]
                elif not isinstance(key, AggregateKey):
                    fl_keys = _iter_first_last_leaves(key)
                for fl in fl_keys:
                    for a in fl.args:
                        if isinstance(a, ColumnKey):
                            _add(a.path)
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
                        join_on_parts.append(exp.EQ(
                            this=exp.Column(
                                this=exp.to_identifier(src_col),
                                table=exp.to_identifier(current_alias),
                            ),
                            expression=exp.Column(
                                this=exp.to_identifier(tgt_col),
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
                        join_expr = exp.to_table(target_table, alias=next_alias)
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
            measure = f'"{input_alias}"'

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
            time_alias = f'"{available_alias_by_slot_id[tk_sid]}"'

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
            _SQL_PARTITION_BY + ", ".join(f'"{a}"' for a in partition_aliases)
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

    def _render_post_phase_filter_conditions(
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
        ``LiteralKey`` compose recursively.
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
            TimeTruncKey,
            TransformKey,
        )

        slotted_kinds = (
            ColumnKey, ColumnSqlKey, TimeTruncKey, AggregateKey, TransformKey,
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
            v = key.value
            if v is None:
                return exp.Null()
            if isinstance(v, bool):
                return exp.true() if v else exp.false()
            if isinstance(v, (int, float, Decimal)):
                return exp.Literal.number(str(v))
            return exp.Literal.string(str(v))

        if isinstance(key, ArithmeticKey):
            operands = [
                self._render_value_key_against_aliases(
                    key=o,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                )
                for o in key.operands
            ]
            return self._compose_arithmetic_op(op=key.op, operands=operands)

        if isinstance(key, ScalarCallKey):
            args = []
            for a in key.args:
                if isinstance(
                    a,
                    (
                        TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey,
                        InKey, ColumnKey, ColumnSqlKey, TimeTruncKey,
                        AggregateKey, LiteralKey,
                    ),
                ):
                    args.append(self._render_value_key_against_aliases(
                        key=a,
                        slot_id_by_key=slot_id_by_key,
                        available_alias_by_slot_id=available_alias_by_slot_id,
                    ))
                elif a is None:
                    args.append(exp.Null())
                elif isinstance(a, bool):
                    args.append(exp.true() if a else exp.false())
                elif isinstance(a, (int, float, Decimal)):
                    args.append(exp.Literal.number(str(a)))
                else:
                    args.append(exp.Literal.string(str(a)))
            if key.name == "like":
                return exp.Like(this=args[0], expression=args[1])
            return exp.func(key.name.upper(), *args)

        if isinstance(key, BetweenKey):
            col = self._render_value_key_against_aliases(
                key=key.column,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            )
            low = self._render_value_key_against_aliases(
                key=key.low,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            )
            high = self._render_value_key_against_aliases(
                key=key.high,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            )
            return exp.Between(this=col, low=low, high=high)

        if isinstance(key, InKey):
            # DEV-1475: POST-phase IN filter — LHS column resolves to a
            # quoted alias materialised in the ``_filtered`` wrapper; RHS
            # literals are inlined as bare sqlglot scalars.
            col = self._render_value_key_against_aliases(
                key=key.column,
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id=available_alias_by_slot_id,
            )
            value_exprs = [
                self._render_value_key_against_aliases(
                    key=lit,
                    slot_id_by_key=slot_id_by_key,
                    available_alias_by_slot_id=available_alias_by_slot_id,
                )
                for lit in key.values
            ]
            in_expr = exp.In(this=col, expressions=value_exprs)
            return exp.Not(this=in_expr) if key.negated else in_expr

        raise NotImplementedError(
            f"DEV-1450 stage 7b.10: POST-phase filter key type "
            f"{type(key).__name__} not yet supported.",
        )

    @staticmethod
    def _paren_if_lower_prec(
        child: exp.Expression, *, parent_prec: int, is_right: bool, op: str,
    ) -> exp.Expression:
        """Wrap ``child`` in parens when its arithmetic precedence is lower
        than the parent op's (or equal, for the RIGHT operand of the
        non-associative ``-`` / ``/``). Leaves / functions / casts / already-
        parenthesised nodes are returned untouched.
        """
        child_prec = {
            exp.Add: 1, exp.Sub: 1, exp.Mul: 2, exp.Div: 2,
        }.get(type(child))
        if child_prec is None:
            return child
        if child_prec < parent_prec:
            return exp.Paren(this=child)
        if child_prec == parent_prec and is_right and op in ("-", "/"):
            return exp.Paren(this=child)
        return child

    @staticmethod
    def _compose_arithmetic_op(
        *, op: str, operands: List[exp.Expression],
    ) -> exp.Expression:
        """Compose an arithmetic / comparison / boolean operator over
        already-rendered operands.

        Accepts the operator aliases ``=``/``==``, ``<>``/``!=`` so the
        rendered SQL surfaces the canonical SQL spellings for POST
        filters. Unary ``-`` and N-ary ``and``/``or`` left-fold to the
        sqlglot binary nodes.
        """
        if len(operands) == 1:
            if op == "not":
                return exp.Not(this=operands[0])
            if op == "-":
                return exp.Neg(this=operands[0])
        if len(operands) == 2:
            lhs, rhs = operands
            # ``IS`` / ``IS NOT`` (Codex review): see ``_build_arith_or_cmp_ast``.
            if op == "is":
                return exp.Is(this=lhs, expression=rhs)
            if op == "is not":
                return exp.Not(this=exp.Is(this=lhs, expression=rhs))
            binary = {
                "+": exp.Add, "-": exp.Sub, "*": exp.Mul, "/": exp.Div,
                "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
                "==": exp.EQ, "=": exp.EQ,
                "!=": exp.NEQ, "<>": exp.NEQ,
            }
            if op in binary:
                # sqlglot does NOT add precedence parens for a nested AST, so
                # ``Div(Sub(a, b), c)`` would render as ``a - b / c`` (wrong:
                # ``b / c`` binds first). Parenthesise a lower-precedence
                # operand — and an equal-precedence RIGHT operand under the
                # non-associative ``-`` / ``/`` — so ``change_pct`` and friends
                # emit ``(a - b) / c``.
                arith_prec = {"+": 1, "-": 1, "*": 2, "/": 2}
                parent_prec = arith_prec.get(op)
                if parent_prec is not None:
                    lhs = SQLGenerator._paren_if_lower_prec(
                        lhs, parent_prec=parent_prec, is_right=False, op=op,
                    )
                    rhs = SQLGenerator._paren_if_lower_prec(
                        rhs, parent_prec=parent_prec, is_right=True, op=op,
                    )
                return binary[op](this=lhs, expression=rhs)
            if op == "and":
                return exp.And(this=lhs, expression=rhs)
            if op == "or":
                return exp.Or(this=lhs, expression=rhs)
        if len(operands) >= 2 and op in ("and", "or"):
            node_cls = exp.And if op == "and" else exp.Or
            acc = operands[0]
            for rhs in operands[1:]:
                acc = node_cls(this=acc, expression=rhs)
            return acc
        raise NotImplementedError(
            f"DEV-1450 stage 7b.10: arithmetic op {op!r} arity "
            f"{len(operands)} not supported in POST-filter rendering.",
        )

    def _apply_order_limit_to_planned_sql_string(
        self,
        *,
        sql: str,
        planned_query,
        slots_by_id: Dict[str, Any],
        available_alias_by_slot_id: Dict[str, str],
    ) -> str:
        """Apply ORDER BY / LIMIT / OFFSET to a raw SQL string.

        Mirrors legacy ``_apply_pagination_to_sql`` but resolves order
        targets through the typed plan: each ``OrderEntry`` slot id is
        looked up in ``available_alias_by_slot_id`` (which includes
        every public + materialised alias).
        """
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
            order_parts.append(f'"{alias}" {direction}')
        if order_parts:
            sql += "\nORDER BY " + ", ".join(order_parts)
        if planned_query.limit is not None:
            sql += f"\nLIMIT {planned_query.limit}"
        if planned_query.offset is not None:
            sql += f"\nOFFSET {planned_query.offset}"
        return sql

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
    ) -> List[str]:
        """Build the WHERE clauses for the shifted CTE that re-aggregates
        the source relation.

        7b.3c invariant: ``BetweenKey`` filters (those derived from
        ``TimeDimension.date_range``) MUST be omitted from the shifted
        inner CTE so the earliest visible bucket can still carry a
        non-null shifted value. Other ROW-phase filters
        (e.g. ``status = 'active'``) are propagated unchanged so the
        shifted aggregation runs over the same row population.
        AGGREGATE / POST phase filters never apply to the shifted CTE
        (they're outer-projection concerns).
        """
        from slayer.core.keys import BetweenKey, Phase

        def _guard_no_joined_refs(rendered_part: exp.Expression, *, fid) -> None:
            # The shifted CTE re-aggregates the bare source (no joins), so a
            # ROW filter referencing a joined column cannot be applied here.
            # This combination (time_shift + joined-column filter) is deferred
            # — raise loudly rather than emit SQL that references an unjoined
            # alias.
            for c in rendered_part.find_all(exp.Column):
                tbl = c.args.get("table")
                if tbl is not None and tbl.name not in (
                    source_relation, source_model.name,
                ):
                    raise NotImplementedError(
                        f"DEV-1450: time_shift combined with a ROW filter on "
                        f"a joined column ({tbl.name}.{c.name}) is not yet "
                        f"supported (the shifted CTE carries no joins). "
                        f"filter id={fid!r}."
                    )

        out: List[str] = []
        for fp in planned_query.filters_by_phase:
            if fp.phase != Phase.ROW:
                continue
            if fp.expression is not None:
                if isinstance(fp.expression.value_key, BetweenKey):
                    # date_range filter — omit from inner shifted CTE.
                    continue
                rendered = self._render_value_key_for_filter(
                    key=fp.expression.value_key,
                    source_relation=source_relation,
                    source_model=source_model,
                    bundle=bundle,
                )
                if isinstance(rendered, (exp.And, exp.Or)):
                    rendered = exp.Paren(this=rendered)
                _guard_no_joined_refs(rendered, fid=fp.id)
                out.append(rendered.sql(dialect=self.dialect))
            elif fp.text is not None:
                qualified = self._render_model_filter_sql(
                    sql=fp.text,
                    columns=fp.text_columns,
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                )
                _guard_no_joined_refs(self._parse(qualified), fid=fp.id)
                out.append(qualified)
        return out

    def _emit_time_shift_ctes_for_planned(
        self,
        *,
        slot,
        ctes: list,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
        aliases_by_slot_id: Dict[str, List[str]],
        source_model,
        source_relation: str,
        shifted_where_parts: List[str],
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

        # 3. partition_keys (DEV-1450 C6) + auto-include query dimensions.
        #
        # Legacy auto-joins on EVERY query dimension regardless of
        # partition_by (``_generate_with_computed:1559``). Without this,
        # ``time_shift(amount:sum, periods=-1)`` with ``status`` in
        # ``dimensions`` would broadcast the prior-period total across
        # every status value. The typed pipeline mirrors this: explicit
        # ``partition_keys`` may add MORE columns (DEV-1450 C6), but
        # query dimensions are always included.
        from slayer.core.keys import Phase as _Phase
        partition_specs: list[tuple[str, str, exp.Expression]] = []
        # entries: (slot_id, full_alias, raw_column_expr_for_group_by)
        seen_partition_sids: set = set()

        def _add_partition(pk_obj, *, where: str) -> None:
            pk_sid = slot_id_by_key.get(pk_obj)
            if pk_sid is None or pk_sid not in available_alias_by_slot_id:
                raise RuntimeError(
                    f"time_shift {where} not materialised: "
                    f"slot id={slot.id!r}, key={pk_obj!r}.",
                )
            if pk_sid in seen_partition_sids:
                return
            pk_alias = available_alias_by_slot_id[pk_sid]
            if isinstance(pk_obj, ColumnKey):
                if pk_obj.path != ():
                    raise NotImplementedError(
                        f"DEV-1450 stage 7b.12: cross-model partition "
                        f"(path={pk_obj.path!r}) deferred to the "
                        f"cross-model slice (slot id={slot.id!r}).",
                    )
                col_expr = self._dim_column_expr_from_planned(
                    source_model=source_model,
                    source_relation=source_relation,
                    leaf=pk_obj.leaf,
                )
            else:
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.11: partition on "
                    f"{type(pk_obj).__name__} not supported (only "
                    f"ColumnKey leaves render in the shifted CTE).",
                )
            partition_specs.append((pk_sid, pk_alias, col_expr))
            seen_partition_sids.add(pk_sid)

        # Auto-include query-dimension ColumnKey row slots (NOT TimeTruncKey;
        # the time-key is already the time-join axis).
        for sid in planned_query.projection:
            dim_slot = slots_by_id.get(sid)
            if dim_slot is None or dim_slot.phase != _Phase.ROW:
                continue
            if not isinstance(dim_slot.key, ColumnKey):
                continue
            _add_partition(dim_slot.key, where="query dimension")

        # Explicit partition_keys (DEV-1450 C6) may add more.
        for pk in sorted(key.partition_keys, key=lambda k: repr(k)):
            _add_partition(pk, where="partition_key")

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
        # DEV-1450 #4a: a derived (ColumnSqlKey) time column yields its
        # EXPANDED expression here; the calendar offset and DATE_TRUNC apply
        # over that expression exactly as over a bare column.
        raw_time_col_expr = self._raw_time_col_expr_for_planned(
            time_column=time_key.column,
            source_model=source_model,
            source_relation=source_relation,
            bundle=bundle,
        )
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
            f'{shifted_trunc_sql} AS "{time_alias}"',
        )
        shifted_group_by.append(shifted_trunc_sql)

        # partition_keys: SELECT + GROUP BY under their base aliases.
        for _, pk_alias, pk_expr in partition_specs:
            pk_sql = pk_expr.sql(dialect=self.dialect)
            shifted_select_parts.append(f'{pk_sql} AS "{pk_alias}"')
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
                f'{agg_expr.sql(dialect=self.dialect)} AS "{input_alias}"',
            )
        else:
            # Row-level column input (not aggregated). Pass-through.
            col_expr = self._dim_column_expr_from_planned(
                source_model=source_model,
                source_relation=source_relation,
                leaf=inner_key.leaf,
            )
            shifted_select_parts.append(
                f'{col_expr.sql(dialect=self.dialect)} AS "{input_alias}"',
            )
            shifted_group_by.append(col_expr.sql(dialect=self.dialect))

        from_clause = self._build_from_clause_from_planned(
            source_model=source_model, source_relation=source_relation,
        )
        from_sql = from_clause.sql(dialect=self.dialect)

        shifted_sql_parts = [
            "SELECT\n  " + ",\n  ".join(shifted_select_parts),
            f"FROM {from_sql}",
        ]
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
        slot_aliases: List[str] = list(slot.public_aliases) or [slot.declared_name]
        cte_name_alias = slot_aliases[0]
        shifted_cte_name = f"shifted_{cte_name_alias}"
        sjoin_cte_name = f"sjoin_{cte_name_alias}"

        ctes.append((shifted_cte_name, shifted_sql))

        # Build the sjoin CTE: LEFT JOIN prev_cte + shifted on time +
        # partition equalities. Carry every prev_cte alias forward,
        # then add the shifted measure under EACH of the slot's public
        # aliases (DEV-1450 C13).
        prev_cte = ctes[-2][0]  # the CTE just before the shifted CTE
        carry_aliases_sorted = sorted(
            a for aliases in aliases_by_slot_id.values() for a in aliases
        )
        sjoin_select_parts = [
            f'{prev_cte}."{a}"' for a in carry_aliases_sorted
        ]
        slot_full_aliases: List[str] = []
        for slot_alias in slot_aliases:
            full_slot_alias = f"{source_relation}.{slot_alias}"
            slot_full_aliases.append(full_slot_alias)
            sjoin_select_parts.append(
                f'{shifted_cte_name}."{input_alias}" AS "{full_slot_alias}"',
            )

        # JOIN conditions: time equality + every partition equality.
        join_conds = [
            f'{prev_cte}."{time_alias}" = {shifted_cte_name}."{time_alias}"',
        ]
        for _, pk_alias, _ in partition_specs:
            join_conds.append(
                f'{prev_cte}."{pk_alias}" = {shifted_cte_name}."{pk_alias}"',
            )

        sjoin_sql = (
            "SELECT " + ", ".join(sjoin_select_parts)
            + f"\nFROM {prev_cte}"
            + f"\nLEFT JOIN {shifted_cte_name}"
            + "\n    ON " + _SQL_AND_JOINER.join(join_conds)
        )
        ctes.append((sjoin_cte_name, sjoin_sql))

        # Record EACH alias in both the per-slot list (C13 carry-forward
        # in the outer SELECT) and the "pick one" map (transform input /
        # filter / order lookups by downstream layers).
        for full_slot_alias in slot_full_aliases:
            aliases_by_slot_id.setdefault(slot.id, []).append(full_slot_alias)
        # ``available_alias_by_slot_id`` is "pick one" — first alias wins.
        available_alias_by_slot_id.setdefault(slot.id, slot_full_aliases[0])

    def _emit_consecutive_periods_ctes_for_planned(
        self,
        *,
        slot,
        ctes: list,
        slots_by_id: Dict[str, Any],
        slot_id_by_key: Dict[Any, str],
        available_alias_by_slot_id: Dict[str, str],
        aliases_by_slot_id: Dict[str, List[str]],
        planned_query,
        source_relation: str,
    ) -> None:
        """Emit ``cp_reset_<alias>`` + ``cp_value_<alias>`` CTEs for one
        consecutive_periods transform slot.

        Legacy reference: ``_build_consecutive_periods_ctes`` in this
        file. The typed implementation differs in two principled ways:

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
                f'"{input_alias}" IS NOT NULL AND "{input_alias}" <> 0'
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

        slot_alias = (
            slot.public_aliases[0]
            if slot.public_aliases
            else slot.declared_name
        )
        full_slot_alias = f"{source_relation}.{slot_alias}"
        cp_reset_alias = f"_cp_reset_{full_slot_alias}"

        # Build the reset CTE.
        prev_cte = ctes[-1][0]
        carry_aliases_sorted = sorted(
            a for aliases in aliases_by_slot_id.values() for a in aliases
        )
        carry_select = ",\n  ".join(f'"{a}"' for a in carry_aliases_sorted)
        partition_clause = (
            _SQL_PARTITION_BY + ", ".join(f'"{a}"' for a in partition_aliases)
            if partition_aliases
            else ""
        )
        over_reset = " ".join(p for p in (
            partition_clause,
            f'ORDER BY "{time_alias}"',
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        ) if p)
        reset_window_sql = (
            f'SUM(CASE WHEN {pred_in_case} THEN 0 ELSE 1 END) '
            f'OVER ({over_reset}) AS "{cp_reset_alias}"'
        )
        cp_reset_cte_name = f"cp_reset_{slot_alias}"
        cp_reset_sql = (
            "SELECT\n  " + carry_select
            + ",\n  " + reset_window_sql
            + f"\nFROM {prev_cte}"
        )
        ctes.append((cp_reset_cte_name, cp_reset_sql))

        # Build the value CTE — references the cp_reset CTE's added
        # column in PARTITION BY so each run of true predicate is
        # counted within its own reset group.
        value_partition_aliases = partition_aliases + [cp_reset_alias]
        value_partition_clause = _SQL_PARTITION_BY + ", ".join(
            f'"{a}"' for a in value_partition_aliases
        )
        over_value = " ".join((
            value_partition_clause,
            f'ORDER BY "{time_alias}"',
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
            f'AS "{full_slot_alias}"'
        )
        cp_value_cte_name = f"cp_value_{slot_alias}"
        cp_value_sql = (
            "SELECT\n  " + carry_select
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

    def _build_from_clause_from_planned(
        self,
        *,
        source_model,
        source_relation: str,
    ) -> exp.Expression:
        if source_model.sql_table:
            return exp.to_table(source_model.sql_table, alias=source_relation)
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
                expanded_sql = self._expand_derived_column_sql(
                    source_model=joined_model,
                    source_relation="__".join(time_column.path),
                    column_name=time_column.column_name,
                    bundle=bundle,
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
        """
        root_ids = _root_scope_column_ids(parsed=sql_expr)
        seen: set = set()
        ordered: List[Tuple[str, ...]] = []
        for col in sql_expr.find_all(exp.Column):
            tbl = col.args.get("table")
            if tbl is None or col.args.get("db") or col.args.get("catalog"):
                continue
            if id(col) not in root_ids:
                continue
            alias = tbl.name
            if alias in (source_relation, source_model.name):
                continue
            segments = alias.split("__")
            current = source_model
            resolved = True
            for seg in segments:
                join = next(
                    (j for j in current.joins if j.target_model == seg), None,
                )
                if join is None:
                    resolved = False
                    break
                nxt = bundle.get_referenced_model(seg)
                if nxt is None:
                    resolved = False
                    break
                current = nxt
            if not resolved:
                continue
            for i in range(1, len(segments) + 1):
                prefix = tuple(segments[:i])
                if prefix not in seen:
                    seen.add(prefix)
                    ordered.append(prefix)
        return ordered

    def _collect_filter_join_paths(
        self, *, planned_query, source_model, source_relation: str, bundle,
        skip_filter_ids: Optional[Set[str]] = None,
    ) -> List[Tuple[str, ...]]:
        """Collect the join paths a query's WHERE-phase filters reference so
        the FROM pulls them in.

        Covers three shapes:
        * typed joined column ref (``customers.regions.name == 'US'``) —
          ``ColumnKey.path``;
        * typed derived column whose ``Column.sql`` crosses a join
          (``is_eu = 1`` where ``is_eu`` references ``customers.region``) —
          ``ColumnSqlKey``, expanded then scanned;
        * Mode-A ``SlayerModel.filters`` text with a ``__`` join path
          (``customers__regions.name = 'EU'``) — parsed and scanned.
        """
        from slayer.core.keys import (
            ArithmeticKey,
            BetweenKey,
            ColumnKey,
            ColumnSqlKey,
            InKey,
            Phase,
            ScalarCallKey,
        )

        seen: set = set()
        ordered: List[Tuple[str, ...]] = []

        def _add_path(path: Tuple[str, ...]) -> None:
            for i in range(1, len(path) + 1):
                prefix = tuple(path[:i])
                if prefix and prefix not in seen:
                    seen.add(prefix)
                    ordered.append(prefix)

        def _add_from_sql(parsed: exp.Expression) -> None:
            for p in self._joined_paths_in_sql(
                sql_expr=parsed, source_relation=source_relation,
                source_model=source_model, bundle=bundle,
            ):
                if p not in seen:
                    seen.add(p)
                    ordered.append(p)

        def _walk(key) -> None:
            if isinstance(key, ColumnKey):
                if key.path:
                    _add_path(key.path)
            elif isinstance(key, ColumnSqlKey) and not key.path:
                expanded = self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=key.column_name,
                    bundle=bundle,
                )
                _add_from_sql(self._parse(expanded))
            elif isinstance(key, ColumnSqlKey) and key.path:
                # Joined derived-column ref — pull the join walk to the column's
                # owning model into the FROM, plus any further cross-joins the
                # column's own ``sql`` references (expanded under the column's
                # ``__``-path alias, then scanned from the host's perspective).
                _add_path(key.path)
                joined_model = bundle.get_referenced_model(key.path[-1])
                if joined_model is not None:
                    expanded = self._expand_derived_column_sql(
                        source_model=joined_model,
                        source_relation="__".join(key.path),
                        column_name=key.column_name,
                        bundle=bundle,
                    )
                    _add_from_sql(self._parse(expanded))
            elif isinstance(key, ArithmeticKey):
                for o in key.operands:
                    _walk(o)
            elif isinstance(key, ScalarCallKey):
                for a in key.args:
                    _walk(a)
            elif isinstance(key, BetweenKey):
                _walk(key.column)
                _walk(key.low)
                _walk(key.high)
            elif isinstance(key, InKey):
                # DEV-1475: an IN filter on a joined column must still
                # pull the join into the FROM. Only the LHS column can
                # carry a join path; literal RHS values never do.
                _walk(key.column)

        skip = skip_filter_ids or set()
        for fp in planned_query.filters_by_phase:
            if fp.phase != Phase.ROW or fp.id in skip:
                continue
            if fp.expression is not None:
                _walk(fp.expression.value_key)
            elif fp.text is not None:
                # DEV-1450 #4b: expand the model-filter text first so a bare
                # derived-column reference (e.g. ``is_eu`` whose sql crosses a
                # join to ``customers``) surfaces the join its expansion
                # introduces, pulling the LEFT JOIN into the FROM.
                expanded_text = self._render_model_filter_sql(
                    sql=fp.text,
                    columns=fp.text_columns,
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                )
                _add_from_sql(self._parse(expanded_text))
        return ordered

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
        """Reject kwarg column refs whose join path disagrees with the
        aggregate source path.

        A kwarg path that doesn't match the aggregate source path would
        silently bind the kwarg to a different model (host vs joined
        target) than the aggregate value column — meaningless SQL
        semantically. Caller-side cross-model rerooting strips the
        matching prefix from source AND kwargs before reaching this
        point; any residual mismatch surfaces here. Both bare-column
        (``ColumnKey``) and derived-column (``ColumnSqlKey``) kwarg
        refs go through this gate (CodeRabbit fold-in on PR #144).
        """
        from slayer.core.keys import ColumnKey, ColumnSqlKey

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
            # DEV-1450 stage 7b.13: stringify kwargs through the shared
            # helper. ``AggRenderSpec.agg_kwargs`` is ``Dict[str, str]``
            # and downstream ``_validate_agg_param_value`` rejects
            # anything not matching ``_SAFE_AGG_PARAM_RE``; the helper
            # emits identifiers / dotted identifiers / numeric literals
            # that satisfy the regex, and rejects bool / None / unknown
            # types at the boundary.
            agg_kwargs_str = {
                k: agg_kwarg_canonical_str(v) for k, v in key.kwargs
            }
            # DEV-1450 stage 7b.12: propagate ``AggregateKey.column_filter_key``
            # into ``AggRenderSpec.filter_sql`` so ``_build_agg`` wraps the
            # aggregate argument as ``SUM(CASE WHEN <filter> THEN col END)``.
            # Legacy ``resolve_filter_columns`` qualifies bare-identifier refs
            # in the filter with the host model name (so ``status = 'paid'``
            # becomes ``orders.status = 'paid'``); mirror that here on the
            # parsed AST so dialect-independent wiring works in the new
            # pipeline.
            filter_sql = self._qualify_column_filter_sql(
                canonical_sql=(
                    key.column_filter_key.canonical_sql
                    if key.column_filter_key is not None
                    else None
                ),
                source_relation=source_relation,
                source_model=source_model,
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

    def _build_where_having_from_planned(
        self,
        *,
        planned_query,
        source_relation: str,
        source_model,
        bundle,
        skip_filter_ids: Optional[Set[str]] = None,
        first_last_state: Optional[FirstLastRenderState] = None,
        aliases_by_slot_id: Optional[Dict[str, List[str]]] = None,
    ):
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
        for fp in planned_query.filters_by_phase:
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
                # FROM via _collect_filter_join_paths).
                target_parts.append(self._render_model_filter_sql(
                    sql=fp.text,
                    columns=fp.text_columns,
                    source_model=source_model,
                    source_relation=source_relation,
                    bundle=bundle,
                ))
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
        """Render a ``SlayerModel.filters`` Mode-A SQL predicate (DEV-1450 #4b).

        If any name in ``columns`` is a non-trivial DERIVED column on
        ``source_model``, the whole predicate is inline-expanded via
        ``expand_derived_refs_sync`` (AST-based — it also qualifies bare base
        refs and resolves sibling / joined derived refs). Otherwise the
        base-only path (``_qualify_mode_a_sql_filter``) is used unchanged.
        """
        if any(
            self._is_nontrivial_derived(source_model, c) for c in columns
        ):
            # Degenerate case: the whole predicate IS a single bare derived
            # column (``filters=["is_eu"]``). It parses to a root ``exp.Column``,
            # and ``expand_derived_refs_sync`` rewrites refs in place via
            # ``col.replace`` — which is a no-op on the AST root. Expand the
            # column directly so the bare boolean derived filter still inlines.
            parsed_ast = self._parse(sql)
            if (
                isinstance(parsed_ast, exp.Column)
                and parsed_ast.args.get("table") is None
                and self._is_nontrivial_derived(source_model, parsed_ast.name)
            ):
                return self._expand_derived_column_sql(
                    source_model=source_model,
                    source_relation=source_relation,
                    column_name=parsed_ast.name,
                    bundle=bundle,
                )
            expanded = expand_derived_refs_sync(
                sql=sql,
                model=source_model,
                alias_path=source_relation,
                resolve_model=bundle.get_referenced_model,
                dialect=self.dialect,
            )
            return expanded if expanded is not None else sql
        return self._qualify_mode_a_sql_filter(
            sql=sql,
            columns=columns,
            source_model=source_model,
            source_relation=source_relation,
        )

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
        ``_collect_filter_join_paths``), ``ColumnSqlKey`` (derived column —
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
            synth = self._build_agg_render_spec_from_planned(
                slot=slot,
                key=key,
                source_model=source_model,
                source_relation=source_relation,
                full_alias=having_full_alias,
                bundle=bundle,
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
                # ``_collect_filter_join_paths``.
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
                # ``_collect_filter_join_paths`` (which adds ``key.path``).
                joined_model = bundle.get_referenced_model(key.path[-1])
                if joined_model is None:
                    raise ValueError(
                        f"Filter references derived column {key.column_name!r} "
                        f"on joined model {key.path[-1]!r} which is not in the "
                        f"resolved source bundle.",
                    )
                path_alias = "__".join(key.path)
                expanded_sql = self._expand_derived_column_sql(
                    source_model=joined_model,
                    source_relation=path_alias,
                    column_name=key.column_name,
                    bundle=bundle,
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
            # FROM (via ``_collect_filter_join_paths``).
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
            if key.name == "like":
                return exp.Like(this=args[0], expression=args[1])
            return exp.Anonymous(this=key.name.upper(), expressions=args)
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
    def _build_arithmetic_for_filter(
        *, op: str, operands: list,
    ) -> exp.Expression:
        # DSL ``==``/``!=`` map to sqlglot EQ/NEQ; sqlglot then emits the
        # dialect-correct SQL operator (postgres ``=``/``!=``).
        if op in ("==", "="):
            return exp.EQ(this=operands[0], expression=operands[1])
        if op in ("!=", "<>"):
            return exp.NEQ(this=operands[0], expression=operands[1])
        if op == "<":
            return exp.LT(this=operands[0], expression=operands[1])
        if op == "<=":
            return exp.LTE(this=operands[0], expression=operands[1])
        if op == ">":
            return exp.GT(this=operands[0], expression=operands[1])
        if op == ">=":
            return exp.GTE(this=operands[0], expression=operands[1])
        if op == "+":
            # Unary plus is a no-op; legacy never emits it explicitly.
            if len(operands) == 1:
                return operands[0]
            return exp.Add(this=operands[0], expression=operands[1])
        if op == "-":
            # Unary minus: the binder represents ``-x`` / ``-10`` as
            # ``ArithmeticKey(op="-", operands=(x,))`` — handle the
            # single-operand form so a filter like ``amount > -10``
            # doesn't crash with IndexError.
            if len(operands) == 1:
                return exp.Neg(this=operands[0])
            return exp.Sub(
                this=SQLGenerator._paren_if_lower_prec(
                    operands[0], parent_prec=1, is_right=False, op="-",
                ),
                expression=SQLGenerator._paren_if_lower_prec(
                    operands[1], parent_prec=1, is_right=True, op="-",
                ),
            )
        if op == "*":
            return exp.Mul(
                this=SQLGenerator._paren_if_lower_prec(
                    operands[0], parent_prec=2, is_right=False, op="*",
                ),
                expression=SQLGenerator._paren_if_lower_prec(
                    operands[1], parent_prec=2, is_right=True, op="*",
                ),
            )
        if op == "/":
            return exp.Div(
                this=SQLGenerator._paren_if_lower_prec(
                    operands[0], parent_prec=2, is_right=False, op="/",
                ),
                expression=SQLGenerator._paren_if_lower_prec(
                    operands[1], parent_prec=2, is_right=True, op="/",
                ),
            )
        if op == "and":
            result = operands[0]
            for o in operands[1:]:
                result = exp.And(this=result, expression=o)
            return result
        if op == "or":
            result = operands[0]
            for o in operands[1:]:
                result = exp.Or(this=result, expression=o)
            return result
        if op == "not":
            return exp.Not(this=operands[0])
        # ``IS`` / ``IS NOT`` (Codex round 2): the filter normalizer lowers
        # SQL ``IS NULL`` / ``IS NOT NULL`` to Python ``is None`` / ``is
        # not None``. Render against the rhs (a ``Null`` literal) as the
        # standard SQL forms. Without these branches a local-stage filter
        # ``deleted_at IS NULL`` parses and binds but raises here at SQL
        # generation. Mirrors the patches in ``_build_arith_or_cmp_ast``
        # and ``_compose_arithmetic_op``.
        if op == "is":
            return exp.Is(this=operands[0], expression=operands[1])
        if op == "is not":
            return exp.Not(this=exp.Is(this=operands[0], expression=operands[1]))
        raise NotImplementedError(
            f"DEV-1450 stage 7b.8: ArithmeticKey op {op!r} not "
            f"supported in filter rendering."
        )

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
            exp.Subquery(this=base_select, alias=exp.to_identifier("_outer")),
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
        from slayer.core.keys import AggregateKey

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
                if aliases and isinstance(slot.key, AggregateKey):
                    full_alias = aliases[0]
                    order_col = exp.Column(
                        this=exp.to_identifier(full_alias, quoted=True),
                    )
                    ascending = order_entry.direction == "asc"
                    select = select.order_by(
                        exp.Ordered(this=order_col, desc=not ascending),
                    )
                    continue
                # Hidden ROW / transform / cross-model / composite ORDER
                # targets aren't materialised in the local-only SELECT —
                # preserved as NotImplementedError. DEV-1501's
                # ``aggregates_only=True`` keeps hidden ROW targets out
                # of ``base_render_order``; hidden composite-aggregate
                # ORDER BY is rejected at the ``OrderItem`` input
                # validation layer.
                raise NotImplementedError(
                    f"DEV-1450 stage 7b.10+: ORDER BY references a "
                    f"hidden slot (id={slot.id!r}, key="
                    f"{type(slot.key).__name__}) not materialised in "
                    f"the local-only SELECT. Deferred to a later slice."
                )
            if slot.public_aliases:
                alias = slot.public_aliases[0]
            elif slot.public_name:
                alias = slot.public_name
            else:
                alias = slot.declared_name
            full_alias = f"{source_relation}.{alias}"
            order_col = exp.Column(
                this=exp.to_identifier(full_alias, quoted=True),
            )
            ascending = order_entry.direction == "asc"
            select = select.order_by(
                exp.Ordered(this=order_col, desc=not ascending),
            )

        if planned_query.limit is not None:
            select = select.limit(planned_query.limit)
        if planned_query.offset is not None:
            select = select.offset(planned_query.offset)

        return select


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
        return generate_from_planned(
            planned_queries[0], bundle=bundle, dialect=dialect,
        )

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

    return root_ast.sql(dialect=dialect, pretty=True)


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
