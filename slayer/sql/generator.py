"""SQL generator — converts EnrichedQuery to SQL via sqlglot AST.

The generator works exclusively with EnrichedQuery objects (fully resolved
SQL expressions). It never looks up model definitions — that's done by the
query engine's _enrich() step.
"""

import copy
import logging
import re
from typing import List, Optional, Union

import sqlglot
from sqlglot import exp

from slayer.core.enums import (
    BUILTIN_AGGREGATION_FORMULAS,
    BUILTIN_AGGREGATION_REQUIRED_PARAMS,
    DataType,
    TimeGranularity,
)
from slayer.engine.enriched import EnrichedMeasure, EnrichedQuery, public_projection_aliases
from slayer.sql.dialects import SqlDialect, get_dialect


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
# registered Python UDFs; Postgres/DuckDB/ClickHouse use the native
# function emitted via sqlglot transpilation. MySQL and T-SQL have no
# native CORR / COVAR_SAMP / COVAR_POP — these use the
# variance-decomposition formula in _build_covar_formula instead.
_STAT_AGG_NAMES: frozenset[str] = frozenset({
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop",
})

# Subset of _STAT_AGG_NAMES that take two columns (LHS + `other=` kwarg).
# Dialect-specific stat-agg behaviour (T-SQL name overrides, MySQL/T-SQL
# variance-decomposition formula, log10/log2 native flags) moved to the
# per-dialect classes under ``slayer/sql/dialects/`` (DEV-1542).
_TWO_ARG_STAT_AGGS: frozenset[str] = frozenset({"corr", "covar_samp", "covar_pop"})

# Transforms that use self-join CTEs instead of window functions.
# This gives correct results at result-set edges (no NULLs when the DB has the data)
# and handles gaps in time series correctly.
_SELF_JOIN_TRANSFORMS = {"time_shift"}

# DEV-1539: compound AST shapes that ALWAYS need an outer ``(...)``
# wrap when substituted into the HAVING text. Checked **before** the
# atomic list below because in sqlglot 30.4.3 ``exp.And`` / ``exp.Or``
# inherit from ``exp.Func``; without this priority, a custom-aggregation
# formula rooted at an ``And``/``Or`` would mis-classify as atomic and
# emit ambiguous SQL like ``SUM(x) > 0 AND SUM(y) > 0 IS NULL`` instead
# of ``(SUM(x) > 0 AND SUM(y) > 0) IS NULL``. Mirrors the WHERE-side
# ``_COMPOUND_FILTER_INLINE_TYPES`` in ``slayer/engine/enrichment.py``.
_HAVING_AGG_COMPOUND_TYPES: tuple = (
    exp.Binary,
    exp.Connector,
    exp.Unary,
    exp.Predicate,
)

# DEV-1539: AST shapes considered atomic for HAVING aggregate
# substitution. Used as the fallback after the compound-types check.
# Mirrors the WHERE-side ``_ATOMIC_FILTER_INLINE_TYPES`` in
# ``slayer/engine/enrichment.py``.
_HAVING_AGG_ATOMIC_TYPES: tuple = (
    exp.Column,
    exp.Literal,
    exp.Func,      # covers function calls, CAST, CASE, Anonymous, …
    exp.Paren,
    exp.Boolean,
    exp.Null,
)

# Separator used when joining pre-rendered SQL fragments into a conjunctive
# WHERE/HAVING clause; extracted as a constant so Sonar S1192 doesn't flag it
# at every join site.
_SQL_AND_JOINER = " AND "

# DEV-1444: separator used between pretty-printed SELECT projection columns
# (",\n    "). Extracted as a constant so Sonar S1192 doesn't flag every
# join site that follows the same pattern.
_SQL_COL_SEP = ",\n    "

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
# `_WINDOW_UNIT_SQLITE` moved to ``slayer/sql/dialects/sqlite.py`` (DEV-1542).


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

    def __init__(self, dialect: Union[str, SqlDialect] = "postgres"):
        if isinstance(dialect, SqlDialect):
            self._dialect: SqlDialect = dialect
        else:
            self._dialect = get_dialect(dialect)

    @property
    def dialect(self) -> str:
        """The sqlglot dialect name. Read-only — derived from
        ``self._dialect.sqlglot_name``. Mutating it would desync the
        strategy object from the string sqlglot consumes."""
        return self._dialect.sqlglot_name

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
        tree = sqlglot.parse_one(sql, dialect=d)
        tree = active.rewrite_parsed_ast(tree)
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
        active = self._dialect if d == self.dialect else get_dialect(d)
        wrapped = sqlglot.parse_one(f"SELECT 1 WHERE {sql}", dialect=d)
        where = wrapped.args.get("where")
        if where is None or where.this is None:  # pragma: no cover — defensive
            raise ValueError(
                f"Could not extract WHERE predicate from {sql!r} (dialect={d!r})"
            )
        tree = active.rewrite_parsed_ast(where.this)
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

                agg_expr, _ = self._build_agg(measure=cm.measure)
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
                    measure=unfiltered,
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

                agg_expr, _ = self._build_agg(measure=unfiltered)
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
        return self._dialect.build_time_offset_expr(col_expr, offset, granularity)

    def _duration_interval_exprs(self, duration: str, sign: int = 1) -> list[exp.Expression]:
        """Return per-unit AST nodes that `_add_intervals_expr` will chain.

        Delegates to the dialect strategy — Postgres-shape returns
        ``exp.Interval`` nodes; SQLite returns DATETIME-modifier string
        literals with sign baked in.
        """
        parts = _parse_window_duration(duration)
        return self._dialect.duration_interval_exprs(parts, sign)

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

        Delegates to the dialect strategy — defaults to chained Add/Sub
        with ``exp.Interval`` nodes; SQLite wraps as ``DATETIME(...)``;
        T-SQL chains ``DATEADD(...)`` calls.
        """
        return self._dialect.add_intervals_expr(expr, intervals, sign)

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
                measure=measure,
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
        cte_clause = "WITH " + ",\n".join(cte_strs)

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
        """Build a DATE_TRUNC expression. Dispatches to the dialect strategy.

        The dialect determines the wire form — DATE_TRUNC for
        Postgres/DuckDB/ClickHouse, STRFTIME for SQLite (with CASE WHEN
        for quarter and weekday-modifier for week), DATETRUNC for T-SQL.
        Cast-wrapping of non-column operands is handled inside each
        dialect's override.
        """
        return self._dialect.build_date_trunc(col_expr, granularity, parse=self._parse)

    @staticmethod
    def _build_transform_sql(t) -> str:  # NOSONAR S3776 — flat dispatch over transform names; per-transform SQL forms read better as one if/elif tree than as named helpers
        """Build a window function SQL expression for a transform."""
        measure = f'"{t.measure_alias}"'
        time_col = f'"{t.time_alias}"' if t.time_alias else None
        partition_cols = getattr(t, "partition_aliases", []) or []
        partition_clause = (
            "PARTITION BY " + ", ".join(f'"{a}"' for a in partition_cols)
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
        if base_val == 10 and self._dialect.should_use_native_log(10):
            return exp.Anonymous(this="log10", expressions=[arg.copy()])
        if base_val == 2 and self._dialect.should_use_native_log(2):
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

    def _resolve_value_sql(self, measure: "EnrichedMeasure") -> str:
        """Resolve ``measure.sql`` (or ``measure.name``) into a fully-qualified
        SQL string for the value column. Mirrors what ``_build_agg`` does for
        the standard sum/avg/min/max path so the dialect-aware builders
        (median/percentile/stat-aggs/formula) emit the same qualified
        identifiers.
        """
        return self._resolve_sql(
            sql=measure.sql,
            name=measure.name,
            model_name=measure.model_name,
            type=measure.column_type,
        ).sql(dialect=self.dialect)

    def _resolve_agg_param(
        self,
        measure: "EnrichedMeasure",
        *,
        name: str,
        agg_name: str,
    ) -> str:
        """Pull a named aggregation parameter, with query-time SQL-injection
        validation and model-level-default fallback. Returns the SQL string
        with bare identifiers qualified under ``measure.model_name`` (via
        ``_resolve_sql``); qualified names and numeric literals pass
        through unchanged. Raises ``ValueError`` if neither source supplies
        the parameter — reused by ``_build_percentile`` (``p=``) and
        ``_build_stat_agg`` (``other=``); mirrors ``weighted_avg``'s
        ``weight=`` flow.
        """
        raw: Optional[str] = None
        if name in measure.agg_kwargs:
            raw = measure.agg_kwargs[name]
            _validate_agg_param_value(raw, name, agg_name)
        elif measure.aggregation_def:
            for param in measure.aggregation_def.params:
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
            sql=raw, name=raw, model_name=measure.model_name,
        ).sql(dialect=self.dialect)

    def _build_agg(
        self,
        measure: EnrichedMeasure,
        rn_suffix_map: Optional[dict[str, str]] = None,
        default_time_col: Optional[str] = None,
        filtered_rn_map: Optional[dict[str, str]] = None,
        filtered_match_map: Optional[dict[str, str]] = None,
    ) -> tuple[exp.Expression, bool]:
        """Build an aggregation expression from an enriched measure."""
        agg_name = measure.aggregation
        if not agg_name:
            # Not an aggregation — raw expression
            if measure.sql:
                return self._resolve_sql(
                    sql=measure.sql,
                    name=measure.name,
                    model_name=measure.model_name,
                    type=measure.column_type,
                ), False
            return exp.Column(
                this=exp.to_identifier(measure.name),
                table=exp.to_identifier(measure.model_name),
            ), False

        # --- first/last: MAX(CASE WHEN _rn = 1 THEN col END) ---
        if agg_name in ("first", "last"):
            col_expr = self._resolve_sql(
                sql=measure.sql,
                name=measure.name,
                model_name=measure.model_name,
                type=measure.column_type,
            )
            col = col_expr.sql(dialect=self.dialect)
            suffix = ""
            if rn_suffix_map and default_time_col:
                effective_tc = measure.time_column or default_time_col
                suffix = rn_suffix_map.get(effective_tc, "")
            rn_col = f"_first_rn{suffix}" if agg_name == "first" else f"_last_rn{suffix}"
            # For filtered first/last, use the dedicated ROW_NUMBER column
            # that pushes non-matching rows to the bottom of the ranking.
            # Look up by alias (unique per enriched measure) so two filtered
            # measures sharing source/agg but with different filters map to
            # their own respective rank columns. Use the per-measure match
            # flag (also projected by the ranked subquery) instead of
            # re-emitting measure.filter_sql here — the filter can reference
            # joined-table columns that are not in scope outside the subquery.
            if measure.filter_sql and filtered_rn_map:
                filtered_rn = filtered_rn_map.get(measure.alias, rn_col)
                match_col = (
                    filtered_match_map.get(measure.alias)
                    if filtered_match_map
                    else None
                )
                # Fall back to the raw filter expression only if no match flag
                # was projected (legacy callers); accepts the leak risk.
                filter_clause = f"{match_col} = 1" if match_col else measure.filter_sql
                case_sql = (
                    f"MAX(CASE WHEN {filtered_rn} = 1 AND {filter_clause} "
                    f"THEN {col} END)"
                )
            else:
                # ``col`` is already a fully-qualified SQL expression resolved
                # via ``_resolve_sql`` earlier in this branch, so we don't need
                # to re-prefix ``measure.model_name``. (DEV-1333.)
                case_sql = f"MAX(CASE WHEN {rn_col} = 1 THEN {col} END)"
            return self._parse(case_sql), True

        # --- Custom or parameterized aggregation (formula-based) ---
        if agg_name not in _AGG_FUNCTION_MAP:
            # percentile is dialect-dependent (no static formula works on
            # SQLite/ClickHouse/MySQL) so it gets its own builder rather than
            # going through the BUILTIN_AGGREGATION_FORMULAS path.
            if agg_name == "percentile":
                return self._build_percentile(measure), True
            # Statistical aggregates also dispatch to a dedicated builder so
            # the SQLite-UDF / native-function / NotImplementedError split
            # mirrors _build_median.
            if agg_name in _STAT_AGG_NAMES:
                return self._build_stat_agg(measure), True
            return self._build_formula_agg(measure, agg_name), True

        # --- Resolve inner expression ---
        if agg_name == "count" and measure.sql is None:
            # COUNT(*) — if filtered, use COUNT(CASE WHEN filter THEN 1 END)
            if measure.filter_sql:
                case_sql = f"CASE WHEN {measure.filter_sql} THEN 1 END"
                inner = self._parse(case_sql)
            else:
                inner = exp.Star()
        elif measure.sql:
            inner = self._resolve_sql(
                sql=measure.sql,
                name=measure.name,
                model_name=measure.model_name,
                type=measure.column_type,
            )
        else:
            inner = exp.Column(
                this=exp.to_identifier(measure.name),
                table=exp.to_identifier(measure.model_name),
            )

        # --- Apply measure-level filter as CASE WHEN wrapper ---
        if measure.filter_sql and not (agg_name == "count" and measure.sql is None):
            inner_sql = inner.sql(dialect=self.dialect)
            case_sql = f"CASE WHEN {measure.filter_sql} THEN {inner_sql} END"
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

    def _build_formula_agg(self, measure: EnrichedMeasure, agg_name: str) -> exp.Expression:
        """Build SQL for formula-based aggregations (weighted_avg, custom)."""
        # Get formula: from aggregation_def or built-in
        formula = None
        if measure.aggregation_def and measure.aggregation_def.formula:
            formula = measure.aggregation_def.formula
        elif agg_name in BUILTIN_AGGREGATION_FORMULAS:
            formula = BUILTIN_AGGREGATION_FORMULAS[agg_name]

        if formula is None:
            raise ValueError(
                f"Aggregation '{agg_name}' has no formula. "
                f"Custom aggregations must define a formula."
            )

        # Collect param values: query-time overrides > aggregation_def defaults
        param_defaults = {}
        if measure.aggregation_def:
            param_defaults = {p.name: p.sql for p in measure.aggregation_def.params}
        params = {**param_defaults, **measure.agg_kwargs}

        # Validate query-time parameter values to prevent SQL injection
        for pname, pval in measure.agg_kwargs.items():
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
        # are qualified under measure.model_name (matching the standard
        # sum/avg/min/max path). When the measure carries a row-level filter,
        # wrap row-level references (the value AND any column-ref params) in
        # CASE WHEN so non-matching rows contribute NULL to all terms — but
        # leave literal-default params unwrapped, since `(CASE WHEN ... THEN
        # 100 END)` for a constant `scale=100` would turn it into a row
        # expression and break grouped SQL semantics.
        col_expr = _wrap_filter(self._resolve_value_sql(measure), measure.filter_sql)
        substituted = formula.replace("{value}", col_expr)
        for param_name, param_val in params.items():
            param_ast = self._resolve_sql(
                sql=param_val, name=param_val, model_name=measure.model_name,
            )
            param_expr = param_ast.sql(dialect=self.dialect)
            if measure.filter_sql and not isinstance(param_ast, exp.Literal):
                param_expr = _wrap_filter(param_expr, measure.filter_sql)
            substituted = substituted.replace(f"{{{param_name}}}", param_expr)

        return self._parse(substituted)

    def _build_median(self, inner: exp.Expression) -> exp.Expression:
        """Build a median aggregation expression. Dispatches to the dialect."""
        return self._dialect.build_median(inner, parse=self._parse)

    def _build_percentile(self, measure: "EnrichedMeasure") -> exp.Expression:
        """Build a PERCENTILE_CONT(p) aggregation expression (dialect-dependent).

        ``p`` comes from ``measure.agg_kwargs['p']`` (validated against
        SQL injection) or from a model-level ``Aggregation`` default.
        Filter handling mirrors ``_build_formula_agg``: when the measure
        carries a row-level filter, the value column is wrapped in
        ``CASE WHEN ... END`` so non-matching rows contribute NULL and
        are ignored by the aggregate. Both the value column and ``p``
        flow through ``_resolve_sql`` so bare identifiers are qualified
        under ``measure.model_name`` and numeric literals pass through
        unchanged.
        """
        p = self._resolve_agg_param(measure, name="p", agg_name="percentile")
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

        # Pass the **original string** ``p`` (not ``p_float``) to the dialect
        # so user literals like ``0.50`` / ``1`` / ``5e-2`` survive verbatim
        # in the emitted SQL. Range validation above keeps the safety guard.
        col_expr = _wrap_filter(self._resolve_value_sql(measure), measure.filter_sql)
        return self._dialect.build_percentile(p, col_expr, parse=self._parse)

    # ``_build_covar_formula`` lived here in DEV-1317 — moved to
    # ``slayer/sql/dialects/base._build_covar_decomposition`` in DEV-1542 and
    # is now called by MysqlDialect / TsqlDialect overrides of
    # ``build_covar_2arg``.

    def _build_stat_agg(self, measure: "EnrichedMeasure") -> exp.Expression:
        """Build SQL for the statistical aggregations added in DEV-1317.

        Handles ``stddev_samp``, ``stddev_pop``, ``var_samp``, ``var_pop``
        (1-arg) and ``corr`` / ``covar_samp`` / ``covar_pop`` (2-arg via
        ``other=`` kwarg). All seven are native on Postgres / DuckDB /
        ClickHouse; ``stddev*`` / ``var*`` are also native on MySQL but
        ``corr`` / ``covar_*`` are not. SQLite gets them via Python UDFs
        registered in ``slayer.sql.sqlite_udfs`` — the UDFs alias
        sqlglot's transpiled names (e.g. ``var_samp`` → ``VARIANCE`` on
        SQLite) so generator output resolves at runtime. MySQL and T-SQL
        implement ``corr`` / ``covar_*`` via the variance-decomposition
        formula in ``_build_covar_formula``.

        Both legs flow through ``_resolve_sql`` so bare identifiers are
        qualified under ``measure.model_name`` (matches the standard
        sum/avg/min/max path). Filter handling mirrors
        ``_build_percentile`` / ``_build_formula_agg``: a row-level
        filter wraps the value AND the ``other`` column in
        ``CASE WHEN filter THEN col END`` so non-matching rows
        contribute NULL — which the aggregates skip.
        """
        agg_name = measure.aggregation

        # Resolve the `other=` kwarg before the dialect guard so that a
        # missing-required-param error takes priority over any dialect-specific
        # error when both conditions hold — the missing-param message points at
        # the actual user mistake. Closes Codex #5 on PR #82.
        other_expr: Optional[str] = None
        if agg_name in _TWO_ARG_STAT_AGGS:
            other_expr = _wrap_filter(
                self._resolve_agg_param(measure, name="other", agg_name=agg_name),
                measure.filter_sql,
            )

        col_expr = _wrap_filter(self._resolve_value_sql(measure), measure.filter_sql)

        if agg_name in _TWO_ARG_STAT_AGGS:
            assert other_expr is not None  # set above when two-arg
            return self._dialect.build_covar_2arg(
                agg_name, col_expr, other_expr, parse=self._parse,
            )
        return self._dialect.build_stat_agg_1arg(
            agg_name, col_expr, parse=self._parse,
        )

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
                                measure=m,
                                rn_suffix_map=rn_suffix_map,
                                default_time_col=enriched.last_agg_time_column,
                                filtered_rn_map=filtered_rn_map,
                            )
                            agg_sql = agg_expr.sql(dialect=self.dialect)
                            # DEV-1539: wrap the substituted aggregate
                            # in outer parens when it's a compound
                            # shape (arithmetic, AND/OR connector,
                            # NOT, BETWEEN, IN, LIKE, IS, …) so any
                            # surrounding comparator's precedence is
                            # explicit. The compound check fires
                            # first because in sqlglot 30.4.3
                            # ``exp.And`` / ``exp.Or`` inherit from
                            # ``exp.Func``; a pure inverse-atomic check
                            # would mis-classify a connector-rooted
                            # aggregate as atomic. Single function-call
                            # aggregates (SUM/COUNT/AVG/…) match
                            # ``exp.Func`` and stay unwrapped.
                            if (
                                isinstance(agg_expr, _HAVING_AGG_COMPOUND_TYPES)
                                or not isinstance(agg_expr, _HAVING_AGG_ATOMIC_TYPES)
                            ):
                                agg_sql = f"({agg_sql})"
                            # DEV-1539: lambda replacement (backslash
                            # safety) + trailing ``(?!\.)`` guard so a
                            # measure name doesn't mis-substitute inside
                            # a dotted continuation in ``having_sql``.
                            having_sql = re.sub(
                                rf'(?<!\.)(?<!\w)\b{re.escape(col_name)}\b(?!\.)',
                                lambda _m, s=agg_sql: s,
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
