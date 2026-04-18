"""SQL generator — converts EnrichedQuery to SQL via sqlglot AST.

The generator works exclusively with EnrichedQuery objects (fully resolved
SQL expressions). It never looks up model definitions — that's done by the
query engine's _enrich() step.
"""

import copy
import logging
import re
from typing import Optional

import sqlglot
from sqlglot import exp

from slayer.core.enums import (
    BUILTIN_AGGREGATION_FORMULAS,
    BUILTIN_AGGREGATION_REQUIRED_PARAMS,
    TimeGranularity,
)
from slayer.engine.enriched import EnrichedMeasure, EnrichedQuery

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
}

# Transforms that use self-join CTEs instead of window functions.
# This gives correct results at result-set edges (no NULLs when the DB has the data)
# and handles gaps in time series correctly.
_SELF_JOIN_TRANSFORMS = {"time_shift", "change", "change_pct"}

# Matches safe aggregation parameter values: identifiers, qualified names, numeric literals.
_SAFE_AGG_PARAM_RE = re.compile(
    r'^(?:'
    r'[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*'  # identifier or qualified name
    r'|'
    r'-?\d+(?:\.\d+)?'  # numeric literal
    r')$'
)


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


class SQLGenerator:
    """Generates SQL from an EnrichedQuery."""

    def __init__(self, dialect: str = "postgres"):
        self.dialect = dialect

    def generate(self, enriched: EnrichedQuery) -> str:
        """Generate SQL from a fully resolved EnrichedQuery.

        Architecture:
        1. Base CTE: simple (non-isolated) measures + dimensions
        2. Per-measure CTEs: cross-model measures + cross-model-filtered measures
        3. Combined: LEFT JOIN base + measure CTEs on shared dimensions
        4. Expressions/transforms stacked on top of combined
        """
        has_isolated = any(_has_cross_model_filter(m) for m in enriched.measures)
        has_cross_model = bool(enriched.cross_model_measures)
        has_measure_ctes = has_isolated or has_cross_model
        has_computed = bool(enriched.expressions or enriched.transforms)

        base_sql = self._generate_base(enriched=enriched, skip_isolated=has_measure_ctes)

        if not has_measure_ctes and not has_computed:
            return base_sql

        if has_measure_ctes:
            # Get structured CTE definitions (no WITH wrapper)
            measure_ctes = self._build_combined(enriched=enriched, base_sql=base_sql)
            if has_computed:
                # Pass CTE list to computed layer — it merges into a flat WITH
                return self._generate_with_computed(enriched=enriched, prefix_ctes=measure_ctes)
            # No expressions: assemble CTEs + outer SELECT + pagination
            return self._assemble_combined_sql(enriched=enriched, measure_ctes=measure_ctes)

        # No measure CTEs, just computed columns
        return self._generate_with_computed(enriched=enriched, base_sql=base_sql)

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
        measure_cte_refs = []  # (cte_name, measure_alias)

        # --- Cross-model measure CTEs ---
        seen_cm_ctes: set = set()
        for cm in enriched.cross_model_measures:
            cte_name = _cte_name_from_alias("_cm_", cm.alias)
            if cte_name in seen_cm_ctes:
                measure_cte_refs.append((cte_name, cm.alias))
                continue
            seen_cm_ctes.add(cte_name)

            select_parts = []
            group_parts = []

            for dim in cm.shared_dimensions:
                col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=cm.source_model_name)
                col_sql = col_expr.sql(dialect=self.dialect)
                select_parts.append(f'{col_sql} AS "{dim.alias}"')
                group_parts.append(col_sql)
            for td in cm.shared_time_dimensions:
                col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=cm.source_model_name)
                td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
                td_sql = td_expr.sql(dialect=self.dialect)
                select_parts.append(f'{td_sql} AS "{td.alias}"')
                group_parts.append(td_sql)

            agg_expr, _ = self._build_agg(measure=cm.measure)
            select_parts.append(f'{agg_expr.sql(dialect=self.dialect)} AS "{cm.alias}"')

            if cm.source_sql:
                from_sql = f"({cm.source_sql}) AS {cm.source_model_name}"
            else:
                from_sql = f"{cm.source_sql_table} AS {cm.source_model_name}"
            if cm.target_model_sql:
                target_from = f"({cm.target_model_sql}) AS {cm.target_model_name}"
            else:
                target_from = f"{cm.target_model_sql_table} AS {cm.target_model_name}"
            join_conditions = [
                f"{cm.source_model_name}.{src} = {cm.target_model_name}.{tgt}"
                for src, tgt in cm.join_pairs
            ]

            cte_sql = (
                f"SELECT {', '.join(select_parts)}\n"
                f"FROM {from_sql}\n"
                f"{cm.join_type.upper()} JOIN {target_from} ON {' AND '.join(join_conditions)}"
            )
            # Only include WHERE conditions whose tables are in this CTE
            cm_available = {cm.source_model_name, cm.target_model_name}
            original_filters = enriched.filters
            enriched.filters = [f for f in original_filters
                                if _filter_references_available(f, cm_available)]
            where_clause, _ = self._build_where_and_having(enriched=enriched)
            enriched.filters = original_filters
            if where_clause is not None:
                cte_sql += f"\nWHERE {where_clause.sql(dialect=self.dialect)}"
            if group_parts:
                cte_sql += f"\nGROUP BY {', '.join(group_parts)}"

            ctes.append((cte_name, cte_sql))
            measure_cte_refs.append((cte_name, cm.alias))

        # --- Isolated filtered-measure CTEs ---
        for measure in enriched.measures:
            if not _has_cross_model_filter(measure):
                continue
            cte_name = _cte_name_from_alias("_fm_", measure.alias)

            select_parts = []
            group_parts = []
            for dim in enriched.dimensions:
                col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name)
                col_sql = col_expr.sql(dialect=self.dialect)
                select_parts.append(f'{col_sql} AS "{dim.alias}"')
                group_parts.append(col_sql)
            for td in enriched.time_dimensions:
                col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
                td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
                td_sql = td_expr.sql(dialect=self.dialect)
                select_parts.append(f'{td_sql} AS "{td.alias}"')
                group_parts.append(td_sql)

            # Measure aggregation without CASE WHEN (the join IS the filter)
            unfiltered = copy.copy(measure)
            unfiltered.filter_sql = None
            unfiltered.filter_columns = []
            agg_expr, _ = self._build_agg(measure=unfiltered)
            select_parts.append(f'{agg_expr.sql(dialect=self.dialect)} AS "{measure.alias}"')

            from_clause = self._build_from_clause(enriched=enriched)
            from_sql = from_clause.sql(dialect=self.dialect)

            # Only include dimension joins + this measure's filter joins
            needed = _needed_join_aliases(enriched, extra_columns=measure.filter_columns)
            join_parts = ""
            for target_table, target_alias, join_cond, jtype in enriched.resolved_joins:
                if target_alias in needed:
                    if target_table.startswith("("):
                        target_ref = f"({target_table}) AS {target_alias}"
                    else:
                        target_ref = f"{target_table} AS {target_alias}"
                    join_parts += f"\n{jtype.upper()} JOIN {target_ref} ON {join_cond}"

            cte_sql = f"SELECT {', '.join(select_parts)}\nFROM {from_sql}{join_parts}"
            # Only include WHERE conditions whose tables are in this CTE
            fm_available = needed | {enriched.model_name}
            original_filters = enriched.filters
            enriched.filters = [f for f in original_filters
                                if _filter_references_available(f, fm_available)]
            where_clause, _ = self._build_where_and_having(enriched=enriched)
            enriched.filters = original_filters
            if where_clause is not None:
                cte_sql += f"\nWHERE {where_clause.sql(dialect=self.dialect)}"
            if group_parts:
                cte_sql += f"\nGROUP BY {', '.join(group_parts)}"

            ctes.append((cte_name, cte_sql))
            measure_cte_refs.append((cte_name, measure.alias))

        # --- Build combined SELECT: _base LEFT JOIN measure CTEs ---
        base_cols = list(dim_aliases) + list(td_aliases)
        for m in enriched.measures:
            if not _has_cross_model_filter(m):
                base_cols.append(m.alias)
        final_parts = [f'_base."{a}"' for a in base_cols]
        for cte_name, alias in measure_cte_refs:
            final_parts.append(f'{cte_name}."{alias}"')

        from_clause_str = "FROM _base"
        joined_ctes: set = set()
        for cte_name, _ in measure_cte_refs:
            if cte_name in joined_ctes:
                continue
            joined_ctes.add(cte_name)

            join_on_parts = []
            for a in join_aliases:
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

        # ORDER BY qualified with _base. to avoid ambiguity in multi-CTE joins
        if enriched.order:
            order_parts = []
            for order_item in enriched.order:
                col = order_item.column
                col_name = self._resolve_order_column(col=col, enriched=enriched)
                direction = "ASC" if order_item.direction == "asc" else "DESC"
                order_parts.append(f'_base."{col_name}" {direction}')
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
                col_name = f"{col.model or enriched.model_name}.{col.name}"
                direction = "ASC" if order_item.direction == "asc" else "DESC"
                order_parts.append(f'"{col_name}" {direction}')
            sql += "\nORDER BY " + ", ".join(order_parts)
        if enriched.limit is not None:
            sql += f"\nLIMIT {enriched.limit}"
        if enriched.offset is not None:
            sql += f"\nOFFSET {enriched.offset}"
        return sql

    def _generate_shifted_base(self, enriched: EnrichedQuery, transform,
                               calendar_join: bool = False) -> str:
        """Generate a base query with date ranges shifted for a self-join transform.

        Instead of copying the base CTE (which has the original date filter and
        would miss data outside that range), this generates a fresh query against
        the source table with adjusted date ranges so the shifted CTE contains
        the data needed for the join.

        When calendar_join is True, the raw timestamps are also shifted by -offset
        inside the DATE_TRUNC so that the aggregated time buckets align with the
        base query's buckets. This allows a simple equality join (no date arithmetic
        in the ON clause).
        """
        # Determine the shift: use transform's granularity, or fall back to
        # the query's time dimension granularity for row-based transforms
        gran = transform.granularity
        offset = transform.offset
        if not gran:
            # Row-based: use the time dimension's granularity
            for td in enriched.time_dimensions:
                if td.alias == transform.time_alias:
                    gran = td.granularity.value
                    break
            if not gran:
                gran = "month"  # Shouldn't happen — transforms require a time dim

        # Create a copy of enriched with shifted date ranges and (optionally)
        # shifted time dimension expressions
        shifted = copy.deepcopy(enriched)

        # Shift date ranges if present
        has_date_ranges = any(
            td.date_range and len(td.date_range) == 2
            for td in enriched.time_dimensions
        )
        if has_date_ranges:
            for td in shifted.time_dimensions:
                if td.date_range and len(td.date_range) == 2:
                    td.date_range = [
                        self._shift_date(date=td.date_range[0], offset=offset, granularity=gran),
                        self._shift_date(date=td.date_range[1], offset=offset, granularity=gran),
                    ]

        # For calendar joins, pass the time offset so _generate_base shifts raw
        # timestamps before DATE_TRUNC. This makes aggregated buckets align with
        # the base query's buckets → simple equality join.
        time_offset = None
        if calendar_join:
            time_offset = (-offset, gran)

        return self._generate_base(enriched=shifted, time_offset=time_offset)

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
            col_sql = col_expr.sql(dialect="sqlite")
            return sqlglot.parse_one(
                f"DATE({col_sql}, '{sqlite_val} {sqlite_unit}')", dialect="sqlite"
            )

        # Standard SQL: col + INTERVAL 'N' UNIT
        interval_str = f"INTERVAL '{val}' {unit}"
        col_sql = col_expr.sql(dialect=self.dialect)
        return sqlglot.parse_one(f"{col_sql} + {interval_str}", dialect=self.dialect)

    @staticmethod
    def _shift_date(date: str, offset: int, granularity: str) -> str:
        """Shift a date string by offset units of granularity."""
        from datetime import datetime, timedelta
        from dateutil.relativedelta import relativedelta

        dt = datetime.strptime(date[:10], "%Y-%m-%d")
        gran_map = {
            "year": relativedelta(years=offset),
            "quarter": relativedelta(months=offset * 3),
            "month": relativedelta(months=offset),
            "week": timedelta(weeks=offset),
            "day": timedelta(days=offset),
            "hour": timedelta(hours=offset),
            "minute": timedelta(minutes=offset),
            "second": timedelta(seconds=offset),
        }
        delta = gran_map.get(granularity, relativedelta(months=offset))
        shifted = dt + delta
        return shifted.strftime("%Y-%m-%d")

    def _generate_base(self, enriched: EnrichedQuery,
                        time_offset: Optional[tuple[int, str]] = None,
                        skip_isolated: bool = False) -> str:
        """Generate the base SELECT (measures, dimensions, filters).

        Args:
            time_offset: Optional (offset, granularity) to shift raw timestamps
                before DATE_TRUNC. Used by shifted CTEs so aggregated buckets
                align with the base query for simple equality joins.
        """
        from_clause = self._build_from_clause(enriched=enriched)

        # If any measure has first/last aggregation, prepend a ROW_NUMBER CTE
        # to mark the latest (or earliest) row per group.
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
                enriched=enriched, base_from=from_clause, time_offset=time_offset,
            )

        select_columns = []
        group_by_columns = []

        for dim in enriched.dimensions:
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name)
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
                # Apply time offset before DATE_TRUNC (for shifted CTEs)
                if time_offset is not None:
                    offset_val, offset_gran = time_offset
                    col_expr = self._build_time_offset_expr(
                        col_expr=col_expr, offset=offset_val, granularity=offset_gran,
                    )
                col_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
            select_columns.append(col_expr.as_(td.alias))
            group_by_columns.append(col_expr)

        has_aggregation = False
        for measure in enriched.measures:
            if skip_isolated and _has_cross_model_filter(measure):
                continue  # Will be handled in its own CTE
            agg_expr, is_agg = self._build_agg(
                measure=measure,
                rn_suffix_map=rn_suffix_map,
                default_time_col=enriched.last_agg_time_column,
                filtered_rn_map=filtered_rn_map,
                filtered_match_map=filtered_match_map,
            )
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
        # or isolated measures were skipped (to deduplicate the dimension spine)
        needs_group_by = has_aggregation or bool(enriched.cross_model_measures) or skip_isolated
        if needs_group_by and group_by_columns:
            for gb in group_by_columns:
                select = select.group_by(gb)

        if having_clause is not None:
            select = select.having(having_clause)

        # When no computed columns and no measure CTEs, apply order/limit/offset
        # to the base query. Otherwise, they'll be applied to the outer query.
        if not enriched.expressions and not enriched.transforms and not skip_isolated:
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
                    parsed_target = sqlglot.parse_one(target_table, dialect=self.dialect)
                    join_target = exp.Subquery(
                        this=parsed_target, alias=exp.to_identifier(target_alias),
                    )
                else:
                    join_target = exp.to_table(target_table, alias=target_alias)
                join_on = sqlglot.parse_one(join_cond, dialect=self.dialect)
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
                    layer_parts.append(f'{expr.sql} AS "{expr.alias}"')
                    added_this_layer.append(expr.alias)
                else:
                    remaining_expressions.append(expr)

            # Batch window-function transforms into this layer
            deferred_self_joins = []
            for t in pending_transforms:
                if t.measure_alias not in available_aliases:
                    remaining_transforms.append(t)
                elif t.transform in _SELF_JOIN_TRANSFORMS:
                    deferred_self_joins.append(t)  # Handle after window layer
                else:
                    window_sql = self._build_transform_sql(t)
                    layer_parts.append(f'{window_sql} AS "{t.alias}"')
                    added_this_layer.append(t.alias)

            # Emit window layer CTE if anything was added
            if added_this_layer:
                layer_name = f"step{layer_num}"
                layer_select = "SELECT\n    " + ",\n    ".join(layer_parts)
                ctes.append((layer_name, f"{layer_select}\nFROM {prev_cte}"))
                available_aliases.update(added_this_layer)

            # Now emit each self-join transform as its own CTE layer
            for t in deferred_self_joins:
                src_cte = ctes[-1][0]

                # Determine effective join granularity:
                # - If transform has explicit granularity (calendar-based), use it
                # - If no granularity (row-based) but date ranges are shifted,
                #   use the time dimension's granularity for calendar join
                # - If no granularity and no date ranges, use row-number join
                has_date_ranges = any(
                    td.date_range and len(td.date_range) == 2
                    for td in enriched.time_dimensions
                )
                join_granularity = t.granularity
                if not join_granularity and has_date_ranges:
                    # Use query's time dimension granularity for calendar-based join
                    for td in enriched.time_dimensions:
                        if td.alias == t.time_alias:
                            join_granularity = td.granularity.value
                            break

                # Add ROW_NUMBER if using row-number join
                if not join_granularity:
                    time_col = f'"{t.time_alias}"'
                    all_cols = ", ".join(f'"{a}"' for a in sorted(available_aliases))
                    rn_cte = f"{src_cte}_rn"
                    rn_sql = f"SELECT {all_cols}, ROW_NUMBER() OVER (ORDER BY {time_col}) AS _rn FROM {src_cte}"
                    ctes.append((rn_cte, rn_sql))
                    src_cte = rn_cte

                # Generate shifted CTE as a fresh base query with adjusted date ranges.
                # For calendar joins, also shift raw timestamps so buckets align.
                is_calendar = join_granularity is not None
                shift_base_name = f"shifted_base_{t.name}"
                shift_name = f"shifted_{t.name}"
                shifted_sql = self._generate_shifted_base(
                    enriched=enriched, transform=t, calendar_join=is_calendar,
                )
                ctes.append((shift_base_name, shifted_sql))

                # For row-number joins, add ROW_NUMBER to the shifted CTE too
                if not is_calendar:
                    time_col = f'"{t.time_alias}"'
                    shift_base_cols = ", ".join(f'"{a}"' for a in sorted(base_aliases))
                    shift_rn_sql = f"SELECT {shift_base_cols}, ROW_NUMBER() OVER (ORDER BY {time_col}) AS _rn FROM {shift_base_name}"
                    ctes.append((shift_name, shift_rn_sql))
                else:
                    ctes.append((shift_name, f"SELECT * FROM {shift_base_name}"))

                # Build the self-join CTE: src LEFT JOIN shifted ON condition
                time_col = f'"{t.time_alias}"'
                if is_calendar:
                    # Calendar join: simple equality (shifted timestamps are already aligned)
                    join_cond = f'{src_cte}.{time_col} = {shift_name}.{time_col}'
                else:
                    # Row-number join
                    join_cond = self._build_row_number_join(
                        left_table=src_cte, right_table=shift_name, offset=t.offset,
                    )
                col_sql = self._build_self_join_column(
                    transform=t.transform, left_table=src_cte,
                    right_table=shift_name, measure_alias=t.measure_alias,
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

            if not added_this_layer:
                remaining_transforms.extend(deferred_self_joins)
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
            window_sql = self._build_transform_sql(t)
            final_parts.append(f'{window_sql} AS "{t.alias}"')

        outer_select = "SELECT\n    " + ",\n    ".join(final_parts)

        sql = f"{cte_clause}\n{outer_select}\nFROM {final_cte}"

        # Apply order/limit/offset
        sql = self._apply_pagination_to_sql(enriched=enriched, sql=sql)

        # Apply post-filters (filters referencing computed columns)
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
            where_clause = " AND ".join(conditions)
            sql = f"SELECT *\nFROM (\n{sql}\n) AS _filtered\nWHERE {where_clause}"

        return sql

    @staticmethod
    def _deps_available(sql: str, available: set[str]) -> bool:
        """Check if all quoted aliases referenced in SQL are in the available set."""
        import re
        refs = re.findall(r'"([^"]+)"', sql)
        return all(ref in available for ref in refs)

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
                return sqlglot.parse_one(
                    f"DATE({col_expr.sql(dialect='sqlite')}, 'weekday 0', '-6 days')",
                    dialect="sqlite",
                )
            if gran_str == "quarter":
                # Quarter start: derive from month
                col_sql = col_expr.sql(dialect="sqlite")
                return sqlglot.parse_one(
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
    def _build_transform_sql(t) -> str:
        """Build a window function SQL expression for a transform."""
        measure = f'"{t.measure_alias}"'
        time_col = f'"{t.time_alias}"' if t.time_alias else None
        order_clause = f"ORDER BY {time_col}" if time_col else ""

        if t.transform == "cumsum":
            return f"SUM({measure}) OVER ({order_clause})"
        elif t.transform in _SELF_JOIN_TRANSFORMS:
            raise ValueError(f"{t.transform} should not reach _build_transform_sql; it uses self-join CTE")
        elif t.transform == "lag":
            return f"LAG({measure}, {abs(t.offset)}) OVER ({order_clause})"
        elif t.transform == "lead":
            return f"LEAD({measure}, {abs(t.offset)}) OVER ({order_clause})"
        elif t.transform == "rank":
            return f"RANK() OVER (ORDER BY {measure} DESC)"
        elif t.transform == "last":
            return (
                f"FIRST_VALUE({measure}) OVER ({order_clause} DESC "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            )
        else:
            raise ValueError(f"Unsupported transform: {t.transform}")

    @staticmethod
    def _build_self_join_column(transform: str, left_table: str,
                                right_table: str, measure_alias: str) -> str:
        """Build the SELECT expression for a self-join transform."""
        cur = f'{left_table}."{measure_alias}"'
        prev = f'{right_table}."{measure_alias}"'
        if transform == "time_shift":
            return prev
        elif transform == "change":
            return f"{cur} - {prev}"
        elif transform == "change_pct":
            return f"CASE WHEN {prev} != 0 THEN ({cur} - {prev}) * 1.0 / {prev} END"
        raise ValueError(f"Unknown self-join transform: {transform}")

    @staticmethod
    def _build_row_number_join(left_table: str, right_table: str, offset: int) -> str:
        """Build a row-number-based JOIN condition for row-based self-join transforms."""
        return f"{left_table}._rn + {offset} = {right_table}._rn"

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
            parsed = sqlglot.parse_one(sql=enriched.sql, dialect=self.dialect)
            return exp.Subquery(this=parsed, alias=exp.to_identifier(enriched.model_name))
        else:
            raise ValueError(f"Model '{enriched.model_name}' has neither sql_table nor sql defined")

    def _build_last_ranked_from(
        self,
        enriched: EnrichedQuery,
        base_from: exp.Expression,
        time_offset: Optional[tuple[int, str]] = None,
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
            if time_offset is not None:
                offset_val, offset_gran = time_offset
                col_expr = self._build_time_offset_expr(
                    col_expr=col_expr, offset=offset_val, granularity=offset_gran,
                )
            td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
            parts.append(f"{td_expr.sql(dialect=self.dialect)} AS _td_{td.name}")

        # Build PARTITION BY from query dimensions + time dimensions
        # Must use full expressions (not aliases) since aliases aren't visible in OVER()
        partition_parts = []
        for dim in enriched.dimensions:
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name)
            partition_parts.append(col_expr.sql(dialect=self.dialect))
        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
            if time_offset is not None:
                offset_val, offset_gran = time_offset
                col_expr = self._build_time_offset_expr(
                    col_expr=col_expr, offset=offset_val, granularity=offset_gran,
                )
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

        parsed = sqlglot.parse_one(ranked_sql, dialect=self.dialect)
        return (
            exp.Subquery(this=parsed, alias=exp.to_identifier(model)),
            rn_suffix_map,
            filtered_rn_map,
            filtered_match_map,
        )

    # ------------------------------------------------------------------
    # Column / measure resolution (from enriched SQL expressions)
    # ------------------------------------------------------------------

    def _resolve_sql(self, sql: Optional[str], name: str, model_name: str) -> exp.Expression:
        """Resolve an enriched SQL expression to a sqlglot AST node."""
        if sql is None:
            return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(model_name))
        # Bare column name → qualify with model name
        # Use isidentifier() to distinguish column names from literals (e.g. "1")
        if sql.isidentifier():
            return exp.Column(this=exp.to_identifier(sql), table=exp.to_identifier(model_name))
        return sqlglot.parse_one(sql=sql, dialect=self.dialect)

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
                return self._resolve_sql(sql=measure.sql, name=measure.name, model_name=measure.model_name), False
            return exp.Column(
                this=exp.to_identifier(measure.name),
                table=exp.to_identifier(measure.model_name),
            ), False

        # --- first/last: MAX(CASE WHEN _rn = 1 THEN col END) ---
        if agg_name in ("first", "last"):
            col = measure.sql or measure.name
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
                    f"THEN {measure.model_name}.{col} END)"
                )
            else:
                case_sql = f"MAX(CASE WHEN {rn_col} = 1 THEN {measure.model_name}.{col} END)"
            return sqlglot.parse_one(case_sql, dialect=self.dialect), True

        # --- Custom or parameterized aggregation (formula-based) ---
        if agg_name not in _AGG_FUNCTION_MAP:
            return self._build_formula_agg(measure, agg_name), True

        # --- Resolve inner expression ---
        if agg_name == "count" and measure.sql is None:
            # COUNT(*) — if filtered, use COUNT(CASE WHEN filter THEN 1 END)
            if measure.filter_sql:
                case_sql = f"CASE WHEN {measure.filter_sql} THEN 1 END"
                inner = sqlglot.parse_one(case_sql, dialect=self.dialect)
            else:
                inner = exp.Star()
        elif measure.sql:
            inner = self._resolve_sql(sql=measure.sql, name=measure.name, model_name=measure.model_name)
        else:
            inner = exp.Column(
                this=exp.to_identifier(measure.name),
                table=exp.to_identifier(measure.model_name),
            )

        # --- Apply measure-level filter as CASE WHEN wrapper ---
        if measure.filter_sql and not (agg_name == "count" and measure.sql is None):
            inner_sql = inner.sql(dialect=self.dialect)
            case_sql = f"CASE WHEN {measure.filter_sql} THEN {inner_sql} END"
            inner = sqlglot.parse_one(case_sql, dialect=self.dialect)

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

        # Resolve {value} and {param_name} in formula. When the measure is
        # filtered we must wrap *every* row-level reference (the value AND
        # every parameter) in a CASE WHEN so non-matching rows contribute
        # NULL to all terms. Otherwise formulas like weighted_avg
        # ("SUM({value}*{weight}) / SUM({weight})") filter the numerator
        # only and leave the denominator summing all weights.
        col_expr = measure.sql or measure.name
        if measure.filter_sql:
            col_expr = f"(CASE WHEN {measure.filter_sql} THEN {col_expr} END)"
            params = {
                name: f"(CASE WHEN {measure.filter_sql} THEN {val} END)"
                for name, val in params.items()
            }
        substituted = formula.replace("{value}", col_expr)
        for param_name, param_val in params.items():
            substituted = substituted.replace(f"{{{param_name}}}", param_val)

        return sqlglot.parse_one(substituted, dialect=self.dialect)

    def _build_median(self, inner: exp.Expression) -> exp.Expression:
        """Build a median aggregation expression (dialect-dependent)."""
        if self.dialect in ("clickhouse",):
            return sqlglot.parse_one(f"median({inner.sql(dialect=self.dialect)})", dialect=self.dialect)
        # Postgres, DuckDB, and most others: PERCENTILE_CONT
        inner_sql = inner.sql(dialect=self.dialect)
        return sqlglot.parse_one(
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {inner_sql})",
            dialect=self.dialect,
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

        # Time dimension date ranges
        for td in enriched.time_dimensions:
            if td.date_range and len(td.date_range) == 2:
                col = f"{enriched.model_name}.{td.name}" if td.sql and "." not in (td.sql or "") else f"{enriched.model_name}.{td.name}"
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
            where_sql = " AND ".join(where_parts)
            where_clause = sqlglot.parse_one(where_sql, dialect=self.dialect)

        having_clause = None
        if having_parts:
            having_sql = " AND ".join(having_parts)
            having_clause = sqlglot.parse_one(having_sql, dialect=self.dialect)

        return where_clause, having_clause
