"""SQL generator — converts EnrichedQuery to SQL via sqlglot AST.

The generator works exclusively with EnrichedQuery objects (fully resolved
SQL expressions). It never looks up model definitions — that's done by the
query engine's _enrich() step.
"""

import copy
import logging
from typing import Optional

import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.engine.enriched import EnrichedMeasure, EnrichedQuery

logger = logging.getLogger(__name__)

_AGG_FUNCTION_MAP = {
    DataType.COUNT: "COUNT",
    DataType.COUNT_DISTINCT: "COUNT_DISTINCT",
    DataType.SUM: "SUM",
    DataType.AVERAGE: "AVG",
    DataType.MIN: "MIN",
    DataType.MAX: "MAX",
    # DataType.LAST is not here — it uses a special ROW_NUMBER + conditional aggregate
}

# Transforms that use self-join CTEs instead of window functions.
# This gives correct results at result-set edges (no NULLs when the DB has the data)
# and handles gaps in time series correctly.
_SELF_JOIN_TRANSFORMS = {"time_shift", "change", "change_pct"}

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


class SQLGenerator:
    """Generates SQL from an EnrichedQuery."""

    def __init__(self, dialect: str = "postgres"):
        self.dialect = dialect

    def generate(self, enriched: EnrichedQuery) -> str:
        """Generate SQL from a fully resolved EnrichedQuery.

        When expressions or transforms are present, the base query becomes a CTE
        and computed columns are added in an outer SELECT.
        """
        base_sql = self._generate_base(enriched=enriched)
        has_computed = bool(enriched.expressions or enriched.transforms)

        if not has_computed:
            return base_sql

        # Wrap base query as CTE, compute expressions/transforms in outer SELECT
        return self._generate_with_computed(enriched=enriched, base_sql=base_sql)

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
                        time_offset: Optional[tuple[int, str]] = None) -> str:
        """Generate the base SELECT (measures, dimensions, filters).

        Args:
            time_offset: Optional (offset, granularity) to shift raw timestamps
                before DATE_TRUNC. Used by shifted CTEs so aggregated buckets
                align with the base query for simple equality joins.
        """
        from_clause = self._build_from_clause(enriched=enriched)

        # If any measure has type=last, prepend a ROW_NUMBER CTE to mark the
        # latest row per group. The FROM is replaced with this ranked subquery.
        has_last_measures = any(m.type == DataType.LAST for m in enriched.measures)
        if has_last_measures and enriched.last_agg_time_column:
            from_clause = self._build_last_ranked_from(
                enriched=enriched, base_from=from_clause, time_offset=time_offset,
            )

        select_columns = []
        group_by_columns = []

        for dim in enriched.dimensions:
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name)
            if has_last_measures:
                # In ranked subquery, dimensions are already columns — reference directly
                col_expr = exp.Column(this=exp.to_identifier(dim.name))
            select_columns.append(col_expr.as_(dim.alias))
            group_by_columns.append(col_expr)

        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
            if has_last_measures:
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
            agg_expr, is_agg = self._build_agg(measure=measure)
            select_columns.append(agg_expr.as_(measure.alias))
            if is_agg:
                has_aggregation = True

        where_clause, having_clause = self._build_where_and_having(enriched=enriched)

        select = exp.Select()
        for col in select_columns:
            select = select.select(col)

        select = select.from_(from_clause)

        # When using ranked subquery for type=last, WHERE is already inside the subquery
        if where_clause is not None and not has_last_measures:
            select = select.where(where_clause)

        if has_aggregation and group_by_columns:
            for gb in group_by_columns:
                select = select.group_by(gb)

        if having_clause is not None:
            select = select.having(having_clause)

        # When no computed columns, apply order/limit/offset to the base query.
        # Otherwise, they'll be applied to the outer query.
        if not enriched.expressions and not enriched.transforms:
            select = self._apply_order_limit(select=select, enriched=enriched)

        return select.sql(dialect=self.dialect, pretty=True)

    def _generate_with_computed(self, enriched: EnrichedQuery, base_sql: str) -> str:
        """Wrap the base query as a CTE and add expressions/transforms as stacked CTE layers.

        Transforms that reference other transforms' outputs get their own CTE layer.
        This handles arbitrary nesting like change(cumsum(revenue)).
        """
        # Collect base aliases
        base_aliases = []
        for dim in enriched.dimensions:
            base_aliases.append(dim.alias)
        for td in enriched.time_dimensions:
            base_aliases.append(td.alias)
        for m in enriched.measures:
            base_aliases.append(m.alias)

        # Build stacked CTEs. Each layer can reference aliases from previous layers.
        ctes = [("base", base_sql)]
        available_aliases = set(base_aliases)  # Aliases available in the current layer

        # All transforms go into a unified layering loop. Each iteration tries
        # to resolve transforms whose inputs are available. Self-join transforms
        # (time_shift, change, change_pct) get their own CTE with a LEFT JOIN.
        # Window transforms (cumsum, lag, lead, rank, last) are batched into a
        # single CTE layer with OVER() expressions.
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

        # Apply order/limit/offset to the outer query
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
                col_name = f"{col.model or enriched.model_name}.{col.name}"
                order_col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
                ascending = order_item.direction == "asc"
                select = select.order_by(exp.Ordered(this=order_col, desc=not ascending))

        if enriched.limit is not None:
            select = select.limit(enriched.limit)

        if enriched.offset is not None:
            select = select.offset(enriched.offset)

        return select

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

    def _build_last_ranked_from(self, enriched: EnrichedQuery,
                                 base_from: exp.Expression,
                                 time_offset: Optional[tuple[int, str]] = None) -> exp.Expression:
        """Build a ranked subquery for `type: last` aggregation.

        Wraps the source table in a subquery that adds:
          ROW_NUMBER() OVER (PARTITION BY [group-dims] ORDER BY time_col DESC) AS _last_rn
        Plus all original columns and pre-computed time dimension expressions.
        The outer query then uses MAX(CASE WHEN _last_rn = 1 THEN col END) for last-type measures.
        """
        model = enriched.model_name
        time_col = enriched.last_agg_time_column

        # Build SELECT * plus ROW_NUMBER
        parts = [f"{model}.*"]

        # Add pre-computed time dimension expressions (DATE_TRUNC)
        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=model)
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
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=model)
            partition_parts.append(col_expr.sql(dialect=self.dialect))
        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=model)
            if time_offset is not None:
                offset_val, offset_gran = time_offset
                col_expr = self._build_time_offset_expr(
                    col_expr=col_expr, offset=offset_val, granularity=offset_gran,
                )
            td_expr = self._build_date_trunc(col_expr=col_expr, granularity=td.granularity)
            partition_parts.append(td_expr.sql(dialect=self.dialect))

        partition_clause = f"PARTITION BY {', '.join(partition_parts)}" if partition_parts else ""

        # ORDER BY the resolved time column
        time_col_expr = self._resolve_sql(sql=None, name=time_col, model_name=model)
        order_sql = time_col_expr.sql(dialect=self.dialect)

        rn_expr = f"ROW_NUMBER() OVER ({partition_clause} ORDER BY {order_sql} DESC) AS _last_rn"
        parts.append(rn_expr)

        select_sql = ", ".join(parts)
        from_sql = base_from.sql(dialect=self.dialect)
        ranked_sql = f"SELECT {select_sql} FROM {from_sql}"

        # Apply WHERE filters to the subquery (they filter raw data before ranking)
        where_clause, _ = self._build_where_and_having(enriched=enriched)
        if where_clause is not None:
            ranked_sql += f" WHERE {where_clause.sql(dialect=self.dialect)}"

        parsed = sqlglot.parse_one(ranked_sql, dialect=self.dialect)
        return exp.Subquery(this=parsed, alias=exp.to_identifier(model))

    # ------------------------------------------------------------------
    # Column / measure resolution (from enriched SQL expressions)
    # ------------------------------------------------------------------

    def _resolve_sql(self, sql: Optional[str], name: str, model_name: str) -> exp.Expression:
        """Resolve an enriched SQL expression to a sqlglot AST node."""
        if sql is None:
            return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(model_name))
        # Bare column name → qualify with model name
        if "." not in sql and " " not in sql and "(" not in sql:
            return exp.Column(this=exp.to_identifier(sql), table=exp.to_identifier(model_name))
        return sqlglot.parse_one(sql=sql, dialect=self.dialect)

    def _build_agg(self, measure: EnrichedMeasure) -> tuple[exp.Expression, bool]:
        """Build an aggregation expression from an enriched measure."""
        # type=last: MAX(CASE WHEN _last_rn = 1 THEN col END)
        # The _last_rn column comes from _build_last_ranked_from
        if measure.type == DataType.LAST:
            col = measure.sql or measure.name
            case_sql = f"MAX(CASE WHEN _last_rn = 1 THEN {measure.model_name}.{col} END)"
            return sqlglot.parse_one(case_sql, dialect=self.dialect), True

        agg_func = _AGG_FUNCTION_MAP.get(measure.type)
        if agg_func is None:
            # Not an aggregation — raw expression
            if measure.sql:
                return self._resolve_sql(sql=measure.sql, name=measure.name, model_name=measure.model_name), False
            return exp.Column(
                this=exp.to_identifier(measure.name),
                table=exp.to_identifier(measure.model_name),
            ), False

        # COUNT(*) special case
        if measure.type == DataType.COUNT and measure.sql is None:
            inner = exp.Star()
        elif measure.sql:
            inner = self._resolve_sql(sql=measure.sql, name=measure.name, model_name=measure.model_name)
        else:
            inner = exp.Column(
                this=exp.to_identifier(measure.name),
                table=exp.to_identifier(measure.model_name),
            )

        if measure.type == DataType.COUNT_DISTINCT:
            return exp.Count(this=exp.Distinct(expressions=[inner])), True

        agg_class_map = {
            "COUNT": exp.Count,
            "SUM": exp.Sum,
            "AVG": exp.Avg,
            "MIN": exp.Min,
            "MAX": exp.Max,
        }
        agg_class = agg_class_map[agg_func]
        return agg_class(this=inner), True

    # ------------------------------------------------------------------
    # WHERE / HAVING (filters still use ColumnRef for member resolution)
    # ------------------------------------------------------------------

    def _build_where_and_having(
        self, enriched: EnrichedQuery,
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
                            agg_expr, _ = self._build_agg(measure=m)
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
                qualified_sql = f.sql
                for col_name in dict.fromkeys(f.columns):
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
