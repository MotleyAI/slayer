"""SQL generator — converts EnrichedQuery to SQL via sqlglot AST.

The generator works exclusively with EnrichedQuery objects (fully resolved
SQL expressions). It never looks up model definitions — that's done by the
query engine's _enrich() step.
"""

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
}

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

    def _generate_base(self, enriched: EnrichedQuery) -> str:
        """Generate the base SELECT (measures, dimensions, filters)."""
        from_clause = self._build_from_clause(enriched=enriched)

        select_columns = []
        group_by_columns = []

        for dim in enriched.dimensions:
            col_expr = self._resolve_sql(sql=dim.sql, name=dim.name, model_name=dim.model_name)
            select_columns.append(col_expr.as_(dim.alias))
            group_by_columns.append(col_expr)

        for td in enriched.time_dimensions:
            col_expr = self._resolve_sql(sql=td.sql, name=td.name, model_name=td.model_name)
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

        if where_clause is not None:
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

        # Group transforms into layers: a transform goes in the first layer where
        # its measure_alias is available.
        # Separate calendar-based time_shift (with granularity) — handled via self-join CTE
        # Row-based time_shift (without granularity) goes through normal window function path
        time_shifts = [t for t in enriched.transforms if t.transform == "time_shift" and t.granularity]
        pending_expressions = list(enriched.expressions)
        pending_transforms = [t for t in enriched.transforms if not (t.transform == "time_shift" and t.granularity)]
        layer_num = 0

        while pending_expressions or pending_transforms:
            layer_num += 1
            prev_cte = ctes[-1][0]
            layer_name = f"step{layer_num}"
            layer_parts = [f'"{a}"' for a in sorted(available_aliases)]
            added_this_layer = []
            remaining_expressions = []
            remaining_transforms = []

            # Try to add expressions whose dependencies are all available
            for expr in pending_expressions:
                if self._deps_available(expr.sql, available_aliases):
                    layer_parts.append(f'{expr.sql} AS "{expr.alias}"')
                    added_this_layer.append(expr.alias)
                else:
                    remaining_expressions.append(expr)

            # Try to add transforms whose measure_alias is available
            for t in pending_transforms:
                if t.measure_alias in available_aliases:
                    window_sql = self._build_transform_sql(t)
                    layer_parts.append(f'{window_sql} AS "{t.alias}"')
                    added_this_layer.append(t.alias)
                else:
                    remaining_transforms.append(t)

            if not added_this_layer:
                break  # Nothing could be added — remaining items have unresolved deps

            layer_select = "SELECT\n    " + ",\n    ".join(layer_parts)
            ctes.append((layer_name, f"{layer_select}\nFROM {prev_cte}"))
            available_aliases.update(added_this_layer)

            pending_expressions = remaining_expressions
            pending_transforms = remaining_transforms

        # Build final CTE clause
        cte_strs = [f"{name} AS (\n{sql}\n)" for name, sql in ctes]

        # Add time_shift source CTEs
        final_cte = ctes[-1][0]
        for t in time_shifts:
            shift_name = f"shifted_{t.name}"
            cte_strs.append(f"{shift_name} AS (\n    SELECT * FROM {final_cte}\n)")

        cte_clause = "WITH " + ",\n".join(cte_strs)

        # Build final SELECT
        final_parts = [f'{final_cte}."{a}"' for a in sorted(available_aliases)]

        # Add any remaining expressions/transforms that couldn't be layered
        for expr in pending_expressions:
            final_parts.append(f'{expr.sql} AS "{expr.alias}"')
        for t in pending_transforms:
            if t.transform != "time_shift":
                window_sql = self._build_transform_sql(t)
                final_parts.append(f'{window_sql} AS "{t.alias}"')

        # Add time_shift columns
        for t in time_shifts:
            shift_name = f"shifted_{t.name}"
            final_parts.append(f'{shift_name}."{t.measure_alias}" AS "{t.alias}"')

        outer_select = "SELECT\n    " + ",\n    ".join(final_parts)

        # Build FROM with time_shift JOINs
        from_clause = f"FROM {final_cte}"
        for t in time_shifts:
            shift_name = f"shifted_{t.name}"
            time_col = f'"{t.time_alias}"'
            join_condition = self._build_time_shift_join(
                left_table=final_cte, right_table=shift_name,
                time_col=time_col, offset=t.offset, granularity=t.granularity or "month",
            )
            from_clause += f"\nLEFT JOIN {shift_name}\n    ON {join_condition}"

        sql = f"{cte_clause}\n{outer_select}\n{from_clause}"

        # Apply order/limit/offset to the outer query
        # We need to do this as raw SQL since we're building the outer query as a string
        if enriched.order:
            order_parts = []
            for order_item in enriched.order:
                col = order_item.column
                col_name = f"{col.model or enriched.model_name}.{col.name}"
                direction = "ASC" if order_item.direction == "asc" else "DESC"
                # Qualify with final CTE to avoid ambiguity in self-join
                qualifier = f"{final_cte}." if time_shifts else ""
                order_parts.append(f'{qualifier}"{col_name}" {direction}')
            sql += "\nORDER BY " + ", ".join(order_parts)

        if enriched.limit is not None:
            sql += f"\nLIMIT {enriched.limit}"

        if enriched.offset is not None:
            sql += f"\nOFFSET {enriched.offset}"

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
        elif t.transform == "time_shift":
            # Without granularity: row-based shift via LAG/LEAD
            # With granularity: handled separately via self-join CTE
            if t.granularity:
                raise ValueError("time_shift with granularity should not reach _build_transform_sql")
            if t.offset < 0:
                return f"LAG({measure}, {abs(t.offset)}) OVER ({order_clause})"
            else:
                return f"LEAD({measure}, {t.offset}) OVER ({order_clause})"
        elif t.transform == "change":
            return f"{measure} - LAG({measure}, {t.offset}) OVER ({order_clause})"
        elif t.transform == "change_pct":
            lag = f"LAG({measure}, {t.offset}) OVER ({order_clause})"
            return f"CASE WHEN {lag} != 0 THEN ({measure} - {lag}) * 1.0 / {lag} END"
        elif t.transform == "rank":
            return f"RANK() OVER (ORDER BY {measure} DESC)"
        elif t.transform == "last":
            return (
                f"FIRST_VALUE({measure}) OVER ({order_clause} DESC "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            )
        else:
            raise ValueError(f"Unsupported transform: {t.transform}")

    def _build_time_shift_join(self, left_table: str, right_table: str,
                               time_col: str, offset: int, granularity: str) -> str:
        """Build a JOIN condition for calendar-based time_shift."""
        if self.dialect == "sqlite":
            # SQLite: DATE(col, 'N months') for date arithmetic
            unit_map = {"year": "years", "month": "months", "day": "days",
                        "quarter": "months", "week": "days"}
            unit = unit_map.get(granularity, granularity + "s")
            multiplier = 3 if granularity == "quarter" else 7 if granularity == "week" else 1
            val = offset * multiplier
            return f"DATE({left_table}.{time_col}, '{val} {unit}') = {right_table}.{time_col}"
        # Standard SQL date arithmetic with dialect-specific syntax
        unit_map = {"year": "YEAR", "month": "MONTH", "day": "DAY",
                    "quarter": "MONTH", "week": "WEEK"}
        unit = unit_map.get(granularity, granularity.upper())
        val = offset * 3 if granularity == "quarter" else offset
        right_col = f"{right_table}.{time_col}"
        left_col = f"{left_table}.{time_col}"
        if self.dialect == "bigquery":
            return f"{left_col} = DATE_ADD({right_col}, INTERVAL {val} {unit})"
        elif self.dialect in ("snowflake", "redshift"):
            return f"{left_col} = DATEADD('{unit}', {val}, {right_col})"
        elif self.dialect == "clickhouse":
            return f"{left_col} = DATE_ADD({unit}, {val}, {right_col})"
        elif self.dialect in ("trino", "presto"):
            return f"{left_col} = DATE_ADD('{unit}', {val}, {right_col})"
        elif self.dialect in ("databricks", "spark"):
            return f"{left_col} = DATEADD({unit}, {val}, {right_col})"
        elif self.dialect == "tsql":
            return f"{left_col} = DATEADD({unit}, {val}, {right_col})"
        # Postgres / MySQL / DuckDB — standard INTERVAL syntax
        return f"{left_col} = {right_col} + INTERVAL '{val}' {unit}"

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



    # ------------------------------------------------------------------
    # Column / measure resolution (from enriched SQL expressions)
    # ------------------------------------------------------------------

    def _resolve_sql(self, sql: Optional[str], name: str, model_name: str) -> exp.Expression:
        """Resolve an enriched SQL expression to a sqlglot AST node."""
        if sql is None:
            return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(model_name))
        # Bare column name → qualify with model name
        if "${" not in sql and "." not in sql and " " not in sql and "(" not in sql:
            return exp.Column(this=exp.to_identifier(sql), table=exp.to_identifier(model_name))
        # ${TABLE} placeholder expansion
        cleaned = sql.replace("${TABLE}", model_name)
        cleaned = cleaned.replace("${" + model_name + "}", model_name)
        return sqlglot.parse_one(sql=cleaned, dialect=self.dialect)

    def _build_agg(self, measure: EnrichedMeasure) -> tuple[exp.Expression, bool]:
        """Build an aggregation expression from an enriched measure."""
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
            # Qualify column names with model name (deduplicate, word boundary, skip already qualified)
            qualified_sql = f.sql
            for col_name in dict.fromkeys(f.columns):  # deduplicate preserving order
                qualified_sql = re.sub(
                    rf'(?<!\.)(?<!\w)\b{re.escape(col_name)}\b',
                    f"{model}.{col_name}",
                    qualified_sql,
                )
            if f.is_having:
                having_parts.append(qualified_sql)
            else:
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
