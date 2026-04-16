"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → _enrich() → EnrichedQuery → SQLGenerator → SQL → execute
"""

import decimal
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType, format_number
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.enriched import (
    CrossModelMeasure,
    EnrichedMeasure,
    EnrichedQuery,
)
from slayer.engine.enrichment import enrich_query
from slayer.sql.client import SlayerSQLClient
from slayer.sql.generator import SQLGenerator
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


_EXPLAIN_PREFIX = {
    "postgres": "EXPLAIN ANALYZE",
    "redshift": "EXPLAIN",
    "mysql": "EXPLAIN FORMAT=JSON",
    "sqlite": "EXPLAIN QUERY PLAN",
    "duckdb": "EXPLAIN ANALYZE",
    "clickhouse": "EXPLAIN",
    "snowflake": "EXPLAIN USING JSON",
    "bigquery": None,  # BigQuery doesn't support EXPLAIN via SQL
    "trino": "EXPLAIN ANALYZE",
    "presto": "EXPLAIN ANALYZE",
    "databricks": "EXPLAIN EXTENDED",
    "spark": "EXPLAIN EXTENDED",
    "tsql": "SET SHOWPLAN_ALL ON;",  # SQL Server: batch prefix, needs suffix too
    "oracle": "EXPLAIN PLAN FOR",
}


_EXPLAIN_POSTFIX = {
    "tsql": "; SET SHOWPLAN_ALL OFF",
}


def _build_explain_sql(dialect: str, sql: str) -> str:
    """Build a dialect-appropriate EXPLAIN statement."""
    prefix = _EXPLAIN_PREFIX.get(dialect)
    if prefix is None:
        raise ValueError(
            f"EXPLAIN is not supported for dialect '{dialect}'. Use dry_run=True to inspect the generated SQL instead."
        )
    suffix = _EXPLAIN_POSTFIX.get(dialect, "")
    return f"{prefix} {sql}{suffix}"


@dataclass
class FieldMetadata:
    """Metadata for a single field in the query response."""

    label: Optional[str] = None
    format: Optional[NumberFormat] = None


@dataclass
class SlayerResponse:
    """Response from a SLayer query."""

    data: List[Dict[str, Any]]
    columns: List[str] = field(default_factory=list)
    sql: Optional[str] = None
    meta: Dict[str, FieldMetadata] = field(default_factory=dict)

    def __post_init__(self):
        if not self.columns and self.data:
            self.columns = list(self.data[0].keys())

    @property
    def row_count(self) -> int:
        return len(self.data)

    def _format_value(self, column: str, value: Any) -> str:
        """Format a single cell value using column format metadata if available."""
        if value is None:
            return ""
        fm = self.meta.get(column)
        if fm and fm.format and isinstance(value, (int, float, decimal.Decimal)):
            return format_number(value=value, format_spec=fm.format)
        return str(value)

    def to_markdown(self) -> str:
        """Format data as a Markdown table with number formatting applied."""
        if not self.data:
            return "No results."
        header = "| " + " | ".join(self.columns) + " |"
        separator = "| " + " | ".join("---" for _ in self.columns) + " |"
        body_lines = []
        for row in self.data:
            cells = [self._format_value(column=c, value=row.get(c, "")) for c in self.columns]
            body_lines.append("| " + " | ".join(cells) + " |")
        return "\n".join([header, separator] + body_lines)


def _infer_aggregated_format(
    model: SlayerModel,
    measure_name: str,
    aggregation: str,
) -> Optional[NumberFormat]:
    """Infer NumberFormat for an aggregated measure based on aggregation type and source measure format.

    Rules:
    - count, count_distinct: always INTEGER
    - avg, weighted_avg, median: always FLOAT
    - sum, min, max, first, last: inherit from source measure
    - *:count (measure_name="*"): INTEGER
    """
    if measure_name == "*":
        return NumberFormat(type=NumberFormatType.INTEGER)

    if aggregation in ("count", "count_distinct"):
        return NumberFormat(type=NumberFormatType.INTEGER)

    if aggregation in ("avg", "weighted_avg", "median"):
        return NumberFormat(type=NumberFormatType.FLOAT)

    # sum, min, max, first, last: inherit from source measure
    source_measure = model.get_measure(measure_name)
    if source_measure and source_measure.format:
        return source_measure.format

    return None


class SlayerQueryEngine:
    """Central orchestrator: resolves queries via storage, generates SQL, executes.

    The engine enriches a SlayerQuery (user-facing, just names) into an
    EnrichedQuery (fully resolved SQL expressions), then passes it to the
    SQLGenerator for SQL generation.
    """

    def __init__(self, storage: StorageBackend):
        self.storage = storage
        self._sql_clients: Dict[str, SlayerSQLClient] = {}  # connection string → cached client

    async def execute(self, query: "SlayerQuery | dict | list[SlayerQuery | dict]") -> SlayerResponse:
        # Accept dicts and validate them into SlayerQuery objects
        if isinstance(query, list):
            queries = [SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in query]
            query = queries[-1]
            named_queries = {}
            for q in queries[:-1]:
                if q.name:
                    if q.name in named_queries:
                        raise ValueError(f"Duplicate query name '{q.name}' in query list")
                    named_queries[q.name] = q
        else:
            if isinstance(query, dict):
                query = SlayerQuery.model_validate(query)
            named_queries = {}

        # Preprocessing
        if query.whole_periods_only:
            query = query.snap_to_whole_periods()

        # Resolve model from query.source_model (str, SlayerModel, or ModelExtension)
        resolving: set = set()
        model = await self._resolve_query_model(
            query_model=query.source_model,
            named_queries=named_queries,
            _resolving=resolving,
        )

        datasource = await self._resolve_datasource(model=model)

        # Enrich: SlayerQuery + model → EnrichedQuery
        enriched = await self._enrich(query=query, model=model, named_queries=named_queries)

        # Generate SQL from EnrichedQuery
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        sql = generator.generate(enriched=enriched)
        logger.debug("Generated SQL:\n%s", sql)

        # Collect field metadata from enriched query
        meta: Dict[str, FieldMetadata] = {}
        for d in enriched.dimensions:
            if d.label or d.format:
                meta[d.alias] = FieldMetadata(label=d.label, format=d.format)
        for td in enriched.time_dimensions:
            if td.label:
                meta[td.alias] = FieldMetadata(label=td.label)
        for m in enriched.measures:
            measure_fmt = _infer_aggregated_format(
                model=model,
                measure_name=m.source_measure_name or m.name,
                aggregation=m.aggregation,
            )
            if m.label or measure_fmt:
                meta[m.alias] = FieldMetadata(label=m.label, format=measure_fmt)
        for e in enriched.expressions:
            meta[e.alias] = FieldMetadata(
                label=e.label,
                format=NumberFormat(type=NumberFormatType.FLOAT),
            )
        for t in enriched.transforms:
            meta[t.alias] = FieldMetadata(
                label=t.label,
                format=NumberFormat(type=NumberFormatType.FLOAT),
            )
        for cm in enriched.cross_model_measures:
            if cm.label or cm.format:
                meta[cm.alias] = FieldMetadata(label=cm.label, format=cm.format)

        # Derive expected column names from the enriched query, excluding internal aliases
        # (_inner_* from nested transforms, _ft* from filter transform extraction)
        expected_columns = (
            [d.alias for d in enriched.dimensions]
            + [td.alias for td in enriched.time_dimensions]
            + [m.alias for m in enriched.measures if not m.name.startswith(("_inner_", "_ft"))]
            + [e.alias for e in enriched.expressions]
            + [t.alias for t in enriched.transforms if not t.name.startswith(("_inner_", "_ft"))]
            + [cm.alias for cm in enriched.cross_model_measures]
        )

        # dry_run: return SQL without executing
        if query.dry_run:
            return SlayerResponse(data=[], columns=expected_columns, sql=sql, meta=meta)

        # Execute — reuse SQL client (and its connection pool) per datasource
        ds_key = datasource.get_connection_string()
        if ds_key not in self._sql_clients:
            self._sql_clients[ds_key] = SlayerSQLClient(datasource=datasource)
        client = self._sql_clients[ds_key]

        # explain: run dialect-appropriate EXPLAIN on the query
        if query.explain:
            explain_sql = _build_explain_sql(dialect=dialect, sql=sql)
            rows = await client.execute(sql=explain_sql)
            return SlayerResponse(data=rows, sql=sql, meta=meta)

        rows = await client.execute(sql=sql)
        columns = expected_columns if not rows else []  # fallback for empty results; [] triggers auto-derive
        return SlayerResponse(data=rows, columns=columns, sql=sql, meta=meta)

    def execute_sync(self, query: "SlayerQuery | dict | list[SlayerQuery | dict]") -> SlayerResponse:
        """Synchronous wrapper for execute(). For CLI, notebooks, and scripts."""
        from slayer.async_utils import run_sync

        return run_sync(self.execute(query))

    def create_model_from_query_sync(
        self, query: "SlayerQuery | list[SlayerQuery]", name: str,
        description: str = None, save: bool = True,
    ) -> SlayerModel:
        """Synchronous wrapper for create_model_from_query()."""
        from slayer.async_utils import run_sync

        return run_sync(self.create_model_from_query(query=query, name=name, description=description, save=save))

    async def _resolve_query_model(self, query_model, named_queries: dict = None, _resolving: set = None) -> SlayerModel:
        """Resolve query.source_model — handles str, SlayerModel, and ModelExtension."""
        from slayer.core.query import ModelExtension

        named_queries = named_queries or {}

        if isinstance(query_model, str):
            return await self._resolve_model(model_name=query_model, named_queries=named_queries, _resolving=_resolving)
        elif isinstance(query_model, SlayerModel):
            return query_model
        elif isinstance(query_model, ModelExtension):
            base = await self._resolve_model(
                model_name=query_model.source_name,
                named_queries=named_queries,
                _resolving=_resolving,
            )
            # Extend the base model with extra dims/measures/joins
            from slayer.core.models import ModelJoin

            extra_dims = [
                Dimension.model_validate(d) if isinstance(d, dict) else d for d in (query_model.dimensions or [])
            ]
            extra_measures = [
                Measure.model_validate(m) if isinstance(m, dict) else m for m in (query_model.measures or [])
            ]
            extra_joins = [ModelJoin.model_validate(j) if isinstance(j, dict) else j for j in (query_model.joins or [])]
            return base.model_copy(
                update={
                    "dimensions": list(base.dimensions) + extra_dims,
                    "measures": list(base.measures) + extra_measures,
                    "joins": list(base.joins) + extra_joins,
                }
            )
        elif isinstance(query_model, dict):
            # Dict — could be ModelExtension or SlayerModel
            if "source_name" in query_model:
                ext = ModelExtension.model_validate(query_model)
                return await self._resolve_query_model(ext, named_queries, _resolving=_resolving)
            else:
                model = SlayerModel.model_validate(query_model)
                return model
        else:
            raise ValueError(f"Invalid query.source_model type: {type(query_model)}")

    async def _resolve_model(
        self, model_name: str, named_queries: dict[str, SlayerQuery] = None,
        _resolving: set = None,
    ) -> SlayerModel:
        """Resolve a model by name — checks named queries first, then storage."""
        named_queries = named_queries or {}
        _resolving = _resolving if _resolving is not None else set()

        # Circular reference protection (per-call set, safe for concurrent requests)
        if model_name in _resolving:
            raise ValueError(
                f"Circular reference detected: '{model_name}' references itself "
                f"(resolution chain: {' → '.join(_resolving)} → {model_name})"
            )
        _resolving.add(model_name)
        try:
            return await self._resolve_model_inner(model_name, named_queries, _resolving=_resolving)
        finally:
            _resolving.discard(model_name)

    async def _resolve_model_inner(self, model_name: str, named_queries: dict[str, SlayerQuery], _resolving: set = None) -> SlayerModel:
        # Named query overrides stored model
        if model_name in named_queries:
            return await self._query_as_model(inner_query=named_queries[model_name], named_queries=named_queries, _resolving=_resolving)

        model = await self.storage.get_model(model_name)
        if model is None:
            raise ValueError(f"Model '{model_name}' not found")

        # If model has source_queries, re-enrich from stored queries
        if hasattr(model, "source_queries") and model.source_queries:
            # Parse stored queries (may be dicts from YAML round-trip)
            parsed = [SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in model.source_queries]
            return await self._query_as_model(
                inner_query=parsed[-1],
                named_queries={q.name: q for q in parsed[:-1] if q.name},
                override_name=model.name,
                _resolving=_resolving,
            )

        return model

    async def create_model_from_query(
        self,
        query: "SlayerQuery | list[SlayerQuery]",
        name: str,
        description: str = None,
        save: bool = True,
    ) -> SlayerModel:
        """Create a permanent model from a query (or list of queries).

        Saves the query structure in the model so it can be re-enriched
        when underlying models change. Also snapshots dimensions/measures
        for discoverability.

        Args:
            query: The source query or list of queries (last is main).
            name: Name for the new model.
            description: Optional model description.
            save: If True, persist to storage immediately.
        """
        queries = query if isinstance(query, list) else [query]
        main_query = queries[-1]
        named = {q.name: q for q in queries[:-1] if q.name}
        virtual = await self._query_as_model(inner_query=main_query, named_queries=named, _resolving=set())
        model = SlayerModel(
            name=name,
            source_queries=queries,
            data_source=virtual.data_source,
            dimensions=virtual.dimensions,
            measures=virtual.measures,
            description=description,
        )
        if save:
            await self.storage.save_model(model)
        return model

    async def _enrich(
        self,
        query: SlayerQuery,
        model: SlayerModel,
        named_queries: dict[str, SlayerQuery] = None,
    ) -> EnrichedQuery:
        """Resolve a SlayerQuery against model definitions into an EnrichedQuery.

        Delegates to enrich_query() in enrichment.py, passing engine callbacks
        for model resolution (joins, cross-model measures, join targets).
        """

        async def _resolve_join_target(target_model_name, named_queries):
            nq = named_queries or {}
            if target_model_name in nq:
                target = await self._query_as_model(
                    inner_query=nq[target_model_name],
                    named_queries=nq,
                )
            else:
                target = await self.storage.get_model(target_model_name) if self.storage else None
            if target and target.sql_table:
                return target.sql_table, target
            elif target and target.sql:
                return f"({target.sql})", target
            return None

        return await enrich_query(
            query=query,
            model=model,
            named_queries=named_queries,
            resolve_dimension_via_joins=self._resolve_dimension_via_joins,
            resolve_cross_model_measure=self._resolve_cross_model_measure,
            resolve_join_target=_resolve_join_target,
        )

    async def _query_as_model(
        self, inner_query: SlayerQuery, named_queries: dict[str, SlayerQuery] = None,
        override_name: str = None, _resolving: set = None,
    ) -> SlayerModel:
        """Build a virtual SlayerModel from a nested query's result.

        Enriches and generates SQL for the inner query, then creates a model
        whose `sql` is the inner query's SQL and whose dimensions/measures
        are derived from the inner query's enriched columns.
        """
        named_queries = named_queries or {}

        # Resolve the inner model (handles str, SlayerModel, ModelExtension)
        inner_model = await self._resolve_query_model(
            query_model=inner_query.source_model,
            named_queries=named_queries,
            _resolving=_resolving,
        )

        # Enrich the inner query
        enriched = await self._enrich(query=inner_query, model=inner_model)

        # Generate SQL
        datasource = await self._resolve_datasource(model=inner_model)
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        inner_sql = generator.generate(enriched=enriched)

        # Build virtual model from enriched columns.
        # Inner query columns have aliases like "orders.count" (with dots).
        # We wrap the inner SQL in a renaming subquery so the virtual model
        # has clean column names that work naturally in JOINs and references.
        virtual_name = override_name or inner_query.name or f"_subquery_{inner_model.name}"

        # Build lookups for labels/descriptions from the source model
        source_dim_label = {d.name: d.label for d in inner_model.dimensions if d.label}
        source_dim_desc = {d.name: d.description for d in inner_model.dimensions if d.description}
        source_measure_label = {m.name: m.label for m in inner_model.measures if m.label}
        source_measure_desc = {m.name: m.description for m in inner_model.measures if m.description}

        # Collect all inner aliases and their short names.
        # Short names must be valid SQL identifiers (no dots). We derive them
        # from the alias by stripping the source model prefix and replacing
        # dots with underscores.
        def _alias_to_short(alias: str) -> str:
            """Convert result alias to a flat column name for the virtual model.

            The query result is a self-contained table without the joins the
            source model may have had, so dot syntax (join paths) is not
            applicable. We use ``__`` to preserve the path information:

            'orders.customers.regions.name' → 'customers__regions__name'
            'orders.count'                  → 'count'
            """
            # Strip source model prefix
            stripped = alias.split(".", 1)[-1] if "." in alias else alias
            # Replace remaining dots with __ to encode the original join path
            return stripped.replace(".", "__")

        # (inner_alias, short_name, data_type, is_measure, label, description, format)
        column_map = []
        for d in enriched.dimensions:
            short = _alias_to_short(d.alias)
            label = d.label or source_dim_label.get(d.name)
            desc = source_dim_desc.get(d.name)
            column_map.append((d.alias, short, d.type, False, label, desc, d.format))
        for td in enriched.time_dimensions:
            short = _alias_to_short(td.alias)
            label = td.label or source_dim_label.get(td.name)
            desc = source_dim_desc.get(td.name)
            column_map.append((td.alias, short, DataType.TIMESTAMP, False, label, desc, None))
        for m in enriched.measures:
            src_name = m.source_measure_name or m.name
            label = m.label or source_measure_label.get(src_name)
            desc = source_measure_desc.get(src_name)
            fmt = _infer_aggregated_format(
                model=inner_model,
                measure_name=src_name,
                aggregation=m.aggregation,
            )
            column_map.append((m.alias, m.name, DataType.NUMBER, True, label, desc, fmt))
        for t in enriched.transforms:
            column_map.append(
                (t.alias, t.name, DataType.NUMBER, True, t.label, None, NumberFormat(type=NumberFormatType.FLOAT))
            )
        for e in enriched.expressions:
            column_map.append(
                (e.alias, e.name, DataType.NUMBER, True, e.label, None, NumberFormat(type=NumberFormatType.FLOAT))
            )
        for cm in enriched.cross_model_measures:
            short = _alias_to_short(cm.alias)
            column_map.append((cm.alias, short, DataType.NUMBER, True, cm.label, None, cm.format))

        # Wrap inner SQL: SELECT "orders.id" AS id, "orders.count" AS count, ... FROM (inner) AS _inner
        rename_parts = [f'"{alias}" AS {short}' for alias, short, _, _, _, _, _ in column_map]
        wrapped_sql = f"SELECT {', '.join(rename_parts)} FROM ({inner_sql}) AS _inner"

        dims = []
        for _, short, dtype, _, label, desc, fmt in column_map:
            dims.append(Dimension(name=short, sql=short, type=dtype, label=label, description=desc, format=fmt))

        # One measure per column. Aggregation is specified at query time
        # using colon syntax (e.g., "order_total_sum:avg"). *:count is always
        # available for COUNT(*) without a measure definition.
        measures = []
        for _, short, _, _, label, desc, fmt in column_map:
            measures.append(Measure(name=short, sql=short, label=label, description=desc, format=fmt))

        return SlayerModel(
            name=virtual_name,
            sql=wrapped_sql,
            data_source=inner_model.data_source,
            dimensions=dims,
            measures=measures,
            default_time_dimension=inner_model.default_time_dimension,
        )

    async def _resolve_dimension_via_joins(
        self,
        model: SlayerModel,
        parts: list[str],
        named_queries: dict = None,
    ) -> "Dimension | None":
        """Walk the join graph to resolve a multi-hop dimension.

        For "customers.regions.name", walks: model → customers → regions,
        then looks up "name" on the regions model.
        """
        current_model = model
        visited = {model.name}
        # Walk intermediate models (all parts except the last, which is the dim name)
        for hop_name in parts[:-1]:
            if hop_name in visited:
                raise ValueError(
                    f"Circular join detected while resolving '{'.'.join(parts)}': "
                    f"'{hop_name}' already visited ({' → '.join(visited)} → {hop_name})"
                )
            # Find join to this hop
            join = None
            for j in current_model.joins:
                if j.target_model == hop_name:
                    join = j
                    break
            if join is None:
                return None  # No join found for this hop
            # Load the target model
            target = await self._resolve_model(
                model_name=hop_name,
                named_queries=named_queries or {},
            )
            visited.add(hop_name)
            current_model = target

        # Look up the final dimension on the terminal model
        dim_name = parts[-1]
        return current_model.get_dimension(dim_name)

    async def _resolve_cross_model_measure(
        self,
        spec_name: str,
        field_name: str,
        model: SlayerModel,
        query,
        dimensions: list,
        time_dimensions: list,
        label: str = None,
        named_queries: dict = None,
        aggregation_name: str = None,
        agg_kwargs: dict = None,
    ) -> CrossModelMeasure:
        """Resolve a cross-model measure reference like 'customers.avg_score'.

        Looks up the join from the source model, loads the target model
        (checking named queries first), finds shared dimensions, and returns
        a CrossModelMeasure for SQL generation.
        """
        parts = spec_name.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid cross-model measure reference: '{spec_name}'")
        target_model_name, measure_name = parts

        # Find the join to the target model
        join = None
        for j in model.joins:
            if j.target_model == target_model_name:
                join = j
                break
        if join is None:
            raise ValueError(
                f"Model '{model.name}' has no join to '{target_model_name}'. "
                f"Available joins: {[j.target_model for j in model.joins]}"
            )

        # Load the target model (named queries take precedence)
        target_model = await self._resolve_model(
            model_name=target_model_name,
            named_queries=named_queries or {},
        )

        # Find the measure in the target model (* = COUNT(*), no measure needed)
        if measure_name == "*":
            from slayer.core.models import Measure

            measure_def = Measure(name="*", sql=None)
        else:
            measure_def = target_model.get_measure(measure_name)
            if measure_def is None:
                raise ValueError(
                    f"Measure '{measure_name}' not found in model '{target_model_name}'. "
                    f"Available: {[m.name for m in target_model.measures]}"
                )

        # The cross-model sub-query starts FROM the source table with JOIN to
        # the target, so all source dimensions are available for grouping.
        # Use all query dimensions and time dimensions as the grouping context.
        shared_dims = list(dimensions)
        shared_time_dims = list(time_dimensions)

        query_model_name = query.source_model if isinstance(query.source_model, str) else model.name

        # Resolve aggregation: explicit colon syntax required
        if aggregation_name:
            agg = aggregation_name
            canonical = f"_{aggregation_name}" if measure_name == "*" else f"{measure_name}_{aggregation_name}"
        else:
            raise ValueError(
                f"Cross-model measure '{spec_name}' must include an aggregation (e.g., '{spec_name}:sum')."
            )

        alias = f"{query_model_name}.{target_model_name}.{canonical}"
        aggregation_def = target_model.get_aggregation(agg)

        # Infer format from the target model's measure and aggregation
        cm_format = _infer_aggregated_format(
            model=target_model,
            measure_name=measure_name,
            aggregation=agg,
        )

        return CrossModelMeasure(
            name=field_name,
            alias=alias,
            target_model_name=target_model_name,
            target_model_sql_table=target_model.sql_table,
            target_model_sql=target_model.sql,
            measure=EnrichedMeasure(
                name=canonical,
                sql=measure_def.sql,
                aggregation=agg,
                alias=f"{target_model_name}.{canonical}",
                model_name=target_model_name,
                aggregation_def=aggregation_def,
                agg_kwargs=agg_kwargs or {},
                source_measure_name=measure_name,
            ),
            join_pairs=join.join_pairs,
            shared_dimensions=shared_dims,
            shared_time_dimensions=shared_time_dims,
            source_model_name=model.name,
            source_sql_table=model.sql_table,
            source_sql=model.sql,
            label=label,
            format=cm_format,
        )

    async def _resolve_datasource(self, model: SlayerModel) -> DatasourceConfig:
        ds_name = model.data_source
        if not ds_name:
            raise ValueError(
                f"Model '{model.name}' has no data_source configured. "
                f"Set data_source on the model or ensure the source model has one."
            )
        ds = await self.storage.get_datasource(ds_name)
        if ds is None:
            raise ValueError(f"Datasource '{ds_name}' not found for model '{model.name}'")
        return ds

    @staticmethod
    def _dialect_for_type(ds_type: Optional[str]) -> str:
        _DIALECT_MAP = {
            "postgres": "postgres",
            "postgresql": "postgres",
            "mysql": "mysql",
            "mariadb": "mysql",
            "clickhouse": "clickhouse",
            "bigquery": "bigquery",
            "snowflake": "snowflake",
            "sqlite": "sqlite",
            "duckdb": "duckdb",
            "redshift": "redshift",
            "trino": "trino",
            "presto": "presto",
            "athena": "presto",
            "databricks": "databricks",
            "spark": "spark",
            "mssql": "tsql",
            "sqlserver": "tsql",
            "tsql": "tsql",
            "oracle": "oracle",
        }
        return _DIALECT_MAP.get(ds_type or "", "postgres")
