"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → _enrich() → EnrichedQuery → SQLGenerator → SQL → execute
"""

import decimal
import logging
from contextvars import ContextVar
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field as PydanticField, model_validator

from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, DataType
from slayer.core.format import NumberFormat, NumberFormatType, format_number
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import (
    ColumnRef,
    SlayerQuery,
    TimeDimension,
    extract_placeholder_names,
)
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


# Per-task in-flight join-target names. Used by _resolve_join_target to break
# loops when a query-backed target's own join graph references it back. Lives
# in a ContextVar (not on the engine) so concurrent requests through the same
# engine don't see each other's in-flight state — each asyncio task gets its
# own copy of the context. The default=None + lazy-init pattern below means
# only tasks that actually hit a query-backed join target allocate a set.
_join_target_resolving_var: ContextVar[Optional[set]] = ContextVar(
    "_join_target_resolving", default=None
)


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


_PLACEHOLDER_FILL_VALUE = "0"


def _merge_query_variables(
    *,
    outer: Optional[Dict[str, Any]],
    stage: Optional[Dict[str, Any]],
    runtime: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge variable layers per spec precedence: ``runtime > stage > outer``.

    Model-level defaults are folded into ``outer`` by the caller before
    invoking this helper.
    """
    return {**(outer or {}), **(stage or {}), **(runtime or {})}


def _apply_placeholder_fill(
    query: SlayerQuery, effective: Dict[str, Any]
) -> Dict[str, Any]:
    """Add ``{var: '0'}`` for any unresolved ``{var}`` placeholder in
    ``query.filters`` so save-time dry-run SQL generation can proceed even
    when a runtime variable has no default.

    Existing values in ``effective`` are preserved.
    """
    placeholders = extract_placeholder_names(query)
    missing = {p: _PLACEHOLDER_FILL_VALUE for p in placeholders if p not in effective}
    if not missing:
        return effective
    return {**missing, **effective}


def _build_explain_sql(dialect: str, sql: str) -> str:
    """Build a dialect-appropriate EXPLAIN statement."""
    prefix = _EXPLAIN_PREFIX.get(dialect)
    if prefix is None:
        raise ValueError(
            f"EXPLAIN is not supported for dialect '{dialect}'. Use dry_run=True to inspect the generated SQL instead."
        )
    suffix = _EXPLAIN_POSTFIX.get(dialect, "")
    return f"{prefix} {sql}{suffix}"


class FieldMetadata(BaseModel):
    """Metadata for a single field in the query response."""

    label: Optional[str] = None
    format: Optional[NumberFormat] = None


class ResponseAttributes(BaseModel):
    """Field metadata for a query response, split by type."""

    dimensions: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)
    measures: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)

    def get(self, column: str) -> Optional[FieldMetadata]:
        """Look up metadata for a column across both dicts."""
        return self.dimensions.get(column) or self.measures.get(column)


class SlayerResponse(BaseModel):
    """Response from a SLayer query."""

    data: List[Dict[str, Any]]
    columns: List[str] = PydanticField(default_factory=list)
    sql: Optional[str] = None
    attributes: ResponseAttributes = PydanticField(default_factory=ResponseAttributes)

    @model_validator(mode="after")
    def _populate_columns(self) -> "SlayerResponse":
        if not self.columns and self.data:
            self.columns = list(self.data[0].keys())
        return self

    @property
    def row_count(self) -> int:
        return len(self.data)

    def _format_value(self, column: str, value: Any) -> str:
        """Format a single cell value using column format metadata if available."""
        if value is None:
            return ""
        fm = self.attributes.get(column)
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

    # sum, min, max, first, last: inherit from source column's format
    source_col = model.get_column(measure_name)
    if source_col and source_col.format:
        return source_col.format

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

    def _get_join_target_resolving(self) -> set:
        """Return the per-task in-flight join-target name set, allocating one
        on first access in this asyncio context. See ``_join_target_resolving_var``.
        """
        s = _join_target_resolving_var.get()
        if s is None:
            s = set()
            _join_target_resolving_var.set(s)
        return s

    async def execute(  # NOSONAR S3776 — public dispatch over str/dict/list/SlayerQuery; splitting hides the input-shape contract
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        runtime_kwarg = variables or {}

        # Run-by-name dispatch: ``execute("model_name", variables=...)`` runs
        # the backing query of a query-backed model. ``dry_run``/``explain``
        # apply only to this overload — for SlayerQuery/dict/list inputs, set
        # those flags on the SlayerQuery itself.
        if isinstance(query, str):
            return await self._execute_by_name(
                name=query,
                runtime_kwarg=runtime_kwarg,
                dry_run=dry_run,
                explain=explain,
            )
        if dry_run or explain:
            raise ValueError(
                "dry_run/explain kwargs are only valid for run-by-name "
                "execute(str, ...); set them on the SlayerQuery itself "
                "for query-object execution."
            )

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

        # Merge ``variables=`` kwarg into query.variables so filter
        # substitution and downstream resolution see the merged set.
        # ``runtime_kwarg`` always wins (per spec precedence).
        if runtime_kwarg:
            merged_top = {**(query.variables or {}), **runtime_kwarg}
            if merged_top != (query.variables or {}):
                query = query.model_copy(update={"variables": merged_top})

        return await self._execute_pipeline(
            query=query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
        )

    async def _execute_by_name(
        self,
        name: str,
        runtime_kwarg: Dict[str, Any],
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        """Run the backing query of a query-backed model by name.

        ``dry_run``/``explain`` from the caller take precedence over any flag
        baked into the stored final stage — required so REST/MCP/CLI run-by-name
        callers can request plan-only without mutating the stored model.
        """
        model = await self.storage.get_model(name)
        if model is None:
            raise ValueError(f"Model '{name}' not found")
        if not model.source_queries:
            raise ValueError(
                f"Model '{name}' is not query-backed; pass a SlayerQuery "
                f"with source_model='{name}'."
            )

        stages = list(model.source_queries)
        main_query = stages[-1]
        named_queries: Dict[str, SlayerQuery] = {}
        for q in stages[:-1]:
            if q.name:
                if q.name in named_queries:
                    raise ValueError(
                        f"Duplicate query name '{q.name}' in source_queries "
                        f"of model '{name}'"
                    )
                named_queries[q.name] = q

        # Merge precedence at the run-by-name entry point:
        # ``runtime_kwarg > stage > model_defaults``. There's no enclosing
        # outer query for direct execution, so ``model.query_variables`` acts
        # as the lowest layer.
        merged = _merge_query_variables(
            outer=model.query_variables,
            stage=main_query.variables,
            runtime=runtime_kwarg,
        )
        updates: Dict[str, Any] = {}
        if merged != (main_query.variables or {}):
            updates["variables"] = merged
        # Caller-supplied dry_run / explain wins over any stage-baked value
        # (only set when truthy so we don't clobber an explicit stage flag with False).
        if dry_run:
            updates["dry_run"] = True
        if explain:
            updates["explain"] = True
        if updates:
            main_query = main_query.model_copy(update=updates)

        response = await self._execute_pipeline(
            query=main_query,
            named_queries=named_queries,
            runtime_kwarg=runtime_kwarg,
        )

        # Refresh the model's cache (columns + backing_query_sql). The
        # pipeline above does not call ``_resolve_model_inner`` for ``model``
        # itself (we go directly through its final stage), so cache refresh
        # has to be wired here. Cost: one extra enrich+SQL-gen pass per
        # run-by-name, in-process, no DB hit.
        # Use the ORIGINAL final stage (not the runtime-merged ``main_query``)
        # and pass ``runtime_kwarg=None`` + ``dry_run_placeholders=True`` so
        # the cached SQL stays canonical (model-defaults + placeholder fill,
        # never per-request values).
        try:
            virtual = await self._query_as_model(
                inner_query=stages[-1],
                named_queries=named_queries,
                override_name=name,
                _resolving=set(),
                outer_vars=dict(model.query_variables),
                runtime_kwarg=None,
                dry_run_placeholders=True,
            )
            await self._refresh_cache_after_resolution(model, virtual)
        except Exception:
            # Pipeline already succeeded; don't fail the user's call on a
            # cache-side error. Log and move on.
            # Sanitize for log injection (S5145): name comes from the public
            # run-by-name API so could contain CR/LF.
            safe_name = name.replace("\r", "\\r").replace("\n", "\\n")
            logger.warning(
                "Cache refresh failed for query-backed model '%s'; "
                "pipeline result still returned.",
                safe_name,
                exc_info=True,
            )

        return response

    async def _execute_pipeline(  # NOSONAR S3776 — linear pipeline (resolve→enrich→generate→execute); breaking it up obscures the order of operations
        self,
        query: SlayerQuery,
        named_queries: Dict[str, SlayerQuery],
        runtime_kwarg: Dict[str, Any],
    ) -> SlayerResponse:
        """Shared pipeline used by both ``execute()`` and ``_execute_by_name()``.

        Assumes ``query.variables`` already reflects the resolved variable
        context for the top of the chain (kwarg merged in by the caller).
        """
        # Pre-processing: strip redundant source model name prefixes from all references
        query = query.strip_source_model_prefix()
        named_queries = {
            name: q.strip_source_model_prefix()
            for name, q in named_queries.items()
        }

        # Preprocessing
        if query.whole_periods_only:
            query = query.snap_to_whole_periods()

        # Resolve model from query.source_model (str, SlayerModel, or ModelExtension).
        # Pass query.variables as the outer-vars context for any nested
        # query-backed model resolution; runtime_kwarg threads through unchanged.
        resolving: set = set()
        model = await self._resolve_query_model(
            query_model=query.source_model,
            named_queries=named_queries,
            _resolving=resolving,
            outer_vars=query.variables,
            runtime_kwarg=runtime_kwarg,
        )

        # Auto-correct: move bare field names to dimensions if they match
        query = await self._auto_move_fields_to_dimensions(query, model, named_queries)

        datasource = await self._resolve_datasource(model=model)

        # Enrich: SlayerQuery + model → EnrichedQuery
        enriched = await self._enrich(query=query, model=model, named_queries=named_queries)

        # Generate SQL from EnrichedQuery
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        sql = generator.generate(enriched=enriched)
        logger.debug("Generated SQL:\n%s", sql)

        # Collect field metadata from enriched query, split by type
        dim_meta: Dict[str, FieldMetadata] = {}
        measure_meta: Dict[str, FieldMetadata] = {}
        for d in enriched.dimensions:
            if d.label or d.format:
                dim_meta[d.alias] = FieldMetadata(label=d.label, format=d.format)
        for td in enriched.time_dimensions:
            if td.label:
                dim_meta[td.alias] = FieldMetadata(label=td.label)
        for m in enriched.measures:
            measure_fmt = _infer_aggregated_format(
                model=model,
                measure_name=m.source_measure_name or m.name,
                aggregation=m.aggregation,
            )
            if m.label or measure_fmt:
                measure_meta[m.alias] = FieldMetadata(label=m.label, format=measure_fmt)
        for e in enriched.expressions:
            measure_meta[e.alias] = FieldMetadata(
                label=e.label,
                format=NumberFormat(type=NumberFormatType.FLOAT),
            )
        for t in enriched.transforms:
            measure_meta[t.alias] = FieldMetadata(
                label=t.label,
                format=NumberFormat(type=NumberFormatType.FLOAT),
            )
        for cm in enriched.cross_model_measures:
            if cm.label or cm.format:
                measure_meta[cm.alias] = FieldMetadata(label=cm.label, format=cm.format)
        attributes = ResponseAttributes(dimensions=dim_meta, measures=measure_meta)

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
            return SlayerResponse(data=[], columns=expected_columns, sql=sql, attributes=attributes)

        # Execute — reuse SQL client (and its connection pool) per datasource
        ds_key = datasource.get_connection_string()
        if ds_key not in self._sql_clients:
            self._sql_clients[ds_key] = SlayerSQLClient(datasource=datasource)
        client = self._sql_clients[ds_key]

        # explain: run dialect-appropriate EXPLAIN on the query
        if query.explain:
            explain_sql = _build_explain_sql(dialect=dialect, sql=sql)
            rows = await client.execute(sql=explain_sql)
            return SlayerResponse(data=rows, sql=sql, attributes=attributes)

        rows = await client.execute(sql=sql)
        columns = expected_columns if not rows else []  # fallback for empty results; [] triggers auto-derive
        return SlayerResponse(data=rows, columns=columns, sql=sql, attributes=attributes)

    def _build_type_probe_query(self, model: SlayerModel) -> SlayerQuery:
        """Build a SlayerQuery for type-probing all of a model's columns.

        Picks an aggregation per column from its effective allowed set:
        explicit ``allowed_aggregations`` if present, otherwise the type
        default. Prefers ``max`` (preserves the column's SQL type for orderable
        types) and falls back to the first allowed aggregation otherwise.
        Skips primary-key columns (they're identifiers, not values to probe).
        """
        measures: List[ModelMeasure] = []
        for c in model.columns:
            if c.hidden or c.primary_key:
                continue
            if c.allowed_aggregations is not None:
                allowed = list(c.allowed_aggregations)
            else:
                allowed = sorted(DEFAULT_AGGREGATIONS_BY_TYPE.get(c.type, frozenset()))
            if not allowed:
                continue
            agg = "max" if "max" in allowed else allowed[0]
            measures.append(ModelMeasure(formula=f"{c.name}:{agg}"))
        return SlayerQuery(source_model=model.name, measures=measures)

    async def get_column_types(self, model_name: str) -> Dict[str, str]:
        """Infer column types for a model's columns via a type-probe query.

        Builds a real query through the engine's enrich+generate pipeline
        so cross-model measures (with JOINs) are resolved correctly.

        Returns {column_name: type_category} where type_category is
        "number", "string", "time", or "boolean".
        """
        model = await self.storage.get_model(model_name)
        if model is None:
            return {}

        # For query-backed models, expand FIRST so the resolved virtual model
        # (with refreshed ``data_source`` from its final stage AND with
        # ``columns`` derived from the inner query) drives both the
        # datasource selection and the probeable-columns check. Otherwise a
        # stale or blank stored ``model.data_source``/``columns`` would point
        # us at the wrong backend or short-circuit on an empty column list.
        if model.source_queries:
            try:
                model = await self._resolve_model(model_name=model_name)
            except Exception:
                logger.warning(
                    "get_column_types: failed to resolve query-backed model '%s'",
                    model_name,
                )
                return {}

        probeable = [c for c in model.columns if not c.hidden and not c.primary_key]
        if not probeable:
            return {}

        try:
            datasource = await self._resolve_datasource(model=model)
        except ValueError:
            return {}

        ds_key = datasource.get_connection_string()
        if ds_key not in self._sql_clients:
            self._sql_clients[ds_key] = SlayerSQLClient(datasource=datasource)
        client = self._sql_clients[ds_key]

        probe_query = self._build_type_probe_query(model=model)
        try:
            enriched = await self._enrich(query=probe_query, model=model)
            dialect = self._dialect_for_type(datasource.type)
            generator = SQLGenerator(dialect=dialect)
            sql = generator.generate(enriched=enriched)
        except Exception:
            logger.warning("get_column_types enrich/generate failed for model '%s'", model_name)
            return {}

        try:
            raw_types = await client.get_column_types(sql=sql)
        except Exception:
            logger.warning("get_column_types probe failed for model '%s'", model_name)
            return {}

        # Map qualified aliases (e.g., "orders.revenue_max") back to bare measure names
        result: Dict[str, str] = {}
        for em in enriched.measures:
            if em.alias in raw_types:
                result[em.source_measure_name or em.name] = raw_types[em.alias]
        return result

    def execute_sync(
        self,
        query: "SlayerQuery | dict | list[SlayerQuery | dict] | str",
        variables: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        """Synchronous wrapper for execute(). For CLI, notebooks, and scripts."""
        from slayer.async_utils import run_sync

        return run_sync(self.execute(query, variables=variables, dry_run=dry_run, explain=explain))

    def create_model_from_query_sync(
        self,
        query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]",
        name: str,
        description: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        save: bool = True,
    ) -> SlayerModel:
        """Synchronous wrapper for create_model_from_query()."""
        from slayer.async_utils import run_sync

        return run_sync(
            self.create_model_from_query(
                query=query,
                name=name,
                description=description,
                variables=variables,
                save=save,
            )
        )

    async def _expand_query_backed_model(
        self,
        model: SlayerModel,
        outer_vars: Optional[Dict[str, Any]],
        runtime_kwarg: Optional[Dict[str, Any]],
        dry_run_placeholders: bool,
        _resolving: Optional[set],
        refresh_cache: bool,
    ) -> SlayerModel:
        """If ``model`` is query-backed, expand its ``source_queries`` into a
        virtual model (with rendered SQL). Otherwise return ``model`` unchanged.

        ``refresh_cache`` controls whether the stored model's cache is updated
        from the freshly-resolved virtual — only valid for storage-backed
        models (``model.name`` matches a stored entry); pass False for inline
        models that aren't persisted. The cache refresh always runs a SECOND,
        canonical render (no runtime variables, placeholder fill) so per-request
        values never end up in ``backing_query_sql``.
        """
        if not model.source_queries:
            return model
        stages = list(model.source_queries)
        merged_outer = {**model.query_variables, **(outer_vars or {})}
        named_q = {q.name: q for q in stages[:-1] if q.name}
        virtual = await self._query_as_model(
            inner_query=stages[-1],
            named_queries=named_q,
            override_name=model.name,
            _resolving=_resolving,
            outer_vars=merged_outer,
            runtime_kwarg=runtime_kwarg,
            dry_run_placeholders=dry_run_placeholders,
        )
        if refresh_cache and not dry_run_placeholders:
            # Render a canonical version for the cache whenever request-scoped
            # variables affected the expansion — that's both runtime_kwarg AND
            # outer_vars beyond model.query_variables (e.g. when this model is
            # the source_model of an outer SlayerQuery that carries its own
            # variables). Otherwise the virtual already IS canonical.
            cache_depends_on_request_vars = (
                bool(runtime_kwarg)
                or merged_outer != dict(model.query_variables)
            )
            if cache_depends_on_request_vars:
                try:
                    canonical = await self._query_as_model(
                        inner_query=stages[-1],
                        named_queries=named_q,
                        override_name=model.name,
                        _resolving=set(),  # fresh — different render
                        outer_vars=dict(model.query_variables),
                        runtime_kwarg=None,
                        dry_run_placeholders=True,
                    )
                except Exception:
                    canonical = None
                if canonical is not None:
                    await self._refresh_cache_after_resolution(model, canonical)
            else:
                await self._refresh_cache_after_resolution(model, virtual)
        return virtual

    async def _resolve_query_model(  # NOSONAR S3776 — type-dispatch on str/SlayerModel/ModelExtension/dict; flat is clearer than per-shape helpers here
        self,
        query_model,
        named_queries: dict = None,
        _resolving: set = None,
        outer_vars: Optional[Dict[str, Any]] = None,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        dry_run_placeholders: bool = False,
    ) -> SlayerModel:
        """Resolve query.source_model — handles str, SlayerModel, and ModelExtension."""
        from slayer.core.query import ModelExtension

        named_queries = named_queries or {}

        if isinstance(query_model, str):
            return await self._resolve_model(
                model_name=query_model,
                named_queries=named_queries,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
            )
        elif isinstance(query_model, SlayerModel):
            # Inline SlayerModel may itself be query-backed; expand its
            # source_queries the same way storage-backed models do, otherwise
            # the outer enrichment can't see the virtual columns.
            return await self._expand_query_backed_model(
                model=query_model,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
                _resolving=_resolving,
                refresh_cache=False,  # inline model isn't stored
            )
        elif isinstance(query_model, ModelExtension):
            base = await self._resolve_model(
                model_name=query_model.source_name,
                named_queries=named_queries,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
            )
            # Extend the base model with extra columns/measures/joins
            from slayer.core.models import ModelJoin

            extra_cols = [
                Column.model_validate(c) if isinstance(c, dict) else c for c in (query_model.columns or [])
            ]
            extra_measures = [
                ModelMeasure.model_validate(m) if isinstance(m, dict) else m for m in (query_model.measures or [])
            ]
            extra_joins = [ModelJoin.model_validate(j) if isinstance(j, dict) else j for j in (query_model.joins or [])]
            return base.model_copy(
                update={
                    "columns": list(base.columns) + extra_cols,
                    "measures": list(base.measures) + extra_measures,
                    "joins": list(base.joins) + extra_joins,
                }
            )
        elif isinstance(query_model, dict):
            # Dict — could be ModelExtension or SlayerModel
            if "source_name" in query_model:
                ext = ModelExtension.model_validate(query_model)
                return await self._resolve_query_model(
                    ext,
                    named_queries,
                    _resolving=_resolving,
                    outer_vars=outer_vars,
                    runtime_kwarg=runtime_kwarg,
                    dry_run_placeholders=dry_run_placeholders,
                )
            else:
                model = SlayerModel.model_validate(query_model)
                return await self._expand_query_backed_model(
                    model=model,
                    outer_vars=outer_vars,
                    runtime_kwarg=runtime_kwarg,
                    dry_run_placeholders=dry_run_placeholders,
                    _resolving=_resolving,
                    refresh_cache=False,
                )
        else:
            raise ValueError(f"Invalid query.source_model type: {type(query_model)}")

    async def _resolve_model(
        self,
        model_name: str,
        named_queries: dict[str, SlayerQuery] = None,
        _resolving: set = None,
        outer_vars: Optional[Dict[str, Any]] = None,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        dry_run_placeholders: bool = False,
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
            return await self._resolve_model_inner(
                model_name,
                named_queries,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
            )
        finally:
            _resolving.discard(model_name)

    async def _resolve_model_inner(
        self,
        model_name: str,
        named_queries: dict[str, SlayerQuery],
        _resolving: set = None,
        outer_vars: Optional[Dict[str, Any]] = None,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        dry_run_placeholders: bool = False,
    ) -> SlayerModel:
        # Named query overrides stored model
        if model_name in named_queries:
            return await self._query_as_model(
                inner_query=named_queries[model_name],
                named_queries=named_queries,
                _resolving=_resolving,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
            )

        model = await self.storage.get_model(model_name)
        if model is None:
            raise ValueError(f"Model '{model_name}' not found")

        # If model has source_queries, re-enrich from stored queries and
        # refresh the persisted cache. Model-level defaults are folded into
        # outer_vars by the helper (precedence: runtime > stage > outer >
        # model_defaults).
        return await self._expand_query_backed_model(
            model=model,
            outer_vars=outer_vars,
            runtime_kwarg=runtime_kwarg,
            dry_run_placeholders=dry_run_placeholders,
            _resolving=_resolving,
            refresh_cache=True,
        )

    async def create_model_from_query(
        self,
        query: "SlayerQuery | list[SlayerQuery] | dict | list[dict]",
        name: str,
        description: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        save: bool = True,
    ) -> SlayerModel:
        """Create a query-backed model from a query (or list of stages).

        The returned model has ``source_queries`` populated, plus ``columns``
        and ``backing_query_sql`` populated from a save-time dry-run of the
        final stage (with literal ``0`` substituted for any unresolved
        ``{var}`` placeholder). ``query_variables`` is set from the
        ``variables=`` kwarg.

        Args:
            query: One ``SlayerQuery`` or a list of stages (last is the
                final/main query). Dicts are accepted and validated.
            name: Name for the new model.
            description: Optional model description.
            variables: Default values for ``{var}`` placeholders in the
                stages — saved as ``model.query_variables``.
            save: If True (default), persist to storage immediately.
        """
        raw = query if isinstance(query, list) else [query]
        stages = [
            SlayerQuery.model_validate(q) if isinstance(q, dict) else q for q in raw
        ]
        # Construct the SlayerModel — Pydantic validators enforce source-mode
        # exclusivity and stage-name rules.
        model = SlayerModel(
            name=name,
            description=description,
            source_queries=stages,
            query_variables=variables or {},
        )
        if save:
            return await self.save_model(model)
        # save=False: still validate and populate the cache so the caller
        # can use the returned model directly.
        return await self._validate_and_populate_cache(model)

    async def save_model(self, model: SlayerModel) -> SlayerModel:
        """Persist a SlayerModel through the engine.

        For query-backed models, rejects user-supplied cache fields and runs
        save-time dry-run validation before populating the cache. For non-
        query-backed models, persists as-is.
        """
        if model.source_queries:
            if model.columns:
                raise ValueError(
                    f"Model '{model.name}' is query-backed; columns are "
                    f"auto-generated and must not be supplied "
                    f"(got {len(model.columns)} columns)."
                )
            if model.backing_query_sql is not None:
                raise ValueError(
                    f"Model '{model.name}' is query-backed; backing_query_sql "
                    f"is auto-managed and must not be supplied."
                )
            model = await self._validate_and_populate_cache(model)
        await self.storage.save_model(model)
        return model

    async def _validate_and_populate_cache(self, model: SlayerModel) -> SlayerModel:
        """Run save-time dry-run validation on a query-backed model and
        return a copy with ``columns``, ``backing_query_sql``, and
        ``data_source`` populated from the virtual model.
        """
        stages = list(model.source_queries or [])
        if not stages:
            return model
        virtual = await self._query_as_model(
            inner_query=stages[-1],
            named_queries={q.name: q for q in stages[:-1] if q.name},
            override_name=model.name,
            _resolving=set(),
            outer_vars=dict(model.query_variables),
            runtime_kwarg={},
            dry_run_placeholders=True,
        )
        return model.model_copy(update={
            "columns": list(virtual.columns),
            "backing_query_sql": virtual.sql,
            # data_source is refreshed from the resolved virtual model: the
            # backing query may now resolve through a different upstream
            # datasource than the caller passed (or the previous save), and
            # downstream callers like get_column_types() open the SQL client
            # from the persisted data_source BEFORE expanding the model.
            "data_source": virtual.data_source,
        })

    async def _refresh_cache_after_resolution(
        self,
        stored_model: SlayerModel,
        virtual: SlayerModel,
    ) -> None:
        """Write-if-changed update of a query-backed model's cache fields
        using a freshly-resolved virtual model. No-op when nothing changed.
        """
        if (
            list(stored_model.columns) == list(virtual.columns)
            and stored_model.backing_query_sql == virtual.sql
            and stored_model.data_source == virtual.data_source
        ):
            return
        updated = stored_model.model_copy(update={
            "columns": list(virtual.columns),
            "backing_query_sql": virtual.sql,
            "data_source": virtual.data_source,
        })
        await self.storage.save_model(updated)

    async def _enrich(  # NOSONAR S3776 — orchestrates resolve-callback closures + cross-model post-processing; splitting into helpers obscures the closure variables threaded through enrich_query
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
                # Named-query stages inherit the variable context of the query
                # being enriched (its filter substitutions) so nested query-
                # backed model resolution works through joins as well.
                target = await self._query_as_model(
                    inner_query=nq[target_model_name],
                    named_queries=nq,
                    outer_vars=query.variables,
                )
            elif self.storage:
                target = await self.storage.get_model(target_model_name)
                if target and target.source_queries:
                    target = await self._render_query_backed_join_target(
                        target=target,
                        outer_query_variables=query.variables,
                    )
            else:
                target = None
            if target and target.sql_table:
                return target.sql_table, target
            elif target and target.sql:
                return f"({target.sql})", target
            return None

        enriched = await enrich_query(
            query=query,
            model=model,
            named_queries=named_queries,
            resolve_dimension_via_joins=self._resolve_dimension_via_joins,
            resolve_cross_model_measure=self._resolve_cross_model_measure,
            resolve_join_target=_resolve_join_target,
        )

        # Post-process: build re-rooted enriched queries for cross-model measures
        for cm in enriched.cross_model_measures:
            cm.rerooted_enriched = await self._build_rerooted_enriched(
                cm=cm, query=query, model=model,
                named_queries=named_queries or {},
            )

        return enriched

    async def _render_query_backed_join_target(
        self,
        target: SlayerModel,
        outer_query_variables: Optional[Dict[str, Any]],
    ) -> SlayerModel:
        """Resolve a query-backed model used as a JOIN target.

        Threads the enclosing query's variables into the target's stage filter
        substitution so a target with ``filters=["amount > {threshold}"]`` sees
        the runtime value, not the cached/default fill.

        Recursion guard: ``self._join_target_resolving`` blocks re-entry on the
        same target name. The call stack crosses ``_enrich`` invocations
        (target's source_queries → target's own joins → _resolve_join_target
        again), so this guard lives on the engine instance, not on a closure.
        Re-entry returns the cached SQL if available, else returns the raw
        target unchanged so enrichment fails with a clear "no sql" error
        instead of looping.
        """
        resolving = self._get_join_target_resolving()
        if target.name in resolving:
            if target.backing_query_sql:
                return target.model_copy(update={"sql": target.backing_query_sql})
            return target
        # When the enclosing query has no variables AND a canonical cache
        # exists, prefer the cached SQL (avoids the second render).
        if not outer_query_variables and target.backing_query_sql:
            return target.model_copy(update={"sql": target.backing_query_sql})
        # Otherwise render fresh with merged variables (target defaults +
        # enclosing query's vars; enclosing wins).
        stages = list(target.source_queries or [])
        if not stages:
            return target
        merged = {**dict(target.query_variables), **(outer_query_variables or {})}
        resolving.add(target.name)
        try:
            return await self._query_as_model(
                inner_query=stages[-1],
                named_queries={q.name: q for q in stages[:-1] if q.name},
                override_name=target.name,
                outer_vars=merged,
                runtime_kwarg=outer_query_variables or None,
            )
        finally:
            resolving.discard(target.name)

    async def _query_as_model(  # NOSONAR S3776 — variable-precedence + enrich + SQL-gen + virtual-model assembly is a single conceptual unit
        self,
        inner_query: SlayerQuery,
        named_queries: dict[str, SlayerQuery] = None,
        override_name: str = None,
        _resolving: set = None,
        outer_vars: Optional[Dict[str, Any]] = None,
        runtime_kwarg: Optional[Dict[str, Any]] = None,
        dry_run_placeholders: bool = False,
    ) -> SlayerModel:
        """Build a virtual SlayerModel from a nested query's result.

        Enriches and generates SQL for the inner query, then creates a model
        whose `sql` is the inner query's SQL and whose dimensions/measures
        are derived from the inner query's enriched columns.

        ``outer_vars``, ``runtime_kwarg``, and ``dry_run_placeholders`` thread
        the variable-precedence machinery through nested query-backed model
        resolution; see ``_merge_query_variables`` and
        ``_apply_placeholder_fill``.
        """
        named_queries = named_queries or {}

        # Compute effective variables for this stage and stamp them onto a
        # copy of the inner query so substitution at enrichment time uses
        # the merged set.
        effective = _merge_query_variables(
            outer=outer_vars,
            stage=inner_query.variables,
            runtime=runtime_kwarg,
        )
        if dry_run_placeholders:
            effective = _apply_placeholder_fill(inner_query, effective)
        if effective != (inner_query.variables or {}):
            inner_query = inner_query.model_copy(update={"variables": effective})

        # Resolve the inner model (handles str, SlayerModel, ModelExtension).
        # Pass ``effective`` as the next layer's outer_vars so nested
        # query-backed models inherit this stage's resolved context.
        inner_model = await self._resolve_query_model(
            query_model=inner_query.source_model,
            named_queries=named_queries,
            _resolving=_resolving,
            outer_vars=effective,
            runtime_kwarg=runtime_kwarg,
            dry_run_placeholders=dry_run_placeholders,
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

        # Build lookups for labels/descriptions from the source model.
        # In v2 there is no dim/measure split — every column carries both
        # potential roles, so a single map per attribute is sufficient.
        source_label = {c.name: c.label for c in inner_model.columns if c.label}
        source_desc = {c.name: c.description for c in inner_model.columns if c.description}

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

        # (inner_alias, short_name, data_type, label, description, format)
        column_map = []
        for d in enriched.dimensions:
            short = _alias_to_short(d.alias)
            label = d.label or source_label.get(d.name)
            desc = source_desc.get(d.name)
            column_map.append((d.alias, short, d.type, label, desc, d.format))
        for td in enriched.time_dimensions:
            short = _alias_to_short(td.alias)
            label = td.label or source_label.get(td.name)
            desc = source_desc.get(td.name)
            column_map.append((td.alias, short, DataType.TIMESTAMP, label, desc, None))
        for m in enriched.measures:
            src_name = m.source_measure_name or m.name
            label = m.label or source_label.get(src_name)
            desc = source_desc.get(src_name)
            fmt = _infer_aggregated_format(
                model=inner_model,
                measure_name=src_name,
                aggregation=m.aggregation,
            )
            column_map.append((m.alias, m.name, DataType.NUMBER, label, desc, fmt))
        for t in enriched.transforms:
            column_map.append(
                (t.alias, t.name, DataType.NUMBER, t.label, None, NumberFormat(type=NumberFormatType.FLOAT))
            )
        for e in enriched.expressions:
            column_map.append(
                (e.alias, e.name, DataType.NUMBER, e.label, None, NumberFormat(type=NumberFormatType.FLOAT))
            )
        for cm in enriched.cross_model_measures:
            short = _alias_to_short(cm.alias)
            column_map.append((cm.alias, short, DataType.NUMBER, cm.label, None, cm.format))

        # Wrap inner SQL: SELECT "orders.id" AS id, "orders.count" AS count, ... FROM (inner) AS _inner
        rename_parts = [f'"{alias}" AS {short}' for alias, short, _, _, _, _ in column_map]
        wrapped_sql = f"SELECT {', '.join(rename_parts)} FROM ({inner_sql}) AS _inner"

        # One Column per result column — each is potentially both a dimension
        # (group-by) or measure (with colon-aggregation) at query time.
        cols: List[Column] = []
        for _, short, dtype, label, desc, fmt in column_map:
            cols.append(Column(name=short, sql=short, type=dtype, label=label, description=desc, format=fmt))

        return SlayerModel(
            name=virtual_name,
            sql=wrapped_sql,
            data_source=inner_model.data_source,
            columns=cols,
            default_time_dimension=inner_model.default_time_dimension,
        )

    async def _resolve_dimension_via_joins(
        self,
        model: SlayerModel,
        parts: list[str],
        named_queries: dict = None,
    ) -> "Column | None":
        """Walk the join graph to resolve a multi-hop column reference.

        For "customers.regions.name", walks: model → customers → regions,
        then looks up "name" on the regions model.
        """
        current_model = model
        visited = {model.name}
        # Walk intermediate models (all parts except the last, which is the column name)
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

        # Look up the final column on the terminal model
        return current_model.get_column(parts[-1])

    async def _auto_move_fields_to_dimensions(
        self,
        query: SlayerQuery,
        model: SlayerModel,
        named_queries: dict,
    ) -> SlayerQuery:
        """Move bare (no-colon) measure-formula entries to dimensions when they
        name a column that isn't a (named) ModelMeasure formula.

        LLMs frequently place column names in ``measures`` instead of
        ``dimensions``. When an entry has no colon (no aggregation) and
        resolves as a column but NOT as a model-level ModelMeasure formula,
        silently move it to ``dimensions`` with a warning.
        """
        if not query.measures:
            return query

        kept: List = []
        extra_dims = list(query.dimensions or [])
        moved = False

        for f in query.measures:
            formula = f.formula.strip()
            # Only consider bare names (no colon, no operators, no parens)
            if ":" not in formula and not any(c in formula for c in "+-*/()"):
                if "." not in formula:
                    # Local reference
                    is_col = model.get_column(formula) is not None
                    is_named_measure = model.get_measure(formula) is not None
                    if is_col and not is_named_measure:
                        logger.warning(
                            "Auto-moved '%s' from measures to dimensions (not a named measure formula)",
                            formula,
                        )
                        extra_dims.append(ColumnRef(name=formula))
                        moved = True
                        continue
                else:
                    # Cross-model reference — walk the full join path
                    parts = formula.split(".")
                    try:
                        col_def = await self._resolve_dimension_via_joins(
                            model=model, parts=parts, named_queries=named_queries,
                        )
                    except ValueError:
                        col_def = None  # Circular join — leave in measures
                    if col_def is not None:
                        # parts[-2] is the terminal model containing the column at parts[-1]
                        terminal_model_name = parts[-2]
                        try:
                            terminal_model = await self._resolve_model(
                                model_name=terminal_model_name,
                                named_queries=named_queries or {},
                            )
                        except ValueError:
                            terminal_model = None
                        is_named_measure = (
                            terminal_model.get_measure(parts[-1]) is not None
                            if terminal_model else False
                        )
                        if not is_named_measure:
                            logger.warning(
                                "Auto-moved '%s' from measures to dimensions (not a named measure formula)",
                                formula,
                            )
                            extra_dims.append(ColumnRef(name=formula))
                            moved = True
                            continue
            kept.append(f)

        if not moved:
            return query
        return query.model_copy(update={"measures": kept or None, "dimensions": extra_dims})

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

        Supports multi-hop paths: 'claim_coverage.claim_amount.total_claim_amount'
        walks the join graph hop-by-hop to reach the final model.

        Looks up the join from the source model, loads the target model
        (checking named queries first), finds shared dimensions, and returns
        a CrossModelMeasure for SQL generation.
        """
        parts = spec_name.split(".")
        if len(parts) < 2:
            raise ValueError(f"Invalid cross-model measure reference: '{spec_name}'")
        measure_name = parts[-1]
        hop_names = parts[:-1]  # e.g. ["claim_coverage", "claim_amount"]

        # Walk the join chain to find the final target model
        current_model = model
        first_join = None
        for i, hop_name in enumerate(hop_names):
            join = None
            for j in current_model.joins:
                if j.target_model == hop_name:
                    join = j
                    break
            if join is None:
                raise ValueError(
                    f"Model '{current_model.name}' has no join to '{hop_name}'. "
                    f"Available joins: {[j.target_model for j in current_model.joins]}"
                )
            if i == 0:
                first_join = join
            current_model = await self._resolve_model(
                model_name=hop_name,
                named_queries=named_queries or {},
            )

        target_model_name = hop_names[-1]
        target_model = current_model
        join = first_join  # For join_pairs: source model → first hop

        # Find the column in the target model
        if measure_name == "*":
            measure_def = Column(name="*", sql=None)
        else:
            from slayer.core.enums import NUMERIC_ONLY_AGGREGATIONS

            col_def = target_model.get_column(measure_name)
            if col_def is None:
                raise ValueError(
                    f"Column '{measure_name}' not found in model '{target_model_name}'. "
                    f"Available columns: {[c.name for c in target_model.columns]}"
                )
            if (
                aggregation_name
                and aggregation_name in NUMERIC_ONLY_AGGREGATIONS
                and str(col_def.type) == "string"
            ):
                raise ValueError(
                    f"Aggregation '{aggregation_name}' is not applicable to "
                    f"string column '{measure_name}' in model '{target_model_name}'."
                )
            measure_def = col_def

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

        hop_path = ".".join(hop_names)
        alias = f"{query_model_name}.{hop_path}.{canonical}"
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
            join_type=str(join.join_type),
            shared_dimensions=shared_dims,
            shared_time_dimensions=shared_time_dims,
            source_model_name=model.name,
            source_sql_table=model.sql_table,
            source_sql=model.sql,
            label=label,
            format=cm_format,
        )

    async def _build_rerooted_enriched(
        self,
        cm: CrossModelMeasure,
        query: SlayerQuery,
        model: SlayerModel,
        named_queries: dict,
    ) -> EnrichedQuery:
        """Build a re-rooted EnrichedQuery for a cross-model measure.

        Instead of the minimal source→target CTE, this constructs a full query
        with the target model as source. All of the target model's joins are
        available, so filters on related tables (e.g., premium.has_premium)
        are applied correctly.

        Dimensions and filters referencing models not reachable from the
        target are dropped.
        """
        import re

        from slayer.core.formula import parse_filter

        target_model = await self._resolve_model(
            model_name=cm.target_model_name,
            named_queries=named_queries,
        )

        source_model_name = model.name
        target_model_name = cm.target_model_name

        # --- Build re-rooted field (measure becomes local) ---
        measure_name = cm.measure.source_measure_name or cm.measure.name
        aggregation = cm.measure.aggregation
        if cm.measure.agg_kwargs:
            kwargs_str = ", ".join(f"{k}={v}" for k, v in cm.measure.agg_kwargs.items())
            field_formula = f"{measure_name}:{aggregation}({kwargs_str})"
        else:
            field_formula = f"{measure_name}:{aggregation}"

        # --- Remap dimensions ---
        rerooted_dims = []
        for dim in (query.dimensions or []):
            if dim.model is None:
                # Source-local dimension → cross-model from target's perspective
                rerooted_dims.append(ColumnRef(name=f"{source_model_name}.{dim.name}"))
            elif dim.model == target_model_name:
                # Dimension on target model → now local
                rerooted_dims.append(ColumnRef(name=dim.name))
            elif dim.model.startswith(target_model_name + "."):
                # Path through target → strip target prefix
                new_model = dim.model[len(target_model_name) + 1:]
                rerooted_dims.append(ColumnRef(name=f"{new_model}.{dim.name}"))
            else:
                # Other cross-model dim → keep as-is (enrichment resolves via target's joins)
                rerooted_dims.append(ColumnRef(name=dim.full_name))

        # --- Remap time dimensions ---
        rerooted_time_dims = []
        for td in (query.time_dimensions or []):
            dim_ref = td.dimension
            if dim_ref.model is None:
                new_ref = ColumnRef(name=f"{source_model_name}.{dim_ref.name}")
            elif dim_ref.model == target_model_name:
                new_ref = ColumnRef(name=dim_ref.name)
            elif dim_ref.model.startswith(target_model_name + "."):
                new_model = dim_ref.model[len(target_model_name) + 1:]
                new_ref = ColumnRef(name=f"{new_model}.{dim_ref.name}")
            else:
                new_ref = ColumnRef(name=dim_ref.full_name)
            rerooted_time_dims.append(TimeDimension(
                dimension=new_ref,
                granularity=td.granularity,
                date_range=td.date_range,
                label=td.label,
            ))

        # --- Remap filters ---
        rerooted_filters = []
        target_prefix = target_model_name + "."
        _custom_agg_names = frozenset(
            a.name for m in (model, target_model)
            for a in m.aggregations
        ) or None
        for f_str in (query.filters or []) + list(model.filters):
            remapped = f_str
            # Strip target model prefix from dotted references
            # e.g., "policy_amount.premium.has_premium = '1'" → "premium.has_premium = '1'"
            if target_prefix in remapped:
                remapped = remapped.replace(target_prefix, "")
            # For unqualified column references that are source model dimensions,
            # prepend source model name (they're now on a joined table)
            parsed = parse_filter(remapped, extra_agg_names=_custom_agg_names)
            for col in parsed.columns:
                if "." not in col:
                    src_col = model.get_column(col)
                    if src_col:
                        remapped = re.sub(
                            rf"(?<!\.)(?<!\w)\b{re.escape(col)}\b(?!\.)",
                            f"{source_model_name}.{col}",
                            remapped,
                        )
            rerooted_filters.append(remapped)

        # --- Build and enrich re-rooted query ---
        rerooted_query = SlayerQuery(
            source_model=target_model_name,
            measures=[ModelMeasure(formula=field_formula)],
            dimensions=rerooted_dims or None,
            time_dimensions=rerooted_time_dims or None,
            filters=rerooted_filters or None,
        )

        rerooted_enriched = await self._enrich(
            query=rerooted_query,
            model=target_model,
            named_queries=named_queries,
        )

        # --- Fix aliases to match main query's expectations ---
        # Dimensions: rerooted aliases are "target.source.dim", main expects "source.dim"
        main_dim_aliases = [d.alias for d in cm.shared_dimensions]
        for i, dim in enumerate(rerooted_enriched.dimensions):
            if i < len(main_dim_aliases):
                dim.alias = main_dim_aliases[i]

        main_td_aliases = [td.alias for td in cm.shared_time_dimensions]
        for i, td in enumerate(rerooted_enriched.time_dimensions):
            if i < len(main_td_aliases):
                td.alias = main_td_aliases[i]

        # Measure alias
        if rerooted_enriched.measures:
            rerooted_enriched.measures[0].alias = cm.alias

        # --- Strip unreachable dimensions and filters ---
        available_aliases = {target_model_name}
        for _, alias, _, _ in rerooted_enriched.resolved_joins:
            available_aliases.add(alias)

        rerooted_enriched.dimensions = [
            d for d in rerooted_enriched.dimensions
            if d.model_name == target_model_name or d.model_name in available_aliases
        ]
        rerooted_enriched.time_dimensions = [
            td for td in rerooted_enriched.time_dimensions
            if td.model_name == target_model_name or td.model_name in available_aliases
        ]
        rerooted_enriched.filters = [
            f for f in rerooted_enriched.filters
            if all(
                col.split(".")[0] in available_aliases or "." not in col
                for col in f.columns
            )
        ]

        return rerooted_enriched

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
