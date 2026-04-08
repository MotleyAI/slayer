"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → _enrich() → EnrichedQuery → SQLGenerator → SQL → execute
"""

import logging
from typing import Any, Dict, List, Optional

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.enriched import (
    CrossModelMeasure,
    EnrichedDimension,
    EnrichedExpression,
    EnrichedMeasure,
    EnrichedQuery,
    EnrichedTimeDimension,
    EnrichedTransform,
)
from slayer.sql.client import SlayerSQLClient
from slayer.sql.generator import SQLGenerator
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class FieldMetadata:
    """Metadata for a single field in the query response."""

    def __init__(self, label: Optional[str] = None):
        self.label = label


class SlayerResponse:
    """Response from a SLayer query."""

    def __init__(self, data: List[Dict[str, Any]], columns: Optional[List[str]] = None,
                 sql: Optional[str] = None, meta: Optional[Dict[str, "FieldMetadata"]] = None):
        self.data = data
        self.columns = columns or (list(data[0].keys()) if data else [])
        self.sql = sql
        self.meta = meta or {}  # column alias → FieldMetadata

    @property
    def row_count(self) -> int:
        return len(self.data)


class SlayerQueryEngine:
    """Central orchestrator: resolves queries via storage, generates SQL, executes.

    The engine enriches a SlayerQuery (user-facing, just names) into an
    EnrichedQuery (fully resolved SQL expressions), then passes it to the
    SQLGenerator for SQL generation.
    """

    def __init__(self, storage: StorageBackend):
        self.storage = storage
        self._resolving: set = set()  # Track currently resolving models to detect cycles

    def execute(self, query: "SlayerQuery | list[SlayerQuery]") -> SlayerResponse:
        # Accept single query or list (last is main)
        if isinstance(query, list):
            queries = query
            query = queries[-1]
            named_queries = {}
            for q in queries[:-1]:
                if q.name:
                    if q.name in named_queries:
                        raise ValueError(f"Duplicate query name '{q.name}' in query list")
                    named_queries[q.name] = q
        else:
            named_queries = {}

        # Preprocessing
        if query.whole_periods_only:
            query = query.snap_to_whole_periods()

        # Resolve model from query.source_model (str, SlayerModel, or ModelExtension)
        model = self._resolve_query_model(
            query_model=query.source_model, named_queries=named_queries,
        )

        datasource = self._resolve_datasource(model=model)

        # Enrich: SlayerQuery + model → EnrichedQuery
        enriched = self._enrich(query=query, model=model,
                                named_queries=named_queries)

        # Generate SQL from EnrichedQuery
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        sql = generator.generate(enriched=enriched)
        logger.debug("Generated SQL:\n%s", sql)

        # Collect field metadata from enriched query
        meta: Dict[str, FieldMetadata] = {}
        for d in enriched.dimensions:
            if d.label:
                meta[d.alias] = FieldMetadata(label=d.label)
        for td in enriched.time_dimensions:
            if td.label:
                meta[td.alias] = FieldMetadata(label=td.label)
        for m in enriched.measures:
            if m.label:
                meta[m.alias] = FieldMetadata(label=m.label)
        for e in enriched.expressions:
            if e.label:
                meta[e.alias] = FieldMetadata(label=e.label)
        for t in enriched.transforms:
            if t.label:
                meta[t.alias] = FieldMetadata(label=t.label)

        # dry_run: return SQL without executing
        if query.dry_run:
            return SlayerResponse(data=[], sql=sql, meta=meta)

        # Execute
        client = SlayerSQLClient(datasource=datasource)

        # explain: run EXPLAIN ANALYZE instead of the query itself
        if query.explain:
            explain_sql = f"EXPLAIN ANALYZE {sql}"
            rows = client.execute(sql=explain_sql)
            return SlayerResponse(data=rows, sql=sql, meta=meta)

        rows = client.execute(sql=sql)
        return SlayerResponse(data=rows, sql=sql, meta=meta)

    def _resolve_query_model(self, query_model, named_queries: dict = None) -> SlayerModel:
        """Resolve query.source_model — handles str, SlayerModel, and ModelExtension."""
        from slayer.core.query import ModelExtension
        named_queries = named_queries or {}

        if isinstance(query_model, str):
            return self._resolve_model(model_name=query_model, named_queries=named_queries)
        elif isinstance(query_model, SlayerModel):
            return query_model
        elif isinstance(query_model, ModelExtension):
            base = self._resolve_model(
                model_name=query_model.source_name, named_queries=named_queries,
            )
            # Extend the base model with extra dims/measures/joins
            from slayer.core.models import ModelJoin
            extra_dims = [Dimension.model_validate(d) if isinstance(d, dict) else d
                          for d in (query_model.dimensions or [])]
            extra_measures = [Measure.model_validate(m) if isinstance(m, dict) else m
                              for m in (query_model.measures or [])]
            extra_joins = [ModelJoin.model_validate(j) if isinstance(j, dict) else j
                           for j in (query_model.joins or [])]
            return base.model_copy(update={
                "dimensions": list(base.dimensions) + extra_dims,
                "measures": list(base.measures) + extra_measures,
                "joins": list(base.joins) + extra_joins,
            })
        elif isinstance(query_model, dict):
            # Dict — could be ModelExtension or SlayerModel
            if "source_name" in query_model:
                ext = ModelExtension.model_validate(query_model)
                return self._resolve_query_model(ext, named_queries)
            else:
                model = SlayerModel.model_validate(query_model)
                return model
        else:
            raise ValueError(f"Invalid query.source_model type: {type(query_model)}")

    def _resolve_model(self, model_name: str,
                        named_queries: dict[str, SlayerQuery] = None) -> SlayerModel:
        """Resolve a model by name — checks named queries first, then storage."""
        named_queries = named_queries or {}

        # Circular reference protection
        if model_name in self._resolving:
            raise ValueError(
                f"Circular reference detected: '{model_name}' references itself "
                f"(resolution chain: {' → '.join(self._resolving)} → {model_name})"
            )
        self._resolving.add(model_name)
        try:
            return self._resolve_model_inner(model_name, named_queries)
        finally:
            self._resolving.discard(model_name)

    def _resolve_model_inner(self, model_name: str,
                              named_queries: dict[str, SlayerQuery]) -> SlayerModel:
        # Named query overrides stored model
        if model_name in named_queries:
            return self._query_as_model(inner_query=named_queries[model_name],
                                        named_queries=named_queries)

        model = self.storage.get_model(model_name)
        if model is None:
            raise ValueError(f"Model '{model_name}' not found")

        # If model has source_queries, re-enrich from stored queries
        if hasattr(model, 'source_queries') and model.source_queries:
            # Parse stored queries (may be dicts from YAML round-trip)
            parsed = [
                SlayerQuery.model_validate(q) if isinstance(q, dict) else q
                for q in model.source_queries
            ]
            return self._query_as_model(
                inner_query=parsed[-1],
                named_queries={q.name: q for q in parsed[:-1] if q.name},
                override_name=model.name,
            )

        return model

    def create_model_from_query(
        self, query: "SlayerQuery | list[SlayerQuery]", name: str,
        description: str = None, save: bool = True,
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
        virtual = self._query_as_model(inner_query=main_query, named_queries=named)
        model = SlayerModel(
            name=name,
            source_queries=queries,
            data_source=virtual.data_source,
            dimensions=virtual.dimensions,
            measures=virtual.measures,
            description=description,
        )
        if save:
            self.storage.save_model(model)
        return model

    def _enrich(
        self,
        query: SlayerQuery,
        model: SlayerModel,
        named_queries: dict[str, SlayerQuery] = None,
    ) -> EnrichedQuery:
        """Resolve a SlayerQuery against model definitions into an EnrichedQuery.

        This is where name-based references (e.g., field="count") get resolved
        to their SQL expressions, aggregation types, and model context.
        """
        # Resolve dimensions — look up each from the model definition.
        # Supports multi-hop dotted names (e.g., "customers.regions.name") by
        # translating to the __ convention used by rollup dimensions.
        model_name_str = query.source_model if isinstance(query.source_model, str) else model.name
        dimensions = []
        for dim_ref in (query.dimensions or []):
            dim_name = dim_ref.name
            # Multi-hop dotted name → translate to __ convention
            # "customers.regions.name" → "customers__regions__name"
            # Also try direct lookup first (for explicitly named dims)
            dim_def = model.get_dimension(dim_name)
            if dim_def is None and "." in dim_name:
                # Try progressively shorter __ translations (for rollup dims):
                parts = dim_name.split(".")
                for start in range(len(parts)):
                    candidate = "__".join(parts[start:])
                    dim_def = model.get_dimension(candidate)
                    if dim_def:
                        dim_name = candidate
                        break

                # If still not found, walk the join graph
                if dim_def is None:
                    dim_def = self._resolve_dimension_via_joins(
                        model=model, parts=parts, named_queries=named_queries or {},
                    )
                    if dim_def:
                        dim_name = dim_def.name
            # For joined dimensions (dotted names), model_name is the join target
            # so _resolve_sql qualifies the column with the right table alias
            if "." in dim_ref.name:
                table_prefix = dim_ref.name.rsplit(".", 1)[0]
                # Use path-based alias: "customers.regions" → "customers__regions"
                effective_model = "__".join(table_prefix.split("."))
            else:
                effective_model = model.name
            dimensions.append(EnrichedDimension(
                name=dim_ref.name,
                sql=dim_def.sql if dim_def else None,
                type=dim_def.type if dim_def else DataType.STRING,
                alias=f"{model_name_str}.{dim_ref.name}",
                model_name=effective_model, label=dim_ref.label,
            ))

        # Measures are populated from fields (bare measure names auto-add here)
        measures: list[EnrichedMeasure] = []

        # Resolve time dimensions
        time_dimensions = []
        for td in (query.time_dimensions or []):
            dim_def = model.get_dimension(td.dimension.name)
            time_dimensions.append(EnrichedTimeDimension(
                name=td.dimension.name,
                sql=dim_def.sql if dim_def else None,
                granularity=td.granularity,
                date_range=td.date_range,
                alias=f"{model_name_str}.{td.dimension.name}",
                model_name=model.name,
                label=td.label,
            ))

        # Resolve time dimension for transforms that need ORDER BY time.
        # - Single time dimension → use it (no ambiguity)
        # - Multiple time dimensions → use main_time_dimension if specified,
        #   then model's default_time_dimension if it's among the query's
        #   time dimensions, otherwise error
        # - No time dimensions → fall back to model's default_time_dimension
        resolved_time_alias = None
        if len(time_dimensions) == 1:
            resolved_time_alias = time_dimensions[0].alias
        elif len(time_dimensions) > 1:
            if query.main_time_dimension:
                resolved_time_alias = f"{model.name}.{query.main_time_dimension}"
            elif model.default_time_dimension:
                # Only use default if it's among the query's time dimensions
                td_names = {td.name for td in time_dimensions}
                if model.default_time_dimension in td_names:
                    resolved_time_alias = f"{model.name}.{model.default_time_dimension}"
        else:
            # No time dimensions in query — fall back to model default
            if model.default_time_dimension:
                resolved_time_alias = f"{model.name}.{model.default_time_dimension}"

        # Resolve time column for `type: last` aggregation.
        # Unlike transforms (which need granularity), this only needs a time column
        # for ORDER BY within each group — no bucketing required.
        # Resolution: main_time_dimension → first time dim in dimensions →
        #   first time dim in filters → model default_time_dimension
        last_agg_time_column = None
        if query.main_time_dimension:
            last_agg_time_column = query.main_time_dimension
        if last_agg_time_column is None:
            # Check regular dimensions for time/date types
            for dim_ref in (query.dimensions or []):
                dim_def = model.get_dimension(dim_ref.name)
                if dim_def and dim_def.type in (DataType.TIMESTAMP, DataType.DATE):
                    last_agg_time_column = dim_ref.name
                    break
        if last_agg_time_column is None:
            # Check time_dimensions
            if time_dimensions:
                last_agg_time_column = time_dimensions[0].name
        if last_agg_time_column is None and query.filters:
            # Check filters for time dimension references
            time_dim_names = {
                d.name for d in model.dimensions
                if d.type in (DataType.TIMESTAMP, DataType.DATE)
            }
            for f_str in (query.filters or []):
                for td_name in time_dim_names:
                    if td_name in f_str:
                        last_agg_time_column = td_name
                        break
                if last_agg_time_column:
                    break
        if last_agg_time_column is None and model.default_time_dimension:
            last_agg_time_column = model.default_time_dimension

        # Process fields — parse formulas and flatten into measures/expressions/transforms
        import re
        from slayer.core.formula import (
            ArithmeticField, MeasureRef, MixedArithmeticField,
            TransformField, TIME_TRANSFORMS, parse_formula, parse_filter,
        )

        enriched_expressions: list[EnrichedExpression] = []
        enriched_transforms: list[EnrichedTransform] = []
        cross_model_measures: list = []

        # Track all known aliases (measures, expressions, transforms) for resolution
        known_aliases: dict[str, str] = {}  # name → alias

        def _ensure_measure(mname: str):
            """Add a measure to the base query if not already present."""
            if not any(m.name == mname for m in measures):
                measure_def = model.get_measure(mname)
                if measure_def is None:
                    raise ValueError(f"Measure '{mname}' not found in model '{model.name}'")
                # For joined measures (dotted names), model_name is the join target
                if "." in mname:
                    effective_model = mname.rsplit(".", 1)[0].split(".")[-1]
                else:
                    effective_model = model.name
                measures.append(EnrichedMeasure(
                    name=mname, sql=measure_def.sql, type=measure_def.type,
                    alias=f"{model_name_str}.{mname}", model_name=effective_model,
                ))
                known_aliases[mname] = f"{model_name_str}.{mname}"

        def _resolve_sql(sql: str) -> str:
            """Replace known names with their quoted aliases."""
            resolved = sql
            for name, alias in sorted(known_aliases.items(), key=lambda x: -len(x[0])):
                resolved = re.sub(rf'\b{re.escape(name)}\b', f'"{alias}"', resolved)
            return resolved

        _self_join_transforms = {"time_shift", "change", "change_pct"}

        def _add_transform(name: str, transform: str, measure_alias: str,
                           offset: int = 1, granularity: str = None):
            """Add a transform to the enriched list, checking time requirements."""
            needs_time = transform in TIME_TRANSFORMS
            if needs_time and resolved_time_alias is None:
                raise ValueError(
                    f"Field '{name}' ({transform}) requires a time dimension. "
                    f"Add a time_dimension to the query or set default_time_dimension on the model."
                )
            # Self-join transforms and last() need actual time dimensions in the
            # query for meaningful time bucketing. A bare default_time_dimension
            # fallback (no time dimensions) produces no grouping → wrong results.
            if transform in (_self_join_transforms | {"last"}) and not time_dimensions:
                raise ValueError(
                    f"Field '{name}' ({transform}) requires a time_dimension in the query "
                    f"with a granularity for time bucketing."
                )
            alias = f"{model_name_str}.{name}"
            enriched_transforms.append(EnrichedTransform(
                name=name, transform=transform, measure_alias=measure_alias,
                alias=alias, offset=offset, granularity=granularity,
                time_alias=resolved_time_alias if needs_time else None,
            ))
            known_aliases[name] = alias

        def _flatten_spec(spec, field_name: str) -> str:
            """Recursively flatten a FieldSpec into enriched steps.

            Returns the alias of the final result (measure alias, expression alias,
            or transform alias) so parent specs can reference it.
            """
            if isinstance(spec, MeasureRef):
                # Check local measure first (dotted names from ingestion like "customers.count")
                # Only treat as cross-model if not found locally
                if "." in spec.name and model.get_measure(spec.name) is not None:
                    # Local measure with dotted name — treat as regular measure
                    pass
                elif "." in spec.name:
                    cm = self._resolve_cross_model_measure(
                        spec_name=spec.name, field_name=field_name,
                        model=model, query=query,
                        dimensions=dimensions, time_dimensions=time_dimensions,
                        named_queries=named_queries,
                    )
                    cross_model_measures.append(cm)
                    known_aliases[field_name] = cm.alias
                    return cm.alias

                _ensure_measure(spec.name)
                return f"{model_name_str}.{spec.name}"

            elif isinstance(spec, ArithmeticField):
                for mname in spec.measure_names:
                    _ensure_measure(mname)
                alias = f"{model_name_str}.{field_name}"
                enriched_expressions.append(EnrichedExpression(
                    name=field_name, sql=_resolve_sql(spec.sql), alias=alias,
                ))
                known_aliases[field_name] = alias
                return alias

            elif isinstance(spec, MixedArithmeticField):
                # Ensure bare measures
                for mname in spec.measure_names:
                    _ensure_measure(mname)
                # Flatten sub-transforms first
                for placeholder, sub_transform in spec.sub_transforms:
                    _flatten_spec(sub_transform, placeholder)
                # Now build the arithmetic referencing placeholders
                alias = f"{model_name_str}.{field_name}"
                enriched_expressions.append(EnrichedExpression(
                    name=field_name, sql=_resolve_sql(spec.sql), alias=alias,
                ))
                known_aliases[field_name] = alias
                return alias

            elif isinstance(spec, TransformField):

                # Validate: nesting a self-join transform inside another is not supported
                # (e.g., change(time_shift(x)) — the outer's shifted CTE can't replay the inner)
                if (spec.transform in _self_join_transforms
                        and isinstance(spec.inner, TransformField)
                        and spec.inner.transform in _self_join_transforms):
                    raise ValueError(
                        f"Nesting '{spec.transform}' around '{spec.inner.transform}' is not supported. "
                        f"Both use self-join CTEs. Try wrapping with a window function instead "
                        f"(e.g., cumsum, lag)."
                    )

                # Flatten inner first
                inner_name = f"_inner_{field_name}"
                if isinstance(spec.inner, MeasureRef):
                    inner_alias = _flatten_spec(spec.inner, spec.inner.name)
                elif isinstance(spec.inner, TransformField):
                    inner_alias = _flatten_spec(spec.inner, inner_name)
                elif isinstance(spec.inner, ArithmeticField):
                    inner_alias = _flatten_spec(spec.inner, inner_name)
                elif isinstance(spec.inner, MixedArithmeticField):
                    inner_alias = _flatten_spec(spec.inner, inner_name)
                else:
                    raise ValueError(f"Unsupported inner type in formula: {spec!r}")

                # Extract transform args
                offset = 1
                granularity = None
                if spec.args:
                    offset = spec.args[0] if isinstance(spec.args[0], int) else 1
                if len(spec.args) >= 2:
                    granularity = str(spec.args[1])

                # change/change_pct look backward by default (like LAG),
                # so negate offset for self-join semantics
                if spec.transform in ("change", "change_pct") and not spec.args:
                    offset = -1

                _add_transform(
                    name=field_name, transform=spec.transform,
                    measure_alias=inner_alias, offset=offset, granularity=granularity,
                )
                return f"{model_name_str}.{field_name}"

            raise ValueError(f"Unsupported field spec: {spec!r}")

        # Process each field
        for field in (query.fields or []):
            spec = parse_formula(field.formula)
            field_name = field.name or field.formula.replace(" ", "_").replace("/", "_div_")

            if isinstance(spec, MeasureRef):
                # Check for cross-model measure reference (e.g., "customers.avg_score")
                # But check local measures first (dotted names from ingestion like "customers.count")
                if "." in spec.name and model.get_measure(spec.name) is None:
                    cm = self._resolve_cross_model_measure(
                        spec_name=spec.name, field_name=field_name,
                        model=model, query=query,
                        dimensions=dimensions, time_dimensions=time_dimensions,
                        label=field.label, named_queries=named_queries,
                    )
                    cross_model_measures.append(cm)
                    continue

                _ensure_measure(spec.name)
                # Validate type=last has a time column for ordering
                measure_def = model.get_measure(spec.name)
                if measure_def and measure_def.type == DataType.LAST and last_agg_time_column is None:
                    raise ValueError(
                        f"Measure '{spec.name}' has type=last but no time column could be resolved. "
                        f"Add a time dimension, set main_time_dimension, or set default_time_dimension on the model."
                    )
                # Apply label to the measure if provided
                if field.label:
                    for m in measures:
                        if m.name == spec.name:
                            m.label = field.label
            else:
                _flatten_spec(spec, field_name)
                # Apply label to the last enriched expression or transform
                if field.label:
                    alias = f"{model_name_str}.{field_name}"
                    for e in enriched_expressions:
                        if e.alias == alias:
                            e.label = field.label
                    for t in enriched_transforms:
                        if t.alias == alias:
                            t.label = field.label

        measure_names_set = {m.name for m in measures}

        # Validate model-level filters: must be WHERE-only (table columns, including joined).
        # Dotted names (e.g., "customers.status") reference joined table columns — allowed.
        # Measure references are not allowed (those are HAVING, not WHERE).
        for mf in model.filters:
            parsed_mf = parse_filter(mf)
            for col in parsed_mf.columns:
                if col in measure_names_set:
                    raise ValueError(
                        f"Model filter '{mf}' references measure '{col}'. "
                        f"Model filters can only reference table columns (WHERE). "
                        f"Use query-level filters for measure conditions."
                    )

        # Pre-process filters: extract inline transform expressions
        # (e.g., "last(change(revenue)) < 0" → hidden field + rewritten filter)
        # Combine model-level filters with query-level filters
        all_filter_strs = list(model.filters) + list(query.filters or [])
        processed_filters = []
        ft_counter = [0]  # Shared counter across all filters for unique _ftN names
        for f_str in all_filter_strs:
            rewritten, extra_fields = SlayerQueryEngine._extract_filter_transforms(
                f_str, counter=ft_counter,
            )
            for name, formula in extra_fields:
                spec = parse_formula(formula)
                _flatten_spec(spec, name)
            processed_filters.append(rewritten)

        # Only include last_agg_time_column if there are actual type=last measures
        has_last_measures = any(m.type == DataType.LAST for m in measures)

        # Resolve JOIN clauses — only include JOINs the query actually needs.
        # Collect referenced join targets from dimensions, measures, and cross-model measures.
        needed_tables = set()
        for d in dimensions:
            if "." in d.name:
                # e.g., "customers.name" → need "customers"
                # e.g., "customers.regions.name" → need "customers" and "regions"
                parts = d.name.split(".")
                for part in parts[:-1]:  # all except the column name
                    needed_tables.add(part)
        for m in measures:
            if "." in m.name:
                parts = m.name.split(".")
                for part in parts[:-1]:
                    needed_tables.add(part)
        for cm in cross_model_measures:
            needed_tables.add(cm.target_model_name)
        # Scan processed filters for dotted column references (joined table columns)
        for f_str in processed_filters:
            parsed_f = parse_filter(f_str)
            for col in parsed_f.columns:
                if "." in col:
                    parts = col.split(".")
                    for part in parts[:-1]:
                        needed_tables.add(part)

        # Compute transitive dependencies: if "regions" is needed but it's reached
        # via "customers", include "customers" too. BFS walk the join graph backward.
        expanded = set(needed_tables)
        queue = list(needed_tables)
        while queue:
            table = queue.pop()
            for mj in model.joins:
                if mj.target_model == table:
                    for src_col, _ in mj.join_pairs:
                        if "." in src_col:
                            intermediate = src_col.split(".")[0]
                            if intermediate not in expanded:
                                expanded.add(intermediate)
                                queue.append(intermediate)
        needed_tables = expanded

        # Resolve only the needed joins (in model.joins order for stability)
        nq = named_queries or {}
        resolved_joins = []
        for mj in model.joins:
            if mj.target_model not in needed_tables:
                continue
            if mj.target_model in nq:
                target = self._query_as_model(
                    inner_query=nq[mj.target_model], named_queries=nq,
                )
            else:
                target = self.storage.get_model(mj.target_model) if self.storage else None
            if target and target.sql_table:
                target_table = target.sql_table
            elif target and target.sql:
                target_table = f"({target.sql})"
            else:
                target_table = mj.target_model
            # Compute path-based alias for the joined table.
            # Direct join: alias = target_model (e.g., "customers")
            # Transitive join: alias = path__target (e.g., "customers__regions")
            # This disambiguates diamond joins where the same table is reached via different paths.
            join_conds = []
            path_prefix = ""
            for src_col, tgt_col in mj.join_pairs:
                if "." in src_col:
                    src_parts = src_col.rsplit(".", 1)
                    src_table = src_parts[0]
                    src_raw = src_parts[1]
                    path_prefix = src_table + "__"
                    join_conds.append(f"{src_table}.{src_raw} = {path_prefix}{mj.target_model}.{tgt_col}")
                else:
                    join_conds.append(f"{model_name_str}.{src_col} = {mj.target_model}.{tgt_col}")
            table_alias = f"{path_prefix}{mj.target_model}"
            resolved_joins.append((target_table, table_alias, " AND ".join(join_conds)))

        return EnrichedQuery(
            model_name=model.name,
            sql_table=model.sql_table,
            sql=model.sql,
            resolved_joins=resolved_joins,
            dimensions=dimensions,
            measures=measures,
            time_dimensions=time_dimensions,
            expressions=enriched_expressions,
            transforms=enriched_transforms,
            cross_model_measures=cross_model_measures,
            last_agg_time_column=last_agg_time_column if has_last_measures else None,
            filters=SlayerQueryEngine._classify_filters(
                filters=[parse_filter(f) for f in processed_filters],
                measure_names={m.name for m in measures},
                computed_names={t.name for t in enriched_transforms}
                              | {e.name for e in enriched_expressions},
                groupby_names={d.name for d in dimensions}
                             | {td.name for td in time_dimensions},
            ),
            order=query.order,
            limit=query.limit,
            offset=query.offset,
        )

    @staticmethod
    def _extract_filter_transforms(filter_str: str,
                                   counter: list[int] = None) -> tuple[str, list[tuple[str, str]]]:
        """Extract transform function calls from a filter string.

        Returns (rewritten_filter, [(name, formula), ...]) where transform
        calls are replaced with generated field names.

        Args:
            counter: Shared mutable counter [n] for unique _ftN names across
                multiple filter strings. If None, starts at 0.

        Example: "last(change(revenue)) < 0"
            → ("_ft0 < 0", [("_ft0", "last(change(revenue))")])
        """
        import ast as _ast
        from slayer.core.formula import ALL_TRANSFORMS, _preprocess_like

        if counter is None:
            counter = [0]

        # Pre-process `like`/`not like` operators so ast.parse doesn't fail
        preprocessed = _preprocess_like(filter_str)
        try:
            tree = _ast.parse(preprocessed, mode="eval")
        except SyntaxError:
            return filter_str, []

        transforms: list[tuple[str, str]] = []

        def _replace(node):
            if isinstance(node, _ast.Call) and isinstance(node.func, _ast.Name) and node.func.id in ALL_TRANSFORMS:
                name = f"_ft{counter[0]}"
                counter[0] += 1
                formula = _ast.unparse(node)
                transforms.append((name, formula))
                return _ast.Name(id=name, ctx=_ast.Load())
            # Recurse into child nodes
            if isinstance(node, _ast.BinOp):
                node.left = _replace(node.left)
                node.right = _replace(node.right)
            elif isinstance(node, _ast.UnaryOp):
                node.operand = _replace(node.operand)
            elif isinstance(node, _ast.Compare):
                node.left = _replace(node.left)
                node.comparators = [_replace(c) for c in node.comparators]
            elif isinstance(node, _ast.BoolOp):
                node.values = [_replace(v) for v in node.values]
            return node

        modified = _replace(tree.body)
        if not transforms:
            return filter_str, []
        return _ast.unparse(modified), transforms

    @staticmethod
    def _classify_filters(filters: list, measure_names: set,
                          computed_names: set = None,
                          groupby_names: set = None) -> list:
        """Classify filters as WHERE, HAVING, or post-filter.

        Post-filters reference computed columns (transforms/expressions) and
        are applied as a WHERE on an outer wrapper around the final query.

        HAVING filters that reference dimensions not in the GROUP BY are
        rejected early — they would produce invalid SQL.
        """
        computed_names = computed_names or set()
        groupby_names = groupby_names or set()
        for f in filters:
            if any(col in computed_names for col in f.columns):
                f.is_post_filter = True
            elif any(col in measure_names for col in f.columns):
                f.is_having = True
                # Validate: non-measure columns in a HAVING filter must be in GROUP BY
                for col in f.columns:
                    if col not in measure_names and col not in groupby_names:
                        raise ValueError(
                            f"Filter '{f.sql}' references measure and dimension '{col}', "
                            f"but '{col}' is not in the query's dimensions or time_dimensions. "
                            f"Add it to dimensions/time_dimensions or split into separate filters."
                        )
        return filters

    def _query_as_model(self, inner_query: SlayerQuery,
                         named_queries: dict[str, SlayerQuery] = None,
                         override_name: str = None) -> SlayerModel:
        """Build a virtual SlayerModel from a nested query's result.

        Enriches and generates SQL for the inner query, then creates a model
        whose `sql` is the inner query's SQL and whose dimensions/measures
        are derived from the inner query's enriched columns.
        """
        named_queries = named_queries or {}

        # Resolve the inner model (handles str, SlayerModel, ModelExtension)
        inner_model = self._resolve_query_model(
            query_model=inner_query.source_model, named_queries=named_queries,
        )

        # Enrich the inner query
        enriched = self._enrich(query=inner_query, model=inner_model)

        # Generate SQL
        datasource = self._resolve_datasource(model=inner_model)
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        inner_sql = generator.generate(enriched=enriched)

        # Build virtual model from enriched columns.
        # Inner query columns have aliases like "orders.count" (with dots).
        # We wrap the inner SQL in a renaming subquery so the virtual model
        # has clean column names that work naturally in JOINs and references.
        virtual_name = override_name or inner_query.name or f"_subquery_{inner_model.name}"

        # Build lookup for labels/descriptions from the source model
        source_dim_desc = {d.name: d.description for d in inner_model.dimensions if d.description}
        source_measure_desc = {m.name: m.description for m in inner_model.measures if m.description}

        # Collect all inner aliases and their short names
        column_map = []  # (inner_alias, short_name, data_type, is_measure, label)
        for d in enriched.dimensions:
            label = d.label or source_dim_desc.get(d.name)
            column_map.append((d.alias, d.name, d.type, False, label))
        for td in enriched.time_dimensions:
            label = td.label or source_dim_desc.get(td.name)
            column_map.append((td.alias, td.name, DataType.TIMESTAMP, False, label))
        for m in enriched.measures:
            label = m.label or source_measure_desc.get(m.name)
            column_map.append((m.alias, m.name, DataType.NUMBER, True, label))
        for t in enriched.transforms:
            column_map.append((t.alias, t.name, DataType.NUMBER, True, t.label))
        for e in enriched.expressions:
            column_map.append((e.alias, e.name, DataType.NUMBER, True, e.label))
        for cm in enriched.cross_model_measures:
            # Cross-model measure alias is like "orders.customers__avg_score"
            # Short name: strip the source model prefix
            short = cm.alias.split(".", 1)[-1] if "." in cm.alias else cm.alias
            column_map.append((cm.alias, short, DataType.NUMBER, True, None))

        # Wrap inner SQL: SELECT "orders.id" AS id, "orders.count" AS count, ... FROM (inner) AS _inner
        rename_parts = [f'"{alias}" AS {short}' for alias, short, _, _, _ in column_map]
        wrapped_sql = f"SELECT {', '.join(rename_parts)} FROM ({inner_sql}) AS _inner"

        dims = []
        for alias, short, dtype, is_measure, label in column_map:
            dims.append(Dimension(name=short, sql=short, type=dtype, description=label))

        # Provide standard aggregation measures for the outer query
        measures = [
            Measure(name="count", type=DataType.COUNT),
        ]
        # Add aggregation measures for inner columns
        for alias, short, dtype, is_measure, label in column_map:
            if is_measure:
                # Numeric: SUM, AVG, MIN, MAX, COUNT_DISTINCT
                measures.append(Measure(name=f"{short}_sum", sql=short, type=DataType.SUM))
                measures.append(Measure(name=f"{short}_avg", sql=short, type=DataType.AVERAGE))
                measures.append(Measure(name=f"{short}_min", sql=short, type=DataType.MIN))
                measures.append(Measure(name=f"{short}_max", sql=short, type=DataType.MAX))
                measures.append(Measure(name=f"{short}_distinct", sql=short, type=DataType.COUNT_DISTINCT))
            else:
                # Non-numeric dimensions: COUNT_DISTINCT and COUNT (non-null)
                measures.append(Measure(name=f"{short}_distinct", sql=short, type=DataType.COUNT_DISTINCT))
                measures.append(Measure(name=f"{short}_count", sql=short, type=DataType.COUNT))

        return SlayerModel(
            name=virtual_name,
            sql=wrapped_sql,
            data_source=inner_model.data_source,
            dimensions=dims,
            measures=measures,
            default_time_dimension=inner_model.default_time_dimension,
        )

    def _resolve_dimension_via_joins(
        self, model: SlayerModel, parts: list[str],
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
            target = self._resolve_model(
                model_name=hop_name, named_queries=named_queries or {},
            )
            visited.add(hop_name)
            current_model = target

        # Look up the final dimension on the terminal model
        dim_name = parts[-1]
        return current_model.get_dimension(dim_name)

    def _resolve_cross_model_measure(
        self, spec_name: str, field_name: str,
        model: SlayerModel, query,
        dimensions: list, time_dimensions: list,
        label: str = None, named_queries: dict = None,
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
        target_model = self._resolve_model(
            model_name=target_model_name, named_queries=named_queries or {},
        )

        # Find the measure in the target model
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
        alias = f"{query_model_name}.{target_model_name}__{measure_name}"

        return CrossModelMeasure(
            name=field_name,
            alias=alias,
            target_model_name=target_model_name,
            target_model_sql_table=target_model.sql_table,
            target_model_sql=target_model.sql,
            measure=EnrichedMeasure(
                name=measure_name, sql=measure_def.sql, type=measure_def.type,
                alias=f"{target_model_name}.{measure_name}",
                model_name=target_model_name,
            ),
            join_pairs=join.join_pairs,
            shared_dimensions=shared_dims,
            shared_time_dimensions=shared_time_dims,
            source_model_name=model.name,
            source_sql_table=model.sql_table,
            source_sql=model.sql,
            label=label,
        )

    def _resolve_datasource(self, model: SlayerModel) -> DatasourceConfig:
        ds_name = model.data_source
        if not ds_name:
            raise ValueError(
                f"Model '{model.name}' has no data_source configured. "
                f"Set data_source on the model or ensure the source model has one."
            )
        ds = self.storage.get_datasource(ds_name)
        if ds is None:
            raise ValueError(
                f"Datasource '{ds_name}' not found for model '{model.name}'"
            )
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
