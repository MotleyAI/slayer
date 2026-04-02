"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → _enrich() → EnrichedQuery → SQLGenerator → SQL → execute
"""

import logging
from typing import Any, Dict, List, Optional

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, SlayerModel
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


class SlayerResponse:
    """Response from a SLayer query."""

    def __init__(self, data: List[Dict[str, Any]], columns: Optional[List[str]] = None,
                 sql: Optional[str] = None, labels: Optional[Dict[str, str]] = None):
        self.data = data
        self.columns = columns or (list(data[0].keys()) if data else [])
        self.sql = sql
        self.labels = labels or {}  # column alias → human-readable label

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

    def execute(self, query: SlayerQuery) -> SlayerResponse:
        # Preprocessing
        if query.whole_periods_only:
            query = query.snap_to_whole_periods()

        model = self.storage.get_model(query.model)
        if model is None:
            raise ValueError(f"Model '{query.model}' not found")

        datasource = self._resolve_datasource(model=model)

        # Enrich: SlayerQuery + model → EnrichedQuery
        enriched = self._enrich(query=query, model=model)

        # Generate SQL from EnrichedQuery
        dialect = self._dialect_for_type(datasource.type)
        generator = SQLGenerator(dialect=dialect)
        sql = generator.generate(enriched=enriched)
        logger.debug("Generated SQL:\n%s", sql)

        # Execute
        client = SlayerSQLClient(datasource=datasource)
        rows = client.execute(sql=sql)

        # Collect labels from enriched query
        labels = {}
        for d in enriched.dimensions:
            if d.label:
                labels[d.alias] = d.label
        for td in enriched.time_dimensions:
            if td.label:
                labels[td.alias] = td.label
        for m in enriched.measures:
            if m.label:
                labels[m.alias] = m.label
        for e in enriched.expressions:
            if e.label:
                labels[e.alias] = e.label
        for t in enriched.transforms:
            if t.label:
                labels[t.alias] = t.label

        return SlayerResponse(data=rows, sql=sql, labels=labels)

    def _enrich(
        self,
        query: SlayerQuery,
        model: SlayerModel,
    ) -> EnrichedQuery:
        """Resolve a SlayerQuery against model definitions into an EnrichedQuery.

        This is where name-based references (e.g., field="count") get resolved
        to their SQL expressions, aggregation types, and model context.
        """
        # Resolve dimensions
        dimensions = []
        for dim_ref in (query.dimensions or []):
            dim_def = model.get_dimension(dim_ref.name)
            dimensions.append(EnrichedDimension(
                name=dim_ref.name,
                sql=dim_def.sql if dim_def else None,
                type=dim_def.type if dim_def else model.dimensions[0].type if model.dimensions else None,
                alias=f"{query.model}.{dim_ref.name}",
                model_name=model.name,
                label=dim_ref.label,
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
                alias=f"{query.model}.{td.dimension.name}",
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
                measures.append(EnrichedMeasure(
                    name=mname, sql=measure_def.sql, type=measure_def.type,
                    alias=f"{query.model}.{mname}", model_name=model.name,
                ))
                known_aliases[mname] = f"{query.model}.{mname}"

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
            alias = f"{query.model}.{name}"
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
                _ensure_measure(spec.name)
                return f"{query.model}.{spec.name}"

            elif isinstance(spec, ArithmeticField):
                for mname in spec.measure_names:
                    _ensure_measure(mname)
                alias = f"{query.model}.{field_name}"
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
                alias = f"{query.model}.{field_name}"
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
                return f"{query.model}.{field_name}"

            raise ValueError(f"Unsupported field spec: {spec!r}")

        # Process each field
        for field in (query.fields or []):
            spec = parse_formula(field.formula)
            field_name = field.name or field.formula.replace(" ", "_").replace("/", "_div_")

            if isinstance(spec, MeasureRef):
                # Check for cross-model measure reference (e.g., "customers.avg_score")
                if "." in spec.name:
                    cm = self._resolve_cross_model_measure(
                        spec_name=spec.name, field_name=field_name,
                        model=model, query=query,
                        dimensions=dimensions, time_dimensions=time_dimensions,
                        label=field.label,
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
                    alias = f"{query.model}.{field_name}"
                    for e in enriched_expressions:
                        if e.alias == alias:
                            e.label = field.label
                    for t in enriched_transforms:
                        if t.alias == alias:
                            t.label = field.label

        # Pre-process filters: extract inline transform expressions
        # (e.g., "last(change(revenue)) < 0" → hidden field + rewritten filter)
        processed_filters = []
        ft_counter = [0]  # Shared counter across all filters for unique _ftN names
        for f_str in (query.filters or []):
            rewritten, extra_fields = SlayerQueryEngine._extract_filter_transforms(
                f_str, counter=ft_counter,
            )
            for name, formula in extra_fields:
                spec = parse_formula(formula)
                _flatten_spec(spec, name)
            processed_filters.append(rewritten)

        # Only include last_agg_time_column if there are actual type=last measures
        has_last_measures = any(m.type == DataType.LAST for m in measures)

        return EnrichedQuery(
            model_name=model.name,
            sql_table=model.sql_table,
            sql=model.sql,
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

    def _resolve_cross_model_measure(
        self, spec_name: str, field_name: str,
        model: SlayerModel, query,
        dimensions: list, time_dimensions: list,
        label: str = None,
    ) -> CrossModelMeasure:
        """Resolve a cross-model measure reference like 'customers.avg_score'.

        Looks up the join from the source model, loads the target model,
        finds shared dimensions, and returns a CrossModelMeasure for SQL generation.
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

        # Load the target model
        target_model = self.storage.get_model(target_model_name)
        if target_model is None:
            raise ValueError(f"Target model '{target_model_name}' not found in storage")

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

        alias = f"{query.model}.{target_model_name}__{measure_name}"

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
        if ds_name:
            ds = self.storage.get_datasource(ds_name)
            if ds is not None:
                return ds
        raise ValueError(
            f"Datasource '{ds_name}' not found for model '{model.name}'"
        )

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
