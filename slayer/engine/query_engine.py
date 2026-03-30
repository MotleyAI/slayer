"""Query engine — central orchestrator for SLayer queries.

Flow: SlayerQuery → _enrich() → EnrichedQuery → SQLGenerator → SQL → execute
"""

import logging
from typing import Any, Dict, List, Optional

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.enriched import (
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
        # Resolution chain: query main_time_dimension → query time_dimensions (if exactly 1) → model default.
        resolved_time_alias = None
        if query.main_time_dimension:
            resolved_time_alias = f"{model.name}.{query.main_time_dimension}"
        if resolved_time_alias is None and time_dimensions:
            if len(time_dimensions) == 1:
                resolved_time_alias = time_dimensions[0].alias
        if resolved_time_alias is None and model.default_time_dimension:
            resolved_time_alias = f"{model.name}.{model.default_time_dimension}"

        # Process fields — parse formulas and flatten into measures/expressions/transforms
        import re
        from slayer.core.formula import (
            ArithmeticField, MeasureRef, MixedArithmeticField,
            TransformField, TIME_TRANSFORMS, parse_formula, parse_filter,
        )

        enriched_expressions: list[EnrichedExpression] = []
        enriched_transforms: list[EnrichedTransform] = []

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

        def _add_transform(name: str, transform: str, measure_alias: str,
                           offset: int = 1, granularity: str = None):
            """Add a transform to the enriched list, checking time requirements."""
            needs_time = transform in TIME_TRANSFORMS
            if needs_time and resolved_time_alias is None:
                raise ValueError(
                    f"Field '{name}' ({transform}) requires a time dimension. "
                    f"Add a time_dimension to the query or set default_time_dimension on the model."
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
                _self_join = {"time_shift", "change", "change_pct"}
                if (spec.transform in _self_join
                        and isinstance(spec.inner, TransformField)
                        and spec.inner.transform in _self_join):
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
                _ensure_measure(spec.name)
                # If the measure has type=last, auto-wrap with last() transform
                measure_def = model.get_measure(spec.name)
                if measure_def and measure_def.type == DataType.LAST:
                    # Rename the base measure's alias to an internal name
                    # so it doesn't collide with the transform's output alias
                    base_alias = f"{query.model}.{spec.name}"
                    internal_alias = f"{query.model}._base_{spec.name}"
                    for m in measures:
                        if m.alias == base_alias:
                            m.alias = internal_alias
                            known_aliases[spec.name] = internal_alias
                    _add_transform(
                        name=field_name, transform="last",
                        measure_alias=internal_alias, offset=1,
                    )
                # Apply label to the measure if provided
                elif field.label:
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
        for f_str in (query.filters or []):
            rewritten, extra_fields = SlayerQueryEngine._extract_filter_transforms(f_str)
            for name, formula in extra_fields:
                spec = parse_formula(formula)
                _flatten_spec(spec, name)
            processed_filters.append(rewritten)

        return EnrichedQuery(
            model_name=model.name,
            sql_table=model.sql_table,
            sql=model.sql,
            dimensions=dimensions,
            measures=measures,
            time_dimensions=time_dimensions,
            expressions=enriched_expressions,
            transforms=enriched_transforms,
            filters=SlayerQueryEngine._classify_filters(
                filters=[parse_filter(f) for f in processed_filters],
                measure_names={m.name for m in measures},
                computed_names={t.name for t in enriched_transforms}
                              | {e.name for e in enriched_expressions},
            ),
            order=query.order,
            limit=query.limit,
            offset=query.offset,
        )

    @staticmethod
    def _extract_filter_transforms(filter_str: str) -> tuple[str, list[tuple[str, str]]]:
        """Extract transform function calls from a filter string.

        Returns (rewritten_filter, [(name, formula), ...]) where transform
        calls are replaced with generated field names.

        Example: "last(change(revenue)) < 0"
            → ("_ft0 < 0", [("_ft0", "last(change(revenue))")])
        """
        import ast as _ast
        from slayer.core.formula import ALL_TRANSFORMS

        try:
            tree = _ast.parse(filter_str, mode="eval")
        except SyntaxError:
            return filter_str, []

        transforms: list[tuple[str, str]] = []
        counter = [0]

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
                          computed_names: set = None) -> list:
        """Classify filters as WHERE, HAVING, or post-filter.

        Post-filters reference computed columns (transforms/expressions) and
        are applied as a WHERE on an outer wrapper around the final query.
        """
        computed_names = computed_names or set()
        for f in filters:
            if any(col in computed_names for col in f.columns):
                f.is_post_filter = True
            elif any(col in measure_names for col in f.columns):
                f.is_having = True
        return filters

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
