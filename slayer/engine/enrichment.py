"""Query enrichment — resolves a SlayerQuery into an EnrichedQuery.

Converts user-facing name-based references (e.g., field="count") into fully
resolved SQL expressions, aggregation types, and model context. The result
is an EnrichedQuery ready for SQL generation.

Separated from query_engine.py for clarity — this is the largest single
transformation step in the query pipeline.
"""

import re
from typing import Dict, List, Optional, Set

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType
from slayer.core.formula import (
    ALL_TRANSFORMS,
    AggregatedMeasureRef,
    ArithmeticField,
    MixedArithmeticField,
    TIME_TRANSFORMS,
    TransformField,
    _preprocess_like,
    parse_filter,
    parse_formula,
)
from slayer.core.models import SlayerModel
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

_SELF_JOIN_TRANSFORMS = {"time_shift", "change", "change_pct"}
_TABLE_COL_RE = re.compile(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b")

def enrich_query(
    query: SlayerQuery,
    model: SlayerModel,
    named_queries: Optional[Dict[str, SlayerQuery]] = None,
    *,
    resolve_dimension_via_joins,
    resolve_cross_model_measure,
    resolve_join_target,
) -> EnrichedQuery:
    """Resolve a SlayerQuery against model definitions into an EnrichedQuery.

    Args:
        query: The user-facing query.
        model: The resolved model definition.
        named_queries: Named sub-queries (for query lists).
        resolve_dimension_via_joins: Callback(model, parts, named_queries) -> Dimension|None
        resolve_cross_model_measure: Callback(spec_name, field_name, model, query,
            dimensions, time_dimensions, label, named_queries) -> CrossModelMeasure
        resolve_join_target: Callback(target_model_name, named_queries) -> (table_sql, model)|None
    """
    named_queries = named_queries or {}
    model_name_str = query.source_model if isinstance(query.source_model, str) else model.name

    # --- Dimensions ---
    dimensions = _resolve_dimensions(
        query=query,
        model=model,
        model_name_str=model_name_str,
        named_queries=named_queries,
        resolve_dimension_via_joins=resolve_dimension_via_joins,
    )

    # --- Measures (populated from fields below) ---
    measures: List[EnrichedMeasure] = []

    # --- Time dimensions ---
    time_dimensions = _resolve_time_dimensions(
        query=query,
        model=model,
        model_name_str=model_name_str,
        named_queries=named_queries,
        resolve_dimension_via_joins=resolve_dimension_via_joins,
    )

    # --- Time resolution for transforms ---
    resolved_time_alias = _resolve_time_alias(
        time_dimensions=time_dimensions,
        query=query,
        model=model,
    )

    # --- Time column for type=last aggregation ---
    last_agg_time_column = _resolve_last_agg_time(
        query=query,
        model=model,
        dimensions=dimensions,
        time_dimensions=time_dimensions,
    )

    # --- Process fields ---
    enriched_expressions: List[EnrichedExpression] = []
    enriched_transforms: List[EnrichedTransform] = []
    cross_model_measures: List[CrossModelMeasure] = []
    known_aliases: Dict[str, str] = {}

    def _ensure_aggregated_measure(
        alias_key: str,
        measure_name: str,
        aggregation_name: str,
        agg_args: Optional[list] = None,
        agg_kwargs: Optional[dict] = None,
    ):
        """Create an EnrichedMeasure for an aggregated measure ref.

        Args:
            alias_key: Key to use in known_aliases (placeholder ID or canonical name).
            measure_name: Measure name ("revenue") or "*" for COUNT(*).
            aggregation_name: Aggregation name ("sum", "weighted_avg", etc.).
            agg_args: Positional args from colon syntax (e.g., time col for last/first).
            agg_kwargs: Keyword args from colon syntax (e.g., weight override).
        """
        agg_args = agg_args or []
        agg_kwargs = agg_kwargs or {}

        # Canonical name for the result column (colon → underscore)
        if measure_name == "*":
            canonical_name = f"_{aggregation_name}"  # *:count → "_count"
        else:
            canonical_name = f"{measure_name}_{aggregation_name}"

        # Skip if already ensured with this alias_key
        alias = f"{model_name_str}.{canonical_name}"
        if any(m.alias == alias for m in measures):
            known_aliases[alias_key] = alias
            return

        # Resolve measure SQL
        if measure_name == "*":
            if aggregation_name != "count":
                raise ValueError(
                    f"Aggregation '{aggregation_name}' not allowed with measure '*' "
                    f"— use '*:count' for COUNT(*)"
                )
            sql = None
        else:
            measure_def = model.get_measure(measure_name)
            if measure_def is None:
                raise ValueError(f"Measure '{measure_name}' not found in model '{model.name}'")
            if measure_def.allowed_aggregations is not None:
                if aggregation_name not in measure_def.allowed_aggregations:
                    raise ValueError(
                        f"Aggregation '{aggregation_name}' not allowed for measure "
                        f"'{measure_name}'. Allowed: {measure_def.allowed_aggregations}"
                    )
            sql = measure_def.sql

        # Validate aggregation exists
        aggregation_def = model.get_aggregation(aggregation_name)
        if aggregation_name not in BUILTIN_AGGREGATIONS and aggregation_def is None:
            raise ValueError(
                f"Aggregation '{aggregation_name}' is not a built-in aggregation "
                f"and is not defined in model '{model.name}'."
            )

        # For first/last with explicit time dimension arg, store on the measure
        explicit_time_col = None
        if aggregation_name in ("first", "last") and agg_args:
            explicit_time_col = agg_args[0]
            if "." not in explicit_time_col:
                explicit_time_col = f"{model.name}.{explicit_time_col}"

        measures.append(
            EnrichedMeasure(
                name=canonical_name,
                sql=sql,
                aggregation=aggregation_name,
                alias=alias,
                model_name=model.name,
                aggregation_def=aggregation_def,
                agg_kwargs=agg_kwargs,
                time_column=explicit_time_col,
            )
        )
        known_aliases[alias_key] = alias

    def _resolve_sql(sql: str) -> str:
        resolved = sql
        for name, alias in sorted(known_aliases.items(), key=lambda x: -len(x[0])):
            # Negative lookbehind for . and " prevents matching inside
            # already-quoted identifiers (e.g., _count inside "orders._count")
            resolved = re.sub(rf'(?<![."])\b{re.escape(name)}\b', f'"{alias}"', resolved)
        return resolved

    def _add_transform(name: str, transform: str, measure_alias: str, offset: int = 1, granularity: str = None):
        needs_time = transform in TIME_TRANSFORMS
        if needs_time and resolved_time_alias is None:
            raise ValueError(
                f"Field '{name}' ({transform}) requires a time dimension. "
                f"Add a time_dimension to the query or set default_time_dimension on the model."
            )
        if transform in (_SELF_JOIN_TRANSFORMS | {"last"}) and not time_dimensions:
            raise ValueError(
                f"Field '{name}' ({transform}) requires a time_dimension in the query "
                f"with a granularity for time bucketing."
            )
        alias = f"{model_name_str}.{name}"
        enriched_transforms.append(
            EnrichedTransform(
                name=name,
                transform=transform,
                measure_alias=measure_alias,
                alias=alias,
                offset=offset,
                granularity=granularity,
                time_alias=resolved_time_alias if needs_time else None,
            )
        )
        known_aliases[name] = alias

    def _ensure_measure_from_spec(mname: str, agg_refs: Optional[dict] = None):
        """Ensure a measure is resolved — handles agg refs only."""
        agg_refs = agg_refs or {}
        if mname in agg_refs:
            ref = agg_refs[mname]
            if "." in ref.measure_name and ref.measure_name != "*":
                # Cross-model aggregated measure — not yet supported via agg_refs
                raise ValueError(
                    "Cross-model measures with explicit aggregation not yet supported "
                    "in arithmetic expressions. Use a separate field."
                )
            _ensure_aggregated_measure(
                alias_key=mname,
                measure_name=ref.measure_name,
                aggregation_name=ref.aggregation_name,
                agg_args=ref.agg_args,
                agg_kwargs=ref.agg_kwargs,
            )
        else:
            raise ValueError(
                f"Bare measure name '{mname}' in expression is not valid. "
                f"Use colon syntax."
            )

    def _flatten_spec(spec, field_name: str) -> str:
        if isinstance(spec, AggregatedMeasureRef):
            if "." in spec.measure_name and spec.measure_name != "*":
                # Cross-model aggregated measure
                cm = resolve_cross_model_measure(
                    spec_name=spec.measure_name,
                    field_name=field_name,
                    model=model,
                    query=query,
                    dimensions=dimensions,
                    time_dimensions=time_dimensions,
                    named_queries=named_queries,
                    aggregation_name=spec.aggregation_name,
                    agg_kwargs=spec.agg_kwargs,
                )
                cross_model_measures.append(cm)
                known_aliases[field_name] = cm.alias
                return cm.alias

            canonical_name = (
                f"_{spec.aggregation_name}"
                if spec.measure_name == "*"
                else f"{spec.measure_name}_{spec.aggregation_name}"
            )
            _ensure_aggregated_measure(
                alias_key=canonical_name,
                measure_name=spec.measure_name,
                aggregation_name=spec.aggregation_name,
                agg_args=spec.agg_args,
                agg_kwargs=spec.agg_kwargs,
            )
            return f"{model_name_str}.{canonical_name}"

        elif isinstance(spec, ArithmeticField):
            for mname in spec.measure_names:
                _ensure_measure_from_spec(mname, spec.agg_refs)
            alias = f"{model_name_str}.{field_name}"
            enriched_expressions.append(
                EnrichedExpression(
                    name=field_name,
                    sql=_resolve_sql(spec.sql),
                    alias=alias,
                )
            )
            known_aliases[field_name] = alias
            return alias

        elif isinstance(spec, MixedArithmeticField):
            for mname in spec.measure_names:
                _ensure_measure_from_spec(mname, spec.agg_refs)
            for placeholder, sub_transform in spec.sub_transforms:
                _flatten_spec(sub_transform, placeholder)
            alias = f"{model_name_str}.{field_name}"
            enriched_expressions.append(
                EnrichedExpression(
                    name=field_name,
                    sql=_resolve_sql(spec.sql),
                    alias=alias,
                )
            )
            known_aliases[field_name] = alias
            return alias

        elif isinstance(spec, TransformField):
            if (
                spec.transform in _SELF_JOIN_TRANSFORMS
                and isinstance(spec.inner, TransformField)
                and spec.inner.transform in _SELF_JOIN_TRANSFORMS
            ):
                raise ValueError(
                    f"Nesting '{spec.transform}' around '{spec.inner.transform}' is not supported. "
                    f"Both use self-join CTEs. Try wrapping with a window function instead "
                    f"(e.g., cumsum, lag)."
                )
            inner_name = f"_inner_{field_name}"
            if isinstance(spec.inner, AggregatedMeasureRef):
                canonical = (
                    spec.inner.aggregation_name
                    if spec.inner.measure_name == "*"
                    else f"{spec.inner.measure_name}_{spec.inner.aggregation_name}"
                )
                inner_alias = _flatten_spec(spec.inner, canonical)
            else:
                inner_alias = _flatten_spec(spec.inner, inner_name)

            offset = 1
            granularity = None
            if spec.args:
                offset = spec.args[0] if isinstance(spec.args[0], int) else 1
            if len(spec.args) >= 2:
                granularity = str(spec.args[1])
            if spec.transform in ("change", "change_pct") and not spec.args:
                offset = -1

            _add_transform(
                name=field_name,
                transform=spec.transform,
                measure_alias=inner_alias,
                offset=offset,
                granularity=granularity,
            )
            return f"{model_name_str}.{field_name}"

        raise ValueError(f"Unsupported field spec: {spec!r}")

    # Process each query field
    for qfield in query.fields or []:
        spec = parse_formula(qfield.formula)
        field_name = qfield.name or qfield.formula.replace(" ", "_").replace("/", "_div_").replace(":", "_").replace("*", "")

        if isinstance(spec, AggregatedMeasureRef):
            # New colon syntax: "revenue:sum", "*:count", etc.
            canonical_name = (
                f"_{spec.aggregation_name}"
                if spec.measure_name == "*"
                else f"{spec.measure_name}_{spec.aggregation_name}"
            )
            if field_name == qfield.formula.replace(" ", "_").replace("/", "_div_").replace(":", "_").replace("*", ""):
                field_name = canonical_name

            if "." in spec.measure_name and spec.measure_name != "*":
                # Cross-model aggregated measure
                cm = resolve_cross_model_measure(
                    spec_name=spec.measure_name,
                    field_name=field_name,
                    model=model,
                    query=query,
                    dimensions=dimensions,
                    time_dimensions=time_dimensions,
                    label=qfield.label,
                    named_queries=named_queries,
                    aggregation_name=spec.aggregation_name,
                    agg_kwargs=spec.agg_kwargs,
                )
                cross_model_measures.append(cm)
                continue

            _ensure_aggregated_measure(
                alias_key=canonical_name,
                measure_name=spec.measure_name,
                aggregation_name=spec.aggregation_name,
                agg_args=spec.agg_args,
                agg_kwargs=spec.agg_kwargs,
            )

            if spec.aggregation_name in ("first", "last") and last_agg_time_column is None:
                raise ValueError(
                    f"Aggregation '{spec.aggregation_name}' on measure '{spec.measure_name}' "
                    f"requires a time column. Add a time dimension, use an explicit arg "
                    f"(e.g., '{spec.measure_name}:{spec.aggregation_name}(time_col)'), "
                    f"or set default_time_dimension on the model."
                )
            if qfield.label:
                for m in measures:
                    if m.name == canonical_name:
                        m.label = qfield.label

        else:
            _flatten_spec(spec, field_name)
            if qfield.label:
                alias = f"{model_name_str}.{field_name}"
                for e in enriched_expressions:
                    if e.alias == alias:
                        e.label = qfield.label
                for t in enriched_transforms:
                    if t.alias == alias:
                        t.label = qfield.label

    # --- Validate model filters ---
    measure_names_set = {m.name for m in measures}
    for mf in model.filters:
        parsed_mf = parse_filter(mf)
        for col in parsed_mf.columns:
            if col in measure_names_set:
                raise ValueError(
                    f"Model filter '{mf}' references measure '{col}'. "
                    f"Model filters can only reference table columns (WHERE). "
                    f"Use query-level filters for measure conditions."
                )

    # --- Process filters ---
    all_filter_strs = list(model.filters) + list(query.filters or [])
    processed_filters = []
    ft_counter = [0]
    for f_str in all_filter_strs:
        rewritten, extra_fields = extract_filter_transforms(f_str, counter=ft_counter)
        for name, formula in extra_fields:
            spec = parse_formula(formula)
            _flatten_spec(spec, name)
        processed_filters.append(rewritten)

    has_first_or_last = any(m.aggregation in ("first", "last") for m in measures)

    # --- Resolve JOINs ---
    resolved_joins = _resolve_joins(
        model=model,
        model_name_str=model_name_str,
        dimensions=dimensions,
        time_dimensions=time_dimensions,
        measures=measures,
        cross_model_measures=cross_model_measures,
        processed_filters=processed_filters,
        named_queries=named_queries,
        resolve_join_target=resolve_join_target,
    )

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
        last_agg_time_column=last_agg_time_column if has_first_or_last else None,
        filters=classify_filters(
            filters=resolve_filter_columns(
                parsed_filters=[parse_filter(f) for f in processed_filters],
                model=model,
                model_name=model_name_str,
                resolve_join_target=resolve_join_target,
                named_queries=named_queries,
            ),
            measure_names={m.name for m in measures},
            computed_names={t.name for t in enriched_transforms} | {e.name for e in enriched_expressions},
            groupby_names={d.name for d in dimensions} | {td.name for td in time_dimensions},
        ),
        order=query.order,
        limit=query.limit,
        offset=query.offset,
    )


# ---------------------------------------------------------------------------
# Dimension / time resolution helpers
# ---------------------------------------------------------------------------


def _resolve_dimensions(
    query: SlayerQuery,
    model: SlayerModel,
    model_name_str: str,
    named_queries: dict,
    resolve_dimension_via_joins,
) -> List[EnrichedDimension]:
    dimensions = []
    for dim_ref in query.dimensions or []:
        if dim_ref.model is None:
            dim_def = model.get_dimension(dim_ref.name)
            effective_model = model.name
        else:
            parts = dim_ref.model.split(".") + [dim_ref.name]
            dim_def = resolve_dimension_via_joins(
                model=model,
                parts=parts,
                named_queries=named_queries,
            )
            effective_model = "__".join(dim_ref.model.split("."))
        dimensions.append(
            EnrichedDimension(
                name=dim_ref.name,
                sql=dim_def.sql if dim_def else None,
                type=dim_def.type if dim_def else DataType.STRING,
                alias=f"{model_name_str}.{dim_ref.full_name}",
                model_name=effective_model,
                label=dim_ref.label,
            )
        )
    return dimensions


def _resolve_time_dimensions(
    query: SlayerQuery,
    model: SlayerModel,
    model_name_str: str,
    named_queries: dict,
    resolve_dimension_via_joins,
) -> List[EnrichedTimeDimension]:
    time_dimensions = []
    for td in query.time_dimensions or []:
        if td.dimension.model is None:
            dim_def = model.get_dimension(td.dimension.name)
            td_model_name = model.name
        else:
            parts = td.dimension.model.split(".") + [td.dimension.name]
            dim_def = resolve_dimension_via_joins(
                model=model,
                parts=parts,
                named_queries=named_queries,
            )
            td_model_name = "__".join(td.dimension.model.split("."))
        time_dimensions.append(
            EnrichedTimeDimension(
                name=td.dimension.name,
                sql=dim_def.sql if dim_def else None,
                granularity=td.granularity,
                date_range=td.date_range,
                alias=f"{model_name_str}.{td.dimension.full_name}",
                model_name=td_model_name,
                label=td.label,
            )
        )
    return time_dimensions


def _resolve_time_alias(
    time_dimensions: List[EnrichedTimeDimension],
    query: SlayerQuery,
    model: SlayerModel,
) -> Optional[str]:
    if len(time_dimensions) == 1:
        return time_dimensions[0].alias
    elif len(time_dimensions) > 1:
        if query.main_time_dimension:
            return f"{model.name}.{query.main_time_dimension}"
        elif model.default_time_dimension:
            td_names = {td.name for td in time_dimensions}
            if model.default_time_dimension in td_names:
                return f"{model.name}.{model.default_time_dimension}"
    else:
        if model.default_time_dimension:
            return f"{model.name}.{model.default_time_dimension}"
    return None


def _resolve_last_agg_time(
    query: SlayerQuery,
    model: SlayerModel,
    dimensions: List[EnrichedDimension],
    time_dimensions: List[EnrichedTimeDimension],
) -> Optional[str]:
    if query.main_time_dimension:
        mtd = query.main_time_dimension
        if "." not in mtd:
            mtd = f"{model.name}.{mtd}"
        return mtd
    for d in dimensions:
        if d.type in (DataType.TIMESTAMP, DataType.DATE):
            return f"{d.model_name}.{d.sql or d.name}"
    if time_dimensions:
        td = time_dimensions[0]
        return f"{td.model_name}.{td.sql or td.name}"
    if query.filters:
        time_dim_names = {d.name for d in model.dimensions if d.type in (DataType.TIMESTAMP, DataType.DATE)}
        for f_str in query.filters or []:
            for td_name in time_dim_names:
                if td_name in f_str:
                    return f"{model.name}.{td_name}"
    if model.default_time_dimension:
        return f"{model.name}.{model.default_time_dimension}"
    return None


# ---------------------------------------------------------------------------
# JOIN resolution
# ---------------------------------------------------------------------------


def _resolve_joins(
    model: SlayerModel,
    model_name_str: str,
    dimensions: List[EnrichedDimension],
    time_dimensions: List[EnrichedTimeDimension],
    measures: List[EnrichedMeasure],
    cross_model_measures: list,
    processed_filters: List[str],
    named_queries: dict,
    resolve_join_target,
) -> List[tuple]:
    """Resolve only the JOINs the query actually needs."""
    needed_tables: Set[str] = set()
    for d in dimensions:
        if d.model_name != model.name:
            for part in d.model_name.split("__"):
                needed_tables.add(part)
    for td in time_dimensions:
        if td.model_name != model.name:
            for part in td.model_name.split("__"):
                needed_tables.add(part)
    for cm in cross_model_measures:
        needed_tables.add(cm.target_model_name)
    # Scan SQL expressions for table references
    sql_refs = [d.sql for d in dimensions] + [td.sql for td in time_dimensions] + [m.sql for m in measures]
    for sql_expr in sql_refs:
        if sql_expr and "." in sql_expr:
            for match in _TABLE_COL_RE.finditer(sql_expr):
                needed_tables.update(match.group(1).split("__"))
    # Scan filters for dotted column references
    for f_str in processed_filters:
        parsed_f = parse_filter(f_str)
        for col in parsed_f.columns:
            if "." in col:
                parts = col.split(".")
                for part in parts[:-1]:
                    needed_tables.add(part)

    # BFS transitive expansion
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

    # Build resolved joins
    resolved_joins = []
    for mj in model.joins:
        if mj.target_model not in needed_tables:
            continue
        target_info = resolve_join_target(
            target_model_name=mj.target_model,
            named_queries=named_queries,
        )
        if target_info:
            target_table, _ = target_info
        else:
            target_table = mj.target_model

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

    return resolved_joins


# ---------------------------------------------------------------------------
# Filter processing
# ---------------------------------------------------------------------------


def extract_filter_transforms(
    filter_str: str,
    counter: Optional[List[int]] = None,
) -> tuple:
    """Extract transform function calls from a filter string.

    Returns (rewritten_filter, [(name, formula), ...]) where transform
    calls are replaced with generated field names.
    """
    import ast as _ast

    from slayer.core.formula import _preprocess_agg_refs

    if counter is None:
        counter = [0]

    preprocessed = _preprocess_like(filter_str)
    # Preprocess colon syntax (e.g., "order_total:sum") into ast-safe placeholders
    preprocessed, agg_refs = _preprocess_agg_refs(preprocessed)
    # Build reverse map: placeholder → original colon form
    _agg_reverse = {
        ph: (
            f"{ref.measure_name}:{ref.aggregation_name}"
            if not ref.agg_args and not ref.agg_kwargs
            else f"{ref.measure_name}:{ref.aggregation_name}({', '.join(ref.agg_args + [f'{k}={v}' for k, v in ref.agg_kwargs.items()])})"
        )
        for ph, ref in agg_refs.items()
    }

    try:
        tree = _ast.parse(preprocessed, mode="eval")
    except SyntaxError:
        return filter_str, []

    transforms: List[tuple] = []

    def _unmangle(s: str) -> str:
        """Restore colon syntax from placeholders in unparsed formulas."""
        for ph, orig in _agg_reverse.items():
            s = s.replace(ph, orig)
        return s

    def _replace(node):
        if isinstance(node, _ast.Call) and isinstance(node.func, _ast.Name) and node.func.id in ALL_TRANSFORMS:
            name = f"_ft{counter[0]}"
            counter[0] += 1
            formula = _unmangle(_ast.unparse(node))
            transforms.append((name, formula))
            return _ast.Name(id=name, ctx=_ast.Load())
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
    return _unmangle(_ast.unparse(modified)), transforms


def resolve_filter_columns(
    parsed_filters: list,
    model: SlayerModel,
    model_name: str,
    resolve_join_target=None,
    named_queries: dict = None,
) -> list:
    """Resolve filter column references through model dimensions/measures."""
    import re as _re

    for f in parsed_filters:
        resolved_sql = f.sql
        resolved_columns = []
        for col_name in dict.fromkeys(f.columns):
            if "." not in col_name:
                dim = model.get_dimension(col_name)
                if dim:
                    sql_expr = dim.sql or col_name
                    qualified = f"{model_name}.{sql_expr}"
                    resolved_sql = _re.sub(
                        rf"(?<!\.)(?<!\w)\b{_re.escape(col_name)}\b(?!\.)",
                        qualified,
                        resolved_sql,
                    )
                    resolved_columns.append(qualified)
                else:
                    resolved_columns.append(col_name)
            else:
                parts = col_name.split(".")
                path_parts = parts[:-1]
                dim_name = parts[-1]

                # Walk the join graph
                current_model = model
                resolved = True
                for segment in path_parts:
                    target_model = None
                    for mj in current_model.joins:
                        if mj.target_model == segment:
                            target_info = (
                                resolve_join_target(
                                    target_model_name=segment,
                                    named_queries=named_queries or {},
                                )
                                if resolve_join_target
                                else None
                            )
                            if target_info:
                                _, target_model = target_info
                            break
                    if target_model is None:
                        resolved = False
                        break
                    current_model = target_model

                if resolved and current_model:
                    dim = current_model.get_dimension(dim_name)
                    if dim:
                        sql_expr = dim.sql or dim_name
                        table_alias = "__".join(path_parts)
                        qualified = f"{table_alias}.{sql_expr}"
                        resolved_sql = _re.sub(
                            rf"(?<!\w)\b{_re.escape(col_name)}\b",
                            qualified,
                            resolved_sql,
                        )
                        resolved_columns.append(qualified)
                        continue

                resolved_columns.append(col_name)

        f.sql = resolved_sql
        f.columns = resolved_columns

    return parsed_filters


def classify_filters(
    filters: list,
    measure_names: set,
    computed_names: Optional[set] = None,
    groupby_names: Optional[set] = None,
) -> list:
    """Classify filters as WHERE, HAVING, or post-filter."""
    computed_names = computed_names or set()
    groupby_names = groupby_names or set()
    for f in filters:
        if any(col in computed_names for col in f.columns):
            f.is_post_filter = True
        elif any(col in measure_names for col in f.columns):
            f.is_having = True
            for col in f.columns:
                if col not in measure_names and col not in groupby_names:
                    raise ValueError(
                        f"Filter '{f.sql}' references measure and dimension '{col}', "
                        f"but '{col}' is not in the query's dimensions or time_dimensions. "
                        f"Add it to dimensions/time_dimensions or split into separate filters."
                    )
    return filters
