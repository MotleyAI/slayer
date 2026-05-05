"""Query enrichment — resolves a SlayerQuery into an EnrichedQuery.

Converts user-facing name-based references (e.g., field="count") into fully
resolved SQL expressions, aggregation types, and model context. The result
is an EnrichedQuery ready for SQL generation.

Separated from query_engine.py for clarity — this is the largest single
transformation step in the query pipeline.
"""

import re
from typing import Dict, List, Mapping, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    DEFAULT_AGGREGATIONS_BY_TYPE,
    DataType,
    PRIMARY_KEY_AGGREGATIONS,
)
from slayer.core.formula import (
    canonical_agg_name,
    ALL_TRANSFORMS,
    AggregatedMeasureRef,
    ArithmeticField,
    MixedArithmeticField,
    TIME_TRANSFORMS,
    TransformField,
    _preprocess_like,
    _rewrite_funcstyle_aggregations,
    parse_filter,
    parse_formula,
)
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.column_expansion import _is_trivial_base, expand_derived_refs
from slayer.engine.enriched import (
    CrossModelMeasure,
    EnrichedDimension,
    EnrichedExpression,
    EnrichedMeasure,
    EnrichedQuery,
    EnrichedTimeDimension,
    EnrichedTransform,
)
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function

_SELF_JOIN_TRANSFORMS = {"time_shift"}
_TABLE_COL_RE = re.compile(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b")
def _strip_string_literal(value: str) -> str:
    """Strip one layer of single/double quotes from a query parameter value."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


_canonical_agg_name = canonical_agg_name  # Module-internal alias for the shared helper


async def _collect_reachable_agg_names(
    model: SlayerModel,
    resolve_join_target,
    named_queries: Dict,
) -> Optional[frozenset[str]]:
    """Collect custom aggregation names from the source model and all reachable joined models.

    Walks the full reachable join graph via BFS, bounded only by the ``visited``
    cycle guard (no fixed depth cap). Dotted-path resolution supports arbitrary
    depth, so the rewrite must too. Returns ``None`` when no custom aggregations
    exist anywhere.
    """
    names: set[str] = set()
    visited: set[str] = set()
    queue: list[SlayerModel] = [model]

    while queue:
        current = queue.pop(0)
        if current.name in visited:
            continue
        visited.add(current.name)

        if current.aggregations:
            names.update(a.name for a in current.aggregations)

        for join in current.joins:
            if join.target_model not in visited:
                target_info = await resolve_join_target(
                    target_model_name=join.target_model,
                    named_queries=named_queries,
                )
                if target_info:
                    _, target_model_obj = target_info
                    if target_model_obj:
                        queue.append(target_model_obj)

    return frozenset(names) if names else None


async def enrich_query(
    query: SlayerQuery,
    model: SlayerModel,
    named_queries: Optional[Dict[str, SlayerQuery]] = None,
    *,
    resolve_dimension_via_joins,
    resolve_cross_model_measure,
    resolve_join_target,
    resolve_model=None,
    dialect: str = "postgres",
) -> EnrichedQuery:
    """Resolve a SlayerQuery against model definitions into an EnrichedQuery.

    Args:
        query: The user-facing query.
        model: The resolved model definition.
        named_queries: Named sub-queries (for query lists).
        resolve_dimension_via_joins: Callback(model, parts, named_queries) ->
            (Column, SlayerModel) | None — returns the resolved column AND
            the terminal model so the SQL expander can recurse into derived
            references in ``Column.sql``. (Legacy single-value callbacks
            that return just a Column are also accepted; in that case the
            engine falls back to ``model`` as the terminal, which is fine
            for tests that pass ``_noop_async``.)
        resolve_cross_model_measure: Callback for cross-model measure refs.
        resolve_join_target: Callback(target_model_name, named_queries) -> (table_sql, model)|None
        resolve_model: Async callback ``(model_name, named_queries)`` ->
            ``SlayerModel | None``, used by the column-SQL expander to
            recursively walk join paths inside derived ``Column.sql``
            expressions. May be None in tests that don't exercise the
            expansion path.
        dialect: sqlglot dialect for parsing/emitting expanded SQL.
    """
    named_queries = named_queries or {}
    model_name_str = query.source_model if isinstance(query.source_model, str) else model.name

    # Custom aggregation names from source + all reachable joined models
    custom_agg_names = await _collect_reachable_agg_names(
        model=model,
        resolve_join_target=resolve_join_target,
        named_queries=named_queries,
    )

    # Saved-formula library for bare-name resolution. Only the source model's
    # named measures are in scope here; cross-model references (`other.aov`)
    # remain handled by the cross-model resolver.
    named_measures: Dict[str, str] = {}
    for m in model.measures:
        if not m.name:
            continue
        if m.name in named_measures:
            raise ValueError(
                f"Duplicate saved measure name '{m.name}' in model "
                f"'{model.name}'. Saved measure names must be unique."
            )
        named_measures[m.name] = m.formula

    # --- Dimensions ---
    dimensions = await _resolve_dimensions(
        query=query,
        model=model,
        model_name_str=model_name_str,
        named_queries=named_queries,
        resolve_dimension_via_joins=resolve_dimension_via_joins,
        resolve_model=resolve_model,
        dialect=dialect,
    )

    # --- Measures (populated from fields below) ---
    measures: List[EnrichedMeasure] = []

    # --- Time dimensions ---
    time_dimensions = await _resolve_time_dimensions(
        query=query,
        model=model,
        model_name_str=model_name_str,
        named_queries=named_queries,
        resolve_dimension_via_joins=resolve_dimension_via_joins,
        resolve_model=resolve_model,
        dialect=dialect,
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
    field_name_aliases: Dict[str, str] = {}

    async def _ensure_aggregated_measure(
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
        agg_kwargs = {k: _strip_string_literal(v) for k, v in (agg_kwargs or {}).items()}

        window = agg_kwargs.pop("window", None)
        window_time_alias = None
        if window is not None:
            if aggregation_name not in ("sum", "avg"):
                raise ValueError(
                    f"Aggregation parameter 'window' is only supported for sum and avg, "
                    f"not '{aggregation_name}'."
                )
            if resolved_time_alias is None:
                raise ValueError(
                    f"Windowed aggregation '{measure_name}:{aggregation_name}' requires an "
                    f"unambiguous time dimension. Add a single time_dimensions entry, or set "
                    f"main_time_dimension to select among multiple time dimensions."
                )
            window_time_alias = resolved_time_alias

        # Canonical name for the result column (colon → underscore). Includes a
        # signature suffix when args/kwargs are present so that parameterized
        # variants (e.g. percentile(p=0.5) vs percentile(p=0.95)) don't collide.
        canonical_name = _canonical_agg_name(
            measure_name=measure_name,
            aggregation_name=aggregation_name,
            agg_args=agg_args,
            agg_kwargs={**agg_kwargs, **({"window": window} if window is not None else {})},
        )

        # Skip if already ensured with this alias_key
        alias = f"{model_name_str}.{canonical_name}"
        if any(m.alias == alias for m in measures):
            known_aliases[alias_key] = alias
            return

        # Resolve column SQL
        measure_def = None
        if measure_name == "*":
            if aggregation_name != "count":
                raise ValueError(
                    f"Aggregation '{aggregation_name}' not allowed with measure '*' — use '*:count' for COUNT(*)"
                )
            sql = None
        else:
            measure_def = model.get_column(measure_name)
            if measure_def is None:
                raise ValueError(
                    f"Column '{measure_name}' not found in model '{model.name}'"
                )
            # Apply aggregation eligibility gates per the v2 contract:
            # 1. Primary-key columns are always restricted to count/count_distinct
            #    (regardless of type or any explicit whitelist).
            # 2. An explicit allowed_aggregations whitelist on a non-PK column
            #    overrides type defaults.
            # 3. Otherwise, built-in aggregations are gated by type defaults;
            #    custom model-level aggregations are allowed without further
            #    type restriction.
            if measure_def.primary_key:
                if aggregation_name not in PRIMARY_KEY_AGGREGATIONS:
                    raise ValueError(
                        f"Aggregation '{aggregation_name}' not allowed for "
                        f"primary-key column '{measure_name}'. "
                        f"Allowed: {sorted(PRIMARY_KEY_AGGREGATIONS)}"
                    )
            elif measure_def.allowed_aggregations is not None:
                if aggregation_name not in measure_def.allowed_aggregations:
                    raise ValueError(
                        f"Aggregation '{aggregation_name}' not allowed for column "
                        f"'{measure_name}'. Allowed: {measure_def.allowed_aggregations}"
                    )
            else:
                is_custom_agg = model.get_aggregation(aggregation_name) is not None
                if not is_custom_agg:
                    allowed = DEFAULT_AGGREGATIONS_BY_TYPE.get(
                        measure_def.type, frozenset()
                    )
                    if aggregation_name not in allowed:
                        raise ValueError(
                            f"Aggregation '{aggregation_name}' is not applicable to "
                            f"{measure_def.type} column '{measure_name}' in model "
                            f"'{model.name}'. Default aggregations: {sorted(allowed)}"
                        )
            sql = measure_def.sql or measure_name
            if measure_def.sql and resolve_model is not None:
                expanded_sql = await expand_derived_refs(
                    sql=measure_def.sql,
                    model=model,
                    alias_path=model_name_str,
                    resolve_model=resolve_model,
                    named_queries=named_queries,
                    dialect=dialect,
                )
                if expanded_sql is not None:
                    sql = expanded_sql

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

        # Resolve measure-level filter
        filter_sql = None
        filter_columns: List[str] = []
        if measure_def and measure_def.filter:
            parsed = parse_filter(
                measure_def.filter,
                extra_agg_names=custom_agg_names,
                named_measures=named_measures,
            )
            resolved = await resolve_filter_columns(
                parsed_filters=[parsed],
                model=model,
                model_name=model_name_str,
                resolve_join_target=resolve_join_target,
                named_queries=named_queries,
                resolve_model=resolve_model,
                dialect=dialect,
            )
            filter_sql = resolved[0].sql
            filter_columns = list(resolved[0].columns)

        measures.append(
            EnrichedMeasure(
                name=canonical_name,
                sql=sql,
                aggregation=aggregation_name,
                alias=alias,
                model_name=model_name_str,
                aggregation_def=aggregation_def,
                agg_kwargs=agg_kwargs,
                window=window,
                window_time_alias=window_time_alias,
                label=measure_def.label if measure_def else None,
                time_column=explicit_time_col,
                source_measure_name=measure_name,
                filter_sql=filter_sql,
                filter_columns=filter_columns,
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

    def _add_transform(
        name: str,
        transform: str,
        measure_alias: str,
        offset: int = 1,
        granularity: str = None,
        predicate_is_boolean: bool = False,
    ):
        needs_time = transform in TIME_TRANSFORMS
        if needs_time and resolved_time_alias is None:
            raise ValueError(
                f"Field '{name}' ({transform}) requires an unambiguous time dimension. "
                f"Add a single time_dimensions entry, or set main_time_dimension to "
                f"select among multiple time dimensions."
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
                partition_aliases=[d.alias for d in dimensions],
                predicate_is_boolean=predicate_is_boolean,
            )
        )
        known_aliases[name] = alias

    async def _ensure_measure_from_spec(mname: str, agg_refs: Optional[dict] = None):
        """Ensure a measure is resolved — handles agg refs only."""
        agg_refs = agg_refs or {}
        if mname in agg_refs:
            ref = agg_refs[mname]
            if "." in ref.measure_name and ref.measure_name != "*":
                # Cross-model aggregated measure inside an expression —
                # resolve as a CrossModelMeasure (gets its own CTE).
                cm = await resolve_cross_model_measure(
                    spec_name=ref.measure_name,
                    field_name=mname,
                    model=model,
                    query=query,
                    dimensions=dimensions,
                    time_dimensions=time_dimensions,
                    named_queries=named_queries,
                    aggregation_name=ref.aggregation_name,
                    agg_kwargs=ref.agg_kwargs,
                )
                cross_model_measures.append(cm)
                known_aliases[mname] = cm.alias
                return
            await _ensure_aggregated_measure(
                alias_key=mname,
                measure_name=ref.measure_name,
                aggregation_name=ref.aggregation_name,
                agg_args=ref.agg_args,
                agg_kwargs=ref.agg_kwargs,
            )
        else:
            raise ValueError(f"Bare measure name '{mname}' in expression is not valid. Use colon syntax.")

    async def _flatten_spec(spec, field_name: str) -> str:
        if isinstance(spec, AggregatedMeasureRef):
            if "." in spec.measure_name and spec.measure_name != "*":
                # Cross-model aggregated measure
                cm = await resolve_cross_model_measure(
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

            canonical_name = _canonical_agg_name(
                measure_name=spec.measure_name,
                aggregation_name=spec.aggregation_name,
                agg_args=spec.agg_args,
                agg_kwargs=spec.agg_kwargs,
            )
            await _ensure_aggregated_measure(
                alias_key=canonical_name,
                measure_name=spec.measure_name,
                aggregation_name=spec.aggregation_name,
                agg_args=spec.agg_args,
                agg_kwargs=spec.agg_kwargs,
            )
            return f"{model_name_str}.{canonical_name}"

        elif isinstance(spec, ArithmeticField):
            for mname in spec.measure_names:
                await _ensure_measure_from_spec(mname, spec.agg_refs)
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
                await _ensure_measure_from_spec(mname, spec.agg_refs)
            for placeholder, sub_transform in spec.sub_transforms:
                await _flatten_spec(sub_transform, placeholder)
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
            if spec.transform in ("change", "change_pct"):
                # Desugar: change(a) → a - time_shift(a, offset)
                #          change_pct(a) → CASE WHEN ts != 0 THEN (a - ts) / ts END
                if (
                    isinstance(spec.inner, TransformField)
                    and spec.inner.transform in (*_SELF_JOIN_TRANSFORMS, "change", "change_pct")
                ):
                    raise ValueError(
                        f"Nesting '{spec.transform}' around '{spec.inner.transform}' is not supported. "
                        f"Both use self-join CTEs. Try wrapping with a window function instead "
                        f"(e.g., cumsum, lag)."
                    )

                # Flatten the inner spec to get the measure alias
                inner_name = f"_inner_{field_name}"
                if isinstance(spec.inner, AggregatedMeasureRef):
                    canonical = _canonical_agg_name(
                        measure_name=spec.inner.measure_name,
                        aggregation_name=spec.inner.aggregation_name,
                        agg_args=spec.inner.agg_args,
                        agg_kwargs=spec.inner.agg_kwargs,
                    )
                    inner_alias = await _flatten_spec(spec.inner, canonical)
                else:
                    inner_alias = await _flatten_spec(spec.inner, inner_name)

                # Determine offset and granularity
                offset = -1
                granularity = None
                if spec.args:
                    offset = spec.args[0] if isinstance(spec.args[0], int) else -1
                if len(spec.args) >= 2:
                    granularity = str(spec.args[1])

                # Create hidden time_shift transform
                ts_name = f"_ts_{field_name}"
                _add_transform(
                    name=ts_name,
                    transform="time_shift",
                    measure_alias=inner_alias,
                    offset=offset,
                    granularity=granularity,
                )
                # Find the known_aliases key for the inner measure
                inner_key = next(k for k, v in known_aliases.items() if v == inner_alias)

                # Build expression
                if spec.transform == "change":
                    expr_sql = _resolve_sql(f"{inner_key} - {ts_name}")
                else:  # change_pct
                    expr_sql = _resolve_sql(
                        f"CASE WHEN {ts_name} != 0 "
                        f"THEN ({inner_key} - {ts_name}) * 1.0 / {ts_name} END"
                    )

                alias = f"{model_name_str}.{field_name}"
                enriched_expressions.append(
                    EnrichedExpression(name=field_name, sql=expr_sql, alias=alias)
                )
                known_aliases[field_name] = alias
                return alias

            # Non-change transforms (time_shift, cumsum, lag, lead, rank, last)
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
                canonical = _canonical_agg_name(
                    measure_name=spec.inner.measure_name,
                    aggregation_name=spec.inner.aggregation_name,
                    agg_args=spec.inner.agg_args,
                    agg_kwargs=spec.inner.agg_kwargs,
                )
                inner_alias = await _flatten_spec(spec.inner, canonical)
            else:
                inner_alias = await _flatten_spec(spec.inner, inner_name)

            offset = 1
            granularity = None
            if spec.args:
                offset = spec.args[0] if isinstance(spec.args[0], int) else 1
            if len(spec.args) >= 2:
                granularity = str(spec.args[1])

            # consecutive_periods (and any other transform that wraps a
            # predicate) needs to know whether the inner expression renders
            # as boolean — Postgres rejects `boolean <> integer` so the
            # numeric form `<expr> IS NOT NULL AND <expr> <> 0` cannot be
            # used for boolean inputs.
            inner_is_predicate = (
                isinstance(spec.inner, (ArithmeticField, MixedArithmeticField))
                and spec.inner.is_predicate
            )
            _add_transform(
                name=field_name,
                transform=spec.transform,
                measure_alias=inner_alias,
                offset=offset,
                granularity=granularity,
                predicate_is_boolean=inner_is_predicate,
            )
            return f"{model_name_str}.{field_name}"

        raise ValueError(f"Unsupported field spec: {spec!r}")

    # Process each query field
    for qfield in query.measures or []:
        spec = parse_formula(
            qfield.formula,
            extra_agg_names=custom_agg_names,
            named_measures=named_measures,
        )
        field_name = qfield.name or qfield.formula.replace(" ", "_").replace("/", "_div_").replace(":", "_").replace(
            "*", ""
        )

        if isinstance(spec, AggregatedMeasureRef):
            # New colon syntax: "revenue:sum", "*:count", etc.
            canonical_name = _canonical_agg_name(
                measure_name=spec.measure_name,
                aggregation_name=spec.aggregation_name,
                agg_args=spec.agg_args,
                agg_kwargs=spec.agg_kwargs,
            )
            if field_name == qfield.formula.replace(" ", "_").replace("/", "_div_").replace(":", "_").replace("*", ""):
                field_name = canonical_name

            if "." in spec.measure_name and spec.measure_name != "*":
                # Cross-model aggregated measure
                cm = await resolve_cross_model_measure(
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

            await _ensure_aggregated_measure(
                alias_key=canonical_name,
                measure_name=spec.measure_name,
                aggregation_name=spec.aggregation_name,
                agg_args=spec.agg_args,
                agg_kwargs=spec.agg_kwargs,
            )
            # When the user supplies an explicit ``name`` on the measure spec,
            # surface it as the EnrichedMeasure's name/alias so downstream
            # stages (and the wrap subquery) emit the user's chosen alias
            # instead of the canonical ``col_agg`` form. The canonical alias
            # remains resolvable via known_aliases for inline references.
            if qfield.name and qfield.name != canonical_name:
                user_alias = f"{model_name_str}.{qfield.name}"
                for m in measures:
                    if m.alias == f"{model_name_str}.{canonical_name}":
                        m.name = qfield.name
                        m.alias = user_alias
                        break
                known_aliases[qfield.name] = user_alias
                known_aliases[canonical_name] = user_alias
            # Register custom field name so ORDER BY can resolve it
            if field_name != canonical_name and canonical_name in known_aliases:
                field_name_aliases[field_name] = known_aliases[canonical_name]

            if spec.aggregation_name in ("first", "last") and last_agg_time_column is None:
                raise ValueError(
                    f"Aggregation '{spec.aggregation_name}' on measure '{spec.measure_name}' "
                    f"requires a time column. Add a time dimension, use an explicit arg "
                    f"(e.g., '{spec.measure_name}:{spec.aggregation_name}(time_col)'), "
                    f"or set default_time_dimension on the model."
                )
            if qfield.label:
                target_name = qfield.name if (qfield.name and qfield.name != canonical_name) else canonical_name
                for m in measures:
                    if m.name == target_name:
                        m.label = qfield.label

        else:
            await _flatten_spec(spec, field_name)
            if qfield.label:
                alias = f"{model_name_str}.{field_name}"
                for e in enriched_expressions:
                    if e.alias == alias:
                        e.label = qfield.label
                for t in enriched_transforms:
                    if t.alias == alias:
                        t.label = qfield.label

    # --- Enrich ORDER BY formulas as hidden fields ---
    for item in query.order or []:
        if not item.raw_formula:
            continue
        spec = parse_formula(
            item.raw_formula,
            extra_agg_names=custom_agg_names,
            named_measures=named_measures,
        )
        if isinstance(spec, AggregatedMeasureRef):
            canonical = _canonical_agg_name(
                measure_name=spec.measure_name,
                aggregation_name=spec.aggregation_name,
                agg_args=spec.agg_args,
                agg_kwargs=spec.agg_kwargs,
            )
        else:
            canonical = item.raw_formula.replace(" ", "_").replace("/", "_div_").replace(
                ":", "_"
            ).replace("*", "").replace("(", "_").replace(")", "").replace(",", "_")
        # Only enrich if not already present from fields
        if canonical not in known_aliases:
            await _flatten_spec(spec, canonical)
        item.column.name = canonical

    # --- Validate model filters ---
    measure_names_set = {m.name for m in measures}
    for mf in model.filters:
        parsed_mf = parse_filter(mf, extra_agg_names=custom_agg_names)
        for col in parsed_mf.columns:
            if col in measure_names_set:
                raise ValueError(
                    f"Model filter '{mf}' references measure '{col}'. "
                    f"Model filters can only reference table columns (WHERE). "
                    f"Use query-level filters for measure conditions."
                )

    # --- Process filters ---
    # Apply variable substitution to query-level filters (not model-level)
    query_filters = list(query.filters or [])
    if query.variables and query_filters:
        from slayer.core.query import substitute_variables

        query_filters = [
            substitute_variables(filter_str=f, variables=query.variables) for f in query_filters
        ]

    all_filter_strs = list(model.filters) + query_filters
    processed_filters = []
    ft_counter = [0]
    for f_str in all_filter_strs:
        rewritten, extra_fields = extract_filter_transforms(
            f_str, counter=ft_counter, extra_agg_names=custom_agg_names,
            named_measures=named_measures,
        )
        for name, formula in extra_fields:
            spec = parse_formula(
                formula,
                extra_agg_names=custom_agg_names,
                named_measures=named_measures,
            )
            await _flatten_spec(spec, name)
        processed_filters.append(rewritten)

    has_first_or_last = any(m.aggregation in ("first", "last") for m in measures)

    # --- DEV-1336: detect filters that resolve to a model column whose `sql`
    # contains a window function. Such columns must be materialized in the base
    # SELECT (not inlined into WHERE) and the predicate applied as a post-filter
    # on the alias.
    windowed_column_names: Set[str] = {
        c.name for c in model.columns if c.sql and has_window_function(c.sql)
    }

    # --- Resolve JOINs ---
    resolved_joins = await _resolve_joins(
        model=model,
        model_name_str=model_name_str,
        dimensions=dimensions,
        time_dimensions=time_dimensions,
        measures=measures,
        cross_model_measures=cross_model_measures,
        processed_filters=processed_filters,
        named_queries=named_queries,
        resolve_join_target=resolve_join_target,
        extra_agg_names=custom_agg_names,
        dialect=dialect,
    )

    parsed_filters = await resolve_filter_columns(
        parsed_filters=[parse_filter(f, extra_agg_names=custom_agg_names) for f in processed_filters],
        model=model,
        model_name=model_name_str,
        resolve_join_target=resolve_join_target,
        named_queries=named_queries,
        windowed_column_names=windowed_column_names,
        resolve_model=resolve_model,
        dialect=dialect,
    )

    # Materialize each windowed Column referenced by any filter. Each becomes a
    # SELECT-only entry in the base CTE that the post-filter wrap targets.
    referenced_windowed: Dict[str, EnrichedDimension] = {}
    for pf in parsed_filters:
        for col_name in pf.columns:
            short = col_name.split(".")[-1]
            if short in windowed_column_names and short not in referenced_windowed:
                col_def = model.get_column(short)
                if col_def is None or col_def.sql is None:
                    continue
                referenced_windowed[short] = EnrichedDimension(
                    name=short,
                    sql=col_def.sql,
                    type=col_def.type,
                    alias=f"{model_name_str}.{short}",
                    model_name=model_name_str,
                    label=col_def.label,
                    format=col_def.format,
                )
    windowed_filter_columns = list(referenced_windowed.values())

    return EnrichedQuery(
        model_name=model_name_str,
        sql_table=model.sql_table,
        sql=model.sql,
        resolved_joins=resolved_joins,
        dimensions=dimensions,
        measures=measures,
        time_dimensions=time_dimensions,
        expressions=enriched_expressions,
        transforms=enriched_transforms,
        windowed_filter_columns=windowed_filter_columns,
        cross_model_measures=cross_model_measures,
        last_agg_time_column=last_agg_time_column if has_first_or_last else None,
        filters=classify_filters(
            filters=parsed_filters,
            measure_names={m.name for m in measures},
            computed_names=(
                {t.name for t in enriched_transforms}
                | {e.name for e in enriched_expressions}
                | {c.name for c in windowed_filter_columns}
            ),
            groupby_names={d.name for d in dimensions} | {td.name for td in time_dimensions},
            windowed_measure_names={m.name for m in measures if m.window},
        ),
        order=query.order,
        limit=query.limit,
        offset=query.offset,
        field_name_aliases=field_name_aliases,
    )


# ---------------------------------------------------------------------------
# Dimension / time resolution helpers
# ---------------------------------------------------------------------------


def _unpack_dim_resolution(result):
    """Accept either ``Column`` or ``(Column, SlayerModel)`` from
    ``resolve_dimension_via_joins`` so legacy test callbacks (which return a
    plain Column or None) keep working alongside the engine's tuple form.
    """
    if result is None:
        return None, None
    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


async def _maybe_expand(
    *,
    sql: Optional[str],
    terminal_model: Optional[SlayerModel],
    fallback_model: SlayerModel,
    alias_path: str,
    resolve_model,
    named_queries: dict,
    dialect: str,
    is_root: bool = True,
) -> Optional[str]:
    """Run the column-SQL expander when we have what we need; otherwise
    return ``sql`` unchanged. Lets tests that don't supply ``resolve_model``
    keep getting the legacy unexpanded behavior — production always supplies
    it via the engine.

    ``is_root=False`` for cross-model dims/measures: the source has been
    reached via a join path, so any further walks inside its derived
    Column.sql must prefix the alias path (closes PR #89 alias-prefix bug).
    """
    if not sql or resolve_model is None:
        return sql
    return await expand_derived_refs(
        sql=sql,
        model=terminal_model or fallback_model,
        alias_path=alias_path,
        resolve_model=resolve_model,
        named_queries=named_queries,
        dialect=dialect,
        is_root=is_root,
    )


async def _resolve_dimensions(
    query: SlayerQuery,
    model: SlayerModel,
    model_name_str: str,
    named_queries: dict,
    resolve_dimension_via_joins,
    resolve_model=None,
    dialect: str = "postgres",
) -> List[EnrichedDimension]:
    dimensions = []
    for dim_ref in query.dimensions or []:
        terminal_model: Optional[SlayerModel] = None
        is_local = dim_ref.model is None
        if is_local:
            dim_def = model.get_column(dim_ref.name)
            effective_model = model_name_str
            terminal_model = model
        else:
            parts = dim_ref.model.split(".") + [dim_ref.name]
            raw = await resolve_dimension_via_joins(
                model=model,
                parts=parts,
                named_queries=named_queries,
            )
            dim_def, terminal_model = _unpack_dim_resolution(raw)
            effective_model = "__".join(dim_ref.model.split("."))
        expanded_sql = await _maybe_expand(
            sql=dim_def.sql if dim_def else None,
            terminal_model=terminal_model,
            fallback_model=model,
            alias_path=effective_model,
            resolve_model=resolve_model,
            named_queries=named_queries,
            dialect=dialect,
            is_root=is_local,
        )
        dimensions.append(
            EnrichedDimension(
                name=dim_ref.name,
                sql=expanded_sql,
                type=dim_def.type if dim_def else DataType.STRING,
                alias=f"{model_name_str}.{dim_ref.full_name}",
                model_name=effective_model,
                label=dim_ref.label or (dim_def.label if dim_def else None),
                format=dim_def.format if dim_def else None,
            )
        )
    return dimensions


async def _resolve_time_dimensions(
    query: SlayerQuery,
    model: SlayerModel,
    model_name_str: str,
    named_queries: dict,
    resolve_dimension_via_joins,
    resolve_model=None,
    dialect: str = "postgres",
) -> List[EnrichedTimeDimension]:
    time_dimensions = []
    for td in query.time_dimensions or []:
        terminal_model: Optional[SlayerModel] = None
        is_local = td.dimension.model is None
        if is_local:
            dim_def = model.get_column(td.dimension.name)
            td_model_name = model_name_str
            terminal_model = model
        else:
            parts = td.dimension.model.split(".") + [td.dimension.name]
            raw = await resolve_dimension_via_joins(
                model=model,
                parts=parts,
                named_queries=named_queries,
            )
            dim_def, terminal_model = _unpack_dim_resolution(raw)
            td_model_name = "__".join(td.dimension.model.split("."))
        expanded_sql = await _maybe_expand(
            sql=dim_def.sql if dim_def else None,
            terminal_model=terminal_model,
            fallback_model=model,
            alias_path=td_model_name,
            resolve_model=resolve_model,
            named_queries=named_queries,
            dialect=dialect,
            is_root=is_local,
        )
        time_dimensions.append(
            EnrichedTimeDimension(
                name=td.dimension.name,
                sql=expanded_sql,
                granularity=td.granularity,
                date_range=td.date_range,
                alias=f"{model_name_str}.{td.dimension.full_name}",
                model_name=td_model_name,
                label=td.label or (dim_def.label if dim_def else None),
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
    # No fallback to default_time_dimension without explicit time_dimensions —
    # transforms require a time_dimensions entry so the column is in the base CTE.
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

    def _qualified(model_name: str, sql: Optional[str], name: str) -> str:
        # Once derived-ref expansion has run, `sql` may already be qualified
        # (e.g. ``orders.created_at`` instead of bare ``created_at``); don't
        # double-prefix in that case.
        expr = sql or name
        if "." in expr:
            return expr
        return f"{model_name}.{expr}"

    for d in dimensions:
        if d.type in (DataType.TIMESTAMP, DataType.DATE):
            return _qualified(d.model_name, d.sql, d.name)
    if time_dimensions:
        td = time_dimensions[0]
        return _qualified(td.model_name, td.sql, td.name)
    if query.filters:
        time_dim_names = {c.name for c in model.columns if c.type in (DataType.TIMESTAMP, DataType.DATE)}
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


def _add_with_prefixes(segments: List[str], paths: Set[Tuple[str, ...]]) -> None:
    """Add ``segments[:1], segments[:2], …, segments`` to ``paths``."""
    for i in range(1, len(segments) + 1):
        paths.add(tuple(segments[:i]))


def _raise_column_cycle(
    visited: Tuple[Tuple[str, str], ...], key: Tuple[str, str],
) -> None:
    """Raise a deterministic ``Circular column reference`` error matching
    the chain format used by ``expand_derived_refs``.
    """
    cycle_start = visited.index(key)
    cycle = (*visited[cycle_start:], key)
    chain = " → ".join(f"{m}.{c}" for m, c in cycle)
    raise ValueError(f"Circular column reference detected: {chain}")


def _scan_sql_table_refs(*, sql: str, model_name: str, paths: Set[Tuple[str, ...]]) -> None:
    """Regex-fallback scan: pick out ``<table>.<col>`` shapes and add the
    table prefix paths (skipping references to ``model_name`` itself).
    """
    for match in _TABLE_COL_RE.finditer(sql):
        segments = match.group(1).split("__")
        if segments and segments[0] != model_name:
            _add_with_prefixes(segments, paths)


def _process_node_for_paths(
    *,
    node: exp.Column,
    model: SlayerModel,
    paths: Set[Tuple[str, ...]],
    visited: Tuple[Tuple[str, str], ...],
    dialect: Optional[str] = None,
) -> None:
    """Resolve one ``exp.Column`` node into either a recursion into a
    local derived column or a join-path-prefix add.

    Branches:
    - multi-part qualifier (catalog/db) → ignore (outside SLayer's contract)
    - bare identifier → recurse into a possibly-derived local column
    - ``<source_model>.<col>`` → self-qualified local ref, recurse
    - ``<table>.<col>`` (table not the source model) → add the prefix path
    """
    if node.args.get("db") or node.args.get("catalog"):
        return
    table_id = node.args.get("table")
    if table_id is None:
        _collect_paths_from_local_column_chain(
            model=model, col_name=node.name, paths=paths,
            visited=visited, dialect=dialect,
        )
        return
    segments = table_id.name.split("__")
    if not segments:
        return
    if segments[0] == model.name:
        _collect_paths_from_local_column_chain(
            model=model, col_name=node.name, paths=paths,
            visited=visited, dialect=dialect,
        )
        return
    _add_with_prefixes(segments, paths)


def _collect_paths_from_local_column_chain(
    *,
    model: SlayerModel,
    col_name: str,
    paths: Set[Tuple[str, ...]],
    visited: Tuple[Tuple[str, str], ...] = (),
    dialect: Optional[str] = None,
) -> None:
    """Walk the SQL of a *local* derived column on ``model`` to discover
    the join paths its expression implies — recursing through references
    to other derived columns on the same model.

    Closes DEV-1334. ``_collect_needed_paths`` previously only saw cross-
    table aliases that already appeared verbatim in the *parsed-out filter
    columns* (dotted refs like ``customers.region``). When a filter
    referenced a *bare-named* derived column (e.g. ``is_eu = 1`` where
    ``is_eu.sql`` references ``customers.region``), the chain was never
    walked and the join was silently dropped. This helper closes that
    gap by inspecting the column's SQL body.

    ``dialect`` is the active sqlglot dialect — passed to ``parse_one`` so
    dialect-specific syntax in derived ``Column.sql`` parses correctly
    (PR #96 review).
    """
    col = model.get_column(col_name)
    if col is None or _is_trivial_base(column=col):
        return
    sql = col.sql or ""
    if not sql:
        return
    key = (model.name, col_name)
    if key in visited:
        _raise_column_cycle(visited, key)
    next_visited = (*visited, key)

    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        _scan_sql_table_refs(sql=sql, model_name=model.name, paths=paths)
        return

    for node in parsed.find_all(exp.Column):
        _process_node_for_paths(
            node=node, model=model, paths=paths,
            visited=next_visited, dialect=dialect,
        )


def _collect_needed_paths(
    model: SlayerModel,
    dimensions: List[EnrichedDimension],
    time_dimensions: List[EnrichedTimeDimension],
    measures: List[EnrichedMeasure],
    cross_model_measures: list,
    processed_filters: List[str],
    extra_agg_names: Optional[frozenset] = None,
    dialect: Optional[str] = None,
) -> Set[Tuple[str, ...]]:
    """Extract ordered join-path tuples the query needs (including all prefixes)."""
    paths: Set[Tuple[str, ...]] = set()

    for d in dimensions:
        if d.model_name != model.name:
            _add_with_prefixes(d.model_name.split("__"), paths)
    for td in time_dimensions:
        if td.model_name != model.name:
            _add_with_prefixes(td.model_name.split("__"), paths)
    for cm in cross_model_measures:
        paths.add((cm.target_model_name,))

    # Scan SQL expressions for __-delimited table references
    sql_refs = [d.sql for d in dimensions] + [td.sql for td in time_dimensions] + [m.sql for m in measures]
    for sql_expr in sql_refs:
        if sql_expr and "." in sql_expr:
            for match in _TABLE_COL_RE.finditer(sql_expr):
                _add_with_prefixes(match.group(1).split("__"), paths)

    # Scan filters for column references — dotted refs add their join
    # path directly; bare-name refs to derived local columns trigger a
    # walk of the column's SQL chain (DEV-1334).
    for f_str in processed_filters:
        parsed_f = parse_filter(f_str, extra_agg_names=extra_agg_names)
        for col in parsed_f.columns:
            _scan_filter_column_ref(model=model, col=col, paths=paths, dialect=dialect)

    # Scan measure filter columns. For column-level ``filter=`` attributes
    # ``resolve_filter_columns`` may store the fully-expanded SQL fragment
    # rather than the original column name (when the filter references a
    # bare-named derived column whose own sql is non-trivial — DEV-1334).
    # ``_scan_filter_column_ref`` distinguishes the three shapes (bare name,
    # dotted ref, expanded SQL) and routes each accordingly.
    for m in measures:
        for col in m.filter_columns:
            _scan_filter_column_ref(model=model, col=col, paths=paths, dialect=dialect)

    return paths


def _scan_filter_column_ref(
    *,
    model: SlayerModel,
    col: str,
    paths: Set[Tuple[str, ...]],
    dialect: Optional[str] = None,
) -> None:
    """Route one entry from a parsed filter's column list to the right
    path-discovery branch.

    Three shapes occur:
    - **bare name** (``"is_eu"``): a reference to a local column. Walk
      the column's SQL chain to find any cross-table refs it implies.
    - **identifier dotted ref** (``"customers.region"``,
      ``"customers.regions.name"``): a join-path-qualified reference —
      add the prefix path directly.
    - **expanded SQL fragment** (``"CASE WHEN customers.region = 'EU' …"``):
      arises when ``resolve_filter_columns`` stores the inlined SQL of a
      bare-name reference to a derived column. Scan via the same
      ``_TABLE_COL_RE`` regex that handles ``EnrichedMeasure.sql``.
    """
    if "." not in col:
        _collect_paths_from_local_column_chain(
            model=model, col_name=col, paths=paths, dialect=dialect,
        )
        return
    if _looks_like_dotted_identifier_ref(col):
        parts = col.split(".")
        expanded: List[str] = []
        for part in parts[:-1]:
            # Model filters convert dots to __; expand both forms.
            expanded.extend(part.split("__"))
        if expanded:
            _add_with_prefixes(expanded, paths)
        return
    # Expanded SQL fragment.
    for match in _TABLE_COL_RE.finditer(col):
        table_alias = match.group(1)
        segments = table_alias.split("__")
        if segments and segments[0] != model.name:
            _add_with_prefixes(segments, paths)


_DOTTED_IDENT_REF_RE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)+$")


def _looks_like_dotted_identifier_ref(value: str) -> bool:
    """True iff ``value`` is a chain of ``.``-joined identifiers — e.g.
    ``customers.region``, ``customers.regions.name``. False for SQL
    fragments containing parens, spaces, operators, or quotes.
    """
    return bool(_DOTTED_IDENT_REF_RE.match(value))


async def _resolve_joins(
    model: SlayerModel,
    model_name_str: str,
    dimensions: List[EnrichedDimension],
    time_dimensions: List[EnrichedTimeDimension],
    measures: List[EnrichedMeasure],
    cross_model_measures: list,
    processed_filters: List[str],
    named_queries: dict,
    resolve_join_target,
    extra_agg_names: Optional[frozenset] = None,
    dialect: Optional[str] = None,
) -> List[tuple]:
    """Resolve only the JOINs the query actually needs by walking the join graph.

    Instead of relying on baked-in multi-hop joins, this walks each intermediate
    model's own direct joins hop-by-hop to build the complete chain.

    ``dialect`` is the active sqlglot dialect; it propagates into the
    derived-column SQL parser used to discover join paths (PR #96 review).
    """
    needed_paths = _collect_needed_paths(
        model=model,
        dimensions=dimensions,
        time_dimensions=time_dimensions,
        measures=measures,
        cross_model_measures=cross_model_measures,
        processed_filters=processed_filters,
        extra_agg_names=extra_agg_names,
        dialect=dialect,
    )
    if not needed_paths:
        return []

    # Sort shorter paths first so prefixes are resolved before extensions
    sorted_paths = sorted(needed_paths, key=len)

    resolved_joins: Dict[str, tuple] = {}  # alias -> (table_sql, alias, condition)
    resolved_models: Dict[str, SlayerModel] = {}  # model_name -> SlayerModel

    for path in sorted_paths:
        alias = "__".join(path)
        if alias in resolved_joins:
            continue

        current_model = model
        current_alias = model_name_str

        for i, segment in enumerate(path):
            hop_alias = "__".join(path[: i + 1])
            if hop_alias in resolved_joins:
                # Already resolved from a previous path prefix — advance
                if segment in resolved_models:
                    current_model = resolved_models[segment]
                current_alias = hop_alias
                continue

            # Find a direct join on the current model
            join = None
            for j in current_model.joins:
                if j.target_model == segment:
                    join = j
                    break

            if join is None:
                break  # No join found — remaining hops unresolvable

            # Resolve the target model
            target_info = await resolve_join_target(
                target_model_name=segment,
                named_queries=named_queries,
            )
            if target_info:
                target_table, target_model_obj = target_info
            else:
                target_table = segment
                target_model_obj = None

            if target_model_obj:
                resolved_models[segment] = target_model_obj

            # Build join condition
            join_conds = []
            for src_col, tgt_col in join.join_pairs:
                join_conds.append(f"{current_alias}.{src_col} = {hop_alias}.{tgt_col}")

            resolved_joins[hop_alias] = (target_table, hop_alias, " AND ".join(join_conds), str(join.join_type))

            # Advance to the resolved model for the next hop
            if target_model_obj:
                current_model = target_model_obj
            current_alias = hop_alias

    return list(resolved_joins.values())




# ---------------------------------------------------------------------------
# Filter processing
# ---------------------------------------------------------------------------


def extract_filter_transforms(
    filter_str: str,
    counter: Optional[List[int]] = None,
    extra_agg_names: Optional[frozenset[str]] = None,
    named_measures: Optional[Mapping[str, str]] = None,
) -> tuple:
    """Extract transform function calls from a filter string.

    Returns (rewritten_filter, [(name, formula), ...]) where transform
    calls are replaced with generated field names.

    Bare references to ``named_measures`` keys are inline-expanded before
    transform extraction so that filters like ``change(aov) > 0`` work when
    ``aov`` is a saved formula.
    """
    import ast as _ast

    from slayer.core.formula import _expand_named_measures, _preprocess_agg_refs

    if counter is None:
        counter = [0]

    if named_measures:
        filter_str = _expand_named_measures(filter_str, named_measures)
    # DEV-1336: reject raw window-function syntax (`OVER (...)`) before AST parsing.
    # Without this, the AST parser fails on `over` and falls through to a
    # confusing "Invalid filter syntax" error from parse_filter; here we surface
    # a helpful error that points at SLayer's transforms / Column.sql.
    if has_window_function(filter_str):
        raise ValueError(f"Filter '{filter_str}' {WINDOW_IN_FILTER_ERROR}")
    preprocessed = _rewrite_funcstyle_aggregations(filter_str, extra_agg_names)
    funcstyle_rewritten = preprocessed  # capture after funcstyle rewrite, before further preprocessing
    preprocessed = _preprocess_like(preprocessed)
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
        return funcstyle_rewritten, []
    return _unmangle(_ast.unparse(modified)), transforms


async def resolve_filter_columns(
    parsed_filters: list,
    model: SlayerModel,
    model_name: str,
    resolve_join_target=None,
    named_queries: dict = None,
    windowed_column_names: Optional[Set[str]] = None,
    resolve_model=None,
    dialect: str = "postgres",
) -> list:
    """Resolve filter column references through model dimensions/measures.

    For ``Column`` entries whose ``sql`` contains a window function
    (``windowed_column_names``), do NOT inline the SQL. Instead emit the
    canonical ``{model}.{name}`` alias so the filter can be applied as a
    post-filter on the materialized base-CTE alias (DEV-1336).

    When ``resolve_model`` is supplied, derived ``Column.sql`` expressions
    are recursively expanded so chained derivations (cross-model or local)
    yield fully-qualified physical-table SQL inside WHERE clauses.
    """
    import re as _re

    windowed_column_names = windowed_column_names or set()

    async def _expanded_sql_expr(*, sql_expr: str, owning_model: SlayerModel,
                                 alias_path: str, is_root: bool) -> str:
        """Expand derived references inside a filter's resolved SQL fragment."""
        if resolve_model is None:
            return sql_expr
        expanded = await expand_derived_refs(
            sql=sql_expr,
            model=owning_model,
            alias_path=alias_path,
            resolve_model=resolve_model,
            named_queries=named_queries or {},
            dialect=dialect,
            is_root=is_root,
        )
        return expanded if expanded is not None else sql_expr

    for f in parsed_filters:
        resolved_sql = f.sql
        resolved_columns = []
        for col_name in dict.fromkeys(f.columns):
            if "." not in col_name:
                if col_name in windowed_column_names:
                    qualified = f"{model_name}.{col_name}"
                    resolved_sql = _re.sub(
                        rf"(?<!\.)(?<!\w)\b{_re.escape(col_name)}\b(?!\.)",
                        qualified,
                        resolved_sql,
                    )
                    resolved_columns.append(col_name)
                    continue
                dim = model.get_column(col_name)
                if dim:
                    sql_expr = dim.sql or col_name
                    if sql_expr.isidentifier():
                        qualified = f"{model_name}.{sql_expr}"
                    else:
                        qualified = await _expanded_sql_expr(
                            sql_expr=sql_expr,
                            owning_model=model,
                            alias_path=model_name,
                            is_root=True,
                        )
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
                                await resolve_join_target(
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
                    dim = current_model.get_column(dim_name)
                    if dim:
                        sql_expr = dim.sql or dim_name
                        table_alias = "__".join(path_parts)
                        if sql_expr.isidentifier():
                            qualified = f"{table_alias}.{sql_expr}"
                        else:
                            qualified = await _expanded_sql_expr(
                                sql_expr=sql_expr,
                                owning_model=current_model,
                                alias_path=table_alias,
                                is_root=False,
                            )
                        resolved_sql = _re.sub(
                            rf"(?<!\w)\b{_re.escape(col_name)}\b",
                            qualified,
                            resolved_sql,
                        )
                        # Keep the original dotted path in resolved_columns
                        # so _collect_needed_paths picks up the join requirement,
                        # even when sql_expr is a constant (e.g., "1").
                        resolved_columns.append(col_name)
                        continue

                resolved_columns.append(col_name)

        f.sql = resolved_sql
        f.columns = resolved_columns

    return parsed_filters


def _classify_one_filter(
    f,
    *,
    measure_names: set,
    computed_names: set,
    groupby_names: set,
    windowed_measure_names: set,
) -> None:
    """Mutate one ParsedFilter to set is_post_filter / is_having flags.

    Order matters: post-filter classifications take precedence (computed
    columns can only be referenced after the base aggregate is built); then
    windowed-measure refs (their value lives in a downstream CTE so they
    can't be HAVING); then plain non-windowed measure HAVING.
    """
    if any(col in computed_names for col in f.columns):
        f.is_post_filter = True
        return
    if any(col in windowed_measure_names for col in f.columns):
        f.is_post_filter = True
        return
    if any(col in measure_names for col in f.columns):
        f.is_having = True
        for col in f.columns:
            if col not in measure_names and col not in groupby_names:
                raise ValueError(
                    f"Filter '{f.sql}' references measure and dimension '{col}', "
                    f"but '{col}' is not in the query's dimensions or time_dimensions. "
                    f"Add it to dimensions/time_dimensions or split into separate filters."
                )


def classify_filters(
    filters: list,
    measure_names: set,
    computed_names: Optional[set] = None,
    groupby_names: Optional[set] = None,
    windowed_measure_names: Optional[set] = None,
) -> list:
    """Classify filters as WHERE, HAVING, or post-filter.

    Delegates per-filter classification to `_classify_one_filter` so this
    function stays a flat for-loop.
    """
    computed_names = computed_names or set()
    groupby_names = groupby_names or set()
    windowed_measure_names = windowed_measure_names or set()
    for f in filters:
        _classify_one_filter(
            f,
            measure_names=measure_names,
            computed_names=computed_names,
            groupby_names=groupby_names,
            windowed_measure_names=windowed_measure_names,
        )
    return filters
