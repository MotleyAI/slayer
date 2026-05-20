"""Query enrichment — resolves a SlayerQuery into an EnrichedQuery.

Converts user-facing name-based references (e.g., field="count") into fully
resolved SQL expressions, aggregation types, and model context. The result
is an EnrichedQuery ready for SQL generation.

Separated from query_engine.py for clarity — this is the largest single
transformation step in the query pipeline.
"""

import re
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

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
    ParsedFilter,
    RANK_FAMILY_TRANSFORMS,
    TIME_TRANSFORMS,
    TransformField,
    _preprocess_like,
    _rewrite_funcstyle_aggregations,
    parse_filter,
    parse_formula,
)
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.refs import DOTTED_IDENT_REF_RE as _DOTTED_IDENT_REF_RE
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
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function

_SELF_JOIN_TRANSFORMS = {"time_shift"}
_TABLE_COL_RE = re.compile(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b")
def _strip_string_literal(value: str) -> str:
    """Strip one layer of single/double quotes from a query parameter value."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


_canonical_agg_name = canonical_agg_name  # Module-internal alias for the shared helper


def canonical_expression_key(node: Any) -> Tuple[Any, ...]:  # NOSONAR(S8495) — variable-length tuple shape IS the discriminator; type signature already declares Tuple[Any, ...]
    """DEV-1444: build an alias-independent, structural hash key for a
    parsed formula AST node.

    Two formulas that are *structurally equal* — same aggregation, same
    column, same transform stack, same args / kwargs (order-insensitive) —
    yield identical keys regardless of any user ``name`` override.

    Consumed by the provenance-merge step in enrichment: when an
    auto-extracted entry (order-by aggregate, filter-extracted hidden
    field, window-arg hoist) shares a key with an already-user-declared
    entry, the entries collapse to the declared one — preventing phantom
    ``orders.revenue_sum`` columns alongside a user-declared
    ``{"formula":"revenue:sum","name":"total"}``.
    """
    if isinstance(node, AggregatedMeasureRef):
        return (
            "agg",
            node.measure_name,
            node.aggregation_name,
            tuple(node.agg_args),
            tuple(sorted(node.agg_kwargs.items())),
        )
    if isinstance(node, TransformField):
        return (
            "transform",
            node.transform,
            canonical_expression_key(node.inner),
            tuple(node.args),
            tuple(sorted((k, str(v)) for k, v in node.kwargs.items())),
        )
    if isinstance(node, (ArithmeticField, MixedArithmeticField)):
        # Arithmetic structural keys: the preprocessed SQL string with
        # placeholders, plus the sorted set of inner agg-ref keys so
        # ``a+b`` and ``b+a`` produce the same key when the underlying
        # placeholders are interchangeable.
        agg_keys = tuple(sorted(
            canonical_expression_key(ref) for ref in node.agg_refs.values()
        ))
        return ("arith", node.sql, agg_keys)
    # Fallback: stringify so downstream callers always get a hashable.
    return ("raw", repr(node))


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
    drop_unreachable_filters: bool = False,
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

    # DEV-1444: a query that lists the same column as both a regular
    # dimension and a time dimension produces ambiguous aliases (same
    # ``<model>.<col>`` key in both EnrichedDimension and
    # EnrichedTimeDimension). Reject up-front with a clear error rather
    # than silently picking one.
    dim_alias_set = {d.alias for d in dimensions}
    for td in time_dimensions:
        if td.alias in dim_alias_set:
            raise ValueError(
                f"Column {td.alias!r} appears in both `dimensions` and "
                f"`time_dimensions` — ambiguous projection. Use one or the other."
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
    # DEV-1443: canonical-agg alias → user-supplied measure name. Populated
    # when a query measure renames the canonical (``{"formula": "col:agg",
    # "name": "alias"}``). Consumed by the filter pre-pass (so a filter
    # written as ``col:agg <op> N`` resolves to the user alias and HAVINGs
    # correctly) and by the ORDER BY enrichment (same shape).
    canonical_to_user_name: Dict[str, str] = {}
    # Cached source-column name set for the remap eligibility guard
    # (Codex Finding 1 — skip remap when the canonical alias also literally
    # names a source column on the model, since the regex sub would then
    # clobber the literal source-column reference).
    _source_column_names: Set[str] = {c.name for c in model.columns}

    # DEV-1444 provenance-merge index: canonical_expression_key →
    # surfaced alias. Populated when an EnrichedMeasure is created;
    # consulted by ``_ensure_aggregated_measure`` so an auto-extracted
    # ref whose canonical form matches an already-declared measure
    # (e.g. order-by ``revenue:sum`` matching a user-declared
    # ``{"formula":"revenue:sum","name":"total"}``) reuses the existing
    # alias instead of materialising a phantom ``orders.revenue_sum``.
    measure_canonical_key_to_alias: Dict[Tuple[Any, ...], str] = {}

    def _mark_user_declared(alias: str) -> bool:
        """DEV-1444: flip ``user_declared=True`` on the enriched entry that
        owns ``alias``. The entry could live in any of measures, expressions,
        transforms, or cross_model_measures. Returns True iff an entry was
        found — callers use that to detect missing wiring.
        """
        for m in measures:
            if m.alias == alias:
                m.user_declared = True
                return True
        for e in enriched_expressions:
            if e.alias == alias:
                e.user_declared = True
                return True
        for t in enriched_transforms:
            if t.alias == alias:
                t.user_declared = True
                return True
        for cm in cross_model_measures:
            if cm.alias == alias:
                cm.user_declared = True
                return True
        return False

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

        # DEV-1444 provenance merge: structural key collapsed across user
        # ``name`` overrides. If a previous call already created an
        # EnrichedMeasure for the same canonical form (e.g. a
        # user-declared ``{"formula":"revenue:sum","name":"total"}``),
        # reuse its surfaced alias and skip the duplicate hoist.
        merged_kwargs = {
            **agg_kwargs,
            **({"window": window} if window is not None else {}),
        }
        canon_key = (
            "agg",
            measure_name,
            aggregation_name,
            tuple(agg_args),
            tuple(sorted(merged_kwargs.items())),
        )
        existing_alias = measure_canonical_key_to_alias.get(canon_key)
        if existing_alias is not None:
            known_aliases[alias_key] = existing_alias
            return

        # Skip if already ensured with this alias_key
        alias = f"{model_name_str}.{canonical_name}"
        if any(m.alias == alias for m in measures):
            known_aliases[alias_key] = alias
            measure_canonical_key_to_alias[canon_key] = alias
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

        # Resolve measure-level filter. ``Column.filter`` is Mode A SQL
        # (DEV-1369 / DEV-1378): arbitrary SQL function calls
        # (``json_extract``, ``coalesce``, ``CASE WHEN``, dialect-specific
        # operators) flow through; DSL constructs (aggregation colon
        # syntax, transform calls, ``OVER``) were rejected at construction
        # by ``parse_sql_predicate``.
        filter_sql = None
        filter_columns: List[str] = []
        if measure_def and measure_def.filter:
            parsed = parse_sql_predicate(measure_def.filter)
            resolved = await resolve_filter_columns(
                parsed_filters=[parsed],
                model=model,
                model_name=model_name_str,
                resolve_join_target=resolve_join_target,
                named_queries=named_queries,
                resolve_model=resolve_model,
                dialect=dialect,
                strict=False,
            )
            filter_sql = resolved[0].sql
            filter_columns = list(resolved[0].columns)

        # DEV-1361: pull through the source Column's declared type so the
        # generator can wrap the pre-aggregation expression in CAST when the
        # column's sql is non-bare (e.g. json_extract).
        column_type = (
            measure_def.type
            if measure_def is not None and isinstance(measure_def.type, DataType)
            else None
        )
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
                column_type=column_type,
            )
        )
        known_aliases[alias_key] = alias
        # DEV-1444: record the canonical key so later refs to the same
        # canonical form collapse onto this entry's alias (and onto any
        # subsequent user-name rename of it).
        measure_canonical_key_to_alias[canon_key] = alias

    def _resolve_sql(sql: str) -> str:
        resolved = sql
        for name, alias in sorted(known_aliases.items(), key=lambda x: -len(x[0])):
            # Negative lookbehind for . and " prevents matching inside
            # already-quoted identifiers (e.g., _count inside "orders._count")
            resolved = re.sub(rf'(?<![."])\b{re.escape(name)}\b', f'"{alias}"', resolved)
        return resolved

    def _resolve_rank_partition(transform: str, partition_by: List[str]) -> List[str]:
        """Resolve partition_by= column references to base-CTE aliases.

        partition_by entries must reference query dimensions or time dimensions —
        otherwise the column wouldn't be in the base CTE. Match by bare name
        (e.g. 'customer_id' against EnrichedDimension.name) or by qualified
        alias (e.g. 'orders.customer_id'). Cross-model dotted paths
        ('customers.region') match via the dimension alias as built by
        _resolve_dimensions.
        """
        by_name = {d.name: d.alias for d in dimensions}
        by_alias = {d.alias: d.alias for d in dimensions}
        for td in time_dimensions:
            by_name.setdefault(td.name, td.alias)
            by_alias.setdefault(td.alias, td.alias)

        resolved: List[str] = []
        for col in partition_by:
            if col in by_alias:
                resolved.append(by_alias[col])
            elif col in by_name:
                resolved.append(by_name[col])
            else:
                available = sorted(set(by_name) | set(by_alias))
                raise ValueError(
                    f"Transform '{transform}': partition_by column '{col}' is not "
                    f"a query dimension. Add it to dimensions/time_dimensions, or "
                    f"choose one of: {', '.join(available) or '(none)'}."
                )
        return resolved

    def _add_transform(
        name: str,
        transform: str,
        measure_alias: str,
        offset: int = 1,
        granularity: str = None,
        predicate_is_boolean: bool = False,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        needs_time = transform in TIME_TRANSFORMS
        if needs_time and resolved_time_alias is None:
            raise ValueError(
                f"Field '{name}' ({transform}) requires an unambiguous time dimension. "
                f"Add a single time_dimensions entry, or set main_time_dimension to "
                f"select among multiple time dimensions."
            )
        alias = f"{model_name_str}.{name}"
        kwargs = kwargs or {}

        # Rank-family transforms default to no partition (rank across the entire
        # result set) and accept an explicit partition_by= override. Other
        # transforms (cumsum, lag, lead, first, last, time_shift,
        # consecutive_periods) partition by all query dimensions, matching the
        # invariant that adding a measure must not change cardinality.
        if transform in RANK_FAMILY_TRANSFORMS:
            partition_by = kwargs.get("partition_by")
            partition_aliases = (
                _resolve_rank_partition(transform, partition_by) if partition_by else []
            )
        else:
            partition_aliases = [d.alias for d in dimensions]

        enriched_transforms.append(
            EnrichedTransform(
                name=name,
                transform=transform,
                measure_alias=measure_alias,
                alias=alias,
                offset=offset,
                granularity=granularity,
                time_alias=resolved_time_alias if needs_time else None,
                partition_aliases=partition_aliases,
                predicate_is_boolean=predicate_is_boolean,
                n=kwargs.get("n"),
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

    async def _resolve_inner_alias(inner_spec, fallback_name: str) -> str:
        """Flatten a transform's inner spec to a measure alias.

        ``AggregatedMeasureRef`` inners reuse their canonical alias
        (e.g. ``revenue:sum`` → ``revenue_sum``) so the hidden inner
        measure shares the same column key as a sibling-level reference;
        every other shape falls back to ``fallback_name``.
        """
        if isinstance(inner_spec, AggregatedMeasureRef):
            canonical = _canonical_agg_name(
                measure_name=inner_spec.measure_name,
                aggregation_name=inner_spec.aggregation_name,
                agg_args=inner_spec.agg_args,
                agg_kwargs=inner_spec.agg_kwargs,
            )
            return await _flatten_spec(inner_spec, canonical)
        return await _flatten_spec(inner_spec, fallback_name)

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
            # DEV-1444: after provenance-merge the canonical alias may
            # point at a previously declared user measure (e.g. when the
            # user renamed ``revenue:sum`` to ``total``). Consult
            # known_aliases so downstream callers receive the surfaced
            # alias rather than synthesising the unrenamed canonical form.
            return known_aliases.get(
                canonical_name, f"{model_name_str}.{canonical_name}"
            )

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
                inner_alias = await _resolve_inner_alias(
                    spec.inner, f"_inner_{field_name}"
                )

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
            inner_alias = await _resolve_inner_alias(
                spec.inner, f"_inner_{field_name}"
            )

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
                kwargs=spec.kwargs,
            )
            return f"{model_name_str}.{field_name}"

        raise ValueError(f"Unsupported field spec: {spec!r}")

    # DEV-1444: track aliases in declared order so EnrichedQuery.user_projection
    # can be populated at the end. Dims and time-dims come first.
    user_projection: List[str] = [d.alias for d in dimensions]
    user_projection.extend(td.alias for td in time_dimensions)

    # DEV-1444 (Codex review on PR #134): the provenance-merge index in
    # ``_ensure_aggregated_measure`` would silently collapse two
    # user-declared measures that share a canonical key — e.g.
    # ``{"formula":"amount:sum","name":"revenue1"}`` followed by
    # ``{"formula":"amount:sum","name":"revenue2"}`` — onto whichever
    # surfaced alias was claimed first, leaving the second qfield's
    # alias in ``user_projection`` with no matching EnrichedMeasure. The
    # outer trim would then project a column the inner SELECT doesn't
    # expose. Track the set of canonical keys already owned by a
    # user-declared qfield and refuse the duplicate.
    user_declared_canon_keys: Dict[Tuple[Any, ...], str] = {}

    # DEV-1443 (CodeRabbit thread + Codex round 4 on PR #133): the
    # duplicate-explicit-name check must run for every query measure
    # kind, not just inside the local AggregatedMeasureRef rename branch.
    # Cross-model aggregates ``continue`` before reaching that branch and
    # arithmetic/transform measures fall through to ``_flatten_spec``; in
    # both cases a duplicate ``name`` would silently collapse two
    # measures onto a single alias. Run the pairwise check once up front.
    _seen_explicit_names: Dict[str, str] = {}
    for qf in (query.measures or []):
        if not qf.name:
            continue
        if qf.name in _seen_explicit_names:
            raise ValueError(
                f"Measure '{qf.formula}' and measure "
                f"'{_seen_explicit_names[qf.name]}' both declare name "
                f"'{qf.name}'. Two distinct aggregates would otherwise be "
                f"silently merged into one column. Pick a different `name` "
                f"for one of them."
            )
        _seen_explicit_names[qf.name] = qf.formula

    # DEV-1448: lift the canonical-collision guard out of the local-rename
    # branch so it runs symmetrically for local AND cross-model renames. A
    # query measure whose ``name`` equals the canonical alias of another
    # sibling measure would otherwise let ``_ensure_aggregated_measure``'s
    # alias-keyed dedup silently merge two distinct aggregates into one
    # column. Run the pairwise check once up front for every qfield kind.
    for qfield_pre in (query.measures or []):
        if not qfield_pre.name:
            continue
        for qf_other in (query.measures or []):
            if qf_other is qfield_pre:
                continue
            spec_other = parse_formula(
                qf_other.formula,
                extra_agg_names=custom_agg_names,
                named_measures=named_measures,
            )
            if not isinstance(spec_other, AggregatedMeasureRef):
                continue
            other_canonical = _canonical_agg_name(
                measure_name=spec_other.measure_name,
                aggregation_name=spec_other.aggregation_name,
                agg_args=spec_other.agg_args,
                agg_kwargs=spec_other.agg_kwargs,
            )
            if other_canonical == qfield_pre.name:
                raise ValueError(
                    f"Measure '{qfield_pre.formula}' renamed to "
                    f"'{qfield_pre.name}', but that name collides with "
                    f"the canonical alias of another query measure "
                    f"'{qf_other.formula}' (also canonicalises to "
                    f"'{qfield_pre.name}'). Two distinct aggregates would "
                    f"otherwise be silently merged into one column. "
                    f"Pick a different `name`, or rename the other "
                    f"measure too."
                )

    # Process each query field
    for qfield in query.measures or []:
        spec = parse_formula(
            qfield.formula,
            extra_agg_names=custom_agg_names,
            named_measures=named_measures,
        )
        # DEV-1443 (Codex Finding 2): block the latent bug where an
        # alias-form filter against a renamed measure silently resolves to
        # the source column instead of the HAVING aggregate. Reject up
        # front rather than letting strict resolution misfire downstream.
        if qfield.name and qfield.name in _source_column_names:
            raise ValueError(
                f"Query measure name '{qfield.name}' collides with a source "
                f"column on model '{model.name}'. Pick a different name "
                f"(or omit `name` to use the canonical alias). Filters and "
                f"ORDER BY would otherwise bind to the source column "
                f"instead of the renamed aggregate."
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
                # DEV-1361: propagate declared result type into the inner
                # EnrichedMeasure so _build_combined wraps the agg in CAST.
                if qfield.type is not None:
                    cm.measure.type = qfield.type
                # DEV-1444: this CrossModelMeasure corresponds to a user-
                # declared qfield, so mark it as such and surface its alias
                # in the public projection.
                cm.user_declared = True
                cross_model_measures.append(cm)
                # DEV-1448: when the user supplies an explicit ``name`` on a
                # cross-model measure spec, surface it as the
                # CrossModelMeasure's outer handle so the public projection
                # and downstream nested stages emit the user's chosen alias
                # instead of the canonical ``<query_model>.<hop_path>.<col>_<agg>``
                # form. Only the **leaf** of the dotted path is swapped to
                # the user name; the hop path is preserved (e.g.
                # ``customers.regions.population:sum`` + ``name="region_pop"``
                # surfaces as ``orders.customers.regions.region_pop``). This
                # matches the dot-syntax convention every other multi-hop
                # caller-facing key in SLayer uses. Downstream-stage virtual
                # models then use the bare ``cm.name`` (no ``__`` flattening)
                # via a special-case in ``_query_as_model`` so callers can
                # reference the user's chosen name directly. Cross-model
                # canonicals always contain dots and ``ModelMeasure.name``
                # rejects dots, so the ``!= canonical_name`` guard is
                # structurally true when ``qfield.name`` is supplied; we
                # keep the explicit check for forward-compat. Filter /
                # ORDER BY remap of the colon-form ``<other>.<col>:<agg>``
                # is intentionally NOT wired up here (DEV-1445 territory);
                # ``known_aliases[qfield.name]`` only registers the user
                # alias so user-alias-form filters / ORDER BY resolve via
                # the existing alias-lookup path.
                if qfield.name and qfield.name != canonical_name:
                    hop_path = spec.measure_name.rsplit(".", 1)[0]
                    user_alias = f"{model_name_str}.{hop_path}.{qfield.name}"
                    cm.alias = user_alias
                    cm.name = qfield.name
                    known_aliases[qfield.name] = user_alias
                user_projection.append(cm.alias)
                continue

            # DEV-1444 (Codex review on PR #134): refuse two user-
            # declared qfields with the same canonical aggregation. The
            # provenance-merge index would otherwise reuse the first
            # alias for the second qfield, surfacing ``user_projection``
            # entries that no EnrichedMeasure backs.
            qfield_canon_key = (
                "agg",
                spec.measure_name,
                spec.aggregation_name,
                tuple(spec.agg_args),
                tuple(sorted(spec.agg_kwargs.items())),
            )
            if qfield_canon_key in user_declared_canon_keys:
                prior_name = user_declared_canon_keys[qfield_canon_key]
                this_name = qfield.name or canonical_name
                if prior_name != this_name:
                    raise ValueError(
                        f"Measure '{qfield.formula}' (surfacing as "
                        f"'{this_name}') canonicalises to the same "
                        f"aggregation as an earlier query measure "
                        f"(surfacing as '{prior_name}'). Two user-declared "
                        f"measures with the same canonical aggregation "
                        f"would otherwise collapse into one column, "
                        f"leaving the second name with no backing "
                        f"aggregate. Pick a single name, or drop the "
                        f"duplicate."
                    )
            user_declared_canon_keys[qfield_canon_key] = (
                qfield.name or canonical_name
            )

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
                # DEV-1443 (Codex review on PR #133): if the canonical alias
                # itself shadows a source ``Column`` on the model, the colon-
                # form filter ``col:agg <op> N`` is ambiguous — the remap
                # would resolve to the user alias, while strict resolution
                # would resolve a literal ``col_agg`` reference to the source
                # column. Refuse the query at construction time rather than
                # silently picking one and producing surprising SQL.
                if canonical_name in _source_column_names:
                    raise ValueError(
                        f"Measure '{qfield.formula}' renamed to '{qfield.name}', "
                        f"but model '{model.name}' has a source column named "
                        f"'{canonical_name}' that shadows the canonical alias of "
                        f"this aggregation. Filters / ORDER BY using "
                        f"'{qfield.formula}' would be ambiguous. Pick a different "
                        f"`name` (so the canonical alias is unused), rename the "
                        f"source column, or reference the measure by its user "
                        f"alias '{qfield.name}'."
                    )
                # DEV-1448: the rename-vs-other-canonical collision guard
                # previously inlined here was lifted into the pre-pass at
                # the top of this function so it runs symmetrically for
                # local AND cross-model renames. See the pre-pass next to
                # ``_seen_explicit_names``.
                user_alias = f"{model_name_str}.{qfield.name}"
                prev_alias = f"{model_name_str}.{canonical_name}"
                for m in measures:
                    if m.alias == prev_alias:
                        m.name = qfield.name
                        m.alias = user_alias
                        break
                known_aliases[qfield.name] = user_alias
                known_aliases[canonical_name] = user_alias
                # DEV-1444 provenance merge: any canonical key currently
                # pointing at the pre-rename alias must follow the rename
                # so later auto-extracted refs collapse onto the new alias.
                for k, v in list(measure_canonical_key_to_alias.items()):
                    if v == prev_alias:
                        measure_canonical_key_to_alias[k] = user_alias
                # DEV-1443: record the canonical → user-name mapping so
                # query filters and ORDER BY items referencing the raw
                # ``col:agg`` formula can be remapped to the user alias
                # before resolution.
                canonical_to_user_name[canonical_name] = qfield.name
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
            # DEV-1361: declared result type → wrap aggregation in CAST.
            if qfield.type is not None:
                target_name = qfield.name if (qfield.name and qfield.name != canonical_name) else canonical_name
                for m in measures:
                    if m.name == target_name:
                        m.type = qfield.type
            # DEV-1444: mark the surfaced EnrichedMeasure as user-declared
            # and append its alias to the projection.
            surfaced_alias = (
                f"{model_name_str}.{qfield.name}"
                if (qfield.name and qfield.name != canonical_name)
                else f"{model_name_str}.{canonical_name}"
            )
            _mark_user_declared(surfaced_alias)
            user_projection.append(surfaced_alias)

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
            # DEV-1361: declared result type → wrap arithmetic / transform
            # expression in CAST at the outer SELECT.
            if qfield.type is not None:
                alias = f"{model_name_str}.{field_name}"
                for e in enriched_expressions:
                    if e.alias == alias:
                        e.type = qfield.type
                # Pure-transform measures (lag/lead/cumsum/...) end up in
                # ``enriched_transforms``, not ``enriched_expressions``;
                # propagate the declared type there too so the window-layer
                # emitter can wrap in CAST.
                for t in enriched_transforms:
                    if t.alias == alias:
                        t.type = qfield.type
            # DEV-1444: mark the surfaced EnrichedExpression / EnrichedTransform
            # (whichever the formula landed in) as user-declared. The inner
            # hoisted measures created by _flatten_spec remain user_declared=False.
            surfaced_alias = f"{model_name_str}.{field_name}"
            _mark_user_declared(surfaced_alias)
            user_projection.append(surfaced_alias)

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
        # DEV-1443: when the canonical points at a measure renamed by the
        # query, the user alias is the real column key in the projection.
        # Setting the order item's column name to the canonical would send
        # the generator's ``_resolve_order_by_column`` down the fallback
        # branch (``{model_prefix}.{canonical}``), producing a reference
        # to a column that does not exist. DEV-1444's provenance-merge
        # also ensures any auto-extracted canonical-form ref resolves to
        # the surfaced user alias through this map.
        item.column.name = canonical_to_user_name.get(canonical, canonical)

    # --- Validate model filters ---
    # DEV-1378: Mode A model filters get parsed via ``parse_sql_predicate``
    # (the SQL-mode validator) so arbitrary SQL function calls
    # (``json_extract``, ``coalesce``, ``CASE WHEN``, dialect-specific
    # operators) flow through unchanged. The construction-time validator
    # at ``slayer/core/models.py:412`` already rejected DSL constructs.
    measure_names_set = {m.name for m in measures}
    parsed_model_filters: List[ParsedFilter] = []
    for mf in model.filters:
        parsed_mf = parse_sql_predicate(mf)
        for col in parsed_mf.columns:
            if col in measure_names_set:
                raise ValueError(
                    f"Model filter '{mf}' references measure '{col}'. "
                    f"Model filters can only reference table columns (WHERE). "
                    f"Use query-level filters for measure conditions."
                )
        parsed_model_filters.append(parsed_mf)

    # --- Process filters ---
    # Apply variable substitution to query-level filters (not model-level —
    # SQL-mode filters are constructed before the query runs and don't see
    # query-time variable substitution).
    query_filters = list(query.filters or [])
    if query.variables and query_filters:
        from slayer.core.query import substitute_variables

        query_filters = [
            substitute_variables(filter_str=f, variables=query.variables) for f in query_filters
        ]

    # Transform extraction runs only on Mode B (DSL) query filters. Model
    # filters are SQL mode — they don't carry SLayer transforms (rejected
    # at construction by ``parse_sql_predicate``) and don't go through
    # ``_preprocess_like`` / ``_preprocess_agg_refs``.
    processed_query_filters: List[str] = []
    ft_counter = [0]
    for f_str in query_filters:
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
        processed_query_filters.append(rewritten)

    # Mode-tagged filter list, in WHERE order: model filters first, then
    # query filters. Used by the windowed-column scan, ``_resolve_joins`` /
    # ``_collect_needed_paths``, and the ordering of the final
    # ``EnrichedQuery.filters`` list.
    processed_filters_with_mode: List[Tuple[str, str]] = (
        [(mf, "sql") for mf in model.filters]
        + [(qf, "dsl") for qf in processed_query_filters]
    )

    has_first_or_last = any(m.aggregation in ("first", "last") for m in measures)

    # DEV-1369: a query filter that names a Column whose `sql` contains a
    # window function used to auto-promote to a post-aggregation outer
    # WHERE. The escape hatch is removed — the rank-family transforms
    # (`rank` / `percent_rank` / `dense_rank` / `ntile`) cover top-N
    # filtering in pure DSL. Applied to both modes — neither standard SQL
    # nor SLayer DSL allows window functions in WHERE.
    _windowed_columns: Dict[str, str] = {
        c.name: c.sql for c in model.columns if c.sql and has_window_function(c.sql)
    }
    if _windowed_columns:
        for f, _mode in processed_filters_with_mode:
            for col_name in _windowed_columns:
                if re.search(rf"(?<!\w)\b{re.escape(col_name)}\b(?!\w)", f):
                    raise ValueError(
                        f"Filter references column '{col_name}' whose SQL "
                        f"contains a window function. Use a rank-family "
                        f"transform (e.g. `rank(<measure>) <= N`, "
                        f"`percent_rank(...)`, `dense_rank(...)`, `ntile(n=4, ...)`) "
                        f"or factor the column into a multi-stage source_queries "
                        f"model. The filter was: {f!r}"
                    )

    # --- Resolve JOINs ---
    resolved_joins = await _resolve_joins(
        model=model,
        model_name_str=model_name_str,
        dimensions=dimensions,
        time_dimensions=time_dimensions,
        measures=measures,
        cross_model_measures=cross_model_measures,
        processed_filters=processed_filters_with_mode,
        named_queries=named_queries,
        resolve_join_target=resolve_join_target,
        extra_agg_names=custom_agg_names,
        dialect=dialect,
    )

    # Names that resolve at the query level (named measures, transforms,
    # expressions) — pass through as legitimate filter targets even though
    # they are not Columns / ModelMeasures on the source model.
    _query_aliases: Set[str] = set()
    _query_aliases.update(m.name for m in measures if m.name)
    _query_aliases.update(t.name for t in enriched_transforms if t.name)
    _query_aliases.update(e.name for e in enriched_expressions if e.name)
    # DEV-1448 (Codex review on PR #136): we previously added cross-model
    # measure names to ``_query_aliases`` so a same-stage filter
    # ``"cust_rev > 100"`` would pass strict resolution. That admission was
    # half-baked — the SQL generator has no path to route the bare name to
    # the cross-model CTE's output column ``"orders.customers.cust_rev"``,
    # so it qualified the bare alias as ``orders.cust_rev`` (a column that
    # doesn't exist on the base table) and shipped invalid SQL. Until the
    # full cross-model filter remap lands in DEV-1445, the bare user alias
    # is NOT a valid filter / ORDER BY target on a renamed cross-model
    # measure — strict resolution must reject it cleanly rather than
    # silently produce broken SQL. The rename remains useful for the
    # projection alias and the downstream-stage virtual model column,
    # which is the ticket's actual repro shape.
    # DEV-1378: model filters and query filters resolve under different
    # strictness rules. Model filters are SQL-mode and may reference any
    # column on the underlying table even when not declared as a
    # ``Column`` (``strict=False`` — see the comment block at
    # ``resolve_filter_columns`` lines ~1652-1657). Query filters are
    # DSL-mode and must strictly resolve to a Column / ModelMeasure /
    # custom aggregation / canonical agg alias / query-level alias
    # (``strict=True``). Run the resolver twice and concatenate.
    resolved_model_filters = await resolve_filter_columns(
        parsed_filters=parsed_model_filters,
        model=model,
        model_name=model_name_str,
        resolve_join_target=resolve_join_target,
        named_queries=named_queries,
        resolve_model=resolve_model,
        dialect=dialect,
        strict=False,
        drop_if_unresolved=False,
        query_aliases=set(),
    )
    # DEV-1443: pre-pass remap of canonical agg aliases → user aliases for
    # query filters. Applied here (and ONLY here) so model filters and
    # ``Column.filter`` predicates — which never carry colon-syntax
    # synthesized aliases anyway — are left untouched.
    parsed_query_filters_pre = [
        parse_filter(f, extra_agg_names=custom_agg_names)
        for f in processed_query_filters
    ]
    for pf in parsed_query_filters_pre:
        _remap_renamed_aliases_in_filter(
            pf=pf,
            canonical_to_user_name=canonical_to_user_name,
        )
    resolved_query_filters = await resolve_filter_columns(
        parsed_filters=parsed_query_filters_pre,
        model=model,
        model_name=model_name_str,
        resolve_join_target=resolve_join_target,
        named_queries=named_queries,
        resolve_model=resolve_model,
        dialect=dialect,
        # Strict resolution for DSL query filters — bare names AND dotted
        # paths must resolve. Rerooted CTEs may drop unresolved filters
        # (DEV-1367) via ``drop_if_unresolved``.
        strict=True,
        drop_if_unresolved=drop_unreachable_filters,
        query_aliases=_query_aliases,
    )
    parsed_filters = list(resolved_model_filters) + list(resolved_query_filters)

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
        cross_model_measures=cross_model_measures,
        last_agg_time_column=last_agg_time_column if has_first_or_last else None,
        filters=classify_filters(
            filters=parsed_filters,
            measure_names={m.name for m in measures},
            computed_names=(
                {t.name for t in enriched_transforms}
                | {e.name for e in enriched_expressions}
            ),
            groupby_names={d.name for d in dimensions} | {td.name for td in time_dimensions},
            windowed_measure_names={m.name for m in measures if m.window},
        ),
        order=query.order,
        limit=query.limit,
        offset=query.offset,
        field_name_aliases=field_name_aliases,
        user_projection=user_projection,
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
                type=dim_def.type if dim_def else DataType.TEXT,
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
    processed_filters: List[Tuple[str, str]],
    extra_agg_names: Optional[frozenset] = None,
    dialect: Optional[str] = None,
) -> Set[Tuple[str, ...]]:
    """Extract ordered join-path tuples the query needs (including all prefixes).

    ``processed_filters`` is a list of ``(filter_text, mode)`` tuples
    where ``mode`` is ``"sql"`` for Mode A (model-side) filters and
    ``"dsl"`` for Mode B (query-side) filters; each is parsed by the
    matching parser so model filters with arbitrary SQL functions
    don't trip the DSL allowlist (DEV-1378).
    """
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
    for f_str, mode in processed_filters:
        if mode == "sql":
            parsed_f = parse_sql_predicate(f_str)
        else:
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
    processed_filters: List[Tuple[str, str]],
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


def _remap_renamed_aliases_in_filter(
    *,
    pf: ParsedFilter,
    canonical_to_user_name: Dict[str, str],
) -> None:
    """DEV-1443: rewrite canonical-agg aliases in a parsed query filter
    to the user-supplied alias when the same node renamed the measure.

    Eligibility: ``c in pf.synthesized_aliases`` — only remap names the
    parser saw as colon-syntax in *this* filter. A literal column reference
    (no colon syntax) is left alone since the parser would not have
    synthesized it.

    Mutates ``pf.sql`` and ``pf.columns`` in place. ``synthesized_aliases``
    and ``agg_refs`` are left intact (they're parser provenance, not the
    rendered SQL).

    Note: the case where ``canonical_name`` is also the name of a source
    ``Column`` on the model is rejected up front at measure enrichment
    (DEV-1443 Codex-review on PR #133). By the time this helper runs the
    mapping is guaranteed not to alias a source column, so any
    ``\\bcanonical\\b`` occurrence in ``pf.sql`` came from colon syntax
    and is safe to rewrite — outside of quoted string literals.

    DEV-1443 (CodeRabbit thread on PR #133): the regex sub must not touch
    string-literal contents. Mask single-quoted spans with placeholders
    before applying the rewrite, then restore them. Otherwise
    ``country:first = 'country_first'`` with the measure renamed to
    ``primary_country`` becomes ``primary_country = 'primary_country'``
    and the filter compares the column to itself.
    """
    if not canonical_to_user_name:
        return
    eligible = {
        c: u for c, u in canonical_to_user_name.items()
        if c in pf.synthesized_aliases
    }
    if not eligible:
        return
    # Mask single-quoted spans so the identifier sub below can't reach
    # into string literals. ``_STRING_LITERAL_RE`` is the same pattern
    # used elsewhere in the codebase for this purpose
    # (slayer/core/formula.py).
    literal_re = re.compile(r"'(?:[^'\\]|\\.)*'")
    literals = literal_re.findall(pf.sql)
    masked = literal_re.sub("\x00LIT\x00", pf.sql)
    for canonical, user_name in eligible.items():
        # Word-boundary regex mirroring ``_resolve_sql`` (line ~393) so
        # canonical names embedded inside dotted paths or already-quoted
        # identifiers are not rewritten.
        masked = re.sub(
            rf'(?<![."\w])\b{re.escape(canonical)}\b(?![\w."])',
            user_name,
            masked,
        )
    # Restore literals in original order.
    for literal in literals:
        masked = masked.replace("\x00LIT\x00", literal, 1)
    pf.sql = masked
    pf.columns = [eligible.get(c, c) for c in pf.columns]


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

    from slayer.core.formula import (
        _expand_named_measures,
        _preprocess_agg_refs,
        _preprocess_concat,
    )

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
    # DEV-1378: rewrite SQL `||` to `<<` so AST parsing accepts the filter.
    preprocessed = _preprocess_concat(preprocessed)
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
    resolve_model=None,
    dialect: str = "postgres",
    *,
    strict: bool = False,
    drop_if_unresolved: bool = False,
    query_aliases: Optional[Set[str]] = None,
) -> list:
    """Resolve filter column references through model dimensions/measures.

    When ``resolve_model`` is supplied, derived ``Column.sql`` expressions
    are recursively expanded so chained derivations (cross-model or local)
    yield fully-qualified physical-table SQL inside WHERE clauses.

    With ``strict=True`` (DSL-mode callers — query-level filters) any
    bare name that doesn't resolve to a Column / ModelMeasure / custom
    aggregation / canonical agg alias / query-level alias raises
    ``ValueError``; the same applies on the dotted-path branch when the
    head segment names no join target on the source model. With
    ``strict=False`` (SQL-mode callers — ``Column.filter``, model-level
    filters) unknown bare names pass through as references to
    underlying-table columns.

    With ``strict=True`` and ``drop_if_unresolved=True`` (DEV-1367 —
    used by the cross-model-measure rerooting path in
    ``query_engine._build_rerooted_enriched``), unresolved bare names and
    dotted paths cause the **entire filter** to be dropped from the
    output rather than raising. The rerooting machinery inherits the
    outer query's filter list; only the subset reachable from the
    rerooted source applies, so this turns the would-raise into a clean
    drop. With ``drop_if_unresolved=False`` (the default), unresolved
    strict-mode references raise ``ValueError`` as documented above.
    """
    import re as _re

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

    out_filters: list = []
    for f in parsed_filters:
        resolved_sql = f.sql
        resolved_columns = []
        # DEV-1369: precise allowlist of synthesised aliases this filter
        # introduced via colon syntax (e.g. ``revenue:sum`` → ``revenue_sum``,
        # ``*:count`` → ``_count``). Strict-resolution checks against this
        # set, replacing the prior permissive regex that matched any
        # ``*_sum``-shaped name and let typos like ``made_up_sum`` through.
        filter_synthesized_aliases = set(getattr(f, "synthesized_aliases", []))
        drop_this_filter = False
        for col_name in dict.fromkeys(f.columns):
            if "." not in col_name:
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
                    # DEV-1369: strict resolution. Only fires for DSL-mode
                    # callers (``strict=True`` — query-level filters). For
                    # SQL-mode callers (``Column.filter``, model-level
                    # filters) bare names are valid references to columns
                    # on the underlying table even when not declared as
                    # SLayer ``Column`` entries.
                    if strict:
                        is_measure = model.get_measure(col_name) is not None
                        is_custom_agg = model.get_aggregation(col_name) is not None
                        is_synthesized_alias = col_name in filter_synthesized_aliases
                        is_query_alias = (
                            query_aliases is not None
                            and col_name in query_aliases
                        )
                        if not (
                            is_measure
                            or is_custom_agg
                            or is_synthesized_alias
                            or is_query_alias
                        ):
                            # ``drop_if_unresolved`` (rerooted CTE path):
                            # turn the would-raise into a drop so the
                            # inherited filter is excluded from the
                            # rerooted enrichment. Bare names that don't
                            # resolve there must NOT silently pass through
                            # — that would put a raw column reference in
                            # the inner SQL.
                            if drop_if_unresolved:
                                drop_this_filter = True
                            else:
                                raise ValueError(
                                    f"Filter references unknown name "
                                    f"'{col_name}' on model '{model.name}'. "
                                    f"It is not a Column, a ModelMeasure, a "
                                    f"custom aggregation, or a named measure "
                                    f"/ transform alias in this query. "
                                    f"Define it on the model first or check "
                                    f"spelling."
                                )
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

                # DEV-1367: a dotted reference that doesn't resolve through
                # the join graph used to silently pass through, producing
                # SQL with an unbound table reference (``transportation_assets``
                # in WHERE but never in FROM/JOIN).
                #
                # * ``strict=True`` (DSL-mode outer-query path): raise so
                #   agents get a translation-time error rather than a
                #   cryptic "no such column" at execution.
                # * ``drop_if_unresolved=True`` (re-rooted CTE path): mark
                #   the entire filter for dropping; the cross-model
                #   re-rooting machinery inherits the outer filter list and
                #   asks us to skip whatever doesn't reach from the new
                #   source.
                # * Otherwise (SQL-mode model-side filters): silently pass
                #   through — bare table references inside ``__``-aliased
                #   paths are valid SQL even when sqlglot's join walker
                #   doesn't know about them.
                if strict:
                    if drop_if_unresolved:
                        # Re-rooted CTE path: drop the filter rather than
                        # raise. The cross-model machinery inherits the
                        # outer query's filter list and only the subset
                        # reachable from the rerooted source applies.
                        drop_this_filter = True
                    else:
                        head = path_parts[0]
                        if not resolved and not any(j.target_model == head for j in model.joins):
                            raise ValueError(
                                f"Filter '{col_name}' references model "
                                f"'{head}' but it is not in joins for source "
                                f"model '{model.name}'. Add it to "
                                f"source_model.joins or rewrite the filter "
                                f"to use a local derived column."
                            )
                        raise ValueError(
                            f"Filter '{col_name}' references column "
                            f"'{dim_name}' on '{'.'.join(path_parts)}', "
                            f"which doesn't resolve to a known column on "
                            f"the joined model. Check the path or define "
                            f"the column on the model."
                        )
                resolved_columns.append(col_name)

        f.sql = resolved_sql
        f.columns = resolved_columns
        if not drop_this_filter:
            out_filters.append(f)

    return out_filters


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
