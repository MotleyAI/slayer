"""THE checker + the ``ElaboratedQuery`` environment builder (D2, D4).

Never imports a planner module: binding and compilation both consult it — every
algebra type error raises here, each invoked at its family's original checkpoint.
"""

from __future__ import annotations

from typing import (
    Callable, Dict, List, NoReturn, Optional,
    Sequence, Tuple, Union,
)

from slayer.core.enums import RANK_FAMILY_TRANSFORMS, DataType, TimeGranularity
from slayer.core.errors import (
    CanonicalAliasShadowsColumnError,
    DistinctDimensionValuesError,
    DuplicateMeasureNameError,
    MeasureNameCollidesWithColumnError,
    PositionTypingError,
    SlayerError,
    TimeDimensionColumnError,
)
from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.window_duration import parse_window_duration
from slayer.core.keys import (
    AggregateKey,
    attached_inputs,
    is_boolean_shaped,
    is_cross_model_agg,
    is_local_combined_regroup_ref,
    is_local_partitioned_agg,
    split_top_level_and,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    Grain,
    InKey,
    ScalarCallKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    regroup_root_grain,
    source_anchor_path,
    transform_operand_grain,
    walk_value_keys,
)
from slayer.core.models import SlayerModel
from slayer.core.refs import dotted_key_display
from slayer.core.scope import ModelScope, StageSchema
from slayer.sql.sql_expr import has_window_function
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.ir.planned import MaskTyping, ModeAFilter
from slayer.ir.elaborated import (
    ConjunctTyping, ElaboratedQuery, ExpressionEntry, PositionVerdict, Term,
)
from slayer.ir.bound import BoundFilter, bound_filter_from_key
from slayer.ir.prebound import PreboundQuery, position_typing_context
from slayer.ir.terms import (
    Aggregate,
    Broadcast,
    DatasetT,
    ModelDataset,
    StageDataset,
    Transform,
)

def home_dataset(
    *, scope: Union[ModelScope, StageSchema], model: Optional[SlayerModel],
) -> Optional[DatasetT]:
    """The query's root dataset: model-backed for a ModelScope, stage-backed otherwise."""
    if isinstance(scope, StageSchema):
        return StageDataset(stage_name=scope.relation_name)
    if model is not None:
        return ModelDataset(data_source=model.data_source, model_name=model.name)
    return None


def _field_blockers(cj: ValueKey, row_agg_set: frozenset) -> Tuple[List[ValueKey], bool]:
    """(aggregates/transforms blocking field typing, saw-attached-ref); a ``row_agg_set``
    aggregate resolves to its row-attached value and counts as aggregate-free."""
    blockers: List[ValueKey] = []
    attached = False

    def _walk(k: ValueKey) -> None:
        nonlocal attached
        if isinstance(k, (AggregateKey, TransformKey)):
            # A row-attach root (partitioned aggregate or transform root of a
            # computed dimension) resolves row-side; its subtree is its own scope.
            if k in row_agg_set:
                attached = True
            else:
                blockers.append(k)
            return
        for c in k.children():
            _walk(c)

    _walk(cj)
    return blockers, attached


def _measure_blockers(cj: ValueKey, dim_keys: frozenset) -> List[ValueKey]:
    """Row-level refs outside aggregate subtrees not available at query grain.
    A subtree equal to a query dimension's bound key IS the grouped value —
    available at query grain wholesale."""
    blockers: List[ValueKey] = []

    def _walk(k: ValueKey) -> None:
        if k in dim_keys or isinstance(k, AggregateKey):
            return
        if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            blockers.append(k)
            return
        if isinstance(k, TransformKey):
            _walk(k.input)  # partition/time keys are transform machinery, not refs
            return
        for c in k.children():
            _walk(c)

    _walk(cj)
    return blockers


def _key_display(k: ValueKey) -> str:
    if isinstance(k, AggregateKey):
        leaf = getattr(k.source, "leaf", None) or getattr(k.source, "column_name", None) or "*"
        path = source_anchor_path(k.source)
        name = f"{k.agg}({'.'.join((*path, leaf))})"
        return f"{name} (partition_by)" if k.partition_keys is not None else name
    if isinstance(k, TransformKey):
        return f"{k.op}(...)"
    if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        col = k.column if isinstance(k, TimeTruncKey) else k
        leaf = getattr(col, "leaf", None) or getattr(col, "column_name", "?")
        return ".".join((*col.path, leaf))
    return type(k).__name__


def type_position_conjunct(
    cj: ValueKey,
    *,
    dim_keys: frozenset,
    row_agg_set: frozenset = frozenset(),
    has_measure_position: bool = True,
    position: str = "filter",
) -> ConjunctTyping:
    """Type one conjunct as field (aggregate-free after resolution; attached refs count
    as row-level) else measure (every bare ref available at query grain), else raise
    :class:`PositionTypingError` naming both failures. Field wins a tie."""
    field_blockers, attached = _field_blockers(cj, row_agg_set)
    if not field_blockers:
        return ConjunctTyping(MaskTyping.FIELD, 1 if attached else 0)
    if has_measure_position:
        measure_blockers = _measure_blockers(cj, dim_keys)
        if not measure_blockers:
            return ConjunctTyping(MaskTyping.MEASURE, 1)
        raise PositionTypingError(
            f"This {position} expression is valid as neither a field nor a "
            f"measure. Field typing failed: it references "
            f"{', '.join(_key_display(k) for k in field_blockers)}, available "
            f"only after aggregation. Measure typing failed: it references "
            f"row-level {', '.join(_key_display(k) for k in measure_blockers)}, "
            f"not available at the query grain (not among the query "
            f"dimensions). Split the top-level AND conjuncts so each resolves "
            f"in one typing, or add the row-level reference to the query "
            f"dimensions."
        )
    raise PositionTypingError(
        f"This {position} expression references "
        f"{', '.join(_key_display(k) for k in field_blockers)}, so it is not a "
        f"field, and measure typing is unavailable because the query has no "
        f"measure position (distinct_dimension_values=False)."
    )




def type_and_split_filters(
    prebound: PreboundQuery,
    *,
    crossing_root: Optional[Callable[[ValueKey], bool]] = None,
    split: bool = True,
) -> Tuple[PreboundQuery, List[ConjunctTyping]]:
    """Type every filter conjunct as field or measure (raising the typing error for
    neither), splitting a filter string into per-conjunct masks when its conjuncts
    route differently. Returns (rebuilt prebound, typings aligned with its filters)."""
    old = list(prebound.bound_filters)
    dim_keys, row_agg_set = position_typing_context(prebound)
    has_measure_position = prebound.distinct_dimension_values is not False

    def _has_partitioned_ref(vk: ValueKey) -> bool:
        # ANY partitioned / cross-model / crossing aggregate ref forces the split so
        # each conjunct lowers to its own placement.
        return any(
            is_local_combined_regroup_ref(k, row_agg_set=row_agg_set)
            or is_local_partitioned_agg(k)
            or is_cross_model_agg(k)
            or (crossing_root is not None and crossing_root(k))
            for k in walk_value_keys(vk)
        )

    def _typed(cj: ValueKey) -> ConjunctTyping:
        return type_position_conjunct(
            cj, dim_keys=dim_keys, row_agg_set=row_agg_set,
            has_measure_position=has_measure_position,
        )

    texts = list(prebound.bound_filter_texts)
    new_filters: List[BoundFilter] = []
    new_texts: List[Optional[str]] = []
    typings: List[ConjunctTyping] = []
    changed = False
    for i, bf in enumerate(old):
        conjuncts = (
            split_top_level_and(bf.value_key)
            if split and i >= prebound.n_date_range
            else [bf.value_key]
        )
        conjunct_typings = [_typed(cj) for cj in conjuncts]
        if len(conjuncts) > 1 and (
            _has_partitioned_ref(bf.value_key)
            or len(set(conjunct_typings)) > 1
        ):
            changed = True
            for cj, ct in zip(conjuncts, conjunct_typings):
                new_filters.append(bound_filter_from_key(cj))
                new_texts.append(None)
                typings.append(ct)
        else:
            new_filters.append(bf)
            new_texts.append(texts[i])
            typings.append(max(conjunct_typings, key=lambda ct: ct.stratum))
    if not changed:
        return prebound, typings
    updated = prebound.model_copy(update={
        "bound_filters": new_filters,
        "bound_filter_texts": new_texts,
    })
    return updated, typings


def type_order_positions(prebound: "PreboundQuery") -> None:
    """Type every ORDER target (field / measure / typing error) — the same pass
    ``build_environment`` runs; kept callable for compiler-synthesized sub-plans."""
    dim_keys, row_agg_set = position_typing_context(prebound)
    for spec in prebound.order_specs:
        type_position_conjunct(
            spec.bound.value_key,
            dim_keys=dim_keys,
            row_agg_set=row_agg_set,
            has_measure_position=prebound.distinct_dimension_values is not False,
            position="order",
        )


def check_computed_dim_name_collision(
    *, name: str, model_name: Optional[str], query_measure_collision: bool,
) -> None:
    """A computed dimension's name must not shadow a model column/measure or a query measure."""
    if model_name is not None:
        raise ValueError(
            f"Computed dimension name {name!r} collides with an existing "
            f"column or measure on model {model_name!r}. Choose a different "
            f"name."
        )
    if query_measure_collision:
        raise ValueError(
            f"Computed dimension name {name!r} collides with a query measure "
            f"of the same name. Choose a different name."
        )


def check_stage_flatten_collision(*, flat_name: str, collides: bool) -> None:
    """Two projected columns must not flatten to one downstream name (both the declaration-time and stage-schema firing points)."""
    if collides:
        raise ValueError(flatten_collision_message(flat_name))


def check_measure_dedupe_collision(
    *, prior_formula: str, formula: str, public_name: str,
    same_key: bool, same_meta: bool,
) -> None:
    """Unnamed measures sharing a derived result key must be the same value with the same metadata."""
    if not same_key:
        raise ValueError(
            f"Measures {prior_formula!r} and {formula!r} both derive "
            f"the result key {public_name!r} but compute different "
            f"values; rename one (set 'name') to disambiguate."
        )
    if not same_meta:
        raise ValueError(
            f"Measures {prior_formula!r} and {formula!r} merge into "
            f"one result column {public_name!r} but declare "
            f"different label/type; rename one (set 'name') to "
            f"disambiguate."
        )


def check_measure_name_collision(*, name: Optional[str], model: str) -> None:
    """A public measure name shadowing a source column raises."""
    if name is not None:
        raise MeasureNameCollidesWithColumnError(name=name, model=model)


def check_canonical_alias_shadows_column(
    *, formula: str, canonical: Optional[str], model: str,
) -> None:
    """A renamed measure's canonical alias shadowing a source column raises."""
    if canonical is not None:
        raise CanonicalAliasShadowsColumnError(
            formula=formula, canonical=canonical, model=model,
        )


def check_duplicate_measure_name(*, name: str, occurrences: List[str]) -> NoReturn:
    """Two different expressions may not claim one public name (both registry firing points)."""
    raise DuplicateMeasureNameError(name=name, occurrences=occurrences)


def check_reserved_regroup_prefix(columns: List[str]) -> None:
    """A real column may not carry the reserved regroup placeholder prefix while a regroup is active."""
    if columns:
        raise ValueError(
            f"Column(s) {columns!r} use the reserved '__regroup__' prefix, which "
            f"collides with the regroup primitive's placeholders. Rename them."
        )


def validate_model_filter(
    *,
    mf: str,
    idx: int,
    model: SlayerModel,
) -> ModeAFilter:
    """Validate a ``SlayerModel.filters`` entry and emit its Mode-A text carrier (rejects same-model ModelMeasure and window-function column refs)."""
    parsed = parse_sql_predicate(mf)
    measure_names = {m.name for m in (model.measures or [])}
    windowed_columns = {
        c.name for c in model.columns
        if c.sql and has_window_function(c.sql)
    }
    for col in parsed.columns:
        if col in measure_names:
            raise ValueError(
                f"Model filter {mf!r} references measure {col!r}. "
                f"Model filters can only reference table columns (WHERE). "
                f"Use query-level filters for measure conditions."
            )
        if col in windowed_columns:
            raise ValueError(
                f"Model filter {mf!r} references column {col!r} whose "
                f"SQL contains a window function. Factor it into a "
                f"multi-stage source_queries model or use a rank-family "
                f"transform at query time."
            )
    return ModeAFilter(id=f"mf{idx}", text=mf)


def _aggregate_terms(
    roots: List[ValueKey], *, home: DatasetT, query_grain: Grain,
    home_paths: Dict[AggregateKey, Tuple[str, ...]],
) -> Tuple[Dict[ValueKey, Term], List[TransformKey]]:
    """(aggregate terms, transform keys seen) across ``roots``. Each aggregate's
    home path comes from the elaborator's map (fallback: the source anchor)."""
    terms: Dict[ValueKey, Term] = {}
    transforms: List[TransformKey] = []
    for root in roots:
        for k in walk_value_keys(root):
            if isinstance(k, AggregateKey) and k not in terms:
                grain = (
                    k.partition_keys if k.partition_keys is not None
                    else query_grain
                )
                terms[k] = Aggregate(
                    home=home, recipe=k, grain=grain,
                    home_path=home_paths.get(k, source_anchor_path(k.source)),
                )
            elif isinstance(k, TransformKey):
                transforms.append(k)
    return terms, transforms


def _add_transform_terms(
    *, terms: Dict[ValueKey, Term], transforms: List[TransformKey],
) -> None:
    for tk in transforms:
        if tk in terms or not isinstance(tk.input, AggregateKey):
            continue
        if tk.op in TIME_TRANSFORMS and tk.time_key is None:
            continue
        inner = terms.get(tk.input)
        if isinstance(inner, Aggregate):
            terms[tk] = Transform(input=inner, recipe=tk)


def _terms_for(
    roots: List[ValueKey], *, home: Optional[DatasetT], query_grain: Grain,
    home_paths: Dict[AggregateKey, Tuple[str, ...]],
) -> Dict[ValueKey, Term]:
    """One term per unique aggregate/transform key, memoized by key identity.

    An aggregate's grain is its explicit partition grain, else the query grain;
    a transform term exists where its input is an aggregate with a term (the
    checker owns rejecting the rest — inert here).
    """
    if home is None:
        return {}
    terms, transforms = _aggregate_terms(
        roots, home=home, query_grain=query_grain, home_paths=home_paths,
    )
    _add_transform_terms(terms=terms, transforms=transforms)
    return terms


_FIELD: PositionVerdict = "field"
_MEASURE: PositionVerdict = "measure"
_VERDICT_OF: Dict[MaskTyping, PositionVerdict] = {
    MaskTyping.FIELD: _FIELD, MaskTyping.MEASURE: _MEASURE,
}


def _entry(
    root: ValueKey,
    *,
    verdict: PositionVerdict,
    home: Optional[DatasetT],
    query_grain: Grain,
    terms: Dict[ValueKey, Term],
) -> ExpressionEntry:
    """Type one top-level expression: grain and broadcast insertions (D5)."""
    agg_terms: List[Aggregate] = []
    seen: set = set()
    for k in walk_value_keys(root):
        if isinstance(k, AggregateKey) and k in terms and k not in seen:
            seen.add(k)
            term = terms[k]
            if isinstance(term, Aggregate):
                agg_terms.append(term)
    grains = {t.grain for t in agg_terms}
    broadcasts: Tuple[Broadcast, ...] = ()
    grain = query_grain
    if len(grains) > 1:
        union = Grain.EMPTY
        for g in grains:
            union = union.union(g)
        broadcasts = tuple(
            Broadcast(source=t, into=union)
            for t in agg_terms
            if t.grain != union
        )
        grain = union
    elif len(grains) == 1 and verdict == "field":
        grain = next(iter(grains))
    return ExpressionEntry(
        verdict=verdict, home=home, grain=grain, broadcasts=broadcasts,
    )


def flatten_collision_message(flat_name: str) -> str:
    return (
        f"Stage column name collision on {flat_name!r}: two projected "
        f"columns flatten to the same downstream name. Give one an "
        f"explicit measure `name` to disambiguate."
    )


def check_computed_dimension(*, name, bound, distinct_dimension_values) -> None:  # NOSONAR(S3776) — sequential fail-closed guard checks over one shared walk (all_keys / transforms / inner_aggs); each arm raises its own contract error, and extracting them scatters the shared state and the ordered narrative.
    """Grain rules for one computed dimension."""
    all_keys = list(walk_value_keys(bound.value_key))
    transforms = [k for k in all_keys if isinstance(k, TransformKey)]
    for tk in transforms:
        inner_aggs = [
            k for k in walk_value_keys(tk.input) if isinstance(k, AggregateKey)
        ]
        # Two permanent type rules (closure-axiom typed residue), not deferrals.
        if not inner_aggs:
            raise ValueError(
                f"The transform '{tk.op}' inside computed dimension {name!r} "
                f"must take an aggregate input — a transform acts on "
                f"aggregates, e.g. {tk.op}(sum(amount, partition_by=city))."
            )
        ungrained = [a for a in inner_aggs if a.partition_keys is None]
        if ungrained:
            raise ValueError(
                f"The aggregate '{ungrained[0].agg}"
                f"({dotted_key_display(ungrained[0].source)})' inside the transform in computed "
                f"dimension {name!r} must declare partition_by= explicitly: "
                f"the ungrained default (the query's own dimensions) would "
                f"include the dimension being defined."
            )
    aggs = [k for k in all_keys if isinstance(k, AggregateKey)]
    if not aggs:
        return  # row-level
    if not distinct_dimension_values:
        raise DistinctDimensionValuesError(
            f"Computed dimension {name!r} references an aggregate, so it cannot "
            f"be used with distinct_dimension_values=False (raw rows). Remove the "
            f"flag (the default aggregates) or drop the aggregate from the "
            f"dimension."
        )
    for agg in aggs:
        if agg.partition_keys is None:
            raise ValueError(
                f"The aggregate inside computed dimension {name!r} must declare "
                f"the grain it aggregates over with partition_by=, e.g. "
                f"'CASE WHEN sum(amount, partition_by=city) > 5000 THEN 1 ELSE 0 END'. "
                f"Without partition_by the group key is a function of the query's "
                f"own dimensions and adds no grouping."
            )


def check_opaque_grouping_dim(
    *, full_name: str, dim_type: Optional[DataType], will_group_by: bool,
) -> None:
    """Reject an opaque dimension the query will GROUP BY."""
    if not will_group_by:
        return
    if dim_type is not None and dim_type.is_opaque:
        raise ValueError(
            f"Column '{full_name}' cannot be used as a dimension: its type does "
            f"not support the GROUP BY / DISTINCT this query requires. Define a "
            f"derived column that extracts a comparable value instead, e.g. "
            f"sql=\"payload->>'status'\" with type TEXT."
        )


def _temporal_axis_transforms(vk: ValueKey, *, is_dimension: bool):
    """Time-ordered transforms whose grain must contain their axis: any transform
    in a dimension expression, and every transform reachable inside an
    aggregation-source constituent of a measure/filter/order expression — nested
    ones included, mirroring the dimension arm, so a nested time transform cannot
    evade Axiom 11.5 (a top-level transform measure carries its bucket through the
    windowed producer instead)."""
    if is_dimension:
        yield from (k for k in walk_value_keys(vk) if isinstance(k, TransformKey))
        return
    for k in walk_value_keys(vk):
        if isinstance(k, AggregateKey):
            for c in attached_inputs(k):
                if isinstance(c, TransformKey):
                    yield from (
                        t for t in walk_value_keys(c)
                        if isinstance(t, TransformKey)
                    )


def check_dimension_temporal_axis(
    declared_measures, *, bound_filters=(), order_specs=(),
) -> None:
    """Fail closed if a time-ordered transform — in a dimension, or aggregated as
    a source constituent in a measure / filter / order expression — evaluates at a
    grain not containing its time axis."""
    roots = [
        (dm.bound.value_key, dm.is_dimension) for dm in declared_measures
    ]
    roots += [(bf.value_key, False) for bf in bound_filters]
    roots += [(sp.bound.value_key, False) for sp in order_specs]
    for vk, is_dimension in roots:
        for tk in _temporal_axis_transforms(vk, is_dimension=is_dimension):
            if tk.op not in TIME_TRANSFORMS or tk.time_key is None:
                continue
            if tk.time_key not in regroup_root_grain(tk):
                axis = dotted_key_display(tk.time_key)
                raise NotImplementedError(
                    f"A time-ordered transform '{tk.op}' evaluates at a grain "
                    f"that does not contain its time axis '{axis}'; a producer "
                    f"bucketed by time joined back on the coarser grain would "
                    f"duplicate result rows. Include the time key in the "
                    f"aggregate's partition_by= so the transform accumulates "
                    f"within its own grain."
                )


def check_windowed_time_dimension(*, resolved: bool) -> None:
    """A windowed measure needs a resolvable query time dimension (both windowed guard sites)."""
    if resolved:
        return
    raise ValueError(
        "Windowed measure could not resolve its time dimension. Add a single "
        "time_dimensions entry, or set main_time_dimension to select among "
        "multiple time dimensions."
    )


def check_time_dimension_date_range(*, full_name: str, date_range) -> None:
    """A null date_range bound is inexpressible as a range — fail loudly rather than emit ``BETWEEN x AND NULL``."""
    if any(bound is None for bound in date_range):
        raise ValueError(
            f"TimeDimension {full_name!r} has a date_range with a "
            f"null bound ({date_range!r}); a null bound cannot be expressed "
            f"as a range. Use a one-sided filter (e.g. '>=' / '<=') instead."
        )


def check_time_dimension_column(
    *,
    name: str,
    column_type: Optional[DataType],
    upstream_granularity: Optional[TimeGranularity],
    requested_granularity: TimeGranularity,
) -> None:
    """A time dimension's column must be temporal (DATE / TIMESTAMP); a bucketed column — stage, query-backed cache, or hand-set ``Column.granularity`` — re-buckets only to the same or a nesting-coarser granularity (closure Axiom 9). One message for all three origins."""
    if column_type not in (DataType.DATE, DataType.TIMESTAMP):
        raise TimeDimensionColumnError(
            f"TimeDimension {name!r} must reference a temporal column "
            f"(DATE / TIMESTAMP); got column type {column_type!r}."
        )
    if (
        upstream_granularity is not None
        and requested_granularity != upstream_granularity
        and not upstream_granularity.nests_into(requested_granularity)
    ):
        raise TimeDimensionColumnError(
            f"TimeDimension {name!r} cannot re-bucket to "
            f"'{requested_granularity.value}': its column is already bucketed "
            f"at '{upstream_granularity.value}', which does not nest into "
            f"'{requested_granularity.value}'. Request the same or a "
            f"nesting-coarser granularity, or bucket the raw column instead."
        )


def _time_search_children(key: ValueKey) -> List[ValueKey]:
    if isinstance(key, AggregateKey):
        # A transform constituent lives in the source (or a composite parameter);
        # descend so its no-time-dimension error reaches an aggregated transform.
        return [
            key.source,
            *[a for a in key.args if isinstance(
                a, (AggregateKey, TransformKey, ArithmeticKey, ScalarCallKey))],
            *[v for _, v in key.kwargs if isinstance(
                v, (AggregateKey, TransformKey, ArithmeticKey, ScalarCallKey))],
        ]
    if isinstance(key, TransformKey):
        return [key.input]
    if isinstance(key, ArithmeticKey):
        return list(key.operands)
    if isinstance(key, ScalarCallKey):
        return [
            a for a in key.args
            if isinstance(
                a, (AggregateKey, TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey, InKey),
            )
        ]
    if isinstance(key, BetweenKey):
        return [key.column, key.low, key.high]
    if isinstance(key, InKey):
        return [key.column]
    return []


def _find_unresolved_time_needing_op(key: ValueKey) -> Optional[str]:
    if (
        isinstance(key, TransformKey)
        and key.op in TIME_TRANSFORMS
        and key.time_key is None
    ):
        return key.op
    for child in _time_search_children(key):
        found = _find_unresolved_time_needing_op(child)
        if found:
            return found
    return None


def check_time_transforms_resolved(*, roots) -> None:
    """A time-needing transform still at ``time_key=None`` after attachment means no resolvable TD."""
    for vk in roots:
        op = _find_unresolved_time_needing_op(vk)
        if op is not None:
            raise ValueError(
                f"Transform '{op}' requires an unambiguous time "
                f"dimension. Add a single time_dimensions entry, or "
                f"set main_time_dimension to select among multiple "
                f"time dimensions."
            )


def check_window_duration(*, window_val) -> None:
    """The ``window=`` duration is a well-formed compact string;
    every aggregation accepts it — no aggregation allowlist."""
    if not isinstance(window_val, str):
        raise ValueError(
            f"Window duration must be a compact duration string like '90d', got "
            f"{window_val!r}. Use syntax like '1y2m3w5d6h7min8s'."
        )
    parse_window_duration(window_val)  # raises on empty / malformed


def _first_row_leaf(key: ValueKey, *, exempt: frozenset) -> Optional[ValueKey]:
    """First row-level leaf of ``key`` (a transform: of its input) not in
    ``exempt``, or None. Aggregates and nested transforms are opaque."""
    if isinstance(key, TransformKey):
        return _first_opaque_row_leaf(key.input, exempt=exempt)
    return _first_opaque_row_leaf(key, exempt=exempt)


def _first_opaque_row_leaf(key: ValueKey, *, exempt: frozenset) -> Optional[ValueKey]:
    if key in exempt or isinstance(key, (AggregateKey, TransformKey)):
        return None
    if isinstance(key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        return key
    for c in key.children():
        found = _first_opaque_row_leaf(c, exempt=exempt)
        if found is not None:
            return found
    return None


#: Per-op input rules beyond the total row-leaf rule: ops rejecting a boolean input.
_TRANSFORM_INPUT_RULES = {"reject_boolean": frozenset({"change", "change_pct"})}


def check_transform_inputs(*, roots, projected_grain_keys: frozenset) -> None:
    """Judge every transform node's own input (pre-lowering): no boolean-shaped
    input for ``change`` / ``change_pct``, and no row-level leaf refining the
    consumer grain — a leaf that is not a projected query dimension."""
    for root in roots:
        for k in walk_value_keys(root):
            if not isinstance(k, TransformKey):
                continue
            if k.op in _TRANSFORM_INPUT_RULES["reject_boolean"] and is_boolean_shaped(k.input):
                raise ValueError(
                    f"'{k.op}' cannot consume a boolean-shaped predicate: its "
                    f"desugared arithmetic subtracts the shifted series, and "
                    f"subtraction over truth values is undefined. Shift the "
                    f"predicate itself with time_shift, or compare the shifted "
                    f"values instead."
                )
            leaf = _first_row_leaf(k, exempt=projected_grain_keys)
            if leaf is None:
                continue
            disp = dotted_key_display(leaf)
            raise ValueError(
                f"Transform '{k.op}' cannot consume the row-level "
                f"(non-aggregate) leaf '{disp}', which refines the query "
                f"grain: it would inflate the base grain to one row per "
                f"(bucket, {disp}-value). Aggregate the leaf — e.g. "
                f"{k.op}({disp}:sum) — project '{disp}' as a query dimension, "
                f"or compute it in an earlier stage of a multi-stage "
                f"`source_queries` model."
            )


def check_partition_key_resolves(
    *, label: str, pk: ValueKey, is_query_dim: bool, ambiguous: bool,
    maps_to_bucket: bool, lenient: bool, available_dims: Sequence[str],
) -> None:
    """Every rank-family partition_by column resolves to a query dim/td; a lenient key declares a finer producer grain."""
    if is_query_dim:
        return
    if ambiguous:
        raise ValueError(
            f"{label}: partition_by column "
            f"'{dotted_key_display(pk)}' is ambiguous — it is a "
            f"time dimension at multiple granularities. Partition by a "
            f"single query dimension instead."
        )
    if maps_to_bucket or lenient:
        return
    raise ValueError(
        f"{label}: partition_by column "
        f"'{dotted_key_display(pk)}' is not a query dimension. "
        f"Add it to dimensions/time_dimensions, or choose one of: "
        f"{', '.join(available_dims) or '(none)'}."
    )


def check_transform_partition_keys_in_operand_grain(
    *, roots, query_grain: Grain, active_bucket: Optional[ValueKey],
) -> None:
    """A rank-family transform's own partition keys name operand-grain members (Axiom 11.2)."""
    for root in roots:
        for k in walk_value_keys(root):
            if not (isinstance(k, TransformKey) and k.op in RANK_FAMILY_TRANSFORMS):
                continue
            operand = transform_operand_grain(
                k.input, query_grain=query_grain, active_bucket=active_bucket,
            )
            for pk in k.partition_keys:
                if pk in operand:
                    continue
                members = ", ".join(sorted(dotted_key_display(m) for m in operand))
                raise ValueError(
                    f"Transform '{k.op}': partition_by column "
                    f"'{dotted_key_display(pk)}' is not a member of the transform's "
                    f"operand grain ({members}); a rank-family transform partitions "
                    f"its operand's cells. Add it to the inner aggregate's "
                    f"partition_by=, or partition by one of: {members or '(none)'}."
                )


def check_partition_key_attributable(
    *, label: str, pk: ValueKey, attributable: bool, reason: Optional[str],
) -> None:
    """A partition key reached over a join must be attributable from the aggregate's root; the compiler resolves ``attributable``/``reason``."""
    if attributable:
        return
    raise ValueError(
        f"{label}: partition_by column '{dotted_key_display(pk)}' {reason}; "
        f"every partition key must be attributable from the aggregate's root — "
        f"declare join cardinality or a covering unique key on the target."
    )


def check_local_producer_inputs_safe(
    *, alias: Optional[str], host: str,
    ranked_crossings: Sequence[Tuple[str, str]],
    gated_crossings: Sequence[str],
    source_crossings: Sequence[str] = (),
) -> None:
    """Per-role crossing-input safety for a HOST-rooted producer answer; crossings are the compiler-resolved unproven hops."""
    remedy = "declare join cardinality or a covering unique key on the target"
    if ranked_crossings:
        leaf, hop = ranked_crossings[0]
        raise ValueError(
            f"Aggregate {alias!r} ranks/reads by {leaf}, which crosses an "
            f"unproven join hop to {hop} from {host}; "
            f"{remedy}."
        )
    if gated_crossings:
        raise ValueError(
            f"Aggregate {alias!r} reads an input across an unproven join "
            f"hop to {gated_crossings[0]} from {host}; {remedy}."
        )
    if source_crossings:
        raise ValueError(
            f"Aggregate {alias!r} reads its source across an unproven or fanning join "
            f"hop to {source_crossings[0]} from {host}: a column of {host} cannot be "
            f"aggregated across a to-many target — aggregate the target column directly "
            f"({source_crossings[0]}.<column>:<aggregation>), or declare a to-one "
            f"cardinality or a covering unique key if the hop is to-one."
        )


def check_cross_model_source_resolves(*, target_path, host_name: str) -> NoReturn:
    """An unresolvable cross-model source path; bind resolves the path first, so the compiler calls this only on that invariant's breach."""
    raise ValueError(  # pragma: no cover — bind resolved the path already
        f"Cross-model aggregate source path {target_path!r} does not resolve "
        f"to a model from {host_name}."
    )


def check_cross_model_partition_keys_attributable(
    *, alias: Optional[str], root_name: str, explicit: bool,
    unattributable: Sequence[Tuple[str, str]],
) -> None:
    """An unattributable EXPLICIT partition key on a cross-model aggregate is a hard error; an implicit grain broadcasts instead. ``unattributable`` = compiler-resolved (name, reason) pairs."""
    if not explicit or not unattributable:
        return
    name, reason = unattributable[0]
    raise ValueError(
        f"Cross-model aggregate {alias!r} declares partition_by="
        f"{name}, which {reason}; every explicit "
        f"partition key must be attributable from {root_name} — declare "
        f"join cardinality or a covering unique key on the target."
    )


def check_windowed_time_axis_attributable(
    *, alias: Optional[str], root_name: str, active_td_name: Optional[str],
    attributable: bool,
) -> None:
    """A windowed aggregate needs the query's active time dimension, attributable from its root."""
    if active_td_name is None:
        raise ValueError(
            f"Windowed aggregate {alias!r} has no active time "
            f"dimension; add a single time_dimensions entry."
        )
    if not attributable:
        raise ValueError(
            f"Windowed aggregate {alias!r} needs the query's "
            f"active time dimension ('{active_td_name}') "
            f"attributable from {root_name}, but it crosses a fanning join; "
            f"declare join cardinality or a covering unique key on the target."
        )


def check_cross_model_inputs_safe(
    *, alias: Optional[str], root_name: str,
    unsafe_input_hops: Sequence[str],
    unattributable_arg_leaves: Sequence[Tuple[str, str]],
) -> None:
    """Every input of a cross-model aggregate must be attributable from its root; hops / (leaf, reason) pairs are the compiler-resolved violations, an explicit argument's reported first."""
    remedy = "declare join cardinality or a covering unique key on the target"
    if unattributable_arg_leaves:
        leaf, reason = unattributable_arg_leaves[0]
        raise ValueError(
            f"Cross-model aggregate {alias!r} "
            f"ranks/reads by {leaf}, which is not attributable from "
            f"{root_name} ({reason}); {remedy}."
        )
    if unsafe_input_hops:
        raise ValueError(
            f"Cross-model aggregate {alias!r} "
            f"reads an input across an unproven join hop to {unsafe_input_hops[0]} from "
            f"{root_name}; {remedy}."
        )


def check_input_dependencies_analyzable(
    *, alias: Optional[str], column: Optional[str],
) -> None:
    """An aggregate input whose dependency no dialect can analyse for join
    dependencies is unsafe, never 'crosses nothing'. Callers invoke
    this ONLY once the input closure has come back unanalysable, so it always
    raises; ``column`` names the offending derived column when one can be
    identified (the common case), else ``None`` (e.g. an unresolvable
    expression-default qualifier), which still fails closed."""
    if column is None:
        raise ValueError(
            f"Aggregate {alias!r} has an input dependency whose definition no "
            f"supported dialect can analyse for join dependencies; an "
            f"unanalyzable dependency is unsafe. Fix the input's SQL, or remove "
            f"it from the aggregate."
        )
    raise ValueError(
        f"Aggregate {alias!r} names derived column {column!r}, whose definition "
        f"no supported dialect can analyse for join dependencies; an unanalyzable "
        f"dependency is unsafe. Fix the column's SQL, or remove it from the "
        f"aggregate."
    )


def check_filter_dependencies_analyzable(
    *, filter_text: str, column: Optional[str],
) -> NoReturn:
    """A filter conjunct whose dependency closure no dialect can analyse for join
    dependencies is unsafe, never 'crosses nothing'. ``column`` names the
    offending derived column (the common case for a filter, which references
    columns by name), else ``None``."""
    if column is None:
        raise ValueError(
            f"Filter {filter_text!r} has a dependency whose definition no "
            f"supported dialect can analyse for join dependencies; an "
            f"unanalyzable dependency is unsafe. Fix the referenced column's SQL, "
            f"or remove the filter."
        )
    raise ValueError(
        f"Filter {filter_text!r} names derived column {column!r}, whose "
        f"definition no supported dialect can analyse for join dependencies; an "
        f"unanalyzable dependency is unsafe. Fix the column's SQL, or remove the "
        f"filter."
    )


def check_association_windowed_ranked(*, alias: str, windowed_or_ranked: bool) -> None:
    """window=/first/last cannot associate — the pick per entity is undefined."""
    if windowed_or_ranked:
        raise SlayerError(
            f"Aggregate {alias!r} needs distinct-entity association over an "
            f"unattributable dimension, which is unsupported in combination with "
            f"window=/first/last; drop the window/first-last or attribute the "
            f"dimension."
        )


def check_association_root_unique_key(
    *, alias: str, root_name: str, has_unique_key: bool,
) -> None:
    """The association root must declare a unique key to dedup its entities."""
    if not has_unique_key:
        raise SlayerError(
            f"Aggregate {alias!r} needs distinct-entity association, but its root "
            f"model {root_name!r} declares no primary or unique key to deduplicate "
            f"entities by; declare a primary or unique key on {root_name!r}."
        )


def check_parameter_determined(
    *, alias: str, param_name: str, grain_display: str, determined: bool,
) -> None:
    """One rule for every aggregation parameter: it must be determined by the
    grain of the dataset the aggregation runs over — a grain member, a cell of
    that dataset (an aggregate grained within it), or a column the grain pins
    over to-one hops. The residue names the parameter, the grain, and the
    remedy."""
    if determined:
        return
    raise SlayerError(
        f"Aggregation {alias!r} parameter {param_name!r} is not determined by "
        f"the operand grain ({grain_display}); the aggregation reads one value "
        f"per cell of that grain. Aggregate the parameter to that grain, or add "
        f"its determining keys to the operand's partition_by=."
    )




def check_reaggregation_no_window(*, alias: str, window_val) -> None:
    """window= on the outer aggregation has no defined cell-time semantics."""
    if window_val is not None:
        raise SlayerError(
            f"Re-aggregation {alias!r} cannot carry window= on its outer "
            f"aggregation; apply the window inside the operand or consume the "
            f"re-aggregated value through a transform."
        )


def check_reaggregation_partition_key_is_query_dim(
    *, alias: str, offending: Optional[str],
) -> None:
    """Every explicit outer partition key must be a query dimension; ``offending`` = the key's display name when it is not."""
    if offending is not None:
        raise ValueError(
            f"Re-aggregation {alias!r} declares partition_by="
            f"{offending}, which is not a query dimension; "
            f"every explicit partition key must be a query dimension — "
            f"add it to dimensions/time_dimensions."
        )


def check_reaggregation_dims_attributable(
    *, alias: str, mode: str, unattributable_names: Sequence[str],
) -> None:
    """Unattributable outer dims are a hard error under to_many_handling='error'; associate/broadcast resolution stays compiler-side."""
    if mode != "error" or not unattributable_names:
        return
    names = ", ".join(unattributable_names)
    raise ValueError(
        f"Re-aggregation {alias!r} cannot attribute dimension(s) {names} "
        f"to the operand dataset under to_many_handling='error'; add them "
        f"to the inner partition_by= so the operand is grained by them, "
        f"or choose 'broadcast'/'associate'."
    )


_RAW_ROW_FIX_HINT = (
    "Either remove the measure reference, or set "
    "distinct_dimension_values=True (the default) to keep the "
    "auto-aggregating behaviour."
)


def check_raw_rows_filter_measure_ref(*, offending: Optional[str]) -> None:
    """Raw-rows mode (distinct_dimension_values=False) rejects measure references in filters; ``offending`` = the raw filter string when one does."""
    if offending is not None:
        raise DistinctDimensionValuesError(
            f"distinct_dimension_values=False rejects measure references, "
            f"but filter {offending!r} contains one. {_RAW_ROW_FIX_HINT}"
        )


def check_raw_rows_order_measure_ref(
    *, contains: Optional[str] = None, saved_name: Optional[str] = None,
    source_name: Optional[str] = None, saved_dotted: Optional[str] = None,
) -> None:
    """Raw-rows mode rejects measure references in ORDER BY; at most one offense per call, resolution stays compiler-side."""
    if contains is not None:
        raise DistinctDimensionValuesError(
            f"distinct_dimension_values=False rejects measure "
            f"references, but order item {contains!r} contains one. "
            f"{_RAW_ROW_FIX_HINT}"
        )
    if saved_name is not None:
        raise DistinctDimensionValuesError(
            f"distinct_dimension_values=False rejects measure references, "
            f"but order item {saved_name!r} resolves to a saved measure on "
            f"{source_name or 'the source model'!r}. "
            f"{_RAW_ROW_FIX_HINT}"
        )
    if saved_dotted is not None:
        raise DistinctDimensionValuesError(
            f"distinct_dimension_values=False rejects measure references, "
            f"but order item {saved_dotted!r} resolves to a saved measure. "
            f"{_RAW_ROW_FIX_HINT}"
        )


def check_raw_rows_no_aggregate_slots(*, offender: str) -> NoReturn:
    """An aggregate-phase slot under raw-rows mode came from a filter or order item (measures were rejected upstream)."""
    raise DistinctDimensionValuesError(
        f"distinct_dimension_values=False rejects measure references, but "
        f"this query references the aggregation {offender!r} in its "
        f"filters or order. Either remove the measure reference, or set "
        f"distinct_dimension_values=True (the default) to keep the "
        f"auto-aggregating behaviour."
    )


def check_order_target_has_slot(*, type_name: str) -> NoReturn:
    """An order target with no materialisable slot would be silently dropped."""
    raise PositionTypingError(
        f"ORDER BY expression is not supported: "
        f"{type_name} has no materialisable "
        f"slot. Order by an aggregate, a transform, a composite "
        f"arithmetic / scalar expression, a dimension, or declare the "
        f"expression as a measure and order by its name."
    )


def build_environment(
    *,
    prebound: PreboundQuery,
    home: Optional[DatasetT],
    dim_keys: frozenset,
    row_agg_set: frozenset,
    filter_typings: List[ConjunctTyping],
    home_paths: Optional[Dict[AggregateKey, Tuple[str, ...]]] = None,
) -> ElaboratedQuery:
    """Assemble the typing environment for one typed, split prebound query."""
    home_paths = home_paths or {}
    query_grain = Grain.of(dim_keys)
    n_leading = prebound.n_dims + prebound.n_time_dimensions
    dim_roots = [
        dm.bound.value_key for dm in prebound.declared_measures[:n_leading]
    ]
    measure_roots = [
        dm.bound.value_key for dm in prebound.declared_measures[n_leading:]
    ]
    filter_roots = [bf.value_key for bf in prebound.bound_filters]
    order_roots = [spec.bound.value_key for spec in prebound.order_specs]

    terms = _terms_for(
        [*dim_roots, *measure_roots, *filter_roots, *order_roots],
        home=home, query_grain=query_grain, home_paths=home_paths,
    )

    def _typed_verdict(root: ValueKey) -> PositionVerdict:
        return _VERDICT_OF[type_position_conjunct(
            root,
            dim_keys=dim_keys,
            row_agg_set=row_agg_set,
            has_measure_position=prebound.distinct_dimension_values is not False,
            position="order",
        ).typing]

    def _entries(
        roots: List[ValueKey], verdicts: List[PositionVerdict],
    ) -> tuple:
        return tuple(
            _entry(
                root, verdict=v, home=home,
                query_grain=query_grain, terms=terms,
            )
            for root, v in zip(roots, verdicts)
        )

    return ElaboratedQuery(
        dimensions=_entries(dim_roots, [_FIELD] * len(dim_roots)),
        measures=_entries(measure_roots, [_MEASURE] * len(measure_roots)),
        filters=_entries(
            filter_roots, [_VERDICT_OF[ct.typing] for ct in filter_typings],
        ),
        order=_entries(order_roots, [_typed_verdict(r) for r in order_roots]),
        terms=terms,
    )
