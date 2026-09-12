"""Builds the ``ElaboratedQuery`` typing environment from a typed prebound (D2).

Split from ``elaborate.py`` so the transitional in-planner call needs no import
cycle: this module never imports the planner. The environment informs nothing
yet; the checker owns the migrated algebra guards (DEV-1871 G9+), invoked from
the compiler at their original checkpoints until the G16 reroute.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Union

from slayer.core.enums import DataType
from slayer.core.errors import DistinctDimensionValuesError
from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    Grain,
    InKey,
    ScalarCallKey,
    TransformKey,
    ValueKey,
    walk_value_keys,
)
from slayer.core.models import SlayerModel
from slayer.core.refs import dotted_key_display
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.compile.regroup import (
    ConjunctTyping,
    regroup_root_grain,
    type_position_conjunct,
)
from slayer.ir.elaborated import ElaboratedQuery, ExpressionEntry, Term
from slayer.ir.terms import (
    Aggregate,
    Broadcast,
    DatasetT,
    ModelDataset,
    StageDataset,
    Transform,
)

from slayer.engine.prebound import PreboundQuery


def home_dataset(
    *, scope: Union[ModelScope, StageSchema], model: Optional[SlayerModel],
) -> Optional[DatasetT]:
    """The query's root dataset: model-backed for a ModelScope, stage-backed otherwise."""
    if isinstance(scope, StageSchema):
        return StageDataset(stage_name=scope.relation_name)
    if model is not None:
        return ModelDataset(data_source=model.data_source, model_name=model.name)
    return None


def _terms_for(
    roots: List[ValueKey], *, home: Optional[DatasetT], query_grain: Grain,
) -> Dict[ValueKey, Term]:
    """One term per unique aggregate/transform key, memoized by key identity.

    An aggregate's grain is its explicit partition grain, else the query grain;
    a transform term exists where its input is an aggregate with a term (the
    checker owns rejecting the rest — inert here).
    """
    terms: Dict[ValueKey, Term] = {}
    if home is None:
        return terms
    transforms: List[TransformKey] = []
    for root in roots:
        for k in walk_value_keys(root):
            if isinstance(k, AggregateKey) and k not in terms:
                terms[k] = Aggregate(
                    home=home,
                    recipe=k,
                    grain=(
                        k.partition_keys if k.partition_keys is not None
                        else query_grain
                    ),
                )
            elif isinstance(k, TransformKey):
                transforms.append(k)
    for tk in transforms:
        if tk in terms or not isinstance(tk.input, AggregateKey):
            continue
        if tk.op in TIME_TRANSFORMS and tk.time_key is None:
            continue
        inner = terms.get(tk.input)
        if isinstance(inner, Aggregate):
            terms[tk] = Transform(input=inner, recipe=tk)
    return terms


def _entry(
    root: ValueKey,
    *,
    verdict: str,
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


def check_computed_dimension(*, name, bound, distinct_dimension_values) -> None:  # NOSONAR(S3776) — sequential fail-closed guard checks over one shared walk (all_keys / transforms / inner_aggs); each arm raises its own contract error, and extracting them scatters the shared state and the ordered narrative.
    """Grain rules for one computed dimension (DEV-1871 G9, was ``_guard_computed_dimension``)."""
    all_keys = list(walk_value_keys(bound.value_key))
    transforms = [k for k in all_keys if isinstance(k, TransformKey)]
    for tk in transforms:
        inner_aggs = [
            k for k in walk_value_keys(tk.input) if isinstance(k, AggregateKey)
        ]
        # A transform is legal in a dimension only over an explicitly-grained aggregate.
        if not inner_aggs or any(a.partition_keys is None for a in inner_aggs):
            raise NotImplementedError(
                f"A transform inside computed dimension {name!r} must wrap an "
                f"explicitly-grained aggregate — declare partition_by= on the "
                f"aggregate it transforms (DEV-1868)."
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
                f"'CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END'. "
                f"Without partition_by the group key is a function of the query's "
                f"own dimensions and adds no grouping."
            )


def check_opaque_grouping_dim(
    *, full_name: str, dim_type: Optional[DataType], will_group_by: bool,
) -> None:
    """Reject an opaque dimension the query will GROUP BY (DEV-1871 G9, was ``_reject_opaque_grouping_dim``)."""
    if not will_group_by:
        return
    if dim_type is not None and dim_type.is_opaque:
        raise ValueError(
            f"Column '{full_name}' cannot be used as a dimension: its type does "
            f"not support the GROUP BY / DISTINCT this query requires. Define a "
            f"derived column that extracts a comparable value instead, e.g. "
            f"sql=\"payload->>'status'\" with type TEXT."
        )


def check_dimension_temporal_axis(declared_measures) -> None:
    """Fail closed if a time-ordered transform inside a dimension evaluates at a grain not containing its time axis (DEV-1871 G10, was ``_guard_dimension_temporal_axis``)."""
    for dm in declared_measures:
        if not dm.is_dimension:
            continue
        for tk in walk_value_keys(dm.bound.value_key):
            if not isinstance(tk, TransformKey):
                continue
            if tk.op not in TIME_TRANSFORMS or tk.time_key is None:
                continue
            if tk.time_key not in regroup_root_grain(tk):
                axis = dotted_key_display(tk.time_key)
                raise NotImplementedError(
                    f"A time-ordered transform '{tk.op}' inside a computed "
                    f"dimension evaluates at a grain that does not contain its "
                    f"time axis '{axis}'; a producer bucketed by time joined back "
                    f"on the coarser grain would duplicate result rows. Include "
                    f"the time key in the aggregate's partition_by= so the "
                    f"transform accumulates within its own grain."
                )


def check_windowed_time_dimension(*, resolved: bool) -> None:
    """A windowed measure needs a resolvable query time dimension (DEV-1871 G10, both windowed guard sites)."""
    if resolved:
        return
    raise ValueError(
        "Windowed measure could not resolve its time dimension. Add a single "
        "time_dimensions entry, or set main_time_dimension to select among "
        "multiple time dimensions."
    )


def check_time_dimension_date_range(*, full_name: str, date_range) -> None:
    """A null date_range bound is inexpressible as a range — fail loudly rather than emit ``BETWEEN x AND NULL`` (DEV-1871 G10)."""
    if any(bound is None for bound in date_range):
        raise ValueError(
            f"TimeDimension {full_name!r} has a date_range with a "
            f"null bound ({date_range!r}); a null bound cannot be expressed "
            f"as a range. Use a one-sided filter (e.g. '>=' / '<=') instead."
        )


def _find_unresolved_time_needing_op(key: ValueKey) -> Optional[str]:
    if isinstance(key, TransformKey):
        if key.op in TIME_TRANSFORMS and key.time_key is None:
            return key.op
        return _find_unresolved_time_needing_op(key.input)
    if isinstance(key, ArithmeticKey):
        for o in key.operands:
            found = _find_unresolved_time_needing_op(o)
            if found:
                return found
        return None
    if isinstance(key, ScalarCallKey):
        for a in key.args:
            if isinstance(
                a, (TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey),
            ):
                found = _find_unresolved_time_needing_op(a)
                if found:
                    return found
        return None
    if isinstance(key, BetweenKey):
        for k in (key.column, key.low, key.high):
            found = _find_unresolved_time_needing_op(k)
            if found:
                return found
        return None
    if isinstance(key, InKey):
        return _find_unresolved_time_needing_op(key.column)
    return None


def check_time_transforms_resolved(*, roots) -> None:
    """A time-needing transform still at ``time_key=None`` after attachment means no resolvable TD (DEV-1871 G10)."""
    for vk in roots:
        op = _find_unresolved_time_needing_op(vk)
        if op is not None:
            raise ValueError(
                f"Transform '{op}' requires an unambiguous time "
                f"dimension. Add a single time_dimensions entry, or "
                f"set main_time_dimension to select among multiple "
                f"time dimensions."
            )


def build_environment(
    *,
    prebound: PreboundQuery,
    home: Optional[DatasetT],
    dim_keys: frozenset,
    row_agg_set: frozenset,
    filter_typings: List[ConjunctTyping],
) -> ElaboratedQuery:
    """Assemble the typing environment for one typed, split prebound query."""
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
        home=home, query_grain=query_grain,
    )

    def _typed_verdict(root: ValueKey) -> str:
        return type_position_conjunct(
            root,
            dim_keys=dim_keys,
            row_agg_set=row_agg_set,
            has_measure_position=prebound.distinct_dimension_values is not False,
            position="order",
        ).typing.value

    def _entries(roots: List[ValueKey], verdicts: List[str]) -> tuple:
        return tuple(
            _entry(
                root, verdict=v, home=home,
                query_grain=query_grain, terms=terms,
            )
            for root, v in zip(roots, verdicts)
        )

    return ElaboratedQuery(
        dimensions=_entries(dim_roots, ["field"] * len(dim_roots)),
        measures=_entries(measure_roots, ["measure"] * len(measure_roots)),
        filters=_entries(
            filter_roots, [ct.typing.value for ct in filter_typings],
        ),
        order=_entries(order_roots, [_typed_verdict(r) for r in order_roots]),
        terms=terms,
    )
