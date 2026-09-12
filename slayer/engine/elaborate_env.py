"""Builds the ``ElaboratedQuery`` typing environment from a typed prebound (D2).

Split from ``elaborate.py`` so the transitional in-planner call needs no import
cycle: this module never imports the planner. The environment informs nothing
yet; the checker owns the migrated algebra guards (DEV-1871 G9+), invoked from
the compiler at their original checkpoints until the G16 reroute.
"""

from __future__ import annotations

from typing import AbstractSet, Dict, List, NoReturn, Optional, Sequence, Tuple, Union

from slayer.core.enums import DataType
from slayer.core.errors import DistinctDimensionValuesError, SlayerError
from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.window_duration import parse_window_duration
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


def check_windowed_key_supported(*, key: AggregateKey, window_val) -> None:
    """Per-key windowed guards (DEV-1871 G11, was ``_reject_unsupported_windowed_key``): sum/avg only, compact-duration-string window."""
    if key.agg not in ("sum", "avg"):
        raise ValueError(
            f"Aggregation parameter 'window' is only supported for sum and avg, "
            f"not '{key.agg}'."
        )
    if not isinstance(window_val, str):
        raise ValueError(
            f"Window duration must be a compact duration string like '90d', got "
            f"{window_val!r}. Use syntax like '1y2m3w5d6h7min8s'."
        )
    parse_window_duration(window_val)  # raises on empty / malformed


def _partitioned_agg_keys(
    vk: ValueKey, *, exclude: AbstractSet[AggregateKey] = frozenset(),
) -> list:
    return [
        k for k in walk_value_keys(vk)
        if isinstance(k, AggregateKey)
        and k.partition_keys is not None
        and k not in exclude
    ]


def check_partitioned_measures(
    *, measure_vks: list, filter_vks: list, order_vks: list,
    exclude: AbstractSet[AggregateKey] = frozenset(),
) -> None:
    """Reject still-deferred cross-model partition_by shapes (DEV-1871 G11, was ``_guard_partitioned_measures``): first/last, nested-in-transform; computed-dimension aggregates excluded."""
    def _part(vk: ValueKey) -> list:
        return _partitioned_agg_keys(vk, exclude=exclude)

    def _cross_model(k: AggregateKey) -> bool:
        return bool(getattr(k.source, "path", ()))

    all_vks = [*measure_vks, *filter_vks, *order_vks]
    part_keys = [k for vk in all_vks for k in _part(vk)]
    if not part_keys:
        return
    if any(k.agg in ("first", "last") and _cross_model(k) for k in part_keys):
        raise NotImplementedError(
            "partition_by on a cross-model first/last aggregation is not yet "
            "supported (DEV-1868); the aggregate must be local to the query's "
            "source."
        )
    # A cross-model partitioned aggregate nested in a transform is never desugared, so fail closed.
    if any(
        isinstance(tk, TransformKey) and any(_cross_model(k) for k in _part(tk.input))
        for vk in all_vks for tk in walk_value_keys(vk)
    ):
        raise NotImplementedError(
            "A cross-model partition_by aggregate nested inside a transform is "
            "not yet supported (DEV-1868); the partitioned aggregate must be "
            "local to the query's source."
        )


def check_partition_key_resolves(
    *, label: str, pk: ValueKey, is_query_dim: bool, ambiguous: bool,
    maps_to_bucket: bool, lenient: bool, available_dims: Sequence[str],
) -> None:
    """Every rank-family partition_by column resolves to a query dim/td (DEV-1871 G11, was inline in ``_validate_partition_keys``); a lenient key declares a finer producer grain (DEV-1825)."""
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


def check_partition_key_attributable(
    *, label: str, pk: ValueKey, attributable: bool, reason: Optional[str],
) -> None:
    """A partition key reached over a join must be attributable from the aggregate's root (DEV-1871 G11, was ``_assert_partition_key_attributable``); the compiler resolves ``attributable``/``reason``."""
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
) -> None:
    """Per-role crossing-input safety for a HOST-rooted producer answer (DEV-1871 G11, was ``_assert_local_producer_inputs_safe``); crossings are the compiler-resolved unproven hops."""
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


def check_cross_model_source_resolves(*, target_path, host_name: str) -> NoReturn:
    """An unresolvable cross-model source path (DEV-1871 G12, was inline in ``_synthesize_cross_model_producer``); bind resolves the path first, so the compiler calls this only on that invariant's breach."""
    raise ValueError(  # pragma: no cover — bind resolved the path already
        f"Cross-model aggregate source path {target_path!r} does not resolve "
        f"to a model from {host_name}."
    )


def check_cross_model_partition_keys_attributable(
    *, alias: Optional[str], root_name: str, explicit: bool,
    unattributable: Sequence[Tuple[str, str]],
) -> None:
    """An unattributable EXPLICIT partition key on a cross-model aggregate is a hard error (DEV-1871 G12); an implicit grain broadcasts instead. ``unattributable`` = compiler-resolved (name, reason) pairs."""
    if not explicit or not unattributable:
        return
    name, reason = unattributable[0]
    raise ValueError(
        f"Cross-model aggregate {alias!r} declares partition_by="
        f"{name}, which {reason}; every explicit "
        f"partition key must be attributable from {root_name} — declare "
        f"join cardinality or a covering unique key on the target."
    )


def check_windowed_cross_model_time_axis(
    *, alias: Optional[str], root_name: str, active_td_name: Optional[str],
    attributable: bool,
) -> None:
    """A windowed cross-model aggregate needs the query's active time dimension, attributable from its root (DEV-1871 G12)."""
    if active_td_name is None:
        raise ValueError(
            f"Windowed cross-model aggregate {alias!r} has no active time "
            f"dimension; add a single time_dimensions entry."
        )
    if not attributable:
        raise ValueError(
            f"Windowed cross-model aggregate {alias!r} needs the query's "
            f"active time dimension ('{active_td_name}') "
            f"attributable from {root_name}, but it crosses a fanning join; "
            f"declare join cardinality or a covering unique key on the target."
        )


def check_cross_model_inputs_safe(
    *, alias: Optional[str], root_name: str,
    unsafe_input_hops: Sequence[str], unattributable_arg_leaves: Sequence[str],
) -> None:
    """Every input of a cross-model aggregate must be attributable from its root (DEV-1871 G12, was the raises of ``_assert_cross_model_inputs_safe``); hops/leaves are the compiler-resolved violations."""
    remedy = "declare join cardinality or a covering unique key on the target"
    if unsafe_input_hops:
        raise ValueError(
            f"Cross-model aggregate {alias!r} "
            f"reads an input across an unproven join hop to {unsafe_input_hops[0]} from "
            f"{root_name}; {remedy}."
        )
    if unattributable_arg_leaves:
        raise ValueError(
            f"Cross-model aggregate {alias!r} "
            f"ranks/reads by {unattributable_arg_leaves[0]}, which is not attributable from "
            f"{root_name} (crosses a fanning join); {remedy}."
        )


def check_association_windowed_ranked(*, alias: str, windowed_or_ranked: bool) -> None:
    """window=/first/last cannot associate — the pick per entity is undefined (DEV-1871 G13)."""
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
    """The association root must declare a unique key to dedup its entities (DEV-1871 G13)."""
    if not has_unique_key:
        raise SlayerError(
            f"Aggregate {alias!r} needs distinct-entity association, but its root "
            f"model {root_name!r} declares no primary or unique key to deduplicate "
            f"entities by; declare a primary or unique key on {root_name!r}."
        )


def check_association_column_param(*, alias: str, column_param) -> None:
    """The level-2 aggregate runs over the deduped ``_base`` and cannot carry a column parameter (DEV-1871 G13; DEV-1892 tracks lifting them)."""
    if column_param is not None:
        raise SlayerError(
            f"Aggregate {alias!r} needs distinct-entity association over an "
            f"unattributable dimension, which is unsupported with a "
            f"column-reference parameter (e.g. weighted_avg(weight=…)); the "
            f"per-entity pick carries only the aggregate's own value. Attribute "
            f"the dimension or drop the column parameter."
        )


def check_reaggregation_no_window(*, alias: str, window_val) -> None:
    """window= on the outer aggregation has no defined cell-time semantics (DEV-1871 G14)."""
    if window_val is not None:
        raise SlayerError(
            f"Re-aggregation {alias!r} cannot carry window= on its outer "
            f"aggregation; apply the window inside the operand or consume the "
            f"re-aggregated value through a transform."
        )


def check_reaggregation_no_column_param(*, alias: str, column_param) -> None:
    """The outer aggregate consumes only the operand's per-cell values — no column parameter (DEV-1871 G14; DEV-1892 tracks lifting them)."""
    if column_param is not None:
        raise SlayerError(
            f"Re-aggregation {alias!r} carries a column-reference parameter on "
            f"its outer aggregation (e.g. weighted_avg(weight=…)), which is "
            f"unsupported: the outer aggregate consumes only the operand's "
            f"per-cell values. Drop the column parameter or use a numeric "
            f"literal."
        )


def check_reaggregation_partition_key_is_query_dim(
    *, alias: str, offending: Optional[str],
) -> None:
    """Every explicit outer partition key must be a query dimension (DEV-1871 G14); ``offending`` = the key's display name when it is not."""
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
    """Unattributable outer dims are a hard error under to_many_handling='error' (DEV-1871 G14); associate/broadcast resolution stays compiler-side."""
    if mode != "error" or not unattributable_names:
        return
    names = ", ".join(unattributable_names)
    raise ValueError(
        f"Re-aggregation {alias!r} cannot attribute dimension(s) {names} "
        f"to the operand dataset under to_many_handling='error'; add them "
        f"to the inner partition_by= so the operand is grained by them, "
        f"or choose 'broadcast'/'associate'."
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
