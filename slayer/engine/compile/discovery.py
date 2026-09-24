"""The one regroup discovery walk: every producer root of a prebound query with
its phase (from the consumer position) and routing (from the key itself)."""

from __future__ import annotations

from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import RANKED_AGGREGATIONS, DataType
from slayer.core.keys import (
    AggregateKey,
    ConsumerNode,
    TransformKey,
    ValueKey,
    attached_inputs,
    is_cross_model_agg,
    is_reaggregation_key,
    is_row_attach_root,
    source_anchor_path,
    walk_consumer_positions,
    window_kwarg_of,
)
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.compile.shift import _series_mode
from slayer.engine.elaborate_env import opaque_keys as _opaque
from slayer.engine.elaborate_env import (
    ConsumerPosition,
    combined_kind,
    consumer_roots,
    dimension_transform_roots,
    is_grained_aggregate,
    position_classes,
)
from slayer.engine.join_safety import crossing_local_root_predicate, grain_member_attributable
from slayer.ir.elaborated import ConjunctTyping
from slayer.ir.planned import MaskTyping
from slayer.ir.prebound import PreboundQuery
from slayer.ir.source_bundle import ResolvedSourceBundle

RootPhase = Literal["row", "combined"]
Routing = Literal[
    "inline", "local_producer", "target_rooted", "reaggregation",
    "reaggregation_constituent", "shifted",
]


class RootDisposition(BaseModel):
    """One consumer occurrence of a producer root."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    root: ValueKey
    phase: RootPhase
    routing: Routing
    #: Public measures this root answers for (a row-attach root's inputs: the
    #: measures containing that root).
    consumer_public_names: Tuple[str, ...] = ()
    declared_type: Optional[DataType] = None
    #: ``time_shift`` candidates only: shifts its materialised series.
    series: Optional[bool] = None


def _is_bare_windowed_or_ranked(k: ValueKey) -> bool:
    return (
        isinstance(k, AggregateKey) and k.partition_keys is None
        and not source_anchor_path(k.source)
        and (window_kwarg_of(k) is not None or k.agg in RANKED_AGGREGATIONS)
    )


def _input_routing(a: ValueKey) -> Routing:
    if is_reaggregation_key(a):
        return "reaggregation_constituent"
    return "target_rooted" if is_cross_model_agg(a) else "local_producer"


def row_attach_inputs(root: ValueKey, *, names: Tuple[str, ...]) -> List[RootDisposition]:
    """The row-phase dispositions of an inline row-attach root's attached inputs."""
    return [
        RootDisposition(root=a, phase="row", routing=_input_routing(a),
                        consumer_public_names=names)
        for a in attached_inputs(root)
    ]


class _Walker:
    """Classifies each walked node by consumer position; ``out`` in first-seen order."""

    def __init__(
        self, *, prebound: PreboundQuery, scope: Union[ModelScope, StageSchema],
        bundle: ResolvedSourceBundle,
    ) -> None:
        self.out: List[RootDisposition] = []
        n_grain = prebound.n_dims + prebound.n_time_dimensions
        self.classes = position_classes(prebound.declared_measures, n_grain=n_grain)
        self.row_attach_names: Dict[ValueKey, List[str]] = {}
        self.crossing = crossing_local_root_predicate(scope=scope, bundle=bundle)
        self.host = scope.source_model if isinstance(scope, ModelScope) else None
        self.bundle = bundle
        self.grain = [dm.bound.value_key for dm in prebound.declared_measures[:n_grain]]

    def emit(self, root: ValueKey, phase: RootPhase, routing: Routing, *,
             names: Tuple[str, ...] = (), declared_type: Optional[DataType] = None,
             series: Optional[bool] = None) -> None:
        d = RootDisposition(root=root, phase=phase, routing=routing,
                            consumer_public_names=names, declared_type=declared_type,
                            series=series)
        if d not in self.out:
            self.out.append(d)

    def local_broadcasts(self, k: ValueKey) -> bool:
        """A plain local aggregate grouped by a dimension unattributable from the host."""
        host = self.host
        if (
            host is None or not isinstance(k, AggregateKey) or is_reaggregation_key(k)
            or is_cross_model_agg(k) or k.locus == "host" or k.partition_keys is not None
            or self.crossing(k) or k.agg in RANKED_AGGREGATIONS
            or window_kwarg_of(k) is not None
        ):
            return False
        return any(
            not grain_member_attributable(
                key=g, target_path=(), root_model=host,
                models_by_name=self.bundle.models_by_name, bundle=self.bundle,
                host_model=host, host_name=host.name,
            )
            for g in self.grain
        )

    def emit_named(self, k: ValueKey, phase: RootPhase, routing: Routing, *, top: bool,
                   name: Optional[str], declared: Optional[DataType]) -> None:
        """Emit ``k``; only the consumer root itself carries the public name and type."""
        self.emit(k, phase, routing, names=(name,) if name and top else (),
                  declared_type=declared if top else None)

    def dimension(self, vk: ValueKey, *, name: Optional[str], declared: Optional[DataType]) -> None:
        nodes = [n.key for n in walk_consumer_positions(vk)]
        transform_roots = dimension_transform_roots(nodes)
        covered = {g for t in transform_roots for g in _opaque(t.input)}
        for k in nodes:
            if is_reaggregation_key(k):
                self.emit_named(k, "row", "reaggregation", top=k == vk, name=name,
                                declared=declared)
                continue
            if k in transform_roots or (is_grained_aggregate(k) and k not in covered):
                self.emit(k, "row", _input_routing(k))
            self.row_attach(k, attach_pk=False)

    def row_attach(self, k: ValueKey, *, attach_pk: bool, names: Sequence[str] = ()) -> None:
        if not attach_pk and is_row_attach_root(k):
            bucket = self.row_attach_names.setdefault(k, [])
            bucket.extend(n for n in names if n not in bucket)

    def consumer(
        self, vk: ValueKey, *, position: ConsumerPosition, name: Optional[str] = None,
        declared: Optional[DataType] = None, containing: Sequence[str] = (),
    ) -> None:
        """Route every node of a measure / order / filter root."""
        for n in walk_consumer_positions(vk, dim_keys=self.classes.dim_keys):
            top = n.key is vk
            if is_reaggregation_key(n.key):
                self.emit_named(n.key, "combined", "reaggregation", top=top, name=name,
                                declared=declared)
                continue
            self.combined_node(n, root=vk, position=position,
                               names=(name,) if name and top else (),
                               declared=declared if top else None)
            self.row_attach(n.key, attach_pk=n.attach_pk, names=containing)

    def combined_node(
        self, n: ConsumerNode, *, root: ValueKey, position: ConsumerPosition,
        names: Tuple[str, ...], declared: Optional[DataType],
    ) -> None:
        k = n.key
        if isinstance(k, TransformKey) and k.op == "time_shift":
            self.emit(k, "combined", "shifted", series=_series_mode(k.input, to_original={}))
        kind = combined_kind(k)
        if kind is not None and self.classes.combined_admits(n, position=position, root=root):
            if kind == "local":
                self.emit(k, "combined", "local_producer", names=names)
            else:
                self.emit(k, "combined", "target_rooted", names=names, declared_type=declared)
        if _is_bare_windowed_or_ranked(k) or self.crossing(k):
            self.emit(k, "combined", "local_producer", names=names)
        elif self.local_broadcasts(k):
            self.emit(k, "combined", "target_rooted", names=names)

    def inline_row_attach_roots(self) -> None:
        """Emit each row-attach root no producer disposition routes, with its inputs."""
        routed = {d.root for d in self.out if d.routing != "inline"}
        for root, names in self.row_attach_names.items():
            if root in routed:
                continue
            ns = tuple(names)
            self.emit(root, "combined", "inline", names=ns)
            for d in row_attach_inputs(root, names=ns):
                self.emit(d.root, d.phase, d.routing, names=d.consumer_public_names)


def discover_roots(
    prebound: PreboundQuery, *, filter_typings: Sequence[ConjunctTyping],
    scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> List[RootDisposition]:
    """Every producer root of ``prebound``, one disposition per consumer occurrence."""
    w = _Walker(prebound=prebound, scope=scope, bundle=bundle)
    measure_typed = frozenset(
        i for i, ct in enumerate(filter_typings) if ct.typing == MaskTyping.MEASURE
    )
    for r in consumer_roots(
        declared_measures=prebound.declared_measures, order_specs=prebound.order_specs,
        bound_filters=prebound.bound_filters, measure_typed=measure_typed,
    ):
        dm = r.measure
        declared = dm.type if dm is not None and dm.type_is_explicit else None
        name = dm.public_name if dm is not None else None
        if r.position == "dimension":
            w.dimension(r.key, name=name, declared=declared)
        else:
            w.consumer(r.key, position=r.position, name=name, declared=declared,
                       containing=(name,) if r.position == "measure" and name else ())
    w.inline_row_attach_roots()
    return w.out
