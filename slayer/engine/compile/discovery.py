"""The one regroup discovery walk: every producer root of a prebound query with
its phase (from the consumer position) and routing (from the key itself)."""

from __future__ import annotations

from typing import Dict, Iterator, List, Literal, NamedTuple, Optional, Sequence, Tuple, Union

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import RANKED_AGGREGATIONS, DataType
from slayer.core.keys import (
    AggregateKey,
    TransformKey,
    ValueKey,
    attached_inputs,
    is_cross_model_agg,
    is_reaggregation_key,
    is_row_attach_root,
    source_anchor_path,
    window_kwarg_of,
)
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.compile.shift import _series_mode
from slayer.engine.join_safety import crossing_local_root_predicate, grain_member_attributable
from slayer.ir.elaborated import ConjunctTyping
from slayer.ir.planned import MaskTyping
from slayer.ir.prebound import PreboundQuery, position_typing_context
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


class _At(NamedTuple):
    key: ValueKey
    own_pk: bool  # inside an ancestor's partition keys
    attach_pk: bool  # inside a row-attach root's partition keys
    dim_key: bool  # inside a subtree equal to a query dimension


def _consumer_walk(
    key: ValueKey, *, dim_keys: frozenset,
    own_pk: bool = False, attach_pk: bool = False, dim_key: bool = False,
) -> Iterator[_At]:
    """Pre-order; opaque below a re-aggregation root and below a row-attach root's
    inputs (its partition keys stay visible)."""
    dim_key = dim_key or key in dim_keys
    yield _At(key, own_pk, attach_pk, dim_key)
    if is_reaggregation_key(key):
        return
    if is_row_attach_root(key):
        for pk in key.partition_keys or ():
            yield from _consumer_walk(pk, dim_keys=dim_keys, own_pk=True,
                                      attach_pk=True, dim_key=dim_key)
        return
    pks = frozenset(getattr(key, "partition_keys", None) or ())
    for c in key.children():
        yield from _consumer_walk(c, dim_keys=dim_keys, own_pk=own_pk or c in pks,
                                  attach_pk=attach_pk, dim_key=dim_key)


def _opaque_keys(key: ValueKey) -> Iterator[ValueKey]:
    """``walk_value_keys`` not descending a re-aggregation root."""
    yield key
    if is_reaggregation_key(key):
        return
    for c in key.children():
        yield from _opaque_keys(c)


def _is_grained(k: ValueKey) -> bool:
    return isinstance(k, AggregateKey) and k.partition_keys is not None \
        and not is_reaggregation_key(k)


def _is_bare_windowed_or_ranked(k: ValueKey) -> bool:
    return (
        isinstance(k, AggregateKey) and k.partition_keys is None
        and not source_anchor_path(k.source)
        and (window_kwarg_of(k) is not None or k.agg in RANKED_AGGREGATIONS)
    )


def _combined_kind(k: ValueKey) -> Optional[str]:
    """``local`` / ``cross_partitioned`` / ``cross_bare`` for a combined consumer."""
    if not isinstance(k, AggregateKey) or is_reaggregation_key(k):
        return None
    partitioned = k.partition_keys is not None
    if not source_anchor_path(k.source):
        return "local" if partitioned else None
    if k.locus == "host":
        return None
    return "cross_partitioned" if partitioned else "cross_bare"


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
        self.dim_keys = position_typing_context(prebound)[0]
        self.row_agg_set: set = set()
        self.row_attach_names: Dict[ValueKey, List[str]] = {}
        self.crossing = crossing_local_root_predicate(scope=scope, bundle=bundle)
        self.host = scope.source_model if isinstance(scope, ModelScope) else None
        self.bundle = bundle
        n_grain = prebound.n_dims + prebound.n_time_dimensions
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

    def dimension(self, vk: ValueKey, *, name: Optional[str], declared: Optional[DataType]) -> None:
        nodes = [at.key for at in _consumer_walk(vk, dim_keys=frozenset())]
        transform_roots = [
            k for k in nodes
            if isinstance(k, TransformKey) and any(_is_grained(g) for g in _opaque_keys(k.input))
        ]
        covered = {g for t in transform_roots for g in _opaque_keys(t.input)}
        for k in nodes:
            if is_reaggregation_key(k):
                self.emit(k, "row", "reaggregation",
                          names=(name,) if name and k == vk else (),
                          declared_type=declared if k == vk else None)
                continue
            if _is_grained(k):
                self.row_agg_set.add(k)
            if k in transform_roots or (_is_grained(k) and k not in covered):
                self.emit(k, "row", "target_rooted" if is_cross_model_agg(k) else "local_producer")
            self.row_attach(k, attach_pk=False)

    def row_attach(self, k: ValueKey, *, attach_pk: bool, names: Sequence[str] = ()) -> None:
        if not attach_pk and is_row_attach_root(k):
            bucket = self.row_attach_names.setdefault(k, [])
            bucket.extend(n for n in names if n not in bucket)

    def consumer(  # NOSONAR(S3776) — the per-position classification table of the one walk; each arm is one routing rule.
        self, vk: ValueKey, *, position: str, name: Optional[str] = None,
        declared: Optional[DataType] = None, containing: Sequence[str] = (),
    ) -> None:
        """Classify a measure / order / filter root. ``position``: ``measure``,
        ``order``, ``field_filter`` or ``measure_filter``."""
        raw_top = position == "order" and _combined_kind(vk) in ("local", "cross_partitioned")
        for at in _consumer_walk(vk, dim_keys=self.dim_keys):
            k, top = at.key, at.key is vk
            names = (name,) if name and top else ()
            if is_reaggregation_key(k):
                self.emit(k, "combined", "reaggregation", names=names,
                          declared_type=declared if top else None)
                continue
            if isinstance(k, TransformKey) and k.op == "time_shift":
                self.emit(k, "combined", "shifted", series=_series_mode(k.input, to_original={}))
            kind = _combined_kind(k)
            if kind is not None and self._combined_ok(at, position=position, raw_top=raw_top, top=top, kind=kind):
                routing: Routing = "local_producer" if kind == "local" else "target_rooted"
                self.emit(k, "combined", routing, names=names,
                          declared_type=declared if top and kind != "local" else None)
            if _is_bare_windowed_or_ranked(k) or self.crossing(k):
                self.emit(k, "combined", "local_producer", names=names)
            elif self.local_broadcasts(k):
                self.emit(k, "combined", "target_rooted", names=names)
            self.row_attach(k, attach_pk=at.attach_pk, names=containing)

    def _combined_ok(self, at: _At, *, position: str, raw_top: bool, top: bool, kind: str) -> bool:
        """Consumer-context exclusions: a measure skips partition-key subtrees; a
        measure-typed filter skips a dimension's grouped value; an order target that
        is itself a partitioned aggregate is its only consumer; order-by-name and
        field-typed filter references to a dimension's own aggregate are row-scope."""
        if position == "measure":
            return not (at.own_pk or at.attach_pk)
        if position == "measure_filter":
            return not at.dim_key
        if raw_top:
            return top
        return not (kind != "cross_bare" and at.key in self.row_agg_set)

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
    for dm in prebound.declared_measures:
        vk = dm.bound.value_key
        declared = dm.type if dm.type_is_explicit else None
        if dm.is_dimension:
            w.dimension(vk, name=dm.public_name, declared=declared)
        else:
            containing = (dm.public_name,) if dm.public_name else ()
            w.consumer(vk, position="measure", name=dm.public_name, declared=declared,
                       containing=containing)
    for sp in prebound.order_specs:
        w.consumer(sp.bound.value_key, position="order")
    measure_typed = {i for i, ct in enumerate(filter_typings) if ct.typing == MaskTyping.MEASURE}
    for i, bf in enumerate(prebound.bound_filters):
        w.consumer(bf.value_key, position="measure_filter" if i in measure_typed else "field_filter")
    w.inline_row_attach_roots()
    return w.out

