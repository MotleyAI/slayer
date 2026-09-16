"""The pre-bound planner seam (DEV-1742 §5.4, P-E).

``plan_query`` used to be the only door into binding: hand it a
``SlayerQuery`` and it parsed every measure / filter / order string, bound
each against a scope, and planned the result in one pass. Re-rooting needed a
nested plan built from keys it already held, so it SERIALIZED them back to
formula text and let ``plan_query`` re-derive the very identities it had just
thrown away.

``PreboundQuery`` is that bind product made explicit. ``bind_query_inputs``
produces it; a compiler consumes it and never parses. A caller holding typed
keys re-roots them structurally and hands them straight back — no text in the
loop.

``StrictQueryCarrier`` closes the second half. The compiler reads a handful
of query-level scalars that are not bind products (``source_model``,
``name``); a pre-bound caller that forgot one would silently inherit a Pydantic
default and plan the wrong thing. The carrier approves exactly those two and
raises on everything else, so a new post-bind ``query.*`` read fails loudly
instead of quietly.
"""

from __future__ import annotations

from typing import List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.join_walker import resolve_hop
from slayer.core.keys import TimeTruncKey, TransformKey
from slayer.core.models import SlayerModel
from slayer.ir.bound import (
    BoundFilter,
    DeclaredMeasure,
    OrderSpec,
    dimension_partitioned_aggregates,
    dimension_regroup_roots,
)
from slayer.ir.planned import SemiJoinFilter

__all__ = [
    "PreboundQuery",
    "StrictQueryCarrier",
    "partition_declared_measures",
    "position_typing_context",
    "walk_key_path",
]


def partition_declared_measures(
    *,
    declared_measures: List[DeclaredMeasure],
    n_dims: int,
    n_time_dimensions: int,
) -> Tuple[List[DeclaredMeasure], List[DeclaredMeasure], List[DeclaredMeasure]]:
    """Split ``declared_measures`` into its (dims, time_dims, aggregates) prefix
    partition — the slice arithmetic the planners used to inline. ``n_dims`` /
    ``n_time_dimensions`` are the grain prefix lengths (see ``PreboundQuery``)."""
    grain = n_dims + n_time_dimensions
    return (
        declared_measures[:n_dims],
        declared_measures[n_dims:grain],
        declared_measures[grain:],
    )


class PreboundQuery(BaseModel):
    """The typed product of the bind block.

    Everything downstream of binding reads from here, so a caller that already
    holds bound keys can plan without a parser. The ``n_*`` counts are the
    dimension / time-dimension prefix lengths of ``declared_measures``, which
    the projection and partition-key passes slice by.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    declared_measures: List[DeclaredMeasure] = Field(default_factory=list)
    bound_filters: List[BoundFilter] = Field(default_factory=list)
    # Parallel to ``bound_filters``: the originating user-filter text, or
    # ``None`` for a synthesized date-range bound. Carried so the cross-model
    # routing can report a filter the way the caller wrote it.
    bound_filter_texts: List[Optional[str]] = Field(default_factory=list)
    # Every count here is a LIST SLICE bound. A negative one is not a smaller
    # slice, it is a slice from the other end — ``bound_filters[:-1]`` silently
    # drops the LAST filter and keeps the rest, which is a wrong answer rather
    # than an error. Constrained at the field so no construction site can pass
    # one (Codex).
    n_date_range: int = Field(default=0, ge=0)
    order_specs: List[OrderSpec] = Field(default_factory=list)
    main_time_key: Optional[TimeTruncKey] = None
    n_dims: int = Field(default=0, ge=0)
    n_time_dimensions: int = Field(default=0, ge=0)
    limit: Optional[int] = None
    offset: Optional[int] = None
    distinct_dimension_values: bool = True
    # How aggregates resolve query dimensions unattributable from their root;
    # threaded onto nested producer prebounds so every plan resolves alike.
    to_many_handling: Literal["broadcast", "associate", "error"] = "broadcast"
    # Correlated EXISTS semi-joins the compiler copies onto this plan (the host
    # population's fanning-filter groups, or a target-rooted producer's own),
    # each rooted at ``source_relation`` (compile asserts the match).
    semi_join_filters: List[SemiJoinFilter] = Field(default_factory=list)

    @model_validator(mode="after")
    def _filter_texts_are_parallel(self) -> "PreboundQuery":
        """``bound_filter_texts`` is positionally parallel to
        ``bound_filters``, and nothing downstream would notice if it were not:
        the routing pass reads them with ``zip``, which silently TRUNCATES to
        the shorter list. A short texts list would therefore drop host-filter
        routings entirely rather than raise — the exact silent-narrowing class
        this seam exists to make impossible, so the invariant is enforced here
        rather than trusted at each construction site.
        """
        if len(self.bound_filter_texts) != len(self.bound_filters):
            raise ValueError(
                f"PreboundQuery.bound_filter_texts must be parallel to "
                f"bound_filters: got {len(self.bound_filter_texts)} texts for "
                f"{len(self.bound_filters)} filters.",
            )
        if self.n_date_range > len(self.bound_filters):
            raise ValueError(
                f"PreboundQuery.n_date_range={self.n_date_range} exceeds the "
                f"{len(self.bound_filters)} bound filters it slices.",
            )
        # ``n_dims`` and ``n_time_dimensions`` are the DIMENSION prefix lengths
        # of ``declared_measures``; the rest of the list is measures. Python
        # slicing past the end returns a SHORTER list rather than raising, so
        # an over-count silently plans fewer dimensions than the caller
        # declared — and the measures it does reach are misclassified as
        # dimensions on the way (CodeRabbit).
        grain = self.n_dims + self.n_time_dimensions
        if grain > len(self.declared_measures):
            raise ValueError(
                f"PreboundQuery declares {self.n_dims} dimensions + "
                f"{self.n_time_dimensions} time dimensions = {grain} grain "
                f"members, but carries only {len(self.declared_measures)} "
                f"declared measures for them to be a prefix of.",
            )
        return self

    @property
    def grain_declared_measures(self) -> List[DeclaredMeasure]:
        """The dimension + time-dimension grain prefix of ``declared_measures``."""
        dims, time_dims, _ = partition_declared_measures(
            declared_measures=self.declared_measures,
            n_dims=self.n_dims,
            n_time_dimensions=self.n_time_dimensions,
        )
        return dims + time_dims


class StrictQueryCarrier(BaseModel):
    """The post-bind ``query.*`` surface the §5.4 seam approves.

    Anything not declared here raises rather than returning a default, so a
    new post-bind read in the compiler cannot silently plan a re-rooted
    sub-query against the wrong value.
    """

    model_config = ConfigDict(extra="forbid")

    source_model: Optional[str] = None
    name: Optional[str] = None
    prebound: Optional[PreboundQuery] = None

    def __getattr__(self, item: str):
        if item.startswith("_"):
            return super().__getattr__(item)
        raise AttributeError(
            f"{type(self).__name__} does not carry {item!r}. The pre-bound "
            f"seam approves only "
            f"{sorted(type(self).model_fields)}; add the field here (and "
            f"populate it at every construction site) rather than letting the "
            f"planner read a default."
        )


def position_typing_context(
    prebound: PreboundQuery,
) -> Tuple[frozenset, frozenset]:
    """(dim_keys, row_agg_set) for position typing: the attached set is the computed
    dimensions' partitioned aggregates plus their transform roots."""
    dim_keys = frozenset(
        dm.bound.value_key
        for dm in prebound.declared_measures[
            : prebound.n_dims + prebound.n_time_dimensions
        ]
    )
    row_agg_set = frozenset(
        dimension_partitioned_aggregates(prebound.declared_measures),
    ) | frozenset(
        k for k in dimension_regroup_roots(prebound.declared_measures)
        if isinstance(k, TransformKey)
    )
    return dim_keys, row_agg_set


def walk_key_path(
    *, model: SlayerModel, path: Tuple[str, ...], bundle,
) -> Optional[SlayerModel]:
    """Walk ``path`` as join hops from ``model``; ``None`` on any miss.

    The structural counterpart to binding a dotted reference: it answers
    "is this join path traversable?" without a parser and without raising,
    which is what re-rooting needs to decide reachability. Traversal is
    bidirectional (DEV-1853); an ambiguous hop reads as non-traversable here.
    """
    models_by_name = bundle.models_by_name
    models_by_name.setdefault(model.name, model)
    current = model
    visited = {current.name}
    for hop in path:
        try:
            edge = resolve_hop(
                current=current, token=hop, models_by_name=models_by_name,
            )
        except AmbiguousJoinPathError:
            return None
        if edge is None:
            return None
        nxt = models_by_name.get(edge.target_model)
        if nxt is None or nxt.name in visited:
            return None
        visited.add(nxt.name)
        current = nxt
    return current
