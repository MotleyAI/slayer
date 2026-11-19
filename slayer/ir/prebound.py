"""The pre-bound planner seam (DEV-1742 §5.4, P-E).

``plan_query`` used to be the only door into binding: hand it a
``SlayerQuery`` and it parsed every metric / filter / order string, bound
each against a scope, and planned the result in one pass. Re-rooting needed a
nested plan built from keys it already held, so it SERIALIZED them back to
expression text and let ``plan_query`` re-derive the very identities it had just
thrown away.

``PreboundQuery`` is that bind product made explicit. ``bind_query_inputs``
produces it; a compiler consumes it and never parses. A caller holding typed
keys re-roots them structurally and hands them straight back — no text in the
loop.

``StrictQueryCarrier`` closes the second half. The compiler reads a handful
of query-level scalars that are not bind products (``population``,
``name``); a pre-bound caller that forgot one would silently inherit a Pydantic
default and plan the wrong thing. The carrier approves exactly those two and
raises on everything else, so a new post-bind ``query.*`` read fails loudly
instead of quietly.
"""

from __future__ import annotations

from typing import List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from slayer.core.relationship_walker import resolved_terminal
from slayer.core.keys import TimeTruncKey
from slayer.core.models import SlayerDataset
from slayer.core.warnings import DateOnlyMidnightWarning
from slayer.ir.bound import BoundFilter, DeclaredMetric, GranularityConflict, OrderSpec, WholeListRead
from slayer.ir.source_bundle import ResolvedSourceBundle

__all__ = [
    "ModePolicy",
    "PreboundQuery",
    "StrictQueryCarrier",
    "partition_declared_metrics",
    "walk_key_path",
]

ToManyHandling = Literal["broadcast", "associate", "error"]


class ModePolicy(BaseModel):
    """The resolved ``to_many_handling``: how an unattributable dimension resolves."""

    model_config = ConfigDict(frozen=True)

    mode: ToManyHandling = "associate"
    # The default: an association-ineligible aggregate broadcasts instead of raising.
    fallback: bool = True

    @classmethod
    def resolve(cls, requested: Optional[ToManyHandling]) -> "ModePolicy":
        """The one reader of the raw field: omitted associates with the broadcast fallback."""
        return cls() if requested is None else cls(mode=requested, fallback=False)

    @property
    def associates(self) -> bool:
        return self.mode == "associate"

    @property
    def refuses(self) -> bool:
        return self.mode == "error"


def partition_declared_metrics(
    *,
    declared_metrics: List[DeclaredMetric],
    n_dims: int,
    n_time_dimensions: int,
) -> Tuple[List[DeclaredMetric], List[DeclaredMetric], List[DeclaredMetric]]:
    """Split ``declared_metrics`` into its (dims, time_dims, aggregates) prefix
    partition — the slice arithmetic the planners used to inline. ``n_dims`` /
    ``n_time_dimensions`` are the grain prefix lengths (see ``PreboundQuery``)."""
    grain = n_dims + n_time_dimensions
    return (
        declared_metrics[:n_dims],
        declared_metrics[n_dims:grain],
        declared_metrics[grain:],
    )


class PreboundQuery(BaseModel):
    """The typed product of the bind block.

    Everything downstream of binding reads from here, so a caller that already
    holds bound keys can plan without a parser. The ``n_*`` counts are the
    dimension / time-dimension prefix lengths of ``declared_metrics``, which
    the projection and partition-key passes slice by.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    declared_metrics: List[DeclaredMetric] = Field(default_factory=list)
    bound_filters: List[BoundFilter] = Field(default_factory=list)
    # Parallel to ``bound_filters``: the originating user-filter text, or
    # ``None`` for a synthesized date-range bound. Carried so the cross-dataset
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
    mode_policy: ModePolicy = ModePolicy()
    granularity_conflicts: List[GranularityConflict] = Field(default_factory=list)
    whole_list_reads: List[WholeListRead] = Field(default_factory=list)
    date_only_notices: List[DateOnlyMidnightWarning] = Field(default_factory=list)

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
        # of ``declared_metrics``; the rest of the list is metrics. Python
        # slicing past the end returns a SHORTER list rather than raising, so
        # an over-count silently plans fewer dimensions than the caller
        # declared — and the metrics it does reach are misclassified as
        # dimensions on the way (CodeRabbit).
        grain = self.n_dims + self.n_time_dimensions
        if grain > len(self.declared_metrics):
            raise ValueError(
                f"PreboundQuery declares {self.n_dims} dimensions + "
                f"{self.n_time_dimensions} time dimensions = {grain} grain "
                f"members, but carries only {len(self.declared_metrics)} "
                f"declared metrics for them to be a prefix of.",
            )
        return self

    @property
    def grain_declared_metrics(self) -> List[DeclaredMetric]:
        """The dimension + time-dimension grain prefix of ``declared_metrics``."""
        dims, time_dims, _ = partition_declared_metrics(
            declared_metrics=self.declared_metrics,
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

    population: Optional[str] = None
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


def walk_key_path(
    *, dataset: SlayerDataset, path: Tuple[str, ...], bundle: ResolvedSourceBundle,
) -> Optional[SlayerDataset]:
    """The terminal dataset of a bound (possibly composed) key ``path`` from ``dataset``; ``None`` on any miss."""
    return resolved_terminal(root=dataset, path=path, datasets_by_name=bundle.datasets_by_name)
