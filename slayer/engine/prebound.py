"""The pre-bound planner seam (DEV-1742 §5.4, P-E).

``plan_query`` used to be the only door into binding: hand it a
``SlayerQuery`` and it parsed every measure / filter / order string, bound
each against a scope, and planned the result in one pass. Re-rooting needed a
nested plan built from keys it already held, so it SERIALIZED them back to
formula text and let ``plan_query`` re-derive the very identities it had just
thrown away.

``PreboundQuery`` is that bind product made explicit. ``bind_query_inputs``
produces it; ``plan_query(prebound=…)`` consumes it and skips binding
entirely. A caller holding typed keys re-roots them structurally and hands
them straight back — no text in the loop.

``StrictQueryCarrier`` closes the second half. ``plan_query`` reads a handful
of query-level scalars that are not bind products (``source_model``,
``name``); a pre-bound caller that forgot one would silently inherit a Pydantic
default and plan the wrong thing. The carrier approves exactly those two and
raises on everything else, so a new post-bind ``query.*`` read fails loudly
instead of quietly.

Also home to the key → slot-metadata lifts (``type`` / ``format`` /
``description``). They live here rather than in ``stage_planner`` because both
the planner and the re-rooting strategy need them, and the strategy cannot
import the planner (the recursion is injected as ``subplan_builder`` precisely
to avoid that cycle).
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from slayer.core.enums import (
    AggregationValueClass,
    DataType,
    classify_aggregation,
)
from slayer.core.format import NumberFormat
from decimal import Decimal

from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    ConditionalKey,
    LiteralKey,
    StarKey,
    TimeTruncKey,
    ValueKey,
    join_conditional_branch_types,
)
from slayer.core.models import SlayerModel
from slayer.engine.binding import BoundFilter
from slayer.engine.planning import DeclaredMeasure, OrderSpec
from slayer.engine.response_meta import _infer_aggregated_format


__all__ = [
    "PreboundQuery",
    "StrictQueryCarrier",
    "aggregated_type",
    "partition_declared_measures",
    "dimension_key_metadata",
    "measure_key_format_description",
    "measure_key_type",
    "walk_key_path",
]


# ---------------------------------------------------------------------------
# The seam types
# ---------------------------------------------------------------------------


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
    """The typed product of ``plan_query``'s bind block.

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
    new post-bind read in ``plan_query`` cannot silently plan a re-rooted
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


# ---------------------------------------------------------------------------
# Key -> slot metadata
# ---------------------------------------------------------------------------

def aggregated_type(
    *,
    model: SlayerModel,
    measure_name: Optional[str],
    aggregation: str,
) -> Optional[DataType]:
    """Type for an aggregated measure slot, via the shared
    ``classify_aggregation`` (DEV-1788), so it cannot drift from
    ``_infer_aggregated_format``:

    * ``COUNT`` (``*:count`` / count-family) → ``INT``
    * ``FLOAT_SOURCE_UNITS`` / ``FLOAT_PLAIN`` (avg-family, stat, parametric) →
      ``DOUBLE``
    * ``PRESERVING`` (sum / min / max / first / last, and custom aggs) → inherit
      source column type (``None`` if absent).
    """
    cls = classify_aggregation(measure_name=measure_name, aggregation=aggregation)
    if cls is AggregationValueClass.COUNT:
        return DataType.INT
    if cls in (
        AggregationValueClass.FLOAT_SOURCE_UNITS,
        AggregationValueClass.FLOAT_PLAIN,
    ):
        return DataType.DOUBLE
    # PRESERVING — inherit source column type.
    if measure_name is None:
        return None
    col = model.get_column(measure_name)
    if col is not None and col.type is not None:
        return col.type
    return None


def _local_aggregate_source_name(key: ValueKey) -> Optional[str]:
    """The source column name of a LOCAL aggregate, or ``None``.

    ``None`` for anything that isn't a bare local aggregate — a non-aggregate
    key, an unsupported source shape, or a cross-model source (whose metadata
    is lifted by ``response_meta`` against the target model instead).
    """
    if not isinstance(key, AggregateKey):
        return None
    src = key.source
    if isinstance(src, StarKey):
        return "*"
    if not isinstance(src, (ColumnKey, ColumnSqlKey)):
        return None
    if getattr(src, "path", ()):
        return None
    return getattr(src, "leaf", None) or getattr(src, "column_name", None)


def _literal_data_type(value) -> Optional[DataType]:
    """The SLayer type of a scalar literal (``None`` for a NULL literal)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return DataType.BOOLEAN
    if isinstance(value, Decimal):
        return DataType.INT if value == value.to_integral_value() else DataType.DOUBLE
    if isinstance(value, str):
        return DataType.TEXT
    return None


def _branch_type(*, model: SlayerModel, key: ValueKey) -> Optional[DataType]:
    """The type of one CASE/iif branch, from the model alone (``None`` when it
    cannot be determined — a joined column, an arithmetic expression — so it is
    treated as NULL-absorbing rather than an incompatibility)."""
    if isinstance(key, LiteralKey):
        return _literal_data_type(key.value)
    if isinstance(key, ConditionalKey):
        return measure_key_type(model=model, key=key)
    if isinstance(key, ColumnKey) and not key.path:
        col = model.get_column(key.leaf)
        return col.type if col is not None else None
    return measure_key_type(model=model, key=key)


def measure_key_type(
    *, model: SlayerModel, key: ValueKey,
) -> Optional[DataType]:
    """``type`` for a measure slot, from its bound key alone."""
    if isinstance(key, ConditionalKey):
        # Postgres branch typing: the join over every THEN branch and the final
        # ELSE. Raises on an incompatible mix (DEV-1740).
        result: Optional[DataType] = None
        node: ValueKey = key
        branches = []
        while isinstance(node, ConditionalKey):
            branches.append(node.then)
            node = node.otherwise
        branches.append(node)
        for branch in branches:
            result = join_conditional_branch_types(
                result, _branch_type(model=model, key=branch),
            )
        return result
    name = _local_aggregate_source_name(key)
    if name is None:
        return None
    return aggregated_type(
        model=model, measure_name=name, aggregation=key.agg,
    )


def measure_key_format_description(
    *, model: SlayerModel, key: ValueKey,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    """``format`` / ``description`` for a measure slot, from its bound key.

    ``*:count`` has an inferred INTEGER format but no description — there is
    no source column to document it.
    """
    name = _local_aggregate_source_name(key)
    if name is None:
        return None, None
    fmt = _infer_aggregated_format(
        model=model, measure_name=name, aggregation=key.agg,
    )
    if name == "*":
        return fmt, None
    col = model.get_column(name)
    return fmt, (col.description if col is not None else None)


def walk_key_path(
    *, model: SlayerModel, path: Tuple[str, ...], bundle,
) -> Optional[SlayerModel]:
    """Walk ``path`` as join hops from ``model``; ``None`` on any miss.

    The structural counterpart to binding a dotted reference: it answers
    "is this join path traversable?" without a parser and without raising,
    which is what re-rooting needs to decide reachability.
    """
    current = model
    visited = {current.name}
    for hop in path:
        if not any(j.target_model == hop for j in current.joins):
            return None
        nxt = bundle.get_referenced_model(hop)
        if nxt is None or nxt.name in visited:
            return None
        visited.add(nxt.name)
        current = nxt
    return current


def dimension_key_metadata(
    *, model: SlayerModel, key: ValueKey, bundle,
) -> Tuple[Optional[DataType], Optional[NumberFormat], Optional[str]]:
    """``(type, format, description)`` for a dimension slot, from its key.

    A LOCAL dimension carries the source column's full display contract; a
    JOINED one carries only its type. That asymmetry is deliberate and
    pre-existing: joined refs surface format / description through
    ``response_meta``, which resolves them against the owning model.
    """
    inner = key.column if isinstance(key, TimeTruncKey) else key
    path = tuple(getattr(inner, "path", ()) or ())
    leaf = getattr(inner, "leaf", None) or getattr(inner, "column_name", None)
    if leaf is None:
        return None, None, None
    if not path:
        col = model.get_column(leaf)
        if col is None:
            return None, None, None
        return col.type, col.format, col.description
    terminal = walk_key_path(model=model, path=path, bundle=bundle)
    if terminal is None:
        return None, None, None
    col = terminal.get_column(leaf)
    return (col.type if col is not None else None), None, None
