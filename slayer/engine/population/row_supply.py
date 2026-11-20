"""The row supply of a query without a population (semantics Axiom 12.3–12.4).

Every metric supplies the cells of its full-grain aggregates — those typed by the query's dimensions whose home
determines or associates each — and a metric with none supplies the anchor's cells agreeing with its coarser
aggregates' cells. Only ``metrics`` supply rows.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import UnrelatedDatasetError, UnresolvableDimensionRelationshipError
from slayer.core.field_expression import portable_text
from slayer.core.query import SlayerQuery
from slayer.core.syntax import (
    AggCall, DottedRef, Ref, StarSource, call_keywords, parse_template, respell_bare_names, walk_parsed_refs,
)
from slayer.core.models import SlayerDataset
from slayer.core.time_spine import is_spine_query
from slayer.engine.population.anchors import anchor_route, find_anchor
from slayer.engine.population.inference import (
    DimensionAnchor,
    _OK,
    _all_filter_refs,
    _is_saved_metric_ref,
    _query_dimension_refs,
    _walk_terminal_dataset,
    determination_items,
    metric_anchor_names,
    probe_item,
)
from slayer.ir.source_bundle import RowSupply, RowSupplyBranch

#: ``row_sources`` of a query with no dimensions.
UNIT = "unit"
#: Aggregations that associate only through a window or a ranking: they broadcast instead (semantics Axiom 8).
_BROADCAST_ONLY = frozenset({"first", "last"})


class CoarseCells(BaseModel):
    """A coarser aggregate's cells: its home's rows grouped by ``grain`` (dimension references; empty: the unit)."""

    model_config = ConfigDict(frozen=True)

    home: str
    grain: tuple[str, ...] = ()


class _Operand(BaseModel):
    home: str
    full: bool
    grain: tuple[str, ...] = ()


class RowSources(BaseModel):
    """The homes of a query's full-grain aggregates, and the coarser cells of metrics with none."""

    homes: tuple[str, ...] = ()
    coarse: tuple[CoarseCells, ...] = ()

    @property
    def needs_anchor(self) -> bool:
        return bool(self.coarse)


class RowPlan(BaseModel):
    """How an inferred stage is planned: from ``population``, its rows ``supply``; what it reports."""

    population: str
    supply: RowSupply
    row_sources: tuple[str, ...]
    reported: Optional[str] = None


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #
def _saved_metric(node: Any, *, owner: Optional[str], datasets_by_name: dict[str, SlayerDataset],
                  seen: frozenset[str]) -> Optional[tuple[Any, str, str]]:
    """``(parsed definition, its dataset, its key)`` when ``node`` names a saved metric not yet in ``seen``."""
    parts = [*((owner,) if owner else ()), *(node.parts if isinstance(node, DottedRef) else (node.name,))]
    dataset = _walk_terminal_dataset(parts[:-1], datasets_by_name=datasets_by_name)
    if dataset is None or dataset.get_field(parts[-1]) is not None:
        return None
    metric = dataset.get_metric(parts[-1])
    key = f"{dataset.name}.{parts[-1]}"
    text = portable_text(metric.expression) if metric is not None and key not in seen else None
    if text is None:
        return None
    try:
        return parse_template(text), dataset.name, key
    except Exception:  # noqa: BLE001 — an unparseable definition is refused where it binds
        return None


def _top_aggregates(tree: Any, *, owner: Optional[str], datasets_by_name: dict[str, SlayerDataset],
                    seen: frozenset[str] = frozenset()) -> Iterator[tuple[AggCall, Optional[str]]]:
    """Each aggregation of ``tree`` outside another, a saved metric's expanded on its dataset (``owner``)."""
    for node in walk_parsed_refs(tree):
        if isinstance(node, AggCall):
            yield node, owner
            continue
        saved = _saved_metric(node, owner=owner, datasets_by_name=datasets_by_name, seen=seen)
        if saved is not None:
            expanded, dataset_name, key = saved
            yield from _top_aggregates(expanded, owner=dataset_name, datasets_by_name=datasets_by_name,
                                       seen=seen | {key})


def _leaf_paths(agg: AggCall, *, owner: Optional[str]) -> tuple[list[tuple[str, ...]], list[AggCall]]:
    """``(row-level leaf paths without their leaf, constituent aggregations)`` of ``agg``'s source."""
    prefix = (owner,) if owner else ()
    if isinstance(agg.source, StarSource):
        return ([prefix] if prefix else []), []
    paths: list[tuple[str, ...]] = []
    inner: list[AggCall] = []
    for node in walk_parsed_refs(agg.source):
        if isinstance(node, AggCall):
            inner.append(node)
        elif isinstance(node, DottedRef):
            paths.append((*prefix, *node.parts[:-1]))
        elif prefix:
            paths.append(prefix)
    return paths, inner


def _home(agg: AggCall, *, owner: Optional[str], datasets_by_name: dict[str, SlayerDataset]) -> Optional[str]:
    """The stored dataset ``agg`` is counted over: where its leaves' common path ends (a second-order aggregation's
    constituents' home)."""
    paths, inner = _leaf_paths(agg, owner=owner)
    if not paths:
        return next((h for c in inner if (h := _home(c, owner=owner, datasets_by_name=datasets_by_name))), None)
    common = paths[0]
    for p in paths[1:]:
        i = 0
        while i < min(len(common), len(p)) and common[i] == p[i]:
            i += 1
        common = common[:i]
    if not common:
        return paths[0][0] if paths[0] and paths[0][0] in datasets_by_name else None
    terminal = _walk_terminal_dataset(list(common), datasets_by_name=datasets_by_name)
    return terminal.name if terminal is not None and terminal.name in datasets_by_name else common[0]


def _partition_ref(v: Any, *, owner: Optional[str]) -> Optional[str]:
    if isinstance(v, DottedRef):
        return ".".join(((owner,) if owner else ()) + v.parts)
    if isinstance(v, Ref):
        return f"{owner}.{v.name}" if owner else v.name
    return None


def _partition_grain(agg: AggCall, *, owner: Optional[str]) -> Optional[tuple[str, ...]]:
    values = [value for name, value in call_keywords(agg) if name == "partition_by"]
    if not values:
        return None
    flat = [v for value in values for v in (value if isinstance(value, (list, tuple)) else (value,))]
    return tuple(r for v in flat if (r := _partition_ref(v, owner=owner)) is not None)


def _reaches(*, home: str, item: str, datasets_by_name: dict[str, SlayerDataset]) -> bool:
    """Whether a join path leads from ``home`` to ``item``'s anchor (a product hop is no path)."""
    reader = datasets_by_name.get(home)
    parts = item.split(".")
    if reader is None:
        return False
    anchor = find_anchor(parts[0], quoted=False, reader=reader, datasets_by_name=datasets_by_name)
    if anchor is None:
        return False
    try:
        return not anchor_route(reader=reader, anchor=anchor, datasets_by_name=datasets_by_name,
                                rest=parts[1:]).unrelated
    except UnresolvableDimensionRelationshipError:
        return True


def _operand(agg: AggCall, *, owner: Optional[str], dims: list[str],
             datasets_by_name: dict[str, SlayerDataset], broadcast: bool = False) -> Optional[_Operand]:
    home = _home(agg, owner=owner, datasets_by_name=datasets_by_name)
    if home is None:
        return None
    partition = _partition_grain(agg, owner=owner)
    if partition is not None and set(partition) != set(dims):
        return _Operand(home=home, full=False, grain=partition)
    determined = [d for d in dims if probe_item(root=home, item=d, datasets_by_name=datasets_by_name)[0] == _OK]
    if len(determined) == len(dims):
        return _Operand(home=home, full=True)
    reached = [d for d in dims if d in determined or _reaches(home=home, item=d, datasets_by_name=datasets_by_name)]
    associates = not broadcast and agg.agg.lower() not in _BROADCAST_ONLY and not any(
        n == "window" for n, _ in call_keywords(agg))
    if len(reached) == len(dims) and associates:
        return _Operand(home=home, full=True)
    return _Operand(home=home, full=False, grain=tuple(reached if associates else determined))


def classify_row_sources(query: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset]) -> RowSources:
    """The full-grain homes of ``query``'s metrics, and the coarser cells of each metric with no full-grain one."""
    dims = determination_items(query, datasets_by_name=datasets_by_name)
    homes: list[str] = []
    coarse: list[CoarseCells] = []
    for metric in query.metrics or []:
        try:
            tree = parse_template(metric.metric)
        except Exception:  # noqa: BLE001 — an unparseable metric is refused where it binds
            continue
        operands = [op for agg, owner in _top_aggregates(tree, owner=None, datasets_by_name=datasets_by_name)
                    if (op := _operand(agg, owner=owner, dims=dims, datasets_by_name=datasets_by_name,
                                       broadcast=query.to_many_handling == "broadcast")) is not None]
        full = [op.home for op in operands if op.full]
        homes.extend(full)
        if not full:
            coarse.extend(CoarseCells(home=op.home, grain=op.grain) for op in operands)
    return RowSources(homes=tuple(sorted(set(homes))), coarse=tuple(dict.fromkeys(coarse)))


# --------------------------------------------------------------------------- #
# Composition
# --------------------------------------------------------------------------- #
def _field_filters(query: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset]) -> list[str]:
    """The query's aggregate-free filters reading only fields (no metric alias, no saved metric), a dimension alias
    spelled as its field."""
    fields = {d.alias: d.field for d in query.dimensions or [] if d.alias and d.ref is not None}
    out: list[str] = []
    for f in (respell_bare_names(c, respell=fields.get) for c in query.filter_conditions):
        refs, ok = _all_filter_refs(f, root=None)
        if ok and all("." in r and not _is_saved_metric_ref(r, datasets_by_name=datasets_by_name) for r in refs):
            out.append(f)
    return out


def _cells(query: SlayerQuery, *, population: str, filters: list[str],
           grain: Optional[tuple[str, ...]] = None) -> SlayerQuery:
    """A dimension-only query over ``population``: the cells its filtered rows reach, on the query's dimensions
    whose references lie in ``grain`` when given (each kept as the query spells it, alias included)."""
    data = query.model_dump(mode="json", exclude_none=True, exclude={
        "metrics", "order", "limit", "offset", "name", "population", "population_inferred", "version", "filter",
        *(("main_time_dimension",) if grain is not None else ()),
    })
    data["population"] = population
    if filters:
        data["filter"] = filters
    if grain is not None:
        data["dimensions"] = [
            d.model_dump(mode="json", exclude_none=True) for d in query.dimensions or []
            if set(_query_dimension_refs(query.model_copy(update={"dimensions": [d], "time_dimensions": None})))
            <= set(grain)
        ]
        data["time_dimensions"] = [
            t.model_dump(mode="json", exclude_none=True) for t in query.time_dimensions or []
            if set(_query_dimension_refs(query.model_copy(update={"dimensions": None, "time_dimensions": [t]})))
            <= set(grain)
        ]
    return SlayerQuery.model_validate(data)


def first_metric_anchor(query: SlayerQuery) -> Optional[str]:
    """The first dataset a metric (else a filter) of ``query`` names."""
    for text in [*(m.metric for m in query.metrics or []), *query.filter_conditions]:
        try:
            tree = parse_template(text)
        except Exception:  # noqa: BLE001
            continue
        stack = list(walk_parsed_refs(tree))
        while stack:
            node = stack.pop(0)
            if isinstance(node, AggCall):
                stack[:0] = [n for n in walk_parsed_refs(node.source) if n is not node]
            elif isinstance(node, DottedRef):
                return node.parts[0]
    return None


def _routes(*, reader: SlayerDataset, home: str, datasets_by_name: dict[str, SlayerDataset]) -> bool:
    """Whether ``reader`` reaches ``home`` unambiguously (or meets it as a product)."""
    target = datasets_by_name.get(home)
    if target is None or home == reader.name:
        return True
    try:
        anchor_route(reader=reader, anchor=target, datasets_by_name=datasets_by_name)
    except UnresolvableDimensionRelationshipError:
        return False
    return True


def _require_route(*, reader: SlayerDataset, home: str, datasets_by_name: dict[str, SlayerDataset]) -> None:
    """Raise the typed ambiguity error when ``reader`` has no unambiguous route to ``home``."""
    target = datasets_by_name.get(home)
    if target is not None and home != reader.name:
        anchor_route(reader=reader, anchor=target, datasets_by_name=datasets_by_name)


def anchored_rows(query: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset]) -> Optional[RowPlan]:
    """A named population that cannot route to a metric's home: the stage is planned from that home, its rows the
    population's cells (the population's reading of the home never decides a value, Axiom 6); else ``None``.
    DEV-2388 removes it: aggregations bind from their own anchor."""
    population = query.population_name
    reader = datasets_by_name.get(population or "")
    has_dims = bool(query.dimensions or query.time_dimensions)
    if reader is None or population is None or not query.metrics or not query.distinct_dimension_values \
            or not has_dims:
        return None
    free = query.model_copy(update={"population": None})
    unroutable = sorted(a for a in metric_anchor_names(free)
                        if a in datasets_by_name and not _routes(reader=reader, home=a, datasets_by_name=datasets_by_name))
    if not unroutable:
        return None
    filters = _field_filters(free, datasets_by_name=datasets_by_name)
    for f in filters:
        for r in _all_filter_refs(f, root=None)[0]:
            _require_route(reader=reader, home=r.split(".")[0], datasets_by_name=datasets_by_name)
    cells = _cells(free, population=population, filters=filters)
    return RowPlan(population=unroutable[0], supply=RowSupply(branches=(RowSupplyBranch(cells=cells),), anchor=population),
                   row_sources=(population,), reported=population)


def _connected(*, home: str, target: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> bool:
    """Whether a join path (ambiguous or not) leads from ``home`` to ``target``."""
    reader = datasets_by_name.get(home)
    if reader is None:
        return False
    if reader.name == target.name:
        return True
    try:
        return not anchor_route(reader=reader, anchor=target, datasets_by_name=datasets_by_name).unrelated
    except UnresolvableDimensionRelationshipError:
        return True


def refuse_product_split(
    query: SlayerQuery, *, anchor_datasets: tuple[str, ...], datasets_by_name: dict[str, SlayerDataset],
) -> None:
    """A product anchor beside a metric homed in its first group's component is refused, as an anchored query reading
    the other groups' dimensions is (DEV-2390 lifts it)."""
    host = datasets_by_name.get(anchor_datasets[0]) if len(anchor_datasets) > 1 else None
    if host is None or not query.metrics:
        return
    homes = sorted({c.home for c in classify_row_sources(query, datasets_by_name=datasets_by_name).coarse})
    joined = [h for h in homes if _connected(home=h, target=host, datasets_by_name=datasets_by_name)]
    if joined:
        others = ", ".join(anchor_datasets[1:])
        raise UnrelatedDatasetError(
            summary=f"The dimensions of {others} meet {host.name} as a product, and the metrics homed on "
            f"{', '.join(joined)} reach no relationship path to {others}: their values by those dimensions are "
            f"undefined.",
            suggestion=f"Drop the dimensions of {others}, or measure {others} only through its own aggregates.",
        )


def _single(datasets: tuple[str, ...]) -> Optional[str]:
    return datasets[0] if len(datasets) == 1 else None


def _unit_plan(query: SlayerQuery, *, anchor: DimensionAnchor,
               datasets_by_name: dict[str, SlayerDataset]) -> RowPlan:
    """A query with no dimensions: one row, planned from a supplying home (else the first dataset named)."""
    host = (classify_row_sources(query, datasets_by_name=datasets_by_name).homes or (None,))[0]
    population = host or first_metric_anchor(query)
    if population is None:
        anchor.required()
        raise ValueError("a query with no dimensions, metrics or filters has no rows")
    return RowPlan(population=population, supply=RowSupply(unit=True), row_sources=(UNIT,))


def _supply_branches(query: SlayerQuery, *, sources: RowSources, anchor_datasets: tuple[str, ...],
                     filters: list[str]) -> list[RowSupplyBranch]:
    """One branch per full-grain home, plus the anchor's cells restricted to each coarser term's cells."""
    branches = [RowSupplyBranch(cells=_cells(query, population=h, filters=filters)) for h in sources.homes]
    if not sources.needs_anchor:
        return branches
    anchor_cells = _cells(query, population=anchor_datasets[0], filters=filters)
    for c in sources.coarse:
        restrict = (_cells(query, population=c.home, filters=filters, grain=c.grain),) if c.grain else ()
        branch = RowSupplyBranch(cells=anchor_cells, restrict=restrict)
        if branch not in branches:
            branches.append(branch)
    return branches


def stored_rows(query: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset]) -> Optional[RowPlan]:
    """A stored inferred stage's row plan: from its pinned anchor, or the unit for an unpinned stage with no
    dimensions; ``None`` for an anchored or spine stage (whose rows are its population's)."""
    anchor = query.population_name
    if is_spine_query(query):
        return None
    if anchor is None:
        unit = not (query.dimensions or query.time_dimensions)
        return plan_rows(query, anchor=DimensionAnchor(), datasets_by_name=datasets_by_name) if unit else None
    if not query.population_inferred or anchor not in datasets_by_name:
        return None
    free = query.model_copy(update={"population": None, "population_inferred": False})
    return plan_rows(free, anchor=DimensionAnchor(datasets=(anchor,)), datasets_by_name=datasets_by_name)


def plan_rows(
    query: SlayerQuery, *, anchor: DimensionAnchor, datasets_by_name: dict[str, SlayerDataset],
) -> RowPlan:
    """The population an inferred ``query`` is planned from, its row supply, and what it reports."""
    if not query.distinct_dimension_values:
        (population, *_rest) = anchor.required()
        return RowPlan(population=population, supply=RowSupply(anchor=population), row_sources=(population,),
                       reported=population)
    if not (query.dimensions or query.time_dimensions):
        return _unit_plan(query, anchor=anchor, datasets_by_name=datasets_by_name)
    if not query.metrics:
        datasets = anchor.required()
        return RowPlan(population=datasets[0], supply=RowSupply(product=len(datasets) > 1, anchor=_single(datasets)),
                       row_sources=tuple(sorted(datasets)), reported=anchor.display)
    sources = classify_row_sources(query, datasets_by_name=datasets_by_name)
    anchor_datasets = anchor.required() if sources.needs_anchor else anchor.datasets
    refuse_product_split(query, anchor_datasets=anchor_datasets, datasets_by_name=datasets_by_name)
    filters = _field_filters(query, datasets_by_name=datasets_by_name)
    branches = _supply_branches(query, sources=sources, anchor_datasets=anchor_datasets, filters=filters)
    row_sources = tuple(sorted({*sources.homes, *(anchor_datasets if sources.needs_anchor else ())}))
    if not sources.needs_anchor and len(sources.homes) == 1:
        return RowPlan(population=sources.homes[0], supply=RowSupply(anchor=_single(anchor.datasets)), row_sources=row_sources,
                       reported=anchor.display)
    # A supplying home hosts the stage: a re-aggregation's carrier is built over the host's rows.
    population = (sources.homes or anchor_datasets or (first_metric_anchor(query),))[0]
    if population is None:
        anchor.required()
        raise ValueError("an inferred query with metrics names no dataset")
    return RowPlan(population=population, supply=RowSupply(branches=tuple(branches), product=len(anchor_datasets) > 1,
                                                           anchor=_single(anchor_datasets)),
                   row_sources=row_sources or (population,), reported=anchor.display)
