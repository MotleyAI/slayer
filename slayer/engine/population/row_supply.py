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
    AggCall, DottedRef, Literal, NamedArg, Ref, StarSource, TransformCall, UnaryOp, Placeholder, call_keywords, parse_expr,
    parse_template, respell_bare_names,
    walk_parsed_refs,
)
from slayer.core.models import SlayerDataset
from slayer.core.time_spine import TIME_SPINE_DATASET, axis_field, is_spine_query, reads_spine
from slayer.engine.population.anchors import anchor_route, find_anchor
from slayer.engine.population.inference import (
    DimensionAnchor,
    _OK,
    _all_filter_refs,
    _is_saved_metric_ref,
    _query_dimension_refs,
    _spine_only_filter,
    _walk_terminal_dataset,
    determination_items,
    dimension_anchor,
    metric_anchor_names,
    probe_item,
    spine_factor_out,
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
    """The homes of a query's full-grain aggregates, the coarser cells of metrics with none, and whether an
    aggregate-free metric supplies every anchor cell."""

    homes: tuple[str, ...] = ()
    coarse: tuple[CoarseCells, ...] = ()
    whole_anchor: bool = False

    @property
    def needs_anchor(self) -> bool:
        return bool(self.coarse) or self.whole_anchor


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
        return parse_expr(text), dataset.name, key
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
    whole_anchor = False
    for metric in query.metrics or []:
        try:
            tree = parse_template(metric.metric)
        except Exception:  # noqa: BLE001 — an unparseable metric is refused where it binds
            continue
        aggregates = list(_top_aggregates(tree, owner=None, datasets_by_name=datasets_by_name))
        operands = [op for agg, owner in aggregates
                    if (op := _operand(agg, owner=owner, dims=dims, datasets_by_name=datasets_by_name,
                                       broadcast=query.to_many_handling == "broadcast")) is not None]
        full = [op.home for op in operands if op.full]
        homes.extend(full)
        if not full:
            coarse.extend(CoarseCells(home=op.home, grain=op.grain) for op in operands)
        # An aggregate-free metric (a transform over dimension values) supplies the anchor's cells (Axiom 11.6).
        whole_anchor = whole_anchor or not aggregates
    return RowSources(homes=tuple(sorted(set(homes))), coarse=tuple(dict.fromkeys(coarse)), whole_anchor=whole_anchor)


# --------------------------------------------------------------------------- #
# Composition
# --------------------------------------------------------------------------- #
def _row_level(text: str) -> bool:
    """Whether ``text`` parses and reads no aggregation."""
    try:
        return not any(isinstance(n, AggCall) for n in walk_parsed_refs(parse_template(text)))
    except Exception:  # noqa: BLE001 — an unparseable entry is refused where it binds
        return False


def _field_filters(query: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset]) -> list[str]:
    """The query's aggregate-free filters reading only fields (no metric alias, no saved metric), a dimension alias
    spelled as its field."""
    fields = {d.alias: d.field if d.ref is not None else f"({d.field})"
              for d in query.dimensions or [] if d.alias and _row_level(d.field)}
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
    if sources.whole_anchor:
        branches.append(RowSupplyBranch(cells=anchor_cells))
    for c in sources.coarse:
        restrict = (_cells(query, population=c.home, filters=filters, grain=c.grain),) if c.grain else ()
        branch = RowSupplyBranch(cells=anchor_cells, restrict=restrict)
        if branch not in branches:
            branches.append(branch)
    return branches


def _inferred_spine_anchor(free: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset]) -> DimensionAnchor:
    """An inferred pin of the spine anchors nothing: the anchor of its other dimensions, required and refused as a
    product split exactly as for the unpinned query."""
    factor = spine_factor_out(free)
    anchor = dimension_anchor(factor, datasets_by_name=datasets_by_name)
    if factor.dimensions or factor.time_dimensions:
        datasets = anchor.required()
        if len(datasets) > 1:
            refuse_product_split(factor.model_copy(update={"metrics": free.metrics}), anchor_datasets=datasets,
                                 datasets_by_name=datasets_by_name)
    return anchor


def stored_rows(query: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset]) -> Optional[RowPlan]:
    """A stored inferred stage's row plan: from its pinned anchor, or the unit for an unpinned stage with no
    dimensions; ``None`` for an anchored or spine stage (whose rows are its population's)."""
    anchor = query.population_name
    if is_spine_query(query):
        if not query.population_inferred or (anchor not in datasets_by_name and anchor != TIME_SPINE_DATASET):
            return None
        free = query.model_copy(update={"population": None, "population_inferred": False})
        dims = (_inferred_spine_anchor(free, datasets_by_name=datasets_by_name)
                if anchor == TIME_SPINE_DATASET else DimensionAnchor(datasets=(anchor,)))
        return plan_spine_rows(free, anchor=dims, datasets_by_name=datasets_by_name)
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


# --------------------------------------------------------------------------- #
# Spine rows (semantics Axiom 15)
# --------------------------------------------------------------------------- #
def _literal_text(node: Any) -> Optional[str]:
    """A literal argument (or a variable placeholder) as Mode B text; ``None`` for any other node."""
    if isinstance(node, Placeholder):
        return f"{{{node.name}}}"
    if isinstance(node, UnaryOp) and node.op == "-":
        inner = _literal_text(node.operand)
        return None if inner is None else f"-{inner}"
    if not isinstance(node, Literal) or node.value is None or isinstance(node.value, bool):
        return None
    if isinstance(node.value, str):
        return "'" + node.value.replace("'", "''") + "'"
    return str(node.value)


def _shift_text(call: TransformCall) -> Optional[str]:
    """``time_shift(…)``'s argument tail (``, -1`` / ``, -1, 'month'``) when every argument is a literal."""
    args = [_literal_text(a) for a in call.args if not isinstance(a, NamedArg)]
    kwargs = [(n, _literal_text(v)) for n, v in call_keywords(call)]
    if any(a is None for a in args) or any(v is None for _, v in kwargs):
        return None
    return "".join(f", {a}" for a in args) + "".join(f", {n}={v}" for n, v in kwargs)


def _witnessed(tree: Any, *, owner: Optional[str], shifts: tuple[str, ...], datasets_by_name: dict[str, SlayerDataset],
               seen: frozenset[str] = frozenset()) -> Iterator[tuple[AggCall, Optional[str], tuple[str, ...]]]:
    """Each top aggregation of ``tree`` with the ``time_shift`` tails enclosing it (innermost last)."""
    if isinstance(tree, AggCall):
        yield tree, owner, shifts
        return
    if isinstance(tree, TransformCall):
        tail = _shift_text(tree) if tree.op == "time_shift" else ""
        yield from _witnessed(tree.input, owner=owner, shifts=shifts if tail in ("", None) else (*shifts, tail),
                              datasets_by_name=datasets_by_name, seen=seen)
        return
    saved = _saved_metric(tree, owner=owner, datasets_by_name=datasets_by_name, seen=seen) \
        if isinstance(tree, (Ref, DottedRef)) else None
    if saved is not None:
        expanded, dataset_name, key = saved
        yield from _witnessed(expanded, owner=dataset_name, shifts=shifts, datasets_by_name=datasets_by_name,
                              seen=seen | {key})
        return
    for child in _children(tree):
        yield from _witnessed(child, owner=owner, shifts=shifts, datasets_by_name=datasets_by_name, seen=seen)


def _children(node: Any) -> list[Any]:
    """A parsed node's direct operands (calls, arithmetic, comparisons, casts, conditionals)."""
    if not isinstance(node, BaseModel):
        return []
    out: list[Any] = []
    for name in type(node).model_fields:
        value = getattr(node, name)
        for v in (value if isinstance(value, tuple) else (value,)):
            if isinstance(v, tuple):
                out.extend(x for x in v if isinstance(x, BaseModel))
            elif isinstance(v, BaseModel):
                out.append(v)
    return out


def _witness(home: str, *, agg: AggCall, shifts: tuple[str, ...]) -> str:
    """``count(<home>.*)`` with ``agg``'s window, under its enclosing shifts: positive where ``agg`` has home rows."""
    window = next((_literal_text(v) for n, v in call_keywords(agg) if n == "window"), None)
    text = f"count({home}.*{f', window={window}' if window else ''})"
    for tail in reversed(shifts):
        text = f"time_shift({text}{tail})"
    return text


def _spine_cells(query: SlayerQuery, *, population: str, witnesses: list[str], filters: list[str],
                 grain: Optional[tuple[str, ...]] = None) -> SlayerQuery:
    """A spine-bucketed cells query over ``population``: its rows whose witness is positive in a visible bucket."""
    cells = _cells(query, population=population, filters=filters, grain=grain)
    spine_tds = [t.model_dump(mode="json", exclude_none=True) for t in query.time_dimensions or []
                 if reads_spine(t.field)]
    bounds = [f for f in query.filter_conditions if _spine_only_filter(f)]
    witness = " or ".join(f"({w})" if " > 0" in w else f"{w} > 0" for w in dict.fromkeys(witnesses))
    data = cells.model_dump(mode="json", exclude_none=True)
    data["time_dimensions"] = [*data.get("time_dimensions", []), *spine_tds]
    data["filter"] = [*data.get("filter", []), *bounds, *([witness] if witness else [])]
    return SlayerQuery.model_validate(data)


def _reaches_axis(name: str, *, datasets_by_name: dict[str, SlayerDataset]) -> bool:
    """Whether a relationship path (any direction) leads from ``name`` to a dataset with an axis."""
    seen, frontier = {name}, [name]
    while frontier:
        dataset = datasets_by_name.get(frontier.pop())
        if dataset is None:
            continue
        if axis_field(dataset) is not None:
            return True
        linked = {j.to for j in dataset.relationships} | {
            n for n, m in datasets_by_name.items() if any(j.to == dataset.name for j in m.relationships)}
        frontier.extend(sorted(linked - seen))
        seen |= linked
    return False


def _anchor_spine_cells(
    query: SlayerQuery, *, free: SlayerQuery, anchor_datasets: tuple[str, ...], filters: list[str],
    datasets_by_name: dict[str, SlayerDataset], bucketed: bool = True,
) -> tuple[SlayerQuery, tuple[SlayerQuery, ...]]:
    """The anchor's cells and the in-frame cells of each component reaching an axis (its rows with a visible bucket);
    a component reaching none meets every bucket. ``bucketed=False`` keeps a single anchor's cells bucket-free, its
    framing a restriction like a product's components."""
    framing = [d for d in anchor_datasets if _reaches_axis(d, datasets_by_name=datasets_by_name)]
    if len(anchor_datasets) == 1:
        witnesses = [f"count({anchor_datasets[0]}.*)"] if framing else []
        in_frame = _spine_cells(query, population=anchor_datasets[0], witnesses=witnesses, filters=filters)
        if bucketed:
            return in_frame, ()
        return _cells(free, population=anchor_datasets[0], filters=filters), (in_frame,) if framing else ()
    refs = _query_dimension_refs(free)
    framed = tuple(
        _spine_cells(query, population=d, witnesses=[f"count({d}.*)"], filters=[],
                     grain=tuple(r for r in refs if r.split(".")[0] == d))
        for d in framing
    )
    return _cells(free, population=anchor_datasets[0], filters=filters), framed


class _SpineCoarse(BaseModel):
    """A coarser operand's cells and its witness: positive where the operand has home rows in a visible bucket."""

    cells: CoarseCells
    witness: str


class _SpineWitnesses(BaseModel):
    """Per home, the witnesses of the measures it fully supplies; the coarser cells of the rest; whether any measure
    leaves the anchor to supply the rows whole."""

    by_home: dict[str, list[str]] = {}
    coarse: list[_SpineCoarse] = []
    whole_anchor: bool = False


def _spine_witnesses(query: SlayerQuery, *, dims: list[str],
                     datasets_by_name: dict[str, SlayerDataset]) -> _SpineWitnesses:
    out = _SpineWitnesses(whole_anchor=not query.metrics)
    broadcast = query.to_many_handling == "broadcast"
    for metric in query.metrics or []:
        try:
            # Placeholders stay text: each stage's supply binds its own layered variables.
            tree = parse_expr(metric.metric)
        except Exception:  # noqa: BLE001 — an unparseable metric is refused where it binds
            continue
        found = list(_witnessed(tree, owner=None, shifts=(), datasets_by_name=datasets_by_name))
        operands = [(op, agg, shifts) for agg, owner, shifts in found
                    if (op := _operand(agg, owner=owner, dims=dims, datasets_by_name=datasets_by_name,
                                       broadcast=broadcast)) is not None]
        full = [(op, agg, shifts) for op, agg, shifts in operands if op.full]
        for op, agg, shifts in full:
            out.by_home.setdefault(op.home, []).append(_witness(op.home, agg=agg, shifts=shifts))
        if not full:
            out.coarse.extend(_SpineCoarse(cells=CoarseCells(home=op.home, grain=op.grain),
                                           witness=_witness(op.home, agg=agg, shifts=shifts))
                              for op, agg, shifts in operands)
        out.whole_anchor = out.whole_anchor or not found
    return out


def _anchor_branches(query: SlayerQuery, *, free: SlayerQuery, anchor_datasets: tuple[str, ...],
                     witnesses: _SpineWitnesses, filters: list[str],
                     datasets_by_name: dict[str, SlayerDataset]) -> list[RowSupplyBranch]:
    """The anchor's in-frame cells: whole for an anchor-supplied measure, and restricted to each coarser measure's
    in-frame cells (agreeing on the non-time tuple, never the bucket)."""
    anchor_cells, framed = _anchor_spine_cells(query, free=free, anchor_datasets=anchor_datasets, filters=filters,
                                               datasets_by_name=datasets_by_name, bucketed=not witnesses.coarse)
    out: list[RowSupplyBranch] = []
    for c in [*([None] if witnesses.whole_anchor else []), *witnesses.coarse]:
        restrict = (_spine_cells(query, population=c.cells.home, witnesses=[c.witness], filters=filters,
                                 grain=c.cells.grain),) if c else ()
        out.append(RowSupplyBranch(cells=anchor_cells, restrict=(*framed, *restrict)))
    return out


def plan_spine_rows(
    query: SlayerQuery, *, anchor: DimensionAnchor, datasets_by_name: dict[str, SlayerDataset],
) -> RowPlan:
    """An inferred spine stage's rows: every bucket × the union of each measure's non-time tuples with home rows in a
    visible bucket — the anchor's in-frame tuples with no measures, the unit with no other dimensions."""
    factor = spine_factor_out(query)
    if not (factor.dimensions or factor.time_dimensions):
        return RowPlan(population=TIME_SPINE_DATASET, supply=RowSupply(), row_sources=(UNIT,))
    datasets = anchor.required() if not query.metrics else anchor.datasets
    free = factor.model_copy(update={"metrics": query.metrics})
    filters = _field_filters(free, datasets_by_name=datasets_by_name)
    witnesses = _spine_witnesses(query, dims=determination_items(free, datasets_by_name=datasets_by_name),
                                 datasets_by_name=datasets_by_name)
    by_home, coarse = witnesses.by_home, witnesses.coarse
    branches = [RowSupplyBranch(cells=_spine_cells(query, population=h, witnesses=w, filters=filters))
                for h, w in by_home.items()]
    needs_anchor = witnesses.whole_anchor or bool(coarse)
    anchor_datasets = anchor.required() if needs_anchor else datasets
    if needs_anchor:
        for branch in _anchor_branches(query, free=free, anchor_datasets=anchor_datasets, witnesses=witnesses,
                                       filters=filters, datasets_by_name=datasets_by_name):
            if branch not in branches:
                branches.append(branch)
    row_sources = tuple(sorted({*by_home, *(anchor_datasets if needs_anchor else ())}))
    population = (anchor_datasets or tuple(by_home) or (first_metric_anchor(query) or TIME_SPINE_DATASET,))[0]
    return RowPlan(population=population,
                   supply=RowSupply(branches=tuple(branches), product=len(anchor_datasets) > 1,
                                    anchor=_single(anchor_datasets)),
                   row_sources=row_sources, reported=anchor.display)
