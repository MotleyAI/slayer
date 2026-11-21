"""The dimension anchor (semantics Axiom 12.2).

The anchor of a root-less query is the dataset with the fewest routed relationship hops that
determines every queried dimension, read from the dimensions only — never filters or metrics.
An unproven hop counts by association (raw rows excepted); dimension groups no join path
connects take the product of their anchors. The same determination core backs
``recommend_population``.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import JoinType
from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    CircularRelationshipPathError,
    PopulationErrorReason,
    PopulationInferenceError,
    SlayerError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.names import match_name
from slayer.core.relationship_search import HopFilter, UnaddressablePair, blocking_pairs, shortest_route, unaddressable_pairs
from slayer.core.relationship_walker import canonical_path, provably_to_one, terminal_dataset, walk
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery, declared_aliases
from slayer.core.time_spine import TIME_SPINE_DATASET, reads_spine, names_spine
from slayer.engine.population.anchors import (
    anchor_route,
    edge_first_refusal,
    explicit_spellings,
    find_anchor,
    unknown_dataset,
    unknown_metric,
    unqualified_count,
)
from slayer.engine.cardinality.relationship_safety import provably_fans
from slayer.engine.population.dimension_routing import safe_route_reachable
from slayer.core.syntax import (
    AggCall,
    DottedRef,
    Ref,
    StarSource,
    call_keywords,
    drop_root_qualifier,
    parse_template,
    respell_path_heads,
    root_relative,
    walk_parsed_refs,
    walk_references,
)
from slayer.storage.document_loading import all_or_raise
from slayer.storage.base import StorageBackend

# Probe verdicts per (candidate, determination item).
_OK = "ok"
_UNREACHABLE = "unreachable"
_AMBIGUOUS = "ambiguous"
# Internal to probe_item: the anchor hop has no literal edge (short-form fallback).
_NO_EDGE = "no_edge"


class PopulationChoice(BaseModel):
    """The inferred population: the anchor's datasets (several for a product) and their datasource."""

    datasets: tuple[str, ...]
    data_source: str

    @property
    def dataset_name(self) -> str:
        """The dataset planned as the population: the anchor's first group."""
        return self.datasets[0]

    @property
    def display(self) -> str:
        return " × ".join(self.datasets)


class DimensionAnchor(BaseModel):
    """A query's dimension anchor: one dataset, a product of group anchors, the unit (no dimensions), or undefined
    (``error``, raised only where the anchor is needed)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    datasets: tuple[str, ...] = ()
    error: Optional[PopulationInferenceError] = None

    @property
    def unit(self) -> bool:
        return not self.datasets and self.error is None

    @property
    def display(self) -> Optional[str]:
        """How the anchor is reported: ``a`` or ``a × b``; ``None`` for the unit or an undefined anchor."""
        return " × ".join(self.datasets) or None

    def required(self) -> tuple[str, ...]:
        """The anchor's datasets, raising its error when it is undefined."""
        if self.error is not None:
            raise self.error
        return self.datasets


class InferredRows(BaseModel):
    """A root-less query's dimension anchor, resolved datasource and that datasource's datasets, and the query with
    each first segment spelled as the dataset it anchors at."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    anchor: DimensionAnchor
    data_source: str
    datasets_by_name: dict[str, SlayerDataset]
    query: SlayerQuery


# --------------------------------------------------------------------------- #
# Determination-item extraction (design §3).
# --------------------------------------------------------------------------- #
def _ref_str(node: Ref | DottedRef) -> str:
    return ".".join(node.parts) if isinstance(node, DottedRef) else node.name


def _parse(text: str, *, root: str | None) -> Any:
    """``text`` as binding parses it, each reference relative to ``root``."""
    return root_relative(parse_template(text), root)


def _partition_refs(agg: AggCall) -> list[Ref | DottedRef]:
    """``partition_by=`` references of an aggregation node (row-valued regroup keys)."""
    out: list[Ref | DottedRef] = []
    for name, value in call_keywords(agg):
        if name != "partition_by":
            continue
        values = value if isinstance(value, (list, tuple)) else (value,)
        for v in values:
            if isinstance(v, (Ref, DottedRef)):
                out.append(v)
    return out


def _computed_dimension_refs(expression: str, *, root: str | None) -> list[str]:
    """Row-valued + partition_by refs of a computed dimension; refs inside an aggregation are excluded."""
    try:
        parsed = _parse(expression, root=root)
    except Exception:
        return []
    refs: list[str] = []
    for node in walk_parsed_refs(parsed):
        if isinstance(node, (Ref, DottedRef)):
            refs.append(_ref_str(node))
        elif isinstance(node, AggCall):
            refs.extend(_ref_str(r) for r in _partition_refs(node))
    return refs


def _walk_terminal_dataset(
    path: list[str], *, datasets_by_name: dict[str, SlayerDataset]
) -> SlayerDataset | None:
    """Dataset at the end of a dotted ``path`` (a dataset name then relationship tokens), or None."""
    root = datasets_by_name.get(path[0]) if path else None
    if root is None:
        return None
    return terminal_dataset(root=root, path=path[1:], datasets_by_name=datasets_by_name)


def _is_saved_metric_ref(ref: str, *, datasets_by_name: dict[str, SlayerDataset]) -> bool:
    """Whether ``ref`` resolves to a saved metric / custom aggregation (dropped from determination).

    The owner is the dataset the relationship path lands on — walked, not guessed from a
    segment name — so a metric reached through a named relationship is recognised too.
    A real field always wins: aggregation names share no namespace with fields,
    so a field that happens to share an aggregation's name stays a determination item.
    """
    parts = ref.split(".")
    owner = _walk_terminal_dataset(parts[:-1], datasets_by_name=datasets_by_name)
    if owner is None:
        return False
    leaf = parts[-1]
    if owner.get_field(leaf) is not None:
        return False
    return owner.get_metric(leaf) is not None or owner.get_aggregation(leaf) is not None


def _parsed_filter_refs(filter_str: str, *, root: str | None) -> tuple[list[str], bool]:
    """``(row-valued refs, ok)`` of one masked filter; spine refs are never determination items."""
    refs, ok = _all_filter_refs(filter_str, root=root)
    return [r for r in refs if not names_spine(r)], ok


def _all_filter_refs(filter_str: str, *, root: str | None) -> tuple[list[str], bool]:
    """``(row-valued refs, ok)`` of one masked filter via the execution filter parser.

    Uses ``parse_template`` (the shape execution binds), so SQL operator
    spellings and aggregations in either spelling (``sum(x)``) are all
    handled uniformly and string literals never surface as references. ``ok`` is False
    when the filter is aggregate-bearing or unparseable — either way it contributes
    nothing to determination.
    """
    try:
        parsed = _parse(filter_str, root=root)
    except Exception:
        return [], False
    nodes = list(walk_parsed_refs(parsed))
    if any(isinstance(n, AggCall) for n in nodes):
        return [], False
    return [_ref_str(n) for n in nodes if isinstance(n, (Ref, DottedRef))], True


def _filter_refs(
    filter_str: str, *, root: str | None, dataset_scopes: list[dict[str, SlayerDataset]]
) -> list[str]:
    """Field references of one query filter; drops aggregate-bearing filters and saved-metric refs.

    All ``{var}`` are masked to a neutral literal first, so a reference introduced
    only by a variable value never participates.
    """
    refs, _ = _parsed_filter_refs(filter_str, root=root)
    return [
        r for r in refs
        if not _saved_metric_in_every_scope(r, dataset_scopes=dataset_scopes)
    ]


def _query_dimension_refs(query: SlayerQuery) -> list[str]:
    """Row-valued refs of the query's dimensions + time dimensions.

    Computed and time dimensions contribute their row-valued and partition_by refs
    (never refs inside an aggregation); plain dimensions contribute their name.
    """
    root = query.population_name
    refs: list[str] = []
    for dim in query.dimensions or []:
        ref = dim.ref
        if dim.is_computed or ref is None:
            refs.extend(_computed_dimension_refs(dim.field, root=root))
        else:
            refs.append(".".join(drop_root_qualifier(ref.parts, root, quoted=ref.quoted_parts)))
    for td in query.time_dimensions or []:
        refs.extend(_computed_dimension_refs(td.field, root=root))
    return refs


def _restriction_refs(query: SlayerQuery) -> list[str]:
    """Each query-declared INNER join, read as a field filter on its declaring dataset's key."""
    return [
        f"{e.dataset}.{j.column_pairs[0][0]}"
        for e in query.extensions or [] for j in e.relationships or [] if j.join_type is JoinType.INNER
    ]


def determination_items(
    query: SlayerQuery,
    *,
    datasets_by_name: dict[str, SlayerDataset],
    dataset_scopes: list[dict[str, SlayerDataset]] | None = None,
) -> list[str]:
    """The dimension + time dimension refs the anchor must determine; filters, metrics and order entries never
    participate. Deduped, preserving first occurrence."""
    del datasets_by_name, dataset_scopes
    items: list[str] = []
    seen: set[str] = set()

    def _add(ref: str) -> None:
        if ref and "." not in ref and query.population_name is None:
            return  # an alias, or refused before inference
        if ref and ref not in seen:
            seen.add(ref)
            items.append(ref)

    for ref in _query_dimension_refs(query):
        if not names_spine(ref):
            _add(ref)
    return items


# --------------------------------------------------------------------------- #
# Per-candidate determination probe (design §2).
# --------------------------------------------------------------------------- #
def _associates(edge: Any, *, datasets_by_name: dict[str, SlayerDataset]) -> bool:
    """An unproven hop in its declared direction: it counts by association (semantics 12.2)."""
    return (
        edge.cardinality is None and edge.declaring_dataset == edge.source_dataset
        and edge.product is None and edge.lateral is None
        and not provably_fans(edge=edge, target_dataset=datasets_by_name[edge.to])
    )


def _walk_to_one(
    *, start: SlayerDataset, tokens: list[str], datasets_by_name: dict[str, SlayerDataset], associate: bool = False,
) -> tuple[SlayerDataset | None, str, tuple[str, ...]]:
    """Walk ``tokens`` from ``start``; ``(terminal dataset, verdict, canonical path)``.

    Every oriented hop must be provably to-one, or with ``associate`` an unproven hop in its declared direction;
    ``_NO_EDGE`` distinguishes a path that does not resolve from a resolved-but-unsafe (``_UNREACHABLE``) one.
    """
    try:
        chain = walk(root=start, path=tokens, datasets_by_name=datasets_by_name)
    except AmbiguousRelationshipPathError:
        return (None, _AMBIGUOUS, ())
    except CircularRelationshipPathError:
        return (None, _UNREACHABLE, ())
    if chain is None:
        return (None, _NO_EDGE, ())
    if not all(provably_to_one(edge=e, target_dataset=datasets_by_name[e.to])
               or (associate and _associates(e, datasets_by_name=datasets_by_name)) for e in chain):
        return (None, _UNREACHABLE, ())
    return (datasets_by_name[chain[-1].to] if chain else start, _OK, canonical_path(chain))


def _anchored_tokens(*, start: SlayerDataset, parts: list[str], datasets_by_name: dict[str, SlayerDataset]) -> tuple[list[str] | None, str]:
    """``item``'s path from ``start``: the route binding takes to its anchor, then its explicit route."""
    anchor = find_anchor(parts[0], quoted=False, reader=start, datasets_by_name=datasets_by_name)
    if anchor is None:
        return None, _UNREACHABLE
    try:
        reached = anchor_route(reader=start, anchor=anchor, datasets_by_name=datasets_by_name, rest=parts[1:])
    except UnresolvableDimensionRelationshipError:
        safe = safe_route_reachable(root=start, target_dataset=anchor.name, datasets_by_name=datasets_by_name)
        return None, _AMBIGUOUS if safe else _UNREACHABLE
    if reached.unrelated:
        return None, _UNREACHABLE
    return [*reached.route, *parts[1:-1]], _OK


def probe_item(
    *, root: str, item: str, datasets_by_name: dict[str, SlayerDataset], associate: bool = False,
) -> tuple[str, tuple[str, ...]]:
    """Route ``item`` (``anchor[.route].leaf``) from ``root`` as binding does; ``(verdict, canonical path)`` — hops are
    its length.

    The anchor is reached by its only to-one path, else its only route (the shared anchor resolver); the item is
    determined when every hop of that path and its explicit route is provably to-one and the leaf is a field on the
    terminal dataset. Verdict is ``_OK`` then, ``_AMBIGUOUS`` when two to-one paths reach the anchor or a hop spans
    parallel edges, else ``_UNREACHABLE``.
    """
    parts = item.split(".")
    leaf = parts[-1]
    start = datasets_by_name.get(root)
    if start is None or len(parts) < 2:
        return (_UNREACHABLE, ())
    tokens, verdict = _anchored_tokens(start=start, parts=parts, datasets_by_name=datasets_by_name)
    if tokens is None:
        return (verdict, ())
    terminal, verdict, hops = _walk_to_one(start=start, tokens=tokens, datasets_by_name=datasets_by_name,
                                           associate=associate)
    if terminal is None:
        return (_UNREACHABLE if verdict == _NO_EDGE else verdict, ())
    # A saved metric read in a dimension is the terminal's own (binding refuses an aggregate there).
    if terminal.get_field(leaf) is None and terminal.get_metric(leaf) is None:
        return (_UNREACHABLE, ())
    return (_OK, hops)


def determined_path(*, root: str, item: str, datasets_by_name: dict[str, SlayerDataset]) -> tuple[str, ...] | None:
    """``item``'s canonical path from ``root`` when ``root`` determines it, an unproven hop by association as the
    dimension anchor counts it (semantics 12.2), else ``None``."""
    verdict, path = probe_item(root=root, item=item, datasets_by_name=datasets_by_name, associate=True)
    return path if verdict == _OK else None


def ambiguity_resolved_path(*, root: str, item: str, datasets_by_name: dict[str, SlayerDataset]) -> tuple[str, ...] | None:
    """For a ``dataset.leaf`` item whose probe fails only on ambiguity, its tie-break-best fan-out-free canonical path."""
    if probe_item(root=root, item=item, datasets_by_name=datasets_by_name)[0] != _AMBIGUOUS:
        return None
    *path, leaf = item.split(".")
    target = datasets_by_name.get(path[0]) if len(path) == 1 else None
    if target is None or target.get_field(leaf) is None:
        return None
    route = shortest_route(root=datasets_by_name[root], target=target.name, datasets_by_name=datasets_by_name,
                           hop_filter=HopFilter.TO_ONE)
    return route.tokens if route is not None else None


def item_blockers(*, root: str, item: str, datasets_by_name: dict[str, SlayerDataset]) -> list[UnaddressablePair]:
    """The unaddressable pairs that keep ``item``'s path from resolving from ``root``."""
    start = datasets_by_name.get(root)
    parts = item.split(".")
    if start is None:
        return []
    path, _verdict = _anchored_tokens(start=start, parts=parts, datasets_by_name=datasets_by_name)
    if path is None:
        anchor = find_anchor(parts[0], quoted=False, reader=start, datasets_by_name=datasets_by_name)
        if anchor is None or anchor.name == start.name:
            return []
        return blocking_pairs(root=start, target=anchor.name, datasets_by_name=datasets_by_name)
    if not path:
        return []
    try:
        if walk(root=start, path=path, datasets_by_name=datasets_by_name) is not None or len(path) > 1:
            return []
    except AmbiguousRelationshipPathError as e:
        return [p for p in unaddressable_pairs(datasets_by_name=datasets_by_name)
                if {p.source_dataset, p.to} == {e.source_dataset, e.target_dataset}]
    except CircularRelationshipPathError:
        return []
    return blocking_pairs(root=start, target=path[0], datasets_by_name=datasets_by_name)


class CandidateVerdict(BaseModel):
    """A candidate population's determination outcome over the item set."""

    dataset_name: str
    viable: bool
    total_hops: int
    ambiguous_blocked: bool
    ambiguous_item: str | None = None
    reachable_items: list[str]
    unreachable_items: list[str]


def evaluate_candidates(
    *, items: list[str], candidates: list[str], datasets_by_name: dict[str, SlayerDataset], associate: bool = False,
) -> list[CandidateVerdict]:
    """Probe every candidate over every determination item."""
    verdicts: list[CandidateVerdict] = []
    for name in candidates:
        probes = {it: probe_item(root=name, item=it, datasets_by_name=datasets_by_name, associate=associate)
                  for it in items}
        statuses = [v for v, _ in probes.values()]
        reachable = [it for it in items if probes[it][0] == _OK]
        unreachable = [it for it in items if probes[it][0] != _OK]
        viable = all(s == _OK for s in statuses)
        ambiguous_blocked = _UNREACHABLE not in statuses and _AMBIGUOUS in statuses
        ambiguous_item = next(
            (it for it in items if probes[it][0] == _AMBIGUOUS), None
        )
        verdicts.append(CandidateVerdict(
            dataset_name=name,
            viable=viable,
            total_hops=sum(len(h) for _, h in probes.values()),
            ambiguous_blocked=ambiguous_blocked,
            ambiguous_item=ambiguous_item,
            reachable_items=reachable,
            unreachable_items=unreachable,
        ))
    return verdicts


def select_from_verdicts(verdicts: list[CandidateVerdict]) -> tuple[str | None, list[str]]:
    """Unique fewest-hops viable candidate, or ``(None, tied)`` when several tie at the minimum."""
    viable = [v for v in verdicts if v.viable]
    if not viable:
        return None, []
    minimum = min(v.total_hops for v in viable)
    winners = sorted(v.dataset_name for v in viable if v.total_hops == minimum)
    if len(winners) == 1:
        return winners[0], []
    return None, winners


# --------------------------------------------------------------------------- #
# Datasource scoping (spec: Datasource scoping for root-less queries).
# --------------------------------------------------------------------------- #
def _saved_metric_in_every_scope(
    ref: str, *, dataset_scopes: list[dict[str, SlayerDataset]]
) -> bool:
    """Whether ``ref`` resolves to a saved metric in every scope holding its anchor dataset.

    A scope where the anchor dataset is absent abstains; a real field in any scope
    keeps the reference (field-wins precedence, extended across datasources).
    """
    scopes = [s for s in dataset_scopes if ref.split(".")[0] in s]
    return bool(scopes) and all(
        _is_saved_metric_ref(ref, datasets_by_name=s) for s in scopes
    )


def _dimension_anchor_names(query: SlayerQuery) -> set[str]:
    """First segments of the dimensions' and time dimensions' references (the sibling-stage refusal reads these)."""
    return {ref.split(".")[0] for ref in _query_dimension_refs(query) if "." in ref and not names_spine(ref)}


def _anchor_names(
    query: SlayerQuery, *, dataset_scopes: list[dict[str, SlayerDataset]] | None = None
) -> set[str]:
    """First segments of the dotted references of the dimensions, time dimensions and field-typed filters, read
    from the parser so string literals never surface. With ``dataset_scopes`` a filter reference resolving to a
    saved metric is excluded."""
    refs = _query_dimension_refs(query)
    for f in query.filter_conditions:
        filter_refs, _ = _parsed_filter_refs(f, root=query.population_name)
        refs.extend(
            r for r in filter_refs
            if not dataset_scopes
            or not _saved_metric_in_every_scope(r, dataset_scopes=dataset_scopes)
        )
    refs.extend(_restriction_refs(query))
    root = query.population_name
    # A bare name in a root-less query is an alias (or refused): it anchors nothing.
    heads = {ref.split(".")[0] for ref in refs if "." in ref}
    return heads | ({root} if root is not None and any(r and "." not in r for r in refs) else set())


async def _classification_scopes(
    *, storage: StorageBackend, data_source: str | None, anchors: set[str]
) -> tuple[dict[str, dict[str, SlayerDataset]], dict[str, set[str]]]:
    """``(datasets-by-name per candidate datasource, datasources per dataset name)``.

    Candidates are the datasources holding at least one syntactic anchor dataset —
    or just the explicit ``data_source``, which pins scoping before anchor voting.
    """
    ds_by_dataset: dict[str, set[str]] = {}
    if data_source is not None:
        candidates = {data_source}
    else:
        for ds, name in await storage._list_all_dataset_identities():
            ds_by_dataset.setdefault(name, set()).add(ds)
        candidates = set().union(
            *(ds_by_dataset[a] for a in anchors if a in ds_by_dataset)
        )
    scopes: dict[str, dict[str, SlayerDataset]] = {}
    for ds in sorted(candidates):
        scopes[ds] = {m.name: m for m in all_or_raise(await storage.load_datasets(data_source=ds))}
    return scopes, ds_by_dataset


def _resolve_datasource(
    *, ds_by_dataset: dict[str, set[str]], data_source: str | None, anchors: set[str]
) -> str:
    """The single datasource holding every referenced anchor dataset, or fail closed."""
    if data_source is not None:
        return data_source
    relevant = [a for a in anchors if a in ds_by_dataset]
    if not relevant:
        raise PopulationInferenceError(PopulationErrorReason.NO_DATASOURCE)
    candidate = set.intersection(*(ds_by_dataset[a] for a in relevant))
    if not candidate:
        involved = sorted(set().union(*(ds_by_dataset[a] for a in relevant)))
        raise PopulationInferenceError(
            PopulationErrorReason.NO_DATASOURCE, datasources=involved
        )
    if len(candidate) > 1:
        raise PopulationInferenceError(
            PopulationErrorReason.AMBIGUOUS_DATASOURCE, datasources=sorted(candidate)
        )
    return candidate.pop()


# --------------------------------------------------------------------------- #
# The spine factor (queries/time-spine).
# --------------------------------------------------------------------------- #
def spine_factor_out(query: SlayerQuery) -> SlayerQuery:
    """``query``'s P factor: its spine time dimensions and spine filters removed (metrics too)."""
    return query.model_copy(update={
        "dimensions": [d for d in query.dimensions or [] if not reads_spine(d.field)] or None,
        "time_dimensions": [td for td in query.time_dimensions or [] if not reads_spine(td.field)] or None,
        "filter": [f for f in query.filter_conditions if not _spine_only_filter(f)] or None,
        "metrics": None,
    })


def _spine_only_filter(filter_str: str) -> bool:
    refs, ok = _all_filter_refs(filter_str, root=None)
    return ok and bool(refs) and all(names_spine(r) for r in refs)


def _qualified_refs(parsed) -> list[str]:
    """Every dotted reference in ``parsed``, aggregation sources included."""
    out: list[str] = []
    for node in walk_parsed_refs(parsed):
        if isinstance(node, AggCall):
            out.extend(_qualified_refs(node.source))
        elif isinstance(node, DottedRef):
            out.append(_ref_str(node))
    return out


def metric_anchor_names(query: SlayerQuery) -> set[str]:
    """First segments of every dotted reference in ``query``'s metrics, aggregation sources included."""
    names: set[str] = set()
    for m in query.metrics or []:
        try:
            parsed = _parse(m.metric, root=query.population_name)
        except Exception:
            continue
        names.update(ref.split(".")[0] for ref in _qualified_refs(parsed))
    return names - {TIME_SPINE_DATASET}


async def infer_spine_population(
    *,
    query: SlayerQuery,
    storage: StorageBackend,
    data_source: str | None = None,
    sibling_stage_names: set[str] | None = None,
) -> PopulationChoice:
    """P for a spine query: the anchor of its other dimensions, else the unit (``time_spine``)."""
    factor = spine_factor_out(query)
    if factor.dimensions or factor.time_dimensions:
        return await infer_population(
            query=factor, storage=storage, data_source=data_source,
            sibling_stage_names=sibling_stage_names,
        )
    return PopulationChoice(
        datasets=(TIME_SPINE_DATASET,),
        data_source=await spine_datasource(
            queries=[query], storage=storage, data_source=data_source, sibling_stage_names=sibling_stage_names,
        ),
    )


async def spine_datasource(
    *,
    queries: list[SlayerQuery],
    storage: StorageBackend,
    data_source: str | None,
    sibling_stage_names: set[str] | None = None,
) -> str:
    """The datasource of spine-rooted queries: pinned, else the one holding every dataset they name."""
    if data_source is not None:
        return data_source
    anchors: set[str] = set()
    for q in queries:
        anchors |= _anchor_names(q) | metric_anchor_names(q) | {q.population_name or TIME_SPINE_DATASET}
    anchors -= {TIME_SPINE_DATASET, *(sibling_stage_names or ())}
    if not anchors:
        datasources = await storage.list_datasources()
        if len(datasources) == 1:
            return datasources[0]
    _scopes, ds_by_dataset = await _classification_scopes(
        storage=storage, data_source=None, anchors=anchors,
    )
    return _resolve_datasource(ds_by_dataset=ds_by_dataset, data_source=None, anchors=anchors)


def _spelled_paths(verdict: CandidateVerdict, *, datasets_by_name: dict[str, SlayerDataset]) -> str:
    """The explicit spellings of ``verdict``'s ambiguous item from its candidate, as a clause."""
    item = verdict.ambiguous_item
    reader = datasets_by_name.get(verdict.dataset_name)
    if item is None or reader is None:
        return ""
    parts = item.split(".")
    anchor = find_anchor(parts[0], quoted=False, reader=reader, datasets_by_name=datasets_by_name)
    if anchor is None:
        return ""
    spellings = explicit_spellings(reader=reader, anchor=anchor, datasets_by_name=datasets_by_name, rest=parts[1:])
    return f"; spell the path: {', '.join(repr(p) for p in spellings)}" if spellings else ""


# --------------------------------------------------------------------------- #
# Anchors and refusals (queries/references).
# --------------------------------------------------------------------------- #
def _entry_trees(query: SlayerQuery, *, metrics: bool) -> list[Any]:
    """The parse trees of the query's dimensions, time dimensions and filters (and metrics), unparseable ones skipped."""
    texts = [d.field for d in query.dimensions or []] + [td.field for td in query.time_dimensions or []]
    texts += list(query.filter_conditions)
    if metrics:
        texts += [m.metric for m in query.metrics or []]
    out: list[Any] = []
    for text in texts:
        try:
            out.append(parse_template(text))
        except Exception:  # noqa: BLE001 — an unparseable text is reported where it binds
            continue
    return out


async def _visible_datasets(*, storage: StorageBackend, data_source: str | None) -> dict[str, SlayerDataset]:
    """Every dataset the caller sees (in ``data_source`` when pinned), for a refusal's suggestions."""
    names = [ds for ds in await storage.list_datasources() if data_source in (None, ds)]
    out: dict[str, SlayerDataset] = {}
    for ds in names:
        loaded, _unloaded = await storage.load_datasets(data_source=ds)
        out.update({m.name: m for m in loaded if m.name not in out})
    return out


async def _anchor_spellings(
    query: SlayerQuery, *, storage: StorageBackend, data_source: str | None, siblings: set[str],
) -> SlayerQuery:
    """``query`` with each first segment spelled as the dataset it anchors at; a bare name, ``count(*)`` or a first
    segment naming no dataset is refused as binding refuses it."""
    aliases = declared_aliases(query)
    anchors = await _anchor_table(storage=storage, data_source=data_source, siblings=siblings)
    respell: dict[str, str] = {}
    for tree in _entry_trees(query, metrics=True):
        for node, _agg in walk_references(tree):
            if isinstance(node, StarSource):
                raise unqualified_count(await _would_infer(query, storage=storage, data_source=data_source,
                                                           siblings=siblings))
            name = await _anchor_name(node, aliases=aliases, anchors=anchors, storage=storage,
                                      data_source=data_source)
            if name is not None and name != node.parts[0]:
                respell[node.parts[0]] = name
    return _respelled_heads(query, respell) if respell else query


async def declared_head_spellings(
    query: SlayerQuery, *, storage: StorageBackend, data_source: str | None, siblings: set[str],
) -> SlayerQuery:
    """``query`` with each first segment naming a dataset spelled as declared; nothing refused (a named population
    converts legacy forms where it binds)."""
    anchors = await _anchor_table(storage=storage, data_source=data_source, siblings=siblings)
    respell: dict[str, str] = {}
    for tree in _entry_trees(query, metrics=True):
        for node, _agg in walk_references(tree):
            if not isinstance(node, DottedRef):
                continue
            hit = find_anchor(node.parts[0], quoted=node.quoted_at(0), reader=None, datasets_by_name=anchors)
            if hit is not None and hit.name != node.parts[0]:
                respell[node.parts[0]] = hit.name
    return _respelled_heads(query, respell) if respell else query


async def _anchor_table(
    *, storage: StorageBackend, data_source: str | None, siblings: set[str],
) -> dict[str, SlayerDataset]:
    """Every name a first segment may anchor at: the visible datasets and the spine, and the sibling stages, spelled as
    stages so they precede a same-named dataset."""
    identities = await storage._list_all_dataset_identities()
    names = sorted({n for ds, n in identities if data_source in (None, ds)} | {TIME_SPINE_DATASET})
    table = {n: SlayerDataset.model_construct(name=n) for n in names}
    table.update({f"<stage {s}>": SlayerDataset.model_construct(name=s).with_spelling(s) for s in sorted(siblings)})
    return table


async def _anchor_name(
    node: Ref | DottedRef, *, aliases: list[str], anchors: dict[str, SlayerDataset], storage: StorageBackend,
    data_source: str | None,
) -> str | None:
    """The dataset a dotted ``node`` anchors at (``None`` for an alias); a bare non-alias or unknown anchor refused."""
    if isinstance(node, Ref):
        if match_name(name=node.name, declared=aliases, quoted=node.quoted) is None:
            raise unknown_metric(node.name, datasets=(
                await _visible_datasets(storage=storage, data_source=data_source)).values())
        return None
    hit = find_anchor(node.parts[0], quoted=node.quoted_at(0), reader=None, datasets_by_name=anchors)
    if hit is None:
        visible = await _visible_datasets(storage=storage, data_source=data_source)
        raise edge_first_refusal(node.parts, datasets_by_name=visible) or unknown_dataset(node.parts)
    return hit.name


def _respelled_heads(query: SlayerQuery, respell: dict[str, str]) -> SlayerQuery:
    """``query`` with each unquoted first segment of ``respell`` written as its dataset's name, every other byte kept."""
    def heads(text: str) -> str:
        return respell_path_heads(text, respell=respell.get)
    return query.model_copy(update={
        "dimensions": [d.model_copy(update={"field": heads(d.field)}) for d in query.dimensions or []] or None,
        "time_dimensions": [td.model_copy(update={"field": heads(td.field)})
                            for td in query.time_dimensions or []] or None,
        "filter": [heads(f) for f in query.filter_conditions] or None,
        "metrics": [m.model_copy(update={"metric": heads(m.metric)}) for m in query.metrics or []] or None,
    })


async def _would_infer(
    query: SlayerQuery, *, storage: StorageBackend, data_source: str | None, siblings: set[str],
) -> str | None:
    """The population inference picks for ``query``'s dimensions and filters, if it picks one."""
    def starless(text: str) -> bool:
        try:
            return not any(isinstance(n, StarSource) for n, _ in walk_references(parse_template(text)))
        except Exception:  # noqa: BLE001
            return False
    rest = query.model_copy(update={"metrics": None, "filter": [f for f in query.filter_conditions if starless(f)] or None})
    if not (rest.dimensions or rest.time_dimensions or rest.filter):
        return None
    try:
        choice = await infer_population(query=rest, storage=storage, data_source=data_source,
                                        sibling_stage_names=siblings)
    except SlayerError:
        return None
    return choice.dataset_name if len(choice.datasets) == 1 else None


# --------------------------------------------------------------------------- #
# Entry point.
# --------------------------------------------------------------------------- #
def _components(datasets_by_name: dict[str, SlayerDataset]) -> dict[str, str]:
    """Each dataset's connected component (declared relationships, either direction), named by a representative."""
    parent = {n: n for n in datasets_by_name}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for dataset in datasets_by_name.values():
        for rel in dataset.relationships:
            if rel.to in parent:
                parent[find(dataset.name)] = find(rel.to)
    return {n: find(n) for n in parent}


def _choose_anchor(
    *, items: list[str], candidates: list[str], datasets_by_name: dict[str, SlayerDataset], associate: bool,
) -> str:
    """The unique fewest-hops candidate determining every item, or the reasoned refusal."""
    verdicts = evaluate_candidates(items=items, candidates=candidates, datasets_by_name=datasets_by_name,
                                   associate=associate)
    winner, tied = select_from_verdicts(verdicts)
    if winner is not None:
        return winner
    if tied:
        raise PopulationInferenceError(PopulationErrorReason.TIE, candidates=tied)
    pairs = {
        p.describe(): None
        for v in verdicts for it in v.unreachable_items
        for p in item_blockers(root=v.dataset_name, item=it, datasets_by_name=datasets_by_name)
    }
    blocked = [v for v in verdicts if v.ambiguous_blocked]
    if blocked:
        first = blocked[0]
        raise PopulationInferenceError(
            PopulationErrorReason.AMBIGUOUS_PATH,
            candidates=[v.dataset_name for v in blocked],
            detail="; ".join([
                f"from candidate {first.dataset_name!r}, the relationship path to "
                f"{first.ambiguous_item!r} is ambiguous{_spelled_paths(first, datasets_by_name=datasets_by_name)}",
                *pairs,
            ]),
        )
    raise PopulationInferenceError(
        PopulationErrorReason.NO_VIABLE_CANDIDATE, candidates=sorted(candidates),
        detail="; ".join(pairs) or None,
    )


def dimension_anchor(
    query: SlayerQuery, *, datasets_by_name: dict[str, SlayerDataset], raw_rows: bool = False,
) -> DimensionAnchor:
    """``query``'s dimension anchor (semantics 12.2); raw rows probe provable determination only, with no product."""
    items = determination_items(query, datasets_by_name=datasets_by_name)
    if not items:
        return DimensionAnchor()
    component = _components(datasets_by_name)
    groups: dict[str, list[str]] = {}
    for item in items:
        head = item.split(".")[0]
        groups.setdefault(component.get(head, head), []).append(item)
    if len(groups) > 1 and raw_rows:
        return DimensionAnchor(error=PopulationInferenceError(
            PopulationErrorReason.NO_VIABLE_CANDIDATE,
            detail=f"no join path connects the dimensions {items}; raw rows need one dataset determining every "
                   f"dimension",
        ))
    try:
        return DimensionAnchor(datasets=tuple(
            _choose_anchor(
                items=group, datasets_by_name=datasets_by_name, associate=not raw_rows,
                candidates=sorted(n for n, c in component.items() if c == key) if len(groups) > 1
                else sorted(datasets_by_name),
            )
            for key, group in groups.items()
        ))
    except PopulationInferenceError as e:
        return DimensionAnchor(error=e)


def _filter_anchor_names(query: SlayerQuery) -> set[str]:
    """First segments of every dotted reference in ``query``'s filters, aggregation sources included."""
    names: set[str] = set()
    for f in query.filter_conditions:
        try:
            parsed = _parse(f, root=None)
        except Exception:  # noqa: BLE001 — an unparseable filter is refused where it binds
            continue
        names.update(ref.split(".")[0] for ref in _qualified_refs(parsed))
    return names


def _filter_source_anchors(query: SlayerQuery, *, dataset_scopes: list[dict[str, SlayerDataset]]) -> set[str]:
    """First segments of the references inside filters' aggregations, a saved metric excepted."""
    names: set[str] = set()
    for f in query.filter_conditions:
        try:
            parsed = _parse(f, root=None)
        except Exception:  # noqa: BLE001 — an unparseable filter is refused where it binds
            continue
        names.update(r.split(".")[0] for r in _qualified_refs(parsed)
                     if "." in r and not _saved_metric_in_every_scope(r, dataset_scopes=dataset_scopes))
    return names


def _scoping_anchors(query: SlayerQuery) -> set[str]:
    """The datasets that scope a root-less query's datasource: its dimensions', filters' and metrics' anchors."""
    return _anchor_names(query) | metric_anchor_names(query)


def _refuse_sibling_anchors(anchored: set[str], *, what: str) -> None:
    """A root-less stage reading a sibling stage's name: binding would read the stage, inference the dataset."""
    if anchored:
        name = min(anchored)
        raise PopulationInferenceError(
            PopulationErrorReason.SIBLING_STAGE, candidates=sorted(anchored),
            detail=f"{what} anchor at sibling stage {name!r}; name it as the stage's population",
        )


async def infer_rows(
    *,
    query: SlayerQuery,
    storage: StorageBackend,
    data_source: str | None = None,
    sibling_stage_names: set[str] | None = None,
    raw_rows: bool = False,
) -> InferredRows:
    """A root-less query's dimension anchor (undefined anchors carry their error) and datasource."""
    siblings = sibling_stage_names or set()
    query = await _anchor_spellings(query, storage=storage, data_source=data_source, siblings=siblings)
    _refuse_sibling_anchors(_dimension_anchor_names(query) & set(siblings), what="dimensions")
    syntactic = _scoping_anchors(query) - siblings - {TIME_SPINE_DATASET}
    scopes, ds_by_dataset = await _classification_scopes(storage=storage, data_source=data_source, anchors=syntactic)
    # A saved-measure filter reference anchors nothing, so it never meets a sibling nor steers scoping.
    read = (_anchor_names(query, dataset_scopes=list(scopes.values())) | metric_anchor_names(query)
            | _filter_source_anchors(query, dataset_scopes=list(scopes.values())))
    _refuse_sibling_anchors(read & set(siblings), what="references")
    # A saved-measure filter reference never steers scoping; measure filters alone scope a query with nothing else.
    anchors = read - siblings
    anchors = (anchors or _filter_anchor_names(query) - siblings) - {TIME_SPINE_DATASET}
    if anchors - syntactic:
        scopes, ds_by_dataset = await _classification_scopes(storage=storage, data_source=data_source,
                                                             anchors=anchors)
    if not anchors and data_source is None:
        datasources = await storage.list_datasources()
        if len(datasources) == 1:
            data_source = datasources[0]
            scopes, ds_by_dataset = await _classification_scopes(storage=storage, data_source=data_source,
                                                                 anchors=set())
    ds = _resolve_datasource(ds_by_dataset=ds_by_dataset, data_source=data_source, anchors=anchors)
    datasets_by_name = scopes[ds]
    return InferredRows(
        anchor=dimension_anchor(query, datasets_by_name=datasets_by_name, raw_rows=raw_rows),
        data_source=ds, datasets_by_name=datasets_by_name, query=query,
    )


async def infer_population(
    *,
    query: SlayerQuery,
    storage: StorageBackend,
    data_source: str | None = None,
    sibling_stage_names: set[str] | None = None,
) -> PopulationChoice:
    """A root-less query's dimension anchor, or raise :class:`PopulationInferenceError`."""
    rows = await infer_rows(query=query, storage=storage, data_source=data_source,
                            sibling_stage_names=sibling_stage_names)
    datasets = rows.anchor.required()
    if not datasets:
        raise PopulationInferenceError(PopulationErrorReason.EMPTY_DETERMINATION)
    return PopulationChoice(datasets=tuple(datasets), data_source=rows.data_source)
