"""Dimension-determined population inference.

The population of a rootless query is the model with the fewest routed join hops
that determines every queried dimension along provably to-one paths, read from
the dimensions and field-typed (aggregate-free) filters only — never measures,
so adding a measure can never re-quantify the rows. The same to-one determination
core backs ``recommend_root_model``.
"""

from __future__ import annotations

from pydantic import BaseModel

from slayer.core.errors import (
    AmbiguousJoinPathError,
    CircularJoinPathError,
    PopulationErrorReason,
    PopulationInferenceError,
)
from slayer.core.join_search import HopFilter, UnaddressablePair, blocking_pairs, shortest_route, unaddressable_pairs
from slayer.core.join_walker import canonical_path, provably_to_one, terminal_model, walk
from slayer.core.models import SlayerModel
from slayer.core.query import ComputedDimension, SlayerQuery, render_probe_text
from slayer.core.time_spine import TIME_SPINE_MODEL, mentions_spine, names_spine
from slayer.engine.dimension_routing import resolve_route, safe_route_reachable
from slayer.core.syntax import (
    AggCall,
    DottedRef,
    Ref,
    parse_expr,
    walk_parsed_refs,
)
from slayer.memories.resolver import _all_models_in_datasource
from slayer.storage.base import StorageBackend

# Probe verdicts per (candidate, determination item).
_OK = "ok"
_UNREACHABLE = "unreachable"
_AMBIGUOUS = "ambiguous"
# Internal to probe_item: the anchor hop has no literal edge (short-form fallback).
_NO_EDGE = "no_edge"


class PopulationChoice(BaseModel):
    """The inferred population: the chosen model and its datasource."""

    model_name: str
    data_source: str


# --------------------------------------------------------------------------- #
# Determination-item extraction (design §3).
# --------------------------------------------------------------------------- #
def _ref_str(node: Ref | DottedRef) -> str:
    return ".".join(node.parts) if isinstance(node, DottedRef) else node.name


def _partition_refs(agg: AggCall) -> list[Ref | DottedRef]:
    """``partition_by=`` references of an aggregation node (row-valued regroup keys)."""
    out: list[Ref | DottedRef] = []
    for name, value in agg.kwargs:
        if name != "partition_by":
            continue
        values = value if isinstance(value, (list, tuple)) else (value,)
        for v in values:
            if isinstance(v, (Ref, DottedRef)):
                out.append(v)
    return out


def _computed_dimension_refs(expression: str) -> list[str]:
    """Row-valued + partition_by refs of a computed dimension; refs inside an aggregation are excluded."""
    try:
        parsed = parse_expr(expression)
    except Exception:
        return []
    refs: list[str] = []
    for node in walk_parsed_refs(parsed):
        if isinstance(node, (Ref, DottedRef)):
            refs.append(_ref_str(node))
        elif isinstance(node, AggCall):
            refs.extend(_ref_str(r) for r in _partition_refs(node))
    return refs


def _walk_terminal_model(
    path: list[str], *, models_by_name: dict[str, SlayerModel]
) -> SlayerModel | None:
    """Model at the end of a dotted ``path`` (a model name then join tokens), or None."""
    root = models_by_name.get(path[0]) if path else None
    if root is None:
        return None
    return terminal_model(root=root, path=path[1:], models_by_name=models_by_name)


def _is_saved_measure_ref(ref: str, *, models_by_name: dict[str, SlayerModel]) -> bool:
    """Whether ``ref`` resolves to a saved measure / custom aggregation (dropped from determination).

    The owner is the model the join path lands on — walked, not guessed from a
    segment name — so a measure reached through a named join is recognised too.
    A real column always wins: aggregation names share no namespace with columns,
    so a column that happens to share an aggregation's name stays a determination item.
    """
    parts = ref.split(".")
    owner = _walk_terminal_model(parts[:-1], models_by_name=models_by_name)
    if owner is None:
        return False
    leaf = parts[-1]
    if owner.get_column(leaf) is not None:
        return False
    return owner.get_measure(leaf) is not None or owner.get_aggregation(leaf) is not None


def _parsed_filter_refs(filter_str: str) -> tuple[list[str], bool]:
    """``(row-valued refs, ok)`` of one masked filter; spine refs are never determination items."""
    refs, ok = _all_filter_refs(filter_str)
    return [r for r in refs if not names_spine(r)], ok


def _all_filter_refs(filter_str: str) -> tuple[list[str], bool]:
    """``(row-valued refs, ok)`` of one masked filter via the execution filter parser.

    Uses ``parse_expr`` (the same parser execution binds with), so SQL operator
    spellings and aggregations in either spelling (``sum(x)``) are all
    handled uniformly and string literals never surface as references. ``ok`` is False
    when the filter is aggregate-bearing or unparseable — either way it contributes
    nothing to determination.
    """
    try:
        parsed = parse_expr(render_probe_text(filter_str))
    except Exception:
        return [], False
    nodes = list(walk_parsed_refs(parsed))
    if any(isinstance(n, AggCall) for n in nodes):
        return [], False
    return [_ref_str(n) for n in nodes if isinstance(n, (Ref, DottedRef))], True


def _filter_refs(
    filter_str: str, *, model_scopes: list[dict[str, SlayerModel]]
) -> list[str]:
    """Field references of one query filter; drops aggregate-bearing filters and saved-measure refs.

    All ``{var}`` are masked to a neutral literal first, so a reference introduced
    only by a variable value never participates.
    """
    refs, _ = _parsed_filter_refs(filter_str)
    return [
        r for r in refs
        if not _saved_measure_in_every_scope(r, model_scopes=model_scopes)
    ]


def _query_dimension_refs(query: SlayerQuery) -> list[str]:
    """Row-valued refs of the query's dimensions + time dimensions.

    Computed dimensions contribute their row-valued and partition_by refs (never
    refs inside an aggregation); plain and time dimensions contribute their name.
    """
    refs: list[str] = []
    for dim in query.dimensions or []:
        if isinstance(dim, ComputedDimension):
            refs.extend(_computed_dimension_refs(dim.expression))
        else:
            refs.append(dim.full_name)
    for td in query.time_dimensions or []:
        refs.append(td.dimension.full_name)
    return refs


def determination_items(
    query: SlayerQuery,
    *,
    models_by_name: dict[str, SlayerModel],
    model_scopes: list[dict[str, SlayerModel]] | None = None,
) -> list[str]:
    """The dimensions + time dimensions + field-typed filter refs that the population must determine.

    Measures, measure-typed filters, saved-measure refs, order entries, and
    model-level filters contribute nothing. Deduped, preserving first occurrence.
    Runtime variable values never participate — a filter's ``{var}`` is masked to a
    neutral literal before parsing, so an injected value can't introduce a ref.
    ``model_scopes`` widens saved-measure classification beyond ``models_by_name``
    (the anchors-fold scopes), so items and anchors can't disagree.
    """
    scopes = model_scopes if model_scopes is not None else [models_by_name]
    items: list[str] = []
    seen: set[str] = set()

    def _add(ref: str) -> None:
        if ref and ref not in seen:
            seen.add(ref)
            items.append(ref)

    for ref in _query_dimension_refs(query):
        _add(ref)
    for f in query.filters or []:
        for ref in _filter_refs(f, model_scopes=scopes):
            _add(ref)
    return items


# --------------------------------------------------------------------------- #
# Per-candidate determination probe (design §2).
# --------------------------------------------------------------------------- #
def _walk_to_one(
    *, start: SlayerModel, tokens: list[str], models_by_name: dict[str, SlayerModel]
) -> tuple[SlayerModel | None, str, tuple[str, ...]]:
    """Walk ``tokens`` from ``start``; ``(terminal model, verdict, canonical path)``.

    Every oriented hop must be provably to-one; ``_NO_EDGE`` distinguishes a path
    that does not resolve from a resolved-but-unsafe (``_UNREACHABLE``) one.
    """
    try:
        chain = walk(root=start, path=tokens, models_by_name=models_by_name)
    except AmbiguousJoinPathError:
        return (None, _AMBIGUOUS, ())
    except CircularJoinPathError:
        return (None, _UNREACHABLE, ())
    if chain is None:
        return (None, _NO_EDGE, ())
    if not all(provably_to_one(edge=e, target_model=models_by_name[e.target_model]) for e in chain):
        return (None, _UNREACHABLE, ())
    return (models_by_name[chain[-1].target_model] if chain else start, _OK, canonical_path(chain))


def _short_form_probe(
    *, start: SlayerModel, target: str, models_by_name: dict[str, SlayerModel]
) -> tuple[SlayerModel | None, str, tuple[str, ...]]:
    """Routed fallback for a single-token anchor with no literal edge."""
    route, status = resolve_route(
        root=start, target_model=target, models_by_name=models_by_name
    )
    if status == "ambiguous":
        # Two safe routes are genuinely ambiguous; many routes with none
        # safe leave nothing for determination — unreachable, not ambiguous.
        reachable = safe_route_reachable(
            root=start, target_model=target, models_by_name=models_by_name
        )
        return (None, _AMBIGUOUS if reachable else _UNREACHABLE, ())
    if route is None:
        return (None, _UNREACHABLE, ())
    return _walk_to_one(start=start, tokens=route, models_by_name=models_by_name)


def probe_item(*, root: str, item: str, models_by_name: dict[str, SlayerModel]) -> tuple[str, tuple[str, ...]]:
    """Route ``item``'s dotted path from ``root``; ``(verdict, canonical path)`` — hops are its length.

    Literal resolution first: the spelled path is walked hop by hop, and a path
    that resolves literally is never reinterpreted through routing. A single-token
    anchor with no literal edge falls back to short-form routing — the same route
    enumeration binding applies — viable only when every hop of the selected route
    is provably to-one, with hops counted along it. Verdict is ``_OK`` when the
    walk succeeds and the leaf is a column on the terminal model, ``_AMBIGUOUS``
    when a hop spans parallel edges (or the routed probe finds two safe routes),
    else ``_UNREACHABLE``. A leading self-prefix (``root.``) is stripped first.
    """
    parts = item.split(".")
    leaf = parts[-1]
    path = parts[:-1]
    if path and path[0] == root:
        path = path[1:]
    start = models_by_name.get(root)
    if start is None:
        return (_UNREACHABLE, ())
    terminal, verdict, hops = _walk_to_one(start=start, tokens=path, models_by_name=models_by_name)
    if verdict == _NO_EDGE and len(path) == 1:
        terminal, verdict, hops = _short_form_probe(
            start=start, target=path[0], models_by_name=models_by_name
        )
    if terminal is None:
        return (_UNREACHABLE if verdict == _NO_EDGE else verdict, ())
    if terminal.get_column(leaf) is None:
        return (_UNREACHABLE, ())
    return (_OK, hops)


def determined_path(*, root: str, item: str, models_by_name: dict[str, SlayerModel]) -> tuple[str, ...] | None:
    """``item``'s canonical path from ``root`` when ``root`` determines it, else ``None``."""
    verdict, path = probe_item(root=root, item=item, models_by_name=models_by_name)
    return path if verdict == _OK else None


def ambiguity_resolved_path(*, root: str, item: str, models_by_name: dict[str, SlayerModel]) -> tuple[str, ...] | None:
    """For a ``model.leaf`` item whose probe fails only on ambiguity, its tie-break-best fan-out-free canonical path."""
    if probe_item(root=root, item=item, models_by_name=models_by_name)[0] != _AMBIGUOUS:
        return None
    *path, leaf = item.split(".")
    target = models_by_name.get(path[0]) if len(path) == 1 else None
    if target is None or target.get_column(leaf) is None:
        return None
    route = shortest_route(root=models_by_name[root], target=target.name, models_by_name=models_by_name,
                           hop_filter=HopFilter.TO_ONE)
    return route.tokens if route is not None else None


def item_blockers(*, root: str, item: str, models_by_name: dict[str, SlayerModel]) -> list[UnaddressablePair]:
    """The unaddressable pairs that keep ``item``'s path from resolving from ``root``."""
    path = item.split(".")[:-1]
    if path and path[0] == root:
        path = path[1:]
    start = models_by_name.get(root)
    if start is None or not path:
        return []
    try:
        if walk(root=start, path=path, models_by_name=models_by_name) is not None or len(path) > 1:
            return []
    except AmbiguousJoinPathError as e:
        return [p for p in unaddressable_pairs(models_by_name=models_by_name)
                if {p.source_model, p.target_model} == {e.source_model, e.target_model}]
    except CircularJoinPathError:
        return []
    return blocking_pairs(root=start, target=path[0], models_by_name=models_by_name)


class CandidateVerdict(BaseModel):
    """A candidate population's determination outcome over the item set."""

    model_name: str
    viable: bool
    total_hops: int
    ambiguous_blocked: bool
    ambiguous_item: str | None = None
    reachable_items: list[str]
    unreachable_items: list[str]


def evaluate_candidates(
    *, items: list[str], candidates: list[str], models_by_name: dict[str, SlayerModel]
) -> list[CandidateVerdict]:
    """Probe every candidate over every determination item."""
    verdicts: list[CandidateVerdict] = []
    for name in candidates:
        probes = {it: probe_item(root=name, item=it, models_by_name=models_by_name) for it in items}
        statuses = [v for v, _ in probes.values()]
        reachable = [it for it in items if probes[it][0] == _OK]
        unreachable = [it for it in items if probes[it][0] != _OK]
        viable = all(s == _OK for s in statuses)
        ambiguous_blocked = _UNREACHABLE not in statuses and _AMBIGUOUS in statuses
        ambiguous_item = next(
            (it for it in items if probes[it][0] == _AMBIGUOUS), None
        )
        verdicts.append(CandidateVerdict(
            model_name=name,
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
    winners = sorted(v.model_name for v in viable if v.total_hops == minimum)
    if len(winners) == 1:
        return winners[0], []
    return None, winners


# --------------------------------------------------------------------------- #
# Datasource scoping (spec: Datasource scoping for root-less queries).
# --------------------------------------------------------------------------- #
def _saved_measure_in_every_scope(
    ref: str, *, model_scopes: list[dict[str, SlayerModel]]
) -> bool:
    """Whether ``ref`` resolves to a saved measure in every scope holding its anchor model.

    A scope where the anchor model is absent abstains; a real column in any scope
    keeps the reference (column-wins precedence, extended across datasources).
    """
    scopes = [s for s in model_scopes if ref.split(".")[0] in s]
    return bool(scopes) and all(
        _is_saved_measure_ref(ref, models_by_name=s) for s in scopes
    )


def _anchor_names(
    query: SlayerQuery, *, model_scopes: list[dict[str, SlayerModel]] | None = None
) -> set[str]:
    """First segments of the dotted references that drive inference — dimensions,
    time dimensions, and field-typed filters only.

    Measures are excluded (they must never influence the population, datasource
    scoping, or sibling detection); references are read from the parser, so string
    literals never surface as anchors. With ``model_scopes`` (phase 2 of the fold),
    a filter reference resolving to a saved measure is excluded too — the same
    classification :func:`determination_items` applies.
    """
    refs = _query_dimension_refs(query)
    for f in query.filters or []:
        filter_refs, _ = _parsed_filter_refs(f)
        refs.extend(
            r for r in filter_refs
            if not model_scopes
            or not _saved_measure_in_every_scope(r, model_scopes=model_scopes)
        )
    return {ref.split(".")[0] for ref in refs if ref}


async def _classification_scopes(
    *, storage: StorageBackend, data_source: str | None, anchors: set[str]
) -> tuple[dict[str, dict[str, SlayerModel]], dict[str, set[str]]]:
    """``(models-by-name per candidate datasource, datasources per model name)``.

    Candidates are the datasources holding at least one syntactic anchor model —
    or just the explicit ``data_source``, which pins scoping before anchor voting.
    """
    ds_by_model: dict[str, set[str]] = {}
    if data_source is not None:
        candidates = {data_source}
    else:
        for ds, name in await storage._list_all_model_identities():
            ds_by_model.setdefault(name, set()).add(ds)
        candidates = set().union(
            *(ds_by_model[a] for a in anchors if a in ds_by_model)
        )
    scopes: dict[str, dict[str, SlayerModel]] = {}
    for ds in sorted(candidates):
        scopes[ds] = {m.name: m for m in await _all_models_in_datasource(storage, ds)}
    return scopes, ds_by_model


def _resolve_datasource(
    *, ds_by_model: dict[str, set[str]], data_source: str | None, anchors: set[str]
) -> str:
    """The single datasource holding every referenced anchor model, or fail closed."""
    if data_source is not None:
        return data_source
    relevant = [a for a in anchors if a in ds_by_model]
    if not relevant:
        raise PopulationInferenceError(PopulationErrorReason.NO_DATASOURCE)
    candidate = set.intersection(*(ds_by_model[a] for a in relevant))
    if not candidate:
        involved = sorted(set().union(*(ds_by_model[a] for a in relevant)))
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
    """``query``'s P factor: its spine time dimensions and spine filters removed (measures too)."""
    return query.model_copy(update={
        "dimensions": [
            d for d in query.dimensions or []
            if not mentions_spine(getattr(d, "expression", None) or getattr(d, "full_name", ""))
        ] or None,
        "time_dimensions": [
            td for td in query.time_dimensions or [] if not names_spine(td.dimension.full_name)
        ] or None,
        "filters": [f for f in query.filters or [] if not _spine_only_filter(f)] or None,
        "measures": None,
    })


def _spine_only_filter(filter_str: str) -> bool:
    refs, ok = _all_filter_refs(filter_str)
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


def _measure_anchor_names(query: SlayerQuery) -> set[str]:
    names: set[str] = set()
    for m in query.measures or []:
        try:
            parsed = parse_expr(m.formula)
        except Exception:
            continue
        names.update(ref.split(".")[0] for ref in _qualified_refs(parsed))
    return names - {TIME_SPINE_MODEL}


async def infer_spine_population(
    *,
    query: SlayerQuery,
    storage: StorageBackend,
    data_source: str | None = None,
    sibling_stage_names: set[str] | None = None,
) -> PopulationChoice:
    """P for a spine query: inferred from its non-spine items, else the unit (``time_spine``)."""
    factor = spine_factor_out(query)
    if factor.dimensions or factor.time_dimensions or factor.filters:
        return await infer_population(
            query=factor, storage=storage, data_source=data_source,
            sibling_stage_names=sibling_stage_names,
        )
    return PopulationChoice(
        model_name=TIME_SPINE_MODEL,
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
    """The datasource of spine-rooted queries: pinned, else the one holding every model they name."""
    if data_source is not None:
        return data_source
    anchors: set[str] = set()
    for q in queries:
        anchors |= _anchor_names(q) | _measure_anchor_names(q) | {q.source_model_name or TIME_SPINE_MODEL}
    anchors -= {TIME_SPINE_MODEL, *(sibling_stage_names or ())}
    if not anchors:
        datasources = await storage.list_datasources()
        if len(datasources) == 1:
            return datasources[0]
    _scopes, ds_by_model = await _classification_scopes(
        storage=storage, data_source=None, anchors=anchors,
    )
    return _resolve_datasource(ds_by_model=ds_by_model, data_source=None, anchors=anchors)


# --------------------------------------------------------------------------- #
# Entry point.
# --------------------------------------------------------------------------- #
async def infer_population(
    *,
    query: SlayerQuery,
    storage: StorageBackend,
    data_source: str | None = None,
    sibling_stage_names: set[str] | None = None,
) -> PopulationChoice:
    """Infer a rootless query's population, or raise :class:`PopulationInferenceError`."""
    siblings = sibling_stage_names or set()

    # Nothing to infer from (e.g. a measures-only query) — a dedicated error,
    # checked before datasource resolution so it never masquerades as NO_DATASOURCE.
    if not (query.dimensions or query.time_dimensions or query.filters):
        raise PopulationInferenceError(PopulationErrorReason.EMPTY_DETERMINATION)

    # Phase 1 — syntactic anchors only pick the model scopes to classify against;
    # every verdict below re-runs on the model-aware classification.
    syntactic = _anchor_names(query)

    # No dimensions and only aggregate-bearing filters ⇒ nothing to infer from;
    # a dedicated error, not the NO_DATASOURCE that empty anchors would otherwise
    # trigger at datasource resolution.
    if not syntactic:
        raise PopulationInferenceError(PopulationErrorReason.EMPTY_DETERMINATION)

    scopes, ds_by_model = await _classification_scopes(
        storage=storage, data_source=data_source, anchors=syntactic
    )

    # Phase 2 — anchors reclassified saved-measure-excluding, precedence re-run:
    # EMPTY_DETERMINATION, then SIBLING_STAGE, then datasource resolution.
    anchors = _anchor_names(query, model_scopes=list(scopes.values()))
    if not anchors:
        raise PopulationInferenceError(PopulationErrorReason.EMPTY_DETERMINATION)

    anchored_siblings = anchors & set(siblings)
    if anchored_siblings:
        name = min(anchored_siblings)
        raise PopulationInferenceError(
            PopulationErrorReason.SIBLING_STAGE,
            candidates=sorted(anchored_siblings),
            detail=(
                f"dimensions anchor at sibling stage {name!r}; name it as the "
                f"stage's source_model"
            ),
        )

    ds = _resolve_datasource(ds_by_model=ds_by_model, data_source=data_source, anchors=anchors)
    models_by_name = scopes[ds]  # the winning datasource always holds a loaded anchor

    items = determination_items(
        query, models_by_name=models_by_name, model_scopes=list(scopes.values())
    )
    if not items:
        raise PopulationInferenceError(PopulationErrorReason.EMPTY_DETERMINATION)

    verdicts = evaluate_candidates(
        items=items, candidates=sorted(models_by_name), models_by_name=models_by_name
    )
    winner, tied = select_from_verdicts(verdicts)
    if winner is not None:
        return PopulationChoice(model_name=winner, data_source=ds)
    if tied:
        raise PopulationInferenceError(PopulationErrorReason.TIE, candidates=tied)

    pairs = {
        p.describe(): None
        for v in verdicts for it in v.unreachable_items
        for p in item_blockers(root=v.model_name, item=it, models_by_name=models_by_name)
    }
    blocked = [v for v in verdicts if v.ambiguous_blocked]
    if blocked:
        first = blocked[0]
        raise PopulationInferenceError(
            PopulationErrorReason.AMBIGUOUS_PATH,
            candidates=[v.model_name for v in blocked],
            detail="; ".join([
                f"from candidate {first.model_name!r}, the join path to "
                f"{first.ambiguous_item!r} is ambiguous", *pairs,
            ]),
        )
    raise PopulationInferenceError(
        PopulationErrorReason.NO_VIABLE_CANDIDATE, candidates=sorted(models_by_name),
        detail="; ".join(pairs) or None,
    )
