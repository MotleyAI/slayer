"""The one anchor resolver: a query reference's first segment names its anchor, which the reading dataset reaches by
its only to-one path, else its only route (``queries/references``). Pure over a datasets-by-name map."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Optional, Tuple

from pydantic import BaseModel, ConfigDict, InstanceOf

from slayer.core.errors import (
    SlayerError,
    UnknownMetricError,
    UnknownReferenceError,
    UnqualifiedRowCountError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.models import SlayerDataset
from slayer.core.names import match_name, spelled_name
from slayer.core.relationship_search import HopFilter, blocking_pairs, routes
from slayer.core.relationship_walker import neighbors, walk_terminal
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine
from slayer.engine.population.dimension_routing import resolve_route

__all__ = [
    "Anchor",
    "AnchoredSpelling",
    "anchored_spelling",
    "anchor_route",
    "edge_first_refusal",
    "explicit_spellings",
    "find_anchor",
    "unknown_dataset",
    "unknown_metric",
    "unqualified_count",
]

#: How many explicit spellings an ambiguity names.
_SPELLINGS_CAP = 4


class Anchor(BaseModel):
    """A reference's anchor and the canonical hop tokens from the reading dataset to it (empty when it is the reader).
    An ``unrelated`` anchor is reached by its own name, the product hop."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    # Held as given: a stage's or a query-local frame's dataset is built without validation.
    dataset: InstanceOf[SlayerDataset]
    route: Tuple[str, ...] = ()
    unrelated: bool = False


def find_anchor(
    name: str, *, quoted: bool, reader: Optional[SlayerDataset], datasets_by_name: Mapping[str, SlayerDataset],
) -> Optional[SlayerDataset]:
    """The dataset ``name`` anchors at: the reader by its spelling, a query stage by its spelling (which precedes a
    same-named dataset), a dataset by name, or the time spine; identifier-case rule throughout."""
    if reader is not None and _matches(name, [reader.spelling], quoted=quoted):
        return reader
    stages = {d.explicit_spelling: d for d in datasets_by_name.values() if d.explicit_spelling is not None}
    hit = _match(name, stages, quoted=quoted)
    if hit is not None:
        return stages[hit]
    plain = {n: d for n, d in datasets_by_name.items() if d.explicit_spelling is None}
    hit = _match(name, plain, quoted=quoted)
    if hit is not None:
        return plain[hit]
    if reader is not None and _matches(name, [reader.name], quoted=quoted):
        return reader
    return None


def _match(name: str, declared: Iterable[str], *, quoted: bool) -> Optional[str]:
    return match_name(name=name, declared=declared, quoted=quoted)


def _matches(name: str, declared: Sequence[str], *, quoted: bool) -> bool:
    return _match(name, declared, quoted=quoted) is not None


def anchor_route(
    *, reader: SlayerDataset, anchor: SlayerDataset, datasets_by_name: Mapping[str, SlayerDataset],
    rest: Sequence[str] = (),
) -> Anchor:
    """How ``reader`` reaches ``anchor``: its only to-one path, else its only route; with no route it is unrelated.
    Two or more to-one paths, or none and several routes, raise the ambiguity naming each explicit spelling (``rest``
    is the reference after its anchor)."""
    if anchor.name == reader.name:
        return Anchor(dataset=anchor)
    if is_spine(anchor) or anchor.name == TIME_SPINE_DATASET:
        return Anchor(dataset=anchor, route=(anchor.name,))
    universe = {**datasets_by_name, reader.name: reader}
    route, status = resolve_route(root=reader, target_dataset=anchor.name, datasets_by_name=universe)
    if status == "ok":
        assert route is not None
        return Anchor(dataset=anchor, route=tuple(route))
    if status == "unreachable":
        return Anchor(dataset=anchor, route=(anchor.spelling,), unrelated=True)
    spellings = explicit_spellings(reader=reader, anchor=anchor, datasets_by_name=universe, rest=rest)
    remedy = (f"spell the path: {', '.join(repr(s) for s in spellings)}." if spellings else "; ".join(
        p.describe() for p in blocking_pairs(root=reader, target=anchor.name, datasets_by_name=universe)) + ".")
    raise UnresolvableDimensionRelationshipError(
        reference=".".join((anchor.spelling, *rest)),
        root_dataset=reader.spelling,
        reason=f"{anchor.spelling!r} is reached by more than one path from {reader.spelling!r}; {remedy}",
        suggested_path=spellings[0] if spellings else None,
    )


def explicit_spellings(
    *, reader: SlayerDataset, anchor: SlayerDataset, datasets_by_name: Mapping[str, SlayerDataset],
    rest: Sequence[str] = (),
) -> list[str]:
    """Each executable explicit spelling of a path ``reader → anchor``: its to-one paths when it has several, else
    its routes; best first."""
    universe = dict(datasets_by_name)
    found = routes(root=reader, target=anchor.name, datasets_by_name=universe, hop_filter=HopFilter.TO_ONE,
                   cap=_SPELLINGS_CAP)
    if len(found) < 2:
        found = routes(root=reader, target=anchor.name, datasets_by_name=universe, cap=_SPELLINGS_CAP)
    return [".".join((spelled_name(reader.spelling), *r.tokens, *rest)) for r in found]


def edge_first_refusal(
    parts: Sequence[str], *, datasets_by_name: Mapping[str, SlayerDataset],
) -> Optional[UnknownReferenceError]:
    """The refusal of a reference whose first segment is an edge name, suggesting ``<dataset>.<edge>.<rest>``."""
    token = parts[0]
    owners: list[str] = []
    for name in sorted(datasets_by_name):
        dataset = datasets_by_name[name]
        if any(e.name == token for e in neighbors(dataset=dataset, datasets_by_name=dict(datasets_by_name))):
            owners.append(dataset.spelling)
    if not owners:
        return None
    spellings = [".".join((spelled_name(o), *parts)) for o in owners]
    return UnknownReferenceError(
        name=".".join(parts), scope_kind="DatasetScope",
        scope_summary=f"{token!r} is a relationship name, not a dataset; a reference starts at a dataset",
        suggestion=f"Write {' or '.join(repr(s) for s in spellings)}.",
    )


def unknown_dataset(parts: Sequence[str]) -> UnknownReferenceError:
    """A first segment that names no dataset the caller sees."""
    return UnknownReferenceError(
        name=".".join(parts), scope_kind="DatasetScope",
        scope_summary=f"no dataset {parts[0]!r}", suggestion=None,
    )


def unknown_metric(name: str, *, datasets: Iterable[SlayerDataset]) -> UnknownMetricError:
    """A bare name that is no alias and no metric, suggesting each qualified field and metric of that name."""
    folded = name.casefold()
    hits: list[str] = []
    for dataset in sorted(datasets, key=lambda d: d.spelling):
        owned = [f.name for f in dataset.fields] + [m.name for m in dataset.metrics if m.name]
        hits.extend(f"{spelled_name(dataset.spelling)}.{spelled_name(n)}" for n in owned if n.casefold() == folded)
    hits = list(dict.fromkeys(hits))
    return UnknownMetricError(
        name=name, scope_kind="query",
        scope_summary="a bare name is a metric: an alias of the query",
        suggestion=(f"Qualify it by its dataset: {', '.join(repr(h) for h in hits)}." if hits
                    else "Qualify a field or metric by its dataset, as '<dataset>.<name>'."),
    )


def unqualified_count(dataset: Optional[str] = None) -> UnqualifiedRowCountError:
    """``count(*)`` with no population, suggesting ``count(<dataset>.*)`` for ``dataset`` when known."""
    return UnqualifiedRowCountError(suggestion=(
        f"Write count({spelled_name(dataset)}.*), or name the query's population." if dataset else None
    ))


class AnchoredSpelling(BaseModel):
    """A reference's identity up to its leaf: its anchor and canonical route (``parts``), and the canonical path from
    the reading dataset to the same place (``reach``)."""

    model_config = ConfigDict(frozen=True)

    parts: Tuple[str, ...]
    reach: Tuple[str, ...]


def anchored_spelling(
    parts: Sequence[str], *, quoted: Sequence[bool], reader: SlayerDataset,
    datasets_by_name: Mapping[str, SlayerDataset],
) -> Optional[AnchoredSpelling]:
    """The identity of the reference ``parts`` (leaf excluded) read from ``reader``; ``None`` when it does not resolve."""
    try:
        anchor = find_anchor(parts[0], quoted=bool(quoted) and quoted[0], reader=reader,
                             datasets_by_name=datasets_by_name)
        if anchor is None:
            return None
        reached = anchor_route(reader=reader, anchor=anchor, datasets_by_name=datasets_by_name, rest=parts[1:])
        walked = walk_terminal(root=anchor, path=list(parts[1:-1]),
                               datasets_by_name={**datasets_by_name, reader.name: reader},
                               quoted=list(quoted[1:-1]))
    except SlayerError:
        return None
    if walked is None:
        return None
    return AnchoredSpelling(parts=(anchor.spelling, *walked[1]), reach=(*reached.route, *walked[1]))
