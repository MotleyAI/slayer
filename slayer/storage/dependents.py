"""Stored dependents an explicit change would break: refused, or warned about under a database-driven change."""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import Generator, Mapping, Set as AbstractSet
from contextvars import ContextVar

from slayer.core.datasource_scope import DatasourceScope
from slayer.core.deprecation import outside_stacklevel
from slayer.core.errors import BrokenDependent, StoredDependentBreakError
from slayer.core.semantic_model import ModelScopedMetric
from slayer.storage.model_metric_rules import (
    Unresolved, chain_warning, document_names, metric_references, unresolved_references,
)
from slayer.storage.semantic_models import metric_graph

_DATABASE_DRIVEN: ContextVar[bool] = ContextVar("_DATABASE_DRIVEN", default=False)

#: Unresolved references a metric reaches, by reference: the metric spellings leading to one holding it, and
#: ``(spelling, why)``.
Reach = dict[str, tuple[list[str], tuple[str, str]]]


class StoredDependentUnresolvedWarning(UserWarning):
    """A database-driven change left stored mappings or model-scoped metrics unresolved."""


@contextlib.contextmanager
def database_driven() -> Generator[None]:
    """Changes made inside follow the database: they apply, warning about each stored dependent they break."""
    token = _DATABASE_DRIVEN.set(True)
    try:
        yield
    finally:
        _DATABASE_DRIVEN.reset(token)


def report_broken(*, change: str, broken: list[BrokenDependent], own: list[str] | None = None) -> None:
    """Refuse ``change`` for breaking ``broken`` (listing the changed document's ``own`` problems too); a
    database-driven change applies and warns instead."""
    if not broken:
        return
    error = StoredDependentBreakError(change=change, broken=broken, own=own or [])
    if not _DATABASE_DRIVEN.get():
        raise error
    names = ", ".join(b.label for b in broken)
    warnings.warn(f"{change} leaves {names} unresolved:\n" + "\n".join(f"  - {r}" for r in error.reasons),
                  StoredDependentUnresolvedWarning, stacklevel=outside_stacklevel())


def _names(scope: DatasourceScope) -> set[str]:
    return {m.name.casefold() for metrics in scope.metrics.values() for m in metrics}


def _direct(scope: DatasourceScope, *, gone: AbstractSet[str] = frozenset()) -> dict[str, Unresolved]:
    """Each stored model-scoped metric's unresolved references in ``scope`` (``gone``: the metrics a change removes),
    by case-folded name."""
    names = _names(scope)
    out: dict[str, Unresolved] = {}
    for document, metrics in scope.metrics.items():
        fields, dataset_metrics = document_names(scope.documents.get(document, []))
        for metric in metrics:
            out[metric.name.casefold()] = unresolved_references(
                metric.expression, fields=fields, dataset_metrics=dataset_metrics, metric_names=names,
                gone_metrics=gone, document=document, data_source=scope.data_source, bare_metrics=metric.slayer_held)
    return out


def _reads(name: str, *, graph: dict[str, tuple[set[str], set[str]]]) -> list[str]:
    """The datasets and metrics ``name`` reads, through other metrics too, case-folded."""
    seen: set[str] = set()
    datasets: set[str] = set()
    frontier = [name]
    while frontier:
        current = frontier.pop()
        if current in seen or current not in graph:
            continue
        seen.add(current)
        read_datasets, read_metrics = graph[current]
        datasets |= read_datasets
        frontier += sorted(read_metrics)
    return sorted(datasets | (seen - {name}))


def _reached(name: str, *, direct: Mapping[str, Unresolved], graph: Mapping[str, tuple[str, set[str]]]) -> Reach:
    """Every unresolved reference metric ``name`` reaches, itself or through the metrics it reads (``graph``: folded
    name → spelling, reads), each by its shortest chain; cycle-safe."""
    out: Reach = {}
    seen: set[str] = set()
    frontier: list[tuple[str, list[str]]] = [(name, [])]
    while frontier:
        nxt: list[tuple[str, list[str]]] = []
        for current, chain in frontier:
            if current in seen:
                continue
            seen.add(current)
            for key, found in sorted(direct.get(current, {}).items()):
                out.setdefault(key, (chain, found))
            nxt += [(n, [*chain, graph[n][0]]) for n in sorted(graph.get(current, ("", set()))[1]) if n in graph]
        frontier = nxt
    return out


def _metrics(scope: DatasourceScope) -> list[ModelScopedMetric]:
    return [m for metrics in scope.metrics.values() for m in metrics]


def broken_model_metrics(*, before: DatasourceScope, after: DatasourceScope) -> list[BrokenDependent]:
    """The stored model-scoped metrics reaching, in ``after``, an unresolved reference they do not reach in ``before``,
    directly or through other metrics; a metric ``after`` adds is not judged."""
    held = _names(before)
    if not held:
        return []
    was_direct, was_graph = _direct(before), metric_graph(_metrics(before))
    now_direct, now_graph = _direct(after, gone=held - _names(after)), metric_graph(_metrics(after))
    references = {m.name.casefold(): metric_references(m.expression) for m in _metrics(after)}
    out = []
    for document, metrics in after.metrics.items():
        for metric in metrics:
            folded = metric.name.casefold()
            if folded not in held:
                continue
            was = _reached(folded, direct=was_direct, graph=was_graph)
            new = [(key, hit) for key, hit in _reached(folded, direct=now_direct, graph=now_graph).items()
                   if key not in was]
            problems = [chain_warning([*chain, spelling], why) for _, (chain, (spelling, why)) in
                        sorted(new, key=lambda item: (len(item[1][0]), item[0]))]
            if problems:
                out.append(BrokenDependent(kind="metric", data_source=after.data_source, name=metric.name,
                                           semantic_model=document, problems=list(dict.fromkeys(problems)),
                                           reads=_reads(folded, graph=references)))
    return out
