"""Stored dependents an explicit change would break: refused, or warned about under a database-driven change."""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import Generator, Set as AbstractSet
from contextvars import ContextVar

from slayer.core.datasource_scope import DatasourceScope
from slayer.core.deprecation import outside_stacklevel
from slayer.core.errors import BrokenDependent, StoredDependentBreakError
from slayer.storage.model_metric_rules import (
    Unresolved, document_names, metric_references, unresolved_metrics, unresolved_references,
)
from slayer.storage.semantic_models import metric_graph

_DATABASE_DRIVEN: ContextVar[bool] = ContextVar("_DATABASE_DRIVEN", default=False)


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
                gone_metrics=gone, document=document, data_source=scope.data_source)
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


def broken_model_metrics(*, before: DatasourceScope, after: DatasourceScope) -> list[BrokenDependent]:
    """The stored model-scoped metrics with an unresolved reference in ``after`` they lack in ``before``, and those
    reading such a metric; a metric ``after`` adds is not judged."""
    held = _names(before)
    if not held:
        return []
    was = _direct(before)
    new = {k: {r: v for r, v in found.items() if r not in was.get(k, {})}
           for k, found in _direct(after, gone=held - _names(after)).items()}
    kept = [(document, m) for document, metrics in after.metrics.items() for m in metrics if m.name.casefold() in held]
    every = [m for metrics in after.metrics.values() for m in metrics]
    failed = unresolved_metrics({k: v for k, v in new.items() if k in held}, graph=metric_graph(every))
    references = {m.name.casefold(): metric_references(m.expression) for m in every}
    return [BrokenDependent(kind="metric", data_source=after.data_source, name=m.name, semantic_model=document,
                            problems=failed[m.name.casefold()], reads=_reads(m.name.casefold(), graph=references))
            for document, m in kept if m.name.casefold() in failed]
