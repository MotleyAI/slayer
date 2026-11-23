"""Model-scoped metrics expanded in place: a bare query name that is no alias reads its metric's expression."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Dict, List, Optional, Tuple

from slayer.core.errors import AggArgumentError, AmbiguousReferenceError, MetricCycleError, UnevaluableEntityError
from slayer.core.field_expression import portable_text
from slayer.core.names import match_name
from slayer.core.query import OrderItem, QueryMetric, SlayerQuery, declared_aliases
from slayer.core.semantic_model import ModelScopedMetric
from slayer.core.syntax import DottedRef, Ref, canonical_metric_text, map_refs, parse_template
from slayer.storage.base import StorageBackend

__all__ = ["ModelScope", "expand_model_scoped", "model_scope"]


class ModelScope:
    """The model-scoped metrics a statement may name bare, across the datasources it may read."""

    def __init__(self, metrics: Iterable[ModelScopedMetric]) -> None:
        self._metrics = list(metrics)
        self._trees: Dict[Tuple[str, str], Any] = {}

    @property
    def names(self) -> List[str]:
        return [m.name for m in self._metrics]

    def find(self, name: str, *, quoted: bool) -> Optional[ModelScopedMetric]:
        """The metric ``name`` reaches (``slayer.core.names.match_name``); one name in several datasources is ambiguous."""
        hits: Dict[str, ModelScopedMetric] = {}
        for ds in dict.fromkeys(m.data_source for m in self._metrics):
            own = [m for m in self._metrics if m.data_source == ds]
            found = match_name(name=name, declared=[m.name for m in own], quoted=quoted)
            if found is not None:
                hits[ds] = next(m for m in own if m.name == found)
        if len(hits) > 1:
            raise AmbiguousReferenceError(name, [f"{ds}.{m.name}" for ds, m in hits.items()],
                                          suggestion="Pin the query's data_source.")
        return next(iter(hits.values()), None)

    def tree(self, metric: ModelScopedMetric, *, chain: Tuple[str, ...] = ()) -> Any:
        """``metric``'s expression parsed, every model-scoped name in it expanded."""
        if metric.load_warning is not None:
            raise UnevaluableEntityError(entity=f"Metric '{metric.name}'", reason=metric.load_warning)
        if metric.name in chain:
            raise MetricCycleError([*chain[chain.index(metric.name):], metric.name])
        key = (metric.data_source, metric.name)
        if key not in self._trees:
            text = portable_text(metric.expression)
            if text is None:
                raise UnevaluableEntityError(entity=f"Metric '{metric.name}'", reason="has no portable SQL expression")
            self._trees[key] = self._expanded(parse_template(text), aliases=(), chain=(*chain, metric.name))
        return self._trees[key]

    def _expanded(self, tree: Any, *, aliases: Sequence[str], chain: Tuple[str, ...]) -> Any:
        def expand(ref: Ref | DottedRef) -> Any:
            if not isinstance(ref, Ref) or (aliases and match_name(name=ref.name, declared=aliases,
                                                                  quoted=ref.quoted) is not None):
                return ref
            metric = self.find(ref.name, quoted=ref.quoted)
            if metric is not None and ref.evaluated:
                raise AggArgumentError(expression=canonical_metric_text(ref),
                                       reason=f"{metric.name!r} is a model-scoped metric")
            return ref if metric is None else self.tree(metric, chain=chain)
        return map_refs(tree, expand)

    def text(self, text: str, *, aliases: Sequence[str]) -> str:
        """``text`` with each bare non-alias model-scoped name read as its expression; an unparseable text is kept."""
        try:
            tree = parse_template(text)
        except ValueError:
            return text
        expanded = self._expanded(tree, aliases=aliases, chain=())
        return text if expanded is tree else canonical_metric_text(expanded)

    def bare(self, text: str, *, aliases: Sequence[str]) -> Optional[ModelScopedMetric]:
        """The metric ``text`` names when it is one bare non-alias model-scoped name."""
        try:
            tree = parse_template(text)
        except ValueError:
            return None
        if not isinstance(tree, Ref) or match_name(name=tree.name, declared=aliases, quoted=tree.quoted) is not None:
            return None
        return self.find(tree.name, quoted=tree.quoted)


async def model_scope(storage: StorageBackend, queries: Sequence[SlayerQuery], *,
                      data_source: Optional[str]) -> ModelScope:
    """The model-scoped metrics of ``data_source``, else of the datasources the queries' populations live in, else of
    every datasource the caller sees."""
    if data_source is not None:
        chosen = [data_source]
    else:
        identities = await storage._list_all_dataset_identities()
        populations = {p for q in queries if (p := q.population_name)}
        chosen = sorted({ds for ds, n in identities if _names(populations, n)}) or sorted({ds for ds, _ in identities})
    metrics: List[ModelScopedMetric] = []
    for ds in chosen:
        metrics.extend(await storage.list_model_scoped_metrics(ds))
    return ModelScope(metrics)


def _names(populations: Iterable[str], name: str) -> bool:
    folded = name.casefold()
    return any(p == name or p.casefold() == folded for p in populations)


def expand_model_scoped(query: SlayerQuery, *, scope: ModelScope) -> SlayerQuery:
    """``query`` with its model-scoped names expanded in metrics, filters and order keys; a bare metric entry keys by
    the metric's declared name."""
    if not scope.names:
        return query
    aliases = declared_aliases(query)
    metrics = [_metric_entry(m, scope=scope, aliases=aliases) for m in query.metrics or []]
    aliases = declared_aliases(query.model_copy(update={"metrics": metrics or None}))
    update: Dict[str, Any] = {}
    if metrics != list(query.metrics or []):
        update["metrics"] = metrics
    conditions = query.filter_conditions
    filters = [scope.text(c, aliases=aliases) for c in conditions]
    if filters != conditions:
        update["filter"] = filters
    order = [_order_item(o, scope=scope, aliases=aliases) for o in query.order or []]
    if any(a is not b for a, b in zip(order, query.order or [])):
        update["order"] = order
    return query.model_copy(update=update) if update else query


def _metric_entry(m: QueryMetric, *, scope: ModelScope, aliases: Sequence[str]) -> QueryMetric:
    """``m`` expanded; a bare model-scoped metric keys by its declared name."""
    own = [a for a in aliases if a != m.alias] if m.alias else aliases
    named = None if m.alias else scope.bare(m.metric, aliases=own)
    expanded = scope.text(m.metric, aliases=own)
    if expanded == m.metric:
        return m
    return m.model_copy(update={"metric": expanded, **({"alias": named.name} if named else {})})


def _order_item(item: OrderItem, *, scope: ModelScope, aliases: Sequence[str]) -> OrderItem:
    expanded = scope.text(item.field, aliases=aliases)
    if expanded == item.field:
        return item
    return OrderItem.model_validate({"field": expanded, "direction": item.direction, "nulls": item.nulls,
                                     "implied": item.implied})
