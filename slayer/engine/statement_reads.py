"""The persisted elements a rendered statement reads, and whether a drift entry touches them (system P10 closure)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, Field

from slayer.core.join_edges import JoinEdgeRef
from slayer.core.join_walker import aggregation_owner, follow, resolved_terminal, with_list_children
from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey, StarKey, ValueKey, walk_value_keys
from slayer.core.models import SlayerModel, aggregation_definition
from slayer.engine.column_dependency import definition_reads
from slayer.engine.schema_drift import EditModelDelete, ListRetypeFinding, WholeModelDelete
from slayer.ir.planned import plan_value_keys
from slayer.ir.source_bundle import ResolvedSourceBundle

#: ``(declaring model, target model, edge name, join pairs)``.
JoinRead = tuple[str, str, str | None, frozenset[tuple[str, str]]]


class StatementReads(BaseModel):
    """Per-model ``(model, name)`` reads; ``columns`` is closed over derived definitions and crossed join keys."""

    columns: set[tuple[str, str]] = Field(default_factory=set)
    joins: set[JoinRead] = Field(default_factory=set)
    measures: set[tuple[str, str]] = Field(default_factory=set)
    aggregations: set[tuple[str, str]] = Field(default_factory=set)

    def touched_by(self, entry: Any) -> bool:
        """Whether a drift entry or finding of a read model removes or re-types something the statement reads."""
        if isinstance(entry, WholeModelDelete):
            return entry.cause == "schema_drift"
        if isinstance(entry, ListRetypeFinding):
            return (entry.model_name, entry.column) in self.columns
        if not isinstance(entry, EditModelDelete):
            return False
        model, remove = entry.model_name, entry.remove
        return bool(entry.remove_filters) or any((
            any((model, c) in self.columns for c in remove.columns),
            any((model, m) in self.measures for m in remove.measures),
            any((model, a) in self.aggregations for a in remove.aggregations),
            any(_reads_edge(self.joins, model=model, edge=e) for e in remove.join_edges),
        ))


def _reads_edge(joins: Iterable[JoinRead], *, model: str, edge: JoinEdgeRef) -> bool:
    pairs = frozenset((p[0], p[1]) for p in edge.join_pairs)
    return any(
        declaring == model and target == edge.target_model and read_pairs == pairs
        and (edge.name is None or name == edge.name)
        for declaring, target, name, read_pairs in joins
    )


def statement_reads(
    *, plans: list, bundle: ResolvedSourceBundle, measures: Iterable[tuple[str, str]] = (),
) -> StatementReads:
    """The reads of the emitted ``plans`` (each against its own stage bundle, else ``bundle``)."""
    reads = StatementReads(measures=set(measures))
    for plan in plans:
        own = plan.stage_bundle or bundle
        root = own.source_model
        if root is None:
            continue
        models = with_list_children({**own.models_by_name, root.name: root})
        for root_key in plan_value_keys(plan):
            for key in walk_value_keys(root_key):
                _add_key(reads, key=key, root=root, models=models)
    _close_columns(reads, models=_all_models(plans, bundle=bundle))
    return reads


def _add_key(reads: StatementReads, *, key: ValueKey, root: SlayerModel, models: dict[str, SlayerModel]) -> None:
    if isinstance(key, (ColumnKey, ColumnSqlKey, StarKey)):
        chain = follow(root=root, path=key.path, models_by_name=models) or []
        for edge in chain:
            forward = edge.source_model == edge.declaring_model
            pairs = [(s, t) if forward else (t, s) for s, t in edge.join_pairs]
            target = edge.target_model if forward else edge.source_model
            reads.joins.add((edge.declaring_model, target, edge.name, frozenset(pairs)))
            reads.columns.update((edge.declaring_model, s) for s, _ in pairs)
            reads.columns.update((target, t) for _, t in pairs)
        terminal = resolved_terminal(root=root, path=key.path, models_by_name=models)
        if terminal is not None and not isinstance(key, StarKey):
            reads.columns.add((terminal.name, key.column_name if isinstance(key, ColumnSqlKey) else key.leaf))
    elif isinstance(key, AggregateKey):
        owner = aggregation_owner(root=root, source=key.source, models_by_name=models)
        if owner is not None and aggregation_definition(owner=owner, agg=key.agg) is not None:
            reads.aggregations.add((owner.name, key.agg))


def _all_models(plans: list, *, bundle: ResolvedSourceBundle) -> dict[str, SlayerModel]:
    models = dict(bundle.models_by_name)
    for plan in plans:
        if plan.stage_bundle is not None:
            models.update(plan.stage_bundle.models_by_name)
    return models


def _close_columns(reads: StatementReads, *, models: dict[str, SlayerModel]) -> None:
    """Add every column a read derived column's definition reads, transitively."""
    pending = list(reads.columns)
    while pending:
        model_name, column_name = pending.pop()
        host = models.get(model_name)
        column = host.get_column(column_name) if host is not None else None
        if host is None or column is None:
            continue
        for read in definition_reads(column=column, host=host, reachable=models):
            if read not in reads.columns:
                reads.columns.add(read)
                pending.append(read)
