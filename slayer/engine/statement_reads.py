"""The persisted elements a rendered statement reads (engine P10 closure)."""

from __future__ import annotations

from collections.abc import Iterable

from pydantic import BaseModel, Field

from slayer.core.errors import AmbiguousRelationshipPathError, CircularRelationshipPathError
from slayer.core.relationship_walker import aggregation_owner, follow, resolved_terminal, with_list_children
from slayer.core.keys import AggregateKey, FieldKey, FieldSqlKey, StarKey, ValueKey, walk_value_keys
from slayer.core.models import SlayerDataset, aggregation_definition
from slayer.engine.field_dependency import definition_reads
from slayer.ir.planned import PlannedQuery, plan_value_keys_by_root
from slayer.ir.source_bundle import ResolvedSourceBundle

#: ``(declaring dataset, target dataset, edge name, join pairs)``.
RelationshipRead = tuple[str, str, str | None, frozenset[tuple[str, str]]]


class StatementReads(BaseModel):
    """Per-dataset ``(dataset, name)`` reads; ``fields`` is closed over derived definitions and crossed relationship keys."""

    columns: set[tuple[str, str]] = Field(default_factory=set)
    relationships: set[RelationshipRead] = Field(default_factory=set)
    metrics: set[tuple[str, str]] = Field(default_factory=set)
    aggregations: set[tuple[str, str]] = Field(default_factory=set)


def statement_reads(
    *, plans: list[PlannedQuery], bundle: ResolvedSourceBundle, metrics: Iterable[tuple[str, str]] = (),
) -> StatementReads:
    """The reads of the emitted ``plans`` (each against its own stage bundle, else ``bundle``)."""
    reads = StatementReads(metrics=set(metrics))
    for plan in plans:
        own = plan.stage_bundle or bundle
        root = own.population
        if root is None:
            continue
        models = with_list_children({**own.datasets_by_name, root.name: root})
        # Closed in the plan's own universe: a stored dataset and another stage's inline namesake never mix.
        local = StatementReads()
        for root_name, root_key in plan_value_keys_by_root(plan):
            key_root = models.get(root_name, root) if root_name else root
            for key in walk_value_keys(root_key):
                _add_key(local, key=key, root=key_root, datasets=models)
        _close_fields(local, datasets=models)
        reads.columns |= local.columns
        reads.relationships |= local.relationships
        reads.aggregations |= local.aggregations
    return reads


def _add_key(reads: StatementReads, *, key: ValueKey, root: SlayerDataset, datasets: dict[str, SlayerDataset]) -> None:
    if isinstance(key, (FieldKey, FieldSqlKey, StarKey)):
        _add_path(reads, path=key.path, root=root, datasets=datasets)
        terminal = resolved_terminal(root=root, path=key.path, datasets_by_name=datasets)
        if terminal is not None and not isinstance(key, StarKey):
            reads.columns.add((terminal.name, key.field_name if isinstance(key, FieldSqlKey) else key.leaf))
    elif isinstance(key, AggregateKey):
        owner = aggregation_owner(root=root, source=key.source, datasets_by_name=datasets)
        definition = aggregation_definition(owner=owner, agg=key.agg) if owner is not None else None
        if owner is not None and definition is not None:
            reads.aggregations.add((owner.name, definition.name))


def _add_path(reads: StatementReads, *, path: tuple[str, ...], root: SlayerDataset, datasets: dict[str, SlayerDataset]) -> None:
    """Every relationship ``path`` crosses, and its key fields on both sides."""
    try:
        chain = follow(root=root, path=path, datasets_by_name=datasets) or []
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        chain = []
    for edge in chain:
        forward = edge.source_dataset == edge.declaring_dataset
        pairs = [(s, t) if forward else (t, s) for s, t in edge.join_pairs]
        target = edge.to if forward else edge.source_dataset
        reads.relationships.add((edge.declaring_dataset, target, edge.name, frozenset(pairs)))
        reads.columns.update((edge.declaring_dataset, s) for s, _ in pairs)
        reads.columns.update((target, t) for _, t in pairs)


def _close_fields(reads: StatementReads, *, datasets: dict[str, SlayerDataset]) -> None:
    """Add every field a read derived field's definition reads, transitively."""
    pending = list(reads.columns)
    while pending:
        model_name, column_name = pending.pop()
        host = datasets.get(model_name)
        column = host.get_field(column_name, quoted=True) if host is not None else None
        if host is None or column is None:
            continue
        for read in definition_reads(field=column, host=host, reachable=datasets):
            if read not in reads.columns:
                reads.columns.add(read)
                pending.append(read)
