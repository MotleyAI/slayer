"""Import-time validation: every metric and field resolves among its dataset's metrics, fields and relationship paths, and
no aggregation reads a list SLayer refuses to aggregate."""

from __future__ import annotations

from collections.abc import Callable, Mapping

import sqlglot
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.syntax import AggCall, DottedRef, Ref, metric_dependency_problems, parse_expr, walk_parsed
from slayer.importers.aggregates import expression_refusal
from slayer.importers.column_types import list_refusal, refused_over_list
from slayer.sql.field_expansion import reference_sites, resolve_ref_target, root_scope_field_ids
from slayer.sql.dialects import SqlDialect


def metric_problems(dataset: SlayerDataset, *, datasets_by_name: Mapping[str, SlayerDataset],
                     dialect: SqlDialect | None = None) -> list[str | None]:
    """Per metric of ``dataset``: why it cannot resolve (or ``dialect``, the target's, cannot compute it), else
    ``None``."""
    def relationship_path(parts: tuple[str, ...]) -> bool:
        return _path_field(parts, dataset=dataset, datasets_by_name=datasets_by_name) is not None

    found = {i: p for i, m in enumerate(dataset.metrics)
             if (p := _list_problem(m, dataset=dataset, datasets_by_name=datasets_by_name)
                 or expression_refusal(m.expression, dialect=dialect)) is not None}
    while True:  # a metric reading a dropped one is dropped too
        kept = [(i, m) for i, m in enumerate(dataset.metrics) if i not in found]
        problems = metric_dependency_problems(
            [(m.name, m.expression) for _, m in kept],
            aggregations=frozenset(a.name for a in dataset.aggregations),
            fields=frozenset(c.name for c in dataset.fields), relationship_path=relationship_path,
        )
        new = {i: p for (i, _), p in zip(kept, problems) if p is not None}
        if not new:
            return [found.get(i) for i in range(len(dataset.metrics))]
        found.update(new)


def _list_problem(metric: DatasetMetric, *, dataset: SlayerDataset,
                  datasets_by_name: Mapping[str, SlayerDataset]) -> str | None:
    """Why ``metric`` aggregates a list field (its own dataset's or a joined one's) SLayer refuses, else ``None``."""
    try:
        tree = parse_expr(metric.expression)
    except ValueError:
        return None  # reported by the dependency check
    for node in walk_parsed(tree):
        if not (isinstance(node, AggCall) and isinstance(node.source, (Ref, DottedRef)) and refused_over_list(node.agg)):
            continue
        parts = (node.source.name,) if isinstance(node.source, Ref) else node.source.parts
        col = dataset.get_field(parts[0]) if len(parts) == 1 else _path_field(
            parts, dataset=dataset, datasets_by_name=datasets_by_name)
        if col is not None and col.datatype is DataType.ARRAY:
            return list_refusal(metric.name or "", agg=node.agg, field=".".join(parts))
    return None


def unreachable_fields(dataset: SlayerDataset, *, datasets_by_name: Mapping[str, SlayerDataset], dialect: str | None,
                        dropped: frozenset[str] = frozenset(), prepare: Callable[[str], str] = str) -> dict[str, str]:
    """Fields of ``dataset`` reading a joined field no relationship reaches, or one of ``dropped`` (``dataset``'s own)."""
    found: dict[str, str] = {}
    for col in dataset.fields:
        if (problem := _field_problem(col, dataset=dataset, datasets_by_name=datasets_by_name, dialect=dialect,
                                       dropped=dropped, prepare=prepare)) is not None:
            found[col.name] = problem
    return found


def _field_problem(col: DatasetField, *, dataset: SlayerDataset, datasets_by_name: Mapping[str, SlayerDataset],
                    dialect: str | None, dropped: frozenset[str], prepare: Callable[[str], str]) -> str | None:
    for parts in (p for sql in (col.sql, col.filter) for p in _root_references(sql, dialect=dialect, prepare=prepare)):
        if parts[-1] in dropped and (len(parts) == 1 or parts[:-1] == (dataset.name,)):
            return f"it reads the dropped field {parts[-1]!r}"
        if len(parts) > 1 and parts[:-1] != (dataset.name,) and _path_field(
                parts, dataset=dataset, datasets_by_name=datasets_by_name) is None:
            return f"it reads {'.'.join(parts)!r}, which no relationship of {dataset.name!r} reaches"
    return None


def _root_references(sql: str | None, *, dialect: str | None, prepare: Callable[[str], str]) -> list[tuple[str, ...]]:
    """Each root-scope field reference in ``sql`` as its dotted parts."""
    try:
        tree = sqlglot.parse_one(prepare(sql), read=dialect) if sql is not None else None
    except SqlglotError:
        return []  # reported where the SQL was translated
    if not isinstance(tree, Expression):
        return []
    return [(*quals, leaf) for _, quals, leaf in reference_sites(tree, root_scope_field_ids(parsed=tree))]


def _path_field(parts: tuple[str, ...], *, dataset: SlayerDataset,
                 datasets_by_name: Mapping[str, SlayerDataset]) -> DatasetField | None:
    target = resolve_ref_target(qualifiers=parts[:-1], source_dataset=dataset, datasets_by_name=dict(datasets_by_name))
    return target.get_field(parts[-1]) if target is not None else None
