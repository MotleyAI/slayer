"""Import-time validation: every measure and column resolves among its model's measures, columns and join paths, and
no aggregation reads a list SLayer refuses to aggregate."""

from __future__ import annotations

from collections.abc import Callable, Mapping

import sqlglot
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.syntax import AggCall, DottedRef, Ref, measure_dependency_problems, parse_expr, walk_parsed
from slayer.importers.column_types import list_refusal, refused_over_list
from slayer.sql.column_expansion import reference_sites, resolve_ref_target, root_scope_column_ids


def measure_problems(model: SlayerModel, *, models_by_name: Mapping[str, SlayerModel]) -> list[str | None]:
    """Per measure of ``model``: why it cannot resolve, else ``None``."""
    def join_path(parts: tuple[str, ...]) -> bool:
        return _path_column(parts, model=model, models_by_name=models_by_name) is not None

    found = {i: p for i, m in enumerate(model.measures)
             if (p := _list_problem(m, model=model, models_by_name=models_by_name)) is not None}
    while True:  # a measure reading a dropped one is dropped too
        kept = [(i, m) for i, m in enumerate(model.measures) if i not in found]
        problems = measure_dependency_problems(
            [(m.name, m.formula) for _, m in kept],
            aggregations=frozenset(a.name for a in model.aggregations),
            columns=frozenset(c.name for c in model.columns), join_path=join_path,
        )
        new = {i: p for (i, _), p in zip(kept, problems) if p is not None}
        if not new:
            return [found.get(i) for i in range(len(model.measures))]
        found.update(new)


def _list_problem(measure: ModelMeasure, *, model: SlayerModel,
                  models_by_name: Mapping[str, SlayerModel]) -> str | None:
    """Why ``measure`` aggregates a list column (its own model's or a joined one's) SLayer refuses, else ``None``."""
    try:
        tree = parse_expr(measure.formula)
    except ValueError:
        return None  # reported by the dependency check
    for node in walk_parsed(tree):
        if not (isinstance(node, AggCall) and isinstance(node.source, (Ref, DottedRef)) and refused_over_list(node.agg)):
            continue
        parts = (node.source.name,) if isinstance(node.source, Ref) else node.source.parts
        col = model.get_column(parts[0]) if len(parts) == 1 else _path_column(
            parts, model=model, models_by_name=models_by_name)
        if col is not None and col.type is DataType.ARRAY:
            return list_refusal(measure.name or "", agg=node.agg, column=".".join(parts))
    return None


def unreachable_columns(model: SlayerModel, *, models_by_name: Mapping[str, SlayerModel], dialect: str | None,
                        dropped: frozenset[str] = frozenset(), prepare: Callable[[str], str] = str) -> dict[str, str]:
    """Columns of ``model`` reading a joined column no join reaches, or one of ``dropped`` (``model``'s own)."""
    found: dict[str, str] = {}
    for col in model.columns:
        if (problem := _column_problem(col, model=model, models_by_name=models_by_name, dialect=dialect,
                                       dropped=dropped, prepare=prepare)) is not None:
            found[col.name] = problem
    return found


def _column_problem(col: Column, *, model: SlayerModel, models_by_name: Mapping[str, SlayerModel],
                    dialect: str | None, dropped: frozenset[str], prepare: Callable[[str], str]) -> str | None:
    for parts in (p for sql in (col.sql, col.filter) for p in _root_references(sql, dialect=dialect, prepare=prepare)):
        if parts[-1] in dropped and (len(parts) == 1 or parts[:-1] == (model.name,)):
            return f"it reads the dropped column {parts[-1]!r}"
        if len(parts) > 1 and parts[:-1] != (model.name,) and _path_column(
                parts, model=model, models_by_name=models_by_name) is None:
            return f"it reads {'.'.join(parts)!r}, which no join of {model.name!r} reaches"
    return None


def _root_references(sql: str | None, *, dialect: str | None, prepare: Callable[[str], str]) -> list[tuple[str, ...]]:
    """Each root-scope column reference in ``sql`` as its dotted parts."""
    try:
        tree = sqlglot.parse_one(prepare(sql), read=dialect) if sql is not None else None
    except SqlglotError:
        return []  # reported where the SQL was translated
    if not isinstance(tree, Expression):
        return []
    return [(*quals, leaf) for _, quals, leaf in reference_sites(tree, root_scope_column_ids(parsed=tree))]


def _path_column(parts: tuple[str, ...], *, model: SlayerModel,
                 models_by_name: Mapping[str, SlayerModel]) -> Column | None:
    target = resolve_ref_target(qualifiers=parts[:-1], source_model=model, models_by_name=dict(models_by_name))
    return target.get_column(parts[-1]) if target is not None else None
