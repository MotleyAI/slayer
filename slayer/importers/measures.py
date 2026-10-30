"""Import-time validation: every measure and column resolves among its model's measures, columns and join paths."""

from __future__ import annotations

from collections.abc import Callable, Mapping

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.models import Column, SlayerModel
from slayer.core.syntax import measure_dependency_problems
from slayer.sql.column_expansion import resolve_ref_target, root_scope_column_ids


def measure_problems(model: SlayerModel, *, models_by_name: Mapping[str, SlayerModel]) -> list[str | None]:
    """Per measure of ``model``: why it cannot resolve, else ``None``."""
    def join_path(parts: tuple[str, ...]) -> bool:
        return _path_column(parts, model=model, models_by_name=models_by_name) is not None

    return measure_dependency_problems(
        [(m.name, m.formula) for m in model.measures],
        aggregations=frozenset(a.name for a in model.aggregations),
        columns=frozenset(c.name for c in model.columns), join_path=join_path,
    )


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
    for sql in (col.sql, col.filter):
        try:
            tree = sqlglot.parse_one(prepare(sql), read=dialect) if sql is not None else None
        except SqlglotError:
            continue  # reported where the SQL was translated
        if not isinstance(tree, Expression):
            continue
        root = root_scope_column_ids(parsed=tree)
        for node in tree.find_all(exp.Column):
            parts = tuple(p.name for p in node.parts)
            if id(node) not in root:
                continue
            if parts[-1] in dropped and (len(parts) == 1 or parts[:-1] == (model.name,)):
                return f"it reads the dropped column {parts[-1]!r}"
            if len(parts) > 1 and parts[0] != model.name and _path_column(
                    parts, model=model, models_by_name=models_by_name) is None:
                return f"it reads {'.'.join(parts)!r}, which no join of {model.name!r} reaches"
    return None


def _path_column(parts: tuple[str, ...], *, model: SlayerModel,
                 models_by_name: Mapping[str, SlayerModel]) -> Column | None:
    target = resolve_ref_target(qualifiers=parts[:-1], source_model=model, models_by_name=dict(models_by_name))
    return target.get_column(parts[-1]) if target is not None else None
