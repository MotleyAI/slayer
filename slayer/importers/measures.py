"""Import-time measure validation: every measure resolves among its model's measures, columns and join paths."""

from __future__ import annotations

from collections.abc import Mapping

from slayer.core.models import SlayerModel
from slayer.core.syntax import measure_dependency_problems
from slayer.sql.column_expansion import resolve_ref_target


def measure_problems(model: SlayerModel, *, models_by_name: Mapping[str, SlayerModel]) -> list[str | None]:
    """Per measure of ``model``: why it cannot resolve, else ``None``."""
    def join_path(parts: tuple[str, ...]) -> bool:
        target = resolve_ref_target(qualifiers=parts[:-1], source_model=model, models_by_name=dict(models_by_name))
        return target is not None and target.get_column(parts[-1]) is not None

    return measure_dependency_problems(
        [(m.name, m.formula) for m in model.measures],
        aggregations=frozenset(a.name for a in model.aggregations),
        columns=frozenset(c.name for c in model.columns), join_path=join_path,
    )
