"""The Ossie document dialect: the neutral builders, with date addition as Ossie SQL ``DATEADD``."""

from __future__ import annotations

from typing import ClassVar

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, SUB_DAY_GRANULARITIES, TimeGranularity
from slayer.sql.dialects.base import SqlDialect


class OssieDialect(SqlDialect):
    """Writes ``OSSIE_SQL_2026`` metric expressions."""
    enforces_unique_keys: ClassVar[bool] = False

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``DATEADD(unit, count, expr)``; a DATE stays a DATE for day-or-coarser units."""
        word = "WEEK" if unit is TimeGranularity.WEEK_SUNDAY else unit.value.upper()
        moved = exp.DateAdd(this=expr.copy(), expression=count.copy(), unit=exp.var(word))
        if operand is DataType.DATE and unit not in SUB_DAY_GRANULARITIES:
            return exp.Cast(this=moved, to=exp.DataType.build("DATE"))
        return moved
