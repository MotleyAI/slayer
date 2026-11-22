"""The Ossie document dialect writes date addition as ``DATEADD``, never ``INTERVAL`` arithmetic."""

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.ossie import OssieDialect

_PORTABLE = "snowflake"


def _render(node: exp.Expr) -> str:
    return node.sql(dialect=_PORTABLE)


def test_is_a_sql_dialect() -> None:
    assert isinstance(OssieDialect(), SqlDialect)


@pytest.mark.parametrize("operand", [DataType.DATE, DataType.DATETIME])
def test_sunday_week_uses_dateadd(operand: DataType) -> None:
    text = _render(OssieDialect().build_date_trunc(
        col_expr=exp.column("x"), granularity=TimeGranularity.WEEK_SUNDAY, operand=operand))
    tree = sqlglot.parse_one(text, read=_PORTABLE)
    assert tree.find(exp.Interval) is None and "INTERVAL" not in text.upper(), text
    adds = list(tree.find_all(exp.DateAdd))
    assert len(adds) == 2 and all(a.text("unit").upper() == "DAY" for a in adds), text


@pytest.mark.parametrize(("unit", "part"), [
    (TimeGranularity.DAY, "DAY"), (TimeGranularity.MONTH, "MONTH"), (TimeGranularity.HOUR, "HOUR"),
])
def test_date_add_writes_dateadd(unit: TimeGranularity, part: str) -> None:
    text = _render(OssieDialect().build_date_add(
        expr=exp.column("x"), count=exp.Literal.number(3), unit=unit, operand=DataType.DATETIME))
    assert f"DATEADD({part}, 3, X)" in text.upper(), text
    assert "INTERVAL" not in text.upper(), text


def test_date_add_keeps_a_date_a_date() -> None:
    text = _render(OssieDialect().build_date_add(
        expr=exp.column("x"), count=exp.Literal.number(1), unit=TimeGranularity.DAY, operand=DataType.DATE))
    tree = sqlglot.parse_one(text, read=_PORTABLE)
    assert isinstance(tree, exp.Cast) and tree.to.this == exp.DataType.Type.DATE, text
