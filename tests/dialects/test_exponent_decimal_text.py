"""Spec: queries/casts › exponent-form numeric text casts to a DECIMAL exactly where the database cannot read it."""

from __future__ import annotations

from decimal import Decimal
from typing import ClassVar

import duckdb
import pytest
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.duckdb import DuckdbDialect
from slayer.sql.dialects.trino import TrinoDialect
from slayer.sql.dialects.tsql import TsqlDialect


class _NoTextExponent(DuckdbDialect):
    """DuckDB, read as Trino and SQL Server read text: no exponent in a cast to DECIMAL."""

    decimal_text_exponent: ClassVar[bool] = False

    def text_repeat(self, value: Expression, count: Expression) -> Expression:
        return exp.Anonymous(this="REPEAT", expressions=[value.copy(), count.copy()])


def _cast(text: str, precision: int, scale: int, *, try_: bool = False) -> Expression:
    dialect = _NoTextExponent()
    return dialect.build_decimal_cast(exp.Literal.string(text), exp.DataType.build(f"DECIMAL({precision}, {scale})"),
                                      precision=precision, scale=scale, source=None, try_=try_)


@pytest.mark.parametrize(("text", "precision", "scale", "value"), [
    ("9007199254740993e0", 20, 0, "9007199254740993"),
    ("12345e-2", 10, 2, "123.45"),
    ("1.2345e1", 10, 3, "12.345"),
    ("1.25e0", 5, 1, "1.3"),
    ("-1.5E3", 10, 0, "-1500"),
    ("+2.5e-3", 10, 4, "0.0025"),
    (" .5e1 ", 5, 0, "5"),
    ("5.e2", 5, 0, "500"),
    ("1e-45", 10, 2, "0.00"),
    ("-0.00012e2", 10, 3, "-0.012"),
    ("12.3456e+1", 10, 2, "123.46"),
    ("12.5", 5, 0, "13"),
])
def test_exponent_text_keeps_every_digit(text: str, precision: int, scale: int, value: str) -> None:
    assert duckdb.sql(f"SELECT {_cast(text, precision, scale).sql(dialect='duckdb')}").fetchone() == (Decimal(value),)


@pytest.mark.parametrize("text", ["abc", "1e", "1ex", "e1", ".e1", "+e1", "1.2.3e1", "+-1e1", "--1e1", "1-2e1"])
def test_try_cast_of_text_that_is_no_number_is_null(text: str) -> None:
    assert duckdb.sql(f"SELECT {_cast(text, 10, 2, try_=True).sql(dialect='duckdb')}").fetchone() == (None,)


class _EmptyIntIsZero(_NoTextExponent):
    """Reads ``''``, ``'+'`` and ``'-'`` as INT 0, as SQL Server does."""

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        if to.this != exp.DataType.Type.INT:
            return super().build_try_cast(value, to)
        empty = exp.In(this=exp.Trim(this=value.copy()), expressions=[exp.Literal.string(s) for s in ("", "+", "-")])
        return exp.Case(ifs=[exp.If(this=empty, true=exp.Literal.number(0))], default=super().build_try_cast(value, to))


@pytest.mark.parametrize("text", ["1e", "1e-", "1e+", "1e 1"])
def test_an_exponent_without_digits_is_null_where_empty_text_reads_as_zero(text: str) -> None:
    cast = _EmptyIntIsZero().build_decimal_cast(exp.Literal.string(text), exp.DataType.build("DECIMAL(10, 2)"),
                                                precision=10, scale=2, source=None, try_=True)
    assert duckdb.sql(f"SELECT {cast.sql(dialect='duckdb')}").fetchone() == (None,)


@pytest.mark.parametrize("text", ["e1", "1.2.3e1", "+-1e1"])
def test_cast_of_an_exponent_without_a_number_fails(text: str) -> None:
    relation = duckdb.sql(f"SELECT {_cast(text, 10, 2).sql(dialect='duckdb')}")
    with pytest.raises(duckdb.ConversionException):
        relation.fetchone()


@pytest.mark.parametrize("dialect", [TrinoDialect(), TsqlDialect()], ids=["trino", "tsql"])
def test_exponent_text_never_reads_through_a_float(dialect: TrinoDialect | TsqlDialect) -> None:
    to = exp.DataType.build("DECIMAL(20, 0)")
    cast = dialect.build_decimal_cast(exp.Literal.string("9007199254740993e0"), to, precision=20, scale=0,
                                      source=None, try_=False)
    assert not any(isinstance(n, exp.DataType) and n.this in exp.DataType.FLOAT_TYPES for n in cast.walk())
