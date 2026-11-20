"""A field's native SQL parses as exactly one expression (spec: models/column-definitions › Column.sql is one expression)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField


def _native(sql: str) -> ExpressionObject:
    return ExpressionObject(dialects=[DialectEntry(dialect="POSTGRES", expression=sql)])


@pytest.mark.parametrize("sql", [
    "SELECT pw FROM secrets",
    "WITH x AS (SELECT 1) SELECT * FROM x",
    "amount; DROP TABLE t",
    "DELETE FROM t",
    "UPDATE t SET amount = 0",
    "DROP TABLE t",
    "amount; amount",
])
def test_a_statement_is_rejected_naming_the_column(sql: str) -> None:
    with pytest.raises(ValidationError, match="leaky"):
        DatasetField(name="leaky", expression=_native(sql), datatype=DataType.FLOAT)


@pytest.mark.parametrize("sql", [
    "amount * 2",
    "CASE WHEN status = 'SELECT' THEN 1 END",
    "amount -- SELECT",
    "(SELECT max(x) FROM t)",
])
def test_an_expression_is_accepted(sql: str) -> None:
    assert DatasetField(name="c", expression=_native(sql), datatype=DataType.FLOAT).native_texts == [sql]
