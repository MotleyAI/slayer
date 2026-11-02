"""A name bound by a lambda or list comprehension in Mode-A SQL is not a column reference."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.sql.column_expansion import expand_derived_refs_sync, root_scope_column_ids

MODEL = SlayerModel(name="orders", sql_table="orders", data_source="ds", columns=[
    Column(name="id", type=DataType.INT, primary_key=True),
    Column(name="tags", type=DataType.ARRAY, element_type=DataType.TEXT),
])


@pytest.mark.parametrize("sql", [
    "LEN([e FOR e IN tags IF e IS NOT NULL])",
    "LIST_FILTER(tags, e -> e IS NOT NULL)",
])
def test_bound_names_are_not_root_columns(sql: str) -> None:
    tree = sqlglot.parse_one(sql, read="duckdb")
    assert isinstance(tree, Expression)
    root = {c.name for c in tree.find_all(exp.Column) if id(c) in root_scope_column_ids(parsed=tree)}
    assert root == {"tags"}


@pytest.mark.parametrize("sql", [
    "LEN([e FOR e IN tags IF e IS NOT NULL])",
    "LIST_FILTER(tags, e -> e IS NOT NULL)",
])
def test_expansion_qualifies_only_the_list(sql: str) -> None:
    out = expand_derived_refs_sync(sql=sql, model=MODEL, alias_path="orders", models_by_name={"orders": MODEL},
                                   dialect="duckdb")
    assert out is not None
    assert "orders.tags" in out
    assert "orders.e" not in out
