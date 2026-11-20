"""A name bound by a lambda or list comprehension in Mode-A SQL is not a field reference."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.sql.field_expansion import expand_derived_refs_sync, root_scope_field_ids

DATASET = SlayerDataset(name="orders", sql_table="orders", data_source="ds", fields=[
    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
    DatasetField(name="tags", datatype=DataType.ARRAY, element_type=DataType.STRING),
])


@pytest.mark.parametrize("sql", [
    "LEN([e FOR e IN tags IF e IS NOT NULL])",
    "LIST_FILTER(tags, e -> e IS NOT NULL)",
])
def test_bound_names_are_not_root_columns(sql: str) -> None:
    tree = sqlglot.parse_one(sql, read="duckdb")
    assert isinstance(tree, Expression)
    root = {c.name for c in tree.find_all(exp.Column) if id(c) in root_scope_field_ids(parsed=tree)}
    assert root == {"tags"}


@pytest.mark.parametrize("sql", [
    "LEN([e FOR e IN tags IF e IS NOT NULL])",
    "LIST_FILTER(tags, e -> e IS NOT NULL)",
])
def test_expansion_qualifies_only_the_list(sql: str) -> None:
    out = expand_derived_refs_sync(sql=sql, dataset=DATASET, alias_path="orders", datasets_by_name={"orders": DATASET},
                                   dialect="duckdb")
    assert out is not None
    assert "orders.tags" in out
    assert "orders.e" not in out
