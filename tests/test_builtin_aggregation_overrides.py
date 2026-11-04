"""A formula-backed definition of a built-in aggregation is the model's own at every site (spec: aggregations/collect)."""

from __future__ import annotations

import pytest

from tests._library_names_fixtures import SUM_A, agg, measure, stored_engine, t_doc, value

_COLLECT_SUM = agg("collect", "SUM({value})")


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
class TestOwnCollect:
    async def test_answers_the_formula(self, dialect: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_COLLECT_SUM])], dialect=dialect) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure("collect(a)")]})
        assert float(value(resp)) == SUM_A

    async def test_the_empty_value_follows_the_formula(self, dialect: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_COLLECT_SUM])], dialect=dialect) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure("collect(a)")],
                                         "filters": ["a > 100"]})
        assert value(resp) is None

    async def test_its_result_type_is_the_formulas(self, dialect: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_COLLECT_SUM])], dialect=dialect) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure("collect(a) + 1")]})
        assert float(value(resp)) == SUM_A + 1


async def test_another_model_keeps_the_builtin_collect() -> None:
    async with stored_engine(models=[t_doc(aggregations=[_COLLECT_SUM])], dialect="duckdb") as engine:
        resp = await engine.execute({"source_model": "t", "measures": [measure("collect(customers.name)")]})
    assert list(value(resp)) == ["abc", "xyz"]
