"""A formula-backed definition of a built-in aggregation is the model's own at every site (spec: aggregations/collect)."""

from __future__ import annotations

import pytest

from tests._library_names_fixtures import MAX_A, SUM_A, agg, measure, stored_engine, t_doc, value

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


class TestOtherClassificationSites:
    @pytest.mark.parametrize(("name", "formula", "expected"), [
        ("first", "MIN({value})", 1.0), ("last", "MAX({value})", MAX_A)])
    async def test_a_ranked_name_needs_no_ranking_column(self, name: str, formula: str, expected: float) -> None:
        async with stored_engine(models=[t_doc(aggregations=[agg(name, formula)])]) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [
                measure(f"{name}(a)"), measure(f"a:{name}", "c")]})
        row = resp.data[0]
        assert (float(row["t.v"]), float(row["t.c"])) == (expected, expected)

    @pytest.mark.parametrize(("formula", "factor"), [("percentile(a, 2)", 2), ("a:percentile(3)", 3)])
    async def test_positional_parameters_follow_the_definition(self, formula: str, factor: int) -> None:
        own = agg("percentile", "SUM({value}) * {k}", k="1")
        async with stored_engine(models=[t_doc(aggregations=[own])]) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure(formula)]})
        assert float(value(resp)) == factor * SUM_A
