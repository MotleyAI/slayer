"""A built-in aggregation name resolves only to a model definition of exactly its lowercase name
(spec: models/aggregation-names)."""

from __future__ import annotations

import pytest

from tests._library_names_fixtures import MAX_A, SUM_A, agg, measure, saved_query_doc, stored_engine, t_doc, value

_SPELLINGS = ["{f}(a)", "{F}(a)", "a:{f}", "t.{f}(a)", "{C}(a)"]
_BUILTINS = [pytest.param("sum", SUM_A, id="sum"), pytest.param("max", MAX_A, id="max")]


def _spell(template: str, builtin: str) -> str:
    return template.format(f=builtin, F=builtin.upper(), C=builtin.capitalize())


def _hijacker(builtin: str, *, name: str | None = None) -> dict:
    return agg(name or builtin.capitalize(), f"{builtin.upper()}({{value}}) * 100")


class TestACaseVariantIsNoOverride:
    @pytest.mark.parametrize(("builtin", "expected"), _BUILTINS)
    @pytest.mark.parametrize("spelling", _SPELLINGS)
    async def test_every_spelling_is_the_builtin(self, builtin: str, expected: float, spelling: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_hijacker(builtin)])]) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [{"formula": _spell(spelling, builtin)}]})
        (row,) = resp.data
        assert float(row[f"t.a_{builtin}"]) == expected, row

    async def test_an_uppercase_definition_is_no_override_either(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_hijacker("sum", name="SUM")])]) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure("SUM(a)")]})
        assert float(value(resp)) == SUM_A


class TestContainers:
    async def test_a_stored_measure(self) -> None:
        doc = t_doc(aggregations=[_hijacker("sum")], measures=[{"name": "total", "formula": "SUM(a)"}])
        async with stored_engine(models=[doc]) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure("total")]})
        assert float(value(resp)) == SUM_A

    async def test_a_saved_query(self) -> None:
        saved = saved_query_doc(name="sum_q", stage={"source_model": "t", "measures": [measure("sum(a)", "total")]})
        async with stored_engine(models=[t_doc(aggregations=[_hijacker("sum")]), saved]) as engine:
            resp = await engine.execute("sum_q")
        assert float(value(resp, name="total")) == SUM_A


class TestExactDefinitions:
    async def test_an_exact_lowercase_definition_still_overrides(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_hijacker("sum", name="sum")])]) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure("SUM(a)")]})
        assert float(value(resp)) == SUM_A * 100

    async def test_several_case_variants_are_not_ambiguous(self) -> None:
        aggs = [_hijacker("sum", name="Sum"), _hijacker("sum", name="SUM")]
        async with stored_engine(models=[t_doc(aggregations=aggs)]) as engine:
            resp = await engine.execute({"source_model": "t", "measures": [measure("sum(a)")]})
        assert float(value(resp)) == SUM_A


class TestOtherNamesKeepCaseResolution:
    async def test_a_library_name_and_a_custom_name(self) -> None:
        aggs = [agg("upper"), agg("Total", "MAX({value})")]
        async with stored_engine(models=[t_doc(aggregations=aggs)]) as engine:
            resp = await engine.execute({"source_model": "t",
                                         "measures": [measure("UPPER(a)", "up"), measure("total(a)", "tot")]})
        (row,) = resp.data
        assert (float(row["t.up"]), float(row["t.tot"])) == (SUM_A, MAX_A)
