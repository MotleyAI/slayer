"""A model aggregation may take a library function name and shadows it in its own model (spec: models/aggregation-names)."""

from __future__ import annotations

import pydantic
import pytest

from slayer.core.errors import SlayerError, UnknownReferenceError
from slayer.core.keys import SCALAR_SPELLINGS
from slayer.core.models import Aggregation
from tests._library_names_fixtures import (
    MAX_A,
    SUM_A,
    agg,
    measure,
    memory_doc,
    saved_query_doc,
    stored_engine,
    t_doc,
    value,
)
from tests._stored_upgrade_fixtures import DS


def _query(*measures: dict, source: str = "t", **body: object) -> dict:
    return {"source_model": source, "measures": list(measures), **body}


class TestStoredShadow:
    async def test_reverse_in_every_spelling(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[agg("reverse")])]) as engine:
            assert await engine.storage.get_model("t", data_source=DS) is not None
            resp = await engine.execute(_query(
                measure("reverse(a)", "f"), measure("a:reverse", "c"), measure("t.reverse(a)", "m")))
        row = resp.data[0]
        assert [float(row[f"t.{k}"]) for k in ("f", "c", "m")] == [SUM_A] * 3

    async def test_another_model_keeps_the_library_function(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[agg("reverse")])]) as engine:
            resp = await engine.execute({
                "source_model": "t", "dimensions": [{"expression": "reverse(customers.name)", "name": "rn"}],
                "measures": [measure("count(*)", "n")]})
        assert {r["t.rn"] for r in resp.data} == {"cba", "zyx"}

    async def test_a_literal_argument_keeps_the_library_function(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[agg("reverse")])]) as engine:
            resp = await engine.execute({
                "source_model": "t", "dimensions": [{"expression": "concat(s, reverse('abc'))", "name": "lit"}],
                "measures": [measure("count(*)", "n")]})
        assert {r["t.lit"] for r in resp.data} == {"xcba", "ycba", "zcba"}

    @pytest.mark.parametrize("spelling", sorted(SCALAR_SPELLINGS))
    async def test_a_parser_spelling_is_shadowed_by_its_own_name(self, spelling: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[agg(spelling)])]) as engine:
            resp = await engine.execute(_query(measure(f"{spelling}(a)"), measure(f"a:{spelling}", "c")))
        row = resp.data[0]
        assert (float(row["t.v"]), float(row["t.c"])) == (SUM_A, SUM_A)

    @pytest.mark.parametrize(("formula", "expected"), [
        pytest.param("left(a)", SUM_A, id="default"),
        pytest.param("left(a, n=2)", 2 * SUM_A, id="functional-kwarg"),
        pytest.param("a:left(n=3)", 3 * SUM_A, id="colon-kwarg"),
        pytest.param("t.left(a, n=2)", 2 * SUM_A, id="method-kwarg"),
    ])
    async def test_parameters_the_library_function_would_refuse(self, formula: str, expected: float) -> None:
        left = agg("left", "SUM({value}) * {n}", n="1")
        async with stored_engine(models=[t_doc(aggregations=[left])]) as engine:
            resp = await engine.execute(_query(measure(formula)))
        assert float(value(resp)) == expected


class TestCaseResolution:
    @pytest.mark.parametrize("formula", ["REVERSE(a)", "Reverse(a)", "a:REVERSE"])
    async def test_a_case_insensitive_match(self, formula: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[agg("reverse")])]) as engine:
            resp = await engine.execute(_query(measure(formula)))
        assert float(value(resp)) == SUM_A

    async def test_an_exact_case_match_wins(self) -> None:
        aggs = [agg("reverse"), agg("REVERSE", "MAX({value})")]
        async with stored_engine(models=[t_doc(aggregations=aggs)]) as engine:
            resp = await engine.execute(_query(measure("reverse(a)", "lo"), measure("REVERSE(a)", "up")))
        row = resp.data[0]
        assert (float(row["t.lo"]), float(row["t.up"])) == (SUM_A, MAX_A)

    async def test_an_ambiguous_case_insensitive_match(self) -> None:
        aggs = [agg("Reverse"), agg("REVERSE", "MAX({value})")]
        async with stored_engine(models=[t_doc(aggregations=aggs)]) as engine:
            assert await engine.storage.get_model("t", data_source=DS) is not None
            with pytest.raises(SlayerError) as info:
                await engine.execute(_query(measure("reverse(a)")))
        msg = str(info.value)
        assert "Reverse" in msg and "REVERSE" in msg, msg
        assert "ambiguous" in msg.lower(), msg


class TestContainers:
    async def test_a_stored_measure(self) -> None:
        doc = t_doc(aggregations=[agg("reverse")], measures=[{"name": "rev", "formula": "reverse(a)"}])
        async with stored_engine(models=[doc]) as engine:
            resp = await engine.execute(_query(measure("rev")))
        assert float(value(resp)) == SUM_A

    async def test_a_saved_query(self) -> None:
        saved = saved_query_doc(name="rev_q", stage=_query(measure("reverse(a)", "rev")))
        async with stored_engine(models=[t_doc(aggregations=[agg("reverse")]), saved]) as engine:
            resp = await engine.execute("rev_q")
        assert float(value(resp, name="rev")) == SUM_A

    async def test_a_memory_query(self) -> None:
        mem = memory_doc(memory_id="m_rev", query=_query(measure("reverse(a)", "rev")))
        async with stored_engine(models=[t_doc(aggregations=[agg("reverse")])], memories=[mem]) as engine:
            stored = await engine.storage.get_memory("m_rev")
            assert stored.query is not None
            resp = await engine.execute(stored.query)
        assert float(value(resp, name="rev")) == SUM_A


class TestLibraryErrorsKept:
    async def test_an_undefined_name_keeps_the_library_error(self) -> None:
        async with stored_engine(models=[t_doc()]) as engine:
            with pytest.raises(UnknownReferenceError, match="scalar functions take positional arguments"):
                await engine.execute(_query(measure("left(a, n=2)")))

    async def test_the_library_function_still_answers_without_a_definition(self) -> None:
        async with stored_engine(models=[t_doc()]) as engine:
            resp = await engine.execute({
                "source_model": "t", "dimensions": [{"expression": "reverse(s)", "name": "rs"}],
                "measures": [measure("count(*)", "n")]})
        assert {r["t.rs"] for r in resp.data} == {"x", "y", "z"}


class TestStillReserved:
    @pytest.mark.parametrize("name", ["cumsum", "lag", "time_shift"])
    def test_a_transform_name(self, name: str) -> None:
        with pytest.raises(pydantic.ValidationError, match="transform"):
            Aggregation(name=name, formula="SUM({value})")

    @pytest.mark.parametrize("name", ["extract", "doy", "ISO_WEEK"])
    def test_a_date_syntax_name(self, name: str) -> None:
        with pytest.raises(pydantic.ValidationError, match="date"):
            Aggregation(name=name, formula="SUM({value})")

    @pytest.mark.parametrize("name", ["round", "reverse", "strpos", "contains", "Left"])
    def test_a_function_name_is_accepted(self, name: str) -> None:
        assert Aggregation(name=name, formula="SUM({value})").name == name
