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
    metric,
    memory_doc,
    saved_query_doc,
    stored_engine,
    t_doc,
    value,
)
from tests._stored_upgrade_fixtures import DS


def _query(*measures: dict, source: str = "t", **body: object) -> dict:
    return {"population": source, "metrics": list(measures), **body}


class TestStoredShadow:
    async def test_reverse_in_every_spelling(self) -> None:
        async with stored_engine(datasets=[t_doc(aggregations=[agg("reverse")])]) as engine:
            assert await engine.storage.get_dataset("t", data_source=DS) is not None
            resp = await engine.execute(_query(
                metric("reverse(t.a)", "f"), metric("t.reverse(a)", "m")))
        row = resp.data[0]
        assert [float(row[k]) for k in ("f", "m")] == [SUM_A] * 2

    async def test_a_list_function_name(self) -> None:
        async with stored_engine(datasets=[t_doc(aggregations=[agg("contains")])]) as engine:
            assert await engine.storage.get_dataset("t", data_source=DS) is not None
            resp = await engine.execute(_query(metric("contains(t.a)")))
        assert float(value(resp)) == SUM_A

    async def test_another_model_keeps_the_library_function(self) -> None:
        async with stored_engine(datasets=[t_doc(aggregations=[agg("reverse")])]) as engine:
            resp = await engine.execute({
                "population": "t", "dimensions": [{"field": "reverse(customers.name)", "alias": "rn"}],
                "metrics": [metric("count(t.*)", "n")]})
        assert {r["rn"] for r in resp.data} == {"cba", "zyx"}

    async def test_a_literal_argument_keeps_the_library_function(self) -> None:
        async with stored_engine(datasets=[t_doc(aggregations=[agg("reverse")])]) as engine:
            resp = await engine.execute({
                "population": "t", "dimensions": [{"field": "concat(t.s, reverse('abc'))", "alias": "lit"}],
                "metrics": [metric("count(t.*)", "n")]})
        assert {r["lit"] for r in resp.data} == {"xcba", "ycba", "zcba"}

    @pytest.mark.parametrize("spelling", sorted(SCALAR_SPELLINGS))
    async def test_a_parser_spelling_is_shadowed_by_its_own_name(self, spelling: str) -> None:
        async with stored_engine(datasets=[t_doc(aggregations=[agg(spelling)])]) as engine:
            resp = await engine.execute(_query(metric(f"{spelling}(a)")))
        assert float(resp.data[0]["v"]) == SUM_A

    @pytest.mark.parametrize(("formula", "expected"), [
        pytest.param("left(t.a)", SUM_A, id="default"),
        pytest.param("left(t.a, t.n=2)", 2 * SUM_A, id="functional-kwarg"),
        pytest.param("t.left(a, n=2)", 2 * SUM_A, id="method-kwarg"),
    ])
    async def test_parameters_the_library_function_would_refuse(self, formula: str, expected: float) -> None:
        left = agg("left", "SUM({value}) * {n}", n="1")
        async with stored_engine(datasets=[t_doc(aggregations=[left])]) as engine:
            resp = await engine.execute(_query(metric(formula)))
        assert float(value(resp)) == expected

    async def test_a_parser_spelling_keeps_its_written_arguments(self) -> None:
        formula = "SUM({value}) * 0 + MAX({k1}) + MAX({k2})"
        own = agg("json_extract_path_text", formula, k1="id", k2="id")
        twin = agg("twin", formula, k1="id", k2="id")
        async with stored_engine(datasets=[t_doc(aggregations=[own]), t_doc(name="u", aggregations=[twin])]) as engine:
            spelled = await engine.execute(_query(metric("json_extract_path_text(t.a, 'id', 'cust_id')")))
            expected = await engine.execute(_query(metric("twin(a, 'id', 'cust_id')"), source="u"))
        assert float(value(spelled)) == float(value(expected))

    async def test_a_spelling_inside_a_method_call_reads_through_its_path(self) -> None:
        formula = "customers.max(strpos(id, '1'))"
        async with stored_engine(datasets=[t_doc(aggregations=[agg("strpos")]), t_doc(name="u")]) as engine:
            shadowing = await engine.execute(_query(metric(formula)))
            plain = await engine.execute(_query(metric(formula), source="u"))
        assert value(shadowing) == value(plain)

    @pytest.mark.parametrize("name", ["date_part", "interval"])
    @pytest.mark.parametrize("call", ["{name}(a, 2)", "{name}(a, k = 2)"], ids=["positional", "keyword"])
    async def test_a_date_function_name_with_the_library_arity(self, name: str, call: str) -> None:
        own = agg(name, "SUM({value}) * {k}", k="1")
        async with stored_engine(datasets=[t_doc(aggregations=[own])]) as engine:
            resp = await engine.execute(_query(metric(call.format(name=name))))
        assert float(value(resp)) == 2 * SUM_A


class TestCaseResolution:
    @pytest.mark.parametrize("formula", ["REVERSE(t.a)", "Reverse(t.a)"])
    async def test_a_case_insensitive_match(self, formula: str) -> None:
        async with stored_engine(datasets=[t_doc(aggregations=[agg("reverse")])]) as engine:
            resp = await engine.execute(_query(metric(formula)))
        assert float(value(resp)) == SUM_A

    async def test_an_exact_case_match_wins(self) -> None:
        aggs = [agg("reverse"), agg("REVERSE", "MAX({value})")]
        async with stored_engine(datasets=[t_doc(aggregations=aggs)]) as engine:
            resp = await engine.execute(_query(metric("reverse(t.a)", "lo"), metric("REVERSE(t.a)", "up")))
        row = resp.data[0]
        assert (float(row["lo"]), float(row["up"])) == (SUM_A, MAX_A)

    async def test_an_ambiguous_case_insensitive_match(self) -> None:
        aggs = [agg("Reverse"), agg("REVERSE", "MAX({value})")]
        async with stored_engine(datasets=[t_doc(aggregations=aggs)]) as engine:
            assert await engine.storage.get_dataset("t", data_source=DS) is not None
            query = _query(metric("reverse(t.a)"))
            with pytest.raises(SlayerError) as info:
                await engine.execute(query)
        msg = str(info.value)
        assert "Reverse" in msg, msg
        assert "REVERSE" in msg, msg
        assert "ambiguous" in msg.lower(), msg


class TestContainers:
    async def test_a_stored_measure(self) -> None:
        doc = t_doc(aggregations=[agg("reverse")], metrics=[{"name": "rev", "formula": "reverse(a)"}])
        async with stored_engine(datasets=[doc]) as engine:
            resp = await engine.execute(_query(metric("rev")))
        assert float(value(resp)) == SUM_A

    async def test_a_saved_query(self) -> None:
        saved = saved_query_doc(name="rev_q", stage=_query(metric("reverse(t.a)", "rev")))
        async with stored_engine(datasets=[t_doc(aggregations=[agg("reverse")]), saved]) as engine:
            resp = await engine.execute("rev_q")
        assert float(value(resp, name="rev")) == SUM_A

    async def test_a_memory_query(self) -> None:
        mem = memory_doc(memory_id="m_rev", query=_query(metric("reverse(t.a)", "rev")))
        async with stored_engine(datasets=[t_doc(aggregations=[agg("reverse")])], memories=[mem]) as engine:
            stored = await engine.storage.get_memory("m_rev")
            assert stored.query is not None
            resp = await engine.execute(stored.query)
        assert float(value(resp, name="rev")) == SUM_A


class TestLibraryErrorsKept:
    async def test_an_undefined_name_keeps_the_library_error(self) -> None:
        async with stored_engine(datasets=[t_doc()]) as engine:
            query = _query(metric("left(t.a, t.n=2)"))
            with pytest.raises(UnknownReferenceError, match="scalar functions take positional arguments"):
                await engine.execute(query)

    @pytest.mark.parametrize("formula", ["date_part(t.a, 2)", "interval(t.a, 2)"])
    async def test_an_undefined_date_function_keeps_the_unit_error(self, formula: str) -> None:
        async with stored_engine(datasets=[t_doc()]) as engine:
            query = _query(metric(formula))
            with pytest.raises(ValueError, match="must be a string literal"):
                await engine.execute(query)

    async def test_the_library_function_still_answers_without_a_definition(self) -> None:
        async with stored_engine(datasets=[t_doc()]) as engine:
            resp = await engine.execute({
                "population": "t", "dimensions": [{"field": "reverse(t.s)", "alias": "rs"}],
                "metrics": [metric("count(t.*)", "n")]})
        assert {r["rs"] for r in resp.data} == {"x", "y", "z"}


class TestStillReserved:
    @pytest.mark.parametrize("name", ["cumsum", "lag", "time_shift"])
    def test_a_transform_name(self, name: str) -> None:
        with pytest.raises(pydantic.ValidationError, match="transform"):
            Aggregation(name=name, expression="SUM({value})")

    @pytest.mark.parametrize("name", ["extract", "doy", "ISO_WEEK"])
    def test_a_date_syntax_name(self, name: str) -> None:
        with pytest.raises(pydantic.ValidationError, match="date"):
            Aggregation(name=name, expression="SUM({value})")

    @pytest.mark.parametrize("name", ["round", "reverse", "strpos", "contains", "Left"])
    def test_a_function_name_is_accepted(self, name: str) -> None:
        assert Aggregation(name=name, expression="SUM({value})").name == name
