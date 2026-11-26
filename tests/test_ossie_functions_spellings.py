"""Ossie conditional and string-search spellings (spec: queries/scalar-functions › Ossie conditional spellings,
Ossie string-search spellings)."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ScalarOperandTypeError, SlayerError
from slayer.core.keys import SCALAR_SPECS
from slayer.core.models import Aggregation, DatasetField
from slayer.engine.query_engine import SlayerQueryEngine
from tests._ossie_functions_fixtures import (
    BACKENDS, assert_twins, by_id, emit_fn, fn_dataset, fn_body, fn_engine, fn_query, named, refusal,
)
from tests._ossie_gate import TIER1


@pytest.fixture(scope="module", params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with fn_engine(request.param) as e:
        yield e


#: Ossie spelling → its library twin.
TWINS = {
    "IF(fn.amount > 20, 1, 0)": "iif(fn.amount > 20, 1, 0)",
    "if(fn.amount > 20, 1, 0)": "iif(fn.amount > 20, 1, 0)",
    "IFF(fn.amount > 20, 1, 0)": "iif(fn.amount > 20, 1, 0)",
    "NVL(fn.sla_days, -1)": "ifnull(fn.sla_days, -1)",
    "nvl(fn.sla_days, -1)": "ifnull(fn.sla_days, -1)",
    "NVL2(fn.shipped_at, 'y', 'n')": "iif(fn.shipped_at IS NOT NULL, 'y', 'n')",
    "STARTSWITH(fn.status, 'pe')": "starts_with(fn.status, 'pe')",
    "EndsWith(fn.status, 'ing')": "ends_with(fn.status, 'ing')",
    "CHARINDEX('d', fn.status)": "instr(fn.status, 'd')",
}


class TestTwins:
    @pytest.mark.parametrize("written", sorted(TWINS))
    async def test_spelling_is_its_twin(self, engine: SlayerQueryEngine, written: str) -> None:
        await assert_twins(engine, written, TWINS[written])

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("written", sorted(TWINS))
    async def test_spelling_emits_its_twin_sql(self, written: str, dialect: str) -> None:
        assert await emit_fn(fn_query(written), dialect=dialect) == await emit_fn(fn_query(TWINS[written]),
                                                                                 dialect=dialect)


class TestConditionals:
    async def test_conditionals_evaluate(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query(
            named("IF(fn.amount > 20, 1, 0)", "i"), named("NVL(fn.shipped_at, fn.created_at)", "nv"),
            named("NVL2(fn.shipped_at, 'y', 'n')", "n2"), named("ZEROIFNULL(fn.sla_days)", "z"),
            named("NULLIFZERO(fn.sla_days)", "nz"), named("fn.created_at", "c"), named("fn.shipped_at", "s"),
            filter=["fn.id IN (1, 2)"]))
        got = {int(r["fn.id"]): r for r in resp.data}
        assert (int(got[1]["i"]), got[1]["nv"], got[1]["n2"], int(got[1]["z"]), got[1]["nz"]) == (
            1, got[1]["c"], "n", 0, None)
        assert (int(got[2]["i"]), got[2]["nv"], got[2]["n2"], int(got[2]["z"]), got[2]["nz"]) == (
            0, got[2]["s"], "y", 0, None)

    async def test_zeroifnull_keeps_a_value(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query("ZEROIFNULL(fn.sla_days)"))
        assert {k: int(v) for k, v in by_id(resp).items()} == {1: 0, 2: 0, 3: 0, 4: 3, 5: 1, 6: 2}

    async def test_nullifzero_keeps_a_value(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query("nullifzero(fn.sla_days)"))
        assert {k: None if v is None else int(v) for k, v in by_id(resp).items()} == {
            1: None, 2: None, 3: None, 4: 3, 5: 1, 6: 2}

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_zero_and_null_conversions_emit(self, dialect: str) -> None:
        await emit_fn(fn_query("ZEROIFNULL(fn.amount)", named("NULLIFZERO(fn.amount)", "w")), dialect=dialect)

    @pytest.mark.parametrize("expr", [
        "IF(fn.amount > 20, 1)", "NVL(fn.status)", "NVL(fn.status, 'a', 'b')", "NVL2(fn.status, 'y')",
        "ZEROIFNULL(fn.amount, 1)", "NULLIFZERO()", "IFF(fn.amount > 20, 1)",
    ])
    async def test_wrong_arity_names_the_function(self, engine: SlayerQueryEngine, expr: str) -> None:
        error = await refusal(engine, fn_body(expr))
        assert expr.split("(", 1)[0].lower() in str(error).lower(), str(error)


class TestNumericArgument:
    async def test_text_argument_refused_at_query_time(self, engine: SlayerQueryEngine) -> None:
        error = await refusal(engine, fn_body("NULLIFZERO(fn.status)"))
        assert isinstance(error, ScalarOperandTypeError), (type(error), str(error))
        assert "TRY_CAST" in str(error)

#: Numeric library functions this change adds.
NEW_NUMERIC = frozenset({"zeroifnull", "nullifzero", "radians", "degrees"})
#: Every numeric library function over a TEXT field, at its least argument count (the others ``1``).
TEXT_FIELD_EXPRESSIONS = sorted(
    {f"{name}(status{', 1' * (SCALAR_SPECS[name].min_args - 1)})"
     for name in SCALAR_SPECS if SCALAR_SPECS[name].numeric and SCALAR_SPECS[name].min_args >= 1}
    | {f"{name}(status)" for name in NEW_NUMERIC}
)


def _text_field(expression: str) -> DatasetField:
    return DatasetField(name="z", expression=expression, datatype=DataType.FLOAT)


class TestNumericFieldExpressions:
    """Every numeric library function, the pre-existing ones included, checks a field expression's argument."""

    @pytest.mark.parametrize("expression", TEXT_FIELD_EXPRESSIONS)
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_text_argument_refused_when_saved(self, backend: str, expression: str) -> None:
        async with fn_engine(backend, validate=True) as eng:
            with pytest.raises(SlayerError) as info:
                await eng.storage.save_dataset(fn_dataset(extra=[_text_field(expression)]))
        assert isinstance(info.value, ScalarOperandTypeError), (type(info.value), str(info.value))
        assert "TRY_CAST" in str(info.value)

    @pytest.mark.parametrize("expression", TEXT_FIELD_EXPRESSIONS)
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_stored_text_argument_refused_when_queried(self, backend: str, expression: str) -> None:
        async with fn_engine(backend, datasets=[fn_dataset(extra=[_text_field(expression)])]) as eng:
            error = await refusal(eng, fn_body("fn.z"))
        assert isinstance(error, ScalarOperandTypeError), (type(error), str(error))
        assert "TRY_CAST" in str(error)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_try_cast_argument_saves_and_runs(self, backend: str) -> None:
        async with fn_engine(backend, validate=True) as eng:
            await eng.storage.save_dataset(fn_dataset(extra=[_text_field("ZEROIFNULL(TRY_CAST(status AS DOUBLE))")]))
            resp = await eng.execute(fn_query("fn.z"))
        assert {k: float(v) for k, v in by_id(resp).items()} == dict.fromkeys(range(1, 7), 0.0)


class TestStringSearch:
    async def test_search_spellings_evaluate(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query(
            named("STARTSWITH(fn.status, 'pe')", "s"), named("ENDSWITH(fn.status, 'ing')", "e"),
            named("CHARINDEX('d', fn.status)", "c"), named("CHARINDEX('z', fn.status)", "z"),
            filter=["fn.id = 1"]))
        (row,) = resp.data
        assert (bool(row["s"]), bool(row["e"]), int(row["c"]), int(row["z"])) == (True, True, 4, 0)

    async def test_charindex_argument_order(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query("CHARINDEX(fn.code, 'xxabc')", filter=["fn.id = 1"]))
        assert int(by_id(resp)[1]) == 3

    async def test_charindex_with_a_start_position(self, engine: SlayerQueryEngine) -> None:
        error = await refusal(engine, fn_body("CHARINDEX('d', fn.status, 2)"))
        assert "charindex" in str(error).lower(), str(error)
        assert "3" in str(error), str(error)

    @pytest.mark.parametrize("expr", ["STARTSWITH(fn.status)", "ENDSWITH(fn.status, 'a', 'b')", "CHARINDEX('d')"])
    async def test_wrong_arity_names_the_function(self, engine: SlayerQueryEngine, expr: str) -> None:
        error = await refusal(engine, fn_body(expr))
        assert expr.split("(", 1)[0].lower() in str(error).lower(), str(error)


class TestCustomAggregations:
    @pytest.mark.parametrize("name", ["nvl", "if", "nvl2", "zeroifnull", "startswith", "charindex"])
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_model_aggregation_wins(self, backend: str, name: str) -> None:
        dataset = fn_dataset(aggregations=[Aggregation(name=name, expression="MAX({value})")])
        async with fn_engine(backend, datasets=[dataset]) as eng:
            resp = await eng.execute({"population": "fn", "metrics": [{"metric": f"{name}(fn.amount)", "alias": "m"}]})
        assert [float(r["m"]) for r in resp.data] == [30.0]
