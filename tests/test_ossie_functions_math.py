"""``trunc(x, d)``, ``truncate``, ``radians``, ``degrees`` and whole digits for ``round`` / ``trunc`` (spec:
queries/scalar-functions › Math functions, Division and rounding follow Ossie's numeric semantics)."""

from __future__ import annotations

import math
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from slayer.core.errors import QueryTypeError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._ossie_functions_fixtures import (
    BACKENDS, assert_twins, by_id, emit_fn, fn_body, fn_engine, fn_query, named, refusal,
)
from tests._ossie_gate import TIER1


@pytest.fixture(scope="module", params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with fn_engine(request.param) as e:
        yield e


#: Dimension → value per row holding ``x`` = 2.789, -2.789 and 1234.5.
TRUNCATED = {
    "trunc(fn.x, 1)": {1: 2.7, 2: -2.7, 3: 1234.5},
    "TRUNCATE(fn.x, 1)": {1: 2.7, 2: -2.7, 3: 1234.5},
    "trunc(fn.x, 0)": {1: 2, 2: -2, 3: 1234},
    "trunc(fn.x, -1)": {1: 0, 2: 0, 3: 1230},
}
DIGIT_REFUSALS = ["round(fn.x, 1.5)", "trunc(fn.x, 1.5)", "round(fn.x, fn.ratio)", "trunc(fn.x, fn.ratio)",
                  "TRUNCATE(fn.x, 1.5)"]


def _close(got: dict[int, Any], expected: dict[int, float]) -> None:
    assert set(got) >= set(expected), got
    for rid, value in expected.items():
        assert got[rid] is not None and math.isclose(float(got[rid]), value, abs_tol=1e-9), (rid, got[rid], value)


class TestTrunc:
    @pytest.mark.parametrize("expr", sorted(TRUNCATED))
    async def test_truncates_toward_zero(self, engine: SlayerQueryEngine, expr: str) -> None:
        resp = await engine.execute(fn_query(expr, filter=["fn.id IN (1, 2, 3)"]))
        _close(by_id(resp), TRUNCATED[expr])

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("expr", sorted(TRUNCATED))
    async def test_emits_on_every_dialect(self, expr: str, dialect: str) -> None:
        await emit_fn(fn_query(expr), dialect=dialect)

    async def test_integer_column_digits(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query("trunc(fn.x, fn.n)", filter=["fn.id = 1"]))
        _close(by_id(resp), {1: 2.78})

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_integer_column_digits_emit(self, dialect: str) -> None:
        await emit_fn(fn_query("trunc(fn.x, fn.n)"), dialect=dialect)

    async def test_truncate_is_trunc(self, engine: SlayerQueryEngine) -> None:
        await assert_twins(engine, "TRUNCATE(fn.x, 1)", "trunc(fn.x, 1)")
        await assert_twins(engine, "truncate(fn.x)", "trunc(fn.x)")

    async def test_null_digits_or_value(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query("trunc(fn.x, fn.n)", filter=["fn.id IN (2, 4)"]))
        assert by_id(resp) == {2: None, 4: None}

    async def test_wrong_arity(self, engine: SlayerQueryEngine) -> None:
        error = await refusal(engine, fn_body("trunc(fn.x, 1, 2)"))
        assert "trunc" in str(error).lower(), str(error)


class TestDigitsMustBeWhole:
    @pytest.mark.parametrize("expr", DIGIT_REFUSALS)
    async def test_refused_at_plan_time(self, engine: SlayerQueryEngine, expr: str) -> None:
        error = await refusal(engine, fn_body(expr))
        assert isinstance(error, QueryTypeError), (type(error), str(error))
        message = str(error).lower()
        assert expr.split("(", 1)[0].lower() in message, message
        assert "digit" in message, message

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("expr", DIGIT_REFUSALS)
    async def test_refused_on_every_dialect(self, expr: str, dialect: str) -> None:
        with pytest.raises(QueryTypeError):
            await emit_fn(fn_query(expr), dialect=dialect)

    @pytest.mark.parametrize("expr", ["round(fn.x, 2)", "round(fn.x, -1)", "round(fn.x, 2.0)", "round(fn.x, fn.n)"])
    async def test_whole_digits_accepted(self, engine: SlayerQueryEngine, expr: str) -> None:
        await engine.execute(fn_query(expr))


class TestAngles:
    async def test_radians_and_degrees(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query(named("radians(180)", "r"), named("DEGREES(pi())", "d"),
                                             filter=["fn.id = 1"]))
        (row,) = resp.data
        assert math.isclose(float(row["r"]), math.pi, rel_tol=1e-12)
        assert math.isclose(float(row["d"]), 180.0, rel_tol=1e-12)

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_emits_on_every_dialect(self, dialect: str) -> None:
        await emit_fn(fn_query(named("radians(fn.x)", "r"), named("degrees(fn.x)", "d")), dialect=dialect)

    @pytest.mark.parametrize("expr", ["radians(fn.status)", "degrees(fn.status)"])
    async def test_text_argument_refused(self, engine: SlayerQueryEngine, expr: str) -> None:
        error = await refusal(engine, fn_body(expr))
        assert "TRY_CAST" in str(error), str(error)
