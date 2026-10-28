"""TEXT against a number is numeric, through ``TRY_CAST(text AS DOUBLE)`` (spec: queries/mixed-type-coercion)."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import SlayerError
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2120_fixtures import (
    EXEC_BACKENDS,
    emit_lib,
    lib_engine,
    lib_ids,
    lib_query,
    lib_values,
    patch_profile,
)


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


def _measure(formula: str, **kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "lib", "measures": [{"formula": formula, "name": "v"}], **kw})


def _where(sql: str, *, dialect: str) -> exp.Where:
    where = sqlglot.parse_one(sql, dialect=dialect).find(exp.Where)
    assert where is not None, sql
    return where


class TestScenarios:
    async def test_comparison_against_an_integer_literal(self, engine: SlayerQueryEngine) -> None:
        assert lib_ids(await engine.execute(lib_query(filters=["num_txt > 100"]))) == {1}

    async def test_comparison_against_a_numeric_column(self, engine: SlayerQueryEngine) -> None:
        assert 5 in lib_ids(await engine.execute(lib_query(filters=["num_txt = qty"])))

    async def test_numeric_not_lexicographic(self, engine: SlayerQueryEngine) -> None:
        assert 6 not in lib_ids(await engine.execute(lib_query(filters=["num_txt < 9"])))

    async def test_arithmetic(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("num_txt * 2", filters=["id = 7"])))
        assert float(values[7]) == 3.0

    async def test_arithmetic_result_aggregates_as_a_number(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_measure("sum(num_txt * 2)", filters=["id IN (5, 7)"]))
        assert float(resp.data[0]["lib.v"]) == 9.0

    async def test_in_list(self, engine: SlayerQueryEngine) -> None:
        assert lib_ids(await engine.execute(lib_query(filters=["num_txt IN (1, 2)"]))) == {8, 9}

    async def test_numeric_string_literal(self, engine: SlayerQueryEngine) -> None:
        assert lib_ids(await engine.execute(lib_query(filters=["qty > '1'"]))) == {1, 4, 5, 7, 9}

    @pytest.mark.parametrize("filt", ["qty > 'abc'", "qty IN (1, 'abc')", "qty BETWEEN 'abc' AND 3", "qty + 'abc' > 1"])
    async def test_non_numeric_literal_rejected(self, engine: SlayerQueryEngine, filt: str) -> None:
        query = lib_query(filters=[filt])
        with pytest.raises(SlayerError, match="'abc'"):
            await engine.execute(query)

    async def test_exact_large_integers(self, engine: SlayerQueryEngine) -> None:
        exact = lib_ids(await engine.execute(lib_query(
            filters=["TRY_CAST('12345678901234567' AS BIGINT) = 12345678901234567"])))
        off_by_one = lib_ids(await engine.execute(lib_query(
            filters=["TRY_CAST('12345678901234567' AS BIGINT) = 12345678901234568"])))
        assert (len(exact), off_by_one) == (10, set())


class TestLimitedToMixedOperands:
    @pytest.mark.parametrize("formula", ["sum(num_txt)", "avg(num_txt)"])
    async def test_numeric_aggregation_over_text_names_try_cast(self, engine: SlayerQueryEngine, formula: str) -> None:
        query = _measure(formula)
        with pytest.raises(SlayerError, match="TRY_CAST"):
            await engine.execute(query)

    async def test_numeric_function_over_text_names_try_cast(self, engine: SlayerQueryEngine) -> None:
        query = lib_query("abs(num_txt)")
        with pytest.raises(SlayerError, match="TRY_CAST"):
            await engine.execute(query)

    async def test_text_against_text_is_untouched(self) -> None:
        sql = await emit_lib(lib_query(filters=["num_txt = '1'"]), dialect="duckdb")
        assert "TRY_CAST" not in sql.upper(), sql

    async def test_text_against_a_timestamp_is_a_time_point(self) -> None:
        sql = await emit_lib(lib_query(filters=["ts > '2025-03-01'"]), dialect="duckdb")
        assert "DOUBLE" not in sql.upper(), sql

    async def test_concat_with_a_number_is_text(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("num_txt || qty", filters=["id = 1"])))
        assert values == {1: "120.53"}

    async def test_concat_ws_with_a_number_is_text(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("concat_ws('-', word, qty)", filters=["id = 1"])))
        assert values == {1: "paid-3"}

    @pytest.mark.parametrize("expr", ["word || (qty > 1)", "concat(word, qty > 1)", "concat_ws('-', word, qty > 1)",
                                      "concat_ws(qty > 1, word)"])
    async def test_boolean_in_concatenation_is_refused(self, engine: SlayerQueryEngine, expr: str) -> None:
        query = lib_query(expr)
        with pytest.raises(SlayerError, match="CASE WHEN"):
            await engine.execute(query)

    @pytest.mark.parametrize("dialect", ["bigquery", "clickhouse"])
    async def test_concat_ws_array_engines_get_text_elements(self, dialect: str) -> None:
        sql = await emit_lib(lib_query("concat_ws('-', word, qty)"), dialect=dialect)
        assert "CAST(LIB.QTY AS " in sql.upper(), sql


class TestEmission:
    async def test_text_side_is_a_try_cast_to_double(self) -> None:
        sql = await emit_lib(lib_query(filters=["num_txt > 100"]), dialect="duckdb")
        casts = [c for c in _where(sql, dialect="duckdb").find_all(exp.TryCast)]
        assert any(c.to.is_type(exp.DataType.Type.DOUBLE) for c in casts), sql

    async def test_author_cast_is_never_rewritten(self) -> None:
        sql = await emit_lib(lib_query(filters=["CAST(num_txt AS INT) > 100"]), dialect="duckdb")
        where = _where(sql, dialect="duckdb")
        assert not list(where.find_all(exp.TryCast)), sql
        assert len([c for c in where.find_all(exp.Cast) if not isinstance(c, exp.TryCast)]) == 1, sql

    async def test_numeric_string_literal_becomes_a_number(self) -> None:
        sql = await emit_lib(lib_query(filters=["qty > '1'"]), dialect="duckdb")
        literals = list(_where(sql, dialect="duckdb").find_all(exp.Literal))
        assert literals, sql
        assert not any(lit.is_string for lit in literals), sql

    async def test_postgres_16_gets_the_null_on_error_form(self, monkeypatch) -> None:
        patch_profile(monkeypatch, version=(16, 2))
        sql = await emit_lib(lib_query(filters=["num_txt > 100"]), dialect="postgres")
        assert "PG_INPUT_IS_VALID(" in sql.upper(), sql

    @pytest.mark.parametrize("version", [(15, 4), None])
    async def test_older_or_unknown_postgres_gets_a_plain_cast(self, monkeypatch, version) -> None:
        patch_profile(monkeypatch, version=version)
        sql = await emit_lib(lib_query(filters=["num_txt > 100"]), dialect="postgres")
        assert "PG_INPUT_IS_VALID(" not in sql.upper(), sql
        assert list(_where(sql, dialect="postgres").find_all(exp.Cast)), sql

    async def test_profile_probed_for_coercion(self, monkeypatch) -> None:
        calls = patch_profile(monkeypatch, version=(16, 2))
        await emit_lib(lib_query(filters=["qty > 1"]), dialect="postgres")
        assert calls == []
        await emit_lib(lib_query(filters=["num_txt > 1"]), dialect="postgres")
        assert calls == [1]

    @pytest.mark.parametrize("filt", ["num_txt BETWEEN 1 AND 10", "num_txt IS DISTINCT FROM 99", "greatest(num_txt, 5) > 6"])
    async def test_every_position_coerces(self, filt: str) -> None:
        sql = await emit_lib(lib_query(filters=[filt]), dialect="duckdb")
        assert list(_where(sql, dialect="duckdb").find_all(exp.TryCast)), sql


class TestClickHouseCasts:
    async def test_is_distinct_from_is_two_valued_outside_join_on(self) -> None:
        sql = " ".join((await emit_lib(lib_query(filters=["num_txt IS DISTINCT FROM 99"]), dialect="clickhouse")).split())
        assert "IS NOT DISTINCT FROM" not in sql.upper(), sql
        assert "<=>" not in sql, sql
        assert "IS NULL" in sql.upper(), sql

    async def test_cast_target_is_nullable(self) -> None:
        sql = await emit_lib(lib_query("qty || '#'"), dialect="clickhouse")
        assert "CAST(lib.qty AS Nullable(String))" in sql, sql

    @pytest.mark.parametrize("expr", ["CAST(word AS BOOLEAN)", "TRY_CAST(word AS BOOLEAN)"])
    async def test_boolean_reads_as_zero_or_one(self, expr: str) -> None:
        sql = await emit_lib(lib_query(expr), dialect="clickhouse")
        assert "toUInt8(" in sql, sql
