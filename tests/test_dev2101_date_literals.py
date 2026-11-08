"""SQL interval literals, typed date literals and clock keywords (spec: queries/date-functions)."""

from __future__ import annotations

import pytest

from slayer.core.errors import TimeLiteralError
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from tests._dev2101_fixtures import EXEC_BACKENDS, TIER1, by_key, emit, engine_for, ids, make_exec_engine, q


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


async def _sql(filters: list[str], **kw) -> str:
    return await emit(q(dimensions=["id"], filters=filters, **kw), dialect="postgres")


async def _same_sql(a: str, b: str) -> None:
    assert await _sql([a]) == await _sql([b]), (a, b)


class TestIntervalLiterals:
    @pytest.mark.parametrize("literal", ["INTERVAL '7 days'", "INTERVAL '7' DAY", "INTERVAL 7 DAY",
                                         "interval '7 day'", "INTERVAL '7' days"])
    async def test_literal_equals_date_add(self, literal: str) -> None:
        await _same_sql(f"order_date + {literal} > CURRENT_DATE", "date_add(order_date, 7, 'day') > current_date()")

    async def test_subtraction(self) -> None:
        await _same_sql("created_at - INTERVAL '2 hours' < now()", "date_add(created_at, -2, 'hour') < now()")

    async def test_commuted_addition(self) -> None:
        await _same_sql("INTERVAL '7 days' + order_date > CURRENT_DATE", "date_add(order_date, 7, 'day') > current_date()")

    async def test_chain_applies_left_to_right(self) -> None:
        await _same_sql("created_at + INTERVAL '1 month' + INTERVAL 2 HOUR < now()",
                        "date_add(date_add(created_at, 1, 'month'), 2, 'hour') < now()")

    @pytest.mark.parametrize(("literal", "unit"), [
        ("INTERVAL '3 weeks'", "week"), ("INTERVAL '1 MONTH'", "month"), ("INTERVAL 2 years", "year"),
        ("INTERVAL '90' MINUTE", "minute"), ("INTERVAL '1 quarter'", "quarter"),
    ])
    async def test_units(self, literal: str, unit: str) -> None:
        n = literal.split("'")[1].split()[0] if "'" in literal else literal.split()[1]
        await _same_sql(f"created_at + {literal} < now()", f"date_add(created_at, {n}, '{unit}') < now()")

    async def test_executes(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["order_date + INTERVAL '7 days' > DATE '2024-03-01'"]))
        assert ids(resp) == {4, 5}

    async def test_multi_unit_literal_suggests_a_chain(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["id"], filters=["order_date + INTERVAL '1 day 2 hours' > now()"])
        with pytest.raises(ValueError) as info:
            await engine.execute(query)
        assert "+ INTERVAL '1 day' + INTERVAL '2 hours'" in str(info.value), str(info.value)

    @pytest.mark.parametrize("filt", [
        "INTERVAL '7 days' > order_date",
        "INTERVAL '7 days' - order_date > 0",
        "-INTERVAL '7 days' + order_date > order_date",
        "order_date + INTERVAL '1 day' * 2 > order_date",
        "date_part('day', INTERVAL '1 day') > 0",
        "order_date + (INTERVAL '1 day' + INTERVAL '1 day') > order_date",
    ])
    async def test_stray_interval_literal_points_to_date_add(self, engine: SlayerQueryEngine, filt: str) -> None:
        query = q(dimensions=["id"], filters=[filt])
        with pytest.raises(ValueError) as info:
            await engine.execute(query)
        assert "date_add" in str(info.value), str(info.value)

    @pytest.mark.parametrize("literal", ["INTERVAL 'x days'", "INTERVAL '1.5 days'", "INTERVAL '7 fortnights'"])
    async def test_bad_count_or_unit(self, engine: SlayerQueryEngine, literal: str) -> None:
        query = q(dimensions=["id"], filters=[f"order_date + {literal} > order_date"])
        with pytest.raises(ValueError):
            await engine.execute(query)

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_emits_everywhere(self, dialect: str) -> None:
        await emit(q(dimensions=["id", {"expression": "order_date + INTERVAL '1 month'", "name": "x"}],
                     filters=["created_at - INTERVAL 2 HOUR < CURRENT_TIMESTAMP"]), dialect=dialect)


class TestTypedLiterals:
    async def test_typed_literals_bind_and_execute(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            dimensions=["id", {"expression": "date_diff('day', DATE '2024-01-01', created_at)", "name": "x"}],
            filters=["created_at >= TIMESTAMP '2024-01-20 00:00'"],
        ))
        assert {int(k): int(v) for k, v in by_key(resp, "id", "x").items()} == {2: 19, 3: 40, 4: 55, 5: 74}

    async def test_typed_literal_equals_iso_literal_in_a_date_position(self) -> None:
        await _same_sql("order_date >= DATE '2024-02-25'", "order_date >= '2024-02-25'")

    @pytest.mark.parametrize("filt", ["created_at >= DATE '2024-02-30'", "created_at >= TIMESTAMP '2024-01-01 25:00'",
                                      "order_date >= DATE 'tomorrow'"])
    async def test_impossible_or_malformed_literal(self, engine: SlayerQueryEngine, filt: str) -> None:
        query = q(dimensions=["id"], filters=[filt])
        with pytest.raises(TimeLiteralError) as info:
            await engine.execute(query)
        literal = filt.split("'")[1]
        assert literal in str(info.value), str(info.value)

    async def test_typed_literal_outside_a_date_position(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": "DATE '2024-01-01'", "name": "d"}],
                                      filters=["id = 1"]))
        assert str(resp.data[0]["orders.d"])[:10] == "2024-01-01"


@pytest.fixture
def data_calls(monkeypatch) -> list[str]:
    calls: list[str] = []
    orig = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


class TestClockKeywords:
    @pytest.mark.parametrize(("keyword", "call"), [("CURRENT_DATE", "current_date()"), ("CURRENT_TIMESTAMP", "now()")])
    async def test_keyword_equals_call(self, keyword: str, call: str) -> None:
        await _same_sql(f"created_at >= {keyword}", f"created_at >= {call}")

    @pytest.mark.parametrize("keyword", ["CURRENT_DATE", "current_timestamp"])
    async def test_keyword_bypasses_the_result_cache(self, data_calls: list[str], keyword: str) -> None:
        async for eng in engine_for("sqlite"):
            query = q(dimensions=["id"], filters=[f"created_at <= {keyword}"])
            await eng.execute(query, cache=True)
            await eng.execute(query, cache=True)
            assert len([s for s in data_calls if "slayer_rk_" not in s]) == 2
            assert eng.cache_size == 0
