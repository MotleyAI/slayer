"""Mode-B date functions executed on SQLite and DuckDB: the oracle matrix plus the spec scenarios."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from slayer.core.query import SlayerQuery
from tests._dev1737_fixtures import (
    BACKENDS,
    DT,
    DateCase,
    as_temporal,
    assert_case,
    assert_value,
    dt_dataset,
    exec_engine,
    matrix_cases,
    matrix_query,
    orders_dataset,
    customers_dataset,
    scenario_datasets,
    scenario_tables,
    sqlite_malformed_seed,
)
from tests._engine_helpers import seeded_exec_engine
from slayer.engine.query_engine import SlayerQueryEngine

CASES = matrix_cases()


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("case", CASES, ids=[c.case_id for c in CASES])
async def test_matrix(backend: str, case: DateCase) -> None:
    async with exec_engine(backend, tables=[DT], datasets=[dt_dataset()]) as engine:
        resp = await engine.execute(matrix_query(case))
    assert_case(resp.data, case)


@pytest.fixture(params=BACKENDS)
async def orders_engine(request):
    today = datetime.now(timezone.utc).date()
    async with exec_engine(request.param, tables=scenario_tables(today=today), datasets=scenario_datasets()) as engine:
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery.model_validate(kw)


async def _count(engine, *filters: str, **kw) -> int:
    resp = await engine.execute(_q(metrics=[{"metric": "count(*)", "alias": "n"}], filter=list(filters), **kw))
    return int(resp.data[0]["orders.n"])


async def _by_id(engine, expr: str, *, dataset: str = "orders") -> dict[int, object]:
    resp = await engine.execute(_q(population=dataset, dimensions=["id", {"field": expr, "alias": "v"}]))
    return {int(r[f"{dataset}.id"]): r[f"{dataset}.v"] for r in resp.data}


class TestSpecScenarios:
    async def test_extraction_dimension(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(
            dimensions=[{"field": "date_part('day_of_week', created_at)", "alias": "dow"}],
            metrics=[{"metric": "count(*)", "alias": "n"}],
        ))
        got = {int(r["orders.dow"]): int(r["orders.n"]) for r in resp.data}
        assert got == {5: 1, 7: 1, 1: 2, 3: 1}

    async def test_filter(self, orders_engine) -> None:
        assert await _count(orders_engine, "date_diff('day', created_at, shipped_at) > 3") == 3

    async def test_case_insensitive_names(self, orders_engine) -> None:
        assert await _count(orders_engine, "DATE_DIFF('DAY', created_at, shipped_at) > 3") == 3

    async def test_clock_calls(self, orders_engine) -> None:
        assert await _count(orders_engine, "created_at <= now()") == 5
        assert await _count(orders_engine, "order_date <= current_date()") == 5

    async def test_bucket_form_unchanged(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(dimensions=["date_trunc('month', created_at)"], metrics=[{"metric": "count(*)", "alias": "n"}]))
        key = next(k for k in resp.data[0] if k != "orders.n")
        assert {str(r[key])[:10] for r in resp.data} == {"2024-01-01", "2024-03-01", "2024-06-01", "2024-12-01"}

    async def test_unit_case_folds(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(metrics=[
            {"metric": "max(date_part('MONTH', created_at))", "alias": "a"},
            {"metric": "max(date_part('month', created_at))", "alias": "b"},
        ]))
        assert resp.data[0]["orders.a"] == resp.data[0]["orders.b"] == 12

    async def test_hour_of_a_date(self, orders_engine) -> None:
        assert set((await _by_id(orders_engine, "date_part('hour', order_date)")).values()) == {0}

    async def test_mixed_date_and_timestamp(self, orders_engine) -> None:
        assert (await _by_id(orders_engine, "date_diff('hour', order_date, created_at)"))[1] == 5

    async def test_computed_count(self, orders_engine) -> None:
        got = await _by_id(orders_engine, "date_add(order_date, sla_days, 'day')")
        assert_value(got[1], date(2024, 3, 3), where="sla 2.7")
        assert_value(got[2], date(2024, 2, 28), where="sla -2.7")
        assert got[3] is None

    @pytest.mark.parametrize("expr,expected", [
        ("date_add('2024-01-31', 1, 'month')", date(2024, 2, 29)),
        ("date_add('2024-03-31', -1, 'month')", date(2024, 2, 29)),
        ("date_add('2024-02-29', 1, 'year')", date(2025, 2, 28)),
        ("date_add(order_date, 2, 'hour')", datetime(2024, 3, 1, 2, 0)),
        ("date_add('2024-01-31 10:15:00', 1, 'month')", datetime(2024, 2, 29, 10, 15)),
        ("date_part('day_of_week', '2024-06-02')", 7),
        ("date_part('day_of_week', '2024-06-03')", 1),
        ("date_part('week', '2024-12-30')", 1),
        ("date_part('iso_year', '2024-12-30')", 2025),
        ("date_part('year', '2024-12-30')", 2024),
        ("date_diff('month', '2024-01-31', '2024-02-01')", 1),
        ("date_diff('month', '2024-02-01', '2024-02-29')", 0),
        ("date_diff('year', '2024-12-31', '2025-01-01')", 1),
        ("date_diff('month', '2024-02-01', '2024-01-31')", -1),
        ("date_diff('day', '2024-03-01 23:59:00', '2024-03-02 00:01:00')", 1),
        ("date_diff('week', '2024-06-02', '2024-06-03')", 1),
        ("date_diff('week_sunday', '2024-06-02', '2024-06-03')", 0),
        ("date_add('2024-01-31', 1, 'quarter')", date(2024, 4, 30)),
        ("'2024-01-31' + interval(1, 'month')", date(2024, 2, 29)),
    ])
    async def test_literal_scenarios(self, orders_engine, expr: str, expected) -> None:
        assert_value((await _by_id(orders_engine, expr))[1], expected, where=expr)

    async def test_aggregate_span(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(metrics=[
            {"metric": "date_diff('day', min(created_at), max(created_at))", "alias": "span"},
        ]))
        assert resp.data[0]["orders.span"] == 334

    async def test_coalesce_with_the_clock(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(metrics=[
            {"metric": "avg(date_diff('day', created_at, coalesce(shipped_at, now())))", "alias": "age"},
        ]))
        open_days = (datetime.now(timezone.utc).date() - date(2024, 6, 3)).days
        assert resp.data[0]["orders.age"] == pytest.approx((1 + 5 + 5 + 5 + open_days) / 5, abs=0.21)

    async def test_literal_anchor(self, orders_engine) -> None:
        assert await _count(orders_engine, "date_diff('day', '2024-01-01', created_at) < 30") == 0
        assert await _count(orders_engine, "date_diff('day', '2024-01-01', created_at) < 31") == 1

    async def test_literal_inside_coalesce(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(metrics=[
            {"metric": "min(date_part('year', coalesce(shipped_at, '2024-01-01')))", "alias": "y"},
        ]))
        assert resp.data[0]["orders.y"] == 2024
        assert (await _by_id(orders_engine, "date_part('year', coalesce(shipped_at, '2023-06-01'))"))[3] == 2023

    async def test_placeholder(self, orders_engine) -> None:
        n = await _count(orders_engine, "date_diff('day', '{launch}', created_at) >= 0", variables={"launch": "2024-05-01"})
        assert n == 3

    async def test_recent_orders(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(
            population="recent", metrics=[{"metric": "count(*)", "alias": "n"}],
            filter=["created_at >= date_add(current_date(), -30, 'day')"],
        ))
        assert int(resp.data[0]["recent.n"]) == 1

    async def test_clock_relative_sanity(self, orders_engine) -> None:
        got = await _by_id(orders_engine, "date_diff('day', created_at, now())", dataset="recent")
        assert got[1] in (0, 1, 2)
        assert got[2] in (39, 40, 41)
        today = await _by_id(orders_engine, "current_date()", dataset="recent")
        today_value = as_temporal(today[1])
        assert not isinstance(today_value, datetime)
        assert isinstance(today_value, date)
        assert abs((today_value - datetime.now(timezone.utc).date()).days) <= 1

    async def test_joined_operand(self, orders_engine) -> None:
        got = await _by_id(orders_engine, "date_diff('day', customers.signup_date, order_date)")
        assert got == {1: 61, 2: 61, 3: 95, 4: 305, 5: None}

    async def test_operator_equals_date_add(self, orders_engine) -> None:
        ops = await orders_engine.execute(_q(metrics=[{"metric": "max(shipped_at - interval(2, 'day'))", "alias": "m"}]))
        fn = await orders_engine.execute(_q(metrics=[{"metric": "max(date_add(shipped_at, -2, 'day'))", "alias": "m"}]))
        assert ops.sql == fn.sql
        assert_value(ops.data[0]["orders.m"], datetime(2025, 1, 2, 12, 0), where="interval")

    async def test_operator_auto_name_matches(self, orders_engine: SlayerQueryEngine) -> None:
        ops = await orders_engine.execute(_q(metrics=["max(shipped_at - interval(2, 'day'))"]))
        fn = await orders_engine.execute(_q(metrics=["max(date_add(shipped_at, -2, 'day'))"]))
        assert ops.columns == fn.columns

    async def test_chained_and_commuted_intervals(self, orders_engine) -> None:
        assert await _count(orders_engine, "created_at + interval(1, 'month') + interval(2, 'hour') < now()") == 5
        assert await _count(orders_engine, "interval(7, 'day') + order_date > current_date()") == 0

    async def test_date_functions_in_order_and_filters(self, orders_engine) -> None:
        resp = await orders_engine.execute(_q(
            dimensions=["id"], metrics=[{"metric": "count(*)", "alias": "n"}],
            filter=["date_part('year', created_at) = 2024"],
            order=[{"field": "max(date_diff('day', created_at, shipped_at))", "direction": "desc"}],
        ))
        assert [int(r["orders.id"]) for r in resp.data][-2:] == [1, 3]


async def test_sqlite_malformed_stored_date_is_null() -> None:
    async with seeded_exec_engine(
        dialect="sqlite", seed=sqlite_malformed_seed, datasets=[orders_dataset(), customers_dataset()],
    ) as (engine, _db):
        for expr in (
            "date_add(order_date, 1, 'month')", "date_part('year', order_date)",
            "date_diff('day', order_date, created_at)", "date_add(created_at, 1, 'hour')",
        ):
            got = await _by_id(engine, expr)
            assert got[9] is None, expr
            assert got[1] is not None, expr
