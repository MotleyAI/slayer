"""Derived fields and dataset metrics read converted values; the predicates defining a dataset read stored values
(spec: queries/time-zones; design D8)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetMetric
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy

from tests._dev2000_fixtures import (
    BACKENDS,
    NY,
    ROWS,
    TOKYO,
    amount,
    d,
    day,
    default_datasets,
    field,
    m,
    naive,
    orders_dataset,
    q,
    tz_engine,
    utc,
    warnings_of,
)

ORDER_DATE = field("order_date", DataType.DATE, expression="CAST(ordered_at AS DATE)")
LATE_ORDERS = DatasetMetric(name="late_orders", expression="count(*) FILTER (WHERE hour(ordered_at) >= 18)")
DAY_PART = field("day_part", DataType.INTEGER, expression="hour(ordered_at)")
PAID_AMOUNT = field("paid_amount", DataType.FLOAT, expression="amount", filter="day_part >= 6")
CREATED_PLUS = field("created_plus", DataType.DATETIME, expression="date_add(created_raw, 1, 'hour')")
NO_READ = field("fixed_at", DataType.DATETIME, expression="CAST('2025-01-01 00:00:00' AS TIMESTAMP)")


def _datasets(*extra_fields, metrics=(), filters=()):
    return default_datasets(orders_dataset(extra_fields=extra_fields, metrics=metrics, filters=filters))


@pytest.mark.parametrize("backend", BACKENDS)
class TestValueContext:
    async def test_derived_local_date_equals_its_inline_expression(self, backend) -> None:
        async with tz_engine(backend, datasets=_datasets(ORDER_DATE)) as engine:
            derived = await engine.execute(q(time_zone=NY, dimensions=["orders.order_date"],
                                             metrics=[m("count(orders.*)", "n")]))
            inline = await engine.execute(q(time_zone=NY, dimensions=[d("CAST(orders.ordered_at AS DATE)", "order_date")],
                                            metrics=[m("count(orders.*)", "n")]))
        got = {day(r["orders.order_date"]) for r in derived.data}
        assert got == {day(r["order_date"]) for r in inline.data}
        assert got == {date(2024, 12, 31), date(2025, 1, 1), date(2025, 1, 31), date(2025, 3, 9)}

    async def test_dataset_metric_filter_converts(self, backend) -> None:
        async with tz_engine(backend, datasets=_datasets(metrics=[LATE_ORDERS])) as engine:
            saved = await engine.execute(q(time_zone=NY, metrics=["orders.late_orders"]))
            inline = await engine.execute(q(time_zone=NY, metrics=[
                m("count(orders.*) FILTER (WHERE hour(orders.ordered_at) >= 18)", "late")]))
        assert int(saved.data[0]["orders.late_orders"]) == 2
        assert int(inline.data[0]["late"]) == 2

    async def test_an_unmapped_column_is_read_as_stored_and_warns(self, backend) -> None:
        async with tz_engine(backend, datasets=_datasets(CREATED_PLUS)) as engine:
            with pytest.warns(UserWarning, match="created_raw"):
                resp = await engine.execute(q(time_zone=NY, dimensions=["orders.id", "orders.created_plus"]))
        got = {int(r["orders.id"]): naive(r["orders.created_plus"]) for r in resp.data}
        assert got == {i: utc(i) + timedelta(hours=1) for i in ROWS}
        [w] = warnings_of(resp)
        assert "created_raw" in w.human_message()

    async def test_a_derived_timestamp_from_no_read_is_civil(self, backend) -> None:
        stage = {"name": "s1", "population": "orders", "time_zone": NY,
                 "dimensions": ["orders.id", "orders.fixed_at"]}
        final = {"population": "s1", "time_zone": TOKYO, "dimensions": ["s1.id", "s1.fixed_at"]}
        async with tz_engine(backend, datasets=_datasets(NO_READ)) as engine:
            resp = await engine.execute([stage, final])
        assert {naive(r["s1.fixed_at"]) for r in resp.data} == {naive("2025-01-01 00:00:00")}
        assert not warnings_of(resp)


@pytest.mark.parametrize("backend", BACKENDS)
class TestStoredPredicateContext:
    @pytest.mark.parametrize("zone", [None, NY, TOKYO])
    async def test_a_dataset_filter_is_zone_independent(self, backend, zone) -> None:
        extra = {} if zone is None else {"time_zone": zone}
        async with tz_engine(backend, datasets=_datasets(filters=["ordered_at >= '2025-01-01 04:00'"])) as engine:
            resp = await engine.execute(q(metrics=[m("count(orders.*)", "n")], **extra))
        assert int(resp.data[0]["n"]) == 3

    async def test_a_field_filter_through_a_derived_field_reads_stored_hours(self, backend) -> None:
        async with tz_engine(backend, datasets=_datasets(DAY_PART, PAID_AMOUNT)) as engine:
            resp = await engine.execute(q(time_zone=NY, metrics=[m("sum(orders.paid_amount)", "s")]))
        assert float(resp.data[0]["s"]) == sum(amount(i) for i in ROWS if utc(i).hour >= 6) == 90.0

    async def test_the_derived_field_itself_still_converts(self, backend) -> None:
        async with tz_engine(backend, datasets=_datasets(DAY_PART, PAID_AMOUNT)) as engine:
            resp = await engine.execute(q(time_zone=NY, dimensions=["orders.id", "orders.day_part"]))
        assert {int(r["orders.id"]): int(r["orders.day_part"]) for r in resp.data} == {1: 22, 2: 1, 3: 18, 4: 1}

    @pytest.mark.parametrize("zone", [None, TOKYO])
    async def test_row_level_security_is_zone_independent(self, backend, zone) -> None:
        policy = SessionPolicy(ruleset=ColumnFilterRuleset(column="ordered_at", value="2025-01-01 03:30:00"))
        extra = {} if zone is None else {"time_zone": zone}
        async with tz_engine(backend, policy=policy) as engine:
            resp = await engine.execute(q(metrics=[m("count(orders.*)", "n")], **extra))
        assert int(resp.data[0]["n"]) == 1, resp.sql
