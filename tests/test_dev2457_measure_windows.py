"""A window with an aggregate-valued operand is a transform over its cells, on SQLite and DuckDB.

Spec: queries/window-functions › A window with an aggregate-valued operand is a transform over its cells.

Fixture rows: tests/_dev2457_fixtures.py. Totals of orders.amount —
  by region: EU 60, US 150, AS 70 | by day: 120, 70, 90 | by customer: 1 100, 2 70, 3 110, 4 none
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.errors import IllegalWindowInFilterError, QueryTypeError
from slayer.core.models import DatasetMetric
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev2243_fixtures import DS, dataset, document_engine, document_text, field, metric, relationship
from tests._dev2457_fixtures import BACKENDS, CUSTOMERS, DAYS, ORDERS, by, day, grouped, ints, orders_dataset, run, window_engine

REGION, DAY, CUSTOMER = "orders.region", "orders.ordered_at", "customers.id"
REVENUE = "sum(orders.amount)"
#: Rank of a cell by its revenue, at the query grain and at an explicit region grain.
RANK = "RANK() OVER (ORDER BY SUM(orders.amount) DESC)"
REGION_RANK = "RANK() OVER (ORDER BY sum(orders.amount, partition_by=orders.region) DESC)"
RUNNING = "SUM(SUM(orders.amount)) OVER (ORDER BY {key} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
RANK_OF_REGION = {"US": 1, "AS": 2, "EU": 3}
RANK_OF_CUSTOMER = {1: 2, 2: 3, 3: 1}
RUNNING_OF_CUSTOMER = {1: 100, 2: 170, 3: 280}
STORED_RANK = "RANK() OVER (ORDER BY SUM(amount) DESC)"


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
async def engine(backend: str) -> AsyncIterator[SlayerQueryEngine]:
    async with window_engine(backend) as eng:
        yield eng


def _cells(resp: SlayerResponse, value: str = "m") -> dict[Any, Any]:
    return ints(by(resp, key="d", value=value))


def _by_day(resp: SlayerResponse) -> dict[str | None, Any]:
    return {day(k): v for k, v in by(resp, key="d", value="m").items()}


def _query(*, dimension: str, metrics: dict[str, str], **extra: Any) -> dict[str, Any]:
    return {"dimensions": [{"field": dimension, "alias": "d"}],
            "metrics": [{"metric": text, "alias": alias} for alias, text in metrics.items()], **extra}


def _document(*metrics: dict[str, Any]) -> str:
    return document_text(
        datasets=[
            dataset("orders", primary_key=["id"], fields=[
                field("id", datatype="Integer"), field("region"), field("amount", datatype="Integer"),
                field("customer_id", datatype="Integer")]),
            dataset("customers", primary_key=["id"], fields=[field("id", datatype="Integer"), field("tier")]),
        ],
        relationships=[relationship("orders_to_customers", src="orders", to="customers", from_columns=["customer_id"],
                                    to_columns=["id"])],
        metrics=list(metrics),
    )


class TestScenarios:
    async def test_a_running_total_over_the_querys_cells(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, grouped("orders", DAY, RUNNING.format(key=DAY)))
        assert ints(_by_day(resp)) == dict(zip(DAYS, (120, 190, 280)))

    async def test_a_period_over_period_change(self, engine: SlayerQueryEngine) -> None:
        metric_text = "SUM(orders.amount) - LAG(SUM(orders.amount), 1) OVER (ORDER BY orders.ordered_at)"
        resp = await run(engine, grouped("orders", DAY, metric_text))
        assert ints(_by_day(resp)) == dict(zip(DAYS, (None, -50, 20)))

    async def test_a_rank_within_a_partition(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, {
            "population": "orders",
            "dimensions": [{"field": REGION, "alias": "r"}, {"field": DAY, "alias": "d"}],
            "metrics": [{"metric": "RANK() OVER (PARTITION BY orders.region ORDER BY SUM(orders.amount) DESC)",
                         "alias": "m"}],
        })
        got = {(row["r"], day(row["d"])): int(row["m"]) for row in resp.data}
        assert len(resp.data) == 7
        assert got == {**{(region, d): rank for region in ("EU", "US") for d, rank in zip(DAYS, (3, 2, 1))},
                       ("AS", DAYS[0]): 1}

    async def test_a_key_that_is_not_a_dimension_is_refused(self, engine: SlayerQueryEngine) -> None:
        body = grouped("orders", REGION, "SUM(SUM(orders.amount)) OVER (ORDER BY orders.ordered_at)")
        with pytest.raises(QueryTypeError, match=r"orders\.ordered_at") as exc:
            await run(engine, body)
        assert not isinstance(exc.value, IllegalWindowInFilterError)
        assert "dimension" in str(exc.value)

    async def test_extra_dimensions_do_not_partition_the_window(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, {
            "population": "orders",
            "dimensions": [{"field": REGION, "alias": "r"}, {"field": DAY, "alias": "d"}],
            "metrics": [{"metric": "SUM(SUM(orders.amount)) OVER ()", "alias": "m"}],
        })
        assert len(resp.data) == 7
        assert {int(row["m"]) for row in resp.data} == {280}

    async def test_the_window_runs_over_its_own_cells(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, grouped("customers", CUSTOMER, RANK))
        assert _cells(resp) == {1: 2, 2: 3, 3: 1, 4: None}

    async def test_a_measure_filter_does_not_change_the_window(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, grouped("orders", REGION, RANK, filter="sum(orders.amount) < 100"))
        assert _cells(resp) == {"AS": 2, "EU": 3}

    @pytest.mark.parametrize("door", ["engine", "storage"])
    async def test_a_stored_windowed_metric(self, engine: SlayerQueryEngine, door: str) -> None:
        orders = orders_dataset(metrics=[DatasetMetric(name="revenue_rank", expression=STORED_RANK)])
        if door == "engine":
            await engine.save_dataset(orders)
        else:
            await engine.storage.save_dataset(orders)
        saved = await engine.storage.get_dataset("orders", data_source="test")
        assert saved is not None
        assert [m.name for m in saved.metrics] == ["revenue_rank"]
        resp = await run(engine, grouped("orders", REGION, "orders.revenue_rank"))
        assert _cells(resp) == RANK_OF_REGION

    async def test_a_measure_level_window_as_an_aggregation_source(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, {"population": "orders", "metrics": [{"metric": f"max({REGION_RANK})", "alias": "m"}]})
        assert [int(row["m"]) for row in resp.data] == [3]


class TestPositions:
    async def test_as_an_order_key_of_a_grouped_query(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, grouped("orders", REGION, REVENUE, order=[{"field": RANK, "direction": "asc"}]))
        assert [(row["d"], int(row["m"])) for row in resp.data] == [("US", 150), ("AS", 70), ("EU", 60)]
        assert set(resp.data[0]) == {"d", "m"}

    async def test_as_a_measure_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, grouped("orders", REGION, REVENUE, filter=f"{RANK} = 1"))
        assert _cells(resp) == {"US": 150}

    async def test_as_an_aggregation_parameter(self, engine: SlayerQueryEngine) -> None:
        """Weights US 1, AS 2, EU 3: (150 * 1 + 70 * 2 + 60 * 3) / (3 * 1 + 1 * 2 + 3 * 3)."""
        resp = await run(engine, {"population": "orders", "metrics": [
            {"metric": f"weighted_avg(orders.amount, weight={REGION_RANK})", "alias": "m"}]})
        assert [float(row["m"]) for row in resp.data] == [pytest.approx(470 / 14)]

    async def test_in_dimension_position_with_an_explicit_grain(self, engine: SlayerQueryEngine) -> None:
        resp = await run(engine, {"population": "orders", **_query(dimension=REGION_RANK, metrics={"m": REVENUE})})
        assert _cells(resp) == {1: 150, 2: 70, 3: 60}

    async def test_in_dimension_position_without_an_explicit_grain_is_refused(self, engine: SlayerQueryEngine) -> None:
        body = {"population": "orders", **_query(dimension=RANK, metrics={"m": REVENUE})}
        with pytest.raises(QueryTypeError, match="partition_by") as exc:
            await run(engine, body)
        assert not isinstance(exc.value, IllegalWindowInFilterError)


class TestModelScopedMetric:
    @pytest.mark.parametrize("door", ["document", "edit"])
    async def test_a_model_scoped_windowed_metric_is_saved_and_evaluated(self, backend: str, door: str) -> None:
        windowed = metric("region_rank", RANK)
        stored = _document(windowed) if door == "document" else _document(metric("revenue", "SUM(orders.amount)"))
        async with document_engine(backend=backend, documents=(stored,), tables=[ORDERS, CUSTOMERS]) as engine:
            if door == "edit":
                await engine.storage.edit_semantic_model("sales", data_source=DS, metrics=[windowed])
            text = await engine.storage.get_semantic_model_text("sales", data_source=DS)
            resp = await run(engine, {"dimensions": [{"field": REGION, "alias": "d"}], "metrics": ["region_rank"]})
        assert text is not None
        assert "region_rank" in text
        assert _cells(resp, value="region_rank") == RANK_OF_REGION


class TestInvariance:
    async def test_a_sibling_measure_does_not_change_the_window(self, engine: SlayerQueryEngine) -> None:
        """Law 3: the sibling adds customer 4's cell, where the window reads NULL."""
        alone = await run(engine, _query(dimension=CUSTOMER, metrics={"m": RANK}))
        beside = await run(engine, _query(dimension=CUSTOMER, metrics={"m": RANK, "n": "count(customers.*)"}))
        assert _cells(alone) == RANK_OF_CUSTOMER
        assert _cells(beside) == {**RANK_OF_CUSTOMER, 4: None}
        assert _cells(beside, value="n") == {1: 1, 2: 1, 3: 1, 4: 1}

    async def test_an_anchored_sibling_measure_does_not_change_the_window(self, engine: SlayerQueryEngine) -> None:
        alone = await run(engine, grouped("orders", REGION, RANK))
        beside = await run(engine, {"population": "orders",
                                    **_query(dimension=REGION, metrics={"m": RANK, "n": "count(orders.*)"})})
        assert _cells(alone) == RANK_OF_REGION
        assert _cells(beside) == RANK_OF_REGION
        assert _cells(beside, value="n") == {"EU": 3, "US": 3, "AS": 1}

    async def test_naming_a_population_does_not_change_the_window(self, engine: SlayerQueryEngine) -> None:
        """Law 7: customer 4, a row only under the population ``customers``, is outside the window's domain."""
        body = _query(dimension=CUSTOMER, metrics={"m": RUNNING.format(key=CUSTOMER), "rk": RANK})
        inferred = await run(engine, body)
        of_orders = await run(engine, {"population": "orders", **body})
        of_customers = await run(engine, {"population": "customers", **body})
        for resp in (inferred, of_orders):
            assert _cells(resp) == RUNNING_OF_CUSTOMER
            assert _cells(resp, value="rk") == RANK_OF_CUSTOMER
        assert _cells(of_customers) == {**RUNNING_OF_CUSTOMER, 4: None}
        assert _cells(of_customers, value="rk") == {**RANK_OF_CUSTOMER, 4: None}
