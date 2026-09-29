"""Aggregates are virtual models: an attached aggregate equals the same aggregate
materialised on its own, NULL grain cell included, however the query is rooted.
Executed on SQLite + DuckDB.

Spec: queries/semantics — "Aggregates are virtual models"; queries/attribution-modes —
"Distinct-entity association over the virtual model".
"""

from __future__ import annotations

import pytest

from tests._dev1840_fixtures import make_exec_engine
from tests._dev1995_fixtures import (
    APP_SPEND_BY_STATUS,
    COUNT_BY_CITY,
    COUNT_BY_PLAN,
    COUNT_BY_REGION,
    ORDER_KEY_STATUS_ORDER,
    ORPHAN_NULL_REGION_COUNT,
    ORPHAN_NULL_REGION_SUM,
    SPEND_BY_STATUS,
    STORE_COUNT_BY_CITY,
    STORE_SUM_BY_CITY,
    SUM_BY_CITY,
    SUM_BY_PLAN,
    SUM_BY_REGION,
    assert_parity,
    cells,
    make_app_null_engine,
    make_vm_engine,
    materialise,
    q,
)
from tests._dev1910_fixtures import make_null_status_engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def base_engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def vm_engine(request):
    async for e in make_vm_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def null_engine(request):
    async for e in make_null_status_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def app_null_engine(request):
    async for e in make_app_null_engine(request):
        yield e


def _cross(prefix: str = "") -> list:
    return [{"formula": f"sum({prefix}orders.amount)", "name": "s"},
            {"formula": f"count({prefix}orders.id)", "name": "n"}]


LOCAL = [{"formula": "amount:sum", "name": "s"}, {"formula": "id:count", "name": "n"}]


def _assert_cells(actual: dict, expected: dict) -> None:
    assert set(actual) == set(expected)
    for key, value in expected.items():
        if value is None:
            assert actual[key] is None, key
        else:
            assert float(actual[key]) == pytest.approx(value), key


class TestOrphanNullCell:
    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    async def test_null_region_cell_holds_orphans(self, base_engine, mode):
        """The NULL-region cell holds c4's order plus the customerless order: 47 / 2."""
        resp = await base_engine.execute(q({
            "source_model": "customers", "dimensions": ["regions.name"],
            "measures": _cross(), "to_many_handling": mode}))
        dim = "customers.regions.name"
        assert float(cells(resp, dim, "customers.s")[None]) == pytest.approx(
            ORPHAN_NULL_REGION_SUM)
        assert cells(resp, dim, "customers.n")[None] == ORPHAN_NULL_REGION_COUNT

    async def test_inferred_population(self, base_engine):
        resp = await base_engine.execute(q({
            "dimensions": ["customers.regions.name"], "measures": _cross()}))
        assert resp.population == "customers"
        dim = "customers.regions.name"
        assert float(cells(resp, dim, "customers.s")[None]) == pytest.approx(
            ORPHAN_NULL_REGION_SUM)
        assert cells(resp, dim, "customers.n")[None] == ORPHAN_NULL_REGION_COUNT


def _case(payload, dim, grain, population, source, id_):
    return pytest.param({"payload": payload, "dim": dim, "grain": grain,
                         "population": population, "source": source}, id=id_)


PARITY_CASES = [
    _case({"source_model": "customers", "dimensions": ["plan_code"], "measures": _cross()},
          "customers.plan_code", "customers.plan_code",
          (SUM_BY_PLAN, COUNT_BY_PLAN), (SUM_BY_PLAN, COUNT_BY_PLAN), "single-hop-dangling"),
    _case({"source_model": "customers", "dimensions": ["regions.name"], "measures": _cross()},
          "customers.regions.name", "customers.regions.name",
          (SUM_BY_REGION, COUNT_BY_REGION), (SUM_BY_REGION, COUNT_BY_REGION), "multi-hop-grain"),
    _case({"source_model": "regions", "dimensions": ["name"], "measures": _cross("customers.")},
          "regions.name", "customers.regions.name",
          (SUM_BY_REGION, COUNT_BY_REGION), (SUM_BY_REGION, COUNT_BY_REGION),
          "multi-hop-source-null-name"),
    _case({"source_model": "stores", "dimensions": ["city"], "measures": _cross()},
          "stores.city", "stores.city",
          (STORE_SUM_BY_CITY, STORE_COUNT_BY_CITY), (SUM_BY_CITY, COUNT_BY_CITY),
          "partial-composite"),
]


def _local(grain: str) -> dict:
    return {"source_model": "orders", "dimensions": [grain], "measures": LOCAL}


class TestParityWithMaterialisedAggregate:
    @pytest.mark.parametrize("mode", ["broadcast", "associate"])
    @pytest.mark.parametrize("case", PARITY_CASES)
    async def test_population_rooted(self, vm_engine, mode, case):
        """Every population cell, NULL included, equals the materialised row (or the empty value)."""
        vm = await materialise(vm_engine, name="vm", payload=_local(case["grain"]))
        resp = await vm_engine.execute(q({**case["payload"], "to_many_handling": mode}))
        root = case["payload"]["source_model"]
        s = cells(resp, case["dim"], f"{root}.s")
        n = cells(resp, case["dim"], f"{root}.n")
        assert_parity(s, vm["s"], empty=None)
        assert_parity(n, vm["n"], empty=0)
        _assert_cells(s, case["population"][0])
        _assert_cells(n, case["population"][1])

    @pytest.mark.parametrize("case", PARITY_CASES)
    async def test_source_rooted(self, vm_engine, case):
        """Rooted at the aggregate's source, the NULL cell holds every broken-path row."""
        grain = case["grain"]
        vm = await materialise(vm_engine, name="vm", payload=_local(grain))
        resp = await vm_engine.execute(q(_local(grain)))
        s = cells(resp, f"orders.{grain}", "orders.s")
        n = cells(resp, f"orders.{grain}", "orders.n")
        assert None in s
        assert_parity(s, vm["s"], empty=None)
        assert_parity(n, vm["n"], empty=0)
        _assert_cells(s, case["source"][0])
        _assert_cells(n, case["source"][1])


SPEND = {"formula": "customers.spend:sum", "name": "c"}
AMOUNT = {"formula": "amount:sum", "name": "a"}


def _orders_assoc(**kw) -> dict:
    return {"source_model": "orders", "to_many_handling": "associate",
            "dimensions": ["status"], **kw}


class TestSpellingInvariantAssociation:
    @pytest.mark.parametrize("formula", [
        "customers.spend:sum",
        "customers.spend:sum(partition_by=status)",
        "sum(customers.spend, partition_by=status)",
    ])
    async def test_orders_rooted_null_cell_holds_the_orderless_customer(
            self, null_engine, formula):
        """NULL-status cell = c1 (owner) + c7 (no orders) = 155 in every measure spelling."""
        resp = await null_engine.execute(q(_orders_assoc(
            measures=[{"formula": formula, "name": "c"}])))
        _assert_cells(cells(resp, "orders.status", "orders.c"), SPEND_BY_STATUS)

    async def test_customers_rooted_twin_agrees(self, null_engine):
        resp = await null_engine.execute(q({
            "source_model": "customers", "to_many_handling": "associate",
            "dimensions": ["orders.status"],
            "measures": [{"formula": "spend:sum", "name": "c"}]}))
        _assert_cells(cells(resp, "customers.orders.status", "customers.c"),
                      SPEND_BY_STATUS)

    async def test_filter_only_reads_the_same_null_cell(self, null_engine):
        """Only the NULL cell (155) lies in (120, 200)."""
        resp = await null_engine.execute(q(_orders_assoc(
            measures=[AMOUNT],
            filters=["customers.spend:sum > 120", "customers.spend:sum < 200"])))
        assert [r["orders.status"] for r in resp.data] == [None]

    async def test_order_only_reads_the_same_null_cell(self, null_engine):
        """|spend − 200|: NULL 45, new 90, ok 220."""
        resp = await null_engine.execute(q(_orders_assoc(
            measures=[AMOUNT],
            order=[{"column": "abs(customers.spend:sum - 200)", "direction": "asc"}])))
        assert [r["orders.status"] for r in resp.data] == ORDER_KEY_STATUS_ORDER


class TestRowFilterNarrowsTheVirtualModel:
    async def test_orderless_customer_fails_the_row_filter(self, app_null_engine):
        """channel='app': NULL-status cell = c5 only (80), never c7's null-extended row (135)."""
        resp = await app_null_engine.execute(q(_orders_assoc(
            measures=[SPEND], filters=["channel = 'app'"])))
        _assert_cells(cells(resp, "orders.status", "orders.c"), APP_SPEND_BY_STATUS)
