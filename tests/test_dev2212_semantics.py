"""Inferred-query semantics: compositionality, filters restricting every home, unrelated datasets and spine queries
(spec: queries/semantics, queries/unrelated-datasets, queries/time-spine)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2212_fixtures import BACKENDS, DS, kinds, rows, run, seed_datasets, seed_engine, table
from tests._engine_helpers import seeded_exec_engine

R, REV, RET, TIX = "customers.region", "orders.total_revenue", "returns.total_returns", "tickets.ticket_count"
SPINE_MONTH = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-03-31"]}
MONTHS = ("2025-01", "2025-02", "2025-03")


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def test_adding_a_measure_to_an_inferred_query_only_adds_rows(backend: str) -> None:
    async with seed_engine(backend) as eng:
        small = await run(eng, dimensions=[R], metrics=[REV])
        large = await run(eng, dimensions=[R], metrics=[REV, RET])
    assert table(small, R, REV) == rows(("EAST", 350), ("WEST", 300))
    assert table(large, R, REV) == rows(("EAST", 350), ("WEST", 300), ("NORTH", None))


# --------------------------------------------------------------------------- #
# Filters restrict every home of an inferred query
# --------------------------------------------------------------------------- #
async def test_one_filter_two_homes(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, RET], filter=["orders.amount > 100"])
    assert table(resp, R, REV, RET) == rows(("EAST", 200, 25), ("WEST", 300, None))


async def test_a_filter_on_a_dataset_no_metric_is_homed_on(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV], filter=["visits.visit_id is not null"])
    assert table(resp, R, REV) == rows(("WEST", 300),)
    assert resp.population == "customers"


VIP = TableSpec(name="vip", columns=[("customer_id", "TEXT")], rows=[("C1",), ("C4",)])
VIP_DATASET = SlayerDataset(name="vip", sql_table="vip", data_source=DS,
                            fields=[DatasetField(name="customer_id", datatype=DataType.STRING, primary_key=True)])


async def test_a_query_declared_inner_join_restricts_each_home(backend: str) -> None:
    extension = {"dataset": "customers", "relationships": [
        {"to": "vip", "from_columns": ["customer_id"], "to_columns": ["customer_id"], "join_type": "inner"}]}
    async with seed_engine(backend, extra_tables=(VIP,), datasets=seed_datasets(extra=(VIP_DATASET,))) as eng:
        resp = await run(eng, extensions=[extension], dimensions=[R], metrics=[REV, RET])
    assert table(resp, R, REV, RET) == rows(("EAST", 300, 25), ("NORTH", None, 10))


# --------------------------------------------------------------------------- #
# Unrelated datasets
# --------------------------------------------------------------------------- #
async def test_an_unrelated_metric_alone_supplies_the_anchors_cells(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[TIX])
    assert table(resp, R, TIX) == rows(("EAST", 3), ("WEST", 3), ("NORTH", 3))


async def test_a_filter_on_the_unrelated_dataset_in_an_inferred_query(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=["count(customers.*)", TIX], filter=["tickets.priority = 'none'"])
    assert resp.data == []


async def test_dimensionless_inferred_query_over_unrelated_homes(backend: str) -> None:
    async with seed_engine(backend) as eng:
        disconnected = await run(eng, metrics=["count(customers.*)", TIX])
        connected = await run(eng, metrics=["count(customers.*)", "count_distinct(customers.region)"])
    entries = kinds(disconnected, "unrelated_dataset")
    assert {w.dataset for w in entries} == {"customers", "tickets"}
    assert len(entries) == 2
    assert all(w.population is None for w in entries)
    assert not kinds(connected, "unrelated_dataset")


# --------------------------------------------------------------------------- #
# Spine queries
# --------------------------------------------------------------------------- #
def _spine_rows(resp) -> list[tuple]:
    value = next(c for c in resp.columns if c not in (R, "time_spine.timestamp"))
    return rows(*((r[R], str(r["time_spine.timestamp"])[:7], r[value]) for r in resp.data))


async def test_a_filter_on_a_finer_dataset_keeps_p(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], time_dimensions=[SPINE_MONTH], metrics=["sum(orders.amount)"],
                         filter=["orders.amount > 60"])
    assert resp.population == "time_spine × customers"
    expected = {("EAST", "2025-01"): 100, ("EAST", "2025-02"): 200, ("WEST", "2025-02"): 300}
    assert _spine_rows(resp) == rows(*((reg, m, expected.get((reg, m))) for reg in ("EAST", "WEST") for m in MONTHS))


async def test_an_orphan_beside_the_spine_is_not_yet_a_row(backend: str) -> None:
    async with seed_engine(backend, orphan=True) as eng:
        resp = await run(eng, dimensions=[R], time_dimensions=[SPINE_MONTH], metrics=["sum(orders.amount)"])
    assert {r[R] for r in resp.data} == {"EAST", "WEST", "NORTH"}
    assert resp.population == "time_spine × customers"


async def test_a_spine_query_over_a_product_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, "tickets.priority"], time_dimensions=[SPINE_MONTH])
    assert resp.row_count == 3 * 2 * 3
    assert resp.population == "time_spine × customers × tickets"
    assert resp.row_sources == ["customers", "tickets"]


# --------------------------------------------------------------------------- #
# Transforms over result rows (target: DEV-2218)
# --------------------------------------------------------------------------- #
DAYS = TableSpec(name="days", columns=[("day", "DATE")], rows=[("2025-01-05",), ("2025-02-05",), ("2025-03-05",)])
SALES = TableSpec(name="sales", columns=[("sale_id", "TEXT"), ("day", "DATE"), ("amount", "DOUBLE")],
                  rows=[("S1", "2025-01-05", 10.0), ("S2", "2025-03-05", 30.0)])
REFUNDS = TableSpec(name="refunds", columns=[("refund_id", "TEXT"), ("day", "DATE"), ("amount", "DOUBLE")],
                    rows=[("F1", "2025-02-05", 5.0)])


def _dated(name: str, pk: str) -> SlayerDataset:
    return SlayerDataset(
        name=name, sql_table=name, data_source=DS,
        fields=[DatasetField(name=pk, datatype=DataType.STRING, primary_key=True),
                DatasetField(name="day", datatype=DataType.DATE), DatasetField(name="amount", datatype=DataType.FLOAT)],
        relationships=[DatasetRelationship(to="days", from_columns=["day"], to_columns=["day"])],
    )


DATED = [SlayerDataset(name="days", sql_table="days", data_source=DS,
                       fields=[DatasetField(name="day", datatype=DataType.DATE, primary_key=True)]),
         _dated("sales", "sale_id"), _dated("refunds", "refund_id")]


@pytest.mark.xfail(strict=True, reason="transforms evaluate over result rows until DEV-2218")
async def test_lag_keeps_its_values_when_a_sibling_adds_rows(backend: str) -> None:
    month = [{"field": "days.day", "granularity": "month"}]
    lag = {"metric": "lag(sum(sales.amount))", "alias": "lag"}
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, [DAYS, SALES, REFUNDS]),
                                  datasets=DATED) as (eng, _):
        alone = await run(eng, time_dimensions=month, metrics=[lag])
        both = await run(eng, time_dimensions=month, metrics=[lag, {"metric": "sum(refunds.amount)", "alias": "f"}])
    by_month = {str(r["days.day"])[:7]: r["lag"] for r in both.data}
    for r in alone.data:
        assert by_month[str(r["days.day"])[:7]] == r["lag"]


@pytest.mark.xfail(strict=True, reason="transforms evaluate over result rows until DEV-2218")
async def test_percent_rank_keeps_its_values_when_a_sibling_adds_rows(backend: str) -> None:
    pr = {"metric": "percent_rank(count(orders.*))", "alias": "pr"}
    async with seed_engine(backend) as eng:
        alone = await run(eng, dimensions=[R], metrics=[pr])
        both = await run(eng, dimensions=[R], metrics=[pr, RET])
    by_region = {r[R]: r["pr"] for r in both.data}
    for r in alone.data:
        assert by_region[r[R]] == r["pr"]
