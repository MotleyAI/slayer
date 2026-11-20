"""An aggregation reads its grain dimensions and its filters from its home (spec: queries/references › The anchor
is reached by its only to-one path; queries/population › Ossie's home routing answers a query the population could
not route).

Schema, every edge many-to-one: ``orders → customers → regions``, ``regions → managers``, ``orders → managers``::

    regions (region_id, name, manager_id): r1 North m1 | r2 South m2
    customers (customer_id, region_id, segment): C1 r1 retail | C2 r1 corporate | C3 r2 retail
    orders (order_id, customer_id, manager_id, amount): O1 C1 m2 100 | O2 C2 m1 50 | O3 C3 m1 300

Through ``customers`` North is 150 and South 300; the detour through ``managers`` would give North 350, South 100.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import UnresolvableDimensionRelationshipError
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2212_fixtures import BACKENDS, DS, rows, run, table
from tests._engine_helpers import seeded_exec_engine

TABLES = [
    TableSpec(name="regions", columns=[("region_id", "TEXT"), ("name", "TEXT"), ("manager_id", "TEXT")],
              rows=[("r1", "North", "m1"), ("r2", "South", "m2")]),
    TableSpec(name="managers", columns=[("manager_id", "TEXT"), ("name", "TEXT")],
              rows=[("m1", "mia"), ("m2", "max")]),
    TableSpec(name="customers", columns=[("customer_id", "TEXT"), ("region_id", "TEXT"), ("segment", "TEXT")],
              rows=[("C1", "r1", "retail"), ("C2", "r1", "corporate"), ("C3", "r2", "retail")]),
    TableSpec(name="orders", columns=[("order_id", "TEXT"), ("customer_id", "TEXT"), ("manager_id", "TEXT"),
                                      ("amount", "DOUBLE")],
              rows=[("O1", "C1", "m2", 100.0), ("O2", "C2", "m1", 50.0), ("O3", "C3", "m1", 300.0)]),
]


def _rel(to: str, col: str) -> DatasetRelationship:
    return DatasetRelationship(to=to, from_columns=[col], to_columns=[col],
                               cardinality=RelationshipCardinality.MANY_TO_ONE)


def _ds(name: str, pk: str, *cols: str, rels: tuple[DatasetRelationship, ...] = (), **extra) -> SlayerDataset:
    fields = [DatasetField(name=pk, datatype=DataType.STRING, primary_key=True),
              *(DatasetField(name=c, datatype=DataType.FLOAT if c == "amount" else DataType.STRING) for c in cols)]
    return SlayerDataset(name=name, sql_table=name, data_source=DS, fields=fields, relationships=list(rels), **extra)


DATASETS = [
    _ds("regions", "region_id", "name", "manager_id", rels=(_rel("managers", "manager_id"),)),
    _ds("managers", "manager_id", "name"),
    _ds("customers", "customer_id", "region_id", "segment", rels=(_rel("regions", "region_id"),)),
    _ds("orders", "order_id", "customer_id", "manager_id", "amount",
        rels=(_rel("customers", "customer_id"), _rel("managers", "manager_id")),
        metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)")]),
]


@pytest.fixture(params=BACKENDS)
async def engine(request: pytest.FixtureRequest):
    backend = request.param
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, TABLES),
                                  datasets=DATASETS) as (eng, _):
        yield eng


@pytest.mark.parametrize("population", [None, "regions"])
async def test_an_aggregation_reads_its_grain_from_its_home(engine, population) -> None:
    resp = await run(engine, **({"population": population} if population else {}),
                     dimensions=["regions.name"], metrics=["orders.total_revenue"])
    assert table(resp, "regions.name", "orders.total_revenue") == rows(("North", 150), ("South", 300))


async def test_a_named_population_routes_a_home_spelled_in_another_case(engine) -> None:
    resp = await run(engine, population="regions", dimensions=["regions.name"],
                     metrics=[{"metric": "sum(ORDERS.amount)", "alias": "rev"}])
    assert table(resp, "regions.name", "rev") == rows(("North", 150), ("South", 300))


async def test_a_filter_restricting_a_home_is_routed_from_that_home(engine) -> None:
    resp = await run(engine, dimensions=["regions.name"], metrics=["orders.total_revenue"],
                     filter=["customers.segment = 'retail'"])
    assert table(resp, "regions.name", "orders.total_revenue") == rows(("North", 100), ("South", 300))


@pytest.mark.parametrize("condition", ["customers.segment = 'retail'", "orders.amount > 100"])
async def test_a_named_population_refuses_a_filter_it_cannot_route(engine, condition) -> None:
    # regions reaches customers and orders by two routes, neither to-one: the restriction is ambiguous.
    with pytest.raises(UnresolvableDimensionRelationshipError):
        await run(engine, population="regions", dimensions=["regions.name"], metrics=["orders.total_revenue"],
                  filter=[condition])


async def test_a_named_population_planned_on_its_home_keeps_its_datasource(engine, tmp_path) -> None:
    # A higher-priority datasource also holds an `orders`; the stage stays on the population's datasource.
    path = str(tmp_path / "other.duckdb")
    seed_backend("duckdb", path, [TableSpec(name="orders", columns=TABLES[3].columns, rows=[])])
    await engine.storage.save_datasource(DatasourceConfig(name="other", type="duckdb", database=path))
    await engine.storage.save_dataset(DATASETS[3].model_copy(update={"data_source": "other", "relationships": []}))
    await engine.storage.set_datasource_priority(["other", DS])
    resp = await run(engine, population="regions", dimensions=["regions.name"], metrics=["sum(orders.amount)"])
    assert table(resp, "regions.name", "orders.amount_sum") == rows(("North", 150), ("South", 300))


_VIA_SOURCE, _AT_MANAGERS = "customers.regions.managers.name", "managers.name"


@pytest.mark.parametrize("dimensions", [
    pytest.param([_VIA_SOURCE, _AT_MANAGERS], marks=pytest.mark.xfail(
        strict=True, reason="DEV-2388: the first spelling of a column decides its anchor")),
    [_AT_MANAGERS, _VIA_SOURCE],
])
async def test_each_spelling_of_a_column_is_routed_from_the_home(engine, dimensions) -> None:
    # `managers.name` is anchored at managers: from the home orders it has two to-one paths, whatever the order.
    with pytest.raises(UnresolvableDimensionRelationshipError):
        await run(engine, population="customers", dimensions=dimensions, metrics=["sum(customers.orders.amount)"])
