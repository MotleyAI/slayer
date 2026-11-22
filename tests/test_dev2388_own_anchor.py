"""An aggregation binds from its own anchor (spec: queries/semantics › An aggregation binds from its own anchor,
› Second-order aggregation over attached values; queries/references › The anchor is reached by its only to-one
path; queries/population › Population may be omitted; queries/multi-stage › Stages inherit edges on pass-through
keys).

Schemas (every relationship many-to-one):

* two-route — ``orders → customers → regions``, ``regions → managers``, ``orders → managers``::

      regions (region_id, name, manager_id): r1 North m1 | r2 South m2
      managers (manager_id, name): m1 mia | m2 max
      customers (customer_id, region_id, segment, weight): C1 r1 retail 3 | C2 r1 corporate 2 | C3 r2 retail 1
      orders (order_id, customer_id, manager_id, amount, ordered_at):
          O1 C1 m2 100 2025-01-10 | O2 C2 m1 50 2025-01-20 | O3 C3 m1 300 2025-03-05

  Through ``customers`` North is 150 and South 300; through ``managers`` North 350 and South 100.
* Appendix B — ``tests/_dev2212_fixtures.py``.
* stores — ``store_sales`` and ``store_returns`` each to ``item`` and ``date_dim``.
* multi-anchor — ``order_lines → orders``, ``order_lines → products``, ``orders`` and ``Tickets`` each to
  ``customers`` and ``managers``; ``widgets`` unrelated.
* diamond — ``orders → customers → regions``, ``orders → stores → regions``.
* geo — ``sales (city, region, a, b)``.
"""

from __future__ import annotations

import contextlib
import itertools
from collections.abc import AsyncGenerator
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import (
    PopulationInferenceError,
    UnresolvableDimensionRelationshipError,
    UnsafeRelationshipInputError,
)
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetMetric,
    DatasetRelationship,
    DatasourceConfig,
    SlayerDataset,
)
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2212_fixtures import (
    BACKENDS,
    DS,
    HIDE_C3,
    NOTES,
    baskets_dataset,
    customers_dataset,
    notes_dataset,
    q,
    rows,
    run,
    seed_datasets,
    seed_engine,
    table,
    with_policy,
)
from tests._engine_helpers import seeded_exec_engine

R, REV, RET = "customers.region", "orders.total_revenue", "returns.total_returns"


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _rel(to: str, col: str, to_col: Optional[str] = None) -> DatasetRelationship:
    return DatasetRelationship(to=to, from_columns=[col], to_columns=[to_col or col],
                               cardinality=RelationshipCardinality.MANY_TO_ONE)


def _ds(name: str, pk: Optional[str], *cols: str, rels: tuple[DatasetRelationship, ...] = (),
        numeric: tuple[str, ...] = (), **extra: Any) -> SlayerDataset:
    def dtype(c: str) -> DataType:
        return DataType.DATE if c == "ordered_at" else DataType.FLOAT if c in numeric else DataType.STRING
    fields = [*([DatasetField(name=pk, datatype=DataType.STRING, primary_key=True)] if pk else []),
              *(DatasetField(name=c, datatype=dtype(c)) for c in cols)]
    return SlayerDataset(name=name, sql_table=name, data_source=DS, fields=fields, relationships=list(rels), **extra)


@contextlib.asynccontextmanager
async def _engine(backend: str, tables: list[TableSpec], datasets: list[SlayerDataset],
                  ) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, tables),
                                  datasets=datasets) as (eng, _):
        yield eng


def _m(metric: str, alias: str = "v") -> dict[str, str]:
    return {"metric": metric, "alias": alias}


# --------------------------------------------------------------------------- #
# Two-route schema
# --------------------------------------------------------------------------- #
TWO_ROUTE_TABLES = [
    TableSpec(name="regions", columns=[("region_id", "TEXT"), ("name", "TEXT"), ("manager_id", "TEXT")],
              rows=[("r1", "North", "m1"), ("r2", "South", "m2")]),
    TableSpec(name="managers", columns=[("manager_id", "TEXT"), ("name", "TEXT")],
              rows=[("m1", "mia"), ("m2", "max")]),
    TableSpec(name="customers", columns=[("customer_id", "TEXT"), ("region_id", "TEXT"), ("segment", "TEXT"),
                                         ("weight", "DOUBLE")],
              rows=[("C1", "r1", "retail", 3.0), ("C2", "r1", "corporate", 2.0), ("C3", "r2", "retail", 1.0)]),
    TableSpec(name="orders", columns=[("order_id", "TEXT"), ("customer_id", "TEXT"), ("manager_id", "TEXT"),
                                      ("amount", "DOUBLE"), ("ordered_at", "DATE")],
              rows=[("O1", "C1", "m2", 100.0, "2025-01-10"), ("O2", "C2", "m1", 50.0, "2025-01-20"),
                    ("O3", "C3", "m1", 300.0, "2025-03-05")]),
]

#: ``SUM({value} * {w})``, ``w`` defaulting to the order's customer weight.
WSUM = Aggregation(name="wsum", expression="SUM({value} * {w})", params=[AggregationParam(name="w", sql="customers.weight")])


def two_route_datasets(*, customer_metrics: tuple[DatasetMetric, ...] = (),
                       region_metrics: tuple[DatasetMetric, ...] = ()) -> list[SlayerDataset]:
    return [
        _ds("regions", "region_id", "name", "manager_id", rels=(_rel("managers", "manager_id"),),
            metrics=list(region_metrics)),
        _ds("managers", "manager_id", "name"),
        _ds("customers", "customer_id", "region_id", "segment", "weight", numeric=("weight",),
            rels=(_rel("regions", "region_id"),), metrics=list(customer_metrics)),
        _ds("orders", "order_id", "customer_id", "manager_id", "amount", "ordered_at", numeric=("amount",),
            rels=(_rel("customers", "customer_id"), _rel("managers", "manager_id")),
            metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)")], aggregations=[WSUM],
            default_time_dimension="ordered_at"),
    ]


@pytest.fixture
async def two_route(backend: str):
    async with _engine(backend, TWO_ROUTE_TABLES, two_route_datasets()) as eng:
        yield eng


NAME = "regions.name"
BY_REGION = rows(("North", 150), ("South", 300))


async def test_a_named_population_with_two_routes_measures_from_the_home(two_route) -> None:
    resp = await run(two_route, population="regions", dimensions=[NAME], metrics=[REV])
    assert table(resp, NAME, REV) == BY_REGION


async def test_a_named_population_with_two_routes_filters_by_the_aggregate(two_route) -> None:
    resp = await run(two_route, population="regions", dimensions=[NAME], filter=["sum(orders.amount) > 200"])
    assert table(resp, NAME) == rows(("South",))


async def test_a_named_population_with_two_routes_orders_by_the_aggregate(two_route) -> None:
    resp = await run(two_route, population="regions", dimensions=[NAME],
                     order=[{"field": "sum(orders.amount)", "direction": "desc"}])
    assert [r[NAME] for r in resp.data] == ["South", "North"]


async def test_a_named_population_with_two_routes_groups_by_a_partitioned_aggregate(two_route) -> None:
    resp = await run(two_route, population="regions",
                     dimensions=[NAME, {"field": "sum(orders.amount, partition_by=[regions.name])", "alias": "t"}])
    assert table(resp, NAME, "t") == BY_REGION


@pytest.mark.parametrize(("dimensions", "expected"), [
    pytest.param([NAME], [("North", 75), ("South", 300)], id="by-region"),
    pytest.param([], [(187.5,)], id="no-dimension"),
])
async def test_a_named_population_with_two_routes_attaches_a_parameter(two_route, dimensions: list,
                                                                        expected: list) -> None:
    resp = await run(two_route, population="regions", dimensions=dimensions, metrics=[
        _m("weighted_avg(orders.amount, weight=sum(orders.amount, partition_by=[regions.name]))")])
    assert table(resp, *dimensions, "v") == rows(*expected)


@pytest.mark.parametrize("population", ["regions", None])
async def test_ranked_and_filtered_aggregations_bind_from_their_anchor(two_route, population) -> None:
    resp = await run(two_route, **({"population": population} if population else {}), dimensions=[NAME], metrics=[
        _m("last(orders.amount, orders.ordered_at)", "last"),
        _m("sum(orders.amount) FILTER (WHERE orders.amount > 60)", "big")])
    assert table(resp, NAME, "last", "big") == rows(("North", 50, 100), ("South", 300, 300))


async def test_a_spine_query_keeps_its_spine_rows(two_route) -> None:
    spine = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-03-31"]}
    resp = await run(two_route, population="regions", dimensions=[NAME], time_dimensions=[spine], metrics=[REV])
    month = next(c for c in resp.columns if c not in (NAME, REV))
    got = sorted((r[NAME], str(r[month])[:7], r[REV]) for r in resp.data)
    assert got == [("North", "2025-01", 150.0), ("North", "2025-02", None), ("North", "2025-03", None),
                   ("South", "2025-01", None), ("South", "2025-02", None), ("South", "2025-03", 300.0)]


# Reference identity: each spelling is its own reference, routed from every reader.
_VIA_SOURCE, _AT_MANAGERS = "customers.regions.managers.name", "managers.name"


@pytest.mark.parametrize("dimension", [f"{_VIA_SOURCE} || {_AT_MANAGERS}", f"{_AT_MANAGERS} || {_VIA_SOURCE}"])
async def test_two_spellings_in_one_expression_are_each_routed_from_the_home(two_route, dimension: str) -> None:
    with pytest.raises(UnresolvableDimensionRelationshipError):
        await run(two_route, population="customers", dimensions=[{"field": dimension, "alias": "mm"}],
                  metrics=["sum(customers.orders.amount)"])


@pytest.mark.parametrize(("metric", "expected"), [
    pytest.param("wsum(orders.amount)", [("North", 400), ("South", 300)], id="definition-default"),
    pytest.param("sum(orders.amount) FILTER (WHERE customers.segment = 'retail')", [("North", 100), ("South", 300)],
                 id="filter"),
    pytest.param("last(orders.amount, customers.weight)", [("North", 100), ("South", 300)], id="ranking-key"),
])
async def test_an_aggregation_reads_another_anchor_from_its_own(two_route, metric: str, expected: list) -> None:
    resp = await run(two_route, population="regions", dimensions=[NAME], metrics=[_m(metric)])
    assert table(resp, NAME, "v") == rows(*expected)


async def test_saved_metrics_composed_through_two_frames_bind_from_their_declaring_datasets(backend: str) -> None:
    datasets = two_route_datasets(
        customer_metrics=(DatasetMetric(name="order_rev", expression="sum(orders.amount)"),),
        region_metrics=(DatasetMetric(name="rev", expression="customers.order_rev"),))
    async with _engine(backend, TWO_ROUTE_TABLES, datasets) as eng:
        resp = await run(eng, population="regions", dimensions=[NAME], metrics=["regions.rev"])
    assert table(resp, NAME, "regions.rev") == BY_REGION


async def test_a_nested_saved_metric_stays_on_the_populations_datasource(backend: str, tmp_path) -> None:
    datasets = two_route_datasets(customer_metrics=(DatasetMetric(name="order_rev", expression="sum(orders.amount)"),))
    async with _engine(backend, TWO_ROUTE_TABLES, datasets) as eng:
        path = str(tmp_path / "other.duckdb")
        seed_backend("duckdb", path, [TableSpec(name="orders", columns=TWO_ROUTE_TABLES[3].columns, rows=[])])
        await eng.storage.save_datasource(DatasourceConfig(name="other", type="duckdb", database=path))
        await eng.storage.save_dataset(datasets[3].model_copy(update={"data_source": "other", "relationships": []}))
        await eng.storage.set_datasource_priority(["other", DS])
        resp = await run(eng, population="regions", dimensions=[NAME], metrics=["customers.order_rev"])
    assert table(resp, NAME, "customers.order_rev") == BY_REGION


# --------------------------------------------------------------------------- #
# Appendix B seed
# --------------------------------------------------------------------------- #
THROUGH_CUSTOMERS = _m("sum(customers.orders.amount)", "s")


@pytest.mark.parametrize(("dimensions", "extra", "expected"), [
    pytest.param([R], [], [("EAST", 350), ("WEST", 300)], id="by-region"),
    pytest.param([R], [RET], [("EAST", 350, 25), ("NORTH", None, 10), ("WEST", 300, None)], id="beside-returns"),
    pytest.param([], [], [(650,)], id="no-dimension"),
])
async def test_a_metric_written_through_another_anchor_binds_from_that_anchor(
        backend: str, dimensions: list, extra: list, expected: list) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=dimensions, metrics=[THROUGH_CUSTOMERS, *extra])
    assert table(resp, *dimensions, "s", *extra) == rows(*expected)


REAGG = _m("count(count(returns.*, partition_by=[customers.region, customers.segment]))", "c")


@pytest.mark.parametrize(("query", "expected"), [
    pytest.param({"population": "orders"}, [("EAST", 1), ("WEST", 0)], id="orders"),
    pytest.param({"population": "customers"}, [("EAST", 1), ("NORTH", 1), ("WEST", 0)], id="customers"),
    pytest.param({}, [("EAST", 1), ("NORTH", 1)], id="inferred"),
    pytest.param({"metrics": [REAGG, REV]}, [("EAST", 1), ("NORTH", 1), ("WEST", 0)], id="inferred-beside-revenue"),
])
async def test_the_operands_cells_never_depend_on_the_population(backend: str, query: dict, expected: list) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, **{"dimensions": [R], "metrics": [REAGG], **query})
    assert table(resp, R, "c") == rows(*expected)


@pytest.mark.parametrize("order", list(itertools.permutations([REV, RET, REAGG["metric"]])))
async def test_the_order_of_metrics_never_changes_a_value(backend: str, order: tuple) -> None:
    metrics = [_m(x, "c") if x == REAGG["metric"] else x for x in order]
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=metrics)
    assert table(resp, R, REV, RET, "c") == rows(
        ("EAST", 350, 25, 1), ("NORTH", None, 10, 1), ("WEST", 300, None, 0))


RETAIL = customers_dataset(filters=["segment = 'retail'"])


@pytest.mark.parametrize(("dimension", "expected"), [
    pytest.param("orders.status", [("completed", 150), ("open", 500), (None, None)], id="off-path"),
    pytest.param(R, [("EAST", 300), ("NORTH", None), ("WEST", 300)], id="on-path"),
])
async def test_a_model_filtered_population_narrows_a_home_only_on_its_own_paths(
        backend: str, dimension: str, expected: list) -> None:
    async with seed_engine(backend, datasets=seed_datasets(customers=RETAIL)) as eng:
        resp = await run(eng, population="customers", dimensions=[dimension], metrics=["sum(orders.amount)"])
    assert table(resp, dimension, "orders.amount_sum") == rows(*expected)


@pytest.mark.parametrize(("dimension", "expected"), [
    pytest.param("orders.status", [("completed", 150), ("open", 500), (None, None)], id="off-path"),
    pytest.param(R, [("EAST", 350), ("NORTH", None)], id="on-path"),
])
async def test_policy_hidden_population_rows_narrow_a_home_only_on_its_own_paths(
        backend: str, dimension: str, expected: list) -> None:
    async with seed_engine(backend) as eng, with_policy(eng, HIDE_C3) as hidden:
        resp = await run(hidden, population="customers", dimensions=[dimension], metrics=["sum(orders.amount)"])
    assert table(resp, dimension, "orders.amount_sum") == rows(*expected)


_PC = {"name": "pc", "population": "orders", "dimensions": ["orders.customer_id"],
       "metrics": [_m("sum(orders.amount)", "revenue")]}


@pytest.mark.parametrize("restriction", [
    pytest.param({"filter": ["orders.amount > 60"]}, id="filtered"),
    pytest.param({"filter": ["revenue > 250"]}, id="metric-filtered"),
])
async def test_a_restricted_stage_restricts_a_home_only_along_the_homes_own_paths(backend: str,
                                                                                restriction: dict) -> None:
    stage = q(**_PC, **restriction)
    async with seed_engine(backend) as eng:
        grouped = await eng.execute([stage, q(population="pc", dimensions=["pc.customer_id"],
                                              metrics=[_m("sum(orders.amount)")])])
        counted = await eng.execute([stage, q(population="pc", metrics=[_m("count(customers.customer_id)")])])
    assert table(grouped, "pc.customer_id", "v") == rows(("C1", 300), ("C3", 300))
    assert table(counted, "v") == rows((4,))


async def test_a_home_reaching_a_restricted_stage_through_an_inherited_reverse_edge(backend: str) -> None:
    stage = q(name="s", population="customers", dimensions=["customers.customer_id", R],
              filter=["customers.region <> 'WEST'"], metrics=[_m("count(customers.*)", "n")])
    async with seed_engine(backend) as eng:
        grouped = await eng.execute([stage, q(population="s", dimensions=["s.region"],
                                              metrics=[_m("sum(orders.amount)")])])
        total = await eng.execute([stage, q(population="s", metrics=[_m("sum(orders.amount)")])])
    assert table(grouped, grouped.columns[0], "v") == rows(("EAST", 350), ("NORTH", None))
    assert table(total, "v") == rows((650,))


async def test_leaves_anchored_apart_meet_at_an_inheriting_stage(backend: str) -> None:
    final = q(population="customers", dimensions=[R], metrics=[
        _m("sum(case when customers.segment = 'retail' then pc.revenue else 0 end)")])
    async with seed_engine(backend) as eng:
        resp = await eng.execute([q(**_PC), final])
    assert table(resp, R, "v") == rows(("EAST", 300), ("WEST", 300), ("NORTH", None))


@pytest.mark.parametrize("population", ["customers", None])
async def test_leaves_anchored_apart_meet_at_a_keyless_home(backend: str, population) -> None:
    async with seed_engine(backend, extra_tables=(NOTES,), datasets=seed_datasets(extra=(notes_dataset(),))) as eng:
        resp = await run(eng, **({"population": population} if population else {}), dimensions=[R], metrics=[
            _m("sum(case when customers.segment = 'retail' then notes.amount end)")])
    expected = [("EAST", 5), ("WEST", 6), ("NORTH", None) if population else (None, None)]
    assert table(resp, R, "v") == rows(*expected)


_BASKETS = ("CREATE TABLE baskets (basket_id VARCHAR, customer_id VARCHAR, items VARCHAR[])",
            "INSERT INTO baskets VALUES ('B1', 'C1', ['a', 'a', NULL]), ('B2', 'C3', ['b'])")


async def test_leaves_anchored_apart_meet_at_a_list_child() -> None:
    async with seed_engine("duckdb", extra_statements=_BASKETS,
                           datasets=seed_datasets(extra=(baskets_dataset(),))) as eng:
        resp = await run(eng, population="customers", dimensions=["customers.segment"], metrics=[
            _m("count(case when customers.region = 'EAST' then baskets.items.value end)")])
    assert table(resp, "customers.segment", "v") == rows(("corporate", 0), ("retail", 2))


@pytest.mark.parametrize(("population", "expected"), [
    pytest.param("customers", [("completed", 2), ("open", 3), (None, 0)], id="customers"),
    pytest.param(None, [("completed", 2), ("open", 3)], id="inferred"),
])
async def test_repeated_and_null_list_elements_associate_per_element(population, expected: list) -> None:
    # The repeated `a` counts twice; the NULL element is no element.
    async with seed_engine("duckdb", extra_statements=_BASKETS,
                           datasets=seed_datasets(extra=(baskets_dataset(),))) as eng:
        resp = await run(eng, **({"population": population} if population else {}), dimensions=["orders.status"],
                         metrics=[_m("count(baskets.items.*)")])
    assert table(resp, "orders.status", "v") == rows(*expected)


DUP_NOTES = TableSpec(name="notes", columns=NOTES.columns, rows=[("C1", 5.0), ("C1", 5.0), ("C3", 6.0)])


@pytest.mark.parametrize(("population", "expected"), [
    pytest.param("customers", [("completed", 10), ("open", 16), (None, None)], id="customers"),
    pytest.param(None, [("completed", 10), ("open", 16)], id="inferred"),
])
async def test_keyless_duplicate_rows_associate_per_row(backend: str, population, expected: list) -> None:
    async with seed_engine(backend, extra_tables=(DUP_NOTES,), datasets=seed_datasets(extra=(notes_dataset(),))) as eng:
        resp = await run(eng, **({"population": population} if population else {}), dimensions=["orders.status"],
                         metrics=[_m("sum(notes.amount)")])
    assert table(resp, "orders.status", "v") == rows(*expected)


@pytest.mark.parametrize(("condition", "expected"), [
    pytest.param("orders.amount > 100", [("EAST", 200, 25), ("WEST", 300, None)], id="two-homes"),
    pytest.param("orders.status = 'open' or customers.segment = 'corporate'",
                 [("EAST", 250, 25), ("WEST", 300, None)], id="or"),
    pytest.param("not (orders.status = 'open')", [("EAST", 150, 25)], id="not"),
])
async def test_a_named_populations_filter_is_judged_per_home(backend: str, condition: str, expected: list) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[R], metrics=[REV, RET], filter=[condition])
    assert table(resp, R, REV, RET) == rows(*expected)


# --------------------------------------------------------------------------- #
# Stores schema
# --------------------------------------------------------------------------- #
STORES_TABLES = [
    TableSpec(name="item", columns=[("item_id", "TEXT"), ("brand", "TEXT")], rows=[("I1", "Acme"), ("I2", "Beta")]),
    TableSpec(name="date_dim", columns=[("date_id", "TEXT"), ("year", "TEXT")], rows=[("D1", "2023"), ("D2", "2024")]),
    TableSpec(name="store_sales", columns=[("ss_id", "TEXT"), ("item_id", "TEXT"), ("date_id", "TEXT"),
                                           ("amount", "DOUBLE")],
              rows=[("S1", "I1", "D1", 125.0), ("S2", "I1", "D2", 200.0), ("S3", "I2", "D2", 90.0)]),
    TableSpec(name="store_returns", columns=[("sr_id", "TEXT"), ("item_id", "TEXT"), ("date_id", "TEXT"),
                                             ("amount", "DOUBLE")],
              rows=[("T1", "I1", "D1", 10.0), ("T2", "I2", "D2", 5.0)]),
]


def _fact(name: str, pk: str, metric: str) -> SlayerDataset:
    return _ds(name, pk, "item_id", "date_id", "amount", numeric=("amount",),
               rels=(_rel("item", "item_id"), _rel("date_dim", "date_id")),
               metrics=[DatasetMetric(name=metric, expression="sum(amount)")])


STORES_DATASETS = [_ds("item", "item_id", "brand"), _ds("date_dim", "date_id", "year"),
                   _fact("store_sales", "ss_id", "total_sales"), _fact("store_returns", "sr_id", "total_returns")]


async def test_a_saved_metric_bound_through_a_host_with_two_routes_to_its_home(backend: str) -> None:
    async with _engine(backend, STORES_TABLES, STORES_DATASETS) as eng:
        resp = await run(eng, dimensions=["item.brand", "date_dim.year"],
                         metrics=["store_sales.total_sales", "store_returns.total_returns"])
    assert table(resp, "item.brand", "date_dim.year", "store_sales.total_sales", "store_returns.total_returns") == rows(
        ("Acme", "2023", 125, 10), ("Acme", "2024", 200, None), ("Beta", "2024", 90, 5))


# --------------------------------------------------------------------------- #
# Multi-anchor schema
# --------------------------------------------------------------------------- #
MA_TABLES = [
    TableSpec(name="customers", columns=[("customer_id", "TEXT"), ("name", "TEXT"), ("discount", "DOUBLE")],
              rows=[("C1", "Ann", 1.0), ("C2", "Bob", 2.0)]),
    TableSpec(name="managers", columns=[("manager_id", "TEXT"), ("bonus", "DOUBLE")], rows=[("M1", 10.0), ("M2", 20.0)]),
    TableSpec(name="products", columns=[("product_id", "TEXT"), ("price", "DOUBLE")], rows=[("P1", 10.0), ("P2", 20.0)]),
    TableSpec(name="orders", columns=[("order_id", "TEXT"), ("customer_id", "TEXT"), ("manager_id", "TEXT")],
              rows=[("O1", "C1", "M1"), ("O2", "C2", "M1"), ("O3", "C1", "M2")]),
    TableSpec(name="order_lines", columns=[("line_id", "TEXT"), ("order_id", "TEXT"), ("product_id", "TEXT")],
              rows=[("L1", "O1", "P1"), ("L2", "O1", "P2"), ("L3", "O2", "P1"), ("L4", "O3", "P1")]),
    TableSpec(name="tickets", columns=[("ticket_id", "TEXT"), ("customer_id", "TEXT"), ("manager_id", "TEXT")],
              rows=[("T1", "C2", "M2")]),
    TableSpec(name="widgets", columns=[("widget_id", "TEXT"), ("weight", "DOUBLE")], rows=[("W1", 3.0)]),
]
MA_DATASETS = [
    _ds("customers", "customer_id", "name", "discount", numeric=("discount",)),
    _ds("managers", "manager_id", "bonus", numeric=("bonus",)),
    _ds("products", "product_id", "price", numeric=("price",)),
    _ds("orders", "order_id", "customer_id", "manager_id",
        rels=(_rel("customers", "customer_id"), _rel("managers", "manager_id"))),
    _ds("order_lines", "line_id", "order_id", "product_id",
        rels=(_rel("orders", "order_id"), _rel("products", "product_id"))),
    _ds("Tickets", "ticket_id", "customer_id", "manager_id",
        rels=(_rel("customers", "customer_id"), _rel("managers", "manager_id"))).model_copy(
        update={"sql_table": "tickets"}),
    _ds("widgets", "widget_id", "weight", numeric=("weight",)),
]


@pytest.fixture
async def multi_anchor(backend: str):
    async with _engine(backend, MA_TABLES, MA_DATASETS) as eng:
        yield eng


LINE_VALUE = _m("sum(customers.discount * products.price)")


@pytest.mark.parametrize("population", ["customers", "products", None])
async def test_leaves_anchored_apart_meet_at_the_smallest_determining_dataset(multi_anchor, population) -> None:
    resp = await run(multi_anchor, **({"population": population} if population else {}), metrics=[LINE_VALUE])
    assert table(resp, "v") == rows((60,))


@pytest.mark.parametrize("population", ["customers", None])
async def test_leaves_anchored_apart_group_by_the_homes_reach(multi_anchor, population) -> None:
    resp = await run(multi_anchor, **({"population": population} if population else {}),
                     dimensions=["customers.name"], metrics=[LINE_VALUE])
    assert table(resp, "customers.name", "v") == rows(("Ann", 40), ("Bob", 20))


_ORDERS_SPELLING = "orders.customers.discount * orders.managers.bonus"
_TICKETS_SPELLING = "Tickets.customers.discount * Tickets.managers.bonus"


async def test_tied_minimal_datasets_are_refused(multi_anchor) -> None:
    # `Tickets` sorts after `orders` by canonical name, before it by declared case.
    query = q(population="orders", metrics=[_m("sum(customers.discount * managers.bonus)")])
    with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
        await multi_anchor.execute(query)
    msg = str(ei.value)
    assert _ORDERS_SPELLING in msg
    assert _TICKETS_SPELLING in msg
    assert msg.index(_ORDERS_SPELLING) < msg.index(_TICKETS_SPELLING)
    resp = await run(multi_anchor, population="orders", metrics=[_m(f"sum({_ORDERS_SPELLING})")])
    assert table(resp, "v") == rows((50,))


async def test_unconnected_anchors_are_refused(multi_anchor) -> None:
    query = q(metrics=[_m("sum(products.price * widgets.weight)")])
    with pytest.raises(UnsafeRelationshipInputError) as ei:
        await multi_anchor.execute(query)
    assert "widgets" in str(ei.value)


@pytest.mark.parametrize("population", ["customers", None])
async def test_an_undefined_union_grain_anchor_fails_closed(multi_anchor, population) -> None:
    query = q(**({"population": population} if population else {}), metrics=[_m(
        "count(count(orders.*, partition_by=[customers.name]) * count(tickets.*, partition_by=[managers.manager_id]))")])
    with pytest.raises(PopulationInferenceError):
        await multi_anchor.execute(query)


# --------------------------------------------------------------------------- #
# Diamond schema
# --------------------------------------------------------------------------- #
DIAMOND_TABLES = [
    TableSpec(name="regions", columns=[("region_id", "TEXT")], rows=[("R1",)]),
    TableSpec(name="customers", columns=[("customer_id", "TEXT"), ("region_id", "TEXT"), ("discount", "DOUBLE")],
              rows=[("C1", "R1", 1.0), ("C2", "R1", 2.0)]),
    TableSpec(name="stores", columns=[("store_id", "TEXT"), ("region_id", "TEXT"), ("fee", "DOUBLE")],
              rows=[("S1", "R1", 3.0), ("S2", "R1", 5.0)]),
    TableSpec(name="orders", columns=[("order_id", "TEXT"), ("customer_id", "TEXT"), ("store_id", "TEXT")],
              rows=[("O1", "C1", "S1"), ("O2", "C2", "S1"), ("O3", "C1", "S2")]),
]
DIAMOND_DATASETS = [
    _ds("regions", "region_id"),
    _ds("customers", "customer_id", "region_id", "discount", numeric=("discount",), rels=(_rel("regions", "region_id"),)),
    _ds("stores", "store_id", "region_id", "fee", numeric=("fee",), rels=(_rel("regions", "region_id"),)),
    _ds("orders", "order_id", "customer_id", "store_id",
        rels=(_rel("customers", "customer_id"), _rel("stores", "store_id"))),
]


@pytest.mark.parametrize("population", ["customers", "stores", None])
async def test_leaves_anchored_apart_meet_below_a_diamond(backend: str, population) -> None:
    async with _engine(backend, DIAMOND_TABLES, DIAMOND_DATASETS) as eng:
        resp = await run(eng, **({"population": population} if population else {}),
                         metrics=[_m("sum(customers.discount * stores.fee)")])
    assert table(resp, "v") == rows((14,))


# --------------------------------------------------------------------------- #
# Geo schema
# --------------------------------------------------------------------------- #
GEO_TABLES = [TableSpec(name="sales", columns=[("sale_id", "TEXT"), ("city", "TEXT"), ("region", "TEXT"),
                                               ("a", "DOUBLE"), ("b", "DOUBLE")],
                        rows=[("S1", "Paris", "EU", 1.0, 1.0), ("S2", "Lyon", "EU", 2.0, 2.0),
                              ("S3", "Tokyo", "Asia", 3.0, 3.0), ("S4", "Paris", "EU", 4.0, 4.0)])]
GEO_DATASETS = [_ds("sales", "sale_id", "city", "region", "a", "b", numeric=("a", "b"))]


@pytest.mark.parametrize("population", ["sales", None])
async def test_constituents_with_no_full_grain_member_take_the_union_grains_anchor(backend: str, population) -> None:
    async with _engine(backend, GEO_TABLES, GEO_DATASETS) as eng:
        resp = await run(eng, **({"population": population} if population else {}), dimensions=["sales.region"],
                         metrics=[_m("count(sum(sales.a, partition_by=[sales.city]) * "
                                     "sum(sales.b, partition_by=[sales.region]))")])
    assert table(resp, "sales.region", "v") == rows(("Asia", 1), ("EU", 2))
