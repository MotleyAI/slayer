"""A query with dimensions and no ``order`` is ordered by its dimensions (spec: queries/result-order)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pyarrow as pa
import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.flight.handlers import FlightHandlers
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import _engine_generate, seeded_exec_engine

REVENUE = "sum(order_total)"
STORE_MONTH: dict[str, Any] = {
    "source_model": "orders", "measures": [REVENUE], "dimensions": ["stores.name", "month(ordered_at)"],
}
STATUS_MONTH: dict[str, Any] = {
    "source_model": "orders", "measures": ["count(*)"], "dimensions": ["status", "month(ordered_at)"],
}
STATUS_MONTH_EXPLICIT: dict[str, Any] = {
    **STATUS_MONTH,
    "order": [
        {"column": "status", "direction": "asc"},
        {"column": "month(ordered_at)", "direction": "asc"},
    ],
}
PER_CUSTOMER_STAGE: dict[str, Any] = {
    "name": "per_customer", "source_model": "orders",
    "dimensions": ["customer_id", "region"], "measures": [{"formula": REVENUE, "name": "rev"}],
}
BY_REGION_STAGE: dict[str, Any] = {
    "source_model": "per_customer", "dimensions": ["region"], "measures": [{"formula": "sum(rev)", "name": "total"}],
}
SPINE: dict[str, Any] = {
    "source_model": "orders", "dimensions": ["region"], "measures": [REVENUE],
    "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month",
                         "date_range": ["2024-01-01", "2024-03-31"]}],
}

_STORES_ROWS = [(1, "Beta"), (2, "Alpha"), (3, "Gamma")]
_CUSTOMERS_ROWS = [(1, "Zed"), (2, "Amy"), (3, "Mo")]
# (id, store_id, customer_id, status, region, order_total, ordered_at) — deliberately unsorted.
_ORDERS_ROWS = [
    (1, 1, 1, "c", "EU", 10.0, "2024-02-10"),
    (2, 2, 2, "a", "US", 20.0, "2024-01-05"),
    (3, 3, 3, None, "EU", 70.0, "2024-03-01"),
    (4, 1, 2, "d", "US", 55.0, "2024-01-20"),
    (5, 2, 3, "b", "EU", 30.0, "2024-02-25"),
    (6, 3, 1, "a", "US", 5.0, "2024-03-15"),
    (7, 1, 3, "d", "EU", 80.0, "2024-02-02"),
]


def _stores() -> SlayerModel:
    return SlayerModel(
        name="stores", data_source="test", sql_table="stores",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT)],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT)],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders", default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="region", type=DataType.TEXT),
            Column(name="order_total", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.DATE),
            # Sorts opposite to order_total: big orders label "a-big".
            Column(
                name="size_label", type=DataType.TEXT,
                sql="CASE WHEN order_total >= 50 THEN 'a-big' ELSE 'b-small' END",
            ),
        ],
        joins=[
            ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]]),
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _per_customer_backed() -> SlayerModel:
    return SlayerModel(
        name="per_customer_m", data_source="test",
        source_queries=[SlayerQuery.model_validate({k: v for k, v in PER_CUSTOMER_STAGE.items() if k != "name"})],
    )


def _models() -> list[SlayerModel]:
    return [_orders(), _stores(), _customers()]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute("CREATE TABLE stores (id INTEGER PRIMARY KEY, name TEXT)")
        con.executemany("INSERT INTO stores VALUES (?,?)", _STORES_ROWS)
        con.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        con.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, store_id INTEGER, customer_id INTEGER, "
            "status TEXT, region TEXT, order_total REAL, ordered_at TEXT)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", _ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE TABLE stores (id INTEGER, name VARCHAR)")
        con.executemany("INSERT INTO stores VALUES (?,?)", _STORES_ROWS)
        con.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
        con.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE orders (id INTEGER, store_id INTEGER, customer_id INTEGER, "
            "status VARCHAR, region VARCHAR, order_total DOUBLE, ordered_at DATE)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", _ORDERS_ROWS)
    finally:
        con.close()


_SEEDS = {"sqlite": _seed_sqlite, "duckdb": _seed_duckdb}


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=request.param, seed=_SEEDS[request.param], models=[*_models(), _per_customer_backed()],
    ) as (eng, _db):
        yield eng


async def _sql(query: Any, *, dialect: str) -> str:
    models = [*_models(), _per_customer_backed()]
    return await _engine_generate(query=query, model=models[0], extra_models=models[1:], dialect=dialect)


def _outer_order(sql: str, *, dialect: str) -> exp.Order | None:
    """The top-level statement's ORDER BY."""
    return sqlglot.parse_one(sql, dialect=dialect).args.get("order")


def _ordered_ctes(sql: str, *, dialect: str) -> list[str]:
    """Names of CTEs whose body carries an ORDER BY."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    return [cte.alias for cte in tree.find_all(exp.CTE) if cte.this.args.get("order") is not None]


def _order_text(sql: str, *, dialect: str) -> str:
    order = _outer_order(sql, dialect=dialect)
    assert order is not None, f"no outermost ORDER BY:\n{sql}"
    return order.sql(dialect=dialect)


def _null_last_key(value: Any) -> tuple[bool, Any]:
    return (value is None, "" if value is None else str(value))


def _column(resp: SlayerResponse, key: str) -> list[Any]:
    return [row[key] for row in resp.data]


def _month(value: Any) -> str:
    return str(value)[:7]


DIALECTS = ["sqlite", "duckdb", "postgres"]


class TestEmission:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_default_order_is_the_explicit_ascending_order(self, dialect) -> None:
        explicit = {**STORE_MONTH, "order": [
            {"column": "stores.name", "direction": "asc"},
            {"column": "month(ordered_at)", "direction": "asc"},
        ]}
        default_sql = await _sql(STORE_MONTH, dialect=dialect)
        assert _outer_order(default_sql, dialect=dialect) is not None
        assert default_sql == await _sql(explicit, dialect=dialect)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_dimensions_then_time_dimensions(self, dialect) -> None:
        query = {
            "source_model": "orders", "measures": ["count(*)"], "dimensions": ["status", "region"],
            "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
        }
        explicit = {**query, "order": [
            {"column": "status", "direction": "asc"},
            {"column": "region", "direction": "asc"},
            {"column": "month(ordered_at)", "direction": "asc"},
        ]}
        assert await _sql(query, dialect=dialect) == await _sql(explicit, dialect=dialect)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_empty_order_list_means_no_order(self, dialect) -> None:
        assert await _sql({**STATUS_MONTH, "order": []}, dialect=dialect) == await _sql(
            STATUS_MONTH_EXPLICIT, dialect=dialect,
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_explicit_order_used_verbatim(self, dialect) -> None:
        query = {
            "source_model": "orders", "measures": ["count(*)"], "dimensions": ["status", "region"],
            "order": [{"column": "count(*)", "direction": "desc"}],
        }
        text = _order_text(await _sql(query, dialect=dialect), dialect=dialect)
        assert "status" not in text
        assert "region" not in text

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_raw_rows_are_not_ordered(self, dialect) -> None:
        query = {"source_model": "orders", "dimensions": ["status", "region"], "distinct_dimension_values": False}
        assert _outer_order(await _sql(query, dialect=dialect), dialect=dialect) is None

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_measure_only_is_not_ordered(self, dialect) -> None:
        query = {"source_model": "orders", "measures": [REVENUE, "count(*)"]}
        assert _outer_order(await _sql(query, dialect=dialect), dialect=dialect) is None


class TestExecution:
    async def test_monthly_revenue_by_store_is_sorted(self, engine) -> None:
        resp = await engine.execute(STORE_MONTH)
        keys = [(r["orders.stores.name"], _month(r["orders.ordered_at"])) for r in resp.data]
        assert keys == sorted(keys)
        assert len(keys) == 5

    async def test_null_dimension_sorts_last(self, engine) -> None:
        resp = await engine.execute({"source_model": "orders", "measures": ["count(*)"], "dimensions": ["status"]})
        assert _column(resp, "orders.status") == ["a", "b", "c", "d", None]

    async def test_identical_to_explicit_spelling(self, engine) -> None:
        default = await engine.execute(STATUS_MONTH)
        explicit = await engine.execute(STATUS_MONTH_EXPLICIT)
        assert default.data == explicit.data
        statuses = _column(default, "orders.status")
        assert statuses[-1] is None
        assert statuses.index(None) == len(statuses) - 1

    async def test_default_order_picks_the_page(self, engine) -> None:
        resp = await engine.execute({
            "source_model": "orders", "measures": ["count(*)"], "dimensions": ["status"], "limit": 2, "offset": 1,
        })
        assert _column(resp, "orders.status") == ["b", "c"]

    async def test_explicit_order_wins(self, engine) -> None:
        resp = await engine.execute({
            "source_model": "orders", "measures": ["count(*)"], "dimensions": ["region"],
            "order": [{"column": "count(*)", "direction": "desc"}],
        })
        # EU has 4 orders, US 3.
        assert _column(resp, "orders.region") == ["EU", "US"]

    async def test_computed_and_joined_dimensions_sort_by_projected_values(self, engine) -> None:
        resp = await engine.execute({
            "source_model": "orders", "measures": ["count(*)"], "dimensions": ["size_label", "customers.name"],
        })
        keys = [(r["orders.size_label"], r["orders.customers.name"]) for r in resp.data]
        assert keys == [("a-big", "Amy"), ("a-big", "Mo"), ("b-small", "Amy"), ("b-small", "Mo"), ("b-small", "Zed")]


class TestOnlyOutermost:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_query_list_inner_stage_unordered(self, dialect) -> None:
        sql = await _sql([PER_CUSTOMER_STAGE, BY_REGION_STAGE], dialect=dialect)
        assert _ordered_ctes(sql, dialect=dialect) == []
        assert "region" in _order_text(sql, dialect=dialect)
        assert "customer_id" not in _order_text(sql, dialect=dialect)

    async def test_query_list_final_stage_rows_sorted(self, engine) -> None:
        resp = await engine.execute([PER_CUSTOMER_STAGE, BY_REGION_STAGE])
        assert [r for r in resp.columns if r.endswith("region")]
        region_key = next(c for c in resp.columns if c.endswith("region"))
        assert _column(resp, region_key) == ["EU", "US"]

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_query_backed_model_backing_query_unordered(self, dialect) -> None:
        sql = await _sql({
            "source_model": "per_customer_m", "dimensions": ["region"],
            "measures": [{"formula": "sum(rev)", "name": "total"}],
        }, dialect=dialect)
        assert _ordered_ctes(sql, dialect=dialect) == []
        assert "region" in _order_text(sql, dialect=dialect)

    async def test_saved_query_used_as_source_unordered(self, engine) -> None:
        await engine.create_model_from_query(query=[PER_CUSTOMER_STAGE, BY_REGION_STAGE], name="by_region")
        resp = await engine.execute(
            {"source_model": "by_region", "dimensions": ["region"], "measures": ["sum(total)"]}, dry_run=True,
        )
        assert resp.sql is not None
        assert _ordered_ctes(resp.sql, dialect="duckdb") == []
        assert _outer_order(resp.sql, dialect="duckdb") is not None

    async def test_saved_query_run_by_name_is_ordered(self, engine) -> None:
        await engine.create_model_from_query(
            query={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"]},
            name="status_counts",
        )
        resp = await engine.execute("status_counts")
        assert _column(resp, "orders.status") == ["a", "b", "c", "d", None]

    async def test_refined_saved_query_is_ordered(self, engine) -> None:
        await engine.create_model_from_query(
            query={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"]},
            name="status_counts",
        )
        resp = await engine.execute("status_counts", refine={"dimensions": ["region"]})
        keys = [(r["orders.status"], r["orders.region"]) for r in resp.data]
        assert keys == sorted(keys, key=lambda k: (_null_last_key(k[0]), _null_last_key(k[1])))
        assert keys[-1][0] is None

    async def test_saved_multistage_query_ordered_on_final_stage(self, engine) -> None:
        await engine.create_model_from_query(query=[PER_CUSTOMER_STAGE, BY_REGION_STAGE], name="by_region")
        resp = await engine.execute("by_region")
        region_key = next(c for c in resp.columns if c.endswith("region"))
        assert _column(resp, region_key) == ["EU", "US"]
        sql = (await engine.execute("by_region", dry_run=True)).sql
        assert sql is not None
        assert _outer_order(sql, dialect="duckdb") is not None
        assert _ordered_ctes(sql, dialect="duckdb") == []


class TestCombinations:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_transform_measure_matches_explicit_spelling(self, dialect) -> None:
        query = {"source_model": "orders", "measures": [REVENUE, f"change_pct({REVENUE})"],
                 "dimensions": ["month(ordered_at)"]}
        explicit = {**query, "order": [{"column": "month(ordered_at)", "direction": "asc"}]}
        sql = await _sql(query, dialect=dialect)
        assert _outer_order(sql, dialect=dialect) is not None
        assert sql == await _sql(explicit, dialect=dialect)

    async def test_transform_values_unchanged(self, engine) -> None:
        query = {"source_model": "orders", "measures": [REVENUE, f"change_pct({REVENUE})"],
                 "dimensions": ["month(ordered_at)"]}
        explicit = {**query, "order": [{"column": "month(ordered_at)", "direction": "asc"}]}
        default = await engine.execute(query)
        assert default.data == (await engine.execute(explicit)).data
        months = [_month(v) for v in _column(default, "orders.ordered_at")]
        assert months == ["2024-01", "2024-02", "2024-03"]

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_hidden_order_only_slot_gets_no_default(self, dialect) -> None:
        query = {"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"],
                 "order": [{"column": REVENUE, "direction": "desc"}]}
        assert "status" not in _order_text(await _sql(query, dialect=dialect), dialect=dialect)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_filter_only_hidden_value_excluded_from_default(self, dialect) -> None:
        query = {"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"],
                 "filters": [f"{REVENUE} > 0"]}
        explicit = {**query, "order": [{"column": "status", "direction": "asc"}]}
        sql = await _sql(query, dialect=dialect)
        assert _outer_order(sql, dialect=dialect) is not None
        assert sql == await _sql(explicit, dialect=dialect)

    async def test_filter_only_hidden_value_rows_sorted(self, engine) -> None:
        resp = await engine.execute({"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"],
                                     "filters": [f"{REVENUE} > 0"]})
        assert resp.columns == ["orders.status", "orders._count"]
        assert _column(resp, "orders.status") == ["a", "b", "c", "d", None]

    async def test_time_spine_query_is_ordered(self, engine) -> None:
        resp = await engine.execute(SPINE)
        spine_key = next(c for c in resp.columns if "timestamp" in c)
        keys = [(r["orders.region"], _month(r[spine_key])) for r in resp.data]
        assert keys == sorted(keys, key=lambda k: (_null_last_key(k[0]), k[1]))
        assert len(keys) == 6
        explicit = {**SPINE, "order": [
            {"column": "region", "direction": "asc"},
            {"column": "month(time_spine.timestamp)", "direction": "asc"},
        ]}
        sql = (await engine.execute(SPINE, dry_run=True)).sql
        assert sql is not None
        assert _outer_order(sql, dialect="duckdb") is not None
        assert sql == (await engine.execute(explicit, dry_run=True)).sql


class TestFacades:
    async def test_pg_facade_query_is_ordered(self, engine) -> None:
        catalog = build_catalog(models_by_datasource={"test": _models()})
        result = translate(sql="SELECT status, order_total_sum FROM orders", catalog=catalog, dialect="postgres")
        assert isinstance(result, QueryResult), result
        resp = await engine.execute(query=result.query, data_source=result.data_source)
        assert _column(resp, "orders.status") == ["a", "b", "c", "d", None]

    async def test_flight_facade_query_is_ordered(self, engine) -> None:
        handlers = FlightHandlers(engine=engine, storage=engine.storage)
        result = handlers._translate("SELECT status, order_total_sum FROM orders")
        assert isinstance(result, QueryResult), result
        table: pa.Table = handlers._execute_full(result)
        assert table.column(0).to_pylist() == ["a", "b", "c", "d", None]
