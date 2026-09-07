"""DEV-1751 e2e — facade-translated time filters executed on DuckDB.

A one-sided filter used to lift into a half-open ``date_range`` and render as
``BETWEEN x AND NULL``, silently returning zero rows.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.storage.yaml_storage import YAMLStorage

duckdb = pytest.importorskip("duckdb")


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
    )


def _seed(db_path: str) -> None:
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE orders(id INT, revenue DOUBLE, ordered_at TIMESTAMP)")
    con.executemany(
        "INSERT INTO orders VALUES (?,?,?)",
        [
            (1, 5.0, "2023-12-15 00:00:00"),
            (2, 10.0, "2024-01-10 00:00:00"),
            (3, 20.0, "2024-02-20 00:00:00"),
        ],
    )
    con.close()


def _translated_query(sql: str) -> SlayerQuery:
    catalog = build_catalog(models_by_datasource={"test": [_orders()]})
    result = translate(sql=sql, catalog=catalog, dialect="postgres")
    assert isinstance(result, QueryResult), result
    return result.query


async def _execute_facade_sql(tmp_path, sql: str):
    db_path = str(tmp_path / "orders.duckdb")
    _seed(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="duckdb", database=db_path)
    )
    await storage.save_model(_orders())
    engine = SlayerQueryEngine(storage=storage)
    return await engine.execute(_translated_query(sql))


def _by_month(resp) -> dict[str, float]:
    return {
        str(row["orders.ordered_at"])[:7]: row["orders.revenue_sum"]
        for row in resp.data
    }


ONE_SIDED = [
    pytest.param("WHERE ordered_at >= '2024-01-01'", id="gte"),
    pytest.param("WHERE '2024-01-01' <= ordered_at", id="reversed-operand"),
]


@pytest.mark.parametrize("where", ONE_SIDED)
async def test_one_sided_filter_returns_matching_rows(tmp_path, where) -> None:
    resp = await _execute_facade_sql(
        tmp_path,
        f"SELECT month(ordered_at), revenue_sum FROM orders {where}",
    )
    assert _by_month(resp) == {"2024-01": 10.0, "2024-02": 20.0}


async def test_between_filters_inclusively(tmp_path) -> None:
    # Guard: the BETWEEN → date_range lift keeps its inclusive-bound
    # semantics — rows exactly AT each bound are included.
    resp = await _execute_facade_sql(
        tmp_path,
        "SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at BETWEEN '2023-12-15 00:00:00' AND '2024-01-10 00:00:00'",
    )
    assert _by_month(resp) == {"2023-12": 5.0, "2024-01": 10.0}


async def test_strict_upper_bound_excludes_the_boundary_row(tmp_path) -> None:
    # `>= a AND < b` used to lift to an INCLUSIVE BETWEEN a AND b, wrongly
    # returning the row exactly at b.
    resp = await _execute_facade_sql(
        tmp_path,
        "SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01' AND ordered_at < '2024-02-20 00:00:00'",
    )
    assert _by_month(resp) == {"2024-01": 10.0}
