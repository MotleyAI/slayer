"""Facade-emitted time bounds carry date-only literals as midnight instants, so translated SQL keeps its meaning."""

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

SELECT = "SELECT month(ordered_at), revenue_sum FROM orders "


@pytest.fixture(params=[None, "postgres"])
def dialect(request):
    return request.param


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="delivered_at", type=DataType.DATE),
            Column(name="status", type=DataType.TEXT),
        ],
    )


def _translate(sql: str, dialect=None) -> SlayerQuery:
    result = translate(sql=sql, catalog=build_catalog(models_by_datasource={"test": [_orders()]}), dialect=dialect)
    assert isinstance(result, QueryResult), result
    return result.query


def _range(q: SlayerQuery):
    assert q.time_dimensions
    return q.time_dimensions[0].date_range


class TestLiftedBetween:
    def test_typed_literals_in_between(self, dialect) -> None:
        q = _translate(SELECT + "WHERE ordered_at BETWEEN DATE '2024-01-01' AND TIMESTAMP '2024-12-31'", dialect)
        assert _range(q) == ["2024-01-01 00:00:00", "2024-12-31 00:00:00"]

    def test_instant_bounds_are_kept(self, dialect) -> None:
        q = _translate(SELECT + "WHERE ordered_at BETWEEN '2024-01-01 10:00:00' AND '2024-12-31 23:59:59'", dialect)
        assert _range(q) == ["2024-01-01 10:00:00", "2024-12-31 23:59:59"]


class TestVerbatimComparators:
    @pytest.mark.parametrize(("where", "expected"), [
        ("ordered_at < '2025-01-01 10:30:00'", ["ordered_at < '2025-01-01 10:30:00'"]),
        ("ordered_at < TIMESTAMP '2025-01-01'", ["ordered_at < '2025-01-01 00:00:00'"]),
    ])
    def test_instants_and_typed_literals(self, dialect, where, expected) -> None:
        q = _translate(SELECT + "WHERE " + where, dialect)
        assert _range(q) is None
        assert q.filters == expected

    def test_reversed_typed_literal(self, dialect) -> None:
        q = _translate(SELECT + "WHERE DATE '2024-01-01' <= ordered_at", dialect)
        assert _range(q) is None
        assert q.filters in (
            ["'2024-01-01 00:00:00' <= ordered_at"], ["ordered_at >= '2024-01-01 00:00:00'"],
        )

    @pytest.mark.parametrize(("where", "expected"), [
        ("ordered_at <= '2024-12-31' AND ordered_at != '2024-06-01'",
         ["ordered_at <= '2024-12-31 00:00:00'", "ordered_at != '2024-06-01 00:00:00'"]),
        ("ordered_at = '2024-06-01'", ["ordered_at = '2024-06-01 00:00:00'"]),
        ("delivered_at > '2024-12-31'", ["delivered_at > '2024-12-31 00:00:00'"]),
    ])
    def test_unprojected_temporal_column(self, dialect, where, expected) -> None:
        q = _translate("SELECT revenue_sum FROM orders WHERE " + where, dialect)
        assert not q.time_dimensions
        assert q.filters == expected

    def test_text_column_untouched(self, dialect) -> None:
        q = _translate("SELECT revenue_sum FROM orders WHERE status = '2024-12-31'", dialect)
        assert q.filters == ["status = '2024-12-31'"]

    def test_non_time_filter_untouched(self, dialect) -> None:
        q = _translate("SELECT revenue_sum FROM orders WHERE revenue >= 10", dialect)
        assert q.filters == ["revenue >= 10"]


def _seed(db_path: str) -> None:
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE orders(id INT, revenue DOUBLE, ordered_at TIMESTAMP, delivered_at DATE, status VARCHAR)")
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", [
        (1, 1.0, "2024-01-01 00:00:00", "2024-01-01", "a"),
        (2, 2.0, "2024-12-31 00:00:00", "2024-12-31", "a"),
        (3, 4.0, "2024-12-31 10:00:00", "2024-12-31", "a"),
        (4, 8.0, "2025-01-01 00:00:00", "2025-01-01", "a"),
    ])
    con.close()


async def _revenue(tmp_path, where: str, *, column: str | None = "ordered_at") -> float:
    db_path = str(tmp_path / "orders.duckdb")
    _seed(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="duckdb", database=db_path))
    await storage.save_model(_orders())
    engine = SlayerQueryEngine(storage=storage)
    try:
        resp = await engine.execute(_translate(
            f"SELECT {f'month({column}), ' if column else ''}revenue_sum FROM orders WHERE {where}", "postgres",
        ))
    finally:
        engine.close()
    return sum(r["orders.revenue_sum"] for r in resp.data)


class TestExecutedMidnightSemantics:
    @pytest.mark.parametrize(("where", "expected"), [
        ("ordered_at BETWEEN '2024-01-01' AND '2024-12-31'", 3.0),
        ("ordered_at <= '2024-12-31'", 3.0),
        ("ordered_at > '2024-12-31'", 12.0),
        ("'2024-12-31' < ordered_at", 12.0),
        ("DATE '2024-12-31' >= ordered_at", 3.0),
    ])
    async def test_translated_query_matches_sql_meaning(self, tmp_path, where, expected) -> None:
        assert await _revenue(tmp_path, where) == pytest.approx(expected)

    async def test_date_column_bound(self, tmp_path) -> None:
        assert await _revenue(tmp_path, "delivered_at <= '2024-12-31'", column="delivered_at") == pytest.approx(7.0)

    async def test_unprojected_timestamp_bound(self, tmp_path) -> None:
        where = "ordered_at <= '2024-12-31' AND ordered_at != '2024-06-01'"
        assert await _revenue(tmp_path, where, column=None) == pytest.approx(3.0)  # ids 1, 2; not the 10:00 row
