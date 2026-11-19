"""``source.column`` on Postgres: the leaf matches with the datasource's identifier folding (spec:
importers/osi-import › Source columns in dataset-scoped metrics)."""

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasetField, DatasetMetric, DatasourceConfig, SlayerDataset
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiToSlayerConverter
from tests._osi_helpers import by_name, documents, ds, f, flat, m, run_converter, unconverted, visible
from tests._pg_helpers import pg_database, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)

DDL = [
    "CREATE TABLE orders (order_id INTEGER, quantity INTEGER)",
    "INSERT INTO orders VALUES (1, 2), (2, 3)",
    'CREATE TABLE twins (order_id INTEGER, quantity INTEGER, "Quantity" INTEGER)',
    "INSERT INTO twins VALUES (1, 2, 20)",
]


@pytest.fixture
def pg(postgresql_proc: Any) -> Generator[DatasourceConfig]:
    with pg_database(postgresql_proc, statements=DDL, name="osi_source") as datasource:
        yield datasource


def _import(datasource: DatasourceConfig, table: str, sql: str) -> ConversionResult:
    dataset = ds(table, fields=[f("order_id")], metrics=[m("units", sql)])
    converter = OsiToSlayerConverter(documents=documents(flat([dataset])), data_source=datasource.name,
                                     target_dialect="postgres")
    return run_converter(converter, datasource=datasource)


async def test_unquoted_leaf_folds(pg: DatasourceConfig, tmp_path: Path) -> None:
    result = _import(pg, "orders", "SUM(source.Quantity)")
    assert visible(by_name(result)["orders"]) == {"order_id"}
    async with pg_query_engine(pg, base_dir=str(tmp_path), datasets=result.datasets) as engine:
        rows = (await engine.execute({"population": "orders", "metrics": ["units"]})).data
    assert rows[0]["orders.units"] == 5


def test_leaf_matching_two_live_columns_is_unconverted(pg: DatasourceConfig) -> None:
    result = _import(pg, "twins", "SUM(source.Quantity)")
    assert unconverted(result, "units")
    assert [c.name for c in by_name(result)["twins"].fields] == ["order_id"]


def _stored(datasource: DatasourceConfig, table: str, **metrics: str) -> SlayerDataset:
    return SlayerDataset(name=table, sql_table=table, data_source=datasource.name, fields=[DatasetField(name="order_id")],
                         metrics=[DatasetMetric(name=name, expression=sql) for name, sql in metrics.items()])


async def test_stored_unquoted_leaf_folds(pg: DatasourceConfig, tmp_path: Path) -> None:
    async with pg_query_engine(pg, base_dir=str(tmp_path), datasets=[_stored(pg, "orders", units="SUM(source.Quantity)")]) as engine:
        rows = (await engine.execute({"population": "orders", "metrics": ["units"]})).data
    assert rows[0]["orders.units"] == 5


async def test_stored_quoted_and_unquoted_leaves_read_their_own_columns(pg: DatasourceConfig, tmp_path: Path) -> None:
    twins = _stored(pg, "twins", exact='SUM(source."Quantity")', folded="SUM(source.Quantity)")
    async with pg_query_engine(pg, base_dir=str(tmp_path), datasets=[twins]) as engine:
        rows = (await engine.execute({"population": "twins", "metrics": ["exact", "folded"]})).data
    assert (rows[0]["twins.exact"], rows[0]["twins.folded"]) == (20, 2)
