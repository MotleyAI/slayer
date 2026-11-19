"""Ossie expression objects on Postgres: the target's own entry runs, portable sql keeps Ossie's meaning, dialect
entries export as themselves.

Specs: importers/osi-import, importers/osi-export.
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.exporter import export_osi
from tests._osi_export_helpers import dataset, field
from tests._osi_helpers import documents, ds, f, f_dialects, flat, run_converter
from tests._pg_helpers import pg_database, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)

DDL = [
    "CREATE TABLE orders (o_orderkey INTEGER, amount INTEGER, qty INTEGER, orderamount INTEGER)",
    "INSERT INTO orders VALUES (1, 10, 4, 3), (2, -5, 2, -1), (3, 7, 7, 0)",
]


@pytest.fixture
def pg(postgresql_proc: Any) -> Generator[DatasourceConfig]:
    with pg_database(postgresql_proc, statements=DDL, name="osi_forms") as datasource:
        yield datasource


def _import(datasource: DatasourceConfig, *fields: dict[str, Any]) -> ConversionResult:
    doc = flat([ds("orders", fields=[f("id", "o_orderkey"), *fields])])
    converter = OsiToSlayerConverter(documents=documents(doc), data_source=datasource.name, target_dialect="postgres")
    return run_converter(converter, datasource=datasource)


async def _by_id(datasource: DatasourceConfig, tmp_path: Path, datasets: list[SlayerDataset], name: str) -> list[Any]:
    async with pg_query_engine(datasource, base_dir=str(tmp_path / "store"), datasets=datasets) as engine:
        out = (await engine.execute({"population": "orders", "dimensions": ["id", name],
                                     "order": [{"field": "id"}]})).data
    return [r[f"orders.{name}"] for r in out]


# ── import ────────────────────────────────────────────────────────────────

async def test_ansi_and_target_native_kept_together_runs_the_target_entry(pg: DatasourceConfig, tmp_path: Path) -> None:
    result = _import(pg, f_dialects("pick", ("ANSI_SQL", "amount * 3"), ("POSTGRES", "amount * 4")))
    assert await _by_id(pg, tmp_path, result.datasets, "pick") == [40, -20, 28]


async def test_portable_function_rendered_for_postgres(pg: DatasourceConfig, tmp_path: Path) -> None:
    result = _import(pg, f("pos", "IFF(amount > 0, amount, 0)"))
    assert await _by_id(pg, tmp_path, result.datasets, "pos") == [10, 0, 7]


async def test_portable_expression_folded_by_the_target(pg: DatasourceConfig, tmp_path: Path) -> None:
    result = _import(pg, f("pos", "IFF(OrderAmount > 0, OrderAmount, 0)"))
    assert await _by_id(pg, tmp_path, result.datasets, "pos") == [3, 0, 0]


# ── export ────────────────────────────────────────────────────────────────

def _orders(datasource: DatasourceConfig, *fields: dict[str, Any]) -> SlayerDataset:
    return SlayerDataset.model_validate({
        "name": "orders", "sql_table": "orders", "data_source": datasource.name,
        "fields": [{"name": "id", "expression": "o_orderkey", "datatype": "Integer", "primary_key": True},
                   {"name": "amount", "datatype": "Integer"}, {"name": "qty", "datatype": "Integer"}, *fields]})


async def _exported_field(datasource: DatasourceConfig, tmp_path: Path, model: SlayerDataset,
                          name: str) -> dict[str, Any]:
    async with pg_query_engine(datasource, base_dir=str(tmp_path / "store"), datasets=[model]) as engine:
        document = (await export_osi(engine=engine, datasource=datasource.name)).document
    found = field(dataset(document, "orders"), name)
    assert found is not None
    return found


PART = {"name": "part", "expression": {"dialects": [{"dialect": "POSTGRES", "expression": "amount / qty"}]}}


async def test_dialect_only_expression(pg: DatasourceConfig, tmp_path: Path) -> None:
    part = await _exported_field(pg, tmp_path, _orders(pg, PART), "part")
    assert part["expression"] == {"dialects": [{"dialect": "POSTGRES", "expression": "amount / qty"}]}


async def test_a_portable_field_over_a_dialect_field(pg: DatasourceConfig, tmp_path: Path) -> None:
    ratio = {"name": "ratio", "expression": "orders.part * 2"}
    exported = await _exported_field(pg, tmp_path, _orders(pg, PART, ratio), "ratio")
    assert "sql" not in exported["expression"]
    (entry,) = exported["expression"]["dialects"]
    assert entry["dialect"] == "POSTGRES"
    assert entry["expression"].replace(" ", "") == "(amount/qty)*2"
