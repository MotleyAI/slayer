"""End-to-end: import the anonymized RrDrivers JS fixture and execute it on
DuckDB (in-process, DEV-1730 acceptance bar).

Proves the FILTER_PARAMS-bearing single-CTE-chain cube is not just representable
but *executable* as a SLayer sql-mode model: required date pushdown, optional
categorical IN pushdowns, and the required-omitted clean raise.
"""
import os

import pytest

duckdb = pytest.importorskip("duckdb")

from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery, extract_model_variables
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.js_parser import parse_cube_js
from slayer.cube.models import CubeProject
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

_FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "cube_js", "rr_drivers.js"
)
_FULL_VARS = {"fulfillment_date_from": "2025-01-01", "fulfillment_date_to": "2025-12-31"}


def _seed(db_path: str) -> None:
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA analytics")
    con.execute(
        "CREATE TABLE analytics.fact_lines(fulfillment_date DATE, product_key INT, "
        "market TEXT, quantity_fulfilled INT, quantity_returned INT, date_returned DATE)"
    )
    con.execute("CREATE TABLE analytics.dim_items(product_key INT, category TEXT, brand TEXT)")
    con.executemany(
        "INSERT INTO analytics.dim_items VALUES (?,?,?)",
        [(1, "Shoes", "Acme"), (2, "Hats", "Zeta")],
    )
    con.executemany(
        "INSERT INTO analytics.fact_lines VALUES (?,?,?,?,?,?)",
        [
            ("2025-03-01", 1, "US", 10, 2, "2025-06-01"),
            ("2025-04-01", 1, "EU", 5, 1, None),
            ("2025-05-01", 2, "US", 8, 4, "2025-03-15"),
            ("2024-03-01", 1, "US", 6, 3, "2024-06-01"),
            ("2024-04-01", 2, "US", 4, 1, "2024-02-01"),
        ],
    )
    con.close()


async def _import_and_engine(tmp_path):
    db_path = str(tmp_path / "rr.duckdb")
    _seed(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="rr_ds", type="duckdb", database=db_path)
    )
    with open(_FIXTURE, encoding="utf-8") as fh:
        source = fh.read()
    parsed = parse_cube_js(source)
    assert not [i for i in parsed.issues if i.severity == "error"], \
        [i.message for i in parsed.issues]
    result = CubeToSlayerConverter(
        project=CubeProject(cubes=parsed.cubes, views=parsed.views),
        data_source="rr_ds", parse_issues=parsed.issues,
    ).convert()
    assert not result.report.has_errors, [i.message for i in result.report.issues if i.severity == "error"]
    model = next(m for m in result.models if m.name == "RrDrivers")
    await storage.save_model(model)
    return SlayerQueryEngine(storage=storage), model


def _sole_value(resp):
    assert resp.row_count == 1, f"expected 1 row: {resp.data}"
    return next(iter(resp.data[0].values()))


async def test_import_yields_expected_variable_contract(tmp_path):
    _, model = await _import_and_engine(tmp_path)
    v = extract_model_variables(model)
    assert set(v.required) == {"fulfillment_date_from", "fulfillment_date_to"}
    assert set(v.optional) == {"brand", "market", "category"}


async def test_count_with_optional_omitted_is_unfiltered(tmp_path):
    engine, _ = await _import_and_engine(tmp_path)
    q = SlayerQuery(
        source_model="RrDrivers", measures=[{"formula": "count"}], variables=_FULL_VARS,
    )
    resp = await engine.execute(q)
    assert _sole_value(resp) == 3


async def test_count_with_brand_pushdown(tmp_path):
    engine, _ = await _import_and_engine(tmp_path)
    q = SlayerQuery(
        source_model="RrDrivers", measures=[{"formula": "count"}],
        variables={**_FULL_VARS, "brand": ["Acme"]},
    )
    resp = await engine.execute(q)
    assert _sole_value(resp) == 2


async def test_grouped_max_measure(tmp_path):
    engine, _ = await _import_and_engine(tmp_path)
    q = SlayerQuery(
        source_model="RrDrivers", dimensions=["category"],
        measures=[{"formula": "qty_ty"}], variables=_FULL_VARS,
    )
    resp = await engine.execute(q)
    by_cat = {}
    for row in resp.data:
        cat = row["RrDrivers.category"]
        qty = next(v for k, v in row.items() if "qty_ty" in k)
        by_cat[cat] = qty
    assert by_cat == {"Shoes": 10, "Hats": 8}


async def test_required_filter_omitted_raises_and_names_variable(tmp_path):
    engine, _ = await _import_and_engine(tmp_path)
    q = SlayerQuery(
        source_model="RrDrivers", measures=[{"formula": "count"}],
        variables={"brand": ["Acme"]},  # required date vars omitted
    )
    with pytest.raises(Exception, match="fulfillment_date_from"):
        await engine.execute(q)
