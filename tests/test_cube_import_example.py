"""Non-integration coverage for the ``docs/examples/15_cube_import`` walkthrough.

The example notebook itself only runs under ``pytest -m integration`` (the
``tests/integration/test_notebooks.py`` glob harness). This module guards the
sample Cube configs and the ``setup_cube.py`` helper in the DEFAULT unit suite:
it seeds the same deterministic DuckDB, imports the committed ``cube_project``
(both the library path and the real ``slayer import-cube`` CLI), and asserts the
variable contract, optional-block collapse, scalar→one-element-list coercion,
required-omitted and empty-list raises, and the view/join numbers — every answer
checked against gold SQL computed from the same rows.
"""
import json
import os
import sys

import pytest

duckdb = pytest.importorskip("duckdb")

from slayer.async_utils import run_sync  # noqa: E402
from slayer.core.models import DatasourceConfig  # noqa: E402
from slayer.core.query import (  # noqa: E402
    SlayerQuery,
    extract_model_variables,
    extract_variable_refs,
    list_valued_variable_names,
)
from slayer.engine.query_engine import SlayerQueryEngine  # noqa: E402
from slayer.inspect.model_render import render_model_skeleton  # noqa: E402
from slayer.storage.yaml_storage import YAMLStorage  # noqa: E402

_EXAMPLE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "docs", "examples", "15_cube_import")
)
sys.path.insert(0, _EXAMPLE_DIR)

import setup_cube  # noqa: E402

DS = setup_cube.DATASOURCE_NAME
EXPECTED_MODELS = {"orders", "customers", "order_facts", "orders_overview"}
_BOTH = ["Beverages", "Bakery"]


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
async def _prepare(tmp_path):
    """Seed DuckDB, import the cube_project (library path), register the
    datasource, and return ``(engine, storage, gold, db_path)``.

    Order matches the notebook: build → gold (read-only) → import (wipes the
    model store) → save datasource → engine. The datasource is saved AFTER the
    import because the importer wipes the model directory first.
    """
    db_path = tmp_path / "shop.duckdb"
    store = tmp_path / "slayer_models"
    setup_cube.build_shop_duckdb(db_path)
    gold = setup_cube.compute_gold(db_path)  # read-only, before SLayer opens RW

    result = setup_cube.import_cube_lib(models_dir=store)
    assert not result.report.has_errors, [
        i.message for i in result.report.issues if i.severity == "error"
    ]

    storage = YAMLStorage(base_dir=str(store))
    await storage.save_datasource(
        DatasourceConfig(name=DS, type="duckdb", database=str(db_path.resolve()))
    )
    return SlayerQueryEngine(storage=storage), storage, gold, db_path


def _sole(resp):
    assert resp.row_count == 1, f"expected 1 row: {resp.data}"
    assert len(resp.data[0]) == 1, f"expected 1 column: {resp.data[0]}"
    return next(iter(resp.data[0].values()))


def _unique(row, suffix):
    keys = [k for k in row if k == suffix or k.endswith("." + suffix)]
    assert len(keys) == 1, f"expected exactly one {suffix!r} key in {list(row)}"
    return row[keys[0]]


def _by_group(resp, *, group="region", value="total_amount"):
    return {_unique(r, group): _unique(r, value) for r in resp.data}


async def _count(engine, **variables):
    """Count order_facts rows with the given FILTER_PARAMS variables."""
    q = SlayerQuery(
        source_model="order_facts",
        measures=[{"formula": "count"}],
        variables=variables,
    )
    return _sole(await engine.execute(q))


# --------------------------------------------------------------------------- #
# fixture / gold are independently correct (guards a coordinated seed+gold bug)
# --------------------------------------------------------------------------- #
def test_gold_values_are_exact(tmp_path):
    db_path = tmp_path / "shop.duckdb"
    setup_cube.build_shop_duckdb(db_path)
    gold = setup_cube.compute_gold(db_path)
    assert gold["total"] == 1400
    assert {g["region"]: g["amount"] for g in gold["by_region"]} == {
        "North": 750,
        "South": 650,
    }
    assert (gold["of_all"], gold["of_north"], gold["of_south"], gold["of_beverages"]) == (
        6,
        4,
        2,
        3,
    )


# --------------------------------------------------------------------------- #
# import boundary: both the library result and the persisted CLI report
# --------------------------------------------------------------------------- #
def test_library_import_persists_all_models(tmp_path):
    store = tmp_path / "slayer_models"
    result = setup_cube.import_cube_lib(models_dir=store)
    assert not result.report.has_errors
    assert {m.name for m in result.models} == EXPECTED_MODELS
    fp_members = {
        i.member
        for i in result.report.issues
        if i.category.value == "filter_params_variable"
    }
    assert {"category", "region"} <= fp_members


def test_cli_import_writes_report_and_models(tmp_path):
    store = tmp_path / "slayer_models"
    report = tmp_path / "cube_import_report.json"
    proc = setup_cube.import_cube_cli(models_dir=store, report_path=report)
    assert proc.returncode == 0, proc.stderr

    data = json.loads(report.read_text())
    errors = [i for i in data["issues"] if i["severity"] == "error"]
    assert not errors, errors

    storage = YAMLStorage(base_dir=str(store))
    for name in EXPECTED_MODELS:
        model = run_sync(storage.get_model(name, data_source=DS))
        assert model is not None
        assert model.name == name

    fp_members = {
        i["member"] for i in data["issues"] if i["category"] == "filter_params_variable"
    }
    assert {"category", "region"} <= fp_members


# --------------------------------------------------------------------------- #
# variable contract (skeleton + meta.cube_variables + structural SQL)
# --------------------------------------------------------------------------- #
async def test_variable_contract_required_optional(tmp_path):
    _, storage, _, _ = await _prepare(tmp_path)
    model = await storage.get_model("order_facts", data_source=DS)
    mv = extract_model_variables(model)
    assert mv.required == ["category"]
    assert mv.optional == ["region"]


async def test_both_pushdowns_are_list_valued(tmp_path):
    _, storage, _, _ = await _prepare(tmp_path)
    model = await storage.get_model("order_facts", data_source=DS)
    assert list_valued_variable_names(model) == {"category", "region"}
    cube_vars = model.meta["cube_variables"]
    for name in ("category", "region"):
        spec = cube_vars[name]
        assert spec["member"] == name
        assert spec["kind"] == "string"
        assert spec["list_valued"] is True
    assert cube_vars["category"]["required"] is True
    assert cube_vars["region"]["required"] is False


async def test_skeleton_variables_line_exact(tmp_path):
    _, storage, _, _ = await _prepare(tmp_path)
    model = await storage.get_model("order_facts", data_source=DS)
    lines = render_model_skeleton(model=model).splitlines()
    var_line = next(ln for ln in lines if ln.startswith("Variables:"))
    assert var_line == "Variables: category (required), region"


async def test_required_bare_optional_blocked_in_sql(tmp_path):
    """Structural proof: the required var is bare (outside any ``{? ?}`` block)
    and the optional var lives inside one; all three tables are joined."""
    _, storage, _, _ = await _prepare(tmp_path)
    model = await storage.get_model("order_facts", data_source=DS)
    bare, blocked = extract_variable_refs(model.sql)
    assert "category" in bare
    assert "category" not in blocked
    assert "region" in blocked
    assert "region" not in bare
    low = model.sql.lower()
    assert "orders" in low
    assert "customers" in low
    assert "products" in low


# --------------------------------------------------------------------------- #
# YAML structural contract (join + clean view)
# --------------------------------------------------------------------------- #
async def test_orders_joins_customers(tmp_path):
    _, storage, _, _ = await _prepare(tmp_path)
    orders = await storage.get_model("orders", data_source=DS)
    assert "customers" in {j.target_model for j in orders.joins}


async def test_view_is_clean_facade(tmp_path):
    _, storage, _, _ = await _prepare(tmp_path)
    view = await storage.get_model("orders_overview", data_source=DS)
    assert "region" in {c.name for c in view.columns}
    assert not view.filters  # no default_filters -> no always-applied WHERE


# --------------------------------------------------------------------------- #
# FILTER_PARAMS behavior (counts vs gold)
# --------------------------------------------------------------------------- #
async def test_optional_omitted_is_unfiltered(tmp_path):
    engine, _, gold, _ = await _prepare(tmp_path)
    # required category supplied (both), optional region omitted -> block collapses
    assert await _count(engine, category=_BOTH) == gold["of_all"]


async def test_optional_list_filters(tmp_path):
    engine, _, gold, _ = await _prepare(tmp_path)
    assert await _count(engine, category=_BOTH, region=["North"]) == gold["of_north"]
    assert await _count(engine, category=_BOTH, region=["South"]) == gold["of_south"]


async def test_scalar_coerced_to_one_element_list(tmp_path):
    engine, _, gold, _ = await _prepare(tmp_path)
    scalar = await _count(engine, category=_BOTH, region="North")
    listed = await _count(engine, category=_BOTH, region=["North"])
    assert scalar == listed == gold["of_north"]
    # a scalar for the *required* list-valued var coerces too
    assert await _count(engine, category="Beverages") == gold["of_beverages"]


async def test_required_omitted_raises_naming_category(tmp_path):
    engine, _, _, _ = await _prepare(tmp_path)
    q = SlayerQuery(
        source_model="order_facts",
        measures=[{"formula": "count"}],
        variables={"region": ["North"]},  # category (required) omitted
    )
    with pytest.raises(ValueError, match=r"Undefined variable 'category'"):
        await engine.execute(q)


async def test_empty_list_raises_not_treated_as_omission(tmp_path):
    engine, _, _, _ = await _prepare(tmp_path)
    q = SlayerQuery(
        source_model="order_facts",
        measures=[{"formula": "count"}],
        variables={"category": _BOTH, "region": []},
    )
    with pytest.raises(ValueError, match=r"cannot be an empty list"):
        await engine.execute(q)


# --------------------------------------------------------------------------- #
# view + join (totals vs gold)
# --------------------------------------------------------------------------- #
async def test_view_total_by_region(tmp_path):
    engine, _, gold, _ = await _prepare(tmp_path)
    q = SlayerQuery(
        source_model="orders_overview",
        measures=["total_amount"],
        dimensions=["region"],
    )
    got = _by_group(await engine.execute(q))
    assert got == {g["region"]: g["amount"] for g in gold["by_region"]}


async def test_join_total_by_customer_region(tmp_path):
    engine, _, gold, _ = await _prepare(tmp_path)
    q = SlayerQuery(
        source_model="orders",
        measures=["total_amount"],
        dimensions=["customers.region"],
    )
    got = _by_group(await engine.execute(q))
    assert got == {g["region"]: g["amount"] for g in gold["by_region"]}
