"""Importers store physical SQL folded with the datasource's identifier rule, without a database server (spec:
importers/meaning-preservation, importers/cube-import, importers/osi-import)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sa
import sqlglot
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.elements import quoted_name
from sqlglot import exp

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.js_parser import parse_cube_js
from slayer.cube.models import CubeProject
from slayer.cube.report import CubeConversionResult
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject
from slayer.importers.physical import UndecidableIdentifierCaseError
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2171_fixtures import (
    column, cube_issues, cube_report_messages, run_cli, source_column, stored_model, typing_notes, write_yaml,
)
from tests._engine_helpers import _engine_generate, disposable_engine
from tests._osi_helpers import by_name, cols, convert, disable_type_probe, documents, ds, f, f_dialects, flat, seed_db

DS = "ds"
ITEMS = {"name": "items", "sql_table": "Sales.OrderItems",
         "dimensions": [{"name": "amount", "sql": "{CUBE}.OrderAmount", "type": "number"}],
         "measures": [{"name": "total", "type": "sum", "sql": "{CUBE}.OrderAmount"}]}


def _cube(*cubes: dict[str, Any], dialect: str | None = "postgres", engine: sa.Engine | None = None,
          project: CubeProject | None = None) -> CubeConversionResult:
    project = project or CubeProject.model_validate({"cubes": list(cubes)})
    return CubeToSlayerConverter(project=project, data_source=DS, target_dialect=dialect, sa_engine=engine).convert()


def _models(result: CubeConversionResult) -> dict[str, SlayerModel]:
    return {m.name: m for m in result.models}


def _columns_read(sql: str, *, dialect: str = "postgres") -> set[str]:
    return {c.name for c in sqlglot.parse_one(sql, read=dialect).find_all(exp.Column)}


# ── stored text per dialect ───────────────────────────────────────────────

def test_postgres_stores_the_folded_table_and_columns() -> None:
    items = _models(_cube(ITEMS))["items"]
    assert items.sql_table == "sales.orderitems"
    assert column(items, "amount").physical_name == "orderamount"
    assert source_column(items, "total").physical_name == "orderamount"


def test_a_dialect_that_does_not_fold_stores_as_written() -> None:
    items = _models(_cube(ITEMS, dialect="duckdb"))["items"]
    assert items.sql_table == "Sales.OrderItems"
    assert column(items, "amount").physical_name == "OrderAmount"


def test_quoted_names_literals_comments_and_logical_references_kept() -> None:
    result = _cube({"name": "orders", "sql_table": "orders", "dimensions": [
        {"name": "label", "sql": "CONCAT({CUBE}.\"Status\", 'Paid', {CUBE}.Region) -- Note", "type": "string"}]})
    assert column(_models(result)["orders"], "label").sql == "CONCAT(\"Status\", 'Paid', region) -- Note"


def test_names_local_to_a_source_query() -> None:
    result = _cube({"name": "recent", "dimensions": [{"name": "total", "sql": "{CUBE}.total", "type": "number"}],
                    "sql": "WITH Recent AS (SELECT Qty AS Total FROM Sales.OrderItems) "
                           "SELECT Total FROM Recent ORDER BY Total"})
    recent = _models(result)["recent"]
    assert recent.sql == ("WITH recent AS (SELECT qty AS total FROM sales.orderitems) "
                          "SELECT total FROM recent ORDER BY total")
    assert recent.get_column("total") is not None


def test_calculated_measure_with_a_mixed_case_operand() -> None:
    result = _cube({"name": "orders", "sql_table": "orders", "measures": [
        {"name": "paid_amount", "type": "number", "sql": "SUM(CASE WHEN {CUBE}.Status = 'paid' THEN {CUBE}.Amount END)"}]})
    operand = source_column(_models(result)["orders"], "paid_amount").sql
    assert operand is not None
    assert _columns_read(operand) == {"status", "amount"}
    assert "'paid'" in operand


def test_mixed_case_foreign_physical_join_keys() -> None:
    result = _cube(
        {"name": "orders", "sql_table": "orders",
         "dimensions": [{"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}],
         "joins": [{"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.CustId = {customers}.CustId"}]},
        {"name": "customers", "sql_table": "customers",
         "dimensions": [{"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}]},
    )
    models = _models(result)
    (join,) = models["orders"].joins
    ((src, tgt),) = join.join_pairs
    assert column(models["orders"], src).physical_name == "custid"
    assert column(models["customers"], tgt).physical_name == "custid"


# ── FILTER_PARAMS ─────────────────────────────────────────────────────────

def test_filter_params_string_bodies_fold_and_keep_their_variables() -> None:
    result = _cube({"name": "orders", "sql_table": "orders", "dimensions": [
        {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True},
        {"name": "CreatedOn", "sql": "{CUBE}.CreatedAt", "type": "time"},
        {"name": "brand", "sql": "{CUBE}.\"Brand\"", "type": "string"},
        {"name": "region", "sql": "{CUBE}.Region", "type": "string", "meta": {"required": True}},
    ], "segments": [{"name": "recent", "sql": (
        "{CUBE}.CreatedAt > '2020-01-01' AND {FILTER_PARAMS.orders.CreatedOn.filter('CreatedAt')} "
        "AND {FILTER_PARAMS.orders.brand.filter('\"Brand\"')} AND {FILTER_PARAMS.orders.region.filter('Region')}")}]})
    orders = _models(result)["orders"]
    assert column(orders, "recent").sql == (
        "createdat > '2020-01-01' AND {? createdat IN ({CreatedOn}) ?} "
        "AND {? \"Brand\" IN ({brand}) ?} AND region IN ({region})")
    assert column(orders, "CreatedOn").physical_name == "createdat"


def test_filter_params_arrow_body_folds() -> None:
    parsed = parse_cube_js(
        "cube(`orders`, {\n"
        "  sql: `SELECT * FROM Fol WHERE ${FILTER_PARAMS.orders.created.filter((from, to) => "
        "'Fol.CreatedAt >= ' + from + ' AND Fol.\"FD\" <= ' + to)}`,\n"
        "  dimensions: { created: { sql: `${CUBE}.CreatedAt`, type: `time` } }\n"
        "});\n")
    orders = _models(_cube(project=CubeProject(cubes=parsed.cubes, views=parsed.views)))["orders"]
    assert orders.sql == ("SELECT * FROM fol WHERE {? fol.createdat >= '{created_from}' "
                          "AND fol.\"FD\" <= '{created_to}' ?}")
    assert column(orders, "created").physical_name == "createdat"


# ── unparseable fragments ─────────────────────────────────────────────────

def test_unparseable_mixed_case_dimension_is_dropped() -> None:
    result = _cube({"name": "orders", "sql_table": "orders", "dimensions": [
        {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True},
        {"name": "doubled", "sql": "{CUBE}.Amount +* 2", "type": "number"}]})
    orders = _models(result)["orders"]
    assert orders.get_column("doubled") is None
    assert orders.get_column("id") is not None
    issues = cube_issues(result, "doubled")
    assert [i for i in issues if "Amount +* 2" in i.message and "identifier" in i.message.lower()], issues


def test_unparseable_mixed_case_source_query_drops_its_model() -> None:
    result = _cube({"name": "broken", "sql": "SELECT Qty FROM Sales.OrderItems WHERE ((",
                    "dimensions": [{"name": "qty", "sql": "{CUBE}.qty", "type": "number"}]},
                   {"name": "orders", "sql_table": "orders",
                    "dimensions": [{"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}]})
    assert set(_models(result)) == {"orders"}
    assert [i for i in cube_issues(result, "broken") if "identifier" in i.message.lower()]


def test_unparseable_mixed_case_table_name_drops_its_model() -> None:
    result = _cube({"name": "broken", "sql_table": "Sales.Order Items",
                    "dimensions": [{"name": "qty", "sql": "{CUBE}.qty", "type": "number"}]})
    assert "broken" not in _models(result)
    assert [i for i in cube_issues(result, "broken") if "identifier" in i.message.lower()]


# ── Snowflake / Oracle emission ───────────────────────────────────────────

@pytest.mark.parametrize("dialect", ["snowflake", "oracle"])
async def test_folding_dialect_generates_unquoted_folded_names(dialect: str) -> None:
    items = _models(_cube(ITEMS, dialect=dialect))["items"]
    sql = await _engine_generate(query=SlayerQuery.model_validate({"source_model": "items", "dimensions": ["amount"]}),
                                 model=items, dialect=dialect)
    tree = sqlglot.parse_one(sql, read=dialect)
    (table,) = tree.find_all(exp.Table)
    assert [(p.name, p.quoted) for p in cast(list[exp.Identifier], table.parts)] == [
        ("sales", False), ("orderitems", False)]
    physical = [c.this for c in tree.find_all(exp.Column) if c.name.lower() == "orderamount"]
    assert physical
    assert all((i.name, i.quoted) == ("orderamount", False) for i in physical)


@pytest.fixture
def snowflake_reflection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[sa.Engine]:
    """An engine whose live table ``t`` reflects as Snowflake would hold ``FOO``, ``"foo"`` and ``"Foo"``."""
    reflected = [{"name": "foo", "type": sa.INTEGER()}, {"name": quoted_name("foo", True), "type": sa.INTEGER()},
                 {"name": "Foo", "type": sa.INTEGER()}]
    monkeypatch.setattr(Inspector, "get_columns", lambda _self, _table, schema=None, **_kw: list(reflected))
    with disposable_engine(f"sqlite:///{tmp_path / 'live.db'}") as engine:
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE t (x INTEGER)"))
            conn.commit()
        yield engine


def test_folded_and_exact_live_spellings_on_snowflake(snowflake_reflection: sa.Engine) -> None:
    result = _cube({"name": "t", "sql_table": "t", "dimensions": [{"name": "f", "sql": "{CUBE}.foo", "type": "number"}]},
                   dialect="snowflake", engine=snowflake_reflection)
    assert _models(result)["t"].get_column("f") is None
    assert [i for i in cube_issues(result, "f") if "ambiguous" in i.message and "foo" in i.message]


# ── an import whose identifier case cannot be determined ──────────────────

def test_offline_mixed_case_cube_is_refused() -> None:
    with pytest.raises(UndecidableIdentifierCaseError) as raised:
        _cube(ITEMS, dialect=None)
    assert "Sales" in str(raised.value)
    assert "OrderItems" in str(raised.value)


def test_offline_lowercase_cube_imports() -> None:
    result = _cube({"name": "items", "sql_table": "sales.orderitems",
                    "dimensions": [{"name": "amount", "sql": "{CUBE}.order_amount", "type": "number"}]}, dialect=None)
    assert _models(result)["items"].sql_table == "sales.orderitems"


@pytest.mark.parametrize("semantic_model, raw_code, named", [
    ({"name": "items", "model": "OrderItems", "dimensions": [{"name": "qty", "type": "categorical"}]}, None,
     "OrderItems"),
    ({"name": "items", "model": "src", "dimensions": [{"name": "qty", "type": "categorical"}]},
     "SELECT Qty AS qty FROM orders", "Qty"),
    ({"name": "items", "model": "orders", "dimensions": [{"name": "Qty", "type": "categorical"}]}, None, "Qty"),
])
def test_offline_mixed_case_dbt_is_refused(semantic_model: dict[str, Any], raw_code: str | None, named: str) -> None:
    regular = [{"name": "src", "raw_code": raw_code}] if raw_code else []
    project = DbtProject.model_validate({"semantic_models": [semantic_model], "regular_models": regular})
    converter = DbtToSlayerConverter(project=project, data_source=DS)
    with pytest.raises(UndecidableIdentifierCaseError) as raised:
        converter.convert()
    assert named in str(raised.value)


def _cube_project(directory: Path, cube: dict[str, Any]) -> Path:
    write_yaml(directory / "model" / "cubes.yml", {"cubes": [cube]})
    return directory


def _dbt_project(directory: Path) -> Path:
    write_yaml(directory / "models" / "schema.yml", {"semantic_models": [
        {"name": "items", "model": "ref('OrderItems')", "dimensions": [{"name": "qty", "type": "categorical"}]}]})
    return directory


@pytest.mark.parametrize("command", ["import-cube", "import-dbt"])
def test_offline_mixed_case_cli_import_fails_cleanly(tmp_path: Path, command: str,
                                                     capsys: pytest.CaptureFixture[str]) -> None:
    project = (_cube_project(tmp_path / "cube", ITEMS) if command == "import-cube"
               else _dbt_project(tmp_path / "dbt"))
    store = tmp_path / "store"
    code = run_cli(command, str(project), "--datasource", "missing_ds", "--storage", str(store))
    err = capsys.readouterr().err
    assert code != 0
    assert "OrderItems" in err
    assert command != "import-cube" or "Sales" in err
    assert "Traceback" not in err
    assert asyncio.run(YAMLStorage(base_dir=str(store)).list_models()) == []


@pytest.mark.parametrize("command", ["import-cube", "import-dbt"])
def test_offline_lowercase_cli_import_succeeds(tmp_path: Path, command: str) -> None:
    if command == "import-cube":
        project = _cube_project(tmp_path / "cube", {"name": "items", "sql_table": "sales.orderitems", "dimensions": [
            {"name": "qty", "sql": "{CUBE}.qty", "type": "number"}]})
    else:
        project = tmp_path / "dbt"
        write_yaml(project / "models" / "schema.yml", {"semantic_models": [
            {"name": "items", "model": "ref('orderitems')", "dimensions": [{"name": "qty", "type": "categorical"}]}]})
    store = tmp_path / "store"
    assert run_cli(command, str(project), "--datasource", "missing_ds", "--storage", str(store)) == 0
    expected = "sales.orderitems" if command == "import-cube" else "orderitems"
    assert stored_model(store, "items").sql_table == expected


def test_configured_but_unreachable_datasource_folds(tmp_path: Path) -> None:
    store = tmp_path / "store"
    datasource = DatasourceConfig(name=DS, type="postgres", host="127.0.0.1", port=1, database="shop",
                                  username="nobody", password="")
    asyncio.run(YAMLStorage(base_dir=str(store)).save_datasource(datasource))
    try:
        code = run_cli("import-cube", str(_cube_project(tmp_path / "cube", ITEMS)), "--datasource", DS,
                       "--storage", str(store))
    finally:
        engine_factory.invalidate_engine(datasource)
    assert code == 0
    assert stored_model(store, "items").sql_table == "sales.orderitems"
    assert typing_notes(i["message"] for i in cube_report_messages(store) if i["severity"] == "info")


# ── OSI ───────────────────────────────────────────────────────────────────

@pytest.fixture
def osi_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    disable_type_probe(monkeypatch)
    return seed_db(directory=tmp_path, statements=[
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, orderamount DOUBLE)", "INSERT INTO orders VALUES (1, 2.5)"])


def _osi_sql(db: Path, field: dict[str, Any], *, target: str) -> str | None:
    result = convert(db=db, docs=documents(flat([ds("orders", fields=[f("id"), field])])), target_dialect=target)
    found = cols(by_name(result)["orders"]).get(field["name"])
    return found.sql if found is not None else None


def test_osi_target_native_kept_verbatim(osi_db: Path) -> None:
    assert _osi_sql(osi_db, f_dialects("pick", ("SNOWFLAKE", "orderamount::NUMBER(10,2)")),
                    target="snowflake") == "orderamount::NUMBER(10,2)"


def test_osi_target_native_mixed_case_identifier_folded(osi_db: Path) -> None:
    assert _osi_sql(osi_db, f_dialects("pick", ("SNOWFLAKE", "OrderAmount::NUMBER(10,2)")),
                    target="snowflake") == "orderamount::NUMBER(10,2)"


def test_osi_portable_expression_folded_by_the_target(osi_db: Path) -> None:
    sql = _osi_sql(osi_db, f("pos", "IFF(OrderAmount > 0, OrderAmount, 0)"), target="postgres")
    assert sql is not None
    assert _columns_read(sql) == {"orderamount"}


def test_osi_candidate_read_with_its_dialect_folded_by_the_target(osi_db: Path) -> None:
    sql = _osi_sql(osi_db, f_dialects("pick", ("BIGQUERY", "OrderAmount * 5")), target="postgres")
    assert sql is not None
    assert _columns_read(sql) == {"orderamount"}


def test_osi_candidates_differing_in_case(osi_db: Path) -> None:
    sql = _osi_sql(osi_db, f_dialects("pick", ("SNOWFLAKE", "ORDERAMOUNT * 2"), ("POSTGRES", "OrderAmount * 3")),
                   target="postgres")
    assert sql == "orderamount * 3"


def test_osi_field_matches_a_live_column_case_insensitively_on_duckdb(osi_db: Path) -> None:
    assert _osi_sql(osi_db, f("pick", "OrderAmount * 5"), target="duckdb") == "OrderAmount * 5"
    assert _osi_sql(osi_db, f("bare", "OrderAmount"), target="duckdb") is not None
