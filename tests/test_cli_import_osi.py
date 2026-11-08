"""End-to-end CLI tests for `slayer import-osi`.

Run the importer over the legacy shop.yaml fixture (SQLite) and the current-spec
current_shop.yaml fixture (DuckDB), asserting saved models, queries and the report.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from slayer.async_utils import run_sync
from slayer.cli import _build_parser, _CommandFailed, _run_import_osi
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import disposable_engine
from tests._osi_helpers import DATASOURCE, FIXTURES, ds, f, f_dialects, flat, seed_db, write_yaml

_SCHEMA = [
    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, "
    "product_id INTEGER, amount REAL, quantity INTEGER, ordered_at DATE, status TEXT)",
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, region_id INTEGER, "
    "name TEXT, segment TEXT)",
    "CREATE TABLE products (product_id INTEGER PRIMARY KEY, category TEXT, price REAL)",
    "CREATE TABLE regions (region_id INTEGER PRIMARY KEY, name TEXT, population INTEGER)",
]


@pytest.fixture
def shop_setup(tmp_path: Path):
    db = tmp_path / "shop.db"
    with disposable_engine(f"sqlite:///{db}") as engine, engine.connect() as conn:
        for ddl in _SCHEMA:
            conn.execute(sa.text(ddl))
        conn.commit()

    store = tmp_path / "store"
    storage = YAMLStorage(base_dir=str(store))
    run_sync(storage.save_datasource(
        DatasourceConfig(name="testds", type="sqlite", database=str(db))
    ))
    return store, db


def _args(store: Path, path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        osi_path=str(path),
        datasource="testds",
        dialect="ANSI_SQL",
        storage=str(store),
        models_dir=None,
    )


def test_import_osi_saves_models_and_prints_report(shop_setup, capsys) -> None:
    store, _ = shop_setup
    _run_import_osi(_args(store, FIXTURES / "shop.yaml"))

    out = capsys.readouterr().out
    assert "orders" in out  # per-model summary printed

    storage = YAMLStorage(base_dir=str(store))
    names = run_sync(storage.list_models())
    assert {"orders", "customers", "products", "regions"}.issubset(set(names))

    orders = run_sync(storage.get_model("orders", data_source="testds"))
    assert orders is not None
    measure_names = {m.name for m in orders.measures}
    assert {"total_amount", "order_count", "aov"}.issubset(measure_names)
    # relationship -> join persisted
    assert any(j.target_model == "customers" for j in orders.joins)


def test_import_osi_nonexistent_path_exits(tmp_path: Path) -> None:
    store = tmp_path / "store"
    YAMLStorage(base_dir=str(store))
    args = _args(store, tmp_path / "does_not_exist.yaml")
    with pytest.raises(_CommandFailed):
        _run_import_osi(args)


def test_import_osi_missing_datasource_exits(tmp_path: Path) -> None:
    store = tmp_path / "empty_store"
    YAMLStorage(base_dir=str(store))  # no datasource registered
    args = _args(store, FIXTURES / "shop.yaml")
    with pytest.raises(SystemExit):
        _run_import_osi(args)


def test_import_osi_duplicate_datasets_exits_cleanly(
    shop_setup, tmp_path: Path
) -> None:
    # Duplicate dataset names raise OsiConversionError in the converter; the CLI
    # must catch it and exit non-zero with the message, not dump a traceback.
    store, _ = shop_setup
    osi = tmp_path / "dupe.yaml"
    osi.write_text(
        "version: '0.2.0.dev0'\n"
        "semantic_model:\n"
        "  - name: shop\n"
        "    datasets:\n"
        "      - name: orders\n"
        "        source: orders\n"
        "        fields:\n"
        "          - name: order_id\n"
        "            expression: {dialects: [{dialect: ANSI_SQL, expression: order_id}]}\n"
        "      - name: orders\n"
        "        source: customers\n"
        "        fields:\n"
        "          - name: customer_id\n"
        "            expression: {dialects: [{dialect: ANSI_SQL, expression: customer_id}]}\n"
    )
    args = _args(store, osi)
    with pytest.raises(_CommandFailed, match="Duplicate dataset names"):
        _run_import_osi(args)


# ─────────────────── current-spec document into DuckDB ─────────────────────

@pytest.fixture
def duck_store(tmp_path: Path) -> tuple[Path, Path]:
    db = seed_db(directory=tmp_path)
    store = tmp_path / "store"
    storage = YAMLStorage(base_dir=str(store))
    run_sync(storage.save_datasource(DatasourceConfig(name=DATASOURCE, type="duckdb", database=str(db))))
    return store, db


def _duck_args(store: Path, path: Path, *, dialect: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(osi_path=str(path), datasource=DATASOURCE, dialect=dialect, storage=str(store),
                           models_dir=None)


async def _rows(store: Path, db: Path, payload: dict) -> list[dict]:
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(store)))
    try:
        return (await engine.execute(payload)).data
    finally:
        engine.close()
        engine_factory.invalidate_engine(DatasourceConfig(name=DATASOURCE, type="duckdb", database=str(db)))


def test_current_spec_document_imports_end_to_end(duck_store, capsys) -> None:
    store, db = duck_store
    _run_import_osi(_duck_args(store, FIXTURES / "current_shop.yaml"))
    assert "0 unconverted" in capsys.readouterr().out

    storage = YAMLStorage(base_dir=str(store))
    orders = run_sync(storage.get_model("orders", data_source=DATASOURCE))
    assert orders is not None
    assert {c.name for c in orders.columns if not c.hidden} == {
        "order_id", "customer_id", "amount", "quantity", "status", "ordered_on", "tip_or_zero"}
    assert [j.join_pairs for j in orders.joins] == [[["customer_id", "id"]]]

    rows = run_sync(_rows(store, db, {"source_model": "orders",
                                      "measures": ["total_amount", "completed_amount", "order_count"]}))
    assert float(rows[0]["orders.total_amount"]) == pytest.approx(22)
    assert float(rows[0]["orders.completed_amount"]) == pytest.approx(17)
    assert rows[0]["orders.order_count"] == 3

    rows = run_sync(_rows(store, db, {"source_model": "orders", "dimensions": ["customers.segment"],
                                      "measures": ["total_amount"]}))
    assert {r["orders.customers.segment"]: float(r["orders.total_amount"]) for r in rows} == {"b2b": 15, "b2c": 7}

    loaded = {name: run_sync(storage.get_model(name, data_source=DATASOURCE)) for name in ("orders", "customers")}
    [home] = [name for name, model in loaded.items()
              if model is not None and any(x.name == "net_amount" for x in model.measures)]
    rows = run_sync(_rows(store, db, {"source_model": home, "measures": ["net_amount", "amount_per_customer"]}))
    assert float(rows[0][f"{home}.net_amount"]) == pytest.approx(19.5)
    assert float(rows[0][f"{home}.amount_per_customer"]) == pytest.approx(11)


def test_dialect_option_defaults_to_unset() -> None:
    args = _build_parser().parse_args(["import-osi", "shop.yaml", "--datasource", "ds"])
    assert args.dialect is None


def test_explicit_dialect_is_honoured_first(duck_store, tmp_path: Path) -> None:
    store, _ = duck_store
    doc = flat([ds("orders", fields=[f("order_id", "o_orderkey"),
                                     f_dialects("pick", ("SNOWFLAKE", "amount * 2"), sql="amount * 1")])])
    path = write_yaml(tmp_path / "doc.yaml", doc)
    _run_import_osi(_duck_args(store, path, dialect="SNOWFLAKE"))
    orders = run_sync(YAMLStorage(base_dir=str(store)).get_model("orders", data_source=DATASOURCE))
    assert orders is not None
    pick = orders.get_column("pick")
    assert pick is not None
    assert pick.sql == "amount * 2"
