"""End-to-end CLI test for `slayer import-osi`.

Registers a file-backed SQLite datasource, runs the importer over the crafted
shop.yaml fixture, and asserts models (with overlaid measures/joins) are saved
and the conversion report is printed.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from slayer.async_utils import run_sync
from slayer.cli import _run_import_osi
from slayer.core.models import DatasourceConfig
from slayer.storage.yaml_storage import YAMLStorage

FIXTURES = Path(__file__).parent / "fixtures" / "osi"

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
    engine = sa.create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        for ddl in _SCHEMA:
            conn.execute(sa.text(ddl))
        conn.commit()
    engine.dispose()

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
    with pytest.raises(SystemExit):
        _run_import_osi(args)


def test_import_osi_missing_datasource_exits(tmp_path: Path) -> None:
    store = tmp_path / "empty_store"
    YAMLStorage(base_dir=str(store))  # no datasource registered
    args = _args(store, FIXTURES / "shop.yaml")
    with pytest.raises(SystemExit):
        _run_import_osi(args)


def test_import_osi_duplicate_datasets_exits_cleanly(
    shop_setup, tmp_path: Path, capsys
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
    with pytest.raises(SystemExit) as exc_info:
        _run_import_osi(args)
    assert exc_info.value.code == 1
    assert "Duplicate dataset names" in capsys.readouterr().out
