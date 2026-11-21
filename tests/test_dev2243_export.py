"""Spec: importers/osi-export (native Ossie semantic-model documents)."""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
import sqlglot
import yaml

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.field_expression import portable_text
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.core import slayer_extension as ext
from slayer.sql.ossie_dialects import source_digest
from slayer.osi.exporter import export_osi
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import open_connection, transaction
from tests._cli_inprocess import run_cli_in_process
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2243_fixtures import (
    DS, SALES, dataset, document_text, field, importer_module, make_storage, semantic_model_module,
    store_document, upstream_validate,
)
from tests._dev2277_fixtures import SEED_TABLES
from tests._engine_helpers import owned_query_engine
from tests._dataset_access_fixtures import mcp_text
from tests._osi_export_fixtures import (
    BASIC_METRICS, DATASOURCE as EXPORT_DS, EXPORT_DDL, customers, monthly_revenue, orders, slayer_metric, visits,
)

SHOP_EXAMPLE = Path(__file__).parent.parent / "docs" / "examples" / "13_osi_import" / "shop.osi.yaml"
#: The demo DuckDB of ``docs/examples/13_osi_import/setup_osi.py``.
SHOP_EXAMPLE_DDL = [
    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER, amount DOUBLE, "
    "quantity INTEGER, ordered_at DATE, status VARCHAR)",
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, region_id INTEGER, name VARCHAR, segment VARCHAR)",
    "CREATE TABLE products (product_id INTEGER PRIMARY KEY, category VARCHAR, price DOUBLE)",
    "CREATE TABLE regions (region_id INTEGER PRIMARY KEY, name VARCHAR, population INTEGER)",
    "INSERT INTO regions VALUES (1, 'North', 1000), (2, 'South', 2000)",
    "INSERT INTO customers VALUES (1, 1, 'Alice', 'consumer'), (2, 1, 'Bob', 'business'), (3, 2, 'Carol', 'consumer')",
    "INSERT INTO products VALUES (1, 'Beverages', 5.0), (2, 'Bakery', 3.0)",
    "INSERT INTO orders VALUES (1, 1, 1, 100.0, 2, '2024-01-01', 'completed'), "
    "(2, 1, 2, 200.0, 1, '2024-01-05', 'completed'), (3, 2, 1, 300.0, 3, '2024-02-01', 'completed'), "
    "(4, 2, 2, 150.0, 1, '2024-02-10', 'pending'), (5, 3, 1, 250.0, 5, '2024-03-01', 'completed'), "
    "(6, 3, 2, 400.0, 2, '2024-03-15', 'completed')",
]

SUPPORT = document_text(name="support", description="Returns.", datasets=[
    dataset("returns", primary_key=["return_id"], fields=[field("return_id"), field("customer_id"),
                                                          field("amount", datatype="Float")]),
])
STORES = SlayerDataset(
    name="stores", sql_table="stores", data_source=DS,
    fields=[DatasetField(name="store_id", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.STRING)],
    relationships=[DatasetRelationship(name="stores_to_orders", to="orders", from_columns=["customer_id"],
                                       to_columns=["customer_id"],
                                       cardinality=RelationshipCardinality.MANY_TO_ONE)],
)
BIG_ORDERS = SlayerDataset(
    name="big_orders", sql="SELECT order_id, amount FROM orders WHERE amount > 100", data_source=DS,
    fields=[DatasetField(name="order_id", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="amount", datatype=DataType.FLOAT)],
)

LINES = TableSpec(name="lines", columns=[("line_id", "INT"), ("quantity", "INT"), ("amount", "DOUBLE")],
                  rows=[(1, 2, 10.0), (2, 3, 5.5)])
LINES_DOC = """\
version: 0.3.0.dev0
name: lines_model
# Metrics read source columns no field declares.
datasets:
  - name: lines
    source: lines
    primary_key: [line_id]
    fields:
      - name: line_id
        expression: {sql: line_id}
        datatype: Integer
    metrics:
      - name: units
        expression: {sql: SUM(source.quantity)}
      - name: revenue
        expression: {sql: SUM(source.amount)}
"""
LINES_QUERY = {"population": "lines", "metrics": ["lines.units", "lines.revenue"]}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def seed_sql(path: Path, statements: list[str], *, dialect: str) -> None:
    if dialect == "sqlite":
        with transaction(path) as con:
            for statement in statements:
                con.execute(statement)
        return
    with contextlib.closing(duckdb.connect(str(path))) as con:
        for statement in statements:
            con.execute(statement)


def seeded_config(base: Path, *, tables: list[TableSpec] | None = None, statements: list[str] | None = None,
                  dialect: str = "duckdb", db_name: str | None = None, data_source: str = DS) -> DatasourceConfig:
    """Seed a database under ``base`` (Appendix B unless ``statements``/``tables`` say otherwise); its config."""
    base.mkdir(parents=True, exist_ok=True)
    db = base / (db_name or f"seed.{'duckdb' if dialect == 'duckdb' else 'db'}")
    if statements is not None:
        seed_sql(db, statements, dialect=dialect)
    else:
        seed_backend(dialect, str(db), tables or SEED_TABLES)
    return DatasourceConfig(name=data_source, type=dialect, database=str(db))


@contextlib.asynccontextmanager
async def engine_over(base: Path, config: DatasourceConfig, *, documents: tuple[str, ...] = (),
                      datasets: tuple[SlayerDataset, ...] = ()) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over a fresh YAML store in ``base`` holding ``documents`` and standalone ``datasets``."""
    async with owned_query_engine(storage=make_storage("yaml", base), datasources=[config]) as engine:
        for text in documents:
            await store_document(engine.storage, text, data_source=config.name)
        for model in datasets:
            await engine.save_dataset(model.model_copy(update={"data_source": config.name}))
        yield engine


async def export(engine: SlayerQueryEngine, *, data_source: str = DS, datasets: list[str] | None = None) -> Any:
    return await export_osi(engine=engine, datasource=data_source, datasets=datasets, extensions=True)


async def import_text(storage: StorageBackend, text: str, *, base: Path, data_source: str = DS) -> Any:
    path = base / "exported.osi.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    result = await importer_module().import_osi(storage=storage, path=path, data_source=data_source)
    assert result.refused == [], result.refused
    return result


def assert_valid(doc: dict[str, Any]) -> None:
    assert doc["version"] == "0.3.0.dev0"
    assert semantic_model_module().ossie_validity_errors(doc) == []


def metric_of(container: dict[str, Any], name: str) -> dict[str, Any] | None:
    return next((m for m in container.get("metrics") or [] if m["name"] == name), None)


def ds_of(doc: dict[str, Any], name: str) -> dict[str, Any]:
    [hit] = [d for d in doc["datasets"] if d["name"] == name]
    return hit


def same_sql(actual: str, expected: str) -> bool:
    return sqlglot.parse_one(actual).sql() == sqlglot.parse_one(expected).sql()


def sql_of(entity: dict[str, Any] | None) -> str:
    assert entity is not None
    return entity["expression"]["sql"]


def strings(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        for value in node.values():
            yield from strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from strings(value)
    elif isinstance(node, str):
        yield node


def slayer_entries(entity: dict[str, Any]) -> list[dict[str, Any]]:
    return [e for e in entity.get("nonportable_extensions") or [] if e["vendor_name"] == "SLAYER"]


async def values(engine: SlayerQueryEngine, payload: dict[str, Any]) -> list[tuple[Any, ...]]:
    rows = (await engine.execute(payload)).data
    return sorted((tuple(sorted((k, round(float(v), 6) if isinstance(v, (int, float)) else v) for k, v in r.items()))
                   for r in rows), key=repr)


def _no_database(_self: SlayerQueryEngine, _datasource: DatasourceConfig) -> Any:
    raise AssertionError("database accessed")


# --------------------------------------------------------------------------- #
# Export writes one current-spec document per datasource
# --------------------------------------------------------------------------- #
async def test_exported_document_validates(tmp_path: Path) -> None:
    """Exported document validates."""
    config = seeded_config(tmp_path, statements=EXPORT_DDL, data_source=EXPORT_DS)
    async with engine_over(tmp_path, config,
                           datasets=(customers(), orders(metrics=BASIC_METRICS), visits(), monthly_revenue())) as engine:
        result = await export(engine, data_source=EXPORT_DS)
    assert yaml.safe_load(result.text) == result.document
    assert_valid(result.document)


async def test_a_single_document_datasource_exports_verbatim(tmp_path: Path) -> None:
    """A single-document datasource exports verbatim."""
    assert "# owned by finance" in SALES
    async with engine_over(tmp_path, seeded_config(tmp_path), documents=(SALES,)) as engine:
        result = await export(engine)
    assert result.text == SALES
    assert result.document == yaml.safe_load(SALES)


def _orders_slayer_entry() -> dict[str, Any]:
    """A valid ``SLAYER`` entry hiding ``orders``."""
    payload = ext.DatasetPayload(entity="orders", source=ext.SourceStructure(sql_table="orders"), hidden=True)
    return ext.encode(payload, ossie=source_digest("orders", reader="duckdb"))


SLAYER_LINES = ["      - vendor_name: SLAYER\n", f"        data: {json.dumps(_orders_slayer_entry()['data'])}\n"]
#: ``SALES`` whose ``orders`` carries a ``DBT`` entry and a ``SLAYER`` entry.
SALES_EXTENDED = SALES.replace(
    "    primary_key: [order_id]\n",
    "    primary_key: [order_id]\n    nonportable_extensions:\n      - vendor_name: DBT\n"
    "        data: {\"model\": \"fct_orders\"}\n" + "".join(SLAYER_LINES), 1)
SALES_EXTENDED_STRIPPED = SALES_EXTENDED.replace("".join(SLAYER_LINES), "", 1)


async def test_no_extensions_strips_only_slayer_entries(tmp_path: Path) -> None:
    """Spec scenario: No extensions strips only SLAYER entries."""
    assert "vendor_name: DBT" in SALES_EXTENDED_STRIPPED
    assert "SLAYER" not in SALES_EXTENDED_STRIPPED
    async with engine_over(tmp_path, seeded_config(tmp_path), documents=(SALES_EXTENDED,)) as engine:
        orders_ds = await engine.storage.get_dataset("orders", data_source=DS)
        assert orders_ds is not None
        assert orders_ds.hidden
        full = await export_osi(engine=engine, datasource=DS, extensions=True)
        stripped = await export_osi(engine=engine, datasource=DS, extensions=False)
    assert full.text == SALES_EXTENDED
    assert stripped.text == SALES_EXTENDED_STRIPPED
    assert stripped.document == yaml.safe_load(SALES_EXTENDED_STRIPPED)


async def test_the_cli_no_extensions_flag_strips_only_slayer_entries(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    await storage.save_datasource(seeded_config(tmp_path / "db"))
    await store_document(storage, SALES_EXTENDED)
    out = tmp_path / "sales.osi.yaml"
    result = run_cli_in_process(["export-osi", "--datasource", DS, "--out", str(out), "--no-extensions",
                                 "--storage", str(tmp_path / "store")])
    assert result.returncode == 0, result.stderr
    assert out.read_text(encoding="utf-8") == SALES_EXTENDED_STRIPPED


async def _merged(base: Path) -> tuple[str, DatasourceConfig]:
    config = seeded_config(base)
    async with engine_over(base, config, documents=(SALES, SUPPORT), datasets=(STORES,)) as engine:
        return (await export(engine)).text, config


async def test_two_documents_and_a_standalone_dataset_merge(tmp_path: Path) -> None:
    """Two documents and a standalone dataset merge."""
    text, config = await _merged(tmp_path / "src")
    doc = yaml.safe_load(text)
    assert_valid(doc)
    assert doc["name"] == DS
    assert [d["name"] for d in doc["datasets"]] == ["customers", "orders", "returns", "stores"]
    assert any(r["from"] == "stores" and r["to"] == "orders" for r in doc["relationships"])
    assert metric_of(doc, "revenue_per_customer") is not None
    assert slayer_entries(doc), "document boundaries travel in a document-level SLAYER entry"
    storage = make_storage("yaml", tmp_path / "dst")
    await storage.save_datasource(config)
    await import_text(storage, text, base=tmp_path / "dst")
    assert sorted(await storage.list_semantic_models(DS)) == [(DS, "sales"), (DS, "support")]
    assert await storage.semantic_model_owner("orders", DS) == "sales"
    assert await storage.semantic_model_owner("returns", DS) == "support"
    assert await storage.semantic_model_owner("stores", DS) is None
    restored_stores = await storage.get_dataset("stores", data_source=DS)
    assert restored_stores is not None
    assert [r.to for r in restored_stores.relationships] == ["orders"]
    sales_text = await storage.get_semantic_model_text("sales", DS)
    assert sales_text is not None
    sales = yaml.safe_load(sales_text)
    assert sales["description"] == "Orders and customers."
    assert sorted(d["name"] for d in sales["datasets"]) == ["customers", "orders"]
    assert [m["name"] for m in sales["metrics"]] == ["revenue_per_customer"]


async def test_deterministic_output(tmp_path: Path) -> None:
    """Deterministic output."""
    async with engine_over(tmp_path, seeded_config(tmp_path), documents=(SALES, SUPPORT), datasets=(STORES,)) as engine:
        first, second = await export(engine), await export(engine)
    assert first.text == second.text


async def test_no_direct_database_access_for_table_and_sql_mode_models(tmp_path: Path,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """No direct database access for table and sql-mode models."""
    async with engine_over(tmp_path, seeded_config(tmp_path), documents=(SALES,),
                           datasets=(STORES, BIG_ORDERS)) as engine:
        monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)
        result = await export(engine)
    assert [d["name"] for d in yaml.safe_load(result.text)["datasets"]] == ["big_orders", "customers", "orders", "stores"]


async def test_imported_source_columns_round_trip(tmp_path: Path) -> None:
    """Imported source columns round trip."""
    config = seeded_config(tmp_path / "db", tables=[LINES])
    async with engine_over(tmp_path / "first", config) as engine:
        await import_text(engine.storage, LINES_DOC, base=tmp_path / "in")
        before = await values(engine, LINES_QUERY)
        first = (await export(engine)).text
    assert before == [(("lines.revenue", 15.5), ("lines.units", 5.0))]
    assert first == LINES_DOC
    assert_valid(yaml.safe_load(first))
    async with engine_over(tmp_path / "second", config) as engine:
        await import_text(engine.storage, first, base=tmp_path / "again")
        second = (await export(engine)).text
        assert await values(engine, LINES_QUERY) == before
    assert second == LINES_DOC


async def test_exported_documents_validate_upstream(tmp_path: Path) -> None:
    """Exported document validates (pinned upstream validator)."""
    text, _ = await _merged(tmp_path / "merged")
    merged = tmp_path / "merged.osi.yaml"
    merged.write_text(text, encoding="utf-8")
    config = seeded_config(tmp_path / "std", statements=EXPORT_DDL, data_source=EXPORT_DS)
    async with engine_over(tmp_path / "std", config,
                           datasets=(customers(), orders(metrics=BASIC_METRICS), visits(), monthly_revenue())) as engine:
        standalone = tmp_path / "standalone.osi.yaml"
        standalone.write_text((await export(engine, data_source=EXPORT_DS)).text, encoding="utf-8")
    done = upstream_validate([merged, standalone], tmp=tmp_path)
    assert done.returncode == 0, done.stdout + done.stderr


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #
async def export_standalone(base: Path, *models: SlayerDataset, dialect: str = "duckdb") -> dict[str, Any]:
    config = seeded_config(base, statements=EXPORT_DDL, dialect=dialect, data_source=EXPORT_DS)
    async with engine_over(base, config, datasets=models) as engine:
        return (await export(engine, data_source=EXPORT_DS)).document


async def test_dataset_scoped_metric(tmp_path: Path) -> None:
    """Dataset-scoped metric."""
    doc = await export_standalone(tmp_path, customers(), orders(metrics=BASIC_METRICS))
    assert same_sql(sql_of(metric_of(ds_of(doc, "orders"), "revenue")), "SUM(amount)")


async def test_measure_over_another_model(tmp_path: Path) -> None:
    """Measure over another model."""
    model = orders(metrics=[*BASIC_METRICS, slayer_metric("customer_discount", "sum(customers.discount)")])
    doc = await export_standalone(tmp_path / "src", customers(), model)
    assert any(same_sql(sql_of(m), "SUM(discount)") for m in ds_of(doc, "customers").get("metrics") or [])
    config = seeded_config(tmp_path / "db", statements=EXPORT_DDL, data_source=EXPORT_DS)
    async with engine_over(tmp_path / "dst", config) as engine:
        await import_text(engine.storage, yaml.safe_dump(doc, sort_keys=False), base=tmp_path / "in",
                          data_source=EXPORT_DS)
        [row] = (await engine.execute({"population": "orders", "metrics": ["orders.customer_discount"]})).data
    assert float(row["orders.customer_discount"]) == pytest.approx(3.5)


async def test_cross_model_ratio(tmp_path: Path) -> None:
    """Cross-model ratio."""
    ratio = slayer_metric("aov_per_customer", "sum(amount) / count_distinct(customers.id)")
    doc = await export_standalone(tmp_path, customers(), orders(metrics=[*BASIC_METRICS, ratio]))
    assert same_sql(sql_of(metric_of(doc, "aov_per_customer")), "SUM(orders.amount) / COUNT(DISTINCT customers.id)")


async def test_measure_reference_expanded(tmp_path: Path) -> None:
    """Measure reference expanded."""
    doc = await export_standalone(tmp_path, customers(), orders(metrics=BASIC_METRICS))
    assert same_sql(sql_of(metric_of(ds_of(doc, "orders"), "aov")), "SUM(amount) / COUNT(*)")


async def test_model_scoped_reference_to_a_dataset_metric_uses_agg(tmp_path: Path) -> None:
    """Model-scoped reference to a dataset metric uses AGG."""
    measures = [slayer_metric("revenue", "sum(amount)"),
                slayer_metric("revenue_per_customer", "revenue / count_distinct(customers.id)")]
    doc = await export_standalone(tmp_path, customers(), orders(metrics=measures))
    assert same_sql(sql_of(metric_of(doc, "revenue_per_customer")),
                    "AGG(orders.revenue) / COUNT(DISTINCT customers.id)")


async def test_percentile_on_sqlite(tmp_path: Path) -> None:
    """Percentile on SQLite."""
    doc = await export_standalone(tmp_path, customers(), orders(metrics=[slayer_metric("p90", "percentile(amount, 0.9)")]),
                                  dialect="sqlite")
    assert same_sql(sql_of(metric_of(ds_of(doc, "orders"), "p90")),
                    "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)")


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
async def test_boolean_sum(tmp_path: Path, dialect: str) -> None:
    """Boolean sum."""
    doc = await export_standalone(tmp_path, customers(), orders(metrics=[slayer_metric("paid", "sum(is_paid)")]),
                                  dialect=dialect)
    rendered = sqlglot.transpile(sql_of(metric_of(ds_of(doc, "orders"), "paid")), write=dialect)[0]
    db = tmp_path / f"seed.{'duckdb' if dialect == 'duckdb' else 'db'}"
    connect = open_connection if dialect == "sqlite" else (lambda p: contextlib.closing(duckdb.connect(p)))
    with connect(str(db)) as con:
        assert con.execute(f"SELECT {rendered} FROM orders").fetchall() == [(3,)]


async def test_stored_model_scoped_metric_exports_unchanged(tmp_path: Path) -> None:
    async with engine_over(tmp_path, seeded_config(tmp_path), documents=(SALES,), datasets=(STORES,)) as engine:
        doc = yaml.safe_load((await export(engine)).text)
    assert metric_of(doc, "revenue_per_customer") == yaml.safe_load(SALES)["metrics"][0]


async def test_metric_over_a_model_scoped_metric_travels_only_in_slayer(tmp_path: Path) -> None:
    async with engine_over(tmp_path, seeded_config(tmp_path), documents=(SALES,), datasets=(STORES,)) as engine:
        await mcp_text(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                       metrics=[{"name": "arpu_pct", "expression": "revenue_per_customer * 100"}])
        doc = yaml.safe_load((await export(engine)).text)
    assert metric_of(doc, "arpu_pct") is None
    assert all(metric_of(d, "arpu_pct") is None for d in doc["datasets"])
    assert any("arpu_pct" in s for e in slayer_entries(doc) for s in strings(e["data"]))
    assert_valid(doc)


# --------------------------------------------------------------------------- #
# Round trip
# --------------------------------------------------------------------------- #
EXAMPLE_QUERIES: list[dict[str, Any]] = [
    {"population": "orders", "metrics": ["orders.total_amount", "orders.completed_amount", "orders.order_count",
                                         "orders.aov", "orders.revenue_line"]},
    {"population": "orders", "dimensions": ["orders.status"],
     "metrics": ["orders.total_amount", "orders.completed_amount", "orders.order_count", "orders.aov"]},
    {"metrics": ["cust_reach", "rev_plus_pop"]},
    {"population": "orders", "dimensions": ["orders.order_id", "orders.customer_id", "orders.product_id",
                                            "orders.amount", "orders.quantity", "orders.ordered_at", "orders.status"]},
    {"population": "customers", "dimensions": ["customers.customer_id", "customers.region_id", "customers.name",
                                               "customers.segment"]},
    {"population": "products", "dimensions": ["products.product_id", "products.category", "products.price"]},
    {"population": "regions", "dimensions": ["regions.region_id", "regions.name", "regions.population"]},
]


async def test_worked_example_round_trip(tmp_path: Path) -> None:
    """Worked example round trip."""
    original = SHOP_EXAMPLE.read_text(encoding="utf-8")
    config = seeded_config(tmp_path / "db", statements=SHOP_EXAMPLE_DDL, db_name="shop.duckdb")
    async with engine_over(tmp_path / "first", config) as engine:
        await import_text(engine.storage, original, base=tmp_path / "in")
        first = [await values(engine, q) for q in EXAMPLE_QUERIES]
        exported = (await export(engine)).text
    assert exported == original
    async with engine_over(tmp_path / "second", config) as engine:
        await import_text(engine.storage, exported, base=tmp_path / "again")
        assert [await values(engine, q) for q in EXAMPLE_QUERIES] == first
        assert await engine.storage.get_semantic_model_text("shop", DS) == original


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
async def test_formula_restored(tmp_path: Path, dialect: str) -> None:
    """Formula restored."""
    doc = await export_standalone(tmp_path / "src", customers(), orders(metrics=BASIC_METRICS), dialect=dialect)
    config = seeded_config(tmp_path / "db", statements=EXPORT_DDL, dialect=dialect, data_source=EXPORT_DS)
    async with engine_over(tmp_path / "dst", config) as engine:
        await import_text(engine.storage, yaml.safe_dump(doc, sort_keys=False), base=tmp_path / "in",
                          data_source=EXPORT_DS)
        restored = await engine.storage.get_dataset("orders", data_source=EXPORT_DS)
        [row] = (await engine.execute({"population": "orders", "metrics": ["orders.aov"]})).data
    assert restored is not None
    [aov] = [m for m in restored.metrics if m.name == "aov"]
    assert portable_text(aov.expression) == "revenue / n"
    assert float(row["orders.aov"]) == pytest.approx(46.0 / 5)
