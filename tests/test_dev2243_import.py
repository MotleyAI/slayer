"""Spec: importers/osi-import (native Ossie semantic-model documents)."""

from __future__ import annotations

import contextlib
import copy
import json
import shutil
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

from slayer.core.document_text import without_vendor
from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import SlayerError
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.osi.exporter import export_osi
from slayer.storage.base import StorageBackend
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2243_fixtures import (
    CORPUS, DS, SALES, STORAGES, dataset, document_text, error, field, importer_module, make_storage, metric,
    relationship, upstream_validate,
)
from tests._dev2277_fixtures import SEED_TABLES
from tests._engine_helpers import owned_query_engine
from tests._osi_export_fixtures import BASIC_METRICS, DATASOURCE as EXPORT_DS, EXPORT_DDL, customers, orders, \
    orders_fields

EXAMPLES = CORPUS / "examples"
SHOP_EXAMPLE = Path(__file__).parent.parent / "docs" / "examples" / "13_osi_import" / "shop.osi.yaml"
CORE_EXAMPLES = ["flights.semantic_model.yaml", "renamed_fields.yaml", "sap_business_partner.semantic_model.yaml",
                 "tpcds_semantic_model.yaml"]
ONTOLOGY_EXAMPLES = ["flights.yaml", "flights.ontology.yaml", "flights.mapping.yaml",
                     "sap_business_partner.ontology.yaml", "sap_business_partner.mapping.yaml", "foaf_owl_import.yaml"]

RENAMED_DDL = [
    "CREATE SCHEMA public",
    "CREATE TABLE public.orders (o_orderkey INTEGER, o_custkey INTEGER, o_orderdate DATE, "
    "o_totalprice DECIMAL(12,2), total_cents INTEGER)",
    "CREATE TABLE public.customers (c_custkey INTEGER, c_name VARCHAR, c_mktsegment VARCHAR)",
    "INSERT INTO public.orders VALUES (1, 10, '2026-01-05', 10.50, 1050), (2, 10, '2026-01-06', 4.25, 425), "
    "(3, 11, '2026-02-01', 7.00, 700)",
    "INSERT INTO public.customers VALUES (10, 'Ann', 'b2b'), (11, 'Bo', 'b2c')",
]
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

SUPPORT = document_text(name="support", datasets=[
    dataset("returns", primary_key=["return_id"], fields=[field("return_id"), field("customer_id"),
                                                          field("amount", datatype="Float")]),
])


def shop_document(*, version: str = "0.3.0.dev0", **extra: Any) -> str:
    """The spec's flat ``shop`` document over the Appendix B seed."""
    text = document_text(
        name="shop",
        datasets=[
            dataset("orders", primary_key=["order_id"],
                    fields=[field("order_id"), field("customer_id"), field("amount", datatype="Float")]),
            dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("region")]),
        ],
        relationships=[relationship("orders_to_customers", src="orders", to="customers",
                                    from_columns=["customer_id"], to_columns=["customer_id"])],
        metrics=[metric("revenue", "SUM(orders.amount)")],
        **extra,
    )
    return text.replace("version: 0.3.0.dev0", f"version: '{version}'", 1)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def write_yaml(path: Path, mapping: Any) -> Path:
    return write(path, yaml.safe_dump(mapping, sort_keys=False))


async def run_import(storage: StorageBackend, path: Path, *, data_source: str = DS,
                     engine: SlayerQueryEngine | None = None) -> Any:
    return await importer_module().import_osi(storage=storage, path=path, data_source=data_source, engine=engine)


def kinds(result: Any, kind: str) -> list[Any]:
    return [e for e in result.entries if e.kind == kind]


def mentions(entry: Any, name: str) -> bool:
    return name in f"{entry.entity or ''} {entry.message}"


def refusal_text(result: Any, file_name: str) -> str:
    hits = [r for r in result.refused if Path(r.file).name == file_name]
    assert len(hits) == 1, result.refused
    return "\n".join(hits[0].reasons)


async def stored_doc(storage: StorageBackend, name: str, *, data_source: str = DS) -> dict[str, Any]:
    text = await storage.get_semantic_model_text(name, data_source)
    assert text is not None, f"document {name!r} not stored"
    return yaml.safe_load(text)


def ds_of(doc: dict[str, Any], name: str) -> dict[str, Any]:
    [hit] = [d for d in doc["datasets"] if d["name"] == name]
    return hit


def field_of(container: dict[str, Any], name: str) -> dict[str, Any]:
    [hit] = [f for f in container.get("fields") or [] if f["name"] == name]
    return hit


def reads(entity: dict[str, Any]) -> str:
    """The column text an Ossie expression object reads (portable ``sql`` or its ANSI dialect entry)."""
    expression = entity["expression"]
    if expression.get("sql"):
        return expression["sql"]
    return next(d["expression"] for d in expression["dialects"] if d["dialect"] == "ANSI_SQL")


def vendors(entity: dict[str, Any]) -> list[tuple[str, Any]]:
    return [(e["vendor_name"], e["data"]) for e in entity.get("nonportable_extensions") or []]


def _no_database(_self: SlayerQueryEngine, _datasource: DatasourceConfig) -> Any:
    raise AssertionError("database accessed")


async def offline_storage(kind: str, base: Path, monkeypatch: pytest.MonkeyPatch, *,
                          data_source: str = DS) -> StorageBackend:
    """A storage whose datasource points at a missing database; any client construction fails."""
    storage = make_storage(kind, base)
    await storage.save_datasource(DatasourceConfig(name=data_source, type="duckdb",
                                                   database=str(base / "absent" / "missing.duckdb")))
    monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)
    return storage


def seed_sql(path: Path, statements: list[str]) -> None:
    with contextlib.closing(duckdb.connect(str(path))) as con:
        for statement in statements:
            con.execute(statement)


@contextlib.asynccontextmanager
async def seeded(base: Path, *, tables: list[TableSpec] | None = None, statements: list[str] | None = None,
                 db_name: str = "seed.duckdb", data_source: str = DS, kind: str = "yaml",
                 ) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over a seeded DuckDB (Appendix B unless ``statements`` are given)."""
    base.mkdir(parents=True, exist_ok=True)
    db = base / db_name
    if statements is not None:
        seed_sql(db, statements)
    else:
        seed_backend("duckdb", str(db), tables or SEED_TABLES)
    config = DatasourceConfig(name=data_source, type="duckdb", database=str(db))
    async with owned_query_engine(storage=make_storage(kind, base), datasources=[config]) as engine:
        yield engine


async def scalar(engine: SlayerQueryEngine, payload: dict[str, Any], key: str) -> float:
    rows = (await engine.execute(payload)).data
    assert len(rows) == 1, rows
    return float(rows[0][key])


# --------------------------------------------------------------------------- #
# Import stores the document natively, verbatim
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_import_keeps_comments_and_needs_no_database(tmp_path: Path, kind: str,
                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    """Import keeps comments and needs no database."""
    storage = await offline_storage(kind, tmp_path, monkeypatch)
    assert "# owned by finance" in SALES
    result = await run_import(storage, write(tmp_path / "sales.yaml", SALES))
    assert result.stored == ["sales"]
    assert result.refused == []
    assert await storage.get_semantic_model_text("sales", DS) == SALES


async def test_import_over_an_existing_document_replaces_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    await run_import(storage, write(tmp_path / "a" / "sales.yaml", SALES))
    changed = SALES.replace("description: Orders and customers.", "description: Orders and buyers.")
    await run_import(storage, write(tmp_path / "b" / "sales.yaml", changed))
    assert await storage.get_semantic_model_text("sales", DS) == changed
    assert await storage.list_semantic_models(DS) == [(DS, "sales")]


async def test_a_windowed_metric_is_reported_not_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A windowed metric is reported, not refused."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    path = EXAMPLES / "tpcds_semantic_model.yaml"
    result = await run_import(storage, path)
    assert result.stored == ["tpcds_retail_model"]
    assert await storage.get_semantic_model_text("tpcds_retail_model", DS) == path.read_text(encoding="utf-8")
    assert {e.entity for e in kinds(result, "not_evaluable")} >= {"cumulative_sales"}
    assert all("window" in e.message.lower() for e in kinds(result, "not_evaluable"))


# --------------------------------------------------------------------------- #
# Current-spec documents import
# --------------------------------------------------------------------------- #
async def _assert_shop_exposed(engine: SlayerQueryEngine) -> None:
    storage = engine.storage
    assert {"orders", "customers"} <= set(await storage.list_datasets(DS))
    orders_ds = await storage.get_dataset("orders", data_source=DS)
    assert orders_ds is not None
    [rel] = orders_ds.relationships
    assert rel.to == "customers"
    assert rel.cardinality == RelationshipCardinality.MANY_TO_ONE
    assert await scalar(engine, {"metrics": ["revenue"]}, "revenue") == pytest.approx(650.0)


async def test_flat_document_imports_end_to_end(tmp_path: Path) -> None:
    """Flat document imports end to end."""
    text = shop_document()
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write(tmp_path / "shop.yaml", text))
        assert result.stored == ["shop"]
        assert await engine.storage.get_semantic_model_text("shop", DS) == text
        await _assert_shop_exposed(engine)


async def test_earlier_flat_document_keeps_importing(tmp_path: Path) -> None:
    """Earlier flat document keeps importing."""
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write(tmp_path / "shop.yaml", shop_document(version="0.2.0.dev0")))
        assert result.stored == ["shop"]
        assert (await stored_doc(engine.storage, "shop"))["version"] == "0.3.0.dev0"
        assert any(e.document == "shop" and "0.2.0.dev0" in e.message for e in kinds(result, "upgraded"))
        await _assert_shop_exposed(engine)


@pytest.mark.parametrize("version", ["1.0", "0.1.0", "0.1.1", "0.2.0.dev0"])
async def test_every_earlier_version_is_upgraded(tmp_path: Path, version: str, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    result = await run_import(storage, write(tmp_path / "shop.yaml", shop_document(version=version)))
    assert result.stored == ["shop"]
    assert (await stored_doc(storage, "shop"))["version"] == "0.3.0.dev0"
    assert kinds(result, "upgraded")


async def test_upgrade_removes_root_dialects_and_vendors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    text = shop_document(version="0.1.1", dialects=["ANSI_SQL"], vendors=["ACME"])
    result = await run_import(storage, write(tmp_path / "shop.yaml", text))
    doc = await stored_doc(storage, "shop")
    assert "dialects" not in doc
    assert "vendors" not in doc
    assert len(kinds(result, "upgraded")) >= 1


async def test_unknown_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown version."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    result = await run_import(storage, write(tmp_path / "shop.yaml", shop_document(version="0.4.0")))
    assert result.stored == []
    assert "0.4.0" in refusal_text(result, "shop.yaml")
    assert await storage.list_semantic_models(DS) == []
    assert "orders" not in await storage.list_datasets(DS)


async def test_document_level_extensions_kept(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Document-level extensions kept."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    entry = {"vendor_name": "DBT", "data": {"project_name": "tpcds_analytics"}}
    text = shop_document(nonportable_extensions=[entry])
    await run_import(storage, write(tmp_path / "shop.yaml", text))
    assert (await stored_doc(storage, "shop"))["nonportable_extensions"] == [entry]
    assert await storage.get_semantic_model_text("shop", DS) == text


# --------------------------------------------------------------------------- #
# Legacy wrapper documents keep importing (introspection: reachable DuckDB)
# --------------------------------------------------------------------------- #
def legacy_field(name: str) -> dict[str, Any]:
    return {"name": name, "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": name}]}}


def legacy_model(name: str, table: str, key: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "datasets": [{"name": table, "source": table, "fields": [legacy_field(key)], **extra}]}


async def test_legacy_array_with_two_models(tmp_path: Path) -> None:
    """Legacy array with two models."""
    acme = {"vendor_name": "ACME", "data": {"k": 1}}
    wrapper = {"version": "0.2.0.dev0", "semantic_model": [
        legacy_model("a", "orders", "order_id", nonportable_extensions=[acme]),
        legacy_model("b", "customers", "customer_id")]}
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write_yaml(tmp_path / "legacy.yaml", wrapper), engine=engine)
        assert sorted(result.stored) == ["a", "b"]
        assert sorted(await engine.storage.list_semantic_models(DS)) == [(DS, "a"), (DS, "b")]
        for name in ("a", "b"):
            assert (await stored_doc(engine.storage, name))["version"] == "0.3.0.dev0"
        assert ("ACME", {"k": 1}) in vendors(ds_of(await stored_doc(engine.storage, "a"), "orders"))
        assert kinds(result, "split")


async def test_legacy_object_valued_wrapper(tmp_path: Path) -> None:
    """Legacy object-valued wrapper."""
    wrapper = {"version": "0.2.0.dev0", "semantic_model": legacy_model("a", "orders", "order_id")}
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write_yaml(tmp_path / "legacy.yaml", wrapper), engine=engine)
        assert result.stored == ["a"]
        assert await engine.storage.semantic_model_owner("orders", DS) == "a"


async def test_wrapper_without_a_version(tmp_path: Path) -> None:
    """Wrapper without a version."""
    wrapper = {"semantic_model": [legacy_model("a", "orders", "order_id"), legacy_model("b", "customers", "customer_id")]}
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write_yaml(tmp_path / "legacy.yaml", wrapper), engine=engine)
        assert sorted(result.stored) == ["a", "b"]
        for name in ("a", "b"):
            assert (await stored_doc(engine.storage, name))["version"] == "0.3.0.dev0"


async def test_introspected_columns_become_declared_fields(tmp_path: Path) -> None:
    """Introspected columns become declared fields."""
    wrapper = {"version": "0.2.0.dev0", "semantic_model": [legacy_model("a", "orders", "order_id")]}
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write_yaml(tmp_path / "legacy.yaml", wrapper), engine=engine)
        orders_doc = ds_of(await stored_doc(engine.storage, "a"), "orders")
        assert {f["name"] for f in orders_doc["fields"]} == {
            "order_id", "customer_id", "seller_id", "store_id", "manager_id", "amount", "status"}
        amount = field_of(orders_doc, "amount")
        assert reads(amount) == "amount"
        assert amount.get("datatype")
        assert any(mentions(e, "amount") for e in kinds(result, "field_added"))


async def test_split_over_an_existing_document_names_it(tmp_path: Path) -> None:
    async with seeded(tmp_path) as engine:
        flat_a = document_text(name="a", datasets=[dataset("orders", fields=[field("order_id")])])
        await run_import(engine.storage, write(tmp_path / "a.yaml", flat_a))
        wrapper = {"version": "0.2.0.dev0", "semantic_model": [legacy_model("a", "orders", "order_id")]}
        result = await run_import(engine.storage, write_yaml(tmp_path / "legacy.yaml", wrapper), engine=engine)
        assert any(e.document == "a" for e in kinds(result, "replaced"))


def legacy_metric(name: str, sql: str) -> dict[str, Any]:
    return {"name": name, "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": sql}]}}


def squeezed(sql: str) -> str:
    return "".join(sql.split()).upper()


def model_metric(doc: dict[str, Any], name: str) -> dict[str, Any]:
    [hit] = [m for m in doc.get("metrics") or [] if m["name"] == name]
    return hit


def rewritten(result: Any, *, name: str, qualified: str) -> bool:
    """The report lists the rewrite of metric ``name`` to its ``qualified`` reference."""
    return any(mentions(e, name) and qualified in e.message for e in result.entries if e.kind != "not_evaluable")


@pytest.mark.parametrize("declared", [["order_id", "amount"], ["order_id"]], ids=["declared-field", "added-column"])
async def test_unqualified_legacy_metrics_are_qualified(tmp_path: Path, declared: list[str]) -> None:
    """Spec scenario: Unqualified legacy metrics are qualified."""
    model = {"name": "a", "datasets": [{"name": "orders", "source": "orders",
                                        "fields": [legacy_field(n) for n in declared]}],
             "metrics": [legacy_metric("total_amount", "SUM(amount)")]}
    wrapper = {"version": "0.2.0.dev0", "semantic_model": [model]}
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write_yaml(tmp_path / "legacy.yaml", wrapper), engine=engine)
        assert result.stored == ["a"]
        stored = model_metric(await stored_doc(engine.storage, "a"), "total_amount")
        assert squeezed(reads(stored)) == "SUM(ORDERS.AMOUNT)"
        assert await scalar(engine, {"metrics": ["total_amount"]}, "total_amount") == pytest.approx(650.0)
        assert rewritten(result, name="total_amount", qualified="orders.amount")
        assert not [e for e in kinds(result, "not_evaluable") if mentions(e, "total_amount")]


@pytest.mark.parametrize(("sql", "offender"), [
    pytest.param("COUNT(DISTINCT customer_id)", "customer_id", id="ambiguous"),
    pytest.param("SUM(nope)", "nope", id="unresolvable"),
])
async def test_an_unqualifiable_legacy_metric_is_stored_as_written_and_reported(tmp_path: Path, sql: str,
                                                                               offender: str) -> None:
    model = {"name": "a",
             "datasets": [{"name": "orders", "source": "orders", "fields": [legacy_field("order_id")]},
                          {"name": "customers", "source": "customers", "fields": [legacy_field("customer_id")]}],
             "metrics": [legacy_metric("odd", sql), legacy_metric("total_amount", "SUM(amount)")]}
    wrapper = {"version": "0.2.0.dev0", "semantic_model": [model]}
    async with seeded(tmp_path) as engine:
        result = await run_import(engine.storage, write_yaml(tmp_path / "legacy.yaml", wrapper), engine=engine)
        assert result.stored == ["a"]
        doc = await stored_doc(engine.storage, "a")
        assert squeezed(reads(model_metric(doc, "odd"))) == squeezed(sql)
        assert [e for e in kinds(result, "not_evaluable") if mentions(e, "odd") and offender in e.message]
        assert not rewritten(result, name="odd", qualified=f"orders.{offender}")
        assert squeezed(reads(model_metric(doc, "total_amount"))) == "SUM(ORDERS.AMOUNT)"
        assert await scalar(engine, {"metrics": ["total_amount"]}, "total_amount") == pytest.approx(650.0)
        with pytest.raises(SlayerError, match="odd"):
            await engine.execute({"metrics": ["odd"]})


LEGACY_SHOP = Path(__file__).parent / "fixtures" / "osi" / "shop.yaml"
#: ``tests/fixtures/osi/shop.yaml``'s tables: three orders of 10, 5 and 7.
LEGACY_SHOP_DDL = [
    "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, product_id INTEGER, amount DOUBLE, quantity INTEGER, "
    "ordered_at DATE, status VARCHAR)",
    "CREATE TABLE customers (customer_id INTEGER, region_id INTEGER, name VARCHAR, segment VARCHAR)",
    "CREATE TABLE products (product_id INTEGER, category VARCHAR, price DOUBLE)",
    "CREATE TABLE regions (region_id INTEGER, name VARCHAR, population INTEGER)",
    "INSERT INTO orders VALUES (1, 1, 1, 10, 1, '2024-01-01', 'completed'), (2, 1, 2, 5, 2, '2024-01-05', 'open'), "
    "(3, 2, 1, 7, 3, '2024-02-01', 'completed')",
    "INSERT INTO customers VALUES (1, 1, 'Ann', 'b2b'), (2, 2, 'Bo', 'b2c')",
    "INSERT INTO products VALUES (1, 'Beverages', 5.0), (2, 'Bakery', 3.0)",
    "INSERT INTO regions VALUES (1, 'North', 1000), (2, 'South', 2000)",
]


@contextlib.asynccontextmanager
async def legacy_shop(base: Path) -> AsyncGenerator[tuple[SlayerQueryEngine, Any]]:
    """``tests/fixtures/osi/shop.yaml`` imported over its tables."""
    async with seeded(base, statements=LEGACY_SHOP_DDL, db_name="shop.duckdb") as engine:
        result = await run_import(engine.storage, LEGACY_SHOP, engine=engine)
        assert result.stored == ["shop"]
        yield engine, result


async def test_the_legacy_shop_fixtures_metric_answers_by_bare_name(tmp_path: Path) -> None:
    async with legacy_shop(tmp_path) as (engine, result):
        assert squeezed(reads(model_metric(await stored_doc(engine.storage, "shop"), "total_amount"))) == \
            "SUM(ORDERS.AMOUNT)"
        assert await scalar(engine, {"metrics": ["total_amount"]}, "total_amount") == pytest.approx(22.0)
        assert rewritten(result, name="total_amount", qualified="orders.amount")


@pytest.mark.parametrize("name", ["order_count", "aov"])
async def test_a_legacy_model_level_count_star_is_not_evaluable(tmp_path: Path, name: str) -> None:
    """Model-scoped ``COUNT(*)`` reads no dataset (Ossie E_AMBIGUOUS_MEASURE_GRAIN): stored as written, reported."""
    async with legacy_shop(tmp_path) as (engine, result):
        assert [e for e in kinds(result, "not_evaluable") if mentions(e, name)]
        with pytest.raises(error("UnevaluableEntityError"), match=name):
            await engine.execute({"metrics": [name]})


# --------------------------------------------------------------------------- #
# Invalid documents are refused with every reason
# --------------------------------------------------------------------------- #
def sales_dict() -> dict[str, Any]:
    return yaml.safe_load(SALES)


async def test_every_reason_is_listed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every reason is listed."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    bad = sales_dict()
    bad["datasets"][0]["fields"].append(field("amount", datatype="Float"))
    bad["relationships"][0]["from_columns"] = ["customer_ref"]
    folder = tmp_path / "in"
    write_yaml(folder / "bad.yaml", bad)
    write(folder / "good.yaml", SUPPORT)
    result = await run_import(storage, folder)
    text = refusal_text(result, "bad.yaml")
    assert "amount" in text
    assert "customer_ref" in text
    assert len([r for r in result.refused if Path(r.file).name == "bad.yaml"][0].reasons) >= 2
    assert result.stored == ["support"]
    assert await storage.get_semantic_model_text("sales", DS) is None


def _unknown_key(doc: dict[str, Any]) -> None:
    doc["datasets"][0]["colour"] = "red"


def _case_duplicate_field(doc: dict[str, Any]) -> None:
    doc["datasets"][0]["fields"].append(field("AMOUNT", datatype="Float"))


def _mismatched_key_lengths(doc: dict[str, Any]) -> None:
    doc["relationships"][0]["from_columns"] = ["customer_id", "status"]


def _outside_dataset(doc: dict[str, Any]) -> None:
    doc["relationships"][0]["to"] = "suppliers"


def _broken_metric_scoping(doc: dict[str, Any]) -> None:
    doc["datasets"][0]["metrics"].append(metric("bad_metric", "COUNT(DISTINCT customers.region)"))


def _empty_expression(doc: dict[str, Any]) -> None:
    doc["datasets"][0]["fields"].append({"name": "empty_expr", "expression": {}})


def _non_object_extension(doc: dict[str, Any]) -> None:
    doc["datasets"][1]["nonportable_extensions"] = [{"vendor_name": "ACME", "data": [1, 2]}]


@pytest.mark.parametrize(("mutate", "names"), [
    pytest.param(_unknown_key, ["colour"], id="unknown-key"),
    pytest.param(_case_duplicate_field, ["amount"], id="duplicate-under-identifier-comparison"),
    pytest.param(_mismatched_key_lengths, ["orders_to_customers"], id="mismatched-key-lengths"),
    pytest.param(_outside_dataset, ["suppliers"], id="relationship-outside-document"),
    pytest.param(_broken_metric_scoping, ["bad_metric"], id="metric-scoping"),
    pytest.param(_empty_expression, ["empty_expr"], id="expression-without-form"),
    pytest.param(_non_object_extension, ["customers", "ACME"], id="extension-data-not-object"),
])
async def test_invalid_document_is_refused_naming_its_entity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                             mutate: Callable[[dict[str, Any]], None],
                                                             names: list[str]) -> None:
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    doc = sales_dict()
    mutate(doc)
    result = await run_import(storage, write_yaml(tmp_path / "bad.yaml", doc))
    text = refusal_text(result, "bad.yaml").lower()
    assert all(name.lower() in text for name in names), text
    assert result.stored == []
    assert await storage.list_semantic_models(DS) == []


async def _refused_beside_a_valid_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad: Any) -> tuple[Any, str]:
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    folder = tmp_path / "in"
    if isinstance(bad, str):
        write(folder / "bad.yaml", bad)
    else:
        write_yaml(folder / "bad.yaml", bad)
    write(folder / "good.yaml", SUPPORT)
    result = await run_import(storage, folder)
    assert result.stored == ["support"]
    assert await storage.get_semantic_model_text("support", DS) == SUPPORT
    return result, refusal_text(result, "bad.yaml")


async def test_mixed_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mixed shape."""
    bad = {"version": "0.2.0.dev0", "semantic_model": [legacy_model("a", "orders", "order_id")],
           "datasets": [dataset("orders", fields=[field("order_id")])]}
    _, text = await _refused_beside_a_valid_file(tmp_path, monkeypatch, bad)
    assert text


async def test_empty_legacy_wrapper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty legacy wrapper."""
    _, text = await _refused_beside_a_valid_file(tmp_path, monkeypatch, {"semantic_model": []})
    assert text


@pytest.mark.parametrize("bad", [{"foo": 1}, {"semantic_model": 5}], ids=["neither-shape", "malformed-wrapper"])
async def test_root_holding_neither_shape_or_a_malformed_wrapper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                 bad: dict[str, Any]) -> None:
    _, text = await _refused_beside_a_valid_file(tmp_path, monkeypatch, bad)
    assert text


async def test_wrapped_0_3_document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Wrapped 0.3 document."""
    bad = {"version": "0.3.0.dev0", "semantic_model": [legacy_model("a", "orders", "order_id")]}
    _, text = await _refused_beside_a_valid_file(tmp_path, monkeypatch, bad)
    assert "0.3.0.dev0" in text
    assert "flat" in text.lower()


async def test_ontology_document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ontology document."""
    text_in = (EXAMPLES / "flights.ontology.yaml").read_text(encoding="utf-8")
    _, text = await _refused_beside_a_valid_file(tmp_path, monkeypatch, text_in)
    assert "semantic-model document" in text.lower()
    assert "not" in text.lower()


# --------------------------------------------------------------------------- #
# Legacy custom_extensions keep importing
# --------------------------------------------------------------------------- #
def to_custom_extensions(node: Any) -> Any:
    """``node`` with every ``nonportable_extensions`` list rewritten as legacy ``custom_extensions`` strings."""
    if isinstance(node, list):
        return [to_custom_extensions(v) for v in node]
    if not isinstance(node, dict):
        return node
    out: dict[str, Any] = {}
    for key, value in node.items():
        if key == "nonportable_extensions":
            out["custom_extensions"] = [{"vendor_name": e["vendor_name"], "data": json.dumps(e["data"])} for e in value]
        else:
            out[key] = to_custom_extensions(value)
    return out


async def test_slayer_1_8_export_restores(tmp_path: Path) -> None:
    """SLayer 1.8 export restores."""
    columns = orders_fields()
    for c in columns:
        if c.name == "amount":
            c.format = NumberFormat(type=NumberFormatType.CURRENCY, precision=2)
        if c.name == "note":
            c.hidden = True
    async with seeded(tmp_path / "src", statements=EXPORT_DDL, data_source=EXPORT_DS) as engine:
        for model in (customers(), orders(fields=columns, metrics=BASIC_METRICS)):
            await engine.save_dataset(model)
        exported = (await export_osi(engine=engine, datasource=EXPORT_DS)).document
    assert '"vendor_name": "SLAYER"' in json.dumps(exported)
    modern = write_yaml(tmp_path / "modern.yaml", exported)
    legacy = write_yaml(tmp_path / "legacy.yaml", to_custom_extensions(copy.deepcopy(exported)))
    assert "custom_extensions" in legacy.read_text(encoding="utf-8")
    db = str(tmp_path / "src" / "seed.duckdb")
    stores: list[StorageBackend] = []
    results: list[Any] = []
    for label, path in (("modern", modern), ("legacy", legacy)):
        storage = make_storage("yaml", tmp_path / label)
        await storage.save_datasource(DatasourceConfig(name=EXPORT_DS, type="duckdb", database=db))
        results.append(await run_import(storage, path, data_source=EXPORT_DS))
        stores.append(storage)
    name = exported["name"]
    legacy_text = await stores[1].get_semantic_model_text(name, EXPORT_DS)
    assert legacy_text is not None
    assert "custom_extensions" not in legacy_text
    assert yaml.safe_load(legacy_text) == yaml.safe_load(modern.read_text(encoding="utf-8"))
    for dataset_name in ("customers", "orders"):
        modern_ds = await stores[0].get_dataset(dataset_name, data_source=EXPORT_DS)
        legacy_ds = await stores[1].get_dataset(dataset_name, data_source=EXPORT_DS)
        assert modern_ds is not None
        assert legacy_ds is not None
        assert legacy_ds.model_dump(mode="json") == modern_ds.model_dump(mode="json")
    [entry] = kinds(results[1], "legacy_extensions")
    assert entry.document == name
    assert kinds(results[0], "legacy_extensions") == []


async def test_legacy_key_at_every_level(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy key at every level."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    legacy = [{"vendor_name": "ACME", "data": '{"k": 1}'}]
    doc = sales_dict()
    doc["custom_extensions"] = legacy
    orders_doc = doc["datasets"][0]
    orders_doc["custom_extensions"] = legacy
    field_of(orders_doc, "amount")["custom_extensions"] = legacy
    orders_doc["metrics"][0]["custom_extensions"] = legacy
    doc["relationships"][0]["custom_extensions"] = legacy
    result = await run_import(storage, write_yaml(tmp_path / "sales.yaml", doc))
    assert result.stored == ["sales"]
    text = await storage.get_semantic_model_text("sales", DS)
    assert text is not None
    assert "custom_extensions" not in text
    stored = yaml.safe_load(text)
    stored_orders = ds_of(stored, "orders")
    for entity in (stored, stored_orders, field_of(stored_orders, "amount"), stored_orders["metrics"][0],
                   stored["relationships"][0]):
        assert vendors(entity) == [("ACME", {"k": 1})]
    [entry] = kinds(result, "legacy_extensions")
    assert entry.document == "sales"


async def test_both_keys_on_one_entity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Both keys on one entity."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    doc = sales_dict()
    amount = field_of(doc["datasets"][0], "amount")
    amount["nonportable_extensions"] = [{"vendor_name": "ACME", "data": {"a": 1}}]
    amount["custom_extensions"] = [{"vendor_name": "OTHER", "data": '{"b": 2}'}]
    await run_import(storage, write_yaml(tmp_path / "sales.yaml", doc))
    stored_amount = field_of(ds_of(await stored_doc(storage, "sales"), "orders"), "amount")
    assert vendors(stored_amount) == [("ACME", {"a": 1}), ("OTHER", {"b": 2})]
    assert "custom_extensions" not in stored_amount


@pytest.mark.parametrize("data", ["[1, 2]", "not json"])
async def test_legacy_string_that_is_not_an_object(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: str) -> None:
    """Legacy string that is not an object."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    doc = sales_dict()
    field_of(doc["datasets"][0], "amount")["custom_extensions"] = [{"vendor_name": "ACME", "data": data}]
    result = await run_import(storage, write_yaml(tmp_path / "sales.yaml", doc))
    text = refusal_text(result, "sales.yaml")
    assert "amount" in text
    assert "ACME" in text
    assert await storage.list_semantic_models(DS) == []


# --------------------------------------------------------------------------- #
# Extension data must be a JSON object
# --------------------------------------------------------------------------- #
EXTENSION_TEMPLATE = """\
version: 0.3.0.dev0
name: sales
datasets:
  - name: orders
    source: orders
    fields:
      - name: order_id
        expression: {{sql: order_id}}
    nonportable_extensions:
      - vendor_name: ACME
        data: {data}
"""


async def _extension_import(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: str) -> tuple[StorageBackend, Any]:
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    result = await run_import(storage, write(tmp_path / "sales.yaml", EXTENSION_TEMPLATE.format(data=data)))
    return storage, result


async def _assert_extension_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: str) -> None:
    storage, result = await _extension_import(tmp_path, monkeypatch, data)
    text = refusal_text(result, "sales.yaml")
    assert "orders" in text
    assert "ACME" in text
    assert await storage.list_semantic_models(DS) == []


async def test_unquoted_date_under_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unquoted date under data."""
    await _assert_extension_refused(tmp_path, monkeypatch, "{released: 2024-01-01}")


@pytest.mark.parametrize("value", [".inf", "-.inf", ".nan"])
async def test_nested_non_finite_number(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Nested non-finite number."""
    await _assert_extension_refused(tmp_path, monkeypatch, f"{{stats: {{ratio: {value}}}}}")


@pytest.mark.parametrize("data", ["{log: {at: 2024-01-01T00:00:00Z}}", "{codes: {1: one}}"],
                         ids=["timestamp", "non-string-key"])
async def test_nested_timestamp_or_non_string_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: str) -> None:
    """Nested timestamp or non-string key."""
    await _assert_extension_refused(tmp_path, monkeypatch, data)


@pytest.mark.parametrize("data", ["[1, 2]", "text"])
async def test_data_that_is_not_an_object(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: str) -> None:
    """Data that is not an object."""
    await _assert_extension_refused(tmp_path, monkeypatch, data)


async def test_nested_json_data_is_accepted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage, result = await _extension_import(
        tmp_path, monkeypatch, "{stats: {ratio: 1.5, tags: [a, b], none: null, ok: true}, label: '2024-01-01'}")
    assert result.stored == ["sales"]
    assert vendors(ds_of(await stored_doc(storage, "sales"), "orders")) == [
        ("ACME", {"stats": {"ratio": 1.5, "tags": ["a", "b"], "none": None, "ok": True}, "label": "2024-01-01"})]


# --------------------------------------------------------------------------- #
# The shipped example is a valid current-spec document
# --------------------------------------------------------------------------- #
def test_example_validates_against_the_pinned_schema(tmp_path: Path) -> None:
    """Example validates and imports (validation half)."""
    done = upstream_validate([SHOP_EXAMPLE], tmp=tmp_path)
    assert done.returncode == 0, done.stdout + done.stderr


async def test_example_validates_and_imports(tmp_path: Path) -> None:
    """Example validates and imports."""
    async with seeded(tmp_path, statements=SHOP_EXAMPLE_DDL, db_name="shop.duckdb") as engine:
        result = await run_import(engine.storage, SHOP_EXAMPLE)
        assert result.stored == ["shop"]
        assert await engine.storage.get_semantic_model_text("shop", DS) == SHOP_EXAMPLE.read_text(encoding="utf-8")
        total = await scalar(engine, {"population": "orders", "metrics": ["orders.total_amount"]},
                             "orders.total_amount")
        assert total == pytest.approx(1400.0)
        rows = (await engine.execute({
            "population": "orders", "metrics": ["orders.total_amount"], "dimensions": ["customers.regions.name"],
            "order": [{"field": "customers.regions.name"}]})).data
        # Result keys follow the canonical path: the hop's edge name.
        assert [(r["customers.customers_to_regions.name"], float(r["orders.total_amount"])) for r in rows] == [
            ("North", 750.0), ("South", 650.0)]
        assert await scalar(engine, {"population": "orders", "metrics": ["orders.aov"]}, "orders.aov") == \
            pytest.approx(1400.0 / 6)
        assert await scalar(engine, {"metrics": ["cust_reach"]}, "cust_reach") == pytest.approx(1400.0 / 3)
        assert await scalar(engine, {"metrics": ["rev_plus_pop"]}, "rev_plus_pop") == pytest.approx(4400.0)


# --------------------------------------------------------------------------- #
# Upstream examples import
# --------------------------------------------------------------------------- #
def test_every_upstream_example_is_vendored() -> None:
    assert sorted(p.name for p in EXAMPLES.iterdir()) == sorted([*CORE_EXAMPLES, *ONTOLOGY_EXAMPLES])


async def _loaded_dataset_names(storage: StorageBackend) -> list[str]:
    loaded, failures = await storage.load_datasets(data_source=DS)
    assert [f for f in failures if getattr(f, "kind", None) == "dataset"] == []
    return sorted(d.name for d in loaded)


@pytest.mark.parametrize("name", CORE_EXAMPLES)
async def test_core_example_imports_natively_and_loads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                       name: str) -> None:
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    path = EXAMPLES / name
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    result = await run_import(storage, path)
    assert result.stored == [document["name"]]
    assert await storage.get_semantic_model_text(document["name"], DS) == path.read_text(encoding="utf-8")
    assert await _loaded_dataset_names(storage) == sorted(d["name"] for d in document["datasets"])


@pytest.mark.parametrize("name", ONTOLOGY_EXAMPLES)
async def test_ontology_examples(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Ontology examples."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    shutil.copy(EXAMPLES / name, tmp_path / name)
    result = await run_import(storage, tmp_path / name)
    assert result.stored == []
    assert "semantic-model document" in refusal_text(result, name).lower()
    assert await storage.list_semantic_models(DS) == []


async def test_renamed_fields_example(tmp_path: Path) -> None:
    """Renamed-fields example."""
    async with seeded(tmp_path, statements=RENAMED_DDL, db_name="sales.duckdb") as engine:
        result = await run_import(engine.storage, EXAMPLES / "renamed_fields.yaml")
        assert result.stored == ["renamed_fields"]
        assert await _loaded_dataset_names(engine.storage) == ["customers", "orders"]
        assert await scalar(engine, {"metrics": ["gross_revenue"]}, "gross_revenue") == pytest.approx(21.75)


async def test_tpcds_example(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """TPC-DS example."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    result = await run_import(storage, EXAMPLES / "tpcds_semantic_model.yaml")
    assert await _loaded_dataset_names(storage) == ["customer", "date_dim", "item", "store", "store_sales"]
    assert sorted(e.entity for e in kinds(result, "not_evaluable")) == [
        "brand_rank_in_store", "cumulative_sales", "monthly_sales_change"]


async def test_flights_semantic_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Flights semantic model."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    result = await run_import(storage, EXAMPLES / "flights.semantic_model.yaml")
    assert len(await _loaded_dataset_names(storage)) == 6
    assert kinds(result, "not_evaluable") == []


# --------------------------------------------------------------------------- #
# Re-import replaces datasets converted in 1.x
# --------------------------------------------------------------------------- #
#: The document the 1.x converter anchored ``revenue_per_customer`` from, without ``orders.status``.
REIMPORT = document_text(
    name="sales", description="Orders and customers.",
    datasets=[
        dataset("orders", primary_key=["order_id"],
                fields=[field("order_id"), field("customer_id"), field("amount", datatype="Float")],
                metrics=[metric("total_revenue", "SUM(amount)")]),
        dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("region")]),
    ],
    relationships=[relationship("orders_to_customers", src="orders", to="customers",
                                from_columns=["customer_id"], to_columns=["customer_id"])],
    metrics=[metric("revenue_per_customer", "SUM(orders.amount) / COUNT(DISTINCT customers.customer_id)",
                    description="Revenue per customer.", ai_context={"synonyms": ["ARPU"]})],
)
_DOC_META = {"osi_semantic_model": {"name": "sales", "description": "Orders and customers.", "ai_context": None}}
STAGE_QUERY = {"population": "orders", "dimensions": ["orders.customer_id"],
               "metrics": [{"metric": "orders.total_revenue", "alias": "revenue"}]}
STAGE_TOTAL = {"population": "revenue_by_customer", "metrics": ["sum(revenue_by_customer.revenue)"]}
MEMORY_QUERY = {"population": "orders", "metrics": ["orders.total_revenue"]}


def converted_1x() -> list[SlayerDataset]:
    """The standalone datasets a 1.x import stored from ``sales``, plus SLayer-only additions made since."""
    orders_1x = SlayerDataset(
        name="orders", sql_table="orders", data_source=DS, meta=_DOC_META,
        fields=[DatasetField(name="order_id", primary_key=True), DatasetField(name="customer_id"),
                DatasetField(name="amount", datatype=DataType.FLOAT,
                             format=NumberFormat(type=NumberFormatType.CURRENCY, precision=2)),
                DatasetField(name="status", allowed_aggregations=["count"])],
        metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)"),
                 DatasetMetric(name="revenue_per_customer",
                               expression="sum(amount) / count_distinct(customers.customer_id)",
                               description="Revenue per customer.\n\nSynonyms: ARPU",
                               meta={"osi_ai_context": {"synonyms": ["ARPU"]}}),
                 DatasetMetric(name="max_amount", expression="max(amount)")],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["customer_id"],
                                           cardinality=RelationshipCardinality.MANY_TO_ONE)],
    )
    customers_1x = SlayerDataset(
        name="customers", sql_table="customers", data_source=DS, meta=_DOC_META,
        fields=[DatasetField(name="customer_id", primary_key=True), DatasetField(name="region")],
    )
    return [customers_1x, orders_1x]


@pytest.fixture
async def legacy_1x(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    """A seeded engine holding the 1.x-converted datasets, a query-backed stage over them and a memory."""
    async with seeded(tmp_path) as engine:
        for model in converted_1x():
            await engine.storage.save_dataset(model)
        await engine.save_dataset(SlayerDataset(name="revenue_by_customer", data_source=DS,
                                                source_queries=[SlayerQuery.model_validate(STAGE_QUERY)]))
        await engine.storage.save_memory(id="m1", learning="Revenue lives on orders", entities=[f"{DS}.orders"],
                                         query=SlayerQuery.model_validate(MEMORY_QUERY))
        [row] = (await engine.execute(STAGE_TOTAL)).data
        assert [float(v) for v in row.values()] == [650.0]
        memory = await engine.storage.get_memory("m1")
        assert memory.query is not None
        [row] = (await engine.execute(memory.query)).data
        assert float(row["orders.total_revenue"]) == pytest.approx(650.0)
        yield engine


async def _reimport(engine: SlayerQueryEngine, base: Path) -> Any:
    return await run_import(engine.storage, write(base / "reimport" / "sales.yaml", REIMPORT))


async def test_reimport_replaces_the_converted_datasets(legacy_1x: SlayerQueryEngine, tmp_path: Path) -> None:
    storage = legacy_1x.storage
    assert await storage.semantic_model_owner("orders", DS) == DS
    result = await _reimport(legacy_1x, tmp_path)
    assert result.stored == ["sales"]
    text = await storage.get_semantic_model_text("sales", DS)
    assert text is not None
    # The file as written, plus the SLAYER entries carrying the replaced datasets' SLayer-only content.
    assert without_vendor(yaml.safe_load(text), "SLAYER") == yaml.safe_load(REIMPORT)
    kept = text.splitlines()
    assert all(line in kept for line in REIMPORT.splitlines())
    for name in ("orders", "customers"):
        assert await storage.semantic_model_owner(name, DS) == "sales"
    names = await storage.list_datasets(DS)
    assert names.count("orders") == 1
    assert names.count("customers") == 1


async def test_an_anchored_metric_moves_into_the_document_scope(legacy_1x: SlayerQueryEngine, tmp_path: Path) -> None:
    """An anchored metric moves into the document scope."""
    result = await _reimport(legacy_1x, tmp_path)
    assert await scalar(legacy_1x, {"metrics": ["revenue_per_customer"]}, "revenue_per_customer") == \
        pytest.approx(162.5)
    orders_ds = await legacy_1x.storage.get_dataset("orders", data_source=DS)
    assert orders_ds is not None
    assert "revenue_per_customer" not in {m.name for m in orders_ds.metrics}
    with pytest.raises(SlayerError):
        await legacy_1x.execute({"population": "orders", "metrics": ["orders.revenue_per_customer"]})
    assert any(mentions(e, "revenue_per_customer") for e in kinds(result, "moved_metric"))


async def test_reimport_carries_slayer_only_content_into_slayer_entries(legacy_1x: SlayerQueryEngine,
                                                                        tmp_path: Path) -> None:
    await _reimport(legacy_1x, tmp_path)
    text = await legacy_1x.storage.get_semantic_model_text("sales", DS)
    assert text is not None
    assert "SLAYER" in text
    orders_ds = await legacy_1x.storage.get_dataset("orders", data_source=DS)
    assert orders_ds is not None
    amount = next(f for f in orders_ds.fields if f.name == "amount")
    assert amount.format == NumberFormat(type=NumberFormatType.CURRENCY, precision=2)
    assert "max_amount" in {m.name for m in orders_ds.metrics}
    assert await scalar(legacy_1x, {"population": "orders", "metrics": ["orders.max_amount"]},
                        "orders.max_amount") == pytest.approx(300.0)


async def test_reimport_lists_content_naming_undeclared_entities(legacy_1x: SlayerQueryEngine, tmp_path: Path) -> None:
    result = await _reimport(legacy_1x, tmp_path)
    assert any(mentions(e, "status") for e in kinds(result, "unexpressed"))


async def test_memory_and_stage_references_survive_reimport(legacy_1x: SlayerQueryEngine, tmp_path: Path) -> None:
    before = (await legacy_1x.execute(STAGE_TOTAL)).data
    await _reimport(legacy_1x, tmp_path)
    memory = await legacy_1x.storage.get_memory("m1")
    assert memory.entities == [f"{DS}.orders"]
    assert memory.query is not None
    [row] = (await legacy_1x.execute(memory.query)).data
    assert float(row["orders.total_revenue"]) == pytest.approx(650.0)
    stage = await legacy_1x.storage.get_dataset("revenue_by_customer", data_source=DS)
    assert stage is not None
    assert (await legacy_1x.execute(STAGE_TOTAL)).data == before
