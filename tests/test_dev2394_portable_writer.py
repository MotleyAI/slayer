"""Portable SQL SLayer writes is valid ``OSSIE_SQL_2026``: ``INTERVAL`` arithmetic is written as ``DATEADD``.

Spec: models/semantic-model-documents › Portable SQL SLayer writes is valid OSSIE_SQL_2026;
importers/osi-export › Exported portable SQL is valid OSSIE_SQL_2026.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, DatasourceConfig, SlayerDataset
from slayer.osi.exporter import export_osi
from tests._dev2243_fixtures import (
    DS, STORAGES, dataset, document_text, field, importer_module, make_storage, upstream_validate,
)
from tests._dev2394_fixtures import DUE, dated_engine, dates, mcp_reply, stored_text, validity_errors
from tests._engine_helpers import owned_query_engine
from tests._osi_export_fixtures import DATASOURCE as EXPORT_DS, EXPORT_DDL, customers, orders, slayer_metric

ORDERS_DOC = document_text(datasets=[dataset("orders", primary_key=["order_id"], fields=[
    field("order_id"), field("amount", datatype="Float"), field("ordered_at", datatype="Date")])])
DUE_QUERY = {"population": "orders", "dimensions": ["orders.order_id", "orders.due"]}
#: ``amount`` of the orders after 2026-01-11.
RECENT_TOTAL = 250.0


def standalone_orders(*, fields: list[DatasetField] | None = None,
                      metrics: list[DatasetMetric] | None = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[DatasetField(name="order_id", datatype=DataType.STRING, primary_key=True),
                DatasetField(name="amount", datatype=DataType.FLOAT),
                DatasetField(name="ordered_at", datatype=DataType.DATE), *(fields or [])],
        metrics=metrics or [])


def expression_of(model: SlayerDataset | None, name: str) -> Any:
    assert model is not None
    found = model.get_field(name)
    assert found is not None
    return found.expression


def doc_field(text: str, name: str) -> dict[str, Any]:
    [orders] = [d for d in yaml.safe_load(text)["datasets"] if d["name"] == "orders"]
    return next(f for f in orders["fields"] if f["name"] == name)


def doc_metric(text: str, name: str, *, dataset_name: str | None = "orders") -> dict[str, Any]:
    doc = yaml.safe_load(text)
    container = doc if dataset_name is None else next(d for d in doc["datasets"] if d["name"] == dataset_name)
    return next(m for m in container["metrics"] if m["name"] == name)


# --------------------------------------------------------------------------- #
# Saves through the dataset tools
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("written", "stored"), [
    ("ordered_at + INTERVAL '7 days'", "DATEADD(day, 7, ordered_at)"),
    ("ordered_at + INTERVAL 7 DAY", "DATEADD(day, 7, ordered_at)"),
    ("ordered_at + INTERVAL '1 day'", "DATEADD(day, 1, ordered_at)"),
    ("ordered_at - INTERVAL '30 days'", "DATEADD(day, -30, ordered_at)"),
    ("ordered_at + INTERVAL '7 days' + INTERVAL '1 month'", "DATEADD(month, 1, DATEADD(day, 7, ordered_at))"),
], ids=["string", "number-unit", "singular", "subtraction", "chain"])
async def test_a_saved_standalone_interval_field_is_stored_as_dateadd(written: str, stored: str) -> None:
    async with dated_engine() as engine:
        await engine.save_dataset(standalone_orders(fields=[DatasetField(name="due", expression=written,
                                                                         datatype=DataType.DATE)]))
        saved = await engine.storage.get_dataset("orders", data_source=DS)
    assert saved is not None
    assert expression_of(saved, "due") == stored


async def test_a_saved_interval_field_is_stored_as_dateadd() -> None:
    """Spec scenario: A saved INTERVAL field is stored as DATEADD."""
    async with dated_engine() as engine:
        await engine.save_dataset(standalone_orders(fields=[DatasetField(
            name="due", expression="ordered_at + INTERVAL '7 days'", datatype=DataType.DATE)]))
        first = await engine.storage.get_dataset("orders", data_source=DS)
        assert first is not None
        await engine.save_dataset(first)
        second = await engine.storage.get_dataset("orders", data_source=DS)
        resp = await engine.execute(DUE_QUERY)
    assert expression_of(first, "due") == "DATEADD(day, 7, ordered_at)"
    assert second is not None
    assert expression_of(second, "due") == "DATEADD(day, 7, ordered_at)"
    assert dates(resp.data, key="orders.order_id", value="orders.due") == DUE


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_document_field_saved_through_edit_dataset_is_written_as_dateadd(kind: str) -> None:
    async with dated_engine(kind=kind, documents=(ORDERS_DOC,)) as engine:
        reply = await mcp_reply(engine.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                                fields=[{"name": "due", "expression": "ordered_at + INTERVAL '7 days'",
                                         "datatype": "DATE"}])
        first = await stored_text(engine)
        orders = await engine.storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        await engine.save_dataset(orders)
        second = await stored_text(engine)
        resp = await engine.execute(DUE_QUERY)
    assert "Error" not in reply, reply
    assert doc_field(first, "due")["expression"] == {"sql": "DATEADD(day, 7, ordered_at)"}
    assert second == first
    assert validity_errors(yaml.safe_load(first)) == []
    assert dates(resp.data, key="orders.order_id", value="orders.due") == DUE


async def test_a_dataset_metric_with_interval_is_written_as_dateadd() -> None:
    async with dated_engine(documents=(ORDERS_DOC,)) as engine:
        reply = await mcp_reply(engine.storage, "edit_dataset", dataset_name="orders", data_source=DS, metrics=[{
            "name": "recent",
            "expression": "SUM(amount) FILTER (WHERE ordered_at > DATE '2026-01-01' + INTERVAL '10 days')"}])
        text = await stored_text(engine)
        resp = await engine.execute({"population": "orders", "metrics": ["orders.recent"]})
    assert "Error" not in reply, reply
    sql = doc_metric(text, "recent")["expression"]["sql"]
    assert "DATEADD(day, 10, DATE '2026-01-01')" in sql
    assert "INTERVAL" not in sql.upper()
    assert validity_errors(yaml.safe_load(text)) == []
    assert [float(r["orders.recent"]) for r in resp.data] == [RECENT_TOTAL]


async def test_a_model_scoped_metric_filter_with_interval_is_stored_as_dateadd() -> None:
    """Spec scenario: A model-scoped metric filter with INTERVAL is stored as DATEADD."""
    async with dated_engine(documents=(ORDERS_DOC,)) as engine:
        reply = await mcp_reply(engine.storage, "edit_semantic_model", name="sales", data_source=DS, metrics=[{
            "name": "recent",
            "expression": "SUM(orders.amount) FILTER (WHERE orders.ordered_at > CURRENT_DATE - INTERVAL '30 days')"}])
        text = await stored_text(engine)
    assert "Error" not in reply, reply
    sql = doc_metric(text, "recent", dataset_name=None)["expression"]["sql"]
    assert "DATEADD(day, -30, CURRENT_DATE)" in sql
    assert "INTERVAL" not in sql.upper()
    assert validity_errors(yaml.safe_load(text)) == []


async def test_a_model_scoped_metric_with_interval_keeps_its_values() -> None:
    async with dated_engine(documents=(ORDERS_DOC,)) as engine:
        await mcp_reply(engine.storage, "edit_semantic_model", name="sales", data_source=DS, metrics=[{
            "name": "recent",
            "expression": "SUM(orders.amount) FILTER (WHERE orders.ordered_at > DATE '2026-01-01' + INTERVAL '10 days')"}])
        resp = await engine.execute({"metrics": ["recent"]})
    assert [float(r["recent"]) for r in resp.data] == [RECENT_TOTAL]


async def test_queries_keep_accepting_interval() -> None:
    async with dated_engine(documents=(ORDERS_DOC,)) as engine:
        resp = await engine.execute({"population": "orders", "metrics": [{"metric": "sum(amount)", "alias": "v"}],
                                     "filter": "orders.ordered_at > DATE '2026-01-01' + INTERVAL '10 days'"})
    assert [float(r["v"]) for r in resp.data] == [RECENT_TOTAL]


# --------------------------------------------------------------------------- #
# The import upgrade
# --------------------------------------------------------------------------- #
async def test_an_upgraded_legacy_document_is_written_as_dateadd(tmp_path: Path) -> None:
    legacy = document_text(datasets=[dataset("orders", primary_key=["order_id"], fields=[
        field("order_id"), field("ordered_at", datatype="Date"),
        field("due", "ordered_at + INTERVAL '7 days'", datatype="Date")])]).replace(
        "version: 0.3.0.dev0", "version: '0.2.0.dev0'", 1)
    path = tmp_path / "in" / "sales.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(legacy, encoding="utf-8")
    async with dated_engine(base=tmp_path) as engine:
        result = await importer_module().import_osi(storage=engine.storage, path=path, data_source=DS)
        text = await stored_text(engine)
        resp = await engine.execute(DUE_QUERY)
    assert result.stored == ["sales"], result.refused
    assert doc_field(text, "due")["expression"] == {"sql": "DATEADD(day, 7, ordered_at)"}
    assert validity_errors(yaml.safe_load(text)) == []
    assert dates(resp.data, key="orders.order_id", value="orders.due") == DUE


# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #
async def test_an_interval_field_exports_as_dateadd(tmp_path: Path) -> None:
    """Spec scenario: An INTERVAL field exports as DATEADD."""
    legacy = standalone_orders(fields=[DatasetField(name="due", expression="ordered_at + INTERVAL '7 days'",
                                                    datatype=DataType.DATE)])
    async with dated_engine(base=tmp_path / "a") as engine:
        await getattr(engine.storage, "_save_dataset_impl")(legacy)
        exported = await export_osi(engine=engine, datasource=DS)
    assert doc_field(exported.text, "due")["expression"] == {"sql": "DATEADD(day, 7, ordered_at)"}
    assert validity_errors(exported.document) == []
    path = tmp_path / "exported.yaml"
    path.write_text(exported.text, encoding="utf-8")
    async with dated_engine(base=tmp_path / "b") as engine:
        result = await importer_module().import_osi(storage=engine.storage, path=path, data_source=DS)
        resp = await engine.execute(DUE_QUERY)
    assert result.refused == []
    assert dates(resp.data, key="orders.order_id", value="orders.due") == DUE


async def test_the_exported_interval_document_passes_upstreams_validator(tmp_path: Path) -> None:
    legacy = standalone_orders(fields=[DatasetField(name="due", expression="ordered_at - INTERVAL '30 days'",
                                                    datatype=DataType.DATE)],
                               metrics=[DatasetMetric(name="recent", expression=(
                                   "SUM(amount) FILTER (WHERE ordered_at > DATE '2026-01-01' + INTERVAL '10 days')"))])
    async with dated_engine(base=tmp_path / "a") as engine:
        await getattr(engine.storage, "_save_dataset_impl")(legacy)
        exported = await export_osi(engine=engine, datasource=DS)
    assert doc_field(exported.text, "due")["expression"] == {"sql": "DATEADD(day, -30, ordered_at)"}
    assert "INTERVAL" not in doc_metric(exported.text, "recent")["expression"]["sql"].upper()
    path = tmp_path / "exported.yaml"
    path.write_text(exported.text, encoding="utf-8")
    run = upstream_validate([path], tmp=tmp_path)
    assert "Validation PASSED" in run.stdout, run.stdout + run.stderr


async def test_a_stored_document_still_exports_its_original_text(tmp_path: Path) -> None:
    async with dated_engine(base=tmp_path, documents=(ORDERS_DOC,)) as engine:
        exported = await export_osi(engine=engine, datasource=DS)
    assert exported.text == ORDERS_DOC


async def test_a_cross_dataset_metric_exports_its_interval_as_dateadd(tmp_path: Path) -> None:
    """A standalone metric reading another dataset exports as a model-scoped metric, with portable ``DATEADD``."""
    ratio = slayer_metric("recent_per_customer", "sum(amount) filter (where ordered_on > DATE '2026-01-01' "
                                                 "+ INTERVAL '10 days') / count_distinct(customers.id)")
    query = {"metrics": [{"metric": "orders.recent_per_customer", "alias": "v"}]}
    config = DatasourceConfig(name=EXPORT_DS, type="duckdb", database=str(tmp_path / "export.duckdb"))
    with contextlib.closing(duckdb.connect(config.database)) as con:
        for statement in EXPORT_DDL:
            con.execute(statement)
    async with owned_query_engine(storage=make_storage("yaml", tmp_path / "a"), datasources=[config]) as engine:
        for model in (customers(), orders(metrics=[ratio])):
            await getattr(engine.storage, "_save_dataset_impl")(model)
        before = await engine.execute({"population": "orders", **query})
        exported = await export_osi(engine=engine, datasource=EXPORT_DS)
    sql = doc_metric(exported.text, "recent_per_customer", dataset_name=None)["expression"]["sql"]
    assert "DATEADD(day, 10, " in sql
    assert "INTERVAL" not in sql.upper()
    assert validity_errors(exported.document) == []
    path = tmp_path / "exported.yaml"
    path.write_text(exported.text, encoding="utf-8")
    async with owned_query_engine(storage=make_storage("yaml", tmp_path / "b"), datasources=[config]) as engine:
        result = await importer_module().import_osi(storage=engine.storage, path=path, data_source=EXPORT_DS)
        after = await engine.execute({"metrics": [{"metric": "recent_per_customer", "alias": "v"}]})
    assert result.refused == []
    assert [float(r["v"]) for r in after.data] == [float(r["v"]) for r in before.data]
