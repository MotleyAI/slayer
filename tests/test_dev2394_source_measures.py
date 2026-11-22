"""A dataset-scoped metric evaluating a measure of its source with ``AGG(source.measure)`` loads, and every query
reading it fails: SLayer's sources expose no measure columns (Ossie spec.md, Metric Scoping, rule 9).

Spec: models/semantic-model-documents › Entities SLayer cannot evaluate yet still load;
queries/model-scoped-metrics › AGG evaluates a dataset metric as one component.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from slayer.core import slayer_extension as ext
from slayer.core.models import DatasetMetric
from slayer.osi.exporter import export_osi
from slayer.search.service import SearchService
from tests._dataset_access_fixtures import mcp_text
from tests._dev2243_fixtures import DS, STORAGES, document_engine, error, importer_module, make_storage, metric
from tests._dev2277_fixtures import DS as SEED_DS
from tests._dev2277_fixtures import orders_dataset, seed_datasets, seed_engine
from tests._dev2394_fixtures import (
    MEASURE_REASON, RPC_SM, mcp_reply, source_measures_text, stored_text, with_relationship_named_source,
)

SOURCE_MEASURES = source_measures_text()


def _metric_entry(text: str, *, dataset: str | None, name: str) -> dict:
    doc = yaml.safe_load(text)
    container = doc if dataset is None else next(d for d in doc["datasets"] if d["name"] == dataset)
    return next(m for m in container.get("metrics") or [] if m["name"] == name)


# --------------------------------------------------------------------------- #
# Load and query
# --------------------------------------------------------------------------- #
async def test_a_source_measure_metric_loads_and_fails_only_when_read() -> None:
    """Spec scenario: A source-measure metric loads and fails only when read."""
    async with document_engine(documents=(SOURCE_MEASURES,)) as engine:
        _, errs = await engine.storage.load_datasets(data_source=DS)
        assert errs == []
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.orders.revenue_total",
                                   entity_type="metric")
        assert MEASURE_REASON in inspected, inspected
        resp = await engine.execute({"population": "orders", "metrics": ["orders.order_count"]})
        assert [int(r["orders.order_count"]) for r in resp.data] == [4]
        with pytest.raises(error("UnevaluableEntityError")) as info:
            await engine.execute({"population": "orders", "metrics": ["orders.revenue_total"]})
    assert "revenue_total" in str(info.value)
    assert MEASURE_REASON in str(info.value)


async def test_a_source_measure_warning_shows_on_summary_and_search() -> None:
    async with document_engine(documents=(SOURCE_MEASURES,)) as engine:
        summary = await mcp_text(engine.storage, "datasets_summary", datasource_name=DS, compact=False)
        assert "revenue_total" in summary
        assert MEASURE_REASON in summary
        hits = await SearchService(storage=engine.storage).search(question="revenue_total", compact=False,
                                                                  max_results=20)
        [hit] = [h for h in hits.results if h.id == f"{DS}.orders.revenue_total"]
        assert MEASURE_REASON in f"{hit.text} {hit.description or ''}"


async def test_a_source_measure_inside_a_larger_expression_is_unevaluable() -> None:
    text = SOURCE_MEASURES.replace("AGG(source.revenue)", "AGG(source.revenue) / COUNT(DISTINCT order_id)", 1)
    async with document_engine(documents=(text,)) as engine:
        with pytest.raises(error("UnevaluableEntityError"), match="revenue_total"):
            await engine.execute({"population": "orders", "metrics": ["orders.revenue_total"]})


async def test_a_model_scoped_metric_over_a_source_measure_fails_with_its_chain() -> None:
    """Spec scenario: A model-scoped metric over a source-measure metric fails with its chain."""
    async with document_engine(documents=(SOURCE_MEASURES,)) as engine:
        model = {m.name: m for m in await engine.storage.list_model_scoped_metrics(DS)}
        assert "orders.revenue_total" in (model["revenue_per_customer"].load_warning or "")
        doubled_warning = model["doubled"].load_warning or ""
        assert "revenue_per_customer" in doubled_warning
        assert "orders.revenue_total" in doubled_warning
        with pytest.raises(error("UnevaluableEntityError")) as info:
            await engine.execute({"metrics": ["doubled"]})
        ok = await engine.execute({"metrics": ["rpc_ok"]})
    message = str(info.value)
    assert "doubled" in message
    assert message.index("revenue_per_customer") < message.index("orders.revenue_total")
    assert MEASURE_REASON in message
    assert [float(r["rpc_ok"]) for r in ok.data] == [162.5]


async def test_a_metric_cycle_beside_a_source_measure_loads() -> None:
    cycle = [ext.ModelMetricContent(name="loop_a", expression="loop_b * 2"),
             ext.ModelMetricContent(name="loop_b", expression="loop_a + doubled")]
    async with document_engine(documents=(source_measures_text(extra_slayer_metrics=cycle),)) as engine:
        model = {m.name: m for m in await engine.storage.list_model_scoped_metrics(DS)}
        orders = await engine.storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    assert MEASURE_REASON in (next(m for m in orders.metrics if m.name == "revenue_total").load_warning or "")
    for name in ("doubled", "loop_a", "loop_b"):
        assert "orders.revenue_total" in (model[name].load_warning or ""), name
    assert "doubled" in (model["loop_b"].load_warning or "")


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_source_measure_document_round_trips(kind: str) -> None:
    """Spec scenario: The source-measure document loads and round-trips (store, save unchanged, export)."""
    async with document_engine(kind=kind, documents=(SOURCE_MEASURES,)) as engine:
        assert await stored_text(engine) == SOURCE_MEASURES
        for name in await engine.storage.list_datasets(data_source=DS):
            dataset = await engine.storage.get_dataset(name, data_source=DS)
            assert dataset is not None
            await engine.storage.save_dataset(dataset)
        assert await stored_text(engine) == SOURCE_MEASURES
        exported = await export_osi(engine=engine, datasource=DS)
    assert exported.text == SOURCE_MEASURES


# --------------------------------------------------------------------------- #
# Import
# --------------------------------------------------------------------------- #
async def test_import_reports_source_measure_metrics_as_not_evaluable(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    path = tmp_path / "sales.yaml"
    path.write_text(SOURCE_MEASURES, encoding="utf-8")
    result = await importer_module().import_osi(storage=storage, path=path, data_source=DS)
    assert result.refused == []
    assert result.stored == ["sales"]
    assert await storage.get_semantic_model_text("sales", DS) == SOURCE_MEASURES
    reported = {e.entity: e.message for e in result.entries if e.kind == "not_evaluable"}
    assert {"revenue_total", "revenue_per_customer"} <= set(reported), result.entries
    assert MEASURE_REASON in reported["revenue_total"]


# --------------------------------------------------------------------------- #
# Saves SLayer authors
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_authoring_a_source_measure_metric_is_refused(kind: str) -> None:
    """Spec scenario: Authoring a source-measure metric is refused."""
    async with document_engine(kind=kind) as engine:
        before = await stored_text(engine)
        reply = await mcp_reply(engine.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                                metrics=[{"name": "revenue", "expression": "AGG(source.revenue)"}])
        after = await stored_text(engine)
    assert "UnevaluableEntityError" in reply, reply
    assert "revenue" in reply
    assert MEASURE_REASON in reply, reply
    assert after == before


async def test_agg_of_source_in_a_dataset_metric_is_never_a_relationship_path() -> None:
    """Spec scenario: AGG(source.x) in a dataset metric is never a path."""
    async with document_engine(documents=(with_relationship_named_source(),)) as engine:
        before = await stored_text(engine)
        reply = await mcp_reply(engine.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                                metrics=[{"name": "r", "expression": "AGG(source.revenue)"}])
        after = await stored_text(engine)
    assert "UnevaluableEntityError" in reply, reply
    assert "'r'" in reply or '"r"' in reply or "`r`" in reply, reply
    assert MEASURE_REASON in reply, reply
    assert after == before


async def test_a_standalone_source_measure_metric_is_refused() -> None:
    async with seed_engine("duckdb", datasets=seed_datasets()) as engine:
        bad = orders_dataset(metrics=[DatasetMetric(name="revenue", expression="AGG(source.revenue)")])
        with pytest.raises(error("UnevaluableEntityError"), match=MEASURE_REASON):
            await engine.save_dataset(bad)
        stored = await engine.storage.get_dataset("orders", data_source=SEED_DS)
    assert stored is not None
    assert "revenue" not in {m.name for m in stored.metrics}


async def test_a_model_scoped_metric_over_a_source_measure_is_refused_at_save() -> None:
    async with document_engine(documents=(SOURCE_MEASURES,)) as engine:
        before = await stored_text(engine)
        reply = await mcp_reply(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                                metrics=[metric("rev_sm", "AGG(orders.revenue_total)")])
        after = await stored_text(engine)
    assert "UnevaluableEntityError" in reply, reply
    assert "rev_sm" in reply
    assert after == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_unrelated_edit_keeps_a_warned_metric(kind: str) -> None:
    """Spec scenario: An unrelated edit keeps a warned metric."""
    async with document_engine(kind=kind, documents=(SOURCE_MEASURES,)) as engine:
        before = await stored_text(engine)
        reply = await mcp_reply(engine.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                                fields=[{"name": "order_id", "description": "The order key."}])
        after = await stored_text(engine)
        orders = await engine.storage.get_dataset("orders", data_source=DS)
    assert "Error" not in reply, reply
    assert orders is not None
    assert next(f for f in orders.fields if f.name == "order_id").description == "The order key."
    assert _metric_entry(after, dataset="orders", name="revenue_total") == \
        _metric_entry(before, dataset="orders", name="revenue_total")
    assert _metric_entry(after, dataset=None, name="revenue_per_customer")["expression"]["sql"] == RPC_SM


async def test_an_unrelated_document_edit_keeps_the_chain() -> None:
    async with document_engine(documents=(SOURCE_MEASURES,)) as engine:
        before = await stored_text(engine)
        reply = await mcp_reply(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                                description="Orders, customers and source measures.")
        after = await stored_text(engine)
    assert "Error" not in reply, reply
    assert _metric_entry(after, dataset=None, name="revenue_per_customer") == \
        _metric_entry(before, dataset=None, name="revenue_per_customer")
    assert yaml.safe_load(after)["nonportable_extensions"] == yaml.safe_load(before)["nonportable_extensions"]
