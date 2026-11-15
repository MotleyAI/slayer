"""Ossie import: SLAYER extensions restore SLayer structure when the document is unedited, per entity."""

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.models import ModelJoin, SlayerModel
from tests._osi_export_fixtures import (
    BASIC_MEASURES, customers, measure, monthly_revenue, orders, rich_orders, to_customers,
)
from tests._osi_export_helpers import (
    SLAYER, Env, convert_document, dataset, export_models, field, make_env, mentions, metric, normalized, reimport,
    rows, slayer_entries, stored,
)
from tests._osi_helpers import by_name, ds, f, flat


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _join(model: SlayerModel, target: str) -> ModelJoin:
    [join] = [j for j in model.joins if j.target_model == target]
    return join


def _without(document: dict[str, Any], name: str) -> dict[str, Any]:
    """``document`` with dataset ``name`` and everything referring to it removed."""
    doc = copy.deepcopy(document)
    doc["datasets"] = [d for d in doc["datasets"] if d["name"] != name]
    doc["relationships"] = [r for r in doc.get("relationships") or [] if name not in (r["from"], r["to"])]
    doc["metrics"] = [m for m in doc.get("metrics") or [] if f"{name}." not in json.dumps(m["expression"])]
    return doc


# ──────────────────────────────── measures ─────────────────────────────────

async def test_unedited_formula_restored(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    aov = by_name(convert_document(env, result.document))["orders"].get_measure("aov")
    assert aov is not None
    assert aov.formula == "revenue / n"


async def test_edited_metric_falls_back_to_its_sql(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    aov_metric = metric(dataset(result.document, "orders"), "aov")
    assert aov_metric is not None
    aov_metric["expression"] = {"sql": "SUM(amount) / NULLIF(COUNT(*), 0)"}
    imported = convert_document(env, result.document)
    aov = by_name(imported)["orders"].get_measure("aov")
    assert aov is not None
    assert aov.formula != "revenue / n"
    assert mentions(imported, "aov", severity="info")


async def test_unedited_column_split_restored(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    completed = by_name(convert_document(env, result.document))["orders"].get_column("completed_amount")
    assert completed is not None
    assert (completed.sql, completed.filter) == ("amount", "status = 'completed'")


async def test_edited_field_falls_back_to_its_expression(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    completed_field = field(dataset(result.document, "orders"), "completed_amount")
    assert completed_field is not None
    completed_field["expression"] = {"sql": "CASE WHEN status = 'open' THEN amount END"}
    imported = convert_document(env, result.document)
    completed = by_name(imported)["orders"].get_column("completed_amount")
    assert completed is not None
    assert completed.filter is None
    assert completed.sql is not None
    assert "'open'" in completed.sql
    assert mentions(imported, "completed_amount", severity="info")


async def test_edited_field_drops_its_type_dependent_attributes(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    ordered_on = field(dataset(result.document, "orders"), "ordered_on")
    assert ordered_on is not None
    ordered_on.update(expression={"sql": "status"}, datatype="String")
    restored = by_name(convert_document(env, result.document))["orders"]
    column = restored.get_column("ordered_on")
    assert column is not None
    assert column.granularity is None
    SlayerModel.model_validate(restored.model_dump())


async def test_edited_metric_attributes_stay_on_its_model(env) -> None:
    result = await export_models(env, customers(measures=[measure("n", "count(*)", label="Customers")]),
                                 orders(measures=[measure("n", "count(*)", label="Orders")]))
    for name in ("customers", "orders"):
        n_metric = metric(dataset(result.document, name), "n")
        assert n_metric is not None
        n_metric["expression"] = {"sql": "COUNT(1)"}
    models = by_name(convert_document(env, result.document))
    labels = {name: (m.label if (m := models[name].get_measure("n")) else None) for name in ("customers", "orders")}
    assert labels == {"customers": "Customers", "orders": "Orders"}


async def test_restored_measure_attributes(env) -> None:
    model = orders(measures=[*BASIC_MEASURES, measure("avg_amount", "avg(amount)", label="Average",
                                                      meta={"owner": "finance"})])
    result = await export_models(env, customers(), model)
    avg = by_name(convert_document(env, result.document))["orders"].get_measure("avg_amount")
    assert avg is not None
    assert avg.label == "Average"
    assert (avg.meta or {}).get("owner") == "finance"


# ───────────────────────────────── joins ───────────────────────────────────

async def test_inner_join_restored(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(join_type=JoinType.INNER, name="buyer")])
    result = await export_models(env, customers(), model)
    join = _join(by_name(convert_document(env, result.document))["orders"], "customers")
    assert join.join_type is JoinType.INNER
    assert join.name == "buyer"


async def test_one_to_many_declaration_restored(env) -> None:
    to_orders = ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]],
                          cardinality=JoinCardinality.ONE_TO_MANY)
    result = await export_models(env, customers(joins=[to_orders]), orders(measures=BASIC_MEASURES, joins=[]))
    models = by_name(convert_document(env, result.document))
    join = _join(models["customers"], "orders")
    assert join.cardinality is JoinCardinality.ONE_TO_MANY
    assert join.join_pairs == [["id", "customer_id"]]
    assert models["orders"].joins == []


async def test_undetermined_cardinality_restored(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(cardinality=None)])
    result = await export_models(env, customers(), model)
    assert _join(by_name(convert_document(env, result.document))["orders"], "customers").cardinality is None


# ──────────────────────────── malformed payloads ───────────────────────────

def _newly_reported(env: Env, *, unedited: dict[str, Any], edited: dict[str, Any], name: str) -> list:
    baseline = {e.message for e in mentions(convert_document(env, unedited), name)}
    imported = convert_document(env, edited)
    return [e for e in mentions(imported, name) if e.message not in baseline]


async def test_malformed_payload_is_reported_and_ignored(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES, hidden=True))
    edited = copy.deepcopy(result.document)
    [entry] = slayer_entries(dataset(edited, "orders"))
    del entry["data"]["kind"]
    restored = by_name(convert_document(env, edited))["orders"]
    assert not restored.hidden
    assert {"id", "amount"} <= {c.name for c in restored.columns}
    assert _newly_reported(env, unedited=result.document, edited=edited, name="orders")


async def test_unknown_payload_version_is_reported_and_ignored(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES, hidden=True))
    edited = copy.deepcopy(result.document)
    [entry] = slayer_entries(dataset(edited, "orders"))
    entry["data"]["version"] = 999
    assert not by_name(convert_document(env, edited))["orders"].hidden
    assert _newly_reported(env, unedited=result.document, edited=edited, name="orders")


async def test_payload_on_the_wrong_entity_is_reported_and_ignored(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES, hidden=True))
    edited = copy.deepcopy(result.document)
    orders_ds = dataset(edited, "orders")
    [entry] = slayer_entries(orders_ds)
    amount = field(orders_ds, "amount")
    assert amount is not None
    amount["nonportable_extensions"] = [entry]
    orders_ds["nonportable_extensions"] = []
    assert not by_name(convert_document(env, edited))["orders"].hidden
    assert _newly_reported(env, unedited=result.document, edited=edited, name="amount")


async def test_foreign_extension_stored_as_an_object(env) -> None:
    acme = {"vendor_name": "ACME", "data": {"k": 1, "nested": {"flag": True}}}
    doc = flat([ds("orders", fields=[f("id", "o_orderkey"), f("amount", nonportable_extensions=[acme])],
                   primary_key=["id"], nonportable_extensions=[acme])])
    imported = by_name(convert_document(env, doc))["orders"]
    assert (imported.meta or {})["osi_nonportable_extensions"] == [acme]
    amount = imported.get_column("amount")
    assert amount is not None
    assert (amount.meta or {})["osi_nonportable_extensions"] == [acme]


async def test_slayer_extensions_are_not_stored_as_foreign(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    imported = by_name(convert_document(env, result.document))["orders"]
    stored_extensions = [e for c in imported.columns for e in (c.meta or {}).get("osi_nonportable_extensions", [])]
    stored_extensions += (imported.meta or {}).get("osi_nonportable_extensions", [])
    assert all(e["vendor_name"] != SLAYER for e in stored_extensions)


# ───────────────────────────── per-entity isolation ────────────────────────

async def test_restored_join_to_dropped_dataset(env) -> None:
    model = rich_orders()
    model.joins = [to_customers(name="buyer", join_type=JoinType.INNER)]
    model.columns = [c for c in model.columns if c.name != "segment"]
    model.measures = BASIC_MEASURES
    result = await export_models(env, customers(), model)
    dataset(result.document, "customers")["source"] = "no_such_table"
    imported = convert_document(env, result.document)
    restored = by_name(imported)["orders"]
    assert restored.joins == []
    assert mentions(imported, "buyer")
    amount = restored.get_column("amount")
    assert amount is not None
    assert amount.format is not None
    assert amount.label == "Order amount"
    aov = restored.get_measure("aov")
    assert aov is not None
    assert aov.formula == "revenue / n"


async def test_carried_content_that_does_not_validate_is_skipped(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    imported = convert_document(env, _without(result.document, "customers"))
    restored = by_name(imported)["orders"]
    assert restored.get_column("segment") is None
    assert restored.get_measure("discounted") is None
    assert mentions(imported, "segment")
    assert mentions(imported, "discounted")
    revenue_growth = restored.get_measure("revenue_growth")
    assert revenue_growth is not None


# ─────────────────────────────── sources ───────────────────────────────────

def _active_orders() -> SlayerModel:
    return orders(name="active_orders", joins=[], filters=["status <> 'test'", "region IS NOT NULL"],
                  measures=BASIC_MEASURES)


async def test_filtered_model_restored(env) -> None:
    result = await export_models(env, _active_orders())
    restored = by_name(convert_document(env, result.document))["active_orders"]
    assert restored.sql_table == "orders"
    assert restored.sql is None
    assert restored.filters == ["status <> 'test'", "region IS NOT NULL"]


async def test_edited_source_falls_back_to_the_source(env) -> None:
    result = await export_models(env, _active_orders())
    active = dataset(result.document, "active_orders")
    active["source"] = "SELECT * FROM orders WHERE amount > 5"
    imported = convert_document(env, result.document)
    restored = by_name(imported)["active_orders"]
    assert restored.filters == []
    assert restored.sql == "SELECT * FROM orders WHERE amount > 5"
    assert mentions(imported, "active_orders", severity="info")


async def test_query_backed_model_restored(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES), monthly_revenue())
    imported, _ = await reimport(env, result.document)
    restored = await stored(imported, "monthly_revenue")
    original = monthly_revenue()
    assert [q.model_dump(exclude_none=True) for q in restored.source_queries or []] == [
        q.model_dump(exclude_none=True) for q in original.source_queries or []]
    assert restored.query_variables == {"region": "US"}


async def test_query_backed_model_whose_input_is_missing(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES), monthly_revenue())
    imported, out = await reimport(env, _without(result.document, "orders"))
    restored = await stored(imported, "monthly_revenue")
    assert not restored.source_queries
    assert restored.sql
    report = out.split("Conversion report:", 1)[1]
    assert "monthly_revenue" in report


# ──────────────────────────── SLayer-only content ──────────────────────────

async def test_transform_measure_restored(env) -> None:
    model = orders(measures=[*BASIC_MEASURES, measure("revenue_growth", "change_pct(revenue)")])
    imported, _ = await reimport(env, (await export_models(env, customers(), model)).document)
    restored = await stored(imported, "orders")
    assert {"revenue", "revenue_growth"} <= {m.name for m in restored.measures}
    payload = {"source_model": "orders", "measures": ["revenue_growth"],
               "time_dimensions": [{"dimension": "ordered_on", "granularity": "month"}]}
    assert normalized(await rows(imported, payload)) == normalized(await rows(env, payload))
