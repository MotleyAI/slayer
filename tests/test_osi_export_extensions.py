"""Ossie export: the SLAYER extension, foreign extensions, --no-extensions, and the export report."""

from pathlib import Path

import pytest

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.models import DatasourceConfig, ModelJoin
from slayer.ingest_report import ConversionReport, ConversionResult
from slayer.osi import slayer_extension as ext
from slayer.osi.exporter import OsiExportResult
from slayer.osi.models import OSIExpression
from slayer.storage.yaml_storage import YAMLStorage
from tests._osi_export_fixtures import (
    BASIC_MEASURES, col, customers, measure, monthly_revenue, orders, orders_columns, rich_orders, to_customers,
    visits,
)
from tests._osi_export_helpers import (
    SLAYER, all_metrics, carries, dataset, dataset_names, doc_text, entities, export, export_models, field,
    make_env, mentions, payload, reimport, relationships, slayer_entries, validate,
)
from tests._osi_helpers import ds, f, flat

_SLAYER_ONLY_MEASURES = ["discounted", "revenue_growth", "running", "latest_amount", "rev_30d", "region_total"]


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _metric_names(document: dict) -> set[str]:
    return {m["name"] for m in all_metrics(document)}


# ───────────────────────────── SLAYER extension ────────────────────────────

async def test_every_entity_carries_one_versioned_slayer_extension(env) -> None:
    result = await export_models(env, customers(), rich_orders(), visits(), monthly_revenue())
    for entity in entities(result.document):
        assert len(slayer_entries(entity)) == 1, entity["name"]
        assert isinstance(payload(entity)["version"], int)


def test_fingerprint_normalization_is_stable() -> None:
    # A sqlglot upgrade that changes these renderings invalidates every exported fingerprint.
    expression = OSIExpression.model_validate({
        "sql": "sum(amount) filter (where status = 'completed') / nullif(count(distinct customer_id), 0)",
        "dialects": [{"dialect": "DUCKDB", "expression": "percentile_cont(0.9) within group (order by amount)"}],
    })
    assert ext.expression_digest(expression) == [
        "SUM(IFF(status = 'completed', amount, NULL)) / NULLIF(COUNT(DISTINCT customer_id), 0)",
        [["DUCKDB", "QUANTILE_CONT(amount, 0.9 ORDER BY amount)"]],
    ]
    source = ext.source_digest("select o_orderkey, amount from orders where region = 'US'", reader="duckdb")
    assert source == "SELECT o_orderkey, amount FROM orders WHERE region = 'US'"


@pytest.mark.parametrize("name", _SLAYER_ONLY_MEASURES)
async def test_slayer_only_measure_travels_in_the_extension(env, name: str) -> None:
    model = rich_orders()
    result = await export_models(env, customers(), model)
    assert name not in _metric_names(result.document)
    formula = next(m.formula for m in model.measures if m.name == name)
    assert carries(dataset(result.document, "orders"), formula)
    [entry] = mentions(result, name, severity="info", category="extension_only")
    assert entry.suggestion


async def test_column_reading_joined_model_travels_in_the_extension(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    orders_ds = dataset(result.document, "orders")
    assert field(orders_ds, "segment") is None
    assert carries(orders_ds, "customers.segment")
    assert mentions(result, "segment", severity="info", category="extension_only")


async def test_join_attributes_travel_in_the_extension(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(name="buyer", join_type=JoinType.INNER)])
    [rel] = relationships((await export_models(env, customers(), model)).document)
    assert carries(rel, "inner")
    assert carries(rel, "buyer")


async def test_access_tags_travel_in_the_extension(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES, access_tags=["finance"]))
    assert carries(dataset(result.document, "orders"), "finance")


async def test_foreign_extension_preserved(env) -> None:
    acme = {"vendor_name": "ACME", "data": {"k": 1}}
    doc = flat([ds("orders", fields=[f("id", "o_orderkey"), f("amount", nonportable_extensions=[acme])],
                   primary_key=["id"])])
    imported, _ = await reimport(env, doc)
    exported = (await export(imported)).document
    amount = field(dataset(exported, "orders"), "amount")
    assert amount is not None
    assert acme in amount["nonportable_extensions"]
    assert len(slayer_entries(amount)) == 1
    validate(exported)


async def test_malformed_foreign_extension_is_reported_and_omitted(env) -> None:
    malformed = col("amount", meta={"osi_nonportable_extensions": [{"vendor_name": "ACME"}]})
    columns = [malformed if c.name == "amount" else c for c in orders_columns()]
    result = await export_models(env, customers(), orders(columns=columns, measures=BASIC_MEASURES))
    amount = field(dataset(result.document, "orders"), "amount")
    assert amount is not None
    assert all(e["vendor_name"] == SLAYER for e in amount.get("nonportable_extensions") or [])
    assert mentions(result, "ACME")
    validate(result.document)


async def test_reexport_keeps_one_slayer_extension_per_entity(env) -> None:
    first = await export_models(env, customers(), rich_orders())
    imported, _ = await reimport(env, first.document)
    for entity in entities((await export(imported)).document):
        assert len(slayer_entries(entity)) == 1, entity["name"]


# ──────────────────────────── --no-extensions ──────────────────────────────

async def test_no_extensions_writes_no_slayer_extension(env) -> None:
    result = await export_models(env, customers(), rich_orders(), visits(), extensions=False)
    assert all(not slayer_entries(e) for e in entities(result.document))
    validate(result.document)


async def test_access_tagged_model_not_exported_without_extensions(env) -> None:
    payroll = orders(name="payroll", joins=[], measures=BASIC_MEASURES, access_tags=["hr"])
    result = await export_models(env, customers(), payroll, extensions=False)
    assert "payroll" not in dataset_names(result.document)
    assert mentions(result, "payroll", severity="dropped")


async def test_inner_join_is_lossy_without_extensions(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(join_type=JoinType.INNER)])
    result = await export_models(env, customers(), model, extensions=False)
    assert len(relationships(result.document)) == 1
    [entry] = [e for e in result.entries if e.category == "lossy"]
    assert entry.severity == "info"
    assert entry.suggestion


async def test_one_to_many_join_is_lossy_without_extensions(env) -> None:
    to_orders = ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]],
                          cardinality=JoinCardinality.ONE_TO_MANY)
    result = await export_models(env, customers(joins=[to_orders]), orders(measures=BASIC_MEASURES, joins=[]),
                                 extensions=False)
    assert len(relationships(result.document)) == 1
    assert [e.category for e in result.entries] == ["lossy"]


async def test_relocated_measure_is_lossy_without_extensions(env) -> None:
    model = orders(measures=[*BASIC_MEASURES, measure("customer_discount", "sum(customers.discount)")])
    result = await export_models(env, customers(), model, extensions=False)
    assert mentions(result, "customer_discount", severity="info", category="lossy")


async def test_slayer_only_content_is_dropped_without_extensions(env) -> None:
    result = await export_models(env, customers(), rich_orders(), extensions=False)
    for name in [*_SLAYER_ONLY_MEASURES, "segment"]:
        assert mentions(result, name, severity="dropped"), name
    assert not [e for e in result.entries if e.category == "extension_only"]


# ─────────────────────────────── report ────────────────────────────────────

async def test_complete_report(env) -> None:
    m2m = ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                    cardinality=JoinCardinality.MANY_TO_MANY)
    growth = measure("revenue_growth", "change_pct(revenue)")
    result = await export_models(env, customers(), orders(measures=[*BASIC_MEASURES, growth]), visits(joins=[m2m]))
    assert sorted((e.severity, e.category if e.severity == "info" else None) for e in result.entries) == [
        ("dropped", None), ("info", "extension_only")]
    dropped = next(e for e in result.entries if e.severity == "dropped")
    assert "visits" in (dropped.model_name, dropped.metric_name) or "visits" in dropped.message
    assert mentions(result, "revenue_growth", severity="info")
    assert all(e.suggestion for e in result.entries)


@pytest.mark.parametrize("extensions", [True, False])
async def test_every_entry_has_entity_reason_and_workaround(env, extensions: bool) -> None:
    b2b = orders(name="b2b_orders", filters=["customers.segment = 'b2b'"], measures=BASIC_MEASURES)
    result = await export_models(env, customers(), rich_orders(), b2b, visits(), extensions=extensions)
    assert result.entries
    for entry in result.entries:
        assert entry.model_name or entry.metric_name
        assert entry.message
        assert entry.suggestion
        assert entry.severity in ("dropped", "info")


async def test_report_renders_workarounds(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    rendered = result.render_report()
    assert "revenue_growth" in rendered
    assert "workaround:" in rendered


def test_import_and_export_results_share_the_report_base() -> None:
    assert issubclass(OsiExportResult, ConversionReport)
    assert issubclass(ConversionResult, ConversionReport)


async def test_connection_details_memories_and_samples_are_not_exported(env) -> None:
    storage = YAMLStorage(base_dir=str(env.store))
    config = await storage.get_datasource(env.datasource)
    assert isinstance(config, DatasourceConfig)
    await storage.save_datasource(config.model_copy(update={"username": "u53r", "password": "s3cret"}))
    await storage.save_memory(learning="zzmemory: refunds live in status", entities=["orders.status"])
    model = customers()
    for c in model.columns:
        if c.name == "segment":
            c.sampled, c.sampled_values, c.distinct_count = "zzsampled", ["zzvalue"], 2
    result = await export_models(env, model, orders(measures=BASIC_MEASURES))
    text = doc_text(result.document)
    for secret in ("s3cret", "u53r", str(env.db), "zzmemory", "zzsampled", "zzvalue"):
        assert secret not in text, secret
    assert not result.entries
