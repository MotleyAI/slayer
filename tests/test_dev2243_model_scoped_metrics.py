"""Model-scoped metrics by bare name, their save-time rules, and ``AGG(dataset.metric)`` in every position.

Spec: queries/model-scoped-metrics. Appendix B seed: ``sum(orders.amount)`` = 650 over 4 customers; by region
EAST 350 / 2 customers, WEST 300 / 1, NORTH no orders / 1.
"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.errors import NoRowSourceError, UnknownMetricError
from slayer.core.models import DatasetMetric
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import StorageBackend

from tests._dev1954_fixtures import execution_spy
from tests._dev2243_fixtures import (
    DS,
    SALES,
    STORAGES,
    dataset,
    document_engine,
    document_text,
    error,
    field,
    metric,
    place_document,
    store_document,
)
from tests._dev2277_fixtures import DS as SEED_DS
from tests._dev2277_fixtures import (
    by,
    conversions,
    customers_dataset,
    execute_recording,
    orders_dataset,
    response_conversions,
    seed_datasets,
    seed_engine,
    table,
)

BACKENDS = ("duckdb", "sqlite")
RPC = "revenue_per_customer"
RPC_SQL = "AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)"
INLINE_RPC = "sum(orders.amount) / count(distinct customers.customer_id)"
REGION = "customers.region"
BY_REGION = {"dimensions": [REGION]}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def sales_with(mutate: Callable[[dict[str, Any]], None]) -> str:
    """``SALES`` with ``mutate`` applied to its parsed mapping."""
    doc = yaml.safe_load(SALES)
    mutate(doc)
    return yaml.safe_dump(doc, sort_keys=False)


def _ds(doc: dict[str, Any], name: str) -> dict[str, Any]:
    return next(d for d in doc["datasets"] if d["name"] == name)


def _add_dataset_metric(*, ds_name: str, name: str, sql: str) -> Callable[[dict[str, Any]], None]:
    def mutate(doc: dict[str, Any]) -> None:
        _ds(doc, ds_name).setdefault("metrics", []).append(metric(name, sql))
    return mutate


def _rename_rpc(new: str) -> Callable[[dict[str, Any]], None]:
    def mutate(doc: dict[str, Any]) -> None:
        doc["metrics"][0]["name"] = new
    return mutate


def _add_source_dataset(doc: dict[str, Any]) -> None:
    doc["datasets"].append(dataset("source", source="returns", primary_key=["return_id"],
                                   fields=[field("return_id"), field("amount", datatype="Float")]))


def _add_windowed_metric(doc: dict[str, Any]) -> None:
    doc["metrics"].append(metric("running_rev", "SUM(orders.amount) OVER ()"))


CRM = document_text(name="crm", datasets=[
    dataset("managers", primary_key=["manager_id"], fields=[field("manager_id"), field("name")]),
])


def only(resp: SlayerResponse, key: str) -> float:
    """The single row's ``key`` as a float."""
    assert len(resp.data) == 1, resp.data
    return float(resp.data[0][key])


def ordered(resp: SlayerResponse, key: str) -> list[tuple[Any, float]]:
    """``(region, value)`` in result order, NULL values dropped."""
    return [(r[REGION], float(r[key])) for r in resp.data if r[key] is not None]


def names(reply: str, name: str) -> bool:
    """``reply`` names ``name`` quoted."""
    return re.search(rf"['\"`]{re.escape(name)}['\"`]", reply) is not None


async def mcp_reply(storage: StorageBackend, tool: str, **arguments: Any) -> str:
    """One MCP ``tool`` call on a fresh server: its text, or the raised error's text."""
    server = create_mcp_server(storage=storage, _seed_help=False)
    try:
        blocks = (await server.call_tool(name=tool, arguments=arguments)).content
        return getattr(next(iter(blocks)), "text")
    except Exception as exc:  # noqa: BLE001 — the refusal text is the assertion subject
        return f"{type(exc).__name__}: {exc}"
    finally:
        await getattr(server, "_slayer_engine").aclose()


async def mcp_ok(storage: StorageBackend, tool: str, **arguments: Any) -> str:
    """One MCP ``tool`` call that must succeed."""
    server = create_mcp_server(storage=storage, _seed_help=False)
    try:
        blocks = (await server.call_tool(name=tool, arguments=arguments)).content
    finally:
        await getattr(server, "_slayer_engine").aclose()
    return getattr(next(iter(blocks)), "text")


async def stored_text(engine: SlayerQueryEngine, name: str = "sales") -> str:
    text = await engine.storage.get_semantic_model_text(name=name, data_source=DS)
    assert text is not None
    return text


def slayer_entries(entity: dict[str, Any]) -> list[Any]:
    """The ``data`` of ``entity``'s ``SLAYER`` non-portable extension entries."""
    return [e.get("data") for e in entity.get("nonportable_extensions") or [] if e.get("vendor_name") == "SLAYER"]


def without_extensions(entity: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in entity.items() if k != "nonportable_extensions"}


async def add_model_metric(engine: SlayerQueryEngine, name: str, sql: str) -> str:
    """``edit_semantic_model`` adding one model-scoped metric to ``sales``; the reply or refusal text."""
    return await mcp_reply(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                           metrics=[metric(name, sql)])


async def assert_refused(engine: SlayerQueryEngine, *, name: str, sql: str, error_name: str,
                         phrase: str | None = None) -> str:
    """The model-scoped metric ``name`` is refused with ``error_name`` and the stored document is unchanged."""
    before = await stored_text(engine)
    reply = await add_model_metric(engine, name, sql)
    assert error_name in reply, reply
    if phrase is not None:
        assert phrase.lower() in reply.lower(), reply
    assert await stored_text(engine) == before
    return reply


async def with_sales(engine: SlayerQueryEngine) -> SlayerQueryEngine:
    """Store ``SALES`` in ``engine``'s datasource (in the test body, so a missing storage method fails the test)."""
    await store_document(engine.storage, SALES)
    return engine


@pytest.fixture(params=BACKENDS)
async def sales(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    """The seed engine with nothing stored yet; the test calls ``with_sales``."""
    async with document_engine(backend=request.param, documents=()) as engine:
        yield engine


@pytest.fixture
async def shop() -> AsyncGenerator[SlayerQueryEngine]:
    """The DuckDB seed engine with nothing stored yet; the test calls ``with_sales``."""
    async with document_engine(documents=()) as engine:
        yield engine


@pytest.fixture(params=("sqlite", "duckdb"))
async def standalone(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    """The Appendix B seed as standalone datasets, ``customers.n = count(customer_id)``, and stored ``AGG`` metrics."""
    datasets = seed_datasets(
        customers=customers_dataset(metrics=[DatasetMetric(name="n", expression="count(customer_id)")]),
        orders=orders_dataset(metrics=[
            DatasetMetric(name="rev_agg", expression="AGG(total_revenue)"),
            DatasetMetric(name="cn", expression="customers.n"),
            DatasetMetric(name="cn_agg", expression="AGG(customers.n)"),
        ]),
    )
    async with seed_engine(request.param, datasets=datasets) as engine:
        yield engine


# --------------------------------------------------------------------------- #
# A bare name resolves as alias, then model-scoped metric
# --------------------------------------------------------------------------- #
async def test_a_bare_model_scoped_metric_in_a_root_less_query(sales: SlayerQueryEngine) -> None:
    """Spec scenario: A bare model-scoped metric in a root-less query."""
    await with_sales(sales)
    resp = await sales.execute({"metrics": [RPC]})
    assert resp.columns == [RPC]
    assert only(resp, RPC) == pytest.approx(162.5)


async def test_an_alias_shadows_a_model_scoped_metric(sales: SlayerQueryEngine) -> None:
    """Spec scenario: An alias shadows a model-scoped metric (filter and order name the alias)."""
    await with_sales(sales)
    resp = await sales.execute({
        **BY_REGION, "metrics": [{"metric": "sum(orders.amount)", "alias": RPC}],
        "filter": [f"{RPC} > 320"], "order": [{"field": RPC, "direction": "asc"}],
    })
    assert resp.columns == [REGION, RPC]
    assert by(resp, REGION, RPC) == {"EAST": 350.0}


async def test_neither_alias_nor_metric(sales: SlayerQueryEngine) -> None:
    """Spec scenario: Neither alias nor metric."""
    await with_sales(sales)
    with pytest.raises(UnknownMetricError) as info:
        await sales.execute({"metrics": ["nope"]})
    assert "'nope'" in str(info.value)
    assert "qualify" in str(info.value).lower()


@pytest.mark.parametrize("entry", ["now_metric", "now()", "1"])
async def test_a_root_less_metric_reading_no_dataset_is_refused(shop: SlayerQueryEngine, entry: str) -> None:
    """Ossie e14_measure_grain_no_dataset: a typed refusal naming the metric."""
    await store_document(shop.storage, sales_with(lambda doc: doc["metrics"].append(metric("now_metric", "NOW()"))))
    with pytest.raises(NoRowSourceError, match=re.escape(repr(entry))):
        await shop.execute({"metrics": [entry]}, dry_run=True)


async def test_a_bare_field_name_suggests_its_qualified_spelling(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    with pytest.raises(UnknownMetricError, match="'orders.amount'"):
        await shop.execute({"metrics": ["amount"]})


async def test_a_bare_dataset_metric_without_a_population_is_unknown(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    with pytest.raises(UnknownMetricError, match="'orders.total_revenue'"):
        await shop.execute({**BY_REGION, "metrics": ["total_revenue"]})


async def test_an_unknown_bare_name_fails_in_population_inference(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    with pytest.raises(UnknownMetricError, match="'nope'"):
        await shop.execute({**BY_REGION, "metrics": ["count(orders.*)"], "filter": ["nope > 1"]})


async def test_a_model_scoped_metric_wins_over_the_legacy_conversion() -> None:
    """With population ``orders`` also declaring ``revenue_per_customer``, the bare name is the model-scoped metric."""
    text = sales_with(_add_dataset_metric(ds_name="orders", name=RPC, sql="SUM(amount)"))
    async with document_engine(documents=(text,)) as engine:
        inline, _ = await execute_recording(engine, {"population": "orders",
                                                     "metrics": [{"metric": INLINE_RPC, "alias": RPC}]})
        resp, caught = await execute_recording(engine, {"population": "orders", "metrics": [RPC]})
    assert conversions(caught) == []
    assert response_conversions(resp) == []
    assert resp.columns == [RPC]
    assert only(resp, RPC) == pytest.approx(only(inline, RPC))
    assert only(resp, RPC) == pytest.approx(162.5)


async def test_the_legacy_conversion_follows_when_no_model_scoped_metric_matches(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    resp, caught = await execute_recording(shop, {"population": "orders", "metrics": ["total_revenue"]})
    assert conversions(caught) == [("total_revenue", "orders.total_revenue")]
    assert resp.columns == ["orders.total_revenue"]
    assert only(resp, "orders.total_revenue") == pytest.approx(650.0)


async def test_an_unknown_bare_name_with_a_population_is_unknown(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    with pytest.raises(UnknownMetricError, match="'nope'"):
        await shop.execute({"population": "orders", "metrics": ["nope"]})


async def test_recommend_population_resolves_a_model_scoped_metric(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    rec = await shop.recommend_population([REGION, RPC], data_source=DS)
    assert rec.reachable
    assert rec.population == "customers"
    assert {ip.input_item: ip.path for ip in rec.item_paths}[RPC] == RPC


async def test_recommend_population_matches_a_model_scoped_metric_in_any_case(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    rec = await shop.recommend_population([REGION, RPC.upper()], data_source=DS)
    assert rec.reachable
    assert RPC.upper() in {ip.input_item for ip in rec.item_paths}


@pytest.mark.parametrize(("item", "suggestion"), [("nope", "qualify"), ("amount", "'orders.amount'")])
async def test_recommend_population_refuses_an_unknown_bare_name(shop: SlayerQueryEngine, item: str,
                                                                 suggestion: str) -> None:
    await with_sales(shop)
    with pytest.raises(UnknownMetricError) as info:
        await shop.recommend_population([REGION, item], data_source=DS)
    assert suggestion in str(info.value).lower()


async def test_the_recommend_population_tool_resolves_a_model_scoped_metric(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    rec = json.loads(await mcp_ok(shop.storage, "recommend_population", items=[REGION, RPC], data_source=DS,
                                  format="json"))
    assert rec["population"] == "customers"
    assert RPC in {ip["input_item"] for ip in rec["item_paths"]}


async def test_the_recommend_population_tool_refuses_an_unknown_bare_name(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    reply = await mcp_ok(shop.storage, "recommend_population", items=[REGION, "nope"], data_source=DS)
    assert "UnknownMetricError" in reply
    assert "'nope'" in reply


async def test_the_query_tool_answers_a_bare_model_scoped_metric(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    out = json.loads(await mcp_ok(shop.storage, "query", query={"metrics": [RPC]}, format="json"))
    assert [{k: float(v) for k, v in row.items()} for row in out["data"]] == [{RPC: 162.5}]


@pytest.mark.parametrize("spelling", [RPC.upper(), "Revenue_Per_Customer", f'"{RPC}"'])
async def test_a_model_scoped_metric_matches_in_any_case(shop: SlayerQueryEngine, spelling: str) -> None:
    """Spec: queries/identifier-case — the key keeps the declared spelling."""
    await with_sales(shop)
    resp = await shop.execute({"metrics": [spelling]})
    assert resp.columns == [RPC]
    assert only(resp, RPC) == pytest.approx(162.5)


async def test_a_quoted_wrong_case_name_is_unknown(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    with pytest.raises(UnknownMetricError):
        await shop.execute({"metrics": ['"Revenue_Per_Customer"']})


async def test_a_mixed_case_declared_metric_keys_by_its_declared_spelling() -> None:
    async with document_engine(documents=(sales_with(_rename_rpc("RevenuePerCustomer")),)) as engine:
        resp = await engine.execute({"metrics": ["revenuepercustomer"]})
    assert resp.columns == ["RevenuePerCustomer"]
    assert only(resp, "RevenuePerCustomer") == pytest.approx(162.5)


# --------------------------------------------------------------------------- #
# A model-scoped metric in every query position
# --------------------------------------------------------------------------- #
async def test_a_model_scoped_metric_inside_a_metric_expression(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    resp = await shop.execute({"metrics": [{"metric": f"{RPC} * 100", "alias": "pct"}]})
    assert only(resp, "pct") == pytest.approx(16250.0)
    by_region = await shop.execute({**BY_REGION, "metrics": [{"metric": f"{RPC} * 100", "alias": "pct"}]})
    values = by(by_region, REGION, "pct")
    assert (values["EAST"], values["WEST"]) == (17500.0, 30000.0)


async def test_a_model_scoped_metric_in_a_measure_typed_filter(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    resp = await shop.execute({**BY_REGION, "metrics": [RPC], "filter": [f"{RPC} > 200"]})
    assert by(resp, REGION, RPC) == {"WEST": 300.0}


@pytest.mark.xfail(strict=True, reason="DEV-2387: a saved metric (here via AGG) in a filter is refused unless also listed")
async def test_a_model_scoped_metric_only_in_a_filter(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    resp = await shop.execute({**BY_REGION, "metrics": ["orders.total_revenue"], "filter": [f"{RPC} > 200"]})
    assert by(resp, REGION, "orders.total_revenue") == {"WEST": 300.0}


@pytest.mark.parametrize(("direction", "expected"), [
    ("asc", [("EAST", 175.0), ("WEST", 300.0)]), ("desc", [("WEST", 300.0), ("EAST", 175.0)]),
])
async def test_a_model_scoped_metric_as_an_order_key(shop: SlayerQueryEngine, direction: str,
                                                     expected: list[tuple[str, float]]) -> None:
    await with_sales(shop)
    resp = await shop.execute({**BY_REGION, "metrics": [RPC], "order": [{"field": RPC, "direction": direction}]})
    assert ordered(resp, RPC) == expected


async def test_an_aliased_model_scoped_metric_keys_by_its_alias(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    resp = await shop.execute({"metrics": [{"metric": RPC, "alias": "arpu"}]})
    assert resp.columns == ["arpu"]
    assert only(resp, "arpu") == pytest.approx(162.5)


# --------------------------------------------------------------------------- #
# A model-scoped metric evaluates as its expression in place
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("population", [None, "customers", "orders"])
async def test_inline_equality(sales: SlayerQueryEngine, population: str | None) -> None:
    """Spec scenario: Inline equality (under every row rule)."""
    await with_sales(sales)
    rooted = {"population": population} if population else {}
    named = await sales.execute({**rooted, **BY_REGION, "metrics": [RPC]})
    inline = await sales.execute({**rooted, **BY_REGION, "metrics": [{"metric": INLINE_RPC, "alias": RPC}]})
    assert named.columns == [REGION, RPC]
    assert table(named, REGION, RPC) == table(inline, REGION, RPC)
    values = by(named, REGION, RPC)
    assert (values["EAST"], values["WEST"]) == (175.0, 300.0)


# --------------------------------------------------------------------------- #
# References inside a model-scoped metric read like a query
# --------------------------------------------------------------------------- #
async def test_an_unqualified_field_reference_is_refused_at_save(shop: SlayerQueryEngine) -> None:
    """Spec scenario: An unqualified field reference is refused at save."""
    await with_sales(shop)
    await assert_refused(shop, name="revenue", sql="SUM(amount)",
                         error_name="UnqualifiedModelMetricReferenceError", phrase="qualified")


@pytest.mark.parametrize(("sql", "offender"), [
    ("COUNT(DISTINCT orders.seller_id)", "seller_id"),
    ("SUM(source.amount)", "source.amount"),
    ("orders.total_revenue / 2", "orders.total_revenue"),
    ("nope + 1", "nope"),
], ids=["source-column", "source-dot", "bare-dataset-metric", "bare-non-metric"])
async def test_a_reference_rule_is_enforced_at_save(shop: SlayerQueryEngine, sql: str, offender: str) -> None:
    await with_sales(shop)
    reply = await assert_refused(shop, name="bad", sql=sql, error_name="ModelMetricReferenceError")
    assert offender in reply


async def test_a_dataset_of_another_document_is_refused_at_save() -> None:
    async with document_engine(documents=(SALES, CRM)) as engine:
        reply = await assert_refused(engine, name="heads", sql="COUNT(managers.manager_id)",
                                     error_name="ModelMetricReferenceError")
    assert "managers" in reply


async def test_source_dot_is_a_field_when_a_dataset_is_named_source() -> None:
    async with document_engine(documents=(sales_with(_add_source_dataset),)) as engine:
        await mcp_ok(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                     metrics=[metric("returned", "SUM(source.amount)")])
        resp = await engine.execute({"metrics": ["returned"]})
    assert only(resp, "returned") == pytest.approx(35.0)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_metric_over_another_metric_stays_slayer_only(kind: str) -> None:
    """Spec scenario: A metric over another metric stays SLayer-only."""
    async with document_engine(kind=kind) as engine:
        await mcp_ok(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                     metrics=[metric("arpu_pct", f"{RPC} * 100")])
        resp = await engine.execute({"metrics": ["arpu_pct"]})
        doc = yaml.safe_load(await stored_text(engine))
    assert resp.columns == ["arpu_pct"]
    assert only(resp, "arpu_pct") == pytest.approx(16250.0)
    assert [m["name"] for m in doc["metrics"]] == [RPC]
    assert "arpu_pct" not in yaml.safe_dump(without_extensions(doc))
    entries = slayer_entries(doc)
    assert len(entries) == 1
    assert "arpu_pct" in yaml.safe_dump(entries[0])


# --------------------------------------------------------------------------- #
# Model-scoped metric names and definitions are checked at save
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name", ["null", "true"])
async def test_a_reserved_word_name_is_refused(shop: SlayerQueryEngine, name: str) -> None:
    await with_sales(shop)
    reply = await assert_refused(shop, name=name, sql=RPC_SQL, error_name="ModelMetricNameError")
    assert names(reply, name)


@pytest.mark.parametrize("name", ["sum", "SUM", "coalesce"])
async def test_a_name_equal_to_a_library_function_is_refused(shop: SlayerQueryEngine, name: str) -> None:
    """Spec scenario: A name equal to a library function is refused."""
    await with_sales(shop)
    reply = await assert_refused(shop, name=name, sql=RPC_SQL, error_name="ModelMetricNameError")
    assert names(reply, name)


@pytest.mark.parametrize("name", ["orders", "Customers"])
async def test_a_name_equal_to_a_dataset_name_is_refused(shop: SlayerQueryEngine, name: str) -> None:
    await with_sales(shop)
    reply = await assert_refused(shop, name=name, sql=RPC_SQL, error_name="ModelMetricNameError")
    assert names(reply, name)


async def test_a_circular_definition_is_refused(shop: SlayerQueryEngine) -> None:
    """Spec scenario: A circular definition is refused."""
    await with_sales(shop)
    before = await stored_text(shop)
    reply = await mcp_reply(shop.storage, "edit_semantic_model", name="sales", data_source=DS,
                            metrics=[metric("a", "b + 1"), metric("b", "a + 1")])
    assert "MetricCycleError" in reply, reply
    assert "a → b" in reply or "b → a" in reply
    assert await stored_text(shop) == before


# --------------------------------------------------------------------------- #
# AGG evaluates a dataset metric as one component
# --------------------------------------------------------------------------- #
async def test_agg_in_a_query_equals_the_metric_reference(sales: SlayerQueryEngine) -> None:
    """Spec scenario: AGG in a query equals the metric reference."""
    await with_sales(sales)
    agg = await sales.execute({**BY_REGION, "metrics": [{"metric": "AGG(orders.total_revenue)", "alias": "r"}]})
    ref = await sales.execute({**BY_REGION, "metrics": [{"metric": "orders.total_revenue", "alias": "r"}]})
    assert table(agg, REGION, "r") == table(ref, REGION, "r")
    values = by(agg, REGION, "r")
    assert (values["EAST"], values["WEST"]) == (350.0, 300.0)


async def test_agg_in_a_query_over_standalone_datasets(standalone: SlayerQueryEngine) -> None:
    agg = await standalone.execute({**BY_REGION, "metrics": [{"metric": "AGG(orders.total_revenue) * 2", "alias": "r"}]})
    ref = await standalone.execute({**BY_REGION, "metrics": [{"metric": "orders.total_revenue * 2", "alias": "r"}]})
    assert table(agg, REGION, "r") == table(ref, REGION, "r")
    values = by(agg, REGION, "r")
    assert (values["EAST"], values["WEST"]) == (700.0, 600.0)


async def test_agg_in_a_model_scoped_metric_equals_the_metric_reference(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    await mcp_ok(shop.storage, "edit_semantic_model", name="sales", data_source=DS,
                 metrics=[metric("rev_total", "AGG(orders.total_revenue)")])
    agg = await shop.execute({**BY_REGION, "metrics": [{"metric": "rev_total", "alias": "r"}]})
    ref = await shop.execute({**BY_REGION, "metrics": [{"metric": "orders.total_revenue", "alias": "r"}]})
    assert table(agg, REGION, "r") == table(ref, REGION, "r")
    values = by(agg, REGION, "r")
    assert (values["EAST"], values["WEST"]) == (350.0, 300.0)


@pytest.mark.parametrize(("agg_metric", "ref_metric", "east", "west"), [
    ("orders.rev_agg", "orders.total_revenue", 350.0, 300.0),
    ("orders.cn_agg", "orders.cn", 2.0, 1.0),
], ids=["same-dataset", "to-one-path"])
async def test_agg_in_a_stored_dataset_metric_equals_the_metric_reference(
        standalone: SlayerQueryEngine, agg_metric: str, ref_metric: str, east: float, west: float) -> None:
    agg = await standalone.execute({**BY_REGION, "metrics": [{"metric": agg_metric, "alias": "v"}]})
    ref = await standalone.execute({**BY_REGION, "metrics": [{"metric": ref_metric, "alias": "v"}]})
    assert table(agg, REGION, "v") == table(ref, REGION, "v")
    values = by(agg, REGION, "v")
    assert (values["EAST"], values["WEST"]) == (east, west)


async def test_a_document_dataset_metric_using_agg_is_kept_in_its_slayer_entry(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    await mcp_ok(shop.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                 metrics=[{"name": "rev_agg", "expression": "AGG(total_revenue)"}])
    agg = await shop.execute({**BY_REGION, "metrics": [{"metric": "orders.rev_agg", "alias": "v"}]})
    ref = await shop.execute({**BY_REGION, "metrics": [{"metric": "orders.total_revenue", "alias": "v"}]})
    assert table(agg, REGION, "v") == table(ref, REGION, "v")
    doc = yaml.safe_load(await stored_text(shop))
    orders = _ds(doc, "orders")
    assert [m["name"] for m in orders["metrics"]] == ["total_revenue"]
    assert "rev_agg" not in yaml.safe_dump(without_extensions(orders))
    assert any("rev_agg" in yaml.safe_dump(data) for data in slayer_entries(orders))
    assert "rev_agg" not in yaml.safe_dump(slayer_entries(doc))


AGG_QUERY_REFUSALS = [
    pytest.param("AGG(orders.amount)", "AggArgumentError", "AGG takes a dataset metric", id="field"),
    pytest.param("AGG(orders.total_revenue + 1)", "AggArgumentError", "AGG takes a dataset metric",
                 id="other-expression"),
    pytest.param("AGG(orders.total_revenue) FILTER (WHERE orders.status = 'paid')", "AggFilterError",
                 "AGG takes no FILTER clause", id="filter"),
]


@pytest.mark.parametrize(("expression", "error_name", "phrase"), AGG_QUERY_REFUSALS)
async def test_an_agg_refusal_in_a_query_precedes_sql(standalone: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch,
                                                      expression: str, error_name: str, phrase: str) -> None:
    """Spec scenarios: AGG of a field is refused; AGG with FILTER is refused (query position)."""
    sent = execution_spy(monkeypatch)
    query = {**BY_REGION, "metrics": [{"metric": expression, "alias": "x"}]}
    with pytest.raises(error(error_name), match=phrase):
        await standalone.execute(query, dry_run=True)
    with pytest.raises(error(error_name), match=phrase):
        await standalone.execute(query)
    assert sent == []


async def test_agg_of_a_model_scoped_metric_in_a_query_is_refused(shop: SlayerQueryEngine,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    await with_sales(shop)
    sent = execution_spy(monkeypatch)
    with pytest.raises(error("AggArgumentError"), match="AGG takes a dataset metric"):
        await shop.execute({**BY_REGION, "metrics": [{"metric": f"AGG({RPC})", "alias": "x"}]}, dry_run=True)
    assert sent == []


@pytest.mark.parametrize(("sql", "error_name", "phrase"), [
    ("AGG(orders.amount)", "AggArgumentError", "AGG takes a dataset metric"),
    (f"AGG({RPC})", "AggArgumentError", "AGG takes a dataset metric"),
    ("AGG(orders.total_revenue + 1)", "AggArgumentError", "AGG takes a dataset metric"),
    ("AGG(orders.total_revenue) FILTER (WHERE orders.status = 'paid')", "AggFilterError", "AGG takes no FILTER clause"),
], ids=["field", "model-scoped-metric", "other-expression", "filter"])
async def test_an_agg_refusal_in_a_model_scoped_metric_at_save(shop: SlayerQueryEngine, sql: str, error_name: str,
                                                               phrase: str) -> None:
    """Spec scenario: AGG with FILTER is refused (and every other AGG refusal) at save."""
    await with_sales(shop)
    await assert_refused(shop, name="bad_agg", sql=sql, error_name=error_name, phrase=phrase)


@pytest.mark.parametrize(("expression", "error_name", "phrase"), [
    ("AGG(amount)", "AggArgumentError", "AGG takes a dataset metric"),
    ("AGG(total_revenue + 1)", "AggArgumentError", "AGG takes a dataset metric"),
    ("AGG(total_revenue) FILTER (WHERE status = 'paid')", "AggFilterError", "AGG takes no FILTER clause"),
], ids=["field", "other-expression", "filter"])
async def test_an_agg_refusal_in_a_stored_dataset_metric_at_save(standalone: SlayerQueryEngine, expression: str,
                                                                 error_name: str, phrase: str) -> None:
    bad = orders_dataset(metrics=[DatasetMetric(name="bad_agg", expression=expression)])
    with pytest.raises(error(error_name), match=phrase):
        await standalone.save_dataset(bad)
    stored = await standalone.storage.get_dataset("orders", data_source=SEED_DS)
    assert stored is not None
    assert "bad_agg" not in {m.name for m in stored.metrics}


async def test_agg_of_a_model_scoped_metric_in_a_dataset_metric_is_refused(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    before = await stored_text(shop)
    reply = await mcp_reply(shop.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                            metrics=[{"name": "bad_agg", "expression": f"AGG({RPC})"}])
    assert "AggArgumentError" in reply, reply
    assert await stored_text(shop) == before


# --------------------------------------------------------------------------- #
# Inspect and summaries show model-scoped metrics
# --------------------------------------------------------------------------- #
async def test_inspecting_by_bare_name(shop: SlayerQueryEngine) -> None:
    """Spec scenario: Inspecting by bare name."""
    await with_sales(shop)
    out = await mcp_ok(shop.storage, "inspect", reference=RPC, entity_type="metric")
    assert RPC_SQL in out
    assert "sales" in out


async def test_inspect_shows_description_and_ai_context(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    out = await mcp_ok(shop.storage, "inspect", reference=RPC, entity_type="metric", compact=False)
    assert "Revenue per customer." in out
    assert "ARPU" in out


async def test_inspect_shows_a_model_scoped_metric_load_warning(tmp_path: Path) -> None:
    async with document_engine(documents=(), base=tmp_path) as engine:
        place_document(tmp_path, sales_with(_add_windowed_metric), name="sales")
        out = await mcp_ok(engine.storage, "inspect", reference="running_rev", entity_type="metric", compact=False)
    assert "running_rev" in out
    assert "window" in out.lower()


async def test_datasets_summary_lists_model_scoped_metrics(shop: SlayerQueryEngine) -> None:
    await with_sales(shop)
    out = await mcp_ok(shop.storage, "datasets_summary", datasource_name=DS)
    assert RPC in out
