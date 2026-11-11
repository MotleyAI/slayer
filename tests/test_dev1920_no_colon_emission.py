"""SLayer emits no colon text (spec: aggregations/functional-form › SLayer emits only the functional spelling)."""

from __future__ import annotations

import re

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.syntax import canonical_measure_text, parse_expr
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.live_schema.broken_dependents import _Dependent, _probe
from slayer.memories.resolver import _field_entity_tokens, _formula_entity_tokens, extract_entities_from_query
from slayer.memories.service import MemoryService
from tests._dev1920_fixtures import DS, StoreFactory, make_store  # noqa: F401 — fixture

_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_COLON_SPAN = re.compile(r"[\w*\"]:[A-Za-z_]")


def _has_colon_span(text: str) -> bool:
    return _COLON_SPAN.search(_STRING_LITERAL.sub("''", text)) is not None


class TestCanonicalText:
    @pytest.mark.parametrize("text", [
        "sum(amount)", "count(*)", "count(customers.*)", "sum(customers.regions.population)",
        "percentile(price, p=0.9)", "last(balance, updated_at)", "sum(amount, window='30d')",
    ])
    def test_single_aggregation_renders_as_written(self, text: str) -> None:
        assert canonical_measure_text(parse_expr(text)) == text

    @pytest.mark.parametrize("text", [
        "sum(amount) / count(*)", "cumsum(sum(amount))", "sum(amount) FILTER (WHERE status = 'paid')",
        "rank(sum(amount), direction='desc')", "sum(amount, partition_by=region)",
        "case when sum(amount, partition_by=customer_id) > 50 then 'high' else 'other' end",
        "sum(amount::INT) / max(price)",
    ])
    def test_renders_functional_and_round_trips(self, text: str) -> None:
        rendered = canonical_measure_text(parse_expr(text))
        assert not _has_colon_span(rendered), rendered
        assert parse_expr(rendered) == parse_expr(text)


def _catalog() -> FacadeCatalog:
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        measures=[ModelMeasure(name="aov", formula="sum(revenue) / count(*)", type=DataType.DOUBLE)],
        joins=[ModelJoin.model_validate({
            "target_model": "customers",
            "join_pairs": [["customer_id", "id"]],
            "cardinality": "many_to_one",
        })],
    )
    customers = SlayerModel(
        name="customers", data_source="jaffle", sql_table="customers",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="region", type=DataType.TEXT)],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, customers]})


def _translated(sql: str, *, catalog: FacadeCatalog | None = None) -> QueryResult:
    result = translate(sql=sql, catalog=catalog or _catalog(), dialect="postgres")
    assert isinstance(result, QueryResult)
    return result


def _stores_catalog(*, configured: bool) -> FacadeCatalog:
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]], join_type=JoinType.LEFT)]
        if configured else [],
    )
    stores = SlayerModel(
        name="stores", data_source="jaffle", sql_table="stores",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="tax_rate", type=DataType.DOUBLE),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, stores]})


def _joined_having(metric: str) -> str:
    """A Metabase-shaped LEFT JOIN onto ``stores`` filtering on the joined ``metric``."""
    return (
        'SELECT "public"."orders"."status" FROM "public"."orders" '
        f'LEFT JOIN (SELECT "public"."stores"."id" AS "id", "public"."stores"."{metric}" AS "{metric}" '
        'FROM "public"."stores") AS "Stores" ON "public"."orders"."store_id" = "Stores"."id" '
        f'GROUP BY "public"."orders"."status" HAVING "Stores"."{metric}" > 1'
    )


class TestFacade:
    def test_catalog_formulas_are_functional(self) -> None:
        tables = [t for schema in _catalog().schemas for t in schema.tables]
        formulas = [m.measure_formula for t in tables for m in t.metrics]
        assert formulas
        assert [f for f in formulas if _has_colon_span(f)] == []
        for formula in formulas:
            parse_expr(formula)

    def test_catalog_joined_metrics(self) -> None:
        [orders] = [t for schema in _catalog().schemas for t in schema.tables if t.name == "orders"]
        by_name = {m.name: m.measure_formula for m in orders.metrics}
        assert by_name["row_count"] == "count(*)"
        assert by_name["revenue_sum"] == "sum(revenue)"
        assert by_name["customers.row_count"] == "count(customers.*)"
        assert by_name["customers.region_count"] == "count(customers.region)"

    def test_aggregates_and_having(self) -> None:
        result = _translated(
            "SELECT status, COUNT(*), SUM(revenue) FROM orders GROUP BY status HAVING SUM(revenue) > 1000")
        assert [m.formula for m in result.query.measures or []] == ["count(*)", "sum(revenue)"]
        assert result.query.filters == ["sum(revenue) > 1000"]
        assert dict(result.column_name_mapping) == {
            "orders.status": "status", "orders.row_count": "row_count", "orders.revenue_sum": "revenue_sum"}

    @pytest.mark.parametrize(("sql", "formula"), [
        ("SELECT COUNT(status) FROM orders", "count(status)"),
        ("SELECT COUNT(DISTINCT status) FROM orders", "count_distinct(status)"),
        ("SELECT AVG(revenue) FROM orders", "avg(revenue)"),
        ("SELECT row_count FROM orders", "count(*)"),
        ("SELECT revenue_sum FROM orders", "sum(revenue)"),
    ])
    def test_projected_metric(self, sql: str, formula: str) -> None:
        assert [m.formula for m in _translated(sql).query.measures or []] == [formula]

    @pytest.mark.parametrize(("sql", "filters"), [
        ("SELECT status FROM orders WHERE revenue_sum > 1000", ["sum(revenue) > 1000"]),
        ("SELECT status FROM orders WHERE row_count > 1", ["count(*) > 1"]),
        ("SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) >= 5", ["count(*) >= 5"]),
    ])
    def test_metric_filters(self, sql: str, filters: list[str]) -> None:
        assert _translated(sql).query.filters == filters

    def test_order_by_aggregate(self) -> None:
        result = _translated("SELECT status, SUM(revenue) FROM orders GROUP BY status ORDER BY SUM(revenue) DESC")
        assert [(o.column.name, o.raw_formula) for o in result.query.order or []] == [("revenue_sum", None)]
        assert [m.formula for m in result.query.measures or []] == ["sum(revenue)"]

    @pytest.mark.parametrize("configured", [True, False], ids=["configured-join", "dynamic-join"])
    @pytest.mark.parametrize(("metric", "filter_"), [
        ("row_count", "count(stores.*) > 1"),
        ("name_count", "count(stores.name) > 1"),
        ("tax_rate_avg", "avg(stores.tax_rate) > 1"),
    ])
    def test_joined_metric_filter(self, configured: bool, metric: str, filter_: str) -> None:
        result = _translated(_joined_having(metric), catalog=_stores_catalog(configured=configured))
        assert result.query.filters == [filter_]


class TestBrokenDependentsProbe:
    def test_filter_dependant_probes_with_a_functional_row_count(self) -> None:
        probe = _probe(_Dependent(model="orders", kind="filter", name="f"))
        assert probe == {"source_model": "orders", "measures": ["count(*)"]}


class TestMemoryTokens:
    @pytest.mark.parametrize(("formula", "tokens"), [
        ("sum(amount)", [("amount", "sum")]),
        ("count(*)", [("*", "count")]),
        ("sum(customers.revenue)", [("customers.revenue", "sum")]),
        ("sum(amount) / count(*)", [("amount", "sum"), ("*", "count")]),
        ("cumsum(sum(amount), partition_by=region)", [("amount", "sum")]),
        ("sum(amount - cost)", ["amount", "cost"]),
        ("status", ["status"]),
    ])
    def test_formula_tokens(self, formula: str, tokens: list) -> None:
        assert list(_formula_entity_tokens(parse_expr(formula))) == tokens

    def test_filter_tokens_carry_no_colon_text(self) -> None:
        tokens = _field_entity_tokens("filter", "count(*) FILTER (WHERE amount > 0) > 1 AND sum(price) > 2")
        assert ("*", "count") in tokens
        assert ("price", "sum") in tokens
        assert [t for t in tokens if isinstance(t, str) and ":" in t] == []

    async def test_query_entities(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        query = SlayerQuery.model_validate({
            "source_model": "orders", "filters": ["max(price) > 1"],
            "measures": ["sum(amount) / count(*)", "count(customers.*)", "sum(customers.regions.population)"],
        })
        resolved = await extract_entities_from_query(query, storage=storage)
        assert resolved.canonical_forms == [
            "shop.orders", "shop.orders.amount", "shop.customers", "shop.regions.population", "shop.orders.price"]

    async def test_linked_entities(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        saved = await MemoryService(storage=storage).save_memory(
            learning="x", linked_entities=["sum(orders.amount)", "count(orders.*)"])
        assert saved.resolved_entities == ["shop.orders.amount", "shop.orders"]


async def test_recommendation_paths(make_store: StoreFactory) -> None:  # noqa: F811
    storage = await make_store()
    engine = SlayerQueryEngine(storage=storage)
    try:
        rec = await engine.recommend_root_model(
            ["sum(orders.amount)", "percentile(orders.amount, p=0.9)", "sum(customers.regions.population)"],
            data_source=DS)
    finally:
        engine.close()
    assert rec.root_model == "orders"
    assert [p.path for p in rec.item_paths] == [
        "sum(amount)", "percentile(amount, p=0.9)", "sum(customers.regions.population)"]
