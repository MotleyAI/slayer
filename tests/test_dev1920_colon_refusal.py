"""Fresh colon-spelled input is refused naming its functional twin (spec: aggregations/functional-form › The colon
spelling is refused)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from slayer.core.enums import AGGREGATION_ALIASES, BUILTIN_AGGREGATIONS
from slayer.core.errors import ColonSpellingError
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.syntax import parse_expr
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.service import MemoryService
from slayer.storage.base import StorageBackend
from tests._dev1920_colon_errors import assert_colon_refused
from tests._dev1920_fixtures import (  # noqa: F401 — fixture
    DS,
    FISCAL_QUARTER,
    StoreFactory,
    make_store,
    rows,
    run,
)

AGG_NAMES = sorted(BUILTIN_AGGREGATIONS | set(AGGREGATION_ALIASES))

#: (colon text, its twin) — one colon span each.
SPANS = [
    pytest.param("amount:sum", "sum(amount)", id="measure"),
    pytest.param("*:count", "count(orders.*)", id="star"),
    pytest.param("customers.*:count", "count(customers.*)", id="cross-model-star"),
    pytest.param("customers.regions.population:sum", "sum(customers.regions.population)", id="join-path"),
    pytest.param('"order total":sum', 'sum("order total")', id="quoted-source"),
    pytest.param("price:percentile(p=0.9)", "percentile(price, p=0.9)", id="keyword-param"),
    pytest.param("price:percentile(0.9)", "percentile(price, 0.9)", id="positional-param"),
    pytest.param("balance:last(updated_at)", "last(balance, updated_at)", id="positional-arg"),
    pytest.param("amount:sum(window='30d')", "sum(amount, window='30d')", id="window"),
    pytest.param("amount:sum(partition_by=region)", "sum(amount, partition_by=region)", id="partition-by"),
    pytest.param("created_at:month", "date_trunc('month', created_at)", id="granularity"),
    pytest.param("customers.created_at:month", "date_trunc('month', customers.created_at)", id="joined-granularity"),
    pytest.param("created_at:fiscal_quarter", "fiscal_quarter(created_at)", id="custom-granularity"),
]


def _refused(text: str) -> ColonSpellingError:
    with pytest.raises(ColonSpellingError) as ei:
        parse_expr(text)
    return ei.value


class TestParser:
    @pytest.mark.parametrize("agg", AGG_NAMES)
    def test_every_builtin_and_alias(self, agg: str) -> None:
        err = _refused(f"amount:{agg}")
        assert (err.span, err.twin) == (f"amount:{agg}", f"{agg}(amount)")

    @pytest.mark.parametrize(("text", "twin"), SPANS)
    def test_span(self, text: str, twin: str) -> None:
        err = _refused(text)
        assert (err.span, err.twin) == (text, twin)

    @pytest.mark.parametrize(("text", "span", "twin"), [
        pytest.param("amount:sum > 100", "amount:sum", "sum(amount)", id="comparison"),
        pytest.param("rank(amount:sum, direction='desc')", "amount:sum", "sum(amount)", id="transform-input"),
        pytest.param("sum(amount) / *:count", "*:count", "count(orders.*)", id="arithmetic"),
        pytest.param("amount:sum FILTER (WHERE status = 'paid')", "amount:sum", "sum(amount)", id="filter-clause"),
    ])
    def test_span_inside_larger_text(self, text: str, span: str, twin: str) -> None:
        err = _refused(text)
        assert (err.span, err.twin) == (span, twin)

    def test_is_a_value_error(self) -> None:
        assert isinstance(_refused("amount:sum"), ValueError)

    @pytest.mark.parametrize("text", [
        "orders.note = 'a:sum'",
        "sum(orders.amount::INT)",
        "sum(orders.amount) FILTER (WHERE orders.note = 'x:count')",
        '"x:sum"',
        "sum(amount) -- x:sum",
    ])
    def test_colons_that_are_not_the_colon_spelling(self, text: str) -> None:
        parse_expr(text)


async def _dry_run(engine: SlayerQueryEngine, query: dict) -> None:
    """Validate ``query`` and dry-run it; the refusal may come from either step."""
    await engine.execute(SlayerQuery.model_validate(query), dry_run=True)


async def _query_failure(storage: StorageBackend, query: dict) -> BaseException:
    engine = SlayerQueryEngine(storage=storage)
    try:
        with pytest.raises(ValueError) as ei:
            await _dry_run(engine, query)
    finally:
        engine.close()
    return ei.value


#: (query fields, colon span, twin).
QUERY_POSITIONS = [
    pytest.param({"metrics": ["amount:sum"]}, "amount:sum", "sum(amount)", id="measure"),
    pytest.param({"metrics": [{"alias": "t", "metric": "amount:sum"}]}, "amount:sum", "sum(amount)",
                 id="named-measure"),
    pytest.param({"metrics": ["*:count"]}, "*:count", "count(orders.*)", id="star"),
    pytest.param({"metrics": ["customers.*:count"]}, "customers.*:count", "count(customers.*)",
                 id="cross-model-star"),
    pytest.param({"metrics": ["price:percentile(p=0.9)"]}, "price:percentile(p=0.9)", "percentile(price, p=0.9)",
                 id="keyword-param"),
    pytest.param({"metrics": ["balance:last(updated_at)"]}, "balance:last(updated_at)",
                 "last(balance, updated_at)", id="positional-param"),
    pytest.param({"metrics": ["amount:sum(window='30d')"], "time_dimensions": ["date_trunc('month', created_at)"]},
                 "amount:sum(window='30d')", "sum(amount, window='30d')", id="window"),
    pytest.param({"metrics": ["count(orders.*)"], "filter": ["amount:sum > 100"]}, "amount:sum", "sum(amount)",
                 id="filter"),
    pytest.param({"metrics": ["count(orders.*)"], "dimensions": ["status"],
                  "order": [{"field": "amount:sum", "direction": "desc"}]}, "amount:sum", "sum(amount)",
                 id="order-string"),
    pytest.param({"metrics": ["count(orders.*)"], "dimensions": ["status"],
                  "order": [{"field": "amount:sum", "direction": "desc"}]}, "amount:sum", "sum(amount)",
                 id="order-object"),
    pytest.param({"metrics": ["count(orders.*)"], "dimensions": ["status"], "order": [{"amount:sum": "desc"}]},
                 "amount:sum", "sum(amount)", id="order-shorthand"),
    pytest.param({"metrics": ["count(orders.*)"], "dimensions": ["created_at:month"]}, "created_at:month",
                 "date_trunc('month', created_at)", id="dimension"),
    pytest.param({"metrics": ["count(orders.*)"], "time_dimensions": ["created_at:month"]}, "created_at:month",
                 "date_trunc('month', created_at)", id="time-dimension"),
    pytest.param({"metrics": ["count(orders.*)"], "time_dimensions": [{"field": "created_at:month",
                                                                  "granularity": "month"}]},
                 "created_at:month", "date_trunc('month', created_at)", id="time-dimension-column"),
    pytest.param({"metrics": ["count(orders.*)"], "dimensions": ["status"],
                  "order": [{"field": "customers.created_at:month", "direction": "asc"}]},
                 "customers.created_at:month", "date_trunc('month', customers.created_at)", id="order-granularity"),
    pytest.param({"metrics": ["count(orders.*)"], "main_time_dimension": "created_at:month"}, "created_at:month",
                 "date_trunc('month', created_at)", id="main-time-dimension"),
    pytest.param({"metrics": ["count(orders.*)"],
                  "dimensions": [{"alias": "d", "field": "amount:sum(partition_by=region)"}]},
                 "amount:sum(partition_by=region)", "sum(amount, partition_by=region)", id="computed-dimension"),
    pytest.param({"population": {"dataset": "orders", "metrics": [{"name": "m", "expression": "amount:sum"}]},
                  "metrics": ["orders.m"]}, "amount:sum", "sum(amount)", id="model-extension-measure"),
]


class TestFreshQueries:
    @pytest.mark.parametrize(("fields", "span", "twin"), QUERY_POSITIONS)
    async def test_position(self, make_store: StoreFactory, fields: dict, span: str,  # noqa: F811
                            twin: str) -> None:
        storage = await make_store()
        exc = await _query_failure(storage, {"population": "orders", **fields})
        assert_colon_refused(exc, span=span, twin=twin)

    @pytest.mark.parametrize("agg", AGG_NAMES)
    async def test_every_builtin_and_alias_as_a_measure(self, make_store: StoreFactory,  # noqa: F811
                                                        agg: str) -> None:
        storage = await make_store()
        exc = await _query_failure(storage, {"population": "orders", "metrics": [f"amount:{agg}"]})
        assert_colon_refused(exc, span=f"amount:{agg}", twin=f"{agg}(amount)")

    async def test_custom_granularity(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store(granularities=[FISCAL_QUARTER])
        exc = await _query_failure(storage, {"population": "orders", "metrics": ["count(orders.*)"],
                                             "time_dimensions": ["created_at:fiscal_quarter"]})
        assert_colon_refused(exc, span="created_at:fiscal_quarter", twin="fiscal_quarter(created_at)")

    async def test_non_colon_colons_run(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        resp = await run(storage, {"population": "orders", "filter": ["orders.note = 'a:sum'"], "metrics": [
            {"alias": "total", "metric": "sum(orders.amount::INT)"},
            {"alias": "tagged", "metric": "sum(orders.amount) FILTER (WHERE orders.note = 'x:count')"},
        ]})
        assert rows(resp) == [{"total": 60.0, "tagged": None}]


async def _save_failure(storage: StorageBackend, doc: dict) -> BaseException:
    model = SlayerDataset.model_validate(doc)
    engine = SlayerQueryEngine(storage=storage)
    try:
        with pytest.raises(ValueError) as ei:
            await engine.save_dataset(model)
    finally:
        engine.close()
    return ei.value


class TestSaves:
    async def test_model_measure(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        doc = {"name": "orders_m", "sql_table": "orders", "data_source": DS,
               "fields": [{"name": "order_id", "datatype": "Integer", "primary_key": True},
                           {"name": "amount", "datatype": "Float"}],
               "metrics": [{"name": "total", "expression": "amount:sum"}]}
        exc = await _save_failure(storage, doc)
        assert_colon_refused(exc, span="amount:sum", twin="sum(amount)")
        assert await storage.get_dataset("orders_m", data_source=DS) is None

    async def test_query_backed_model(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        doc = {"name": "order_count", "data_source": DS,
               "source_queries": [{"population": "orders", "metrics": [{"alias": "n", "metric": "*:count"}]}]}
        exc = await _save_failure(storage, doc)
        assert_colon_refused(exc, span="*:count", twin="count(orders.*)")
        assert await storage.get_dataset("order_count", data_source=DS) is None


EntityCall = Callable[[StorageBackend], Awaitable[Any]]


async def _recommend(storage: StorageBackend) -> Any:
    engine = SlayerQueryEngine(storage=storage)
    try:
        return await engine.recommend_population(["orders.amount:sum"], data_source=DS)
    finally:
        engine.close()


async def _save_memory_query(storage: StorageBackend) -> Any:
    return await MemoryService(storage=storage).save_memory(
        learning="revenue", linked_entities={"population": "orders", "metrics": ["amount:sum"]})


async def _save_memory(storage: StorageBackend) -> Any:
    return await MemoryService(storage=storage).save_memory(learning="revenue", linked_entities=["orders.amount:sum"])


class TestEntityReferences:
    @pytest.mark.parametrize("call", [_recommend, _save_memory], ids=["recommend-population", "memory"])
    async def test_refused(self, make_store: StoreFactory, call: EntityCall) -> None:  # noqa: F811
        storage = await make_store()
        with pytest.raises(ValueError) as ei:
            await call(storage)
        assert_colon_refused(ei.value, span="orders.amount:sum", twin="sum(orders.amount)")

    async def test_memory_query_is_refused_and_not_stored(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        with pytest.raises(ValueError) as ei:
            await _save_memory_query(storage)
        assert_colon_refused(ei.value, span="amount:sum", twin="sum(amount)")
        assert await storage.list_memories() == []
