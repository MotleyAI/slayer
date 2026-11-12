"""Stored colon spellings load as the functional spelling (spec: models/stored-upgrade › Stored colon spellings are
upgraded to the functional spelling)."""

from __future__ import annotations

import logging
import re
import warnings
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import pytest

from slayer.core.errors import GranularityCallError, NameCollisionError
from slayer.core.models import SlayerModel
from slayer.core.query import ComputedDimension, SlayerQuery, computed_dimension_name
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._dev1920_colon_errors import assert_colon_refused
from tests._dev1920_fixtures import (  # noqa: F401 — fixture
    BACKENDS,
    DIALECTS,
    DS,
    FISCAL_QUARTER,
    StoreFactory,
    customers_doc,
    make_store,
    memory_doc,
    orders_doc,
    query_backed_doc,
    regions_doc,
    rows,
    run,
    store_base,
    stored_query,
)
from tests._library_names_fixtures import SUM_A, agg, stored_engine, t_doc, value
from tests._granularity_rename_fixtures import (
    DS as EVENTS_DS,
    SUM,
    datasource_doc as events_datasource_doc,
    events_doc,
    granularity_definition,
    live_db,
    memory_doc as events_memory_doc,
    query_backed_doc as events_query_backed_doc,
    raw_documents,
    stage_doc,
    write_raw_store,
)


def _model(**kwargs: Any) -> SlayerModel:
    return SlayerModel.model_validate(orders_doc(**kwargs))


def _stored(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(stored_query(**fields))


def _fresh(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **fields})


class TestVersions:
    def test_versions_bumped(self) -> None:
        assert CURRENT_VERSIONS["SlayerModel"] == 22
        assert CURRENT_VERSIONS["SlayerQuery"] == 14
        assert CURRENT_VERSIONS["DatasourceConfig"] == 3
        assert CURRENT_VERSIONS["Memory"] == 10


#: (stored measure formula, the formula it loads as).
MODEL_MEASURES = [
    ("amount:sum", "sum(amount)"),
    ("*:count", "count(*)"),
    ("customers.*:count", "count(customers.*)"),
    ("customers.regions.population:sum", "sum(customers.regions.population)"),
    ("price:percentile(p=0.9)", "percentile(price, p=0.9)"),
    ("balance:last(updated_at)", "last(balance, updated_at)"),
]


def _measures(formulas: list[str]) -> list[dict]:
    return [{"name": f"m{i}", "formula": f} for i, f in enumerate(formulas)]


class TestModelMeasures:
    def test_formulas_load_functional(self) -> None:
        model = _model(measures=_measures([s for s, _ in MODEL_MEASURES]))
        assert [m.formula for m in model.measures] == [f for _, f in MODEL_MEASURES]

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_values_unchanged(self, make_store: StoreFactory, backend: str,  # noqa: F811
                                    dialect: str) -> None:
        stored = orders_doc(measures=_measures([s for s, _ in MODEL_MEASURES]))
        storage = await make_store(backend=backend, dialect=dialect, models=[stored, customers_doc(), regions_doc()])
        model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == [f for _, f in MODEL_MEASURES]
        names = [f"m{i}" for i in range(len(MODEL_MEASURES))]
        expected = await run(storage, {"source_model": "orders",
                                       "measures": _measures([f for _, f in MODEL_MEASURES])})
        assert rows(await run(storage, {"source_model": "orders", "measures": names})) == rows(expected)


#: (query field, stored value, the value a fresh functional query writes).
QUERY_POSITIONS = [
    pytest.param("measures", ["amount:sum"], ["sum(amount)"], id="measure"),
    pytest.param("measures", [{"name": "r", "formula": "rank(rev:sum)"}], [{"name": "r", "formula": "rank(sum(rev))"}],
                 id="transform-input"),
    pytest.param("measures", ["sum(amount::INT) / *:count"], ["sum(amount::INT) / count(*)"], id="cast-beside-span"),
    pytest.param("filters", ["amount:sum > 100 AND note = 'a:sum'"], ["sum(amount) > 100 AND note = 'a:sum'"],
                 id="filter"),
    pytest.param("dimensions", [{"name": "d", "expression": "amount:sum(partition_by=region)"}],
                 [{"name": "d", "expression": "sum(amount, partition_by=region)"}], id="computed-dimension"),
    pytest.param("dimensions", ["created_at:month"], ["date_trunc('month', created_at)"], id="dimension-granularity"),
    pytest.param("time_dimensions", ["created_at:month"], ["date_trunc('month', created_at)"],
                 id="time-dimension"),
    pytest.param("main_time_dimension", "created_at:month", "date_trunc('month', created_at)",
                 id="main-time-dimension"),
    pytest.param("order", [{"column": {"name": "amount_sum"}, "direction": "desc", "raw_formula": "amount:sum"}],
                 [{"column": "sum(amount)", "direction": "desc"}], id="order-persisted"),
    pytest.param("order", [{"column": {"name": "_count"}, "raw_formula": "*:count"}], [{"column": "count(*)"}],
                 id="order-star"),
    pytest.param("order", [{"column": {"name": "created_at_month"}, "raw_formula": "created_at:month"}],
                 [{"column": "date_trunc('month', created_at)"}], id="order-granularity"),
    pytest.param("order", [{"column": "rank(amount:sum, direction='desc')"}],
                 [{"column": "rank(sum(amount), direction='desc')"}], id="order-expression"),
    pytest.param("order", [{"amount:sum": "desc"}], [{"sum(amount)": "desc"}], id="order-shorthand"),
    pytest.param("source_model", {"source_name": "orders", "measures": [{"name": "m", "formula": "amount:sum"}]},
                 {"source_name": "orders", "measures": [{"name": "m", "formula": "sum(amount)"}]},
                 id="model-extension-measure"),
]


class TestQueryPositions:
    @pytest.mark.parametrize(("field", "stored", "functional"), QUERY_POSITIONS)
    def test_loads_as_the_fresh_functional_query(self, field: str, stored: Any, functional: Any) -> None:
        assert _stored(**{field: stored}).model_dump() == _fresh(**{field: functional}).model_dump()

    def test_inline_model_measure(self) -> None:
        inline = {k: v for k, v in orders_doc(measures=_measures(["amount:sum"])).items() if k != "version"}
        query = _stored(source_model=inline, measures=["m0"])
        assert isinstance(query.source_model, SlayerModel)
        assert [m.formula for m in query.source_model.measures] == ["sum(amount)"]

    def test_saved_query_stage(self) -> None:
        model = SlayerModel.model_validate(query_backed_doc(
            name="rev_q", stage=stored_query(measures=[{"name": "rev", "formula": "amount:sum"}],
                                             filters=["*:count > 1"])))
        [stage] = model.source_queries or []
        assert [m.formula for m in stage.measures or []] == ["sum(amount)"]
        assert stage.filters == ["count(*) > 1"]


class TestExecution:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_order_items_keep_the_row_order(self, make_store: StoreFactory, dialect: str) -> None:  # noqa: F811
        storage = await make_store(dialect=dialect)
        stored = stored_query(dimensions=["status"], measures=["sum(amount)"], order=[
            {"column": {"name": "amount_sum"}, "direction": "desc", "raw_formula": "amount:sum"},
            {"column": {"name": "_count"}, "raw_formula": "*:count"},
        ])
        resp = await run(storage, stored)
        assert rows(resp) == [{"orders.status": "paid", "orders.amount_sum": 80.0},
                              {"orders.status": "new", "orders.amount_sum": 70.0}]

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_granularity_spans(self, make_store: StoreFactory, dialect: str) -> None:  # noqa: F811
        storage = await make_store(dialect=dialect, granularities=[FISCAL_QUARTER])
        by_month = _stored(measures=["sum(amount)"], time_dimensions=[{"dimension": "created_at",
                                                                        "granularity": "month"}],
                           order=[{"column": {"name": "created_at_month"}, "direction": "desc",
                                   "raw_formula": "created_at:month"}])
        [order] = by_month.order or []
        assert order.raw_formula == "date_trunc('month', created_at)"
        assert rows(await run(storage, by_month)) == rows(await run(storage, _fresh(
            measures=["sum(amount)"], time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            order=[{"column": "date_trunc('month', created_at)", "direction": "desc"}])))
        by_fiscal = _stored(measures=["sum(amount)"], dimensions=[
            {"name": "created_at_fiscal_quarter", "expression": "created_at:fiscal_quarter"}])
        [dim] = by_fiscal.dimensions or []
        assert isinstance(dim, ComputedDimension)
        assert dim.expression == "fiscal_quarter(created_at)"
        with pytest.raises(GranularityCallError, match=re.escape("date_trunc('fiscal_quarter', created_at)")):
            await run(storage, by_fiscal)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_saved_query_and_extension_execute(self, make_store: StoreFactory,  # noqa: F811
                                                     backend: str) -> None:
        stage = stored_query(dimensions=["status"], measures=[{"name": "rev", "formula": "amount:sum"}])
        storage = await make_store(backend=backend, models=[
            orders_doc(), customers_doc(), regions_doc(), query_backed_doc(name="rev_q", stage=stage)])
        resp = await run(storage, {"source_model": "rev_q", "dimensions": ["status"], "measures": ["sum(rev)"]})
        assert {r["rev_q.status"]: r["rev_q.rev_sum"] for r in rows(resp)} == {"paid": 80.0, "new": 70.0}
        extended = stored_query(source_model={"source_name": "orders", "measures": [
            {"name": "m", "formula": "amount:sum"}]}, measures=["m"])
        assert rows(await run(storage, extended)) == [{"orders.m": 150.0}]


#: The stored measures and computed dimensions of the key scenario, with the keys ``main`` returned for them.
KEYED_DIMENSIONS = [
    {"name": "amount_sum_partition_by_region", "expression": "amount:sum(partition_by=region)"},
    {"name": "iif_amount_sum_partition_by_002eb04b_gh_other",
     "expression": "case when amount:sum(partition_by=customer_id) > 50 then 'high' else 'other' end"},
    {"name": "band", "expression": "amount:sum(partition_by=status)"},
]
KEYED_MEASURES = ["amount:sum / *:count", "amount:sum + price:sum + balance:sum + *:count + price:max + price:min"]
RECORDED_KEYS = [
    "orders.amount_sum_partition_by_region", "orders.iif_amount_sum_partition_by_002eb04b_gh_other", "orders.band",
    "orders.amount_sum_count", "orders.amount_sum_price_sum_balance_4ffaf4a2_rice_min",
]


class TestKeysAndNames:
    def test_names_kept_and_still_automatic(self) -> None:
        query = _stored(dimensions=KEYED_DIMENSIONS)
        dims = [d for d in query.dimensions or [] if isinstance(d, ComputedDimension)]
        assert [d.name for d in dims] == [d["name"] for d in KEYED_DIMENSIONS]
        assert [d.expression for d in dims] == [
            "sum(amount, partition_by=region)",
            "case when sum(amount, partition_by=customer_id) > 50 then 'high' else 'other' end",
            "sum(amount, partition_by=status)",
        ]
        assert [d.name == computed_dimension_name(d.template) for d in dims] == [True, True, False]

    def test_names_survive_a_dump(self) -> None:
        query = _stored(dimensions=KEYED_DIMENSIONS)
        reloaded = SlayerQuery.model_validate(query.model_dump(mode="json"))
        assert reloaded.model_dump() == query.model_dump()

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_result_keys_unchanged(self, make_store: StoreFactory, dialect: str) -> None:  # noqa: F811
        storage = await make_store(dialect=dialect)
        resp = await run(storage, stored_query(measures=KEYED_MEASURES, dimensions=KEYED_DIMENSIONS))
        assert resp.columns == RECORDED_KEYS

    async def test_collides_like_the_functional_spelling(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        stored = stored_query(measures=["sum(amount)"],
                              dimensions=[KEYED_DIMENSIONS[0], "sum(amount, partition_by=region)"])
        with pytest.raises(NameCollisionError):
            await run(storage, stored)


class TestMemories:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_loaded_twice_without_write_back(self, tmp_path: Any, make_store: StoreFactory,  # noqa: F811
                                                   backend: str) -> None:
        mem = memory_doc(memory_id="m_rev", query=stored_query(measures=[{"name": "rev", "formula": "amount:sum"}]))
        storage = await make_store(backend=backend, memories=[mem])
        before = raw_documents(backend=backend, base=store_base(tmp_path, backend=backend))["memories/m_rev"]
        for _ in range(2):
            loaded = await storage.get_memory("m_rev")
            assert loaded.query is not None
            assert [m.formula for m in loaded.query.measures or []] == ["sum(amount)"]
            assert rows(await run(storage, loaded.query)) == [{"orders.rev": 150.0}]
        assert raw_documents(backend=backend, base=store_base(tmp_path, backend=backend))["memories/m_rev"] == before


async def _renamed_store(*, tmp_path: Any, backend: str) -> Any:
    """A v1.4.1 store defining the granularity ``left``; a saved query and a memory bucket by ``created_at:left``."""
    base = str(tmp_path / f"renamed-{backend}")
    stage = stage_doc(dimensions=["created_at:left"], measures=[SUM])
    datasource = {**events_datasource_doc(db_path=live_db(base)), "granularities": [granularity_definition("left")]}
    return await write_raw_store(
        backend=backend, base=base, datasources=[datasource],
        models=[events_doc(), events_query_backed_doc(name="by_left", stage=stage)],
        memories=[events_memory_doc(memory_id="m_left", query=stage)],
    )


class TestGranularityRename:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_renamed_before_the_rewrite(self, tmp_path: Any, backend: str) -> None:
        storage = await _renamed_store(tmp_path=tmp_path, backend=backend)
        model = await storage.get_model("by_left", data_source=EVENTS_DS)
        assert model is not None
        [stage] = model.source_queries or []
        memory = await storage.get_memory("m_left")
        assert memory.query is not None
        fresh = SlayerQuery.model_validate({"source_model": "events",
                                            "dimensions": ["date_trunc('left_granularity', created_at)"],
                                            "measures": [SUM]})
        expected = rows(await run(storage, fresh))
        for loaded in (stage, memory.query):
            dumped = loaded.model_dump()
            assert (dumped["dimensions"], dumped["time_dimensions"]) == (
                fresh.model_dump()["dimensions"], fresh.model_dump()["time_dimensions"])
            assert rows(await run(storage, loaded)) == expected


async def test_stored_colon_call_to_a_shadowing_aggregation_answers() -> None:
    doc = t_doc(aggregations=[agg("reverse")], measures=[{"name": "c", "formula": "a:reverse"}])
    async with stored_engine(models=[doc]) as engine:
        model = await engine.storage.get_model("t", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == ["reverse(a)"]
        resp = await engine.execute({"source_model": "t", "measures": ["c"]})
    assert float(value(resp, name="c")) == SUM_A


@contextmanager
def _messages(caplog: pytest.LogCaptureFixture) -> Generator[list[str]]:
    out: list[str] = []
    with caplog.at_level(logging.WARNING), warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield out
    out += [str(w.message) for w in caught] + [r.getMessage() for r in caplog.records]


async def _assert_refused_unrewritten(storage: Any, payload: dict) -> None:
    """``payload`` keeps its colon text and fails with the colon-spelling error."""
    query = SlayerQuery.model_validate(payload)
    assert [m.formula for m in query.measures or []] == ["amount:sum"]
    engine = SlayerQueryEngine(storage=storage)
    try:
        with pytest.raises(ValueError) as ei:
            await engine.execute(query, dry_run=True)
    finally:
        engine.close()
    assert_colon_refused(ei.value, span="amount:sum", twin="sum(amount)")


class TestLeftAlone:
    def test_unparseable_text_is_kept_with_a_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with _messages(caplog) as messages:
            model = _model(measures=[{"name": "good", "formula": "amount:sum"},
                                     {"name": "broken", "formula": "amount:sum +"}])
        assert [m.formula for m in model.measures] == ["sum(amount)", "amount:sum +"]
        assert [m for m in messages if "orders" in m and "measures['broken'].formula" in m], messages

    def test_mode_a_text_is_not_rewritten(self) -> None:
        doc = orders_doc()
        doc["columns"] = [*doc["columns"], {"name": "p", "sql": "payload:sum", "type": "TEXT"}]
        model = SlayerModel.model_validate(doc)
        column = next(c for c in model.columns if c.name == "p")
        assert column.sql == "payload:sum"

    async def test_a_current_document_is_refused_not_rewritten(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        current = {**stored_query(measures=["amount:sum"]), "version": CURRENT_VERSIONS["SlayerQuery"]}
        await _assert_refused_unrewritten(storage, current)

    async def test_a_client_query_is_refused_not_rewritten(self, make_store: StoreFactory) -> None:  # noqa: F811
        storage = await make_store()
        await _assert_refused_unrewritten(storage, {"source_model": "orders", "measures": ["amount:sum"]})
