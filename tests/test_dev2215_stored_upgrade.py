"""Stored bucket calls become ``date_trunc``, stored deprecated spellings their canonical twins, and stored ``agg``
aggregations ``agg_aggregation`` (spec: models/stored-upgrade)."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.database import engine_factory
from slayer.memories.models import Memory
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._dev2215_fixtures import future_warnings, recorded_warnings
from tests._stored_upgrade_fixtures import BACKENDS, DS, memory_v2, raw_store, run, seed_shop

#: The versions every document written before this change carries.
PRE = {"SlayerModel": 19, "SlayerQuery": 10, "Memory": 8}
FISCAL_YEAR = {"name": "fiscal_year", "base": "year", "origin": "2000-04-01"}
_COLUMNS = [
    {"name": "order_id", "type": "INT", "primary_key": True},
    {"name": "customer_id", "type": "INT"},
    {"name": "status", "type": "TEXT"},
    {"name": "ordered_at", "type": "TIMESTAMP"},
    {"name": "amount", "type": "DOUBLE"},
]
_CUSTOMERS = {"version": PRE["SlayerModel"], "name": "customers", "sql_table": "customers", "data_source": DS,
              "columns": [{"name": "id", "type": "INT", "primary_key": True}, {"name": "name", "type": "TEXT"},
                          {"name": "region", "type": "TEXT"}, {"name": "signed_up_at", "type": "TIMESTAMP"}]}
#: sum(amount) per month of the 2024+ orders of the shop seed.
MONTHLY_2024_ON = {"2024-01": 50.0, "2024-06": 40.0, "2024-07": 60.0, "2025-01": 50.0}
#: sum(amount) per April-origin fiscal year.
FISCAL = {"2023-04-01": 60.0, "2024-04-01": 150.0}
#: A month / year call outside quoting or ``date_trunc``.
_BUCKET_CALL = re.compile(r"(?<![\w'])(month|year|week_sunday|fiscal_year)\(")


def orders_doc(**extra: Any) -> dict:
    return {"version": PRE["SlayerModel"], "name": "orders", "sql_table": "orders", "data_source": DS,
            "columns": _COLUMNS,
            "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]],
                       "cardinality": "many_to_one"}], **extra}


def saved_query_doc(name: str, *stages: dict) -> dict:
    return {"version": PRE["SlayerModel"], "name": name, "data_source": DS,
            "source_queries": [{"version": PRE["SlayerQuery"], **s} for s in stages]}


def dumped(obj: Any) -> str:
    return json.dumps(obj.model_dump(mode="json"), default=str)


@pytest.fixture
def shop_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db, dialect="sqlite")
    ds = DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "database": db,
                                          "granularities": [FISCAL_YEAR]})
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def store(tmp_path: Path, ds: DatasourceConfig, *, backend: str, models: list[dict],
                memories: tuple[dict, ...] = ()) -> Any:
    return await raw_store(backend=backend, base=str(tmp_path / f"store-{backend}"), datasource=ds,
                           models=[_CUSTOMERS, *models], memories=memories)


@contextmanager
def logged(caplog: pytest.LogCaptureFixture) -> Generator[list[str]]:
    texts: list[str] = []
    with caplog.at_level(logging.WARNING):
        yield texts
    texts.extend(r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)


def by_month(resp: Any, *, key: str, value: str) -> dict[str, float]:
    return {str(r[key])[:7]: float(r[value]) for r in resp.data}


def test_versions_bumped() -> None:
    assert (CURRENT_VERSIONS["SlayerModel"], CURRENT_VERSIONS["SlayerQuery"], CURRENT_VERSIONS["Memory"]) == (
        PRE["SlayerModel"] + 1, PRE["SlayerQuery"] + 1, PRE["Memory"] + 1)


# --------------------------------------------------------------------------- #
# Stored bucket calls are upgraded to date_trunc
# --------------------------------------------------------------------------- #
MONTHLY_STAGE = {"source_model": "orders", "dimensions": ["month(ordered_at)"],
                 "measures": [{"formula": "sum(amount)", "name": "rev"}],
                 "filters": ["year(ordered_at) >= '2024'"],
                 "order": [{"column": "month(ordered_at)", "direction": "asc"}]}


class TestBucketCalls:
    def test_query_fields_are_rewritten(self) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], **MONTHLY_STAGE,
                                            "main_time_dimension": "month(ordered_at)"})
        text = dumped(query)
        assert "date_trunc('year', ordered_at) >= '2024'" in (query.filters or [])
        assert "date_trunc('month', ordered_at)" in text
        assert not _BUCKET_CALL.search(text), text

    @pytest.mark.parametrize("unit", ["second", "minute", "hour", "day", "week", "month", "quarter", "year"])
    def test_every_built_in_unit(self, unit: str) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": "orders",
                                            "measures": [f"count_distinct({unit}(ordered_at))"]})
        assert [m.formula for m in query.measures or []] == [f"count_distinct(date_trunc('{unit}', ordered_at))"]

    def test_week_sunday(self) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": "orders",
                                            "filters": ["week_sunday(ordered_at) = '2024-01-07'"]})
        assert query.filters == ["date_trunc('week_sunday', ordered_at) = '2024-01-07'"]

    def test_saved_measure_formula(self) -> None:
        model = SlayerModel.model_validate(orders_doc(measures=[
            {"name": "months", "formula": "count_distinct(month(ordered_at))"}]))
        assert [m.formula for m in model.measures] == ["count_distinct(date_trunc('month', ordered_at))"]

    def test_rest_of_the_text_is_byte_identical(self) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": "orders",
                                            "filters": ["month(ordered_at)>='2024-03'  AND status = 'ok'"]})
        assert query.filters == ["date_trunc('month', ordered_at)>='2024-03'  AND status = 'ok'"]

    def test_quoted_text_is_untouched(self) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": "orders",
                                            "filters": ["status = 'month(x)'"]})
        assert query.filters == ["status = 'month(x)'"]

    def test_a_current_document_is_not_rewritten(self) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"] + 1, "source_model": "orders",
                                            "dimensions": [{"expression": "month(ordered_at)", "name": "m"}]})
        assert "month(ordered_at)" in dumped(query)
        assert "date_trunc" not in dumped(query)

    def test_a_client_query_is_not_rewritten(self) -> None:
        query = SlayerQuery.model_validate({"source_model": "orders",
                                            "dimensions": [{"expression": "month(ordered_at)", "name": "m"}]})
        assert "date_trunc" not in dumped(query)

    def test_custom_aggregation_is_not_a_bucket(self) -> None:
        model = SlayerModel.model_validate(orders_doc(
            aggregations=[{"name": "fiscal_total", "formula": "SUM({value})"}],
            measures=[{"name": "ft", "formula": "fiscal_total(amount)"}]))
        assert [m.formula for m in model.measures] == ["fiscal_total(amount)"]

    def test_mode_a_text_is_untouched(self) -> None:
        columns = [*_COLUMNS, {"name": "mo", "type": "INT", "sql": "month(ordered_at)"}]
        model = SlayerModel.model_validate(orders_doc(columns=columns, filters=["year(ordered_at) > 2020"],
                                                      aggregations=[{"name": "yr", "formula": "MAX(year({value}))"}]))
        assert next(c for c in model.columns if c.name == "mo").sql == "month(ordered_at)"
        assert model.filters == ["year(ordered_at) > 2020"]
        assert next(a for a in model.aggregations if a.name == "yr").formula == "MAX(year({value}))"

    def test_structured_time_dimensions_are_untouched(self) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": "orders",
                                            "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
                                            "measures": ["cumsum(sum(amount))"]})
        assert [(str(t.dimension), getattr(t.granularity, "value", t.granularity))
                for t in query.time_dimensions or []] == [("ordered_at", "month")]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_saved_query_returns_its_rows_and_keys(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                           backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              models=[orders_doc(), saved_query_doc("monthly_rev", MONTHLY_STAGE)])
        model = await storage.get_model("monthly_rev", data_source=DS)
        assert model is not None
        text = dumped(model)
        assert "date_trunc('year', ordered_at) >= '2024'" in text
        assert not _BUCKET_CALL.search(text), text
        resp = await run(storage, {"source_model": "monthly_rev", "dimensions": ["ordered_at"],
                                   "measures": [{"formula": "sum(rev)", "name": "r"}]})
        assert by_month(resp, key="monthly_rev.ordered_at", value="monthly_rev.r") == MONTHLY_2024_ON

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_custom_granularity_in_a_memory(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                  backend: str) -> None:
        query = {"version": PRE["SlayerQuery"], "source_model": "orders", "dimensions": ["fiscal_year(ordered_at)"],
                 "measures": [{"formula": "sum(amount)", "name": "s"}]}
        storage = await store(tmp_path, shop_ds, backend=backend, models=[orders_doc()],
                              memories=(memory_v2(memory_id="m_fy", version=PRE["Memory"], query=query),))
        loads = [await storage.get_memory("m_fy") for _ in range(2)]  # idempotent across loads
        for mem in loads:
            assert mem.query is not None
            assert [(str(t.dimension), str(getattr(t.granularity, "value", t.granularity)))
                    for t in mem.query.time_dimensions or []] == [("ordered_at", "fiscal_year")]
        assert loads[1].query is not None
        resp = await run(storage, loads[1].query)
        assert {str(r["orders.ordered_at"])[:10]: float(r["orders.s"]) for r in resp.data} == FISCAL

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_custom_granularity_in_a_saved_measure(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                         backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, models=[orders_doc(measures=[
            {"name": "fys", "formula": "count_distinct(fiscal_year(ordered_at))"}])])
        model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == ["count_distinct(date_trunc('fiscal_year', ordered_at))"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_stored_colon_custom_granularity_becomes_date_trunc(
        self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
    ) -> None:
        doc = orders_doc(version=PRE["SlayerModel"] - 1, measures=[
            {"name": "fys", "formula": "count_distinct(ordered_at:fiscal_year)"}])
        storage = await store(tmp_path, shop_ds, backend=backend, models=[doc])
        model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == ["count_distinct(date_trunc('fiscal_year', ordered_at))"]

    def test_inline_source_model_and_extension_are_rewritten(self) -> None:
        inline = {"name": "orders_x", "sql_table": "orders", "data_source": DS, "columns": _COLUMNS,
                  "measures": [{"name": "months", "formula": "count_distinct(month(ordered_at))"}]}
        extension = {"source_name": "orders",
                     "measures": [{"name": "ok", "formula": "sum(iif(status == 'ok', amount, None))"}]}
        for source in (inline, extension):
            query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": source})
            text = dumped(query)
            assert not _BUCKET_CALL.search(text), text
            assert "==" not in text, text
            assert "None)" not in text, text
        assert "count_distinct(date_trunc('month', ordered_at))" in dumped(
            SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": inline}))

    def test_nested_saved_query_stages_are_rewritten(self) -> None:
        model = SlayerModel.model_validate(saved_query_doc(
            "two_stage",
            {"name": "inner", "source_model": "orders", "measures": [{"formula": "sum(amount)", "name": "rev"}],
             "filters": ["year(ordered_at) >= '2024' and status == 'ok'"]},
            {"source_model": "inner", "measures": [{"formula": "sum(rev)", "name": "t"}]}))
        stage = (model.source_queries or [])[0]
        assert stage.filters == ["date_trunc('year', ordered_at) >= '2024' and status = 'ok'"]

    def test_already_converted_text_is_left_alone(self) -> None:
        query = SlayerQuery.model_validate({"version": PRE["SlayerQuery"], "source_model": "orders",
                                            "filters": ["date_trunc('month', ordered_at) = '2024-01' AND x IS NULL"]})
        assert query.filters == ["date_trunc('month', ordered_at) = '2024-01' AND x IS NULL"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_an_interrupted_load_completes(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                 backend: str) -> None:
        converted = {"version": PRE["SlayerQuery"], "source_model": "orders",
                     "dimensions": ["date_trunc('fiscal_year', ordered_at)"],
                     "measures": [{"formula": "sum(amount)", "name": "s"}]}
        pending = {**converted, "dimensions": ["fiscal_year(ordered_at)"]}
        storage = await store(tmp_path, shop_ds, backend=backend, models=[orders_doc()], memories=(
            memory_v2(memory_id="m_done", version=PRE["Memory"], query=converted),
            memory_v2(memory_id="m_pending", version=PRE["Memory"], query=pending)))
        for memory_id in ("m_done", "m_pending"):
            mem = await storage.get_memory(memory_id)
            assert mem.query is not None
            resp = await run(storage, mem.query)
            assert {str(r["orders.ordered_at"])[:10]: float(r["orders.s"]) for r in resp.data} == FISCAL

    def test_memory_query_is_rewritten(self) -> None:
        query = {"version": PRE["SlayerQuery"], "source_model": "orders", "filters": ["month(ordered_at) = '2024-01'"]}
        mem = Memory.model_validate(memory_v2(memory_id="m", version=PRE["Memory"], query=query))
        assert mem.query is not None
        assert mem.query.filters == ["date_trunc('month', ordered_at) = '2024-01'"]


# --------------------------------------------------------------------------- #
# Stored deprecated spellings are upgraded
# --------------------------------------------------------------------------- #
class TestDeprecatedSpellings:
    def test_python_spellings_in_a_saved_measure(self) -> None:
        with recorded_warnings() as caught:
            model = SlayerModel.model_validate(orders_doc(measures=[{
                "name": "ok", "formula": "sum(iif(status == 'ok' and customer_id is not None, amount, None))"}]))
        assert [m.formula for m in model.measures] == [
            "sum(iif(status = 'ok' and customer_id IS NOT NULL, amount, NULL))"]
        assert future_warnings(caught) == []

    def test_method_call_and_list_after_in_in_a_saved_query(self) -> None:
        model = SlayerModel.model_validate(saved_query_doc("q", {
            "source_model": "orders", "measures": [{"formula": "customers.count(*)", "name": "n"}],
            "filters": ["status in ['ok', 'new']", "customer_id is None or status == 'ok'"]}))
        stage = (model.source_queries or [])[0]
        assert [m.formula for m in stage.measures or []] == ["count(customers.*)"]
        assert stage.filters == ["status in ('ok', 'new')", "customer_id IS NULL or status = 'ok'"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_the_same_value(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, models=[orders_doc(measures=[{
            "name": "ok", "formula": "sum(iif(status == 'ok' and customer_id is not None, amount, None))"}])])
        with recorded_warnings() as caught:
            resp = await run(storage, {"source_model": "orders", "measures": ["ok"]})
        assert float(resp.data[0]["orders.ok"]) == 80.0
        assert future_warnings(caught) == []


# --------------------------------------------------------------------------- #
# Stored agg aggregations are renamed
# --------------------------------------------------------------------------- #
class TestAggRename:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_aggregation_named_agg(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                         caplog: pytest.LogCaptureFixture) -> None:
        doubled = orders_doc(aggregations=[{"name": "agg", "formula": "SUM({value}) * 2"}])
        saved = saved_query_doc("doubled", {"source_model": "orders",
                                            "measures": [{"formula": "agg(amount)", "name": "d"}]})
        storage = await store(tmp_path, shop_ds, backend=backend, models=[doubled, saved])
        with logged(caplog) as texts:
            model = await storage.get_model("orders", data_source=DS)
            query_backed = await storage.get_model("doubled", data_source=DS)
        assert model is not None
        assert query_backed is not None
        assert [a.name for a in model.aggregations] == ["agg_aggregation"]
        assert "agg_aggregation(amount)" in dumped(query_backed)
        assert any("agg_aggregation" in t and "orders" in t for t in texts), texts
        resp = await run(storage, {"source_model": "doubled", "measures": [{"formula": "sum(d)", "name": "t"}]})
        assert float(resp.data[0]["doubled.t"]) == 420.0

    def test_any_case(self) -> None:
        model = SlayerModel.model_validate(orders_doc(aggregations=[{"name": "AGG", "formula": "SUM({value})"}],
                                                      measures=[{"name": "x", "formula": "Agg(amount)"}]))
        assert [a.name for a in model.aggregations] == ["agg_aggregation"]
        assert [m.formula for m in model.measures] == ["agg_aggregation(amount)"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_the_new_name_is_taken(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                         caplog: pytest.LogCaptureFixture) -> None:
        both = orders_doc(aggregations=[{"name": "agg", "formula": "SUM({value})"},
                                        {"name": "agg_aggregation", "formula": "MAX({value})"}],
                          measures=[{"name": "a", "formula": "agg(amount)"}])
        storage = await store(tmp_path, shop_ds, backend=backend, models=[both])
        with logged(caplog) as texts:
            model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        assert sorted(a.name for a in model.aggregations) == ["agg", "agg_aggregation"]
        assert [m.formula for m in model.measures] == ["agg(amount)"]
        assert any("agg" in t and "orders" in t for t in texts), texts
        with pytest.raises(SlayerError, match="(?i)AGG.*reserved"):
            await run(storage, {"source_model": "orders", "measures": ["agg(amount)"]})
