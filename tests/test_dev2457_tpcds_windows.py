"""Spec: importers/osi-import › The TPC-DS window metrics evaluate.

Upstream's ``tpcds_semantic_model.yaml`` imported over a hand-made DuckDB catalog ``tpcds`` (schema ``public``):
date_dim (sk, date, year, moy): 1 2024-01-10 2024 1 | 2 2024-01-20 2024 1 | 3 2024-02-05 2024 2 | 4 2023-12-15 2023 12
item (sk, brand): 1 A | 2 B | 3 C | 4 A          store (sk): 1 | 2
store_sales (date, item, ticket, store, ext_price): 1 1 1 1 10 | 1 2 1 1 25 | 2 3 2 1 7 | 2 4 2 1 30 | 3 1 3 2 5 |
  3 2 3 2 50 | 4 3 4 2 20 | 4 2 5 1 3
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.core.errors import IllegalWindowInFilterError, QueryTypeError, UnevaluableEntityError
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2243_fixtures import CORPUS, DS, importer_module, make_storage
from tests._engine_helpers import owned_query_engine

TPCDS = CORPUS / "examples" / "tpcds_semantic_model.yaml"

DDL = (
    "CREATE SCHEMA public",
    "CREATE TABLE public.date_dim (d_date_sk INTEGER, d_date DATE, d_year INTEGER, d_moy INTEGER)",
    "INSERT INTO public.date_dim VALUES (1, '2024-01-10', 2024, 1), (2, '2024-01-20', 2024, 1), "
    "(3, '2024-02-05', 2024, 2), (4, '2023-12-15', 2023, 12)",
    "CREATE TABLE public.item (i_item_sk INTEGER, i_brand VARCHAR)",
    "INSERT INTO public.item VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'A')",
    "CREATE TABLE public.store (s_store_sk INTEGER)",
    "INSERT INTO public.store VALUES (1), (2)",
    "CREATE TABLE public.store_sales (ss_sold_date_sk INTEGER, ss_item_sk INTEGER, ss_ticket_number INTEGER, "
    "ss_store_sk INTEGER, ss_ext_sales_price DECIMAL(7, 2))",
    "INSERT INTO public.store_sales VALUES (1, 1, 1, 1, 10), (1, 2, 1, 1, 25), (2, 3, 2, 1, 7), (2, 4, 2, 1, 30), "
    "(3, 1, 3, 2, 5), (3, 2, 3, 2, 50), (4, 3, 4, 2, 20), (4, 2, 5, 1, 3)",
)

BY_DATE = ["date_dim.d_date"]
BY_STORE_BRAND = ["store.s_store_sk", "item.i_brand"]
BY_MONTH = ["date_dim.d_year", "date_dim.d_moy"]


@contextlib.asynccontextmanager
async def tpcds_engine(base: Path) -> AsyncGenerator[SlayerQueryEngine]:
    """The TPC-DS document imported into ``shop``, over the fixture; the file name makes the catalog ``tpcds``."""
    db = base / "tpcds.duckdb"
    with contextlib.closing(duckdb.connect(str(db))) as con:
        for statement in DDL:
            con.execute(statement)
    storage = make_storage("yaml", base)
    async with owned_query_engine(storage=storage,
                                  datasources=[DatasourceConfig(name=DS, type="duckdb", database=str(db))]) as engine:
        result = await importer_module().import_osi(storage=storage, path=TPCDS, data_source=DS)
        assert result.stored == ["tpcds_retail_model"]
        yield engine


async def cells(engine: SlayerQueryEngine, *, metric: str, dimensions: list[str]) -> dict[tuple[str, ...], Any]:
    """``metric`` per dimension tuple (each key value as text, a date as its day)."""
    resp = await engine.execute({"dimensions": dimensions, "metrics": [metric]})
    got = {tuple(str(r[d])[:10] for d in dimensions): (None if r[metric] is None else float(r[metric]))
           for r in resp.data}
    assert len(got) == len(resp.data), resp.data
    return got


async def test_the_fixture_answers_a_plain_model_scoped_metric(tmp_path: Path) -> None:
    """Guard: the non-window ``total_sales`` answers every grouping the window metrics use."""
    async with tpcds_engine(tmp_path) as engine:
        assert await cells(engine, metric="total_sales", dimensions=BY_DATE) == {
            ("2023-12-15",): 23.0, ("2024-01-10",): 35.0, ("2024-01-20",): 37.0, ("2024-02-05",): 55.0}
        assert await cells(engine, metric="total_sales", dimensions=BY_STORE_BRAND) == {
            ("1", "A"): 40.0, ("1", "B"): 28.0, ("1", "C"): 7.0, ("2", "A"): 5.0, ("2", "B"): 50.0, ("2", "C"): 20.0}
        assert await cells(engine, metric="total_sales", dimensions=BY_MONTH) == {
            ("2023", "12"): 23.0, ("2024", "1"): 72.0, ("2024", "2"): 55.0}


async def test_the_tpcds_window_metrics_evaluate(tmp_path: Path) -> None:
    """Spec scenario: The TPC-DS window metrics evaluate."""
    async with tpcds_engine(tmp_path) as engine:
        assert await cells(engine, metric="cumulative_sales", dimensions=BY_DATE) == {
            ("2023-12-15",): 23.0, ("2024-01-10",): 58.0, ("2024-01-20",): 95.0, ("2024-02-05",): 150.0}
        assert await cells(engine, metric="brand_rank_in_store", dimensions=BY_STORE_BRAND) == {
            ("1", "A"): 1.0, ("1", "B"): 2.0, ("1", "C"): 3.0, ("2", "A"): 3.0, ("2", "B"): 1.0, ("2", "C"): 2.0}
        assert await cells(engine, metric="monthly_sales_change", dimensions=BY_MONTH) == {
            ("2023", "12"): None, ("2024", "1"): 49.0, ("2024", "2"): -17.0}


@pytest.mark.parametrize(("metric", "dimensions", "missing"), [
    pytest.param("cumulative_sales", ["date_dim.d_year"], r"date_dim\.d_date(?!_)", id="cumulative_sales"),
    pytest.param("brand_rank_in_store", ["item.i_brand"], r"store\.s_store_sk", id="brand_rank_in_store"),
    pytest.param("monthly_sales_change", ["date_dim.d_year"], r"date_dim\.d_moy", id="monthly_sales_change"),
])
async def test_a_tpcds_window_metric_without_its_grouping_names_the_missing_key(
        tmp_path: Path, metric: str, dimensions: list[str], missing: str) -> None:
    """Spec scenario: The TPC-DS window metrics evaluate (queried without the grouping its window names)."""
    async with tpcds_engine(tmp_path) as engine:
        with pytest.raises(QueryTypeError, match=missing) as caught:
            await engine.execute({"dimensions": dimensions, "metrics": [metric]})
    assert not isinstance(caught.value, IllegalWindowInFilterError | UnevaluableEntityError)
