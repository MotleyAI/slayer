"""Windows under the two conformances: native evaluates them, ``ossie-l3`` keeps Ossie's two refusals.

Spec: queries/ossie-l3-binding › The filter is an Ossie expression (scenario "The native binding still refuses a
window in the filter"); › Model-scoped metrics are judged by their components.
Retail orders (order_id, customer_id, amount): O1 C1 100 | O2 C1 200 | O3 C2 50 | O4 C3 300.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import NATIVE, STRICT, fixture_texts, rows, run_as, sql_engine, strict_error
from tests.test_dev2217_filter import DATASETS, SQL

WINDOW_FILTER = "ROW_NUMBER() OVER (ORDER BY orders.amount) = 1"
AMOUNT_RANK = {"name": "amount_rank", "datatype": "Integer", "expression": {"sql": "RANK() OVER (ORDER BY amount)"}}
REVENUE_RANK = {"name": "revenue_rank", "datatype": "Integer",
                "expression": {"sql": "RANK() OVER (ORDER BY SUM(amount) DESC)"}}


def _retail(*, fields: tuple[dict[str, Any], ...] = (), metrics: tuple[dict[str, Any], ...] = ()) -> str:
    """The retail model, its dataset ``orders`` gaining ``fields`` and ``metrics``."""
    model = yaml.safe_load(fixture_texts("retail")[1])
    orders = next(d for d in model["datasets"] if d["name"] == "orders")
    orders["fields"].extend(fields)
    orders["metrics"].extend(metrics)
    return yaml.safe_dump(model, sort_keys=False)


@pytest.fixture
async def filter_engine(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with sql_engine(tmp_path, sql=SQL, datasets=DATASETS) as engine:
        yield engine


@pytest.fixture
async def retail(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with sql_engine(tmp_path, sql=fixture_texts("retail")[0], documents=[_retail()]) as engine:
        yield engine


@pytest.fixture
async def retail_ranked(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    document = _retail(fields=(AMOUNT_RANK,), metrics=(REVENUE_RANK,))
    async with sql_engine(tmp_path, sql=fixture_texts("retail")[0], documents=[document]) as engine:
        yield engine


async def test_native_evaluates_a_window_in_the_filter_before_and_after_strict(filter_engine: SlayerQueryEngine) -> None:
    """Scenario "The native binding evaluates a window in the filter"."""
    query = {"dimensions": ["orders.order_id"], "filter": WINDOW_FILTER}
    before = await filter_engine.execute(query)
    await strict_error(filter_engine, query, "E_WINDOW_IN_WHERE")
    after = await filter_engine.execute(query)
    assert rows(before) == [("O3",)]
    assert rows(after) == [("O3",)]


async def test_a_model_scoped_windowed_metric_stays_deferred_under_strict_and_answers_natively(
        retail: SlayerQueryEngine) -> None:
    """Scenario "A dataset-scoped windowed metric answers under strict", its ``running_revenue`` half."""
    query = {"metrics": ["running_revenue"]}
    await strict_error(retail, query, "E_DEFERRED_WINDOWED_METRIC")
    native = await retail.execute(query)
    assert rows(native) == [(650.0,)]
    await strict_error(retail, query, "E_DEFERRED_WINDOWED_METRIC")


@pytest.mark.parametrize("conformance", [STRICT, NATIVE])
async def test_a_windowed_field_is_evaluated_under_either_conformance(retail_ranked: SlayerQueryEngine,
                                                                       conformance: str) -> None:
    raw = await run_as(retail_ranked, {"dimensions": ["orders.order_id", "orders.amount_rank"]}, conformance)
    assert rows(raw) == [("O1", 2.0), ("O2", 3.0), ("O3", 1.0), ("O4", 4.0)]
    grouped = await run_as(retail_ranked, {"dimensions": ["orders.amount_rank"], "metrics": ["orders.total_revenue"]},
                           conformance)
    assert rows(grouped) == [(1.0, 50.0), (2.0, 100.0), (3.0, 200.0), (4.0, 300.0)]


@pytest.mark.parametrize("conformance", [STRICT, NATIVE])
async def test_a_dataset_scoped_windowed_metric_is_evaluated_under_either_conformance(
        retail_ranked: SlayerQueryEngine, conformance: str) -> None:
    query = {"dimensions": ["orders.customer_id"], "metrics": ["orders.revenue_rank"]}
    resp = await run_as(retail_ranked, query, conformance)
    assert rows(resp) == [("C1", 1.0), ("C2", 3.0), ("C3", 1.0)]
