"""Engines for the strict filter's Ossie Layer 1 admission tests.

orders (order_id, status, amount, order_date): O1 complete 100 2025-01-15 | O2 cancelled 200 2025-02-10 |
O3 complete 50 2025-04-01 | O4 NULL 300 2024-12-31; metric ``total_revenue`` = ``sum(amount)``.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2215_fixtures import dev2215_datasets, dev2215_engine
from tests._dev2217_fixtures import DS, sql_engine

SQL = """
CREATE TABLE orders (order_id VARCHAR PRIMARY KEY, status VARCHAR, amount INTEGER, order_date DATE);
INSERT INTO orders VALUES
  ('O1', 'complete', 100, DATE '2025-01-15'),
  ('O2', 'cancelled', 200, DATE '2025-02-10'),
  ('O3', 'complete', 50, DATE '2025-04-01'),
  ('O4', NULL, 300, DATE '2024-12-31');
"""


def _field(name: str, datatype: DataType, **extra: object) -> DatasetField:
    return DatasetField.model_validate({"name": name, "datatype": datatype, **extra})


ORDERS = SlayerDataset(
    name="orders", sql_table="orders", data_source=DS,
    fields=[_field("order_id", DataType.STRING, primary_key=True), _field("status", DataType.STRING),
            _field("amount", DataType.INTEGER), _field("order_date", DataType.DATE)],
    metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)")],
)


@contextlib.asynccontextmanager
async def orders_engine(base: Path) -> AsyncGenerator[SlayerQueryEngine]:
    """DuckDB engine over ``SQL``."""
    async with sql_engine(base, sql=SQL, datasets=[ORDERS]) as engine:
        yield engine


def revenue_query(filter_text: str) -> dict[str, Any]:
    return {"metrics": ["orders.total_revenue"], "filter": filter_text}


@contextlib.asynccontextmanager
async def counting_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    """The ``tests._dev2215_fixtures`` seed with metric ``orders.n`` = ``count(*)``."""
    datasets = dev2215_datasets()
    datasets[0] = datasets[0].model_copy(update={"metrics": [DatasetMetric(name="n", expression="count(*)")]})
    async with dev2215_engine(backend, datasets=datasets) as engine:
        yield engine
