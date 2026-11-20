"""Ranked and windowed association render on every Tier-1 dialect and execute on Postgres (spec:
queries/attribution-modes › Ranked and windowed association — The windowed association renders on every dialect)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pytest_postgresql import factories

from slayer.core.query import SlayerQuery
from tests._dev1840_fixtures import gen
from tests._dev1914_fixtures import (
    LAST_BY_STATUS,
    ORDERED_MONTH,
    WINDOW_BY_ORDER_MONTH,
    by_month,
    by_status,
    dev1840_datasets,
    metric,
    postgres_statements,
    query,
)
from tests._dev2120_fixtures import TIER1
from tests._pg_helpers import pg_database, pg_query_engine

postgresql_proc = factories.postgresql_proc(port=None)

#: shape → (query, reader, oracle).
SHAPES = {
    "ranked": (query("associate", population="orders", dimensions=["orders.status"],
                     metrics=[metric("last(customers.spend, customers.signup_at)", "l")]),
               lambda resp: by_status(resp, "l"), LAST_BY_STATUS),
    "windowed": (query("associate", population="customers", time_dimensions=ORDERED_MONTH,
                       metrics=[metric("sum(customers.spend, window='1y')", "w")]),
                 lambda resp: by_month(resp, "w"), {**WINDOW_BY_ORDER_MONTH, None: None}),
}


def _datasets(data_source: str = "test"):
    datasets = dev1840_datasets()
    for ds in datasets:
        ds.data_source = data_source
    return datasets


@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("dialect", TIER1)
async def test_renders(dialect: str, shape: str) -> None:
    q: SlayerQuery = SHAPES[shape][0]
    assert await gen(q, dialect=dialect, datasets=_datasets())


@pytest.mark.parametrize("shape", SHAPES)
async def test_executes_on_postgres(postgresql_proc: Any, tmp_path: Path, shape: str) -> None:
    q, read, expected = SHAPES[shape]
    with pg_database(postgresql_proc, statements=postgres_statements(), name="pg") as ds:
        async with pg_query_engine(ds, base_dir=str(tmp_path), datasets=_datasets("pg"), validate=False) as engine:
            assert read(await engine.execute(q)) == expected
