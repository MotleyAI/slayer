"""Server-backend seed, datasets and checks for the time spine and custom granularities."""

from __future__ import annotations

from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1737_fixtures import TableSpec, server_table_statements
from tests._dev2015_fixtures import (
    CG_ORDERS,
    CUSTOMERS,
    DS_GRANULARITIES,
    EVENTS,
    ORDER_ITEMS,
    RETURNS,
    TWO_FACTS,
    TWO_FACT_ROWS,
    bucket_key,
    by_bucket,
    cg_orders_dataset,
    events_dataset,
    m,
    orders_table,
    spine_datasets,
    spine_query,
    value,
)

def _tables() -> list[TableSpec]:
    orders_ts = orders_table(
        [(i, c, f"{d} 09:30:00", a) for i, c, d, a in orders_table().rows], date_type="TIMESTAMP",
    ).model_copy(update={"name": "orders_ts"})
    return [
        CUSTOMERS, orders_table(), orders_ts, RETURNS, ORDER_ITEMS, EVENTS,
        CG_ORDERS.model_copy(update={"name": "fy_orders"}),
    ]


def server_statements(backend: str) -> list[str]:
    out: list[str] = []
    for table in _tables():
        out += server_table_statements(table, backend=backend)
    return out


def with_granularities(cfg: DatasourceConfig, *, name: str) -> DatasourceConfig:
    return DatasourceConfig.model_validate({**cfg.model_dump(), "name": name, **DS_GRANULARITIES})


def _on(dataset: SlayerDataset, *, data_source: str, **update: Any) -> SlayerDataset:
    return dataset.model_copy(update={"data_source": data_source, **update})


def server_datasets(*, data_source: str) -> list[SlayerDataset]:
    """DATE axes: the spine datasets, ``events`` and ``fy_orders``."""
    return [
        *(_on(x, data_source=data_source) for x in spine_datasets()),
        _on(events_dataset(), data_source=data_source),
        _on(cg_orders_dataset(), data_source=data_source, name="fy_orders", sql_table="fy_orders"),
    ]


def server_datasets_ts(*, data_source: str) -> list[SlayerDataset]:
    """``orders`` over the TIMESTAMP-axis ``orders_ts`` table, plus ``customers`` and ``returns``."""
    models = spine_datasets(date_type=DataType.DATETIME)
    return [
        _on(x, data_source=data_source, **({"sql_table": "orders_ts"} if x.name == "orders" else {}))
        for x in models if x.name in {"customers", "orders", "returns"}
    ]


async def check_spine(engine: SlayerQueryEngine, *, data_source: str) -> None:
    resp = await engine.execute(spine_query(metrics=TWO_FACTS), data_source=data_source)
    assert by_bucket(resp, ["o", "r", "n"]) == TWO_FACT_ROWS


async def check_quarter_hour_day(engine: SlayerQueryEngine, *, data_source: str) -> None:
    resp = await engine.execute(spine_query(
        metrics=[m("count(events.id)", "n")], granularity="quarter_hour", date_range=["2025-06-02", "2025-06-02"],
    ), data_source=data_source)
    got = {k: v[0] for k, v in by_bucket(resp, ["n"], width=16).items()}
    assert len(got) == 96
    assert {k: v for k, v in got.items() if v} == {
        "2025-06-02 10:00": 2.0, "2025-06-02 10:15": 1.0, "2025-06-02 10:30": 1.0,
    }


async def check_quarter_hour_year(engine: SlayerQueryEngine, *, data_source: str) -> None:
    resp = await engine.execute(spine_query(
        metrics=[m("count(orders.id)", "n")], granularity="quarter_hour", date_range=["2025-01-01", "2025-12-31"],
    ), data_source=data_source)
    assert len(resp.data) == 35_040


async def _buckets(engine: SlayerQueryEngine, *, data_source: str, dataset: str, column: str, granularity: str,
                   series: str | None = None, width: int = 10) -> dict[str, float]:
    resp = await engine.execute(SlayerQuery.model_validate({
        "population": dataset, "metrics": [m("sum(amount)", "s")],
        "time_dimensions": [{"field": column, "granularity": granularity}],
        "filter": [f"series = '{series}'"] if series else [],
    }), data_source=data_source)
    return {bucket_key(value(r, column), width=width): float(value(r, "s")) for r in resp.data}


async def check_custom_buckets(engine: SlayerQueryEngine, *, data_source: str) -> None:
    kw = {"engine": engine, "data_source": data_source}
    assert await _buckets(**kw, dataset="fy_orders", column="order_date", granularity="fiscal_year") == {
        "2023-04-01": 10.0, "2024-04-01": 50.0, "2025-04-01": 40.0,
    }
    for column in ("ts", "d"):
        assert await _buckets(**kw, dataset="events", column=column, granularity="billing_month", series="bill") == {
            "2025-02-15": 1.0, "2025-03-15": 1.0,
        }
        assert await _buckets(**kw, dataset="events", column=column, granularity="sprint", series="sprint") == {
            "2024-12-23": 1.0, "2025-01-06": 1.0, "2025-01-20": 1.0,
        }
    assert await _buckets(**kw, dataset="events", column="ts", granularity="quarter_hour", series="qh", width=16) == {
        "2025-06-02 10:00": 3.0, "2025-06-02 10:15": 4.0, "2025-06-02 10:30": 8.0,
    }


async def check_all(engine: SlayerQueryEngine, *, data_source: str, ts_data_source: str) -> None:
    """Every server scenario: two facts on DATE and TIMESTAMP axes, the quarter-hour day, custom buckets."""
    await check_spine(engine, data_source=data_source)
    await check_spine(engine, data_source=ts_data_source)
    await check_quarter_hour_day(engine, data_source=data_source)
    await check_custom_buckets(engine, data_source=data_source)
