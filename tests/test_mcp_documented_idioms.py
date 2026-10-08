"""Every query idiom the MCP surface documents runs and returns the documented result."""

from __future__ import annotations

from typing import Any, AsyncIterator

import pytest

from slayer.core.errors import AssociatedGrainWarning, BroadcastGrainWarning
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests import _mcp_idiom_fixtures as fx

_YEAR_2025 = [{"dimension": "order_date", "granularity": "month", "date_range": "2025"}]


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    async for e in fx.make_idiom_engine():
        yield e


async def _run(engine: SlayerQueryEngine, query: dict[str, Any] | list[dict[str, Any]]) -> SlayerResponse:
    if isinstance(query, list):
        return await engine.execute(query=[SlayerQuery.model_validate(q) for q in query])
    return await engine.execute(query=SlayerQuery.model_validate(query))


def _kinds(resp: SlayerResponse) -> set[str]:
    return {w.kind for w in resp.warnings}


async def test_change_change_pct_and_time_shift_look_back_before_date_range(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "time_dimensions": _YEAR_2025,
        "measures": [
            {"formula": fx.CHANGE, "name": "chg"},
            {"formula": fx.CHANGE_PCT, "name": "chg_pct"},
            {"formula": fx.TIME_SHIFT, "name": "prev"},
        ],
    })
    jan = fx.rows_by(resp, key="order_date")["2025-01"]
    assert fx.value(jan, ".chg") == 105
    assert fx.value(jan, ".chg_pct") == pytest.approx(105 / 45)
    assert fx.value(jan, ".prev") == 45


async def test_trailing_window_reads_rows_before_date_range(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "time_dimensions": _YEAR_2025,
        "measures": [{"formula": fx.TRAILING_WINDOW, "name": "w"}, {"formula": "count_distinct(customer_id)", "name": "nw"}],
    })
    jan = fx.rows_by(resp, key="order_date")["2025-01"]
    assert fx.value(jan, ".w") == 3
    assert fx.value(jan, ".nw") == 2


async def test_cumsum_starts_at_the_range_start(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "time_dimensions": _YEAR_2025,
        "measures": [{"formula": fx.CUMSUM, "name": "run"}],
    })
    by_month = fx.rows_by(resp, key="order_date")
    assert fx.value(by_month["2025-01"], ".run") == 150
    assert fx.value(by_month["2025-02"], ".run") == 220


async def test_anti_join_keeps_rows_without_a_joined_row(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {"source_model": "customers", "dimensions": ["name"], "filters": [fx.ANTI_JOIN]})
    assert {fx.value(r, ".name") for r in resp.data} == {"Dan", "Eve"}


async def test_root_model_keeps_every_row(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "regions", "dimensions": ["name"],
        "measures": [
            {"formula": "count(customers.id)", "name": "n_customers"},
            {"formula": fx.POPULATION_COUNT, "name": "n_orders"},
            {"formula": fx.CONDITIONAL, "name": "orders_2025"},
        ],
    })
    rows = fx.rows_by(resp, key=".name")
    assert {k: fx.value(r, ".n_customers") for k, r in rows.items()} == {"North": 2, "South": 2, "East": 1, "West": 0}
    assert {k: fx.value(r, ".n_orders") for k, r in rows.items()} == {"North": 8, "South": 3, "East": 0, "West": 0}
    # A conditional sum over no rows is NULL, not 0.
    assert {k: fx.value(r, ".orders_2025") for k, r in rows.items()} == {
        "North": 6, "South": 2, "East": None, "West": None,
    }


async def test_order_by_an_undisplayed_aggregate_with_limit(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": ["customers.name"], "measures": ["count(*)"],
        "order": [{"column": "sum(amount)", "direction": "desc"}], "limit": 2,
    })
    assert [fx.value(r, ".name") for r in resp.data] == ["Carol", "Alice"]
    assert not any("amount" in c for c in resp.columns)


async def test_rank_filter_keeps_top_n_per_group(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": ["customers.region_id", "customers.name"],
        "measures": [{"formula": "sum(amount)", "name": "total"}], "filters": [fx.RANK_FILTER],
    })
    assert {fx.value(r, ".name") for r in resp.data} == {"Alice", "Carol"}


async def test_share_of_group_and_of_grand_total(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": ["customers.region_id", "customers.name"],
        "measures": [
            {"formula": fx.SHARE_OF_GROUP, "name": "share_region"},
            {"formula": fx.SHARE_OF_TOTAL, "name": "share_total"},
        ],
    })
    rows = fx.rows_by(resp, key=".name")
    assert {k: fx.value(r, ".share_region") for k, r in rows.items()} == {
        "Alice": pytest.approx(170 / 300), "Bob": pytest.approx(130 / 300), "Carol": pytest.approx(1.0),
    }
    assert {k: fx.value(r, ".share_total") for k, r in rows.items()} == {
        "Alice": pytest.approx(170 / 490), "Bob": pytest.approx(130 / 490), "Carol": pytest.approx(190 / 490),
    }


async def test_cross_model_aggregate_broadcasts_by_default(engine: SlayerQueryEngine) -> None:
    with pytest.warns(BroadcastGrainWarning):
        resp = await _run(engine, {
            "source_model": "orders", "dimensions": ["status"],
            "measures": [{"formula": fx.CROSS_MODEL, "name": "credit"}],
        })
    assert {k: fx.value(r, ".credit") for k, r in fx.rows_by(resp, key=".status").items()} == {
        "paid": 1500, "open": 1500,
    }
    assert "broadcast" in _kinds(resp)


async def test_cross_model_aggregate_splits_under_associate(engine: SlayerQueryEngine) -> None:
    with pytest.warns(AssociatedGrainWarning):
        resp = await _run(engine, {
            "source_model": "orders", "dimensions": ["status"], "to_many_handling": "associate",
            "measures": [{"formula": fx.CROSS_MODEL, "name": "credit"}],
        })
    assert {k: fx.value(r, ".credit") for k, r in fx.rows_by(resp, key=".status").items()} == {
        "paid": 600, "open": 600,
    }


async def test_nested_aggregation_averages_inner_values(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": ["customers.region_id"],
        "measures": [{"formula": fx.NESTED, "name": "avg_customer_total"}],
    })
    rows = fx.rows_by(resp, key="region_id")
    assert {k: fx.value(r, ".avg_customer_total") for k, r in rows.items()} == {
        1: pytest.approx(150), 2: pytest.approx(190),
    }


async def test_outer_stage_reads_flattened_inner_dimension(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, [
        {
            "name": "per_customer", "source_model": "orders",
            "dimensions": ["customers.region_id", "customers.name"],
            "measures": [{"formula": "sum(amount)", "name": "total"}],
        },
        {
            "source_model": "per_customer", "dimensions": [fx.STAGE_COLUMN],
            "measures": ["count(*)", {"formula": "sum(total)", "name": "region_total"}],
        },
    ])
    rows = fx.rows_by(resp, key=fx.STAGE_COLUMN)
    assert {k: (fx.value(r, "._count"), fx.value(r, ".region_total")) for k, r in rows.items()} == {
        1: (2, 300), 2: (1, 190),
    }
