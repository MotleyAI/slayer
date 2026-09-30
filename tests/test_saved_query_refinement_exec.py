"""Executed saved-query refinement on SQLite and DuckDB (spec: queries/saved-query-refinement)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, Dict, List

import pytest

from slayer.core.errors import RefinementConflictError, UnknownReferenceError
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql.client import SlayerSQLClient
from tests._saved_query_refinement_fixtures import (
    AVG_CUSTOMER_REVENUE,
    BY_REGION_MONTH,
    MONTH_TD,
    MONTHLY,
    MONTHLY_REVENUE,
    MONTHLY_REVENUE_Q1,
    REVENUE,
    REVENUE_BY_STATUS,
    build_refine_storage,
    by_month,
    month,
    refine_engine,
)

ONLY_BY_NAME = "refine applies only to a saved query run by name"


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with refine_engine(request.param) as (eng, _):
        yield eng


def executed_sql(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    calls: List[str] = []
    real = SlayerSQLClient.execute

    async def spy(self, *args: Any, **kwargs: Any):
        calls.append(kwargs.get("sql", args[0] if args else ""))
        return await real(self, *args, **kwargs)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def region_month(resp: SlayerResponse) -> Dict[tuple, Any]:
    return {(r["orders.region"], month(r["orders.ordered_at"])): r["orders.revenue"] for r in resp.data}


def canonical_rows(resp: SlayerResponse) -> List[tuple]:
    return sorted(tuple(str(r[c]) for c in resp.columns) for r in resp.data)


def with_fields(base: Dict[str, Any], **fields: Any) -> Dict[str, Any]:
    return {**base, **fields}


# Refinement → the merged final stage written by hand.
PARITY = {
    "extra_dimension": ({"dimensions": ["region"]}, with_fields(MONTHLY_REVENUE, dimensions=["region"])),
    "extra_measure": (
        {"measures": ["count(*)"]}, with_fields(MONTHLY_REVENUE, measures=[REVENUE, "count(*)"]),
    ),
    "extra_filter": (
        {"filters": ["amount >= 50"]},
        with_fields(MONTHLY_REVENUE, filters=["status = 'paid'", "amount >= 50"]),
    ),
    "window": (
        {"time_dimensions": [{**MONTH_TD, "date_range": ["2025-02-01", "2025-03-31"]}]},
        with_fields(MONTHLY_REVENUE, time_dimensions=[{**MONTH_TD, "date_range": ["2025-02-01", "2025-03-31"]}]),
    ),
    "order_limit": (
        {"order": [{"column": "revenue", "direction": "desc"}], "limit": 1},
        with_fields(MONTHLY_REVENUE, order=[{"column": "revenue", "direction": "desc"}], limit=1),
    ),
    "second_granularity": (
        {"time_dimensions": ["year(ordered_at)"]},
        with_fields(MONTHLY_REVENUE, time_dimensions=[MONTH_TD, "year(ordered_at)"]),
    ),
    "functional_dimension": (
        {"dimensions": ["year(ordered_at)"]},
        with_fields(MONTHLY_REVENUE, time_dimensions=[MONTH_TD, "year(ordered_at)"]),
    ),
    "identical_duplicates": (
        {"dimensions": ["orders.region"], "measures": [REVENUE]},
        with_fields(MONTHLY_REVENUE, dimensions=["region"]),
    ),
}


class TestRefinedRunEqualsHandWritten:
    @pytest.mark.parametrize("case", sorted(PARITY))
    async def test_parity(self, engine: SlayerQueryEngine, case: str) -> None:
        refinement, hand_written = PARITY[case]
        refined = await engine.execute("monthly_revenue", refine=refinement)
        expected = await engine.execute(hand_written)
        assert refined.columns == expected.columns
        assert canonical_rows(refined) == canonical_rows(expected)
        assert [w.human_message() for w in refined.warnings] == [w.human_message() for w in expected.warnings]


class TestSingleStage:
    async def test_plain_run_by_name(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue")
        assert resp.columns == ["orders.ordered_at", "orders.revenue"]
        assert by_month(resp.data) == MONTHLY

    async def test_extra_dimension(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue", refine={"dimensions": ["region"]})
        assert set(resp.columns) == {"orders.region", "orders.ordered_at", "orders.revenue"}
        assert region_month(resp) == BY_REGION_MONTH

    async def test_extra_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue", refine={"measures": ["count(*)"]})
        assert by_month(resp.data, value="orders._count") == {"2025-01": 1, "2025-02": 2, "2025-03": 1}
        assert by_month(resp.data) == MONTHLY

    async def test_extra_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue", refine={"filters": ["amount >= 50"]})
        assert by_month(resp.data) == {"2025-01": 100.0, "2025-02": 70.0, "2025-03": 50.0}

    async def test_window_added(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue", refine={
            "time_dimensions": [{**MONTH_TD, "date_range": ["2025-02-01", "2025-03-31"]}],
        })
        assert by_month(resp.data) == {"2025-02": 105.0, "2025-03": 50.0}

    async def test_order_and_limit(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(
            "monthly_revenue", refine={"order": [{"column": "revenue", "direction": "desc"}], "limit": 1},
        )
        assert [(month(r["orders.ordered_at"]), r["orders.revenue"]) for r in resp.data] == [("2025-02", 105.0)]

    async def test_second_granularity_renames_saved_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue", refine={"time_dimensions": ["year(ordered_at)"]})
        assert set(resp.columns) == {"orders.ordered_at.month", "orders.ordered_at.year", "orders.revenue"}
        assert by_month(resp.data, key="orders.ordered_at.month") == MONTHLY

    async def test_identical_duplicates_collapse(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(
            "monthly_revenue", refine={"dimensions": ["orders.region"], "measures": [REVENUE]},
        )
        assert region_month(resp) == BY_REGION_MONTH

    async def test_duplicate_filter_is_dropped(self, engine: SlayerQueryEngine) -> None:
        plain = await engine.execute("monthly_revenue")
        refined = await engine.execute("monthly_revenue", refine={"filters": ["status = 'paid'"]})
        assert refined.sql == plain.sql

    async def test_empty_refinement_is_plain_run(self, engine: SlayerQueryEngine) -> None:
        plain = await engine.execute("monthly_revenue")
        refined = await engine.execute("monthly_revenue", refine={})
        assert refined.sql == plain.sql
        assert refined.columns == plain.columns
        assert canonical_rows(refined) == canonical_rows(plain)

    async def test_conflict_executes_nothing(
        self, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls = executed_sql(monkeypatch)
        with pytest.raises(RefinementConflictError) as info:
            await engine.execute("monthly_revenue", refine={"measures": [{"formula": "count(*)", "name": "revenue"}]})
        for part in ("measures", "revenue", "sum(amount)", "count(*)"):
            assert part in str(info.value)
        assert calls == []

    @pytest.mark.parametrize("refinement", [
        {"source_model": "orders"}, {"name": "x"}, {"version": 4}, {"variables": {"a": 1}}, {"bogus": 1},
    ])
    async def test_forbidden_fields_rejected_before_execution(
        self, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch, refinement: Dict[str, Any],
    ) -> None:
        calls = executed_sql(monkeypatch)
        with pytest.raises(ValueError):
            await engine.execute("monthly_revenue", refine=refinement)
        assert calls == []

    async def test_stored_model_unchanged(self, engine: SlayerQueryEngine) -> None:
        before = await engine.storage.get_model("monthly_revenue")
        await engine.execute("monthly_revenue", refine={"dimensions": ["region"], "limit": 1})
        assert await engine.storage.get_model("monthly_revenue") == before


class TestSavedWindow:
    async def test_same_range_runs_unchanged(self, engine: SlayerQueryEngine) -> None:
        plain = await engine.execute("monthly_revenue_q1")
        refined = await engine.execute("monthly_revenue_q1", refine={
            "time_dimensions": [{**MONTH_TD, "date_range": ["2025-01-01", "2025-02-28"]}],
        })
        assert refined.sql == plain.sql
        assert [(month(r["orders.ordered_at"]), r["orders.revenue"]) for r in refined.data] == [
            ("2025-02", 105.0), ("2025-01", 100.0),
        ]

    async def test_filter_narrows_window(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue_q1", refine={"filters": ["ordered_at >= '2025-02-01'"]})
        assert [(month(r["orders.ordered_at"]), r["orders.revenue"]) for r in resp.data] == [("2025-02", 105.0)]

    async def test_limit_cleared(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("monthly_revenue_q1", refine={"limit": None})
        assert [(month(r["orders.ordered_at"]), r["orders.revenue"]) for r in resp.data] == [
            ("2025-02", 105.0), ("2025-01", 100.0),
        ]

    async def test_limit_null_drops_limit_from_sql(self, engine: SlayerQueryEngine) -> None:
        refined = await engine.execute("monthly_revenue_q1", refine={"limit": None}, dry_run=True)
        hand_written = {k: v for k, v in MONTHLY_REVENUE_Q1.items() if k != "limit"}
        assert refined.sql == (await engine.execute(hand_written, dry_run=True)).sql

    async def test_conflicting_range(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(RefinementConflictError) as info:
            await engine.execute("monthly_revenue_q1", refine={
                "time_dimensions": [{**MONTH_TD, "date_range": ["2025-02-01", "2025-03-31"]}],
            })
        message = str(info.value)
        assert "time_dimensions" in message
        assert "ordered_at@month" in message
        assert "filter" in message


class TestMultiStage:
    async def test_plain(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("avg_customer_revenue")
        assert resp.data == [{"per_customer.avg_revenue": pytest.approx(63.75)}]

    async def test_dimension_merges_into_final_stage(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("avg_customer_revenue", refine={"dimensions": ["region"]})
        assert {r["per_customer.region"]: r["per_customer.avg_revenue"] for r in resp.data} == {
            "US": pytest.approx(75.0), "EU": pytest.approx(52.5),
        }

    async def test_filter_applies_to_final_stage(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("avg_customer_revenue", refine={"filters": ["region = 'EU'"]})
        assert resp.data == [{"per_customer.avg_revenue": pytest.approx(52.5)}]

    async def test_earlier_stage_untouched(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("avg_customer_revenue", refine={"measures": ["count(*)"]})
        assert resp.data == [{"per_customer.avg_revenue": pytest.approx(63.75), "per_customer._count": 4}]

    async def test_parity_with_hand_written(self, engine: SlayerQueryEngine) -> None:
        refined = await engine.execute("avg_customer_revenue", refine={"dimensions": ["region"]})
        first, final = AVG_CUSTOMER_REVENUE
        expected = await engine.execute([first, {**final, "dimensions": ["region"]}])
        assert refined.columns == expected.columns
        assert canonical_rows(refined) == canonical_rows(expected)

    async def test_aggregated_away_column_is_unknown(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(UnknownReferenceError, match="status"):
            await engine.execute("avg_customer_revenue", refine={"dimensions": ["status"]})


class TestVariables:
    async def test_saved_default(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("revenue_by_status")
        assert {r["orders.region"]: r["orders.revenue"] for r in resp.data} == {"US": 150.0, "EU": 105.0}

    async def test_runtime_override(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("revenue_by_status", variables={"status": "refunded"})
        assert resp.data == [{"orders.region": "US", "orders.revenue": 40.0}]

    async def test_refinement_with_runtime_variables(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(
            "revenue_by_status", refine={"measures": ["count(*)"]}, variables={"status": "refunded"},
        )
        assert resp.data == [{"orders.region": "US", "orders.revenue": 40.0, "orders._count": 1}]

    async def test_refinement_placeholder_gets_saved_default(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute("revenue_by_status", refine={"filters": ["'{status}' = status"]})
        assert {r["orders.region"]: r["orders.revenue"] for r in resp.data} == {"US": 150.0, "EU": 105.0}

    async def test_refinement_placeholder_gets_runtime_value(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(
            "revenue_by_status", refine={"filters": ["'{status}' = status"]}, variables={"status": "refunded"},
        )
        assert resp.data == [{"orders.region": "US", "orders.revenue": 40.0}]

    async def test_refinement_placeholder_new_variable(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(
            "monthly_revenue", refine={"filters": ["region = '{region}'"]}, variables={"region": "EU"},
        )
        assert by_month(resp.data) == {"2025-02": 105.0}


class TestOnlyWithAName:
    @pytest.mark.parametrize("query", [
        MONTHLY_REVENUE,
        SlayerQuery.model_validate(MONTHLY_REVENUE),
        AVG_CUSTOMER_REVENUE,
    ], ids=["dict", "query", "list"])
    async def test_execute_rejects_non_name(self, engine: SlayerQueryEngine, query: Any) -> None:
        with pytest.raises(ValueError, match=ONLY_BY_NAME):
            await engine.execute(query, refine={"dimensions": ["region"]})

    @pytest.mark.parametrize("query", [
        MONTHLY_REVENUE,
        SlayerQuery.model_validate(MONTHLY_REVENUE),
        AVG_CUSTOMER_REVENUE,
    ], ids=["dict", "query", "list"])
    async def test_execute_rejects_empty_refine_with_non_name(self, engine: SlayerQueryEngine, query: Any) -> None:
        with pytest.raises(ValueError, match=ONLY_BY_NAME):
            await engine.execute(query, refine={})

    async def test_evict_rejects_non_name(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError, match=ONLY_BY_NAME):
            await engine.evict(MONTHLY_REVENUE, refine={"dimensions": ["region"]})

    async def test_non_query_backed_model_error_unchanged(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError, match="not query-backed"):
            await engine.execute("orders", refine={"dimensions": ["region"]})


class TestSyncWrappers:
    def test_execute_sync(self, tmp_path) -> None:
        engine = SlayerQueryEngine(storage=asyncio.run(build_refine_storage(str(tmp_path))))
        resp = engine.execute_sync("monthly_revenue", refine={"dimensions": ["region"]})
        assert region_month(resp) == BY_REGION_MONTH
        with pytest.raises(ValueError, match=ONLY_BY_NAME):
            engine.execute_sync(REVENUE_BY_STATUS, refine={"dimensions": ["status"]})
