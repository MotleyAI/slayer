"""Expression time dimensions in every time position, executed on SQLite and DuckDB (spec: queries/time-dimensions)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from slayer.core.errors import (
    GranularityCallError,
    QueryTypeError,
    TimeDimensionColumnError,
    TimeLiteralError,
    UnsafeJoinInputError,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev2119_fixtures import (
    BACKENDS,
    CREATED_MONTH_COUNT,
    EFF,
    EFF_MONTH_COUNT,
    EFF_MONTH_SUM,
    EFF_NAME,
    bucket,
    by_bucket,
    engine_for,
    m,
    num,
    q,
    td,
)
from tests._time_points_fixtures import PinnedClock

KEY = f"orders.{EFF_NAME}"
CAST_DAY = "CAST(created_at AS DATE)"
CAST_KEY = "orders.cast_created_at_as_date"
#: Months of ``EFF`` from February: plain sum, trailing-3-month sum, change vs the previous month.
FEB_APR_SUM = {"2025-02": 50.0, "2025-03": 100.0, "2025-04": 50.0}
FEB_APR_WINDOW = {"2025-02": 60.0, "2025-03": 160.0, "2025-04": 200.0}
FEB_APR_CHANGE = {"2025-02": 40.0, "2025-03": 50.0, "2025-04": -50.0}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with engine_for(request.param) as e:
        yield e


async def _plan_fails(engine: SlayerQueryEngine, query: Any, error: type[Exception]) -> Exception:
    with pytest.raises(error) as ei:
        await engine.execute(query, dry_run=True)
    return ei.value


def _count(**kw: Any) -> SlayerQuery:
    return q(measures=[m("count(*)")], **kw)


def _rows(resp: SlayerResponse, *, key: str) -> set[tuple]:
    """Rows as ``(bucket, values...)`` with the bucket under ``key`` normalised."""
    return {(bucket(r[key]), *(num(r[c]) for c in resp.columns if c != key)) for r in resp.data}


class TestExpressionHomes:
    async def test_coalesced_timestamp_groups_by_month(self, engine) -> None:
        resp = await engine.execute(_count(time_dimensions=[td(EFF)]))
        assert by_bucket(resp, KEY, "orders.v") == EFF_MONTH_COUNT

    async def test_cast_text_to_date(self, engine) -> None:
        resp = await engine.execute(_count(time_dimensions=[td("CAST(created_txt AS DATE)")]))
        assert by_bucket(resp, "orders.cast_created_txt_as_date", "orders.v") == CREATED_MONTH_COUNT

    async def test_cross_model_expression_over_a_to_one_hop(self, engine) -> None:
        resp = await engine.execute(_count(time_dimensions=[td("coalesce(customers.signup_ts, created_at)")]))
        got = by_bucket(resp, "orders.coalesce_customers_signup_ts_created_at", "orders.v")
        assert got == {"2024-11": 3, "2025-02": 2, "2025-03": 1}

    async def test_stage_scope_expression(self, engine) -> None:
        inner = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders", "dimensions": ["id", "shipped_at", "created_at"],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "source_model": "s1", "time_dimensions": [td(EFF)], "measures": [{"formula": "sum(rev)", "name": "v"}],
        })
        resp = await engine.execute([inner, outer])
        assert by_bucket(resp, f"s1.{EFF_NAME}", "s1.v") == EFF_MONTH_SUM


class TestExpressionTyping:
    async def test_leaf_across_a_fanning_hop_fails_closed(self, engine) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "accounts", "measures": [{"formula": "count(*)", "name": "v"}],
            "time_dimensions": [td("coalesce(orders.shipped_at, signup_ts)")],
        })
        msg = str(await _plan_fails(engine, query, UnsafeJoinInputError))
        assert "shipped_at" in msg
        assert "orders" in msg

    async def test_non_temporal_expression_rejected(self, engine) -> None:
        msg = str(await _plan_fails(engine, _count(time_dimensions=[td("upper(status)")]), TimeDimensionColumnError))
        assert "upper_status" in msg or "upper(status)" in msg
        assert "TEXT" in msg.upper()

    async def test_undetermined_type_names_the_cast_remedy(self, engine) -> None:
        query = _count(time_dimensions=[td("CASE WHEN status = 'paid' THEN NULL END")])
        msg = str(await _plan_fails(engine, query, TimeDimensionColumnError))
        assert "CAST" in msg

    async def test_aggregate_inside_rejected(self, engine) -> None:
        query = _count(time_dimensions=[td("max(created_at, partition_by=status)")])
        msg = str(await _plan_fails(engine, query, QueryTypeError))
        assert "row-level" in msg


class TestTimePositions:
    async def test_date_range(self, engine) -> None:
        resp = await engine.execute(_count(time_dimensions=[td(EFF, date_range=["2025-01-01", "2025-03-31"])]))
        assert by_bucket(resp, KEY, "orders.v") == {"2025-01": 1, "2025-02": 2, "2025-03": 2}

    async def test_timestamp_expression_takes_instant_bounds(self, engine) -> None:
        resp = await engine.execute(_count(time_dimensions=[td(EFF, date_range=["2025-01-07 09:30:00", "2025-02-28"])]))
        assert by_bucket(resp, KEY, "orders.v") == {"2025-02": 2}

    async def test_date_expression_takes_date_bounds_whole_periods(self, engine) -> None:
        resp = await engine.execute(_count(
            time_dimensions=[td(CAST_DAY, date_range=["2025-01", "2025-02"])], whole_periods_only=True,
        ))
        assert by_bucket(resp, "orders.cast_created_at_as_date", "orders.v") == {"2025-01": 2, "2025-02": 2}

    async def test_date_expression_rejects_a_sub_day_point(self, engine) -> None:
        query = _count(time_dimensions=[td(CAST_DAY, date_range=["2025-01-05 12:00:00", "2025-02-28"])])
        await _plan_fails(engine, query, TimeLiteralError)

    @pytest.mark.parametrize("dimension, key, snap, expected", [
        (CAST_DAY, CAST_KEY, False, {"2025-01": 1, "2025-02": 2}),
        (CAST_DAY, CAST_KEY, True, {"2025-01": 2, "2025-02": 2}),
        (EFF, KEY, False, {"2025-02": 2, "2025-03": 1}),
        (EFF, KEY, True, {"2025-01": 1, "2025-02": 2}),
    ], ids=["date", "date-snapped", "timestamp", "timestamp-snapped"])
    async def test_whole_periods_snap_explicit_bounds(self, engine, dimension: str, key: str, snap: bool,
                                                      expected: dict) -> None:
        resp = await engine.execute(_count(
            time_dimensions=[td(dimension, date_range=["2025-01-15", "2025-03-10"])], whole_periods_only=snap,
        ))
        assert by_bucket(resp, key, "orders.v") == expected

    @pytest.mark.parametrize("dimension, key, expected", [
        (CAST_DAY, CAST_KEY, {"2025-01": 2, "2025-02": 2}),
        (EFF, KEY, {"2025-01": 1, "2025-02": 2}),
    ], ids=["date", "timestamp"])
    async def test_whole_periods_snap_mirrored_filter_bounds(self, engine, dimension: str, key: str,
                                                             expected: dict) -> None:
        resp = await engine.execute(_count(
            time_dimensions=[td(dimension)], whole_periods_only=True,
            filters=[f"'2025-01-15' <= {dimension}", f"{dimension} <= '2025-03-10'"],
        ))
        assert by_bucket(resp, key, "orders.v") == expected

    @pytest.mark.parametrize("dimension, key, expected", [
        (CAST_DAY, CAST_KEY, {"2024-12": 1, "2025-01": 2, "2025-02": 2}),
        (EFF, KEY, {"2025-01": 1, "2025-02": 2}),
    ], ids=["date", "timestamp"])
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_whole_periods_synthesized_upper_bound(self, backend: str, dimension: str, key: str,
                                                         expected: dict) -> None:
        async with engine_for(backend, clock=PinnedClock(datetime(2025, 3, 10, 12, 0, 0))) as e:
            resp = await e.execute(_count(time_dimensions=[td(dimension)], whole_periods_only=True))
        assert by_bucket(resp, key, "orders.v") == expected

    async def test_rolling_window_and_change_over_an_expression_axis(self, engine) -> None:
        resp = await engine.execute(q(
            time_dimensions=[td(EFF, date_range=["2025-02-01", "2025-04-30"])],
            measures=[m("sum(amount)", "s"), m("sum(amount, window='3m')", "w"), m("change(sum(amount))", "c")],
        ))
        assert by_bucket(resp, KEY, "orders.s") == FEB_APR_SUM
        assert by_bucket(resp, KEY, "orders.w") == FEB_APR_WINDOW
        assert by_bucket(resp, KEY, "orders.c") == FEB_APR_CHANGE

    @pytest.mark.parametrize("bound", [f"{EFF} >= '2025-02'", f"month({EFF}) >= '2025-02-01'"])
    async def test_filter_on_the_expression_is_a_frame_bound(self, engine, bound: str) -> None:
        resp = await engine.execute(q(time_dimensions=[td(EFF)], filters=[bound],
                                      measures=[m("sum(amount, window='3m')", "w")]))
        assert by_bucket(resp, KEY, "orders.w") == FEB_APR_WINDOW

    @pytest.mark.parametrize("measures", [
        [m("count(*)", "n"), m("sum(amount)", "s")],
        [m("sum(amount, window='3m')", "w"), m("change(sum(amount))", "c"), m("cumsum(count(*))", "k")],
        [m("last(amount)", "l"), m("first(amount)", "f")],
    ], ids=["plain", "time-ordered", "ranked"])
    async def test_equal_to_a_column_holding_the_per_row_values(self, engine, measures: list) -> None:
        by_expr = await engine.execute(q(time_dimensions=[td(EFF)], measures=measures))
        by_col = await engine.execute(q(time_dimensions=[td("eff")], measures=measures))
        assert _rows(by_expr, key=KEY) == _rows(by_col, key="orders.eff")


class TestStageBuckets:
    def _inner(self) -> SlayerQuery:
        return SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders", "time_dimensions": [td(EFF)],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        })

    def _outer(self, granularity: str) -> SlayerQuery:
        return SlayerQuery.model_validate({
            "source_model": "s1", "time_dimensions": [td(EFF_NAME, granularity)],
            "measures": [{"formula": "sum(rev)", "name": "v"}],
        })

    async def test_coarser_rebucket_sums_the_monthly_sums(self, engine) -> None:
        resp = await engine.execute([self._inner(), self._outer("year")])
        assert by_bucket(resp, f"s1.{EFF_NAME}", "s1.v", width=4) == {"2025": 210.0}

    async def test_finer_rebucket_rejected(self, engine) -> None:
        msg = str(await _plan_fails(engine, [self._inner(), self._outer("day")], TimeDimensionColumnError))
        assert "month" in msg
        assert "day" in msg


class TestPerLeafRebucketing:
    def _monthly(self, dimension: str, granularity: str) -> SlayerQuery:
        return SlayerQuery.model_validate({
            "source_model": "monthly", "time_dimensions": [td(dimension, granularity)],
            "measures": [{"formula": "sum(rev)", "name": "v"}],
        })

    async def test_cast_of_a_month_bucketed_column_at_day_rejected(self, engine) -> None:
        msg = str(await _plan_fails(engine, self._monthly(CAST_DAY, "day"), TimeDimensionColumnError))
        assert "created_at" in msg
        assert "month" in msg
        assert "day" in msg

    @pytest.mark.parametrize("granularity, width, expected", [
        ("month", 7, {"2024-12": 60.0, "2025-01": 30.0, "2025-02": 70.0, "2025-03": 50.0}),
        ("year", 4, {"2024": 60.0, "2025": 150.0}),
    ])
    async def test_same_and_coarser_granularities_bind(self, engine, granularity: str, width: int,
                                                       expected: dict) -> None:
        resp = await engine.execute(self._monthly(CAST_DAY, granularity))
        assert by_bucket(resp, "monthly.cast_created_at_as_date", "monthly.v", width=width) == expected

    async def test_bucketed_leaf_beside_an_unbucketed_one_binds_the_rule(self, engine) -> None:
        query = self._monthly("coalesce(created_at, refunded_at)", "day")
        msg = str(await _plan_fails(engine, query, TimeDimensionColumnError))
        assert "created_at" in msg
        assert "month" in msg


class TestNamesAndKeys:
    async def test_unnamed_expression_key(self, engine) -> None:
        resp = await engine.execute(_count(time_dimensions=[td(EFF)]))
        assert resp.columns == [KEY, "orders.v"]

    async def test_named_expression_key_and_main_time_dimension(self, engine) -> None:
        resp = await engine.execute(q(
            time_dimensions=[td("created_at"), td(EFF, name="effective_at")], main_time_dimension="effective_at",
            measures=[m("cumsum(count(*))")],
        ))
        assert set(resp.columns) == {"orders.created_at", "orders.effective_at", "orders.v"}
        got = {(bucket(r["orders.created_at"]), bucket(r["orders.effective_at"])): num(r["orders.v"])
               for r in resp.data}
        assert got == {
            ("2024-12", "2025-03"): 1, ("2025-01", "2025-01"): 1, ("2025-01", "2025-02"): 2,
            ("2025-02", "2025-02"): 1, ("2025-02", "2025-03"): 2, ("2025-03", "2025-04"): 1,
        }

    async def test_named_column_time_dimension(self, engine) -> None:
        named = await engine.execute(_count(time_dimensions=[td("created_at", name="m")]))
        assert named.columns == ["orders.m", "orders.v"]
        assert by_bucket(named, "orders.m", "orders.v") == CREATED_MONTH_COUNT

    async def test_named_time_dimension_never_carries_the_granularity_suffix(self, engine) -> None:
        resp = await engine.execute(_count(time_dimensions=[td(EFF, name="em"), td(EFF, "year", name="ey")]))
        assert set(resp.columns) == {"orders.em", "orders.ey", "orders.v"}

    async def test_partition_by_a_time_dimension_name(self, engine) -> None:
        resp = await engine.execute(q(
            dimensions=["region"], time_dimensions=[td(EFF, name="effective_at")],
            measures=[m("sum(amount, partition_by=effective_at)")],
        ))
        got = by_bucket(resp, "orders.effective_at", "orders.v", by=["orders.region"])
        assert got == {("EU", "2025-01"): 10.0, ("EU", "2025-02"): 50.0, ("EU", "2025-04"): 50.0,
                       ("US", "2025-02"): 50.0, ("US", "2025-03"): 100.0}

    async def test_two_granularities_of_one_expression(self, engine) -> None:
        resp = await engine.execute(_count(dimensions=[f"month({EFF})", f"year({EFF})"]))
        assert set(resp.columns) == {f"{KEY}.month", f"{KEY}.year", "orders.v"}
        got = {(bucket(r[f"{KEY}.month"]), bucket(r[f"{KEY}.year"], width=4)): num(r["orders.v"]) for r in resp.data}
        assert got == {(k, "2025"): v for k, v in EFF_MONTH_COUNT.items()}

    async def test_equivalent_spellings_with_differing_label_rejected_at_binding(self, engine) -> None:
        query = _count(time_dimensions=[td(EFF, label="a"), td("COALESCE(shipped_at,created_at)", label="b")])
        msg = str(await _plan_fails(engine, query, GranularityCallError))
        assert "Conflicting" in msg

    async def test_main_time_dimension_by_expression_name(self, engine) -> None:
        resp = await engine.execute(q(
            dimensions=["month(created_at)"], time_dimensions=[td(EFF, name="effective_at")],
            main_time_dimension="effective_at", measures=[m("cumsum(count(*))")],
        ))
        got = {(bucket(r["orders.created_at"]), bucket(r["orders.effective_at"])): num(r["orders.v"])
               for r in resp.data}
        assert got[("2025-01", "2025-02")] == 2
        assert got[("2025-02", "2025-03")] == 2


class TestOrderKey:
    async def test_order_by_a_projected_expression_bucket(self, engine) -> None:
        resp = await engine.execute(_count(
            dimensions=[f"month({EFF})"], order=[{"column": f"month({EFF})", "direction": "desc"}],
        ))
        assert [bucket(r[KEY]) for r in resp.data] == ["2025-04", "2025-03", "2025-02", "2025-01"]

    async def test_order_by_an_unprojected_granularity_names_the_remedy(self, engine) -> None:
        query = _count(dimensions=[f"year({EFF})"], order=[{"column": f"month({EFF})"}])
        msg = str(await _plan_fails(engine, query, GranularityCallError))
        assert "month" in msg

    async def test_functional_dimension_equals_explicit_form(self, engine) -> None:
        functional = await engine.execute(_count(dimensions=[f"month({EFF})"]))
        explicit = await engine.execute(_count(time_dimensions=[td(EFF)]))
        assert functional.sql == explicit.sql
        assert functional.data == explicit.data
