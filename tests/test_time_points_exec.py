"""Time points in Mode-B filters, by executed values on SQLite and DuckDB."""

from __future__ import annotations

from collections import Counter
from datetime import datetime

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.errors import DateOperandTypeError, GranularityCallError, QueryTypeError, TimeLiteralError
from slayer.core.query import SlayerQuery
from slayer.sql.client import SlayerSQLClient

from tests._time_points_fixtures import (
    BACKENDS, EV_TS, SHIPPED_NEXT_MONTH, UNSHIPPED, bucket_text, code_of, ev_dt, ids, ids_in, ids_where,
    shipped_dt, tp_engine,
)

D = datetime
ALL = set(EV_TS)
GRANULARITIES = [g.value for g in TimeGranularity]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with tp_engine(request.param) as eng:
        yield eng


@pytest.fixture
async def sqlite_engine():
    async with tp_engine("sqlite") as eng:
        yield eng


def _assert_lists_forms(msg: str) -> None:
    for form in ("YYYY-Qn", "YYYY-MM", "YYYY-Www", "last N"):
        assert form in msg, f"error must list {form!r}: {msg}"


class TestPeriodLiterals:
    @pytest.mark.parametrize(("literal", "start", "end"), [
        ("2025", D(2025, 1, 1), D(2026, 1, 1)),
        ("2025-Q1", D(2025, 1, 1), D(2025, 4, 1)),
        ("2025-03", D(2025, 3, 1), D(2025, 4, 1)),
        ("2025-W05", D(2025, 1, 27), D(2025, 2, 3)),
        ("2025-03-15", D(2025, 3, 15), D(2025, 3, 16)),
        ("2020-W53", D(2020, 12, 28), D(2021, 1, 4)),
        ("2024-02-29", D(2024, 2, 29), D(2024, 3, 1)),
    ])
    async def test_equality_is_half_open_range(self, engine, literal, start, end) -> None:
        assert await ids(engine, f"ts = '{literal}'") == ids_in(start, end)

    @pytest.mark.parametrize("literal", ["2021-W53", "2025-02-29", "2025-13", "2025-Q5"])
    async def test_non_existent_period_rejected(self, engine, literal) -> None:
        with pytest.raises(TimeLiteralError) as ei:
            await ids(engine, f"ts = '{literal}'")
        _assert_lists_forms(str(ei.value))


class TestUnparseable:
    @pytest.mark.parametrize("literal", ["last fortnight", "2025/01/01"])
    async def test_rejected_before_any_sql(self, engine, monkeypatch, literal) -> None:
        calls: list[str] = []
        orig = SlayerSQLClient.execute

        async def spy(self, sql, timeout_seconds=120):
            calls.append(sql)
            return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

        monkeypatch.setattr(SlayerSQLClient, "execute", spy)
        with pytest.raises(TimeLiteralError) as ei:
            await ids(engine, f"ts >= '{literal}'")
        _assert_lists_forms(str(ei.value))
        assert [s for s in calls if "slayer_rk_" not in s] == []


class TestInstants:
    async def test_inclusive_instant(self, engine) -> None:
        bound = D(2024, 12, 31, 12)
        assert await ids(engine, "ts <= '2024-12-31 12:00:00'") == ids_where(lambda t: t <= bound)

    async def test_t_separated_instant_equality(self, engine) -> None:
        assert await ids(engine, "ts = '2024-06-01T10:00:00'") == {4}

    @pytest.mark.parametrize("literal", ["2025-01-01 10:00:00Z", "2025-01-01 10:00:00+02:00"])
    async def test_zone_offset_rejected(self, engine, literal) -> None:
        with pytest.raises(TimeLiteralError):
            await ids(engine, f"ts >= '{literal}'")

    async def test_midnight_instant_is_not_a_day(self, engine) -> None:
        assert await ids(engine, "ts = '2024-01-01 00:00:00'") == {1}
        assert await ids(engine, "ts > '2024-12-31 00:00:00'") == ids_where(lambda t: t > D(2024, 12, 31))


class TestComparisonTable:
    FOUR = {1, 2, 5, 6}

    @pytest.mark.parametrize(("filt", "expected"), [
        ("ts <= '2024-12-31'", {1, 2, 5, 6}),
        ("ts > '2024-01-01'", {5, 6}),
        ("ts = '2024-01-01'", {1, 2}),
        ("ts != '2024-01-01'", {5, 6}),
    ])
    async def test_date_only_bounds_cover_whole_days(self, engine, filt, expected) -> None:
        assert await ids(engine, filt) & self.FOUR == expected

    @pytest.mark.parametrize(("filt", "pred"), [
        ("ts >= '2025-Q1'", lambda t: t >= D(2025, 1, 1)),
        ("ts > '2025-Q1'", lambda t: t >= D(2025, 4, 1)),
        ("ts < '2025-Q1'", lambda t: t < D(2025, 1, 1)),
        ("ts <= '2025-Q1'", lambda t: t < D(2025, 4, 1)),
        ("ts = '2025-Q1'", lambda t: D(2025, 1, 1) <= t < D(2025, 4, 1)),
        ("ts != '2025-Q1'", lambda t: t < D(2025, 1, 1) or t >= D(2025, 4, 1)),
    ])
    async def test_period_operators(self, engine, filt, pred) -> None:
        assert await ids(engine, filt) == ids_where(pred)

    @pytest.mark.parametrize(("mirrored", "plain"), [
        ("'2025-Q1' <= ts", "ts >= '2025-Q1'"),
        ("'2025-Q1' < ts", "ts > '2025-Q1'"),
        ("'2025-Q1' > ts", "ts < '2025-Q1'"),
        ("'2025-Q1' >= ts", "ts <= '2025-Q1'"),
        ("'2025-Q1' = ts", "ts = '2025-Q1'"),
        ("'2025-Q1' != ts", "ts != '2025-Q1'"),
    ])
    async def test_literal_on_left_mirrors(self, engine, mirrored, plain) -> None:
        assert await ids(engine, mirrored) == await ids(engine, plain)

    async def test_relative_bound(self, engine) -> None:
        assert await ids(engine, "ts >= 'last month'") == ids_where(lambda t: t >= D(2026, 8, 1))

    async def test_period_bound_combines_with_other_conditions(self, engine) -> None:
        got = await ids(engine, "ts = '2025-Q1' and amount > 10")
        assert got == {i for i in ids_in(D(2025, 1, 1), D(2025, 4, 1)) if i > 10}


class TestRelativeTokensExecuted:
    @pytest.mark.parametrize(("token", "start", "end"), [
        ("today", D(2026, 9, 29), D(2026, 9, 30)),
        ("yesterday", D(2026, 9, 28), D(2026, 9, 29)),
        ("this month", D(2026, 9, 1), D(2026, 10, 1)),
        ("last month", D(2026, 8, 1), D(2026, 9, 1)),
        ("this week", D(2026, 9, 28), D(2026, 10, 5)),
        ("this week_sunday", D(2026, 9, 27), D(2026, 10, 4)),
        ("last 6 hours", D(2026, 9, 29, 6), D(2026, 9, 29, 12)),
        ("year to date", D(2026, 1, 1), D(2026, 9, 30)),
        ("12 months ago", D(2025, 9, 1), D(2025, 10, 1)),
    ])
    async def test_token_as_period(self, engine, token, start, end) -> None:
        assert await ids(engine, f"ts = '{token}'") == ids_in(start, end)

    async def test_case_and_whitespace_normalised(self, engine) -> None:
        assert await ids(engine, "ts = 'Last   Month'") == await ids(engine, "ts = 'last month'")


class TestSingleStringIn:
    async def test_in_period(self, engine) -> None:
        assert await ids(engine, "ts in '2025-Q1'") == ids_in(D(2025, 1, 1), D(2025, 4, 1))

    async def test_not_in_relative(self, engine) -> None:
        assert await ids(engine, "ts not in 'this month'") == ALL - ids_in(D(2026, 9, 1), D(2026, 10, 1))

    async def test_tuple_keeps_list_meaning(self, engine) -> None:
        assert await ids(engine, "code in ('2025-Q1',)") == {2}
        assert await ids(engine, "code in ('2025', '2025-Q1')") == {1, 2}


class TestTemporalOperands:
    async def test_temporal_aggregate_in_measure_filter(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["code"],
            "measures": [{"formula": "count(*)", "name": "n"}],
            "filters": ["max(ts) >= 'last month'"],
        }))
        latest = {c: max(ev_dt(i) for i in EV_TS if code_of(i) == c) for c in map(code_of, EV_TS)}
        expected = {c for c, t in latest.items() if t >= D(2026, 8, 1)}
        assert {r["ev.code"] for r in resp.data} == expected == {"y"}

    @pytest.mark.parametrize(("filt", "expected"), [
        ("min(ts) < '2024-06'", {"2025", "2025-Q1", "y"}),
        ("first(ts) < '2021'", {"y"}),
        ("last(ts) >= 'last month'", {"y"}),
    ])
    async def test_min_first_last_are_temporal(self, engine, filt, expected) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["code"],
            "measures": [{"formula": "count(*)", "name": "n"}], "filters": [filt],
        }))
        assert {r["ev.code"] for r in resp.data} == expected

    async def test_temporal_aggregate_below_a_period(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["code"],
            "measures": [{"formula": "count(*)", "name": "n"}],
            "filters": ["max(ts) < '2024-06'"],
        }))
        assert {r["ev.code"] for r in resp.data} == {"2025", "2025-Q1"}

    async def test_joined_column(self, engine) -> None:
        # Only customer 1 (created 2025-01-15) is in Q1; ev rows map to customer 1 + id % 3.
        assert await ids(engine, "customers.created_at = '2025-Q1'") == {i for i in ALL if i % 3 == 0}

    async def test_derived_column(self, engine) -> None:
        assert await ids(engine, "ts_copy = '2025-Q1'") == await ids(engine, "ts = '2025-Q1'")

    async def test_stage_column(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "daily", "measures": [{"formula": "sum(rev)", "name": "r"}],
            "filters": ["ts = '2025-Q1'"],
        }))
        assert resp.data[0]["daily.r"] == pytest.approx(float(sum(ids_in(D(2025, 1, 1), D(2025, 4, 1)))))

    async def test_date_column_takes_periods(self, engine) -> None:
        assert await ids(engine, "d = '2025-Q1'") == ids_in(D(2025, 1, 1), D(2025, 4, 1))
        assert await ids(engine, "d <= '2024-12-31'") == ids_where(lambda t: t < D(2025, 1, 1))


class TestTypingErrors:
    @pytest.mark.parametrize("filt", ["raw_ts >= 'last month'", "raw_ts in '2025-Q1'"])
    async def test_untyped_column_names_column_and_remedy(self, engine, filt) -> None:
        with pytest.raises(DateOperandTypeError) as ei:
            await ids(engine, filt)
        msg = str(ei.value)
        assert "raw_ts" in msg, msg
        assert "type" in msg.lower(), msg

    async def test_relative_token_against_text_column(self, engine) -> None:
        with pytest.raises(DateOperandTypeError) as ei:
            await ids(engine, "code = 'last month'")
        assert "code" in str(ei.value)

    async def test_bad_date_function_operand_is_named(self, engine) -> None:
        with pytest.raises(DateOperandTypeError) as ei:
            await ids(engine, "date_add(code, 1, 'day') >= 'last month'")
        msg = str(ei.value)
        assert "`code` is not one" in msg, msg

    async def test_date_string_against_text_keeps_meaning(self, engine) -> None:
        assert await ids(engine, "code = '2025'") == {1}
        assert await ids(engine, "code = '2025-Q1'") == {2}

    async def test_plain_date_string_against_untyped_keeps_meaning(self, engine) -> None:
        assert await ids(engine, "raw_ts >= '2026-09-01'") == ids_where(lambda t: t >= D(2026, 9, 1))

    @pytest.mark.parametrize("filt", ["d >= 'last 6 hours'", "d = '2025-01-01 10:00:00'", "d < 'this hour'"])
    async def test_sub_day_point_against_date(self, engine, filt) -> None:
        with pytest.raises(TimeLiteralError) as ei:
            await ids(engine, filt)
        assert "day" in str(ei.value).lower()

    async def test_date_function_interval_and_conditional_operands(self, engine) -> None:
        assert await ids(engine, "date_add(ts, 1, 'day') >= 'last month'") == ids_where(lambda t: t >= D(2026, 7, 31))
        assert await ids(engine, "ts - interval(1, 'day') < '2025-Q1'") == ids_where(lambda t: t < D(2025, 1, 2))
        expected = {i for i in ALL if D(2025, 1, 1) <= (shipped_dt(i) or ev_dt(i)) < D(2025, 4, 1)}
        assert await ids(engine, "coalesce(shipped_at, ts) in '2025-Q1'") == expected
        assert UNSHIPPED & expected  # an unshipped Q1 row matches through its ts

    async def test_iso_literal_inside_a_conditional_operand(self, engine) -> None:
        got = await ids(engine, "coalesce(shipped_at, '2099-12-31') >= 'last month'")
        shipped_late = {i for i in ALL if (s := shipped_dt(i)) is not None and s >= D(2026, 8, 1)}
        assert got == UNSHIPPED | shipped_late

    async def test_temporal_aggregate_with_partition(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["id", "code"],
            "filters": ["max(ts, partition_by=[code]) >= 'last month'"],
        }))
        assert {r["ev.id"] for r in resp.data} == {i for i in ALL if code_of(i) == "y"}

    def test_error_is_a_query_type_error(self) -> None:
        assert issubclass(TimeLiteralError, QueryTypeError)


class TestDateOnlyTextStorage:
    @pytest.mark.parametrize("filt", ["ts = '2025-03-01'", "ts >= '2025-03-01 00:00:00'"])
    async def test_date_only_text_matches_like_midnight(self, engine, filt) -> None:
        assert await ids(engine, filt, model="mixed") == {1, 2, 4, 5, 6}

    async def test_millisecond_resolution(self, engine) -> None:
        assert await ids(engine, "ts >= '2025-03-01 10:00:00.250'", model="mixed") == {4, 6}
        assert await ids(engine, "ts < '2025-03-01 10:00:00.250'", model="mixed") == {1, 2, 3, 5}

    async def test_below_the_day(self, engine) -> None:
        assert await ids(engine, "ts < '2025-03-01'", model="mixed") == {3}


class TestSqliteTimestampSpelling:
    @pytest.mark.parametrize("filt", [
        "ts < '2025-03-01 10:00:00'",
        "ts >= '2025-03-01 10:00:00'",
        "ts <= '2025-03-01 09:30:00'",
        "ts = '2025-03-01 09:30:00'",
        "ts >= 'last 6 hours'",
        "ts = '2025-03-01'",
        "ts <= '2024-12-31'",
    ])
    async def test_t_storage_matches_space_storage(self, sqlite_engine, filt) -> None:
        spaced = await ids(sqlite_engine, filt)
        assert await ids(sqlite_engine, filt, model="ev_t") == spaced

    async def test_t_storage_sub_day_bound(self, sqlite_engine) -> None:
        assert await ids(sqlite_engine, "ts < '2025-03-01 10:00:00'", model="ev_t") & {12, 13} == {12}


class TestGranularityCalls:
    async def test_in_filter_lowers_to_exact_bound(self, engine) -> None:
        assert await ids(engine, "month(ts) >= '2024-03-15'") == ids_where(lambda t: t >= D(2024, 4, 1))

    @pytest.mark.parametrize(("filt", "pred"), [
        ("month(ts) > '2025-01'", lambda t: t >= D(2025, 2, 1)),
        ("month(ts) < '2025-03-15'", lambda t: t < D(2025, 4, 1)),
        ("month(ts) <= '2025-03-15'", lambda t: t < D(2025, 4, 1)),
        ("month(ts) = '2025-03'", lambda t: D(2025, 3, 1) <= t < D(2025, 4, 1)),
        ("quarter(ts) = '2025'", lambda t: D(2025, 1, 1) <= t < D(2026, 1, 1)),
        ("quarter(ts) = '2025-02'", lambda t: False),  # no quarter starts in February
        ("year(ts) = 'last year'", lambda t: D(2025, 1, 1) <= t < D(2026, 1, 1)),
        ("day(ts) >= '2025-03-01 10:00:00'", lambda t: t >= D(2025, 3, 2)),
    ])
    async def test_exact_bounds(self, engine, filt, pred) -> None:
        assert await ids(engine, filt) == ids_where(pred)

    async def test_compared_with_each_other(self, engine) -> None:
        assert await ids(engine, "month(shipped_at) = month(ts)") == ALL - SHIPPED_NEXT_MONTH - UNSHIPPED

    async def test_against_instants(self, engine) -> None:
        assert await ids(engine, "month(ts) = '2025-03-15 10:00:00'") == set()
        assert await ids(engine, "month(ts) = '2025-03-01 00:00:00'") == ids_in(D(2025, 3, 1), D(2025, 4, 1))
        assert await ids(engine, "month(ts) > '2025-03-15 10:00:00'") == ids_where(lambda t: t >= D(2025, 4, 1))
        assert await ids(engine, "month(ts) <= '2025-03-15 10:00:00'") == ids_where(lambda t: t < D(2025, 4, 1))

    async def test_case_insensitive_callee(self, engine) -> None:
        assert await ids(engine, "MONTH(ts) >= '2024-03-15'") == await ids(engine, "month(ts) >= '2024-03-15'")

    async def test_count_distinct_months(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "measures": [{"formula": "count_distinct(month(ts))", "name": "m"}],
        }))
        assert resp.data[0]["ev.m"] == len({(t.year, t.month) for t in map(ev_dt, EV_TS)})

    async def test_computed_dimension_groups_by_year_bucket(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": [{"expression": "year(ts)", "name": "y"}],
            "measures": [{"formula": "count(*)", "name": "n"}],
        }))
        got = {bucket_text(r["ev.y"])[:10]: r["ev.n"] for r in resp.data}
        assert got == {f"{y}-01-01": n for y, n in Counter(t.year for t in map(ev_dt, EV_TS)).items()}

    async def test_inside_arithmetic(self, engine) -> None:
        got = await ids(engine, "date_diff('day', month(ts), ts) = 0")
        assert got == ids_where(lambda t: t.day == 1)

    @pytest.mark.parametrize(("filt", "column"), [
        ("month(code) = '2025-03'", "code"),
        ("month(code) = month(ts)", "code"),
        ("date_diff('day', month(raw_ts), ts) = 0", "raw_ts"),
    ])
    async def test_non_temporal_operand_rejected(self, engine, filt, column) -> None:
        with pytest.raises(DateOperandTypeError) as ei:
            await ids(engine, filt)
        assert f"month() needs a DATE or TIMESTAMP operand; `{column}` is not one" in str(ei.value)

    async def test_non_temporal_operand_rejected_in_measure(self, engine) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "ev", "measures": [{"formula": "count_distinct(month(code))", "name": "m"}],
        })
        with pytest.raises(DateOperandTypeError):
            await engine.execute(query)

    @pytest.mark.parametrize("filt", [
        "month() >= '2024-01-01'",
        "month(ts, d) >= '2024-01-01'",
        "month(upper(code)) >= '2024-01-01'",
        "month(*) >= '2024-01-01'",
        "month(ts, granularity=2) >= '2024-01-01'",
    ])
    async def test_wrong_shape_rejected(self, engine, filt) -> None:
        with pytest.raises(GranularityCallError) as ei:
            await ids(engine, filt)
        msg = str(ei.value)
        for gran in GRANULARITIES:
            assert gran in msg, f"error must name granularity {gran!r}: {msg}"
        assert "(col" in msg, msg


class TestDateFunctionGrammar:
    async def test_minute_precision_instant_in_a_date_position(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["id", {"expression": "date_add('2024-01-31 10:15', 1, 'month')", "name": "v"}],
            "filters": ["id = 1"],
        }))
        assert bucket_text(resp.data[0]["ev.v"]) == "2024-02-29 10:15:00"
