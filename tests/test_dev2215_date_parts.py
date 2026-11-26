"""Date-part names are extraction; ``date_trunc`` is the only bucket spelling (specs: queries/date-functions,
queries/time-dimensions, queries/time-points, queries/transforms, queries/time-spine, queries/custom-granularities,
usage-reporting/collection)."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

import pytest

from slayer.core.errors import DateOperandTypeError, SlayerError, TimeDimensionFieldError, UnknownGranularityError
from slayer.core.models import Aggregation
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.usage_reporting.features import query_features
from tests._dev2215_fixtures import (
    BACKENDS,
    BUILT_IN_UNITS,
    EXTRACTIONS,
    ROW3_PARTS,
    count_by,
    day,
    dev2215_engine,
    dev2215_datasets,
    ids,
    m,
    num,
    q,
    single,
    sorted_rows,
)

COUNT = [m("count(orders.*)", "n")]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with dev2215_engine(request.param) as eng:
        yield eng


def _remedy(text: str) -> str:
    return re.escape(text)


def _tds(query: SlayerQuery) -> list[tuple[str, str]]:
    return [(str(t.field), str(getattr(t.granularity, "value", t.granularity)))
            for t in query.time_dimensions or [] if isinstance(t, TimeDimension)]


async def _ids_where(engine: SlayerQueryEngine, *filters: str) -> set[int]:
    return ids(await engine.execute(q(dimensions=["orders.id"], filter=list(filters))))


# --------------------------------------------------------------------------- #
# queries/date-functions
# --------------------------------------------------------------------------- #
class TestExtraction:
    async def test_month_call_is_month_of_year(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["month(orders.created_at)"], metrics=COUNT))
        (key,) = [c for c in resp.columns if c != "n"]
        assert {int(r[key]): int(r["n"]) for r in resp.data} == {3: 3, 4: 1, 12: 1, 6: 1}

    @pytest.mark.parametrize("part", EXTRACTIONS)
    async def test_extraction_equals_its_date_part_twin(self, engine: SlayerQueryEngine, part: str) -> None:
        alias = await engine.execute(q(dimensions=["orders.id", {"field": f"{part}(orders.created_at)", "alias": "x"}]))
        twin = await engine.execute(q(dimensions=["orders.id", {"field": f"date_part('{part}', orders.created_at)", "alias": "x"}]))
        assert alias.columns == twin.columns
        assert alias.sql == twin.sql
        assert sorted_rows(alias) == sorted_rows(twin)
        assert int(next(r["x"] for r in alias.data if int(r["orders.id"]) == 3)) == ROW3_PARTS[part]

    @pytest.mark.parametrize("part", EXTRACTIONS)
    async def test_unnamed_extraction_shares_the_twin_key(self, engine: SlayerQueryEngine, part: str) -> None:
        alias = await engine.execute(q(dimensions=[f"{part}(orders.created_at)"], metrics=COUNT))
        twin = await engine.execute(q(dimensions=[f"date_part('{part}', orders.created_at)"], metrics=COUNT))
        assert alias.columns == twin.columns
        assert alias.sql == twin.sql

    @pytest.mark.parametrize("part", EXTRACTIONS)
    @pytest.mark.parametrize("position", ["filter", "aggregation", "order"])
    async def test_extraction_in_every_position(self, engine: SlayerQueryEngine, part: str, position: str) -> None:
        def build(expr: str) -> SlayerQuery:
            if position == "filter":
                return q(dimensions=["orders.id"], filter=[f"{expr} >= 1"])
            if position == "aggregation":
                return q(metrics=[m(f"max({expr})")])
            return q(dimensions=["orders.id"], order=[{"field": expr, "direction": "desc"}, {"field": "orders.id", "direction": "asc"}])
        alias = await engine.execute(build(f"{part}(orders.created_at)"))
        twin = await engine.execute(build(f"date_part('{part}', orders.created_at)"))
        assert alias.sql == twin.sql
        assert alias.data == twin.data

    async def test_upper_case_extraction(self, engine: SlayerQueryEngine) -> None:
        assert await _ids_where(engine, "MONTH(orders.created_at) = 3") == {1, 4, 5}

    async def test_week_is_the_iso_week_number(self, engine: SlayerQueryEngine) -> None:
        assert await _ids_where(engine, "week(orders.created_at) = 1") == await _ids_where(
            engine, "date_part('week', orders.created_at) = 1") == {3}

    async def test_week_call_and_week_bucket(self, engine: SlayerQueryEngine) -> None:
        bucket = await engine.execute(q(dimensions=["date_trunc('week', orders.created_at)"], metrics=COUNT))
        assert "orders.created_at" in bucket.columns
        assert "2024-12-30" in {day(r["orders.created_at"]) for r in bucket.data}
        number = await engine.execute(q(dimensions=[{"field": "week(orders.created_at)", "alias": "w"}], metrics=COUNT))
        assert count_by(number, "w") == {10: 1, 15: 1, 1: 1, 12: 1, 13: 1, 23: 1}

    async def test_wrong_extraction_arity(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["orders.id"], filter=["month(created_at, 1) = 1"])
        with pytest.raises(SlayerError, match="month"):
            await engine.execute(query)


class TestDateTrunc:
    async def test_row_value(self, engine: SlayerQueryEngine) -> None:
        assert await _ids_where(engine, "date_trunc('month', orders.created_at) = '2025-03'") == {4, 5}

    async def test_time_dimension_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["date_trunc('month', orders.created_at)"], metrics=COUNT))
        assert resp.columns == ["orders.created_at", "n"]
        assert {day(r["orders.created_at"]) for r in resp.data} == {
            "2024-03-01", "2024-04-01", "2024-12-01", "2025-03-01", "2025-06-01"}

    async def test_unknown_unit(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["date_trunc('fortnight', orders.created_at)"], metrics=COUNT)
        with pytest.raises(UnknownGranularityError):
            await engine.execute(query)

    async def test_sunday_weeks_and_custom_units(self, engine: SlayerQueryEngine) -> None:
        sunday = await engine.execute(q(dimensions=["date_trunc('week_sunday', orders.created_at)"], metrics=COUNT))
        assert "2024-12-29" in {day(r["orders.created_at"]) for r in sunday.data}
        fiscal = await engine.execute(q(dimensions=["date_trunc('FISCAL_YEAR', orders.order_date)"], metrics=COUNT))
        assert {day(r["orders.order_date"]): int(r["n"]) for r in fiscal.data} == {
            "2023-04-01": 1, "2024-04-01": 4, "2025-04-01": 1}

    async def test_unit_never_captured_by_an_aggregation(self) -> None:
        models = dev2215_datasets(aggregations=[Aggregation(name="fiscal_year", expression="SUM({value}) * 10")])
        async with dev2215_engine("sqlite", datasets=models) as eng:
            resp = await eng.execute(q(dimensions=["date_trunc('fiscal_year', orders.order_date)"],
                                       metrics=[m("fiscal_year(orders.amount)")]))
        assert {day(r["orders.order_date"]): num(r["v"]) for r in resp.data} == {
            "2023-04-01": 100.0, "2024-04-01": 900.0, "2025-04-01": 600.0}


class TestGranularityNamesHaveNoCallForm:
    async def test_sunday_week_call(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["week_sunday(orders.created_at)"], metrics=COUNT)
        with pytest.raises(SlayerError, match=_remedy("date_trunc('week_sunday', orders.created_at)")):
            await engine.execute(query, dry_run=True)

    async def test_custom_granularity_call_in_a_filter(self, engine: SlayerQueryEngine) -> None:
        query = q(metrics=COUNT, filter=["fiscal_year(orders.order_date) = '2024'"])
        with pytest.raises(SlayerError, match=_remedy("date_trunc('fiscal_year', orders.order_date)")):
            await engine.execute(query, dry_run=True)

    async def test_custom_granularity_dimension_entry(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["fiscal_year(orders.order_date)"], metrics=COUNT)
        with pytest.raises(SlayerError, match=_remedy("date_trunc('fiscal_year', orders.order_date)")):
            await engine.execute(query, dry_run=True)


class TestDateShapedLiteral:
    async def test_month_compared_with_a_month_literal(self, engine: SlayerQueryEngine) -> None:
        query = q(metrics=COUNT, filter=["month(orders.created_at) = '2025-03'"])
        with pytest.raises(SlayerError) as err:
            await engine.execute(query, dry_run=True)
        assert "'2025-03'" in str(err.value)
        assert "date_trunc('month', created_at) = '2025-03'" in str(err.value)

    @pytest.mark.parametrize("text", [
        "date_part('year', orders.created_at) = 'this year'",
        "month(orders.created_at) BETWEEN '2025-01' AND '2025-03'",
        "month(orders.created_at) IN ('2025-03', '2025-04')",
        "day(orders.created_at) <> '2025-03-17'",
        "quarter(orders.created_at) >= '2025-Q1'",
        "week(orders.created_at) < '2025-W10'",
        "hour(orders.created_at) = '2025-03-17 12:00:00'",
    ])
    async def test_date_shaped_literal_refused(self, engine: SlayerQueryEngine, text: str) -> None:
        query = q(metrics=COUNT, filter=[text])
        with pytest.raises(SlayerError, match="date_trunc"):
            await engine.execute(query, dry_run=True)

    async def test_numeric_strings_stay_legal(self, engine: SlayerQueryEngine) -> None:
        assert await _ids_where(engine, "year(orders.created_at) = '2025'") == {4, 5, 6}
        assert await _ids_where(engine, "month(orders.created_at) IN ('03', '04')") == {1, 2, 4, 5}


# --------------------------------------------------------------------------- #
# queries/time-dimensions
# --------------------------------------------------------------------------- #
class TestFunctionalTimeDimensions:
    @pytest.mark.parametrize("unit", BUILT_IN_UNITS)
    def test_every_built_in_unit_is_rewritten(self, unit: str) -> None:
        query = q(dimensions=[f"date_trunc('{unit}', orders.created_at)"])
        assert not query.dimensions
        assert _tds(query) == [("orders.created_at", unit)]

    def test_case_insensitive_call_and_dotted_path(self) -> None:
        assert _tds(q(dimensions=["DATE_TRUNC('MONTH', customers.updated_at)"])) == [("customers.updated_at", "month")]

    @pytest.mark.parametrize("part", EXTRACTIONS)
    def test_extraction_dimension_is_not_rewritten(self, part: str) -> None:
        query = q(dimensions=[f"{part}(orders.created_at)"], metrics=COUNT)
        assert not query.time_dimensions
        assert len(query.dimensions or []) == 1

    async def test_extraction_dimension_executes_without_warning(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["month(orders.created_at)"], metrics=COUNT))
        assert resp.warnings == []
        assert sorted(Counter(int(v) for r in resp.data for k, v in r.items() if k != "n")) == [3, 4, 6, 12]

    async def test_datasource_granularity_unit(self, engine: SlayerQueryEngine) -> None:
        functional = await engine.execute(q(dimensions=["date_trunc('fiscal_year', orders.created_at)"], metrics=COUNT))
        explicit = await engine.execute(q(time_dimensions=[{"field": "orders.created_at", "granularity": "fiscal_year"}],
                                          metrics=COUNT))
        assert functional.sql == explicit.sql
        assert sorted_rows(functional) == sorted_rows(explicit)

    async def test_string_time_dimension_entry_coerces(self, engine: SlayerQueryEngine) -> None:
        functional = await engine.execute(q(time_dimensions=["date_trunc('month', orders.created_at)"], metrics=COUNT))
        explicit = await engine.execute(q(time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
                                          metrics=COUNT))
        assert functional.sql == explicit.sql

    @pytest.mark.parametrize(("entry", "remedy"), [
        ("month(orders.created_at)", "date_trunc('month', orders.created_at)"),
        ("week_sunday(orders.created_at)", "date_trunc('week_sunday', orders.created_at)"),
        ("fiscal_year(created_at)", "date_trunc('fiscal_year', created_at)"),
        ("year(coalesce(shipped_at, created_at))", "date_trunc('year', coalesce(shipped_at, created_at))"),
    ])
    def test_call_string_time_dimension_rejected_with_the_remedy(self, entry: str, remedy: str) -> None:
        with pytest.raises(ValueError, match=_remedy(remedy)):
            q(time_dimensions=[entry])

    def test_bare_column_time_dimension_names_the_form(self) -> None:
        with pytest.raises(ValueError, match=_remedy("date_trunc('<unit>', expr)")):
            q(time_dimensions=["orders.created_at"])

    async def test_unknown_unit_lists_the_datasource_granularities(self, engine: SlayerQueryEngine) -> None:
        query = q(time_dimensions=["date_trunc('mnth', orders.created_at)"], metrics=COUNT)
        with pytest.raises(UnknownGranularityError) as err:
            await engine.execute(query)
        assert "fiscal_year" in str(err.value)
        assert all(u in str(err.value) for u in BUILT_IN_UNITS)

    @pytest.mark.parametrize("entry", [
        "date_trunc('month')", "date_trunc('month', created_at, shipped_at)", "date_trunc('month', *)",
        "date_trunc(status, created_at)",
    ])
    def test_wrong_shape_names_the_date_trunc_form(self, entry: str) -> None:
        with pytest.raises(ValueError, match=_remedy("date_trunc('<unit>', expr)")):
            q(dimensions=[entry])

    async def test_non_temporal_argument(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["date_trunc('month', upper(orders.status))"], metrics=COUNT)
        with pytest.raises(TimeDimensionFieldError, match="String"):
            await engine.execute(query)

    async def test_unknown_single_column_call(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["mnth(orders.created_at)"], metrics=COUNT)
        with pytest.raises(SlayerError) as err:
            await engine.execute(query)
        text = str(err.value)
        assert "date_trunc('<unit>', expr)" in text
        assert "fiscal_year" in text
        assert "partition_by" in text
        assert all(u in text for u in BUILT_IN_UNITS)


class TestTimeDimensionKeysAndOrder:
    async def test_two_granularities_of_one_column(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["date_trunc('month', orders.created_at)", "date_trunc('year', orders.created_at)"],
                                      metrics=COUNT))
        assert {"orders.created_at.month", "orders.created_at.year"} <= set(resp.columns)

    async def test_ambiguous_main_time_dimension_names_date_trunc_candidates(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["date_trunc('month', orders.created_at)", "date_trunc('year', orders.created_at)"],
                  main_time_dimension="orders.created_at", metrics=[m("cumsum(count(orders.*))")])
        with pytest.raises(SlayerError) as err:
            await engine.execute(query)
        assert "date_trunc('month', orders.created_at)" in str(err.value)
        assert "date_trunc('year', orders.created_at)" in str(err.value)

    async def test_order_by_projected_bucket(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["date_trunc('month', orders.created_at)"], metrics=COUNT,
                                      order=[{"field": "date_trunc('month', orders.created_at)", "direction": "desc"}]))
        assert [day(r["orders.created_at"]) for r in resp.data] == [
            "2025-06-01", "2025-03-01", "2024-12-01", "2024-04-01", "2024-03-01"]

    async def test_order_by_bucket_without_matching_time_dimension(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["date_trunc('year', orders.created_at)"], metrics=COUNT,
                  order=[{"field": "date_trunc('month', orders.created_at)", "direction": "desc"}])
        with pytest.raises(SlayerError, match="date_trunc"):
            await engine.execute(query)

    async def test_extraction_order_key_is_an_ordinary_expression(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=[{"field": "month(orders.created_at)", "alias": "mo"}], metrics=COUNT,
                                      order=[{"field": "month(orders.created_at)", "direction": "asc"}]))
        assert [int(r["mo"]) for r in resp.data] == [3, 4, 6, 12]

    async def test_stage_order_by_bucket(self, engine: SlayerQueryEngine) -> None:
        stages: list[Any] = [
            {"name": "inner", "population": "orders",
             "time_dimensions": [{"field": "orders.created_at", "granularity": "day"}], "metrics": COUNT},
            {"population": "inner", "time_dimensions": [{"field": "inner.created_at", "granularity": "month"}],
             "metrics": [m("sum(inner.n)", "t")], "order": [{"field": "date_trunc('month', inner.created_at)", "direction": "desc"}]},
        ]
        resp = await engine.execute(stages)
        months = [str(day(r["inner.created_at"])) for r in resp.data]
        assert months == sorted(months, reverse=True)


# --------------------------------------------------------------------------- #
# queries/time-points
# --------------------------------------------------------------------------- #
class TestDateTruncRowValue:
    async def test_filter_bound(self, engine: SlayerQueryEngine) -> None:
        assert await _ids_where(engine, "date_trunc('month', orders.created_at) >= '2024-03-15'") == {2, 3, 4, 5, 6}

    async def test_buckets_compared_with_each_other(self, engine: SlayerQueryEngine) -> None:
        assert await _ids_where(engine, "date_trunc('month', orders.shipped_at) = date_trunc('month', orders.created_at)") == {
            1, 2, 4, 6}

    async def test_measure_and_computed_dimension(self, engine: SlayerQueryEngine) -> None:
        measure = await engine.execute(q(metrics=[m("count_distinct(date_trunc('month', orders.created_at))")]))
        assert int(single(measure)) == 5
        dim = await engine.execute(q(dimensions=[{"field": "date_trunc('year', orders.created_at)", "alias": "y"}],
                                     metrics=COUNT))
        assert {day(r["y"]): int(r["n"]) for r in dim.data} == {"2024-01-01": 3, "2025-01-01": 3}

    @pytest.mark.parametrize("text", [
        "date_trunc('month') >= '2024-01-01'", "date_trunc('month', created_at, shipped_at) >= '2024-01-01'",
        "date_trunc(orders.status, orders.created_at) >= '2024-01-01'",
    ])
    async def test_wrong_shape_in_a_filter(self, engine: SlayerQueryEngine, text: str) -> None:
        query = q(metrics=COUNT, filter=[text])
        with pytest.raises(ValueError, match="date_trunc"):
            await engine.execute(query)

    async def test_non_temporal_argument_in_a_filter(self, engine: SlayerQueryEngine) -> None:
        query = q(metrics=COUNT, filter=["date_trunc('month', upper(orders.status)) >= '2024-01-01'"])
        with pytest.raises(DateOperandTypeError, match=re.escape("upper(status)")):
            await engine.execute(query)

    async def test_bucket_against_an_instant(self, engine: SlayerQueryEngine) -> None:
        assert await _ids_where(engine, "date_trunc('month', orders.created_at) = '2025-03-15 10:00:00'") == set()
        assert await _ids_where(engine, "date_trunc('month', orders.created_at) = '2025-03-01 00:00:00'") == {4, 5}

    async def test_whole_periods_only_with_only_an_extraction(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["month(orders.created_at)"], metrics=COUNT, whole_periods_only=True)
        with pytest.raises(SlayerError, match=_remedy("date_trunc('month', created_at)")):
            await engine.execute(query, dry_run=True)


# --------------------------------------------------------------------------- #
# queries/transforms, queries/time-spine
# --------------------------------------------------------------------------- #
class TestBucketRequiredRemedy:
    async def _no_axis_error(self, engine: SlayerQueryEngine) -> type[BaseException]:
        query = q(dimensions=["orders.status"], metrics=[m("cumsum(sum(orders.amount))")])
        with pytest.raises(SlayerError) as err:
            await engine.execute(query, dry_run=True)
        assert "date_trunc" not in str(err.value)
        return type(err.value)

    @pytest.mark.parametrize(("dimension", "measure", "remedy"), [
        ("month(orders.created_at)", "cumsum(sum(orders.amount))", "date_trunc('month', created_at)"),
        ("year(orders.created_at)", "change(sum(orders.amount))", "date_trunc('year', created_at)"),
        ("year(orders.created_at)", "last(sum(orders.amount))", "date_trunc('year', created_at)"),
        ("year(orders.created_at)", "sum(orders.amount, window='3m')", "date_trunc('year', created_at)"),
        ("date_part('quarter', orders.created_at)", "time_shift(sum(orders.amount), -1)", "date_trunc('quarter', created_at)"),
    ])
    async def test_no_time_dimension_names_the_remedy(self, engine: SlayerQueryEngine, dimension: str, measure: str,
                                                      remedy: str) -> None:
        expected = await self._no_axis_error(engine)
        query = q(dimensions=[dimension], metrics=[m(measure)])
        with pytest.raises(expected, match=_remedy(remedy)):
            await engine.execute(query, dry_run=True)

    async def test_the_remedy_executes(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["date_trunc('month', orders.created_at)"],
                                      metrics=[m("cumsum(sum(orders.amount))")]))
        got = {day(r["orders.created_at"]): num(r["v"]) for r in resp.data}
        assert got == {"2024-03-01": 10.0, "2024-04-01": 30.0, "2024-12-01": 60.0, "2025-03-01": 100.0,
                       "2025-06-01": 160.0}

    async def test_spine_extraction_dimension(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["month(time_spine.timestamp)"], metrics=COUNT,
                  filter=["time_spine.timestamp >= '2024-01-01'"])
        with pytest.raises(SlayerError, match=_remedy("date_trunc('month', time_spine.timestamp)")):
            await engine.execute(query, dry_run=True)

    async def test_spine_extraction_time_dimension(self, engine: SlayerQueryEngine) -> None:
        query = q(time_dimensions=[{"field": "month(time_spine.timestamp)", "granularity": "month",
                                    "date_range": ["2024-01-01", "2025-12-31"]}], metrics=COUNT)
        with pytest.raises(SlayerError, match=_remedy("date_trunc('month', time_spine.timestamp)")):
            await engine.execute(query, dry_run=True)


# --------------------------------------------------------------------------- #
# queries/custom-granularities
# --------------------------------------------------------------------------- #
class TestCustomGranularityUnits:
    async def test_functional_form_and_order_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["date_trunc('fiscal_year', orders.order_date)"], metrics=[m("sum(orders.amount)")],
                                      order=[{"field": "date_trunc('fiscal_year', orders.order_date)", "direction": "desc"}]))
        assert [(day(r["orders.order_date"]), num(r["v"])) for r in resp.data] == [
            ("2025-04-01", 60.0), ("2024-04-01", 90.0), ("2023-04-01", 10.0)]

    async def test_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[m("sum(orders.amount)")],
                                      filter=["date_trunc('fiscal_year', orders.order_date) = '2024-04-01'"]))
        assert num(single(resp)) == 90.0

    async def test_unit_case_insensitive(self, engine: SlayerQueryEngine) -> None:
        lower = await engine.execute(q(dimensions=["date_trunc('fiscal_year', orders.order_date)"], metrics=[m("sum(orders.amount)")]))
        mixed = await engine.execute(q(dimensions=["date_trunc('Fiscal_Year', orders.order_date)"], metrics=[m("sum(orders.amount)")]))
        assert lower.columns == mixed.columns
        assert sorted_rows(lower) == sorted_rows(mixed)

    @pytest.mark.parametrize("entry", ["date_trunc('mnth', orders.order_date)", "mnth(orders.order_date)"])
    async def test_unknown_name(self, engine: SlayerQueryEngine, entry: str) -> None:
        query = q(dimensions=[entry], metrics=COUNT)
        with pytest.raises(SlayerError, match="fiscal_year"):
            await engine.execute(query)


# --------------------------------------------------------------------------- #
# usage-reporting/collection
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("body", [
    {"dimensions": [{"alias": "m", "field": "month(ordered_at)"}], "metrics": ["count(orders.*)"]},
    {"metrics": ["count(orders.*)"], "filter": ["year(ordered_at) = 2024"]},
    {"metrics": ["max(second(ordered_at))"]},
])
def test_extraction_calls_count_as_date_part(body: dict[str, Any]) -> None:
    assert Counter(query_features({"population": "orders", **body}).functions)["date_part"] == 1


def test_date_trunc_is_not_counted() -> None:
    found = query_features({"population": "orders", "metrics": ["count(orders.*)"],
                            "filter": ["date_trunc('month', orders.ordered_at) = '2024-01-01'"]})
    assert found.functions == []
