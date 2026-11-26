"""What a time of day can do: dimension, order, comparisons, conditionals, aggregations, the three parts, and the
refusals (spec: queries/time-of-day, queries/date-functions › Date operands must be temporal)."""

from __future__ import annotations

from datetime import time
from typing import Any

import pytest

from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, TEMPORAL_DATATYPES, DataType
from slayer.core.errors import (
    DateOperandTypeError, DefaultTimeDimensionTypeError, SlayerError, TimeLiteralError,
)
from slayer.core.models import DatasetField
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from tests._time_of_day_fixtures import (
    ALL_IDS, BACKENDS, NY, d, emit, filtered_ids, is_time, m, planned_type, q, shifts_dataset, shifts_engine, value_of,
)

START = "shifts.starts_at"
END = "shifts.ends_at"
#: A filter between times of day and the shifts it keeps.
COMPARISONS: list[tuple[str, set[int]]] = [
    (f"{START} = TIME '09:30'", {1, 2}),
    (f"{START} <> TIME '09:30'", {3}),
    (f"{START} != TIME '09:30:00'", {3}),
    (f"{START} < TIME '17:00'", {1, 2}),
    (f"{START} <= TIME '17:00'", {1, 2, 3}),
    (f"{START} > TIME '09:30'", {3}),
    (f"{START} >= TIME '17:00:00'", {3}),
    (f"TIME '12:00' > {START}", {1, 2}),
    (f"{START} IN (TIME '09:30', TIME '12:00')", {1, 2}),
    (f"{START} NOT IN (TIME '09:30')", {3}),
    (f"{START} BETWEEN TIME '09:00' AND TIME '12:00'", {1, 2}),
    (f"{START} NOT BETWEEN TIME '09:00' AND TIME '12:00'", {3}),
    (f"{END} IS NULL", {2}),
    (f"{END} IS NOT NULL", {1, 3}),
    (f"{END} IS DISTINCT FROM TIME '17:00'", {2, 3}),
    (f"{END} IS NOT DISTINCT FROM TIME '17:00'", {1}),
    (f"{START} < {END}", {1, 3}),
    (f"{END} > TIME '23:00:00'", {3}),
    (f"{END} = TIME '23:00:00.25'", {3}),
    (f"{START} < current_time() OR {START} >= current_time()", {1, 2, 3}),
]
#: A string in a comparison slot and the same filter with ``TIME`` literals.
STRING_SLOTS: list[tuple[str, str, set[int]]] = [
    (f"{START} > '09:00'", f"{START} > TIME '09:00'", {1, 2, 3}),
    (f"{START} = '09:30'", f"{START} = TIME '09:30'", {1, 2}),
    (f"{START} <> '09:30:00'", f"{START} <> TIME '09:30:00'", {3}),
    (f"{START} < '17:00'", f"{START} < TIME '17:00'", {1, 2}),
    (f"{START} <= '09:30:00.000'", f"{START} <= TIME '09:30:00.000'", {1, 2}),
    (f"{START} >= '17:00'", f"{START} >= TIME '17:00'", {3}),
    (f"'09:30' = {START}", f"TIME '09:30' = {START}", {1, 2}),
    (f"{START} IN ('09:30:00', '17:00')", f"{START} IN (TIME '09:30:00', TIME '17:00')", {1, 2, 3}),
    (f"{START} NOT IN ('09:30')", f"{START} NOT IN (TIME '09:30')", {3}),
    (f"{START} BETWEEN '09:00' AND '12:00:00'", f"{START} BETWEEN TIME '09:00' AND TIME '12:00:00'", {1, 2}),
    (f"{START} IS DISTINCT FROM '09:30'", f"{START} IS DISTINCT FROM TIME '09:30'", {3}),
    (f"{END} IS NOT DISTINCT FROM '17:00'", f"{END} IS NOT DISTINCT FROM TIME '17:00'", {1}),
    (f"nullif({START}, '09:30') IS NULL", f"nullif({START}, TIME '09:30') IS NULL", {1, 2}),
    ("CAST(shifts.created_at AS TIME) > '09:00'", "CAST(shifts.created_at AS TIME) > TIME '09:00'", {1, 3}),
    (f"TIME '09:30' = '09:30' AND {START} > '00:00'", f"{START} > TIME '00:00'", {1, 2, 3}),
    (f"min({START}) >= '09:30'", f"min({START}) >= TIME '09:30'", {1, 2, 3}),
]
NOT_A_TIME = [
    (f"{START} > 'noon'", "noon"), (f"{START} IN ('09:00', '2024-01-15')", "2024-01-15"),
    (f"{START} BETWEEN '09:00' AND '25:00'", "25:00"), (f"{START} = '10:30:00+02:00'", "10:30:00+02:00"),
    (f"{START} IS DISTINCT FROM 'never'", "never"), (f"nullif({START}, 'x') IS NULL", "x"),
    (f"'24:00:00' < {START}", "24:00:00"), ("current_time() > 'now'", "now"),
]
#: A refused use and the operand its error names.
REFUSED = [
    (d(f"{START} + 1"), "starts_at"), (d(f"{START} - {END}"), "starts_at"), (d(f"{START} * 2"), "starts_at"),
    (d(f"-{START}"), "starts_at"), (d("TIME '10:00' + 1"), "10:00"),
    (m(f"sum({START})"), "starts_at"), (m(f"avg({START})"), "starts_at"), (m(f"median({START})"), "starts_at"),
    (d(f"{START} > shifts.created_at"), "starts_at"), (d(f"{START} = shifts.order_date"), "starts_at"),
    (d(f"coalesce({START}, 0)"), "starts_at"), (d(f"coalesce({START}, shifts.created_at)"), "starts_at"),
    (d(f"greatest({START}, shifts.order_date)"), "starts_at"), (d(f"iif(shifts.id = 1, {START}, 'x')"), "starts_at"),
    (d(f"CASE WHEN shifts.id = 1 THEN {START} ELSE shifts.created_at END"), "starts_at"),
]
PARTS = [
    (f"date_part('hour', {START})", 9), (f"date_part('minute', {START})", 30), (f"date_part('second', {START})", 0),
    (f"EXTRACT(HOUR FROM {START})", 9), (f"EXTRACT(MINUTE FROM {START})", 30), (f"EXTRACT(SECOND FROM {START})", 0),
    (f"DATE_PART('hour', {START})", 9), (f"HOUR({START})", 9), (f"MINUTE({START})", 30), (f"SECOND({START})", 0),
    ("SECOND(TIME '10:30:45')", 45), ("hour(TIME '23:59:59.999999')", 23), ("second(TIME '23:59:59.999999')", 59),
    ("date_part('minute', CAST(shifts.created_at AS TIME))", 30),
    ("hour(coalesce(shifts.ends_at, TIME '01:02:03'))", 17),
]
REFUSED_DATE_FUNCTIONS = [
    f"date_trunc('hour', {START})", f"date_add({START}, 1, 'hour')", f"date_diff('minute', {START}, {END})",
    f"date_diff('minute', shifts.created_at, {START})", f"date_part('year', {START})", f"date_part('day', {START})",
    f"EXTRACT(MONTH FROM {START})", f"YEAR({START})", f"{START} + INTERVAL '1 hour'", f"{START} - INTERVAL 2 MINUTE",
    "date_trunc('minute', TIME '10:30:45')", "date_part('quarter', current_time())",
]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with shifts_engine(request.param) as eng:
        yield eng


@pytest.fixture
def data_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    real = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await real(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def _data(calls: list[str]) -> list[str]:
    return [s for s in calls if "slayer_rk_" not in s]


def _entry(entry: dict[str, str]) -> dict[str, Any]:
    return {"metrics": [entry]} if "metric" in entry else {"dimensions": [entry]}


class TestTypeShape:
    def test_time_is_not_temporal(self) -> None:
        assert DataType.TIME not in TEMPORAL_DATATYPES
        assert DataType.TIME.is_temporal is False
        assert DataType.TIME.is_opaque is False

    def test_default_aggregations(self) -> None:
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.TIME] == frozenset({
            "count", "count_distinct", "count_distinct_approx", "first", "last", "min", "max", "collect"})

    @pytest.mark.parametrize("text", [
        START, f"min({START})", f"max({START})", f"first({START}, shifts.created_at)",
        f"last({START}, shifts.created_at)", f"coalesce({END}, TIME '23:59:59')", f"ifnull({END}, {START})",
        f"greatest({START}, TIME '10:00')", f"least({START}, {END})", f"nullif({START}, TIME '09:30')",
        f"iif(shifts.id = 1, {START}, {END})", f"CASE WHEN shifts.id = 1 THEN {START} ELSE NULL END",
        f"coalesce(NULL, {START})",
    ])
    def test_stays_a_time_of_day(self, text: str) -> None:
        assert planned_type(text) is DataType.TIME

    @pytest.mark.parametrize("text", [f"count({START})", f"count_distinct({START})", f"count_distinct_approx({START})",
                                      f"date_part('hour', {START})", f"EXTRACT(MINUTE FROM {START})",
                                      "SECOND(TIME '10:30:45')"])
    def test_counts_and_parts_are_integers(self, text: str) -> None:
        assert planned_type(text) is DataType.INTEGER

    def test_collect_is_a_list(self) -> None:
        assert planned_type(f"collect({START})") is DataType.ARRAY


class TestDimensionOrderAggregate:
    async def test_a_field_and_a_literal_behave_alike(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=[START], filter=[f"{START} >= TIME '09:00:00'"]))
        values = [r[START] for r in resp.data]
        assert all(type(v) is time for v in values), values
        assert sorted(values) == [time(9, 30), time(17, 0)]

    async def test_group_order_and_aggregate(self, engine: SlayerQueryEngine) -> None:
        metrics = [m("count(shifts.*)", "n"), m(f"min({START})", "lo"), m(f"max({START})", "hi")]
        resp = await engine.execute(q(dimensions=[START], metrics=metrics, order=[{"field": START}]))
        assert [(r[START], int(r["n"])) for r in resp.data] == [(time(9, 30), 2), (time(17, 0), 1)]
        total = await engine.execute(q(metrics=metrics))
        [row] = total.data
        assert is_time(row["lo"], time(9, 30)), row
        assert is_time(row["hi"], time(17, 0)), row

    @pytest.mark.parametrize(("direction", "expected"), [("asc", [1, 3, 2]), ("desc", [3, 1, 2])])
    async def test_order_key_is_clock_time(self, engine: SlayerQueryEngine, direction: str, expected: list[int]) -> None:
        resp = await engine.execute(q(dimensions=["shifts.id"],
                                      order=[{"field": END, "direction": direction, "nulls": "last"}]))
        assert [int(r["shifts.id"]) for r in resp.data] == expected

    async def test_counts_and_first_last(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[
            m(f"count({END})", "n"), m(f"count_distinct({START})", "nd"), m(f"count_distinct_approx({START})", "na"),
            m(f"first({START}, shifts.created_at)", "f"), m(f"last({START}, shifts.created_at)", "l")]))
        [row] = resp.data
        assert (int(row["n"]), int(row["nd"]), int(row["na"])) == (2, 2, 2)
        assert is_time(row["f"], time(9, 30)), row
        assert is_time(row["l"], time(17, 0)), row

    async def test_collect_where_the_database_has_lists(self) -> None:
        async with shifts_engine("duckdb") as eng:
            resp = await eng.execute(q(metrics=[m(f"collect({START})")]))
        assert sorted(resp.data[0]["v"]) == [time(9, 30), time(9, 30), time(17, 0)]

    async def test_a_time_metric_filters_and_orders(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["shifts.is_paid"], metrics=[m(f"max({START})", "hi")],
                                      filter=["hi > TIME '12:00'"]))
        assert len(resp.data) == 1, resp.data
        assert is_time(resp.data[0]["hi"], time(17, 0)), resp.data


class TestComparisons:
    @pytest.mark.parametrize(("filt", "expected"), COMPARISONS, ids=[f for f, _ in COMPARISONS])
    async def test_against_another_time_of_day(self, engine: SlayerQueryEngine, filt: str, expected: set[int]) -> None:
        assert await filtered_ids(engine, filt) == expected


class TestConditionals:
    @pytest.mark.parametrize(("text", "expected"), [
        (f"coalesce({END}, TIME '23:59:59')", {1: time(17, 0), 2: time(23, 59, 59), 3: time(23, 0, 0, 250000)}),
        (f"ifnull({END}, {START})", {1: time(17, 0), 2: time(9, 30), 3: time(23, 0, 0, 250000)}),
        (f"greatest({START}, TIME '10:00')", {1: time(10, 0), 2: time(10, 0), 3: time(17, 0)}),
        (f"least({START}, TIME '10:00')", {1: time(9, 30), 2: time(9, 30), 3: time(10, 0)}),
        (f"nullif({START}, TIME '09:30')", {1: None, 2: None, 3: time(17, 0)}),
        (f"iif(shifts.id = 1, TIME '08:00', {START})", {1: time(8, 0), 2: time(9, 30), 3: time(17, 0)}),
        (f"CASE WHEN {END} IS NULL THEN {START} ELSE {END} END",
         {1: time(17, 0), 2: time(9, 30), 3: time(23, 0, 0, 250000)}),
    ])
    async def test_value(self, engine: SlayerQueryEngine, text: str, expected: dict[int, time | None]) -> None:
        resp = await engine.execute(q(dimensions=["shifts.id", d(text)]))
        got = {int(r["shifts.id"]): r["v"] for r in resp.data}
        assert got == expected
        assert all(v is None or type(v) is time for v in got.values()), got


class TestRefusedOperations:
    @pytest.mark.parametrize(("entry", "operand"), REFUSED, ids=[next(iter(e.values())) for e, _ in REFUSED])
    async def test_refused_naming_the_operand_and_its_type(self, engine: SlayerQueryEngine, data_calls: list[str],
                                                           entry: dict[str, str], operand: str) -> None:
        with pytest.raises(SlayerError) as info:
            await engine.execute(q(**_entry(entry)))
        msg = str(info.value)
        assert operand in msg, msg
        assert "time" in msg.lower(), msg
        assert _data(data_calls) == []


class TestParts:
    @pytest.mark.parametrize(("text", "expected"), PARTS, ids=[t for t, _ in PARTS])
    async def test_sub_day_parts(self, engine: SlayerQueryEngine, text: str, expected: int) -> None:
        got = await value_of(engine, text)
        assert type(got) is not str, got
        assert int(got) == expected

    async def test_a_part_filters(self, engine: SlayerQueryEngine) -> None:
        assert await filtered_ids(engine, f"date_part('hour', {START}) = 17") == {3}

    @pytest.mark.parametrize("text", REFUSED_DATE_FUNCTIONS)
    async def test_every_other_date_function_refuses_it(self, engine: SlayerQueryEngine, data_calls: list[str],
                                                        text: str) -> None:
        with pytest.raises(DateOperandTypeError) as info:
            await value_of(engine, text)
        msg = str(info.value).lower()
        assert all(part in msg for part in ("hour", "minute", "second")), msg
        assert "time of day" in msg, msg
        assert _data(data_calls) == []


class TestNotATimeAxis:
    @pytest.mark.parametrize("entry", [
        {"field": START, "granularity": "hour"},
        {"field": START, "granularity": "day", "date_range": ["2024-01-01", "2024-02-01"]},
        {"field": f"coalesce({END}, {START})", "granularity": "hour", "alias": "t"},
    ])
    async def test_refused_as_a_time_dimension(self, engine: SlayerQueryEngine, data_calls: list[str],
                                               entry: dict[str, Any]) -> None:
        with pytest.raises(SlayerError) as info:
            await engine.execute(q(time_dimensions=[entry], metrics=[m("count(shifts.*)")]))
        msg = str(info.value)
        assert "starts_at" in msg, msg
        assert "Time" in msg, msg
        assert _data(data_calls) == []

    async def test_refused_as_a_default_time_dimension(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(DefaultTimeDimensionTypeError) as info:
            await engine.storage.save_dataset(shifts_dataset(default_time_dimension="starts_at"))
        assert "starts_at" in str(info.value)
        assert "Time" in str(info.value)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_lone_time_field_is_no_spine_axis(self, backend: str) -> None:
        dataset = shifts_dataset().model_copy(update={"fields": [
            f for f in shifts_dataset().fields if f.name in ("id", "starts_at", "ends_at")]})
        spine = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2024-01-01", "2024-02-29"]}
        async with shifts_engine(backend, dataset=dataset) as eng:
            with pytest.warns(UserWarning):
                resp = await eng.execute(q(time_dimensions=[spine], metrics=[m("count(shifts.*)", "n")]))
        assert [int(r["n"]) for r in resp.data] == [3, 3]
        assert "unrelated_dataset" in {w.kind for w in resp.warnings}

    async def test_last_does_not_rank_by_a_time_of_day(self, engine: SlayerQueryEngine) -> None:
        dataset = shifts_dataset().model_copy(update={"fields": [
            f for f in shifts_dataset().fields if f.name in ("id", "starts_at", "amount")]})
        await engine.storage.save_dataset(dataset)
        with pytest.raises(ValueError, match="ranking time field"):
            await engine.execute(q(metrics=[m("last(shifts.amount)")]))


class TestNoZoneConversion:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("zone", [NY, "Asia/Tokyo"])
    async def test_a_time_field_is_returned_as_stored(self, backend: str, zone: str) -> None:
        async with shifts_engine(backend, datasource_fields={"time_zone": "UTC"}) as eng:
            resp = await eng.execute(q(time_zone=zone, dimensions=["shifts.id", START, END]))
            kept = await filtered_ids(eng, f"{START} = TIME '09:30'", time_zone=zone)
        rows = {int(r["shifts.id"]): (r[START], r[END]) for r in resp.data}
        assert rows == {1: (time(9, 30), time(17, 0)), 2: (time(9, 30), None), 3: (time(17, 0), time(23, 0, 0, 250000))}
        assert kept == {1, 2}

    async def test_no_conversion_is_rendered(self) -> None:
        plain = await emit(q(dimensions=[START]), dialect="postgres", datasource_fields={"time_zone": "UTC"})
        zoned = await emit(q(dimensions=[START], time_zone=NY), dialect="postgres",
                           datasource_fields={"time_zone": "UTC"})
        assert zoned == plain


class TestStringIsATimeLiteral:
    @pytest.mark.parametrize(("text", "typed", "expected"), STRING_SLOTS, ids=[s for s, _, _ in STRING_SLOTS])
    async def test_every_slot(self, engine: SlayerQueryEngine, text: str, typed: str, expected: set[int]) -> None:
        if text.startswith("min("):
            query = {"dimensions": ["shifts.id"], "metrics": [m(f"min({START})", "lo")]}
            got = {int(r["shifts.id"]) for r in (await engine.execute(q(**query, filter=["lo >= '09:30'"]))).data}
            assert got == expected == ALL_IDS
            return
        assert await filtered_ids(engine, text) == await filtered_ids(engine, typed) == expected

    @pytest.mark.parametrize(("text", "typed", "_expected"), STRING_SLOTS[:-1], ids=[s for s, _, _ in STRING_SLOTS[:-1]])
    async def test_renders_as_the_typed_literal(self, text: str, typed: str, _expected: set[int]) -> None:
        assert await emit(q(dimensions=["shifts.id"], filter=[text]), dialect="postgres") == await emit(
            q(dimensions=["shifts.id"], filter=[typed]), dialect="postgres")

    async def test_nullif_as_a_value(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["shifts.id", d(f"nullif({START}, '09:30')")]))
        assert {int(r["shifts.id"]): r["v"] for r in resp.data} == {1: None, 2: None, 3: time(17, 0)}

    @pytest.mark.parametrize(("text", "named"), NOT_A_TIME, ids=[t for t, _ in NOT_A_TIME])
    async def test_a_string_that_names_no_time(self, engine: SlayerQueryEngine, data_calls: list[str], text: str,
                                               named: str) -> None:
        with pytest.raises(TimeLiteralError) as info:
            await filtered_ids(engine, text)
        assert named in str(info.value), str(info.value)
        assert _data(data_calls) == []

    async def test_a_string_stays_a_string_elsewhere(self, engine: SlayerQueryEngine) -> None:
        assert await filtered_ids(engine, "shifts.label = '09:30'") == set()
        assert await value_of(engine, "coalesce(shifts.label, '09:30')") == "a"

    async def test_a_field_named_like_a_part_is_unaffected(self, engine: SlayerQueryEngine) -> None:
        dataset = shifts_dataset(extra_fields=[DatasetField(name="slot", expression="label", datatype=DataType.STRING)])
        await engine.storage.save_dataset(dataset)
        assert await filtered_ids(engine, "shifts.slot = 'a'") == {1}


class TestNotASpineAxis:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize(("field", "type_name"), [("starts_at", "Time"), ("label", "String")])
    async def test_a_stored_non_temporal_default_is_refused(self, backend: str, data_calls: list[str], field: str,
                                                            type_name: str) -> None:
        spine = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2024-01-01", "2024-02-29"]}
        async with shifts_engine(backend) as eng:
            await eng.storage.save_dataset(shifts_dataset(default_time_dimension=field), _validate=False)
            with pytest.raises(DefaultTimeDimensionTypeError) as info:
                await eng.execute(q(time_dimensions=[spine], metrics=[m("count(shifts.*)")]))
        msg = str(info.value)
        assert all(part in msg for part in ("shifts", field, type_name)), msg
        assert _data(data_calls) == []
