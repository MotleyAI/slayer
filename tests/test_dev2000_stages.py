"""Stage zones: inheritance, pinned stages converting into their reader, output zone state, cross-zone re-bucketing
(spec: queries/time-zones; design D9–D11)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import TimeDimensionFieldError
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery

from tests._dev2000_fixtures import (
    BACKENDS,
    DS,
    NY,
    ROWS,
    TOKYO,
    UNDECLARED,
    d,
    day,
    default_datasets,
    local,
    m,
    naive,
    orders_dataset,
    to_zone,
    tz_engine,
    utc,
    warnings_of,
)

MONTH_TD = {"field": "orders.ordered_at", "granularity": "month"}
DAY_TD = {"field": "orders.ordered_at", "granularity": "day"}


def _slot(i: int) -> datetime:
    return datetime.fromisoformat(ROWS[i][2])


def _stage(name: str = "s1", **fields: Any) -> dict[str, Any]:
    return {"name": name, "population": "orders", **fields}


@pytest.mark.parametrize("backend", BACKENDS)
class TestInheritance:
    async def test_a_stage_zone_overrides_the_query_zone(self, backend) -> None:
        stage = _stage(time_zone=TOKYO, time_dimensions=[MONTH_TD], metrics=[m("sum(orders.amount)", "s")])
        final = {"population": "s1", "time_zone": NY, "dimensions": ["s1.ordered_at", "s1.s"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        got = {naive(r["s1.ordered_at"]): float(r["s1.s"]) for r in resp.data}
        months = {datetime(2025, 1, 1): 30.0, datetime(2025, 2, 1): 30.0, datetime(2025, 3, 1): 40.0}
        assert got == {to_zone(start, NY, source=TOKYO): total for start, total in months.items()}

    async def test_an_unpinned_stage_takes_the_query_zone(self, backend) -> None:
        stage = _stage(time_dimensions=[DAY_TD], metrics=[m("sum(orders.amount)", "s")])
        final = {"population": "s1", "time_zone": NY, "dimensions": ["s1.ordered_at", "s1.s"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        assert {day(r["s1.ordered_at"]) for r in resp.data} == {
            date(2024, 12, 31), date(2025, 1, 1), date(2025, 1, 31), date(2025, 3, 9)}

    async def test_a_query_backed_dataset_follows_the_reader(self, backend) -> None:
        daily = SlayerDataset(name="daily", data_source=DS, source_queries=[SlayerQuery.model_validate({
            "population": "orders", "time_dimensions": [DAY_TD], "metrics": [m("sum(orders.amount)", "s")]})])
        async with tz_engine(backend, datasets=[*default_datasets(), daily]) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "population": "daily", "time_zone": NY, "dimensions": ["daily.ordered_at"]}))
        assert {day(r["daily.ordered_at"]) for r in resp.data} == {
            date(2024, 12, 31), date(2025, 1, 1), date(2025, 1, 31), date(2025, 3, 9)}

    async def test_a_query_backed_dataset_keeps_its_pinned_zone(self, backend) -> None:
        daily = SlayerDataset(name="daily", data_source=DS, source_queries=[SlayerQuery.model_validate({
            "population": "orders", "time_zone": TOKYO, "time_dimensions": [DAY_TD],
            "metrics": [m("sum(orders.amount)", "s")]})])
        async with tz_engine(backend, datasets=[*default_datasets(), daily]) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "population": "daily", "time_zone": TOKYO, "dimensions": ["daily.ordered_at"]}))
        assert {day(r["daily.ordered_at"]) for r in resp.data} == {local(i, TOKYO).date() for i in ROWS}


@pytest.mark.parametrize("backend", BACKENDS)
class TestPinnedStageIntoReader:
    async def test_a_pinned_stage_converts_into_its_reader(self, backend) -> None:
        stage = _stage(time_zone=TOKYO, dimensions=["orders.id", "orders.ordered_at"])
        final = {"population": "s1", "time_zone": "UTC", "dimensions": ["s1.id", "s1.ordered_at"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        assert {int(r["s1.id"]): naive(r["s1.ordered_at"]) for r in resp.data} == {i: utc(i) for i in ROWS}

    async def test_a_pinned_stage_converts_into_a_reader_in_another_zone(self, backend) -> None:
        stage = _stage(time_zone=TOKYO, dimensions=["orders.id", "orders.ordered_at"])
        final = {"population": "s1", "time_zone": NY, "dimensions": ["s1.id", "s1.ordered_at"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        assert {int(r["s1.id"]): naive(r["s1.ordered_at"]) for r in resp.data} == {i: local(i, NY) for i in ROWS}

    async def test_a_repeated_hour_reconverts_as_the_later_occurrence(self, backend) -> None:
        stage = {"name": "s1", "population": "dst", "time_zone": NY, "dimensions": ["dst.id", "dst.instant_at"]}
        final = {"population": "s1", "time_zone": "UTC", "dimensions": ["s1.id", "s1.instant_at"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        assert {naive(r["s1.instant_at"]) for r in resp.data} == {datetime(2024, 11, 3, 6, 30)}


def _ny_to_tokyo(value: datetime) -> datetime:
    return to_zone(value, TOKYO, source=NY)


# Stage pinned to New York, read under Tokyo: (output expression, expected Tokyo value per row id).
ROW_OUTPUTS: dict[str, tuple[str, Any]] = {
    "plain": ("orders.ordered_at", lambda i: local(i, TOKYO)),
    "date_add": ("date_add(orders.ordered_at, 1, 'hour')", lambda i: local(i, TOKYO) + timedelta(hours=1)),
    "mixed_civil_and_converted": ("greatest(orders.ordered_at, orders.local_slot)",
                                  lambda i: _ny_to_tokyo(max(local(i, NY), _slot(i)))),
    "civil": ("orders.local_slot", _slot),
    "no_read": ("CAST('2025-01-01 00:00:00' AS TIMESTAMP)", lambda _i: datetime(2025, 1, 1)),
}


@pytest.mark.parametrize("backend", BACKENDS)
class TestOutputZoneState:
    @pytest.mark.parametrize("output", sorted(ROW_OUTPUTS))
    async def test_row_level_output(self, backend, output) -> None:
        expr, expected = ROW_OUTPUTS[output]
        stage = _stage(time_zone=NY, dimensions=["orders.id", d(expr, "v")])
        final = {"population": "s1", "time_zone": TOKYO, "dimensions": ["s1.id", "s1.v"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        assert {int(r["s1.id"]): naive(r["s1.v"]) for r in resp.data} == {i: expected(i) for i in ROWS}, resp.sql

    async def test_bucket_output(self, backend) -> None:
        stage = _stage(time_zone=NY, time_dimensions=[DAY_TD], metrics=[m("count(orders.*)", "n")])
        final = {"population": "s1", "time_zone": TOKYO, "dimensions": ["s1.ordered_at"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        starts = {datetime.combine(local(i, NY).date(), datetime.min.time()) for i in ROWS}
        assert {naive(r["s1.ordered_at"]) for r in resp.data} == {_ny_to_tokyo(s) for s in starts}

    async def test_aggregate_outputs(self, backend) -> None:
        stage = _stage(time_zone=NY, metrics=[
            m("max(orders.ordered_at)", "hi"), m("first(orders.ordered_at, orders.paid_at)", "f"),
            m("last(orders.local_slot, orders.ordered_at)", "l")])
        final = {"population": "s1", "time_zone": TOKYO, "dimensions": ["s1.hi", "s1.f", "s1.l"]}
        async with tz_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        [row] = resp.data
        assert naive(row["s1.hi"]) == local(4, TOKYO)  # value converted: the stage zone
        assert naive(row["s1.f"]) == local(1, TOKYO)  # earliest paid_at is row 1's; value converted
        assert naive(row["s1.l"]) == _slot(4)  # value civil, whatever its ranking key

    async def test_an_undeclared_output_is_read_as_stored_and_warns(self, backend) -> None:
        datasets = default_datasets(orders_dataset(field_zones={"ordered_at": "UTC"}))
        stage = _stage(time_zone=NY, dimensions=["orders.id", d("greatest(orders.ordered_at, orders.local_slot)", "v")])
        final = {"population": "s1", "time_zone": TOKYO, "dimensions": ["s1.id", "s1.v"]}
        async with tz_engine(backend, datasets=datasets, ds_zone=None) as engine:
            resp = await engine.execute([stage, final])
        assert {int(r["s1.id"]): naive(r["s1.v"]) for r in resp.data} == {
            i: max(local(i, NY), _slot(i)) for i in ROWS}
        assert [w.field for w in warnings_of(resp, UNDECLARED)] == ["local_slot"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_cross_zone_rebucketing_is_refused(backend) -> None:
    stage = _stage(time_zone="UTC", time_dimensions=[MONTH_TD], metrics=[m("sum(orders.amount)", "s")])
    final = {"population": "s1", "time_zone": NY, "time_dimensions": [{"field": "s1.ordered_at", "granularity": "year"}],
             "metrics": [m("sum(s1.s)", "t")]}
    async with tz_engine(backend) as engine:
        with pytest.raises(TimeDimensionFieldError) as exc:
            await engine.execute([stage, final], dry_run=True)
    msg = str(exc.value)
    assert "ordered_at" in msg
    assert "UTC" in msg
    assert NY in msg


@pytest.mark.parametrize("backend", BACKENDS)
async def test_same_zone_rebucketing_still_works(backend) -> None:
    stage = _stage(time_zone=NY, time_dimensions=[MONTH_TD], metrics=[m("sum(orders.amount)", "s")])
    final = {"population": "s1", "time_zone": NY, "time_dimensions": [{"field": "s1.ordered_at", "granularity": "year"}],
             "metrics": [m("sum(s1.s)", "t")]}
    async with tz_engine(backend) as engine:
        resp = await engine.execute([stage, final])
    assert {day(r["s1.ordered_at"]).year: float(r["t"]) for r in resp.data} == {2024: 10.0, 2025: 90.0}


class TestQueryBackedStamping:
    @pytest.mark.parametrize(("stage_zone", "stamped"), [(NY, NY), (None, None)])
    async def test_a_pinned_stage_stamps_its_zone(self, stage_zone, stamped) -> None:
        query: dict[str, Any] = {"population": "orders", "dimensions": ["orders.id", "orders.ordered_at"]}
        if stage_zone is not None:
            query["time_zone"] = stage_zone
        async with tz_engine("sqlite") as engine:
            model = await engine.create_dataset_from_query(query=query, name="listed")
            reloaded = await engine.storage.get_dataset("listed", data_source=DS)
        for dataset in (model, reloaded):
            assert dataset is not None
            [ordered] = [f for f in dataset.fields if f.name == "ordered_at"]
            assert ordered.datatype is DataType.DATETIME
            assert ordered.time_zone == stamped

