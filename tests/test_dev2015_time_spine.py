"""The built-in time spine: axes, routing, the ``time_spine × P`` population, bounds, fail-closed shapes."""

from __future__ import annotations

from datetime import datetime

import pytest
import yaml

from slayer.core.errors import (
    QueryTypeError,
    SlayerError,
    TimeDimensionColumnError,
    UnresolvableDimensionJoinError,
)
from slayer.core.models import Column, SlayerModel
from slayer.core.enums import DataType
from slayer.core.policy import JoinFilterRule, JoinFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev2015_fixtures import (
    BACKENDS,
    JAN_JUN,
    JAN_MAR,
    MONTHS,
    TWO_FACTS,
    TWO_FACT_ROWS,
    broadcast_warnings,
    bucket_key,
    by_bucket,
    calendar_models,
    m,
    num,
    spine_engine,
    spine_models,
    spine_query,
    spine_td,
    value,
)
from tests._time_points_fixtures import PinnedClock

D = datetime
PER_GROUP = [m("sum(orders.amount)", "o"), m("sum(returns.amount)", "r")]
PER_GROUP_ROWS = {
    ("N", "2025-01"): (100.0, None), ("N", "2025-02"): (70.0, 20.0), ("N", "2025-03"): (None, None),
    ("S", "2025-01"): (50.0, None), ("S", "2025-02"): (None, None), ("S", "2025-03"): (None, 10.0),
    ("E", "2025-01"): (None, None), ("E", "2025-02"): (None, None), ("E", "2025-03"): (None, None),
}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with spine_engine(request.param) as eng:
        yield eng


def _per_group(**extra) -> SlayerQuery:
    return spine_query(measures=PER_GROUP, date_range=JAN_MAR, dimensions=["customers.region"], **extra)


def _canon(rows: list[dict]) -> list[tuple]:
    return sorted((tuple(sorted((k, bucket_key(v, width=19) if k.endswith("timestamp") else v)
                                for k, v in r.items())) for r in rows), key=repr)


def _data_calls(monkeypatch) -> list[str]:
    calls: list[str] = []
    orig = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


# ---------------------------------------------------------------------------
# The spine model and its reserved name
# ---------------------------------------------------------------------------

class TestReservedName:
    async def test_save_rejected(self, engine) -> None:
        clash = SlayerModel(name="time_spine", sql_table="customers", data_source="test",
                            columns=[Column(name="foo", type=DataType.INT, primary_key=True)])
        with pytest.raises(SlayerError) as exc:
            await engine.storage.save_model(clash)
        assert "time_spine" in str(exc.value)
        assert "reserved" in str(exc.value).lower()
        stored = await engine.storage.get_model("time_spine", data_source="test")
        assert stored is None or "foo" not in {c.name for c in stored.columns}

    async def test_stored_clash_fails_closed(self, engine) -> None:
        clash = SlayerModel(name="time_spine", sql_table="customers", data_source="test",
                            columns=[Column(name="timestamp", type=DataType.DATE, primary_key=True)])
        await engine.storage._save_model_impl(clash)
        query = spine_query(measures=TWO_FACTS)
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        msg = str(exc.value)
        assert "time_spine" in msg
        assert "rename" in msg.lower()

    async def test_below_version_stored_clash_still_loads(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        path = tmp_path / "models" / "test" / "time_spine.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(yaml.safe_dump({
            "version": CURRENT_VERSIONS["SlayerModel"] - 1, "name": "time_spine", "sql_table": "cal",
            "data_source": "test", "columns": [{"name": "day", "type": "date", "primary_key": True}],
        }))
        loaded = await storage.get_model("time_spine", data_source="test")
        assert loaded is not None
        assert [c.name for c in loaded.columns] == ["day"]
        assert yaml.safe_load(path.read_text())["version"] == CURRENT_VERSIONS["SlayerModel"]


# ---------------------------------------------------------------------------
# Axes
# ---------------------------------------------------------------------------

class TestAxes:
    async def test_declared_and_sole_column_axes_are_wired(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS[:2]))
        assert by_bucket(resp, ["o", "r"]) == {k: v[:2] for k, v in TWO_FACT_ROWS.items()}
        assert broadcast_warnings(resp) == []

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_two_temporal_columns_mean_no_axis(self, backend) -> None:
        async with spine_engine(backend, models=spine_models(shipments="two_dates")) as eng:
            resp = await eng.execute(spine_query(measures=[m("sum(shipments.weight)", "w")]))
            assert by_bucket(resp, ["w"]) == {k: (7.0,) for k in MONTHS}
            assert [w.measure for w in broadcast_warnings(resp)]
            assert "w" in broadcast_warnings(resp)[0].measure
            strict = spine_query(measures=[m("sum(shipments.weight)", "w")], to_many_handling="error")
            with pytest.raises((SlayerError, ValueError)):
                await eng.execute(strict)

    async def test_stage_with_one_temporal_column_is_wired(self, engine) -> None:
        firsts = SlayerQuery.model_validate({
            "name": "firsts", "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [m("min(order_date)", "first_order")],
        })
        resp = await engine.execute([firsts, spine_query(measures=[m("count(firsts.customer_id)", "c")])])
        assert by_bucket(resp, ["c"]) == {
            "2025-01": (2.0,), **{k: (0.0,) for k in MONTHS[1:]},
        }

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_stage_propagates_its_sources_default(self, backend) -> None:
        stage = SlayerQuery.model_validate({
            "name": "ob", "source_model": "orders", "dimensions": ["customer_id"],
            "time_dimensions": [{"dimension": "order_date", "granularity": "day"}],
            "measures": [m("sum(amount)", "rev"), m("min(customers.signed_up_at)", "signup")],
        })
        async with spine_engine(backend, models=spine_models(signed_up_at=True)) as eng:
            resp = await eng.execute([stage, spine_query(measures=[m("sum(ob.rev)", "rev")])])
        assert by_bucket(resp, ["rev"]) == {"2025-01": (150.0,), "2025-02": (70.0,),
                                            **{k: (None,) for k in MONTHS[2:]}}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_sole_column_default_feeds_transforms(self, backend) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "returns",
            "time_dimensions": [{"dimension": "return_date", "granularity": "month"},
                                {"dimension": "customers.signed_up_at", "granularity": "month"}],
            "measures": [m("cumsum(sum(amount))", "cs")],
        })
        got = {}
        for default in (None, "return_date"):
            models = spine_models(signed_up_at=True, returns_default=default)
            async with spine_engine(backend, models=models) as eng:
                resp = await eng.execute(query)
            got[default] = {bucket_key(value(r, "return_date")): float(value(r, "cs")) for r in resp.data}
        assert got[None] == got["return_date"] == {"2025-02": 20.0, "2025-03": 10.0, "2025-05": 25.0}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_sole_column_default_feeds_last(self, backend) -> None:
        query = SlayerQuery.model_validate({"source_model": "returns", "measures": [m("last(amount)", "l")]})
        for default in (None, "return_date"):
            async with spine_engine(backend, models=spine_models(returns_default=default)) as eng:
                resp = await eng.execute(query)
            assert float(value(resp.data[0], "l")) == 5.0, default


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

class TestRouting:
    async def test_dataset_without_axis_reaches_through_its_parent(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=[m("sum(order_items.qty)", "q")]))
        assert by_bucket(resp, ["q"]) == {"2025-01": (6.0,), "2025-02": (4.0,), **{k: (None,) for k in MONTHS[2:]}}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_own_axis_beats_a_parents_axis(self, backend) -> None:
        async with spine_engine(backend, models=spine_models(signed_up_at=True)) as eng:
            resp = await eng.execute(spine_query(measures=TWO_FACTS[:2]))
        assert by_bucket(resp, ["o", "r"]) == {k: v[:2] for k, v in TWO_FACT_ROWS.items()}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_equal_length_routes_fail_loudly(self, backend) -> None:
        async with spine_engine(backend, models=spine_models(shipments="two_parents")) as eng:
            query = spine_query(measures=[m("count(shipments.id)", "c")])
            with pytest.raises(UnresolvableDimensionJoinError) as exc:
                await eng.execute(query)
            assert "shipments.orders" in str(exc.value)
            assert "shipments.returns" in str(exc.value)
            # Only a query needing that route raises.
            resp = await eng.execute(spine_query(measures=TWO_FACTS))
            assert by_bucket(resp, ["o", "r", "n"]) == TWO_FACT_ROWS

    async def test_existing_queries_never_route_through_the_spine(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders",
            "time_dimensions": [{"dimension": "order_date", "granularity": "month"}],
            "measures": [m("sum(returns.amount)", "r")],
        }))
        assert by_bucket(resp, ["r"], bucket="orders.order_date") == {"2025-01": (35.0,), "2025-02": (35.0,)}
        assert broadcast_warnings(resp)
        assert "time_spine" not in (resp.sql or "")


# ---------------------------------------------------------------------------
# Population: time_spine × P
# ---------------------------------------------------------------------------

class TestPopulation:
    async def test_every_month_both_facts(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS))
        assert by_bucket(resp, ["o", "r", "n"]) == TWO_FACT_ROWS
        assert resp.warnings == []

    async def test_per_group_grid(self, engine) -> None:
        resp = await engine.execute(_per_group())
        assert by_bucket(resp, ["o", "r"], by=["region"]) == PER_GROUP_ROWS

    async def test_explicit_p_and_explicit_spine(self, engine) -> None:
        inferred = await engine.execute(_per_group())
        explicit = await engine.execute(_per_group(source_model="customers"))
        assert _canon(explicit.data) == _canon(inferred.data)
        spine_only = await engine.execute(spine_query(measures=TWO_FACTS))
        named = await engine.execute(spine_query(measures=TWO_FACTS, source_model="time_spine"))
        assert _canon(named.data) == _canon(spine_only.data)

    async def test_result_keys(self, engine) -> None:
        resp = await engine.execute(_per_group())
        assert set(resp.data[0]) == {"time_spine.timestamp", "customers.region", "customers.o", "customers.r"}
        resp = await engine.execute(spine_query(measures=TWO_FACTS))
        assert set(resp.data[0]) == {"time_spine.timestamp", "time_spine.o", "time_spine.r", "time_spine.n"}

    async def test_two_spine_time_dimensions(self, engine) -> None:
        resp = await engine.execute(spine_query(
            measures=TWO_FACTS[:1],
            time_dimensions=[spine_td(granularity="year"), spine_td(granularity="month")],
        ))
        pairs = sorted((bucket_key(value(r, "timestamp.year"), width=4), bucket_key(value(r, "timestamp.month")))
                       for r in resp.data)
        assert pairs == [("2025", k) for k in MONTHS]

    @pytest.mark.parametrize("variant", ["drop", "add"])
    async def test_measures_never_change_the_rows(self, engine, variant) -> None:
        def cells(resp, by=()):
            return sorted((*(value(r, d) for d in by), bucket_key(r["time_spine.timestamp"])) for r in resp.data)

        edit = (lambda ms: [x for x in ms if x["name"] != "r"]) if variant == "drop" else (
            lambda ms: [*ms, m("avg(orders.amount)", "a")])
        base = await engine.execute(spine_query(measures=TWO_FACTS))
        other = await engine.execute(spine_query(measures=edit(TWO_FACTS)))
        assert cells(other) == cells(base)
        base = await engine.execute(_per_group())
        other = await engine.execute(spine_query(measures=edit(PER_GROUP), date_range=JAN_MAR,
                                                 dimensions=["customers.region"]))
        assert cells(other, by=["region"]) == cells(base, by=["region"])

    @pytest.mark.parametrize("dimensions", [[], ["customers.region"]], ids=["unit", "per-group"])
    async def test_without_measures(self, engine, dimensions) -> None:
        resp = await engine.execute(spine_query(measures=[], date_range=JAN_MAR, dimensions=dimensions))
        cells = sorted((*(value(r, "region") for _ in dimensions), bucket_key(r["time_spine.timestamp"]))
                       for r in resp.data)
        regions = ["E", "N", "S"] if dimensions else [None]
        assert cells == sorted((*([g] if dimensions else []), k) for g in regions for k in MONTHS[:3])


def _monthly_stage(*, measures=None, date_range=JAN_MAR, **extra) -> SlayerQuery:
    return spine_query(measures=measures or [m("sum(orders.amount)", "o")], date_range=date_range,
                       name="monthly", **extra)


def _over_monthly(**extra) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "monthly", "measures": [m("sum(o)", "t"), m("count(*)", "c")], **extra,
    })


class TestSpineStages:
    @pytest.mark.parametrize("extra", [{}, {"source_model": "time_spine"}], ids=["rootless", "explicit-spine"])
    async def test_a_spine_stage_keeps_every_bucket(self, engine, extra) -> None:
        resp = await engine.execute([_monthly_stage(**extra), _over_monthly()])
        assert [(num(value(r, "t")), num(value(r, "c"))) for r in resp.data] == [(220.0, 3.0)]

    async def test_a_per_group_spine_stage(self, engine) -> None:
        resp = await engine.execute([_monthly_stage(dimensions=["customers.region"]),
                                     _over_monthly(dimensions=["region"])])
        assert {value(r, "region"): (num(value(r, "t")), num(value(r, "c"))) for r in resp.data} == {
            "N": (170.0, 3.0), "S": (50.0, 3.0), "E": (None, 3.0),
        }

    async def test_a_spine_stage_feeding_a_spine_query(self, engine) -> None:
        resp = await engine.execute([_monthly_stage(), spine_query(measures=[m("sum(monthly.o)", "t")])])
        assert by_bucket(resp, ["t"]) == {"2025-01": (150.0,), "2025-02": (70.0,),
                                          **{k: (None,) for k in MONTHS[2:]}}

    @pytest.mark.parametrize(("extra", "pinned", "count"), [
        ({}, "time_spine", 3.0), ({"dimensions": ["customers.region"]}, "customers", 9.0),
    ], ids=["unit", "per-group"])
    async def test_a_saved_spine_query_read_as_a_source(self, engine, extra, pinned, count) -> None:
        await engine.create_model_from_query(query=_monthly_stage(**extra), name="saved_monthly")
        stored = await engine.storage.get_model("saved_monthly", data_source="test")
        assert [s.source_model for s in stored.source_queries] == [pinned]
        resp = await engine.execute(_over_monthly(source_model="saved_monthly"))
        assert [(num(value(r, "t")), num(value(r, "c"))) for r in resp.data] == [(220.0, count)]

    async def test_missing_lower_bound_inside_a_stage(self, engine) -> None:
        query = [_monthly_stage(date_range=None), _over_monthly()]
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        assert "lower bound" in str(exc.value).lower()

    @pytest.mark.parametrize(("measure", "extra"), [
        (m("count(*)", "o"), {"source_model": "time_spine"}),
        (m("min(time_spine.timestamp)", "o"), {}),
    ], ids=["count-star", "min-timestamp"])
    async def test_aggregation_over_the_spine_inside_a_stage(self, engine, measure, extra) -> None:
        query = [_monthly_stage(measures=[measure], **extra), _over_monthly()]
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        assert "time_spine" in str(exc.value)


# ---------------------------------------------------------------------------
# Bounds
# ---------------------------------------------------------------------------

class TestBounds:
    @pytest.mark.parametrize("date_range", [None, [None, "2025-03-31"]], ids=["none", "upper-only"])
    async def test_missing_lower_bound(self, engine, date_range) -> None:
        query = spine_query(measures=TWO_FACTS, date_range=date_range)
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        assert "lower bound" in str(exc.value).lower()

    async def test_bound_filters_bound_the_spine(self, engine) -> None:
        resp = await engine.execute(spine_query(
            measures=TWO_FACTS, date_range=None,
            filters=["time_spine.timestamp >= '2025-04-01'", "time_spine.timestamp < '2025-07-01'"],
        ))
        assert by_bucket(resp, ["o", "r", "n"]) == {k: TWO_FACT_ROWS[k] for k in MONTHS[3:]}

    async def test_mid_bucket_lower_bound_keeps_the_bucket_not_the_rows(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS, date_range=["2025-01-15", "2025-02-28"]))
        assert by_bucket(resp, ["o", "r", "n"]) == {"2025-01": (50.0, None, 1.0), "2025-02": (70.0, 20.0, 1.0)}

    async def test_exclusive_period_upper_bound(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS, date_range=["2025-01", "2025-02"]))
        assert set(by_bucket(resp, ["o"])) == {"2025-01", "2025-02"}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_implied_upper_bound_is_the_current_bucket(self, backend) -> None:
        async with spine_engine(backend, clock=PinnedClock(D(2025, 3, 2, 12))) as eng:
            resp = await eng.execute(spine_query(measures=TWO_FACTS, date_range=["2025-01-01", None]))
            assert by_bucket(resp, ["o", "r", "n"]) == {k: TWO_FACT_ROWS[k] for k in MONTHS[:3]}
            relative = await eng.execute(spine_query(measures=TWO_FACTS, date_range="last 3 months"))
            assert by_bucket(relative, ["o", "r", "n"]) == {
                "2024-12": (None, None, 0.0), "2025-01": TWO_FACT_ROWS["2025-01"], "2025-02": TWO_FACT_ROWS["2025-02"],
            }

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_implied_upper_bound_follows_the_finest_granularity(self, backend) -> None:
        async with spine_engine(backend, clock=PinnedClock(D(2025, 3, 2, 12))) as eng:
            resp = await eng.execute(spine_query(measures=TWO_FACTS, granularity="day",
                                                 date_range=["2025-02-27", None]))
        days = sorted(by_bucket(resp, ["o"], width=10))
        assert days == ["2025-02-27", "2025-02-28", "2025-03-01", "2025-03-02"]

    async def test_bounds_stay_frame_bounds(self, engine) -> None:
        resp = await engine.execute(spine_query(
            measures=[m("time_shift(sum(orders.amount), -1)", "prev"), m("sum(orders.amount, window='2m')", "w")],
            date_range=["2025-02-01", "2025-03-31"],
        ))
        assert by_bucket(resp, ["prev", "w"]) == {"2025-02": (150.0, 220.0), "2025-03": (70.0, 70.0)}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_whole_periods_only_excludes_the_current_bucket(self, backend) -> None:
        async with spine_engine(backend, clock=PinnedClock(D(2025, 3, 15, 12))) as eng:
            resp = await eng.execute(spine_query(measures=TWO_FACTS, date_range=["2025-01-01", None],
                                                 whole_periods_only=True))
        assert set(by_bucket(resp, ["o"])) == {"2025-01", "2025-02"}


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

class TestFilters:
    async def test_a_fact_filter_keeps_every_month(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS[:2], filters=["orders.amount > 60"]))
        assert by_bucket(resp, ["o", "r"]) == {
            "2025-01": (100.0, None), "2025-02": (70.0, 20.0), "2025-03": (None, None),
            "2025-04": (None, None), "2025-05": (None, 5.0), "2025-06": (None, None),
        }

    async def test_a_filter_on_p_keeps_every_month(self, engine) -> None:
        resp = await engine.execute(_per_group(filters=["customers.region = 'N'"]))
        assert by_bucket(resp, ["o", "r"], by=["region"]) == {
            k: v for k, v in PER_GROUP_ROWS.items() if k[0] == "N"
        }

    @pytest.mark.parametrize("filters", [
        ["time_spine.timestamp >= '2025-01-01'", "time_spine.timestamp < '2025-04-01'", "customers.region = 'N'"],
        ["time_spine.timestamp >= '2025-01-01' and time_spine.timestamp < '2025-04-01' and customers.region = 'N'"],
    ], ids=["separate", "one-conjunction"])
    async def test_bounds_and_a_p_filter(self, engine, filters) -> None:
        resp = await engine.execute(spine_query(measures=PER_GROUP, date_range=None, filters=filters))
        assert by_bucket(resp, ["o", "r"]) == {
            "2025-01": (100.0, None), "2025-02": (70.0, 20.0), "2025-03": (None, None),
        }

    async def test_non_bound_spine_filter(self, engine) -> None:
        query = spine_query(measures=TWO_FACTS, filters=["date_part('day_of_week', time_spine.timestamp) = 1"])
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        msg = str(exc.value)
        assert "day_of_week" in msg
        assert "time dimension" in msg.lower() or "time_dimensions" in msg


# ---------------------------------------------------------------------------
# The spine has no countable rows; it is only a bucketed time dimension or a bound
# ---------------------------------------------------------------------------

class TestNoCountableRows:
    @pytest.mark.parametrize(("measure", "extra"), [
        (m("count(*)", "c"), {"source_model": "time_spine"}),
        (m("min(time_spine.timestamp)", "t"), {}),
    ], ids=["count-star", "min-timestamp"])
    async def test_aggregation_over_the_spine(self, engine, measure, extra) -> None:
        query = spine_query(measures=[measure], **extra)
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        assert "time_spine" in str(exc.value)

    @pytest.mark.parametrize("extra", [
        {"dimensions": ["time_spine.timestamp"], "time_dimensions": [], "measures": TWO_FACTS},
        {"distinct_dimension_values": False, "measures": []},
        {"dimensions": [{"expression": "date_part('month', time_spine.timestamp)", "name": "mo"}],
         "measures": TWO_FACTS},
        {"order": [{"column": "day(time_spine.timestamp)"}], "measures": TWO_FACTS},
    ], ids=["plain-dimension", "raw-rows", "computed-dimension-operand", "unprojected-order-key"])
    async def test_spine_column_only_as_a_bucketed_time_dimension(self, engine, extra) -> None:
        extra = dict(extra)  # the parametrized dict is shared across backends
        tds = extra.pop("time_dimensions", None)
        query = {"time_dimensions": [spine_td()] if tds is None else tds, **extra}
        if not query["time_dimensions"]:
            query["filters"] = ["time_spine.timestamp >= '2025-01-01'"]
        parsed = SlayerQuery.model_validate(query)
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(parsed, dry_run=True)
        msg = str(exc.value)
        assert "time_spine" in msg
        assert "granularity" in msg.lower() or "time dimension" in msg.lower()

    async def test_spine_order_key_outside_a_spine_query(self, engine) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "orders", "measures": [m("sum(amount)", "s")], "dimensions": ["customer_id"],
            "order": [{"column": "time_spine.timestamp", "direction": "asc"}],
        })
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        assert "time_spine" in str(exc.value)


# ---------------------------------------------------------------------------
# Transforms and windows over the dense series
# ---------------------------------------------------------------------------

class TestDenseSeries:
    async def test_transforms_over_a_filled_series(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=[
            m("change(sum(orders.amount))", "chg"),
            m("cumsum(sum(orders.amount))", "cs"),
            m("consecutive_periods(count(returns.id) > 0)", "cp"),
            m("lag(sum(returns.amount))", "lg"),
            m("sum(orders.amount, window='2m')", "w"),
        ]))
        got = by_bucket(resp, ["chg", "cs", "cp", "lg", "w"])
        assert [got[k] for k in MONTHS] == [
            (None, 150.0, 0.0, None, 150.0),
            (-80.0, 220.0, 1.0, None, 220.0),
            (None, 220.0, 2.0, 20.0, 70.0),
            (None, 220.0, 0.0, 10.0, None),
            (None, 220.0, 1.0, None, None),
            (None, 220.0, 0.0, 5.0, None),
        ]

    async def test_a_fill_value(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=[
            m("coalesce(sum(orders.amount), 0)", "f"), m("cumsum(coalesce(sum(orders.amount), 0))", "cf"),
        ]))
        got = by_bucket(resp, ["f", "cf"])
        assert [got[k] for k in MONTHS] == [
            (150.0, 150.0), (70.0, 220.0), (0.0, 220.0), (0.0, 220.0), (0.0, 220.0), (0.0, 220.0),
        ]


# ---------------------------------------------------------------------------
# Re-bucketing through the spine
# ---------------------------------------------------------------------------

class TestRebucketing:
    async def test_monthly_model_on_a_daily_spine(self, engine) -> None:
        daily = spine_query(measures=[m("sum(monthly.rev)", "rev")], granularity="day")
        with pytest.raises(TimeDimensionColumnError) as exc:
            await engine.execute(daily, dry_run=True)
        msg = str(exc.value)
        assert "order_date" in msg
        assert "month" in msg
        assert "day" in msg
        resp = await engine.execute(spine_query(measures=[m("sum(monthly.rev)", "rev")], granularity="quarter"))
        assert by_bucket(resp, ["rev"]) == {"2025-01": (220.0,), "2025-04": (None,)}


# ---------------------------------------------------------------------------
# Tenancy and cache
# ---------------------------------------------------------------------------

def _region_policy() -> SessionPolicy:
    return SessionPolicy(ruleset=JoinFilterRuleset(
        table="customers", column="region", value="N",
        joins=(
            JoinFilterRule(target_table="orders", join_path=("orders.customer_id = customers.id",)),
            JoinFilterRule(target_table="returns", join_path=("returns.customer_id = customers.id",)),
        ),
    ))


class TestTenancyAndCache:
    async def test_policy_scopes_the_facts_not_the_calendar(self, engine) -> None:
        scoped = SlayerQueryEngine(storage=engine.storage, policy=_region_policy())
        try:
            resp = await scoped.execute(spine_query(measures=TWO_FACTS[:2]))
        finally:
            scoped.close()
        assert by_bucket(resp, ["o", "r"]) == {
            "2025-01": (100.0, None), "2025-02": (70.0, 20.0), "2025-03": (None, None),
            "2025-04": (None, None), "2025-05": (None, 5.0), "2025-06": (None, None),
        }

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_cache_hit_within_the_bucket_miss_after_rollover(self, backend, monkeypatch) -> None:
        calls = _data_calls(monkeypatch)
        clock = PinnedClock(D(2025, 3, 2, 12))
        query = spine_query(measures=TWO_FACTS, date_range=["2025-01-01", None])
        async with spine_engine(backend, clock=clock) as eng:
            await eng.execute(query, cache=True)
            clock.now = D(2025, 3, 20, 9)
            await eng.execute(query, cache=True)
            assert len([s for s in calls if "slayer_rk_" not in s]) == 1
            clock.now = D(2025, 4, 1, 0, 0, 1)
            resp = await eng.execute(query, cache=True)
        assert len([s for s in calls if "slayer_rk_" not in s]) == 2
        assert "2025-04" in by_bucket(resp, ["o"])


# ---------------------------------------------------------------------------
# The hand-made calendar pattern keeps working
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
async def test_calendar_model_parity(backend) -> None:
    async with spine_engine(backend, models=calendar_models()) as eng:
        cal = await eng.execute(SlayerQuery.model_validate({
            "source_model": "calendar", "measures": TWO_FACTS,
            "time_dimensions": [{"dimension": "date", "granularity": "month", "date_range": JAN_JUN}],
        }))
        spine = await eng.execute(spine_query(measures=TWO_FACTS))
    assert cal.warnings == []
    assert by_bucket(cal, ["o", "r", "n"], bucket="calendar.date") == TWO_FACT_ROWS
    assert by_bucket(spine, ["o", "r", "n"]) == TWO_FACT_ROWS


# ---------------------------------------------------------------------------
# Large ranges
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("year", "rows"), [(2025, 35_040), (2024, 35_136)])
async def test_quarter_hour_year(backend, year, rows) -> None:
    async with spine_engine(backend) as eng:
        resp = await eng.execute(spine_query(measures=[m("count(orders.id)", "n")], granularity="quarter_hour",
                                             date_range=[f"{year}-01-01", f"{year}-12-31"]))
    assert len(resp.data) == rows
    assert sum(int(value(r, "n")) for r in resp.data) == (3 if year == 2025 else 0)
