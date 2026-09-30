"""Datasource custom granularities: definitions, origin-anchored buckets, every granularity position, nesting."""

from __future__ import annotations

import tempfile
from datetime import datetime
from typing import Any

import pytest
from sqlglot import exp

from slayer.core.errors import QueryTypeError, SlayerError, TimeDimensionColumnError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.granularity import CustomGranularity
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import get_dialect
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev2015_fixtures import (
    BACKENDS,
    BUILT_IN_GRANULARITIES,
    GRANULARITIES,
    GRANULARITY_NAMES,
    bucket_key,
    bucketed_model,
    cg_engine,
    cg_orders_model,
    m,
    spine_td,
    value,
)

FY_ROWS = {"2023-04-01": 10.0, "2024-04-01": 50.0, "2025-04-01": 40.0}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with cg_engine(request.param) as eng:
        yield eng


def _ds(granularities: list[dict[str, Any]], *, name: str = "gds") -> DatasourceConfig:
    return DatasourceConfig.model_validate({"name": name, "type": "sqlite", "granularities": granularities})


def _dump(cfg: DatasourceConfig) -> list[dict[str, Any]]:
    return [g.model_dump(mode="json") for g in cfg.granularities]  # type: ignore[attr-defined]


def _series(resp, *, column: str, width: int = 10) -> dict[str, float]:
    return {bucket_key(value(r, column), width=width): float(value(r, "s")) for r in resp.data}


async def _grouped(engine, *, granularity: str, column: str = "ts", model: str = "events",
                   series: str | None = None, width: int = 10) -> dict[str, float]:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": model, "measures": [m("sum(amount)", "s")],
        "time_dimensions": [{"dimension": column, "granularity": granularity}],
        "filters": [f"series = '{series}'"] if series else [],
    }))
    return _series(resp, column=column, width=width)


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

class TestDefinitions:
    async def test_definitions_persist_unchanged(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        cfg = _ds(GRANULARITIES)
        await storage.save_datasource(cfg)
        loaded = await storage.get_datasource("gds")
        assert loaded is not None
        assert _dump(loaded) == _dump(cfg)
        assert [g["name"] for g in _dump(loaded)] == list(GRANULARITY_NAMES)

    def test_defaults(self) -> None:
        by_name = {g["name"]: g for g in _dump(_ds(GRANULARITIES))}
        assert by_name["fiscal_year"]["multiple"] == 1
        assert str(by_name["quarter_hour"]["origin"]).startswith("2000-01-01")  # midnight on 1 January
        week = _dump(_ds([{"name": "wk", "base": "week", "multiple": 2}]))[0]
        week_sunday = _dump(_ds([{"name": "wks", "base": "week_sunday", "multiple": 2}]))[0]
        assert _weekday(week["origin"]) == 0
        assert _weekday(week_sunday["origin"]) == 6

    def test_absent_field_behaves_as_before(self) -> None:
        cfg = DatasourceConfig(name="plain", type="sqlite")
        assert list(cfg.granularities) == []  # type: ignore[attr-defined]

    @pytest.mark.parametrize(("entry", "rule"), [
        ({"name": "month", "base": "day"}, "built-in"),
        ({"name": "sum", "base": "day"}, "aggregation"),
        ({"name": "date_add", "base": "day"}, "function"),
        ({"name": "cumsum", "base": "day"}, "transform"),
        ({"name": "fiscal year", "base": "year"}, "identifier"),
        ({"name": "bad_base", "base": "fortnight"}, "fortnight"),
        ({"name": "bad_multiple", "base": "day", "multiple": 0}, "multiple"),
        ({"name": "bad_dom", "base": "month", "origin": "2000-01-31"}, "28"),
        ({"name": "bad_tod", "base": "year", "origin": "2000-04-01 06:00:00"}, "time of day"),
        ({"name": "bad_align", "base": "minute", "origin": "2000-01-01 10:07:30"}, "align"),
        ({"name": "bad_zone", "base": "day", "origin": "2000-01-01 00:00:00+02:00"}, "zone"),
        ({"name": "bad_fraction", "base": "second", "origin": "2000-01-01 00:00:00.500"}, "second"),
    ], ids=lambda v: v if isinstance(v, str) else v["name"])
    async def test_invalid_definitions_rejected(self, entry, rule) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        ds = _ds([entry])
        with pytest.raises(SlayerError) as exc:
            await storage.save_datasource(ds)
        msg = str(exc.value)
        assert entry["name"] in msg
        assert rule in msg.lower()
        assert await storage.get_datasource("gds") is None

    async def test_duplicate_names_rejected_case_insensitively(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        ds = _ds([{"name": "fiscal_year", "base": "year"}, {"name": "Fiscal_Year", "base": "year"}])
        with pytest.raises(SlayerError) as exc:
            await storage.save_datasource(ds)
        assert "fiscal_year" in str(exc.value).lower()
        assert await storage.get_datasource("gds") is None


def _weekday(origin: Any) -> int:
    return datetime.fromisoformat(str(origin).replace("T", " ")).weekday()


# ---------------------------------------------------------------------------
# Origin-anchored buckets
# ---------------------------------------------------------------------------

class TestBucketing:
    async def test_fiscal_year(self, engine) -> None:
        assert await _grouped(engine, granularity="fiscal_year", column="order_date", model="orders") == FY_ROWS

    @pytest.mark.parametrize("column", ["ts", "d"])
    async def test_mid_month_origin(self, engine, column) -> None:
        got = await _grouped(engine, granularity="billing_month", column=column, series="bill")
        assert got == {"2025-02-15": 1.0, "2025-03-15": 1.0}

    @pytest.mark.parametrize("column", ["ts", "d"])
    async def test_before_the_origin_and_multi_week_steps(self, engine, column) -> None:
        got = await _grouped(engine, granularity="sprint", column=column, series="sprint")
        assert got == {"2024-12-23": 1.0, "2025-01-06": 1.0, "2025-01-20": 1.0}

    async def test_sub_hour_buckets(self, engine) -> None:
        got = await _grouped(engine, granularity="quarter_hour", series="qh", width=16)
        assert got == {"2025-06-02 10:00": 3.0, "2025-06-02 10:15": 4.0, "2025-06-02 10:30": 8.0}


# ---------------------------------------------------------------------------
# Accepted wherever a built-in granularity is
# ---------------------------------------------------------------------------

class TestEveryPosition:
    async def test_functional_form_and_order_key(self, engine) -> None:
        functional = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["fiscal_year(order_date)"], "measures": [m("sum(amount)", "s")],
            "order": [{"column": "fiscal_year(order_date)", "direction": "desc"}],
        }))
        explicit = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [m("sum(amount)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
            "order": [{"column": "order_date", "direction": "desc"}],
        }))
        assert functional.sql == explicit.sql
        assert functional.data == explicit.data
        assert [bucket_key(r["orders.order_date"], width=10) for r in functional.data] == sorted(FY_ROWS, reverse=True)

    async def test_time_dimensions_string_entry(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [m("sum(amount)", "s")],
            "time_dimensions": ["fiscal_year(order_date)"],
        }))
        assert _series(resp, column="order_date") == FY_ROWS

    async def test_filter(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [m("sum(amount)", "s")],
            "filters": ["fiscal_year(order_date) = '2024-04-01'"],
        }))
        assert float(value(resp.data[0], "s")) == 50.0

    async def test_inside_an_expression(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [m("count_distinct(fiscal_year(order_date))", "fys")],
        }))
        assert int(value(resp.data[0], "fys")) == 3

    async def test_on_the_spine(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "measures": [m("sum(orders.amount)", "s")],
            "time_dimensions": [spine_td(granularity="fiscal_year", date_range=["2023-04-01", "2026-03-31"])],
        }))
        assert _series(resp, column="time_spine.timestamp") == FY_ROWS

    async def test_result_keys_follow_the_built_in_rules(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [m("sum(amount)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "month"},
                                {"dimension": "order_date", "granularity": "fiscal_year"}],
        }))
        assert {"orders.order_date.month", "orders.order_date.fiscal_year"} <= set(resp.data[0])

    @pytest.mark.parametrize("query", [
        {"source_model": "orders", "measures": [m("sum(amount)", "s")],
         "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_yr"}]},
        {"source_model": "orders", "measures": [m("sum(amount)", "s")], "dimensions": ["mnth(order_date)"]},
    ], ids=["time-dimension", "functional"])
    async def test_unknown_name(self, engine, query) -> None:
        parsed = SlayerQuery.model_validate(query)  # construction accepts any name
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(parsed, dry_run=True)
        msg = str(exc.value)
        for name in (*BUILT_IN_GRANULARITIES, *GRANULARITY_NAMES):
            assert name in msg

    async def test_name_scoped_to_its_datasource(self, engine) -> None:
        cfg = await engine.storage.get_datasource("test")
        assert cfg is not None
        await engine.storage.save_datasource(DatasourceConfig(name="other", type=cfg.type, database=cfg.database))
        other = cg_orders_model().model_copy(update={"name": "orders2", "data_source": "other"})
        await engine.storage.save_model(other)
        query = SlayerQuery.model_validate({
            "source_model": "orders2", "measures": [m("sum(amount)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
        })
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        assert "fiscal_year" in str(exc.value)
        assert "month" in str(exc.value)


# ---------------------------------------------------------------------------
# Nesting is boundary containment
# ---------------------------------------------------------------------------

async def _save(engine, model: SlayerModel) -> None:
    await engine.storage.save_model(model)


class TestNesting:
    async def test_month_nests_into_the_fiscal_year_week_does_not(self, engine) -> None:
        await _save(engine, bucketed_model(name="o_month", source="orders", column="order_date", granularity="month"))
        await _save(engine, bucketed_model(name="o_week", source="orders", column="order_date", granularity="week"))
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "o_month", "measures": [m("sum(rev)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
        }))
        assert _series(resp, column="order_date") == FY_ROWS
        weekly = SlayerQuery.model_validate({
            "source_model": "o_week", "measures": [m("sum(rev)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
        })
        with pytest.raises(TimeDimensionColumnError) as exc:
            await engine.execute(weekly, dry_run=True)
        assert "week" in str(exc.value)
        assert "fiscal_year" in str(exc.value)

    async def test_the_fiscal_year_does_not_nest_into_the_calendar_year(self, engine) -> None:
        stage = SlayerQuery.model_validate({
            "name": "fy", "source_model": "orders", "measures": [m("sum(amount)", "rev")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
        })
        main = SlayerQuery.model_validate({
            "source_model": "fy", "measures": [m("sum(rev)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "year"}],
        })
        with pytest.raises(TimeDimensionColumnError) as exc:
            await engine.execute([stage, main], dry_run=True)
        assert "fiscal_year" in str(exc.value)
        assert "year" in str(exc.value).replace("fiscal_year", "")

    @pytest.mark.parametrize(("stored", "requested", "ok"), [
        ("quarter_hour", "hour", True),
        ("minute", "quarter_hour", True),
        ("hour", "quarter_hour", False),
    ])
    async def test_minute_multiples(self, engine, stored, requested, ok) -> None:
        await _save(engine, bucketed_model(name="ev_b", source="events", column="ts", granularity=stored))
        query = SlayerQuery.model_validate({
            "source_model": "ev_b", "measures": [m("sum(rev)", "s")],
            "time_dimensions": [{"dimension": "ts", "granularity": requested}],
        })
        if ok:
            resp = await engine.execute(query)
            assert sum(float(value(r, "s")) for r in resp.data) == pytest.approx(23.0)
        else:
            with pytest.raises(TimeDimensionColumnError) as exc:
                await engine.execute(query, dry_run=True)
            assert stored in str(exc.value)
            assert requested in str(exc.value)

    @pytest.mark.parametrize(("stored", "requested", "ok"), [
        ("week", "sprint", True),          # 14 days = 2 weeks, Monday origin
        ("sprint", "week", False),
        ("day", "sprint", True),
        ("month", "fiscal_year", True),
        ("quarter", "fiscal_year", True),  # April origin is a quarter boundary
        ("month", "billing_month", False),  # origins differ in day of month
        ("billing_month", "fiscal_year", False),
        ("fiscal_year", "year", False),
        ("quarter", "year", True),
        ("day", "fiscal_year", True),
        ("hour", "fiscal_year", True),
        ("quarter_hour", "month", True),
        ("week", "fiscal_year", False),    # never week into a month family
        ("month", "sprint", False),        # never a month family into a fixed length
        ("quarter_hour", "quarter_hour", True),
    ])
    async def test_boundary_containment(self, engine, stored, requested, ok) -> None:
        model = SlayerModel.model_validate({
            "name": "hand", "sql_table": "events", "data_source": "test",
            "columns": [{"name": "id", "type": "INT", "primary_key": True},
                        {"name": "ts", "type": "TIMESTAMP", "granularity": stored},
                        {"name": "amount", "type": "DOUBLE"}],
        })
        await _save(engine, model)
        query = SlayerQuery.model_validate({
            "source_model": "hand", "measures": [m("sum(amount)", "s")],
            "time_dimensions": [{"dimension": "ts", "granularity": requested}],
        })
        if ok:
            await engine.execute(query, dry_run=True)
        else:
            with pytest.raises(TimeDimensionColumnError):
                await engine.execute(query, dry_run=True)


# ---------------------------------------------------------------------------
# Column.granularity accepts a datasource granularity
# ---------------------------------------------------------------------------

class TestColumnGranularity:
    async def test_custom_value_round_trips_and_rebuckets(self, engine) -> None:
        model = cg_orders_model(order_date_granularity="fiscal_year").model_copy(update={"name": "orders_fy"})
        await _save(engine, model)
        loaded = await engine.storage.get_model("orders_fy", data_source="test")
        assert loaded is not None
        assert str(next(c for c in loaded.columns if c.name == "order_date").granularity) == "fiscal_year"
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "orders_fy", "measures": [m("sum(amount)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
        }))
        assert _series(resp, column="order_date") == FY_ROWS
        monthly = SlayerQuery.model_validate({
            "source_model": "orders_fy", "measures": [m("sum(amount)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "month"}],
        })
        with pytest.raises(TimeDimensionColumnError):
            await engine.execute(monthly, dry_run=True)

    async def test_undefined_custom_value(self, engine) -> None:
        cfg = await engine.storage.get_datasource("test")
        assert cfg is not None
        await engine.storage.save_datasource(DatasourceConfig(name="other", type=cfg.type, database=cfg.database))
        model = cg_orders_model(order_date_granularity="fiscal_year").model_copy(
            update={"name": "orders2", "data_source": "other"},
        )
        with pytest.raises(SlayerError) as exc:
            await engine.storage.save_model(model)
        assert "order_date" in str(exc.value)
        assert "fiscal_year" in str(exc.value)
        assert await engine.storage.get_model("orders2", data_source="other") is None
        await engine.storage.save_model(model, _validate=False)
        query = SlayerQuery.model_validate({"source_model": "orders2", "measures": [m("sum(amount)", "s")]})
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        assert "fiscal_year" in str(exc.value)


@pytest.mark.parametrize("dialect", ["mysql", "snowflake"])
def test_bucket_index_floors_where_an_integer_cast_rounds(dialect: str) -> None:
    quarter_hour = CustomGranularity.model_validate({"name": "quarter_hour", "base": "minute", "multiple": 15})
    bucket = get_dialect(dialect).build_bucket(col_expr=exp.column("t"), granularity=quarter_hour)
    assert bucket.find(exp.Floor) is not None
