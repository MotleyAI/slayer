"""Mode-B date functions: operand typing, ISO literals, count rules, and result types."""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import QueryTypeError, TimeDimensionColumnError
from slayer.core.keys import (
    ColumnKey,
    LiteralKey,
    ScalarCallKey,
    ValueKey,
    normalize_scalar,
    temporal_type,
    walk_value_keys,
)
from slayer.core.models import DatasourceConfig
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import dimension_key_metadata, measure_key_type
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import customers_model, orders_model

ORDERS = orders_model()
CUSTOMERS = customers_model()
BUNDLE = ResolvedSourceBundle(dialect="postgres", source_model=ORDERS, referenced_models=[CUSTOMERS])


def _bind(text: str) -> ValueKey:
    return bind_expr(parse_expr(text), scope=ModelScope(source_model=ORDERS), bundle=BUNDLE, allow_measures=True).value_key


def _column_type(key: ValueKey):
    assert isinstance(key, ColumnKey), key
    model = CUSTOMERS if key.path else ORDERS
    col = model.get_column(key.leaf)
    return col.type if col is not None else None


@pytest.fixture
async def engine():
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=os.path.join(tmp, "x.db")))
        await storage.save_model(orders_model())
        await storage.save_model(customers_model())
        yield SlayerQueryEngine(storage=storage)


async def _dry(engine, **kw):
    kw.setdefault("source_model", "orders")
    kw.setdefault("measures", [{"formula": "count(*)", "name": "n"}])
    return await engine.execute(SlayerQuery.model_validate(kw), dry_run=True)


def _assert_type_error(exc: pytest.ExceptionInfo, *needles: str) -> None:
    assert isinstance(exc.value, QueryTypeError)
    assert type(exc.value) is not QueryTypeError, "expected the dedicated QueryTypeError subclass"
    msg = str(exc.value)
    for needle in needles:
        assert needle in msg, f"{needle!r} not in: {msg}"


# ---------------------------------------------------------------------------
# temporal_type — the one pure typing function shared by checker and renderer.
# ---------------------------------------------------------------------------

class TestTemporalType:
    @pytest.mark.parametrize("text,expected", [
        ("order_date", DataType.DATE),
        ("created_at", DataType.TIMESTAMP),
        ("customers.signup_date", DataType.DATE),
        ("status", None),
        ("raw_dt", None),
        ("amount", None),
        ("max(created_at)", DataType.TIMESTAMP),
        ("min(order_date)", DataType.DATE),
        ("first(created_at)", DataType.TIMESTAMP),
        ("last(order_date)", DataType.DATE),
        ("sum(amount)", None),
        ("count(created_at)", None),
        ("date_add(order_date, 1, 'month')", DataType.DATE),
        ("date_add(order_date, sla_days, 'week')", DataType.DATE),
        ("date_add(order_date, 2, 'hour')", DataType.TIMESTAMP),
        ("date_add(created_at, 1, 'day')", DataType.TIMESTAMP),
        ("date_add('2024-01-01', 1, 'day')", DataType.DATE),
        ("date_add('2024-01-01 10:00:00', 1, 'day')", DataType.TIMESTAMP),
        ("now()", DataType.TIMESTAMP),
        ("current_date()", DataType.DATE),
        ("coalesce(shipped_at, now())", DataType.TIMESTAMP),
        ("coalesce(order_date, current_date())", DataType.DATE),
        ("coalesce(order_date, created_at)", DataType.TIMESTAMP),
        ("coalesce(order_date, None)", DataType.DATE),
        ("coalesce(order_date, 5)", None),
        ("coalesce(order_date, status)", None),
        ("ifnull(shipped_at, created_at)", DataType.TIMESTAMP),
        ("nullif(order_date, current_date())", DataType.DATE),
        ("greatest(order_date, created_at)", DataType.TIMESTAMP),
        ("least(order_date, customers.signup_date)", DataType.DATE),
        ("iif(amount > 10, shipped_at, created_at)", DataType.TIMESTAMP),
        ("CASE WHEN amount > 10 THEN order_date ELSE NULL END", DataType.DATE),
        ("iif(amount > 10, order_date, 1)", None),
        ("date_part('year', created_at)", None),
        ("date_diff('day', order_date, created_at)", None),
        ("created_at + 1", None),
        ("'2024-01-01'", None),
    ])
    def test_bottom_up(self, text: str, expected) -> None:
        assert temporal_type(_bind(text), column_type=_column_type) == expected


class TestTypedDateLiterals:
    def test_normalize_scalar_keeps_dates(self) -> None:
        assert normalize_scalar(date(2024, 1, 1)) == date(2024, 1, 1)
        assert normalize_scalar(datetime(2024, 1, 1, 10)) == datetime(2024, 1, 1, 10)

    def test_date_literal_is_not_the_string(self) -> None:
        assert LiteralKey(value=date(2024, 1, 1)) != LiteralKey(value="2024-01-01")
        assert LiteralKey(value=date(2024, 1, 1)) != LiteralKey(value=datetime(2024, 1, 1))

    @pytest.mark.parametrize("text,value", [
        ("date_diff('day', '2024-01-01', created_at)", date(2024, 1, 1)),
        ("date_diff('day', '2024-01-01 10:30:00', created_at)", datetime(2024, 1, 1, 10, 30)),
        ("date_diff('day', '2024-01-01T10:30:00', created_at)", datetime(2024, 1, 1, 10, 30)),
        ("date_part('year', coalesce(shipped_at, '2024-01-01'))", date(2024, 1, 1)),
        ("date_part('year', iif(amount > 1, shipped_at, '2024-01-01'))", date(2024, 1, 1)),
        ("date_add(greatest(order_date, '2024-02-29'), 1, 'day')", date(2024, 2, 29)),
    ])
    def test_iso_literal_in_date_position_binds_typed(self, text: str, value: date) -> None:
        literals = [k.value for k in walk_value_keys(_bind(text)) if isinstance(k, LiteralKey)]
        assert value in literals
        assert not any(isinstance(v, str) and v[:10] == f"{value:%Y-%m-%d}" for v in literals)

    def test_string_outside_date_position_unchanged(self) -> None:
        key = _bind("coalesce(status, '2024-01-01')")
        assert isinstance(key, ScalarCallKey)
        assert LiteralKey(value="2024-01-01") in list(walk_value_keys(key))


# ---------------------------------------------------------------------------
# Checker rejections (typed errors) and acceptances, end to end at plan time.
# ---------------------------------------------------------------------------

class TestOperandRejections:
    @pytest.mark.parametrize("filt,operand", [
        ("date_part('month', status) = 1", "status"),
        ("date_part('month', raw_dt) = 1", "raw_dt"),
        ("date_part('month', id) = 1", "id"),
        ("date_part('month', is_paid) = 1", "is_paid"),
        ("date_diff('day', created_at, status) > 1", "status"),
        ("date_add(status, 1, 'day') > created_at", "status"),
        ("date_part('year', created_at + 1) = 2024", "created_at"),
        ("date_part('year', 2024) = 2024", "2024"),
        ("date_part('year', 'yesterday') = 2024", "yesterday"),
        ("date_part('year', coalesce(shipped_at, 5)) = 2024", "coalesce"),
        ("date_part('year', date_part('year', created_at)) = 1", "date_part"),
    ])
    async def test_non_temporal_operand(self, engine, filt: str, operand: str) -> None:
        with pytest.raises(QueryTypeError) as exc:
            await _dry(engine, filters=[filt])
        _assert_type_error(exc, operand, "DATE", "TIMESTAMP")

    async def test_numeric_measure_operand(self, engine) -> None:
        with pytest.raises(QueryTypeError) as exc:
            await _dry(engine, measures=[{"formula": "max(date_part('year', amount))", "name": "y"}])
        _assert_type_error(exc, "amount")

    @pytest.mark.parametrize("filt,literal", [
        ("date_diff('day', '2024-02-30', created_at) > 0", "2024-02-30"),
        ("date_diff('day', '2024-13-01', created_at) > 0", "2024-13-01"),
        ("date_diff('day', '2024-01-01 25:00:00', created_at) > 0", "2024-01-01 25:00:00"),
        ("date_diff('day', '01/02/2024', created_at) > 0", "01/02/2024"),
        ("date_part('year', coalesce(shipped_at, 'soon')) > 0", "soon"),
    ])
    async def test_invalid_literal(self, engine, filt: str, literal: str) -> None:
        with pytest.raises(QueryTypeError) as exc:
            await _dry(engine, filters=[filt])
        assert literal in str(exc.value)


class TestCountRules:
    @pytest.mark.parametrize("count", ["1.5", "True", "None", "'1'", "-0.5"])
    async def test_bad_literal_count(self, engine, count: str) -> None:
        with pytest.raises(ValueError, match=r"(?s)date_add.*count|count.*date_add"):
            await _dry(engine, filters=[f"date_add(order_date, {count}, 'day') > '2024-01-01'"])

    @pytest.mark.parametrize("count", ["1.5", "True", "None", "'1'", "-0.5"])
    async def test_bad_interval_literal_count(self, engine, count: str) -> None:
        with pytest.raises(ValueError, match=r"(?s)(date_add|interval).*count|count.*(date_add|interval)"):
            await _dry(engine, filters=[f"order_date + interval({count}, 'day') > order_date"])

    @pytest.mark.parametrize("count", ["status", "created_at", "is_paid"])
    async def test_non_numeric_interval_count(self, engine, count: str) -> None:
        with pytest.raises(QueryTypeError) as exc:
            await _dry(engine, filters=[f"order_date - interval({count}, 'day') > order_date"])
        _assert_type_error(exc, "date_add")

    @pytest.mark.parametrize("count", [
        "status", "created_at", "order_date", "is_paid", "date_add(order_date, 1, 'day')", "now()",
    ])
    async def test_non_numeric_computed_count(self, engine, count: str) -> None:
        with pytest.raises(QueryTypeError) as exc:
            await _dry(engine, filters=[f"date_add(order_date, {count}, 'day') > '2024-01-01'"])
        _assert_type_error(exc, "date_add")

    @pytest.mark.parametrize("count", ["3", "-3", "sla_days", "-sla_days", "sla_days * 2", "id"])
    async def test_numeric_counts_accepted(self, engine, count: str) -> None:
        await _dry(engine, filters=[f"date_add(order_date, {count}, 'day') > '2024-01-01'"])

    async def test_aggregate_count_accepted(self, engine) -> None:
        await _dry(engine, measures=[{"formula": "date_add(max(order_date), max(sla_days), 'month')", "name": "m"}])


class TestAcceptedOperands:
    @pytest.mark.parametrize("filt", [
        "date_part('day_of_week', order_date) = 1",
        "date_diff('day', created_at, shipped_at) > 3",
        "DATE_DIFF('day', created_at, shipped_at) > 3",
        "date_part('year', customers.signup_date) = 2024",
        "date_diff('day', customers.signup_date, order_date) > 0",
        "date_part('month', date_add(order_date, 1, 'month')) = 1",
        "date_diff('day', created_at, now()) > 0",
        "date_part('year', current_date()) > 2000",
        "created_at <= now()",
        "order_date <= current_date()",
        "created_at >= date_add(current_date(), -30, 'day')",
        "date_diff('day', created_at, coalesce(shipped_at, now())) > 0",
        "date_diff('day', created_at, ifnull(shipped_at, created_at)) >= 0",
        "date_diff('day', created_at, nullif(shipped_at, created_at)) >= 0",
        "date_diff('day', least(order_date, created_at), greatest(shipped_at, created_at)) >= 0",
        "date_part('year', iif(amount > 10, shipped_at, created_at)) = 2024",
        "date_part('year', CASE WHEN amount > 10 THEN order_date ELSE NULL END) = 2024",
        "date_diff('day', '2024-01-01', created_at) < 30",
        "date_part('year', coalesce(shipped_at, '2024-01-01')) = 2024",
        "created_at + interval(1, 'month') + interval(2, 'hour') < now()",
        "interval(7, 'day') + order_date > current_date()",
    ])
    async def test_filter_binds(self, engine, filt: str) -> None:
        await _dry(engine, filters=[filt])

    @pytest.mark.parametrize("formula", [
        "date_diff('day', min(created_at), max(created_at))",
        "avg(date_diff('day', created_at, coalesce(shipped_at, now())))",
        "min(date_part('year', coalesce(shipped_at, '2024-01-01')))",
        "date_part('year', first(created_at))",
        "date_part('year', last(order_date))",
        "max(shipped_at - interval(2, 'day'))",
    ])
    async def test_measure_binds(self, engine, formula: str) -> None:
        await _dry(engine, measures=[{"formula": formula, "name": "m"}])

    async def test_placeholder_in_date_position(self, engine) -> None:
        await _dry(
            engine, filters=["date_diff('day', '{launch}', created_at) >= 0"], variables={"launch": "2024-05-01"},
        )

    async def test_placeholder_invalid_date_rejected(self, engine) -> None:
        with pytest.raises(QueryTypeError):
            await _dry(
                engine, filters=["date_diff('day', '{launch}', created_at) >= 0"], variables={"launch": "someday"},
            )

    async def test_placeholder_unit(self, engine) -> None:
        await _dry(engine, filters=["date_diff('{u}', created_at, shipped_at) > 1"], variables={"u": "day"})

    async def test_placeholder_unit_must_be_listed(self, engine) -> None:
        with pytest.raises(ValueError, match="week_sunday"):
            await _dry(engine, filters=["date_diff('{u}', created_at, shipped_at) > 1"], variables={"u": "fortnight"})


class TestArityEndToEnd:
    @pytest.mark.parametrize("filt,name", [
        ("date_add(created_at, 3) > created_at", "date_add"),
        ("date_part('year') = 1", "date_part"),
        ("date_diff('day', created_at) > 1", "date_diff"),
        ("created_at <= now(1)", "now"),
        ("order_date <= current_date(1)", "current_date"),
    ])
    async def test_wrong_arity_rejected_naming_function(self, engine, filt: str, name: str) -> None:
        with pytest.raises(ValueError, match=rf"(?s){name}.*argument"):
            await _dry(engine, filters=[filt])


class TestStageOperands:
    async def test_stage_dimension_column(self, engine) -> None:
        s1 = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders", "dimensions": ["created_at", "status"],
            "measures": [{"formula": "count(*)", "name": "n"}],
        })
        outer = SlayerQuery.model_validate({
            "source_model": "s1", "dimensions": [{"expression": "date_part('year', created_at)", "name": "y"}],
            "measures": [{"formula": "n:sum", "name": "n"}],
        })
        await engine.execute([s1, outer], dry_run=True)

    async def test_stage_measure_column(self, engine) -> None:
        s1 = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders", "dimensions": ["status"],
            "measures": [{"formula": "max(created_at)", "name": "last_at"}],
        })
        outer = SlayerQuery.model_validate({
            "source_model": "s1", "dimensions": ["status"],
            "filters": ["date_diff('day', last_at, now()) > 0"],
            "measures": [{"formula": "count(*)", "name": "n"}],
        })
        await engine.execute([s1, outer], dry_run=True)

    async def test_stage_text_column_rejected(self, engine) -> None:
        s1 = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders", "dimensions": ["status"],
            "measures": [{"formula": "count(*)", "name": "n"}],
        })
        outer = SlayerQuery.model_validate({
            "source_model": "s1", "filters": ["date_part('year', status) = 1"],
            "measures": [{"formula": "n:sum", "name": "n"}],
        })
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute([s1, outer], dry_run=True)
        _assert_type_error(exc, "status")


# ---------------------------------------------------------------------------
# Result types.
# ---------------------------------------------------------------------------

class TestResultTypes:
    @pytest.mark.parametrize("text,expected", [
        ("date_part('month', created_at)", DataType.INT),
        ("date_diff('day', created_at, shipped_at)", DataType.INT),
        ("date_add(order_date, 1, 'month')", DataType.DATE),
        ("date_add(order_date, 2, 'hour')", DataType.TIMESTAMP),
        ("date_add(created_at, 1, 'day')", DataType.TIMESTAMP),
        ("current_date()", DataType.DATE),
        ("now()", DataType.TIMESTAMP),
        ("date_add(customers.signup_date, 1, 'year')", DataType.DATE),
    ])
    def test_dimension_metadata(self, text: str, expected: DataType) -> None:
        dim_type, _fmt, _desc = dimension_key_metadata(model=ORDERS, key=_bind(text), bundle=BUNDLE)
        assert dim_type == expected

    @pytest.mark.parametrize("text,expected", [
        ("date_diff('day', min(created_at), max(created_at))", DataType.INT),
        ("date_part('year', max(order_date))", DataType.INT),
        ("date_add(max(order_date), 1, 'month')", DataType.DATE),
        ("date_add(max(created_at), 1, 'month')", DataType.TIMESTAMP),
        ("current_date()", DataType.DATE),
        ("now()", DataType.TIMESTAMP),
    ])
    def test_measure_type(self, text: str, expected: DataType) -> None:
        assert measure_key_type(model=ORDERS, key=_bind(text)) == expected

    async def test_query_backed_model_column_types(self, engine) -> None:
        model = await engine.create_model_from_query(
            query={
                "source_model": "orders",
                "dimensions": [
                    {"expression": "date_part('month', created_at)", "name": "m"},
                    {"expression": "date_add(order_date, 1, 'month')", "name": "due"},
                    {"expression": "date_add(order_date, 2, 'hour')", "name": "due_at"},
                ],
                "measures": [
                    {"formula": "date_diff('day', min(created_at), max(created_at))", "name": "span"},
                    {"formula": "date_add(max(order_date), 1, 'month')", "name": "next_due"},
                ],
            },
            name="dated", save=False,
        )
        types = {c.name: c.type for c in model.columns}
        assert types["m"] == DataType.INT
        assert types["due"] == DataType.DATE
        assert types["due_at"] == DataType.TIMESTAMP
        assert types["span"] == DataType.INT
        assert types["next_due"] == DataType.DATE

    async def test_joined_and_stage_backed_result_types(self, engine) -> None:
        model = await engine.create_model_from_query(
            query=[
                {
                    "name": "s1", "source_model": "orders", "dimensions": ["status", "created_at"],
                    "measures": [{"formula": "date_add(max(customers.signup_date), 1, 'day')", "name": "joined_due"}],
                },
                {
                    "source_model": "s1",
                    "dimensions": [{"expression": "date_part('year', created_at)", "name": "y"}],
                    "measures": [
                        {"formula": "max(joined_due)", "name": "due"},
                        {"formula": "date_diff('day', max(joined_due), max(created_at))", "name": "lag"},
                        {"formula": "date_add(max(created_at), 1, 'hour')", "name": "next_at"},
                    ],
                },
            ],
            name="staged", save=False,
        )
        types = {c.name: c.type for c in model.columns}
        assert types["y"] == DataType.INT
        assert types["due"] == DataType.DATE
        assert types["lag"] == DataType.INT
        assert types["next_at"] == DataType.TIMESTAMP

    async def test_date_add_stage_column_is_a_time_axis(self, engine) -> None:
        s1 = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders",
            "dimensions": [{"expression": "date_add(order_date, 1, 'month')", "name": "due"}],
            "measures": [{"formula": "count(*)", "name": "n"}],
        })
        outer = SlayerQuery.model_validate({
            "source_model": "s1",
            "time_dimensions": [TimeDimension(dimension=ColumnRef(name="due"), granularity=TimeGranularity.MONTH)],
            "measures": [{"formula": "n:sum", "name": "n"}],
        })
        await engine.execute([s1, outer], dry_run=True)

    async def test_date_part_stage_column_is_not_a_time_axis(self, engine) -> None:
        s1 = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders",
            "dimensions": [{"expression": "date_part('month', created_at)", "name": "m"}],
            "measures": [{"formula": "count(*)", "name": "n"}],
        })
        outer = SlayerQuery.model_validate({
            "source_model": "s1",
            "time_dimensions": [TimeDimension(dimension=ColumnRef(name="m"), granularity=TimeGranularity.MONTH)],
            "measures": [{"formula": "n:sum", "name": "n"}],
        })
        with pytest.raises(TimeDimensionColumnError, match="INT"):
            await engine.execute([s1, outer], dry_run=True)
