"""An unnamed expression aggregate's result key is the sanitized formula text of its source."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

import pytest

from slayer.core.errors import DuplicateMeasureNameError, NameCollisionError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Grain,
    LiteralKey,
    Phase,
    _FrozenKey,
)
from slayer.core.query import SlayerQuery
from slayer.core.refs import auto_name_from_expression, key_display
from slayer.engine.compile.projection import ValueRegistry
from slayer.sql.naming import canonical_aggregate_alias, expression_source_leaf

from tests._dev2070_fixtures import make_exec_engine

_CLOCK_2024_03 = datetime(2024, 3, 20, 12, 0)
_CLOCK_2025_02 = datetime(2025, 2, 15, 12, 0)
_INTERNAL_TOKENS = ("columnkey", "aggregatekey", "transformkey", "timetrunckey", "op_", "input_", "source_")


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request.param, clock=lambda: _CLOCK_2024_03):
        yield e


def q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery.model_validate(kw)


async def only_value(engine, formula: str, key: str, **kw):
    """Execute ``formula`` as the query's one measure; assert its key, return the single value."""
    resp = await engine.execute(q(measures=[formula], **kw))
    assert key in resp.columns, resp.columns
    assert len(resp.data) == 1, resp.data
    return resp.data[0][key]


def by(resp, *, dim: str, key: str) -> dict:
    assert key in resp.columns, resp.columns
    return {r[dim]: r[key] for r in resp.data}


# --------------------------------------------------------------------------- #
# Date comparisons
# --------------------------------------------------------------------------- #
REPRO = "sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))"
REPRO_KEY = "regions.customers.orders.iif_order_date_2025_01_01_1_0_sum"


async def test_repro_date_comparison_inside_a_scalar_call(engine) -> None:
    for _ in range(2):
        resp = await engine.execute(q(source_model="regions", dimensions=["name"], measures=[REPRO]))
        assert by(resp, dim="regions.name", key=REPRO_KEY) == {"north": 1, "south": 1}


@pytest.mark.parametrize(
    ("formula", "key", "value"),
    [
        ("sum(iif(ordered_at == '2024-02', 1, 0))", "orders.iif_ordered_at_2024_02_1_0_sum", 2),
        ("sum(iif(ordered_at != '2024-02', 1, 0))", "orders.iif_ordered_at_2024_02_1_0_sum", 4),
        ("sum(iif(month(ordered_at) >= '2024-02', 1, 0))", "orders.iif_month_ordered_at_2024_02_1_0_sum", 5),
        ("sum(amount > 5 and ordered_at >= '2024-02-01')", "orders.amount_5_and_ordered_at_2024_02_01_sum", 4),
    ],
)
async def test_date_comparison_forms(engine, formula, key, value) -> None:
    assert await only_value(engine, formula, key) == value


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
@pytest.mark.parametrize(("now", "value"), [(_CLOCK_2024_03, 5), (_CLOCK_2025_02, 2)])
async def test_relative_point_names_by_its_text_under_any_clock(dialect, now, value) -> None:
    async for e in make_exec_engine(dialect, clock=lambda: now):
        assert await only_value(
            e, "sum(iif(ordered_at >= 'last month', 1, 0))", "orders.iif_ordered_at_last_month_1_0_sum",
        ) == value


async def test_date_comparison_as_the_whole_source(engine) -> None:
    assert await only_value(engine, "sum(ordered_at >= '2024-02-01')", "orders.ordered_at_2024_02_01_sum") == 5


async def test_plain_comparison_as_the_whole_source(engine) -> None:
    assert await only_value(engine, "sum(amount > 5)", "orders.amount_5_sum") == 5


# --------------------------------------------------------------------------- #
# Nested constituents and literals
# --------------------------------------------------------------------------- #
async def test_nested_aggregate_operand(engine) -> None:
    value = await only_value(
        engine, "sum(amount * avg(amount, partition_by=status))", "orders.amount_avg_amount_partition_by_status_sum",
    )
    assert value == pytest.approx(5456.25)


NESTED_TRANSFORM_KEY = (
    "orders." + auto_name_from_expression("cumsum(sum(amount, partition_by=[ordered_at, status])) - 1") + "_sum"
)


@pytest.mark.parametrize("partition", ["[status, ordered_at]", "[ordered_at, status]"])
async def test_nested_transform_operand(engine, partition) -> None:
    formula = f"sum(cumsum(sum(amount, partition_by={partition})) - 1)"
    month = {"dimension": "ordered_at", "granularity": "month"}
    for _ in range(2):
        resp = await engine.execute(q(measures=[formula], time_dimensions=[month]))
        key = next(c for c in resp.columns if c != "orders.ordered_at")
        assert key == NESTED_TRANSFORM_KEY
        assert not any(t in key for t in _INTERNAL_TOKENS), key
        assert {str(r["orders.ordered_at"])[:7]: r[key] for r in resp.data} == {
            "2024-01": 9, "2024-02": 58, "2024-03": 34, "2025-01": 79, "2025-02": 94,
        }


async def test_decimal_literal_spelled_plainly(engine) -> None:
    value = await only_value(engine, "sum(amount * 0.0000001)", "orders.amount_0_0000001_sum")
    assert value == pytest.approx(0.0000175)


async def test_null_literal_spelled_null(engine) -> None:
    assert await only_value(engine, "sum(coalesce(amount, null))", "orders.coalesce_amount_null_sum") == 175


# --------------------------------------------------------------------------- #
# Home-relative operand spelling
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("formula", "key", "value"),
    [
        ("sum(customers.spend - 1)", "orders.customers.spend_1_sum", 347),
        ("sum(customers.spend - customers.regions.weight)", "orders.customers.spend_regions_weight_sum", 343),
        ("sum(customers.spend_x2 + 1)", "orders.customers.spend_x2_1_sum", 703),
        (
            "sum(iif(month(customers.signup_at) >= '2024-02', 1, 0))",
            "orders.customers.iif_month_signup_at_2024_02_1_0_sum", 2,
        ),
        (
            "sum(iif(customers.region_id in (1, 3), customers.spend, 0))",
            "orders.customers.iif_region_id_in_1_3_spend_0_sum", 150,
        ),
        ("sum(amount - customers.spend)", "orders.amount_customers_spend_sum", -525),
    ],
)
async def test_home_relative_operand_spelling(engine, formula, key, value) -> None:
    assert await only_value(engine, formula, key) == value


async def test_nested_constituent_keeps_its_spelling(engine) -> None:
    key = "orders.customers." + auto_name_from_expression(
        "spend * sum(amount, partition_by=[customers.regions.name])",
    ) + "_sum"
    formula = "sum(customers.spend * sum(amount, partition_by=customers.regions.name))"
    assert await only_value(engine, formula, key) == 29000


# --------------------------------------------------------------------------- #
# Collisions
# --------------------------------------------------------------------------- #
async def test_equal_and_not_equal_date_comparisons_collide_loudly(engine) -> None:
    eq, ne = "sum(iif(ordered_at == '2024-02', 1, 0))", "sum(iif(ordered_at != '2024-02', 1, 0))"
    with pytest.raises(NameCollisionError, match="(?s)Rename") as exc:
        await engine.execute(q(measures=[eq, ne]))
    assert eq in str(exc.value) and ne in str(exc.value)


def _outer_over_inner(*, locus: Literal["target", "host"]) -> AggregateKey:
    inner = AggregateKey(
        source=ColumnKey(leaf="amount"), agg="sum", locus=locus,
        partition_keys=Grain.of([ColumnKey(leaf="status")]),
    )
    return AggregateKey(source=ArithmeticKey(op="*", operands=(ColumnKey(leaf="amount"), inner)), agg="sum")


def test_constituents_differing_only_in_locus_never_share_a_slot() -> None:
    target, host = _outer_over_inner(locus="target"), _outer_over_inner(locus="host")
    assert target != host
    alias = canonical_aggregate_alias(target, profile="stage_formula")
    assert alias == canonical_aggregate_alias(host, profile="stage_formula")
    assert alias is not None

    public = ValueRegistry()
    public.intern(key=target, declared_name=alias, public_name=alias, phase=Phase.AGGREGATE)
    with pytest.raises(DuplicateMeasureNameError):
        public.intern(key=host, declared_name=alias, public_name=alias, phase=Phase.AGGREGATE)

    hidden = ValueRegistry()
    a = hidden.intern(key=target, declared_name=alias, phase=Phase.AGGREGATE, hidden=True)
    b = hidden.intern(key=host, declared_name=alias, phase=Phase.AGGREGATE, hidden=True)
    assert a != b
    assert hidden.get(a).declared_name != hidden.get(b).declared_name


# --------------------------------------------------------------------------- #
# Derived keys are referenceable
# --------------------------------------------------------------------------- #
_REFERENCED = [
    pytest.param(
        {"source_model": "regions", "dimensions": ["name"],
         "measures": ["sum(iif(customers.orders.order_date >= '2024-02-01', 1, 0))"]},
        "name", "customers.orders.iif_order_date_2024_02_01_1_0_sum",
        {"north": 3, "south": 2}, id="pathed-date-comparison",
    ),
    pytest.param(
        {"source_model": "orders", "dimensions": ["status"],
         "measures": ["sum(iif(ordered_at >= 'last month', 1, 0))"]},
        "status", "iif_ordered_at_last_month_1_0_sum",
        {"paid": 3, "pending": 2}, id="relative-point",
    ),
    pytest.param(
        {"source_model": "orders", "dimensions": ["customers.regions.name"],
         "measures": ["sum(customers.spend - 1)"]},
        "customers.regions.name", "customers.spend_1_sum",
        {"north": 148, "south": 199}, id="pathed-operand",
    ),
]


def _rows(resp, *, root: str, dim: str, key: str) -> list:
    return [(r[f"{root}.{dim}"], r[f"{root}.{key}"]) for r in resp.data]


@pytest.mark.parametrize(("query", "dim", "key", "values"), _REFERENCED)
async def test_derived_key_in_filter(engine, query, dim, key, values) -> None:
    root, top = query["source_model"], max(values.values())
    resp = await engine.execute(SlayerQuery.model_validate({**query, "filters": [f"{key} >= {top}"]}))
    assert _rows(resp, root=root, dim=dim, key=key) == [(k, v) for k, v in values.items() if v == top]


@pytest.mark.parametrize(("query", "dim", "key", "values"), _REFERENCED)
async def test_derived_key_in_order(engine, query, dim, key, values) -> None:
    root = query["source_model"]
    for direction, reverse in (("asc", False), ("desc", True)):
        resp = await engine.execute(SlayerQuery.model_validate(
            {**query, "order": [{"column": key, "direction": direction}]},
        ))
        assert _rows(resp, root=root, dim=dim, key=key) == sorted(
            values.items(), key=lambda kv: kv[1], reverse=reverse,
        )


@pytest.mark.parametrize(("query", "dim", "key", "values"), _REFERENCED)
async def test_derived_key_from_a_downstream_stage(engine, query, dim, key, values) -> None:
    flat_dim, flat_key = dim.replace(".", "__"), key.replace(".", "__")
    inner = SlayerQuery.model_validate({**query, "name": "s"})
    outer = SlayerQuery.model_validate({"source_model": "s", "dimensions": [flat_dim], "measures": [f"max({flat_key})"]})
    resp = await engine.execute([inner, outer])
    assert {r[f"s.{flat_dim}"]: r[f"s.{flat_key}_max"] for r in resp.data} == values


# --------------------------------------------------------------------------- #
# The leaf derivation
# --------------------------------------------------------------------------- #
def _col(*path: str) -> ColumnKey:
    return ColumnKey(path=tuple(path[:-1]), leaf=path[-1])


class _AlienKey(_FrozenKey, frozen=True):
    def children(self):
        return ()

    def map_children(self, fn):
        return self


class TestExpressionSourceLeaf:
    def test_anchor_is_stripped_and_the_remainder_kept(self) -> None:
        source = ArithmeticKey(op="-", operands=(_col("customers", "regions", "weight"), _col("customers", "spend")))
        assert expression_source_leaf(source) == "regions_weight_spend"

    def test_nested_aggregate_interior_is_not_stripped(self) -> None:
        nested = AggregateKey(source=_col("customers", "spend"), agg="sum")
        source = ArithmeticKey(op="*", operands=(_col("customers", "spend"), nested))
        assert expression_source_leaf(source) == "spend_sum_customers_spend"

    def test_root_anchor_renders_the_formula_text(self) -> None:
        source = ArithmeticKey(op="-", operands=(_col("amount"), _col("customers", "spend")))
        assert expression_source_leaf(source) == auto_name_from_expression(key_display(source))
        assert expression_source_leaf(source) == "amount_customers_spend"

    def test_literal_operands(self) -> None:
        source = ArithmeticKey(op="*", operands=(_col("amount"), LiteralKey(value=Decimal("0.0000001"))))
        assert expression_source_leaf(source) == "amount_0_0000001"

    def test_unknown_key_kind_fails_closed(self) -> None:
        with pytest.raises(TypeError):
            expression_source_leaf(_AlienKey())
