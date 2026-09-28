"""A population cell with no home rows for an attached aggregate takes the aggregation's empty
value: 0 for the count family, NULL otherwise.

Spec: openspec …/specs/queries/cross-model-aggregates — "Empty cells take the aggregation's
empty value". Executed on SQLite and DuckDB against ``tests/_dev1994_fixtures.py``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from pydantic import ValidationError

from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Aggregation
from slayer.ir.planned import RegroupSubstitution
from tests._dev1994_fixtures import (
    AVG_BY_CUSTOMER,
    AVG_ORDERS_PER_CUSTOMER,
    AVG_ORDERS_PER_CUSTOMER_BY_REGION,
    CHANGE_BY_MONTH,
    CHILDLESS,
    CUMSUM_BY_MONTH,
    CUSTOMERS_BY_REGION,
    HAS_NON_BAD_ORDER,
    LAST_BY_CUSTOMER,
    ORDERS_BY_CUSTOMER,
    ORDERS_BY_MONTH,
    ORDERS_BY_REGION,
    REGION_OF,
    SHIFTED_BY_MONTH,
    SUM_BY_CUSTOMER,
    by,
    col,
    customers_model,
    dev1994_engine,
    m,
    month,
    orders_model,
    query,
    regions_model,
)

MONTH_TD = [{"dimension": "signup_date", "granularity": "month"}]
COUNT_OVERRIDE = Aggregation(name="count", formula="COUNT({value})")
COUNT_DECLARED = Aggregation(name="count")


@pytest.fixture(params=["sqlite", "duckdb"])
def dialect(request) -> str:
    return request.param


async def _run(dialect: str, models=None, **kw: Any):
    async with dev1994_engine(dialect, models=models) as e:
        return await e.execute(query(**kw))


def _ints(d: Dict[Any, Any]) -> Dict[Any, Optional[int]]:
    return {k: None if v is None else int(v) for k, v in d.items()}


def _by_month(resp, value: str) -> Dict[str, Any]:
    return {month(k): v for k, v in by(resp.data, resp.columns, key="signup_date", value=value).items()}


def _names(resp) -> List[str]:
    k = col(resp.columns, "name")
    return [r[k] for r in resp.data]


# --------------------------------------------------------------------------- #
# Single hop, inferred population, multi-hop, associate.
# --------------------------------------------------------------------------- #
class TestSingleHop:
    @pytest.mark.parametrize("formula", [
        "count(orders.id)", "count(orders.*)", "count_distinct(orders.id)",
        "count_distinct_approx(orders.id)",
    ])
    async def test_childless_customer_counts_zero(self, dialect, formula) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          measures=[m(formula, "c")])
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == ORDERS_BY_CUSTOMER

    async def test_all_four_in_one_query(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"], measures=[
            m("count(orders.id)", "c"), m("count(orders.*)", "cs"),
            m("count_distinct(orders.id)", "cd"), m("count_distinct_approx(orders.id)", "ca")])
        for name in ("c", "cs", "cd", "ca"):
            assert _ints(by(resp.data, resp.columns, key="name", value=name)) == ORDERS_BY_CUSTOMER

    async def test_no_population_cell_is_fabricated(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          measures=[m("count(orders.id)", "c")])
        assert sorted(_names(resp)) == sorted(ORDERS_BY_CUSTOMER)


class TestNonCountStaysNull:
    async def test_sum_and_avg_are_null_for_childless(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"], measures=[
            m("sum(orders.amount)", "s"), m("avg(orders.amount)", "a"), m("count(orders.id)", "c")])
        assert by(resp.data, resp.columns, key="name", value="s") == pytest.approx(SUM_BY_CUSTOMER)
        assert by(resp.data, resp.columns, key="name", value="a") == pytest.approx(AVG_BY_CUSTOMER)
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == ORDERS_BY_CUSTOMER

    async def test_ranked_last_is_null_for_childless(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"], measures=[
            m("last(orders.amount)", "l"), m("count(orders.id)", "c")])
        assert by(resp.data, resp.columns, key="name", value="l") == pytest.approx(LAST_BY_CUSTOMER)
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == ORDERS_BY_CUSTOMER


async def test_inferred_population(dialect) -> None:
    resp = await _run(dialect, dimensions=["customers.name"], measures=[m("count(orders.id)", "c")])
    assert _ints(by(resp.data, resp.columns, key="name", value="c")) == ORDERS_BY_CUSTOMER


async def test_multi_hop(dialect) -> None:
    resp = await _run(dialect, source_model="regions", dimensions=["name"], measures=[
        m("count(customers.orders.id)", "co"), m("count(customers.id)", "cc")])
    assert _ints(by(resp.data, resp.columns, key="name", value="co")) == ORDERS_BY_REGION
    assert _ints(by(resp.data, resp.columns, key="name", value="cc")) == CUSTOMERS_BY_REGION


@pytest.mark.parametrize("mode", ["associate", "error"])
class TestToManyHandling:
    async def test_single_hop(self, dialect, mode) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          to_many_handling=mode, measures=[m("count(orders.id)", "c")])
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == ORDERS_BY_CUSTOMER

    async def test_multi_hop(self, dialect, mode) -> None:
        resp = await _run(dialect, source_model="regions", dimensions=["name"],
                          to_many_handling=mode, measures=[
                              m("count(customers.orders.id)", "co"), m("count(customers.id)", "cc")])
        assert _ints(by(resp.data, resp.columns, key="name", value="co")) == ORDERS_BY_REGION
        assert _ints(by(resp.data, resp.columns, key="name", value="cc")) == CUSTOMERS_BY_REGION


# --------------------------------------------------------------------------- #
# Windowed.
# --------------------------------------------------------------------------- #
class TestWindowed:
    async def test_per_customer_cells(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          time_dimensions=MONTH_TD,
                          measures=[m("count(orders.id, window='30d')", "w")])
        assert _ints(by(resp.data, resp.columns, key="name", value="w")) == ORDERS_BY_CUSTOMER

    async def test_per_month_cells(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", time_dimensions=MONTH_TD,
                          measures=[m("count(orders.id, window='30d')", "w")])
        assert _ints(_by_month(resp, "w")) == ORDERS_BY_MONTH


# --------------------------------------------------------------------------- #
# Every position.
# --------------------------------------------------------------------------- #
class TestPositions:
    async def test_composite(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          measures=[m("count(orders.id) * 2", "c2")])
        assert _ints(by(resp.data, resp.columns, key="name", value="c2")) == {
            k: 2 * v for k, v in ORDERS_BY_CUSTOMER.items()}

    async def test_measure_filter_keeps_childless(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          filters=["count(orders.id) = 0"])
        assert sorted(_names(resp)) == sorted(CHILDLESS)

    async def test_order_only_key(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"], order=[
            {"column": "count(orders.id)", "direction": "asc"},
            {"column": "name", "direction": "asc"}])
        assert _names(resp) == ["Eve", "Frank", "Grace", "Bob", "Dave", "Alice", "Heidi", "Carol"]

    async def test_computed_dimension(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=[
            "name", {"expression": "count(orders.id, partition_by=[id]) == 0", "name": "none"}],
            measures=[m("count(*)", "n")])
        got = by(resp.data, resp.columns, key="name", value="none")
        assert {k: None if v is None else bool(v) for k, v in got.items()} == {
            k: k in CHILDLESS for k in ORDERS_BY_CUSTOMER}

    async def test_every_position_in_one_query(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=[
            "name", {"expression": "count(orders.id, partition_by=[id]) == 0", "name": "none"}],
            measures=[m("count(orders.id) * 2", "c2")],
            filters=["count(orders.id) = 0"],
            order=[{"column": "count(orders.id)", "direction": "asc"},
                   {"column": "name", "direction": "asc"}])
        assert _names(resp) == list(CHILDLESS)
        assert _ints(by(resp.data, resp.columns, key="name", value="c2")) == dict.fromkeys(CHILDLESS, 0)
        got = by(resp.data, resp.columns, key="name", value="none")
        assert {k: None if v is None else bool(v) for k, v in got.items()} == dict.fromkeys(CHILDLESS, True)


async def test_coarser_partition_broadcasts_zero(dialect) -> None:
    resp = await _run(dialect, source_model="customers", dimensions=["regions.name", "name"],
                      measures=[m("count(orders.id, partition_by=[regions.name])", "rc")])
    rc = col(resp.columns, "rc")
    got = {r[col(resp.columns, "customers.name")]: r[rc] for r in resp.data}
    assert _ints(got) == {name: ORDERS_BY_REGION[REGION_OF[name]] for name in ORDERS_BY_CUSTOMER}


# --------------------------------------------------------------------------- #
# Re-aggregation and transforms.
# --------------------------------------------------------------------------- #
class TestReaggregation:
    async def test_avg_counts_the_zeros(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers",
                          measures=[m("avg(count(orders.id, partition_by=[name]))", "a")])
        (row,) = resp.data
        assert row[col(resp.columns, "a")] == pytest.approx(AVG_ORDERS_PER_CUSTOMER)

    async def test_avg_per_region_counts_the_zeros(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["regions.name"], measures=[
            m("avg(count(orders.id, partition_by=[regions.name, name]))", "a")])
        assert by(resp.data, resp.columns, key="name", value="a") == pytest.approx(
            AVG_ORDERS_PER_CUSTOMER_BY_REGION)


class TestTransforms:
    async def test_transforms_consume_zero_and_missing_shift_is_null(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", time_dimensions=MONTH_TD, measures=[
            m("count(orders.id)", "c"), m("cumsum(count(orders.id))", "cs"),
            m("change(count(orders.id))", "ch"), m("time_shift(count(orders.id), -1)", "ts")])
        assert _ints(_by_month(resp, "c")) == ORDERS_BY_MONTH
        assert _ints(_by_month(resp, "cs")) == CUMSUM_BY_MONTH
        assert _ints(_by_month(resp, "ch")) == CHANGE_BY_MONTH
        assert _ints(_by_month(resp, "ts")) == SHIFTED_BY_MONTH


# --------------------------------------------------------------------------- #
# Model-level declarations: consulted on the owning model.
# --------------------------------------------------------------------------- #
class TestOverrides:
    async def test_formula_override_on_owner_stays_null(self, dialect) -> None:
        models = [regions_model(), customers_model(), orders_model(aggregations=[COUNT_OVERRIDE])]
        resp = await _run(dialect, models=models, source_model="customers", dimensions=["name"],
                          measures=[m("count(orders.id)", "c")])
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == {
            k: None if k in CHILDLESS else v for k, v in ORDERS_BY_CUSTOMER.items()}

    async def test_formula_less_declaration_keeps_zero(self, dialect) -> None:
        models = [regions_model(), customers_model(), orders_model(aggregations=[COUNT_DECLARED])]
        resp = await _run(dialect, models=models, source_model="customers", dimensions=["name"],
                          measures=[m("count(orders.id)", "c")])
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == ORDERS_BY_CUSTOMER

    async def test_custom_aggregation_stays_null(self, dialect) -> None:
        my_count = Aggregation(name="my_count", formula="COUNT({value})")
        models = [regions_model(), customers_model(), orders_model(aggregations=[my_count])]
        resp = await _run(dialect, models=models, source_model="customers", dimensions=["name"],
                          measures=[m("my_count(orders.amount)", "c")])
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == {
            k: None if k in CHILDLESS else v for k, v in ORDERS_BY_CUSTOMER.items()}

    @pytest.mark.parametrize(("declared", "childless"), [(COUNT_OVERRIDE, None), (COUNT_DECLARED, 0)])
    async def test_windowed_consults_the_owner(self, dialect, declared, childless) -> None:
        models = [regions_model(), customers_model(), orders_model(aggregations=[declared])]
        resp = await _run(dialect, models=models, source_model="customers", dimensions=["name"],
                          time_dimensions=MONTH_TD,
                          measures=[m("count(orders.id, window='30d')", "w")])
        assert _ints(by(resp.data, resp.columns, key="name", value="w")) == {
            k: childless if k in CHILDLESS else v for k, v in ORDERS_BY_CUSTOMER.items()}

    async def test_override_on_query_root_does_not_apply(self, dialect) -> None:
        models = [regions_model(), customers_model(aggregations=[COUNT_OVERRIDE]), orders_model()]
        resp = await _run(dialect, models=models, source_model="customers", dimensions=["name"],
                          measures=[m("count(orders.id)", "c")])
        assert _ints(by(resp.data, resp.columns, key="name", value="c")) == ORDERS_BY_CUSTOMER

    async def test_multi_hop_consults_the_terminal_owner(self, dialect) -> None:
        models = [regions_model(aggregations=[COUNT_OVERRIDE]),
                  customers_model(aggregations=[COUNT_OVERRIDE]), orders_model()]
        resp = await _run(dialect, models=models, source_model="regions", dimensions=["name"],
                          measures=[m("count(customers.orders.id)", "co"),
                                    m("count(customers.id)", "cc")])
        assert _ints(by(resp.data, resp.columns, key="name", value="co")) == ORDERS_BY_REGION
        assert _ints(by(resp.data, resp.columns, key="name", value="cc")) == {
            k: None if v == 0 else v for k, v in CUSTOMERS_BY_REGION.items()}


# --------------------------------------------------------------------------- #
# Negated condition on a joined model does not mean "has none" (docs sentence).
# --------------------------------------------------------------------------- #
class TestNegatedJoinedFilter:
    async def test_negation_requires_a_matching_child(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          filters=["not orders.status = 'bad'"])
        assert sorted(_names(resp)) == sorted(HAS_NON_BAD_ORDER)

    async def test_or_id_is_null_includes_childless(self, dialect) -> None:
        resp = await _run(dialect, source_model="customers", dimensions=["name"],
                          filters=["not orders.status = 'bad' or orders.id is null"])
        assert sorted(_names(resp)) == sorted([*HAS_NON_BAD_ORDER, *CHILDLESS])


# --------------------------------------------------------------------------- #
# IR: every synthesiser decides the empty value.
# --------------------------------------------------------------------------- #
_KEY = AggregateKey(source=ColumnKey(path=("orders",), leaf="id"), agg="count")
_PLACEHOLDER = ColumnKey(path=(), leaf="__regroup__0__id_count")


class TestRegroupSubstitutionIR:
    def test_empty_value_is_required(self) -> None:
        with pytest.raises(ValidationError, match="empty_value"):
            RegroupSubstitution(placeholder=_PLACEHOLDER, producer_slot_id="p1", original_key=_KEY)  # pyright: ignore[reportCallIssue]

    @pytest.mark.parametrize("value", [0, None])
    def test_empty_value_is_carried(self, value) -> None:
        sub = RegroupSubstitution(
            placeholder=_PLACEHOLDER, producer_slot_id="p1", original_key=_KEY, empty_value=value)
        assert sub.empty_value == value
