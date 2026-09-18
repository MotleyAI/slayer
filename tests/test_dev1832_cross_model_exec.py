"""DEV-1832 task 1.4 — executed cross-model expression aggregation.

Every ``#### Scenario`` of ``aggregations/expression-aggregation`` › *Row-level
expressions can be aggregated* and the execution/typing scenarios of *Expression
source typing*, plus ``queries/cross-model-aggregates``. Executed on SQLite and
DuckDB. Same-model shapes and fully-attached / mixed shapes already compile;
the cross-model shapes now compile too — all pass here as regression guards.
"""

from __future__ import annotations

import re

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1832_fixtures import (
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _REGIONS_ROWS,
    _STORES_ROWS,
    AVG_CITY_TOTAL_BY_REGION,
    Aggregation,
    Column,
    DataType,
    HOST_DISCOUNT_BY_STATUS,
    MIXED_SUM_BY_REGION,
    ModelMeasure,
    dev1832_models,
    dev1832_unparseable_models,
    gen,
    make_exec_engine,
    month_td,
    orders_q,
    rows_by,
    sales_q,
)

_C_REGION, _C_TIER, _C_SPEND = 1, 3, 4
_O_CUST, _O_STATUS, _O_STORE_CO, _O_STORE_NO = 1, 2, 6, 7
_SPEND = {r[0]: r[_C_SPEND] for r in _CUSTOMERS_ROWS}
_C_REGION_OF = {r[0]: r[_C_REGION] for r in _CUSTOMERS_ROWS}
_C_TIER_OF = {r[0]: r[_C_TIER] for r in _CUSTOMERS_ROWS}
_REGION_POP = {r[0]: r[2] for r in _REGIONS_ROWS}
_RENT = {(r[0], r[1]): r[3] for r in _STORES_ROWS}


def _wsum_anchor_by_tier() -> dict:
    """wsum(customers.spend - customers.regions.pop), home customers, by tier:
    every customer counted once — even one with no orders — exactly as
    ``_spend_minus_pop_by_tier`` (Option 1; consented 2026-09-16)."""
    by: dict = {}
    for cust in _SPEND:
        pop = _REGION_POP.get(_C_REGION_OF[cust])
        spend = _SPEND[cust]
        v = None if pop is None else (spend - pop) * spend
        by.setdefault(_C_TIER_OF[cust], []).append(v)
    return {t: (None if all(x is None for x in vs)
                else sum(x for x in vs if x is not None)) for t, vs in by.items()}


def _spend_minus_pop_by_tier() -> dict:
    """sum(customers.spend - customers.regions.pop), home customers, by tier: every
    customer counted once with its own region's pop (a to-one determined field), NULL
    where the region is absent. Homed at customers, so ALL customers count — even one
    with no orders (metric independence). NOT the colon twin, whose pop:sum homes at
    regions (per-region broadcast); the expression homes the whole term at customers."""
    by: dict = {}
    for r in _CUSTOMERS_ROWS:
        pop = _REGION_POP.get(r[_C_REGION])
        v = None if pop is None else r[_C_SPEND] - pop
        by.setdefault(r[_C_TIER], []).append(v)
    return {t: (None if all(x is None for x in vs)
                else sum(x for x in vs if x is not None)) for t, vs in by.items()}


def _tier_cells(resp, name: str) -> dict:
    return {k[0]: v[f"orders.{name}"]
            for k, v in rows_by(resp, "orders.customers.tier").items()}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_net(request):
    """Engine whose orders carry a derived ``net`` column and a ``my_agg``."""
    models = dev1832_models()
    orders = next(m for m in models if m.name == "orders")
    orders.columns.append(Column(name="net", type=DataType.DOUBLE, sql="amount - cost"))
    orders.aggregations = [*(orders.aggregations or []),
                           Aggregation(name="my_agg", formula="AVG({value})")]
    async for e in make_exec_engine(request, models=models):
        yield e


def _cells(resp, dim: str, name: str, root: str = "orders") -> dict:
    return {k[0]: v[f"{root}.{name}"] for k, v in rows_by(resp, f"{root}.{dim}").items()}


async def _val(engine, formula: str, **q) -> dict:
    """{status cell: value} for a single measure grouped by status."""
    resp = await engine.execute(
        orders_q(dimensions=["status"], measures=[ModelMeasure(formula=formula, name="m")], **q))
    return _cells(resp, "status", "m")


def _two_branch_by_status() -> dict:
    """sum(customers.spend - stores.rent) homed at orders, by status (raw-row oracle)."""
    by: dict = {}
    for o in _ORDERS_ROWS:
        spend = _SPEND.get(o[_O_CUST]) if o[_O_CUST] is not None else None
        if spend is None:
            continue
        v = spend - _RENT[(o[_O_STORE_CO], o[_O_STORE_NO])]
        by[o[_O_STATUS]] = by.get(o[_O_STATUS], 0.0) + v
    return by


# --------------------------------------------------------------------------- #
# Row-level expressions can be aggregated — same-model (regression guards).
# --------------------------------------------------------------------------- #
class TestSameModelExpressions:
    async def test_arithmetic_expression(self, exec_backend):
        got = await _val(exec_backend, "sum(amount - cost)")
        twin = await _val(exec_backend, "amount:sum - cost:sum")
        assert got == pytest.approx(twin)

    async def test_scalar_function_inside(self, exec_backend):
        resp = await exec_backend.execute(
            orders_q(measures=[ModelMeasure(formula="count_distinct(upper(channel))", name="m")]))
        assert int(resp.data[0]["orders.m"]) == 2

    async def test_constant_only_expression(self, exec_backend):
        resp = await exec_backend.execute(
            orders_q(measures=[ModelMeasure(formula="count(1)", name="m")]))
        assert int(resp.data[0]["orders.m"]) == len(_ORDERS_ROWS)

    async def test_parametric_over_expression(self, exec_backend):
        resp = await exec_backend.execute(
            orders_q(measures=[ModelMeasure(formula="percentile(amount * quantity, p=0.5)", name="m")]))
        assert resp.data[0]["orders.m"] is not None

    async def test_custom_over_expression(self, exec_net):
        resp = await exec_net.execute(
            orders_q(measures=[ModelMeasure(formula="my_agg(amount * quantity)", name="m")]))
        assert resp.data[0]["orders.m"] is not None

    async def test_derived_sql_column_operand(self, exec_net):
        # net = amount - cost; sum(net * 2) == 2 * sum(amount - cost).
        got = await _val(exec_net, "sum(net * 2)")
        twin = await _val(exec_net, "sum((amount - cost) * 2)")
        assert got == pytest.approx(twin)

    async def test_post_aggregation_filter(self, exec_backend):
        base = await _val(exec_backend, "sum(amount - cost)")
        resp = await exec_backend.execute(orders_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="sum(amount - cost)", name="m")],
            filters=["sum(amount - cost) > 45"]))
        kept = _cells(resp, "status", "m")
        assert set(kept) <= set(base)  # HAVING prunes rows, never invents them
        assert all(v > 45 for v in kept.values())


# --------------------------------------------------------------------------- #
# Cross-model expression sources (the lifted boundary — fail on current tree).
# --------------------------------------------------------------------------- #
class TestCrossModelExpressions:
    async def test_host_homed_expression(self, exec_backend):
        assert await _val(exec_backend, "sum(amount - customers.discount)") == \
            pytest.approx(HOST_DISCOUNT_BY_STATUS)

    async def test_target_homed_expression_counts_each_customer_once(self, exec_backend):
        resp = await exec_backend.execute(orders_q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="sum(customers.spend - customers.regions.pop)", name="m")]))
        got = _tier_cells(resp, "m")
        for tier, expected in _spend_minus_pop_by_tier().items():
            if expected is None:
                assert got[tier] is None, tier
            else:
                assert got[tier] == pytest.approx(expected), tier

    async def test_spelling_never_moves_the_home(self, exec_backend):
        plain = await _val(exec_backend, "sum(customers.spend)")
        spelled = await _val(exec_backend, "sum(customers.spend + 0)")
        assert plain == pytest.approx(spelled)

    async def test_two_branch_homes_at_common_ancestor(self, exec_backend):
        assert await _val(exec_backend, "sum(customers.spend - stores.rent)") == \
            pytest.approx(_two_branch_by_status())

    async def test_explicit_grain_matches_the_query_grain_value(self, exec_backend):
        # partition_by=status over a status-grouped query == the ungrouped homed value.
        assert await _val(exec_backend, "sum(amount - customers.discount, partition_by=status)") == \
            pytest.approx(HOST_DISCOUNT_BY_STATUS)

    async def test_window_is_cardinality_neutral_and_valued_every_bucket(self, exec_backend):
        base = await exec_backend.execute(orders_q(
            time_dimensions=month_td(), measures=[ModelMeasure(formula="amount:sum", name="a")]))
        withw = await exec_backend.execute(orders_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="a"),
                      ModelMeasure(formula="sum(amount - customers.discount, window='90d')", name="m")]))
        assert len(withw.data) == len(base.data)
        # Every bucket has orders, so the trailing-window measure is non-NULL throughout.
        assert all(row["orders.m"] is not None for row in withw.data)

    async def test_filter_position_prunes_only(self, exec_backend):
        base = await _val(exec_backend, "amount:sum")
        resp = await exec_backend.execute(orders_q(
            dimensions=["status"], measures=[ModelMeasure(formula="amount:sum", name="m")],
            filters=["sum(amount - customers.discount) > 45"]))
        kept = _cells(resp, "status", "m")
        for cell, v in kept.items():
            assert v == pytest.approx(base[cell])  # surviving values unchanged

    async def test_order_position_sorts_by_the_expression(self, exec_backend):
        # host-discount sums are new=50 > ok=39, so desc order is [new, ok].
        resp = await exec_backend.execute(orders_q(
            dimensions=["status"], measures=[ModelMeasure(formula="amount:sum", name="m")],
            order=[{"column": "sum(amount - customers.discount)", "direction": "desc"}]))
        assert [row["orders.status"] for row in resp.data] == ["new", "ok"]

    async def test_computed_dimension_position_bands(self, exec_backend):
        # Both statuses' host-discount sums are positive (39, 50) → band 'hi'.
        resp = await exec_backend.execute(orders_q(
            dimensions=[{"expression": "CASE WHEN sum(amount - customers.discount, "
                         "partition_by=status) > 0 THEN 'hi' ELSE 'lo' END", "name": "band"},
                        "status"],
            measures=[ModelMeasure(formula="amount:sum", name="a")]))
        assert {row["orders.band"] for row in resp.data} == {"hi"}

    async def test_custom_aggregation_resolves_on_the_anchor(self, exec_backend):
        resp = await exec_backend.execute(orders_q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="wsum(customers.spend - customers.regions.pop)", name="m")]))
        got = {k[0]: v["orders.m"] for k, v in rows_by(resp, "orders.customers.tier").items()}
        for tier, expected in _wsum_anchor_by_tier().items():
            if expected is None:
                assert got[tier] is None
            else:
                assert got[tier] == pytest.approx(expected)

    async def test_custom_aggregation_unknown_off_the_anchor(self):
        # wsum is defined on customers, not orders — the orders-anchored spelling is unknown.
        query = orders_q(measures=[ModelMeasure(formula="wsum(amount - cost)", name="m")])
        with pytest.raises((ValueError,), match="(?i)unknown aggregation"):
            await gen(query)

    async def test_target_homed_scope_closed_no_placeholder_leak(self):
        # The cross-model producer closes its scope and leaks no internal placeholder.
        sql = await gen(orders_q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="sum(customers.spend - customers.regions.pop)", name="m")]))
        assert_scope_closed(sql, dialect="duckdb")
        tree = sqlglot.parse_one(sql, read="duckdb")
        for group in tree.find_all(exp.Group):
            assert "__regroup__" not in group.sql(), sql
            assert not re.search(r"__agg\d+__", group.sql()), sql

    async def test_stage_scope_expression(self, exec_backend):
        # Stage 2 aggregates an expression over stage 1's output column.
        stage1 = orders_q(name="stage1", dimensions=["status"],
                          measures=[ModelMeasure(formula="sum(amount - cost)",
                                                 name="amount_cost_sum")])
        root = orders_q(source_model="stage1",
                        measures=[ModelMeasure(formula="sum(amount_cost_sum - 1)",
                                               name="total")])
        resp = await exec_backend.execute([stage1, root])
        col = next(c for c in resp.columns if c.endswith(".total"))
        assert float(resp.data[0][col]) == pytest.approx(101.0)  # (51-1)+(52-1)

    async def test_dotted_leaf_derived_key(self, exec_backend):
        # No rename → the dotted leaf's dots collapse into the auto-named key.
        resp = await exec_backend.execute(orders_q(
            dimensions=["status"], measures=["sum(amount - customers.discount)"]))
        assert "orders.amount_customers_discount_sum" in resp.columns


# --------------------------------------------------------------------------- #
# Expression source typing — fail-closed and kept-error scenarios.
# --------------------------------------------------------------------------- #
class TestExpressionSourceTyping:
    async def test_fanning_leaf_fails_closed(self):
        query = orders_q(measures=[
            ModelMeasure(formula="sum(amount - customers.regions.bad_pop)", name="m")])
        with pytest.raises(ValueError, match="(?i)unproven join hop|fanning") as ei:
            await gen(query)
        assert not re.search(r"DEV-\d+", str(ei.value))
        # A source-leaf crossing names the cross-model spelling as the remedy.
        assert "region_events" in str(ei.value)
        assert "aggregate the target column directly" in str(ei.value)

    async def test_unanalysable_leaf_fails_closed(self):
        query = orders_q(measures=[
            ModelMeasure(formula="sum(amount - customers.regions.unparseable)", name="m")])
        models = dev1832_unparseable_models()
        with pytest.raises(ValueError, match="(?i)analy") as ei:
            await gen(query, models=models)
        assert "unparseable" in str(ei.value)

    async def test_first_over_expression_keeps_its_error(self):
        query = orders_q(measures=[
            ModelMeasure(formula="first(amount - customers.discount, ordered_at)", name="m")])
        with pytest.raises(ValueError, match="not supported over an expression") as ei:
            await gen(query)
        assert "cross-model" not in str(ei.value).lower()

    async def test_fully_attached_source_accepted(self, exec_backend):
        # Regression guard (DEV-1847): the expression gate must not reject it.
        resp = await exec_backend.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="avg(sum(amount, partition_by=[city, region]))", name="m")]))
        got = {k[0]: v["sales.m"] for k, v in rows_by(resp, "sales.region").items()
               if v["sales.m"] is not None}
        assert got == pytest.approx(AVG_CITY_TOTAL_BY_REGION)

    async def test_mixed_row_and_attached_source_accepted(self, exec_backend):
        # Regression guard (DEV-1859).
        resp = await exec_backend.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="sum(quantity * avg(unit_price, partition_by=product))", name="m")]))
        got = {k[0]: v["sales.m"] for k, v in rows_by(resp, "sales.region").items()
               if v["sales.m"] is not None}
        assert got == pytest.approx(MIXED_SUM_BY_REGION)

    _ATTACHED_PARAM = ("customers.spend:weighted_avg("
                       "weight=sum(amount, partition_by=customers.regions.name))")

    async def test_attached_parameter_accepted_under_associate(self, exec_backend):
        resp = await exec_backend.execute(orders_q(
            to_many_handling="associate", dimensions=["customers.tier"],
            measures=[ModelMeasure(formula=self._ATTACHED_PARAM, name="m")]))
        vals = {k[0]: v["orders.m"] for k, v in rows_by(resp, "orders.customers.tier").items()}
        # Every REAL tier cell carries a weighted-average value; the orphan-order
        # NULL-tier cell is a normal population group and carries NULL, as in the
        # sibling cross-model-by-tier tests (accepted + compiled, per the spec).
        assert {t: v for t, v in vals.items() if t is not None}
        assert all(v is not None for t, v in vals.items() if t is not None)

    async def test_attached_parameter_rejected_under_broadcast(self):
        query = orders_q(measures=[ModelMeasure(formula=self._ATTACHED_PARAM, name="m")])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        msg = str(ei.value)
        assert "customers" in msg
        assert "amount" in msg
        assert "associate" in msg.lower()
        assert not re.search(r"DEV-\d+", msg)


# --------------------------------------------------------------------------- #
# queries/cross-model-aggregates.
# --------------------------------------------------------------------------- #
class TestCrossModelCardinality:
    async def test_joined_sum_not_multiplied(self, exec_backend):
        # Regression guard: each customer's spend counted once regardless of order count.
        alone = await exec_backend.execute(orders_q(
            dimensions=["customers.tier"], measures=[ModelMeasure(formula="customers.spend:sum", name="s")]))
        withm = await exec_backend.execute(orders_q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="s"),
                      ModelMeasure(formula="amount:sum", name="a")]))
        a = {k[0]: v["orders.s"] for k, v in rows_by(alone, "orders.customers.tier").items()}
        b = {k[0]: v["orders.s"] for k, v in rows_by(withm, "orders.customers.tier").items()}
        assert a == pytest.approx(b)

    async def test_expression_homed_at_joined_model_not_multiplied(self, exec_backend):
        # Homed at customers, each customer counted once regardless of order count —
        # the per-customer oracle, not the colon twin (whose pop:sum homes at regions).
        resp = await exec_backend.execute(orders_q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="sum(customers.spend - customers.regions.pop)", name="m")]))
        got = _tier_cells(resp, "m")
        for tier, expected in _spend_minus_pop_by_tier().items():
            if expected is None:
                assert got[tier] is None, tier
            else:
                assert got[tier] == pytest.approx(expected), tier

    async def test_adding_cross_model_measure_is_cardinality_neutral(self, exec_backend):
        base = await exec_backend.execute(orders_q(
            dimensions=["status"], measures=[ModelMeasure(formula="amount:sum", name="a")]))
        withm = await exec_backend.execute(orders_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum", name="a"),
                      ModelMeasure(formula="sum(amount - customers.discount)", name="m")]))
        a = {k[0]: v["orders.a"] for k, v in rows_by(base, "orders.status").items()}
        b = {k[0]: v["orders.a"] for k, v in rows_by(withm, "orders.status").items()}
        assert a == pytest.approx(b)
