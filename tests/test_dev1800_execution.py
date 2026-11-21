"""Execution — change/change_pct (and any transform) over a cross-dataset
inner, combined into a composite, executes correctly in metric, filter and
order positions.

Two datasets:

* the ``_dev1750_fixtures`` dataset (``customers.spend`` broadcasts across ``ordered_at``,
  ``wscaled_sum(amount)`` crosses a fragment) — for the failing composite class,
  asserted by *invariance*: the composite equals the same expression built from
  its separately-selected operands (which already render today), and selecting an
  operand on its own changes nothing (semantics Law 7, metric closure);
* the **attributable** dataset (``_dev1800_fixtures``: the cross-dataset inner
  varies along the customer-signup axis) — for hand-computed absolute oracles.

Metric-position composites raise ``RenderContextMissingFacilityError`` before
this change and execute after; the named single-transform shapes and the
filter/order positions already render and are pinned here against regression.
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryDimension, QueryMetric

from tests._dev1750_fixtures import (
    SlayerQuery,
    make_exec_engine as make_dev1750_engine,
    month_key,
    month_td,
    rows_by,
)
from tests._dev1800_fixtures import (
    AMOUNT_BY_SIGNUP,
    CHANGE_AMOUNT_BY_SIGNUP,
    CHANGE_PCT_AMOUNT_BY_SIGNUP,
    CHANGE_PCT_RATIO_BY_SIGNUP,
    CHANGE_SPEND_BY_SIGNUP,
    RATIO_BY_SIGNUP,
    SPEND_BY_SIGNUP,
    TimeDimension,
    TimeGranularity,
    by_signup,
    make_exec_engine as make_attr_engine,
    signup_td,
)

CM = "sum(customers.spend)"


@pytest.fixture(params=["sqlite", "duckdb"])
async def dev1750_engine(request):
    async for engine in make_dev1750_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def attr_engine(request):
    async for engine in make_attr_engine(request):
        yield engine


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
def _by_month(resp) -> dict:
    return {month_key(k[0]): r for k, r in rows_by(resp, "orders.ordered_at").items()}


async def _orders(engine, **kw):
    kw.setdefault("population", "orders")
    kw.setdefault("time_dimensions", month_td())
    return await engine.execute(SlayerQuery(**kw))


def _approx_or_none(cell, expected) -> None:
    if expected is None:
        assert cell is None
    else:
        assert float(cell) == pytest.approx(expected)


# --------------------------------------------------------------------------- #
# The failing metric-position class — asserted by invariance on the dev1750 dataset.
# --------------------------------------------------------------------------- #
class TestTransformPlusHiddenAggregate:
    """``change(cm) + sum(amount)`` and its variants: the composite equals the sum
    of its separately-selected operands, evaluated row by row. Raises
    ``RenderContextMissingFacilityError`` before the change."""

    async def _assert_sum_invariant(self, engine, *, transform, operand) -> None:
        comps = _by_month(await _orders(engine, metrics=[
            QueryMetric(metric=transform, alias="t"),
            QueryMetric(metric=operand, alias="o"),
        ]))
        got = _by_month(await _orders(engine, metrics=[
            QueryMetric(metric=f"{transform} + {operand}", alias="c"),
        ]))
        assert set(got) == set(comps)
        for m, cr in comps.items():
            t, o = cr["t"], cr["o"]
            expected = None if t is None else t + o
            _approx_or_none(got[m]["c"], expected)

    async def test_change_plus_local_sum(self, dev1750_engine) -> None:
        await self._assert_sum_invariant(dev1750_engine, transform=f"change({CM})", operand="sum(orders.amount)")

    async def test_change_plus_star_count(self, dev1750_engine) -> None:
        await self._assert_sum_invariant(dev1750_engine, transform=f"change({CM})", operand="count(orders.*)")

    async def test_change_plus_amount_max(self, dev1750_engine) -> None:
        await self._assert_sum_invariant(dev1750_engine, transform=f"change({CM})", operand="max(orders.amount)")

    async def test_time_shift_plus_sum(self, dev1750_engine) -> None:
        await self._assert_sum_invariant(dev1750_engine, transform=f"time_shift({CM}, -1)", operand="sum(orders.amount)")

    async def test_cumsum_plus_sum(self, dev1750_engine) -> None:
        await self._assert_sum_invariant(dev1750_engine, transform=f"cumsum({CM})", operand="sum(orders.amount)")

    async def test_change_of_wscaled_plus_sum(self, dev1750_engine) -> None:
        """The inner crosses a relationship fragment (``wscaled_sum(amount)``)."""
        await self._assert_sum_invariant(dev1750_engine, transform="change(wscaled_sum(orders.amount))", operand="sum(orders.amount)")

    async def test_transform_plus_transform(self, dev1750_engine) -> None:
        """Both operands are transforms (renders today; pinned by invariance)."""
        await self._assert_sum_invariant(dev1750_engine, transform="change(sum(orders.amount))", operand=f"change({CM})")

    async def test_transform_plus_literal(self, dev1750_engine) -> None:
        comps = _by_month(await _orders(dev1750_engine, metrics=[
            QueryMetric(metric=f"change({CM})", alias="t"),
        ]))
        got = _by_month(await _orders(dev1750_engine, metrics=[
            QueryMetric(metric=f"change({CM}) + 100", alias="c"),
        ]))
        assert set(got) == set(comps)
        for m, cr in comps.items():
            t = cr["t"]
            _approx_or_none(got[m]["c"], None if t is None else t + 100)

    async def test_composite_with_a_dimension(self, dev1750_engine) -> None:
        """The same failing class with a ``status`` dimension present (incl. the
        NULL-status group)."""
        def by_status_month(resp):
            return {
                (r["orders.status"], month_key(r["orders.ordered_at"])): r
                for r in resp.data
            }

        comps = by_status_month(await _orders(dev1750_engine, dimensions=["orders.status"], metrics=[
            QueryMetric(metric=f"change({CM})", alias="t"),
            QueryMetric(metric="sum(orders.amount)", alias="o"),
        ]))
        got = by_status_month(await _orders(dev1750_engine, dimensions=["orders.status"], metrics=[
            QueryMetric(metric=f"change({CM}) + sum(orders.amount)", alias="c"),
        ]))
        assert set(got) == set(comps)
        for k, cr in comps.items():
            t, o = cr["t"], cr["o"]
            _approx_or_none(got[k]["c"], None if t is None else t + o)


class TestConditionalOverTransform:
    """``iif(change(cm) > 0, cm, sum(amount))`` on the attributable dataset, where
    the change is positive in one period, negative in another and NULL in the
    first — so all three branches are exercised."""

    async def test_iif_picks_by_sign_of_change(self, attr_engine) -> None:
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric=f"iif(change({CM}) > 0, {CM}, sum(orders.amount))", alias="v")],
        )))
        # change=[None,50,-120]; picks cm where change>0 else amount:sum.
        expected = {
            m: (AMOUNT_BY_SIGNUP[m] if (CHANGE_SPEND_BY_SIGNUP[m] is None or CHANGE_SPEND_BY_SIGNUP[m] <= 0)
                else SPEND_BY_SIGNUP[m])
            for m in AMOUNT_BY_SIGNUP
        }
        assert set(got) == set(expected)
        for m, want in expected.items():
            _approx_or_none(got[m]["v"], want)


# --------------------------------------------------------------------------- #
# Absolute oracles on the attributable dataset (inner varies along the axis).
# --------------------------------------------------------------------------- #
class TestAttributableComposite:
    """``change(cm) + sum(amount)`` with hand-computed per-period values; the
    inner (spend re-aggregated per signup cohort) genuinely varies month over
    month, so the change contributes a non-zero term."""

    async def test_change_spend_plus_amount(self, attr_engine) -> None:
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric=f"change({CM}) + sum(orders.amount)", alias="c")],
        )))
        expected = {
            m: (None if CHANGE_SPEND_BY_SIGNUP[m] is None
                else CHANGE_SPEND_BY_SIGNUP[m] + AMOUNT_BY_SIGNUP[m])
            for m in AMOUNT_BY_SIGNUP
        }  # [None, 150, -113]
        assert set(got) == set(expected)
        for m, want in expected.items():
            _approx_or_none(got[m]["c"], want)

    async def test_selecting_operand_on_its_own_changes_nothing(self, attr_engine) -> None:
        """Scenario: also selecting ``sum(amount)`` leaves the composite, the row
        count and every other column identical (semantics Law 7)."""
        alone = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric=f"change({CM}) + sum(orders.amount)", alias="c")],
        )))
        both = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric=f"change({CM}) + sum(orders.amount)", alias="c"),
                      QueryMetric(metric="sum(orders.amount)", alias="a")],
        )))
        assert set(both) == set(alone)
        for m in alone:
            _approx_or_none(both[m]["c"], alone[m]["c"])
            _approx_or_none(both[m]["a"], AMOUNT_BY_SIGNUP[m])


class TestInnerVaryingAlongTimeAxis:
    """Scenario: the cross-dataset inner is attributable to the query's time axis;
    the change and change_pct percentages are correct per period, and adding the
    metric changes no row and no other column."""

    async def test_change_of_orders_amount_by_customer_signup(self, attr_engine) -> None:
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="customers", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="a"),
                      QueryMetric(metric="change(sum(orders.amount))", alias="ch"),
                      QueryMetric(metric="change_pct(sum(orders.amount))", alias="cp")],
        )))
        for m in AMOUNT_BY_SIGNUP:
            _approx_or_none(got[m]["a"], AMOUNT_BY_SIGNUP[m])
            _approx_or_none(got[m]["ch"], CHANGE_AMOUNT_BY_SIGNUP[m])
            _approx_or_none(got[m]["cp"], CHANGE_PCT_AMOUNT_BY_SIGNUP[m])

    async def test_change_of_customer_spend_rooted_at_orders(self, attr_engine) -> None:
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric=CM, alias="s"),
                      QueryMetric(metric=f"change({CM})", alias="ch")],
        )))
        for m in SPEND_BY_SIGNUP:
            _approx_or_none(got[m]["s"], SPEND_BY_SIGNUP[m])
            _approx_or_none(got[m]["ch"], CHANGE_SPEND_BY_SIGNUP[m])

    async def test_mixed_grain_change_pct_of_ratio(self, attr_engine) -> None:
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric=f"{CM} / sum(orders.amount)", alias="r"),
                      QueryMetric(metric=f"change_pct({CM} / sum(orders.amount))", alias="cp")],
        )))
        for m in RATIO_BY_SIGNUP:
            _approx_or_none(got[m]["r"], RATIO_BY_SIGNUP[m])
            _approx_or_none(got[m]["cp"], CHANGE_PCT_RATIO_BY_SIGNUP[m])


# --------------------------------------------------------------------------- #
# The issue's named single-transform shapes execute directly.
# --------------------------------------------------------------------------- #
class TestNamedShapes:
    async def test_change_of_wscaled(self, dev1750_engine) -> None:
        got = _by_month(await _orders(dev1750_engine, metrics=[
            QueryMetric(metric="change(wscaled_sum(orders.amount))", alias="d"),
        ]))
        for m, want in {"2024-01": None, "2024-02": 13.0, "2024-03": -2.0}.items():
            _approx_or_none(got[m]["d"], want)

    async def test_change_pct_of_wscaled(self, dev1750_engine) -> None:
        got = _by_month(await _orders(dev1750_engine, metrics=[
            QueryMetric(metric="change_pct(wscaled_sum(orders.amount))", alias="d"),
        ]))
        for m, want in {"2024-01": None, "2024-02": 13.0 / 35.0, "2024-03": -2.0 / 48.0}.items():
            _approx_or_none(got[m]["d"], want)

    async def test_change_of_broadcast_spend_is_zero_after_first_period(self, dev1750_engine) -> None:
        """The broadcast cross-dataset inner (spend constant across ordered_at)
        yields a zero delta and zero percentage after the first period."""
        for formula in (f"change({CM})", f"change_pct({CM})"):
            got = _by_month(await _orders(dev1750_engine, metrics=[
                QueryMetric(metric=formula, alias="d"),
            ], to_many_handling="broadcast"))
            _approx_or_none(got["2024-01"]["d"], None)
            _approx_or_none(got["2024-02"]["d"], 0.0)
            _approx_or_none(got["2024-03"]["d"], 0.0)


# --------------------------------------------------------------------------- #
# Filter and order positions of the composite (regression pins: the post-wrapper
# path already renders these; D5 must keep the executed values correct).
# --------------------------------------------------------------------------- #
class TestCompositeInFilterAndOrder:
    async def test_filter_only_masks_by_composite(self, attr_engine) -> None:
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="a")],
            filter=[f"change({CM}) + sum(orders.amount) > 0"],
        )))
        # composite = [None, 150, -113] → only 2024-02 survives.
        assert set(got) == {"2024-02"}
        _approx_or_none(got["2024-02"]["a"], AMOUNT_BY_SIGNUP["2024-02"])

    async def test_order_only_sorts_by_composite(self, attr_engine) -> None:
        resp = await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="a")],
            order=[{"field": f"change({CM}) + sum(orders.amount)", "direction": "desc"}],  # pyright: ignore[reportArgumentType] — expression order targets enter via the shorthand coercion
        ))
        signup_col = next(k for k in resp.data[0] if k.endswith("signup_at"))
        months = [month_key(r[signup_col]) for r in resp.data]
        # composite = [Feb 150, Mar -113, Jan None] → nulls last.
        assert months[:2] == ["2024-02", "2024-03"]
        assert set(months) == {"2024-01", "2024-02", "2024-03"}


class TestTermAloneExecutionParity:
    """Filtering on ``last(change(x))`` keeps the same rows with and without the
    composite projected (staging is a function of the term alone). The projected
    variant is the dev1859 stall shape and raises before the change."""

    _FILTER = "last(change(sum(orders.amount))) < 0"

    @staticmethod
    def _month_orders_td():
        return [TimeDimension(field="orders.ordered_at",
                              granularity=TimeGranularity.MONTH)]

    async def test_surviving_rows_identical_with_and_without_projection(self, attr_engine) -> None:
        def keyed(resp) -> dict:
            return {(r["orders.customer_id"], month_key(r["orders.ordered_at"])): r
                    for r in resp.data}

        without = keyed(await attr_engine.execute(SlayerQuery(
            population="orders", dimensions=[QueryDimension(field="orders.customer_id")],
            time_dimensions=self._month_orders_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="a")],
            filter=[self._FILTER])))
        with_proj = keyed(await attr_engine.execute(SlayerQuery(
            population="orders", dimensions=[QueryDimension(field="orders.customer_id")],
            time_dimensions=self._month_orders_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="a"),
                      QueryMetric(metric="change(sum(orders.amount))", alias="ch")],
            filter=[self._FILTER])))
        # per customer over order months: c1 last change −5, c2 −20 (both kept
        # whole); c3 has one month, change NULL → dropped.
        expected_amounts = {(1, "2024-01"): 20.0, (1, "2024-02"): 15.0,
                            (2, "2024-02"): 60.0, (2, "2024-03"): 40.0}
        assert set(without) == set(expected_amounts)
        assert set(with_proj) == set(without)
        for k, want in expected_amounts.items():
            _approx_or_none(without[k]["a"], want)
            _approx_or_none(with_proj[k]["a"], want)
        expected_change = {(1, "2024-01"): None, (1, "2024-02"): -5.0,
                           (2, "2024-02"): None, (2, "2024-03"): -20.0}
        for k, want in expected_change.items():
            _approx_or_none(with_proj[k]["ch"], want)


class TestMaskPlacementExecuted:
    """A row-typed mask filters rows before aggregation (WHERE); a metric-typed
    mask filters groups after aggregation (HAVING) — distinct results confirm the
    placement is by mask typing, not text (executed on SQLite + DuckDB)."""

    async def test_row_mask_filters_before_aggregation(self, attr_engine) -> None:
        # amount > 10 drops order o5 (7.0, the Mar cohort's only order).
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="a")],
            filter=["orders.amount > 10"],
        )))
        assert set(got) == {"2024-01", "2024-02"}
        _approx_or_none(got["2024-01"]["a"], 35.0)
        _approx_or_none(got["2024-02"]["a"], 100.0)

    async def test_measure_mask_filters_after_aggregation(self, attr_engine) -> None:
        got = by_signup(await attr_engine.execute(SlayerQuery(
            population="orders", time_dimensions=signup_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="a")],
            filter=["sum(orders.amount) > 50"],
        )))
        assert set(got) == {"2024-02"}
        _approx_or_none(got["2024-02"]["a"], 100.0)
