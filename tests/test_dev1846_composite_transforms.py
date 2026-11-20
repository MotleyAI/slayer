"""Composite-input ``time_shift`` / ``consecutive_periods``, executed
on SQLite + DuckDB against ``tests/_dev1846_fixtures.py`` (every expectation
hand-computed there). Covers the lifted shapes, the ``ValueError`` typing
contract for still-unsupported inputs, the uniform fail-closed gate, and the
planner ``_iter_slot_deps`` recursion (bottom), covered structurally.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from slayer.core.keys import (
    ArithmeticKey,
    FieldKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
)
from slayer.engine.compile.projection import _iter_slot_deps

from slayer.core.query import QueryMetric

from tests._dev1846_fixtures import (
    SlayerQuery,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    """SQLite + DuckDB engines over the hand-computed dataset. Defined here so
    the fixture name is not a cross-module import ruff reads as shadowing the
    parameter (F811)."""
    async for engine in make_exec_engine(request):
        yield engine


def _q(*, metrics, dimensions=None) -> SlayerQuery:
    kw = dict(population="sales", time_dimensions=month_td(), metrics=metrics)
    if dimensions is not None:
        kw["dimensions"] = dimensions
    return SlayerQuery.model_validate(kw)


def _by_month(resp) -> dict:
    return {
        month_key(k[0]): r
        for k, r in rows_by(resp, "sales.ordered_at").items()
    }


def _by_store_month(resp) -> dict:
    return {
        (r["sales.store"], month_key(r["sales.ordered_at"])): r
        for r in resp.data
    }


def _f(v):
    return None if v is None else float(v)


async def _error(*, metrics, dimensions=None):
    """The (type-name, message) of the raise a query produces, or fail."""
    try:
        await gen(_q(metrics=metrics, dimensions=dimensions))
    except Exception as exc:  # noqa: BLE001 — the exception itself is the contract
        return type(exc).__name__, str(exc)
    raise AssertionError("expected the query to fail closed, but it generated SQL")


class TestFixtureSmoke:
    async def test_trivial_query_executes(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="sum(sales.revenue)", alias="r"),
            QueryMetric(metric="count(sales.*)", alias="n"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["r"]) == pytest.approx(60.0)
        assert int(by["2024-01"]["n"]) == 3
        assert _f(by["2024-02"]["r"]) == pytest.approx(100.0)


class TestTimeShiftCompositeExecution:
    async def test_ratio_shift_by_store(self, exec_engine) -> None:
        """``time_shift(sum(revenue) / sum(qty), -1)`` carries each store's prior
        month ratio; the earliest month is NULL, partitions never cross."""
        resp = await exec_engine.execute(_q(
            dimensions=["sales.store"],
            metrics=[
                QueryMetric(metric="sum(sales.revenue) / sum(sales.qty)", alias="cur"),
                QueryMetric(metric="time_shift(sum(sales.revenue) / sum(sales.qty), -1)",
                             alias="prev"),
            ],
        ))
        by = _by_store_month(resp)
        assert _f(by[("A", "2024-01")]["cur"]) == pytest.approx(6.0)
        assert _f(by[("A", "2024-02")]["cur"]) == pytest.approx(8.0)
        assert _f(by[("B", "2024-02")]["cur"]) == pytest.approx(10.0)
        assert by[("A", "2024-01")]["prev"] is None
        assert by[("B", "2024-01")]["prev"] is None
        assert _f(by[("A", "2024-02")]["prev"]) == pytest.approx(6.0)
        assert _f(by[("A", "2024-03")]["prev"]) == pytest.approx(8.0)
        assert _f(by[("B", "2024-02")]["prev"]) == pytest.approx(6.0)
        assert _f(by[("B", "2024-03")]["prev"]) == pytest.approx(10.0)

    async def test_change_pct_ratio_resets_per_store(self, exec_engine) -> None:
        """``change_pct(sum(revenue) / count(*))`` desugars onto the composite
        time_shift; each store's first month is NULL, later months use that
        store's own MoM growth."""
        resp = await exec_engine.execute(_q(
            dimensions=["sales.store"],
            metrics=[
                QueryMetric(metric="change_pct(sum(sales.revenue) / count(sales.*))", alias="pct"),
            ],
        ))
        by = _by_store_month(resp)
        assert by[("A", "2024-01")]["pct"] is None
        assert by[("B", "2024-01")]["pct"] is None
        assert _f(by[("A", "2024-02")]["pct"]) == pytest.approx((40 - 15) / 15)
        assert _f(by[("A", "2024-03")]["pct"]) == pytest.approx((50 - 40) / 40)
        assert _f(by[("B", "2024-02")]["pct"]) == pytest.approx((60 - 30) / 30)
        assert _f(by[("B", "2024-03")]["pct"]) == pytest.approx((10 - 60) / 60)

    async def test_change_ratio_by_store(self, exec_engine) -> None:
        """``change(sum(revenue) / sum(qty))`` = ratio − prior ratio, per store,
        NULL in each store's first month (A: 6→8→10, B: 6→10→5)."""
        resp = await exec_engine.execute(_q(
            dimensions=["sales.store"],
            metrics=[
                QueryMetric(metric="change(sum(sales.revenue) / sum(sales.qty))", alias="delta"),
            ],
        ))
        by = _by_store_month(resp)
        assert by[("A", "2024-01")]["delta"] is None
        assert by[("B", "2024-01")]["delta"] is None
        assert _f(by[("A", "2024-02")]["delta"]) == pytest.approx(2.0)
        assert _f(by[("A", "2024-03")]["delta"]) == pytest.approx(2.0)
        assert _f(by[("B", "2024-02")]["delta"]) == pytest.approx(4.0)
        assert _f(by[("B", "2024-03")]["delta"]) == pytest.approx(-5.0)

    async def test_coalesce_missing_bucket_is_null(self, exec_engine) -> None:
        """A NULL-absorbing wrapper does NOT turn a missing shifted bucket into
        0: the earliest month's shifted value is NULL."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="coalesce(sum(sales.revenue), 0)", alias="cur"),
            QueryMetric(metric="time_shift(coalesce(sum(sales.revenue), 0), -1)",
                         alias="prev"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["cur"]) == pytest.approx(60.0)
        assert by["2024-01"]["prev"] is None
        assert _f(by["2024-02"]["prev"]) == pytest.approx(60.0)
        assert _f(by["2024-03"]["prev"]) == pytest.approx(100.0)

    async def test_two_differently_parameterized_leaves(self, exec_engine) -> None:
        """A relationship-crossing fragment leaf (``wrevenue_sum``) plus a field-filtered
        leaf (``hi_rev``): each re-aggregates with its own parameters/filter in
        the shifted period."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="wrevenue_sum(sales.revenue) + sum(sales.hi_rev)", alias="cur"),
            QueryMetric(metric="time_shift(wrevenue_sum(sales.revenue) + sum(sales.hi_rev), -1)",
                         alias="prev"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["cur"]) == pytest.approx(200.0)  # 150 + 50
        assert _f(by["2024-02"]["cur"]) == pytest.approx(360.0)  # 260 + 100
        assert _f(by["2024-03"]["cur"]) == pytest.approx(180.0)  # 130 + 50
        assert by["2024-01"]["prev"] is None
        assert _f(by["2024-02"]["prev"]) == pytest.approx(200.0)
        assert _f(by["2024-03"]["prev"]) == pytest.approx(360.0)

    async def test_crossing_param_registers_join_per_leaf(self, exec_engine) -> None:
        """A composite whose only aggregate leaf crosses the ``regions`` relationship:
        the shifted CTE must bind that relationship to compute ``SUM(revenue * factor)``
        in the prior period."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="wrevenue_sum(sales.revenue) * 2", alias="cur"),
            QueryMetric(metric="time_shift(wrevenue_sum(sales.revenue) * 2, -1)",
                         alias="prev"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["cur"]) == pytest.approx(300.0)  # 150 * 2
        assert _f(by["2024-02"]["cur"]) == pytest.approx(520.0)  # 260 * 2
        assert by["2024-01"]["prev"] is None
        assert _f(by["2024-02"]["prev"]) == pytest.approx(300.0)
        assert _f(by["2024-03"]["prev"]) == pytest.approx(520.0)


class TestConsecutivePeriodsExecution:
    async def test_numeric_delta_truthiness(self, exec_engine) -> None:
        """A bare numeric composite: streak counts consecutive months where the
        delta is non-NULL and non-zero (40, 62, 30 → 1, 2, 3)."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="consecutive_periods(sum(sales.revenue) - sum(sales.cost))",
                         alias="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["streak"]) == 1
        assert int(by["2024-02"]["streak"]) == 2
        assert int(by["2024-03"]["streak"]) == 3

    async def test_growth_streak_over_nested_transform(self, exec_engine) -> None:
        """Comparison predicate over a nested transform (already renders today —
        regression anchor): change > 0 holds only in Feb → 0, 1, 0."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="consecutive_periods(change(sum(sales.revenue)) > 0)",
                         alias="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["streak"]) == 0
        assert int(by["2024-02"]["streak"]) == 1
        assert int(by["2024-03"]["streak"]) == 0

    async def test_bare_nested_transform(self, exec_engine) -> None:
        """A bare nested transform value: cumsum is non-zero every month → 1,2,3."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="consecutive_periods(cumsum(sum(sales.revenue)))",
                         alias="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["streak"]) == 1
        assert int(by["2024-02"]["streak"]) == 2
        assert int(by["2024-03"]["streak"]) == 3

    async def test_scalar_call_in_comparison(self, exec_engine) -> None:
        """Scalar call inside a comparison (already renders today — regression
        anchor): round(revenue:sum) >= 10 holds every month → 1, 2, 3."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="consecutive_periods(round(sum(sales.revenue)) >= 10)",
                         alias="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["streak"]) == 1
        assert int(by["2024-02"]["streak"]) == 2
        assert int(by["2024-03"]["streak"]) == 3

    async def test_or_connective(self, exec_engine) -> None:
        """``sum(revenue) > 90 or sum(cost) > 40`` holds only in Feb → 0, 1, 0."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(
                metric="consecutive_periods(sum(sales.revenue) > 90 or sum(sales.cost) > 40)",
                alias="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["streak"]) == 0
        assert int(by["2024-02"]["streak"]) == 1
        assert int(by["2024-03"]["streak"]) == 0

    async def test_not_connective(self, exec_engine) -> None:
        """``not (sum(revenue) > 90)`` fails only in Feb → 1, 0, 1."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(metric="consecutive_periods(not (sum(sales.revenue) > 90))",
                         alias="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["streak"]) == 1
        assert int(by["2024-02"]["streak"]) == 0
        assert int(by["2024-03"]["streak"]) == 1

    async def test_in_predicate(self, exec_engine) -> None:
        """A top-level IN over a grouped dimension: both stores are in the set
        every month → each streak 1, 2, 3."""
        resp = await exec_engine.execute(_q(
            dimensions=["sales.store"],
            metrics=[QueryMetric(
                metric="consecutive_periods(sales.store in ('A', 'B'))", alias="streak")],
        ))
        by = _by_store_month(resp)
        for store in ("A", "B"):
            assert int(by[(store, "2024-01")]["streak"]) == 1
            assert int(by[(store, "2024-02")]["streak"]) == 2
            assert int(by[(store, "2024-03")]["streak"]) == 3

    async def test_negated_in_predicate(self, exec_engine) -> None:
        """A negated IN: store A is never in {B, C} (true → 1,2,3), store B
        always is (false → 0,0,0)."""
        resp = await exec_engine.execute(_q(
            dimensions=["sales.store"],
            metrics=[QueryMetric(
                metric="consecutive_periods(sales.store not in ('B', 'C'))",
                alias="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-03")]["streak"]) == 3
        assert int(by[("B", "2024-01")]["streak"]) == 0
        assert int(by[("B", "2024-03")]["streak"]) == 0

    async def test_null_predicate_group_treated_as_false(self, exec_engine) -> None:
        """A group whose ``or`` predicate evaluates to NULL breaks the streak
        (NULL treated as false): store B's March ``sum(hi_rev)`` is NULL (no row
        clears the >15 filter), so ``sum(hi_rev) > 0 or sum(cost) > 1000`` is NULL
        there → B streak 1, 2, 0; store A never NULL → 1, 2, 3."""
        resp = await exec_engine.execute(_q(
            dimensions=["sales.store"],
            metrics=[QueryMetric(
                metric="consecutive_periods(sum(sales.hi_rev) > 0 or sum(sales.cost) > 1000)",
                alias="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-03")]["streak"]) == 3
        assert int(by[("B", "2024-01")]["streak"]) == 1
        assert int(by[("B", "2024-02")]["streak"]) == 2
        assert int(by[("B", "2024-03")]["streak"]) == 0

    async def test_nested_in_under_and(self, exec_engine) -> None:
        """An IN nested under ``and`` (its column must materialise): store A
        matches with revenue>0 (1,2,3); store B never matches (0,0,0)."""
        resp = await exec_engine.execute(_q(
            dimensions=["sales.store"],
            metrics=[QueryMetric(
                metric="consecutive_periods(sales.store in ('A', 'C') and sum(sales.revenue) > 0)",
                alias="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-01")]["streak"]) == 1
        assert int(by[("A", "2024-03")]["streak"]) == 3
        assert int(by[("B", "2024-02")]["streak"]) == 0


class TestPredicateTypingContract:
    async def test_iif_condition_position_accepts_predicate(self, exec_engine) -> None:
        """A boolean-shaped node is legal in ``iif``'s condition seat; the value
        drives truthiness (iif → 1 every month → 1, 2, 3)."""
        resp = await exec_engine.execute(_q(metrics=[
            QueryMetric(
                metric="consecutive_periods(iif(sum(sales.revenue) > 0, 1, 0))",
                alias="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["streak"]) == 1
        assert int(by["2024-03"]["streak"]) == 3

    @pytest.mark.parametrize("formula", [
        "consecutive_periods((sum(sales.revenue) > 0) + (sum(sales.cost) > 0))",
        "consecutive_periods(coalesce(sum(sales.revenue) > 0, 0))",
        "consecutive_periods((sum(sales.revenue) > 0) in (1, 0))",
    ])
    async def test_boolean_in_numeric_position_is_its_integer(self, exec_engine, formula: str) -> None:
        """Every month's revenue and cost are positive, so each value is non-zero and the run never breaks."""
        resp = await exec_engine.execute(_q(metrics=[QueryMetric(metric=formula, alias="streak")]))
        by = _by_month(resp)
        assert [int(by[m]["streak"]) for m in ("2024-01", "2024-02", "2024-03")] == [1, 2, 3]

    async def test_string_family_predicate_rejected(self) -> None:
        name, msg = await _error(metrics=[QueryMetric(
            metric="consecutive_periods(lower(max(sales.sku)))", alias="x")])
        assert name == "ValueError", (name, msg)
        low = msg.lower()
        assert "consecutive_periods" in low, msg
        assert "string" in low, msg
        assert "truthiness" in low or "predicate" in low, msg

    async def test_boolean_in_in_operand_is_its_integer(self) -> None:
        """Never passed through as a bare ``(SUM(revenue) > 0) IN (...)``."""
        sql = await gen(_q(metrics=[QueryMetric(
            metric="consecutive_periods((sum(sales.revenue) > 0) in (1, 0))", alias="x")]), dialect="postgres")
        assert "> 0 IN" not in sql.replace("\n", " ").upper()


class TestUniformFailClosed:
    #: shape id -> the still-unsupported time_shift input: row-level leaves
    #: only (nested transforms, cross-dataset leaves, and
    #: aggregate-typed predicates are lifted into the series regime).
    SHAPES = {
        "mixed_composite": "time_shift(sum(sales.revenue) * sales.weight, -1)",
        "pure_row_composite": "time_shift(sales.weight * sales.qty, -1)",
        "row_predicate_input": "time_shift(sales.store in ('A', 'B'), -1)",
    }
    #: shape id -> substrings the user-facing message must name.
    TOKENS = {
        "mixed_composite": ("time_shift", "row", "source_queries"),
        "pure_row_composite": ("time_shift", "row", "source_queries"),
        "row_predicate_input": ("time_shift", "row", "source_queries"),
    }

    @pytest.mark.parametrize("shape", list(SHAPES))
    async def test_plain_path_names_shape_and_remedy(self, shape) -> None:
        name, msg = await _error(metrics=[QueryMetric(metric=self.SHAPES[shape], alias="prev")])
        assert name == "TransformInputError", (shape, name, msg)
        low = msg.lower()
        for token in self.TOKENS[shape]:
            assert token in low, (shape, token, msg)

    @pytest.mark.parametrize("shape", list(SHAPES))
    async def test_same_error_with_and_without_cross_model_sibling(self, shape) -> None:
        """The presence of a cross-dataset metric elsewhere must not change which
        error the unsupported shape produces (today the plain path and the
        cross-dataset-chain path diverge)."""
        plain = await _error(metrics=[QueryMetric(metric=self.SHAPES[shape], alias="prev")])
        with_sibling = await _error(metrics=[
            QueryMetric(metric="sum(regions.factor)", alias="cm"),
            QueryMetric(metric=self.SHAPES[shape], alias="prev"),
        ])
        assert plain == with_sibling, (shape, plain, with_sibling)


class TestIterSlotDepsRecursion:
    STATUS = FieldKey(path=(), leaf="status")
    QTY = FieldKey(path=(), leaf="qty")
    INK = InKey(field=STATUS, values=(LiteralKey(value="a"), LiteralKey(value="b")))
    BOUND = ArithmeticKey(op="and", operands=(
        ArithmeticKey(op=">=", operands=(QTY, LiteralKey(value=date(2024, 1, 1)))),
        ArithmeticKey(op="<", operands=(QTY, LiteralKey(value=date(2025, 1, 1)))),
    ))
    _ONE = LiteralKey(value=Decimal(1))
    _ZERO = LiteralKey(value=Decimal(0))

    def _deps(self, key):
        return list(_iter_slot_deps(key))

    def test_top_level_in_surfaces_column(self) -> None:
        assert self.STATUS in self._deps(self.INK)

    def test_top_level_bound_surfaces_column(self) -> None:
        assert self.QTY in self._deps(self.BOUND)

    def test_in_nested_under_scalar_call_surfaces_column(self) -> None:
        key = ScalarCallKey(name="iif", args=(self.INK, self._ONE, self._ZERO))
        assert self.STATUS in self._deps(key)

    def test_bound_nested_under_scalar_call_surfaces_column(self) -> None:
        key = ScalarCallKey(name="iif", args=(self.BOUND, self._ONE, self._ZERO))
        assert self.QTY in self._deps(key)

    def test_in_nested_under_arithmetic_surfaces_column(self) -> None:
        key = ArithmeticKey(op="and", operands=(self.INK, self.INK))
        assert self.STATUS in self._deps(key)

    def test_bound_nested_under_arithmetic_surfaces_column(self) -> None:
        key = ArithmeticKey(op="or", operands=(self.BOUND, self.BOUND))
        assert self.QTY in self._deps(key)
