"""DEV-1846 — composite-input ``time_shift`` / ``consecutive_periods``.

Executed ground truth on SQLite AND DuckDB (both issue-required), every
expectation hand-computed from ``tests/_dev1846_fixtures.py``. The suite lands
BEFORE the lift, so:

* the *lift* tests (composite time_shift, non-comparison / boolean / scalar-call
  cp) fail today with the current fail-closed guard and pass once the lift
  renders + re-aggregates them;
* the *typing-contract* tests pin the ``ValueError`` shape the lift must raise
  for still-unsupported inputs;
* the *uniform-gate* tests pin that every render path raises the SAME error
  (today the plain path and the cross-model-sibling path diverge);
* two comparison-predicate cp cases (``change(x) > 0``, ``round(x) >= 10``)
  already render — kept as regression anchors the lift must not disturb.

The planner ``_iter_slot_deps`` recursion is unit-tested at the bottom.

One spec predicate family — a top-level ``BETWEEN`` — is NOT reachable through
the Mode-B DSL (the parser has no ``between`` construct; ``BetweenKey`` is
produced only internally by ``TimeDimension.date_range``), so it has no
end-to-end formula test. Its handling is covered structurally: the boolean-vs-
value classification is exercised by the ``iif`` / ``and`` / ``or`` / ``not`` /
``IN`` cases here, and ``BetweenKey`` column materialisation by
``TestIterSlotDepsRecursion``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import (
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
)
from slayer.engine.planning import _iter_slot_deps

from tests._dev1846_fixtures import (
    ModelMeasure,
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


def _q(*, measures, dimensions=None) -> SlayerQuery:
    kw = dict(source_model="sales", time_dimensions=month_td(), measures=measures)
    if dimensions is not None:
        kw["dimensions"] = dimensions
    return SlayerQuery(**kw)


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


async def _error(measures, dimensions=None):
    """The (type-name, message) of the raise a query produces, or fail."""
    try:
        await gen(_q(measures=measures, dimensions=dimensions))
    except Exception as exc:  # noqa: BLE001 — the exception itself is the contract
        return type(exc).__name__, str(exc)
    raise AssertionError("expected the query to fail closed, but it generated SQL")


# --------------------------------------------------------------------------- #
# Fixture smoke (task 1.1) — the dataset/engine works independently of the lift.
# --------------------------------------------------------------------------- #
class TestFixtureSmoke:
    async def test_trivial_query_executes(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="revenue:sum", name="r"),
            ModelMeasure(formula="*:count", name="n"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["sales.r"]) == pytest.approx(60.0)
        assert int(by["2024-01"]["sales.n"]) == 3
        assert _f(by["2024-02"]["sales.r"]) == pytest.approx(100.0)


# --------------------------------------------------------------------------- #
# time_shift over composite inputs — executed values (task 1.2).
# --------------------------------------------------------------------------- #
class TestTimeShiftCompositeExecution:
    async def test_ratio_shift_by_store(self, exec_engine) -> None:
        """``time_shift(revenue:sum / qty:sum, -1)`` carries each store's prior
        month ratio; the earliest month is NULL, partitions never cross."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[
                ModelMeasure(formula="revenue:sum / qty:sum", name="cur"),
                ModelMeasure(formula="time_shift(revenue:sum / qty:sum, -1)",
                             name="prev"),
            ],
        ))
        by = _by_store_month(resp)
        assert _f(by[("A", "2024-01")]["sales.cur"]) == pytest.approx(6.0)
        assert _f(by[("A", "2024-02")]["sales.cur"]) == pytest.approx(8.0)
        assert _f(by[("B", "2024-02")]["sales.cur"]) == pytest.approx(10.0)
        assert by[("A", "2024-01")]["sales.prev"] is None
        assert by[("B", "2024-01")]["sales.prev"] is None
        assert _f(by[("A", "2024-02")]["sales.prev"]) == pytest.approx(6.0)
        assert _f(by[("A", "2024-03")]["sales.prev"]) == pytest.approx(8.0)
        assert _f(by[("B", "2024-02")]["sales.prev"]) == pytest.approx(6.0)
        assert _f(by[("B", "2024-03")]["sales.prev"]) == pytest.approx(10.0)

    async def test_change_pct_ratio_resets_per_store(self, exec_engine) -> None:
        """``change_pct(revenue:sum / *:count)`` desugars onto the composite
        time_shift; each store's first month is NULL, later months use that
        store's own MoM growth."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[
                ModelMeasure(formula="change_pct(revenue:sum / *:count)", name="pct"),
            ],
        ))
        by = _by_store_month(resp)
        assert by[("A", "2024-01")]["sales.pct"] is None
        assert by[("B", "2024-01")]["sales.pct"] is None
        assert _f(by[("A", "2024-02")]["sales.pct"]) == pytest.approx((40 - 15) / 15)
        assert _f(by[("A", "2024-03")]["sales.pct"]) == pytest.approx((50 - 40) / 40)
        assert _f(by[("B", "2024-02")]["sales.pct"]) == pytest.approx((60 - 30) / 30)
        assert _f(by[("B", "2024-03")]["sales.pct"]) == pytest.approx((10 - 60) / 60)

    async def test_change_ratio_by_store(self, exec_engine) -> None:
        """``change(revenue:sum / qty:sum)`` = ratio − prior ratio, per store,
        NULL in each store's first month (A: 6→8→10, B: 6→10→5)."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[
                ModelMeasure(formula="change(revenue:sum / qty:sum)", name="delta"),
            ],
        ))
        by = _by_store_month(resp)
        assert by[("A", "2024-01")]["sales.delta"] is None
        assert by[("B", "2024-01")]["sales.delta"] is None
        assert _f(by[("A", "2024-02")]["sales.delta"]) == pytest.approx(2.0)
        assert _f(by[("A", "2024-03")]["sales.delta"]) == pytest.approx(2.0)
        assert _f(by[("B", "2024-02")]["sales.delta"]) == pytest.approx(4.0)
        assert _f(by[("B", "2024-03")]["sales.delta"]) == pytest.approx(-5.0)

    async def test_coalesce_missing_bucket_is_null(self, exec_engine) -> None:
        """A NULL-absorbing wrapper does NOT turn a missing shifted bucket into
        0: the earliest month's shifted value is NULL."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="coalesce(revenue:sum, 0)", name="cur"),
            ModelMeasure(formula="time_shift(coalesce(revenue:sum, 0), -1)",
                         name="prev"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["sales.cur"]) == pytest.approx(60.0)
        assert by["2024-01"]["sales.prev"] is None
        assert _f(by["2024-02"]["sales.prev"]) == pytest.approx(60.0)
        assert _f(by["2024-03"]["sales.prev"]) == pytest.approx(100.0)

    async def test_two_differently_parameterized_leaves(self, exec_engine) -> None:
        """A join-crossing fragment leaf (``wrevenue_sum``) plus a column-filtered
        leaf (``hi_rev``): each re-aggregates with its own parameters/filter in
        the shifted period."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="revenue:wrevenue_sum + hi_rev:sum", name="cur"),
            ModelMeasure(formula="time_shift(revenue:wrevenue_sum + hi_rev:sum, -1)",
                         name="prev"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["sales.cur"]) == pytest.approx(200.0)  # 150 + 50
        assert _f(by["2024-02"]["sales.cur"]) == pytest.approx(360.0)  # 260 + 100
        assert _f(by["2024-03"]["sales.cur"]) == pytest.approx(180.0)  # 130 + 50
        assert by["2024-01"]["sales.prev"] is None
        assert _f(by["2024-02"]["sales.prev"]) == pytest.approx(200.0)
        assert _f(by["2024-03"]["sales.prev"]) == pytest.approx(360.0)

    async def test_crossing_param_registers_join_per_leaf(self, exec_engine) -> None:
        """A composite whose only aggregate leaf crosses the ``regions`` join:
        the shifted CTE must bind that join to compute ``SUM(revenue * factor)``
        in the prior period."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="revenue:wrevenue_sum * 2", name="cur"),
            ModelMeasure(formula="time_shift(revenue:wrevenue_sum * 2, -1)",
                         name="prev"),
        ]))
        by = _by_month(resp)
        assert _f(by["2024-01"]["sales.cur"]) == pytest.approx(300.0)  # 150 * 2
        assert _f(by["2024-02"]["sales.cur"]) == pytest.approx(520.0)  # 260 * 2
        assert by["2024-01"]["sales.prev"] is None
        assert _f(by["2024-02"]["sales.prev"]) == pytest.approx(300.0)
        assert _f(by["2024-03"]["sales.prev"]) == pytest.approx(520.0)


# --------------------------------------------------------------------------- #
# consecutive_periods over composite / value inputs — executed values (1.3).
# --------------------------------------------------------------------------- #
class TestConsecutivePeriodsExecution:
    async def test_numeric_delta_truthiness(self, exec_engine) -> None:
        """A bare numeric composite: streak counts consecutive months where the
        delta is non-NULL and non-zero (40, 62, 30 → 1, 2, 3)."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="consecutive_periods(revenue:sum - cost:sum)",
                         name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["sales.streak"]) == 1
        assert int(by["2024-02"]["sales.streak"]) == 2
        assert int(by["2024-03"]["sales.streak"]) == 3

    async def test_growth_streak_over_nested_transform(self, exec_engine) -> None:
        """Comparison predicate over a nested transform (already renders today —
        regression anchor): change > 0 holds only in Feb → 0, 1, 0."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="consecutive_periods(change(revenue:sum) > 0)",
                         name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["sales.streak"]) == 0
        assert int(by["2024-02"]["sales.streak"]) == 1
        assert int(by["2024-03"]["sales.streak"]) == 0

    async def test_bare_nested_transform(self, exec_engine) -> None:
        """A bare nested transform value: cumsum is non-zero every month → 1,2,3."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="consecutive_periods(cumsum(revenue:sum))",
                         name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["sales.streak"]) == 1
        assert int(by["2024-02"]["sales.streak"]) == 2
        assert int(by["2024-03"]["sales.streak"]) == 3

    async def test_scalar_call_in_comparison(self, exec_engine) -> None:
        """Scalar call inside a comparison (already renders today — regression
        anchor): round(revenue:sum) >= 10 holds every month → 1, 2, 3."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="consecutive_periods(round(revenue:sum) >= 10)",
                         name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["sales.streak"]) == 1
        assert int(by["2024-02"]["sales.streak"]) == 2
        assert int(by["2024-03"]["sales.streak"]) == 3

    async def test_or_connective(self, exec_engine) -> None:
        """``revenue:sum > 90 or cost:sum > 40`` holds only in Feb → 0, 1, 0."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(
                formula="consecutive_periods(revenue:sum > 90 or cost:sum > 40)",
                name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["sales.streak"]) == 0
        assert int(by["2024-02"]["sales.streak"]) == 1
        assert int(by["2024-03"]["sales.streak"]) == 0

    async def test_not_connective(self, exec_engine) -> None:
        """``not (revenue:sum > 90)`` fails only in Feb → 1, 0, 1."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="consecutive_periods(not (revenue:sum > 90))",
                         name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["sales.streak"]) == 1
        assert int(by["2024-02"]["sales.streak"]) == 0
        assert int(by["2024-03"]["sales.streak"]) == 1

    async def test_in_predicate(self, exec_engine) -> None:
        """A top-level IN over a grouped dimension: both stores are in the set
        every month → each streak 1, 2, 3."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(store in ('A', 'B'))", name="streak")],
        ))
        by = _by_store_month(resp)
        for store in ("A", "B"):
            assert int(by[(store, "2024-01")]["sales.streak"]) == 1
            assert int(by[(store, "2024-02")]["sales.streak"]) == 2
            assert int(by[(store, "2024-03")]["sales.streak"]) == 3

    async def test_negated_in_predicate(self, exec_engine) -> None:
        """A negated IN: store A is never in {B, C} (true → 1,2,3), store B
        always is (false → 0,0,0)."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(store not in ('B', 'C'))",
                name="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-03")]["sales.streak"]) == 3
        assert int(by[("B", "2024-01")]["sales.streak"]) == 0
        assert int(by[("B", "2024-03")]["sales.streak"]) == 0

    async def test_null_predicate_group_treated_as_false(self, exec_engine) -> None:
        """A group whose ``or`` predicate evaluates to NULL breaks the streak
        (NULL treated as false): store B's March ``hi_rev:sum`` is NULL (no row
        clears the >15 filter), so ``hi_rev:sum > 0 or cost:sum > 1000`` is NULL
        there → B streak 1, 2, 0; store A never NULL → 1, 2, 3."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(hi_rev:sum > 0 or cost:sum > 1000)",
                name="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-03")]["sales.streak"]) == 3
        assert int(by[("B", "2024-01")]["sales.streak"]) == 1
        assert int(by[("B", "2024-02")]["sales.streak"]) == 2
        assert int(by[("B", "2024-03")]["sales.streak"]) == 0

    async def test_nested_in_under_and(self, exec_engine) -> None:
        """An IN nested under ``and`` (its column must materialise): store A
        matches with revenue>0 (1,2,3); store B never matches (0,0,0)."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(store in ('A', 'C') and revenue:sum > 0)",
                name="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-01")]["sales.streak"]) == 1
        assert int(by[("A", "2024-03")]["sales.streak"]) == 3
        assert int(by[("B", "2024-02")]["sales.streak"]) == 0


# --------------------------------------------------------------------------- #
# consecutive_periods predicate typing contract (task 1.4).
# --------------------------------------------------------------------------- #
class TestPredicateTypingContract:
    async def test_iif_condition_position_accepts_predicate(self, exec_engine) -> None:
        """A boolean-shaped node is legal in ``iif``'s condition seat; the value
        drives truthiness (iif → 1 every month → 1, 2, 3)."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(
                formula="consecutive_periods(iif(revenue:sum > 0, 1, 0))",
                name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["sales.streak"]) == 1
        assert int(by["2024-03"]["sales.streak"]) == 3

    async def test_boolean_in_arithmetic_rejected(self) -> None:
        name, msg = await _error([ModelMeasure(
            formula="consecutive_periods((revenue:sum > 0) + (cost:sum > 0))",
            name="x")])
        assert name == "ValueError", (name, msg)
        low = msg.lower()
        assert "consecutive_periods" in low, msg
        assert "boolean" in low and ("numeric" in low or "arithmetic" in low), msg

    async def test_boolean_as_scalar_call_argument_rejected(self) -> None:
        name, msg = await _error([ModelMeasure(
            formula="consecutive_periods(coalesce(revenue:sum > 0, 0))", name="x")])
        assert name == "ValueError", (name, msg)
        low = msg.lower()
        assert "consecutive_periods" in low and "boolean" in low, msg

    async def test_string_family_predicate_rejected(self) -> None:
        name, msg = await _error([ModelMeasure(
            formula="consecutive_periods(lower(sku:max))", name="x")])
        assert name == "ValueError", (name, msg)
        low = msg.lower()
        assert "consecutive_periods" in low, msg
        assert "string" in low and ("truthiness" in low or "predicate" in low), msg


# --------------------------------------------------------------------------- #
# Uniform fail-closed errors (task 1.5) — every render path raises the SAME
# ValueError, and a cross-model sibling never changes which error fires.
# --------------------------------------------------------------------------- #
class TestUniformFailClosed:
    #: shape id -> the still-unsupported time_shift input.
    SHAPES = {
        "nested_transform": "time_shift(cumsum(revenue:sum), -1)",
        "mixed_composite": "time_shift(revenue:sum * weight, -1)",
        "pure_row_composite": "time_shift(weight * qty, -1)",
        "cross_model_leaf": "time_shift(revenue:sum + regions.factor:sum, -1)",
    }
    #: shape id -> substrings the user-facing message must name.
    TOKENS = {
        "nested_transform": ("time_shift", "transform", "source_queries"),
        "mixed_composite": ("time_shift", "row", "source_queries"),
        "pure_row_composite": ("time_shift", "row", "source_queries"),
        "cross_model_leaf": ("time_shift", "cross-model", "source_queries"),
    }

    @pytest.mark.parametrize("shape", list(SHAPES))
    async def test_plain_path_names_shape_and_remedy(self, shape) -> None:
        name, msg = await _error([ModelMeasure(formula=self.SHAPES[shape], name="prev")])
        assert name == "ValueError", (shape, name, msg)
        low = msg.lower()
        for token in self.TOKENS[shape]:
            assert token in low, (shape, token, msg)

    @pytest.mark.parametrize("shape", list(SHAPES))
    async def test_same_error_with_and_without_cross_model_sibling(self, shape) -> None:
        """The presence of a cross-model measure elsewhere must not change which
        error the unsupported shape produces (today the plain path and the
        cross-model-chain path diverge)."""
        plain = await _error([ModelMeasure(formula=self.SHAPES[shape], name="prev")])
        with_sibling = await _error([
            ModelMeasure(formula="regions.factor:sum", name="cm"),
            ModelMeasure(formula=self.SHAPES[shape], name="prev"),
        ])
        assert plain == with_sibling, (shape, plain, with_sibling)


# --------------------------------------------------------------------------- #
# Planner dep-walk completeness (task 1.6) — _iter_slot_deps must surface the
# column leaves of BetweenKey / InKey wherever they nest.
# --------------------------------------------------------------------------- #
class TestIterSlotDepsRecursion:
    STATUS = ColumnKey(path=(), leaf="status")
    QTY = ColumnKey(path=(), leaf="qty")
    INK = InKey(column=STATUS, values=(LiteralKey(value="a"), LiteralKey(value="b")))
    BET = BetweenKey(
        column=QTY, low=LiteralKey(value=Decimal(1)), high=LiteralKey(value=Decimal(10)),
    )
    _ONE = LiteralKey(value=Decimal(1))
    _ZERO = LiteralKey(value=Decimal(0))

    def _deps(self, key):
        return list(_iter_slot_deps(key))

    def test_top_level_in_surfaces_column(self) -> None:
        assert self.STATUS in self._deps(self.INK)

    def test_top_level_between_surfaces_column(self) -> None:
        assert self.QTY in self._deps(self.BET)

    def test_in_nested_under_scalar_call_surfaces_column(self) -> None:
        key = ScalarCallKey(name="iif", args=(self.INK, self._ONE, self._ZERO))
        assert self.STATUS in self._deps(key)

    def test_between_nested_under_scalar_call_surfaces_column(self) -> None:
        key = ScalarCallKey(name="iif", args=(self.BET, self._ONE, self._ZERO))
        assert self.QTY in self._deps(key)

    def test_in_nested_under_arithmetic_surfaces_column(self) -> None:
        key = ArithmeticKey(op="and", operands=(self.INK, self.INK))
        assert self.STATUS in self._deps(key)

    def test_between_nested_under_arithmetic_surfaces_column(self) -> None:
        key = ArithmeticKey(op="and", operands=(self.BET, self.BET))
        assert self.QTY in self._deps(key)
