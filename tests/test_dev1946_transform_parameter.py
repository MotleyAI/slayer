"""DEV-1946 — a grained transform (``rank``, ``cumsum``, ``last`` …) is a legal
aggregation parameter in every position and mode, typed at its result grain and
failing closed exactly where an aggregate parameter would. Executed on SQLite +
DuckDB (DEV-1840 orders graph, DEV-1847 sales graph).

Spec: queries/partitioned-aggregates — "Attached parameters on row-level
sources", "Re-aggregation consumes attached operands as datasets";
aggregations/functional-form — "Positional parameters fold onto declared
parameter order". Axiom 9 (closure), 11.4 (parameter position), 11.2/11.3
(rank / time-ordered grain).
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.core.keys import AggregateKey, ColumnKey, Grain, TimeTruncKey, TransformKey
from slayer.core.refs import agg_kwarg_canonical_str
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.plan import plan_query
from slayer.engine.syntax import parse_expr
from slayer.ir.planned import PlainProducerKernel
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1840_fixtures import (
    bundle as orders_bundle,
    dev1840_models,
    gen as orders_gen,
    make_exec_engine as make_orders_engine,
)
from tests._dev1841_fixtures import ModelMeasure, associated_warnings, broadcast_warnings
from tests._dev1847_fixtures import (
    dev1847_models,
    gen as sales_gen,
    make_exec_engine as make_sales_engine,
    sales_q,
)
from tests._dev1892_fixtures import assert_grain_residue
from tests._dev1919_fixtures import (
    MODES,
    assert_cells,
    mode_q,
    month_vals,
    ordered_month_td,
    signup_month_td,
    status_vals,
    tier_vals,
)
from tests._dev1946_fixtures import (
    DET_CUMSUM,
    EXPR_ARG,
    LAST_PARAM,
    LAST_RANK_KEY,
    LOCAL_SALES,
    LOCAL_SALES_CITY,
    NESTED,
    NOAXIS_CUMSUM,
    OWN_PARTITION,
    RANKED,
    RANKED_POSITIONAL,
    REAGG_RANK_COUNT,
    REAGG_RANK_PRODUCT,
    UNGRAINED_RANK,
    WINDOWED_INNER,
    WINDOWED_OUTER,
    XMODEL_LOCAL,
    det_cumsum_by_signup_month,
    global_val,
    last_by_tier_month,
    last_default_value,
    nested_by_tier,
    nested_global,
    ordered_month_vals,
    own_by_tier,
    own_global,
    ranked_assoc_by_status,
    reagg_rank_by_region,
    region_city_vals,
    region_vals,
    tier_month_vals,
    ungrained_rank_by_tier,
    verify_oracles,
    windowed_outer_by_signup_month,
    xmodel_by_status,
    xmodel_global,
    local_sales_by_region,
    local_sales_by_region_city,
    local_sales_global,
)

AMOUNT = ModelMeasure(formula="amount:sum", name="a")


def _m(formula: str) -> ModelMeasure:
    return ModelMeasure(formula=formula, name="w")


@pytest.fixture(params=["sqlite", "duckdb"])
async def orders_engine(request):
    async for engine in make_orders_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def sales_engine(request):
    async for engine in make_sales_engine(request):
        yield engine


def _no_mode_warnings(resp) -> None:
    assert not broadcast_warnings(resp)
    assert not associated_warnings(resp)


def _assert_time_axis_error(exc) -> None:
    """The time-axis error names the axis and the ``partition_by=`` remedy."""
    msg = str(exc)
    assert "time axis" in msg, msg
    assert "partition_by" in msg, msg


def _orders_scope_bundle():
    models = dev1840_models()
    return ModelScope(source_model=models[0]), orders_bundle()


def _sales_scope_bundle():
    models = dev1847_models()
    return (ModelScope(source_model=models[0]),
            ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:]))


# --------------------------------------------------------------------------- #
# Dataset guard (green without the feature): every oracle == its spec constant.
# --------------------------------------------------------------------------- #
class TestOracleSelfCheck:
    def test_oracles_reproduce_spec_constants(self):
        verify_oracles()


# --------------------------------------------------------------------------- #
# Executed shapes.
# --------------------------------------------------------------------------- #
class TestPositionalEqualsKeyword:
    """Scenario: Positional transform parameter folds onto the declared name."""

    async def test_positional_equals_keyword_values(self, orders_engine):
        kw = await orders_engine.execute(
            mode_q(None, dimensions=["customers.tier"], measures=[_m(RANKED)]))
        pos = await orders_engine.execute(
            mode_q(None, dimensions=["customers.tier"], measures=[_m(RANKED_POSITIONAL)]))
        assert tier_vals(pos) == tier_vals(kw)
        assert_cells(tier_vals(kw),
                     {"gold": 475 / 8, "silver": 97.5, "bronze": 40.0, None: None})


class TestAssociateMode:
    """Scenario: Associate-mode transform parameter."""

    async def test_associate_by_status(self, orders_engine):
        resp = await orders_engine.execute(
            mode_q("associate", dimensions=["status"], measures=[_m(RANKED)]))
        assert_cells(status_vals(resp), ranked_assoc_by_status())
        assert associated_warnings(resp)


class TestLocalRootSales:
    """Scenario: Local-root transform parameter (row-attached rank of the row's cell)."""

    async def test_by_region(self, sales_engine):
        resp = await sales_engine.execute(
            sales_q(dimensions=["region"], measures=[_m(LOCAL_SALES)]))
        assert_cells(region_vals(resp), local_sales_by_region())
        _no_mode_warnings(resp)

    async def test_by_region_and_city(self, sales_engine):
        resp = await sales_engine.execute(
            sales_q(dimensions=["region", "city"], measures=[_m(LOCAL_SALES_CITY)]))
        got = region_city_vals(resp)
        want = local_sales_by_region_city()
        assert set(got) == set(want), (set(got), set(want))
        for key, value in want.items():
            if value is None:
                assert got[key] is None, key
            else:
                assert float(got[key]) == pytest.approx(value), key

    async def test_global(self, sales_engine):
        resp = await sales_engine.execute(sales_q(measures=[_m(LOCAL_SALES)]))
        assert float(global_val(resp)) == pytest.approx(local_sales_global())  # 810/43


class TestCollapsingLast:
    """Scenario: Collapsing transform parameter drops the axis."""

    async def test_default_mode_broadcasts_over_months(self, orders_engine):
        resp = await orders_engine.execute(
            mode_q(None, time_dimensions=ordered_month_td(), measures=[_m(LAST_PARAM)]))
        vals = ordered_month_vals(resp)
        assert set(vals) == {"2024-01", "2024-02", "2024-03", "2024-04"}
        for value in vals.values():
            assert float(value) == pytest.approx(last_default_value())  # 8165/128
        assert broadcast_warnings(resp)  # broadcast over the unattributable month

    @pytest.mark.parametrize("mode", [None, "broadcast"])
    async def test_by_tier_and_month(self, orders_engine, mode):
        resp = await orders_engine.execute(mode_q(
            mode, dimensions=["customers.tier"], time_dimensions=ordered_month_td(),
            measures=[_m(LAST_PARAM)]))
        assert_cells(tier_month_vals(resp), last_by_tier_month())
        assert broadcast_warnings(resp)


class TestNoAxisCumsum:
    """Scenario: Time-ordered transform parameter without its axis fails closed."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_measure_position(self, orders_engine, mode):
        q = mode_q(mode, time_dimensions=ordered_month_td(), measures=[_m(NOAXIS_CUMSUM)])
        with pytest.raises(NotImplementedError) as ei:
            await orders_engine.execute(q)
        _assert_time_axis_error(ei.value)

    @pytest.mark.parametrize("mode", MODES)
    async def test_filter_position(self, orders_engine, mode):
        q = mode_q(mode, dimensions=["customers.tier"],
                   time_dimensions=ordered_month_td(), measures=[AMOUNT],
                   filters=[f"{NOAXIS_CUMSUM} > 50"])
        with pytest.raises(NotImplementedError) as ei:
            await orders_engine.execute(q)
        _assert_time_axis_error(ei.value)


class TestDeterminedCumsum:
    """Scenario: Time-ordered transform parameter over a determined axis executes."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_values_every_mode(self, orders_engine, mode):
        resp = await orders_engine.execute(
            mode_q(mode, time_dimensions=signup_month_td(), measures=[_m(DET_CUMSUM)]))
        assert_cells(month_vals(resp), {**det_cumsum_by_signup_month(), None: None})
        _no_mode_warnings(resp)

    async def test_filter_gt_50_keeps_jan_feb(self, orders_engine):
        full = await orders_engine.execute(
            mode_q(None, time_dimensions=signup_month_td(), measures=[AMOUNT]))
        pruned = await orders_engine.execute(mode_q(
            None, time_dimensions=signup_month_td(), measures=[AMOUNT],
            filters=[f"{DET_CUMSUM} > 50"]))
        kept = month_vals(pruned, "a")
        assert set(kept) == {"2024-01", "2024-02"}
        for month, value in kept.items():        # surviving values unchanged
            assert float(value) == pytest.approx(float(month_vals(full, "a")[month]))

    async def test_order_by_descending(self, orders_engine):
        resp = await orders_engine.execute(mode_q(
            None, time_dimensions=signup_month_td(), measures=[AMOUNT],
            order=[{"column": DET_CUMSUM, "direction": "desc"}]))
        months = [k for k in month_vals(resp, "a") if k is not None]
        assert months == ["2024-02", "2024-01", "2024-03", "2024-04"]


class TestUngrainedInner:
    """Scenario: Transform over an ungrained inner types at the query grain."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_by_tier_executes(self, orders_engine, mode):
        resp = await orders_engine.execute(
            mode_q(mode, dimensions=["customers.tier"], measures=[_m(UNGRAINED_RANK)]))
        assert_cells(tier_vals(resp), {**ungrained_rank_by_tier(), None: None})
        _no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_by_status_fails_closed(self, orders_engine, mode):
        q = mode_q(mode, dimensions=["status"], measures=[_m(UNGRAINED_RANK)])
        with pytest.raises(SlayerError) as ei:
            await orders_engine.execute(q)
        assert_grain_residue(ei.value, param="weight")


class TestWindowedInner:
    """Scenario: Windowed inner joins the bucket to the parameter's grain — the
    order-month bucket is undetermined by ``customers``."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_fails_closed_every_mode(self, orders_engine, mode):
        q = mode_q(mode, time_dimensions=ordered_month_td(), measures=[_m(WINDOWED_INNER)])
        with pytest.raises(SlayerError) as ei:
            await orders_engine.execute(q)
        assert_grain_residue(ei.value, param="weight")


class TestWindowedOuter:
    """Scenario: Windowed aggregation with a transform parameter — the
    trailing-window picks the transform once per interval row."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_by_signup_month_every_mode(self, orders_engine, mode):
        resp = await orders_engine.execute(mode_q(
            mode, time_dimensions=signup_month_td(), measures=[_m(WINDOWED_OUTER)]))
        assert_cells(month_vals(resp), {**windowed_outer_by_signup_month(), None: None})
        _no_mode_warnings(resp)


class TestRankedFilterAndOrder:
    """Scenario: Transform parameter in filter and order positions."""

    async def test_filter_gt_50_by_tier(self, orders_engine):
        full = await orders_engine.execute(
            mode_q(None, dimensions=["customers.tier"], measures=[AMOUNT]))
        pruned = await orders_engine.execute(mode_q(
            None, dimensions=["customers.tier"], measures=[AMOUNT],
            filters=[f"{RANKED} > 50"]))
        kept = tier_vals(pruned, "a")
        assert set(kept) == {"gold", "silver"}  # 59.375, 97.5 > 50; bronze 40, NULL out
        for tier, value in kept.items():        # surviving values unchanged
            assert float(value) == pytest.approx(float(tier_vals(full, "a")[tier]))

    async def test_order_by_descending(self, orders_engine):
        resp = await orders_engine.execute(mode_q(
            None, dimensions=["customers.tier"], measures=[AMOUNT],
            order=[{"column": RANKED, "direction": "desc"}]))
        tiers = [r["orders.customers.tier"] for r in resp.data
                 if r["orders.customers.tier"] is not None]
        assert tiers == ["silver", "gold", "bronze"]  # 97.5, 59.375, 40


class TestNestedParameter:
    """Scenario: Nested transform parameter inside an attached aggregate parameter."""

    @pytest.mark.parametrize("mode", [None, "broadcast"])
    async def test_broadcast_to_status(self, orders_engine, mode):
        resp = await orders_engine.execute(
            mode_q(mode, dimensions=["status"], measures=[_m(NESTED)]))
        got = status_vals(resp)
        assert set(got) == {"ok", "new"}
        for value in got.values():
            assert float(value) == pytest.approx(nested_global())  # 45340/621
        (warning,) = broadcast_warnings(resp)
        assert "status" in warning.human_message()

    @pytest.mark.parametrize("mode", MODES)
    async def test_by_tier_every_mode(self, orders_engine, mode):
        resp = await orders_engine.execute(
            mode_q(mode, dimensions=["customers.tier"], measures=[_m(NESTED)]))
        assert_cells(tier_vals(resp), {**nested_by_tier(), None: None})
        _no_mode_warnings(resp)


class TestCrossModelLocalRoot:
    """Scenario: Cross-model transform parameter on a local root."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_global(self, orders_engine, mode):
        resp = await orders_engine.execute(mode_q(mode, measures=[_m(XMODEL_LOCAL)]))
        (row,) = resp.data
        assert float(row["orders.w"]) == pytest.approx(xmodel_global())  # 281/16
        _no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_by_status(self, orders_engine, mode):
        resp = await orders_engine.execute(
            mode_q(mode, dimensions=["status"], measures=[_m(XMODEL_LOCAL)]))
        assert_cells(status_vals(resp), xmodel_by_status())  # ok 116/11, new 33
        _no_mode_warnings(resp)


class TestOwnPartitionBy:
    """Scenario: Transform parameter with its own partition_by outside the query
    dimensions — exempt from the combined-consumer partition-key rule."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_global(self, orders_engine, mode):
        resp = await orders_engine.execute(mode_q(mode, measures=[_m(OWN_PARTITION)]))
        (row,) = resp.data
        assert float(row["orders.w"]) == pytest.approx(own_global())  # 760/11
        _no_mode_warnings(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_by_tier(self, orders_engine, mode):
        resp = await orders_engine.execute(
            mode_q(mode, dimensions=["customers.tier"], measures=[_m(OWN_PARTITION)]))
        assert_cells(tier_vals(resp), {**own_by_tier(), None: None})
        _no_mode_warnings(resp)


# --------------------------------------------------------------------------- #
# Plan shape.
# --------------------------------------------------------------------------- #
def _customers_attach(planned):
    (att,) = [a for a in planned.regroup_attach_plans
              if a.producer_root_model == "customers"]
    return att


def _attaches_of_kind(planned, kind: str) -> list:
    return [a for a in planned.regroup_attach_plans
            if getattr(a.kernel, "kind", None) == kind]


class TestPlanShape:
    """Scenario: Transform parameter plan shape — the plain kernel under default /
    error, the association / trailing-window kernels each carry ``weight``."""

    @pytest.mark.parametrize("mode, dims", [
        (None, ["status"]), (None, ["customers.tier"]), ("error", ["customers.tier"]),
    ])
    def test_plain_kernel_under_default_and_error(self, mode, dims):
        att = _customers_attach(plan_query(
            query=mode_q(mode, dimensions=dims, measures=[_m(RANKED)]),
            bundle=orders_bundle()))
        assert isinstance(att.kernel, PlainProducerKernel)  # no new kernel

    def test_association_kernel_picks_the_weight(self):
        planned = plan_query(
            query=mode_q("associate", dimensions=["status"], measures=[_m(RANKED)]),
            bundle=orders_bundle())
        [attach] = _attaches_of_kind(planned, "association")
        assert "weight" in [p.name for p in attach.kernel.picked_params]

    def test_trailing_window_kernel_picks_the_weight(self):
        planned = plan_query(
            query=mode_q(None, time_dimensions=signup_month_td(),
                         measures=[_m(WINDOWED_OUTER)]),
            bundle=orders_bundle())
        [attach] = _attaches_of_kind(planned, "trailing-window")
        assert "weight" in [p.name for p in attach.kernel.picked_params]

    @pytest.mark.parametrize("formula, tds, mode", [
        (RANKED, None, None), (RANKED, None, "associate"),
        (LAST_PARAM, ordered_month_td(), None),
        (DET_CUMSUM, signup_month_td(), None),
        (WINDOWED_OUTER, signup_month_td(), None),
    ])
    async def test_scope_closed_no_placeholder_leak(self, formula, tds, mode):
        kw = {} if tds is None else {"time_dimensions": tds}
        dims = None if tds is not None else ["status"]
        q = mode_q(mode, measures=[_m(formula)],
                   **({"dimensions": dims} if dims else {}), **kw)
        sql = await orders_gen(q, dialect="sqlite")
        assert "__regroup__" not in sql, sql
        assert_scope_closed(sql, dialect="sqlite")

    async def test_adding_the_measure_is_cardinality_neutral(self, orders_engine):
        base = await orders_engine.execute(
            mode_q(None, dimensions=["status"], measures=[AMOUNT]))
        withp = await orders_engine.execute(
            mode_q(None, dimensions=["status"], measures=[AMOUNT, _m(RANKED)]))
        assert len(withp.data) == len(base.data)
        assert status_vals(withp, "a") == pytest.approx(status_vals(base, "a"))


# --------------------------------------------------------------------------- #
# Re-aggregation outer (sales graph).
# --------------------------------------------------------------------------- #
def _sales_bundle() -> ResolvedSourceBundle:
    models = dev1847_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


class TestReaggregationOuter:
    """Scenario: Transform-valued outer parameter rides the carrier / outside the
    operand grain fails closed."""

    async def test_by_region(self, sales_engine):
        resp = await sales_engine.execute(
            sales_q(dimensions=["region"], measures=[_m(REAGG_RANK_COUNT)]))
        assert_cells(region_vals(resp), reagg_rank_by_region())

    def test_carrier_picks_the_transform_weight(self):
        bundle = _sales_bundle()
        # The weight IS a transform constituent (not a column/aggregate).
        bound = bind_expr(parse_expr(REAGG_RANK_COUNT),
                          scope=ModelScope(source_model=bundle.source_model), bundle=bundle)
        assert isinstance(bound.value_key, AggregateKey)
        assert isinstance(dict(bound.value_key.kwargs)["weight"], TransformKey)
        # …and rides the carrier as a picked parameter named ``weight``.
        planned = plan_query(
            query=sales_q(dimensions=["region"], measures=[_m(REAGG_RANK_COUNT)]),
            bundle=bundle)
        picked = [p.name for a in planned.regroup_attach_plans
                  for p in getattr(a.kernel, "picked_params", [])]
        assert "weight" in picked

    async def test_scope_closed_no_leak(self):
        sql = await sales_gen(
            sales_q(dimensions=["region"], measures=[_m(REAGG_RANK_COUNT)]),
            dialect="sqlite")
        assert "__regroup__" not in sql, sql
        assert_scope_closed(sql, dialect="sqlite")

    async def test_partition_by_product_fails_closed(self, sales_engine):
        q = sales_q(dimensions=["region"], measures=[_m(REAGG_RANK_PRODUCT)])
        with pytest.raises(SlayerError) as ei:
            await sales_engine.execute(q)
        assert_grain_residue(ei.value, param="weight")


# --------------------------------------------------------------------------- #
# Bind level.
# --------------------------------------------------------------------------- #
class TestBindLevel:
    """Scenarios: the transform argument binds to a ``TransformKey``; positional
    ≡ keyword identity; invalid first/last ranking key; expression arg refused."""

    def test_transform_kwarg_binds_to_transform_key(self):
        scope, bundle = _sales_scope_bundle()
        bound = bind_expr(parse_expr(LOCAL_SALES), scope=scope, bundle=bundle)
        assert isinstance(bound.value_key, AggregateKey)
        assert isinstance(dict(bound.value_key.kwargs)["weight"], TransformKey)

    def test_positional_equals_keyword_identity(self):
        scope, bundle = _sales_scope_bundle()
        positional = "weighted_avg(amount, rank(sum(amount, partition_by=region)))"
        kw = bind_expr(parse_expr(LOCAL_SALES), scope=scope, bundle=bundle)
        pos = bind_expr(parse_expr(positional), scope=scope, bundle=bundle)
        assert pos.value_key == kw.value_key

    def test_transform_ranking_key_refused(self):
        scope, bundle = _orders_scope_bundle()
        parsed = parse_expr(LAST_RANK_KEY)
        with pytest.raises(ValueError, match="ranks by a column"):
            bind_expr(parsed, scope=scope, bundle=bundle)

    def test_expression_argument_still_refused(self):
        """Non-goal: an expression parameter stays refused, and the reworded
        message names grained transforms among the accepted kinds (D1)."""
        scope, bundle = _sales_scope_bundle()
        parsed = parse_expr(EXPR_ARG)
        with pytest.raises(ValueError, match="grained transform"):
            bind_expr(parsed, scope=scope, bundle=bundle)

    def test_time_bucketed_partition_key_granularity_in_fragment(self):
        """A rank parameter partitioned by the same column at different
        granularities gets distinct canonical fragments — the fragment keeps a
        ``TimeTruncKey``'s granularity, as it already does for the resolved
        ``time_key``, so month- and year-bucketed aliases stay legible and stable."""
        inner = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                             partition_keys=Grain.of([ColumnKey(leaf="region")]))

        def _ranked(gran: str) -> TransformKey:
            return TransformKey(op="rank", input=inner, partition_keys=Grain.of([
                TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity=gran)]))

        assert agg_kwarg_canonical_str(_ranked("month")) != agg_kwarg_canonical_str(_ranked("year"))
