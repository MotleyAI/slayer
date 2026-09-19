"""DEV-1919 — an attached input (aggregate-valued parameter or source
constituent) is compiled at its own home and attached per home row in every
``to_many_handling`` mode; the mode governs only the aggregation's own
unattributable dimensions. Executed on SQLite + DuckDB (DEV-1840 dataset).

Spec: queries/partitioned-aggregates — "Attached parameters on row-level
sources"; aggregations/expression-aggregation — "Expression source typing";
queries/attribution-modes — "Association eligibility and input handling";
queries/cross-model-aggregates — "Unsafe aggregate inputs fail closed".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.engine.plan import plan_query
from slayer.ir.planned import PlainProducerKernel
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1840_fixtures import bundle, gen, make_exec_engine as make_orders_engine
from tests._dev1841_fixtures import ModelMeasure, associated_warnings, broadcast_warnings
from tests._dev1859_fixtures import broadcast_wavg_global
from tests._dev1892_fixtures import assert_grain_residue, assert_ref_free
from tests._dev1900_fixtures import (
    make_exec_engine as make_1900_engine,
    unparseable_derived_models,
)
from tests._dev1919_fixtures import (
    CUMSUM_PARAM,
    HEADLINE,
    LAST_BADPOP,
    LAST_HOST,
    MIXED,
    MODES,
    OWN_FAN_BADPOP,
    OWN_FAN_STATUS,
    RANKED,
    RECURSIVE,
    STRAY_POSITIONAL,
    UNDETERMINED_PARAM,
    UNPARSE_PARAM,
    WINDOWED_CONSTITUENT,
    WINDOWED_PARAM,
    assert_cells,
    keyless_declared_models,
    local_wavg_status_assoc_by_status,
    mixed_by_tier,
    mixed_global,
    mode_q,
    month_vals,
    ordered_month_td,
    ranked_by_tier,
    ranked_global,
    recursive_by_tier,
    recursive_global,
    signup_month_td,
    status_vals,
    tier_vals,
    wavg_by_tier,
    windowed_constituent_by_month,
    windowed_param_by_month,
)

AMOUNT = ModelMeasure(formula="amount:sum", name="a")


def _m(formula: str) -> ModelMeasure:
    return ModelMeasure(formula=formula, name="w")


@pytest.fixture(params=["sqlite", "duckdb"])
async def orders_engine(request):
    async for engine in make_orders_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def keyless_engine(request):
    async for engine in make_orders_engine(request, models=keyless_declared_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def dev1900_engine(request):
    async for engine in make_1900_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_engine(request):
    async for engine in make_1900_engine(request, models=unparseable_derived_models()):
        yield engine


def _assert_no_mode_warnings(resp) -> None:
    assert not broadcast_warnings(resp)
    assert not associated_warnings(resp)


async def _assert_broadcast_to_status(engine, formula: str, expected, mode=None) -> None:
    resp = await engine.execute(mode_q(mode, dimensions=["status"], measures=[_m(formula)]))
    got = status_vals(resp)
    assert set(got) == {"ok", "new"}
    for value in got.values():
        assert float(value) == pytest.approx(expected)
    (warning,) = broadcast_warnings(resp)
    assert "status" in warning.human_message()
    assert not associated_warnings(resp)


async def _assert_error_mode_refuses_status(engine, formula: str) -> None:
    q = mode_q("error", dimensions=["status"], measures=[_m(formula)])
    with pytest.raises(SlayerError) as ei:
        await engine.execute(q)
    msg = str(ei.value)
    assert "status" in msg
    assert "attached input" not in msg
    assert "determine" not in msg


async def _assert_by_tier(engine, formula: str, expected, mode) -> None:
    resp = await engine.execute(
        mode_q(mode, dimensions=["customers.tier"], measures=[_m(formula)]))
    assert_cells(tier_vals(resp), {**expected, None: None})
    _assert_no_mode_warnings(resp)


class TestHeadline:
    """Scenarios: Default-mode twin of the associate shape; Every mode agrees on
    attributable dimensions."""

    @pytest.mark.parametrize("mode", [None, "broadcast"])
    async def test_default_mode_broadcasts_the_customers_rooted_value(self, orders_engine, mode):
        await _assert_broadcast_to_status(
            orders_engine, HEADLINE, broadcast_wavg_global(), mode=mode)

    async def test_error_mode_refuses_the_dimension_never_the_parameter(self, orders_engine):
        await _assert_error_mode_refuses_status(orders_engine, HEADLINE)

    @pytest.mark.parametrize("mode", MODES)
    async def test_every_mode_agrees_by_tier(self, orders_engine, mode):
        await _assert_by_tier(orders_engine, HEADLINE, wavg_by_tier(), mode)


class TestMixedSourceConstituent:
    """Scenario: Mixed-source constituent homed toward the host executes in every mode."""

    async def test_default_mode_broadcasts_the_customers_rooted_total(self, orders_engine):
        await _assert_broadcast_to_status(orders_engine, MIXED, mixed_global())

    async def test_error_mode_refuses_the_dimension(self, orders_engine):
        await _assert_error_mode_refuses_status(orders_engine, MIXED)

    @pytest.mark.parametrize("mode", MODES)
    async def test_every_mode_agrees_by_tier(self, orders_engine, mode):
        await _assert_by_tier(orders_engine, MIXED, mixed_by_tier(), mode)


class TestRecursiveNesting:
    """Scenario: Recursively nested attached parameters — three homes (customers,
    orders, regions), each producer attached one level up by its grain."""

    async def test_default_mode_broadcasts(self, orders_engine):
        await _assert_broadcast_to_status(orders_engine, RECURSIVE, recursive_global())

    @pytest.mark.parametrize("mode", MODES)
    async def test_every_mode_agrees_by_tier(self, orders_engine, mode):
        await _assert_by_tier(orders_engine, RECURSIVE, recursive_by_tier(), mode)


class TestAttributableDimensionsNeedNoAssociation:
    """Scenario: Attributable dimensions need no association (D4)."""

    async def test_keyless_home_takes_the_plain_path(self, keyless_engine):
        assoc = await keyless_engine.execute(
            mode_q("associate", dimensions=["customers.tier"], measures=[_m(HEADLINE)]))
        bcast = await keyless_engine.execute(
            mode_q(None, dimensions=["customers.tier"], measures=[_m(HEADLINE)]))
        assert_cells(tier_vals(assoc), {**wavg_by_tier(), None: None})
        assert_cells(tier_vals(bcast), {**wavg_by_tier(), None: None})
        _assert_no_mode_warnings(assoc)

    async def test_ranked_pick_with_attributable_dimensions_never_associates(self, orders_engine):
        """D4 control: first/last by an attributable dimension takes the plain path."""
        resp = await orders_engine.execute(mode_q(
            "associate", dimensions=["customers.tier"],
            measures=[_m("customers.spend:last(customers.signup_at)")]))
        assert_cells(tier_vals(resp), {"gold": 55.0, "silver": 80.0, "bronze": 40.0, None: None})
        _assert_no_mode_warnings(resp)

    async def test_association_still_needs_a_unique_key(self, keyless_engine):
        q = mode_q("associate", dimensions=["status"], measures=[_m(HEADLINE)])
        with pytest.raises(SlayerError) as ei:
            await keyless_engine.execute(q)
        msg = str(ei.value)
        assert "unique key" in msg
        assert "customers" in msg


def _customers_attach(planned):
    (att,) = [a for a in planned.regroup_attach_plans
              if a.producer_root_model == "customers"]
    return att


class TestPlanShape:
    """The customers-rooted plain attach nests the parameter's own orders-rooted
    producer (mirror of the association-arm plan pin)."""

    @pytest.mark.parametrize("mode, dims", [
        (None, ["status"]), (None, ["customers.tier"]),
        ("error", ["customers.tier"]), ("associate", ["customers.tier"]),
    ])
    def test_plain_attach_nests_an_orders_rooted_producer(self, mode, dims):
        att = _customers_attach(plan_query(
            query=mode_q(mode, dimensions=dims, measures=[_m(HEADLINE)]), bundle=bundle()))
        assert isinstance(att.kernel, PlainProducerKernel)  # D6: no new kernel
        assert "orders" in [n.producer_root_model
                            for n in att.producer_plan.regroup_attach_plans]

    @pytest.mark.parametrize("formula", [HEADLINE, MIXED, RECURSIVE])
    async def test_scope_closed_and_no_placeholder_leak(self, formula):
        sql = await gen(mode_q(None, dimensions=["status"], measures=[_m(formula)]),
                        dialect="sqlite")
        assert "__regroup__" not in sql, sql
        assert_scope_closed(sql, dialect="sqlite")

    async def test_adding_the_measure_is_cardinality_neutral(self, orders_engine):
        base = await orders_engine.execute(mode_q(None, dimensions=["status"], measures=[AMOUNT]))
        withp = await orders_engine.execute(
            mode_q(None, dimensions=["status"], measures=[AMOUNT, _m(HEADLINE)]))
        assert len(withp.data) == len(base.data)
        assert status_vals(withp, "a") == pytest.approx(status_vals(base, "a"))


class TestFilterAndOrderPositions:
    """Requirement: the shape is legal in filter (typing as a measure) and ORDER BY positions."""

    async def test_filter_prunes_with_values_unchanged(self, orders_engine):
        full = await orders_engine.execute(
            mode_q(None, dimensions=["customers.tier"], measures=[AMOUNT]))
        pruned = await orders_engine.execute(mode_q(
            None, dimensions=["customers.tier"], measures=[AMOUNT],
            filters=[f"{HEADLINE} > 50"]))
        kept = tier_vals(pruned, "a")
        assert set(kept) == {"gold", "silver"}  # 63.75, 138.33 > 50; bronze 40, NULL out
        for tier, value in kept.items():
            assert float(value) == pytest.approx(float(tier_vals(full, "a")[tier]))

    async def test_order_by_the_raw_formula(self, orders_engine):
        resp = await orders_engine.execute(mode_q(
            None, dimensions=["customers.tier"], measures=[AMOUNT],
            order=[{"column": HEADLINE, "direction": "desc"}]))
        tiers = [r["orders.customers.tier"] for r in resp.data
                 if r["orders.customers.tier"] is not None]
        assert tiers == ["silver", "gold", "bronze"]


class TestWindowedOuter:
    """Scenarios: Windowed aggregation with an attached constituent / parameter."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_windowed_constituent_every_mode(self, orders_engine, mode):
        resp = await orders_engine.execute(mode_q(
            mode, time_dimensions=signup_month_td(), measures=[_m(WINDOWED_CONSTITUENT)]))
        assert_cells(month_vals(resp), {**windowed_constituent_by_month(), None: None})
        _assert_no_mode_warnings(resp)

    @pytest.mark.xfail(strict=True, reason="window= restricted to sum/avg (DEV-1915)")
    @pytest.mark.parametrize("mode", MODES)
    async def test_windowed_parameter_every_mode(self, orders_engine, mode):
        resp = await orders_engine.execute(mode_q(
            mode, time_dimensions=signup_month_td(), measures=[_m(WINDOWED_PARAM)]))
        assert_cells(month_vals(resp), {**windowed_param_by_month(), None: None})
        _assert_no_mode_warnings(resp)


@pytest.mark.xfail(strict=True, reason="a transform is refused as an aggregation argument (DEV-1903)")
class TestRankedTransformParameter:
    """Scenario: Ranked transform as the attached parameter."""

    async def test_default_mode_broadcasts(self, orders_engine):
        await _assert_broadcast_to_status(orders_engine, RANKED, ranked_global())

    @pytest.mark.parametrize("mode", MODES)
    async def test_every_mode_agrees_by_tier(self, orders_engine, mode):
        await _assert_by_tier(orders_engine, RANKED, ranked_by_tier(), mode)


class TestFailClosedEveryMode:
    """The attached input's own ill-typed inputs, and a grain the home does not
    determine, are refused in every mode — never a multiplied or broadcast value."""

    @pytest.mark.xfail(strict=True, reason="a transform is refused as an aggregation argument (DEV-1903)")
    @pytest.mark.parametrize("mode", MODES)
    async def test_transform_parameter_grain_not_determined(self, orders_engine, mode):
        """Scenario: Transform parameter whose grain the home does not determine fails closed."""
        q = mode_q(mode, time_dimensions=ordered_month_td(), measures=[_m(CUMSUM_PARAM)])
        with pytest.raises(SlayerError) as ei:
            await orders_engine.execute(q)
        assert_grain_residue(ei.value, param="weight")

    @pytest.mark.parametrize("mode", MODES)
    async def test_unanalysable_dependency_inside_the_parameter(self, unparse_engine, mode):
        """Scenario: Unanalysable dependency inside an attached parameter fails closed."""
        q = mode_q(mode, dimensions=["customers.tier"], measures=[_m(UNPARSE_PARAM)])
        with pytest.raises(ValueError) as ei:
            await unparse_engine.execute(q)
        msg = str(ei.value)
        assert "unparseable" in msg
        assert "no supported dialect can analyse" in msg
        assert_ref_free(msg)

    @pytest.mark.parametrize("mode", MODES)
    async def test_parameter_key_fanning_from_its_own_home(self, dev1900_engine, mode):
        """Scenario: Attached parameter whose own partition key fans from its own home fails closed."""
        q = mode_q(mode, dimensions=["status"], measures=[_m(OWN_FAN_BADPOP)])
        with pytest.raises(ValueError) as ei:
            await dev1900_engine.execute(q)
        msg = str(ei.value)
        assert "bad_pop" in msg
        assert "region_events" in msg
        assert "every partition key must be attributable" in msg

    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_host_keyed_parameter_refused_outside_associate(self, orders_engine, mode):
        """Scenario: Attached parameter keyed by a host column keeps the mode-aware rule."""
        q = mode_q(mode, dimensions=["status"], measures=[_m(OWN_FAN_STATUS)])
        with pytest.raises(ValueError) as ei:
            await orders_engine.execute(q)
        msg = str(ei.value)
        assert "status" in msg
        assert "attributable from customers" in msg

    async def test_host_keyed_parameter_associates_under_associate(self, orders_engine):
        resp = await orders_engine.execute(
            mode_q("associate", dimensions=["status"], measures=[_m(OWN_FAN_STATUS)]))
        assert_cells(status_vals(resp), local_wavg_status_assoc_by_status())

    @pytest.mark.parametrize("mode, dims", [
        (None, ["status"]), (None, ["customers.tier"]),
        ("associate", ["customers.tier"]), ("associate", ["status"]),
        ("error", ["customers.tier"]), ("error", ["status"]),
    ])
    async def test_undetermined_parameter_refused_in_every_mode(self, orders_engine, mode, dims):
        """Scenario: Undetermined attached parameter stays rejected (D2b) — the plain
        path refuses exactly what the association arm refuses."""
        q = mode_q(mode, dimensions=dims, measures=[_m(UNDETERMINED_PARAM)])
        with pytest.raises(SlayerError) as ei:
            await orders_engine.execute(q)
        assert_grain_residue(ei.value, param="weight")

    @pytest.mark.parametrize("mode", MODES)
    async def test_stray_positional_attached_value_refused_at_bind(self, orders_engine, mode):
        """Scenario: Positional value on a parameterless aggregation errors (D8)."""
        q = mode_q(mode, dimensions=["customers.tier"], measures=[_m(STRAY_POSITIONAL)])
        with pytest.raises(ValueError, match="takes no parameters"):
            await orders_engine.execute(q)


class TestArgumentMessagePrecedence:
    """Scenarios: Host column as a target ranking key names the column and the hop;
    Argument violation is reported ahead of a source violation (D5)."""

    @pytest.mark.parametrize("mode, dims", [
        ("broadcast", ["status"]), ("error", ["status"]), ("associate", None),
    ])
    async def test_host_ranking_key_names_column_root_and_hop(self, orders_engine, mode, dims):
        kw = {} if dims is None else {"dimensions": dims}
        q = mode_q(mode, measures=[_m(LAST_HOST)], **kw)
        with pytest.raises(ValueError) as ei:
            await orders_engine.execute(q)
        msg = str(ei.value)
        assert "ordered_at" in msg
        assert "customers" in msg
        assert "not attributable from" in msg
        assert "join hop to orders" in msg
        assert_ref_free(msg)

    @pytest.mark.parametrize("mode", MODES)
    async def test_argument_violation_wins_over_source_violation(self, dev1900_engine, mode):
        q = mode_q(mode, measures=[_m(LAST_BADPOP)])
        with pytest.raises(ValueError) as ei:
            await dev1900_engine.execute(q)
        msg = str(ei.value)
        assert "ordered_at" in msg
        assert "not attributable from regions" in msg
        assert "join hop to customers" in msg
        assert "region_events" not in msg
